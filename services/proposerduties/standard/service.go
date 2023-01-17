// Copyright © 2020, 2021 Weald Technology Trading.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package standard

import (
	"context"

	eth2client "github.com/attestantio/go-eth2-client"
	api "github.com/attestantio/go-eth2-client/api/v1"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
	"github.com/wealdtech/chaind/services/chaindb"
	"github.com/wealdtech/chaind/services/chaintime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/semaphore"
)

// Service is a chain database service.
type Service struct {
	eth2Client           eth2client.Service
	chainDB              chaindb.Service
	proposerDutiesSetter chaindb.ProposerDutiesSetter
	chainTime            chaintime.Service
	activitySem          *semaphore.Weighted
}

// module-wide log.
var log zerolog.Logger

// New creates a new service.
func New(ctx context.Context, params ...Parameter) (*Service, error) {
	parameters, err := parseAndCheckParameters(params...)
	if err != nil {
		return nil, errors.Wrap(err, "problem with parameters")
	}

	// Set logging.
	log = zerologger.With().Str("service", "proposerduties").Str("impl", "standard").Logger().Level(parameters.logLevel)

	if err := registerMetrics(ctx, parameters.monitor); err != nil {
		return nil, errors.New("failed to register metrics")
	}

	proposerDutiesSetter, isProposerDutiesSetter := parameters.chainDB.(chaindb.ProposerDutiesSetter)
	if !isProposerDutiesSetter {
		return nil, errors.New("chain DB does not support proposer duty setting")
	}

	s := &Service{
		eth2Client:           parameters.eth2Client,
		chainDB:              parameters.chainDB,
		proposerDutiesSetter: proposerDutiesSetter,
		chainTime:            parameters.chainTime,
		activitySem:          semaphore.NewWeighted(1),
	}

	// Update to current epoch before starting (in the background).
	go s.updateAfterRestart(ctx, parameters.startEpoch)

	return s, nil
}

func (s *Service) updateAfterRestart(ctx context.Context, startEpoch int64) {
	// Work out the epoch from which to start.
	md, err := s.getMetadata(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to obtain metadata before catchup")
	}
	if startEpoch >= 0 {
		// Explicit requirement to start at a given epoch.
		md.LatestEpoch = startEpoch - 1
	}

	log.Info().Uint64("epoch", uint64(md.LatestEpoch)).Msg("Catching up from epoch")
	s.catchup(ctx, md)
	if len(md.MissedEpochs) > 0 {
		// Need this as a []uint64 for logging only.
		missedEpochs := make([]uint64, len(md.MissedEpochs))
		for i := range md.MissedEpochs {
			missedEpochs[i] = uint64(md.MissedEpochs[i])
		}
		log.Info().Uints64("missed_epochs", missedEpochs).Msg("Re-fetching missed epochs")
		s.handleMissed(ctx, md)
		// Catchup again, in case handling the missed epochs took a while.
		log.Info().Uint64("epoch", uint64(md.LatestEpoch)).Msg("Catching up from epoch")
		s.catchup(ctx, md)
	}
	log.Info().Msg("Caught up")

	// Set up the handler for new chain head updates.
	if err := s.eth2Client.(eth2client.EventsProvider).Events(ctx, []string{"head"}, func(event *api.Event) {
		eventData := event.Data.(*api.HeadEvent)
		s.OnBeaconChainHeadUpdated(ctx, eventData.Slot, eventData.Block, eventData.State, eventData.EpochTransition)
	}); err != nil {
		log.Fatal().Err(err).Msg("Failed to add beacon chain head updated handler")
	}
}

func (s *Service) catchup(ctx context.Context, md *metadata) {
	for epoch := phase0.Epoch(md.LatestEpoch + 1); epoch <= s.chainTime.CurrentEpoch(); epoch++ {
		if err := s.UpdateEpoch(ctx, md, epoch); err != nil {
			log.Error().Uint64("epoch", uint64(epoch)).Err(err).Msg("Failed to catchup")
			return
		}
	}
}

// UpdateEpoch updates proposer duties for the given epoch.
func (s *Service) UpdateEpoch(ctx context.Context,
	md *metadata,
	epoch phase0.Epoch,
) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.proposerduties.standard").Start(ctx, "UpdateEpoch",
		trace.WithAttributes(
			attribute.Int64("epoch", int64(epoch)),
		))
	defer span.End()

	// Each epoch goes in to its own transaction, to make the data available sooner.
	dbCtx, cancel, err := s.chainDB.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}

	if err := s.updateProposerDutiesForEpoch(dbCtx, epoch); err != nil {
		cancel()
		return errors.Wrap(err, "failed to update proposer duties")
	}
	span.AddEvent("Updated epoch")

	md.LatestEpoch = int64(epoch)
	if err := s.setMetadata(dbCtx, md); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set metadata")
	}
	span.AddEvent("Set metadata")

	if err := s.chainDB.CommitTx(dbCtx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to commit transaction")
	}
	span.AddEvent("Committed transaction")

	monitorEpochProcessed(epoch)
	return nil
}

func (s *Service) handleMissed(ctx context.Context, md *metadata) {
	failed := 0
	for i := 0; i < len(md.MissedEpochs); i++ {
		log := log.With().Uint64("epoch", uint64(md.MissedEpochs[i])).Logger()
		// Each update goes in to its own transaction, to make the data available sooner.
		dbCtx, cancel, err := s.chainDB.BeginTx(ctx)
		if err != nil {
			log.Error().Err(err).Msg("Failed to begin transaction on update after restart")
			return
		}

		if err := s.updateProposerDutiesForEpoch(dbCtx, md.MissedEpochs[i]); err != nil {
			log.Warn().Err(err).Msg("Failed to update proposer duties")
			failed++
			cancel()
			continue
		}
		// Remove this from the list of missed epochs.
		missedEpochs := make([]phase0.Epoch, len(md.MissedEpochs)-1)
		copy(missedEpochs[:failed], md.MissedEpochs[:failed])
		copy(missedEpochs[failed:], md.MissedEpochs[i+1:])
		md.MissedEpochs = missedEpochs
		i--

		if err := s.setMetadata(dbCtx, md); err != nil {
			log.Error().Err(err).Msg("Failed to set metadata")
			cancel()
			return
		}

		if err := s.chainDB.CommitTx(dbCtx); err != nil {
			log.Error().Err(err).Msg("Failed to commit transaction")
			cancel()
			return
		}
	}
}
