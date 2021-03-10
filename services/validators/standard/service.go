// Copyright © 2020 Weald Technology Trading.
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
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
	"github.com/wealdtech/chaind/services/chaindb"
	"github.com/wealdtech/chaind/services/chaintime"
	"golang.org/x/sync/semaphore"
)

// Service is a chain database service.
type Service struct {
	eth2Client       eth2client.Service
	chainDB          chaindb.Service
	validatorsSetter chaindb.ValidatorsSetter
	chainTime        chaintime.Service
	balances         bool
	activitySem      *semaphore.Weighted
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
	log = zerologger.With().Str("service", "validators").Str("impl", "standard").Logger().Level(parameters.logLevel)

	if err := registerMetrics(ctx, parameters.monitor); err != nil {
		return nil, errors.New("failed to register metrics")
	}

	validatorsSetter, isValidatorsSetter := parameters.chainDB.(chaindb.ValidatorsSetter)
	if !isValidatorsSetter {
		return nil, errors.New("chain DB does not support validator setting")
	}

	s := &Service{
		eth2Client:       parameters.eth2Client,
		chainDB:          parameters.chainDB,
		validatorsSetter: validatorsSetter,
		chainTime:        parameters.chainTime,
		balances:         parameters.balances,
		activitySem:      semaphore.NewWeighted(1),
	}

	// Update to current epoch (in the background).
	go s.updateAfterRestart(ctx, parameters.startEpoch)

	return s, nil
}

func (s *Service) updateAfterRestart(ctx context.Context, startEpoch int64) {
	// Only allow 1 handler to be active.
	acquired := s.activitySem.TryAcquire(1)
	if !acquired {
		log.Debug().Msg("Another handler running")
		return
	}

	md, err := s.getMetadata(ctx)
	if err != nil {
		s.activitySem.Release(1)
		log.Fatal().Err(err).Msg("Failed to obtain metadata before catchup")
	}
	if startEpoch >= 0 {
		// Explicit requirement to start at a given epoch; update metadata accordingly.
		ctx, cancel, err := s.chainDB.BeginTx(ctx)
		if err != nil {
			s.activitySem.Release(1)
			log.Fatal().Err(err).Msg("Failed to begin transaction to set start epoch")
		}
		if err := s.setMetadata(ctx, md); err != nil {
			cancel()
			s.activitySem.Release(1)
			log.Fatal().Err(err).Msg("Failed to set metadata with start epoch")
		}
		if err := s.chainDB.CommitTx(ctx); err != nil {
			cancel()
			s.activitySem.Release(1)
			log.Fatal().Err(err).Msg("Failed to commit transaction to set start epoch")
		}
	}

	log.Info().Uint64("epoch", uint64(md.LatestEpoch)).Msg("Catching up from epoch")
	currentEpoch := s.chainTime.CurrentEpoch()
	if err := s.onEpochTransitionValidators(ctx, md, currentEpoch); err != nil {
		log.Warn().Err(err).Msg("Failed to update to head; will retry")
	}
	if err := s.onEpochTransitionValidatorBalances(ctx, md, currentEpoch); err != nil {
		log.Warn().Err(err).Msg("Failed to update validators")
	}
	s.activitySem.Release(1)

	log.Info().Uint64("epoch", uint64(md.LatestEpoch)).Msg("Caught up")

	// Set up the handler for new chain head updates.
	if err := s.eth2Client.(eth2client.EventsProvider).Events(ctx, []string{"head"}, func(event *api.Event) {
		eventData := event.Data.(*api.HeadEvent)
		s.OnBeaconChainHeadUpdated(ctx, eventData.Slot, eventData.Block, eventData.State, eventData.EpochTransition)
	}); err != nil {
		log.Fatal().Err(err).Msg("Failed to add beacon chain head updated handler")
	}
}
