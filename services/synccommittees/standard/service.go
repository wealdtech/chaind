// Copyright © 2021 Weald Technology Trading.
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

// Service is a sync committee service.
type Service struct {
	eventsProvider               eth2client.EventsProvider
	syncCommitteesProvider       eth2client.SyncCommitteesProvider
	chainDB                      chaindb.Service
	syncCommitteesSetter         chaindb.SyncCommitteesSetter
	chainTime                    chaintime.Service
	activitySem                  *semaphore.Weighted
	epochsPerSyncCommitteePeriod uint64
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
	log = zerologger.With().Str("service", "synccommittees").Str("impl", "standard").Logger().Level(parameters.logLevel)

	if err := registerMetrics(ctx, parameters.monitor); err != nil {
		return nil, errors.New("failed to register metrics")
	}

	spec, err := parameters.specProvider.Spec(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to obtain spec")
	}
	var epochsPerSyncCommitteePeriod uint64
	if tmp, exists := spec["EPOCHS_PER_SYNC_COMMITTEE_PERIOD"]; exists {
		tmp2, ok := tmp.(uint64)
		if !ok {
			return nil, errors.New("EPOCHS_PER_SYNC_COMMITTEE_PERIOD of unexpected type")
		}
		epochsPerSyncCommitteePeriod = tmp2
	}

	if epochsPerSyncCommitteePeriod == 0 {
		log.Debug().Msg("Beacon chain node does not support Altair; not obtaining sync committees")
		return nil, nil
	}

	syncCommitteesSetter, isSyncCommitteesSetter := parameters.chainDB.(chaindb.SyncCommitteesSetter)
	if !isSyncCommitteesSetter {
		return nil, errors.New("chain DB does not support sync committee setting")
	}

	s := &Service{
		eventsProvider:               parameters.eth2Client.(eth2client.EventsProvider),
		syncCommitteesProvider:       parameters.eth2Client.(eth2client.SyncCommitteesProvider),
		chainDB:                      parameters.chainDB,
		syncCommitteesSetter:         syncCommitteesSetter,
		chainTime:                    parameters.chainTime,
		activitySem:                  semaphore.NewWeighted(1),
		epochsPerSyncCommitteePeriod: epochsPerSyncCommitteePeriod,
	}

	// Update to current epoch (synchronous, as sync committee information is needed by blocks).
	s.updateAfterRestart(ctx, parameters.startPeriod)

	return s, nil
}

func (s *Service) updateAfterRestart(ctx context.Context, startPeriod int64) {
	// Work out the period from which to start.
	md, err := s.getMetadata(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to obtain metadata before catchup")
	}
	if startPeriod >= 0 {
		// Explicit requirement to start at a given epoch.
		md.LatestPeriod = uint64(startPeriod)
	} else if md.LatestPeriod > 0 {
		// We have a definite hit on this being the last processed period; increment it to avoid duplication of work.
		md.LatestPeriod++
	}
	if s.chainTime.AltairInitialSyncCommitteePeriod() > md.LatestPeriod {
		md.LatestPeriod = s.chainTime.AltairInitialSyncCommitteePeriod()
	}

	log.Info().Uint64("period", md.LatestPeriod).Msg("Catching up from period")
	s.catchup(ctx, md)
	log.Info().Msg("Caught up")

	// Set up the handler for new chain head updates.
	if err := s.eventsProvider.Events(ctx, []string{"head"}, func(event *api.Event) {
		eventData := event.Data.(*api.HeadEvent)
		s.OnBeaconChainHeadUpdated(ctx, eventData.Slot, eventData.Block, eventData.State, eventData.EpochTransition)
	}); err != nil {
		log.Fatal().Err(err).Msg("Failed to add sync chain head updated handler")
	}
}

func (s *Service) catchup(ctx context.Context, md *metadata) {
	for period := md.LatestPeriod; period <= s.chainTime.CurrentSyncCommitteePeriod(); period++ {
		log := log.With().Uint64("period", period).Logger()
		// Each update goes in to its own transaction, to make the data available sooner.
		ctx, cancel, err := s.chainDB.BeginTx(ctx)
		if err != nil {
			log.Error().Err(err).Msg("Failed to begin transaction on update after restart")
			return
		}

		if err := s.updateSyncCommitteeForPeriod(ctx, period); err != nil {
			log.Warn().Err(err).Msg("Failed to update sync committee")
			cancel()
			return
		}

		md.LatestPeriod = period
		if err := s.setMetadata(ctx, md); err != nil {
			log.Error().Err(err).Msg("Failed to set metadata")
			cancel()
			return
		}

		if err := s.chainDB.CommitTx(ctx); err != nil {
			log.Error().Err(err).Msg("Failed to commit transaction")
			cancel()
			return
		}
		log.Trace().Msg("Added sync committee")
	}
}
