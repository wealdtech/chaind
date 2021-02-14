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
	spec "github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
	"github.com/wealdtech/chaind/services/chaindb"
	"github.com/wealdtech/chaind/services/chaintime"
)

// Service is a chain database service.
type Service struct {
	eth2Client               eth2client.Service
	chainDB                  chaindb.Service
	blocksSetter             chaindb.BlocksSetter
	attestationsSetter       chaindb.AttestationsSetter
	attesterSlashingsSetter  chaindb.AttesterSlashingsSetter
	proposerSlashingsSetter  chaindb.ProposerSlashingsSetter
	depositsSetter           chaindb.DepositsSetter
	voluntaryExitsSetter     chaindb.VoluntaryExitsSetter
	beaconCommitteesProvider chaindb.BeaconCommitteesProvider
	chainTime                chaintime.Service
	refetch                  bool
	lastHandledBlockRoot     spec.Root
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
	log = zerologger.With().Str("service", "blocks").Str("impl", "standard").Logger().Level(parameters.logLevel)

	if err := registerMetrics(ctx, parameters.monitor); err != nil {
		return nil, errors.New("failed to register metrics")
	}

	blocksSetter, isBlocksSetter := parameters.chainDB.(chaindb.BlocksSetter)
	if !isBlocksSetter {
		return nil, errors.New("chain DB does not support block setting")
	}

	attestationsSetter, isAttestationsSetter := parameters.chainDB.(chaindb.AttestationsSetter)
	if !isAttestationsSetter {
		return nil, errors.New("chain DB does not support attestation setting")
	}

	attesterSlashingsSetter, isAttesterSlashingsSetter := parameters.chainDB.(chaindb.AttesterSlashingsSetter)
	if !isAttesterSlashingsSetter {
		return nil, errors.New("chain DB does not support attester slashing setting")
	}

	proposerSlashingsSetter, isProposerSlashingsSetter := parameters.chainDB.(chaindb.ProposerSlashingsSetter)
	if !isProposerSlashingsSetter {
		return nil, errors.New("chain DB does not support proposer slashing setting")
	}

	depositsSetter, isDepositsSetter := parameters.chainDB.(chaindb.DepositsSetter)
	if !isDepositsSetter {
		return nil, errors.New("chain DB does not support deposits setting")
	}

	voluntaryExitsSetter, isVoluntaryExitsSetter := parameters.chainDB.(chaindb.VoluntaryExitsSetter)
	if !isVoluntaryExitsSetter {
		return nil, errors.New("chain DB does not support voluntary exit setting")
	}

	beaconCommitteesProvider, isBeaconCommitteesProvider := parameters.chainDB.(chaindb.BeaconCommitteesProvider)
	if !isBeaconCommitteesProvider {
		return nil, errors.New("chain DB does not support beacon committee providing")
	}

	s := &Service{
		eth2Client:               parameters.eth2Client,
		chainDB:                  parameters.chainDB,
		blocksSetter:             blocksSetter,
		attestationsSetter:       attestationsSetter,
		attesterSlashingsSetter:  attesterSlashingsSetter,
		proposerSlashingsSetter:  proposerSlashingsSetter,
		depositsSetter:           depositsSetter,
		voluntaryExitsSetter:     voluntaryExitsSetter,
		beaconCommitteesProvider: beaconCommitteesProvider,
		chainTime:                parameters.chainTime,
		refetch:                  parameters.refetch,
	}

	// Update to current epoch before starting (in the background).
	go s.updateAfterRestart(ctx, parameters.startSlot)

	return s, nil
}

func (s *Service) updateAfterRestart(ctx context.Context, startSlot int64) {
	// Work out the slot from which to start.
	md, err := s.getMetadata(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to obtain metadata before catchup")
	}
	if startSlot >= 0 {
		// Explicit requirement to start at a given slot.
		md.LatestSlot = spec.Slot(startSlot)
	} else if md.LatestSlot > 0 {
		// We have a definite hit on this being the last processed slot; increment it to avoid duplication of work.
		md.LatestSlot++
	}

	log.Info().Uint64("slot", uint64(md.LatestSlot)).Msg("Catching up from slot")
	s.catchupOnRestart(ctx, md)
	if len(md.MissedSlots) > 0 {
		// Need this as a []uint64 for logging only.
		missedSlots := make([]uint64, len(md.MissedSlots))
		for i := range md.MissedSlots {
			missedSlots[i] = uint64(md.MissedSlots[i])
		}
		log.Info().Uints64("missed_slots", missedSlots).Msg("Re-fetching missed slots")
		s.handleMissed(ctx, md)
		// Catchup again, in case handling the missed took a while.
		log.Info().Uint64("slot", uint64(md.LatestSlot)).Msg("Catching up from slot")
		s.catchupOnRestart(ctx, md)
	}
	log.Info().Msg("Caught up")

	// At this stage we should be up-to-date; if not we need to make a note of the items we missed.
	md, err = s.getMetadata(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to obtain metadata after catchup")
	}
	// We will be updating latest slot as the daemon progresses, so mark anything between the last
	// slot we processed and the current slot as missed.
	for ; md.LatestSlot < s.chainTime.CurrentSlot(); md.LatestSlot++ {
		md.MissedSlots = append(md.MissedSlots, md.LatestSlot)
	}
	ctx, cancel, err := s.chainDB.BeginTx(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to begin transaction after catchup")
		return
	}
	if err := s.setMetadata(ctx, md); err != nil {
		cancel()
		log.Fatal().Err(err).Msg("Failed to set metadata after catchup")
	}
	if err := s.chainDB.CommitTx(ctx); err != nil {
		cancel()
		log.Fatal().Err(err).Msg("Failed to commit transaction")
	}

	// Set up the handler for new chain head updates.
	if err := s.eth2Client.(eth2client.EventsProvider).Events(ctx, []string{"head"}, func(event *api.Event) {
		if event.Data == nil {
			// Happens when the channel shuts down, nothing to worry about.
			return
		}
		eventData := event.Data.(*api.HeadEvent)
		s.OnBeaconChainHeadUpdated(ctx, eventData.Slot, eventData.Block, eventData.State, eventData.EpochTransition)
	}); err != nil {
		log.Fatal().Err(err).Msg("Failed to add beacon chain head updated handler")
	}
}

func (s *Service) catchupOnRestart(ctx context.Context, md *metadata) {
	for slot := md.LatestSlot; slot <= s.chainTime.CurrentSlot(); slot++ {
		log := log.With().Uint64("slot", uint64(slot)).Logger()
		// Each update goes in to its own transaction, to make the data available sooner.
		ctx, cancel, err := s.chainDB.BeginTx(ctx)
		if err != nil {
			log.Error().Err(err).Msg("Failed to begin transaction on update after restart")
			return
		}

		if err := s.updateBlockForSlot(ctx, slot); err != nil {
			log.Warn().Err(err).Msg("Failed to update block")
			md.MissedSlots = append(md.MissedSlots, slot)
		}

		md.LatestSlot = slot
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
		log.Trace().Msg("Updated block")
	}
}

func (s *Service) handleMissed(ctx context.Context, md *metadata) {
	failed := 0
	for i := 0; i < len(md.MissedSlots); i++ {
		log := log.With().Uint64("slot", uint64(md.MissedSlots[i])).Logger()
		// Each update goes in to its own transaction, to make the data available sooner.
		ctx, cancel, err := s.chainDB.BeginTx(ctx)
		if err != nil {
			log.Error().Err(err).Msg("Failed to begin transaction on update after restart")
			return
		}

		if err := s.updateBlockForSlot(ctx, md.MissedSlots[i]); err != nil {
			log.Warn().Err(err).Msg("Failed to update block")
			failed++
			cancel()
			continue
		} else {
			log.Trace().Msg("Updated block")
			// Remove this from the list of missed slots.
			missedSlots := make([]spec.Slot, len(md.MissedSlots)-1)
			copy(missedSlots[:failed], md.MissedSlots[:failed])
			copy(missedSlots[failed:], md.MissedSlots[i+1:])
			md.MissedSlots = missedSlots
			i--
		}

		if err := s.setMetadata(ctx, md); err != nil {
			log.Error().Err(err).Msg("Failed to set metadata")
			cancel()
			return
		}

		if err := s.chainDB.CommitTx(ctx); err != nil {
			log.Error().Err(err).Uint64("slot", uint64(md.MissedSlots[i])).Msg("Failed to commit transaction")
			cancel()
			return
		}
	}
}
