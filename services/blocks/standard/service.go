// Copyright © 2020 - 2022 Weald Technology Trading.
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
	"golang.org/x/sync/semaphore"
)

// Service is a chain database service.
type Service struct {
	eth2Client               eth2client.Service
	chainDB                  chaindb.Service
	blocksSetter             chaindb.BlocksSetter
	attestationsSetter       chaindb.AttestationsSetter
	attesterSlashingsSetter  chaindb.AttesterSlashingsSetter
	proposerSlashingsSetter  chaindb.ProposerSlashingsSetter
	syncAggregateSetter      chaindb.SyncAggregateSetter
	depositsSetter           chaindb.DepositsSetter
	voluntaryExitsSetter     chaindb.VoluntaryExitsSetter
	beaconCommitteesProvider chaindb.BeaconCommitteesProvider
	syncCommitteesProvider   chaindb.SyncCommitteesProvider
	chainTime                chaintime.Service
	refetch                  bool
	lastHandledBlockRoot     phase0.Root
	activitySem              *semaphore.Weighted
	syncCommittees           map[uint64]*chaindb.SyncCommittee
	//lmdFinalizer             lmdfinalizer.LMDFinalizer
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

	syncAggregateSetter, isSyncAggregateSetter := parameters.chainDB.(chaindb.SyncAggregateSetter)
	if !isSyncAggregateSetter {
		return nil, errors.New("chain DB does not support sync aggregate setting")
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

	syncCommitteesProvider, isSyncCommitteesProvider := parameters.chainDB.(chaindb.SyncCommitteesProvider)
	if !isSyncCommitteesProvider {
		return nil, errors.New("chain DB does not support sync committee providing")
	}

	s := &Service{
		eth2Client:               parameters.eth2Client,
		chainDB:                  parameters.chainDB,
		blocksSetter:             blocksSetter,
		attestationsSetter:       attestationsSetter,
		attesterSlashingsSetter:  attesterSlashingsSetter,
		proposerSlashingsSetter:  proposerSlashingsSetter,
		syncAggregateSetter:      syncAggregateSetter,
		depositsSetter:           depositsSetter,
		voluntaryExitsSetter:     voluntaryExitsSetter,
		beaconCommitteesProvider: beaconCommitteesProvider,
		syncCommitteesProvider:   syncCommitteesProvider,
		chainTime:                parameters.chainTime,
		refetch:                  parameters.refetch,
		activitySem:              parameters.activitySem,
		syncCommittees:           make(map[uint64]*chaindb.SyncCommittee),
		//lmdFinalizer:             nil,
	}

	// Note the current highest processed block for the monitor.
	md, err := s.getMetadata(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to obtain metadata")
	}
	monitorLatestBlock(md.LatestSlot)

	// Update to current epoch before starting (in the background).
	go s.updateAfterRestart(ctx, parameters.startSlot)

	return s, nil
}

func (s *Service) updateAfterRestart(ctx context.Context, startSlot int64) {
	// Only allow 1 handler to be active.
	acquired := s.activitySem.TryAcquire(1)
	if !acquired {
		log.Debug().Msg("Another handler running")
		return
	}
	defer s.activitySem.Release(1)

	// Work out the slot from which to start.
	md, err := s.getMetadata(ctx)
	if err != nil {
		// This will exit so not release the semaphore, but it's exiting so we don't care.
		//nolint:gocritic
		log.Fatal().Err(err).Msg("Failed to obtain metadata before catchup")
	}

	//go s.catchupLMDFinalizer(ctx, md) // I need to launch it before md is modified

	if startSlot >= 0 {
		// Explicit requirement to start at a given slot.
		md.LatestSlot = phase0.Slot(startSlot)
	} else if md.LatestSlot > 0 {
		// We have a definite hit on this being the last processed slot; increment it to avoid duplication of work.
		md.LatestSlot++
	}

	log.Info().Uint64("slot", uint64(md.LatestSlot)).Msg("Catching up from slot")
	s.catchup(ctx, md)
	log.Info().Msg("Caught up")

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

//func (s *Service) catchupLMDFinalizer(ctx context.Context, md *metadata) {
//	LFBRoot := md.LMDLatestFinalizedBlockRoot
//	LFBSlot := md.LMDLatestFinalizedSlot
//
//	var LFB *chaindb.Block
//	if LFBSlot != 0 {
//		block, err := s.chainDB.(chaindb.BlocksProvider).BlockByRoot(ctx, LFBRoot)
//		if err != nil {
//			log.Error().Err(err).Msg("could not fetch LMD latest finalized block")
//			return
//		}
//		LFB = block
//	} else {
//		blocks, err := s.chainDB.(chaindb.BlocksProvider).BlocksBySlot(ctx, 0)
//		if err != nil || len(blocks) == 0 {
//			log.Error().Err(err).Msg("could not fetch genesis block")
//			return
//		}
//		if len(blocks) > 1 {
//			log.Error().Msg("more than one genesis block")
//			return
//		}
//		LFB = blocks[0]
//	}
//
//	log.Info().Msg("Starting LMD finalizer")
//	s.lmdFinalizer = lmdfinalizer.New(LFB, log, s.onNewLMDFinalizedBlock)
//	log.Info().Msg("Started LMD finalizer")
//
//	log.Info().Uint64("from_slot", uint64(LFB.Slot)).Uint64("to_slot", uint64(md.LatestSlot)).Msg("Catching up LMD finalizer")
//
//	for slot := LFB.Slot + 1; slot <= md.LatestSlot; slot++ {
//		blocks, err := s.chainDB.(chaindb.BlocksProvider).BlocksBySlot(ctx, slot)
//		if err != nil {
//			log.Debug().Msg("block not in DB")
//			continue
//		}
//
//		for _, block := range blocks {
//			attestations, err := s.chainDB.(chaindb.AttestationsProvider).AttestationsInBlock(ctx, block.Root)
//			if err != nil {
//				log.Error().Err(err).Msg("error getting attestations from db")
//				attestations = nil
//			}
//			s.lmdFinalizer.AddBlock(block, attestations)
//		}
//	}
//	log.Info().Msg("Caught up LMD finalizer")
//}

func (s *Service) catchup(ctx context.Context, md *metadata) {
	firstSlot := md.LatestSlot
	// Increment if not 0 (as we do not differentiate between 0 and unset).
	if firstSlot > 0 {
		firstSlot++
	}

	for slot := firstSlot; slot <= s.chainTime.CurrentSlot(); slot++ {
		log := log.With().Uint64("slot", uint64(slot)).Logger()
		// Each update goes in to its own transaction, to make the data available sooner.
		ctx, cancel, err := s.chainDB.BeginTx(ctx)
		if err != nil {
			log.Error().Err(err).Msg("Failed to begin transaction on update after restart")
			return
		}

		if err := s.updateBlockForSlot(ctx, slot); err != nil {
			log.Warn().Err(err).Msg("Failed to update block")
			cancel()
			return
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
		monitorBlockProcessed(slot)
	}
}
