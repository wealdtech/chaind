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
	"github.com/wealdtech/chaind/services/blocks"
	"github.com/wealdtech/chaind/services/chaindb"
	"github.com/wealdtech/chaind/services/chaintime"
	"golang.org/x/sync/semaphore"
)

// Service is a finalizer service.
type Service struct {
	eth2Client     eth2client.Service
	chainDB        chaindb.Service
	blocksProvider chaindb.BlocksProvider
	blocksSetter   chaindb.BlocksSetter
	chainTime      chaintime.Service
	blocks         blocks.Service
	activitySem    *semaphore.Weighted
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
	log = zerologger.With().Str("service", "finalizer").Str("impl", "standard").Logger().Level(parameters.logLevel)

	blocksProvider, isBlocksProvider := parameters.chainDB.(chaindb.BlocksProvider)
	if !isBlocksProvider {
		return nil, errors.New("chain DB does not support block providing")
	}

	blocksSetter, isBlocksSetter := parameters.chainDB.(chaindb.BlocksSetter)
	if !isBlocksSetter {
		return nil, errors.New("chain DB does not support block setting")
	}

	s := &Service{
		eth2Client:     parameters.eth2Client,
		chainDB:        parameters.chainDB,
		blocksProvider: blocksProvider,
		blocksSetter:   blocksSetter,
		chainTime:      parameters.chainTime,
		blocks:         parameters.blocks,
		activitySem:    semaphore.NewWeighted(1),
	}

	// Set up the handler for new chain head updates.
	if err := s.eth2Client.(eth2client.EventsProvider).Events(ctx, []string{"finalized_checkpoint"}, func(event *api.Event) {
		if event.Data == nil {
			// Happens when the channel shuts down, nothing to worry about.
			return
		}
		eventData := event.Data.(*api.FinalizedCheckpointEvent)
		log.Trace().Str("event", eventData.String()).Msg("Received event")
		s.OnFinalityCheckpointReceived(ctx, eventData.Epoch, eventData.Block, eventData.State)
	}); err != nil {
		return nil, errors.Wrap(err, "failed to add finality checkpoint received handler")
	}

	return s, nil
}

// func (s *Service) updateAfterRestart(ctx context.Context) {
// 	log.Info().Msg("Catching up")
//
// 	acquired := s.activitySem.TryAcquire(1)
// 	if !acquired {
// 		log.Error().Msg("Failed to obtain update semaphore")
// 		return
// 	}
// 	defer s.activitySem.Release(1)
//
// 	ctx, cancel, err := s.chainDB.BeginTx(ctx)
// 	if err != nil {
// 		log.Error().Err(err).Msg("Failed to begin transaction")
// 		return
// 	}
//
// 	md, err := s.getMetadata(ctx)
// 	if err != nil {
// 		cancel()
// 		log.Error().Err(err).Msg("Failed to obtain metadata")
// 		return
// 	}
//
// 	finality, err := s.eth2Client.(eth2client.FinalityProvider).Finality(ctx, fmt.Sprintf("%d", s.chainTime.FirstSlotOfEpoch(md.LastFinalizedEpoch)))
// 	if err != nil {
// 		cancel()
// 		log.Error().Err(err).Msg("Failed to obtain finality")
// 		return
// 	}
// 	log.Trace().
// 		Uint64("last_finalized_epoch", uint64(md.LastFinalizedEpoch)).
// 		Uint64("finalzed_epoch", uint64(finality.Finalized.Epoch)).
// 		Str("finalized_root", fmt.Sprintf("%#x", finality.Finalized.Root)).
// 		Msg("Obtained chaindb finality")
//
// 	// Only fix indeterminate blocks if the finalizer has already run at least once.
// 	if md.LatestCanonicalSlot != 0 {
// 		// Find any indeterminate blocks and finalized them.
// 		log.Trace().Uint64("slot", uint64(md.LatestCanonicalSlot)).Msg("Updating canonical blocks after restart")
// 		md.LatestCanonicalSlot, err = s.updateCanonicalBlocks(ctx, finality.Finalized.Root, md.LatestCanonicalSlot)
// 		if err != nil {
// 			cancel()
// 			log.Error().Err(err).Msg("Failed to update canonical blocks after restart")
// 			return
// 		}
// 		log.Trace().Uint64("slot", uint64(md.LatestCanonicalSlot)).Msg("New latest canonical slot")
// 		if err := s.setMetadata(ctx, md); err != nil {
// 			cancel()
// 			log.Error().Err(err).Msg("Failed to update metadata for epoch")
// 			return
// 		}
//
// 		// Commit the transaction and start a new one.
// 		if err := s.chainDB.CommitTx(ctx); err != nil {
// 			cancel()
// 			log.Error().Err(err).Msg("Failed to commit initial transaction")
// 			return
// 		}
// 		ctx, cancel, err = s.chainDB.BeginTx(ctx)
// 		if err != nil {
// 			log.Error().Err(err).Msg("Failed to begin new transaction")
// 			return
// 		}
//
// 	}
//
// 	if finality.Finalized.Epoch != 0 {
// 		// Find any epochs with indeterminate attestations and finalize them.
// 		log.Trace().Msg("Updating attestation votes after restart")
// 		slots, err := s.chainDB.(chaindb.AttestationsProvider).IndeterminateAttestationSlots(ctx, 0, s.chainTime.FirstSlotOfEpoch(md.LastFinalizedEpoch))
// 		if err != nil {
// 			cancel()
// 			log.Error().Err(err).Msg("Failed to fetch indeterminate attestations after restart")
// 			return
// 		}
// 		epochs := make(map[spec.Epoch]bool)
// 		for _, slot := range slots {
// 			epochs[s.chainTime.SlotToEpoch(slot)] = true
// 		}
// 		orderedEpochs := make([]spec.Epoch, 0, len(epochs))
// 		for epoch := range epochs {
// 			orderedEpochs = append(orderedEpochs, epoch)
// 		}
// 		sort.Slice(orderedEpochs, func(i int, j int) bool {
// 			return orderedEpochs[i] < orderedEpochs[j]
// 		})
// 		log.Trace().Int("epochs", len(orderedEpochs)).Msg("Found unfinalized epochs")
// 		for _, epoch := range orderedEpochs {
// 			if err := s.updateAttestationsForEpoch(ctx, epoch); err != nil {
// 				cancel()
// 				log.Error().Uint64("epoch", uint64(epoch)).Err(err).Msg("Failed to update attestations for epoch after restart")
// 				return
// 			}
// 		}
// 	}
//
// 	if err := s.chainDB.CommitTx(ctx); err != nil {
// 		cancel()
// 		log.Error().Err(err).Msg("Failed to commit transaction")
// 		return
// 	}
//
// 	// Set up the handler for new chain head updates.
// 	if err := s.eth2Client.(eth2client.EventsProvider).Events(ctx, []string{"finalized_checkpoint"}, func(event *api.Event) {
// 		if event.Data == nil {
// 			// Happens when the channel shuts down, nothing to worry about.
// 			return
// 		}
// 		eventData := event.Data.(*api.FinalizedCheckpointEvent)
// 		log.Trace().Str("event", eventData.String()).Msg("Received event")
// 		s.OnFinalityCheckpointReceived(ctx, eventData.Epoch, eventData.Block, eventData.State)
// 	}); err != nil {
// 		log.Fatal().Err(err).Msg("Failed to add finality checkpoint received handler")
// 	}
//
// 	log.Info().Msg("Caught up")
// }
