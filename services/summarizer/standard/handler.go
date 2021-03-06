// Copyright © 2021 Weald Technology Limited.
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

	spec "github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
)

// OnFinalityUpdated is called when finality has been updated in the database.
func (s *Service) OnFinalityUpdated(
	ctx context.Context,
	finalizedEpoch spec.Epoch,
) {
	// We summarize 1 epoch behind finality, so decrement the value here.
	if finalizedEpoch == 0 {
		return
	}
	finalizedEpoch--

	log := log.With().Uint64("finalized_epoch", uint64(finalizedEpoch)).Logger()
	log.Trace().Msg("Handler called")

	// Only allow 1 handler to be active.
	acquired := s.activitySem.TryAcquire(1)
	if !acquired {
		log.Debug().Msg("Another handler running")
		return
	}
	defer s.activitySem.Release(1)

	md, err := s.getMetadata(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to obtain metadata on finality")
		return
	}

	if err := s.onFinalityUpdatedEpochs(ctx, md, finalizedEpoch); err != nil {
		log.Warn().Err(err).Msg("Failed to update epochs")
	}
	if err := s.onFinalityUpdatedBlocks(ctx, md, finalizedEpoch); err != nil {
		log.Warn().Err(err).Msg("Failed to update blocks")
	}
	if err := s.onFinalityUpdatedValidators(ctx, md, finalizedEpoch); err != nil {
		log.Warn().Err(err).Msg("Failed to update validators")
	}

	ctx, cancel, err := s.chainDB.BeginTx(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to begin transaction for summarizer metadata")
		return
	}
	if err := s.setMetadata(ctx, md); err != nil {
		cancel()
		log.Error().Err(err).Msg("Failed to set summarizer metadata")
		return
	}
	if err := s.chainDB.CommitTx(ctx); err != nil {
		cancel()
		log.Error().Err(err).Msg("Failed to commit transaction for summarizer metadata")
	}

	monitorEpochProcessed(finalizedEpoch - 1)
	log.Trace().Msg("Finished handling finality checkpoint")
}

func (s *Service) onFinalityUpdatedEpochs(ctx context.Context,
	md *metadata,
	finalizedEpoch spec.Epoch,
) error {
	if !s.epochSummaries {
		return nil
	}
	lastEpoch := md.LastEpoch
	if lastEpoch != 0 {
		lastEpoch++
	}
	log.Trace().Uint64("last_epoch", uint64(lastEpoch)).Uint64("finalized_epoch", uint64(finalizedEpoch)).Msg("Catchup bounds")

	for epoch := lastEpoch; epoch <= finalizedEpoch; epoch++ {
		if err := s.updateSummaryForEpoch(ctx, epoch); err != nil {
			// Could have failed because we don't have balances, in which case don't error but do return.
			if err.Error() != "no balances for epoch" {
				return errors.Wrapf(err, "failed to update summary for epoch %d", epoch)
			}
			log.Debug().Uint64("epoch", uint64(epoch)).Msg("no balances for epoch; exiting update early")
			return nil
		}
		md.LastEpoch = epoch
	}

	return nil
}

func (s *Service) onFinalityUpdatedBlocks(ctx context.Context,
	md *metadata,
	finalizedEpoch spec.Epoch,
) error {
	if !s.blockSummaries {
		return nil
	}
	lastBlockEpoch := md.LastBlockEpoch
	if lastBlockEpoch != 0 {
		lastBlockEpoch++
	}
	for epoch := lastBlockEpoch; epoch <= finalizedEpoch; epoch++ {
		if err := s.updateBlockSummariesForEpoch(ctx, epoch); err != nil {
			return errors.Wrap(err, "failed to update block summaries for epoch")
		}
		md.LastBlockEpoch = epoch
	}

	return nil
}

func (s *Service) onFinalityUpdatedValidators(ctx context.Context,
	md *metadata,
	finalizedEpoch spec.Epoch,
) error {
	if !s.validatorSummaries {
		return nil
	}
	lastValidatorEpoch := md.LastValidatorEpoch
	if lastValidatorEpoch != 0 {
		lastValidatorEpoch++
	}
	for epoch := lastValidatorEpoch; epoch <= finalizedEpoch; epoch++ {
		if err := s.updateValidatorSummariesForEpoch(ctx, epoch); err != nil {
			return errors.Wrap(err, "failed to update validator summaries for epoch")
		}
		md.LastValidatorEpoch = finalizedEpoch
	}

	return nil
}
