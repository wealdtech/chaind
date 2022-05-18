// Copyright © 2021, 2022 Weald Technology Limited.
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
	"fmt"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
)

// OnFinalityUpdated is called when finality has been updated in the database.
func (s *Service) OnFinalityUpdated(
	ctx context.Context,
	finalizedEpoch phase0.Epoch,
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

	if err := s.onFinalityUpdatedEpochs(ctx, finalizedEpoch); err != nil {
		log.Warn().Err(err).Msg("Failed to update epochs")
	}
	if err := s.onFinalityUpdatedBlocks(ctx, finalizedEpoch); err != nil {
		log.Warn().Err(err).Msg("Failed to update blocks")
	}
	if err := s.onFinalityUpdatedValidators(ctx, finalizedEpoch); err != nil {
		log.Warn().Err(err).Msg("Failed to update validators")
	}

	monitorEpochProcessed(finalizedEpoch - 1)
	log.Trace().Msg("Finished handling finality checkpoint")
}

func (s *Service) onFinalityUpdatedEpochs(ctx context.Context, finalizedEpoch phase0.Epoch) error {
	if !s.epochSummaries {
		return nil
	}

	md, err := s.getMetadata(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to obtain metadata for epoch summarizer")
	}

	lastEpoch := md.LastEpoch
	if lastEpoch != 0 {
		lastEpoch++
	}
	log.Trace().Uint64("last_epoch", uint64(lastEpoch)).Uint64("finalized_epoch", uint64(finalizedEpoch)).Msg("Catchup bounds")

	for epoch := lastEpoch; epoch <= finalizedEpoch; epoch++ {
		updated, err := s.updateSummaryForEpoch(ctx, md, epoch)
		if err != nil {
			return errors.Wrapf(err, "failed to update summary for epoch %d", epoch)
		}
		if !updated {
			log.Debug().Uint64("epoch", uint64(epoch)).Msg("not enough data to update summary")
			return nil
		}
	}

	return nil
}

func (s *Service) onFinalityUpdatedBlocks(ctx context.Context,
	finalizedEpoch phase0.Epoch,
) error {
	if !s.blockSummaries {
		return nil
	}

	md, err := s.getMetadata(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to obtain metadata for block finality")
	}

	lastBlockEpoch := md.LastBlockEpoch
	if lastBlockEpoch != 0 {
		lastBlockEpoch++
	}

	// The last epoch updated in the metadata tells us how far we can summarize,
	// as it checks for the component data.  As such, if the finalized epoch
	// is beyond our summarized epoch we truncate to the summarized value.
	// However, if we don't have validator balances the summarizer won't run at all
	// for epochs, so if the last epoch is 0 we continue.
	if finalizedEpoch > md.LastEpoch && md.LastEpoch != 0 {
		finalizedEpoch = md.LastEpoch
	}

	for epoch := lastBlockEpoch; epoch <= finalizedEpoch; epoch++ {
		if err := s.updateBlockSummariesForEpoch(ctx, md, epoch); err != nil {
			return errors.Wrap(err, "failed to update block summaries for epoch")
		}
	}

	return nil
}

func (s *Service) onFinalityUpdatedValidators(ctx context.Context, finalizedEpoch phase0.Epoch) error {
	if !s.validatorSummaries {
		return nil
	}

	md, err := s.getMetadata(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to obtain metadata for validator summarizer")
	}

	// The last epoch updated in the meadata tells us how far we can summarize,
	// as it checks for the component data.  As such, if the finalized epoch
	// is beyond our summarized epoch we truncate to the summarized value.
	// However, if we don't have validator balances the summarizer won't run at all
	// for epochs, so if the last epoch is 0 we continue.
	if finalizedEpoch > md.LastEpoch && md.LastEpoch != 0 {
		finalizedEpoch = md.LastEpoch
	}

	lastValidatorEpoch := md.LastValidatorEpoch
	if lastValidatorEpoch != 0 {
		lastValidatorEpoch++
	}
	for epoch := lastValidatorEpoch; epoch <= finalizedEpoch; epoch++ {
		if err := s.updateValidatorSummariesForEpoch(ctx, md, epoch); err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to update validator summaries for epoch %d", epoch))
		}
	}

	return nil
}
