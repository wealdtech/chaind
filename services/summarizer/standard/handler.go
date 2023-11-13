// Copyright © 2021 - 2023 Weald Technology Limited.
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
	"time"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// OnFinalityUpdated is called when finality has been updated in the database.
// This is usually triggered by the finalizer.
func (s *Service) OnFinalityUpdated(
	ctx context.Context,
	finalizedEpoch phase0.Epoch,
) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.summarizer.standard").Start(ctx, "OnFinalityUpdated",
		trace.WithAttributes(
			attribute.Int64("finalized epoch", int64(finalizedEpoch)),
		))
	defer span.End()

	log := log.With().Uint64("finalized_epoch", uint64(finalizedEpoch)).Logger()
	log.Trace().Msg("Handler called")

	// Only allow 1 handler to be active.
	acquired := s.activitySem.TryAcquire(1)
	if !acquired {
		log.Debug().Msg("Another handler running")
		return
	}
	defer s.activitySem.Release(1)

	if finalizedEpoch == 0 {
		log.Debug().Msg("Not summarizing on epoch 0")
		return
	}
	summaryEpoch := finalizedEpoch - 1

	if err := s.summarizeEpochs(ctx, summaryEpoch); err != nil {
		log.Warn().Err(err).Msg("Failed to update epochs; finished handling finality checkpoint")
		return
	}
	if err := s.summarizeBlocks(ctx, summaryEpoch); err != nil {
		log.Warn().Err(err).Msg("Failed to update blocks; finished handling finality checkpoint")
		return
	}
	if err := s.summarizeValidators(ctx, summaryEpoch); err != nil {
		log.Warn().Err(err).Msg("Failed to update validators; finished handling finality checkpoint")
		return
	}

	md, err := s.getMetadata(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to obtain metadata for day summarizer; finished handling finality checkpoint")
		return
	}
	if md.PeriodicValidatorRollups {
		if err := s.summarizeValidatorDays(ctx); err != nil {
			log.Warn().Err(err).Msg("Failed to update validator days; finished handling finality checkpoint")
			return
		}

		if err := s.prune(ctx, summaryEpoch); err != nil {
			log.Warn().Err(err).Msg("Failed to prune summaries; finished handling finality checkpoint")
			return
		}
	}

	monitorEpochProcessed(finalizedEpoch)
	log.Trace().Msg("Finished handling finality checkpoint")
}

func (s *Service) summarizeEpochs(ctx context.Context, summaryEpoch phase0.Epoch) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.summarizer.standard").Start(ctx, "summarizeEpochs",
		trace.WithAttributes(
			attribute.Int64("target epoch", int64(summaryEpoch)),
		))
	defer span.End()

	if !s.epochSummaries {
		return nil
	}

	md, err := s.getMetadata(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to obtain metadata for epoch summarizer")
	}

	firstEpoch := md.LastEpoch
	if firstEpoch != 0 {
		firstEpoch++
	}

	// Limit the number of epochs summarised per pass, if we are also pruning.
	targetEpoch := summaryEpoch
	maxEpochsPerRun := phase0.Epoch(s.maxDaysPerRun) * s.epochsPerDay()
	if s.validatorEpochRetention != nil && maxEpochsPerRun > 0 && summaryEpoch-firstEpoch > maxEpochsPerRun {
		targetEpoch = firstEpoch + maxEpochsPerRun
		if targetEpoch > summaryEpoch {
			targetEpoch = summaryEpoch
		}
	}

	log.Trace().Uint64("first_epoch", uint64(firstEpoch)).Uint64("target_epoch", uint64(targetEpoch)).Msg("Epochs catchup bounds")

	for epoch := firstEpoch; epoch <= targetEpoch; epoch++ {
		updated, err := s.summarizeEpoch(ctx, md, epoch)
		if err != nil {
			return errors.Wrapf(err, "failed to update summary for epoch %d", epoch)
		}
		if !updated {
			log.Debug().Uint64("epoch", uint64(epoch)).Msg("Not enough data to update summary")
			return nil
		}
	}

	return nil
}

func (s *Service) summarizeBlocks(ctx context.Context,
	summaryEpoch phase0.Epoch,
) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.summarizer.standard").Start(ctx, "summarizeBlocks",
		trace.WithAttributes(
			attribute.Int64("target epoch", int64(summaryEpoch)),
		))
	defer span.End()

	if !s.blockSummaries {
		return nil
	}

	md, err := s.getMetadata(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to obtain metadata for block finality")
	}

	targetEpoch := summaryEpoch
	firstEpoch := md.LastBlockEpoch
	if firstEpoch != 0 {
		firstEpoch++
	}
	log.Trace().Uint64("first_epoch", uint64(firstEpoch)).Uint64("target_epoch", uint64(targetEpoch)).Msg("Blocks catchup bounds")

	// The last epoch updated in the metadata tells us how far we can summarize,
	// as it checks for the component data.  As such, if the finalized epoch
	// is beyond our summarized epoch we truncate to the summarized value.
	// However, if we don't have validator balances the summarizer won't run at all
	// for epochs, so if the last epoch is 0 we continue.
	if targetEpoch > md.LastEpoch && md.LastEpoch != 0 {
		targetEpoch = md.LastEpoch
	}

	for epoch := firstEpoch; epoch <= targetEpoch; epoch++ {
		if err := s.summarizeBlocksInEpoch(ctx, md, epoch); err != nil {
			return errors.Wrap(err, "failed to update block summaries for epoch")
		}
	}

	return nil
}

func (s *Service) summarizeValidators(ctx context.Context, summaryEpoch phase0.Epoch) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.summarizer.standard").Start(ctx, "summarizeValidators",
		trace.WithAttributes(
			attribute.Int64("target epoch", int64(summaryEpoch)),
		))
	defer span.End()

	if !s.validatorSummaries {
		return nil
	}

	md, err := s.getMetadata(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to obtain metadata for validator summarizer")
	}

	// The last epoch updated in the metadata tells us how far we can summarize,
	// as it checks for the component data.  As such, if the finalized epoch
	// is beyond our summarized epoch we truncate to the summarized value.
	// However, if we don't have validator balances the summarizer won't run at all
	// for epochs, so if the last epoch is 0 we continue.
	if summaryEpoch > md.LastEpoch && md.LastEpoch != 0 {
		summaryEpoch = md.LastEpoch
	}

	firstEpoch := md.LastValidatorEpoch
	if firstEpoch != 0 {
		firstEpoch++
	}

	// Limit the number of epochs summarised per pass, if we are also pruning.
	maxEpochsPerRun := phase0.Epoch(s.maxDaysPerRun) * s.epochsPerDay()
	targetEpoch := summaryEpoch
	if s.validatorEpochRetention != nil && maxEpochsPerRun > 0 && targetEpoch-firstEpoch > maxEpochsPerRun {
		targetEpoch = firstEpoch + maxEpochsPerRun
		if targetEpoch > summaryEpoch {
			targetEpoch = summaryEpoch
		}
	}
	log.Trace().Uint64("first_epoch", uint64(firstEpoch)).Uint64("target_epoch", uint64(targetEpoch)).Msg("Validators catchup bounds")

	for epoch := firstEpoch; epoch <= targetEpoch; epoch++ {
		if err := s.summarizeValidatorsInEpoch(ctx, md, epoch); err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to update validator summaries in epoch %d", epoch))
		}
	}

	return nil
}

func (s *Service) summarizeValidatorDays(ctx context.Context) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.summarizer.standard").Start(ctx, "summarizeValidatorDays")
	defer span.End()

	md, err := s.getMetadata(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to obtain metadata for validator day summarizer")
	}

	epochSummariesTime := s.chainTime.StartOfEpoch(md.LastValidatorEpoch).In(time.UTC)
	daySummariesTime := time.Unix(md.LastValidatorDay, 0).In(time.UTC)
	log.Trace().Time("epoch_summaries_time", epochSummariesTime).Time("day_summaries_time", daySummariesTime).Msg("Times")
	if epochSummariesTime.After(daySummariesTime.AddDate(0, 0, 1)) {
		// We have updates.
		var startTime time.Time
		if md.LastValidatorDay == -1 {
			// Start at the beginning of the day in which genesis occurred.
			genesis := s.chainTime.GenesisTime().In(time.UTC)
			startTime = time.Date(genesis.Year(), genesis.Month(), genesis.Day(), 0, 0, 0, 0, time.UTC)
		} else {
			startTime = daySummariesTime.AddDate(0, 0, 1)
		}
		endTimestamp := epochSummariesTime.AddDate(0, 0, -1)

		for timestamp := startTime; timestamp.Before(endTimestamp); timestamp = timestamp.AddDate(0, 0, 1) {
			if err := s.summarizeValidatorsInDay(ctx, timestamp); err != nil {
				return errors.Wrap(err, fmt.Sprintf("failed to update validator summaries for day %s", timestamp.Format("2006-01-02")))
			}
		}
	}

	return nil
}

func (s *Service) epochsPerDay() phase0.Epoch {
	return phase0.Epoch(86400.0 / s.chainTime.SlotDuration().Seconds() / float64(s.chainTime.SlotsPerEpoch()))
}
