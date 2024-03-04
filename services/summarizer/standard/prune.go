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
	"time"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
)

func (s *Service) prune(ctx context.Context, summaryEpoch phase0.Epoch) error {
	if s.validatorBalanceRetention != nil {
		if err := s.pruneBalances(ctx, summaryEpoch); err != nil {
			return err
		}
		monitorBalancePruned()
	}

	if s.validatorEpochRetention != nil {
		if err := s.pruneEpochs(ctx, summaryEpoch); err != nil {
			return err
		}
		monitorEpochPruned()
	}

	return nil
}

func (s *Service) pruneBalances(ctx context.Context, summaryEpoch phase0.Epoch) error {
	summaryTime := s.chainTime.StartOfEpoch(summaryEpoch)
	pruneTime := s.validatorBalanceRetention.Decrement(summaryTime)

	// Ensure that we're not pruning to a point before the summary.
	daySummaries, err := s.chainDB.(chaindb.ValidatorDaySummariesProvider).ValidatorDaySummaries(ctx, &chaindb.ValidatorDaySummaryFilter{
		Order: chaindb.OrderLatest,
		Limit: 1,
	})
	if err != nil {
		return errors.Wrap(err, "failed to obtain latest day summary")
	}
	if len(daySummaries) == 0 {
		log.Trace().Msg("No validator day summaries, not pruning")
		return nil
	}
	summarizedTime := daySummaries[0].StartTimestamp.AddDate(0, 0, 1)
	if summarizedTime.Before(pruneTime) {
		// We are attempting to prune data that we have not yet summarized; do not do this.
		pruneTime = summarizedTime.Add(-1 * time.Second)
	}

	pruneEpoch := s.chainTime.TimestampToEpoch(pruneTime)
	log.Trace().Stringer("retention", s.validatorBalanceRetention).Time("summary_time", summaryTime).Time("summarized_time", summarizedTime).Time("prune_time", pruneTime).Uint64("prune_epoch", uint64(pruneEpoch)).Msg("Prune parameters for balances")

	ctx, cancel, err := s.chainDB.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction to prune validator balances")
	}

	if err := s.chainDB.(chaindb.ValidatorBalancesPruner).PruneValidatorBalances(ctx, pruneEpoch, s.validatorRetain); err != nil {
		cancel()
		return errors.Wrap(err, "failed to prune validator balances")
	}

	if err := s.chainDB.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set commit transaction to prune validator balances")
	}
	log.Trace().Msg("Pruned validator balances")

	return nil
}

func (s *Service) pruneEpochs(ctx context.Context, summaryEpoch phase0.Epoch) error {
	summaryTime := s.chainTime.StartOfEpoch(summaryEpoch)
	pruneTime := s.validatorEpochRetention.Decrement(summaryTime)

	// Ensure that we're not pruning to a point before the summary.
	daySummaries, err := s.chainDB.(chaindb.ValidatorDaySummariesProvider).ValidatorDaySummaries(ctx, &chaindb.ValidatorDaySummaryFilter{
		Order: chaindb.OrderLatest,
		Limit: 1,
	})
	if err != nil {
		return errors.Wrap(err, "failed to obtain latest day summary")
	}
	if len(daySummaries) == 0 {
		log.Trace().Msg("No validator day summaries, not pruning")
		return nil
	}
	summarizedTime := daySummaries[0].StartTimestamp.AddDate(0, 0, 1)
	if summarizedTime.Before(pruneTime) {
		// We are attempting to prune data that we have not yet summarized; do not do this.
		pruneTime = summarizedTime.Add(-1 * time.Second)
	}

	pruneEpoch := s.chainTime.TimestampToEpoch(pruneTime)
	log.Trace().Stringer("retention", s.validatorEpochRetention).Time("summary_time", summaryTime).Time("summarized_time", summarizedTime).Time("prune_time", pruneTime).Uint64("prune_epoch", uint64(pruneEpoch)).Msg("Prune parameters for epochs")

	ctx, cancel, err := s.chainDB.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction to prune validator epoch summaries")
	}

	if err := s.chainDB.(chaindb.ValidatorEpochSummariesPruner).PruneValidatorEpochSummaries(ctx, pruneEpoch, s.validatorRetain); err != nil {
		cancel()
		return errors.Wrap(err, "failed to prune validator epoch summaries")
	}

	if err := s.chainDB.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set commit transaction to prune validator epoch summaries")
	}
	log.Trace().Msg("Pruned validator epoch summaries")

	return nil
}
