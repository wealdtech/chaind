// Copyright © 2023 Weald Technology Limited.
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
	"github.com/prysmaticlabs/go-bitfield"
	"github.com/wealdtech/chaind/services/chaindb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// summarizeValidatorsInDay updates the validator summaries in a given day.
func (s *Service) summarizeValidatorsInDay(ctx context.Context,
	startTime time.Time,
) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.summarizer.standard").Start(ctx, "summarizeValidatorsInDay",
		trace.WithAttributes(
			attribute.Int64("star time", startTime.Unix()),
		))
	defer span.End()

	// Ensure the timestamp points to the start of a day.
	startTime = time.Date(startTime.Year(), startTime.Month(), startTime.Day(), 0, 0, 0, 0, time.UTC)
	log := log.With().Str("date", startTime.Format("2006-01-02")).Logger()
	startEpoch := s.chainTime.TimestampToEpoch(startTime)
	endTime := startTime.AddDate(0, 0, 1)
	// The end epoch should be the last epoch that has finished at the given time, not the epoch in progress
	// at the given time, so this is always reduced by 1.
	endEpoch := s.chainTime.TimestampToEpoch(endTime) - 1
	log.Trace().Stringer("start_time", startTime).Stringer("end_time", endTime).Uint64("start_epoch", uint64(startEpoch)).Uint64("end_epoch", uint64(endEpoch)).Msg("epochs")

	// Ensure that we have enough epoch-level summarised data to turn this in to a day summary.
	md, err := s.getMetadata(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to obtain metadata for validator day summarizer check")
	}
	if md.LastEpoch < endEpoch {
		log.Debug().Uint64("last_epoch", uint64(md.LastEpoch)).Msg("Epoch summaries not yet ready to be rolled up to a daily entry")
		return nil
	}
	if md.LastValidatorEpoch < endEpoch {
		log.Debug().Uint64("last_epoch", uint64(md.LastEpoch)).Msg("Validator epoch summaries not yet ready to be rolled up to a daily entry")
		return nil
	}

	log.Trace().Msg("Summarising validator day")

	// Generate and populate the day summaries map.
	daySummaries := make(map[phase0.ValidatorIndex]*chaindb.ValidatorDaySummary)
	if err := s.addValidatorEpochSummaries(ctx, daySummaries, startTime, endTime); err != nil {
		return err
	}
	span.AddEvent("Set epoch information")
	if err := s.addValidatorSyncCommitteeSummaries(ctx, daySummaries, startTime, endTime); err != nil {
		return err
	}
	span.AddEvent("Set sync committee information")
	found, err := s.addValidatorBalanceSummaries(ctx, daySummaries, startTime, endTime)
	if err != nil {
		return err
	}
	if !found {
		log.Debug().Time("startTime", startTime).Msg("Validator balances not yet ready to be rolled up to a daily entry")
		return nil
	}
	span.AddEvent("Set balance information")

	// Turn in to array.
	summaries := make([]*chaindb.ValidatorDaySummary, 0, len(daySummaries))
	for _, daySummary := range daySummaries {
		summaries = append(summaries, daySummary)
	}

	ctx, cancel, err := s.chainDB.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction to set validator day summaries")
	}

	if err := s.chainDB.(chaindb.ValidatorDaySummariesSetter).SetValidatorDaySummaries(ctx, summaries); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set validator day summaries")
	}

	log.Trace().Msg("Set summaries")

	// Fetch updated metadata as it may have changed since we last obtained it.
	md, err = s.getMetadata(ctx)
	if err != nil {
		cancel()
		return errors.Wrap(err, "failed to obtain metadata for validator day summarizer")
	}
	md.LastValidatorDay = startTime.Unix()
	if err := s.setMetadata(ctx, md); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set summarizer metadata for validator day summary")
	}
	if err := s.chainDB.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set commit transaction to set validator day summary")
	}

	return nil
}

func (s *Service) addValidatorEpochSummaries(ctx context.Context,
	daySummaries map[phase0.ValidatorIndex]*chaindb.ValidatorDaySummary,
	startTime time.Time,
	endTime time.Time,
) error {
	startEpoch := s.chainTime.TimestampToEpoch(startTime)
	// The end epoch should be the last epoch that has finished at the given time, not the epoch in progress
	// at the given time, so this is always reduced by 1.
	endEpoch := s.chainTime.TimestampToEpoch(endTime) - 1

	// Take the epoch summaries and turn them in to day summaries.
	epochSummaries, err := s.chainDB.(chaindb.ValidatorEpochSummariesProvider).ValidatorSummaries(ctx, &chaindb.ValidatorSummaryFilter{
		From: &startEpoch,
		To:   &endEpoch,
	})
	if err != nil {
		return errors.Wrap(err, "failed to obtain validator epoch summaries")
	}
	log.Trace().Msg("Fetched validator epoch summaries")

	for _, epochSummary := range epochSummaries {
		if _, exists := daySummaries[epochSummary.Index]; !exists {
			daySummaries[epochSummary.Index] = &chaindb.ValidatorDaySummary{
				Index:          epochSummary.Index,
				StartTimestamp: startTime,
			}
		}
		daySummaries[epochSummary.Index].Proposals += epochSummary.ProposerDuties
		daySummaries[epochSummary.Index].ProposalsIncluded += epochSummary.ProposalsIncluded
		daySummaries[epochSummary.Index].Attestations++
		if epochSummary.AttestationIncluded {
			daySummaries[epochSummary.Index].AttestationsIncluded++
			if epochSummary.AttestationTargetCorrect != nil && *epochSummary.AttestationTargetCorrect {
				daySummaries[epochSummary.Index].AttestationsTargetCorrect++
			}
			if epochSummary.AttestationHeadCorrect != nil && *epochSummary.AttestationHeadCorrect {
				daySummaries[epochSummary.Index].AttestationsHeadCorrect++
			}
			if epochSummary.AttestationSourceTimely != nil && *epochSummary.AttestationSourceTimely {
				daySummaries[epochSummary.Index].AttestationsSourceTimely++
			}
			if epochSummary.AttestationTargetTimely != nil && *epochSummary.AttestationTargetTimely {
				daySummaries[epochSummary.Index].AttestationsTargetTimely++
			}
			if epochSummary.AttestationHeadTimely != nil && *epochSummary.AttestationHeadTimely {
				daySummaries[epochSummary.Index].AttestationsHeadTimely++
			}
			if epochSummary.AttestationInclusionDelay != nil {
				daySummaries[epochSummary.Index].AttestationsInclusionDelay += float64(*epochSummary.AttestationInclusionDelay)
			}
		}
	}

	// Fix up inclusion delay.
	for _, daySummary := range daySummaries {
		daySummary.AttestationsInclusionDelay /= float64(daySummary.AttestationsIncluded)
	}

	return nil
}

func (s *Service) addValidatorSyncCommitteeSummaries(ctx context.Context,
	daySummaries map[phase0.ValidatorIndex]*chaindb.ValidatorDaySummary,
	startTime time.Time,
	endTime time.Time,
) error {
	startEpoch := s.chainTime.TimestampToEpoch(startTime)
	// The end epoch should be the last epoch that has finished at the given time, not the epoch in progress
	// at the given time, so this is always reduced by 1.
	endEpoch := s.chainTime.TimestampToEpoch(endTime)

	syncCommitteeSummary, err := s.syncCommitteeSummary(ctx, startEpoch, endEpoch)
	if err != nil {
		return err
	}

	for index, daySummary := range daySummaries {
		if scSummary, exists := syncCommitteeSummary[index]; exists {
			daySummary.SyncCommitteeMessages = scSummary.messages
			daySummary.SyncCommitteeMessagesIncluded = scSummary.messagesIncluded
		}
	}

	return nil
}

func (s *Service) addValidatorBalanceSummaries(ctx context.Context,
	daySummaries map[phase0.ValidatorIndex]*chaindb.ValidatorDaySummary,
	startTime time.Time,
	endTime time.Time,
) (
	bool,
	error,
) {
	startEpoch := s.chainTime.TimestampToEpoch(startTime)
	// The end epoch should be the last epoch that has finished at the given time, not the epoch in progress
	// at the given time, so this is always reduced by 1.
	endEpoch := s.chainTime.TimestampToEpoch(endTime) - 1

	// Obtain start balances.
	startBalances, err := s.chainDB.(chaindb.ValidatorsProvider).ValidatorBalancesByEpoch(ctx, startEpoch)
	if err != nil {
		return false, errors.Wrap(err, "failed to obtain validator start epoch balances")
	}
	if len(startBalances) == 0 {
		// Balances are not yet present.
		return false, nil
	}
	for _, startBalance := range startBalances {
		if _, exists := daySummaries[startBalance.Index]; !exists {
			// If we do not have a record of the validator it means that it has no summary, most likely
			// because it is in the activation queue but not yet active.  We can safely ignore it.
			continue
		}
		daySummaries[startBalance.Index].StartBalance = uint64(startBalance.Balance)
		daySummaries[startBalance.Index].StartEffectiveBalance = uint64(startBalance.EffectiveBalance)
	}

	firstSlot := s.chainTime.FirstSlotOfEpoch(startEpoch)
	lastSlot := s.chainTime.LastSlotOfEpoch(endEpoch)
	// Obtain deposits, and turn them in to a map for easy lookup.
	// TODO see if this is inclusive or exclusive.
	dbDeposits, err := s.chainDB.(chaindb.DepositsProvider).DepositsForSlotRange(ctx, firstSlot, lastSlot+1)
	if err != nil {
		return false, errors.Wrap(err, "failed to obtain deposits")
	}
	totalDeposits := make(map[phase0.ValidatorIndex]phase0.Gwei)
	for _, deposit := range dbDeposits {
		validators, err := s.chainDB.(chaindb.ValidatorsProvider).ValidatorsByPublicKey(ctx, []phase0.BLSPubKey{deposit.ValidatorPubKey})
		if err != nil {
			return false, err
		}
		if len(validators) == 0 {
			// This can happen with an invalid deposit, so ignore it.
			continue
		}
		totalDeposits[validators[deposit.ValidatorPubKey].Index] += deposit.Amount
	}

	// Obtain withdrawals, and turn them in to a map for easy lookup.
	dbWithdrawals, err := s.chainDB.(chaindb.WithdrawalsProvider).Withdrawals(ctx, &chaindb.WithdrawalFilter{
		From: &firstSlot,
		To:   &lastSlot,
	})
	if err != nil {
		return false, errors.Wrap(err, "failed to obtain withdrawals")
	}
	totalWithdrawals := make(map[phase0.ValidatorIndex]phase0.Gwei)
	for _, withdrawal := range dbWithdrawals {
		totalWithdrawals[withdrawal.ValidatorIndex] += withdrawal.Amount
	}

	// We want the start balance for the epoch after our end epoch so that we obtain the correct rewards.
	endBalances, err := s.chainDB.(chaindb.ValidatorsProvider).ValidatorBalancesByEpoch(ctx, endEpoch+1)
	if err != nil {
		return false, errors.Wrap(err, "failed to obtain validator end epoch balances")
	}
	if len(endBalances) == 0 {
		// Balances are not yet present.
		return false, nil
	}
	for _, endBalance := range endBalances {
		if _, exists := daySummaries[endBalance.Index]; !exists {
			// If we do not have a record of the validator it means that it has no summary, most likely
			// because it is in the activation queue but not yet active.  We can safely ignore it.
			continue
		}
		// Start off assuming that all balance changes are down to rewards.
		rewards := int64(endBalance.Balance) - int64(daySummaries[endBalance.Index].StartBalance)
		// Reduce by any deposits present.
		deposits, exists := totalDeposits[endBalance.Index]
		if exists {
			// Have to reduce rewards due to presence of deposits.
			rewards -= int64(deposits)
		} else {
			deposits = 0
		}
		// Increase by any withdrawals present.
		withdrawals, exists := totalWithdrawals[endBalance.Index]
		if exists {
			// Have to increase rewards due to presence of withdrawals.
			rewards += int64(withdrawals)
		} else {
			withdrawals = 0
		}

		capitalChange := int64(deposits) - int64(withdrawals)

		daySummaries[endBalance.Index].CapitalChange = capitalChange
		daySummaries[endBalance.Index].RewardChange = rewards
		daySummaries[endBalance.Index].EffectiveBalanceChange = int64(endBalance.EffectiveBalance) - int64(daySummaries[endBalance.Index].StartEffectiveBalance)
	}

	return true, nil
}

type scSummary struct {
	messages         int
	messagesIncluded int
}

func (s *Service) syncCommitteeSummary(ctx context.Context,
	startEpoch phase0.Epoch,
	endEpoch phase0.Epoch,
) (
	map[phase0.ValidatorIndex]*scSummary,
	error,
) {
	res := make(map[phase0.ValidatorIndex]*scSummary)

	if endEpoch < s.chainTime.AltairInitialEpoch() {
		// The timeframe ends before sync committees existed, nothing to summarise.
		return res, nil
	}

	if startEpoch < s.chainTime.AltairInitialEpoch() {
		// The timeframs starts before sync committees existed, fix it.
		startEpoch = s.chainTime.AltairInitialEpoch()
	}

	syncCommittees, err := s.syncCommitteesForEpochs(ctx, startEpoch, endEpoch)
	if err != nil {
		return nil, err
	}

	syncCommitteeMap := make(map[uint64][]phase0.ValidatorIndex)
	for _, syncCommittee := range syncCommittees {
		syncCommitteeMap[syncCommittee.Period] = syncCommittee.Committee
	}

	startSlot := s.chainTime.FirstSlotOfEpoch(startEpoch)
	endSlot := s.chainTime.FirstSlotOfEpoch(endEpoch+1) - 1

	syncAggregates, err := s.chainDB.(chaindb.SyncAggregateProvider).SyncAggregates(ctx, &chaindb.SyncAggregateFilter{
		From: &startSlot,
		To:   &endSlot,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to obtain sync aggregates")
	}

	for _, aggregate := range syncAggregates {
		if aggregate.InclusionSlot == 0 {
			log.Trace().Msg("Aggregate for slot 0 ignored")
			continue
		}
		period := s.chainTime.SlotToSyncCommitteePeriod(aggregate.InclusionSlot - 1)
		syncCommittee, exists := syncCommitteeMap[period]
		if !exists {
			log.Warn().Uint64("start_epoch", uint64(startEpoch)).Uint64("end_epoch", uint64(endEpoch)).Uint64("inclusion_slot", uint64(aggregate.InclusionSlot)).Uint64("period", period).Uint64("slot", uint64(aggregate.InclusionSlot-1)).Msg("No sync committee found for block, cannot progress")
			return nil, errors.New("no sync committee for block")
		}

		aggregateBits := bitfield.Bitlist(aggregate.Bits)
		for i := uint64(0); i < aggregateBits.Len(); i++ {
			if _, exists := res[syncCommittee[i]]; !exists {
				res[syncCommittee[i]] = &scSummary{}
			}
			res[syncCommittee[i]].messages++
			if aggregateBits.BitAt(i) {
				res[syncCommittee[i]].messagesIncluded++
			}
		}
	}

	return res, nil
}

func (s *Service) syncCommitteesForEpochs(ctx context.Context,
	startEpoch phase0.Epoch,
	endEpoch phase0.Epoch,
) (
	[]*chaindb.SyncCommittee,
	error,
) {
	startPeriod := s.chainTime.EpochToSyncCommitteePeriod(startEpoch)
	if startPeriod > 0 {
		// We could be on the first epoch of a period, so also grab the prior period.
		startPeriod--
	}
	endPeriod := s.chainTime.EpochToSyncCommitteePeriod(endEpoch)

	syncCommittees := make([]*chaindb.SyncCommittee, 0, endPeriod+1-startPeriod)
	for period := startPeriod; period <= endPeriod; period++ {
		syncCommittee, err := s.chainDB.(chaindb.SyncCommitteesProvider).SyncCommittee(ctx, period)
		if err != nil {
			return nil, errors.Wrap(err, "failed to obtain sync commiittees")
		}
		syncCommittees = append(syncCommittees, syncCommittee)
	}

	return syncCommittees, nil
}
