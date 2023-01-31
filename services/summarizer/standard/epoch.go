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
	"sort"
	"time"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
)

// summarizeEpoch updates the summary for a given epoch.
// Returns true if the epoch has been updated, otherwise false.
func (s *Service) summarizeEpoch(ctx context.Context,
	md *metadata,
	epoch phase0.Epoch,
) (
	bool,
	error,
) {
	started := time.Now()
	log := log.With().Uint64("epoch", uint64(epoch)).Logger()
	if !s.epochSummaries {
		log.Trace().Msg("Epoch summaries not enabled")
		return false, nil
	}
	log.Trace().Msg("Summarizing epoch")

	summary := &chaindb.EpochSummary{
		Epoch: epoch,
	}

	activeValidators, err := s.validatorSummaryStatsForEpoch(ctx, epoch, summary)
	if err != nil {
		return false, errors.Wrap(err, "failed to calculate validator summary statistics for epoch")
	}
	if summary.ActiveValidators == 0 {
		return false, errors.New("no active validators to summarize for epoch")
	}
	log.Trace().Dur("elapsed", time.Since(started)).Msg("Set validator summary stats")

	// Active balance and active effective balance.
	balances, err := s.validatorsProvider.ValidatorBalancesByEpoch(ctx, epoch)
	if err != nil {
		return false, errors.Wrap(err, "failed to obtain validator balances")
	}
	if len(balances) == 0 {
		// This can happen if chaind does not have validator balances enabled, or has not yet obtained
		// the balances.  We return false but no error.
		return false, nil
	}
	for i, balance := range balances {
		if activeValidators[i] {
			summary.ActiveRealBalance += balance.Balance
			summary.ActiveBalance += balance.EffectiveBalance
		}
	}
	log.Trace().Dur("elapsed", time.Since(started)).Msg("Set validator balances")

	err = s.blockStatsForEpoch(ctx, epoch, summary)
	if err != nil {
		return false, errors.Wrap(err, "failed to calculate block summary statistics for epoch")
	}
	log.Trace().Dur("elapsed", time.Since(started)).Msg("Set block summary stats")

	err = s.slashingsStatsForEpoch(ctx, epoch, summary)
	if err != nil {
		return false, errors.Wrap(err, "failed to calculate slashings summary statistics for epoch")
	}
	log.Trace().Dur("elapsed", time.Since(started)).Msg("Set slashing stats")

	err = s.attestationStatsForEpoch(ctx, epoch, balances, summary)
	if err != nil {
		return false, errors.Wrap(err, "failed to calculate attestation summary statistics for epoch")
	}
	log.Trace().Dur("elapsed", time.Since(started)).Msg("Set attestation stats")

	err = s.depositStatsForEpoch(ctx, epoch, summary)
	if err != nil {
		return false, errors.Wrap(err, "failed to calculate deposit summary statistics for epoch")
	}
	log.Trace().Dur("elapsed", time.Since(started)).Msg("Set deposit stats")

	ctx, cancel, err := s.chainDB.BeginTx(ctx)
	if err != nil {
		return false, errors.Wrap(err, "failed to begin transaction to set epoch summary")
	}
	if err := s.chainDB.(chaindb.EpochSummariesSetter).SetEpochSummary(ctx, summary); err != nil {
		cancel()
		return false, errors.Wrap(err, "failed to set epoch summary")
	}
	log.Warn().Uint64("md.lastEpoch", uint64(epoch)).Msg("Updated last epoch")
	md.LastEpoch = epoch
	if err := s.setMetadata(ctx, md); err != nil {
		cancel()
		return false, errors.Wrap(err, "failed to set summarizer metadata for epoch summary")
	}
	if err := s.chainDB.CommitTx(ctx); err != nil {
		cancel()
		return false, errors.Wrap(err, "failed to set commit transaction to set epoch summary")
	}
	log.Trace().Dur("elapsed", time.Since(started)).Msg("Set summary")

	return true, nil
}

func (s *Service) validatorSummaryStatsForEpoch(ctx context.Context,
	epoch phase0.Epoch,
	summary *chaindb.EpochSummary,
) (
	[]bool,
	error,
) {
	// Number of validators that are active, became active, and exited in this epoch.
	validators, err := s.validatorsProvider.Validators(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to obtain validators")
	}

	activeValidators := make([]bool, len(validators))
	for i, validator := range validators {
		switch {
		case validator.ActivationEpoch == epoch:
			summary.ActiveValidators++
			summary.ActivatingValidators++
			activeValidators[i] = true
		case validator.ExitEpoch == epoch:
			summary.ExitingValidators++
		case validator.ActivationEpoch <= epoch &&
			validator.ExitEpoch > epoch:
			summary.ActiveValidators++
			activeValidators[i] = true
		case validator.ActivationEligibilityEpoch <= epoch &&
			validator.ActivationEpoch != s.farFutureEpoch &&
			validator.ActivationEpoch > epoch:
			summary.ActivationQueueLength++
		}
	}
	return activeValidators, nil
}

func (s *Service) blockStatsForEpoch(ctx context.Context,
	epoch phase0.Epoch,
	summary *chaindb.EpochSummary,
) error {
	minSlot := s.chainTime.FirstSlotOfEpoch(epoch)
	maxSlot := s.chainTime.FirstSlotOfEpoch(epoch+1) - 1
	log.Trace().Uint64("epoch", uint64(epoch)).Uint64("min_slot", uint64(minSlot)).Uint64("max_slot", uint64(maxSlot)).Msg("Updating block statistics")

	blocks, err := s.blocksProvider.BlocksForSlotRange(ctx, minSlot, maxSlot+1)
	if err != nil {
		return errors.Wrap(err, "failed to obtain blocks")
	}

	for _, block := range blocks {
		if block.Canonical == nil || !*block.Canonical {
			continue
		}
		summary.CanonicalBlocks++
	}
	return nil
}

func (s *Service) depositStatsForEpoch(ctx context.Context,
	epoch phase0.Epoch,
	summary *chaindb.EpochSummary,
) error {
	minSlot := s.chainTime.FirstSlotOfEpoch(epoch)
	maxSlot := s.chainTime.FirstSlotOfEpoch(epoch+1) - 1
	log.Trace().Uint64("epoch", uint64(epoch)).Uint64("min_slot", uint64(minSlot)).Uint64("max_slot", uint64(maxSlot)).Msg("Updating deposit statistics")

	deposits, err := s.depositsProvider.DepositsForSlotRange(ctx, minSlot, maxSlot+1)
	if err != nil {
		return errors.Wrap(err, "failed to obtain deposits")
	}

	summary.Deposits = len(deposits)

	return nil
}

func (s *Service) attestationStatsForEpoch(ctx context.Context,
	epoch phase0.Epoch,
	balances []*chaindb.ValidatorBalance,
	summary *chaindb.EpochSummary,
) error {
	minSlot := s.chainTime.FirstSlotOfEpoch(epoch)
	maxSlot := s.chainTime.FirstSlotOfEpoch(epoch+1) - 1
	log.Trace().Uint64("epoch", uint64(epoch)).Uint64("min_slot", uint64(minSlot)).Uint64("max_slot", uint64(maxSlot)).Msg("Updating attestation statistics")

	attestationsForEpoch, err := s.attestationsProvider.AttestationsForSlotRange(ctx, minSlot, maxSlot+1)
	if err != nil {
		return errors.Wrap(err, "failed to obtain attestations")
	}

	epochAttestations := make([]*chaindb.Attestation, 0)
	seenAttestations := make(map[phase0.Root]bool)
	for _, attestation := range attestationsForEpoch {
		if attestation.Canonical == nil {
			// This should not happen, so flag it as an error.
			log.Error().Uint64("inclusion_slot", uint64(attestation.InclusionSlot)).Uint64("inclusion_index", attestation.InclusionIndex).Msg("Attestation is indeterminate; ignoring")
			continue
		}
		if !*attestation.Canonical {
			// This commonly happens when the block in which the attestation is included is non-canonical, so note it but no more.
			log.Trace().Uint64("inclusion_slot", uint64(attestation.InclusionSlot)).Uint64("inclusion_index", attestation.InclusionIndex).Msg("Attestation is not canonical; ignoring")
			continue
		}
		specAttestation := &phase0.Attestation{
			AggregationBits: attestation.AggregationBits,
			Data: &phase0.AttestationData{
				Slot:            attestation.Slot,
				Index:           attestation.CommitteeIndex,
				BeaconBlockRoot: attestation.BeaconBlockRoot,
				Source: &phase0.Checkpoint{
					Epoch: attestation.SourceEpoch,
					Root:  attestation.SourceRoot,
				},
				Target: &phase0.Checkpoint{
					Epoch: attestation.TargetEpoch,
					Root:  attestation.TargetRoot,
				},
			},
			// N.B. we don't keep the signature in the database so cannot provide it here.
		}
		specAttestationRoot, err := specAttestation.HashTreeRoot()
		if err != nil {
			return errors.Wrap(err, "failed to obtain attestation hash tree root")
		}
		if _, exists := seenAttestations[specAttestationRoot]; exists {
			// This is a duplicate.
			if attestation.Slot >= minSlot && attestation.Slot <= maxSlot {
				summary.DuplicateAttestationsForEpoch++
			}
			continue
		}
		seenAttestations[specAttestationRoot] = true
		if attestation.Slot >= minSlot && attestation.Slot <= maxSlot {
			summary.AttestationsForEpoch++
			epochAttestations = append(epochAttestations, attestation)
		}
	}

	// Fetch all attestations in the epoch for a simple count.
	attestationsInEpoch, err := s.attestationsProvider.AttestationsInSlotRange(ctx, minSlot, maxSlot+1)
	if err != nil {
		return errors.Wrap(err, "failed to obtain attestations in epoch")
	}
	summary.AttestationsInEpoch = len(attestationsInEpoch)

	// epochAttestations contains the list of attestations we need to process.
	attestingValidatorBalances := make(map[phase0.ValidatorIndex]phase0.Gwei)
	targetCorrectBalances := make(map[phase0.ValidatorIndex]phase0.Gwei)
	headCorrectBalances := make(map[phase0.ValidatorIndex]phase0.Gwei)
	for _, attestation := range epochAttestations {
		for _, index := range attestation.AggregationIndices {
			attestingValidatorBalances[index] = balances[index].EffectiveBalance
			if attestation.TargetCorrect != nil && *attestation.TargetCorrect {
				targetCorrectBalances[index] = balances[index].EffectiveBalance
			}
			if attestation.HeadCorrect != nil && *attestation.HeadCorrect {
				headCorrectBalances[index] = balances[index].EffectiveBalance
			}
		}
	}
	for _, attestingValidatorBalance := range attestingValidatorBalances {
		summary.AttestingValidators++
		summary.AttestingBalance += attestingValidatorBalance
	}
	for _, targetCorrectBalance := range targetCorrectBalances {
		summary.TargetCorrectValidators++
		summary.TargetCorrectBalance += targetCorrectBalance
	}
	for _, headCorrectBalance := range headCorrectBalances {
		summary.HeadCorrectValidators++
		summary.HeadCorrectBalance += headCorrectBalance
	}

	return nil
}

func (s *Service) slashingsStatsForEpoch(ctx context.Context,
	epoch phase0.Epoch,
	summary *chaindb.EpochSummary,
) error {
	minSlot := s.chainTime.FirstSlotOfEpoch(epoch)
	maxSlot := s.chainTime.FirstSlotOfEpoch(epoch+1) - 1
	log.Trace().Uint64("epoch", uint64(epoch)).Uint64("min_slot", uint64(minSlot)).Uint64("max_slot", uint64(maxSlot)).Msg("Updating slashing statistics")

	proposerSlashings, err := s.proposerSlashingsProvider.ProposerSlashingsForSlotRange(ctx, minSlot, maxSlot+1)
	if err != nil {
		return errors.Wrap(err, "failed to obtain proposer slashings")
	}

	for _, proposerSlashing := range proposerSlashings {
		// Need to check for any canonical proposer slashing for this validator prior to the current one.

		// Start off by fetching the proposer index.
		duties, err := s.proposerDutiesProvider.ProposerDutiesForSlotRange(ctx, proposerSlashing.Header1Slot, proposerSlashing.Header1Slot+1)
		if err != nil {
			return errors.Wrap(err, "failed to obtain validator index for proposer slashing")
		}
		if len(duties) != 1 {
			return errors.New("incorrect number of duties for proposer slashing")
		}

		// Fetch all proposer slashings for this index.
		slashings, err := s.proposerSlashingsProvider.ProposerSlashingsForValidator(ctx, duties[0].ValidatorIndex)
		if err != nil {
			return errors.Wrap(err, "failed to obtain proposer slashings for valiator")
		}
		if len(slashings) == 0 {
			return errors.New("incorrect number of slashings for proposer slashing")
		}

		// Ensure that we are the earliest.
		if slashings[0].InclusionSlot == proposerSlashing.InclusionSlot &&
			slashings[0].InclusionIndex == proposerSlashing.InclusionIndex {
			summary.ProposerSlashings++
		}
	}

	attesterSlashings, err := s.attesterSlashingsProvider.AttesterSlashingsForSlotRange(ctx, minSlot, maxSlot+1)
	if err != nil {
		return errors.Wrap(err, "failed to obtain attester slashings")
	}
	for _, attesterSlashing := range attesterSlashings {
		// Obtain all indices that have actually been slashed.
		slashedIndices := intersection(attesterSlashing.Attestation1Indices, attesterSlashing.Attestation2Indices)
		for _, slashedIndex := range slashedIndices {
			// Fetch all attester slashings for this index.
			slashings, err := s.attesterSlashingsProvider.AttesterSlashingsForValidator(ctx, slashedIndex)
			if err != nil {
				return errors.Wrap(err, "failed to obtain attester slashings for valiator")
			}
			if len(slashings) == 0 {
				return errors.New("incorrect number of slashings for attester slashing")
			}

			// Ensure that we are the earliest.
			if slashings[0].InclusionSlot == attesterSlashing.InclusionSlot &&
				slashings[0].InclusionIndex == attesterSlashing.InclusionIndex {
				summary.AttesterSlashings++
			}
		}
	}

	return nil
}

// intersection returns a list of items common between the two sets.
func intersection(set1 []phase0.ValidatorIndex, set2 []phase0.ValidatorIndex) []phase0.ValidatorIndex {
	sort.Slice(set1, func(i, j int) bool { return set1[i] < set1[j] })
	sort.Slice(set2, func(i, j int) bool { return set2[i] < set2[j] })
	res := make([]phase0.ValidatorIndex, 0)

	set1Pos := 0
	set2Pos := 0
	for set1Pos < len(set1) && set2Pos < len(set2) {
		switch {
		case set1[set1Pos] < set2[set2Pos]:
			set1Pos++
		case set2[set2Pos] < set1[set1Pos]:
			set2Pos++
		default:
			res = append(res, set1[set1Pos])
			set1Pos++
			set2Pos++
		}
	}

	return res
}
