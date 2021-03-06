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
	"github.com/wealdtech/chaind/services/chaindb"
)

// updateValidatorSummariesForEpoch updates the validator summaries for a given epoch.
func (s *Service) updateValidatorSummariesForEpoch(ctx context.Context, epoch spec.Epoch) error {
	log := log.With().Uint64("epoch", uint64(epoch)).Logger()
	if !s.validatorSummaries {
		log.Trace().Msg("Validator epoch summaries not enabled")
		return nil
	}
	log.Trace().Msg("Summarizing epoch")

	proposerDuties, validatorProposerDuties, err := s.validatorProposerDutiesForEpoch(ctx, epoch)
	if err != nil {
		return err
	}

	validatorProposals, err := s.validatorProposalsForEpoch(ctx, epoch, proposerDuties, validatorProposerDuties)
	if err != nil {
		return err
	}

	attestationsIncluded, attestationsTargetCorrect, attestationsHeadCorrect, attestationsInclusionDelay, err := s.attestationsForEpoch(ctx, epoch)
	if err != nil {
		return err
	}

	// Store the data.
	ctx, cancel, err := s.chainDB.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction to set validator epoch summaries")
	}
	for index := range attestationsIncluded {
		summary := &chaindb.ValidatorEpochSummary{
			Index:               index,
			Epoch:               epoch,
			ProposerDuties:      validatorProposerDuties[index],
			ProposalsIncluded:   validatorProposals[index],
			AttestationIncluded: attestationsIncluded[index],
		}
		if summary.AttestationIncluded {
			attestationTargetCorrect := attestationsTargetCorrect[index]
			summary.AttestationTargetCorrect = &attestationTargetCorrect
			attestationHeadCorrect := attestationsHeadCorrect[index]
			summary.AttestationHeadCorrect = &attestationHeadCorrect
			attestationInclusionDelay := int(attestationsInclusionDelay[index])
			summary.AttestationInclusionDelay = &attestationInclusionDelay
		}

		if err := s.chainDB.(chaindb.ValidatorEpochSummariesSetter).SetValidatorEpochSummary(ctx, summary); err != nil {
			cancel()
			return err
		}
	}
	if err := s.chainDB.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set commit transaction to set validator epoch summaries")
	}

	return nil
}

func (s *Service) validatorProposerDutiesForEpoch(ctx context.Context,
	epoch spec.Epoch,
) (
	[]*chaindb.ProposerDuty,
	map[spec.ValidatorIndex]int,
	error,
) {
	// Fetch the proposer duties for the epoch.
	proposerDuties, err := s.proposerDutiesProvider.ProposerDutiesForSlotRange(ctx,
		s.chainTime.FirstSlotOfEpoch(epoch),
		s.chainTime.FirstSlotOfEpoch(epoch+1),
	)
	if err != nil {
		return nil, nil, err
	}
	if len(proposerDuties) == 0 {
		return nil, nil, errors.New("no proposer duties to summarize for epoch")
	}
	if epoch == 0 {
		// Epoch 0 only has 31 proposer duties.  Drop in a dummy to avoid special cases below.
		tmp := make([]*chaindb.ProposerDuty, 32)
		tmp[0] = &chaindb.ProposerDuty{}
		copy(tmp[1:], proposerDuties)
		proposerDuties = tmp
	}

	// Summarise the proposer duties information by validator.
	validatorProposerDuties := make(map[spec.ValidatorIndex]int)
	for _, proposerDuty := range proposerDuties {
		if _, exists := validatorProposerDuties[proposerDuty.ValidatorIndex]; !exists {
			validatorProposerDuties[proposerDuty.ValidatorIndex] = 0
		}
		validatorProposerDuties[proposerDuty.ValidatorIndex]++
	}

	return proposerDuties, validatorProposerDuties, nil
}

func (s *Service) validatorProposalsForEpoch(ctx context.Context,
	epoch spec.Epoch,
	proposerDuties []*chaindb.ProposerDuty,
	validatorProposerDuties map[spec.ValidatorIndex]int,
) (
	map[spec.ValidatorIndex]int,
	error,
) {
	// Fetch the block presence for the epoch.
	presence, err := s.blocksProvider.CanonicalBlockPresenceForSlotRange(ctx,
		s.chainTime.FirstSlotOfEpoch(epoch),
		s.chainTime.FirstSlotOfEpoch(epoch+1),
	)
	if err != nil {
		return nil, err
	}
	validatorProposals := make(map[spec.ValidatorIndex]int)
	for i, present := range presence {
		if proposerDuties[i].Slot == 0 {
			// Not a real proposer duty; ignore.
			continue
		}
		proposerIndex := proposerDuties[i].ValidatorIndex
		if _, exists := validatorProposerDuties[proposerIndex]; !exists {
			validatorProposerDuties[proposerIndex] = 0
		}
		if present {
			validatorProposals[proposerIndex]++
		}
	}
	return validatorProposals, nil
}

func (s *Service) attestationsForEpoch(ctx context.Context,
	epoch spec.Epoch,
) (
	map[spec.ValidatorIndex]bool,
	map[spec.ValidatorIndex]bool,
	map[spec.ValidatorIndex]bool,
	map[spec.ValidatorIndex]spec.Slot,
	error,
) {
	// Fetch all attestations for the epoch.
	attestations, err := s.attestationsProvider.AttestationsForSlotRange(ctx,
		s.chainTime.FirstSlotOfEpoch(epoch),
		s.chainTime.FirstSlotOfEpoch(epoch+1),
	)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	log.Trace().Int("attestations", len(attestations)).Uint64("epoch", uint64(epoch)).Msg("Fetched attestations")

	// Mark up attestations for each validator.
	attestationsIncluded := make(map[spec.ValidatorIndex]bool)
	attestationsTargetCorrect := make(map[spec.ValidatorIndex]bool)
	attestationsHeadCorrect := make(map[spec.ValidatorIndex]bool)
	attestationsInclusionDelay := make(map[spec.ValidatorIndex]spec.Slot)
	for _, attestation := range attestations {
		if attestation.Canonical == nil || !*attestation.Canonical {
			log.Trace().Uint64("slot", uint64(attestation.Slot)).Uint64("inclusion_slot", uint64(attestation.InclusionSlot)).Msg("Non-canonical attestation; ignoring")
			continue
		}
		inclusionDelay := attestation.InclusionSlot - attestation.Slot
		for _, index := range attestation.AggregationIndices {
			attestationsIncluded[index] = true
			if *attestation.TargetCorrect {
				attestationsTargetCorrect[index] = true
			}
			if *attestation.HeadCorrect {
				attestationsHeadCorrect[index] = true
			}
			shortestDelay, exists := attestationsInclusionDelay[index]
			if !exists || inclusionDelay < shortestDelay {
				attestationsInclusionDelay[index] = inclusionDelay
			}
		}
	}

	// Add in any validators that did not attest.
	validators, err := s.chainDB.(chaindb.ValidatorsProvider).Validators(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for _, validator := range validators {
		// Confirm active.
		if validator.ActivationEpoch > epoch || validator.ExitEpoch <= epoch {
			continue
		}
		if _, exists := attestationsIncluded[validator.Index]; !exists {
			attestationsIncluded[validator.Index] = false
		}
	}
	return attestationsIncluded, attestationsTargetCorrect, attestationsHeadCorrect, attestationsInclusionDelay, nil
}
