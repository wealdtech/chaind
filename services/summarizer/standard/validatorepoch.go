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
	"time"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
)

// updateValidatorSummariesForEpoch updates the validator summaries for a given epoch.
func (s *Service) updateValidatorSummariesForEpoch(ctx context.Context,
	md *metadata,
	epoch phase0.Epoch,
) error {
	started := time.Now()

	log := log.With().Uint64("epoch", uint64(epoch)).Logger()
	if !s.validatorSummaries {
		log.Trace().Msg("Validator epoch summaries not enabled")
		return nil
	}
	log.Trace().Msg("Summarizing validator epoch")

	proposerDuties, validatorProposerDuties, err := s.validatorProposerDutiesForEpoch(ctx, epoch)
	if err != nil {
		return err
	}
	log.Trace().Dur("elapsed", time.Since(started)).Msg("Fetched proposer duties")

	validatorProposals, err := s.validatorProposalsForEpoch(ctx, epoch, proposerDuties, validatorProposerDuties)
	if err != nil {
		return err
	}
	log.Trace().Dur("elapsed", time.Since(started)).Msg("Fetched proposals")

	attestationsIncluded, attestationsTargetCorrect, attestationsHeadCorrect, attestationsInclusionDelay, attestationsSourceTimely, attestationsTargetTimely, attestationsHeadTimely, err := s.attestationsForEpoch(ctx, epoch)
	if err != nil {
		return err
	}
	log.Trace().Dur("elapsed", time.Since(started)).Msg("Fetched attestations")

	// Store the data.
	ctx, cancel, err := s.chainDB.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction to set validator epoch summary")
	}
	summaries := make([]*chaindb.ValidatorEpochSummary, 0, len(attestationsIncluded))
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
			if epoch >= s.chainTime.AltairInitialEpoch() {
				if attestationSourceTimely, exists := attestationsSourceTimely[index]; exists {
					summary.AttestationSourceTimely = &attestationSourceTimely
				}
				if attestationTargetTimely, exists := attestationsTargetTimely[index]; exists {
					summary.AttestationTargetTimely = &attestationTargetTimely
				}
				if attestationHeadTimely, exists := attestationsHeadTimely[index]; exists {
					summary.AttestationHeadTimely = &attestationHeadTimely
				}
			}
		}
		summaries = append(summaries, summary)
	}

	if err := s.chainDB.(chaindb.ValidatorEpochSummariesSetter).SetValidatorEpochSummaries(ctx, summaries); err != nil {
		cancel()
		return err
	}

	log.Trace().Dur("elapsed", time.Since(started)).Msg("Set summary")
	md.LastValidatorEpoch = epoch
	if err := s.setMetadata(ctx, md); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set summarizer metadata for validator epoch summary")
	}
	if err := s.chainDB.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set commit transaction to set validator epoch summary")
	}

	return nil
}

func (s *Service) validatorProposerDutiesForEpoch(ctx context.Context,
	epoch phase0.Epoch,
) (
	[]*chaindb.ProposerDuty,
	map[phase0.ValidatorIndex]int,
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
		// Epoch 0 only has 31 proposer duties.  Drop in a dummy for slot 0 to avoid special cases below.
		tmp := make([]*chaindb.ProposerDuty, 32)
		tmp[0] = &chaindb.ProposerDuty{
			ValidatorIndex: 0xffffffffffffffff,
		}
		copy(tmp[1:], proposerDuties)
		proposerDuties = tmp
	}

	// Summarise the proposer duties information by validator.
	validatorProposerDuties := make(map[phase0.ValidatorIndex]int)
	for _, proposerDuty := range proposerDuties {
		if _, exists := validatorProposerDuties[proposerDuty.ValidatorIndex]; !exists {
			validatorProposerDuties[proposerDuty.ValidatorIndex] = 0
		}
		validatorProposerDuties[proposerDuty.ValidatorIndex]++
	}

	return proposerDuties, validatorProposerDuties, nil
}

func (s *Service) validatorProposalsForEpoch(ctx context.Context,
	epoch phase0.Epoch,
	proposerDuties []*chaindb.ProposerDuty,
	validatorProposerDuties map[phase0.ValidatorIndex]int,
) (
	map[phase0.ValidatorIndex]int,
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
	validatorProposals := make(map[phase0.ValidatorIndex]int)
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
	epoch phase0.Epoch,
) (
	map[phase0.ValidatorIndex]bool,
	map[phase0.ValidatorIndex]bool,
	map[phase0.ValidatorIndex]bool,
	map[phase0.ValidatorIndex]phase0.Slot,
	map[phase0.ValidatorIndex]bool,
	map[phase0.ValidatorIndex]bool,
	map[phase0.ValidatorIndex]bool,
	error,
) {
	// Fetch all attestations for the epoch.
	attestations, err := s.attestationsProvider.AttestationsForSlotRange(ctx,
		s.chainTime.FirstSlotOfEpoch(epoch),
		s.chainTime.FirstSlotOfEpoch(epoch+1),
	)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	log.Trace().Int("attestations", len(attestations)).Uint64("epoch", uint64(epoch)).Msg("Fetched attestations")

	// Mark up attestations for each validator.
	attestationsIncluded := make(map[phase0.ValidatorIndex]bool)
	attestationsTargetCorrect := make(map[phase0.ValidatorIndex]bool)
	attestationsHeadCorrect := make(map[phase0.ValidatorIndex]bool)
	attestationsInclusionDelay := make(map[phase0.ValidatorIndex]phase0.Slot)
	attestationsSourceTimely := make(map[phase0.ValidatorIndex]bool)
	attestationsTargetTimely := make(map[phase0.ValidatorIndex]bool)
	attestationsHeadTimely := make(map[phase0.ValidatorIndex]bool)
	for _, attestation := range attestations {
		if attestation.Canonical == nil || !*attestation.Canonical {
			log.Trace().Uint64("slot", uint64(attestation.Slot)).Uint64("inclusion_slot", uint64(attestation.InclusionSlot)).Msg("Non-canonical attestation; ignoring")
			continue
		}
		inclusionDelay := attestation.InclusionSlot - attestation.Slot
		attestationSourceTimely := uint64(inclusionDelay) <= s.maxTimelyAttestationSourceDelay
		attestationTargetTimely := false
		attestationHeadTimely := false
		for _, index := range attestation.AggregationIndices {
			attestationsIncluded[index] = true
			if *attestation.TargetCorrect {
				attestationsTargetCorrect[index] = true
				attestationTargetTimely = uint64(inclusionDelay) <= s.maxTimelyAttestationTargetDelay
			}
			if *attestation.HeadCorrect {
				attestationsHeadCorrect[index] = true
				attestationHeadTimely = uint64(inclusionDelay) <= s.maxTimelyAttestationHeadDelay
			}
			shortestDelay, exists := attestationsInclusionDelay[index]
			if !exists || inclusionDelay < shortestDelay {
				attestationsInclusionDelay[index] = inclusionDelay
				attestationsSourceTimely[index] = attestationSourceTimely
				attestationsTargetTimely[index] = attestationTargetTimely
				attestationsHeadTimely[index] = attestationHeadTimely
			}
		}
	}

	// Add in any validators that did not attest.
	validators, err := s.chainDB.(chaindb.ValidatorsProvider).Validators(ctx)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
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
	return attestationsIncluded, attestationsTargetCorrect, attestationsHeadCorrect, attestationsInclusionDelay, attestationsSourceTimely, attestationsTargetTimely, attestationsHeadTimely, nil
}
