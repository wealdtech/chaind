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
	"github.com/wealdtech/chaind/services/chaindb"
)

// OnFinalityUpdated is called when finality has been updated in the database.
func (s *Service) OnFinalityUpdated(
	ctx context.Context,
	finalizedEpoch spec.Epoch,
) {
	log := log.With().Uint64("finalized_epoch", uint64(finalizedEpoch)).Logger()
	log.Trace().Msg("Handler called")

	// Only allow 1 handler to be active.
	acquired := s.activitySem.TryAcquire(1)
	if !acquired {
		log.Debug().Msg("Another handler running")
		return
	}
	defer s.activitySem.Release(1)

	ctx, cancel, err := s.chainDB.BeginTx(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to begin transaction on finality")
		return
	}

	md, err := s.getMetadata(ctx)
	if err != nil {
		cancel()
		log.Error().Err(err).Msg("Failed to obtain metadata on finality")
		return
	}

	// Update individual epochs.
	for epoch := md.LastValidatorEpoch; epoch < finalizedEpoch; epoch++ {
		if err := s.updateValidatorSummariesForEpoch(ctx, epoch); err != nil {
			cancel()
			log.Warn().Err(err).Msg("Failed to update validator summaries for epoch")
			return
		}

	}

	md.LastValidatorEpoch = finalizedEpoch
	if err := s.setMetadata(ctx, md); err != nil {
		cancel()
		log.Error().Err(err).Msg("Failed to set metadata on finality")
		return
	}

	if err := s.chainDB.CommitTx(ctx); err != nil {
		cancel()
		log.Error().Err(err).Msg("Failed to commit transaction on finality")
		return
	}

	monitorEpochProcessed(finalizedEpoch)
	log.Trace().Msg("Finished handling finality checkpoint")
}

// updateValidatorSummariesForEpoch updates the validator summaries for a given epoch.
func (s *Service) updateValidatorSummariesForEpoch(ctx context.Context, epoch spec.Epoch) error {
	log.Trace().Uint64("epoch", uint64(epoch)).Msg("Summarizing")

	// Fetch the proposer duties for the epoch.
	proposerDuties, err := s.proposerDutiesProvider.ProposerDutiesForSlotRange(ctx,
		s.chainTime.FirstSlotOfEpoch(epoch),
		s.chainTime.FirstSlotOfEpoch(epoch+1),
	)
	if err != nil {
		return err
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

	// Fetch the block presence for the epoch.
	presence, err := s.blocksProvider.CanonicalBlockPresenceForSlotRange(ctx,
		s.chainTime.FirstSlotOfEpoch(epoch),
		s.chainTime.FirstSlotOfEpoch(epoch+1),
	)
	if err != nil {
		return err
	}
	validatorProposals := make(map[spec.ValidatorIndex]int)
	for i, present := range presence {
		if proposerDuties[i].Slot == 0 {
			// Not a real proposer duty, as per above.
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

	// Fetch all attestations for the epoch.
	attestations, err := s.attestationsProvider.AttestationsForSlotRange(ctx,
		s.chainTime.FirstSlotOfEpoch(epoch),
		s.chainTime.FirstSlotOfEpoch(epoch+1),
	)
	if err != nil {
		return err
	}
	log.Trace().Int("attestations", len(attestations)).Uint64("epoch", uint64(epoch)).Msg("Fetched attestations")

	// Mark up attestations for each validator.
	attestationsIncluded := make(map[spec.ValidatorIndex]bool)
	attestationsTargetCorrect := make(map[spec.ValidatorIndex]bool)
	attestationsHeadCorrect := make(map[spec.ValidatorIndex]bool)
	attestationsInclusionDelay := make(map[spec.ValidatorIndex]spec.Slot)
	for _, attestation := range attestations {
		if attestation.Canonical == nil || !*attestation.Canonical {
			log.Trace().Msg("Non-canonical attestation; ignoring")
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
		return err
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

	// Store the data.
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
			return err
		}
	}

	return nil
}
