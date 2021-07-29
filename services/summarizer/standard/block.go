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
	"fmt"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
)

// updateBlockSummariesForEpoch updates the block summaries for a given epoch.
func (s *Service) updateBlockSummariesForEpoch(ctx context.Context,
	md *metadata,
	epoch phase0.Epoch,
) error {
	log := log.With().Uint64("epoch", uint64(epoch)).Logger()
	if !s.blockSummaries {
		log.Trace().Msg("Block epoch summaries not enabled")
		return nil
	}
	log.Trace().Msg("Summarizing blocks for finalized epoch")

	minSlot := s.chainTime.FirstSlotOfEpoch(epoch)
	maxSlot := s.chainTime.FirstSlotOfEpoch(epoch + 1)

	for slot := minSlot; slot < maxSlot; slot++ {
		if err := s.updateBlockSummaryForSlot(ctx, slot); err != nil {
			return errors.Wrap(err, "failed to create summary for block")
		}
	}

	ctx, cancel, err := s.chainDB.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction to set summarizer metadata for block")
	}
	md.LastBlockEpoch = epoch
	if err := s.setMetadata(ctx, md); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set summarizer metadata for block")
	}
	if err := s.chainDB.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set commit transaction to set summarizer metadata for block")
	}
	return nil
}

func (s *Service) updateBlockSummaryForSlot(ctx context.Context, slot phase0.Slot) error {
	summary := &chaindb.BlockSummary{
		Slot: slot,
	}

	err := s.attestationStatsForBlock(ctx, slot, summary)
	if err != nil {
		return errors.Wrap(err, "failed to calculate block attestation summary statistics for epoch")
	}

	if summary.VotesForBlock == 0 {
		// No votes implies no block.
		return nil
	}

	ctx, cancel, err := s.chainDB.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction to set epoch summary")
	}
	if err := s.chainDB.(chaindb.BlockSummariesSetter).SetBlockSummary(ctx, summary); err != nil {
		cancel()
		return err
	}
	if err := s.chainDB.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set commit transaction to set epoch summary")
	}

	return nil
}

func (s *Service) attestationStatsForBlock(ctx context.Context,
	slot phase0.Slot,
	summary *chaindb.BlockSummary,
) error {
	// Ensure the block is canonical.
	blocks, err := s.blocksProvider.BlocksForSlotRange(ctx, slot, slot+1)
	if err != nil {
		return errors.Wrap(err, "failed to obtain block")
	}
	if len(blocks) == 0 {
		return nil
	}
	var block *chaindb.Block
	for i := range blocks {
		if blocks[i].Canonical != nil && *blocks[i].Canonical {
			block = blocks[i]
			break
		}
	}
	if block == nil {
		// No canonical block found.
		return nil
	}
	log.Trace().Uint64("slot", uint64(slot)).Str("root", fmt.Sprintf("%#x", block.Root)).Msg("Fetching attestations for canonical block")

	attestations, err := s.attestationsProvider.AttestationsForBlock(ctx, block.Root)
	if err != nil {
		return errors.Wrap(err, "failed to obtain attestations")
	}

	seenAttestations := make(map[phase0.Root]bool)
	votesForBlock := make(map[phase0.ValidatorIndex]bool)
	for _, attestation := range attestations {
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
			summary.DuplicateAttestationsForBlock++
			continue
		}
		seenAttestations[specAttestationRoot] = true
		if attestation.Canonical == nil || !*attestation.Canonical {
			log.Debug().Uint64("slot", uint64(attestation.Slot)).Uint64("inclusion_slot", uint64(attestation.InclusionSlot)).Uint64("inclusion_index", attestation.InclusionIndex).Msg("Attestation is not canonical")
			continue
		}
		summary.AttestationsForBlock++
		for _, index := range attestation.AggregationIndices {
			votesForBlock[index] = true
		}
	}
	summary.VotesForBlock = len(votesForBlock)

	return nil
}
