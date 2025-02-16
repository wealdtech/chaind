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

	"github.com/attestantio/go-eth2-client/spec/electra"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/prysmaticlabs/go-bitfield"
	"github.com/wealdtech/chaind/services/chaindb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// summarizeBlocksInEpoch summarizes all blocks in the given epoch.
func (s *Service) summarizeBlocksInEpoch(ctx context.Context,
	md *metadata,
	epoch phase0.Epoch,
) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.summarizer.standard").Start(ctx, "summarizeBlocksInEpoch",
		trace.WithAttributes(
			attribute.Int64("epoch", int64(epoch)),
		))
	defer span.End()

	log := log.With().Uint64("epoch", uint64(epoch)).Logger()
	if !s.blockSummaries {
		log.Trace().Msg("Block epoch summaries not enabled")
		return nil
	}

	minSlot := s.chainTime.FirstSlotOfEpoch(epoch)
	maxSlot := s.chainTime.LastSlotOfEpoch(epoch)
	log.Trace().Uint64("min_slot", uint64(minSlot)).Uint64("max_slot", uint64(maxSlot)).Msg("Summarizing blocks for epoch")

	for slot := minSlot; slot <= maxSlot; slot++ {
		if err := s.summarizeBlock(ctx, slot); err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to create summary for block %d", slot))
		}
	}
	md.LastBlockEpoch = epoch

	ctx, cancel, err := s.chainDB.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction to set summarizer metadata for block")
	}
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

// summarizeBlock summarizes the block at the given slot.
func (s *Service) summarizeBlock(ctx context.Context, slot phase0.Slot) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.summarizer.standard").Start(ctx, "summarizeBlock",
		trace.WithAttributes(
			attribute.Int64("slot", int64(slot)),
		))
	defer span.End()

	summary := &chaindb.BlockSummary{
		Slot: slot,
	}

	blocks, err := s.blocksProvider.BlocksBySlot(ctx, slot)
	if err != nil {
		return errors.Wrap(err, "failed to obtain blocks for slot")
	}
	if len(blocks) == 0 {
		// No block for this slot.
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
		// No canonical block for this slot.
		return nil
	}
	log.Trace().Uint64("slot", uint64(slot)).Msg("Summarising block")

	if err := s.attestationStatsForBlock(ctx, slot, summary, block); err != nil {
		return errors.Wrap(err, "failed to calculate block attestation summary statistics for epoch")
	}

	if err := s.parentDistanceForBlock(ctx, slot, summary, block); err != nil {
		return errors.Wrap(err, "failed to calculate parent distance summary statistics for epoch")
	}

	ctx, cancel, err := s.chainDB.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction to set block summary")
	}
	if err := s.chainDB.(chaindb.BlockSummariesSetter).SetBlockSummary(ctx, summary); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set block summary")
	}
	if err := s.chainDB.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set commit transaction to set block summary")
	}

	return nil
}

func (s *Service) attestationStatsForBlock(ctx context.Context,
	slot phase0.Slot,
	summary *chaindb.BlockSummary,
	block *chaindb.Block,
) error {
	log.Trace().Uint64("slot", uint64(slot)).Str("root", fmt.Sprintf("%#x", block.Root)).Msg("Fetching attestations for canonical block")

	attestations, err := s.attestationsProvider.AttestationsForBlock(ctx, block.Root)
	if err != nil {
		return errors.Wrap(err, "failed to obtain attestations")
	}

	seenAttestations := make(map[phase0.Root]bool)
	votesForBlock := make(map[phase0.ValidatorIndex]bool)
	for _, attestation := range attestations {
		// It's possible for the attestation to be indeterminate here, because a new (non-finalised) attestation can vote (incorrectly) for
		// an old block as the head of the chain.  We consider these as valid as far as the block statistics go, so do not reject them at this point.
		if attestation.Canonical != nil && !*attestation.Canonical {
			// This commonly happens when the block in which the attestation is included is non-canonical, so note it but no more.
			log.Trace().Uint64("inclusion_slot", uint64(attestation.InclusionSlot)).Uint64("inclusion_index", attestation.InclusionIndex).Msg("Attestation is not canonical; ignoring")
			continue
		}
		committeeBits := bitfield.NewBitvector64()
		for _, committeeIndex := range attestation.CommitteeIndices {
			committeeBits.SetBitAt(uint64(committeeIndex), true)
		}
		specAttestation := &electra.Attestation{
			AggregationBits: attestation.AggregationBits,
			Data: &phase0.AttestationData{
				Slot:            attestation.Slot,
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
			CommitteeBits: committeeBits,
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
		summary.AttestationsForBlock++
		for _, index := range attestation.AggregationIndices {
			votesForBlock[index] = true
		}
	}
	summary.VotesForBlock = len(votesForBlock)

	return nil
}

func (s *Service) parentDistanceForBlock(ctx context.Context,
	slot phase0.Slot,
	summary *chaindb.BlockSummary,
	block *chaindb.Block,
) error {
	if slot == 0 {
		return nil
	}

	log.Trace().Uint64("slot", uint64(slot)).Str("root", fmt.Sprintf("%#x", block.Root)).Msg("Fetching parent for canonical block")

	parentBlock, err := s.blocksProvider.BlockByRoot(ctx, block.ParentRoot)
	if err != nil {
		return errors.Wrap(err, "failed to obtain parent block")
	}

	summary.ParentDistance = int(slot - parentBlock.Slot)

	return nil
}
