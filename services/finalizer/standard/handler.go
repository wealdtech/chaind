// Copyright © 2021 Weald Technology Trading.
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
	"bytes"
	"context"
	"fmt"

	eth2client "github.com/attestantio/go-eth2-client"
	spec "github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
)

// OnFinalityCheckpointReceived receives finality checkpoint notifications.
func (s *Service) OnFinalityCheckpointReceived(
	ctx context.Context,
	epoch spec.Epoch,
	blockRoot spec.Root,
	stateRoot spec.Root,
) {
	log := log.With().Uint64("epoch", uint64(epoch)).Logger()
	log.Trace().
		Str("block_root", fmt.Sprintf("%#x", blockRoot)).
		Str("state_root", fmt.Sprintf("%#x", stateRoot)).
		Msg("Handler called")

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

	log.Trace().Msg("Updating canonical blocks on finality")
	md.LatestCanonicalSlot, err = s.updateCanonicalBlocks(ctx, blockRoot, md.LatestCanonicalSlot)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to update canonical blocks on finality")
	}
	if err := s.setMetadata(ctx, md); err != nil {
		cancel()
		log.Error().Err(err).Msg("Failed to update metadata on finality")
		return
	}

	log.Trace().Msg("Updating attestation votes on finality")
	if err := s.updateAttestations(ctx, epoch); err != nil {
		log.Warn().Err(err).Msg("Failed to update attestations on finality")
	}

	if err := s.chainDB.CommitTx(ctx); err != nil {
		cancel()
		log.Error().Err(err).Msg("Failed to commit transaction on finality")
		return
	}

	monitorEpochProcessed(epoch)
	log.Trace().Msg("Finished handling finality checkpoint")
}

// updateCanonicalBlocks updates all canonical blocks given a canonical block root.
func (s *Service) updateCanonicalBlocks(ctx context.Context, root spec.Root, limit spec.Slot) (spec.Slot, error) {
	// Fetch the block from either the database or the chain.
	block, err := s.fetchBlock(ctx, root)
	if err != nil {
		return 0, errors.Wrap(err, "failed to obtain block")
	}
	if block == nil {
		// No block to update; return without error.
		return limit, nil
	}

	if err := s.canonicalizeBlocks(ctx, root, limit); err != nil {
		return 0, errors.Wrap(err, "failed to update canonical blocks from canonical root")
	}

	if err := s.noncanonicalizeBlocks(ctx, block.Slot); err != nil {
		return 0, errors.Wrap(err, "failed to update non-canonical blocks from canonical root")
	}

	return block.Slot, nil
}

// canonicalizeBlocks marks the given block and all its parents as canonical.
func (s *Service) canonicalizeBlocks(ctx context.Context, root spec.Root, limit spec.Slot) error {
	log.Trace().Str("root", fmt.Sprintf("%#x", root)).Uint64("limit", uint64(limit)).Msg("Canonicalizing blocks")

	for {
		block, err := s.fetchBlock(ctx, root)
		if err != nil {
			return err
		}

		if block == nil {
			log.Error().Str("block_root", fmt.Sprintf("%#x", root)).Msg("Block not found for root")
			return errors.New("block not found for root")
		}

		if limit != 0 && block.Slot == limit {
			break
		}

		// Update if the current status is either indeterminate or non-canonical.
		if block.Canonical == nil || !*block.Canonical {
			canonical := true
			block.Canonical = &canonical
			if err := s.blocksSetter.SetBlock(ctx, block); err != nil {
				return errors.Wrap(err, "failed to set block to canonical")
			}
			log.Trace().Uint64("slot", uint64(block.Slot)).Str("root", fmt.Sprintf("%#x", block.Root)).Msg("Block is canonical")
		}

		if block.Slot == 0 {
			// Reached the genesis block; done.
			break
		}

		// Loop for parent.
		root = block.ParentRoot
	}

	return nil
}

// noncanonicalizeBlocks marks all indeterminate blocks before the given slot as non-canonical.
func (s *Service) noncanonicalizeBlocks(ctx context.Context, slot spec.Slot) error {
	nonCanonicalRoots, err := s.blocksProvider.IndeterminateBlocks(ctx, 0, slot)
	if err != nil {
		return errors.Wrap(err, "failed to obtain indeterminate blocks")
	}
	canonical := false
	for _, nonCanonicalRoot := range nonCanonicalRoots {
		nonCanonicalBlock, err := s.blocksProvider.BlockByRoot(ctx, nonCanonicalRoot)
		if err != nil {
			return err
		}
		nonCanonicalBlock.Canonical = &canonical
		if err := s.blocksSetter.SetBlock(ctx, nonCanonicalBlock); err != nil {
			return err
		}
		log.Trace().Uint64("slot", uint64(nonCanonicalBlock.Slot)).Str("root", fmt.Sprintf("%#x", nonCanonicalBlock.Root)).Msg("Block is non-canonical")
	}

	return nil
}

// updateAttestations updates attestations given a finalized epoch.
func (s *Service) updateAttestations(ctx context.Context, epoch spec.Epoch) error {
	md, err := s.getMetadata(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to obtain metadata")
	}

	// We need to ensure the finalizer is not running ahead of the blocks service.  To do so, we compare the slot of the block
	// we fetched with the highest known slot in the database.  If our block is higher than that already stored it means that
	// we are waiting on the blocks service, so bow out.
	latestBlocks, err := s.blocksProvider.LatestBlocks(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to obtain latest blocks")
	}
	if len(latestBlocks) == 0 {
		// No blocks yet; bow out but no error.
		return nil
	}
	earliestAllowableSlot := s.chainTime.FirstSlotOfEpoch(epoch)
	if earliestAllowableSlot < 1024 {
		earliestAllowableSlot = 0
	} else {
		earliestAllowableSlot -= 1024
	}
	if latestBlocks[0].Slot < earliestAllowableSlot {
		// Bow out, but no error.
		log.Trace().Msg("Not enough blocks in the database; not updating attestations")
		return nil
	}

	// Obtain all attestations since the last finalized epoch, and update them.

	// First epoch is last finalized epoch + 1, unless it's 0 because we don't know the
	// difference between actually 0 and undefined.
	firstEpoch := md.LastFinalizedEpoch
	if firstEpoch != 0 {
		firstEpoch++
	}

	for curEpoch := firstEpoch; curEpoch < epoch; curEpoch++ {
		if err := s.updateAttestationsForEpoch(ctx, curEpoch); err != nil {
			return errors.Wrap(err, "failed to update attestations for epoch")
		}
		md.LastFinalizedEpoch = curEpoch
		if err := s.setMetadata(ctx, md); err != nil {
			return errors.Wrap(err, "failed to update metadata for epoch")
		}
	}

	return nil
}

func (s *Service) updateAttestationsForEpoch(ctx context.Context, epoch spec.Epoch) error {
	// epoch is a finalized epoch, so fetch all attestations for the epoch.
	attestations, err := s.chainDB.(chaindb.AttestationsProvider).AttestationsInSlotRange(ctx, s.chainTime.FirstSlotOfEpoch(epoch), s.chainTime.FirstSlotOfEpoch(epoch+1))
	if err != nil {
		return errors.Wrap(err, "failed to obtain attestations for epoch")
	}

	// Keep track of block canonical state for slots to reduce lookups.
	blockCanonicals := make(map[spec.Slot]bool)

	// Keep track of roots for epochs to reduce lookups.
	epochRoots := make(map[spec.Epoch]spec.Root)

	// Keep track of roots for heads to reduce lookups.
	headRoots := make(map[spec.Slot]spec.Root)

	for _, attestation := range attestations {
		if err := s.updateCanonical(ctx, attestation, blockCanonicals); err != nil {
			return errors.Wrap(err, "failed to update canonical state")
		}
		if err := s.updateAttestationTargetCorrect(ctx, attestation, epochRoots); err != nil {
			return errors.Wrap(err, "failed to update attestation target vote state")
		}
		if err := s.updateAttestationHeadCorrect(ctx, attestation, headRoots); err != nil {
			return errors.Wrap(err, "failed to update attestation head vote state")
		}
		if err := s.chainDB.(chaindb.AttestationsSetter).SetAttestation(ctx, attestation); err != nil {
			return errors.Wrap(err, "failed to update attestation")
		}
		log.Trace().
			Uint64("inclusion_slot", uint64(attestation.InclusionSlot)).
			Uint64("inclusion_index", attestation.InclusionIndex).
			Bool("canonical", *attestation.Canonical).
			Bool("target_correct", *attestation.TargetCorrect).
			Bool("head_correct", *attestation.HeadCorrect).
			Msg("Updated attestation")
	}

	return nil
}

// updateCanonical updates the attestation to confirm if it is canonical.
// An attestation is canonical if it is in a canonical block.
func (s *Service) updateCanonical(ctx context.Context, attestation *chaindb.Attestation, blockCanonicals map[spec.Slot]bool) error {
	if canonical, exists := blockCanonicals[attestation.InclusionSlot]; exists {
		attestation.Canonical = &canonical
	} else {
		block, err := s.chainDB.(chaindb.BlocksProvider).BlockByRoot(ctx, attestation.InclusionBlockRoot)
		if err != nil {
			return errors.Wrap(err, "failed to obtain block")
		}
		if block == nil {
			return fmt.Errorf("failed to find block %#x when updating canonical attestations", attestation.InclusionBlockRoot)
		}
		if block.Canonical == nil {
			return fmt.Errorf("found indeterminate block %#x when updating canonical attestations", attestation.InclusionBlockRoot)
		}
		blockCanonicals[block.Slot] = *block.Canonical
		attestation.Canonical = block.Canonical
	}

	return nil
}

// updateAttestationTargetCorrect updates the attestation to confirm if its target vote is correct.
// An attestation has a correct target vote if it matches the root of the latest canonical block
// since the start of the target epoch.
func (s *Service) updateAttestationTargetCorrect(ctx context.Context, attestation *chaindb.Attestation, epochRoots map[spec.Epoch]spec.Root) error {
	targetCorrect := false
	if epochRoot, exists := epochRoots[attestation.TargetEpoch]; exists {
		targetCorrect = bytes.Equal(attestation.TargetRoot[:], epochRoot[:])
	} else {
		// Start with first slot of the target epoch.
		startSlot := s.chainTime.FirstSlotOfEpoch(attestation.TargetEpoch)

		// Work backwards until we find a canonical block.
		canonicalBlockFound := false
		for slot := startSlot; !canonicalBlockFound; slot-- {
			log.Trace().Uint64("slot", uint64(slot)).Msg("Fetching blocks at slot")
			blocks, err := s.chainDB.(chaindb.BlocksProvider).BlocksBySlot(ctx, slot)
			if err != nil {
				return errors.Wrap(err, "failed to obtain block")
			}
			for _, block := range blocks {
				if block.Canonical != nil && *block.Canonical {
					log.Trace().Uint64("target_epoch", uint64(attestation.TargetEpoch)).Uint64("slot", uint64(block.Slot)).Msg("Found canonical block")
					canonicalBlockFound = true
					epochRoots[attestation.TargetEpoch] = block.Root
					targetCorrect = bytes.Equal(attestation.TargetRoot[:], block.Root[:])
					break
				}
			}
			if slot == 0 {
				break
			}
		}
		if !canonicalBlockFound {
			return errors.New("failed to obtain canonical block")
		}
	}

	attestation.TargetCorrect = &targetCorrect

	return nil
}

// updateAttestationHeadCorrect updates the attestation to confirm if its head vote is correct.
// An attestation has a correct head vote if it matches the root of the last canonical block
// prior to the attestation slot.
func (s *Service) updateAttestationHeadCorrect(ctx context.Context,
	attestation *chaindb.Attestation,
	headRoots map[spec.Slot]spec.Root,
) error {
	headCorrect := false
	if headRoot, exists := headRoots[attestation.Slot]; exists {
		headCorrect = bytes.Equal(attestation.BeaconBlockRoot[:], headRoot[:])
	} else {
		log.Trace().Uint64("slot", uint64(attestation.Slot)).Msg("Checking attestation head vote")
		// Start with slot of the attestation.
		canonicalBlockFound := false
		for slot := attestation.Slot; !canonicalBlockFound; slot-- {
			log.Trace().Uint64("slot", uint64(slot)).Msg("Fetching blocks at slot")
			blocks, err := s.chainDB.(chaindb.BlocksProvider).BlocksBySlot(ctx, slot)
			if err != nil {
				return errors.Wrap(err, "failed to obtain block")
			}
			for _, block := range blocks {
				if block.Canonical != nil && *block.Canonical {
					log.Trace().Uint64("slot", uint64(block.Slot)).Msg("Found canonical block")
					canonicalBlockFound = true
					headRoots[attestation.Slot] = block.Root
					headCorrect = bytes.Equal(attestation.BeaconBlockRoot[:], block.Root[:])
					log.Trace().Str("attestation_root", fmt.Sprintf("%#x", attestation.BeaconBlockRoot)).Str("block_root", fmt.Sprintf("%#x", block.Root)).Msg("Found canonical block")
					break
				}
			}
			if slot == 0 {
				break
			}
		}
		if !canonicalBlockFound {
			return errors.New("failed to obtain canonical block, cannot update attestation")
		}
	}

	attestation.HeadCorrect = &headCorrect

	return nil
}

// fetchBlock fetches the block from the database, and if not found attempts to fetch it from the chain.
func (s *Service) fetchBlock(ctx context.Context, root spec.Root) (*chaindb.Block, error) {
	// Start with a simple fetch from the database.
	block, err := s.blocksProvider.BlockByRoot(ctx, root)
	if err != nil {
		if err != pgx.ErrNoRows {
			// Real error.
			return nil, errors.Wrap(err, "failed to obtain block from provider")
		}
		// Not found in the database, try fetching it from the chain.
		log.Debug().Str("block_root", fmt.Sprintf("%#x", root)).Msg("Failed to obtain block from provider; fetching from chain")
		signedBlock, err := s.eth2Client.(eth2client.SignedBeaconBlockProvider).SignedBeaconBlock(ctx, fmt.Sprintf("%#x", root))
		if err != nil {
			return nil, errors.Wrap(err, "failed to obtain block from chain")
		}

		// We need to ensure the finalizer is not running ahead of the blocks service.  To do so, we compare the slot of the block
		// we fetched with the highest known slot in the database.  If our block is higher than that already stored it means that
		// we are waiting on the blocks service, so bow out.
		latestBlocks, err := s.blocksProvider.LatestBlocks(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to obtain latest blocks")
		}
		if len(latestBlocks) == 0 {
			// No blocks yet; bow out but no error.
			return nil, nil
		}
		earliestAllowableSlot := signedBlock.Message.Slot
		if earliestAllowableSlot < 1024 {
			earliestAllowableSlot = 0
		} else {
			earliestAllowableSlot -= 1024
		}
		if latestBlocks[0].Slot < earliestAllowableSlot {
			// Bow out, but no error.
			log.Trace().Msg("Not enough blocks in the database; not finalizing")
			return nil, nil
		}

		if err := s.blocks.OnBlock(ctx, signedBlock); err != nil {
			return nil, errors.Wrap(err, "failed to store block")
		}

		// Re-fetch from the database.
		block, err = s.blocksProvider.BlockByRoot(ctx, root)
		if err != nil {
			return nil, errors.Wrap(err, "failed to obtain block from provider after fetching it from chain")
		}
	}
	return block, nil
}
