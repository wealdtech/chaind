// Copyright © 2021, 2023 Weald Technology Limited.
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
	"net/http"
	"time"

	eth2client "github.com/attestantio/go-eth2-client"
	"github.com/attestantio/go-eth2-client/api"
	apiv1 "github.com/attestantio/go-eth2-client/api/v1"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
)

// OnFinalityCheckpointReceived receives finality checkpoint notifications.
func (s *Service) OnFinalityCheckpointReceived(
	ctx context.Context,
	finality *apiv1.Finality,
) {
	log := log.With().Uint64("finalized_epoch", uint64(finality.Finalized.Epoch)).Logger()
	log.Trace().
		Stringer("finalized_block_root", finality.Finalized.Root).
		Uint64("justified_epoch", uint64(finality.Justified.Epoch)).
		Stringer("justified_bock_root", finality.Justified.Root).
		Msg("Finality checkpoint received")

	// Only allow 1 handler to be active.
	acquired := s.activitySem.TryAcquire(1)
	if !acquired {
		// If this semaphore is held by the blocks handler it will be free very soon, so
		// wait for a bit and try again.
		time.Sleep(2 * time.Second)
		acquired = s.activitySem.TryAcquire(1)
		if !acquired {
			log.Debug().Msg("Another handler (either finalizer or blocks) running")
			return
		}
	}
	defer s.activitySem.Release(1)

	// We have been informed that epoch x has finalised.  At this point we can finalise
	// all blocks up to the justified root, and all attestations within them.

	// Rather than attempt to update everything from here back to what could be
	// the genesis block we break the process in to batches of ~1024 slots.  To do this,
	// pick checkpoints from here backwards and act on each one individually.
	stack, err := s.buildFinalityStack(ctx, finality.Justified.Root, finality.Justified.Epoch)
	if err != nil {
		log.Error().Err(err).Msg("Failed to build finality stack")
		return
	}

	for {
		if len(stack) == 0 {
			break
		}
		index := len(stack) - 1
		checkpoint := stack[index]
		stack = stack[:index]

		log.Trace().Uint64("update_epoch", uint64(checkpoint.Epoch)).Int("remaining", len(stack)).Msg("Updating to epoch")
		if err := s.runFinalityTransaction(ctx, checkpoint); err != nil {
			log.Error().Err(err).Msg("Failed to run finality transaction")
			return
		}
		monitorEpochProcessed(checkpoint.Epoch)
	}

	log.Trace().Msg("Finished handling finality checkpoint")

	// Notify that finality has been updated.
	for _, finalityHandler := range s.finalityHandlers {
		go finalityHandler.OnFinalityUpdated(ctx, finality.Finalized.Epoch)
	}
}

func (s *Service) buildFinalityStack(ctx context.Context,
	blockRoot phase0.Root,
	epoch phase0.Epoch,
) (
	[]*phase0.Checkpoint,
	error,
) {
	// Find the latest slot that we marked as canonical so that we do not repeat ourself.
	md, err := s.getMetadata(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to obtain finalizer metadata")
	}
	earliestNonCanonicalSlot := phase0.Slot(md.LatestCanonicalSlot + 1)

	// Build a list of checkpoints that allow us to break up a potentially large
	// set of database operations into smaller chunks of ~1024 slots each (first
	// chunk will be larger as we are double-counting its finality so usually another
	// 1024 slots).
	stack := make([]*phase0.Checkpoint, 0)
	slot := s.chainTime.FirstSlotOfEpoch(epoch)
	slotsPerChunk := phase0.Slot(1024)

	item := &phase0.Checkpoint{
		Epoch: epoch,
		Root:  blockRoot,
	}
	for {
		if epoch == 0 {
			// If we reach here we're done.
			break
		}

		// Store the existing justified checkpoint on the stack.
		stack = append(stack, item)

		// Move backwards (up to) one chunk's worth.
		if slot < slotsPerChunk {
			// Reached the beginning of the chain.
			break
		}
		slot -= slotsPerChunk
		if slot < earliestNonCanonicalSlot {
			// Reached the location we have already finalized.
			break
		}

		finalityResponse, err := s.eth2Client.(eth2client.FinalityProvider).Finality(ctx, &api.FinalityOpts{
			State: fmt.Sprintf("%d", slot),
		})
		if err != nil {
			return nil, errors.Wrap(err, "failed to obtain finality for state")
		}
		log.Trace().Uint64("slot", uint64(slot)).Uint64("justified_epoch", uint64(finalityResponse.Data.Justified.Epoch)).Msg("Obtained finality")
		item = finalityResponse.Data.Justified
	}

	return stack, nil
}

func (s *Service) runFinalityTransaction(
	ctx context.Context,
	checkpoint *phase0.Checkpoint,
) error {
	ctx, cancel, err := s.chainDB.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to start transaction on finality")
	}

	log.Trace().Uint64("epoch", uint64(checkpoint.Epoch)).Msg("Updating canonical blocks on finality")
	if err := s.updateCanonicalBlocks(ctx, checkpoint.Root); err != nil {
		cancel()
		return errors.Wrap(err, "Failed to update canonical blocks on finality")
	}

	if err := s.updateAttestations(ctx, checkpoint.Epoch); err != nil {
		// It is possible for a finalized block to arrive after block finalization has
		// completed, in which case we will receive an error here (because the block is
		// not marked as finalized).  As such we do not log this error as a problem; the
		// block and related attestations will be finalized again next time around.
		log.Debug().Err(err).Msg("Failed to update attestations on finality; will retry next finality update")
	}

	if err := s.chainDB.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "Failed to commit transaction on finality")
	}

	return nil
}

// updateCanonicalBlocks updates all canonical blocks given a canonical block root.
func (s *Service) updateCanonicalBlocks(ctx context.Context, root phase0.Root) error {
	md, err := s.getMetadata(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to obtain metadata on finality")
	}

	// Fetch the block from either the database or the chain.
	block, err := s.fetchBlock(ctx, root)
	if err != nil {
		return errors.Wrap(err, "failed to obtain block")
	}

	log.Trace().Uint64("slot", uint64(block.Slot)).Msg("Canonicalizing up to slot")

	if err := s.canonicalizeBlocks(ctx, root, phase0.Slot(md.LatestCanonicalSlot)); err != nil {
		return errors.Wrap(err, "failed to update canonical blocks from canonical root")
	}

	if err := s.updateIndeterminateBlocks(ctx, block.Slot); err != nil {
		return errors.Wrap(err, "failed to update indeterminate blocks from canonical root")
	}

	md.LatestCanonicalSlot = int64(block.Slot)
	if err := s.setMetadata(ctx, md); err != nil {
		return errors.Wrap(err, "failed to update metadata on finality")
	}

	return nil
}

// canonicalizeBlocks marks the given block and all its parents as canonical.
func (s *Service) canonicalizeBlocks(ctx context.Context, root phase0.Root, limit phase0.Slot) error {
	log.Trace().Str("root", fmt.Sprintf("%#x", root)).Uint64("limit", uint64(limit)).Msg("Canonicalizing blocks")

	for {
		block, err := s.fetchBlock(ctx, root)
		if err != nil {
			return err
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

// updateIndeterminateBlocks marks all indeterminate blocks before the given slot as canonical
// if they have a canonical child else as non-canonical.
func (s *Service) updateIndeterminateBlocks(ctx context.Context, slot phase0.Slot) error {
	nonCanonicalRoots, err := s.blocksProvider.IndeterminateBlocks(ctx, 0, slot)
	if err != nil {
		return errors.Wrap(err, "failed to obtain indeterminate blocks")
	}

	for _, nonCanonicalRoot := range nonCanonicalRoots {
		log.Trace().Str("root", fmt.Sprintf("%#x", nonCanonicalRoot)).Msg("Fetching indeterminate block")
		nonCanonicalBlock, err := s.blocksProvider.BlockByRoot(ctx, nonCanonicalRoot)
		if err != nil {
			return err
		}
		log.Trace().Str("root", fmt.Sprintf("%#x", nonCanonicalRoot)).Uint64("slot", uint64(nonCanonicalBlock.Slot)).Msg("Fetched indeterminate block")
		childrenBlocks, err := s.blocksProvider.BlocksByParentRoot(ctx, nonCanonicalRoot)
		if err != nil {
			return err
		}
		canonical := false
		for _, child := range childrenBlocks {
			if child.Canonical != nil && *child.Canonical {
				canonical = true
			}
		}
		nonCanonicalBlock.Canonical = &canonical
		if err := s.blocksSetter.SetBlock(ctx, nonCanonicalBlock); err != nil {
			return err
		}
		log.Trace().Str("root", fmt.Sprintf("%#x", nonCanonicalRoot)).Uint64("slot", uint64(nonCanonicalBlock.Slot)).Bool("canonical", *nonCanonicalBlock.Canonical).Msg("Marking block")
	}

	return nil
}

// updateAttestations updates attestations in the given epoch.
func (s *Service) updateAttestations(ctx context.Context, epoch phase0.Epoch) error {
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
		log.Trace().Msg("No blocks in database")
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

	firstEpoch := phase0.Epoch(md.LastFinalizedEpoch + 1)

	log.Trace().Uint64("first_epoch", uint64(firstEpoch)).Uint64("latest_epoch", uint64(epoch)).Msg("Epochs over which to update attestations")
	for curEpoch := firstEpoch; curEpoch <= epoch; curEpoch++ {
		if err := s.updateAttestationsInEpoch(ctx, curEpoch); err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to update attestations in epoch %d", epoch))
		}
		md.LastFinalizedEpoch = int64(curEpoch)

		if err := s.setMetadata(ctx, md); err != nil {
			return errors.Wrap(err, "failed to update metadata for epoch")
		}
	}

	return nil
}

// updateAttestationsInEpoch updates the attestations in the given epoch.
func (s *Service) updateAttestationsInEpoch(ctx context.Context, epoch phase0.Epoch) error {
	log := log.With().Uint64("epoch", uint64(epoch)).Logger()

	fromSlot := phase0.Slot(0)
	if epoch > 0 {
		fromSlot = s.chainTime.FirstSlotOfEpoch(epoch-1) + 1
	}
	toSlot := s.chainTime.FirstSlotOfEpoch(epoch)
	// Because the epoch is canonical, all of the slots in the epoch are canonical.
	log.Trace().Uint64("from_slot", uint64(fromSlot)).Uint64("to_slot", uint64(toSlot)).Msg("Updating attestations in slot range")
	attestations, err := s.chainDB.(chaindb.AttestationsProvider).AttestationsInSlotRange(ctx, fromSlot, toSlot+1)
	if err != nil {
		return errors.Wrap(err, "failed to obtain attestations for epoch")
	}

	// Keep track of block canonical state for slots to reduce lookups.
	blockCanonicals := make(map[phase0.Slot]bool)

	// Keep track of roots for epochs to reduce lookups.
	epochRoots := make(map[phase0.Epoch]phase0.Root)

	// Keep track of roots for heads to reduce lookups.
	headRoots := make(map[phase0.Slot]phase0.Root)

	updatedSlots := make(map[int]struct{})
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
		updatedSlots[int(attestation.InclusionSlot)] = struct{}{}
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
func (s *Service) updateCanonical(ctx context.Context, attestation *chaindb.Attestation, blockCanonicals map[phase0.Slot]bool) error {
	if canonical, exists := blockCanonicals[attestation.InclusionSlot]; exists {
		attestation.Canonical = &canonical
	} else {
		block, err := s.chainDB.(chaindb.BlocksProvider).BlockByRoot(ctx, attestation.InclusionBlockRoot)
		if err != nil {
			return errors.Wrap(err, "failed to obtain block")
		}
		if block == nil {
			return fmt.Errorf("no block %#x when updating canonical attestations", attestation.InclusionBlockRoot)
		}
		if block.Canonical == nil {
			return fmt.Errorf("found indeterminate block %#x at slot %d when updating canonical attestations", block.Root, block.Slot)
		}
		blockCanonicals[block.Slot] = *block.Canonical
		attestation.Canonical = block.Canonical
	}

	return nil
}

// updateAttestationTargetCorrect updates the attestation to confirm if its target vote is correct.
// An attestation has a correct target vote if it matches the root of the latest canonical block
// since the start of the target epoch.
func (s *Service) updateAttestationTargetCorrect(ctx context.Context, attestation *chaindb.Attestation, epochRoots map[phase0.Epoch]phase0.Root) error {
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
	headRoots map[phase0.Slot]phase0.Root,
) error {
	headCorrect := false
	if headRoot, exists := headRoots[attestation.Slot]; exists {
		// We know the root, simple check.
		headCorrect = bytes.Equal(attestation.BeaconBlockRoot[:], headRoot[:])
		attestation.HeadCorrect = &headCorrect

		return nil
	}

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

	attestation.HeadCorrect = &headCorrect

	return nil
}

// fetchBlock fetches the block from the database, and if not found attempts to fetch it from the chain.
func (s *Service) fetchBlock(ctx context.Context, root phase0.Root) (*chaindb.Block, error) {
	// Start with a simple fetch from the database.
	block, err := s.blocksProvider.BlockByRoot(ctx, root)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			// Real error.
			return nil, errors.Wrap(err, "failed to obtain block from provider")
		}
		// Not found in the database, try fetching it from the chain.
		log.Debug().Stringer("block_root", root).Msg("Failed to obtain block from provider; fetching from chain")
		signedBlockResponse, err := s.eth2Client.(eth2client.SignedBeaconBlockProvider).SignedBeaconBlock(ctx, &api.SignedBeaconBlockOpts{
			Block: root.String(),
		})
		if err != nil {
			var apiErr *api.Error
			if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
				// Possible that this is a missed slot, don't error.
				log.Debug().Msg("No beacon block obtained for slot")
				return nil, nil
			}

			return nil, errors.Wrap(err, "failed to obtain block from chain")
		}
		signedBlock := signedBlockResponse.Data

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
		earliestAllowableSlot, err := signedBlock.Slot()
		if err != nil {
			return nil, errors.Wrap(err, "failed to obtain slot of block")
		}
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
