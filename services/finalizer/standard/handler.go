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
	"bytes"
	"context"
	"fmt"
	"sort"

	eth2client "github.com/attestantio/go-eth2-client"
	"github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
)

// OnFinalityCheckpointReceived receives finality checkpoint notifications.
func (s *Service) OnFinalityCheckpointReceived(
	ctx context.Context,
	epoch phase0.Epoch,
	blockRoot phase0.Root,
	stateRoot phase0.Root,
) {
	log := log.With().Uint64("epoch", uint64(epoch)).Logger()
	log.Trace().
		Str("block_root", fmt.Sprintf("%#x", blockRoot)).
		Str("state_root", fmt.Sprintf("%#x", stateRoot)).
		Msg("Handler called")
	log.Info().Msg("Finality checkpoint received")

	// Only allow 1 handler to be active.
	acquired := s.activitySem.TryAcquire(1)
	if !acquired {
		log.Debug().Msg("Another handler running")
		return
	}
	defer s.activitySem.Release(1)

	// Receiving epoch x means that slots up to (x*32) have been finalized
	// one way or the other (canonical or non-canonical), so attempt to update
	// all blocks from this slot backwards as either canonical or not.

	// Rather than attempt to update everything from here back to what could be
	// the genesis block we break the process in to batches of ~256 slots.  To do this,
	// pick checkpoints from here backwards and act on each one individually.
	rootStack, epochStack, err := s.buildFinalityStack(ctx, blockRoot, epoch)
	if err != nil {
		log.Error().Err(err).Msg("Failed to build finality stack")
	}

	for {
		if len(rootStack) == 0 {
			break
		}
		rootIndex := len(rootStack) - 1
		root := rootStack[rootIndex]
		rootStack = rootStack[:rootIndex]

		epochIndex := len(epochStack) - 1
		epoch := epochStack[epochIndex]
		epochStack = epochStack[:epochIndex]

		log.Trace().Uint64("update_epoch", uint64(epoch)).Int("remaining", len(epochStack)).Msg("Running finality for epoch")
		if err := s.runFinalityTransaction(ctx, root, epoch); err != nil {
			log.Error().Err(err).Msg("Failed to run finality transaction")
			break
		}

	}

	monitorEpochProcessed(epoch)
	log.Trace().Msg("Finished handling finality checkpoint")

	// Notify that finality has been updated.
	for _, finalityHandler := range s.finalityHandlers {
		go finalityHandler.OnFinalityUpdated(ctx, epoch)
	}
}

func (s *Service) buildFinalityStack(ctx context.Context,
	blockRoot phase0.Root,
	epoch phase0.Epoch,
) (
	[]phase0.Root,
	[]phase0.Epoch,
	error,
) {
	// Find the latest slot that we marked as canonical so that we do not repeat ourself.
	md, err := s.getMetadata(ctx)
	if err != nil {
		return []phase0.Root{}, []phase0.Epoch{}, errors.Wrap(err, "failed to obtain finalizer metadata")
	}
	lastestKnownCanonicalSlot := md.LatestCanonicalSlot

	// Build a list of checkpoints that allow us to break up a potentially large
	// set of database operations into smaller chunks of ~256 slots each (first
	// chunk will be larger as we are double-counting its finality so usually another
	// 64 slots).
	rootStack := []phase0.Root{}
	epochStack := []phase0.Epoch{}
	slot := s.chainTime.FirstSlotOfEpoch(epoch)
	slotsPerChunk := phase0.Slot(256)

	rootStackItem := blockRoot
	epochStackItem := epoch
	for {
		rootStack = append(rootStack, rootStackItem)
		epochStack = append(epochStack, epochStackItem)

		// Move backwards (up to) one chunk's worth.
		if slot < slotsPerChunk {
			// Reached the beginning of the chain.
			break
		}
		slot -= slotsPerChunk
		if slot < lastestKnownCanonicalSlot {
			// Reached the location we have already finalized.
			break
		}

		finality, err := s.eth2Client.(eth2client.FinalityProvider).Finality(ctx, fmt.Sprintf("%d", slot))
		if err != nil {
			return []phase0.Root{}, []phase0.Epoch{}, errors.Wrap(err, "failed to obtain finality for state")
		}
		log.Trace().Uint64("slot", uint64(slot)).Uint64("finalized_epoch", uint64(finality.Finalized.Epoch)).Msg("Obtained finality")
		rootStackItem = finality.Finalized.Root
		epochStackItem = finality.Finalized.Epoch
	}

	if e := log.Trace(); e.Enabled() {
		roots := make([]string, len(rootStack))
		epochs := make([]uint64, len(epochStack))
		for i := range rootStack {
			roots[i] = fmt.Sprintf("%#x", rootStack[i])
		}
		for i := range epochStack {
			epochs[i] = uint64(epochStack[i])
		}
		e.Uints64("epoch_stack", epochs).Strs("root_stack", roots).Msg("Built commit stack")
	}

	return rootStack, epochStack, nil
}

func (s *Service) runFinalityTransaction(
	ctx context.Context,
	root phase0.Root,
	epoch phase0.Epoch,
) error {
	ctx, cancel, err := s.chainDB.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to start transaction on finality")
	}

	log.Trace().Uint64("epoch", uint64(epoch)).Msg("Updating canonical blocks on finality")
	if err := s.updateCanonicalBlocks(ctx, root); err != nil {
		return errors.Wrap(err, "Failed to update canonical blocks on finality")
	}

	if err := s.updateAttestations(ctx, epoch); err != nil {
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
	if block == nil {
		return errors.New("missing canonical block")
	}
	log.Trace().Uint64("slot", uint64(block.Slot)).Msg("Canonicalizing up to slot")

	if err := s.canonicalizeBlocks(ctx, root, md.LatestCanonicalSlot); err != nil {
		return errors.Wrap(err, "failed to update canonical blocks from canonical root")
	}

	if err := s.updateIndeterminateBlocks(ctx, block.Slot); err != nil {
		return errors.Wrap(err, "failed to update indeterminate blocks from canonical root")
	}

	md.LatestCanonicalSlot = block.Slot
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
			log.Info().Uint64("slot", uint64(block.Slot)).Bool("canonical", canonical).Msg("Set block canonical state (1)")
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
		log.Info().Uint64("slot", uint64(nonCanonicalBlock.Slot)).Bool("canonical", canonical).Msg("Set block canonical state (2)")
		log.Trace().Str("root", fmt.Sprintf("%#x", nonCanonicalRoot)).Uint64("slot", uint64(nonCanonicalBlock.Slot)).Bool("canonical", *nonCanonicalBlock.Canonical).Msg("Marking block")
	}

	return nil
}

// updateAttestations updates attestations for the given epoch.
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

	// First epoch is last finalized epoch + 1, unless it's 0 because we don't know the
	// difference between actually 0 and undefined.
	firstEpoch := md.LastFinalizedEpoch
	if firstEpoch != 0 {
		firstEpoch++
	}

	log.Trace().Uint64("first_epoch", uint64(firstEpoch)).Uint64("latest_epoch", uint64(epoch)).Msg("Epochs over which to update attestations")
	for curEpoch := firstEpoch; curEpoch <= epoch; curEpoch++ {
		if err := s.updateAttestationsForEpoch(ctx, curEpoch); err != nil {
			return errors.Wrap(err, "failed to update attestations in epoch")
		}
		md.LastFinalizedEpoch = curEpoch

		if err := s.setMetadata(ctx, md); err != nil {
			return errors.Wrap(err, "failed to update metadata for epoch")
		}
	}

	return nil
}

// updateAttestationsForEpoch updates the attestations for the given epoch.
func (s *Service) updateAttestationsForEpoch(ctx context.Context, epoch phase0.Epoch) error {
	log := log.With().Uint64("epoch", uint64(epoch)).Logger()
	log.Trace().Msg("Updating attestation finality for epoch")

	// Because the epoch is canonical, all of the slots in the epoch are canonical.
	log.Info().Uint64("from_slot", uint64(s.chainTime.FirstSlotOfEpoch(epoch-1)+1)).Uint64("to_slot", uint64(s.chainTime.FirstSlotOfEpoch(epoch))).Msg("Updating attestations in slot range")
	attestations, err := s.chainDB.(chaindb.AttestationsProvider).AttestationsInSlotRange(ctx, s.chainTime.FirstSlotOfEpoch(epoch-1)+1, s.chainTime.FirstSlotOfEpoch(epoch)+1)
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
	// TODO remove.
	{
		slots := make([]int, 0)
		for slot := range updatedSlots {
			slots = append(slots, slot)
		}
		sort.Ints(slots)
		log.Info().Ints("updated_slots", slots).Msg("Updated attestations for slots")
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
func (s *Service) fetchBlock(ctx context.Context, root phase0.Root) (*chaindb.Block, error) {
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
		if signedBlock == nil {
			return nil, nil
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
		var earliestAllowableSlot phase0.Slot
		switch signedBlock.Version {
		case spec.DataVersionPhase0:
			earliestAllowableSlot = signedBlock.Phase0.Message.Slot
		case spec.DataVersionAltair:
			earliestAllowableSlot = signedBlock.Altair.Message.Slot
		case spec.DataVersionBellatrix:
			earliestAllowableSlot = signedBlock.Bellatrix.Message.Slot
		default:
			return nil, errors.New("unknown block version")
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
