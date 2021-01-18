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

	log.Trace().Msg("Updating canonical blocks")
	if err := s.updateCanonicalBlocks(ctx, blockRoot, true /* incremental */); err != nil {
		log.Warn().Err(err).Msg("Failed to update canonical blocks")
	}

	log.Trace().Msg("Updating attestation votes")
	if err := s.updateAttestations(ctx, epoch); err != nil {
		log.Warn().Err(err).Msg("Failed to update attestations")
	}

	log.Trace().Msg("Finished handling finality checkpoint")
}

// updateCanonicalBlocks updates all canonical blocks given a canonical block root.
func (s *Service) updateCanonicalBlocks(ctx context.Context, root spec.Root, incremental bool) error {
	block, err := s.blocksProvider.BlockByRoot(ctx, root)
	if err != nil {
		if err == pgx.ErrNoRows {
			// If the block is not present in the database we cannot use it as a canonical
			// head, so nothing to do.
			return nil
		}
		return errors.Wrap(err, "failed to obtain block supplied by finalized checkpoint")
	}

	if err := s.canonicalizeBlocks(ctx, root, incremental); err != nil {
		return errors.Wrap(err, "failed to update canonical blocks from canonical root")
	}

	if err := s.noncanonicalizeBlocks(ctx, block.Slot); err != nil {
		return errors.Wrap(err, "failed to update non-canonical blocks from canonical root")
	}

	return nil
}

// canonicalizeBlocks marks the given block and all its parents as canonical.
func (s *Service) canonicalizeBlocks(ctx context.Context, root spec.Root, incremental bool) error {
	dbCtx, cancel, err := s.chainDB.BeginTx(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to begin transaction")
	}

	block, err := s.blocksProvider.BlockByRoot(dbCtx, root)
	if err != nil {
		if err == pgx.ErrNoRows {
			return fmt.Errorf("missing canonical block %#x; please restart chaind with --blocks.start-slot=0 and --finalizer.enable=false to catch up missing blocks, then restart again without to allow finalizer to resume", root)
		}

		cancel()
		return errors.Wrap(err, fmt.Sprintf("failed to obtain canonical block %#x", root))
	}

	if block.Canonical != nil && *block.Canonical && incremental {
		// Canonical chain fully linked; done.
		cancel()
		return nil
	}

	// Update if the current status is either indeterminate or non-canonical.
	if block.Canonical == nil || !*block.Canonical {
		canonical := true
		block.Canonical = &canonical
		if err := s.blocksSetter.SetBlock(dbCtx, block); err != nil {
			cancel()
			return errors.Wrap(err, "failed to set block to canonical")
		}
	}

	if err := s.chainDB.CommitTx(dbCtx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to commit transaction")
	}

	if block.Slot == 0 {
		// Reached the genesis block; done.
		return nil
	}

	// Recurse.
	return s.canonicalizeBlocks(ctx, block.ParentRoot, incremental)
}

// noncanonicalizeBlocks marks all indeterminate blocks before the given slot as non-canonical.
func (s *Service) noncanonicalizeBlocks(ctx context.Context, slot spec.Slot) error {
	ctx, cancel, err := s.chainDB.BeginTx(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to begin transaction")
	}

	nonCanonicalRoots, err := s.blocksProvider.IndeterminateBlocks(ctx, 0, slot)
	if err != nil {
		cancel()
		return errors.Wrap(err, "failed to obtain indeterminate blocks")
	}
	canonical := false
	for _, nonCanonicalRoot := range nonCanonicalRoots {
		nonCanonicalBlock, err := s.blocksProvider.BlockByRoot(ctx, nonCanonicalRoot)
		if err != nil {
			cancel()
			return err
		}
		nonCanonicalBlock.Canonical = &canonical
		if err := s.blocksSetter.SetBlock(ctx, nonCanonicalBlock); err != nil {
			cancel()
			return err
		}
	}

	if err := s.chainDB.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to commit transaction")
	}

	return nil
}

// updateAttestations updates attestations given a finalized epoch.
func (s *Service) updateAttestations(ctx context.Context, epoch spec.Epoch) error {

	md, err := s.getMetadata(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to obtain metadata")
	}

	// Obtain all attestations since the last finalized epoch, and update them.

	// First epoch is last finalized epoch + 1, unless it's 0 because we don't know the
	// difference between actually 0 and undefined.
	firstEpoch := md.LastFinalizedEpoch
	if firstEpoch != 0 {
		firstEpoch++
	}

	for curEpoch := firstEpoch; curEpoch < epoch; curEpoch++ {
		ctx, cancel, err := s.chainDB.BeginTx(ctx)
		if err != nil {
			cancel()
			return errors.Wrap(err, "failed to begin transaction")
		}
		if err := s.updateAttestationsForEpoch(ctx, curEpoch); err != nil {
			cancel()
			return errors.Wrap(err, "failed to update attestations for epoch")
		}
		md.LastFinalizedEpoch = curEpoch
		if err := s.setMetadata(ctx, md); err != nil {
			cancel()
			return errors.Wrap(err, "failed to update metadata for epoch")
		}
		if err := s.chainDB.CommitTx(ctx); err != nil {
			cancel()
			return errors.Wrap(err, "failed to commit transaction")
		}
	}

	return nil
}

func (s *Service) updateAttestationsForEpoch(ctx context.Context, epoch spec.Epoch) error {
	// epoch is a finalized epoch, so fetch all attestations for the epoch.
	attestations, err := s.chainDB.(chaindb.AttestationsProvider).AttestationsForSlotRange(ctx, s.chainTime.FirstSlotOfEpoch(epoch), s.chainTime.FirstSlotOfEpoch(epoch+1))
	if err != nil {
		return errors.Wrap(err, "failed to obtain attestations for epoch")
	}

	// Keep track of roots for epochs to reduce lookups.
	epochRoots := make(map[spec.Epoch]spec.Root)

	// Keep track of roots for heads to reduce lookups.
	headRoots := make(map[spec.Slot]spec.Root)

	for _, attestation := range attestations {
		if err := s.updateAttestationTargetCorrect(ctx, attestation, epochRoots); err != nil {
			return errors.Wrap(err, "failed to update attestation target vote state")
		}
		if err := s.updateAttestationHeadCorrect(ctx, attestation, headRoots); err != nil {
			return errors.Wrap(err, "failed to update attestation head vote state")
		}
		if err := s.chainDB.(chaindb.AttestationsSetter).SetAttestation(ctx, attestation); err != nil {
			return errors.Wrap(err, "failed to update attestation")
		}
		log.Trace().Uint64("inclusion_slot", uint64(attestation.InclusionSlot)).Uint64("inclusion_index", attestation.InclusionIndex).Msg("Updated attestation")
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
			return errors.New("failed to obtain canonical block, cannot update attestation")
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
