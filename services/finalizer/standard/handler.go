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
	"context"
	"fmt"

	spec "github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
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

	if err := s.updateCanonicalBlocks(ctx, blockRoot); err != nil {
		log.Warn().Err(err).Msg("Failed to update canonical blocks")
	}
}

// updateCanonicalBlocks updates all canonical blocks given a canonical block root.
func (s *Service) updateCanonicalBlocks(ctx context.Context, root spec.Root) error {

	block, err := s.blocksProvider.BlockByRoot(ctx, root)
	if err != nil {
		if err == pgx.ErrNoRows {
			// If the block is not present in the database we cannot use it as a canonical
			// head, so nothing to do.
			return nil
		}
		return errors.Wrap(err, "failed to obtain block supplied by finalized checkpoint")
	}

	if err := s.canonicalizeBlocks(ctx, root); err != nil {
		return err
	}

	if err := s.noncanonicalizeBlocks(ctx, block.Slot); err != nil {
		return err
	}

	return nil
}

// canonicalizeBlocks marks the given block and all its parents as canonical.
func (s *Service) canonicalizeBlocks(ctx context.Context, root spec.Root) error {
	dbCtx, cancel, err := s.chainDB.BeginTx(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to begin transaction")
	}

	block, err := s.blocksProvider.BlockByRoot(dbCtx, root)
	if err != nil {
		cancel()
		return errors.Wrap(err, "failed to obtain canonical block")
	}

	if block.Canonical != nil && *block.Canonical {
		// Canonical chain fully linked; done.
		cancel()
		return nil
	}

	canonical := true
	block.Canonical = &canonical
	if err := s.blocksSetter.SetBlock(dbCtx, block); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set block to canonical")
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
	return s.canonicalizeBlocks(ctx, block.ParentRoot)
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
