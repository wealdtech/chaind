// Copyright © 2020 - 2022 Weald Technology Trading.
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

package postgresql

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
	"go.opentelemetry.io/otel"
)

// SetBlock sets a block.
func (s *Service) SetBlock(ctx context.Context, block *chaindb.Block) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SetBlock")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	var canonical sql.NullBool
	if block.Canonical != nil {
		canonical.Valid = true
		canonical.Bool = *block.Canonical
	}
	if _, err := tx.Exec(ctx, `
      INSERT INTO t_blocks(f_slot
                          ,f_proposer_index
                          ,f_root
                          ,f_graffiti
                          ,f_randao_reveal
                          ,f_body_root
                          ,f_parent_root
                          ,f_state_root
                          ,f_canonical
                          ,f_eth1_block_hash
                          ,f_eth1_deposit_count
                          ,f_eth1_deposit_root
						  )
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
      ON CONFLICT (f_root) DO
      UPDATE
      SET f_slot = excluded.f_slot
         ,f_proposer_index = excluded.f_proposer_index
         ,f_graffiti = excluded.f_graffiti
         ,f_randao_reveal = excluded.f_randao_reveal
         ,f_body_root = excluded.f_body_root
         ,f_parent_root = excluded.f_parent_root
         ,f_state_root = excluded.f_state_root
         ,f_canonical = excluded.f_canonical
         ,f_eth1_block_hash = excluded.f_eth1_block_hash
         ,f_eth1_deposit_count = excluded.f_eth1_deposit_count
         ,f_eth1_deposit_root = excluded.f_eth1_deposit_root
	  `,
		block.Slot,
		block.ProposerIndex,
		block.Root[:],
		block.Graffiti,
		block.RANDAOReveal[:],
		block.BodyRoot[:],
		block.ParentRoot[:],
		block.StateRoot[:],
		canonical,
		block.ETH1BlockHash,
		block.ETH1DepositCount,
		block.ETH1DepositRoot[:],
	); err != nil {
		return err
	}

	// Set execution payload (will return without error if payload is not present).
	if err := s.setExecutionPayload(ctx, block); err != nil {
		return errors.Wrap(err, "failed to set execution payload")
	}

	// Set BLS to execution changes (will return without error if changes are not present).
	if err := s.setBLSToExecutionChanges(ctx, block); err != nil {
		return errors.Wrap(err, "failed to set BLS to execution changes")
	}

	return nil
}

// Blocks provides withdrawals according to the filter.
func (s *Service) Blocks(ctx context.Context, filter *chaindb.BlockFilter) ([]*chaindb.Block, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "Blocks")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		ctx, err := s.BeginROTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		defer s.CommitROTx(ctx)
		tx = s.tx(ctx)
	}

	// Build the query.
	queryBuilder := strings.Builder{}
	queryVals := make([]any, 0)

	queryBuilder.WriteString(`
SELECT f_slot
      ,f_proposer_index
      ,f_root
      ,f_graffiti
      ,f_randao_reveal
      ,f_body_root
      ,f_parent_root
      ,f_state_root
      ,f_canonical
      ,f_eth1_block_hash
      ,f_eth1_deposit_count
      ,f_eth1_deposit_root
FROM t_blocks`)

	wherestr := "WHERE"

	if filter.From != nil {
		queryVals = append(queryVals, *filter.From)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_slot >= $%d`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if filter.To != nil {
		queryVals = append(queryVals, *filter.To)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_slot <= $%d`, wherestr, len(queryVals)))
	}

	if filter.Canonical != nil {
		queryVals = append(queryVals, *filter.Canonical)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_canonical = $%d`, wherestr, len(queryVals)))
	}

	switch filter.Order {
	case chaindb.OrderEarliest:
		queryBuilder.WriteString(`
ORDER BY f_slot, f_root`)
	case chaindb.OrderLatest:
		queryBuilder.WriteString(`
ORDER BY f_slot DESC,f_root DESC`)
	default:
		return nil, errors.New("no order specified")
	}

	if filter.Limit > 0 {
		queryVals = append(queryVals, filter.Limit)
		queryBuilder.WriteString(fmt.Sprintf(`
LIMIT $%d`, len(queryVals)))
	}

	if e := log.Trace(); e.Enabled() {
		params := make([]string, len(queryVals))
		for i := range queryVals {
			params[i] = fmt.Sprintf("%v", queryVals[i])
		}
		e.Str("query", strings.ReplaceAll(queryBuilder.String(), "\n", " ")).Strs("params", params).Msg("SQL query")
	}

	rows, err := tx.Query(ctx,
		queryBuilder.String(),
		queryVals...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	blocks := make([]*chaindb.Block, 0)
	for rows.Next() {
		block := &chaindb.Block{}
		var blockRoot []byte
		var randaoReveal []byte
		var bodyRoot []byte
		var parentRoot []byte
		var stateRoot []byte
		var canonical sql.NullBool
		var eth1DepositRoot []byte
		err := rows.Scan(
			&block.Slot,
			&block.ProposerIndex,
			&blockRoot,
			&block.Graffiti,
			&randaoReveal,
			&bodyRoot,
			&parentRoot,
			&stateRoot,
			&canonical,
			&block.ETH1BlockHash,
			&block.ETH1DepositCount,
			&eth1DepositRoot,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		copy(block.Root[:], blockRoot)
		copy(block.RANDAOReveal[:], randaoReveal)
		copy(block.BodyRoot[:], bodyRoot)
		copy(block.ParentRoot[:], parentRoot)
		copy(block.StateRoot[:], stateRoot)
		if canonical.Valid {
			val := canonical.Bool
			block.Canonical = &val
		}
		copy(block.ETH1DepositRoot[:], eth1DepositRoot)
		blocks = append(blocks, block)
	}

	// Add execution payload to blocks.
	roots := make([]phase0.Root, len(blocks))
	for i := range blocks {
		roots[i] = blocks[i].Root
	}
	payloads, err := s.executionPayloads(ctx, tx, roots)
	if err != nil {
		return nil, err
	}
	for _, block := range blocks {
		if payload, exists := payloads[block.Root]; exists {
			block.ExecutionPayload = payload
		}
	}

	// Always return order of slot then root.
	sort.Slice(blocks, func(i int, j int) bool {
		if blocks[i].Slot != blocks[j].Slot {
			return blocks[i].Slot < blocks[j].Slot
		}
		return bytes.Compare(blocks[i].Root[:], blocks[j].Root[:]) < 0
	})

	return blocks, nil
}

// BlocksBySlot fetches all blocks with the given slot.
func (s *Service) BlocksBySlot(ctx context.Context, slot phase0.Slot) ([]*chaindb.Block, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "BlocksBySlot")
	defer span.End()

	var err error

	tx := s.tx(ctx)
	if tx == nil {
		ctx, err = s.BeginROTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer s.CommitROTx(ctx)
	}

	rows, err := tx.Query(ctx, `
      SELECT f_slot
            ,f_proposer_index
            ,f_root
            ,f_graffiti
            ,f_randao_reveal
            ,f_body_root
            ,f_parent_root
            ,f_state_root
            ,f_canonical
            ,f_eth1_block_hash
            ,f_eth1_deposit_count
            ,f_eth1_deposit_root
      FROM t_blocks
      WHERE f_slot = $1`,
		slot,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	blocks := make([]*chaindb.Block, 0)

	for rows.Next() {
		block := &chaindb.Block{}
		var blockRoot []byte
		var randaoReveal []byte
		var bodyRoot []byte
		var parentRoot []byte
		var stateRoot []byte
		var canonical sql.NullBool
		var eth1DepositRoot []byte
		err := rows.Scan(
			&block.Slot,
			&block.ProposerIndex,
			&blockRoot,
			&block.Graffiti,
			&randaoReveal,
			&bodyRoot,
			&parentRoot,
			&stateRoot,
			&canonical,
			&block.ETH1BlockHash,
			&block.ETH1DepositCount,
			&eth1DepositRoot,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		copy(block.Root[:], blockRoot)
		copy(block.RANDAOReveal[:], randaoReveal)
		copy(block.BodyRoot[:], bodyRoot)
		copy(block.ParentRoot[:], parentRoot)
		copy(block.StateRoot[:], stateRoot)
		if canonical.Valid {
			val := canonical.Bool
			block.Canonical = &val
		}
		copy(block.ETH1DepositRoot[:], eth1DepositRoot)
		blocks = append(blocks, block)
	}

	// Add execution payload to the blocks where available.
	for _, block := range blocks {
		block.ExecutionPayload, err = s.executionPayload(ctx, tx, block.Root)
		if err != nil {
			return nil, err
		}
	}

	return blocks, nil
}

// BlocksForSlotRange fetches all blocks with the given slot range.
// Ranges are inclusive of start and exclusive of end i.e. a request with startSlot 2 and endSlot 4 will provide
// blocks duties for slots 2 and 3.
func (s *Service) BlocksForSlotRange(ctx context.Context, startSlot phase0.Slot, endSlot phase0.Slot) ([]*chaindb.Block, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "BlocksForSlotRange")
	defer span.End()

	var err error

	tx := s.tx(ctx)
	if tx == nil {
		ctx, err = s.BeginROTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer s.CommitROTx(ctx)
	}

	rows, err := tx.Query(ctx, `
      SELECT f_slot
            ,f_proposer_index
            ,f_root
            ,f_graffiti
            ,f_randao_reveal
            ,f_body_root
            ,f_parent_root
            ,f_state_root
            ,f_canonical
            ,f_eth1_block_hash
            ,f_eth1_deposit_count
            ,f_eth1_deposit_root
      FROM t_blocks
      WHERE f_slot >= $1
        AND f_slot < $2
      ORDER BY f_slot`,
		startSlot,
		endSlot,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	blocks := make([]*chaindb.Block, 0)
	for rows.Next() {
		block := &chaindb.Block{}
		var blockRoot []byte
		var randaoReveal []byte
		var bodyRoot []byte
		var parentRoot []byte
		var stateRoot []byte
		var canonical sql.NullBool
		var eth1DepositRoot []byte
		err := rows.Scan(
			&block.Slot,
			&block.ProposerIndex,
			&blockRoot,
			&block.Graffiti,
			&randaoReveal,
			&bodyRoot,
			&parentRoot,
			&stateRoot,
			&canonical,
			&block.ETH1BlockHash,
			&block.ETH1DepositCount,
			&eth1DepositRoot,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		copy(block.Root[:], blockRoot)
		copy(block.RANDAOReveal[:], randaoReveal)
		copy(block.BodyRoot[:], bodyRoot)
		copy(block.ParentRoot[:], parentRoot)
		copy(block.StateRoot[:], stateRoot)
		if canonical.Valid {
			val := canonical.Bool
			block.Canonical = &val
		}
		copy(block.ETH1DepositRoot[:], eth1DepositRoot)
		blocks = append(blocks, block)
	}

	// Add execution payload to the blocks where available.
	for _, block := range blocks {
		block.ExecutionPayload, err = s.executionPayload(ctx, tx, block.Root)
		if err != nil {
			return nil, err
		}
	}

	return blocks, nil
}

// BlockByRoot fetches the block with the given root.
func (s *Service) BlockByRoot(ctx context.Context, root phase0.Root) (*chaindb.Block, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "BlockByroot")
	defer span.End()

	var err error

	tx := s.tx(ctx)
	if tx == nil {
		ctx, err = s.BeginROTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer s.CommitROTx(ctx)
	}

	block := &chaindb.Block{}
	var blockRoot []byte
	var randaoReveal []byte
	var bodyRoot []byte
	var parentRoot []byte
	var stateRoot []byte
	var canonical sql.NullBool
	var eth1DepositRoot []byte

	err = tx.QueryRow(ctx, `
      SELECT f_slot
            ,f_proposer_index
            ,f_root
            ,f_graffiti
            ,f_randao_reveal
            ,f_body_root
            ,f_parent_root
            ,f_state_root
            ,f_canonical
            ,f_eth1_block_hash
            ,f_eth1_deposit_count
            ,f_eth1_deposit_root
      FROM t_blocks
      WHERE f_root = $1`,
		root[:],
	).Scan(
		&block.Slot,
		&block.ProposerIndex,
		&blockRoot,
		&block.Graffiti,
		&randaoReveal,
		&bodyRoot,
		&parentRoot,
		&stateRoot,
		&canonical,
		&block.ETH1BlockHash,
		&block.ETH1DepositCount,
		&eth1DepositRoot,
	)
	if err != nil {
		return nil, err
	}
	copy(block.Root[:], blockRoot)
	copy(block.RANDAOReveal[:], randaoReveal)
	copy(block.BodyRoot[:], bodyRoot)
	copy(block.ParentRoot[:], parentRoot)
	copy(block.StateRoot[:], stateRoot)
	if canonical.Valid {
		val := canonical.Bool
		block.Canonical = &val
	}
	copy(block.ETH1DepositRoot[:], eth1DepositRoot)

	// Add execution payload to the block if available.
	block.ExecutionPayload, err = s.executionPayload(ctx, tx, block.Root)
	if err != nil {
		return nil, err
	}

	return block, nil
}

// CanonicalBlockPresenceForSlotRange returns a boolean for each slot in the range for the presence
// of a canonical block.
// Ranges are inclusive of start and exclusive of end i.e. a request with startSlot 2 and endSlot 4 will provide
// presence duties for slots 2 and 3.
func (s *Service) CanonicalBlockPresenceForSlotRange(ctx context.Context, startSlot phase0.Slot, endSlot phase0.Slot) ([]bool, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "CanonicalBlockPresenceForSlotRange")
	defer span.End()

	var err error

	tx := s.tx(ctx)
	if tx == nil {
		ctx, err = s.BeginROTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer s.CommitROTx(ctx)
	}

	rows, err := tx.Query(ctx, `
      SELECT f_slot
      FROM t_blocks
      WHERE f_slot >= $1
        AND f_slot < $2
        AND f_canonical = TRUE
	  ORDER BY f_slot`,
		startSlot,
		endSlot,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	presence := make([]bool, endSlot-startSlot)
	for rows.Next() {
		var slot phase0.Slot
		err := rows.Scan(
			&slot,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		presence[slot-startSlot] = true
	}

	return presence, nil
}

// BlocksByParentRoot fetches the blocks with the given root.
func (s *Service) BlocksByParentRoot(ctx context.Context, parentRoot phase0.Root) ([]*chaindb.Block, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "BlocksByParentRoot")
	defer span.End()

	var err error

	tx := s.tx(ctx)
	if tx == nil {
		ctx, err = s.BeginROTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer s.CommitROTx(ctx)
	}

	rows, err := tx.Query(ctx, `
      SELECT f_slot
            ,f_proposer_index
            ,f_root
            ,f_graffiti
            ,f_randao_reveal
            ,f_body_root
            ,f_parent_root
            ,f_state_root
            ,f_canonical
            ,f_eth1_block_hash
            ,f_eth1_deposit_count
            ,f_eth1_deposit_root
      FROM t_blocks
      WHERE f_parent_root = $1`,
		parentRoot[:],
	)
	if err != nil {
		return nil, err
	}

	blocks := make([]*chaindb.Block, 0)

	for rows.Next() {
		block := &chaindb.Block{}
		var blockRoot []byte
		var randaoReveal []byte
		var bodyRoot []byte
		var parentRoot []byte
		var stateRoot []byte
		var canonical sql.NullBool
		var eth1DepositRoot []byte
		err := rows.Scan(
			&block.Slot,
			&block.ProposerIndex,
			&blockRoot,
			&block.Graffiti,
			&randaoReveal,
			&bodyRoot,
			&parentRoot,
			&stateRoot,
			&canonical,
			&block.ETH1BlockHash,
			&block.ETH1DepositCount,
			&eth1DepositRoot,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		copy(block.Root[:], blockRoot)
		copy(block.RANDAOReveal[:], randaoReveal)
		copy(block.BodyRoot[:], bodyRoot)
		copy(block.ParentRoot[:], parentRoot)
		copy(block.StateRoot[:], stateRoot)
		if canonical.Valid {
			val := canonical.Bool
			block.Canonical = &val
		}
		copy(block.ETH1DepositRoot[:], eth1DepositRoot)
		blocks = append(blocks, block)
	}

	// Add execution payload to the blocks where available.
	for _, block := range blocks {
		block.ExecutionPayload, err = s.executionPayload(ctx, tx, block.Root)
		if err != nil {
			return nil, err
		}
	}

	return blocks, nil
}

// EmptySlots fetches the slots in the given range without a block in the database.
func (s *Service) EmptySlots(ctx context.Context, minSlot phase0.Slot, maxSlot phase0.Slot) ([]phase0.Slot, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "EmptySlots")
	defer span.End()

	var err error

	tx := s.tx(ctx)
	if tx == nil {
		ctx, err = s.BeginROTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer s.CommitROTx(ctx)
	}

	rows, err := tx.Query(ctx, `
      SELECT missed
	  FROM generate_series($1,$2,1) missed
	  LEFT JOIN t_blocks
	  ON missed = t_blocks.f_slot
	  WHERE f_slot IS NULL`,
		minSlot,
		maxSlot,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	missedSlots := make([]phase0.Slot, 0)
	for rows.Next() {
		missedSlot := phase0.Slot(0)
		err := rows.Scan(&missedSlot)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		missedSlots = append(missedSlots, missedSlot)
	}

	return missedSlots, nil
}

// IndeterminateBlocks fetches the blocks in the given range that do not have a canonical status.
func (s *Service) IndeterminateBlocks(ctx context.Context, minSlot phase0.Slot, maxSlot phase0.Slot) ([]phase0.Root, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "IndeterminateBlocks")
	defer span.End()

	var err error

	tx := s.tx(ctx)
	if tx == nil {
		ctx, err = s.BeginROTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer s.CommitROTx(ctx)
	}

	rows, err := tx.Query(ctx, `
      SELECT f_root
      FROM t_blocks
      WHERE f_slot >= $1
        AND f_slot < $2
        AND f_canonical IS NULL
	  ORDER BY f_slot`,
		minSlot,
		maxSlot,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	indeterminateRoots := make([]phase0.Root, 0)
	var missedRootBytes []byte
	for rows.Next() {
		err := rows.Scan(&missedRootBytes)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		var missedRoot phase0.Root
		copy(missedRoot[:], missedRootBytes)
		indeterminateRoots = append(indeterminateRoots, missedRoot)
	}

	return indeterminateRoots, nil
}

// LatestBlocks fetches the blocks with the highest slot number for in the database.
func (s *Service) LatestBlocks(ctx context.Context) ([]*chaindb.Block, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "LatestBlocks")
	defer span.End()

	var err error

	tx := s.tx(ctx)
	if tx == nil {
		ctx, err = s.BeginROTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer s.CommitROTx(ctx)
	}

	rows, err := tx.Query(ctx, `
      SELECT f_slot
            ,f_proposer_index
            ,f_root
            ,f_graffiti
            ,f_randao_reveal
            ,f_body_root
            ,f_parent_root
            ,f_state_root
            ,f_canonical
            ,f_eth1_block_hash
            ,f_eth1_deposit_count
            ,f_eth1_deposit_root
      FROM t_blocks
      WHERE f_slot = (SELECT MAX(f_slot) FROM t_blocks)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	blocks := make([]*chaindb.Block, 0)

	for rows.Next() {
		block := &chaindb.Block{}
		var blockRoot []byte
		var randaoReveal []byte
		var bodyRoot []byte
		var parentRoot []byte
		var stateRoot []byte
		var canonical sql.NullBool
		var eth1DepositRoot []byte
		err := rows.Scan(
			&block.Slot,
			&block.ProposerIndex,
			&blockRoot,
			&block.Graffiti,
			&randaoReveal,
			&bodyRoot,
			&parentRoot,
			&stateRoot,
			&canonical,
			&block.ETH1BlockHash,
			&block.ETH1DepositCount,
			&eth1DepositRoot,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		copy(block.Root[:], blockRoot)
		copy(block.RANDAOReveal[:], randaoReveal)
		copy(block.BodyRoot[:], bodyRoot)
		copy(block.ParentRoot[:], parentRoot)
		copy(block.StateRoot[:], stateRoot)
		if canonical.Valid {
			val := canonical.Bool
			block.Canonical = &val
		}
		copy(block.ETH1DepositRoot[:], eth1DepositRoot)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, block)
	}

	// Add execution payload to the blocks where available.
	for _, block := range blocks {
		block.ExecutionPayload, err = s.executionPayload(ctx, tx, block.Root)
		if err != nil {
			return nil, err
		}
	}

	return blocks, nil
}

// LatestCanonicalBlock returns the slot of the latest canonical block known in the database.
func (s *Service) LatestCanonicalBlock(ctx context.Context) (phase0.Slot, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "LatestCanonicalBlock")
	defer span.End()

	var err error

	tx := s.tx(ctx)
	if tx == nil {
		ctx, err = s.BeginROTx(ctx)
		if err != nil {
			return 0, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer s.CommitROTx(ctx)
	}

	var slot phase0.Slot
	err = tx.QueryRow(ctx, `
      SELECT COALESCE(MAX(f_slot),0)
      FROM t_blocks
      WHERE f_canonical = true`,
	).Scan(
		&slot,
	)
	if err != nil {
		return 0, err
	}

	return slot, nil
}

// ProposalCount provides the number of proposals for the given validators.
// Ranges are inclusive of start and exclusive of end i.e. a request with startSlot 2 and endSlot 4 will provide
// blocks duties for slots 2 and 3.
func (s *Service) ProposalCount(ctx context.Context, validatorIndices []phase0.ValidatorIndex, startSlot phase0.Slot, endSlot phase0.Slot) (uint64, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "ProposalCount")
	defer span.End()

	var err error

	tx := s.tx(ctx)
	if tx == nil {
		ctx, err = s.BeginROTx(ctx)
		if err != nil {
			return 0, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer s.CommitROTx(ctx)
	}

	proposals := uint64(0)
	err = tx.QueryRow(ctx, `
      SELECT COUNT(*)
	  FROM t_blocks
      WHERE f_slot >= $1
        AND f_slot < $2
        AND f_proposer_index = ANY($3)`,
		startSlot,
		endSlot,
		validatorIndices,
	).Scan(&proposals)
	if err != nil {
		return 0, err
	}

	return proposals, nil
}
