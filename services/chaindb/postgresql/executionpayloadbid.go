// Copyright © 2026 Weald Technology Trading.
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
	"context"
	"math/big"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/wealdtech/chaind/services/chaindb"
	"go.opentelemetry.io/otel"
)

// setExecutionPayloadBid sets the execution payload bid of a block.
func (s *Service) setExecutionPayloadBid(ctx context.Context, block *chaindb.Block) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "setExecutionPayloadBid")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	if block == nil {
		return errors.New("block missing")
	}
	if block.ExecutionPayloadBid == nil {
		// Do not treat this as an error, as pre-Gloas blocks will not have a bid.
		return nil
	}

	bid := block.ExecutionPayloadBid
	_, err := tx.Exec(ctx, `
INSERT INTO t_block_execution_payload_bids(f_block_root
                                          ,f_parent_block_hash
                                          ,f_block_hash
                                          ,f_fee_recipient
                                          ,f_gas_limit
                                          ,f_builder_index
                                          ,f_value
                                          ,f_execution_payment
                                          )
VALUES($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (f_block_root) DO
UPDATE
SET f_parent_block_hash = excluded.f_parent_block_hash
   ,f_block_hash = excluded.f_block_hash
   ,f_fee_recipient = excluded.f_fee_recipient
   ,f_gas_limit = excluded.f_gas_limit
   ,f_builder_index = excluded.f_builder_index
   ,f_value = excluded.f_value
   ,f_execution_payment = excluded.f_execution_payment
`,
		block.Root[:],
		bid.ParentBlockHash[:],
		bid.BlockHash[:],
		bid.FeeRecipient[:],
		bid.GasLimit,
		decimal.NewFromBigInt(new(big.Int).SetUint64(bid.BuilderIndex), 0),
		bid.Value,
		bid.ExecutionPayment,
	)

	return err
}

// executionPayloadBid fetches the execution payload bid of a block.
func (s *Service) executionPayloadBid(ctx context.Context,
	tx pgx.Tx,
	root phase0.Root,
) (
	*chaindb.ExecutionPayloadBid,
	error,
) {
	bids, err := s.executionPayloadBids(ctx, tx, []phase0.Root{root})
	if err != nil {
		return nil, err
	}

	return bids[root], nil
}

// executionPayloadBids fetches the execution payload bids of multiple blocks.
func (s *Service) executionPayloadBids(ctx context.Context,
	tx pgx.Tx,
	roots []phase0.Root,
) (
	map[phase0.Root]*chaindb.ExecutionPayloadBid,
	error,
) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "executionPayloadBids")
	defer span.End()

	broots := make([][]byte, len(roots))
	for i := range roots {
		broots[i] = roots[i][:]
	}

	rows, err := tx.Query(ctx, `
SELECT f_block_root
      ,f_parent_block_hash
      ,f_block_hash
      ,f_fee_recipient
      ,f_gas_limit
      ,f_builder_index
      ,f_value
      ,f_execution_payment
FROM t_block_execution_payload_bids
WHERE f_block_root = ANY($1)`,
		broots,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := make(map[phase0.Root]*chaindb.ExecutionPayloadBid)
	for rows.Next() {
		bid := &chaindb.ExecutionPayloadBid{}
		var blockRoot []byte
		var parentBlockHash []byte
		var blockHash []byte
		var feeRecipient []byte
		var builderIndex decimal.Decimal
		err := rows.Scan(&blockRoot,
			&parentBlockHash,
			&blockHash,
			&feeRecipient,
			&bid.GasLimit,
			&builderIndex,
			&bid.Value,
			&bid.ExecutionPayment,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		copy(bid.ParentBlockHash[:], parentBlockHash)
		copy(bid.BlockHash[:], blockHash)
		copy(bid.FeeRecipient[:], feeRecipient)
		bid.BuilderIndex = builderIndex.BigInt().Uint64()

		var key phase0.Root
		copy(key[:], blockRoot)
		res[key] = bid
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "failed to read rows")
	}

	return res, nil
}
