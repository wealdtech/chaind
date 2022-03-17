// Copyright © 2022 Weald Technology Trading.
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

	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/wealdtech/chaind/services/chaindb"
)

// setExecutionPayload sets the execution payload of a block.
func (s *Service) setExecutionPayload(ctx context.Context, block *chaindb.Block) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	if block == nil {
		return errors.New("block missing")
	}
	if block.ExecutionPayload == nil {
		// Do not treat this as an error, as pre-Bellatrix blocks will not have
		// an execution payload.
		return nil
	}
	if block.ExecutionPayload.BlockHash == [32]byte{} {
		// This is an empty execution payload, which happens after the bellatrix
		// fork but before terminal total difficulty; ignore it.
		return nil
	}

	_, err := tx.Exec(ctx, `
INSERT INTO t_block_execution_payloads(f_block_root
                                      ,f_block_number
                                      ,f_block_hash
                                      ,f_parent_hash
                                      ,f_fee_recipient
                                      ,f_state_root
                                      ,f_receipts_root
                                      ,f_logs_bloom
                                      ,f_prev_randao
                                      ,f_gas_limit
                                      ,f_gas_used
                                      ,f_base_fee_per_gas
                                      ,f_extra_data
                                      )
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
ON CONFLICT (f_block_root) DO
UPDATE
SET f_block_number = excluded.f_block_number
   ,f_block_hash = excluded.f_block_hash
   ,f_parent_hash = excluded.f_parent_hash
   ,f_fee_recipient = excluded.f_fee_recipient
   ,f_state_root = excluded.f_state_root
   ,f_receipts_root = excluded.f_receipts_root
   ,f_logs_bloom = excluded.f_logs_bloom
   ,f_prev_randao = excluded.f_prev_randao
   ,f_gas_limit = excluded.f_gas_limit
   ,f_gas_used = excluded.f_gas_used
   ,f_base_fee_per_gas = excluded.f_base_fee_per_gas
   ,f_extra_data = excluded.f_extra_data
`,
		block.Root[:],
		block.ExecutionPayload.BlockNumber,
		block.ExecutionPayload.BlockHash[:],
		block.ExecutionPayload.ParentHash[:],
		block.ExecutionPayload.FeeRecipient[:],
		block.ExecutionPayload.StateRoot[:],
		block.ExecutionPayload.ReceiptsRoot[:],
		block.ExecutionPayload.LogsBloom[:],
		block.ExecutionPayload.PrevRandao[:],
		block.ExecutionPayload.GasLimit,
		block.ExecutionPayload.GasUsed,
		decimal.NewFromBigInt(block.ExecutionPayload.BaseFeePerGas, 0),
		block.ExecutionPayload.ExtraData,
	)

	return err
}
