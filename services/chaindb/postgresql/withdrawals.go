// Copyright © 2023 Weald Technology Limited.
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
	"fmt"
	"sort"
	"strings"

	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
	"go.opentelemetry.io/otel"
)

// setWithdrawals sets the withdrawals of a block.
func (s *Service) setWithdrawals(ctx context.Context, block *chaindb.Block) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "setWithdrawals")
	defer span.End()

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
	if len(block.ExecutionPayload.Withdrawals) == 0 {
		// This is a pre-Capella execution payload, ignore it.
		return nil
	}

	for _, withdrawal := range block.ExecutionPayload.Withdrawals {
		if _, err := tx.Exec(ctx, `
INSERT INTO t_block_withdrawals(f_block_root
                               ,f_block_number
                               ,f_index
                               ,f_withdrawal_index
                               ,f_validator_index
                               ,f_address
                               ,f_amount
                               )
VALUES($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT (f_block_root,f_block_number,f_index) DO
UPDATE
SET f_withdrawal_index = excluded.f_withdrawal_index
   ,f_validator_index = excluded.f_validator_index
   ,f_address = excluded.f_address
   ,f_amount = excluded.f_amount
`,
			withdrawal.InclusionBlockRoot[:],
			withdrawal.InclusionSlot,
			withdrawal.InclusionIndex,
			withdrawal.Index,
			withdrawal.ValidatorIndex,
			withdrawal.Address[:],
			withdrawal.Amount,
		); err != nil {
			return err
		}
	}

	return nil
}

// Withdrawals provides withdrawals according to the filter.
func (s *Service) Withdrawals(ctx context.Context, filter *chaindb.WithdrawalFilter) ([]*chaindb.Withdrawal, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "Withdrawals")
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
	queryVals := make([]interface{}, 0)

	queryBuilder.WriteString(`
SELECT f_block_root
      ,f_block_number
      ,f_index
      ,f_withdrawal_index
      ,f_validator_index
      ,f_address
      ,f_amount
FROM t_block_withdrawals`)

	wherestr := "WHERE"

	if filter.Canonical != nil {
		queryVals = append(queryVals, *filter.Canonical)
		queryBuilder.WriteString(fmt.Sprintf(`
LEFT JOIN t_blocks ON f_block_number = f_slot
%s f_canonical = $%d`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if filter.From != nil {
		queryVals = append(queryVals, *filter.From)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_block_number >= $%d`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if filter.To != nil {
		queryVals = append(queryVals, *filter.To)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_block_number <= $%d`, wherestr, len(queryVals)))
	}

	if len(filter.ValidatorIndices) > 0 {
		queryVals = append(queryVals, filter.ValidatorIndices)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_validator_index = ANY($%d)`, wherestr, len(queryVals)))
	}

	switch filter.Order {
	case chaindb.OrderEarliest:
		queryBuilder.WriteString(`
ORDER BY f_block_number, f_index`)
	case chaindb.OrderLatest:
		queryBuilder.WriteString(`
ORDER BY f_block_number DESC,f_index DESC`)
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

	withdrawals := make([]*chaindb.Withdrawal, 0)
	inclusionBlockRoot := make([]byte, phase0.RootLength)
	address := make([]byte, bellatrix.ExecutionAddressLength)
	for rows.Next() {
		withdrawal := &chaindb.Withdrawal{}
		err := rows.Scan(
			&inclusionBlockRoot,
			&withdrawal.InclusionSlot,
			&withdrawal.InclusionIndex,
			&withdrawal.Index,
			&withdrawal.ValidatorIndex,
			&address,
			&withdrawal.Amount,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		copy(withdrawal.InclusionBlockRoot[:], inclusionBlockRoot)
		copy(withdrawal.Address[:], address)
		withdrawals = append(withdrawals, withdrawal)
	}

	// Always return order of block number then inclusion index.
	sort.Slice(withdrawals, func(i int, j int) bool {
		if withdrawals[i].InclusionSlot != withdrawals[j].InclusionSlot {
			return withdrawals[i].InclusionSlot < withdrawals[j].InclusionSlot
		}
		return withdrawals[i].InclusionIndex < withdrawals[j].InclusionIndex
	})
	return withdrawals, nil
}
