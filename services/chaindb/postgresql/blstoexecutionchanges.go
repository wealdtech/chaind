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

// setBLSToExecutionChanges sets the BLS to execution changes of a block.
func (s *Service) setBLSToExecutionChanges(ctx context.Context, block *chaindb.Block) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "setBLSToExecutionChanges")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	if block == nil {
		return errors.New("block missing")
	}
	if len(block.BLSToExecutionChanges) == 0 {
		return nil
	}

	for _, change := range block.BLSToExecutionChanges {
		if _, err := tx.Exec(ctx, `
INSERT INTO t_block_bls_to_execution_changes(f_block_root
                                            ,f_block_number
                                            ,f_index
                                            ,f_validator_index
                                            ,f_from_bls_pubkey
                                            ,f_to_execution_address
                                            )
VALUES($1,$2,$3,$4,$5,$6)
ON CONFLICT (f_block_root,f_block_number,f_index) DO
UPDATE
SET f_validator_index = excluded.f_validator_index 
   ,f_from_bls_pubkey = excluded.f_from_bls_pubkey
   ,f_to_execution_address = excluded.f_to_execution_address
`,
			change.InclusionBlockRoot[:],
			change.InclusionSlot,
			change.InclusionIndex,
			change.ValidatorIndex,
			change.FromBLSPubKey[:],
			change.ToExecutionAddress[:],
		); err != nil {
			return err
		}
	}

	return nil
}

// BLSToExecutionChanges provides withdrawals according to the filter.
func (s *Service) BLSToExecutionChanges(ctx context.Context, filter *chaindb.BLSToExecutionChangeFilter) ([]*chaindb.BLSToExecutionChange, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "BLSToExecutionChanges")
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
      ,f_validator_index
      ,f_from_bls_pubkey
      ,f_to_execution_address
FROM t_block_bls_to_execution_changes`)

	wherestr := "WHERE"

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

	changes := make([]*chaindb.BLSToExecutionChange, 0)
	inclusionBlockRoot := make([]byte, phase0.RootLength)
	fromBLSPubKey := make([]byte, phase0.PublicKeyLength)
	toExecutionAddress := make([]byte, bellatrix.ExecutionAddressLength)
	for rows.Next() {
		change := &chaindb.BLSToExecutionChange{}
		err := rows.Scan(
			&inclusionBlockRoot,
			&change.InclusionSlot,
			&change.InclusionIndex,
			&change.ValidatorIndex,
			&fromBLSPubKey,
			&toExecutionAddress,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		copy(change.InclusionBlockRoot[:], inclusionBlockRoot)
		copy(change.FromBLSPubKey[:], fromBLSPubKey)
		copy(change.ToExecutionAddress[:], toExecutionAddress)
		changes = append(changes, change)
	}

	// Always return order of block number then inclusion index.
	sort.Slice(changes, func(i int, j int) bool {
		if changes[i].InclusionSlot != changes[j].InclusionSlot {
			return changes[i].InclusionSlot < changes[j].InclusionSlot
		}
		return changes[i].InclusionIndex < changes[j].InclusionIndex
	})
	return changes, nil
}
