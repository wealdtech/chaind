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

package postgresql

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
	"go.opentelemetry.io/otel"
)

// SetSyncAggregate sets the sync aggregate.
func (s *Service) SetSyncAggregate(ctx context.Context, syncAggregate *chaindb.SyncAggregate) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SetSyncAggregate")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.Exec(ctx, `
      INSERT INTO t_sync_aggregates(f_inclusion_slot
                                   ,f_inclusion_block_root
                                   ,f_bits
                                   ,f_indices
                                  )
      VALUES($1,$2,$3,$4)
      ON CONFLICT (f_inclusion_slot, f_inclusion_block_root) DO
      UPDATE
      SET f_bits = excluded.f_bits
         ,f_indices = excluded.f_indices
	  `,
		syncAggregate.InclusionSlot,
		syncAggregate.InclusionBlockRoot[:],
		syncAggregate.Bits,
		syncAggregate.Indices,
	)

	return err
}

// SyncAggregates provides sync aggregates according to the filter.
func (s *Service) SyncAggregates(ctx context.Context, filter *chaindb.SyncAggregateFilter) ([]*chaindb.SyncAggregate, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SyncAggregates")
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
SELECT f_inclusion_slot
      ,f_inclusion_block_root
      ,f_bits
      ,f_indices
FROM t_sync_aggregates`)

	wherestr := "WHERE"

	if filter.From != nil {
		queryVals = append(queryVals, *filter.From)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_inclusion_slot >= $%d`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if filter.To != nil {
		queryVals = append(queryVals, *filter.To)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_inclusion_slot <= $%d`, wherestr, len(queryVals)))
	}

	switch filter.Order {
	case chaindb.OrderEarliest:
		queryBuilder.WriteString(`
ORDER BY f_inclusion_slot`)
	case chaindb.OrderLatest:
		queryBuilder.WriteString(`
ORDER BY f_inclusion_slot DESC`)
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

	aggregates := make([]*chaindb.SyncAggregate, 0)
	for rows.Next() {
		summary := &chaindb.SyncAggregate{}
		indices := make([]uint64, 0)
		var inclusionBlockRoot []byte
		err := rows.Scan(
			&summary.InclusionSlot,
			&inclusionBlockRoot,
			&summary.Bits,
			&indices,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		copy(summary.InclusionBlockRoot[:], inclusionBlockRoot)
		summary.Indices = make([]phase0.ValidatorIndex, len(indices))
		for i := range indices {
			summary.Indices[i] = phase0.ValidatorIndex(indices[i])
		}
		aggregates = append(aggregates, summary)
	}

	// Always return order of inclusion slot.
	sort.Slice(aggregates, func(i int, j int) bool {
		return aggregates[i].InclusionSlot < aggregates[j].InclusionSlot
	})
	return aggregates, nil

}
