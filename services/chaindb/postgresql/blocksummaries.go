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

package postgresql

import (
	"context"
	"fmt"
	"strings"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
	"go.opentelemetry.io/otel"
)

// SetBlockSummary sets a block summary.
func (s *Service) SetBlockSummary(ctx context.Context, summary *chaindb.BlockSummary) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SetBlockSummary")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.Exec(ctx, `
      INSERT INTO t_block_summaries(f_slot
                                   ,f_attestations_for_block
                                   ,f_duplicate_attestations_for_block
                                   ,f_votes_for_block
                                   ,f_parent_distance)
      VALUES($1,$2,$3,$4,$5)
      ON CONFLICT (f_slot) DO
      UPDATE
      SET f_attestations_for_block = excluded.f_attestations_for_block
         ,f_duplicate_attestations_for_block = excluded.f_duplicate_attestations_for_block
         ,f_votes_for_block = excluded.f_votes_for_block
         ,f_parent_distance = excluded.f_parent_distance
		 `,
		summary.Slot,
		summary.AttestationsForBlock,
		summary.DuplicateAttestationsForBlock,
		summary.VotesForBlock,
		summary.ParentDistance,
	)

	return err
}

func (s *Service) BlockSummaries(ctx context.Context, filter *chaindb.BlockSummaryFilter) ([]*chaindb.BlockSummary, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "BlockSummaries")
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
SELECT f_slot
      ,f_attestations_for_block
      ,f_duplicate_attestations_for_block
      ,f_votes_for_block
      ,f_parent_distance
FROM t_block_summaries`)

	conditions := make([]string, 0)

	if filter.From != nil {
		queryVals = append(queryVals, *filter.From)
		conditions = append(conditions, fmt.Sprintf(`f_slot >= $%d`, len(queryVals)))
	}

	if filter.To != nil {
		queryVals = append(queryVals, *filter.To)
		conditions = append(conditions, fmt.Sprintf(`f_to <= $%d`, len(queryVals)))
	}

	if len(conditions) > 0 {
		queryBuilder.WriteString(`
WHERE`)
		queryBuilder.WriteString(strings.Join(conditions, " OR "))
	}

	switch filter.Order {
	case chaindb.OrderEarliest:
		queryBuilder.WriteString(`
ORDER BY f_slot`)
	case chaindb.OrderLatest:
		queryBuilder.WriteString(`
ORDER BY f_slot DESC`)
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

	summaries := make([]*chaindb.BlockSummary, 0)
	for rows.Next() {
		summary := &chaindb.BlockSummary{}
		if err := rows.Scan(
			&summary.Slot,
			&summary.AttestationsForBlock,
			&summary.DuplicateAttestationsForBlock,
			&summary.VotesForBlock,
			&summary.ParentDistance,
		); err != nil {
			return nil, err
		}
		summaries = append(summaries, summary)
	}

	return summaries, nil
}

// BlockSummaryForSlot obtains the summary of a block for a given slot.
func (s *Service) BlockSummaryForSlot(ctx context.Context, slot phase0.Slot) (*chaindb.BlockSummary, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "BlockSummaryForSlot")
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

	summary := &chaindb.BlockSummary{
		Slot: slot,
	}
	err := tx.QueryRow(ctx, `
SELECT f_attestations_for_block
      ,f_duplicate_attestations_for_block
      ,f_votes_for_block
      ,f_parent_distance
FROM t_block_summaries
WHERE f_slot = $1
`,
		slot,
	).Scan(
		&summary.AttestationsForBlock,
		&summary.DuplicateAttestationsForBlock,
		&summary.VotesForBlock,
		&summary.ParentDistance,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to scan row")
	}

	return summary, nil
}
