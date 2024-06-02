// Copyright © 2020, 2021 Weald Technology Trading.
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

// SetProposerDuty sets a proposer duty.
func (s *Service) SetProposerDuty(ctx context.Context, proposerDuty *chaindb.ProposerDuty) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SetProposerDuty")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.Exec(ctx, `
      INSERT INTO t_proposer_duties(f_slot
                                   ,f_validator_index)
      VALUES($1,$2)
      ON CONFLICT (f_slot) DO
      UPDATE
      SET f_validator_index = excluded.f_validator_index
		 `,
		proposerDuty.Slot,
		proposerDuty.ValidatorIndex,
	)

	return err
}

// ProposerDutiesForSlotRange fetches all proposer duties for a slot range.
func (s *Service) ProposerDutiesForSlotRange(ctx context.Context,
	startSlot phase0.Slot,
	endSlot phase0.Slot,
) (
	[]*chaindb.ProposerDuty,
	error,
) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "ProposerDutiesForSlotRange")
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

	rows, err := tx.Query(ctx, `
      SELECT f_slot
            ,f_validator_index
      FROM t_proposer_duties
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

	proposerDuties := make([]*chaindb.ProposerDuty, 0)

	for rows.Next() {
		proposerDuty := &chaindb.ProposerDuty{}
		err := rows.Scan(
			&proposerDuty.Slot,
			&proposerDuty.ValidatorIndex,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		proposerDuties = append(proposerDuties, proposerDuty)
	}

	return proposerDuties, nil
}

// ProposerDutiesForValidator provides all proposer duties for the given validator index.
func (s *Service) ProposerDutiesForValidator(ctx context.Context, proposer phase0.ValidatorIndex) ([]*chaindb.ProposerDuty, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "ProposerDutiesForValidator")
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

	rows, err := tx.Query(ctx, `
SELECT f_slot
FROM t_proposer_duties
WHERE f_validator_index = $1
ORDER BY f_slot
`,
		proposer,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	proposerDuties := make([]*chaindb.ProposerDuty, 0)

	for rows.Next() {
		proposerDuty := &chaindb.ProposerDuty{
			ValidatorIndex: proposer,
		}
		err := rows.Scan(
			&proposerDuty.Slot,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		proposerDuties = append(proposerDuties, proposerDuty)
	}

	return proposerDuties, nil
}

// ProposerDuties provides data according to the filter.
func (s *Service) ProposerDuties(ctx context.Context, filter *chaindb.ProposerDutyFilter) ([]*chaindb.ProposerDuty, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "PrpopserDuties")
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
      ,f_validator_index
FROM t_proposer_duties`)

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

	if filter.ValidatorIndices != nil && len(*filter.ValidatorIndices) > 0 {
		queryVals = append(queryVals, *filter.ValidatorIndices)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_validator_index = ANY($%d)`, wherestr, len(queryVals)))
	}

	switch filter.Order {
	case chaindb.OrderEarliest:
		queryBuilder.WriteString(`
ORDER BY f_slot, f_validator_index`)
	case chaindb.OrderLatest:
		queryBuilder.WriteString(`
ORDER BY f_slot DESC,f_validator_index DESC`)
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

	duties := make([]*chaindb.ProposerDuty, 0)
	for rows.Next() {
		duty := &chaindb.ProposerDuty{}
		err := rows.Scan(
			&duty.Slot,
			&duty.ValidatorIndex,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		duties = append(duties, duty)
	}

	// Always return order of slot then validator index.
	sort.Slice(duties, func(i int, j int) bool {
		if duties[i].Slot != duties[j].Slot {
			return duties[i].Slot < duties[j].Slot
		}
		return duties[i].ValidatorIndex < duties[j].ValidatorIndex
	})

	return duties, nil
}
