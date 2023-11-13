// Copyright © 2021 - 2023 Weald Technology Limited.
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
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
	"go.opentelemetry.io/otel"
)

// SetValidatorEpochSummaries sets multiple validator epoch summaries.
func (s *Service) SetValidatorEpochSummaries(ctx context.Context, summaries []*chaindb.ValidatorEpochSummary) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SetValidatorEpochSummaries")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	// Create a savepoint in case the copy fails.
	nestedTx, err := tx.Begin(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create nested transaction")
	}

	_, err = nestedTx.CopyFrom(ctx,
		pgx.Identifier{"t_validator_epoch_summaries"},
		[]string{
			"f_validator_index",
			"f_epoch",
			"f_proposer_duties",
			"f_proposals_included",
			"f_attestation_included",
			"f_attestation_target_correct",
			"f_attestation_head_correct",
			"f_attestation_inclusion_delay",
			"f_attestation_source_timely",
			"f_attestation_target_timely",
			"f_attestation_head_timely",
		},
		pgx.CopyFromSlice(len(summaries), func(i int) ([]any, error) {
			return []any{
				summaries[i].Index,
				summaries[i].Epoch,
				summaries[i].ProposerDuties,
				summaries[i].ProposalsIncluded,
				summaries[i].AttestationIncluded,
				summaries[i].AttestationTargetCorrect,
				summaries[i].AttestationHeadCorrect,
				summaries[i].AttestationInclusionDelay,
				summaries[i].AttestationSourceTimely,
				summaries[i].AttestationTargetTimely,
				summaries[i].AttestationHeadTimely,
			}, nil
		}))

	if err == nil {
		if err := nestedTx.Commit(ctx); err != nil {
			return errors.Wrap(err, "failed to commit nested transaction")
		}
	} else {
		if err := nestedTx.Rollback(ctx); err != nil {
			return errors.Wrap(err, "failed to roll back nested transaction")
		}

		log.Debug().Err(err).Msg("Failed to copy insert validator epoch summaries; applying one at a time")
		for _, summary := range summaries {
			if err := s.SetValidatorEpochSummary(ctx, summary); err != nil {
				log.Error().Err(err).Msg("Failure to insert individual summary")
				return err
			}
		}

		// Succeeded so clear the error.
		err = nil
	}

	return err
}

// SetValidatorEpochSummary sets a validator epoch summary.
func (s *Service) SetValidatorEpochSummary(ctx context.Context, summary *chaindb.ValidatorEpochSummary) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SetValidatorEpochSummary")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	var attestationTargetCorrect sql.NullBool
	var attestationHeadCorrect sql.NullBool
	var attestationInclusionDelay sql.NullInt32
	var attestationSourceTimely sql.NullBool
	var attestationTargetTimely sql.NullBool
	var attestationHeadTimely sql.NullBool

	if summary.AttestationTargetCorrect != nil {
		attestationTargetCorrect.Valid = true
		attestationTargetCorrect.Bool = *summary.AttestationTargetCorrect
	}
	if summary.AttestationHeadCorrect != nil {
		attestationHeadCorrect.Valid = true
		attestationHeadCorrect.Bool = *summary.AttestationHeadCorrect
	}
	if summary.AttestationInclusionDelay != nil {
		attestationInclusionDelay.Valid = true
		attestationInclusionDelay.Int32 = int32(*summary.AttestationInclusionDelay)
	}
	if summary.AttestationSourceTimely != nil {
		attestationSourceTimely.Valid = true
		attestationSourceTimely.Bool = *summary.AttestationSourceTimely
	}
	if summary.AttestationTargetTimely != nil {
		attestationTargetTimely.Valid = true
		attestationTargetTimely.Bool = *summary.AttestationTargetTimely
	}
	if summary.AttestationHeadTimely != nil {
		attestationHeadTimely.Valid = true
		attestationHeadTimely.Bool = *summary.AttestationHeadTimely
	}

	_, err := tx.Exec(ctx, `
      INSERT INTO t_validator_epoch_summaries(f_validator_index
                              ,f_epoch
                              ,f_proposer_duties
                              ,f_proposals_included
                              ,f_attestation_included
                              ,f_attestation_target_correct
                              ,f_attestation_head_correct
                              ,f_attestation_inclusion_delay
                              ,f_attestation_source_timely
                              ,f_attestation_target_timely
                              ,f_attestation_head_timely)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
      ON CONFLICT (f_validator_index,f_epoch) DO
      UPDATE
      SET f_proposer_duties = excluded.f_proposer_duties
         ,f_proposals_included = excluded.f_proposals_included
         ,f_attestation_included = excluded.f_attestation_included
         ,f_attestation_target_correct = excluded.f_attestation_target_correct
         ,f_attestation_head_correct = excluded.f_attestation_head_correct
         ,f_attestation_inclusion_delay = excluded.f_attestation_inclusion_delay
         ,f_attestation_source_timely = excluded.f_attestation_source_timely
         ,f_attestation_target_timely = excluded.f_attestation_target_timely
         ,f_attestation_head_timely = excluded.f_attestation_head_timely
		 `,
		summary.Index,
		summary.Epoch,
		summary.ProposerDuties,
		summary.ProposalsIncluded,
		summary.AttestationIncluded,
		attestationTargetCorrect,
		attestationHeadCorrect,
		attestationInclusionDelay,
		attestationSourceTimely,
		attestationTargetTimely,
		attestationHeadTimely,
	)

	return err
}

// ValidatorSummaries provides summaries according to the filter.
func (s *Service) ValidatorSummaries(ctx context.Context, filter *chaindb.ValidatorSummaryFilter) ([]*chaindb.ValidatorEpochSummary, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "ValidatorSummaries")
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
SELECT f_validator_index
      ,f_epoch
      ,f_proposer_duties
      ,f_proposals_included
      ,f_attestation_included
      ,f_attestation_target_correct
      ,f_attestation_head_correct
      ,f_attestation_inclusion_delay
      ,f_attestation_source_timely
      ,f_attestation_target_timely
      ,f_attestation_head_timely
FROM t_validator_epoch_summaries`)

	wherestr := "WHERE"

	if filter.From != nil {
		queryVals = append(queryVals, *filter.From)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_epoch >= $%d`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if filter.To != nil {
		queryVals = append(queryVals, *filter.To)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_epoch <= $%d`, wherestr, len(queryVals)))
	}

	if filter.ValidatorIndices != nil && len(*filter.ValidatorIndices) > 0 {
		queryVals = append(queryVals, *filter.ValidatorIndices)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_validator_index = ANY($%d)`, wherestr, len(queryVals)))
	}

	switch filter.Order {
	case chaindb.OrderEarliest:
		queryBuilder.WriteString(`
ORDER BY f_epoch, f_validator_index`)
	case chaindb.OrderLatest:
		queryBuilder.WriteString(`
ORDER BY f_epoch DESC,f_validator_index DESC`)
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

	summaries := make([]*chaindb.ValidatorEpochSummary, 0)
	for rows.Next() {
		summary := &chaindb.ValidatorEpochSummary{}
		var attestationTargetCorrect sql.NullBool
		var attestationHeadCorrect sql.NullBool
		var attestationInclusionDelay sql.NullInt32
		var attestationSourceTimely sql.NullBool
		var attestationTargetTimely sql.NullBool
		var attestationHeadTimely sql.NullBool
		err := rows.Scan(
			&summary.Index,
			&summary.Epoch,
			&summary.ProposerDuties,
			&summary.ProposalsIncluded,
			&summary.AttestationIncluded,
			&attestationTargetCorrect,
			&attestationHeadCorrect,
			&attestationInclusionDelay,
			&attestationSourceTimely,
			&attestationTargetTimely,
			&attestationHeadTimely,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		if attestationTargetCorrect.Valid {
			val := attestationTargetCorrect.Bool
			summary.AttestationTargetCorrect = &val
		}
		if attestationHeadCorrect.Valid {
			val := attestationHeadCorrect.Bool
			summary.AttestationHeadCorrect = &val
		}
		if attestationInclusionDelay.Valid {
			val := int(attestationInclusionDelay.Int32)
			summary.AttestationInclusionDelay = &val
		}
		if attestationSourceTimely.Valid {
			val := attestationSourceTimely.Bool
			summary.AttestationSourceTimely = &val
		}
		if attestationTargetTimely.Valid {
			val := attestationTargetTimely.Bool
			summary.AttestationTargetTimely = &val
		}
		if attestationHeadTimely.Valid {
			val := attestationHeadTimely.Bool
			summary.AttestationHeadTimely = &val
		}
		summaries = append(summaries, summary)
	}

	// Always return order of epoch then validator index.
	sort.Slice(summaries, func(i int, j int) bool {
		if summaries[i].Epoch != summaries[j].Epoch {
			return summaries[i].Epoch < summaries[j].Epoch
		}
		return summaries[i].Index < summaries[j].Index
	})
	return summaries, nil
}

// ValidatorSummariesForEpoch obtains all summaries for a given epoch.
func (s *Service) ValidatorSummariesForEpoch(ctx context.Context, epoch phase0.Epoch) ([]*chaindb.ValidatorEpochSummary, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "ValidatorSummariesForEpoch")
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
SELECT f_validator_index
      ,f_epoch
      ,f_proposer_duties
      ,f_proposals_included
      ,f_attestation_included
      ,f_attestation_target_correct
      ,f_attestation_head_correct
      ,f_attestation_inclusion_delay
      ,f_attestation_source_timely
      ,f_attestation_target_timely
      ,f_attestation_head_timely
FROM t_validator_epoch_summaries
WHERE f_epoch = $1
ORDER BY f_validator_index
`,
		epoch,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	summaries := make([]*chaindb.ValidatorEpochSummary, 0)

	for rows.Next() {
		summary := &chaindb.ValidatorEpochSummary{}
		var attestationTargetCorrect sql.NullBool
		var attestationHeadCorrect sql.NullBool
		var attestationInclusionDelay sql.NullInt32
		var attestationSourceTimely sql.NullBool
		var attestationTargetTimely sql.NullBool
		var attestationHeadTimely sql.NullBool
		err := rows.Scan(
			&summary.Index,
			&summary.Epoch,
			&summary.ProposerDuties,
			&summary.ProposalsIncluded,
			&summary.AttestationIncluded,
			&attestationTargetCorrect,
			&attestationHeadCorrect,
			&attestationInclusionDelay,
			&attestationSourceTimely,
			&attestationTargetTimely,
			&attestationHeadTimely,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		if attestationTargetCorrect.Valid {
			val := attestationTargetCorrect.Bool
			summary.AttestationTargetCorrect = &val
		}
		if attestationHeadCorrect.Valid {
			val := attestationHeadCorrect.Bool
			summary.AttestationHeadCorrect = &val
		}
		if attestationInclusionDelay.Valid {
			val := int(attestationInclusionDelay.Int32)
			summary.AttestationInclusionDelay = &val
		}
		if attestationSourceTimely.Valid {
			val := attestationSourceTimely.Bool
			summary.AttestationSourceTimely = &val
		}
		if attestationTargetTimely.Valid {
			val := attestationTargetTimely.Bool
			summary.AttestationTargetTimely = &val
		}
		if attestationHeadTimely.Valid {
			val := attestationHeadTimely.Bool
			summary.AttestationHeadTimely = &val
		}
		summaries = append(summaries, summary)
	}

	return summaries, nil
}

// ValidatorSummaryForEpoch obtains the summary of a validator for a given epoch.
func (s *Service) ValidatorSummaryForEpoch(ctx context.Context,
	index phase0.ValidatorIndex,
	epoch phase0.Epoch,
) (
	*chaindb.ValidatorEpochSummary,
	error,
) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "ValidatorSummaryForEpoch")
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

	summary := &chaindb.ValidatorEpochSummary{}
	var attestationTargetCorrect sql.NullBool
	var attestationHeadCorrect sql.NullBool
	var attestationInclusionDelay sql.NullInt32
	var attestationSourceTimely sql.NullBool
	var attestationTargetTimely sql.NullBool
	var attestationHeadTimely sql.NullBool

	err := tx.QueryRow(ctx, `
SELECT f_validator_index
      ,f_epoch
      ,f_proposer_duties
      ,f_proposals_included
      ,f_attestation_included
      ,f_attestation_target_correct
      ,f_attestation_head_correct
      ,f_attestation_inclusion_delay
      ,f_attestation_source_timely
      ,f_attestation_target_timely
      ,f_attestation_head_timely
FROM t_validator_epoch_summaries
WHERE f_validator_index = $1
  AND f_epoch = $2
`,
		index,
		epoch,
	).Scan(
		&summary.Index,
		&summary.Epoch,
		&summary.ProposerDuties,
		&summary.ProposalsIncluded,
		&summary.AttestationIncluded,
		&attestationTargetCorrect,
		&attestationHeadCorrect,
		&attestationInclusionDelay,
		&attestationSourceTimely,
		&attestationTargetTimely,
		&attestationHeadTimely,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to scan row")
	}

	if attestationTargetCorrect.Valid {
		val := attestationTargetCorrect.Bool
		summary.AttestationTargetCorrect = &val
	}
	if attestationHeadCorrect.Valid {
		val := attestationHeadCorrect.Bool
		summary.AttestationHeadCorrect = &val
	}
	if attestationInclusionDelay.Valid {
		val := int(attestationInclusionDelay.Int32)
		summary.AttestationInclusionDelay = &val
	}
	if attestationSourceTimely.Valid {
		val := attestationSourceTimely.Bool
		summary.AttestationSourceTimely = &val
	}
	if attestationTargetTimely.Valid {
		val := attestationTargetTimely.Bool
		summary.AttestationTargetTimely = &val
	}
	if attestationHeadTimely.Valid {
		val := attestationHeadTimely.Bool
		summary.AttestationHeadTimely = &val
	}

	return summary, nil
}

// PruneValidatorEpochSummaries prunes validator epoch summaries up to (but not including) the given point.
func (s *Service) PruneValidatorEpochSummaries(ctx context.Context, to phase0.Epoch, retain []phase0.ValidatorIndex) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "PruneValidatorEpochSummaries")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	// Build the query.
	queryBuilder := strings.Builder{}
	queryVals := make([]any, 0)

	queryBuilder.WriteString(`
DELETE FROM t_validator_epoch_summaries
WHERE f_epoch <= $1
`)
	queryVals = append(queryVals, to)

	if len(retain) > 0 {
		queryBuilder.WriteString(`
AND  f_validator_index NOT IN($2)
`)
		queryVals = append(queryVals, retain)
	}

	if e := log.Trace(); e.Enabled() {
		params := make([]string, len(queryVals))
		for i := range queryVals {
			params[i] = fmt.Sprintf("%v", queryVals[i])
		}
		e.Str("statement", strings.ReplaceAll(queryBuilder.String(), "\n", " ")).Strs("params", params).Msg("SQL statement")
	}

	_, err := tx.Exec(ctx,
		queryBuilder.String(),
		queryVals...,
	)

	return err
}
