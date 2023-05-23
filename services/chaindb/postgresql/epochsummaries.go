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
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
	"go.opentelemetry.io/otel"
)

// SetEpochSummary sets an epoch summary.
func (s *Service) SetEpochSummary(ctx context.Context, summary *chaindb.EpochSummary) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SetEpochSummary")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.Exec(ctx, `
      INSERT INTO t_epoch_summaries(f_epoch
                                   ,f_activation_queue_length
                                   ,f_activating_validators
                                   ,f_active_validators
                                   ,f_active_real_balance
                                   ,f_active_balance
                                   ,f_attesting_validators
                                   ,f_attesting_balance
                                   ,f_target_correct_validators
                                   ,f_target_correct_balance
                                   ,f_head_correct_validators
                                   ,f_head_correct_balance
                                   ,f_attestations_for_epoch
                                   ,f_attestations_in_epoch
                                   ,f_duplicate_attestations_for_epoch
                                   ,f_proposer_slashings
                                   ,f_attester_slashings
                                   ,f_deposits
                                   ,f_exiting_validators
                                   ,f_canonical_blocks)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
      ON CONFLICT (f_epoch) DO
      UPDATE
      SET f_activation_queue_length = excluded.f_activation_queue_length
         ,f_activating_validators = excluded.f_activating_validators
         ,f_active_validators = excluded.f_active_validators
         ,f_active_real_balance = excluded.f_active_real_balance
         ,f_active_balance = excluded.f_active_balance
         ,f_attesting_validators = excluded.f_attesting_validators
         ,f_attesting_balance = excluded.f_attesting_balance
         ,f_target_correct_validators = excluded.f_target_correct_validators
         ,f_target_correct_balance = excluded.f_target_correct_balance
         ,f_head_correct_validators = excluded.f_head_correct_validators
         ,f_head_correct_balance = excluded.f_head_correct_balance
         ,f_attestations_for_epoch = excluded.f_attestations_for_epoch
         ,f_attestations_in_epoch = excluded.f_attestations_in_epoch
         ,f_duplicate_attestations_for_epoch = excluded.f_duplicate_attestations_for_epoch
         ,f_proposer_slashings = excluded.f_proposer_slashings
         ,f_attester_slashings = excluded.f_attester_slashings
         ,f_deposits = excluded.f_deposits
         ,f_exiting_validators = excluded.f_exiting_validators
         ,f_canonical_blocks = excluded.f_canonical_blocks
		 `,
		summary.Epoch,
		summary.ActivationQueueLength,
		summary.ActivatingValidators,
		summary.ActiveValidators,
		summary.ActiveRealBalance,
		summary.ActiveBalance,
		summary.AttestingValidators,
		summary.AttestingBalance,
		summary.TargetCorrectValidators,
		summary.TargetCorrectBalance,
		summary.HeadCorrectValidators,
		summary.HeadCorrectBalance,
		summary.AttestationsForEpoch,
		summary.AttestationsInEpoch,
		summary.DuplicateAttestationsForEpoch,
		summary.ProposerSlashings,
		summary.AttesterSlashings,
		summary.Deposits,
		summary.ExitingValidators,
		summary.CanonicalBlocks,
	)

	return err
}

// EpochSummaries provides summaries according to the filter.
func (s *Service) EpochSummaries(ctx context.Context, filter *chaindb.EpochSummaryFilter) ([]*chaindb.EpochSummary, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "EpochSummaries")
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
SELECT f_epoch
      ,f_activation_queue_length
      ,f_activating_validators
      ,f_active_validators
      ,f_active_real_balance
      ,f_active_balance
      ,f_attesting_validators
      ,f_attesting_balance
      ,f_target_correct_validators
      ,f_target_correct_balance
      ,f_head_correct_validators
      ,f_head_correct_balance
      ,f_attestations_for_epoch
      ,f_attestations_in_epoch
      ,f_duplicate_attestations_for_epoch
      ,f_proposer_slashings
      ,f_attester_slashings
      ,f_deposits
      ,f_exiting_validators
      ,f_canonical_blocks
FROM t_epoch_summaries`)

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

	switch filter.Order {
	case chaindb.OrderEarliest:
		queryBuilder.WriteString(`
ORDER BY f_epoch`)
	case chaindb.OrderLatest:
		queryBuilder.WriteString(`
ORDER BY f_epoch DESC`)
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

	summaries := make([]*chaindb.EpochSummary, 0)
	for rows.Next() {
		summary := &chaindb.EpochSummary{}
		err := rows.Scan(
			&summary.Epoch,
			&summary.ActivationQueueLength,
			&summary.ActivatingValidators,
			&summary.ActiveValidators,
			&summary.ActiveRealBalance,
			&summary.ActiveBalance,
			&summary.AttestingValidators,
			&summary.AttestingBalance,
			&summary.TargetCorrectValidators,
			&summary.TargetCorrectBalance,
			&summary.HeadCorrectValidators,
			&summary.HeadCorrectBalance,
			&summary.AttestationsForEpoch,
			&summary.AttestationsInEpoch,
			&summary.DuplicateAttestationsForEpoch,
			&summary.ProposerSlashings,
			&summary.AttesterSlashings,
			&summary.Deposits,
			&summary.ExitingValidators,
			&summary.CanonicalBlocks,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		summaries = append(summaries, summary)
	}

	// Always return order of epoch.
	sort.Slice(summaries, func(i int, j int) bool {
		return summaries[i].Epoch < summaries[j].Epoch
	})
	return summaries, nil
}
