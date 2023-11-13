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

	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
	"go.opentelemetry.io/otel"
)

// SetValidatorDaySummaries sets multiple validator day summaries.
func (s *Service) SetValidatorDaySummaries(ctx context.Context, summaries []*chaindb.ValidatorDaySummary) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SetValidatorDaySummaries")
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
		pgx.Identifier{"t_validator_day_summaries"},
		[]string{
			"f_validator_index",
			"f_start_timestamp",
			"f_start_balance",
			"f_start_effective_balance",
			"f_capital_change",
			"f_reward_change",
			"f_withdrawals",
			"f_effective_balance_change",
			"f_proposals",
			"f_proposals_included",
			"f_attestations",
			"f_attestations_included",
			"f_attestations_target_correct",
			"f_attestations_head_correct",
			"f_attestations_source_timely",
			"f_attestations_target_timely",
			"f_attestations_head_timely",
			"f_attestations_inclusion_delay",
			"f_sync_committee_messages",
			"f_sync_committee_messages_included",
		},
		pgx.CopyFromSlice(len(summaries), func(i int) ([]any, error) {
			return []any{
				summaries[i].Index,
				summaries[i].StartTimestamp,
				summaries[i].StartBalance,
				summaries[i].StartEffectiveBalance,
				summaries[i].CapitalChange,
				summaries[i].RewardChange,
				summaries[i].Withdrawals,
				summaries[i].EffectiveBalanceChange,
				summaries[i].Proposals,
				summaries[i].ProposalsIncluded,
				summaries[i].Attestations,
				summaries[i].AttestationsIncluded,
				summaries[i].AttestationsTargetCorrect,
				summaries[i].AttestationsHeadCorrect,
				summaries[i].AttestationsSourceTimely,
				summaries[i].AttestationsTargetTimely,
				summaries[i].AttestationsHeadTimely,
				summaries[i].AttestationsInclusionDelay,
				summaries[i].SyncCommitteeMessages,
				summaries[i].SyncCommitteeMessagesIncluded,
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

		log.Debug().Err(err).Msg("Failed to copy insert validator day summaries; applying one at a time")
		for _, summary := range summaries {
			if err := s.SetValidatorDaySummary(ctx, summary); err != nil {
				log.Error().Err(err).Msg("Failure to insert individual validator day summary")
				return err
			}
		}

		// Succeeded so clear the error.
		err = nil
	}

	return err
}

// SetValidatorDaySummary sets a validator day summary.
func (s *Service) SetValidatorDaySummary(ctx context.Context, summary *chaindb.ValidatorDaySummary) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SetValidatorDaySummary")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.Exec(ctx, `
INSERT INTO t_validator_day_summaries(f_validator_index
                                     ,f_start_timestamp
                                     ,f_start_balance
                                     ,f_start_effective_balance
                                     ,f_capital_change
                                     ,f_reward_change
                                     ,f_withdrawals
                                     ,f_effective_balance_change
                                     ,f_proposals
                                     ,f_proposals_included
                                     ,f_attestations
                                     ,f_attestations_included
                                     ,f_attestations_target_correct
                                     ,f_attestations_head_correct
                                     ,f_attestations_source_timely
                                     ,f_attestations_target_timely
                                     ,f_attestations_head_timely
                                     ,f_attestations_inclusion_delay
                                     ,f_sync_committee_messages
                                     ,f_sync_committee_messages_included)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
ON CONFLICT (f_validator_index,f_start_timestamp) DO
UPDATE
SET f_start_balance = excluded.f_start_balance
   ,f_start_effective_balance = excluded.f_start_effective_balance
   ,f_capital_change = excluded.f_capital_change
   ,f_reward_change = excluded.f_reward_change
   ,f_withdrawals = excluded.f_withdrawals
   ,f_effective_balance_change = excluded.f_effective_balance_change
   ,f_proposals = excluded.f_proposals
   ,f_proposals_included = excluded.f_proposals_included
   ,f_attestations = excluded.f_attestations
   ,f_attestations_included = excluded.f_attestations_included
   ,f_attestations_target_correct = excluded.f_attestations_target_correct
   ,f_attestations_head_correct = excluded.f_attestations_head_correct
   ,f_attestations_source_timely = excluded.f_attestations_source_timely
   ,f_attestations_target_timely = excluded.f_attestations_target_timely
   ,f_attestations_head_timely = excluded.f_attestations_head_timely
   ,f_attestations_inclusion_delay = excluded.f_attestations_inclusion_delay
   ,f_sync_committee_messages = excluded.f_sync_committee_messages
   ,f_sync_committee_messages_included = excluded.f_sync_committee_messages_included
     `,
		summary.Index,
		summary.StartTimestamp,
		summary.StartBalance,
		summary.StartEffectiveBalance,
		summary.CapitalChange,
		summary.RewardChange,
		summary.Withdrawals,
		summary.EffectiveBalanceChange,
		summary.Proposals,
		summary.ProposalsIncluded,
		summary.Attestations,
		summary.AttestationsIncluded,
		summary.AttestationsTargetCorrect,
		summary.AttestationsHeadCorrect,
		summary.AttestationsSourceTimely,
		summary.AttestationsTargetTimely,
		summary.AttestationsHeadTimely,
		summary.AttestationsInclusionDelay,
		summary.SyncCommitteeMessages,
		summary.SyncCommitteeMessagesIncluded,
	)

	return err
}

// ValidatorDaySummaries provides validator day summaries according to the filter.
func (s *Service) ValidatorDaySummaries(ctx context.Context, filter *chaindb.ValidatorDaySummaryFilter) ([]*chaindb.ValidatorDaySummary, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "ValidatorDaySummaries")
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
      ,f_start_timestamp
      ,f_start_balance
      ,f_start_effective_balance
      ,f_capital_change
      ,f_reward_change
      ,f_withdrawals
      ,f_effective_balance_change
      ,f_proposals
      ,f_proposals_included
      ,f_attestations
      ,f_attestations_included
      ,f_attestations_target_correct
      ,f_attestations_head_correct
      ,f_attestations_source_timely
      ,f_attestations_target_timely
      ,f_attestations_head_timely
      ,f_attestations_inclusion_delay
      ,f_sync_committee_messages
      ,f_sync_committee_messages_included
FROM t_validator_day_summaries`)

	wherestr := "WHERE"

	if filter.From != nil {
		queryVals = append(queryVals, *filter.From)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_start_timestamp >= $%d`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if filter.To != nil {
		queryVals = append(queryVals, *filter.To)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_start_timestamp <= $%d`, wherestr, len(queryVals)))
	}

	if filter.ValidatorIndices != nil && len(*filter.ValidatorIndices) > 0 {
		queryVals = append(queryVals, *filter.ValidatorIndices)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_validator_index = ANY($%d)`, wherestr, len(queryVals)))
	}

	switch filter.Order {
	case chaindb.OrderEarliest:
		queryBuilder.WriteString(`
ORDER BY f_start_timestamp, f_validator_index`)
	case chaindb.OrderLatest:
		queryBuilder.WriteString(`
ORDER BY f_start_timestamp DESC,f_validator_index DESC`)
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

	summaries := make([]*chaindb.ValidatorDaySummary, 0)
	for rows.Next() {
		summary := &chaindb.ValidatorDaySummary{}
		err := rows.Scan(
			&summary.Index,
			&summary.StartTimestamp,
			&summary.StartBalance,
			&summary.StartEffectiveBalance,
			&summary.CapitalChange,
			&summary.RewardChange,
			&summary.Withdrawals,
			&summary.EffectiveBalanceChange,
			&summary.Proposals,
			&summary.ProposalsIncluded,
			&summary.Attestations,
			&summary.AttestationsIncluded,
			&summary.AttestationsTargetCorrect,
			&summary.AttestationsHeadCorrect,
			&summary.AttestationsSourceTimely,
			&summary.AttestationsTargetTimely,
			&summary.AttestationsHeadTimely,
			&summary.AttestationsInclusionDelay,
			&summary.SyncCommitteeMessages,
			&summary.SyncCommitteeMessagesIncluded,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		summaries = append(summaries, summary)
	}

	// Always return order of start timestamp then validator index.
	sort.Slice(summaries, func(i int, j int) bool {
		if summaries[i].StartTimestamp != summaries[j].StartTimestamp {
			return summaries[i].StartTimestamp.Before(summaries[j].StartTimestamp)
		}
		return summaries[i].Index < summaries[j].Index
	})
	return summaries, nil
}
