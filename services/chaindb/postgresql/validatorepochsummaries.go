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
	"database/sql"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
)

// SetValidatorEpochSummaries sets multiple validator epoch summaries.
func (s *Service) SetValidatorEpochSummaries(ctx context.Context, summaries []*chaindb.ValidatorEpochSummary) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}
	_, err := tx.CopyFrom(ctx,
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
		pgx.CopyFromSlice(len(summaries), func(i int) ([]interface{}, error) {
			return []interface{}{
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

	return err
}

// SetValidatorEpochSummary sets a validator epoch summary.
func (s *Service) SetValidatorEpochSummary(ctx context.Context, summary *chaindb.ValidatorEpochSummary) error {
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

// ValidatorSummariesForEpoch obtains all summaries for a given epoch.
func (s *Service) ValidatorSummariesForEpoch(ctx context.Context, epoch phase0.Epoch) ([]*chaindb.ValidatorEpochSummary, error) {
	tx := s.tx(ctx)
	if tx == nil {
		ctx, cancel, err := s.BeginTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer cancel()
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
