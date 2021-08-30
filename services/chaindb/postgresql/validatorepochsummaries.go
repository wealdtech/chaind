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

	"github.com/wealdtech/chaind/services/chaindb"
)

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
