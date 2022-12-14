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
