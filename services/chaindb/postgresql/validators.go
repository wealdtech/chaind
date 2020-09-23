// Copyright © 2020 Weald Technology Trading.
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
)

// SetValidator sets a validator.
func (s *Service) SetValidator(ctx context.Context, validator *chaindb.Validator) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.Exec(ctx, `
      INSERT INTO t_validators(f_public_key
                              ,f_index
                              ,f_slashed
                              ,f_activation_eligibility_epoch
                              ,f_activation_epoch
                              ,f_exit_epoch
                              ,f_withdrawable_epoch
                              ,f_effective_balance)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8)
      ON CONFLICT (f_index) DO
      UPDATE
      SET f_slashed = excluded.f_slashed
         ,f_activation_eligibility_epoch = excluded.f_activation_eligibility_epoch
         ,f_activation_epoch = excluded.f_activation_epoch
         ,f_exit_epoch = excluded.f_exit_epoch
         ,f_withdrawable_epoch = excluded.f_withdrawable_epoch
         ,f_effective_balance = excluded.f_effective_balance
		 `,
		validator.PublicKey,
		validator.Index,
		validator.Slashed,
		int64(validator.ActivationEligibilityEpoch),
		int64(validator.ActivationEpoch),
		int64(validator.WithdrawableEpoch),
		int64(validator.WithdrawableEpoch),
		validator.EffectiveBalance,
	)

	return err
}

// SetValidatorBalance sets a validator's balance.
func (s *Service) SetValidatorBalance(ctx context.Context, balance *chaindb.ValidatorBalance) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.Exec(ctx, `
      INSERT INTO t_validator_balances(f_validator_index
                                      ,f_epoch
                                      ,f_balance
                                      ,f_effective_balance)
      VALUES($1,$2,$3,$4)
      ON CONFLICT (f_validator_index, f_epoch) DO
      UPDATE
      SET f_balance = excluded.f_balance
         ,f_effective_balance = excluded.f_effective_balance
		 `,
		balance.Index,
		balance.Epoch,
		balance.Balance,
		balance.EffectiveBalance,
	)

	return err
}
