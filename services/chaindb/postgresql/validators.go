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

	spec "github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
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
		validator.PublicKey[:],
		validator.Index,
		validator.Slashed,
		int64(validator.ActivationEligibilityEpoch),
		int64(validator.ActivationEpoch),
		int64(validator.ExitEpoch),
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

// GetValidators fetches the validators.
func (s *Service) GetValidators(ctx context.Context) ([]*chaindb.Validator, error) {
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
      SELECT f_public_key
            ,f_index
            ,f_slashed
            ,f_activation_eligibility_epoch
            ,f_activation_epoch
            ,f_exit_epoch
            ,f_withdrawable_epoch
            ,f_effective_balance
      FROM t_validators
      ORDER BY f_index
	  `,
	)
	if err != nil {
		return nil, err
	}

	validators := make([]*chaindb.Validator, 0)

	var activationEligibilityEpoch int64
	var activationEpoch int64
	var exitEpoch int64
	var withdrawableEpoch int64
	for rows.Next() {
		validator := &chaindb.Validator{}
		err := rows.Scan(
			&validator.PublicKey,
			&validator.Index,
			&validator.Slashed,
			&activationEligibilityEpoch,
			&activationEpoch,
			&exitEpoch,
			&withdrawableEpoch,
			&validator.EffectiveBalance,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		validator.ActivationEligibilityEpoch = spec.Epoch(activationEligibilityEpoch)
		validator.ActivationEpoch = spec.Epoch(activationEpoch)
		validator.ExitEpoch = spec.Epoch(exitEpoch)
		validator.WithdrawableEpoch = spec.Epoch(withdrawableEpoch)
		validators = append(validators, validator)
	}

	return validators, nil
}

// GetValidatorBalancesByValidatorsAndEpoch fetches the validator balances for the given validators and epoch.
func (s *Service) GetValidatorBalancesByValidatorsAndEpoch(ctx context.Context, validators []*chaindb.Validator, epoch uint64) ([]*chaindb.ValidatorBalance, error) {
	tx := s.tx(ctx)
	if tx == nil {
		ctx, cancel, err := s.BeginTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer cancel()
	}

	validatorIndices := make([]spec.ValidatorIndex, len(validators))
	for i := range validators {
		validatorIndices[i] = validators[i].Index
	}

	rows, err := tx.Query(ctx, `
      SELECT f_validator_index
            ,f_epoch
            ,f_balance
            ,f_effective_balance
      FROM t_validator_balances
      WHERE f_validator_index = ANY($1)
        AND f_epoch = $2
      ORDER BY f_validator_index
	  `,
		validatorIndices,
		epoch,
	)
	if err != nil {
		return nil, err
	}

	validatorBalances := make([]*chaindb.ValidatorBalance, 0, len(validatorIndices))

	for rows.Next() {
		validatorBalance := &chaindb.ValidatorBalance{}
		err := rows.Scan(
			&validatorBalance.Index,
			&validatorBalance.Epoch,
			&validatorBalance.Balance,
			&validatorBalance.EffectiveBalance,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		validatorBalances = append(validatorBalances, validatorBalance)
	}

	return validatorBalances, nil
}
