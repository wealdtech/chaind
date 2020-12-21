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

// Validators fetches the validators.
func (s *Service) Validators(ctx context.Context) ([]*chaindb.Validator, error) {
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

	var publicKey []byte
	var activationEligibilityEpoch int64
	var activationEpoch int64
	var exitEpoch int64
	var withdrawableEpoch int64
	for rows.Next() {
		validator := &chaindb.Validator{}
		err := rows.Scan(
			&publicKey,
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
		copy(validator.PublicKey[:], publicKey)
		validator.ActivationEligibilityEpoch = spec.Epoch(activationEligibilityEpoch)
		validator.ActivationEpoch = spec.Epoch(activationEpoch)
		validator.ExitEpoch = spec.Epoch(exitEpoch)
		validator.WithdrawableEpoch = spec.Epoch(withdrawableEpoch)
		validators = append(validators, validator)
	}

	return validators, nil
}

// ValidatorsByPublicKey fetches all validators matching the given public keys.
func (s *Service) ValidatorsByPublicKey(ctx context.Context, pubKeys []spec.BLSPubKey) (map[spec.BLSPubKey]*chaindb.Validator, error) {
	tx := s.tx(ctx)
	if tx == nil {
		ctx, cancel, err := s.BeginTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer cancel()
	}

	sqlPubKeys := make([][]byte, len(pubKeys))
	for i := range pubKeys {
		sqlPubKeys[i] = pubKeys[i][:]
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
      WHERE f_public_key = ANY($1)
      ORDER BY f_index
	  `,
		sqlPubKeys,
	)
	if err != nil {
		return nil, err
	}

	validators := make(map[spec.BLSPubKey]*chaindb.Validator)
	var publicKey []byte
	var activationEligibilityEpoch int64
	var activationEpoch int64
	var exitEpoch int64
	var withdrawableEpoch int64
	for rows.Next() {
		validator := &chaindb.Validator{}
		err := rows.Scan(
			&publicKey,
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
		copy(validator.PublicKey[:], publicKey)
		validator.ActivationEligibilityEpoch = spec.Epoch(activationEligibilityEpoch)
		validator.ActivationEpoch = spec.Epoch(activationEpoch)
		validator.ExitEpoch = spec.Epoch(exitEpoch)
		validator.WithdrawableEpoch = spec.Epoch(withdrawableEpoch)
		validators[validator.PublicKey] = validator
	}

	return validators, nil
}

// ValidatorsByIndex fetches all validators matching the given indices.
func (s *Service) ValidatorsByIndex(ctx context.Context, indices []spec.ValidatorIndex) (map[spec.ValidatorIndex]*chaindb.Validator, error) {
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
      WHERE f_index = ANY($1)
      ORDER BY f_index
	  `,
		indices,
	)
	if err != nil {
		return nil, err
	}

	validators := make(map[spec.ValidatorIndex]*chaindb.Validator)
	var publicKey []byte
	var activationEligibilityEpoch int64
	var activationEpoch int64
	var exitEpoch int64
	var withdrawableEpoch int64
	for rows.Next() {
		validator := &chaindb.Validator{}
		err := rows.Scan(
			&publicKey,
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
		copy(validator.PublicKey[:], publicKey)
		validator.ActivationEligibilityEpoch = spec.Epoch(activationEligibilityEpoch)
		validator.ActivationEpoch = spec.Epoch(activationEpoch)
		validator.ExitEpoch = spec.Epoch(exitEpoch)
		validator.WithdrawableEpoch = spec.Epoch(withdrawableEpoch)
		validators[validator.Index] = validator
	}

	return validators, nil
}

// ValidatorBalancesByIndexAndEpoch fetches the validator balances for the given validators and epoch.
func (s *Service) ValidatorBalancesByIndexAndEpoch(
	ctx context.Context,
	validatorIndices []spec.ValidatorIndex,
	epoch spec.Epoch,
) (
	map[spec.ValidatorIndex]*chaindb.ValidatorBalance,
	error,
) {
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
            ,f_balance
            ,f_effective_balance
      FROM t_validator_balances
      WHERE f_validator_index = ANY($1)
		AND f_epoch = $2::BIGINT
      ORDER BY f_validator_index
	  `,
		validatorIndices,
		uint64(epoch),
	)
	if err != nil {
		return nil, err
	}

	validatorBalances := make(map[spec.ValidatorIndex]*chaindb.ValidatorBalance, len(validatorIndices))

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
		validatorBalances[validatorBalance.Index] = validatorBalance
	}

	return validatorBalances, nil
}

// ValidatorBalancesByIndexAndEpochRange fetches the validator balances for the given validators and epoch range.
// Ranges are inclusive of start and exclusive of end i.e. a request with startEpoch 2 and endEpoch 4 will provide
// balances for epochs 2 and 3.
func (s *Service) ValidatorBalancesByIndexAndEpochRange(
	ctx context.Context,
	validatorIndices []spec.ValidatorIndex,
	startEpoch spec.Epoch,
	endEpoch spec.Epoch,
) (
	map[spec.ValidatorIndex][]*chaindb.ValidatorBalance,
	error,
) {
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
            ,f_balance
            ,f_effective_balance
      FROM t_validator_balances
      WHERE f_validator_index = ANY($1)
		AND f_epoch >= $2
		AND f_epoch < $3
      ORDER BY f_validator_index
	          ,f_epoch
	  `,
		validatorIndices,
		uint64(startEpoch),
		uint64(endEpoch),
	)
	if err != nil {
		return nil, err
	}

	validatorBalances := make(map[spec.ValidatorIndex][]*chaindb.ValidatorBalance, len(validatorIndices))
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
		_, exists := validatorBalances[validatorBalance.Index]
		if !exists {
			validatorBalances[validatorBalance.Index] = make([]*chaindb.ValidatorBalance, 0, endEpoch+1-startEpoch)
		}
		validatorBalances[validatorBalance.Index] = append(validatorBalances[validatorBalance.Index], validatorBalance)
	}

	// If a validator is not present until after the beginning of the range, for example we ask for epochs 5->10 and
	// the validator is first present at epoch 7, we need to front-pad the data for that validator with 0s.
	padValidatorBalances(ctx, validatorBalances, int(uint64(endEpoch)-uint64(startEpoch)), startEpoch)

	return validatorBalances, nil
}
func padValidatorBalances(ctx context.Context, validatorBalances map[spec.ValidatorIndex][]*chaindb.ValidatorBalance, entries int, startEpoch spec.Epoch) {
	for validatorIndex, balances := range validatorBalances {
		if len(balances) != entries {
			paddedBalances := make([]*chaindb.ValidatorBalance, entries)
			padding := entries - len(balances)
			for i := 0; i < padding; i++ {
				paddedBalances[i] = &chaindb.ValidatorBalance{
					Index:            validatorIndex,
					Epoch:            startEpoch + spec.Epoch(i),
					Balance:          0,
					EffectiveBalance: 0,
				}
			}
			copy(paddedBalances[padding:], balances)
			validatorBalances[validatorIndex] = paddedBalances
		}
	}
}
