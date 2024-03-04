// Copyright © 2020 - 2023 Weald Technology Trading.
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
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
	"go.opentelemetry.io/otel"
)

var farFutureEpoch = phase0.Epoch(0xffffffffffffffff)

// SetValidator sets a validator.
func (s *Service) SetValidator(ctx context.Context, validator *chaindb.Validator) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SetValidator")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	var activationEligibilityEpoch sql.NullInt64
	var activationEpoch sql.NullInt64
	var exitEpoch sql.NullInt64
	var withdrawableEpoch sql.NullInt64

	if validator.ActivationEligibilityEpoch != farFutureEpoch {
		activationEligibilityEpoch.Valid = true
		activationEligibilityEpoch.Int64 = (int64)(validator.ActivationEligibilityEpoch)
	}
	if validator.ActivationEpoch != farFutureEpoch {
		activationEpoch.Valid = true
		activationEpoch.Int64 = (int64)(validator.ActivationEpoch)
	}
	if validator.ExitEpoch != farFutureEpoch {
		exitEpoch.Valid = true
		exitEpoch.Int64 = (int64)(validator.ExitEpoch)
	}
	if validator.WithdrawableEpoch != farFutureEpoch {
		withdrawableEpoch.Valid = true
		withdrawableEpoch.Int64 = (int64)(validator.WithdrawableEpoch)
	}

	_, err := tx.Exec(ctx, `
      INSERT INTO t_validators(f_public_key
                              ,f_index
                              ,f_slashed
                              ,f_activation_eligibility_epoch
                              ,f_activation_epoch
                              ,f_exit_epoch
                              ,f_withdrawable_epoch
                              ,f_effective_balance
                              ,f_withdrawal_credentials)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
      ON CONFLICT (f_index) DO
      UPDATE
      SET f_public_key = excluded.f_public_key
         ,f_slashed = excluded.f_slashed
         ,f_activation_eligibility_epoch = excluded.f_activation_eligibility_epoch
         ,f_activation_epoch = excluded.f_activation_epoch
         ,f_exit_epoch = excluded.f_exit_epoch
         ,f_withdrawable_epoch = excluded.f_withdrawable_epoch
         ,f_effective_balance = excluded.f_effective_balance
         ,f_withdrawal_credentials = excluded.f_withdrawal_credentials
		 `,
		validator.PublicKey[:],
		validator.Index,
		validator.Slashed,
		activationEligibilityEpoch,
		activationEpoch,
		exitEpoch,
		withdrawableEpoch,
		validator.EffectiveBalance,
		validator.WithdrawalCredentials[:],
	)

	return err
}

// SetValidatorBalance sets a validator's balance.
func (s *Service) SetValidatorBalance(ctx context.Context, balance *chaindb.ValidatorBalance) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SetValidatorBalance")
	defer span.End()

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

// SetValidatorBalances sets multiple validator balances.
func (s *Service) SetValidatorBalances(ctx context.Context, balances []*chaindb.ValidatorBalance) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SetValidatorBalances")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.CopyFrom(ctx,
		pgx.Identifier{"t_validator_balances"},
		[]string{
			"f_validator_index",
			"f_epoch",
			"f_balance",
			"f_effective_balance",
		},
		pgx.CopyFromSlice(len(balances), func(i int) ([]any, error) {
			return []any{
				balances[i].Index,
				balances[i].Epoch,
				balances[i].Balance,
				balances[i].EffectiveBalance,
			}, nil
		}))
	return err
}

// Validators fetches all validators.
func (s *Service) Validators(ctx context.Context) ([]*chaindb.Validator, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "Validators")
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
      SELECT f_public_key
            ,f_index
            ,f_slashed
            ,f_activation_eligibility_epoch
            ,f_activation_epoch
            ,f_exit_epoch
            ,f_withdrawable_epoch
            ,f_effective_balance
            ,f_withdrawal_credentials
      FROM t_validators
      ORDER BY f_index
	  `)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	validators := make([]*chaindb.Validator, 0)
	for rows.Next() {
		validator, err := validatorFromRow(rows)
		if err != nil {
			return nil, err
		}
		validators = append(validators, validator)
	}

	return validators, nil
}

// ValidatorsByPublicKey fetches all validators matching the given public keys.
// This is a common starting point for external entities to query specific validators, as they should
// always have the public key at a minimum, hence the return map keyed by public key.
func (s *Service) ValidatorsByPublicKey(ctx context.Context, pubKeys []phase0.BLSPubKey) (map[phase0.BLSPubKey]*chaindb.Validator, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "ValidatorsByPublicKey")
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
            ,f_withdrawal_credentials
      FROM t_validators
      WHERE f_public_key = ANY($1)
      ORDER BY f_index
	  `,
		sqlPubKeys,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	validators := make(map[phase0.BLSPubKey]*chaindb.Validator)
	for rows.Next() {
		validator, err := validatorFromRow(rows)
		if err != nil {
			return nil, err
		}
		validators[validator.PublicKey] = validator
	}

	return validators, nil
}

// ValidatorsByIndex fetches all validators matching the given indices.
func (s *Service) ValidatorsByIndex(ctx context.Context, indices []phase0.ValidatorIndex) (map[phase0.ValidatorIndex]*chaindb.Validator, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "ValidatorsByIndex")
	defer span.End()

	if len(indices) == 0 {
		return map[phase0.ValidatorIndex]*chaindb.Validator{}, nil
	}

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
      SELECT f_public_key
            ,f_index
            ,f_slashed
            ,f_activation_eligibility_epoch
            ,f_activation_epoch
            ,f_exit_epoch
            ,f_withdrawable_epoch
            ,f_effective_balance
            ,f_withdrawal_credentials
      FROM t_validators
      WHERE f_index = ANY($1)
      ORDER BY f_index
	  `,
		indices,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	validators := make(map[phase0.ValidatorIndex]*chaindb.Validator)
	for rows.Next() {
		validator, err := validatorFromRow(rows)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		validators[validator.Index] = validator
	}

	return validators, nil
}

// ValidatorsByWithdrawalCredential fetches all validators with the given withdrawal credential.
func (s *Service) ValidatorsByWithdrawalCredential(ctx context.Context, withdrawalCredentials []byte) ([]*chaindb.Validator, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "ValidatorsByWithdrawalCredential")
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

	sqlWithdrawalCredentials := make([]byte, len(withdrawalCredentials))
	copy(sqlWithdrawalCredentials, withdrawalCredentials)

	rows, err := tx.Query(ctx, `
      SELECT f_public_key
            ,f_index
            ,f_slashed
            ,f_activation_eligibility_epoch
            ,f_activation_epoch
            ,f_exit_epoch
            ,f_withdrawable_epoch
            ,f_effective_balance
            ,f_withdrawal_credentials
      FROM t_validators
      WHERE f_withdrawal_credentials = ANY($1)
      ORDER BY f_index
	  `,
		sqlWithdrawalCredentials,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	validators := make([]*chaindb.Validator, 0)
	for rows.Next() {
		validator, err := validatorFromRow(rows)
		if err != nil {
			return nil, err
		}
		validators = append(validators, validator)
	}

	return validators, nil
}

// ValidatorBalancesByEpoch fetches the validator balances for the given epoch.
func (s *Service) ValidatorBalancesByEpoch(
	ctx context.Context,
	epoch phase0.Epoch,
) (
	[]*chaindb.ValidatorBalance,
	error,
) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "ValidatorBalancesByEpoch")
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
            ,f_balance
            ,f_effective_balance
      FROM t_validator_balances
      WHERE f_epoch = $1::BIGINT
      ORDER BY f_validator_index`,
		uint64(epoch),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	validatorBalances := make([]*chaindb.ValidatorBalance, 0)

	for rows.Next() {
		validatorBalance, err := validatorBalanceFromRow(rows)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		validatorBalances = append(validatorBalances, validatorBalance)
		if uint64(validatorBalance.Index) != uint64(len(validatorBalances)-1) {
			panic(fmt.Sprintf("bad index %d with len %d", validatorBalance.Index, len(validatorBalances)))
		}
	}

	return validatorBalances, nil
}

// ValidatorBalancesByIndexAndEpoch fetches the validator balances for the given validators and epoch.
func (s *Service) ValidatorBalancesByIndexAndEpoch(
	ctx context.Context,
	validatorIndices []phase0.ValidatorIndex,
	epoch phase0.Epoch,
) (
	map[phase0.ValidatorIndex]*chaindb.ValidatorBalance,
	error,
) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "ValidatorBalancesByIndexAndEpoch")
	defer span.End()

	if len(validatorIndices) == 0 {
		return map[phase0.ValidatorIndex]*chaindb.ValidatorBalance{}, nil
	}

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
            ,f_balance
            ,f_effective_balance
      FROM t_validator_balances
      WHERE f_epoch = $2::BIGINT
        AND f_validator_index = ANY($1)
      ORDER BY f_validator_index`,
		validatorIndices,
		uint64(epoch),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	validatorBalances := make(map[phase0.ValidatorIndex]*chaindb.ValidatorBalance, len(validatorIndices))

	for rows.Next() {
		validatorBalance, err := validatorBalanceFromRow(rows)
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
	validatorIndices []phase0.ValidatorIndex,
	startEpoch phase0.Epoch,
	endEpoch phase0.Epoch,
) (
	map[phase0.ValidatorIndex][]*chaindb.ValidatorBalance,
	error,
) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "ValidatorBalancesByIndexAndEpochRange")
	defer span.End()

	if len(validatorIndices) == 0 {
		return map[phase0.ValidatorIndex][]*chaindb.ValidatorBalance{}, nil
	}

	tx := s.tx(ctx)
	if tx == nil {
		ctx, err := s.BeginROTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		defer s.CommitROTx(ctx)
		tx = s.tx(ctx)
	}

	// Sort the validator indices.
	sort.Slice(validatorIndices, func(i, j int) bool {
		return validatorIndices[i] < validatorIndices[j]
	})

	// Create an array for the validator indices.  This gives us higher performance for our query.
	indices := make([]string, len(validatorIndices))
	for i, validatorIndex := range validatorIndices {
		indices[i] = fmt.Sprintf("(%d)", validatorIndex)
	}

	rows, err := tx.Query(ctx, fmt.Sprintf(`
      SELECT f_validator_index
            ,f_epoch
            ,f_balance
            ,f_effective_balance
      FROM t_validator_balances
      JOIN (VALUES %s)
        AS x(id)
        ON x.id = t_validator_balances.f_validator_index
      WHERE f_epoch >= $1
        AND f_epoch < $2
      ORDER BY f_validator_index
              ,f_epoch`, strings.Join(indices, ",")),
		uint64(startEpoch),
		uint64(endEpoch),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	validatorBalances := make(map[phase0.ValidatorIndex][]*chaindb.ValidatorBalance, len(validatorIndices))
	for rows.Next() {
		validatorBalance, err := validatorBalanceFromRow(rows)
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
	if err := padValidatorBalances(validatorBalances, int(uint64(endEpoch)-uint64(startEpoch)), startEpoch); err != nil {
		return nil, err
	}

	return validatorBalances, nil
}

// ValidatorBalancesByIndexAndEpochs fetches the validator balances for the given validators at the specified epochs.
func (s *Service) ValidatorBalancesByIndexAndEpochs(
	ctx context.Context,
	validatorIndices []phase0.ValidatorIndex,
	epochs []phase0.Epoch,
) (
	map[phase0.ValidatorIndex][]*chaindb.ValidatorBalance,
	error,
) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "ValidatorBalancesByIndexAndEpochs")
	defer span.End()

	if len(validatorIndices) == 0 {
		return map[phase0.ValidatorIndex][]*chaindb.ValidatorBalance{}, nil
	}

	tx := s.tx(ctx)
	if tx == nil {
		ctx, err := s.BeginROTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		defer s.CommitROTx(ctx)
		tx = s.tx(ctx)
	}

	// Sort the validator indices.
	sort.Slice(validatorIndices, func(i, j int) bool {
		return validatorIndices[i] < validatorIndices[j]
	})

	// Create an array for the validator indices.  This gives us higher performance for our query.
	indices := make([]string, len(validatorIndices))
	for i, validatorIndex := range validatorIndices {
		indices[i] = fmt.Sprintf("(%d)", validatorIndex)
	}

	dbEpochs := make([]uint64, len(epochs))
	for i, epoch := range epochs {
		dbEpochs[i] = uint64(epoch)
	}
	rows, err := tx.Query(ctx, fmt.Sprintf(`
      SELECT f_validator_index
            ,f_epoch
            ,f_balance
            ,f_effective_balance
      FROM t_validator_balances
	  JOIN (VALUES %s)
	           AS x(id)
	           ON x.id = t_validator_balances.f_validator_index
      WHERE f_epoch = ANY($1)
      ORDER BY f_validator_index
              ,f_epoch`, strings.Join(indices, ",")),
		dbEpochs,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	validatorBalances := make(map[phase0.ValidatorIndex][]*chaindb.ValidatorBalance, len(validatorIndices))
	for rows.Next() {
		validatorBalance, err := validatorBalanceFromRow(rows)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		_, exists := validatorBalances[validatorBalance.Index]
		if !exists {
			validatorBalances[validatorBalance.Index] = make([]*chaindb.ValidatorBalance, 0, len(epochs))
		}
		validatorBalances[validatorBalance.Index] = append(validatorBalances[validatorBalance.Index], validatorBalance)
	}

	return validatorBalances, nil
}

func padValidatorBalances(validatorBalances map[phase0.ValidatorIndex][]*chaindb.ValidatorBalance, entries int, startEpoch phase0.Epoch) error {
	for validatorIndex, balances := range validatorBalances {
		if len(balances) != entries {
			paddedBalances := make([]*chaindb.ValidatorBalance, entries)
			padding := entries - len(balances)
			for i := 0; i < padding; i++ {
				paddedBalances[i] = &chaindb.ValidatorBalance{
					Index:            validatorIndex,
					Epoch:            startEpoch + phase0.Epoch(i),
					Balance:          0,
					EffectiveBalance: 0,
				}
			}
			if len(balances) > 0 && balances[0].Epoch != startEpoch+phase0.Epoch(padding) {
				return fmt.Errorf("data missing in chaindb for validator %d", validatorIndex)
			}

			copy(paddedBalances[padding:], balances)
			validatorBalances[validatorIndex] = paddedBalances
		}
	}

	return nil
}

// validatorFromRow converts a SQL row in to a validator.
func validatorFromRow(rows pgx.Rows) (*chaindb.Validator, error) {
	var publicKey []byte
	var activationEligibilityEpoch sql.NullInt64
	var activationEpoch sql.NullInt64
	var exitEpoch sql.NullInt64
	var withdrawableEpoch sql.NullInt64
	var withdrawalCredentials []byte
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
		&withdrawalCredentials,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to scan row")
	}
	copy(validator.PublicKey[:], publicKey)
	if !activationEligibilityEpoch.Valid {
		validator.ActivationEligibilityEpoch = farFutureEpoch
	} else {
		validator.ActivationEligibilityEpoch = phase0.Epoch(activationEligibilityEpoch.Int64)
	}
	if !activationEpoch.Valid {
		validator.ActivationEpoch = farFutureEpoch
	} else {
		validator.ActivationEpoch = phase0.Epoch(activationEpoch.Int64)
	}
	if !exitEpoch.Valid {
		validator.ExitEpoch = farFutureEpoch
	} else {
		validator.ExitEpoch = phase0.Epoch(exitEpoch.Int64)
	}
	if !withdrawableEpoch.Valid {
		validator.WithdrawableEpoch = farFutureEpoch
	} else {
		validator.WithdrawableEpoch = phase0.Epoch(withdrawableEpoch.Int64)
	}
	copy(validator.WithdrawalCredentials[:], withdrawalCredentials)

	return validator, nil
}

// validatorBalanceFromRow converts a SQL row in to a validator balance.
func validatorBalanceFromRow(rows pgx.Rows) (*chaindb.ValidatorBalance, error) {
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
	return validatorBalance, nil
}

// PruneValidatorBalances prunes validator balances up to (but not including) the given epoch.
func (s *Service) PruneValidatorBalances(ctx context.Context, to phase0.Epoch, retain []phase0.BLSPubKey) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "PruneValidatorBalances")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	// Build the query.
	queryBuilder := strings.Builder{}
	queryVals := make([]any, 0)

	queryVals = append(queryVals, to)

	if len(retain) > 0 {
		queryBuilder.WriteString(`
DELETE FROM t_validator_balances
USING t_validators
WHERE f_epoch <= $1
AND t_validator_balances.f_validator_index = t_validators.f_index
AND NOT (t_validators.f_public_key = ANY($2))`)

		pubkeysBytes := make([][]byte, 0, len(retain))

		for i := range retain {
			pubkeysBytes = append(pubkeysBytes, retain[i][:])
		}

		queryVals = append(queryVals, pubkeysBytes)

	} else {
		queryBuilder.WriteString(`
DELETE FROM t_validator_balances
WHERE f_epoch <= $1`)
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
