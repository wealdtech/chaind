// Copyright © 2021 Weald Technology Trading.
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

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
)

// AggregateValidatorBalancesByIndexAndEpoch fetches the aggregate validator balances for the given validators and epoch.
func (s *Service) AggregateValidatorBalancesByIndexAndEpoch(
	ctx context.Context,
	validatorIndices []phase0.ValidatorIndex,
	epoch phase0.Epoch,
) (
	*chaindb.AggregateValidatorBalance,
	error,
) {
	if len(validatorIndices) == 0 {
		return &chaindb.AggregateValidatorBalance{}, nil
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

	var balance phase0.Gwei
	var effectiveBalance phase0.Gwei

	err := tx.QueryRow(ctx, fmt.Sprintf(`
      SELECT SUM(f_balance)
            ,SUM(f_effective_balance)
      FROM t_validator_balances
      JOIN (VALUES %s)
        AS x(id)
        ON x.id = t_validator_balances.f_validator_index
      WHERE f_epoch = $1`, fastIndices(validatorIndices)),
		uint64(epoch),
	).Scan(
		&balance,
		&effectiveBalance,
	)
	if err != nil {
		return nil, err
	}

	aggregateBalance := &chaindb.AggregateValidatorBalance{
		Epoch:            epoch,
		Balance:          balance,
		EffectiveBalance: effectiveBalance,
	}

	return aggregateBalance, nil
}

// AggregateValidatorBalancesByIndexAndEpochRange fetches the aggregate validator balances for the given validators and
// epoch range.
// Ranges are inclusive of start and exclusive of end i.e. a request with startEpoch 2 and endEpoch 4 will provide
// balances for epochs 2 and 3.
func (s *Service) AggregateValidatorBalancesByIndexAndEpochRange(
	ctx context.Context,
	validatorIndices []phase0.ValidatorIndex,
	startEpoch phase0.Epoch,
	endEpoch phase0.Epoch,
) (
	[]*chaindb.AggregateValidatorBalance,
	error,
) {
	if len(validatorIndices) == 0 {
		return []*chaindb.AggregateValidatorBalance{}, nil
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

	rows, err := tx.Query(ctx, fmt.Sprintf(`
      SELECT f_epoch
            ,SUM(f_balance)
            ,SUM(f_effective_balance)
      FROM t_validator_balances
      JOIN (VALUES %s)
        AS x(id)
        ON x.id = t_validator_balances.f_validator_index
      WHERE f_epoch >= $1
        AND f_epoch < $2
      GROUP BY f_epoch
      ORDER BY f_epoch`, fastIndices(validatorIndices)),
		uint64(startEpoch),
		uint64(endEpoch),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	aggregateBalances := make([]*chaindb.AggregateValidatorBalance, 0, int(endEpoch-startEpoch))
	for rows.Next() {
		aggregateBalance := &chaindb.AggregateValidatorBalance{}
		err := rows.Scan(
			&aggregateBalance.Epoch,
			&aggregateBalance.Balance,
			&aggregateBalance.EffectiveBalance,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		aggregateBalances = append(aggregateBalances, aggregateBalance)
	}

	return aggregateBalances, nil
}

// AggregateValidatorBalancesByIndexAndEpochs fetches the validator balances for the given validators at the specified epochs.
func (s *Service) AggregateValidatorBalancesByIndexAndEpochs(
	ctx context.Context,
	validatorIndices []phase0.ValidatorIndex,
	epochs []phase0.Epoch,
) (
	[]*chaindb.AggregateValidatorBalance,
	error,
) {
	if len(validatorIndices) == 0 {
		return []*chaindb.AggregateValidatorBalance{}, nil
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

	dbEpochs := make([]uint64, len(epochs))
	for i, epoch := range epochs {
		dbEpochs[i] = uint64(epoch)
	}
	rows, err := tx.Query(ctx, fmt.Sprintf(`
      SELECT f_epoch
            ,SUM(f_balance)
            ,SUM(f_effective_balance)
      FROM t_validator_balances
      JOIN (VALUES %s)
        AS x(id)
        ON x.id = t_validator_balances.f_validator_index
      WHERE f_epoch = ANY($1)
      ORDER BY f_epoch`, fastIndices(validatorIndices)),
		dbEpochs,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	aggregateBalances := make([]*chaindb.AggregateValidatorBalance, 0, len(epochs))
	for rows.Next() {
		aggregateBalance := &chaindb.AggregateValidatorBalance{}
		err := rows.Scan(
			&aggregateBalance.Epoch,
			&aggregateBalance.Balance,
			&aggregateBalance.EffectiveBalance,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		aggregateBalances = append(aggregateBalances, aggregateBalance)
	}

	return aggregateBalances, nil
}

// fastIndices orders and munges the values for validator indices supplied to balance requests.
// This allows us to form a query that is significantly faster than the simple IN() style.
func fastIndices(validatorIndices []phase0.ValidatorIndex) string {
	// Sort the validator indices.
	sort.Slice(validatorIndices, func(i, j int) bool {
		return validatorIndices[i] < validatorIndices[j]
	})

	// Create an array for the validator indices.  This gives us higher performance for our query.
	indices := make([]string, len(validatorIndices))
	for i, validatorIndex := range validatorIndices {
		indices[i] = fmt.Sprintf("(%d)", validatorIndex)
	}

	return strings.Join(indices, ",")
}
