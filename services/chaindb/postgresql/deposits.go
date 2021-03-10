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

	spec "github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
)

// SetDeposit sets a deposit.
func (s *Service) SetDeposit(ctx context.Context, deposit *chaindb.Deposit) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.Exec(ctx, `
      INSERT INTO t_deposits(f_inclusion_slot
                            ,f_inclusion_block_root
                            ,f_inclusion_index
                            ,f_validator_pubkey
                            ,f_withdrawal_credentials
                            ,f_amount)
      VALUES($1,$2,$3,$4,$5,$6)
      ON CONFLICT (f_inclusion_slot,f_inclusion_block_root,f_inclusion_index) DO
      UPDATE
      SET f_validator_pubkey = excluded.f_validator_pubkey
         ,f_withdrawal_credentials = excluded.f_withdrawal_credentials
         ,f_amount = excluded.f_amount
      `,
		deposit.InclusionSlot,
		deposit.InclusionBlockRoot[:],
		deposit.InclusionIndex,
		deposit.ValidatorPubKey[:],
		deposit.WithdrawalCredentials,
		deposit.Amount,
	)

	return err
}

// DepositsByPublicKey fetches deposits for a given set of validator public keys.
func (s *Service) DepositsByPublicKey(ctx context.Context, pubKeys []spec.BLSPubKey) (map[spec.BLSPubKey][]*chaindb.Deposit, error) {
	tx := s.tx(ctx)
	if tx == nil {
		ctx, cancel, err := s.BeginTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer cancel()
	}

	validatorPubKeys := make([][]byte, len(pubKeys))
	for i := range pubKeys {
		validatorPubKeys[i] = pubKeys[i][:]
	}
	rows, err := tx.Query(ctx, `
      SELECT f_inclusion_slot
            ,f_inclusion_block_root
            ,f_inclusion_index
            ,f_validator_pubkey
            ,f_withdrawal_credentials
            ,f_amount
      FROM t_deposits
      WHERE f_validator_pubkey = ANY($1)
      ORDER BY f_inclusion_slot
              ,f_inclusion_index
	  `,
		validatorPubKeys,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	deposits := make(map[spec.BLSPubKey][]*chaindb.Deposit, len(pubKeys))
	for rows.Next() {
		deposit := &chaindb.Deposit{}
		var inclusionBlockRoot []byte
		var validatorPubKey []byte
		err := rows.Scan(
			&deposit.InclusionSlot,
			&inclusionBlockRoot,
			&deposit.InclusionIndex,
			&validatorPubKey,
			&deposit.WithdrawalCredentials,
			&deposit.Amount,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		copy(deposit.InclusionBlockRoot[:], inclusionBlockRoot)
		copy(deposit.ValidatorPubKey[:], validatorPubKey)

		_, exists := deposits[deposit.ValidatorPubKey]
		if !exists {
			deposits[deposit.ValidatorPubKey] = make([]*chaindb.Deposit, 0)
		}
		deposits[deposit.ValidatorPubKey] = append(deposits[deposit.ValidatorPubKey], deposit)
	}

	return deposits, nil
}

// DepositsForSlotRange fetches all deposits made in the given slot range.
// It will return deposits from blocks that are canonical or undefined, but not from non-canonical blocks.
func (s *Service) DepositsForSlotRange(ctx context.Context, minSlot spec.Slot, maxSlot spec.Slot) ([]*chaindb.Deposit, error) {
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
      SELECT f_inclusion_slot
            ,f_inclusion_block_root
            ,f_inclusion_index
            ,f_validator_pubkey
            ,f_withdrawal_credentials
            ,f_amount
      FROM t_deposits
      WHERE f_inclusion_slot >= $1
        AND f_inclusion_slot < $2
		AND f_inclusion_slot IN (SELECT f_slot FROM t_blocks WHERE f_slot >= $1 AND f_slot < $2 AND (f_canonical IS NULL OR f_canonical = true))
      ORDER BY f_inclusion_slot
              ,f_inclusion_index`,
		minSlot,
		maxSlot,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	deposits := make([]*chaindb.Deposit, 0)
	for rows.Next() {
		deposit := &chaindb.Deposit{}
		var inclusionBlockRoot []byte
		var validatorPubKey []byte
		err := rows.Scan(
			&deposit.InclusionSlot,
			&inclusionBlockRoot,
			&deposit.InclusionIndex,
			&validatorPubKey,
			&deposit.WithdrawalCredentials,
			&deposit.Amount,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		copy(deposit.InclusionBlockRoot[:], inclusionBlockRoot)
		copy(deposit.ValidatorPubKey[:], validatorPubKey)

		deposits = append(deposits, deposit)
	}

	return deposits, nil
}
