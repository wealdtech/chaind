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

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
	"go.opentelemetry.io/otel"
)

// SetETH1Deposit sets an Ethereum 1 deposit.
func (s *Service) SetETH1Deposit(ctx context.Context, deposit *chaindb.ETH1Deposit) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SetETH1Deposit")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.Exec(ctx, `
      INSERT INTO t_eth1_deposits(f_eth1_block_number
                                 ,f_eth1_block_hash
                                 ,f_eth1_block_timestamp
                                 ,f_eth1_tx_hash
                                 ,f_eth1_log_index
                                 ,f_eth1_sender
                                 ,f_eth1_recipient
                                 ,f_eth1_gas_used
                                 ,f_eth1_gas_price
                                 ,f_deposit_index
                                 ,f_validator_pubkey
                                 ,f_withdrawal_credentials
                                 ,f_signature
                                 ,f_amount)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
      ON CONFLICT (f_deposit_index) DO
      UPDATE
      SET f_eth1_block_number = excluded.f_eth1_block_number
         ,f_eth1_block_hash = excluded.f_eth1_block_hash
         ,f_eth1_block_timestamp = excluded.f_eth1_block_timestamp
         ,f_eth1_tx_hash = excluded.f_eth1_tx_hash
         ,f_eth1_log_index = excluded.f_eth1_log_index
         ,f_eth1_sender = excluded.f_eth1_sender
         ,f_eth1_recipient = excluded.f_eth1_recipient
         ,f_eth1_gas_used = excluded.f_eth1_gas_used
         ,f_validator_pubkey = excluded.f_validator_pubkey
         ,f_withdrawal_credentials = excluded.f_withdrawal_credentials
         ,f_signature = excluded.f_signature
         ,f_amount = excluded.f_amount
      `,
		deposit.ETH1BlockNumber,
		deposit.ETH1BlockHash,
		deposit.ETH1BlockTimestamp,
		deposit.ETH1TxHash,
		deposit.ETH1LogIndex,
		deposit.ETH1Sender,
		deposit.ETH1Recipient,
		deposit.ETH1GasUsed,
		deposit.ETH1GasPrice,
		deposit.DepositIndex,
		deposit.ValidatorPubKey[:],
		deposit.WithdrawalCredentials,
		deposit.Signature[:],
		deposit.Amount,
	)

	return err
}

// ETH1DepositsByPublicKey fetches Ethereum 1 deposits for a given set of validator public keys.
func (s *Service) ETH1DepositsByPublicKey(ctx context.Context, pubKeys []phase0.BLSPubKey) ([]*chaindb.ETH1Deposit, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "ETH1DepositsByPublicKey")
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

	validatorPubKeys := make([][]byte, len(pubKeys))
	for i := range pubKeys {
		validatorPubKeys[i] = pubKeys[i][:]
	}
	rows, err := tx.Query(ctx, `
      SELECT f_eth1_block_number
            ,f_eth1_block_hash
            ,f_eth1_block_timestamp
            ,f_eth1_tx_hash
            ,f_eth1_log_index
            ,f_eth1_sender
            ,f_eth1_recipient
            ,f_eth1_gas_used
            ,f_eth1_gas_price
            ,f_deposit_index
            ,f_validator_pubkey
            ,f_withdrawal_credentials
            ,f_signature
            ,f_amount
      FROM t_eth1_deposits
      WHERE f_validator_pubkey = ANY($1)
      ORDER BY f_eth1_block_number
              ,f_eth1_log_index
	  `,
		validatorPubKeys,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	deposits := make([]*chaindb.ETH1Deposit, 0)
	for rows.Next() {
		deposit := &chaindb.ETH1Deposit{}
		var validatorPubKey []byte
		var signature []byte
		err := rows.Scan(
			&deposit.ETH1BlockNumber,
			&deposit.ETH1BlockHash,
			&deposit.ETH1BlockTimestamp,
			&deposit.ETH1TxHash,
			&deposit.ETH1LogIndex,
			&deposit.ETH1Sender,
			&deposit.ETH1Recipient,
			&deposit.ETH1GasUsed,
			&deposit.ETH1GasPrice,
			&deposit.DepositIndex,
			&validatorPubKey,
			&deposit.WithdrawalCredentials,
			&signature,
			&deposit.Amount,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		copy(deposit.ValidatorPubKey[:], validatorPubKey)
		copy(deposit.Signature[:], signature)
		deposits = append(deposits, deposit)
	}

	return deposits, nil
}
