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
	"encoding/json"

	"github.com/pkg/errors"
)

type schemaMetadata struct {
	Version uint64 `json:"version"`
}

var currentVersion = uint64(1)

var upgradeFunctions = map[uint64][]func(context.Context, *Service) error{
	1: {
		validatorsEpochNull,
		createDeposits,
	},
}

// Upgrade upgrades the database.
func (s *Service) Upgrade(ctx context.Context) error {
	version, err := s.version(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to obtain version")
	}

	if version == currentVersion {
		// Nothing to do.
		return nil
	}

	ctx, cancel, err := s.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin upgrade transaction")
	}

	for i := version; i <= currentVersion; i++ {
		if upgradeFuncs, exists := upgradeFunctions[i]; exists {
			for _, upgradeFunc := range upgradeFuncs {
				if err := upgradeFunc(ctx, s); err != nil {
					cancel()
					return errors.Wrap(err, "failed to upgrade")
				}
			}
		}
	}

	if err := s.setVersion(ctx, currentVersion); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set latest schema version")
	}

	if err := s.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to commit upgrade transaction")
	}

	return nil
}

// validatorsEpochNull allows epochs in the t_validators table to be NULL.
func validatorsEpochNull(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	// Drop NOT NULL constraints.
	if _, err := tx.Exec(ctx, "ALTER TABLE t_validators ALTER COLUMN f_activation_eligibility_epoch DROP NOT NULL"); err != nil {
		return errors.Wrap(err, "failed to drop NOT NULL constraint on f_activation_eligibility_epoch")
	}
	if _, err := tx.Exec(ctx, "ALTER TABLE t_validators ALTER COLUMN f_activation_epoch DROP NOT NULL"); err != nil {
		return errors.Wrap(err, "failed to drop NOT NULL constraint on f_activation_epoch")
	}
	if _, err := tx.Exec(ctx, "ALTER TABLE t_validators ALTER COLUMN f_exit_epoch DROP NOT NULL"); err != nil {
		return errors.Wrap(err, "failed to drop NOT NULL constraint on f_exit_epoch")
	}
	if _, err := tx.Exec(ctx, "ALTER TABLE t_validators ALTER COLUMN f_withdrawable_epoch DROP NOT NULL"); err != nil {
		return errors.Wrap(err, "failed to drop NOT NULL constraint on f_withdrawable_epoch")
	}

	// Change -1 values to NULL.
	if _, err := tx.Exec(ctx, "UPDATE t_validators SET f_activation_eligibility_epoch = NULL WHERE f_activation_eligibility_epoch = -1"); err != nil {
		return errors.Wrap(err, "failed to change -1 to NULL on f_activation_eligibility_epoch")
	}
	if _, err := tx.Exec(ctx, "UPDATE t_validators SET f_activation_epoch = NULL WHERE f_activation_epoch = -1"); err != nil {
		return errors.Wrap(err, "failed to change -1 to NULL on f_activation_epoch")
	}
	if _, err := tx.Exec(ctx, "UPDATE t_validators SET f_exit_epoch = NULL WHERE f_exit_epoch = -1"); err != nil {
		return errors.Wrap(err, "failed to change -1 to NULL on f_exit_epoch")
	}
	if _, err := tx.Exec(ctx, "UPDATE t_validators SET f_withdrawable_epoch = NULL WHERE f_withdrawable_epoch = -1"); err != nil {
		return errors.Wrap(err, "failed to change -1 to NULL on f_withdrawable_epoch")
	}

	return nil
}

// createDeposits creates the t_deposits table.
func createDeposits(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	if _, err := tx.Exec(ctx, `CREATE TABLE t_deposits (
  f_inclusion_slot         BIGINT NOT NULL
 ,f_inclusion_block_root   BYTEA NOT NULL REFERENCES t_blocks(f_root) ON DELETE CASCADE
 ,f_inclusion_index        BIGINT NOT NULL
 ,f_validator_pubkey       BYTEA NOT NULL
 ,f_withdrawal_credentials BYTEA NOT NULL
 ,f_amount                 BIGINT NOT NULL
)`); err != nil {
		return errors.Wrap(err, "failed to create deposits table")
	}

	if _, err := tx.Exec(ctx, "CREATE UNIQUE INDEX i_deposits_1 ON t_deposits(f_inclusion_slot,f_inclusion_block_root,f_inclusion_index)"); err != nil {
		return errors.Wrap(err, "failed to create deposits index")
	}
	return nil
}

// version obtains the version of the schema.
func (s *Service) version(ctx context.Context) (uint64, error) {
	data, err := s.Metadata(ctx, "schema")
	if err != nil {
		return 0, errors.Wrap(err, "failed to obtain schema metadata")
	}

	// No data means it's version 0 of the schema.
	if len(data) == 0 {
		return 0, nil
	}

	var metadata schemaMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return 0, errors.Wrap(err, "failed to unmarshal metadata JSON")
	}

	return metadata.Version, nil
}

// setVersion sets the version of the schema.
func (s *Service) setVersion(ctx context.Context, version uint64) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	metadata := &schemaMetadata{
		Version: version,
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		return errors.Wrap(err, "failed to marshal metadata")
	}

	return s.SetMetadata(ctx, "schema", data)
}
