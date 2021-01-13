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

	spec "github.com/attestantio/go-eth2-client/spec/phase0"
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
		createChainSpec,
		createGenesis,
		addProposerSlashingBlockRoots,
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
			for i, upgradeFunc := range upgradeFuncs {
				log.Info().Int("current", i+1).Int("total", len(upgradeFuncs)).Msg("Running upgrade function")
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

// createChainSpec creates the t_chain_spec table.
func createChainSpec(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	if _, err := tx.Exec(ctx, `CREATE TABLE t_chain_spec (
  f_key TEXT NOT NULL PRIMARY KEY
 ,f_value TEXT NOT NULL
)`); err != nil {
		return errors.Wrap(err, "failed to create chain spec table")
	}

	return nil
}

// createGenesis creates the t_genesis table.
func createGenesis(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	if _, err := tx.Exec(ctx, `CREATE TABLE t_genesis (
  f_validators_root BYTEA NOT NULL PRIMARY KEY
 ,f_time TIMESTAMPTZ NOT NULL
 ,f_fork_version BYTEA NOT NULL
)`); err != nil {
		return errors.Wrap(err, "failed to create genesis table")
	}

	return nil
}

// addProposerSlashingBlockRoots adds calculated block roots to the t_proposer_slashings table.
func addProposerSlashingBlockRoots(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	// Start by altering the table to add the columns, but allow them to be NULL.
	if _, err := tx.Exec(ctx, `
ALTER TABLE t_proposer_slashings
ADD COLUMN f_block_1_root BYTEA
`); err != nil {
		return errors.Wrap(err, "failed to add f_block_1_root to proposer slashings table")
	}
	if _, err := tx.Exec(ctx, `
ALTER TABLE t_proposer_slashings
ADD COLUMN f_block_2_root BYTEA
`); err != nil {
		return errors.Wrap(err, "failed to add f_block_2_root to proposer slashings table")
	}

	// Need to update all of the existing slashings so that they have the block root populated.
	// Using hard-coded maximum values for slots to catch all proposer slashings.
	proposerSlashings, err := s.ProposerSlashingsForSlotRange(ctx, 0, 0x7fffffffffffffff)
	if err != nil {
		return errors.Wrap(err, "failed to obtain current proposer slashings")
	}

	for _, proposerSlashing := range proposerSlashings {
		header1 := &spec.BeaconBlockHeader{
			Slot:          proposerSlashing.Header1Slot,
			ProposerIndex: proposerSlashing.Header1ProposerIndex,
			ParentRoot:    proposerSlashing.Header1ParentRoot,
			StateRoot:     proposerSlashing.Header1StateRoot,
			BodyRoot:      proposerSlashing.Header1BodyRoot,
		}
		proposerSlashing.Block1Root, err = header1.HashTreeRoot()
		if err != nil {
			return errors.Wrap(err, "failed to calculate proposer slashing block 1 root")
		}
		header2 := &spec.BeaconBlockHeader{
			Slot:          proposerSlashing.Header2Slot,
			ProposerIndex: proposerSlashing.Header2ProposerIndex,
			ParentRoot:    proposerSlashing.Header2ParentRoot,
			StateRoot:     proposerSlashing.Header2StateRoot,
			BodyRoot:      proposerSlashing.Header2BodyRoot,
		}
		proposerSlashing.Block2Root, err = header2.HashTreeRoot()
		if err != nil {
			return errors.Wrap(err, "failed to calculate proposer slashing block 2 root")
		}
		if err := s.SetProposerSlashing(ctx, proposerSlashing); err != nil {
			return errors.Wrap(err, "failed to update proposer slashing")
		}
	}

	// Add the NOT NULL constraint to the created columns.
	if _, err := tx.Exec(ctx, `
ALTER TABLE t_proposer_slashings
ALTER COLUMN f_block_1_root SET NOT NULL
`); err != nil {
		return errors.Wrap(err, "failed to add NOT NULL constraint for f_block_1_root to proposer slashings table")
	}
	if _, err := tx.Exec(ctx, `
ALTER TABLE t_proposer_slashings
ALTER COLUMN f_block_2_root SET NOT NULL
`); err != nil {
		return errors.Wrap(err, "failed to add NOT NULL constraint for f_block_2_root to proposer slashings table")
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
