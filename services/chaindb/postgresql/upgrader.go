// Copyright © 2021 - 2023 Weald Technology Trading.
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
	"fmt"
	"time"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
)

type schemaMetadata struct {
	Version uint64 `json:"version"`
}

var currentVersion = uint64(11)

type upgrade struct {
	requiresRefetch bool
	funcs           []func(context.Context, *Service) error
}

var upgrades = map[uint64]*upgrade{
	1: {
		requiresRefetch: true,
		funcs: []func(context.Context, *Service) error{
			validatorsEpochNull,
			createDeposits,
			createChainSpec,
			createGenesis,
			addBlocksCanonical,
			addProposerSlashingBlockRoots,
			createETH1Deposits,
			addAttestationAggregationIndices,
			addAttestationsVoteFields,
		},
	},
	2: {
		funcs: []func(context.Context, *Service) error{
			dropEpochsTable,
			createSummaryTables,
			setValidatorBalancesMetadata,
		},
	},
	3: {
		funcs: []func(context.Context, *Service) error{
			createForkSchedule,
			createSyncCommittees,
			createSyncAggregates,
			addValidatorSummaryTimely,
		},
	},
	4: {
		funcs: []func(context.Context, *Service) error{
			addDepositsIndex,
		},
	},
	5: {
		funcs: []func(context.Context, *Service) error{
			addBlockParentDistance,
		},
	},
	6: {
		funcs: []func(context.Context, *Service) error{
			fixSyncAggregatesIndex,
		},
	},
	7: {
		funcs: []func(context.Context, *Service) error{
			addBellatrixSubtable,
		},
	},
	8: {
		funcs: []func(context.Context, *Service) error{
			addTimestamp,
		},
	},
	9: {
		funcs: []func(context.Context, *Service) error{
			createValidatorDaySummaries,
		},
	},
	10: {
		funcs: []func(context.Context, *Service) error{
			createBlockBLSToExecutionChanges,
			createBlockWithdrawals,
		},
	},
	11: {
		funcs: []func(context.Context, *Service) error{
			recreateForkSchedule,
		},
	},
	12: {
		funcs: []func(context.Context, *Service) error{
			recreateValidators,
		},
	},
}

// Upgrade upgrades the database.
// Returns true if the upgrade requires blocks to be refetched.
func (s *Service) Upgrade(ctx context.Context) (bool, error) {
	// See if we have anything at all.
	tableExists, err := s.tableExists(ctx, "t_metadata")
	if err != nil {
		return false, errors.Wrap(err, "failed to check presence of tables")
	}
	if !tableExists {
		return false, s.Init(ctx)
	}

	version, err := s.version(ctx)
	if err != nil {
		return false, errors.Wrap(err, "failed to obtain version")
	}

	log.Trace().Uint64("current_version", version).Uint64("required_version", currentVersion).Msg("Checking if database upgrade is required")
	if version == currentVersion {
		// Nothing to do.
		return false, nil
	}
	if version > currentVersion {
		log.Warn().Msg("This release is running an older version than that in the database, please upgrade to the latest release")
		return false, nil
	}

	ctx, cancel, err := s.BeginTx(ctx)
	if err != nil {
		return false, errors.Wrap(err, "failed to begin upgrade transaction")
	}

	requiresRefetch := false
	for i := version + 1; i <= currentVersion; i++ {
		log.Info().Uint64("target_version", i).Msg("Upgrading database")
		if upgrade, exists := upgrades[i]; exists {
			for i, upgradeFunc := range upgrade.funcs {
				log.Info().Int("current", i+1).Int("total", len(upgrade.funcs)).Msg("Running upgrade function")
				if err := upgradeFunc(ctx, s); err != nil {
					cancel()
					return false, errors.Wrap(err, "failed to upgrade")
				}
			}
			requiresRefetch = requiresRefetch || upgrade.requiresRefetch
		}
	}

	if err := s.setVersion(ctx, currentVersion); err != nil {
		cancel()
		return false, errors.Wrap(err, "failed to set latest schema version")
	}

	if err := s.CommitTx(ctx); err != nil {
		cancel()
		return false, errors.Wrap(err, "failed to commit upgrade transaction")
	}

	log.Info().Msg("Upgrade complete")

	return requiresRefetch, nil
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

	if _, err := tx.Exec(ctx, "CREATE INDEX i_deposits_2 ON t_deposits(f_validator_pubkey,f_inclusion_slot)"); err != nil {
		return errors.Wrap(err, "failed to create deposits index (2)")
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
		header1 := &phase0.BeaconBlockHeader{
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
		header2 := &phase0.BeaconBlockHeader{
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

// createETH1Deposits creates the t_et1_deposits table.
func createETH1Deposits(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	if _, err := tx.Exec(ctx, `
CREATE TABLE IF NOT EXISTS t_eth1_deposits (
  f_eth1_block_number      BIGINT NOT NULL
 ,f_eth1_block_hash        BYTEA NOT NULL
 ,f_eth1_block_timestamp   TIMESTAMPTZ NOT NULL
 ,f_eth1_tx_hash           BYTEA NOT NULL
 ,f_eth1_log_index         BIGINT NOT NULL
 ,f_eth1_sender            BYTEA NOT NULL
 ,f_eth1_recipient         BYTEA NOT NULL
 ,f_eth1_gas_used          BIGINT NOT NULL
 ,f_eth1_gas_price         BIGINT NOT NULL
 ,f_deposit_index          BIGINT UNIQUE NOT NULL
 ,f_validator_pubkey       BYTEA NOT NULL
 ,f_withdrawal_credentials BYTEA NOT NULL
 ,f_signature              BYTEA NOT NULL
 ,f_amount                 BIGINT NOT NULL
)`); err != nil {
		return errors.Wrap(err, "failed to create Ethereum 1 deposits table")
	}

	if _, err := tx.Exec(ctx, "CREATE UNIQUE INDEX IF NOT EXISTS i_eth1_deposits_1 ON t_eth1_deposits(f_eth1_block_hash, f_eth1_tx_hash, f_eth1_log_index)"); err != nil {
		return errors.Wrap(err, "failed to create Ethereum 1 deposits index 1")
	}
	if _, err := tx.Exec(ctx, "CREATE INDEX IF NOT EXISTS i_eth1_deposits_2 ON t_eth1_deposits(f_validator_pubkey)"); err != nil {
		return errors.Wrap(err, "failed to create Ethereum 1 deposits index 2")
	}
	if _, err := tx.Exec(ctx, "CREATE INDEX IF NOT EXISTS i_eth1_deposits_3 ON t_eth1_deposits(f_withdrawal_credentials)"); err != nil {
		return errors.Wrap(err, "failed to create Ethereum 1 deposits index 3")
	}
	if _, err := tx.Exec(ctx, "CREATE INDEX IF NOT EXISTS i_eth1_deposits_4 ON t_eth1_deposits(f_eth1_sender)"); err != nil {
		return errors.Wrap(err, "failed to create Ethereum 1 deposits index 4")
	}
	if _, err := tx.Exec(ctx, "CREATE INDEX IF NOT EXISTS i_eth1_deposits_5 ON t_eth1_deposits(f_eth1_recipient)"); err != nil {
		return errors.Wrap(err, "failed to create Ethereum 1 deposits index 5")
	}

	return nil
}

// addAttestationAggregationIndices adds aggregation indices to the t_attestations table.
func addAttestationAggregationIndices(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	// Earlier versions of the upgrade carried this out manually; check if it's already present.
	alreadyPresent, err := s.columnExists(ctx, "t_attestations", "f_aggregation_indices")
	if err != nil {
		return errors.Wrap(err, "failed to check if f_aggregation_indices is present in t_attestations")
	}
	if alreadyPresent {
		// Nothing more to do.
		return nil
	}

	if _, err := tx.Exec(ctx, `
ALTER TABLE t_attestations
ADD COLUMN f_aggregation_indices BIGINT[]
`); err != nil {
		return errors.Wrap(err, "failed to add f_aggregation_indices to attestations table")
	}

	return nil
}

// addBlocksCanonical adds canonical status to the t_blocks table.
func addBlocksCanonical(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	if _, err := tx.Exec(ctx, `
ALTER TABLE t_blocks
ADD COLUMN f_canonical BOOL
`); err != nil {
		return errors.Wrap(err, "failed to add f_canonical to blocks table")
	}

	return nil
}

// addAttestationsVoteFields adds vote-related fields the t_attestations table.
func addAttestationsVoteFields(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	if _, err := tx.Exec(ctx, `
ALTER TABLE t_attestations
ADD COLUMN f_canonical BOOL
`); err != nil {
		return errors.Wrap(err, "failed to add f_canonical to attestations table")
	}

	if _, err := tx.Exec(ctx, `
ALTER TABLE t_attestations
ADD COLUMN f_target_correct BOOL
`); err != nil {
		return errors.Wrap(err, "failed to add f_target_correct to attestations table")
	}

	if _, err := tx.Exec(ctx, `
ALTER TABLE t_attestations
ADD COLUMN f_head_correct BOOL
`); err != nil {
		return errors.Wrap(err, "failed to add f_head_correct to attestations table")
	}

	return nil
}

// columnExists returns true if the given column exists in the given table.
func (s *Service) columnExists(ctx context.Context, tableName string, columnName string) (bool, error) {
	var err error

	tx := s.tx(ctx)
	if tx == nil {
		ctx, err = s.BeginROTx(ctx)
		if err != nil {
			return false, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer s.CommitROTx(ctx)
	}

	query := fmt.Sprintf(`SELECT true
FROM pg_attribute
WHERE attrelid = '%s'::regclass
  AND attname = '%s'
  AND NOT attisdropped`, tableName, columnName)

	rows, err := tx.Query(ctx, query)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	found := false
	if rows.Next() {
		err = rows.Scan(
			&found,
		)
		if err != nil {
			return false, errors.Wrap(err, "failed to scan row")
		}
	}
	return found, nil
}

// tableExists returns true if the given table exists.
func (s *Service) tableExists(ctx context.Context, tableName string) (bool, error) {
	var err error

	tx := s.tx(ctx)
	if tx == nil {
		ctx, err = s.BeginROTx(ctx)
		if err != nil {
			return false, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer s.CommitROTx(ctx)
	}

	rows, err := tx.Query(ctx, `SELECT true
FROM information_schema.tables
WHERE table_schema = (SELECT current_schema())
  AND table_name = $1`, tableName)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	found := false
	if rows.Next() {
		err = rows.Scan(
			&found,
		)
		if err != nil {
			return false, errors.Wrap(err, "failed to scan row")
		}
	}
	return found, nil
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
	if tx := s.tx(ctx); tx == nil {
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

func dropEpochsTable(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}
	if _, err := tx.Exec(ctx, `DROP TABLE IF EXISTS t_epochs`); err != nil {
		return errors.Wrap(err, "failed to drop epochs table")
	}
	return nil
}

// createSummaryTables creates the summary tables.
func createSummaryTables(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	if _, err := tx.Exec(ctx, `CREATE TABLE t_validator_epoch_summaries (
  f_validator_index             BIGINT NOT NULL
 ,f_epoch                       BIGINT NOT NULL
 ,f_proposer_duties             INTEGER NOT NULL
 ,f_proposals_included          INTEGER NOT NULL
 ,f_attestation_included        BOOL NOT NULL
 ,f_attestation_target_correct  BOOL
 ,f_attestation_head_correct    BOOL
 ,f_attestation_inclusion_delay INTEGER
)`); err != nil {
		return errors.Wrap(err, "failed to create validator epoch summaries table")
	}
	if _, err := tx.Exec(ctx, "CREATE UNIQUE INDEX IF NOT EXISTS i_validator_epoch_summaries_1 ON t_validator_epoch_summaries(f_validator_index, f_epoch)"); err != nil {
		return errors.Wrap(err, "failed to create validator epoch summaries index 1")
	}

	if _, err := tx.Exec(ctx, `CREATE TABLE t_block_summaries (
  f_slot                             BIGINT NOT NULL
 ,f_attestations_for_block           INTEGER NOT NULL
 ,f_duplicate_attestations_for_block INTEGER NOT NULL
 ,f_votes_for_block                  INTEGER NOT NULL
)`); err != nil {
		return errors.Wrap(err, "failed to create block summaries table")
	}
	if _, err := tx.Exec(ctx, "CREATE UNIQUE INDEX IF NOT EXISTS i_block_summaries_1 ON t_block_summaries(f_slot)"); err != nil {
		return errors.Wrap(err, "failed to create block summaries index 1")
	}

	if _, err := tx.Exec(ctx, `CREATE TABLE t_epoch_summaries (
  f_epoch                            BIGINT UNIQUE NOT NULL
 ,f_activation_queue_length          BIGINT NOT NULL
 ,f_activating_validators            BIGINT NOT NULL
 ,f_active_validators                BIGINT NOT NULL
 ,f_active_real_balance              BIGINT NOT NULL
 ,f_active_balance                   BIGINT NOT NULL
 ,f_attesting_validators             BIGINT NOT NULL
 ,f_attesting_balance                BIGINT NOT NULL
 ,f_target_correct_validators        BIGINT NOT NULL
 ,f_target_correct_balance           BIGINT NOT NULL
 ,f_head_correct_validators          BIGINT NOT NULL
 ,f_head_correct_balance             BIGINT NOT NULL
 ,f_attestations_for_epoch           BIGINT NOT NULL
 ,f_attestations_in_epoch            BIGINT NOT NULL
 ,f_duplicate_attestations_for_epoch BIGINT NOT NULL
 ,f_proposer_slashings               BIGINT NOT NULL
 ,f_attester_slashings               BIGINT NOT NULL
 ,f_deposits                         BIGINT NOT NULL
 ,f_exiting_validators               BIGINT NOT NULL
 ,f_canonical_blocks                 BIGINT NOT NULL
)`); err != nil {
		return errors.Wrap(err, "failed to create epoch summaries table")
	}

	return nil
}

// setValidatorBalancesMetadata sets the validator balances field in the validator metadata.
func setValidatorBalancesMetadata(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		ctx, cancel, err := s.BeginTx(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer cancel()
	}

	var latestValidatorBalanceEpoch uint64
	err := tx.QueryRow(ctx, `
SELECT MIN(missed)
FROM generate_series(0,(SELECT MAX(f_epoch)+1 FROM t_validator_balances),1) missed
LEFT JOIN t_validator_balances
  ON missed = t_validator_balances.f_epoch
WHERE f_epoch IS NULL`,
	).Scan(
		&latestValidatorBalanceEpoch,
	)
	if err != nil {
		return errors.Wrap(err, "failed to obtain maximum validator balance epoch")
	}
	if latestValidatorBalanceEpoch > 0 {
		latestValidatorBalanceEpoch--
	}

	_, err = tx.Exec(ctx, fmt.Sprintf(`
UPDATE t_metadata
SET f_value = f_value || '{"latest_balances_epoch":%d}'::JSONB
WHERE f_key = 'validators.standard'`,
		latestValidatorBalanceEpoch),
	)
	if err != nil {
		return errors.Wrap(err, "failed to set maximum validator balance epoch")
	}

	return nil
}

// createForkSchedule creates the t_fork_schedule table.
func createForkSchedule(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	if _, err := tx.Exec(ctx, `
CREATE TABLE t_fork_schedule (
  f_version BYTEA UNIQUE NOT NULL
 ,f_epoch   BIGINT NOT NULL
 ,f_previous_version BYTEA NOT NULL
)
`); err != nil {
		return errors.Wrap(err, "failed to create fork schedule table")
	}

	return nil
}

// createSyncCommittees creates the t_sync_committees table.
func createSyncCommittees(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	if _, err := tx.Exec(ctx, `
CREATE TABLE t_sync_committees (
  f_period    BIGINT NOT NULL
 ,f_committee BIGINT[] NOT NULL -- REFERENCES t_validators(f_index)
)
`); err != nil {
		return errors.Wrap(err, "failed to create sync committees table")
	}

	if _, err := tx.Exec(ctx, `
CREATE UNIQUE INDEX i_sync_committees_1 ON t_sync_committees(f_period)
`); err != nil {
		return errors.Wrap(err, "failed to create sync committees index")
	}

	return nil
}

// createSyncAggregates creates the t_sync_aggregates table.
func createSyncAggregates(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	if _, err := tx.Exec(ctx, `
CREATE TABLE t_sync_aggregates (
  f_inclusion_slot       BIGINT NOT NULL
 ,f_inclusion_block_root BYTEA NOT NULL REFERENCES t_blocks(f_root) ON DELETE CASCADE
 ,f_bits                 BYTEA NOT NULL
 ,f_indices              BIGINT[] -- REFERENCES t_validators(f_index)
)
`); err != nil {
		return errors.Wrap(err, "failed to create sync aggregates table")
	}

	if _, err := tx.Exec(ctx, `
CREATE UNIQUE INDEX i_sync_aggregates_1 ON t_sync_aggregates(f_inclusion_slot, f_inclusion_block_root)
`); err != nil {
		return errors.Wrap(err, "failed to create sync aggregates index")
	}

	return nil
}

// fixSyncAggregatesIndex fixes the t_sync_aggregates table index.
func fixSyncAggregatesIndex(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	// See if we need to fix it.
	var goodIndices uint64
	err := tx.QueryRow(ctx, `
SELECT COUNT(*)
FROM pg_indexes
WHERE indexname = 'i_sync_aggregates_1'
  AND indexdef LIKE '%f_inclusion_block_root%'
`).Scan(
		&goodIndices,
	)
	if err != nil {
		return errors.Wrap(err, "failed to obtain sync aggregates index information")
	}
	log.Info().Uint64("good_indices", goodIndices).Msg("Found number of good indices")

	if goodIndices == 1 {
		// The index is already good.
		return nil
	}

	log.Info().Msg("Dropping existing index")
	if _, err := tx.Exec(ctx, `DROP INDEX IF EXISTS i_sync_aggregates_1`); err != nil {
		return errors.Wrap(err, "failed to drop i_sync_aggregates_1 index")
	}

	log.Info().Msg("Creating new index")
	if _, err := tx.Exec(ctx, `
CREATE UNIQUE INDEX i_sync_aggregates_1 ON t_sync_aggregates(f_inclusion_slot, f_inclusion_block_root)
`); err != nil {
		return errors.Wrap(err, "failed to create i_sync_aggregates_1 index")
	}

	return nil
}

// Init initialises the database.
func (s *Service) Init(ctx context.Context) error {
	ctx, cancel, err := s.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin initial tables transaction")
	}
	tx := s.tx(ctx)
	if tx == nil {
		cancel()
		return ErrNoTransaction
	}

	if _, err := tx.Exec(ctx, `
-- t_metadata stores data about chaind processing functions.
CREATE TABLE t_metadata (
  f_key    TEXT NOT NULL PRIMARY KEY
 ,f_value JSONB NOT NULL
);
CREATE UNIQUE INDEX i_metadata_1 ON t_metadata(f_key);

-- t_chain_spec contains the specification of the chain to which the rest of
-- the tables relate.
CREATE TABLE t_chain_spec (
  f_key TEXT NOT NULL PRIMARY KEY
 ,f_value TEXT NOT NULL
);

-- t_genesis contains the genesis parameters of the chain.
CREATE TABLE t_genesis (
  f_validators_root BYTEA NOT NULL PRIMARY KEY
 ,f_time TIMESTAMPTZ NOT NULL
 ,f_fork_version BYTEA NOT NULL
);

-- t_validators contains all validators known by the chain.
-- This information is not stored per-epoch, as the latest values contain the
-- majority of information required to calculate all state about the validator.
CREATE TABLE t_validators (
  f_public_key                   BYTEA NOT NULL
 ,f_index                        BIGINT NOT NULL
 ,f_slashed                      BOOLEAN NOT NULL
 ,f_activation_eligibility_epoch BIGINT
 ,f_activation_epoch             BIGINT
 ,f_exit_epoch                   BIGINT
 ,f_withdrawable_epoch           BIGINT
 ,f_effective_balance            BIGINT NOT NULL
 ,f_withdrawal_credentials		 BYTEA NOT NULL
);
CREATE UNIQUE INDEX i_validators_1 ON t_validators(f_index);
CREATE UNIQUE INDEX i_validators_2 ON t_validators(f_public_key);
CREATE INDEX i_validators_3 ON t_validators(f_withdrawal_credentials);

-- t_blocks contains all blocks proposed by validators.
-- N.B. it is possible for multiple valid blocks to be proposed in a single slot
-- by different proposers in the case of a chain re-org.
CREATE TABLE t_blocks (
  f_slot               BIGINT NOT NULL
 ,f_proposer_index     BIGINT NOT NULL
 ,f_root               BYTEA NOT NULL
 ,f_graffiti           BYTEA NOT NULL
 ,f_randao_reveal      BYTEA NOT NULL
 ,f_body_root          BYTEA NOT NULL
 ,f_parent_root        BYTEA NOT NULL
 ,f_state_root         BYTEA NOT NULL
  -- f_canonical can have one of the following values:
  -- - true if it is canonical
  -- - false if it is not canonical
  -- - NULL if it has yet to be determined
 ,f_canonical          BOOL
 ,f_eth1_block_hash    BYTEA NOT NULL
 ,f_eth1_deposit_count BIGINT NOT NULL
 ,f_eth1_deposit_root  BYTEA NOT NULL
);
CREATE UNIQUE INDEX i_blocks_1 ON t_blocks(f_slot,f_root);
CREATE UNIQUE INDEX i_blocks_2 ON t_blocks(f_root);
CREATE INDEX i_blocks_3 ON t_blocks(f_parent_root);

-- t_block_execution_payloads is a subtable for t_blocks.
CREATE TABLE t_block_execution_payloads (
  f_block_root       BYTEA UNIQUE NOT NULL REFERENCES t_blocks(f_root) ON DELETE CASCADE
 ,f_block_number     BIGINT NOT NULL
 ,f_block_hash       BYTEA NOT NULL
 ,f_parent_hash      BYTEA NOT NULL
 ,f_fee_recipient    BYTEA NOT NULL
 ,f_state_root       BYTEA NOT NULL
 ,f_receipts_root    BYTEA NOT NULL
 ,f_logs_bloom       BYTEA NOT NULL
 ,f_prev_randao      BYTEA NOT NULL
 ,f_gas_limit        BIGINT NOT NULL
 ,f_gas_used         BIGINT NOT NULL
 ,f_base_fee_per_gas NUMERIC NOT NULL
 ,f_extra_data       BYTEA
 ,f_timestamp        BIGINT NOT NULL
);

-- t_beacon_committees contains all beacon committees.
-- N.B. in the case of a chain re-org the committees can alter.
CREATE TABLE t_beacon_committees (
  f_slot BIGINT NOT NULL
 ,f_index BIGINT NOT NULL
 ,f_committee BIGINT[] NOT NULL -- REFERENCES t_validators(f_index)
);
CREATE UNIQUE INDEX i_beacon_committees_1 ON t_beacon_committees(f_slot, f_index);

-- t_proposer_duties contains all proposer duties.
-- N.B. in the case of a chain re-org the duties can alter.
CREATE TABLE t_proposer_duties (
  f_slot BIGINT NOT NULL
 ,f_validator_index BIGINT NOT NULL -- REFERENCES t_validators(f_index)
);
CREATE UNIQUE INDEX i_proposer_duties_1 ON t_proposer_duties(f_slot);

-- t_attestations contains all attestations included in blocks.
CREATE TABLE t_attestations (
  f_inclusion_slot       BIGINT NOT NULL
 ,f_inclusion_block_root BYTEA NOT NULL REFERENCES t_blocks(f_root) ON DELETE CASCADE
 ,f_inclusion_index      BIGINT NOT NULL
 ,f_slot                 BIGINT NOT NULL
 ,f_committee_index      BIGINT NOT NULL
 ,f_aggregation_bits     BYTEA NOT NULL
 ,f_aggregation_indices  BIGINT[] -- REFERENCES t_validators(f_index)
 ,f_beacon_block_root    BYTEA NOT NULL -- we don't reference this because the block may not exist in the canonical chain
 ,f_source_epoch         BIGINT NOT NULL
 ,f_source_root          BYTEA NOT NULL
 ,f_target_epoch         BIGINT NOT NULL
 ,f_target_root          BYTEA NOT NULL
 ,f_canonical            BOOL
 ,f_target_correct       BOOL
 ,f_head_correct         BOOL
);
CREATE UNIQUE INDEX i_attestations_1 ON t_attestations(f_inclusion_slot,f_inclusion_block_root,f_inclusion_index);
CREATE INDEX i_attestations_2 ON t_attestations(f_slot);
CREATE INDEX i_attestations_3 ON t_attestations(f_beacon_block_root);

-- t_sync_aggregates contains the sync committee aggregates included in blocks.
CREATE TABLE t_sync_aggregates (
  f_inclusion_slot       BIGINT NOT NULL
 ,f_inclusion_block_root BYTEA NOT NULL REFERENCES t_blocks(f_root) ON DELETE CASCADE
 ,f_bits                 BYTEA NOT NULL
 ,f_indices              BIGINT[] -- REFERENCES t_validators(f_index)
);
CREATE UNIQUE INDEX i_sync_aggregates_1 ON t_sync_aggregates(f_inclusion_slot, f_inclusion_block_root);

-- t_attester_slashings contains all attester slashings included in blocks.
CREATE TABLE t_attester_slashings (
  f_inclusion_slot                  BIGINT NOT NULL
 ,f_inclusion_block_root            BYTEA NOT NULL REFERENCES t_blocks(f_root) ON DELETE CASCADE
 ,f_inclusion_index                 BIGINT NOT NULL
 ,f_attestation_1_indices           BIGINT[] NOT NULL -- REFERENCES t_validators(f_index)
 ,f_attestation_1_slot              BIGINT NOT NULL
 ,f_attestation_1_committee_index   BIGINT NOT NULL
 ,f_attestation_1_beacon_block_root BYTEA NOT NULL
 ,f_attestation_1_source_epoch      BIGINT NOT NULL
 ,f_attestation_1_source_root       BYTEA NOT NULL
 ,f_attestation_1_target_epoch      BIGINT NOT NULL
 ,f_attestation_1_target_root       BYTEA NOT NULL
 ,f_attestation_1_signature         BYTEA NOT NULL
 ,f_attestation_2_indices           BIGINT[] NOT NULL -- REFERENCES t_validators(f_index)
 ,f_attestation_2_slot              BIGINT NOT NULL
 ,f_attestation_2_committee_index   BIGINT NOT NULL
 ,f_attestation_2_beacon_block_root BYTEA NOT NULL
 ,f_attestation_2_source_epoch      BIGINT NOT NULL
 ,f_attestation_2_source_root       BYTEA NOT NULL
 ,f_attestation_2_target_epoch      BIGINT NOT NULL
 ,f_attestation_2_target_root       BYTEA NOT NULL
 ,f_attestation_2_signature         BYTEA NOT NULL
);
CREATE UNIQUE INDEX i_attester_slashings_1 ON t_attester_slashings(f_inclusion_slot,f_inclusion_block_root,f_inclusion_index);

-- t_proposer_slashings contains all proposer slashings included in blocks.
CREATE TABLE t_proposer_slashings (
  f_inclusion_slot          BIGINT NOT NULL
 ,f_inclusion_block_root    BYTEA NOT NULL REFERENCES t_blocks(f_root) ON DELETE CASCADE
 ,f_inclusion_index         BIGINT NOT NULL
 ,f_block_1_root            BYTEA NOT NULL
 ,f_header_1_slot           BIGINT NOT NULL
 ,f_header_1_proposer_index BIGINT NOT NULL
 ,f_header_1_parent_root    BYTEA NOT NULL
 ,f_header_1_state_root     BYTEA NOT NULL
 ,f_header_1_body_root      BYTEA NOT NULL
 ,f_header_1_signature      BYTEA NOT NULL
 ,f_block_2_root            BYTEA NOT NULL
 ,f_header_2_slot           BIGINT NOT NULL
 ,f_header_2_proposer_index BIGINT NOT NULL
 ,f_header_2_parent_root    BYTEA NOT NULL
 ,f_header_2_state_root     BYTEA NOT NULL
 ,f_header_2_body_root      BYTEA NOT NULL
 ,f_header_2_signature      BYTEA NOT NULL
);
CREATE UNIQUE INDEX i_proposer_slashings_1 ON t_proposer_slashings(f_inclusion_slot,f_inclusion_block_root,f_inclusion_index);

-- t_voluntary_exits contains all voluntary exits included in blocks.
CREATE TABLE t_voluntary_exits (
  f_inclusion_slot       BIGINT NOT NULL
 ,f_inclusion_block_root BYTEA NOT NULL REFERENCES t_blocks(f_root) ON DELETE CASCADE
 ,f_inclusion_index      BIGINT NOT NULL
 ,f_validator_index      BIGINT NOT NULL
 ,f_epoch                BIGINT NOT NULL
);
CREATE UNIQUE INDEX i_voluntary_exits_1 ON t_voluntary_exits(f_inclusion_slot,f_inclusion_block_root,f_inclusion_index);

-- t_deposits contains all deposits included in blocks.
CREATE TABLE t_deposits (
  f_inclusion_slot         BIGINT NOT NULL
 ,f_inclusion_block_root   BYTEA NOT NULL REFERENCES t_blocks(f_root) ON DELETE CASCADE
 ,f_inclusion_index        BIGINT NOT NULL
 ,f_validator_pubkey       BYTEA NOT NULL
 ,f_withdrawal_credentials BYTEA NOT NULL
 ,f_amount                 BIGINT NOT NULL
);
CREATE UNIQUE INDEX i_deposits_1 ON t_deposits(f_inclusion_slot,f_inclusion_block_root,f_inclusion_index);
CREATE INDEX i_deposits_2 ON t_deposits(f_validator_pubkey,f_inclusion_slot);

-- t_eth1_deposits stores information about each Ethereum 1 deposit that has occurred for the deposit contract.
CREATE TABLE t_eth1_deposits (
  f_eth1_block_number      BIGINT NOT NULL
 ,f_eth1_block_hash        BYTEA NOT NULL
 ,f_eth1_block_timestamp   TIMESTAMPTZ NOT NULL
 ,f_eth1_tx_hash           BYTEA NOT NULL
 ,f_eth1_log_index         BIGINT NOT NULL
 ,f_eth1_sender            BYTEA NOT NULL
 ,f_eth1_recipient         BYTEA NOT NULL
 ,f_eth1_gas_used          BIGINT NOT NULL
 ,f_eth1_gas_price         BIGINT NOT NULL
 ,f_deposit_index          BIGINT UNIQUE NOT NULL
 ,f_validator_pubkey       BYTEA NOT NULL
 ,f_withdrawal_credentials BYTEA NOT NULL
 ,f_signature              BYTEA NOT NULL
 ,f_amount                 BIGINT NOT NULL
);
CREATE UNIQUE INDEX i_eth1_deposits_1 ON t_eth1_deposits(f_eth1_block_hash, f_eth1_tx_hash, f_eth1_log_index);
CREATE INDEX i_eth1_deposits_2 ON t_eth1_deposits(f_validator_pubkey);
CREATE INDEX i_eth1_deposits_3 ON t_eth1_deposits(f_withdrawal_credentials);
CREATE INDEX i_eth1_deposits_4 ON t_eth1_deposits(f_eth1_sender);
CREATE INDEX i_eth1_deposits_5 ON t_eth1_deposits(f_eth1_recipient);

-- t_validator_balances contains per-epoch balances.
CREATE TABLE t_validator_balances (
  f_validator_index   BIGINT NOT NULL REFERENCES t_validators(f_index) ON DELETE CASCADE
 ,f_epoch             BIGINT NOT NULL
 ,f_balance           BIGINT NOT NULL
 ,f_effective_balance BIGINT NOT NULL
);
CREATE UNIQUE INDEX i_validator_balances_1 ON t_validator_balances(f_validator_index, f_epoch);
CREATE INDEX i_validator_balances_2 ON t_validator_balances(f_epoch);

CREATE TABLE t_validator_epoch_summaries (
  f_validator_index             BIGINT NOT NULL
 ,f_epoch                       BIGINT NOT NULL
 ,f_proposer_duties             INTEGER NOT NULL
 ,f_proposals_included          INTEGER NOT NULL
 ,f_attestation_included        BOOL NOT NULL
 ,f_attestation_source_timely   BOOL
 ,f_attestation_target_correct  BOOL
 ,f_attestation_target_timely   BOOL
 ,f_attestation_head_correct    BOOL
 ,f_attestation_head_timely     BOOL
 ,f_attestation_inclusion_delay INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS i_validator_epoch_summaries_1 ON t_validator_epoch_summaries(f_validator_index, f_epoch);

CREATE TABLE t_block_summaries (
  f_slot                             BIGINT NOT NULL
 ,f_attestations_for_block           INTEGER NOT NULL
 ,f_duplicate_attestations_for_block INTEGER NOT NULL
 ,f_votes_for_block                  INTEGER NOT NULL
 ,f_parent_distance                  INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS i_block_summaries_1 ON t_block_summaries(f_slot);

CREATE TABLE t_epoch_summaries (
  f_epoch                            BIGINT UNIQUE NOT NULL
 ,f_activation_queue_length          BIGINT NOT NULL
 ,f_activating_validators            BIGINT NOT NULL
 ,f_active_validators                BIGINT NOT NULL
 ,f_active_real_balance              BIGINT NOT NULL
 ,f_active_balance                   BIGINT NOT NULL
 ,f_attesting_validators             BIGINT NOT NULL
 ,f_attesting_balance                BIGINT NOT NULL
 ,f_target_correct_validators        BIGINT NOT NULL
 ,f_target_correct_balance           BIGINT NOT NULL
 ,f_head_correct_validators          BIGINT NOT NULL
 ,f_head_correct_balance             BIGINT NOT NULL
 ,f_attestations_for_epoch           BIGINT NOT NULL
 ,f_attestations_in_epoch            BIGINT NOT NULL
 ,f_duplicate_attestations_for_epoch BIGINT NOT NULL
 ,f_proposer_slashings               BIGINT NOT NULL
 ,f_attester_slashings               BIGINT NOT NULL
 ,f_deposits                         BIGINT NOT NULL
 ,f_exiting_validators               BIGINT NOT NULL
 ,f_canonical_blocks                 BIGINT NOT NULL
);

CREATE TABLE t_fork_schedule (
  f_version BYTEA UNIQUE NOT NULL
 ,f_epoch   BIGINT NOT NULL
 ,f_previous_version BYTEA NOT NULL
);

CREATE TABLE t_sync_committees (
  f_period    BIGINT NOT NULL
 ,f_committee BIGINT[] NOT NULL -- REFERENCES t_validators(f_index)
);
CREATE UNIQUE INDEX IF NOT EXISTS i_sync_committees_1 ON t_sync_committees(f_period);

CREATE TABLE t_validator_day_summaries (
  f_validator_index                  BIGINT NOT NULL
 ,f_start_timestamp                  TIMESTAMPTZ NOT NULL
 ,f_start_balance                    BIGINT NOT NULL
 ,f_start_effective_balance          BIGINT NOT NULL
 ,f_capital_change                   BIGINT NOT NULL
 ,f_reward_change                    BIGINT NOT NULL
 ,f_effective_balance_change         BIGINT NOT NULL
 ,f_proposals                        INTEGER NOT NULL
 ,f_proposals_included               INTEGER NOT NULL
 ,f_attestations                     INTEGER NOT NULL
 ,f_attestations_included            INTEGER NOT NULL
 ,f_attestations_source_timely       INTEGER NOT NULL
 ,f_attestations_target_correct      INTEGER NOT NULL
 ,f_attestations_target_timely       INTEGER NOT NULL
 ,f_attestations_head_correct        INTEGER NOT NULL
 ,f_attestations_head_timely         INTEGER NOT NULL
 ,f_attestations_inclusion_delay     FLOAT(4) NOT NULL
 ,f_sync_committee_messages          INTEGER NOT NULL
 ,f_sync_committee_messages_included INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS i_validator_day_summaries_1 ON t_validator_day_summaries(f_validator_index, f_start_timestamp);
CREATE INDEX IF NOT EXISTS i_validator_day_summaries_2 ON t_validator_day_summaries(f_start_timestamp);

CREATE TABLE t_block_bls_to_execution_changes (
  f_block_root            BYTEA   NOT NULL REFERENCES t_blocks(f_root) ON DELETE CASCADE
 ,f_block_number          BIGINT  NOT NULL
 ,f_index                 INTEGER NOT NULL
 ,f_validator_index       BIGINT  NOT NULL
 ,f_from_bls_pubkey       BYTEA   NOT NULL
 ,f_to_execution_address  BYTEA   NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS i_block_bls_to_execution_changes_1 ON t_block_bls_to_execution_changes(f_block_root,f_block_number,f_index);
CREATE INDEX IF NOT EXISTS i_block_bls_to_execution_changes_2 ON t_block_bls_to_execution_changes(f_block_number);
CREATE INDEX IF NOT EXISTS i_block_bls_to_execution_changes_3 ON t_block_bls_to_execution_changes(f_to_execution_address);

-- t_block_withdrawals is a subtable for t_blocks.
-- This data is actually part of the execution payload, but flattened for our purposes.
CREATE TABLE t_block_withdrawals (
  f_block_root       BYTEA   NOT NULL REFERENCES t_blocks(f_root) ON DELETE CASCADE
 ,f_block_number     BIGINT  NOT NULL
 ,f_index            INTEGER NOT NULL
 ,f_withdrawal_index INTEGER NOT NULL
 ,f_validator_index  BIGINT  NOT NULL
 ,f_address          BYTEA   NOT NULL
 ,f_amount           BIGINT  NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS i_block_withdrawals_1 ON t_block_withdrawals(f_block_root,f_block_number,f_index);
CREATE INDEX IF NOT EXISTS i_block_withdrawals_2 ON t_block_withdrawals(f_block_number);
CREATE INDEX IF NOT EXISTS i_block_withdrawals_3 ON t_block_withdrawals(f_validator_index);
CREATE INDEX IF NOT EXISTS i_block_withdrawals_4 ON t_block_withdrawals(f_address);
`); err != nil {
		cancel()
		return errors.Wrap(err, "failed to create initial tables")
	}

	if err := s.setVersion(ctx, currentVersion); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set initial schema version")
	}

	if err := s.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to commit initial tables transaction")
	}

	return nil
}

// addValidatorSummaryTimely adds timely fields to the t_validator_epoch_summaries table.
func addValidatorSummaryTimely(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	if _, err := tx.Exec(ctx, `
ALTER TABLE t_validator_epoch_summaries
ADD COLUMN f_attestation_source_timely BOOL
`); err != nil {
		return errors.Wrap(err, "failed to add f_attestation_source_timely to validator epoch summaries table")
	}

	if _, err := tx.Exec(ctx, `
ALTER TABLE t_validator_epoch_summaries
ADD COLUMN f_attestation_target_timely BOOL
`); err != nil {
		return errors.Wrap(err, "failed to add f_attestation_target_timely to validator epoch summaries table")
	}

	if _, err := tx.Exec(ctx, `
ALTER TABLE t_validator_epoch_summaries
ADD COLUMN f_attestation_head_timely BOOL
`); err != nil {
		return errors.Wrap(err, "failed to add f_attestation_head_timely to validator epoch summaries table")
	}

	return nil
}

// addDepositsIndex adds the i_deposits_2 index to the t_deposits table.
func addDepositsIndex(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	if _, err := tx.Exec(ctx, "CREATE INDEX IF NOT EXISTS i_deposits_2 ON t_deposits(f_validator_pubkey,f_inclusion_slot)"); err != nil {
		return errors.Wrap(err, "failed to create deposits index (2)")
	}

	return nil
}

// addBlockParentDistance adds f_parent_distance index to the t_block_summaries table.
func addBlockParentDistance(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	// This exists in the initial SQL, so don't attempt to add it if already present.
	alreadyPresent, err := s.columnExists(ctx, "t_block_summaries", "f_parent_distance")
	if err != nil {
		return errors.Wrap(err, "failed to check if f_parent_distance is present in t_block_summaries")
	}
	if alreadyPresent {
		// Nothing more to do.
		return nil
	}

	// Add column.
	if _, err := tx.Exec(ctx, `
ALTER TABLE t_block_summaries
ADD COLUMN f_parent_distance INTEGER
`); err != nil {
		return errors.Wrap(err, "failed to add f_parent_distance to block epoch summaries table")
	}

	// Populate column.
	if _, err := tx.Exec(ctx, `
WITH new_values AS (
  WITH cte AS (
    SELECT f_slot,f_parent_root
	FROM t_blocks
	WHERE f_slot != 0
  )
  SELECT cte.f_slot
        ,cte.f_slot - t_blocks.f_slot AS f_parent_distance
  FROM cte
  LEFT JOIN t_blocks ON cte.f_parent_root = t_blocks.f_root
  UNION
  SELECT 0 AS f_slot, 0 as f_parent_distance
)
UPDATE t_block_summaries
SET f_parent_distance = new_values.f_parent_distance
FROM new_values
WHERE new_values.f_slot = t_block_summaries.f_slot
`); err != nil {
		return errors.Wrap(err, "failed to drop NOT NULL constraint on f_parent_distance")
	}

	// Drop NOT NULL constraints.
	if _, err := tx.Exec(ctx, `
ALTER TABLE t_block_summaries
ALTER COLUMN f_parent_distance
SET NOT NULL
`); err != nil {
		return errors.Wrap(err, "failed to drop NOT NULL constraint on f_parent_distance")
	}

	return nil
}

// addBellatrixSubtable adds bellatrix information as a subtable of t_blocks.
func addBellatrixSubtable(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	// This exists in the initial SQL, so don't attempt to add it if already present.
	alreadyPresent, err := s.tableExists(ctx, "t_block_execution_payloads")
	if err != nil {
		return errors.Wrap(err, "failed to check if t_block_execution_payloads exist")
	}
	if alreadyPresent {
		// Nothing more to do.
		return nil
	}

	// Add columns.
	if _, err := tx.Exec(ctx, `
CREATE TABLE t_block_execution_payloads (
  f_block_root       BYTEA UNIQUE NOT NULL REFERENCES t_blocks(f_root) ON DELETE CASCADE
 ,f_block_number     BIGINT NOT NULL
 ,f_block_hash       BYTEA NOT NULL
 ,f_parent_hash      BYTEA NOT NULL
 ,f_fee_recipient    BYTEA NOT NULL
 ,f_state_root       BYTEA NOT NULL
 ,f_receipts_root    BYTEA NOT NULL
 ,f_logs_bloom       BYTEA NOT NULL
 ,f_prev_randao      BYTEA NOT NULL
 ,f_gas_limit        BIGINT NOT NULL
 ,f_gas_used         BIGINT NOT NULL
 ,f_base_fee_per_gas NUMERIC NOT NULL
 ,f_extra_data       BYTEA
)
`); err != nil {
		return errors.Wrap(err, "failed to create subtable t_block_execution_payloads")
	}

	return nil
}

// addTimestamp adds timestamp to t_block_execution_payloads.
func addTimestamp(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	// This exists in the initial SQL, so don't attempt to add it if already present.
	alreadyPresent, err := s.columnExists(ctx, "t_block_execution_payloads", "f_timestamp")
	if err != nil {
		return errors.Wrap(err, "failed to check if f_timestamp exists in t_block_execution_payloads")
	}
	if alreadyPresent {
		// Nothing more to do.
		return nil
	}

	// Add column.
	if _, err := tx.Exec(ctx, `
ALTER TABLE t_block_execution_payloads
ADD COLUMN f_timestamp BIGINT NOT NULL DEFAULT 0
`); err != nil {
		return errors.Wrap(err, "failed to add f_timestamp to t_block_execution_payloads")
	}

	// Set value for the column.
	// The only merged chain at time this was placed is kiln, so use those values.
	genesisTime := 1647007500
	slotDuration := 12 * time.Second

	if _, err := tx.Exec(ctx, fmt.Sprintf(`
UPDATE t_block_execution_payloads
SET f_timestamp = (SELECT %d + f_slot*%d FROM t_blocks WHERE t_blocks.f_root = t_block_execution_payloads.f_block_root)
`, genesisTime, int(slotDuration.Seconds()))); err != nil {
		return errors.Wrap(err, "failed to add f_timestamp to t_block_execution_payloads")
	}

	// Drop default.
	if _, err := tx.Exec(ctx, `
ALTER TABLE t_block_execution_payloads
ALTER COLUMN f_timestamp DROP DEFAULT
`); err != nil {
		return errors.Wrap(err, "failed to remove default for f_timestamp in t_block_execution_payloads")
	}

	return nil
}

// createValidatorDaySummaries adds t_validator_day_summaries.
func createValidatorDaySummaries(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	if _, err := tx.Exec(ctx, `
CREATE TABLE t_validator_day_summaries (
  f_validator_index                  BIGINT NOT NULL
 ,f_start_timestamp                  TIMESTAMPTZ NOT NULL
 ,f_start_balance                    BIGINT NOT NULL
 ,f_start_effective_balance          BIGINT NOT NULL
 ,f_capital_change                   BIGINT NOT NULL
 ,f_reward_change                    BIGINT NOT NULL
 ,f_effective_balance_change         BIGINT NOT NULL
 ,f_proposals                        INTEGER NOT NULL
 ,f_proposals_included               INTEGER NOT NULL
 ,f_attestations                     INTEGER NOT NULL
 ,f_attestations_included            INTEGER NOT NULL
 ,f_attestations_source_timely       INTEGER NOT NULL
 ,f_attestations_target_correct      INTEGER NOT NULL
 ,f_attestations_target_timely       INTEGER NOT NULL
 ,f_attestations_head_correct        INTEGER NOT NULL
 ,f_attestations_head_timely         INTEGER NOT NULL
 ,f_attestations_inclusion_delay     FLOAT(4) NOT NULL
 ,f_sync_committee_messages          INTEGER NOT NULL
 ,f_sync_committee_messages_included INTEGER NOT NULL
)
`); err != nil {
		return errors.Wrap(err, "failed to create validator day summaries table")
	}

	if _, err := tx.Exec(ctx, `
CREATE UNIQUE INDEX IF NOT EXISTS i_validator_day_summaries_1 ON t_validator_day_summaries(f_validator_index, f_start_timestamp)
`); err != nil {
		return errors.Wrap(err, "failed to create validator day summaries index 1")
	}

	if _, err := tx.Exec(ctx, `
CREATE INDEX IF NOT EXISTS i_validator_day_summaries_2 ON t_validator_day_summaries(f_start_timestamp)
`); err != nil {
		return errors.Wrap(err, "failed to create validator day summaries index 1")
	}
	return nil
}

// createBlockBLSToExecutionChanges adds t_block_bls_to_execution_changes.
func createBlockBLSToExecutionChanges(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	if _, err := tx.Exec(ctx, `
CREATE TABLE t_block_bls_to_execution_changes (
  f_block_root            BYTEA   NOT NULL REFERENCES t_blocks(f_root) ON DELETE CASCADE
 ,f_block_number          BIGINT  NOT NULL
 ,f_index                 INTEGER NOT NULL
 ,f_validator_index       BIGINT  NOT NULL
 ,f_from_bls_pubkey       BYTEA   NOT NULL
 ,f_to_execution_address  BYTEA   NOT NULL
)
`); err != nil {
		return errors.Wrap(err, "failed to create block bls to execution changes table")
	}

	if _, err := tx.Exec(ctx, `
CREATE UNIQUE INDEX IF NOT EXISTS i_block_bls_to_execution_changes_1 ON t_block_bls_to_execution_changes(f_block_root,f_block_number,f_index);
`); err != nil {
		return errors.Wrap(err, "failed to create block bls to execution changes index 1")
	}

	if _, err := tx.Exec(ctx, `
CREATE INDEX IF NOT EXISTS i_block_bls_to_execution_changes_2 ON t_block_bls_to_execution_changes(f_block_number);
`); err != nil {
		return errors.Wrap(err, "failed to create block bls to execution changes index 2")
	}

	if _, err := tx.Exec(ctx, `
CREATE INDEX IF NOT EXISTS i_block_bls_to_execution_changes_3 ON t_block_bls_to_execution_changes(f_to_execution_address);
`); err != nil {
		return errors.Wrap(err, "failed to create block bls to execution changes index 3")
	}

	if _, err := tx.Exec(ctx, `
CREATE INDEX IF NOT EXISTS i_block_bls_to_execution_changes_4 ON t_block_bls_to_execution_changes(f_validator_index);
`); err != nil {
		return errors.Wrap(err, "failed to create block bls to execution changes index 4")
	}

	return nil
}

// createBlockWithdrawals adds t_block_withdrawals.
func createBlockWithdrawals(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	if _, err := tx.Exec(ctx, `
CREATE TABLE t_block_withdrawals (
  f_block_root       BYTEA   NOT NULL REFERENCES t_blocks(f_root) ON DELETE CASCADE
 ,f_block_number     BIGINT  NOT NULL
 ,f_index            INTEGER NOT NULL
 ,f_withdrawal_index INTEGER NOT NULL
 ,f_validator_index  BIGINT  NOT NULL
 ,f_address          BYTEA   NOT NULL
 ,f_amount           BIGINT  NOT NULL
)
`); err != nil {
		return errors.Wrap(err, "failed to create block withdrawals table")
	}

	if _, err := tx.Exec(ctx, `
CREATE UNIQUE INDEX IF NOT EXISTS i_block_withdrawals_1 ON t_block_withdrawals(f_block_root,f_block_number,f_index);
`); err != nil {
		return errors.Wrap(err, "failed to create block withdrawals index 1")
	}

	if _, err := tx.Exec(ctx, `
CREATE INDEX IF NOT EXISTS i_block_withdrawals_2 ON t_block_withdrawals(f_block_number);
`); err != nil {
		return errors.Wrap(err, "failed to create block withdrawals index 2")
	}

	if _, err := tx.Exec(ctx, `
CREATE INDEX IF NOT EXISTS i_block_withdrawals_3 ON t_block_withdrawals(f_validator_index);
`); err != nil {
		return errors.Wrap(err, "failed to create block withdrawals index 3")
	}

	return nil
}

func recreateForkSchedule(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}
	if _, err := tx.Exec(ctx, `
DROP TABLE t_fork_schedule
`); err != nil {
		return errors.Wrap(err, "failed to drop fork schedule")
	}
	if _, err := tx.Exec(ctx, `
CREATE TABLE t_fork_schedule (
  f_version BYTEA UNIQUE NOT NULL
 ,f_epoch   BIGINT NOT NULL
 ,f_previous_version BYTEA NOT NULL
)
`); err != nil {
		return errors.Wrap(err, "failed to create fork schedule")
	}

	return nil
}

func recreateValidators(ctx context.Context, s *Service) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	if _, err := tx.Exec(ctx, `
CREATE TABLE t_validators (
  f_public_key                   BYTEA NOT NULL
 ,f_index                        BIGINT NOT NULL
 ,f_slashed                      BOOLEAN NOT NULL
 ,f_activation_eligibility_epoch BIGINT
 ,f_activation_epoch             BIGINT
 ,f_exit_epoch                   BIGINT
 ,f_withdrawable_epoch           BIGINT
 ,f_effective_balance            BIGINT NOT NULL
 ,f_withdrawal_credentials		 BYTEA NOT NULL
);
`); err != nil {
		return errors.Wrap(err, "failed to create block bls to execution changes table")
	}

	if _, err := tx.Exec(ctx, `
CREATE UNIQUE INDEX IF NOT EXISTS i_validators_1 ON t_validators(f_index);
`); err != nil {
		return errors.Wrap(err, "failed to create validators index 1")
	}

	if _, err := tx.Exec(ctx, `
CREATE UNIQUE INDEX IF NOT EXISTS i_validators_2 ON t_validators(f_public_key);
`); err != nil {
		return errors.Wrap(err, "failed to create validators index 2")
	}

	if _, err := tx.Exec(ctx, `
CREATE INDEX IF NOT EXISTS i_validators_3 ON t_validators(f_withdrawal_credentials);
`); err != nil {
		return errors.Wrap(err, "failed to create validators index 3")
	}

	return nil
}
