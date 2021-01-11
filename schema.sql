-- t_metadata stores data about chaind processing functions.
DROP TABLE IF EXISTS t_metadata;
CREATE TABLE t_metadata (
  f_key    TEXT NOT NULL PRIMARY KEY
 ,f_value JSONB NOT NULL
);
CREATE UNIQUE INDEX i_metadata_1 ON t_metadata(f_key);

-- t_validators contains all validators known by the chain.
-- This information is not stored per-epoch, as the latest values contain the
-- majority of information required to calculate all state about the validator.
DROP TABLE IF EXISTS t_validators CASCADE;
CREATE TABLE t_validators (
  f_public_key                   BYTEA NOT NULL
 ,f_index                        BIGINT NOT NULL
 ,f_slashed                      BOOLEAN NOT NULL
 ,f_activation_eligibility_epoch BIGINT
 ,f_activation_epoch             BIGINT
 ,f_exit_epoch                   BIGINT
 ,f_withdrawable_epoch           BIGINT
 ,f_effective_balance            BIGINT NOT NULL
);
CREATE UNIQUE INDEX i_validators_1 ON t_validators(f_index);
CREATE UNIQUE INDEX i_validators_2 ON t_validators(f_public_key);

-- t_blocks contains all blocks proposed by validators.
-- N.B. it is possible for multiple valid blocks to be proposed in a single slot
-- by different proposers in the case of a chain re-org.
DROP TABLE IF EXISTS t_blocks CASCADE;
CREATE TABLE t_blocks (
  f_slot               BIGINT NOT NULL
 ,f_proposer_index     BIGINT NOT NULL
 ,f_root               BYTEA NOT NULL
 ,f_graffiti           BYTEA NOT NULL
 ,f_randao_reveal      BYTEA NOT NULL
 ,f_body_root          BYTEA NOT NULL
 ,f_parent_root        BYTEA NOT NULL
 ,f_state_root         BYTEA NOT NULL
 ,f_eth1_block_hash    BYTEA NOT NULL
 ,f_eth1_deposit_count BIGINT NOT NULL
 ,f_eth1_deposit_root  BYTEA NOT NULL
);
CREATE UNIQUE INDEX i_blocks_1 ON t_blocks(f_slot,f_root);
CREATE UNIQUE INDEX i_blocks_2 ON t_blocks(f_root);
CREATE INDEX i_blocks_3 ON t_blocks(f_parent_root);

-- t_beacon_committees contains all beacon committees.
-- N.B. in the case of a chain re-org the committees can alter.
DROP TABLE IF EXISTS t_beacon_committees CASCADE;
CREATE TABLE t_beacon_committees (
  f_slot BIGINT NOT NULL
 ,f_index BIGINT NOT NULL
 ,f_committee BIGINT[] NOT NULL -- REFERENCES t_validators(f_index)
);
CREATE UNIQUE INDEX i_beacon_committees_1 ON t_beacon_committees(f_slot, f_index);

-- t_propser_duties contains all proposer duties.
-- N.B. in the case of a chain re-org the duties can alter.
DROP TABLE IF EXISTS t_proposer_duties CASCADE;
CREATE TABLE t_proposer_duties (
  f_slot BIGINT NOT NULL
 ,f_validator_index BIGINT NOT NULL -- REFERENCES t_validators(f_index)
);
CREATE UNIQUE INDEX i_proposer_duties_1 ON t_proposer_duties(f_slot);

-- t_attestations contains all attestations included in blocks.
DROP TABLE IF EXISTS t_attestations CASCADE;
CREATE TABLE t_attestations (
  f_inclusion_slot       BIGINT NOT NULL
 ,f_inclusion_block_root BYTEA NOT NULL REFERENCES t_blocks(f_root) ON DELETE CASCADE
 ,f_inclusion_index      BIGINT NOT NULL
 ,f_slot                 BIGINT NOT NULL
 ,f_committee_index      BIGINT NOT NULL
 ,f_aggregation_bits     BYTEA NOT NULL
 ,f_beacon_block_root    BYTEA NOT NULL -- we don't reference this because the block may not exist in the canonical chain
 ,f_source_epoch         BIGINT NOT NULL
 ,f_source_root          BYTEA NOT NULL
 ,f_target_epoch         BIGINT NOT NULL
 ,f_target_root          BYTEA NOT NULL
);
CREATE UNIQUE INDEX i_attestations_1 ON t_attestations(f_inclusion_slot,f_inclusion_block_root,f_inclusion_index);
CREATE INDEX i_attestations_2 ON t_attestations(f_slot);
CREATE INDEX i_attestations_3 ON t_attestations(f_beacon_block_root);

-- t_attester_slashings contains all attester slashings included in blocks.
DROP TABLE IF EXISTS t_attester_slashings CASCADE;
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
DROP TABLE IF EXISTS t_proposer_slashings CASCADE;
CREATE TABLE t_proposer_slashings (
  f_inclusion_slot          BIGINT NOT NULL
 ,f_inclusion_block_root    BYTEA NOT NULL REFERENCES t_blocks(f_root) ON DELETE CASCADE
 ,f_inclusion_index         BIGINT NOT NULL
 ,f_header_1_slot           BIGINT NOT NULL
 ,f_header_1_proposer_index BIGINT NOT NULL
 ,f_header_1_parent_root    BYTEA NOT NULL
 ,f_header_1_state_root     BYTEA NOT NULL
 ,f_header_1_body_root      BYTEA NOT NULL
 ,f_header_1_signature      BYTEA NOT NULL
 ,f_header_2_slot           BIGINT NOT NULL
 ,f_header_2_proposer_index BIGINT NOT NULL
 ,f_header_2_parent_root    BYTEA NOT NULL
 ,f_header_2_state_root     BYTEA NOT NULL
 ,f_header_2_body_root      BYTEA NOT NULL
 ,f_header_2_signature      BYTEA NOT NULL
);
CREATE UNIQUE INDEX i_proposer_slashings_1 ON t_proposer_slashings(f_inclusion_slot,f_inclusion_block_root,f_inclusion_index);

-- t_voluntary_exits contains all voluntary exits included in blocks.
DROP TABLE IF EXISTS t_voluntary_exits CASCADE;
CREATE TABLE t_voluntary_exits (
  f_inclusion_slot       BIGINT NOT NULL
 ,f_inclusion_block_root BYTEA NOT NULL REFERENCES t_blocks(f_root) ON DELETE CASCADE
 ,f_inclusion_index      BIGINT NOT NULL
 ,f_validator_index      BIGINT NOT NULL
 ,f_epoch                BIGINT NOT NULL
);
CREATE UNIQUE INDEX i_voluntary_exits_1 ON t_voluntary_exits(f_inclusion_slot,f_inclusion_block_root,f_inclusion_index);

-- t_deposits contains all deposits included in blocks.
DROP TABLE IF EXISTS t_deposits CASCADE;
CREATE TABLE t_deposits (
  f_inclusion_slot         BIGINT NOT NULL
 ,f_inclusion_block_root   BYTEA NOT NULL REFERENCES t_blocks(f_root) ON DELETE CASCADE
 ,f_inclusion_index        BIGINT NOT NULL
 ,f_validator_pubkey       BYTEA NOT NULL
 ,f_withdrawal_credentials BYTEA NOT NULL
 ,f_amount                 BIGINT NOT NULL
);
CREATE UNIQUE INDEX i_deposits_1 ON t_deposits(f_inclusion_slot,f_inclusion_block_root,f_inclusion_index);

-- t_validator_balances contains per-epoch balances.
DROP TABLE IF EXISTS t_validator_balances CASCADE;
CREATE TABLE t_validator_balances (
  f_validator_index   BIGINT NOT NULL REFERENCES t_validators(f_index) ON DELETE CASCADE
 ,f_epoch             BIGINT NOT NULL
 ,f_balance           BIGINT NOT NULL
 ,f_effective_balance BIGINT NOT NULL
);
CREATE UNIQUE INDEX i_validator_balances_1 ON t_validator_balances(f_validator_index, f_epoch);
CREATE INDEX i_validator_balances_2 ON t_validator_balances(f_epoch);

-- t_epochs contain rollup information for epochs.
DROP TABLE IF EXISTS t_epochs CASCADE;
CREATE TABLE t_epochs (
  f_epoch                    BIGINT NOT NULL
 ,f_active_validators        BIGINT NOT NULL
 ,f_active_effective_balance BIGINT NOT NULL
 ,f_justified_at             BIGINT
 ,f_finalized_at             BIGINT
);
CREATE UNIQUE INDEX i_epochs_1 ON t_epochs(f_epoch);
