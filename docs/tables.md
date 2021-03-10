# Notes on database tables

# t_attestations

This table has both `f_aggregation_bits` and `f_aggregation_indices` fields.  The former is part of the official attestation data structure, whereas the latter is a decoded validator index for ease of querying.

The `f_canonical` field takes one of three values: _true_ if the block in which the attestation is included is canonical, _false_ if the block in which the attestation is included is not canonical, or _null_ if its canonical state has yet to be decided (usually because the chain has not reached finality for the block in which the attestation was included).

The `f_target_correct` and `f_head_correct` fields will be _null_ if the `f_canonical` is _null_.

# t_block_summaries

This is a summary table to help with aggregate statistics.  The specific fields here are:
 - f_slot the slot for which the row holds statistics
 - f_attestations_for_block the number of attestations for this block that were included in canonical blocks
 - f_duplicate_attestations_for_block the number of exact duplicate attestations for this block that were included in canonical blocks
 - f_votes_for_block the number of validators that attested to this block

# t_blocks

The `f_canonical` field takes one of three values: _true_ if the block is canonical, _false_ if the block is not canonical, or _null_ if its canonical state has yet to be decided (usually because the chain has not reached finality for that block).

# t_chain_spec

This table contains the specification data of the Ethereum 2 beacon chain for which data is obtained.  This, along with the genesis information, allows epoch and slot values to be converted into timestamps without additional external information.

# t_deposits

This table contains deposits that are included in Ethereum 2 blocks.

# t_epoch_summaries

This is a summary table to help with aggregate statistics.  The specific fields here are:
 - f_epoch the epoch for which the row holds statistics
 - f_activation_queue_length the number of validators awaiting activation
 - f_activating_validators the number of validators that entered the active state on this epoch
 - f_active_validators the number of validators in the active state on this epoch
 - f_active_real_balance the total balance of active validators
 - f_active_balance the total effective balance of active validators
 - f_attesting_validators the number of validators that made an attestation for this epoch that was recorded in a canonical block
 - f_attesting_balance the total effective balance of validators that made an attestation for this epoch that was recorded in a canonical block
 - f_target_correct_validators the number of validators with canonical attestations that voted for the correct target
 - f_target_correct_balance the total effective balance of validators with canonical attestations that voted for the correct target
 - f_head_correct_validators the number of validators with canonical attestations that voted for the correct head
 - f_head_correct_balance the total effective balance of validators with canonical attestations that voted for the correct head
 - f_attestations_for_epoch the number of attestations for this epoch that were included in canonical blocks
 - f_attestations_in_epoch the number of attestations that were included in canonical blocks within this epoch
 - f_duplicate_attestations_for_epoch the number of exact duplicate attestations for this epoch that were included in canonical blocks
 - f_proposer_slashings the number of validators that were slashed in this epoch due to proposer violations
 - f_attester_slashings the number of validators that were slashed in this epoch due to attester violations
 - f_deposits the number of deposits that were registered in this epoch
 - f_exiting_validators the number of validators that entered the exited state on this epoch
 - f_canonical_blocks the number of canonical blocks in this epoch

# t_eth1_deposits

This table contains deposits that are included in Ethereum 1 blocks.

It is possible for `f_eth1_recipient` to be something other than the deposit contract.  In this situation the recipient will be a smart contract that sent the actual deposit transaction.

# t_genesis

This table contains the genesis data of the Ethereum 2 beacon chain for which data is obtained.  This, along with the chain spec information, allows epoch and slot values to be converted into timestamps without additional external information.

# t_metadata

This table is used by chaind itself for keeping track of what it has and has not processed, and is not part of the blockchain data.

# t_proposer_slashings

This table contains the fields `f_block_1_root` and `f_block_2_root` which are not in the proposer slashings themselves but are derived from that data.

# t_validator_balances

This table contains the balance of the validator at the _start_ of the given epoch.

# t_validator_epoch_summaries

This is a summary table to help with aggregate statistics.  The specific fields here are:
 - f_validator_index the index of the validator for whih the row holds statistics
 - f_epoch the epoch for which the row holds statistics
 - f_proposer_duties the number of proposer duties this validator had in this epoch
 - f_proposals_included the number of block proposals included in the canonical chain
 - f_attestation_included true if the validator's attestation for this epoch was included in a canonical block
 - f_attestation_target_correct true if the validator attested correctly to the target
 - f_attestation_head_correct true if the validator attested correctly to the head
 - f_attestation_inclusion_delay number of blocks between the block to which the validator attested and the block in which the attestation was included

# t_validators

The values `f_activation_eligibility_epoch`, `f_activation_epoch`, `f_exit_epoch`, and `f_withdrawable_epoch` use _null_ instead of the spec `FAR_FUTURE_EPOCH` value.
