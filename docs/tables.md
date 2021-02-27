# Notes on database tables

# t_attestations

This table has both `f_aggregation_bits` and `f_aggregation_indices` fields.  The former is part of the official attestation data structure, whereas the latter is a decoded validator index for ease of querying.

The `f_canonical` field takes one of three values: _true_ if the block in which the attestation is included is canonical, _false_ if the block in which the attestation is included is not canonical, or _null_ if its canonical state has yet to be decided (usually because the chain has not reached finality for the block in which the attestation was included).

The `f_target_correct` and `f_head_correct` fields will be _null_ if the `f_canonical` is _null_.

# t_blocks

The `f_canonical` field takes one of three values: _true_ if the block is canonical, _false_ if the block is not canonical, or _null_ if its canonical state has yet to be decided (usually because the chain has not reached finality for that block).

# t_chain_spec

This table contains the specification data of the Ethereum 2 beacon chain for which data is obtained.  This, along with the genesis information, allows epoch and slot values to be converted into timestamps without additional external information.

# t_deposits

This table contains deposits that are included in Ethereum 2 blocks.

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

# t_validators

The values `f_activation_eligibility_epoch`, `f_activation_epoch`, `f_exit_epoch`, and `f_withdrawable_epoch` use _null_ instead of the spec `FAR_FUTURE_EPOCH` value.
