0.7.6:
  - Fix error in the Blocks() provider

0.7.5:
  - Add provider for epoch summaries
  - Add withdrawals to epoch summaries table
  - Add withdrawals to validator day summaries table
  - Add excess data gas to block execution payload table

0.7.4:
  - Ensure that only canonical withdrawals are included in summaries

0.7.3:
  - API update only

0.7.2:
  - fix upgrade issue from significantly earlier version of database

0.7.1:
  - fix issue where capella fork was not detected
  - pick up capella epochs in finalizer
  - add validator index to BLS to execution changes (thanks to @samlaf)
  - add withdrawal credentials to validators (thanks to @samlaf)

0.7.0:
  - speed up sync by only updating changed validators
  - add open telemetry tracing
  - avoid fetching duplicate proposer duties
  - introduce daily validator summaries, and pruning of validator balances and validator epoch summaries
  - support Capella

0.6.17:
  - expose read-only transaction functions

0.6.16:
  - tidy up summarizer error messages on failures
  - use read-only transactions where applicable

0.6.15:
  - catch edge case where summarizer may not include all attestations first time around
  - warn if running against a newer version of the database schema

0.6.14:
  - avoid crash if no response is given to a 'eth_getBlockByHash' call
  - do not refetch beacon committees if we already have them

0.6.12:
  - periodically update the information obtained by the spec service

0.6.11:
  - add configurable database connection pool size
  - separate read-only and read-write transactions internally
  - share activity semaphore between blocks and finalizer modules
  - ensure only a single beacon committees update can be run at any time
  - optimize caching of beacon committees during block processing (thanks to @ariskk)
  - optimize fetching of validator balances when calculating epoch summaries (thanks to @henridf)
  - avoid crash when execution node does not return transaction for which it supplied a log entry

0.6.10
  - avoid crash with uninitialised metrics
  - avoid double fetching of blocks

0.6.8
  - add missing column

0.6.7
  - attempt to fetch chain configuration prior to genesis

0.6.6
  - initialize monitoring metrics on startup to avoid drop to 0 on restart

0.6.5
  - store validator epoch summaries individually if batch store fails

0.6.4
  - fix fetching of execution paylods with blocks

0.6.3
  - add f_timestamp to execution payload
  - update go version to 1.18

0.6.2
  - update to chain database libraries (no binary release)

0.6.1
  - fix issue in obtaining sync committees after restart

0.6.0
  - Bellatrix support

0.5.6
  - do not fetch validator data twice for the same epoch

0.5.5
  - fix bad index for sync aggregates

0.5.3
  - correct summary value for attestations in epoch
  - batch finality updates, reducing memory usage and increasing responsiveness
  - avoid issue of duplicate column when upgrading f_parent_distance

0.5.1
  - fix missed sync committee information if first block of period is missed

0.5.0
  - add f_parent_distance to t_block_summaries
  - fix crash on database disconnect
  - use bulk insert for validator balances and validator epoch summaries

0.4.2:
  - add index to t_deposits for validator public key

0.4.1:
  - provide a more nuanced method for connecting to the chain database other than simple URL

0.4.0:
  - Altair support

0.3.6:
  - on failure to gather data wait and retry rather than mark as missed

0.3.5:
  - wait until chain has started and node is synced before fetching data

0.3.4:
  - update summarizer metadata for each update
  - avoid crash in situation where transaction receipt is no longer available
  - add HTTP request headers to avoid issue with latest version of geth
  - update block metadata when attempt to obtain block is unsuccessful

0.3.3:
  - avoid crash in situation where beacon committee information is not provided when expected
  - update go-eth2-client dependency

0.3.2:
  - standardise HTTP connections; avoid leaving connections open when no longer needed

0.3.1:
  - fix issue with creation of database schema on clean installation

0.3.0:
  - add summarizer module, providing summarized information for validators, blocks and epochs
  - add t_epoch_summaries, t_block_summaries and t_validator_epoch_summaries tables
  - add Ethereum 1 deposits module, queries an Ethereum 1 instance for deposits
  - add section on querying the database, and notes on the individual tables
  - fix potential crash in blocks handler if database goes away
  - add prometheus metrics; details in docs/prometheus.md

0.2.0:
  - automatically create initial tables if the database does not contain them
  - add f_canonical, f_target_correct and f_head_correct to t_attestations; NULL if not yet sure
  - add f_canonical to t_blocks to state if a block is canonical (true), non-canonical (false) or not yet sure (NULL)
  - add provider functions to fetch aggregate validator balances
  - add f_aggregation_indices to t_attestations to provide explicit indices for attestations
  - add t_eth1_deposits table to hold data about deposits on the Ethereum 1 chain (N.B. not currently populated)
  - add f_block_1_root and f_block_2_root to t_proposer_slashings table
  - make log level configuration hierarchical; if a log level is not present it will use the next one up the hierarchy
  - add t_chain_spec table, holding the chain specification
  - add t_genesis table, holding the chain genesis information
  - add t_deposits table, holding Ethereum 1 deposits recognized on the beacon chain and provided in the beacon block
  - t_validators table no longer uses '-1' as an indicator for the Ethereum spec value `FAR_FUTURE_EPOCH`, instead it uses NULL
  - add upgrader to update the database schema when changes are made
