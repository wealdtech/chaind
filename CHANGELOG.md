development
  - correct summary value for attestations in epoch
  - batch finality updates, reducing memory usage and increasing responsiveness

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
