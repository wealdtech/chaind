# Prometheus metrics
chaind provides a number of metrics to check the health and performance of its activities.  chaind's default implementation uses Prometheus to provide these metrics.  The metrics server listens on the address provided by the `metrics.prometheus.listen-address` configuration value.

## Version
The version of chaind can be found in the `chaind_release` metric, in the `version` label.

## Health
Health metrics provide a mechanism to confirm if chaind is active.

`chaind_start_time_secs` is the Unix timestamp at which chaind was started.  This value will remain the same throughout a run of chaind; if it increments it implies that chaind has restarted.

`chaind_ready` is `1` if chaind's services are all on-line and it is able to operate.  If not, this will be `0`.

## Operations
Operations metrics provide information about numbers of operations performed.  These are generally lower-level information that can be useful to monitor activities for fine-tuning of server parameters, comparing one instance to another, _etc._

  - `chaind_beaconcommittees_epochs_processed` number of epochs processed by the beacon committees module this run of chaind
  - `chaind_beaconcommittees_latest_epoch` latest epoch processed by the beacon committees module this run of chaind
  - `chaind_blocks_blocks_processed` number of blocks processed by the blocks module this run of chaind
  - `chaind_blocks_latest_block` latest block processed by the blocks module this run of chaind
  - `chaind_eth1deposits_blocks_processed` number of blocks processed by the Ethereum 1 deposits module this run of chaind
  - `chaind_eth1deposits_latest_block` latest block processed by the Ethereum 1 deposits module this run of chaind
  - `chaind_finalizer_epochs_processed` number of epochs processed by the finalizer module this run of chaind
  - `chaind_finalizer_latest_epoch` latest epoch processed by the finalizer module this run of chaind
  - `chaind_proposerduties_epochs_processed` number of epochs processed by the proposer duties module this run of chaind
  - `chaind_proposerduties_latest_epoch` latest epoch processed by the proposer duties module this run of chaind
  - `chaind_summarizer_epochs_processed_total` number of epochs processed by the summarizer module this run of chaind
  - `chaind_summarizer_latest_epoch` latest epoch processed by the summarizer module this run of chaind
  - `chaind_summarizer_days_processed_total` number of days processed by the daily-rollup submodule of the summarizer module this run of chaind
  - `chaind_summarizer_latest_day` latest day (Unix timestamp of midnight UTC) processed by the daily-rollup submodule of the summarizer module this run of chaind
  - `chaind_summarizer_balance_prune_ts` Unix timestamp of the last validator-balance prune run by the summarizer module
  - `chaind_summarizer_epoch_prune_ts` Unix timestamp of the last epoch-summary prune run by the summarizer module
  - `chaind_summarizer_lag_epochs{pipeline="epoch|block|validator"}` number of epochs by which each summarizer pipeline lags its direct upstream cursor: `pipeline=epoch` is the gap between `validators.latest_balances_epoch` and the summarizer's epoch cursor (i.e. how far the upstream balances pipeline is ahead of summarization); `pipeline=block` and `pipeline=validator` are the gaps between the summarizer's epoch cursor and its own block / validator sub-pipeline cursors. Values are clamped at zero — disabled pipelines (e.g. `summarizer.blocks.enable=false`) produce no series for that label
  - `chaind_validators_epochs_processed` number of epochs processed by the validators module this run of chaind
  - `chaind_validators_latest_epoch` latest epoch processed by the validators module this run of chaind
  - `chaind_validators_balances_epochs_processed` number of epochs processed by the balances submodule of the validators module this run of chaind
  - `chaind_validators_balances_latest_epoch` latest epoch processed by the balances submodule of the validators module this run of chaind
