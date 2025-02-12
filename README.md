# chaind

[![Tag](https://img.shields.io/github/tag/wealdtech/chaind.svg)](https://github.com/wealdtech/chaind/releases/)
[![License](https://img.shields.io/github/license/wealdtech/chaind.svg)](LICENSE)
[![GoDoc](https://godoc.org/github.com/wealdtech/chaind?status.svg)](https://godoc.org/github.com/wealdtech/chaind)
![Lint](https://github.com/wealdtech/chaind/workflows/golangci-lint/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/wealdtech/chaind)](https://goreportcard.com/report/github.com/wealdtech/chaind)

`chaind` is a process that reads information from an Ethereum 2 beacon node and stores it in a database for reporting and analysis.

## Table of Contents

- [Install](#install)
  - [Binaries](#binaries)
  - [Docker](#docker)
  - [Source](#source)
- [Usage](#usage)
- [Maintainers](#maintainers)
- [Contribute](#contribute)
- [License](#license)


## Notes for 0.9.0 and Electra
Release 0.9.0 of `chaind` brings with it support for the Electra hard fork.  A significant change in Electra is that attestations now cover multiple committees.  As such, there is a new column in the `t_attestations` table called `f_committee_indices`.  The existing column, called `f_committee_index`, is deprecated and will no longer be populated from this release onwards.

Usually in these situations `chaind` would carry out an automatic migration of data, however mainnet has over 1 billion attestations and so any migration will take a large amount of time and storage.  To avoid the upgrade locking out `chaind` updates there is _no_ automatic movement of this data, however there are a number of options for existing users:

* leave the data as-is.  If this data is required then code to query the database will need to fetch data from either the `f_committee_index` or `f_committee_indices` columns, depending on the slot time at which `chaind` was updated to release 0.9.0.
* move the data manually.  Data can be moved from one column to the other with the following SQL:
```sql
UPDATE t_attestations
SET f_committee_indices = ARRAY[f_committee_index]
   ,f_committee_index = NULL
WHERE f_committee_indices IS NULL
AND <slot conditions>
```
Where `<slot_conditions>` are conditions to restrict the attestations to a subset of all slots.  This restriction is important, as otherwise the update will take a very long time and require significant disk space.  It would be possible to migrate the data over time by migrating a smaller number (e.g. 100000) slots at a time.

Note that although `f_committee_index` is now deprecated it is not removed by the update, due to the data not being automatically migrated.  If a manual migration is completed then the column can be safely dropped, as it is no longer referenced within the `chaind` codebase.

## Install

### Binaries

Binaries for the latest version of `chaind` can be obtained from [the releases page](https://github.com/wealdtech/chaind/releases/latest).

### Docker

You can obtain the latest version of `chaind` using docker with:

```
docker pull wealdtech/chaind
```

### Source

`chaind` is a standard Go binary which can be installed with:

```sh
go install github.com/wealdtech/chaind@latest
```

## Usage
Data gathers four pieces of information from the beacon node, broken down by the modules that obtain the information:

  - **Proposer duties** The proposer duties module provides information on the validator expected to propose a beacon block at a given slot;
  - **Beacon committees** The beacon committees module provides information on the validators expected to attest to a beacon block at a given slot;
  - **Validators** The validators module provides information on the current statue of validators.  It can also obtain information on the validators' balances and effective balances at a given epoch;
  - **Blocks** The blocks module provides information on blocks proposed for each slot.  This includes:
    - the block structure
    - attestations
    - proposer slashings
    - attester slashings
    - deposits
    - voluntary exits
    - BLS to execution change requests
    - blobs
    - deposit requests
    - withdrawal requests
    - consolidation requests
  - **Ethereum execution deposits** The Ethereum execution deposits module provides information on deposits made on the Ethereum execution network;
  - **Finalizer** The finalizer module augments the information present in the database from finalized states.  This includes:
    - the canonical state of blocks.

In addition, the summarizer module takes the finalized information and generates summary statistics at the validator, block and epoch level.

## Requirements to run `chaind`
### Database
At current the only supported backend is PostgreSQL.  Once you have a  PostgreSQL instance you will need to create a user and database that `chaind` can use, for example run the following commands as the PostgreSQL superuser (`postgres` on most linux installations):

```sh
# This command creates a user named 'chain' and will prompt for a password.
createuser chain -P
# This command creates a database named 'chain' owned by the 'chain' user.
createdb -E UTF8 --owner=chain chain
```

You can also run chaind using the example docker-compose file, it setups the Postgres database automatically a container.

### Beacon node
`chaind` supports Teku and Lighthouse beacon nodes.  The current state of obtaining data from beacon nodes is as follows:

  - Teku: must be run in [archive mode](https://docs.teku.consensys.net/en/latest/Reference/CLI/CLI-Syntax/#data-storage-mode) to allow `chaind` to obtain historical data
  - Lighthouse: Make sure to run with `--reconstruct-historic-states --genesis-backfill --disable-backfill-rate-limiting`,otherwise fetching historical information will be **very** slow. For more information please refer to the [Advanced Database Configuration](https://lighthouse-book.sigmaprime.io/advanced_database.html) section in the Lighthouse Book.

`chaind` supports all execution nodes.  The current state of obtaining data from execution nodes is as follows:

  - geth: must run with `--txlookuplimit 0` to ensure that all deposit transactions can be obtained

### Example
To start a Teku node suitable for `chaind` download Teku and run the following command:

```sh
teku --rest-api-enabled --data-storage-mode=archive
```

Once Teku has finished syncing, run:

```sh
chaind --eth2client-address=http://localhost:5051/
```

Or if you prefer using docker-compose:
```sh
CHAIND_ETH2CLIENT_ADDRESS=http://localhost:5051/ docker-compose up -d
```
You can modify the configuration in chaind.config.docker-compose.yml file. The postgres server is exposed at 127.0.0.1:5432.

### Managing the database size
Two tables take up the majority of the database size.  These are:

- `t_validator_balances` the balance and effective balance of each validator at each epoch
- `t_validator_epoch_summaries` the expected and actual actions undertaken by each validator each epoch

These tables, along with their indices, take over 90% of the space required for the database on mainnet and goerli.  The `t_validator_day_summaries` table contains the same information but aggregated per day (a day being 00:00 to 00:00 UTC).  If the information in this table is sufficient for your needs you can prune old data from the larger tables, keeping their size down and hence managing the overall space requirement for `chaind`.

Pruning data removes data older than a certain age from the two database tables mentioned above.  Pruning is set for each table individually and so it is possible to prune either or both of the tables, and to have different retentions for each.  For example, the following configuration:

It is also possible to retain data for specific validator pubkeys.
```yaml
summarizer:
  validators:
    balance-retention: "P6M"
    epoch-retention: "P1Y"
    retain:
      - 0xab0bdda0f85f842f431beaccf1250bf1fd7ba51b4100fd64364b6401fda85bb0069b3e715b58819684e7fc0b10a72a34
      - 0x876dd4705157eb66dc71bc2e07fb151ea53e1a62a0bb980a7ce72d15f58944a8a3752d754f52f4a60dbfc7b18169f268
      - 0x9314c6de0386635e2799af798884c2ea09c63b9f079e572acc00b06a7faccce501ea4dfc0b1a23b8603680a5e3481327
```

This will store 6 month's worth of balances, and 1 year's worth of epoch summaries.  Retention periods are [ISO 8601 durations](https://en.wikipedia.org/wiki/ISO_8601#Durations).  Note that if it is not desired to retain any balance or epoch summary data then the retention can be set to "PT0s".

## Upgrading `chaind`
`chaind` should upgrade automatically from earlier versions.  Note that the upgrade process can take a long time to complete, especially where data needs to be refetched or recalculated.  `chaind` should be left to complete the upgrade, to avoid the situation where additional fields are not fully populated.  If chaind is ever stopped or crashes while upgrading and this situation does happen, one should rerun `chaind` with the options `--blocks.start-slot=0 --blocks.refetch=true` to force `chaind` to refetch all blocks.

## Querying `chaind`
`chaind` attempts to lay its data out in a standard fashion for a SQL database, mirroring the data structures that are present in Ethereum 2.  There are some places where the structure or data deviates from the specification, commonly to provide additional information or to make the data easier to query with SQL.  It is recommended that the [notes on the tables](docs/tables.md) are read before attempting to write any complicated queries.

## Configuring `chaind`
The minimal requirements for `chaind` are references to the database and beacon node, for example:

```sh
chaind --chaindb.url=postgres://chain:secret@localhost:5432 --eth2client.address=localhost:5051
```

Here, `chaindb.url` is the URL of a local PostgreSQL database with password 'secret' and 'eth2client.address' is the address of a supported beacon client node (gRPC for Prysm, HTTP for Teku and Lighthouse).

`chaind` allows additional configuration for itself and its modules.  It takes configuration from the command line, environment variables or a configuration file, but for the purposes of explaining the configuration options the configuration file is used.  This should be in the home directory and called `.chaind.yml`.  Alternatively, the configuration file can be placed in a different directory and referenced by `--base-dir`, for example `--base-dir=/home/user/config/chaind`; in this case the file should be called `chaind.yml` (without the leading period).

```yaml
# log-level is the base log level of the process.
# 'info' should be a suitable log level, unless detailed information is
# required in which case 'debug' or 'trace' can be used.
log-level: info
# log-file specifies that log output should go to a file.  If this is not
# present log output will be to stderr.
log-file: /var/log/chaind.log
chaindb:
  # url is the URL of the PostgreSQL database.
  url: postgres://chain:secret@localhost:5432
  max-connections: 16
# eth2client contains configuration for the Ethereum 2 client.
eth2client:
  # log-level is the log level of the specific module.  If not present the base log
  # level will be used.
  log-level: debug
  # address is the address of the beacon node.
  address: localhost:5051
# eth1client contains configuration for the Ethereum 1 client.
eth1client:
  # address is the address of the Ethereum 1 node.
  address: localhost:8545
# blocks contains configuration for obtaining block-related information.
blocks:
  # enable states if this module will be operational.
  enable: true
  # address is a separate connection for this module.  If not present then
  # chaind will use the eth2client connection.
  address: localhost:5051
  # start-slot is the slot from which to start.  chaind should keep track of this itself,
  # however if you wish to start from a later slot this can be set.
  # start-slot: 2000
  # refetch will refetch block data from a beacon node even if it has already has a block
  # in its database.
  # refetch: false
# validators contains configuration for obtaining validator-related information.
validators:
  enable: true
  # balances contains configuration for obtaining validator balances.  This is
  # a separate configuration flag for two reasons.  First, it can take a long
  # time to retrieve this information.  Second, the information can be
  # derived from the data obtained by the other modules.
  balances:
    enable: false
# beacon-committees contains configuration for obtaining beacon committee-related
# information.
beacon-committees:
  enable: true
# proposer-duties contains configuration for obtaining proposer duty-related
# information.
proposer-duties:
  enable: true
# finalizer updates tables with information available for finalized states.
finalizer:
  enable: true
# eth1deposits contains information about transactions made to the deposit contract
# on the Ethereum 1 network.
eth1deposits:
  enable: false
  # start-block is the block from which to start fetching deposits.  chaind should
  # keep track of this itself, however if you wish to start from a different block this
  # can be set.
  # start-block: 500
summarizer:
  enable: true
  epochs:
    enable: true
  blocks:
    enable: true
  validators:
    enable: false
    # retention period for validator balances (ISO_8601 duration format)
    # balance-retention: "P6M"
    # retention period for validator epoch summaries (ISO_8601 duration format)
    # epoch-retention: "P1Y"
    # validator pubkeys for which to ignore above retention values (it won't prune validator balances and summaries)
    # retain:
    #   - 0xab0bdda0f85f842f431beaccf1250bf1fd7ba51b4100fd64364b6401fda85bb0069b3e715b58819684e7fc0b10a72a34
    #   - 0x876dd4705157eb66dc71bc2e07fb151ea53e1a62a0bb980a7ce72d15f58944a8a3752d754f52f4a60dbfc7b18169f268
    #   - 0x9314c6de0386635e2799af798884c2ea09c63b9f079e572acc00b06a7faccce501ea4dfc0b1a23b8603680a5e3481327
```

## Support

We gratefully acknowledge the Ethereum Foundation for supporting chaind through their grant FY21-0360, which allowed collection of Ethereum 1 deposits.

## Maintainers

Jim McDonald: [@mcdee](https://github.com/mcdee).

## Contribute

Contributions welcome. Please check out [the issues](https://github.com/wealdtech/chaind/issues).

## License

[Apache-2.0](LICENSE) © 2020 Weald Technology Trading.
