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

## Install

### Binaries

Binaries for the latest version of `chaind` can be obtained from [the releases page](https://github.com/wealdtech/chaind/releases/latest  ).

### Docker

You can obtain the latest version of `chaind` using docker with:

```
docker pull wealdtech/chaind
```

### Source

`chaind` is a standard Go binary which can be installed with:

```sh
GO111MODULE=on go get github.com/wealdtech/chaind
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
    - voluntary exits; and
  - **Ethereum 1 deposits** The Ethereum 1 deposits module provides information on deposits made on the Ethereum 1 network;
  - **Finalizer** The finalizer module augments the information present in the database from finalized states.  This includes:
    - the canonical state of blocks.

In addition, the summarizer module takes the finalized information and generates summary statistics at the validator, block and epoch level.

## Requirements to run `chaind`
### Database
At current the only supported backend is PostgreSQL.  Once you have a  PostgreSQL instance you will need to create a user and database that `chaind` can use, for example run the following commands as the PostgreSQL superuser (`postgres` on most linux installations):

```
# This command creates a user named 'chain' and will prompt for a password.
createuser chain -P
# This command creates a database named 'chain' owned by the 'chain' user.
createdb -E UTF8 --owner=chain chain
```

### Beacon node
`chaind` supports Teku and Lighthouse beacon nodes.  The current state of obtaining data from beacon nodes is as follows:

  - Teku: must be run in [archive mode](https://docs.teku.consensys.net/en/latest/Reference/CLI/CLI-Syntax/#data-storage-mode) to allow `chaind` to obtain historical data
  - Lighthouse: Make sure to run with `--slots-per-restore-point 64`, else fetching historical information will be **very** slow. For more information on the trade off between Freezer DB size and fetching performance, please refer to [Database Configuration](https://lighthouse-book.sigmaprime.io/advanced_database.html) in the Lighthouse Book.

At current Prysm is not supported due to its lack of Altair-related information in its gRPC and HTTP APIs.  We expect to be able to support Prysm again soon.

### Example
To start a Teku node suitable for `chaind` download Teku and run the following command:

```sh
teku --rest-api-enabled --data-storage-mode=archive
```

Once Teku has finished syncing, run:

```sh
chaind --eth2client-address=http://localhost:5051/
```

## Upgrading `chaind`
`chaind` should upgrade automatically from earlier versions.  Note that the upgrade process can take a long time to complete, especially where data needs to be refetched or recalculated.  `chaind` should be left to complete the upgrade, to avoid the situation where additional fields are not fully populated.  If this does occur then `chaind` can be run with the options `--blocks.start-slot=0 --blocks.refetch=true` to force `chaind` to refetch all blocks.

## Querying `chaind`
`chaind` attempts to lay its data out in a standard fashion for a SQL database, mirroring the data structures that are present in Ethereum 2.  There are some places where the structure or data deviates from the specification, commonly to provide additional information or to make the data easier to query with SQL.  It is recommended that the [notes on the tables](docs/tables.md) are read before attempting to write any complicated queries.

## Configuring `chaind`
The minimal requirements for `chaind` are references to the database and beacon node, for example:

```
chaind --chaindb.url=postgres://chain:secret@localhost:5432 --eth2client.address=localhost:5051
```

Here, `chaindb.url` is the URL of a local PostgreSQL database with password 'secret' and 'eth2client.address' is the address of a supported beacon client node (gRPC for Prysm, HTTP for Teku and Lighthouse).

`chaind` allows additional configuration for itself and its modules.  It takes configuration from the command line, environment variables or a configuration file, but for the purposes of explaining the configuration options the configuration file is used.  This should be in the home directory and called `.chaind.yml`.  Alternatively, the configuration file can be placed in a different directory and referenced by `--base-dir`, for example `--base-dir=/home/user/config/chaind`; in this case the file should be called `chaind.yml` (without the leading period).

```
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
```

## Support

We gratefully acknowledge the Ethereum Foundation for supporting chaind through their grant FY21-0360, which allowed collection of Ethereum 1 deposits.

## Maintainers

Jim McDonald: [@mcdee](https://github.com/mcdee).

## Contribute

Contributions welcome. Please check out [the issues](https://github.com/wealdtech/chaind/issues).

## License

[Apache-2.0](LICENSE) © 2020 Weald Technology Trading.
