// Copyright © 2021 Weald Technology Limited.
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

package standard_test

import (
	"context"
	"os"
	"testing"

	autoeth2client "github.com/attestantio/go-eth2-client/auto"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	mockblocks "github.com/wealdtech/chaind/services/blocks/mock"
	postgresqlchaindb "github.com/wealdtech/chaind/services/chaindb/postgresql"
	standardchaintime "github.com/wealdtech/chaind/services/chaintime/standard"
	"github.com/wealdtech/chaind/services/finalizer/standard"
)

func TestService(t *testing.T) {
	ctx := context.Background()

	chainDB, err := postgresqlchaindb.New(ctx,
		postgresqlchaindb.WithLogLevel(zerolog.Disabled),
		postgresqlchaindb.WithConnectionURL(os.Getenv("CHAINDB_DATABASE_URL")),
	)
	require.NoError(t, err)

	chainTime, err := standardchaintime.New(ctx,
		standardchaintime.WithGenesisTimeProvider(chainDB),
		standardchaintime.WithSlotDurationProvider(chainDB),
		standardchaintime.WithSlotsPerEpochProvider(chainDB),
	)
	require.NoError(t, err)

	blocks := mockblocks.New()

	eth2Client, err := autoeth2client.New(ctx,
		autoeth2client.WithAddress(os.Getenv("ETH2CLIENT_ADDRESS")),
	)
	require.NoError(t, err)

	tests := []struct {
		name   string
		params []standard.Parameter
		err    string
	}{
		{
			name: "ChainDBMissing",
			params: []standard.Parameter{
				standard.WithLogLevel(zerolog.Disabled),
				standard.WithChainTime(chainTime),
				standard.WithETH2Client(eth2Client),
				standard.WithBlocks(blocks),
			},
			err: "problem with parameters: no chain database specified",
		},
		{
			name: "ChainTimeMissing",
			params: []standard.Parameter{
				standard.WithLogLevel(zerolog.Disabled),
				standard.WithChainDB(chainDB),
				standard.WithETH2Client(eth2Client),
				standard.WithBlocks(blocks),
			},
			err: "problem with parameters: no chain time specified",
		},
		{
			name: "ETH2ClientMissing",
			params: []standard.Parameter{
				standard.WithLogLevel(zerolog.Disabled),
				standard.WithChainDB(chainDB),
				standard.WithChainTime(chainTime),
				standard.WithBlocks(blocks),
			},
			err: "problem with parameters: no Ethereum 2 client specified",
		},
		{
			name: "BlocksMissing",
			params: []standard.Parameter{
				standard.WithLogLevel(zerolog.Disabled),
				standard.WithChainDB(chainDB),
				standard.WithChainTime(chainTime),
				standard.WithETH2Client(eth2Client),
			},
			err: "problem with parameters: no blocks specified",
		},
		{
			name: "Good",
			params: []standard.Parameter{
				standard.WithLogLevel(zerolog.Disabled),
				standard.WithChainDB(chainDB),
				standard.WithChainTime(chainTime),
				standard.WithETH2Client(eth2Client),
				standard.WithBlocks(blocks),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := standard.New(context.Background(), test.params...)
			if test.err != "" {
				require.EqualError(t, err, test.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
