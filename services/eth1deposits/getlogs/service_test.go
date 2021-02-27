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

package getlogs_test

import (
	"context"
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	postgresqlchaindb "github.com/wealdtech/chaind/services/chaindb/postgresql"
	"github.com/wealdtech/chaind/services/eth1deposits/getlogs"
)

func TestService(t *testing.T) {
	ctx := context.Background()

	chainDB, err := postgresqlchaindb.New(ctx,
		postgresqlchaindb.WithLogLevel(zerolog.Disabled),
		postgresqlchaindb.WithConnectionURL(os.Getenv("CHAINDB_DATABASE_URL")),
	)
	require.NoError(t, err)

	tests := []struct {
		name   string
		params []getlogs.Parameter
		err    string
	}{
		{
			name: "ChainDBMissing",
			params: []getlogs.Parameter{
				getlogs.WithLogLevel(zerolog.Disabled),
				getlogs.WithETH1DepositsSetter(chainDB),
				getlogs.WithConnectionURL(os.Getenv("ETH1CLIENT_ADDRESS")),
			},
			err: "problem with parameters: no chain database specified",
		},
		{
			name: "ConnectionURLMissing",
			params: []getlogs.Parameter{
				getlogs.WithLogLevel(zerolog.Disabled),
				getlogs.WithChainDB(chainDB),
				getlogs.WithETH1DepositsSetter(chainDB),
			},
			err: "problem with parameters: no connection URL specified",
		},
		{
			name: "Good",
			params: []getlogs.Parameter{
				getlogs.WithLogLevel(zerolog.Disabled),
				getlogs.WithChainDB(chainDB),
				getlogs.WithETH1DepositsSetter(chainDB),
				getlogs.WithConnectionURL("postgres://mock:mock@localhost/"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := getlogs.New(ctx, test.params...)
			if test.err != "" {
				require.EqualError(t, err, test.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
