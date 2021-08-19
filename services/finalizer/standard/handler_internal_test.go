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

package standard

import (
	"context"
	"os"
	"testing"

	autoeth2client "github.com/attestantio/go-eth2-client/auto"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	standardblocks "github.com/wealdtech/chaind/services/blocks/standard"
	postgresqlchaindb "github.com/wealdtech/chaind/services/chaindb/postgresql"
	standardchaintime "github.com/wealdtech/chaind/services/chaintime/standard"
)

func TestUpdateAttestationHeadCorrect(t *testing.T) {
	ctx := context.Background()

	chainDB, err := postgresqlchaindb.New(ctx,
		postgresqlchaindb.WithLogLevel(zerolog.Disabled),
		postgresqlchaindb.WithConnectionURL(os.Getenv("CHAINDB_DATABASE_URL")),
	)
	require.NoError(t, err)

	chainTime, err := standardchaintime.New(ctx,
		standardchaintime.WithGenesisTimeProvider(chainDB),
		standardchaintime.WithSpecProvider(chainDB),
		standardchaintime.WithForkScheduleProvider(chainDB),
	)
	require.NoError(t, err)

	eth2Client, err := autoeth2client.New(ctx,
		autoeth2client.WithAddress(os.Getenv("ETH2CLIENT_ADDRESS")),
	)
	require.NoError(t, err)

	blocks, err := standardblocks.New(ctx,
		standardblocks.WithChainDB(chainDB),
		standardblocks.WithChainTime(chainTime),
		standardblocks.WithETH2Client(eth2Client),
	)
	require.NoError(t, err)

	s, err := New(ctx,
		WithChainDB(chainDB),
		WithChainTime(chainTime),
		WithETH2Client(eth2Client),
		WithBlocks(blocks),
		WithLogLevel(zerolog.TraceLevel),
	)
	require.NoError(t, err)

	tests := []struct {
		name        string
		slot        phase0.Slot
		validator   phase0.ValidatorIndex
		headCorrect bool
	}{
		{
			name:        "329828-7",
			slot:        329828,
			validator:   7,
			headCorrect: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			attestations, err := chainDB.AttestationsForSlotRange(ctx, test.slot, test.slot+1)
			require.NoError(t, err)

			for _, attestation := range attestations {
				for _, validatorIndex := range attestation.AggregationIndices {
					if validatorIndex == test.validator {
						err = s.updateAttestationHeadCorrect(ctx, attestation, make(map[phase0.Slot]phase0.Root))
						require.NoError(t, err)
						require.NotNil(t, attestation.HeadCorrect)
						require.Equal(t, test.headCorrect, *attestation.HeadCorrect)
					}
				}
			}
		})
	}
}
