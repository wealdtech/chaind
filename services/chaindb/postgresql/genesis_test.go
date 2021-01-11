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

package postgresql_test

import (
	"context"
	"os"
	"testing"
	"time"

	eth2client "github.com/attestantio/go-eth2-client"
	api "github.com/attestantio/go-eth2-client/api/v1"
	spec "github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/chaind/services/chaindb"
	"github.com/wealdtech/chaind/services/chaindb/postgresql"
)

func TestSetGenesis(t *testing.T) {
	ctx := context.Background()
	s, err := postgresql.New(ctx,
		postgresql.WithLogLevel(zerolog.Disabled),
		postgresql.WithConnectionURL(os.Getenv("CHAINDB_DATABASE_URL")),
	)
	require.NoError(t, err)

	genesis := &api.Genesis{
		GenesisTime: time.Unix(1600000000, 0),
		GenesisValidatorsRoot: spec.Root{
			0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
			0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
		},
		GenesisForkVersion: spec.Version{0x00, 0x01, 0x02, 0x03},
	}

	// Try to set outside of a transaction; should fail.
	require.EqualError(t, s.SetGenesis(ctx, genesis), postgresql.ErrNoTransaction.Error())

	ctx, cancel, err := s.BeginTx(ctx)
	require.NoError(t, err)
	defer cancel()

	// Set.
	require.NoError(t, s.SetGenesis(ctx, genesis))

	// Attempt to set again; should succeed.
	require.NoError(t, s.SetGenesis(ctx, genesis))
}

func TestGenesisTime(t *testing.T) {
	ctx := context.Background()

	var s chaindb.Service
	var err error
	s, err = postgresql.New(ctx,
		postgresql.WithLogLevel(zerolog.Disabled),
		postgresql.WithConnectionURL(os.Getenv("CHAINDB_DATABASE_URL")),
	)
	require.NoError(t, err)

	// Ensure this meets the eth2client interface requirement.
	_, isProvider := s.(eth2client.GenesisTimeProvider)
	require.True(t, isProvider)

	// Ensure the value
	genesisTime, err := s.(eth2client.GenesisTimeProvider).GenesisTime(ctx)
	require.NoError(t, err)
	require.NotNil(t, genesisTime)
}
