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

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/chaind/services/chaindb/postgresql"
)

func TestSetChainSpecValue(t *testing.T) {
	ctx := context.Background()
	s, err := postgresql.New(ctx,
		postgresql.WithLogLevel(zerolog.Disabled),
		postgresql.WithConnectionURL(os.Getenv("CHAINDB_DATABASE_URL")),
	)
	require.NoError(t, err)

	// Try to set outside of a transaction; should fail.
	require.EqualError(t, s.SetChainSpecValue(ctx, "TEST_VALUE", "1"), postgresql.ErrNoTransaction.Error())

	ctx, cancel, err := s.BeginTx(ctx)
	require.NoError(t, err)
	defer cancel()

	// Set a value.
	require.NoError(t, s.SetChainSpecValue(ctx, "SECONDS_PER_MINUTE", 59*time.Second))

	// Attempt to set the same deposit again; should succeed.
	require.NoError(t, s.SetChainSpecValue(ctx, "SECONDS_PER_MINUTE", 60*time.Second))
}

func TestChainSpec(t *testing.T) {
	ctx := context.Background()
	s, err := postgresql.New(ctx,
		postgresql.WithLogLevel(zerolog.Disabled),
		postgresql.WithConnectionURL(os.Getenv("CHAINDB_DATABASE_URL")),
	)
	require.NoError(t, err)

	ctx, cancel, err := s.BeginTx(ctx)
	require.NoError(t, err)
	defer cancel()

	tests := []struct {
		name string
		key  string
		val  interface{}
	}{
		{
			name: "Duration",
			key:  "SECONDS_PER_MINUTE",
			val:  60 * time.Second,
		},
		{
			name: "Integer",
			key:  "IntegerVal",
			val:  uint64(12345),
		},
		{
			name: "String",
			key:  "NAME",
			val:  "mainnet",
		},
		{
			name: "Domain",
			key:  "DOMAIN_VOLUNTARY_EXIT",
			val:  phase0.DomainType{0x01, 0x02, 0x03, 0x04},
		},
		{
			name: "ForkVersion",
			key:  "GENESIS_FORK_VERSION",
			val:  phase0.Version{0x01, 0x02, 0x03, 0x04},
		},
		{
			name: "Hex",
			key:  "GENERIC_HEX_STRING",
			val:  []byte{0x01, 0x02, 0x03, 0x04},
		},
		{
			name: "Time",
			key:  "GENESIS_TIME",
			val:  time.Unix(1600000000, 0),
		},
	}

	// Set the values.
	for _, test := range tests {
		require.NoError(t, s.SetChainSpecValue(ctx, test.key, test.val))
	}

	// Fetch the values
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			val, err := s.ChainSpecValue(ctx, test.key)
			require.NoError(t, err)
			require.Equal(t, test.val, val)
		})
	}
}
