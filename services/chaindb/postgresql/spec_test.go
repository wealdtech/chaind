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
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/chaind/services/chaindb"
	"github.com/wealdtech/chaind/services/chaindb/postgresql"
)

func TestSlotDuration(t *testing.T) {
	ctx := context.Background()

	var s chaindb.Service
	var err error
	s, err = postgresql.New(ctx,
		postgresql.WithLogLevel(zerolog.Disabled),
		postgresql.WithConnectionURL(os.Getenv("CHAINDB_DATABASE_URL")),
	)
	require.NoError(t, err)

	// Ensure this meets the eth2client interface requirement.
	_, isProvider := s.(eth2client.SlotDurationProvider)
	require.True(t, isProvider)

	// Ensure the value.
	slotDuration, err := s.(eth2client.SlotDurationProvider).SlotDuration(ctx)
	require.NoError(t, err)
	// This is hard-coded to the common value; may fail on non-standard chains.
	require.Equal(t, 12*time.Second, slotDuration)
}

func TestSlotsPerEpoch(t *testing.T) {
	ctx := context.Background()

	var s chaindb.Service
	var err error
	s, err = postgresql.New(ctx,
		postgresql.WithLogLevel(zerolog.Disabled),
		postgresql.WithConnectionURL(os.Getenv("CHAINDB_DATABASE_URL")),
	)
	require.NoError(t, err)

	// Ensure this meets the eth2client interface requirement.
	_, isProvider := s.(eth2client.SlotsPerEpochProvider)
	require.True(t, isProvider)

	// Ensure the value.
	slotsPerEpoch, err := s.(eth2client.SlotsPerEpochProvider).SlotsPerEpoch(ctx)
	require.NoError(t, err)
	// This is hard-coded to the common value; may fail on non-standard chains.
	require.Equal(t, uint64(32), slotsPerEpoch)
}
