// Copyright © 2026 Weald Technology Trading.
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
	"math"
	"os"
	"testing"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/chaind/services/chaindb"
	"github.com/wealdtech/chaind/services/chaindb/postgresql"
)

func TestExecutionPayloadBid(t *testing.T) {
	ctx := context.Background()
	s, err := postgresql.New(ctx,
		postgresql.WithLogLevel(zerolog.Disabled),
		postgresql.WithConnectionURL(os.Getenv("CHAINDB_URL")),
	)
	require.NoError(t, err)

	bid := &chaindb.ExecutionPayloadBid{
		ParentBlockHash:  [32]byte{0x01},
		BlockHash:        [32]byte{0x02},
		FeeRecipient:     [20]byte{0x03},
		GasLimit:         60000000,
		BuilderIndex:     math.MaxUint64, // Self-build.
		Value:            5,
		ExecutionPayment: 6,
	}
	block := &chaindb.Block{
		Slot:                1,
		Root:                phase0.Root{0xb1, 0xd0},
		Graffiti:            make([]byte, 32),
		ETH1BlockHash:       make([]byte, 32),
		ExecutionPayloadBid: bid,
	}

	ctx, cancel, err := s.BeginTx(ctx)
	require.NoError(t, err)
	defer cancel()

	require.NoError(t, s.SetBlock(ctx, block))

	res, err := s.BlockByRoot(ctx, block.Root)
	require.NoError(t, err)
	require.Equal(t, bid, res.ExecutionPayloadBid)

	blocks, err := s.Blocks(ctx, &chaindb.BlockFilter{From: &block.Slot, To: &block.Slot})
	require.NoError(t, err)
	require.Len(t, blocks, 1)
	require.Equal(t, bid, blocks[0].ExecutionPayloadBid)
}
