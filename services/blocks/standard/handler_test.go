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

package standard

import (
	"context"
	"testing"

	"github.com/OffchainLabs/go-bitfield"
	"github.com/attestantio/go-eth2-client/spec/altair"
	"github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/attestantio/go-eth2-client/spec/deneb"
	"github.com/attestantio/go-eth2-client/spec/electra"
	"github.com/attestantio/go-eth2-client/spec/gloas"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/chaind/services/chaindb"
)

func TestDBBlockGloas(t *testing.T) {
	block := &gloas.BeaconBlock{
		Slot: 100,
		Body: &gloas.BeaconBlockBody{
			ETH1Data: &phase0.ETH1Data{BlockHash: make([]byte, 32)},
			SyncAggregate: &altair.SyncAggregate{
				SyncCommitteeBits: bitfield.NewBitvector512(),
			},
			SignedExecutionPayloadBid: &gloas.SignedExecutionPayloadBid{
				Message: &gloas.ExecutionPayloadBid{
					BlockHash:          phase0.Hash32{0x02},
					FeeRecipient:       [20]byte{0x03},
					BuilderIndex:       4,
					Value:              5,
					ExecutionPayment:   6,
					BlobKZGCommitments: []deneb.KZGCommitment{{0x01}},
				},
			},
			ParentExecutionRequests: &gloas.ExecutionRequests{
				Deposits:       []*electra.DepositRequest{{WithdrawalCredentials: make([]byte, 32), Index: 7}},
				Withdrawals:    []*electra.WithdrawalRequest{{Amount: 5}},
				Consolidations: []*electra.ConsolidationRequest{{}},
			},
		},
	}

	dbBlock, err := (&Service{}).dbBlockGloas(context.Background(), block)
	require.NoError(t, err)
	require.Nil(t, dbBlock.ExecutionPayload)
	require.Equal(t, phase0.Slot(100), dbBlock.Slot)
	require.Len(t, dbBlock.BlobKZGCommitments, 1)
	require.Len(t, dbBlock.DepositRequests, 1)
	require.Equal(t, uint64(7), dbBlock.DepositRequests[0].Index)
	require.Equal(t, dbBlock.Root, dbBlock.DepositRequests[0].InclusionBlockRoot)
	require.Len(t, dbBlock.WithdrawalRequests, 1)
	require.Len(t, dbBlock.ConsolidationRequests, 1)
	require.Equal(t, &chaindb.ExecutionPayloadBid{
		BlockHash:        [32]byte{0x02},
		FeeRecipient:     [20]byte{0x03},
		BuilderIndex:     4,
		Value:            5,
		ExecutionPayment: 6,
	}, dbBlock.ExecutionPayloadBid)
}

func TestDBExecutionPayloadGloas(t *testing.T) {
	root := phase0.Root{0x01}
	payload := &gloas.ExecutionPayload{
		FeeRecipient:  [20]byte{0x02},
		BlockNumber:   123,
		BaseFeePerGas: uint256.NewInt(7),
		Withdrawals: []*capella.Withdrawal{
			{Index: 1, ValidatorIndex: 10, Address: [20]byte{0x03}, Amount: 32},
			// Builder payment.
			{Index: 2, ValidatorIndex: phase0.ValidatorIndex(4 | 1<<40), Address: [20]byte{0x02}, Amount: 5},
		},
	}

	res := dbExecutionPayloadGloas(root, 100, payload)
	require.Equal(t, uint64(123), res.BlockNumber)
	require.Equal(t, [20]byte{0x02}, res.FeeRecipient)
	require.Equal(t, int64(7), res.BaseFeePerGas.Int64())
	require.Len(t, res.Withdrawals, 2)
	require.Equal(t, &chaindb.Withdrawal{
		InclusionBlockRoot: root,
		InclusionSlot:      100,
		InclusionIndex:     1,
		Index:              2,
		ValidatorIndex:     phase0.ValidatorIndex(4 | 1<<40),
		Address:            [20]byte{0x02},
		Amount:             5,
	}, res.Withdrawals[1])
}

func TestParentPayloadIsFull(t *testing.T) {
	parent := &chaindb.Block{ExecutionPayloadBid: &chaindb.ExecutionPayloadBid{BlockHash: [32]byte{0x01}}}
	tests := []struct {
		name   string
		parent *chaindb.Block
		child  *chaindb.Block
		full   bool
	}{
		{
			name:   "PreGloasParent",
			parent: &chaindb.Block{},
			child:  &chaindb.Block{ExecutionPayloadBid: &chaindb.ExecutionPayloadBid{ParentBlockHash: [32]byte{0x01}}},
		},
		{
			name:   "Full",
			parent: parent,
			child:  &chaindb.Block{ExecutionPayloadBid: &chaindb.ExecutionPayloadBid{ParentBlockHash: [32]byte{0x01}}},
			full:   true,
		},
		{
			name:   "Empty",
			parent: parent,
			child:  &chaindb.Block{ExecutionPayloadBid: &chaindb.ExecutionPayloadBid{ParentBlockHash: [32]byte{0x09}}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.full, parentPayloadIsFull(test.parent, test.child))
		})
	}
}
