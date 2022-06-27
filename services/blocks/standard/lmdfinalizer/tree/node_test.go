package tree_test

import (
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/stretchr/testify/assert"
	"github.com/wealdtech/chaind/services/blocks/standard/lmdfinalizer/mock"
	"github.com/wealdtech/chaind/services/blocks/standard/lmdfinalizer/tree"
	"testing"
)

func TestNewNode(t *testing.T) {
	mBlock, mAtts := mock.MockBlock(
		10,
		10,
		9,
		[]mock.MockAttestation{
			{
				Slot:       0,
				Root:       0,
				NumIndices: 10,
			},
		},
	)

	node := tree.NewNode(mBlock, mAtts)

	assert.Equal(t, node.CurrenVote(), tree.VoteWeight(0))
	assert.Equal(t, node.Slot(), phase0.Slot(10))
	assert.Equal(t, node.Root()[0], byte(10))
	assert.Len(t, node.Attestations(), 1)
	assert.Equal(t, node.Attestations()[0].Slot, phase0.Slot(0))
	assert.Equal(t, node.Attestations()[0].BeaconBlockRoot[0], byte(0))
	assert.Len(t, node.Attestations()[0].AggregationIndices, 10)
}

func TestNode_RemoveAttestations(t *testing.T) {
	mBlock, mAtts := mock.MockBlock(
		10,
		10,
		9,
		[]mock.MockAttestation{
			{
				Slot:       0,
				Root:       0,
				NumIndices: 10,
			},
		},
	)

	node := tree.NewNode(mBlock, mAtts)

	assert.Len(t, node.Attestations(), 1)

	node.RemoveAttestations()

	assert.Len(t, node.Attestations(), 0)
}

func TestNode_CountVote(t *testing.T) {
	mBlock, mAtts := mock.MockBlock(
		10,
		10,
		9,
		[]mock.MockAttestation{
			{
				Slot:       0,
				Root:       0,
				NumIndices: 10,
			},
		},
	)

	node := tree.NewNode(mBlock, mAtts)

	assert.Equal(t, node.CurrenVote(), tree.VoteWeight(0))

	node.CountVote(tree.VoteWeight(111))

	assert.Equal(t, node.CurrenVote(), tree.VoteWeight(111))

	node.CountVote(tree.VoteWeight(222))

	assert.Equal(t, node.CurrenVote(), tree.VoteWeight(333))
}
