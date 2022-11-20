package tree

import (
	"github.com/stretchr/testify/assert"
	"github.com/wealdtech/chaind/services/blocks/standard/lmdfinalizer/mock"
	"testing"
)

var genesis = NewNode(mock.MockBlock(0, 0, 0, nil))

func TestTree_InsertOutOfOrderAndAdopt(t *testing.T) {
	tr := New(genesis)

	node1 := NewNode(mock.MockBlock(2, 1, 0, nil))
	node2 := NewNode(mock.MockBlock(2, 2, 0, nil))
	node3 := NewNode(mock.MockBlock(5, 3, 1, nil))
	node4 := NewNode(mock.MockBlock(10, 4, 7, nil))

	tr.Insert(node3)
	tr.Insert(node4)

	tr.Insert(node1)
	tr.Insert(node2)

	assert.Len(t, tr.orphans, 2)
	assert.Equal(t, tr.orphans[node3.Root()], tr.GetByRoot(node3.Root()))
	assert.Equal(t, tr.orphans[node4.Root()], tr.GetByRoot(node4.Root()))

	assert.Equal(t, tr.GetByRoot(genesis.Root()), tr.GetByRoot(node1.Root()).parent)
	assert.Equal(t, tr.GetByRoot(genesis.Root()), tr.GetByRoot(node2.Root()).parent)
	assert.Equal(t, (*Node)(nil), tr.GetByRoot(node3.Root()).parent)
	assert.Equal(t, (*Node)(nil), tr.GetByRoot(node4.Root()).parent)

	tr.Adopt(tr.GetByRoot(node1.Root()), []*Node{tr.GetByRoot(node3.Root()), tr.GetByRoot(node4.Root())})

	assert.Equal(t, tr.GetByRoot(node1.Root()), tr.GetByRoot(node3.Root()).parent)
	assert.Equal(t, (*Node)(nil), tr.GetByRoot(node4.Root()).parent)

	assert.Len(t, tr.orphans, 1)
	assert.Equal(t, tr.orphans[node4.Root()], tr.GetByRoot(node4.Root()))
}

func TestTree_OnNewLatestFinalizedBlock(t *testing.T) {
	tr := New(genesis)

	node1 := NewNode(mock.MockBlock(2, 1, 0, nil))   // child of genesis
	node2 := NewNode(mock.MockBlock(2, 2, 0, nil))   // child of genesis
	node3 := NewNode(mock.MockBlock(100, 3, 1, nil)) // child of 1
	node4 := NewNode(mock.MockBlock(10, 4, 7, nil))  // child of none
	node5 := NewNode(mock.MockBlock(101, 5, 3, nil)) // child of 3
	node6 := NewNode(mock.MockBlock(104, 6, 8, nil)) // child of none

	tr.Insert(node1)
	tr.Insert(node2)
	tr.Insert(node3)
	tr.Insert(node4)
	tr.Insert(node5)
	tr.Insert(node6)

	assert.Equal(t, tr.latestFinalizedBlock, tr.GetByRoot(genesis.Root()))

	assert.Len(t, tr.rootIndex, 7)
	assert.Equal(t, tr.rootIndex[genesis.Root()], tr.GetByRoot(genesis.Root()))
	assert.Equal(t, tr.rootIndex[node1.Root()], tr.GetByRoot(node1.Root()))
	assert.Equal(t, tr.rootIndex[node2.Root()], tr.GetByRoot(node2.Root()))
	assert.Equal(t, tr.rootIndex[node3.Root()], tr.GetByRoot(node3.Root()))
	assert.Equal(t, tr.rootIndex[node4.Root()], tr.GetByRoot(node4.Root()))
	assert.Equal(t, tr.rootIndex[node5.Root()], tr.GetByRoot(node5.Root()))
	assert.Equal(t, tr.rootIndex[node6.Root()], tr.GetByRoot(node6.Root()))

	assert.Len(t, tr.orphans, 2)
	assert.Equal(t, tr.orphans[node4.Root()], tr.GetByRoot(node4.Root()))
	assert.Equal(t, tr.orphans[node6.Root()], tr.GetByRoot(node6.Root()))

	tr.OnNewLatestFinalizedBlock(tr.GetByRoot(node3.Root()))

	assert.Equal(t, tr.latestFinalizedBlock, tr.GetByRoot(node3.Root()))

	assert.Len(t, tr.rootIndex, 3)
	assert.Equal(t, tr.rootIndex[node3.Root()], tr.GetByRoot(node3.Root()))
	assert.Equal(t, tr.rootIndex[node5.Root()], tr.GetByRoot(node5.Root()))
	assert.Equal(t, tr.rootIndex[node6.Root()], tr.GetByRoot(node6.Root()))

	assert.Len(t, tr.orphans, 1)
	assert.Equal(t, tr.orphans[node6.Root()], tr.GetByRoot(node6.Root()))
}

func TestTree_InsertSameRoot(t *testing.T) {
	tr := New(genesis)

	node1 := NewNode(mock.MockBlock(2, 1, 0, nil))
	node2 := NewNode(mock.MockBlock(2, 1, 0, nil)) // has same root as 1
	node3 := NewNode(mock.MockBlock(100, 3, 1, nil))
	node4 := NewNode(mock.MockBlock(10, 4, 7, nil))
	node5 := NewNode(mock.MockBlock(101, 5, 3, nil))
	node6 := NewNode(mock.MockBlock(104, 6, 8, nil))

	tr.Insert(node1)
	tr.Insert(node2)
	tr.Insert(node3)
	tr.Insert(node4)
	tr.Insert(node5)
	tr.Insert(node6)

	assert.Len(t, tr.rootIndex, 6)
	assert.Equal(t, tr.rootIndex[genesis.Root()], tr.GetByRoot(genesis.Root()))
	assert.Equal(t, tr.rootIndex[node1.Root()], tr.GetByRoot(node1.Root()))
	// node2 not present
	assert.Equal(t, tr.rootIndex[node3.Root()], tr.GetByRoot(node3.Root()))
	assert.Equal(t, tr.rootIndex[node4.Root()], tr.GetByRoot(node4.Root()))
	assert.Equal(t, tr.rootIndex[node5.Root()], tr.GetByRoot(node5.Root()))
	assert.Equal(t, tr.rootIndex[node6.Root()], tr.GetByRoot(node6.Root()))
}
