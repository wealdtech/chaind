package tree_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wealdtech/chaind/services/blocks/standard/lmdfinalizer/mock"
	"github.com/wealdtech/chaind/services/blocks/standard/lmdfinalizer/tree"
	"testing"
)

var genesis = tree.NewNode(mock.MockBlock(0, 0, 0, nil))

func TestTree_InsertAndGetByRoot(t *testing.T) {
	tr := tree.New(genesis)

	node1 := tree.NewNode(mock.MockBlock(2, 1, 0, nil))
	node2 := tree.NewNode(mock.MockBlock(2, 2, 0, nil))

	tr.Insert(node1)
	tr.Insert(node2)

	expectGenesis := tr.GetByRoot(genesis.Root())
	expectNode1 := tr.GetByRoot(node1.Root())
	expectNode2 := tr.GetByRoot(node2.Root())

	assert.Equal(t, expectGenesis, genesis)
	assert.Equal(t, expectNode1, node1)
	assert.Equal(t, expectNode2, node2)
}

func TestTree_InsertSameRoot(t *testing.T) {
	tr := tree.New(genesis)

	node1 := tree.NewNode(mock.MockBlock(2, 1, 0, nil))
	node2 := tree.NewNode(mock.MockBlock(2, 2, 0, nil))

	tr.Insert(node1)
	tr.Insert(node2)

	expectGenesis := tr.GetByRoot(genesis.Root())
	expectNode1 := tr.GetByRoot(node1.Root())
	expectNode2 := tr.GetByRoot(node2.Root())

	assert.Equal(t, expectGenesis, genesis)
	assert.Equal(t, expectNode1, node1)
	assert.Equal(t, expectNode2, node2)
}

func TestTree_IsOldSlot(t *testing.T) {
	node1 := tree.NewNode(mock.MockBlock(100, 2, 0, nil))

	tr := tree.New(node1)

	assert.True(t, tr.IsOldSlot(2))
	assert.False(t, tr.IsOldSlot(200))
}

func TestTree_IsOldBlock(t *testing.T) {
	node1 := tree.NewNode(mock.MockBlock(100, 1, 0, nil))

	tr := tree.New(node1)

	node2 := tree.NewNode(mock.MockBlock(10, 2, 0, nil))
	node3 := tree.NewNode(mock.MockBlock(200, 3, 0, nil))

	assert.True(t, tr.IsOldBlock(node2))
	assert.False(t, tr.IsOldBlock(node3))
}

func TestTree_InsertOld(t *testing.T) {
	node1 := tree.NewNode(mock.MockBlock(100, 2, 0, nil))

	tr := tree.New(node1)

	node2 := tree.NewNode(mock.MockBlock(10, 1, 0, nil))

	tr.Insert(node2)

	assert.Equal(t, (*tree.Node)(nil), tr.GetByRoot(node2.Root()))
}

func TestTree_FindOrphans(t *testing.T) {
	tr := tree.New(genesis)

	node1 := tree.NewNode(mock.MockBlock(2, 1, 0, nil))
	node2 := tree.NewNode(mock.MockBlock(2, 2, 0, nil))
	node3 := tree.NewNode(mock.MockBlock(5, 3, 1, nil))
	node4 := tree.NewNode(mock.MockBlock(10, 4, 7, nil))

	tr.Insert(node3)
	tr.Insert(node4)

	tr.Insert(node1)
	tr.Insert(node2)

	orphans := tr.FindOrphans(tr.GetByRoot(node1.Root()))
	assert.Len(t, orphans, 1)
	assert.Equal(t, orphans[0], tr.GetByRoot(node3.Root()))

	tr.Adopt(tr.GetByRoot(node1.Root()), orphans)

	orphans = tr.FindOrphans(tr.GetByRoot(node1.Root()))
	assert.Len(t, orphans, 0)
}

func TestTree_Climb(t *testing.T) {
	tr := tree.New(genesis)

	node1 := tree.NewNode(mock.MockBlock(2, 1, 0, nil))
	node2 := tree.NewNode(mock.MockBlock(2, 2, 0, nil))
	node3 := tree.NewNode(mock.MockBlock(5, 3, 1, nil))
	node4 := tree.NewNode(mock.MockBlock(10, 4, 7, nil))
	node5 := tree.NewNode(mock.MockBlock(11, 5, 3, nil))

	tr.Insert(node3)
	tr.Insert(node4)
	tr.Insert(node5)

	tr.Insert(node1)
	tr.Insert(node2)

	count := 0
	tr.Climb(tr.GetByRoot(node5.Root()), func(node *tree.Node) bool {
		count++

		switch count {
		case 1:
			assert.Equal(t, node, tr.GetByRoot(node5.Root()))
		case 2:
			assert.Equal(t, node, tr.GetByRoot(node3.Root()))
		default:
			assert.Fail(t, "should only be 2")
		}

		return true
	})
	assert.Equal(t, count, 2)

	orphans := tr.FindOrphans(tr.GetByRoot(node1.Root()))
	tr.Adopt(tr.GetByRoot(node1.Root()), orphans)

	count = 0
	tr.Climb(tr.GetByRoot(node5.Root()), func(node *tree.Node) bool {
		count++

		switch count {
		case 1:
			assert.Equal(t, node, tr.GetByRoot(node5.Root()))
		case 2:
			assert.Equal(t, node, tr.GetByRoot(node3.Root()))
		case 3:
			assert.Equal(t, node, tr.GetByRoot(node1.Root()))
		default:
			assert.Fail(t, "should only be 3")
		}

		return true
	})
	assert.Equal(t, count, 3)

	count = 0
	tr.Climb(tr.GetByRoot(node5.Root()), func(node *tree.Node) bool {
		count++

		switch count {
		case 1:
			assert.Equal(t, node, tr.GetByRoot(node5.Root()))
		case 2:
			assert.Equal(t, node, tr.GetByRoot(node3.Root()))
			return false // do not keep climbing
		default:
			assert.Fail(t, "should only be 2")
		}

		return true
	})
	assert.Equal(t, count, 2)
}
