package tree

import (
	"github.com/attestantio/go-eth2-client/spec/phase0"
)

// Tree for LMD finalizer.
type Tree struct {
	latestFinalizedBlock *Node // root of the tree
	rootIndex            map[phase0.Root]*Node
	orphans              map[phase0.Root]*Node
}

// New creates a tree.
func New(latestFinalizedBlock *Node) Tree {
	t := Tree{
		latestFinalizedBlock: latestFinalizedBlock,
		rootIndex:            make(map[phase0.Root]*Node),
		orphans:              map[phase0.Root]*Node{},
	}
	t.rootIndex[latestFinalizedBlock.root] = latestFinalizedBlock

	return t
}

// Insert new node into the tree.
func (t *Tree) Insert(node *Node) {
	if t.IsOldBlock(node) {
		// Do not insert it, as it is not in the future of LFB.
		// TODO logging?
		return
	}

	curBlock, already := t.rootIndex[node.root]
	if already && curBlock != nil {
		// TODO logging
		return // Already included, be idempotent.
	}

	t.rootIndex[node.root] = node

	parent := t.rootIndex[node.parentRoot]
	if parent != nil {
		parent.adopt(node)
	} else {
		t.insertOrphan(node)
	}
}

// Adopt makes parent the parent of children, and remove the children from the orphan list.
func (t *Tree) Adopt(parent *Node, children []*Node) {
	for _, child := range children {
		if child.parentRoot == parent.root {
			parent.adopt(child)
			t.removeOrphan(child)
		} // TODO else case logging?
	}
}

// GetByRoot returns a node by its block root.
func (t *Tree) GetByRoot(root phase0.Root) *Node {
	return t.rootIndex[root]
}

// IsOldSlot returns true if a slot is less or equal than the LFB slot.
func (t *Tree) IsOldSlot(slot phase0.Slot) bool {
	return slot <= t.latestFinalizedBlock.slot
}

// IsOldBlock returns true if the node slot is less or equal than the LFB slot.
func (t *Tree) IsOldBlock(node *Node) bool {
	return t.IsOldSlot(node.slot)
}

// Climb from a node towards the LFB by the tree, it passes each node to the `callback`.
// It does not include the LFB, stopping in the direct child of it.
// If the callback returns `false` the tree climbing is stopped at the current node.
func (t *Tree) Climb(node *Node, callback func(*Node) bool) {
	for {
		if node == nil || node.root == t.latestFinalizedBlock.root {
			// Stop when no parent (orphan) or arrived to the top of the tree.
			return
		}

		if !callback(node) {
			// Stop if iterator wants to stop.
			return
		}

		node = node.parent
	}
}

// OnNewLatestFinalizedBlock reroots the tree on a new LFB.
// Old nodes are removed as they are not relevant anymore.
func (t *Tree) OnNewLatestFinalizedBlock(newLFB *Node) {
	t.latestFinalizedBlock = newLFB

	t.removeOldRootIndex()
	t.removeOldOrphans()
}

// FindOrphans which father is a given node.
func (t *Tree) FindOrphans(node *Node) []*Node {
	children := []*Node{}

	for _, orphan := range t.orphans {
		if orphan.parentRoot == node.root {
			children = append(children, orphan)
		}
	}

	return children
}

func (t *Tree) removeOldRootIndex() {
	for root, block := range t.rootIndex {
		if t.IsOldBlock(block) && root != t.latestFinalizedBlock.root {
			delete(t.rootIndex, root)
		}
	}
}

func (t *Tree) removeOldOrphans() {
	for _, orphan := range t.orphans {
		if t.IsOldBlock(orphan) {
			t.removeOrphan(orphan)
		}
	}
}

func (t *Tree) insertOrphan(block *Node) {
	t.orphans[block.root] = block
}

func (t *Tree) removeOrphan(orphan *Node) {
	delete(t.orphans, orphan.root)
}
