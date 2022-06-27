package tree

import (
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/wealdtech/chaind/services/chaindb"
)

// VoteWeight contains weighted votes towards a block
type VoteWeight uint64

// Node in LMD finalizer Tree
type Node struct {
	parent *Node

	root       phase0.Root
	parentRoot phase0.Root
	slot       phase0.Slot

	attestations []*chaindb.Attestation

	curVote VoteWeight
}

// NewNode creates a node from a chaindb block and attestations array
func NewNode(block *chaindb.Block, attestations []*chaindb.Attestation) *Node {
	n := &Node{
		slot:         block.Slot,
		attestations: attestations, // TODO perhaps copy the attestations as they are a pointer
	}

	copy(n.root[:], block.Root[:])
	copy(n.parentRoot[:], block.ParentRoot[:])

	return n
}

// CountVote into the node
func (n *Node) CountVote(vote VoteWeight) {
	n.curVote += vote
}

// RemoveAttestations clears the memory from attestations, useful if they are not needed anymore
func (n *Node) RemoveAttestations() {
	n.attestations = nil
}

// Root of node block
func (n Node) Root() phase0.Root {
	return n.root
}

// Slot of node block
func (n Node) Slot() phase0.Slot {
	return n.slot
}

// Attestations included in node block
func (n Node) Attestations() []*chaindb.Attestation {
	// TODO perhaps copy to do not break encapsulation
	return n.attestations
}

// CurrenVote towards this node block
func (n Node) CurrenVote() VoteWeight {
	return n.curVote
}

// adopt makes node parent of other node
func (n *Node) adopt(orphan *Node) {
	orphan.parent = n
}
