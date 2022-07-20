// Package lmdfinalizer establishes which blocks are finalized from its LDM votes and the LDM votes of its children blocks.
// Abbreviation: LFB means Latest Finalized Block.
package lmdfinalizer

import (
	"encoding/hex"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
	"github.com/wealdtech/chaind/services/blocks/standard/lmdfinalizer/tree"
	"github.com/wealdtech/chaind/services/chaindb"
)

// PreBalanceThreshold is a mock threshold used until we connect the finalizer with the validator balances.
const PreBalanceThreshold = 10000

// LMDFinalizer is a beacon chain finalizer based on LMD votes.
type LMDFinalizer interface {
	// AddBlock to finalizer to be candidate for finalization and use its included attestations as votes for
	// other blocks.
	AddBlock(dbblock *chaindb.Block, attestations []*chaindb.Attestation)
}

// NewLFBHandler event handler to be triggered when a new LFB is finalized.
type NewLFBHandler func(phase0.Root, phase0.Slot)

// finalizer is the implementation of LMDFinalizer.
type finalizer struct {
	tree  tree.Tree
	votes lmdVotes

	log zerolog.Logger

	onAddNode chan *tree.Node

	newLFBHandler NewLFBHandler
}

// New LMDFinalizer.
func New(params ...Parameter) (LMDFinalizer, error) {
	parameters, err := parseAndCheckParameters(params...)
	if err != nil {
		return nil, errors.Wrap(err, "problem with parameters")
	}

	LFB := tree.NewNode(parameters.lfb, nil)

	f := &finalizer{
		tree:  tree.New(LFB),
		votes: newLMDVotes(),

		log: zerologger.With().Str("service", "LMD finalizer").Str("impl", "standard").Logger().Level(parameters.logLevel),

		onAddNode: make(chan *tree.Node, 1000), // TODO: size of channel

		newLFBHandler: parameters.newLFBHandler,
	}

	go f.mainLoop()

	return f, nil
}

// AddBlock to finalizer to be candidate for finalization and use its included attestations as votes for
// other blocks.
func (f *finalizer) AddBlock(dbblock *chaindb.Block, attestations []*chaindb.Attestation) {
	node := tree.NewNode(dbblock, attestations)

	f.onAddNode <- node
}

// mainLoop receives via channels commands and executed them, it is run in its own goroutine so public functions.
func (f *finalizer) mainLoop() {
	for {
		node := <-f.onAddNode
		err := f.addNode(node)
		if err != nil {
			f.log.Error().Err(err).Msg("error adding block")
		}
	}
}

// addNode to the finalizer.
func (f *finalizer) addNode(node *tree.Node) error {
	if f.tree.IsOldBlock(node) {
		return errors.New("adding block that is not in the future of latest finalized block")
	}

	f.tree.Insert(node)

	// Finalizer works even if blocks come out of order.
	children := f.tree.FindOrphans(node)
	f.adopt(node, children)

	f.attestationsToVotes(node)

	newLFB := f.countVotes()
	if newLFB != nil {
		// we have a new LFB
		f.onNewLatestFinalizedBlock(newLFB)
	}

	return nil
}

// attestationsToVotes takes all the attestations included in a node block and convert them to votes and includes them
// to be counted.
func (f *finalizer) attestationsToVotes(node *tree.Node) {
	for _, attestation := range node.Attestations() {
		if f.tree.IsOldSlot(attestation.Slot) {
			// We should ignore this attestation, as it refers to a block that is not later than the
			// latest LDM finalized block.
			continue
		}

		votes := f.attestationToVotes(attestation)
		f.votes.insert(votes)
	}

	// memory optimization
	node.RemoveAttestations()
}

// attestationToVotes returns an array of votes from an attestation.
func (finalizer) attestationToVotes(attestation *chaindb.Attestation) []lmdVote {
	return []lmdVote{
		{
			slot:   attestation.Slot,
			root:   attestation.BeaconBlockRoot,
			weight: voteWeight(len(attestation.AggregationIndices)),
		},
	}
}

// adopt children nodes by parent node.
func (f *finalizer) adopt(parent *tree.Node, children []*tree.Node) {
	f.tree.Adopt(parent, children)
	votes := []lmdVote{}
	for _, child := range children {
		votes = append(votes, lmdVote{
			root:   parent.Root(),
			slot:   parent.Slot(),
			weight: child.CurrenVote(),
		})
	}
	f.votes.insert(votes)
}

// countVotes check votes that have not been counted yet, and count them if their referred block exists in the tree.
func (f *finalizer) countVotes() *tree.Node {
	var newLFB *tree.Node

	f.votes.tryUncounted(func(vote lmdVote) bool {
		if newLFB != nil {
			// Skip counting this vote as we found a new LFB.
			return false
		}

		block := f.tree.GetByRoot(vote.root)
		if block == nil {
			// Cannot count this vote as we do not have the block it counts into.
			return false
		}

		newLFB = f.countVote(block, vote.weight)

		return true
	})

	return newLFB
}

// countVote for block and its ancestors.
// Returns a block if its weight reached threshold, otherwise nil.
func (f *finalizer) countVote(block *tree.Node, vote voteWeight) *tree.Node {
	var newLFB *tree.Node

	f.tree.Climb(block, func(block *tree.Node) bool {
		block.CountVote(vote)

		if f.threshold(block) {
			newLFB = block
			return false // Do not climb anymore, as this is the new latest finalized block.
		}
		return true
	})

	return newLFB
}

// threshold returns true if a block votes have reach the threshold to become finalized.
func (finalizer) threshold(block *tree.Node) bool {
	return block.CurrenVote() > PreBalanceThreshold
}

// onNewLatestFinalizedBlock is called when the finalizer find a new LFB. It handles the transition to a new LFB,
// and calls the event handler.
func (f *finalizer) onNewLatestFinalizedBlock(newLFB *tree.Node) {
	f.tree.OnNewLatestFinalizedBlock(newLFB)
	f.votes.newLatestFinalizedBlockSlot(newLFB.Slot())

	root := newLFB.Root()
	f.log.Info().Str("root", hex.EncodeToString(root[:])).Uint64("slot", uint64(newLFB.Slot())).Msg("new finalized block")

	if f.newLFBHandler != nil {
		f.newLFBHandler(newLFB.Root(), newLFB.Slot())
	}
}
