package lmdfinalizer

import (
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/wealdtech/chaind/services/blocks/standard/lmdfinalizer/tree"
)

type voteWeight = tree.VoteWeight

type lmdVote struct {
	root   phase0.Root
	slot   phase0.Slot
	weight voteWeight
}

type lmdVotes struct {
	uncounted []lmdVote
}

func newLMDVotes() lmdVotes {
	return lmdVotes{
		uncounted: []lmdVote{},
	}
}

func (v *lmdVotes) insert(votes []lmdVote) {
	v.uncounted = append(v.uncounted, votes...)
}

func (v *lmdVotes) tryUncounted(fn func(vote lmdVote) bool) {
	newUncounted := []lmdVote{}

	for _, vote := range v.uncounted {
		if !fn(vote) {
			newUncounted = append(newUncounted, vote)
		}
	}

	v.uncounted = newUncounted
}

func (v *lmdVotes) newLatestFinalizedBlockSlot(slot phase0.Slot) {
	// TODO perhaps uncounted could be a map of slot to array of votes and this would be more efficient.
	newUncounted := []lmdVote{}

	for _, vote := range v.uncounted {
		if slot < vote.slot {
			newUncounted = append(newUncounted, vote)
		}
	}

	v.uncounted = newUncounted
}
