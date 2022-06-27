package lmdfinalizer

import (
	"github.com/stretchr/testify/assert"
	"github.com/wealdtech/chaind/services/blocks/standard/lmdfinalizer/mock"
	"testing"
)

func TestLMDVotes_Insert(t *testing.T) {
	votes := newLMDVotes()

	vote1 := lmdVote{
		root:   mock.MockRoot(100),
		slot:   100,
		weight: PreBalanceThreshold,
	}
	vote2 := lmdVote{
		root:   mock.MockRoot(200),
		slot:   200,
		weight: PreBalanceThreshold * 2,
	}

	votes.insert([]lmdVote{vote1, vote2})

	assert.Len(t, votes.uncounted, 2)
}

func TestLMDVotes_TryUncounted(t *testing.T) {
	votes := newLMDVotes()

	vote1 := lmdVote{
		root:   mock.MockRoot(100),
		slot:   100,
		weight: PreBalanceThreshold,
	}
	vote2 := lmdVote{
		root:   mock.MockRoot(200),
		slot:   200,
		weight: PreBalanceThreshold * 2,
	}

	votes.insert([]lmdVote{vote1, vote2})

	count := 0
	votes.tryUncounted(func(vote lmdVote) bool {
		count++
		return false
	})
	assert.Equal(t, 2, count)

	count = 0
	votes.tryUncounted(func(vote lmdVote) bool {
		count++
		return true
	})
	assert.Equal(t, 2, count)

	count = 0
	votes.tryUncounted(func(vote lmdVote) bool {
		count++
		return true
	})
	assert.Equal(t, 0, count)
}

func TestLMDVotes_onNewLatestFinalizedBlock(t *testing.T) {
	votes := newLMDVotes()

	vote1 := lmdVote{
		root:   mock.MockRoot(100),
		slot:   100,
		weight: PreBalanceThreshold,
	}
	vote2 := lmdVote{
		root:   mock.MockRoot(200),
		slot:   200,
		weight: PreBalanceThreshold * 2,
	}

	votes.insert([]lmdVote{vote1, vote2})

	assert.Len(t, votes.uncounted, 2)

	votes.newLatestFinalizedBlockSlot(150)

	assert.Len(t, votes.uncounted, 1)
}
