package lmdfinalizer_test

import (
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/wealdtech/chaind/services/blocks/standard/lmdfinalizer"
	"github.com/wealdtech/chaind/services/blocks/standard/lmdfinalizer/mock"
	"sync"
	"testing"
)

var genesis, _ = mock.MockBlock(0, 0, 0, nil)

func TestFinalizer_SimpleRun(t *testing.T) {
	log := zerologger.With().Logger().Level(zerolog.ErrorLevel)
	f := lmdfinalizer.New(log)

	f.Start(genesis)

	count := 0
	var wg sync.WaitGroup
	wg.Add(2)
	f.HandleNewLatestFinalizedBlock(func(root phase0.Root, slot phase0.Slot) {
		wg.Done()
		count++

		switch count {
		case 1:
			assert.Equal(t, phase0.Slot(2), slot)
			assert.EqualValues(t, mock.MockRoot(1), root)
		case 2:
			assert.Equal(t, phase0.Slot(100), slot)
			assert.EqualValues(t, mock.MockRoot(3), root)
		default:
			assert.Fail(t, "should there be only 2")
		}
	})

	f.AddBlock(mock.MockBlock(2, 1, 0, nil))     // 1: child of genesis
	f.AddBlock(mock.MockBlock(2, 2, 0, nil))     // 2: child of genesis
	f.AddBlock(mock.MockBlock(100, 3, 1, nil))   // 3: child of 1
	f.AddBlock(mock.MockBlock(10, 4, 1000, nil)) // 4: child of none
	f.AddBlock(mock.MockBlock(101, 5, 3, []mock.MockAttestation{
		{
			Slot:       2,
			Root:       1,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
		{
			Slot:       100,
			Root:       3,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
	})) // 5: child of 3
	f.AddBlock(mock.MockBlock(104, 6, 1000, nil)) // 6: child of none
	f.AddBlock(mock.MockBlock(110, 7, 1000, []mock.MockAttestation{
		{
			Slot:       2,
			Root:       1,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
		{
			Slot:       100,
			Root:       3,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
		{
			Slot:       101,
			Root:       5,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
	})) // 7: child of 5
	f.AddBlock(mock.MockBlock(112, 8, 7, []mock.MockAttestation{
		{
			Slot:       2,
			Root:       1,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
		{
			Slot:       100,
			Root:       3,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
		{
			Slot:       101,
			Root:       5,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
		{
			Slot:       110,
			Root:       7,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
	})) // 8: child of 7
	f.AddBlock(mock.MockBlock(113, 9, 8, nil)) // 8: child of 8

	wg.Wait()

	assert.Equal(t, 2, count)
}

func TestFinalizer_SimpleRunOutOfOrder(t *testing.T) {
	log := zerologger.With().Logger().Level(zerolog.ErrorLevel)
	f := lmdfinalizer.New(log)

	f.Start(genesis)

	count := 0
	var wg sync.WaitGroup
	wg.Add(2)
	f.HandleNewLatestFinalizedBlock(func(root phase0.Root, slot phase0.Slot) {
		wg.Done()
		count++

		switch count {
		case 1:
			assert.Equal(t, phase0.Slot(2), slot)
			assert.EqualValues(t, mock.MockRoot(1), root)
		case 2:
			assert.Equal(t, phase0.Slot(100), slot)
			assert.EqualValues(t, mock.MockRoot(3), root)
		default:
			assert.Fail(t, "should there be only 2")
		}
	})

	f.AddBlock(mock.MockBlock(100, 3, 1, nil))   // 3: child of 1
	f.AddBlock(mock.MockBlock(10, 4, 1000, nil)) // 4: child of none
	// 5: child of 3
	f.AddBlock(mock.MockBlock(104, 6, 1000, nil)) // 6: child of none
	f.AddBlock(mock.MockBlock(110, 7, 1000, []mock.MockAttestation{
		{
			Slot:       2,
			Root:       1,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
		{
			Slot:       100,
			Root:       3,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
		{
			Slot:       101,
			Root:       5,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
	})) // 7: child of 5
	f.AddBlock(mock.MockBlock(101, 5, 3, []mock.MockAttestation{
		{
			Slot:       2,
			Root:       1,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
		{
			Slot:       100,
			Root:       3,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
	}))
	f.AddBlock(mock.MockBlock(2, 1, 0, nil)) // 1: child of genesis
	f.AddBlock(mock.MockBlock(2, 2, 0, nil)) // 2: child of genesis
	f.AddBlock(mock.MockBlock(112, 8, 7, []mock.MockAttestation{
		{
			Slot:       2,
			Root:       1,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
		{
			Slot:       100,
			Root:       3,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
		{
			Slot:       101,
			Root:       5,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
		{
			Slot:       110,
			Root:       7,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
	})) // 8: child of 7
	f.AddBlock(mock.MockBlock(113, 9, 8, nil)) // 8: child of 8

	wg.Wait()

	assert.Equal(t, 2, count)
}

func TestFinalizer_SimpleRunStartAfter(t *testing.T) {
	log := zerologger.With().Logger().Level(zerolog.ErrorLevel)
	f := lmdfinalizer.New(log)

	count := 0
	var wg sync.WaitGroup
	wg.Add(2)
	f.HandleNewLatestFinalizedBlock(func(root phase0.Root, slot phase0.Slot) {
		wg.Done()
		count++

		switch count {
		case 1:
			assert.Equal(t, phase0.Slot(2), slot)
			assert.EqualValues(t, mock.MockRoot(1), root)
		case 2:
			assert.Equal(t, phase0.Slot(100), slot)
			assert.EqualValues(t, mock.MockRoot(3), root)
		default:
			assert.Fail(t, "should there be only 2")
		}
	})

	f.AddBlock(mock.MockBlock(2, 1, 0, nil))     // 1: child of genesis
	f.AddBlock(mock.MockBlock(2, 2, 0, nil))     // 2: child of genesis
	f.AddBlock(mock.MockBlock(100, 3, 1, nil))   // 3: child of 1
	f.AddBlock(mock.MockBlock(10, 4, 1000, nil)) // 4: child of none
	f.AddBlock(mock.MockBlock(101, 5, 3, []mock.MockAttestation{
		{
			Slot:       2,
			Root:       1,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
		{
			Slot:       100,
			Root:       3,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
	})) // 5: child of 3
	f.AddBlock(mock.MockBlock(104, 6, 1000, nil)) // 6: child of none
	f.AddBlock(mock.MockBlock(110, 7, 1000, []mock.MockAttestation{
		{
			Slot:       2,
			Root:       1,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
		{
			Slot:       100,
			Root:       3,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
		{
			Slot:       101,
			Root:       5,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
	})) // 7: child of 5
	f.AddBlock(mock.MockBlock(112, 8, 7, []mock.MockAttestation{
		{
			Slot:       2,
			Root:       1,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
		{
			Slot:       100,
			Root:       3,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
		{
			Slot:       101,
			Root:       5,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
		{
			Slot:       110,
			Root:       7,
			NumIndices: lmdfinalizer.PreBalanceThreshold / 4,
		},
	})) // 8: child of 7
	f.AddBlock(mock.MockBlock(113, 9, 8, nil)) // 8: child of 8

	f.Start(genesis)

	wg.Wait()

	assert.Equal(t, 2, count)
}
