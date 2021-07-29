// Copyright © 2020, 2021 Weald Technology Trading.
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

package standard_test

import (
	"context"
	"testing"
	"time"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/chaind/services/chaintime/standard"
	"github.com/wealdtech/chaind/testing/mock"
)

func TestService(t *testing.T) {
	genesisTime := time.Now()
	slotDuration := 12 * time.Second
	slotsPerEpoch := uint64(32)
	mockGenesisTimeProvider := mock.NewGenesisTimeProvider(genesisTime)
	mockSlotDurationProvider := mock.NewSlotDurationProvider(slotDuration)
	mockSlotsPerEpochProvider := mock.NewSlotsPerEpochProvider(slotsPerEpoch)
	tests := []struct {
		name   string
		params []standard.Parameter
		err    string
	}{
		{
			name: "GenesisTimeProviderMissing",
			params: []standard.Parameter{
				standard.WithLogLevel(zerolog.Disabled),
				standard.WithSlotDurationProvider(mockSlotDurationProvider),
				standard.WithSlotsPerEpochProvider(mockSlotsPerEpochProvider),
			},
			err: "problem with parameters: no genesis time provider specified",
		},
		{
			name: "SlotDurationProviderMissing",
			params: []standard.Parameter{
				standard.WithLogLevel(zerolog.Disabled),
				standard.WithGenesisTimeProvider(mockGenesisTimeProvider),
				standard.WithSlotsPerEpochProvider(mockSlotsPerEpochProvider),
			},
			err: "problem with parameters: no slot duration provider specified",
		},
		{
			name: "SlotsPerEpochProviderMissing",
			params: []standard.Parameter{
				standard.WithLogLevel(zerolog.Disabled),
				standard.WithGenesisTimeProvider(mockGenesisTimeProvider),
				standard.WithSlotDurationProvider(mockSlotDurationProvider),
			},
			err: "problem with parameters: no slots per epoch provider specified",
		},
		{
			name: "Good",
			params: []standard.Parameter{
				standard.WithLogLevel(zerolog.Disabled),
				standard.WithGenesisTimeProvider(mockGenesisTimeProvider),
				standard.WithSlotDurationProvider(mockSlotDurationProvider),
				standard.WithSlotsPerEpochProvider(mockSlotsPerEpochProvider),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := standard.New(context.Background(), test.params...)
			if test.err != "" {
				require.EqualError(t, err, test.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestGenesisTime(t *testing.T) {
	genesisTime := time.Now()
	slotDuration := 12 * time.Second
	slotsPerEpoch := uint64(32)
	mockGenesisTimeProvider := mock.NewGenesisTimeProvider(genesisTime)
	mockSlotDurationProvider := mock.NewSlotDurationProvider(slotDuration)
	mockSlotsPerEpochProvider := mock.NewSlotsPerEpochProvider(slotsPerEpoch)
	s, err := standard.New(context.Background(),
		standard.WithGenesisTimeProvider(mockGenesisTimeProvider),
		standard.WithSlotDurationProvider(mockSlotDurationProvider),
		standard.WithSlotsPerEpochProvider(mockSlotsPerEpochProvider),
	)
	require.NoError(t, err)

	require.Equal(t, genesisTime, s.GenesisTime())
}

func TestStartOfSlot(t *testing.T) {
	genesisTime := time.Now()
	slotDuration := 12 * time.Second
	slotsPerEpoch := uint64(32)
	mockGenesisTimeProvider := mock.NewGenesisTimeProvider(genesisTime)
	mockSlotDurationProvider := mock.NewSlotDurationProvider(slotDuration)
	mockSlotsPerEpochProvider := mock.NewSlotsPerEpochProvider(slotsPerEpoch)
	s, err := standard.New(context.Background(),
		standard.WithGenesisTimeProvider(mockGenesisTimeProvider),
		standard.WithSlotDurationProvider(mockSlotDurationProvider),
		standard.WithSlotsPerEpochProvider(mockSlotsPerEpochProvider),
	)
	require.NoError(t, err)

	require.Equal(t, genesisTime, s.StartOfSlot(0))
	require.Equal(t, genesisTime.Add(1000*slotDuration), s.StartOfSlot(1000))
}

func TestStartOfEpoch(t *testing.T) {
	genesisTime := time.Now()
	slotDuration := 12 * time.Second
	slotsPerEpoch := uint64(32)
	mockGenesisTimeProvider := mock.NewGenesisTimeProvider(genesisTime)
	mockSlotDurationProvider := mock.NewSlotDurationProvider(slotDuration)
	mockSlotsPerEpochProvider := mock.NewSlotsPerEpochProvider(slotsPerEpoch)
	s, err := standard.New(context.Background(),
		standard.WithGenesisTimeProvider(mockGenesisTimeProvider),
		standard.WithSlotDurationProvider(mockSlotDurationProvider),
		standard.WithSlotsPerEpochProvider(mockSlotsPerEpochProvider),
	)
	require.NoError(t, err)

	require.Equal(t, genesisTime, s.StartOfEpoch(0))
	require.Equal(t, genesisTime.Add(time.Duration(1000*slotsPerEpoch)*slotDuration), s.StartOfEpoch(1000))
}

func TestCurrentSlot(t *testing.T) {
	slotDuration := 12 * time.Second
	slotsPerEpoch := uint64(32)
	genesisTime := time.Now().Add(-5 * slotDuration)
	mockGenesisTimeProvider := mock.NewGenesisTimeProvider(genesisTime)
	mockSlotDurationProvider := mock.NewSlotDurationProvider(slotDuration)
	mockSlotsPerEpochProvider := mock.NewSlotsPerEpochProvider(slotsPerEpoch)
	s, err := standard.New(context.Background(),
		standard.WithGenesisTimeProvider(mockGenesisTimeProvider),
		standard.WithSlotDurationProvider(mockSlotDurationProvider),
		standard.WithSlotsPerEpochProvider(mockSlotsPerEpochProvider),
	)
	require.NoError(t, err)

	require.Equal(t, phase0.Slot(5), s.CurrentSlot())
}

func TestCurrentEpoch(t *testing.T) {
	slotDuration := 12 * time.Second
	slotsPerEpoch := uint64(32)
	genesisTime := time.Now().Add(time.Duration(int64(-2)*int64(slotsPerEpoch)) * slotDuration)
	mockGenesisTimeProvider := mock.NewGenesisTimeProvider(genesisTime)
	mockSlotDurationProvider := mock.NewSlotDurationProvider(slotDuration)
	mockSlotsPerEpochProvider := mock.NewSlotsPerEpochProvider(slotsPerEpoch)
	s, err := standard.New(context.Background(),
		standard.WithGenesisTimeProvider(mockGenesisTimeProvider),
		standard.WithSlotDurationProvider(mockSlotDurationProvider),
		standard.WithSlotsPerEpochProvider(mockSlotsPerEpochProvider),
	)
	require.NoError(t, err)

	require.Equal(t, phase0.Epoch(2), s.CurrentEpoch())
}

func TestTimestampToSlot(t *testing.T) {
	slotDuration := 12 * time.Second
	slotsPerEpoch := uint64(32)
	now := time.Now()
	genesisTime := now
	mockGenesisTimeProvider := mock.NewGenesisTimeProvider(genesisTime)
	mockSlotDurationProvider := mock.NewSlotDurationProvider(slotDuration)
	mockSlotsPerEpochProvider := mock.NewSlotsPerEpochProvider(slotsPerEpoch)
	s, err := standard.New(context.Background(),
		standard.WithGenesisTimeProvider(mockGenesisTimeProvider),
		standard.WithSlotDurationProvider(mockSlotDurationProvider),
		standard.WithSlotsPerEpochProvider(mockSlotsPerEpochProvider),
	)
	require.NoError(t, err)

	tests := []struct {
		name      string
		timestamp time.Time
		slot      phase0.Slot
	}{
		{
			name:      "PreGenesis",
			timestamp: now.AddDate(0, 0, -1),
			slot:      0,
		},
		{
			name:      "Genesis",
			timestamp: now,
			slot:      0,
		},
		{
			name:      "Slot1",
			timestamp: now.Add(slotDuration),
			slot:      1,
		},
		{
			name:      "Slot999",
			timestamp: now.Add(999 * slotDuration),
			slot:      999,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.slot, s.TimestampToSlot(test.timestamp))
		})
	}
}

func TestTimestampToEpoch(t *testing.T) {
	slotDuration := 12 * time.Second
	slotsPerEpoch := uint64(32)
	now := time.Now()
	genesisTime := now
	mockGenesisTimeProvider := mock.NewGenesisTimeProvider(genesisTime)
	mockSlotDurationProvider := mock.NewSlotDurationProvider(slotDuration)
	mockSlotsPerEpochProvider := mock.NewSlotsPerEpochProvider(slotsPerEpoch)
	s, err := standard.New(context.Background(),
		standard.WithGenesisTimeProvider(mockGenesisTimeProvider),
		standard.WithSlotDurationProvider(mockSlotDurationProvider),
		standard.WithSlotsPerEpochProvider(mockSlotsPerEpochProvider),
	)
	require.NoError(t, err)

	tests := []struct {
		name      string
		timestamp time.Time
		epoch     phase0.Epoch
	}{
		{
			name:      "PreGenesis",
			timestamp: now.AddDate(0, 0, -1),
			epoch:     0,
		},
		{
			name:      "Genesis",
			timestamp: now,
			epoch:     0,
		},
		{
			name:      "Epoch1",
			timestamp: now.Add(time.Duration(slotsPerEpoch) * slotDuration),
			epoch:     1,
		},
		{
			name:      "Epoch1Boundary",
			timestamp: now.Add(2 * time.Duration(slotsPerEpoch) * slotDuration).Add(-1 * time.Millisecond),
			epoch:     1,
		},
		{
			name:      "Epoch999",
			timestamp: now.Add(999 * time.Duration(slotsPerEpoch) * slotDuration),
			epoch:     999,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.epoch, s.TimestampToEpoch(test.timestamp))
		})
	}
}
