// Copyright © 2022 Weald Technology Trading.
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

package chaintime

import (
	"time"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/wealdtech/chaind/services/chaintime"
)

type service struct{}

// New creates a new mock chain time service.
func New() chaintime.Service {
	return &service{}
}

// GenesisTime provides the time of the chain's genesis.
func (s *service) GenesisTime() time.Time { return time.Time{} }

// SlotDuration provides the duration of a single slot.
func (s *service) SlotDuration() time.Duration {
	return 12 * time.Second
}

// SlotsPerEpoch provides the number of slots in an epoch.
func (s *service) SlotsPerEpoch() uint64 {
	return 12
}

// StartOfSlot provides the time at which a given slot starts.
func (s *service) StartOfSlot(_ phase0.Slot) time.Time { return time.Time{} }

// StartOfEpoch provides the time at which a given epoch starts.
func (s *service) StartOfEpoch(_ phase0.Epoch) time.Time { return time.Time{} }

// CurrentSlot provides the current slot.
func (s *service) CurrentSlot() phase0.Slot {
	return 0
}

// CurrentEpoch provides the current epoch.
func (s *service) CurrentEpoch() phase0.Epoch {
	return 0
}

// CurrentSyncCommitteePeriod provides the current sync committee period.
func (s *service) CurrentSyncCommitteePeriod() uint64 {
	return 0
}

// SlotToEpoch provides the epoch of the given slot.
func (s *service) SlotToEpoch(_ phase0.Slot) phase0.Epoch {
	return 0
}

// SlotToSyncCommitteePeriod provides the sync committee period of the given slot.
func (s *service) SlotToSyncCommitteePeriod(_ phase0.Slot) uint64 {
	return 0
}

// EpochToSyncCommitteePeriod provides the sync committee period of the given epoch.
func (s *service) EpochToSyncCommitteePeriod(_ phase0.Epoch) uint64 {
	return 0
}

// FirstSlotOfEpoch provides the first slot of the given epoch.
func (s *service) FirstSlotOfEpoch(_ phase0.Epoch) phase0.Slot {
	return 0
}

// LastSlotOfEpoch provides the last slot of the given epoch.
func (s *service) LastSlotOfEpoch(_ phase0.Epoch) phase0.Slot {
	return 0
}

// TimestampToSlot provides the slot of the given timestamp.
func (s *service) TimestampToSlot(_ time.Time) phase0.Slot {
	return 0
}

// TimestampToEpoch provides the epoch of the given timestamp.
func (s *service) TimestampToEpoch(_ time.Time) phase0.Epoch {
	return 0
}

// FirstEpochOfSyncPeriod provides the first epoch of the given sync period.
func (s *service) FirstEpochOfSyncPeriod(_ uint64) phase0.Epoch {
	return 0
}

// AltairInitialEpoch provides the epoch at which the Altair hard fork takes place.
func (s *service) AltairInitialEpoch() phase0.Epoch {
	return 0
}

// AltairInitialSyncCommitteePeriod provides the sync committee period in which the Altair hard fork takes place.
func (s *service) AltairInitialSyncCommitteePeriod() uint64 {
	return 0
}

// BellatrixInitialEpoch provides the epoch at which the Bellatrix hard fork takes place.
func (s *service) BellatrixInitialEpoch() phase0.Epoch {
	return 0
}

// CapellaInitialEpoch provides the epoch at which the Capella hard fork takes place.
func (s *service) CapellaInitialEpoch() phase0.Epoch {
	return 0
}
