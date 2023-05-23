// Copyright © 2021 - 2023 Weald Technology Trading.
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

package chaindb

import (
	"time"

	"github.com/attestantio/go-eth2-client/spec/phase0"
)

// Order is the order in which results should be fetched (N.B. fetched, not returned).
type Order uint8

const (
	// OrderEarliest fetches earliest transactions first.
	OrderEarliest Order = iota
	// OrderLatest fetches latest transactions first.
	OrderLatest
)

// ValidatorSummaryFilter defines a filter for fetching validator summaries.
// Filter elements are ANDed together.
// Results are always returned in ascending (epoch, validator index) order.
type ValidatorSummaryFilter struct {
	// Limit is the maximum number of summaries to return.
	Limit uint32

	// Order is either OrderEarliest, in which case the earliest results
	// that match the filter are returned, or OrderLatest, in which case the
	// latest results that match the filter are returned.
	// The default is OrderEarliest.
	Order Order

	// From is the earliest epoch from which to fetch summaries.
	// If nil then there is no earliest epoch.
	From *phase0.Epoch

	// To is the latest epoch from which to fetch summaries.
	// If nil then there is no latest epoch.
	To *phase0.Epoch

	// ValidatorIndices is the list of validator indices for which to obtain summaries.
	// If nil then no filter is applied
	ValidatorIndices *[]phase0.ValidatorIndex
}

// EpochSummaryFilter defines a filter for fetching epoch summaries.
// Filter elements are ANDed together.
// Results are always returned in ascending epoch order.
type EpochSummaryFilter struct {
	// Limit is the maximum number of summaries to return.
	Limit uint32

	// Order is either OrderEarliest, in which case the earliest results
	// that match the filter are returned, or OrderLatest, in which case the
	// latest results that match the filter are returned.
	// The default is OrderEarliest.
	Order Order

	// From is the earliest epoch from which to fetch summaries.
	// If nil then there is no earliest epoch.
	From *phase0.Epoch

	// To is the latest epoch from which to fetch summaries.
	// If nil then there is no latest epoch.
	To *phase0.Epoch
}

// ValidatorDaySummaryFilter defines a filter for fetching validator day summaries.
// Filter elements are ANDed together.
// Results are always returned in ascending (start timestamp, validator index) order.
type ValidatorDaySummaryFilter struct {
	// Limit is the maximum number of summaries to return.
	Limit uint32

	// Order is either OrderEarliest, in which case the earliest results
	// that match the filter are returned, or OrderLatest, in which case the
	// latest results that match the filter are returned.
	// The default is OrderEarliest.
	Order Order

	// From is the earliest timestamp from which to fetch summaries.
	// If nil then there is no earliest epoch.
	From *time.Time

	// To is the latest timestamp from which to fetch summaries.
	// If nil then there is no latest epoch.
	To *time.Time

	// ValidatorIndices is the list of validator indices for which to obtain summaries.
	// If nil then no filter is applied
	ValidatorIndices *[]phase0.ValidatorIndex
}

// BeaconCommitteeFilter defines a filter for fetching beacon committees.
// Filter elements are ANDed together.
// Results are always returned in ascending (slot, committee index) order.
type BeaconCommitteeFilter struct {
	// Limit is the maximum number of items to return.
	Limit uint32

	// Order is either OrderEarliest, in which case the earliest results
	// that match the filter are returned, or OrderLatest, in which case the
	// latest results that match the filter are returned.
	// The default is OrderEarliest.
	Order Order

	// From is the earliest slot from which to fetch items.
	// If nil then there is no earliest slot.
	From *phase0.Slot

	// To is the latest slot to which to fetch items.
	// If nil then there is no latest slot.
	To *phase0.Slot

	// CommitteeIndices is the list of committee indices for which to obtain items.
	// If nil then no filter is applied
	CommitteeIndices []phase0.CommitteeIndex
}

// SyncAggregateFilter defines a filter for fetching sync aggregates.
// Filter elements are ANDed together.
// Results are always returned in ascending slot order.
type SyncAggregateFilter struct {
	// Limit is the maximum number of items to return.
	Limit uint32

	// Order is either OrderEarliest, in which case the earliest results
	// that match the filter are returned, or OrderLatest, in which case the
	// latest results that match the filter are returned.
	// The default is OrderEarliest.
	Order Order

	// From is the earliest slot from which to fetch items.
	// This relates to the inclusion slot.
	// If nil then there is no earliest slot.
	From *phase0.Slot

	// To is the latest slot to which to fetch items.
	// This relates to the inclusion slot.
	// If nil then there is no latest slot.
	To *phase0.Slot
}

// BLSToExecutionChangeFilter defines a filter for fetching BLS to execution changes.
// Filter elements are ANDed together.
// Results are always returned in ascending (slot,index) order.
type BLSToExecutionChangeFilter struct {
	// Limit is the maximum number of items to return.
	Limit uint32

	// Order is either OrderEarliest, in which case the earliest results
	// that match the filter are returned, or OrderLatest, in which case the
	// latest results that match the filter are returned.
	// The default is OrderEarliest.
	Order Order

	// From is the earliest slot from which to fetch items.
	// This relates to the inclusion slot.
	// If nil then there is no earliest slot.
	From *phase0.Slot

	// To is the latest slot to which to fetch items.
	// This relates to the inclusion slot.
	// If nil then there is no latest slot.
	To *phase0.Slot

	// ValidatorIndices is the list of validator indices for which to obtain items.
	// If nil then no filter is applied
	ValidatorIndices []phase0.ValidatorIndex
}

// BlockFilter defines a filter for fetching blocks.
// Filter elements are ANDed together.
// Results are always returned in ascending (slot,root) order.
type BlockFilter struct {
	// Limit is the maximum number of items to return.
	Limit uint32

	// Order is either OrderEarliest, in which case the earliest results
	// that match the filter are returned, or OrderLatest, in which case the
	// latest results that match the filter are returned.
	// The default is OrderEarliest.
	Order Order

	// From is the earliest slot from which to fetch items.
	// This relates to the inclusion slot.
	// If nil then there is no earliest slot.
	From *phase0.Slot

	// To is the latest slot to which to fetch items.
	// This relates to the inclusion slot.
	// If nil then there is no latest slot.
	To *phase0.Slot

	// Canonical must match the canonical flag.
	// If nil then no filter is applied
	Canonical *bool
}

// WithdrawalFilter defines a filter for fetching withdrawals.
// Filter elements are ANDed together.
// Results are always returned in ascending (slot,index) order.
type WithdrawalFilter struct {
	// Limit is the maximum number of items to return.
	Limit uint32

	// Order is either OrderEarliest, in which case the earliest results
	// that match the filter are returned, or OrderLatest, in which case the
	// latest results that match the filter are returned.
	// The default is OrderEarliest.
	Order Order

	// From is the earliest slot from which to fetch items.
	// This relates to the inclusion slot.
	// If nil then there is no earliest slot.
	From *phase0.Slot

	// To is the latest slot to which to fetch items.
	// This relates to the inclusion slot.
	// If nil then there is no latest slot.
	To *phase0.Slot

	// ValidatorIndices is the list of validator indices for which to obtain items.
	// If nil then no filter is applied.
	ValidatorIndices []phase0.ValidatorIndex

	// Canonical will return only withdrawals from canonical or non-canonical blocks.
	// Note that neither true nor false will return withdrawals from indeterminate blocks.
	// If nil then no filter is applied.
	Canonical *bool
}
