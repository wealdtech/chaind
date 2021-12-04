// Copyright © 2021 Weald Technology Trading.
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

import "github.com/attestantio/go-eth2-client/spec/phase0"

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
