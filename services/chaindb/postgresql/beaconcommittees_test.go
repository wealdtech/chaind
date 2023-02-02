// Copyright © 2022 Weald Technology Limited.
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

package postgresql_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/chaind/services/chaindb"
	"github.com/wealdtech/chaind/services/chaindb/postgresql"
)

func TestBeaconCommittees(t *testing.T) {
	ctx := context.Background()
	s, err := postgresql.New(ctx,
		postgresql.WithServer(os.Getenv("CHAINDB_SERVER")),
		postgresql.WithPort(atoi(os.Getenv("CHAINDB_PORT"))),
		postgresql.WithUser(os.Getenv("CHAINDB_USER")),
		postgresql.WithPassword(os.Getenv("CHAINDB_PASSWORD")),
	)
	require.NoError(t, err)

	ctx, cancel, err := s.BeginTx(ctx)
	require.NoError(t, err)
	defer cancel()

	committee1 := &chaindb.BeaconCommittee{
		Slot:      2000000001,
		Index:     1,
		Committee: []phase0.ValidatorIndex{1, 2, 3},
	}
	require.NoError(t, s.SetBeaconCommittee(ctx, committee1))

	committee2 := &chaindb.BeaconCommittee{
		Slot:      2000000001,
		Index:     2,
		Committee: []phase0.ValidatorIndex{4, 5, 6},
	}
	require.NoError(t, s.SetBeaconCommittee(ctx, committee2))

	committee3 := &chaindb.BeaconCommittee{
		Slot:      2000000002,
		Index:     1,
		Committee: []phase0.ValidatorIndex{7, 8, 9},
	}
	require.NoError(t, s.SetBeaconCommittee(ctx, committee3))

	tests := []struct {
		name       string
		filter     *chaindb.BeaconCommitteeFilter
		committees string
		err        string
	}{
		{
			name: "SingleSlot",
			filter: &chaindb.BeaconCommitteeFilter{
				From: slotPtr(2000000001),
				To:   slotPtr(2000000001),
			},
			committees: `[{"Slot":2000000001,"Index":1,"Committee":[1,2,3]},{"Slot":2000000001,"Index":2,"Committee":[4,5,6]}]`,
		},
		{
			name: "SingleSlotAndIndex",
			filter: &chaindb.BeaconCommitteeFilter{
				From:             slotPtr(2000000001),
				To:               slotPtr(2000000001),
				CommitteeIndices: []phase0.CommitteeIndex{1},
			},
			committees: `[{"Slot":2000000001,"Index":1,"Committee":[1,2,3]}]`,
		},
		{
			name: "MultipleSlotsSingleIndex",
			filter: &chaindb.BeaconCommitteeFilter{
				From:             slotPtr(2000000001),
				To:               slotPtr(2000000002),
				CommitteeIndices: []phase0.CommitteeIndex{1},
			},
			committees: `[{"Slot":2000000001,"Index":1,"Committee":[1,2,3]},{"Slot":2000000002,"Index":1,"Committee":[7,8,9]}]`,
		},
		{
			name: "All",
			filter: &chaindb.BeaconCommitteeFilter{
				From: slotPtr(2000000001),
				To:   slotPtr(2000000003),
			},
			committees: `[{"Slot":2000000001,"Index":1,"Committee":[1,2,3]},{"Slot":2000000001,"Index":2,"Committee":[4,5,6]},{"Slot":2000000002,"Index":1,"Committee":[7,8,9]}]`,
		},
		{
			name: "Limit2",
			filter: &chaindb.BeaconCommitteeFilter{
				From:  slotPtr(2000000001),
				To:    slotPtr(2000000003),
				Limit: 2,
			},
			committees: `[{"Slot":2000000001,"Index":1,"Committee":[1,2,3]},{"Slot":2000000001,"Index":2,"Committee":[4,5,6]}]`,
		},
		{
			name: "ReverseLimit2",
			filter: &chaindb.BeaconCommitteeFilter{
				From:  slotPtr(2000000001),
				To:    slotPtr(2000000003),
				Limit: 2,
				Order: chaindb.OrderLatest,
			},
			committees: `[{"Slot":2000000001,"Index":2,"Committee":[4,5,6]},{"Slot":2000000002,"Index":1,"Committee":[7,8,9]}]`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := s.BeaconCommittees(ctx, test.filter)
			if test.err != "" {
				require.NotNil(t, err)
				require.EqualError(t, err, test.err)
			} else {
				require.NoError(t, err)
				output, err := json.Marshal(res)
				require.NoError(t, err)
				require.Equal(t, test.committees, string(output))
			}
		})
	}
}
