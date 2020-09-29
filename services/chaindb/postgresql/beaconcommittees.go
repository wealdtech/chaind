// Copyright © 2020 Weald Technology Trading.
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

package postgresql

import (
	"context"

	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
)

// SetBeaconCommittee sets a beacon committee.
func (s *Service) SetBeaconCommittee(ctx context.Context, beaconCommittee *chaindb.BeaconCommittee) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.Exec(ctx, `
      INSERT INTO t_beacon_committees(f_slot
                                     ,f_index
                                     ,f_committee)
      VALUES($1,$2,$3)
      ON CONFLICT (f_slot,f_index) DO
      UPDATE
      SET f_committee = excluded.f_committee
		 `,
		beaconCommittee.Slot,
		beaconCommittee.Index,
		beaconCommittee.Committee,
	)

	return err
}

// GetBeaconCommitteeBySlotAndIndex fetches the beacon committee with the given slot and index.
func (s *Service) GetBeaconCommitteeBySlotAndIndex(ctx context.Context, slot uint64, index uint64) (*chaindb.BeaconCommittee, error) {
	tx := s.tx(ctx)
	if tx == nil {
		ctx, cancel, err := s.BeginTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer cancel()
	}

	committee := &chaindb.BeaconCommittee{}

	err := tx.QueryRow(ctx, `
      SELECT f_slot
            ,f_index
            ,f_committee
      FROM t_beacon_committees
      WHERE f_slot = $1
        AND f_index = $2`,
		slot,
		index,
	).Scan(
		&committee.Slot,
		&committee.Index,
		&committee.Committee,
	)
	if err != nil {
		return nil, err
	}
	return committee, nil
}
