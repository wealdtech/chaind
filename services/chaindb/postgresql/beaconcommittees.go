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

	"github.com/wealdtech/chaind/services/chaindb"
)

// SetBeaconComittee sets a beacon committee.
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
