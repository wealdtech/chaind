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

// SetProposerDuty sets a proposer duty.
func (s *Service) SetProposerDuty(ctx context.Context, proposerDuty *chaindb.ProposerDuty) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.Exec(ctx, `
      INSERT INTO t_proposer_duties(f_slot
                                   ,f_validator_index)
      VALUES($1,$2)
      ON CONFLICT (f_slot) DO
      UPDATE
      SET f_validator_index = excluded.f_validator_index
		 `,
		proposerDuty.Slot,
		proposerDuty.ValidatorIndex,
	)

	return err
}
