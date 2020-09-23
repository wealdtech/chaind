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

// SetProposerSlashing sets a proposer slashing.
func (s *Service) SetProposerSlashing(ctx context.Context, proposerSlashing *chaindb.ProposerSlashing) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.Exec(ctx, `
      INSERT INTO t_proposer_slashings(f_inclusion_slot
                                      ,f_inclusion_block_root
                                      ,f_inclusion_index
                                      ,f_header_1_slot
                                      ,f_header_1_proposer_index
                                      ,f_header_1_parent_root
                                      ,f_header_1_state_root
                                      ,f_header_1_body_root
                                      ,f_header_1_signature
                                      ,f_header_2_slot
                                      ,f_header_2_proposer_index
                                      ,f_header_2_parent_root
                                      ,f_header_2_state_root
                                      ,f_header_2_body_root
                                      ,f_header_2_signature
      )
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
      ON CONFLICT (f_inclusion_slot,f_inclusion_block_root,f_inclusion_index) DO
      UPDATE
      SET f_header_1_slot = excluded.f_header_1_slot
         ,f_header_1_proposer_index = excluded.f_header_1_proposer_index
         ,f_header_1_parent_root = excluded.f_header_1_parent_root
         ,f_header_1_state_root = excluded.f_header_1_state_root
         ,f_header_1_body_root = excluded.f_header_1_body_root
         ,f_header_1_signature = excluded.f_header_1_signature
         ,f_header_2_slot = excluded.f_header_2_slot
         ,f_header_2_proposer_index = excluded.f_header_2_proposer_index
         ,f_header_2_parent_root = excluded.f_header_2_parent_root
         ,f_header_2_state_root = excluded.f_header_2_state_root
         ,f_header_2_body_root = excluded.f_header_2_body_root
         ,f_header_2_signature = excluded.f_header_2_signature
      `,
		proposerSlashing.InclusionSlot,
		proposerSlashing.InclusionBlockRoot,
		proposerSlashing.InclusionIndex,
		proposerSlashing.Header1Slot,
		proposerSlashing.Header1ProposerIndex,
		proposerSlashing.Header1ParentRoot,
		proposerSlashing.Header1StateRoot,
		proposerSlashing.Header1BodyRoot,
		proposerSlashing.Header1Signature,
		proposerSlashing.Header2Slot,
		proposerSlashing.Header2ProposerIndex,
		proposerSlashing.Header2ParentRoot,
		proposerSlashing.Header2StateRoot,
		proposerSlashing.Header2BodyRoot,
		proposerSlashing.Header2Signature,
	)

	return err
}
