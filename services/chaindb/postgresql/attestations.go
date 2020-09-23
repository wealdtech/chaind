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

// SetAttestation sets an attestation.
func (s *Service) SetAttestation(ctx context.Context, attestation *chaindb.Attestation) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.Exec(ctx, `
      INSERT INTO t_attestations(f_inclusion_slot
                                ,f_inclusion_block_root
                                ,f_inclusion_index
                                ,f_slot
                                ,f_committee_index
                                ,f_aggregation_bits
                                ,f_beacon_block_root
                                ,f_source_epoch
                                ,f_source_root
                                ,f_target_epoch
                                ,f_target_root
						  )
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
      ON CONFLICT (f_inclusion_slot,f_inclusion_block_root,f_inclusion_index) DO
      UPDATE
      SET f_slot = excluded.f_slot
         ,f_committee_index = excluded.f_committee_index
         ,f_aggregation_bits = excluded.f_aggregation_bits
         ,f_beacon_block_root = excluded.f_beacon_block_root
         ,f_source_epoch = excluded.f_source_epoch
         ,f_source_root = excluded.f_source_root
         ,f_target_epoch = excluded.f_target_epoch
         ,f_target_root = excluded.f_target_root
	  `,
		attestation.InclusionSlot,
		attestation.InclusionBlockRoot,
		attestation.InclusionIndex,
		attestation.Slot,
		attestation.CommitteeIndex,
		attestation.AggregationBits,
		attestation.BeaconBlockRoot,
		attestation.SourceEpoch,
		attestation.SourceRoot,
		attestation.TargetEpoch,
		attestation.TargetRoot,
	)

	return err
}
