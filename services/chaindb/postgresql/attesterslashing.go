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

// SetAttesterSlashing sets an attester slashing.
func (s *Service) SetAttesterSlashing(ctx context.Context, attesterSlashing *chaindb.AttesterSlashing) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.Exec(ctx, `
      INSERT INTO t_attester_slashings(f_inclusion_slot
                                      ,f_inclusion_block_root
                                      ,f_inclusion_index
                                      ,f_attestation_1_indices
                                      ,f_attestation_1_slot
                                      ,f_attestation_1_committee_index
                                      ,f_attestation_1_beacon_block_root
                                      ,f_attestation_1_source_epoch
                                      ,f_attestation_1_source_root
                                      ,f_attestation_1_target_epoch
                                      ,f_attestation_1_target_root
                                      ,f_attestation_1_signature
                                      ,f_attestation_2_indices
                                      ,f_attestation_2_slot
                                      ,f_attestation_2_committee_index
                                      ,f_attestation_2_beacon_block_root
                                      ,f_attestation_2_source_epoch
                                      ,f_attestation_2_source_root
                                      ,f_attestation_2_target_epoch
                                      ,f_attestation_2_target_root
                                      ,f_attestation_2_signature
      )
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)
      ON CONFLICT (f_inclusion_slot,f_inclusion_block_root,f_inclusion_index) DO
      UPDATE
      SET f_attestation_1_indices = excluded.f_attestation_1_indices
         ,f_attestation_1_slot = excluded.f_attestation_1_slot
         ,f_attestation_1_committee_index = excluded.f_attestation_1_committee_index
         ,f_attestation_1_beacon_block_root = excluded.f_attestation_1_beacon_block_root
         ,f_attestation_1_source_epoch = excluded.f_attestation_1_source_epoch
         ,f_attestation_1_source_root = excluded.f_attestation_1_source_root
         ,f_attestation_1_target_epoch = excluded.f_attestation_1_target_epoch
         ,f_attestation_1_target_root = excluded.f_attestation_1_target_root
         ,f_attestation_1_signature = excluded.f_attestation_1_signature
         ,f_attestation_2_indices = excluded.f_attestation_2_indices
         ,f_attestation_2_slot = excluded.f_attestation_2_slot
         ,f_attestation_2_committee_index = excluded.f_attestation_2_committee_index
         ,f_attestation_2_beacon_block_root = excluded.f_attestation_2_beacon_block_root
         ,f_attestation_2_source_epoch = excluded.f_attestation_2_source_epoch
         ,f_attestation_2_source_root = excluded.f_attestation_2_source_root
         ,f_attestation_2_target_epoch = excluded.f_attestation_2_target_epoch
         ,f_attestation_2_target_root = excluded.f_attestation_2_target_root
         ,f_attestation_2_signature = excluded.f_attestation_2_signature
      `,
		attesterSlashing.InclusionSlot,
		attesterSlashing.InclusionBlockRoot[:],
		attesterSlashing.InclusionIndex,
		attesterSlashing.Attestation1Indices,
		attesterSlashing.Attestation1Slot,
		attesterSlashing.Attestation1CommitteeIndex,
		attesterSlashing.Attestation1BeaconBlockRoot[:],
		attesterSlashing.Attestation1SourceEpoch,
		attesterSlashing.Attestation1SourceRoot[:],
		attesterSlashing.Attestation1TargetEpoch,
		attesterSlashing.Attestation1TargetRoot[:],
		attesterSlashing.Attestation1Signature[:],
		attesterSlashing.Attestation2Indices,
		attesterSlashing.Attestation2Slot,
		attesterSlashing.Attestation2CommitteeIndex,
		attesterSlashing.Attestation2BeaconBlockRoot[:],
		attesterSlashing.Attestation2SourceEpoch,
		attesterSlashing.Attestation2SourceRoot[:],
		attesterSlashing.Attestation2TargetEpoch,
		attesterSlashing.Attestation2TargetRoot[:],
		attesterSlashing.Attestation2Signature[:],
	)

	return err
}
