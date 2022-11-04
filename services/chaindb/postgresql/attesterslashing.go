// Copyright © 2020, 2021 Weald Technology Trading.
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

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
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

// AttesterSlashingsForSlotRange fetches all attester slashings made for the given slot range.
// It will return slashings from blocks that are canonical or undefined, but not from non-canonical blocks.
func (s *Service) AttesterSlashingsForSlotRange(ctx context.Context, minSlot phase0.Slot, maxSlot phase0.Slot) ([]*chaindb.AttesterSlashing, error) {
	tx := s.tx(ctx)
	if tx == nil {
		ctx, err := s.BeginROTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		defer s.CommitROTx(ctx)
		tx = s.tx(ctx)
	}

	rows, err := tx.Query(ctx, `
      SELECT f_inclusion_slot
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
      FROM t_attester_slashings
      WHERE f_inclusion_slot >= $1
        AND f_inclusion_slot < $2
		AND f_inclusion_slot IN (SELECT f_slot FROM t_blocks WHERE f_slot >= $1 AND f_slot < $2 AND (f_canonical IS NULL OR f_canonical = true))
      ORDER BY f_inclusion_slot
	          ,f_inclusion_index`,
		minSlot,
		maxSlot,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	attesterSlashings := make([]*chaindb.AttesterSlashing, 0)

	var inclusionBlockRoot []byte
	var attestation1Indices []uint64
	var attestation1BeaconBlockRoot []byte
	var attestation1SourceRoot []byte
	var attestation1TargetRoot []byte
	var attestation1Signature []byte
	var attestation2Indices []uint64
	var attestation2BeaconBlockRoot []byte
	var attestation2SourceRoot []byte
	var attestation2TargetRoot []byte
	var attestation2Signature []byte
	for rows.Next() {
		attesterSlashing := &chaindb.AttesterSlashing{}
		err := rows.Scan(
			&attesterSlashing.InclusionSlot,
			&inclusionBlockRoot,
			&attesterSlashing.InclusionIndex,
			&attestation1Indices,
			&attesterSlashing.Attestation1Slot,
			&attesterSlashing.Attestation1CommitteeIndex,
			&attestation1BeaconBlockRoot,
			&attesterSlashing.Attestation1SourceEpoch,
			&attestation1SourceRoot,
			&attesterSlashing.Attestation1TargetEpoch,
			&attestation1TargetRoot,
			&attestation1Signature,
			&attestation2Indices,
			&attesterSlashing.Attestation2Slot,
			&attesterSlashing.Attestation2CommitteeIndex,
			&attestation2BeaconBlockRoot,
			&attesterSlashing.Attestation2SourceEpoch,
			&attestation2SourceRoot,
			&attesterSlashing.Attestation2TargetEpoch,
			&attestation2TargetRoot,
			&attestation2Signature,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		copy(attesterSlashing.InclusionBlockRoot[:], inclusionBlockRoot)
		attesterSlashing.Attestation1Indices = make([]phase0.ValidatorIndex, len(attestation1Indices))
		for i := range attestation1Indices {
			attesterSlashing.Attestation1Indices[i] = phase0.ValidatorIndex(attestation1Indices[i])
		}
		copy(attesterSlashing.Attestation1BeaconBlockRoot[:], attestation1BeaconBlockRoot)
		copy(attesterSlashing.Attestation1SourceRoot[:], attestation1SourceRoot)
		copy(attesterSlashing.Attestation1TargetRoot[:], attestation1TargetRoot)
		copy(attesterSlashing.Attestation1Signature[:], attestation1Signature)
		attesterSlashing.Attestation2Indices = make([]phase0.ValidatorIndex, len(attestation2Indices))
		for i := range attestation2Indices {
			attesterSlashing.Attestation2Indices[i] = phase0.ValidatorIndex(attestation2Indices[i])
		}
		copy(attesterSlashing.Attestation2BeaconBlockRoot[:], attestation2BeaconBlockRoot)
		copy(attesterSlashing.Attestation2SourceRoot[:], attestation2SourceRoot)
		copy(attesterSlashing.Attestation2TargetRoot[:], attestation2TargetRoot)
		copy(attesterSlashing.Attestation2Signature[:], attestation2Signature)
		attesterSlashings = append(attesterSlashings, attesterSlashing)
	}

	return attesterSlashings, nil
}

// AttesterSlashingsForValidator fetches all attester slashings made for the given validator.
// It will return slashings from blocks that are canonical or undefined, but not from non-canonical blocks.
func (s *Service) AttesterSlashingsForValidator(ctx context.Context, index phase0.ValidatorIndex) ([]*chaindb.AttesterSlashing, error) {
	tx := s.tx(ctx)
	if tx == nil {
		ctx, err := s.BeginROTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		defer s.CommitROTx(ctx)
		tx = s.tx(ctx)
	}

	rows, err := tx.Query(ctx, `
      SELECT f_inclusion_slot
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
      FROM t_attester_slashings
      WHERE $1 = ANY(f_attestation_1_indices)
        AND $1 = ANY(f_attestation_2_indices)
      ORDER BY f_inclusion_slot
	          ,f_inclusion_index`,
		index,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	attesterSlashings := make([]*chaindb.AttesterSlashing, 0)

	var inclusionBlockRoot []byte
	var attestation1Indices []uint64
	var attestation1BeaconBlockRoot []byte
	var attestation1SourceRoot []byte
	var attestation1TargetRoot []byte
	var attestation1Signature []byte
	var attestation2Indices []uint64
	var attestation2BeaconBlockRoot []byte
	var attestation2SourceRoot []byte
	var attestation2TargetRoot []byte
	var attestation2Signature []byte
	for rows.Next() {
		attesterSlashing := &chaindb.AttesterSlashing{}
		err := rows.Scan(
			&attesterSlashing.InclusionSlot,
			&inclusionBlockRoot,
			&attesterSlashing.InclusionIndex,
			&attestation1Indices,
			&attesterSlashing.Attestation1Slot,
			&attesterSlashing.Attestation1CommitteeIndex,
			&attestation1BeaconBlockRoot,
			&attesterSlashing.Attestation1SourceEpoch,
			&attestation1SourceRoot,
			&attesterSlashing.Attestation1TargetEpoch,
			&attestation1TargetRoot,
			&attestation1Signature,
			&attestation2Indices,
			&attesterSlashing.Attestation2Slot,
			&attesterSlashing.Attestation2CommitteeIndex,
			&attestation2BeaconBlockRoot,
			&attesterSlashing.Attestation2SourceEpoch,
			&attestation2SourceRoot,
			&attesterSlashing.Attestation2TargetEpoch,
			&attestation2TargetRoot,
			&attestation2Signature,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		copy(attesterSlashing.InclusionBlockRoot[:], inclusionBlockRoot)
		attesterSlashing.Attestation1Indices = make([]phase0.ValidatorIndex, len(attestation1Indices))
		for i := range attestation1Indices {
			attesterSlashing.Attestation1Indices[i] = phase0.ValidatorIndex(attestation1Indices[i])
		}
		copy(attesterSlashing.Attestation1BeaconBlockRoot[:], attestation1BeaconBlockRoot)
		copy(attesterSlashing.Attestation1SourceRoot[:], attestation1SourceRoot)
		copy(attesterSlashing.Attestation1TargetRoot[:], attestation1TargetRoot)
		copy(attesterSlashing.Attestation1Signature[:], attestation1Signature)
		attesterSlashing.Attestation2Indices = make([]phase0.ValidatorIndex, len(attestation2Indices))
		for i := range attestation2Indices {
			attesterSlashing.Attestation2Indices[i] = phase0.ValidatorIndex(attestation2Indices[i])
		}
		copy(attesterSlashing.Attestation2BeaconBlockRoot[:], attestation2BeaconBlockRoot)
		copy(attesterSlashing.Attestation2SourceRoot[:], attestation2SourceRoot)
		copy(attesterSlashing.Attestation2TargetRoot[:], attestation2TargetRoot)
		copy(attesterSlashing.Attestation2Signature[:], attestation2Signature)
		attesterSlashings = append(attesterSlashings, attesterSlashing)
	}

	return attesterSlashings, nil
}
