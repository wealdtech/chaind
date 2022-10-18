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
                                      ,f_block_1_root
                                      ,f_header_1_slot
                                      ,f_header_1_proposer_index
                                      ,f_header_1_parent_root
                                      ,f_header_1_state_root
                                      ,f_header_1_body_root
                                      ,f_header_1_signature
                                      ,f_block_2_root
                                      ,f_header_2_slot
                                      ,f_header_2_proposer_index
                                      ,f_header_2_parent_root
                                      ,f_header_2_state_root
                                      ,f_header_2_body_root
                                      ,f_header_2_signature
      )
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
      ON CONFLICT (f_inclusion_slot,f_inclusion_block_root,f_inclusion_index) DO
      UPDATE
      SET f_block_1_root = excluded.f_block_1_root
         ,f_header_1_slot = excluded.f_header_1_slot
         ,f_header_1_proposer_index = excluded.f_header_1_proposer_index
         ,f_header_1_parent_root = excluded.f_header_1_parent_root
         ,f_header_1_state_root = excluded.f_header_1_state_root
         ,f_header_1_body_root = excluded.f_header_1_body_root
         ,f_header_1_signature = excluded.f_header_1_signature
         ,f_block_2_root = excluded.f_block_2_root
         ,f_header_2_slot = excluded.f_header_2_slot
         ,f_header_2_proposer_index = excluded.f_header_2_proposer_index
         ,f_header_2_parent_root = excluded.f_header_2_parent_root
         ,f_header_2_state_root = excluded.f_header_2_state_root
         ,f_header_2_body_root = excluded.f_header_2_body_root
         ,f_header_2_signature = excluded.f_header_2_signature
      `,
		proposerSlashing.InclusionSlot,
		proposerSlashing.InclusionBlockRoot[:],
		proposerSlashing.InclusionIndex,
		proposerSlashing.Block1Root[:],
		proposerSlashing.Header1Slot,
		proposerSlashing.Header1ProposerIndex,
		proposerSlashing.Header1ParentRoot[:],
		proposerSlashing.Header1StateRoot[:],
		proposerSlashing.Header1BodyRoot[:],
		proposerSlashing.Header1Signature[:],
		proposerSlashing.Block2Root[:],
		proposerSlashing.Header2Slot,
		proposerSlashing.Header2ProposerIndex,
		proposerSlashing.Header2ParentRoot[:],
		proposerSlashing.Header2StateRoot[:],
		proposerSlashing.Header2BodyRoot[:],
		proposerSlashing.Header2Signature[:],
	)

	return err
}

// ProposerSlashingsForSlotRange fetches all proposer slashings made for the given slot range.
// It will return slashings from blocks that are canonical or undefined, but not from non-canonical blocks.
func (s *Service) ProposerSlashingsForSlotRange(ctx context.Context, minSlot phase0.Slot, maxSlot phase0.Slot) ([]*chaindb.ProposerSlashing, error) {
	tx := s.tx(ctx)
	if tx == nil {
		ctx, err := s.beginROTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		defer s.commitROTx(ctx)
		tx = s.tx(ctx)
	}

	rows, err := tx.Query(ctx, `
      SELECT f_inclusion_slot
            ,f_inclusion_block_root
            ,f_inclusion_index
            ,f_block_1_root
            ,f_header_1_slot
            ,f_header_1_proposer_index
            ,f_header_1_parent_root
            ,f_header_1_state_root
            ,f_header_1_body_root
            ,f_header_1_signature
            ,f_block_2_root
            ,f_header_2_slot
            ,f_header_2_proposer_index
            ,f_header_2_parent_root
            ,f_header_2_state_root
            ,f_header_2_body_root
            ,f_header_2_signature
      FROM t_proposer_slashings
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

	proposerSlashings := make([]*chaindb.ProposerSlashing, 0)

	var inclusionBlockRoot []byte
	var block1Root []byte
	var header1ParentRoot []byte
	var header1StateRoot []byte
	var header1BodyRoot []byte
	var header1Signature []byte
	var block2Root []byte
	var header2ParentRoot []byte
	var header2StateRoot []byte
	var header2BodyRoot []byte
	var header2Signature []byte
	for rows.Next() {
		proposerSlashing := &chaindb.ProposerSlashing{}
		err := rows.Scan(
			&proposerSlashing.InclusionSlot,
			&inclusionBlockRoot,
			&proposerSlashing.InclusionIndex,
			&block1Root,
			&proposerSlashing.Header1Slot,
			&proposerSlashing.Header1ProposerIndex,
			&header1ParentRoot,
			&header1StateRoot,
			&header1BodyRoot,
			&header1Signature,
			&block2Root,
			&proposerSlashing.Header2Slot,
			&proposerSlashing.Header2ProposerIndex,
			&header2ParentRoot,
			&header2StateRoot,
			&header2BodyRoot,
			&header2Signature,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		copy(proposerSlashing.InclusionBlockRoot[:], inclusionBlockRoot)
		copy(proposerSlashing.Block1Root[:], block1Root)
		copy(proposerSlashing.Header1ParentRoot[:], header1ParentRoot)
		copy(proposerSlashing.Header1StateRoot[:], header1StateRoot)
		copy(proposerSlashing.Header1BodyRoot[:], header1BodyRoot)
		copy(proposerSlashing.Header1Signature[:], header1Signature)
		copy(proposerSlashing.Block2Root[:], block2Root)
		copy(proposerSlashing.Header2ParentRoot[:], header2ParentRoot)
		copy(proposerSlashing.Header2StateRoot[:], header2StateRoot)
		copy(proposerSlashing.Header2BodyRoot[:], header2BodyRoot)
		copy(proposerSlashing.Header2Signature[:], header2Signature)
		proposerSlashings = append(proposerSlashings, proposerSlashing)
	}

	return proposerSlashings, nil
}

// ProposerSlashingsForValidator fetches all proposer slashings made for the given validator.
// It will return slashings from blocks that are canonical or undefined, but not from non-canonical blocks.
func (s *Service) ProposerSlashingsForValidator(ctx context.Context, index phase0.ValidatorIndex) ([]*chaindb.ProposerSlashing, error) {
	tx := s.tx(ctx)
	if tx == nil {
		ctx, err := s.beginROTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		defer s.commitROTx(ctx)
		tx = s.tx(ctx)
	}

	rows, err := tx.Query(ctx, `
      SELECT f_inclusion_slot
            ,f_inclusion_block_root
            ,f_inclusion_index
            ,f_block_1_root
            ,f_header_1_slot
            ,f_header_1_proposer_index
            ,f_header_1_parent_root
            ,f_header_1_state_root
            ,f_header_1_body_root
            ,f_header_1_signature
            ,f_block_2_root
            ,f_header_2_slot
            ,f_header_2_proposer_index
            ,f_header_2_parent_root
            ,f_header_2_state_root
            ,f_header_2_body_root
            ,f_header_2_signature
      FROM t_proposer_slashings
      WHERE f_header_1_slot IN (SELECT f_slot FROM t_proposer_duties WHERE f_validator_index = $1)
      ORDER BY f_inclusion_slot
	          ,f_inclusion_index`,
		index,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	proposerSlashings := make([]*chaindb.ProposerSlashing, 0)

	var inclusionBlockRoot []byte
	var block1Root []byte
	var header1ParentRoot []byte
	var header1StateRoot []byte
	var header1BodyRoot []byte
	var header1Signature []byte
	var block2Root []byte
	var header2ParentRoot []byte
	var header2StateRoot []byte
	var header2BodyRoot []byte
	var header2Signature []byte
	for rows.Next() {
		proposerSlashing := &chaindb.ProposerSlashing{}
		err := rows.Scan(
			&proposerSlashing.InclusionSlot,
			&inclusionBlockRoot,
			&proposerSlashing.InclusionIndex,
			&block1Root,
			&proposerSlashing.Header1Slot,
			&proposerSlashing.Header1ProposerIndex,
			&header1ParentRoot,
			&header1StateRoot,
			&header1BodyRoot,
			&header1Signature,
			&block2Root,
			&proposerSlashing.Header2Slot,
			&proposerSlashing.Header2ProposerIndex,
			&header2ParentRoot,
			&header2StateRoot,
			&header2BodyRoot,
			&header2Signature,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		copy(proposerSlashing.InclusionBlockRoot[:], inclusionBlockRoot)
		copy(proposerSlashing.Block1Root[:], block1Root)
		copy(proposerSlashing.Header1ParentRoot[:], header1ParentRoot)
		copy(proposerSlashing.Header1StateRoot[:], header1StateRoot)
		copy(proposerSlashing.Header1BodyRoot[:], header1BodyRoot)
		copy(proposerSlashing.Header1Signature[:], header1Signature)
		copy(proposerSlashing.Block2Root[:], block2Root)
		copy(proposerSlashing.Header2ParentRoot[:], header2ParentRoot)
		copy(proposerSlashing.Header2StateRoot[:], header2StateRoot)
		copy(proposerSlashing.Header2BodyRoot[:], header2BodyRoot)
		copy(proposerSlashing.Header2Signature[:], header2Signature)
		proposerSlashings = append(proposerSlashings, proposerSlashing)
	}

	return proposerSlashings, nil
}
