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
	"database/sql"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
	"go.opentelemetry.io/otel"
)

// SetAttestation sets an attestation.
func (s *Service) SetAttestation(ctx context.Context, attestation *chaindb.Attestation) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SetAttestations")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	var canonical sql.NullBool
	if attestation.Canonical != nil {
		canonical.Valid = true
		canonical.Bool = *attestation.Canonical
	}
	var targetCorrect sql.NullBool
	if attestation.TargetCorrect != nil {
		targetCorrect.Valid = true
		targetCorrect.Bool = *attestation.TargetCorrect
	}
	var headCorrect sql.NullBool
	if attestation.HeadCorrect != nil {
		headCorrect.Valid = true
		headCorrect.Bool = *attestation.HeadCorrect
	}
	_, err := tx.Exec(ctx, `
      INSERT INTO t_attestations(f_inclusion_slot
                                ,f_inclusion_block_root
                                ,f_inclusion_index
                                ,f_slot
                                ,f_committee_index
                                ,f_aggregation_bits
                                ,f_aggregation_indices
                                ,f_beacon_block_root
                                ,f_source_epoch
                                ,f_source_root
                                ,f_target_epoch
                                ,f_target_root
                                ,f_canonical
                                ,f_target_correct
                                ,f_head_correct
						  )
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
      ON CONFLICT (f_inclusion_slot,f_inclusion_block_root,f_inclusion_index) DO
      UPDATE
      SET f_slot = excluded.f_slot
         ,f_committee_index = excluded.f_committee_index
         ,f_aggregation_bits = excluded.f_aggregation_bits
         ,f_aggregation_indices = excluded.f_aggregation_indices
         ,f_beacon_block_root = excluded.f_beacon_block_root
         ,f_source_epoch = excluded.f_source_epoch
         ,f_source_root = excluded.f_source_root
         ,f_target_epoch = excluded.f_target_epoch
         ,f_target_root = excluded.f_target_root
         ,f_canonical = excluded.f_canonical
         ,f_target_correct = excluded.f_target_correct
         ,f_head_correct = excluded.f_head_correct
	  `,
		attestation.InclusionSlot,
		attestation.InclusionBlockRoot[:],
		attestation.InclusionIndex,
		attestation.Slot,
		attestation.CommitteeIndex,
		attestation.AggregationBits,
		attestation.AggregationIndices,
		attestation.BeaconBlockRoot[:],
		attestation.SourceEpoch,
		attestation.SourceRoot[:],
		attestation.TargetEpoch,
		attestation.TargetRoot[:],
		canonical,
		targetCorrect,
		headCorrect,
	)

	return err
}

// AttestationsForBlock fetches all attestations made for the given block.
func (s *Service) AttestationsForBlock(ctx context.Context, blockRoot phase0.Root) ([]*chaindb.Attestation, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "AttestationsForBlock")
	defer span.End()

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
            ,f_slot
            ,f_committee_index
            ,f_aggregation_bits
            ,f_aggregation_indices
            ,f_beacon_block_root
            ,f_source_epoch
            ,f_source_root
            ,f_target_epoch
            ,f_target_root
            ,f_canonical
            ,f_target_correct
            ,f_head_correct
      FROM t_attestations
      WHERE f_beacon_block_root = $1
      ORDER BY f_inclusion_slot
	          ,f_inclusion_index`,
		blockRoot[:],
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	attestations := make([]*chaindb.Attestation, 0)

	for rows.Next() {
		attestation := &chaindb.Attestation{}
		var inclusionBlockRoot []byte
		var aggregationIndices []uint64
		var beaconBlockRoot []byte
		var sourceRoot []byte
		var targetRoot []byte
		var canonical sql.NullBool
		var targetCorrect sql.NullBool
		var headCorrect sql.NullBool
		err := rows.Scan(
			&attestation.InclusionSlot,
			&inclusionBlockRoot,
			&attestation.InclusionIndex,
			&attestation.Slot,
			&attestation.CommitteeIndex,
			&attestation.AggregationBits,
			&aggregationIndices,
			&beaconBlockRoot,
			&attestation.SourceEpoch,
			&sourceRoot,
			&attestation.TargetEpoch,
			&targetRoot,
			&canonical,
			&targetCorrect,
			&headCorrect,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		copy(attestation.InclusionBlockRoot[:], inclusionBlockRoot)
		attestation.AggregationIndices = make([]phase0.ValidatorIndex, len(aggregationIndices))
		for i := range aggregationIndices {
			attestation.AggregationIndices[i] = phase0.ValidatorIndex(aggregationIndices[i])
		}
		copy(attestation.BeaconBlockRoot[:], beaconBlockRoot)
		copy(attestation.SourceRoot[:], sourceRoot)
		copy(attestation.TargetRoot[:], targetRoot)
		if canonical.Valid {
			val := canonical.Bool
			attestation.Canonical = &val
		}
		if targetCorrect.Valid {
			val := targetCorrect.Bool
			attestation.TargetCorrect = &val
		}
		if headCorrect.Valid {
			val := headCorrect.Bool
			attestation.HeadCorrect = &val
		}
		attestations = append(attestations, attestation)
	}

	return attestations, nil
}

// AttestationsInBlock fetches all attestations contained in the given block.
func (s *Service) AttestationsInBlock(ctx context.Context, blockRoot phase0.Root) ([]*chaindb.Attestation, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "AttestationsInBlock")
	defer span.End()

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
            ,f_slot
            ,f_committee_index
            ,f_aggregation_bits
            ,f_aggregation_indices
            ,f_beacon_block_root
            ,f_source_epoch
            ,f_source_root
            ,f_target_epoch
            ,f_target_root
            ,f_canonical
            ,f_target_correct
            ,f_head_correct
      FROM t_attestations
      WHERE f_inclusion_block_root = $1
      ORDER BY f_inclusion_slot
	          ,f_inclusion_index`,
		blockRoot[:],
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	attestations := make([]*chaindb.Attestation, 0)

	for rows.Next() {
		attestation := &chaindb.Attestation{}
		var inclusionBlockRoot []byte
		var aggregationIndices []uint64
		var beaconBlockRoot []byte
		var sourceRoot []byte
		var targetRoot []byte
		var canonical sql.NullBool
		var targetCorrect sql.NullBool
		var headCorrect sql.NullBool
		err := rows.Scan(
			&attestation.InclusionSlot,
			&inclusionBlockRoot,
			&attestation.InclusionIndex,
			&attestation.Slot,
			&attestation.CommitteeIndex,
			&attestation.AggregationBits,
			&aggregationIndices,
			&beaconBlockRoot,
			&attestation.SourceEpoch,
			&sourceRoot,
			&attestation.TargetEpoch,
			&targetRoot,
			&canonical,
			&targetCorrect,
			&headCorrect,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		copy(attestation.InclusionBlockRoot[:], inclusionBlockRoot)
		attestation.AggregationIndices = make([]phase0.ValidatorIndex, len(aggregationIndices))
		for i := range aggregationIndices {
			attestation.AggregationIndices[i] = phase0.ValidatorIndex(aggregationIndices[i])
		}
		copy(attestation.BeaconBlockRoot[:], beaconBlockRoot)
		copy(attestation.SourceRoot[:], sourceRoot)
		copy(attestation.TargetRoot[:], targetRoot)
		if canonical.Valid {
			val := canonical.Bool
			attestation.Canonical = &val
		}
		if targetCorrect.Valid {
			val := targetCorrect.Bool
			attestation.TargetCorrect = &val
		}
		if headCorrect.Valid {
			val := headCorrect.Bool
			attestation.HeadCorrect = &val
		}
		attestations = append(attestations, attestation)
	}

	return attestations, nil
}

// AttestationsForSlotRange fetches all attestations made for the given slot range.
// Ranges are inclusive of start and exclusive of end i.e. a request with startSlot 2 and endSlot 4 will provide
// attestations for slots 2 and 3.
func (s *Service) AttestationsForSlotRange(ctx context.Context, startSlot phase0.Slot, endSlot phase0.Slot) ([]*chaindb.Attestation, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "AttestationsForSlotRange")
	defer span.End()

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
            ,f_slot
            ,f_committee_index
            ,f_aggregation_bits
            ,f_aggregation_indices
            ,f_beacon_block_root
            ,f_source_epoch
            ,f_source_root
            ,f_target_epoch
            ,f_target_root
            ,f_canonical
            ,f_target_correct
            ,f_head_correct
      FROM t_attestations
      WHERE f_slot >= $1
        AND f_slot < $2
      ORDER BY f_inclusion_slot
	          ,f_inclusion_index`,
		startSlot,
		endSlot,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	attestations := make([]*chaindb.Attestation, 0)

	for rows.Next() {
		attestation := &chaindb.Attestation{}
		var inclusionBlockRoot []byte
		var aggregationIndices []uint64
		var beaconBlockRoot []byte
		var sourceRoot []byte
		var targetRoot []byte
		var canonical sql.NullBool
		var targetCorrect sql.NullBool
		var headCorrect sql.NullBool
		err := rows.Scan(
			&attestation.InclusionSlot,
			&inclusionBlockRoot,
			&attestation.InclusionIndex,
			&attestation.Slot,
			&attestation.CommitteeIndex,
			&attestation.AggregationBits,
			&aggregationIndices,
			&beaconBlockRoot,
			&attestation.SourceEpoch,
			&sourceRoot,
			&attestation.TargetEpoch,
			&targetRoot,
			&canonical,
			&targetCorrect,
			&headCorrect,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		copy(attestation.InclusionBlockRoot[:], inclusionBlockRoot)
		attestation.AggregationIndices = make([]phase0.ValidatorIndex, len(aggregationIndices))
		for i := range aggregationIndices {
			attestation.AggregationIndices[i] = phase0.ValidatorIndex(aggregationIndices[i])
		}
		copy(attestation.BeaconBlockRoot[:], beaconBlockRoot)
		copy(attestation.SourceRoot[:], sourceRoot)
		copy(attestation.TargetRoot[:], targetRoot)
		if canonical.Valid {
			val := canonical.Bool
			attestation.Canonical = &val
		}
		if targetCorrect.Valid {
			val := targetCorrect.Bool
			attestation.TargetCorrect = &val
		}
		if headCorrect.Valid {
			val := headCorrect.Bool
			attestation.HeadCorrect = &val
		}
		attestations = append(attestations, attestation)
	}

	return attestations, nil
}

// AttestationsInSlotRange fetches all attestations made in the given slot range.
// Ranges are inclusive of start and exclusive of end i.e. a request with startSlot 2 and endSlot 4 will provide
// attestations in slots 2 and 3.
func (s *Service) AttestationsInSlotRange(ctx context.Context, startSlot phase0.Slot, endSlot phase0.Slot) ([]*chaindb.Attestation, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "AttestationsInSlotRange")
	defer span.End()

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
            ,f_slot
            ,f_committee_index
            ,f_aggregation_bits
            ,f_aggregation_indices
            ,f_beacon_block_root
            ,f_source_epoch
            ,f_source_root
            ,f_target_epoch
            ,f_target_root
            ,f_canonical
            ,f_target_correct
            ,f_head_correct
      FROM t_attestations
      WHERE f_inclusion_slot >= $1
        AND f_inclusion_slot < $2
      ORDER BY f_inclusion_slot
	          ,f_inclusion_index`,
		startSlot,
		endSlot,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	attestations := make([]*chaindb.Attestation, 0)

	for rows.Next() {
		attestation := &chaindb.Attestation{}
		var inclusionBlockRoot []byte
		var aggregationIndices []uint64
		var beaconBlockRoot []byte
		var sourceRoot []byte
		var targetRoot []byte
		var canonical sql.NullBool
		var targetCorrect sql.NullBool
		var headCorrect sql.NullBool
		err := rows.Scan(
			&attestation.InclusionSlot,
			&inclusionBlockRoot,
			&attestation.InclusionIndex,
			&attestation.Slot,
			&attestation.CommitteeIndex,
			&attestation.AggregationBits,
			&aggregationIndices,
			&beaconBlockRoot,
			&attestation.SourceEpoch,
			&sourceRoot,
			&attestation.TargetEpoch,
			&targetRoot,
			&canonical,
			&targetCorrect,
			&headCorrect,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		copy(attestation.InclusionBlockRoot[:], inclusionBlockRoot)
		attestation.AggregationIndices = make([]phase0.ValidatorIndex, len(aggregationIndices))
		for i := range aggregationIndices {
			attestation.AggregationIndices[i] = phase0.ValidatorIndex(aggregationIndices[i])
		}
		copy(attestation.BeaconBlockRoot[:], beaconBlockRoot)
		copy(attestation.SourceRoot[:], sourceRoot)
		copy(attestation.TargetRoot[:], targetRoot)
		if canonical.Valid {
			val := canonical.Bool
			attestation.Canonical = &val
		}
		if targetCorrect.Valid {
			val := targetCorrect.Bool
			attestation.TargetCorrect = &val
		}
		if headCorrect.Valid {
			val := headCorrect.Bool
			attestation.HeadCorrect = &val
		}
		attestations = append(attestations, attestation)
	}

	return attestations, nil
}

// IndeterminateAttestationSlots fetches the slots in the given range with attestations that do not have a canonical status.
func (s *Service) IndeterminateAttestationSlots(ctx context.Context, minSlot phase0.Slot, maxSlot phase0.Slot) ([]phase0.Slot, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "IndeterminateAttestationSlots")
	defer span.End()

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
      SELECT DISTINCT f_slot
      FROM t_attestations
      WHERE f_slot >= $1
        AND f_slot < $2
        AND f_canonical IS NULL
      ORDER BY f_slot`,
		minSlot,
		maxSlot,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	slots := make([]phase0.Slot, 0)

	for rows.Next() {
		var slot phase0.Slot
		err := rows.Scan(&slot)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		slots = append(slots, slot)
	}

	return slots, nil
}
