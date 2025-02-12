// Copyright © 2020 - 2025 Weald Technology Trading.
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
	"fmt"
	"sort"
	"strings"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
	"go.opentelemetry.io/otel"
)

// SetAttestation sets an attestation.
func (s *Service) SetAttestation(ctx context.Context, attestation *chaindb.Attestation) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SetAttestation")
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
                                ,f_committee_indices
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
         ,f_committee_indices = excluded.f_committee_indices
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
		attestation.CommitteeIndices,
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

// SetAttestations sets multiple attestations.
func (s *Service) SetAttestations(ctx context.Context, attestations []*chaindb.Attestation) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SetAttestations")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.CopyFrom(ctx,
		pgx.Identifier{"t_attestations"},
		[]string{
			"f_inclusion_slot",
			"f_inclusion_block_root",
			"f_inclusion_index",
			"f_slot",
			"f_committee_indices",
			"f_aggregation_bits",
			"f_aggregation_indices",
			"f_beacon_block_root",
			"f_source_epoch",
			"f_source_root",
			"f_target_epoch",
			"f_target_root",
			"f_canonical",
			"f_target_correct",
			"f_head_correct",
		},
		pgx.CopyFromSlice(len(attestations), func(i int) ([]any, error) {
			var canonical sql.NullBool
			if attestations[i].Canonical != nil {
				canonical.Valid = true
				canonical.Bool = *attestations[i].Canonical
			}
			var targetCorrect sql.NullBool
			if attestations[i].TargetCorrect != nil {
				targetCorrect.Valid = true
				targetCorrect.Bool = *attestations[i].TargetCorrect
			}
			var headCorrect sql.NullBool
			if attestations[i].HeadCorrect != nil {
				headCorrect.Valid = true
				headCorrect.Bool = *attestations[i].HeadCorrect
			}
			return []any{
				attestations[i].InclusionSlot,
				attestations[i].InclusionBlockRoot[:],
				attestations[i].InclusionIndex,
				attestations[i].Slot,
				attestations[i].CommitteeIndices,
				attestations[i].AggregationBits,
				attestations[i].AggregationIndices,
				attestations[i].BeaconBlockRoot[:],
				attestations[i].SourceEpoch,
				attestations[i].SourceRoot[:],
				attestations[i].TargetEpoch,
				attestations[i].TargetRoot[:],
				canonical,
				targetCorrect,
				headCorrect,
			}, nil
		}))
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
            ,f_committee_indices
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
			&attestation.CommitteeIndices,
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
            ,f_committee_indices
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
			&attestation.CommitteeIndices,
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
            ,f_committee_indices
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
			&attestation.CommitteeIndices,
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
            ,f_committee_indices
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
			&attestation.CommitteeIndices,
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

// Attestations provides attestations according to the filter.
//
//nolint:gocyclo,maintidx
func (s *Service) Attestations(ctx context.Context, filter *chaindb.AttestationFilter) ([]*chaindb.Attestation, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "Attestations")
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

	// Build the query.
	queryBuilder := strings.Builder{}
	queryVals := make([]any, 0)

	queryBuilder.WriteString(`
SELECT f_inclusion_slot
      ,f_inclusion_block_root
      ,f_inclusion_index
      ,f_slot
      ,f_committee_indices
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
FROM t_attestations`)

	conditions := make([]string, 0)

	if filter.ScheduledFrom != nil {
		queryVals = append(queryVals, *filter.ScheduledFrom)
		conditions = append(conditions, fmt.Sprintf("f_slot >= $%d", len(queryVals)))
	}

	if filter.ScheduledTo != nil {
		queryVals = append(queryVals, *filter.ScheduledTo)
		conditions = append(conditions, fmt.Sprintf("f_slot <= $%d", len(queryVals)))
	}

	if filter.From != nil {
		queryVals = append(queryVals, *filter.From)
		conditions = append(conditions, fmt.Sprintf("f_inclusion_slot >= $%d", len(queryVals)))
	}

	if filter.To != nil {
		queryVals = append(queryVals, *filter.To)
		conditions = append(conditions, fmt.Sprintf("f_inclusion_slot <= $%d", len(queryVals)))
	}

	if len(filter.ValidatorIndices) > 0 {
		queryVals = append(queryVals, filter.ValidatorIndices)
		conditions = append(conditions, fmt.Sprintf("f_aggregation_indices && $%d", len(queryVals)))
	}

	if filter.Canonical != nil {
		queryVals = append(queryVals, *filter.Canonical)
		conditions = append(conditions, fmt.Sprintf("f_canonical = $%d", len(queryVals)))
	}

	if filter.HeadCorrect != nil {
		queryVals = append(queryVals, *filter.HeadCorrect)
		conditions = append(conditions, fmt.Sprintf("f_head_correct = $%d", len(queryVals)))
	}

	if len(conditions) > 0 {
		queryBuilder.WriteString("\nWHERE ")
		queryBuilder.WriteString(strings.Join(conditions, "\n  AND "))
	}

	switch filter.Order {
	case chaindb.OrderEarliest:
		queryBuilder.WriteString(`
ORDER BY f_inclusion_slot, f_inclusion_index`)
	case chaindb.OrderLatest:
		queryBuilder.WriteString(`
ORDER BY f_inclusion_slot DESC,f_inclusion_index DESC`)
	default:
		return nil, errors.New("no order specified")
	}

	if filter.Limit > 0 {
		queryVals = append(queryVals, filter.Limit)
		queryBuilder.WriteString(fmt.Sprintf(`
LIMIT $%d`, len(queryVals)))
	}

	if e := log.Trace(); e.Enabled() {
		params := make([]string, len(queryVals))
		for i := range queryVals {
			params[i] = fmt.Sprintf("%v", queryVals[i])
		}
		e.Str("query", strings.ReplaceAll(queryBuilder.String(), "\n", " ")).Strs("params", params).Msg("SQL query")
	}

	rows, err := tx.Query(ctx,
		queryBuilder.String(),
		queryVals...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	attestations := make([]*chaindb.Attestation, 0)

	boolTrue := true
	boolFalse := false

	inclusionBlockRoot := make([]byte, phase0.RootLength)
	aggregationIndices := make([]uint64, 0)
	beaconBlockRoot := make([]byte, phase0.RootLength)
	sourceRoot := make([]byte, phase0.RootLength)
	targetRoot := make([]byte, phase0.RootLength)
	canonical := sql.NullBool{}
	targetCorrect := sql.NullBool{}
	headCorrect := sql.NullBool{}
	for rows.Next() {
		attestation := &chaindb.Attestation{}
		err := rows.Scan(
			&attestation.InclusionSlot,
			&inclusionBlockRoot,
			&attestation.InclusionIndex,
			&attestation.Slot,
			&attestation.CommitteeIndices,
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
		if canonical.Valid && canonical.Bool {
			attestation.Canonical = &boolTrue
		}
		if canonical.Valid && !canonical.Bool {
			attestation.Canonical = &boolFalse
		}
		if targetCorrect.Valid && targetCorrect.Bool {
			attestation.TargetCorrect = &boolTrue
		}
		if targetCorrect.Valid && !targetCorrect.Bool {
			attestation.TargetCorrect = &boolFalse
		}
		if headCorrect.Valid && headCorrect.Bool {
			attestation.HeadCorrect = &boolTrue
		}
		if headCorrect.Valid && !headCorrect.Bool {
			attestation.HeadCorrect = &boolFalse
		}
		attestations = append(attestations, attestation)
	}

	// Always return order of inclusion slot then inclusion index.
	sort.Slice(attestations, func(i int, j int) bool {
		if attestations[i].InclusionSlot != attestations[j].InclusionSlot {
			return attestations[i].InclusionSlot < attestations[j].InclusionSlot
		}
		return attestations[i].InclusionIndex < attestations[j].InclusionIndex
	})

	return attestations, nil
}
