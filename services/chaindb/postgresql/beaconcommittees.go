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
	"fmt"
	"sort"
	"strings"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// SetBeaconCommittee sets a beacon committee.
func (s *Service) SetBeaconCommittee(ctx context.Context, beaconCommittee *chaindb.BeaconCommittee) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SetBeaconCommittee")
	defer span.End()

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

// BeaconCommittees fetches the beacon committees matching the filter.
func (s *Service) BeaconCommittees(ctx context.Context,
	filter *chaindb.BeaconCommitteeFilter,
) (
	[]*chaindb.BeaconCommittee,
	error,
) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "BeaconCommittees")
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
	queryVals := make([]interface{}, 0)

	queryBuilder.WriteString(`
SELECT f_slot
      ,f_index
      ,f_committee
FROM t_beacon_committees`)

	wherestr := "WHERE"

	if filter.From != nil {
		queryVals = append(queryVals, *filter.From)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_slot >= $%d`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if filter.To != nil {
		queryVals = append(queryVals, *filter.To)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_slot <= $%d`, wherestr, len(queryVals)))
		wherestr = "  AND"
	}

	if len(filter.CommitteeIndices) > 0 {
		queryVals = append(queryVals, filter.CommitteeIndices)
		queryBuilder.WriteString(fmt.Sprintf(`
%s f_index = ANY($%d)`, wherestr, len(queryVals)))
	}

	switch filter.Order {
	case chaindb.OrderEarliest:
		queryBuilder.WriteString(`
ORDER BY f_slot, f_index`)
	case chaindb.OrderLatest:
		queryBuilder.WriteString(`
ORDER BY f_slot DESC,f_committee DESC`)
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
		return nil, errors.Wrap(err, "query failed")
	}
	defer rows.Close()
	span.AddEvent("Ran query")

	committees := make([]*chaindb.BeaconCommittee, 0)
	var committeeMembers []uint64
	for rows.Next() {
		committee := &chaindb.BeaconCommittee{}
		err := rows.Scan(
			&committee.Slot,
			&committee.Index,
			&committeeMembers,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		committee.Committee = make([]phase0.ValidatorIndex, len(committeeMembers))
		for i := range committeeMembers {
			committee.Committee[i] = phase0.ValidatorIndex(committeeMembers[i])
		}
		committees = append(committees, committee)
	}
	span.AddEvent("Compiled results", trace.WithAttributes(attribute.Int("entries", len(committees))))

	// Always return order of slot then committee index.
	sort.Slice(committees, func(i int, j int) bool {
		if committees[i].Slot != committees[j].Slot {
			return committees[i].Slot < committees[j].Slot
		}
		return committees[i].Index < committees[j].Index
	})
	return committees, nil
}

// BeaconCommitteeBySlotAndIndex fetches the beacon committee with the given slot and index.
func (s *Service) BeaconCommitteeBySlotAndIndex(ctx context.Context, slot phase0.Slot, index phase0.CommitteeIndex) (*chaindb.BeaconCommittee, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "BeaconCommitteeBySlotAndIndex")
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

	committee := &chaindb.BeaconCommittee{}
	var committeeMembers []uint64

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
		&committeeMembers,
	)
	if err != nil {
		return nil, err
	}
	committee.Committee = make([]phase0.ValidatorIndex, len(committeeMembers))
	for i := range committeeMembers {
		committee.Committee[i] = phase0.ValidatorIndex(committeeMembers[i])
	}
	return committee, nil
}

// AttesterDuties fetches the attester duties at the given slot range for the given validator indices.
func (s *Service) AttesterDuties(ctx context.Context, startSlot phase0.Slot, endSlot phase0.Slot, validatorIndices []phase0.ValidatorIndex) ([]*chaindb.AttesterDuty, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "AttesterDuties")
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
      SELECT f_slot
            ,f_index
            ,f_committee
      FROM t_beacon_committees
      WHERE f_slot >= $1
        AND f_slot < $2
        AND $3 && f_committee
        ORDER BY f_slot, f_index`,
		startSlot,
		endSlot,
		validatorIndices,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := make([]*chaindb.AttesterDuty, 0)

	// Map for fast lookup of validator indices.
	validatorIndicesMap := make(map[phase0.ValidatorIndex]bool, len(validatorIndices))
	for _, validatorIndex := range validatorIndices {
		validatorIndicesMap[validatorIndex] = true
	}

	for rows.Next() {
		var slot uint64
		var index uint64
		var committee []uint64
		err := rows.Scan(
			&slot,
			&index,
			&committee,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}

		for i, validatorIndex := range committee {
			if validatorIndicesMap[phase0.ValidatorIndex(validatorIndex)] {
				res = append(res, &chaindb.AttesterDuty{
					Slot:           phase0.Slot(slot),
					Committee:      phase0.CommitteeIndex(index),
					ValidatorIndex: phase0.ValidatorIndex(validatorIndex),
					CommitteeIndex: uint64(i),
				})
			}
		}
	}

	return res, nil
}
