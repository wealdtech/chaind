// Copyright © 2021 Weald Technology Trading.
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
	"go.opentelemetry.io/otel"
)

// SetSyncCommittee sets a sync committee.
func (s *Service) SetSyncCommittee(ctx context.Context, syncCommittee *chaindb.SyncCommittee) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SetSyncCommittee")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.Exec(ctx, `
      INSERT INTO t_sync_committees(f_period
                                   ,f_committee)
      VALUES($1,$2)
      ON CONFLICT (f_period) DO
      UPDATE
      SET f_committee = excluded.f_committee
		 `,
		syncCommittee.Period,
		syncCommittee.Committee,
	)

	return err
}

// SyncCommittee provides a sync committee for the given sync committee period.
func (s *Service) SyncCommittee(ctx context.Context, period uint64) (*chaindb.SyncCommittee, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SyncCommittee")
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

	committee := &chaindb.SyncCommittee{}
	var committeeMembers []uint64

	err := tx.QueryRow(ctx, `
      SELECT f_period
            ,f_committee
      FROM t_sync_committees
      WHERE f_period = $1
`,
		period,
	).Scan(
		&committee.Period,
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
