// Copyright © 2021, 2023 Weald Technology Limited.
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
	"go.opentelemetry.io/otel"
)

// SetForkSchedule sets the fork schedule.
// This carries out a complete rewrite of the table.
func (s *Service) SetForkSchedule(ctx context.Context, schedule []*phase0.Fork) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SetForkSchedule")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.Exec(ctx, `
TRUNCATE TABLE t_fork_schedule
`)
	if err != nil {
		return err
	}

	for _, fork := range schedule {
		_, err := tx.Exec(ctx, `
INSERT INTO t_fork_schedule(f_version
                           ,f_epoch
                           ,f_previous_version
)
VALUES($1,$2,$3)
	  `,
			fork.CurrentVersion[:],
			fork.Epoch,
			fork.PreviousVersion[:],
		)
		if err != nil {
			return err
		}
	}

	return err
}

// ForkSchedule provides details of past and future changes in the chain's fork version.
func (s *Service) ForkSchedule(ctx context.Context) ([]*phase0.Fork, error) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "ForkSchedule")
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

	schedule := make([]*phase0.Fork, 0)
	rows, err := tx.Query(ctx, `
SELECT f_version
      ,f_epoch
      ,f_previous_version
FROM t_fork_schedule
ORDER BY f_epoch
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		fork := &phase0.Fork{}
		var version []byte
		var previousVersion []byte
		err := rows.Scan(
			&version,
			&fork.Epoch,
			&previousVersion,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		copy(fork.CurrentVersion[:], version)
		copy(fork.PreviousVersion[:], previousVersion)
		schedule = append(schedule, fork)
	}

	return schedule, nil
}
