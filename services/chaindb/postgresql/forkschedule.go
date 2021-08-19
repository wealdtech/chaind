// Copyright © 2021 Weald Technology Limited.
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
)

// SetForkSchedule sets the fork schedule.
// This carries out a complete rewrite of the table.
func (s *Service) SetForkSchedule(ctx context.Context, schedule []*phase0.Fork) error {
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
      INSERT INTO t_fork_schedule(f_epoch
                                 ,f_version
						         )
      VALUES($1,$2)
	  `,
			fork.Epoch,
			fork.CurrentVersion[:],
		)
		if err != nil {
			return err
		}
	}

	return err
}

// ForkSchedule provides details of past and future changes in the chain's fork version.
func (s *Service) ForkSchedule(ctx context.Context) ([]*phase0.Fork, error) {
	tx := s.tx(ctx)
	if tx == nil {
		ctx, cancel, err := s.BeginTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer cancel()
	}

	schedule := make([]*phase0.Fork, 0)
	rows, err := tx.Query(ctx, `
      SELECT f_epoch
            ,f_version
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
		err := rows.Scan(
			&fork.Epoch,
			&version,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		copy(fork.CurrentVersion[:], version)
		if fork.Epoch == 0 {
			fork.PreviousVersion = fork.CurrentVersion
		} else {
			fork.PreviousVersion = schedule[len(schedule)-1].CurrentVersion
		}
		schedule = append(schedule, fork)
	}

	return schedule, nil
}
