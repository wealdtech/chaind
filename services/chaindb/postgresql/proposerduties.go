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

// SetProposerDuty sets a proposer duty.
func (s *Service) SetProposerDuty(ctx context.Context, proposerDuty *chaindb.ProposerDuty) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.Exec(ctx, `
      INSERT INTO t_proposer_duties(f_slot
                                   ,f_validator_index)
      VALUES($1,$2)
      ON CONFLICT (f_slot) DO
      UPDATE
      SET f_validator_index = excluded.f_validator_index
		 `,
		proposerDuty.Slot,
		proposerDuty.ValidatorIndex,
	)

	return err
}

// ProposerDutiesForSlotRange fetches all proposer duties for a slot range.
func (s *Service) ProposerDutiesForSlotRange(ctx context.Context,
	startSlot phase0.Slot,
	endSlot phase0.Slot,
) (
	[]*chaindb.ProposerDuty,
	error,
) {
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
      SELECT f_slot
            ,f_validator_index
      FROM t_proposer_duties
      WHERE f_slot >= $1
        AND f_slot < $2
      ORDER BY f_slot`,
		startSlot,
		endSlot,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	proposerDuties := make([]*chaindb.ProposerDuty, 0)

	for rows.Next() {
		proposerDuty := &chaindb.ProposerDuty{}
		err := rows.Scan(
			&proposerDuty.Slot,
			&proposerDuty.ValidatorIndex,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		proposerDuties = append(proposerDuties, proposerDuty)
	}

	return proposerDuties, nil
}

// ProposerDutiesForValidator provides all proposer duties for the given validator index.
func (s *Service) ProposerDutiesForValidator(ctx context.Context, proposer phase0.ValidatorIndex) ([]*chaindb.ProposerDuty, error) {
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
SELECT f_slot
FROM t_proposer_duties
WHERE f_validator_index = $1
ORDER BY f_slot
`,
		proposer,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	proposerDuties := make([]*chaindb.ProposerDuty, 0)

	for rows.Next() {
		proposerDuty := &chaindb.ProposerDuty{
			ValidatorIndex: proposer,
		}
		err := rows.Scan(
			&proposerDuty.Slot,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		proposerDuties = append(proposerDuties, proposerDuty)
	}

	return proposerDuties, nil
}
