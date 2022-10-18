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
	"github.com/wealdtech/chaind/services/chaindb"
)

// SetBlockSummary sets a block summary.
func (s *Service) SetBlockSummary(ctx context.Context, summary *chaindb.BlockSummary) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.Exec(ctx, `
      INSERT INTO t_block_summaries(f_slot
                                   ,f_attestations_for_block
                                   ,f_duplicate_attestations_for_block
                                   ,f_votes_for_block
                                   ,f_parent_distance)
      VALUES($1,$2,$3,$4,$5)
      ON CONFLICT (f_slot) DO
      UPDATE
      SET f_attestations_for_block = excluded.f_attestations_for_block
         ,f_duplicate_attestations_for_block = excluded.f_duplicate_attestations_for_block
         ,f_votes_for_block = excluded.f_votes_for_block
         ,f_parent_distance = excluded.f_parent_distance
		 `,
		summary.Slot,
		summary.AttestationsForBlock,
		summary.DuplicateAttestationsForBlock,
		summary.VotesForBlock,
		summary.ParentDistance,
	)

	return err
}

// BlockSummaryForSlot obtains the summary of a block for a given slot.
func (s *Service) BlockSummaryForSlot(ctx context.Context, slot phase0.Slot) (*chaindb.BlockSummary, error) {
	tx := s.tx(ctx)
	if tx == nil {
		ctx, err := s.beginROTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		defer s.commitROTx(ctx)
		tx = s.tx(ctx)
	}

	summary := &chaindb.BlockSummary{
		Slot: slot,
	}
	err := tx.QueryRow(ctx, `
SELECT f_attestations_for_block
      ,f_duplicate_attestations_for_block
      ,f_votes_for_block
      ,f_parent_distance
FROM t_block_summaries
WHERE f_slot = $1
`,
		slot,
	).Scan(
		&summary.AttestationsForBlock,
		&summary.DuplicateAttestationsForBlock,
		&summary.VotesForBlock,
		&summary.ParentDistance,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to scan row")
	}

	return summary, nil
}
