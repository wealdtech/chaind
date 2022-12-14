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

	"github.com/wealdtech/chaind/services/chaindb"
	"go.opentelemetry.io/otel"
)

// SetSyncAggregate sets the sync aggregate.
func (s *Service) SetSyncAggregate(ctx context.Context, syncAggregate *chaindb.SyncAggregate) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SetSyncAggregate")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.Exec(ctx, `
      INSERT INTO t_sync_aggregates(f_inclusion_slot
                                   ,f_inclusion_block_root
                                   ,f_bits
                                   ,f_indices
                                  )
      VALUES($1,$2,$3,$4)
      ON CONFLICT (f_inclusion_slot, f_inclusion_block_root) DO
      UPDATE
      SET f_bits = excluded.f_bits
         ,f_indices = excluded.f_indices
	  `,
		syncAggregate.InclusionSlot,
		syncAggregate.InclusionBlockRoot[:],
		syncAggregate.Bits,
		syncAggregate.Indices,
	)

	return err
}
