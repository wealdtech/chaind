// Copyright © 2025 Weald Technology Trading.
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

// SetWithdrawalRequest sets a withdrawal request.
func (s *Service) SetWithdrawalRequest(ctx context.Context, request *chaindb.WithdrawalRequest) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SetWithdrawalRequest")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	if _, err := tx.Exec(ctx, `
INSERT INTO t_block_withdrawal_requests(f_block_root
                                       ,f_slot
                                       ,f_index
                                       ,f_source_address
                                       ,f_validator_pubkey
                                       ,f_amount
                                       )
VALUES($1,$2,$3,$4,$5,$6)
ON CONFLICT(f_block_root,f_index) DO
UPDATE
SET f_slot = excluded.f_slot
   ,f_source_address = excluded.f_source_address
   ,f_validator_pubkey = excluded.f_validator_pubkey
   ,f_amount = excluded.f_amount
`,
		request.InclusionBlockRoot[:],
		request.InclusionSlot,
		request.InclusionIndex,
		request.SourceAddress[:],
		request.ValidatorPubkey[:],
		request.Amount,
	); err != nil {
		return err
	}
	return nil
}
