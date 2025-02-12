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

// SetDepositRequest sets a deposit request.
func (s *Service) SetDepositRequest(ctx context.Context, request *chaindb.DepositRequest) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SetDepositRequest")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	if _, err := tx.Exec(ctx, `
INSERT INTO t_block_deposit_requests(f_block_root
                                    ,f_slot
                                    ,f_index
                                    ,f_pubkey
                                    ,f_withdrawal_credentials
                                    ,f_amount
                                    ,f_signature
                                    ,f_deposit_index
                                    )
VALUES($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT(f_block_root,f_index) DO
UPDATE
SET f_slot = excluded.f_slot
   ,f_pubkey = excluded.f_pubkey
   ,f_withdrawal_credentials = excluded.f_withdrawal_credentials
   ,f_amount = excluded.f_amount
   ,f_signature = excluded.f_signature
   ,f_index = excluded.f_index
`,
		request.InclusionBlockRoot[:],
		request.InclusionSlot,
		request.InclusionIndex,
		request.Pubkey[:],
		request.WithdrawalCredentials[:],
		request.Amount,
		request.Signature[:],
		request.Index,
	); err != nil {
		return err
	}
	return nil
}
