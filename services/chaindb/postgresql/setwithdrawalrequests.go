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

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
	"go.opentelemetry.io/otel"
)

// SetWithdrawalRequests sets withdrawal requests.
func (s *Service) SetWithdrawalRequests(ctx context.Context, requests []*chaindb.WithdrawalRequest) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SetWithdrawalRequests")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	// Create a savepoint in case the copy fails.
	nestedTx, err := tx.Begin(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create nested transaction")
	}

	_, err = nestedTx.CopyFrom(ctx,
		pgx.Identifier{"t_block_withdrawal_requests"},
		[]string{
			"f_block_root",
			"f_slot",
			"f_index",
			"f_source_address",
			"f_validator_pubkey",
			"f_amount",
		},
		pgx.CopyFromSlice(len(requests), func(i int) ([]interface{}, error) {
			return []interface{}{
				requests[i].InclusionBlockRoot[:],
				requests[i].InclusionSlot,
				requests[i].InclusionIndex,
				requests[i].SourceAddress[:],
				requests[i].ValidatorPubkey[:],
				requests[i].Amount,
			}, nil
		}))
	if err != nil {
		if err := nestedTx.Rollback(ctx); err != nil {
			return errors.Wrap(err, "failed to roll back nested transaction")
		}

		log.Debug().Err(err).Msg("Failed to copy insert withdrawal requests; applying one at a time")
		for _, request := range requests {
			if err := s.SetWithdrawalRequest(ctx, request); err != nil {
				return err
			}
		}
	}

	return nil
}
