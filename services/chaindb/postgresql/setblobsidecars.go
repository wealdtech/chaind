// Copyright © 2023 Weald Technology Trading.
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
	"bytes"
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
	"go.opentelemetry.io/otel"
)

// SetBlobSidecars sets blob sidecars.
func (s *Service) SetBlobSidecars(ctx context.Context, blobSidecars []*chaindb.BlobSidecar) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SetBlobSidecars")
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
		pgx.Identifier{"t_blob_sidecars"},
		[]string{
			"f_block_root",
			"f_index",
			"f_slot",
			"f_block_parent_root",
			"f_proposer_index",
			"f_blob",
			"f_kzg_commitment",
			"f_kzg_proof",
		},
		pgx.CopyFromSlice(len(blobSidecars), func(i int) ([]interface{}, error) {
			var blob *[]byte
			if len(blobSidecars[i].Blob) > 0 {
				blobBytes := blobSidecars[i].Blob[:]
				// Trim trailing 0s.
				blobBytes = bytes.TrimRight(blobBytes, string([]byte{0x00}))
				blob = &blobBytes
			}
			return []interface{}{
				blobSidecars[i].BlockRoot[:],
				blobSidecars[i].Index,
				blobSidecars[i].Slot,
				blobSidecars[i].BlockParentRoot[:],
				blobSidecars[i].ProposerIndex,
				blob,
				blobSidecars[i].KZGCommitment[:],
				blobSidecars[i].KZGProof[:],
			}, nil
		}))

	if err != nil {
		if err := nestedTx.Rollback(ctx); err != nil {
			return errors.Wrap(err, "failed to roll back nested transaction")
		}

		log.Debug().Err(err).Msg("Failed to copy insert blob sidecars; applying one at a time")
		for _, blobSidecar := range blobSidecars {
			if err := s.SetBlobSidecar(ctx, blobSidecar); err != nil {
				return err
			}
		}
	}

	return nil
}
