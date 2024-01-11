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

	"github.com/wealdtech/chaind/services/chaindb"
	"go.opentelemetry.io/otel"
)

// SetBlobSidecar sets a blob sidecar.
func (s *Service) SetBlobSidecar(ctx context.Context, blobSidecar *chaindb.BlobSidecar) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "SetBlobSidecar")
	defer span.End()

	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	var blob *[]byte
	if len(blobSidecar.Blob) > 0 {
		blobBytes := blobSidecar.Blob[:]
		// Trim trailing 0s.
		blobBytes = bytes.TrimRight(blobBytes, string([]byte{0x00}))
		blob = &blobBytes
	}

	if _, err := tx.Exec(ctx, `
INSERT INTO t_blob_sidecars(f_block_root
                           ,f_index
                           ,f_slot
                           ,f_block_parent_root
                           ,f_proposer_index
                           ,f_blob
                           ,f_kzg_commitment
                           ,f_kzg_proof
						   )
VALUES($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT(f_block_root,f_index) DO
UPDATE
SET f_slot = excluded.f_slot
   ,f_block_parent_root = excluded.f_block_parent_root
   ,f_proposer_index = excluded.f_proposer_index
   ,f_blob = excluded.f_blob
   ,f_kzg_commitment = excluded.f_kzg_commitment
   ,f_kzg_proof = excluded.f_kzg_proof
`,
		blobSidecar.BlockRoot[:],
		blobSidecar.Index,
		blobSidecar.Slot,
		blobSidecar.BlockParentRoot[:],
		blobSidecar.ProposerIndex,
		blob,
		blobSidecar.KZGCommitment[:],
		blobSidecar.KZGProof[:],
	); err != nil {
		return err
	}
	return nil
}
