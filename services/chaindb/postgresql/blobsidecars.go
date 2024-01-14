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
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
	"go.opentelemetry.io/otel"
)

// BlobSidecars provides blob sidecars according to the filter.
func (s *Service) BlobSidecars(ctx context.Context,
	filter *chaindb.BlobSidecarFilter,
) (
	[]*chaindb.BlobSidecar,
	error,
) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.chaindb.postgresql").Start(ctx, "BlobSidecars")
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

	// Build the query.
	queryBuilder := strings.Builder{}
	queryVals := make([]interface{}, 0)

	queryBuilder.WriteString(`
SELECT f_block_root
      ,f_slot
      ,f_index
      ,f_blob
      ,f_kzg_commitment
      ,f_kzg_proof
      ,f_kzg_commitment_inclusion_proof
FROM t_blob_sidecars`)

	conditions := make([]string, 0)

	if filter.From != nil {
		queryVals = append(queryVals, *filter.From)
		conditions = append(conditions, fmt.Sprintf("f_slot >= $%d", len(queryVals)))
	}

	if filter.To != nil {
		queryVals = append(queryVals, *filter.To)
		queryBuilder.WriteString(fmt.Sprintf("f_slot <= $%d", len(queryVals)))
	}

	if len(filter.BlockRoots) > 0 {
		queryVals = append(queryVals, filter.BlockRoots)
		queryBuilder.WriteString(fmt.Sprintf("f_block_root = ANY($%d)", len(queryVals)))
	}

	if len(filter.Indices) > 0 {
		queryVals = append(queryVals, filter.Indices)
		queryBuilder.WriteString(fmt.Sprintf("f_index = ANY($%d)", len(queryVals)))
	}

	if len(conditions) > 0 {
		queryBuilder.WriteString("\nWHERE ")
		queryBuilder.WriteString(strings.Join(conditions, "\n  AND "))
	}

	switch filter.Order {
	case chaindb.OrderEarliest:
		queryBuilder.WriteString(`
ORDER BY f_slot, f_index`)
	case chaindb.OrderLatest:
		queryBuilder.WriteString(`
ORDER BY f_slot DESC,f_index DESC`)
	default:
		return nil, errors.New("no order specified")
	}

	if filter.Limit > 0 {
		queryVals = append(queryVals, filter.Limit)
		queryBuilder.WriteString(fmt.Sprintf(`
LIMIT $%d`, len(queryVals)))
	}

	if e := log.Trace(); e.Enabled() {
		params := make([]string, len(queryVals))
		for i := range queryVals {
			params[i] = fmt.Sprintf("%v", queryVals[i])
		}
		e.Str("query", strings.ReplaceAll(queryBuilder.String(), "\n", " ")).Strs("params", params).Msg("SQL query")
	}

	rows, err := tx.Query(ctx,
		queryBuilder.String(),
		queryVals...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	blobSidecars := make([]*chaindb.BlobSidecar, 0)
	for rows.Next() {
		blobSidecar := &chaindb.BlobSidecar{}
		var blockRoot []byte
		var blob []byte
		var kzgCommitment []byte
		var kzgProof []byte
		var kzgCommitmentInclusionProof [][]byte
		err := rows.Scan(
			&blockRoot,
			&blobSidecar.InclusionSlot,
			&blobSidecar.InclusionIndex,
			&blob,
			&kzgCommitment,
			&kzgProof,
			&kzgCommitmentInclusionProof,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}
		copy(blobSidecar.InclusionBlockRoot[:], blockRoot)
		copy(blobSidecar.Blob[:], blob)
		copy(blobSidecar.KZGCommitment[:], kzgCommitment)
		copy(blobSidecar.KZGProof[:], kzgProof)
		for i := range kzgCommitmentInclusionProof {
			copy(blobSidecar.KZGCommitmentInclusionProof[i][:], kzgCommitmentInclusionProof[i])
		}
		blobSidecars = append(blobSidecars, blobSidecar)
	}

	// Always return order of slot then index.
	sort.Slice(blobSidecars, func(i int, j int) bool {
		if blobSidecars[i].InclusionSlot != blobSidecars[j].InclusionSlot {
			return blobSidecars[i].InclusionSlot < blobSidecars[j].InclusionSlot
		}
		return blobSidecars[i].InclusionIndex < blobSidecars[j].InclusionIndex
	})

	return blobSidecars, nil
}
