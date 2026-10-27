// Copyright © 2026 Weald Technology Trading.
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

package postgresql_test

import (
	"context"
	"os"
	"testing"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/chaind/services/chaindb"
	"github.com/wealdtech/chaind/services/chaindb/postgresql"
)

// TestRefetchKeepsCanonical ensures that re-setting blocks and attestations without
// canonical information, as a refetch does, keeps the existing canonical information.
func TestRefetchKeepsCanonical(t *testing.T) {
	ctx := context.Background()
	s, err := postgresql.New(ctx,
		postgresql.WithLogLevel(zerolog.Disabled),
		postgresql.WithConnectionURL(os.Getenv("CHAINDB_URL")),
	)
	require.NoError(t, err)

	ctx, cancel, err := s.BeginTx(ctx)
	require.NoError(t, err)
	defer cancel()

	trueVal := true
	falseVal := false
	newBlock := func(canonical *bool) *chaindb.Block {
		return &chaindb.Block{
			Slot:          9999999,
			Root:          phase0.Root{0xfe, 0x01},
			Graffiti:      make([]byte, 32),
			ETH1BlockHash: make([]byte, 32),
			Canonical:     canonical,
		}
	}

	require.NoError(t, s.SetBlock(ctx, newBlock(&trueVal)))
	require.NoError(t, s.SetBlock(ctx, newBlock(nil)))
	block, err := s.BlockByRoot(ctx, phase0.Root{0xfe, 0x01})
	require.NoError(t, err)
	require.NotNil(t, block.Canonical)
	require.True(t, *block.Canonical)

	// An explicit status, as set by the finalizer, still applies.
	require.NoError(t, s.SetBlock(ctx, newBlock(&falseVal)))
	block, err = s.BlockByRoot(ctx, phase0.Root{0xfe, 0x01})
	require.NoError(t, err)
	require.False(t, *block.Canonical)

	newAttestation := func(canonical *bool) *chaindb.Attestation {
		return &chaindb.Attestation{
			InclusionSlot:      9999999,
			InclusionBlockRoot: phase0.Root{0xfe, 0x01},
			Slot:               9999998,
			CommitteeIndices:   []phase0.CommitteeIndex{0},
			AggregationBits:    []byte{0x01},
			AggregationIndices: []phase0.ValidatorIndex{1},
			BeaconBlockRoot:    phase0.Root{0xfe, 0x02},
			Canonical:          canonical,
			TargetCorrect:      canonical,
			HeadCorrect:        canonical,
		}
	}

	require.NoError(t, s.SetAttestations(ctx, []*chaindb.Attestation{newAttestation(&trueVal)}))
	// The bulk set fails for existing attestations, but must leave the transaction usable for the fallback.
	require.Error(t, s.SetAttestations(ctx, []*chaindb.Attestation{newAttestation(nil)}))
	require.NoError(t, s.SetAttestation(ctx, newAttestation(nil)))
	attestations, err := s.AttestationsForBlock(ctx, phase0.Root{0xfe, 0x02})
	require.NoError(t, err)
	require.Len(t, attestations, 1)
	require.True(t, *attestations[0].Canonical)
	require.True(t, *attestations[0].TargetCorrect)
	require.True(t, *attestations[0].HeadCorrect)
}
