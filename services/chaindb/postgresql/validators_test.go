// Copyright © 2021 Weald Technology Trading.
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

	spec "github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/chaind/services/chaindb"
	"github.com/wealdtech/chaind/services/chaindb/postgresql"
)

func TestSetValidators(t *testing.T) {
	ctx := context.Background()
	s, err := postgresql.New(ctx,
		postgresql.WithConnectionURL(os.Getenv("CHAINDB_DATABASE_URL")),
	)
	require.NoError(t, err)

	validator1 := &chaindb.Validator{
		PublicKey: spec.BLSPubKey{
			0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
			0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
			0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
		},
		Index:                      1,
		EffectiveBalance:           32000000000,
		Slashed:                    false,
		ActivationEligibilityEpoch: 1,
		ActivationEpoch:            2,
		ExitEpoch:                  3,
		WithdrawableEpoch:          4,
	}
	validator2 := &chaindb.Validator{
		PublicKey: spec.BLSPubKey{
			0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
			0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
			0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
		},
		Index:                      2,
		EffectiveBalance:           0,
		Slashed:                    false,
		ActivationEligibilityEpoch: 0xffffffffffffffff,
		ActivationEpoch:            0xffffffffffffffff,
		ExitEpoch:                  0xffffffffffffffff,
		WithdrawableEpoch:          0xffffffffffffffff,
	}

	// Attempt to set the validator without a transaction; should fail.
	require.Error(t, s.SetValidator(ctx, validator1))

	ctx, cancel, err := s.BeginTx(ctx)
	require.NoError(t, err)
	defer cancel()

	// Set the validator.
	require.NoError(t, s.SetValidator(ctx, validator1))
	// Update the validator.
	require.NoError(t, s.SetValidator(ctx, validator1))

	// Ensure the correct data is returned.
	res, err := s.ValidatorsByPublicKey(ctx, []spec.BLSPubKey{validator1.PublicKey})
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, validator1, res[validator1.PublicKey])

	// Ensure that validator with far future epoch values is stored and returned.
	require.NoError(t, s.SetValidator(ctx, validator2))
	res, err = s.ValidatorsByPublicKey(ctx, []spec.BLSPubKey{validator2.PublicKey})
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, validator2, res[validator2.PublicKey])
}

func TestValidators(t *testing.T) {
	ctx := context.Background()
	s, err := postgresql.New(ctx,
		postgresql.WithConnectionURL(os.Getenv("CHAINDB_DATABASE_URL")),
	)
	require.NoError(t, err)

	validators, err := s.Validators(ctx)
	require.NoError(t, err)
	require.True(t, len(validators) > 0)
}
