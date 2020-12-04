// Copyright © 2020 Weald Technology Trading.
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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/chaind/services/chaindb"
	"github.com/wealdtech/chaind/services/chaindb/postgresql"
)

func TestService(t *testing.T) {
	tests := []struct {
		name          string
		connectionURL string
		err           string
	}{
		{
			name: "ConnectionURLMissing",
			err:  "problem with parameters: no connection URL specified",
		},
		{
			name:          "Good",
			connectionURL: os.Getenv("CHAINDB_DATABASE_URL"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			_, err := postgresql.New(ctx,
				postgresql.WithConnectionURL(test.connectionURL),
			)
			if test.err != "" {
				assert.EqualError(t, err, test.err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestInterfaces(t *testing.T) {
	ctx := context.Background()
	s, err := postgresql.New(ctx, postgresql.WithConnectionURL(os.Getenv("CHAINDB_DATABASE_URL")))
	require.NoError(t, err)

	require.Implements(t, (*chaindb.Service)(nil), s)
	require.Implements(t, (*chaindb.AttestationsProvider)(nil), s)
	require.Implements(t, (*chaindb.AttestationsSetter)(nil), s)
	require.Implements(t, (*chaindb.AttesterSlashingsSetter)(nil), s)
	require.Implements(t, (*chaindb.BeaconCommitteesProvider)(nil), s)
	require.Implements(t, (*chaindb.BeaconCommitteesSetter)(nil), s)
	require.Implements(t, (*chaindb.BlocksProvider)(nil), s)
	require.Implements(t, (*chaindb.BlocksSetter)(nil), s)
	require.Implements(t, (*chaindb.ProposerDutiesSetter)(nil), s)
	require.Implements(t, (*chaindb.ProposerSlashingsSetter)(nil), s)
	require.Implements(t, (*chaindb.ValidatorsProvider)(nil), s)
	require.Implements(t, (*chaindb.ValidatorsSetter)(nil), s)
	require.Implements(t, (*chaindb.VoluntaryExitsSetter)(nil), s)
}
