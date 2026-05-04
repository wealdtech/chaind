// Copyright © 2021 - 2026 Weald Technology Limited.
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

package standard

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/chaind/services/chaindb"
)

// stubValidatorsProvider is a hand-built chaindb.ValidatorsProvider used to
// drive summarizeEpoch into the silent-skip branch.  Only Validators and
// ValidatorBalancesByEpoch are exercised; the remaining methods satisfy the
// interface but are not invoked along the tested path.
type stubValidatorsProvider struct {
	validators []*chaindb.Validator
	balances   []*chaindb.ValidatorBalance
}

func (s *stubValidatorsProvider) Validators(_ context.Context) ([]*chaindb.Validator, error) {
	return s.validators, nil
}

func (s *stubValidatorsProvider) ValidatorsByPublicKey(_ context.Context, _ []phase0.BLSPubKey) (map[phase0.BLSPubKey]*chaindb.Validator, error) {
	return nil, nil
}

func (s *stubValidatorsProvider) ValidatorsByIndex(_ context.Context, _ []phase0.ValidatorIndex) (map[phase0.ValidatorIndex]*chaindb.Validator, error) {
	return nil, nil
}

func (s *stubValidatorsProvider) ValidatorBalancesByEpoch(_ context.Context, _ phase0.Epoch) ([]*chaindb.ValidatorBalance, error) {
	return s.balances, nil
}

func (s *stubValidatorsProvider) ValidatorBalancesByIndexAndEpoch(_ context.Context, _ []phase0.ValidatorIndex, _ phase0.Epoch) (map[phase0.ValidatorIndex]*chaindb.ValidatorBalance, error) {
	return nil, nil
}

func (s *stubValidatorsProvider) ValidatorBalancesByIndexAndEpochRange(_ context.Context, _ []phase0.ValidatorIndex, _ phase0.Epoch, _ phase0.Epoch) (map[phase0.ValidatorIndex][]*chaindb.ValidatorBalance, error) {
	return nil, nil
}

func (s *stubValidatorsProvider) ValidatorBalancesByIndexAndEpochs(_ context.Context, _ []phase0.ValidatorIndex, _ []phase0.Epoch) (map[phase0.ValidatorIndex][]*chaindb.ValidatorBalance, error) {
	return nil, nil
}

// TestSummarizeEpochSilentSkipEmitsWarn drives summarizeEpoch with one active
// validator and an empty balances slice, asserting the silent-skip path returns
// (false, nil) and emits a Warn log with the agreed stable text.
func TestSummarizeEpochSilentSkipEmitsWarn(t *testing.T) {
	ctx := context.Background()

	// main_test.go sets the global level to Disabled; locally raise it so
	// the buffer logger below actually emits.
	originalGlobalLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.TraceLevel)
	defer zerolog.SetGlobalLevel(originalGlobalLevel)

	var buf bytes.Buffer
	originalLog := log
	log = zerolog.New(&buf).Level(zerolog.WarnLevel)
	defer func() { log = originalLog }()

	const epoch = phase0.Epoch(5)
	farFuture := phase0.Epoch(0xffffffffffffffff)

	s := &Service{
		farFutureEpoch: farFuture,
		epochSummaries: true,
		validatorsProvider: &stubValidatorsProvider{
			validators: []*chaindb.Validator{
				{
					Index:           1,
					ActivationEpoch: 0,
					ExitEpoch:       farFuture,
				},
			},
			balances: nil,
		},
	}

	updated, err := s.summarizeEpoch(ctx, &metadata{}, epoch)
	require.NoError(t, err)
	require.False(t, updated)

	logged := buf.String()
	require.True(t,
		strings.Contains(logged, "No validator balances available; cannot summarize epoch"),
		"warn log missing expected stable message text; got: %s", logged)
	require.True(t,
		strings.Contains(logged, `"level":"warn"`),
		"silent-skip log emitted at unexpected level; got: %s", logged)
}

// TestSetLagGaugesWiring drives setLagGauges with synthetic metadata + finalized
// epoch and asserts per-pipeline values, that disabled pipelines produce no
// series, and that values are non-negative for the typical lag scenario.
func TestSetLagGaugesWiring(t *testing.T) {
	originalGauge := summarizerLagEpochs
	summarizerLagEpochs = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "lag_epochs",
		Help:      "test",
	}, []string{"pipeline"})
	defer func() { summarizerLagEpochs = originalGauge }()

	tests := []struct {
		name           string
		service        *Service
		md             *metadata
		finalizedEpoch phase0.Epoch
		wantSeries     int
		wantValues     map[string]float64
	}{
		{
			name: "all pipelines enabled",
			service: &Service{
				epochSummaries:     true,
				blockSummaries:     true,
				validatorSummaries: true,
			},
			md: &metadata{
				LastEpoch:          7,
				LastBlockEpoch:     5,
				LastValidatorEpoch: 4,
			},
			finalizedEpoch: 11,
			wantSeries:     3,
			wantValues: map[string]float64{
				"epoch":     3, // targetEpoch=10, 10-7
				"block":     5, // 10-5
				"validator": 6, // 10-4
			},
		},
		{
			name: "block pipeline disabled",
			service: &Service{
				epochSummaries:     true,
				blockSummaries:     false,
				validatorSummaries: true,
			},
			md: &metadata{
				LastEpoch:          9,
				LastValidatorEpoch: 8,
			},
			finalizedEpoch: 11,
			wantSeries:     2,
			wantValues: map[string]float64{
				"epoch":     1,
				"validator": 2,
			},
		},
		{
			name: "only epoch pipeline",
			service: &Service{
				epochSummaries: true,
			},
			md:             &metadata{LastEpoch: 9},
			finalizedEpoch: 11,
			wantSeries:     1,
			wantValues:     map[string]float64{"epoch": 1},
		},
		{
			name:           "no pipelines enabled",
			service:        &Service{},
			md:             &metadata{},
			finalizedEpoch: 11,
			wantSeries:     0,
			wantValues:     map[string]float64{},
		},
		{
			name: "fully caught up - lag is zero not negative",
			service: &Service{
				epochSummaries:     true,
				blockSummaries:     true,
				validatorSummaries: true,
			},
			md: &metadata{
				LastEpoch:          10,
				LastBlockEpoch:     10,
				LastValidatorEpoch: 10,
			},
			finalizedEpoch: 11,
			wantSeries:     3,
			wantValues: map[string]float64{
				"epoch":     0,
				"block":     0,
				"validator": 0,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			summarizerLagEpochs.Reset()

			test.service.setLagGauges(test.md, test.finalizedEpoch)

			require.Equal(t, test.wantSeries, testutil.CollectAndCount(summarizerLagEpochs),
				"unexpected number of label series emitted")

			for label, want := range test.wantValues {
				got := testutil.ToFloat64(summarizerLagEpochs.WithLabelValues(label))
				require.Equal(t, want, got, "unexpected value for pipeline=%s", label)
				require.GreaterOrEqual(t, got, 0.0, "lag for pipeline=%s should be non-negative", label)
			}
		})
	}
}
