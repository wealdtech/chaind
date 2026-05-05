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
// drive summarizeEpoch into the silent-skip and zero-balance branches.  Only
// Validators and ValidatorBalancesByEpoch are exercised; the remaining methods
// satisfy the interface but are not invoked along the tested paths.
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
// validator and an empty balances slice (the bootstrap path), asserting it
// returns (false, nil) and emits the alert-contract warn log.
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

// recordingChainDB is a chaindb.Service / EpochSummariesSetter that records
// every SetEpochSummary call so a test can assert that the corrupt-summary
// guard prevents writes.  No transaction enforcement — tests that drive the
// guard path exit before BeginTx is reached.
type recordingChainDB struct {
	summariesSet []*chaindb.EpochSummary
}

func (c *recordingChainDB) BeginTx(ctx context.Context) (context.Context, context.CancelFunc, error) {
	return ctx, func() {}, nil
}

func (c *recordingChainDB) CommitTx(_ context.Context) error {
	return nil
}

func (c *recordingChainDB) BeginROTx(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

func (c *recordingChainDB) CommitROTx(_ context.Context) {}

func (c *recordingChainDB) SetMetadata(_ context.Context, _ string, _ []byte) error {
	return nil
}

func (c *recordingChainDB) Metadata(_ context.Context, _ string) ([]byte, error) {
	return nil, nil
}

func (c *recordingChainDB) SetEpochSummary(_ context.Context, summary *chaindb.EpochSummary) error {
	c.summariesSet = append(c.summariesSet, summary)
	return nil
}

// TestSummarizeEpochZeroBalanceAssertion drives the production-state path:
// ValidatorBalancesByEpoch returns one row per validator with Balance=0 and
// EffectiveBalance=0 (mimicking the LEFT JOIN behavior when t_validator_balances
// is empty for the epoch).  Asserts the guard returns (false, nil), emits the
// alert-contract warn log, and writes nothing to t_epoch_summaries.
func TestSummarizeEpochZeroBalanceAssertion(t *testing.T) {
	ctx := context.Background()

	originalGlobalLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.TraceLevel)
	defer zerolog.SetGlobalLevel(originalGlobalLevel)

	var buf bytes.Buffer
	originalLog := log
	log = zerolog.New(&buf).Level(zerolog.WarnLevel)
	defer func() { log = originalLog }()

	const epoch = phase0.Epoch(5)
	const numValidators = 1000
	farFuture := phase0.Epoch(0xffffffffffffffff)

	validators := make([]*chaindb.Validator, numValidators)
	balances := make([]*chaindb.ValidatorBalance, numValidators)
	for i := 0; i < numValidators; i++ {
		idx := phase0.ValidatorIndex(i)
		validators[i] = &chaindb.Validator{
			Index:           idx,
			ActivationEpoch: 0,
			ExitEpoch:       farFuture,
		}
		balances[i] = &chaindb.ValidatorBalance{
			Index:            idx,
			Epoch:            epoch,
			Balance:          0,
			EffectiveBalance: 0,
		}
	}

	chainDB := &recordingChainDB{}
	s := &Service{
		chainDB:        chainDB,
		farFutureEpoch: farFuture,
		epochSummaries: true,
		validatorsProvider: &stubValidatorsProvider{
			validators: validators,
			balances:   balances,
		},
	}

	updated, err := s.summarizeEpoch(ctx, &metadata{}, epoch)
	require.NoError(t, err)
	require.False(t, updated)
	require.Empty(t, chainDB.summariesSet,
		"corrupt summary written to t_epoch_summaries despite zero-balance guard")

	logged := buf.String()
	require.True(t,
		strings.Contains(logged, "No validator balances available; cannot summarize epoch"),
		"warn log missing expected stable message text; got: %s", logged)
	require.True(t,
		strings.Contains(logged, `"level":"warn"`),
		"zero-balance log emitted at unexpected level; got: %s", logged)
}

// TestSetLagGaugesWiring drives setLagGauges with synthetic (summarizer,
// upstream) metadata pairs and asserts per-pipeline cursor-diff values,
// disabled-pipeline absence, and clamp-to-zero behavior in the brief race
// window where a downstream cursor is read ahead of its upstream.
func TestSetLagGaugesWiring(t *testing.T) {
	originalGauge := summarizerLagEpochs
	summarizerLagEpochs = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "lag_epochs",
		Help:      "test",
	}, []string{"pipeline"})
	defer func() { summarizerLagEpochs = originalGauge }()

	tests := []struct {
		name       string
		service    *Service
		md         *metadata
		upstream   *upstreamMetadata
		wantSeries int
		wantValues map[string]float64
	}{
		{
			name: "all caught up",
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
			upstream:   &upstreamMetadata{LatestBalancesEpoch: 10},
			wantSeries: 3,
			wantValues: map[string]float64{
				"epoch":     0,
				"block":     0,
				"validator": 0,
			},
		},
		{
			name: "upstream ahead of summarizer",
			service: &Service{
				epochSummaries:     true,
				blockSummaries:     true,
				validatorSummaries: true,
			},
			md: &metadata{
				LastEpoch:          7,
				LastBlockEpoch:     7,
				LastValidatorEpoch: 7,
			},
			upstream:   &upstreamMetadata{LatestBalancesEpoch: 10},
			wantSeries: 3,
			wantValues: map[string]float64{
				"epoch":     3, // 10 - 7
				"block":     0, // summarizer epoch == sub-pipelines
				"validator": 0,
			},
		},
		{
			name: "intra-summarizer lag (block + validator behind epoch)",
			service: &Service{
				epochSummaries:     true,
				blockSummaries:     true,
				validatorSummaries: true,
			},
			md: &metadata{
				LastEpoch:          10,
				LastBlockEpoch:     5,
				LastValidatorEpoch: 4,
			},
			upstream:   &upstreamMetadata{LatestBalancesEpoch: 10},
			wantSeries: 3,
			wantValues: map[string]float64{
				"epoch":     0, // 10 - 10
				"block":     5, // 10 - 5
				"validator": 6, // 10 - 4
			},
		},
		{
			name: "race window — summarizer briefly ahead of upstream",
			service: &Service{
				epochSummaries:     true,
				blockSummaries:     true,
				validatorSummaries: true,
			},
			md: &metadata{
				LastEpoch:          12,
				LastBlockEpoch:     12,
				LastValidatorEpoch: 12,
			},
			upstream:   &upstreamMetadata{LatestBalancesEpoch: 10},
			wantSeries: 3,
			wantValues: map[string]float64{
				"epoch":     0, // clamped, not 1.8e19
				"block":     0,
				"validator": 0,
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
			upstream:   &upstreamMetadata{LatestBalancesEpoch: 11},
			wantSeries: 2,
			wantValues: map[string]float64{
				"epoch":     2, // 11 - 9
				"validator": 1, // 9 - 8
			},
		},
		{
			name:       "no pipelines enabled",
			service:    &Service{},
			md:         &metadata{},
			upstream:   &upstreamMetadata{},
			wantSeries: 0,
			wantValues: map[string]float64{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			summarizerLagEpochs.Reset()

			test.service.setLagGauges(test.md, test.upstream)

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
