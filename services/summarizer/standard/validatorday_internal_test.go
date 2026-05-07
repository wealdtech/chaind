// Copyright © 2026 Weald Technology Limited.
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
	"time"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/chaind/services/chaindb"
	mockchaintime "github.com/wealdtech/chaind/services/chaintime/mock"
)

// stubBalanceChainDB is a chaindb.Service that also implements the four
// interfaces addValidatorBalanceSummaries type-asserts against:
// ValidatorsProvider, DepositsProvider, WithdrawalsProvider, and
// ValidatorDaySummariesSetter.  The balance responses are popped from a
// queue so a single fixture can drive the start-balances and end-balances
// trip paths from one slice of test inputs.  daySummariesSet records every
// SetValidatorDaySummaries call so the test can assert that the corrupt-
// summary guard prevents writes.
type stubBalanceChainDB struct {
	balancesResponses [][]*chaindb.ValidatorBalance
	balancesCallCount int
	daySummariesSet   [][]*chaindb.ValidatorDaySummary
}

// chaindb.Service.
func (c *stubBalanceChainDB) BeginTx(ctx context.Context) (context.Context, context.CancelFunc, error) {
	return ctx, func() {}, nil
}

func (c *stubBalanceChainDB) CommitTx(_ context.Context) error                        { return nil }
func (c *stubBalanceChainDB) BeginROTx(ctx context.Context) (context.Context, error)  { return ctx, nil }
func (c *stubBalanceChainDB) CommitROTx(_ context.Context)                            {}
func (c *stubBalanceChainDB) SetMetadata(_ context.Context, _ string, _ []byte) error { return nil }
func (c *stubBalanceChainDB) Metadata(_ context.Context, _ string) ([]byte, error)    { return nil, nil }

// chaindb.ValidatorsProvider.
func (c *stubBalanceChainDB) Validators(_ context.Context) ([]*chaindb.Validator, error) {
	return nil, nil
}

func (c *stubBalanceChainDB) ValidatorsByPublicKey(_ context.Context, _ []phase0.BLSPubKey) (map[phase0.BLSPubKey]*chaindb.Validator, error) {
	return nil, nil
}

func (c *stubBalanceChainDB) ValidatorsByIndex(_ context.Context, _ []phase0.ValidatorIndex) (map[phase0.ValidatorIndex]*chaindb.Validator, error) {
	return nil, nil
}

func (c *stubBalanceChainDB) ValidatorBalancesByEpoch(_ context.Context, _ phase0.Epoch) ([]*chaindb.ValidatorBalance, error) {
	if c.balancesCallCount >= len(c.balancesResponses) {
		return nil, nil
	}
	resp := c.balancesResponses[c.balancesCallCount]
	c.balancesCallCount++
	return resp, nil
}

func (c *stubBalanceChainDB) ValidatorBalancesByIndexAndEpoch(_ context.Context, _ []phase0.ValidatorIndex, _ phase0.Epoch) (map[phase0.ValidatorIndex]*chaindb.ValidatorBalance, error) {
	return nil, nil
}

func (c *stubBalanceChainDB) ValidatorBalancesByIndexAndEpochRange(_ context.Context, _ []phase0.ValidatorIndex, _ phase0.Epoch, _ phase0.Epoch) (map[phase0.ValidatorIndex][]*chaindb.ValidatorBalance, error) {
	return nil, nil
}

func (c *stubBalanceChainDB) ValidatorBalancesByIndexAndEpochs(_ context.Context, _ []phase0.ValidatorIndex, _ []phase0.Epoch) (map[phase0.ValidatorIndex][]*chaindb.ValidatorBalance, error) {
	return nil, nil
}

// chaindb.DepositsProvider.
func (c *stubBalanceChainDB) DepositsByPublicKey(_ context.Context, _ []phase0.BLSPubKey) (map[phase0.BLSPubKey][]*chaindb.Deposit, error) {
	return nil, nil
}

func (c *stubBalanceChainDB) DepositsForSlotRange(_ context.Context, _ phase0.Slot, _ phase0.Slot) ([]*chaindb.Deposit, error) {
	return nil, nil
}

// chaindb.WithdrawalsProvider.
func (c *stubBalanceChainDB) Withdrawals(_ context.Context, _ *chaindb.WithdrawalFilter) ([]*chaindb.Withdrawal, error) {
	return nil, nil
}

// chaindb.ValidatorDaySummariesSetter — addValidatorBalanceSummaries does
// not call these directly; the parent summarizeValidatorsInDay does, after
// addValidatorBalanceSummaries returns (true, nil).  Recording these calls
// here is a structural-impossibility witness: the test asserts the
// recording slice stays empty, which is the test's way of documenting the
// guard contract (false, nil) → caller short-circuits → no day summary
// written.  Mirrors the recordingChainDB pattern in handler_internal_test.go.
func (c *stubBalanceChainDB) SetValidatorDaySummary(_ context.Context, _ *chaindb.ValidatorDaySummary) error {
	return nil
}

func (c *stubBalanceChainDB) SetValidatorDaySummaries(_ context.Context, summaries []*chaindb.ValidatorDaySummary) error {
	c.daySummariesSet = append(c.daySummariesSet, summaries)
	return nil
}

// TestAddValidatorBalanceSummariesZeroBalanceAssertion drives
// addValidatorBalanceSummaries with the LEFT-JOIN-all-zero shape that
// ValidatorBalancesByEpoch returns when no t_validator_balances rows
// exist for an epoch but t_validators is populated, mirroring the
// epoch-layer guard from commit 1edcb45 (issue 10).  Both call sites
// (startBalances at validatorday.go:224 and endBalances at :280) must
// refuse to advance: return (false, nil), emit the alert-contract warn
// log with the same stable message string, and write nothing to
// t_validator_day_summaries.
func TestAddValidatorBalanceSummariesZeroBalanceAssertion(t *testing.T) {
	const numValidators = 1000

	makeZeroBalances := func() []*chaindb.ValidatorBalance {
		balances := make([]*chaindb.ValidatorBalance, numValidators)
		for i := range numValidators {
			balances[i] = &chaindb.ValidatorBalance{
				Index:            phase0.ValidatorIndex(i),
				Balance:          0,
				EffectiveBalance: 0,
			}
		}
		return balances
	}

	makeNonZeroBalances := func() []*chaindb.ValidatorBalance {
		balances := make([]*chaindb.ValidatorBalance, numValidators)
		for i := range numValidators {
			balances[i] = &chaindb.ValidatorBalance{
				Index:            phase0.ValidatorIndex(i),
				Balance:          32_000_000_000,
				EffectiveBalance: 32_000_000_000,
			}
		}
		return balances
	}

	tests := []struct {
		name              string
		balancesResponses [][]*chaindb.ValidatorBalance
	}{
		{
			// Bootstrap-or-production-state path 1: the very first
			// ValidatorBalancesByEpoch call returns all-zero rows; the
			// startBalances guard must trip immediately.
			name: "startBalances all zero",
			balancesResponses: [][]*chaindb.ValidatorBalance{
				makeZeroBalances(),
			},
		},
		{
			// Path 2: startBalances are valid, function proceeds past
			// deposits/withdrawals, then endBalances trips the guard.
			name: "endBalances all zero",
			balancesResponses: [][]*chaindb.ValidatorBalance{
				makeNonZeroBalances(),
				makeZeroBalances(),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()

			// main_test.go sets the global level to Disabled; locally
			// raise it so the buffer logger below actually emits.
			originalGlobalLevel := zerolog.GlobalLevel()
			zerolog.SetGlobalLevel(zerolog.TraceLevel)
			defer zerolog.SetGlobalLevel(originalGlobalLevel)

			var buf bytes.Buffer
			originalLog := log
			log = zerolog.New(&buf).Level(zerolog.WarnLevel)
			defer func() { log = originalLog }()

			chainDB := &stubBalanceChainDB{
				balancesResponses: test.balancesResponses,
			}

			s := &Service{
				chainDB:   chainDB,
				chainTime: mockchaintime.New(),
			}

			daySummaries := make(map[phase0.ValidatorIndex]*chaindb.ValidatorDaySummary)
			startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
			endTime := startTime.AddDate(0, 0, 1)

			found, err := s.addValidatorBalanceSummaries(ctx, daySummaries, startTime, endTime)
			require.NoError(t, err)
			require.False(t, found,
				"guard should refuse to compute a corrupt day summary when balances are all zero")
			require.Empty(t, chainDB.daySummariesSet,
				"guard contract violation: when addValidatorBalanceSummaries returns (false, nil) the parent summarizeValidatorsInDay never reaches SetValidatorDaySummaries — recording any write would indicate the contract was broken")

			logged := buf.String()
			require.True(t,
				strings.Contains(logged, "No validator balances available; cannot summarize epoch (will retry on next finality tick)"),
				"warn log missing exact alert-contract message text; got: %s", logged)
			require.True(t,
				strings.Contains(logged, `"level":"warn"`),
				"zero-balance log emitted at unexpected level; got: %s", logged)
			require.True(t,
				strings.Contains(logged, `"start_time"`),
				"warn log missing structured start_time field for day-layer disambiguation; got: %s", logged)
			require.True(t,
				strings.Contains(logged, `"end_time"`),
				"warn log missing structured end_time field for day-layer disambiguation; got: %s", logged)
		})
	}
}
