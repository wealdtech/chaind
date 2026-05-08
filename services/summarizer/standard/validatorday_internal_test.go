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

// stubBalanceChainDB pops balance responses from a queue so one fixture can
// drive both the start-balances and end-balances guard paths.  daySummariesSet
// records writes so tests can assert the guard short-circuits.
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

// chaindb.ValidatorDaySummariesSetter.
func (c *stubBalanceChainDB) SetValidatorDaySummary(_ context.Context, _ *chaindb.ValidatorDaySummary) error {
	return nil
}

func (c *stubBalanceChainDB) SetValidatorDaySummaries(_ context.Context, summaries []*chaindb.ValidatorDaySummary) error {
	c.daySummariesSet = append(c.daySummariesSet, summaries)
	return nil
}

// TestAddValidatorBalanceSummariesZeroBalanceAssertion drives the LEFT-JOIN-
// all-zero shape into both startBalances and endBalances guard sites and
// asserts neither writes to t_validator_day_summaries.
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
			name: "startBalances all zero",
			balancesResponses: [][]*chaindb.ValidatorBalance{
				makeZeroBalances(),
			},
		},
		{
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

			// main_test.go sets GlobalLevel to Disabled; raise it so the buffer captures warns.
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
