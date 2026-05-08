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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/attestantio/go-eth2-client/api"
	apiv1 "github.com/attestantio/go-eth2-client/api/v1"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/wealdtech/chaind/services/chaindb"
)

// captureWarnLog redirects the package logger to a byte buffer at WarnLevel
// and returns the buffer plus a restore func.
func captureWarnLog(t *testing.T) (*bytes.Buffer, func()) {
	t.Helper()
	originalGlobalLevel := zerolog.GlobalLevel()
	zerolog.SetGlobalLevel(zerolog.TraceLevel)

	var buf bytes.Buffer
	originalLog := log
	log = zerolog.New(&buf).Level(zerolog.WarnLevel)

	return &buf, func() {
		log = originalLog
		zerolog.SetGlobalLevel(originalGlobalLevel)
	}
}

// stubEth2Client implements eth2client.Service + ValidatorsProvider; only
// Validators is exercised.
type stubEth2Client struct {
	response *api.Response[map[phase0.ValidatorIndex]*apiv1.Validator]
	err      error
	calls    int
}

func (s *stubEth2Client) Name() string    { return "stub" }
func (s *stubEth2Client) Address() string { return "stub" }
func (s *stubEth2Client) IsActive() bool  { return true }
func (s *stubEth2Client) IsSynced() bool  { return true }

func (s *stubEth2Client) Validators(_ context.Context, _ *api.ValidatorsOpts) (
	*api.Response[map[phase0.ValidatorIndex]*apiv1.Validator], error,
) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return s.response, nil
}

// recordingChainDB captures BeginTx/CommitTx ordering and SetMetadata payloads
// so tests can assert the cursor advanced and the transaction committed.
type recordingChainDB struct {
	beginTxCalls  int
	commitTxCalls int
	setMetadata   [][]byte
	cancelCalls   int
}

func (c *recordingChainDB) BeginTx(ctx context.Context) (context.Context, context.CancelFunc, error) {
	c.beginTxCalls++
	return ctx, func() { c.cancelCalls++ }, nil
}

func (c *recordingChainDB) CommitTx(_ context.Context) error {
	c.commitTxCalls++
	return nil
}

func (c *recordingChainDB) BeginROTx(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

func (c *recordingChainDB) CommitROTx(_ context.Context) {}

func (c *recordingChainDB) SetMetadata(_ context.Context, _ string, value []byte) error {
	dup := make([]byte, len(value))
	copy(dup, value)
	c.setMetadata = append(c.setMetadata, dup)
	return nil
}

func (c *recordingChainDB) Metadata(_ context.Context, _ string) ([]byte, error) {
	return nil, nil
}

// recordingValidatorsSetter captures every SetValidatorBalances/SetValidatorBalance
// call so tests can assert what was persisted.
type recordingValidatorsSetter struct {
	bulkCalls          [][]*chaindb.ValidatorBalance
	individualBalances []*chaindb.ValidatorBalance
}

func (r *recordingValidatorsSetter) SetValidator(_ context.Context, _ *chaindb.Validator) error {
	return nil
}

func (r *recordingValidatorsSetter) SetValidatorBalance(_ context.Context, balance *chaindb.ValidatorBalance) error {
	r.individualBalances = append(r.individualBalances, balance)
	return nil
}

func (r *recordingValidatorsSetter) SetValidatorBalances(_ context.Context, balances []*chaindb.ValidatorBalance) error {
	dup := make([]*chaindb.ValidatorBalance, len(balances))
	copy(dup, balances)
	r.bulkCalls = append(r.bulkCalls, dup)
	return nil
}

// stubChainTime satisfies chaintime.Service; only FirstSlotOfEpoch is invoked.
type stubChainTime struct{}

func (stubChainTime) GenesisTime() time.Time                           { return time.Time{} }
func (stubChainTime) SlotDuration() time.Duration                      { return 0 }
func (stubChainTime) SlotsPerEpoch() uint64                            { return 32 }
func (stubChainTime) StartOfSlot(_ phase0.Slot) time.Time              { return time.Time{} }
func (stubChainTime) StartOfEpoch(_ phase0.Epoch) time.Time            { return time.Time{} }
func (stubChainTime) CurrentSlot() phase0.Slot                         { return 0 }
func (stubChainTime) CurrentEpoch() phase0.Epoch                       { return 0 }
func (stubChainTime) CurrentSyncCommitteePeriod() uint64               { return 0 }
func (stubChainTime) SlotToEpoch(_ phase0.Slot) phase0.Epoch           { return 0 }
func (stubChainTime) SlotToSyncCommitteePeriod(_ phase0.Slot) uint64   { return 0 }
func (stubChainTime) EpochToSyncCommitteePeriod(_ phase0.Epoch) uint64 { return 0 }
func (stubChainTime) FirstSlotOfEpoch(epoch phase0.Epoch) phase0.Slot {
	return phase0.Slot(uint64(epoch) * 32)
}
func (stubChainTime) LastSlotOfEpoch(_ phase0.Epoch) phase0.Slot   { return 0 }
func (stubChainTime) TimestampToSlot(_ time.Time) phase0.Slot      { return 0 }
func (stubChainTime) TimestampToEpoch(_ time.Time) phase0.Epoch    { return 0 }
func (stubChainTime) FirstEpochOfSyncPeriod(_ uint64) phase0.Epoch { return 0 }
func (stubChainTime) AltairInitialEpoch() phase0.Epoch             { return 0 }
func (stubChainTime) AltairInitialSyncCommitteePeriod() uint64     { return 0 }
func (stubChainTime) BellatrixInitialEpoch() phase0.Epoch          { return 0 }
func (stubChainTime) CapellaInitialEpoch() phase0.Epoch            { return 0 }

// makeService wires the stubs into a *Service for the
// onEpochTransitionValidatorBalancesForEpoch path.
func makeService(eth2 *stubEth2Client, db *recordingChainDB, setter *recordingValidatorsSetter) *Service {
	return &Service{
		eth2Client:       eth2,
		chainDB:          db,
		validatorsSetter: setter,
		chainTime:        stubChainTime{},
		balances:         true,
	}
}

// Empty 200-OK response: cursor stays put, no transaction opens, warn-log
// fires.  Regression boundary for ADR 0002.
func TestOnEpochTransitionValidatorBalancesForEpoch_EmptyValidatorsRejectedAndCursorUnchanged(t *testing.T) {
	const epoch = phase0.Epoch(86823)
	const startCursor = phase0.Epoch(86822)

	buf, restore := captureWarnLog(t)
	defer restore()

	eth2 := &stubEth2Client{
		response: &api.Response[map[phase0.ValidatorIndex]*apiv1.Validator]{
			Data: map[phase0.ValidatorIndex]*apiv1.Validator{},
		},
	}
	db := &recordingChainDB{}
	setter := &recordingValidatorsSetter{}
	s := makeService(eth2, db, setter)

	md := &metadata{LatestBalancesEpoch: startCursor}

	err := s.onEpochTransitionValidatorBalancesForEpoch(context.Background(), md, epoch)

	require.Error(t, err,
		"empty 200-OK response must surface an error so the parent handler logs it")
	require.Contains(t, err.Error(), "beacon returned no validator balances",
		"error text must carry the stable contract substring used by alert rules")

	require.Equal(t, 1, eth2.calls, "beacon Validators must be called exactly once")
	require.Equal(t, 0, db.beginTxCalls,
		"guard 1 fires before BeginTx; no transaction may open on this path")
	require.Equal(t, 0, db.commitTxCalls,
		"no transaction may commit when the guard rejects the response")
	require.Equal(t, 0, db.cancelCalls,
		"no transaction was opened, so cancel must not be invoked")

	require.Empty(t, setter.bulkCalls,
		"SetValidatorBalances must not be called when the response is empty")
	require.Empty(t, setter.individualBalances,
		"SetValidatorBalance must not be called when the response is empty")
	require.Empty(t, db.setMetadata,
		"SetMetadata must not be called when the response is empty")

	require.Equal(t, startCursor, md.LatestBalancesEpoch,
		"cursor must NOT advance when the beacon returns no validators")

	logged := buf.String()
	require.True(t, strings.Contains(logged, noValidatorBalancesMsg),
		"warn log missing alert-contract message text; got: %s", logged)
	require.True(t, strings.Contains(logged, `"level":"warn"`),
		"empty-response log emitted at unexpected level; got: %s", logged)
	require.True(t, strings.Contains(logged, `"epoch":86823`),
		"warn log missing structured epoch field for operator disambiguation; got: %s", logged)
}

// All-zero beacon response amplified by the "Do not store 0 balances" filter
// into a zero-row insert.  Cursor stays put, no transaction opens, warn-log
// fires.
func TestOnEpochTransitionValidatorBalancesForEpoch_AllZeroBalancesRejectedAndCursorUnchanged(t *testing.T) {
	const epoch = phase0.Epoch(86824)
	const startCursor = phase0.Epoch(86823)
	const numValidators = 1000

	buf, restore := captureWarnLog(t)
	defer restore()

	validators := make(map[phase0.ValidatorIndex]*apiv1.Validator, numValidators)
	for i := range numValidators {
		idx := phase0.ValidatorIndex(i)
		validators[idx] = &apiv1.Validator{
			Index:   idx,
			Balance: 0,
			Status:  apiv1.ValidatorStateActiveOngoing,
			Validator: &phase0.Validator{
				EffectiveBalance: 0,
			},
		}
	}

	eth2 := &stubEth2Client{
		response: &api.Response[map[phase0.ValidatorIndex]*apiv1.Validator]{
			Data: validators,
		},
	}
	db := &recordingChainDB{}
	setter := &recordingValidatorsSetter{}
	s := makeService(eth2, db, setter)

	md := &metadata{LatestBalancesEpoch: startCursor}

	err := s.onEpochTransitionValidatorBalancesForEpoch(context.Background(), md, epoch)

	require.Error(t, err,
		"all-zero-balance response must surface an error so the parent handler logs it")
	require.Contains(t, err.Error(), "beacon returned no validator balances",
		"error text must carry the stable contract substring used by alert rules")

	require.Equal(t, 1, eth2.calls, "beacon Validators must be called exactly once")
	require.Equal(t, 0, db.beginTxCalls,
		"guard 2 fires before BeginTx; no transaction may open on this path")
	require.Equal(t, 0, db.commitTxCalls,
		"no transaction may commit when the guard rejects the filtered response")
	require.Equal(t, 0, db.cancelCalls,
		"no transaction was opened, so cancel must not be invoked")

	require.Empty(t, setter.bulkCalls,
		"SetValidatorBalances must not be called when the filtered slice is empty")
	require.Empty(t, setter.individualBalances,
		"SetValidatorBalance must not be called when the filtered slice is empty")
	require.Empty(t, db.setMetadata,
		"SetMetadata must not be called when the cursor is not advanced")

	require.Equal(t, startCursor, md.LatestBalancesEpoch,
		"cursor must NOT advance when the all-zero filter collapses the response to nothing")

	logged := buf.String()
	require.True(t, strings.Contains(logged, noValidatorBalancesMsg),
		"warn log missing alert-contract message text; got: %s", logged)
	require.True(t, strings.Contains(logged, `"level":"warn"`),
		"all-zero log emitted at unexpected level; got: %s", logged)
	require.True(t, strings.Contains(logged, `"epoch":86824`),
		"warn log missing structured epoch field for operator disambiguation; got: %s", logged)
}

// Mixed zero/non-zero balances: filter drops zeros, function persists the
// non-zero half and advances the cursor.
func TestOnEpochTransitionValidatorBalancesForEpoch_MixedBalancesPersistsNonZero(t *testing.T) {
	const epoch = phase0.Epoch(100)
	const numValidators = 1000
	const numNonZero = 500
	const nonZeroBalance = phase0.Gwei(32_000_000_000)

	validators := make(map[phase0.ValidatorIndex]*apiv1.Validator, numValidators)
	for i := range numValidators {
		idx := phase0.ValidatorIndex(i)
		var balance phase0.Gwei
		if i < numNonZero {
			balance = nonZeroBalance
		}
		validators[idx] = &apiv1.Validator{
			Index:   idx,
			Balance: balance,
			Status:  apiv1.ValidatorStateActiveOngoing,
			Validator: &phase0.Validator{
				EffectiveBalance: balance,
			},
		}
	}

	eth2 := &stubEth2Client{
		response: &api.Response[map[phase0.ValidatorIndex]*apiv1.Validator]{
			Data: validators,
		},
	}
	db := &recordingChainDB{}
	setter := &recordingValidatorsSetter{}
	s := makeService(eth2, db, setter)

	md := &metadata{LatestBalancesEpoch: epoch - 1}

	err := s.onEpochTransitionValidatorBalancesForEpoch(context.Background(), md, epoch)

	require.NoError(t, err, "mixed-balance response surfaces no error today")
	require.Equal(t, 1, eth2.calls, "beacon Validators must be called exactly once")
	require.Equal(t, 1, db.beginTxCalls, "must begin exactly one transaction")
	require.Equal(t, 1, db.commitTxCalls, "transaction must commit")
	require.Empty(t, setter.individualBalances,
		"fallback per-row insert path must not be entered when bulk insert succeeds")

	require.Len(t, setter.bulkCalls, 1, "SetValidatorBalances must be called once")
	persisted := setter.bulkCalls[0]
	require.Len(t, persisted, numNonZero,
		"only validators with Balance > 0 should be persisted")
	for _, b := range persisted {
		require.Equal(t, nonZeroBalance, b.Balance,
			"every persisted row must carry the non-zero balance")
		require.Equal(t, epoch, b.Epoch,
			"every persisted row must be tagged with the requested epoch")
		require.Less(t, uint64(b.Index), uint64(numNonZero),
			"only validators with index < numNonZero have non-zero balances")
	}

	require.Equal(t, epoch, md.LatestBalancesEpoch,
		"cursor advances when at least some rows persist (expected today)")
}

// Negative control: beacon error surfaces, no transaction opens, cursor
// stays put.
func TestOnEpochTransitionValidatorBalancesForEpoch_BeaconErrorLeavesCursorUnchanged(t *testing.T) {
	const epoch = phase0.Epoch(50)
	const startCursor = phase0.Epoch(49)

	eth2 := &stubEth2Client{
		err: errors.New("beacon error"),
	}
	db := &recordingChainDB{}
	setter := &recordingValidatorsSetter{}
	s := makeService(eth2, db, setter)

	md := &metadata{LatestBalancesEpoch: startCursor}

	err := s.onEpochTransitionValidatorBalancesForEpoch(context.Background(), md, epoch)

	require.Error(t, err, "beacon error must surface to the caller")
	require.Contains(t, err.Error(), "failed to obtain validators for validator balances",
		"error wrapping at handler.go:190 must be preserved")
	require.Contains(t, err.Error(), "beacon error",
		"original beacon error must be preserved through the wrap")

	require.Equal(t, 1, eth2.calls, "beacon Validators must be called exactly once")
	require.Equal(t, 0, db.beginTxCalls, "no transaction may begin when the beacon errors")
	require.Equal(t, 0, db.commitTxCalls, "no transaction may commit when the beacon errors")
	require.Empty(t, setter.bulkCalls, "SetValidatorBalances must not be called")
	require.Empty(t, setter.individualBalances, "SetValidatorBalance must not be called")
	require.Empty(t, db.setMetadata, "SetMetadata must not be called when the beacon errors")
	require.Equal(t, startCursor, md.LatestBalancesEpoch,
		"cursor must NOT advance on beacon error (negative control)")
}
