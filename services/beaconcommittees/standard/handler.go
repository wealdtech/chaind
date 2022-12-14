// Copyright © 2020, 2022 Weald Technology Trading.
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
	"context"
	"fmt"

	eth2client "github.com/attestantio/go-eth2-client"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// OnBeaconChainHeadUpdated receives beacon chain head updated notifications.
func (s *Service) OnBeaconChainHeadUpdated(
	ctx context.Context,
	slot phase0.Slot,
	blockRoot phase0.Root,
	stateRoot phase0.Root,
	// skipcq: RVV-A0005
	epochTransition bool,
) {
	ctx, span := otel.Tracer("wealdtech.chaind.services.beaconcommittees.standard").Start(ctx, "OnBeaconChainHeadUpdated",
		trace.WithAttributes(
			attribute.Int64("slot", int64(slot)),
		))
	defer span.End()

	if !epochTransition {
		// Only interested in epoch transitions.
		return
	}

	log := log.With().Uint64("slot", uint64(slot)).Str("block_root", fmt.Sprintf("%#x", blockRoot)).Logger()

	// Only allow 1 handler to be active.
	acquired := s.activitySem.TryAcquire(1)
	if !acquired {
		log.Debug().Msg("Another handler running")
		return
	}
	defer s.activitySem.Release(1)

	log.Trace().
		Str("state_root", fmt.Sprintf("%#x", stateRoot)).
		Bool("epoch_transition", epochTransition).
		Msg("Handler called")

	md, err := s.getMetadata(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to obtain metadata")
		return
	}

	s.catchup(ctx, md)
}

// catchup is the general-purpose catchup system.
func (s *Service) catchup(ctx context.Context, md *metadata) {
	for epoch := phase0.Epoch(md.LatestEpoch + 1); epoch <= s.chainTime.CurrentEpoch(); epoch++ {
		if err := s.UpdateEpoch(ctx, md, epoch); err != nil {
			log.Error().Uint64("epoch", uint64(epoch)).Err(err).Msg("Failed to catchup")
			return
		}
	}
}

func (s *Service) UpdateEpoch(ctx context.Context, md *metadata, epoch phase0.Epoch) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.beaconcommittees.standard").Start(ctx, "UpdateEpoch",
		trace.WithAttributes(
			attribute.Int64("epoch", int64(epoch)),
		))
	defer span.End()

	// Each epoch runs in its own transaction, to make the data available sooner.
	ctx, cancel, err := s.chainDB.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}

	if err := s.updateBeaconCommitteesForEpoch(ctx, epoch); err != nil {
		cancel()
		return errors.Wrap(err, "failed to update beacon committees")
	}

	md.LatestEpoch = int64(epoch)
	if err := s.setMetadata(ctx, md); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set metadata")
	}

	if err := s.chainDB.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to commit transaction")
	}

	monitorEpochProcessed(epoch)
	return nil
}

// updateBeaconCommitteesForEpoch sets the beacon committee information for the given epoch.
// This assumes that a database transaction is already in progress.
func (s *Service) updateBeaconCommitteesForEpoch(ctx context.Context, epoch phase0.Epoch) error {
	ctx, span := otel.Tracer("wealdtech.chaind.services.beaconcommittees.standard").Start(ctx, "updateBeaconCommitteesForEpoch",
		trace.WithAttributes(
			attribute.Int64("epoch", int64(epoch)),
		))
	defer span.End()
	log.Trace().Uint64("epoch", uint64(epoch)).Msg("Updating beacon committees")

	beaconCommittees, err := s.eth2Client.(eth2client.BeaconCommitteesProvider).BeaconCommittees(ctx, fmt.Sprintf("%d", s.chainTime.FirstSlotOfEpoch(epoch)))
	if err != nil {
		return errors.Wrap(err, "failed to fetch beacon committees")
	}

	for _, beaconCommittee := range beaconCommittees {
		dbBeaconCommittee := &chaindb.BeaconCommittee{
			Slot:      beaconCommittee.Slot,
			Index:     beaconCommittee.Index,
			Committee: beaconCommittee.Validators,
		}
		if err := s.beaconCommitteesSetter.SetBeaconCommittee(ctx, dbBeaconCommittee); err != nil {
			return errors.Wrap(err, "failed to set beacon committee")
		}
	}
	monitorEpochProcessed(epoch)

	return nil
}
