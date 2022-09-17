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
)

// OnBeaconChainHeadUpdated receives beacon chain head updated notifications.
func (s *Service) OnBeaconChainHeadUpdated(
	ctx context.Context,
	slot phase0.Slot,
	_ phase0.Root,
	_ phase0.Root,
	// skipcq: RVV-A0005
	epochTransition bool,
) {
	if !epochTransition {
		// Only interested in epoch transitions.
		return
	}

	// Only allow 1 handler to be active.
	acquired := s.activitySem.TryAcquire(1)
	if !acquired {
		log.Debug().Msg("Another handler running")
		return
	}

	epoch := s.chainTime.SlotToEpoch(slot)
	log := log.With().Uint64("epoch", uint64(epoch)).Logger()

	md, err := s.getMetadata(ctx)
	if err != nil {
		s.activitySem.Release(1)
		log.Error().Err(err).Msg("Failed to obtain metadata")
		return
	}

	if md.LatestEpoch > 0 {
		// We have a definite hit on this being the last processed epoch; increment it to avoid duplication of work.
		md.LatestEpoch++
	}

	s.catchup(ctx, md)
	s.activitySem.Release(1)
}

// updateBeaconCommitteesForEpoch sets the beacon committee information for the given epoch.
// This assumes that a database transaction is already in progress.
func (s *Service) updateBeaconCommitteesForEpoch(ctx context.Context, epoch phase0.Epoch) error {
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
