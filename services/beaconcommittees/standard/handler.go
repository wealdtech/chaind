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

package standard

import (
	"context"
	"fmt"

	eth2client "github.com/attestantio/go-eth2-client"
	spec "github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
)

// OnBeaconChainHeadUpdated receives beacon chain head updated notifications.
func (s *Service) OnBeaconChainHeadUpdated(
	ctx context.Context,
	slot spec.Slot,
	blockRoot spec.Root,
	stateRoot spec.Root,
	epochTransition bool,
) {
	if !epochTransition {
		// Only interested in epoch transitions.
		return
	}

	epoch := s.chainTime.SlotToEpoch(slot)
	log := log.With().Uint64("epoch", uint64(epoch)).Logger()

	ctx, cancel, err := s.chainDB.BeginTx(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to begin transaction on beacon chain head update")
		return
	}

	md, err := s.getMetadata(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to obtain metadata")
	}

	if err := s.updateBeaconCommitteesForEpoch(ctx, epoch); err != nil {
		log.Error().Err(err).Msg("Failed to update beacon committees on beacon chain head update")
		md.MissedEpochs = append(md.MissedEpochs, epoch)
	}

	md.LatestEpoch = epoch
	if err := s.setMetadata(ctx, md); err != nil {
		log.Error().Err(err).Msg("Failed to set metadata")
	}

	if err := s.chainDB.CommitTx(ctx); err != nil {
		log.Error().Err(err).Msg("Failed to commit transaction")
		cancel()
		return
	}

	log.Trace().Msg("Stored beacon committees")
}

func (s *Service) updateBeaconCommitteesForEpoch(ctx context.Context, epoch spec.Epoch) error {
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
		if err := s.chainDB.(chaindb.BeaconCommitteeService).SetBeaconCommittee(ctx, dbBeaconCommittee); err != nil {
			return errors.Wrap(err, "failed to set beacon committee")
		}
	}
	return nil
}
