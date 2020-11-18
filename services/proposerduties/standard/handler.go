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

	if err := s.updateProposerDutiesForEpoch(ctx, epoch); err != nil {
		log.Error().Err(err).Msg("Failed to update proposer duties on beacon chain head update")
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

	log.Trace().Msg("Stored attester duties")
}

func (s *Service) updateProposerDutiesForEpoch(ctx context.Context, epoch spec.Epoch) error {
	duties, err := s.eth2Client.(eth2client.ProposerDutiesProvider).ProposerDuties(ctx, epoch, nil)
	if err != nil {
		return errors.Wrap(err, "failed to fetch proposer duties")
	}

	for _, duty := range duties {
		dbProposerDuty := &chaindb.ProposerDuty{
			Slot:           duty.Slot,
			ValidatorIndex: duty.ValidatorIndex,
		}
		if err := s.proposerDutiesSetter.SetProposerDuty(ctx, dbProposerDuty); err != nil {
			return errors.Wrap(err, "failed to set proposer duty")
		}
	}
	return nil
}
