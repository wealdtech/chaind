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
	"github.com/attestantio/go-eth2-client/api"
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

	s.catchup(ctx, md)
	s.activitySem.Release(1)
}

func (s *Service) updateProposerDutiesForEpoch(ctx context.Context, epoch phase0.Epoch) error {
	dutiesResponse, err := s.eth2Client.(eth2client.ProposerDutiesProvider).ProposerDuties(ctx, &api.ProposerDutiesOpts{
		Epoch: epoch,
	})
	if err != nil {
		return errors.Wrap(err, "failed to fetch proposer duties")
	}
	duties := dutiesResponse.Data

	log.Trace().Uint64("epoch", uint64(epoch)).Msg("Setting proposer duties")
	for _, duty := range duties {
		dbProposerDuty := &chaindb.ProposerDuty{
			Slot:           duty.Slot,
			ValidatorIndex: duty.ValidatorIndex,
		}
		if err := s.proposerDutiesSetter.SetProposerDuty(ctx, dbProposerDuty); err != nil {
			return errors.Wrap(err, "failed to set proposer duty")
		}
	}

	monitorEpochProcessed(epoch)
	return nil
}
