// Copyright © 2021 Weald Technology Trading.
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

	"github.com/attestantio/go-eth2-client/api"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
)

// OnBeaconChainHeadUpdated receives beacon chain head updated notifications.
func (s *Service) OnBeaconChainHeadUpdated(
	ctx context.Context,
	slot phase0.Slot,
) {
	// Only allow 1 handler to be active.
	acquired := s.activitySem.TryAcquire(1)
	if !acquired {
		log.Debug().Msg("Another handler running")
		return
	}
	defer s.activitySem.Release(1)

	epoch := s.chainTime.SlotToEpoch(slot)
	period := s.chainTime.EpochToSyncCommitteePeriod(epoch)
	log := log.With().Uint64("slot", uint64(slot)).Uint64("epoch", uint64(epoch)).Uint64("period", period).Logger()

	if epoch < s.chainTime.AltairInitialEpoch() {
		log.Trace().Msg("Chain is not yet generating sync committees")
		return
	}

	md, err := s.getMetadata(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to obtain metadata")
		return
	}

	if int64(period) <= md.LatestPeriod {
		log.Trace().Msg("Already have sync committees for this period")
		return
	}

	s.catchup(ctx, md)
}

func (s *Service) updateSyncCommitteeForPeriod(ctx context.Context, period uint64) error {
	log.Trace().Uint64("period", period).Msg("Updating sync committee")

	if period < s.chainTime.AltairInitialSyncCommitteePeriod() {
		log.Trace().Uint64("period", period).Msg("period before Altair; nothing to do")
	}

	syncCommitteeResponse, err := s.syncCommitteesProvider.SyncCommittee(ctx, &api.SyncCommitteeOpts{
		State: fmt.Sprintf("%d", s.chainTime.FirstSlotOfEpoch(s.chainTime.FirstEpochOfSyncPeriod(period))),
	})
	if err != nil {
		return errors.Wrap(err, "failed to fetch sync committee")
	}
	syncCommittee := syncCommitteeResponse.Data

	dbSyncCommittee := &chaindb.SyncCommittee{
		Period:    period,
		Committee: syncCommittee.Validators,
	}
	if err := s.syncCommitteesSetter.SetSyncCommittee(ctx, dbSyncCommittee); err != nil {
		return errors.Wrap(err, "failed to set sync committee")
	}

	monitorPeriodProcessed(period)

	return nil
}
