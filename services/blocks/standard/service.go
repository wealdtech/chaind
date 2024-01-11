// Copyright © 2020 - 2022 Weald Technology Trading.
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
	api "github.com/attestantio/go-eth2-client/api/v1"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
	"github.com/wealdtech/chaind/services/chaindb"
	"github.com/wealdtech/chaind/services/chaintime"
	"golang.org/x/sync/semaphore"
)

// Service is a chain database service.
type Service struct {
	eth2Client               eth2client.Service
	chainDB                  chaindb.Service
	blocksSetter             chaindb.BlocksSetter
	attestationsSetter       chaindb.AttestationsSetter
	attesterSlashingsSetter  chaindb.AttesterSlashingsSetter
	proposerSlashingsSetter  chaindb.ProposerSlashingsSetter
	syncAggregateSetter      chaindb.SyncAggregateSetter
	depositsSetter           chaindb.DepositsSetter
	voluntaryExitsSetter     chaindb.VoluntaryExitsSetter
	beaconCommitteesProvider chaindb.BeaconCommitteesProvider
	syncCommitteesProvider   chaindb.SyncCommitteesProvider
	blobSidecarsSetter       chaindb.BlobSidecarsSetter
	chainTime                chaintime.Service
	refetch                  bool
	lastHandledBlockRoot     phase0.Root
	activitySem              *semaphore.Weighted
	syncCommittees           map[uint64]*chaindb.SyncCommittee
}

// module-wide log.
var log zerolog.Logger

// New creates a new service.
func New(ctx context.Context, params ...Parameter) (*Service, error) {
	parameters, err := parseAndCheckParameters(params...)
	if err != nil {
		return nil, errors.Wrap(err, "problem with parameters")
	}

	// Set logging.
	log = zerologger.With().Str("service", "blocks").Str("impl", "standard").Logger().Level(parameters.logLevel)

	if err := registerMetrics(ctx, parameters.monitor); err != nil {
		return nil, errors.New("failed to register metrics")
	}

	blocksSetter, isBlocksSetter := parameters.chainDB.(chaindb.BlocksSetter)
	if !isBlocksSetter {
		return nil, errors.New("chain DB does not support block setting")
	}

	attestationsSetter, isAttestationsSetter := parameters.chainDB.(chaindb.AttestationsSetter)
	if !isAttestationsSetter {
		return nil, errors.New("chain DB does not support attestation setting")
	}

	attesterSlashingsSetter, isAttesterSlashingsSetter := parameters.chainDB.(chaindb.AttesterSlashingsSetter)
	if !isAttesterSlashingsSetter {
		return nil, errors.New("chain DB does not support attester slashing setting")
	}

	proposerSlashingsSetter, isProposerSlashingsSetter := parameters.chainDB.(chaindb.ProposerSlashingsSetter)
	if !isProposerSlashingsSetter {
		return nil, errors.New("chain DB does not support proposer slashing setting")
	}

	syncAggregateSetter, isSyncAggregateSetter := parameters.chainDB.(chaindb.SyncAggregateSetter)
	if !isSyncAggregateSetter {
		return nil, errors.New("chain DB does not support sync aggregate setting")
	}

	depositsSetter, isDepositsSetter := parameters.chainDB.(chaindb.DepositsSetter)
	if !isDepositsSetter {
		return nil, errors.New("chain DB does not support deposits setting")
	}

	voluntaryExitsSetter, isVoluntaryExitsSetter := parameters.chainDB.(chaindb.VoluntaryExitsSetter)
	if !isVoluntaryExitsSetter {
		return nil, errors.New("chain DB does not support voluntary exit setting")
	}

	beaconCommitteesProvider, isBeaconCommitteesProvider := parameters.chainDB.(chaindb.BeaconCommitteesProvider)
	if !isBeaconCommitteesProvider {
		return nil, errors.New("chain DB does not support beacon committee providing")
	}

	syncCommitteesProvider, isSyncCommitteesProvider := parameters.chainDB.(chaindb.SyncCommitteesProvider)
	if !isSyncCommitteesProvider {
		return nil, errors.New("chain DB does not support sync committee providing")
	}

	blobSidecarsSetter, isBlobSidecarsSetter := parameters.chainDB.(chaindb.BlobSidecarsSetter)
	if !isBlobSidecarsSetter {
		return nil, errors.New("chain DB does not support blob sidecar setting")
	}

	s := &Service{
		eth2Client:               parameters.eth2Client,
		chainDB:                  parameters.chainDB,
		blocksSetter:             blocksSetter,
		attestationsSetter:       attestationsSetter,
		attesterSlashingsSetter:  attesterSlashingsSetter,
		proposerSlashingsSetter:  proposerSlashingsSetter,
		syncAggregateSetter:      syncAggregateSetter,
		depositsSetter:           depositsSetter,
		voluntaryExitsSetter:     voluntaryExitsSetter,
		beaconCommitteesProvider: beaconCommitteesProvider,
		syncCommitteesProvider:   syncCommitteesProvider,
		blobSidecarsSetter:       blobSidecarsSetter,
		chainTime:                parameters.chainTime,
		refetch:                  parameters.refetch,
		activitySem:              parameters.activitySem,
		syncCommittees:           make(map[uint64]*chaindb.SyncCommittee),
	}

	// Note the current highest processed block for the monitor.
	md, err := s.getMetadata(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to obtain metadata")
	}
	monitorLatestSlot(phase0.Slot(md.LatestSlot))

	// Update to current epoch before starting (in the background).
	go s.updateAfterRestart(ctx, parameters.startSlot)

	return s, nil
}

func (s *Service) updateAfterRestart(ctx context.Context, startSlot int64) {
	// Only allow 1 handler to be active.
	acquired := s.activitySem.TryAcquire(1)
	if !acquired {
		log.Debug().Msg("Another handler running")
		return
	}
	defer s.activitySem.Release(1)

	// Work out the slot from which to start.
	md, err := s.getMetadata(ctx)
	if err != nil {
		// This will exit so not release the semaphore, but it's exiting so we don't care.
		//nolint:gocritic
		log.Fatal().Err(err).Msg("Failed to obtain metadata before catchup")
	}
	if startSlot >= 0 {
		// Explicit requirement to start at a given slot.
		md.LatestSlot = startSlot - 1
	}

	log.Info().Uint64("slot", uint64(md.LatestSlot)).Msg("Catching up from slot")
	s.catchup(ctx, md)
	log.Info().Msg("Caught up")

	// Set up the handler for new chain head updates.
	if err := s.eth2Client.(eth2client.EventsProvider).Events(ctx, []string{"head"}, func(event *api.Event) {
		if event.Data == nil {
			// Happens when the channel shuts down, nothing to worry about.
			return
		}
		eventData := event.Data.(*api.HeadEvent)
		s.OnBeaconChainHeadUpdated(ctx, eventData.Slot, eventData.Block, eventData.State, eventData.EpochTransition)
	}); err != nil {
		log.Fatal().Err(err).Msg("Failed to add beacon chain head updated handler")
	}
}
