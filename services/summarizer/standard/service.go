// Copyright © 2021, 2023 Weald Technology Limited.
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
	"math"

	eth2client "github.com/attestantio/go-eth2-client"
	"github.com/attestantio/go-eth2-client/api"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
	"github.com/wealdtech/chaind/services/chaindb"
	"github.com/wealdtech/chaind/services/chaintime"
	"github.com/wealdtech/chaind/util"
	"golang.org/x/sync/semaphore"
)

// Service is a summarizer service.
type Service struct {
	eth2Client                      eth2client.Service
	chainDB                         chaindb.Service
	farFutureEpoch                  phase0.Epoch
	proposerDutiesProvider          chaindb.ProposerDutiesProvider
	attestationsProvider            chaindb.AttestationsProvider
	blocksProvider                  chaindb.BlocksProvider
	depositsProvider                chaindb.DepositsProvider
	withdrawalsProvider             chaindb.WithdrawalsProvider
	validatorsProvider              chaindb.ValidatorsProvider
	attesterSlashingsProvider       chaindb.AttesterSlashingsProvider
	proposerSlashingsProvider       chaindb.ProposerSlashingsProvider
	chainTime                       chaintime.Service
	maxTimelyAttestationSourceDelay uint64
	maxTimelyAttestationTargetDelay uint64
	maxTimelyAttestationHeadDelay   uint64
	epochSummaries                  bool
	blockSummaries                  bool
	validatorSummaries              bool
	maxDaysPerRun                   uint64
	validatorRetain                 []phase0.BLSPubKey
	validatorEpochRetention         *util.CalendarDuration
	validatorBalanceRetention       *util.CalendarDuration
	activitySem                     *semaphore.Weighted
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
	log = zerologger.With().Str("service", "summarizer").Str("impl", "standard").Logger().Level(parameters.logLevel)

	if err := registerMetrics(ctx, parameters.monitor); err != nil {
		return nil, errors.New("failed to register metrics")
	}

	proposerDutiesProvider, isProvider := parameters.chainDB.(chaindb.ProposerDutiesProvider)
	if !isProvider {
		return nil, errors.New("chain DB does not provide proposer duties")
	}

	attestationsProvider, isProvider := parameters.chainDB.(chaindb.AttestationsProvider)
	if !isProvider {
		return nil, errors.New("chain DB does not provide attestations")
	}

	blocksProvider, isProvider := parameters.chainDB.(chaindb.BlocksProvider)
	if !isProvider {
		return nil, errors.New("chain DB does not provide blocks")
	}

	depositsProvider, isProvider := parameters.chainDB.(chaindb.DepositsProvider)
	if !isProvider {
		return nil, errors.New("chain DB does not provide deposits")
	}

	withdrawalsProvider, isProvider := parameters.chainDB.(chaindb.WithdrawalsProvider)
	if !isProvider {
		return nil, errors.New("chain DB does not provide withdrawals")
	}

	validatorsProvider, isProvider := parameters.chainDB.(chaindb.ValidatorsProvider)
	if !isProvider {
		return nil, errors.New("chain DB does not provide validators")
	}

	attesterSlashingsProvider, isProvider := parameters.chainDB.(chaindb.AttesterSlashingsProvider)
	if !isProvider {
		return nil, errors.New("chain DB does not provide attester slashings")
	}

	proposerSlashingsProvider, isProvider := parameters.chainDB.(chaindb.ProposerSlashingsProvider)
	if !isProvider {
		return nil, errors.New("chain DB does not provide proposer slashings")
	}

	specResponse, err := parameters.eth2Client.(eth2client.SpecProvider).Spec(ctx, &api.SpecOpts{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to obtain spec")
	}
	spec := specResponse.Data

	tmp, exists := spec["MIN_ATTESTATION_INCLUSION_DELAY"]
	if !exists {
		return nil, errors.New("MIN_ATTESTATION_INCLUSION_DELAY not found in spec")
	}
	minAttestationInclusionDelay, ok := tmp.(uint64)
	if !ok {
		return nil, errors.New("MIN_ATTESTATION_INCLUSION_DELAY of unexpected type")
	}

	tmp, exists = spec["SLOTS_PER_EPOCH"]
	if !exists {
		return nil, errors.New("SLOTS_PER_EPOCH not found in spec")
	}
	slotsPerEpoch, ok := tmp.(uint64)
	if !ok {
		return nil, errors.New("SLOTS_PER_EPOCH of unexpected type")
	}

	var validatorEpochRetention *util.CalendarDuration
	if parameters.validatorEpochRetention != "" {
		validatorEpochRetention, err = util.ParseCalendarDuration(parameters.validatorEpochRetention)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse validator epoch retention")
		}
	}

	var validatorBalanceRetention *util.CalendarDuration
	if parameters.validatorBalanceRetention != "" {
		validatorBalanceRetention, err = util.ParseCalendarDuration(parameters.validatorBalanceRetention)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse validator balance retention")
		}
	}

	s := &Service{
		eth2Client:                      parameters.eth2Client,
		chainDB:                         parameters.chainDB,
		farFutureEpoch:                  phase0.Epoch(0xffffffffffffffff),
		proposerDutiesProvider:          proposerDutiesProvider,
		attestationsProvider:            attestationsProvider,
		blocksProvider:                  blocksProvider,
		depositsProvider:                depositsProvider,
		withdrawalsProvider:             withdrawalsProvider,
		validatorsProvider:              validatorsProvider,
		attesterSlashingsProvider:       attesterSlashingsProvider,
		proposerSlashingsProvider:       proposerSlashingsProvider,
		chainTime:                       parameters.chainTime,
		maxTimelyAttestationSourceDelay: uint64(math.Sqrt(float64(slotsPerEpoch))),
		maxTimelyAttestationTargetDelay: slotsPerEpoch,
		maxTimelyAttestationHeadDelay:   minAttestationInclusionDelay,
		epochSummaries:                  parameters.epochSummaries,
		blockSummaries:                  parameters.blockSummaries,
		validatorSummaries:              parameters.validatorSummaries,
		maxDaysPerRun:                   parameters.maxDaysPerRun,
		validatorRetain:                 parameters.validatorRetain,
		validatorEpochRetention:         validatorEpochRetention,
		validatorBalanceRetention:       validatorBalanceRetention,
		activitySem:                     semaphore.NewWeighted(1),
	}

	// Note the current highest summarized epoch for the monitor.
	md, err := s.getMetadata(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to obtain metadata")
	}
	monitorLatestEpoch(md.LastEpoch)
	monitorLatestDay(md.LastValidatorDay)

	if !md.PeriodicValidatorRollups {
		s.catchup(ctx)
	}

	return s, nil
}

func (s *Service) catchup(ctx context.Context) {
	// Only allow 1 handler to be active.
	acquired := s.activitySem.TryAcquire(1)
	if !acquired {
		log.Debug().Msg("Another handler running")
		return
	}
	defer s.activitySem.Release(1)

	response, err := s.eth2Client.(eth2client.FinalityProvider).Finality(ctx, &api.FinalityOpts{
		State: "head",
	})
	// If we receive an error it could be because the chain hasn't yet started.
	// Even if not, the handler will kick the process off again.
	if err != nil {
		return
	}
	finality := response.Data
	if finality.Finalized.Epoch <= 2 {
		return
	}

	go func(ctx context.Context,
	) {
		if err := s.summarizeValidatorDays(ctx); err != nil {
			log.Error().Err(err).Msg("Failed to summarize validator days")
			return
		}
		md, err := s.getMetadata(ctx)
		if err != nil {
			log.Error().Err(err).Msg("Failed to obtain metadata after initial rollup")
			return
		}
		md.PeriodicValidatorRollups = true
		ctx, cancel, err := s.chainDB.BeginTx(ctx)
		if err != nil {
			log.Error().Err(err).Msg("Failed to begin transaction to set metadata after initial rollup")
		}
		if err := s.setMetadata(ctx, md); err != nil {
			cancel()
			log.Error().Err(err).Msg("Failed to update metadata after initial rollup")
			return
		}
		if err := s.chainDB.CommitTx(ctx); err != nil {
			cancel()
			log.Error().Err(err).Msg("failed to set commit transaction to set metadata after initial rollup")
		}
	}(ctx)
}
