// Copyright © 2021 - 2024 Weald Technology Trading.
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
	"time"

	eth2client "github.com/attestantio/go-eth2-client"
	"github.com/attestantio/go-eth2-client/api"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
	"github.com/wealdtech/chaind/handlers"
	"github.com/wealdtech/chaind/services/blocks"
	"github.com/wealdtech/chaind/services/chaindb"
	"github.com/wealdtech/chaind/services/chaintime"
	"golang.org/x/sync/semaphore"
)

// Service is a finalizer service.
type Service struct {
	log              zerolog.Logger
	eth2Client       eth2client.Service
	chainDB          chaindb.Service
	blocksProvider   chaindb.BlocksProvider
	blocksSetter     chaindb.BlocksSetter
	chainTime        chaintime.Service
	blocks           blocks.Service
	finalityHandlers []handlers.FinalityHandler
	activitySem      *semaphore.Weighted
}

// New creates a new service.
func New(ctx context.Context, params ...Parameter) (*Service, error) {
	parameters, err := parseAndCheckParameters(params...)
	if err != nil {
		return nil, errors.Wrap(err, "problem with parameters")
	}

	// Set logging.
	log := zerologger.With().Str("service", "finalizer").Str("impl", "standard").Logger().Level(parameters.logLevel)

	if err := registerMetrics(ctx, parameters.monitor); err != nil {
		return nil, errors.New("failed to register metrics")
	}

	blocksProvider, isBlocksProvider := parameters.chainDB.(chaindb.BlocksProvider)
	if !isBlocksProvider {
		return nil, errors.New("chain DB does not support block providing")
	}

	blocksSetter, isBlocksSetter := parameters.chainDB.(chaindb.BlocksSetter)
	if !isBlocksSetter {
		return nil, errors.New("chain DB does not support block setting")
	}

	s := &Service{
		log:              log,
		eth2Client:       parameters.eth2Client,
		chainDB:          parameters.chainDB,
		blocksProvider:   blocksProvider,
		blocksSetter:     blocksSetter,
		chainTime:        parameters.chainTime,
		blocks:           parameters.blocks,
		finalityHandlers: parameters.finalityHandlers,
		activitySem:      parameters.activitySem,
	}

	if err := parameters.scheduler.SchedulePeriodicJob(ctx,
		"Finalizer",
		"Update finalization",
		s.scheduleFinalizer,
		nil,
		s.runFinalizer,
		nil,
	); err != nil {
		return nil, errors.Wrap(err, "failed to schedule finalizer")
	}

	// Note the current highest finalized epoch for the monitor.
	md, err := s.getMetadata(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to obtain metadata")
	}
	if md.LastFinalizedEpoch > -1 {
		monitorLatestEpoch(phase0.Epoch(md.LastFinalizedEpoch))
	}

	return s, nil
}

func (*Service) scheduleFinalizer(_ context.Context, _ any) (time.Time, error) {
	return time.Now().Add(time.Minute), nil
}

func (s *Service) runFinalizer(ctx context.Context, _ any) {
	finalityResponse, err := s.eth2Client.(eth2client.FinalityProvider).Finality(ctx, &api.FinalityOpts{
		State: "head",
	})
	if err != nil {
		s.log.Error().Err(err).Msg("Failed to obtain finality data")
	}
	finality := finalityResponse.Data

	s.Finalize(ctx, finality)
}
