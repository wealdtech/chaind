// Copyright © 2021 Weald Technology Limited.
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
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
	"github.com/wealdtech/chaind/services/blocks"
	"github.com/wealdtech/chaind/services/chaindb"
	"github.com/wealdtech/chaind/services/chaintime"
	"golang.org/x/sync/semaphore"
)

// Service is a summarizer service.
type Service struct {
	eth2Client             eth2client.Service
	chainDB                chaindb.Service
	proposerDutiesProvider chaindb.ProposerDutiesProvider
	attestationsProvider   chaindb.AttestationsProvider
	blocksProvider         chaindb.BlocksProvider
	blocksSetter           chaindb.BlocksSetter
	chainTime              chaintime.Service
	blocks                 blocks.Service
	activitySem            *semaphore.Weighted
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
		return nil, errors.New("chain DB does not provider proposer duties")
	}

	attestationsProvider, isProvider := parameters.chainDB.(chaindb.AttestationsProvider)
	if !isProvider {
		return nil, errors.New("chain DB does not provider attestations")
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
		eth2Client:             parameters.eth2Client,
		chainDB:                parameters.chainDB,
		proposerDutiesProvider: proposerDutiesProvider,
		attestationsProvider:   attestationsProvider,
		blocksProvider:         blocksProvider,
		blocksSetter:           blocksSetter,
		chainTime:              parameters.chainTime,
		blocks:                 parameters.blocks,
		activitySem:            semaphore.NewWeighted(1),
	}

	go s.updateOnRestart(ctx)

	return s, nil
}

func (s *Service) updateOnRestart(ctx context.Context) {

	s.OnFinalityUpdated(ctx, 32)
}
