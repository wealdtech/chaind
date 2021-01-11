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

	eth2client "github.com/attestantio/go-eth2-client"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
	"github.com/wealdtech/chaind/services/chaindb"
)

// Service is a spec service.
type Service struct {
	eth2Client      eth2client.Service
	chainDB         chaindb.Service
	chainSpecSetter chaindb.ChainSpecSetter
	genesisSetter   chaindb.GenesisSetter
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
	log = zerologger.With().Str("service", "spec").Str("impl", "standard").Logger()
	if parameters.logLevel != log.GetLevel() {
		log = log.Level(parameters.logLevel)
	}

	chainSpecSetter, isChainSpecSetter := parameters.chainDB.(chaindb.ChainSpecSetter)
	if !isChainSpecSetter {
		return nil, errors.New("chain DB does not support chain spec setting")
	}

	genesisSetter, isGenesisSetter := parameters.chainDB.(chaindb.GenesisSetter)
	if !isGenesisSetter {
		return nil, errors.New("chain DB does not support genesis setting")
	}

	s := &Service{
		eth2Client:      parameters.eth2Client,
		chainDB:         parameters.chainDB,
		chainSpecSetter: chainSpecSetter,
		genesisSetter:   genesisSetter,
	}

	// Update spec in the background
	go s.updateAfterRestart(ctx)

	return s, nil
}

func (s *Service) updateAfterRestart(ctx context.Context) {
	ctx, cancel, err := s.chainDB.BeginTx(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to begin transaction")
		return
	}

	if err := s.updateChainSpec(ctx); err != nil {
		log.Fatal().Err(err).Msg("Failed to update spec")
	}

	if err := s.updateGenesis(ctx); err != nil {
		log.Fatal().Err(err).Msg("Failed to update genesis")
	}

	if err := s.chainDB.CommitTx(ctx); err != nil {
		cancel()
		log.Fatal().Err(err).Msg("Failed to commit transaction")
	}

}

func (s *Service) updateChainSpec(ctx context.Context) error {
	// Fetch the chain spec.
	spec, err := s.eth2Client.(eth2client.SpecProvider).Spec(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to obtain chain spec")
	}

	// Update the database.
	for k, v := range spec {
		if err := s.chainSpecSetter.SetChainSpecValue(ctx, k, v); err != nil {
			return errors.Wrap(err, "failed to set chain spec value")
		}
	}
	return nil
}

func (s *Service) updateGenesis(ctx context.Context) error {
	// Fetch genesis parameters.
	genesis, err := s.eth2Client.(eth2client.GenesisProvider).Genesis(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to obtain genesis")
	}

	// Update the database.
	if err := s.genesisSetter.SetGenesis(ctx, genesis); err != nil {
		return errors.Wrap(err, "failed to set genesis")
	}

	return nil
}
