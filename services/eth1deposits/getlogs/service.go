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

package getlogs

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
	"github.com/wealdtech/chaind/services/chaindb"
	"golang.org/x/sync/semaphore"
)

// module-wide log.
var log zerolog.Logger

// Service is an Ethereum 1 deposits service that fetches deposits through fetching logs.
type Service struct {
	chainDB                chaindb.Service
	base                   *url.URL
	client                 *http.Client
	eth1DepositsSetter     chaindb.ETH1DepositsSetter
	eth1Confirmations      uint64
	blockTimestamps        map[[32]byte]time.Time
	blocksPerRequest       uint64
	depositContractAddress []byte
	activitySem            *semaphore.Weighted
}

// New creates a new Ethereum 1 deposit service.
func New(ctx context.Context, params ...Parameter) (*Service, error) {
	parameters, err := parseAndCheckParameters(params...)
	if err != nil {
		return nil, errors.Wrap(err, "problem with parameters")
	}

	// Set logging.
	log = zerologger.With().Str("service", "eth1deposits").Str("impl", "getlogs").Logger()
	if parameters.logLevel != log.GetLevel() {
		log = log.Level(parameters.logLevel)
	}

	if err := registerMetrics(ctx, parameters.monitor); err != nil {
		return nil, errors.New("failed to register metrics")
	}

	// Connect to Ethereum 1.
	connectionURL := parameters.connectionURL
	if !strings.HasPrefix(connectionURL, "http") {
		connectionURL = fmt.Sprintf("http://%s", parameters.connectionURL)
	}
	base, err := url.Parse(connectionURL)
	if err != nil {
		return nil, errors.Wrap(err, "invalid URL")
	}

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).DialContext,
			MaxIdleConns:        64,
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     384 * time.Second,
		},
	}

	spec, err := parameters.chainDB.(chaindb.ChainSpecProvider).ChainSpec(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to obtain chain specification")
	}

	depositContractAddress, exists := spec["DEPOSIT_CONTRACT_ADDRESS"].([]byte)
	if !exists {
		return nil, errors.New("failed to obtain deposit contract address")
	}

	s := &Service{
		chainDB:                parameters.chainDB,
		eth1DepositsSetter:     parameters.eth1DepositsSetter,
		base:                   base,
		client:                 client,
		eth1Confirmations:      parameters.eth1Confirmations,
		blockTimestamps:        make(map[[32]byte]time.Time),
		blocksPerRequest:       64,
		depositContractAddress: depositContractAddress,
		activitySem:            semaphore.NewWeighted(1),
	}

	chainID, err := s.chainID(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to obtain Ethereum 1 chain ID")
	}
	if chainID == 0 {
		log.Warn().Msg("Ethereum 1 client not synced; cannot confirm chain ID")
	} else {
		depositChainID, exists := spec["DEPOSIT_CHAIN_ID"].(uint64)
		if !exists {
			return nil, errors.Wrap(err, "failed to obtain deposit contract chain ID")
		}
		if chainID != depositChainID {
			return nil, fmt.Errorf("incorrect Ethereum 1 client chain ID %d", chainID)
		}
	}

	startBlock, err := strconv.ParseInt(parameters.startBlock, 10, 64)
	if err != nil {
		startBlock = -1
	}

	go s.updateAfterRestart(ctx, startBlock)

	return s, nil
}

func (s *Service) updateAfterRestart(ctx context.Context, startBlock int64) {
	// Work out the block from which to start.
	md, err := s.getMetadata(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to obtain metadata before catchup")
	}
	if startBlock >= 0 {
		// Explicit requirement to start at a given block.
		if startBlock > 0 {
			md.LatestBlock = uint64(startBlock - 1)
		} else {
			md.LatestBlock = 0
		}
	}
	log.Info().Uint64("block", md.LatestBlock).Msg("Last processed block")

	s.parseNewBlocks(ctx, md)
	if len(md.MissedBlocks) > 0 {
		log.Info().Uints64("missed_blocks", md.MissedBlocks).Msg("Re-fetching missed blocks")
		s.handleMissed(ctx, md)
		log.Info().Uint64("block", md.LatestBlock).Msg("Catching up from block")
		s.parseNewBlocks(ctx, md)
	}
	log.Info().Msg("Caught up")

	// Run periodically.
	go func(ctx context.Context, s *Service) {
		for {
			select {
			case <-time.After(2 * time.Minute):
				s.checkLatestBlock(ctx)
			case <-ctx.Done():
				log.Debug().Msg("Context done")
				break
			}
		}
	}(ctx, s)
}

func (s *Service) getLatestHeadBlock(ctx context.Context) (uint64, error) {
	head, err := s.blockNumber(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "failed to obtain block number")
	}
	if head > s.eth1Confirmations {
		return head - s.eth1Confirmations, nil
	}
	return 0, nil
}

func (s *Service) checkLatestBlock(ctx context.Context) {
	// Work out the block from which to start.
	md, err := s.getMetadata(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to obtain metadata before catchup")
	}
	s.parseNewBlocks(ctx, md)
}

func (s *Service) parseNewBlocks(ctx context.Context, md *metadata) {
	// Only allow 1 handler to be active.
	acquired := s.activitySem.TryAcquire(1)
	if !acquired {
		log.Debug().Msg("Another handler running")
		return
	}
	defer s.activitySem.Release(1)

	latestHeadBlock, err := s.getLatestHeadBlock(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to obtain latest head block")
		return
	}

	if latestHeadBlock == 0 {
		log.Debug().Msg("Ethereum 1 node is syncing; not fetching blocks")
		return
	}

	log.Trace().Uint64("start_block", md.LatestBlock+1).Uint64("end_block", latestHeadBlock).Msg("Fetching ETH1 logs in batches")
	for block := md.LatestBlock + 1; block <= latestHeadBlock; block += s.blocksPerRequest {
		startBlock := block
		endBlock := block + s.blocksPerRequest - 1
		if endBlock > latestHeadBlock {
			endBlock = latestHeadBlock
		}

		log := log.With().Uint64("start_block", startBlock).Uint64("end_block", endBlock).Logger()
		// Each update goes in to its own transaction, to make the data available sooner.
		ctx, cancel, err := s.chainDB.BeginTx(ctx)
		if err != nil {
			log.Error().Err(err).Msg("Failed to begin transaction on update after restart")
			return
		}

		if err := s.handleBlocks(ctx, startBlock, endBlock); err != nil {
			log.Warn().Err(err).Msg("Failed to update ETH1 deposits")
			for missedBlock := block; missedBlock <= endBlock; missedBlock++ {
				md.MissedBlocks = append(md.MissedBlocks, missedBlock)
			}
		}

		md.LatestBlock = endBlock
		if err := s.setMetadata(ctx, md); err != nil {
			log.Error().Err(err).Msg("Failed to set metadata")
			cancel()
			return
		}

		if err := s.chainDB.CommitTx(ctx); err != nil {
			log.Error().Err(err).Msg("Failed to commit transaction")
			cancel()
			return
		}
	}
}
