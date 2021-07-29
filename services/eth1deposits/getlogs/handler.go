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
	"encoding/binary"
	"time"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
)

// handleBlocks handles a range of blocks.
func (s *Service) handleBlocks(ctx context.Context, startBlock uint64, endBlock uint64) error {
	logs, err := s.getLogs(ctx, startBlock, endBlock)
	if err != nil {
		return errors.Wrap(err, "failed to obtain logs")
	}

	ctx, cancel, err := s.eth1DepositsSetter.(chaindb.Service).BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}

	for _, logEntry := range logs {
		if len(logEntry.Data) == 0 {
			continue
		}

		tx, err := s.transactionByHash(ctx, logEntry.TransactionHash)
		if err != nil {
			cancel()
			return errors.Wrap(err, "failed to obtain transaction from transaction hash")
		}
		receipt, err := s.transactionReceiptByHash(ctx, logEntry.TransactionHash)
		if err != nil {
			cancel()
			return errors.Wrap(err, "failed to obtain transaction receipt from transaction hash")
		}

		deposit, err := s.depositFromLogEntry(ctx, logEntry, tx, receipt)
		if err != nil {
			cancel()
			return errors.Wrap(err, "failed to obtain ETH1 deposit from log entry")
		}

		if err := s.eth1DepositsSetter.SetETH1Deposit(ctx, deposit); err != nil {
			cancel()
			return errors.Wrap(err, "failed to set ETH1 deposit")
		}
		log.Trace().Uint64("deposit_index", deposit.DepositIndex).Msg("Processed deposit")
	}

	if err := s.eth1DepositsSetter.(chaindb.Service).CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to commit transaction")
	}

	for block := startBlock; block < endBlock; block++ {
		monitorBlockProcessed(block)
	}

	return nil
}

func (s *Service) handleMissed(ctx context.Context, md *metadata) {
	failed := 0
	for i := 0; i < len(md.MissedBlocks); i++ {
		log := log.With().Uint64("block", md.MissedBlocks[i]).Logger()
		// Each update goes in to its own transaction, to make the data available sooner.
		ctx, cancel, err := s.chainDB.BeginTx(ctx)
		if err != nil {
			log.Error().Err(err).Msg("Failed to begin transaction on update after restart")
			return
		}

		if err := s.handleBlocks(ctx, md.MissedBlocks[i], md.MissedBlocks[i]); err != nil {
			log.Warn().Err(err).Msg("Failed to update block")
			failed++
			cancel()
			continue
		} else {
			log.Trace().Msg("Updated block")
			// Remove this from the list of missed blocks.
			missedBlocks := make([]uint64, len(md.MissedBlocks)-1)
			copy(missedBlocks[:failed], md.MissedBlocks[:failed])
			copy(missedBlocks[failed:], md.MissedBlocks[i+1:])
			md.MissedBlocks = missedBlocks
			i--
		}

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
func (s *Service) depositFromLogEntry(ctx context.Context, logEntry *logResponse, tx *transaction, receipt *transactionReceipt) (*chaindb.ETH1Deposit, error) {
	deposit := &chaindb.ETH1Deposit{}
	deposit.ETH1BlockHash = logEntry.BlockHash
	deposit.ETH1BlockNumber = logEntry.BlockNumber
	eth1BlockTimestamp, err := s.blockHashToTime(ctx, logEntry.BlockHash)
	if err != nil {
		return nil, err
	}
	deposit.ETH1BlockTimestamp = eth1BlockTimestamp
	deposit.ETH1TxHash = logEntry.TransactionHash
	deposit.ETH1LogIndex = logEntry.LogIndex
	if receipt != nil {
		deposit.ETH1Sender = receipt.From
		deposit.ETH1Recipient = receipt.To
		deposit.ETH1GasUsed = receipt.GasUsed
	}
	deposit.ETH1GasPrice = tx.GasPrice
	deposit.DepositIndex = binary.LittleEndian.Uint64(logEntry.Data[544:552])
	copy(deposit.ValidatorPubKey[:], logEntry.Data[192:240])
	deposit.WithdrawalCredentials = logEntry.Data[288:320]
	copy(deposit.Signature[:], logEntry.Data[416:512])
	deposit.Amount = phase0.Gwei(binary.LittleEndian.Uint64(logEntry.Data[352:360]))
	return deposit, nil
}

func (s *Service) blockHashToTime(ctx context.Context, blockHash []byte) (time.Time, error) {
	var hash [32]byte
	copy(hash[:], blockHash)
	if blocktime, exists := s.blockTimestamps[hash]; exists {
		return blocktime, nil
	}
	blocktime, err := s.blockTimestampByHash(ctx, blockHash)
	if err != nil {
		return time.Time{}, err
	}
	s.blockTimestamps[hash] = blocktime
	return blocktime, nil
}
