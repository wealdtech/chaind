// Copyright © 2024 Weald Technology Trading.
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

package postgresql

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaintime"
	standardchaintime "github.com/wealdtech/chaind/services/chaintime/standard"
)

func (s *Service) startPartitioningTicker(ctx context.Context) error {
	chainTime, err := standardchaintime.New(ctx,
		standardchaintime.WithGenesisProvider(s),
		standardchaintime.WithSpecProvider(s),
		standardchaintime.WithForkScheduleProvider(s),
	)
	if err != nil {
		return errors.Wrap(err, "failed to start chaintime service for partitioning")
	}

	go func(s *Service,
		ctx context.Context,
		chainTime chaintime.Service,
	) {
		// Start with an immediate update.
		s.updatePartitions(ctx, chainTime)
		// Set up a ticker for future updates.
		ticker := time.NewTicker(4 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.updatePartitions(ctx, chainTime)
			case <-ctx.Done():
				return
			}
		}
	}(s, ctx, chainTime)

	return nil
}

func (s *Service) updatePartitions(ctx context.Context,
	chainTime chaintime.Service,
) {
	today := time.Now().In(time.UTC)
	today = time.Date(today.Year(), today.Month(), today.Day(), 0, 0, 0, 0, time.UTC)
	if err := s.createTablePartitions(ctx, chainTime, today); err != nil {
		log.Error().Err(err).Time("date", today).Msg("Failed to create table partitions for current date")
	}

	tomorrow := today.AddDate(0, 0, 1)
	if err := s.createTablePartitions(ctx, chainTime, tomorrow); err != nil {
		log.Error().Err(err).Time("date", today).Msg("Failed to create table partitions for future date")
	}
}

func (s *Service) createTablePartitions(ctx context.Context,
	chainTime chaintime.Service,
	start time.Time,
) error {
	suffix := fmt.Sprintf("%04d_%02d_%02d", start.Year(), start.Month(), start.Day())

	startEpoch := chainTime.TimestampToEpoch(start)

	end := start.AddDate(0, 0, 1)
	endEpoch := chainTime.TimestampToEpoch(end)
	endEpochStart := chainTime.StartOfEpoch(endEpoch)
	if endEpochStart.Before(end) {
		endEpoch--
	}

	ctx, cancel, err := s.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin create table partitions transaction")
	}

	tx := s.tx(ctx)
	if tx == nil {
		cancel()
		return ErrNoTransaction
	}

	if _, err := tx.Exec(ctx,
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS t_validator_balances_%s
PARTITION OF t_validator_balances
FOR VALUES FROM (%d) TO (%d)
`, suffix,
			startEpoch,
			endEpoch,
		)); err != nil {
		cancel()
		return errors.Wrap(err, "failed to create validator balances partition")
	}

	if _, err := tx.Exec(ctx,
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS t_validator_epoch_summaries_%s
PARTITION OF t_validator_epoch_summaries
FOR VALUES FROM (%d) TO (%d)
`, suffix,
			startEpoch,
			endEpoch,
		)); err != nil {
		cancel()
		return errors.Wrap(err, "failed to create validator epoch summaries partition")
	}

	if err := s.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to commit create table partitions transaction")
	}

	return nil
}
