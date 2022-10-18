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

package postgresql

import (
	"context"
	"time"

	api "github.com/attestantio/go-eth2-client/api/v1"
	"github.com/pkg/errors"
)

// SetGenesis sets the genesis information.
func (s *Service) SetGenesis(ctx context.Context, genesis *api.Genesis) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.Exec(ctx, `
      INSERT INTO t_genesis(f_validators_root
                           ,f_time
                           ,f_fork_version)
      VALUES($1,$2,$3)
      ON CONFLICT (f_validators_root) DO
      UPDATE
      SET f_time = excluded.f_time
         ,f_fork_version = excluded.f_fork_version
      `,
		genesis.GenesisValidatorsRoot[:],
		genesis.GenesisTime,
		genesis.GenesisForkVersion[:],
	)

	return err
}

// Genesis fetches genesis values.
func (s *Service) Genesis(ctx context.Context) (*api.Genesis, error) {
	tx := s.tx(ctx)
	if tx == nil {
		ctx, err := s.beginROTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		defer s.commitROTx(ctx)
		tx = s.tx(ctx)
	}

	genesis := &api.Genesis{}
	var genesisValidatorsRoot []byte
	var genesisForkVersion []byte
	err := tx.QueryRow(ctx, `
      SELECT f_validators_root
            ,f_time
            ,f_fork_version
      FROM t_genesis
	  `).Scan(
		&genesisValidatorsRoot,
		&genesis.GenesisTime,
		&genesisForkVersion,
	)
	if err != nil {
		return nil, err
	}
	copy(genesis.GenesisValidatorsRoot[:], genesisValidatorsRoot)
	copy(genesis.GenesisForkVersion[:], genesisForkVersion)

	return genesis, nil
}

// GenesisTime provides the genesis time of the chain.
func (s *Service) GenesisTime(ctx context.Context) (time.Time, error) {
	genesis, err := s.Genesis(ctx)
	if err != nil {
		return time.Time{}, errors.Wrap(err, "failed to obtain genesis")
	}
	return genesis.GenesisTime, nil
}
