// Copyright © 2020 Weald Technology Trading.
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

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

// SetMetadata sets a metadata key to a JSON value.
func (s *Service) SetMetadata(ctx context.Context, key string, value []byte) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	_, err := tx.Exec(ctx, `
      INSERT INTO t_metadata(f_key
                            ,f_value)
      VALUES($1,$2)
      ON CONFLICT (f_key) DO
      UPDATE
      SET f_value = excluded.f_value`,
		key,
		value,
	)

	return err
}

// Metadata obtains the JSON value from a metadata key.
func (s *Service) Metadata(ctx context.Context, key string) ([]byte, error) {
	var err error

	tx := s.tx(ctx)
	if tx == nil {
		ctx, err = s.BeginROTx(ctx)
		if err != nil {
			return nil, err
		}
		tx = s.tx(ctx)
		defer s.CommitROTx(ctx)
	}

	res := &pgtype.JSONB{}
	err = tx.QueryRow(ctx, `
      SELECT f_value
      FROM t_metadata
      WHERE f_key = $1`,
		key).Scan(
		res,
	)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, errors.Wrap(err, "failed to obtain metadata")
	}

	return res.Bytes, nil
}
