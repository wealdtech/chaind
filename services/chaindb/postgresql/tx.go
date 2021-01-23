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

	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

var (
	// ErrNoTransaction is returned when an attempt to carry out a mutation to the database
	// is not inside a transaction.
	ErrNoTransaction = errors.New("no transaction for action")
)

// Tx is a context tag for the database transaction.
type Tx struct{}

// BeginTx begins a transaction on the database.
// The transaction can be rolled back by invoking the cancel function.
func (s *Service) BeginTx(ctx context.Context) (context.Context, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(ctx)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		cancel()
		return nil, nil, errors.Wrap(err, "failed to begin transaction")
	}
	ctx = context.WithValue(ctx, &Tx{}, tx)
	return ctx, func() {
		if err := tx.Rollback(ctx); err != nil {
			log.Warn().Err(err).Msg("Failed to rollback transaction")
		}
		cancel()
	}, nil
}

// hasTx returns true if the context has a transaction.
func (s *Service) hasTx(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	_, ok := ctx.Value(&Tx{}).(pgx.Tx)
	return ok
}

// tx returns the transaction; nil if no transaction
func (s *Service) tx(ctx context.Context) pgx.Tx {
	if ctx == nil {
		return nil
	}

	if tx, ok := ctx.Value(&Tx{}).(pgx.Tx); ok {
		return tx
	}
	return nil
}

// CommitTx commits a transaction on the ops datastore.
func (s *Service) CommitTx(ctx context.Context) error {
	if ctx == nil {
		return errors.New("no context")
	}

	tx, ok := ctx.Value(&Tx{}).(pgx.Tx)
	if !ok {
		return errors.New("no transaction")
	}
	return tx.Commit(ctx)
}
