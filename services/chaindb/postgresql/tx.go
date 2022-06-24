// Copyright © 2020, 2022 Weald Technology Trading.
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
	"math/rand"
	"time"

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

// TxID is a context tag for the transaction TxID.
type TxID struct{}

func init() {
	// We seed math.rand here so that we can obtain different IDs for requests.
	// This is purely used as a way to match request and response entries in logs, so there is no
	// requirement for this to cryptographically secure.
	rand.Seed(time.Now().UnixNano())
}

// BeginTx begins a transaction on the database.
// The transaction can be rolled back by invoking the cancel function.
func (s *Service) BeginTx(ctx context.Context) (context.Context, context.CancelFunc, error) {
	// #nosec G404
	id := fmt.Sprintf("%02x", rand.Int31())
	log := log.With().Str("id", id).Logger()

	ctx, cancel := context.WithCancel(ctx)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		log.Trace().Err(err).Str("trace", fmt.Sprintf("+%v", errors.Wrap(err, "stack"))).Msg("Failed to begin transaction")
		cancel()
		return nil, nil, errors.Wrap(err, "failed to begin transaction")
	}

	ctx = context.WithValue(ctx, &Tx{}, tx)
	ctx = context.WithValue(ctx, &TxID{}, id)

	log.Trace().Str("trace", fmt.Sprintf("%+v", errors.New("stack"))).Msg("Transaction started")
	return ctx, func() {
		if err := tx.Rollback(ctx); err != nil {
			log.Debug().Err(err).Str("trace", fmt.Sprintf("%+v", errors.Wrap(err, "stack"))).Msg("Failed to rollback transaction")
			log.Warn().Err(err).Msg("Failed to rollback transaction")
		}
		log.Debug().Str("trace", fmt.Sprintf("%+v", errors.New("stack"))).Msg("Rolled back transaction")
		cancel()
	}, nil
}

// beginROTx begins a read-only transaction on the database.
// The transaction should be committed.
func (s *Service) beginROTx(ctx context.Context) (context.Context, error) {
	// #nosec G404
	id := fmt.Sprintf("%02x", rand.Int31())
	log := log.With().Str("id", id).Logger()

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		log.Trace().Err(err).Str("trace", fmt.Sprintf("+%v", errors.Wrap(err, "stack"))).Msg("Failed to begin read-only transaction")
		return nil, errors.Wrap(err, "failed to begin read-only transaction")
	}

	ctx = context.WithValue(ctx, &Tx{}, tx)
	ctx = context.WithValue(ctx, &TxID{}, id)

	log.Trace().Str("trace", fmt.Sprintf("%+v", errors.New("stack"))).Msg("Read-only transaction started")
	return ctx, nil
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

// txID returns the transaction ID; "<unknown>" string if no transaction
func (s *Service) txID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}

	if txID, ok := ctx.Value(&TxID{}).(string); ok {
		return txID
	}
	return "<unknown>"
}

// CommitTx commits a transaction on the ops datastore.
func (s *Service) CommitTx(ctx context.Context) error {
	log := log.With().Str("id", s.txID(ctx)).Logger()

	if ctx == nil {
		log.Debug().Str("trace", fmt.Sprintf("%+v", errors.New("stack"))).Msg("No context")
		return errors.New("no context")
	}

	tx, ok := ctx.Value(&Tx{}).(pgx.Tx)
	if !ok {
		log.Debug().Str("trace", fmt.Sprintf("%+v", errors.New("stack"))).Msg("No transaction")
		return errors.New("no transaction")
	}

	err := tx.Commit(ctx)
	if err != nil {
		log.Debug().Err(err).Str("trace", fmt.Sprintf("%+v", errors.Wrap(err, "stack"))).Msg("Failed to commit")
		return err
	}

	log.Trace().Str("trace", fmt.Sprintf("%+v", errors.New("stack"))).Msg("Transaction committed")
	return nil
}

// commitROTx commits a read-only transaction on the ops datastore.
func (s *Service) commitROTx(ctx context.Context) {
	log := log.With().Str("id", s.txID(ctx)).Logger()

	if ctx == nil {
		log.Debug().Str("trace", fmt.Sprintf("%+v", errors.New("stack"))).Msg("No context")
		return
	}

	tx, ok := ctx.Value(&Tx{}).(pgx.Tx)
	if !ok {
		log.Debug().Str("trace", fmt.Sprintf("%+v", errors.New("stack"))).Msg("No transaction")
		return
	}

	err := tx.Commit(ctx)
	if err != nil {
		log.Debug().Err(err).Str("trace", fmt.Sprintf("%+v", errors.Wrap(err, "stack"))).Msg("Failed to commit")
		return
	}

	log.Trace().Str("trace", fmt.Sprintf("%+v", errors.New("stack"))).Msg("Transaction committed")
}
