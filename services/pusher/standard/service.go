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
	"time"

	"github.com/gorilla/websocket"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	zerologger "github.com/rs/zerolog/log"
	"github.com/wealdtech/chaind/services/chaindb"
	"github.com/wealdtech/chaind/services/pusher"
)

// Service is a pusher service.
type Service struct {
	chainDB       chaindb.Service
	listenAddress string
	timeout       time.Duration
	clients       map[*websocket.Conn]*client
	broadcast     chan *pusher.Message
}

// module-wide log.
var log zerolog.Logger

type client struct {
	topics map[string]bool
}

// New creates a new service.
func New(ctx context.Context, params ...Parameter) (*Service, error) {
	parameters, err := parseAndCheckParameters(params...)
	if err != nil {
		return nil, errors.Wrap(err, "problem with parameters")
	}

	// Set logging.
	log = zerologger.With().Str("service", "pusher").Str("impl", "standard").Logger().Level(parameters.logLevel)

	if err := registerMetrics(ctx, parameters.monitor); err != nil {
		return nil, errors.New("failed to register metrics")
	}

	s := &Service{
		chainDB:       parameters.chainDB,
		listenAddress: parameters.listenAddress,
		timeout:       parameters.timeout,
		clients:       make(map[*websocket.Conn]*client),
		broadcast:     make(chan *pusher.Message),
	}

	// Start the distributor.
	go s.distributeMessages(ctx)

	if err := s.startDaemon(ctx); err != nil {
		log.Error().Err(err).Msg("Failed to start daemon")
	}

	return s, nil
}
