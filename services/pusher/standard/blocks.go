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
	"fmt"
	"net/http"

	"github.com/wealdtech/chaind/services/chaindb"
)

type blockData struct {
	Slot          string `json:"slot"`
	ProposerIndex string `json:"proposer_index"`
}

func (s *Service) handleBlocks(w http.ResponseWriter, r *http.Request) {
	// Upgrade initial GET request to a websocket
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Warn().Err(err).Msg("Failed to upgrade connection to websocket")
	}

	// Obtain initial data.
	opCtx, cancel := context.WithTimeout(context.Background(), s.timeout)
	blocks, err := s.chainDB.(chaindb.BlocksProvider).BlocksForSlotRange(opCtx, 0, 99999999999)
	cancel()
	if err != nil {
		log.Error().Err(err).Msg("Failed to obtain blocks")
		ws.Close()
		return
	}

	// Build initial data.
	msg := make([]*blockData, len(blocks))

	for i, block := range blocks {
		msg[i] = &blockData{
			Slot:          fmt.Sprintf("%d", block.Slot),
			ProposerIndex: fmt.Sprintf("%d", block.ProposerIndex),
		}
	}

	// Send initial data.
	if err := ws.WriteJSON(msg); err != nil {
		log.Error().Err(err).Msg("Failed to send initial block data")
		ws.Close()
		return
	}

	// Register for updates.
	s.clients[ws] = &client{
		topics: map[string]bool{
			"blocks": true,
		},
	}
}
