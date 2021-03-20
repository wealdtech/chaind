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

	"github.com/wealdtech/chaind/services/pusher"
)

// Push pushes generic data.
// We push the message on to the broadcast queue.
func (s *Service) Push(ctx context.Context, msg *pusher.Message) {
	// Any validation?
	s.broadcast <- msg
	messageReceived()
}

// distributeMessages distributes messages to clients.
func (s *Service) distributeMessages(ctx context.Context) {
	for {
		msg := <-s.broadcast
		log.Trace().Str("topic", msg.Topic).Str("data", msg.Data).Msg("Message to distribute")
		for conn, client := range s.clients {
			// Ensure this client is interested in our message.
			if _, exists := client.topics[msg.Topic]; !exists {
				log.Info().Msg("No topic match; skipping")
				continue
			}

			// Use goroutines?
			err := conn.WriteJSON(msg)
			if err != nil {
				log.Debug().Err(err).Msg("Failed to write to client; disconnecting")
				conn.Close()
				delete(s.clients, conn)
			}
		}
	}
}
