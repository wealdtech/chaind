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
	"net/http"
)

// startDameon starts the daemon.
func (s *Service) startDaemon(ctx context.Context) error {
	// Data requests.
	http.HandleFunc("/blocks", s.handleBlocks)

	// Start the server on localhost port 8000 and log any errors
	go func() {
		log.Info().Str("listen_address", s.listenAddress).Msg("http server starting")
		err := http.ListenAndServe(s.listenAddress, nil)
		if err != nil {
			log.Fatal().Err(err).Msg("ListenAndServe")
		}
	}()

	return nil
}
