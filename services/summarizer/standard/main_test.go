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

package standard_test

import (
	"flag"
	"os"
	"testing"

	"github.com/rs/zerolog"
)

func TestMain(m *testing.M) {
	zerolog.SetGlobalLevel(zerolog.Disabled)
	// `package standard` (internal) and `package standard_test` (external) tests
	// share a single test binary, so this TestMain governs both.  When the env
	// vars required by service_test.go's TestService are absent we skip that
	// test by name and still call m.Run(), so internal-package unit tests like
	// the lag-gauge wiring tests run unconditionally under `go test ./...`.
	if os.Getenv("CHAINDB_URL") == "" || os.Getenv("ETH2CLIENT_ADDRESS") == "" {
		_ = flag.Set("test.skip", "^TestService$")
	}
	os.Exit(m.Run())
}
