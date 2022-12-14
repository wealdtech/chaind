// Copyright © 2020 Weald Technology Limited.
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

package postgresql_test

import (
	"os"
	"strconv"
	"testing"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/rs/zerolog"
)

func TestMain(m *testing.M) {
	zerolog.SetGlobalLevel(zerolog.Disabled)
	if os.Getenv("CHAINDB_URL") != "" ||
		os.Getenv("CHAINDB_SERVER") != "" {
		os.Exit(m.Run())
	}
}

func slotPtr(input phase0.Slot) *phase0.Slot {
	return &input
}

func atoi(input string) int32 {
	val, err := strconv.ParseInt(input, 10, 32)
	if err != nil {
		panic(err)
	}
	return int32(val)
}
