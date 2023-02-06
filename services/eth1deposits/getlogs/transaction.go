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

package getlogs

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type transaction struct {
	GasPrice uint64
}

//nolint:tagliatelle
type transactionJSON struct {
	GasPrice string `json:"gasPrice"`
}

// UnmarshalJSON implements json.Unmarshaler.
func (t *transaction) UnmarshalJSON(input []byte) error {
	var transactionJSON transactionJSON
	var err error
	if err := json.Unmarshal(input, &transactionJSON); err != nil {
		return errors.Wrap(err, "invalid JSON")
	}

	if transactionJSON.GasPrice == "" {
		return errors.New("gas price missing")
	}
	t.GasPrice, err = strconv.ParseUint(strings.TrimPrefix(transactionJSON.GasPrice, "0x"), 16, 64)
	if err != nil {
		return errors.Wrap(err, "invalid format for gas price")
	}

	return nil
}

// MarshalJSON implements json.Marshaler.
func (t *transaction) MarshalJSON() ([]byte, error) {
	return json.Marshal(&transactionJSON{
		GasPrice: fmt.Sprintf("%#x", t.GasPrice),
	})
}

// String returns a string version of the structure.
func (t *transaction) String() string {
	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Sprintf("ERR: %v", err)
	}
	return string(data)
}
