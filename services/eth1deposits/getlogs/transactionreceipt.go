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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type transactionReceipt struct {
	BlockHash         []byte
	BlockNumber       uint64
	ContractAddress   []byte
	From              []byte
	To                []byte
	CumulativeGasUsed uint64
	GasUsed           uint64
	Logs              []*logResponse
	// LogsBloom etc.
}

//nolint:tagliatelle
type transactionReceiptJSON struct {
	BlockHash         string         `json:"blockHash"`
	BlockNumber       string         `json:"blockNumber"`
	ContractAddress   string         `json:"contractAddress"`
	From              string         `json:"from"`
	To                string         `json:"to"`
	CumulativeGasUsed string         `json:"cumulativeGasUsed"`
	GasUsed           string         `json:"gasUsed"`
	Logs              []*logResponse `json:"logs"`
}

// UnmarshalJSON implements json.Unmarshaler.
func (t *transactionReceipt) UnmarshalJSON(input []byte) error {
	var transactionReceiptJSON transactionReceiptJSON
	var err error
	if err := json.Unmarshal(input, &transactionReceiptJSON); err != nil {
		return errors.Wrap(err, "invalid JSON")
	}

	if transactionReceiptJSON.BlockHash == "" {
		return errors.New("block hash missing")
	}
	t.BlockHash, err = hex.DecodeString(strings.TrimPrefix(transactionReceiptJSON.BlockHash, "0x"))
	if err != nil {
		return errors.Wrap(err, "invalid value for block hash")
	}
	if transactionReceiptJSON.BlockNumber == "" {
		return errors.New("block number missing")
	}
	t.BlockNumber, err = strconv.ParseUint(strings.TrimPrefix(transactionReceiptJSON.BlockNumber, "0x"), 16, 64)
	if err != nil {
		return errors.Wrap(err, "invalid format for block number")
	}
	if transactionReceiptJSON.ContractAddress != "" {
		t.ContractAddress, err = hex.DecodeString(strings.TrimPrefix(transactionReceiptJSON.ContractAddress, "0x"))
		if err != nil {
			return errors.Wrap(err, "invalid value for contract address")
		}
	}
	if transactionReceiptJSON.From == "" {
		return errors.New("From missing")
	}
	t.From, err = hex.DecodeString(strings.TrimPrefix(transactionReceiptJSON.From, "0x"))
	if err != nil {
		return errors.Wrap(err, "invalid value for from")
	}
	if transactionReceiptJSON.To != "" {
		t.To, err = hex.DecodeString(strings.TrimPrefix(transactionReceiptJSON.To, "0x"))
		if err != nil {
			return errors.Wrap(err, "invalid value for to")
		}
	}
	if transactionReceiptJSON.CumulativeGasUsed == "" {
		return errors.New("cumulative gas used missing")
	}
	t.CumulativeGasUsed, err = strconv.ParseUint(strings.TrimPrefix(transactionReceiptJSON.CumulativeGasUsed, "0x"), 16, 64)
	if err != nil {
		return errors.Wrap(err, "invalid format for cumulative gas used")
	}
	if transactionReceiptJSON.GasUsed == "" {
		return errors.New("gas used missing")
	}
	t.GasUsed, err = strconv.ParseUint(strings.TrimPrefix(transactionReceiptJSON.GasUsed, "0x"), 16, 64)
	if err != nil {
		return errors.Wrap(err, "invalid format for gas used")
	}
	t.Logs = transactionReceiptJSON.Logs

	return nil
}

// MarshalJSON implements json.Marshaler.
func (t *transactionReceipt) MarshalJSON() ([]byte, error) {
	return json.Marshal(&transactionReceiptJSON{
		BlockHash:         fmt.Sprintf("%#x", t.BlockHash),
		BlockNumber:       strconv.FormatUint(t.BlockNumber, 10),
		ContractAddress:   fmt.Sprintf("%#x", t.ContractAddress),
		From:              fmt.Sprintf("%#x", t.From),
		To:                fmt.Sprintf("%#x", t.To),
		CumulativeGasUsed: fmt.Sprintf("%#x", t.CumulativeGasUsed),
		GasUsed:           fmt.Sprintf("%#x", t.GasUsed),
		Logs:              t.Logs,
	})
}

// String returns a string version of the structure.
func (t *transactionReceipt) String() string {
	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Sprintf("ERR: %v", err)
	}
	return string(data)
}
