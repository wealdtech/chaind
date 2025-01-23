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

type logResponse struct {
	Address          []byte
	Topics           [][]byte
	Data             []byte
	BlockNumber      uint64
	TransactionHash  []byte
	TransactionIndex uint64
	BlockHash        []byte
	LogIndex         uint64
	Removed          bool
}

//nolint:tagliatelle
type logResponseJSON struct {
	Address          string   `json:"address"`
	Topics           []string `json:"topics"`
	Data             string   `json:"data"`
	BlockNumber      string   `json:"blockNumber"`
	TransactionHash  string   `json:"transactionHash"`
	TransactionIndex string   `json:"transactionIndex"`
	BlockHash        string   `json:"blockHash"`
	LogIndex         string   `json:"logIndex"`
	Removed          bool     `json:"removed"`
}

// UnmarshalJSON implements json.Unmarshaler.
func (l *logResponse) UnmarshalJSON(input []byte) error {
	var logResponseJSON logResponseJSON
	var err error
	if err := json.Unmarshal(input, &logResponseJSON); err != nil {
		return errors.Wrap(err, "invalid JSON")
	}

	if logResponseJSON.Address == "" {
		return errors.New("address missing")
	}
	l.Address, err = hex.DecodeString(strings.TrimPrefix(logResponseJSON.Address, "0x"))
	if err != nil {
		return errors.Wrap(err, "invalid value for address")
	}
	l.Topics = make([][]byte, len(logResponseJSON.Topics))
	for i := range logResponseJSON.Topics {
		l.Topics[i], err = hex.DecodeString(strings.TrimPrefix(logResponseJSON.Topics[i], "0x"))
		if err != nil {
			return errors.Wrap(err, "invalid value for topic")
		}
	}
	if logResponseJSON.Data == "" {
		return errors.New("data missing")
	}
	l.Data, err = hex.DecodeString(strings.TrimPrefix(logResponseJSON.Data, "0x"))
	if err != nil {
		return errors.Wrap(err, "invalid value for data")
	}
	if logResponseJSON.BlockNumber == "" {
		return errors.New("block number missing")
	}
	l.BlockNumber, err = strconv.ParseUint(strings.TrimPrefix(logResponseJSON.BlockNumber, "0x"), 16, 64)
	if err != nil {
		return errors.Wrap(err, "invalid format for block number")
	}
	if logResponseJSON.TransactionHash == "" {
		return errors.New("transaction hash missing")
	}
	l.TransactionHash, err = hex.DecodeString(strings.TrimPrefix(logResponseJSON.TransactionHash, "0x"))
	if err != nil {
		return errors.Wrap(err, "invalid value for transaction hash")
	}
	if logResponseJSON.BlockHash == "" {
		return errors.New("transaction index missing")
	}
	if logResponseJSON.TransactionIndex == "" {
		return errors.New("transaction index missing")
	}
	l.TransactionIndex, err = strconv.ParseUint(strings.TrimPrefix(logResponseJSON.TransactionIndex, "0x"), 16, 64)
	if err != nil {
		return errors.Wrap(err, "invalid format for transaction index")
	}
	if logResponseJSON.BlockHash == "" {
		return errors.New("block hash missing")
	}
	l.BlockHash, err = hex.DecodeString(strings.TrimPrefix(logResponseJSON.BlockHash, "0x"))
	if err != nil {
		return errors.Wrap(err, "invalid value for block hash")
	}
	if logResponseJSON.LogIndex == "" {
		return errors.New("log index missing")
	}
	l.LogIndex, err = strconv.ParseUint(strings.TrimPrefix(logResponseJSON.LogIndex, "0x"), 16, 64)
	if err != nil {
		return errors.Wrap(err, "invalid format for log index")
	}
	l.Removed = logResponseJSON.Removed

	return nil
}

// MarshalJSON implements json.Marshaler.
func (l *logResponse) MarshalJSON() ([]byte, error) {
	topics := make([]string, len(l.Topics))
	for i := range l.Topics {
		topics[i] = fmt.Sprintf("%#x", l.Topics[i])
	}
	return json.Marshal(&logResponseJSON{
		Address:          fmt.Sprintf("%#x", l.Address),
		Topics:           topics,
		Data:             fmt.Sprintf("%#x", l.Data),
		BlockNumber:      strconv.FormatUint(l.BlockNumber, 10),
		TransactionHash:  fmt.Sprintf("%#x", l.TransactionHash),
		TransactionIndex: strconv.FormatUint(l.TransactionIndex, 10),
		BlockHash:        fmt.Sprintf("%#x", l.BlockHash),
		LogIndex:         strconv.FormatUint(l.LogIndex, 10),
		Removed:          l.Removed,
	})
}

// String returns a string version of the structure.
func (l *logResponse) String() string {
	data, err := json.Marshal(l)
	if err != nil {
		return fmt.Sprintf("ERR: %v", err)
	}
	return string(data)
}
