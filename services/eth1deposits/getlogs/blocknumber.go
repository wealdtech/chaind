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
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type blockNumberResponse struct {
	Result string `json:"result"`
}

// blockNumber fetches the current block number from an Ethereum 1 client.
func (s *Service) blockNumber(ctx context.Context) (uint64, error) {
	reference, err := url.Parse("")
	if err != nil {
		return 0, errors.Wrap(err, "invalid endpoint")
	}
	url := s.base.ResolveReference(reference).String()

	reqBody := bytes.NewBufferString(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1901}`)
	respBodyReader, err := s.post(ctx, url, reqBody)
	if err != nil {
		log.Trace().Str("url", url).Err(err).Msg("Request failed")
		return 0, errors.Wrap(err, "request failed")
	}
	if respBodyReader == nil {
		return 0, errors.New("empty response")
	}

	var response blockNumberResponse
	if err := json.NewDecoder(respBodyReader).Decode(&response); err != nil {
		return 0, errors.Wrap(err, "invalid response")
	}

	blockNumber, err := strconv.ParseUint(strings.TrimPrefix(response.Result, "0x"), 16, 64)
	if err != nil {
		return 0, errors.Wrap(err, "invalid block number")
	}

	return blockNumber, nil
}
