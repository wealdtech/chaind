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
	"fmt"
	"net/url"

	"github.com/pkg/errors"
)

type getLogsResponse struct {
	Result []*logResponse `json:"result"`
}

// getLogs gets the logs for a range of blocks.
func (s *Service) getLogs(ctx context.Context, startBlock uint64, endBlock uint64) ([]*logResponse, error) {
	reference, err := url.Parse("")
	if err != nil {
		return nil, errors.Wrap(err, "invalid endpoint")
	}
	url := s.base.ResolveReference(reference).String()

	reqBody := bytes.NewBufferString(fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"address":["%#x"],"topics":["0x649bbc62d0e31342afea4e5cd82d4049e7e1ee912fc0889aa790803be39038c5"],"fromBlock":"%#x","toBlock":"%#x"}],"id":11}`, s.depositContractAddress, startBlock, endBlock))
	respBodyReader, err := s.post(ctx, url, reqBody)
	if err != nil {
		log.Trace().Str("url", url).Err(err).Msg("Request failed")
		return nil, errors.Wrap(err, "request failed")
	}
	if respBodyReader == nil {
		return nil, errors.New("empty response")
	}

	var response getLogsResponse
	if err := json.NewDecoder(respBodyReader).Decode(&response); err != nil {
		return nil, errors.Wrap(err, "invalid response")
	}
	log.Trace().Uint64("start_block", startBlock).Uint64("end_block", endBlock).Int("logs", len(response.Result)).Msg("Obtained logs")

	return response.Result, nil
}
