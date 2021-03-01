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
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/pkg/errors"
)

type getLogsResponse struct {
	Result []*logResponse `json:"result"`
}

// getLogs gets the logs for a range of blocks.
func (s *Service) getLogs(ctx context.Context, startBlock uint64, endBlock uint64) ([]*logResponse, error) {
	reference, err := url.Parse("/")
	if err != nil {
		return nil, errors.Wrap(err, "invalid endpoint")
	}
	url := s.base.ResolveReference(reference).String()

	body := bytes.NewBuffer([]byte(fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"address":["%#x"],"topics":["0x649bbc62d0e31342afea4e5cd82d4049e7e1ee912fc0889aa790803be39038c5"],"fromBlock":"%#x","toBlock":"%#x"}],"id":11}`, s.depositContractAddress, startBlock, endBlock)))
	log.Trace().Uint64("start_block", startBlock).Uint64("end_block", endBlock).Msg("Fetching logs for blocks")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to setup request context")
	}
	req.Header.Set("Content-type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to call POST endpoint")
	}

	statusFamily := resp.StatusCode / 100
	if statusFamily != 2 {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read faled POST response")
		}
		return nil, fmt.Errorf("POST failed: %s (%d)", string(data), resp.StatusCode)
	}

	var response getLogsResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		log.Error().Err(err).Msg("Failed to parse getLogs response")
	}
	log.Trace().Uint64("start_block", startBlock).Uint64("end_block", endBlock).Int("logs", len(response.Result)).Msg("Obtained logs")

	return response.Result, nil
}
