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
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type blockByHashResponse struct {
	Result *blockByHashBlockResponse `json:"result"`
}
type blockByHashBlockResponse struct {
	Timestamp string `json:"timestamp"`
}

// blockTimestampByHash fetches the timestamp of a block given its hash.
func (s *Service) blockTimestampByHash(ctx context.Context, blockHash []byte) (time.Time, error) {
	reference, err := url.Parse("")
	if err != nil {
		return time.Time{}, errors.Wrap(err, "invalid endpoint")
	}
	url := s.base.ResolveReference(reference).String()

	reqBody := bytes.NewBuffer([]byte(fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getBlockByHash","params":["%#x",false],"id":1901}`, blockHash)))
	respBodyReader, err := s.post(ctx, url, reqBody)
	if err != nil {
		log.Trace().Str("url", url).Err(err).Msg("Request failed")
		return time.Time{}, errors.Wrap(err, "request failed")
	}
	if respBodyReader == nil {
		return time.Time{}, errors.New("empty response")
	}

	var response blockByHashResponse
	if err := json.NewDecoder(respBodyReader).Decode(&response); err != nil {
		return time.Time{}, errors.Wrap(err, "invalid response")
	}
	if response.Result == nil {
		return time.Time{}, errors.Wrap(err, "empty response")
	}

	timestamp, err := strconv.ParseInt(strings.TrimPrefix(response.Result.Timestamp, "0x"), 16, 64)
	if err != nil {
		return time.Time{}, errors.Wrap(err, "invalid timestamp")
	}

	return time.Unix(timestamp, 0), nil
}
