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

type transactionByHashResponse struct {
	Result *transaction `json:"result"`
}

// transactionByHash fetches a transaction receipt given its hash.
func (s *Service) transactionByHash(ctx context.Context, txHash []byte) (*transaction, error) {
	reference, err := url.Parse("")
	if err != nil {
		return nil, errors.Wrap(err, "invalid endpoint")
	}
	url := s.base.ResolveReference(reference).String()

	reqBody := bytes.NewBuffer([]byte(fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getTransactionByHash","params":["%#x"],"id":1901}`, txHash)))
	respBodyReader, err := s.post(ctx, url, reqBody)
	if err != nil {
		log.Trace().Str("url", url).Err(err).Msg("Request failed")
		return nil, errors.Wrap(err, "request failed")
	}
	if respBodyReader == nil {
		return nil, errors.New("empty response")
	}

	var response transactionByHashResponse
	if err := json.NewDecoder(respBodyReader).Decode(&response); err != nil {
		return nil, errors.Wrap(err, "invalid response")
	}
	if response.Result == nil {
		return nil, errors.Wrap(err, "invalid response")
	}

	return response.Result, nil
}
