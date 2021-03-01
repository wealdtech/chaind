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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogResponse(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		err   string
	}{
		{
			name:  "Good",
			input: []byte(`{"address":"0x8c5fecdc472e27bc447696f431e425d02dd46a8c","topics":["0x649bbc62d0e31342afea4e5cd82d4049e7e1ee912fc0889aa790803be39038c5"],"data":"0x00000000000000000000000000000000000000000000000000000000000000a000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000140000000000000000000000000000000000000000000000000000000000000018000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000000000000000000000030b55446978b2d229265caceb97cb4d59c0187ba91fcf11675330c1a373f137fa3fb553acb663a0d83f5dbcdc17c9f4f92000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000020005db2c8fb17330066824de63245948b3c2077f39a7e6bebb46ae93da8271148000000000000000000000000000000000000000000000000000000000000000800405973070000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000060b896411caf11780020b5656c5ebf0ff3ff245e4d679d9c6860e4ccbc695a672aa59d41c27b42bb9babf4c1b458e773c708fe4fce4cfe8ae43f9630a19c938d4c18165b5a3ff5f5e5dc2bd374a8dcfa531f3e189c1ba341cd511c4cd451c488d60000000000000000000000000000000000000000000000000000000000000008c58f010000000000000000000000000000000000000000000000000000000000","blockNumber":"0x39e9b3","transactionHash":"0x4428f17853c0237564eb7d97651fbb3390f444d223de5459799144cace695f91","transactionIndex":"0x0","blockHash":"0xfa3a6f5e2f5781bbdd4c68aa6ddd9ac3de8523188a9f8a71451007ad7f2c33c4","logIndex":"0x0","removed":false}`),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var res logResponse
			err := json.Unmarshal(test.input, &res)
			if test.err != "" {
				require.EqualError(t, err, test.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
