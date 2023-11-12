// Copyright © 2020 Weald Technology Trading.
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

package main

import (
	"context"
	"sync"

	eth2client "github.com/attestantio/go-eth2-client"
	autoclient "github.com/attestantio/go-eth2-client/auto"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/wealdtech/chaind/util"
)

var (
	clients   map[string]eth2client.Service
	clientsMu sync.Mutex
)

// fetchClient fetches a client service, instantiating it if required.
func fetchClient(ctx context.Context, address string) (eth2client.Service, error) {
	clientsMu.Lock()
	defer clientsMu.Unlock()
	if clients == nil {
		clients = make(map[string]eth2client.Service)
	}

	var client eth2client.Service
	var exists bool
	if client, exists = clients[address]; !exists {
		var err error
		client, err = autoclient.New(ctx,
			autoclient.WithLogLevel(util.LogLevel("eth2client")),
			autoclient.WithTimeout(viper.GetDuration("eth2client.timeout")),
			autoclient.WithAddress(address))
		if err != nil {
			return nil, errors.Wrap(err, "failed to initiate client")
		}
		// Confirm that the client provides the required interfaces.
		if err := confirmClientInterfaces(client); err != nil {
			return nil, errors.Wrap(err, "missing required interface")
		}
		clients[address] = client
	}

	return client, nil
}

func confirmClientInterfaces(client eth2client.Service) error {
	if _, isProvider := client.(eth2client.GenesisProvider); !isProvider {
		return errors.New("client is not a GenesisProvider")
	}
	if _, isProvider := client.(eth2client.SpecProvider); !isProvider {
		return errors.New("client is not a SpecProvider")
	}
	if _, isProvider := client.(eth2client.ForkScheduleProvider); !isProvider {
		return errors.New("client is not a ForkScheduleProvider")
	}
	if _, isProvider := client.(eth2client.NodeSyncingProvider); !isProvider {
		return errors.New("client is not a NodeSyncingProvider")
	}

	return nil
}
