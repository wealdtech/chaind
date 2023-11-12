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

package standard

import (
	"errors"

	eth2client "github.com/attestantio/go-eth2-client"
	"github.com/rs/zerolog"
	"github.com/wealdtech/chaind/services/chaindb"
	"github.com/wealdtech/chaind/services/chaintime"
	"github.com/wealdtech/chaind/services/metrics"
)

type parameters struct {
	logLevel   zerolog.Level
	monitor    metrics.Service
	eth2Client eth2client.Service
	chainDB    chaindb.Service
	chainTime  chaintime.Service
	startEpoch int64
}

// Parameter is the interface for service parameters.
type Parameter interface {
	apply(p *parameters)
}

type parameterFunc func(*parameters)

func (f parameterFunc) apply(p *parameters) {
	f(p)
}

// WithLogLevel sets the log level for the module.
func WithLogLevel(logLevel zerolog.Level) Parameter {
	return parameterFunc(func(p *parameters) {
		p.logLevel = logLevel
	})
}

// WithMonitor sets the monitor for the module.
func WithMonitor(monitor metrics.Service) Parameter {
	return parameterFunc(func(p *parameters) {
		p.monitor = monitor
	})
}

// WithETH2Client sets the Ethereum 2 client for this module.
func WithETH2Client(eth2Client eth2client.Service) Parameter {
	return parameterFunc(func(p *parameters) {
		p.eth2Client = eth2Client
	})
}

// WithChainDB sets the chain database for this module.
func WithChainDB(chainDB chaindb.Service) Parameter {
	return parameterFunc(func(p *parameters) {
		p.chainDB = chainDB
	})
}

// WithChainTime sets the chain time service for this module.
func WithChainTime(chainTime chaintime.Service) Parameter {
	return parameterFunc(func(p *parameters) {
		p.chainTime = chainTime
	})
}

// WithStartEpoch sets the start epoch for this module.
func WithStartEpoch(startEpoch int64) Parameter {
	return parameterFunc(func(p *parameters) {
		p.startEpoch = startEpoch
	})
}

// parseAndCheckParameters parses and checks parameters to ensure that mandatory parameters are present and correct.
func parseAndCheckParameters(params ...Parameter) (*parameters, error) {
	parameters := parameters{
		logLevel:   zerolog.GlobalLevel(),
		startEpoch: -1,
	}
	for _, p := range params {
		if params != nil {
			p.apply(&parameters)
		}
	}

	if parameters.eth2Client == nil {
		return nil, errors.New("no Ethereum 2 client specified")
	}
	if parameters.chainDB == nil {
		return nil, errors.New("no chain database specified")
	}
	if parameters.chainTime == nil {
		return nil, errors.New("no chain time specified")
	}

	return &parameters, nil
}
