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
	"strconv"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/wealdtech/chaind/services/chaindb"
	"github.com/wealdtech/chaind/services/metrics"
)

type parameters struct {
	logLevel           zerolog.Level
	monitor            metrics.Service
	connectionURL      string
	chainDB            chaindb.Service
	eth1DepositsSetter chaindb.ETH1DepositsSetter
	eth1Confirmations  uint64
	startBlock         string
}

// Parameter is the interface for service parameters.
type Parameter interface {
	apply(*parameters)
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

// WithChainDB sets the chain database service for this module.
func WithChainDB(chainDB chaindb.Service) Parameter {
	return parameterFunc(func(p *parameters) {
		p.chainDB = chainDB
	})
}

// WithETH1DepositsSetter sets the Ethereum 1 deposits setter for this module.
func WithETH1DepositsSetter(setter chaindb.ETH1DepositsSetter) Parameter {
	return parameterFunc(func(p *parameters) {
		p.eth1DepositsSetter = setter
	})
}

// WithETH1Confirmations sets the number of confirmations we wait for before processing.
func WithETH1Confirmations(confirmations uint64) Parameter {
	return parameterFunc(func(p *parameters) {
		p.eth1Confirmations = confirmations
	})
}

// WithConnectionURL sets the Ethereum 1 connection URL service for this module.
func WithConnectionURL(url string) Parameter {
	return parameterFunc(func(p *parameters) {
		p.connectionURL = url
	})
}

// WithStartBlock sets the start block for this module.
func WithStartBlock(block string) Parameter {
	return parameterFunc(func(p *parameters) {
		p.startBlock = block
	})
}

// parseAndCheckParameters parses and checks parameters to ensure that mandatory parameters are present and correct.
func parseAndCheckParameters(params ...Parameter) (*parameters, error) {
	parameters := parameters{
		logLevel:          zerolog.GlobalLevel(),
		eth1Confirmations: 12, // Default number of confirmations.
	}
	for _, p := range params {
		if params != nil {
			p.apply(&parameters)
		}
	}

	if parameters.chainDB == nil {
		return nil, errors.New("no chain database specified")
	}
	if parameters.eth1DepositsSetter == nil {
		return nil, errors.New("no Ethereum 1 deposits setter specified")
	}
	if parameters.connectionURL == "" {
		return nil, errors.New("no connection URL specified")
	}
	if parameters.startBlock != "" {
		_, err := strconv.ParseInt(parameters.startBlock, 10, 64)
		if err != nil {
			return nil, errors.Wrap(err, "invalid start block specified")
		}
	}

	return &parameters, nil
}
