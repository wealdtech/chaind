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

package postgresql

import (
	"errors"

	"github.com/rs/zerolog"
)

type parameters struct {
	logLevel       zerolog.Level
	connectionURL  string
	server         string
	port           int32
	user           string
	password       string
	clientCert     []byte
	clientKey      []byte
	caCert         []byte
	maxConnections uint
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

// WithConnectionURL sets the connection URL for this module.
// Deprecated.  Use the individual Server/User/Port/... functions.
func WithConnectionURL(connectionURL string) Parameter {
	return parameterFunc(func(p *parameters) {
		p.connectionURL = connectionURL
	})
}

// WithServer sets the server for this module.
func WithServer(server string) Parameter {
	return parameterFunc(func(p *parameters) {
		p.server = server
	})
}

// WithUser sets the user for this module.
func WithUser(user string) Parameter {
	return parameterFunc(func(p *parameters) {
		p.user = user
	})
}

// WithPassword sets the password for this module.
func WithPassword(password string) Parameter {
	return parameterFunc(func(p *parameters) {
		p.password = password
	})
}

// WithPort sets the port for this module.
func WithPort(port int32) Parameter {
	return parameterFunc(func(p *parameters) {
		p.port = port
	})
}

// WithClientCert sets the bytes of the client TLS certificate.
func WithClientCert(cert []byte) Parameter {
	return parameterFunc(func(p *parameters) {
		p.clientCert = cert
	})
}

// WithClientKey sets the bytes of the client TLS key.
func WithClientKey(key []byte) Parameter {
	return parameterFunc(func(p *parameters) {
		p.clientKey = key
	})
}

// WithCACert sets the bytes of the certificate authority TLS certificate.
func WithCACert(cert []byte) Parameter {
	return parameterFunc(func(p *parameters) {
		p.caCert = cert
	})
}

// WithMaxConnections sets the maximum number of connections for the database pool.
func WithMaxConnections(maxConnections uint) Parameter {
	return parameterFunc(func(p *parameters) {
		p.maxConnections = maxConnections
	})
}

// parseAndCheckParameters parses and checks parameters to ensure that mandatory parameters are present and correct.
func parseAndCheckParameters(params ...Parameter) (*parameters, error) {
	parameters := parameters{
		logLevel:       zerolog.GlobalLevel(),
		maxConnections: 16,
	}
	for _, p := range params {
		if params != nil {
			p.apply(&parameters)
		}
	}

	if parameters.connectionURL != "" {
		// Allow deprecated connection URL.
		return &parameters, nil
	}

	if parameters.server == "" {
		return nil, errors.New("no server specified")
	}
	if parameters.user == "" {
		return nil, errors.New("no user specified")
	}
	if parameters.port == 0 {
		return nil, errors.New("no port specified")
	}
	if parameters.maxConnections == 0 {
		return nil, errors.New("no maximum pool connections specified")
	}

	return &parameters, nil
}
