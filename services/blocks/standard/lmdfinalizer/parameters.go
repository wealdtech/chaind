package lmdfinalizer

import (
	"errors"

	"github.com/rs/zerolog"
	"github.com/wealdtech/chaind/services/chaindb"
)

type parameters struct {
	lfb           *chaindb.Block
	logLevel      zerolog.Level
	newLFBHandler NewLFBHandler
}

// Parameter is the interface for service parameters.
type Parameter interface {
	apply(*parameters)
}

type parameterFunc func(*parameters)

func (f parameterFunc) apply(p *parameters) {
	f(p)
}

func WithLFB(lfb *chaindb.Block) Parameter {
	return parameterFunc(func(p *parameters) {
		p.lfb = lfb
	})
}

// WithLogLevel sets the log level for the module.
func WithLogLevel(logLevel zerolog.Level) Parameter {
	return parameterFunc(func(p *parameters) {
		p.logLevel = logLevel
	})
}

func WithHandler(handler NewLFBHandler) Parameter {
	return parameterFunc(func(p *parameters) {
		p.newLFBHandler = handler
	})
}

// parseAndCheckParameters parses and checks parameters to ensure that mandatory parameters are present and correct.
func parseAndCheckParameters(params ...Parameter) (*parameters, error) {
	parameters := parameters{
		logLevel: zerolog.GlobalLevel(),
	}
	for _, p := range params {
		if params != nil {
			p.apply(&parameters)
		}
	}

	if parameters.lfb == nil {
		return nil, errors.New("no latest finalized block specified")
	}

	return &parameters, nil
}
