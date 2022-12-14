// Copyright © 2022 Weald Technology Trading.
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

package util

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	majordomo "github.com/wealdtech/go-majordomo"
	asmconfidant "github.com/wealdtech/go-majordomo/confidants/asm"
	directconfidant "github.com/wealdtech/go-majordomo/confidants/direct"
	fileconfidant "github.com/wealdtech/go-majordomo/confidants/file"
	gsmconfidant "github.com/wealdtech/go-majordomo/confidants/gsm"
	standardmajordomo "github.com/wealdtech/go-majordomo/standard"
)

// InitMajordomo initialises the majordomo service, for fetching secrets.
func InitMajordomo(ctx context.Context) (majordomo.Service, error) {
	majordomo, err := standardmajordomo.New(ctx,
		standardmajordomo.WithLogLevel(LogLevel("majordomo")),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create majordomo service")
	}

	directConfidant, err := directconfidant.New(ctx,
		directconfidant.WithLogLevel(LogLevel("majordomo.confidants.direct")),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create direct confidant")
	}
	if err := majordomo.RegisterConfidant(ctx, directConfidant); err != nil {
		return nil, errors.Wrap(err, "failed to register direct confidant")
	}

	fileConfidant, err := fileconfidant.New(ctx,
		fileconfidant.WithLogLevel(LogLevel("majordomo.confidants.file")),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create file confidant")
	}
	if err := majordomo.RegisterConfidant(ctx, fileConfidant); err != nil {
		return nil, errors.Wrap(err, "failed to register file confidant")
	}

	if viper.GetString("majordomo.asm.region") != "" {
		var asmCredentials *credentials.Credentials
		if viper.GetString("majordomo.asm.id") != "" {
			asmCredentials = credentials.NewStaticCredentials(viper.GetString("majordomo.asm.id"), viper.GetString("ma  jordomo.asm.secret"), "")
		}
		asmConfidant, err := asmconfidant.New(ctx,
			asmconfidant.WithLogLevel(LogLevel("majordomo.confidants.asm")),
			asmconfidant.WithCredentials(asmCredentials),
			asmconfidant.WithRegion(viper.GetString("majordomo.asm.region")),
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create AWS secrets manager confidant")
		}
		if err := majordomo.RegisterConfidant(ctx, asmConfidant); err != nil {
			return nil, errors.Wrap(err, "failed to register AWS secrets manager confidant")
		}
	}

	if viper.GetString("majordomo.gsm.project") != "" {
		var gsmCredentialsPath string
		if viper.GetString("majordomo.gsm.credentials") != "" {
			gsmCredentialsPath = ResolvePath(viper.GetString("majordomo.gsm.credentials"))
		}
		gsmConfidant, err := gsmconfidant.New(ctx,
			gsmconfidant.WithLogLevel(LogLevel("majordomo.confidants.gsm")),
			gsmconfidant.WithCredentialsPath(gsmCredentialsPath),
			gsmconfidant.WithProject(viper.GetString("majordomo.gsm.project")),
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create Google secrets manager confidant")
		}
		if err := majordomo.RegisterConfidant(ctx, gsmConfidant); err != nil {
			return nil, errors.Wrap(err, "failed to register Google secrets manager confidant")
		}
	}

	return majordomo, nil
}
