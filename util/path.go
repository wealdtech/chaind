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
	"path/filepath"

	"github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
)

// ResolvePath resolves a potentially relative path to an absolute path.
func ResolvePath(path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	baseDir := viper.GetString("base-dir")
	if baseDir == "" {
		homeDir, err := homedir.Dir()
		if err != nil {
			panic("could not determine a home directory")
		}
		baseDir = homeDir
	}
	return filepath.Join(baseDir, path)
}
