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

package handlers

import (
	"context"

	spec "github.com/attestantio/go-eth2-client/spec/phase0"
)

// FinalityHandler provides interfaces for handling finality updates.
type FinalityHandler interface {
	// OnFinalityUpdated is called when finality has been updated in the database.
	OnFinalityUpdated(ctx context.Context, epoch spec.Epoch)
}
