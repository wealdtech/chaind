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

package postgresql

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

// SlotDuration provides the duration of a slot of the chain.
func (s *Service) SlotDuration(ctx context.Context) (time.Duration, error) {
	val, err := s.ChainSpecValue(ctx, "SECONDS_PER_SLOT")
	if err != nil {
		return 0, errors.Wrap(err, "failed to obtain SECONDS_PER_SLOT")
	}
	specVal, ok := val.(time.Duration)
	if !ok {
		return 0, errors.New("invalid type for SECONDS_PER_SLOT")
	}
	return specVal, nil
}

// SlotsPerEpoch provides the slots per epoch of the chain.
func (s *Service) SlotsPerEpoch(ctx context.Context) (uint64, error) {
	val, err := s.ChainSpecValue(ctx, "SLOTS_PER_EPOCH")
	if err != nil {
		return 0, errors.Wrap(err, "failed to obtain SLOTS_PER_EPOCH")
	}
	specVal, ok := val.(uint64)
	if !ok {
		return 0, errors.New("invalid type for SLOTS_PER_EPOCH")
	}
	return specVal, nil
}
