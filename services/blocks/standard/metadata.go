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
	"context"
	"encoding/json"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
)

// metadata stored about this service.
type metadata struct {
	LatestSlot                  phase0.Slot `json:"latest_slot"`
	LMDLatestFinalizedBlockRoot phase0.Root `json:"lmd_lfb_root,omitempty"`
	LMDLatestFinalizedSlot      phase0.Slot `json:"lmd_lfb_slot,omitempty"`
}

// metadataKey is the key for the metadata.
var metadataKey = "blocks.standard"

// getMetadata gets metadata for this service.
func (s *Service) getMetadata(ctx context.Context) (*metadata, error) {
	md := &metadata{}
	mdJSON, err := s.chainDB.Metadata(ctx, metadataKey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch metadata")
	}
	if mdJSON == nil {
		return md, nil
	}
	if err := json.Unmarshal(mdJSON, md); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal metadata")
	}
	return md, nil
}

// setMetadata sets metadata for this service.
func (s *Service) setMetadata(ctx context.Context, md *metadata) error {
	mdJSON, err := json.Marshal(md)
	if err != nil {
		return errors.Wrap(err, "failed to marshal metadata")
	}
	if err := s.chainDB.SetMetadata(ctx, metadataKey, mdJSON); err != nil {
		return errors.Wrap(err, "failed to update metadata")
	}
	return nil
}
