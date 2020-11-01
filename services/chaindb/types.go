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

package chaindb

// Block holds information about a block.
type Block struct {
	Slot             uint64
	ProposerIndex    uint64
	Root             []byte
	Graffiti         []byte
	RANDAOReveal     []byte
	BodyRoot         []byte
	ParentRoot       []byte
	StateRoot        []byte
	ETH1BlockHash    []byte
	ETH1DepositCount uint64
	ETH1DepositRoot  []byte
}

// Validator holds information about a validator.
type Validator struct {
	PublicKey                  []byte
	Index                      uint64
	EffectiveBalance           uint64
	Slashed                    bool
	ActivationEligibilityEpoch uint64
	ActivationEpoch            uint64
	ExitEpoch                  uint64
	WithdrawableEpoch          uint64
}

// ValidatorBalance holds information about a validator's balance at a given epoch.
type ValidatorBalance struct {
	Index            uint64
	Epoch            uint64
	Balance          uint64
	EffectiveBalance uint64
}

// BeaconCommittee holds information for beacon committees.
type BeaconCommittee struct {
	Slot      uint64
	Index     uint64
	Committee []uint64
}

// ProposerDuty holds information for proposer duties.
type ProposerDuty struct {
	Slot           uint64
	ValidatorIndex uint64
}

// AttesterDuty holds information for attester duties.
type AttesterDuty struct {
	Slot           uint64
	Committee      uint64
	ValidatorIndex uint64
	// CommitteeIndex is the index of the validator in the committee.
	CommitteeIndex uint64
}

// Attestation holds information about an attestation included by a block.
type Attestation struct {
	InclusionSlot      uint64
	InclusionBlockRoot []byte
	InclusionIndex     uint64
	Slot               uint64
	CommitteeIndex     uint64
	AggregationBits    []byte
	BeaconBlockRoot    []byte
	SourceEpoch        uint64
	SourceRoot         []byte
	TargetEpoch        uint64
	TargetRoot         []byte
}

// VoluntaryExit holds information about a voluntary exit included in a block.
type VoluntaryExit struct {
	InclusionSlot      uint64
	InclusionBlockRoot []byte
	InclusionIndex     uint64
	ValidatorIndex     uint64
	Epoch              uint64
}

// AttesterSlashing holds information about an attester slashing included by a block.
type AttesterSlashing struct {
	InclusionSlot               uint64
	InclusionBlockRoot          []byte
	InclusionIndex              uint64
	Attestation1Indices         []uint64
	Attestation1Slot            uint64
	Attestation1CommitteeIndex  uint64
	Attestation1BeaconBlockRoot []byte
	Attestation1SourceEpoch     uint64
	Attestation1SourceRoot      []byte
	Attestation1TargetEpoch     uint64
	Attestation1TargetRoot      []byte
	Attestation1Signature       []byte
	Attestation2Indices         []uint64
	Attestation2Slot            uint64
	Attestation2CommitteeIndex  uint64
	Attestation2BeaconBlockRoot []byte
	Attestation2SourceEpoch     uint64
	Attestation2SourceRoot      []byte
	Attestation2TargetEpoch     uint64
	Attestation2TargetRoot      []byte
	Attestation2Signature       []byte
}

// ProposerSlashing holds information about a proposer slashing included by a block.
type ProposerSlashing struct {
	InclusionSlot        uint64
	InclusionBlockRoot   []byte
	InclusionIndex       uint64
	Header1Slot          uint64
	Header1ProposerIndex uint64
	Header1ParentRoot    []byte
	Header1StateRoot     []byte
	Header1BodyRoot      []byte
	Header1Signature     []byte
	Header2Slot          uint64
	Header2ProposerIndex uint64
	Header2ParentRoot    []byte
	Header2StateRoot     []byte
	Header2BodyRoot      []byte
	Header2Signature     []byte
}
