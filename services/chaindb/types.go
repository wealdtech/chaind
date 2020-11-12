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

import (
	spec "github.com/attestantio/go-eth2-client/spec/phase0"
)

// Block holds information about a block.
type Block struct {
	Slot             spec.Slot
	ProposerIndex    spec.ValidatorIndex
	Root             spec.Root
	Graffiti         []byte
	RANDAOReveal     spec.BLSSignature
	BodyRoot         spec.Root
	ParentRoot       spec.Root
	StateRoot        spec.Root
	ETH1BlockHash    []byte
	ETH1DepositCount uint64
	ETH1DepositRoot  spec.Root
}

// Validator holds information about a validator.
type Validator struct {
	PublicKey                  spec.BLSPubKey
	Index                      spec.ValidatorIndex
	EffectiveBalance           spec.Gwei
	Slashed                    bool
	ActivationEligibilityEpoch spec.Epoch
	ActivationEpoch            spec.Epoch
	ExitEpoch                  spec.Epoch
	WithdrawableEpoch          spec.Epoch
}

// ValidatorBalance holds information about a validator's balance at a given epoch.
type ValidatorBalance struct {
	Index            spec.ValidatorIndex
	Epoch            spec.Epoch
	Balance          spec.Gwei
	EffectiveBalance spec.Gwei
}

// BeaconCommittee holds information for beacon committees.
type BeaconCommittee struct {
	Slot      spec.Slot
	Index     spec.CommitteeIndex
	Committee []spec.ValidatorIndex
}

// ProposerDuty holds information for proposer duties.
type ProposerDuty struct {
	Slot           spec.Slot
	ValidatorIndex spec.ValidatorIndex
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
	InclusionSlot      spec.Slot
	InclusionBlockRoot spec.Root
	InclusionIndex     uint64
	Slot               spec.Slot
	CommitteeIndex     spec.CommitteeIndex
	AggregationBits    []byte
	BeaconBlockRoot    spec.Root
	SourceEpoch        spec.Epoch
	SourceRoot         spec.Root
	TargetEpoch        spec.Epoch
	TargetRoot         spec.Root
}

// VoluntaryExit holds information about a voluntary exit included in a block.
type VoluntaryExit struct {
	InclusionSlot      spec.Slot
	InclusionBlockRoot spec.Root
	InclusionIndex     uint64
	ValidatorIndex     spec.ValidatorIndex
	Epoch              spec.Epoch
}

// AttesterSlashing holds information about an attester slashing included by a block.
type AttesterSlashing struct {
	InclusionSlot               spec.Slot
	InclusionBlockRoot          spec.Root
	InclusionIndex              uint64
	Attestation1Indices         []spec.ValidatorIndex
	Attestation1Slot            spec.Slot
	Attestation1CommitteeIndex  spec.CommitteeIndex
	Attestation1BeaconBlockRoot spec.Root
	Attestation1SourceEpoch     spec.Epoch
	Attestation1SourceRoot      spec.Root
	Attestation1TargetEpoch     spec.Epoch
	Attestation1TargetRoot      spec.Root
	Attestation1Signature       spec.BLSSignature
	Attestation2Indices         []spec.ValidatorIndex
	Attestation2Slot            spec.Slot
	Attestation2CommitteeIndex  spec.CommitteeIndex
	Attestation2BeaconBlockRoot spec.Root
	Attestation2SourceEpoch     spec.Epoch
	Attestation2SourceRoot      spec.Root
	Attestation2TargetEpoch     spec.Epoch
	Attestation2TargetRoot      spec.Root
	Attestation2Signature       spec.BLSSignature
}

// ProposerSlashing holds information about a proposer slashing included by a block.
type ProposerSlashing struct {
	InclusionSlot        spec.Slot
	InclusionBlockRoot   spec.Root
	InclusionIndex       uint64
	Header1Slot          spec.Slot
	Header1ProposerIndex spec.ValidatorIndex
	Header1ParentRoot    spec.Root
	Header1StateRoot     spec.Root
	Header1BodyRoot      spec.Root
	Header1Signature     spec.BLSSignature
	Header2Slot          spec.Slot
	Header2ProposerIndex spec.ValidatorIndex
	Header2ParentRoot    spec.Root
	Header2StateRoot     spec.Root
	Header2BodyRoot      spec.Root
	Header2Signature     spec.BLSSignature
}
