// Copyright © 2020 - 2023 Weald Technology Trading.
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
	"math/big"
	"time"

	"github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/attestantio/go-eth2-client/spec/phase0"
)

// Block holds information about a block.
type Block struct {
	Slot             phase0.Slot
	ProposerIndex    phase0.ValidatorIndex
	Root             phase0.Root
	Graffiti         []byte
	RANDAOReveal     phase0.BLSSignature
	BodyRoot         phase0.Root
	ParentRoot       phase0.Root
	StateRoot        phase0.Root
	Canonical        *bool
	ETH1BlockHash    []byte
	ETH1DepositCount uint64
	ETH1DepositRoot  phase0.Root
	// Information only available from Bellatrix onwards.
	ExecutionPayload *ExecutionPayload
	// Information only available from Capella onwards.
	BLSToExecutionChanges []*BLSToExecutionChange
}

// Validator holds information about a validator.
type Validator struct {
	PublicKey                  phase0.BLSPubKey
	Index                      phase0.ValidatorIndex
	EffectiveBalance           phase0.Gwei
	Slashed                    bool
	ActivationEligibilityEpoch phase0.Epoch
	ActivationEpoch            phase0.Epoch
	ExitEpoch                  phase0.Epoch
	WithdrawableEpoch          phase0.Epoch
	WithdrawalCredentials      [32]byte
}

// ValidatorBalance holds information about a validator's balance at a given epoch.
type ValidatorBalance struct {
	Index            phase0.ValidatorIndex
	Epoch            phase0.Epoch
	Balance          phase0.Gwei
	EffectiveBalance phase0.Gwei
}

// AggregateValidatorBalance holds aggreated information about validators' balances at a given epoch.
type AggregateValidatorBalance struct {
	Epoch            phase0.Epoch
	Balance          phase0.Gwei
	EffectiveBalance phase0.Gwei
}

// BeaconCommittee holds information for beacon committees.
type BeaconCommittee struct {
	Slot      phase0.Slot
	Index     phase0.CommitteeIndex
	Committee []phase0.ValidatorIndex
}

// ProposerDuty holds information for proposer duties.
type ProposerDuty struct {
	Slot           phase0.Slot
	ValidatorIndex phase0.ValidatorIndex
}

// AttesterDuty holds information for attester duties.
type AttesterDuty struct {
	Slot           phase0.Slot
	Committee      phase0.CommitteeIndex
	ValidatorIndex phase0.ValidatorIndex
	// CommitteeIndex is the index of the validator in the committee.
	CommitteeIndex uint64
}

// Attestation holds information about an attestation included by a block.
type Attestation struct {
	InclusionSlot      phase0.Slot
	InclusionBlockRoot phase0.Root
	InclusionIndex     uint64
	Slot               phase0.Slot
	CommitteeIndex     phase0.CommitteeIndex
	AggregationBits    []byte
	AggregationIndices []phase0.ValidatorIndex
	BeaconBlockRoot    phase0.Root
	SourceEpoch        phase0.Epoch
	SourceRoot         phase0.Root
	TargetEpoch        phase0.Epoch
	TargetRoot         phase0.Root
	Canonical          *bool
	TargetCorrect      *bool
	HeadCorrect        *bool
}

// SyncAggregate holds information about a sync aggregate included in a block.
type SyncAggregate struct {
	InclusionSlot      phase0.Slot
	InclusionBlockRoot phase0.Root
	Bits               []byte
	Indices            []phase0.ValidatorIndex
}

// Deposit holds information about an Ethereum 2 deposit included by a block.
type Deposit struct {
	InclusionSlot         phase0.Slot
	InclusionBlockRoot    phase0.Root
	InclusionIndex        uint64
	ValidatorPubKey       phase0.BLSPubKey
	WithdrawalCredentials []byte
	Amount                phase0.Gwei
}

// ETH1Deposit holds information about an Ethereum 2 deposit made on the Ethereum 1 chain.
type ETH1Deposit struct {
	ETH1BlockNumber       uint64
	ETH1BlockHash         []byte
	ETH1BlockTimestamp    time.Time
	ETH1TxHash            []byte
	ETH1LogIndex          uint64
	ETH1Sender            []byte
	ETH1Recipient         []byte
	ETH1GasUsed           uint64
	ETH1GasPrice          uint64
	DepositIndex          uint64
	ValidatorPubKey       phase0.BLSPubKey
	WithdrawalCredentials []byte
	Signature             phase0.BLSSignature
	Amount                phase0.Gwei
}

// VoluntaryExit holds information about a voluntary exit included in a block.
type VoluntaryExit struct {
	InclusionSlot      phase0.Slot
	InclusionBlockRoot phase0.Root
	InclusionIndex     uint64
	ValidatorIndex     phase0.ValidatorIndex
	Epoch              phase0.Epoch
}

// AttesterSlashing holds information about an attester slashing included by a block.
type AttesterSlashing struct {
	InclusionSlot               phase0.Slot
	InclusionBlockRoot          phase0.Root
	InclusionIndex              uint64
	Attestation1Indices         []phase0.ValidatorIndex
	Attestation1Slot            phase0.Slot
	Attestation1CommitteeIndex  phase0.CommitteeIndex
	Attestation1BeaconBlockRoot phase0.Root
	Attestation1SourceEpoch     phase0.Epoch
	Attestation1SourceRoot      phase0.Root
	Attestation1TargetEpoch     phase0.Epoch
	Attestation1TargetRoot      phase0.Root
	Attestation1Signature       phase0.BLSSignature
	Attestation2Indices         []phase0.ValidatorIndex
	Attestation2Slot            phase0.Slot
	Attestation2CommitteeIndex  phase0.CommitteeIndex
	Attestation2BeaconBlockRoot phase0.Root
	Attestation2SourceEpoch     phase0.Epoch
	Attestation2SourceRoot      phase0.Root
	Attestation2TargetEpoch     phase0.Epoch
	Attestation2TargetRoot      phase0.Root
	Attestation2Signature       phase0.BLSSignature
}

// ProposerSlashing holds information about a proposer slashing included by a block.
type ProposerSlashing struct {
	InclusionSlot        phase0.Slot
	InclusionBlockRoot   phase0.Root
	InclusionIndex       uint64
	Block1Root           phase0.Root
	Header1Slot          phase0.Slot
	Header1ProposerIndex phase0.ValidatorIndex
	Header1ParentRoot    phase0.Root
	Header1StateRoot     phase0.Root
	Header1BodyRoot      phase0.Root
	Header1Signature     phase0.BLSSignature
	Block2Root           phase0.Root
	Header2Slot          phase0.Slot
	Header2ProposerIndex phase0.ValidatorIndex
	Header2ParentRoot    phase0.Root
	Header2StateRoot     phase0.Root
	Header2BodyRoot      phase0.Root
	Header2Signature     phase0.BLSSignature
}

// ValidatorEpochSummary provides a summary of a validator's operations for an epoch.
type ValidatorEpochSummary struct {
	Index                     phase0.ValidatorIndex
	Epoch                     phase0.Epoch
	ProposerDuties            int
	ProposalsIncluded         int
	AttestationIncluded       bool
	AttestationTargetCorrect  *bool
	AttestationHeadCorrect    *bool
	AttestationInclusionDelay *int
	AttestationSourceTimely   *bool
	AttestationTargetTimely   *bool
	AttestationHeadTimely     *bool
}

// ValidatorDaySummary provides a summary of a validator's operations for a day.
type ValidatorDaySummary struct {
	Index                         phase0.ValidatorIndex
	StartTimestamp                time.Time
	StartBalance                  uint64
	StartEffectiveBalance         uint64
	CapitalChange                 int64
	RewardChange                  int64
	Withdrawals                   uint64
	EffectiveBalanceChange        int64
	Proposals                     int
	ProposalsIncluded             int
	Attestations                  int
	AttestationsIncluded          int
	AttestationsTargetCorrect     int
	AttestationsHeadCorrect       int
	AttestationsSourceTimely      int
	AttestationsTargetTimely      int
	AttestationsHeadTimely        int
	AttestationsInclusionDelay    float64
	SyncCommitteeMessages         int
	SyncCommitteeMessagesIncluded int
}

// BlockSummary provides a summary of an epoch.
type BlockSummary struct {
	Slot                          phase0.Slot
	AttestationsForBlock          int
	DuplicateAttestationsForBlock int
	VotesForBlock                 int
	ParentDistance                int
}

// EpochSummary provides a summary of an epoch.
type EpochSummary struct {
	Epoch                         phase0.Epoch
	ActivationQueueLength         int
	ActivatingValidators          int
	ActiveValidators              int
	ActiveRealBalance             phase0.Gwei
	ActiveBalance                 phase0.Gwei
	AttestingValidators           int
	AttestingBalance              phase0.Gwei
	TargetCorrectValidators       int
	TargetCorrectBalance          phase0.Gwei
	HeadCorrectValidators         int
	HeadCorrectBalance            phase0.Gwei
	AttestationsForEpoch          int
	AttestationsInEpoch           int
	DuplicateAttestationsForEpoch int
	ProposerSlashings             int
	AttesterSlashings             int
	Deposits                      int
	ExitingValidators             int
	CanonicalBlocks               int
	Withdrawals                   phase0.Gwei
}

// SyncCommittee holds information for sync committees.
type SyncCommittee struct {
	Period    uint64
	Committee []phase0.ValidatorIndex
}

// ExecutionPayload holds information about a block's execution payload.
type ExecutionPayload struct {
	ParentHash    [32]byte
	FeeRecipient  [20]byte
	StateRoot     [32]byte
	ReceiptsRoot  [32]byte
	LogsBloom     [256]byte
	PrevRandao    [32]byte
	BlockNumber   uint64
	GasLimit      uint64
	GasUsed       uint64
	Timestamp     uint64
	ExtraData     []byte
	BaseFeePerGas *big.Int
	BlockHash     [32]byte
	// No transactions, they are stored in execd.
	Withdrawals []*Withdrawal
}

// BLSToExecutionChange holds information about credentials change operations.
type BLSToExecutionChange struct {
	InclusionBlockRoot phase0.Root
	InclusionSlot      phase0.Slot
	InclusionIndex     uint
	ValidatorIndex     phase0.ValidatorIndex
	FromBLSPubKey      [32]byte
	ToExecutionAddress [20]byte
}

// Withdrawal holds information about a withdrawal from consensus to execution layer.
type Withdrawal struct {
	InclusionBlockRoot phase0.Root
	InclusionSlot      phase0.Slot
	InclusionIndex     uint
	Index              capella.WithdrawalIndex
	ValidatorIndex     phase0.ValidatorIndex
	Address            [20]byte
	Amount             phase0.Gwei
}
