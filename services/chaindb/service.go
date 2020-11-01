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
	"context"
)

// MetadataService defines the metadata service.
type MetadataService interface {
	// SetMetadata sets a metadata key to a JSON value.
	SetMetadata(ctx context.Context, key string, value []byte) error

	// GetMetadata obtains the JSON value from a metadata key.
	GetMetadata(ctx context.Context, key string) ([]byte, error)
}

// BlocksService defines the blocks service.
type BlocksService interface {
	// SetBlock sets a block.
	SetBlock(ctx context.Context, block *Block) error

	// GetBlocksBySlot fetches all blocks with the given slot.
	GetBlocksBySlot(ctx context.Context, slot uint64) ([]*Block, error)

	// GetBlockByRoot fetches the block with the given root.
	GetBlockByRoot(ctx context.Context, root []byte) (*Block, error)

	// GetBlocksByParentRoot fetches the blocks with the given parent root.
	GetBlocksByParentRoot(ctx context.Context, root []byte) ([]*Block, error)

	// GetEmptySlots fetches the slots in the given range without a block in the database.
	GetEmptySlots(ctx context.Context, minSlot uint64, maxSlot uint64) ([]uint64, error)
}

// AttestationsService defines the attestations service.
type AttestationsService interface {
	// SetAttestation sets an attestation.
	SetAttestation(ctx context.Context, attestation *Attestation) error

	// GetAttestationsForBlock fetches all attestations made for the given block.
	GetAttestationsForBlock(ctx context.Context, blockRoot []byte) ([]*Attestation, error)

	// GetAttestationsInBlock fetches all attestations contained in the given block.
	GetAttestationsInBlock(ctx context.Context, blockRoot []byte) ([]*Attestation, error)

	// GetAttestationsForSlotRange fetches all attestations made for the given slot range.
	GetAttestationsForSlotRange(ctx context.Context, minSlot uint64, maxSlot uint64) ([]*Attestation, error)
}

// VoluntaryExitsService defines the voluntary exits service.
type VoluntaryExitsService interface {
	// SetVoluntaryExit sets a voluntary exit.
	SetVoluntaryExit(ctx context.Context, voluntaryExit *VoluntaryExit) error
}

// AttesterSlashingsService defines the attester slashings service.
type AttesterSlashingsService interface {
	// SetAttesterSlashing sets an attester slashing.
	SetAttesterSlashing(ctx context.Context, attesterSlashing *AttesterSlashing) error
}

// ProposerSlashingsService defines the proposer slashings service.
type ProposerSlashingsService interface {
	// SetProposerSlashing sets an proposer slashing.
	SetProposerSlashing(ctx context.Context, proposerSlashing *ProposerSlashing) error

	// GetProposerSlashingsForSlotRange fetches all proposer slashings made for the given slot range.
	GetProposerSlashingsForSlotRange(ctx context.Context, minSlot uint64, maxSlot uint64) ([]*ProposerSlashing, error)
}

// ValidatorsService defines the validators service.
type ValidatorsService interface {
	// SetValidator sets a validator.
	SetValidator(ctx context.Context, validator *Validator) error

	// SetValidatorBalance sets a validator balance.
	SetValidatorBalance(ctx context.Context, validatorBalance *ValidatorBalance) error

	// GetValidators fetches the validators.
	GetValidators(ctx context.Context) ([]*Validator, error)

	// GetValidatorBalancesByValidatorsAndEpoch fetches the validator balances for the given validators and epoch.
	GetValidatorBalancesByValidatorsAndEpoch(ctx context.Context, validators []*Validator, epoch uint64) ([]*ValidatorBalance, error)
}

// BeaconCommitteeService defines the beacon committee service.
type BeaconCommitteeService interface {
	// SetBeaconComittee sets a beacon committee.
	SetBeaconCommittee(ctx context.Context, beaconCommittee *BeaconCommittee) error

	// GetBeaconComitteeBySlotAndIndex fetches the beacon committee with the given slot and index.
	GetBeaconCommitteeBySlotAndIndex(ctx context.Context, slot uint64, index uint64) (*BeaconCommittee, error)

	// GetAttesterDuties fetches the attester duties at the given slot range for the given validator indices.
	GetAttesterDuties(ctx context.Context, startSlot uint64, endSlot uint64, validatorIndices []uint64) ([]*AttesterDuty, error)
}

// ProposerDutyService defines the proposer duty service.
type ProposerDutyService interface {
	// SetProposerDuty sets a proposer duty.
	SetProposerDuty(ctx context.Context, proposerDuty *ProposerDuty) error
}

// Service defines the chain database service.
type Service interface {
	BeaconCommitteeService
	ProposerDutyService
	BlocksService
	AttestationsService
	VoluntaryExitsService
	AttesterSlashingsService
	ProposerSlashingsService
	ValidatorsService
	MetadataService

	// BeginTx begins a transaction.
	BeginTx(ctx context.Context) (context.Context, context.CancelFunc, error)
	// CommitTx commits a transaction.
	CommitTx(ctx context.Context) error
}
