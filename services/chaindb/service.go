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

	spec "github.com/attestantio/go-eth2-client/spec/phase0"
)

// AttestationsProvider defines functions to access attestations.
type AttestationsProvider interface {
	// AttestationsForBlock fetches all attestations made for the given block.
	AttestationsForBlock(ctx context.Context, blockRoot spec.Root) ([]*Attestation, error)

	// AttestationsInBlock fetches all attestations contained in the given block.
	AttestationsInBlock(ctx context.Context, blockRoot spec.Root) ([]*Attestation, error)

	// AttestationsForSlotRange fetches all attestations made for the given slot range.
	AttestationsForSlotRange(ctx context.Context, minSlot spec.Slot, maxSlot spec.Slot) ([]*Attestation, error)
}

// AttestationsSetter defines functions to create and update attestations.
type AttestationsSetter interface {
	// SetAttestation sets an attestation.
	SetAttestation(ctx context.Context, attestation *Attestation) error
}

// AttesterSlashingsSetter defines functions to create and update attester slashings.
type AttesterSlashingsSetter interface {
	// SetAttesterSlashing sets an attester slashing.
	SetAttesterSlashing(ctx context.Context, attesterSlashing *AttesterSlashing) error
}

// BeaconCommitteesProvider defines functions to access beacon committee information.
type BeaconCommitteesProvider interface {
	// BeaconComitteeBySlotAndIndex fetches the beacon committee with the given slot and index.
	BeaconCommitteeBySlotAndIndex(ctx context.Context, slot spec.Slot, index spec.CommitteeIndex) (*BeaconCommittee, error)

	// AttesterDuties fetches the attester duties at the given slot range for the given validator indices.
	AttesterDuties(ctx context.Context, startSlot spec.Slot, endSlot spec.Slot, validatorIndices []spec.ValidatorIndex) ([]*AttesterDuty, error)
}

// BeaconCommitteesSetter defines functions to create and update beacon committee information.
type BeaconCommitteesSetter interface {
	// SetBeaconComittee sets a beacon committee.
	SetBeaconCommittee(ctx context.Context, beaconCommittee *BeaconCommittee) error
}

// BlocksProvider defines functions to access blocks.
type BlocksProvider interface {
	// BlocksBySlot fetches all blocks with the given slot.
	BlocksBySlot(ctx context.Context, slot spec.Slot) ([]*Block, error)

	// BlockByRoot fetches the block with the given root.
	BlockByRoot(ctx context.Context, root spec.Root) (*Block, error)

	// BlocksByParentRoot fetches the blocks with the given parent root.
	BlocksByParentRoot(ctx context.Context, root spec.Root) ([]*Block, error)

	// EmptySlots fetches the slots in the given range without a block in the database.
	EmptySlots(ctx context.Context, minSlot spec.Slot, maxSlot spec.Slot) ([]spec.Slot, error)
}

// BlocksSetter defines functions to create and update blocks.
type BlocksSetter interface {
	// SetBlock sets a block.
	SetBlock(ctx context.Context, block *Block) error
}

// ProposerDutiesSetter defines the functions to create and update proposer duties.
type ProposerDutiesSetter interface {
	// SetProposerDuty sets a proposer duty.
	SetProposerDuty(ctx context.Context, proposerDuty *ProposerDuty) error
}

// ProposerSlashingsProvider defines functions to access proposer slashings.
type ProposerSlashingsProvider interface {
	// ProposerSlashingsForSlotRange fetches all proposer slashings made for the given slot range.
	ProposerSlashingsForSlotRange(ctx context.Context, minSlot spec.Slot, maxSlot spec.Slot) ([]*ProposerSlashing, error)
}

// ProposerSlashingsSetter defines functions to create and update proposer slashings.
type ProposerSlashingsSetter interface {
	// SetProposerSlashing sets an proposer slashing.
	SetProposerSlashing(ctx context.Context, proposerSlashing *ProposerSlashing) error
}

// ValidatorsProvider defines functions to access validator information.
type ValidatorsProvider interface {
	// Validators fetches all validators.
	Validators(ctx context.Context) ([]*Validator, error)

	// ValidatorsByPublicKey fetches all validators matching the given public keys.
	// This is a common starting point for external entities to query specific validators, as they should
	// always have the public key at a minimum, hence the return map keyed by public key.
	ValidatorsByPublicKey(ctx context.Context, pubKeys []spec.BLSPubKey) (map[spec.BLSPubKey]*Validator, error)

	// ValidatorsByIndex fetches all validators matching the given indices.
	ValidatorsByIndex(ctx context.Context, indices []spec.ValidatorIndex) (map[spec.ValidatorIndex]*Validator, error)

	// ValidatorBalancesByIndexAndEpoch fetches the validator balances for the given validators and epoch.
	ValidatorBalancesByIndexAndEpoch(
		ctx context.Context,
		indices []spec.ValidatorIndex,
		epoch spec.Epoch,
	) (
		map[spec.ValidatorIndex]*ValidatorBalance,
		error,
	)

	// ValidatorBalancesByIndexAndEpochRange fetches the validator balances for the given validators and epoch range.
	// Ranges are inclusive of start and exclusive of end i.e. a request with startEpoch 2 and endEpoch 4 will provide
	// balances for epochs 2 and 3.
	ValidatorBalancesByIndexAndEpochRange(
		ctx context.Context,
		indices []spec.ValidatorIndex,
		startEpoch spec.Epoch,
		endEpoch spec.Epoch,
	) (
		map[spec.ValidatorIndex][]*ValidatorBalance,
		error,
	)

	// ValidatorBalancesByIndexAndEpochs fetches the validator balances for the given validators at the specified epochs.
	ValidatorBalancesByIndexAndEpochs(
		ctx context.Context,
		indices []spec.ValidatorIndex,
		epochs []spec.Epoch,
	) (
		map[spec.ValidatorIndex][]*ValidatorBalance,
		error,
	)
}

// ValidatorsSetter defines functions to create and update validator information.
type ValidatorsSetter interface {
	// SetValidator sets a validator.
	SetValidator(ctx context.Context, validator *Validator) error

	// SetValidatorBalance sets a validator balance.
	SetValidatorBalance(ctx context.Context, validatorBalance *ValidatorBalance) error
}

// VoluntaryExitsSetter defines functions to create and update voluntary exits.
type VoluntaryExitsSetter interface {
	// SetVoluntaryExit sets a voluntary exit.
	SetVoluntaryExit(ctx context.Context, voluntaryExit *VoluntaryExit) error
}

// Service defines a minimal chain database service.
type Service interface {
	// BeginTx begins a transaction.
	BeginTx(ctx context.Context) (context.Context, context.CancelFunc, error)

	// CommitTx commits a transaction.
	CommitTx(ctx context.Context) error

	// SetMetadata sets a metadata key to a JSON value.
	SetMetadata(ctx context.Context, key string, value []byte) error

	// Metadata obtains the JSON value from a metadata key.
	Metadata(ctx context.Context, key string) ([]byte, error)
}
