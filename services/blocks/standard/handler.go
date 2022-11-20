// Copyright © 2020, 2021 Weald Technology Trading.
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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math/big"

	eth2client "github.com/attestantio/go-eth2-client"
	"github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/altair"
	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
)

// OnBeaconChainHeadUpdated receives beacon chain head updated notifications.
func (s *Service) OnBeaconChainHeadUpdated(
	ctx context.Context,
	slot phase0.Slot,
	blockRoot phase0.Root,
	stateRoot phase0.Root,
	// skipcq: RVV-A0005
	epochTransition bool,
) {
	// Only allow 1 handler to be active.
	acquired := s.activitySem.TryAcquire(1)
	if !acquired {
		log.Debug().Msg("Another handler running")
		return
	}
	defer s.activitySem.Release(1)

	log := log.With().Uint64("slot", uint64(slot)).Str("block_root", fmt.Sprintf("%#x", blockRoot)).Logger()
	log.Trace().
		Str("state_root", fmt.Sprintf("%#x", stateRoot)).
		Bool("epoch_transition", epochTransition).
		Msg("Handler called")

	if bytes.Equal(s.lastHandledBlockRoot[:], blockRoot[:]) {
		log.Trace().Msg("Already handled this block; ignoring")
		return
	}

	md, err := s.getMetadata(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to obtain metadata")
		return
	}

	s.catchup(ctx, md)

	s.lastHandledBlockRoot = blockRoot
	monitorBlockProcessed(slot)
}

func (s *Service) onNewLMDFinalizedBlock(root phase0.Root, slot phase0.Slot) {
	ctx, cancel, err := s.chainDB.BeginTx(context.Background())

	md, err := s.getMetadata(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to obtain metadata")
		cancel()
		return
	}

	if md.LMDLatestFinalizedSlot >= slot {
		log.Error().Err(err).Msg("trying to set a past LMD finalized block")
		cancel()
		return
	}

	md.LMDLatestFinalizedBlockRoot = root
	md.LMDLatestFinalizedSlot = slot

	if err := s.setMetadata(ctx, md); err != nil {
		log.Error().Err(err).Msg("Failed to set metadata")
		cancel()
		return
	}
	if err := s.chainDB.CommitTx(ctx); err != nil {
		log.Error().Err(err).Msg("Failed to commit transaction")
		cancel()
		return
	}
	log.Debug().Str("root", hex.EncodeToString(root[:])).Uint64("slot", uint64(slot)).Msg("stored new LMD finalized block")
}

func (s *Service) updateBlockForSlot(ctx context.Context, slot phase0.Slot) error {
	log := log.With().Uint64("slot", uint64(slot)).Logger()

	// Start off by seeing if we already have the block (unless we are re-fetching regardless).
	if !s.refetch {
		blocks, err := s.chainDB.(chaindb.BlocksProvider).BlocksBySlot(ctx, slot)
		if err == nil && len(blocks) > 0 {
			log.Debug().Msg("Already have this block; not re-fetching")
			return nil
		}
	}

	log.Trace().Msg("Updating block for slot")
	signedBlock, err := s.eth2Client.(eth2client.SignedBeaconBlockProvider).SignedBeaconBlock(ctx, fmt.Sprintf("%d", slot))
	if err != nil {
		return errors.Wrap(err, "failed to obtain beacon block for slot")
	}
	if signedBlock == nil {
		log.Debug().Msg("No beacon block obtained for slot")
		return nil
	}
	return s.OnBlock(ctx, signedBlock)
}

// OnBlock handles a block.
// This requires the context to hold an active transaction.
func (s *Service) OnBlock(ctx context.Context, signedBlock *spec.VersionedSignedBeaconBlock) error {
	// Update the block in the database.
	dbBlock, err := s.dbBlock(ctx, signedBlock)
	if err != nil {
		return errors.Wrap(err, "failed to obtain database block")
	}
	if err := s.blocksSetter.SetBlock(ctx, dbBlock); err != nil {
		return errors.Wrap(err, "failed to set block")
	}
	attestations, err := signedBlock.Attestations()
	if err != nil {
		return errors.Wrap(err, "failed to obtain attestations")
	}
	dbAttestations, err := s.dbAttestations(ctx, dbBlock.Slot, dbBlock.Root, attestations)
	if err != nil {
		return errors.Wrap(err, "failed to obtain database attestations")
	}
	s.lmdFinalizer.AddBlock(dbBlock, dbAttestations)
	switch signedBlock.Version {
	case spec.DataVersionPhase0:
		return s.onBlockPhase0(ctx, signedBlock.Phase0, dbBlock)
	case spec.DataVersionAltair:
		return s.onBlockAltair(ctx, signedBlock.Altair, dbBlock)
	case spec.DataVersionBellatrix:
		return s.onBlockBellatrix(ctx, signedBlock.Bellatrix, dbBlock)
	default:
		return errors.New("unknown block version")
	}
}

func (s *Service) onBlockPhase0(ctx context.Context, signedBlock *phase0.SignedBeaconBlock, dbBlock *chaindb.Block) error {
	if err := s.updateAttestationsForBlock(ctx,
		signedBlock.Message.Slot,
		dbBlock.Root,
		signedBlock.Message.Body.Attestations); err != nil {
		return errors.Wrap(err, "failed to update attestations")
	}
	if err := s.updateProposerSlashingsForBlock(ctx,
		signedBlock.Message.Slot,
		dbBlock.Root,
		signedBlock.Message.Body.ProposerSlashings); err != nil {
		return errors.Wrap(err, "failed to update proposer slashings")
	}
	if err := s.updateAttesterSlashingsForBlock(ctx,
		signedBlock.Message.Slot,
		dbBlock.Root,
		signedBlock.Message.Body.AttesterSlashings); err != nil {
		return errors.Wrap(err, "failed to update attester slashings")
	}
	if err := s.updateDepositsForBlock(ctx,
		signedBlock.Message.Slot,
		dbBlock.Root,
		signedBlock.Message.Body.Deposits); err != nil {
		return errors.Wrap(err, "failed to update deposits")
	}
	if err := s.updateVoluntaryExitsForBlock(ctx,
		signedBlock.Message.Slot,
		dbBlock.Root,
		signedBlock.Message.Body.VoluntaryExits); err != nil {
		return errors.Wrap(err, "failed to update voluntary exits")
	}
	return nil
}

func (s *Service) onBlockAltair(ctx context.Context, signedBlock *altair.SignedBeaconBlock, dbBlock *chaindb.Block) error {
	if err := s.updateAttestationsForBlock(ctx,
		signedBlock.Message.Slot,
		dbBlock.Root,
		signedBlock.Message.Body.Attestations); err != nil {
		return errors.Wrap(err, "failed to update attestations")
	}
	if err := s.updateProposerSlashingsForBlock(ctx,
		signedBlock.Message.Slot,
		dbBlock.Root,
		signedBlock.Message.Body.ProposerSlashings); err != nil {
		return errors.Wrap(err, "failed to update proposer slashings")
	}
	if err := s.updateAttesterSlashingsForBlock(ctx,
		signedBlock.Message.Slot,
		dbBlock.Root,
		signedBlock.Message.Body.AttesterSlashings); err != nil {
		return errors.Wrap(err, "failed to update attester slashings")
	}
	if err := s.updateDepositsForBlock(ctx,
		signedBlock.Message.Slot,
		dbBlock.Root,
		signedBlock.Message.Body.Deposits); err != nil {
		return errors.Wrap(err, "failed to update deposits")
	}
	if err := s.updateVoluntaryExitsForBlock(ctx,
		signedBlock.Message.Slot,
		dbBlock.Root,
		signedBlock.Message.Body.VoluntaryExits); err != nil {
		return errors.Wrap(err, "failed to update voluntary exits")
	}
	if err := s.updateSyncAggregateForBlock(ctx,
		signedBlock.Message.Slot,
		dbBlock.Root,
		signedBlock.Message.Body.SyncAggregate); err != nil {
		return errors.Wrap(err, "failed to update sync aggregate")
	}
	return nil
}

func (s *Service) onBlockBellatrix(ctx context.Context, signedBlock *bellatrix.SignedBeaconBlock, dbBlock *chaindb.Block) error {
	if err := s.updateAttestationsForBlock(ctx,
		signedBlock.Message.Slot,
		dbBlock.Root,
		signedBlock.Message.Body.Attestations); err != nil {
		return errors.Wrap(err, "failed to update attestations")
	}
	if err := s.updateProposerSlashingsForBlock(ctx,
		signedBlock.Message.Slot,
		dbBlock.Root,
		signedBlock.Message.Body.ProposerSlashings); err != nil {
		return errors.Wrap(err, "failed to update proposer slashings")
	}
	if err := s.updateAttesterSlashingsForBlock(ctx,
		signedBlock.Message.Slot,
		dbBlock.Root,
		signedBlock.Message.Body.AttesterSlashings); err != nil {
		return errors.Wrap(err, "failed to update attester slashings")
	}
	if err := s.updateDepositsForBlock(ctx,
		signedBlock.Message.Slot,
		dbBlock.Root,
		signedBlock.Message.Body.Deposits); err != nil {
		return errors.Wrap(err, "failed to update deposits")
	}
	if err := s.updateVoluntaryExitsForBlock(ctx,
		signedBlock.Message.Slot,
		dbBlock.Root,
		signedBlock.Message.Body.VoluntaryExits); err != nil {
		return errors.Wrap(err, "failed to update voluntary exits")
	}
	if err := s.updateSyncAggregateForBlock(ctx,
		signedBlock.Message.Slot,
		dbBlock.Root,
		signedBlock.Message.Body.SyncAggregate); err != nil {
		return errors.Wrap(err, "failed to update sync aggregate")
	}
	return nil
}

func (s *Service) updateAttestationsForBlock(ctx context.Context,
	slot phase0.Slot,
	blockRoot phase0.Root,
	attestations []*phase0.Attestation,
) error {
	dbAttestations, err := s.dbAttestations(ctx, slot, blockRoot, attestations)
	if err != nil {
		return errors.Wrap(err, "failed to obtain database attestations")
	}
	for _, dbAttestation := range dbAttestations {
		if err := s.attestationsSetter.SetAttestation(ctx, dbAttestation); err != nil {
			return errors.Wrap(err, "failed to set attestation")
		}
	}
	return nil
}

func (s *Service) updateProposerSlashingsForBlock(ctx context.Context,
	slot phase0.Slot,
	blockRoot phase0.Root,
	proposerSlashings []*phase0.ProposerSlashing,
) error {
	for i, proposerSlashing := range proposerSlashings {
		dbProposerSlashing, err := s.dbProposerSlashing(ctx, slot, blockRoot, uint64(i), proposerSlashing)
		if err != nil {
			return errors.Wrap(err, "failed to obtain database proposer slashing")
		}
		if err := s.proposerSlashingsSetter.SetProposerSlashing(ctx, dbProposerSlashing); err != nil {
			return errors.Wrap(err, "failed to set proposer slashing")
		}
	}
	return nil
}

func (s *Service) updateAttesterSlashingsForBlock(ctx context.Context,
	slot phase0.Slot,
	blockRoot phase0.Root,
	attesterSlashings []*phase0.AttesterSlashing,
) error {
	for i, attesterSlashing := range attesterSlashings {
		dbAttesterSlashing, err := s.dbAttesterSlashing(ctx, slot, blockRoot, uint64(i), attesterSlashing)
		if err != nil {
			return errors.Wrap(err, "failed to obtain database attester slashing")
		}
		if err := s.attesterSlashingsSetter.SetAttesterSlashing(ctx, dbAttesterSlashing); err != nil {
			return errors.Wrap(err, "failed to set attester slashing")
		}
	}
	return nil
}

func (s *Service) updateDepositsForBlock(ctx context.Context,
	slot phase0.Slot,
	blockRoot phase0.Root,
	deposits []*phase0.Deposit,
) error {
	for i, deposit := range deposits {
		dbDeposit, err := s.dbDeposit(ctx, slot, blockRoot, uint64(i), deposit)
		if err != nil {
			return errors.Wrap(err, "failed to obtain database deposit")
		}
		if err := s.depositsSetter.SetDeposit(ctx, dbDeposit); err != nil {
			return errors.Wrap(err, "failed to set deposit")
		}
	}
	return nil
}

func (s *Service) updateVoluntaryExitsForBlock(ctx context.Context,
	slot phase0.Slot,
	blockRoot phase0.Root,
	voluntaryExits []*phase0.SignedVoluntaryExit,
) error {
	for i, voluntaryExit := range voluntaryExits {
		dbVoluntaryExit, err := s.dbVoluntaryExit(ctx, slot, blockRoot, uint64(i), voluntaryExit)
		if err != nil {
			return errors.Wrap(err, "failed to obtain database voluntary exit")
		}
		if err := s.voluntaryExitsSetter.SetVoluntaryExit(ctx, dbVoluntaryExit); err != nil {
			return errors.Wrap(err, "failed to set voluntary exit")
		}
	}
	return nil
}

func (s *Service) updateSyncAggregateForBlock(ctx context.Context,
	slot phase0.Slot,
	blockRoot phase0.Root,
	syncAggregate *altair.SyncAggregate,
) error {
	dbSyncAggregate, err := s.dbSyncAggregate(ctx, slot, blockRoot, syncAggregate)
	if err != nil {
		return errors.Wrap(err, "failed to obtain database sync aggregate")
	}

	if err := s.syncAggregateSetter.SetSyncAggregate(ctx, dbSyncAggregate); err != nil {
		return errors.Wrap(err, "failed to set sync aggregate")
	}
	return nil
}

func (s *Service) dbBlock(
	ctx context.Context,
	block *spec.VersionedSignedBeaconBlock,
) (*chaindb.Block, error) {
	switch block.Version {
	case spec.DataVersionPhase0:
		return s.dbBlockPhase0(ctx, block.Phase0.Message)
	case spec.DataVersionAltair:
		return s.dbBlockAltair(ctx, block.Altair.Message)
	case spec.DataVersionBellatrix:
		return s.dbBlockBellatrix(ctx, block.Bellatrix.Message)
	default:
		return nil, errors.New("unknown block version")
	}
}

func (*Service) dbBlockPhase0(
	// skipcq: RVV-B0012
	ctx context.Context,
	block *phase0.BeaconBlock,
) (*chaindb.Block, error) {
	bodyRoot, err := block.Body.HashTreeRoot()
	if err != nil {
		return nil, errors.Wrap(err, "failed to calculate body root")
	}

	header := &phase0.BeaconBlockHeader{
		Slot:          block.Slot,
		ProposerIndex: block.ProposerIndex,
		ParentRoot:    block.ParentRoot,
		StateRoot:     block.StateRoot,
		BodyRoot:      bodyRoot,
	}
	root, err := header.HashTreeRoot()
	if err != nil {
		return nil, errors.Wrap(err, "failed to calculate block root")
	}

	dbBlock := &chaindb.Block{
		Slot:             block.Slot,
		ProposerIndex:    block.ProposerIndex,
		Root:             root,
		Graffiti:         block.Body.Graffiti[:],
		RANDAOReveal:     block.Body.RANDAOReveal,
		BodyRoot:         bodyRoot,
		ParentRoot:       block.ParentRoot,
		StateRoot:        block.StateRoot,
		ETH1BlockHash:    block.Body.ETH1Data.BlockHash,
		ETH1DepositCount: block.Body.ETH1Data.DepositCount,
		ETH1DepositRoot:  block.Body.ETH1Data.DepositRoot,
	}

	return dbBlock, nil
}

func (*Service) dbBlockAltair(
	// skipcq: RVV-B0012
	ctx context.Context,
	block *altair.BeaconBlock,
) (*chaindb.Block, error) {
	bodyRoot, err := block.Body.HashTreeRoot()
	if err != nil {
		return nil, errors.Wrap(err, "failed to calculate body root")
	}

	header := &phase0.BeaconBlockHeader{
		Slot:          block.Slot,
		ProposerIndex: block.ProposerIndex,
		ParentRoot:    block.ParentRoot,
		StateRoot:     block.StateRoot,
		BodyRoot:      bodyRoot,
	}
	root, err := header.HashTreeRoot()
	if err != nil {
		return nil, errors.Wrap(err, "failed to calculate block root")
	}

	dbBlock := &chaindb.Block{
		Slot:             block.Slot,
		ProposerIndex:    block.ProposerIndex,
		Root:             root,
		Graffiti:         block.Body.Graffiti[:],
		RANDAOReveal:     block.Body.RANDAOReveal,
		BodyRoot:         bodyRoot,
		ParentRoot:       block.ParentRoot,
		StateRoot:        block.StateRoot,
		ETH1BlockHash:    block.Body.ETH1Data.BlockHash,
		ETH1DepositCount: block.Body.ETH1Data.DepositCount,
		ETH1DepositRoot:  block.Body.ETH1Data.DepositRoot,
	}

	return dbBlock, nil
}

func (*Service) dbBlockBellatrix(
	// skipcq: RVV-B0012
	ctx context.Context,
	block *bellatrix.BeaconBlock,
) (*chaindb.Block, error) {
	bodyRoot, err := block.Body.HashTreeRoot()
	if err != nil {
		return nil, errors.Wrap(err, "failed to calculate body root")
	}

	header := &phase0.BeaconBlockHeader{
		Slot:          block.Slot,
		ProposerIndex: block.ProposerIndex,
		ParentRoot:    block.ParentRoot,
		StateRoot:     block.StateRoot,
		BodyRoot:      bodyRoot,
	}
	root, err := header.HashTreeRoot()
	if err != nil {
		return nil, errors.Wrap(err, "failed to calculate block root")
	}

	// base fee per gas is stored little-endian but we need it
	// big-endian for big.Int.
	var baseFeePerGasBEBytes [32]byte
	for i := 0; i < 32; i++ {
		baseFeePerGasBEBytes[i] = block.Body.ExecutionPayload.BaseFeePerGas[32-1-i]
	}
	baseFeePerGas := new(big.Int).SetBytes(baseFeePerGasBEBytes[:])

	dbBlock := &chaindb.Block{
		Slot:             block.Slot,
		ProposerIndex:    block.ProposerIndex,
		Root:             root,
		Graffiti:         block.Body.Graffiti[:],
		RANDAOReveal:     block.Body.RANDAOReveal,
		BodyRoot:         bodyRoot,
		ParentRoot:       block.ParentRoot,
		StateRoot:        block.StateRoot,
		ETH1BlockHash:    block.Body.ETH1Data.BlockHash,
		ETH1DepositCount: block.Body.ETH1Data.DepositCount,
		ETH1DepositRoot:  block.Body.ETH1Data.DepositRoot,
		ExecutionPayload: &chaindb.ExecutionPayload{
			ParentHash:    block.Body.ExecutionPayload.ParentHash,
			FeeRecipient:  block.Body.ExecutionPayload.FeeRecipient,
			StateRoot:     block.Body.ExecutionPayload.StateRoot,
			ReceiptsRoot:  block.Body.ExecutionPayload.ReceiptsRoot,
			LogsBloom:     block.Body.ExecutionPayload.LogsBloom,
			PrevRandao:    block.Body.ExecutionPayload.PrevRandao,
			BlockNumber:   block.Body.ExecutionPayload.BlockNumber,
			GasLimit:      block.Body.ExecutionPayload.GasLimit,
			GasUsed:       block.Body.ExecutionPayload.GasUsed,
			Timestamp:     block.Body.ExecutionPayload.Timestamp,
			ExtraData:     block.Body.ExecutionPayload.ExtraData,
			BaseFeePerGas: baseFeePerGas,
			BlockHash:     block.Body.ExecutionPayload.BlockHash,
		},
	}

	return dbBlock, nil
}

func (s *Service) dbAttestation(
	ctx context.Context,
	inclusionSlot phase0.Slot,
	blockRoot phase0.Root,
	inclusionIndex uint64,
	attestation *phase0.Attestation,
	beaconCommittees map[phase0.Slot]map[phase0.CommitteeIndex]*chaindb.BeaconCommittee,
) (*chaindb.Attestation, error) {
	var aggregationIndices []phase0.ValidatorIndex

	committee, err := s.beaconCommittee(ctx, attestation.Data.Slot, attestation.Data.Index, beaconCommittees)
	if err != nil {
		return nil, err
	}
	if committee == nil {
		return nil, errors.New("no committee obtained")
	}

	if len(committee.Committee) == int(attestation.AggregationBits.Len()) {
		aggregationIndices = make([]phase0.ValidatorIndex, 0, len(committee.Committee))
		for i := uint64(0); i < attestation.AggregationBits.Len(); i++ {
			if attestation.AggregationBits.BitAt(i) {
				aggregationIndices = append(aggregationIndices, committee.Committee[i])
			}
		}
	} else {
		log.Warn().Int("committee_length", len(committee.Committee)).Uint64("aggregation_bits_length", attestation.AggregationBits.Len()).Msg("Attestation and committee size mismatch")
	}

	dbAttestation := &chaindb.Attestation{
		InclusionSlot:      inclusionSlot,
		InclusionBlockRoot: blockRoot,
		InclusionIndex:     inclusionIndex,
		Slot:               attestation.Data.Slot,
		CommitteeIndex:     attestation.Data.Index,
		BeaconBlockRoot:    attestation.Data.BeaconBlockRoot,
		AggregationBits:    []byte(attestation.AggregationBits),
		AggregationIndices: aggregationIndices,
		SourceEpoch:        attestation.Data.Source.Epoch,
		SourceRoot:         attestation.Data.Source.Root,
		TargetEpoch:        attestation.Data.Target.Epoch,
		TargetRoot:         attestation.Data.Target.Root,
	}

	return dbAttestation, nil
}

func (s *Service) dbAttestations(
	ctx context.Context,
	inclusionSlot phase0.Slot,
	blockRoot phase0.Root,
	attestations []*phase0.Attestation,
) ([]*chaindb.Attestation, error) {
	beaconCommittees := make(map[phase0.Slot]map[phase0.CommitteeIndex]*chaindb.BeaconCommittee)
	result := []*chaindb.Attestation{}
	for i, attestation := range attestations {
		dbAttestation, err := s.dbAttestation(ctx, inclusionSlot, blockRoot, uint64(i), attestation, beaconCommittees)
		if err != nil {
			return nil, errors.Wrap(err, "failed to obtain database attestation")
		}

		result = append(result, dbAttestation)
	}
	return result, nil
}

func (s *Service) dbSyncAggregate(
	ctx context.Context,
	slot phase0.Slot,
	blockRoot phase0.Root,
	syncAggregate *altair.SyncAggregate,
) (*chaindb.SyncAggregate, error) {
	period := s.chainTime.SlotToSyncCommitteePeriod(slot)
	var syncCommittee *chaindb.SyncCommittee
	var exists bool
	if syncCommittee, exists = s.syncCommittees[period]; !exists {
		// Fetch the sync committee.
		var err error
		syncCommittee, err = s.syncCommitteesProvider.SyncCommittee(ctx, period)
		if err != nil {
			log.Warn().Err(err).Uint64("slot", uint64(slot)).Uint64("sync_committee_period", period).Msg("Failed to obtain sync committee period")
			return nil, errors.Wrap(err, "failed to obtain sync committee")
		}
		s.syncCommittees[period] = syncCommittee
		// Remove older sync committee.
		if period > 1 {
			delete(s.syncCommittees, period-2)
		}
	}

	indices := make([]phase0.ValidatorIndex, 0, syncAggregate.SyncCommitteeBits.Count())
	for i := 0; i < int(syncAggregate.SyncCommitteeBits.Len()); i++ {
		if syncAggregate.SyncCommitteeBits.BitAt(uint64(i)) {
			indices = append(indices, syncCommittee.Committee[i])
		}
	}

	dbSyncAggregate := &chaindb.SyncAggregate{
		InclusionSlot:      slot,
		InclusionBlockRoot: blockRoot,
		Bits:               syncAggregate.SyncCommitteeBits,
		Indices:            indices,
	}

	return dbSyncAggregate, nil
}

func (*Service) dbDeposit(
	// skipcq: RVV-B0012
	ctx context.Context,
	slot phase0.Slot,
	blockRoot phase0.Root,
	index uint64,
	deposit *phase0.Deposit,
) (*chaindb.Deposit, error) {
	dbDeposit := &chaindb.Deposit{
		InclusionSlot:         slot,
		InclusionBlockRoot:    blockRoot,
		InclusionIndex:        index,
		ValidatorPubKey:       deposit.Data.PublicKey,
		WithdrawalCredentials: deposit.Data.WithdrawalCredentials,
		Amount:                deposit.Data.Amount,
	}

	return dbDeposit, nil
}

func (*Service) dbVoluntaryExit(
	// skipcq: RVV-B0012
	ctx context.Context,
	slot phase0.Slot,
	blockRoot phase0.Root,
	index uint64,
	voluntaryExit *phase0.SignedVoluntaryExit,
) (*chaindb.VoluntaryExit, error) {
	dbVoluntaryExit := &chaindb.VoluntaryExit{
		InclusionSlot:      slot,
		InclusionBlockRoot: blockRoot,
		InclusionIndex:     index,
		ValidatorIndex:     voluntaryExit.Message.ValidatorIndex,
		Epoch:              voluntaryExit.Message.Epoch,
	}

	return dbVoluntaryExit, nil
}

func (*Service) dbAttesterSlashing(
	// skipcq: RVV-B0012
	ctx context.Context,
	slot phase0.Slot,
	blockRoot phase0.Root,
	index uint64,
	attesterSlashing *phase0.AttesterSlashing,
) (*chaindb.AttesterSlashing, error) {
	// This is temporary, until attester fastssz is fixed to support []phase0.ValidatorIndex.
	attestation1Indices := make([]phase0.ValidatorIndex, len(attesterSlashing.Attestation1.AttestingIndices))
	for i := range attesterSlashing.Attestation1.AttestingIndices {
		attestation1Indices[i] = phase0.ValidatorIndex(attesterSlashing.Attestation1.AttestingIndices[i])
	}
	attestation2Indices := make([]phase0.ValidatorIndex, len(attesterSlashing.Attestation2.AttestingIndices))
	for i := range attesterSlashing.Attestation2.AttestingIndices {
		attestation2Indices[i] = phase0.ValidatorIndex(attesterSlashing.Attestation2.AttestingIndices[i])
	}

	dbAttesterSlashing := &chaindb.AttesterSlashing{
		InclusionSlot:               slot,
		InclusionBlockRoot:          blockRoot,
		InclusionIndex:              index,
		Attestation1Indices:         attestation1Indices,
		Attestation1Slot:            attesterSlashing.Attestation1.Data.Slot,
		Attestation1CommitteeIndex:  attesterSlashing.Attestation1.Data.Index,
		Attestation1BeaconBlockRoot: attesterSlashing.Attestation1.Data.BeaconBlockRoot,
		Attestation1SourceEpoch:     attesterSlashing.Attestation1.Data.Source.Epoch,
		Attestation1SourceRoot:      attesterSlashing.Attestation1.Data.Source.Root,
		Attestation1TargetEpoch:     attesterSlashing.Attestation1.Data.Target.Epoch,
		Attestation1TargetRoot:      attesterSlashing.Attestation1.Data.Target.Root,
		Attestation1Signature:       attesterSlashing.Attestation1.Signature,
		Attestation2Indices:         attestation2Indices,
		Attestation2Slot:            attesterSlashing.Attestation2.Data.Slot,
		Attestation2CommitteeIndex:  attesterSlashing.Attestation2.Data.Index,
		Attestation2BeaconBlockRoot: attesterSlashing.Attestation2.Data.BeaconBlockRoot,
		Attestation2SourceEpoch:     attesterSlashing.Attestation2.Data.Source.Epoch,
		Attestation2SourceRoot:      attesterSlashing.Attestation2.Data.Source.Root,
		Attestation2TargetEpoch:     attesterSlashing.Attestation2.Data.Target.Epoch,
		Attestation2TargetRoot:      attesterSlashing.Attestation2.Data.Target.Root,
		Attestation2Signature:       attesterSlashing.Attestation2.Signature,
	}

	return dbAttesterSlashing, nil
}

func (*Service) dbProposerSlashing(
	// skipcq: RVV-B0012
	ctx context.Context,
	slot phase0.Slot,
	blockRoot phase0.Root,
	index uint64,
	proposerSlashing *phase0.ProposerSlashing,
) (*chaindb.ProposerSlashing, error) {
	header1 := &phase0.BeaconBlockHeader{
		Slot:          proposerSlashing.SignedHeader1.Message.Slot,
		ProposerIndex: proposerSlashing.SignedHeader1.Message.ProposerIndex,
		ParentRoot:    proposerSlashing.SignedHeader1.Message.ParentRoot,
		StateRoot:     proposerSlashing.SignedHeader1.Message.StateRoot,
		BodyRoot:      proposerSlashing.SignedHeader1.Message.BodyRoot,
	}
	block1Root, err := header1.HashTreeRoot()
	if err != nil {
		return nil, errors.Wrap(err, "failed to calculate hash tree root of header 1")
	}

	header2 := &phase0.BeaconBlockHeader{
		Slot:          proposerSlashing.SignedHeader2.Message.Slot,
		ProposerIndex: proposerSlashing.SignedHeader2.Message.ProposerIndex,
		ParentRoot:    proposerSlashing.SignedHeader2.Message.ParentRoot,
		StateRoot:     proposerSlashing.SignedHeader2.Message.StateRoot,
		BodyRoot:      proposerSlashing.SignedHeader2.Message.BodyRoot,
	}
	block2Root, err := header2.HashTreeRoot()
	if err != nil {
		return nil, errors.Wrap(err, "failed to calculate hash tree root of header 2")
	}

	dbProposerSlashing := &chaindb.ProposerSlashing{
		InclusionSlot:        slot,
		InclusionBlockRoot:   blockRoot,
		InclusionIndex:       index,
		Block1Root:           block1Root,
		Header1Slot:          proposerSlashing.SignedHeader1.Message.Slot,
		Header1ProposerIndex: proposerSlashing.SignedHeader1.Message.ProposerIndex,
		Header1ParentRoot:    proposerSlashing.SignedHeader1.Message.ParentRoot,
		Header1StateRoot:     proposerSlashing.SignedHeader1.Message.StateRoot,
		Header1BodyRoot:      proposerSlashing.SignedHeader1.Message.BodyRoot,
		Header1Signature:     proposerSlashing.SignedHeader1.Signature,
		Block2Root:           block2Root,
		Header2Slot:          proposerSlashing.SignedHeader2.Message.Slot,
		Header2ProposerIndex: proposerSlashing.SignedHeader2.Message.ProposerIndex,
		Header2ParentRoot:    proposerSlashing.SignedHeader2.Message.ParentRoot,
		Header2StateRoot:     proposerSlashing.SignedHeader2.Message.StateRoot,
		Header2BodyRoot:      proposerSlashing.SignedHeader2.Message.BodyRoot,
		Header2Signature:     proposerSlashing.SignedHeader2.Signature,
	}

	return dbProposerSlashing, nil
}

func (s *Service) beaconCommittee(ctx context.Context,
	slot phase0.Slot,
	index phase0.CommitteeIndex,
	beaconCommittees map[phase0.Slot]map[phase0.CommitteeIndex]*chaindb.BeaconCommittee,
) (
	*chaindb.BeaconCommittee,
	error,
) {
	// Check in the map.
	_, exists := beaconCommittees[slot]
	if !exists {
		beaconCommittees[slot] = make(map[phase0.CommitteeIndex]*chaindb.BeaconCommittee)
	}
	beaconCommittee, exists := beaconCommittees[slot][index]
	if exists {
		return beaconCommittee, nil
	}
	// Try to fetch from local provider
	var err error
	beaconCommittee, err = s.beaconCommitteesProvider.BeaconCommitteeBySlotAndIndex(ctx, slot, index)
	if err == nil && beaconCommittee != nil {
		beaconCommittees[slot][index] = beaconCommittee
		return beaconCommittee, nil
	}
	// Try to fetch from the chain.
	chainBeaconCommittees, err := s.eth2Client.(eth2client.BeaconCommitteesProvider).BeaconCommittees(ctx, fmt.Sprintf("%d", slot))
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch beacon committees")
	}
	log.Debug().Uint64("slot", uint64(slot)).Msg("Obtained beacon committees from API")

	for _, chainBeaconCommittee := range chainBeaconCommittees {
		newBeaconCommittee := &chaindb.BeaconCommittee{
			Slot:      chainBeaconCommittee.Slot,
			Index:     chainBeaconCommittee.Index,
			Committee: chainBeaconCommittee.Validators,
		}
		_, slotExists := beaconCommittees[chainBeaconCommittee.Slot]
		if !slotExists {
			beaconCommittees[chainBeaconCommittee.Slot] = make(map[phase0.CommitteeIndex]*chaindb.BeaconCommittee)
		}
		beaconCommittees[chainBeaconCommittee.Slot][chainBeaconCommittee.Index] = newBeaconCommittee
	}

	beaconCommittee, exists = beaconCommittees[slot][index]
	if exists {
		return beaconCommittee, nil
	}

	return nil, errors.Wrap(err, "failed to obtain beacon committees")
}
