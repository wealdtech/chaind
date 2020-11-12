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
	"fmt"

	eth2client "github.com/attestantio/go-eth2-client"
	spec "github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/wealdtech/chaind/services/chaindb"
)

// OnBeaconChainHeadUpdated receives beacon chain head updated notifications.
func (s *Service) OnBeaconChainHeadUpdated(
	ctx context.Context,
	slot spec.Slot,
	blockRoot spec.Root,
	stateRoot spec.Root,
	epochTransition bool,
) {
	log := log.With().Uint64("slot", uint64(slot)).Logger()

	log.Trace().Msg("Handler called")
	ctx, cancel, err := s.chainDB.BeginTx(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to begin transaction")
	}

	md, err := s.getMetadata(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to obtain metadata")
	}

	if err := s.updateBlockForSlot(ctx, slot); err != nil {
		log.Error().Err(err).Msg("Failed to update block on chain head updated")
		md.MissedSlots = append(md.MissedSlots, slot)
	}

	md.LatestSlot = slot
	if err := s.setMetadata(ctx, md); err != nil {
		log.Error().Err(err).Msg("Failed to set metadata")
	}

	if err := s.chainDB.CommitTx(ctx); err != nil {
		log.Error().Err(err).Msg("Failed to commit transaction")
		cancel()
		return
	}

	log.Trace().Msg("Stored block")
}

func (s *Service) updateBlockForSlot(ctx context.Context, slot spec.Slot) error {
	log := log.With().Uint64("slot", uint64(slot)).Logger()

	log.Trace().Msg("Updating block for slot")
	signedBlock, err := s.eth2Client.(eth2client.SignedBeaconBlockProvider).SignedBeaconBlock(ctx, fmt.Sprintf("%d", slot))
	if err != nil {
		return errors.Wrap(err, "failed to obtain beacon block for slot")
	}
	if signedBlock == nil {
		log.Debug().Msg("No beacon block obtained for slot")
		return nil
	}

	// Update the block in the database.
	dbBlock, err := s.dbBlock(ctx, signedBlock.Message)
	if err != nil {
		return errors.Wrap(err, "failed to obtain database block")
	}
	if err := s.chainDB.SetBlock(ctx, dbBlock); err != nil {
		return errors.Wrap(err, "failed to set block")
	}
	if err := s.updateAttestationsForBlock(ctx, signedBlock, dbBlock.Root); err != nil {
		return errors.Wrap(err, "failed to update attestations")
	}
	if err := s.updateProposerSlashingsForBlock(ctx, signedBlock, dbBlock.Root); err != nil {
		return errors.Wrap(err, "failed to update proposer slashings")
	}
	if err := s.updateAttesterSlashingsForBlock(ctx, signedBlock, dbBlock.Root); err != nil {
		return errors.Wrap(err, "failed to update attester slashings")
	}
	if err := s.updateVoluntaryExitsForBlock(ctx, signedBlock, dbBlock.Root); err != nil {
		return errors.Wrap(err, "failed to update voluntary exits")
	}

	return nil
}

func (s *Service) updateAttestationsForBlock(ctx context.Context, signedBlock *spec.SignedBeaconBlock, blockRoot spec.Root) error {
	for i, attestation := range signedBlock.Message.Body.Attestations {
		dbAttestation, err := s.dbAttestation(ctx, signedBlock.Message.Slot, blockRoot, uint64(i), attestation)
		if err != nil {
			return errors.Wrap(err, "failed to obtain database attestation")
		}
		if err := s.chainDB.SetAttestation(ctx, dbAttestation); err != nil {
			return errors.Wrap(err, "failed to set attestation")
		}
	}
	return nil
}

func (s *Service) updateProposerSlashingsForBlock(ctx context.Context, signedBlock *spec.SignedBeaconBlock, blockRoot spec.Root) error {
	for i, proposerSlashing := range signedBlock.Message.Body.ProposerSlashings {
		dbProposerSlashing, err := s.dbProposerSlashing(ctx, signedBlock.Message.Slot, blockRoot, uint64(i), proposerSlashing)
		if err != nil {
			return errors.Wrap(err, "failed to obtain database proposer slashing")
		}
		if err := s.chainDB.SetProposerSlashing(ctx, dbProposerSlashing); err != nil {
			return errors.Wrap(err, "failed to set proposer slashing")
		}
	}
	return nil
}

func (s *Service) updateAttesterSlashingsForBlock(ctx context.Context, signedBlock *spec.SignedBeaconBlock, blockRoot spec.Root) error {
	for i, attesterSlashing := range signedBlock.Message.Body.AttesterSlashings {
		dbAttesterSlashing, err := s.dbAttesterSlashing(ctx, signedBlock.Message.Slot, blockRoot, uint64(i), attesterSlashing)
		if err != nil {
			return errors.Wrap(err, "failed to obtain database attester slashing")
		}
		if err := s.chainDB.SetAttesterSlashing(ctx, dbAttesterSlashing); err != nil {
			return errors.Wrap(err, "failed to set attester slashing")
		}
	}
	return nil
}

func (s *Service) updateVoluntaryExitsForBlock(ctx context.Context, signedBlock *spec.SignedBeaconBlock, blockRoot spec.Root) error {
	for i, voluntaryExit := range signedBlock.Message.Body.VoluntaryExits {
		dbVoluntaryExit, err := s.dbVoluntaryExit(ctx, signedBlock.Message.Slot, blockRoot, uint64(i), voluntaryExit)
		if err != nil {
			return errors.Wrap(err, "failed to obtain database voluntary exit")
		}
		if err := s.chainDB.SetVoluntaryExit(ctx, dbVoluntaryExit); err != nil {
			return errors.Wrap(err, "failed to set voluntary exit")
		}
	}
	return nil
}

func (s *Service) dbBlock(
	ctx context.Context,
	block *spec.BeaconBlock,
) (*chaindb.Block, error) {
	bodyRoot, err := block.Body.HashTreeRoot()
	if err != nil {
		return nil, errors.Wrap(err, "failed to calculate body root")
	}

	header := &spec.BeaconBlockHeader{
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
		Graffiti:         block.Body.Graffiti,
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

func (s *Service) dbAttestation(
	ctx context.Context,
	slot spec.Slot,
	blockRoot spec.Root,
	index uint64,
	attestation *spec.Attestation,
) (*chaindb.Attestation, error) {
	dbAttestation := &chaindb.Attestation{
		InclusionSlot:      slot,
		InclusionBlockRoot: blockRoot,
		InclusionIndex:     index,
		Slot:               attestation.Data.Slot,
		CommitteeIndex:     attestation.Data.Index,
		BeaconBlockRoot:    attestation.Data.BeaconBlockRoot,
		AggregationBits:    []byte(attestation.AggregationBits),
		SourceEpoch:        attestation.Data.Source.Epoch,
		SourceRoot:         attestation.Data.Source.Root,
		TargetEpoch:        attestation.Data.Target.Epoch,
		TargetRoot:         attestation.Data.Target.Root,
	}

	return dbAttestation, nil
}

func (s *Service) dbVoluntaryExit(
	ctx context.Context,
	slot spec.Slot,
	blockRoot spec.Root,
	index uint64,
	voluntaryExit *spec.SignedVoluntaryExit,
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

func (s *Service) dbAttesterSlashing(
	ctx context.Context,
	slot spec.Slot,
	blockRoot spec.Root,
	index uint64,
	attesterSlashing *spec.AttesterSlashing,
) (*chaindb.AttesterSlashing, error) {
	// This is temporary, until attester fastssz is fixed to support []spec.ValidatorIndex.
	attestation1Indices := make([]spec.ValidatorIndex, len(attesterSlashing.Attestation1.AttestingIndices))
	for i := range attesterSlashing.Attestation1.AttestingIndices {
		attestation1Indices[i] = spec.ValidatorIndex(attesterSlashing.Attestation1.AttestingIndices[i])
	}
	attestation2Indices := make([]spec.ValidatorIndex, len(attesterSlashing.Attestation2.AttestingIndices))
	for i := range attesterSlashing.Attestation2.AttestingIndices {
		attestation2Indices[i] = spec.ValidatorIndex(attesterSlashing.Attestation2.AttestingIndices[i])
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

func (s *Service) dbProposerSlashing(
	ctx context.Context,
	slot spec.Slot,
	blockRoot spec.Root,
	index uint64,
	proposerSlashing *spec.ProposerSlashing,
) (*chaindb.ProposerSlashing, error) {
	dbProposerSlashing := &chaindb.ProposerSlashing{
		InclusionSlot:        slot,
		InclusionBlockRoot:   blockRoot,
		InclusionIndex:       index,
		Header1Slot:          proposerSlashing.SignedHeader1.Message.Slot,
		Header1ProposerIndex: proposerSlashing.SignedHeader1.Message.ProposerIndex,
		Header1ParentRoot:    proposerSlashing.SignedHeader1.Message.ParentRoot,
		Header1StateRoot:     proposerSlashing.SignedHeader1.Message.StateRoot,
		Header1BodyRoot:      proposerSlashing.SignedHeader1.Message.BodyRoot,
		Header1Signature:     proposerSlashing.SignedHeader1.Signature,
		Header2Slot:          proposerSlashing.SignedHeader2.Message.Slot,
		Header2ProposerIndex: proposerSlashing.SignedHeader2.Message.ProposerIndex,
		Header2ParentRoot:    proposerSlashing.SignedHeader2.Message.ParentRoot,
		Header2StateRoot:     proposerSlashing.SignedHeader2.Message.StateRoot,
		Header2BodyRoot:      proposerSlashing.SignedHeader2.Message.BodyRoot,
		Header2Signature:     proposerSlashing.SignedHeader2.Signature,
	}

	return dbProposerSlashing, nil
}
