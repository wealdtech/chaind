package mock

import (
	"encoding/binary"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/wealdtech/chaind/services/chaindb"
)

func MockRoot(value uint16) phase0.Root {
	var root phase0.Root
	binary.LittleEndian.PutUint16(root[0:2], value)

	return root
}

type MockAttestation struct {
	Slot       phase0.Slot
	Root       uint16
	NumIndices uint
}

func MockBlock(
	slot phase0.Slot,
	root uint16,
	parentRoot uint16,
	mockAtts []MockAttestation,
) (*chaindb.Block, []*chaindb.Attestation) {
	block := &chaindb.Block{
		Slot:       slot,
		Root:       MockRoot(root),
		ParentRoot: MockRoot(parentRoot),

		// These fields are not use by finalizer
		ProposerIndex:    0,
		Graffiti:         nil,
		RANDAOReveal:     phase0.BLSSignature{},
		BodyRoot:         phase0.Root{},
		StateRoot:        phase0.Root{},
		Canonical:        nil,
		ETH1BlockHash:    nil,
		ETH1DepositCount: 0,
		ETH1DepositRoot:  phase0.Root{},
		ExecutionPayload: nil,
	}

	attestations := []*chaindb.Attestation{}
	for _, mockAtt := range mockAtts {
		attestation := &chaindb.Attestation{
			Slot:               mockAtt.Slot,
			BeaconBlockRoot:    MockRoot(mockAtt.Root),
			AggregationIndices: make([]phase0.ValidatorIndex, mockAtt.NumIndices),

			// These fields are not use by finalizer
			InclusionSlot:      0,
			InclusionBlockRoot: phase0.Root{},
			InclusionIndex:     0,
			CommitteeIndex:     0,
			AggregationBits:    nil,
			SourceEpoch:        0,
			SourceRoot:         phase0.Root{},
			TargetEpoch:        0,
			TargetRoot:         phase0.Root{},
			Canonical:          nil,
			TargetCorrect:      nil,
			HeadCorrect:        nil,
		}

		attestations = append(attestations, attestation)
	}

	return block, attestations
}
