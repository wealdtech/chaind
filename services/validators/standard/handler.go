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
	if !epochTransition {
		// Only interested in epoch transitions.
		return
	}

	epoch := s.chainTime.SlotToEpoch(slot)
	log := log.With().Uint64("epoch", uint64(epoch)).Logger()

	ctx, cancel, err := s.chainDB.BeginTx(ctx)
	if err != nil {
		log.Error().Err(err).Msg("Failed to begin transaction on beacon chain head update")
		return
	}

	md, err := s.getMetadata(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to obtain metadata")
	}

	if err := s.updateValidatorsForState(ctx, fmt.Sprintf("%d", slot)); err != nil {
		log.Error().Err(err).Msg("Failed to update validators on beacon chain head update")
		md.MissedEpochs = append(md.MissedEpochs, epoch)
	}

	md.LatestEpoch = epoch
	if err := s.setMetadata(ctx, md); err != nil {
		log.Error().Err(err).Msg("Failed to set metadata")
	}

	if err := s.chainDB.CommitTx(ctx); err != nil {
		log.Error().Err(err).Msg("Failed to commit transaction")
		cancel()
		return
	}
}

func (s *Service) updateValidatorsForState(ctx context.Context, stateID string) error {
	validators, err := s.eth2Client.(eth2client.ValidatorsProvider).Validators(ctx, stateID, nil)
	if err != nil {
		return errors.Wrap(err, "failed to obtain validators")
	}

	epoch, err := s.eth2Client.(eth2client.EpochFromStateIDProvider).EpochFromStateID(ctx, stateID)
	if err != nil {
		return errors.Wrap(err, "failed to calculate epoch from state ID")
	}

	for index, validator := range validators {
		dbValidator := &chaindb.Validator{
			PublicKey:                  validator.Validator.PublicKey,
			Index:                      index,
			EffectiveBalance:           validator.Validator.EffectiveBalance,
			Slashed:                    validator.Validator.Slashed,
			ActivationEligibilityEpoch: validator.Validator.ActivationEligibilityEpoch,
			ActivationEpoch:            validator.Validator.ActivationEpoch,
			ExitEpoch:                  validator.Validator.ExitEpoch,
			WithdrawableEpoch:          validator.Validator.WithdrawableEpoch,
		}
		if err := s.validatorsSetter.SetValidator(ctx, dbValidator); err != nil {
			return errors.Wrap(err, "failed to set validator")
		}
		if s.balances {
			dbValidatorBalance := &chaindb.ValidatorBalance{
				Index:            index,
				Epoch:            epoch,
				Balance:          validator.Balance,
				EffectiveBalance: validator.Validator.EffectiveBalance,
			}
			if err := s.validatorsSetter.SetValidatorBalance(ctx, dbValidatorBalance); err != nil {
				return errors.Wrap(err, "failed to set validator balance")
			}
		}
	}
	monitorEpochProcessed(epoch)
	if s.balances {
		monitorBalancesEpochProcessed(epoch)
	}

	return nil
}

func (s *Service) updateValidatorBalancesForState(ctx context.Context, stateID string) error {
	if s.balances {
		epoch, err := s.eth2Client.(eth2client.EpochFromStateIDProvider).EpochFromStateID(ctx, stateID)
		if err != nil {
			return errors.Wrap(err, "failed to calculate epoch from state ID")
		}
		validators, err := s.eth2Client.(eth2client.ValidatorsProvider).Validators(ctx, stateID, nil)
		if err != nil {
			return errors.Wrap(err, "failed to obtain validator balances")
		}
		for index, validator := range validators {
			dbValidatorBalance := &chaindb.ValidatorBalance{
				Index:            index,
				Epoch:            epoch,
				Balance:          validator.Balance,
				EffectiveBalance: validator.Validator.EffectiveBalance,
			}
			if err := s.validatorsSetter.SetValidatorBalance(ctx, dbValidatorBalance); err != nil {
				return errors.Wrap(err, "failed to set validator balance")
			}
		}
	}
	return nil
}
