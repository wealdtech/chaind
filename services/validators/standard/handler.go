// Copyright © 2020, 2021 Weald Technology Limited.
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
	epochTransition bool,
) {
	epoch := s.chainTime.SlotToEpoch(slot)
	log := log.With().Uint64("epoch", uint64(epoch)).Logger()

	if !epochTransition {
		// Only interested in epoch transitions.
		return
	}

	// Only allow 1 handler to be active.
	acquired := s.activitySem.TryAcquire(1)
	if !acquired {
		log.Debug().Msg("Another handler running")
		return
	}

	log.Trace().Msg("Handling epoch transition")

	md, err := s.getMetadata(ctx)
	if err != nil {
		s.activitySem.Release(1)
		log.Fatal().Err(err).Msg("Failed to obtain metadata")
	}

	if err := s.onEpochTransitionValidators(ctx, md, epoch); err != nil {
		log.Warn().Err(err).Msg("Failed to update validators")
	}
	if err := s.onEpochTransitionValidatorBalances(ctx, md, epoch); err != nil {
		log.Warn().Err(err).Msg("Failed to update validators")
	}
	s.activitySem.Release(1)

	monitorEpochProcessed(epoch)
	log.Trace().Msg("Finished handling epoch transition")
}

func (s *Service) onEpochTransitionValidators(ctx context.Context,
	md *metadata,
	transitionedEpoch phase0.Epoch,
) error {
	// We always fetch the latest validator information regardless of epoch.
	validators, err := s.eth2Client.(eth2client.ValidatorsProvider).Validators(ctx, "head", nil)
	if err != nil {
		return errors.Wrap(err, "failed to obtain validators")
	}

	ctx, cancel, err := s.chainDB.BeginTx(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction for validators")
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
			cancel()
			return errors.Wrap(err, "failed to set validator")
		}
	}
	md.LatestEpoch = transitionedEpoch
	if err := s.setMetadata(ctx, md); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set metadata for validators")
	}
	if err := s.chainDB.CommitTx(ctx); err != nil {
		cancel()
		return errors.Wrap(err, "failed to set commit transaction for validators")
	}
	monitorEpochProcessed(transitionedEpoch)

	return nil
}

func (s *Service) onEpochTransitionValidatorBalances(ctx context.Context,
	md *metadata,
	transitionedEpoch phase0.Epoch,
) error {
	if !s.balances {
		return nil
	}

	for epoch := md.LatestBalancesEpoch; epoch <= transitionedEpoch; epoch++ {
		log := log.With().Uint64("epoch", uint64(epoch)).Logger()
		stateID := fmt.Sprintf("%d", s.chainTime.FirstSlotOfEpoch(epoch))
		log.Trace().Uint64("slot", uint64(s.chainTime.FirstSlotOfEpoch(epoch))).Msg("Fetching validators")
		validators, err := s.eth2Client.(eth2client.ValidatorsProvider).Validators(ctx, stateID, nil)
		if err != nil {
			return errors.Wrap(err, "failed to obtain validators for validator balances")
		}

		dbCtx, cancel, err := s.chainDB.BeginTx(ctx)
		if err != nil {
			return errors.Wrap(err, "failed to begin transaction for validator balances")
		}
		if s.balances {
			dbValidatorBalances := make([]*chaindb.ValidatorBalance, 0, len(validators))
			for index, validator := range validators {
				dbValidatorBalances = append(dbValidatorBalances, &chaindb.ValidatorBalance{
					Index:            index,
					Epoch:            epoch,
					Balance:          validator.Balance,
					EffectiveBalance: validator.Validator.EffectiveBalance,
				})
			}
			if err := s.validatorsSetter.SetValidatorBalances(dbCtx, dbValidatorBalances); err != nil {
				log.Trace().Err(err).Msg("Bulk insert failed; falling back to individual insert")
				// This error will have caused the transaction to fail, so cancel it and start a new one.
				cancel()
				dbCtx, cancel, err = s.chainDB.BeginTx(ctx)
				if err != nil {
					return errors.Wrap(err, "failed to begin transaction for validator balances (2)")
				}
				for _, dbValidatorBalance := range dbValidatorBalances {
					if err := s.validatorsSetter.SetValidatorBalance(dbCtx, dbValidatorBalance); err != nil {
						return errors.Wrap(err, "failed to set validator balance")
					}
				}
			}
			md.LatestBalancesEpoch = epoch
		}

		if err := s.setMetadata(dbCtx, md); err != nil {
			cancel()
			return errors.Wrap(err, "failed to set metadata for validator balances")
		}

		if err := s.chainDB.CommitTx(dbCtx); err != nil {
			cancel()
			return errors.Wrap(err, "failed to set commit transaction for validator balances")
		}
		monitorBalancesEpochProcessed(epoch)
	}

	return nil
}
