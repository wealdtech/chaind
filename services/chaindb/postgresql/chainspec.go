// Copyright © 2021 Weald Technology Limited.
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

package postgresql

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	spec "github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
)

// SetChainSpecValue sets the value of the provided key.
func (s *Service) SetChainSpecValue(ctx context.Context, key string, value interface{}) error {
	tx := s.tx(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	var dbVal string
	switch v := value.(type) {
	case spec.Slot, spec.Epoch, spec.CommitteeIndex, spec.ValidatorIndex, spec.Gwei:
		dbVal = fmt.Sprintf("%d", v)
	case spec.Root, spec.Version, spec.DomainType, spec.ForkDigest, spec.Domain, spec.BLSPubKey, spec.BLSSignature, []byte:
		dbVal = fmt.Sprintf("%#x", v)
	case time.Duration:
		dbVal = fmt.Sprintf("%d", int(v.Seconds()))
	case time.Time:
		dbVal = fmt.Sprintf("%d", v.Unix())
	default:
		dbVal = fmt.Sprintf("%v", v)
	}
	_, err := tx.Exec(ctx, `
      INSERT INTO t_chain_spec(f_key
                              ,f_value)
      VALUES($1,$2)
      ON CONFLICT (f_key) DO
      UPDATE
      SET f_value = excluded.f_value
      `,
		key,
		dbVal,
	)

	return err
}

// ChainSpec fetches all chain specification values.
func (s *Service) ChainSpec(ctx context.Context) (map[string]interface{}, error) {
	tx := s.tx(ctx)
	if tx == nil {
		ctx, cancel, err := s.BeginTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer cancel()
	}

	spec := make(map[string]interface{})
	rows, err := tx.Query(ctx, `
      SELECT f_key
            ,f_value
      FROM t_chain_spec
	  `)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var key string
		var dbVal string
		err := rows.Scan(
			&key,
			&dbVal,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan row")
		}

		spec[key] = dbValToSpec(ctx, key, dbVal)
	}

	return spec, nil
}

// ChainSpecValue fetches a chain specification value given its key.
func (s *Service) ChainSpecValue(ctx context.Context, key string) (interface{}, error) {
	tx := s.tx(ctx)
	if tx == nil {
		ctx, cancel, err := s.BeginTx(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to begin transaction")
		}
		tx = s.tx(ctx)
		defer cancel()
	}

	var dbVal string
	err := tx.QueryRow(ctx, `
      SELECT f_value
      FROM t_chain_spec
	  WHERE f_key = $1
	  `, key).Scan(&dbVal)
	if err != nil {
		return nil, err
	}

	return dbValToSpec(ctx, key, dbVal), nil
}

// dbValToSpec turns a database value in to a spec value.
func dbValToSpec(ctx context.Context, key string, val string) interface{} {
	// Handle domains.
	if strings.HasPrefix(key, "DOMAIN_") {
		byteVal, err := hex.DecodeString(strings.TrimPrefix(val, "0x"))
		if err == nil {
			var domainType spec.DomainType
			copy(domainType[:], byteVal)
			return domainType
		}
	}

	// Handle fork versions.
	if strings.HasSuffix(key, "_FORK_VERSION") {
		byteVal, err := hex.DecodeString(strings.TrimPrefix(val, "0x"))
		if err == nil {
			var version spec.Version
			copy(version[:], byteVal)
			return version
		}
	}

	// Handle hex strings.
	if strings.HasPrefix(val, "0x") {
		byteVal, err := hex.DecodeString(strings.TrimPrefix(val, "0x"))
		if err == nil {
			return byteVal
		}
	}

	// Handle times.
	if strings.HasSuffix(key, "_TIME") {
		intVal, err := strconv.ParseInt(val, 10, 64)
		if err == nil && intVal != 0 {
			return time.Unix(intVal, 0)
		}
	}

	// Handle durations.
	if strings.HasPrefix(key, "SECONDS_PER_") || strings.HasSuffix(key, "_DELAY") {
		intVal, err := strconv.ParseUint(val, 10, 64)
		if err == nil && intVal != 0 {
			return time.Duration(intVal) * time.Second
		}
	}

	// Handle integers.
	if val == "0" {
		return uint64(0)
	}
	intVal, err := strconv.ParseUint(val, 10, 64)
	if err == nil && intVal != 0 {
		return intVal
	}

	// Assume string.
	return val
}
