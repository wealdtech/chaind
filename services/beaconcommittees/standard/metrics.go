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

package standard

import (
	"context"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/wealdtech/chaind/services/metrics"
)

var metricsNamespace = "chaind_beaconcommittees"

var (
	highestEpoch    phase0.Epoch
	latestEpoch     prometheus.Gauge
	epochsProcessed prometheus.Gauge
)

func registerMetrics(_ context.Context, monitor metrics.Service) error {
	if latestEpoch != nil {
		// Already registered.
		return nil
	}
	if monitor == nil {
		// No monitor.
		return nil
	}
	if monitor.Presenter() == "prometheus" {
		return registerPrometheusMetrics()
	}
	return nil
}

// skipcq: RVV-B0012
func registerPrometheusMetrics() error {
	latestEpoch = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "latest_epoch",
		Help:      "Latest epoch processed",
	})
	if err := prometheus.Register(latestEpoch); err != nil {
		return errors.Wrap(err, "failed to register latest_epoch")
	}

	epochsProcessed = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "epochs_processed",
		Help:      "Number of epochs processed",
	})
	if err := prometheus.Register(epochsProcessed); err != nil {
		return errors.Wrap(err, "failed to register epochs_processed")
	}

	return nil
}

// monitorLatestEpoch sets the latest epoch without registering an
// increase in blocks processed.  This does not usually need to be
// called directly, as it is called as part of monitorEpochProcessed.
func monitorLatestEpoch(epoch phase0.Epoch) {
	highestEpoch = epoch
	if latestEpoch != nil {
		latestEpoch.Set(float64(epoch))
	}
}

func monitorEpochProcessed(epoch phase0.Epoch) {
	if epochsProcessed != nil {
		epochsProcessed.Inc()
		if epoch > highestEpoch {
			monitorLatestEpoch(epoch)
		}
	}
}
