// Copyright © 2021, 2022 Weald Technology Limited.
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

var metricsNamespace = "chaind_blocks"

var (
	highestSlot    phase0.Slot
	latestSlot     prometheus.Gauge
	slotsProcessed prometheus.Gauge
)

func registerMetrics(_ context.Context, monitor metrics.Service) error {
	if latestSlot != nil {
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
	latestSlot = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "latest_slot",
		Help:      "Latest slot processed",
	})
	if err := prometheus.Register(latestSlot); err != nil {
		return errors.Wrap(err, "failed to register latest_slot")
	}

	slotsProcessed = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "slots_processed",
		Help:      "Number of slots processed",
	})
	if err := prometheus.Register(slotsProcessed); err != nil {
		return errors.Wrap(err, "failed to register slots_processed")
	}

	return nil
}

// monitorLatestSlot sets the latest slot without registering an
// increase in slots processed.  This does not usually need to be
// called directly, as it is called as part of monitorSlotProcessed.
func monitorLatestSlot(slot phase0.Slot) {
	highestSlot = slot
	if latestSlot != nil {
		latestSlot.Set(float64(slot))
	}
}

func monitorSlotProcessed(slot phase0.Slot) {
	if slotsProcessed != nil {
		slotsProcessed.Inc()
		if slot > highestSlot {
			monitorLatestSlot(slot)
		}
	}
}
