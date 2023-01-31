// Copyright © 2021 - 2023 Weald Technology Limited.
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

var metricsNamespace = "chaind_summarizer"

var highestEpoch phase0.Epoch
var latestEpoch prometheus.Gauge
var epochsProcessed prometheus.Gauge

var highestDay int64
var latestDay prometheus.Gauge
var daysProcessed prometheus.Gauge

var lastEpochPrune prometheus.Gauge
var lastBalancePrune prometheus.Gauge

func registerMetrics(ctx context.Context, monitor metrics.Service) error {
	if latestEpoch != nil {
		// Already registered.
		return nil
	}
	if monitor == nil {
		// No monitor.
		return nil
	}
	if monitor.Presenter() == "prometheus" {
		return registerPrometheusMetrics(ctx)
	}
	return nil
}

func registerPrometheusMetrics(ctx context.Context) error {
	latestEpoch = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "latest_epoch",
		Help:      "Latest epoch processed for summarizer",
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

	latestDay = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "latest_day",
		Help:      "Latest day processed for summarizer",
	})
	if err := prometheus.Register(latestDay); err != nil {
		return errors.Wrap(err, "failed to register latest_day")
	}

	daysProcessed = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "days_processed",
		Help:      "Number of days processed",
	})
	if err := prometheus.Register(daysProcessed); err != nil {
		return errors.Wrap(err, "failed to register days_processed")
	}

	lastBalancePrune = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "balance_prune_ts",
		Help:      "Timestamp of last balance prune",
	})
	if err := prometheus.Register(lastBalancePrune); err != nil {
		return errors.Wrap(err, "failed to register balance_prune_ts")
	}

	lastEpochPrune = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "epoch_prune_ts",
		Help:      "Timestamp of last epoch prune",
	})
	if err := prometheus.Register(lastEpochPrune); err != nil {
		return errors.Wrap(err, "failed to register epoch_prune_ts")
	}

	return nil
}

// monitorLatestEpoch sets the latest epoch without registering an
// increase in epochs processed.  This does not usually need to be
// called directly, as it is called as part ofr monitorEpochProcessed.
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

// monitorLatestDay sets the latest day without registering an
// increase in days processed.  This does not usually need to be
// called directly, as it is called as part ofr monitorDayProcessed.
func monitorLatestDay(day int64) {
	highestDay = day
	if latestDay != nil {
		latestDay.Set(float64(day))
	}
}

func monitorDayProcessed(day int64) {
	if daysProcessed != nil {
		daysProcessed.Inc()
		if day > highestDay {
			monitorLatestDay(day)
		}
	}
}

func monitorBalancePruned() {
	if lastBalancePrune != nil {
		lastBalancePrune.SetToCurrentTime()
	}
}

func monitorEpochPruned() {
	if lastEpochPrune != nil {
		lastEpochPrune.SetToCurrentTime()
	}
}
