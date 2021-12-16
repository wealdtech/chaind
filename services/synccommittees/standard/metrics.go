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

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/wealdtech/chaind/services/metrics"
)

var metricsNamespace = "chaind_synccommittees"

var highestPeriod uint64
var latestPeriod prometheus.Gauge
var periodsProcessed prometheus.Gauge

func registerMetrics(ctx context.Context, monitor metrics.Service) error {
	if latestPeriod != nil {
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

// skipcq: RVV-B0012
func registerPrometheusMetrics(ctx context.Context) error {
	latestPeriod = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "latest_period",
		Help:      "Latest sync committee period processed",
	})
	if err := prometheus.Register(latestPeriod); err != nil {
		return errors.Wrap(err, "failed to register latest_period")
	}

	periodsProcessed = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "periods_processed",
		Help:      "Number of periods processed",
	})
	if err := prometheus.Register(periodsProcessed); err != nil {
		return errors.Wrap(err, "failed to register periods_processed")
	}

	return nil
}

func monitorPeriodProcessed(period uint64) {
	if periodsProcessed != nil {
		periodsProcessed.Inc()
		if period > highestPeriod {
			latestPeriod.Set(float64(period))
			highestPeriod = period
		}
	}
}
