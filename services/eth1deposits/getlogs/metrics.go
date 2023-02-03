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

package getlogs

import (
	"context"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/wealdtech/chaind/services/metrics"
)

var metricsNamespace = "chaind_eth1deposits"

var (
	highestBlock    uint64
	latestBlock     prometheus.Gauge
	blocksProcessed prometheus.Gauge
)

func registerMetrics(_ context.Context, monitor metrics.Service) error {
	if latestBlock != nil {
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

func registerPrometheusMetrics() error {
	latestBlock = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "latest_block",
		Help:      "Latest Ethereum 1 block processed",
	})
	if err := prometheus.Register(latestBlock); err != nil {
		return errors.Wrap(err, "failed to register latest_block")
	}

	blocksProcessed = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Name:      "blocks_processed",
		Help:      "Number of Ethereum 1 blocks processed",
	})
	if err := prometheus.Register(blocksProcessed); err != nil {
		return errors.Wrap(err, "failed to register blocks_processed")
	}

	return nil
}

func monitorBlockProcessed(block uint64) {
	if blocksProcessed != nil {
		blocksProcessed.Inc()
		if block > highestBlock {
			latestBlock.Set(float64(block))
			highestBlock = block
		}
	}
}
