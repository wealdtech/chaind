// Copyright © 2022 Weald Technology Limited.
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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/wealdtech/chaind/services/metrics"
)

var metricsNamespace = "scheduler"

var schedulerJobsScheduled *prometheus.CounterVec
var schedulerJobsCancelled *prometheus.CounterVec
var schedulerJobsStarted *prometheus.CounterVec

func registerMetrics(ctx context.Context, monitor metrics.Service) error {
	if schedulerJobsScheduled != nil {
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

func registerPrometheusMetrics(_ context.Context) error {
	schedulerJobsScheduled = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "jobs",
		Name:      "scheduled_total",
		Help:      "The number of jobs scheduled.",
	}, []string{"class"})
	if err := prometheus.Register(schedulerJobsScheduled); err != nil {
		return err
	}

	schedulerJobsCancelled = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "jobs",
		Name:      "cancelled_total",
		Help:      "The number of scheduled jobs cancelled.",
	}, []string{"class"})
	if err := prometheus.Register(schedulerJobsCancelled); err != nil {
		return err
	}

	schedulerJobsStarted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: "jobs",
		Name:      "started_total",
		Help:      "The number of scheduled jobs started.",
	}, []string{"class", "trigger"})
	return prometheus.Register(schedulerJobsStarted)
}

// jobScheduled is called when a job is scheduled.
func jobScheduled(class string) {
	if schedulerJobsScheduled != nil {
		schedulerJobsScheduled.WithLabelValues(class).Inc()
	}
}

// jobCancelled is called when a scheduled job is cancelled.
func jobCancelled(class string) {
	if schedulerJobsCancelled != nil {
		schedulerJobsCancelled.WithLabelValues(class).Inc()
	}
}

// jobStartedOnTimer is called when a scheduled job is started due to meeting its time.
func jobStartedOnTimer(class string) {
	if schedulerJobsScheduled != nil {
		schedulerJobsStarted.WithLabelValues(class, "timer").Inc()
	}
}

// jobStartedOnSignal is called when a scheduled job is started due to being manually signalled.
func jobStartedOnSignal(class string) {
	if schedulerJobsStarted != nil {
		schedulerJobsStarted.WithLabelValues(class, "signal").Inc()
	}
}
