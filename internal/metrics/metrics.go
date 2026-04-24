// Package metrics defines all Prometheus metrics for the master-worker system.
//
// Prometheus follows the pull model: the master exposes a /metrics endpoint,
// and Prometheus scrapes it periodically. This is the opposite of push-based
// systems (StatsD, Datadog Agent) where the app sends metrics to a collector.
//
// The pull model has advantages for distributed systems:
//   - Prometheus controls scrape rate (no thundering herd)
//   - Failed scrapes are detected (up metric)
//   - Service discovery integrates naturally with K8s
//
// Metric types used:
//   - Counter: monotonically increasing value (tasks completed, errors)
//   - Gauge: value that goes up and down (queue depth, connected workers)
//   - Histogram: distribution of values (task duration, gRPC latency)
//
// Naming follows Prometheus conventions:
//   - Prefix: mw_ (master-worker)
//   - Suffix: _total (counters), _seconds (histograms with time)
//
// Reference: Google Borgmon → Prometheus lineage described in
// "Practical Monitoring" (Julien Pivotto, O'Reilly) and the original
// Prometheus paper (Soundcloud, 2012).
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Task metrics — track the full lifecycle of tasks through the system.

var (
	// TasksQueued counts the total number of tasks submitted to the system.
	// Use rate(mw_tasks_queued_total[5m]) to compute submission throughput.
	TasksQueued = promauto.NewCounter(prometheus.CounterOpts{
		Name: "mw_tasks_queued_total",
		Help: "Total number of tasks enqueued via HTTP API",
	})

	// TasksCompleted counts tasks that reached a terminal state, labeled by status.
	// Labels: status={"completed","failed"}
	// Use sum(rate(mw_tasks_completed_total[5m])) by (status) for throughput by outcome.
	TasksCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mw_tasks_completed_total",
		Help: "Total tasks that reached terminal state, by status",
	}, []string{"status"})

	// TasksAssigned counts the total number of tasks dispatched to workers.
	TasksAssigned = promauto.NewCounter(prometheus.CounterOpts{
		Name: "mw_tasks_assigned_total",
		Help: "Total number of tasks assigned to workers",
	})

	// TasksRetried counts how many times tasks have been retried.
	TasksRetried = promauto.NewCounter(prometheus.CounterOpts{
		Name: "mw_tasks_retried_total",
		Help: "Total number of task retry attempts",
	})

	// TaskDuration is a histogram of task execution time in seconds.
	// Default buckets: 0.1s, 0.25s, 0.5s, 1s, 2.5s, 5s, 10s, 30s, 60s, 120s
	// Use histogram_quantile(0.95, rate(mw_task_duration_seconds_bucket[5m])) for p95.
	TaskDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "mw_task_duration_seconds",
		Help:    "Histogram of task execution duration in seconds",
		Buckets: []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120},
	})

	// TaskQueueDepth is a gauge showing current queue depth.
	// High values indicate workers can't keep up (backpressure).
	TaskQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "mw_task_queue_depth",
		Help: "Current number of tasks waiting in the dispatch queue",
	})
)

// Worker metrics — track worker fleet health.

var (
	// WorkersConnected is a gauge of currently connected workers.
	// Sudden drops indicate worker failures or network partitions.
	WorkersConnected = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "mw_workers_connected",
		Help: "Current number of connected workers",
	})

	// HeartbeatsReceived counts heartbeats received from workers.
	HeartbeatsReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "mw_heartbeats_received_total",
		Help: "Total heartbeats received from workers",
	})

	// WorkerTasksActive is a gauge of currently executing tasks per worker.
	WorkerTasksActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mw_worker_tasks_active",
		Help: "Number of tasks currently executing on each worker",
	}, []string{"worker_id"})
)

// gRPC metrics — track RPC performance.

var (
	// GRPCRequestDuration is a histogram of gRPC request duration by method and status.
	// Labels: method (e.g., "ReportTaskStatus"), status (gRPC status code string).
	GRPCRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "mw_grpc_request_duration_seconds",
		Help:    "Histogram of gRPC request durations by method and status code",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "status"})

	// GRPCRequestsTotal counts all gRPC requests by method and status.
	GRPCRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mw_grpc_requests_total",
		Help: "Total gRPC requests by method and status code",
	}, []string{"method", "status"})
)

// HTTP metrics — track API performance.

var (
	// HTTPRequestDuration is a histogram of HTTP request duration by path and method.
	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "mw_http_request_duration_seconds",
		Help:    "Histogram of HTTP request durations",
		Buckets: prometheus.DefBuckets,
	}, []string{"path", "method", "status_code"})

	// HTTPRequestsTotal counts all HTTP requests.
	HTTPRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mw_http_requests_total",
		Help: "Total HTTP requests by path, method, and status code",
	}, []string{"path", "method", "status_code"})
)
