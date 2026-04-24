// Package health provides Kubernetes-style health check endpoints.
//
// Why two endpoints:
//   - /healthz (liveness): "Is the process alive?" If this fails, K8s restarts the pod.
//     Similar to a heartbeat — checks that the process hasn't deadlocked or crashed.
//   - /readyz (readiness): "Can this pod accept traffic?" If this fails, K8s removes
//     the pod from Service endpoints but does NOT restart it.
//     Used during startup (before gRPC is ready) and during graceful shutdown.
//
// This pattern is universal in production systems:
//   - Google Borg: every task exposes /healthz (Verma et al., EuroSys 2015)
//   - Kubernetes: livenessProbe + readinessProbe in pod spec
//   - AWS ELB: health check endpoints determine routing targets
//
// Reference: Kubernetes docs "Configure Liveness, Readiness and Startup Probes"
package health

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sync/atomic"
)

// Checker provides liveness and readiness HTTP handlers.
// Thread-safe via atomic.Bool — safe to call SetReady from any goroutine.
type Checker struct {
	ready  atomic.Bool
	logger *slog.Logger
}

// NewChecker creates a health checker. Starts as not-ready until SetReady(true).
func NewChecker(logger *slog.Logger) *Checker {
	return &Checker{logger: logger}
}

// SetReady marks the service as ready (or not ready) to accept traffic.
// Called after gRPC server is listening (ready=true) and during shutdown (ready=false).
func (c *Checker) SetReady(ready bool) {
	c.ready.Store(ready)
	c.logger.Info("readiness state changed", "ready", ready)
}

// LivenessHandler returns 200 if the process is alive.
// In K8s, a failing liveness probe triggers a pod restart.
func (c *Checker) LivenessHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "alive"})
}

// ReadinessHandler returns 200 if the service is ready, 503 otherwise.
// In K8s, a failing readiness probe removes the pod from Service endpoints.
func (c *Checker) ReadinessHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if c.ready.Load() {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"status": "not ready"})
	}
}
