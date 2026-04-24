// Package circuitbreaker implements the Circuit Breaker pattern for per-worker
// fault isolation.
//
// The Circuit Breaker pattern prevents cascading failures by stopping requests
// to a failing component. It has three states:
//
//	CLOSED (normal):    requests flow through, failures are counted
//	OPEN (tripped):     requests are rejected immediately, no load on failing component
//	HALF_OPEN (probing): one test request allowed through to check if component recovered
//
// State transitions:
//
//	CLOSED  → OPEN      (failure count exceeds threshold)
//	OPEN    → HALF_OPEN (after timeout period elapses)
//	HALF_OPEN → CLOSED  (test request succeeds)
//	HALF_OPEN → OPEN    (test request fails)
//
// Origin: Michael Nygard's "Release It!" (2007). Used in:
//   - Netflix Hystrix (now deprecated, concepts live in Resilience4j)
//   - Go: sony/gobreaker, afex/hystrix-go
//   - Service meshes: Envoy, Istio, Linkerd all implement circuit breaking
//
// In our system, each worker gets its own circuit breaker. If a worker keeps
// failing tasks, its circuit opens and we stop assigning tasks to it.
package circuitbreaker

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// State represents the circuit breaker's current state.
type State int

const (
	StateClosed   State = iota // Normal operation
	StateOpen                  // Rejecting requests
	StateHalfOpen              // Testing with one request
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "CLOSED"
	case StateOpen:
		return "OPEN"
	case StateHalfOpen:
		return "HALF_OPEN"
	default:
		return "UNKNOWN"
	}
}

// CircuitBreaker tracks failure state for a single worker.
type CircuitBreaker struct {
	mu               sync.Mutex
	state            State
	failureCount     int
	successCount     int
	failureThreshold int
	successThreshold int // Successes needed in HALF_OPEN to close
	timeout          time.Duration
	lastFailure      time.Time
	workerID         string
	logger           *slog.Logger
}

// New creates a circuit breaker for a worker.
func New(workerID string, failureThreshold, successThreshold int, timeout time.Duration, logger *slog.Logger) *CircuitBreaker {
	return &CircuitBreaker{
		state:            StateClosed,
		failureThreshold: failureThreshold,
		successThreshold: successThreshold,
		timeout:          timeout,
		workerID:         workerID,
		logger:           logger,
	}
}

// Allow checks if a request should be allowed through.
// Returns true if the circuit is closed or half-open (testing).
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return true
	case StateOpen:
		// Check if timeout has elapsed → transition to half-open.
		if time.Since(cb.lastFailure) > cb.timeout {
			cb.state = StateHalfOpen
			cb.successCount = 0
			cb.logger.Info("circuit breaker half-open (probing)",
				"worker_id", cb.workerID)
			return true
		}
		return false
	case StateHalfOpen:
		return true
	default:
		return false
	}
}

// RecordSuccess records a successful operation.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateHalfOpen:
		cb.successCount++
		if cb.successCount >= cb.successThreshold {
			cb.state = StateClosed
			cb.failureCount = 0
			cb.successCount = 0
			cb.logger.Info("circuit breaker closed (recovered)",
				"worker_id", cb.workerID)
		}
	case StateClosed:
		cb.failureCount = 0 // Reset consecutive failure count on success
	}
}

// RecordFailure records a failed operation.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failureCount++
	cb.lastFailure = time.Now()

	switch cb.state {
	case StateClosed:
		if cb.failureCount >= cb.failureThreshold {
			cb.state = StateOpen
			cb.logger.Warn("circuit breaker OPEN (tripped)",
				"worker_id", cb.workerID,
				"failures", cb.failureCount)
		}
	case StateHalfOpen:
		cb.state = StateOpen
		cb.logger.Warn("circuit breaker re-opened (probe failed)",
			"worker_id", cb.workerID)
	}
}

// Info holds exported circuit breaker details for API consumers.
type Info struct {
	State            string `json:"state"`
	FailureCount     int    `json:"failure_count"`
	FailureThreshold int    `json:"failure_threshold"`
	SuccessCount     int    `json:"success_count"`
	SuccessThreshold int    `json:"success_threshold"`
	LastFailure      string `json:"last_failure,omitempty"`
}

// GetState returns the current circuit breaker state.
func (cb *CircuitBreaker) GetState() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// GetInfo returns a snapshot of the circuit breaker's state for API consumers.
func (cb *CircuitBreaker) GetInfo() Info {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	info := Info{
		State:            cb.state.String(),
		FailureCount:     cb.failureCount,
		FailureThreshold: cb.failureThreshold,
		SuccessCount:     cb.successCount,
		SuccessThreshold: cb.successThreshold,
	}
	if !cb.lastFailure.IsZero() {
		info.LastFailure = cb.lastFailure.Format(time.RFC3339)
	}
	return info
}

// Trip forces the circuit breaker into OPEN state (chaos engineering).
func (cb *CircuitBreaker) Trip() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = StateOpen
	cb.lastFailure = time.Now()
	cb.logger.Warn("circuit breaker manually TRIPPED", "worker_id", cb.workerID)
}

// Reset forces the circuit breaker back to closed state.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = StateClosed
	cb.failureCount = 0
	cb.successCount = 0
	cb.logger.Info("circuit breaker manually reset", "worker_id", cb.workerID)
}

func (cb *CircuitBreaker) String() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return fmt.Sprintf("CB[%s state=%s failures=%d]", cb.workerID, cb.state, cb.failureCount)
}

// Registry manages circuit breakers for all workers.
type Registry struct {
	mu               sync.RWMutex
	breakers         map[string]*CircuitBreaker
	failureThreshold int
	successThreshold int
	timeout          time.Duration
	logger           *slog.Logger
}

// NewRegistry creates a circuit breaker registry with shared configuration.
func NewRegistry(failureThreshold, successThreshold int, timeout time.Duration, logger *slog.Logger) *Registry {
	return &Registry{
		breakers:         make(map[string]*CircuitBreaker),
		failureThreshold: failureThreshold,
		successThreshold: successThreshold,
		timeout:          timeout,
		logger:           logger,
	}
}

// Get returns the circuit breaker for a worker, creating one if it doesn't exist.
func (r *Registry) Get(workerID string) *CircuitBreaker {
	r.mu.RLock()
	if cb, ok := r.breakers[workerID]; ok {
		r.mu.RUnlock()
		return cb
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check after acquiring write lock.
	if cb, ok := r.breakers[workerID]; ok {
		return cb
	}

	cb := New(workerID, r.failureThreshold, r.successThreshold, r.timeout, r.logger)
	r.breakers[workerID] = cb
	return cb
}

// Remove removes a worker's circuit breaker.
func (r *Registry) Remove(workerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.breakers, workerID)
}

// GetAll returns Info for all registered circuit breakers.
func (r *Registry) GetAll() map[string]Info {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make(map[string]Info, len(r.breakers))
	for id, cb := range r.breakers {
		result[id] = cb.GetInfo()
	}
	return result
}
