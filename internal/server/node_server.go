// Package server implements the gRPC NodeService — the core distributed logic.
//
// This is where the distributed systems patterns live:
//   - Priority queue scheduler with retry, backoff, and dead letter queue
//   - Worker registry with heartbeat-based failure detection (HDFS model)
//   - Circuit breakers for per-worker fault isolation
//   - Fault recovery: re-queues tasks from disconnected workers (MapReduce pattern)
//
// Key distributed systems concept: this is a centralized coordinator (like
// HDFS NameNode, YARN ResourceManager, or Borg's Borgmaster). The master is
// a single point of failure — HA would require consensus (Raft/Paxos), which
// is out of scope for this project but noted in the design.
package server

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	api "github.com/jalala984/master-worker/api"
	"github.com/jalala984/master-worker/internal/circuitbreaker"
	"github.com/jalala984/master-worker/internal/config"
	"github.com/jalala984/master-worker/internal/events"
	"github.com/jalala984/master-worker/internal/metrics"
	"github.com/jalala984/master-worker/internal/store"
	"github.com/jalala984/master-worker/internal/task"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// WorkerInfo tracks a connected worker's registration and health state.
// Pattern from HDFS NameNode: maintains a registry of DataNodes with their
// last-known state and heartbeat timestamp for failure detection.
type WorkerInfo struct {
	ID            string
	Hostname      string
	State         api.WorkerState
	StartTime     time.Time
	LastHeartbeat time.Time
	ActiveTasks   int32
}

// NodeServer implements the gRPC NodeService defined in the proto file.
// It is the brain of the master — responsible for task dispatch, worker
// tracking, and failure detection.
type NodeServer struct {
	api.UnimplementedNodeServiceServer

	// Scheduler replaces the raw channel with a priority queue that supports
	// retry with exponential backoff, timeout enforcement, and DLQ.
	Scheduler *task.Scheduler

	// Store persists tasks for crash recovery.
	Store store.TaskStore

	// CircuitBreakers tracks per-worker failure state.
	CircuitBreakers *circuitbreaker.Registry

	// EventBus broadcasts state changes to WebSocket subscribers.
	EventBus *events.Bus

	// mu protects Workers map.
	mu      sync.RWMutex
	Workers map[string]*WorkerInfo

	// workerStreams stores cancel functions for each worker's gRPC stream.
	// Used by chaos engineering to forcefully disconnect workers.
	streamMu      sync.Mutex
	workerStreams  map[string]context.CancelFunc

	logger *slog.Logger
	cfg    *config.Config
}

// NewNodeServer creates a NodeServer with initialized data structures.
func NewNodeServer(logger *slog.Logger, cfg *config.Config, taskStore store.TaskStore, eventBus *events.Bus) *NodeServer {
	scheduler := task.NewScheduler(logger.With("subsystem", "scheduler"), 5*time.Minute)
	cbRegistry := circuitbreaker.NewRegistry(5, 2, 30*time.Second, logger.With("subsystem", "circuit-breaker"))

	return &NodeServer{
		Scheduler:       scheduler,
		Store:           taskStore,
		CircuitBreakers: cbRegistry,
		EventBus:        eventBus,
		Workers:         make(map[string]*WorkerInfo),
		workerStreams:    make(map[string]context.CancelFunc),
		logger:          logger,
		cfg:             cfg,
	}
}

// publishEvent broadcasts an event to all WebSocket subscribers.
// Nil-safe: does nothing if no event bus is configured (e.g., in tests).
func (s *NodeServer) publishEvent(eventType events.EventType, data interface{}) {
	if s.EventBus != nil {
		s.EventBus.Publish(events.Event{Type: eventType, Data: data})
	}
}

// EnqueueTask creates a task domain object and adds it to the scheduler.
// Called by the HTTP handler when tasks are submitted.
func (s *NodeServer) EnqueueTask(id, script string) {
	t := task.New(id, script)
	s.enqueue(t)
}

// EnqueueInlineTask creates an inline script task and adds it to the scheduler.
func (s *NodeServer) EnqueueInlineTask(id, content, language string) {
	t := task.NewInline(id, content, language)
	s.enqueue(t)
}

// EnqueueArchiveTask creates an archive upload task and adds it to the scheduler.
func (s *NodeServer) EnqueueArchiveTask(id, content, entrypoint, language string) {
	t := task.NewArchive(id, content, entrypoint, language)
	s.enqueue(t)
}

// enqueue is the shared logic for adding any task to the scheduler.
func (s *NodeServer) enqueue(t *task.Task) {
	if s.Store != nil {
		s.Store.Save(t)
	}
	s.Scheduler.Enqueue(t)
	metrics.TasksQueued.Inc()
	metrics.TaskQueueDepth.Set(float64(s.Scheduler.Stats().QueueDepth))
}

// SendHeartbeat handles periodic liveness probes from workers.
func (s *NodeServer) SendHeartbeat(ctx context.Context, hb *api.Heartbeat) (*api.HeartbeatResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	metrics.HeartbeatsReceived.Inc()

	w, exists := s.Workers[hb.WorkerId]
	if !exists {
		s.logger.Warn("heartbeat from unregistered worker", "worker_id", hb.WorkerId)
		return &api.HeartbeatResponse{Acknowledged: true}, nil
	}

	w.LastHeartbeat = time.Now()
	w.ActiveTasks = hb.ActiveTasks
	metrics.WorkerTasksActive.WithLabelValues(hb.WorkerId).Set(float64(hb.ActiveTasks))

	if w.State == api.WorkerState_WORKER_SUSPECT {
		s.logger.Info("worker recovered from suspect state", "worker_id", hb.WorkerId)
		w.State = api.WorkerState_WORKER_ACTIVE
	}

	return &api.HeartbeatResponse{Acknowledged: true}, nil
}

// ReportTaskStatus handles task completion/failure reports from workers.
func (s *NodeServer) ReportTaskStatus(ctx context.Context, report *api.TaskReport) (*api.TaskReportResponse, error) {
	cb := s.CircuitBreakers.Get(report.WorkerId)

	if report.State == api.TaskState_TASK_COMPLETED {
		t := s.Scheduler.Complete(report.TaskId, report.Output, report.ExitCode)
		if t != nil && s.Store != nil {
			s.Store.Save(t)
		}
		cb.RecordSuccess()
		metrics.TasksCompleted.WithLabelValues("completed").Inc()
		s.logger.Info("task completed",
			"task_id", report.TaskId,
			"worker_id", report.WorkerId,
			"exit_code", report.ExitCode,
		)
		s.publishEvent(events.EventTaskCompleted, map[string]interface{}{
			"task_id":   report.TaskId,
			"worker_id": report.WorkerId,
			"exit_code": report.ExitCode,
		})
	} else {
		t := s.Scheduler.Fail(report.TaskId, report.ErrorMessage, report.ExitCode)
		if t != nil && s.Store != nil {
			s.Store.Save(t)
		}
		cb.RecordFailure()
		metrics.TasksCompleted.WithLabelValues("failed").Inc()
		if t != nil && t.State == task.StateRetrying {
			metrics.TasksRetried.Inc()
		}
		s.logger.Warn("task failed",
			"task_id", report.TaskId,
			"worker_id", report.WorkerId,
			"exit_code", report.ExitCode,
			"error", report.ErrorMessage,
		)

		eventType := events.EventTaskFailed
		if t != nil && t.State == task.StateRetrying {
			eventType = events.EventTaskRetrying
		} else if t != nil && t.State == task.StateDead {
			eventType = events.EventTaskDead
		}
		s.publishEvent(eventType, map[string]interface{}{
			"task_id":   report.TaskId,
			"worker_id": report.WorkerId,
			"error":     report.ErrorMessage,
			"exit_code": report.ExitCode,
		})
	}

	// Record task execution duration from worker-reported timestamps.
	if report.StartedAt != nil && report.CompletedAt != nil {
		duration := report.CompletedAt.AsTime().Sub(report.StartedAt.AsTime())
		metrics.TaskDuration.Observe(duration.Seconds())
	}

	if report.Output != "" {
		s.logger.Debug("task output", "task_id", report.TaskId, "output", report.Output)
	}

	return &api.TaskReportResponse{Acknowledged: true}, nil
}

// AssignTask handles server-streaming RPC: master pushes tasks to a worker.
func (s *NodeServer) AssignTask(reg *api.WorkerRegistration, stream api.NodeService_AssignTaskServer) error {
	// Create a cancellable context derived from the stream context.
	// This enables the chaos engineering "kill worker" feature.
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	// Store the cancel function so HTTP chaos handlers can trigger disconnect.
	s.streamMu.Lock()
	s.workerStreams[reg.WorkerId] = cancel
	s.streamMu.Unlock()

	s.mu.Lock()
	s.Workers[reg.WorkerId] = &WorkerInfo{
		ID:            reg.WorkerId,
		Hostname:      reg.Hostname,
		State:         api.WorkerState_WORKER_ACTIVE,
		StartTime:     reg.StartTime.AsTime(),
		LastHeartbeat: time.Now(),
	}
	s.mu.Unlock()

	metrics.WorkersConnected.Inc()
	s.logger.Info("worker connected", "worker_id", reg.WorkerId, "hostname", reg.Hostname)
	s.publishEvent(events.EventWorkerConnected, map[string]string{
		"worker_id": reg.WorkerId,
		"hostname":  reg.Hostname,
	})

	defer func() {
		// Clean up stream cancel function.
		s.streamMu.Lock()
		delete(s.workerStreams, reg.WorkerId)
		s.streamMu.Unlock()

		metrics.WorkersConnected.Dec()

		// Re-queue all in-flight tasks for this worker.
		count := s.Scheduler.RequeueFromWorker(reg.WorkerId)
		if count > 0 {
			s.logger.Warn("requeued tasks from disconnected worker",
				"worker_id", reg.WorkerId, "count", count)
		}

		s.mu.Lock()
		if w, exists := s.Workers[reg.WorkerId]; exists {
			w.State = api.WorkerState_WORKER_DEAD
		}
		s.mu.Unlock()
		s.logger.Info("worker disconnected", "worker_id", reg.WorkerId)
		s.publishEvent(events.EventWorkerDisconnected, map[string]string{
			"worker_id": reg.WorkerId,
		})
	}()

	// Main dispatch loop: wait for tasks from the scheduler.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-s.Scheduler.NotifyChan():
			// Check circuit breaker before assigning.
			cb := s.CircuitBreakers.Get(reg.WorkerId)
			if !cb.Allow() {
				s.logger.Debug("circuit breaker open, skipping worker",
					"worker_id", reg.WorkerId)
				// Re-signal so another worker (whose CB is still CLOSED)
				// can pick up the task we just declined.
				s.Scheduler.Notify()
				continue
			}

			t := s.Scheduler.Dequeue()
			if t == nil {
				continue
			}

			t.Assign(reg.WorkerId)
			// Transition ASSIGNED → RUNNING immediately: the moment we send
			// the task the worker begins executing it. This keeps the state
			// machine valid (Complete/Fail both expect StateRunning as the
			// predecessor), which ensures output and timing are stored.
			t.Start()
			if s.Store != nil {
				s.Store.Save(t)
			}

			metrics.TasksAssigned.Inc()
			stats := s.Scheduler.Stats()
			metrics.TaskQueueDepth.Set(float64(stats.QueueDepth))

			s.logger.Info("assigning task",
				"task_id", t.ID,
				"worker_id", reg.WorkerId,
				"attempt", t.Attempt,
			)

			assignment := &api.TaskAssignment{
				TaskId:         t.ID,
				Script:         t.Script,
				AssignedAt:     timestamppb.Now(),
				Attempt:        t.Attempt,
				TimeoutSeconds: t.TimeoutSec,
				TaskType:       api.TaskType(t.Type),
				Content:        t.Content,
				Language:       t.Language,
				Entrypoint:     t.Entrypoint,
			}
			if err := stream.Send(assignment); err != nil {
				// Failed to send — re-queue the task.
				t.State = task.StateQueued
				t.WorkerID = ""
				s.Scheduler.Enqueue(t)
				return err
			}

			s.publishEvent(events.EventTaskAssigned, map[string]string{
				"task_id":   t.ID,
				"worker_id": reg.WorkerId,
				"attempt":   fmt.Sprintf("%d", t.Attempt),
			})
		}
	}
}

// StartHealthChecker runs background goroutines for health checking,
// retry processing, and timeout enforcement.
func (s *NodeServer) StartHealthChecker(ctx context.Context) {
	healthTicker := time.NewTicker(s.cfg.HeartbeatInterval)
	retryTicker := time.NewTicker(2 * time.Second)
	timeoutTicker := time.NewTicker(10 * time.Second)
	defer healthTicker.Stop()
	defer retryTicker.Stop()
	defer timeoutTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-healthTicker.C:
			s.checkWorkerHealth()
		case <-retryTicker.C:
			s.Scheduler.ProcessRetries()
		case <-timeoutTicker.C:
			s.Scheduler.CheckTimeouts()
		}
	}
}

// checkWorkerHealth inspects all workers and transitions their state
// based on heartbeat freshness.
func (s *NodeServer) checkWorkerHealth() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for id, w := range s.Workers {
		if w.State == api.WorkerState_WORKER_DEAD || w.State == api.WorkerState_WORKER_DRAINING {
			continue
		}

		sinceLastHB := now.Sub(w.LastHeartbeat)

		if sinceLastHB > s.cfg.WorkerDeadTimeout && w.State == api.WorkerState_WORKER_SUSPECT {
			s.logger.Error("worker declared DEAD",
				"worker_id", id,
				"last_heartbeat", w.LastHeartbeat.Format(time.RFC3339),
				"silent_for", sinceLastHB.String(),
			)
			w.State = api.WorkerState_WORKER_DEAD
			s.publishEvent(events.EventWorkerDead, map[string]string{
				"worker_id":  id,
				"silent_for": sinceLastHB.String(),
			})
		} else if sinceLastHB > s.cfg.HeartbeatTimeout && w.State == api.WorkerState_WORKER_ACTIVE {
			s.logger.Warn("worker marked SUSPECT",
				"worker_id", id,
				"last_heartbeat", w.LastHeartbeat.Format(time.RFC3339),
				"silent_for", sinceLastHB.String(),
			)
			w.State = api.WorkerState_WORKER_SUSPECT
			s.publishEvent(events.EventWorkerSuspect, map[string]string{
				"worker_id":  id,
				"silent_for": sinceLastHB.String(),
			})
		}
	}
}

// GetWorkers returns a snapshot of all known workers.
func (s *NodeServer) GetWorkers() []WorkerInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	workers := make([]WorkerInfo, 0, len(s.Workers))
	for _, w := range s.Workers {
		workers = append(workers, *w)
	}
	return workers
}

// DisconnectWorker forcefully disconnects a worker by cancelling its gRPC stream.
// The stream cancellation triggers the existing defer cleanup in AssignTask,
// which re-queues in-flight tasks automatically.
func (s *NodeServer) DisconnectWorker(workerID string) bool {
	s.streamMu.Lock()
	cancel, exists := s.workerStreams[workerID]
	s.streamMu.Unlock()

	if !exists {
		return false
	}

	s.logger.Warn("chaos: forcefully disconnecting worker", "worker_id", workerID)
	cancel()
	return true
}

// EnqueueInlineTaskDirect enqueues a pre-constructed task (used by batch submit).
func (s *NodeServer) EnqueueInlineTaskDirect(t *task.Task) {
	s.enqueue(t)
}
