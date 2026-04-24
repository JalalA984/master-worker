// Package task's Scheduler replaces the raw buffered channel with a priority
// queue that supports retry with exponential backoff, timeout enforcement,
// and a dead letter queue.
//
// Priority queue scheduling ensures high-priority tasks are dispatched first.
// This is the same pattern used by:
//   - Linux CFS scheduler (priority + fairness)
//   - Kubernetes scheduler (priority classes)
//   - Amazon SQS FIFO queues with message group priorities
//
// The dead letter queue (DLQ) holds tasks that exhausted all retry attempts.
// Operators can inspect DLQ entries and manually retry them. This pattern
// originates from message queuing systems (IBM MQ, RabbitMQ, Amazon SQS).
package task

import (
	"container/heap"
	"log/slog"
	"sync"
	"time"
)

// Scheduler manages the task lifecycle: enqueue, dequeue, retry, timeout, DLQ.
type Scheduler struct {
	mu         sync.Mutex
	queue      priorityQueue
	retryQueue []*Task
	deadLetter []*Task
	inFlight   map[string]*Task
	completed  []*Task // Recent completed tasks (capped at maxCompleted)
	logger     *slog.Logger
	taskTimeout time.Duration

	// Lifetime counters for the stats API.
	totalCompleted int64
	totalFailed    int64

	// notify signals that a new task is available for dispatch.
	notify chan struct{}
}

const maxCompleted = 100  // Max completed tasks to keep in memory for the dashboard.
const maxQueuedInDash = 200 // Max queued tasks returned to the dashboard per poll.

// NewScheduler creates a scheduler with the given task timeout.
func NewScheduler(logger *slog.Logger, taskTimeout time.Duration) *Scheduler {
	s := &Scheduler{
		inFlight:    make(map[string]*Task),
		logger:      logger,
		taskTimeout: taskTimeout,
		notify:      make(chan struct{}, 1),
	}
	heap.Init(&s.queue)
	return s
}

// Enqueue adds a task to the priority queue.
func (s *Scheduler) Enqueue(t *Task) {
	s.mu.Lock()
	defer s.mu.Unlock()

	heap.Push(&s.queue, t)
	s.logger.Debug("task enqueued", "task_id", t.ID, "priority", t.Priority, "queue_size", s.queue.Len())

	// Non-blocking signal that work is available.
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// Dequeue removes and returns the highest-priority task ready for dispatch.
// Returns nil if no tasks are available.
//
// After dequeuing, if there are still tasks waiting we re-arm the notify
// channel (non-blocking). This is critical for correctness: Enqueue only
// sends one signal even when enqueueing many tasks at once (the channel
// buffer is 1, all subsequent sends hit the default branch). Without the
// re-arm here, each notify signal would unblock exactly one worker and the
// rest of the queue would sit idle forever.
func (s *Scheduler) Dequeue() *Task {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.queue.Len() == 0 {
		return nil
	}

	t := heap.Pop(&s.queue).(*Task)
	s.inFlight[t.ID] = t

	// Re-signal if more tasks remain so the next waiting worker wakes up.
	if s.queue.Len() > 0 {
		select {
		case s.notify <- struct{}{}:
		default:
		}
	}

	return t
}

// NotifyChan returns the channel that signals new tasks are available.
func (s *Scheduler) NotifyChan() <-chan struct{} {
	return s.notify
}

// Notify sends a non-blocking signal that work may be available.
// Used when a worker skips a task (e.g., circuit breaker open) so that
// another worker can still pick up the task.
func (s *Scheduler) Notify() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// Complete marks a task as completed and removes it from in-flight.
func (s *Scheduler) Complete(taskID string, output string, exitCode int32) *Task {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, exists := s.inFlight[taskID]
	if !exists {
		return nil
	}
	delete(s.inFlight, taskID)

	t.Complete(output, exitCode)
	s.totalCompleted++
	s.completed = append(s.completed, t)
	if len(s.completed) > maxCompleted {
		s.completed = s.completed[len(s.completed)-maxCompleted:]
	}
	return t
}

// Fail marks a task as failed and either schedules a retry or moves to DLQ.
func (s *Scheduler) Fail(taskID string, errMsg string, exitCode int32) *Task {
	s.mu.Lock()
	defer s.mu.Unlock()

	t, exists := s.inFlight[taskID]
	if !exists {
		return nil
	}
	delete(s.inFlight, taskID)

	t.Fail(errMsg, exitCode)
	s.totalFailed++

	if err := t.ScheduleRetry(); err != nil {
		s.logger.Error("failed to schedule retry", "task_id", t.ID, "error", err)
	}

	if t.State == StateDead {
		s.deadLetter = append(s.deadLetter, t)
		s.logger.Warn("task moved to dead letter queue",
			"task_id", t.ID, "attempts", t.Attempt)
	} else {
		s.retryQueue = append(s.retryQueue, t)
		s.logger.Info("task scheduled for retry",
			"task_id", t.ID, "attempt", t.Attempt, "next_retry", t.NextRetryAt)
	}

	return t
}

// RequeueFromWorker re-enqueues all in-flight tasks for a specific worker.
// Called when a worker disconnects to recover its tasks.
func (s *Scheduler) RequeueFromWorker(workerID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	for id, t := range s.inFlight {
		if t.WorkerID == workerID {
			delete(s.inFlight, id)
			t.State = StateQueued
			t.WorkerID = ""
			heap.Push(&s.queue, t)
			count++
			s.logger.Warn("requeued task from disconnected worker",
				"task_id", id, "worker_id", workerID)
		}
	}

	if count > 0 {
		select {
		case s.notify <- struct{}{}:
		default:
		}
	}

	return count
}

// ProcessRetries checks the retry queue and moves tasks that are ready back to the main queue.
// Should be called periodically by a background goroutine.
func (s *Scheduler) ProcessRetries() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	count := 0
	remaining := make([]*Task, 0, len(s.retryQueue))

	for _, t := range s.retryQueue {
		if now.After(t.NextRetryAt) {
			t.Requeue()
			heap.Push(&s.queue, t)
			count++
			s.logger.Info("retrying task", "task_id", t.ID, "attempt", t.Attempt+1)
		} else {
			remaining = append(remaining, t)
		}
	}
	s.retryQueue = remaining

	if count > 0 {
		select {
		case s.notify <- struct{}{}:
		default:
		}
	}

	return count
}

// CheckTimeouts checks in-flight tasks for timeout violations.
// Tasks that have exceeded their timeout are failed and scheduled for retry.
func (s *Scheduler) CheckTimeouts() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	count := 0
	var timedOut []*Task

	for _, t := range s.inFlight {
		if t.State == StateRunning && t.TimeoutSec > 0 {
			deadline := t.StartedAt.Add(time.Duration(t.TimeoutSec) * time.Second)
			if now.After(deadline) {
				timedOut = append(timedOut, t)
			}
		}
	}

	for _, t := range timedOut {
		delete(s.inFlight, t.ID)
		t.Fail("task execution timed out", -1)

		if err := t.ScheduleRetry(); err != nil {
			s.logger.Error("failed to schedule retry for timed-out task", "task_id", t.ID, "error", err)
		}

		if t.State == StateDead {
			s.deadLetter = append(s.deadLetter, t)
			s.logger.Warn("timed-out task moved to DLQ", "task_id", t.ID)
		} else {
			s.retryQueue = append(s.retryQueue, t)
			s.logger.Warn("timed-out task scheduled for retry",
				"task_id", t.ID, "attempt", t.Attempt)
		}
		count++
	}

	return count
}

// DeadLetterQueue returns a copy of all tasks in the DLQ.
func (s *Scheduler) DeadLetterQueue() []*Task {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make([]*Task, len(s.deadLetter))
	copy(result, s.deadLetter)
	return result
}

// RetryFromDLQ moves a task from the DLQ back to the main queue.
func (s *Scheduler) RetryFromDLQ(taskID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, t := range s.deadLetter {
		if t.ID == taskID {
			s.deadLetter = append(s.deadLetter[:i], s.deadLetter[i+1:]...)
			t.RetryFromDLQ()
			heap.Push(&s.queue, t)
			s.logger.Info("task retried from DLQ", "task_id", taskID)

			select {
			case s.notify <- struct{}{}:
			default:
			}
			return true
		}
	}
	return false
}

// Stats returns current scheduler statistics.
type Stats struct {
	QueueDepth     int
	InFlight       int
	RetryPending   int
	DeadLettered   int
	TotalCompleted int64
	TotalFailed    int64
}

func (s *Scheduler) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()

	return Stats{
		QueueDepth:     s.queue.Len(),
		InFlight:       len(s.inFlight),
		RetryPending:   len(s.retryQueue),
		DeadLettered:   len(s.deadLetter),
		TotalCompleted: s.totalCompleted,
		TotalFailed:    s.totalFailed,
	}
}

// InFlightTasks returns a snapshot of all in-flight tasks.
func (s *Scheduler) InFlightTasks() []*Task {
	s.mu.Lock()
	defer s.mu.Unlock()

	tasks := make([]*Task, 0, len(s.inFlight))
	for _, t := range s.inFlight {
		tasks = append(tasks, t)
	}
	return tasks
}

// AllTasks returns tasks from all queues (for dashboard/API use).
//
// The queued set is capped at maxQueuedInDash entries (highest priority
// first) to keep the HTTP response small when the queue is large (e.g.,
// 50 000 tasks). The accurate total is always available via Stats().
// In-flight, retry, DLQ, and recent-completed tasks are returned in full
// because their counts are bounded by system capacity, not batch size.
func (s *Scheduler) AllTasks() []*Task {
	s.mu.Lock()
	defer s.mu.Unlock()

	// The heap array is in heap order (not perfect priority order), but
	// slicing the front gives the highest-priority tasks which are most
	// useful to show in the dashboard when the queue is large.
	queueLimit := len(s.queue)
	if queueLimit > maxQueuedInDash {
		queueLimit = maxQueuedInDash
	}
	all := make([]*Task, 0, queueLimit+len(s.inFlight)+len(s.retryQueue)+len(s.deadLetter)+50)
	for i := 0; i < queueLimit; i++ {
		all = append(all, s.queue[i])
	}
	// In-flight: always return all (bounded by worker count × concurrency).
	for _, t := range s.inFlight {
		all = append(all, t)
	}
	// Retry queue.
	all = append(all, s.retryQueue...)
	// DLQ.
	all = append(all, s.deadLetter...)
	// Recent completed (last 50 for dashboard).
	start := 0
	if len(s.completed) > 50 {
		start = len(s.completed) - 50
	}
	all = append(all, s.completed[start:]...)
	return all
}

// priorityQueue implements heap.Interface for priority-based task scheduling.
// Higher priority tasks are dequeued first. For equal priority, earlier
// creation time wins (FIFO within priority level).
type priorityQueue []*Task

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	// Higher priority first
	if pq[i].Priority != pq[j].Priority {
		return pq[i].Priority > pq[j].Priority
	}
	// Same priority: earlier created first (FIFO)
	return pq[i].CreatedAt.Before(pq[j].CreatedAt)
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *priorityQueue) Push(x interface{}) {
	*pq = append(*pq, x.(*Task))
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*pq = old[:n-1]
	return item
}
