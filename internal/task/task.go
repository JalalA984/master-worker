// Package task provides the task domain model with validated state machine transitions.
//
// State machines are fundamental to distributed systems — every task, worker,
// and connection has a lifecycle modeled as states + transitions. Invalid
// transitions (e.g., COMPLETED → RUNNING) indicate bugs.
//
// This model is inspired by:
//   - Google Borg task states (Verma et al., EuroSys 2015)
//   - Amazon SQS message lifecycle (visible → in-flight → deleted/dead-letter)
//   - Kubernetes Pod phases (Pending → Running → Succeeded/Failed)
package task

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

// State represents the lifecycle phase of a task.
type State int

const (
	StateQueued    State = iota // Waiting in queue for assignment
	StateAssigned               // Assigned to a worker, not yet executing
	StateRunning                // Worker is actively executing
	StateCompleted              // Finished successfully (exit code 0)
	StateFailed                 // Execution failed (non-zero exit or error)
	StateRetrying               // Awaiting retry after failure
	StateDead                   // Exhausted retries, in dead letter queue
)

func (s State) String() string {
	switch s {
	case StateQueued:
		return "QUEUED"
	case StateAssigned:
		return "ASSIGNED"
	case StateRunning:
		return "RUNNING"
	case StateCompleted:
		return "COMPLETED"
	case StateFailed:
		return "FAILED"
	case StateRetrying:
		return "RETRYING"
	case StateDead:
		return "DEAD"
	default:
		return "UNKNOWN"
	}
}

// IsTerminal returns true if the task is in a final state.
func (s State) IsTerminal() bool {
	return s == StateCompleted || s == StateDead
}

// Priority levels for the scheduler priority queue.
type Priority int

const (
	PriorityLow    Priority = 0
	PriorityNormal Priority = 1
	PriorityHigh   Priority = 2
)

// validTransitions defines the allowed state machine transitions.
// Any transition not in this map is a bug.
var validTransitions = map[State][]State{
	StateQueued:    {StateAssigned},
	StateAssigned:  {StateRunning, StateQueued},       // StateQueued for reassignment on worker failure
	StateRunning:   {StateCompleted, StateFailed},
	StateFailed:    {StateRetrying, StateDead},
	StateRetrying:  {StateQueued},                     // Goes back to queue for retry
	StateCompleted: {},                                // Terminal
	StateDead:      {StateQueued},                     // Manual retry from DLQ
}

// TaskType defines how the task content is delivered and executed.
type TaskType int

const (
	TaskTypeScriptPath TaskType = 0 // Execute script at filesystem path
	TaskTypeInline     TaskType = 1 // Execute inline script content
	TaskTypeArchive    TaskType = 2 // Extract tar.gz archive, run entrypoint
)

func (t TaskType) String() string {
	switch t {
	case TaskTypeScriptPath:
		return "SCRIPT_PATH"
	case TaskTypeInline:
		return "INLINE"
	case TaskTypeArchive:
		return "ARCHIVE"
	default:
		return "UNKNOWN"
	}
}

// Task is the domain model for a unit of work in the system.
type Task struct {
	ID          string
	Script      string
	State       State
	Priority    Priority
	Attempt     int32
	MaxRetries  int32
	WorkerID    string
	Output      string
	Error       string
	ExitCode    int32

	// Enhanced task types: inline scripts, multi-language, archive uploads.
	Type       TaskType // SCRIPT_PATH, INLINE, or ARCHIVE
	Content    string   // Script body (INLINE) or base64 archive (ARCHIVE)
	Language   string   // Interpreter: bash, python, node
	Entrypoint string   // Command to run after archive extraction

	// Timing
	CreatedAt   time.Time
	AssignedAt  time.Time
	StartedAt   time.Time
	CompletedAt time.Time
	NextRetryAt time.Time

	// Timeout
	TimeoutSec  int64
}

// New creates a new task in the QUEUED state.
func New(id, script string) *Task {
	return &Task{
		ID:         id,
		Script:     script,
		State:      StateQueued,
		Priority:   PriorityNormal,
		Attempt:    0,
		MaxRetries: 3,
		CreatedAt:  time.Now(),
		TimeoutSec: 300, // 5 minute default
	}
}

// NewInline creates an inline script task.
func NewInline(id, content, language string) *Task {
	t := New(id, "")
	t.Type = TaskTypeInline
	t.Content = content
	t.Language = language
	return t
}

// NewArchive creates an archive upload task.
func NewArchive(id, content, entrypoint, language string) *Task {
	t := New(id, "")
	t.Type = TaskTypeArchive
	t.Content = content
	t.Entrypoint = entrypoint
	t.Language = language
	return t
}

// Transition validates and applies a state transition.
// Returns an error if the transition is invalid per the state machine.
func (t *Task) Transition(to State) error {
	allowed := validTransitions[t.State]
	for _, s := range allowed {
		if s == to {
			t.State = to
			return nil
		}
	}
	return fmt.Errorf("invalid transition: %s → %s for task %s", t.State, to, t.ID)
}

// Assign transitions the task to ASSIGNED and records the worker.
func (t *Task) Assign(workerID string) error {
	if err := t.Transition(StateAssigned); err != nil {
		return err
	}
	t.WorkerID = workerID
	t.AssignedAt = time.Now()
	t.Attempt++
	return nil
}

// Start transitions the task to RUNNING.
func (t *Task) Start() error {
	if err := t.Transition(StateRunning); err != nil {
		return err
	}
	t.StartedAt = time.Now()
	return nil
}

// Complete transitions the task to COMPLETED.
func (t *Task) Complete(output string, exitCode int32) error {
	if err := t.Transition(StateCompleted); err != nil {
		return err
	}
	t.Output = output
	t.ExitCode = exitCode
	t.CompletedAt = time.Now()
	return nil
}

// Fail transitions the task to FAILED.
func (t *Task) Fail(errMsg string, exitCode int32) error {
	if err := t.Transition(StateFailed); err != nil {
		return err
	}
	t.Error = errMsg
	t.ExitCode = exitCode
	t.CompletedAt = time.Now()
	return nil
}

// ScheduleRetry transitions FAILED → RETRYING, computing the next retry time
// with exponential backoff and jitter.
//
// Backoff formula: base * 2^attempt + random_jitter
// This is the "full jitter" strategy recommended by AWS:
// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
//
// Without jitter, retrying tasks could "thundering herd" the system.
func (t *Task) ScheduleRetry() error {
	if t.Attempt >= t.MaxRetries {
		return t.Transition(StateDead)
	}
	if err := t.Transition(StateRetrying); err != nil {
		return err
	}
	backoff := time.Duration(math.Pow(2, float64(t.Attempt))) * time.Second
	jitter := time.Duration(rand.Int63n(int64(time.Second)))
	t.NextRetryAt = time.Now().Add(backoff + jitter)
	return nil
}

// Requeue transitions the task back to QUEUED (from RETRYING or ASSIGNED).
func (t *Task) Requeue() error {
	return t.Transition(StateQueued)
}

// RetryFromDLQ manually retries a dead-lettered task.
func (t *Task) RetryFromDLQ() error {
	if err := t.Transition(StateQueued); err != nil {
		return err
	}
	t.Attempt = 0
	t.Error = ""
	return nil
}

// Duration returns the execution duration if timing data is available.
func (t *Task) Duration() time.Duration {
	if t.StartedAt.IsZero() || t.CompletedAt.IsZero() {
		return 0
	}
	return t.CompletedAt.Sub(t.StartedAt)
}
