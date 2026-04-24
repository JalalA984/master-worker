// Package store defines the TaskStore interface (Repository pattern) and
// provides implementations for task persistence.
//
// The Repository pattern decouples business logic from storage concerns:
//   - Business logic (scheduler) works with the interface
//   - Tests use the in-memory implementation
//   - Production uses BadgerDB for crash recovery
//
// This is the same pattern used by:
//   - Kubernetes: etcd backend behind the storage interface
//   - CockroachDB: storage engine interface with multiple backends
//   - Docker: image/layer store abstractions
package store

import (
	"github.com/jalala984/master-worker/internal/task"
)

// TaskStore defines the persistence interface for tasks.
// Implementations must be safe for concurrent use.
type TaskStore interface {
	// Save persists a task (insert or update).
	Save(t *task.Task) error

	// Get retrieves a task by ID. Returns nil if not found.
	Get(id string) (*task.Task, error)

	// List returns all stored tasks.
	List() ([]*task.Task, error)

	// Delete removes a task by ID.
	Delete(id string) error

	// GetByState returns all tasks in a given state.
	GetByState(state task.State) ([]*task.Task, error)

	// Close releases any resources held by the store.
	Close() error
}
