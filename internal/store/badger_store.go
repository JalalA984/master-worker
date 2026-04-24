// BadgerDB-backed TaskStore for crash recovery.
//
// BadgerDB is an embedded key-value store (no separate server process).
// It uses an LSM-tree (Log-Structured Merge-tree) storage engine, the same
// approach used by LevelDB, RocksDB, and Cassandra's storage layer.
//
// Why BadgerDB over Redis:
//   - Embedded: no extra container/process to manage
//   - Crash-safe: WAL + SSTable persistence
//   - Written in Go: easy to debug and understand
//   - LSM-tree teaches an important storage concept
//
// Trade-off: no network access (single-process only), no replication.
// For HA, you'd need a distributed store (etcd, CockroachDB).
package store

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/dgraph-io/badger/v4"
	"github.com/jalala984/master-worker/internal/task"
)

// BadgerStore persists tasks to BadgerDB on disk.
type BadgerStore struct {
	db     *badger.DB
	logger *slog.Logger
}

// NewBadgerStore opens (or creates) a BadgerDB at the given path.
func NewBadgerStore(path string, logger *slog.Logger) (*BadgerStore, error) {
	opts := badger.DefaultOptions(path).
		WithLogger(nil) // Suppress badger's internal logging
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("badger open %s: %w", path, err)
	}

	logger.Info("badger store opened", "path", path)
	return &BadgerStore{db: db, logger: logger}, nil
}

func taskKey(id string) []byte {
	return []byte("task:" + id)
}

func (s *BadgerStore) Save(t *task.Task) error {
	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("marshal task %s: %w", t.ID, err)
	}

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(taskKey(t.ID), data)
	})
}

func (s *BadgerStore) Get(id string) (*task.Task, error) {
	var t task.Task
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(taskKey(id))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &t)
		})
	})
	if err != nil {
		return nil, err
	}
	if t.ID == "" {
		return nil, nil
	}
	return &t, nil
}

func (s *BadgerStore) List() ([]*task.Task, error) {
	var tasks []*task.Task
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte("task:")
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			var t task.Task
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &t)
			}); err != nil {
				return err
			}
			tasks = append(tasks, &t)
		}
		return nil
	})
	return tasks, err
}

func (s *BadgerStore) Delete(id string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(taskKey(id))
	})
}

func (s *BadgerStore) GetByState(state task.State) ([]*task.Task, error) {
	all, err := s.List()
	if err != nil {
		return nil, err
	}
	var result []*task.Task
	for _, t := range all {
		if t.State == state {
			result = append(result, t)
		}
	}
	return result, nil
}

func (s *BadgerStore) Close() error {
	s.logger.Info("closing badger store")
	return s.db.Close()
}

// RecoverTasks loads non-terminal tasks from the store for crash recovery.
// Called on master startup to re-enqueue tasks that were in-flight when
// the previous master instance crashed.
func (s *BadgerStore) RecoverTasks() ([]*task.Task, error) {
	all, err := s.List()
	if err != nil {
		return nil, err
	}
	var recoverable []*task.Task
	for _, t := range all {
		if !t.State.IsTerminal() {
			// Reset to queued for re-dispatch
			t.State = task.StateQueued
			t.WorkerID = ""
			recoverable = append(recoverable, t)
		}
	}
	s.logger.Info("recovered tasks from store", "count", len(recoverable))
	return recoverable, nil
}
