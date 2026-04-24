package store

import (
	"sync"

	"github.com/jalala984/master-worker/internal/task"
)

// MemoryStore is an in-memory TaskStore for testing and development.
// Not crash-safe — all data is lost on restart.
type MemoryStore struct {
	mu    sync.RWMutex
	tasks map[string]*task.Task
}

// NewMemoryStore creates an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		tasks: make(map[string]*task.Task),
	}
}

func (m *MemoryStore) Save(t *task.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks[t.ID] = t
	return nil
}

func (m *MemoryStore) Get(id string) (*task.Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.tasks[id]
	if !ok {
		return nil, nil
	}
	return t, nil
}

func (m *MemoryStore) List() ([]*task.Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*task.Task, 0, len(m.tasks))
	for _, t := range m.tasks {
		result = append(result, t)
	}
	return result, nil
}

func (m *MemoryStore) Delete(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tasks, id)
	return nil
}

func (m *MemoryStore) GetByState(state task.State) ([]*task.Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*task.Task
	for _, t := range m.tasks {
		if t.State == state {
			result = append(result, t)
		}
	}
	return result, nil
}

func (m *MemoryStore) Close() error {
	return nil
}
