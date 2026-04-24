package store

import (
	"testing"

	"github.com/jalala984/master-worker/internal/task"
)

func TestMemoryStoreSaveAndGet(t *testing.T) {
	s := NewMemoryStore()
	tk := task.New("t1", "s.sh")
	if err := s.Save(tk); err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	got, err := s.Get("t1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.ID != "t1" {
		t.Fatalf("expected t1, got %s", got.ID)
	}
}

func TestMemoryStoreGetNotFound(t *testing.T) {
	s := NewMemoryStore()
	got, err := s.Get("nonexistent")
	if err != nil {
		t.Fatalf("Get should not error for missing key: %v", err)
	}
	if got != nil {
		t.Fatal("expected nil for missing key")
	}
}

func TestMemoryStoreList(t *testing.T) {
	s := NewMemoryStore()
	s.Save(task.New("a", "a.sh"))
	s.Save(task.New("b", "b.sh"))
	list, err := s.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2, got %d", len(list))
	}
}

func TestMemoryStoreDelete(t *testing.T) {
	s := NewMemoryStore()
	s.Save(task.New("d", "d.sh"))
	if err := s.Delete("d"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	got, _ := s.Get("d")
	if got != nil {
		t.Fatal("expected nil after delete")
	}
}

func TestMemoryStoreGetByState(t *testing.T) {
	s := NewMemoryStore()
	t1 := task.New("q1", "q.sh")
	t2 := task.New("q2", "q.sh")
	t3 := task.New("c1", "c.sh")
	t3.State = task.StateCompleted
	s.Save(t1)
	s.Save(t2)
	s.Save(t3)

	queued, err := s.GetByState(task.StateQueued)
	if err != nil {
		t.Fatalf("GetByState failed: %v", err)
	}
	if len(queued) != 2 {
		t.Fatalf("expected 2 queued, got %d", len(queued))
	}
}

func TestMemoryStoreClose(t *testing.T) {
	s := NewMemoryStore()
	if err := s.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}
