package task

import (
	"log/slog"
	"os"
	"testing"
	"time"
)

func newTestScheduler() *Scheduler {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	return NewScheduler(logger, 5*time.Minute)
}

func TestEnqueueDequeue(t *testing.T) {
	s := newTestScheduler()
	t1 := New("a", "1.sh")
	t2 := New("b", "2.sh")
	s.Enqueue(t1)
	s.Enqueue(t2)

	stats := s.Stats()
	if stats.QueueDepth != 2 {
		t.Fatalf("expected queue depth 2, got %d", stats.QueueDepth)
	}

	got := s.Dequeue()
	if got == nil {
		t.Fatal("expected a task, got nil")
	}
	if stats = s.Stats(); stats.InFlight != 1 {
		t.Fatalf("expected 1 in-flight, got %d", stats.InFlight)
	}
}

func TestPriorityOrder(t *testing.T) {
	s := newTestScheduler()
	low := New("low", "l.sh")
	low.Priority = PriorityLow
	high := New("high", "h.sh")
	high.Priority = PriorityHigh

	s.Enqueue(low)
	s.Enqueue(high)

	got := s.Dequeue()
	if got.ID != "high" {
		t.Fatalf("expected high-priority task first, got %s", got.ID)
	}
}

func TestCompleteRemovesFromInFlight(t *testing.T) {
	s := newTestScheduler()
	tk := New("x", "x.sh")
	s.Enqueue(tk)
	dequeued := s.Dequeue()
	dequeued.Assign("w1")
	dequeued.Start()

	result := s.Complete("x", "done", 0)
	if result == nil {
		t.Fatal("expected task back from Complete")
	}
	if result.State != StateCompleted {
		t.Fatalf("expected COMPLETED, got %s", result.State)
	}
	if s.Stats().InFlight != 0 {
		t.Fatal("expected 0 in-flight after completion")
	}
}

func TestFailSchedulesRetry(t *testing.T) {
	s := newTestScheduler()
	tk := New("y", "y.sh")
	s.Enqueue(tk)
	dequeued := s.Dequeue()
	dequeued.Assign("w1")
	dequeued.Start()

	result := s.Fail("y", "oops", 1)
	if result == nil {
		t.Fatal("expected task back from Fail")
	}
	if s.Stats().RetryPending != 1 {
		t.Fatalf("expected 1 retry pending, got %d", s.Stats().RetryPending)
	}
}

func TestRequeueFromWorker(t *testing.T) {
	s := newTestScheduler()
	tk := New("z", "z.sh")
	s.Enqueue(tk)
	dequeued := s.Dequeue()
	dequeued.WorkerID = "w1"

	count := s.RequeueFromWorker("w1")
	if count != 1 {
		t.Fatalf("expected 1 requeued, got %d", count)
	}
	if s.Stats().QueueDepth != 1 {
		t.Fatal("expected task back in queue")
	}
}

func TestDLQFlow(t *testing.T) {
	s := newTestScheduler()
	tk := New("dlq", "d.sh")
	tk.MaxRetries = 0
	s.Enqueue(tk)
	dequeued := s.Dequeue()
	dequeued.Assign("w1")
	dequeued.Start()

	s.Fail("dlq", "fatal", 1)
	if s.Stats().DeadLettered != 1 {
		t.Fatalf("expected 1 dead-lettered, got %d", s.Stats().DeadLettered)
	}

	dlq := s.DeadLetterQueue()
	if len(dlq) != 1 {
		t.Fatalf("expected 1 in DLQ, got %d", len(dlq))
	}

	ok := s.RetryFromDLQ("dlq")
	if !ok {
		t.Fatal("RetryFromDLQ should return true")
	}
	if s.Stats().QueueDepth != 1 {
		t.Fatal("expected task back in queue after DLQ retry")
	}
}

func TestProcessRetries(t *testing.T) {
	s := newTestScheduler()
	tk := New("r", "r.sh")
	s.Enqueue(tk)
	dequeued := s.Dequeue()
	dequeued.Assign("w1")
	dequeued.Start()

	s.Fail("r", "err", 1)
	// Force NextRetryAt to past
	s.mu.Lock()
	s.retryQueue[0].NextRetryAt = time.Now().Add(-1 * time.Second)
	s.mu.Unlock()

	count := s.ProcessRetries()
	if count != 1 {
		t.Fatalf("expected 1 retry processed, got %d", count)
	}
	if s.Stats().QueueDepth != 1 {
		t.Fatal("expected task back in queue after retry processing")
	}
}

func TestAllTasks(t *testing.T) {
	s := newTestScheduler()
	s.Enqueue(New("a", "a.sh"))
	s.Enqueue(New("b", "b.sh"))
	s.Dequeue() // moves one to in-flight

	all := s.AllTasks()
	if len(all) != 2 {
		t.Fatalf("expected 2 total tasks, got %d", len(all))
	}
}

func TestDequeueEmptyReturnsNil(t *testing.T) {
	s := newTestScheduler()
	if got := s.Dequeue(); got != nil {
		t.Fatal("expected nil from empty queue")
	}
}
