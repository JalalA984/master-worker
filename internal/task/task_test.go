package task

import (
	"testing"
)

func TestNewTask(t *testing.T) {
	tk := New("test-1", "/path/script.sh")
	if tk.ID != "test-1" {
		t.Fatalf("expected ID test-1, got %s", tk.ID)
	}
	if tk.State != StateQueued {
		t.Fatalf("expected QUEUED, got %s", tk.State)
	}
	if tk.Priority != PriorityNormal {
		t.Fatalf("expected PriorityNormal, got %d", tk.Priority)
	}
	if tk.MaxRetries != 3 {
		t.Fatalf("expected MaxRetries=3, got %d", tk.MaxRetries)
	}
	if tk.Type != TaskTypeScriptPath {
		t.Fatalf("expected SCRIPT_PATH, got %s", tk.Type)
	}
}

func TestNewInlineTask(t *testing.T) {
	tk := NewInline("inline-1", "print('hi')", "python")
	if tk.Type != TaskTypeInline {
		t.Fatalf("expected INLINE, got %s", tk.Type)
	}
	if tk.Content != "print('hi')" {
		t.Fatalf("expected script content, got %s", tk.Content)
	}
	if tk.Language != "python" {
		t.Fatalf("expected python, got %s", tk.Language)
	}
}

func TestNewArchiveTask(t *testing.T) {
	tk := NewArchive("arc-1", "base64data", "make test", "bash")
	if tk.Type != TaskTypeArchive {
		t.Fatalf("expected ARCHIVE, got %s", tk.Type)
	}
	if tk.Entrypoint != "make test" {
		t.Fatalf("expected entrypoint 'make test', got %s", tk.Entrypoint)
	}
}

func TestValidTransitions(t *testing.T) {
	// QUEUED → ASSIGNED → RUNNING → COMPLETED
	tk := New("t1", "s.sh")
	if err := tk.Assign("w1"); err != nil {
		t.Fatalf("Assign failed: %v", err)
	}
	if tk.State != StateAssigned {
		t.Fatalf("expected ASSIGNED, got %s", tk.State)
	}
	if err := tk.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if err := tk.Complete("output", 0); err != nil {
		t.Fatalf("Complete failed: %v", err)
	}
	if tk.State != StateCompleted {
		t.Fatalf("expected COMPLETED, got %s", tk.State)
	}
}

func TestFailAndRetry(t *testing.T) {
	tk := New("t2", "s.sh")
	tk.Assign("w1")
	tk.Start()
	if err := tk.Fail("err", 1); err != nil {
		t.Fatalf("Fail failed: %v", err)
	}
	if tk.State != StateFailed {
		t.Fatalf("expected FAILED, got %s", tk.State)
	}
	if err := tk.ScheduleRetry(); err != nil {
		t.Fatalf("ScheduleRetry failed: %v", err)
	}
	if tk.State != StateRetrying {
		t.Fatalf("expected RETRYING, got %s", tk.State)
	}
	if err := tk.Requeue(); err != nil {
		t.Fatalf("Requeue failed: %v", err)
	}
	if tk.State != StateQueued {
		t.Fatalf("expected QUEUED, got %s", tk.State)
	}
}

func TestExhaustRetriesMovesToDLQ(t *testing.T) {
	tk := New("t3", "s.sh")
	tk.MaxRetries = 1
	tk.Attempt = 1

	tk.Assign("w1")
	tk.Start()
	tk.Fail("err", 1)
	if err := tk.ScheduleRetry(); err != nil {
		t.Fatalf("ScheduleRetry should transition to DEAD: %v", err)
	}
	if tk.State != StateDead {
		t.Fatalf("expected DEAD after exhausting retries, got %s", tk.State)
	}
}

func TestInvalidTransition(t *testing.T) {
	tk := New("t4", "s.sh")
	// QUEUED → COMPLETED should fail
	err := tk.Transition(StateCompleted)
	if err == nil {
		t.Fatal("expected error for invalid transition QUEUED → COMPLETED")
	}
}

func TestRetryFromDLQ(t *testing.T) {
	tk := New("t5", "s.sh")
	tk.MaxRetries = 0
	tk.Assign("w1")
	tk.Start()
	tk.Fail("err", 1)
	tk.ScheduleRetry() // → DEAD
	if tk.State != StateDead {
		t.Fatalf("expected DEAD, got %s", tk.State)
	}
	if err := tk.RetryFromDLQ(); err != nil {
		t.Fatalf("RetryFromDLQ failed: %v", err)
	}
	if tk.State != StateQueued {
		t.Fatalf("expected QUEUED after DLQ retry, got %s", tk.State)
	}
	if tk.Attempt != 0 {
		t.Fatalf("expected Attempt reset to 0, got %d", tk.Attempt)
	}
}

func TestIsTerminal(t *testing.T) {
	if !StateCompleted.IsTerminal() {
		t.Fatal("COMPLETED should be terminal")
	}
	if !StateDead.IsTerminal() {
		t.Fatal("DEAD should be terminal")
	}
	if StateRunning.IsTerminal() {
		t.Fatal("RUNNING should not be terminal")
	}
}

func TestDuration(t *testing.T) {
	tk := New("t6", "s.sh")
	if tk.Duration() != 0 {
		t.Fatal("expected 0 duration before start")
	}
}
