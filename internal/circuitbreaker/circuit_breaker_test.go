package circuitbreaker

import (
	"log/slog"
	"os"
	"testing"
	"time"
)

var testLogger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

func TestClosedToOpen(t *testing.T) {
	cb := New("w1", 3, 1, 100*time.Millisecond, testLogger)
	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}
	if cb.GetState() != StateOpen {
		t.Fatalf("expected OPEN after 3 failures, got %s", cb.GetState())
	}
	if cb.Allow() {
		t.Fatal("OPEN circuit should not allow requests")
	}
}

func TestOpenToHalfOpen(t *testing.T) {
	cb := New("w2", 1, 1, 50*time.Millisecond, testLogger)
	cb.RecordFailure() // → OPEN
	time.Sleep(60 * time.Millisecond)
	if !cb.Allow() {
		t.Fatal("should allow after timeout (HALF_OPEN)")
	}
	if cb.GetState() != StateHalfOpen {
		t.Fatalf("expected HALF_OPEN, got %s", cb.GetState())
	}
}

func TestHalfOpenToClosed(t *testing.T) {
	cb := New("w3", 1, 1, 50*time.Millisecond, testLogger)
	cb.RecordFailure()                 // → OPEN
	time.Sleep(60 * time.Millisecond)  // timeout expires
	cb.Allow()                         // → HALF_OPEN
	cb.RecordSuccess()                 // → CLOSED
	if cb.GetState() != StateClosed {
		t.Fatalf("expected CLOSED after success in HALF_OPEN, got %s", cb.GetState())
	}
}

func TestHalfOpenBackToOpen(t *testing.T) {
	cb := New("w4", 1, 1, 50*time.Millisecond, testLogger)
	cb.RecordFailure()                 // → OPEN
	time.Sleep(60 * time.Millisecond)  // timeout expires
	cb.Allow()                         // → HALF_OPEN
	cb.RecordFailure()                 // → OPEN again
	if cb.GetState() != StateOpen {
		t.Fatalf("expected OPEN after failure in HALF_OPEN, got %s", cb.GetState())
	}
}

func TestSuccessInClosedResets(t *testing.T) {
	cb := New("w5", 3, 1, time.Second, testLogger)
	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordSuccess()
	// After success, failure count resets. 2 more failures should not trip.
	cb.RecordFailure()
	cb.RecordFailure()
	if cb.GetState() != StateClosed {
		t.Fatalf("expected CLOSED (count reset), got %s", cb.GetState())
	}
}

func TestRegistryGetOrCreate(t *testing.T) {
	reg := NewRegistry(5, 2, 30*time.Second, testLogger)

	cb1 := reg.Get("worker-1")
	cb2 := reg.Get("worker-1")
	if cb1 != cb2 {
		t.Fatal("expected same circuit breaker for same worker ID")
	}

	cb3 := reg.Get("worker-2")
	if cb1 == cb3 {
		t.Fatal("expected different circuit breakers for different workers")
	}
}
