package health

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func newTestChecker() *Checker {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	return NewChecker(logger)
}

func TestLivenessAlways200(t *testing.T) {
	c := newTestChecker()
	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()
	c.LivenessHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestReadinessBeforeReady(t *testing.T) {
	c := newTestChecker()
	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()
	c.ReadinessHandler(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 before SetReady, got %d", w.Code)
	}
}

func TestReadinessAfterReady(t *testing.T) {
	c := newTestChecker()
	c.SetReady(true)
	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()
	c.ReadinessHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 after SetReady, got %d", w.Code)
	}
}

func TestReadinessAfterUnready(t *testing.T) {
	c := newTestChecker()
	c.SetReady(true)
	c.SetReady(false)
	req := httptest.NewRequest("GET", "/readyz", nil)
	w := httptest.NewRecorder()
	c.ReadinessHandler(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 after SetReady(false), got %d", w.Code)
	}
}
