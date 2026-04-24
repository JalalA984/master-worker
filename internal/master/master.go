// Package master orchestrates the master node's lifecycle.
//
// The master runs two servers:
//   - gRPC (:50051): workers connect here for task streaming and heartbeats
//   - HTTP  (:9092): humans/scripts submit tasks, check health, and view the dashboard
//
// Lifecycle management uses errgroup (golang.org/x/sync) for structured
// concurrency: if any server goroutine fails, all others are cancelled.
package master

import (
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/jalala984/master-worker/api"
	"github.com/jalala984/master-worker/internal/circuitbreaker"
	"github.com/jalala984/master-worker/internal/config"
	"github.com/jalala984/master-worker/internal/events"
	"github.com/jalala984/master-worker/internal/health"
	"github.com/jalala984/master-worker/internal/interceptors"
	"github.com/jalala984/master-worker/internal/middleware"
	"github.com/jalala984/master-worker/internal/server"
	"github.com/jalala984/master-worker/internal/store"
	"github.com/jalala984/master-worker/internal/task"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
)

// WebContent is set by main.go using go:embed from the module root.
// embed.FS can only reference paths relative to the source file's directory,
// so the embed directive must be in main.go (project root) and passed here.
var WebContent embed.FS

// DocsContent is set by main.go using go:embed for the docs/ directory.
var DocsContent embed.FS

// Master coordinates the gRPC server, HTTP server, and health checker.
type Master struct {
	grpcServer *grpc.Server
	nodeServer *server.NodeServer
	health     *health.Checker
	eventBus   *events.Bus
	store      store.TaskStore
	logger     *slog.Logger
	cfg        *config.Config
}

// NewMaster creates a master with all subsystems initialized.
func NewMaster(logger *slog.Logger, cfg *config.Config) *Master {
	healthChecker := health.NewChecker(logger)
	grpcLogger := logger.With("subsystem", "grpc")

	taskStore := store.NewMemoryStore()
	eventBus := events.NewBus()

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(interceptors.UnaryServerInterceptor(grpcLogger)),
		grpc.StreamInterceptor(interceptors.StreamServerInterceptor(grpcLogger)),
	)

	return &Master{
		grpcServer: grpcServer,
		nodeServer: server.NewNodeServer(grpcLogger, cfg, taskStore, eventBus),
		health:     healthChecker,
		eventBus:   eventBus,
		store:      taskStore,
		logger:     logger,
		cfg:        cfg,
	}
}

// Start launches the master using errgroup for coordinated goroutine lifecycle.
func (m *Master) Start() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return m.startGRPC(ctx)
	})
	g.Go(func() error {
		return m.startHTTP(ctx)
	})
	g.Go(func() error {
		m.nodeServer.StartHealthChecker(ctx)
		return nil
	})

	m.logger.Info("master starting",
		"grpc_port", m.cfg.GRPCPort,
		"http_port", m.cfg.HTTPPort,
	)

	err := g.Wait()

	if m.store != nil {
		m.store.Close()
	}

	return err
}

// startGRPC starts the gRPC server and wires graceful shutdown.
func (m *Master) startGRPC(ctx context.Context) error {
	lis, err := net.Listen("tcp", m.cfg.GRPCPort)
	if err != nil {
		return fmt.Errorf("gRPC listen on %s: %w", m.cfg.GRPCPort, err)
	}

	api.RegisterNodeServiceServer(m.grpcServer, m.nodeServer)
	m.logger.Info("gRPC server listening", "addr", m.cfg.GRPCPort)

	go func() {
		<-ctx.Done()
		m.logger.Info("shutting down gRPC server")
		m.health.SetReady(false)
		m.grpcServer.GracefulStop()
	}()

	m.health.SetReady(true)
	return m.grpcServer.Serve(lis)
}

// startHTTP starts the HTTP API server.
func (m *Master) startHTTP(ctx context.Context) error {
	mux := http.NewServeMux()

	// Health check endpoints (K8s probes).
	mux.HandleFunc("GET /healthz", m.health.LivenessHandler)
	mux.HandleFunc("GET /readyz", m.health.ReadinessHandler)

	// Prometheus metrics endpoint.
	mux.Handle("GET /metrics", promhttp.Handler())

	// Task submission endpoint.
	mux.HandleFunc("POST /tasks", m.handleTask)

	// REST API endpoints.
	mux.HandleFunc("GET /api/v1/stats", m.handleStats)
	mux.HandleFunc("GET /api/v1/workers", m.handleWorkers)
	mux.HandleFunc("GET /api/v1/tasks", m.handleTasks)
	mux.HandleFunc("GET /api/v1/dead-letter", m.handleDeadLetter)
	mux.HandleFunc("POST /api/v1/dead-letter/{id}/retry", m.handleRetryDLQ)

	// Enhanced task submission endpoints.
	mux.HandleFunc("POST /api/v1/submit", m.handleSubmit)   // Inline script submission
	mux.HandleFunc("POST /api/v1/upload", m.handleUpload)   // Archive upload

	// WebSocket endpoint for real-time event streaming.
	mux.HandleFunc("GET /api/v1/events", handleWebSocket(m.eventBus, m.logger))

	// Chaos engineering endpoints.
	mux.HandleFunc("POST /api/v1/chaos/kill-worker/{id}", m.handleChaosKillWorker)
	mux.HandleFunc("POST /api/v1/chaos/trip-cb/{id}", m.handleChaosTripCB)
	mux.HandleFunc("POST /api/v1/chaos/reset-cb/{id}", m.handleChaosResetCB)
	mux.HandleFunc("POST /api/v1/chaos/fail-tasks", m.handleChaosFailTasks)

	// Server-side batch submit for stress testing at scale.
	mux.HandleFunc("POST /api/v1/batch-submit", m.handleBatchSubmit)

	// Documentation endpoints — serves embedded markdown docs for the dashboard.
	mux.HandleFunc("GET /api/v1/docs", m.handleDocsList)
	mux.HandleFunc("GET /api/v1/docs/{name}", m.handleDocContent)

	// pprof profiling endpoints — free production diagnostics.
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)

	// Dashboard — serve embedded web/ directory.
	webFS, err := fs.Sub(WebContent, "web")
	if err != nil {
		// Fallback: if embed fails, serve JSON at root.
		m.logger.Warn("failed to load embedded web content, dashboard unavailable", "error", err)
		mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"service":"master-worker","status":"running","dashboard":"unavailable"}`)
		})
	} else {
		mux.Handle("GET /", http.FileServer(http.FS(webFS)))
	}

	// Wrap with rate limiter: 100 req/s with burst of 200.
	rl := middleware.NewRateLimiter(rate.Limit(100), 200, m.logger.With("subsystem", "rate-limiter"))

	srv := &http.Server{
		Addr:    m.cfg.HTTPPort,
		Handler: rl.Wrap(mux),
	}

	m.logger.Info("HTTP server listening", "addr", m.cfg.HTTPPort)

	go func() {
		<-ctx.Done()
		m.logger.Info("shutting down HTTP server")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), m.cfg.GracefulShutdownTimeout)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		return fmt.Errorf("HTTP server on %s: %w", m.cfg.HTTPPort, err)
	}
	return nil
}

// handleTask handles POST /tasks?dir=<path>.
func (m *Master) handleTask(w http.ResponseWriter, r *http.Request) {
	dirPath := r.URL.Query().Get("dir")
	if dirPath == "" {
		http.Error(w, `{"error":"missing 'dir' query parameter"}`, http.StatusBadRequest)
		return
	}

	files, err := os.ReadDir(dirPath)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"failed to read directory: %v"}`, err), http.StatusInternalServerError)
		return
	}

	count := 0
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".sh") {
			fullPath := filepath.Join(dirPath, file.Name())
			taskID := fmt.Sprintf("task-%d-%s", time.Now().UnixNano(), file.Name())
			m.nodeServer.EnqueueTask(taskID, fullPath)
			count++
			m.logger.Info("task enqueued", "task_id", taskID, "script", fullPath)

			// Broadcast event.
			m.eventBus.Publish(events.Event{
				Type: events.EventTaskQueued,
				Data: map[string]string{"task_id": taskID, "script": fullPath},
			})
		}
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"enqueued":%d,"directory":"%s"}`, count, dirPath)
}

// handleStats returns scheduler statistics.
func (m *Master) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := m.nodeServer.Scheduler.Stats()
	total := stats.TotalCompleted + stats.TotalFailed
	successRate := 0.0
	if total > 0 {
		successRate = math.Round(float64(stats.TotalCompleted)/float64(total)*1000) / 10
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"queue_depth":   stats.QueueDepth,
		"in_flight":     stats.InFlight,
		"retry_pending": stats.RetryPending,
		"dead_lettered": stats.DeadLettered,
		"workers":       len(m.nodeServer.GetWorkers()),
		"completed":     stats.TotalCompleted,
		"failed":        stats.TotalFailed,
		"success_rate":  successRate,
	})
}

// workerJSON is the JSON representation of a worker for the REST API.
type workerJSON struct {
	ID             string                     `json:"id"`
	Hostname       string                     `json:"hostname"`
	State          string                     `json:"state"`
	ActiveTasks    int32                      `json:"active_tasks"`
	LastHeartbeat  string                     `json:"last_heartbeat"`
	UptimeSeconds  int64                      `json:"uptime_seconds"`
	CircuitBreaker *circuitbreaker.Info        `json:"circuit_breaker,omitempty"`
}

// handleWorkers returns worker information with circuit breaker state.
func (m *Master) handleWorkers(w http.ResponseWriter, r *http.Request) {
	workers := m.nodeServer.GetWorkers()
	cbStates := m.nodeServer.CircuitBreakers.GetAll()

	now := time.Now()
	result := make([]workerJSON, 0, len(workers))
	for _, wk := range workers {
		wj := workerJSON{
			ID:            wk.ID,
			Hostname:      wk.Hostname,
			State:         wk.State.String(),
			ActiveTasks:   wk.ActiveTasks,
			LastHeartbeat: wk.LastHeartbeat.Format(time.RFC3339),
			UptimeSeconds: int64(now.Sub(wk.StartTime).Seconds()),
		}
		if info, ok := cbStates[wk.ID]; ok {
			wj.CircuitBreaker = &info
		}
		result = append(result, wj)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"workers": result})
}

// taskJSON is the JSON representation of a task for the REST API.
type taskJSON struct {
	ID          string `json:"id"`
	Script      string `json:"script"`
	State       string `json:"state"`
	WorkerID    string `json:"worker_id"`
	Attempt     int32  `json:"attempt"`
	Priority    int    `json:"priority"`
	Type        string `json:"type"`
	Language    string `json:"language"`
	Output      string `json:"output,omitempty"`
	Error       string `json:"error,omitempty"`
	ExitCode    int32  `json:"exit_code"`
	CreatedAt   string `json:"created_at,omitempty"`
	AssignedAt  string `json:"assigned_at,omitempty"`
	StartedAt   string `json:"started_at,omitempty"`
	CompletedAt string `json:"completed_at,omitempty"`
	DurationMs  int64  `json:"duration_ms"`
}

// handleTasks returns all tasks from the scheduler.
func (m *Master) handleTasks(w http.ResponseWriter, r *http.Request) {
	tasks := m.nodeServer.Scheduler.AllTasks()
	result := make([]taskJSON, 0, len(tasks))
	for _, t := range tasks {
		tj := taskJSON{
			ID:       t.ID,
			Script:   t.Script,
			State:    t.State.String(),
			WorkerID: t.WorkerID,
			Attempt:  t.Attempt,
			Priority: int(t.Priority),
			Type:     t.Type.String(),
			Language: t.Language,
			Output:   t.Output,
			Error:    t.Error,
			ExitCode: t.ExitCode,
		}
		if !t.CreatedAt.IsZero() {
			tj.CreatedAt = t.CreatedAt.Format(time.RFC3339)
		}
		if !t.AssignedAt.IsZero() {
			tj.AssignedAt = t.AssignedAt.Format(time.RFC3339)
		}
		if !t.StartedAt.IsZero() {
			tj.StartedAt = t.StartedAt.Format(time.RFC3339)
		}
		if !t.CompletedAt.IsZero() {
			tj.CompletedAt = t.CompletedAt.Format(time.RFC3339)
		}
		if d := t.Duration(); d > 0 {
			tj.DurationMs = d.Milliseconds()
		}
		result = append(result, tj)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"tasks": result})
}

// handleDeadLetter returns tasks in the dead letter queue.
func (m *Master) handleDeadLetter(w http.ResponseWriter, r *http.Request) {
	tasks := m.nodeServer.Scheduler.DeadLetterQueue()
	type dlqJSON struct {
		ID        string `json:"id"`
		Script    string `json:"script"`
		Error     string `json:"error"`
		Attempts  int32  `json:"attempts"`
		Language  string `json:"language"`
		Type      string `json:"type"`
		Output    string `json:"output,omitempty"`
		ExitCode  int32  `json:"exit_code"`
		CreatedAt string `json:"created_at,omitempty"`
	}
	result := make([]dlqJSON, 0, len(tasks))
	for _, t := range tasks {
		d := dlqJSON{
			ID:       t.ID,
			Script:   t.Script,
			Error:    t.Error,
			Attempts: t.Attempt,
			Language: t.Language,
			Type:     t.Type.String(),
			Output:   t.Output,
			ExitCode: t.ExitCode,
		}
		if !t.CreatedAt.IsZero() {
			d.CreatedAt = t.CreatedAt.Format(time.RFC3339)
		}
		result = append(result, d)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"dead_letter": result})
}

// handleRetryDLQ retries a task from the dead letter queue.
func (m *Master) handleRetryDLQ(w http.ResponseWriter, r *http.Request) {
	taskID := r.PathValue("id")
	if taskID == "" {
		http.Error(w, `{"error":"missing task id"}`, http.StatusBadRequest)
		return
	}

	if m.nodeServer.Scheduler.RetryFromDLQ(taskID) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"retried":"%s"}`, taskID)
	} else {
		http.Error(w, `{"error":"task not found in dead letter queue"}`, http.StatusNotFound)
	}
}

// submitRequest is the JSON body for POST /api/v1/submit.
type submitRequest struct {
	Language string `json:"language"` // bash, python, node
	Script   string `json:"script"`   // Script body
}

// handleSubmit accepts an inline script submission via JSON body.
// Example: curl -X POST http://localhost:9092/api/v1/submit \
//   -H "Content-Type: application/json" \
//   -d '{"language":"python","script":"print(\"hello\")"}'
func (m *Master) handleSubmit(w http.ResponseWriter, r *http.Request) {
	var req submitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"invalid JSON: %v"}`, err), http.StatusBadRequest)
		return
	}
	if req.Script == "" {
		http.Error(w, `{"error":"missing 'script' field"}`, http.StatusBadRequest)
		return
	}
	if req.Language == "" {
		req.Language = "bash"
	}

	taskID := fmt.Sprintf("inline-%d", time.Now().UnixNano())
	m.nodeServer.EnqueueInlineTask(taskID, req.Script, req.Language)
	m.logger.Info("inline task enqueued", "task_id", taskID, "language", req.Language)

	m.eventBus.Publish(events.Event{
		Type: events.EventTaskQueued,
		Data: map[string]string{"task_id": taskID, "type": "inline", "language": req.Language},
	})

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"task_id":"%s","type":"inline","language":"%s"}`, taskID, req.Language)
}

// handleUpload accepts a tar.gz archive upload via multipart form.
// Example: curl -X POST http://localhost:9092/api/v1/upload \
//   -F "archive=@myproject.tar.gz" -F "entrypoint=make test" -F "language=bash"
func (m *Master) handleUpload(w http.ResponseWriter, r *http.Request) {
	// 32 MB max upload size.
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"parse form: %v"}`, err), http.StatusBadRequest)
		return
	}

	file, _, err := r.FormFile("archive")
	if err != nil {
		http.Error(w, `{"error":"missing 'archive' file field"}`, http.StatusBadRequest)
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"read archive: %v"}`, err), http.StatusInternalServerError)
		return
	}

	entrypoint := r.FormValue("entrypoint")
	if entrypoint == "" {
		entrypoint = "./run.sh"
	}
	language := r.FormValue("language")
	if language == "" {
		language = "bash"
	}

	// Encode archive as base64 for transport over gRPC.
	encoded := base64.StdEncoding.EncodeToString(data)
	taskID := fmt.Sprintf("archive-%d", time.Now().UnixNano())
	m.nodeServer.EnqueueArchiveTask(taskID, encoded, entrypoint, language)
	m.logger.Info("archive task enqueued", "task_id", taskID, "size_bytes", len(data))

	m.eventBus.Publish(events.Event{
		Type: events.EventTaskQueued,
		Data: map[string]string{"task_id": taskID, "type": "archive", "entrypoint": entrypoint},
	})

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"task_id":"%s","type":"archive","entrypoint":"%s","size_bytes":%d}`, taskID, entrypoint, len(data))
}

// handleDocsList returns a JSON list of available documentation files.
func (m *Master) handleDocsList(w http.ResponseWriter, r *http.Request) {
	docs := []map[string]string{
		{"name": "ARCHITECTURE", "title": "Architecture", "description": "System diagrams, communication patterns, and package dependencies"},
		{"name": "DISTRIBUTED_SYSTEMS_CONCEPTS", "title": "Distributed Systems Concepts", "description": "Every feature mapped to distributed systems theory and real-world systems"},
		{"name": "INSTALLATION", "title": "Installation Guide", "description": "Setup for local, Docker Compose, and Kubernetes deployment"},
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"docs": docs})
}

// handleDocContent returns the raw markdown content of a specific doc file.
func (m *Master) handleDocContent(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		http.Error(w, `{"error":"missing doc name"}`, http.StatusBadRequest)
		return
	}

	content, err := DocsContent.ReadFile("docs/" + name + ".md")
	if err != nil {
		http.Error(w, `{"error":"document not found"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
	w.Write(content)
}

// ── Chaos Engineering Handlers ──

// handleChaosKillWorker forcefully disconnects a worker by cancelling its gRPC stream.
func (m *Master) handleChaosKillWorker(w http.ResponseWriter, r *http.Request) {
	workerID := r.PathValue("id")
	if workerID == "" {
		http.Error(w, `{"error":"missing worker id"}`, http.StatusBadRequest)
		return
	}

	if m.nodeServer.DisconnectWorker(workerID) {
		m.eventBus.Publish(events.Event{
			Type: events.EventChaosWorkerKilled,
			Data: map[string]string{"worker_id": workerID},
		})
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"killed":"%s"}`, workerID)
	} else {
		http.Error(w, `{"error":"worker not found or not connected"}`, http.StatusNotFound)
	}
}

// handleChaosTripCB forces a worker's circuit breaker into OPEN state.
func (m *Master) handleChaosTripCB(w http.ResponseWriter, r *http.Request) {
	workerID := r.PathValue("id")
	if workerID == "" {
		http.Error(w, `{"error":"missing worker id"}`, http.StatusBadRequest)
		return
	}

	cb := m.nodeServer.CircuitBreakers.Get(workerID)
	cb.Trip()
	m.eventBus.Publish(events.Event{
		Type: events.EventChaosCBTripped,
		Data: map[string]string{"worker_id": workerID},
	})

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"tripped":"%s","state":"OPEN"}`, workerID)
}

// handleChaosResetCB resets a worker's circuit breaker to CLOSED.
func (m *Master) handleChaosResetCB(w http.ResponseWriter, r *http.Request) {
	workerID := r.PathValue("id")
	if workerID == "" {
		http.Error(w, `{"error":"missing worker id"}`, http.StatusBadRequest)
		return
	}

	cb := m.nodeServer.CircuitBreakers.Get(workerID)
	cb.Reset()
	m.eventBus.Publish(events.Event{
		Type: events.EventChaosCBReset,
		Data: map[string]string{"worker_id": workerID},
	})

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"reset":"%s","state":"CLOSED"}`, workerID)
}

// handleChaosFailTasks enqueues tasks designed to fail (for testing retry/DLQ flow).
func (m *Master) handleChaosFailTasks(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Count    int    `json:"count"`
		Language string `json:"language"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
		return
	}
	if req.Count <= 0 {
		req.Count = 5
	}
	if req.Count > 100 {
		req.Count = 100
	}
	if req.Language == "" {
		req.Language = "bash"
	}

	failScripts := map[string]string{
		"bash":   "echo 'chaos: intentional failure'; exit 1",
		"python": "import sys; print('chaos: intentional failure'); sys.exit(1)",
		"node":   "console.log('chaos: intentional failure'); process.exit(1)",
	}
	script := failScripts[req.Language]
	if script == "" {
		script = failScripts["bash"]
	}

	for i := 0; i < req.Count; i++ {
		taskID := fmt.Sprintf("chaos-fail-%d-%d", time.Now().UnixNano(), i)
		m.nodeServer.EnqueueInlineTask(taskID, script, req.Language)
	}

	m.eventBus.Publish(events.Event{
		Type: events.EventBatchSubmitted,
		Data: map[string]string{"count": fmt.Sprintf("%d", req.Count), "type": "chaos-fail"},
	})

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"submitted":%d,"type":"chaos-fail"}`, req.Count)
}

// ── Batch Submit Handler ──

// batchRequest defines the JSON body for POST /api/v1/batch-submit.
type batchRequest struct {
	Count    int    `json:"count"`
	Language string `json:"language"`
	Script   string `json:"script"`
	SleepMs  int    `json:"sleep_ms"`
	Priority int    `json:"priority"`
}

// handleBatchSubmit generates tasks server-side for stress testing at scale.
func (m *Master) handleBatchSubmit(w http.ResponseWriter, r *http.Request) {
	var req batchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"invalid JSON: %v"}`, err), http.StatusBadRequest)
		return
	}

	if req.Count <= 0 {
		req.Count = 100
	}
	if req.Count > 10000 {
		req.Count = 10000
	}
	if req.Language == "" {
		req.Language = "bash"
	}

	sleepSec := "0.1"
	if req.SleepMs > 0 {
		sleepSec = fmt.Sprintf("0.%03d", req.SleepMs)
	}
	defaultScripts := map[string]string{
		"bash":   fmt.Sprintf(`echo "batch-task $(hostname) $$"; sleep %s; echo "done"`, sleepSec),
		"python": fmt.Sprintf(`import time, os; print(f"batch-task {os.getpid()}"); time.sleep(%s); print("done")`, sleepSec),
		"node":   fmt.Sprintf(`console.log("batch-task " + process.pid); setTimeout(() => console.log("done"), %d)`, req.SleepMs),
	}
	script := req.Script
	if script == "" {
		script = defaultScripts[req.Language]
		if script == "" {
			script = defaultScripts["bash"]
		}
	}

	startTime := time.Now()
	batchID := fmt.Sprintf("batch-%d", startTime.UnixNano())
	for i := 0; i < req.Count; i++ {
		taskID := fmt.Sprintf("%s-%05d", batchID, i)
		t := task.NewInline(taskID, script, req.Language)
		if req.Priority >= 0 && req.Priority <= 2 {
			t.Priority = task.Priority(req.Priority)
		}
		m.nodeServer.EnqueueInlineTaskDirect(t)
	}
	elapsed := time.Since(startTime)

	m.eventBus.Publish(events.Event{
		Type: events.EventBatchSubmitted,
		Data: map[string]string{
			"batch_id": batchID,
			"count":    fmt.Sprintf("%d", req.Count),
			"language": req.Language,
		},
	})

	m.logger.Info("batch submitted", "batch_id", batchID, "count", req.Count, "elapsed", elapsed)

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"batch_id":"%s","count":%d,"elapsed_ms":%d}`, batchID, req.Count, elapsed.Milliseconds())
}
