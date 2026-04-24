// Package worker implements the worker node that connects to the master,
// receives task assignments via gRPC streaming, and executes tasks.
//
// Supports 3 execution modes (heterogeneous task execution, like Google Borg):
//   - SCRIPT_PATH: execute a bash script at a filesystem path (original mode)
//   - INLINE: execute an inline script body in bash, python, or node
//   - ARCHIVE: extract a tar.gz project archive and run an entrypoint command
//
// Key distributed systems patterns:
//   - Heartbeat protocol: worker proves liveness to master (HDFS model)
//   - Graceful shutdown: finish in-flight tasks before exiting (SIGTERM handling)
//   - At-least-once delivery: worker reports completion; if it crashes before
//     reporting, the master re-queues the task for another worker
//
// The worker is designed to be stateless — all task state lives on the master.
// This simplifies scaling: just add more worker replicas in K8s.
package worker

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jalala984/master-worker/api"
	"github.com/jalala984/master-worker/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// interpreters maps language names to their interpreter binary.
// Workers must have these installed (handled in Dockerfile).
var interpreters = map[string]string{
	"bash":   "/bin/bash",
	"sh":     "/bin/sh",
	"python": "python3",
	"node":   "node",
}

// Worker connects to the master and executes assigned tasks.
type Worker struct {
	client   api.NodeServiceClient
	conn     *grpc.ClientConn
	id       string
	hostname string
	logger   *slog.Logger
	cfg      *config.Config

	// activeTasks tracks the number of currently executing tasks.
	// Used in heartbeat reporting so the master knows worker load.
	// atomic.Int32 is safe for concurrent access from task goroutines.
	activeTasks atomic.Int32

	// sem is a counting semaphore that caps concurrent subprocess execution.
	// Each slot corresponds to one running task (bash/python/node process).
	// When all slots are occupied the task-receive loop blocks, which stalls
	// stream.Recv(), which stalls the master's stream.Send() via gRPC flow
	// control — creating natural back-pressure all the way to the scheduler.
	sem chan struct{}
}

// NewWorker creates a worker and establishes a gRPC connection to the master.
func NewWorker(target string, logger *slog.Logger, cfg *config.Config) (*Worker, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("gRPC connect to %s: %w", target, err)
	}

	hostname, _ := os.Hostname()
	workerID := fmt.Sprintf("worker-%s-%d", hostname, os.Getpid())

	maxConcurrent := cfg.MaxConcurrentTasks
	if maxConcurrent <= 0 {
		maxConcurrent = 16
	}

	return &Worker{
		client:   api.NewNodeServiceClient(conn),
		conn:     conn,
		id:       workerID,
		hostname: hostname,
		logger:   logger,
		cfg:      cfg,
		sem:      make(chan struct{}, maxConcurrent),
	}, nil
}

// Start begins the worker's main loop: connect to master, receive tasks, execute them.
func (w *Worker) Start() error {
	w.logger.Info("connecting to master",
		"master_addr", w.cfg.MasterAddr,
		"worker_id", w.id,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := &api.WorkerRegistration{
		WorkerId:  w.id,
		Hostname:  w.hostname,
		StartTime: timestamppb.Now(),
	}
	stream, err := w.client.AssignTask(ctx, reg)
	if err != nil {
		return fmt.Errorf("failed to open task stream: %w", err)
	}

	w.logger.Info("connected to master, waiting for tasks")

	go w.heartbeatLoop(ctx)

	taskChan := make(chan *api.TaskAssignment)
	errChan := make(chan error, 1)
	var wg sync.WaitGroup

	go func() {
		for {
			assignment, err := stream.Recv()
			if err != nil {
				errChan <- err
				return
			}
			taskChan <- assignment
		}
	}()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("shutdown signal received, waiting for active tasks to finish")
			wg.Wait()
			w.logger.Info("graceful shutdown complete")
			return nil

		case err := <-errChan:
			if err == io.EOF {
				w.logger.Info("task stream closed by master")
				return nil
			}
			return fmt.Errorf("task stream error: %w", err)

		case assignment := <-taskChan:
			// Acquire a concurrency slot before spawning.
			// If all slots are full this blocks here, which stalls
			// the taskChan send, which stalls stream.Recv(), which
			// lets gRPC flow-control stall the master's stream.Send().
			// The master's dispatch loop then stops dequeuing, so the
			// priority queue accumulates work at a sustainable pace.
			select {
			case w.sem <- struct{}{}:
			case <-ctx.Done():
				wg.Wait()
				return nil
			}
			wg.Add(1)
			w.activeTasks.Add(1)
			go func(task *api.TaskAssignment) {
				defer wg.Done()
				defer w.activeTasks.Add(-1)
				defer func() { <-w.sem }()
				w.executeTask(task)
			}(assignment)
		}
	}
}

// executeTask dispatches to the appropriate handler based on TaskType.
func (w *Worker) executeTask(task *api.TaskAssignment) {
	w.logger.Info("executing task",
		"task_id", task.TaskId,
		"type", task.TaskType.String(),
		"language", task.Language,
		"attempt", task.Attempt,
	)

	startedAt := time.Now()
	var output []byte
	var execErr error

	switch task.TaskType {
	case api.TaskType_INLINE:
		output, execErr = w.executeInline(task)
	case api.TaskType_ARCHIVE:
		output, execErr = w.executeArchive(task)
	default:
		// SCRIPT_PATH (default/zero value) — original behavior.
		output, execErr = w.executeScriptPath(task)
	}

	completedAt := time.Now()
	duration := completedAt.Sub(startedAt)

	report := &api.TaskReport{
		TaskId:      task.TaskId,
		WorkerId:    w.id,
		Output:      string(output),
		StartedAt:   timestamppb.New(startedAt),
		CompletedAt: timestamppb.New(completedAt),
	}

	if execErr != nil {
		report.State = api.TaskState_TASK_FAILED
		report.ErrorMessage = execErr.Error()
		if exitErr, ok := execErr.(*exec.ExitError); ok {
			report.ExitCode = int32(exitErr.ExitCode())
		} else {
			report.ExitCode = -1
		}
		w.logger.Warn("task failed",
			"task_id", task.TaskId,
			"type", task.TaskType.String(),
			"error", execErr,
			"duration", duration,
		)
	} else {
		report.State = api.TaskState_TASK_COMPLETED
		report.ExitCode = 0
		w.logger.Info("task completed",
			"task_id", task.TaskId,
			"type", task.TaskType.String(),
			"duration", duration,
		)
	}

	_, reportErr := w.client.ReportTaskStatus(context.Background(), report)
	if reportErr != nil {
		w.logger.Error("failed to report task status",
			"task_id", task.TaskId, "error", reportErr)
	}
}

// executeScriptPath runs a bash script from a filesystem path (original mode).
func (w *Worker) executeScriptPath(task *api.TaskAssignment) ([]byte, error) {
	cmd := exec.Command("/bin/bash", task.Script)
	return cmd.CombinedOutput()
}

// executeInline writes the script content to a temp file and executes it
// with the appropriate interpreter (bash, python, node).
func (w *Worker) executeInline(task *api.TaskAssignment) ([]byte, error) {
	lang := task.Language
	if lang == "" {
		lang = "bash"
	}

	interpreter, ok := interpreters[lang]
	if !ok {
		return nil, fmt.Errorf("unsupported language: %s (supported: bash, python, node)", lang)
	}

	// Determine file extension for the temp file.
	ext := ".sh"
	switch lang {
	case "python":
		ext = ".py"
	case "node":
		ext = ".js"
	}

	// Write script to a temp file.
	tmpFile, err := os.CreateTemp("", "mw-inline-*"+ext)
	if err != nil {
		return nil, fmt.Errorf("create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(task.Content); err != nil {
		tmpFile.Close()
		return nil, fmt.Errorf("write script: %w", err)
	}
	tmpFile.Close()

	if err := os.Chmod(tmpFile.Name(), 0755); err != nil {
		return nil, fmt.Errorf("chmod temp file: %w", err)
	}

	cmd := exec.Command(interpreter, tmpFile.Name())
	return cmd.CombinedOutput()
}

// executeArchive decodes a base64 tar.gz archive, extracts it to a temp
// directory, and runs the entrypoint command inside it.
// This simulates CI/CD pipeline behavior (like GitHub Actions or Tekton).
func (w *Worker) executeArchive(task *api.TaskAssignment) ([]byte, error) {
	// Decode base64 archive content.
	archiveData, err := base64.StdEncoding.DecodeString(task.Content)
	if err != nil {
		return nil, fmt.Errorf("decode base64 archive: %w", err)
	}

	// Create temp directory for extraction.
	tmpDir, err := os.MkdirTemp("", "mw-archive-*")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	// Write archive to temp file.
	archivePath := filepath.Join(tmpDir, "archive.tar.gz")
	if err := os.WriteFile(archivePath, archiveData, 0644); err != nil {
		return nil, fmt.Errorf("write archive: %w", err)
	}

	// Extract using tar.
	extractCmd := exec.Command("tar", "xzf", archivePath, "-C", tmpDir)
	if extractOut, err := extractCmd.CombinedOutput(); err != nil {
		return extractOut, fmt.Errorf("extract archive: %w", err)
	}

	// Determine the entrypoint command.
	entrypoint := task.Entrypoint
	if entrypoint == "" {
		entrypoint = "./run.sh"
	}

	// Determine interpreter for the entrypoint.
	lang := task.Language
	if lang == "" {
		lang = "bash"
	}
	interpreter, ok := interpreters[lang]
	if !ok {
		interpreter = "/bin/bash"
	}

	// Run the entrypoint inside the extracted directory.
	cmd := exec.Command(interpreter, "-c", entrypoint)
	cmd.Dir = tmpDir
	return cmd.CombinedOutput()
}

// heartbeatLoop sends periodic heartbeats to the master.
func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			hb := &api.Heartbeat{
				WorkerId:    w.id,
				ActiveTasks: w.activeTasks.Load(),
				Timestamp:   timestamppb.Now(),
			}
			_, err := w.client.SendHeartbeat(ctx, hb)
			if err != nil {
				w.logger.Warn("heartbeat failed", "error", err)
			}
		}
	}
}

// Close closes the gRPC connection to the master.
func (w *Worker) Close() {
	w.conn.Close()
}
