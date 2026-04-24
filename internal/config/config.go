// Package config provides centralized configuration from environment variables.
//
// Follows the 12-Factor App methodology (https://12factor.net/config):
// all config comes from environment variables with sensible defaults.
// This achieves strict separation of config from code and makes the app
// deployable across environments (local, Docker, K8s) without code changes.
//
// Reference: Heroku's "The Twelve-Factor App" — Factor III: Config.
package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds all configuration for the master-worker system.
// Every field has a default value suitable for local development.
// In K8s, values come from ConfigMaps/env vars in the Helm chart.
type Config struct {
	// Role is "master" or "worker", set via CLI arg (not env).
	Role string

	// GRPCPort is the port the master listens on for gRPC worker connections.
	// Default ":50051" follows gRPC convention.
	GRPCPort string

	// HTTPPort is the port the master listens on for the HTTP API and dashboard.
	HTTPPort string

	// MasterAddr is the address workers use to connect to the master's gRPC server.
	// In K8s, this is the Service DNS name (e.g., "master-service:50051").
	MasterAddr string

	// LogLevel controls log verbosity: debug, info, warn, error.
	LogLevel string

	// LogFormat controls log output format: "text" for local dev, "json" for production.
	LogFormat string

	// TaskQueueSize is the buffer size of the task dispatch channel.
	// Acts as backpressure: if the queue is full, HTTP task submissions block.
	TaskQueueSize int

	// HeartbeatInterval is how often workers send heartbeats to the master.
	// Modeled after HDFS DataNode heartbeat protocol.
	// HDFS default is 3s; we use 10s for teaching visibility in logs.
	HeartbeatInterval time.Duration

	// HeartbeatTimeout is how long the master waits before marking a worker SUSPECT.
	// In HDFS, the default is 10 minutes. We use 30s for demo responsiveness.
	// A SUSPECT worker may still recover if it sends a heartbeat.
	HeartbeatTimeout time.Duration

	// WorkerDeadTimeout is how long after SUSPECT before declaring a worker DEAD.
	// Dead workers have their in-flight tasks reassigned.
	WorkerDeadTimeout time.Duration

	// GracefulShutdownTimeout is the max time to wait for in-flight work during shutdown.
	// After this timeout, the process force-exits.
	GracefulShutdownTimeout time.Duration

	// MaxConcurrentTasks is the maximum number of tasks a single worker executes
	// at the same time. Each task spawns a subprocess (bash/python/node), so
	// running too many in parallel causes OS fork/exec pressure that makes every
	// task take far longer than its actual execution time.
	//
	// Setting this to a small multiple of available CPU cores (e.g., 16) keeps
	// the OS scheduler happy. The semaphore also creates natural back-pressure:
	// when all slots are taken the worker's stream.Recv() stalls, which causes
	// the master's gRPC stream.Send() to stall, which stops the dispatch loop
	// from pulling more tasks from the priority queue — the queue accumulates
	// tasks at a rate the system can actually process rather than flooding workers.
	MaxConcurrentTasks int
}

// Load reads configuration from environment variables with sensible defaults.
// This is the single source of truth for all configurable values.
func Load() *Config {
	return &Config{
		GRPCPort:                envOrDefault("GRPC_PORT", ":50051"),
		HTTPPort:                envOrDefault("HTTP_PORT", ":9092"),
		MasterAddr:              envOrDefault("MASTER_ADDR", "localhost:50051"),
		LogLevel:                envOrDefault("LOG_LEVEL", "info"),
		LogFormat:               envOrDefault("LOG_FORMAT", "text"),
		TaskQueueSize:           envOrDefaultInt("TASK_QUEUE_SIZE", 100),
		HeartbeatInterval:       envOrDefaultDuration("HEARTBEAT_INTERVAL", 10*time.Second),
		HeartbeatTimeout:        envOrDefaultDuration("HEARTBEAT_TIMEOUT", 30*time.Second),
		WorkerDeadTimeout:       envOrDefaultDuration("WORKER_DEAD_TIMEOUT", 60*time.Second),
		GracefulShutdownTimeout: envOrDefaultDuration("GRACEFUL_SHUTDOWN_TIMEOUT", 30*time.Second),
		MaxConcurrentTasks:      envOrDefaultInt("MAX_CONCURRENT_TASKS", 16),
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envOrDefaultInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func envOrDefaultDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
