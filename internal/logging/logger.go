// Package logging provides structured logging using Go's stdlib slog.
//
// Why slog over zap/zerolog:
//   - Part of Go stdlib since 1.21 (production-proven in Google's internal Go services)
//   - Structured logging is essential for distributed systems debugging
//   - JSON output enables log aggregation in ELK/Loki/CloudWatch
//   - Text output for local development readability
//
// In production distributed systems, unstructured logs (fmt.Println) make it nearly
// impossible to correlate events across nodes. Structured logs with component tags,
// request IDs, and machine-parseable formats are table stakes.
//
// Reference: Go Blog "Structured Logging with slog" (https://go.dev/blog/slog)
package logging

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// NewLogger creates a component-scoped structured logger.
//
// Parameters:
//   - component: identifies which part of the system is logging (e.g., "master", "worker", "grpc")
//   - level: log verbosity — "debug", "info", "warn", "error"
//   - format: output format — "json" for production, "text" for development
//
// Every log line includes the component field, making it easy to filter
// logs by subsystem (e.g., `jq 'select(.component=="grpc")'`).
func NewLogger(component string, level string, format string) *slog.Logger {
	var handler slog.Handler
	opts := &slog.HandlerOptions{
		Level: parseLevel(level),
	}

	var w io.Writer = os.Stdout

	switch strings.ToLower(format) {
	case "json":
		handler = slog.NewJSONHandler(w, opts)
	default:
		handler = slog.NewTextHandler(w, opts)
	}

	return slog.New(handler).With("component", component)
}

// parseLevel converts a string log level to slog.Level.
// Defaults to INFO if the string is unrecognized.
func parseLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
