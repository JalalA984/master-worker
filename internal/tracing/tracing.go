// Package tracing provides OpenTelemetry distributed tracing setup.
//
// Distributed tracing answers: "What happened to this request as it traveled
// across services?" Each request gets a unique trace ID, and each step
// (task enqueue, assignment, execution, report) becomes a span within that trace.
//
// Why this matters:
//   - In a distributed system, a single user action may touch 5+ services
//   - Logs alone can't show the causal chain across services
//   - Traces visualize the full request lifecycle with timing
//
// We use OpenTelemetry (OTEL), the CNCF standard that unified OpenTracing
// and OpenCensus. It's the successor to Google Dapper's ideas.
//
// Local dev: stdout exporter (prints spans to console)
// K8s: OTLP exporter (sends spans to Jaeger/Tempo)
//
// Reference: "Dapper, a Large-Scale Distributed Systems Tracing Infrastructure"
// (Sigelman et al., Google, 2010)
package tracing

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Init initializes the OpenTelemetry tracer provider.
//
// Parameters:
//   - serviceName: identifies this service in traces (e.g., "master", "worker")
//   - logger: for logging initialization errors
//
// Returns a shutdown function that should be deferred by the caller.
// The shutdown function flushes any buffered spans before the process exits.
func Init(ctx context.Context, serviceName string, logger *slog.Logger) (func(context.Context) error, error) {
	// Stdout exporter: prints spans as JSON to stdout.
	// In production, replace with OTLP exporter pointing to Jaeger/Tempo.
	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return nil, err
	}

	// Resource describes what service produced the spans.
	// This is attached to every span and used for filtering in the trace UI.
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
		),
	)
	if err != nil {
		return nil, err
	}

	// TracerProvider manages span creation and export.
	// BatchSpanProcessor batches spans for efficient export (vs. SimpleSpanProcessor
	// which exports one-by-one, useful only for testing).
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	// Set as the global tracer provider so otel.Tracer() works everywhere.
	otel.SetTracerProvider(tp)

	logger.Info("tracing initialized", "service", serviceName, "exporter", "stdout")

	return tp.Shutdown, nil
}

// Tracer returns a named tracer for creating spans.
// Convention: use the package name as the tracer name.
func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}
