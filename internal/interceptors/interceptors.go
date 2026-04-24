// Package interceptors provides gRPC interceptors for cross-cutting concerns.
//
// gRPC interceptors are the equivalent of HTTP middleware — they wrap every
// RPC call to add logging, metrics, tracing, and error recovery without
// modifying business logic.
//
// This is the Chain of Responsibility pattern (GoF): each interceptor
// processes the request and optionally passes it to the next handler.
//
// Why interceptors matter for distributed systems:
//   - Consistent observability across all RPCs (no manual instrumentation)
//   - Panic recovery prevents one bad request from crashing the server
//   - Metrics enable SLO monitoring (p99 latency, error rate)
//
// Reference: gRPC interceptor documentation; Google Dapper (Sigelman et al., 2010)
// for distributed tracing motivation.
package interceptors

import (
	"context"
	"log/slog"
	"path"
	"time"

	"github.com/jalala984/master-worker/internal/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// UnaryServerInterceptor returns a gRPC unary interceptor that:
//  1. Logs every RPC call with method name and duration
//  2. Records Prometheus metrics (latency histogram, request counter)
//  3. Recovers from panics to prevent server crashes
//
// This interceptor is applied to all unary RPCs (ReportTaskStatus, SendHeartbeat).
func UnaryServerInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		start := time.Now()
		method := path.Base(info.FullMethod)

		// Panic recovery — ensures one bad request doesn't crash the server.
		// In production, panics in request handlers should be logged and
		// returned as Internal errors, not crash the entire process.
		defer func() {
			if r := recover(); r != nil {
				logger.Error("panic recovered in gRPC handler",
					"method", method,
					"panic", r,
				)
				err = status.Errorf(13, "internal error") // codes.Internal = 13
			}
		}()

		// Call the actual RPC handler.
		resp, err = handler(ctx, req)

		// Record metrics and log.
		duration := time.Since(start)
		statusCode := status.Code(err).String()

		metrics.GRPCRequestDuration.WithLabelValues(method, statusCode).Observe(duration.Seconds())
		metrics.GRPCRequestsTotal.WithLabelValues(method, statusCode).Inc()

		logger.Debug("gRPC unary call",
			"method", method,
			"status", statusCode,
			"duration", duration,
		)

		return resp, err
	}
}

// StreamServerInterceptor returns a gRPC stream interceptor that:
//  1. Logs stream open/close with duration
//  2. Records Prometheus metrics
//  3. Recovers from panics
//
// This interceptor is applied to the AssignTask server-streaming RPC.
// Stream interceptors wrap the entire stream lifecycle, not individual messages.
func StreamServerInterceptor(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) (err error) {
		start := time.Now()
		method := path.Base(info.FullMethod)

		defer func() {
			if r := recover(); r != nil {
				logger.Error("panic recovered in gRPC stream handler",
					"method", method,
					"panic", r,
				)
				err = status.Errorf(13, "internal error")
			}
		}()

		logger.Debug("gRPC stream opened", "method", method)

		err = handler(srv, ss)

		duration := time.Since(start)
		statusCode := status.Code(err).String()

		metrics.GRPCRequestDuration.WithLabelValues(method, statusCode).Observe(duration.Seconds())
		metrics.GRPCRequestsTotal.WithLabelValues(method, statusCode).Inc()

		logger.Debug("gRPC stream closed",
			"method", method,
			"status", statusCode,
			"duration", duration,
		)

		return err
	}
}
