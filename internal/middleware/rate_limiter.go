// Package middleware provides HTTP middleware for the master's API server.
//
// Rate limiting prevents a single client from overwhelming the system.
// We use the token bucket algorithm via golang.org/x/time/rate:
//
//   - Bucket starts full with B tokens (burst)
//   - Each request consumes 1 token
//   - Tokens are refilled at rate R per second
//   - If bucket is empty, request is rejected (429 Too Many Requests)
//
// Token bucket is the most common rate limiting algorithm in production:
//   - Google API rate limits use token bucket
//   - AWS API Gateway uses token bucket
//   - nginx rate limiting module uses leaky bucket (similar)
//
// Reference: "Token Bucket" algorithm, used in network traffic shaping
// since ATM networking era (1990s).
package middleware

import (
	"log/slog"
	"net/http"

	"golang.org/x/time/rate"
)

// RateLimiter wraps an HTTP handler with token bucket rate limiting.
type RateLimiter struct {
	limiter *rate.Limiter
	logger  *slog.Logger
}

// NewRateLimiter creates a rate limiter that allows r requests per second
// with a burst capacity of b.
func NewRateLimiter(r rate.Limit, b int, logger *slog.Logger) *RateLimiter {
	return &RateLimiter{
		limiter: rate.NewLimiter(r, b),
		logger:  logger,
	}
}

// Wrap returns an HTTP middleware that enforces rate limits.
func (rl *RateLimiter) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !rl.limiter.Allow() {
			rl.logger.Warn("rate limit exceeded",
				"path", r.URL.Path,
				"method", r.Method,
				"remote", r.RemoteAddr,
			)
			http.Error(w, `{"error":"rate limit exceeded"}`, http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}
