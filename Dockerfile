# STAGE 1: Build the Go binary
FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Static binary for scratch/alpine (no CGO needed)
RUN CGO_ENABLED=0 GOOS=linux go build -o main .

# STAGE 2: Production runtime
# Multi-language support: bash + python3 + node for heterogeneous task execution.
FROM alpine:latest

# Install runtimes for multi-language task execution + tar for archive extraction.
RUN apk add --no-cache bash python3 nodejs tar && \
    addgroup -S appgroup && \
    adduser -S appuser -G appgroup

WORKDIR /home/appuser

COPY --from=builder /app/main .
RUN chown appuser:appgroup main

# Run as non-root (security hardening — principle of least privilege).
USER appuser

CMD ["./main"]
