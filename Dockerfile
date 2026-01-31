# STAGE 1: Build the binary
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy go.mod and sum first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Build the app (CGO_ENABLED=0 ensures a static binary for Alpine)
RUN CGO_ENABLED=0 GOOS=linux go build -o main .

# STAGE 2: Final small image
FROM alpine:latest

# Install bash because workers use it to run scripts
RUN apk add --no-cache bash

WORKDIR /root/

# Copy the binary from the builder stage
COPY --from=builder /app/main .

# Don't use ENTRYPOINT here because main.go needs 
# arguments (master or worker) which will be provided at runtime.
CMD ["./main"]