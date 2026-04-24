**Installation** | [Architecture](ARCHITECTURE.md) | [Distributed Systems Concepts](DISTRIBUTED_SYSTEMS_CONCEPTS.md) | [Dashboard](http://localhost:9092/)

# Installation Guide

## Prerequisites

- **Go 1.22+** (uses stdlib `net/http` method routing)
- **Docker** (for containerized deployment)
- **protoc** + Go gRPC plugins (only if modifying `.proto` files)
- **Kind** + **Helm** (for Kubernetes deployment)
- **kubectl** (for K8s interaction)

### Installing Go gRPC Plugins (only needed for proto changes)

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

## Quick Start (Local — 3 Terminals)

### Terminal 1: Start Master

```bash
go run main.go master
```

Output:
```
time=... level=INFO msg="starting master node" component=master
time=... level=INFO msg="master starting" grpc_port=:50051 http_port=:9092
time=... level=INFO msg="gRPC server listening" addr=:50051
time=... level=INFO msg="HTTP server listening" addr=:9092
time=... level=INFO msg="readiness state changed" ready=true
```

The master is now running:
- gRPC on `:50051` (workers connect here)
- HTTP on `:9092` (humans/scripts interact here)
- Dashboard at http://localhost:9092/

### Terminal 2: Start Worker(s)

```bash
go run main.go worker
```

You can start multiple workers — each gets a unique ID (`worker-<hostname>-<pid>`):
```bash
go run main.go worker &  # worker 1
go run main.go worker &  # worker 2
go run main.go worker &  # worker 3
```

### Terminal 3: Submit Tasks

```bash
curl -X POST "http://localhost:9092/tasks?dir=$(pwd)/test_scripts"
```

Output: `{"enqueued":3,"directory":"/path/to/test_scripts"}`

Tasks are distributed to connected workers. Watch the master/worker logs to see task assignment and completion.

## Verifying the System

### Health Checks

```bash
# Is the process alive?
curl http://localhost:9092/healthz
# {"status":"alive"}

# Is it ready to accept traffic?
curl http://localhost:9092/readyz
# {"status":"ready"}
```

### Prometheus Metrics

```bash
curl -s http://localhost:9092/metrics | grep mw_
```

Key metrics:
- `mw_tasks_queued_total` — total tasks submitted
- `mw_tasks_assigned_total` — total tasks dispatched to workers
- `mw_tasks_completed_total{status="completed"}` — successful tasks
- `mw_tasks_completed_total{status="failed"}` — failed tasks
- `mw_task_duration_seconds` — histogram of execution times
- `mw_workers_connected` — current number of connected workers
- `mw_task_queue_depth` — current pending tasks
- `mw_heartbeats_received_total` — heartbeat count

### REST API

```bash
# System overview (queue depth, in-flight, retrying, dead-lettered, workers)
curl http://localhost:9092/api/v1/stats

# Connected workers with state and heartbeat info
curl http://localhost:9092/api/v1/workers

# All tasks (queued, in-flight, retrying, dead-lettered)
curl http://localhost:9092/api/v1/tasks

# Dead letter queue (tasks that exhausted retries)
curl http://localhost:9092/api/v1/dead-letter

# Retry a dead-lettered task
curl -X POST http://localhost:9092/api/v1/dead-letter/<task-id>/retry
```

### Web Dashboard

Open http://localhost:9092/ in your browser. The dashboard provides a real-time view of the entire system:

- **Stats bar**: Queue depth, in-flight, completed, failed, workers, DLQ -- all updated in real-time via WebSocket
- **Workers panel**: Color-coded worker cards (green=active, yellow=suspect, red=dead) with live status
- **Submit task**: Write and execute bash, Python, or Node.js scripts directly from the browser
- **Event log**: WebSocket-powered live stream of all system events (task queued/assigned/completed/failed, worker connect/disconnect/suspect/dead)
- **Tasks table**: Click any task row to expand and view output, errors, and timing
- **Dead letter queue**: Inspect failed tasks and retry them with one click
- **Distributed Systems Concepts**: Educational cards explaining the theory behind each feature
- **Documentation**: Click "Docs" in the header to browse Architecture, Concepts, and Installation guides

The dashboard uses WebSocket for real-time updates -- when a task completes or a worker disconnects, you see it instantly without refreshing.

### Go Profiling (pprof)

```bash
# CPU profile (30 second sample)
go tool pprof http://localhost:9092/debug/pprof/profile?seconds=30

# Heap profile
go tool pprof http://localhost:9092/debug/pprof/heap

# Goroutine dump
curl http://localhost:9092/debug/pprof/goroutine?debug=2
```

## Docker Compose (Recommended for Development)

```bash
docker compose up --build
```

This starts:
- **1 master** on ports 9092 (HTTP) and 50051 (gRPC)
- **3 workers** connected to the master
- **Prometheus** on port 9090 (scraping master metrics every 5s)
- **Grafana** on port 3000 (pre-configured Prometheus datasource)

Access:
| Service | URL | Credentials |
|---------|-----|-------------|
| Dashboard | http://localhost:9092 | - |
| Prometheus | http://localhost:9090 | - |
| Grafana | http://localhost:3000 | admin / admin |

Submit tasks:
```bash
curl -X POST "http://localhost:9092/tasks?dir=/etc/scripts"
```

Stop everything:
```bash
docker compose down
```

## Kubernetes (Kind)

### Deploy

```bash
./deploy.sh
```

This script:
1. Builds the Docker image (`master-worker:v1`)
2. Creates a Kind cluster (`mw-cluster`) if not exists
3. Loads the image into Kind nodes
4. Installs/upgrades the Helm chart (master + 3 workers + Prometheus + Grafana)
5. Waits for pods to be ready

### Access Services

```bash
# Master HTTP API + Dashboard
kubectl port-forward svc/master-service 9092:9092

# Prometheus
kubectl port-forward svc/prometheus-service 9090:9090

# Grafana
kubectl port-forward svc/grafana-service 3000:3000
```

### Submit Tasks on K8s

```bash
curl -X POST "http://localhost:9092/tasks?dir=/etc/scripts"
```

The `/etc/scripts` directory is mounted from a ConfigMap containing test scripts.

### Tear Down

```bash
./destroy.sh
```

## Configuration

All configuration is via environment variables (12-Factor App pattern).

| Variable | Default | Description |
|----------|---------|-------------|
| `GRPC_PORT` | `:50051` | Master gRPC listen port |
| `HTTP_PORT` | `:9092` | Master HTTP API port |
| `MASTER_ADDR` | `localhost:50051` | Worker connection target |
| `LOG_LEVEL` | `info` | debug, info, warn, error |
| `LOG_FORMAT` | `text` | text (dev) or json (prod) |
| `TASK_QUEUE_SIZE` | `100` | Task dispatch buffer size |
| `HEARTBEAT_INTERVAL` | `10s` | Worker heartbeat frequency |
| `HEARTBEAT_TIMEOUT` | `30s` | SUSPECT threshold |
| `WORKER_DEAD_TIMEOUT` | `60s` | DEAD threshold |
| `GRACEFUL_SHUTDOWN_TIMEOUT` | `30s` | Shutdown wait time |

Example with custom config:

```bash
LOG_LEVEL=debug LOG_FORMAT=json HEARTBEAT_INTERVAL=5s go run main.go master
```

## Test Scripts

The `test_scripts/` directory contains sample bash scripts for testing:

```bash
ls test_scripts/
# 1.sh  2.sh  3.sh
```

Each script echoes its name and sleeps to simulate work.

## Makefile Targets

```bash
make help       # Show all targets
make proto      # Regenerate protobuf Go code
make build      # Build binary
make test       # Run all tests
make test-cover # Tests with coverage report
make lint       # Run golangci-lint
make docker     # Build Docker image
make deploy     # Deploy to Kind cluster
make destroy    # Tear down Kind cluster
make clean      # Remove build artifacts
```
