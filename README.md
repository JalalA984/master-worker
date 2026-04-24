# Master-Worker: Distributed Task Orchestrator

A production-grade distributed task orchestrator using master-worker architecture. A single master accepts tasks via HTTP (inline scripts, file paths, or tar.gz archives), distributes them to workers over gRPC streaming, and tracks completion with fault recovery.

## Features

**Core**
- gRPC server-streaming for push-based task dispatch (no polling)
- Heartbeat-based failure detection (HDFS model: ACTIVE → SUSPECT → DEAD)
- At-least-once task delivery with automatic re-queuing from disconnected workers
- Priority queue scheduler with retry, exponential backoff + jitter, dead letter queue

**Task Types**
- **Script Path**: Execute `.sh` files from a directory (original mode)
- **Inline Scripts**: Submit bash, Python, or Node.js scripts via JSON API
- **Archive Upload**: Upload tar.gz projects with entrypoint command (CI/CD mode)

**Reliability**
- Circuit breakers per worker (CLOSED → OPEN → HALF_OPEN pattern)
- BadgerDB persistence for crash recovery
- Token bucket rate limiting (100 req/s, burst 200)
- Graceful shutdown with in-flight task completion

**Observability**
- Prometheus metrics (17 counters/gauges/histograms)
- OpenTelemetry tracing (stdout exporter)
- Structured logging (slog, text/JSON format)
- Real-time web dashboard with WebSocket events
- pprof profiling endpoints

**Deployment**
- Single binary for master and worker roles
- Docker Compose: master + 3 workers + Prometheus + Grafana
- Kubernetes: Helm chart with PDB, HPA, NetworkPolicy, ServiceAccounts
- Kind cluster deployment with `./deploy.sh`
- mTLS certificate generation for gRPC security

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](docs/ARCHITECTURE.md) | System diagrams, gRPC patterns, fault tolerance, package dependencies |
| [Distributed Systems Concepts](docs/DISTRIBUTED_SYSTEMS_CONCEPTS.md) | Every feature mapped to theory and real-world systems |
| [Installation Guide](docs/INSTALLATION.md) | Local, Docker Compose, and Kubernetes setup |

Documentation is also accessible from the web dashboard at http://localhost:9092/ (click "Docs" in the header).

## Quick Start

### Local (2 terminals)

```bash
# Terminal 1: Start master
go run main.go master

# Terminal 2: Start worker
go run main.go worker

# Terminal 3: Submit tasks
# Script path mode
curl -X POST "http://localhost:9092/tasks?dir=$(pwd)/test_scripts"

# Inline bash
curl -X POST http://localhost:9092/api/v1/submit \
  -H "Content-Type: application/json" \
  -d '{"language":"bash","script":"echo Hello from $(hostname); uptime"}'

# Inline Python
curl -X POST http://localhost:9092/api/v1/submit \
  -H "Content-Type: application/json" \
  -d '{"language":"python","script":"import platform; print(f\"Python on {platform.node()}\")"}'

# Dashboard
open http://localhost:9092/
```

### Docker Compose

```bash
docker compose up --build

# Submit tasks
curl -X POST "http://localhost:9092/tasks?dir=/etc/scripts"
curl -X POST http://localhost:9092/api/v1/submit \
  -d '{"language":"bash","script":"echo hello from Docker"}'

# Dashboard: http://localhost:9092/
# Prometheus: http://localhost:9090/
# Grafana: http://localhost:3000/ (admin/admin)
```

### Kubernetes (Kind)

```bash
./deploy.sh                          # Build + create cluster + Helm install
kubectl port-forward svc/master-service 9092:9092
curl -X POST "http://localhost:9092/tasks?dir=/etc/scripts"
./destroy.sh                         # Tear down
```

## API Reference

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/healthz` | GET | Liveness probe |
| `/readyz` | GET | Readiness probe |
| `/metrics` | GET | Prometheus metrics |
| `/tasks?dir=<path>` | POST | Enqueue .sh files from directory |
| `/api/v1/submit` | POST | Submit inline script (JSON body) |
| `/api/v1/upload` | POST | Upload tar.gz archive (multipart) |
| `/api/v1/stats` | GET | Scheduler statistics |
| `/api/v1/workers` | GET | Worker registry |
| `/api/v1/tasks` | GET | All tasks |
| `/api/v1/dead-letter` | GET | Dead letter queue |
| `/api/v1/dead-letter/{id}/retry` | POST | Retry from DLQ |
| `/api/v1/events` | GET | WebSocket event stream |
| `/debug/pprof/` | GET | Go profiling |
| `/` | GET | Web dashboard |

### Submit Inline Script

```bash
curl -X POST http://localhost:9092/api/v1/submit \
  -H "Content-Type: application/json" \
  -d '{"language":"python","script":"print(\"distributed systems are cool\")"}'
```

### Upload Archive

```bash
tar czf myproject.tar.gz -C myproject .
curl -X POST http://localhost:9092/api/v1/upload \
  -F "archive=@myproject.tar.gz" \
  -F "entrypoint=./run.sh" \
  -F "language=bash"
```

## Architecture

```
                    ┌──────────────────────┐
                    │   HTTP Clients       │
                    │  (curl, dashboard)   │
                    └──────────┬───────────┘
                               │ REST API + WebSocket
                    ┌──────────▼───────────┐
                    │      MASTER          │
                    │  ┌────────────────┐  │
                    │  │ Priority Queue │  │
                    │  │  Scheduler     │  │
                    │  └───────┬────────┘  │
                    │  ┌───────▼────────┐  │
                    │  │  gRPC Server   │  │
                    │  │ (streaming)    │  │
                    │  └───┬───┬───┬────┘  │
                    │      │   │   │       │
                    └──────┼───┼───┼───────┘
                           │   │   │  Server-streaming RPC
              ┌────────────┘   │   └────────────┐
              ▼                ▼                 ▼
       ┌──────────┐    ┌──────────┐      ┌──────────┐
       │ WORKER 1 │    │ WORKER 2 │      │ WORKER N │
       │ bash/py/ │    │ bash/py/ │      │ bash/py/ │
       │  node    │    │  node    │      │  node    │
       └──────────┘    └──────────┘      └──────────┘
```

## Distributed Systems Concepts

| Concept | Implementation | Reference |
|---------|---------------|-----------|
| Heartbeat failure detection | ACTIVE → SUSPECT → DEAD | HDFS NameNode |
| Server-streaming RPC | Push-based task dispatch | gRPC patterns |
| Priority queue scheduling | Heap with priority + FIFO | Linux CFS, K8s scheduler |
| Exponential backoff + jitter | Retry with 2^n + random | AWS architecture blog |
| Circuit breaker | Per-worker fault isolation | Netflix Hystrix |
| Dead letter queue | Exhausted retry sink | Amazon SQS, RabbitMQ |
| At-least-once delivery | Re-queue on worker disconnect | MapReduce |
| Structured concurrency | errgroup for goroutine lifecycle | Go concurrency patterns |
| Repository pattern | TaskStore interface | K8s etcd abstraction |
| 12-Factor config | All config from env vars | Heroku methodology |

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `GRPC_PORT` | `:50051` | Master gRPC listen port |
| `HTTP_PORT` | `:9092` | Master HTTP API port |
| `MASTER_ADDR` | `localhost:50051` | Worker → master address |
| `LOG_LEVEL` | `info` | debug, info, warn, error |
| `LOG_FORMAT` | `text` | text or json |
| `HEARTBEAT_INTERVAL` | `10s` | Worker heartbeat frequency |
| `HEARTBEAT_TIMEOUT` | `30s` | Mark worker SUSPECT after |
| `WORKER_DEAD_TIMEOUT` | `60s` | Mark worker DEAD after |

## Development

```bash
make proto       # Regenerate protobuf
make build       # Build binary
make test        # Run tests (27 tests across 6 packages)
make lint        # Run linter
make docker      # Build Docker image
make loadtest    # Run load test
make chaos       # Run chaos test (requires K8s)
```

## Project Structure

```
├── api/                        # Protocol Buffers + generated gRPC code
├── cmd/loadtest/               # Load testing tool
├── charts/master-worker/       # Helm chart (master, workers, Prometheus, Grafana)
│   └── templates/
│       ├── master-deploy.yaml
│       ├── worker-deploy.yaml
│       ├── hpa.yaml            # Horizontal Pod Autoscaler
│       ├── pdb.yaml            # Pod Disruption Budget
│       ├── networkpolicy.yaml  # Network segmentation
│       ├── serviceaccount.yaml # Dedicated service accounts
│       └── servicemonitor.yaml # Prometheus Operator CRD
├── internal/
│   ├── circuitbreaker/         # Per-worker fault isolation
│   ├── config/                 # 12-Factor env config
│   ├── events/                 # Pub/sub event bus for WebSocket
│   ├── health/                 # K8s liveness + readiness probes
│   ├── interceptors/           # gRPC logging/metrics middleware
│   ├── logging/                # Structured logging (slog)
│   ├── master/                 # Master lifecycle + HTTP handlers
│   ├── metrics/                # Prometheus metrics
│   ├── middleware/             # Rate limiting
│   ├── server/                 # gRPC NodeService implementation
│   ├── store/                  # Task persistence (memory + BadgerDB)
│   ├── task/                   # Task state machine + scheduler
│   ├── tls/                    # mTLS helpers
│   ├── tracing/                # OpenTelemetry setup
│   └── worker/                 # Worker execution engine
├── monitoring/                 # Prometheus + Grafana configs
├── scripts/                    # Cert generation + chaos testing
├── test_scripts/               # Sample scripts (bash, python, node)
├── web/                        # Dashboard (embedded in binary)
├── docker-compose.yml          # Full stack local deployment
├── Dockerfile                  # Multi-stage, non-root, multi-language
├── deploy.sh                   # Kind + Helm deployment
└── main.go                     # Single binary entry point
```

## Tech Stack

Go, gRPC, Protocol Buffers, Prometheus, OpenTelemetry, BadgerDB, WebSocket, Docker, Kubernetes, Helm
