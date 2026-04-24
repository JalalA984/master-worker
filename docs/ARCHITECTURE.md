[Installation](INSTALLATION.md) | **Architecture** | [Distributed Systems Concepts](DISTRIBUTED_SYSTEMS_CONCEPTS.md) | [Dashboard](http://localhost:9092/)

# Architecture

## System Overview

```
┌─────────────────────────────────────────────────────────┐
│                    HTTP Clients                         │
│              (curl, dashboard, scripts)                 │
└────────────────────────┬────────────────────────────────┘
                         │ HTTP :9092
                         ▼
┌─────────────────────────────────────────────────────────┐
│                      MASTER                             │
│                                                         │
│  ┌──────────┐  ┌───────────┐  ┌──────────────────────┐ │
│  │ HTTP API │  │  Task     │  │   gRPC Server        │ │
│  │          │  │  Queue    │  │   :50051             │ │
│  │ /tasks   │─▶│ (chan)    │─▶│                      │ │
│  │ /healthz │  │           │  │ AssignTask (stream)  │ │
│  │ /readyz  │  └───────────┘  │ ReportTaskStatus     │ │
│  │ /metrics │                 │ SendHeartbeat        │ │
│  └──────────┘                 └──────┬───────────────┘ │
│                                      │                  │
│  ┌──────────────┐  ┌──────────────┐  │                  │
│  │Worker Registry│  │Active Tasks  │  │                  │
│  │(heartbeat    │  │(in-flight    │  │                  │
│  │ tracking)    │  │ tracking)    │  │                  │
│  └──────────────┘  └──────────────┘  │                  │
└──────────────────────────────────────┼──────────────────┘
                                       │ gRPC streams
                    ┌──────────────────┼──────────────────┐
                    │                  │                   │
                    ▼                  ▼                   ▼
             ┌──────────┐      ┌──────────┐        ┌──────────┐
             │ Worker 1 │      │ Worker 2 │        │ Worker N │
             │          │      │          │        │          │
             │ Execute  │      │ Execute  │        │ Execute  │
             │ /bin/bash│      │ /bin/bash│        │ /bin/bash│
             │          │      │          │        │          │
             │ Heartbeat│      │ Heartbeat│        │ Heartbeat│
             │ Loop     │      │ Loop     │        │ Loop     │
             └──────────┘      └──────────┘        └──────────┘
```

## Task Lifecycle

```
    HTTP POST /tasks
         │
         ▼
    ┌─────────┐     ┌──────────┐     ┌─────────┐     ┌───────────┐
    │ QUEUED  │────▶│ ASSIGNED │────▶│ RUNNING │────▶│ COMPLETED │
    └─────────┘     └──────────┘     └─────────┘     └───────────┘
                                          │
                                          │ failure
                                          ▼
                                     ┌─────────┐     ┌──────────┐
                                     │ FAILED  │────▶│ RETRYING │
                                     └─────────┘     └──────────┘
                                                          │
                                                          │ max retries
                                                          ▼
                                                     ┌─────────┐
                                                     │  DEAD   │
                                                     │ (DLQ)   │
                                                     └─────────┘
```

## Worker State Machine (Heartbeat-Based Failure Detection)

```
    ┌────────┐  heartbeat received   ┌─────────┐
    │ ACTIVE │◀─────────────────────│ SUSPECT │
    └────┬───┘                       └────┬────┘
         │                                │
         │ missed HeartbeatTimeout        │ missed WorkerDeadTimeout
         │                                │
         ▼                                ▼
    ┌─────────┐                      ┌────────┐
    │ SUSPECT │                      │  DEAD  │
    └─────────┘                      └────────┘

    ┌────────┐  graceful shutdown
    │ ACTIVE │──────────────────▶┌──────────┐
    └────────┘                   │ DRAINING │
                                 └──────────┘
```

## gRPC Communication Pattern

```
    Worker                              Master
      │                                   │
      │──── AssignTask(Registration) ────▶│  Server-streaming RPC
      │                                   │  (long-lived connection)
      │◀─── TaskAssignment ──────────────│  Master pushes tasks
      │◀─── TaskAssignment ──────────────│  as they arrive
      │                                   │
      │──── ReportTaskStatus(Report) ───▶│  Unary RPC
      │◀─── TaskReportResponse ─────────│  (one per task)
      │                                   │
      │──── SendHeartbeat(Heartbeat) ───▶│  Unary RPC
      │◀─── HeartbeatResponse ──────────│  (every 10s)
      │                                   │
```

## Observability Stack

```
    ┌──────────┐  scrape /metrics  ┌────────────┐  query  ┌─────────┐
    │  Master  │◀──────────────────│ Prometheus │◀────────│ Grafana │
    │  :9092   │                   │  :9090     │         │  :3000  │
    └──────────┘                   └────────────┘         └─────────┘

    Metrics exposed:
    - mw_tasks_queued_total          (counter)
    - mw_tasks_assigned_total        (counter)
    - mw_tasks_completed_total       (counter, by status)
    - mw_task_duration_seconds       (histogram)
    - mw_task_queue_depth            (gauge)
    - mw_workers_connected           (gauge)
    - mw_heartbeats_received_total   (counter)
    - mw_grpc_request_duration_seconds (histogram, by method)
    - mw_grpc_requests_total         (counter, by method+status)
```

## Package Dependency Graph

```
    main.go
    ├── internal/config      (no deps)
    ├── internal/logging     (no deps)
    ├── internal/master
    │   ├── internal/config
    │   ├── internal/health
    │   ├── internal/interceptors
    │   │   └── internal/metrics
    │   ├── internal/metrics
    │   └── internal/server
    │       ├── internal/config
    │       └── internal/metrics
    └── internal/worker
        └── internal/config
```

## Fault Tolerance

1. **Worker crash**: Master detects via heartbeat timeout → marks SUSPECT → DEAD → re-queues in-flight tasks
2. **Stream disconnect**: Deferred cleanup in `AssignTask` re-queues all tasks from that worker's local in-flight map
3. **Task failure**: Worker reports FAILED state → master logs (Sprint 3 adds retry with backoff)
4. **Master crash**: Stateless workers reconnect automatically. Sprint 3 adds BadgerDB persistence for task state recovery.

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| Single binary, role-based | Simpler deployment (etcd, CockroachDB pattern) |
| Push-based task dispatch | No polling overhead, natural load balancing via channel |
| Buffered channel as queue | Simple backpressure; replaced by priority queue in Sprint 3 |
| slog over zap | Go stdlib (1.21+), no external dependency |
| errgroup for lifecycle | Structured concurrency with first-error semantics |
| Heartbeat failure detection | HDFS-proven model, simple timeout-based |
