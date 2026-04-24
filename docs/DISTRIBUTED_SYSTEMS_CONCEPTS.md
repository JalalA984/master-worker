[Installation](INSTALLATION.md) | [Architecture](ARCHITECTURE.md) | **Distributed Systems Concepts** | [Dashboard](http://localhost:9092/)

# Distributed Systems Concepts

This document maps every feature in the master-worker orchestrator to the distributed systems theory and real-world systems that inspired it.

## 1. Heartbeat-Based Failure Detection

**Implementation:** `internal/server/node_server.go` (StartHealthChecker, checkWorkerHealth)

**Theory:** In a distributed system, you cannot distinguish a slow node from a dead one (FLP impossibility). Heartbeat protocols use timeouts as a pragmatic heuristic.

**State transitions:**
- ACTIVE → SUSPECT (missed `HeartbeatTimeout`, default 30s)
- SUSPECT → ACTIVE (heartbeat received — recovery)
- SUSPECT → DEAD (missed `WorkerDeadTimeout`, default 60s)

**Real-world systems:**
- **HDFS NameNode**: DataNodes heartbeat every 3s, declared dead after 10m (Shvachko et al., MSST 2010)
- **Cassandra**: Phi Accrual Failure Detector — adaptive threshold based on heartbeat history (Hayashibara et al., 2004)
- **Kubernetes**: kubelet heartbeats to API server, node controller marks NotReady after 40s

**Trade-off:** Simple timeout-based detection is less adaptive to network jitter than Phi Accrual, but simpler to implement and debug.

## 2. At-Least-Once Task Delivery

**Implementation:** `internal/server/node_server.go` (AssignTask defer, ReportTaskStatus)

**Theory:** In a distributed system, messages can be lost. At-least-once delivery guarantees that every task will eventually be executed, possibly more than once. The alternative is at-most-once (fire-and-forget) or exactly-once (requires distributed transactions or idempotency).

**How it works:**
1. Master tracks in-flight tasks per worker
2. Worker executes and reports back (the "ack")
3. If worker disconnects before acking, master re-queues the task

**Real-world systems:**
- **MapReduce**: Map/reduce tasks re-executed on worker failure (Dean & Ghemawat, OSDI 2004)
- **Amazon SQS**: Messages become visible again if not deleted within visibility timeout
- **Kafka**: Consumer offset tracking; unacked messages are re-delivered

## 3. Push-Based Task Dispatch (Server-Streaming gRPC)

**Implementation:** `internal/server/node_server.go` (AssignTask), `api/node.proto`

**Theory:** Push vs. pull is a fundamental choice in distributed systems. Push reduces latency (no polling delay) and provides natural load balancing.

**Pattern:** Server-streaming RPC — worker opens one long-lived stream, master pushes tasks as they arrive. Whichever worker's goroutine reads first gets the task.

**Real-world systems:**
- **Kubernetes scheduler**: Watches API server (push via watch stream), assigns pods to nodes
- **gRPC load balancing**: Client-side load balancing uses streaming for service discovery updates

**Alternative patterns:**
- Pull-based (HTTP long-poll): Simpler but higher latency, used by Jenkins workers
- Work-stealing: Idle workers steal from busy workers' queues (Go runtime scheduler, Intel TBB, Cilk)

## 4. Priority Queue Scheduling

**Implementation:** `internal/task/scheduler.go` (priorityQueue via container/heap)

**Theory:** Priority scheduling ensures high-priority tasks are dispatched first. Within the same priority, FIFO ordering maintains fairness.

**Data structure:** Binary min-heap (Go's container/heap) — O(log n) enqueue/dequeue.

**Real-world systems:**
- **Linux CFS**: Completely Fair Scheduler uses a red-black tree of virtual runtimes
- **Kubernetes scheduler**: Priority classes (system-critical > default > low-priority)
- **Amazon SQS**: FIFO queues with message group IDs for ordering

## 5. Exponential Backoff with Jitter

**Implementation:** `internal/task/task.go` (ScheduleRetry)

**Formula:** `delay = base * 2^attempt + random_jitter`

**Theory:** When multiple failed tasks retry simultaneously, they can "thundering herd" the system. Exponential backoff spreads retries over time; jitter adds randomness to prevent synchronization.

**Variants:**
- Full jitter (ours): `random(0, base * 2^attempt)` — best for reducing contention
- Equal jitter: `base * 2^attempt / 2 + random(0, base * 2^attempt / 2)`
- Decorrelated jitter: `min(cap, random(base, prev_delay * 3))`

**Reference:** AWS Architecture Blog, "Exponential Backoff and Jitter" (2015)

**Real-world systems:**
- **gRPC**: Built-in retry with backoff + jitter
- **AWS SDK**: Exponential backoff for throttled API calls
- **Ethernet**: CSMA/CD uses binary exponential backoff for collision resolution

## 6. Dead Letter Queue (DLQ)

**Implementation:** `internal/task/scheduler.go` (deadLetter, RetryFromDLQ)

**Theory:** Tasks that exhaust all retry attempts are moved to a separate queue for human inspection rather than being silently dropped.

**Real-world systems:**
- **Amazon SQS**: DLQ receives messages after maxReceiveCount
- **RabbitMQ**: Dead-letter exchanges for rejected/expired messages
- **Azure Service Bus**: Dead-letter sub-queue

## 7. Circuit Breaker Pattern

**Implementation:** `internal/circuitbreaker/circuit_breaker.go`

**States:** CLOSED → OPEN → HALF_OPEN

**Theory:** The circuit breaker prevents sending requests to a component that is likely to fail, giving it time to recover. This prevents cascading failures where one unhealthy component brings down the whole system.

**Real-world systems:**
- **Netflix Hystrix**: Pioneered circuit breakers in microservices (now deprecated)
- **Envoy proxy**: Built-in circuit breaking for service mesh
- **Resilience4j**: Modern Java circuit breaker library

**Reference:** Michael Nygard, "Release It!" (2007)

## 8. Token Bucket Rate Limiting

**Implementation:** `internal/middleware/rate_limiter.go` (via golang.org/x/time/rate)

**Theory:** Controls the rate of incoming requests to prevent system overload. The token bucket allows short bursts while enforcing a long-term average rate.

**How it works:**
1. Bucket holds B tokens (burst capacity)
2. Tokens refill at rate R per second
3. Each request consumes 1 token
4. Empty bucket → request rejected (429)

**Real-world systems:**
- **Google Cloud API**: Token bucket rate limits per project
- **nginx**: `limit_req` uses leaky bucket (similar concept)
- **Linux tc**: Traffic control uses token bucket for bandwidth shaping

## 9. Embedded Key-Value Store (BadgerDB)

**Implementation:** `internal/store/badger_store.go`

**Theory:** LSM-tree (Log-Structured Merge-tree) storage provides fast writes by appending to a write-ahead log (WAL), then compacting into sorted SSTables.

**Why embedded (not Redis/etcd):**
- No separate process to manage
- Crash-safe via WAL
- Teaches LSM-tree concepts without distributed consensus overhead

**Real-world systems:**
- **LevelDB/RocksDB**: Google/Facebook's LSM-tree stores
- **CockroachDB**: Uses Pebble (Go LSM-tree, forked from LevelDB)
- **Cassandra**: LSM-tree storage engine with SSTables

## 10. Structured Concurrency (errgroup)

**Implementation:** `internal/master/master.go` (Start method)

**Theory:** Structured concurrency ensures that concurrent tasks have a clear parent-child relationship. If any child fails, all siblings are cancelled.

**Real-world systems:**
- **Kubernetes controller-manager**: Uses errgroup-like patterns for controller lifecycle
- **CockroachDB**: Uses `stopper` (their errgroup equivalent) for goroutine lifecycle
- **Python trio**: Structured concurrency library that inspired Go patterns

## 11. Repository Pattern (Store Interface)

**Implementation:** `internal/store/store.go`

**Theory:** The Repository pattern decouples business logic from storage concerns. Tests use in-memory storage; production uses durable storage.

**Real-world systems:**
- **Kubernetes**: `storage.Interface` abstracts etcd backend
- **Docker**: Image store interface with multiple backends

## 12. Graceful Shutdown

**Implementation:** Signal handling in `main.go`, `worker.go`, `master.go`

**Theory:** In containerized environments, processes receive SIGTERM before being killed (SIGKILL). Graceful shutdown ensures in-flight work completes.

**Real-world systems:**
- **Kubernetes**: Sends SIGTERM, waits `terminationGracePeriodSeconds` (default 30s), then SIGKILL
- **systemd**: Similar SIGTERM→SIGKILL lifecycle
