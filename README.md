# SentryRelay

[![CI](https://github.com/suliddin1/SentryRelay/actions/workflows/ci.yml/badge.svg)](https://github.com/suliddin1/SentryRelay/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/suliddin1/SentryRelay)](https://goreportcard.com/report/github.com/suliddin1/SentryRelay)

SentryRelay is a production-oriented, fault-tolerant webhook delivery and ingestion engine written in Go. It guarantees durable event persistence, cryptographic tamper-resistance, crash recovery, lease-based queue dispatching, and exponential backoff retry semantics with automated Dead-Letter Queue (DLQ) routing.

---

## Architecture & Lifecycle

```mermaid
flowchart TD
    Producer["Webhook Producer"] -->|"POST /v1/ingest (HMAC + Timestamp)"| Ingest["Ingestion HTTP Server"]
    Ingest -->|"Verify HMAC & Replay Window"| AuthCheck{"Authentic & Fresh?"}
    AuthCheck -->|No| Reject["401 Unauthorized / 400 Bad Request"]
    AuthCheck -->|Yes| IdempCheck{"(Tenant, IdempotencyKey) Exists?"}
    IdempCheck -->|Duplicate| AckDup["200 OK (Existing Event)"]
    IdempCheck -->|New| WALTx["Atomic SQLite Transaction (WAL Mode)"]
    WALTx -->|"Persist Event & PENDING Job"| AckNew["202 Accepted"]
    
    subgraph QueueEngine["Durable Queue & Worker Pool"]
        Dispatcher["Dispatcher Loop"] -->|"ClaimJobs (Atomic Lease Update)"| InFlight["Job marked IN_FLIGHT"]
        InFlight --> Worker["Worker Goroutines"]
        Reaper["Stale Lease Reaper"] -.->|"Reclaim expired leases (Crash Recovery)"| InFlight
    end

    Worker -->|"HTTP POST (Timeout & Headers)"| Destination["Destination Webhook Server"]
    Destination -->|"Classify HTTP Response"| Classify{"Response Classification"}
    
    Classify -->|2xx Success| Delivered["DELIVERED (Terminal Success)"]
    Classify -->|4xx Permanent (Except 429)| DLQ["DEAD_LETTER (DLQ)"]
    Classify -->|5xx or 429 or Timeout| RetryDecision{"Attempts < MaxAttempts?"}
    RetryDecision -->|Yes| Backoff["RETRY_PENDING (Exponential Backoff + Full Jitter)"]
    RetryDecision -->|No| DLQ
    
    DLQ -.->|"POST /v1/dlq/:id/replay"| Replay["Reset to PENDING"]
    Replay -.-> Dispatcher
```

---

## Key Guarantees & Invariants

1. **Durable Persistence Before Acknowledgment**:
   Webhooks are durably written to SQLite in Write-Ahead Logging (`WAL`) mode within an ACID transaction before responding with `202 Accepted`. Connection pooling is configured to `SetMaxOpenConns(1)` with `synchronous = NORMAL` and `busy_timeout = 5000` to prevent lock contention deadlocks and ensure pragmas remain permanently active across all operations.
2. **Deterministic Integer Epoch Timestamps**:
   All timestamps (`created_at`, `updated_at`, `next_retry_at`, `leased_until`) are stored as 64-bit signed integers representing Unix epoch milliseconds (`int64`), preventing string-based comparison edge cases (such as RFC3339 trailing zero truncation where `'Z' > '.'`).
3. **Replay Protection & Cryptographic Non-Repudiation**:
   Every incoming webhook is verified using HMAC-SHA256 signatures evaluated with constant-time equality comparisons (`crypto/subtle.ConstantTimeCompare`). Timestamps drifting beyond a configurable tolerance window (default 5 minutes) are rejected.
4. **At-Least-Once Delivery & Idempotent Ingestion**:
   SentryRelay provides an **At-Least-Once delivery guarantee**. If a destination successfully processes a webhook but the HTTP ACK (e.g., 200 OK) is lost due to a network reset or timeout, SentryRelay will safely retry the delivery. Producers supply an X-SentryRelay-Idempotency-Key for safe concurrent duplicate ingestion races, and consumers MUST also implement idempotency keys to handle retry duplicates safely.
   Producers supply an `X-SentryRelay-Idempotency-Key`. Ingestion handles concurrent duplicate submission races safely, returning the existing event identifier without enqueuing redundant delivery attempts or failing with 500 errors.
5. **Worker Lease Fencing & Poison-Pill Mitigation**:
   Workers claim batches of jobs using visibility timeouts (`leased_until = now + lease_duration`). Delivery completion checks lease fencing: a worker whose lease expired cannot overwrite newer job states (`ErrLeaseLost`). The background Lease Reaper reclaims abandoned jobs, increments `attempt_count`, and routes poison-pill payloads to `DEAD_LETTER` once `max_attempts` is reached. Stale in-memory jobs are dropped before outbound dispatch.
6. **Outbound SSRF Defense**:
   All webhook destinations are checked against loopback, RFC1918 private networks, and cloud metadata endpoints (`169.254.169.254`) with DNS resolution validation, protecting internal networks from unauthorized access.
7. **Adaptive Backoff with Full Jitter & DLQ Replay**:
   Transient downstream failures (HTTP 429, 5xx, timeouts, network resets) trigger exponential backoff with full jitter to avoid thundering herd spikes. Permanent client errors (e.g. HTTP 400, 401, 404, 422) or events exceeding maximum retry attempts are isolated into a Dead-Letter Queue (DLQ) with REST replay capability (`POST /v1/dlq/:id/replay`).

---

## Benchmark Results

Benchmarked on an Intel Core i7-13650HX (Windows amd64, pure Go SQLite):

| Operation | Throughput | Latency | Memory Allocs |
| :--- | :--- | :--- | :--- |
| **HMAC Sign & Constant-Time Verify** | **~335,000 ops/sec** | `2,983 ns/op` | 24 allocs/op (1.39 KB) |
| **Direct SQLite WAL Transaction** | **~1,760 tx/sec** | `565 us/op` | 96 allocs/op (3.95 KB) |
| **Full HTTP Ingestion Pipeline** | **~780 req/sec** | `1,283 us/op` | 239 allocs/op (16.0 KB) |

*(Note: Benchmark includes overhead of strict SetMaxOpenConns(1) locks, background lease reaper execution, metric aggregations, and synchronous disk writes).*

---

## Observability & Operations

SentryRelay provides a comprehensive suite of tools for operators:

1. **Prometheus Metrics**: `GET /metrics` exposes full Prometheus telemetry including HTTP latencies (histograms), active queue depths grouped by state, and retry counters.
2. **Structured Logging**: Fully integrated with `log/slog` for structured JSON logs, tracing request paths with `trace_id` correlation.
3. **Headless CLI (`sentryrelay-ctl`)**: The included management CLI allows operators to locally triage queues without SQL access.
   - `sentryrelay-ctl status`
   - `sentryrelay-ctl queue inspect`
   - `sentryrelay-ctl dlq list`
   - `sentryrelay-ctl dlq replay <id>`

## Rate Limiting & Protection

- **Tenant Rate Limits**: Token bucket (via `golang.org/x/time/rate`) strictly enforcing per-tenant ingestion limits.
- **Destination Concurrency Limits**: Channel-based semaphores per destination host prevent overwhelming downstream Webhook targets.
- **Queue Backpressure**: Atomic checks against the active queue depth reject incoming traffic (`503 Service Unavailable`) automatically when maximum capacity is reached.

---

## API Reference

### 1. Webhook Ingestion
`POST /v1/ingest`

**Headers:**
- `X-SentryRelay-Tenant-ID`: Identifier for the publishing tenant.
- `X-SentryRelay-Signature`: HMAC-SHA256 hex digest of `{timestamp}.{payload}`.
- `X-SentryRelay-Timestamp`: Unix epoch timestamp in seconds.
- `X-SentryRelay-Idempotency-Key`: Unique deduplication key for this event.
- `X-SentryRelay-Destination-URL`: (Optional if included in body envelope) Target HTTP endpoint.
- `X-Forward-*`: (Optional) Any custom headers prefixed with `X-Forward-` will be stripped of the prefix and forwarded to the destination.

**Response (New Event):** `202 Accepted`
```json
{
  "event_id": "8bb3858c-a813-4cf2-8cb2-20c2dca1c00c",
  "job_id": "c9be1f60-d66a-49c9-a5aa-cbf7c3a07ef4",
  "status": "accepted"
}
```

**Response (Duplicate Key):** `200 OK`
```json
{
  "event_id": "8bb3858c-a813-4cf2-8cb2-20c2dca1c00c",
  "job_id": "c9be1f60-d66a-49c9-a5aa-cbf7c3a07ef4",
  "status": "duplicate"
}
```

### 2. Operational Diagnostics
- `GET /healthz`: Liveness check. Returns `200 OK {"status":"ok"}`.
- `GET /readyz`: Readiness check. Verifies SQLite connectivity and responsiveness.
- `GET /v1/jobs/{id}`: Inspects delivery state, attempt count, and last error diagnostics for a job.
- `GET /v1/dlq?limit=50&offset=0`: Lists dead-lettered jobs for operator triage.
- `POST /v1/dlq/{id}/replay`: Resets a dead-lettered job to `PENDING` for redelivery.

---

## Building & Testing

### Prerequisites
- Go 1.24+ (pure Go SQLite, no C compiler required)

### Run Tests
```bash
go test -v ./...
```

### Run Benchmarks
```bash
go test -bench . -benchmem ./test/benchmark
```

### Build & Run
```bash
go build -o bin/sentryrelay ./cmd/sentryrelay
./bin/sentryrelay -port 8080 -db sentryrelay.db -workers 5 -seed-dev-tenant
```

---

## License

This project is licensed under the MIT License.
