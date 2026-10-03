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
   Webhooks are durably written to SQLite in Write-Ahead Logging (`WAL`) mode within an ACID transaction before responding with `202 Accepted`. If the process crashes immediately after receiving a request, no accepted event is lost.
2. **Replay Protection & Cryptographic Non-Repudiation**:
   Every incoming webhook is verified using HMAC-SHA256 signatures evaluated with constant-time equality comparisons (`crypto/subtle.ConstantTimeCompare`). Timestamps drifting beyond a configurable tolerance window (default 5 minutes) are rejected.
3. **Idempotent Ingestion**:
   Producers supply an `X-SentryRelay-Idempotency-Key`. Duplicate events for the same tenant return the existing event identifier without enqueuing redundant delivery attempts.
4. **Crash-Resilient Lease Architecture**:
   Workers claim batches of jobs using visibility timeouts (`leased_until = now + lease_duration`). If a worker or host process terminates abruptly mid-flight, a background Lease Reaper detects expired leases on restart and returns abandoned jobs to `RETRY_PENDING` (or DLQ if maximum attempts are exceeded).
5. **Adaptive Backoff with Full Jitter**:
   Transient downstream failures (HTTP 429, 5xx, timeouts, network resets) trigger exponential backoff with full jitter to avoid thundering herd spikes against downstream endpoints.
6. **Dead-Letter Queue (DLQ) & Operator Replay**:
   Permanent client errors (e.g. HTTP 400, 401, 404, 422) or events exceeding maximum retry attempts are isolated into a Dead-Letter Queue. Operators can inspect failed payloads and replay them via REST API once downstream systems are restored.

---

## Benchmark Results

Benchmarked on an Intel Core i7-13650HX (Windows amd64, pure Go SQLite):

| Operation | Throughput | Latency | Memory Allocs |
| :--- | :--- | :--- | :--- |
| **HMAC Sign & Constant-Time Verify** | **~905,000 ops/sec** | `1,104 ns/op` | 24 allocs/op (1.39 KB) |
| **Direct SQLite WAL Transaction** | **~6,620 tx/sec** | `151 µs/op` | 103 allocs/op (4.22 KB) |
| **Full HTTP Ingestion Pipeline** | **~5,650 req/sec** | `177 µs/op` | 223 allocs/op (15.0 KB) |

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
