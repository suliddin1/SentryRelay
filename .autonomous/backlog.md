# SentryRelay Autonomous Engineering Backlog

## Phase 1: Core Foundation & Durable End-to-End Delivery Slice (Completed)
- [x] Establish Go module, dependency constraints, and directory layout
- [x] Define domain models: Event, DeliveryJob, DeliveryAttempt, Tenant, State Invariants
- [x] Design SQLite persistence layer:
  - WAL journal mode (`PRAGMA journal_mode = WAL;`)
  - Crash-safe transaction semantics (`PRAGMA synchronous = NORMAL; PRAGMA busy_timeout = 5000;`)
  - Schema migrations for tenants, events, delivery_jobs, delivery_attempts
- [x] Ingestion & Security:
  - HTTP webhook ingestion endpoint `POST /v1/ingest`
  - HMAC SHA256 signature verification (`crypto/subtle.ConstantTimeCompare`)
  - Replay attack mitigation via timestamp tolerance window
  - Idempotency key checking within transaction boundary
- [x] Durable Queue & Worker Pool:
  - Visibility timeout lease model (`PENDING` -> `IN_FLIGHT` -> `DELIVERED` / `RETRY_PENDING` / `DEAD_LETTER`)
  - Concurrency-safe atomic job leasing via SQLite transactions
  - In-flight lease recovery reaper (handles worker crashes and node restarts)
- [x] Delivery & Retry Subsystem:
  - Outbound HTTP delivery client with configurable timeouts
  - Response classification:
    - 2xx: Success (`DELIVERED`)
    - 429 & 5xx: Transient error (`RETRY_PENDING` with exponential backoff + full jitter)
    - 4xx (except 429): Permanent error (`DEAD_LETTER`)
    - Max attempts exceeded: (`DEAD_LETTER`)
  - Dead-Letter Queue (DLQ) inspection and replay capability
- [x] Verification & Tests:
  - Unit tests for HMAC, replay tolerance, backoff jitter
  - Database repository tests (CRUD, concurrent locks, schema constraints)
  - End-to-end integration tests (ingestion -> delivery -> mock destination)
  - Crash recovery tests (abandoned lease reaper)
  - Retry exhaustion & DLQ replay verification
  - Ingestion benchmark
- [x] CI/CD Workflow (`.github/workflows/ci.yml`)

## Phase 1.1: Core Correctness, Data Integrity & Persistence Hardening (Completed)
- [x] Connection pool pragma enforcement & single-writer connection configuration for SQLite (`_pragma` parameters and `SetMaxOpenConns(1)`)
- [x] Integer/epoch millisecond timestamp storage in SQLite for deterministic, bug-free time comparisons
- [x] Worker lease fencing in `RecordAttempt` to prevent stale workers from overwriting reclaimed/completed jobs
- [x] Poison pill mitigation in lease reaper: increment `attempt_count` when recovering abandoned jobs
- [x] Concurrent ingestion idempotency race handling: handle `UNIQUE` constraint collision gracefully without 500 errors
- [x] In-memory queue lease freshness validation prior to HTTP delivery
- [x] Outbound SSRF destination URL validation (reject loopback, private RFC1918, link-local metadata addresses unless permitted)
- [x] Update ADR-0002, ADR-0003, and README to reflect hardened guarantees

## Phase 2: Observability, Metrics & Telemetry (Completed)
- [x] Prometheus metrics endpoint (`/metrics`):
  - Ingestion throughput and latency histogram
  - Queue depth gauge broken down by status (`PENDING`, `IN_FLIGHT`, `RETRY_PENDING`, `DEAD_LETTER`)
  - Delivery attempt latency histogram
  - Retry distribution and DLQ transition counters
- [x] Structured JSON logging (`slog`) with contextual trace/event IDs
- [x] Refined health check and readiness probes (`/healthz`, `/readyz`)

## Phase 3: Rate Limiting & Tenant Protection
- [ ] Per-tenant ingestion rate limits (token bucket)
- [ ] Destination concurrency limits to avoid overwhelming target endpoints
- [ ] Backpressure mechanisms on ingestion when queue exceeds threshold

## Phase 4: Operational Tooling & CLI
- [ ] Implement `sentryrelay-ctl` management CLI:
  - `status`: Display system stats, queue depth, active worker count
  - `queue inspect`: View in-flight and pending jobs
  - `dlq list`: List dead-lettered events with failure diagnostics
  - `dlq replay <event_id>`: Replay dead-lettered event

## Phase 5: Fault Injection & Chaos Testing
- [ ] Fault injection framework simulating random process exits, network timeouts, and database busy locks
- [ ] Long-running stress testing under high concurrency
- [ ] Data integrity verification checks
