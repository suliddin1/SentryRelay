# Architecture Decision Records (ADRs)

## ADR-0001: Core Language and Minimal Dependency Philosophy
- **Status**: Accepted
- **Context**: SentryRelay is a high-reliability, fault-tolerant webhook ingestion and delivery engine. The system requires strong concurrency controls, deterministic resource usage, predictable garbage collection, fast startup, and native systems capabilities.
- **Decision**: Use Go as the primary implementation language. Rely on the Go standard library for HTTP handling, cryptography, concurrency primitives, and synchronization. External dependencies must be justified by clear technical necessity.
- **Consequences**: Minimal supply chain attack surface, predictable binaries, straightforward cross-platform compilation, and transparent code auditability.

## ADR-0002: Persistence Engine and SQLite WAL Configuration
- **Status**: Accepted
- **Context**: Webhooks must be durably stored before acknowledging receipt to the sender (`POST -> persist -> ACK`). The persistence engine must provide ACID transactional guarantees, survive sudden process termination, and support concurrent reads and writes without complex distributed dependencies.
- **Decision**: Use SQLite in WAL (Write-Ahead Logging) mode via `modernc.org/sqlite` (pure Go implementation, zero CGO requirement).
  - Pragmas applied at connection establishment:
    - `PRAGMA journal_mode = WAL;` (enables concurrent readers while writing)
    - `PRAGMA synchronous = NORMAL;` (durable across application crashes, optimal performance in WAL mode)
    - `PRAGMA busy_timeout = 5000;` (waits up to 5s on lock contention rather than failing immediately)
    - `PRAGMA foreign_keys = ON;` (enforces relational referential integrity)
- **Consequences**: Single embedded file persistence, zero external service dependency to operate, deterministic recovery on restart, and strict atomicity between event recording and job scheduling.

## ADR-0003: Lease-Based Visibility Timeout Queue Architecture
- **Status**: Accepted
- **Context**: In-flight webhook deliveries can fail if a worker crashes, an OS kills the process, or an unhandled panic occurs mid-delivery. Without an explicit lease model, crashed deliveries either get lost or get permanently stuck in an active state.
- **Decision**: Implement a lease-based visibility timeout state machine:
  - States: `PENDING`, `IN_FLIGHT`, `DELIVERED`, `RETRY_PENDING`, `DEAD_LETTER`.
  - Workers atomically claim a batch of `PENDING` or eligible `RETRY_PENDING` jobs by advancing them to `IN_FLIGHT` with a lease expiration timestamp (`leased_until = now + lease_duration`).
  - An asynchronous Lease Reaper scans periodically and upon system startup for jobs in `IN_FLIGHT` where `leased_until < now`, reclaiming them into `RETRY_PENDING` (or `DEAD_LETTER` if max attempts exceeded).
- **Consequences**: At-least-once delivery guarantee. Prevents orphaned jobs across arbitrary crashes without requiring distributed consensus.

## ADR-0004: Ingestion Security, Replay Defense, and Idempotency
- **Status**: Accepted
- **Context**: Webhook endpoints are publicly exposed and vulnerable to spoofing, replay attacks, and denial of service via duplicate payload transmission.
- **Decision**:
  - Ingestion authentication requires HMAC-SHA256 signature verification over the raw body and timestamp using tenant shared secrets.
  - Verification uses `crypto/subtle.ConstantTimeCompare` to avoid timing side-channels.
  - Replay protection enforces a strict timestamp tolerance window (default 5 minutes). Timestamps outside this window are rejected with `400 Bad Request`.
  - Idempotency is enforced by a unique compound constraint on `(tenant_id, idempotency_key)`. If an event was already persisted, the server returns the existing event identifier without re-queueing a duplicate delivery job.
- **Consequences**: Strict non-repudiation, tamper detection, and duplicate suppression.

## ADR-0005: Outbound Failure Classification and Backoff Jitter
- **Status**: Accepted
- **Context**: Destination webhooks fail for diverse reasons: downstream rate limits (429), temporary server crashes (500, 502, 503, 504), network timeouts, or permanent client errors (400 Bad Request, 404 Not Found, 422 Unprocessable Entity).
- **Decision**:
  - Responses with status 2xx are marked `DELIVERED`.
  - Responses with 429 or 5xx, or network connection/timeout errors, are classified as **Transient** and scheduled for retry using exponential backoff with full jitter: `sleep = rand(0, min(max_backoff, base_backoff * 2^attempt))`.
  - Responses with 4xx (excluding 429) are classified as **Permanent** failures and immediately routed to `DEAD_LETTER` (DLQ) without wasting retries.
  - Exceeding `max_attempts` transitions the job to `DEAD_LETTER`.
- **Consequences**: Protects downstream systems from thundering herds, avoids retrying malformed payloads, and isolates permanently failing jobs into the DLQ for operator triage.
