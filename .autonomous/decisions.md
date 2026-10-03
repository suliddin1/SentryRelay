# Architecture Decision Records (ADRs)

## ADR-0001: Core Language and Minimal Dependency Philosophy
- **Status**: Accepted
- **Context**: SentryRelay is a high-reliability, fault-tolerant webhook ingestion and delivery engine. The system requires strong concurrency controls, deterministic resource usage, predictable garbage collection, fast startup, and native systems capabilities.
- **Decision**: Use Go as the primary implementation language. Rely on the Go standard library for HTTP handling, cryptography, concurrency primitives, and synchronization. External dependencies must be justified by clear technical necessity.
- **Consequences**: Minimal supply chain attack surface, predictable binaries, straightforward cross-platform compilation, and transparent code auditability.

## ADR-0002: Persistence Engine, Single-Connection Pooling, and Integer Epoch Timestamps
- **Status**: Accepted (Hardened in Phase 1.1)
- **Context**: Webhooks must be durably stored before acknowledging receipt to the sender (`POST -> persist -> ACK`). The persistence engine must provide ACID transactional guarantees, survive sudden process termination, and eliminate lock contention and timestamp comparison errors.
- **Decision**: Use SQLite in WAL mode via `modernc.org/sqlite` (pure Go, zero CGO):
  - Database connection pool is configured to `SetMaxOpenConns(1)` and `SetMaxIdleConns(1)`. This ensures that all transactions run through a single serialized connection, permanently retaining applied pragmas across all queries and eliminating `database is locked (5)` errors.
  - Pragmas applied at initialization:
    - `PRAGMA journal_mode = WAL;` (write-ahead log for high concurrency)
    - `PRAGMA synchronous = NORMAL;` (crash-durable, optimal performance in WAL mode)
    - `PRAGMA busy_timeout = 5000;` (waits up to 5s on contention)
    - `PRAGMA foreign_keys = ON;` (enforces relational integrity)
  - All timestamp columns (`created_at`, `updated_at`, `next_retry_at`, `leased_at`, `leased_until`) are stored as `INTEGER` representing Unix epoch milliseconds (`int64`).
- **Consequences**: Eliminates string-based time comparison bugs (such as RFC3339 trailing zero truncation where `'Z' > '.'`), accelerates index scans, and guarantees deterministic lock-free writes.

## ADR-0003: Lease-Based Visibility Timeout, Fencing, and Poison-Pill Mitigation
- **Status**: Accepted (Hardened in Phase 1.1)
- **Context**: In-flight webhook deliveries can fail if a worker crashes, stalls, or runs past its lease. If a stale worker completes after its lease was reclaimed, it could overwrite a subsequent worker's `DELIVERED` status. Furthermore, crash-inducing payloads could trigger infinite retry loops if the reaper does not track attempts.
- **Decision**: Implement a lease-based visibility timeout state machine with optimistic fencing and attempt accounting:
  - States: `PENDING`, `IN_FLIGHT`, `DELIVERED`, `RETRY_PENDING`, `DEAD_LETTER`.
  - Workers atomically claim batches of eligible jobs by advancing them to `IN_FLIGHT` with a lease expiration timestamp (`leased_until = now + lease_duration`).
  - `RecordAttempt` enforces **Lease Fencing**: updates require `status = 'in_flight' AND leased_until = ?`. If rows affected is zero, the lease was lost or reclaimed; the attempt is logged as orphaned and the worker is forbidden from overwriting the job's modern state (`ErrLeaseLost`).
  - The Lease Reaper detects abandoned jobs (`leased_until < now`), increments `attempt_count`, logs an attempt audit record, and immediately routes the job to `DEAD_LETTER` if `attempt_count >= max_attempts`, preventing poison-pill payloads from hanging or crashing the system indefinitely.
  - In-memory dispatch queue checks lease freshness: jobs whose lease expired while waiting in the channel are dropped without executing redundant HTTP deliveries.
- **Consequences**: Prevents split-brain worker overwrites, eliminates poison-pill loops, and ensures at-least-once delivery with bounded retries.

## ADR-0004: Ingestion Security, Replay Defense, and Idempotency
- **Status**: Accepted (Hardened in Phase 1.1)
- **Context**: Webhook endpoints are publicly exposed and vulnerable to spoofing, replay attacks, and denial of service via duplicate payload transmission or concurrent duplicate races.
- **Decision**:
  - Ingestion authentication requires HMAC-SHA256 signature verification over the raw body and timestamp using tenant shared secrets.
  - Verification uses `crypto/subtle.ConstantTimeCompare` to avoid timing side-channels.
  - Replay protection enforces a strict timestamp tolerance window (default 5 minutes). Timestamps outside this window are rejected with `400 Bad Request`.
  - Idempotency is enforced by a unique compound constraint on `(tenant_id, idempotency_key)`. Concurrent duplicate ingestion requests that collide on the unique constraint are caught gracefully, returning existing event metadata with `200 OK` rather than generating 500 errors.
- **Consequences**: Strict non-repudiation, tamper detection, and race-free duplicate suppression.

## ADR-0005: Outbound Failure Classification and Backoff Jitter
- **Status**: Accepted
- **Context**: Destination webhooks fail for diverse reasons: downstream rate limits (429), temporary server crashes (500, 502, 503, 504), network timeouts, or permanent client errors (400 Bad Request, 404 Not Found, 422 Unprocessable Entity).
- **Decision**:
  - Responses with status 2xx are marked `DELIVERED`.
  - Responses with 429 or 5xx, or network connection/timeout errors, are classified as **Transient** and scheduled for retry using exponential backoff with full jitter: `sleep = rand(0, min(max_backoff, base_backoff * 2^attempt))`.
  - Responses with 4xx (excluding 429) are classified as **Permanent** failures and immediately routed to `DEAD_LETTER` (DLQ) without wasting retries.
  - Exceeding `max_attempts` transitions the job to `DEAD_LETTER`.
- **Consequences**: Protects downstream systems from thundering herds, avoids retrying malformed payloads, and isolates permanently failing jobs into the DLQ for operator triage.

## ADR-0006: Outbound Server-Side Request Forgery (SSRF) Defense
- **Status**: Accepted
- **Context**: Allowing arbitrary webhook destination URLs exposes the relay to Server-Side Request Forgery (SSRF) attacks, wherein attackers specify internal network IP addresses (RFC1918), loopback interfaces, or cloud metadata endpoints (`169.254.169.254`) to exfiltrate credentials or probe infrastructure.
- **Decision**: Implement strict destination URL validation in `internal/security/ssrf.go`:
  - Enforce `http` or `https` schemes.
  - Prohibit loopback (`127.0.0.0/8`, `::1`), private networks (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`), and link-local/cloud metadata networks (`169.254.0.0/16`, `fe80::/10`).
  - Resolve hostnames at ingestion time and verify that all resolved IPs are non-blocked public addresses.
  - Cloud metadata addresses (`169.254.169.254`) are blocked unconditionally even if local testing mode is toggled.
- **Consequences**: Hardens SentryRelay against intranet probing and cloud credential exfiltration.
