package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"

	"github.com/suliddin1/SentryRelay/internal/model"
)

const schema = `
CREATE TABLE IF NOT EXISTS tenants (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    secret TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    idempotency_key TEXT NOT NULL,
    destination_url TEXT NOT NULL,
    payload BLOB NOT NULL,
    headers TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE(tenant_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS delivery_jobs (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    next_retry_at INTEGER NOT NULL,
    leased_at INTEGER,
    leased_until INTEGER,
    last_error_code TEXT,
    last_error_message TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_poll ON delivery_jobs(status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_jobs_lease ON delivery_jobs(status, leased_until);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES delivery_jobs(id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL,
    status_code INTEGER NOT NULL,
    execution_duration_ms INTEGER NOT NULL,
    error_message TEXT,
    created_at INTEGER NOT NULL
);
`

// DB wraps a SQLite sql.DB with hardened concurrency and domain repository operations.
type DB struct {
	db *sql.DB
}

func toEpochMs(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UTC().UnixMilli()
}

func fromEpochMs(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

func nullTimeToEpochMs(t *time.Time) interface{} {
	if t == nil || t.IsZero() {
		return nil
	}
	return t.UTC().UnixMilli()
}

func epochMsToNullTime(ms sql.NullInt64) *time.Time {
	if !ms.Valid || ms.Int64 == 0 {
		return nil
	}
	t := time.UnixMilli(ms.Int64).UTC()
	return &t
}

// Open initializes SQLite, applies WAL mode and concurrency pragmas, and configures single-writer connection pooling.
func Open(dsn string) (*DB, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

	// Single connection for SQLite avoids multi-connection lock contention and ensures pragmas remain active
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	// Pragmas for WAL mode, busy timeout, and relational integrity
	pragmas := []string{
		"PRAGMA journal_mode = WAL;",
		"PRAGMA synchronous = NORMAL;",
		"PRAGMA busy_timeout = 5000;",
		"PRAGMA foreign_keys = ON;",
	}

	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to execute pragma '%s': %w", p, err)
		}
	}

	// Initialize tables
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return &DB{db: db}, nil
}

// Close closes the underlying SQLite database connection.
func (d *DB) Close() error {
	return d.db.Close()
}

// Ping verifies database connectivity.
func (d *DB) Ping(ctx context.Context) error {
	return d.db.PingContext(ctx)
}

// CreateTenant inserts a new tenant publisher.
func (d *DB) CreateTenant(ctx context.Context, tenant *model.Tenant) error {
	query := `INSERT INTO tenants (id, name, secret, enabled, created_at) VALUES (?, ?, ?, ?, ?)`
	_, err := d.db.ExecContext(ctx, query, tenant.ID, tenant.Name, tenant.Secret, tenant.Enabled, toEpochMs(tenant.CreatedAt))
	if err != nil {
		return fmt.Errorf("failed to insert tenant: %w", err)
	}
	return nil
}

// GetTenant retrieves a tenant by ID.
func (d *DB) GetTenant(ctx context.Context, id string) (*model.Tenant, error) {
	query := `SELECT id, name, secret, enabled, created_at FROM tenants WHERE id = ?`
	row := d.db.QueryRowContext(ctx, query, id)

	var t model.Tenant
	var createdAtMs int64
	err := row.Scan(&t.ID, &t.Name, &t.Secret, &t.Enabled, &createdAtMs)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, model.ErrTenantNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query tenant: %w", err)
	}

	t.CreatedAt = fromEpochMs(createdAtMs)
	return &t, nil
}

// IngestEvent idempotently inserts a webhook event and schedules its initial delivery job.
// Handles concurrent ingestion races gracefully without throwing 500 on UNIQUE constraint collisions.
func (d *DB) IngestEvent(ctx context.Context, event *model.Event, maxAttempts int) (*model.Event, *model.DeliveryJob, bool, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// 1. Initial check for existing event
	var existingEventID string
	err = tx.QueryRowContext(ctx, `SELECT id FROM events WHERE tenant_id = ? AND idempotency_key = ?`,
		event.TenantID, event.IdempotencyKey).Scan(&existingEventID)

	if err == nil {
		existingEv, existingJob, fetchErr := d.getEventAndJobTx(ctx, tx, existingEventID)
		if fetchErr != nil {
			return nil, nil, false, fetchErr
		}
		_ = tx.Commit()
		return existingEv, existingJob, true, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, nil, false, fmt.Errorf("failed to query existing event: %w", err)
	}

	// 2. Prepare event metadata
	if event.ID == "" {
		event.ID = uuid.NewString()
	}
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}

	headersJSON, err := json.Marshal(event.Headers)
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to serialize headers: %w", err)
	}

	insertEventQuery := `
		INSERT INTO events (id, tenant_id, idempotency_key, destination_url, payload, headers, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`
	_, err = tx.ExecContext(ctx, insertEventQuery,
		event.ID, event.TenantID, event.IdempotencyKey, event.DestinationURL, event.Payload, string(headersJSON), toEpochMs(event.CreatedAt))

	if err != nil {
		// Handle concurrent insertion race: if unique constraint was violated, fetch existing
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			var raceEventID string
			if queryErr := tx.QueryRowContext(ctx, `SELECT id FROM events WHERE tenant_id = ? AND idempotency_key = ?`,
				event.TenantID, event.IdempotencyKey).Scan(&raceEventID); queryErr == nil {
				existingEv, existingJob, fetchErr := d.getEventAndJobTx(ctx, tx, raceEventID)
				if fetchErr == nil {
					_ = tx.Commit()
					return existingEv, existingJob, true, nil
				}
			}
		}
		return nil, nil, false, fmt.Errorf("failed to insert event: %w", err)
	}

	// 3. Create initial delivery job
	job := &model.DeliveryJob{
		ID:           uuid.NewString(),
		EventID:      event.ID,
		Status:       model.StatusPending,
		AttemptCount: 0,
		MaxAttempts:  maxAttempts,
		NextRetryAt:  event.CreatedAt,
		CreatedAt:    event.CreatedAt,
		UpdatedAt:    event.CreatedAt,
	}

	insertJobQuery := `
		INSERT INTO delivery_jobs (id, event_id, status, attempt_count, max_attempts, next_retry_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	_, err = tx.ExecContext(ctx, insertJobQuery,
		job.ID, job.EventID, string(job.Status), job.AttemptCount, job.MaxAttempts, toEpochMs(job.NextRetryAt), toEpochMs(job.CreatedAt), toEpochMs(job.UpdatedAt))
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to insert delivery job: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, nil, false, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return event, job, false, nil
}

// ClaimJobs atomically reserves up to batchSize ready jobs by transitioning them to IN_FLIGHT.
func (d *DB) ClaimJobs(ctx context.Context, batchSize int, leaseDuration time.Duration, now time.Time) ([]*model.DeliveryJob, error) {
	if batchSize <= 0 {
		batchSize = 10
	}

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin claim transaction: %w", err)
	}
	defer tx.Rollback()

	nowMs := toEpochMs(now)
	query := `
		SELECT id, event_id, status, attempt_count, max_attempts, next_retry_at, created_at, updated_at
		FROM delivery_jobs
		WHERE (status = ? OR status = ?) AND next_retry_at <= ?
		ORDER BY next_retry_at ASC
		LIMIT ?`

	rows, err := tx.QueryContext(ctx, query, string(model.StatusPending), string(model.StatusRetryPending), nowMs, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to query claimable jobs: %w", err)
	}
	defer rows.Close()

	var jobIDs []string
	var jobs []*model.DeliveryJob

	for rows.Next() {
		var j model.DeliveryJob
		var statusStr string
		var nextRetryMs, createdMs, updatedMs int64
		if err := rows.Scan(&j.ID, &j.EventID, &statusStr, &j.AttemptCount, &j.MaxAttempts, &nextRetryMs, &createdMs, &updatedMs); err != nil {
			return nil, fmt.Errorf("failed to scan job: %w", err)
		}
		j.Status = model.DeliveryStatus(statusStr)
		j.NextRetryAt = fromEpochMs(nextRetryMs)
		j.CreatedAt = fromEpochMs(createdMs)
		j.UpdatedAt = fromEpochMs(updatedMs)

		jobs = append(jobs, &j)
		jobIDs = append(jobIDs, j.ID)
	}

	if len(jobs) == 0 {
		return nil, nil
	}

	leasedUntil := now.Add(leaseDuration).UTC()
	leasedAt := now.UTC()
	leasedUntilMs := toEpochMs(leasedUntil)
	leasedAtMs := toEpochMs(leasedAt)

	placeholders := strings.Repeat("?,", len(jobIDs))
	placeholders = placeholders[:len(placeholders)-1]

	updateQuery := fmt.Sprintf(`
		UPDATE delivery_jobs
		SET status = ?, leased_at = ?, leased_until = ?, updated_at = ?
		WHERE id IN (%s)`, placeholders)

	args := []interface{}{string(model.StatusInFlight), leasedAtMs, leasedUntilMs, nowMs}
	for _, id := range jobIDs {
		args = append(args, id)
	}

	if _, err := tx.ExecContext(ctx, updateQuery, args...); err != nil {
		return nil, fmt.Errorf("failed to update claimed jobs: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit claim transaction: %w", err)
	}

	for _, j := range jobs {
		j.Status = model.StatusInFlight
		j.LeasedAt = &leasedAt
		j.LeasedUntil = &leasedUntil
		j.UpdatedAt = now.UTC()
	}

	return jobs, nil
}

// StaleJobInfo represents a job discovered with an expired lease.
type StaleJobInfo struct {
	ID           string
	AttemptCount int
	MaxAttempts  int
}

// ReapStaleLeases recovers jobs stuck in IN_FLIGHT whose lease has expired (e.g. crashed workers).
// It increments attempt_count and creates an attempt log to prevent poison-pill payload loops.
func (d *DB) ReapStaleLeases(ctx context.Context, now time.Time) (int64, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin reap transaction: %w", err)
	}
	defer tx.Rollback()

	nowMs := toEpochMs(now)

	// Query all expired in-flight jobs
	rows, err := tx.QueryContext(ctx, `
		SELECT id, attempt_count, max_attempts
		FROM delivery_jobs
		WHERE status = ? AND leased_until < ?`, string(model.StatusInFlight), nowMs)
	if err != nil {
		return 0, fmt.Errorf("failed to query stale leases: %w", err)
	}
	defer rows.Close()

	var staleJobs []StaleJobInfo
	for rows.Next() {
		var s StaleJobInfo
		if err := rows.Scan(&s.ID, &s.AttemptCount, &s.MaxAttempts); err != nil {
			return 0, fmt.Errorf("failed to scan stale job: %w", err)
		}
		staleJobs = append(staleJobs, s)
	}

	var reapedCount int64
	for _, job := range staleJobs {
		newAttemptCount := job.AttemptCount + 1

		// Log orphaned attempt caused by crash/lease expiration
		insertAttempt := `
			INSERT INTO delivery_attempts (id, job_id, attempt_number, status_code, execution_duration_ms, error_message, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)`
		_, err := tx.ExecContext(ctx, insertAttempt,
			uuid.NewString(), job.ID, newAttemptCount, 0, 0, "Worker lease expired or abandoned; reclaimed by reaper", nowMs)
		if err != nil {
			return 0, fmt.Errorf("failed to log reaper attempt: %w", err)
		}

		if newAttemptCount >= job.MaxAttempts {
			// Max attempts reached -> transition to DEAD_LETTER
			updateQuery := `
				UPDATE delivery_jobs
				SET status = ?, attempt_count = ?, last_error_code = 'LEASE_TIMEOUT',
				    last_error_message = 'Worker lease expired with attempts exhausted',
				    leased_at = NULL, leased_until = NULL, updated_at = ?
				WHERE id = ?`
			_, err = tx.ExecContext(ctx, updateQuery, string(model.StatusDeadLetter), newAttemptCount, nowMs, job.ID)
		} else {
			// Reclaim to RETRY_PENDING
			updateQuery := `
				UPDATE delivery_jobs
				SET status = ?, attempt_count = ?, next_retry_at = ?, last_error_code = 'LEASE_TIMEOUT',
				    last_error_message = 'Worker lease expired; reclaimed by reaper',
				    leased_at = NULL, leased_until = NULL, updated_at = ?
				WHERE id = ?`
			_, err = tx.ExecContext(ctx, updateQuery, string(model.StatusRetryPending), newAttemptCount, nowMs, nowMs, job.ID)
		}

		if err != nil {
			return 0, fmt.Errorf("failed to update stale job %s: %w", job.ID, err)
		}
		reapedCount++
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit reap transaction: %w", err)
	}

	return reapedCount, nil
}

// RecordAttempt persists an individual delivery attempt and updates the job's lifecycle status.
// Includes lease fencing: if expectedLeaseUntil is provided, verifies that the worker still holds
// the granted lease. If the lease was lost or expired, returns model.ErrLeaseLost.
func (d *DB) RecordAttempt(ctx context.Context, attempt *model.DeliveryAttempt, expectedLeaseUntil *time.Time, nextStatus model.DeliveryStatus, nextRetryAt time.Time, lastErrorCode, lastErrorMessage string) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin record attempt transaction: %w", err)
	}
	defer tx.Rollback()

	if attempt.ID == "" {
		attempt.ID = uuid.NewString()
	}
	if attempt.CreatedAt.IsZero() {
		attempt.CreatedAt = time.Now().UTC()
	}

	attemptCreatedAtMs := toEpochMs(attempt.CreatedAt)
	nowMs := toEpochMs(time.Now().UTC())
	nextRetryMs := toEpochMs(nextRetryAt)

	// Lease fencing: update only if status is IN_FLIGHT and leased_until matches
	var updateQuery string
	var args []interface{}

	if expectedLeaseUntil != nil {
		expectedLeaseMs := toEpochMs(*expectedLeaseUntil)
		updateQuery = `
			UPDATE delivery_jobs
			SET status = ?, attempt_count = attempt_count + 1, next_retry_at = ?,
			    leased_at = NULL, leased_until = NULL, last_error_code = ?, last_error_message = ?, updated_at = ?
			WHERE id = ? AND status = ? AND leased_until = ?`
		args = []interface{}{
			string(nextStatus), nextRetryMs, lastErrorCode, lastErrorMessage, nowMs,
			attempt.JobID, string(model.StatusInFlight), expectedLeaseMs,
		}
	} else {
		updateQuery = `
			UPDATE delivery_jobs
			SET status = ?, attempt_count = attempt_count + 1, next_retry_at = ?,
			    leased_at = NULL, leased_until = NULL, last_error_code = ?, last_error_message = ?, updated_at = ?
			WHERE id = ?`
		args = []interface{}{
			string(nextStatus), nextRetryMs, lastErrorCode, lastErrorMessage, nowMs, attempt.JobID,
		}
	}

	res, err := tx.ExecContext(ctx, updateQuery, args...)
	if err != nil {
		return fmt.Errorf("failed to update delivery job state: %w", err)
	}

	rowsAffected, _ := res.RowsAffected()
	if expectedLeaseUntil != nil && rowsAffected == 0 {
		// Fencing violation: lease was reclaimed or job was modified
		attempt.ErrorMessage = fmt.Sprintf("orphaned attempt (lease lost): %s", attempt.ErrorMessage)
		insertAttempt := `
			INSERT INTO delivery_attempts (id, job_id, attempt_number, status_code, execution_duration_ms, error_message, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)`
		_, _ = tx.ExecContext(ctx, insertAttempt,
			attempt.ID, attempt.JobID, attempt.AttemptNumber, attempt.StatusCode, attempt.ExecutionDurationMs, attempt.ErrorMessage, attemptCreatedAtMs)
		_ = tx.Commit()
		return model.ErrLeaseLost
	}

	// Insert attempt record
	insertAttempt := `
		INSERT INTO delivery_attempts (id, job_id, attempt_number, status_code, execution_duration_ms, error_message, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`
	_, err = tx.ExecContext(ctx, insertAttempt,
		attempt.ID, attempt.JobID, attempt.AttemptNumber, attempt.StatusCode, attempt.ExecutionDurationMs, attempt.ErrorMessage, attemptCreatedAtMs)
	if err != nil {
		return fmt.Errorf("failed to insert attempt: %w", err)
	}

	return tx.Commit()
}

// GetJobWithEvent retrieves a job and its associated parent event.
func (d *DB) GetJobWithEvent(ctx context.Context, jobID string) (*model.DeliveryJob, *model.Event, error) {
	query := `
		SELECT j.id, j.event_id, j.status, j.attempt_count, j.max_attempts, j.next_retry_at,
		       j.leased_at, j.leased_until, j.last_error_code, j.last_error_message, j.created_at, j.updated_at,
		       e.id, e.tenant_id, e.idempotency_key, e.destination_url, e.payload, e.headers, e.created_at
		FROM delivery_jobs j
		JOIN events e ON j.event_id = e.id
		WHERE j.id = ?`

	row := d.db.QueryRowContext(ctx, query, jobID)

	var j model.DeliveryJob
	var e model.Event
	var statusStr string
	var nextRetryMs, jCreatedMs, jUpdatedMs, eCreatedMs int64
	var leasedAtMs, leasedUntilMs sql.NullInt64
	var lastErrCode, lastErrMsg sql.NullString
	var headersStr string

	err := row.Scan(
		&j.ID, &j.EventID, &statusStr, &j.AttemptCount, &j.MaxAttempts, &nextRetryMs,
		&leasedAtMs, &leasedUntilMs, &lastErrCode, &lastErrMsg, &jCreatedMs, &jUpdatedMs,
		&e.ID, &e.TenantID, &e.IdempotencyKey, &e.DestinationURL, &e.Payload, &headersStr, &eCreatedMs,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, model.ErrJobNotFound
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to scan job with event: %w", err)
	}

	j.Status = model.DeliveryStatus(statusStr)
	j.NextRetryAt = fromEpochMs(nextRetryMs)
	j.CreatedAt = fromEpochMs(jCreatedMs)
	j.UpdatedAt = fromEpochMs(jUpdatedMs)
	j.LeasedAt = epochMsToNullTime(leasedAtMs)
	j.LeasedUntil = epochMsToNullTime(leasedUntilMs)

	if lastErrCode.Valid {
		j.LastErrorCode = lastErrCode.String
	}
	if lastErrMsg.Valid {
		j.LastErrorMessage = lastErrMsg.String
	}

	e.CreatedAt = fromEpochMs(eCreatedMs)
	_ = json.Unmarshal([]byte(headersStr), &e.Headers)

	return &j, &e, nil
}

// ListDeadLetterJobs retrieves dead-lettered jobs for operational inspection.
func (d *DB) ListDeadLetterJobs(ctx context.Context, limit, offset int) ([]*model.DeliveryJob, error) {
	if limit <= 0 {
		limit = 50
	}
	query := `
		SELECT id, event_id, status, attempt_count, max_attempts, next_retry_at,
		       last_error_code, last_error_message, created_at, updated_at
		FROM delivery_jobs
		WHERE status = ?
		ORDER BY updated_at DESC
		LIMIT ? OFFSET ?`

	rows, err := d.db.QueryContext(ctx, query, string(model.StatusDeadLetter), limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to query dead-letter jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*model.DeliveryJob
	for rows.Next() {
		var j model.DeliveryJob
		var statusStr string
		var nextRetryMs, createdMs, updatedMs int64
		var lastErrCode, lastErrMsg sql.NullString
		if err := rows.Scan(&j.ID, &j.EventID, &statusStr, &j.AttemptCount, &j.MaxAttempts, &nextRetryMs,
			&lastErrCode, &lastErrMsg, &createdMs, &updatedMs); err != nil {
			return nil, fmt.Errorf("failed to scan dead-letter job: %w", err)
		}
		j.Status = model.DeliveryStatus(statusStr)
		j.NextRetryAt = fromEpochMs(nextRetryMs)
		j.CreatedAt = fromEpochMs(createdMs)
		j.UpdatedAt = fromEpochMs(updatedMs)
		if lastErrCode.Valid {
			j.LastErrorCode = lastErrCode.String
		}
		if lastErrMsg.Valid {
			j.LastErrorMessage = lastErrMsg.String
		}
		jobs = append(jobs, &j)
	}
	return jobs, nil
}

// ReplayDeadLetterJob resets a dead-lettered job to PENDING so it will be retried.
func (d *DB) ReplayDeadLetterJob(ctx context.Context, jobID string, now time.Time) error {
	nowMs := toEpochMs(now)
	query := `
		UPDATE delivery_jobs
		SET status = ?, attempt_count = 0, next_retry_at = ?,
		    last_error_code = NULL, last_error_message = NULL,
		    leased_at = NULL, leased_until = NULL, updated_at = ?
		WHERE id = ? AND status = ?`

	res, err := d.db.ExecContext(ctx, query, string(model.StatusPending), nowMs, nowMs, jobID, string(model.StatusDeadLetter))
	if err != nil {
		return fmt.Errorf("failed to replay dead-letter job: %w", err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return model.ErrJobNotFound
	}
	return nil
}

func (d *DB) getEventAndJobTx(ctx context.Context, tx *sql.Tx, eventID string) (*model.Event, *model.DeliveryJob, error) {
	var ev model.Event
	var headersStr string
	var evCreatedMs int64
	err := tx.QueryRowContext(ctx, `
		SELECT id, tenant_id, idempotency_key, destination_url, payload, headers, created_at
		FROM events WHERE id = ?`, eventID).Scan(
		&ev.ID, &ev.TenantID, &ev.IdempotencyKey, &ev.DestinationURL, &ev.Payload, &headersStr, &evCreatedMs)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query event: %w", err)
	}
	ev.CreatedAt = fromEpochMs(evCreatedMs)
	_ = json.Unmarshal([]byte(headersStr), &ev.Headers)

	var job model.DeliveryJob
	var statusStr string
	var nextRetryMs, jobCreatedMs, jobUpdatedMs int64
	err = tx.QueryRowContext(ctx, `
		SELECT id, event_id, status, attempt_count, max_attempts, next_retry_at, created_at, updated_at
		FROM delivery_jobs WHERE event_id = ?`, eventID).Scan(
		&job.ID, &job.EventID, &statusStr, &job.AttemptCount, &job.MaxAttempts, &nextRetryMs, &jobCreatedMs, &jobUpdatedMs)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query delivery job: %w", err)
	}
	job.Status = model.DeliveryStatus(statusStr)
	job.NextRetryAt = fromEpochMs(nextRetryMs)
	job.CreatedAt = fromEpochMs(jobCreatedMs)
	job.UpdatedAt = fromEpochMs(jobUpdatedMs)

	return &ev, &job, nil
}

// GetQueueDepths returns the count of jobs by status.
func (d *DB) GetQueueDepths(ctx context.Context) (map[string]int, error) {
	query := `SELECT status, COUNT(*) FROM delivery_jobs GROUP BY status`
	rows, err := d.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query queue depths: %w", err)
	}
	defer rows.Close()

	counts := make(map[string]int)
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, fmt.Errorf("failed to scan queue depth: %w", err)
		}
		counts[status] = count
	}
	return counts, nil
}

// CountJobs is a test helper that returns the absolute total of delivery_jobs.
func (d *DB) CountJobs(ctx context.Context) int {
	var c int
	d.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM delivery_jobs").Scan(&c)
	return c
}

// CountEvents is a test helper that returns the absolute total of events.
func (d *DB) CountEvents(ctx context.Context) int {
	var c int
	d.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM events").Scan(&c)
	return c
}

// ListQueueJobs retrieves active jobs (PENDING, IN_FLIGHT, RETRY_PENDING) for operational inspection.
func (d *DB) ListQueueJobs(ctx context.Context, limit, offset int) ([]*model.DeliveryJob, error) {
	if limit <= 0 {
		limit = 50
	}
	query := `
		SELECT id, event_id, status, attempt_count, max_attempts, next_retry_at,
		       leased_at, leased_until, created_at, updated_at
		FROM delivery_jobs
		WHERE status IN (?, ?, ?)
		ORDER BY created_at ASC
		LIMIT ? OFFSET ?`

	rows, err := d.db.QueryContext(ctx, query, 
		string(model.StatusPending), string(model.StatusInFlight), string(model.StatusRetryPending),
		limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to query queue jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*model.DeliveryJob
	for rows.Next() {
		var j model.DeliveryJob
		var statusStr string
		var nextRetryMs, createdMs, updatedMs int64
		var leasedAtMs, leasedUntilMs sql.NullInt64

		if err := rows.Scan(&j.ID, &j.EventID, &statusStr, &j.AttemptCount, &j.MaxAttempts, &nextRetryMs,
			&leasedAtMs, &leasedUntilMs, &createdMs, &updatedMs); err != nil {
			return nil, fmt.Errorf("failed to scan queue job: %w", err)
		}
		j.Status = model.DeliveryStatus(statusStr)
		j.NextRetryAt = fromEpochMs(nextRetryMs)
		j.LeasedAt = epochMsToNullTime(leasedAtMs)
		j.LeasedUntil = epochMsToNullTime(leasedUntilMs)
		j.CreatedAt = fromEpochMs(createdMs)
		j.UpdatedAt = fromEpochMs(updatedMs)
		
		jobs = append(jobs, &j)
	}
	return jobs, nil
}
