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
    created_at DATETIME NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    idempotency_key TEXT NOT NULL,
    destination_url TEXT NOT NULL,
    payload BLOB NOT NULL,
    headers TEXT NOT NULL,
    created_at DATETIME NOT NULL,
    UNIQUE(tenant_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS delivery_jobs (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    next_retry_at DATETIME NOT NULL,
    leased_at DATETIME,
    leased_until DATETIME,
    last_error_code TEXT,
    last_error_message TEXT,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL
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
    created_at DATETIME NOT NULL
);
`

const TimeFormat = time.RFC3339Nano

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(TimeFormat)
}

// DB wraps a SQLite sql.DB with domain repository operations.
type DB struct {
	db *sql.DB
}

// Open initializes SQLite, applies WAL mode and concurrency pragmas, and migrates the schema.
func Open(dsn string) (*DB, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite database: %w", err)
	}

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
	_, err := d.db.ExecContext(ctx, query, tenant.ID, tenant.Name, tenant.Secret, tenant.Enabled, formatTime(tenant.CreatedAt))
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
	var createdAtStr string
	err := row.Scan(&t.ID, &t.Name, &t.Secret, &t.Enabled, &createdAtStr)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, model.ErrTenantNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query tenant: %w", err)
	}

	t.CreatedAt, _ = parseTime(createdAtStr)
	return &t, nil
}

// IngestEvent idempotently inserts a webhook event and schedules its initial delivery job.
// If an event with (tenant_id, idempotency_key) already exists, it returns the existing records
// and duplicate=true without re-queueing a job.
func (d *DB) IngestEvent(ctx context.Context, event *model.Event, maxAttempts int) (*model.Event, *model.DeliveryJob, bool, error) {
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Check if already exists for idempotency
	var existingEventID string
	err = tx.QueryRowContext(ctx, `SELECT id FROM events WHERE tenant_id = ? AND idempotency_key = ?`,
		event.TenantID, event.IdempotencyKey).Scan(&existingEventID)

	if err == nil {
		// Existing event found - fetch event and delivery job
		existingEv, existingJob, fetchErr := d.getEventAndJobTx(ctx, tx, existingEventID)
		if fetchErr != nil {
			return nil, nil, false, fetchErr
		}
		_ = tx.Commit()
		return existingEv, existingJob, true, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, nil, false, fmt.Errorf("failed to query existing event: %w", err)
	}

	// Insert new event
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
		event.ID, event.TenantID, event.IdempotencyKey, event.DestinationURL, event.Payload, string(headersJSON), formatTime(event.CreatedAt))
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to insert event: %w", err)
	}

	// Create initial delivery job
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
		job.ID, job.EventID, string(job.Status), job.AttemptCount, job.MaxAttempts, formatTime(job.NextRetryAt), formatTime(job.CreatedAt), formatTime(job.UpdatedAt))
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

	// Select eligible jobs: PENDING or RETRY_PENDING where next_retry_at <= now
	query := `
		SELECT id, event_id, status, attempt_count, max_attempts, next_retry_at, created_at, updated_at
		FROM delivery_jobs
		WHERE (status = ? OR status = ?) AND next_retry_at <= ?
		ORDER BY next_retry_at ASC
		LIMIT ?`

	rows, err := tx.QueryContext(ctx, query, string(model.StatusPending), string(model.StatusRetryPending), formatTime(now), batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to query claimable jobs: %w", err)
	}
	defer rows.Close()

	var jobIDs []string
	var jobs []*model.DeliveryJob

	for rows.Next() {
		var j model.DeliveryJob
		var statusStr, nextRetryStr, createdStr, updatedStr string
		if err := rows.Scan(&j.ID, &j.EventID, &statusStr, &j.AttemptCount, &j.MaxAttempts, &nextRetryStr, &createdStr, &updatedStr); err != nil {
			return nil, fmt.Errorf("failed to scan job: %w", err)
		}
		j.Status = model.DeliveryStatus(statusStr)
		j.NextRetryAt, _ = parseTime(nextRetryStr)
		j.CreatedAt, _ = parseTime(createdStr)
		j.UpdatedAt, _ = parseTime(updatedStr)

		jobs = append(jobs, &j)
		jobIDs = append(jobIDs, j.ID)
	}

	if len(jobs) == 0 {
		return nil, nil
	}

	leasedUntil := now.Add(leaseDuration).UTC()
	leasedAt := now.UTC()

	// Update claimed jobs to IN_FLIGHT with lease timestamps
	placeholders := strings.Repeat("?,", len(jobIDs))
	placeholders = placeholders[:len(placeholders)-1]

	updateQuery := fmt.Sprintf(`
		UPDATE delivery_jobs
		SET status = ?, leased_at = ?, leased_until = ?, updated_at = ?
		WHERE id IN (%s)`, placeholders)

	args := []interface{}{string(model.StatusInFlight), formatTime(leasedAt), formatTime(leasedUntil), formatTime(now)}
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

// ReapStaleLeases recovers jobs stuck in IN_FLIGHT whose lease has expired (e.g. crashed workers).
func (d *DB) ReapStaleLeases(ctx context.Context, now time.Time) (int64, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin reap transaction: %w", err)
	}
	defer tx.Rollback()

	// 1. Move exhausted stale jobs to DEAD_LETTER
	queryExhausted := `
		UPDATE delivery_jobs
		SET status = ?, last_error_code = 'LEASE_TIMEOUT', last_error_message = 'Worker lease expired with attempts exhausted',
		    leased_at = NULL, leased_until = NULL, updated_at = ?
		WHERE status = ? AND leased_until < ? AND attempt_count >= max_attempts`
	resExhausted, err := tx.ExecContext(ctx, queryExhausted, string(model.StatusDeadLetter), formatTime(now), string(model.StatusInFlight), formatTime(now))
	if err != nil {
		return 0, fmt.Errorf("failed to dead-letter exhausted stale leases: %w", err)
	}
	deadCount, _ := resExhausted.RowsAffected()

	// 2. Move remaining stale jobs back to RETRY_PENDING
	queryRecoverable := `
		UPDATE delivery_jobs
		SET status = ?, next_retry_at = ?, last_error_code = 'LEASE_TIMEOUT',
		    last_error_message = 'Worker lease expired; reclaimed by reaper',
		    leased_at = NULL, leased_until = NULL, updated_at = ?
		WHERE status = ? AND leased_until < ?`
	resRecoverable, err := tx.ExecContext(ctx, queryRecoverable, string(model.StatusRetryPending), formatTime(now), formatTime(now), string(model.StatusInFlight), formatTime(now))
	if err != nil {
		return 0, fmt.Errorf("failed to reclaim recoverable stale leases: %w", err)
	}
	reclaimedCount, _ := resRecoverable.RowsAffected()

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit reap transaction: %w", err)
	}

	return deadCount + reclaimedCount, nil
}

// RecordAttempt persists an individual delivery attempt and updates the job's lifecycle status.
func (d *DB) RecordAttempt(ctx context.Context, attempt *model.DeliveryAttempt, nextStatus model.DeliveryStatus, nextRetryAt time.Time, lastErrorCode, lastErrorMessage string) error {
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

	// Insert attempt record
	insertAttempt := `
		INSERT INTO delivery_attempts (id, job_id, attempt_number, status_code, execution_duration_ms, error_message, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`
	_, err = tx.ExecContext(ctx, insertAttempt,
		attempt.ID, attempt.JobID, attempt.AttemptNumber, attempt.StatusCode, attempt.ExecutionDurationMs, attempt.ErrorMessage, formatTime(attempt.CreatedAt))
	if err != nil {
		return fmt.Errorf("failed to insert attempt: %w", err)
	}

	// Update job state
	updateJob := `
		UPDATE delivery_jobs
		SET status = ?, attempt_count = attempt_count + 1, next_retry_at = ?,
		    leased_at = NULL, leased_until = NULL, last_error_code = ?, last_error_message = ?, updated_at = ?
		WHERE id = ?`
	_, err = tx.ExecContext(ctx, updateJob,
		string(nextStatus), formatTime(nextRetryAt), lastErrorCode, lastErrorMessage, formatTime(time.Now()), attempt.JobID)
	if err != nil {
		return fmt.Errorf("failed to update delivery job state: %w", err)
	}

	return tx.Commit()
}

// GetJobWithEvent retrieves a job and its associated parent event.
func (d *DB) GetJobWithEvent(ctx context.Context, jobID string) (*model.DeliveryJob, *model.Event, error) {
	query := `
		SELECT j.id, j.event_id, j.status, j.attempt_count, j.max_attempts, j.next_retry_at,
		       j.last_error_code, j.last_error_message, j.created_at, j.updated_at,
		       e.id, e.tenant_id, e.idempotency_key, e.destination_url, e.payload, e.headers, e.created_at
		FROM delivery_jobs j
		JOIN events e ON j.event_id = e.id
		WHERE j.id = ?`

	row := d.db.QueryRowContext(ctx, query, jobID)

	var j model.DeliveryJob
	var e model.Event
	var statusStr, nextRetryStr, jCreatedStr, jUpdatedStr string
	var lastErrCode, lastErrMsg sql.NullString
	var headersStr, eCreatedStr string

	err := row.Scan(
		&j.ID, &j.EventID, &statusStr, &j.AttemptCount, &j.MaxAttempts, &nextRetryStr,
		&lastErrCode, &lastErrMsg, &jCreatedStr, &jUpdatedStr,
		&e.ID, &e.TenantID, &e.IdempotencyKey, &e.DestinationURL, &e.Payload, &headersStr, &eCreatedStr,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, model.ErrJobNotFound
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to scan job with event: %w", err)
	}

	j.Status = model.DeliveryStatus(statusStr)
	j.NextRetryAt, _ = parseTime(nextRetryStr)
	j.CreatedAt, _ = parseTime(jCreatedStr)
	j.UpdatedAt, _ = parseTime(jUpdatedStr)
	if lastErrCode.Valid {
		j.LastErrorCode = lastErrCode.String
	}
	if lastErrMsg.Valid {
		j.LastErrorMessage = lastErrMsg.String
	}

	e.CreatedAt, _ = parseTime(eCreatedStr)
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
		var statusStr, nextRetryStr, createdStr, updatedStr string
		var lastErrCode, lastErrMsg sql.NullString
		if err := rows.Scan(&j.ID, &j.EventID, &statusStr, &j.AttemptCount, &j.MaxAttempts, &nextRetryStr,
			&lastErrCode, &lastErrMsg, &createdStr, &updatedStr); err != nil {
			return nil, fmt.Errorf("failed to scan dead-letter job: %w", err)
		}
		j.Status = model.DeliveryStatus(statusStr)
		j.NextRetryAt, _ = parseTime(nextRetryStr)
		j.CreatedAt, _ = parseTime(createdStr)
		j.UpdatedAt, _ = parseTime(updatedStr)
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
	query := `
		UPDATE delivery_jobs
		SET status = ?, attempt_count = 0, next_retry_at = ?,
		    last_error_code = NULL, last_error_message = NULL,
		    leased_at = NULL, leased_until = NULL, updated_at = ?
		WHERE id = ? AND status = ?`

	res, err := d.db.ExecContext(ctx, query, string(model.StatusPending), formatTime(now), formatTime(now), jobID, string(model.StatusDeadLetter))
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
	var headersStr, evCreatedStr string
	err := tx.QueryRowContext(ctx, `
		SELECT id, tenant_id, idempotency_key, destination_url, payload, headers, created_at
		FROM events WHERE id = ?`, eventID).Scan(
		&ev.ID, &ev.TenantID, &ev.IdempotencyKey, &ev.DestinationURL, &ev.Payload, &headersStr, &evCreatedStr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query event: %w", err)
	}
	ev.CreatedAt, _ = parseTime(evCreatedStr)
	_ = json.Unmarshal([]byte(headersStr), &ev.Headers)

	var job model.DeliveryJob
	var statusStr, nextRetryStr, jobCreatedStr, jobUpdatedStr string
	err = tx.QueryRowContext(ctx, `
		SELECT id, event_id, status, attempt_count, max_attempts, next_retry_at, created_at, updated_at
		FROM delivery_jobs WHERE event_id = ?`, eventID).Scan(
		&job.ID, &job.EventID, &statusStr, &job.AttemptCount, &job.MaxAttempts, &nextRetryStr, &jobCreatedStr, &jobUpdatedStr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query delivery job: %w", err)
	}
	job.Status = model.DeliveryStatus(statusStr)
	job.NextRetryAt, _ = parseTime(nextRetryStr)
	job.CreatedAt, _ = parseTime(jobCreatedStr)
	job.UpdatedAt, _ = parseTime(jobUpdatedStr)

	return &ev, &job, nil
}

func parseTime(s string) (time.Time, error) {
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05Z",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unable to parse time string: %s", s)
}
