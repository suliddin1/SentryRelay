package sqlite

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/suliddin1/SentryRelay/internal/model"
)

func newTestDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test_sentryrelay.db")
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open test db: %v", err)
	}
	t.Cleanup(func() {
		db.Close()
	})
	return db
}

func createTestTenant(t *testing.T, db *DB) *model.Tenant {
	t.Helper()
	tenant := &model.Tenant{
		ID:        "tenant_" + uuid.NewString()[:8],
		Name:      "Acme Corp",
		Secret:    "secret_key_123",
		Enabled:   true,
		CreatedAt: time.Now().UTC(),
	}
	if err := db.CreateTenant(context.Background(), tenant); err != nil {
		t.Fatalf("failed to create tenant: %v", err)
	}
	return tenant
}

func TestTenantLifecycle(t *testing.T) {
	db := newTestDB(t)
	tenant := createTestTenant(t, db)

	fetched, err := db.GetTenant(context.Background(), tenant.ID)
	if err != nil {
		t.Fatalf("failed to get tenant: %v", err)
	}
	if fetched.Name != tenant.Name || fetched.Secret != tenant.Secret {
		t.Fatalf("tenant mismatch: got %+v, want %+v", fetched, tenant)
	}

	_, err = db.GetTenant(context.Background(), "non_existent")
	if err != model.ErrTenantNotFound {
		t.Fatalf("expected ErrTenantNotFound, got: %v", err)
	}
}

func TestIngestEvent_Idempotency(t *testing.T) {
	db := newTestDB(t)
	tenant := createTestTenant(t, db)
	ctx := context.Background()

	event := &model.Event{
		TenantID:       tenant.ID,
		IdempotencyKey: "order_12345",
		DestinationURL: "https://example.com/webhook",
		Payload:        []byte(`{"order_id":12345,"status":"paid"}`),
		Headers:        map[string]string{"Content-Type": "application/json"},
	}

	// First ingestion
	createdEv, createdJob, isDuplicate, err := db.IngestEvent(ctx, event, 5)
	if err != nil {
		t.Fatalf("failed to ingest event: %v", err)
	}
	if isDuplicate {
		t.Fatal("expected first ingestion to not be duplicate")
	}
	if createdEv.ID == "" || createdJob.ID == "" {
		t.Fatal("expected non-empty IDs")
	}
	if createdJob.Status != model.StatusPending {
		t.Fatalf("expected job status PENDING, got %s", createdJob.Status)
	}

	// Second ingestion with SAME tenant and idempotency key
	dupEvent := &model.Event{
		TenantID:       tenant.ID,
		IdempotencyKey: "order_12345",
		DestinationURL: "https://example.com/webhook",
		Payload:        []byte(`{"order_id":12345,"status":"paid"}`),
	}

	dupEv, dupJob, isDuplicate2, err := db.IngestEvent(ctx, dupEvent, 5)
	if err != nil {
		t.Fatalf("failed to re-ingest event: %v", err)
	}
	if !isDuplicate2 {
		t.Fatal("expected second ingestion to be detected as duplicate")
	}
	if dupEv.ID != createdEv.ID || dupJob.ID != createdJob.ID {
		t.Fatalf("expected duplicate to return original IDs: got event=%s job=%s, want event=%s job=%s",
			dupEv.ID, dupJob.ID, createdEv.ID, createdJob.ID)
	}
}

func TestClaimJobs_AtomicLeasing(t *testing.T) {
	db := newTestDB(t)
	tenant := createTestTenant(t, db)
	ctx := context.Background()

	// Ingest 5 events
	for i := 0; i < 5; i++ {
		ev := &model.Event{
			TenantID:       tenant.ID,
			IdempotencyKey: uuid.NewString(),
			DestinationURL: "https://example.com/webhook",
			Payload:        []byte(`{"test":true}`),
		}
		_, _, _, err := db.IngestEvent(ctx, ev, 3)
		if err != nil {
			t.Fatalf("failed to ingest: %v", err)
		}
	}

	claimTime := time.Now().Add(time.Second).UTC()

	// Claim batch of 3
	jobs, err := db.ClaimJobs(ctx, 3, 30*time.Second, claimTime)
	if err != nil {
		t.Fatalf("failed to claim jobs: %v", err)
	}
	if len(jobs) != 3 {
		t.Fatalf("expected 3 claimed jobs, got %d", len(jobs))
	}
	for _, j := range jobs {
		if j.Status != model.StatusInFlight {
			t.Fatalf("expected status IN_FLIGHT, got %s", j.Status)
		}
		if j.LeasedUntil == nil || j.LeasedUntil.Before(claimTime) {
			t.Fatalf("invalid lease expiry: %v", j.LeasedUntil)
		}
	}

	// Claim remaining 2
	jobs2, err := db.ClaimJobs(ctx, 3, 30*time.Second, claimTime)
	if err != nil {
		t.Fatalf("failed to claim remaining jobs: %v", err)
	}
	if len(jobs2) != 2 {
		t.Fatalf("expected 2 claimed jobs, got %d", len(jobs2))
	}

	// Attempt claim when none left
	jobs3, err := db.ClaimJobs(ctx, 3, 30*time.Second, claimTime)
	if err != nil {
		t.Fatalf("unexpected error on empty claim: %v", err)
	}
	if len(jobs3) != 0 {
		t.Fatalf("expected 0 jobs claimed, got %d", len(jobs3))
	}
}

func TestConcurrentClaimJobs_NoDuplicateClaim(t *testing.T) {
	db := newTestDB(t)
	tenant := createTestTenant(t, db)
	ctx := context.Background()

	const totalJobs = 20
	for i := 0; i < totalJobs; i++ {
		ev := &model.Event{
			TenantID:       tenant.ID,
			IdempotencyKey: uuid.NewString(),
			DestinationURL: "https://example.com/webhook",
			Payload:        []byte(`{"idx":1}`),
		}
		_, _, _, err := db.IngestEvent(ctx, ev, 3)
		if err != nil {
			t.Fatalf("failed to ingest: %v", err)
		}
	}

	claimTime := time.Now().Add(time.Second).UTC()

	// Concurrently claim from 4 workers
	var wg sync.WaitGroup
	claimedMap := sync.Map{}
	duplicateClaims := 0
	var mu sync.Mutex

	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				claimed, err := db.ClaimJobs(ctx, 2, 30*time.Second, claimTime)
				if err != nil || len(claimed) == 0 {
					return
				}
				for _, j := range claimed {
					if _, loaded := claimedMap.LoadOrStore(j.ID, true); loaded {
						mu.Lock()
						duplicateClaims++
						mu.Unlock()
					}
				}
			}
		}()
	}

	wg.Wait()

	if duplicateClaims > 0 {
		t.Fatalf("detected %d duplicate job claims across concurrent workers", duplicateClaims)
	}

	// Count total unique claimed
	count := 0
	claimedMap.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	if count != totalJobs {
		t.Fatalf("expected %d total jobs claimed, got %d", totalJobs, count)
	}
}

func TestReapStaleLeases(t *testing.T) {
	db := newTestDB(t)
	tenant := createTestTenant(t, db)
	ctx := context.Background()

	ev := &model.Event{
		TenantID:       tenant.ID,
		IdempotencyKey: "reap_test",
		DestinationURL: "https://example.com/webhook",
		Payload:        []byte(`{}`),
	}
	_, job, _, err := db.IngestEvent(ctx, ev, 2)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	// Claim with short 50ms lease
	claimed, err := db.ClaimJobs(ctx, 1, 50*time.Millisecond, time.Now().Add(time.Second).UTC())
	if err != nil || len(claimed) != 1 {
		t.Fatalf("failed to claim job: %v (len=%d)", err, len(claimed))
	}

	// Fast-forward time for reaper to ensure lease is expired
	reapTime := time.Now().Add(5 * time.Second).UTC()
	reaped, err := db.ReapStaleLeases(ctx, reapTime)
	if err != nil {
		t.Fatalf("failed to reap leases: %v", err)
	}
	if reaped != 1 {
		t.Fatalf("expected 1 reaped job, got %d", reaped)
	}

	// Verify job is back in RETRY_PENDING
	updatedJob, _, err := db.GetJobWithEvent(ctx, job.ID)
	if err != nil {
		t.Fatalf("failed to get job: %v", err)
	}
	if updatedJob.Status != model.StatusRetryPending {
		t.Fatalf("expected status RETRY_PENDING after reap, got %s", updatedJob.Status)
	}
}

func TestDeadLetterAndReplay(t *testing.T) {
	db := newTestDB(t)
	tenant := createTestTenant(t, db)
	ctx := context.Background()

	ev := &model.Event{
		TenantID:       tenant.ID,
		IdempotencyKey: "dlq_test",
		DestinationURL: "https://example.com/webhook",
		Payload:        []byte(`{}`),
	}
	_, job, _, err := db.IngestEvent(ctx, ev, 1)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	// Record failed attempt transitioning to DEAD_LETTER
	attempt := &model.DeliveryAttempt{
		JobID:               job.ID,
		AttemptNumber:       1,
		StatusCode:          400,
		ExecutionDurationMs: 50,
		ErrorMessage:        "HTTP 400 Bad Request",
	}
	err = db.RecordAttempt(ctx, attempt, model.StatusDeadLetter, time.Time{}, "HTTP_400", "Bad Request")
	if err != nil {
		t.Fatalf("failed to record attempt: %v", err)
	}

	// Verify in DLQ
	dlqJobs, err := db.ListDeadLetterJobs(ctx, 10, 0)
	if err != nil {
		t.Fatalf("failed to list dlq jobs: %v", err)
	}
	if len(dlqJobs) != 1 || dlqJobs[0].ID != job.ID {
		t.Fatalf("expected job in DLQ, got %d jobs", len(dlqJobs))
	}

	// Replay job
	err = db.ReplayDeadLetterJob(ctx, job.ID, time.Now().UTC())
	if err != nil {
		t.Fatalf("failed to replay dead-letter job: %v", err)
	}

	// Verify job is now PENDING with 0 attempts
	replayedJob, _, err := db.GetJobWithEvent(ctx, job.ID)
	if err != nil {
		t.Fatalf("failed to get replayed job: %v", err)
	}
	if replayedJob.Status != model.StatusPending || replayedJob.AttemptCount != 0 {
		t.Fatalf("expected job to be reset to PENDING with 0 attempts, got %s (%d attempts)",
			replayedJob.Status, replayedJob.AttemptCount)
	}
}
