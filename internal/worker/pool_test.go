package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/suliddin1/SentryRelay/internal/delivery"
	"github.com/suliddin1/SentryRelay/internal/model"
	"github.com/suliddin1/SentryRelay/internal/retry"
	"github.com/suliddin1/SentryRelay/internal/storage/sqlite"
)

func newTestStorage(t *testing.T) (*sqlite.DB, *model.Tenant) {
	t.Helper()
	dir := t.TempDir()
	db, err := sqlite.Open(filepath.Join(dir, "worker_test.db"))
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	tenant := &model.Tenant{
		ID:        "tenant_" + uuid.NewString()[:8],
		Name:      "Test Tenant",
		Secret:    "secret_123",
		Enabled:   true,
		CreatedAt: time.Now().UTC(),
	}
	if err := db.CreateTenant(context.Background(), tenant); err != nil {
		t.Fatalf("failed to create tenant: %v", err)
	}

	return db, tenant
}

func TestPool_SuccessfulDelivery(t *testing.T) {
	var callCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	db, tenant := newTestStorage(t)
	ctx := context.Background()

	// Ingest an event
	ev := &model.Event{
		TenantID:       tenant.ID,
		IdempotencyKey: "evt_succ_1",
		DestinationURL: srv.URL,
		Payload:        []byte(`{"status":"delivered"}`),
	}
	_, job, _, err := db.IngestEvent(ctx, ev, 3)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	client := delivery.NewClient()
	cfg := Config{
		NumWorkers:     2,
		BatchSize:      5,
		PollInterval:   20 * time.Millisecond,
		LeaseDuration:  5 * time.Second,
		ReaperInterval: 1 * time.Second,
		RetryPolicy:    retry.DefaultPolicy(),
	}

	pool := NewPool(cfg, db, client)
	if err := pool.Start(ctx); err != nil {
		t.Fatalf("failed to start pool: %v", err)
	}
	defer pool.Stop()

	// Wait up to 2 seconds for worker to process job
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		j, _, err := db.GetJobWithEvent(ctx, job.ID)
		if err == nil && j.Status == model.StatusDelivered {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	j, _, err := db.GetJobWithEvent(ctx, job.ID)
	if err != nil {
		t.Fatalf("failed to get job: %v", err)
	}
	if j.Status != model.StatusDelivered {
		t.Fatalf("expected job to be DELIVERED, got: %s", j.Status)
	}
	if atomic.LoadInt32(&callCount) != 1 {
		t.Fatalf("expected 1 call to destination, got %d", callCount)
	}
}

func TestPool_PermanentErrorToDLQ(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity) // 422 permanent error
		w.Write([]byte(`unprocessable`))
	}))
	defer srv.Close()

	db, tenant := newTestStorage(t)
	ctx := context.Background()

	ev := &model.Event{
		TenantID:       tenant.ID,
		IdempotencyKey: "evt_dlq_1",
		DestinationURL: srv.URL,
		Payload:        []byte(`{}`),
	}
	_, job, _, err := db.IngestEvent(ctx, ev, 3)
	if err != nil {
		t.Fatalf("failed to ingest: %v", err)
	}

	client := delivery.NewClient()
	cfg := Config{
		NumWorkers:     1,
		BatchSize:      1,
		PollInterval:   20 * time.Millisecond,
		LeaseDuration:  5 * time.Second,
		ReaperInterval: 1 * time.Second,
		RetryPolicy:    retry.DefaultPolicy(),
	}

	pool := NewPool(cfg, db, client)
	if err := pool.Start(ctx); err != nil {
		t.Fatalf("failed to start pool: %v", err)
	}
	defer pool.Stop()

	// Wait for DLQ transition
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		j, _, err := db.GetJobWithEvent(ctx, job.ID)
		if err == nil && j.Status == model.StatusDeadLetter {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	j, _, err := db.GetJobWithEvent(ctx, job.ID)
	if err != nil {
		t.Fatalf("failed to get job: %v", err)
	}
	if j.Status != model.StatusDeadLetter {
		t.Fatalf("expected job to be DEAD_LETTER, got: %s", j.Status)
	}
	if j.LastErrorCode != "PERMANENT_ERROR" {
		t.Fatalf("expected error code PERMANENT_ERROR, got: %s", j.LastErrorCode)
	}
}
