package e2e

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/suliddin1/SentryRelay/internal/delivery"
	"github.com/suliddin1/SentryRelay/internal/model"
	"github.com/suliddin1/SentryRelay/internal/retry"
	"github.com/suliddin1/SentryRelay/internal/storage/sqlite"
	"github.com/suliddin1/SentryRelay/internal/worker"
)

func TestCrashRecovery_AbandonedInFlightJobIsRecoveredAndDelivered(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "crash_recovery.db")

	// Phase 1: Ingest event and simulate worker crash mid-flight
	var jobID string
	var destReceived atomic.Int32

	destSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		destReceived.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer destSrv.Close()

	func() {
		db, err := sqlite.Open(dbPath)
		if err != nil {
			t.Fatalf("failed to open sqlite: %v", err)
		}
		defer db.Close()

		tenant := &model.Tenant{
			ID:        "tenant_crash_test",
			Name:      "Crash Resilience Corp",
			Secret:    "secret_crash_123",
			Enabled:   true,
			CreatedAt: time.Now().UTC(),
		}
		if err := db.CreateTenant(context.Background(), tenant); err != nil {
			t.Fatalf("failed to create tenant: %v", err)
		}

		ev := &model.Event{
			TenantID:       tenant.ID,
			IdempotencyKey: "evt_crash_recovery_1",
			DestinationURL: destSrv.URL,
			Payload:        []byte(`{"crash_test":true}`),
			CreatedAt:      time.Now().UTC(),
		}
		_, job, _, err := db.IngestEvent(context.Background(), ev, 3)
		if err != nil {
			t.Fatalf("failed to ingest: %v", err)
		}
		jobID = job.ID

		// Simulate claiming the job by worker 1 with a short 200ms lease
		claimTime := time.Now().Add(time.Second).UTC()
		claimed, err := db.ClaimJobs(context.Background(), 1, 200*time.Millisecond, claimTime)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("failed to claim job: %v (len=%d)", err, len(claimed))
		}

		// Verify job is now IN_FLIGHT
		inFlightJob, _, err := db.GetJobWithEvent(context.Background(), jobID)
		if err != nil || inFlightJob.Status != model.StatusInFlight {
			t.Fatalf("expected job to be IN_FLIGHT, got %v", inFlightJob.Status)
		}

		// Process "crashes" right here: db connection closed without recording attempt
	}()

	// Wait 300ms so the lease definitely expires
	time.Sleep(300 * time.Millisecond)

	// Phase 2: Process restarts (new DB connection, new Worker Pool)
	restartedDB, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("failed to re-open sqlite: %v", err)
	}
	defer restartedDB.Close()

	client := delivery.NewClient()
	cfg := worker.Config{
		NumWorkers:     2,
		BatchSize:      5,
		PollInterval:   20 * time.Millisecond,
		LeaseDuration:  3 * time.Second,
		ReaperInterval: 100 * time.Millisecond,
		RetryPolicy:    retry.DefaultPolicy(),
	}

	restartedPool := worker.NewPool(cfg, restartedDB, client)
	if err := restartedPool.Start(context.Background()); err != nil {
		t.Fatalf("failed to start restarted pool: %v", err)
	}
	defer restartedPool.Stop()

	// Wait up to 3 seconds for the restarted pool to reap the stale lease and deliver the job
	deadline := time.Now().Add(3 * time.Second)
	delivered := false
	for time.Now().Before(deadline) {
		job, _, err := restartedDB.GetJobWithEvent(context.Background(), jobID)
		if err == nil && job.Status == model.StatusDelivered {
			delivered = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !delivered {
		j, _, _ := restartedDB.GetJobWithEvent(context.Background(), jobID)
		t.Fatalf("expected crashed job to be reaped and delivered, current status: %+v", j)
	}

	if destReceived.Load() != 1 {
		t.Fatalf("expected destination to receive payload exactly once after recovery, got %d", destReceived.Load())
	}
}
