package fairness

import (
	"bytes"
	"context"
		"fmt"
			"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
		"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/suliddin1/SentryRelay/internal/delivery"
	"github.com/suliddin1/SentryRelay/internal/model"
	"github.com/suliddin1/SentryRelay/internal/security"
	"github.com/suliddin1/SentryRelay/internal/server"
	"github.com/suliddin1/SentryRelay/internal/storage/sqlite"
	"github.com/suliddin1/SentryRelay/internal/worker"
)

func TestFairness_HeadOfLineBlocking(t *testing.T) {
	// Disable logging to avoid spam
	// slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))

	dir := t.TempDir()
	db, err := sqlite.Open(filepath.Join(dir, "fairness.db"))
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	// Create Tenant A (Spammer) and Tenant B (VIP)
	tenantA := &model.Tenant{ID: "tenant_a", Secret: "secret_a", Enabled: true, CreatedAt: time.Now().UTC()}
	tenantB := &model.Tenant{ID: "tenant_b", Secret: "secret_b", Enabled: true, CreatedAt: time.Now().UTC()}
	_ = db.CreateTenant(context.Background(), tenantA)
	_ = db.CreateTenant(context.Background(), tenantB)

	var aDeliveries int32
	var bDeliveries int32
	var bFirstDelivery atomic.Int64 // UnixNano

	// Mock destinations
	destA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate a slightly slow destination to occupy workers (50ms)
		time.Sleep(10 * time.Millisecond)
		atomic.AddInt32(&aDeliveries, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer destA.Close()

	destB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&bDeliveries, 1)
		bFirstDelivery.CompareAndSwap(0, time.Now().UnixNano())
		w.WriteHeader(http.StatusOK)
	}))
	defer destB.Close()

	// Start Pool
	workerCfg := worker.DefaultConfig()
	workerCfg.NumWorkers = 5
	workerCfg.BatchSize = 10
	workerCfg.PollInterval = 50 * time.Millisecond

	// Very fast timeout so we don't stall test too long
	client := delivery.NewClient(delivery.WithTimeout(1 * time.Second))
	pool := worker.NewPool(workerCfg, db, client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := pool.Start(ctx); err != nil {
		t.Fatalf("Failed to start pool: %v", err)
	}
	defer pool.Stop()

	// Start Server
	srvConfig := server.Config{
		DefaultMaxRetry:        3,
		ReplayTolerance:        5 * time.Minute,
		AllowLocalDestinations: true,
		TenantRateLimit:        10000,
		TenantBurstLimit:       10000,
		MaxQueueDepth:          100000,
	}
	srv := server.NewServer(srvConfig, db)
	handler := srv.Handler()

	ingest := func(tenant *model.Tenant, dest string, count int) {
		for i := 0; i < count; i++ {
			payload := []byte(fmt.Sprintf("{\"i\":%d}", i))
			ts := time.Now().Unix()
			sig := security.Sign(tenant.Secret, ts, payload)

			req := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(payload))
			req.Header.Set("X-SentryRelay-Tenant-ID", tenant.ID)
			req.Header.Set("X-SentryRelay-Signature", sig)
			req.Header.Set("X-SentryRelay-Timestamp", strconv.FormatInt(ts, 10))
			req.Header.Set("X-SentryRelay-Idempotency-Key", uuid.NewString())
			req.Header.Set("X-SentryRelay-Destination-URL", dest)

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusAccepted {
				t.Errorf("Expected 202, got %d", rec.Code)
			}
		}
	}

	// 1. Tenant A submits 200 jobs.
	t.Log("Tenant A submitting burst of 200 jobs...")
	ingest(tenantA, destA.URL, 200)

	// 2. Wait slightly to ensure A is well into the queue
	time.Sleep(100 * time.Millisecond)

	// 3. Tenant B submits 10 jobs.
	t.Log("Tenant B submitting 10 jobs...")
	bSubmitTime := time.Now()
	ingest(tenantB, destB.URL, 10)

	// Wait until Tenant B's first job is delivered
	for {
		if bFirstDelivery.Load() != 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	bTTFD := time.Duration(bFirstDelivery.Load() - bSubmitTime.UnixNano())
	t.Logf("Tenant A Deliveries at time of B's first delivery: %d", atomic.LoadInt32(&aDeliveries))
	t.Logf("Tenant B TTFD (Time To First Delivery): %v", bTTFD)

	// If the system is strictly FIFO, A will have ~200 deliveries before B gets 1.
	if atomic.LoadInt32(&aDeliveries) > 100 {
		t.Log("CONCLUSION: System exhibits severe Head-Of-Line Blocking. Tenant A starves Tenant B.")
	} else {
		t.Log("CONCLUSION: System exhibits fair scheduling.")
	}
}
