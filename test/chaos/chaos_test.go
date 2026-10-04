package chaos

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/suliddin1/SentryRelay/internal/delivery"
	"github.com/suliddin1/SentryRelay/internal/model"
	"github.com/suliddin1/SentryRelay/internal/server"
	"github.com/suliddin1/SentryRelay/internal/storage/sqlite"
	"github.com/suliddin1/SentryRelay/internal/worker"
)

func TestChaos_ResilienceAndDataIntegrity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}

	// 1. Setup Mock Destination Server with chaotic behavior
	var deliveryAttempts sync.Map // Track how many times a payload was delivered

	chaosDest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payloadID := r.Header.Get("X-Test-Payload-ID")
		if payloadID != "" {
			val, _ := deliveryAttempts.LoadOrStore(payloadID, 0)
			deliveryAttempts.Store(payloadID, val.(int)+1)
		}

		roll := rand.Intn(100)
		switch {
		case roll < 20: // 20% Success
			w.WriteHeader(http.StatusOK)
		case roll < 40: // 20% Transient 429
			w.WriteHeader(http.StatusTooManyRequests)
		case roll < 60: // 20% Transient 503
			w.WriteHeader(http.StatusServiceUnavailable)
		case roll < 70: // 10% Permanent 400
			w.WriteHeader(http.StatusBadRequest)
		case roll < 80: // 10% Timeout / Drop Connection
			hj, ok := w.(http.Hijacker)
			if ok {
				conn, _, _ := hj.Hijack()
				conn.Close() // abrupt close
			}
			return
		default: // 20% Slow Response
			time.Sleep(50 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer chaosDest.Close()

	// 2. Setup embedded SentryRelay
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "chaos.db")
	db, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tenant := &model.Tenant{
		ID:      "chaos-tenant",
		Name:    "Chaos",
		Secret:  "super-secret-chaos-key",
		Enabled: true,
	}
	if err := db.CreateTenant(ctx, tenant); err != nil {
		t.Fatalf("failed to create tenant: %v", err)
	}

	// Configure server
	srv := server.NewServer(server.Config{
		DefaultMaxRetry:        3, // short retries for test speed
		ReplayTolerance:        5 * time.Minute,
		AllowLocalDestinations: true, // required to call our httptest server
		TenantRateLimit:        10000,
		TenantBurstLimit:       20000,
		MaxQueueDepth:          100000,
	}, db)

	apiServer := httptest.NewServer(srv.Handler())
	defer apiServer.Close()

	// 3. Worker Pool Manager (handles simulated crashes)
	var poolMu sync.Mutex
	var currentPool *worker.Pool

	startPool := func() {
		poolMu.Lock()
		defer poolMu.Unlock()
		if currentPool != nil {
			currentPool.Stop()
		}
		
		deliveryClient := delivery.NewClient(delivery.WithTimeout(1 * time.Second)) // short timeout
		
		cfg := worker.DefaultConfig()
		cfg.NumWorkers = 20
		cfg.PollInterval = 50 * time.Millisecond
		cfg.ReaperInterval = 100 * time.Millisecond // fast reaper
		cfg.LeaseDuration = 200 * time.Millisecond // extremely short lease to simulate fast timeouts and test fencing
		cfg.MaxDestConcurrency = 50 // high enough

		currentPool = worker.NewPool(cfg, db, deliveryClient)
		if err := currentPool.Start(ctx); err != nil {
			t.Errorf("failed to start pool: %v", err)
		}
	}

	stopPool := func() {
		poolMu.Lock()
		defer poolMu.Unlock()
		if currentPool != nil {
			currentPool.Stop()
			currentPool = nil
		}
	}

	startPool()
	defer stopPool()

	// 4. Ingest 1,000 requests concurrently with bounded workers to prevent socket exhaustion on Windows
	var wg sync.WaitGroup
	numRequests := 1000

	var (
		successCount int32
		failCount    int32
	)

	startIngest := time.Now()
	jobs := make(chan int, numRequests)
	for i := 0; i < numRequests; i++ {
		jobs <- i
	}
	close(jobs)

	for w := 0; w < 50; w++ { // 50 concurrent ingestion workers
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range jobs {
				payloadID := fmt.Sprintf("msg-%d", id)
				payloadBytes := []byte(fmt.Sprintf(`{"msg":"%s"}`, payloadID))
				
				req, _ := http.NewRequest(http.MethodPost, apiServer.URL+"/v1/ingest", bytes.NewReader(payloadBytes))
				req.Header.Set("X-SentryRelay-Tenant-ID", tenant.ID)
				req.Header.Set("X-SentryRelay-Idempotency-Key", payloadID)
				req.Header.Set("X-SentryRelay-Destination-URL", chaosDest.URL)
				req.Header.Set("X-Forward-X-Test-Payload-ID", payloadID)

				timestamp := time.Now().Unix()
				req.Header.Set("X-SentryRelay-Timestamp", fmt.Sprintf("%d", timestamp))

				mac := hmac.New(sha256.New, []byte(tenant.Secret))
				mac.Write([]byte(fmt.Sprintf("%d.", timestamp)))
				mac.Write(payloadBytes)
				signature := hex.EncodeToString(mac.Sum(nil))
				req.Header.Set("X-SentryRelay-Signature", signature)

				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					fmt.Printf("ERROR: req %d failed: %v\n", id, err)
					atomic.AddInt32(&failCount, 1)
					continue
				}
				
				if resp.StatusCode != http.StatusAccepted {
					body, _ := io.ReadAll(resp.Body)
					fmt.Printf("ERROR: req %d expected 202 got %d: %s\n", id, resp.StatusCode, string(body))
					atomic.AddInt32(&failCount, 1)
				} else {
					atomic.AddInt32(&successCount, 1)
				}
				resp.Body.Close()
			}
		}()
	}

	// 5. Simulate node crashes / restarts during processing
	go func() {
		for i := 0; i < 3; i++ {
			time.Sleep(300 * time.Millisecond)
			stopPool() // Boom!
			time.Sleep(100 * time.Millisecond) // downtime
			startPool() // Restart
		}
	}()

	wg.Wait()
	t.Logf("Ingested %d events in %v (Success: %d, Fail: %d)", numRequests, time.Since(startIngest), atomic.LoadInt32(&successCount), atomic.LoadInt32(&failCount))

	// 6. Wait for quiescence (all jobs finished)
	quiescent := false
	for i := 0; i < 200; i++ { // wait up to 20 seconds
		counts, err := db.GetQueueDepths(ctx)
		if err != nil {
			t.Fatalf("failed to get queue depths: %v", err)
		}
		
		active := counts["pending"] + counts["in_flight"] + counts["retry_pending"]
		
		if i%20 == 0 {
			t.Logf("Queue Depths at %dms: %v | Total Events: %d | Total Jobs: %d", i*100, counts, db.CountEvents(ctx), db.CountJobs(ctx))
		}

		if active == 0 {
			// Double check if all 1000 are processed
			totalProcessed := counts["delivered"] + counts["dead_letter"]
			if totalProcessed >= numRequests {
				t.Logf("Reached quiescence at %dms. active=%d, total=%d", i*100, active, totalProcessed)
				quiescent = true
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !quiescent {
		counts, _ := db.GetQueueDepths(ctx)
		t.Fatalf("System failed to reach quiescent state! Active jobs remain: %v", counts)
	}

	// 7. Verify Data Integrity
	counts, err := db.GetQueueDepths(ctx)
	if err != nil {
		t.Fatalf("failed to get queue depths: %v", err)
	}

	delivered := counts["delivered"]
	dlq := counts["dead_letter"]
	totalProcessed := delivered + dlq

	t.Logf("Final State -> DELIVERED: %d, DLQ: %d", delivered, dlq)

	if totalProcessed != numRequests {
		t.Errorf("Expected exactly %d processed jobs, found %d (Delivered: %d, DLQ: %d)", numRequests, totalProcessed, delivered, dlq)
	}
}
