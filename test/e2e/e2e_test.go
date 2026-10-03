package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/suliddin1/SentryRelay/internal/delivery"
	"github.com/suliddin1/SentryRelay/internal/model"
	"github.com/suliddin1/SentryRelay/internal/retry"
	"github.com/suliddin1/SentryRelay/internal/security"
	"github.com/suliddin1/SentryRelay/internal/server"
	"github.com/suliddin1/SentryRelay/internal/storage/sqlite"
	"github.com/suliddin1/SentryRelay/internal/worker"
)

type testEnvironment struct {
	db     *sqlite.DB
	tenant *model.Tenant
	pool   *worker.Pool
	srv    *httptest.Server
}

func setupE2E(t *testing.T, maxRetries int, retryPolicy retry.Policy) *testEnvironment {
	t.Helper()
	dir := t.TempDir()
	db, err := sqlite.Open(filepath.Join(dir, "e2e_sentryrelay.db"))
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}

	tenant := &model.Tenant{
		ID:        "tenant_e2e",
		Name:      "E2E Enterprise",
		Secret:    "e2e_shared_hmac_secret_key_789",
		Enabled:   true,
		CreatedAt: time.Now().UTC(),
	}
	if err := db.CreateTenant(context.Background(), tenant); err != nil {
		t.Fatalf("failed to create tenant: %v", err)
	}

	client := delivery.NewClient(delivery.WithTimeout(2 * time.Second))
	workerCfg := worker.Config{
		NumWorkers:     2,
		BatchSize:      5,
		PollInterval:   20 * time.Millisecond,
		LeaseDuration:  3 * time.Second,
		ReaperInterval: 100 * time.Millisecond,
		RetryPolicy:    retryPolicy,
	}
	pool := worker.NewPool(workerCfg, db, client)
	if err := pool.Start(context.Background()); err != nil {
		t.Fatalf("failed to start worker pool: %v", err)
	}

	httpSrv := server.NewServer(server.Config{
		DefaultMaxRetry: maxRetries,
		ReplayTolerance: 5 * time.Minute,
	}, db)
	testSrv := httptest.NewServer(httpSrv.Handler())

	t.Cleanup(func() {
		pool.Stop()
		testSrv.Close()
		db.Close()
	})

	return &testEnvironment{
		db:     db,
		tenant: tenant,
		pool:   pool,
		srv:    testSrv,
	}
}

func TestE2E_FullPipeline_DeliverySuccess(t *testing.T) {
	var destinationCalls int32
	var receivedPayload []byte
	var receivedEventID string

	destSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&destinationCalls, 1)
		receivedEventID = r.Header.Get("X-SentryRelay-Event-ID")
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(r.Body)
		receivedPayload = buf.Bytes()
		w.WriteHeader(http.StatusOK)
	}))
	defer destSrv.Close()

	env := setupE2E(t, 3, retry.DefaultPolicy())

	// Ingest webhook
	now := time.Now().UTC()
	timestamp := now.Unix()
	payload := []byte(`{"order_id":"ord_999","amount":15000}`)
	sig := security.Sign(env.tenant.Secret, timestamp, payload)

	req, _ := http.NewRequest(http.MethodPost, env.srv.URL+"/v1/ingest", bytes.NewReader(payload))
	req.Header.Set("X-SentryRelay-Tenant-ID", env.tenant.ID)
	req.Header.Set("X-SentryRelay-Signature", sig)
	req.Header.Set("X-SentryRelay-Timestamp", strconv.FormatInt(timestamp, 10))
	req.Header.Set("X-SentryRelay-Idempotency-Key", "idemp_ord_999")
	req.Header.Set("X-SentryRelay-Destination-URL", destSrv.URL)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to call ingest: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d", resp.StatusCode)
	}

	var ingestRes map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&ingestRes)
	jobID, _ := ingestRes["job_id"].(string)
	eventID, _ := ingestRes["event_id"].(string)

	if jobID == "" || eventID == "" {
		t.Fatalf("expected job_id and event_id, got %+v", ingestRes)
	}

	// Poll job status until DELIVERED
	deadline := time.Now().Add(3 * time.Second)
	var finalStatus string
	for time.Now().Before(deadline) {
		jobResp, err := http.Get(fmt.Sprintf("%s/v1/jobs/%s", env.srv.URL, jobID))
		if err == nil && jobResp.StatusCode == http.StatusOK {
			var body map[string]interface{}
			_ = json.NewDecoder(jobResp.Body).Decode(&body)
			jobResp.Body.Close()
			if jobMap, ok := body["job"].(map[string]interface{}); ok {
				finalStatus, _ = jobMap["status"].(string)
				if finalStatus == string(model.StatusDelivered) {
					break
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	if finalStatus != string(model.StatusDelivered) {
		t.Fatalf("expected job to reach status DELIVERED, got: %s", finalStatus)
	}
	if atomic.LoadInt32(&destinationCalls) != 1 {
		t.Fatalf("expected exactly 1 call to destination, got %d", destinationCalls)
	}
	if receivedEventID != eventID {
		t.Fatalf("expected destination to receive Event-ID %s, got %s", eventID, receivedEventID)
	}
	if string(receivedPayload) != string(payload) {
		t.Fatalf("payload mismatch: got %s, want %s", string(receivedPayload), string(payload))
	}
}

func TestE2E_RetryExhaustion_DLQ_AndReplay(t *testing.T) {
	var shouldFail atomic.Bool
	shouldFail.Store(true)
	var calls atomic.Int32

	destSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if shouldFail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`transient internal server error`))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`success`))
	}))
	defer destSrv.Close()

	// Fast retry policy for testing: 2 attempts max, 50ms interval
	fastPolicy := retry.Policy{
		InitialInterval: 20 * time.Millisecond,
		MaxInterval:     50 * time.Millisecond,
		Multiplier:      1.5,
		MaxAttempts:     2,
	}

	env := setupE2E(t, 2, fastPolicy)

	now := time.Now().UTC()
	timestamp := now.Unix()
	payload := []byte(`{"test":"dlq_and_replay"}`)
	sig := security.Sign(env.tenant.Secret, timestamp, payload)

	req, _ := http.NewRequest(http.MethodPost, env.srv.URL+"/v1/ingest", bytes.NewReader(payload))
	req.Header.Set("X-SentryRelay-Tenant-ID", env.tenant.ID)
	req.Header.Set("X-SentryRelay-Signature", sig)
	req.Header.Set("X-SentryRelay-Timestamp", strconv.FormatInt(timestamp, 10))
	req.Header.Set("X-SentryRelay-Idempotency-Key", "idemp_dlq_replay_1")
	req.Header.Set("X-SentryRelay-Destination-URL", destSrv.URL)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("ingest failed: %v", err)
	}
	defer resp.Body.Close()

	var ingestRes map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&ingestRes)
	jobID, _ := ingestRes["job_id"].(string)

	// Wait until job enters DEAD_LETTER state
	deadline := time.Now().Add(4 * time.Second)
	var finalStatus string
	for time.Now().Before(deadline) {
		jobResp, err := http.Get(fmt.Sprintf("%s/v1/jobs/%s", env.srv.URL, jobID))
		if err == nil && jobResp.StatusCode == http.StatusOK {
			var body map[string]interface{}
			_ = json.NewDecoder(jobResp.Body).Decode(&body)
			jobResp.Body.Close()
			if jobMap, ok := body["job"].(map[string]interface{}); ok {
				finalStatus, _ = jobMap["status"].(string)
				if finalStatus == string(model.StatusDeadLetter) {
					break
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	if finalStatus != string(model.StatusDeadLetter) {
		t.Fatalf("expected job to reach DEAD_LETTER, got %s", finalStatus)
	}

	// Verify DLQ list endpoint
	dlqResp, err := http.Get(env.srv.URL + "/v1/dlq")
	if err != nil || dlqResp.StatusCode != http.StatusOK {
		t.Fatalf("failed to query DLQ: %v", err)
	}
	var dlqBody map[string]interface{}
	_ = json.NewDecoder(dlqResp.Body).Decode(&dlqBody)
	dlqResp.Body.Close()

	count, _ := dlqBody["count"].(float64)
	if count < 1 {
		t.Fatalf("expected at least 1 job in DLQ, got %v", dlqBody)
	}

	// Now fix destination endpoint and replay job
	shouldFail.Store(false)

	replayResp, err := http.Post(fmt.Sprintf("%s/v1/dlq/%s/replay", env.srv.URL, jobID), "application/json", nil)
	if err != nil || replayResp.StatusCode != http.StatusOK {
		t.Fatalf("failed to replay job: %v", err)
	}
	replayResp.Body.Close()

	// Wait until job transitions to DELIVERED
	deliveredDeadline := time.Now().Add(3 * time.Second)
	deliveredStatus := ""
	for time.Now().Before(deliveredDeadline) {
		jobResp, err := http.Get(fmt.Sprintf("%s/v1/jobs/%s", env.srv.URL, jobID))
		if err == nil && jobResp.StatusCode == http.StatusOK {
			var body map[string]interface{}
			_ = json.NewDecoder(jobResp.Body).Decode(&body)
			jobResp.Body.Close()
			if jobMap, ok := body["job"].(map[string]interface{}); ok {
				deliveredStatus, _ = jobMap["status"].(string)
				if deliveredStatus == string(model.StatusDelivered) {
					break
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	if deliveredStatus != string(model.StatusDelivered) {
		t.Fatalf("expected replayed job to reach DELIVERED, got: %s", deliveredStatus)
	}
}
