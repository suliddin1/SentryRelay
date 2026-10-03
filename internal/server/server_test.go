package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/suliddin1/SentryRelay/internal/model"
	"github.com/suliddin1/SentryRelay/internal/security"
	"github.com/suliddin1/SentryRelay/internal/storage/sqlite"
)

func setupTestServer(t *testing.T) (*Server, *sqlite.DB, *model.Tenant) {
	t.Helper()
	dir := t.TempDir()
	db, err := sqlite.Open(filepath.Join(dir, "server_test.db"))
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	tenant := &model.Tenant{
		ID:        "tenant_srv_test",
		Name:      "Test Org",
		Secret:    "super_secret_signing_key_456",
		Enabled:   true,
		CreatedAt: time.Now().UTC(),
	}
	if err := db.CreateTenant(context.Background(), tenant); err != nil {
		t.Fatalf("failed to create tenant: %v", err)
	}

	srv := NewServer(Config{
		DefaultMaxRetry:        3,
		ReplayTolerance:        5 * time.Minute,
		AllowLocalDestinations: true,
	}, db)

	return srv, db, tenant
}

func TestServer_HealthAndReady(t *testing.T) {
	srv, _, _ := setupTestServer(t)

	// Healthz
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from healthz, got %d", rec.Code)
	}

	// Readyz
	reqReady := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recReady := httptest.NewRecorder()
	srv.Handler().ServeHTTP(recReady, reqReady)
	if recReady.Code != http.StatusOK {
		t.Fatalf("expected 200 from readyz, got %d", recReady.Code)
	}
}

func TestServer_IngestValidAndDuplicate(t *testing.T) {
	srv, _, tenant := setupTestServer(t)

	now := time.Now().UTC()
	timestamp := now.Unix()
	payload := []byte(`{"event":"payment_intent.succeeded","amount":4200}`)
	sig := security.Sign(tenant.Secret, timestamp, payload)

	makeRequest := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(payload))
		req.Header.Set("X-SentryRelay-Tenant-ID", tenant.ID)
		req.Header.Set("X-SentryRelay-Signature", sig)
		req.Header.Set("X-SentryRelay-Timestamp", strconv.FormatInt(timestamp, 10))
		req.Header.Set("X-SentryRelay-Idempotency-Key", "idemp_order_4200")
		req.Header.Set("X-SentryRelay-Destination-URL", "https://api.example.com/webhook")
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		return rec
	}

	// 1. Initial ingestion
	rec1 := makeRequest()
	if rec1.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted, got %d: %s", rec1.Code, rec1.Body.String())
	}
	var res1 map[string]interface{}
	_ = json.Unmarshal(rec1.Body.Bytes(), &res1)
	if res1["status"] != "accepted" {
		t.Fatalf("expected status=accepted, got %v", res1["status"])
	}

	// 2. Duplicate ingestion
	rec2 := makeRequest()
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for duplicate, got %d: %s", rec2.Code, rec2.Body.String())
	}
	var res2 map[string]interface{}
	_ = json.Unmarshal(rec2.Body.Bytes(), &res2)
	if res2["status"] != "duplicate" {
		t.Fatalf("expected status=duplicate, got %v", res2["status"])
	}
	if res1["event_id"] != res2["event_id"] {
		t.Fatalf("expected same event_id on duplicate: got %v vs %v", res1["event_id"], res2["event_id"])
	}
}

func TestServer_IngestSecurityFailures(t *testing.T) {
	srv, _, tenant := setupTestServer(t)
	now := time.Now().UTC()
	payload := []byte(`{"test":true}`)

	// 1. Invalid signature
	reqBadSig := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(payload))
	reqBadSig.Header.Set("X-SentryRelay-Tenant-ID", tenant.ID)
	reqBadSig.Header.Set("X-SentryRelay-Signature", "invalid_signature_hex")
	reqBadSig.Header.Set("X-SentryRelay-Timestamp", strconv.FormatInt(now.Unix(), 10))
	reqBadSig.Header.Set("X-SentryRelay-Idempotency-Key", "idemp_1")
	reqBadSig.Header.Set("X-SentryRelay-Destination-URL", "https://example.com")
	recBadSig := httptest.NewRecorder()
	srv.Handler().ServeHTTP(recBadSig, reqBadSig)
	if recBadSig.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized for bad signature, got %d", recBadSig.Code)
	}

	// 2. Expired timestamp (> 5 minutes)
	expiredTimestamp := now.Add(-10 * time.Minute).Unix()
	expiredSig := security.Sign(tenant.Secret, expiredTimestamp, payload)
	reqExpired := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(payload))
	reqExpired.Header.Set("X-SentryRelay-Tenant-ID", tenant.ID)
	reqExpired.Header.Set("X-SentryRelay-Signature", expiredSig)
	reqExpired.Header.Set("X-SentryRelay-Timestamp", strconv.FormatInt(expiredTimestamp, 10))
	reqExpired.Header.Set("X-SentryRelay-Idempotency-Key", "idemp_2")
	reqExpired.Header.Set("X-SentryRelay-Destination-URL", "https://example.com")
	recExpired := httptest.NewRecorder()
	srv.Handler().ServeHTTP(recExpired, reqExpired)
	if recExpired.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for expired timestamp, got %d", recExpired.Code)
	}

	// 3. SSRF metadata address blocked
	metadataTimestamp := now.Unix()
	metadataSig := security.Sign(tenant.Secret, metadataTimestamp, payload)
	reqSSRF := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(payload))
	reqSSRF.Header.Set("X-SentryRelay-Tenant-ID", tenant.ID)
	reqSSRF.Header.Set("X-SentryRelay-Signature", metadataSig)
	reqSSRF.Header.Set("X-SentryRelay-Timestamp", strconv.FormatInt(metadataTimestamp, 10))
	reqSSRF.Header.Set("X-SentryRelay-Idempotency-Key", "idemp_ssrf")
	reqSSRF.Header.Set("X-SentryRelay-Destination-URL", "http://169.254.169.254/latest/meta-data/")
	recSSRF := httptest.NewRecorder()
	srv.Handler().ServeHTTP(recSSRF, reqSSRF)
	if recSSRF.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for SSRF cloud metadata URL, got %d", recSSRF.Code)
	}
}
