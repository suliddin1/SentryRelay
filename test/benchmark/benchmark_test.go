package benchmark

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/suliddin1/SentryRelay/internal/model"
	"github.com/suliddin1/SentryRelay/internal/security"
	"github.com/suliddin1/SentryRelay/internal/server"
	"github.com/suliddin1/SentryRelay/internal/storage/sqlite"
)

func BenchmarkSecurity_SignAndVerify(b *testing.B) {
	secret := "bench_secret_key_12345"
	payload := []byte(`{"event":"order.completed","amount":9950,"customer_id":"cust_98765"}`)
	now := time.Now().UTC()
	ts := now.Unix()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sig := security.Sign(secret, ts, payload)
		err := security.Verify(secret, sig, ts, payload, 5*time.Minute, now)
		if err != nil {
			b.Fatalf("verify failed: %v", err)
		}
	}
}

func BenchmarkStorage_IngestEvent(b *testing.B) {
	dir := b.TempDir()
	db, err := sqlite.Open(filepath.Join(dir, "bench_storage.db"))
	if err != nil {
		b.Fatalf("failed to open sqlite: %v", err)
	}
	defer db.Close()

	tenant := &model.Tenant{
		ID:        "tenant_bench",
		Name:      "Bench Corp",
		Secret:    "secret_123",
		Enabled:   true,
		CreatedAt: time.Now().UTC(),
	}
	if err := db.CreateTenant(context.Background(), tenant); err != nil {
		b.Fatalf("failed to create tenant: %v", err)
	}

	payload := []byte(`{"bench":true,"data":"payload_sample_data"}`)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ev := &model.Event{
			TenantID:       tenant.ID,
			IdempotencyKey: uuid.NewString(),
			DestinationURL: "https://example.com/webhook",
			Payload:        payload,
			CreatedAt:      time.Now().UTC(),
		}
		_, _, _, err := db.IngestEvent(ctx, ev, 3)
		if err != nil {
			b.Fatalf("ingest failed: %v", err)
		}
	}
}

func BenchmarkServer_IngestHTTP(b *testing.B) {
	dir := b.TempDir()
	db, err := sqlite.Open(filepath.Join(dir, "bench_server.db"))
	if err != nil {
		b.Fatalf("failed to open sqlite: %v", err)
	}
	defer db.Close()

	tenant := &model.Tenant{
		ID:        "tenant_srv_bench",
		Name:      "Bench Server Corp",
		Secret:    "bench_server_secret_key_456",
		Enabled:   true,
		CreatedAt: time.Now().UTC(),
	}
	if err := db.CreateTenant(context.Background(), tenant); err != nil {
		b.Fatalf("failed to create tenant: %v", err)
	}

	srv := server.NewServer(server.Config{
		DefaultMaxRetry: 3,
		ReplayTolerance: 5 * time.Minute,
	}, db)

	payload := []byte(`{"action":"sync.created","entity_id":"ent_001"}`)
	now := time.Now().UTC()
	ts := now.Unix()
	tsStr := strconv.FormatInt(ts, 10)
	sig := security.Sign(tenant.Secret, ts, payload)

	handler := srv.Handler()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idempKey := uuid.NewString()
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(payload))
		req.Header.Set("X-SentryRelay-Tenant-ID", tenant.ID)
		req.Header.Set("X-SentryRelay-Signature", sig)
		req.Header.Set("X-SentryRelay-Timestamp", tsStr)
		req.Header.Set("X-SentryRelay-Idempotency-Key", idempKey)
		req.Header.Set("X-SentryRelay-Destination-URL", "https://destination.example.com/webhook")

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusAccepted {
			b.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
		}
	}
}
