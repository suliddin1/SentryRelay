package fairness

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/suliddin1/SentryRelay/internal/model"
	"github.com/suliddin1/SentryRelay/internal/storage/sqlite"
)

func BenchmarkFairness_1000Tenants(b *testing.B) {
	dir := b.TempDir()
	db, _ := sqlite.Open(filepath.Join(dir, "bench_1000.db"))
	defer db.Close()
	
	ctx := context.Background()

	// 1. Insert 1000 tenants, each with 10 jobs
	for i := 0; i < 1000; i++ {
		tenantID := fmt.Sprintf("tenant_%d", i)
		db.CreateTenant(ctx, &model.Tenant{ID: tenantID})
		for j := 0; j < 10; j++ {
			ev := &model.Event{
				TenantID:       tenantID,
				IdempotencyKey: uuid.NewString(),
				DestinationURL: "http://example.com",
				CreatedAt:      time.Now().UTC(),
			}
			db.IngestEvent(ctx, ev, 3)
		}
	}
	
	// Also insert 1 spammer with 100,000 jobs
	db.CreateTenant(ctx, &model.Tenant{ID: "spammer"})
	for j := 0; j < 10000; j++ {
		ev := &model.Event{
			TenantID:       "spammer",
			IdempotencyKey: uuid.NewString(),
			CreatedAt:      time.Now().UTC(),
		}
		db.IngestEvent(ctx, ev, 3)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.ClaimJobs(ctx, 500, time.Minute, time.Now().UTC())
		if err != nil {
			b.Fatalf("claim failed: %v", err)
		}
	}
}
