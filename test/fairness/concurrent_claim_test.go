package fairness

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/suliddin1/SentryRelay/internal/model"
	"github.com/suliddin1/SentryRelay/internal/storage/sqlite"
)

func TestFairness_ConcurrentClaimJobs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "concurrent_claim.db")
	db, _ := sqlite.Open(path)
	defer db.Close()
	
	err := db.CreateTenant(context.Background(), &model.Tenant{ID: "tenant1"})
	if err != nil { t.Fatal(err) }

	for i := 0; i < 100; i++ {
		ev := &model.Event{
			ID:             uuid.NewString(),
			TenantID:       "tenant1",
			IdempotencyKey: uuid.NewString(),
			DestinationURL: "http://example.com",
Payload: []byte("{}"),
			CreatedAt:      time.Now().UTC(),
		}
		_, _, _, err := db.IngestEvent(context.Background(), ev, 3)
		if err != nil { t.Fatal(err) }
	}

	var wg sync.WaitGroup
	var totalClaimed atomic.Int32
	claimedIDs := make(map[string]bool)
	var mu sync.Mutex
	var duplicateClaims int

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(dispatcherID int) {
			defer wg.Done()
			for {
				jobs, err := db.ClaimJobs(context.Background(), 5, time.Minute, time.Now().UTC().Add(time.Hour))
				if err != nil { continue }
				if len(jobs) == 0 { break }
				
				totalClaimed.Add(int32(len(jobs)))
				
				mu.Lock()
				for _, j := range jobs {
					if claimedIDs[j.ID] { duplicateClaims++ }
					claimedIDs[j.ID] = true
				}
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	if duplicateClaims > 0 { t.Fatalf("CRITICAL BUG: %d jobs were double-claimed!", duplicateClaims) }
	if totalClaimed.Load() != 100 { t.Fatalf("Expected 100 jobs claimed, got %d", totalClaimed.Load()) }
}
