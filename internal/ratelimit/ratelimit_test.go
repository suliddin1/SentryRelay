package ratelimit

import (
	"golang.org/x/time/rate"
	"testing"
)

func TestTenantLimiter(t *testing.T) {
	// 5 events per second, burst 5
	limiter := NewTenantLimiter(rate.Limit(5), 5)

	tenantID := "test_tenant"

	// First 5 should succeed
	for i := 0; i < 5; i++ {
		if !limiter.Allow(tenantID) {
			t.Errorf("expected attempt %d to be allowed", i)
		}
	}

	// 6th should fail (rate limit exceeded)
	if limiter.Allow(tenantID) {
		t.Errorf("expected 6th attempt to be rate limited")
	}

	// Another tenant should still be allowed
	if !limiter.Allow("other_tenant") {
		t.Errorf("expected other_tenant to be allowed")
	}
}

func TestDestinationLimiter(t *testing.T) {
	limiter := NewDestinationLimiter(2)
	host := "example.com"

	// Acquire 1
	if !limiter.TryAcquire(host) {
		t.Fatalf("expected to acquire 1st slot")
	}

	// Acquire 2
	if !limiter.TryAcquire(host) {
		t.Fatalf("expected to acquire 2nd slot")
	}

	// Acquire 3 should fail
	if limiter.TryAcquire(host) {
		t.Fatalf("expected 3rd slot to fail")
	}

	// Release 1
	limiter.Release(host)

	// Acquire should succeed again
	if !limiter.TryAcquire(host) {
		t.Fatalf("expected to acquire slot after release")
	}
}
