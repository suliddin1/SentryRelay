package ratelimit

import (
	"sync"

	"golang.org/x/time/rate"
)

// TenantLimiter manages rate limits per tenant.
type TenantLimiter struct {
	mu       sync.RWMutex
	limiters map[string]*rate.Limiter
	r        rate.Limit
	b        int
}

// NewTenantLimiter creates a new tenant rate limiter.
// r is the rate (events per second) and b is the burst size.
func NewTenantLimiter(r rate.Limit, b int) *TenantLimiter {
	return &TenantLimiter{
		limiters: make(map[string]*rate.Limiter),
		r:        r,
		b:        b,
	}
}

// Allow checks if the given tenant is allowed to proceed based on the rate limit.
func (l *TenantLimiter) Allow(tenantID string) bool {
	l.mu.RLock()
	limiter, exists := l.limiters[tenantID]
	l.mu.RUnlock()

	if !exists {
		l.mu.Lock()
		// Double-check after acquiring write lock
		limiter, exists = l.limiters[tenantID]
		if !exists {
			limiter = rate.NewLimiter(l.r, l.b)
			l.limiters[tenantID] = limiter
		}
		l.mu.Unlock()
	}

	return limiter.Allow()
}

// Cleanup removes limiters that haven't been used recently (optional, for memory management if many tenants).
// We skip cleanup for now as the number of tenants is assumed reasonable.
