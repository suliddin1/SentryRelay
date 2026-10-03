package ratelimit

import (
	"sync"
)

// DestinationLimiter manages concurrent delivery limits per destination host.
type DestinationLimiter struct {
	mu       sync.RWMutex
	limiters map[string]chan struct{}
	limit    int
}

// NewDestinationLimiter creates a new concurrency limiter for outbound deliveries.
func NewDestinationLimiter(limit int) *DestinationLimiter {
	return &DestinationLimiter{
		limiters: make(map[string]chan struct{}),
		limit:    limit,
	}
}

// TryAcquire attempts to acquire a concurrency slot for the given host.
// Returns true if acquired, false if at capacity.
func (l *DestinationLimiter) TryAcquire(host string) bool {
	l.mu.RLock()
	sem, exists := l.limiters[host]
	l.mu.RUnlock()

	if !exists {
		l.mu.Lock()
		sem, exists = l.limiters[host]
		if !exists {
			sem = make(chan struct{}, l.limit)
			l.limiters[host] = sem
		}
		l.mu.Unlock()
	}

	select {
	case sem <- struct{}{}:
		return true
	default:
		return false
	}
}

// Release releases a previously acquired concurrency slot for the host.
func (l *DestinationLimiter) Release(host string) {
	l.mu.RLock()
	sem, exists := l.limiters[host]
	l.mu.RUnlock()

	if exists {
		select {
		case <-sem:
			// slot released
		default:
			// should never happen if Release is matched with a successful TryAcquire
		}
	}
}
