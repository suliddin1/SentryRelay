package retry

import (
	"math/rand"
	"net/http"
	"time"
)

// Classification categorizes the outcome of a delivery attempt.
type Classification string

const (
	ClassificationSuccess   Classification = "success"
	ClassificationTransient Classification = "transient"
	ClassificationPermanent Classification = "permanent"
)

// Policy defines parameters for retry exponential backoff and jitter.
type Policy struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
	MaxAttempts     int
}

// DefaultPolicy returns a production-ready retry policy.
func DefaultPolicy() Policy {
	return Policy{
		InitialInterval: 500 * time.Millisecond,
		MaxInterval:     30 * time.Second,
		Multiplier:      2.0,
		MaxAttempts:     5,
	}
}

// ClassifyResponse determines whether an HTTP status code or network error represents
// success, a transient retryable failure, or a permanent failure.
func ClassifyResponse(statusCode int, err error) Classification {
	if err != nil {
		// Network errors, timeouts, connection drops are transient
		return ClassificationTransient
	}

	if statusCode >= 200 && statusCode < 300 {
		return ClassificationSuccess
	}

	if statusCode == http.StatusTooManyRequests || (statusCode >= 500 && statusCode <= 599) {
		return ClassificationTransient
	}

	// 4xx client errors (400, 401, 403, 404, 422, etc.) are permanent
	if statusCode >= 400 && statusCode < 500 {
		return ClassificationPermanent
	}

	return ClassificationTransient
}

// BackoffDuration computes an exponential backoff with full jitter for the given attempt number.
// attempt is 1-indexed (attempt 1 is first retry).
func (p Policy) BackoffDuration(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}

	// Calculate exponential interval: initial * (multiplier ^ (attempt - 1))
	multiplier := 1.0
	for i := 1; i < attempt; i++ {
		multiplier *= p.Multiplier
		if float64(p.InitialInterval)*multiplier >= float64(p.MaxInterval) {
			multiplier = float64(p.MaxInterval) / float64(p.InitialInterval)
			break
		}
	}

	interval := time.Duration(float64(p.InitialInterval) * multiplier)
	if interval > p.MaxInterval {
		interval = p.MaxInterval
	}

	// Apply Full Jitter: uniform random value in [0, interval]
	if interval > 0 {
		// global math/rand functions are safe for concurrent use in Go 1.20+
		// Note: p.RandSource is removed/ignored here to avoid custom PRNG data races
		jittered := rand.Int63n(int64(interval) + 1)
		return time.Duration(jittered)
	}

	return interval
}
