package retry

import (
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestClassifyResponse(t *testing.T) {
	cases := []struct {
		statusCode int
		err        error
		expected   Classification
	}{
		{http.StatusOK, nil, ClassificationSuccess},
		{http.StatusCreated, nil, ClassificationSuccess},
		{http.StatusAccepted, nil, ClassificationSuccess},
		{http.StatusNoContent, nil, ClassificationSuccess},
		{http.StatusTooManyRequests, nil, ClassificationTransient},
		{http.StatusInternalServerError, nil, ClassificationTransient},
		{http.StatusBadGateway, nil, ClassificationTransient},
		{http.StatusServiceUnavailable, nil, ClassificationTransient},
		{http.StatusGatewayTimeout, nil, ClassificationTransient},
		{0, errors.New("dial tcp: connection refused"), ClassificationTransient},
		{0, errors.New("context deadline exceeded"), ClassificationTransient},
		{http.StatusBadRequest, nil, ClassificationPermanent},
		{http.StatusUnauthorized, nil, ClassificationPermanent},
		{http.StatusForbidden, nil, ClassificationPermanent},
		{http.StatusNotFound, nil, ClassificationPermanent},
		{http.StatusUnprocessableEntity, nil, ClassificationPermanent},
	}

	for _, tc := range cases {
		got := ClassifyResponse(tc.statusCode, tc.err)
		if got != tc.expected {
			t.Errorf("ClassifyResponse(%d, %v) = %v; want %v", tc.statusCode, tc.err, got, tc.expected)
		}
	}
}

func TestBackoffDuration(t *testing.T) {
	p := Policy{
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:     1600 * time.Millisecond,
		Multiplier:      2.0,
		MaxAttempts:     5,
	}

	for attempt := 1; attempt <= 10; attempt++ {
		dur := p.BackoffDuration(attempt)
		if dur < 0 {
			t.Errorf("attempt %d: negative duration %v", attempt, dur)
		}
		if dur > p.MaxInterval {
			t.Errorf("attempt %d: duration %v exceeds max interval %v", attempt, dur, p.MaxInterval)
		}
	}
}
