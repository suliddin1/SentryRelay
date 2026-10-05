package telemetry

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/suliddin1/SentryRelay/internal/model"
)

type fakeDB struct {
	counts map[string]int
	err    error
}

func (f fakeDB) GetQueueDepths(ctx context.Context) (map[string]int, error) {
	return f.counts, f.err
}

// Regression: the collector used uppercase keys ("PENDING") while storage returns the
// lowercase persisted values, so gauges and backpressure depth were always 0.
func TestUpdateQueueDepths_UsesPersistedStatusKeys(t *testing.T) {
	db := fakeDB{counts: map[string]int{
		string(model.StatusPending):      7,
		string(model.StatusRetryPending): 5,
		string(model.StatusInFlight):     3,
		string(model.StatusDelivered):    11,
		string(model.StatusDeadLetter):   2,
	}}

	updateQueueDepths(context.Background(), db)

	if got := TotalQueueDepth(); got != 12 {
		t.Fatalf("TotalQueueDepth = %d, want 12 (pending + retry_pending)", got)
	}
	cases := map[model.DeliveryStatus]float64{
		model.StatusPending:      7,
		model.StatusRetryPending: 5,
		model.StatusInFlight:     3,
		model.StatusDelivered:    11,
		model.StatusDeadLetter:   2,
	}
	for st, want := range cases {
		if got := testutil.ToFloat64(QueueDepth.WithLabelValues(string(st))); got != want {
			t.Errorf("gauge %s = %v, want %v", st, got, want)
		}
	}
}

func TestUpdateQueueDepths_ErrorKeepsLastKnownDepth(t *testing.T) {
	updateQueueDepths(context.Background(), fakeDB{counts: map[string]int{string(model.StatusPending): 42}})
	updateQueueDepths(context.Background(), fakeDB{err: errors.New("database is locked")})

	if got := TotalQueueDepth(); got != 42 {
		t.Fatalf("TotalQueueDepth after error = %d, want last known 42", got)
	}
}

func TestMiddleware_TraceIDPropagation(t *testing.T) {
	var seen string
	h := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = TraceIDFromContext(r.Context())
		w.WriteHeader(http.StatusAccepted)
	}), "other")

	// Caller-supplied ID is propagated to context and echoed back.
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set(RequestIDHeader, "abc-123")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if seen != "abc-123" || rec.Header().Get(RequestIDHeader) != "abc-123" {
		t.Fatalf("ctx=%q header=%q, want abc-123", seen, rec.Header().Get(RequestIDHeader))
	}

	// Missing or oversized IDs are replaced with a generated one.
	for _, in := range []string{"", strings.Repeat("a", maxRequestIDLen+1)} {
		req := httptest.NewRequest(http.MethodGet, "/x", nil)
		if in != "" {
			req.Header.Set(RequestIDHeader, in)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		out := rec.Header().Get(RequestIDHeader)
		if out == "" || out == in || out != seen {
			t.Fatalf("input len %d: got header %q ctx %q, want fresh matching ID", len(in), out, seen)
		}
	}
}
