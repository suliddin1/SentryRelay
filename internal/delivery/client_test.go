package delivery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/suliddin1/SentryRelay/internal/model"
	"github.com/suliddin1/SentryRelay/internal/retry"
)

func TestClient_DeliverSuccess(t *testing.T) {
	var receivedHeaderEventID string
	var receivedHeaderAttempt string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeaderEventID = r.Header.Get("X-SentryRelay-Event-ID")
		receivedHeaderAttempt = r.Header.Get("X-SentryRelay-Attempt")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"received":true}`))
	}))
	defer srv.Close()

	client := NewClient(WithTimeout(2 * time.Second))
	job := &model.DeliveryJob{ID: "job_1", AttemptCount: 0}
	event := &model.Event{
		ID:             "evt_100",
		DestinationURL: srv.URL,
		Payload:        []byte(`{"msg":"hello"}`),
	}

	result := client.Deliver(context.Background(), job, event)
	if result.Classification != retry.ClassificationSuccess {
		t.Fatalf("expected ClassificationSuccess, got %v", result.Classification)
	}
	if result.Attempt.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", result.Attempt.StatusCode)
	}
	if receivedHeaderEventID != "evt_100" {
		t.Fatalf("expected X-SentryRelay-Event-ID = evt_100, got %s", receivedHeaderEventID)
	}
	if receivedHeaderAttempt != "1" {
		t.Fatalf("expected X-SentryRelay-Attempt = 1, got %s", receivedHeaderAttempt)
	}
}

func TestClient_DeliverTransientError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`service degraded`))
	}))
	defer srv.Close()

	client := NewClient()
	job := &model.DeliveryJob{ID: "job_2", AttemptCount: 1}
	event := &model.Event{
		ID:             "evt_200",
		DestinationURL: srv.URL,
		Payload:        []byte(`{}`),
	}

	result := client.Deliver(context.Background(), job, event)
	if result.Classification != retry.ClassificationTransient {
		t.Fatalf("expected ClassificationTransient, got %v", result.Classification)
	}
	if result.Attempt.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", result.Attempt.StatusCode)
	}
	if result.Attempt.AttemptNumber != 2 {
		t.Fatalf("expected attempt number 2, got %d", result.Attempt.AttemptNumber)
	}
}

func TestClient_DeliverPermanentError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`invalid json body`))
	}))
	defer srv.Close()

	client := NewClient()
	job := &model.DeliveryJob{ID: "job_3", AttemptCount: 0}
	event := &model.Event{
		ID:             "evt_300",
		DestinationURL: srv.URL,
		Payload:        []byte(`{}`),
	}

	result := client.Deliver(context.Background(), job, event)
	if result.Classification != retry.ClassificationPermanent {
		t.Fatalf("expected ClassificationPermanent, got %v", result.Classification)
	}
	if result.Attempt.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", result.Attempt.StatusCode)
	}
}
