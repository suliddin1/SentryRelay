package security

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSSRF_RedirectToPrivate(t *testing.T) {
	// A server that returns a 302 redirect to localhost
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://127.0.0.1:8080/secret", http.StatusFound)
	}))
	defer redirector.Close()

	tpt := http.DefaultTransport.(*http.Transport).Clone()
	tpt.DialContext = SafeDialContext(false) // NO local IPs allowed
	
	client := &http.Client{
		Transport: tpt,
		Timeout:   2 * time.Second,
	}

	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, redirector.URL, nil)
	resp, err := client.Do(req)
	
	if err == nil {
		t.Fatalf("Expected error when redirecting to private IP, got success (status %d)", resp.StatusCode)
	}
	
	t.Logf("Redirect blocked successfully: %v", err)
}
