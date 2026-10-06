package delivery

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/suliddin1/SentryRelay/internal/security"

	"github.com/google/uuid"
	"github.com/suliddin1/SentryRelay/internal/model"
	"github.com/suliddin1/SentryRelay/internal/retry"
)

// Client is responsible for outbound webhook delivery to destination URLs.
type Client struct {
	httpClient *http.Client
	userAgent  string
}

// ClientOption allows configuring the delivery client.
type ClientOption func(*Client)

// WithTimeout sets a custom HTTP client timeout.
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.httpClient.Timeout = timeout
	}
}

// WithHTTPClient allows passing a custom net/http Client (useful for testing).
func WithHTTPClient(httpClient *http.Client) ClientOption {
	return func(c *Client) {
		c.httpClient = httpClient
	}
}

// WithSSRFProtection enables strict SSRF protection during dialing.
func WithSSRFProtection(allowLocal bool) ClientOption {
	return func(c *Client) {
		t, ok := c.httpClient.Transport.(*http.Transport)
		if !ok || t == nil {
			t = http.DefaultTransport.(*http.Transport).Clone()
		}
		t.DialContext = security.SafeDialContext(allowLocal)
		c.httpClient.Transport = t
	}
}
// NewClient initializes a delivery client with sensible production defaults.
func NewClient(opts ...ClientOption) *Client {
	c := &Client{
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		userAgent: "SentryRelay/1.0",
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

// Result encapsulates the outcome of a delivery execution.
type Result struct {
	Attempt        *model.DeliveryAttempt
	Classification retry.Classification
	Err            error
}

// Deliver executes an outbound HTTP POST to the destination specified in the event.
func (c *Client) Deliver(ctx context.Context, job *model.DeliveryJob, event *model.Event) Result {
	attemptNum := job.AttemptCount + 1
	start := time.Now()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, event.DestinationURL, bytes.NewReader(event.Payload))
	if err != nil {
		durationMs := time.Since(start).Milliseconds()
		return Result{
			Attempt: &model.DeliveryAttempt{
				ID:                  uuid.NewString(),
				JobID:               job.ID,
				AttemptNumber:       attemptNum,
				StatusCode:          0,
				ExecutionDurationMs: durationMs,
				ErrorMessage:        fmt.Sprintf("failed to construct request: %v", err),
				CreatedAt:           start.UTC(),
			},
			Classification: retry.ClassificationPermanent,
			Err:            err,
		}
	}

	// Propagate custom headers
	for k, v := range event.Headers {
		req.Header.Set(k, v)
	}

	// Set standard SentryRelay delivery metadata headers
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("X-SentryRelay-Event-ID", event.ID)
	req.Header.Set("X-SentryRelay-Attempt", fmt.Sprintf("%d", attemptNum))

	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	durationMs := time.Since(start).Milliseconds()

	if err != nil {
		return Result{
			Attempt: &model.DeliveryAttempt{
				ID:                  uuid.NewString(),
				JobID:               job.ID,
				AttemptNumber:       attemptNum,
				StatusCode:          0,
				ExecutionDurationMs: durationMs,
				ErrorMessage:        err.Error(),
				CreatedAt:           start.UTC(),
			},
			Classification: retry.ClassifyResponse(0, err),
			Err:            err,
		}
	}
	defer resp.Body.Close()

	// Drain response body up to 4KB to read error messages if any
	bodySample, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	var errMsg string
	if resp.StatusCode >= 400 {
		errMsg = fmt.Sprintf("HTTP %d: %s", resp.StatusCode, string(bytes.TrimSpace(bodySample)))
	}

	// Drain the remainder of the body to ensure the HTTP connection can be reused
	_, _ = io.Copy(io.Discard, resp.Body)

	classification := retry.ClassifyResponse(resp.StatusCode, nil)

	return Result{
		Attempt: &model.DeliveryAttempt{
			ID:                  uuid.NewString(),
			JobID:               job.ID,
			AttemptNumber:       attemptNum,
			StatusCode:          resp.StatusCode,
			ExecutionDurationMs: durationMs,
			ErrorMessage:        errMsg,
			CreatedAt:           start.UTC(),
		},
		Classification: classification,
		Err:            nil,
	}
}
