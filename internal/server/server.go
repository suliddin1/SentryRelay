package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/suliddin1/SentryRelay/internal/model"
	"github.com/suliddin1/SentryRelay/internal/ratelimit"
	"github.com/suliddin1/SentryRelay/internal/security"
	"github.com/suliddin1/SentryRelay/internal/storage/sqlite"
	"github.com/suliddin1/SentryRelay/internal/telemetry"
	"golang.org/x/time/rate"
)

// Server coordinates HTTP ingestion and operational diagnostics.
type Server struct {
	db                     *sqlite.DB
	mux                    *http.ServeMux
	tenantLimiter          *ratelimit.TenantLimiter
	maxQueueDepth          int64
	defaultMaxRetry        int
	replayTolerance        time.Duration
	allowLocalDestinations bool
	startedAt              time.Time
}

// Config provides configuration parameters for the HTTP server.
type Config struct {
	DefaultMaxRetry        int
	ReplayTolerance        time.Duration
	AllowLocalDestinations bool
	TenantRateLimit        float64
	TenantBurstLimit       int
	MaxQueueDepth          int64
}

// NewServer initializes HTTP routes for webhook ingestion, health probes, and DLQ management.
func NewServer(cfg Config, db *sqlite.DB) *Server {
	if cfg.DefaultMaxRetry <= 0 {
		cfg.DefaultMaxRetry = 5
	}
	if cfg.ReplayTolerance <= 0 {
		cfg.ReplayTolerance = security.DefaultTimestampTolerance
	}
	if cfg.TenantRateLimit <= 0 {
		cfg.TenantRateLimit = 100 // default 100 req/sec per tenant
	}
	if cfg.TenantBurstLimit <= 0 {
		cfg.TenantBurstLimit = 200
	}
	if cfg.MaxQueueDepth <= 0 {
		cfg.MaxQueueDepth = 100000 // default 100k
	}

	s := &Server{
		db:                     db,
		mux:                    http.NewServeMux(),
		tenantLimiter:          ratelimit.NewTenantLimiter(rate.Limit(cfg.TenantRateLimit), cfg.TenantBurstLimit),
		maxQueueDepth:          cfg.MaxQueueDepth,
		defaultMaxRetry:        cfg.DefaultMaxRetry,
		replayTolerance:        cfg.ReplayTolerance,
		allowLocalDestinations: cfg.AllowLocalDestinations,
		startedAt:              time.Now().UTC(),
	}

	s.routes()
	return s
}

// Handler returns the HTTP handler with all registered endpoints.
func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /readyz", s.handleReadyz)
	s.mux.Handle("POST /v1/ingest", telemetry.Middleware(http.HandlerFunc(s.handleIngest), "ingest"))
	s.mux.HandleFunc("GET /v1/jobs/{id}", s.handleGetJob)
	s.mux.HandleFunc("GET /v1/queue", s.handleListQueue)
	s.mux.HandleFunc("GET /v1/dlq", s.handleListDLQ)
	s.mux.HandleFunc("POST /v1/dlq/{id}/replay", s.handleReplayDLQ)
	s.mux.HandleFunc("GET /v1/status", s.handleStatus)
	s.mux.Handle("GET /metrics", promhttp.Handler())
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if err := s.db.Ping(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "unavailable",
			"error":  err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type IngestEnvelope struct {
	DestinationURL string          `json:"destination_url"`
	Payload        json.RawMessage `json:"payload"`
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	// 0. Backpressure Check
	if telemetry.TotalQueueDepth() >= s.maxQueueDepth {
		w.Header().Set("Retry-After", "30")
		writeError(w, http.StatusServiceUnavailable, "system at capacity: queue depth exceeds limit")
		return
	}

	tenantID := r.Header.Get("X-SentryRelay-Tenant-ID")
	if tenantID == "" {
		writeError(w, http.StatusBadRequest, "missing required header: X-SentryRelay-Tenant-ID")
		return
	}

	// 0.5 Rate Limit Check per Tenant
	if !s.tenantLimiter.Allow(tenantID) {
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusTooManyRequests, "rate limit exceeded for tenant")
		return
	}

	signature := r.Header.Get("X-SentryRelay-Signature")
	if signature == "" {
		writeError(w, http.StatusBadRequest, "missing required header: X-SentryRelay-Signature")
		return
	}

	timestampStr := r.Header.Get("X-SentryRelay-Timestamp")
	if timestampStr == "" {
		writeError(w, http.StatusBadRequest, "missing required header: X-SentryRelay-Timestamp")
		return
	}

	timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid X-SentryRelay-Timestamp: must be unix epoch seconds")
		return
	}

	idempotencyKey := r.Header.Get("X-SentryRelay-Idempotency-Key")
	if idempotencyKey == "" {
		writeError(w, http.StatusBadRequest, "missing required header: X-SentryRelay-Idempotency-Key")
		return
	}

	// 1. Authenticate Tenant
	tenant, err := s.db.GetTenant(r.Context(), tenantID)
	if errors.Is(err, model.ErrTenantNotFound) {
		writeError(w, http.StatusUnauthorized, "invalid tenant")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to query tenant")
		return
	}
	if !tenant.Enabled {
		writeError(w, http.StatusForbidden, "tenant is disabled")
		return
	}

	// 2. Read Body (limit to 2MB to protect against memory exhaustion)
	body, err := io.ReadAll(io.LimitReader(r.Body, 2*1024*1024))
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to read request body")
		return
	}

	// 3. Verify HMAC Signature and Replay Tolerance
	if err := security.Verify(tenant.Secret, signature, timestamp, body, s.replayTolerance, time.Now()); err != nil {
		if errors.Is(err, model.ErrTimestampOutOfRange) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeError(w, http.StatusUnauthorized, "invalid signature")
		return
	}

	// 4. Extract Destination URL and Raw Payload
	destURL := r.Header.Get("X-SentryRelay-Destination-URL")
	payload := body

	// If header not provided, parse envelope from body
	if destURL == "" {
		var env IngestEnvelope
		if err := json.Unmarshal(body, &env); err == nil && env.DestinationURL != "" {
			destURL = env.DestinationURL
			if len(env.Payload) > 0 {
				payload = []byte(env.Payload)
			}
		}
	}

	if destURL == "" {
		writeError(w, http.StatusBadRequest, "destination URL must be provided via header X-SentryRelay-Destination-URL or JSON body")
		return
	}

	if err := security.ValidateDestinationURL(destURL, s.allowLocalDestinations); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Forward any caller-specified forwarding headers
	forwardHeaders := make(map[string]string)
	for k, v := range r.Header {
		if strings.HasPrefix(strings.ToLower(k), "x-forward-") {
			targetKey := strings.TrimPrefix(strings.ToLower(k), "x-forward-")
			if len(v) > 0 {
				forwardHeaders[targetKey] = v[0]
			}
		}
	}

	// 5. Durably Persist Event and Delivery Job in SQLite
	event := &model.Event{
		TenantID:       tenant.ID,
		IdempotencyKey: idempotencyKey,
		DestinationURL: destURL,
		Payload:        payload,
		Headers:        forwardHeaders,
		CreatedAt:      time.Now().UTC(),
	}

	ev, job, duplicate, err := s.db.IngestEvent(r.Context(), event, s.defaultMaxRetry)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("persistence failure: %v", err))
		return
	}

	status := "accepted"
	statusCode := http.StatusAccepted
	if duplicate {
		status = "duplicate"
		statusCode = http.StatusOK
	}

	writeJSON(w, statusCode, map[string]interface{}{
		"event_id": ev.ID,
		"job_id":   job.ID,
		"status":   status,
	})
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if jobID == "" {
		writeError(w, http.StatusBadRequest, "missing job id")
		return
	}

	job, ev, err := s.db.GetJobWithEvent(r.Context(), jobID)
	if errors.Is(err, model.ErrJobNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"job":   job,
		"event": ev,
	})
}

func (s *Server) handleListDLQ(w http.ResponseWriter, r *http.Request) {
	limit := 50
	offset := 0

	if l := r.URL.Query().Get("limit"); l != "" {
		if val, err := strconv.Atoi(l); err == nil && val > 0 {
			limit = val
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if val, err := strconv.Atoi(o); err == nil && val >= 0 {
			offset = val
		}
	}

	jobs, err := s.db.ListDeadLetterJobs(r.Context(), limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if jobs == nil {
		jobs = []*model.DeliveryJob{}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"dead_letter_jobs": jobs,
		"count":            len(jobs),
		"limit":            limit,
		"offset":           offset,
	})
}

func (s *Server) handleReplayDLQ(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if jobID == "" {
		writeError(w, http.StatusBadRequest, "missing job id")
		return
	}

	err := s.db.ReplayDeadLetterJob(r.Context(), jobID, time.Now().UTC())
	if errors.Is(err, model.ErrJobNotFound) {
		writeError(w, http.StatusNotFound, "job not found in DLQ")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"job_id":  jobID,
		"status":  "replayed",
		"message": "job reset to pending for delivery retry",
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	uptime := time.Since(s.startedAt)
	totalDepth := telemetry.TotalQueueDepth()

	// In a real system, active worker count might be queried from the worker pool.
	// Since we don't expose it directly yet, we return the total queue depth and uptime.
	status := map[string]interface{}{
		"status":             "ok",
		"uptime_seconds":     int(uptime.Seconds()),
		"queue_depth_active": totalDepth,
	}

	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handleListQueue(w http.ResponseWriter, r *http.Request) {
	limit := 50
	offset := 0

	if l := r.URL.Query().Get("limit"); l != "" {
		if val, err := strconv.Atoi(l); err == nil && val > 0 {
			limit = val
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if val, err := strconv.Atoi(o); err == nil && val >= 0 {
			offset = val
		}
	}

	jobs, err := s.db.ListQueueJobs(r.Context(), limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if jobs == nil {
		jobs = []*model.DeliveryJob{}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"queue_jobs": jobs,
		"count":      len(jobs),
		"limit":      limit,
		"offset":     offset,
	})
}

func writeJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, statusCode int, message string) {
	writeJSON(w, statusCode, map[string]string{"error": message})
}

