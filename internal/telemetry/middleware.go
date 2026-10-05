package telemetry

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// RequestIDHeader is the header used to accept and echo a request correlation ID.
const RequestIDHeader = "X-Request-ID"

// maxRequestIDLen bounds caller-supplied IDs so they cannot bloat every log line.
const maxRequestIDLen = 128

type traceIDKey struct{}

// TraceIDFromContext returns the request trace ID injected by Middleware, or "".
func TraceIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(traceIDKey{}).(string)
	return id
}

type responseWriter struct {
	http.ResponseWriter
	statusCode  int
	wroteHeader bool
}

func (rw *responseWriter) WriteHeader(code int) {
	if !rw.wroteHeader {
		rw.statusCode = code
		rw.wroteHeader = true
	}
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if !rw.wroteHeader {
		rw.wroteHeader = true
	}
	return rw.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (rw *responseWriter) Unwrap() http.ResponseWriter {
	return rw.ResponseWriter
}

// Middleware adds slog structured logging, trace IDs, and prometheus metrics for HTTP endpoints.
// The trace ID is taken from X-Request-ID when present (and reasonably sized), otherwise generated.
// It is echoed in the response header and stored in the request context.
func Middleware(next http.Handler, endpointName string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		traceID := r.Header.Get(RequestIDHeader)
		if traceID == "" || len(traceID) > maxRequestIDLen {
			traceID = uuid.NewString()
		}

		w.Header().Set(RequestIDHeader, traceID)
		r = r.WithContext(context.WithValue(r.Context(), traceIDKey{}, traceID))

		logger := slog.With("trace_id", traceID, "method", r.Method, "path", r.URL.Path)
		logger.Info("http request started")

		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(rw, r)

		duration := time.Since(start)

		logger.Info("http request completed",
			"status", rw.statusCode,
			"duration_ms", duration.Milliseconds(),
		)

		if endpointName == "ingest" {
			statusStr := "success"
			if rw.statusCode >= 400 && rw.statusCode < 500 {
				statusStr = "client_error"
			} else if rw.statusCode >= 500 {
				statusStr = "server_error"
			}
			IngestLatency.WithLabelValues(statusStr).Observe(duration.Seconds())
		}
	})
}
