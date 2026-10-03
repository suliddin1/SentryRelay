package telemetry

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
)

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// Middleware adds slog structured logging, trace IDs, and prometheus metrics for HTTP endpoints.
func Middleware(next http.Handler, endpointName string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		traceID := r.Header.Get("X-Request-ID")
		if traceID == "" {
			traceID = uuid.NewString()
		}

		// Inject traceID into context (can be extracted by handlers if needed)
		// but for now we just use it in the log
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
