package telemetry

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/suliddin1/SentryRelay/internal/model"
)

var (
	// Ingestion throughput and latency
	IngestLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sentryrelay_ingest_duration_seconds",
		Help:    "Latency of webhook ingestion requests in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"status"})

	// Delivery attempt latency
	DeliveryLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sentryrelay_delivery_duration_seconds",
		Help:    "Latency of outbound webhook delivery attempts in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"status_code"})

	// Queue depth gauge
	QueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sentryrelay_queue_depth",
		Help: "Current depth of the delivery queue by status",
	}, []string{"status"})

	// Retry and transition counters
	DLQTransitions = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sentryrelay_dlq_transitions_total",
		Help: "Total number of jobs that transitioned to DEAD_LETTER",
	})

	Retries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sentryrelay_retries_total",
		Help: "Total number of retry attempts scheduled",
	})
)

type DB interface {
	GetQueueDepths(ctx context.Context) (map[string]int, error)
}

var (
	currentTotalDepth atomic.Int64
)

// TotalQueueDepth returns the most recently observed total queue depth (PENDING + RETRY_PENDING).
func TotalQueueDepth() int64 {
	return currentTotalDepth.Load()
}

// StartMetricsCollector starts a background goroutine to periodically update DB gauges.
// It performs one collection immediately so backpressure is effective from startup.
func StartMetricsCollector(ctx context.Context, db DB, interval time.Duration) {
	go func() {
		updateQueueDepths(ctx, db)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				updateQueueDepths(ctx, db)
			}
		}
	}()
}

func updateQueueDepths(ctx context.Context, db DB) {
	collectCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	counts, err := db.GetQueueDepths(collectCtx)
	if err != nil {
		// Keep the last known values rather than zeroing them; a transient DB error
		// must not silently disable backpressure.
		slog.Warn("queue depth collection failed", "error", err)
		return
	}

	// Keys must match the persisted status values (model.Status*), which are lowercase.
	statuses := []model.DeliveryStatus{
		model.StatusPending,
		model.StatusInFlight,
		model.StatusDelivered,
		model.StatusRetryPending,
		model.StatusDeadLetter,
	}
	for _, st := range statuses {
		QueueDepth.WithLabelValues(string(st)).Set(float64(counts[string(st)]))
	}

	// Calculate total active queue depth for backpressure
	totalActive := counts[string(model.StatusPending)] + counts[string(model.StatusRetryPending)]
	currentTotalDepth.Store(int64(totalActive))
}
