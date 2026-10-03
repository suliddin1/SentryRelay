package telemetry

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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
func StartMetricsCollector(ctx context.Context, db DB, interval time.Duration) {
	go func() {
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
		return
	}

	statuses := []string{"PENDING", "IN_FLIGHT", "DELIVERED", "RETRY_PENDING", "DEAD_LETTER"}
	for _, st := range statuses {
		QueueDepth.WithLabelValues(st).Set(float64(counts[st]))
	}

	// Calculate total active queue depth for backpressure
	totalActive := counts["PENDING"] + counts["RETRY_PENDING"]
	currentTotalDepth.Store(int64(totalActive))
}

