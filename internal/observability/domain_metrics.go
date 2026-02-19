package observability

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	ingestRequestsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "duckmesh_ingest_requests_total",
			Help: "Total number of ingest requests.",
		},
	)
	ingestRecordsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "duckmesh_ingest_records_total",
			Help: "Total number of ingest records accepted by API.",
		},
	)
	ingestDuplicatesTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "duckmesh_ingest_duplicates_total",
			Help: "Total number of duplicate ingest records detected by idempotency key.",
		},
	)
	ingestAckLatencyMs = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "duckmesh_ingest_ack_latency_ms",
			Help:    "Ingest API acknowledgment latency in milliseconds.",
			Buckets: []float64{5, 10, 25, 50, 100, 200, 500, 1000, 2000, 5000},
		},
	)
	ingestLagEvents = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "duckmesh_ingest_lag_events",
			Help: "Current count of pending ingest events (accepted + claimed).",
		},
	)
	visibilityLagMs = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "duckmesh_visibility_lag_ms",
			Help: "Current visibility lag in milliseconds, based on oldest pending ingest.",
		},
	)
	latestVisibilityToken = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "duckmesh_latest_visibility_token",
			Help: "Latest published snapshot visibility token.",
		},
	)
	writeToVisibleLatencyMs = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "duckmesh_write_to_visible_latency_ms",
			Help:    "Observed wait latency for min_visibility_token barrier queries in milliseconds.",
			Buckets: []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2000, 5000, 10000},
		},
	)
	consistencyTimeoutTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "duckmesh_consistency_timeout_total",
			Help: "Total number of consistency timeout responses.",
		},
	)
)

func init() {
	prometheus.MustRegister(
		ingestRequestsTotal,
		ingestRecordsTotal,
		ingestDuplicatesTotal,
		ingestAckLatencyMs,
		ingestLagEvents,
		visibilityLagMs,
		latestVisibilityToken,
		writeToVisibleLatencyMs,
		consistencyTimeoutTotal,
	)
}

func ObserveIngestAck(records, duplicates int, elapsed time.Duration) {
	ingestRequestsTotal.Inc()
	if records > 0 {
		ingestRecordsTotal.Add(float64(records))
	}
	if duplicates > 0 {
		ingestDuplicatesTotal.Add(float64(duplicates))
	}
	ingestAckLatencyMs.Observe(float64(elapsed.Milliseconds()))
}

func ObserveWriteToVisibleLatency(elapsed time.Duration) {
	writeToVisibleLatencyMs.Observe(float64(elapsed.Milliseconds()))
}

func IncrementConsistencyTimeout() {
	consistencyTimeoutTotal.Inc()
}

func SetLagMetrics(pendingEvents int64, lagMs int64, latestToken int64) {
	if pendingEvents < 0 {
		pendingEvents = 0
	}
	if lagMs < 0 {
		lagMs = 0
	}
	ingestLagEvents.Set(float64(pendingEvents))
	visibilityLagMs.Set(float64(lagMs))
	latestVisibilityToken.Set(float64(latestToken))
}
