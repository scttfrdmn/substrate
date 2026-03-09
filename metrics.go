package substrate

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// defaultLatencyBuckets are the upper-bound values (in seconds) used for the
// substrate_request_duration_seconds histogram, covering the typical HTTP
// latency range from 1 ms to 10 s.
var defaultLatencyBuckets = []float64{
	0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
}

// MetricsCollector gathers in-process counters, gauges, and histograms for
// the Substrate server pipeline. It emits Prometheus text-format v0.0.4
// output via Render. All methods are safe for concurrent use.
type MetricsCollector struct {
	mu                sync.RWMutex
	requestsTotal     map[string]int64   // "service\toperation\tstatus"
	errorsTotal       map[string]int64   // "service\terrorCode"
	quotaHitsTotal    map[string]int64   // "service\toperation"
	consistencyDelays map[string]int64   // "service"
	latencyBuckets    []float64          // upper bounds in seconds (immutable after construction)
	latencyHist       map[string][]int64 // "svc\top" → per-bucket cumulative counts (+Inf at end)
	latencySum        map[string]float64 // "svc\top" → sum of observed seconds
	latencyCount      map[string]int64   // "svc\top" → observation count
}

// NewMetricsCollector creates an empty MetricsCollector ready to collect
// request, error, quota, consistency, and latency metrics.
func NewMetricsCollector() *MetricsCollector {
	buckets := make([]float64, len(defaultLatencyBuckets))
	copy(buckets, defaultLatencyBuckets)
	return &MetricsCollector{
		requestsTotal:     make(map[string]int64),
		errorsTotal:       make(map[string]int64),
		quotaHitsTotal:    make(map[string]int64),
		consistencyDelays: make(map[string]int64),
		latencyBuckets:    buckets,
		latencyHist:       make(map[string][]int64),
		latencySum:        make(map[string]float64),
		latencyCount:      make(map[string]int64),
	}
}

// RecordRequest increments the requests counter. When isError is true and
// errorCode is non-empty the error counter is also incremented.
func (m *MetricsCollector) RecordRequest(service, operation string, isError bool, errorCode string) {
	status := "ok"
	if isError {
		status = "error"
	}
	key := service + "\t" + operation + "\t" + status
	m.mu.Lock()
	m.requestsTotal[key]++
	if isError && errorCode != "" {
		ekKey := service + "\t" + errorCode
		m.errorsTotal[ekKey]++
	}
	m.mu.Unlock()
}

// RecordQuotaHit increments the quota hit counter for the given
// service/operation pair.
func (m *MetricsCollector) RecordQuotaHit(service, operation string) {
	key := service + "\t" + operation
	m.mu.Lock()
	m.quotaHitsTotal[key]++
	m.mu.Unlock()
}

// RecordConsistencyDelay increments the consistency delay counter for the
// given service.
func (m *MetricsCollector) RecordConsistencyDelay(service string) {
	m.mu.Lock()
	m.consistencyDelays[service]++
	m.mu.Unlock()
}

// RecordLatency records the duration of a completed request for the
// substrate_request_duration_seconds histogram. It is safe for concurrent use.
func (m *MetricsCollector) RecordLatency(service, operation string, d time.Duration) {
	secs := d.Seconds()
	key := service + "\t" + operation

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.latencyHist[key]; !ok {
		m.latencyHist[key] = make([]int64, len(m.latencyBuckets)+1)
	}
	buckets := m.latencyHist[key]
	// Find the smallest bucket that contains the observation and increment
	// only that slot. Render computes a running prefix sum for Prometheus output.
	slotted := false
	for i, bound := range m.latencyBuckets {
		if secs <= bound {
			buckets[i]++
			slotted = true
			break
		}
	}
	if !slotted {
		// Observation exceeds all upper bounds; place in +Inf slot.
		buckets[len(m.latencyBuckets)]++
	}
	m.latencySum[key] += secs
	m.latencyCount[key]++
}

// Render writes Prometheus text-format v0.0.4 metrics to w. It calls
// store.GetCostSummary to produce aggregate cost gauges when store is
// non-nil. Write errors are silently swallowed because the underlying
// io.Writer is always an http.ResponseWriter in production and a
// strings.Builder in tests — neither returns meaningful errors after
// WriteHeader has been called.
func (m *MetricsCollector) Render(w io.Writer, store *EventStore) {
	m.mu.RLock()
	reqs := copyInt64Map(m.requestsTotal)
	errs := copyInt64Map(m.errorsTotal)
	quotas := copyInt64Map(m.quotaHitsTotal)
	consis := copyInt64Map(m.consistencyDelays)
	// Copy histogram data under the read lock.
	latBuckets := make([]float64, len(m.latencyBuckets))
	copy(latBuckets, m.latencyBuckets)
	latHist := make(map[string][]int64, len(m.latencyHist))
	for k, v := range m.latencyHist {
		cp := make([]int64, len(v))
		copy(cp, v)
		latHist[k] = cp
	}
	latSum := make(map[string]float64, len(m.latencySum))
	for k, v := range m.latencySum {
		latSum[k] = v
	}
	latCount := copyInt64Map(m.latencyCount)
	m.mu.RUnlock()

	mw := &metricsWriter{w: w}

	// substrate_requests_total
	mw.line("# HELP substrate_requests_total Total number of AWS API requests handled by Substrate.")
	mw.line("# TYPE substrate_requests_total counter")
	for key, val := range reqs {
		parts := strings.SplitN(key, "\t", 3)
		if len(parts) != 3 {
			continue
		}
		mw.printf("substrate_requests_total{service=%q,operation=%q,status=%q} %d\n",
			parts[0], parts[1], parts[2], val)
	}

	// substrate_request_errors_total
	mw.line("# HELP substrate_request_errors_total Total number of AWS API errors by error code.")
	mw.line("# TYPE substrate_request_errors_total counter")
	for key, val := range errs {
		parts := strings.SplitN(key, "\t", 2)
		if len(parts) != 2 {
			continue
		}
		mw.printf("substrate_request_errors_total{service=%q,error_code=%q} %d\n",
			parts[0], parts[1], val)
	}

	// substrate_quota_hits_total
	mw.line("# HELP substrate_quota_hits_total Total number of requests rejected by quota enforcement.")
	mw.line("# TYPE substrate_quota_hits_total counter")
	for key, val := range quotas {
		parts := strings.SplitN(key, "\t", 2)
		if len(parts) != 2 {
			continue
		}
		mw.printf("substrate_quota_hits_total{service=%q,operation=%q} %d\n",
			parts[0], parts[1], val)
	}

	// substrate_consistency_delays_total
	mw.line("# HELP substrate_consistency_delays_total Total number of requests delayed by consistency simulation.")
	mw.line("# TYPE substrate_consistency_delays_total counter")
	for service, val := range consis {
		mw.printf("substrate_consistency_delays_total{service=%q} %d\n", service, val)
	}

	// substrate_request_duration_seconds histogram.
	if len(latHist) > 0 {
		mw.line("# HELP substrate_request_duration_seconds Request latency distribution in seconds.")
		mw.line("# TYPE substrate_request_duration_seconds histogram")
		keys := make([]string, 0, len(latHist))
		for k := range latHist {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, key := range keys {
			parts := strings.SplitN(key, "\t", 2)
			if len(parts) != 2 {
				continue
			}
			svc, op := parts[0], parts[1]
			buckets := latHist[key]
			var cumulative int64
			for i, bound := range latBuckets {
				cumulative += buckets[i]
				mw.printf("substrate_request_duration_seconds_bucket{service=%q,operation=%q,le=%q} %d\n",
					svc, op, formatFloat(bound), cumulative)
			}
			// +Inf bucket.
			total := latCount[key]
			mw.printf("substrate_request_duration_seconds_bucket{service=%q,operation=%q,le=\"+Inf\"} %d\n",
				svc, op, total)
			mw.printf("substrate_request_duration_seconds_sum{service=%q,operation=%q} %g\n",
				svc, op, latSum[key])
			mw.printf("substrate_request_duration_seconds_count{service=%q,operation=%q} %d\n",
				svc, op, total)
		}
	}

	// Cost gauges from event store.
	if store != nil {
		summary, err := store.GetCostSummary(context.Background(), "", time.Time{}, time.Time{})
		if err == nil {
			mw.line("# HELP substrate_cost_usd_total Estimated total AWS cost in USD by service.")
			mw.line("# TYPE substrate_cost_usd_total gauge")
			for svc, cost := range summary.ByService {
				mw.printf("substrate_cost_usd_total{service=%q} %g\n", svc, cost)
			}

			mw.line("# HELP substrate_events_total Total number of events recorded in the store.")
			mw.line("# TYPE substrate_events_total gauge")
			mw.printf("substrate_events_total %d\n", summary.RequestCount)
		}
	}
}

// metricsWriter wraps an io.Writer and discards write errors because the
// underlying writer is always either an http.ResponseWriter or a
// strings.Builder — both are safe to write to without error checking.
type metricsWriter struct {
	w io.Writer
}

func (mw *metricsWriter) line(s string) {
	_, _ = fmt.Fprintln(mw.w, s)
}

func (mw *metricsWriter) printf(format string, args ...any) {
	_, _ = fmt.Fprintf(mw.w, format, args...)
}

// copyInt64Map returns a shallow copy of src.
func copyInt64Map(src map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

// formatFloat formats a float64 for use as a Prometheus histogram le label,
// omitting trailing zeros while keeping enough precision to distinguish
// standard bucket boundaries (e.g. 0.001, 0.005, 0.01).
func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}
