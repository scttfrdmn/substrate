package substrate_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

func TestMetricsCollector_RecordRequest(t *testing.T) {
	m := substrate.NewMetricsCollector()
	m.RecordRequest("s3", "PutObject", false, "")
	m.RecordRequest("s3", "PutObject", false, "")
	m.RecordRequest("s3", "GetObject", true, "NoSuchKey")

	var buf strings.Builder
	m.Render(&buf, nil)
	out := buf.String()

	assert.Contains(t, out, `substrate_requests_total{service="s3",operation="PutObject",status="ok"} 2`)
	assert.Contains(t, out, `substrate_requests_total{service="s3",operation="GetObject",status="error"} 1`)
	assert.Contains(t, out, `substrate_request_errors_total{service="s3",error_code="NoSuchKey"} 1`)
}

func TestMetricsCollector_RecordQuotaHit(t *testing.T) {
	m := substrate.NewMetricsCollector()
	m.RecordQuotaHit("iam", "CreateUser")
	m.RecordQuotaHit("iam", "CreateUser")

	var buf strings.Builder
	m.Render(&buf, nil)
	out := buf.String()

	assert.Contains(t, out, `substrate_quota_hits_total{service="iam",operation="CreateUser"} 2`)
}

func TestMetricsCollector_RecordConsistencyDelay(t *testing.T) {
	m := substrate.NewMetricsCollector()
	m.RecordConsistencyDelay("iam")

	var buf strings.Builder
	m.Render(&buf, nil)
	out := buf.String()

	assert.Contains(t, out, `substrate_consistency_delays_total{service="iam"} 1`)
}

func TestMetricsCollector_RenderTypeLines(t *testing.T) {
	m := substrate.NewMetricsCollector()

	var buf strings.Builder
	m.Render(&buf, nil)
	out := buf.String()

	require.Contains(t, out, "# TYPE substrate_requests_total counter")
	require.Contains(t, out, "# TYPE substrate_request_errors_total counter")
	require.Contains(t, out, "# TYPE substrate_quota_hits_total counter")
	require.Contains(t, out, "# TYPE substrate_consistency_delays_total counter")
}

func TestMetricsCollector_RaceSafe(t *testing.T) {
	m := substrate.NewMetricsCollector()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.RecordRequest("s3", "PutObject", false, "")
			m.RecordQuotaHit("s3", "PutObject")
			m.RecordConsistencyDelay("s3")
		}()
	}
	wg.Wait()

	var buf strings.Builder
	m.Render(&buf, nil)
	// Just ensure no panic and output is non-empty.
	assert.NotEmpty(t, buf.String())
}

func TestMetricsCollector_RecordLatency(t *testing.T) {
	m := substrate.NewMetricsCollector()
	m.RecordLatency("s3", "PutObject", 5*time.Millisecond)
	m.RecordLatency("s3", "PutObject", 20*time.Millisecond)
	m.RecordLatency("s3", "PutObject", 200*time.Millisecond)

	var buf strings.Builder
	m.Render(&buf, nil)
	out := buf.String()

	// Histogram type line must be present.
	require.Contains(t, out, "# TYPE substrate_request_duration_seconds histogram")

	// The 5 ms observation falls in the 0.005 and higher buckets.
	assert.Contains(t, out, `substrate_request_duration_seconds_bucket{service="s3",operation="PutObject",le="0.005"} 1`)
	// The 20 ms observation falls in the 0.025 and higher buckets — cumulative 2 by 0.025.
	assert.Contains(t, out, `substrate_request_duration_seconds_bucket{service="s3",operation="PutObject",le="0.025"} 2`)
	// All three observations are below 1.0 s.
	assert.Contains(t, out, `substrate_request_duration_seconds_bucket{service="s3",operation="PutObject",le="1"} 3`)
	// +Inf must equal total count.
	assert.Contains(t, out, `substrate_request_duration_seconds_bucket{service="s3",operation="PutObject",le="+Inf"} 3`)
	assert.Contains(t, out, `substrate_request_duration_seconds_count{service="s3",operation="PutObject"} 3`)
	assert.Contains(t, out, `substrate_request_duration_seconds_sum{service="s3",operation="PutObject"}`)
}

func TestMetricsCollector_RecordLatency_RaceSafe(t *testing.T) {
	m := substrate.NewMetricsCollector()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.RecordLatency("iam", "CreateUser", 10*time.Millisecond)
		}()
	}
	wg.Wait()

	var buf strings.Builder
	m.Render(&buf, nil)
	assert.Contains(t, buf.String(), `substrate_request_duration_seconds_count{service="iam",operation="CreateUser"} 50`)
}

func TestMetricsCollector_RenderWithStore(t *testing.T) {
	m := substrate.NewMetricsCollector()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})

	var buf strings.Builder
	m.Render(&buf, store)
	out := buf.String()

	// With an empty store, substrate_events_total should be 0.
	assert.Contains(t, out, "substrate_events_total 0")
}
