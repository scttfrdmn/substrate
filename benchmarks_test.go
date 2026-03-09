package substrate_test

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
	"github.com/spf13/afero"
)

// BenchmarkEventStore_RecordThroughput measures how many events per second
// the EventStore can record under concurrent load.
func BenchmarkEventStore_RecordThroughput(b *testing.B) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{
		Enabled: true,
		Backend: "memory",
	})
	ctx := context.Background()
	reqCtx := &substrate.RequestContext{
		RequestID: "bench-req",
		AccountID: "123456789012",
		Region:    "us-east-1",
		Timestamp: time.Now(),
	}
	req := &substrate.AWSRequest{
		Service:   "s3",
		Operation: "PutObject",
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}
	resp := &substrate.AWSResponse{
		StatusCode: 200,
		Headers:    map[string]string{},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		req.Operation = fmt.Sprintf("PutObject-%d", i)
		if err := store.RecordRequest(ctx, reqCtx, req, resp, time.Microsecond, 0.000005, nil); err != nil {
			b.Fatalf("RecordRequest: %v", err)
		}
	}
}

// BenchmarkReplayEngine_Replay measures replay throughput over a pre-recorded stream.
func BenchmarkReplayEngine_Replay(b *testing.B) {
	const preloadEvents = 100

	store := substrate.NewEventStore(substrate.EventStoreConfig{
		Enabled: true,
		Backend: "memory",
	})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	registry := substrate.NewPluginRegistry()
	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	ctx := context.Background()

	streamID := "bench-stream"
	reqCtx := &substrate.RequestContext{
		RequestID: "bench-req",
		AccountID: "123456789012",
		Region:    "us-east-1",
		Timestamp: time.Now(),
		Metadata:  map[string]interface{}{"stream_id": streamID},
	}
	req := &substrate.AWSRequest{
		Service:   "iam",
		Operation: "ListUsers",
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}
	for range preloadEvents {
		_ = store.RecordRequest(ctx, reqCtx, req, &substrate.AWSResponse{StatusCode: 200}, time.Microsecond, 0, nil)
	}

	engine := substrate.NewReplayEngine(store, state, tc, registry, substrate.ReplayConfig{}, logger)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		if _, err := engine.Replay(ctx, streamID); err != nil {
			b.Fatalf("Replay: %v", err)
		}
	}
}

// BenchmarkServer_HTTPThroughput measures HTTP request throughput through the
// full server pipeline using httptest (no network I/O).
func BenchmarkServer_HTTPThroughput(b *testing.B) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(slog.LevelError, false)

	registry := substrate.NewPluginRegistry()

	cfg := *substrate.DefaultConfig()
	srv := substrate.NewServer(cfg, registry, store, state, tc, logger)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("X-Amz-Target", "")
		r.Host = "localhost"
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, r)
	}
}

// BenchmarkEventStore_FilterByService measures the performance of GetEvents with
// a Service filter, exercising the in-memory service index.
func BenchmarkEventStore_FilterByService(b *testing.B) {
	const totalEvents = 10_000
	services := []string{"s3", "iam", "dynamodb"}

	store := substrate.NewEventStore(substrate.EventStoreConfig{
		Enabled: true,
		Backend: "memory",
	})
	ctx := context.Background()

	for i := range totalEvents {
		svc := services[i%len(services)]
		_ = store.RecordEvent(ctx, &substrate.Event{
			StreamID:  "bench-stream",
			Service:   svc,
			Operation: "Op",
			Timestamp: time.Now(),
		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		if _, err := store.GetEvents(ctx, substrate.EventFilter{Service: "s3"}); err != nil {
			b.Fatalf("GetEvents: %v", err)
		}
	}
}

// BenchmarkS3PutObject_Latency measures the per-request latency for S3 PutObject
// through the full server pipeline with a real S3Plugin backed by MemMapFs.
func BenchmarkS3PutObject_Latency(b *testing.B) {
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	ctx := context.Background()

	s3p := &substrate.S3Plugin{}
	if err := s3p.Initialize(ctx, substrate.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"filesystem":      afero.NewMemMapFs(),
		},
	}); err != nil {
		b.Fatalf("S3Plugin.Initialize: %v", err)
	}

	registry := substrate.NewPluginRegistry()
	registry.Register(s3p)

	cfg := *substrate.DefaultConfig()
	srv := substrate.NewServer(cfg, registry, store, state, tc, logger)

	// Pre-create the bucket so PUT requests succeed.
	r := httptest.NewRequest(http.MethodPut, "/bench-bucket", nil)
	r.Host = "s3.amazonaws.com"
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/s3/aws4_request")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	body := strings.Repeat("x", 128)

	b.ResetTimer()
	b.ReportAllocs()

	var totalNs int64
	for i := range b.N {
		start := time.Now()
		req := httptest.NewRequest(http.MethodPut,
			fmt.Sprintf("/bench-bucket/key-%d", i),
			strings.NewReader(body))
		req.Host = "s3.amazonaws.com"
		req.Header.Set("Authorization",
			"AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/s3/aws4_request")
		req.ContentLength = int64(len(body))
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		totalNs += time.Since(start).Nanoseconds()
	}

	if b.N > 0 {
		avgNs := totalNs / int64(b.N)
		b.ReportMetric(float64(avgNs), "ns/op")
	}
}
