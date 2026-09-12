package emulator_test

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
)

// BenchmarkEventStore_RecordThroughput measures how many events per second
// the EventStore can record under concurrent load.
func BenchmarkEventStore_RecordThroughput(b *testing.B) {
	store := emulator.NewEventStore(emulator.EventStoreConfig{
		Enabled: true,
		Backend: "memory",
	})
	ctx := context.Background()
	reqCtx := &emulator.RequestContext{
		RequestID: "bench-req",
		AccountID: "123456789012",
		Region:    "us-east-1",
		Timestamp: time.Now(),
	}
	req := &emulator.AWSRequest{
		Service:   "s3",
		Operation: "PutObject",
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}
	resp := &emulator.AWSResponse{
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

	store := emulator.NewEventStore(emulator.EventStoreConfig{
		Enabled: true,
		Backend: "memory",
	})
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Now())
	registry := emulator.NewPluginRegistry()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	ctx := context.Background()

	streamID := "bench-stream"
	reqCtx := &emulator.RequestContext{
		RequestID: "bench-req",
		AccountID: "123456789012",
		Region:    "us-east-1",
		Timestamp: time.Now(),
		Metadata:  map[string]interface{}{"stream_id": streamID},
	}
	req := &emulator.AWSRequest{
		Service:   "iam",
		Operation: "ListUsers",
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}
	for range preloadEvents {
		_ = store.RecordRequest(ctx, reqCtx, req, &emulator.AWSResponse{StatusCode: 200}, time.Microsecond, 0, nil)
	}

	engine := emulator.NewReplayEngine(store, state, tc, registry, emulator.ReplayConfig{}, logger)

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
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := emulator.NewMemoryStateManager()
	tc := emulator.NewTimeController(time.Now())
	logger := emulator.NewDefaultLogger(slog.LevelError, false)

	registry := emulator.NewPluginRegistry()

	cfg := *emulator.DefaultConfig()
	srv := emulator.NewServer(cfg, registry, store, state, tc, logger)

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

	store := emulator.NewEventStore(emulator.EventStoreConfig{
		Enabled: true,
		Backend: "memory",
	})
	ctx := context.Background()

	for i := range totalEvents {
		svc := services[i%len(services)]
		_ = store.RecordEvent(ctx, &emulator.Event{
			StreamID:  "bench-stream",
			Service:   svc,
			Operation: "Op",
			Timestamp: time.Now(),
		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		if _, err := store.GetEvents(ctx, emulator.EventFilter{Service: "s3"}); err != nil {
			b.Fatalf("GetEvents: %v", err)
		}
	}
}

// BenchmarkS3PutObject_Latency measures the per-request latency for S3 PutObject
// against a server started by the test harness, over loopback HTTP.
//
// It drives [emulator.StartTestServer] rather than a hand-rolled server and an
// httptest recorder, which is the shape a downstream consumer benchmarks in: the
// measured path includes the transport, so an uncached read costs an HTTP round
// trip here as it does in production instead of a map lookup (#605). Every harness
// entry point takes testing.TB, so a *testing.B reaches all of it — this benchmark
// is the standing proof of that.
func BenchmarkS3PutObject_Latency(b *testing.B) {
	ts := emulator.StartTestServer(b)
	defer ts.ResetState(b)

	client := &http.Client{}
	defer client.CloseIdleConnections()

	// put issues one path-style S3 request. The Host header is what routes a
	// request to the S3 plugin, and the credential is any key at all: a test
	// server accepts unverified signatures.
	put := func(path, body string) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPut,
			ts.URL+path, strings.NewReader(body))
		if err != nil {
			b.Fatalf("new request: %v", err)
		}
		req.Host = "s3.amazonaws.com"
		req.Header.Set("Authorization",
			"AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20260101/us-east-1/s3/aws4_request")
		req.ContentLength = int64(len(body))
		resp, err := client.Do(req)
		if err != nil {
			b.Fatalf("PUT %s: %v", path, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b.Fatalf("PUT %s: unexpected status %d", path, resp.StatusCode)
		}
	}

	// Pre-create the bucket so the measured PUTs succeed.
	put("/bench-bucket", "")

	body := strings.Repeat("x", 128)

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		put(fmt.Sprintf("/bench-bucket/key-%d", i), body)
	}
}
