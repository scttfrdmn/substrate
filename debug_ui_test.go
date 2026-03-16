package substrate_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate"
)

// newDebugServer creates a minimal Server backed by a populated EventStore for
// debug-UI handler tests.
func newDebugServer(t *testing.T) (*substrate.Server, *substrate.EventStore) {
	t.Helper()
	cfg := substrate.DefaultConfig()
	cfg.EventStore.Enabled = true
	cfg.EventStore.IncludeBodies = true
	cfg.Server.ReadTimeout = "5s"
	cfg.Server.WriteTimeout = "5s"
	cfg.Server.ShutdownTimeout = "1s"

	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	registry := substrate.NewPluginRegistry()
	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig(), substrate.WithTimeController(tc))

	ctx := context.Background()
	if err := substrate.RegisterDefaultPlugins(ctx, registry, state, tc, logger, store, nil); err != nil {
		t.Fatalf("register plugins: %v", err)
	}

	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	return srv, store
}

func TestDebugUI(t *testing.T) {
	srv, _ := newDebugServer(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/ui", nil)
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	ct := rec.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("want text/html Content-Type, got %q", ct)
	}
	if !strings.Contains(rec.Body.String(), "<html") {
		t.Fatal("response body does not contain <html>")
	}
}

func TestDebugEvents_Empty(t *testing.T) {
	srv, _ := newDebugServer(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/debug/events", nil)
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	var resp map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["count"].(float64) != 0 {
		t.Errorf("want count=0, got %v", resp["count"])
	}
}

func TestDebugEvents_WithEvents(t *testing.T) {
	srv, store := newDebugServer(t)

	// Inject two synthetic events.
	ctx := context.Background()
	if err := store.RecordEvent(ctx, &substrate.Event{
		Service:   "s3",
		Operation: "PutObject",
		AccountID: "123456789012",
		Region:    "us-east-1",
		Cost:      0.000005,
		Duration:  10 * time.Millisecond,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordEvent(ctx, &substrate.Event{
		Service:   "sqs",
		Operation: "SendMessage",
		AccountID: "123456789012",
		Region:    "us-east-1",
		Duration:  5 * time.Millisecond,
	}); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/debug/events", nil)
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
	var resp map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if int(resp["count"].(float64)) != 2 {
		t.Errorf("want count=2, got %v", resp["count"])
	}
}

func TestDebugEvents_ServiceFilter(t *testing.T) {
	srv, store := newDebugServer(t)
	ctx := context.Background()

	for _, svc := range []string{"s3", "s3", "sqs"} {
		if err := store.RecordEvent(ctx, &substrate.Event{Service: svc, Operation: "op"}); err != nil {
			t.Fatal(err)
		}
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/debug/events?service=s3", nil)
	srv.ServeHTTP(rec, req)

	var resp map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if int(resp["count"].(float64)) != 2 {
		t.Errorf("want 2 s3 events, got %v", resp["count"])
	}
}

func TestDebugStateAt(t *testing.T) {
	ts := substrate.StartTestServer(t)

	// Send a real SQS CreateQueue request to populate state.
	body := `Action=CreateQueue&QueueName=test-queue-debug&Version=2012-11-05`
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/",
		strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Host = "sqs.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()        //nolint:errcheck
	io.Copy(io.Discard, resp.Body) //nolint:errcheck

	// Query state at sequence 0.
	stateResp, err := http.Get(ts.URL + "/v1/debug/events/0/state") //nolint:noctx
	if err != nil {
		t.Fatal(err)
	}
	defer stateResp.Body.Close() //nolint:errcheck
	if stateResp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", stateResp.StatusCode)
	}
	// The state is valid JSON.
	var state map[string]interface{}
	if err := json.NewDecoder(stateResp.Body).Decode(&state); err != nil {
		t.Fatalf("state is not valid JSON: %v", err)
	}
}

func TestDebugStateDiff(t *testing.T) {
	srv, store := newDebugServer(t)
	ctx := context.Background()

	// Record a base event (seq 0) without body.
	if err := store.RecordEvent(ctx, &substrate.Event{
		Service: "s3", Operation: "ListBuckets",
	}); err != nil {
		t.Fatal(err)
	}

	// Query diff between seq 0 and seq 0 (same → empty diff).
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/debug/state/diff?from=0&to=0", nil)
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d; body: %s", rec.Code, rec.Body.String())
	}
	var diff map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&diff); err != nil {
		t.Fatalf("decode diff: %v", err)
	}
	for _, k := range []string{"added", "removed", "changed"} {
		if _, ok := diff[k]; !ok {
			t.Errorf("diff missing %q key", k)
		}
	}
}

func TestDebugCosts(t *testing.T) {
	srv, store := newDebugServer(t)
	ctx := context.Background()

	if err := store.RecordEvent(ctx, &substrate.Event{
		Service:   "s3",
		Operation: "PutObject",
		AccountID: "123456789012",
		Cost:      0.000005,
	}); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/debug/costs?account=123456789012", nil)
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	var summary map[string]interface{}
	if err := json.NewDecoder(rec.Body).Decode(&summary); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if summary["TotalCost"].(float64) == 0 {
		t.Error("want non-zero TotalCost")
	}
}

func TestDebugExport(t *testing.T) {
	srv, store := newDebugServer(t)

	// Inject an event with a request body so the export has content.
	ctx := context.Background()
	if err := store.RecordEvent(ctx, &substrate.Event{
		Sequence:  0,
		Service:   "sqs",
		Operation: "CreateQueue",
		AccountID: "000000000000",
		Region:    "us-east-1",
		StreamID:  "export-stream",
		Request: &substrate.AWSRequest{
			Service:   "sqs",
			Operation: "CreateQueue",
			Path:      "/",
			Headers:   map[string]string{"Host": "sqs.us-east-1.amazonaws.com"},
			Body:      []byte(`{"Action":"CreateQueue","QueueName":"test"}`),
		},
		Response: &substrate.AWSResponse{StatusCode: 200},
	}); err != nil {
		t.Fatal(err)
	}

	httpSrv := httptest.NewServer(srv)
	t.Cleanup(httpSrv.Close)

	exportResp, err := http.Get(httpSrv.URL + "/v1/debug/export?stream=export-stream") //nolint:noctx
	if err != nil {
		t.Fatal(err)
	}
	defer exportResp.Body.Close() //nolint:errcheck

	if exportResp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", exportResp.StatusCode)
	}
	ct := exportResp.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("want text/plain Content-Type, got %q", ct)
	}
	src, err := io.ReadAll(exportResp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "http.NewRequest") {
		t.Errorf("generated source does not contain http.NewRequest:\n%s", src)
	}
}

func TestDebugExport_MissingStream(t *testing.T) {
	srv, _ := newDebugServer(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/debug/export", nil)
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
}
