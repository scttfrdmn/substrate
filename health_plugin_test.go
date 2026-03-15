package substrate_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

func newHealthTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: false})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(0, false)

	p := &substrate.HealthPlugin{}
	if err := p.Initialize(t.Context(), substrate.PluginConfig{ //nolint:contextcheck
		State:  state,
		Logger: logger,
	}); err != nil {
		t.Fatalf("initialize health plugin: %v", err)
	}
	registry.Register(p)

	cfg := substrate.DefaultConfig()
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

func healthRequest(t *testing.T, ts *httptest.Server, op string, body string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/",
		strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AmazonHealthService."+op)
	req.Host = "health.us-east-1.amazonaws.com"
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("health request %s: %v", op, err)
	}
	return resp
}

func TestHealth_DescribeEvents(t *testing.T) {
	ts := newHealthTestServer(t)

	resp := healthRequest(t, ts, "DescribeEvents", `{}`)
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var out map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	events, ok := out["Events"]
	if !ok {
		t.Error("expected Events in response")
	}
	slice, ok := events.([]interface{})
	if !ok || len(slice) != 0 {
		t.Errorf("expected empty Events slice, got %v", events)
	}
}

func TestHealth_DescribeEventDetails(t *testing.T) {
	ts := newHealthTestServer(t)

	resp := healthRequest(t, ts, "DescribeEventDetails", `{"EventArns":["arn:aws:health:us-east-1::event/EC2/EC2_INSTANCE_STOP_SCHEDULED/EC2_INSTANCE_STOP_SCHEDULED_1"]}`)
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var out map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := out["SuccessfulSet"]; !ok {
		t.Error("expected SuccessfulSet in response")
	}
	if _, ok := out["FailedSet"]; !ok {
		t.Error("expected FailedSet in response")
	}
}

func TestHealth_DescribeAffectedEntities(t *testing.T) {
	ts := newHealthTestServer(t)

	resp := healthRequest(t, ts, "DescribeAffectedEntities", `{}`)
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var out map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := out["Entities"]; !ok {
		t.Error("expected Entities in response")
	}
}

func TestHealth_DescribeEventAggregates(t *testing.T) {
	ts := newHealthTestServer(t)

	resp := healthRequest(t, ts, "DescribeEventAggregates", `{}`)
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}
