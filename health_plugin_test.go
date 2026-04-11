package substrate_test

import (
	"bytes"
	"encoding/json"
	"io"
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

func TestHealth_SeedableEvents(t *testing.T) {
	ts := newHealthTestServer(t)

	// Seed events via control-plane.
	seedPayload, _ := json.Marshal(map[string]interface{}{
		"events": []map[string]interface{}{
			{
				"arn":               "arn:aws:health:us-east-1::event/EC2/issue/123",
				"service":           "EC2",
				"eventTypeCode":     "AWS_EC2_INSTANCE_CONNECTIVITY_ISSUE",
				"eventTypeCategory": "issue",
				"region":            "us-east-1",
				"statusCode":        "open",
				"description":       "Instance connectivity degraded",
			},
		},
	})
	seedReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/health/events", bytes.NewReader(seedPayload))
	seedReq.Header.Set("Content-Type", "application/json")
	seedResp, err := http.DefaultClient.Do(seedReq)
	if err != nil {
		t.Fatalf("seed events: %v", err)
	}
	_, _ = io.ReadAll(seedResp.Body)
	_ = seedResp.Body.Close()
	if seedResp.StatusCode != http.StatusOK {
		t.Fatalf("seed events: got %d", seedResp.StatusCode)
	}

	// DescribeEvents should return the seeded event.
	resp := healthRequest(t, ts, "DescribeEvents", `{}`)
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeEvents: got %d", resp.StatusCode)
	}
	var body map[string]interface{}
	bdata, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(bdata, &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	events, _ := body["Events"].([]interface{})
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	ev, _ := events[0].(map[string]interface{})
	if ev["service"] != "EC2" {
		t.Errorf("want service=EC2, got %v", ev["service"])
	}

	// Clear events.
	delReq, _ := http.NewRequest(http.MethodDelete, ts.URL+"/v1/health/events", nil)
	delResp, _ := http.DefaultClient.Do(delReq)
	_, _ = io.ReadAll(delResp.Body)
	_ = delResp.Body.Close()

	// DescribeEvents should return empty.
	resp2 := healthRequest(t, ts, "DescribeEvents", `{}`)
	defer resp2.Body.Close() //nolint:errcheck
	var body2 map[string]interface{}
	bdata2, _ := io.ReadAll(resp2.Body)
	_ = json.Unmarshal(bdata2, &body2)
	events2, _ := body2["Events"].([]interface{})
	if len(events2) != 0 {
		t.Errorf("want 0 events after clear, got %d", len(events2))
	}
}
