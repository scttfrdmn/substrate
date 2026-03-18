package substrate_test

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

func newCETestServer(t *testing.T) *httptest.Server {
	t.Helper()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(0, false)

	p := &substrate.CEPlugin{}
	if err := p.Initialize(t.Context(), substrate.PluginConfig{ //nolint:contextcheck
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"event_store": store,
		},
	}); err != nil {
		t.Fatalf("initialize ce plugin: %v", err)
	}
	registry.Register(p)

	cfg := substrate.DefaultConfig()
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

// newCEWithEC2TestServer builds a server with both EC2 and CE plugins sharing
// the same state and TimeController.  Returns the server and the TC so tests
// can advance simulated time.
func newCEWithEC2TestServer(t *testing.T) (*httptest.Server, *substrate.TimeController) {
	t.Helper()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(0, false)

	ec2p := &substrate.EC2Plugin{}
	if err := ec2p.Initialize(t.Context(), substrate.PluginConfig{ //nolint:contextcheck
		State:  state,
		Logger: logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize ec2 plugin: %v", err)
	}
	registry.Register(ec2p)

	cep := &substrate.CEPlugin{}
	if err := cep.Initialize(t.Context(), substrate.PluginConfig{ //nolint:contextcheck
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"event_store":     store,
			"time_controller": tc,
		},
	}); err != nil {
		t.Fatalf("initialize ce plugin: %v", err)
	}
	registry.Register(cep)

	cfg := substrate.DefaultConfig()
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts, tc
}

// ec2QueryRequest sends an EC2 query-protocol POST to the given server.
func ec2QueryRequest(t *testing.T, ts *httptest.Server, params map[string]string) *http.Response {
	t.Helper()
	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("build ec2 request: %v", err)
	}
	req.Host = "ec2.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("ec2 request: %v", err)
	}
	return resp
}

func ceRequest(t *testing.T, ts *httptest.Server, op string, body interface{}) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal ce request: %v", err)
	}
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSInsightsIndexService."+op)
	req.Host = "ce.us-east-1.amazonaws.com"
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("ce request %s: %v", op, err)
	}
	return resp
}

func TestCE_GetCostAndUsage(t *testing.T) {
	ts := newCETestServer(t)

	resp := ceRequest(t, ts, "GetCostAndUsage", map[string]interface{}{
		"TimePeriod":  map[string]string{"Start": "2026-01-01", "End": "2026-02-01"},
		"Granularity": "MONTHLY",
		"Metrics":     []string{"UnblendedCost"},
	})
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var out map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := out["ResultsByTime"]; !ok {
		t.Error("expected ResultsByTime in response")
	}
	if _, ok := out["DimensionValueAttributes"]; !ok {
		t.Error("expected DimensionValueAttributes in response")
	}
}

func TestCE_GetCostForecast(t *testing.T) {
	ts := newCETestServer(t)

	resp := ceRequest(t, ts, "GetCostForecast", map[string]interface{}{
		"TimePeriod":  map[string]string{"Start": "2026-02-01", "End": "2026-03-01"},
		"Metric":      "UNBLENDED_COST",
		"Granularity": "MONTHLY",
	})
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var out map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := out["Total"]; !ok {
		t.Error("expected Total in response")
	}
	if _, ok := out["ForecastResultsByTime"]; !ok {
		t.Error("expected ForecastResultsByTime in response")
	}

	total, ok := out["Total"].(map[string]interface{})
	if !ok {
		t.Fatal("Total is not an object")
	}
	if _, ok := total["Amount"]; !ok {
		t.Error("expected Amount in Total")
	}
	if unit, _ := total["Unit"].(string); unit != "USD" {
		t.Errorf("expected Unit=USD, got %q", unit)
	}
}

func TestCE_GetDimensionValues(t *testing.T) {
	ts := newCETestServer(t)

	resp := ceRequest(t, ts, "GetDimensionValues", map[string]interface{}{
		"TimePeriod": map[string]string{"Start": "2026-01-01", "End": "2026-02-01"},
		"Dimension":  "SERVICE",
	})
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var out map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := out["DimensionValues"]; !ok {
		t.Error("expected DimensionValues in response")
	}
}

func TestCE_UnsupportedOperation(t *testing.T) {
	ts := newCETestServer(t)

	resp := ceRequest(t, ts, "UnknownOp", map[string]interface{}{})
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

// TestCE_EC2UsageCost_RunningInstance verifies that a running m7i.large instance
// accrues compute cost visible in GetCostAndUsage.
func TestCE_EC2UsageCost_RunningInstance(t *testing.T) {
	baseline := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ts, tc := newCEWithEC2TestServer(t)
	tc.SetTime(baseline)

	// Launch an m7i.large instance.
	runResp := ec2QueryRequest(t, ts, map[string]string{
		"Action":       "RunInstances",
		"ImageId":      "ami-12345678",
		"InstanceType": "m7i.large",
		"MinCount":     "1",
		"MaxCount":     "1",
	})
	defer runResp.Body.Close() //nolint:errcheck
	if runResp.StatusCode != http.StatusOK {
		t.Fatalf("RunInstances: expected 200, got %d", runResp.StatusCode)
	}

	// Advance time by 30 hours — m7i.large $0.192/hr × 30h = $5.76.
	tc.SetTime(baseline.Add(30 * time.Hour))

	// Query CE for the period covering the run.
	resp := ceRequest(t, ts, "GetCostAndUsage", map[string]interface{}{
		"TimePeriod":  map[string]string{"Start": "2026-01-01", "End": "2026-02-01"},
		"Granularity": "MONTHLY",
		"Metrics":     []string{"UnblendedCost"},
	})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GetCostAndUsage: expected 200, got %d", resp.StatusCode)
	}

	var out struct {
		ResultsByTime []struct {
			Total struct {
				UnblendedCost struct {
					Amount string `json:"Amount"`
				} `json:"UnblendedCost"`
			} `json:"Total"`
			Groups []struct {
				Keys    []string                     `json:"Keys"`
				Metrics map[string]map[string]string `json:"Metrics"`
			} `json:"Groups"`
		} `json:"ResultsByTime"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out.ResultsByTime) == 0 {
		t.Fatal("expected ResultsByTime to have at least one entry")
	}

	// Find the EC2 group.
	const ec2Key = "Amazon Elastic Compute Cloud - Compute"
	var ec2Amount string
	for _, g := range out.ResultsByTime[0].Groups {
		if len(g.Keys) > 0 && g.Keys[0] == ec2Key {
			ec2Amount = g.Metrics["UnblendedCost"]["Amount"]
		}
	}
	if ec2Amount == "" || ec2Amount == "0.000000" {
		t.Errorf("expected non-zero EC2 cost group, got %q; groups: %+v", ec2Amount, out.ResultsByTime[0].Groups)
	}

	// The total must also be non-zero.
	totalAmount := out.ResultsByTime[0].Total.UnblendedCost.Amount
	if totalAmount == "" || totalAmount == "0.000000" {
		t.Errorf("expected non-zero total UnblendedCost, got %q", totalAmount)
	}
}

// TestCE_EC2UsageCost_TerminatedInstance verifies that a terminated instance
// only accrues cost up to its termination time.
func TestCE_EC2UsageCost_TerminatedInstance(t *testing.T) {
	baseline := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	ts, tc := newCEWithEC2TestServer(t)
	tc.SetTime(baseline)

	// Launch an instance.
	runResp := ec2QueryRequest(t, ts, map[string]string{
		"Action":       "RunInstances",
		"ImageId":      "ami-12345678",
		"InstanceType": "m7i.large",
		"MinCount":     "1",
		"MaxCount":     "1",
	})
	defer runResp.Body.Close() //nolint:errcheck
	if runResp.StatusCode != http.StatusOK {
		t.Fatalf("RunInstances: expected 200, got %d", runResp.StatusCode)
	}

	// Parse the instance ID.
	var runResult struct {
		XMLName   xml.Name `xml:"RunInstancesResponse"`
		Instances []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"instancesSet>item"`
	}
	// Re-read: the body was already drained by the status check above, so re-run.
	runResp2 := ec2QueryRequest(t, ts, map[string]string{
		"Action":       "DescribeInstances",
	})
	defer runResp2.Body.Close() //nolint:errcheck
	var descResult struct {
		Reservations []struct {
			Instances []struct {
				InstanceID string `xml:"instanceId"`
			} `xml:"instancesSet>item"`
		} `xml:"reservationSet>item"`
	}
	if err := xml.NewDecoder(runResp2.Body).Decode(&descResult); err != nil {
		t.Fatalf("describe: %v", err)
	}
	_ = runResult
	if len(descResult.Reservations) == 0 || len(descResult.Reservations[0].Instances) == 0 {
		t.Fatal("no instances found")
	}
	instID := descResult.Reservations[0].Instances[0].InstanceID

	// Advance 10 hours, then terminate.
	tc.SetTime(baseline.Add(10 * time.Hour))
	termResp := ec2QueryRequest(t, ts, map[string]string{
		"Action":      "TerminateInstances",
		"InstanceId.1": instID,
	})
	defer termResp.Body.Close() //nolint:errcheck

	// Advance another 20 hours — cost should NOT include these extra hours.
	tc.SetTime(baseline.Add(30 * time.Hour))

	resp := ceRequest(t, ts, "GetCostAndUsage", map[string]interface{}{
		"TimePeriod":  map[string]string{"Start": "2026-02-01", "End": "2026-03-01"},
		"Granularity": "MONTHLY",
		"Metrics":     []string{"UnblendedCost"},
	})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GetCostAndUsage: expected 200, got %d", resp.StatusCode)
	}

	var out struct {
		ResultsByTime []struct {
			Groups []struct {
				Keys    []string                     `json:"Keys"`
				Metrics map[string]map[string]string `json:"Metrics"`
			} `json:"Groups"`
		} `json:"ResultsByTime"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out.ResultsByTime) == 0 {
		t.Fatal("no results")
	}

	const ec2Key = "Amazon Elastic Compute Cloud - Compute"
	// m7i.large $0.192/hr × 10h = $1.92; must be < $5.76 (30h full run).
	for _, g := range out.ResultsByTime[0].Groups {
		if len(g.Keys) > 0 && g.Keys[0] == ec2Key {
			amount := g.Metrics["UnblendedCost"]["Amount"]
			if amount == "" || amount == "0.000000" {
				t.Errorf("expected non-zero EC2 cost, got %q", amount)
			}
			// Parse and bound-check.
			var cost float64
			if err := json.Unmarshal([]byte(amount), &cost); err == nil {
				if cost >= 5.76 {
					t.Errorf("terminated instance should not accrue 30h cost; got %.4f, want < 5.76", cost)
				}
				if cost < 1.8 {
					t.Errorf("expected ~1.92 USD for 10h m7i.large, got %.4f", cost)
				}
			}
			return
		}
	}
	t.Errorf("EC2 group %q not found in groups: %+v", ec2Key, out.ResultsByTime[0].Groups)
}
