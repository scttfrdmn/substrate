package substrate_test

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

func TestStartTestServer_boots(t *testing.T) {
	ts := substrate.StartTestServer(t)
	if ts.URL == "" {
		t.Fatal("URL is empty")
	}
	if ts.Port == 0 {
		t.Fatal("Port is zero")
	}

	resp, err := http.Get(ts.URL + "/health") //nolint:noctx
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health status %d", resp.StatusCode)
	}
}

func TestStartTestServer_localstackHealth(t *testing.T) {
	ts := substrate.StartTestServer(t)

	resp, err := http.Get(ts.URL + "/_localstack/health") //nolint:noctx
	if err != nil {
		t.Fatalf("GET /_localstack/health: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)

	var payload struct {
		Services map[string]string `json:"services"`
		Version  string            `json:"version"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("unmarshal: %v — body: %s", err, body)
	}
	if len(payload.Services) == 0 {
		t.Fatal("services map is empty")
	}
	// Spot-check a few well-known services.
	for _, svc := range []string{"s3", "dynamodb", "lambda"} {
		if payload.Services[svc] != "available" {
			t.Errorf("service %q: got %q, want %q", svc, payload.Services[svc], "available")
		}
	}
}

func TestStartTestServer_stateReset(t *testing.T) {
	ts := substrate.StartTestServer(t)

	// Create an S3 bucket via the emulator.
	createBucket := func(bucket string) int {
		req, _ := http.NewRequest(http.MethodPut, ts.URL+"/"+bucket, nil) //nolint:noctx
		req.Host = bucket + ".s3.amazonaws.com"
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIATEST12345678901/20260101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=fakesig")
		req.Header.Set("X-Amz-Date", "20260101T000000Z")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}

	_ = createBucket("my-test-bucket")

	// Reset state.
	ts.ResetState(t)

	// After reset the bucket should be gone; a HEAD should return 404.
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/my-test-bucket?list-type=2", nil) //nolint:noctx
	req.Host = "my-test-bucket.s3.amazonaws.com"
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIATEST12345678901/20260101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=fakesig")
	req.Header.Set("X-Amz-Date", "20260101T000000Z")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET after reset: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("after reset got status %d, want 404", resp.StatusCode)
	}
}

func TestTestServer_AdvanceTime(t *testing.T) {
	ts := substrate.StartTestServer(t)

	// Read baseline simulated time via HTTP.
	resp, err := http.Get(ts.URL + "/v1/control/time") //nolint:noctx
	if err != nil {
		t.Fatalf("GET /v1/control/time: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	var before map[string]any
	if err := json.Unmarshal(body, &before); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	beforeTime, err := time.Parse(time.RFC3339Nano, before["simulated_time"].(string))
	if err != nil {
		t.Fatalf("parse before time: %v", err)
	}

	// Advance by 24 hours in-process.
	ts.AdvanceTime(24 * time.Hour)

	// Read again via HTTP.
	resp2, err := http.Get(ts.URL + "/v1/control/time") //nolint:noctx
	if err != nil {
		t.Fatalf("GET /v1/control/time after advance: %v", err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	_ = resp2.Body.Close()

	var after map[string]any
	if err := json.Unmarshal(body2, &after); err != nil {
		t.Fatalf("unmarshal after: %v", err)
	}
	afterTime, err := time.Parse(time.RFC3339Nano, after["simulated_time"].(string))
	if err != nil {
		t.Fatalf("parse after time: %v", err)
	}

	diff := afterTime.Sub(beforeTime)
	if diff < 23*time.Hour || diff > 25*time.Hour {
		t.Errorf("expected ~24h advance, got %v", diff)
	}
}

func TestTestServer_SetTime(t *testing.T) {
	ts := substrate.StartTestServer(t)

	target := time.Date(2030, 6, 1, 12, 0, 0, 0, time.UTC)
	ts.SetTime(target)

	resp, err := http.Get(ts.URL + "/v1/control/time") //nolint:noctx
	if err != nil {
		t.Fatalf("GET /v1/control/time: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	simTime, err := time.Parse(time.RFC3339Nano, payload["simulated_time"].(string))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !strings.HasPrefix(simTime.UTC().Format(time.RFC3339), "2030-06-01") {
		t.Errorf("expected 2030-06-01, got %s", simTime)
	}
}

func TestTestServer_SetScale(t *testing.T) {
	ts := substrate.StartTestServer(t)

	ts.SetScale(7200)

	resp, err := http.Get(ts.URL + "/v1/control/time") //nolint:noctx
	if err != nil {
		t.Fatalf("GET /v1/control/time: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	scale, ok := payload["scale"].(float64)
	if !ok || scale != 7200 {
		t.Errorf("expected scale 7200, got %v", payload["scale"])
	}
}

func TestStartTestServer_localstackInfoAlias(t *testing.T) {
	ts := substrate.StartTestServer(t)

	resp, err := http.Get(ts.URL + "/_localstack/info") //nolint:noctx
	if err != nil {
		t.Fatalf("GET /_localstack/info: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"services"`) {
		t.Errorf("response missing services key: %s", body)
	}
}
