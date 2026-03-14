package substrate_test

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

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
