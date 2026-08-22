package emulator_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
)

func TestStartTestServer_boots(t *testing.T) {
	ts := emulator.StartTestServer(t)
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
	ts := emulator.StartTestServer(t)

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
	ts := emulator.StartTestServer(t)

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
	ts := emulator.StartTestServer(t)

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
	ts := emulator.StartTestServer(t)

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
	ts := emulator.StartTestServer(t)

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

func TestTestServer_SeedSSMParameter(t *testing.T) {
	ts := emulator.StartTestServer(t)

	// Seed a public AMI discovery path.
	ts.SeedSSMParameter("/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64", "ami-0123456789abcdef0")

	// Seed a second parameter via the batch helper.
	ts.SeedSSMParameters(map[string]string{
		"/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id": "ami-ubuntu2404",
	})

	// Verify that GetParameter returns the seeded values via HTTP.
	for _, tc := range []struct {
		name  string
		value string
	}{
		{"/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64", "ami-0123456789abcdef0"},
		{"/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id", "ami-ubuntu2404"},
	} {
		body, _ := json.Marshal(map[string]string{"Name": tc.name})
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(string(body))) //nolint:noctx
		req.Host = "ssm.us-east-1.amazonaws.com"
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "AmazonSSM.GetParameter")
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIATEST12345678901/20260101/us-east-1/ssm/aws4_request, SignedHeaders=host;x-amz-date, Signature=fakesig")
		req.Header.Set("X-Amz-Date", "20260101T000000Z")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GetParameter %s: %v", tc.name, err)
		}
		respBody, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("GetParameter %s: status %d body %s", tc.name, resp.StatusCode, respBody)
			continue
		}

		var result struct {
			Parameter struct {
				Name  string `json:"Name"`
				Value string `json:"Value"`
			} `json:"Parameter"`
		}
		if err := json.Unmarshal(respBody, &result); err != nil {
			t.Fatalf("unmarshal %s: %v", tc.name, err)
		}
		if result.Parameter.Value != tc.value {
			t.Errorf("GetParameter %s: want value %q, got %q", tc.name, tc.value, result.Parameter.Value)
		}
	}
}

// TestTestServer_SeedEC2Image covers the helper a test reaches for when it needs to launch
// from an AMI ID of its own choosing.
//
// RunInstances refuses an AMI it cannot resolve (#733), so an ID a fixture invents no longer
// launches by itself. Seeding is what makes it resolvable, and the unseeded half of this test
// is what proves the seeding — rather than a permissive handler — is doing the work.
func TestTestServer_SeedEC2Image(t *testing.T) {
	ts := emulator.StartTestServer(t)

	const seeded = "ami-0f00dbaadf00d0001"
	const unseeded = "ami-0e11e11e11e11e11e"

	ts.SeedEC2Image(seeded, "fixture-image")

	runInstances := func(imageID string) (int, string) {
		form := "Action=RunInstances&Version=2016-11-15&MinCount=1&MaxCount=1&ImageId=" + imageID
		req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(form)) //nolint:noctx
		req.Host = "ec2.us-east-1.amazonaws.com"
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIATEST12345678901/20260101/us-east-1/ec2/aws4_request, SignedHeaders=host;x-amz-date, Signature=fakesig")
		req.Header.Set("X-Amz-Date", "20260101T000000Z")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("RunInstances %s: %v", imageID, err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return resp.StatusCode, string(body)
	}

	status, body := runInstances(seeded)
	if status != http.StatusOK {
		t.Fatalf("RunInstances from a seeded AMI: status %d body %s", status, body)
	}
	if !strings.Contains(body, seeded) {
		t.Errorf("the launched instance does not report the seeded AMI: %s", body)
	}

	status, body = runInstances(unseeded)
	if status != http.StatusBadRequest {
		t.Fatalf("RunInstances from an unseeded AMI: status %d body %s", status, body)
	}
	if !strings.Contains(body, "InvalidAMIID.NotFound") {
		t.Errorf("want InvalidAMIID.NotFound for an unseeded AMI, got %s", body)
	}

	// The record is a real image record, so DescribeImages lists it.
	form := "Action=DescribeImages&Version=2016-11-15&ImageId.1=" + seeded
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(form)) //nolint:noctx
	req.Host = "ec2.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIATEST12345678901/20260101/us-east-1/ec2/aws4_request, SignedHeaders=host;x-amz-date, Signature=fakesig")
	req.Header.Set("X-Amz-Date", "20260101T000000Z")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DescribeImages: %v", err)
	}
	described, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeImages: status %d body %s", resp.StatusCode, described)
	}
	if !strings.Contains(string(described), "fixture-image") {
		t.Errorf("DescribeImages does not name the seeded image: %s", described)
	}
}

func TestTestServer_Accessors(t *testing.T) {
	ts := emulator.StartTestServer(t)

	if ts.Store() == nil {
		t.Error("Store() is nil; StartTestServer should enable the event store")
	}
	if ts.StateManager() == nil {
		t.Error("StateManager() is nil")
	}
	if ts.TimeController() == nil {
		t.Error("TimeController() is nil")
	}
	if ts.Registry() == nil {
		t.Error("Registry() is nil")
	}
}

// TestTestServer_CostSummaryViaStore exercises the documented cost-inspection
// path (getting-started.md / testing-guide.md): run operations against the
// server, then read the breakdown from ts.Store().GetCostSummary. It guards the
// example from drifting out of sync with the API again.
func TestTestServer_CostSummaryViaStore(t *testing.T) {
	ts := emulator.StartTestServer(t)

	// Create an S3 bucket via the emulator so at least one billable operation is
	// recorded to the event store.
	req, _ := http.NewRequest(http.MethodPut, ts.URL+"/cost-bucket", nil) //nolint:noctx
	req.Host = "cost-bucket.s3.amazonaws.com"
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIATEST12345678901/20260101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=fakesig")
	req.Header.Set("X-Amz-Date", "20260101T000000Z")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT bucket: %v", err)
	}
	_ = resp.Body.Close()

	summary, err := ts.Store().GetCostSummary(context.Background(), "123456789012", time.Time{}, time.Time{})
	if err != nil {
		t.Fatalf("GetCostSummary: %v", err)
	}
	if summary == nil {
		t.Fatal("GetCostSummary returned nil summary")
	}
	// The summary must be reachable and well-formed; a bucket create records an
	// event, so the store is non-empty.
	t.Logf("total cost: $%.6f across %d services", summary.TotalCost, len(summary.ByService))
}

func TestStartTestServer_localstackInfoAlias(t *testing.T) {
	ts := emulator.StartTestServer(t)

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
