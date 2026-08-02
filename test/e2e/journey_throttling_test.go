package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_SeededThrottling demonstrates seeding a throttling fault through
// the HTTP control plane and observing that the AWS SDK surfaces it — the path a
// consumer's retry logic exists to handle. Substrate fires the fault
// deterministically (no real capacity pressure needed), so the failure path is
// instant and reproducible.
func TestJourney_SeededThrottling(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	// Disable SDK retries so the seeded fault is observed directly rather than
	// silently retried away — the point here is to show the fault surfacing.
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.RetryMaxAttempts = 1
	})
	ctx := context.Background()

	const bucket = "throttle-journey"
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// Seed a throttling fault on S3 writes via the control plane. The operation is
	// the semantic name for every service, S3 included: the request parser resolves
	// an S3 REST request before faults are evaluated (#480), so "PutObject" fires on
	// PutObject alone and not on UploadPart, which is also a PUT. An empty Operation
	// (match all S3 ops) works too.
	//
	// Times is -1 (unlimited) deliberately: the default of one would make the
	// "clear the fault and retry" step below succeed whether or not clearing worked,
	// since the rule would already be spent.
	seedFaultRules(t, ts, emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{{
			Service:     "s3",
			Operation:   "PutObject",
			FaultType:   "error",
			ErrorCode:   "SlowDown",
			HTTPStatus:  503,
			ErrorMsg:    "Please reduce your request rate.",
			Probability: 1.0,
			Times:       -1,
		}},
	})

	// PutObject now fails with the seeded throttling error, and the SDK surfaces the
	// code that was armed. Before #480 the injected error used the wrapped
	// <ErrorResponse> document S3's parser reads no code out of, so this arrived as
	// ServiceUnavailable and a consumer matching on SlowDown never saw their fault.
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("k"),
		Body:   bytes.NewReader([]byte("v")),
	})
	if err == nil {
		t.Fatal("expected seeded throttling error, got nil")
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected a smithy.APIError, got %T: %v", err, err)
	}
	if apiErr.ErrorCode() != "SlowDown" {
		t.Fatalf("ErrorCode() = %q, want SlowDown", apiErr.ErrorCode())
	}

	// The rule reports having fired. A rule that matched nothing produces exactly the
	// same passing test as a consumer's retry working, so the count is what tells
	// those two apart.
	if fired := faultsFired(t, ts); fired != 1 {
		t.Fatalf("fired = %d, want 1", fired)
	}

	// Clear the fault; the same call now succeeds.
	clearFaultRules(t, ts)
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("k"),
		Body:   bytes.NewReader([]byte("v")),
	}); err != nil {
		t.Fatalf("PutObject after clearing fault: %v", err)
	}
}

// seedFaultRules POSTs fault rules to the server's control plane.
func seedFaultRules(t *testing.T, ts *emulator.TestServer, cfg emulator.FaultConfig) {
	t.Helper()
	body, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal fault config: %v", err)
	}
	resp, err := http.Post(ts.URL+"/v1/fault/rules", "application/json", bytes.NewReader(body)) //nolint:noctx
	if err != nil {
		t.Fatalf("POST /v1/fault/rules: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /v1/fault/rules: status %d", resp.StatusCode)
	}
}

// faultsFired GETs the control plane's rule list and totals each rule's fired
// count, which is the assertion that separates a fault that fired from a rule
// that matched nothing.
func faultsFired(t *testing.T, ts *emulator.TestServer) int {
	t.Helper()
	resp, err := http.Get(ts.URL + "/v1/fault/rules") //nolint:noctx
	if err != nil {
		t.Fatalf("GET /v1/fault/rules: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /v1/fault/rules: status %d", resp.StatusCode)
	}
	var cfg emulator.FaultConfig
	if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
		t.Fatalf("decode fault config: %v", err)
	}
	total := 0
	for _, rule := range cfg.Rules {
		total += rule.Fired
	}
	return total
}

// clearFaultRules DELETEs all active fault rules.
func clearFaultRules(t *testing.T, ts *emulator.TestServer) {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, ts.URL+"/v1/fault/rules", nil) //nolint:noctx
	if err != nil {
		t.Fatalf("new DELETE request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE /v1/fault/rules: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("DELETE /v1/fault/rules: status %d", resp.StatusCode)
	}
}
