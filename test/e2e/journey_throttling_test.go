package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

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

	// Seed a throttling fault on S3 writes via the control plane. Fault rules are
	// evaluated before the S3 plugin resolves the semantic operation name, so for
	// S3's REST protocol the operation at match time is the HTTP method ("PUT");
	// an empty Operation (match all S3 ops) works too. JSON-protocol services
	// (e.g. DynamoDB "PutItem") match on the semantic name directly.
	seedFaultRules(t, ts, emulator.FaultConfig{
		Enabled: true,
		Rules: []emulator.FaultRule{{
			Service:     "s3",
			Operation:   "PUT",
			FaultType:   "error",
			ErrorCode:   "SlowDown",
			HTTPStatus:  503,
			ErrorMsg:    "Please reduce your request rate.",
			Probability: 1.0,
		}},
	})

	// PutObject now fails with the seeded throttling error.
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("k"),
		Body:   bytes.NewReader([]byte("v")),
	})
	if err == nil {
		t.Fatal("expected seeded throttling error, got nil")
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
