package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_CostGate demonstrates using Substrate's cost tracking as a
// machine-gradeable guardrail: run a workload, read the recorded cost from the
// event store, and fail the test if it exceeds a budget — before any real spend.
func TestJourney_CostGate(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	ctx := context.Background()

	const bucket = "cost-journey"
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// A modest workload: write 50 small objects.
	for i := range 50 {
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(fmt.Sprintf("obj-%d", i)),
			Body:   bytes.NewReader([]byte("payload")),
		}); err != nil {
			t.Fatalf("PutObject %d: %v", i, err)
		}
	}

	// Read the recorded cost from the SAME store the server wrote to.
	// Zero start/end times mean "unbounded" (all recorded events).
	summary, err := ts.Store().GetCostSummary(ctx, journeyAccountID, time.Time{}, time.Time{})
	if err != nil {
		t.Fatalf("GetCostSummary: %v", err)
	}

	// Gate: fail if the workload would cost more than the budget.
	const maxCost = 1.00 // USD
	if summary.TotalCost > maxCost {
		t.Errorf("workload cost $%.6f exceeds budget $%.2f", summary.TotalCost, maxCost)
		for svc, cost := range summary.ByService {
			t.Logf("  %s: $%.6f", svc, cost)
		}
	}
	t.Logf("workload cost: $%.6f across %d requests", summary.TotalCost, summary.RequestCount)
}
