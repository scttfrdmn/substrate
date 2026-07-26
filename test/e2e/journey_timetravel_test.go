package e2e_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_TimeTravel demonstrates driving time-dependent behavior through
// the simulated clock: pin the clock, write an object, advance the clock a full
// day, write another, and observe that the recorded LastModified timestamps
// differ by the advance — without waiting for real wall-clock time.
func TestJourney_TimeTravel(t *testing.T) {
	ts := emulator.StartTestServer(t)

	// Pin the simulated clock to a known instant for a deterministic baseline.
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	ts.SetTime(base)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	ctx := context.Background()

	const bucket = "timetravel-journey"
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	put := func(key string) {
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte("x")),
		}); err != nil {
			t.Fatalf("PutObject %s: %v", key, err)
		}
	}
	lastModified := func(key string) time.Time {
		head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("HeadObject %s: %v", key, err)
		}
		return aws.ToTime(head.LastModified)
	}

	put("first")
	firstTS := lastModified("first")

	// Jump the simulated clock forward one day, then write again.
	ts.AdvanceTime(24 * time.Hour)
	put("second")
	secondTS := lastModified("second")

	gap := secondTS.Sub(firstTS)
	// HTTP Last-Modified has second granularity; allow a small tolerance.
	if gap < 23*time.Hour || gap > 25*time.Hour {
		t.Fatalf("expected ~24h between object timestamps, got %v (first=%s second=%s)", gap, firstTS, secondTS)
	}
	t.Logf("first=%s second=%s gap=%s", firstTS, secondTS, gap)
}
