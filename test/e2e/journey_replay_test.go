package e2e_test

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// TestJourney_RecordAndReplay demonstrates that AWS calls are recorded as an
// immutable event stream and can be replayed deterministically — the basis for
// reproducing a run exactly in CI. The ReplayEngine is built over the same
// store/state/time/registry the server uses (via the TestServer accessors).
//
// HTTP requests are recorded to the "default" stream (the stream a request maps
// to comes from its RequestContext); replaying that stream re-executes the whole
// recorded session through the plugin registry.
func TestJourney_RecordAndReplay(t *testing.T) {
	ts := emulator.StartTestServer(t)

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	ctx := context.Background()

	// Run a small workload; every request is recorded to the event store.
	const bucket = "replay-journey-bucket"
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("hello.txt"),
		Body:   bytes.NewReader([]byte("hello substrate")),
	}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	// Build a ReplayEngine over the same components the server uses, then replay
	// the recorded session deterministically.
	engine := emulator.NewReplayEngine(
		ts.Store(),
		ts.StateManager(),
		ts.TimeController(),
		ts.Registry(),
		emulator.ReplayConfig{RandomSeed: 42, StopOnError: true},
		emulator.NewDefaultLogger(slog.LevelError, false),
	)

	results, err := engine.Replay(ctx, "default")
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if results.TotalEvents == 0 {
		t.Fatal("expected recorded events to replay, got 0")
	}
	if results.FailedEvents != 0 {
		t.Errorf("replay had %d failed events (want 0)", results.FailedEvents)
	}
	t.Logf("replay total=%d success=%d failed=%d",
		results.TotalEvents, results.SuccessEvents, results.FailedEvents)
}
