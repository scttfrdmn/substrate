package e2e_test

import (
	"bytes"
	"context"
	"io"
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
//
// Two things here are load-bearing, and this test asserted neither before #833.
// [emulator.WithRecordedBodies] is required: [emulator.EventStoreConfig.IncludeBodies]
// defaults to false, a recorded event with no request cannot be re-executed, and a
// replay of such a stream used to report every event as a *success* while executing
// none of them — so this test passed on total=2 success=2 failed=0 having replayed
// nothing. And the proof of execution is the object read back afterwards, not a
// counter: Replay wipes state before re-executing, so an object readable after the
// replay can only have been written by the replay.
func TestJourney_RecordAndReplay(t *testing.T) {
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())

	cfg, err := journeyConfig(ts)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	ctx := context.Background()

	// Run a small workload; every request is recorded to the event store.
	const bucket = "replay-journey-bucket"
	const body = "hello substrate"
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("hello.txt"),
		Body:   bytes.NewReader([]byte(body)),
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
	// The assertions that make the two above mean something: every event carried a
	// request, so every event was re-executed rather than skipped.
	if results.SkippedEvents != 0 {
		t.Errorf("replay skipped %d events (want 0); a skipped event was never re-executed",
			results.SkippedEvents)
	}
	if results.SuccessEvents != results.TotalEvents {
		t.Errorf("replay re-executed %d of %d events (want all)",
			results.SuccessEvents, results.TotalEvents)
	}

	// And the direct evidence: the replay reset state first, so reading the object
	// back proves the replayed PutObject actually ran.
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String("hello.txt"),
	})
	if err != nil {
		t.Fatalf("GetObject after replay: %v", err)
	}
	defer out.Body.Close() //nolint:errcheck // read-only body in a test assertion
	got, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("read replayed object: %v", err)
	}
	if string(got) != body {
		t.Errorf("replayed object body = %q, want %q", got, body)
	}

	t.Logf("replay total=%d success=%d failed=%d skipped=%d differences=%d",
		results.TotalEvents, results.SuccessEvents, results.FailedEvents,
		results.SkippedEvents, len(results.Differences))
}
