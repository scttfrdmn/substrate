# Getting Started with Substrate

Substrate is an event-sourced AWS emulator for deterministic testing of
infrastructure code. This guide gets you from zero to a working Go integration
test in about 15 minutes.

## Prerequisites

- Go 1.21 or later
- Docker (optional — for running Substrate as a standalone server)

## Install

### Docker Compose (recommended for local dev)

The fastest way to run a persistent Substrate server that survives restarts:

```bash
# Clone the repo (includes the config file and compose manifest)
git clone https://github.com/scttfrdmn/substrate
cd substrate

# Start Substrate in the background (pulls image from ghcr.io)
docker compose up -d

# Verify — healthy after ~15 s while SQLite initialises
curl http://localhost:4566/health
# {"status":"ok"}
```

Point any application at `http://localhost:4566`:

```bash
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
```

To change configuration, edit `configs/substrate-local.yaml` and run
`docker compose restart substrate`. To stop and wipe all recorded data:
`docker compose down -v`.

See [`deploy/README.md`](../deploy/README.md) for ECS Fargate and Kubernetes
deployment options. The `substratelocal` wrapper binary also provides a
`localstack`-compatible endpoint for tools that hardcode that name.

### Binary

```bash
go install github.com/scttfrdmn/substrate/cmd/substrate@latest
```

Verify:

```bash
substrate --version
# substrate v0.27.2
```

### Docker

```bash
docker run -p 4566:4566 ghcr.io/scttfrdmn/substrate:latest
```

### Build from source

```bash
git clone https://github.com/scttfrdmn/substrate
cd substrate
make build
./bin/substrate --version
```

## Verify

Start the server (binary or Docker), then confirm it's healthy:

```bash
curl http://localhost:4566/health
# {"status":"ok"}

curl http://localhost:4566/ready
# {"status":"ready","plugins":35}
```

## First 5 minutes: AWS CLI

With Substrate running on port 4566:

```bash
# Create an S3 bucket
aws s3 mb s3://my-test-bucket \
    --endpoint-url http://localhost:4566 \
    --region us-east-1 \
    --no-sign-request

# Upload a file
echo "hello substrate" > /tmp/hello.txt
aws s3 cp /tmp/hello.txt s3://my-test-bucket/hello.txt \
    --endpoint-url http://localhost:4566 \
    --no-sign-request

# Download it back
aws s3 cp s3://my-test-bucket/hello.txt - \
    --endpoint-url http://localhost:4566 \
    --no-sign-request
# hello substrate

# List buckets
aws s3 ls \
    --endpoint-url http://localhost:4566 \
    --no-sign-request
```

Set environment variables to avoid repeating flags:

```bash
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

aws s3 ls
aws dynamodb list-tables
aws iam list-users
```

## First Go integration test

Add Substrate as a test dependency:

```bash
go get github.com/scttfrdmn/substrate
go get github.com/aws/aws-sdk-go-v2/config
go get github.com/aws/aws-sdk-go-v2/credentials
go get github.com/aws/aws-sdk-go-v2/service/s3
```

Create `s3_test.go`:

```go
package myapp_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	substrate "github.com/scttfrdmn/substrate"
)

func TestCreateS3Bucket(t *testing.T) {
	// Start an in-process Substrate server on a random port.
	// t.Cleanup is registered automatically — no defer needed.
	ts := substrate.StartTestServer(t)

	ctx := context.Background()

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithBaseEndpoint(ts.URL),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true // required for emulators
	})

	// Create a bucket.
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("my-test-bucket"),
	})
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	// Put an object.
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("my-test-bucket"),
		Key:    aws.String("hello.txt"),
		Body:   strings.NewReader("hello substrate"),
	})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}

	// Get the object back.
	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("my-test-bucket"),
		Key:    aws.String("hello.txt"),
	})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer result.Body.Close()

	// List objects.
	list, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("my-test-bucket"),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	if len(list.Contents) != 1 {
		t.Fatalf("expected 1 object, got %d", len(list.Contents))
	}
}
```

Run it:

```bash
go test ./... -run TestCreateS3Bucket -v
# --- PASS: TestCreateS3Bucket (0.03s)
# PASS
```

The test starts its own Substrate server, runs entirely in-process with no
network access to real AWS, and shuts down automatically when the test finishes.

## Reusing a server across subtests

Create one `TestServer` and call `ts.ResetState(t)` between subtests to avoid
state leakage without paying the startup cost each time:

```go
func TestS3Operations(t *testing.T) {
	ts := substrate.StartTestServer(t)

	cfg, _ := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithBaseEndpoint(ts.URL),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })

	t.Run("CreateBucket", func(t *testing.T) {
		defer ts.ResetState(t)
		_, err := client.CreateBucket(context.Background(), &s3.CreateBucketInput{
			Bucket: aws.String("bucket-a"),
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("ListBuckets", func(t *testing.T) {
		defer ts.ResetState(t)
		// Clean state — bucket-a from the previous subtest is gone.
		out, err := client.ListBuckets(context.Background(), &s3.ListBucketsInput{})
		if err != nil {
			t.Fatal(err)
		}
		if len(out.Buckets) != 0 {
			t.Fatalf("expected empty bucket list, got %d", len(out.Buckets))
		}
	})
}
```

## Recording and replaying

Use `ReplayEngine` to record a test session and replay it later for
deterministic CI reproduction:

```go
func TestWithRecording(t *testing.T) {
	ts := substrate.StartTestServer(t)

	// Wire up a ReplayEngine against the same store the server uses.
	engine := substrate.NewReplayEngine(
		ts.Store(),           // EventStore
		ts.StateManager(),    // StateManager
		ts.TimeController(),  // TimeController
		ts.Registry(),        // PluginRegistry
		substrate.ReplayConfig{RandomSeed: 42},
		substrate.NewDefaultLogger(slog.LevelError, false),
	)

	ctx := context.Background()
	session, _ := engine.StartRecording(ctx, "my-test")

	// ... run your test against ts.URL ...

	eventCount, _ := engine.StopRecording(ctx, session)
	t.Logf("recorded %d events in stream %s", eventCount, session.StreamID)

	// Replay deterministically.
	results, _ := engine.Replay(ctx, session.StreamID)
	if results.FailedEvents > 0 {
		t.Errorf("replay had %d failures", results.FailedEvents)
	}
}
```

<!-- TODO(#178): expand recording/replay section with full working example -->

## Cost inspection

Substrate tracks real AWS pricing per operation. After running tests, query the
event store directly for a cost breakdown:

```go
func TestCostTracking(t *testing.T) {
	ts := substrate.StartTestServer(t)

	// ... run S3 and Lambda operations against ts ...

	summary, err := ts.Store().GetCostSummary(
		context.Background(),
		"000000000000", // account ID (use your test account)
		time.Time{},    // start (zero = unbounded)
		time.Time{},    // end   (zero = unbounded)
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("total cost: $%.6f", summary.TotalCost)
	for svc, cost := range summary.ByService {
		t.Logf("  %s: $%.6f", svc, cost)
	}
}
```

<!-- TODO(#178): link to Cost Explorer plugin docs once services.md is complete -->

## Next steps

- [Service Reference](services.md) — all 37 plugins with full operation lists
- [Testing Guide](testing-guide.md) — advanced patterns: fault injection,
  multi-region, time-travel debugging, cost assertions
- [Endpoint Configuration](endpoint-configuration.md) — configure Terraform,
  CDK, boto3, and other tools
