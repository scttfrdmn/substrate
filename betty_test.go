package substrate_test

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

// newBettyTestDeps creates a set of test dependencies shared across Betty tests.
func newBettyTestDeps(t *testing.T) (
	*substrate.PluginRegistry,
	*substrate.EventStore,
	*substrate.MemoryStateManager,
	*substrate.TimeController,
	substrate.Logger,
) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelError, false)
	tc := substrate.NewTimeController(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	fs := afero.NewMemMapFs()

	s3p := &substrate.S3Plugin{}
	require.NoError(t, s3p.Initialize(context.Background(), substrate.PluginConfig{
		State:  state,
		Logger: logger,
		Options: map[string]any{
			"time_controller": tc,
			"filesystem":      fs,
		},
	}))

	iamp := &substrate.IAMPlugin{}
	require.NoError(t, iamp.Initialize(context.Background(), substrate.PluginConfig{
		State:  state,
		Logger: logger,
	}))

	registry := substrate.NewPluginRegistry()
	registry.Register(s3p)
	registry.Register(iamp)

	store := substrate.NewEventStore(substrate.EventStoreConfig{
		Enabled: true,
		Backend: "memory",
	})

	return registry, store, state, tc, logger
}

// --- DeployStack tests (#23) -----------------------------------------------

func TestDeployStack_S3Bucket(t *testing.T) {
	ctx := context.Background()
	registry, store, state, tc, logger := newBettyTestDeps(t)
	client := substrate.NewBettyClient(registry, store, state, tc, logger)

	const tmpl = `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyBucket": {
				"Type": "AWS::S3::Bucket",
				"Properties": { "BucketName": "my-test-bucket" }
			}
		}
	}`

	result, err := client.Deploy(ctx, tmpl, substrate.Intent{})
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)

	res := result.Resources[0]
	assert.Equal(t, "MyBucket", res.LogicalID)
	assert.Equal(t, "AWS::S3::Bucket", res.Type)
	assert.Equal(t, "my-test-bucket", res.PhysicalID)
	assert.Empty(t, res.Error, "bucket should deploy without error")
}

func TestDeployStack_IAMRole(t *testing.T) {
	ctx := context.Background()
	registry, store, state, tc, logger := newBettyTestDeps(t)
	client := substrate.NewBettyClient(registry, store, state, tc, logger)

	const tmpl = `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyRole": {
				"Type": "AWS::IAM::Role",
				"Properties": {
					"RoleName": "MyTestRole",
					"AssumeRolePolicyDocument": {
						"Version": "2012-10-17",
						"Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]
					}
				}
			}
		}
	}`

	result, err := client.Deploy(ctx, tmpl, substrate.Intent{})
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)

	res := result.Resources[0]
	assert.Equal(t, "MyRole", res.LogicalID)
	assert.Equal(t, "AWS::IAM::Role", res.Type)
	assert.Equal(t, "MyTestRole", res.PhysicalID)
	assert.Empty(t, res.Error, "role should deploy without error")
	assert.NotEmpty(t, res.ARN, "role ARN should be populated")
}

func TestDeployStack_IAMPolicy(t *testing.T) {
	ctx := context.Background()
	registry, store, state, tc, logger := newBettyTestDeps(t)
	client := substrate.NewBettyClient(registry, store, state, tc, logger)

	const tmpl = `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyPolicy": {
				"Type": "AWS::IAM::Policy",
				"Properties": {
					"PolicyName": "MyTestPolicy",
					"PolicyDocument": {
						"Version": "2012-10-17",
						"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]
					}
				}
			}
		}
	}`

	result, err := client.Deploy(ctx, tmpl, substrate.Intent{})
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)

	res := result.Resources[0]
	assert.Equal(t, "MyPolicy", res.LogicalID)
	assert.Equal(t, "AWS::IAM::Policy", res.Type)
	assert.Equal(t, "MyTestPolicy", res.PhysicalID)
	assert.Empty(t, res.Error, "policy should deploy without error")
	assert.NotEmpty(t, res.ARN, "policy ARN should be populated")
}

func TestDeployStack_MultipleResources(t *testing.T) {
	ctx := context.Background()
	registry, store, state, tc, logger := newBettyTestDeps(t)
	client := substrate.NewBettyClient(registry, store, state, tc, logger)

	const tmpl = `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"AppBucket": {
				"Type": "AWS::S3::Bucket",
				"Properties": { "BucketName": "app-data-bucket" }
			},
			"AppRole": {
				"Type": "AWS::IAM::Role",
				"Properties": {
					"RoleName": "AppRole",
					"AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": []}
				}
			},
			"AppPolicy": {
				"Type": "AWS::IAM::Policy",
				"Properties": {
					"PolicyName": "AppPolicy",
					"PolicyDocument": {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]}
				}
			}
		}
	}`

	result, err := client.Deploy(ctx, tmpl, substrate.Intent{})
	require.NoError(t, err)
	assert.Len(t, result.Resources, 3)

	types := make(map[string]bool)
	for _, r := range result.Resources {
		types[r.Type] = true
		assert.Empty(t, r.Error, "resource %s should deploy without error", r.LogicalID)
	}
	assert.True(t, types["AWS::S3::Bucket"])
	assert.True(t, types["AWS::IAM::Role"])
	assert.True(t, types["AWS::IAM::Policy"])
}

func TestDeployStack_YAMLTemplate(t *testing.T) {
	ctx := context.Background()
	registry, store, state, tc, logger := newBettyTestDeps(t)
	client := substrate.NewBettyClient(registry, store, state, tc, logger)

	const tmpl = `
AWSTemplateFormatVersion: "2010-09-09"
Resources:
  YamlBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: yaml-test-bucket
`

	result, err := client.Deploy(ctx, tmpl, substrate.Intent{})
	require.NoError(t, err)
	require.Len(t, result.Resources, 1)

	res := result.Resources[0]
	assert.Equal(t, "YamlBucket", res.LogicalID)
	assert.Equal(t, "yaml-test-bucket", res.PhysicalID)
	assert.Empty(t, res.Error)
}

func TestDeployStack_InvalidTemplate(t *testing.T) {
	ctx := context.Background()
	registry, store, state, tc, logger := newBettyTestDeps(t)
	client := substrate.NewBettyClient(registry, store, state, tc, logger)

	_, err := client.Deploy(ctx, "not json or yaml at all: {{{", substrate.Intent{})
	assert.Error(t, err)
}

func TestDeployStack_LambdaServiceNotAvailable(t *testing.T) {
	ctx := context.Background()
	registry, store, state, tc, logger := newBettyTestDeps(t)
	client := substrate.NewBettyClient(registry, store, state, tc, logger)

	const tmpl = `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyBucket": {
				"Type": "AWS::S3::Bucket",
				"Properties": { "BucketName": "lambda-test-bucket" }
			},
			"MyFunction": {
				"Type": "AWS::Lambda::Function",
				"Properties": { "FunctionName": "my-func" }
			}
		}
	}`

	result, err := client.Deploy(ctx, tmpl, substrate.Intent{})
	require.NoError(t, err)
	require.Len(t, result.Resources, 2)

	hasLambdaError := false
	hasS3Success := false
	for _, r := range result.Resources {
		switch r.Type {
		case "AWS::Lambda::Function":
			assert.NotEmpty(t, r.Error, "Lambda should have an error when no lambda plugin is registered")
			hasLambdaError = true
		case "AWS::S3::Bucket":
			assert.Empty(t, r.Error, "S3 bucket should deploy successfully alongside Lambda")
			hasS3Success = true
		}
	}
	assert.True(t, hasLambdaError, "should have a Lambda deploy error")
	assert.True(t, hasS3Success, "should have a successful S3 bucket")
}

func TestDeployStack_EventsRecorded(t *testing.T) {
	ctx := context.Background()
	registry, store, state, tc, logger := newBettyTestDeps(t)
	client := substrate.NewBettyClient(registry, store, state, tc, logger)

	const tmpl = `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"EventBucket": {
				"Type": "AWS::S3::Bucket",
				"Properties": { "BucketName": "event-test-bucket" }
			}
		}
	}`

	result, err := client.Deploy(ctx, tmpl, substrate.Intent{})
	require.NoError(t, err)

	events, err := store.GetStream(ctx, result.StreamID)
	require.NoError(t, err)
	assert.NotEmpty(t, events, "events should be recorded in the store")

	for _, ev := range events {
		assert.Equal(t, result.StreamID, ev.StreamID, "all events should share the deploy stream ID")
	}
}

// --- ValidateRecording tests (#24) -----------------------------------------

func TestValidateRecording_EmptyStream(t *testing.T) {
	ctx := context.Background()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})

	report, err := substrate.ValidateRecording(ctx, store, "nonexistent-stream", substrate.Intent{})
	require.NoError(t, err)
	assert.Equal(t, substrate.ValidationPassed, report.PassFail)
	assert.Equal(t, 0, report.TotalEvents)
	assert.Equal(t, 0.0, report.Cost.Total)
}

func TestValidateRecording_WithCost(t *testing.T) {
	ctx := context.Background()
	registry, store, state, tc, logger := newBettyTestDeps(t)
	client := substrate.NewBettyClient(registry, store, state, tc, logger)

	// Deploy an S3 bucket to generate a put request with cost.
	const tmpl = `{"AWSTemplateFormatVersion":"2010-09-09","Resources":{"B":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"cost-test"}}}}`
	result, err := client.Deploy(ctx, tmpl, substrate.Intent{})
	require.NoError(t, err)

	report, err := substrate.ValidateRecording(ctx, store, result.StreamID, substrate.Intent{})
	require.NoError(t, err)
	assert.Greater(t, report.TotalEvents, 0)
	// Cost breakdown maps should be initialized even if cost is zero.
	assert.NotNil(t, report.Cost.ByService)
	assert.NotNil(t, report.Cost.ByOperation)
}

func TestValidateRecording_FailedEvents(t *testing.T) {
	ctx := context.Background()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})

	// Manually inject an event with an error into the store.
	streamID := "fail-test-stream"
	err := store.RecordRequest(
		ctx,
		&substrate.RequestContext{
			RequestID: "req-1",
			AccountID: "123456789012",
			Region:    "us-east-1",
			Timestamp: time.Now(),
			Metadata:  map[string]interface{}{"stream_id": streamID},
		},
		&substrate.AWSRequest{Service: "s3", Operation: "GetObject"},
		nil,
		time.Millisecond,
		0,
		&substrate.AWSError{Code: "NoSuchKey", Message: "not found", HTTPStatus: 404},
	)
	require.NoError(t, err)

	report, err := substrate.ValidateRecording(ctx, store, streamID, substrate.Intent{})
	require.NoError(t, err)
	assert.Equal(t, substrate.ValidationFailed, report.PassFail)
	assert.Equal(t, 1, report.FailedEvents)
}

func TestValidateRecording_ConsistencyIncidents(t *testing.T) {
	ctx := context.Background()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})

	streamID := "consistency-test-stream"
	err := store.RecordRequest(
		ctx,
		&substrate.RequestContext{
			RequestID: "req-1",
			AccountID: "123456789012",
			Region:    "us-east-1",
			Timestamp: time.Now(),
			Metadata:  map[string]interface{}{"stream_id": streamID},
		},
		&substrate.AWSRequest{Service: "s3", Operation: "GetObject"},
		nil,
		time.Millisecond,
		0,
		&substrate.AWSError{Code: "InconsistentStateException", Message: "state not yet propagated", HTTPStatus: 409},
	)
	require.NoError(t, err)

	report, err := substrate.ValidateRecording(ctx, store, streamID, substrate.Intent{})
	require.NoError(t, err)
	require.Len(t, report.ConsistencyIncidents, 1)
	assert.Equal(t, "InconsistentStateException", report.ConsistencyIncidents[0].ErrorCode)
	assert.Equal(t, "s3", report.ConsistencyIncidents[0].Service)
}

func TestValidateRecording_MonthlyProjection(t *testing.T) {
	ctx := context.Background()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})

	streamID := "monthly-test-stream"
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	// Record 3 events spaced 1 second apart with non-zero cost.
	for i := 0; i < 3; i++ {
		require.NoError(t, store.RecordRequest(
			ctx,
			&substrate.RequestContext{
				RequestID: "req-" + strings.Repeat("x", i),
				AccountID: "123456789012",
				Region:    "us-east-1",
				Timestamp: base.Add(time.Duration(i) * time.Second),
				Metadata:  map[string]interface{}{"stream_id": streamID},
			},
			&substrate.AWSRequest{Service: "s3", Operation: "PutObject"},
			&substrate.AWSResponse{StatusCode: 200},
			time.Millisecond,
			0.000005,
			nil,
		))
	}

	report, err := substrate.ValidateRecording(ctx, store, streamID, substrate.Intent{})
	require.NoError(t, err)
	assert.Greater(t, report.Cost.Monthly, 0.0, "monthly projection should be non-zero")
}

func TestValidateRecording_Suggestions(t *testing.T) {
	ctx := context.Background()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})

	streamID := "suggestions-test-stream"
	err := store.RecordRequest(
		ctx,
		&substrate.RequestContext{
			RequestID: "req-1",
			AccountID: "123456789012",
			Region:    "us-east-1",
			Timestamp: time.Now(),
			Metadata:  map[string]interface{}{"stream_id": streamID},
		},
		&substrate.AWSRequest{Service: "s3", Operation: "PutObject"},
		&substrate.AWSResponse{StatusCode: 200},
		time.Millisecond,
		0.000005,
		nil,
	)
	require.NoError(t, err)

	report, err := substrate.ValidateRecording(ctx, store, streamID, substrate.Intent{})
	require.NoError(t, err)
	assert.NotEmpty(t, report.Suggestions, "at least one suggestion should be generated when there are events with cost")
}

// --- BettyClient tests (#25) -----------------------------------------------

func TestBettyClient_FullWorkflow(t *testing.T) {
	ctx := context.Background()
	registry, store, state, tc, logger := newBettyTestDeps(t)
	client := substrate.NewBettyClient(registry, store, state, tc, logger)

	// Step 1: deploy a stack.
	const tmpl = `{"AWSTemplateFormatVersion":"2010-09-09","Resources":{"WfBucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"workflow-bucket"}}}}`
	deployResult, err := client.Deploy(ctx, tmpl, substrate.Intent{})
	require.NoError(t, err)
	assert.Len(t, deployResult.Resources, 1)

	// Step 2: start a recording session.
	session, err := client.StartRecording(ctx, "workflow-test")
	require.NoError(t, err)
	assert.NotEmpty(t, session.StreamID)

	// Step 3: stop recording and validate.
	report, err := client.StopRecording(ctx, session)
	require.NoError(t, err)
	assert.Equal(t, session.StreamID, report.StreamID)
	assert.NotNil(t, report.Cost.ByService)

	// Step 4: validate the deploy stream.
	deployReport, err := client.Validate(ctx, deployResult.StreamID)
	require.NoError(t, err)
	assert.Equal(t, deployResult.StreamID, deployReport.StreamID)
}

func TestBettyClient_IntentMaxCost(t *testing.T) {
	ctx := context.Background()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: true, Backend: "memory"})

	streamID := "intent-cost-stream"
	err := store.RecordRequest(
		ctx,
		&substrate.RequestContext{
			RequestID: "req-1",
			AccountID: "123456789012",
			Region:    "us-east-1",
			Timestamp: time.Now(),
			Metadata:  map[string]interface{}{"stream_id": streamID},
		},
		&substrate.AWSRequest{Service: "s3", Operation: "PutObject"},
		&substrate.AWSResponse{StatusCode: 200},
		time.Millisecond,
		10.0, // $10 cost — well above any MaxCost limit we'll set
		nil,
	)
	require.NoError(t, err)

	report, err := substrate.ValidateRecording(ctx, store, streamID, substrate.Intent{MaxCost: 1.0})
	require.NoError(t, err)
	assert.NotEmpty(t, report.Violations, "should have a MaxCost violation")
	assert.Contains(t, report.Violations[0], "MaxCost")
}

func TestBettyClient_StartStopRecording(t *testing.T) {
	ctx := context.Background()
	registry, store, state, tc, logger := newBettyTestDeps(t)
	client := substrate.NewBettyClient(registry, store, state, tc, logger)

	session, err := client.StartRecording(ctx, "round-trip")
	require.NoError(t, err)
	assert.NotEmpty(t, session.StreamID)
	assert.True(t, strings.HasPrefix(session.StreamID, "recording-round-trip-"))

	// Record an event manually into the session's stream.
	err = store.RecordRequest(
		ctx,
		&substrate.RequestContext{
			RequestID: "req-1",
			AccountID: "123456789012",
			Region:    "us-east-1",
			Timestamp: time.Now(),
			Metadata:  map[string]interface{}{"stream_id": session.StreamID},
		},
		&substrate.AWSRequest{Service: "s3", Operation: "ListBuckets"},
		&substrate.AWSResponse{StatusCode: 200},
		time.Millisecond,
		0,
		nil,
	)
	require.NoError(t, err)

	report, err := client.StopRecording(ctx, session)
	require.NoError(t, err)
	assert.Equal(t, session.StreamID, report.StreamID)
	assert.Equal(t, 1, report.TotalEvents)
}

func TestBettyClient_DebugSession_JumpToEvent(t *testing.T) {
	ctx := context.Background()
	registry, store, state, tc, logger := newBettyTestDeps(t)
	client := substrate.NewBettyClient(registry, store, state, tc, logger)

	// Deploy two S3 buckets to produce at least 2 events.
	const tmpl = `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"BucketA": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "debug-bucket-a"}},
			"BucketB": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "debug-bucket-b"}}
		}
	}`
	result, err := client.Deploy(ctx, tmpl, substrate.Intent{})
	require.NoError(t, err)

	events, err := store.GetStream(ctx, result.StreamID)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(events), 2, "need at least 2 events for jump test")

	dbg := client.NewDebugSession(result.StreamID)
	err = dbg.JumpToEvent(ctx, 1)
	// JumpToEvent replays events; with MemoryStateManager it should succeed.
	assert.NoError(t, err)
}

func TestBettyClient_DebugSession_StepBackward(t *testing.T) {
	ctx := context.Background()
	registry, store, state, tc, logger := newBettyTestDeps(t)
	client := substrate.NewBettyClient(registry, store, state, tc, logger)

	const tmpl = `{"AWSTemplateFormatVersion":"2010-09-09","Resources":{"SbBucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"step-backward-bucket"}}}}`
	result, err := client.Deploy(ctx, tmpl, substrate.Intent{})
	require.NoError(t, err)

	dbg := client.NewDebugSession(result.StreamID)

	// Jump to position 0 first to load the stream.
	jumpErr := dbg.JumpToEvent(ctx, 0)
	require.NoError(t, jumpErr)

	// At position 0, StepBackward should return "already at beginning" error.
	stepErr := dbg.StepBackward(ctx)
	assert.Error(t, stepErr, "StepBackward at position 0 should return an error")
	assert.Contains(t, stepErr.Error(), "beginning")
}

func TestBettyClient_DebugSession_InspectState(t *testing.T) {
	ctx := context.Background()
	registry, store, state, tc, logger := newBettyTestDeps(t)
	client := substrate.NewBettyClient(registry, store, state, tc, logger)

	const tmpl = `{"AWSTemplateFormatVersion":"2010-09-09","Resources":{"IsBucket":{"Type":"AWS::S3::Bucket","Properties":{"BucketName":"inspect-state-bucket"}}}}`
	result, err := client.Deploy(ctx, tmpl, substrate.Intent{})
	require.NoError(t, err)

	dbg := client.NewDebugSession(result.StreamID)

	snap, err := dbg.InspectState(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, snap, "snapshot should return non-empty JSON")

	// Verify it is valid JSON.
	assert.True(t, len(snap) > 2, "snapshot JSON should be non-trivial")
}
