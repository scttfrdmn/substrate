package substrate_test

// Additional tests for coverage of cloudwatch / eventbridge / cwlogs plugins.

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

// --- CloudWatch Logs additional coverage ------------------------------------

func TestCWLogs_Shutdown(t *testing.T) {
	p := &substrate.CloudWatchLogsPlugin{}
	require.NoError(t, p.Shutdown(context.Background()))
}

func TestCWLogs_UnknownOperation(t *testing.T) {
	srv := newCWLogsTestServer(t)
	resp := cwLogsRequest(t, srv, "UnknownOp", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestCWLogs_CreateGroupMissingName(t *testing.T) {
	srv := newCWLogsTestServer(t)
	resp := cwLogsRequest(t, srv, "CreateLogGroup", map[string]string{})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestCWLogs_CreateStreamDuplicate(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwLogsRequest(t, srv, "CreateLogGroup", map[string]string{"logGroupName": "/dup/group"})
	cwLogsRequest(t, srv, "CreateLogStream", map[string]string{"logGroupName": "/dup/group", "logStreamName": "s1"})
	resp := cwLogsRequest(t, srv, "CreateLogStream", map[string]string{"logGroupName": "/dup/group", "logStreamName": "s1"})
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
}

func TestCWLogs_DeleteStreamNotFound(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwLogsRequest(t, srv, "CreateLogGroup", map[string]string{"logGroupName": "/g"})
	resp := cwLogsRequest(t, srv, "DeleteLogStream", map[string]string{"logGroupName": "/g", "logStreamName": "missing"})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestCWLogs_DescribeStreamPrefix(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwLogsRequest(t, srv, "CreateLogGroup", map[string]string{"logGroupName": "/g"})
	for _, s := range []string{"prod-1", "prod-2", "dev-1"} {
		cwLogsRequest(t, srv, "CreateLogStream", map[string]string{"logGroupName": "/g", "logStreamName": s})
	}
	resp := cwLogsRequest(t, srv, "DescribeLogStreams", map[string]string{"logGroupName": "/g", "logStreamNamePrefix": "prod-"})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := cwLogsReadBody(t, resp)
	var result struct {
		LogStreams []any `json:"logStreams"`
	}
	require.NoError(t, json.Unmarshal(body, &result))
	assert.Len(t, result.LogStreams, 2)
}

func TestCWLogs_PutEventsOnMissingStream(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwLogsRequest(t, srv, "CreateLogGroup", map[string]string{"logGroupName": "/g"})
	resp := cwLogsRequest(t, srv, "PutLogEvents", map[string]any{
		"logGroupName":  "/g",
		"logStreamName": "nonexistent",
		"logEvents":     []map[string]any{{"timestamp": 1, "message": "x"}},
	})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestCWLogs_GetEventsTimeFilter(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwLogsRequest(t, srv, "CreateLogGroup", map[string]string{"logGroupName": "/g"})
	cwLogsRequest(t, srv, "CreateLogStream", map[string]string{"logGroupName": "/g", "logStreamName": "s"})

	t0 := int64(1000)
	cwLogsRequest(t, srv, "PutLogEvents", map[string]any{
		"logGroupName":  "/g",
		"logStreamName": "s",
		"logEvents": []map[string]any{
			{"timestamp": t0, "message": "early"},
			{"timestamp": t0 + 5000, "message": "late"},
		},
	})

	// Get only the early event.
	resp := cwLogsRequest(t, srv, "GetLogEvents", map[string]any{
		"logGroupName":  "/g",
		"logStreamName": "s",
		"startTime":     t0,
		"endTime":       t0 + 2000,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := cwLogsReadBody(t, resp)
	var result struct {
		Events []any `json:"events"`
	}
	require.NoError(t, json.Unmarshal(body, &result))
	assert.Len(t, result.Events, 1)
}

func TestCWLogs_DeleteGroupCleansStreams(t *testing.T) {
	srv := newCWLogsTestServer(t)
	cwLogsRequest(t, srv, "CreateLogGroup", map[string]string{"logGroupName": "/g"})
	cwLogsRequest(t, srv, "CreateLogStream", map[string]string{"logGroupName": "/g", "logStreamName": "s1"})
	cwLogsRequest(t, srv, "CreateLogStream", map[string]string{"logGroupName": "/g", "logStreamName": "s2"})

	resp := cwLogsRequest(t, srv, "DeleteLogGroup", map[string]string{"logGroupName": "/g"})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Recreate group to verify streams were cleaned.
	cwLogsRequest(t, srv, "CreateLogGroup", map[string]string{"logGroupName": "/g"})
	resp = cwLogsRequest(t, srv, "DescribeLogStreams", map[string]string{"logGroupName": "/g"})
	body := cwLogsReadBody(t, resp)
	var result struct {
		LogStreams []any `json:"logStreams"`
	}
	require.NoError(t, json.Unmarshal(body, &result))
	assert.Empty(t, result.LogStreams)
}

// --- EventBridge additional coverage ----------------------------------------

func TestEB_Shutdown(t *testing.T) {
	p := &substrate.EventBridgePlugin{}
	require.NoError(t, p.Shutdown(context.Background()))
}

func TestEB_UnknownOperation(t *testing.T) {
	srv := newEBTestServer(t)
	resp := ebRequest(t, srv, "UnknownOp", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestEB_PutRuleMissingName(t *testing.T) {
	srv := newEBTestServer(t)
	resp := ebRequest(t, srv, "PutRule", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestEB_DeleteRuleNotFound(t *testing.T) {
	srv := newEBTestServer(t)
	resp := ebRequest(t, srv, "DeleteRule", map[string]string{"Name": "missing"})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestEB_PutTargetsMissingRule(t *testing.T) {
	srv := newEBTestServer(t)
	resp := ebRequest(t, srv, "PutTargets", map[string]any{
		"Rule":    "missing",
		"Targets": []map[string]string{{"Id": "t1", "Arn": "arn:aws:sqs:us-east-1:123456789012:q"}},
	})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestEB_ListTargetsMissingRule(t *testing.T) {
	srv := newEBTestServer(t)
	resp := ebRequest(t, srv, "ListTargetsByRule", map[string]string{"Rule": "missing"})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestEB_PutRuleUpdatesExisting(t *testing.T) {
	srv := newEBTestServer(t)
	ebRequest(t, srv, "PutRule", map[string]string{"Name": "r1", "State": "ENABLED"})
	ebRequest(t, srv, "PutRule", map[string]string{"Name": "r1", "State": "DISABLED", "Description": "updated"})
	resp := ebRequest(t, srv, "DescribeRule", map[string]string{"Name": "r1"})
	body := ebReadBody(t, resp)
	var result struct {
		State string `json:"State"`
	}
	require.NoError(t, json.Unmarshal(body, &result))
	assert.Equal(t, "DISABLED", result.State)
}

func TestEB_PutTargetsUpdateExisting(t *testing.T) {
	srv := newEBTestServer(t)
	ebRequest(t, srv, "PutRule", map[string]string{"Name": "r1", "State": "ENABLED"})
	ebRequest(t, srv, "PutTargets", map[string]any{
		"Rule":    "r1",
		"Targets": []map[string]string{{"Id": "t1", "Arn": "arn:old"}},
	})
	// Update same target ID.
	resp := ebRequest(t, srv, "PutTargets", map[string]any{
		"Rule":    "r1",
		"Targets": []map[string]string{{"Id": "t1", "Arn": "arn:new"}},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp = ebRequest(t, srv, "ListTargetsByRule", map[string]string{"Rule": "r1"})
	body := ebReadBody(t, resp)
	var result struct {
		Targets []struct {
			Id  string `json:"Id"` //nolint:revive
			ARN string `json:"Arn"`
		} `json:"Targets"`
	}
	require.NoError(t, json.Unmarshal(body, &result))
	require.Len(t, result.Targets, 1)
	assert.Equal(t, "arn:new", result.Targets[0].ARN)
}

// --- CloudWatch Alarms additional coverage ----------------------------------

func TestCWAlarm_Shutdown(t *testing.T) {
	p := &substrate.CloudWatchPlugin{}
	require.NoError(t, p.Shutdown(context.Background()))
}

func TestCWAlarm_UnknownOperation(t *testing.T) {
	srv := newCWAlarmTestServer(t)
	resp := cwRequest(t, srv, map[string]string{"Action": "UnknownOp"})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestCWAlarm_PutAlarmMissingName(t *testing.T) {
	srv := newCWAlarmTestServer(t)
	resp := cwRequest(t, srv, map[string]string{"Action": "PutMetricAlarm"})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestCWAlarm_SetAlarmStateInvalidValue(t *testing.T) {
	srv := newCWAlarmTestServer(t)
	cwRequest(t, srv, map[string]string{
		"Action": "PutMetricAlarm", "AlarmName": "a1",
		"MetricName": "M", "Namespace": "N", "ComparisonOperator": "GT",
		"Threshold": "1", "EvaluationPeriods": "1", "Period": "60",
	})
	resp := cwRequest(t, srv, map[string]string{
		"Action":      "SetAlarmState",
		"AlarmName":   "a1",
		"StateValue":  "INVALID",
		"StateReason": "test",
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestCWAlarm_PutPreservesState(t *testing.T) {
	srv := newCWAlarmTestServer(t)
	cwRequest(t, srv, map[string]string{
		"Action": "PutMetricAlarm", "AlarmName": "preserve",
		"MetricName": "M", "Namespace": "N", "ComparisonOperator": "GT",
		"Threshold": "1", "EvaluationPeriods": "1", "Period": "60",
	})
	cwRequest(t, srv, map[string]string{
		"Action": "SetAlarmState", "AlarmName": "preserve",
		"StateValue": "ALARM", "StateReason": "triggered",
	})
	// Re-put the alarm (update).
	cwRequest(t, srv, map[string]string{
		"Action": "PutMetricAlarm", "AlarmName": "preserve",
		"MetricName": "M", "Namespace": "N", "ComparisonOperator": "GT",
		"Threshold": "2", "EvaluationPeriods": "1", "Period": "60",
	})
	// State should be preserved.
	resp := cwRequest(t, srv, map[string]string{
		"Action": "DescribeAlarms", "AlarmNames.member.1": "preserve",
	})
	body := cwReadBody(t, resp)
	assert.Contains(t, body, "ALARM")
}

// --- Lambda auto-creates log group ------------------------------------------

func TestLambda_AutoCreatesLogGroup(t *testing.T) {
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelInfo, false)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := substrate.NewTimeController(time.Now())

	lambdaPlugin := &substrate.LambdaPlugin{}
	require.NoError(t, lambdaPlugin.Initialize(context.TODO(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(lambdaPlugin)

	cwLogsPlugin := &substrate.CloudWatchLogsPlugin{}
	require.NoError(t, cwLogsPlugin.Initialize(context.TODO(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(cwLogsPlugin)

	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)

	// Create a Lambda function.
	body, _ := json.Marshal(map[string]string{
		"FunctionName": "my-fn",
		"Runtime":      "go1.x",
		"Role":         "arn:aws:iam::123456789012:role/role",
		"Handler":      "main",
	})
	r := httptest.NewRequest(http.MethodPost, "/2015-03-31/functions", bytes.NewReader(body))
	r.Host = "lambda.us-east-1.amazonaws.com"
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIATEST1234567890/20240101/us-east-1/lambda/aws4_request, SignedHeaders=host, Signature=fake")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	require.Equal(t, http.StatusCreated, w.Code)

	// Verify /aws/lambda/my-fn log group was auto-created.
	logsReq := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(func() []byte {
		b, _ := json.Marshal(map[string]string{"logGroupNamePrefix": "/aws/lambda/"})
		return b
	}()))
	logsReq.Host = "logs.us-east-1.amazonaws.com"
	logsReq.Header.Set("Content-Type", "application/x-amz-json-1.1")
	logsReq.Header.Set("X-Amz-Target", "Logs_20140328.DescribeLogGroups")
	logsReq.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIATEST1234567890/20240101/us-east-1/logs/aws4_request, SignedHeaders=host, Signature=fake")
	logsW := httptest.NewRecorder()
	srv.ServeHTTP(logsW, logsReq)
	require.Equal(t, http.StatusOK, logsW.Code)

	var result struct {
		LogGroups []struct {
			LogGroupName string `json:"LogGroupName"`
		} `json:"logGroups"`
	}
	require.NoError(t, json.Unmarshal(logsW.Body.Bytes(), &result))
	require.Len(t, result.LogGroups, 1)
	assert.Equal(t, "/aws/lambda/my-fn", result.LogGroups[0].LogGroupName)
}
