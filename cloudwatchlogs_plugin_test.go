package substrate_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

// newCWLogsTestServer creates a test server with the CloudWatchLogs plugin.
func newCWLogsTestServer(t *testing.T) *substrate.Server {
	t.Helper()
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelInfo, false)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := substrate.NewTimeController(time.Now())

	plugin := &substrate.CloudWatchLogsPlugin{}
	require.NoError(t, plugin.Initialize(context.TODO(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(plugin)

	return substrate.NewServer(*cfg, registry, store, state, tc, logger)
}

// cwLogsRequest sends a CloudWatch Logs JSON-protocol request.
func cwLogsRequest(t *testing.T, srv *substrate.Server, op string, body any) *http.Response {
	t.Helper()
	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	r.Host = "logs.us-east-1.amazonaws.com"
	r.Header.Set("Content-Type", "application/x-amz-json-1.1")
	r.Header.Set("X-Amz-Target", "Logs_20140328."+op)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIATEST1234567890/20240101/us-east-1/logs/aws4_request, SignedHeaders=host, Signature=fake")

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Result()
}

func cwLogsReadBody(t *testing.T, r *http.Response) []byte {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	return body
}

func TestCWLogs_LogGroupLifecycle(t *testing.T) {
	srv := newCWLogsTestServer(t)

	// Create log group.
	resp := cwLogsRequest(t, srv, "CreateLogGroup", map[string]string{
		"logGroupName": "/aws/lambda/my-function",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Describe log groups.
	resp = cwLogsRequest(t, srv, "DescribeLogGroups", map[string]any{})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := cwLogsReadBody(t, resp)
	var result struct {
		LogGroups []struct {
			LogGroupName string `json:"LogGroupName"`
		} `json:"logGroups"`
	}
	require.NoError(t, json.Unmarshal(body, &result))
	require.Len(t, result.LogGroups, 1)
	assert.Equal(t, "/aws/lambda/my-function", result.LogGroups[0].LogGroupName)

	// Create duplicate should fail.
	resp = cwLogsRequest(t, srv, "CreateLogGroup", map[string]string{
		"logGroupName": "/aws/lambda/my-function",
	})
	assert.Equal(t, http.StatusConflict, resp.StatusCode)

	// Delete log group.
	resp = cwLogsRequest(t, srv, "DeleteLogGroup", map[string]string{
		"logGroupName": "/aws/lambda/my-function",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Describe after delete should be empty.
	resp = cwLogsRequest(t, srv, "DescribeLogGroups", map[string]any{})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body = cwLogsReadBody(t, resp)
	var result2 struct {
		LogGroups []any `json:"logGroups"`
	}
	require.NoError(t, json.Unmarshal(body, &result2))
	assert.Empty(t, result2.LogGroups)
}

func TestCWLogs_LogGroupPrefix(t *testing.T) {
	srv := newCWLogsTestServer(t)

	for _, name := range []string{"/aws/lambda/fn1", "/aws/lambda/fn2", "/other/group"} {
		resp := cwLogsRequest(t, srv, "CreateLogGroup", map[string]string{"logGroupName": name})
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	}

	// Filter by prefix.
	resp := cwLogsRequest(t, srv, "DescribeLogGroups", map[string]string{
		"logGroupNamePrefix": "/aws/lambda/",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := cwLogsReadBody(t, resp)
	var result struct {
		LogGroups []struct {
			LogGroupName string `json:"LogGroupName"`
		} `json:"logGroups"`
	}
	require.NoError(t, json.Unmarshal(body, &result))
	assert.Len(t, result.LogGroups, 2)
}

func TestCWLogs_LogStreamLifecycle(t *testing.T) {
	srv := newCWLogsTestServer(t)

	// Create group first.
	resp := cwLogsRequest(t, srv, "CreateLogGroup", map[string]string{"logGroupName": "/test/group"})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Create stream.
	resp = cwLogsRequest(t, srv, "CreateLogStream", map[string]string{
		"logGroupName":  "/test/group",
		"logStreamName": "my-stream",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Describe streams.
	resp = cwLogsRequest(t, srv, "DescribeLogStreams", map[string]string{
		"logGroupName": "/test/group",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := cwLogsReadBody(t, resp)
	var result struct {
		LogStreams []struct {
			LogStreamName string `json:"LogStreamName"`
		} `json:"logStreams"`
	}
	require.NoError(t, json.Unmarshal(body, &result))
	require.Len(t, result.LogStreams, 1)
	assert.Equal(t, "my-stream", result.LogStreams[0].LogStreamName)

	// Delete stream.
	resp = cwLogsRequest(t, srv, "DeleteLogStream", map[string]string{
		"logGroupName":  "/test/group",
		"logStreamName": "my-stream",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestCWLogs_PutAndGetLogEvents(t *testing.T) {
	srv := newCWLogsTestServer(t)
	ctx := context.Background()
	_ = ctx

	// Setup.
	resp := cwLogsRequest(t, srv, "CreateLogGroup", map[string]string{"logGroupName": "/test/lg"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp = cwLogsRequest(t, srv, "CreateLogStream", map[string]string{
		"logGroupName":  "/test/lg",
		"logStreamName": "stream1",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Put events.
	now := time.Now().UnixMilli()
	resp = cwLogsRequest(t, srv, "PutLogEvents", map[string]any{
		"logGroupName":  "/test/lg",
		"logStreamName": "stream1",
		"logEvents": []map[string]any{
			{"timestamp": now, "message": "hello world"},
			{"timestamp": now + 1000, "message": "second event"},
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := cwLogsReadBody(t, resp)
	var putResult struct {
		NextSequenceToken string `json:"nextSequenceToken"`
	}
	require.NoError(t, json.Unmarshal(body, &putResult))
	assert.NotEmpty(t, putResult.NextSequenceToken)

	// Get events.
	resp = cwLogsRequest(t, srv, "GetLogEvents", map[string]any{
		"logGroupName":  "/test/lg",
		"logStreamName": "stream1",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body = cwLogsReadBody(t, resp)
	var getResult struct {
		Events []struct {
			Message string `json:"Message"`
		} `json:"events"`
	}
	require.NoError(t, json.Unmarshal(body, &getResult))
	require.Len(t, getResult.Events, 2)
	assert.Equal(t, "hello world", getResult.Events[0].Message)
	assert.Equal(t, "second event", getResult.Events[1].Message)
}

func TestCWLogs_FilterLogEvents(t *testing.T) {
	srv := newCWLogsTestServer(t)

	// Setup group + stream.
	cwLogsRequest(t, srv, "CreateLogGroup", map[string]string{"logGroupName": "/test/filter"})
	cwLogsRequest(t, srv, "CreateLogStream", map[string]string{"logGroupName": "/test/filter", "logStreamName": "s1"})

	now := time.Now().UnixMilli()
	cwLogsRequest(t, srv, "PutLogEvents", map[string]any{
		"logGroupName":  "/test/filter",
		"logStreamName": "s1",
		"logEvents": []map[string]any{
			{"timestamp": now, "message": "ERROR: disk full"},
			{"timestamp": now + 1, "message": "INFO: all ok"},
		},
	})

	// Filter by pattern.
	resp := cwLogsRequest(t, srv, "FilterLogEvents", map[string]any{
		"logGroupName":  "/test/filter",
		"filterPattern": "ERROR",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := cwLogsReadBody(t, resp)
	var result struct {
		Events []struct {
			Message string `json:"message"`
		} `json:"events"`
	}
	require.NoError(t, json.Unmarshal(body, &result))
	require.Len(t, result.Events, 1)
	assert.Equal(t, "ERROR: disk full", result.Events[0].Message)
}

func TestCWLogs_DeleteNonExistent(t *testing.T) {
	srv := newCWLogsTestServer(t)

	resp := cwLogsRequest(t, srv, "DeleteLogGroup", map[string]string{"logGroupName": "/does/not/exist"})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestCWLogs_CreateStreamMissingGroup(t *testing.T) {
	srv := newCWLogsTestServer(t)

	resp := cwLogsRequest(t, srv, "CreateLogStream", map[string]string{
		"logGroupName":  "/missing/group",
		"logStreamName": "stream",
	})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}
