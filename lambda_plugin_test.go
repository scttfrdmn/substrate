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

func newLambdaTestServer(t *testing.T) *substrate.Server {
	t.Helper()
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

	return substrate.NewServer(*cfg, registry, store, state, tc, logger)
}

func lambdaRequest(t *testing.T, srv *substrate.Server, method, path string, body any) *http.Response {
	t.Helper()
	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}
	r := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	r.Host = "lambda.us-east-1.amazonaws.com"
	r.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Result()
}

func decodeLambdaJSON(t *testing.T, r *http.Response, dst any) {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(body, dst))
}

func TestLambdaPlugin_CRUD(t *testing.T) {
	srv := newLambdaTestServer(t)

	// Create function.
	resp := lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "my-function",
		"Runtime":      "python3.12",
		"Role":         "arn:aws:iam::123456789012:role/lambda-role",
		"Handler":      "index.handler",
	})
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	var created map[string]any
	decodeLambdaJSON(t, resp, &created)
	assert.Equal(t, "my-function", created["FunctionName"])
	assert.Equal(t, "python3.12", created["Runtime"])
	assert.Equal(t, "Active", created["State"])
	assert.Contains(t, created["FunctionArn"].(string), ":function:my-function")

	// Get function.
	resp2 := lambdaRequest(t, srv, http.MethodGet, "/2015-03-31/functions/my-function", nil)
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	var getResult map[string]any
	decodeLambdaJSON(t, resp2, &getResult)
	cfg := getResult["Configuration"].(map[string]any)
	assert.Equal(t, "my-function", cfg["FunctionName"])

	// Update configuration.
	resp3 := lambdaRequest(t, srv, http.MethodPut, "/2015-03-31/functions/my-function/configuration", map[string]any{
		"Timeout":    10,
		"MemorySize": 256,
	})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)
	var updated map[string]any
	decodeLambdaJSON(t, resp3, &updated)
	assert.Equal(t, float64(10), updated["Timeout"])
	assert.Equal(t, float64(256), updated["MemorySize"])

	// Delete function.
	resp4 := lambdaRequest(t, srv, http.MethodDelete, "/2015-03-31/functions/my-function", nil)
	assert.Equal(t, http.StatusNoContent, resp4.StatusCode)

	// Get after delete → 404.
	resp5 := lambdaRequest(t, srv, http.MethodGet, "/2015-03-31/functions/my-function", nil)
	assert.Equal(t, http.StatusNotFound, resp5.StatusCode)
}

func TestLambdaPlugin_ListFunctions(t *testing.T) {
	srv := newLambdaTestServer(t)

	for _, name := range []string{"fn-a", "fn-b", "fn-c"} {
		resp := lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
			"FunctionName": name,
			"Runtime":      "go1.x",
			"Role":         "arn:aws:iam::123456789012:role/r",
			"Handler":      "main",
		})
		assert.Equal(t, http.StatusCreated, resp.StatusCode)
	}

	resp := lambdaRequest(t, srv, http.MethodGet, "/2015-03-31/functions", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]any
	decodeLambdaJSON(t, resp, &result)
	functions := result["Functions"].([]any)
	assert.Len(t, functions, 3)
}

func TestLambdaPlugin_Invoke_Sync(t *testing.T) {
	srv := newLambdaTestServer(t)

	lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "invoke-test",
		"Runtime":      "python3.12",
		"Role":         "arn:aws:iam::123456789012:role/r",
		"Handler":      "index.handler",
	})

	resp := lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions/invoke-test/invocations", map[string]any{
		"key": "value",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "statusCode")
}

func TestLambdaPlugin_InvokeAsync(t *testing.T) {
	srv := newLambdaTestServer(t)

	lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "async-test",
		"Runtime":      "python3.12",
		"Role":         "arn:aws:iam::123456789012:role/r",
		"Handler":      "index.handler",
	})

	resp := lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions/async-test/invoke-async", nil)
	assert.Equal(t, http.StatusAccepted, resp.StatusCode)
}

func TestLambdaPlugin_AddRemovePermission(t *testing.T) {
	srv := newLambdaTestServer(t)

	lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "perm-test",
		"Runtime":      "python3.12",
		"Role":         "arn:aws:iam::123456789012:role/r",
		"Handler":      "index.handler",
	})

	// Add permission.
	resp := lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions/perm-test/policy", map[string]any{
		"StatementId": "allow-s3",
		"Action":      "lambda:InvokeFunction",
		"Principal":   "s3.amazonaws.com",
		"SourceArn":   "arn:aws:s3:::my-bucket",
	})
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// Get policy.
	resp2 := lambdaRequest(t, srv, http.MethodGet, "/2015-03-31/functions/perm-test/policy", nil)
	assert.Equal(t, http.StatusOK, resp2.StatusCode)

	// Remove permission.
	resp3 := lambdaRequest(t, srv, http.MethodDelete, "/2015-03-31/functions/perm-test/policy/allow-s3", nil)
	assert.Equal(t, http.StatusNoContent, resp3.StatusCode)
}

func TestLambdaPlugin_GetPolicy_NotFound(t *testing.T) {
	srv := newLambdaTestServer(t)

	lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "no-policy",
		"Runtime":      "python3.12",
		"Role":         "arn:aws:iam::123456789012:role/r",
		"Handler":      "index.handler",
	})

	resp := lambdaRequest(t, srv, http.MethodGet, "/2015-03-31/functions/no-policy/policy", nil)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestLambdaPlugin_PutFunctionEventInvokeConfig(t *testing.T) {
	srv := newLambdaTestServer(t)

	lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "invoke-cfg",
		"Runtime":      "python3.12",
		"Role":         "arn:aws:iam::123456789012:role/r",
		"Handler":      "index.handler",
	})

	resp := lambdaRequest(t, srv, http.MethodPut, "/2015-03-31/functions/invoke-cfg/event-invoke-config", map[string]any{
		"MaximumRetryAttempts":     2,
		"MaximumEventAgeInSeconds": 3600,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]any
	decodeLambdaJSON(t, resp, &result)
	assert.Equal(t, float64(2), result["MaximumRetryAttempts"])
	assert.Equal(t, float64(3600), result["MaximumEventAgeInSeconds"])
}

func TestLambdaPlugin_UpdateFunctionCode(t *testing.T) {
	srv := newLambdaTestServer(t)

	lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "code-update",
		"Runtime":      "python3.12",
		"Role":         "arn:aws:iam::123456789012:role/r",
		"Handler":      "index.handler",
	})

	resp := lambdaRequest(t, srv, http.MethodPut, "/2015-03-31/functions/code-update/code", map[string]any{
		"ZipFile": "UEsDBBQ=", // base64 stub
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]any
	decodeLambdaJSON(t, resp, &result)
	assert.NotEmpty(t, result["RevisionId"])
}

func TestLambdaPlugin_CreateFunction_Conflict(t *testing.T) {
	srv := newLambdaTestServer(t)

	body := map[string]any{
		"FunctionName": "conflict-fn",
		"Runtime":      "python3.12",
		"Role":         "arn:aws:iam::123456789012:role/r",
		"Handler":      "index.handler",
	}
	resp1 := lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions", body)
	assert.Equal(t, http.StatusCreated, resp1.StatusCode)

	resp2 := lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions", body)
	assert.Equal(t, http.StatusConflict, resp2.StatusCode)
}

func TestLambdaPlugin_TagResource(t *testing.T) {
	srv := newLambdaTestServer(t)

	// Create function.
	createResp := lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "tag-fn",
		"Runtime":      "python3.12",
		"Role":         "arn:aws:iam::123456789012:role/r",
		"Handler":      "index.handler",
	})
	assert.Equal(t, http.StatusCreated, createResp.StatusCode)

	var fnResult map[string]any
	decodeLambdaJSON(t, createResp, &fnResult)
	arn := fnResult["FunctionArn"].(string)

	// TagResource.
	tagResp := lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/tags/"+arn, map[string]any{
		"Tags": map[string]string{"env": "test", "owner": "alice"},
	})
	assert.Equal(t, http.StatusNoContent, tagResp.StatusCode)

	// ListTags.
	listResp := lambdaRequest(t, srv, http.MethodGet, "/2015-03-31/tags/"+arn, nil)
	assert.Equal(t, http.StatusOK, listResp.StatusCode)
	var listResult map[string]any
	decodeLambdaJSON(t, listResp, &listResult)
	tags := listResult["Tags"].(map[string]any)
	assert.Equal(t, "test", tags["env"])
	assert.Equal(t, "alice", tags["owner"])
}

func TestLambdaPlugin_UntagResource(t *testing.T) {
	srv := newLambdaTestServer(t)

	createResp := lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": "untag-fn",
		"Runtime":      "python3.12",
		"Role":         "arn:aws:iam::123456789012:role/r",
		"Handler":      "index.handler",
	})
	var fnResult map[string]any
	decodeLambdaJSON(t, createResp, &fnResult)
	arn := fnResult["FunctionArn"].(string)

	// Add tags.
	lambdaRequest(t, srv, http.MethodPost, "/2015-03-31/tags/"+arn, map[string]any{
		"Tags": map[string]string{"a": "1", "b": "2"},
	})

	// Untag "a".
	untagURL := "/2015-03-31/tags/" + arn + "?tagKeys=a"
	untagResp := lambdaRequest(t, srv, http.MethodDelete, untagURL, nil)
	assert.Equal(t, http.StatusNoContent, untagResp.StatusCode)

	// ListTags — should only have "b".
	listResp := lambdaRequest(t, srv, http.MethodGet, "/2015-03-31/tags/"+arn, nil)
	var listResult map[string]any
	decodeLambdaJSON(t, listResp, &listResult)
	tags := listResult["Tags"].(map[string]any)
	assert.NotContains(t, tags, "a")
	assert.Contains(t, tags, "b")
}
