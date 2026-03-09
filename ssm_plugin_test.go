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

func newSSMTestServer(t *testing.T) *substrate.Server {
	t.Helper()
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelInfo, false)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := substrate.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

	ssmPlugin := &substrate.SSMPlugin{}
	require.NoError(t, ssmPlugin.Initialize(context.TODO(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(ssmPlugin)

	return substrate.NewServer(*cfg, registry, store, state, tc, logger)
}

func ssmRequest(t *testing.T, srv *substrate.Server, operation string, body interface{}) *http.Response {
	t.Helper()
	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	r.Host = "ssm.us-east-1.amazonaws.com"
	r.Header.Set("Content-Type", "application/x-amz-json-1.1")
	r.Header.Set("X-Amz-Target", "AmazonSSM."+operation)

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Result()
}

func readSSMBody(t *testing.T, r *http.Response) map[string]interface{} {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	var out map[string]interface{}
	_ = json.Unmarshal(body, &out)
	return out
}

func TestSSMPlugin_PutParameter(t *testing.T) {
	srv := newSSMTestServer(t)

	// Create new parameter.
	resp := ssmRequest(t, srv, "PutParameter", map[string]interface{}{
		"Name":  "/app/config/key",
		"Value": "my-value",
		"Type":  "String",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readSSMBody(t, resp)
	assert.Equal(t, float64(1), body["Version"])

	// Overwrite should bump version.
	resp2 := ssmRequest(t, srv, "PutParameter", map[string]interface{}{
		"Name":      "/app/config/key",
		"Value":     "new-value",
		"Type":      "String",
		"Overwrite": true,
	})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	body2 := readSSMBody(t, resp2)
	assert.Equal(t, float64(2), body2["Version"])

	// No-overwrite conflict.
	resp3 := ssmRequest(t, srv, "PutParameter", map[string]interface{}{
		"Name":  "/app/config/key",
		"Value": "conflict",
		"Type":  "String",
	})
	assert.Equal(t, http.StatusConflict, resp3.StatusCode)
}

func TestSSMPlugin_GetParameter(t *testing.T) {
	srv := newSSMTestServer(t)

	ssmRequest(t, srv, "PutParameter", map[string]interface{}{
		"Name":  "/test/param",
		"Value": "hello",
		"Type":  "String",
	})

	resp := ssmRequest(t, srv, "GetParameter", map[string]interface{}{
		"Name": "/test/param",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readSSMBody(t, resp)
	param, ok := body["Parameter"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "hello", param["Value"])
	assert.Equal(t, "/test/param", param["Name"])

	// Missing parameter.
	resp2 := ssmRequest(t, srv, "GetParameter", map[string]interface{}{
		"Name": "/does/not/exist",
	})
	assert.Equal(t, http.StatusNotFound, resp2.StatusCode)
}

func TestSSMPlugin_GetParameters(t *testing.T) {
	srv := newSSMTestServer(t)

	ssmRequest(t, srv, "PutParameter", map[string]interface{}{
		"Name":  "/a/b",
		"Value": "v1",
		"Type":  "String",
	})
	ssmRequest(t, srv, "PutParameter", map[string]interface{}{
		"Name":  "/a/c",
		"Value": "v2",
		"Type":  "String",
	})

	resp := ssmRequest(t, srv, "GetParameters", map[string]interface{}{
		"Names": []string{"/a/b", "/a/c", "/does/not/exist"},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readSSMBody(t, resp)
	params, ok := body["Parameters"].([]interface{})
	require.True(t, ok)
	assert.Len(t, params, 2)
	invalid, ok := body["InvalidParameters"].([]interface{})
	require.True(t, ok)
	assert.Len(t, invalid, 1)
}

func TestSSMPlugin_DeleteParameter(t *testing.T) {
	srv := newSSMTestServer(t)

	ssmRequest(t, srv, "PutParameter", map[string]interface{}{
		"Name":  "/to/delete",
		"Value": "bye",
		"Type":  "String",
	})

	resp := ssmRequest(t, srv, "DeleteParameter", map[string]interface{}{
		"Name": "/to/delete",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Should be gone.
	resp2 := ssmRequest(t, srv, "GetParameter", map[string]interface{}{
		"Name": "/to/delete",
	})
	assert.Equal(t, http.StatusNotFound, resp2.StatusCode)
}

func TestSSMPlugin_GetParametersByPath(t *testing.T) {
	srv := newSSMTestServer(t)

	for _, p := range []string{"/svc/db/host", "/svc/db/port", "/svc/app/name", "/other/key"} {
		ssmRequest(t, srv, "PutParameter", map[string]interface{}{
			"Name":  p,
			"Value": "value",
			"Type":  "String",
		})
	}

	// Non-recursive: direct children of /svc/db/.
	resp := ssmRequest(t, srv, "GetParametersByPath", map[string]interface{}{
		"Path":      "/svc/db",
		"Recursive": false,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readSSMBody(t, resp)
	params, ok := body["Parameters"].([]interface{})
	require.True(t, ok)
	assert.Len(t, params, 2)

	// Recursive: all under /svc/.
	resp2 := ssmRequest(t, srv, "GetParametersByPath", map[string]interface{}{
		"Path":      "/svc",
		"Recursive": true,
	})
	body2 := readSSMBody(t, resp2)
	params2, ok := body2["Parameters"].([]interface{})
	require.True(t, ok)
	assert.Len(t, params2, 3)
}
