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

func newSMTestServer(t *testing.T) *substrate.Server {
	t.Helper()
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelInfo, false)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := substrate.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

	smPlugin := &substrate.SecretsManagerPlugin{}
	require.NoError(t, smPlugin.Initialize(context.TODO(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(smPlugin)

	return substrate.NewServer(*cfg, registry, store, state, tc, logger)
}

func smRequest(t *testing.T, srv *substrate.Server, operation string, body interface{}) *http.Response {
	t.Helper()
	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	r.Host = "secretsmanager.us-east-1.amazonaws.com"
	r.Header.Set("Content-Type", "application/x-amz-json-1.1")
	r.Header.Set("X-Amz-Target", "secretsmanager."+operation)

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Result()
}

func readSMBody(t *testing.T, r *http.Response) map[string]interface{} {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	var out map[string]interface{}
	_ = json.Unmarshal(body, &out)
	return out
}

func TestSMPlugin_CreateSecret(t *testing.T) {
	srv := newSMTestServer(t)

	// Create new secret.
	resp := smRequest(t, srv, "CreateSecret", map[string]interface{}{
		"Name":         "my-secret",
		"SecretString": `{"password":"s3cr3t"}`,
		"Description":  "Test secret",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readSMBody(t, resp)
	assert.Equal(t, "my-secret", body["Name"])
	assert.Contains(t, body["ARN"], "my-secret")

	// Duplicate create should return conflict.
	resp2 := smRequest(t, srv, "CreateSecret", map[string]interface{}{
		"Name":         "my-secret",
		"SecretString": "duplicate",
	})
	assert.Equal(t, http.StatusConflict, resp2.StatusCode)
	body2 := readSMBody(t, resp2)
	assert.Equal(t, "ResourceExistsException", body2["Code"])
}

func TestSMPlugin_GetSecretValue(t *testing.T) {
	srv := newSMTestServer(t)

	smRequest(t, srv, "CreateSecret", map[string]interface{}{
		"Name":         "get-secret",
		"SecretString": "hello-world",
	})

	resp := smRequest(t, srv, "GetSecretValue", map[string]interface{}{
		"SecretId": "get-secret",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readSMBody(t, resp)
	assert.Equal(t, "hello-world", body["SecretString"])

	// Missing secret.
	resp2 := smRequest(t, srv, "GetSecretValue", map[string]interface{}{
		"SecretId": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, resp2.StatusCode)
}

func TestSMPlugin_PutSecretValue(t *testing.T) {
	srv := newSMTestServer(t)

	smRequest(t, srv, "CreateSecret", map[string]interface{}{
		"Name":         "put-secret",
		"SecretString": "v1",
	})

	resp := smRequest(t, srv, "PutSecretValue", map[string]interface{}{
		"SecretId":     "put-secret",
		"SecretString": "v2",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// GetSecretValue should now return v2.
	resp2 := smRequest(t, srv, "GetSecretValue", map[string]interface{}{
		"SecretId": "put-secret",
	})
	body2 := readSMBody(t, resp2)
	assert.Equal(t, "v2", body2["SecretString"])
}

func TestSMPlugin_DeleteSecret(t *testing.T) {
	srv := newSMTestServer(t)

	smRequest(t, srv, "CreateSecret", map[string]interface{}{
		"Name":         "delete-secret",
		"SecretString": "temp",
	})

	resp := smRequest(t, srv, "DeleteSecret", map[string]interface{}{
		"SecretId": "delete-secret",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Should be gone.
	resp2 := smRequest(t, srv, "GetSecretValue", map[string]interface{}{
		"SecretId": "delete-secret",
	})
	assert.Equal(t, http.StatusNotFound, resp2.StatusCode)
}

func TestSMPlugin_ListSecrets(t *testing.T) {
	srv := newSMTestServer(t)

	for _, name := range []string{"secret-a", "secret-b", "secret-c"} {
		smRequest(t, srv, "CreateSecret", map[string]interface{}{
			"Name":         name,
			"SecretString": "value",
		})
	}

	resp := smRequest(t, srv, "ListSecrets", map[string]interface{}{})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readSMBody(t, resp)
	secretList, ok := body["SecretList"].([]interface{})
	require.True(t, ok, "SecretList not found")
	assert.Len(t, secretList, 3)
}

func TestSMPlugin_RotateSecret(t *testing.T) {
	srv := newSMTestServer(t)

	smRequest(t, srv, "CreateSecret", map[string]interface{}{
		"Name":         "rotate-secret",
		"SecretString": "value",
	})

	resp := smRequest(t, srv, "RotateSecret", map[string]interface{}{
		"SecretId": "rotate-secret",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// DescribeSecret should show RotationEnabled = true.
	resp2 := smRequest(t, srv, "DescribeSecret", map[string]interface{}{
		"SecretId": "rotate-secret",
	})
	body2 := readSMBody(t, resp2)
	assert.Equal(t, true, body2["RotationEnabled"])
}
