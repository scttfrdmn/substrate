package substrate_test

import (
	"bytes"
	"context"
	"encoding/base64"
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

func newKMSTestServer(t *testing.T) *substrate.Server {
	t.Helper()
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelInfo, false)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := substrate.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

	kmsPlugin := &substrate.KMSPlugin{}
	require.NoError(t, kmsPlugin.Initialize(context.TODO(), substrate.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(kmsPlugin)

	return substrate.NewServer(*cfg, registry, store, state, tc, logger)
}

func kmsRequest(t *testing.T, srv *substrate.Server, operation string, body interface{}) *http.Response {
	t.Helper()
	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	r.Host = "kms.us-east-1.amazonaws.com"
	r.Header.Set("Content-Type", "application/x-amz-json-1.1")
	r.Header.Set("X-Amz-Target", "TrentService."+operation)

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Result()
}

func readKMSBody(t *testing.T, r *http.Response) map[string]interface{} {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	var out map[string]interface{}
	_ = json.Unmarshal(body, &out)
	return out
}

func TestKMSPlugin_CreateKey(t *testing.T) {
	srv := newKMSTestServer(t)

	resp := kmsRequest(t, srv, "CreateKey", map[string]interface{}{
		"Description": "test key",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readKMSBody(t, resp)
	meta, ok := body["KeyMetadata"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "Enabled", meta["KeyState"])
	assert.Equal(t, true, meta["Enabled"])
	assert.NotEmpty(t, meta["KeyId"])
}

func TestKMSPlugin_DescribeKey(t *testing.T) {
	srv := newKMSTestServer(t)

	createResp := kmsRequest(t, srv, "CreateKey", map[string]interface{}{})
	createBody := readKMSBody(t, createResp)
	meta := createBody["KeyMetadata"].(map[string]interface{})
	keyID := meta["KeyId"].(string)

	// Describe by key ID.
	resp := kmsRequest(t, srv, "DescribeKey", map[string]interface{}{
		"KeyId": keyID,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readKMSBody(t, resp)
	meta2 := body["KeyMetadata"].(map[string]interface{})
	assert.Equal(t, keyID, meta2["KeyId"])

	// Non-existent key.
	resp2 := kmsRequest(t, srv, "DescribeKey", map[string]interface{}{
		"KeyId": "00000000-0000-0000-0000-000000000000",
	})
	assert.Equal(t, http.StatusNotFound, resp2.StatusCode)
}

func TestKMSPlugin_EncryptDecrypt(t *testing.T) {
	srv := newKMSTestServer(t)

	createResp := kmsRequest(t, srv, "CreateKey", map[string]interface{}{})
	createBody := readKMSBody(t, createResp)
	meta := createBody["KeyMetadata"].(map[string]interface{})
	keyID := meta["KeyId"].(string)

	// Encrypt.
	plaintext := base64.StdEncoding.EncodeToString([]byte("my-secret-value"))
	encResp := kmsRequest(t, srv, "Encrypt", map[string]interface{}{
		"KeyId":     keyID,
		"Plaintext": plaintext,
	})
	assert.Equal(t, http.StatusOK, encResp.StatusCode)
	encBody := readKMSBody(t, encResp)
	ciphertext, ok := encBody["CiphertextBlob"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, ciphertext)

	// Decrypt.
	decResp := kmsRequest(t, srv, "Decrypt", map[string]interface{}{
		"CiphertextBlob": ciphertext,
	})
	assert.Equal(t, http.StatusOK, decResp.StatusCode)
	decBody := readKMSBody(t, decResp)
	plaintextOut, ok := decBody["Plaintext"].(string)
	require.True(t, ok)
	assert.Equal(t, plaintext, plaintextOut)
}

func TestKMSPlugin_CreateAlias_DescribeKeyViaAlias(t *testing.T) {
	srv := newKMSTestServer(t)

	createResp := kmsRequest(t, srv, "CreateKey", map[string]interface{}{})
	createBody := readKMSBody(t, createResp)
	meta := createBody["KeyMetadata"].(map[string]interface{})
	keyID := meta["KeyId"].(string)

	// Create alias.
	aliasResp := kmsRequest(t, srv, "CreateAlias", map[string]interface{}{
		"AliasName":   "alias/my-key",
		"TargetKeyId": keyID,
	})
	assert.Equal(t, http.StatusOK, aliasResp.StatusCode)

	// Describe via alias.
	resp := kmsRequest(t, srv, "DescribeKey", map[string]interface{}{
		"KeyId": "alias/my-key",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readKMSBody(t, resp)
	meta2 := body["KeyMetadata"].(map[string]interface{})
	assert.Equal(t, keyID, meta2["KeyId"])
}

func TestKMSPlugin_ListKeys(t *testing.T) {
	srv := newKMSTestServer(t)

	// Create 3 keys.
	for i := 0; i < 3; i++ {
		kmsRequest(t, srv, "CreateKey", map[string]interface{}{})
	}

	resp := kmsRequest(t, srv, "ListKeys", map[string]interface{}{})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readKMSBody(t, resp)
	keys, ok := body["Keys"].([]interface{})
	require.True(t, ok)
	assert.Len(t, keys, 3)
}
