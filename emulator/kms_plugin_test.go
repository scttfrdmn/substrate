package emulator_test

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

	"github.com/scttfrdmn/substrate/emulator"
)

func newKMSTestServer(t *testing.T) *emulator.Server {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelInfo, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

	kmsPlugin := &emulator.KMSPlugin{}
	require.NoError(t, kmsPlugin.Initialize(context.TODO(), emulator.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(kmsPlugin)

	return emulator.NewServer(*cfg, registry, store, state, tc, logger)
}

func kmsRequest(t *testing.T, srv *emulator.Server, operation string, body interface{}) *http.Response {
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

	// Non-existent key. The status is 400, which is what API_DescribeKey publishes for
	// NotFoundException; this read 404 until #923. The code is asserted alongside it, because a
	// status alone was what let the wrong one stand for as long as it did.
	resp2 := kmsRequest(t, srv, "DescribeKey", map[string]interface{}{
		"KeyId": "00000000-0000-0000-0000-000000000000",
	})
	assert.Equal(t, http.StatusBadRequest, resp2.StatusCode)
	assert.Equal(t, "NotFoundException", readKMSBody(t, resp2)["__type"])
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

func TestKMSPlugin_EnableDisableKey(t *testing.T) {
	srv := newKMSTestServer(t)

	createResp := kmsRequest(t, srv, "CreateKey", map[string]interface{}{})
	createBody := readKMSBody(t, createResp)
	meta := createBody["KeyMetadata"].(map[string]interface{})
	keyID := meta["KeyId"].(string)

	// Disable.
	resp := kmsRequest(t, srv, "DisableKey", map[string]interface{}{"KeyId": keyID})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify disabled.
	desc := kmsRequest(t, srv, "DescribeKey", map[string]interface{}{"KeyId": keyID})
	body := readKMSBody(t, desc)
	km := body["KeyMetadata"].(map[string]interface{})
	assert.Equal(t, "Disabled", km["KeyState"])
	assert.Equal(t, false, km["Enabled"])

	// Re-enable.
	resp2 := kmsRequest(t, srv, "EnableKey", map[string]interface{}{"KeyId": keyID})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)

	desc2 := kmsRequest(t, srv, "DescribeKey", map[string]interface{}{"KeyId": keyID})
	body2 := readKMSBody(t, desc2)
	km2 := body2["KeyMetadata"].(map[string]interface{})
	assert.Equal(t, "Enabled", km2["KeyState"])
	assert.Equal(t, true, km2["Enabled"])
}

func TestKMSPlugin_ScheduleAndCancelKeyDeletion(t *testing.T) {
	srv := newKMSTestServer(t)

	createResp := kmsRequest(t, srv, "CreateKey", map[string]interface{}{})
	createBody := readKMSBody(t, createResp)
	meta := createBody["KeyMetadata"].(map[string]interface{})
	keyID := meta["KeyId"].(string)

	// Schedule deletion.
	resp := kmsRequest(t, srv, "ScheduleKeyDeletion", map[string]interface{}{
		"KeyId":               keyID,
		"PendingWindowInDays": 7,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readKMSBody(t, resp)
	assert.Equal(t, "PendingDeletion", body["KeyState"])

	// Cancel deletion. The key comes back Disabled, not Enabled — API_CancelKeyDeletion: "when this
	// operation succeeds, the key state of the KMS key is Disabled. To enable the KMS key, use
	// EnableKey." (#963)
	resp2 := kmsRequest(t, srv, "CancelKeyDeletion", map[string]interface{}{"KeyId": keyID})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)

	desc := kmsRequest(t, srv, "DescribeKey", map[string]interface{}{"KeyId": keyID})
	body2 := readKMSBody(t, desc)
	km := body2["KeyMetadata"].(map[string]interface{})
	assert.Equal(t, "Disabled", km["KeyState"])
}

func TestKMSPlugin_TagAndUntagResource(t *testing.T) {
	srv := newKMSTestServer(t)

	createResp := kmsRequest(t, srv, "CreateKey", map[string]interface{}{})
	createBody := readKMSBody(t, createResp)
	meta := createBody["KeyMetadata"].(map[string]interface{})
	keyID := meta["KeyId"].(string)

	// Tag.
	resp := kmsRequest(t, srv, "TagResource", map[string]interface{}{
		"KeyId": keyID,
		"Tags": []map[string]string{
			{"TagKey": "env", "TagValue": "test"},
			{"TagKey": "owner", "TagValue": "alice"},
		},
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// ListResourceTags.
	resp2 := kmsRequest(t, srv, "ListResourceTags", map[string]interface{}{"KeyId": keyID})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	body := readKMSBody(t, resp2)
	tags, ok := body["Tags"].([]interface{})
	require.True(t, ok)
	assert.Len(t, tags, 2)

	// Untag.
	resp3 := kmsRequest(t, srv, "UntagResource", map[string]interface{}{
		"KeyId":   keyID,
		"TagKeys": []string{"env"},
	})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)

	resp4 := kmsRequest(t, srv, "ListResourceTags", map[string]interface{}{"KeyId": keyID})
	body4 := readKMSBody(t, resp4)
	tags4, ok := body4["Tags"].([]interface{})
	require.True(t, ok)
	assert.Len(t, tags4, 1)
}

// TestKMSPlugin_KeyPolicy is the policy round trip (#983).
//
// It is a minimal round trip. The assertions that a stored policy replaces the default are in
// `kms_key_policy_test.go`, where the caller's document differs from the default in more than one
// member so a read of the default cannot pass for a read of the stored document.
func TestKMSPlugin_KeyPolicy(t *testing.T) {
	srv := newKMSTestServer(t)

	createResp := kmsRequest(t, srv, "CreateKey", map[string]interface{}{})
	createBody := readKMSBody(t, createResp)
	meta := createBody["KeyMetadata"].(map[string]interface{})
	keyID := meta["KeyId"].(string)

	policy := `{"Version":"2012-10-17","Statement":[]}`

	// PutKeyPolicy.
	resp := kmsRequest(t, srv, "PutKeyPolicy", map[string]interface{}{
		"KeyId":      keyID,
		"PolicyName": "default",
		"Policy":     policy,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// GetKeyPolicy.
	resp2 := kmsRequest(t, srv, "GetKeyPolicy", map[string]interface{}{
		"KeyId":      keyID,
		"PolicyName": "default",
	})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	body := readKMSBody(t, resp2)
	assert.Equal(t, policy, body["Policy"])
}

func TestKMSPlugin_KeyRotation(t *testing.T) {
	srv := newKMSTestServer(t)

	createResp := kmsRequest(t, srv, "CreateKey", map[string]interface{}{})
	createBody := readKMSBody(t, createResp)
	meta := createBody["KeyMetadata"].(map[string]interface{})
	keyID := meta["KeyId"].(string)

	// Enable rotation.
	resp := kmsRequest(t, srv, "EnableKeyRotation", map[string]interface{}{"KeyId": keyID})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// GetKeyRotationStatus.
	resp2 := kmsRequest(t, srv, "GetKeyRotationStatus", map[string]interface{}{"KeyId": keyID})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	body := readKMSBody(t, resp2)
	assert.Equal(t, true, body["KeyRotationEnabled"])

	// Disable rotation.
	resp3 := kmsRequest(t, srv, "DisableKeyRotation", map[string]interface{}{"KeyId": keyID})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)

	resp4 := kmsRequest(t, srv, "GetKeyRotationStatus", map[string]interface{}{"KeyId": keyID})
	body4 := readKMSBody(t, resp4)
	assert.Equal(t, false, body4["KeyRotationEnabled"])
}

func TestKMSPlugin_ListAliases(t *testing.T) {
	srv := newKMSTestServer(t)

	createResp := kmsRequest(t, srv, "CreateKey", map[string]interface{}{})
	createBody := readKMSBody(t, createResp)
	meta := createBody["KeyMetadata"].(map[string]interface{})
	keyID := meta["KeyId"].(string)

	kmsRequest(t, srv, "CreateAlias", map[string]interface{}{
		"AliasName":   "alias/test-a",
		"TargetKeyId": keyID,
	})
	kmsRequest(t, srv, "CreateAlias", map[string]interface{}{
		"AliasName":   "alias/test-b",
		"TargetKeyId": keyID,
	})

	resp := kmsRequest(t, srv, "ListAliases", map[string]interface{}{})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readKMSBody(t, resp)
	aliases, ok := body["Aliases"].([]interface{})
	require.True(t, ok)
	assert.Len(t, aliases, 2)
}

func TestKMSPlugin_DeleteAlias(t *testing.T) {
	srv := newKMSTestServer(t)

	createResp := kmsRequest(t, srv, "CreateKey", map[string]interface{}{})
	createBody := readKMSBody(t, createResp)
	meta := createBody["KeyMetadata"].(map[string]interface{})
	keyID := meta["KeyId"].(string)

	kmsRequest(t, srv, "CreateAlias", map[string]interface{}{
		"AliasName":   "alias/to-delete",
		"TargetKeyId": keyID,
	})

	// Delete the alias.
	resp := kmsRequest(t, srv, "DeleteAlias", map[string]interface{}{
		"AliasName": "alias/to-delete",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Now describe via alias should fail — at 400, which is the status API_DescribeKey publishes
	// for NotFoundException (#923).
	resp2 := kmsRequest(t, srv, "DescribeKey", map[string]interface{}{
		"KeyId": "alias/to-delete",
	})
	assert.Equal(t, http.StatusBadRequest, resp2.StatusCode)
	assert.Equal(t, "NotFoundException", readKMSBody(t, resp2)["__type"])
}

func TestKMSPlugin_UpdateAlias(t *testing.T) {
	srv := newKMSTestServer(t)

	c1 := readKMSBody(t, kmsRequest(t, srv, "CreateKey", map[string]interface{}{}))
	key1 := c1["KeyMetadata"].(map[string]interface{})["KeyId"].(string)

	c2 := readKMSBody(t, kmsRequest(t, srv, "CreateKey", map[string]interface{}{}))
	key2 := c2["KeyMetadata"].(map[string]interface{})["KeyId"].(string)

	kmsRequest(t, srv, "CreateAlias", map[string]interface{}{
		"AliasName":   "alias/switchable",
		"TargetKeyId": key1,
	})

	// Point the alias at key2.
	resp := kmsRequest(t, srv, "UpdateAlias", map[string]interface{}{
		"AliasName":   "alias/switchable",
		"TargetKeyId": key2,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Describe via alias should return key2.
	desc := readKMSBody(t, kmsRequest(t, srv, "DescribeKey", map[string]interface{}{
		"KeyId": "alias/switchable",
	}))
	km := desc["KeyMetadata"].(map[string]interface{})
	assert.Equal(t, key2, km["KeyId"])
}

func TestKMSPlugin_GenerateDataKey(t *testing.T) {
	srv := newKMSTestServer(t)

	createResp := kmsRequest(t, srv, "CreateKey", map[string]interface{}{})
	createBody := readKMSBody(t, createResp)
	meta := createBody["KeyMetadata"].(map[string]interface{})
	keyID := meta["KeyId"].(string)

	resp := kmsRequest(t, srv, "GenerateDataKey", map[string]interface{}{
		"KeyId":   keyID,
		"KeySpec": "AES_256",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readKMSBody(t, resp)
	assert.NotEmpty(t, body["Plaintext"])
	assert.NotEmpty(t, body["CiphertextBlob"])
}

func TestKMSPlugin_GenerateDataKeyWithoutPlaintext(t *testing.T) {
	srv := newKMSTestServer(t)

	c := readKMSBody(t, kmsRequest(t, srv, "CreateKey", map[string]interface{}{}))
	keyID := c["KeyMetadata"].(map[string]interface{})["KeyId"].(string)

	resp := kmsRequest(t, srv, "GenerateDataKeyWithoutPlaintext", map[string]interface{}{
		"KeyId":   keyID,
		"KeySpec": "AES_256",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readKMSBody(t, resp)
	assert.NotEmpty(t, body["CiphertextBlob"])
	assert.Empty(t, body["Plaintext"])
}

func TestKMSPlugin_ReEncrypt(t *testing.T) {
	srv := newKMSTestServer(t)

	c1 := readKMSBody(t, kmsRequest(t, srv, "CreateKey", map[string]interface{}{}))
	key1ID := c1["KeyMetadata"].(map[string]interface{})["KeyId"].(string)

	c2 := readKMSBody(t, kmsRequest(t, srv, "CreateKey", map[string]interface{}{}))
	key2ID := c2["KeyMetadata"].(map[string]interface{})["KeyId"].(string)

	// Encrypt with key1.
	plaintext := base64.StdEncoding.EncodeToString([]byte("reencrypt-test"))
	encBody := readKMSBody(t, kmsRequest(t, srv, "Encrypt", map[string]interface{}{
		"KeyId":     key1ID,
		"Plaintext": plaintext,
	}))
	ciphertext := encBody["CiphertextBlob"].(string)

	// ReEncrypt to key2.
	resp := kmsRequest(t, srv, "ReEncrypt", map[string]interface{}{
		"CiphertextBlob":   ciphertext,
		"DestinationKeyId": key2ID,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := readKMSBody(t, resp)
	newCiphertext, ok := body["CiphertextBlob"].(string)
	require.True(t, ok)
	assert.NotEqual(t, ciphertext, newCiphertext)

	// Decrypt the re-encrypted blob.
	decBody := readKMSBody(t, kmsRequest(t, srv, "Decrypt", map[string]interface{}{
		"CiphertextBlob": newCiphertext,
	}))
	assert.Equal(t, plaintext, decBody["Plaintext"])
}
