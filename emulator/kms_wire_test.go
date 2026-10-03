package emulator_test

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The raw-bytes assertions scripts/wire-bookkeeping-projected.txt cites for KMSKey (#756).
//
// KMSKey declares AccountID and Region under wire-visible `json` tags, and EverTagged as
// `ever_tagged,omitempty`, because the record is what MemoryStateManager snapshots and a replay reads
// back. None reaches a body: DescribeKey and CreateKey answer KeyMetadata built member by member
// (emulator/kms_key_metadata.go), and every other operation answers a map of published members. So
// KMS was already projected in code, and what it lacked was this file.

// kmsWireClock is the instant every fixture below starts the simulated clock at. No assertion equates
// a timestamp; this file only walks member names.
var kmsWireClock = time.Unix(1700000000, 0).UTC()

// kmsWireAccount and kmsWireRegion scope every state key the plugin writes.
const (
	kmsWireAccount = "123456789012"
	kmsWireRegion  = "us-east-1"
)

// kmsBookkeepingMembers are the members KMSKey declares and no KMS shape publishes. `ever_tagged` is
// listed in its own right because a fold does not reach a snake_case spelling.
var kmsBookkeepingMembers = []string{"AccountID", "Region", "EverTagged", "ever_tagged"}

// setupKMSWirePlugin returns the KMS plugin, a request context and the state manager behind it.
func setupKMSWirePlugin(t *testing.T) (*emulator.KMSPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	p := &emulator.KMSPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(kmsWireClock)},
	}), "emulator.KMSPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: kmsWireAccount,
		Region:    kmsWireRegion,
		RequestID: "req-kms-wire",
		IDs:       emulator.NewIDMint("req-kms-wire"),
	}, state
}

// kmsWire issues one JSON-target operation and returns the raw response body, failing on anything
// but 200.
func kmsWire(t *testing.T, p *emulator.KMSPlugin, ctx *emulator.RequestContext, op string, body map[string]any) []byte {
	t.Helper()
	if body == nil {
		body = map[string]any{}
	}
	raw, err := json.Marshal(body)
	require.NoError(t, err, "marshal %s", op)
	resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
		Service:   "kms",
		Operation: op,
		Path:      "/",
		Body:      raw,
		Headers:   map[string]string{"X-Amz-Target": "TrentService." + op, "Content-Type": "application/x-amz-json-1.1"},
		Params:    map[string]string{},
	})
	require.NoError(t, err, "%s", op)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)
	return resp.Body
}

// kmsWireRecord returns the key record as raw JSON.
func kmsWireRecord(t *testing.T, state emulator.StateManager, keyID string) map[string]json.RawMessage {
	t.Helper()
	key := "key:" + kmsWireAccount + "/" + kmsWireRegion + "/" + keyID
	data, err := state.Get(t.Context(), "kms", key)
	require.NoError(t, err, "state.Get %s", key)
	require.NotNil(t, data, "no record stored at %s", key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	return record
}

func TestKMSWire_KeyResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupKMSWirePlugin(t)

	created := kmsWire(t, p, ctx, "CreateKey", map[string]any{"Description": "kms wire"})
	var out struct {
		KeyMetadata struct {
			KeyID string `json:"KeyId"`
		} `json:"KeyMetadata"`
	}
	require.NoError(t, json.Unmarshal(created, &out), "decode CreateKey: %s", created)
	keyID := out.KeyMetadata.KeyID
	require.NotEmpty(t, keyID, "CreateKey must report a key id")

	record := kmsWireRecord(t, state, keyID)
	require.JSONEq(t, `"`+kmsWireAccount+`"`, string(record["AccountID"]), "the key must persist AccountID")
	require.JSONEq(t, `"`+kmsWireRegion+`"`, string(record["Region"]), "the key must persist Region")

	// ever_tagged is `,omitempty` and set only by a tag write (#938): tag, then read it back, before
	// any response is walked (#1304).
	tagged := kmsWire(t, p, ctx, "TagResource", map[string]any{"KeyId": keyID, "Tags": []map[string]string{{"TagKey": "team", "TagValue": "wire"}}})
	require.JSONEq(t, "true", string(kmsWireRecord(t, state, keyID)["ever_tagged"]),
		"the key must persist ever_tagged before an absence assertion on it means anything")

	key := map[string]any{"KeyId": keyID}
	encrypted := kmsWire(t, p, ctx, "Encrypt", map[string]any{"KeyId": keyID, "Plaintext": "d2lyZQ=="})
	var enc struct {
		CiphertextBlob string `json:"CiphertextBlob"`
	}
	require.NoError(t, json.Unmarshal(encrypted, &enc), "decode Encrypt: %s", encrypted)

	for _, tc := range []struct {
		op     string
		body   map[string]any
		held   []byte
		anchor string
	}{
		{op: "CreateKey", held: created, anchor: `"KeyId":"` + keyID + `"`},
		{op: "TagResource", held: tagged, anchor: "{}"},
		{op: "DescribeKey", body: key, anchor: `"KeyId":"` + keyID + `"`},
		{op: "ListKeys", anchor: `"KeyId":"` + keyID + `"`},
		{op: "ListResourceTags", body: key, anchor: `"TagKey":"team"`},
		{op: "GetKeyPolicy", body: map[string]any{"KeyId": keyID, "PolicyName": "default"}, anchor: `"Policy"`},
		{op: "PutKeyPolicy", body: map[string]any{"KeyId": keyID, "PolicyName": "default", "Policy": `{"Version":"2012-10-17","Statement":[]}`}, anchor: "{}"},
		{op: "EnableKeyRotation", body: key, anchor: "{}"},
		{op: "GetKeyRotationStatus", body: key, anchor: `"KeyRotationEnabled":true`},
		{op: "DisableKeyRotation", body: key, anchor: "{}"},
		{op: "Encrypt", held: encrypted, anchor: `"CiphertextBlob"`},
		{op: "Decrypt", body: map[string]any{"CiphertextBlob": enc.CiphertextBlob}, anchor: `"Plaintext"`},
		{op: "ReEncrypt", body: map[string]any{"CiphertextBlob": enc.CiphertextBlob, "DestinationKeyId": keyID}, anchor: `"CiphertextBlob"`},
		{op: "GenerateDataKey", body: map[string]any{"KeyId": keyID, "KeySpec": "AES_256"}, anchor: `"CiphertextBlob"`},
		{op: "GenerateDataKeyWithoutPlaintext", body: map[string]any{"KeyId": keyID, "KeySpec": "AES_256"}, anchor: `"CiphertextBlob"`},
		{op: "CreateAlias", body: map[string]any{"AliasName": "alias/wire", "TargetKeyId": keyID}, anchor: "{}"},
		{op: "ListAliases", anchor: `"AliasName":"alias/wire"`},
		{op: "UpdateAlias", body: map[string]any{"AliasName": "alias/wire", "TargetKeyId": keyID}, anchor: "{}"},
		{op: "DeleteAlias", body: map[string]any{"AliasName": "alias/wire"}, anchor: "{}"},
		{op: "UntagResource", body: map[string]any{"KeyId": keyID, "TagKeys": []string{"team"}}, anchor: "{}"},
		{op: "DisableKey", body: key, anchor: "{}"},
		{op: "EnableKey", body: key, anchor: "{}"},
		{op: "ScheduleKeyDeletion", body: map[string]any{"KeyId": keyID, "PendingWindowInDays": 7}, anchor: `"KeyId"`},
		{op: "CancelKeyDeletion", body: key, anchor: `"KeyId"`},
	} {
		t.Run(tc.op, func(t *testing.T) {
			body := tc.held
			if body == nil {
				body = kmsWire(t, p, ctx, tc.op, tc.body)
			}
			require.Containsf(t, string(body), tc.anchor,
				"presence anchor: %s has to render %s for an absence to mean anything", tc.op, tc.anchor)
			wireAssertNoMemberJSON(t, tc.op, body, kmsBookkeepingMembers)
		})
	}
}
