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

// The raw-bytes assertions scripts/wire-bookkeeping-projected.txt cites for SecretState (#756).
//
// SecretState declares AccountID and Region under wire-visible `json` tags, and EverTagged as
// `ever_tagged,omitempty`, because the record is what MemoryStateManager snapshots and a replay reads
// back. None reaches a body: DescribeSecret answers smDescribeSecretBody, which emits only the members
// the page publishes, and every other operation answers a map of published members. So Secrets
// Manager was already projected in code, and what it lacked was this file.

// smWireAccount and smWireRegion scope every state key the plugin writes.
const (
	smWireAccount = "123456789012"
	smWireRegion  = "us-east-1"
)

// smBookkeepingMembers are the members SecretState declares and no Secrets Manager shape publishes.
// `ever_tagged` is listed in its own right because a fold does not reach a snake_case spelling.
var smBookkeepingMembers = []string{"AccountID", "Region", "EverTagged", "ever_tagged"}

// setupSMWirePlugin returns the Secrets Manager plugin, a request context and the state manager
// behind it.
func setupSMWirePlugin(t *testing.T) (*emulator.SecretsManagerPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	p := &emulator.SecretsManagerPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(time.Unix(1700000000, 0).UTC())},
	}), "emulator.SecretsManagerPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: smWireAccount,
		Region:    smWireRegion,
		RequestID: "req-sm-wire",
		IDs:       emulator.NewIDMint("req-sm-wire"),
	}, state
}

// smWire issues one JSON-target operation and returns the raw response body, failing on anything but
// 200.
func smWire(t *testing.T, p *emulator.SecretsManagerPlugin, ctx *emulator.RequestContext, op string, body map[string]any) []byte {
	t.Helper()
	if body == nil {
		body = map[string]any{}
	}
	raw, err := json.Marshal(body)
	require.NoError(t, err, "marshal %s", op)
	resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
		Service:   "secretsmanager",
		Operation: op,
		Path:      "/",
		Body:      raw,
		Headers:   map[string]string{"X-Amz-Target": "secretsmanager." + op, "Content-Type": "application/x-amz-json-1.1"},
		Params:    map[string]string{},
	})
	require.NoError(t, err, "%s", op)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)
	return resp.Body
}

// smWireRecord returns the secret record as raw JSON.
func smWireRecord(t *testing.T, state emulator.StateManager, name string) map[string]json.RawMessage {
	t.Helper()
	key := "secret:" + smWireAccount + "/" + smWireRegion + "/" + name
	data, err := state.Get(t.Context(), "secretsmanager", key)
	require.NoError(t, err, "state.Get %s", key)
	require.NotNil(t, data, "no record stored at %s", key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	return record
}

func TestSecretsManagerWire_SecretResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupSMWirePlugin(t)

	const name = "wire/secret"
	created := smWire(t, p, ctx, "CreateSecret", map[string]any{"Name": name, "SecretString": "v1"})
	record := smWireRecord(t, state, name)
	require.JSONEq(t, `"`+smWireAccount+`"`, string(record["AccountID"]), "the secret must persist AccountID")
	require.JSONEq(t, `"`+smWireRegion+`"`, string(record["Region"]), "the secret must persist Region")

	// ever_tagged is `,omitempty` and set only by a tag write (#938): tag, then read it back, before
	// any response is walked (#1304).
	tagged := smWire(t, p, ctx, "TagResource", map[string]any{"SecretId": name, "Tags": []map[string]string{{"Key": "team", "Value": "wire"}}})
	require.JSONEq(t, "true", string(smWireRecord(t, state, name)["ever_tagged"]),
		"the secret must persist ever_tagged before an absence assertion on it means anything")

	secret := map[string]any{"SecretId": name}
	for _, tc := range []struct {
		op     string
		body   map[string]any
		held   []byte
		anchor string
	}{
		{op: "CreateSecret", held: created, anchor: `"Name":"` + name + `"`},
		{op: "TagResource", held: tagged, anchor: "{}"},
		{op: "DescribeSecret", body: secret, anchor: `"Name":"` + name + `"`},
		{op: "ListSecrets", anchor: `"Name":"` + name + `"`},
		{op: "GetSecretValue", body: secret, anchor: `"SecretString":"v1"`},
		{op: "PutSecretValue", body: map[string]any{"SecretId": name, "SecretString": "v2"}, anchor: `"VersionId"`},
		{op: "ListSecretVersionIds", body: secret, anchor: `"Versions"`},
		{op: "UpdateSecret", body: map[string]any{"SecretId": name, "Description": "wire"}, anchor: `"Name":"` + name + `"`},
		// SDKs fill ClientRequestToken in themselves; a raw request has to send one, and a secret with
		// no rotation configured needs the function named.
		{op: "RotateSecret", body: map[string]any{"SecretId": name, "ClientRequestToken": "11111111-2222-3333-4444-555555555555",
			"RotationLambdaARN": "arn:aws:lambda:" + smWireRegion + ":" + smWireAccount + ":function:wire-rotation"},
			anchor: `"Name":"` + name + `"`},
		{op: "UntagResource", body: map[string]any{"SecretId": name, "TagKeys": []string{"team"}}, anchor: "{}"},
		{op: "DeleteSecret", body: map[string]any{"SecretId": name, "RecoveryWindowInDays": 7}, anchor: `"Name":"` + name + `"`},
		// After the delete, so there is a deletion to cancel; the record survives a scheduled delete.
		{op: "RestoreSecret", body: secret, anchor: `"Name":"` + name + `"`},
	} {
		t.Run(tc.op, func(t *testing.T) {
			body := tc.held
			if body == nil {
				body = smWire(t, p, ctx, tc.op, tc.body)
			}
			require.Containsf(t, string(body), tc.anchor,
				"presence anchor: %s has to render %s for an absence to mean anything", tc.op, tc.anchor)
			wireAssertNoMemberJSON(t, tc.op, body, smBookkeepingMembers)
		})
	}
}
