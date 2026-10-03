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

// The raw-bytes assertions scripts/wire-bookkeeping-projected.txt cites for SSMParameter (#756).
//
// SSMParameter declares AccountID and Region under wire-visible `json` tags, and EverTagged as
// `ever_tagged,omitempty`, because the record is what MemoryStateManager snapshots and a replay reads
// back. None reaches a body: the reads answer paramItem and its history and metadata counterparts,
// built member by member, and the writes answer maps of published members. So SSM was already
// projected in code, and what it lacked was this file.

// ssmWireAccount and ssmWireRegion scope every state key the plugin writes.
const (
	ssmWireAccount = "123456789012"
	ssmWireRegion  = "us-east-1"
)

// ssmBookkeepingMembers are the members SSMParameter declares and no SSM shape publishes. `ever_tagged`
// is listed in its own right because a fold does not reach a snake_case spelling.
var ssmBookkeepingMembers = []string{"AccountID", "Region", "EverTagged", "ever_tagged"}

// setupSSMWirePlugin returns the SSM plugin, a request context and the state manager behind it.
func setupSSMWirePlugin(t *testing.T) (*emulator.SSMPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	p := &emulator.SSMPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(time.Unix(1700000000, 0).UTC())},
	}), "emulator.SSMPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: ssmWireAccount,
		Region:    ssmWireRegion,
		RequestID: "req-ssm-wire",
		IDs:       emulator.NewIDMint("req-ssm-wire"),
	}, state
}

// ssmWire issues one JSON-target operation and returns the raw response body, failing on anything but
// 200.
func ssmWire(t *testing.T, p *emulator.SSMPlugin, ctx *emulator.RequestContext, op string, body map[string]any) []byte {
	t.Helper()
	if body == nil {
		body = map[string]any{}
	}
	raw, err := json.Marshal(body)
	require.NoError(t, err, "marshal %s", op)
	resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
		Service:   "ssm",
		Operation: op,
		Path:      "/",
		Body:      raw,
		Headers:   map[string]string{"X-Amz-Target": "AmazonSSM." + op, "Content-Type": "application/x-amz-json-1.1"},
		Params:    map[string]string{},
	})
	require.NoError(t, err, "%s", op)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)
	return resp.Body
}

// ssmWireRecord returns the parameter record as raw JSON.
func ssmWireRecord(t *testing.T, state emulator.StateManager, name string) map[string]json.RawMessage {
	t.Helper()
	key := "parameter:" + ssmWireAccount + "/" + ssmWireRegion + "/" + name
	data, err := state.Get(t.Context(), "ssm", key)
	require.NoError(t, err, "state.Get %s", key)
	require.NotNil(t, data, "no record stored at %s", key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	return record
}

func TestSSMWire_ParameterResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupSSMWirePlugin(t)

	const name = "/wire/param"
	created := ssmWire(t, p, ctx, "PutParameter", map[string]any{"Name": name, "Value": "v1", "Type": "String"})
	record := ssmWireRecord(t, state, name)
	require.JSONEq(t, `"`+ssmWireAccount+`"`, string(record["AccountID"]), "the parameter must persist AccountID")
	require.JSONEq(t, `"`+ssmWireRegion+`"`, string(record["Region"]), "the parameter must persist Region")

	// ever_tagged is `,omitempty` and set only by a tag write (#938): tag, then read it back, before
	// any response is walked (#1304).
	resource := map[string]any{"ResourceType": "Parameter", "ResourceId": name}
	tagged := ssmWire(t, p, ctx, "AddTagsToResource", map[string]any{
		"ResourceType": "Parameter", "ResourceId": name, "Tags": []map[string]string{{"Key": "team", "Value": "wire"}},
	})
	require.JSONEq(t, "true", string(ssmWireRecord(t, state, name)["ever_tagged"]),
		"the parameter must persist ever_tagged before an absence assertion on it means anything")

	for _, tc := range []struct {
		op     string
		body   map[string]any
		held   []byte
		anchor string
	}{
		{op: "PutParameter", held: created, anchor: `"Version":1`},
		{op: "AddTagsToResource", held: tagged, anchor: "{}"},
		{op: "GetParameter", body: map[string]any{"Name": name}, anchor: `"Name":"` + name + `"`},
		{op: "GetParameters", body: map[string]any{"Names": []string{name}}, anchor: `"Name":"` + name + `"`},
		{op: "GetParametersByPath", body: map[string]any{"Path": "/wire"}, anchor: `"Name":"` + name + `"`},
		{op: "DescribeParameters", anchor: `"Name":"` + name + `"`},
		{op: "GetParameterHistory", body: map[string]any{"Name": name}, anchor: `"Name":"` + name + `"`},
		{op: "LabelParameterVersion", body: map[string]any{"Name": name, "ParameterVersion": 1, "Labels": []string{"wire"}}, anchor: `"ParameterVersion":1`},
		{op: "ListTagsForResource", body: resource, anchor: `"Key":"team"`},
		{op: "RemoveTagsFromResource", body: map[string]any{"ResourceType": "Parameter", "ResourceId": name, "TagKeys": []string{"team"}}, anchor: "{}"},
		// Last: they remove the records every case above reads.
		{op: "DeleteParameters", body: map[string]any{"Names": []string{name, "/wire/absent"}}, anchor: `"DeletedParameters"`},
	} {
		t.Run(tc.op, func(t *testing.T) {
			body := tc.held
			if body == nil {
				body = ssmWire(t, p, ctx, tc.op, tc.body)
			}
			require.Containsf(t, string(body), tc.anchor,
				"presence anchor: %s has to render %s for an absence to mean anything", tc.op, tc.anchor)
			wireAssertNoMemberJSON(t, tc.op, body, ssmBookkeepingMembers)
		})
	}

	// DeleteParameter answers the single-parameter delete on a parameter of its own.
	ssmWire(t, p, ctx, "PutParameter", map[string]any{"Name": "/wire/other", "Value": "v", "Type": "String"})
	t.Run("DeleteParameter", func(t *testing.T) {
		body := ssmWire(t, p, ctx, "DeleteParameter", map[string]any{"Name": "/wire/other"})
		wireAssertNoMemberJSON(t, "DeleteParameter", body, ssmBookkeepingMembers)
	})
}
