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

// The raw-bytes assertion scripts/wire-bookkeeping-projected.txt cites for EBRule (#756, #1607).
//
// EBRule declares EverTagged as `ever_tagged,omitempty` and carries the rule's Tags, because the
// record is what MemoryStateManager snapshots and a replay reads back. Neither reaches a body:
// DescribeRule and ListRules answer through ebRuleWire, which carries only the members their pages
// publish, and the tag operations answer the tag list alone.

// ebBookkeepingMembers are the members EBRule declares that no rule response publishes. Tags is one:
// ListTagsForResource is its read path, not DescribeRule or ListRules.
var ebBookkeepingMembers = []string{"EverTagged", "ever_tagged"}

func TestEventBridgeWire_RuleResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	p := &emulator.EventBridgePlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(time.Unix(1700000000, 0).UTC())},
	}))
	ctx := &emulator.RequestContext{
		AccountID: "123456789012", Region: "us-east-1", RequestID: "req-eb-wire", IDs: emulator.NewIDMint("req-eb-wire"),
	}
	call := func(op string, body map[string]any) []byte {
		t.Helper()
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
			Service: "eventbridge", Operation: op, Path: "/", Body: raw,
			Headers: map[string]string{"X-Amz-Target": "AWSEvents." + op, "Content-Type": "application/x-amz-json-1.1"},
			Params:  map[string]string{},
		})
		require.NoError(t, err, "%s", op)
		require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)
		return resp.Body
	}

	const arn = "arn:aws:events:us-east-1:123456789012:rule/wire"
	call("PutRule", map[string]any{"Name": "wire", "EventPattern": `{"source":["x"]}`})
	call("TagResource", map[string]any{"ResourceARN": arn, "Tags": []map[string]string{{"Key": "k", "Value": "v"}}})
	call("UntagResource", map[string]any{"ResourceARN": arn, "TagKeys": []string{"k"}})

	// ever_tagged is set only by a tag write (#938): read it back before any absence assertion.
	data, err := state.Get(t.Context(), "eventbridge", "ebrule:123456789012/us-east-1/wire")
	require.NoError(t, err)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record))
	require.JSONEq(t, "true", string(record["ever_tagged"]), "the rule must persist ever_tagged")
	call("TagResource", map[string]any{"ResourceARN": arn, "Tags": []map[string]string{{"Key": "k", "Value": "v"}}})

	for _, tc := range []struct {
		op      string
		body    map[string]any
		anchor  string
		members []string
	}{
		{op: "DescribeRule", body: map[string]any{"Name": "wire"}, anchor: `"Name":"wire"`, members: append([]string{"Tags"}, ebBookkeepingMembers...)},
		{op: "ListRules", body: map[string]any{}, anchor: `"Name":"wire"`, members: append([]string{"Tags"}, ebBookkeepingMembers...)},
		{op: "PutRule", body: map[string]any{"Name": "wire", "EventPattern": `{"source":["y"]}`}, anchor: `"RuleArn"`, members: append([]string{"Tags"}, ebBookkeepingMembers...)},
		{op: "ListTagsForResource", body: map[string]any{"ResourceARN": arn}, anchor: `"Key":"k"`, members: ebBookkeepingMembers},
	} {
		body := call(tc.op, tc.body)
		require.Containsf(t, string(body), tc.anchor, "presence anchor for %s", tc.op)
		wireAssertNoMemberJSON(t, tc.op, body, tc.members)
	}
}

// TestEventBridgeWire_BusResponsesCarryNoBookkeepingMember is the raw-bytes assertion
// scripts/wire-bookkeeping-projected.txt cites for EBEventBus: its CreatedAt, UpdatedAt and
// EverTagged are persisted and reach no bus response, which render through ebEventBusWire.
func TestEventBridgeWire_BusResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	p := &emulator.EventBridgePlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(time.Unix(1700000000, 0).UTC())},
	}))
	ctx := &emulator.RequestContext{
		AccountID: "123456789012", Region: "us-east-1", RequestID: "req-eb-bus-wire", IDs: emulator.NewIDMint("req-eb-bus-wire"),
	}
	call := func(op string, body map[string]any) []byte {
		t.Helper()
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
			Service: "eventbridge", Operation: op, Path: "/", Body: raw,
			Headers: map[string]string{"X-Amz-Target": "AWSEvents." + op, "Content-Type": "application/x-amz-json-1.1"},
			Params:  map[string]string{},
		})
		require.NoError(t, err, "%s", op)
		require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)
		return resp.Body
	}

	const arn = "arn:aws:events:us-east-1:123456789012:event-bus/wire"
	tags := []map[string]string{{"Key": "k", "Value": "v"}}
	created := call("CreateEventBus", map[string]any{"Name": "wire", "Description": "d", "Tags": tags})
	call("UntagResource", map[string]any{"ResourceARN": arn, "TagKeys": []string{"k"}})
	call("PutPermission", map[string]any{"EventBusName": "wire", "StatementId": "s", "Action": "events:PutEvents", "Principal": "111122223333"})

	data, err := state.Get(t.Context(), "eventbridge", "ebbus:123456789012/us-east-1/wire")
	require.NoError(t, err)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record))
	for _, member := range []string{"ever_tagged", "created_at", "updated_at"} {
		require.Contains(t, record, member, "the bus must persist %s", member)
	}
	call("TagResource", map[string]any{"ResourceARN": arn, "Tags": tags})

	members := []string{"Tags", "EverTagged", "ever_tagged", "CreatedAt", "created_at", "UpdatedAt", "updated_at"}
	for _, tc := range []struct {
		op      string
		body    []byte
		anchor  string
		members []string
	}{
		{op: "CreateEventBus", body: created, anchor: `"EventBusArn"`, members: members},
		{op: "DescribeEventBus", body: call("DescribeEventBus", map[string]any{"Name": "wire"}), anchor: `"CreationTime"`, members: members},
		{op: "ListEventBuses", body: call("ListEventBuses", map[string]any{}), anchor: `"Name":"wire"`, members: members},
		{op: "UpdateEventBus", body: call("UpdateEventBus", map[string]any{"Name": "wire", "Description": "e"}), anchor: `"Arn"`, members: members},
		{op: "ListTagsForResource", body: call("ListTagsForResource", map[string]any{"ResourceARN": arn}), anchor: `"Key":"k"`, members: members[1:]},
	} {
		require.Containsf(t, string(tc.body), tc.anchor, "presence anchor for %s", tc.op)
		wireAssertNoMemberJSON(t, tc.op, tc.body, tc.members)
	}
}
