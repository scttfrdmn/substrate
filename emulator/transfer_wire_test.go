package emulator_test

import (
	"encoding/json"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The raw-bytes assertions scripts/wire-bookkeeping-projected.txt cites for Transfer's two records
// (#756).
//
// TransferServer declares AccountID, Region and CreatedAt and TransferUser declares AccountID and
// Region, none under omitempty, so a record that exists holds every one of them and no absence below
// is vacuous. DescribeServer and DescribeUser used to answer those records whole; they answer
// emulator/transfer_wire.go's projections now, and this file is what says so. The other eight routed
// operations build a map of published members and are driven anyway, so the list of ten reads as
// complete rather than as a sample.
//
// Absence rather than exact membership: DescribeServer answers 8 of API_DescribedServer's members and
// DescribeUser 5 of API_DescribedUser's (#1199), and an exact expectation would have to be rewritten
// by the PR that closes that gap.

// transferWireClock is the instant every fixture below starts the simulated clock at. No assertion
// equates a timestamp; this file only walks member names.
var transferWireClock = time.Unix(1700000000, 0).UTC()

// transferWireAccount and transferWireRegion scope every state key the plugin writes.
const (
	transferWireAccount = "123456789012"
	transferWireRegion  = "us-east-1"
)

// transferBookkeepingMembers are the members Transfer's records declare and no Transfer shape
// publishes. CreatedAt is the server's alone, and is listed for both tests because no Transfer
// response may carry it.
var transferBookkeepingMembers = []string{"AccountID", "Region", "CreatedAt"}

// setupTransferWirePlugin returns the Transfer plugin, a request context and the state manager behind
// it. The state manager is handed back because a record is the only place its bookkeeping members can
// be read from.
func setupTransferWirePlugin(t *testing.T) (*emulator.TransferPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	state := emulator.NewMemoryStateManager()
	p := &emulator.TransferPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": emulator.NewTimeController(transferWireClock)},
	}), "emulator.TransferPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: transferWireAccount,
		Region:    transferWireRegion,
		RequestID: "req-transfer-wire",
		IDs:       emulator.NewIDMint("req-transfer-wire"),
	}, state
}

// transferWire issues one operation and returns the raw response body, failing on anything but 200.
func transferWire(t *testing.T, p *emulator.TransferPlugin, ctx *emulator.RequestContext, op string, body map[string]any) []byte {
	t.Helper()
	resp, err := p.HandleRequest(ctx, transferRequest(t, op, body))
	require.NoError(t, err, "%s", op)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)
	return resp.Body
}

// transferWireRecord returns the record at key as raw JSON, so a member with no published home can be
// read without a Go type deciding which members exist.
func transferWireRecord(t *testing.T, state emulator.StateManager, key string) map[string]json.RawMessage {
	t.Helper()
	data, err := state.Get(t.Context(), "transfer", key)
	require.NoError(t, err, "state.Get %s", key)
	require.NotNil(t, data, "no record stored at %s", key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	return record
}

// transferWireRequireHeld requires that the stored record carries each named member with a non-empty
// value — the presence anchor without which an absence assertion would pass without testing anything.
func transferWireRequireHeld(t *testing.T, state emulator.StateManager, key string, members ...string) {
	t.Helper()
	record := transferWireRecord(t, state, key)
	for _, member := range members {
		require.NotEmptyf(t, record[member], "%s must persist %s before an absence assertion on it means anything", key, member)
		require.NotEqualf(t, `""`, string(record[member]), "%s persists an empty %s", key, member)
	}
}

// transferWireCase is one operation driven by one of the tests below. A non-nil held reuses a response
// the test already has rather than issuing the operation a second time.
type transferWireCase struct {
	op     string
	body   map[string]any
	held   []byte
	anchor string
}

// transferWireRun drives each case as a subtest, in order: the presence anchor first, then the walk.
func transferWireRun(t *testing.T, p *emulator.TransferPlugin, ctx *emulator.RequestContext, cases []transferWireCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.op, func(t *testing.T) {
			body := tc.held
			if body == nil {
				body = transferWire(t, p, ctx, tc.op, tc.body)
			}
			require.Containsf(t, string(body), tc.anchor,
				"presence anchor: %s has to render %s for an absence to mean anything", tc.op, tc.anchor)
			wireAssertNoMemberJSON(t, tc.op, body, transferBookkeepingMembers)
		})
	}
}

// transferWireServer creates a server and returns the body and its id.
func transferWireServer(t *testing.T, p *emulator.TransferPlugin, ctx *emulator.RequestContext) ([]byte, string) {
	t.Helper()
	body := transferWire(t, p, ctx, "CreateServer", map[string]any{"EndpointType": "PUBLIC"})
	var out struct {
		ServerID string `json:"ServerId"`
	}
	require.NoError(t, json.Unmarshal(body, &out), "decode CreateServer: %s", body)
	require.NotEmpty(t, out.ServerID, "CreateServer must report a server id")
	return body, out.ServerID
}

func TestTransferWire_ServerResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupTransferWirePlugin(t)

	created, serverID := transferWireServer(t, p, ctx)
	transferWireRequireHeld(t, state, "server:"+transferWireAccount+"/"+transferWireRegion+"/"+serverID,
		"AccountID", "Region", "CreatedAt")

	server := map[string]any{"ServerId": serverID}
	transferWireRun(t, p, ctx, []transferWireCase{
		{op: "CreateServer", held: created, anchor: `"ServerId":"` + serverID + `"`},
		{op: "DescribeServer", body: server, anchor: `"ServerId":"` + serverID + `"`},
		{op: "UpdateServer", body: map[string]any{"ServerId": serverID, "EndpointType": "PUBLIC"},
			anchor: `"ServerId":"` + serverID + `"`},
		{op: "ListServers", anchor: `"ServerId":"` + serverID + `"`},
		// Last: it removes the record every case above reads.
		{op: "DeleteServer", body: server, anchor: "{}"},
	})
}

func TestTransferWire_UserResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupTransferWirePlugin(t)
	_, serverID := transferWireServer(t, p, ctx)

	const name = "wire-user"
	user := map[string]any{"ServerId": serverID, "UserName": name}
	created := transferWire(t, p, ctx, "CreateUser", map[string]any{
		"ServerId": serverID,
		"UserName": name,
		"Role":     "arn:aws:iam::" + transferWireAccount + ":role/transfer-wire",
	})
	transferWireRequireHeld(t, state,
		"user:"+transferWireAccount+"/"+transferWireRegion+"/"+serverID+"/"+name, "AccountID", "Region")

	described := transferWire(t, p, ctx, "DescribeUser", user)

	transferWireRun(t, p, ctx, []transferWireCase{
		{op: "CreateUser", held: created, anchor: `"UserName":"` + name + `"`},
		{op: "DescribeUser", held: described, anchor: `"UserName":"` + name + `"`},
		{op: "UpdateUser", body: map[string]any{"ServerId": serverID, "UserName": name, "HomeDirectory": "/wire"},
			anchor: `"UserName":"` + name + `"`},
		{op: "ListUsers", body: map[string]any{"ServerId": serverID}, anchor: `"UserName":"` + name + `"`},
		// Last: it removes the record every case above reads.
		{op: "DeleteUser", body: user, anchor: "{}"},
	})

	// API_DescribedUser publishes no ServerId: the response carries the server's ID at the top level,
	// and the record's own copy used to be answered inside `User` beside it.
	var out struct {
		ServerID string                     `json:"ServerId"`
		User     map[string]json.RawMessage `json:"User"`
	}
	require.NoError(t, json.Unmarshal(described, &out), "decode DescribeUser: %s", described)
	require.Equal(t, serverID, out.ServerID, "DescribeUser answers the published top-level ServerId")
	require.NotContains(t, slices.Sorted(maps.Keys(out.User)), "ServerId",
		"API_DescribedUser publishes no ServerId: %s", described)
}
