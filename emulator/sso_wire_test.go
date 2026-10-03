package emulator_test

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The raw-bytes assertions scripts/wire-bookkeeping-projected.txt cites for IAM Identity Center's three
// records (#756), and the published form of their dates (#1345).
//
// SSOInstance, SSOPermissionSet and SSOAccountAssignment each declare the owning account as
// `json:"AccountID"`, never under omitempty, so a record that exists holds it and no absence below is
// vacuous. CreatePermissionSet and DescribePermissionSet used to answer the permission set whole; they
// answer emulator/sso_wire.go's projection now. ListInstances and the assignment operations have
// always built their maps member by member.
//
// # Why this walk compares names exactly, not folded
//
// Every other wire test folds case, because a leak could arrive in any capitalization. Here a fold is
// wrong: CreateAccountAssignment and ListAccountAssignments publish `AccountId`, the account an
// assignment targets, which is the service's own data. The record's bookkeeping member is
// `AccountID`, the account that owns the assignment. They differ only in case, so a folded walk would
// fail on a published member. The comparison is therefore exact, against the one spelling the records
// use.

// ssoWireClock is the instant every fixture below runs at, on a frozen clock.
var ssoWireClock = time.Unix(1700000000, 0).UTC()

// ssoWireAccount scopes every state key the plugin writes. IAM Identity Center keys its records by
// account alone.
const ssoWireAccount = "123456789012"

// ssoWireAssertNoAccountID fails if any member of the JSON document is named exactly AccountID, at any
// depth, reporting the path.
func ssoWireAssertNoAccountID(t *testing.T, op string, body []byte) {
	t.Helper()
	var doc any
	require.NoErrorf(t, json.Unmarshal(body, &doc), "%s answered undecodable JSON: %s", op, body)
	var walk func(path string, node any)
	walk = func(path string, node any) {
		switch typed := node.(type) {
		case map[string]any:
			for key, value := range typed {
				require.NotEqualf(t, "AccountID", key,
					"%s answered %s.%s: AccountID is substrate's bookkeeping and no IAM Identity Center shape publishes it", op, path, key)
				walk(path+"."+key, value)
			}
		case []any:
			for i, value := range typed {
				walk(fmt.Sprintf("%s[%d]", path, i), value)
			}
		}
	}
	walk("$", doc)
}

// setupSSOWirePlugin returns the IAM Identity Center plugin, a request context and the state manager
// behind it. Freeze then SetTime, in the order TimeController.Freeze documents.
func setupSSOWirePlugin(t *testing.T) (*emulator.SSOPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	tc := emulator.NewTimeController(ssoWireClock)
	tc.Freeze()
	tc.SetTime(ssoWireClock)
	state := emulator.NewMemoryStateManager()
	p := &emulator.SSOPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}), "emulator.SSOPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: ssoWireAccount,
		Region:    "us-east-1",
		RequestID: "req-sso-wire",
		IDs:       emulator.NewIDMint("req-sso-wire"),
	}, state
}

// ssoWire issues one operation and returns the raw response body, failing on anything but 200.
func ssoWire(t *testing.T, p *emulator.SSOPlugin, ctx *emulator.RequestContext, op string, body map[string]any) []byte {
	t.Helper()
	if body == nil {
		body = map[string]any{}
	}
	resp, err := p.HandleRequest(ctx, ssoRequest(t, op, body))
	require.NoError(t, err, "%s", op)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)
	return resp.Body
}

// ssoWireRequireAccount requires that the stored record at key carries the owning account.
func ssoWireRequireAccount(t *testing.T, state emulator.StateManager, key string) {
	t.Helper()
	data, err := state.Get(t.Context(), "sso", key)
	require.NoError(t, err, "state.Get %s", key)
	require.NotNil(t, data, "no record stored at %s", key)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
	require.JSONEq(t, `"`+ssoWireAccount+`"`, string(record["AccountID"]), "%s must persist AccountID", key)
}

func TestSSOWire_ResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupSSOWirePlugin(t)

	instances := ssoWire(t, p, ctx, "ListInstances", nil)
	var inst struct {
		Instances []struct {
			InstanceArn string `json:"InstanceArn"`
		} `json:"Instances"`
	}
	require.NoError(t, json.Unmarshal(instances, &inst), "decode ListInstances: %s", instances)
	require.Len(t, inst.Instances, 1, "ListInstances: %s", instances)
	instanceArn := inst.Instances[0].InstanceArn
	ssoWireRequireAccount(t, state, "instance:"+ssoWireAccount)

	created := ssoWire(t, p, ctx, "CreatePermissionSet", map[string]any{"InstanceArn": instanceArn, "Name": "wire-ps", "SessionDuration": "PT1H"})
	var ps struct {
		PermissionSet struct {
			PermissionSetArn string `json:"PermissionSetArn"`
		} `json:"PermissionSet"`
	}
	require.NoError(t, json.Unmarshal(created, &ps), "decode CreatePermissionSet: %s", created)
	psArn := ps.PermissionSet.PermissionSetArn
	require.NotEmpty(t, psArn, "CreatePermissionSet must report an ARN")
	ssoWireRequireAccount(t, state, "permset:"+ssoWireAccount+"/"+psArn)

	described := ssoWire(t, p, ctx, "DescribePermissionSet", map[string]any{"InstanceArn": instanceArn, "PermissionSetArn": psArn})

	assignment := map[string]any{
		"InstanceArn": instanceArn, "PermissionSetArn": psArn,
		"TargetId": "210987654321", "TargetType": "AWS_ACCOUNT",
		"PrincipalType": "USER", "PrincipalId": "wire-user",
	}
	assigned := ssoWire(t, p, ctx, "CreateAccountAssignment", assignment)
	ssoWireRequireAccount(t, state, "assignment:"+ssoWireAccount+"/"+psArn+"/210987654321/USER/wire-user")

	policy := map[string]any{"InstanceArn": instanceArn, "PermissionSetArn": psArn, "ManagedPolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess"}
	for _, tc := range []struct {
		op     string
		body   map[string]any
		held   []byte
		anchor string
	}{
		{op: "ListInstances", held: instances, anchor: `"InstanceArn":"` + instanceArn + `"`},
		{op: "CreatePermissionSet", held: created, anchor: `"PermissionSetArn":"` + psArn + `"`},
		{op: "DescribePermissionSet", held: described, anchor: `"Name":"wire-ps"`},
		{op: "UpdatePermissionSet", body: map[string]any{"InstanceArn": instanceArn, "PermissionSetArn": psArn, "Description": "wire"}, anchor: "{}"},
		{op: "ListPermissionSets", body: map[string]any{"InstanceArn": instanceArn}, anchor: psArn},
		{op: "AttachManagedPolicyToPermissionSet", body: policy, anchor: "{}"},
		{op: "ListManagedPoliciesInPermissionSet", body: map[string]any{"InstanceArn": instanceArn, "PermissionSetArn": psArn}, anchor: "ReadOnlyAccess"},
		{op: "DetachManagedPolicyFromPermissionSet", body: policy, anchor: "{}"},
		{op: "CreateAccountAssignment", held: assigned, anchor: `"TargetId":"210987654321"`},
		// Publishes AccountId, the target — the reason the walk compares names exactly.
		{op: "ListAccountAssignments", body: map[string]any{"InstanceArn": instanceArn, "PermissionSetArn": psArn, "AccountId": "210987654321"}, anchor: `"AccountId":"210987654321"`},
		{op: "DeleteAccountAssignment", body: assignment, anchor: `"AccountAssignmentDeletionStatus"`},
		// Last: it removes the record the cases above read.
		{op: "DeletePermissionSet", body: map[string]any{"InstanceArn": instanceArn, "PermissionSetArn": psArn}, anchor: "{}"},
	} {
		t.Run(tc.op, func(t *testing.T) {
			body := tc.held
			if body == nil {
				body = ssoWire(t, p, ctx, tc.op, tc.body)
			}
			require.Containsf(t, string(body), tc.anchor,
				"presence anchor: %s has to render %s for an absence to mean anything", tc.op, tc.anchor)
			ssoWireAssertNoAccountID(t, tc.op, body)
		})
	}

	// After the walk. API_PermissionSet publishes no InstanceArn — the request names the instance — and
	// its CreatedDate, like ListInstances', is a number under awsJson1_1 (#1345). The clock is frozen.
	for op, body := range map[string][]byte{"CreatePermissionSet": created, "DescribePermissionSet": described} {
		var out struct {
			PermissionSet map[string]json.RawMessage `json:"PermissionSet"`
		}
		require.NoError(t, json.Unmarshal(body, &out), "decode %s: %s", op, body)
		require.NotContainsf(t, slices.Sorted(maps.Keys(out.PermissionSet)), "InstanceArn", "%s: API_PermissionSet publishes no InstanceArn: %s", op, body)
		require.JSONEqf(t, "1700000000.000", string(out.PermissionSet["CreatedDate"]), "%s must answer CreatedDate as epoch seconds: %s", op, body)
	}
	require.Contains(t, string(instances), `"CreatedDate":1700000000.000`, "ListInstances must answer CreatedDate as epoch seconds: %s", instances)
}
