package emulator_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// UpdateUser applies every member API_UpdateUser publishes, and CreateServer holds Tags to its
// published 1–50 items (#1391). Assertions read DescribeUser's raw JSON, because a typed decode cannot
// tell a member that was dropped from one that was never sent.

// transferDescribedUser returns DescribeUser's User object as raw members.
func transferDescribedUser(a *transferAudit, serverID, name string) map[string]json.RawMessage {
	a.t.Helper()
	var out struct {
		User map[string]json.RawMessage `json:"User"`
	}
	raw := a.ok("DescribeUser", map[string]any{"ServerId": serverID, "UserName": name})
	require.NoError(a.t, json.Unmarshal(raw, &out), "decode DescribeUser: %s", raw)
	return out.User
}

func TestTransferUpdateUser_AppliesEveryPublishedMember(t *testing.T) {
	t.Parallel()
	a := newTransferAudit(t)
	serverID := a.server(nil)
	a.ok("CreateUser", map[string]any{
		"ServerId": serverID, "UserName": "patch-user", "Role": transferAuditRole,
		"HomeDirectory": "/bucket/original", "Policy": `{"Version":"2012-10-17"}`,
	})

	const newRole = "arn:aws:iam::123456789012:role/transfer-updated"
	mappings := []map[string]string{{"Entry": "/", "Target": "/bucket/home/patch-user"}}
	posix := map[string]any{"Uid": 1001, "Gid": 1001, "SecondaryGids": []int{2001}}
	resp := a.ok("UpdateUser", map[string]any{
		"ServerId": serverID, "UserName": "patch-user",
		"Role": newRole, "HomeDirectoryType": "LOGICAL", "HomeDirectoryMappings": mappings, "PosixProfile": posix,
	})
	require.JSONEq(t, `{"ServerId":"`+serverID+`","UserName":"patch-user"}`, string(resp), "UpdateUser answers ServerId and UserName")

	user := transferDescribedUser(a, serverID, "patch-user")
	require.JSONEq(t, `"`+newRole+`"`, string(user["Role"]))
	require.JSONEq(t, `"LOGICAL"`, string(user["HomeDirectoryType"]))
	require.JSONEq(t, `[{"Entry":"/","Target":"/bucket/home/patch-user"}]`, string(user["HomeDirectoryMappings"]))
	require.JSONEq(t, `{"Uid":1001,"Gid":1001,"SecondaryGids":[2001]}`, string(user["PosixProfile"]))
	// Patch semantics: the members this update omitted are as CreateUser left them.
	require.JSONEq(t, `"/bucket/original"`, string(user["HomeDirectory"]))
	require.JSONEq(t, `"{\"Version\":\"2012-10-17\"}"`, string(user["Policy"]))

	// A sent empty HomeDirectory or Policy is a value, inside each member's published minimum of 0,
	// so it clears the stored one; omitempty then leaves the member out.
	a.ok("UpdateUser", map[string]any{"ServerId": serverID, "UserName": "patch-user", "HomeDirectory": "", "Policy": ""})
	user = transferDescribedUser(a, serverID, "patch-user")
	require.NotContains(t, user, "HomeDirectory", "an empty HomeDirectory clears it")
	require.NotContains(t, user, "Policy", "an empty Policy clears it")
	require.JSONEq(t, `"`+newRole+`"`, string(user["Role"]), "a member the update omitted is left alone")

	// HomeDirectory and Policy alone, and a PATH type, replace what they name.
	a.ok("UpdateUser", map[string]any{
		"ServerId": serverID, "UserName": "patch-user",
		"HomeDirectory": "/bucket/next", "HomeDirectoryType": "PATH", "Policy": `{"Statement":[]}`,
	})
	user = transferDescribedUser(a, serverID, "patch-user")
	require.JSONEq(t, `"/bucket/next"`, string(user["HomeDirectory"]))
	require.JSONEq(t, `"PATH"`, string(user["HomeDirectoryType"]))
	require.JSONEq(t, `"{\"Statement\":[]}"`, string(user["Policy"]))
}

func TestTransferUpdateUser_RefusesWhatCreateUserRefuses(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{"no ServerId", map[string]any{"UserName": "check-user"}},
		{"no UserName", map[string]any{}},
		{"UserName outside its pattern", map[string]any{"UserName": "-bad"}},
		{"Role shorter than 20", map[string]any{"Role": "arn:aws:x:role/a"}},
		{"Role empty", map[string]any{"Role": ""}},
		{"Role outside its pattern", map[string]any{"Role": "arn:aws:iam::123456789012:user/not-a-role"}},
		{"Role longer than 2048", map[string]any{"Role": "arn:aws:iam::123456789012:role/" + strings.Repeat("r", 2048)}},
		{"HomeDirectory outside its pattern", map[string]any{"HomeDirectory": "no-leading-slash"}},
		{"HomeDirectory longer than 1024", map[string]any{"HomeDirectory": "/" + strings.Repeat("d", 1024)}},
		{"HomeDirectoryType unpublished", map[string]any{"HomeDirectoryType": "ROOT"}},
		{"HomeDirectoryType empty", map[string]any{"HomeDirectoryType": ""}},
		{"Policy longer than 2048", map[string]any{"Policy": strings.Repeat("p", 2049)}},
		{"HomeDirectoryMappings not an array", map[string]any{"HomeDirectoryMappings": map[string]string{"Entry": "/"}}},
		{"HomeDirectoryMappings empty", map[string]any{"HomeDirectoryMappings": []any{}}},
		{"PosixProfile not an object", map[string]any{"PosixProfile": []int{1}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a := newTransferAudit(t)
			serverID := a.server(nil)
			a.ok("CreateUser", map[string]any{
				"ServerId": serverID, "UserName": "check-user", "Role": transferAuditRole, "HomeDirectory": "/bucket/kept",
			})
			body := map[string]any{"ServerId": serverID, "UserName": "check-user"}
			for k, v := range tc.body {
				body[k] = v
			}
			if tc.name == "no ServerId" {
				delete(body, "ServerId")
			}
			if tc.name == "no UserName" {
				delete(body, "UserName")
			}
			_ = a.refused("UpdateUser", body, "InvalidRequestException", http.StatusBadRequest)

			// A refused update changes nothing.
			user := transferDescribedUser(a, serverID, "check-user")
			require.JSONEq(t, `"`+transferAuditRole+`"`, string(user["Role"]), "%s must leave Role unchanged", tc.name)
			require.JSONEq(t, `"/bucket/kept"`, string(user["HomeDirectory"]), "%s must leave HomeDirectory unchanged", tc.name)
		})
	}
}

func TestTransferUpdateUser_AnAbsentUserIsNotFound(t *testing.T) {
	t.Parallel()
	a := newTransferAudit(t)
	serverID := a.server(nil)
	_ = a.refused("UpdateUser", map[string]any{"ServerId": serverID, "UserName": "nobody", "HomeDirectory": "/x"},
		"ResourceNotFoundException", http.StatusBadRequest)
}

func TestTransferCreateServer_TagsHoldBetweenOneAndFifty(t *testing.T) {
	t.Parallel()
	tags := func(n int) []map[string]string {
		out := make([]map[string]string, n)
		for i := range out {
			out[i] = map[string]string{"Key": "k" + strings.Repeat("x", i), "Value": "v"}
		}
		return out
	}
	a := newTransferAudit(t)
	a.server(nil)
	a.server(map[string]any{"Tags": tags(1)})
	a.server(map[string]any{"Tags": tags(50)})
	_ = a.refused("CreateServer", map[string]any{"Tags": []any{}}, "InvalidRequestException", http.StatusBadRequest)
	_ = a.refused("CreateServer", map[string]any{"Tags": tags(51)}, "InvalidRequestException", http.StatusBadRequest)
}

// A store fault in UpdateUser's read or write is an error, never a published refusal or a 200 over a
// record that was not written.
func TestTransferUpdateUser_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
	}{
		{"user read", func(m *cfFaultStateManager) { m.failGet = "user:" }},
		{"user corrupt", func(m *cfFaultStateManager) { m.corruptGet = "user:" }},
		{"user write", func(m *cfFaultStateManager) { m.failPut = "user:" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			p := &emulator.TransferPlugin{}
			require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
				State: fault, Logger: emulator.NewDefaultLogger(0, false),
				Options: map[string]any{"time_controller": emulator.NewTimeController(ec2WireClock)},
			}))
			a := &transferAudit{t: t, p: p, ctx: &emulator.RequestContext{
				AccountID: "123456789012", Region: "us-east-1", RequestID: "req-transfer-update-fault", IDs: emulator.NewIDMint("req-transfer-update-fault"),
			}}
			serverID := a.server(nil)
			a.user(serverID, "fault-user")

			tc.arm(fault)
			raw, err := json.Marshal(map[string]any{"ServerId": serverID, "UserName": "fault-user", "HomeDirectory": "/x"})
			require.NoError(t, err)
			_, callErr := p.HandleRequest(a.ctx, &emulator.AWSRequest{
				Service: "transfer", Operation: "UpdateUser", Path: "/", Body: raw,
				Headers: map[string]string{"X-Amz-Target": "TransferService.UpdateUser"}, Params: map[string]string{},
			})
			require.Errorf(t, callErr, "UpdateUser must fail on a %s fault", tc.name)
			var awsErr *emulator.AWSError
			require.Falsef(t, errors.As(callErr, &awsErr), "UpdateUser answered a %s fault as the published %v", tc.name, awsErr)
		})
	}
}
