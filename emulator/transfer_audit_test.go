package emulator_test

import (
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Transfer Family's share of the batch audits: pagination (#1195), required members (#1197),
// published codes and enum values (#1198), published members (#1199) and the delete bodies (#1206).
// Every assertion is on the raw response bytes or on the refusal's code *and* status, because a
// typed decode cannot see an extra member and a `!= 200` check cannot see a wrong code.

// transferAuditRole is a Role that satisfies API_CreateUser's arn:.*role/\S+ pattern.
const transferAuditRole = "arn:aws:iam::123456789012:role/transfer-audit"

// transferAudit is a Transfer plugin on a frozen clock with a call that answers either the raw
// body or the refusal.
type transferAudit struct {
	t   *testing.T
	p   *emulator.TransferPlugin
	ctx *emulator.RequestContext
}

func newTransferAudit(t *testing.T) *transferAudit {
	t.Helper()
	a := &transferAudit{t: t, p: &emulator.TransferPlugin{}}
	a.ctx, _ = wireSetup(t, a.p, "req-transfer-audit")
	return a
}

// call issues op and returns the raw body on success or the AWS refusal.
func (a *transferAudit) call(op string, body map[string]any) ([]byte, *emulator.AWSError) {
	a.t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(a.t, err)
	resp, err := a.p.HandleRequest(a.ctx, &emulator.AWSRequest{
		Service: "transfer", Operation: op, Path: "/", Body: raw,
		Headers: map[string]string{"X-Amz-Target": "TransferService." + op}, Params: map[string]string{},
	})
	var awsErr *emulator.AWSError
	if errors.As(err, &awsErr) {
		return nil, awsErr
	}
	require.NoError(a.t, err, "%s", op)
	require.Equal(a.t, http.StatusOK, resp.StatusCode, "%s: %s", op, resp.Body)
	return resp.Body, nil
}

// ok issues op and requires success.
func (a *transferAudit) ok(op string, body map[string]any) []byte {
	a.t.Helper()
	out, awsErr := a.call(op, body)
	require.Nilf(a.t, awsErr, "%s: %v", op, awsErr)
	return out
}

// refused issues op and requires the given code at the given status.
func (a *transferAudit) refused(op string, body map[string]any, code string, status int) *emulator.AWSError {
	a.t.Helper()
	_, awsErr := a.call(op, body)
	require.NotNilf(a.t, awsErr, "%s must be refused %s", op, code)
	require.Equalf(a.t, code, awsErr.Code, "%s: %s", op, awsErr.Message)
	require.Equalf(a.t, status, awsErr.HTTPStatus, "%s %s: %s", op, code, awsErr.Message)
	return awsErr
}

func (a *transferAudit) server(body map[string]any) string {
	a.t.Helper()
	var out struct {
		ServerID string `json:"ServerId"`
	}
	require.NoError(a.t, json.Unmarshal(a.ok("CreateServer", body), &out))
	return out.ServerID
}

func (a *transferAudit) user(serverID, name string) {
	a.t.Helper()
	a.ok("CreateUser", map[string]any{"ServerId": serverID, "UserName": name, "Role": transferAuditRole})
}

// transferKeys decodes a JSON object and returns its member names, sorted.
func transferKeys(t *testing.T, raw []byte) []string {
	t.Helper()
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &doc), "decode: %s", raw)
	return slices.Sorted(maps.Keys(doc))
}

func TestTransferAudit_CollectionsPageAndRefuseAnUnissuedToken(t *testing.T) {
	t.Parallel()
	a := newTransferAudit(t)
	ids := []string{a.server(nil), a.server(nil), a.server(nil)}
	firstServer := ids[0]
	// Both indexes are kept sorted, so a walk answers servers in ID order and users in name order.
	slices.Sort(ids)
	for _, n := range []string{"user-c", "user-a", "user-b"} {
		a.user(firstServer, n)
	}

	for _, tc := range []struct {
		op, member string
		base       map[string]any
		want       []string
		idMember   string
	}{
		{op: "ListServers", member: "Servers", base: map[string]any{}, want: ids, idMember: "ServerId"},
		{op: "ListUsers", member: "Users", base: map[string]any{"ServerId": firstServer}, want: []string{"user-a", "user-b", "user-c"}, idMember: "UserName"},
	} {
		t.Run(tc.op, func(t *testing.T) {
			var seen []string
			token := ""
			pages := 0
			for {
				body := maps.Clone(tc.base)
				body["MaxResults"] = 2
				if token != "" {
					body["NextToken"] = token
				}
				raw := a.ok(tc.op, body)
				pages++
				var doc map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(raw, &doc))
				var items []map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(doc[tc.member], &items))
				for _, it := range items {
					var id string
					require.NoError(t, json.Unmarshal(it[tc.idMember], &id))
					seen = append(seen, id)
				}
				next, present := doc["NextToken"]
				if !present {
					break
				}
				require.NoError(t, json.Unmarshal(next, &token))
				require.NotEmptyf(t, token, "%s must omit NextToken rather than send it empty (min length 1): %s", tc.op, raw)
			}
			require.Equal(t, 2, pages, "%s pages three records at MaxResults 2", tc.op)
			require.Equal(t, tc.want, seen, "%s round-trips its token in index order", tc.op)

			for _, bad := range []int{0, 1001} {
				body := maps.Clone(tc.base)
				body["MaxResults"] = bad
				_ = a.refused(tc.op, body, "InvalidRequestException", http.StatusBadRequest)
			}
			body := maps.Clone(tc.base)
			body["NextToken"] = "not-a-token-substrate-issued"
			awsErr := a.refused(tc.op, body, "InvalidNextTokenException", http.StatusBadRequest)
			require.Equal(t, "The NextToken parameter that was passed is invalid.", awsErr.Message)
		})
	}
}

func TestTransferAudit_RequiredMembersAndPublishedConstraints(t *testing.T) {
	t.Parallel()
	a := newTransferAudit(t)
	serverID := a.server(nil)

	for _, tc := range []struct {
		name, op string
		body     map[string]any
		message  string
	}{
		{"CreateUser without Role", "CreateUser", map[string]any{"ServerId": serverID, "UserName": "no-role"}, "Role is required"},
		{"CreateUser without UserName", "CreateUser", map[string]any{"ServerId": serverID, "Role": transferAuditRole}, "ServerId and UserName are required"},
		{"Role below its minimum length", "CreateUser", map[string]any{"ServerId": serverID, "UserName": "bad-role", "Role": "arn:aws:role/x"}, "Role is shorter"},
		{"Role naming a user", "CreateUser", map[string]any{"ServerId": serverID, "UserName": "bad-role", "Role": "arn:aws:iam::123456789012:user/x"}, "does not match"},
		{"Role not naming a role", "CreateUser", map[string]any{"ServerId": serverID, "UserName": "bad-role", "Role": "arn:aws:iam::123456789012:policy/transfer"}, "does not match"},
		{"UserName starting with a hyphen", "CreateUser", map[string]any{"ServerId": serverID, "UserName": "-alice", "Role": transferAuditRole}, "does not match"},
		{"HomeDirectoryType outside PATH|LOGICAL", "CreateUser", map[string]any{"ServerId": serverID, "UserName": "hdt", "Role": transferAuditRole, "HomeDirectoryType": "FLAT"}, "not one of"},
		{"Domain outside S3|EFS", "CreateServer", map[string]any{"Domain": "SFTP"}, "not one of"},
		{"EndpointType outside its values", "CreateServer", map[string]any{"EndpointType": "PRIVATE"}, "not one of"},
		{"IdentityProviderType outside its values", "CreateServer", map[string]any{"IdentityProviderType": "LDAP"}, "not one of"},
		{"IpAddressType outside its values", "CreateServer", map[string]any{"IpAddressType": "IPV6"}, "not one of"},
		{"Protocols outside its values", "CreateServer", map[string]any{"Protocols": []string{"SCP"}}, "not one of"},
		{"Protocols empty", "CreateServer", map[string]any{"Protocols": []string{}}, "between 1 and 4"},
		{"SecurityPolicyName outside its pattern", "CreateServer", map[string]any{"SecurityPolicyName": "MyPolicy"}, "does not match"},
		{"EndpointDetails not an object", "CreateServer", map[string]any{"EndpointDetails": "vpc-1"}, "must be an object"},
		{"UpdateServer Protocols outside its values", "UpdateServer", map[string]any{"ServerId": serverID, "Protocols": []string{"SCP"}}, "not one of"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			awsErr := a.refused(tc.op, tc.body, "InvalidRequestException", http.StatusBadRequest)
			require.Contains(t, awsErr.Message, tc.message)
		})
	}

	// A request supplying every required member answers what it always did.
	raw := a.ok("CreateUser", map[string]any{"ServerId": serverID, "UserName": "complete", "Role": transferAuditRole})
	require.JSONEq(t, `{"ServerId":"`+serverID+`","UserName":"complete"}`, string(raw))
}

func TestTransferAudit_RefusalsAnswerThePublishedCodeAndStatus(t *testing.T) {
	t.Parallel()
	a := newTransferAudit(t)
	serverID := a.server(nil)
	a.user(serverID, "present")
	const absent = "s-0000000000000000a"

	for _, tc := range []struct {
		op   string
		body map[string]any
	}{
		{"DescribeServer", map[string]any{"ServerId": absent}},
		{"UpdateServer", map[string]any{"ServerId": absent, "EndpointType": "VPC"}},
		{"DeleteServer", map[string]any{"ServerId": absent}},
		{"ListUsers", map[string]any{"ServerId": absent}},
		{"CreateUser", map[string]any{"ServerId": absent, "UserName": "nobody", "Role": transferAuditRole}},
		{"DescribeUser", map[string]any{"ServerId": serverID, "UserName": "absent"}},
		{"UpdateUser", map[string]any{"ServerId": serverID, "UserName": "absent", "HomeDirectory": "/x"}},
		{"DeleteUser", map[string]any{"ServerId": serverID, "UserName": "absent"}},
	} {
		t.Run(tc.op+"/notFound", func(t *testing.T) {
			_ = a.refused(tc.op, tc.body, "ResourceNotFoundException", http.StatusBadRequest)
		})
	}
	t.Run("CreateUser/duplicate", func(t *testing.T) {
		_ = a.refused("CreateUser", map[string]any{"ServerId": serverID, "UserName": "present", "Role": transferAuditRole},
			"ResourceExistsException", http.StatusBadRequest)
	})

	// Domain defaults to S3, the page's stated default, where it used to default to SFTP; EndpointType
	// defaults to PUBLIC, which was already right.
	raw := a.ok("DescribeServer", map[string]any{"ServerId": serverID})
	var out struct {
		Server map[string]json.RawMessage `json:"Server"`
	}
	require.NoError(t, json.Unmarshal(raw, &out))
	require.JSONEq(t, `"S3"`, string(out.Server["Domain"]), "%s", raw)
	require.JSONEq(t, `"PUBLIC"`, string(out.Server["EndpointType"]), "%s", raw)
	require.JSONEq(t, `"SERVICE_MANAGED"`, string(out.Server["IdentityProviderType"]), "%s", raw)
	require.JSONEq(t, `"IPV4"`, string(out.Server["IpAddressType"]), "%s", raw)
}

func TestTransferAudit_RecordsCarryTheirPublishedMembers(t *testing.T) {
	t.Parallel()
	a := newTransferAudit(t)
	serverID := a.server(map[string]any{
		"Domain": "EFS", "EndpointType": "VPC", "IdentityProviderType": "AWS_LAMBDA",
		"Protocols": []string{"SFTP", "FTPS"}, "Certificate": "arn:aws:acm:us-east-1:123456789012:certificate/abc",
		"LoggingRole": "arn:aws:iam::123456789012:role/transfer-logs", "SecurityPolicyName": "TransferSecurityPolicy-2024-01",
		"IpAddressType": "DUALSTACK", "PreAuthenticationLoginBanner": "hello", "PostAuthenticationLoginBanner": "welcome",
		"StructuredLogDestinations": []string{"arn:aws:logs:us-east-1:123456789012:log-group:transfer:*"},
		"EndpointDetails":           map[string]any{"VpcId": "vpc-0123456789abcdef0"},
		"IdentityProviderDetails":   map[string]any{"Function": "arn:aws:lambda:us-east-1:123456789012:function:idp"},
		"ProtocolDetails":           map[string]any{"PassiveIp": "10.0.0.1"},
		"S3StorageOptions":          map[string]any{"DirectoryListingOptimization": "ENABLED"},
		"WorkflowDetails":           map[string]any{"OnUpload": []any{}},
		"Tags":                      []map[string]string{{"Key": "team", "Value": "audit"}},
	})

	describe := func() map[string]json.RawMessage {
		t.Helper()
		var out struct {
			Server map[string]json.RawMessage `json:"Server"`
		}
		require.NoError(t, json.Unmarshal(a.ok("DescribeServer", map[string]any{"ServerId": serverID}), &out))
		return out.Server
	}
	server := describe()
	require.Equal(t, []string{
		"Arn", "Certificate", "Domain", "EndpointDetails", "EndpointType", "IdentityProviderDetails",
		"IdentityProviderType", "IpAddressType", "LoggingRole", "PostAuthenticationLoginBanner",
		"PreAuthenticationLoginBanner", "ProtocolDetails", "Protocols", "S3StorageOptions", "SecurityPolicyName",
		"ServerId", "State", "StructuredLogDestinations", "Tags", "UserCount", "WorkflowDetails",
	}, slices.Sorted(maps.Keys(server)), "DescribeServer answers every modeled API_DescribedServer member")
	require.JSONEq(t, `{"VpcId":"vpc-0123456789abcdef0"}`, string(server["EndpointDetails"]))
	require.JSONEq(t, "0", string(server["UserCount"]))

	// UserCount counts the server's users, where it was declared and never assigned.
	a.user(serverID, "count-a")
	a.user(serverID, "count-b")
	require.JSONEq(t, "2", string(describe()["UserCount"]))
	a.ok("DeleteUser", map[string]any{"ServerId": serverID, "UserName": "count-a"})
	require.JSONEq(t, "1", string(describe()["UserCount"]))

	// UpdateServer applies what it is sent, leaves what it is not, and no longer reads Tags.
	a.ok("UpdateServer", map[string]any{
		"ServerId": serverID, "Protocols": []string{"SFTP"}, "LoggingRole": "arn:aws:iam::123456789012:role/other-logs",
		"StructuredLogDestinations": []string{}, "Tags": []map[string]string{{"Key": "team", "Value": "changed"}},
	})
	server = describe()
	require.JSONEq(t, `["SFTP"]`, string(server["Protocols"]))
	require.JSONEq(t, `"arn:aws:iam::123456789012:role/other-logs"`, string(server["LoggingRole"]))
	require.JSONEq(t, `"arn:aws:acm:us-east-1:123456789012:certificate/abc"`, string(server["Certificate"]), "an omitted member is left alone")
	require.NotContains(t, server, "StructuredLogDestinations", "an empty array clears the destination")
	require.JSONEq(t, `[{"Key":"team","Value":"audit"}]`, string(server["Tags"]), "UpdateServer publishes no Tags member")

	// ListedServer and ListedUser carry exactly their published members.
	var servers struct {
		Servers []json.RawMessage `json:"Servers"`
	}
	require.NoError(t, json.Unmarshal(a.ok("ListServers", map[string]any{}), &servers))
	require.Len(t, servers.Servers, 1)
	require.Equal(t, []string{"Arn", "Domain", "EndpointType", "IdentityProviderType", "LoggingRole", "ServerId", "State", "UserCount"},
		transferKeys(t, servers.Servers[0]))

	a.ok("CreateUser", map[string]any{
		"ServerId": serverID, "UserName": "keyed", "Role": transferAuditRole, "HomeDirectory": "/efs/keyed",
		"HomeDirectoryType": "PATH", "Policy": `{"Version":"2012-10-17"}`, "PosixProfile": map[string]any{"Uid": 1000, "Gid": 1000},
		"SshPublicKeyBody": "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIEXAMPLEKEY user@host",
	})
	var users struct {
		Users []json.RawMessage `json:"Users"`
	}
	require.NoError(t, json.Unmarshal(a.ok("ListUsers", map[string]any{"ServerId": serverID}), &users))
	listed := []string{"Arn", "HomeDirectory", "HomeDirectoryType", "Role", "SshPublicKeyCount", "UserName"}
	for _, u := range users.Users {
		require.Subset(t, listed, transferKeys(t, u), "a ListedUser carries no member API_ListedUser does not publish")
	}
	keyedRaw := users.Users[len(users.Users)-1]
	require.Equal(t, listed, transferKeys(t, keyedRaw), "a user with every member set lists all six")
	var keyed map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(keyedRaw, &keyed))
	require.JSONEq(t, "1", string(keyed["SshPublicKeyCount"]))
	require.JSONEq(t, `"PATH"`, string(keyed["HomeDirectoryType"]))

	var described struct {
		User map[string]json.RawMessage `json:"User"`
	}
	require.NoError(t, json.Unmarshal(a.ok("DescribeUser", map[string]any{"ServerId": serverID, "UserName": "keyed"}), &described))
	require.Equal(t, []string{"Arn", "HomeDirectory", "HomeDirectoryType", "Policy", "PosixProfile", "Role", "SshPublicKeys", "UserName"},
		slices.Sorted(maps.Keys(described.User)))
	var keys []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(described.User["SshPublicKeys"], &keys))
	require.Len(t, keys, 1)
	require.Regexp(t, `^"key-[0-9a-f]{17}"$`, string(keys[0]["SshPublicKeyId"]))
	require.Equal(t, "1700000000.000", string(keys[0]["DateImported"]), "DateImported is epoch seconds under awsJson1_1")
}

func TestTransferAudit_DeletesAnswerAnEmptyObject(t *testing.T) {
	t.Parallel()
	a := newTransferAudit(t)
	serverID := a.server(nil)
	a.user(serverID, "doomed")
	require.Equal(t, "{}", string(a.ok("DeleteUser", map[string]any{"ServerId": serverID, "UserName": "doomed"})))
	require.Equal(t, "{}", string(a.ok("DeleteServer", map[string]any{"ServerId": serverID})))
	_ = a.refused("DeleteServer", map[string]any{"ServerId": serverID}, "ResourceNotFoundException", http.StatusBadRequest)
}

// A store fault in a path this audit added is returned as an error, never answered as a
// published refusal or as a success.
func TestTransferAudit_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
		op   string
		body func(serverID string) map[string]any
	}{
		{"DescribeServer user count", func(m *cfFaultStateManager) { m.failGet = "user_names:" }, "DescribeServer",
			func(id string) map[string]any { return map[string]any{"ServerId": id} }},
		{"ListServers server read", func(m *cfFaultStateManager) { m.failGet = "server:" }, "ListServers",
			func(string) map[string]any { return map[string]any{} }},
		{"ListServers user count", func(m *cfFaultStateManager) { m.failGet = "user_names:" }, "ListServers",
			func(string) map[string]any { return map[string]any{} }},
		{"ListUsers user read", func(m *cfFaultStateManager) { m.failGet = "user:" }, "ListUsers",
			func(id string) map[string]any { return map[string]any{"ServerId": id} }},
		{"DeleteServer user index read", func(m *cfFaultStateManager) { m.failGet = "user_names:" }, "DeleteServer",
			func(id string) map[string]any { return map[string]any{"ServerId": id} }},
		{"DeleteServer user delete", func(m *cfFaultStateManager) { m.failDelete = "user:" }, "DeleteServer",
			func(id string) map[string]any { return map[string]any{"ServerId": id} }},
		{"DeleteServer user index delete", func(m *cfFaultStateManager) { m.failDelete = "user_names:" }, "DeleteServer",
			func(id string) map[string]any { return map[string]any{"ServerId": id} }},
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
				AccountID: "123456789012", Region: "us-east-1", RequestID: "req-transfer-fault", IDs: emulator.NewIDMint("req-transfer-fault"),
			}}
			serverID := a.server(nil)
			a.user(serverID, "fault-user")

			tc.arm(fault)
			raw, err := json.Marshal(tc.body(serverID))
			require.NoError(t, err)
			_, callErr := p.HandleRequest(a.ctx, &emulator.AWSRequest{
				Service: "transfer", Operation: tc.op, Path: "/", Body: raw,
				Headers: map[string]string{"X-Amz-Target": "TransferService." + tc.op}, Params: map[string]string{},
			})
			require.Errorf(t, callErr, "%s must fail on a store fault", tc.name)
			var awsErr *emulator.AWSError
			require.Falsef(t, errors.As(callErr, &awsErr), "%s answered a store fault as the published %v", tc.name, awsErr)
		})
	}
}
