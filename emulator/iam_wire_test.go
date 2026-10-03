package emulator_test

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The raw-bytes assertions scripts/wire-bookkeeping-projected.txt cites for IAM's three records (#756).
//
// IAMRole and IAMUser declare EverTagged as `ever_tagged,omitempty`, and IAMAccessKey declares its
// owning account as `AccountId,omitempty` because an access key's ID is what determines the account,
// so the account cannot be in its state key (#737). None of the three reaches a body. IAM speaks the
// query protocol, every response is marshaled from an XML struct declared for its operation, and no
// XML-tagged field is typed as one of these records. So IAM was already projected in code, and what
// it lacked was this file.
//
// Every member under test is `,omitempty`, so each would be absent from a body for free if the record
// did not hold it. Each test therefore proves the record holds it first. The role and the user are
// tagged, because ever_tagged is set only by a tag write (#938, #1304), and the access key's account
// is read back because CreateAccessKey writes it on every key.
//
// "Every response that answers the record" includes the ones that render it nested: GetGroup lists a
// group's users, the instance-profile operations list a profile's roles, and
// GetAccountAuthorizationDetails lists both. They are driven here.

// iamBookkeepingMembers are the members IAM's records declare and no IAM shape publishes as an element
// of these responses. `ever_tagged` is listed in its own right because a fold does not reach a
// snake_case spelling.
//
// AccountId is listed as the record spells it. No IAM shape answered here publishes an element of
// that name: the account appears only inside ARNs.
var iamBookkeepingMembers = []string{"EverTagged", "ever_tagged", "AccountId"}

// iamWireRecord returns the single record in the IAM namespace whose key starts with prefix and ends
// with suffix, as raw JSON. Looking the key up rather than building it keeps the test independent of
// the account the test server resolves a request to.
func iamWireRecord(t *testing.T, state emulator.StateManager, prefix, suffix string) map[string]json.RawMessage {
	t.Helper()
	keys, err := state.List(t.Context(), "iam", prefix)
	require.NoError(t, err, "state.List %s", prefix)
	for _, key := range keys {
		if !strings.HasSuffix(key, suffix) {
			continue
		}
		data, getErr := state.Get(t.Context(), "iam", key)
		require.NoError(t, getErr, "state.Get %s", key)
		var record map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(data, &record), "decode %s: %s", key, data)
		return record
	}
	require.Failf(t, "no record", "no %s…%s record among %v", prefix, suffix, keys)
	return nil
}

// iamWireCase is one operation; a non-empty held reuses a response the test already has.
type iamWireCase struct {
	op     string
	params map[string]string
	held   string
	anchor string
}

// iamWireRun drives each case as a subtest: the presence anchor first, then the walk.
func iamWireRun(t *testing.T, srv *emulator.Server, cases []iamWireCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.op, func(t *testing.T) {
			body := tc.held
			if body == "" {
				body = iamFormRaw(t, srv, tc.op, tc.params)
			}
			require.Containsf(t, body, tc.anchor,
				"presence anchor: %s has to render %s for an absence to mean anything", tc.op, tc.anchor)
			wireAssertNoMemberXML(t, tc.op, []byte(body), iamBookkeepingMembers, "")
		})
	}
}

// iamWireTag is the tag every tag write below adds.
var iamWireTag = map[string]string{"Tags.member.1.Key": "team", "Tags.member.1.Value": "wire"}

// iamWireWith returns base with the tag write's parameters added.
func iamWireWith(base map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range base {
		out[k] = v
	}
	for k, v := range iamWireTag {
		out[k] = v
	}
	return out
}

func TestIAMWire_RoleResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	srv := newIAMTestServerWithState(t, state)

	const name = "wire-role"
	trust := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	created := iamFormRaw(t, srv, "CreateRole", map[string]string{"RoleName": name, "AssumeRolePolicyDocument": trust})
	role := map[string]string{"RoleName": name}

	// ever_tagged is `,omitempty` and set only by a tag write (#938): tag, then read it back, before
	// any response is walked (#1304).
	tagged := iamFormRaw(t, srv, "TagRole", iamWireWith(role))
	require.JSONEq(t, "true", string(iamWireRecord(t, state, "role:", "/"+name)["ever_tagged"]),
		"the role must persist ever_tagged before an absence assertion on it means anything")

	iamFormRaw(t, srv, "CreateInstanceProfile", map[string]string{"InstanceProfileName": "wire-profile"})
	added := iamFormRaw(t, srv, "AddRoleToInstanceProfile", map[string]string{"InstanceProfileName": "wire-profile", "RoleName": name})

	iamWireRun(t, srv, []iamWireCase{
		{op: "CreateRole", held: created, anchor: "<RoleName>" + name + "</RoleName>"},
		{op: "TagRole", held: tagged, anchor: "<RequestId>"},
		{op: "GetRole", params: role, anchor: "<RoleName>" + name + "</RoleName>"},
		{op: "ListRoles", anchor: "<RoleName>" + name + "</RoleName>"},
		{op: "ListRoleTags", params: role, anchor: "<Key>team</Key>"},
		{op: "UpdateAssumeRolePolicy", params: map[string]string{"RoleName": name, "PolicyDocument": trust}, anchor: "<RequestId>"},
		{op: "AddRoleToInstanceProfile", held: added, anchor: "<RequestId>"},
		// The two instance-profile reads render the role nested inside the profile.
		{op: "GetInstanceProfile", params: map[string]string{"InstanceProfileName": "wire-profile"}, anchor: "<RoleName>" + name + "</RoleName>"},
		{op: "ListInstanceProfiles", anchor: "<RoleName>" + name + "</RoleName>"},
		{op: "GetAccountAuthorizationDetails", anchor: "<RoleName>" + name + "</RoleName>"},
		{op: "RemoveRoleFromInstanceProfile", params: map[string]string{"InstanceProfileName": "wire-profile", "RoleName": name}, anchor: "<RequestId>"},
		{op: "UntagRole", params: map[string]string{"RoleName": name, "TagKeys.member.1": "team"}, anchor: "<RequestId>"},
		// Last: it removes the record every case above reads.
		{op: "DeleteRole", params: role, anchor: "<RequestId>"},
	})
}

func TestIAMWire_ServiceLinkedRoleResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	// A service-linked role is an IAMRole too, minted by its own operation, so its response is one
	// more body that answers the record.
	created := iamFormRaw(t, srv, "CreateServiceLinkedRole", map[string]string{"AWSServiceName": "elasticloadbalancing.amazonaws.com"})
	iamWireRun(t, srv, []iamWireCase{
		{op: "CreateServiceLinkedRole", held: created, anchor: "<RoleName>"},
	})
}

func TestIAMWire_UserResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	srv := newIAMTestServerWithState(t, state)

	const name = "wire-user"
	created := iamFormRaw(t, srv, "CreateUser", map[string]string{"UserName": name})
	user := map[string]string{"UserName": name}

	tagged := iamFormRaw(t, srv, "TagUser", iamWireWith(user))
	require.JSONEq(t, "true", string(iamWireRecord(t, state, "user:", "/"+name)["ever_tagged"]),
		"the user must persist ever_tagged before an absence assertion on it means anything")

	iamFormRaw(t, srv, "CreateGroup", map[string]string{"GroupName": "wire-group"})
	added := iamFormRaw(t, srv, "AddUserToGroup", map[string]string{"GroupName": "wire-group", "UserName": name})

	iamWireRun(t, srv, []iamWireCase{
		{op: "CreateUser", held: created, anchor: "<UserName>" + name + "</UserName>"},
		{op: "TagUser", held: tagged, anchor: "<RequestId>"},
		{op: "GetUser", params: user, anchor: "<UserName>" + name + "</UserName>"},
		{op: "ListUsers", anchor: "<UserName>" + name + "</UserName>"},
		{op: "ListUserTags", params: user, anchor: "<Key>team</Key>"},
		{op: "AddUserToGroup", held: added, anchor: "<RequestId>"},
		// GetGroup renders the group's users nested in its response.
		{op: "GetGroup", params: map[string]string{"GroupName": "wire-group"}, anchor: "<UserName>" + name + "</UserName>"},
		{op: "ListGroupsForUser", params: user, anchor: "<GroupName>wire-group</GroupName>"},
		{op: "GetAccountAuthorizationDetails", anchor: "<UserName>" + name + "</UserName>"},
		{op: "RemoveUserFromGroup", params: map[string]string{"GroupName": "wire-group", "UserName": name}, anchor: "<RequestId>"},
		{op: "UntagUser", params: map[string]string{"UserName": name, "TagKeys.member.1": "team"}, anchor: "<RequestId>"},
		// Last: it removes the record every case above reads.
		{op: "DeleteUser", params: user, anchor: "<RequestId>"},
	})
}

func TestIAMWire_AccessKeyResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	srv := newIAMTestServerWithState(t, state)

	const name = "wire-key-user"
	iamFormRaw(t, srv, "CreateUser", map[string]string{"UserName": name})
	created := iamFormRaw(t, srv, "CreateAccessKey", map[string]string{"UserName": name})
	m := regexp.MustCompile(`<AccessKeyId>([^<]+)</AccessKeyId>`).FindStringSubmatch(created)
	require.NotNil(t, m, "CreateAccessKey must report an access key id: %s", created)
	keyID := m[1]

	// AccountId is `,omitempty` on the key record. CreateAccessKey writes it on every key, and it is
	// read back to prove it rather than assume it.
	data, err := state.Get(t.Context(), "iam", "accesskey:"+keyID)
	require.NoError(t, err, "state.Get accesskey:%s", keyID)
	var record map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &record), "decode access key: %s", data)
	require.NotEmpty(t, record["AccountId"], "the access key must persist AccountId before an absence assertion on it means anything")
	require.NotEqual(t, `""`, string(record["AccountId"]), "the access key persists an empty AccountId")

	iamWireRun(t, srv, []iamWireCase{
		{op: "CreateAccessKey", held: created, anchor: "<AccessKeyId>" + keyID + "</AccessKeyId>"},
		{op: "ListAccessKeys", params: map[string]string{"UserName": name}, anchor: "<AccessKeyId>" + keyID + "</AccessKeyId>"},
		// Last: it removes the record every case above reads.
		{op: "DeleteAccessKey", params: map[string]string{"UserName": name, "AccessKeyId": keyID}, anchor: "<RequestId>"},
	})
}
