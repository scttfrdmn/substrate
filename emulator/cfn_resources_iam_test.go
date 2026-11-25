package emulator_test

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// CloudFormation's IAM types in v0.129.0: AWS::IAM::Policy as inline policies (#1562),
// AWS::IAM::ManagedPolicy (#1560), AWS::IAM::User and AWS::IAM::Group, and AWS::IAM::Role's
// ManagedPolicyArns, Policies and in-place update (#1545).

// cfnIAMDo sends one IAM operation through the deployer's registry and returns the response body,
// or the refusal.
func cfnIAMDo(t *testing.T, d *emulator.StackDeployer, op string, body map[string]string) ([]byte, error) {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)
	resp, err := d.DispatchForTest(context.Background(), &emulator.AWSRequest{
		Service: "iam", Operation: op, Body: data, Headers: map[string]string{}, Params: map[string]string{},
	}, "probe")
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// cfnIAMOK is cfnIAMDo for a call that must succeed.
func cfnIAMOK(t *testing.T, d *emulator.StackDeployer, op string, body map[string]string) []byte {
	t.Helper()
	out, err := cfnIAMDo(t, d, op, body)
	require.NoError(t, err, "%s %v", op, body)
	return out
}

// cfnIAMMembers decodes the member values of one list element of an IAM response — the
// ARNs of ListAttached*Policies, the names of List*Policies, and so on.
func cfnIAMMembers(t *testing.T, body []byte, path string) []string {
	t.Helper()
	dec := xml.NewDecoder(strings.NewReader(string(body)))
	var values []string
	var stack []string
	for {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		switch tt := tok.(type) {
		case xml.StartElement:
			stack = append(stack, tt.Name.Local)
		case xml.EndElement:
			stack = stack[:len(stack)-1]
		case xml.CharData:
			if strings.HasSuffix(strings.Join(stack, ">"), path) {
				if s := strings.TrimSpace(string(tt)); s != "" {
					values = append(values, s)
				}
			}
		}
	}
	return values
}

// cfnIAMTrust is an empty trust policy, which is all these roles need.
const cfnIAMTrust = `{"Version":"2012-10-17","Statement":[]}`

// cfnIAMDoc is a one-statement policy document allowing action.
func cfnIAMDoc(action string) string {
	return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"` + action + `","Resource":"*"}]}`
}

func TestCFN_IAMPolicyIsAnInlinePolicyOnEachPrincipal(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	tmpl := `{"Resources":{
		"R":{"Type":"AWS::IAM::Role","Properties":{"RoleName":"inline-role","AssumeRolePolicyDocument":` + cfnIAMTrust + `}},
		"U":{"Type":"AWS::IAM::User","Properties":{"UserName":"inline-user"}},
		"G":{"Type":"AWS::IAM::Group","Properties":{"GroupName":"inline-group"}},
		"P":{"Type":"AWS::IAM::Policy","Properties":{"PolicyName":"shared",
			"Roles":[{"Ref":"R"}],"Users":[{"Ref":"U"}],"Groups":[{"Ref":"G"}],
			"PolicyDocument":` + cfnIAMDoc("s3:GetObject") + `}}},
		"Outputs":{"Ref":{"Value":{"Ref":"P"}},"Id":{"Value":{"Fn::GetAtt":["P","Id"]}}}}`
	result, err := d.Deploy(ctx, tmpl, "inline", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, result)

	for _, probe := range []struct{ op, member, name string }{
		{"GetRolePolicy", "RoleName", "inline-role"},
		{"GetUserPolicy", "UserName", "inline-user"},
		{"GetGroupPolicy", "GroupName", "inline-group"},
	} {
		cfnIAMOK(t, d, probe.op, map[string]string{probe.member: probe.name, "PolicyName": "shared"})
	}
	local := cfnIAMOK(t, d, "ListPolicies", map[string]string{"Scope": "Local"})
	assert.NotContains(t, string(local), "shared", "no customer managed policy is created any more")

	policy := resourceByLogicalID(t, result, "P")
	assert.Empty(t, policy.ARN, "an inline policy has no ARN")
	assert.NotEqual(t, "shared", policy.PhysicalID, "the physical ID is generated, not the policy name")
	assert.Equal(t, policy.PhysicalID, result.Outputs["Ref"])
	assert.Equal(t, policy.PhysicalID, result.Outputs["Id"])

	require.NoError(t, d.DeleteStack(ctx, "inline"),
		"the inline policies are removed first, so every principal deletes")
	_, err = cfnIAMDo(t, d, "GetRole", map[string]string{"RoleName": "inline-role"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NoSuchEntity")
}

func TestCFN_IAMPolicyRefusals(t *testing.T) {
	tests := []struct {
		name   string
		props  string
		reason string
	}{
		{"no principal", `"PolicyName":"orphan","PolicyDocument":` + cfnIAMDoc("s3:GetObject"),
			"you must specify at least one of these properties"},
		{"no name", `"Roles":["someone"],"PolicyDocument":` + cfnIAMDoc("s3:GetObject"),
			"PolicyName is a required property"},
		{"a principal that does not exist", `"PolicyName":"x","Roles":["absent-role"],"PolicyDocument":` +
			cfnIAMDoc("s3:GetObject"), "NoSuchEntity"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, _, _, _ := newSweepDeployer(t)
			result, err := d.Deploy(context.Background(),
				`{"Resources":{"P":{"Type":"AWS::IAM::Policy","Properties":{`+tt.props+`}}}}`, "refused", nil)
			require.NoError(t, err)
			assert.Equal(t, "ROLLBACK_COMPLETE", result.Status)
			assert.Contains(t, resourceByLogicalID(t, result, "P").Error, tt.reason)
		})
	}
}

func TestCFN_IAMPolicyUpdateMovesItBetweenPrincipals(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	tmpl := func(role, name string) string {
		return `{"Resources":{
			"A":{"Type":"AWS::IAM::Role","Properties":{"RoleName":"role-a","AssumeRolePolicyDocument":` + cfnIAMTrust + `}},
			"B":{"Type":"AWS::IAM::Role","Properties":{"RoleName":"role-b","AssumeRolePolicyDocument":` + cfnIAMTrust + `}},
			"P":{"Type":"AWS::IAM::Policy","Properties":{"PolicyName":"` + name + `","Roles":[{"Ref":"` + role + `"}],
				"PolicyDocument":` + cfnIAMDoc("s3:GetObject") + `}}}}`
	}
	created, err := d.Deploy(ctx, tmpl("A", "moving"), "moving", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, created)

	updated, err := d.UpdateStack(ctx, tmpl("B", "moving"), "moving", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
	assert.Equal(t, resourceByLogicalID(t, created, "P").PhysicalID, resourceByLogicalID(t, updated, "P").PhysicalID,
		"every property is No interruption, so the physical ID stands")
	assert.Empty(t, cfnIAMMembers(t, cfnIAMOK(t, d, "ListRolePolicies", map[string]string{"RoleName": "role-a"}), "PolicyNames>member"))
	assert.Equal(t, []string{"moving"},
		cfnIAMMembers(t, cfnIAMOK(t, d, "ListRolePolicies", map[string]string{"RoleName": "role-b"}), "PolicyNames>member"))

	renamed, err := d.UpdateStack(ctx, tmpl("B", "renamed"), "moving", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_COMPLETE", renamed.Status, renamed.StatusReason)
	assert.Equal(t, []string{"renamed"},
		cfnIAMMembers(t, cfnIAMOK(t, d, "ListRolePolicies", map[string]string{"RoleName": "role-b"}), "PolicyNames>member"),
		"a renamed policy replaces the old name on the principal")
}

// TestCFN_IAMPolicyRecordedAsAManagedPolicyIsStillDeleted reproduces a stack recorded before
// #1562, whose AWS::IAM::Policy was a customer managed policy: deleting the stack deletes it.
func TestCFN_IAMPolicyRecordedAsAManagedPolicyIsStillDeleted(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	tmpl := `{"Resources":{
		"R":{"Type":"AWS::IAM::Role","Properties":{"RoleName":"legacy-role","AssumeRolePolicyDocument":` + cfnIAMTrust + `}},
		"P":{"Type":"AWS::IAM::Policy","Properties":{"PolicyName":"legacy","Roles":[{"Ref":"R"}],
			"PolicyDocument":` + cfnIAMDoc("s3:GetObject") + `}}}}`
	_, err := d.Deploy(ctx, tmpl, "legacy", nil)
	require.NoError(t, err)

	// What the old deploy left: a managed policy, and a record naming it.
	const arn = "arn:aws:iam::123456789012:policy/legacy"
	cfnIAMOK(t, d, "CreatePolicy", map[string]string{"PolicyName": "legacy", "PolicyDocument": cfnIAMDoc("s3:GetObject")})
	cfnIAMOK(t, d, "DeleteRolePolicy", map[string]string{"RoleName": "legacy-role", "PolicyName": "legacy"})
	stacks, err := d.ListStacks(ctx)
	require.NoError(t, err)
	require.Len(t, stacks, 1)
	record := stacks[0]
	for i, r := range record.Resources {
		if r.LogicalID == "P" {
			record.Resources[i].PhysicalID = "legacy"
			record.Resources[i].ARN = arn
			record.Resources[i].Metadata = nil
		}
	}
	require.NoError(t, d.PersistStackForTest(ctx, record))

	require.NoError(t, d.DeleteStack(ctx, "legacy"))
	_, err = cfnIAMDo(t, d, "GetPolicy", map[string]string{"PolicyArn": arn})
	require.Error(t, err, "the managed policy an earlier release created does not leak")
	assert.Contains(t, err.Error(), "NoSuchEntity")
}

func TestCFN_IAMManagedPolicyDeploysUpdatesAndDeletes(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	tmpl := func(action string, roles string) string {
		return `{"Resources":{
			"R":{"Type":"AWS::IAM::Role","Properties":{"RoleName":"mp-role","AssumeRolePolicyDocument":` + cfnIAMTrust + `}},
			"U":{"Type":"AWS::IAM::User","Properties":{"UserName":"mp-user"}},
			"Pol":{"Type":"AWS::IAM::ManagedPolicy","Properties":{"ManagedPolicyName":"stack-managed",
				"Description":"for the tests",` + roles + `
				"PolicyDocument":` + cfnIAMDoc(action) + `}}},
			"Outputs":{"Ref":{"Value":{"Ref":"Pol"}},"Arn":{"Value":{"Fn::GetAtt":["Pol","PolicyArn"]}},
				"Id":{"Value":{"Fn::GetAtt":["Pol","PolicyId"]}},
				"Default":{"Value":{"Fn::GetAtt":["Pol","DefaultVersionId"]}},
				"Count":{"Value":{"Fn::GetAtt":["Pol","AttachmentCount"]}}}}`
	}
	const arn = "arn:aws:iam::123456789012:policy/stack-managed"
	result, err := d.Deploy(ctx, tmpl("s3:GetObject", `"Roles":[{"Ref":"R"}],`), "managed", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, result)
	assert.Equal(t, arn, result.Outputs["Ref"], "Ref returns the ARN")
	assert.Equal(t, arn, result.Outputs["Arn"])
	assert.Equal(t, arn, resourceByLogicalID(t, result, "Pol").PhysicalID)
	assert.True(t, strings.HasPrefix(result.Outputs["Id"], "ANPA"), "PolicyId is the policy's own ID: %q", result.Outputs["Id"])
	assert.Equal(t, "v1", result.Outputs["Default"])
	assert.Equal(t, "1", result.Outputs["Count"])
	assert.Equal(t, []string{arn}, cfnIAMMembers(t,
		cfnIAMOK(t, d, "ListAttachedRolePolicies", map[string]string{"RoleName": "mp-role"}), "AttachedPolicies>member>PolicyArn"))

	// A changed document is a new default version, six times over: the fifth version is the
	// limit, and the oldest non-default one is pruned to make room.
	for i, action := range []string{"a:A", "b:B", "c:C", "d:D", "e:E", "f:F"} {
		updated, err := d.UpdateStack(ctx, tmpl(action, `"Users":[{"Ref":"U"}],`), "managed", nil, emulator.CFNDeployOptions{})
		require.NoError(t, err)
		require.Equal(t, "UPDATE_COMPLETE", updated.Status, "update %d: %s", i, updated.StatusReason)
		assert.Equal(t, arn, resourceByLogicalID(t, updated, "Pol").PhysicalID, "the policy is updated, not replaced")
	}
	var versions struct {
		IDs      []string `xml:"ListPolicyVersionsResult>Versions>member>VersionId"`
		Defaults []bool   `xml:"ListPolicyVersionsResult>Versions>member>IsDefaultVersion"`
	}
	require.NoError(t, xml.Unmarshal(cfnIAMOK(t, d, "ListPolicyVersions", map[string]string{"PolicyArn": arn}), &versions))
	assert.Equal(t, []string{"v7", "v6", "v5", "v4", "v3"}, versions.IDs)
	assert.Equal(t, []bool{true, false, false, false, false}, versions.Defaults)
	assert.Empty(t, cfnIAMMembers(t,
		cfnIAMOK(t, d, "ListAttachedRolePolicies", map[string]string{"RoleName": "mp-role"}), "AttachedPolicies>member>PolicyArn"),
		"the role the declaration dropped is detached")
	assert.Equal(t, []string{arn}, cfnIAMMembers(t,
		cfnIAMOK(t, d, "ListAttachedUserPolicies", map[string]string{"UserName": "mp-user"}), "AttachedPolicies>member>PolicyArn"))

	require.NoError(t, d.DeleteStack(ctx, "managed"),
		"the delete detaches the policy and deletes its versions before deleting it")
	_, err = cfnIAMDo(t, d, "GetPolicy", map[string]string{"PolicyArn": arn})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "NoSuchEntity")
}

func TestCFN_IAMUserAndGroupDeployAndDelete(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	const readOnly = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
	tmpl := `{"Resources":{
		"G":{"Type":"AWS::IAM::Group","Properties":{"Path":"/team/","ManagedPolicyArns":["` + readOnly + `"],
			"Policies":[{"PolicyName":"group-inline","PolicyDocument":` + cfnIAMDoc("sqs:SendMessage") + `}]}},
		"U":{"Type":"AWS::IAM::User","Properties":{"UserName":"cfn-user","Groups":[{"Ref":"G"}],
			"ManagedPolicyArns":["` + readOnly + `"],
			"Policies":[{"PolicyName":"user-inline","PolicyDocument":` + cfnIAMDoc("sns:Publish") + `}],
			"Tags":[{"Key":"team","Value":"data"}]}}},
		"Outputs":{"UserArn":{"Value":{"Fn::GetAtt":["U","Arn"]}},"GroupArn":{"Value":{"Fn::GetAtt":["G","Arn"]}},
			"GroupRef":{"Value":{"Ref":"G"}}}}`
	result, err := d.Deploy(ctx, tmpl, "people", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, result)

	group := result.Outputs["GroupRef"]
	require.NotEmpty(t, group)
	assert.NotEqual(t, "G", group, "a group without a GroupName gets a generated one, not the logical ID")
	assert.Equal(t, "arn:aws:iam::123456789012:user/cfn-user", result.Outputs["UserArn"])
	assert.Equal(t, "arn:aws:iam::123456789012:group/team/"+group, result.Outputs["GroupArn"])

	assert.Equal(t, []string{group}, cfnIAMMembers(t,
		cfnIAMOK(t, d, "ListGroupsForUser", map[string]string{"UserName": "cfn-user"}), "Groups>member>GroupName"))
	assert.Equal(t, []string{readOnly}, cfnIAMMembers(t,
		cfnIAMOK(t, d, "ListAttachedUserPolicies", map[string]string{"UserName": "cfn-user"}), "AttachedPolicies>member>PolicyArn"))
	assert.Equal(t, []string{readOnly}, cfnIAMMembers(t,
		cfnIAMOK(t, d, "ListAttachedGroupPolicies", map[string]string{"GroupName": group}), "AttachedPolicies>member>PolicyArn"))
	cfnIAMOK(t, d, "GetUserPolicy", map[string]string{"UserName": "cfn-user", "PolicyName": "user-inline"})
	cfnIAMOK(t, d, "GetGroupPolicy", map[string]string{"GroupName": group, "PolicyName": "group-inline"})
	assert.Equal(t, []string{"team"}, cfnIAMMembers(t,
		cfnIAMOK(t, d, "ListUserTags", map[string]string{"UserName": "cfn-user"}), "Tags>member>Key"))

	require.NoError(t, d.DeleteStack(ctx, "people"),
		"the user leaves its group and drops its policies first, so both delete")
	for _, probe := range []struct{ op, member, name string }{
		{"GetUser", "UserName", "cfn-user"}, {"GetGroup", "GroupName", group},
	} {
		_, err := cfnIAMDo(t, d, probe.op, map[string]string{probe.member: probe.name})
		require.Error(t, err, probe.op)
		assert.Contains(t, err.Error(), "NoSuchEntity")
	}
}

func TestCFN_IAMUserAndGroupPathChangeAppliesInPlace(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	tmpl := func(path string) string {
		return `{"Resources":{
			"G":{"Type":"AWS::IAM::Group","Properties":{"GroupName":"pathy-group","Path":"` + path + `"}},
			"U":{"Type":"AWS::IAM::User","Properties":{"UserName":"pathy","Path":"` + path + `",
				"Groups":[{"Ref":"G"}],"ManagedPolicyArns":["arn:aws:iam::aws:policy/ReadOnlyAccess"]}}}}`
	}
	_, err := d.Deploy(ctx, tmpl("/a/"), "pathy", nil)
	require.NoError(t, err)
	updated, err := d.UpdateStack(ctx, tmpl("/b/"), "pathy", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
	assert.Equal(t, "pathy", resourceByLogicalID(t, updated, "U").PhysicalID, "the user keeps its physical ID")
	assert.Equal(t, []string{"/b/"}, cfnIAMMembers(t, cfnIAMOK(t, d, "GetUser", map[string]string{"UserName": "pathy"}), "User>Path"))
	assert.Equal(t, []string{"arn:aws:iam::123456789012:user/b/pathy"},
		cfnIAMMembers(t, cfnIAMOK(t, d, "GetUser", map[string]string{"UserName": "pathy"}), "User>Arn"))
	assert.Equal(t, []string{"/b/"}, cfnIAMMembers(t,
		cfnIAMOK(t, d, "GetGroup", map[string]string{"GroupName": "pathy-group"}), "Group>Path"))
	assert.Equal(t, []string{"pathy"}, cfnIAMMembers(t,
		cfnIAMOK(t, d, "GetGroup", map[string]string{"GroupName": "pathy-group"}), "Users>member>UserName"),
		"the membership survives the path change")
	require.NoError(t, d.DeleteStack(ctx, "pathy"))
}

func TestCFN_IAMUserLoginProfile(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	tmpl := func(profile string) string {
		return `{"Resources":{"U":{"Type":"AWS::IAM::User","Properties":{"UserName":"console"` + profile + `}}}}`
	}
	result, err := d.Deploy(ctx, tmpl(`,"LoginProfile":{"Password":"Initial-Passw0rd!","PasswordResetRequired":true}`), "console", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, result)
	got := cfnIAMOK(t, d, "GetLoginProfile", map[string]string{"UserName": "console"})
	assert.Equal(t, []string{"true"}, cfnIAMMembers(t, got, "LoginProfile>PasswordResetRequired"))
	assert.NotContains(t, string(got), "Initial-Passw0rd!")

	updated, err := d.UpdateStack(ctx, tmpl(`,"LoginProfile":{"Password":"Next-Passw0rd!","PasswordResetRequired":false}`),
		"console", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
	got = cfnIAMOK(t, d, "GetLoginProfile", map[string]string{"UserName": "console"})
	assert.Equal(t, []string{"false"}, cfnIAMMembers(t, got, "LoginProfile>PasswordResetRequired"))

	updated, err = d.UpdateStack(ctx, tmpl(``), "console", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
	_, err = cfnIAMDo(t, d, "GetLoginProfile", map[string]string{"UserName": "console"})
	require.Error(t, err, "a LoginProfile the declaration drops is deleted")

	_, err = d.UpdateStack(ctx, tmpl(`,"LoginProfile":{"Password":"Again-Passw0rd!"}`), "console", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.NoError(t, d.DeleteStack(ctx, "console"), "the stack delete removes the login profile before the user")
	_, err = cfnIAMDo(t, d, "GetUser", map[string]string{"UserName": "console"})
	require.Error(t, err)
}

func TestCFN_IAMRoleAppliesItsPoliciesAndUpdatesInPlace(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	const readOnly = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
	tmpl := func(description, managed, inline, tags string) string {
		return `{"Resources":{"R":{"Type":"AWS::IAM::Role","Properties":{"RoleName":"rich-role",
			"Description":"` + description + `","MaxSessionDuration":7200,
			"AssumeRolePolicyDocument":` + cfnIAMTrust + `,
			"ManagedPolicyArns":[` + managed + `],
			"Policies":[` + inline + `],"Tags":[` + tags + `]}}},
			"Outputs":{"RoleId":{"Value":{"Fn::GetAtt":["R","RoleId"]}}}}`
	}
	inline := `{"PolicyName":"inline-one","PolicyDocument":` + cfnIAMDoc("s3:GetObject") + `}`
	created, err := d.Deploy(ctx, tmpl("first", `"`+readOnly+`"`, inline, `{"Key":"a","Value":"1"}`), "rich", nil)
	require.NoError(t, err)
	requireNoResourceErrors(t, created)
	assert.True(t, strings.HasPrefix(created.Outputs["RoleId"], "AROA"), "RoleId: %q", created.Outputs["RoleId"])

	assert.Equal(t, []string{readOnly}, cfnIAMMembers(t,
		cfnIAMOK(t, d, "ListAttachedRolePolicies", map[string]string{"RoleName": "rich-role"}), "AttachedPolicies>member>PolicyArn"),
		"ManagedPolicyArns is attached")
	assert.Equal(t, []string{"inline-one"}, cfnIAMMembers(t,
		cfnIAMOK(t, d, "ListRolePolicies", map[string]string{"RoleName": "rich-role"}), "PolicyNames>member"),
		"Policies is embedded")
	role := cfnIAMOK(t, d, "GetRole", map[string]string{"RoleName": "rich-role"})
	assert.Contains(t, string(role), "<MaxSessionDuration>7200</MaxSessionDuration>")

	updated, err := d.UpdateStack(ctx, tmpl("second", "", "", `{"Key":"b","Value":"2"}`), "rich", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
	assert.Equal(t, created.Outputs["RoleId"], updated.Outputs["RoleId"], "the role is updated in place, not re-created")

	role = cfnIAMOK(t, d, "GetRole", map[string]string{"RoleName": "rich-role"})
	assert.Contains(t, string(role), "<Description>second</Description>")
	assert.Empty(t, cfnIAMMembers(t,
		cfnIAMOK(t, d, "ListAttachedRolePolicies", map[string]string{"RoleName": "rich-role"}), "AttachedPolicies>member>PolicyArn"))
	assert.Empty(t, cfnIAMMembers(t,
		cfnIAMOK(t, d, "ListRolePolicies", map[string]string{"RoleName": "rich-role"}), "PolicyNames>member"))
	assert.Equal(t, []string{"b"}, cfnIAMMembers(t,
		cfnIAMOK(t, d, "ListRoleTags", map[string]string{"RoleName": "rich-role"}), "Tags>member>Key"))

	require.NoError(t, d.DeleteStack(ctx, "rich"))
}

// TestCFN_IAMRoleDeletesAfterItsAttachments pins the delete of a role whose template attached a
// managed policy: before #1545 nothing attached one, and DeleteRole refuses a role that still has
// one, so the delete detaches it first.
func TestCFN_IAMRoleDeletesAfterItsAttachments(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	_, err := d.Deploy(ctx, `{"Resources":{"R":{"Type":"AWS::IAM::Role","Properties":{"RoleName":"attached-role",
		"AssumeRolePolicyDocument":`+cfnIAMTrust+`,
		"ManagedPolicyArns":["arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"]}}}}`, "attached", nil)
	require.NoError(t, err)
	require.NoError(t, d.DeleteStack(ctx, "attached"))
}
