package emulator_test

import (
	"context"
	"encoding/xml"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The IAM updaters under a rollback, and the review findings on PR #1585.

// cfnIAMFailAfter is a resource that fails its create after the named resource has deployed: an
// AWS::IAM::Policy naming no principal, which DependsOn orders after it.
func cfnIAMFailAfter(logicalID string) string {
	return `,"Fails":{"Type":"AWS::IAM::Policy","DependsOn":"` + logicalID + `",
		"Properties":{"PolicyName":"fails","PolicyDocument":` + cfnIAMDoc("s3:GetObject") + `}}`
}

// cfnIAMCount counts op among the stream's recorded operations.
func cfnIAMCount(t *testing.T, store *emulator.EventStore, stream, op string) int {
	t.Helper()
	n := 0
	for _, o := range sweepOperations(t, store, stream) {
		if o == op {
			n++
		}
	}
	return n
}

// TestCFN_IAMRoleRollbackUndoesWhatTheFailedUpdateAdded is review finding 1: the rollback diffs
// against what the failed update left in place, not against the previous deployment's record, so
// it detaches the managed policy, deletes the inline policy and removes the tag the failed update
// added — and the stack still deletes cleanly afterwards.
func TestCFN_IAMRoleRollbackUndoesWhatTheFailedUpdateAdded(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	const readOnly = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
	role := func(extra string) string {
		return `"R":{"Type":"AWS::IAM::Role","Properties":{"RoleName":"rollback-role",
			"AssumeRolePolicyDocument":` + cfnIAMTrust + extra + `}}`
	}
	_, err := d.Deploy(ctx, `{"Resources":{`+role("")+`}}`, "rollback-role", nil)
	require.NoError(t, err)

	failed, err := d.UpdateStack(ctx, `{"Resources":{`+role(`,
		"ManagedPolicyArns":["`+readOnly+`"],
		"Policies":[{"PolicyName":"added","PolicyDocument":`+cfnIAMDoc("s3:PutObject")+`}],
		"Tags":[{"Key":"added","Value":"yes"}]`)+cfnIAMFailAfter("R")+`}}`,
		"rollback-role", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_ROLLBACK_COMPLETE", failed.Status, failed.StatusReason)

	assert.Empty(t, cfnIAMMembers(t, cfnIAMOK(t, d, "ListAttachedRolePolicies",
		map[string]string{"RoleName": "rollback-role"}), "AttachedPolicies>member>PolicyArn"),
		"the managed policy the failed update attached is detached")
	assert.Empty(t, cfnIAMMembers(t, cfnIAMOK(t, d, "ListRolePolicies",
		map[string]string{"RoleName": "rollback-role"}), "PolicyNames>member"))
	assert.Empty(t, cfnIAMMembers(t, cfnIAMOK(t, d, "ListRoleTags",
		map[string]string{"RoleName": "rollback-role"}), "Tags>member>Key"))

	require.NoError(t, d.DeleteStack(ctx, "rollback-role"))
}

// TestCFN_IAMUserRollbackKeepsAnUnchangedPath is review finding 2: a rollback re-applies every
// declared property, Path included, and a Path equal to the user's own is not a change to refuse.
func TestCFN_IAMUserRollbackKeepsAnUnchangedPath(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	user := func(tag string) string {
		return `"U":{"Type":"AWS::IAM::User","Properties":{"UserName":"pathed-user","Path":"/team/",
			"Tags":[{"Key":"stage","Value":"` + tag + `"}]}}`
	}
	_, err := d.Deploy(ctx, `{"Resources":{`+user("one")+`}}`, "pathed", nil)
	require.NoError(t, err)

	failed, err := d.UpdateStack(ctx, `{"Resources":{`+user("two")+cfnIAMFailAfter("U")+`}}`,
		"pathed", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_ROLLBACK_COMPLETE", failed.Status, failed.StatusReason)
	assert.NotContains(t, failed.StatusReason, "UpdateUser", "an unchanged Path is not refused")

	var tags struct {
		Values []string `xml:"ListUserTagsResult>Tags>member>Value"`
	}
	require.NoError(t, xml.Unmarshal(cfnIAMOK(t, d, "ListUserTags", map[string]string{"UserName": "pathed-user"}), &tags))
	assert.Equal(t, []string{"one"}, tags.Values, "the rollback restored the previous tag")
}

// TestCFN_IAMPolicyFailedRenameLeavesNoCopy is review finding 4: a rename whose put fails on one
// principal takes the new-name copies it made back out, so nothing is orphaned under the new name,
// and the stack still deletes every copy it holds.
func TestCFN_IAMPolicyFailedRenameLeavesNoCopy(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	for _, r := range []string{"ext-a", "ext-b"} {
		cfnIAMOK(t, d, "CreateRole", map[string]string{"RoleName": r, "AssumeRolePolicyDocument": cfnIAMTrust})
	}
	tmpl := func(name string) string {
		return `{"Resources":{"P":{"Type":"AWS::IAM::Policy","Properties":{"PolicyName":"` + name + `",
			"Roles":["ext-a","ext-b"],"PolicyDocument":` + cfnIAMDoc("s3:GetObject") + `}}}}`
	}
	_, err := d.Deploy(ctx, tmpl("old-name"), "renaming", nil)
	require.NoError(t, err)

	// ext-b goes away out of band, so the rename's put on it fails.
	cfnIAMOK(t, d, "DeleteRolePolicy", map[string]string{"RoleName": "ext-b", "PolicyName": "old-name"})
	cfnIAMOK(t, d, "DeleteRole", map[string]string{"RoleName": "ext-b"})

	failed, err := d.UpdateStack(ctx, tmpl("new-name"), "renaming", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_ROLLBACK_COMPLETE", failed.Status, failed.StatusReason)
	assert.Equal(t, []string{"old-name"}, cfnIAMMembers(t,
		cfnIAMOK(t, d, "ListRolePolicies", map[string]string{"RoleName": "ext-a"}), "PolicyNames>member"),
		"the copy put under the new name is taken back out")

	require.NoError(t, d.DeleteStack(ctx, "renaming"))
	assert.Empty(t, cfnIAMMembers(t,
		cfnIAMOK(t, d, "ListRolePolicies", map[string]string{"RoleName": "ext-a"}), "PolicyNames>member"))
}

// TestCFN_IAMRoleTagKeyCaseChangeKeepsTheTag is review finding 5: a role's tag keys are not case
// sensitive, so a key whose case alone changed is replaced, not removed.
func TestCFN_IAMRoleTagKeyCaseChangeKeepsTheTag(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	tmpl := func(key, value string) string {
		return `{"Resources":{"R":{"Type":"AWS::IAM::Role","Properties":{"RoleName":"cased-role",
			"AssumeRolePolicyDocument":` + cfnIAMTrust + `,"Tags":[{"Key":"` + key + `","Value":"` + value + `"}]}}}}`
	}
	_, err := d.Deploy(ctx, tmpl("team", "a"), "cased", nil)
	require.NoError(t, err)
	updated, err := d.UpdateStack(ctx, tmpl("Team", "b"), "cased", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)

	var tags struct {
		Keys   []string `xml:"ListRoleTagsResult>Tags>member>Key"`
		Values []string `xml:"ListRoleTagsResult>Tags>member>Value"`
	}
	require.NoError(t, xml.Unmarshal(cfnIAMOK(t, d, "ListRoleTags", map[string]string{"RoleName": "cased-role"}), &tags))
	require.Len(t, tags.Keys, 1, "the tag is kept")
	assert.Equal(t, []string{"b"}, tags.Values)
}

// TestCFN_IAMManagedPolicyRollbackReusesAVersion is review finding 8: a rollback re-applies the
// previous document, which a version already holds, so it makes that version the default again
// rather than creating a duplicate — and a rollback that re-applies an unchanged document makes no
// version at all.
func TestCFN_IAMManagedPolicyRollbackReusesAVersion(t *testing.T) {
	const arn = "arn:aws:iam::123456789012:policy/rolled-back"
	tests := []struct {
		name         string
		action, user string
		wantIDs      []string
	}{
		{"a document change", "s3:PutObject", "", []string{"v2", "v1"}},
		{"a principal change", "s3:GetObject", `"Users":[{"Ref":"U"}],`, []string{"v1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, _, _, _ := newSweepDeployer(t)
			ctx := context.Background()
			tmpl := func(action, principals, tail string) string {
				return `{"Resources":{"U":{"Type":"AWS::IAM::User","Properties":{"UserName":"mp-rollback-user"}},
					"Pol":{"Type":"AWS::IAM::ManagedPolicy","Properties":{"ManagedPolicyName":"rolled-back",` +
					principals + `"PolicyDocument":` + cfnIAMDoc(action) + `}}` + tail + `}}`
			}
			_, err := d.Deploy(ctx, tmpl("s3:GetObject", "", ""), "mp-rollback", nil)
			require.NoError(t, err)

			failed, err := d.UpdateStack(ctx, tmpl(tt.action, tt.user, cfnIAMFailAfter("Pol")), "mp-rollback", nil,
				emulator.CFNDeployOptions{})
			require.NoError(t, err)
			require.Equal(t, "UPDATE_ROLLBACK_COMPLETE", failed.Status, failed.StatusReason)

			var versions struct {
				IDs      []string `xml:"ListPolicyVersionsResult>Versions>member>VersionId"`
				Defaults []bool   `xml:"ListPolicyVersionsResult>Versions>member>IsDefaultVersion"`
			}
			require.NoError(t, xml.Unmarshal(cfnIAMOK(t, d, "ListPolicyVersions", map[string]string{"PolicyArn": arn}), &versions))
			assert.Equal(t, tt.wantIDs, versions.IDs, "no duplicate version")
			assert.True(t, versions.Defaults[len(versions.Defaults)-1], "v1 is the default again")
			assert.Empty(t, cfnIAMMembers(t, cfnIAMOK(t, d, "ListAttachedUserPolicies",
				map[string]string{"UserName": "mp-rollback-user"}), "AttachedPolicies>member>PolicyArn"))
			require.NoError(t, d.DeleteStack(ctx, "mp-rollback"))
		})
	}
}

// TestCFN_IAMRoleUpdateDoesNotRePutUnchangedInlinePolicies is review finding 9: an update that
// changes something other than Policies makes no PutRolePolicy call.
func TestCFN_IAMRoleUpdateDoesNotRePutUnchangedInlinePolicies(t *testing.T) {
	d, _, _, store := newSweepDeployer(t)
	ctx := context.Background()
	tmpl := func(description string) string {
		return `{"Resources":{"R":{"Type":"AWS::IAM::Role","Properties":{"RoleName":"steady-role",
			"Description":"` + description + `","AssumeRolePolicyDocument":` + cfnIAMTrust + `,
			"Policies":[{"PolicyName":"steady","PolicyDocument":` + cfnIAMDoc("s3:GetObject") + `}]}}}}`
	}
	_, err := d.Deploy(ctx, tmpl("first"), "steady", nil)
	require.NoError(t, err)
	puts := cfnIAMCount(t, store, "steady", "PutRolePolicy")
	require.Equal(t, 1, puts)

	updated, err := d.UpdateStack(ctx, tmpl("second"), "steady", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
	assert.Equal(t, puts, cfnIAMCount(t, store, "steady", "PutRolePolicy"))
	assert.Equal(t, 1, cfnIAMCount(t, store, "steady", "UpdateRole"))
}

// TestCFN_IAMReplacementsGetFreshGeneratedNames is review finding 3: a change to a Replacement
// property of a ManagedPolicy, User or Group whose name substrate generates creates the
// replacement under a new generated name (#1545's mintReplacementName), so it does not collide with
// the resource it replaces, and the old one is deleted once the update succeeds.
func TestCFN_IAMReplacementsGetFreshGeneratedNames(t *testing.T) {
	tests := []struct {
		name, resType, before, after, getOp, member string
	}{
		{"managed policy", "AWS::IAM::ManagedPolicy",
			`"Description":"first","PolicyDocument":` + cfnIAMDoc("s3:GetObject"),
			`"Description":"second","PolicyDocument":` + cfnIAMDoc("s3:GetObject"), "GetPolicy", "PolicyArn"},
		{"user", "AWS::IAM::User", `"UserName":"named-user"`, ``, "GetUser", "UserName"},
		{"group", "AWS::IAM::Group", `"GroupName":"named-group"`, ``, "GetGroup", "GroupName"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, _, _, _ := newSweepDeployer(t)
			ctx := context.Background()
			tmpl := func(props string) string {
				return `{"Resources":{"X":{"Type":"` + tt.resType + `","Properties":{` + props + `}}}}`
			}
			created, err := d.Deploy(ctx, tmpl(tt.before), "replacing", nil)
			require.NoError(t, err)
			requireNoResourceErrors(t, created)
			before := resourceByLogicalID(t, created, "X").PhysicalID

			updated, err := d.UpdateStack(ctx, tmpl(tt.after), "replacing", nil, emulator.CFNDeployOptions{})
			require.NoError(t, err)
			require.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
			after := resourceByLogicalID(t, updated, "X").PhysicalID
			assert.NotEqual(t, before, after, "the replacement is a new resource")

			cfnIAMOK(t, d, tt.getOp, map[string]string{tt.member: after})
			_, err = cfnIAMDo(t, d, tt.getOp, map[string]string{tt.member: before})
			require.Error(t, err, "the replaced resource is deleted")
			assert.Contains(t, err.Error(), "NoSuchEntity")

			// A second replacement draws yet another name rather than colliding with the first.
			again, err := d.UpdateStack(ctx, tmpl(tt.before), "replacing", nil, emulator.CFNDeployOptions{})
			require.NoError(t, err)
			require.Equal(t, "UPDATE_COMPLETE", again.Status, again.StatusReason)
		})
	}
}
