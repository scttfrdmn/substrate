package emulator_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Group membership and group policies. The point of the whole set is that a group
// now has an observable effect: before this, CreateGroup/GetGroup/DeleteGroup/
// ListGroups were routed and nothing else, so GetGroup reported every group as
// empty and no policy could be put on a group at all.
//
// The authorization half — a group's policy applying to its members' requests —
// lives in authz_groups_test.go, because that is a CheckAccess behavior rather
// than an API-shape one.

// iamCreateGroupAndUser creates a group and a user, failing the test if either
// setup call does not succeed.
func iamCreateGroupAndUser(t *testing.T, srv *emulator.Server, groupName, userName string) {
	t.Helper()
	resp := iamRequest(t, srv, "CreateGroup", map[string]any{"GroupName": groupName})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	resp = iamRequest(t, srv, "CreateUser", map[string]any{"UserName": userName})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}

// iamGroupMemberNames returns the user names a GetGroup response lists.
func iamGroupMemberNames(t *testing.T, srv *emulator.Server, groupName string) []string {
	t.Helper()
	resp := iamRequest(t, srv, "GetGroup", map[string]any{"GroupName": groupName})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result map[string]any
	decodeIAMXML(t, resp, &result)
	raw, ok := result["Users"].([]any)
	if !ok {
		return nil
	}
	names := make([]string, 0, len(raw))
	for _, entry := range raw {
		user, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		name, _ := user["UserName"].(string)
		names = append(names, name)
	}
	return names
}

// iamGroupNamesForUser returns the group names a ListGroupsForUser response lists.
func iamGroupNamesForUser(t *testing.T, srv *emulator.Server, userName string) []string {
	t.Helper()
	resp := iamRequest(t, srv, "ListGroupsForUser", map[string]any{"UserName": userName})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result map[string]any
	decodeIAMXML(t, resp, &result)
	raw, ok := result["Groups"].([]any)
	if !ok {
		return nil
	}
	names := make([]string, 0, len(raw))
	for _, entry := range raw {
		group, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		name, _ := group["GroupName"].(string)
		names = append(names, name)
	}
	return names
}

// TestIAMGroups_MembershipIsVisibleFromBothSides is the reason membership is
// stored twice. Two APIs read it in opposite directions, so a single-sided write
// would leave one of them reporting a membership the other denies.
func TestIAMGroups_MembershipIsVisibleFromBothSides(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)
	iamCreateGroupAndUser(t, srv, "devs", "jill")

	// GetGroup passed iamUserListXML(nil) unconditionally, so this was [] no matter
	// what state held.
	assert.Empty(t, iamGroupMemberNames(t, srv, "devs"), "a new group has no members")
	assert.Empty(t, iamGroupNamesForUser(t, srv, "jill"), "a new user belongs to no group")

	resp := iamRequest(t, srv, "AddUserToGroup", map[string]any{"GroupName": "devs", "UserName": "jill"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	assert.Equal(t, []string{"jill"}, iamGroupMemberNames(t, srv, "devs"))
	assert.Equal(t, []string{"devs"}, iamGroupNamesForUser(t, srv, "jill"))

	resp = iamRequest(t, srv, "RemoveUserFromGroup", map[string]any{"GroupName": "devs", "UserName": "jill"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	assert.Empty(t, iamGroupMemberNames(t, srv, "devs"), "GetGroup still lists a removed member")
	assert.Empty(t, iamGroupNamesForUser(t, srv, "jill"), "ListGroupsForUser still lists a left group")
}

// TestIAMGroups_MembershipWritesAreIdempotent covers both directions: AWS declares
// NoSuchEntity for the group and the user but nothing for the membership itself, so
// neither a repeated add nor a remove of a non-member is an error.
func TestIAMGroups_MembershipWritesAreIdempotent(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)
	iamCreateGroupAndUser(t, srv, "devs", "jill")

	for range 2 {
		resp := iamRequest(t, srv, "AddUserToGroup", map[string]any{"GroupName": "devs", "UserName": "jill"})
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.NoError(t, resp.Body.Close())
	}
	assert.Equal(t, []string{"jill"}, iamGroupMemberNames(t, srv, "devs"),
		"adding a member twice must not duplicate it")

	// Never a member.
	resp := iamRequest(t, srv, "CreateUser", map[string]any{"UserName": "outsider"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
	resp = iamRequest(t, srv, "RemoveUserFromGroup", map[string]any{"GroupName": "devs", "UserName": "outsider"})
	assert.Equal(t, http.StatusOK, resp.StatusCode, "removing a non-member is not an error")
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, []string{"jill"}, iamGroupMemberNames(t, srv, "devs"))
}

// TestIAMGroups_MembersAreSortedAndPaginated pins the member order and the Marker
// cursor, so a caller paging a large group sees each member exactly once.
func TestIAMGroups_MembersAreSortedAndPaginated(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)
	resp := iamRequest(t, srv, "CreateGroup", map[string]any{"GroupName": "devs"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// Added out of order, so the sort is doing the work rather than the insertion.
	for _, name := range []string{"carol", "alice", "bob"} {
		resp = iamRequest(t, srv, "CreateUser", map[string]any{"UserName": name})
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.NoError(t, resp.Body.Close())
		resp = iamRequest(t, srv, "AddUserToGroup", map[string]any{"GroupName": "devs", "UserName": name})
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.NoError(t, resp.Body.Close())
	}
	assert.Equal(t, []string{"alice", "bob", "carol"}, iamGroupMemberNames(t, srv, "devs"))

	resp = iamRequest(t, srv, "GetGroup", map[string]any{"GroupName": "devs", "MaxItems": 2})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var page map[string]any
	decodeIAMXML(t, resp, &page)
	require.Len(t, page["Users"], 2)
	assert.Equal(t, true, page["IsTruncated"])
	marker, ok := page["Marker"].(string)
	require.True(t, ok, "a truncated page must carry a Marker")

	resp = iamRequest(t, srv, "GetGroup", map[string]any{"GroupName": "devs", "Marker": marker})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	decodeIAMXML(t, resp, &page)
	require.Len(t, page["Users"], 1, "the second page holds the remaining member")
	assert.Equal(t, false, page["IsTruncated"])
}

// TestIAMGroups_MembershipRefusesUnknownEntities covers the two NoSuchEntity arms
// the model declares for both membership operations.
func TestIAMGroups_MembershipRefusesUnknownEntities(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)
	iamCreateGroupAndUser(t, srv, "devs", "jill")

	tests := []struct {
		name      string
		operation string
		body      map[string]any
		wantCode  string
		wantHTTP  int
	}{
		{
			name: "add to unknown group", operation: "AddUserToGroup",
			body:     map[string]any{"GroupName": "ghosts", "UserName": "jill"},
			wantCode: "NoSuchEntity", wantHTTP: http.StatusNotFound,
		},
		{
			name: "add unknown user", operation: "AddUserToGroup",
			body:     map[string]any{"GroupName": "devs", "UserName": "ghost"},
			wantCode: "NoSuchEntity", wantHTTP: http.StatusNotFound,
		},
		{
			name: "remove from unknown group", operation: "RemoveUserFromGroup",
			body:     map[string]any{"GroupName": "ghosts", "UserName": "jill"},
			wantCode: "NoSuchEntity", wantHTTP: http.StatusNotFound,
		},
		{
			name: "remove unknown user", operation: "RemoveUserFromGroup",
			body:     map[string]any{"GroupName": "devs", "UserName": "ghost"},
			wantCode: "NoSuchEntity", wantHTTP: http.StatusNotFound,
		},
		{
			name: "list groups for unknown user", operation: "ListGroupsForUser",
			body:     map[string]any{"UserName": "ghost"},
			wantCode: "NoSuchEntity", wantHTTP: http.StatusNotFound,
		},
		{
			name: "add with no user name", operation: "AddUserToGroup",
			body:     map[string]any{"GroupName": "devs"},
			wantCode: "ValidationError", wantHTTP: http.StatusBadRequest,
		},
		{
			name: "add with no group name", operation: "AddUserToGroup",
			body:     map[string]any{"UserName": "jill"},
			wantCode: "ValidationError", wantHTTP: http.StatusBadRequest,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := iamRequest(t, srv, tt.operation, tt.body)
			assert.Equal(t, tt.wantHTTP, resp.StatusCode)
			var result map[string]any
			decodeIAMXML(t, resp, &result)
			assert.Equal(t, tt.wantCode, result["__type"])
		})
	}
}

// TestIAMGroups_ManagedPolicyAttachment mirrors the user and role attach handlers:
// attach, list, detach, and the NoSuchEntity a detach of an unattached policy gets.
func TestIAMGroups_ManagedPolicyAttachment(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)
	resp := iamRequest(t, srv, "CreateGroup", map[string]any{"GroupName": "devs"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	const policyARN = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"

	resp = iamRequest(t, srv, "AttachGroupPolicy", map[string]any{
		"GroupName": "devs", "PolicyArn": policyARN,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// Attaching twice must not duplicate the entry.
	resp = iamRequest(t, srv, "AttachGroupPolicy", map[string]any{
		"GroupName": "devs", "PolicyArn": policyARN,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = iamRequest(t, srv, "ListAttachedGroupPolicies", map[string]any{"GroupName": "devs"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result map[string]any
	decodeIAMXML(t, resp, &result)
	attached, ok := result["AttachedPolicies"].([]any)
	require.True(t, ok)
	require.Len(t, attached, 1)
	entry, ok := attached[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "AmazonS3ReadOnlyAccess", entry["PolicyName"])
	assert.Equal(t, policyARN, entry["PolicyArn"])

	resp = iamRequest(t, srv, "DetachGroupPolicy", map[string]any{
		"GroupName": "devs", "PolicyArn": policyARN,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = iamRequest(t, srv, "ListAttachedGroupPolicies", map[string]any{"GroupName": "devs"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	decodeIAMXML(t, resp, &result)
	assert.Empty(t, result["AttachedPolicies"])

	// Detaching what is not attached is NoSuchEntity, as for a user.
	resp = iamRequest(t, srv, "DetachGroupPolicy", map[string]any{
		"GroupName": "devs", "PolicyArn": policyARN,
	})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	decodeIAMXML(t, resp, &result)
	assert.Equal(t, "NoSuchEntity", result["__type"])

	// An unknown group is NoSuchEntity, matching AttachUserPolicy.
	resp = iamRequest(t, srv, "AttachGroupPolicy", map[string]any{
		"GroupName": "ghosts", "PolicyArn": policyARN,
	})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	decodeIAMXML(t, resp, &result)
	assert.Equal(t, "NoSuchEntity", result["__type"])
}

// TestIAMGroups_InlinePolicyRoundTrip drives the inline family with entityType
// "group". The four handlers previously branched on "role" with a default arm
// treating everything else as a user, so a group policy would have been stored and
// reported under the wrong entity name element and operation name.
func TestIAMGroups_InlinePolicyRoundTrip(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)
	resp := iamRequest(t, srv, "CreateGroup", map[string]any{"GroupName": "devs"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	const doc = `{"Version":"2012-10-17","Statement":[{"Sid":"ReadBuckets","Effect":"Allow",` +
		`"Action":"s3:ListAllMyBuckets","Resource":"*"}]}`

	resp = iamRequest(t, srv, "PutGroupPolicy", map[string]any{
		"GroupName": "devs", "PolicyName": "readbuckets", "PolicyDocument": doc,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = iamRequest(t, srv, "GetGroupPolicy", map[string]any{
		"GroupName": "devs", "PolicyName": "readbuckets",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result map[string]any
	decodeIAMXML(t, resp, &result)
	// GroupName, not UserName: the entity-name element follows the entity type.
	assert.Equal(t, "devs", result["GroupName"])
	assert.Equal(t, "readbuckets", result["PolicyName"])
	assert.Contains(t, result["PolicyDocument"], "ListAllMyBuckets")

	resp = iamRequest(t, srv, "ListGroupPolicies", map[string]any{"GroupName": "devs"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	decodeIAMXML(t, resp, &result)
	assert.Equal(t, []any{"readbuckets"}, result["PolicyNames"])

	resp = iamRequest(t, srv, "DeleteGroupPolicy", map[string]any{
		"GroupName": "devs", "PolicyName": "readbuckets",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = iamRequest(t, srv, "GetGroupPolicy", map[string]any{
		"GroupName": "devs", "PolicyName": "readbuckets",
	})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	decodeIAMXML(t, resp, &result)
	assert.Equal(t, "NoSuchEntity", result["__type"])
}

// TestIAMGroups_PutGroupPolicyRefusesUnknownGroup proves the existence check reads
// group state rather than role state — the default arm it replaced looked every
// non-user type up under "role:", so a group named like an existing role would have
// been accepted.
func TestIAMGroups_PutGroupPolicyRefusesUnknownGroup(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	trustDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`
	resp := iamRequest(t, srv, "CreateRole", map[string]any{
		"RoleName": "devs", "AssumeRolePolicyDocument": trustDoc,
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// A role called "devs" exists; a group called "devs" does not.
	resp = iamRequest(t, srv, "PutGroupPolicy", map[string]any{
		"GroupName": "devs", "PolicyName": "p",
		"PolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
	})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	var result map[string]any
	decodeIAMXML(t, resp, &result)
	assert.Equal(t, "NoSuchEntity", result["__type"])
}

// TestIAMGroups_DeleteRefusesWhileSubordinatesExist covers the DeleteGroup
// reference's requirement — "The group must not contain any users or have any
// attached policies" — and the DeleteUser one, "remove … Group memberships".
//
// Both matter for the same reason: a delete that skipped the check would leave one
// side of the membership index naming an entity that no longer exists, and that
// dangling membership is read by loadPoliciesForPrincipal on every request.
func TestIAMGroups_DeleteRefusesWhileSubordinatesExist(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)
	iamCreateGroupAndUser(t, srv, "devs", "jill")
	resp := iamRequest(t, srv, "AddUserToGroup", map[string]any{"GroupName": "devs", "UserName": "jill"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	var result map[string]any

	resp = iamRequest(t, srv, "DeleteGroup", map[string]any{"GroupName": "devs"})
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	decodeIAMXML(t, resp, &result)
	assert.Equal(t, "DeleteConflict", result["__type"])
	assert.Contains(t, result["message"], "jill", "the message names who to remove")

	resp = iamRequest(t, srv, "DeleteUser", map[string]any{"UserName": "jill"})
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	decodeIAMXML(t, resp, &result)
	assert.Equal(t, "DeleteConflict", result["__type"])
	assert.Contains(t, result["message"], "devs", "the message names which group holds the user")

	// Removing the membership clears both refusals.
	resp = iamRequest(t, srv, "RemoveUserFromGroup", map[string]any{"GroupName": "devs", "UserName": "jill"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = iamRequest(t, srv, "DeleteUser", map[string]any{"UserName": "jill"})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// An attached policy is the other subordinate DeleteGroup refuses on.
	resp = iamRequest(t, srv, "AttachGroupPolicy", map[string]any{
		"GroupName": "devs", "PolicyArn": "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = iamRequest(t, srv, "DeleteGroup", map[string]any{"GroupName": "devs"})
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	decodeIAMXML(t, resp, &result)
	assert.Equal(t, "DeleteConflict", result["__type"])

	resp = iamRequest(t, srv, "DetachGroupPolicy", map[string]any{
		"GroupName": "devs", "PolicyArn": "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	resp = iamRequest(t, srv, "DeleteGroup", map[string]any{"GroupName": "devs"})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}

// TestIAMGroupsWire_MembershipOverTheQueryProtocol drives the new operations the
// way a client does, so an operation that is implemented but unroutable — #636 —
// or a parameter that never reaches the handler — #639 — is caught by this package
// rather than only by the e2e module.
func TestIAMGroupsWire_MembershipOverTheQueryProtocol(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	for _, op := range []struct {
		operation string
		params    map[string]string
	}{
		{"CreateGroup", map[string]string{"GroupName": "devs"}},
		{"CreateUser", map[string]string{"UserName": "jill"}},
		{"AddUserToGroup", map[string]string{"GroupName": "devs", "UserName": "jill"}},
	} {
		resp := iamFormRequest(t, srv, op.operation, op.params)
		require.Equal(t, http.StatusOK, resp.StatusCode, op.operation)
		require.NoError(t, resp.Body.Close())
	}

	resp := iamFormRequest(t, srv, "GetGroup", map[string]string{"GroupName": "devs"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var result map[string]any
	decodeIAMXML(t, resp, &result)
	users, ok := result["Users"].([]any)
	require.True(t, ok, "GetGroup must report a Users list")
	require.Len(t, users, 1)

	resp = iamFormRequest(t, srv, "ListGroupsForUser", map[string]string{"UserName": "jill"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	decodeIAMXML(t, resp, &result)
	groups, ok := result["Groups"].([]any)
	require.True(t, ok, "ListGroupsForUser must report a Groups list")
	require.Len(t, groups, 1)
}
