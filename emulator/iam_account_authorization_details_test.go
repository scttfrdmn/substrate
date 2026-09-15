package emulator_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// GetAccountAuthorizationDetails (#848).
//
// Every assertion here is on the **raw XML** rather than on a decoded map, and that is not a
// style preference: a decoded struct cannot tell an absent member from an empty one, which is
// how `iam_shape_members_test.go` missed the boundary defect #807 fixed. This operation's whole
// value is that four shapes appear in one body, so what it must be shown *not* to render matters
// as much as what it renders.
//
// Everything is created through the API — no `putTest*` helper writes state directly (#765) —
// because a helper writing a record cannot prove the record is reachable through the owning
// service's own call.

// iamAuthzDetailsXML executes GetAccountAuthorizationDetails through the query protocol and
// returns the raw response body.
//
// The query protocol, not iamRequest's hand-marshaled JSON, because `Filter` is a
// `Filter.member.N` list and `MaxItems`' presence is read from the decoded form parameters —
// both are shapes only a real client produces (#639).
func iamAuthzDetailsXML(t *testing.T, srv *emulator.Server, params map[string]string) string {
	t.Helper()
	resp := iamFormRequest(t, srv, "GetAccountAuthorizationDetails", params)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return string(body)
}

// iamXMLSection returns the content of the first element named element, failing the test when
// the body carries none.
func iamXMLSection(t *testing.T, body, element string) string {
	t.Helper()
	openTag, closeTag := "<"+element+">", "</"+element+">"
	start := strings.Index(body, openTag)
	require.GreaterOrEqual(t, start, 0, "%s is not in %s", element, body)
	rest := body[start+len(openTag):]
	end := strings.Index(rest, closeTag)
	require.GreaterOrEqual(t, end, 0, "unterminated %s in %s", element, body)
	return rest[:end]
}

// iamXMLValues returns every value of the named element in body, in document order.
func iamXMLValues(body, element string) []string {
	openTag, closeTag := "<"+element+">", "</"+element+">"
	var out []string
	for rest := body; ; {
		start := strings.Index(rest, openTag)
		if start < 0 {
			return out
		}
		rest = rest[start+len(openTag):]
		end := strings.Index(rest, closeTag)
		if end < 0 {
			return out
		}
		out = append(out, rest[:end])
		rest = rest[end+len(closeTag):]
	}
}

// iamAuthzTestPolicyDoc is the document every policy in this file is created with. It contains a
// colon, a quote and braces, so a percent-encoded rendering is visibly different from a plain
// one.
const iamAuthzTestPolicyDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`

// iamAuthzTestTrustDoc is the trust policy every role in this file is created with.
const iamAuthzTestTrustDoc = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}`

// iamAuthzOK executes an IAM operation and requires a 200, so a fixture step that silently
// failed cannot make a later absence assertion pass for the wrong reason.
func iamAuthzOK(t *testing.T, srv *emulator.Server, operation string, body map[string]string) {
	t.Helper()
	resp := iamRequest(t, srv, operation, body)
	payload, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", operation, payload)
}

// TestIAMAccountAuthorizationDetails_ReportsEveryPopulation asserts the operation reports a
// user, a group, a role and a customer-managed policy created over the wire, each in its own
// list and each with the members its own detail shape carries.
func TestIAMAccountAuthorizationDetails_ReportsEveryPopulation(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	iamAuthzOK(t, srv, "CreateUser", map[string]string{"UserName": "ada"})
	iamAuthzOK(t, srv, "CreateGroup", map[string]string{"GroupName": "engineers"})
	iamAuthzOK(t, srv, "AddUserToGroup", map[string]string{"UserName": "ada", "GroupName": "engineers"})
	iamAuthzOK(t, srv, "CreateRole", map[string]string{
		"RoleName": "deploy", "AssumeRolePolicyDocument": iamAuthzTestTrustDoc,
	})
	iamAuthzOK(t, srv, "CreatePolicy", map[string]string{
		"PolicyName": "read-objects", "PolicyDocument": iamAuthzTestPolicyDoc,
		"Description": "reads objects",
	})
	iamAuthzOK(t, srv, "PutUserPolicy", map[string]string{
		"UserName": "ada", "PolicyName": "inline-user", "PolicyDocument": iamAuthzTestPolicyDoc,
	})
	iamAuthzOK(t, srv, "PutGroupPolicy", map[string]string{
		"GroupName": "engineers", "PolicyName": "inline-group", "PolicyDocument": iamAuthzTestPolicyDoc,
	})
	iamAuthzOK(t, srv, "PutRolePolicy", map[string]string{
		"RoleName": "deploy", "PolicyName": "inline-role", "PolicyDocument": iamAuthzTestPolicyDoc,
	})
	iamAuthzOK(t, srv, "AttachUserPolicy", map[string]string{
		"UserName": "ada", "PolicyArn": "arn:aws:iam::123456789012:policy/read-objects",
	})

	body := iamAuthzDetailsXML(t, srv, nil)

	users := iamXMLSection(t, body, "UserDetailList")
	assert.Contains(t, users, "<UserName>ada</UserName>")
	assert.Contains(t, users, "<GroupList><member>engineers</member></GroupList>",
		"UserDetail carries GroupList, which no other user shape reports")
	assert.Contains(t, users, "<UserPolicyList><member><PolicyName>inline-user</PolicyName>")
	assert.Contains(t, users,
		"<AttachedManagedPolicies><member><PolicyName>read-objects</PolicyName>",
		"the wrapper is AttachedManagedPolicies here, not the ListAttached*Policies spelling")

	groups := iamXMLSection(t, body, "GroupDetailList")
	assert.Contains(t, groups, "<GroupName>engineers</GroupName>")
	assert.Contains(t, groups, "<GroupPolicyList><member><PolicyName>inline-group</PolicyName>")

	roles := iamXMLSection(t, body, "RoleDetailList")
	assert.Contains(t, roles, "<RoleName>deploy</RoleName>")
	assert.Contains(t, roles, "<RolePolicyList><member><PolicyName>inline-role</PolicyName>")

	policies := iamXMLSection(t, body, "Policies")
	assert.Contains(t, policies, "<PolicyName>read-objects</PolicyName>")
	assert.Contains(t, policies, "<Description>reads objects</Description>",
		"ManagedPolicyDetail lists Description; the carve-out AWS documents is on the listing shape")
	assert.Contains(t, policies, "<PolicyVersionList><member><VersionId>v1</VersionId>")
	assert.Contains(t, policies, "<PolicyName>AmazonS3ReadOnlyAccess</PolicyName>",
		"an unfiltered call reports the bundled AWS managed policies as well")
}

// TestIAMAccountAuthorizationDetails_MemberSets asserts the two shapes whose member sets differ
// from the shapes substrate already rendered: `UserDetail` has no `PasswordLastUsed` and
// `RoleDetail` has neither `Description` nor `MaxSessionDuration`.
//
// The role is created *with* both members, so the assertion fails if the handler reuses
// [iamRoleXMLFields] rather than the identity-only builder — which is exactly what a reviewer
// reading the diff would expect it to do.
func TestIAMAccountAuthorizationDetails_MemberSets(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	iamAuthzOK(t, srv, "CreateUser", map[string]string{"UserName": "ada"})
	iamAuthzOK(t, srv, "CreateRole", map[string]string{
		"RoleName":                 "deploy",
		"AssumeRolePolicyDocument": iamAuthzTestTrustDoc,
		"Description":              "the deploy role",
		"MaxSessionDuration":       "7200",
	})

	// GetRole reports both members, so their absence below is this shape's rule and not a
	// failure to store them.
	roleResp := iamRequest(t, srv, "GetRole", map[string]string{"RoleName": "deploy"})
	require.Equal(t, http.StatusOK, roleResp.StatusCode)
	roleBody, err := io.ReadAll(roleResp.Body)
	require.NoError(t, err)
	require.NoError(t, roleResp.Body.Close())
	require.Contains(t, string(roleBody), "<Description>the deploy role</Description>")
	require.Contains(t, string(roleBody), "<MaxSessionDuration>7200</MaxSessionDuration>")

	body := iamAuthzDetailsXML(t, srv, map[string]string{
		"Filter.member.1": "User", "Filter.member.2": "Role",
	})

	users := iamXMLSection(t, body, "UserDetailList")
	require.Contains(t, users, "<UserName>ada</UserName>",
		"the user must be present for the absence assertion to mean anything")
	assert.NotContains(t, users, "PasswordLastUsed",
		"UserDetail has no PasswordLastUsed member; User does")

	roles := iamXMLSection(t, body, "RoleDetailList")
	require.Contains(t, roles, "<RoleName>deploy</RoleName>",
		"the role must be present for the absence assertions to mean anything")
	assert.NotContains(t, roles, "<Description>",
		"RoleDetail has no Description member")
	assert.NotContains(t, roles, "MaxSessionDuration",
		"RoleDetail has no MaxSessionDuration member")
}

// TestIAMAccountAuthorizationDetails_PolicyDocumentEncodings asserts the two encodings AWS's own
// page contradicts itself about appear side by side in **one** body: `PolicyVersionList`'s
// document is percent-encoded, matching `GetPolicyVersion`, while the trust policy and the
// inline documents are plain JSON, matching `GetRole` and `GetRolePolicy`.
func TestIAMAccountAuthorizationDetails_PolicyDocumentEncodings(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	iamAuthzOK(t, srv, "CreateRole", map[string]string{
		"RoleName": "deploy", "AssumeRolePolicyDocument": iamAuthzTestTrustDoc,
	})
	iamAuthzOK(t, srv, "PutRolePolicy", map[string]string{
		"RoleName": "deploy", "PolicyName": "inline-role", "PolicyDocument": iamAuthzTestPolicyDoc,
	})
	iamAuthzOK(t, srv, "CreatePolicy", map[string]string{
		"PolicyName": "read-objects", "PolicyDocument": iamAuthzTestPolicyDoc,
	})

	body := iamAuthzDetailsXML(t, srv, map[string]string{
		"Filter.member.1": "Role", "Filter.member.2": "LocalManagedPolicy",
	})

	roles := iamXMLSection(t, body, "RoleDetailList")
	trust := iamXMLSection(t, roles, "AssumeRolePolicyDocument")
	assert.Contains(t, trust, "&#34;Version&#34;:&#34;2012-10-17&#34;",
		"AssumeRolePolicyDocument is plain JSON, as GetRole sends it")
	assert.NotContains(t, trust, "%22", "AssumeRolePolicyDocument must not be percent-encoded")

	inline := iamXMLSection(t, roles, "PolicyDocument")
	assert.Contains(t, inline, "&#34;Version&#34;", "an inline PolicyDetail document is plain JSON")
	assert.NotContains(t, inline, "%22", "PolicyDetail.PolicyDocument must not be percent-encoded")

	policies := iamXMLSection(t, body, "Policies")
	document := iamXMLSection(t, policies, "Document")
	assert.Contains(t, document, "%22Version%22%3A%222012-10-17%22",
		"PolicyVersion.Document is the one shape whose own page mandates RFC 3986")
	assert.NotContains(t, document, "&#34;", "PolicyVersion.Document must not be plain JSON")

	// The same policy through GetPolicyVersion, so the two operations are shown to agree
	// byte-for-byte rather than merely both being encoded.
	versionResp := iamRequest(t, srv, "GetPolicyVersion", map[string]string{
		"PolicyArn": "arn:aws:iam::123456789012:policy/read-objects", "VersionId": "v1",
	})
	require.Equal(t, http.StatusOK, versionResp.StatusCode)
	versionBody, err := io.ReadAll(versionResp.Body)
	require.NoError(t, err)
	require.NoError(t, versionResp.Body.Close())
	assert.Equal(t, iamXMLSection(t, string(versionBody), "Document"), document,
		"one policy's document must not depend on which operation reported it")
}

// TestIAMAccountAuthorizationDetails_AttachmentCountAgrees asserts `AttachmentCount` matches
// `GetPolicy`'s and `ListPolicies`' across an attach and a detach.
//
// This is the non-divergence the operation exists to prove: all three read the same derived
// count, so an implementation that stored a count on the policy record would show up here as a
// disagreement rather than as three separately-plausible numbers.
func TestIAMAccountAuthorizationDetails_AttachmentCountAgrees(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	const arn = "arn:aws:iam::123456789012:policy/read-objects"
	iamAuthzOK(t, srv, "CreatePolicy", map[string]string{
		"PolicyName": "read-objects", "PolicyDocument": iamAuthzTestPolicyDoc,
	})
	iamAuthzOK(t, srv, "CreateUser", map[string]string{"UserName": "ada"})
	iamAuthzOK(t, srv, "CreateRole", map[string]string{
		"RoleName": "deploy", "AssumeRolePolicyDocument": iamAuthzTestTrustDoc,
	})

	countHere := func(t *testing.T) string {
		t.Helper()
		body := iamAuthzDetailsXML(t, srv, map[string]string{"Filter.member.1": "LocalManagedPolicy"})
		policies := iamXMLSection(t, body, "Policies")
		require.Contains(t, policies, "<PolicyName>read-objects</PolicyName>")
		return iamXMLSection(t, policies, "AttachmentCount")
	}
	countFrom := func(t *testing.T, operation string, params map[string]string) string {
		t.Helper()
		resp := iamRequest(t, srv, operation, params)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		raw, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		return iamXMLSection(t, string(raw), "AttachmentCount")
	}
	listedCount := func(t *testing.T) string {
		t.Helper()
		resp := iamRequest(t, srv, "ListPolicies", map[string]string{"Scope": "Local"})
		require.Equal(t, http.StatusOK, resp.StatusCode)
		raw, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		return iamXMLSection(t, string(raw), "AttachmentCount")
	}

	for _, step := range []struct {
		name  string
		do    func()
		want  string
		after string
	}{
		{name: "unattached", do: func() {}, want: "0"},
		{name: "one user", do: func() {
			iamAuthzOK(t, srv, "AttachUserPolicy", map[string]string{"UserName": "ada", "PolicyArn": arn})
		}, want: "1"},
		{name: "and one role", do: func() {
			iamAuthzOK(t, srv, "AttachRolePolicy", map[string]string{"RoleName": "deploy", "PolicyArn": arn})
		}, want: "2"},
		{name: "user detached", do: func() {
			iamAuthzOK(t, srv, "DetachUserPolicy", map[string]string{"UserName": "ada", "PolicyArn": arn})
		}, want: "1"},
	} {
		t.Run(step.name, func(t *testing.T) {
			step.do()
			assert.Equal(t, step.want, countHere(t), "GetAccountAuthorizationDetails")
			assert.Equal(t, step.want, countFrom(t, "GetPolicy", map[string]string{"PolicyArn": arn}), "GetPolicy")
			assert.Equal(t, step.want, listedCount(t), "ListPolicies")
		})
	}
}

// TestIAMAccountAuthorizationDetails_BoundaryUsageCountAgrees asserts
// `PermissionsBoundaryUsageCount` matches `GetPolicy`'s, and that the boundary itself is
// rendered on `UserDetail` and `RoleDetail` — the third population of
// [iamPermissionsBoundaryXML]'s split, where the listing shapes carry no boundary at all.
func TestIAMAccountAuthorizationDetails_BoundaryUsageCountAgrees(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	const arn = "arn:aws:iam::123456789012:policy/boundary"
	iamAuthzOK(t, srv, "CreatePolicy", map[string]string{
		"PolicyName": "boundary", "PolicyDocument": iamAuthzTestPolicyDoc,
	})
	iamAuthzOK(t, srv, "CreateUser", map[string]string{"UserName": "ada"})
	iamAuthzOK(t, srv, "CreateRole", map[string]string{
		"RoleName": "deploy", "AssumeRolePolicyDocument": iamAuthzTestTrustDoc,
	})
	iamAuthzOK(t, srv, "PutUserPermissionsBoundary", map[string]string{
		"UserName": "ada", "PermissionsBoundary": arn,
	})
	iamAuthzOK(t, srv, "PutRolePermissionsBoundary", map[string]string{
		"RoleName": "deploy", "PermissionsBoundary": arn,
	})

	body := iamAuthzDetailsXML(t, srv, nil)

	policies := iamXMLSection(t, body, "Policies")
	assert.Equal(t, "2", iamXMLSection(t, policies, "PermissionsBoundaryUsageCount"),
		"one user and one role use the policy as a boundary")

	getResp := iamRequest(t, srv, "GetPolicy", map[string]string{"PolicyArn": arn})
	require.Equal(t, http.StatusOK, getResp.StatusCode)
	getBody, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	require.NoError(t, getResp.Body.Close())
	assert.Equal(t, iamXMLSection(t, string(getBody), "PermissionsBoundaryUsageCount"),
		iamXMLSection(t, policies, "PermissionsBoundaryUsageCount"),
		"the count must not depend on which operation reported it")

	users := iamXMLSection(t, body, "UserDetailList")
	assert.Contains(t, users, "<PermissionsBoundaryArn>"+arn+"</PermissionsBoundaryArn>",
		"UserDetail carries PermissionsBoundary; ListUsers does not")
	roles := iamXMLSection(t, body, "RoleDetailList")
	assert.Contains(t, roles, "<PermissionsBoundaryArn>"+arn+"</PermissionsBoundaryArn>",
		"RoleDetail carries PermissionsBoundary; ListRoles does not")
}

// TestIAMAccountAuthorizationDetails_RoleLastUsedInsideInstanceProfile is the frozen-snapshot
// trap: `AddRoleToInstanceProfile` stores a copy of the role inside the profile, STS writes
// `RoleLastUsed` onto the role record, and AWS's sample renders the member inside
// `InstanceProfileList → Roles → member`.
//
// The role is assumed **after** the profile is created, so an implementation rendering the
// stored copy reports nothing inside the profile while `GetRole` reports a last use — which is
// precisely the divergence this operation exists to expose. A test that read the profile back
// immediately after attaching could not tell the two apart.
func TestIAMAccountAuthorizationDetails_RoleLastUsedInsideInstanceProfile(t *testing.T) {
	f := newRoleLastUsedFixture(t)

	iamAuthzOK(t, f.server, "CreateRole", map[string]string{
		"RoleName": "assumed-role", "AssumeRolePolicyDocument": iamAuthzTestTrustDoc,
	})
	iamAuthzOK(t, f.server, "CreateInstanceProfile", map[string]string{
		"InstanceProfileName": "assumed-profile",
	})
	iamAuthzOK(t, f.server, "AddRoleToInstanceProfile", map[string]string{
		"InstanceProfileName": "assumed-profile", "RoleName": "assumed-role",
	})

	before := iamAuthzDetailsXML(t, f.server, map[string]string{"Filter.member.1": "Role"})
	profilesBefore := iamXMLSection(t, before, "InstanceProfileList")
	require.Contains(t, profilesBefore, "<InstanceProfileName>assumed-profile</InstanceProfileName>",
		"the profile must be nested in the RoleDetail for the rest of this test to mean anything")
	assert.NotContains(t, profilesBefore, "RoleLastUsed",
		"a role that has not been assumed reports no last use anywhere")

	f.assumeRoleInRegion(t, "assumed-role", "eu-west-2")

	after := iamAuthzDetailsXML(t, f.server, map[string]string{"Filter.member.1": "Role"})
	roles := iamXMLSection(t, after, "RoleDetailList")
	const want = "<RoleLastUsed><LastUsedDate>2025-03-04T05:06:07Z</LastUsedDate>" +
		"<Region>eu-west-2</Region></RoleLastUsed>"
	profiles := iamXMLSection(t, roles, "InstanceProfileList")
	assert.Contains(t, profiles, want,
		"the embedded role must be re-read from state, not taken from the profile's frozen copy")
	assert.Contains(t, roles, want, "RoleDetail carries RoleLastUsed in its own right")

	// The nested member is a `Role`, not a `RoleDetail`, so the two members
	// TestIAMAccountAuthorizationDetails_MemberSets asserts absent from a RoleDetail are
	// *admissible* here. Asserted so the nested builder is not "corrected" into the detail's
	// member set by someone reading only that test.
	assert.Contains(t, profiles, "<MaxSessionDuration>3600</MaxSessionDuration>",
		"InstanceProfileList nests Role, which carries MaxSessionDuration")
}

// TestIAMAccountAuthorizationDetails_Filter asserts each of the five values selects only its own
// population, and that an unknown value is refused rather than silently ignored.
func TestIAMAccountAuthorizationDetails_Filter(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	iamAuthzOK(t, srv, "CreateUser", map[string]string{"UserName": "ada"})
	iamAuthzOK(t, srv, "CreateGroup", map[string]string{"GroupName": "engineers"})
	iamAuthzOK(t, srv, "CreateRole", map[string]string{
		"RoleName": "deploy", "AssumeRolePolicyDocument": iamAuthzTestTrustDoc,
	})
	iamAuthzOK(t, srv, "CreatePolicy", map[string]string{
		"PolicyName": "read-objects", "PolicyDocument": iamAuthzTestPolicyDoc,
	})

	for _, tc := range []struct {
		filter  string
		present string
		absent  []string
	}{
		{
			filter:  "User",
			present: "<UserName>ada</UserName>",
			absent:  []string{"engineers", "deploy", "read-objects", "AmazonS3ReadOnlyAccess"},
		},
		{
			filter:  "Group",
			present: "<GroupName>engineers</GroupName>",
			absent:  []string{"<UserName>ada</UserName>", "deploy", "read-objects"},
		},
		{
			filter:  "Role",
			present: "<RoleName>deploy</RoleName>",
			absent:  []string{"<UserName>ada</UserName>", "engineers", "read-objects"},
		},
		{
			filter:  "LocalManagedPolicy",
			present: "<PolicyName>read-objects</PolicyName>",
			absent:  []string{"<UserName>ada</UserName>", "engineers", "AmazonS3ReadOnlyAccess"},
		},
		{
			filter:  "AWSManagedPolicy",
			present: "<PolicyName>AmazonS3ReadOnlyAccess</PolicyName>",
			absent:  []string{"<UserName>ada</UserName>", "engineers", "read-objects"},
		},
	} {
		t.Run(tc.filter, func(t *testing.T) {
			t.Parallel()
			body := iamAuthzDetailsXML(t, srv, map[string]string{"Filter.member.1": tc.filter})
			assert.Contains(t, body, tc.present)
			for _, absent := range tc.absent {
				assert.NotContains(t, body, absent, "Filter=%s must not select it", tc.filter)
			}
			// Every list is emitted whether or not the filter selects it, so a consumer
			// decodes one shape rather than branching on which lists are present.
			for _, wrapper := range []string{"UserDetailList", "GroupDetailList", "RoleDetailList", "Policies"} {
				assert.Contains(t, body, "<"+wrapper+">", "%s must be emitted even when empty", wrapper)
			}
		})
	}

	t.Run("unknown value refused", func(t *testing.T) {
		t.Parallel()
		resp := iamFormRequest(t, srv, "GetAccountAuthorizationDetails",
			map[string]string{"Filter.member.1": "SAMLProviderList"})
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		var result map[string]any
		decodeIAMXML(t, resp, &result)
		assert.Equal(t, "ValidationError", result["__type"])
		assert.Contains(t, result["message"], "SAMLProviderList")
		assert.Contains(t, result["message"], "LocalManagedPolicy",
			"the refusal names the permitted set so a caller can fix the call from it alone")
	})
}

// TestIAMAccountAuthorizationDetails_MarkerRoundTrip asserts a truncated walk over the combined
// key space returns every item exactly once.
//
// The key space is combined across the four populations, so a `Marker` has to be unambiguous
// across them: a per-list cursor would either skip an entity or repeat one at every page
// boundary. Two per page over seven entities makes four pages, so the boundary is crossed
// three times rather than once.
func TestIAMAccountAuthorizationDetails_MarkerRoundTrip(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)

	want := make(map[string]bool)
	for _, name := range []string{"ada", "grace", "linus"} {
		iamAuthzOK(t, srv, "CreateUser", map[string]string{"UserName": name})
		want["arn:aws:iam::123456789012:user/"+name] = false
	}
	for _, name := range []string{"engineers", "readers"} {
		iamAuthzOK(t, srv, "CreateGroup", map[string]string{"GroupName": name})
		want["arn:aws:iam::123456789012:group/"+name] = false
	}
	for _, name := range []string{"deploy", "build"} {
		iamAuthzOK(t, srv, "CreateRole", map[string]string{
			"RoleName": name, "AssumeRolePolicyDocument": iamAuthzTestTrustDoc,
		})
		want["arn:aws:iam::123456789012:role/"+name] = false
	}

	params := map[string]string{
		"Filter.member.1": "User", "Filter.member.2": "Group", "Filter.member.3": "Role",
		"MaxItems": "2",
	}
	pages := 0
	for {
		pages++
		require.LessOrEqual(t, pages, 10, "the walk must terminate")
		body := iamAuthzDetailsXML(t, srv, params)

		var seen int
		for _, section := range []string{"UserDetailList", "GroupDetailList", "RoleDetailList"} {
			for _, arn := range iamXMLValues(iamXMLSection(t, body, section), "Arn") {
				reported, expected := want[arn]
				require.True(t, expected, "unexpected ARN %s on page %d", arn, pages)
				require.False(t, reported, "%s reported twice", arn)
				want[arn] = true
				seen++
			}
		}

		if !strings.Contains(body, "<IsTruncated>true</IsTruncated>") {
			assert.NotContains(t, body, "<Marker>",
				"a final page carries no Marker")
			break
		}
		require.Equal(t, 2, seen, "a truncated page holds exactly MaxItems items")
		params["Marker"] = iamXMLSection(t, body, "Marker")
	}

	assert.Equal(t, 4, pages, "seven entities two at a time")
	for arn, reported := range want {
		assert.True(t, reported, "%s was never reported", arn)
	}
}

// TestIAMAccountAuthorizationDetails_StateFailureIsNotAnEmptySnapshot asserts that a store
// failure anywhere in the assembly fails the request rather than reporting a smaller account.
//
// This operation is the worst place in IAM to swallow a read error, because its answer is the
// account: a caller diffing the snapshot against what it deployed would read a swallowed
// failure as "the entity is gone" and act on it. Every population and every nested list is
// armed separately, since each is its own scan.
func TestIAMAccountAuthorizationDetails_StateFailureIsNotAnEmptySnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arm  func(*iamFaultState)
	}{
		{
			name: "the user listing fails",
			arm:  func(s *iamFaultState) { s.listErrPrefix = emulator.IAMUserPrefixForTest(authzTestAccount) },
		},
		{
			name: "the group listing fails",
			arm:  func(s *iamFaultState) { s.listErrPrefix = emulator.IAMGroupPrefixForTest(authzTestAccount) },
		},
		{
			name: "the role listing fails",
			arm:  func(s *iamFaultState) { s.listErrPrefix = emulator.IAMRolePrefixForTest(authzTestAccount) },
		},
		{
			name: "the instance-profile listing fails",
			arm: func(s *iamFaultState) {
				s.listErrPrefix = emulator.IAMInstanceProfilePrefixForTest(authzTestAccount)
			},
		},
		{
			name: "the policy listing fails",
			arm:  func(s *iamFaultState) { s.listErrPrefix = emulator.IAMPolicyPrefixForTest(authzTestAccount) },
		},
		{
			name: "an entity record cannot be read",
			arm:  func(s *iamFaultState) { s.getErrKeySubstr = ":" + authzTestAccount + "/ada" },
		},
		{
			// The inline-policy names list and the document itself are two reads under two
			// key shapes, so a failure on either has to reach the caller.
			name: "an inline policy document cannot be read",
			arm: func(s *iamFaultState) {
				s.getErrKeySubstr = emulator.IAMInlinePolicyKeyForTest(
					authzTestAccount, "user", "ada", "inline-user")
			},
		},
		{
			name: "an attached-policy list cannot be read",
			arm: func(s *iamFaultState) {
				s.getErrKeySubstr = emulator.IAMAttachedPoliciesKeyForTest(authzTestAccount, "role", "deploy")
			},
		},
		{
			// The attachment counts and the boundary usage counts walk their own prefixes,
			// so a failure there is a separate arm from the policy listing.
			name: "the attachment counts fail",
			arm: func(s *iamFaultState) {
				s.listErrPrefix = emulator.IAMAttachedPoliciesPrefixForTest(authzTestAccount, "group")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fault := &iamFaultState{StateManager: emulator.NewMemoryStateManager()}
			srv := newIAMTestServerWithState(t, fault)

			// Armed only after the writes have landed: a store that fails from the first call
			// never gets an entity created to trip over.
			iamAuthzOK(t, srv, "CreateUser", map[string]string{"UserName": "ada"})
			iamAuthzOK(t, srv, "CreateGroup", map[string]string{"GroupName": "engineers"})
			iamAuthzOK(t, srv, "CreateRole", map[string]string{
				"RoleName": "deploy", "AssumeRolePolicyDocument": iamAuthzTestTrustDoc,
			})
			iamAuthzOK(t, srv, "CreateInstanceProfile", map[string]string{"InstanceProfileName": "profile"})
			iamAuthzOK(t, srv, "AddRoleToInstanceProfile", map[string]string{
				"InstanceProfileName": "profile", "RoleName": "deploy",
			})
			iamAuthzOK(t, srv, "CreatePolicy", map[string]string{
				"PolicyName": "read-objects", "PolicyDocument": iamAuthzTestPolicyDoc,
			})
			iamAuthzOK(t, srv, "PutUserPolicy", map[string]string{
				"UserName": "ada", "PolicyName": "inline-user", "PolicyDocument": iamAuthzTestPolicyDoc,
			})
			iamAuthzOK(t, srv, "AttachRolePolicy", map[string]string{
				"RoleName": "deploy", "PolicyArn": "arn:aws:iam::123456789012:policy/read-objects",
			})
			tt.arm(fault)

			resp := iamFormRequest(t, srv, "GetAccountAuthorizationDetails", nil)
			assert.Equal(t, http.StatusInternalServerError, resp.StatusCode,
				"a store failure must not be reported as a smaller account")
			require.NoError(t, resp.Body.Close())
		})
	}
}

// TestIAMAccountAuthorizationDetails_CorruptRecordIsSkipped asserts that one record that does
// not decode leaves the rest of the account readable.
//
// The opposite choice to the store failures above, and deliberately so: a failed *read* means
// "ask again", while a record that cannot be decoded will never decode, so failing the whole
// operation would make one bad key hide an entire account's authorization for good. Which
// entity is skipped is observable, so the caller is not told the account is complete.
func TestIAMAccountAuthorizationDetails_CorruptRecordIsSkipped(t *testing.T) {
	t.Parallel()
	fault := &iamFaultState{StateManager: emulator.NewMemoryStateManager()}
	srv := newIAMTestServerWithState(t, fault)

	iamAuthzOK(t, srv, "CreateUser", map[string]string{"UserName": "ada"})
	iamAuthzOK(t, srv, "CreateRole", map[string]string{
		"RoleName": "deploy", "AssumeRolePolicyDocument": iamAuthzTestTrustDoc,
	})
	iamAuthzOK(t, srv, "CreateInstanceProfile", map[string]string{"InstanceProfileName": "profile"})
	iamAuthzOK(t, srv, "AddRoleToInstanceProfile", map[string]string{
		"InstanceProfileName": "profile", "RoleName": "deploy",
	})
	fault.corruptKeySubstr = emulator.IAMInstanceProfileKeyForTest(authzTestAccount, "profile")

	body := iamAuthzDetailsXML(t, srv, nil)
	roles := iamXMLSection(t, body, "RoleDetailList")
	assert.Contains(t, roles, "<RoleName>deploy</RoleName>",
		"the role is still reported when the profile holding it does not decode")
	assert.Contains(t, roles, "<InstanceProfileList></InstanceProfileList>",
		"the undecodable profile is skipped rather than rendered half-built")
	assert.Contains(t, iamXMLSection(t, body, "UserDetailList"), "<UserName>ada</UserName>",
		"one bad key must not hide the rest of the account")
}

// TestIAMAccountAuthorizationDetails_MaxItemsRefused asserts an out-of-range `MaxItems` is
// refused here **and** at a pre-existing paginated operation that used to coerce it silently
// (#868).
//
// `ListUsers` is the second half of the assertion on purpose: the coercion lived in the shared
// paginator, so fixing only the new operation would leave eight siblings accepting a value IAM
// answers a 400 for.
func TestIAMAccountAuthorizationDetails_MaxItemsRefused(t *testing.T) {
	t.Parallel()
	srv := newIAMTestServer(t)
	iamAuthzOK(t, srv, "CreateUser", map[string]string{"UserName": "ada"})

	for _, operation := range []string{"GetAccountAuthorizationDetails", "ListUsers", "ListPolicies", "ListUserTags"} {
		for _, maxItems := range []string{"0", "-1", "1001"} {
			t.Run(operation+"/"+maxItems, func(t *testing.T) {
				t.Parallel()
				resp := iamFormRequest(t, srv, operation,
					map[string]string{"MaxItems": maxItems, "UserName": "ada"})
				require.Equal(t, http.StatusBadRequest, resp.StatusCode)
				var result map[string]any
				decodeIAMXML(t, resp, &result)
				assert.Equal(t, "ValidationError", result["__type"])
				assert.Contains(t, result["message"], "MaxItems must be between 1 and 1000")
			})
		}
		t.Run(operation+"/in range", func(t *testing.T) {
			t.Parallel()
			resp := iamFormRequest(t, srv, operation,
				map[string]string{"MaxItems": "1000", "UserName": "ada"})
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			require.NoError(t, resp.Body.Close())
		})
		t.Run(operation+"/absent", func(t *testing.T) {
			t.Parallel()
			resp := iamFormRequest(t, srv, operation, map[string]string{"UserName": "ada"})
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			require.NoError(t, resp.Body.Close())
		})
		t.Run(operation+"/present but empty", func(t *testing.T) {
			// Accepted by decision, not by omission: iamInt's own doc comment records that
			// such a caller expressed no limit and that AWS accepts the request.
			t.Parallel()
			resp := iamFormRequest(t, srv, operation,
				map[string]string{"MaxItems": "", "UserName": "ada"})
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			require.NoError(t, resp.Body.Close())
		})
	}

	// The simulate pair applies the same bounds and the same presence rule under a different
	// code, because those two operations publish InvalidInput in their own Errors sections
	// while the seventeen above can only draw a code from CommonErrors. An explicit
	// MaxItems=0 was taken as absent here until #868.
	for _, operation := range []string{"SimulatePrincipalPolicy", "SimulateCustomPolicy"} {
		for _, maxItems := range []string{"0", "1001"} {
			t.Run(operation+"/"+maxItems, func(t *testing.T) {
				t.Parallel()
				resp := iamFormRequest(t, srv, operation, map[string]string{
					"MaxItems":                 maxItems,
					"PolicySourceArn":          "arn:aws:iam::123456789012:user/ada",
					"PolicyInputList.member.1": iamAuthzTestPolicyDoc,
					"ActionNames.member.1":     "s3:GetObject",
				})
				require.Equal(t, http.StatusBadRequest, resp.StatusCode)
				var result map[string]any
				decodeIAMXML(t, resp, &result)
				assert.Equal(t, "InvalidInput", result["__type"],
					"the simulate operations publish InvalidInput, unlike the seventeen above")
				assert.Contains(t, result["message"], "MaxItems must be between 1 and 1000")
			})
		}
	}
}
