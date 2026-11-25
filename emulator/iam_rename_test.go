package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// iamMembers decodes the member values at path of an IAM response body.
func iamMembers(t *testing.T, body, path string) []string {
	t.Helper()
	return cfnIAMMembers(t, []byte(body), path)
}

// iamSignedCall sends an IAM form request signed (unverified) with accessKeyID.
func iamSignedCall(t *testing.T, srv *emulator.Server, accessKeyID, op string, params map[string]string) string {
	t.Helper()
	form := url.Values{"Action": {op}, "Version": {"2010-05-08"}}
	for k, v := range params {
		form.Set(k, v)
	}
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	r.Host = "iam.amazonaws.com"
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Set("Authorization", trustAuthHeader(accessKeyID, "iam"))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	body, err := io.ReadAll(w.Result().Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, w.Code, string(body))
	return string(body)
}

const renamePolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`

// renameFixture creates bob with everything a rename has to carry: an attached and an inline
// policy, two groups, an access key, a login profile and a tag.
func renameFixture(t *testing.T, srv *emulator.Server) (accessKeyID string) {
	t.Helper()
	idpOK(t, srv, "CreateUser", map[string]string{"UserName": "bob", "Path": "/eng/",
		"Tags.member.1.Key": "team", "Tags.member.1.Value": "core"})
	for _, g := range []string{"devs", "ops"} {
		idpOK(t, srv, "CreateGroup", map[string]string{"GroupName": g})
		idpOK(t, srv, "AddUserToGroup", map[string]string{"GroupName": g, "UserName": "bob"})
	}
	idpOK(t, srv, "AttachUserPolicy", map[string]string{"UserName": "bob", "PolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess"})
	idpOK(t, srv, "PutUserPolicy", map[string]string{"UserName": "bob", "PolicyName": "inline", "PolicyDocument": renamePolicy})
	idpOK(t, srv, "CreateLoginProfile", map[string]string{"UserName": "bob", "Password": "Passw0rd!", "PasswordResetRequired": "true"})
	key := iamMembers(t, idpOK(t, srv, "CreateAccessKey", map[string]string{"UserName": "bob"}), "AccessKey>AccessKeyId")
	require.Len(t, key, 1)
	return key[0]
}

func TestIAMUpdateUser_RenameMovesEverythingKeyedByTheName(t *testing.T) {
	srv, _, _ := newSTSTestServer(t)
	keyID := renameFixture(t, srv)

	idpOK(t, srv, "UpdateUser", map[string]string{"UserName": "bob", "NewUserName": "robert", "NewPath": "/ops/"})

	status, body := idpCall(t, srv, "GetUser", map[string]string{"UserName": "bob"})
	assert.Equal(t, http.StatusNotFound, status, body)
	user := idpOK(t, srv, "GetUser", map[string]string{"UserName": "robert"})
	assert.Equal(t, []string{"/ops/"}, iamMembers(t, user, "User>Path"))
	assert.Equal(t, []string{"arn:aws:iam::123456789012:user/ops/robert"}, iamMembers(t, user, "User>Arn"))
	assert.Equal(t, []string{"team"}, iamMembers(t, user, "User>Tags>member>Key"))

	assert.Equal(t, []string{"arn:aws:iam::aws:policy/ReadOnlyAccess"}, iamMembers(t,
		idpOK(t, srv, "ListAttachedUserPolicies", map[string]string{"UserName": "robert"}), "AttachedPolicies>member>PolicyArn"))
	assert.Equal(t, []string{"inline"}, iamMembers(t,
		idpOK(t, srv, "ListUserPolicies", map[string]string{"UserName": "robert"}), "PolicyNames>member"))
	idpOK(t, srv, "GetUserPolicy", map[string]string{"UserName": "robert", "PolicyName": "inline"})
	assert.Equal(t, []string{"devs", "ops"}, iamMembers(t,
		idpOK(t, srv, "ListGroupsForUser", map[string]string{"UserName": "robert"}), "Groups>member>GroupName"))
	for _, g := range []string{"devs", "ops"} {
		assert.Equal(t, []string{"robert"}, iamMembers(t,
			idpOK(t, srv, "GetGroup", map[string]string{"GroupName": g}), "Users>member>UserName"), "group %s", g)
	}
	assert.Equal(t, []string{keyID}, iamMembers(t,
		idpOK(t, srv, "ListAccessKeys", map[string]string{"UserName": "robert"}), "AccessKeyMetadata>member>AccessKeyId"))
	assert.Equal(t, []string{"robert"}, iamMembers(t,
		idpOK(t, srv, "GetLoginProfile", map[string]string{"UserName": "robert"}), "LoginProfile>UserName"))
	assert.Equal(t, []string{"robert"}, iamMembers(t,
		idpOK(t, srv, "ListEntitiesForPolicy", map[string]string{"PolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess"}),
		"PolicyUsers>member>UserName"))

	// The access key now resolves to the renamed user: GetUser with no UserName is the caller.
	caller := iamSignedCall(t, srv, keyID, "GetUser", nil)
	assert.Equal(t, []string{"robert"}, iamMembers(t, caller, "User>UserName"))

	// Nothing is left under the old name.
	_, body = idpCall(t, srv, "ListAttachedUserPolicies", map[string]string{"UserName": "bob"})
	assert.Empty(t, iamMembers(t, body, "AttachedPolicies>member>PolicyArn"))
	status, _ = idpCall(t, srv, "GetLoginProfile", map[string]string{"UserName": "bob"})
	assert.Equal(t, http.StatusNotFound, status)

	// A path-only update keeps the name.
	idpOK(t, srv, "UpdateUser", map[string]string{"UserName": "robert", "NewPath": "/"})
	assert.Equal(t, []string{"arn:aws:iam::123456789012:user/robert"}, iamMembers(t,
		idpOK(t, srv, "GetUser", map[string]string{"UserName": "robert"}), "User>Arn"))
	idpOK(t, srv, "UpdateUser", map[string]string{"UserName": "robert"})
}

func TestIAMUpdateUser_Refusals(t *testing.T) {
	srv := newIAMTestServer(t)
	idpOK(t, srv, "CreateUser", map[string]string{"UserName": "a"})
	idpOK(t, srv, "CreateUser", map[string]string{"UserName": "b"})
	idpOK(t, srv, "CreateGroup", map[string]string{"GroupName": "g1"})
	idpOK(t, srv, "CreateGroup", map[string]string{"GroupName": "g2"})
	cases := []struct {
		op     string
		params map[string]string
		status int
		code   string
	}{
		{"UpdateUser", map[string]string{"UserName": "a", "NewUserName": "b"}, http.StatusConflict, "EntityAlreadyExists"},
		{"UpdateUser", map[string]string{"UserName": "nobody", "NewUserName": "c"}, http.StatusNotFound, "NoSuchEntity"},
		{"UpdateUser", map[string]string{"UserName": "a", "NewPath": "no-slashes"}, http.StatusBadRequest, "ValidationError"},
		{"UpdateUser", map[string]string{}, http.StatusBadRequest, "ValidationError"},
		{"UpdateGroup", map[string]string{"GroupName": "g1", "NewGroupName": "g2"}, http.StatusConflict, "EntityAlreadyExists"},
		{"UpdateGroup", map[string]string{"GroupName": "nogroup", "NewGroupName": "g3"}, http.StatusNotFound, "NoSuchEntity"},
		{"UpdateGroup", map[string]string{"GroupName": "g1", "NewPath": "/bad path/"}, http.StatusBadRequest, "ValidationError"},
		{"UpdateGroup", map[string]string{}, http.StatusBadRequest, "ValidationError"},
	}
	for _, tc := range cases {
		t.Run(tc.op+" "+tc.code, func(t *testing.T) {
			status, body := idpCall(t, srv, tc.op, tc.params)
			assert.Equal(t, tc.status, status, body)
			assert.Equal(t, tc.code, idpErrorCode(t, body))
		})
	}
}

func TestIAMUpdateGroup_RenameMovesEverythingKeyedByTheName(t *testing.T) {
	srv := newIAMTestServer(t)
	idpOK(t, srv, "CreateGroup", map[string]string{"GroupName": "devs", "Path": "/eng/"})
	for _, u := range []string{"ann", "ben"} {
		idpOK(t, srv, "CreateUser", map[string]string{"UserName": u})
		idpOK(t, srv, "AddUserToGroup", map[string]string{"GroupName": "devs", "UserName": u})
	}
	idpOK(t, srv, "AttachGroupPolicy", map[string]string{"GroupName": "devs", "PolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess"})
	idpOK(t, srv, "PutGroupPolicy", map[string]string{"GroupName": "devs", "PolicyName": "inline", "PolicyDocument": renamePolicy})

	idpOK(t, srv, "UpdateGroup", map[string]string{"GroupName": "devs", "NewGroupName": "engineers", "NewPath": "/"})

	status, _ := idpCall(t, srv, "GetGroup", map[string]string{"GroupName": "devs"})
	assert.Equal(t, http.StatusNotFound, status)
	group := idpOK(t, srv, "GetGroup", map[string]string{"GroupName": "engineers"})
	assert.Equal(t, []string{"arn:aws:iam::123456789012:group/engineers"}, iamMembers(t, group, "Group>Arn"))
	assert.Equal(t, []string{"ann", "ben"}, iamMembers(t, group, "Users>member>UserName"))
	for _, u := range []string{"ann", "ben"} {
		assert.Equal(t, []string{"engineers"}, iamMembers(t,
			idpOK(t, srv, "ListGroupsForUser", map[string]string{"UserName": u}), "Groups>member>GroupName"))
	}
	assert.Equal(t, []string{"arn:aws:iam::aws:policy/ReadOnlyAccess"}, iamMembers(t,
		idpOK(t, srv, "ListAttachedGroupPolicies", map[string]string{"GroupName": "engineers"}), "AttachedPolicies>member>PolicyArn"))
	idpOK(t, srv, "GetGroupPolicy", map[string]string{"GroupName": "engineers", "PolicyName": "inline"})
	assert.Equal(t, []string{"engineers"}, iamMembers(t,
		idpOK(t, srv, "ListEntitiesForPolicy", map[string]string{"PolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess"}),
		"PolicyGroups>member>GroupName"))
	idpOK(t, srv, "UpdateGroup", map[string]string{"GroupName": "engineers", "NewPath": "/team/"})
	assert.Equal(t, []string{"arn:aws:iam::123456789012:group/team/engineers"}, iamMembers(t,
		idpOK(t, srv, "GetGroup", map[string]string{"GroupName": "engineers"}), "Group>Arn"))
}

// TestIAMUpdateUser_RenameIsAtomicAgainstConcurrentRequests runs renames alongside reads of both
// names: every read sees the user under exactly one of them, never neither and never a half-moved
// record. Run under -race.
func TestIAMUpdateUser_RenameIsAtomicAgainstConcurrentRequests(t *testing.T) {
	srv := newIAMTestServer(t)
	idpOK(t, srv, "CreateUser", map[string]string{"UserName": "u0"})
	idpOK(t, srv, "AttachUserPolicy", map[string]string{"UserName": "u0", "PolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess"})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			from, to := "u0", "u1"
			if i%2 == 1 {
				from, to = "u1", "u0"
			}
			resp := iamFormRequest(t, srv, "UpdateUser", map[string]string{"UserName": from, "NewUserName": to})
			_ = resp.Body.Close()
		}
	}()
	for i := 0; i < 40; i++ {
		body := idpOK(t, srv, "ListEntitiesForPolicy", map[string]string{"PolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess"})
		users := iamMembers(t, body, "PolicyUsers>member>UserName")
		assert.Len(t, users, 1, "the user is attached under exactly one name: %v", users)
	}
	wg.Wait()
}

func TestIAMLoginProfile_Lifecycle(t *testing.T) {
	srv := newIAMTestServer(t)
	idpOK(t, srv, "CreateUser", map[string]string{"UserName": "carol"})

	status, body := idpCall(t, srv, "GetLoginProfile", map[string]string{"UserName": "carol"})
	assert.Equal(t, http.StatusNotFound, status)
	assert.Contains(t, body, "Login Profile for User carol cannot be found.")

	created := idpOK(t, srv, "CreateLoginProfile", map[string]string{"UserName": "carol", "Password": "S3cret!pass"})
	assert.NotContains(t, created, "S3cret!pass")
	assert.Equal(t, []string{"false"}, iamMembers(t, created, "LoginProfile>PasswordResetRequired"))
	var lp struct {
		CreateDate string `xml:"GetLoginProfileResult>LoginProfile>CreateDate"`
		Reset      string `xml:"GetLoginProfileResult>LoginProfile>PasswordResetRequired"`
	}
	require.NoError(t, xml.Unmarshal([]byte(idpOK(t, srv, "GetLoginProfile", map[string]string{"UserName": "carol"})), &lp))
	assert.NotEmpty(t, lp.CreateDate)
	assert.Equal(t, "false", lp.Reset)

	status, body = idpCall(t, srv, "CreateLoginProfile", map[string]string{"UserName": "carol", "Password": "x"})
	assert.Equal(t, http.StatusConflict, status)
	assert.Equal(t, "EntityAlreadyExists", idpErrorCode(t, body))

	idpOK(t, srv, "UpdateLoginProfile", map[string]string{"UserName": "carol", "PasswordResetRequired": "true"})
	assert.Equal(t, []string{"true"}, iamMembers(t,
		idpOK(t, srv, "GetLoginProfile", map[string]string{"UserName": "carol"}), "LoginProfile>PasswordResetRequired"))
	idpOK(t, srv, "UpdateLoginProfile", map[string]string{"UserName": "carol", "Password": "Other!pass"})
	assert.Equal(t, []string{"true"}, iamMembers(t,
		idpOK(t, srv, "GetLoginProfile", map[string]string{"UserName": "carol"}), "LoginProfile>PasswordResetRequired"),
		"an update that omits PasswordResetRequired leaves it")

	// DeleteUser refuses a user with a password.
	status, body = idpCall(t, srv, "DeleteUser", map[string]string{"UserName": "carol"})
	assert.Equal(t, http.StatusConflict, status)
	assert.Equal(t, "DeleteConflict", idpErrorCode(t, body))
	assert.Contains(t, body, "Cannot delete entity, must delete login profile first.")

	idpOK(t, srv, "DeleteLoginProfile", map[string]string{"UserName": "carol"})
	status, body = idpCall(t, srv, "DeleteLoginProfile", map[string]string{"UserName": "carol"})
	assert.Equal(t, http.StatusNotFound, status)
	assert.Equal(t, "NoSuchEntity", idpErrorCode(t, body))
	idpOK(t, srv, "DeleteUser", map[string]string{"UserName": "carol"})

	for _, tc := range []struct {
		op     string
		params map[string]string
		status int
	}{
		{"CreateLoginProfile", map[string]string{"UserName": "ghost", "Password": "x"}, http.StatusNotFound},
		{"CreateLoginProfile", map[string]string{"UserName": "ghost"}, http.StatusBadRequest},
		{"CreateLoginProfile", map[string]string{"Password": "x"}, http.StatusBadRequest},
		{"GetLoginProfile", map[string]string{}, http.StatusBadRequest},
		{"UpdateLoginProfile", map[string]string{}, http.StatusBadRequest},
		{"UpdateLoginProfile", map[string]string{"UserName": "ghost"}, http.StatusNotFound},
		{"DeleteLoginProfile", map[string]string{}, http.StatusBadRequest},
	} {
		status, body := idpCall(t, srv, tc.op, tc.params)
		assert.Equal(t, tc.status, status, "%s %v: %s", tc.op, tc.params, body)
	}
}

func TestIAMListEntitiesForPolicy(t *testing.T) {
	srv := newIAMTestServer(t)
	policyARN := iamMembers(t, idpOK(t, srv, "CreatePolicy", map[string]string{"PolicyName": "shared",
		"PolicyDocument": renamePolicy}), "Policy>Arn")[0]
	idpOK(t, srv, "CreateUser", map[string]string{"UserName": "u1", "Path": "/eng/"})
	idpOK(t, srv, "CreateUser", map[string]string{"UserName": "u2"})
	idpOK(t, srv, "CreateUser", map[string]string{"UserName": "bounded"})
	idpOK(t, srv, "CreateGroup", map[string]string{"GroupName": "g1"})
	idpOK(t, srv, "CreateRole", map[string]string{"RoleName": "r1", "AssumeRolePolicyDocument": `{"Version":"2012-10-17","Statement":[]}`})
	for _, a := range []struct{ op, param, name string }{
		{"AttachUserPolicy", "UserName", "u1"}, {"AttachUserPolicy", "UserName", "u2"},
		{"AttachGroupPolicy", "GroupName", "g1"}, {"AttachRolePolicy", "RoleName", "r1"},
	} {
		idpOK(t, srv, a.op, map[string]string{a.param: a.name, "PolicyArn": policyARN})
	}
	idpOK(t, srv, "PutUserPermissionsBoundary", map[string]string{"UserName": "bounded", "PermissionsBoundary": policyARN})

	list := func(params map[string]string) string {
		params["PolicyArn"] = policyARN
		return idpOK(t, srv, "ListEntitiesForPolicy", params)
	}
	all := list(map[string]string{})
	assert.Equal(t, []string{"bounded", "u1", "u2"}, iamMembers(t, all, "PolicyUsers>member>UserName"))
	assert.Equal(t, []string{"g1"}, iamMembers(t, all, "PolicyGroups>member>GroupName"))
	assert.Equal(t, []string{"r1"}, iamMembers(t, all, "PolicyRoles>member>RoleName"))
	assert.Len(t, iamMembers(t, all, "PolicyRoles>member>RoleId"), 1)

	assert.Equal(t, []string{"u1", "u2"}, iamMembers(t, list(map[string]string{"PolicyUsageFilter": "PermissionsPolicy"}),
		"PolicyUsers>member>UserName"))
	boundary := list(map[string]string{"PolicyUsageFilter": "PermissionsBoundary"})
	assert.Equal(t, []string{"bounded"}, iamMembers(t, boundary, "PolicyUsers>member>UserName"))
	assert.Empty(t, iamMembers(t, boundary, "PolicyGroups>member>GroupName"))

	roles := list(map[string]string{"EntityFilter": "Role"})
	assert.Empty(t, iamMembers(t, roles, "PolicyUsers>member>UserName"))
	assert.Equal(t, []string{"r1"}, iamMembers(t, roles, "PolicyRoles>member>RoleName"))
	assert.Empty(t, iamMembers(t, list(map[string]string{"EntityFilter": "AWSManagedPolicy"}), "PolicyUsers>member>UserName"),
		"a customer policy is not an AWS managed one")
	assert.Len(t, iamMembers(t, list(map[string]string{"EntityFilter": "LocalManagedPolicy"}), "PolicyUsers>member>UserName"), 3)
	assert.Equal(t, []string{"u1"}, iamMembers(t, list(map[string]string{"PathPrefix": "/eng/"}), "PolicyUsers>member>UserName"))

	// One Marker resumes across the three kinds: groups, then roles, then users.
	var seen []string
	marker := ""
	for range 10 {
		params := map[string]string{"MaxItems": "2"}
		if marker != "" {
			params["Marker"] = marker
		}
		page := list(params)
		seen = append(seen, iamMembers(t, page, "PolicyGroups>member>GroupName")...)
		seen = append(seen, iamMembers(t, page, "PolicyRoles>member>RoleName")...)
		seen = append(seen, iamMembers(t, page, "PolicyUsers>member>UserName")...)
		next := iamMembers(t, page, "Marker")
		if len(next) == 0 {
			break
		}
		marker = next[0]
	}
	assert.Equal(t, []string{"g1", "r1", "bounded", "u1", "u2"}, seen)

	for _, tc := range []struct {
		params map[string]string
		status int
	}{
		{map[string]string{"PolicyArn": "arn:aws:iam::123456789012:policy/missing"}, http.StatusNotFound},
		{map[string]string{}, http.StatusBadRequest},
		{map[string]string{"PolicyArn": policyARN, "EntityFilter": "Robot"}, http.StatusBadRequest},
		{map[string]string{"PolicyArn": policyARN, "PolicyUsageFilter": "Sometimes"}, http.StatusBadRequest},
		{map[string]string{"PolicyArn": policyARN, "PathPrefix": "eng"}, http.StatusBadRequest},
		{map[string]string{"PolicyArn": policyARN, "MaxItems": "0"}, http.StatusBadRequest},
	} {
		status, body := idpCall(t, srv, "ListEntitiesForPolicy", tc.params)
		assert.Equal(t, tc.status, status, "%v: %s", tc.params, body)
	}
}

func TestIAMCredentialListings_AreEmptyForAUserThatExists(t *testing.T) {
	srv := newIAMTestServer(t)
	idpOK(t, srv, "CreateUser", map[string]string{"UserName": "dora"})
	for op, wrapper := range map[string]string{
		"ListMFADevices":                 "MFADevices",
		"ListSSHPublicKeys":              "SSHPublicKeys",
		"ListSigningCertificates":        "Certificates",
		"ListServiceSpecificCredentials": "ServiceSpecificCredentials",
	} {
		t.Run(op, func(t *testing.T) {
			body := idpOK(t, srv, op, map[string]string{"UserName": "dora"})
			assert.Contains(t, body, "<"+wrapper+"></"+wrapper+">")
			status, body := idpCall(t, srv, op, map[string]string{"UserName": "ghost"})
			assert.Equal(t, http.StatusNotFound, status, body)
			idpOK(t, srv, op, map[string]string{})
		})
	}
	assert.Contains(t, idpOK(t, srv, "ListVirtualMFADevices", map[string]string{"AssignmentStatus": "Any"}),
		"<VirtualMFADevices></VirtualMFADevices>")
	status, _ := idpCall(t, srv, "ListVirtualMFADevices", map[string]string{"AssignmentStatus": "Maybe"})
	assert.Equal(t, http.StatusBadRequest, status)
	status, _ = idpCall(t, srv, "ListMFADevices", map[string]string{"UserName": "dora", "MaxItems": "0"})
	assert.Equal(t, http.StatusBadRequest, status)
}
