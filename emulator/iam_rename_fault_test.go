package emulator_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestIAMRenameAndLoginProfile_StoreFaults drives each rename, login-profile and
// ListEntitiesForPolicy handler into a faulting store: a failure surfaces as an error, never as a
// published refusal.
func TestIAMRenameAndLoginProfile_StoreFaults(t *testing.T) {
	const policy = "arn:aws:iam::aws:policy/ReadOnlyAccess"
	seed := func(t *testing.T, d *idpDirect) {
		t.Helper()
		for _, c := range []struct {
			op     string
			body   map[string]string
			params map[string]string
		}{
			{"CreateUser", map[string]string{"UserName": "bob"}, nil},
			{"CreateGroup", map[string]string{"GroupName": "devs"}, nil},
			{"AddUserToGroup", map[string]string{"GroupName": "devs", "UserName": "bob"}, nil},
			{"AttachUserPolicy", map[string]string{"UserName": "bob", "PolicyArn": policy}, nil},
			{"AttachGroupPolicy", map[string]string{"GroupName": "devs", "PolicyArn": policy}, nil},
			{"PutUserPolicy", map[string]string{"UserName": "bob", "PolicyName": "p", "PolicyDocument": renamePolicy}, nil},
			{"PutGroupPolicy", map[string]string{"GroupName": "devs", "PolicyName": "p", "PolicyDocument": renamePolicy}, nil},
			{"CreateAccessKey", map[string]string{"UserName": "bob"}, nil},
			{"CreateLoginProfile", map[string]string{"UserName": "bob", "Password": "pw"}, nil},
		} {
			resp, err := d.call(t, d.iam, c.op, c.body, c.params)
			require.NoError(t, err, c.op)
			require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", c.op, resp.Body)
		}
	}
	renameUser := map[string]string{"UserName": "bob", "NewUserName": "robert"}
	renameGroup := map[string]string{"GroupName": "devs", "NewGroupName": "eng"}
	pathOnlyUser := map[string]string{"UserName": "bob", "NewPath": "/moved/"}
	pathOnlyGroup := map[string]string{"GroupName": "devs", "NewPath": "/moved/"}
	boundaryOnly := map[string]string{"PolicyArn": policy, "PolicyUsageFilter": "PermissionsBoundary"}
	cases := []struct {
		name                       string
		op                         string
		body                       map[string]string
		failGet, corrupt, put, del string
		list                       string
	}{
		{name: "rename user target lookup", op: "UpdateUser", body: renameUser, failGet: "user:123456789012/robert"},
		{name: "rename user group side get", op: "UpdateUser", body: renameUser, failGet: "group_users"},
		{name: "rename user inline doc get", op: "UpdateUser", body: renameUser, failGet: "user_inline:"},
		{name: "rename user key get", op: "UpdateUser", body: renameUser, failGet: "accesskey:"},
		{name: "rename user path only", op: "UpdateUser", body: pathOnlyUser, put: "user:"},
		{name: "rename group target lookup", op: "UpdateGroup", body: renameGroup, failGet: "group:123456789012/eng"},
		{name: "rename group path only", op: "UpdateGroup", body: pathOnlyGroup, put: "group:"},
		{name: "entities attachment scan", op: "ListEntitiesForPolicy", body: map[string]string{"PolicyArn": policy}, list: "user_policies"},
		{name: "entities boundary list", op: "ListEntitiesForPolicy", body: map[string]string{"PolicyArn": policy}, list: "user:"},
		{name: "entities boundary get", op: "ListEntitiesForPolicy", body: boundaryOnly, failGet: "user:"},
		{name: "entities boundary corrupt", op: "ListEntitiesForPolicy", body: boundaryOnly, corrupt: "user:"},
		{name: "login profile user", op: "CreateLoginProfile", body: map[string]string{"UserName": "bob", "Password": "x"}, failGet: "user:"},
		{name: "rename user get", op: "UpdateUser", body: renameUser, failGet: "user:"},
		{name: "rename user put", op: "UpdateUser", body: renameUser, put: "user:"},
		{name: "rename user attached", op: "UpdateUser", body: renameUser, put: "user_policies"},
		{name: "rename user inline names", op: "UpdateUser", body: renameUser, failGet: "user_inline_names"},
		{name: "rename user inline doc", op: "UpdateUser", body: renameUser, del: "user_inline:"},
		{name: "rename user key list", op: "UpdateUser", body: renameUser, failGet: "user_accesskeys"},
		{name: "rename user key record", op: "UpdateUser", body: renameUser, put: "accesskey:"},
		{name: "rename user key corrupt", op: "UpdateUser", body: renameUser, corrupt: "accesskey:"},
		{name: "rename user groups", op: "UpdateUser", body: renameUser, failGet: "user_groups"},
		{name: "rename user group side", op: "UpdateUser", body: renameUser, put: "group_users"},
		{name: "rename user login profile", op: "UpdateUser", body: renameUser, failGet: "login_profile"},
		{name: "rename user login put", op: "UpdateUser", body: renameUser, put: "login_profile"},
		{name: "rename user delete", op: "UpdateUser", body: renameUser, del: "user:"},
		{name: "rename group get", op: "UpdateGroup", body: renameGroup, failGet: "group:"},
		{name: "rename group put", op: "UpdateGroup", body: renameGroup, put: "group:"},
		{name: "rename group policies", op: "UpdateGroup", body: renameGroup, put: "group_policies"},
		{name: "rename group inline", op: "UpdateGroup", body: renameGroup, failGet: "group_inline_names"},
		{name: "rename group members", op: "UpdateGroup", body: renameGroup, failGet: "group_users"},
		{name: "rename group member side", op: "UpdateGroup", body: renameGroup, put: "user_groups"},
		{name: "rename group delete", op: "UpdateGroup", body: renameGroup, del: "group:"},
		{name: "create login profile", op: "CreateLoginProfile", body: map[string]string{"UserName": "bob", "Password": "x"}, failGet: "login_profile"},
		{name: "get login profile corrupt", op: "GetLoginProfile", body: map[string]string{"UserName": "bob"}, corrupt: "login_profile"},
		{name: "update login profile", op: "UpdateLoginProfile", body: map[string]string{"UserName": "bob", "PasswordResetRequired": "true"}, put: "login_profile"},
		{name: "update login profile get", op: "UpdateLoginProfile", body: map[string]string{"UserName": "bob"}, failGet: "login_profile"},
		{name: "delete login profile", op: "DeleteLoginProfile", body: map[string]string{"UserName": "bob"}, del: "login_profile"},
		{name: "delete login profile get", op: "DeleteLoginProfile", body: map[string]string{"UserName": "bob"}, failGet: "login_profile"},
		{name: "delete user profile check", op: "DeleteUser", body: map[string]string{"UserName": "bob"}, failGet: "login_profile"},
		{name: "entities attached", op: "ListEntitiesForPolicy", body: map[string]string{"PolicyArn": policy}, failGet: "group:"},
		{name: "entities corrupt", op: "ListEntitiesForPolicy", body: map[string]string{"PolicyArn": policy}, corrupt: "user:"},
		{name: "credential listing", op: "ListMFADevices", body: map[string]string{"UserName": "bob"}, failGet: "user:"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := newIDPDirect(t)
			seed(t, d)
			if tc.op == "DeleteUser" {
				// Clear what DeleteUser checks before the login profile.
				for _, c := range []struct {
					op   string
					body map[string]string
				}{
					{"DetachUserPolicy", map[string]string{"UserName": "bob", "PolicyArn": policy}},
					{"RemoveUserFromGroup", map[string]string{"GroupName": "devs", "UserName": "bob"}},
				} {
					resp, err := d.call(t, d.iam, c.op, c.body, nil)
					require.NoError(t, err)
					require.Equal(t, http.StatusOK, resp.StatusCode, string(resp.Body))
				}
			}
			d.state.failGet, d.state.corruptGet, d.state.failPut, d.state.failDelete = tc.failGet, tc.corrupt, tc.put, tc.del
			d.state.failList = tc.list
			_, err := d.call(t, d.iam, tc.op, tc.body, nil)
			assert.Error(t, err)
		})
	}
}

// TestIAMDeleteUserAndLoginProfile_StoreFaults reaches the checks DeleteUser makes after the login
// profile, and CreateLoginProfile's write, each with nothing ahead of it to refuse first.
func TestIAMDeleteUserAndLoginProfile_StoreFaults(t *testing.T) {
	for _, tc := range []struct {
		name, op, failGet, put string
		inline                 bool
	}{
		{name: "access key list", op: "DeleteUser", failGet: "user_accesskeys"},
		{name: "inline policy names", op: "DeleteUser", failGet: "user_inline_names"},
		{name: "create login profile write", op: "CreateLoginProfile", put: "login_profile"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := newIDPDirect(t)
			resp, err := d.call(t, d.iam, "CreateUser", map[string]string{"UserName": "bob"}, nil)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode, string(resp.Body))
			d.state.failGet, d.state.failPut = tc.failGet, tc.put
			_, err = d.call(t, d.iam, tc.op, map[string]string{"UserName": "bob", "Password": "pw"}, nil)
			assert.Error(t, err)
		})
	}
}

func TestIAMCredentialListings_Refusals(t *testing.T) {
	d := newIDPDirect(t)
	for _, tc := range []struct {
		op   string
		body string
	}{
		{"ListVirtualMFADevices", `{"AssignmentStatus":5}`},
		{"ListSSHPublicKeys", `{"UserName":5}`},
		{"UpdateUser", `{"UserName":5}`},
		{"UpdateGroup", `{"GroupName":5}`},
		{"CreateLoginProfile", `{"UserName":5}`},
		{"GetLoginProfile", `{"UserName":5}`},
		{"UpdateLoginProfile", `{"UserName":5}`},
		{"DeleteLoginProfile", `{"UserName":5}`},
		{"ListEntitiesForPolicy", `{"PolicyArn":5}`},
		{"ListVirtualMFADevices", `{"Marker":"!not-a-marker"}`},
	} {
		resp, err := d.iam.HandleRequest(d.ctx(), &emulator.AWSRequest{Service: "iam", Operation: tc.op,
			Body: []byte(tc.body), Params: map[string]string{}, Headers: map[string]string{}})
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "%s %s: %s", tc.op, tc.body, resp.Body)
	}
}
