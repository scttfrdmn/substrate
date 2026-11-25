package emulator_test

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// What a rename looks like from outside IAM's own lock: a signed request, the tagging API, an STS
// session, a failure part way through, and the DeleteUser that guards the names.

func TestIAMRename_ASignedRequestDuringRenamesIsNeverSpuriouslyRefused(t *testing.T) {
	ts := emulator.StartTestServer(t)
	const setup = "AKIAIOSFODNN7EXAMPLE"
	iam := func(op string, params url.Values) string {
		params.Set("Action", op)
		params.Set("Version", "2010-05-08")
		status, body := tsCall(t, ts, http.MethodPost, "/", "iam", setup, params, nil)
		require.Equal(t, http.StatusOK, status, "%s: %s", op, body)
		return body
	}
	iam("CreateUser", url.Values{"UserName": {"u0"}})
	iam("AttachUserPolicy", url.Values{"UserName": {"u0"}, "PolicyArn": {"arn:aws:iam::aws:policy/IAMReadOnlyAccess"}})
	key := iamMembers(t, iam("CreateAccessKey", url.Values{"UserName": {"u0"}}), "AccessKey>AccessKeyId")[0]

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			from, to := "u0", "u1"
			if i%2 == 1 {
				from, to = "u1", "u0"
			}
			_, _ = tsCall(t, ts, http.MethodPost, "/", "iam", setup,
				url.Values{"Action": {"UpdateUser"}, "Version": {"2010-05-08"}, "UserName": {from}, "NewUserName": {to}}, nil)
		}
	}()
	for i := 0; i < 40; i++ {
		status, body := tsCall(t, ts, http.MethodPost, "/", "iam", key,
			url.Values{"Action": {"ListRoles"}, "Version": {"2010-05-08"}}, nil)
		assert.Equal(t, http.StatusOK, status, "the key's user is allowed under whichever name it has: %s", body)
	}
	wg.Wait()
}

func TestIAMRename_ATagWriteDuringARenameLeavesNoGhost(t *testing.T) {
	ts := emulator.StartTestServer(t)
	const setup = "AKIAIOSFODNN7EXAMPLE"
	status, body := tsCall(t, ts, http.MethodPost, "/", "iam", setup,
		url.Values{"Action": {"CreateUser"}, "Version": {"2010-05-08"}, "UserName": {"u0"}}, nil)
	require.Equal(t, http.StatusOK, status, body)

	tag := func(name string) {
		r, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/",
			strings.NewReader(`{"ResourceARNList":["arn:aws:iam::123456789012:user/`+name+`"],"Tags":{"k":"v"}}`))
		require.NoError(t, err)
		r.Header.Set("X-Amz-Target", "ResourceGroupsTaggingAPI_20170126.TagResources")
		r.Header.Set("Content-Type", "application/x-amz-json-1.1")
		r.Header.Set("Authorization", trustAuthHeader(setup, "tagging"))
		resp, err := http.DefaultClient.Do(r)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			from, to := "u0", "u1"
			if i%2 == 1 {
				from, to = "u1", "u0"
			}
			_, _ = tsCall(t, ts, http.MethodPost, "/", "iam", setup,
				url.Values{"Action": {"UpdateUser"}, "Version": {"2010-05-08"}, "UserName": {from}, "NewUserName": {to}}, nil)
		}
	}()
	for i := 0; i < 20; i++ {
		tag("u0")
		tag("u1")
	}
	wg.Wait()

	_, body = tsCall(t, ts, http.MethodPost, "/", "iam", setup, url.Values{"Action": {"ListUsers"}, "Version": {"2010-05-08"}}, nil)
	assert.Len(t, iamMembers(t, body, "Users>member>UserName"), 1, "one user, never a ghost under the other name: %s", body)
}

func TestIAMRename_AGetSessionTokenSessionFollowsTheUser(t *testing.T) {
	srv := newTrustPolicyTestServer(t)
	idpOK(t, srv, "CreateUser", map[string]string{"UserName": "bob"})
	idpOK(t, srv, "AttachUserPolicy", map[string]string{"UserName": "bob", "PolicyArn": "arn:aws:iam::aws:policy/IAMReadOnlyAccess"})
	userKey := iamMembers(t, idpOK(t, srv, "CreateAccessKey", map[string]string{"UserName": "bob"}), "AccessKey>AccessKeyId")[0]
	session := fedSessionKey(t, stsSignedCall(t, srv, userKey, "GetSessionToken"))

	idpOK(t, srv, "UpdateUser", map[string]string{"UserName": "bob", "NewUserName": "robert"})

	status, body := fedIAMAs(t, srv, session, "ListRoles", nil)
	assert.Equal(t, http.StatusOK, status, "the session keeps the renamed user's permissions: %s", body)
	assert.Contains(t, stsSignedCall(t, srv, session, "GetCallerIdentity"), "<Arn>arn:aws:iam::123456789012:user/robert</Arn>")
}

func TestIAMRename_AFailurePartWayLeavesTheUserWhollyUnderItsOldName(t *testing.T) {
	const policy = "arn:aws:iam::aws:policy/ReadOnlyAccess"
	for _, fault := range []struct{ name, put, del string }{
		{name: "the new record", put: "user:123456789012/robert"},
		{name: "an access key", put: "accesskey:"},
		{name: "the group side", put: "group_users"},
		{name: "deleting the old record", del: "user:123456789012/bob"},
		{name: "deleting an old dependent", del: "user_policies:123456789012/bob"},
	} {
		t.Run(fault.name, func(t *testing.T) {
			d := newIDPDirect(t)
			for _, c := range []struct {
				op   string
				body map[string]string
			}{
				{"CreateUser", map[string]string{"UserName": "bob"}},
				{"CreateGroup", map[string]string{"GroupName": "devs"}},
				{"AddUserToGroup", map[string]string{"GroupName": "devs", "UserName": "bob"}},
				{"AttachUserPolicy", map[string]string{"UserName": "bob", "PolicyArn": policy}},
				{"PutUserPolicy", map[string]string{"UserName": "bob", "PolicyName": "p", "PolicyDocument": renamePolicy}},
				{"CreateAccessKey", map[string]string{"UserName": "bob"}},
				{"CreateLoginProfile", map[string]string{"UserName": "bob", "Password": "pw"}},
			} {
				resp, err := d.call(t, d.iam, c.op, c.body, nil)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", c.op, resp.Body)
			}
			before := snapshotIAM(t, d.state.inner)

			d.state.failPut, d.state.failDelete = fault.put, fault.del
			_, err := d.call(t, d.iam, "UpdateUser", map[string]string{"UserName": "bob", "NewUserName": "robert"}, nil)
			require.Error(t, err)
			d.state.failPut, d.state.failDelete = "", ""

			assert.Equal(t, before, snapshotIAM(t, d.state.inner), "the rename is undone completely")
			resp, err := d.call(t, d.iam, "UpdateUser", map[string]string{"UserName": "bob", "NewUserName": "robert"}, nil)
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, resp.StatusCode, "the rename can be retried: %s", resp.Body)
		})
	}
}

// snapshotIAM returns every IAM and STS record, by key.
func snapshotIAM(t *testing.T, state emulator.StateManager) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, ns := range []string{"iam", "sts"} {
		keys, err := state.List(context.Background(), ns, "")
		require.NoError(t, err)
		for _, k := range keys {
			raw, err := state.Get(context.Background(), ns, k)
			require.NoError(t, err)
			out[ns+"/"+k] = string(raw)
		}
	}
	return out
}

func TestIAMDeleteUser_RefusesWhileDependentsRemain(t *testing.T) {
	srv := newIAMTestServer(t)
	idpOK(t, srv, "CreateUser", map[string]string{"UserName": "dee"})
	key := iamMembers(t, idpOK(t, srv, "CreateAccessKey", map[string]string{"UserName": "dee"}), "AccessKey>AccessKeyId")[0]
	idpOK(t, srv, "PutUserPolicy", map[string]string{"UserName": "dee", "PolicyName": "p", "PolicyDocument": renamePolicy})

	status, body := idpCall(t, srv, "DeleteUser", map[string]string{"UserName": "dee"})
	assert.Equal(t, http.StatusConflict, status)
	assert.Contains(t, body, "Cannot delete entity, must delete access keys first.")
	idpOK(t, srv, "DeleteAccessKey", map[string]string{"UserName": "dee", "AccessKeyId": key})

	status, body = idpCall(t, srv, "DeleteUser", map[string]string{"UserName": "dee"})
	assert.Equal(t, http.StatusConflict, status)
	assert.Contains(t, body, "Cannot delete entity, must delete policies first.")
	idpOK(t, srv, "DeleteUserPolicy", map[string]string{"UserName": "dee", "PolicyName": "p"})

	idpOK(t, srv, "DeleteUser", map[string]string{"UserName": "dee"})
	// A user created under the name inherits nothing.
	idpOK(t, srv, "CreateUser", map[string]string{"UserName": "dee"})
	assert.Empty(t, iamMembers(t, idpOK(t, srv, "ListUserPolicies", map[string]string{"UserName": "dee"}), "PolicyNames>member"))
}

func TestCFN_IAMPathChangeRefreshesGetAttArn(t *testing.T) {
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	tmpl := func(path string) string {
		return `{"Resources":{"U":{"Type":"AWS::IAM::User","Properties":{"UserName":"arned","Path":"` + path + `"}},
			"G":{"Type":"AWS::IAM::Group","Properties":{"GroupName":"arned-g","Path":"` + path + `"}}},
			"Outputs":{"U":{"Value":{"Fn::GetAtt":["U","Arn"]}},"G":{"Value":{"Fn::GetAtt":["G","Arn"]}}}}`
	}
	_, err := d.Deploy(ctx, tmpl("/a/"), "arned", nil)
	require.NoError(t, err)
	updated, err := d.UpdateStack(ctx, tmpl("/b/"), "arned", nil, emulator.CFNDeployOptions{})
	require.NoError(t, err)
	require.Equal(t, "UPDATE_COMPLETE", updated.Status, updated.StatusReason)
	assert.Equal(t, "arn:aws:iam::123456789012:user/b/arned", updated.Outputs["U"])
	assert.Equal(t, "arn:aws:iam::123456789012:group/b/arned-g", updated.Outputs["G"])
}
