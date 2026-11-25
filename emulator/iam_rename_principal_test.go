package emulator_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// What a request resolved just before a rename is decided as: the server holds IAM's entity lock
// across resolution and the authorization gate, IAM's own handlers and GetSessionToken find a
// renamed user by its unique ID, and a credential whose user has gone is refused rather than left
// unenforced.

// renameRaceSetup creates bob with an Allow on everything, the Deny named by deny (attached to bob,
// or to the group "blocked" bob belongs to when viaGroup is set), and an access key, and returns the
// key.
func renameRaceSetup(t *testing.T, ts *emulator.TestServer, viaGroup bool) string {
	t.Helper()
	const setup = "AKIAIOSFODNN7EXAMPLE"
	iam := func(op string, params url.Values) string {
		params.Set("Action", op)
		params.Set("Version", "2010-05-08")
		status, body := tsCall(t, ts, http.MethodPost, "/", "iam", setup, params, nil)
		require.Equal(t, http.StatusOK, status, "%s: %s", op, body)
		return body
	}
	const deny = `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"sqs:*","Resource":"*"}]}`
	iam("CreateUser", url.Values{"UserName": {"bob"}})
	iam("PutUserPolicy", url.Values{"UserName": {"bob"}, "PolicyName": {"all"},
		"PolicyDocument": {`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`}})
	if viaGroup {
		iam("CreateGroup", url.Values{"GroupName": {"blocked"}})
		iam("AddUserToGroup", url.Values{"GroupName": {"blocked"}, "UserName": {"bob"}})
		iam("PutGroupPolicy", url.Values{"GroupName": {"blocked"}, "PolicyName": {"deny"}, "PolicyDocument": {deny}})
	} else {
		iam("PutUserPolicy", url.Values{"UserName": {"bob"}, "PolicyName": {"deny"}, "PolicyDocument": {deny}})
	}
	return iamMembers(t, iam("CreateAccessKey", url.Values{"UserName": {"bob"}}), "AccessKey>AccessKeyId")[0]
}

func TestIAMRename_ADeniedCallerIsNeverAllowedDuringARename(t *testing.T) {
	for _, tc := range []struct {
		name     string
		viaGroup bool
		op       string
		names    [2]string
	}{
		{name: "user rename", op: "UpdateUser", names: [2]string{"bob", "robert"}},
		{name: "group rename", viaGroup: true, op: "UpdateGroup", names: [2]string{"blocked", "blocked2"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := emulator.StartTestServer(t)
			key := renameRaceSetup(t, ts, tc.viaGroup)
			param, newParam := "UserName", "NewUserName"
			if tc.viaGroup {
				param, newParam = "GroupName", "NewGroupName"
			}

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 30; i++ {
					from, to := tc.names[i%2], tc.names[(i+1)%2]
					_, _ = tsCall(t, ts, http.MethodPost, "/", "iam", "AKIAIOSFODNN7EXAMPLE",
						url.Values{"Action": {tc.op}, "Version": {"2010-05-08"}, param: {from}, newParam: {to}}, nil)
				}
			}()
			for i := 0; i < 60; i++ {
				status, body := tsCall(t, ts, http.MethodPost, "/", "sqs", key,
					url.Values{"Action": {"ListQueues"}, "Version": {"2012-11-05"}}, nil)
				assert.Equal(t, http.StatusForbidden, status, "the Deny holds on whichever side of the rename: %s", body)
			}
			wg.Wait()
		})
	}
}

func TestIAMRename_GetSessionTokenDuringARenameNamesTheRenamedUser(t *testing.T) {
	ts := emulator.StartTestServer(t)
	key := renameRaceSetup(t, ts, false)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		status, body := tsCall(t, ts, http.MethodPost, "/", "iam", "AKIAIOSFODNN7EXAMPLE",
			url.Values{"Action": {"UpdateUser"}, "Version": {"2010-05-08"}, "UserName": {"bob"}, "NewUserName": {"robert"}}, nil)
		assert.Equal(t, http.StatusOK, status, body)
	}()
	var sessions []string
	for i := 0; i < 20; i++ {
		status, body := tsCall(t, ts, http.MethodPost, "/", "sts", key,
			url.Values{"Action": {"GetSessionToken"}, "Version": {"2011-06-15"}}, nil)
		require.Equal(t, http.StatusOK, status, body)
		sessions = append(sessions, xmlField(body, "AccessKeyId"))
	}
	wg.Wait()
	for _, session := range sessions {
		status, body := tsCall(t, ts, http.MethodPost, "/", "sts", session,
			url.Values{"Action": {"GetCallerIdentity"}, "Version": {"2011-06-15"}}, nil)
		require.Equal(t, http.StatusOK, status, body)
		assert.Equal(t, "arn:aws:iam::123456789012:user/robert", xmlField(body, "Arn"),
			"a session minted on either side of the rename follows the user")
	}
}

func TestIAMTags_ConcurrentWritesThroughBothAPIsAreAllKept(t *testing.T) {
	ts := emulator.StartTestServer(t)
	const setup = "AKIAIOSFODNN7EXAMPLE"
	status, body := tsCall(t, ts, http.MethodPost, "/", "iam", setup,
		url.Values{"Action": {"CreateUser"}, "Version": {"2010-05-08"}, "UserName": {"tagged"}}, nil)
	require.Equal(t, http.StatusOK, status, body)

	const writes = 24
	var wg sync.WaitGroup
	for i := 0; i < writes; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("k%02d", i)
			if i%2 == 0 {
				status, body := tsCall(t, ts, http.MethodPost, "/", "iam", setup, url.Values{"Action": {"TagUser"},
					"Version": {"2010-05-08"}, "UserName": {"tagged"}, "Tags.member.1.Key": {key}, "Tags.member.1.Value": {"v"}}, nil)
				assert.Equal(t, http.StatusOK, status, body)
				return
			}
			r, err := http.NewRequestWithContext(context.Background(), http.MethodPost, ts.URL+"/",
				strings.NewReader(`{"ResourceARNList":["arn:aws:iam::123456789012:user/tagged"],"Tags":{"`+key+`":"v"}}`))
			if !assert.NoError(t, err) {
				return
			}
			r.Header.Set("X-Amz-Target", "ResourceGroupsTaggingAPI_20170126.TagResources")
			r.Header.Set("Content-Type", "application/x-amz-json-1.1")
			r.Header.Set("Authorization", trustAuthHeader(setup, "tagging"))
			resp, err := http.DefaultClient.Do(r)
			if assert.NoError(t, err) {
				assert.Equal(t, http.StatusOK, resp.StatusCode)
				assert.NoError(t, resp.Body.Close())
			}
		}(i)
	}
	wg.Wait()
	status, body = tsCall(t, ts, http.MethodPost, "/", "iam", setup,
		url.Values{"Action": {"ListUserTags"}, "Version": {"2010-05-08"}, "UserName": {"tagged"}}, nil)
	require.Equal(t, http.StatusOK, status, body)
	assert.Len(t, iamMembers(t, body, "Tags>member>Key"), writes, "no concurrent tag write is lost")
}

// renameDirect seeds bob through d and returns his unique ID.
func renameDirect(t *testing.T, d *idpDirect, ops ...struct {
	op   string
	body map[string]string
}) string {
	t.Helper()
	resp, err := d.call(t, d.iam, "CreateUser", map[string]string{"UserName": "bob"}, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(resp.Body))
	for _, c := range ops {
		resp, err := d.call(t, d.iam, c.op, c.body, nil)
		require.NoError(t, err, c.op)
		require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", c.op, resp.Body)
	}
	return xmlField(string(resp.Body), "UserId")
}

// callAs sends op to plugin as principal.
func (d *idpDirect) callAs(t *testing.T, plugin emulator.Plugin, principal *emulator.Principal, op string,
	body map[string]string) (*emulator.AWSResponse, error) {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	rc := d.ctx()
	rc.Principal = principal
	return plugin.HandleRequest(rc, &emulator.AWSRequest{Service: plugin.Name(), Operation: op, Body: raw,
		Params: map[string]string{}, Headers: map[string]string{}})
}

func TestIAMRename_ACallerResolvedBeforeARenameIsTheRenamedUser(t *testing.T) {
	d := newIDPDirect(t)
	id := renameDirect(t, d, struct {
		op   string
		body map[string]string
	}{"AttachUserPolicy", map[string]string{"UserName": "bob", "PolicyArn": "arn:aws:iam::aws:policy/IAMReadOnlyAccess"}})
	resp, err := d.call(t, d.iam, "TagUser", map[string]string{"UserName": "bob"},
		map[string]string{"Tags.member.1.Key": "team", "Tags.member.1.Value": "ci"})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(resp.Body))
	resp, err = d.call(t, d.iam, "UpdateUser", map[string]string{"UserName": "bob", "NewUserName": "robert"}, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(resp.Body))
	stale := emulator.IAMCredentialPrincipalForTest("arn:aws:iam::123456789012:user/bob", "bob", id)

	resp, err = d.callAs(t, d.iam, stale, "GetUser", map[string]string{})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "IAM finds the caller by its ID: %s", resp.Body)
	assert.Equal(t, "robert", xmlField(string(resp.Body), "UserName"), "the caller is the renamed user")

	sess, err := d.callAs(t, d.sts, stale, "GetSessionToken", map[string]string{})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, sess.StatusCode, string(sess.Body))
	raw, err := d.state.inner.Get(context.Background(), "sts", "session:"+xmlField(string(sess.Body), "AccessKeyId"))
	require.NoError(t, err)
	var record emulator.STSSessionCredentials
	require.NoError(t, json.Unmarshal(raw, &record))
	assert.Equal(t, "robert", record.UserName, "a session minted for the stale caller names the renamed user")
	assert.Equal(t, "arn:aws:iam::123456789012:user/robert", record.PrincipalARN)

	// The same caller with no ID, as a key written before #771 records it, cannot be followed.
	legacy := emulator.IAMCredentialPrincipalForTest("arn:aws:iam::123456789012:user/bob", "bob", "")
	resp, err = d.callAs(t, d.iam, legacy, "ListRoles", map[string]string{})
	require.NoError(t, err)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode, "a vanished caller is refused: %s", resp.Body)
	assert.Contains(t, string(resp.Body), "because no identity-based policy allows the iam:ListRoles action")
	_, err = d.callAs(t, d.sts, legacy, "GetSessionToken", map[string]string{})
	var awsErr *emulator.AWSError
	require.ErrorAs(t, err, &awsErr)
	assert.Equal(t, "AccessDenied", awsErr.Code)

	// A caller whose ID no user holds any longer has vanished too.
	ghost := emulator.IAMCredentialPrincipalForTest("arn:aws:iam::123456789012:user/bob", "bob", "AIDAGHOSTGHOSTGHOST1")
	resp, err = d.callAs(t, d.iam, ghost, "ListRoles", map[string]string{})
	require.NoError(t, err)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode, string(resp.Body))
}

func TestIAMRename_RefreshingTheCallerFailsOnAStoreFault(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		failGet, corrupt, failLst string
	}{
		{name: "get", failGet: "user:"},
		{name: "corrupt", corrupt: "user:"},
		{name: "list", failLst: "user:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := newIDPDirect(t)
			id := renameDirect(t, d)
			// A caller whose name no longer holds its ID makes the refresh scan the users.
			stale := emulator.IAMCredentialPrincipalForTest("arn:aws:iam::123456789012:user/gone", "gone", id)
			d.state.failGet, d.state.corruptGet, d.state.failList = tc.failGet, tc.corrupt, tc.failLst
			_, err := d.callAs(t, d.iam, stale, "ListRoles", map[string]string{})
			assert.Error(t, err)
			_, err = d.callAs(t, d.sts, stale, "GetSessionToken", map[string]string{})
			assert.Error(t, err)
		})
	}
}

func TestIAMRename_SessionsFollowTheUserByItsUniqueID(t *testing.T) {
	d := newIDPDirect(t)
	ctx := context.Background()
	oldID := renameDirect(t, d)
	sess, err := d.callAs(t, d.sts, emulator.IAMCredentialPrincipalForTest("arn:aws:iam::123456789012:user/bob", "bob", oldID),
		"GetSessionToken", map[string]string{})
	require.NoError(t, err)
	oldSession := "session:" + xmlField(string(sess.Body), "AccessKeyId")

	// The first bob goes; a second user takes the name.
	resp, err := d.call(t, d.iam, "DeleteUser", map[string]string{"UserName": "bob"}, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(resp.Body))
	resp, err = d.call(t, d.iam, "CreateUser", map[string]string{"UserName": "bob"}, nil)
	require.NoError(t, err)
	newID := xmlField(string(resp.Body), "UserId")
	require.NotEqual(t, oldID, newID)
	sess, err = d.callAs(t, d.sts, emulator.IAMCredentialPrincipalForTest("arn:aws:iam::123456789012:user/bob", "bob", newID),
		"GetSessionToken", map[string]string{})
	require.NoError(t, err)
	newSession := "session:" + xmlField(string(sess.Body), "AccessKeyId")
	// A session recorded before #771 carries no ID, and an assumed role's names no user.
	put := func(key string, s emulator.STSSessionCredentials) {
		raw, err := json.Marshal(s)
		require.NoError(t, err)
		require.NoError(t, d.state.inner.Put(ctx, "sts", key, raw))
	}
	put("session:ASIALEGACY", emulator.STSSessionCredentials{AccountID: "123456789012", UserName: "bob",
		PrincipalARN: "arn:aws:iam::123456789012:user/bob"})
	put("session:ASIAROLE", emulator.STSSessionCredentials{AccountID: "123456789012",
		PrincipalARN: "arn:aws:sts::123456789012:assumed-role/r/s"})

	resp, err = d.call(t, d.iam, "UpdateUser", map[string]string{"UserName": "bob", "NewUserName": "robert"}, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(resp.Body))

	userOf := func(key string) string {
		raw, err := d.state.inner.Get(ctx, "sts", key)
		require.NoError(t, err)
		var s emulator.STSSessionCredentials
		require.NoError(t, json.Unmarshal(raw, &s))
		return s.UserName
	}
	assert.Equal(t, "robert", userOf(newSession), "the renamed user's session follows it")
	assert.Equal(t, "bob", userOf(oldSession), "the deleted user's session, of another ID, stays where it was")
	assert.Equal(t, "robert", userOf("session:ASIALEGACY"), "a session with no ID is matched by name and ARN")
	assert.Empty(t, userOf("session:ASIAROLE"))
}

func TestIAMRename_SessionStoreFaultsLeaveTheUserWhole(t *testing.T) {
	for _, tc := range []struct {
		name                                string
		failGet, corrupt, put, failLst, get string
	}{
		{name: "list sessions", failLst: "session:"},
		{name: "get session", failGet: "session:"},
		{name: "corrupt session", corrupt: "session:"},
		{name: "put session", put: "session:"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := newIDPDirect(t)
			id := renameDirect(t, d)
			_, err := d.callAs(t, d.sts, emulator.IAMCredentialPrincipalForTest("arn:aws:iam::123456789012:user/bob", "bob", id),
				"GetSessionToken", map[string]string{})
			require.NoError(t, err)
			d.state.failGet, d.state.corruptGet, d.state.failPut, d.state.failList = tc.failGet, tc.corrupt, tc.put, tc.failLst
			_, err = d.call(t, d.iam, "UpdateUser", map[string]string{"UserName": "bob", "NewUserName": "robert"}, nil)
			require.Error(t, err)
			d.state.failGet, d.state.corruptGet, d.state.failPut, d.state.failList = "", "", "", ""
			resp, err := d.call(t, d.iam, "GetUser", map[string]string{"UserName": "bob"}, nil)
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, resp.StatusCode, "the rename was undone: %s", resp.Body)
			resp, err = d.call(t, d.iam, "GetUser", map[string]string{"UserName": "robert"}, nil)
			require.NoError(t, err)
			assert.Equal(t, http.StatusNotFound, resp.StatusCode)
		})
	}
}

func TestIAMRenameAndLoginProfile_RefuseADeniedCaller(t *testing.T) {
	d := newIDPDirect(t)
	id := renameDirect(t, d,
		struct {
			op   string
			body map[string]string
		}{"CreateGroup", map[string]string{"GroupName": "devs"}},
		struct {
			op   string
			body map[string]string
		}{"CreateUser", map[string]string{"UserName": "nobody"}},
	)
	_ = id
	resp, err := d.call(t, d.iam, "GetUser", map[string]string{"UserName": "nobody"}, nil)
	require.NoError(t, err)
	nobody := emulator.IAMCredentialPrincipalForTest("arn:aws:iam::123456789012:user/nobody", "nobody",
		xmlField(string(resp.Body), "UserId"))
	for _, c := range []struct {
		op   string
		body map[string]string
	}{
		{"UpdateUser", map[string]string{"UserName": "bob", "NewUserName": "robert"}},
		{"UpdateGroup", map[string]string{"GroupName": "devs", "NewGroupName": "eng"}},
		{"CreateLoginProfile", map[string]string{"UserName": "bob", "Password": "pw"}},
		{"GetLoginProfile", map[string]string{"UserName": "bob"}},
		{"UpdateLoginProfile", map[string]string{"UserName": "bob"}},
		{"DeleteLoginProfile", map[string]string{"UserName": "bob"}},
		{"ListMFADevices", map[string]string{"UserName": "bob"}},
		{"ListVirtualMFADevices", map[string]string{}},
		{"ListEntitiesForPolicy", map[string]string{"PolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess"}},
	} {
		resp, err := d.callAs(t, d.iam, nobody, c.op, c.body)
		require.NoError(t, err, c.op)
		assert.Equal(t, http.StatusForbidden, resp.StatusCode, "%s: %s", c.op, resp.Body)
	}

	// Allowed on the source alone, a rename is refused on the target.
	scoped := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["iam:UpdateUser","iam:UpdateGroup"],` +
		`"Resource":["arn:aws:iam::123456789012:user/bob","arn:aws:iam::123456789012:group/devs"]}]}`
	resp, err = d.call(t, d.iam, "PutUserPolicy", map[string]string{"UserName": "nobody", "PolicyName": "scoped",
		"PolicyDocument": scoped}, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(resp.Body))
	for _, c := range []map[string]string{
		{"UserName": "bob", "NewUserName": "robert"},
		{"GroupName": "devs", "NewGroupName": "eng"},
	} {
		op := "UpdateUser"
		if _, ok := c["GroupName"]; ok {
			op = "UpdateGroup"
		}
		resp, err := d.callAs(t, d.iam, nobody, op, c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusForbidden, resp.StatusCode, "%s needs the target too: %s", op, resp.Body)
	}
}

func TestIAMMergeRecordTags_RefusesWhatItCannotRead(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	for _, key := range []string{"user:123456789012/u", "role:123456789012/r", "instance_profile:123456789012/p"} {
		assert.Error(t, emulator.IAMMergeRecordTagsForTest(state, key, []byte("{not json"), map[string]string{"k": "v"}, nil), key)
	}
	assert.Error(t, emulator.IAMMergeRecordTagsForTest(state, "group:123456789012/g", []byte("{}"), nil, nil))
}
