package emulator_test

import (
	"context"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Where a session's restrictions reach: the secondary authorization passes, a CloudFormation
// stack's later operations, replay, role chaining, and the paths where identity policies cannot be
// read.

// tsCall sends a request to a TestServer signed (unverified) with accessKeyID for service.
func tsCall(t *testing.T, ts *emulator.TestServer, method, path, service, accessKeyID string,
	form url.Values, headers map[string]string) (int, string) {
	t.Helper()
	var body io.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	}
	r, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, body)
	require.NoError(t, err)
	if form != nil {
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	r.Header.Set("Authorization", trustAuthHeader(accessKeyID, service))
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(r)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(out)
}

// tsAdminSession creates an AdministratorAccess role and assumes it with the session policy in
// extra, returning the session's access key ID.
func tsAdminSession(t *testing.T, ts *emulator.TestServer, extra url.Values) string {
	t.Helper()
	return tsAdminSessionNamed(t, ts, "admin", extra)
}

// tsAdminSessionNamed is tsAdminSession for a role of the given name.
func tsAdminSessionNamed(t *testing.T, ts *emulator.TestServer, role string, extra url.Values) string {
	t.Helper()
	const setup = "AKIAIOSFODNN7EXAMPLE"
	iam := func(op string, params url.Values) {
		params.Set("Action", op)
		params.Set("Version", "2010-05-08")
		status, body := tsCall(t, ts, http.MethodPost, "/", "iam", setup, params, nil)
		require.Equal(t, http.StatusOK, status, "%s: %s", op, body)
	}
	iam("CreateRole", url.Values{"RoleName": {role}, "AssumeRolePolicyDocument": {`{"Version":"2012-10-17","Statement":[]}`}})
	iam("AttachRolePolicy", url.Values{"RoleName": {role}, "PolicyArn": {"arn:aws:iam::aws:policy/AdministratorAccess"}})
	form := url.Values{"Action": {"AssumeRole"}, "Version": {"2011-06-15"},
		"RoleArn": {"arn:aws:iam::123456789012:role/" + role}, "RoleSessionName": {"s1"}}
	for k, v := range extra {
		form[k] = v
	}
	status, body := tsCall(t, ts, http.MethodPost, "/", "sts", setup, form, nil)
	require.Equal(t, http.StatusOK, status, body)
	return fedSessionKey(t, body)
}

func TestSessionPolicy_NarrowsTheSecondaryPasses(t *testing.T) {
	ts := emulator.StartTestServer(t)
	const setup = "AKIAIOSFODNN7EXAMPLE"
	status, body := tsCall(t, ts, http.MethodPut, "/scope-bucket", "s3", setup, nil, nil)
	require.Equal(t, http.StatusOK, status, body)
	status, body = tsCall(t, ts, http.MethodPut, "/scope-bucket/src", "s3", setup, url.Values{"x": {"y"}}, nil)
	require.Equal(t, http.StatusOK, status, body)

	key := tsAdminSession(t, ts, url.Values{"Policy": {`{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":["s3:PutObject","ec2:RunInstances"],"Resource":"*"}]}`}})

	t.Run("CopyObject reads its source under the session policy", func(t *testing.T) {
		status, body := tsCall(t, ts, http.MethodPut, "/scope-bucket/plain", "s3", key, url.Values{"x": {"y"}}, nil)
		require.Equal(t, http.StatusOK, status, "the session may put: %s", body)
		status, body = tsCall(t, ts, http.MethodPut, "/scope-bucket/copy", "s3", key, nil,
			map[string]string{"x-amz-copy-source": "/scope-bucket/src"})
		assert.Equal(t, http.StatusForbidden, status, "the copy reads a source the session policy does not allow: %s", body)
		assert.Contains(t, body, "blocked by session policy")
	})

	t.Run("a tagged RunInstances needs ec2:CreateTags under the session policy", func(t *testing.T) {
		ts.SeedEC2Image("ami-0123456789abcdef0", "scope")
		run := func(tagged bool) (int, string) {
			form := url.Values{"Action": {"RunInstances"}, "Version": {"2016-11-15"}, "ImageId": {"ami-0123456789abcdef0"},
				"MinCount": {"1"}, "MaxCount": {"1"}, "InstanceType": {"t3.micro"}}
			if tagged {
				form.Set("TagSpecification.1.ResourceType", "instance")
				form.Set("TagSpecification.1.Tag.1.Key", "team")
				form.Set("TagSpecification.1.Tag.1.Value", "ci")
			}
			return tsCall(t, ts, http.MethodPost, "/", "ec2", key, form, nil)
		}
		status, body := run(false)
		require.Equal(t, http.StatusOK, status, body)
		status, body = run(true)
		assert.Equal(t, http.StatusForbidden, status, body)
		assert.Contains(t, body, "blocked by session policy")
	})
}

func TestSessionPolicy_AppliesWhenTheRoleIsGone(t *testing.T) {
	ts := emulator.StartTestServer(t)
	key := tsAdminSession(t, ts, url.Values{"Policy": {`{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":"iam:GetRole","Resource":"*"}]}`}})
	const setup = "AKIAIOSFODNN7EXAMPLE"
	for _, op := range []string{"DetachRolePolicy", "DeleteRole"} {
		form := url.Values{"Action": {op}, "Version": {"2010-05-08"}, "RoleName": {"admin"},
			"PolicyArn": {"arn:aws:iam::aws:policy/AdministratorAccess"}}
		status, body := tsCall(t, ts, http.MethodPost, "/", "iam", setup, form, nil)
		require.Equal(t, http.StatusOK, status, "%s: %s", op, body)
	}
	// The role no longer exists, so its identity policies cannot be read and are not enforced —
	// but the session's own policy still narrows it.
	status, body := tsCall(t, ts, http.MethodPost, "/", "iam", key,
		url.Values{"Action": {"ListRoles"}, "Version": {"2010-05-08"}}, nil)
	assert.Equal(t, http.StatusForbidden, status, body)
	assert.Contains(t, body, "blocked by session policy")
}

func TestSessionPolicy_UnbundledAWSManagedPolicyFailsSafe(t *testing.T) {
	ts := emulator.StartTestServer(t)
	listRoles := url.Values{"Action": {"ListRoles"}, "Version": {"2010-05-08"}}

	// Alone, a managed policy substrate cannot read leaves the session nothing.
	alone := tsAdminSession(t, ts, url.Values{"PolicyArns.member.1.arn": {"arn:aws:iam::aws:policy/NotInSubstratesCatalog"}})
	status, body := tsCall(t, ts, http.MethodPost, "/", "iam", alone, listRoles, nil)
	assert.Equal(t, http.StatusForbidden, status, "an unreadable session policy allows nothing: %s", body)

	// Beside a resolvable one, the session keeps what the resolvable one allows.
	paired := tsAdminSessionNamed(t, ts, "admin2", url.Values{
		"PolicyArns.member.1.arn": {"arn:aws:iam::aws:policy/NotInSubstratesCatalog"},
		"PolicyArns.member.2.arn": {"arn:aws:iam::aws:policy/IAMReadOnlyAccess"}})
	status, body = tsCall(t, ts, http.MethodPost, "/", "iam", paired, listRoles, nil)
	assert.Equal(t, http.StatusOK, status, body)
	status, _ = tsCall(t, ts, http.MethodPost, "/", "iam", paired,
		url.Values{"Action": {"CreateUser"}, "Version": {"2010-05-08"}, "UserName": {"nope"}}, nil)
	assert.Equal(t, http.StatusForbidden, status)
}

func TestSessionPolicy_PersistsOnACloudFormationCreator(t *testing.T) {
	doc := emulator.PolicyDocument{Version: "2012-10-17", Statement: []emulator.PolicyStatement{{
		Effect: "Allow", Action: emulator.StringOrSlice{"sqs:*"}, Resource: emulator.StringOrSlice{"*"}}}}
	creator, err := emulator.CFNStackCreatorRoundTripForTest(&emulator.Principal{
		ARN: "arn:aws:sts::123456789012:assumed-role/admin/s1", UserName: "",
		SessionPolicies: []emulator.PolicyDocument{doc}, SessionTags: map[string]string{"team": "ci"},
		TransitiveTagKeys: []string{"team"}, SourceIdentity: "octocat",
	})
	require.NoError(t, err)
	require.NotNil(t, creator)
	assert.Len(t, creator.SessionPolicies, 1, "an update, delete or rollback runs under the creator's session policy")
	assert.Equal(t, "ci", creator.Tags["team"])
	assert.Equal(t, "octocat", creator.SourceIdentity)
}

func TestSessionPolicy_ARefusalReplaysAsARefusal(t *testing.T) {
	ctx := context.Background()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	stack := func(state emulator.StateManager, store *emulator.EventStore) (*emulator.Server, *emulator.PluginRegistry,
		*emulator.AuthController) {
		registry := emulator.NewPluginRegistry()
		tc := emulator.NewTimeController(stsFedT0)
		iamPlugin := &emulator.IAMPlugin{}
		require.NoError(t, iamPlugin.Initialize(ctx, emulator.PluginConfig{State: state, Logger: logger}))
		registry.Register(iamPlugin)
		stsPlugin := &emulator.STSPlugin{}
		require.NoError(t, stsPlugin.Initialize(ctx, emulator.PluginConfig{State: state, Logger: logger,
			Options: map[string]any{"time_controller": tc}}))
		registry.Register(stsPlugin)
		auth := emulator.NewAuthController(state, logger)
		cfg := emulator.DefaultConfig()
		return emulator.NewServer(*cfg, registry, store, state, tc, logger, emulator.ServerOptions{Auth: auth}), registry, auth
	}

	recorded := newIAMBodyRecordingStore()
	live, _, _ := stack(emulator.NewMemoryStateManager(), recorded)
	idpOK(t, live, "CreateRole", map[string]string{"RoleName": "worker", "AssumeRolePolicyDocument": `{"Version":"2012-10-17","Statement":[]}`})
	idpOK(t, live, "AttachRolePolicy", map[string]string{"RoleName": "worker", "PolicyArn": "arn:aws:iam::aws:policy/AdministratorAccess"})
	resp := stsRequest(t, live, "AssumeRole", map[string]string{"RoleArn": "arn:aws:iam::123456789012:role/worker",
		"RoleSessionName": "s1", "PolicyArns.member.1.arn": "arn:aws:iam::aws:policy/IAMReadOnlyAccess"})
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))
	key := fedSessionKey(t, string(raw))
	status, _ := fedIAMAs(t, live, key, "CreateUser", map[string]string{"UserName": "refused"})
	require.Equal(t, http.StatusForbidden, status, "refused live by the session policy")

	var narrowed int
	events, err := recorded.GetEvents(ctx, emulator.EventFilter{})
	require.NoError(t, err)
	for _, ev := range events {
		if ev.PrincipalSession != nil && len(ev.PrincipalSession.SessionPolicies) > 0 {
			narrowed++
		}
	}
	require.Equal(t, 1, narrowed, "the refused request records its session's policies")

	replayed := emulator.NewMemoryStateManager()
	_, registry, auth := stack(replayed, newIAMBodyRecordingStore())
	engine := emulator.NewReplayEngine(recorded, replayed, emulator.NewTimeController(stsFedT0), registry,
		emulator.ReplayConfig{}, &testLogger{}, emulator.WithReplayPipeline(emulator.ReplayPipeline{Auth: auth}))
	results, err := engine.Replay(ctx, "default")
	require.NoError(t, err)
	// The session's Expiration renders the clock to the nanosecond, which a replay does not
	// reproduce exactly; the refused request is what this asserts on.
	for _, d := range results.Differences {
		assert.NotEqual(t, "CreateUser", d.Operation, "the refusal replays as a refusal: %s %v != %v",
			d.Field, d.Expected, d.Actual)
	}
	got, err := replayed.Get(ctx, "iam", "user:123456789012/refused")
	require.NoError(t, err)
	assert.Nil(t, got, "the replay did not create what the live run refused")
}

func TestSTSAssumeRole_SessionTagsSourceIdentityAndChaining(t *testing.T) {
	srv := newTrustPolicyTestServer(t)
	caller := trustSetupCaller(t, srv, "alice")
	trust := func(actions ...string) string {
		return `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"123456789012"},"Action":["` +
			strings.Join(actions, `","`) + `"]}]}`
	}
	trustCreateRole(t, srv, "plain", trust("sts:AssumeRole"))
	trustCreateRole(t, srv, "tagging", trust("sts:AssumeRole", "sts:TagSession", "sts:SetSourceIdentity"))
	trustCreateRole(t, srv, "next", trust("sts:AssumeRole", "sts:TagSession", "sts:SetSourceIdentity"))
	trustCreateRole(t, srv, "next-no-source", trust("sts:AssumeRole"))
	trustCreateRole(t, srv, "long", trust("sts:AssumeRole"))
	idpOK(t, srv, "UpdateRole", map[string]string{"RoleName": "long", "MaxSessionDuration": "7200"})
	// The first session may itself assume a role, for the chaining cases.
	idpOK(t, srv, "PutRolePolicy", map[string]string{"RoleName": "tagging", "PolicyName": "chain",
		"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sts:AssumeRole","Resource":"*"}]}`})

	assume := func(key, role string, extra map[string]string) (int, string) {
		form := url.Values{"Action": {"AssumeRole"}, "Version": {"2011-06-15"},
			"RoleArn": {"arn:aws:iam::123456789012:role/" + role}, "RoleSessionName": {"s"}}
		for k, v := range extra {
			form.Set(k, v)
		}
		r, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader(form.Encode()))
		require.NoError(t, err)
		r.Host = "sts.amazonaws.com"
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.Header.Set("Authorization", trustAuthHeader(key, "sts"))
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, r)
		return w.Code, w.Body.String()
	}
	tagged := map[string]string{"Tags.member.1.Key": "team", "Tags.member.1.Value": "ci", "TransitiveTagKeys.member.1": "team",
		"SourceIdentity": "alice-laptop"}

	status, body := assume(caller, "plain", tagged)
	assert.Equal(t, http.StatusForbidden, status, body)
	assert.Contains(t, body, "sts:TagSession")

	status, body = assume(caller, "tagging", map[string]string{"Tags.member.1.Key": "team", "Tags.member.1.Value": "a",
		"Tags.member.2.Key": "TEAM", "Tags.member.2.Value": "b"})
	require.Equal(t, http.StatusBadRequest, status, body)
	code, msg := fedErrorCode(t, body)
	assert.Equal(t, "InvalidParameterValue", code)
	assert.Equal(t, "Duplicate tag keys found. Please note that Tag keys are case insensitive.", msg)

	status, body = assume(caller, "tagging", tagged)
	require.Equal(t, http.StatusOK, status, body)
	assert.Contains(t, body, "<SourceIdentity>alice-laptop</SourceIdentity>")
	first := fedSessionKey(t, body)

	// Chained: the transitive tag and the source identity carry forward.
	status, body = assume(first, "next", nil)
	require.Equal(t, http.StatusOK, status, body)
	assert.Contains(t, body, "<SourceIdentity>alice-laptop</SourceIdentity>", "the source identity persists")
	status, body = assume(first, "next", map[string]string{"SourceIdentity": "someone-else"})
	assert.Equal(t, http.StatusForbidden, status, "a source identity cannot be changed: %s", body)
	status, body = assume(first, "next", map[string]string{"Tags.member.1.Key": "Team", "Tags.member.1.Value": "x"})
	assert.Equal(t, http.StatusBadRequest, status, "a transitive tag cannot be overridden: %s", body)
	status, body = assume(first, "next-no-source", nil)
	assert.Equal(t, http.StatusForbidden, status, "carrying a source identity needs sts:SetSourceIdentity: %s", body)

	// MaxSessionDuration.
	status, body = assume(caller, "plain", map[string]string{"DurationSeconds": "7200"})
	require.Equal(t, http.StatusBadRequest, status, body)
	_, msg = fedErrorCode(t, body)
	assert.Equal(t, "Requested DurationSeconds exceeds MaxSessionDuration set for this role.", msg)
	status, body = assume(caller, "long", map[string]string{"DurationSeconds": "7200"})
	assert.Equal(t, http.StatusOK, status, body)
	status, body = assume(caller, "tagging", map[string]string{"SourceIdentity": "x"})
	assert.Equal(t, http.StatusBadRequest, status, body)
}

func TestSTSFederation_DuplicateSessionTagKeysAreRefused(t *testing.T) {
	srv := fedAuthServer(t)
	fedRole(t, srv, "taggable", fedTrust("sts:AssumeRoleWithWebIdentity", "sts:TagSession"))
	claims := ghaClaims(map[string]any{
		"https://aws.amazon.com/tags":                     map[string]any{"principal_tags": map[string]any{"Team": []string{"a"}}},
		"https://aws.amazon.com/tags/principal_tags/team": "b",
	})
	status, body := fedAssume(t, srv, "taggable", claims, nil)
	require.Equal(t, http.StatusBadRequest, status, body)
	code, _ := fedErrorCode(t, body)
	assert.Equal(t, "InvalidParameterValue", code)
}

func TestSessionPolicy_PresignedAndPostPolicyRequestsResolveTheirPrincipal(t *testing.T) {
	ts := emulator.StartTestServer(t)
	const setup = "AKIAIOSFODNN7EXAMPLE"
	status, body := tsCall(t, ts, http.MethodPut, "/presign-scope", "s3", setup, nil, nil)
	require.Equal(t, http.StatusOK, status, body)
	status, body = tsCall(t, ts, http.MethodPut, "/presign-scope/key", "s3", setup, url.Values{"x": {"y"}}, nil)
	require.Equal(t, http.StatusOK, status, body)

	denied := tsAdminSession(t, ts, url.Values{"Policy": {`{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":"s3:PutObject","Resource":"*"}]}`}})
	allowed := tsAdminSessionNamed(t, ts, "reader", nil)
	presigned := func(key string) int {
		// Signed now on the server's clock, so the URL has not expired.
		now := ts.TimeController().Now().UTC()
		u := ts.URL + "/presign-scope/key?X-Amz-Algorithm=AWS4-HMAC-SHA256" +
			"&X-Amz-Credential=" + url.QueryEscape(key+"/"+now.Format("20060102")+"/us-east-1/s3/aws4_request") +
			"&X-Amz-Date=" + now.Format("20060102T150405Z") +
			"&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=fakesig"
		r, err := http.NewRequestWithContext(t.Context(), http.MethodGet, u, nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(r)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		return resp.StatusCode
	}
	assert.Equal(t, http.StatusForbidden, presigned(denied), "a presigned GET is decided as its signer")
	assert.Equal(t, http.StatusOK, presigned(allowed))
	assert.Equal(t, http.StatusOK, presigned(setup), "a key with no IAM entity stays unenforced")

	// A POST-policy upload's form names its signer the same way.
	post := func(key string) int {
		var buf strings.Builder
		mw := multipartWriter(&buf)
		require.NoError(t, mw.WriteField("key", "uploaded"))
		require.NoError(t, mw.WriteField("X-Amz-Credential", key+"/20260402/us-east-1/s3/aws4_request"))
		require.NoError(t, mw.WriteField("X-Amz-Algorithm", "AWS4-HMAC-SHA256"))
		require.NoError(t, mw.Close())
		r, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/presign-scope", strings.NewReader(buf.String()))
		require.NoError(t, err)
		r.Header.Set("Content-Type", mw.FormDataContentType())
		resp, err := http.DefaultClient.Do(r)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		return resp.StatusCode
	}
	readOnly := tsAdminSessionNamed(t, ts, "ro", url.Values{"Policy": {`{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`}})
	assert.Equal(t, http.StatusForbidden, post(readOnly), "a POST-policy upload is decided as its signer")
}

func TestSessionPolicy_RoleGoneKeepsTheSecondaryPasses(t *testing.T) {
	ts := emulator.StartTestServer(t)
	key := tsAdminSession(t, ts, url.Values{"Policy": {`{"Version":"2012-10-17","Statement":[
		{"Effect":"Allow","Action":"ec2:RunInstances","Resource":"*"}]}`}})
	const setup = "AKIAIOSFODNN7EXAMPLE"
	for _, op := range []string{"DetachRolePolicy", "DeleteRole"} {
		form := url.Values{"Action": {op}, "Version": {"2010-05-08"}, "RoleName": {"admin"},
			"PolicyArn": {"arn:aws:iam::aws:policy/AdministratorAccess"}}
		status, body := tsCall(t, ts, http.MethodPost, "/", "iam", setup, form, nil)
		require.Equal(t, http.StatusOK, status, "%s: %s", op, body)
	}
	ts.SeedEC2Image("ami-0123456789abcdef0", "scope")
	form := url.Values{"Action": {"RunInstances"}, "Version": {"2016-11-15"}, "ImageId": {"ami-0123456789abcdef0"},
		"MinCount": {"1"}, "MaxCount": {"1"}, "InstanceType": {"t3.micro"},
		"TagSpecification.1.ResourceType": {"instance"}, "TagSpecification.1.Tag.1.Key": {"team"},
		"TagSpecification.1.Tag.1.Value": {"ci"}}
	status, body := tsCall(t, ts, http.MethodPost, "/", "ec2", key, form, nil)
	assert.Equal(t, http.StatusForbidden, status, "the tag-on-create pass still applies the session policy: %s", body)
	assert.Contains(t, body, "blocked by session policy")
}

func TestSTSAssumeRole_TransitiveKeyMustNameATag(t *testing.T) {
	srv := newTrustPolicyTestServer(t)
	caller := trustSetupCaller(t, srv, "alice")
	trustCreateRole(t, srv, "tagging", `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
		"Principal":{"AWS":"123456789012"},"Action":["sts:AssumeRole","sts:TagSession"]}]}`)
	idpOK(t, srv, "PutRolePolicy", map[string]string{"RoleName": "tagging", "PolicyName": "chain",
		"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sts:*","Resource":"*"}]}`})
	assume := func(key string, extra url.Values) (int, string) {
		form := url.Values{"Action": {"AssumeRole"}, "Version": {"2011-06-15"},
			"RoleArn": {"arn:aws:iam::123456789012:role/tagging"}, "RoleSessionName": {"s"}}
		for k, v := range extra {
			form[k] = v
		}
		r, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "/", strings.NewReader(form.Encode()))
		require.NoError(t, err)
		r.Host = "sts.amazonaws.com"
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.Header.Set("Authorization", trustAuthHeader(key, "sts"))
		w := httptest.NewRecorder()
		srv.ServeHTTP(w, r)
		return w.Code, w.Body.String()
	}
	tagged := func(transitive string) url.Values {
		return url.Values{"Tags.member.1.Key": {"team"}, "Tags.member.1.Value": {"ci"},
			"TransitiveTagKeys.member.1": {transitive}}
	}

	status, body := assume(caller, tagged("project"))
	require.Equal(t, http.StatusBadRequest, status, body)
	code, _ := fedErrorCode(t, body)
	assert.Equal(t, "InvalidParameterValue", code)

	// Tag keys are case-insensitive, so a transitive key names a tag whatever its case, and the tag it
	// names carries forward: a chained call cannot then override it.
	status, body = assume(caller, tagged("TEAM"))
	require.Equal(t, http.StatusOK, status, "a transitive key matches its tag regardless of case: %s", body)
	first := fedSessionKey(t, body)
	status, body = assume(first, url.Values{"Tags.member.1.Key": {"team"}, "Tags.member.1.Value": {"x"}})
	assert.Equal(t, http.StatusBadRequest, status, "the tag named in another case is still transitive: %s", body)
}

func TestSessionPolicy_CloudFormationCreatorKeepsItsEntityTags(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	srv := newIAMTestServerWithState(t, state)
	idpOK(t, srv, "CreateRole", map[string]string{"RoleName": "admin", "AssumeRolePolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
		"Tags.member.1.Key": "dept", "Tags.member.1.Value": "eng"})
	creator, err := emulator.CFNStackCreatorWithStateForTest(state, &emulator.Principal{
		ARN: "arn:aws:sts::123456789012:assumed-role/admin/s1", SessionTags: map[string]string{"team": "ci"}})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"dept": "eng", "team": "ci"}, creator.Tags,
		"the role's own tags under the session's")
}

// multipartWriter returns a multipart writer over buf.
func multipartWriter(buf *strings.Builder) *multipart.Writer { return multipart.NewWriter(buf) }

// presignedCallerAccount sends a presigned (unverified) STS GetCallerIdentity naming key and returns
// the account it reports.
func presignedCallerAccount(t *testing.T, ts *emulator.TestServer, key string) string {
	t.Helper()
	now := ts.TimeController().Now().UTC()
	u := ts.URL + "/?Action=GetCallerIdentity&Version=2011-06-15&X-Amz-Algorithm=AWS4-HMAC-SHA256" +
		"&X-Amz-Credential=" + url.QueryEscape(key+"/"+now.Format("20060102")+"/us-east-1/sts/aws4_request") +
		"&X-Amz-Date=" + now.Format("20060102T150405Z") +
		"&X-Amz-Expires=900&X-Amz-SignedHeaders=host&X-Amz-Signature=forged"
	r, err := http.NewRequestWithContext(t.Context(), http.MethodGet, u, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(r)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	return xmlField(string(body), "Account")
}

func TestPresignedCredential_IsTrustedOnlyWithoutSignatureVerification(t *testing.T) {
	const account = "111122223333"
	t.Run("verification off", func(t *testing.T) {
		ts := emulator.StartTestServer(t, emulator.WithAccounts(account))
		entry, ok := ts.CredentialsFor(account)
		require.True(t, ok)
		assert.Equal(t, account, presignedCallerAccount(t, ts, entry.AccessKeyID),
			"with nothing verified, a presigned request is attributed to its signer")
	})
	t.Run("verification on", func(t *testing.T) {
		ts := emulator.StartTestServerWithAccounts(t, account)
		entry, ok := ts.CredentialsFor(account)
		require.True(t, ok)
		assert.NotEqual(t, account, presignedCallerAccount(t, ts, entry.AccessKeyID),
			"a presigned signature substrate does not verify cannot name a key's account")
	})
}

func TestSigV2Credentials_ResolveTheirPrincipal(t *testing.T) {
	ts := emulator.StartTestServer(t)
	const setup = "AKIAIOSFODNN7EXAMPLE"
	iam := func(op string, params url.Values) string {
		params.Set("Action", op)
		params.Set("Version", "2010-05-08")
		status, body := tsCall(t, ts, http.MethodPost, "/", "iam", setup, params, nil)
		require.Equal(t, http.StatusOK, status, "%s: %s", op, body)
		return body
	}
	status, body := tsCall(t, ts, http.MethodPut, "/sigv2-scope", "s3", setup, nil, nil)
	require.Equal(t, http.StatusOK, status, body)
	status, body = tsCall(t, ts, http.MethodPut, "/sigv2-scope/key", "s3", setup, url.Values{"x": {"y"}}, nil)
	require.Equal(t, http.StatusOK, status, body)
	iam("CreateUser", url.Values{"UserName": {"v2"}})
	iam("PutUserPolicy", url.Values{"UserName": {"v2"}, "PolicyName": {"deny"},
		"PolicyDocument": {`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:GetObject","Resource":"*"}]}`}})
	key := xmlField(iam("CreateAccessKey", url.Values{"UserName": {"v2"}}), "AccessKeyId")

	get := func(path string, header string) int {
		r, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+path, nil)
		require.NoError(t, err)
		r.Host = "s3.amazonaws.com"
		if header != "" {
			r.Header.Set("Authorization", header)
		}
		resp, err := http.DefaultClient.Do(r)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		return resp.StatusCode
	}
	assert.Equal(t, http.StatusForbidden, get("/sigv2-scope/key", "AWS "+key+":c2lnbmF0dXJl"), "a SigV2 header names its signer")
	assert.Equal(t, http.StatusForbidden, get("/sigv2-scope/key?AWSAccessKeyId="+key+"&Expires=4102444800&Signature=x", ""),
		"a SigV2 presigned URL names its signer")
}
