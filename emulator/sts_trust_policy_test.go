package emulator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// #593: AssumeRole never read the role's AssumeRolePolicyDocument, so a trust
// policy was inert and sts:ExternalId — the confused-deputy defense — did nothing.
// Four refusals were verified minting credentials against a live server: a caller
// presenting no ExternalId, a wrong one, an explicit Deny, and a policy naming a
// different account. The tests here are those four plus the negative control, and
// each asserts the *message* as well as the code because AWS reports an implicit
// and an explicit denial under one code and distinguishes them only in prose.
//
// The identity-policy gate is a separate, already-working check
// (see TestCheckAccess_NoPermissionsRequiredActions): the caller must be allowed
// to call sts:AssumeRole *and* be admitted by the role. Both are wired here, so a
// denial from the wrong gate would show up as the wrong code.

// newTrustPolicyTestServer returns a server with IAM and STS registered and an
// AuthController wired, which is what makes ctx.Principal reach the STS plugin.
func newTrustPolicyTestServer(t *testing.T) *emulator.Server {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC))

	iamPlugin := &emulator.IAMPlugin{}
	require.NoError(t, iamPlugin.Initialize(context.TODO(), emulator.PluginConfig{State: state, Logger: logger}))
	registry.Register(iamPlugin)

	stsPlugin := &emulator.STSPlugin{}
	require.NoError(t, stsPlugin.Initialize(context.TODO(), emulator.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(stsPlugin)

	return emulator.NewServer(*cfg, registry, store, state, tc, logger, emulator.ServerOptions{
		Auth: emulator.NewAuthController(state, logger),
	})
}

// trustIAMCall issues an IAM setup request. The AKIA header is what puts the
// created entities in the test account rather than the unauthenticated fallback;
// it resolves to no IAM entity, so the setup calls are themselves unenforced and
// cannot be blocked by the policies they are creating.
func trustIAMCall(t *testing.T, srv *emulator.Server, operation string, body any) *http.Response {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(raw))
	r.Host = "iam.amazonaws.com"
	r.Header.Set("X-Amz-Target", "AmazonIdentityManagementService."+operation)
	r.Header.Set("Content-Type", "application/x-amz-json-1.1")
	r.Header.Set("Authorization", trustAuthHeader("AKIAIOSFODNN7EXAMPLE", "iam"))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Result()
}

// trustAuthHeader builds a SigV4 header. The signature is unverified — no
// CredentialRegistry is wired — so the key ID alone selects the caller.
func trustAuthHeader(accessKeyID, service string) string {
	return "AWS4-HMAC-SHA256 Credential=" + accessKeyID +
		"/20250101/us-east-1/" + service +
		"/aws4_request, SignedHeaders=host, Signature=unverified"
}

// trustAssumeRole calls AssumeRole as accessKeyID, optionally with an ExternalId.
func trustAssumeRole(t *testing.T, srv *emulator.Server, accessKeyID, roleARN, sessionName, externalID string) *http.Response {
	t.Helper()
	q := url.Values{}
	q.Set("Action", "AssumeRole")
	q.Set("Version", "2011-06-15")
	q.Set("RoleArn", roleARN)
	q.Set("RoleSessionName", sessionName)
	if externalID != "" {
		q.Set("ExternalId", externalID)
	}
	r := httptest.NewRequest(http.MethodPost, "/?"+q.Encode(), nil)
	r.Host = "sts.amazonaws.com"
	if accessKeyID != "" {
		r.Header.Set("Authorization", trustAuthHeader(accessKeyID, "sts"))
	}
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Result()
}

// trustSetupCaller creates an IAM user, grants it sts:AssumeRole so the identity
// gate passes, and returns its access key ID.
//
// The grant matters: without it every case below would fail at the *identity*
// gate with AccessDeniedException, and a test asserting only "denied" would pass
// while the trust policy stayed unread — which is exactly the #593 defect.
func trustSetupCaller(t *testing.T, srv *emulator.Server, userName string) string {
	t.Helper()
	require.Equal(t, http.StatusOK,
		trustIAMCall(t, srv, "CreateUser", map[string]any{"UserName": userName}).StatusCode)

	policy := `{"Version":"2012-10-17","Statement":[` +
		`{"Effect":"Allow","Action":"sts:AssumeRole","Resource":"*"}]}`
	require.Equal(t, http.StatusOK, trustIAMCall(t, srv, "PutUserPolicy", map[string]any{
		"UserName":       userName,
		"PolicyName":     "assume",
		"PolicyDocument": policy,
	}).StatusCode)

	resp := trustIAMCall(t, srv, "CreateAccessKey", map[string]any{"UserName": userName})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	// IAM answers XML on every path, whatever the request's content type.
	var parsed struct {
		AccessKey struct {
			AccessKeyID string `xml:"AccessKeyId"`
		} `xml:"CreateAccessKeyResult>AccessKey"`
	}
	require.NoError(t, xml.Unmarshal(body, &parsed))
	require.NotEmpty(t, parsed.AccessKey.AccessKeyID)
	return parsed.AccessKey.AccessKeyID
}

// trustCreateRole creates a role with the given trust policy document.
func trustCreateRole(t *testing.T, srv *emulator.Server, roleName, trustPolicy string) {
	t.Helper()
	require.Equal(t, http.StatusOK, trustIAMCall(t, srv, "CreateRole", map[string]any{
		"RoleName":                 roleName,
		"AssumeRolePolicyDocument": trustPolicy,
	}).StatusCode)
}

// trustSessionKeyFrom extracts the minted session's access key ID from an
// AssumeRole response, so a later call can be made *as* that session.
func trustSessionKeyFrom(t *testing.T, resp *http.Response) string {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	var parsed struct {
		AccessKeyID string `xml:"AssumeRoleResult>Credentials>AccessKeyId"`
	}
	require.NoError(t, xml.Unmarshal(body, &parsed), "body: %s", body)
	require.NotEmpty(t, parsed.AccessKeyID)
	return parsed.AccessKeyID
}

// trustGetCallerIdentity calls GetCallerIdentity as accessKeyID. Whether a
// credential still works is only answerable by making a request with it, which is
// why this exists rather than a state read.
func trustGetCallerIdentity(t *testing.T, srv *emulator.Server, accessKeyID string) *http.Response {
	t.Helper()
	q := url.Values{}
	q.Set("Action", "GetCallerIdentity")
	q.Set("Version", "2011-06-15")
	r := httptest.NewRequest(http.MethodPost, "/?"+q.Encode(), nil)
	r.Host = "sts.amazonaws.com"
	r.Header.Set("Authorization", trustAuthHeader(accessKeyID, "sts"))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Result()
}

// trustErrorFrom decodes the query-protocol error document.
func trustErrorFrom(t *testing.T, resp *http.Response) (code, message string) {
	t.Helper()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	var parsed struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Error   struct {
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
		} `xml:"Error"`
	}
	require.NoError(t, xml.Unmarshal(body, &parsed), "body: %s", body)
	return parsed.Error.Code, parsed.Error.Message
}

// The two message forms AWS uses. One code, two messages, mapping onto
// substrate's DecisionImplicitDeny and DecisionDeny.
const (
	trustMsgImplicit = "User: arn:aws:iam::123456789012:user/alice is not authorized to perform: " +
		"sts:AssumeRole because no role trust policy allows the sts:AssumeRole action"
	trustMsgExplicit = "User: arn:aws:iam::123456789012:user/alice is not authorized to perform: " +
		"sts:AssumeRole with an explicit deny in the role trust policy"
)

// TestSTSAssumeRole_TrustPolicyEnforced is #593's gate: each of these
// combinations minted credentials before the fix.
func TestSTSAssumeRole_TrustPolicyEnforced(t *testing.T) {
	// A third-party trust policy in the shape AWS's ExternalId guide documents.
	const externalIDPolicy = `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"AWS":"123456789012"},"Action":"sts:AssumeRole",` +
		`"Condition":{"StringEquals":{"sts:ExternalId":"secret-123"}}}]}`

	tests := []struct {
		name        string
		trustPolicy string
		externalID  string
		wantStatus  int
		wantCode    string
		wantMessage string
	}{
		{
			// The confused-deputy case. A caller who cannot present the shared
			// secret must be refused; before the fix this returned an ASIA key,
			// which made every ExternalId test in every consumer vacuous.
			name:        "no ExternalId is denied when the policy requires one",
			trustPolicy: externalIDPolicy,
			wantStatus:  http.StatusForbidden,
			wantCode:    "AccessDenied",
			wantMessage: trustMsgImplicit,
		},
		{
			name:        "a wrong ExternalId is denied",
			trustPolicy: externalIDPolicy,
			externalID:  "wrong-secret",
			wantStatus:  http.StatusForbidden,
			wantCode:    "AccessDenied",
			wantMessage: trustMsgImplicit,
		},
		{
			// The negative control. Without it every case above would also pass
			// for a gate that denied unconditionally, which is a different bug
			// with identical test output.
			name:        "the correct ExternalId is allowed",
			trustPolicy: externalIDPolicy,
			externalID:  "secret-123",
			wantStatus:  http.StatusOK,
		},
		{
			name: "an explicit Deny is denied, with the explicit-deny message",
			trustPolicy: `{"Version":"2012-10-17","Statement":[{"Effect":"Deny",` +
				`"Principal":"*","Action":"sts:AssumeRole"}]}`,
			wantStatus:  http.StatusForbidden,
			wantCode:    "AccessDenied",
			wantMessage: trustMsgExplicit,
		},
		{
			// A policy naming the partner account must not admit a local caller,
			// even when the secret is right: the principal and the condition are
			// independent gates and both have to hold.
			name: "a policy naming another account is denied even with the right ExternalId",
			trustPolicy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
				`"Principal":{"AWS":"111111111111"},"Action":"sts:AssumeRole",` +
				`"Condition":{"StringEquals":{"sts:ExternalId":"secret-123"}}}]}`,
			externalID:  "secret-123",
			wantStatus:  http.StatusForbidden,
			wantCode:    "AccessDenied",
			wantMessage: trustMsgImplicit,
		},
		{
			// A trust policy that admits the caller outright still works: the gate
			// must not have become "deny unless an ExternalId was sent".
			name: "a policy allowing the caller's account with no condition is allowed",
			trustPolicy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
				`"Principal":{"AWS":"123456789012"},"Action":"sts:AssumeRole"}]}`,
			wantStatus: http.StatusOK,
		},
		{
			// A trust policy naming an action other than sts:AssumeRole does not
			// permit assumption — the action is matched, not assumed.
			name: "a policy allowing only TagSession does not permit assumption",
			trustPolicy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
				`"Principal":"*","Action":"sts:TagSession"}]}`,
			wantStatus:  http.StatusForbidden,
			wantCode:    "AccessDenied",
			wantMessage: trustMsgImplicit,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newTrustPolicyTestServer(t)
			accessKey := trustSetupCaller(t, srv, "alice")
			trustCreateRole(t, srv, "broker", tt.trustPolicy)

			resp := trustAssumeRole(t, srv, accessKey,
				"arn:aws:iam::123456789012:role/broker", "session1", tt.externalID)
			require.Equal(t, tt.wantStatus, resp.StatusCode)

			if tt.wantStatus == http.StatusOK {
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.NoError(t, resp.Body.Close())
				assert.Contains(t, string(body), "<AccessKeyId>ASIA",
					"an allowed assumption must still mint a session credential")
				return
			}

			code, message := trustErrorFrom(t, resp)
			assert.Equal(t, tt.wantCode, code)
			assert.Equal(t, tt.wantMessage, message)
		})
	}
}

// TestSTSAssumeRole_TrustPolicyNotEnforcedWithoutOne pins the opt-in: a role with
// no trust policy is "not configured", not "trusts nobody".
//
// Real IAM requires a trust policy at CreateRole and would read an empty document
// as an implicit deny, but substrate permits creating a role without one, so
// denying here would refuse roles that cannot exist on AWS — and would break every
// existing caller who never wrote one. This is the same rule authz already follows
// for principals, one level down.
func TestSTSAssumeRole_TrustPolicyNotEnforcedWithoutOne(t *testing.T) {
	tests := []struct {
		name        string
		trustPolicy string
	}{
		{name: "no trust policy at all", trustPolicy: ""},
		{name: "an empty JSON document", trustPolicy: "{}"},
		{name: "a document with no statements", trustPolicy: `{"Version":"2012-10-17","Statement":[]}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newTrustPolicyTestServer(t)
			accessKey := trustSetupCaller(t, srv, "alice")
			trustCreateRole(t, srv, "legacy", tt.trustPolicy)

			resp := trustAssumeRole(t, srv, accessKey,
				"arn:aws:iam::123456789012:role/legacy", "session1", "")
			assert.Equal(t, http.StatusOK, resp.StatusCode)
		})
	}
}

// TestSTSAssumeRole_TrustPolicyNotEnforcedForNilPrincipal pins the second opt-out.
// An unauthenticated caller resolves to no ARN, so there is nothing for a
// Principal element to be true of; denying would break every in-process and
// httptest caller, which is the same reason CheckAccess returns nil on one.
func TestSTSAssumeRole_TrustPolicyNotEnforcedForNilPrincipal(t *testing.T) {
	srv := newTrustPolicyTestServer(t)
	trustCreateRole(t, srv, "broker", `{"Version":"2012-10-17","Statement":[{"Effect":"Deny",`+
		`"Principal":"*","Action":"sts:AssumeRole"}]}`)

	// No Authorization header: the request carries no principal at all. Even an
	// explicit Deny does not apply, because enforcement never runs.
	resp := trustAssumeRole(t, srv, "",
		"arn:aws:iam::123456789012:role/broker", "session1", "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// TestSTSAssumeRole_NoCredentialLeaksOnDenial asserts the ordering. Minting first
// and checking after would leave a usable session in state behind a 403, and no
// assertion on the response body can see that.
func TestSTSAssumeRole_NoCredentialLeaksOnDenial(t *testing.T) {
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC))

	iamPlugin := &emulator.IAMPlugin{}
	require.NoError(t, iamPlugin.Initialize(context.TODO(), emulator.PluginConfig{State: state, Logger: logger}))
	registry.Register(iamPlugin)

	stsPlugin := &emulator.STSPlugin{}
	require.NoError(t, stsPlugin.Initialize(context.TODO(), emulator.PluginConfig{
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(stsPlugin)

	srv := emulator.NewServer(*cfg, registry, store, state, tc, logger, emulator.ServerOptions{
		Auth: emulator.NewAuthController(state, logger),
	})

	accessKey := trustSetupCaller(t, srv, "alice")
	trustCreateRole(t, srv, "broker", `{"Version":"2012-10-17","Statement":[{"Effect":"Deny",`+
		`"Principal":"*","Action":"sts:AssumeRole"}]}`)

	resp := trustAssumeRole(t, srv, accessKey,
		"arn:aws:iam::123456789012:role/broker", "session1", "")
	require.Equal(t, http.StatusForbidden, resp.StatusCode)

	keys, err := state.List(context.TODO(), "sts", "session:")
	require.NoError(t, err)
	assert.Empty(t, keys, "a denied assumption must not have stored a session credential")
}

// TestSTSAssumeRole_ExternalIDValidation covers AssumeRole's documented bounds on
// ExternalId, which substrate did not parse at all before #593.
func TestSTSAssumeRole_ExternalIDValidation(t *testing.T) {
	tests := []struct {
		name       string
		externalID string
		wantStatus int
	}{
		{name: "one character is too short", externalID: "x", wantStatus: http.StatusBadRequest},
		{name: "two characters is the minimum", externalID: "xy", wantStatus: http.StatusOK},
		{name: "1224 characters is the maximum", externalID: trustRepeat("a", 1224), wantStatus: http.StatusOK},
		{name: "1225 characters is too long", externalID: trustRepeat("a", 1225), wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newTrustPolicyTestServer(t)
			accessKey := trustSetupCaller(t, srv, "alice")
			// No trust policy: the ExternalId is validated regardless of whether
			// any policy conditions on it, so this isolates the length check.
			trustCreateRole(t, srv, "broker", "")

			resp := trustAssumeRole(t, srv, accessKey,
				"arn:aws:iam::123456789012:role/broker", "session1", tt.externalID)
			require.Equal(t, tt.wantStatus, resp.StatusCode)
			if tt.wantStatus == http.StatusBadRequest {
				code, _ := trustErrorFrom(t, resp)
				assert.Equal(t, "ValidationError", code)
			}
		})
	}
}

// trustRepeat returns s repeated n times.
func trustRepeat(s string, n int) string {
	out := make([]byte, 0, len(s)*n)
	for range n {
		out = append(out, s...)
	}
	return string(out)
}
