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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A signed request has always carried an access key; until #411 the server only
// looked one up when a CredentialRegistry was wired, and then named the *key*
// rather than the IAM user holding it. The tests below assert on the resolved
// principal ARN, because that string is what the policy loader looks up — a test
// that only checked for a non-nil principal passed before the fix.

// principalCapturePlugin records the RequestContext it was dispatched with, so a
// test can assert on what the server resolved rather than on a side effect of it.
type principalCapturePlugin struct {
	principal *emulator.Principal
	accountID string
	called    bool
}

func (p *principalCapturePlugin) Name() string { return "dynamodb" }

func (p *principalCapturePlugin) Initialize(_ context.Context, _ emulator.PluginConfig) error {
	return nil
}

func (p *principalCapturePlugin) HandleRequest(ctx *emulator.RequestContext, _ *emulator.AWSRequest) (*emulator.AWSResponse, error) {
	p.called = true
	p.accountID = ctx.AccountID
	if ctx.Principal != nil {
		copied := *ctx.Principal
		p.principal = &copied
	}
	return &emulator.AWSResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.0"},
		Body:       []byte(`{}`),
	}, nil
}

func (p *principalCapturePlugin) Shutdown(_ context.Context) error { return nil }

// newPrincipalTestServer returns a server with IAM, STS and a capture plugin
// registered, and no CredentialRegistry — which is how the CLI server runs.
func newPrincipalTestServer(t *testing.T) (*emulator.Server, *principalCapturePlugin) {
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

	capture := &principalCapturePlugin{}
	registry.Register(capture)

	return emulator.NewServer(*cfg, registry, store, state, tc, logger), capture
}

// principalIAMCall issues an IAM request as an unidentified caller. The AKIA
// header is what puts the setup calls in the test account; it resolves to no IAM
// entity, so the calls themselves stay unauthorized.
func principalIAMCall(t *testing.T, srv *emulator.Server, operation string, body any) *http.Response {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(raw))
	r.Host = "iam.amazonaws.com"
	r.Header.Set("X-Amz-Target", "AmazonIdentityManagementService."+operation)
	r.Header.Set("Content-Type", "application/x-amz-json-1.1")
	r.Header.Set("Authorization", principalAuthHeader("AKIAIOSFODNN7EXAMPLE", "iam"))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Result()
}

// principalAuthHeader builds a SigV4 Authorization header. The signature is not
// verified: without a CredentialRegistry the server skips VerifySigV4, which is
// the configuration under test.
func principalAuthHeader(accessKeyID, service string) string {
	return "AWS4-HMAC-SHA256 Credential=" + accessKeyID +
		"/20250101/us-east-1/" + service +
		"/aws4_request, SignedHeaders=host, Signature=unverified"
}

// principalDynamoCall issues a request to the capture plugin signed with the
// given access key.
func principalDynamoCall(t *testing.T, srv *emulator.Server, accessKeyID string) *http.Response {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(`{}`)))
	r.Header.Set("X-Amz-Target", "DynamoDB_20120810.GetItem")
	if accessKeyID != "" {
		r.Header.Set("Authorization", principalAuthHeader(accessKeyID, "dynamodb"))
	}
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Result()
}

// principalCreateAccessKey creates an IAM user and one access key for it,
// returning the key ID.
func principalCreateAccessKey(t *testing.T, srv *emulator.Server, userName string) string {
	t.Helper()
	require.Equal(t, http.StatusOK,
		principalIAMCall(t, srv, "CreateUser", map[string]any{"UserName": userName}).StatusCode)

	resp := principalIAMCall(t, srv, "CreateAccessKey", map[string]any{"UserName": userName})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	var parsed struct {
		AccessKey struct {
			AccessKeyID string `xml:"AccessKeyId"`
		} `xml:"CreateAccessKeyResult>AccessKey"`
	}
	require.NoError(t, xml.Unmarshal(body, &parsed))
	require.NotEmpty(t, parsed.AccessKey.AccessKeyID)
	return parsed.AccessKey.AccessKeyID
}

func TestServer_ResolvePrincipal_IAMAccessKeyNamesTheUser(t *testing.T) {
	srv, capture := newPrincipalTestServer(t)
	keyID := principalCreateAccessKey(t, srv, "alice")

	resp := principalDynamoCall(t, srv, keyID)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	require.True(t, capture.called)
	require.NotNil(t, capture.principal)
	// The user's name, not the access key ID: the policy loader looks up
	// user_policies:alice, and nothing is ever stored under the key.
	assert.Equal(t, "arn:aws:iam::123456789012:user/alice", capture.principal.ARN)
	assert.Equal(t, "IAMUser", capture.principal.Type)
}

func TestServer_ResolvePrincipal_STSSessionNamesTheAssumedRole(t *testing.T) {
	srv, capture := newPrincipalTestServer(t)

	require.Equal(t, http.StatusOK,
		principalIAMCall(t, srv, "CreateRole", map[string]any{"RoleName": "worker"}).StatusCode)

	// AssumeRole as a caller in the test account, so the session records that
	// account rather than the fallback.
	r := httptest.NewRequest(http.MethodPost,
		"/?Action=AssumeRole&RoleArn=arn:aws:iam::123456789012:role/worker&RoleSessionName=sess1", nil)
	r.Host = "sts.amazonaws.com"
	r.Header.Set("Authorization", principalAuthHeader("AKIAIOSFODNN7EXAMPLE", "sts"))
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)

	var assumed struct {
		Credentials struct {
			AccessKeyID string `xml:"AccessKeyId"`
		} `xml:"AssumeRoleResult>Credentials"`
	}
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &assumed))
	sessionKey := assumed.Credentials.AccessKeyID
	require.NotEmpty(t, sessionKey)

	resp := principalDynamoCall(t, srv, sessionKey)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	require.NotNil(t, capture.principal)
	assert.Equal(t, "arn:aws:sts::123456789012:assumed-role/worker/sess1", capture.principal.ARN)
	assert.Equal(t, "AssumedRole", capture.principal.Type)
	// The session's own account, not the one an ASIA prefix would have implied:
	// extractAccount recognizes only AKIA, so this call would otherwise have run
	// in account 000000000000 — a different partition from the resources the
	// caller who assumed the role had just created.
	assert.Equal(t, "123456789012", capture.accountID)
}

func TestServer_ResolvePrincipal_UnknownKeyStaysUnidentified(t *testing.T) {
	tests := []struct {
		name      string
		accessKey string
	}{
		// The credential the README, the testing guide and every e2e check use.
		// It belongs to no IAM entity, so it identifies nobody and — by the
		// existence-is-the-opt-in rule — is enforced against nothing. This is the
		// case that wiring a CredentialRegistry into the server would have turned
		// into an InvalidClientTokenId 403.
		{name: "documented example key", accessKey: "AKIAIOSFODNN7EXAMPLE"},
		{name: "built-in test key", accessKey: "AKIATEST12345678901"},
		{name: "never-minted key", accessKey: "AKIANEVERMINTED00001"},
		{name: "no authorization header", accessKey: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, capture := newPrincipalTestServer(t)

			resp := principalDynamoCall(t, srv, tt.accessKey)
			require.NoError(t, resp.Body.Close())

			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.True(t, capture.called)
			assert.Nil(t, capture.principal)
		})
	}
}

func TestCheckAccess_NoPermissionsRequiredActions(t *testing.T) {
	// AWS documents both of these as requiring no permissions — GetCallerIdentity
	// answers even against an explicit Deny, because a denial would return the
	// same information. Only reachable once a caller resolves to a real principal
	// at all, which is what made a policy-less IAM user's `aws sts
	// get-caller-identity` start returning AccessDeniedException.
	tests := []struct {
		name      string
		operation string
		allowed   bool
	}{
		{name: "GetCallerIdentity", operation: "GetCallerIdentity", allowed: true},
		{name: "GetSessionToken", operation: "GetSessionToken", allowed: true},
		// Not on the list: an ordinary STS call is evaluated, and a principal that
		// exists with no policies is an implicit deny.
		{name: "AssumeRole", operation: "AssumeRole", allowed: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := emulator.NewMemoryStateManager()
			auth := emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false))

			reqCtx := &emulator.RequestContext{
				RequestID: "req-1",
				AccountID: "123456789012",
				Region:    "us-east-1",
				Principal: &emulator.Principal{
					ARN:  "arn:aws:iam::123456789012:user/alice",
					Type: "IAMUser",
				},
			}
			req := &emulator.AWSRequest{
				Service:   "sts",
				Operation: tt.operation,
				Params:    map[string]string{},
			}

			err := auth.CheckAccess(reqCtx, req)
			if tt.allowed {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			var awsErr *emulator.AWSError
			require.ErrorAs(t, err, &awsErr)
			assert.Equal(t, "AccessDeniedException", awsErr.Code)
		})
	}
}

func TestResolvePrincipal_UnreachableOverHTTP(t *testing.T) {
	// Arms an HTTP request cannot produce: resolution has to yield no principal
	// rather than one built from half a record.
	tests := []struct {
		name      string
		namespace string
		key       string
		raw       string
		nilState  bool
		accessKey string
	}{
		{
			name:      "no state manager",
			nilState:  true,
			accessKey: "AKIAANYTHING00000001",
		},
		{
			name:      "empty access key",
			accessKey: "",
		},
		{
			name:      "access key record is not JSON",
			namespace: "iam",
			key:       "accesskey:AKIACORRUPT000000001",
			raw:       "{not json",
			accessKey: "AKIACORRUPT000000001",
		},
		{
			// A record written before CreateAccessKey stored the owner. Deriving a
			// user ARN from it would name the account's root path with an empty
			// name, which matches no policy key.
			name:      "access key record names no user",
			namespace: "iam",
			key:       "accesskey:AKIAORPHAN0000000001",
			raw:       `{"AccessKeyId":"AKIAORPHAN0000000001","Status":"Active"}`,
			accessKey: "AKIAORPHAN0000000001",
		},
		{
			name:      "session record is not JSON",
			namespace: "sts",
			key:       "session:ASIACORRUPT000000001",
			raw:       "{not json",
			accessKey: "ASIACORRUPT000000001",
		},
		{
			// GetSessionToken writes a session with no PrincipalArn when the caller
			// had no principal — an unidentified caller's session identifies nobody
			// in turn, rather than inheriting the account's root.
			name:      "session record names no principal",
			namespace: "sts",
			key:       "session:ASIANOPRINCIPAL00001",
			raw:       `{"AccessKeyId":"ASIANOPRINCIPAL00001","AccountId":"123456789012"}`,
			accessKey: "ASIANOPRINCIPAL00001",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var state emulator.StateManager
			if !tt.nilState {
				mem := emulator.NewMemoryStateManager()
				if tt.key != "" {
					require.NoError(t, mem.Put(context.Background(), tt.namespace, tt.key, []byte(tt.raw)))
				}
				state = mem
			}

			principal, sessionAccount := emulator.ResolvePrincipalForTest(state, "123456789012", tt.accessKey)
			assert.Nil(t, principal)
			assert.Empty(t, sessionAccount)
		})
	}
}

func TestServer_ResolvePrincipal_DeletedAccessKeyStopsResolving(t *testing.T) {
	srv, capture := newPrincipalTestServer(t)
	keyID := principalCreateAccessKey(t, srv, "alice")

	require.Equal(t, http.StatusOK, principalIAMCall(t, srv, "DeleteAccessKey", map[string]any{
		"UserName":    "alice",
		"AccessKeyId": keyID,
	}).StatusCode)

	resp := principalDynamoCall(t, srv, keyID)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Nil(t, capture.principal)
}
