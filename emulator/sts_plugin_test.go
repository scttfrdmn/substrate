package emulator_test

import (
	"context"
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// newSTSTestServer returns a test server with IAMPlugin and STSPlugin registered.
func newSTSTestServer(t *testing.T) (*emulator.Server, *emulator.MemoryStateManager, *emulator.TimeController) {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelInfo, false)
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

	return emulator.NewServer(*cfg, registry, store, state, tc, logger), state, tc
}

// stsRequest builds and executes an STS query-protocol request.
func stsRequest(t *testing.T, srv *emulator.Server, action string, params map[string]string) *http.Response {
	t.Helper()
	urlStr := "/?Action=" + action
	for k, v := range params {
		urlStr += "&" + k + "=" + v
	}
	r := httptest.NewRequest(http.MethodPost, urlStr, nil)
	r.Host = "sts.amazonaws.com"

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Result()
}

// --- GetCallerIdentity -----------------------------------------------------

func TestSTSPlugin_GetCallerIdentity_NoCredentials(t *testing.T) {
	srv, _, _ := newSTSTestServer(t)

	resp := stsRequest(t, srv, "GetCallerIdentity", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "GetCallerIdentityResponse")
	assert.Contains(t, string(body), "Account")
}

func TestSTSPlugin_GetCallerIdentity_WithPrincipal(t *testing.T) {
	srv, _, _ := newSTSTestServer(t)

	r := httptest.NewRequest(http.MethodPost, "/?Action=GetCallerIdentity", nil)
	r.Host = "sts.amazonaws.com"
	// Simulate authenticated request via authorization header (AKIA → testAccountID).
	r.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20250101/us-east-1/sts/aws4_request, SignedHeaders=host, Signature=abc")

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	resp := w.Result()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestSTSPlugin_GetCallerIdentity_InjectPrincipal(t *testing.T) {
	// Exercise the ctx.Principal != nil path by calling HandleRequest directly.
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(0, false)
	tc := emulator.NewTimeController(time.Now())

	stsPlugin := &emulator.STSPlugin{}
	require.NoError(t, stsPlugin.Initialize(context.TODO(), emulator.PluginConfig{
		State: state, Logger: logger,
		Options: map[string]any{"time_controller": tc},
	}))

	ctx := &emulator.RequestContext{
		RequestID: "req-test",
		AccountID: "123456789012",
		Region:    "us-east-1",
		Principal: &emulator.Principal{
			ARN:  "arn:aws:iam::123456789012:user/alice",
			Type: "User",
		},
	}
	req := &emulator.AWSRequest{
		Service:   "sts",
		Operation: "GetCallerIdentity",
		Params:    map[string]string{},
	}

	resp, err := stsPlugin.HandleRequest(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "alice")
}

func TestSTSPlugin_GetCallerIdentity_XMLFormat(t *testing.T) {
	srv, _, _ := newSTSTestServer(t)
	resp := stsRequest(t, srv, "GetCallerIdentity", nil)

	body, _ := io.ReadAll(resp.Body)
	assert.Equal(t, "text/xml; charset=UTF-8", resp.Header.Get("Content-Type"))

	type xmlResult struct {
		XMLName xml.Name `xml:"GetCallerIdentityResponse"`
	}
	var result xmlResult
	require.NoError(t, xml.Unmarshal(body, &result))
}

// --- AssumeRole ------------------------------------------------------------

func TestSTSPlugin_AssumeRole_Success(t *testing.T) {
	srv, _, tc := newSTSTestServer(t)

	// First create the role via IAM.
	iamReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"RoleName":"myrole"}`))
	iamReq.Host = "iam.amazonaws.com"
	iamReq.Header.Set("X-Amz-Target", "AmazonIdentityManagementService.CreateRole")
	iamReq.Header.Set("Content-Type", "application/x-amz-json-1.1")
	iamW := httptest.NewRecorder()
	srv.ServeHTTP(iamW, iamReq)
	require.Equal(t, http.StatusOK, iamW.Code)

	now := tc.Now()
	resp := stsRequest(t, srv, "AssumeRole", map[string]string{
		"RoleArn":         "arn:aws:iam::000000000000:role/myrole",
		"RoleSessionName": "test-session",
		"DurationSeconds": "3600",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "AssumeRoleResponse")
	assert.Contains(t, string(body), "AccessKeyId")
	assert.Contains(t, string(body), "SecretAccessKey")
	assert.Contains(t, string(body), "SessionToken")

	// Expiration should be 1 hour after tc.Now().
	expected := now.Add(time.Hour).UTC().Format("2006")
	assert.Contains(t, string(body), expected)
}

func TestSTSPlugin_AssumeRole_RoleNotFound(t *testing.T) {
	srv, _, _ := newSTSTestServer(t)
	resp := stsRequest(t, srv, "AssumeRole", map[string]string{
		"RoleArn":         "arn:aws:iam::000000000000:role/nonexistent",
		"RoleSessionName": "session",
	})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestSTSPlugin_AssumeRole_MissingRoleArn(t *testing.T) {
	srv, _, _ := newSTSTestServer(t)
	resp := stsRequest(t, srv, "AssumeRole", map[string]string{
		"RoleSessionName": "session",
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestSTSPlugin_AssumeRole_DurationTooShort(t *testing.T) {
	srv, _, _ := newSTSTestServer(t)
	// Create role first.
	iamReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"RoleName":"r"}`))
	iamReq.Host = "iam.amazonaws.com"
	iamReq.Header.Set("X-Amz-Target", "AmazonIdentityManagementService.CreateRole")
	iamW := httptest.NewRecorder()
	srv.ServeHTTP(iamW, iamReq)

	resp := stsRequest(t, srv, "AssumeRole", map[string]string{
		"RoleArn":         "arn:aws:iam::000000000000:role/r",
		"RoleSessionName": "session",
		"DurationSeconds": "100", // < 900
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestSTSPlugin_AssumeRole_DurationTooLong(t *testing.T) {
	srv, _, _ := newSTSTestServer(t)
	iamReq := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"RoleName":"r"}`))
	iamReq.Host = "iam.amazonaws.com"
	iamReq.Header.Set("X-Amz-Target", "AmazonIdentityManagementService.CreateRole")
	iamW := httptest.NewRecorder()
	srv.ServeHTTP(iamW, iamReq)

	resp := stsRequest(t, srv, "AssumeRole", map[string]string{
		"RoleArn":         "arn:aws:iam::000000000000:role/r",
		"RoleSessionName": "session",
		"DurationSeconds": "99999", // > 43200
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// --- GetSessionToken -------------------------------------------------------

func TestSTSPlugin_GetSessionToken_Success(t *testing.T) {
	srv, _, _ := newSTSTestServer(t)
	resp := stsRequest(t, srv, "GetSessionToken", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(body), "GetSessionTokenResponse")
	assert.Contains(t, string(body), "AccessKeyId")
	assert.Contains(t, string(body), "SecretAccessKey")
	assert.Contains(t, string(body), "SessionToken")
}

func TestSTSPlugin_GetSessionToken_CustomDuration(t *testing.T) {
	srv, _, _ := newSTSTestServer(t)
	resp := stsRequest(t, srv, "GetSessionToken", map[string]string{
		"DurationSeconds": "7200",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestSTSPlugin_GetSessionToken_DurationTooShort(t *testing.T) {
	srv, _, _ := newSTSTestServer(t)
	resp := stsRequest(t, srv, "GetSessionToken", map[string]string{
		"DurationSeconds": "500",
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestSTSPlugin_GetSessionToken_DurationTooLong(t *testing.T) {
	srv, _, _ := newSTSTestServer(t)
	resp := stsRequest(t, srv, "GetSessionToken", map[string]string{
		"DurationSeconds": "200000", // > 129600
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestSTSPlugin_UnknownAction(t *testing.T) {
	srv, _, _ := newSTSTestServer(t)
	resp := stsRequest(t, srv, "FakeOperation", nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestSTSPlugin_now_WithoutTimeController(t *testing.T) {
	// When no time_controller is provided, now() falls back to time.Now().
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(0, false)

	stsPlugin := &emulator.STSPlugin{}
	require.NoError(t, stsPlugin.Initialize(context.TODO(), emulator.PluginConfig{
		State:  state,
		Logger: logger,
		// No time_controller in Options.
	}))

	ctx := &emulator.RequestContext{
		RequestID: "req-now",
		AccountID: "123456789012",
		Region:    "us-east-1",
	}
	req := &emulator.AWSRequest{
		Service:   "sts",
		Operation: "GetSessionToken",
		Params:    map[string]string{},
	}

	resp, err := stsPlugin.HandleRequest(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
