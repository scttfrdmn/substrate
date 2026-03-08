package substrate_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	substrate "github.com/scttfrdmn/substrate"
)

// newIAMTestServer returns a test server with IAMPlugin registered.
func newIAMTestServer(t *testing.T) *substrate.Server {
	t.Helper()
	cfg := substrate.DefaultConfig()
	registry := substrate.NewPluginRegistry()
	state := substrate.NewMemoryStateManager()
	logger := substrate.NewDefaultLogger(slog.LevelInfo, false)
	store := substrate.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := substrate.NewTimeController(time.Now())

	iamPlugin := &substrate.IAMPlugin{}
	require.NoError(t, iamPlugin.Initialize(context.TODO(), substrate.PluginConfig{State: state, Logger: logger}))
	registry.Register(iamPlugin)

	return substrate.NewServer(*cfg, registry, store, state, tc, logger)
}

// iamRequest builds and executes an IAM JSON-protocol request against srv.
func iamRequest(t *testing.T, srv *substrate.Server, operation string, body any) *http.Response {
	t.Helper()
	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}
	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	r.Host = "iam.amazonaws.com"
	r.Header.Set("X-Amz-Target", "AmazonIdentityManagementService."+operation)
	r.Header.Set("Content-Type", "application/x-amz-json-1.1")

	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)
	return w.Result()
}

// decodeJSON decodes the response body as JSON into dst.
func decodeJSON(t *testing.T, r *http.Response, dst any) {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(body, dst))
}

// --- User tests ------------------------------------------------------------

func TestIAMPlugin_CreateUser_Success(t *testing.T) {
	srv := newIAMTestServer(t)
	resp := iamRequest(t, srv, "CreateUser", map[string]any{"UserName": "alice"})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]any
	decodeJSON(t, resp, &result)
	user := result["User"].(map[string]any)
	assert.Equal(t, "alice", user["UserName"])
	assert.NotEmpty(t, user["UserId"])
	assert.Contains(t, user["Arn"].(string), ":user/alice")
	assert.Equal(t, "/", user["Path"])
}

func TestIAMPlugin_CreateUser_CustomPath(t *testing.T) {
	srv := newIAMTestServer(t)
	resp := iamRequest(t, srv, "CreateUser", map[string]any{"UserName": "bob", "Path": "/eng/"})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]any
	decodeJSON(t, resp, &result)
	user := result["User"].(map[string]any)
	assert.Equal(t, "/eng/", user["Path"])
	assert.Contains(t, user["Arn"].(string), ":user/eng/bob")
}

func TestIAMPlugin_CreateUser_Duplicate(t *testing.T) {
	srv := newIAMTestServer(t)
	iamRequest(t, srv, "CreateUser", map[string]any{"UserName": "alice"})
	resp := iamRequest(t, srv, "CreateUser", map[string]any{"UserName": "alice"})

	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	var result map[string]string
	decodeJSON(t, resp, &result)
	assert.Equal(t, "EntityAlreadyExistsException", result["__type"])
}

func TestIAMPlugin_CreateUser_MissingName(t *testing.T) {
	srv := newIAMTestServer(t)
	resp := iamRequest(t, srv, "CreateUser", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	var result map[string]string
	decodeJSON(t, resp, &result)
	assert.Equal(t, "ValidationError", result["__type"])
}

func TestIAMPlugin_GetUser_Success(t *testing.T) {
	srv := newIAMTestServer(t)
	iamRequest(t, srv, "CreateUser", map[string]any{"UserName": "alice"})

	resp := iamRequest(t, srv, "GetUser", map[string]any{"UserName": "alice"})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]any
	decodeJSON(t, resp, &result)
	user := result["User"].(map[string]any)
	assert.Equal(t, "alice", user["UserName"])
}

func TestIAMPlugin_GetUser_NotFound(t *testing.T) {
	srv := newIAMTestServer(t)
	resp := iamRequest(t, srv, "GetUser", map[string]any{"UserName": "nobody"})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	var result map[string]string
	decodeJSON(t, resp, &result)
	assert.Equal(t, "NoSuchEntityException", result["__type"])
}

func TestIAMPlugin_DeleteUser_Success(t *testing.T) {
	srv := newIAMTestServer(t)
	iamRequest(t, srv, "CreateUser", map[string]any{"UserName": "alice"})

	resp := iamRequest(t, srv, "DeleteUser", map[string]any{"UserName": "alice"})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Should be gone now.
	resp2 := iamRequest(t, srv, "GetUser", map[string]any{"UserName": "alice"})
	assert.Equal(t, http.StatusNotFound, resp2.StatusCode)
}

func TestIAMPlugin_DeleteUser_WithAttachedPolicy(t *testing.T) {
	srv := newIAMTestServer(t)
	iamRequest(t, srv, "CreateUser", map[string]any{"UserName": "alice"})
	iamRequest(t, srv, "AttachUserPolicy", map[string]any{
		"UserName":  "alice",
		"PolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
	})

	resp := iamRequest(t, srv, "DeleteUser", map[string]any{"UserName": "alice"})
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	var result map[string]string
	decodeJSON(t, resp, &result)
	assert.Equal(t, "DeleteConflictException", result["__type"])
}

func TestIAMPlugin_ListUsers_Empty(t *testing.T) {
	srv := newIAMTestServer(t)
	resp := iamRequest(t, srv, "ListUsers", map[string]any{})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]any
	decodeJSON(t, resp, &result)
	users := result["Users"].([]any)
	assert.Empty(t, users)
	assert.False(t, result["IsTruncated"].(bool))
}

func TestIAMPlugin_ListUsers_Multiple(t *testing.T) {
	srv := newIAMTestServer(t)
	for _, name := range []string{"alice", "bob", "carol"} {
		iamRequest(t, srv, "CreateUser", map[string]any{"UserName": name})
	}

	resp := iamRequest(t, srv, "ListUsers", map[string]any{})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]any
	decodeJSON(t, resp, &result)
	users := result["Users"].([]any)
	assert.Len(t, users, 3)
}

func TestIAMPlugin_ListUsers_Pagination(t *testing.T) {
	srv := newIAMTestServer(t)
	for _, name := range []string{"alice", "bob", "carol", "dave", "eve"} {
		iamRequest(t, srv, "CreateUser", map[string]any{"UserName": name})
	}

	// Page 1: MaxItems=2.
	resp1 := iamRequest(t, srv, "ListUsers", map[string]any{"MaxItems": 2})
	var page1 map[string]any
	decodeJSON(t, resp1, &page1)
	assert.True(t, page1["IsTruncated"].(bool))
	assert.Len(t, page1["Users"].([]any), 2)
	marker := page1["Marker"].(string)
	assert.NotEmpty(t, marker)

	// Page 2: using marker.
	resp2 := iamRequest(t, srv, "ListUsers", map[string]any{"MaxItems": 2, "Marker": marker})
	var page2 map[string]any
	decodeJSON(t, resp2, &page2)
	assert.Len(t, page2["Users"].([]any), 2)

	// Page 3: remaining.
	marker2 := page2["Marker"].(string)
	resp3 := iamRequest(t, srv, "ListUsers", map[string]any{"MaxItems": 2, "Marker": marker2})
	var page3 map[string]any
	decodeJSON(t, resp3, &page3)
	assert.Len(t, page3["Users"].([]any), 1)
	assert.False(t, page3["IsTruncated"].(bool))
}

// --- Role tests ------------------------------------------------------------

func TestIAMPlugin_CreateRole_Success(t *testing.T) {
	srv := newIAMTestServer(t)
	trustPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}`

	resp := iamRequest(t, srv, "CreateRole", map[string]any{
		"RoleName":                 "lambda-role",
		"AssumeRolePolicyDocument": trustPolicy,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]any
	decodeJSON(t, resp, &result)
	role := result["Role"].(map[string]any)
	assert.Equal(t, "lambda-role", role["RoleName"])
	assert.Contains(t, role["Arn"].(string), ":role/lambda-role")
}

func TestIAMPlugin_CreateRole_Duplicate(t *testing.T) {
	srv := newIAMTestServer(t)
	iamRequest(t, srv, "CreateRole", map[string]any{"RoleName": "myrole"})
	resp := iamRequest(t, srv, "CreateRole", map[string]any{"RoleName": "myrole"})
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
}

func TestIAMPlugin_GetRole_Success(t *testing.T) {
	srv := newIAMTestServer(t)
	iamRequest(t, srv, "CreateRole", map[string]any{"RoleName": "myrole"})

	resp := iamRequest(t, srv, "GetRole", map[string]any{"RoleName": "myrole"})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestIAMPlugin_GetRole_NotFound(t *testing.T) {
	srv := newIAMTestServer(t)
	resp := iamRequest(t, srv, "GetRole", map[string]any{"RoleName": "nobody"})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestIAMPlugin_DeleteRole_WithAttachedPolicy(t *testing.T) {
	srv := newIAMTestServer(t)
	iamRequest(t, srv, "CreateRole", map[string]any{"RoleName": "myrole"})
	iamRequest(t, srv, "AttachRolePolicy", map[string]any{
		"RoleName":  "myrole",
		"PolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
	})

	resp := iamRequest(t, srv, "DeleteRole", map[string]any{"RoleName": "myrole"})
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
}

func TestIAMPlugin_ListRoles(t *testing.T) {
	srv := newIAMTestServer(t)
	for _, name := range []string{"role-a", "role-b"} {
		iamRequest(t, srv, "CreateRole", map[string]any{"RoleName": name})
	}
	resp := iamRequest(t, srv, "ListRoles", map[string]any{})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var result map[string]any
	decodeJSON(t, resp, &result)
	assert.Len(t, result["Roles"].([]any), 2)
}

// --- Group tests -----------------------------------------------------------

func TestIAMPlugin_CreateGroup_GetGroup(t *testing.T) {
	srv := newIAMTestServer(t)
	resp := iamRequest(t, srv, "CreateGroup", map[string]any{"GroupName": "devs"})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var create map[string]any
	decodeJSON(t, resp, &create)
	assert.Equal(t, "devs", create["Group"].(map[string]any)["GroupName"])

	resp2 := iamRequest(t, srv, "GetGroup", map[string]any{"GroupName": "devs"})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
}

func TestIAMPlugin_DeleteGroup_NotFound(t *testing.T) {
	srv := newIAMTestServer(t)
	resp := iamRequest(t, srv, "DeleteGroup", map[string]any{"GroupName": "nobody"})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// --- Policy attachment tests -----------------------------------------------

func TestIAMPlugin_AttachDetachUserPolicy(t *testing.T) {
	srv := newIAMTestServer(t)
	iamRequest(t, srv, "CreateUser", map[string]any{"UserName": "alice"})

	// Attach.
	resp := iamRequest(t, srv, "AttachUserPolicy", map[string]any{
		"UserName":  "alice",
		"PolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// List.
	resp2 := iamRequest(t, srv, "ListAttachedUserPolicies", map[string]any{"UserName": "alice"})
	var listResult map[string]any
	decodeJSON(t, resp2, &listResult)
	attached := listResult["AttachedPolicies"].([]any)
	require.Len(t, attached, 1)
	assert.Equal(t, "arn:aws:iam::aws:policy/ReadOnlyAccess",
		attached[0].(map[string]any)["PolicyArn"])

	// Detach.
	resp3 := iamRequest(t, srv, "DetachUserPolicy", map[string]any{
		"UserName":  "alice",
		"PolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
	})
	assert.Equal(t, http.StatusOK, resp3.StatusCode)

	// List again — should be empty.
	resp4 := iamRequest(t, srv, "ListAttachedUserPolicies", map[string]any{"UserName": "alice"})
	var listResult2 map[string]any
	decodeJSON(t, resp4, &listResult2)
	assert.Empty(t, listResult2["AttachedPolicies"].([]any))
}

func TestIAMPlugin_AttachUserPolicy_Idempotent(t *testing.T) {
	srv := newIAMTestServer(t)
	iamRequest(t, srv, "CreateUser", map[string]any{"UserName": "alice"})

	arn := "arn:aws:iam::aws:policy/ReadOnlyAccess"
	iamRequest(t, srv, "AttachUserPolicy", map[string]any{"UserName": "alice", "PolicyArn": arn})
	iamRequest(t, srv, "AttachUserPolicy", map[string]any{"UserName": "alice", "PolicyArn": arn})

	resp := iamRequest(t, srv, "ListAttachedUserPolicies", map[string]any{"UserName": "alice"})
	var result map[string]any
	decodeJSON(t, resp, &result)
	// No duplicates.
	assert.Len(t, result["AttachedPolicies"].([]any), 1)
}

func TestIAMPlugin_DetachUserPolicy_NotAttached(t *testing.T) {
	srv := newIAMTestServer(t)
	iamRequest(t, srv, "CreateUser", map[string]any{"UserName": "alice"})

	resp := iamRequest(t, srv, "DetachUserPolicy", map[string]any{
		"UserName":  "alice",
		"PolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
	})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestIAMPlugin_AttachDetachRolePolicy(t *testing.T) {
	srv := newIAMTestServer(t)
	iamRequest(t, srv, "CreateRole", map[string]any{"RoleName": "myrole"})

	iamRequest(t, srv, "AttachRolePolicy", map[string]any{
		"RoleName":  "myrole",
		"PolicyArn": "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
	})

	resp := iamRequest(t, srv, "ListAttachedRolePolicies", map[string]any{"RoleName": "myrole"})
	var result map[string]any
	decodeJSON(t, resp, &result)
	attached := result["AttachedPolicies"].([]any)
	require.Len(t, attached, 1)
	assert.Equal(t, "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
		attached[0].(map[string]any)["PolicyArn"])
}

// --- Policy CRUD tests -----------------------------------------------------

func TestIAMPlugin_CreateGetPolicy(t *testing.T) {
	srv := newIAMTestServer(t)
	policyDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`

	resp := iamRequest(t, srv, "CreatePolicy", map[string]any{
		"PolicyName":     "my-s3-policy",
		"PolicyDocument": policyDoc,
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var createResult map[string]any
	decodeJSON(t, resp, &createResult)
	pol := createResult["Policy"].(map[string]any)
	arn := pol["Arn"].(string)
	assert.Contains(t, arn, ":policy/my-s3-policy")

	// Get it back.
	resp2 := iamRequest(t, srv, "GetPolicy", map[string]any{"PolicyArn": arn})
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
}

func TestIAMPlugin_GetPolicy_Managed(t *testing.T) {
	srv := newIAMTestServer(t)
	resp := iamRequest(t, srv, "GetPolicy", map[string]any{
		"PolicyArn": "arn:aws:iam::aws:policy/AdministratorAccess",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result map[string]any
	decodeJSON(t, resp, &result)
	pol := result["Policy"].(map[string]any)
	assert.Equal(t, "AdministratorAccess", pol["PolicyName"])
}

func TestIAMPlugin_DeletePolicy(t *testing.T) {
	srv := newIAMTestServer(t)
	iamRequest(t, srv, "CreatePolicy", map[string]any{"PolicyName": "tmp-policy"})

	// Get ARN from list.
	listResp := iamRequest(t, srv, "ListPolicies", map[string]any{})
	var listResult map[string]any
	decodeJSON(t, listResp, &listResult)
	policies := listResult["Policies"].([]any)
	require.Len(t, policies, 1)
	arn := policies[0].(map[string]any)["Arn"].(string)

	resp := iamRequest(t, srv, "DeletePolicy", map[string]any{"PolicyArn": arn})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp2 := iamRequest(t, srv, "GetPolicy", map[string]any{"PolicyArn": arn})
	assert.Equal(t, http.StatusNotFound, resp2.StatusCode)
}

// --- Access key tests ------------------------------------------------------

func TestIAMPlugin_CreateListDeleteAccessKey(t *testing.T) {
	srv := newIAMTestServer(t)
	iamRequest(t, srv, "CreateUser", map[string]any{"UserName": "alice"})

	// Create.
	resp := iamRequest(t, srv, "CreateAccessKey", map[string]any{"UserName": "alice"})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var createResult map[string]any
	decodeJSON(t, resp, &createResult)
	key := createResult["AccessKey"].(map[string]any)
	keyID := key["AccessKeyId"].(string)
	assert.True(t, len(keyID) == 21)
	assert.NotEmpty(t, key["SecretAccessKey"])
	assert.Equal(t, "Active", key["Status"])

	// List.
	listResp := iamRequest(t, srv, "ListAccessKeys", map[string]any{"UserName": "alice"})
	var listResult map[string]any
	decodeJSON(t, listResp, &listResult)
	metadata := listResult["AccessKeyMetadata"].([]any)
	require.Len(t, metadata, 1)
	// Secret not included in list response.
	_, hasSecret := metadata[0].(map[string]any)["SecretAccessKey"]
	assert.False(t, hasSecret)

	// Delete.
	delResp := iamRequest(t, srv, "DeleteAccessKey", map[string]any{
		"AccessKeyId": keyID,
		"UserName":    "alice",
	})
	assert.Equal(t, http.StatusOK, delResp.StatusCode)

	// List again — empty.
	listResp2 := iamRequest(t, srv, "ListAccessKeys", map[string]any{"UserName": "alice"})
	var listResult2 map[string]any
	decodeJSON(t, listResp2, &listResult2)
	assert.Empty(t, listResult2["AccessKeyMetadata"].([]any))
}

// --- ListGroups test -------------------------------------------------------

func TestIAMPlugin_ListGroups(t *testing.T) {
	srv := newIAMTestServer(t)
	for _, name := range []string{"devs", "ops", "qa"} {
		iamRequest(t, srv, "CreateGroup", map[string]any{"GroupName": name})
	}
	resp := iamRequest(t, srv, "ListGroups", map[string]any{})
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var result map[string]any
	decodeJSON(t, resp, &result)
	assert.Len(t, result["Groups"].([]any), 3)
	assert.False(t, result["IsTruncated"].(bool))
}

func TestIAMPlugin_ListGroups_Pagination(t *testing.T) {
	srv := newIAMTestServer(t)
	for _, name := range []string{"g1", "g2", "g3"} {
		iamRequest(t, srv, "CreateGroup", map[string]any{"GroupName": name})
	}
	resp := iamRequest(t, srv, "ListGroups", map[string]any{"MaxItems": 2})
	var result map[string]any
	decodeJSON(t, resp, &result)
	assert.True(t, result["IsTruncated"].(bool))
	assert.Len(t, result["Groups"].([]any), 2)
}

// --- DetachRolePolicy test -------------------------------------------------

func TestIAMPlugin_DetachRolePolicy(t *testing.T) {
	srv := newIAMTestServer(t)
	iamRequest(t, srv, "CreateRole", map[string]any{"RoleName": "myrole"})
	iamRequest(t, srv, "AttachRolePolicy", map[string]any{
		"RoleName":  "myrole",
		"PolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
	})

	resp := iamRequest(t, srv, "DetachRolePolicy", map[string]any{
		"RoleName":  "myrole",
		"PolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
	})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify removed.
	listResp := iamRequest(t, srv, "ListAttachedRolePolicies", map[string]any{"RoleName": "myrole"})
	var listResult map[string]any
	decodeJSON(t, listResp, &listResult)
	assert.Empty(t, listResult["AttachedPolicies"].([]any))
}

func TestIAMPlugin_DetachRolePolicy_NotAttached(t *testing.T) {
	srv := newIAMTestServer(t)
	iamRequest(t, srv, "CreateRole", map[string]any{"RoleName": "myrole"})

	resp := iamRequest(t, srv, "DetachRolePolicy", map[string]any{
		"RoleName":  "myrole",
		"PolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
	})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// --- CreatePolicy duplicate test ------------------------------------------

func TestIAMPlugin_CreatePolicy_Duplicate(t *testing.T) {
	srv := newIAMTestServer(t)
	iamRequest(t, srv, "CreatePolicy", map[string]any{"PolicyName": "my-policy"})
	resp := iamRequest(t, srv, "CreatePolicy", map[string]any{"PolicyName": "my-policy"})
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
}

func TestIAMPlugin_DeletePolicy_NotFound(t *testing.T) {
	srv := newIAMTestServer(t)
	resp := iamRequest(t, srv, "DeletePolicy", map[string]any{
		"PolicyArn": "arn:aws:iam::000000000000:policy/nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// --- CreateRole with bad policy document -----------------------------------

func TestIAMPlugin_CreateRole_BadPolicyDocument(t *testing.T) {
	srv := newIAMTestServer(t)
	resp := iamRequest(t, srv, "CreateRole", map[string]any{
		"RoleName":                 "bad-role",
		"AssumeRolePolicyDocument": "not-valid-json",
	})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// --- GetGroup not found ---------------------------------------------------

func TestIAMPlugin_GetGroup_NotFound(t *testing.T) {
	srv := newIAMTestServer(t)
	resp := iamRequest(t, srv, "GetGroup", map[string]any{"GroupName": "nobody"})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// --- ListRoles pagination --------------------------------------------------

func TestIAMPlugin_ListRoles_Pagination(t *testing.T) {
	srv := newIAMTestServer(t)
	for _, name := range []string{"role-a", "role-b", "role-c"} {
		iamRequest(t, srv, "CreateRole", map[string]any{"RoleName": name})
	}
	resp := iamRequest(t, srv, "ListRoles", map[string]any{"MaxItems": 2})
	var result map[string]any
	decodeJSON(t, resp, &result)
	assert.True(t, result["IsTruncated"].(bool))
}

// --- GetUser with caller identity -----------------------------------------

func TestIAMPlugin_GetUser_EmptyBody(t *testing.T) {
	srv := newIAMTestServer(t)
	resp := iamRequest(t, srv, "GetUser", map[string]any{"UserName": ""})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// --- DeleteAccessKey not found --------------------------------------------

func TestIAMPlugin_DeleteAccessKey_NotFound(t *testing.T) {
	srv := newIAMTestServer(t)
	resp := iamRequest(t, srv, "DeleteAccessKey", map[string]any{
		"AccessKeyId": "AKIAZZZZZZZZZZZZZZZZ",
	})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// --- DeleteRole success ---------------------------------------------------

func TestIAMPlugin_DeleteRole_Success(t *testing.T) {
	srv := newIAMTestServer(t)
	iamRequest(t, srv, "CreateRole", map[string]any{"RoleName": "myrole"})

	resp := iamRequest(t, srv, "DeleteRole", map[string]any{"RoleName": "myrole"})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp2 := iamRequest(t, srv, "GetRole", map[string]any{"RoleName": "myrole"})
	assert.Equal(t, http.StatusNotFound, resp2.StatusCode)
}

func TestIAMPlugin_DeleteRole_NotFound(t *testing.T) {
	srv := newIAMTestServer(t)
	resp := iamRequest(t, srv, "DeleteRole", map[string]any{"RoleName": "nobody"})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// --- DeleteGroup success --------------------------------------------------

func TestIAMPlugin_DeleteGroup_Success(t *testing.T) {
	srv := newIAMTestServer(t)
	iamRequest(t, srv, "CreateGroup", map[string]any{"GroupName": "devs"})

	resp := iamRequest(t, srv, "DeleteGroup", map[string]any{"GroupName": "devs"})
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp2 := iamRequest(t, srv, "GetGroup", map[string]any{"GroupName": "devs"})
	assert.Equal(t, http.StatusNotFound, resp2.StatusCode)
}

// --- ListAccessKeys missing UserName -------------------------------------

func TestIAMPlugin_ListAccessKeys_MissingUserName(t *testing.T) {
	srv := newIAMTestServer(t)
	resp := iamRequest(t, srv, "ListAccessKeys", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// --- CreateAccessKey missing user ----------------------------------------

func TestIAMPlugin_CreateAccessKey_UserNotFound(t *testing.T) {
	srv := newIAMTestServer(t)
	resp := iamRequest(t, srv, "CreateAccessKey", map[string]any{"UserName": "nobody"})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// --- AttachRolePolicy role not found -------------------------------------

func TestIAMPlugin_AttachRolePolicy_RoleNotFound(t *testing.T) {
	srv := newIAMTestServer(t)
	resp := iamRequest(t, srv, "AttachRolePolicy", map[string]any{
		"RoleName":  "nonexistent",
		"PolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
	})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// --- CreateUser not found user for policy attach --------------------------

func TestIAMPlugin_AttachUserPolicy_UserNotFound(t *testing.T) {
	srv := newIAMTestServer(t)
	resp := iamRequest(t, srv, "AttachUserPolicy", map[string]any{
		"UserName":  "nobody",
		"PolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
	})
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// --- Invalid JSON body test -----------------------------------------------

func TestIAMPlugin_InvalidJSONBody(t *testing.T) {
	srv := newIAMTestServer(t)

	// Send invalid JSON to CreateUser.
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`not-valid-json`))
	r.Host = "iam.amazonaws.com"
	r.Header.Set("X-Amz-Target", "AmazonIdentityManagementService.CreateUser")
	r.Header.Set("Content-Type", "application/x-amz-json-1.1")
	w := httptest.NewRecorder()
	srv.ServeHTTP(w, r)

	resp := w.Result()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// --- Shutdown methods -----------------------------------------------------

func TestIAMPlugin_Shutdown(t *testing.T) {
	plugin := &substrate.IAMPlugin{}
	logger := substrate.NewDefaultLogger(0, false)
	state := substrate.NewMemoryStateManager()
	require.NoError(t, plugin.Initialize(context.TODO(), substrate.PluginConfig{State: state, Logger: logger}))
	require.NoError(t, plugin.Shutdown(context.TODO()))
}

func TestSTSPlugin_Shutdown(t *testing.T) {
	plugin := &substrate.STSPlugin{}
	logger := substrate.NewDefaultLogger(0, false)
	state := substrate.NewMemoryStateManager()
	require.NoError(t, plugin.Initialize(context.TODO(), substrate.PluginConfig{State: state, Logger: logger}))
	require.NoError(t, plugin.Shutdown(context.TODO()))
}

// --- Unknown operation test -----------------------------------------------

func TestIAMPlugin_UnknownOperation(t *testing.T) {
	srv := newIAMTestServer(t)
	resp := iamRequest(t, srv, "DoSomethingWeird", nil)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	var result map[string]string
	decodeJSON(t, resp, &result)
	assert.Equal(t, "InvalidAction", result["__type"])
}
