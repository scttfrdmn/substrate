package substrate_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

func setupCognitoIDPPlugin(t *testing.T) (*substrate.CognitoIDPPlugin, *substrate.RequestContext) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := &substrate.CognitoIDPPlugin{}
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  substrate.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("substrate.CognitoIDPPlugin.Initialize: %v", err)
	}
	return p, &substrate.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: "req-1"}
}

func setupCognitoIdentityPlugin(t *testing.T) (*substrate.CognitoIdentityPlugin, *substrate.RequestContext) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := &substrate.CognitoIdentityPlugin{}
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  substrate.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("substrate.CognitoIdentityPlugin.Initialize: %v", err)
	}
	return p, &substrate.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: "req-1"}
}

func cognitoIDPRequest(t *testing.T, op string, body map[string]any) *substrate.AWSRequest {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal cognito-idp request body: %v", err)
	}
	return &substrate.AWSRequest{
		Service:   "cognito-idp",
		Operation: op,
		Headers:   map[string]string{"X-Amz-Target": "AWSCognitoIdentityProviderService." + op},
		Body:      b,
		Params:    map[string]string{},
	}
}

func cognitoIdentityRequest(t *testing.T, op string, body map[string]any) *substrate.AWSRequest {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal cognito-identity request body: %v", err)
	}
	return &substrate.AWSRequest{
		Service:   "cognito-identity",
		Operation: op,
		Headers:   map[string]string{"X-Amz-Target": "AWSCognitoIdentityService." + op},
		Body:      b,
		Params:    map[string]string{},
	}
}

// TestCognitoIDP_CreateAndDescribeUserPool verifies creating and describing a
// user pool, checking the ARN and ProviderName format.
func TestCognitoIDP_CreateAndDescribeUserPool(t *testing.T) {
	p, ctx := setupCognitoIDPPlugin(t)

	// Create user pool.
	req := cognitoIDPRequest(t, "CreateUserPool", map[string]any{
		"PoolName": "test-pool",
	})
	resp, err := p.HandleRequest(ctx, req)
	if err != nil {
		t.Fatalf("CreateUserPool: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateUserPool: want 200, got %d", resp.StatusCode)
	}

	var createOut struct {
		UserPool struct {
			UserPoolID   string `json:"UserPoolId"`
			Name         string `json:"Name"`
			Arn          string `json:"Arn"`
			ProviderName string `json:"ProviderName"`
			Status       string `json:"Status"`
		} `json:"UserPool"`
	}
	if err := json.Unmarshal(resp.Body, &createOut); err != nil {
		t.Fatalf("unmarshal CreateUserPool response: %v", err)
	}

	poolID := createOut.UserPool.UserPoolID
	if poolID == "" {
		t.Fatal("UserPoolId is empty")
	}
	// Pool ID must be in the format {region}_{12-char alphanum}.
	parts := strings.SplitN(poolID, "_", 2)
	if len(parts) != 2 || parts[0] != "us-east-1" {
		t.Errorf("UserPoolId %q does not match expected format {region}_{id}", poolID)
	}
	if len(parts[1]) != 12 {
		t.Errorf("UserPoolId suffix %q should be 12 chars, got %d", parts[1], len(parts[1]))
	}

	// Verify ARN format.
	wantARNPrefix := "arn:aws:cognito-idp:us-east-1:123456789012:userpool/"
	if !strings.HasPrefix(createOut.UserPool.Arn, wantARNPrefix) {
		t.Errorf("Arn %q should start with %q", createOut.UserPool.Arn, wantARNPrefix)
	}

	// Verify ProviderName format.
	wantProviderName := "cognito-idp.us-east-1.amazonaws.com/" + poolID
	if createOut.UserPool.ProviderName != wantProviderName {
		t.Errorf("ProviderName = %q, want %q", createOut.UserPool.ProviderName, wantProviderName)
	}

	if createOut.UserPool.Status != "Enabled" {
		t.Errorf("Status = %q, want Enabled", createOut.UserPool.Status)
	}

	// Describe the pool.
	descReq := cognitoIDPRequest(t, "DescribeUserPool", map[string]any{
		"UserPoolId": poolID,
	})
	descResp, err := p.HandleRequest(ctx, descReq)
	if err != nil {
		t.Fatalf("DescribeUserPool: %v", err)
	}
	if descResp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeUserPool: want 200, got %d", descResp.StatusCode)
	}

	var descOut struct {
		UserPool struct {
			UserPoolID string `json:"UserPoolId"`
			Name       string `json:"Name"`
		} `json:"UserPool"`
	}
	if err := json.Unmarshal(descResp.Body, &descOut); err != nil {
		t.Fatalf("unmarshal DescribeUserPool: %v", err)
	}
	if descOut.UserPool.UserPoolID != poolID {
		t.Errorf("DescribeUserPool UserPoolId = %q, want %q", descOut.UserPool.UserPoolID, poolID)
	}
	if descOut.UserPool.Name != "test-pool" {
		t.Errorf("DescribeUserPool Name = %q, want test-pool", descOut.UserPool.Name)
	}
}

// TestCognitoIDP_UserPoolClient verifies creating, describing, and listing
// app clients within a user pool.
func TestCognitoIDP_UserPoolClient(t *testing.T) {
	p, ctx := setupCognitoIDPPlugin(t)

	// Create pool first.
	createPool := cognitoIDPRequest(t, "CreateUserPool", map[string]any{"PoolName": "client-test-pool"})
	poolResp, err := p.HandleRequest(ctx, createPool)
	if err != nil {
		t.Fatalf("CreateUserPool: %v", err)
	}
	var poolOut struct {
		UserPool struct {
			UserPoolID string `json:"UserPoolId"`
		} `json:"UserPool"`
	}
	if err := json.Unmarshal(poolResp.Body, &poolOut); err != nil {
		t.Fatalf("unmarshal pool: %v", err)
	}
	poolID := poolOut.UserPool.UserPoolID

	// Create client.
	createClient := cognitoIDPRequest(t, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": "my-app-client",
	})
	clientResp, err := p.HandleRequest(ctx, createClient)
	if err != nil {
		t.Fatalf("CreateUserPoolClient: %v", err)
	}
	if clientResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateUserPoolClient: want 200, got %d", clientResp.StatusCode)
	}

	var clientOut struct {
		UserPoolClient struct {
			ClientID   string `json:"ClientId"`
			ClientName string `json:"ClientName"`
			UserPoolID string `json:"UserPoolId"`
		} `json:"UserPoolClient"`
	}
	if err := json.Unmarshal(clientResp.Body, &clientOut); err != nil {
		t.Fatalf("unmarshal CreateUserPoolClient: %v", err)
	}
	clientID := clientOut.UserPoolClient.ClientID
	if clientID == "" {
		t.Fatal("ClientId is empty")
	}
	if clientOut.UserPoolClient.ClientName != "my-app-client" {
		t.Errorf("ClientName = %q, want my-app-client", clientOut.UserPoolClient.ClientName)
	}
	if clientOut.UserPoolClient.UserPoolID != poolID {
		t.Errorf("UserPoolId = %q, want %q", clientOut.UserPoolClient.UserPoolID, poolID)
	}

	// Describe client.
	descClient := cognitoIDPRequest(t, "DescribeUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	})
	descResp, err := p.HandleRequest(ctx, descClient)
	if err != nil {
		t.Fatalf("DescribeUserPoolClient: %v", err)
	}
	if descResp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeUserPoolClient: want 200, got %d", descResp.StatusCode)
	}
	var descOut struct {
		UserPoolClient struct {
			ClientID string `json:"ClientId"`
		} `json:"UserPoolClient"`
	}
	if err := json.Unmarshal(descResp.Body, &descOut); err != nil {
		t.Fatalf("unmarshal DescribeUserPoolClient: %v", err)
	}
	if descOut.UserPoolClient.ClientID != clientID {
		t.Errorf("ClientId = %q, want %q", descOut.UserPoolClient.ClientID, clientID)
	}

	// List clients.
	listClients := cognitoIDPRequest(t, "ListUserPoolClients", map[string]any{
		"UserPoolId": poolID,
	})
	listResp, err := p.HandleRequest(ctx, listClients)
	if err != nil {
		t.Fatalf("ListUserPoolClients: %v", err)
	}
	var listOut struct {
		UserPoolClients []struct {
			ClientID string `json:"ClientId"`
		} `json:"UserPoolClients"`
	}
	if err := json.Unmarshal(listResp.Body, &listOut); err != nil {
		t.Fatalf("unmarshal ListUserPoolClients: %v", err)
	}
	if len(listOut.UserPoolClients) != 1 {
		t.Errorf("want 1 client, got %d", len(listOut.UserPoolClients))
	}
	if listOut.UserPoolClients[0].ClientID != clientID {
		t.Errorf("listed client ID = %q, want %q", listOut.UserPoolClients[0].ClientID, clientID)
	}
}

// TestCognitoIDP_AdminCreateUser verifies creating a user via admin API and
// retrieving the user.
func TestCognitoIDP_AdminCreateUser(t *testing.T) {
	p, ctx := setupCognitoIDPPlugin(t)

	// Create pool.
	createPool := cognitoIDPRequest(t, "CreateUserPool", map[string]any{"PoolName": "user-test-pool"})
	poolResp, err := p.HandleRequest(ctx, createPool)
	if err != nil {
		t.Fatalf("CreateUserPool: %v", err)
	}
	var poolOut struct {
		UserPool struct {
			UserPoolID string `json:"UserPoolId"`
		} `json:"UserPool"`
	}
	if err := json.Unmarshal(poolResp.Body, &poolOut); err != nil {
		t.Fatalf("unmarshal pool: %v", err)
	}
	poolID := poolOut.UserPool.UserPoolID

	// Admin create user.
	createUser := cognitoIDPRequest(t, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "testuser",
		"TemporaryPassword": "Temp@1234",
		"UserAttributes": []map[string]string{
			{"Name": "email", "Value": "testuser@example.com"},
		},
	})
	userResp, err := p.HandleRequest(ctx, createUser)
	if err != nil {
		t.Fatalf("AdminCreateUser: %v", err)
	}
	if userResp.StatusCode != http.StatusOK {
		t.Fatalf("AdminCreateUser: want 200, got %d", userResp.StatusCode)
	}

	var userOut struct {
		User struct {
			Username   string `json:"Username"`
			UserStatus string `json:"UserStatus"`
			Enabled    bool   `json:"Enabled"`
		} `json:"User"`
	}
	if err := json.Unmarshal(userResp.Body, &userOut); err != nil {
		t.Fatalf("unmarshal AdminCreateUser: %v", err)
	}
	if userOut.User.Username != "testuser" {
		t.Errorf("Username = %q, want testuser", userOut.User.Username)
	}
	if userOut.User.UserStatus != "FORCE_CHANGE_PASSWORD" {
		t.Errorf("UserStatus = %q, want FORCE_CHANGE_PASSWORD", userOut.User.UserStatus)
	}
	if !userOut.User.Enabled {
		t.Error("Enabled should be true")
	}

	// Admin get user.
	getUser := cognitoIDPRequest(t, "AdminGetUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "testuser",
	})
	getResp, err := p.HandleRequest(ctx, getUser)
	if err != nil {
		t.Fatalf("AdminGetUser: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("AdminGetUser: want 200, got %d", getResp.StatusCode)
	}

	var getOut struct {
		Username       string `json:"Username"`
		UserStatus     string `json:"UserStatus"`
		UserAttributes []struct {
			Name  string `json:"Name"`
			Value string `json:"Value"`
		} `json:"UserAttributes"`
	}
	if err := json.Unmarshal(getResp.Body, &getOut); err != nil {
		t.Fatalf("unmarshal AdminGetUser: %v", err)
	}
	if getOut.Username != "testuser" {
		t.Errorf("AdminGetUser Username = %q, want testuser", getOut.Username)
	}
	foundEmail := false
	for _, attr := range getOut.UserAttributes {
		if attr.Name == "email" && attr.Value == "testuser@example.com" {
			foundEmail = true
		}
	}
	if !foundEmail {
		t.Error("expected email attribute not found in AdminGetUser response")
	}
}

// TestCognitoIDP_InitiateAuth verifies that InitiateAuth returns stub JWT tokens.
func TestCognitoIDP_InitiateAuth(t *testing.T) {
	p, ctx := setupCognitoIDPPlugin(t)

	// Create pool and client.
	createPool := cognitoIDPRequest(t, "CreateUserPool", map[string]any{"PoolName": "auth-pool"})
	poolResp, err := p.HandleRequest(ctx, createPool)
	if err != nil {
		t.Fatalf("CreateUserPool: %v", err)
	}
	var poolOut struct {
		UserPool struct {
			UserPoolID string `json:"UserPoolId"`
		} `json:"UserPool"`
	}
	if err := json.Unmarshal(poolResp.Body, &poolOut); err != nil {
		t.Fatalf("unmarshal pool: %v", err)
	}
	poolID := poolOut.UserPool.UserPoolID

	createClient := cognitoIDPRequest(t, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": "auth-client",
	})
	clientResp, err := p.HandleRequest(ctx, createClient)
	if err != nil {
		t.Fatalf("CreateUserPoolClient: %v", err)
	}
	var clientOut struct {
		UserPoolClient struct {
			ClientID string `json:"ClientId"`
		} `json:"UserPoolClient"`
	}
	if err := json.Unmarshal(clientResp.Body, &clientOut); err != nil {
		t.Fatalf("unmarshal client: %v", err)
	}

	// Initiate auth.
	authReq := cognitoIDPRequest(t, "InitiateAuth", map[string]any{
		"AuthFlow": "USER_PASSWORD_AUTH",
		"ClientId": clientOut.UserPoolClient.ClientID,
		"AuthParameters": map[string]string{
			"USERNAME": "testuser",
			"PASSWORD": "Password123!",
		},
	})
	authResp, err := p.HandleRequest(ctx, authReq)
	if err != nil {
		t.Fatalf("InitiateAuth: %v", err)
	}
	if authResp.StatusCode != http.StatusOK {
		t.Fatalf("InitiateAuth: want 200, got %d", authResp.StatusCode)
	}

	var authOut struct {
		AuthenticationResult struct {
			AccessToken  string `json:"AccessToken"`
			IDToken      string `json:"IdToken"`
			RefreshToken string `json:"RefreshToken"`
			ExpiresIn    int    `json:"ExpiresIn"`
			TokenType    string `json:"TokenType"`
		} `json:"AuthenticationResult"`
	}
	if err := json.Unmarshal(authResp.Body, &authOut); err != nil {
		t.Fatalf("unmarshal InitiateAuth: %v", err)
	}
	if authOut.AuthenticationResult.AccessToken == "" {
		t.Error("AccessToken is empty")
	}
	if authOut.AuthenticationResult.IDToken == "" {
		t.Error("IdToken is empty")
	}
	if authOut.AuthenticationResult.ExpiresIn != 3600 {
		t.Errorf("ExpiresIn = %d, want 3600", authOut.AuthenticationResult.ExpiresIn)
	}
	if authOut.AuthenticationResult.TokenType != "Bearer" {
		t.Errorf("TokenType = %q, want Bearer", authOut.AuthenticationResult.TokenType)
	}
}

// TestCognitoIDP_Groups verifies creating, getting, listing, and deleting groups.
func TestCognitoIDP_Groups(t *testing.T) {
	p, ctx := setupCognitoIDPPlugin(t)

	// Create pool.
	createPool := cognitoIDPRequest(t, "CreateUserPool", map[string]any{"PoolName": "group-pool"})
	poolResp, err := p.HandleRequest(ctx, createPool)
	if err != nil {
		t.Fatalf("CreateUserPool: %v", err)
	}
	var poolOut struct {
		UserPool struct {
			UserPoolID string `json:"UserPoolId"`
		} `json:"UserPool"`
	}
	if err := json.Unmarshal(poolResp.Body, &poolOut); err != nil {
		t.Fatalf("unmarshal pool: %v", err)
	}
	poolID := poolOut.UserPool.UserPoolID

	// Create group.
	createGroup := cognitoIDPRequest(t, "CreateGroup", map[string]any{
		"GroupName":   "admins",
		"UserPoolId":  poolID,
		"Description": "Administrator group",
	})
	groupResp, err := p.HandleRequest(ctx, createGroup)
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	if groupResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateGroup: want 200, got %d", groupResp.StatusCode)
	}

	var groupOut struct {
		Group struct {
			GroupName   string `json:"GroupName"`
			UserPoolID  string `json:"UserPoolId"`
			Description string `json:"Description"`
		} `json:"Group"`
	}
	if err := json.Unmarshal(groupResp.Body, &groupOut); err != nil {
		t.Fatalf("unmarshal CreateGroup: %v", err)
	}
	if groupOut.Group.GroupName != "admins" {
		t.Errorf("GroupName = %q, want admins", groupOut.Group.GroupName)
	}
	if groupOut.Group.Description != "Administrator group" {
		t.Errorf("Description = %q, want 'Administrator group'", groupOut.Group.Description)
	}

	// Get group.
	getGroup := cognitoIDPRequest(t, "GetGroup", map[string]any{
		"GroupName":  "admins",
		"UserPoolId": poolID,
	})
	getResp, err := p.HandleRequest(ctx, getGroup)
	if err != nil {
		t.Fatalf("GetGroup: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GetGroup: want 200, got %d", getResp.StatusCode)
	}

	// List groups.
	listGroups := cognitoIDPRequest(t, "ListGroups", map[string]any{
		"UserPoolId": poolID,
	})
	listResp, err := p.HandleRequest(ctx, listGroups)
	if err != nil {
		t.Fatalf("ListGroups: %v", err)
	}
	var listOut struct {
		Groups []struct {
			GroupName string `json:"GroupName"`
		} `json:"Groups"`
	}
	if err := json.Unmarshal(listResp.Body, &listOut); err != nil {
		t.Fatalf("unmarshal ListGroups: %v", err)
	}
	if len(listOut.Groups) != 1 {
		t.Errorf("want 1 group, got %d", len(listOut.Groups))
	}
	if listOut.Groups[0].GroupName != "admins" {
		t.Errorf("group name = %q, want admins", listOut.Groups[0].GroupName)
	}

	// Delete group.
	deleteGroup := cognitoIDPRequest(t, "DeleteGroup", map[string]any{
		"GroupName":  "admins",
		"UserPoolId": poolID,
	})
	delResp, err := p.HandleRequest(ctx, deleteGroup)
	if err != nil {
		t.Fatalf("DeleteGroup: %v", err)
	}
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("DeleteGroup: want 200, got %d", delResp.StatusCode)
	}

	// List groups again — should be empty.
	listResp2, err := p.HandleRequest(ctx, listGroups)
	if err != nil {
		t.Fatalf("ListGroups after delete: %v", err)
	}
	var listOut2 struct {
		Groups []struct {
			GroupName string `json:"GroupName"`
		} `json:"Groups"`
	}
	if err := json.Unmarshal(listResp2.Body, &listOut2); err != nil {
		t.Fatalf("unmarshal ListGroups after delete: %v", err)
	}
	if len(listOut2.Groups) != 0 {
		t.Errorf("want 0 groups after delete, got %d", len(listOut2.Groups))
	}
}

// TestCognitoIdentity_CreateAndDescribeIdentityPool verifies creating and
// describing a Cognito Identity Pool, checking the ID format.
func TestCognitoIdentity_CreateAndDescribeIdentityPool(t *testing.T) {
	p, ctx := setupCognitoIdentityPlugin(t)

	// Create identity pool.
	req := cognitoIdentityRequest(t, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "test-identity-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	resp, err := p.HandleRequest(ctx, req)
	if err != nil {
		t.Fatalf("CreateIdentityPool: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CreateIdentityPool: want 200, got %d", resp.StatusCode)
	}

	var createOut struct {
		IdentityPoolID                 string `json:"IdentityPoolId"`
		IdentityPoolName               string `json:"IdentityPoolName"`
		AllowUnauthenticatedIdentities bool   `json:"AllowUnauthenticatedIdentities"`
	}
	if err := json.Unmarshal(resp.Body, &createOut); err != nil {
		t.Fatalf("unmarshal CreateIdentityPool: %v", err)
	}

	poolID := createOut.IdentityPoolID
	if poolID == "" {
		t.Fatal("IdentityPoolId is empty")
	}

	// ID format must be {region}:{uuid}.
	parts := strings.SplitN(poolID, ":", 2)
	if len(parts) != 2 || parts[0] != "us-east-1" {
		t.Errorf("IdentityPoolId %q does not match expected format {region}:{uuid}", poolID)
	}
	// UUID should be 36 chars: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
	if len(parts[1]) != 36 {
		t.Errorf("IdentityPoolId UUID part %q should be 36 chars, got %d", parts[1], len(parts[1]))
	}

	if createOut.IdentityPoolName != "test-identity-pool" {
		t.Errorf("IdentityPoolName = %q, want test-identity-pool", createOut.IdentityPoolName)
	}
	if !createOut.AllowUnauthenticatedIdentities {
		t.Error("AllowUnauthenticatedIdentities should be true")
	}

	// Describe the pool.
	descReq := cognitoIdentityRequest(t, "DescribeIdentityPool", map[string]any{
		"IdentityPoolId": poolID,
	})
	descResp, err := p.HandleRequest(ctx, descReq)
	if err != nil {
		t.Fatalf("DescribeIdentityPool: %v", err)
	}
	if descResp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeIdentityPool: want 200, got %d", descResp.StatusCode)
	}

	var descOut struct {
		IdentityPoolID   string `json:"IdentityPoolId"`
		IdentityPoolName string `json:"IdentityPoolName"`
	}
	if err := json.Unmarshal(descResp.Body, &descOut); err != nil {
		t.Fatalf("unmarshal DescribeIdentityPool: %v", err)
	}
	if descOut.IdentityPoolID != poolID {
		t.Errorf("DescribeIdentityPool IdentityPoolId = %q, want %q", descOut.IdentityPoolID, poolID)
	}
	if descOut.IdentityPoolName != "test-identity-pool" {
		t.Errorf("DescribeIdentityPool Name = %q, want test-identity-pool", descOut.IdentityPoolName)
	}
}

// TestCognitoIdentity_GetCredentialsForIdentity verifies the GetId and
// GetCredentialsForIdentity stubs return valid-looking responses.
func TestCognitoIdentity_GetCredentialsForIdentity(t *testing.T) {
	p, ctx := setupCognitoIdentityPlugin(t)

	// Create an identity pool first.
	createReq := cognitoIdentityRequest(t, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "creds-pool",
		"AllowUnauthenticatedIdentities": true,
	})
	createResp, err := p.HandleRequest(ctx, createReq)
	if err != nil {
		t.Fatalf("CreateIdentityPool: %v", err)
	}
	var createOut struct {
		IdentityPoolID string `json:"IdentityPoolId"`
	}
	if err := json.Unmarshal(createResp.Body, &createOut); err != nil {
		t.Fatalf("unmarshal CreateIdentityPool: %v", err)
	}

	// Get an identity ID.
	getIDReq := cognitoIdentityRequest(t, "GetId", map[string]any{
		"AccountId":      "123456789012",
		"IdentityPoolId": createOut.IdentityPoolID,
	})
	getIDResp, err := p.HandleRequest(ctx, getIDReq)
	if err != nil {
		t.Fatalf("GetId: %v", err)
	}
	if getIDResp.StatusCode != http.StatusOK {
		t.Fatalf("GetId: want 200, got %d", getIDResp.StatusCode)
	}

	var idOut struct {
		IdentityID string `json:"IdentityId"`
	}
	if err := json.Unmarshal(getIDResp.Body, &idOut); err != nil {
		t.Fatalf("unmarshal GetId: %v", err)
	}
	if idOut.IdentityID == "" {
		t.Fatal("IdentityId is empty")
	}
	// IdentityId format: {region}:{uuid}.
	if !strings.HasPrefix(idOut.IdentityID, "us-east-1:") {
		t.Errorf("IdentityId %q should start with us-east-1:", idOut.IdentityID)
	}

	// Get credentials for identity.
	credsReq := cognitoIdentityRequest(t, "GetCredentialsForIdentity", map[string]any{
		"IdentityId": idOut.IdentityID,
	})
	credsResp, err := p.HandleRequest(ctx, credsReq)
	if err != nil {
		t.Fatalf("GetCredentialsForIdentity: %v", err)
	}
	if credsResp.StatusCode != http.StatusOK {
		t.Fatalf("GetCredentialsForIdentity: want 200, got %d", credsResp.StatusCode)
	}

	var credsOut struct {
		IdentityID  string `json:"IdentityId"`
		Credentials struct {
			AccessKeyID  string `json:"AccessKeyId"`
			SecretKey    string `json:"SecretKey"`
			SessionToken string `json:"SessionToken"`
		} `json:"Credentials"`
	}
	if err := json.Unmarshal(credsResp.Body, &credsOut); err != nil {
		t.Fatalf("unmarshal GetCredentialsForIdentity: %v", err)
	}
	if credsOut.IdentityID == "" {
		t.Error("GetCredentialsForIdentity IdentityId is empty")
	}
	if credsOut.Credentials.AccessKeyID == "" {
		t.Error("AccessKeyId is empty")
	}
	if credsOut.Credentials.SecretKey == "" {
		t.Error("SecretKey is empty")
	}
	if credsOut.Credentials.SessionToken == "" {
		t.Error("SessionToken is empty")
	}
}

// createCognitoPool creates a user pool and returns its ID.
func createCognitoPool(t *testing.T, p *substrate.CognitoIDPPlugin, ctx *substrate.RequestContext, name string) string {
	t.Helper()
	resp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "CreateUserPool", map[string]any{"PoolName": name}))
	if err != nil {
		t.Fatalf("CreateUserPool: %v", err)
	}
	var out struct {
		UserPool struct {
			UserPoolID string `json:"UserPoolId"`
		} `json:"UserPool"`
	}
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		t.Fatalf("unmarshal pool: %v", err)
	}
	return out.UserPool.UserPoolID
}

func TestCognitoIDP_UpdateUserPool(t *testing.T) {
	p, ctx := setupCognitoIDPPlugin(t)
	poolID := createCognitoPool(t, p, ctx, "update-pool")

	resp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "UpdateUserPool", map[string]any{
		"UserPoolId": poolID,
		"Policies": map[string]any{
			"PasswordPolicy": map[string]any{"MinimumLength": 12},
		},
	}))
	if err != nil {
		t.Fatalf("UpdateUserPool: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", resp.StatusCode)
	}
}

func TestCognitoIDP_DeleteUserPool(t *testing.T) {
	p, ctx := setupCognitoIDPPlugin(t)
	poolID := createCognitoPool(t, p, ctx, "delete-pool")

	resp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "DeleteUserPool", map[string]any{
		"UserPoolId": poolID,
	}))
	if err != nil {
		t.Fatalf("DeleteUserPool: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", resp.StatusCode)
	}

	// Describing deleted pool should fail.
	_, err = p.HandleRequest(ctx, cognitoIDPRequest(t, "DescribeUserPool", map[string]any{
		"UserPoolId": poolID,
	}))
	if err == nil {
		t.Error("expected error for deleted pool, got nil")
	}
}

func TestCognitoIDP_ListUserPools(t *testing.T) {
	p, ctx := setupCognitoIDPPlugin(t)

	for _, name := range []string{"pool-a", "pool-b"} {
		createCognitoPool(t, p, ctx, name)
	}

	resp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "ListUserPools", map[string]any{
		"MaxResults": 10,
	}))
	if err != nil {
		t.Fatalf("ListUserPools: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", resp.StatusCode)
	}
	var out struct {
		UserPools []struct {
			UserPoolID string `json:"UserPoolId"`
		} `json:"UserPools"`
	}
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(out.UserPools) != 2 {
		t.Errorf("want 2 pools, got %d", len(out.UserPools))
	}
}

func TestCognitoIDP_DeleteUserPoolClient(t *testing.T) {
	p, ctx := setupCognitoIDPPlugin(t)
	poolID := createCognitoPool(t, p, ctx, "client-del-pool")

	// Create client.
	createResp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": "del-client",
	}))
	if err != nil {
		t.Fatalf("CreateUserPoolClient: %v", err)
	}
	var clientOut struct {
		UserPoolClient struct {
			ClientID string `json:"ClientId"`
		} `json:"UserPoolClient"`
	}
	if err := json.Unmarshal(createResp.Body, &clientOut); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	clientID := clientOut.UserPoolClient.ClientID

	// Delete client.
	delResp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "DeleteUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
	}))
	if err != nil {
		t.Fatalf("DeleteUserPoolClient: %v", err)
	}
	if delResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", delResp.StatusCode)
	}
}

func TestCognitoIDP_UserPoolDomain(t *testing.T) {
	p, ctx := setupCognitoIDPPlugin(t)
	poolID := createCognitoPool(t, p, ctx, "domain-pool")

	// CreateUserPoolDomain.
	createResp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "CreateUserPoolDomain", map[string]any{
		"UserPoolId": poolID,
		"Domain":     "my-auth-domain",
	}))
	if err != nil {
		t.Fatalf("CreateUserPoolDomain: %v", err)
	}
	if createResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", createResp.StatusCode)
	}

	// DescribeUserPoolDomain.
	descResp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "DescribeUserPoolDomain", map[string]any{
		"Domain": "my-auth-domain",
	}))
	if err != nil {
		t.Fatalf("DescribeUserPoolDomain: %v", err)
	}
	if descResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", descResp.StatusCode)
	}

	// DeleteUserPoolDomain.
	delResp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "DeleteUserPoolDomain", map[string]any{
		"UserPoolId": poolID,
		"Domain":     "my-auth-domain",
	}))
	if err != nil {
		t.Fatalf("DeleteUserPoolDomain: %v", err)
	}
	if delResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", delResp.StatusCode)
	}
}

func TestCognitoIDP_AdminSetUserPassword(t *testing.T) {
	p, ctx := setupCognitoIDPPlugin(t)
	poolID := createCognitoPool(t, p, ctx, "pwd-pool")

	// Create user.
	_, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "AdminCreateUser", map[string]any{
		"UserPoolId":        poolID,
		"Username":          "pwduser",
		"TemporaryPassword": "Temp@1234",
	}))
	if err != nil {
		t.Fatalf("AdminCreateUser: %v", err)
	}

	// Set permanent password.
	resp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "AdminSetUserPassword", map[string]any{
		"UserPoolId": poolID,
		"Username":   "pwduser",
		"Password":   "Permanent@5678",
		"Permanent":  true,
	}))
	if err != nil {
		t.Fatalf("AdminSetUserPassword: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", resp.StatusCode)
	}
}

func TestCognitoIDP_AdminDeleteUser(t *testing.T) {
	p, ctx := setupCognitoIDPPlugin(t)
	poolID := createCognitoPool(t, p, ctx, "del-user-pool")

	_, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "AdminCreateUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "todeleteuser",
	}))
	if err != nil {
		t.Fatalf("AdminCreateUser: %v", err)
	}

	delResp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "AdminDeleteUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "todeleteuser",
	}))
	if err != nil {
		t.Fatalf("AdminDeleteUser: %v", err)
	}
	if delResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", delResp.StatusCode)
	}
}

func TestCognitoIDP_ListUsers(t *testing.T) {
	p, ctx := setupCognitoIDPPlugin(t)
	poolID := createCognitoPool(t, p, ctx, "list-users-pool")

	for _, name := range []string{"alice", "bob", "carol"} {
		_, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "AdminCreateUser", map[string]any{
			"UserPoolId": poolID,
			"Username":   name,
		}))
		if err != nil {
			t.Fatalf("AdminCreateUser %s: %v", name, err)
		}
	}

	resp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "ListUsers", map[string]any{
		"UserPoolId": poolID,
	}))
	if err != nil {
		t.Fatalf("ListUsers: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", resp.StatusCode)
	}
	var out struct {
		Users []struct {
			Username string `json:"Username"`
		} `json:"Users"`
	}
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(out.Users) != 3 {
		t.Errorf("want 3 users, got %d", len(out.Users))
	}
}

func TestCognitoIDP_GroupMembership(t *testing.T) {
	p, ctx := setupCognitoIDPPlugin(t)
	poolID := createCognitoPool(t, p, ctx, "grp-member-pool")

	// Create user and group.
	_, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "AdminCreateUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "groupuser",
	}))
	if err != nil {
		t.Fatalf("AdminCreateUser: %v", err)
	}
	_, err = p.HandleRequest(ctx, cognitoIDPRequest(t, "CreateGroup", map[string]any{
		"UserPoolId": poolID,
		"GroupName":  "mygroup",
	}))
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}

	// AdminAddUserToGroup.
	addResp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "AdminAddUserToGroup", map[string]any{
		"UserPoolId": poolID,
		"Username":   "groupuser",
		"GroupName":  "mygroup",
	}))
	if err != nil {
		t.Fatalf("AdminAddUserToGroup: %v", err)
	}
	if addResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", addResp.StatusCode)
	}

	// AdminListGroupsForUser.
	listResp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "AdminListGroupsForUser", map[string]any{
		"UserPoolId": poolID,
		"Username":   "groupuser",
	}))
	if err != nil {
		t.Fatalf("AdminListGroupsForUser: %v", err)
	}
	if listResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", listResp.StatusCode)
	}
	var listOut struct {
		Groups []struct {
			GroupName string `json:"GroupName"`
		} `json:"Groups"`
	}
	if err := json.Unmarshal(listResp.Body, &listOut); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(listOut.Groups) != 1 || listOut.Groups[0].GroupName != "mygroup" {
		t.Errorf("expected user in mygroup, got %v", listOut.Groups)
	}

	// AdminRemoveUserFromGroup.
	removeResp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "AdminRemoveUserFromGroup", map[string]any{
		"UserPoolId": poolID,
		"Username":   "groupuser",
		"GroupName":  "mygroup",
	}))
	if err != nil {
		t.Fatalf("AdminRemoveUserFromGroup: %v", err)
	}
	if removeResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", removeResp.StatusCode)
	}
}

func TestCognitoIDP_SignUpAndConfirmSignUp(t *testing.T) {
	p, ctx := setupCognitoIDPPlugin(t)
	poolID := createCognitoPool(t, p, ctx, "signup-pool")

	// Create a client.
	clientResp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": "signup-client",
	}))
	if err != nil {
		t.Fatalf("CreateUserPoolClient: %v", err)
	}
	var clientOut struct {
		UserPoolClient struct {
			ClientID string `json:"ClientId"`
		} `json:"UserPoolClient"`
	}
	if err := json.Unmarshal(clientResp.Body, &clientOut); err != nil {
		t.Fatalf("unmarshal client: %v", err)
	}
	clientID := clientOut.UserPoolClient.ClientID

	// SignUp.
	signUpResp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "SignUp", map[string]any{
		"ClientId": clientID,
		"Username": "newuser",
		"Password": "NewPass@123",
		"UserAttributes": []map[string]string{
			{"Name": "email", "Value": "newuser@example.com"},
		},
	}))
	if err != nil {
		t.Fatalf("SignUp: %v", err)
	}
	if signUpResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", signUpResp.StatusCode)
	}
	var signUpOut struct {
		UserSub       string `json:"UserSub"`
		UserConfirmed bool   `json:"UserConfirmed"`
	}
	if err := json.Unmarshal(signUpResp.Body, &signUpOut); err != nil {
		t.Fatalf("unmarshal signup: %v", err)
	}
	if signUpOut.UserSub == "" {
		t.Error("UserSub is empty")
	}

	// ConfirmSignUp.
	confirmResp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "ConfirmSignUp", map[string]any{
		"ClientId":         clientID,
		"Username":         "newuser",
		"ConfirmationCode": "123456",
	}))
	if err != nil {
		t.Fatalf("ConfirmSignUp: %v", err)
	}
	if confirmResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", confirmResp.StatusCode)
	}
}

func TestCognitoIDP_MFAConfig(t *testing.T) {
	p, ctx := setupCognitoIDPPlugin(t)
	poolID := createCognitoPool(t, p, ctx, "mfa-pool")

	// SetUserPoolMfaConfig.
	setResp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "SetUserPoolMfaConfig", map[string]any{
		"UserPoolId":                    poolID,
		"MfaConfiguration":              "OPTIONAL",
		"SoftwareTokenMfaConfiguration": map[string]any{"Enabled": true},
	}))
	if err != nil {
		t.Fatalf("SetUserPoolMfaConfig: %v", err)
	}
	if setResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", setResp.StatusCode)
	}

	// GetUserPoolMfaConfig.
	getResp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "GetUserPoolMfaConfig", map[string]any{
		"UserPoolId": poolID,
	}))
	if err != nil {
		t.Fatalf("GetUserPoolMfaConfig: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", getResp.StatusCode)
	}
	var out struct {
		MfaConfiguration string `json:"MfaConfiguration"`
	}
	if err := json.Unmarshal(getResp.Body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.MfaConfiguration != "OPTIONAL" {
		t.Errorf("want MfaConfiguration=OPTIONAL, got %q", out.MfaConfiguration)
	}
}

func TestCognitoIdentity_DeleteIdentityPool(t *testing.T) {
	p, ctx := setupCognitoIdentityPlugin(t)

	// Create pool.
	createResp, err := p.HandleRequest(ctx, cognitoIdentityRequest(t, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "del-pool",
		"AllowUnauthenticatedIdentities": false,
	}))
	if err != nil {
		t.Fatalf("CreateIdentityPool: %v", err)
	}
	var out struct {
		IdentityPoolID string `json:"IdentityPoolId"`
	}
	if err := json.Unmarshal(createResp.Body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Delete.
	delResp, err := p.HandleRequest(ctx, cognitoIdentityRequest(t, "DeleteIdentityPool", map[string]any{
		"IdentityPoolId": out.IdentityPoolID,
	}))
	if err != nil {
		t.Fatalf("DeleteIdentityPool: %v", err)
	}
	if delResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", delResp.StatusCode)
	}
}

func TestCognitoIdentity_ListIdentityPools(t *testing.T) {
	p, ctx := setupCognitoIdentityPlugin(t)

	for _, name := range []string{"pool1", "pool2"} {
		_, err := p.HandleRequest(ctx, cognitoIdentityRequest(t, "CreateIdentityPool", map[string]any{
			"IdentityPoolName":               name,
			"AllowUnauthenticatedIdentities": false,
		}))
		if err != nil {
			t.Fatalf("CreateIdentityPool %s: %v", name, err)
		}
	}

	resp, err := p.HandleRequest(ctx, cognitoIdentityRequest(t, "ListIdentityPools", map[string]any{
		"MaxResults": 10,
	}))
	if err != nil {
		t.Fatalf("ListIdentityPools: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", resp.StatusCode)
	}
	var out struct {
		IdentityPools []struct {
			IdentityPoolID string `json:"IdentityPoolId"`
		} `json:"IdentityPools"`
	}
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(out.IdentityPools) != 2 {
		t.Errorf("want 2 pools, got %d", len(out.IdentityPools))
	}
}

func TestCognitoIdentity_SetIdentityPoolRoles(t *testing.T) {
	p, ctx := setupCognitoIdentityPlugin(t)

	createResp, err := p.HandleRequest(ctx, cognitoIdentityRequest(t, "CreateIdentityPool", map[string]any{
		"IdentityPoolName":               "roles-pool",
		"AllowUnauthenticatedIdentities": true,
	}))
	if err != nil {
		t.Fatalf("CreateIdentityPool: %v", err)
	}
	var out struct {
		IdentityPoolID string `json:"IdentityPoolId"`
	}
	if err := json.Unmarshal(createResp.Body, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// SetIdentityPoolRoles.
	rolesResp, err := p.HandleRequest(ctx, cognitoIdentityRequest(t, "SetIdentityPoolRoles", map[string]any{
		"IdentityPoolId": out.IdentityPoolID,
		"Roles": map[string]string{
			"authenticated":   "arn:aws:iam::123456789012:role/AuthRole",
			"unauthenticated": "arn:aws:iam::123456789012:role/UnauthRole",
		},
	}))
	if err != nil {
		t.Fatalf("SetIdentityPoolRoles: %v", err)
	}
	if rolesResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", rolesResp.StatusCode)
	}

	// GetIdentityPoolRoles.
	getRolesResp, err := p.HandleRequest(ctx, cognitoIdentityRequest(t, "GetIdentityPoolRoles", map[string]any{
		"IdentityPoolId": out.IdentityPoolID,
	}))
	if err != nil {
		t.Fatalf("GetIdentityPoolRoles: %v", err)
	}
	if getRolesResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", getRolesResp.StatusCode)
	}
}

func TestCognitoIDP_UpdateUserPoolClient(t *testing.T) {
	p, ctx := setupCognitoIDPPlugin(t)

	// Create pool.
	poolID := createCognitoPool(t, p, ctx, "update-client-pool")

	// Create a client.
	clientResp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "CreateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientName": "orig-name",
	}))
	if err != nil {
		t.Fatalf("CreateUserPoolClient: %v", err)
	}
	if clientResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateUserPoolClient: want 200, got %d", clientResp.StatusCode)
	}
	var createOut struct {
		UserPoolClient struct {
			ClientID string `json:"ClientId"`
		} `json:"UserPoolClient"`
	}
	if err := json.Unmarshal(clientResp.Body, &createOut); err != nil {
		t.Fatalf("unmarshal CreateUserPoolClient: %v", err)
	}
	clientID := createOut.UserPoolClient.ClientID

	// Update the client name.
	updateResp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "UpdateUserPoolClient", map[string]any{
		"UserPoolId": poolID,
		"ClientId":   clientID,
		"ClientName": "updated-name",
	}))
	if err != nil {
		t.Fatalf("UpdateUserPoolClient: %v", err)
	}
	if updateResp.StatusCode != http.StatusOK {
		t.Fatalf("UpdateUserPoolClient: want 200, got %d; body=%s", updateResp.StatusCode, updateResp.Body)
	}

	var updateOut struct {
		UserPoolClient struct {
			ClientID   string `json:"ClientId"`
			ClientName string `json:"ClientName"`
		} `json:"UserPoolClient"`
	}
	if err := json.Unmarshal(updateResp.Body, &updateOut); err != nil {
		t.Fatalf("unmarshal UpdateUserPoolClient: %v", err)
	}
	if updateOut.UserPoolClient.ClientName != "updated-name" {
		t.Errorf("want ClientName=updated-name, got %q", updateOut.UserPoolClient.ClientName)
	}
}

func TestCognitoIDP_RespondToAuthChallenge(t *testing.T) {
	p, ctx := setupCognitoIDPPlugin(t)

	// RespondToAuthChallenge is a stub that always succeeds.
	resp, err := p.HandleRequest(ctx, cognitoIDPRequest(t, "RespondToAuthChallenge", map[string]any{
		"ClientId":      "fake-client-id",
		"ChallengeName": "NEW_PASSWORD_REQUIRED",
		"ChallengeResponses": map[string]string{
			"USERNAME":     "testuser",
			"NEW_PASSWORD": "NewPass123!",
		},
	}))
	if err != nil {
		t.Fatalf("RespondToAuthChallenge: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("RespondToAuthChallenge: want 200, got %d; body=%s", resp.StatusCode, resp.Body)
	}

	var out struct {
		AuthenticationResult struct {
			AccessToken string `json:"AccessToken"`
			ExpiresIn   int    `json:"ExpiresIn"`
		} `json:"AuthenticationResult"`
	}
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		t.Fatalf("unmarshal RespondToAuthChallenge: %v", err)
	}
	if out.AuthenticationResult.AccessToken == "" {
		t.Error("RespondToAuthChallenge: AccessToken is empty")
	}
	if out.AuthenticationResult.ExpiresIn == 0 {
		t.Error("RespondToAuthChallenge: ExpiresIn is 0")
	}
}
