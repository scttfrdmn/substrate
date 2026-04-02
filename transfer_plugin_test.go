package substrate_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

func setupTransferPlugin(t *testing.T) (*substrate.TransferPlugin, *substrate.RequestContext) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := &substrate.TransferPlugin{}
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  substrate.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("TransferPlugin.Initialize: %v", err)
	}
	return p, &substrate.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "req-transfer-1",
	}
}

func transferRequest(t *testing.T, op string, body map[string]any) *substrate.AWSRequest {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal transfer request: %v", err)
	}
	return &substrate.AWSRequest{
		Service:   "transfer",
		Operation: op,
		Headers:   map[string]string{"X-Amz-Target": "TransferService." + op},
		Body:      b,
		Params:    map[string]string{},
	}
}

func TestTransferPlugin_ServerCRUD(t *testing.T) {
	p, ctx := setupTransferPlugin(t)

	// CreateServer.
	resp, err := p.HandleRequest(ctx, transferRequest(t, "CreateServer", map[string]any{
		"Domain":       "SFTP",
		"EndpointType": "PUBLIC",
	}))
	if err != nil {
		t.Fatalf("CreateServer: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", resp.StatusCode, resp.Body)
	}

	var createResult struct {
		ServerId string `json:"ServerId"`
	}
	if err := json.Unmarshal(resp.Body, &createResult); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}
	if createResult.ServerId == "" {
		t.Error("want non-empty ServerId")
	}
	// Verify server ID format: "s-" + 17 hex chars.
	if len(createResult.ServerId) != 19 || createResult.ServerId[:2] != "s-" {
		t.Errorf("want ServerId in format s-{17 hex}, got %q", createResult.ServerId)
	}
	serverID := createResult.ServerId

	// DescribeServer.
	resp, err = p.HandleRequest(ctx, transferRequest(t, "DescribeServer", map[string]any{
		"ServerId": serverID,
	}))
	if err != nil {
		t.Fatalf("DescribeServer: %v", err)
	}
	var descResult struct {
		Server struct {
			ServerId     string `json:"ServerId"`
			State        string `json:"State"`
			Domain       string `json:"Domain"`
			EndpointType string `json:"EndpointType"`
		} `json:"Server"`
	}
	if err := json.Unmarshal(resp.Body, &descResult); err != nil {
		t.Fatalf("unmarshal describe: %v", err)
	}
	if descResult.Server.State != "ONLINE" {
		t.Errorf("want State=ONLINE, got %q", descResult.Server.State)
	}
	if descResult.Server.Domain != "SFTP" {
		t.Errorf("want Domain=SFTP, got %q", descResult.Server.Domain)
	}

	// UpdateServer.
	resp, err = p.HandleRequest(ctx, transferRequest(t, "UpdateServer", map[string]any{
		"ServerId":     serverID,
		"EndpointType": "VPC",
	}))
	if err != nil {
		t.Fatalf("UpdateServer: %v", err)
	}
	var updateResult struct {
		ServerId string `json:"ServerId"`
	}
	if err := json.Unmarshal(resp.Body, &updateResult); err != nil {
		t.Fatalf("unmarshal update: %v", err)
	}
	if updateResult.ServerId != serverID {
		t.Errorf("want ServerId=%s, got %s", serverID, updateResult.ServerId)
	}

	// Verify update applied.
	resp, err = p.HandleRequest(ctx, transferRequest(t, "DescribeServer", map[string]any{
		"ServerId": serverID,
	}))
	if err != nil {
		t.Fatalf("DescribeServer after update: %v", err)
	}
	var descAfterUpdate struct {
		Server struct {
			EndpointType string `json:"EndpointType"`
		} `json:"Server"`
	}
	if err := json.Unmarshal(resp.Body, &descAfterUpdate); err != nil {
		t.Fatalf("unmarshal describe after update: %v", err)
	}
	if descAfterUpdate.Server.EndpointType != "VPC" {
		t.Errorf("want EndpointType=VPC after update, got %q", descAfterUpdate.Server.EndpointType)
	}

	// DeleteServer.
	_, err = p.HandleRequest(ctx, transferRequest(t, "DeleteServer", map[string]any{
		"ServerId": serverID,
	}))
	if err != nil {
		t.Fatalf("DeleteServer: %v", err)
	}

	// DescribeServer after delete.
	_, err = p.HandleRequest(ctx, transferRequest(t, "DescribeServer", map[string]any{
		"ServerId": serverID,
	}))
	if err == nil {
		t.Fatal("want error after delete, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok || awsErr.Code != "ResourceNotFoundException" {
		t.Errorf("want ResourceNotFoundException, got %v", err)
	}
}

func TestTransferPlugin_ListServers(t *testing.T) {
	p, ctx := setupTransferPlugin(t)

	for i := 0; i < 2; i++ {
		resp, err := p.HandleRequest(ctx, transferRequest(t, "CreateServer", map[string]any{
			"Domain": "SFTP",
		}))
		if err != nil {
			t.Fatalf("CreateServer %d: %v", i, err)
		}
		_ = resp
	}

	resp, err := p.HandleRequest(ctx, transferRequest(t, "ListServers", map[string]any{}))
	if err != nil {
		t.Fatalf("ListServers: %v", err)
	}
	var result struct {
		Servers []map[string]any `json:"Servers"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(result.Servers) != 2 {
		t.Errorf("want 2 servers, got %d", len(result.Servers))
	}
	// Verify all servers have ONLINE state.
	for _, s := range result.Servers {
		if s["State"] != "ONLINE" {
			t.Errorf("want State=ONLINE for server, got %v", s["State"])
		}
	}
}

func TestTransferPlugin_UserCRUD(t *testing.T) {
	p, ctx := setupTransferPlugin(t)

	// Create server first.
	resp, err := p.HandleRequest(ctx, transferRequest(t, "CreateServer", map[string]any{
		"Domain": "SFTP",
	}))
	if err != nil {
		t.Fatalf("CreateServer: %v", err)
	}
	var serverResult struct {
		ServerId string `json:"ServerId"`
	}
	if err := json.Unmarshal(resp.Body, &serverResult); err != nil {
		t.Fatalf("unmarshal server: %v", err)
	}
	serverID := serverResult.ServerId

	// CreateUser.
	resp, err = p.HandleRequest(ctx, transferRequest(t, "CreateUser", map[string]any{
		"ServerId":      serverID,
		"UserName":      "alice",
		"HomeDirectory": "/alice",
		"Role":          "arn:aws:iam::123456789012:role/transfer-user-role",
	}))
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	var createUserResult struct {
		ServerId string `json:"ServerId"`
		UserName string `json:"UserName"`
	}
	if err := json.Unmarshal(resp.Body, &createUserResult); err != nil {
		t.Fatalf("unmarshal create user: %v", err)
	}
	if createUserResult.UserName != "alice" {
		t.Errorf("want UserName=alice, got %q", createUserResult.UserName)
	}

	// Duplicate CreateUser.
	_, err = p.HandleRequest(ctx, transferRequest(t, "CreateUser", map[string]any{
		"ServerId": serverID,
		"UserName": "alice",
	}))
	if err == nil {
		t.Fatal("want error for duplicate user, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok || awsErr.Code != "ConflictException" {
		t.Errorf("want ConflictException, got %v", err)
	}

	// DescribeUser.
	resp, err = p.HandleRequest(ctx, transferRequest(t, "DescribeUser", map[string]any{
		"ServerId": serverID,
		"UserName": "alice",
	}))
	if err != nil {
		t.Fatalf("DescribeUser: %v", err)
	}
	var descUserResult struct {
		ServerId string `json:"ServerId"`
		User     struct {
			UserName      string `json:"UserName"`
			HomeDirectory string `json:"HomeDirectory"`
		} `json:"User"`
	}
	if err := json.Unmarshal(resp.Body, &descUserResult); err != nil {
		t.Fatalf("unmarshal describe user: %v", err)
	}
	if descUserResult.User.HomeDirectory != "/alice" {
		t.Errorf("want HomeDirectory=/alice, got %q", descUserResult.User.HomeDirectory)
	}

	// UpdateUser.
	_, err = p.HandleRequest(ctx, transferRequest(t, "UpdateUser", map[string]any{
		"ServerId":      serverID,
		"UserName":      "alice",
		"HomeDirectory": "/alice-updated",
	}))
	if err != nil {
		t.Fatalf("UpdateUser: %v", err)
	}

	// Verify update.
	resp, err = p.HandleRequest(ctx, transferRequest(t, "DescribeUser", map[string]any{
		"ServerId": serverID,
		"UserName": "alice",
	}))
	if err != nil {
		t.Fatalf("DescribeUser after update: %v", err)
	}
	var descAfterUpdate struct {
		User struct {
			HomeDirectory string `json:"HomeDirectory"`
		} `json:"User"`
	}
	if err := json.Unmarshal(resp.Body, &descAfterUpdate); err != nil {
		t.Fatalf("unmarshal describe after update: %v", err)
	}
	if descAfterUpdate.User.HomeDirectory != "/alice-updated" {
		t.Errorf("want HomeDirectory=/alice-updated, got %q", descAfterUpdate.User.HomeDirectory)
	}

	// DeleteUser.
	_, err = p.HandleRequest(ctx, transferRequest(t, "DeleteUser", map[string]any{
		"ServerId": serverID,
		"UserName": "alice",
	}))
	if err != nil {
		t.Fatalf("DeleteUser: %v", err)
	}

	// DescribeUser after delete.
	_, err = p.HandleRequest(ctx, transferRequest(t, "DescribeUser", map[string]any{
		"ServerId": serverID,
		"UserName": "alice",
	}))
	if err == nil {
		t.Fatal("want error after delete, got nil")
	}
}

func TestTransferPlugin_ListUsers(t *testing.T) {
	p, ctx := setupTransferPlugin(t)

	// Create server.
	resp, err := p.HandleRequest(ctx, transferRequest(t, "CreateServer", map[string]any{
		"Domain": "SFTP",
	}))
	if err != nil {
		t.Fatalf("CreateServer: %v", err)
	}
	var serverResult struct {
		ServerId string `json:"ServerId"`
	}
	if err := json.Unmarshal(resp.Body, &serverResult); err != nil {
		t.Fatalf("unmarshal server: %v", err)
	}
	serverID := serverResult.ServerId

	// Create two users.
	for _, name := range []string{"user-alpha", "user-beta"} {
		_, err := p.HandleRequest(ctx, transferRequest(t, "CreateUser", map[string]any{
			"ServerId": serverID,
			"UserName": name,
		}))
		if err != nil {
			t.Fatalf("CreateUser %s: %v", name, err)
		}
	}

	// ListUsers.
	resp, err = p.HandleRequest(ctx, transferRequest(t, "ListUsers", map[string]any{
		"ServerId": serverID,
	}))
	if err != nil {
		t.Fatalf("ListUsers: %v", err)
	}
	var result struct {
		ServerId string           `json:"ServerId"`
		Users    []map[string]any `json:"Users"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(result.Users) != 2 {
		t.Errorf("want 2 users, got %d", len(result.Users))
	}
	if result.ServerId != serverID {
		t.Errorf("want ServerId=%s, got %s", serverID, result.ServerId)
	}
}

func TestTransferPlugin_DeleteServer_CascadesUsers(t *testing.T) {
	p, ctx := setupTransferPlugin(t)

	// Create server.
	resp, err := p.HandleRequest(ctx, transferRequest(t, "CreateServer", map[string]any{
		"Domain": "SFTP",
	}))
	if err != nil {
		t.Fatalf("CreateServer: %v", err)
	}
	var serverResult struct {
		ServerId string `json:"ServerId"`
	}
	if err := json.Unmarshal(resp.Body, &serverResult); err != nil {
		t.Fatalf("unmarshal server: %v", err)
	}
	serverID := serverResult.ServerId

	// Create a user.
	_, err = p.HandleRequest(ctx, transferRequest(t, "CreateUser", map[string]any{
		"ServerId": serverID,
		"UserName": "cascade-user",
	}))
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	// Delete server (should cascade to user).
	_, err = p.HandleRequest(ctx, transferRequest(t, "DeleteServer", map[string]any{
		"ServerId": serverID,
	}))
	if err != nil {
		t.Fatalf("DeleteServer: %v", err)
	}

	// Create a new server and try to create a user with same name — should succeed
	// because the old server's user index was cleaned up.
	resp, err = p.HandleRequest(ctx, transferRequest(t, "CreateServer", map[string]any{
		"Domain": "SFTP",
	}))
	if err != nil {
		t.Fatalf("CreateServer 2: %v", err)
	}
	var serverResult2 struct {
		ServerId string `json:"ServerId"`
	}
	if err := json.Unmarshal(resp.Body, &serverResult2); err != nil {
		t.Fatalf("unmarshal server 2: %v", err)
	}
	_, err = p.HandleRequest(ctx, transferRequest(t, "CreateUser", map[string]any{
		"ServerId": serverResult2.ServerId,
		"UserName": "cascade-user",
	}))
	if err != nil {
		t.Fatalf("CreateUser on new server: %v", err)
	}
}
