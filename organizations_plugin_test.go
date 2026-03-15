package substrate_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

func newOrganizationsTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	registry := substrate.NewPluginRegistry()
	store := substrate.NewEventStore(substrate.EventStoreConfig{Enabled: false})
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	logger := substrate.NewDefaultLogger(0, false)

	p := &substrate.OrganizationsPlugin{}
	if err := p.Initialize(t.Context(), substrate.PluginConfig{ //nolint:contextcheck
		State:   state,
		Logger:  logger,
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("initialize organizations plugin: %v", err)
	}
	registry.Register(p)

	cfg := substrate.DefaultConfig()
	srv := substrate.NewServer(*cfg, registry, store, state, tc, logger)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

func orgsRequest(t *testing.T, ts *httptest.Server, op string, body interface{}) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal orgs request: %v", err)
	}
	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "Organizations_20161128."+op)
	req.Host = "organizations.us-east-1.amazonaws.com"
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("orgs request %s: %v", op, err)
	}
	return resp
}

func TestOrganizations_DescribeOrganization_AutoCreate(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	resp := orgsRequest(t, ts, "DescribeOrganization", map[string]interface{}{})
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var out map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	org, ok := out["Organization"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected Organization object, got %v", out["Organization"])
	}
	if featureSet, _ := org["FeatureSet"].(string); featureSet != "ALL" {
		t.Errorf("expected FeatureSet=ALL, got %q", featureSet)
	}
	orgID, _ := org["Id"].(string)
	if len(orgID) < 2 || orgID[:2] != "o-" {
		t.Errorf("expected org ID to start with 'o-', got %q", orgID)
	}

	// Second call should return the same org.
	resp2 := orgsRequest(t, ts, "DescribeOrganization", map[string]interface{}{})
	defer resp2.Body.Close() //nolint:errcheck
	var out2 map[string]interface{}
	if err := json.NewDecoder(resp2.Body).Decode(&out2); err != nil {
		t.Fatalf("decode second: %v", err)
	}
	org2 := out2["Organization"].(map[string]interface{})
	if org2["Id"] != org["Id"] {
		t.Errorf("org ID changed between calls: %v vs %v", org["Id"], org2["Id"])
	}
}

func TestOrganizations_ListAccounts(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	// Initialise org first.
	initResp := orgsRequest(t, ts, "DescribeOrganization", map[string]interface{}{})
	initResp.Body.Close() //nolint:errcheck

	resp := orgsRequest(t, ts, "ListAccounts", map[string]interface{}{})
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var out map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	accounts, ok := out["Accounts"].([]interface{})
	if !ok || len(accounts) == 0 {
		t.Fatalf("expected at least 1 account, got %v", out["Accounts"])
	}
}

func TestOrganizations_CreateAccount_DescribeAccount(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	// Create account.
	createResp := orgsRequest(t, ts, "CreateAccount", map[string]interface{}{
		"AccountName": "dev-account",
		"Email":       "dev@example.com",
	})
	defer createResp.Body.Close() //nolint:errcheck
	if createResp.StatusCode != http.StatusOK {
		t.Fatalf("CreateAccount: expected 200, got %d", createResp.StatusCode)
	}
	var createOut map[string]interface{}
	if err := json.NewDecoder(createResp.Body).Decode(&createOut); err != nil {
		t.Fatalf("CreateAccount decode: %v", err)
	}
	status, ok := createOut["CreateAccountStatus"].(map[string]interface{})
	if !ok {
		t.Fatal("expected CreateAccountStatus")
	}
	if state, _ := status["State"].(string); state != "SUCCEEDED" {
		t.Errorf("expected State=SUCCEEDED, got %q", state)
	}
	newAccountID, _ := status["AccountId"].(string)
	if newAccountID == "" {
		t.Fatal("expected non-empty AccountId in CreateAccountStatus")
	}

	// DescribeAccount.
	descResp := orgsRequest(t, ts, "DescribeAccount", map[string]interface{}{
		"AccountId": newAccountID,
	})
	defer descResp.Body.Close() //nolint:errcheck
	if descResp.StatusCode != http.StatusOK {
		t.Fatalf("DescribeAccount: expected 200, got %d", descResp.StatusCode)
	}
	var descOut map[string]interface{}
	if err := json.NewDecoder(descResp.Body).Decode(&descOut); err != nil {
		t.Fatalf("DescribeAccount decode: %v", err)
	}
	acct, ok := descOut["Account"].(map[string]interface{})
	if !ok {
		t.Fatal("expected Account in response")
	}
	if acct["Id"] != newAccountID {
		t.Errorf("expected account ID %q, got %q", newAccountID, acct["Id"])
	}
}

func TestOrganizations_DescribeAccount_NotFound(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	resp := orgsRequest(t, ts, "DescribeAccount", map[string]interface{}{
		"AccountId": "999999999999",
	})
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestOrganizations_ListRoots(t *testing.T) {
	ts := newOrganizationsTestServer(t)

	resp := orgsRequest(t, ts, "ListRoots", map[string]interface{}{})
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var out map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	roots, ok := out["Roots"].([]interface{})
	if !ok || len(roots) != 1 {
		t.Fatalf("expected 1 root, got %v", out["Roots"])
	}
	root := roots[0].(map[string]interface{})
	if name, _ := root["Name"].(string); name != "Root" {
		t.Errorf("expected Name=Root, got %q", name)
	}
}
