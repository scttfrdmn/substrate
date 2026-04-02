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

func setupRAMPlugin(t *testing.T) (*substrate.RAMPlugin, *substrate.RequestContext) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := &substrate.RAMPlugin{}
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  substrate.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("RAMPlugin.Initialize: %v", err)
	}
	return p, &substrate.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "req-ram-1",
	}
}

func ramRequest(t *testing.T, method, path string, body map[string]any) *substrate.AWSRequest {
	t.Helper()
	var b []byte
	if body != nil {
		var err error
		b, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal ram request: %v", err)
		}
	}
	return &substrate.AWSRequest{
		Service:   "ram",
		Operation: method,
		Path:      path,
		Headers:   map[string]string{},
		Body:      b,
		Params:    map[string]string{},
	}
}

func TestRAMPlugin_ShareCRUD(t *testing.T) {
	p, ctx := setupRAMPlugin(t)

	// CreateResourceShare.
	resp, err := p.HandleRequest(ctx, ramRequest(t, "POST", "/createresourceshare", map[string]any{
		"name":                    "my-share",
		"allowExternalPrincipals": true,
	}))
	if err != nil {
		t.Fatalf("CreateResourceShare: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", resp.StatusCode, resp.Body)
	}
	var createResult struct {
		ResourceShare struct {
			ResourceShareArn string `json:"resourceShareArn"`
			Name             string `json:"name"`
			Status           string `json:"status"`
		} `json:"resourceShare"`
	}
	if err := json.Unmarshal(resp.Body, &createResult); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}
	if createResult.ResourceShare.Name != "my-share" {
		t.Errorf("want name=my-share, got %q", createResult.ResourceShare.Name)
	}
	if createResult.ResourceShare.ResourceShareArn == "" {
		t.Error("want non-empty ResourceShareArn")
	}
	if createResult.ResourceShare.Status != "ACTIVE" {
		t.Errorf("want status=ACTIVE, got %q", createResult.ResourceShare.Status)
	}
	shareArn := createResult.ResourceShare.ResourceShareArn

	// GetResourceShares.
	resp, err = p.HandleRequest(ctx, ramRequest(t, "POST", "/getresourceshares", map[string]any{
		"resourceOwner": "SELF",
	}))
	if err != nil {
		t.Fatalf("GetResourceShares: %v", err)
	}
	var listResult struct {
		ResourceShares []struct {
			Name string `json:"name"`
		} `json:"resourceShares"`
	}
	if err := json.Unmarshal(resp.Body, &listResult); err != nil {
		t.Fatalf("unmarshal get: %v", err)
	}
	if len(listResult.ResourceShares) != 1 {
		t.Errorf("want 1 share, got %d", len(listResult.ResourceShares))
	}

	// UpdateResourceShare.
	resp, err = p.HandleRequest(ctx, ramRequest(t, "POST", "/updateresourceshare", map[string]any{
		"resourceShareArn": shareArn,
		"name":             "my-share-updated",
	}))
	if err != nil {
		t.Fatalf("UpdateResourceShare: %v", err)
	}
	var updateResult struct {
		ResourceShare struct {
			Name string `json:"name"`
		} `json:"resourceShare"`
	}
	if err := json.Unmarshal(resp.Body, &updateResult); err != nil {
		t.Fatalf("unmarshal update: %v", err)
	}
	if updateResult.ResourceShare.Name != "my-share-updated" {
		t.Errorf("want name=my-share-updated, got %q", updateResult.ResourceShare.Name)
	}

	// DeleteResourceShare.
	_, err = p.HandleRequest(ctx, ramRequest(t, "POST", "/deleteresourceshare", map[string]any{
		"resourceShareArn": shareArn,
	}))
	if err != nil {
		t.Fatalf("DeleteResourceShare: %v", err)
	}

	// GetResourceShares after delete — should return empty.
	resp, err = p.HandleRequest(ctx, ramRequest(t, "POST", "/getresourceshares", nil))
	if err != nil {
		t.Fatalf("GetResourceShares after delete: %v", err)
	}
	var emptyResult struct {
		ResourceShares []struct{} `json:"resourceShares"`
	}
	if err := json.Unmarshal(resp.Body, &emptyResult); err != nil {
		t.Fatalf("unmarshal empty: %v", err)
	}
	if len(emptyResult.ResourceShares) != 0 {
		t.Errorf("want 0 shares after delete, got %d", len(emptyResult.ResourceShares))
	}
}

func TestRAMPlugin_GetResourceShares_Filters(t *testing.T) {
	p, ctx := setupRAMPlugin(t)

	for _, name := range []string{"share-alpha", "share-beta"} {
		_, err := p.HandleRequest(ctx, ramRequest(t, "POST", "/createresourceshare", map[string]any{
			"name": name,
		}))
		if err != nil {
			t.Fatalf("CreateResourceShare %s: %v", name, err)
		}
	}

	// Filter by name.
	resp, err := p.HandleRequest(ctx, ramRequest(t, "POST", "/getresourceshares", map[string]any{
		"name": "share-alpha",
	}))
	if err != nil {
		t.Fatalf("GetResourceShares filtered: %v", err)
	}
	var result struct {
		ResourceShares []struct {
			Name string `json:"name"`
		} `json:"resourceShares"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(result.ResourceShares) != 1 {
		t.Errorf("want 1 share with name filter, got %d", len(result.ResourceShares))
	}
	if len(result.ResourceShares) > 0 && result.ResourceShares[0].Name != "share-alpha" {
		t.Errorf("want name=share-alpha, got %q", result.ResourceShares[0].Name)
	}
}

func TestRAMPlugin_AssociateDisassociate(t *testing.T) {
	p, ctx := setupRAMPlugin(t)

	resp, err := p.HandleRequest(ctx, ramRequest(t, "POST", "/createresourceshare", map[string]any{
		"name": "assoc-share",
	}))
	if err != nil {
		t.Fatalf("CreateResourceShare: %v", err)
	}
	var createResult struct {
		ResourceShare struct {
			ResourceShareArn string `json:"resourceShareArn"`
		} `json:"resourceShare"`
	}
	if err := json.Unmarshal(resp.Body, &createResult); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	shareArn := createResult.ResourceShare.ResourceShareArn

	principal := "arn:aws:iam::999999999999:root"
	resourceArn := "arn:aws:ec2:us-east-1:123456789012:subnet/subnet-12345"

	// AssociateResourceShare.
	resp, err = p.HandleRequest(ctx, ramRequest(t, "POST", "/associateresourceshare", map[string]any{
		"resourceShareArn": shareArn,
		"principals":       []string{principal},
		"resourceArns":     []string{resourceArn},
	}))
	if err != nil {
		t.Fatalf("AssociateResourceShare: %v", err)
	}
	var assocResult struct {
		ResourceShareAssociations []struct {
			AssociationType string `json:"associationType"`
		} `json:"resourceShareAssociations"`
	}
	if err := json.Unmarshal(resp.Body, &assocResult); err != nil {
		t.Fatalf("unmarshal assoc: %v", err)
	}
	if len(assocResult.ResourceShareAssociations) != 2 {
		t.Errorf("want 2 associations (1 principal + 1 resource), got %d", len(assocResult.ResourceShareAssociations))
	}

	// DisassociateResourceShare.
	resp, err = p.HandleRequest(ctx, ramRequest(t, "POST", "/disassociateresourceshare", map[string]any{
		"resourceShareArn": shareArn,
		"principals":       []string{principal},
	}))
	if err != nil {
		t.Fatalf("DisassociateResourceShare: %v", err)
	}
	var disassocResult struct {
		ResourceShareAssociations []struct{} `json:"resourceShareAssociations"`
	}
	if err := json.Unmarshal(resp.Body, &disassocResult); err != nil {
		t.Fatalf("unmarshal disassoc: %v", err)
	}
	if len(disassocResult.ResourceShareAssociations) != 1 {
		t.Errorf("want 1 association after disassoc (principal removed, resource remains), got %d", len(disassocResult.ResourceShareAssociations))
	}
}

func TestRAMPlugin_ListPrincipals_ListResources(t *testing.T) {
	p, ctx := setupRAMPlugin(t)

	for _, name := range []string{"share-1", "share-2"} {
		resp, err := p.HandleRequest(ctx, ramRequest(t, "POST", "/createresourceshare", map[string]any{
			"name":       name,
			"principals": []string{"arn:aws:iam::111111111111:root"},
			"resourceArns": []string{
				"arn:aws:ec2:us-east-1:123456789012:subnet/subnet-" + name,
			},
		}))
		if err != nil {
			t.Fatalf("CreateResourceShare %s: %v", name, err)
		}
		_ = resp
	}

	// ListPrincipals.
	resp, err := p.HandleRequest(ctx, ramRequest(t, "POST", "/listprincipals", nil))
	if err != nil {
		t.Fatalf("ListPrincipals: %v", err)
	}
	var principalResult struct {
		Principals []struct {
			ID string `json:"id"`
		} `json:"principals"`
	}
	if err := json.Unmarshal(resp.Body, &principalResult); err != nil {
		t.Fatalf("unmarshal principals: %v", err)
	}
	// Both shares have the same principal, so deduplication should yield 1.
	if len(principalResult.Principals) != 1 {
		t.Errorf("want 1 unique principal, got %d", len(principalResult.Principals))
	}

	// ListResources.
	resp, err = p.HandleRequest(ctx, ramRequest(t, "POST", "/listresources", nil))
	if err != nil {
		t.Fatalf("ListResources: %v", err)
	}
	var resourceResult struct {
		Resources []struct {
			Arn string `json:"arn"`
		} `json:"resources"`
	}
	if err := json.Unmarshal(resp.Body, &resourceResult); err != nil {
		t.Fatalf("unmarshal resources: %v", err)
	}
	if len(resourceResult.Resources) != 2 {
		t.Errorf("want 2 resources, got %d", len(resourceResult.Resources))
	}
}

func TestRAMPlugin_Errors(t *testing.T) {
	p, ctx := setupRAMPlugin(t)

	// CreateResourceShare missing name.
	_, err := p.HandleRequest(ctx, ramRequest(t, "POST", "/createresourceshare", map[string]any{}))
	if err == nil {
		t.Fatal("want error for missing name")
	}

	// UpdateResourceShare not found.
	_, err = p.HandleRequest(ctx, ramRequest(t, "POST", "/updateresourceshare", map[string]any{
		"resourceShareArn": "arn:aws:ram:us-east-1:123456789012:resource-share/nonexistent",
		"name":             "new-name",
	}))
	if err == nil {
		t.Fatal("want error for unknown resource share")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok || awsErr.Code != "UnknownResourceException" {
		t.Errorf("want UnknownResourceException, got %v", err)
	}

	// DeleteResourceShare not found.
	_, err = p.HandleRequest(ctx, ramRequest(t, "POST", "/deleteresourceshare", map[string]any{
		"resourceShareArn": "arn:aws:ram:us-east-1:123456789012:resource-share/nonexistent",
	}))
	if err == nil {
		t.Fatal("want error for unknown resource share on delete")
	}

	// Unsupported operation.
	_, err = p.HandleRequest(ctx, ramRequest(t, "POST", "/unknownop", nil))
	if err == nil {
		t.Fatal("want error for unsupported operation")
	}

	// UpdateResourceShare with AllowExternalPrincipals.
	resp, err := p.HandleRequest(ctx, ramRequest(t, "POST", "/createresourceshare", map[string]any{
		"name": "update-test",
	}))
	if err != nil {
		t.Fatalf("CreateResourceShare: %v", err)
	}
	var createResult struct {
		ResourceShare struct {
			ResourceShareArn string `json:"resourceShareArn"`
		} `json:"resourceShare"`
	}
	if err := json.Unmarshal(resp.Body, &createResult); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	falseVal := false
	body, _ := json.Marshal(map[string]any{
		"resourceShareArn":        createResult.ResourceShare.ResourceShareArn,
		"allowExternalPrincipals": falseVal,
	})
	req := &substrate.AWSRequest{
		Service:   "ram",
		Operation: "POST",
		Path:      "/updateresourceshare",
		Headers:   map[string]string{},
		Body:      body,
		Params:    map[string]string{},
	}
	_, err = p.HandleRequest(ctx, req)
	if err != nil {
		t.Fatalf("UpdateResourceShare with AllowExternalPrincipals: %v", err)
	}
}
