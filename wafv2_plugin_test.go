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

func setupWAFv2Plugin(t *testing.T) (*substrate.WAFv2Plugin, *substrate.RequestContext) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := &substrate.WAFv2Plugin{}
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  substrate.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("WAFv2Plugin.Initialize: %v", err)
	}
	return p, &substrate.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "req-wafv2-1",
	}
}

func wafv2Request(t *testing.T, op string, body map[string]any) *substrate.AWSRequest {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal wafv2 request body: %v", err)
	}
	return &substrate.AWSRequest{
		Service:   "wafv2",
		Operation: op,
		Headers:   map[string]string{"X-Amz-Target": "AWSWAF_20190729." + op},
		Body:      b,
		Params:    map[string]string{},
	}
}

func TestWAFv2Plugin_CreateGetUpdateDeleteWebACL(t *testing.T) {
	p, ctx := setupWAFv2Plugin(t)

	// Create.
	resp, err := p.HandleRequest(ctx, wafv2Request(t, "CreateWebACL", map[string]any{
		"Name":        "my-acl",
		"Scope":       "REGIONAL",
		"Description": "test acl",
		"DefaultAction": map[string]any{
			"Allow": map[string]any{},
		},
		"VisibilityConfig": map[string]any{
			"SampledRequestsEnabled":   true,
			"CloudWatchMetricsEnabled": true,
			"MetricName":               "my-acl",
		},
	}))
	if err != nil {
		t.Fatalf("CreateWebACL: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", resp.StatusCode, resp.Body)
	}

	var createResult struct {
		Summary struct {
			Id          string `json:"Id"`
			Name        string `json:"Name"`
			ARN         string `json:"ARN"`
			LockToken   string `json:"LockToken"`
			Description string `json:"Description"`
		} `json:"Summary"`
	}
	if err := json.Unmarshal(resp.Body, &createResult); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}
	if createResult.Summary.Id == "" {
		t.Error("want non-empty Id")
	}
	if createResult.Summary.Name != "my-acl" {
		t.Errorf("want Name=my-acl, got %q", createResult.Summary.Name)
	}
	if createResult.Summary.LockToken == "" {
		t.Error("want non-empty LockToken")
	}
	if createResult.Summary.ARN == "" {
		t.Error("want non-empty ARN")
	}

	aclID := createResult.Summary.Id
	lockToken := createResult.Summary.LockToken

	// Get.
	resp, err = p.HandleRequest(ctx, wafv2Request(t, "GetWebACL", map[string]any{
		"Id":    aclID,
		"Name":  "my-acl",
		"Scope": "REGIONAL",
	}))
	if err != nil {
		t.Fatalf("GetWebACL: %v", err)
	}
	var getResult struct {
		WebACL    substrate.WAFv2WebACL `json:"WebACL"`
		LockToken string                `json:"LockToken"`
	}
	if err := json.Unmarshal(resp.Body, &getResult); err != nil {
		t.Fatalf("unmarshal get: %v", err)
	}
	if getResult.WebACL.Name != "my-acl" {
		t.Errorf("want Name=my-acl, got %q", getResult.WebACL.Name)
	}
	if getResult.WebACL.Description != "test acl" {
		t.Errorf("want Description=test acl, got %q", getResult.WebACL.Description)
	}

	// Update.
	resp, err = p.HandleRequest(ctx, wafv2Request(t, "UpdateWebACL", map[string]any{
		"Id":          aclID,
		"Name":        "my-acl",
		"Scope":       "REGIONAL",
		"LockToken":   lockToken,
		"Description": "updated description",
		"VisibilityConfig": map[string]any{
			"SampledRequestsEnabled":   true,
			"CloudWatchMetricsEnabled": true,
			"MetricName":               "my-acl",
		},
	}))
	if err != nil {
		t.Fatalf("UpdateWebACL: %v", err)
	}
	var updateResult struct {
		NextLockToken string `json:"NextLockToken"`
	}
	if err := json.Unmarshal(resp.Body, &updateResult); err != nil {
		t.Fatalf("unmarshal update: %v", err)
	}
	if updateResult.NextLockToken == "" {
		t.Error("want non-empty NextLockToken")
	}
	if updateResult.NextLockToken == lockToken {
		t.Error("want NextLockToken different from original LockToken")
	}
	newLockToken := updateResult.NextLockToken

	// Delete.
	resp, err = p.HandleRequest(ctx, wafv2Request(t, "DeleteWebACL", map[string]any{
		"Id":        aclID,
		"Name":      "my-acl",
		"Scope":     "REGIONAL",
		"LockToken": newLockToken,
	}))
	if err != nil {
		t.Fatalf("DeleteWebACL: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}

	// Get after delete should fail.
	_, err = p.HandleRequest(ctx, wafv2Request(t, "GetWebACL", map[string]any{
		"Id":    aclID,
		"Name":  "my-acl",
		"Scope": "REGIONAL",
	}))
	if err == nil {
		t.Fatal("want error after delete, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok {
		t.Fatalf("want *AWSError, got %T", err)
	}
	if awsErr.Code != "WAFNonexistentItemException" {
		t.Errorf("want WAFNonexistentItemException, got %q", awsErr.Code)
	}
}

func TestWAFv2Plugin_LockToken_Mismatch(t *testing.T) {
	p, ctx := setupWAFv2Plugin(t)

	resp, err := p.HandleRequest(ctx, wafv2Request(t, "CreateWebACL", map[string]any{
		"Name":  "lock-test",
		"Scope": "REGIONAL",
		"DefaultAction": map[string]any{
			"Block": map[string]any{},
		},
		"VisibilityConfig": map[string]any{
			"SampledRequestsEnabled":   false,
			"CloudWatchMetricsEnabled": false,
			"MetricName":               "lock-test",
		},
	}))
	if err != nil {
		t.Fatalf("CreateWebACL: %v", err)
	}

	var createResult struct {
		Summary struct {
			Id string `json:"Id"`
		} `json:"Summary"`
	}
	if err := json.Unmarshal(resp.Body, &createResult); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Update with wrong LockToken.
	_, err = p.HandleRequest(ctx, wafv2Request(t, "UpdateWebACL", map[string]any{
		"Id":        createResult.Summary.Id,
		"Name":      "lock-test",
		"Scope":     "REGIONAL",
		"LockToken": "wrong-token-00000000-0000-0000-0000-000000000000",
		"VisibilityConfig": map[string]any{
			"SampledRequestsEnabled":   false,
			"CloudWatchMetricsEnabled": false,
			"MetricName":               "lock-test",
		},
	}))
	if err == nil {
		t.Fatal("want error for LockToken mismatch, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok {
		t.Fatalf("want *AWSError, got %T", err)
	}
	if awsErr.Code != "WAFOptimisticLockException" {
		t.Errorf("want WAFOptimisticLockException, got %q", awsErr.Code)
	}

	// Delete with wrong LockToken.
	_, err = p.HandleRequest(ctx, wafv2Request(t, "DeleteWebACL", map[string]any{
		"Id":        createResult.Summary.Id,
		"Name":      "lock-test",
		"Scope":     "REGIONAL",
		"LockToken": "wrong-token",
	}))
	if err == nil {
		t.Fatal("want error for LockToken mismatch on delete, got nil")
	}
}

func TestWAFv2Plugin_ListWebACLs(t *testing.T) {
	p, ctx := setupWAFv2Plugin(t)

	for _, name := range []string{"acl-alpha", "acl-beta"} {
		_, err := p.HandleRequest(ctx, wafv2Request(t, "CreateWebACL", map[string]any{
			"Name":  name,
			"Scope": "REGIONAL",
			"DefaultAction": map[string]any{
				"Allow": map[string]any{},
			},
			"VisibilityConfig": map[string]any{
				"SampledRequestsEnabled":   false,
				"CloudWatchMetricsEnabled": false,
				"MetricName":               name,
			},
		}))
		if err != nil {
			t.Fatalf("CreateWebACL %s: %v", name, err)
		}
	}

	resp, err := p.HandleRequest(ctx, wafv2Request(t, "ListWebACLs", map[string]any{
		"Scope": "REGIONAL",
	}))
	if err != nil {
		t.Fatalf("ListWebACLs: %v", err)
	}

	var result struct {
		WebACLs []map[string]interface{} `json:"WebACLs"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		t.Fatalf("unmarshal list: %v", err)
	}
	if len(result.WebACLs) != 2 {
		t.Errorf("want 2 WebACLs, got %d", len(result.WebACLs))
	}
}

func TestWAFv2Plugin_AssociateGetWebACLForResource(t *testing.T) {
	p, ctx := setupWAFv2Plugin(t)

	// Create a WebACL first.
	resp, err := p.HandleRequest(ctx, wafv2Request(t, "CreateWebACL", map[string]any{
		"Name":  "assoc-acl",
		"Scope": "REGIONAL",
		"DefaultAction": map[string]any{
			"Allow": map[string]any{},
		},
		"VisibilityConfig": map[string]any{
			"SampledRequestsEnabled":   false,
			"CloudWatchMetricsEnabled": false,
			"MetricName":               "assoc-acl",
		},
	}))
	if err != nil {
		t.Fatalf("CreateWebACL: %v", err)
	}
	var createResult struct {
		Summary struct {
			ARN string `json:"ARN"`
		} `json:"Summary"`
	}
	if err := json.Unmarshal(resp.Body, &createResult); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}
	webACLArn := createResult.Summary.ARN
	resourceArn := "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/my-alb/abc123"

	// Associate.
	resp, err = p.HandleRequest(ctx, wafv2Request(t, "AssociateWebACL", map[string]any{
		"WebACLArn":   webACLArn,
		"ResourceArn": resourceArn,
	}))
	if err != nil {
		t.Fatalf("AssociateWebACL: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}

	// GetWebACLForResource.
	resp, err = p.HandleRequest(ctx, wafv2Request(t, "GetWebACLForResource", map[string]any{
		"ResourceArn": resourceArn,
	}))
	if err != nil {
		t.Fatalf("GetWebACLForResource: %v", err)
	}
	var assocResult struct {
		WebACL map[string]interface{} `json:"WebACL"`
	}
	if err := json.Unmarshal(resp.Body, &assocResult); err != nil {
		t.Fatalf("unmarshal assoc: %v", err)
	}
	if assocResult.WebACL["ARN"] != webACLArn {
		t.Errorf("want ARN=%q, got %q", webACLArn, assocResult.WebACL["ARN"])
	}

	// GetWebACLForResource for unassociated resource should return 404.
	_, err = p.HandleRequest(ctx, wafv2Request(t, "GetWebACLForResource", map[string]any{
		"ResourceArn": "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/other/xyz",
	}))
	if err == nil {
		t.Fatal("want error for unassociated resource, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok {
		t.Fatalf("want *AWSError, got %T", err)
	}
	if awsErr.Code != "WAFNonexistentItemException" {
		t.Errorf("want WAFNonexistentItemException, got %q", awsErr.Code)
	}
}

func TestWAFv2Plugin_IPSet_CRUD(t *testing.T) {
	p, ctx := setupWAFv2Plugin(t)

	// Create IPSet.
	resp, err := p.HandleRequest(ctx, wafv2Request(t, "CreateIPSet", map[string]any{
		"Name":        "my-ipset",
		"Scope":       "REGIONAL",
		"Description": "test ip set",
		"IPVersion":   "IPV4",
		"Addresses":   []string{"192.168.0.0/16"},
	}))
	if err != nil {
		t.Fatalf("CreateIPSet: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", resp.StatusCode, resp.Body)
	}

	var createResult struct {
		Summary struct {
			Id        string `json:"Id"`
			Name      string `json:"Name"`
			LockToken string `json:"LockToken"`
		} `json:"Summary"`
	}
	if err := json.Unmarshal(resp.Body, &createResult); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}
	if createResult.Summary.Id == "" {
		t.Error("want non-empty Id")
	}
	ipsetID := createResult.Summary.Id
	lockToken := createResult.Summary.LockToken

	// Get IPSet.
	resp, err = p.HandleRequest(ctx, wafv2Request(t, "GetIPSet", map[string]any{
		"Id":    ipsetID,
		"Name":  "my-ipset",
		"Scope": "REGIONAL",
	}))
	if err != nil {
		t.Fatalf("GetIPSet: %v", err)
	}
	var getResult struct {
		IPSet     substrate.WAFv2IPSet `json:"IPSet"`
		LockToken string               `json:"LockToken"`
	}
	if err := json.Unmarshal(resp.Body, &getResult); err != nil {
		t.Fatalf("unmarshal get: %v", err)
	}
	if getResult.IPSet.Name != "my-ipset" {
		t.Errorf("want Name=my-ipset, got %q", getResult.IPSet.Name)
	}
	if len(getResult.IPSet.Addresses) != 1 || getResult.IPSet.Addresses[0] != "192.168.0.0/16" {
		t.Errorf("want Addresses=[192.168.0.0/16], got %v", getResult.IPSet.Addresses)
	}

	// Update IPSet.
	resp, err = p.HandleRequest(ctx, wafv2Request(t, "UpdateIPSet", map[string]any{
		"Id":        ipsetID,
		"Name":      "my-ipset",
		"Scope":     "REGIONAL",
		"LockToken": lockToken,
		"Addresses": []string{"10.0.0.0/8", "172.16.0.0/12"},
	}))
	if err != nil {
		t.Fatalf("UpdateIPSet: %v", err)
	}
	var updateResult struct {
		NextLockToken string `json:"NextLockToken"`
	}
	if err := json.Unmarshal(resp.Body, &updateResult); err != nil {
		t.Fatalf("unmarshal update: %v", err)
	}
	if updateResult.NextLockToken == "" {
		t.Error("want non-empty NextLockToken")
	}
	newLockToken := updateResult.NextLockToken

	// Verify update.
	resp, err = p.HandleRequest(ctx, wafv2Request(t, "GetIPSet", map[string]any{
		"Id":    ipsetID,
		"Name":  "my-ipset",
		"Scope": "REGIONAL",
	}))
	if err != nil {
		t.Fatalf("GetIPSet after update: %v", err)
	}
	if err := json.Unmarshal(resp.Body, &getResult); err != nil {
		t.Fatalf("unmarshal after update: %v", err)
	}
	if len(getResult.IPSet.Addresses) != 2 {
		t.Errorf("want 2 addresses after update, got %d", len(getResult.IPSet.Addresses))
	}

	// List IPSets.
	resp, err = p.HandleRequest(ctx, wafv2Request(t, "ListIPSets", map[string]any{
		"Scope": "REGIONAL",
	}))
	if err != nil {
		t.Fatalf("ListIPSets: %v", err)
	}
	var listResult struct {
		IPSets []map[string]interface{} `json:"IPSets"`
	}
	if err := json.Unmarshal(resp.Body, &listResult); err != nil {
		t.Fatalf("unmarshal list: %v", err)
	}
	if len(listResult.IPSets) != 1 {
		t.Errorf("want 1 IPSet, got %d", len(listResult.IPSets))
	}

	// Delete IPSet.
	_, err = p.HandleRequest(ctx, wafv2Request(t, "DeleteIPSet", map[string]any{
		"Id":        ipsetID,
		"Name":      "my-ipset",
		"Scope":     "REGIONAL",
		"LockToken": newLockToken,
	}))
	if err != nil {
		t.Fatalf("DeleteIPSet: %v", err)
	}

	// List after delete — should be empty.
	resp, err = p.HandleRequest(ctx, wafv2Request(t, "ListIPSets", map[string]any{
		"Scope": "REGIONAL",
	}))
	if err != nil {
		t.Fatalf("ListIPSets after delete: %v", err)
	}
	if err := json.Unmarshal(resp.Body, &listResult); err != nil {
		t.Fatalf("unmarshal list after delete: %v", err)
	}
	if len(listResult.IPSets) != 0 {
		t.Errorf("want 0 IPSets after delete, got %d", len(listResult.IPSets))
	}
}

func TestWAFv2Plugin_DisassociateWebACL(t *testing.T) {
	p, ctx := setupWAFv2Plugin(t)

	// Create and associate.
	resp, err := p.HandleRequest(ctx, wafv2Request(t, "CreateWebACL", map[string]any{
		"Name":  "disassoc-acl",
		"Scope": "REGIONAL",
		"DefaultAction": map[string]any{
			"Allow": map[string]any{},
		},
		"VisibilityConfig": map[string]any{
			"SampledRequestsEnabled":   false,
			"CloudWatchMetricsEnabled": false,
			"MetricName":               "disassoc-acl",
		},
	}))
	if err != nil {
		t.Fatalf("CreateWebACL: %v", err)
	}
	var createResult struct {
		Summary struct {
			ARN string `json:"ARN"`
		} `json:"Summary"`
	}
	if err := json.Unmarshal(resp.Body, &createResult); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}

	resourceArn := "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/test-alb/def456"

	_, err = p.HandleRequest(ctx, wafv2Request(t, "AssociateWebACL", map[string]any{
		"WebACLArn":   createResult.Summary.ARN,
		"ResourceArn": resourceArn,
	}))
	if err != nil {
		t.Fatalf("AssociateWebACL: %v", err)
	}

	// Disassociate.
	_, err = p.HandleRequest(ctx, wafv2Request(t, "DisassociateWebACL", map[string]any{
		"ResourceArn": resourceArn,
	}))
	if err != nil {
		t.Fatalf("DisassociateWebACL: %v", err)
	}

	// GetWebACLForResource should now return not-found.
	_, err = p.HandleRequest(ctx, wafv2Request(t, "GetWebACLForResource", map[string]any{
		"ResourceArn": resourceArn,
	}))
	if err == nil {
		t.Fatal("want error after disassociate, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok {
		t.Fatalf("want *AWSError, got %T", err)
	}
	if awsErr.Code != "WAFNonexistentItemException" {
		t.Errorf("want WAFNonexistentItemException, got %q", awsErr.Code)
	}
}

func TestWAFv2Plugin_UnsupportedOperation(t *testing.T) {
	p, ctx := setupWAFv2Plugin(t)
	_, err := p.HandleRequest(ctx, wafv2Request(t, "TagResource", map[string]any{}))
	if err == nil {
		t.Fatal("want error for unsupported operation, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok {
		t.Fatalf("want *AWSError, got %T", err)
	}
	if awsErr.Code != "InvalidAction" {
		t.Errorf("want InvalidAction, got %q", awsErr.Code)
	}
}
