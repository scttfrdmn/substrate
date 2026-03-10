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

func setupACMPlugin(t *testing.T) (*substrate.ACMPlugin, *substrate.RequestContext) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := &substrate.ACMPlugin{}
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  substrate.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("ACMPlugin.Initialize: %v", err)
	}
	reqCtx := &substrate.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "test-req-1",
	}
	return p, reqCtx
}

func acmRequest(t *testing.T, op string, body map[string]any) *substrate.AWSRequest {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request body: %v", err)
	}
	return &substrate.AWSRequest{
		Service:   "acm",
		Operation: op,
		Headers:   map[string]string{"X-Amz-Target": "CertificateManager." + op},
		Body:      b,
		Params:    map[string]string{},
	}
}

func TestACMPlugin_RequestAndDescribe(t *testing.T) {
	p, ctx := setupACMPlugin(t)

	// Request a certificate.
	req := acmRequest(t, "RequestCertificate", map[string]any{
		"DomainName": "example.com",
	})
	resp, err := p.HandleRequest(ctx, req)
	if err != nil {
		t.Fatalf("RequestCertificate: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want status 200, got %d", resp.StatusCode)
	}

	var createOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	if err := json.Unmarshal(resp.Body, &createOut); err != nil {
		t.Fatalf("unmarshal create response: %v", err)
	}
	if createOut.CertificateArn == "" {
		t.Fatal("CertificateArn is empty")
	}

	// Describe the certificate.
	descReq := acmRequest(t, "DescribeCertificate", map[string]any{
		"CertificateArn": createOut.CertificateArn,
	})
	descResp, err := p.HandleRequest(ctx, descReq)
	if err != nil {
		t.Fatalf("DescribeCertificate: %v", err)
	}

	var descOut struct {
		Certificate substrate.ACMCertificate `json:"Certificate"`
	}
	if err := json.Unmarshal(descResp.Body, &descOut); err != nil {
		t.Fatalf("unmarshal describe response: %v", err)
	}
	if descOut.Certificate.CertificateArn != createOut.CertificateArn {
		t.Errorf("want ARN %q, got %q", createOut.CertificateArn, descOut.Certificate.CertificateArn)
	}
	if descOut.Certificate.Status != "ISSUED" {
		t.Errorf("want status ISSUED, got %q", descOut.Certificate.Status)
	}
	if descOut.Certificate.DomainName != "example.com" {
		t.Errorf("want domain example.com, got %q", descOut.Certificate.DomainName)
	}
}

func TestACMPlugin_ListCertificates(t *testing.T) {
	p, ctx := setupACMPlugin(t)

	for _, domain := range []string{"alpha.example.com", "beta.example.com"} {
		req := acmRequest(t, "RequestCertificate", map[string]any{"DomainName": domain})
		if _, err := p.HandleRequest(ctx, req); err != nil {
			t.Fatalf("RequestCertificate %s: %v", domain, err)
		}
	}

	listReq := acmRequest(t, "ListCertificates", map[string]any{})
	listResp, err := p.HandleRequest(ctx, listReq)
	if err != nil {
		t.Fatalf("ListCertificates: %v", err)
	}

	var out struct {
		CertificateSummaryList []struct {
			CertificateArn string `json:"CertificateArn"`
			DomainName     string `json:"DomainName"`
		} `json:"CertificateSummaryList"`
	}
	if err := json.Unmarshal(listResp.Body, &out); err != nil {
		t.Fatalf("unmarshal list response: %v", err)
	}
	if len(out.CertificateSummaryList) != 2 {
		t.Errorf("want 2 certificates, got %d", len(out.CertificateSummaryList))
	}
}

func TestACMPlugin_Tags(t *testing.T) {
	p, ctx := setupACMPlugin(t)

	// Create a certificate.
	req := acmRequest(t, "RequestCertificate", map[string]any{"DomainName": "tagged.example.com"})
	resp, err := p.HandleRequest(ctx, req)
	if err != nil {
		t.Fatalf("RequestCertificate: %v", err)
	}
	var createOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	if err := json.Unmarshal(resp.Body, &createOut); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	arn := createOut.CertificateArn

	// Add tags.
	addReq := acmRequest(t, "AddTagsToCertificate", map[string]any{
		"CertificateArn": arn,
		"Tags": []map[string]string{
			{"Key": "env", "Value": "test"},
			{"Key": "owner", "Value": "alice"},
		},
	})
	if _, err := p.HandleRequest(ctx, addReq); err != nil {
		t.Fatalf("AddTagsToCertificate: %v", err)
	}

	// List tags.
	listTagsReq := acmRequest(t, "ListTagsForCertificate", map[string]any{"CertificateArn": arn})
	listTagsResp, err := p.HandleRequest(ctx, listTagsReq)
	if err != nil {
		t.Fatalf("ListTagsForCertificate: %v", err)
	}
	var tagsOut struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	if err := json.Unmarshal(listTagsResp.Body, &tagsOut); err != nil {
		t.Fatalf("unmarshal tags: %v", err)
	}
	if len(tagsOut.Tags) != 2 {
		t.Errorf("want 2 tags, got %d", len(tagsOut.Tags))
	}

	// Remove one tag.
	removeReq := acmRequest(t, "RemoveTagsFromCertificate", map[string]any{
		"CertificateArn": arn,
		"Tags":           []map[string]string{{"Key": "owner", "Value": "alice"}},
	})
	if _, err := p.HandleRequest(ctx, removeReq); err != nil {
		t.Fatalf("RemoveTagsFromCertificate: %v", err)
	}

	// Verify one tag remains.
	listTagsResp2, err := p.HandleRequest(ctx, listTagsReq)
	if err != nil {
		t.Fatalf("ListTagsForCertificate after remove: %v", err)
	}
	var tagsOut2 struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	if err := json.Unmarshal(listTagsResp2.Body, &tagsOut2); err != nil {
		t.Fatalf("unmarshal tags2: %v", err)
	}
	if len(tagsOut2.Tags) != 1 {
		t.Errorf("want 1 tag after remove, got %d", len(tagsOut2.Tags))
	}
}

func TestACMPlugin_DeleteCertificate(t *testing.T) {
	p, ctx := setupACMPlugin(t)

	// Create a certificate.
	req := acmRequest(t, "RequestCertificate", map[string]any{"DomainName": "delete.example.com"})
	resp, err := p.HandleRequest(ctx, req)
	if err != nil {
		t.Fatalf("RequestCertificate: %v", err)
	}
	var createOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	if err := json.Unmarshal(resp.Body, &createOut); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	arn := createOut.CertificateArn

	// Delete it.
	delReq := acmRequest(t, "DeleteCertificate", map[string]any{"CertificateArn": arn})
	delResp, err := p.HandleRequest(ctx, delReq)
	if err != nil {
		t.Fatalf("DeleteCertificate: %v", err)
	}
	if delResp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", delResp.StatusCode)
	}

	// Describe should now return not-found.
	descReq := acmRequest(t, "DescribeCertificate", map[string]any{"CertificateArn": arn})
	_, err = p.HandleRequest(ctx, descReq)
	if err == nil {
		t.Fatal("expected error for deleted certificate, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok {
		t.Fatalf("expected *substrate.AWSError, got %T", err)
	}
	if awsErr.Code != "ResourceNotFoundException" {
		t.Errorf("want ResourceNotFoundException, got %q", awsErr.Code)
	}

	// List should be empty.
	listReq := acmRequest(t, "ListCertificates", map[string]any{})
	listResp, err := p.HandleRequest(ctx, listReq)
	if err != nil {
		t.Fatalf("ListCertificates: %v", err)
	}
	var listOut struct {
		CertificateSummaryList []any `json:"CertificateSummaryList"`
	}
	if err := json.Unmarshal(listResp.Body, &listOut); err != nil {
		t.Fatalf("unmarshal list: %v", err)
	}
	if len(listOut.CertificateSummaryList) != 0 {
		t.Errorf("want 0 certificates after delete, got %d", len(listOut.CertificateSummaryList))
	}
}
