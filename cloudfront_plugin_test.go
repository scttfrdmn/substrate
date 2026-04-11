package substrate_test

import (
	"context"
	"encoding/xml"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

func setupCloudFrontPlugin(t *testing.T) (*substrate.CloudFrontPlugin, *substrate.RequestContext) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := &substrate.CloudFrontPlugin{}
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  substrate.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("CloudFrontPlugin.Initialize: %v", err)
	}
	return p, &substrate.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: "req-1"}
}

const cfDistributionConfigXML = `<DistributionConfig><Comment>test distribution</Comment><Enabled>true</Enabled></DistributionConfig>`

func cfRequest(method, path string, params map[string]string, body string) *substrate.AWSRequest {
	if params == nil {
		params = map[string]string{}
	}
	return &substrate.AWSRequest{
		Service:   "cloudfront",
		Operation: method,
		Path:      path,
		Body:      []byte(body),
		Headers:   map[string]string{},
		Params:    params,
	}
}

// createTestDistribution creates a distribution and returns its ID.
func createTestDistribution(t *testing.T, p *substrate.CloudFrontPlugin, ctx *substrate.RequestContext) string {
	t.Helper()
	resp, err := p.HandleRequest(ctx, cfRequest(http.MethodPost, "/2020-05-31/distribution", nil, cfDistributionConfigXML))
	if err != nil {
		t.Fatalf("CreateDistribution: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("CreateDistribution: want 201, got %d; body=%s", resp.StatusCode, resp.Body)
	}

	var dist struct {
		XMLName xml.Name `xml:"Distribution"`
		ID      string   `xml:"Id"`
	}
	if err := xml.Unmarshal(resp.Body, &dist); err != nil {
		// Strip XML declaration if present and retry.
		stripped := strings.TrimPrefix(string(resp.Body), xml.Header)
		if err2 := xml.Unmarshal([]byte(stripped), &dist); err2 != nil {
			t.Fatalf("unmarshal CreateDistribution response: %v (original: %v)", err2, err)
		}
	}
	if dist.ID == "" {
		t.Fatal("CreateDistribution: Id is empty")
	}
	return dist.ID
}

func TestCloudFrontPlugin_CreateAndGetDistribution(t *testing.T) {
	p, ctx := setupCloudFrontPlugin(t)

	distID := createTestDistribution(t, p, ctx)

	// Verify ID format: E followed by 13 uppercase alphanumeric chars.
	if len(distID) != 14 {
		t.Errorf("distribution ID length want 14, got %d (%q)", len(distID), distID)
	}
	if distID[0] != 'E' {
		t.Errorf("distribution ID want E prefix, got %q", distID)
	}

	// Get distribution.
	resp, err := p.HandleRequest(ctx, cfRequest(http.MethodGet, "/2020-05-31/distribution/"+distID, nil, ""))
	if err != nil {
		t.Fatalf("GetDistribution: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GetDistribution: want 200, got %d", resp.StatusCode)
	}

	var result struct {
		XMLName    xml.Name `xml:"Distribution"`
		ID         string   `xml:"Id"`
		ARN        string   `xml:"ARN"`
		Status     string   `xml:"Status"`
		DomainName string   `xml:"DomainName"`
	}
	stripped := strings.TrimPrefix(string(resp.Body), xml.Header)
	if err := xml.Unmarshal([]byte(stripped), &result); err != nil {
		t.Fatalf("unmarshal GetDistribution response: %v", err)
	}

	if result.ID != distID {
		t.Errorf("want Id=%q, got %q", distID, result.ID)
	}
	if result.Status != "Deployed" {
		t.Errorf("want Status=Deployed, got %q", result.Status)
	}

	wantDomain := distID + ".cloudfront.net"
	if result.DomainName != wantDomain {
		t.Errorf("want DomainName=%q, got %q", wantDomain, result.DomainName)
	}

	wantARN := "arn:aws:cloudfront::123456789012:distribution/" + distID
	if result.ARN != wantARN {
		t.Errorf("want ARN=%q, got %q", wantARN, result.ARN)
	}
}

func TestCloudFrontPlugin_ListDistributions(t *testing.T) {
	p, ctx := setupCloudFrontPlugin(t)

	// Create 2 distributions.
	id1 := createTestDistribution(t, p, ctx)
	id2 := createTestDistribution(t, p, ctx)

	if id1 == id2 {
		t.Errorf("expected unique distribution IDs, got duplicate %q", id1)
	}

	// List distributions.
	resp, err := p.HandleRequest(ctx, cfRequest(http.MethodGet, "/2020-05-31/distribution", nil, ""))
	if err != nil {
		t.Fatalf("ListDistributions: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ListDistributions: want 200, got %d", resp.StatusCode)
	}

	var list struct {
		XMLName     xml.Name `xml:"DistributionList"`
		Quantity    int      `xml:"Quantity"`
		IsTruncated bool     `xml:"IsTruncated"`
		Items       []struct {
			ID string `xml:"Id"`
		} `xml:"Items>DistributionSummary"`
	}
	stripped := strings.TrimPrefix(string(resp.Body), xml.Header)
	if err := xml.Unmarshal([]byte(stripped), &list); err != nil {
		t.Fatalf("unmarshal ListDistributions response: %v", err)
	}

	if list.Quantity != 2 {
		t.Errorf("want Quantity=2, got %d", list.Quantity)
	}
	if len(list.Items) != 2 {
		t.Errorf("want 2 items, got %d", len(list.Items))
	}
	if list.IsTruncated {
		t.Error("want IsTruncated=false")
	}
}

func TestCloudFrontPlugin_CreateInvalidation(t *testing.T) {
	p, ctx := setupCloudFrontPlugin(t)

	distID := createTestDistribution(t, p, ctx)

	invalidationBody := `<InvalidationBatch><Paths><Quantity>1</Quantity><Items><Path>/*</Path></Items></Paths><CallerReference>ref-1</CallerReference></InvalidationBatch>`
	resp, err := p.HandleRequest(ctx, cfRequest(http.MethodPost, "/2020-05-31/distribution/"+distID+"/invalidation", nil, invalidationBody))
	if err != nil {
		t.Fatalf("CreateInvalidation: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("CreateInvalidation: want 201, got %d; body=%s", resp.StatusCode, resp.Body)
	}

	var inv struct {
		XMLName xml.Name `xml:"Invalidation"`
		ID      string   `xml:"Id"`
		Status  string   `xml:"Status"`
	}
	stripped := strings.TrimPrefix(string(resp.Body), xml.Header)
	if err := xml.Unmarshal([]byte(stripped), &inv); err != nil {
		t.Fatalf("unmarshal CreateInvalidation response: %v", err)
	}
	if inv.ID == "" {
		t.Error("CreateInvalidation: Id is empty")
	}
	if inv.Status != "Completed" {
		t.Errorf("want Status=Completed, got %q", inv.Status)
	}
	// Invalidation IDs start with "I".
	if !strings.HasPrefix(inv.ID, "I") {
		t.Errorf("want invalidation ID to start with I, got %q", inv.ID)
	}
}

func TestCloudFrontPlugin_DeleteDistribution(t *testing.T) {
	p, ctx := setupCloudFrontPlugin(t)

	distID := createTestDistribution(t, p, ctx)

	// Delete distribution.
	resp, err := p.HandleRequest(ctx, cfRequest(http.MethodDelete, "/2020-05-31/distribution/"+distID, nil, ""))
	if err != nil {
		t.Fatalf("DeleteDistribution: %v", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DeleteDistribution: want 204, got %d", resp.StatusCode)
	}

	// Getting it again should return 404.
	resp, err = p.HandleRequest(ctx, cfRequest(http.MethodGet, "/2020-05-31/distribution/"+distID, nil, ""))
	if err == nil {
		t.Fatal("GetDistribution after delete: expected error, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok {
		t.Fatalf("GetDistribution after delete: want *AWSError, got %T", err)
	}
	if awsErr.HTTPStatus != http.StatusNotFound {
		t.Errorf("want 404, got %d", awsErr.HTTPStatus)
	}
	_ = resp
}

func TestCloudFrontPlugin_UpdateDistribution(t *testing.T) {
	p, ctx := setupCloudFrontPlugin(t)
	distID := createTestDistribution(t, p, ctx)

	updatedConfig := `<DistributionConfig><Comment>updated distribution</Comment><Enabled>false</Enabled></DistributionConfig>`
	resp, err := p.HandleRequest(ctx, cfRequest(http.MethodPut, "/2020-05-31/distribution/"+distID+"/config", nil, updatedConfig))
	if err != nil {
		t.Fatalf("UpdateDistribution: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("UpdateDistribution: want 200, got %d; body=%s", resp.StatusCode, resp.Body)
	}
}

func TestCloudFrontPlugin_TagAndListTags(t *testing.T) {
	p, ctx := setupCloudFrontPlugin(t)
	distID := createTestDistribution(t, p, ctx)

	arn := "arn:aws:cloudfront::123456789012:distribution/" + distID

	// Tag the resource using query param Operation=Tag
	tagBody := `<Tags><Items><Tag><Key>env</Key><Value>prod</Value></Tag></Items></Tags>`
	tagResp, err := p.HandleRequest(ctx, cfRequest(http.MethodPost,
		"/2020-05-31/tagging",
		map[string]string{"Operation": "Tag", "Resource": arn},
		tagBody,
	))
	if err != nil {
		t.Fatalf("TagResource: %v", err)
	}
	if tagResp.StatusCode != http.StatusNoContent {
		t.Fatalf("TagResource: want 204, got %d; body=%s", tagResp.StatusCode, tagResp.Body)
	}

	// List tags.
	listResp, err := p.HandleRequest(ctx, cfRequest(http.MethodGet,
		"/2020-05-31/tagging",
		map[string]string{"Resource": arn},
		"",
	))
	if err != nil {
		t.Fatalf("ListTagsForResource: %v", err)
	}
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("ListTagsForResource: want 200, got %d", listResp.StatusCode)
	}

	// Just verify the body is non-empty XML.
	if len(listResp.Body) == 0 {
		t.Error("ListTagsForResource: empty response body")
	}
}

func TestCloudFrontPlugin_GetDistributionConfig(t *testing.T) {
	p, ctx := setupCloudFrontPlugin(t)

	distID := createTestDistribution(t, p, ctx)

	// Get distribution config via GET /2020-05-31/distribution/{id}/config.
	resp, err := p.HandleRequest(ctx, cfRequest(http.MethodGet, "/2020-05-31/distribution/"+distID+"/config", nil, ""))
	if err != nil {
		t.Fatalf("GetDistributionConfig: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GetDistributionConfig: want 200, got %d; body=%s", resp.StatusCode, resp.Body)
	}

	// The response body should contain XML with a DistributionConfig root element.
	stripped := strings.TrimPrefix(string(resp.Body), xml.Header)
	var result struct {
		XMLName xml.Name `xml:"DistributionConfig"`
		ID      string   `xml:"Id"`
	}
	if err := xml.Unmarshal([]byte(stripped), &result); err != nil {
		t.Fatalf("unmarshal GetDistributionConfig response: %v", err)
	}
	if result.ID != distID {
		t.Errorf("want Id=%q, got %q", distID, result.ID)
	}
}

func TestCloudFront_Invalidation_GetAndList(t *testing.T) {
	p, ctx := setupCloudFrontPlugin(t)

	// Create distribution.
	resp, err := p.HandleRequest(ctx, cfRequest("POST", "/2020-05-31/distribution", nil, cfDistributionConfigXML))
	if err != nil {
		t.Fatalf("CreateDistribution: %v", err)
	}
	var createResp struct {
		XMLName xml.Name `xml:"Distribution"`
		ID      string   `xml:"Id"`
	}
	if err := xml.Unmarshal(resp.Body, &createResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	distID := createResp.ID

	// Create invalidation.
	invResp, err := p.HandleRequest(ctx, cfRequest("POST", "/2020-05-31/distribution/"+distID+"/invalidation", nil, ""))
	if err != nil {
		t.Fatalf("CreateInvalidation: %v", err)
	}
	if invResp.StatusCode != http.StatusCreated {
		t.Fatalf("want 201, got %d", invResp.StatusCode)
	}
	var invResult struct {
		XMLName xml.Name `xml:"Invalidation"`
		ID      string   `xml:"Id"`
		Status  string   `xml:"Status"`
	}
	if err := xml.Unmarshal(invResp.Body, &invResult); err != nil {
		t.Fatalf("unmarshal invalidation: %v", err)
	}
	if invResult.Status != "Completed" {
		t.Errorf("want Status=Completed, got %q", invResult.Status)
	}
	if !strings.HasPrefix(invResult.ID, "I") {
		t.Errorf("invalidation ID should start with I, got %q", invResult.ID)
	}

	// GetInvalidation.
	getResp, err := p.HandleRequest(ctx, cfRequest("GET", "/2020-05-31/distribution/"+distID+"/invalidation/"+invResult.ID, nil, ""))
	if err != nil {
		t.Fatalf("GetInvalidation: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("GetInvalidation: want 200, got %d", getResp.StatusCode)
	}
	var getResult struct {
		XMLName xml.Name `xml:"Invalidation"`
		ID      string   `xml:"Id"`
	}
	if err := xml.Unmarshal(getResp.Body, &getResult); err != nil {
		t.Fatalf("unmarshal GetInvalidation: %v", err)
	}
	if getResult.ID != invResult.ID {
		t.Errorf("want ID=%q, got %q", invResult.ID, getResult.ID)
	}

	// ListInvalidations.
	listResp, err := p.HandleRequest(ctx, cfRequest("GET", "/2020-05-31/distribution/"+distID+"/invalidation", nil, ""))
	if err != nil {
		t.Fatalf("ListInvalidations: %v", err)
	}
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("ListInvalidations: want 200, got %d", listResp.StatusCode)
	}
	body := string(listResp.Body)
	if !strings.Contains(body, invResult.ID) {
		t.Errorf("ListInvalidations should contain invalidation ID %q", invResult.ID)
	}
	if !strings.Contains(body, "<Quantity>1</Quantity>") {
		t.Errorf("ListInvalidations should have Quantity=1, body: %s", body)
	}
}
