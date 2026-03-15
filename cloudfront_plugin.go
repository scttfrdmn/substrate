package substrate

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// CloudFrontPlugin emulates the Amazon CloudFront REST/XML API.
// It handles distribution lifecycle, invalidation, and tagging operations.
// CloudFront is a global service — distributions are stored keyed by account
// ID only, without a region component.
type CloudFrontPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "cloudfront".
func (p *CloudFrontPlugin) Name() string { return "cloudfront" }

// Initialize sets up the CloudFrontPlugin with the provided configuration.
func (p *CloudFrontPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for CloudFrontPlugin.
func (p *CloudFrontPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a CloudFront REST/XML request to the appropriate handler.
// The operation is derived from the HTTP method and URL path.
func (p *CloudFrontPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op, distID := parseCloudFrontOperation(req.Operation, req.Path, req.Params)
	switch op {
	case "CreateDistribution":
		return p.createDistribution(ctx, req)
	case "GetDistribution":
		return p.getDistribution(ctx, req, distID)
	case "GetDistributionConfig":
		return p.getDistributionConfig(ctx, req, distID)
	case "UpdateDistribution":
		return p.updateDistribution(ctx, req, distID)
	case "DeleteDistribution":
		return p.deleteDistribution(ctx, req, distID)
	case "ListDistributions":
		return p.listDistributions(ctx, req)
	case "CreateInvalidation":
		return p.createInvalidation(ctx, req, distID)
	case "TagResource":
		return p.tagResource(ctx, req)
	case "ListTagsForResource":
		return p.listTagsForResource(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "CloudFrontPlugin: unsupported operation for path " + req.Path,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// parseCloudFrontOperation derives the CloudFront operation name and optional
// distribution ID from the HTTP method, URL path, and query parameters.
func parseCloudFrontOperation(method, path string, params map[string]string) (op, distID string) {
	// Normalise path: strip trailing slash and leading "/2020-05-31".
	const apiVersion = "/2020-05-31"
	p2 := strings.TrimSuffix(path, "/")
	p2 = strings.TrimPrefix(p2, apiVersion)

	// Tag operations — check query params first.
	if method == http.MethodPost {
		if op, ok := params["Operation"]; ok && strings.EqualFold(op, "Tag") {
			return "TagResource", ""
		}
	}
	if params["Resource"] != "" {
		switch method {
		case http.MethodGet:
			return "ListTagsForResource", ""
		case http.MethodPost:
			return "TagResource", ""
		}
	}

	switch {
	case p2 == "/distribution" && method == http.MethodPost:
		return "CreateDistribution", ""
	case p2 == "/distribution" && method == http.MethodGet:
		return "ListDistributions", ""
	case p2 == "/tagging" || p2 == "/tags":
		switch method {
		case http.MethodPost:
			return "TagResource", ""
		case http.MethodGet:
			return "ListTagsForResource", ""
		}
	}

	// Paths of the form /distribution/{id}[/...]
	const distPrefix = "/distribution/"
	if !strings.HasPrefix(p2, distPrefix) {
		return "", ""
	}
	rest := p2[len(distPrefix):]

	// Extract the distribution ID (first segment).
	slash := strings.IndexByte(rest, '/')
	if slash < 0 {
		// /distribution/{id}
		switch method {
		case http.MethodGet:
			return "GetDistribution", rest
		case http.MethodPut:
			return "UpdateDistribution", rest
		case http.MethodDelete:
			return "DeleteDistribution", rest
		}
		return "", rest
	}

	id := rest[:slash]
	suffix := rest[slash+1:]

	switch suffix {
	case "config":
		switch method {
		case http.MethodGet:
			return "GetDistributionConfig", id
		case http.MethodPut:
			return "UpdateDistribution", id
		}
	case "invalidation":
		if method == http.MethodPost {
			return "CreateInvalidation", id
		}
	}
	return "", id
}

// --- Distribution operations ------------------------------------------------

func (p *CloudFrontPlugin) createDistribution(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	// Parse optional comment and enabled flag from XML body.
	var xmlBody struct {
		XMLName xml.Name `xml:"DistributionConfig"`
		Comment string   `xml:"Comment"`
		Enabled string   `xml:"Enabled"`
	}
	if len(req.Body) > 0 {
		// Tolerate wrapper element names (CreateDistributionRequest, DistributionConfig).
		_ = xml.NewDecoder(bytes.NewReader(req.Body)).Decode(&xmlBody)
	}

	distID, err := generateCloudFrontID()
	if err != nil {
		return nil, fmt.Errorf("cloudfront createDistribution generateID: %w", err)
	}

	enabled := !strings.EqualFold(xmlBody.Enabled, "false")
	arn := fmt.Sprintf("arn:aws:cloudfront::%s:distribution/%s", ctx.AccountID, distID)
	domainName := distID + ".cloudfront.net"
	now := p.tc.Now()

	dist := CloudFrontDistribution{
		ID:               distID,
		ARN:              arn,
		Status:           "Deployed",
		DomainName:       domainName,
		Comment:          xmlBody.Comment,
		Enabled:          enabled,
		Tags:             map[string]string{},
		CreatedTime:      now,
		LastModifiedTime: now,
		AccountID:        ctx.AccountID,
	}

	data, err := json.Marshal(dist)
	if err != nil {
		return nil, fmt.Errorf("cloudfront createDistribution marshal: %w", err)
	}

	goCtx := context.Background()
	stateKey := cfDistKey(ctx.AccountID, distID)
	if err := p.state.Put(goCtx, cloudfrontNamespace, stateKey, data); err != nil {
		return nil, fmt.Errorf("cloudfront createDistribution state.Put: %w", err)
	}

	idxKey := cfDistIDsKey(ctx.AccountID)
	updateStringIndex(goCtx, p.state, cloudfrontNamespace, idxKey, distID)

	type xmlDist struct {
		XMLName    xml.Name `xml:"Distribution"`
		ID         string   `xml:"Id"`
		ARN        string   `xml:"ARN"`
		Status     string   `xml:"Status"`
		DomainName string   `xml:"DomainName"`
	}
	return cloudfrontXMLResponse(http.StatusCreated, xmlDist{
		ID:         distID,
		ARN:        arn,
		Status:     "Deployed",
		DomainName: domainName,
	})
}

func (p *CloudFrontPlugin) getDistribution(ctx *RequestContext, _ *AWSRequest, distID string) (*AWSResponse, error) {
	dist, err := p.loadDistribution(ctx, distID)
	if err != nil {
		return nil, err
	}
	return p.marshalDistributionXML(dist)
}

func (p *CloudFrontPlugin) getDistributionConfig(ctx *RequestContext, _ *AWSRequest, distID string) (*AWSResponse, error) {
	dist, err := p.loadDistribution(ctx, distID)
	if err != nil {
		return nil, err
	}

	type xmlConfig struct {
		XMLName xml.Name `xml:"DistributionConfig"`
		ID      string   `xml:"Id"`
		ARN     string   `xml:"ARN"`
		Comment string   `xml:"Comment,omitempty"`
		Enabled bool     `xml:"Enabled"`
	}
	return cloudfrontXMLResponse(http.StatusOK, xmlConfig{
		ID:      dist.ID,
		ARN:     dist.ARN,
		Comment: dist.Comment,
		Enabled: dist.Enabled,
	})
}

func (p *CloudFrontPlugin) updateDistribution(ctx *RequestContext, req *AWSRequest, distID string) (*AWSResponse, error) {
	dist, err := p.loadDistribution(ctx, distID)
	if err != nil {
		return nil, err
	}

	var xmlBody struct {
		XMLName xml.Name `xml:"DistributionConfig"`
		Comment string   `xml:"Comment"`
		Enabled string   `xml:"Enabled"`
	}
	if len(req.Body) > 0 {
		_ = xml.NewDecoder(bytes.NewReader(req.Body)).Decode(&xmlBody)
	}
	if xmlBody.Comment != "" {
		dist.Comment = xmlBody.Comment
	}
	if xmlBody.Enabled != "" {
		dist.Enabled = !strings.EqualFold(xmlBody.Enabled, "false")
	}
	dist.LastModifiedTime = p.tc.Now()

	data, err := json.Marshal(dist)
	if err != nil {
		return nil, fmt.Errorf("cloudfront updateDistribution marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), cloudfrontNamespace, cfDistKey(ctx.AccountID, distID), data); err != nil {
		return nil, fmt.Errorf("cloudfront updateDistribution state.Put: %w", err)
	}

	return p.marshalDistributionXML(dist)
}

func (p *CloudFrontPlugin) deleteDistribution(ctx *RequestContext, _ *AWSRequest, distID string) (*AWSResponse, error) {
	if _, err := p.loadDistribution(ctx, distID); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	if err := p.state.Delete(goCtx, cloudfrontNamespace, cfDistKey(ctx.AccountID, distID)); err != nil {
		return nil, fmt.Errorf("cloudfront deleteDistribution state.Delete: %w", err)
	}

	idxKey := cfDistIDsKey(ctx.AccountID)
	removeFromStringIndex(goCtx, p.state, cloudfrontNamespace, idxKey, distID)

	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}, Body: nil}, nil
}

func (p *CloudFrontPlugin) listDistributions(ctx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	idxKey := cfDistIDsKey(ctx.AccountID)
	ids, err := loadStringIndex(goCtx, p.state, cloudfrontNamespace, idxKey)
	if err != nil {
		return nil, fmt.Errorf("cloudfront listDistributions loadIndex: %w", err)
	}

	type xmlSummary struct {
		XMLName    xml.Name `xml:"DistributionSummary"`
		ID         string   `xml:"Id"`
		ARN        string   `xml:"ARN"`
		Status     string   `xml:"Status"`
		DomainName string   `xml:"DomainName"`
		Comment    string   `xml:"Comment,omitempty"`
		Enabled    bool     `xml:"Enabled"`
	}

	summaries := make([]xmlSummary, 0, len(ids))
	for _, id := range ids {
		data, getErr := p.state.Get(goCtx, cloudfrontNamespace, cfDistKey(ctx.AccountID, id))
		if getErr != nil || data == nil {
			continue
		}
		var dist CloudFrontDistribution
		if err := json.Unmarshal(data, &dist); err != nil {
			continue
		}
		summaries = append(summaries, xmlSummary{
			ID:         dist.ID,
			ARN:        dist.ARN,
			Status:     dist.Status,
			DomainName: dist.DomainName,
			Comment:    dist.Comment,
			Enabled:    dist.Enabled,
		})
	}

	type xmlList struct {
		XMLName     xml.Name     `xml:"DistributionList"`
		Items       []xmlSummary `xml:"Items>DistributionSummary"`
		Quantity    int          `xml:"Quantity"`
		IsTruncated bool         `xml:"IsTruncated"`
	}
	return cloudfrontXMLResponse(http.StatusOK, xmlList{
		Items:       summaries,
		Quantity:    len(summaries),
		IsTruncated: false,
	})
}

// --- Invalidation -----------------------------------------------------------

func (p *CloudFrontPlugin) createInvalidation(ctx *RequestContext, _ *AWSRequest, distID string) (*AWSResponse, error) {
	if _, err := p.loadDistribution(ctx, distID); err != nil {
		return nil, err
	}

	invID, err := generateCloudFrontID()
	if err != nil {
		return nil, fmt.Errorf("cloudfront createInvalidation generateID: %w", err)
	}
	// Use I prefix for invalidation IDs per CloudFront API convention.
	invID = "I" + invID[1:]

	type xmlInvalidation struct {
		XMLName    xml.Name `xml:"Invalidation"`
		ID         string   `xml:"Id"`
		Status     string   `xml:"Status"`
		CreateTime string   `xml:"CreateTime"`
	}
	return cloudfrontXMLResponse(http.StatusCreated, xmlInvalidation{
		ID:         invID,
		Status:     "Completed",
		CreateTime: p.tc.Now().UTC().Format(time.RFC3339),
	})
}

// --- Tagging ----------------------------------------------------------------

func (p *CloudFrontPlugin) tagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	// The Resource query param identifies the distribution ARN.
	resourceARN := req.Params["Resource"]
	if resourceARN == "" {
		return nil, &AWSError{Code: "InvalidArgument", Message: "Resource query parameter is required", HTTPStatus: http.StatusBadRequest}
	}

	// Extract distribution ID from ARN: arn:aws:cloudfront::{acct}:distribution/{id}
	distID := extractDistIDFromARN(resourceARN)
	if distID == "" {
		return nil, &AWSError{Code: "NoSuchDistribution", Message: "Distribution not found for ARN: " + resourceARN, HTTPStatus: http.StatusNotFound}
	}

	dist, err := p.loadDistribution(ctx, distID)
	if err != nil {
		return nil, err
	}

	// Parse XML tags from body.
	var xmlTags struct {
		XMLName xml.Name `xml:"Tags"`
		Items   []struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		} `xml:"Items>Tag"`
	}
	if len(req.Body) > 0 {
		_ = xml.NewDecoder(bytes.NewReader(req.Body)).Decode(&xmlTags)
	}

	if dist.Tags == nil {
		dist.Tags = make(map[string]string)
	}
	for _, tag := range xmlTags.Items {
		dist.Tags[tag.Key] = tag.Value
	}

	data, err := json.Marshal(dist)
	if err != nil {
		return nil, fmt.Errorf("cloudfront tagResource marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), cloudfrontNamespace, cfDistKey(ctx.AccountID, distID), data); err != nil {
		return nil, fmt.Errorf("cloudfront tagResource state.Put: %w", err)
	}

	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}, Body: nil}, nil
}

func (p *CloudFrontPlugin) listTagsForResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	resourceARN := req.Params["Resource"]
	if resourceARN == "" {
		return nil, &AWSError{Code: "InvalidArgument", Message: "Resource query parameter is required", HTTPStatus: http.StatusBadRequest}
	}

	distID := extractDistIDFromARN(resourceARN)
	if distID == "" {
		return nil, &AWSError{Code: "NoSuchDistribution", Message: "Distribution not found for ARN: " + resourceARN, HTTPStatus: http.StatusNotFound}
	}

	dist, err := p.loadDistribution(ctx, distID)
	if err != nil {
		return nil, err
	}

	type xmlTag struct {
		XMLName xml.Name `xml:"Tag"`
		Key     string   `xml:"Key"`
		Value   string   `xml:"Value"`
	}
	type xmlTags struct {
		XMLName xml.Name `xml:"Tags"`
		Items   []xmlTag `xml:"Items>Tag"`
	}

	tags := make([]xmlTag, 0, len(dist.Tags))
	for k, v := range dist.Tags {
		tags = append(tags, xmlTag{Key: k, Value: v})
	}

	return cloudfrontXMLResponse(http.StatusOK, xmlTags{Items: tags})
}

// --- Helpers ----------------------------------------------------------------

// loadDistribution loads a CloudFrontDistribution from state, returning a
// NoSuchDistribution error if absent.
func (p *CloudFrontPlugin) loadDistribution(ctx *RequestContext, distID string) (CloudFrontDistribution, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, cloudfrontNamespace, cfDistKey(ctx.AccountID, distID))
	if err != nil {
		return CloudFrontDistribution{}, fmt.Errorf("cloudfront loadDistribution state.Get: %w", err)
	}
	if data == nil {
		return CloudFrontDistribution{}, &AWSError{
			Code:       "NoSuchDistribution",
			Message:    "Distribution not found: " + distID,
			HTTPStatus: http.StatusNotFound,
		}
	}
	var dist CloudFrontDistribution
	if err := json.Unmarshal(data, &dist); err != nil {
		return CloudFrontDistribution{}, fmt.Errorf("cloudfront loadDistribution unmarshal: %w", err)
	}
	return dist, nil
}

// marshalDistributionXML serializes a distribution to a <Distribution> XML response.
func (p *CloudFrontPlugin) marshalDistributionXML(dist CloudFrontDistribution) (*AWSResponse, error) {
	type xmlDist struct {
		XMLName          xml.Name `xml:"Distribution"`
		ID               string   `xml:"Id"`
		ARN              string   `xml:"ARN"`
		Status           string   `xml:"Status"`
		DomainName       string   `xml:"DomainName"`
		Comment          string   `xml:"Comment,omitempty"`
		Enabled          bool     `xml:"Enabled"`
		LastModifiedTime string   `xml:"LastModifiedTime"`
	}
	return cloudfrontXMLResponse(http.StatusOK, xmlDist{
		ID:               dist.ID,
		ARN:              dist.ARN,
		Status:           dist.Status,
		DomainName:       dist.DomainName,
		Comment:          dist.Comment,
		Enabled:          dist.Enabled,
		LastModifiedTime: dist.LastModifiedTime.UTC().Format(time.RFC3339),
	})
}

// generateCloudFrontID generates a CloudFront-style distribution identifier
// of the form E followed by 13 uppercase alphanumeric characters.
func generateCloudFrontID() (string, error) {
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 13)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generateCloudFrontID rand.Read: %w", err)
	}
	out := make([]byte, 13)
	for i, ch := range b {
		out[i] = chars[int(ch)%len(chars)]
	}
	return "E" + string(out), nil
}

// extractDistIDFromARN extracts the distribution ID from a CloudFront ARN of the
// form arn:aws:cloudfront::{acct}:distribution/{id}.
func extractDistIDFromARN(arn string) string {
	const prefix = "distribution/"
	idx := strings.LastIndex(arn, prefix)
	if idx < 0 {
		return ""
	}
	return arn[idx+len(prefix):]
}

// cloudfrontXMLResponse serializes v to XML and returns an AWSResponse with
// Content-Type: application/xml and the given HTTP status code.
func cloudfrontXMLResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := xml.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("cloudfront xml marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/xml"},
		Body:       append([]byte(xml.Header), body...),
	}, nil
}
