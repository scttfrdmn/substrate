package substrate

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// ACMPlugin emulates the AWS Certificate Manager JSON-protocol API.
// It handles RequestCertificate, DescribeCertificate, DeleteCertificate,
// ListCertificates, AddTagsToCertificate, RemoveTagsFromCertificate,
// ListTagsForCertificate, and RenewCertificate.
type ACMPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "acm".
func (p *ACMPlugin) Name() string { return "acm" }

// Initialize sets up the ACMPlugin with the provided configuration.
func (p *ACMPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for ACMPlugin.
func (p *ACMPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an ACM JSON-protocol request to the appropriate handler.
// The operation is derived from the X-Amz-Target header value after stripping the
// "CertificateManager." prefix.
func (p *ACMPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op := req.Operation
	if target := req.Headers["X-Amz-Target"]; target != "" {
		op = strings.TrimPrefix(target, "CertificateManager.")
	}

	switch op {
	case "RequestCertificate":
		return p.requestCertificate(ctx, req)
	case "DescribeCertificate":
		return p.describeCertificate(ctx, req)
	case "DeleteCertificate":
		return p.deleteCertificate(ctx, req)
	case "ListCertificates":
		return p.listCertificates(ctx, req)
	case "AddTagsToCertificate":
		return p.addTagsToCertificate(ctx, req)
	case "RemoveTagsFromCertificate":
		return p.removeTagsFromCertificate(ctx, req)
	case "ListTagsForCertificate":
		return p.listTagsForCertificate(ctx, req)
	case "RenewCertificate":
		return acmJSONResponse(http.StatusOK, struct{}{})
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    fmt.Sprintf("ACMPlugin: unknown operation %q", op),
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

func (p *ACMPlugin) requestCertificate(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		DomainName              string   `json:"DomainName"`
		SubjectAlternativeNames []string `json:"SubjectAlternativeNames"`
		KeyAlgorithm            string   `json:"KeyAlgorithm"`
		Tags                    []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.DomainName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "DomainName is required", HTTPStatus: http.StatusBadRequest}
	}

	keyAlgo := body.KeyAlgorithm
	if keyAlgo == "" {
		keyAlgo = "RSA_2048"
	}

	certID, err := generateACMCertID()
	if err != nil {
		return nil, fmt.Errorf("acm requestCertificate generateACMCertID: %w", err)
	}
	certArn := fmt.Sprintf("arn:aws:acm:%s:%s:certificate/%s", ctx.Region, ctx.AccountID, certID)

	tags := make(map[string]string, len(body.Tags))
	for _, t := range body.Tags {
		tags[t.Key] = t.Value
	}

	now := p.tc.Now()
	cert := ACMCertificate{
		CertificateArn:          certArn,
		DomainName:              body.DomainName,
		SubjectAlternativeNames: body.SubjectAlternativeNames,
		Status:                  "ISSUED",
		Type:                    "AMAZON_ISSUED",
		KeyAlgorithm:            keyAlgo,
		Tags:                    tags,
		CreatedAt:               now,
		IssuedAt:                now,
		NotBefore:               now,
		NotAfter:                now.AddDate(1, 0, 0),
		AccountID:               ctx.AccountID,
		Region:                  ctx.Region,
	}

	data, err := json.Marshal(cert)
	if err != nil {
		return nil, fmt.Errorf("acm requestCertificate marshal: %w", err)
	}

	goCtx := context.Background()
	stateKey := acmCertKey(ctx.AccountID, ctx.Region, certArn)
	if err := p.state.Put(goCtx, acmNamespace, stateKey, data); err != nil {
		return nil, fmt.Errorf("acm requestCertificate state.Put: %w", err)
	}

	idxKey := acmCertARNsKey(ctx.AccountID, ctx.Region)
	updateStringIndex(goCtx, p.state, acmNamespace, idxKey, certArn)

	type response struct {
		CertificateArn string `json:"CertificateArn"`
	}
	return acmJSONResponse(http.StatusOK, response{CertificateArn: certArn})
}

func (p *ACMPlugin) describeCertificate(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		CertificateArn string `json:"CertificateArn"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.CertificateArn == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "CertificateArn is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	stateKey := acmCertKey(ctx.AccountID, ctx.Region, body.CertificateArn)
	data, err := p.state.Get(goCtx, acmNamespace, stateKey)
	if err != nil {
		return nil, fmt.Errorf("acm describeCertificate state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Certificate not found: " + body.CertificateArn, HTTPStatus: http.StatusNotFound}
	}

	var cert ACMCertificate
	if err := json.Unmarshal(data, &cert); err != nil {
		return nil, fmt.Errorf("acm describeCertificate unmarshal: %w", err)
	}

	type response struct {
		Certificate ACMCertificate `json:"Certificate"`
	}
	return acmJSONResponse(http.StatusOK, response{Certificate: cert})
}

func (p *ACMPlugin) deleteCertificate(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		CertificateArn string `json:"CertificateArn"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.CertificateArn == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "CertificateArn is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	stateKey := acmCertKey(ctx.AccountID, ctx.Region, body.CertificateArn)
	data, err := p.state.Get(goCtx, acmNamespace, stateKey)
	if err != nil {
		return nil, fmt.Errorf("acm deleteCertificate state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Certificate not found: " + body.CertificateArn, HTTPStatus: http.StatusNotFound}
	}

	if err := p.state.Delete(goCtx, acmNamespace, stateKey); err != nil {
		return nil, fmt.Errorf("acm deleteCertificate state.Delete: %w", err)
	}

	idxKey := acmCertARNsKey(ctx.AccountID, ctx.Region)
	removeFromStringIndex(goCtx, p.state, acmNamespace, idxKey, body.CertificateArn)

	return acmJSONResponse(http.StatusOK, struct{}{})
}

func (p *ACMPlugin) listCertificates(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		CertificateStatuses []string `json:"CertificateStatuses"`
		NextToken           string   `json:"NextToken"`
		MaxItems            int      `json:"MaxItems"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &body)
	}

	goCtx := context.Background()
	idxKey := acmCertARNsKey(ctx.AccountID, ctx.Region)
	allARNs, err := loadStringIndex(goCtx, p.state, acmNamespace, idxKey)
	if err != nil {
		return nil, fmt.Errorf("acm listCertificates loadIndex: %w", err)
	}

	type certSummary struct {
		CertificateArn string `json:"CertificateArn"`
		DomainName     string `json:"DomainName"`
		Status         string `json:"Status"`
	}

	summaries := make([]certSummary, 0, len(allARNs))
	for _, arn := range allARNs {
		data, getErr := p.state.Get(goCtx, acmNamespace, acmCertKey(ctx.AccountID, ctx.Region, arn))
		if getErr != nil || data == nil {
			continue
		}
		var cert ACMCertificate
		if json.Unmarshal(data, &cert) != nil {
			continue
		}
		if len(body.CertificateStatuses) > 0 {
			found := false
			for _, s := range body.CertificateStatuses {
				if s == cert.Status {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		summaries = append(summaries, certSummary{
			CertificateArn: cert.CertificateArn,
			DomainName:     cert.DomainName,
			Status:         cert.Status,
		})
	}

	type response struct {
		CertificateSummaryList []certSummary `json:"CertificateSummaryList"`
	}
	return acmJSONResponse(http.StatusOK, response{CertificateSummaryList: summaries})
}

func (p *ACMPlugin) addTagsToCertificate(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		CertificateArn string `json:"CertificateArn"`
		Tags           []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.CertificateArn == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "CertificateArn is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	stateKey := acmCertKey(ctx.AccountID, ctx.Region, body.CertificateArn)
	data, err := p.state.Get(goCtx, acmNamespace, stateKey)
	if err != nil {
		return nil, fmt.Errorf("acm addTagsToCertificate state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Certificate not found: " + body.CertificateArn, HTTPStatus: http.StatusNotFound}
	}

	var cert ACMCertificate
	if err := json.Unmarshal(data, &cert); err != nil {
		return nil, fmt.Errorf("acm addTagsToCertificate unmarshal: %w", err)
	}

	if cert.Tags == nil {
		cert.Tags = make(map[string]string)
	}
	for _, t := range body.Tags {
		cert.Tags[t.Key] = t.Value
	}

	updated, err := json.Marshal(cert)
	if err != nil {
		return nil, fmt.Errorf("acm addTagsToCertificate marshal: %w", err)
	}
	if err := p.state.Put(goCtx, acmNamespace, stateKey, updated); err != nil {
		return nil, fmt.Errorf("acm addTagsToCertificate state.Put: %w", err)
	}

	return acmJSONResponse(http.StatusOK, struct{}{})
}

func (p *ACMPlugin) removeTagsFromCertificate(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		CertificateArn string `json:"CertificateArn"`
		Tags           []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.CertificateArn == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "CertificateArn is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	stateKey := acmCertKey(ctx.AccountID, ctx.Region, body.CertificateArn)
	data, err := p.state.Get(goCtx, acmNamespace, stateKey)
	if err != nil {
		return nil, fmt.Errorf("acm removeTagsFromCertificate state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Certificate not found: " + body.CertificateArn, HTTPStatus: http.StatusNotFound}
	}

	var cert ACMCertificate
	if err := json.Unmarshal(data, &cert); err != nil {
		return nil, fmt.Errorf("acm removeTagsFromCertificate unmarshal: %w", err)
	}

	for _, t := range body.Tags {
		delete(cert.Tags, t.Key)
	}

	updated, err := json.Marshal(cert)
	if err != nil {
		return nil, fmt.Errorf("acm removeTagsFromCertificate marshal: %w", err)
	}
	if err := p.state.Put(goCtx, acmNamespace, stateKey, updated); err != nil {
		return nil, fmt.Errorf("acm removeTagsFromCertificate state.Put: %w", err)
	}

	return acmJSONResponse(http.StatusOK, struct{}{})
}

func (p *ACMPlugin) listTagsForCertificate(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		CertificateArn string `json:"CertificateArn"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.CertificateArn == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "CertificateArn is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	stateKey := acmCertKey(ctx.AccountID, ctx.Region, body.CertificateArn)
	data, err := p.state.Get(goCtx, acmNamespace, stateKey)
	if err != nil {
		return nil, fmt.Errorf("acm listTagsForCertificate state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Certificate not found: " + body.CertificateArn, HTTPStatus: http.StatusNotFound}
	}

	var cert ACMCertificate
	if err := json.Unmarshal(data, &cert); err != nil {
		return nil, fmt.Errorf("acm listTagsForCertificate unmarshal: %w", err)
	}

	type tagItem struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	}
	tags := make([]tagItem, 0, len(cert.Tags))
	for k, v := range cert.Tags {
		tags = append(tags, tagItem{Key: k, Value: v})
	}

	type response struct {
		Tags []tagItem `json:"Tags"`
	}
	return acmJSONResponse(http.StatusOK, response{Tags: tags})
}

// generateACMCertID generates a UUID-like string for an ACM certificate ID
// using cryptographically random bytes.
func generateACMCertID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generateACMCertID rand.Read: %w", err)
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

// acmJSONResponse marshals v as JSON and returns an AWSResponse with
// Content-Type: application/json and the given HTTP status code.
func acmJSONResponse(status int, v any) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("acmJSONResponse marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       body,
	}, nil
}
