package substrate

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// CognitoIdentityPlugin emulates the Amazon Cognito Identity Pools JSON-protocol API.
// It handles identity pool lifecycle, GetId, GetCredentialsForIdentity, and role management.
type CognitoIdentityPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "cognito-identity".
func (p *CognitoIdentityPlugin) Name() string { return "cognito-identity" }

// Initialize sets up the CognitoIdentityPlugin with the provided configuration.
func (p *CognitoIdentityPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for CognitoIdentityPlugin.
func (p *CognitoIdentityPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a Cognito Identity JSON-protocol request to the appropriate handler.
// The operation is derived from the X-Amz-Target header suffix after the last dot.
func (p *CognitoIdentityPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op := req.Operation
	if target := req.Headers["X-Amz-Target"]; target != "" {
		if idx := strings.LastIndex(target, "."); idx >= 0 {
			op = target[idx+1:]
		}
	}

	switch op {
	case "CreateIdentityPool":
		return p.createIdentityPool(ctx, req)
	case "DescribeIdentityPool":
		return p.describeIdentityPool(ctx, req)
	case "DeleteIdentityPool":
		return p.deleteIdentityPool(ctx, req)
	case "ListIdentityPools":
		return p.listIdentityPools(ctx, req)
	case "GetId":
		return p.getID(ctx, req)
	case "GetCredentialsForIdentity":
		return p.getCredentialsForIdentity(ctx, req)
	case "SetIdentityPoolRoles":
		return p.setIdentityPoolRoles(ctx, req)
	case "GetIdentityPoolRoles":
		return p.getIdentityPoolRoles(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    fmt.Sprintf("CognitoIdentityPlugin: unknown operation %q", op),
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// --- Identity Pool operations ------------------------------------------------

func (p *CognitoIdentityPlugin) createIdentityPool(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		IdentityPoolName               string            `json:"IdentityPoolName"`
		AllowUnauthenticatedIdentities bool              `json:"AllowUnauthenticatedIdentities"`
		IdentityPoolTags               map[string]string `json:"IdentityPoolTags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.IdentityPoolName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "IdentityPoolName is required", HTTPStatus: http.StatusBadRequest}
	}

	poolID := generateIdentityPoolID(ctx.Region)
	now := p.tc.Now()
	pool := CognitoIdentityPool{
		IdentityPoolID:                 poolID,
		IdentityPoolName:               body.IdentityPoolName,
		AllowUnauthenticatedIdentities: body.AllowUnauthenticatedIdentities,
		Tags:                           body.IdentityPoolTags,
		CreationDate:                   now,
		AccountID:                      ctx.AccountID,
		Region:                         ctx.Region,
	}

	data, err := json.Marshal(pool)
	if err != nil {
		return nil, fmt.Errorf("cognito-identity createIdentityPool marshal: %w", err)
	}

	goCtx := context.Background()
	// Pool ID contains ":" which is safe for state key use here since the prefix is distinct.
	safeKey := cognitoIdentityPoolKey(ctx.AccountID, ctx.Region, poolID)
	if err := p.state.Put(goCtx, cognitoIdentityNamespace, safeKey, data); err != nil {
		return nil, fmt.Errorf("cognito-identity createIdentityPool state.Put: %w", err)
	}
	updateStringIndex(goCtx, p.state, cognitoIdentityNamespace, cognitoIdentityPoolIDsKey(ctx.AccountID, ctx.Region), poolID)

	type response struct {
		IdentityPoolID                 string            `json:"IdentityPoolId"`
		IdentityPoolName               string            `json:"IdentityPoolName"`
		AllowUnauthenticatedIdentities bool              `json:"AllowUnauthenticatedIdentities"`
		IdentityPoolTags               map[string]string `json:"IdentityPoolTags,omitempty"`
	}
	return cognitoIdentityJSONResponse(http.StatusOK, response{
		IdentityPoolID:                 pool.IdentityPoolID,
		IdentityPoolName:               pool.IdentityPoolName,
		AllowUnauthenticatedIdentities: pool.AllowUnauthenticatedIdentities,
		IdentityPoolTags:               pool.Tags,
	})
}

func (p *CognitoIdentityPlugin) describeIdentityPool(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		IdentityPoolID string `json:"IdentityPoolId"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	pool, err := p.loadIdentityPool(ctx, body.IdentityPoolID)
	if err != nil {
		return nil, err
	}
	type response struct {
		IdentityPoolID                 string            `json:"IdentityPoolId"`
		IdentityPoolName               string            `json:"IdentityPoolName"`
		AllowUnauthenticatedIdentities bool              `json:"AllowUnauthenticatedIdentities"`
		Roles                          map[string]string `json:"Roles,omitempty"`
		IdentityPoolTags               map[string]string `json:"IdentityPoolTags,omitempty"`
	}
	return cognitoIdentityJSONResponse(http.StatusOK, response{
		IdentityPoolID:                 pool.IdentityPoolID,
		IdentityPoolName:               pool.IdentityPoolName,
		AllowUnauthenticatedIdentities: pool.AllowUnauthenticatedIdentities,
		Roles:                          pool.Roles,
		IdentityPoolTags:               pool.Tags,
	})
}

func (p *CognitoIdentityPlugin) deleteIdentityPool(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		IdentityPoolID string `json:"IdentityPoolId"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if _, err := p.loadIdentityPool(ctx, body.IdentityPoolID); err != nil {
		return nil, err
	}
	goCtx := context.Background()
	_ = p.state.Delete(goCtx, cognitoIdentityNamespace, cognitoIdentityPoolKey(ctx.AccountID, ctx.Region, body.IdentityPoolID))
	removeFromStringIndex(goCtx, p.state, cognitoIdentityNamespace, cognitoIdentityPoolIDsKey(ctx.AccountID, ctx.Region), body.IdentityPoolID)
	return cognitoIdentityJSONResponse(http.StatusOK, struct{}{})
}

func (p *CognitoIdentityPlugin) listIdentityPools(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		MaxResults int    `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
	}
	_ = json.Unmarshal(req.Body, &body)
	if body.MaxResults <= 0 {
		body.MaxResults = 60
	}

	goCtx := context.Background()
	poolIDs, err := loadStringIndex(goCtx, p.state, cognitoIdentityNamespace, cognitoIdentityPoolIDsKey(ctx.AccountID, ctx.Region))
	if err != nil {
		return nil, fmt.Errorf("cognito-identity listIdentityPools loadStringIndex: %w", err)
	}

	type poolSummary struct {
		IdentityPoolID   string `json:"IdentityPoolId"`
		IdentityPoolName string `json:"IdentityPoolName"`
	}
	summaries := make([]poolSummary, 0, len(poolIDs))
	for _, id := range poolIDs {
		data, err := p.state.Get(goCtx, cognitoIdentityNamespace, cognitoIdentityPoolKey(ctx.AccountID, ctx.Region, id))
		if err != nil || data == nil {
			continue
		}
		var pool CognitoIdentityPool
		if json.Unmarshal(data, &pool) != nil {
			continue
		}
		summaries = append(summaries, poolSummary{
			IdentityPoolID:   pool.IdentityPoolID,
			IdentityPoolName: pool.IdentityPoolName,
		})
	}

	var nextToken string
	if len(summaries) > body.MaxResults {
		nextToken = summaries[body.MaxResults].IdentityPoolID
		summaries = summaries[:body.MaxResults]
	}

	type response struct {
		IdentityPools []poolSummary `json:"IdentityPools"`
		NextToken     string        `json:"NextToken,omitempty"`
	}
	return cognitoIdentityJSONResponse(http.StatusOK, response{IdentityPools: summaries, NextToken: nextToken})
}

// --- Identity operations -----------------------------------------------------

func (p *CognitoIdentityPlugin) getID(ctx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	identityID := generateIdentityPoolID(ctx.Region)
	type response struct {
		IdentityID string `json:"IdentityId"`
	}
	return cognitoIdentityJSONResponse(http.StatusOK, response{IdentityID: identityID})
}

func (p *CognitoIdentityPlugin) getCredentialsForIdentity(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		IdentityID string `json:"IdentityId"`
	}
	_ = json.Unmarshal(req.Body, &body)
	identityID := body.IdentityID
	if identityID == "" {
		identityID = generateIdentityPoolID(ctx.Region)
	}

	expiration := p.tc.Now().Add(time.Hour)
	type credentials struct {
		AccessKeyID  string    `json:"AccessKeyId"`
		SecretKey    string    `json:"SecretKey"`
		SessionToken string    `json:"SessionToken"`
		Expiration   time.Time `json:"Expiration"`
	}
	type response struct {
		IdentityID  string      `json:"IdentityId"`
		Credentials credentials `json:"Credentials"`
	}
	return cognitoIdentityJSONResponse(http.StatusOK, response{
		IdentityID: identityID,
		Credentials: credentials{
			AccessKeyID:  "AKIATEST12345678901",
			SecretKey:    "fakesecretkey",
			SessionToken: "fakesessiontoken",
			Expiration:   expiration,
		},
	})
}

// --- Role management ---------------------------------------------------------

func (p *CognitoIdentityPlugin) setIdentityPoolRoles(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		IdentityPoolID string            `json:"IdentityPoolId"`
		Roles          map[string]string `json:"Roles"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	pool, err := p.loadIdentityPool(ctx, body.IdentityPoolID)
	if err != nil {
		return nil, err
	}
	if pool.Roles == nil {
		pool.Roles = make(map[string]string)
	}
	for k, v := range body.Roles {
		pool.Roles[k] = v
	}

	data, err := json.Marshal(pool)
	if err != nil {
		return nil, fmt.Errorf("cognito-identity setIdentityPoolRoles marshal: %w", err)
	}
	goCtx := context.Background()
	if err := p.state.Put(goCtx, cognitoIdentityNamespace, cognitoIdentityPoolKey(ctx.AccountID, ctx.Region, pool.IdentityPoolID), data); err != nil {
		return nil, fmt.Errorf("cognito-identity setIdentityPoolRoles state.Put: %w", err)
	}
	return cognitoIdentityJSONResponse(http.StatusOK, struct{}{})
}

func (p *CognitoIdentityPlugin) getIdentityPoolRoles(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		IdentityPoolID string `json:"IdentityPoolId"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	pool, err := p.loadIdentityPool(ctx, body.IdentityPoolID)
	if err != nil {
		return nil, err
	}
	type response struct {
		IdentityPoolID string            `json:"IdentityPoolId"`
		Roles          map[string]string `json:"Roles"`
	}
	roles := pool.Roles
	if roles == nil {
		roles = map[string]string{}
	}
	return cognitoIdentityJSONResponse(http.StatusOK, response{
		IdentityPoolID: pool.IdentityPoolID,
		Roles:          roles,
	})
}

// --- Helpers -----------------------------------------------------------------

func (p *CognitoIdentityPlugin) loadIdentityPool(ctx *RequestContext, poolID string) (*CognitoIdentityPool, error) {
	if poolID == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "IdentityPoolId is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, cognitoIdentityNamespace, cognitoIdentityPoolKey(ctx.AccountID, ctx.Region, poolID))
	if err != nil {
		return nil, fmt.Errorf("cognito-identity loadIdentityPool state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Identity pool not found: " + poolID, HTTPStatus: http.StatusNotFound}
	}
	var pool CognitoIdentityPool
	if err := json.Unmarshal(data, &pool); err != nil {
		return nil, fmt.Errorf("cognito-identity loadIdentityPool unmarshal: %w", err)
	}
	return &pool, nil
}

// generateIdentityPoolID returns a Cognito Identity Pool ID of the form
// {region}:{xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx} using crypto/rand.
func generateIdentityPoolID(region string) string {
	return region + ":" + generateIdentityUUID()
}

// generateIdentityUUID generates a lowercase hex UUID in the standard
// xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx format using crypto/rand.
func generateIdentityUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	// Set version 4 and variant bits.
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return hex.EncodeToString(b[0:4]) + "-" +
		hex.EncodeToString(b[4:6]) + "-" +
		hex.EncodeToString(b[6:8]) + "-" +
		hex.EncodeToString(b[8:10]) + "-" +
		hex.EncodeToString(b[10:16])
}

// cognitoIdentityJSONResponse serializes v as JSON and returns an AWSResponse with
// the given HTTP status code and application/json content type.
func cognitoIdentityJSONResponse(status int, v any) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("cognitoIdentityJSONResponse marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}
