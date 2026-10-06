package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// The three Cognito User Pools tagging operations (#1135).
//
// Each reads and writes the same stored map DescribeUserPool reports as `UserPoolTags`, so a tag set
// written by CreateUserPool, UpdateUserPool or TagResource is the one all of them report, and the one
// the resource-groups-tagging scanner reads (scanCognitoUserPools, which already listed pools).
//
// # The ARN accepted
//
// All three pages describe `ResourceArn` as "the Amazon Resource Name (ARN) of the user pool", and
// TagResource's description says it "assigns a set of tags to an Amazon Cognito user pool". No page
// names an app client, and API_UserPoolClientType publishes no ARN member for a client to be
// addressed by. So only a user-pool ARN, arn:aws:cognito-idp:{region}:{account}:userpool/{id}, is
// accepted. An ARN that is not one, or that names a pool absent from the caller's account and
// Region, does not resolve, and is refused with the published ResourceNotFoundException/400.
//
// # The response
//
// TagResource and UntagResource each publish "an HTTP 200 response with an empty HTTP body", so both
// answer an empty body rather than `{}`, following [appsyncDeleted]. (TagResource's Sample Response
// shows `{}`; the Response Elements section is the contract and says empty.)

// cognitoUserPoolARNPrefix is the published ARN form up to the pool ID.
func cognitoUserPoolARNPrefix(ctx *RequestContext) string {
	return "arn:aws:cognito-idp:" + ctx.Region + ":" + ctx.AccountID + ":userpool/"
}

// loadTaggedUserPool resolves a tagging operation's ResourceArn to a user pool in the caller's
// account and Region.
func (p *CognitoIDPPlugin) loadTaggedUserPool(ctx *RequestContext, resourceARN string) (*CognitoUserPool, error) {
	if resourceARN == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "ResourceArn is required", HTTPStatus: http.StatusBadRequest}
	}
	notFound := &AWSError{Code: "ResourceNotFoundException", Message: "Resource not found: " + resourceARN, HTTPStatus: http.StatusBadRequest}
	poolID, ok := strings.CutPrefix(resourceARN, cognitoUserPoolARNPrefix(ctx))
	if !ok || poolID == "" || strings.Contains(poolID, "/") {
		return nil, notFound
	}
	data, err := p.state.Get(context.Background(), cognitoIDPNamespace, cognitoUserPoolKey(ctx.AccountID, ctx.Region, poolID))
	if err != nil {
		return nil, fmt.Errorf("cognito-idp loadTaggedUserPool state.Get: %w", err)
	}
	if data == nil {
		return nil, notFound
	}
	var pool CognitoUserPool
	if err := json.Unmarshal(data, &pool); err != nil {
		return nil, fmt.Errorf("cognito-idp loadTaggedUserPool unmarshal: %w", err)
	}
	return &pool, nil
}

// saveUserPoolTags persists pool after a tag write, stamping the previously-tagged flag from the
// count held before the write.
func (p *CognitoIDPPlugin) saveUserPoolTags(ctx *RequestContext, pool *CognitoUserPool, tagsBefore, tagsAdded int) error {
	pool.EverTagged = taggingEverTagged(pool.EverTagged, tagsBefore, tagsAdded)
	data, err := json.Marshal(pool)
	if err != nil {
		return fmt.Errorf("cognito-idp saveUserPoolTags marshal: %w", err)
	}
	key := cognitoUserPoolKey(ctx.AccountID, ctx.Region, pool.UserPoolID)
	if err := p.state.Put(context.Background(), cognitoIDPNamespace, key, data); err != nil {
		return fmt.Errorf("cognito-idp saveUserPoolTags state.Put: %w", err)
	}
	return nil
}

// cognitoIDPEmpty is the empty-body 200 TagResource and UntagResource publish.
func cognitoIDPEmpty() *AWSResponse {
	return &AWSResponse{StatusCode: http.StatusOK, Headers: map[string]string{"Content-Type": "application/x-amz-json-1.1"}}
}

func (p *CognitoIDPPlugin) tagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ResourceArn string            `json:"ResourceArn"`
		Tags        map[string]string `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	pool, err := p.loadTaggedUserPool(ctx, body.ResourceArn)
	if err != nil {
		return nil, err
	}
	// Tags is Required: Yes on API_TagResource.
	if body.Tags == nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "Tags is required", HTTPStatus: http.StatusBadRequest}
	}
	before := len(pool.Tags)
	if pool.Tags == nil {
		pool.Tags = make(map[string]string, len(body.Tags))
	}
	for k, v := range body.Tags {
		pool.Tags[k] = v
	}
	if err := p.saveUserPoolTags(ctx, pool, before, len(body.Tags)); err != nil {
		return nil, err
	}
	return cognitoIDPEmpty(), nil
}

func (p *CognitoIDPPlugin) untagResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ResourceArn string   `json:"ResourceArn"`
		TagKeys     []string `json:"TagKeys"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	pool, err := p.loadTaggedUserPool(ctx, body.ResourceArn)
	if err != nil {
		return nil, err
	}
	// TagKeys is Required: Yes on API_UntagResource.
	if body.TagKeys == nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "TagKeys is required", HTTPStatus: http.StatusBadRequest}
	}
	before := len(pool.Tags)
	for _, k := range body.TagKeys {
		delete(pool.Tags, k)
	}
	if err := p.saveUserPoolTags(ctx, pool, before, 0); err != nil {
		return nil, err
	}
	return cognitoIDPEmpty(), nil
}

func (p *CognitoIDPPlugin) listTagsForResource(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ResourceArn string `json:"ResourceArn"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	pool, err := p.loadTaggedUserPool(ctx, body.ResourceArn)
	if err != nil {
		return nil, err
	}
	tags := pool.Tags
	if tags == nil {
		tags = map[string]string{}
	}
	return cognitoIDPJSONResponse(http.StatusOK, struct {
		Tags map[string]string `json:"Tags"`
	}{Tags: tags})
}
