package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// API Gateway v2's own tagging operations, and the record that says an API has carried a tag (#1378).
//
// The tags-resource-arn page publishes three operations on /v2/tags/{resource-arn}: TagResource
// (POST, a {"tags": {…}} body, 201 with no body), GetTags (GET, 200 with {"tags": {…}}) and
// UntagResource (DELETE, a required repeated tagKeys query parameter, 204). Each publishes
// BadRequestException/400 and NotFoundException/404. Substrate routed none of them, so an HTTP or
// WebSocket API's tags could be set only at CreateApi and never read back, and the Resource Groups
// Tagging API had no v2 arm to write them through either.
//
// Only an API (/apis/{id}) is taggable here. AWS also tags stages, domain names and VPC links through
// the same operations; substrate stores no tags on those records, so their ARNs are refused with
// BadRequestException rather than answered as if tagged. That refusal is substrate's reading: the page
// publishes the code for an invalid parameter, not a list of taggable types.

// apigwv2APIKeyPrefix is the colon-terminated prefix every v2 API record is stored under; see
// [apigwv2APIKey]. The namespace also holds routes, integrations, stages, indexes and the tag-history
// side-car, and "apiv2" is a prefix of "apiv2_ids" and "apiv2_tagged", which is why the guard tests the
// colon.
const apigwv2APIKeyPrefix = "apiv2:"

// apigwv2APITaggedKeyPrefix prefixes the side-car record that says a v2 API has carried a tag.
//
// GetResources reports a resource that is tagged or ever was (#938). Every other REST-model record
// the tagging API scans holds that as an ever_tagged member; V2ApiState does not, for the reason
// [cwlLogGroupTaggedKeyPrefix] gives for log groups: such a member is a substrate-bookkeeping
// declaration on a persisted record, the class scripts/check-wire-bookkeeping.sh inventories, and a
// side-car keyed by the API's state key carries it without adding one. Every writer of an API's tags
// holds that key: API Gateway v2's TagResource and UntagResource, and the tagging API's merge arm.
// CreateApi's inline tags need no stamp, because an API holding a tag is reported for holding it, and
// the write that later empties the set stamps the flag. DeleteApi removes the side-car with the API,
// so an API re-created under the same ID starts never-tagged.
const apigwv2APITaggedKeyPrefix = "apiv2_tagged:"

// apigwv2KeyIsTaggable reports whether a v2 state key names a record that stores tags: an API.
func apigwv2KeyIsTaggable(key string) bool {
	return strings.HasPrefix(key, apigwv2APIKeyPrefix)
}

// apigwv2APITaggedKey returns the tag-history side-car key for the API stored at apiKey.
func apigwv2APITaggedKey(apiKey string) string {
	return apigwv2APITaggedKeyPrefix + strings.TrimPrefix(apiKey, apigwv2APIKeyPrefix)
}

// apigwv2StampEverTagged records that the API stored at apiKey has carried a tag, when
// [taggingEverTagged] says this write makes that so. tagsBefore is counted before the merge.
func apigwv2StampEverTagged(ctx context.Context, state StateManager, apiKey string, tagsBefore, tagsAdded int) error {
	if !taggingEverTagged(false, tagsBefore, tagsAdded) {
		return nil
	}
	if err := state.Put(ctx, apigatewayv2Namespace, apigwv2APITaggedKey(apiKey), []byte("true")); err != nil {
		return fmt.Errorf("stamp apigatewayv2 api tag history: %w", err)
	}
	return nil
}

// apigwv2EverTagged reports whether the API stored at apiKey has carried a tag.
func apigwv2EverTagged(ctx context.Context, state StateManager, apiKey string) (bool, error) {
	data, err := state.Get(ctx, apigatewayv2Namespace, apigwv2APITaggedKey(apiKey))
	if err != nil {
		return false, fmt.Errorf("read apigatewayv2 api tag history: %w", err)
	}
	return data != nil, nil
}

// apigwv2APIARN returns the ARN of a v2 API. The account segment is empty, as the published format
// is: arn:aws:apigateway:{region}::/apis/{id}.
func apigwv2APIARN(region, apiID string) string {
	return "arn:aws:apigateway:" + region + "::/apis/" + apiID
}

// apigwv2ParseAPIARN parses a v2 API ARN into the account, Region and API ID it names.
//
// The account segment is empty by specification, so an empty one is the caller's, which is
// [TaggingPlugin.resolveARN]'s rule (#1307); an ARN that does carry an account names that account's
// API or none. ok is false for anything that is not an API Gateway ARN naming /apis/{id}.
func apigwv2ParseAPIARN(arn, callerAccount string) (account, region, apiID string, ok bool) {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 6 || parts[0] != "arn" || parts[2] != "apigateway" || parts[3] == "" {
		return "", "", "", false
	}
	id, found := strings.CutPrefix(parts[5], "/apis/")
	if !found || id == "" || strings.Contains(id, "/") {
		return "", "", "", false
	}
	account = parts[4]
	if account == "" {
		account = callerAccount
	}
	return account, parts[3], id, true
}

// apigwv2TagTarget resolves the resource ARN in a /v2/tags/{resource-arn} request to the API it names,
// returning its state key and record, or the refusal the tags page publishes.
func (p *APIGatewayV2Plugin) apigwv2TagTarget(ctx *RequestContext, resourceARN string) (string, *V2ApiState, error) {
	account, region, apiID, ok := apigwv2ParseAPIARN(resourceARN, ctx.AccountID)
	if !ok {
		return "", nil, &AWSError{
			Code:       "BadRequestException",
			Message:    "Invalid resource ARN: " + resourceARN + ". Substrate tags API Gateway v2 APIs (arn:aws:apigateway:{region}::/apis/{apiId}).",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	key := apigwv2APIKey(account, region, apiID)
	data, err := p.state.Get(context.Background(), apigatewayv2Namespace, key)
	if err != nil {
		return "", nil, fmt.Errorf("apigatewayv2 tags state.Get: %w", err)
	}
	if data == nil {
		return "", nil, &AWSError{Code: "NotFoundException", Message: "Invalid API identifier specified " + resourceARN, HTTPStatus: http.StatusNotFound}
	}
	var api V2ApiState
	if err := json.Unmarshal(data, &api); err != nil {
		return "", nil, fmt.Errorf("apigatewayv2 tags unmarshal: %w", err)
	}
	return key, &api, nil
}

// apigwv2PutTags writes an API's tag set back, stamping the tag-history side-car when the write makes
// it carry, or have carried, a tag.
func (p *APIGatewayV2Plugin) apigwv2PutTags(key string, api *V2ApiState, add map[string]string, removeKeys []string) error {
	goCtx := context.Background()
	tagsBefore := len(api.Tags)
	api.Tags = mergeStringMap(api.Tags, add, removeKeys)
	data, err := json.Marshal(api)
	if err != nil {
		return fmt.Errorf("apigatewayv2 tags marshal: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayv2Namespace, key, data); err != nil {
		return fmt.Errorf("apigatewayv2 tags state.Put: %w", err)
	}
	return apigwv2StampEverTagged(goCtx, p.state, key, tagsBefore, len(add))
}

// tagResource handles TagResource: POST /v2/tags/{resource-arn} with {"tags": {…}}, answering 201 and
// no body, as the page publishes.
func (p *APIGatewayV2Plugin) tagResource(ctx *RequestContext, req *AWSRequest, resourceARN string) (*AWSResponse, error) {
	var body struct {
		Tags map[string]string `json:"tags"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return nil, &AWSError{Code: "BadRequestException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
		}
	}
	key, api, err := p.apigwv2TagTarget(ctx, resourceARN)
	if err != nil {
		return nil, err
	}
	if err := p.apigwv2PutTags(key, api, body.Tags, nil); err != nil {
		return nil, err
	}
	return &AWSResponse{StatusCode: http.StatusCreated, Headers: map[string]string{"Content-Type": "application/json"}}, nil
}

// getTags handles GetTags: GET /v2/tags/{resource-arn}, answering {"tags": {…}}. An API with no tags
// answers an empty map rather than omitting the member, as an SDK's Tags field decodes either way and
// the page's model is the map itself.
func (p *APIGatewayV2Plugin) getTags(ctx *RequestContext, resourceARN string) (*AWSResponse, error) {
	_, api, err := p.apigwv2TagTarget(ctx, resourceARN)
	if err != nil {
		return nil, err
	}
	tags := api.Tags
	if tags == nil {
		tags = map[string]string{}
	}
	return apigwJSONResponse(http.StatusOK, struct {
		Tags map[string]string `json:"tags"`
	}{Tags: tags})
}

// untagResource handles UntagResource: DELETE /v2/tags/{resource-arn}?tagKeys=…&tagKeys=…, answering
// 204. tagKeys is Required: True, so its absence is BadRequestException. A repeated key arrives
// through [AWSRequest.MultiValueParams] (#1195); a single one through Params.
func (p *APIGatewayV2Plugin) untagResource(ctx *RequestContext, req *AWSRequest, resourceARN string) (*AWSResponse, error) {
	keys := req.MultiValueParams["tagKeys"]
	if len(keys) == 0 {
		if k, ok := req.Params["tagKeys"]; ok && k != "" {
			keys = []string{k}
		}
	}
	if len(keys) == 0 {
		return nil, &AWSError{Code: "BadRequestException", Message: "tagKeys is required", HTTPStatus: http.StatusBadRequest}
	}
	key, api, err := p.apigwv2TagTarget(ctx, resourceARN)
	if err != nil {
		return nil, err
	}
	if err := p.apigwv2PutTags(key, api, nil, keys); err != nil {
		return nil, err
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{"Content-Type": "application/json"}}, nil
}
