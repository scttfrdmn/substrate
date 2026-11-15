package emulator_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// CreateApiMapping checks its required members, the API and stage it names, and duplicate keys, and
// every routed v2 collection pages by maxResults/nextToken (#1423).

// agwv2CreateMappingErr posts one CreateApiMapping and returns the AWS error it was refused with.
func agwv2CreateMappingErr(t *testing.T, p *emulator.APIGatewayV2Plugin, ctx *emulator.RequestContext,
	domain string, body map[string]any) *emulator.AWSError {
	t.Helper()
	_, err := p.HandleRequest(ctx, apigwv2Request(t, http.MethodPost, "/v2/domainnames/"+domain+"/apimappings", body))
	if err == nil {
		return nil
	}
	var awsErr *emulator.AWSError
	require.Truef(t, errors.As(err, &awsErr), "CreateApiMapping failed with a non-AWS error: %v", err)
	return awsErr
}

func TestAPIGatewayV2CreateMapping_Refusals(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		body       func(apiID string) map[string]any
		wantCode   string
		wantStatus int
	}{
		{"no apiId", func(string) map[string]any { return map[string]any{"stage": "$default"} },
			"BadRequestException", http.StatusBadRequest},
		{"no stage", func(apiID string) map[string]any { return map[string]any{"apiId": apiID} },
			"BadRequestException", http.StatusBadRequest},
		{"an absent API", func(string) map[string]any { return map[string]any{"apiId": "nosuchapi", "stage": "$default"} },
			"NotFoundException", http.StatusNotFound},
		{"an absent stage", func(apiID string) map[string]any { return map[string]any{"apiId": apiID, "stage": "prod"} },
			"NotFoundException", http.StatusNotFound},
		{"a key the domain already maps", func(apiID string) map[string]any {
			return map[string]any{"apiId": apiID, "stage": "$default", "apiMappingKey": "v1"}
		}, "ConflictException", http.StatusConflict},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, ctx := setupAPIGatewayV2Plugin(t)
			apiID := agwv2API(t, p, ctx, "create-mapping")
			agwv2DefaultStage(t, p, ctx, apiID)
			agwv2Wire(t, p, ctx, "POST", "/v2/domainnames", map[string]any{"domainName": "c.example.com"})
			agwv2Mapping(t, p, ctx, "c.example.com", apiID, "v1")

			awsErr := agwv2CreateMappingErr(t, p, ctx, "c.example.com", tc.body(apiID))
			require.NotNil(t, awsErr, "the create must be refused")
			assert.Equal(t, tc.wantCode, awsErr.Code, awsErr.Message)
			assert.Equal(t, tc.wantStatus, awsErr.HTTPStatus)

			list, _ := agwv2Wire(t, p, ctx, "GET", "/v2/domainnames/c.example.com/apimappings", nil)
			assert.Len(t, agwv2Items(t, "GetApiMappings", list), 1, "a refused create stores no mapping")
		})
	}
}

// UpdateApiMapping holds the mapping it leaves to CreateApiMapping's checks.
func TestAPIGatewayV2UpdateMapping_Refusals(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		patch      func(apiID string) map[string]any
		wantCode   string
		wantStatus int
	}{
		{"an empty apiId", func(string) map[string]any { return map[string]any{"apiId": ""} },
			"BadRequestException", http.StatusBadRequest},
		{"an empty stage", func(string) map[string]any { return map[string]any{"stage": ""} },
			"BadRequestException", http.StatusBadRequest},
		{"an absent API", func(string) map[string]any { return map[string]any{"apiId": "nosuchapi"} },
			"NotFoundException", http.StatusNotFound},
		{"an absent stage", func(string) map[string]any { return map[string]any{"stage": "prod"} },
			"NotFoundException", http.StatusNotFound},
		{"another mapping's key", func(string) map[string]any { return map[string]any{"apiMappingKey": "v2"} },
			"ConflictException", http.StatusConflict},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, ctx := setupAPIGatewayV2Plugin(t)
			apiID := agwv2API(t, p, ctx, "update-mapping")
			agwv2DefaultStage(t, p, ctx, apiID)
			agwv2Wire(t, p, ctx, "POST", "/v2/domainnames", map[string]any{"domainName": "u.example.com"})
			id := agwv2Mapping(t, p, ctx, "u.example.com", apiID, "v1")
			agwv2Mapping(t, p, ctx, "u.example.com", apiID, "v2")

			_, err := p.HandleRequest(ctx, apigwv2Request(t, http.MethodPatch,
				"/v2/domainnames/u.example.com/apimappings/"+id, tc.patch(apiID)))
			var awsErr *emulator.AWSError
			require.Truef(t, errors.As(err, &awsErr), "the update must be refused, got %v", err)
			assert.Equal(t, tc.wantCode, awsErr.Code, awsErr.Message)
			assert.Equal(t, tc.wantStatus, awsErr.HTTPStatus)

			got, _ := agwv2Wire(t, p, ctx, "GET", "/v2/domainnames/u.example.com/apimappings/"+id, nil)
			assert.Equal(t, "v1", got["apiMappingKey"], "a refused update changes nothing")
			assert.Equal(t, apiID, got["apiId"])
		})
	}

	// The mapping's own key is not a conflict with itself.
	p, ctx := setupAPIGatewayV2Plugin(t)
	apiID := agwv2API(t, p, ctx, "update-self")
	agwv2DefaultStage(t, p, ctx, apiID)
	agwv2Wire(t, p, ctx, "POST", "/v2/domainnames", map[string]any{"domainName": "s.example.com"})
	id := agwv2Mapping(t, p, ctx, "s.example.com", apiID, "v1")
	agwv2Wire(t, p, ctx, http.MethodPatch, "/v2/domainnames/s.example.com/apimappings/"+id,
		map[string]any{"apiMappingKey": "v1"})
}

func TestAPIGatewayV2CreateMapping_KeysConflictPerDomain(t *testing.T) {
	t.Parallel()
	p, ctx := setupAPIGatewayV2Plugin(t)
	apiID := agwv2API(t, p, ctx, "keys")
	agwv2DefaultStage(t, p, ctx, apiID)
	for _, d := range []string{"a.example.com", "b.example.com"} {
		agwv2Wire(t, p, ctx, "POST", "/v2/domainnames", map[string]any{"domainName": d})
	}

	// The omitted key is the domain's root, and a second root mapping conflicts.
	agwv2Mapping(t, p, ctx, "a.example.com", apiID, "")
	awsErr := agwv2CreateMappingErr(t, p, ctx, "a.example.com", map[string]any{"apiId": apiID, "stage": "$default"})
	require.NotNil(t, awsErr)
	assert.Equal(t, "ConflictException", awsErr.Code)

	// The same key on another domain is a different route, and a different key on one domain is free.
	agwv2Mapping(t, p, ctx, "b.example.com", apiID, "")
	agwv2Mapping(t, p, ctx, "a.example.com", apiID, "v2")

	// A deleted mapping frees its key.
	id := agwv2Mapping(t, p, ctx, "b.example.com", apiID, "v3")
	_, err := p.HandleRequest(ctx, apigwv2Request(t, http.MethodDelete, "/v2/domainnames/b.example.com/apimappings/"+id, nil))
	require.NoError(t, err)
	agwv2Mapping(t, p, ctx, "b.example.com", apiID, "v3")
}

// agwv2ListPage issues one list request with the given query parameters and returns the item count
// and the nextToken (empty when absent).
func agwv2ListPage(t *testing.T, p *emulator.APIGatewayV2Plugin, ctx *emulator.RequestContext,
	path string, params map[string]string) (int, string) {
	t.Helper()
	req := apigwv2Request(t, http.MethodGet, path, nil)
	for k, v := range params {
		req.Params[k] = v
	}
	resp, err := p.HandleRequest(ctx, req)
	require.NoErrorf(t, err, "GET %s %v", path, params)
	var doc struct {
		Items     []json.RawMessage `json:"items"`
		NextToken *string           `json:"nextToken"`
	}
	require.NoError(t, json.Unmarshal(resp.Body, &doc), "decode %s: %s", path, resp.Body)
	require.NotNil(t, doc.Items, "items is always present: %s", resp.Body)
	if doc.NextToken == nil {
		return len(doc.Items), ""
	}
	require.NotEmpty(t, *doc.NextToken, "nextToken is omitted rather than sent empty: %s", resp.Body)
	return len(doc.Items), *doc.NextToken
}

// agwv2Collections creates five elements in each of the six routed v2 collections and returns the
// path that lists each.
func agwv2Collections(t *testing.T, p *emulator.APIGatewayV2Plugin, ctx *emulator.RequestContext) map[string]string {
	t.Helper()
	apiID := agwv2API(t, p, ctx, "paged")
	for i := range 4 {
		agwv2API(t, p, ctx, fmt.Sprintf("paged-%d", i))
	}
	agwv2DefaultStage(t, p, ctx, apiID)
	agwv2Wire(t, p, ctx, "POST", "/v2/domainnames", map[string]any{"domainName": "p.example.com"})
	for i := range 5 {
		agwv2Wire(t, p, ctx, "POST", "/v2/apis/"+apiID+"/routes", map[string]any{"routeKey": fmt.Sprintf("GET /r%d", i)})
		agwv2Wire(t, p, ctx, "POST", "/v2/apis/"+apiID+"/integrations", map[string]any{
			"integrationType": "HTTP_PROXY", "integrationUri": fmt.Sprintf("https://example.com/%d", i),
		})
		agwv2Wire(t, p, ctx, "POST", "/v2/apis/"+apiID+"/authorizers", map[string]any{
			"name": fmt.Sprintf("auth-%d", i), "authorizerType": "JWT",
			"identitySource": []string{"$request.header.Authorization"},
		})
		agwv2Mapping(t, p, ctx, "p.example.com", apiID, fmt.Sprintf("k%d", i))
		if i < 4 {
			agwv2Wire(t, p, ctx, "POST", "/v2/apis/"+apiID+"/stages", map[string]any{"stageName": fmt.Sprintf("s%d", i)})
		}
	}
	return map[string]string{
		"GetApis":         "/v2/apis",
		"GetRoutes":       "/v2/apis/" + apiID + "/routes",
		"GetIntegrations": "/v2/apis/" + apiID + "/integrations",
		"GetStages":       "/v2/apis/" + apiID + "/stages",
		"GetAuthorizers":  "/v2/apis/" + apiID + "/authorizers",
		"GetApiMappings":  "/v2/domainnames/p.example.com/apimappings",
	}
}

func TestAPIGatewayV2Paging_EveryCollectionPagesByMaxResults(t *testing.T) {
	t.Parallel()
	p, ctx := setupAPIGatewayV2Plugin(t)
	for op, path := range agwv2Collections(t, p, ctx) {
		t.Run(op, func(t *testing.T) {
			// No maxResults is the whole collection in one page, with no nextToken.
			n, next := agwv2ListPage(t, p, ctx, path, nil)
			assert.Equal(t, 5, n)
			assert.Empty(t, next)

			// maxResults 2 over five elements is pages of 2, 2 and 1, and the last carries no token.
			var sizes []int
			token := ""
			for {
				params := map[string]string{"maxResults": "2"}
				if token != "" {
					params["nextToken"] = token
				}
				n, token = agwv2ListPage(t, p, ctx, path, params)
				sizes = append(sizes, n)
				if token == "" {
					break
				}
				require.Less(t, len(sizes), 5, "the walk must end")
			}
			assert.Equal(t, []int{2, 2, 1}, sizes)

			// An exact multiple of the page size ends on a full page with no token.
			n, next = agwv2ListPage(t, p, ctx, path, map[string]string{"maxResults": "5"})
			assert.Equal(t, 5, n)
			assert.Empty(t, next)
		})
	}
}

func TestAPIGatewayV2Paging_AnUnusableParameterIsBadRequest(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		params map[string]string
	}{
		{"a maxResults of zero", map[string]string{"maxResults": "0"}},
		{"a negative maxResults", map[string]string{"maxResults": "-1"}},
		{"a maxResults that is not a number", map[string]string{"maxResults": "ten"}},
		{"a nextToken substrate never issued", map[string]string{"nextToken": "not-a-token"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, ctx := setupAPIGatewayV2Plugin(t)
			// The refusal comes before any state is read, so an absent domain still answers it.
			req := apigwv2Request(t, http.MethodGet, "/v2/domainnames/absent.example.com/apimappings", nil)
			for k, v := range tc.params {
				req.Params[k] = v
			}
			_, err := p.HandleRequest(ctx, req)
			var awsErr *emulator.AWSError
			require.Truef(t, errors.As(err, &awsErr), "want an AWSError, got %v", err)
			assert.Equal(t, "BadRequestException", awsErr.Code)
			assert.Equal(t, http.StatusBadRequest, awsErr.HTTPStatus)
		})
	}
}
