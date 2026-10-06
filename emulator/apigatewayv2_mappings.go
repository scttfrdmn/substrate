package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// GetApiMappings, GetApiMapping, UpdateApiMapping and DeleteApiMapping (#566).
//
// Only CreateApiMapping was routed, so a mapping could be created and never read back, retargeted
// or removed, and a teardown of a custom domain could not find what it had to delete. The mapping
// IDs of a domain are now indexed under [apigwv2APIMappingIDsKey], written by CreateApiMapping and
// pruned by DeleteApiMapping, which is the pattern every other v2 collection uses.
//
// GetApiMappings publishes maxResults and nextToken. Like GetRoutes, GetIntegrations and the other
// v2 lists here, it answers every mapping in one page and never emits nextToken, which is a
// complete, final page under the published contract.

// apigwv2APIMappingIDsKey is the state key for the IDs of a domain's API mappings.
func apigwv2APIMappingIDsKey(accountID, region, domain string) string {
	return "apimapping_ids:" + accountID + "/" + region + "/" + domain
}

// requireDomainName answers NotFoundException for a domain name the account and Region hold no
// record of. Every mapping operation publishes 404 NotFoundException, and the domain is the
// first resource in each one's path.
func (p *APIGatewayV2Plugin) requireDomainName(ctx *RequestContext, domainName string) error {
	data, err := p.state.Get(context.Background(), apigatewayv2Namespace, apigwv2DomainNameKey(ctx.AccountID, ctx.Region, domainName))
	if err != nil {
		return fmt.Errorf("apigatewayv2 requireDomainName state.Get: %w", err)
	}
	if data == nil {
		return &AWSError{Code: "NotFoundException", Message: "Domain name not found: " + domainName, HTTPStatus: http.StatusNotFound}
	}
	return nil
}

// loadAPIMapping reads one mapping of a domain, answering NotFoundException when the domain or
// the mapping is absent.
func (p *APIGatewayV2Plugin) loadAPIMapping(ctx *RequestContext, domainName, mappingID string) (v2APIMappingState, error) {
	if err := p.requireDomainName(ctx, domainName); err != nil {
		return v2APIMappingState{}, err
	}
	data, err := p.state.Get(context.Background(), apigatewayv2Namespace, apigwv2APIMappingKey(ctx.AccountID, ctx.Region, domainName, mappingID))
	if err != nil {
		return v2APIMappingState{}, fmt.Errorf("apigatewayv2 loadAPIMapping state.Get: %w", err)
	}
	if data == nil {
		return v2APIMappingState{}, &AWSError{Code: "NotFoundException", Message: "ApiMapping not found: " + mappingID, HTTPStatus: http.StatusNotFound}
	}
	var mapping v2APIMappingState
	if err := json.Unmarshal(data, &mapping); err != nil {
		return v2APIMappingState{}, fmt.Errorf("apigatewayv2 loadAPIMapping unmarshal: %w", err)
	}
	return mapping, nil
}

// getAPIMappings handles GetApiMappings: GET /v2/domainnames/{domainName}/apimappings.
func (p *APIGatewayV2Plugin) getAPIMappings(ctx *RequestContext, domainName string) (*AWSResponse, error) {
	if err := p.requireDomainName(ctx, domainName); err != nil {
		return nil, err
	}
	goCtx := context.Background()
	ids, err := loadStringIndex(goCtx, p.state, apigatewayv2Namespace, apigwv2APIMappingIDsKey(ctx.AccountID, ctx.Region, domainName))
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 getAPIMappings loadIndex: %w", err)
	}
	items := make([]v2APIMappingOut, 0, len(ids))
	for _, id := range ids {
		data, err := p.state.Get(goCtx, apigatewayv2Namespace, apigwv2APIMappingKey(ctx.AccountID, ctx.Region, domainName, id))
		if err != nil {
			return nil, fmt.Errorf("apigatewayv2 getAPIMappings state.Get: %w", err)
		}
		if data == nil {
			continue
		}
		var mapping v2APIMappingState
		if err := json.Unmarshal(data, &mapping); err != nil {
			return nil, fmt.Errorf("apigatewayv2 getAPIMappings unmarshal: %w", err)
		}
		items = append(items, v2APIMappingWire(mapping))
	}
	return apigwJSONResponse(http.StatusOK, apigwV2ItemsOut[v2APIMappingOut]{Items: items})
}

// getAPIMapping handles GetApiMapping: GET /v2/domainnames/{domainName}/apimappings/{apiMappingId}.
func (p *APIGatewayV2Plugin) getAPIMapping(ctx *RequestContext, domainName, mappingID string) (*AWSResponse, error) {
	mapping, err := p.loadAPIMapping(ctx, domainName, mappingID)
	if err != nil {
		return nil, err
	}
	return apigwJSONResponse(http.StatusOK, v2APIMappingWire(mapping))
}

// updateAPIMapping handles UpdateApiMapping: PATCH /v2/domainnames/{domainName}/apimappings/{apiMappingId}.
//
// UpdateApiMappingInput's three members are each Required: No, so an absent member leaves its
// value unchanged and a present one replaces it, including an empty apiMappingKey, which is how a
// caller moves a mapping back to the domain's root.
func (p *APIGatewayV2Plugin) updateAPIMapping(ctx *RequestContext, req *AWSRequest, domainName, mappingID string) (*AWSResponse, error) {
	mapping, err := p.loadAPIMapping(ctx, domainName, mappingID)
	if err != nil {
		return nil, err
	}
	var patch struct {
		APIID         *string `json:"apiId"`
		Stage         *string `json:"stage"`
		APIMappingKey *string `json:"apiMappingKey"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &patch); err != nil {
			return nil, apigwv2InvalidBody()
		}
	}
	if patch.APIID != nil {
		mapping.APIID = *patch.APIID
	}
	if patch.Stage != nil {
		mapping.Stage = *patch.Stage
	}
	if patch.APIMappingKey != nil {
		mapping.APIMappingKey = *patch.APIMappingKey
	}
	data, err := json.Marshal(mapping)
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 updateAPIMapping marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), apigatewayv2Namespace, apigwv2APIMappingKey(ctx.AccountID, ctx.Region, domainName, mappingID), data); err != nil {
		return nil, fmt.Errorf("apigatewayv2 updateAPIMapping state.Put: %w", err)
	}
	return apigwJSONResponse(http.StatusOK, v2APIMappingWire(mapping))
}

// deleteAPIMapping handles DeleteApiMapping: DELETE /v2/domainnames/{domainName}/apimappings/{apiMappingId},
// answering the published 204 with no body.
func (p *APIGatewayV2Plugin) deleteAPIMapping(ctx *RequestContext, domainName, mappingID string) (*AWSResponse, error) {
	if _, err := p.loadAPIMapping(ctx, domainName, mappingID); err != nil {
		return nil, err
	}
	goCtx := context.Background()
	if err := p.state.Delete(goCtx, apigatewayv2Namespace, apigwv2APIMappingKey(ctx.AccountID, ctx.Region, domainName, mappingID)); err != nil {
		return nil, fmt.Errorf("apigatewayv2 deleteAPIMapping state.Delete: %w", err)
	}
	if err := removeFromStringIndex(goCtx, p.state, apigatewayv2Namespace, apigwv2APIMappingIDsKey(ctx.AccountID, ctx.Region, domainName), mappingID); err != nil {
		return nil, fmt.Errorf("apigatewayv2 deleteAPIMapping index: %w", err)
	}
	return apigwJSONResponse(http.StatusNoContent, struct{}{})
}
