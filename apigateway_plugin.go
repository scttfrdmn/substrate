package substrate

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// APIGatewayPlugin emulates the AWS API Gateway v1 (REST API) service.
// It uses path-based routing since there is no X-Amz-Target header.
type APIGatewayPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "apigateway".
func (p *APIGatewayPlugin) Name() string { return "apigateway" }

// Initialize sets up the APIGatewayPlugin with the provided configuration.
func (p *APIGatewayPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for APIGatewayPlugin.
func (p *APIGatewayPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an API Gateway v1 request to the appropriate handler
// using path-based operation routing.
func (p *APIGatewayPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op, params := parseAPIGatewayOperation(req.Operation, req.Path)
	switch op {
	case "CreateRestApi":
		return p.createRestAPI(ctx, req)
	case "GetRestApi":
		return p.getRestAPI(ctx, params["apiId"])
	case "GetRestApis":
		return p.getRestAPIs(ctx)
	case "DeleteRestApi":
		return p.deleteRestAPI(ctx, params["apiId"])
	case "UpdateRestApi":
		return p.updateRestAPI(ctx, req, params["apiId"])
	case "CreateResource":
		return p.createResource(ctx, req, params["apiId"], params["parentId"])
	case "GetResource":
		return p.getResource(ctx, params["apiId"], params["resId"])
	case "GetResources":
		return p.getResources(ctx, params["apiId"])
	case "DeleteResource":
		return p.deleteResource(ctx, params["apiId"], params["resId"])
	case "PutMethod":
		return p.putMethod(ctx, req, params["apiId"], params["resId"], params["verb"])
	case "GetMethod":
		return p.getMethod(ctx, params["apiId"], params["resId"], params["verb"])
	case "DeleteMethod":
		return p.deleteMethod(ctx, params["apiId"], params["resId"], params["verb"])
	case "PutIntegration":
		return p.putIntegration(ctx, req, params["apiId"], params["resId"], params["verb"])
	case "PutIntegrationResponse":
		return p.putIntegrationResponse(ctx, req, params["apiId"], params["resId"], params["verb"], params["statusCode"])
	case "PutMethodResponse":
		return p.putMethodResponse(ctx, req, params["apiId"], params["resId"], params["verb"], params["statusCode"])
	case "CreateDeployment":
		return p.createDeployment(ctx, req, params["apiId"])
	case "GetDeployment":
		return p.getDeployment(ctx, params["apiId"], params["deployId"])
	case "GetDeployments":
		return p.getDeployments(ctx, params["apiId"])
	case "DeleteDeployment":
		return p.deleteDeployment(ctx, params["apiId"], params["deployId"])
	case "CreateStage":
		return p.createStage(ctx, req, params["apiId"])
	case "GetStage":
		return p.getStage(ctx, params["apiId"], params["stageName"])
	case "GetStages":
		return p.getStages(ctx, params["apiId"])
	case "DeleteStage":
		return p.deleteStage(ctx, params["apiId"], params["stageName"])
	case "UpdateStage":
		return p.updateStage(ctx, req, params["apiId"], params["stageName"])
	case "CreateAuthorizer":
		return p.createAuthorizer(ctx, req, params["apiId"])
	case "GetAuthorizer":
		return p.getAuthorizer(ctx, params["apiId"], params["authId"])
	case "GetAuthorizers":
		return p.getAuthorizers(ctx, params["apiId"])
	case "DeleteAuthorizer":
		return p.deleteAuthorizer(ctx, params["apiId"], params["authId"])
	case "CreateApiKey":
		return p.createAPIKey(ctx, req)
	case "GetApiKey":
		return p.getAPIKey(ctx, params["keyId"])
	case "GetApiKeys":
		return p.getAPIKeys(ctx)
	case "DeleteApiKey":
		return p.deleteAPIKey(ctx, params["keyId"])
	case "CreateUsagePlan":
		return p.createUsagePlan(ctx, req)
	case "GetUsagePlan":
		return p.getUsagePlan(ctx, params["planId"])
	case "GetUsagePlans":
		return p.getUsagePlans(ctx)
	case "DeleteUsagePlan":
		return p.deleteUsagePlan(ctx, params["planId"])
	case "CreateUsagePlanKey":
		return apigwJSONResponse(http.StatusCreated, map[string]string{"id": generateAPIGatewayID(), "type": "API_KEY"})
	case "CreateDomainName":
		return p.createDomainName(ctx, req)
	case "GetDomainName":
		return p.getDomainName(ctx, params["name"])
	case "CreateBasePathMapping":
		return p.createBasePathMapping(ctx, req, params["name"])
	case "GetBasePathMappings":
		return p.getBasePathMappings(ctx, params["name"])
	default:
		return nil, &AWSError{
			Code:       "NotFoundException",
			Message:    fmt.Sprintf("APIGatewayPlugin: unknown operation for %s %s", req.Operation, req.Path),
			HTTPStatus: http.StatusNotFound,
		}
	}
}

// parseAPIGatewayOperation maps an HTTP method and path to an operation name
// and a map of path parameters extracted from the URL segments.
func parseAPIGatewayOperation(method, path string) (string, map[string]string) {
	params := make(map[string]string)
	parts := splitPath(path)

	if len(parts) == 0 {
		return "", params
	}

	switch {
	// /restapis
	case len(parts) == 1 && parts[0] == "restapis":
		switch method {
		case "POST":
			return "CreateRestApi", params
		case "GET":
			return "GetRestApis", params
		}

	// /restapis/{id}
	case len(parts) == 2 && parts[0] == "restapis":
		params["apiId"] = parts[1]
		switch method {
		case "GET":
			return "GetRestApi", params
		case "DELETE":
			return "DeleteRestApi", params
		case "PATCH":
			return "UpdateRestApi", params
		}

	// /restapis/{id}/resources
	case len(parts) == 3 && parts[0] == "restapis" && parts[2] == "resources":
		params["apiId"] = parts[1]
		switch method {
		case "GET":
			return "GetResources", params
		}

	// /restapis/{id}/resources/{resId} — also serves as POST /restapis/{id}/resources/{parentId}
	case len(parts) == 4 && parts[0] == "restapis" && parts[2] == "resources":
		params["apiId"] = parts[1]
		switch method {
		case "GET":
			params["resId"] = parts[3]
			return "GetResource", params
		case "DELETE":
			params["resId"] = parts[3]
			return "DeleteResource", params
		case "POST":
			params["parentId"] = parts[3]
			return "CreateResource", params
		}

	// /restapis/{id}/resources/{resId}/methods/{verb}
	case len(parts) == 6 && parts[0] == "restapis" && parts[2] == "resources" && parts[4] == "methods":
		params["apiId"] = parts[1]
		params["resId"] = parts[3]
		params["verb"] = parts[5]
		switch method {
		case "PUT":
			return "PutMethod", params
		case "GET":
			return "GetMethod", params
		case "DELETE":
			return "DeleteMethod", params
		}

	// /restapis/{id}/resources/{resId}/methods/{verb}/integration
	case len(parts) == 7 && parts[0] == "restapis" && parts[2] == "resources" && parts[4] == "methods" && parts[6] == "integration":
		params["apiId"] = parts[1]
		params["resId"] = parts[3]
		params["verb"] = parts[5]
		if method == "PUT" {
			return "PutIntegration", params
		}

	// /restapis/{id}/resources/{resId}/methods/{verb}/integration/responses/{statusCode}
	case len(parts) == 9 && parts[0] == "restapis" && parts[2] == "resources" && parts[4] == "methods" && parts[6] == "integration" && parts[7] == "responses":
		params["apiId"] = parts[1]
		params["resId"] = parts[3]
		params["verb"] = parts[5]
		params["statusCode"] = parts[8]
		if method == "PUT" {
			return "PutIntegrationResponse", params
		}

	// /restapis/{id}/resources/{resId}/methods/{verb}/responses/{statusCode}
	case len(parts) == 8 && parts[0] == "restapis" && parts[2] == "resources" && parts[4] == "methods" && parts[6] == "responses":
		params["apiId"] = parts[1]
		params["resId"] = parts[3]
		params["verb"] = parts[5]
		params["statusCode"] = parts[7]
		if method == "PUT" {
			return "PutMethodResponse", params
		}

	// /restapis/{id}/deployments
	case len(parts) == 3 && parts[0] == "restapis" && parts[2] == "deployments":
		params["apiId"] = parts[1]
		switch method {
		case "POST":
			return "CreateDeployment", params
		case "GET":
			return "GetDeployments", params
		}

	// /restapis/{id}/deployments/{depId}
	case len(parts) == 4 && parts[0] == "restapis" && parts[2] == "deployments":
		params["apiId"] = parts[1]
		params["deployId"] = parts[3]
		switch method {
		case "GET":
			return "GetDeployment", params
		case "DELETE":
			return "DeleteDeployment", params
		}

	// /restapis/{id}/stages
	case len(parts) == 3 && parts[0] == "restapis" && parts[2] == "stages":
		params["apiId"] = parts[1]
		switch method {
		case "POST":
			return "CreateStage", params
		case "GET":
			return "GetStages", params
		}

	// /restapis/{id}/stages/{stageName}
	case len(parts) == 4 && parts[0] == "restapis" && parts[2] == "stages":
		params["apiId"] = parts[1]
		params["stageName"] = parts[3]
		switch method {
		case "GET":
			return "GetStage", params
		case "DELETE":
			return "DeleteStage", params
		case "PATCH":
			return "UpdateStage", params
		}

	// /restapis/{id}/authorizers
	case len(parts) == 3 && parts[0] == "restapis" && parts[2] == "authorizers":
		params["apiId"] = parts[1]
		switch method {
		case "POST":
			return "CreateAuthorizer", params
		case "GET":
			return "GetAuthorizers", params
		}

	// /restapis/{id}/authorizers/{authId}
	case len(parts) == 4 && parts[0] == "restapis" && parts[2] == "authorizers":
		params["apiId"] = parts[1]
		params["authId"] = parts[3]
		switch method {
		case "GET":
			return "GetAuthorizer", params
		case "DELETE":
			return "DeleteAuthorizer", params
		}

	// /apikeys
	case len(parts) == 1 && parts[0] == "apikeys":
		switch method {
		case "POST":
			return "CreateApiKey", params
		case "GET":
			return "GetApiKeys", params
		}

	// /apikeys/{keyId}
	case len(parts) == 2 && parts[0] == "apikeys":
		params["keyId"] = parts[1]
		switch method {
		case "GET":
			return "GetApiKey", params
		case "DELETE":
			return "DeleteApiKey", params
		}

	// /usageplans
	case len(parts) == 1 && parts[0] == "usageplans":
		switch method {
		case "POST":
			return "CreateUsagePlan", params
		case "GET":
			return "GetUsagePlans", params
		}

	// /usageplans/{planId}
	case len(parts) == 2 && parts[0] == "usageplans":
		params["planId"] = parts[1]
		switch method {
		case "GET":
			return "GetUsagePlan", params
		case "DELETE":
			return "DeleteUsagePlan", params
		}

	// /usageplans/{planId}/keys
	case len(parts) == 3 && parts[0] == "usageplans" && parts[2] == "keys":
		params["planId"] = parts[1]
		if method == "POST" {
			return "CreateUsagePlanKey", params
		}

	// /domainnames
	case len(parts) == 1 && parts[0] == "domainnames":
		if method == "POST" {
			return "CreateDomainName", params
		}

	// /domainnames/{name}
	case len(parts) == 2 && parts[0] == "domainnames":
		params["name"] = parts[1]
		if method == "GET" {
			return "GetDomainName", params
		}

	// /domainnames/{name}/basepathmappings
	case len(parts) == 3 && parts[0] == "domainnames" && parts[2] == "basepathmappings":
		params["name"] = parts[1]
		switch method {
		case "POST":
			return "CreateBasePathMapping", params
		case "GET":
			return "GetBasePathMappings", params
		}
	}

	return "", params
}

// splitPath splits a URL path into its non-empty segments.
func splitPath(path string) []string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

// --- REST API operations -----------------------------------------------------

func (p *APIGatewayPlugin) createRestAPI(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Name        string            `json:"name"`
		Description string            `json:"description"`
		Tags        map[string]string `json:"tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "BadRequestException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.Name == "" {
		return nil, &AWSError{Code: "BadRequestException", Message: "name is required", HTTPStatus: http.StatusBadRequest}
	}

	apiID := generateAPIGatewayID()
	rootResID := generateAPIGatewayID()
	now := p.tc.Now()

	api := RestAPIState{
		ID:             apiID,
		Name:           body.Name,
		Description:    body.Description,
		RootResourceID: rootResID,
		Tags:           body.Tags,
		CreatedDate:    now,
		AccountID:      ctx.AccountID,
		Region:         ctx.Region,
	}

	rootRes := ResourceState{
		ID:        rootResID,
		PathPart:  "",
		Path:      "/",
		APIId:     apiID,
		AccountID: ctx.AccountID,
		Region:    ctx.Region,
	}

	goCtx := context.Background()
	apiData, err := json.Marshal(api)
	if err != nil {
		return nil, fmt.Errorf("apigateway createRestApi marshal api: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayNamespace, apigwAPIKey(ctx.AccountID, ctx.Region, apiID), apiData); err != nil {
		return nil, fmt.Errorf("apigateway createRestApi state.Put api: %w", err)
	}
	updateStringIndex(goCtx, p.state, apigatewayNamespace, apigwAPIIDsKey(ctx.AccountID, ctx.Region), apiID)

	resData, err := json.Marshal(rootRes)
	if err != nil {
		return nil, fmt.Errorf("apigateway createRestApi marshal root resource: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayNamespace, apigwResourceKey(ctx.AccountID, ctx.Region, apiID, rootResID), resData); err != nil {
		return nil, fmt.Errorf("apigateway createRestApi state.Put root resource: %w", err)
	}
	updateStringIndex(goCtx, p.state, apigatewayNamespace, apigwResourceIDsKey(ctx.AccountID, ctx.Region, apiID), rootResID)

	return apigwJSONResponse(http.StatusCreated, api)
}

func (p *APIGatewayPlugin) getRestAPI(ctx *RequestContext, apiID string) (*AWSResponse, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, apigatewayNamespace, apigwAPIKey(ctx.AccountID, ctx.Region, apiID))
	if err != nil {
		return nil, fmt.Errorf("apigateway getRestAPI state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "REST API not found: " + apiID, HTTPStatus: http.StatusNotFound}
	}
	var api RestAPIState
	if err := json.Unmarshal(data, &api); err != nil {
		return nil, fmt.Errorf("apigateway getRestAPI unmarshal: %w", err)
	}
	return apigwJSONResponse(http.StatusOK, api)
}

func (p *APIGatewayPlugin) getRestAPIs(ctx *RequestContext) (*AWSResponse, error) {
	goCtx := context.Background()
	ids, err := loadStringIndex(goCtx, p.state, apigatewayNamespace, apigwAPIIDsKey(ctx.AccountID, ctx.Region))
	if err != nil {
		return nil, fmt.Errorf("apigateway getRestApis loadIndex: %w", err)
	}

	items := make([]RestAPIState, 0, len(ids))
	for _, id := range ids {
		data, getErr := p.state.Get(goCtx, apigatewayNamespace, apigwAPIKey(ctx.AccountID, ctx.Region, id))
		if getErr != nil || data == nil {
			continue
		}
		var api RestAPIState
		if json.Unmarshal(data, &api) == nil {
			items = append(items, api)
		}
	}

	type response struct {
		Items []RestAPIState `json:"items"`
	}
	return apigwJSONResponse(http.StatusOK, response{Items: items})
}

func (p *APIGatewayPlugin) deleteRestAPI(ctx *RequestContext, apiID string) (*AWSResponse, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, apigatewayNamespace, apigwAPIKey(ctx.AccountID, ctx.Region, apiID))
	if err != nil {
		return nil, fmt.Errorf("apigateway deleteRestApi state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "REST API not found: " + apiID, HTTPStatus: http.StatusNotFound}
	}
	if err := p.state.Delete(goCtx, apigatewayNamespace, apigwAPIKey(ctx.AccountID, ctx.Region, apiID)); err != nil {
		return nil, fmt.Errorf("apigateway deleteRestApi state.Delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, apigatewayNamespace, apigwAPIIDsKey(ctx.AccountID, ctx.Region), apiID)
	return apigwJSONResponse(http.StatusAccepted, struct{}{})
}

func (p *APIGatewayPlugin) updateRestAPI(ctx *RequestContext, _ *AWSRequest, apiID string) (*AWSResponse, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, apigatewayNamespace, apigwAPIKey(ctx.AccountID, ctx.Region, apiID))
	if err != nil {
		return nil, fmt.Errorf("apigateway updateRestApi state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "REST API not found: " + apiID, HTTPStatus: http.StatusNotFound}
	}
	var api RestAPIState
	if err := json.Unmarshal(data, &api); err != nil {
		return nil, fmt.Errorf("apigateway updateRestApi unmarshal: %w", err)
	}
	// Apply patch operations (simplified: just re-store as-is).
	updated, err := json.Marshal(api)
	if err != nil {
		return nil, fmt.Errorf("apigateway updateRestApi marshal: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayNamespace, apigwAPIKey(ctx.AccountID, ctx.Region, apiID), updated); err != nil {
		return nil, fmt.Errorf("apigateway updateRestApi state.Put: %w", err)
	}
	return apigwJSONResponse(http.StatusOK, api)
}

// --- Resource operations -----------------------------------------------------

func (p *APIGatewayPlugin) createResource(ctx *RequestContext, req *AWSRequest, apiID, parentID string) (*AWSResponse, error) {
	var body struct {
		PathPart string `json:"pathPart"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "BadRequestException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()

	// Load parent resource to build full path.
	parentPath := "/"
	if parentID != "" {
		parentData, getErr := p.state.Get(goCtx, apigatewayNamespace, apigwResourceKey(ctx.AccountID, ctx.Region, apiID, parentID))
		if getErr == nil && parentData != nil {
			var parent ResourceState
			if json.Unmarshal(parentData, &parent) == nil {
				parentPath = parent.Path
			}
		}
	}

	fullPath := parentPath
	if !strings.HasSuffix(fullPath, "/") {
		fullPath += "/"
	}
	fullPath += body.PathPart

	resID := generateAPIGatewayID()
	res := ResourceState{
		ID:        resID,
		ParentID:  parentID,
		PathPart:  body.PathPart,
		Path:      fullPath,
		APIId:     apiID,
		AccountID: ctx.AccountID,
		Region:    ctx.Region,
	}

	resData, err := json.Marshal(res)
	if err != nil {
		return nil, fmt.Errorf("apigateway createResource marshal: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayNamespace, apigwResourceKey(ctx.AccountID, ctx.Region, apiID, resID), resData); err != nil {
		return nil, fmt.Errorf("apigateway createResource state.Put: %w", err)
	}
	updateStringIndex(goCtx, p.state, apigatewayNamespace, apigwResourceIDsKey(ctx.AccountID, ctx.Region, apiID), resID)

	return apigwJSONResponse(http.StatusCreated, res)
}

func (p *APIGatewayPlugin) getResource(ctx *RequestContext, apiID, resID string) (*AWSResponse, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, apigatewayNamespace, apigwResourceKey(ctx.AccountID, ctx.Region, apiID, resID))
	if err != nil {
		return nil, fmt.Errorf("apigateway getResource state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Resource not found: " + resID, HTTPStatus: http.StatusNotFound}
	}
	var res ResourceState
	if err := json.Unmarshal(data, &res); err != nil {
		return nil, fmt.Errorf("apigateway getResource unmarshal: %w", err)
	}
	return apigwJSONResponse(http.StatusOK, res)
}

func (p *APIGatewayPlugin) getResources(ctx *RequestContext, apiID string) (*AWSResponse, error) {
	goCtx := context.Background()
	ids, err := loadStringIndex(goCtx, p.state, apigatewayNamespace, apigwResourceIDsKey(ctx.AccountID, ctx.Region, apiID))
	if err != nil {
		return nil, fmt.Errorf("apigateway getResources loadIndex: %w", err)
	}

	items := make([]ResourceState, 0, len(ids))
	for _, id := range ids {
		data, getErr := p.state.Get(goCtx, apigatewayNamespace, apigwResourceKey(ctx.AccountID, ctx.Region, apiID, id))
		if getErr != nil || data == nil {
			continue
		}
		var res ResourceState
		if json.Unmarshal(data, &res) == nil {
			items = append(items, res)
		}
	}

	type response struct {
		Items []ResourceState `json:"items"`
	}
	return apigwJSONResponse(http.StatusOK, response{Items: items})
}

func (p *APIGatewayPlugin) deleteResource(ctx *RequestContext, apiID, resID string) (*AWSResponse, error) {
	goCtx := context.Background()
	if err := p.state.Delete(goCtx, apigatewayNamespace, apigwResourceKey(ctx.AccountID, ctx.Region, apiID, resID)); err != nil {
		return nil, fmt.Errorf("apigateway deleteResource state.Delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, apigatewayNamespace, apigwResourceIDsKey(ctx.AccountID, ctx.Region, apiID), resID)
	return apigwJSONResponse(http.StatusNoContent, struct{}{})
}

// --- Method operations -------------------------------------------------------

func (p *APIGatewayPlugin) putMethod(ctx *RequestContext, req *AWSRequest, apiID, resID, verb string) (*AWSResponse, error) {
	var body struct {
		AuthorizationType string `json:"authorizationType"`
		AuthorizerID      string `json:"authorizerId"`
		APIKeyRequired    bool   `json:"apiKeyRequired"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &body)
	}

	method := MethodState{
		HTTPMethod:        verb,
		AuthorizationType: body.AuthorizationType,
		AuthorizerID:      body.AuthorizerID,
		APIKeyRequired:    body.APIKeyRequired,
	}

	goCtx := context.Background()
	data, err := json.Marshal(method)
	if err != nil {
		return nil, fmt.Errorf("apigateway putMethod marshal: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayNamespace, apigwMethodKey(ctx.AccountID, ctx.Region, apiID, resID, verb), data); err != nil {
		return nil, fmt.Errorf("apigateway putMethod state.Put: %w", err)
	}
	return apigwJSONResponse(http.StatusCreated, method)
}

func (p *APIGatewayPlugin) getMethod(ctx *RequestContext, apiID, resID, verb string) (*AWSResponse, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, apigatewayNamespace, apigwMethodKey(ctx.AccountID, ctx.Region, apiID, resID, verb))
	if err != nil {
		return nil, fmt.Errorf("apigateway getMethod state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Method not found: " + verb, HTTPStatus: http.StatusNotFound}
	}
	var method MethodState
	if err := json.Unmarshal(data, &method); err != nil {
		return nil, fmt.Errorf("apigateway getMethod unmarshal: %w", err)
	}
	return apigwJSONResponse(http.StatusOK, method)
}

func (p *APIGatewayPlugin) deleteMethod(ctx *RequestContext, apiID, resID, verb string) (*AWSResponse, error) {
	goCtx := context.Background()
	if err := p.state.Delete(goCtx, apigatewayNamespace, apigwMethodKey(ctx.AccountID, ctx.Region, apiID, resID, verb)); err != nil {
		return nil, fmt.Errorf("apigateway deleteMethod state.Delete: %w", err)
	}
	return apigwJSONResponse(http.StatusNoContent, struct{}{})
}

// --- Integration operations --------------------------------------------------

func (p *APIGatewayPlugin) putIntegration(ctx *RequestContext, req *AWSRequest, apiID, resID, verb string) (*AWSResponse, error) {
	var integration IntegrationState
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &integration)
	}

	goCtx := context.Background()
	data, err := json.Marshal(integration)
	if err != nil {
		return nil, fmt.Errorf("apigateway putIntegration marshal: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayNamespace, apigwIntegrationKey(ctx.AccountID, ctx.Region, apiID, resID, verb), data); err != nil {
		return nil, fmt.Errorf("apigateway putIntegration state.Put: %w", err)
	}
	return apigwJSONResponse(http.StatusCreated, integration)
}

func (p *APIGatewayPlugin) putIntegrationResponse(_ *RequestContext, req *AWSRequest, _, _, _, _ string) (*AWSResponse, error) {
	var body map[string]interface{}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &body)
	}
	return apigwJSONResponse(http.StatusCreated, body)
}

func (p *APIGatewayPlugin) putMethodResponse(_ *RequestContext, req *AWSRequest, _, _, _, _ string) (*AWSResponse, error) {
	var body map[string]interface{}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &body)
	}
	return apigwJSONResponse(http.StatusCreated, body)
}

// --- Deployment operations ---------------------------------------------------

func (p *APIGatewayPlugin) createDeployment(ctx *RequestContext, req *AWSRequest, apiID string) (*AWSResponse, error) {
	var body struct {
		Description string `json:"description"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &body)
	}

	dep := DeploymentState{
		ID:          generateAPIGatewayID(),
		Description: body.Description,
		CreatedDate: p.tc.Now(),
		APIId:       apiID,
		AccountID:   ctx.AccountID,
		Region:      ctx.Region,
	}

	goCtx := context.Background()
	data, err := json.Marshal(dep)
	if err != nil {
		return nil, fmt.Errorf("apigateway createDeployment marshal: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayNamespace, apigwDeploymentKey(ctx.AccountID, ctx.Region, apiID, dep.ID), data); err != nil {
		return nil, fmt.Errorf("apigateway createDeployment state.Put: %w", err)
	}
	updateStringIndex(goCtx, p.state, apigatewayNamespace, apigwDeploymentIDsKey(ctx.AccountID, ctx.Region, apiID), dep.ID)

	return apigwJSONResponse(http.StatusCreated, dep)
}

func (p *APIGatewayPlugin) getDeployment(ctx *RequestContext, apiID, deployID string) (*AWSResponse, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, apigatewayNamespace, apigwDeploymentKey(ctx.AccountID, ctx.Region, apiID, deployID))
	if err != nil {
		return nil, fmt.Errorf("apigateway getDeployment state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Deployment not found: " + deployID, HTTPStatus: http.StatusNotFound}
	}
	var dep DeploymentState
	if err := json.Unmarshal(data, &dep); err != nil {
		return nil, fmt.Errorf("apigateway getDeployment unmarshal: %w", err)
	}
	return apigwJSONResponse(http.StatusOK, dep)
}

func (p *APIGatewayPlugin) getDeployments(ctx *RequestContext, apiID string) (*AWSResponse, error) {
	goCtx := context.Background()
	ids, err := loadStringIndex(goCtx, p.state, apigatewayNamespace, apigwDeploymentIDsKey(ctx.AccountID, ctx.Region, apiID))
	if err != nil {
		return nil, fmt.Errorf("apigateway getDeployments loadIndex: %w", err)
	}

	items := make([]DeploymentState, 0, len(ids))
	for _, id := range ids {
		data, getErr := p.state.Get(goCtx, apigatewayNamespace, apigwDeploymentKey(ctx.AccountID, ctx.Region, apiID, id))
		if getErr != nil || data == nil {
			continue
		}
		var dep DeploymentState
		if json.Unmarshal(data, &dep) == nil {
			items = append(items, dep)
		}
	}

	type response struct {
		Items []DeploymentState `json:"items"`
	}
	return apigwJSONResponse(http.StatusOK, response{Items: items})
}

func (p *APIGatewayPlugin) deleteDeployment(ctx *RequestContext, apiID, deployID string) (*AWSResponse, error) {
	goCtx := context.Background()
	if err := p.state.Delete(goCtx, apigatewayNamespace, apigwDeploymentKey(ctx.AccountID, ctx.Region, apiID, deployID)); err != nil {
		return nil, fmt.Errorf("apigateway deleteDeployment state.Delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, apigatewayNamespace, apigwDeploymentIDsKey(ctx.AccountID, ctx.Region, apiID), deployID)
	return apigwJSONResponse(http.StatusNoContent, struct{}{})
}

// --- Stage operations --------------------------------------------------------

func (p *APIGatewayPlugin) createStage(ctx *RequestContext, req *AWSRequest, apiID string) (*AWSResponse, error) {
	var body struct {
		StageName    string            `json:"stageName"`
		DeploymentID string            `json:"deploymentId"`
		Description  string            `json:"description"`
		Variables    map[string]string `json:"variables"`
		Tags         map[string]string `json:"tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "BadRequestException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.StageName == "" {
		return nil, &AWSError{Code: "BadRequestException", Message: "stageName is required", HTTPStatus: http.StatusBadRequest}
	}

	stage := StageState{
		StageName:    body.StageName,
		DeploymentID: body.DeploymentID,
		Description:  body.Description,
		Variables:    body.Variables,
		Tags:         body.Tags,
		CreatedDate:  p.tc.Now(),
		APIId:        apiID,
		AccountID:    ctx.AccountID,
		Region:       ctx.Region,
	}

	goCtx := context.Background()
	data, err := json.Marshal(stage)
	if err != nil {
		return nil, fmt.Errorf("apigateway createStage marshal: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayNamespace, apigwStageKey(ctx.AccountID, ctx.Region, apiID, stage.StageName), data); err != nil {
		return nil, fmt.Errorf("apigateway createStage state.Put: %w", err)
	}
	updateStringIndex(goCtx, p.state, apigatewayNamespace, apigwStageNamesKey(ctx.AccountID, ctx.Region, apiID), stage.StageName)

	return apigwJSONResponse(http.StatusCreated, stage)
}

func (p *APIGatewayPlugin) getStage(ctx *RequestContext, apiID, stageName string) (*AWSResponse, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, apigatewayNamespace, apigwStageKey(ctx.AccountID, ctx.Region, apiID, stageName))
	if err != nil {
		return nil, fmt.Errorf("apigateway getStage state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Stage not found: " + stageName, HTTPStatus: http.StatusNotFound}
	}
	var stage StageState
	if err := json.Unmarshal(data, &stage); err != nil {
		return nil, fmt.Errorf("apigateway getStage unmarshal: %w", err)
	}

	// Augment with computed InvokeURL.
	type stageWithURL struct {
		StageState
		InvokeURL string `json:"InvokeUrl"`
	}
	sw := stageWithURL{
		StageState: stage,
		InvokeURL:  fmt.Sprintf("https://%s.execute-api.%s.amazonaws.com/%s", apiID, ctx.Region, stageName),
	}
	return apigwJSONResponse(http.StatusOK, sw)
}

func (p *APIGatewayPlugin) getStages(ctx *RequestContext, apiID string) (*AWSResponse, error) {
	goCtx := context.Background()
	names, err := loadStringIndex(goCtx, p.state, apigatewayNamespace, apigwStageNamesKey(ctx.AccountID, ctx.Region, apiID))
	if err != nil {
		return nil, fmt.Errorf("apigateway getStages loadIndex: %w", err)
	}

	items := make([]StageState, 0, len(names))
	for _, name := range names {
		data, getErr := p.state.Get(goCtx, apigatewayNamespace, apigwStageKey(ctx.AccountID, ctx.Region, apiID, name))
		if getErr != nil || data == nil {
			continue
		}
		var stage StageState
		if json.Unmarshal(data, &stage) == nil {
			items = append(items, stage)
		}
	}

	type response struct {
		Item []StageState `json:"item"`
	}
	return apigwJSONResponse(http.StatusOK, response{Item: items})
}

func (p *APIGatewayPlugin) deleteStage(ctx *RequestContext, apiID, stageName string) (*AWSResponse, error) {
	goCtx := context.Background()
	if err := p.state.Delete(goCtx, apigatewayNamespace, apigwStageKey(ctx.AccountID, ctx.Region, apiID, stageName)); err != nil {
		return nil, fmt.Errorf("apigateway deleteStage state.Delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, apigatewayNamespace, apigwStageNamesKey(ctx.AccountID, ctx.Region, apiID), stageName)
	return apigwJSONResponse(http.StatusNoContent, struct{}{})
}

func (p *APIGatewayPlugin) updateStage(ctx *RequestContext, _ *AWSRequest, apiID, stageName string) (*AWSResponse, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, apigatewayNamespace, apigwStageKey(ctx.AccountID, ctx.Region, apiID, stageName))
	if err != nil {
		return nil, fmt.Errorf("apigateway updateStage state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Stage not found: " + stageName, HTTPStatus: http.StatusNotFound}
	}
	var stage StageState
	if err := json.Unmarshal(data, &stage); err != nil {
		return nil, fmt.Errorf("apigateway updateStage unmarshal: %w", err)
	}
	updated, err := json.Marshal(stage)
	if err != nil {
		return nil, fmt.Errorf("apigateway updateStage marshal: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayNamespace, apigwStageKey(ctx.AccountID, ctx.Region, apiID, stageName), updated); err != nil {
		return nil, fmt.Errorf("apigateway updateStage state.Put: %w", err)
	}
	return apigwJSONResponse(http.StatusOK, stage)
}

// --- Authorizer operations ---------------------------------------------------

func (p *APIGatewayPlugin) createAuthorizer(ctx *RequestContext, req *AWSRequest, apiID string) (*AWSResponse, error) {
	var body struct {
		Name           string   `json:"name"`
		Type           string   `json:"type"`
		ProviderARNs   []string `json:"providerARNs"`
		AuthorizerURI  string   `json:"authorizerUri"`
		IdentitySource string   `json:"identitySource"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "BadRequestException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	auth := AuthorizerState{
		ID:             generateAPIGatewayID(),
		Name:           body.Name,
		Type:           body.Type,
		ProviderARNs:   body.ProviderARNs,
		AuthorizerURI:  body.AuthorizerURI,
		IdentitySource: body.IdentitySource,
		APIId:          apiID,
		AccountID:      ctx.AccountID,
		Region:         ctx.Region,
	}

	goCtx := context.Background()
	data, err := json.Marshal(auth)
	if err != nil {
		return nil, fmt.Errorf("apigateway createAuthorizer marshal: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayNamespace, apigwAuthorizerKey(ctx.AccountID, ctx.Region, apiID, auth.ID), data); err != nil {
		return nil, fmt.Errorf("apigateway createAuthorizer state.Put: %w", err)
	}
	updateStringIndex(goCtx, p.state, apigatewayNamespace, apigwAuthorizerIDsKey(ctx.AccountID, ctx.Region, apiID), auth.ID)

	return apigwJSONResponse(http.StatusCreated, auth)
}

func (p *APIGatewayPlugin) getAuthorizer(ctx *RequestContext, apiID, authID string) (*AWSResponse, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, apigatewayNamespace, apigwAuthorizerKey(ctx.AccountID, ctx.Region, apiID, authID))
	if err != nil {
		return nil, fmt.Errorf("apigateway getAuthorizer state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Authorizer not found: " + authID, HTTPStatus: http.StatusNotFound}
	}
	var auth AuthorizerState
	if err := json.Unmarshal(data, &auth); err != nil {
		return nil, fmt.Errorf("apigateway getAuthorizer unmarshal: %w", err)
	}
	return apigwJSONResponse(http.StatusOK, auth)
}

func (p *APIGatewayPlugin) getAuthorizers(ctx *RequestContext, apiID string) (*AWSResponse, error) {
	goCtx := context.Background()
	ids, err := loadStringIndex(goCtx, p.state, apigatewayNamespace, apigwAuthorizerIDsKey(ctx.AccountID, ctx.Region, apiID))
	if err != nil {
		return nil, fmt.Errorf("apigateway getAuthorizers loadIndex: %w", err)
	}

	items := make([]AuthorizerState, 0, len(ids))
	for _, id := range ids {
		data, getErr := p.state.Get(goCtx, apigatewayNamespace, apigwAuthorizerKey(ctx.AccountID, ctx.Region, apiID, id))
		if getErr != nil || data == nil {
			continue
		}
		var auth AuthorizerState
		if json.Unmarshal(data, &auth) == nil {
			items = append(items, auth)
		}
	}

	type response struct {
		Items []AuthorizerState `json:"items"`
	}
	return apigwJSONResponse(http.StatusOK, response{Items: items})
}

func (p *APIGatewayPlugin) deleteAuthorizer(ctx *RequestContext, apiID, authID string) (*AWSResponse, error) {
	goCtx := context.Background()
	if err := p.state.Delete(goCtx, apigatewayNamespace, apigwAuthorizerKey(ctx.AccountID, ctx.Region, apiID, authID)); err != nil {
		return nil, fmt.Errorf("apigateway deleteAuthorizer state.Delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, apigatewayNamespace, apigwAuthorizerIDsKey(ctx.AccountID, ctx.Region, apiID), authID)
	return apigwJSONResponse(http.StatusNoContent, struct{}{})
}

// --- API Key operations -------------------------------------------------------

func (p *APIGatewayPlugin) createAPIKey(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Name    string            `json:"name"`
		Enabled bool              `json:"enabled"`
		Tags    map[string]string `json:"tags"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &body)
	}

	keyID, err := generateACMCertID()
	if err != nil {
		return nil, fmt.Errorf("apigateway createApiKey generateID: %w", err)
	}
	keyValue, err := generateACMCertID()
	if err != nil {
		return nil, fmt.Errorf("apigateway createApiKey generateValue: %w", err)
	}

	key := APIKeyState{
		ID:          keyID,
		Name:        body.Name,
		Value:       keyValue,
		Enabled:     body.Enabled,
		Tags:        body.Tags,
		CreatedDate: p.tc.Now(),
		AccountID:   ctx.AccountID,
		Region:      ctx.Region,
	}

	goCtx := context.Background()
	data, err := json.Marshal(key)
	if err != nil {
		return nil, fmt.Errorf("apigateway createApiKey marshal: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayNamespace, apigwAPIKeyKey(ctx.AccountID, ctx.Region, key.ID), data); err != nil {
		return nil, fmt.Errorf("apigateway createApiKey state.Put: %w", err)
	}
	updateStringIndex(goCtx, p.state, apigatewayNamespace, apigwAPIKeyIDsKey(ctx.AccountID, ctx.Region), key.ID)

	return apigwJSONResponse(http.StatusCreated, key)
}

func (p *APIGatewayPlugin) getAPIKey(ctx *RequestContext, keyID string) (*AWSResponse, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, apigatewayNamespace, apigwAPIKeyKey(ctx.AccountID, ctx.Region, keyID))
	if err != nil {
		return nil, fmt.Errorf("apigateway getAPIKey state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "API key not found: " + keyID, HTTPStatus: http.StatusNotFound}
	}
	var key APIKeyState
	if err := json.Unmarshal(data, &key); err != nil {
		return nil, fmt.Errorf("apigateway getAPIKey unmarshal: %w", err)
	}
	return apigwJSONResponse(http.StatusOK, key)
}

func (p *APIGatewayPlugin) getAPIKeys(ctx *RequestContext) (*AWSResponse, error) {
	goCtx := context.Background()
	ids, err := loadStringIndex(goCtx, p.state, apigatewayNamespace, apigwAPIKeyIDsKey(ctx.AccountID, ctx.Region))
	if err != nil {
		return nil, fmt.Errorf("apigateway getApiKeys loadIndex: %w", err)
	}

	items := make([]APIKeyState, 0, len(ids))
	for _, id := range ids {
		data, getErr := p.state.Get(goCtx, apigatewayNamespace, apigwAPIKeyKey(ctx.AccountID, ctx.Region, id))
		if getErr != nil || data == nil {
			continue
		}
		var key APIKeyState
		if json.Unmarshal(data, &key) == nil {
			items = append(items, key)
		}
	}

	type response struct {
		Items []APIKeyState `json:"items"`
	}
	return apigwJSONResponse(http.StatusOK, response{Items: items})
}

func (p *APIGatewayPlugin) deleteAPIKey(ctx *RequestContext, keyID string) (*AWSResponse, error) {
	goCtx := context.Background()
	if err := p.state.Delete(goCtx, apigatewayNamespace, apigwAPIKeyKey(ctx.AccountID, ctx.Region, keyID)); err != nil {
		return nil, fmt.Errorf("apigateway deleteApiKey state.Delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, apigatewayNamespace, apigwAPIKeyIDsKey(ctx.AccountID, ctx.Region), keyID)
	return apigwJSONResponse(http.StatusNoContent, struct{}{})
}

// --- Usage plan operations ---------------------------------------------------

func (p *APIGatewayPlugin) createUsagePlan(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Name        string            `json:"name"`
		Description string            `json:"description"`
		Tags        map[string]string `json:"tags"`
		APIStages   []interface{}     `json:"apiStages"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &body)
	}

	plan := UsagePlanState{
		ID:          generateAPIGatewayID(),
		Name:        body.Name,
		Description: body.Description,
		Tags:        body.Tags,
		APIStages:   body.APIStages,
		CreatedDate: p.tc.Now(),
		AccountID:   ctx.AccountID,
		Region:      ctx.Region,
	}

	goCtx := context.Background()
	data, err := json.Marshal(plan)
	if err != nil {
		return nil, fmt.Errorf("apigateway createUsagePlan marshal: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayNamespace, apigwUsagePlanKey(ctx.AccountID, ctx.Region, plan.ID), data); err != nil {
		return nil, fmt.Errorf("apigateway createUsagePlan state.Put: %w", err)
	}
	updateStringIndex(goCtx, p.state, apigatewayNamespace, apigwUsagePlanIDsKey(ctx.AccountID, ctx.Region), plan.ID)

	return apigwJSONResponse(http.StatusCreated, plan)
}

func (p *APIGatewayPlugin) getUsagePlan(ctx *RequestContext, planID string) (*AWSResponse, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, apigatewayNamespace, apigwUsagePlanKey(ctx.AccountID, ctx.Region, planID))
	if err != nil {
		return nil, fmt.Errorf("apigateway getUsagePlan state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Usage plan not found: " + planID, HTTPStatus: http.StatusNotFound}
	}
	var plan UsagePlanState
	if err := json.Unmarshal(data, &plan); err != nil {
		return nil, fmt.Errorf("apigateway getUsagePlan unmarshal: %w", err)
	}
	return apigwJSONResponse(http.StatusOK, plan)
}

func (p *APIGatewayPlugin) getUsagePlans(ctx *RequestContext) (*AWSResponse, error) {
	goCtx := context.Background()
	ids, err := loadStringIndex(goCtx, p.state, apigatewayNamespace, apigwUsagePlanIDsKey(ctx.AccountID, ctx.Region))
	if err != nil {
		return nil, fmt.Errorf("apigateway getUsagePlans loadIndex: %w", err)
	}

	items := make([]UsagePlanState, 0, len(ids))
	for _, id := range ids {
		data, getErr := p.state.Get(goCtx, apigatewayNamespace, apigwUsagePlanKey(ctx.AccountID, ctx.Region, id))
		if getErr != nil || data == nil {
			continue
		}
		var plan UsagePlanState
		if json.Unmarshal(data, &plan) == nil {
			items = append(items, plan)
		}
	}

	type response struct {
		Items []UsagePlanState `json:"items"`
	}
	return apigwJSONResponse(http.StatusOK, response{Items: items})
}

func (p *APIGatewayPlugin) deleteUsagePlan(ctx *RequestContext, planID string) (*AWSResponse, error) {
	goCtx := context.Background()
	if err := p.state.Delete(goCtx, apigatewayNamespace, apigwUsagePlanKey(ctx.AccountID, ctx.Region, planID)); err != nil {
		return nil, fmt.Errorf("apigateway deleteUsagePlan state.Delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, apigatewayNamespace, apigwUsagePlanIDsKey(ctx.AccountID, ctx.Region), planID)
	return apigwJSONResponse(http.StatusNoContent, struct{}{})
}

// --- Domain name and base path mapping operations ----------------------------

func (p *APIGatewayPlugin) createDomainName(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		DomainName     string `json:"domainName"`
		CertificateArn string `json:"certificateArn"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "BadRequestException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	dn := DomainNameState{
		DomainName:         body.DomainName,
		CertificateArn:     body.CertificateArn,
		RegionalDomainName: body.DomainName + ".regional.execute-api." + ctx.Region + ".amazonaws.com",
		AccountID:          ctx.AccountID,
		Region:             ctx.Region,
	}

	goCtx := context.Background()
	data, err := json.Marshal(dn)
	if err != nil {
		return nil, fmt.Errorf("apigateway createDomainName marshal: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayNamespace, apigwDomainNameKey(ctx.AccountID, ctx.Region, body.DomainName), data); err != nil {
		return nil, fmt.Errorf("apigateway createDomainName state.Put: %w", err)
	}

	return apigwJSONResponse(http.StatusCreated, dn)
}

func (p *APIGatewayPlugin) getDomainName(ctx *RequestContext, name string) (*AWSResponse, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, apigatewayNamespace, apigwDomainNameKey(ctx.AccountID, ctx.Region, name))
	if err != nil {
		return nil, fmt.Errorf("apigateway getDomainName state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Domain name not found: " + name, HTTPStatus: http.StatusNotFound}
	}
	var dn DomainNameState
	if err := json.Unmarshal(data, &dn); err != nil {
		return nil, fmt.Errorf("apigateway getDomainName unmarshal: %w", err)
	}
	return apigwJSONResponse(http.StatusOK, dn)
}

func (p *APIGatewayPlugin) createBasePathMapping(ctx *RequestContext, req *AWSRequest, domainName string) (*AWSResponse, error) {
	var body struct {
		BasePath  string `json:"basePath"`
		RestAPIID string `json:"restApiId"`
		Stage     string `json:"stage"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "BadRequestException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.BasePath == "" {
		body.BasePath = "(none)"
	}

	mapping := BasePathMappingState{
		BasePath:   body.BasePath,
		RestAPIID:  body.RestAPIID,
		Stage:      body.Stage,
		DomainName: domainName,
	}

	goCtx := context.Background()
	data, err := json.Marshal(mapping)
	if err != nil {
		return nil, fmt.Errorf("apigateway createBasePathMapping marshal: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayNamespace, apigwBasePathKey(ctx.AccountID, ctx.Region, domainName, body.BasePath), data); err != nil {
		return nil, fmt.Errorf("apigateway createBasePathMapping state.Put: %w", err)
	}

	return apigwJSONResponse(http.StatusCreated, mapping)
}

func (p *APIGatewayPlugin) getBasePathMappings(ctx *RequestContext, domainName string) (*AWSResponse, error) {
	goCtx := context.Background()
	prefix := "basepath:" + ctx.AccountID + "/" + ctx.Region + "/" + domainName + "/"
	keys, err := p.state.List(goCtx, apigatewayNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("apigateway getBasePathMappings state.List: %w", err)
	}

	items := make([]BasePathMappingState, 0, len(keys))
	for _, k := range keys {
		data, getErr := p.state.Get(goCtx, apigatewayNamespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var m BasePathMappingState
		if json.Unmarshal(data, &m) == nil {
			items = append(items, m)
		}
	}

	type response struct {
		Items []BasePathMappingState `json:"items"`
	}
	return apigwJSONResponse(http.StatusOK, response{Items: items})
}

// --- ID generation -----------------------------------------------------------

// generateAPIGatewayID generates a 10-character lowercase alphanumeric ID
// suitable for use as an API Gateway resource identifier.
func generateAPIGatewayID() string {
	b := make([]byte, 5)
	_, _ = rand.Read(b)
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	out := make([]byte, 10)
	for i, by := range b {
		out[i*2] = chars[by>>4%36]
		out[i*2+1] = chars[by&0xf%36]
	}
	return string(out)
}

// --- Response helper ---------------------------------------------------------

// apigwJSONResponse marshals v as JSON and returns an AWSResponse with
// Content-Type: application/json and the given HTTP status code.
func apigwJSONResponse(status int, v any) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("apigwJSONResponse marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       body,
	}, nil
}

// --- API Gateway Proxy Plugin ------------------------------------------------

// APIGatewayProxyPlugin handles runtime (stage) invocations for API Gateway
// v1 and v2 APIs. Requests arrive at {apiId}.execute-api.{region}.amazonaws.com
// and are dispatched to the configured Lambda AWS_PROXY integration.
type APIGatewayProxyPlugin struct {
	state    StateManager
	logger   Logger
	registry *PluginRegistry
}

// Name returns the service name "execute-api".
func (p *APIGatewayProxyPlugin) Name() string { return "execute-api" }

// Initialize sets up the APIGatewayProxyPlugin.
func (p *APIGatewayProxyPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if reg, ok := cfg.Options["registry"].(*PluginRegistry); ok {
		p.registry = reg
	}
	return nil
}

// Shutdown is a no-op for APIGatewayProxyPlugin.
func (p *APIGatewayProxyPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest routes an API Gateway runtime request to a Lambda function
// via the registered AWS_PROXY integration.
func (p *APIGatewayProxyPlugin) HandleRequest(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	if p.registry == nil {
		return proxyError(http.StatusInternalServerError, "proxy plugin not wired to registry")
	}

	// Extract API ID from the Host header: {apiId}.execute-api.{region}.amazonaws.com
	host := req.Headers["Host"]
	if host == "" {
		return proxyError(http.StatusBadRequest, "missing Host header")
	}
	// Strip port.
	if colon := strings.LastIndexByte(host, ':'); colon > 0 {
		host = host[:colon]
	}
	dotIdx := strings.Index(host, ".execute-api.")
	if dotIdx < 0 {
		return proxyError(http.StatusBadRequest, "unexpected host: "+host)
	}
	apiID := host[:dotIdx]

	// Parse path: /{stage}/{resourcePath...}
	rawPath := req.Path
	rawPath = strings.TrimPrefix(rawPath, "/")
	slashIdx := strings.Index(rawPath, "/")
	var stageName, resourcePath string
	if slashIdx < 0 {
		stageName = rawPath
		resourcePath = "/"
	} else {
		stageName = rawPath[:slashIdx]
		resourcePath = rawPath[slashIdx:]
	}

	// Try v2 API first.
	lambdaARN, isV2, err := p.resolveLambdaARN(reqCtx, apiID, req.Operation, resourcePath)
	if err != nil {
		return proxyError(http.StatusBadGateway, "no Lambda integration found: "+err.Error())
	}

	// Extract function name from ARN.
	arnParts := strings.Split(lambdaARN, ":")
	fnName := arnParts[len(arnParts)-1]

	var eventJSON []byte
	if isV2 {
		eventJSON, err = buildV2ProxyEvent(req, apiID, stageName, resourcePath)
	} else {
		eventJSON, err = buildV1ProxyEvent(req, apiID, stageName, resourcePath)
	}
	if err != nil {
		return proxyError(http.StatusInternalServerError, "build proxy event: "+err.Error())
	}

	// Invoke Lambda via registry.
	invokeReq := &AWSRequest{
		Service:   "lambda",
		Operation: "POST",
		Path:      "/2015-03-31/functions/" + fnName + "/invocations",
		Headers:   map[string]string{"Content-Type": "application/json"},
		Body:      eventJSON,
	}
	invokeCtx := &RequestContext{
		RequestID: generateRequestID(),
		AccountID: reqCtx.AccountID,
		Region:    reqCtx.Region,
		Timestamp: reqCtx.Timestamp,
		Metadata:  make(map[string]interface{}),
	}
	invokeResp, invokeErr := p.registry.RouteRequest(invokeCtx, invokeReq)
	if invokeErr != nil {
		return proxyError(http.StatusBadGateway, "lambda invoke: "+invokeErr.Error())
	}
	if invokeResp == nil {
		return proxyError(http.StatusBadGateway, "nil lambda response")
	}

	// Parse Lambda proxy response.
	return parseProxyResponse(invokeResp.Body)
}

// resolveLambdaARN looks up the Lambda ARN for the given API, HTTP method, and
// resource path. It checks v2 APIs first, then v1.
func (p *APIGatewayProxyPlugin) resolveLambdaARN(reqCtx *RequestContext, apiID, httpMethod, resourcePath string) (arn string, isV2 bool, err error) {
	goCtx := context.Background()
	acct := reqCtx.AccountID
	region := reqCtx.Region

	// --- Try v2 ---
	apiData, _ := p.state.Get(goCtx, apigatewayv2Namespace, apigwv2APIKey(acct, region, apiID))
	if apiData != nil {
		// Find a matching route (exact or $default).
		routeIDs, _ := p.loadStringList(goCtx, apigatewayv2Namespace, apigwv2RouteIDsKey(acct, region, apiID))
		var defaultIntID, matchIntID string
		for _, rid := range routeIDs {
			routeData, _ := p.state.Get(goCtx, apigatewayv2Namespace, apigwv2RouteKey(acct, region, apiID, rid))
			if routeData == nil {
				continue
			}
			var route V2RouteState
			if json.Unmarshal(routeData, &route) != nil {
				continue
			}
			// Extract integration ID from target "integrations/{id}".
			target := strings.TrimPrefix(route.Target, "integrations/")
			if route.RouteKey == "$default" {
				defaultIntID = target
			} else {
				// RouteKey format: "METHOD /path" or just "/path"
				rk := route.RouteKey
				method, path, _ := strings.Cut(rk, " ")
				if (method == httpMethod || method == "ANY") && path == resourcePath {
					matchIntID = target
					break
				}
			}
		}
		intID := matchIntID
		if intID == "" {
			intID = defaultIntID
		}
		if intID != "" {
			intData, _ := p.state.Get(goCtx, apigatewayv2Namespace, apigwv2IntegrationKey(acct, region, apiID, intID))
			if intData != nil {
				var integration V2IntegrationState
				if json.Unmarshal(intData, &integration) == nil && integration.IntegrationType == "AWS_PROXY" {
					lambdaARN := extractLambdaARNFromURI(integration.IntegrationURI)
					if lambdaARN != "" {
						return lambdaARN, true, nil
					}
				}
			}
		}
	}

	// --- Try v1 ---
	v1APIData, _ := p.state.Get(goCtx, apigatewayNamespace, apigwAPIKey(acct, region, apiID))
	if v1APIData == nil {
		return "", false, fmt.Errorf("API %s not found", apiID)
	}
	// Enumerate resources to find a matching path.
	resIDs, _ := p.loadStringList(goCtx, apigatewayNamespace, apigwResourceIDsKey(acct, region, apiID))
	for _, rid := range resIDs {
		resData, _ := p.state.Get(goCtx, apigatewayNamespace, apigwResourceKey(acct, region, apiID, rid))
		if resData == nil {
			continue
		}
		var res ResourceState
		if json.Unmarshal(resData, &res) != nil {
			continue
		}
		if res.Path != resourcePath {
			continue
		}
		// Check integration for this resource and method.
		intData, _ := p.state.Get(goCtx, apigatewayNamespace, apigwIntegrationKey(acct, region, apiID, rid, httpMethod))
		if intData == nil {
			// Try ANY.
			intData, _ = p.state.Get(goCtx, apigatewayNamespace, apigwIntegrationKey(acct, region, apiID, rid, "ANY"))
		}
		if intData == nil {
			continue
		}
		var integration IntegrationState
		if json.Unmarshal(intData, &integration) != nil {
			continue
		}
		if integration.Type != "AWS_PROXY" {
			continue
		}
		lambdaARN := extractLambdaARNFromURI(integration.URI)
		if lambdaARN != "" {
			return lambdaARN, false, nil
		}
	}

	return "", false, fmt.Errorf("no AWS_PROXY integration for %s %s", httpMethod, resourcePath)
}

// loadStringList loads a JSON-encoded string slice from state.
func (p *APIGatewayProxyPlugin) loadStringList(ctx context.Context, ns, key string) ([]string, error) {
	data, err := p.state.Get(ctx, ns, key)
	if err != nil || data == nil {
		return nil, err
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		return nil, err
	}
	return ids, nil
}

// extractLambdaARNFromURI parses the Lambda ARN from an integration URI of the
// form: arn:aws:apigateway:{region}:lambda:path/2015-03-31/functions/{lambdaArn}/invocations
// or the Lambda ARN directly.
func extractLambdaARNFromURI(uri string) string {
	const marker = "functions/"
	idx := strings.Index(uri, marker)
	if idx < 0 {
		// Check if the URI is already a Lambda ARN.
		if strings.HasPrefix(uri, "arn:aws:lambda:") {
			return uri
		}
		return ""
	}
	rest := uri[idx+len(marker):]
	if end := strings.Index(rest, "/"); end >= 0 {
		rest = rest[:end]
	}
	return rest
}

// buildV1ProxyEvent constructs a v1 (REST API) proxy event JSON payload.
func buildV1ProxyEvent(req *AWSRequest, apiID, stage, resourcePath string) ([]byte, error) {
	qs := make(map[string]string)
	for k, v := range req.Params {
		qs[k] = v
	}
	event := map[string]interface{}{
		"version":    "1.0",
		"httpMethod": req.Operation,
		"path":       "/" + stage + resourcePath,
		"resource":   resourcePath,
		"headers":    req.Headers,
		"queryStringParameters": func() interface{} {
			if len(qs) == 0 {
				return nil
			}
			return qs
		}(),
		"pathParameters":  nil,
		"stageVariables":  map[string]string{},
		"isBase64Encoded": false,
		"body": func() interface{} {
			if len(req.Body) == 0 {
				return nil
			}
			return string(req.Body)
		}(),
		"requestContext": map[string]interface{}{
			"stage":        stage,
			"requestId":    generateRequestID(),
			"httpMethod":   req.Operation,
			"resourcePath": resourcePath,
			"apiId":        apiID,
		},
	}
	return json.Marshal(event)
}

// buildV2ProxyEvent constructs a v2 (HTTP API) proxy event JSON payload.
func buildV2ProxyEvent(req *AWSRequest, apiID, stage, resourcePath string) ([]byte, error) {
	rawQS := ""
	sep := ""
	for k, v := range req.Params {
		rawQS += sep + k + "=" + v
		sep = "&"
	}
	event := map[string]interface{}{
		"version":         "2.0",
		"routeKey":        req.Operation + " " + resourcePath,
		"rawPath":         "/" + stage + resourcePath,
		"rawQueryString":  rawQS,
		"headers":         req.Headers,
		"isBase64Encoded": false,
		"body": func() interface{} {
			if len(req.Body) == 0 {
				return nil
			}
			return string(req.Body)
		}(),
		"requestContext": map[string]interface{}{
			"routeKey":  req.Operation + " " + resourcePath,
			"stage":     stage,
			"requestId": generateRequestID(),
			"apiId":     apiID,
			"http": map[string]interface{}{
				"method": req.Operation,
				"path":   "/" + stage + resourcePath,
			},
		},
	}
	return json.Marshal(event)
}

// proxyResponseShape is the expected shape of a Lambda proxy response.
type proxyResponseShape struct {
	StatusCode      int               `json:"statusCode"`
	Headers         map[string]string `json:"headers"`
	Body            string            `json:"body"`
	IsBase64Encoded bool              `json:"isBase64Encoded"`
}

// parseProxyResponse decodes a Lambda proxy response body into an AWSResponse.
func parseProxyResponse(body []byte) (*AWSResponse, error) {
	var pr proxyResponseShape
	if err := json.Unmarshal(body, &pr); err != nil {
		// If we can't parse, pass the raw body through with 200.
		return &AWSResponse{ //nolint:nilerr
			StatusCode: http.StatusOK,
			Headers:    map[string]string{"Content-Type": "application/json"},
			Body:       body,
		}, nil
	}
	status := pr.StatusCode
	if status == 0 {
		status = http.StatusOK
	}
	headers := pr.Headers
	if headers == nil {
		headers = map[string]string{"Content-Type": "application/json"}
	}
	var responseBody []byte
	if pr.IsBase64Encoded {
		decoded, decErr := base64.StdEncoding.DecodeString(pr.Body)
		if decErr == nil {
			responseBody = decoded
		} else {
			responseBody = []byte(pr.Body)
		}
	} else {
		responseBody = []byte(pr.Body)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    headers,
		Body:       responseBody,
	}, nil
}

// proxyError returns a simple JSON error AWSResponse.
func proxyError(status int, msg string) (*AWSResponse, error) {
	body, _ := json.Marshal(map[string]string{"message": msg})
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       body,
	}, nil
}
