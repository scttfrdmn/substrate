package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// APIGatewayV2Plugin emulates the AWS API Gateway v2 (HTTP/WebSocket) service.
// It uses path-based routing since there is no X-Amz-Target header.
type APIGatewayV2Plugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "apigatewayv2".
func (p *APIGatewayV2Plugin) Name() string { return "apigatewayv2" }

// Initialize sets up the APIGatewayV2Plugin with the provided configuration.
func (p *APIGatewayV2Plugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for APIGatewayV2Plugin.
func (p *APIGatewayV2Plugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an API Gateway v2 request to the appropriate handler
// using path-based operation routing.
func (p *APIGatewayV2Plugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op, params := parseAPIGatewayV2Operation(req.Operation, req.Path)
	switch op {
	case "CreateApi":
		return p.createAPI(ctx, req)
	case "GetApi":
		return p.getAPI(ctx, params["apiId"])
	case "GetApis":
		return p.getAPIs(ctx)
	case "DeleteApi":
		return p.deleteAPI(ctx, params["apiId"])
	case "UpdateApi":
		return p.updateAPI(ctx, req, params["apiId"])
	case "CreateRoute":
		return p.createRoute(ctx, req, params["apiId"])
	case "GetRoute":
		return p.getRoute(ctx, params["apiId"], params["routeId"])
	case "GetRoutes":
		return p.getRoutes(ctx, params["apiId"])
	case "DeleteRoute":
		return p.deleteRoute(ctx, params["apiId"], params["routeId"])
	case "CreateIntegration":
		return p.createIntegration(ctx, req, params["apiId"])
	case "GetIntegration":
		return p.getIntegration(ctx, params["apiId"], params["intId"])
	case "GetIntegrations":
		return p.getIntegrations(ctx, params["apiId"])
	case "DeleteIntegration":
		return p.deleteIntegration(ctx, params["apiId"], params["intId"])
	case "CreateStage":
		return p.createStageV2(ctx, req, params["apiId"])
	case "GetStage":
		return p.getStageV2(ctx, params["apiId"], params["stageName"])
	case "GetStages":
		return p.getStagesV2(ctx, params["apiId"])
	case "DeleteStage":
		return p.deleteStageV2(ctx, params["apiId"], params["stageName"])
	case "CreateAuthorizer":
		return p.createAuthorizerV2(ctx, req, params["apiId"])
	case "GetAuthorizer":
		return p.getAuthorizerV2(ctx, params["apiId"], params["authId"])
	case "GetAuthorizers":
		return p.getAuthorizersV2(ctx, params["apiId"])
	case "DeleteAuthorizer":
		return p.deleteAuthorizerV2(ctx, params["apiId"], params["authId"])
	case "CreateDeployment":
		return p.createDeploymentV2(ctx, req, params["apiId"])
	case "GetDeployment":
		return p.getDeploymentV2(ctx, params["apiId"], params["depId"])
	case "CreateDomainName":
		return p.createDomainNameV2(ctx, req)
	case "GetDomainName":
		return p.getDomainNameV2(ctx, params["name"])
	case "CreateApiMapping":
		return p.createAPIMapping(ctx, req, params["name"])
	default:
		return nil, &AWSError{
			Code:       "NotFoundException",
			Message:    fmt.Sprintf("APIGatewayV2Plugin: unknown operation for %s %s", req.Operation, req.Path),
			HTTPStatus: http.StatusNotFound,
		}
	}
}

// parseAPIGatewayV2Operation maps an HTTP method and path to an operation name
// and a map of path parameters extracted from the URL segments.
func parseAPIGatewayV2Operation(method, path string) (string, map[string]string) {
	params := make(map[string]string)

	// Normalize: strip leading /v2 prefix if present.
	p := strings.TrimPrefix(path, "/v2")
	if p == "" {
		p = "/"
	}
	parts := splitPath(p)

	switch {
	// /apis
	case len(parts) == 1 && parts[0] == "apis":
		switch method {
		case "POST":
			return "CreateApi", params
		case "GET":
			return "GetApis", params
		}

	// /apis/{id}
	case len(parts) == 2 && parts[0] == "apis":
		params["apiId"] = parts[1]
		switch method {
		case "GET":
			return "GetApi", params
		case "DELETE":
			return "DeleteApi", params
		case "PATCH":
			return "UpdateApi", params
		}

	// /apis/{id}/routes
	case len(parts) == 3 && parts[0] == "apis" && parts[2] == "routes":
		params["apiId"] = parts[1]
		switch method {
		case "POST":
			return "CreateRoute", params
		case "GET":
			return "GetRoutes", params
		}

	// /apis/{id}/routes/{routeId}
	case len(parts) == 4 && parts[0] == "apis" && parts[2] == "routes":
		params["apiId"] = parts[1]
		params["routeId"] = parts[3]
		switch method {
		case "GET":
			return "GetRoute", params
		case "DELETE":
			return "DeleteRoute", params
		}

	// /apis/{id}/integrations
	case len(parts) == 3 && parts[0] == "apis" && parts[2] == "integrations":
		params["apiId"] = parts[1]
		switch method {
		case "POST":
			return "CreateIntegration", params
		case "GET":
			return "GetIntegrations", params
		}

	// /apis/{id}/integrations/{intId}
	case len(parts) == 4 && parts[0] == "apis" && parts[2] == "integrations":
		params["apiId"] = parts[1]
		params["intId"] = parts[3]
		switch method {
		case "GET":
			return "GetIntegration", params
		case "DELETE":
			return "DeleteIntegration", params
		}

	// /apis/{id}/stages
	case len(parts) == 3 && parts[0] == "apis" && parts[2] == "stages":
		params["apiId"] = parts[1]
		switch method {
		case "POST":
			return "CreateStage", params
		case "GET":
			return "GetStages", params
		}

	// /apis/{id}/stages/{stageName}
	case len(parts) == 4 && parts[0] == "apis" && parts[2] == "stages":
		params["apiId"] = parts[1]
		params["stageName"] = parts[3]
		switch method {
		case "GET":
			return "GetStage", params
		case "DELETE":
			return "DeleteStage", params
		}

	// /apis/{id}/authorizers
	case len(parts) == 3 && parts[0] == "apis" && parts[2] == "authorizers":
		params["apiId"] = parts[1]
		switch method {
		case "POST":
			return "CreateAuthorizer", params
		case "GET":
			return "GetAuthorizers", params
		}

	// /apis/{id}/authorizers/{authId}
	case len(parts) == 4 && parts[0] == "apis" && parts[2] == "authorizers":
		params["apiId"] = parts[1]
		params["authId"] = parts[3]
		switch method {
		case "GET":
			return "GetAuthorizer", params
		case "DELETE":
			return "DeleteAuthorizer", params
		}

	// /apis/{id}/deployments
	case len(parts) == 3 && parts[0] == "apis" && parts[2] == "deployments":
		params["apiId"] = parts[1]
		switch method {
		case "POST":
			return "CreateDeployment", params
		}

	// /apis/{id}/deployments/{depId}
	case len(parts) == 4 && parts[0] == "apis" && parts[2] == "deployments":
		params["apiId"] = parts[1]
		params["depId"] = parts[3]
		if method == "GET" {
			return "GetDeployment", params
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

	// /domainnames/{name}/apimappings
	case len(parts) == 3 && parts[0] == "domainnames" && parts[2] == "apimappings":
		params["name"] = parts[1]
		if method == "POST" {
			return "CreateApiMapping", params
		}
	}

	return "", params
}

// --- API operations ----------------------------------------------------------

func (p *APIGatewayV2Plugin) createAPI(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Name         string            `json:"Name"`
		ProtocolType string            `json:"ProtocolType"`
		Description  string            `json:"Description"`
		Tags         map[string]string `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "BadRequestException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.Name == "" {
		return nil, &AWSError{Code: "BadRequestException", Message: "Name is required", HTTPStatus: http.StatusBadRequest}
	}
	if body.ProtocolType == "" {
		body.ProtocolType = "HTTP"
	}

	apiID := generateAPIGatewayID()
	api := V2ApiState{
		APIID:        apiID,
		Name:         body.Name,
		ProtocolType: body.ProtocolType,
		Description:  body.Description,
		APIEndpoint:  fmt.Sprintf("https://%s.execute-api.%s.amazonaws.com", apiID, ctx.Region),
		Tags:         body.Tags,
		CreatedDate:  p.tc.Now(),
		AccountID:    ctx.AccountID,
		Region:       ctx.Region,
	}

	goCtx := context.Background()
	data, err := json.Marshal(api)
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 createApi marshal: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayv2Namespace, apigwv2APIKey(ctx.AccountID, ctx.Region, apiID), data); err != nil {
		return nil, fmt.Errorf("apigatewayv2 createApi state.Put: %w", err)
	}
	updateStringIndex(goCtx, p.state, apigatewayv2Namespace, apigwv2APIIDsKey(ctx.AccountID, ctx.Region), apiID)

	return apigwJSONResponse(http.StatusCreated, api)
}

func (p *APIGatewayV2Plugin) getAPI(ctx *RequestContext, apiID string) (*AWSResponse, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, apigatewayv2Namespace, apigwv2APIKey(ctx.AccountID, ctx.Region, apiID))
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 getApi state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "API not found: " + apiID, HTTPStatus: http.StatusNotFound}
	}
	var api V2ApiState
	if err := json.Unmarshal(data, &api); err != nil {
		return nil, fmt.Errorf("apigatewayv2 getApi unmarshal: %w", err)
	}
	return apigwJSONResponse(http.StatusOK, api)
}

func (p *APIGatewayV2Plugin) getAPIs(ctx *RequestContext) (*AWSResponse, error) {
	goCtx := context.Background()
	ids, err := loadStringIndex(goCtx, p.state, apigatewayv2Namespace, apigwv2APIIDsKey(ctx.AccountID, ctx.Region))
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 getApis loadIndex: %w", err)
	}

	items := make([]V2ApiState, 0, len(ids))
	for _, id := range ids {
		data, getErr := p.state.Get(goCtx, apigatewayv2Namespace, apigwv2APIKey(ctx.AccountID, ctx.Region, id))
		if getErr != nil || data == nil {
			continue
		}
		var api V2ApiState
		if json.Unmarshal(data, &api) == nil {
			items = append(items, api)
		}
	}

	type response struct {
		Items []V2ApiState `json:"Items"`
	}
	return apigwJSONResponse(http.StatusOK, response{Items: items})
}

func (p *APIGatewayV2Plugin) deleteAPI(ctx *RequestContext, apiID string) (*AWSResponse, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, apigatewayv2Namespace, apigwv2APIKey(ctx.AccountID, ctx.Region, apiID))
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 deleteApi state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "API not found: " + apiID, HTTPStatus: http.StatusNotFound}
	}
	if err := p.state.Delete(goCtx, apigatewayv2Namespace, apigwv2APIKey(ctx.AccountID, ctx.Region, apiID)); err != nil {
		return nil, fmt.Errorf("apigatewayv2 deleteApi state.Delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, apigatewayv2Namespace, apigwv2APIIDsKey(ctx.AccountID, ctx.Region), apiID)
	return apigwJSONResponse(http.StatusNoContent, struct{}{})
}

func (p *APIGatewayV2Plugin) updateAPI(ctx *RequestContext, req *AWSRequest, apiID string) (*AWSResponse, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, apigatewayv2Namespace, apigwv2APIKey(ctx.AccountID, ctx.Region, apiID))
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 updateApi state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "API not found: " + apiID, HTTPStatus: http.StatusNotFound}
	}
	var api V2ApiState
	if err := json.Unmarshal(data, &api); err != nil {
		return nil, fmt.Errorf("apigatewayv2 updateApi unmarshal: %w", err)
	}

	var patch struct {
		Name        string `json:"Name"`
		Description string `json:"Description"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &patch)
	}
	if patch.Name != "" {
		api.Name = patch.Name
	}
	if patch.Description != "" {
		api.Description = patch.Description
	}

	updated, err := json.Marshal(api)
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 updateApi marshal: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayv2Namespace, apigwv2APIKey(ctx.AccountID, ctx.Region, apiID), updated); err != nil {
		return nil, fmt.Errorf("apigatewayv2 updateApi state.Put: %w", err)
	}
	return apigwJSONResponse(http.StatusOK, api)
}

// --- Route operations --------------------------------------------------------

func (p *APIGatewayV2Plugin) createRoute(ctx *RequestContext, req *AWSRequest, apiID string) (*AWSResponse, error) {
	var body struct {
		RouteKey          string `json:"RouteKey"`
		Target            string `json:"Target"`
		AuthorizationType string `json:"AuthorizationType"`
		AuthorizerID      string `json:"AuthorizerId"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "BadRequestException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	route := V2RouteState{
		RouteID:           generateAPIGatewayID(),
		RouteKey:          body.RouteKey,
		Target:            body.Target,
		AuthorizationType: body.AuthorizationType,
		AuthorizerID:      body.AuthorizerID,
		APIID:             apiID,
		AccountID:         ctx.AccountID,
		Region:            ctx.Region,
	}

	goCtx := context.Background()
	data, err := json.Marshal(route)
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 createRoute marshal: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayv2Namespace, apigwv2RouteKey(ctx.AccountID, ctx.Region, apiID, route.RouteID), data); err != nil {
		return nil, fmt.Errorf("apigatewayv2 createRoute state.Put: %w", err)
	}
	updateStringIndex(goCtx, p.state, apigatewayv2Namespace, apigwv2RouteIDsKey(ctx.AccountID, ctx.Region, apiID), route.RouteID)

	return apigwJSONResponse(http.StatusCreated, route)
}

func (p *APIGatewayV2Plugin) getRoute(ctx *RequestContext, apiID, routeID string) (*AWSResponse, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, apigatewayv2Namespace, apigwv2RouteKey(ctx.AccountID, ctx.Region, apiID, routeID))
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 getRoute state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Route not found: " + routeID, HTTPStatus: http.StatusNotFound}
	}
	var route V2RouteState
	if err := json.Unmarshal(data, &route); err != nil {
		return nil, fmt.Errorf("apigatewayv2 getRoute unmarshal: %w", err)
	}
	return apigwJSONResponse(http.StatusOK, route)
}

func (p *APIGatewayV2Plugin) getRoutes(ctx *RequestContext, apiID string) (*AWSResponse, error) {
	goCtx := context.Background()
	ids, err := loadStringIndex(goCtx, p.state, apigatewayv2Namespace, apigwv2RouteIDsKey(ctx.AccountID, ctx.Region, apiID))
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 getRoutes loadIndex: %w", err)
	}

	items := make([]V2RouteState, 0, len(ids))
	for _, id := range ids {
		data, getErr := p.state.Get(goCtx, apigatewayv2Namespace, apigwv2RouteKey(ctx.AccountID, ctx.Region, apiID, id))
		if getErr != nil || data == nil {
			continue
		}
		var route V2RouteState
		if json.Unmarshal(data, &route) == nil {
			items = append(items, route)
		}
	}

	type response struct {
		Items []V2RouteState `json:"Items"`
	}
	return apigwJSONResponse(http.StatusOK, response{Items: items})
}

func (p *APIGatewayV2Plugin) deleteRoute(ctx *RequestContext, apiID, routeID string) (*AWSResponse, error) {
	goCtx := context.Background()
	if err := p.state.Delete(goCtx, apigatewayv2Namespace, apigwv2RouteKey(ctx.AccountID, ctx.Region, apiID, routeID)); err != nil {
		return nil, fmt.Errorf("apigatewayv2 deleteRoute state.Delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, apigatewayv2Namespace, apigwv2RouteIDsKey(ctx.AccountID, ctx.Region, apiID), routeID)
	return apigwJSONResponse(http.StatusNoContent, struct{}{})
}

// --- Integration operations --------------------------------------------------

func (p *APIGatewayV2Plugin) createIntegration(ctx *RequestContext, req *AWSRequest, apiID string) (*AWSResponse, error) {
	var body struct {
		IntegrationType      string `json:"IntegrationType"`
		IntegrationURI       string `json:"IntegrationUri"`
		PayloadFormatVersion string `json:"PayloadFormatVersion"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &body)
	}

	integ := V2IntegrationState{
		IntegrationID:        generateAPIGatewayID(),
		IntegrationType:      body.IntegrationType,
		IntegrationURI:       body.IntegrationURI,
		PayloadFormatVersion: body.PayloadFormatVersion,
		APIID:                apiID,
		AccountID:            ctx.AccountID,
		Region:               ctx.Region,
	}

	goCtx := context.Background()
	data, err := json.Marshal(integ)
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 createIntegration marshal: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayv2Namespace, apigwv2IntegrationKey(ctx.AccountID, ctx.Region, apiID, integ.IntegrationID), data); err != nil {
		return nil, fmt.Errorf("apigatewayv2 createIntegration state.Put: %w", err)
	}
	updateStringIndex(goCtx, p.state, apigatewayv2Namespace, apigwv2IntegrationIDsKey(ctx.AccountID, ctx.Region, apiID), integ.IntegrationID)

	return apigwJSONResponse(http.StatusCreated, integ)
}

func (p *APIGatewayV2Plugin) getIntegration(ctx *RequestContext, apiID, intID string) (*AWSResponse, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, apigatewayv2Namespace, apigwv2IntegrationKey(ctx.AccountID, ctx.Region, apiID, intID))
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 getIntegration state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Integration not found: " + intID, HTTPStatus: http.StatusNotFound}
	}
	var integ V2IntegrationState
	if err := json.Unmarshal(data, &integ); err != nil {
		return nil, fmt.Errorf("apigatewayv2 getIntegration unmarshal: %w", err)
	}
	return apigwJSONResponse(http.StatusOK, integ)
}

func (p *APIGatewayV2Plugin) getIntegrations(ctx *RequestContext, apiID string) (*AWSResponse, error) {
	goCtx := context.Background()
	ids, err := loadStringIndex(goCtx, p.state, apigatewayv2Namespace, apigwv2IntegrationIDsKey(ctx.AccountID, ctx.Region, apiID))
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 getIntegrations loadIndex: %w", err)
	}

	items := make([]V2IntegrationState, 0, len(ids))
	for _, id := range ids {
		data, getErr := p.state.Get(goCtx, apigatewayv2Namespace, apigwv2IntegrationKey(ctx.AccountID, ctx.Region, apiID, id))
		if getErr != nil || data == nil {
			continue
		}
		var integ V2IntegrationState
		if json.Unmarshal(data, &integ) == nil {
			items = append(items, integ)
		}
	}

	type response struct {
		Items []V2IntegrationState `json:"Items"`
	}
	return apigwJSONResponse(http.StatusOK, response{Items: items})
}

func (p *APIGatewayV2Plugin) deleteIntegration(ctx *RequestContext, apiID, intID string) (*AWSResponse, error) {
	goCtx := context.Background()
	if err := p.state.Delete(goCtx, apigatewayv2Namespace, apigwv2IntegrationKey(ctx.AccountID, ctx.Region, apiID, intID)); err != nil {
		return nil, fmt.Errorf("apigatewayv2 deleteIntegration state.Delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, apigatewayv2Namespace, apigwv2IntegrationIDsKey(ctx.AccountID, ctx.Region, apiID), intID)
	return apigwJSONResponse(http.StatusNoContent, struct{}{})
}

// --- Stage operations --------------------------------------------------------

func (p *APIGatewayV2Plugin) createStageV2(ctx *RequestContext, req *AWSRequest, apiID string) (*AWSResponse, error) {
	var body struct {
		StageName      string            `json:"StageName"`
		DeploymentID   string            `json:"DeploymentId"`
		Description    string            `json:"Description"`
		StageVariables map[string]string `json:"StageVariables"`
		Tags           map[string]string `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "BadRequestException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.StageName == "" {
		return nil, &AWSError{Code: "BadRequestException", Message: "StageName is required", HTTPStatus: http.StatusBadRequest}
	}

	stage := V2StageState{
		StageName:      body.StageName,
		DeploymentID:   body.DeploymentID,
		Description:    body.Description,
		StageVariables: body.StageVariables,
		Tags:           body.Tags,
		CreatedDate:    p.tc.Now(),
		APIID:          apiID,
		AccountID:      ctx.AccountID,
		Region:         ctx.Region,
	}

	goCtx := context.Background()
	data, err := json.Marshal(stage)
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 createStage marshal: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayv2Namespace, apigwv2StageKey(ctx.AccountID, ctx.Region, apiID, stage.StageName), data); err != nil {
		return nil, fmt.Errorf("apigatewayv2 createStage state.Put: %w", err)
	}
	updateStringIndex(goCtx, p.state, apigatewayv2Namespace, apigwv2StageNamesKey(ctx.AccountID, ctx.Region, apiID), stage.StageName)

	return apigwJSONResponse(http.StatusCreated, stage)
}

func (p *APIGatewayV2Plugin) getStageV2(ctx *RequestContext, apiID, stageName string) (*AWSResponse, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, apigatewayv2Namespace, apigwv2StageKey(ctx.AccountID, ctx.Region, apiID, stageName))
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 getStage state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Stage not found: " + stageName, HTTPStatus: http.StatusNotFound}
	}
	var stage V2StageState
	if err := json.Unmarshal(data, &stage); err != nil {
		return nil, fmt.Errorf("apigatewayv2 getStage unmarshal: %w", err)
	}
	return apigwJSONResponse(http.StatusOK, stage)
}

func (p *APIGatewayV2Plugin) getStagesV2(ctx *RequestContext, apiID string) (*AWSResponse, error) {
	goCtx := context.Background()
	names, err := loadStringIndex(goCtx, p.state, apigatewayv2Namespace, apigwv2StageNamesKey(ctx.AccountID, ctx.Region, apiID))
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 getStages loadIndex: %w", err)
	}

	items := make([]V2StageState, 0, len(names))
	for _, name := range names {
		data, getErr := p.state.Get(goCtx, apigatewayv2Namespace, apigwv2StageKey(ctx.AccountID, ctx.Region, apiID, name))
		if getErr != nil || data == nil {
			continue
		}
		var stage V2StageState
		if json.Unmarshal(data, &stage) == nil {
			items = append(items, stage)
		}
	}

	type response struct {
		Items []V2StageState `json:"Items"`
	}
	return apigwJSONResponse(http.StatusOK, response{Items: items})
}

func (p *APIGatewayV2Plugin) deleteStageV2(ctx *RequestContext, apiID, stageName string) (*AWSResponse, error) {
	goCtx := context.Background()
	if err := p.state.Delete(goCtx, apigatewayv2Namespace, apigwv2StageKey(ctx.AccountID, ctx.Region, apiID, stageName)); err != nil {
		return nil, fmt.Errorf("apigatewayv2 deleteStage state.Delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, apigatewayv2Namespace, apigwv2StageNamesKey(ctx.AccountID, ctx.Region, apiID), stageName)
	return apigwJSONResponse(http.StatusNoContent, struct{}{})
}

// --- Authorizer operations ---------------------------------------------------

func (p *APIGatewayV2Plugin) createAuthorizerV2(ctx *RequestContext, req *AWSRequest, apiID string) (*AWSResponse, error) {
	var body struct {
		Name             string      `json:"Name"`
		AuthorizerType   string      `json:"AuthorizerType"`
		IdentitySource   []string    `json:"IdentitySource"`
		JwtConfiguration interface{} `json:"JwtConfiguration"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &body)
	}

	auth := V2AuthorizerState{
		AuthorizerID:     generateAPIGatewayID(),
		Name:             body.Name,
		AuthorizerType:   body.AuthorizerType,
		IdentitySource:   body.IdentitySource,
		JwtConfiguration: body.JwtConfiguration,
		APIID:            apiID,
		AccountID:        ctx.AccountID,
		Region:           ctx.Region,
	}

	goCtx := context.Background()
	data, err := json.Marshal(auth)
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 createAuthorizer marshal: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayv2Namespace, apigwv2AuthorizerKey(ctx.AccountID, ctx.Region, apiID, auth.AuthorizerID), data); err != nil {
		return nil, fmt.Errorf("apigatewayv2 createAuthorizer state.Put: %w", err)
	}
	updateStringIndex(goCtx, p.state, apigatewayv2Namespace, apigwv2AuthorizerIDsKey(ctx.AccountID, ctx.Region, apiID), auth.AuthorizerID)

	return apigwJSONResponse(http.StatusCreated, auth)
}

func (p *APIGatewayV2Plugin) getAuthorizerV2(ctx *RequestContext, apiID, authID string) (*AWSResponse, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, apigatewayv2Namespace, apigwv2AuthorizerKey(ctx.AccountID, ctx.Region, apiID, authID))
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 getAuthorizer state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Authorizer not found: " + authID, HTTPStatus: http.StatusNotFound}
	}
	var auth V2AuthorizerState
	if err := json.Unmarshal(data, &auth); err != nil {
		return nil, fmt.Errorf("apigatewayv2 getAuthorizer unmarshal: %w", err)
	}
	return apigwJSONResponse(http.StatusOK, auth)
}

func (p *APIGatewayV2Plugin) getAuthorizersV2(ctx *RequestContext, apiID string) (*AWSResponse, error) {
	goCtx := context.Background()
	ids, err := loadStringIndex(goCtx, p.state, apigatewayv2Namespace, apigwv2AuthorizerIDsKey(ctx.AccountID, ctx.Region, apiID))
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 getAuthorizers loadIndex: %w", err)
	}

	items := make([]V2AuthorizerState, 0, len(ids))
	for _, id := range ids {
		data, getErr := p.state.Get(goCtx, apigatewayv2Namespace, apigwv2AuthorizerKey(ctx.AccountID, ctx.Region, apiID, id))
		if getErr != nil || data == nil {
			continue
		}
		var auth V2AuthorizerState
		if json.Unmarshal(data, &auth) == nil {
			items = append(items, auth)
		}
	}

	type response struct {
		Items []V2AuthorizerState `json:"Items"`
	}
	return apigwJSONResponse(http.StatusOK, response{Items: items})
}

func (p *APIGatewayV2Plugin) deleteAuthorizerV2(ctx *RequestContext, apiID, authID string) (*AWSResponse, error) {
	goCtx := context.Background()
	if err := p.state.Delete(goCtx, apigatewayv2Namespace, apigwv2AuthorizerKey(ctx.AccountID, ctx.Region, apiID, authID)); err != nil {
		return nil, fmt.Errorf("apigatewayv2 deleteAuthorizer state.Delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, apigatewayv2Namespace, apigwv2AuthorizerIDsKey(ctx.AccountID, ctx.Region, apiID), authID)
	return apigwJSONResponse(http.StatusNoContent, struct{}{})
}

// --- Deployment operations ---------------------------------------------------

func (p *APIGatewayV2Plugin) createDeploymentV2(ctx *RequestContext, req *AWSRequest, apiID string) (*AWSResponse, error) {
	var body struct {
		Description string `json:"Description"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &body)
	}

	dep := V2DeploymentState{
		DeploymentID:     generateAPIGatewayID(),
		DeploymentStatus: "DEPLOYED",
		Description:      body.Description,
		CreatedDate:      p.tc.Now(),
		APIID:            apiID,
		AccountID:        ctx.AccountID,
		Region:           ctx.Region,
	}

	goCtx := context.Background()
	data, err := json.Marshal(dep)
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 createDeployment marshal: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayv2Namespace, apigwv2DeploymentKey(ctx.AccountID, ctx.Region, apiID, dep.DeploymentID), data); err != nil {
		return nil, fmt.Errorf("apigatewayv2 createDeployment state.Put: %w", err)
	}
	updateStringIndex(goCtx, p.state, apigatewayv2Namespace, apigwv2DeploymentIDsKey(ctx.AccountID, ctx.Region, apiID), dep.DeploymentID)

	return apigwJSONResponse(http.StatusCreated, dep)
}

func (p *APIGatewayV2Plugin) getDeploymentV2(ctx *RequestContext, apiID, depID string) (*AWSResponse, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, apigatewayv2Namespace, apigwv2DeploymentKey(ctx.AccountID, ctx.Region, apiID, depID))
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 getDeployment state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Deployment not found: " + depID, HTTPStatus: http.StatusNotFound}
	}
	var dep V2DeploymentState
	if err := json.Unmarshal(data, &dep); err != nil {
		return nil, fmt.Errorf("apigatewayv2 getDeployment unmarshal: %w", err)
	}
	return apigwJSONResponse(http.StatusOK, dep)
}

// --- Domain name and API mapping operations ----------------------------------

func (p *APIGatewayV2Plugin) createDomainNameV2(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		DomainName string `json:"DomainName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "BadRequestException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	dn := V2DomainNameState{
		DomainName:         body.DomainName,
		RegionalDomainName: body.DomainName + ".regional.execute-api." + ctx.Region + ".amazonaws.com",
		AccountID:          ctx.AccountID,
		Region:             ctx.Region,
	}

	goCtx := context.Background()
	data, err := json.Marshal(dn)
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 createDomainName marshal: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayv2Namespace, apigwv2DomainNameKey(ctx.AccountID, ctx.Region, body.DomainName), data); err != nil {
		return nil, fmt.Errorf("apigatewayv2 createDomainName state.Put: %w", err)
	}

	return apigwJSONResponse(http.StatusCreated, dn)
}

func (p *APIGatewayV2Plugin) getDomainNameV2(ctx *RequestContext, name string) (*AWSResponse, error) {
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, apigatewayv2Namespace, apigwv2DomainNameKey(ctx.AccountID, ctx.Region, name))
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 getDomainName state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Domain name not found: " + name, HTTPStatus: http.StatusNotFound}
	}
	var dn V2DomainNameState
	if err := json.Unmarshal(data, &dn); err != nil {
		return nil, fmt.Errorf("apigatewayv2 getDomainName unmarshal: %w", err)
	}
	return apigwJSONResponse(http.StatusOK, dn)
}

func (p *APIGatewayV2Plugin) createAPIMapping(ctx *RequestContext, req *AWSRequest, domainName string) (*AWSResponse, error) {
	var body struct {
		APIID         string `json:"ApiId"`
		Stage         string `json:"Stage"`
		APIMappingKey string `json:"ApiMappingKey"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "BadRequestException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	mappingID := generateAPIGatewayID()
	type apiMappingState struct {
		APIMappingID  string `json:"ApiMappingId"`
		APIID         string `json:"ApiId"`
		Stage         string `json:"Stage"`
		APIMappingKey string `json:"ApiMappingKey,omitempty"`
		DomainName    string `json:"DomainName"`
	}
	mapping := apiMappingState{
		APIMappingID:  mappingID,
		APIID:         body.APIID,
		Stage:         body.Stage,
		APIMappingKey: body.APIMappingKey,
		DomainName:    domainName,
	}

	goCtx := context.Background()
	data, err := json.Marshal(mapping)
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 createAPIMapping marshal: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayv2Namespace, apigwv2APIMappingKey(ctx.AccountID, ctx.Region, domainName, mappingID), data); err != nil {
		return nil, fmt.Errorf("apigatewayv2 createAPIMapping state.Put: %w", err)
	}

	return apigwJSONResponse(http.StatusCreated, mapping)
}
