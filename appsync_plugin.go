package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// AppSyncPlugin emulates the AWS AppSync GraphQL service.
// It supports creating and managing GraphQL APIs, data sources, resolvers,
// and pipeline functions. The GraphQL execution endpoint returns a stub response.
type AppSyncPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "appsync".
func (p *AppSyncPlugin) Name() string { return appSyncNamespace }

// Initialize sets up the AppSyncPlugin with the provided configuration.
func (p *AppSyncPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for AppSyncPlugin.
func (p *AppSyncPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an AppSync REST/JSON request to the appropriate handler.
func (p *AppSyncPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op, apiID, segment, resourceID := parseAppSyncOperation(req.Operation, req.Path)
	switch op {
	case "CreateGraphqlApi":
		return p.createGraphqlAPI(ctx, req)
	case "ListGraphqlApis":
		return p.listGraphqlAPIs(ctx, req)
	case "GetGraphqlApi":
		return p.getGraphqlAPI(ctx, req, apiID)
	case "UpdateGraphqlApi":
		return p.updateGraphqlAPI(ctx, req, apiID)
	case "DeleteGraphqlApi":
		return p.deleteGraphqlAPI(ctx, req, apiID)
	case "CreateDataSource":
		return p.createDataSource(ctx, req, apiID)
	case "ListDataSources":
		return p.listDataSources(ctx, req, apiID)
	case "GetDataSource":
		return p.getDataSource(ctx, req, apiID, resourceID)
	case "UpdateDataSource":
		return p.updateDataSource(ctx, req, apiID, resourceID)
	case "DeleteDataSource":
		return p.deleteDataSource(ctx, req, apiID, resourceID)
	case "CreateResolver":
		return p.createResolver(ctx, req, apiID, segment)
	case "ListResolvers":
		return p.listResolvers(ctx, req, apiID, segment)
	case "GetResolver":
		return p.getResolver(ctx, req, apiID, segment, resourceID)
	case "UpdateResolver":
		return p.updateResolver(ctx, req, apiID, segment, resourceID)
	case "DeleteResolver":
		return p.deleteResolver(ctx, req, apiID, segment, resourceID)
	case "CreateFunction":
		return p.createFunction(ctx, req, apiID)
	case "ListFunctions":
		return p.listFunctions(ctx, req, apiID)
	case "GetFunction":
		return p.getFunction(ctx, req, apiID, resourceID)
	case "DeleteFunction":
		return p.deleteFunction(ctx, req, apiID, resourceID)
	case "StartSchemaCreation":
		return p.startSchemaCreation(ctx, req, apiID)
	case "GetIntrospectionSchema":
		return p.getIntrospectionSchema(ctx, req, apiID)
	case "ExecuteGraphQL":
		return p.executeGraphQL(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "AppSyncPlugin: unsupported operation " + req.Path,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// appsyncJSONResponse builds a successful AWSResponse with a JSON body.
func appsyncJSONResponse(status int, body any) (*AWSResponse, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("appsync marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       data,
	}, nil
}

// --- GraphQL API operations ---

func (p *AppSyncPlugin) createGraphqlAPI(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name               string            `json:"name"`
		AuthenticationType string            `json:"authenticationType"`
		Tags               map[string]string `json:"tags"`
		XrayEnabled        bool              `json:"xrayEnabled"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "BadRequestException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	if input.Name == "" {
		return nil, &AWSError{Code: "BadRequestException", Message: "name is required", HTTPStatus: http.StatusBadRequest}
	}
	if input.AuthenticationType == "" {
		input.AuthenticationType = "API_KEY"
	}

	apiID := generateAppSyncAPIID()
	acct := reqCtx.AccountID
	region := reqCtx.Region
	arn := fmt.Sprintf("arn:aws:appsync:%s:%s:apis/%s", region, acct, apiID)

	api := AppSyncGraphQLApi{
		APIID:              apiID,
		Name:               input.Name,
		AuthenticationType: input.AuthenticationType,
		APIARN:             arn,
		Tags:               input.Tags,
		XrayEnabled:        input.XrayEnabled,
		URIS: map[string]string{
			"GRAPHQL":  fmt.Sprintf("https://%s.appsync-api.%s.amazonaws.com/graphql", apiID, region),
			"REALTIME": fmt.Sprintf("wss://%s.appsync-realtime-api.%s.amazonaws.com/graphql", apiID, region),
		},
		Region:    region,
		AccountID: acct,
	}

	data, _ := json.Marshal(api)
	goCtx := context.Background()
	if err := p.state.Put(goCtx, appSyncNamespace, appSyncAPIKey(acct, region, apiID), data); err != nil {
		return nil, fmt.Errorf("put appsync api: %w", err)
	}
	updateStringIndex(goCtx, p.state, appSyncNamespace, appSyncAPIIDsKey(acct, region), apiID)

	return appsyncJSONResponse(http.StatusOK, map[string]any{"graphqlApi": api})
}

func (p *AppSyncPlugin) listGraphqlAPIs(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	acct := reqCtx.AccountID
	region := reqCtx.Region
	ids, _ := loadStringIndex(goCtx, p.state, appSyncNamespace, appSyncAPIIDsKey(acct, region))
	apis := make([]AppSyncGraphQLApi, 0, len(ids))
	for _, id := range ids {
		raw, err := p.state.Get(goCtx, appSyncNamespace, appSyncAPIKey(acct, region, id))
		if err != nil || raw == nil {
			continue
		}
		var api AppSyncGraphQLApi
		if err2 := json.Unmarshal(raw, &api); err2 == nil {
			apis = append(apis, api)
		}
	}
	return appsyncJSONResponse(http.StatusOK, map[string]any{"graphqlApis": apis})
}

func (p *AppSyncPlugin) getGraphqlAPI(reqCtx *RequestContext, req *AWSRequest, apiID string) (*AWSResponse, error) {
	api, err := p.loadAPI(reqCtx.AccountID, reqCtx.Region, apiID)
	if err != nil {
		return nil, err
	}
	return appsyncJSONResponse(http.StatusOK, map[string]any{"graphqlApi": api})
}

func (p *AppSyncPlugin) updateGraphqlAPI(reqCtx *RequestContext, req *AWSRequest, apiID string) (*AWSResponse, error) {
	api, err := p.loadAPI(reqCtx.AccountID, reqCtx.Region, apiID)
	if err != nil {
		return nil, err
	}
	var input struct {
		Name               string `json:"name"`
		AuthenticationType string `json:"authenticationType"`
	}
	if err2 := json.Unmarshal(req.Body, &input); err2 == nil {
		if input.Name != "" {
			api.Name = input.Name
		}
		if input.AuthenticationType != "" {
			api.AuthenticationType = input.AuthenticationType
		}
	}
	data, _ := json.Marshal(api)
	goCtx := context.Background()
	if err3 := p.state.Put(goCtx, appSyncNamespace, appSyncAPIKey(reqCtx.AccountID, reqCtx.Region, apiID), data); err3 != nil {
		return nil, fmt.Errorf("update appsync api: %w", err3)
	}
	return appsyncJSONResponse(http.StatusOK, map[string]any{"graphqlApi": api})
}

func (p *AppSyncPlugin) deleteGraphqlAPI(reqCtx *RequestContext, req *AWSRequest, apiID string) (*AWSResponse, error) {
	if _, err := p.loadAPI(reqCtx.AccountID, reqCtx.Region, apiID); err != nil {
		return nil, err
	}
	goCtx := context.Background()
	acct, region := reqCtx.AccountID, reqCtx.Region
	if err := p.state.Delete(goCtx, appSyncNamespace, appSyncAPIKey(acct, region, apiID)); err != nil {
		return nil, fmt.Errorf("delete appsync api: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, appSyncNamespace, appSyncAPIIDsKey(acct, region), apiID)
	return &AWSResponse{StatusCode: http.StatusNoContent}, nil
}

// --- DataSource operations ---

func (p *AppSyncPlugin) createDataSource(reqCtx *RequestContext, req *AWSRequest, apiID string) (*AWSResponse, error) {
	if _, err := p.loadAPI(reqCtx.AccountID, reqCtx.Region, apiID); err != nil {
		return nil, err
	}
	var input struct {
		Name           string `json:"name"`
		Type           string `json:"type"`
		Description    string `json:"description"`
		ServiceRoleARN string `json:"serviceRoleArn"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil || input.Name == "" || input.Type == "" {
		return nil, &AWSError{Code: "BadRequestException", Message: "name and type are required", HTTPStatus: http.StatusBadRequest}
	}
	acct, region := reqCtx.AccountID, reqCtx.Region
	ds := AppSyncDataSource{
		APIID:          apiID,
		Name:           input.Name,
		Type:           input.Type,
		Description:    input.Description,
		ServiceRoleARN: input.ServiceRoleARN,
		DataSourceARN:  fmt.Sprintf("arn:aws:appsync:%s:%s:apis/%s/datasources/%s", region, acct, apiID, input.Name),
	}
	data, _ := json.Marshal(ds)
	goCtx := context.Background()
	if err := p.state.Put(goCtx, appSyncNamespace, appSyncDataSourceKey(acct, region, apiID, input.Name), data); err != nil {
		return nil, fmt.Errorf("put appsync datasource: %w", err)
	}
	updateStringIndex(goCtx, p.state, appSyncNamespace, appSyncDataSourceNamesKey(acct, region, apiID), input.Name)
	return appsyncJSONResponse(http.StatusOK, map[string]any{"dataSource": ds})
}

func (p *AppSyncPlugin) listDataSources(reqCtx *RequestContext, req *AWSRequest, apiID string) (*AWSResponse, error) {
	goCtx := context.Background()
	acct, region := reqCtx.AccountID, reqCtx.Region
	names, _ := loadStringIndex(goCtx, p.state, appSyncNamespace, appSyncDataSourceNamesKey(acct, region, apiID))
	dsList := make([]AppSyncDataSource, 0, len(names))
	for _, name := range names {
		raw, err := p.state.Get(goCtx, appSyncNamespace, appSyncDataSourceKey(acct, region, apiID, name))
		if err != nil || raw == nil {
			continue
		}
		var ds AppSyncDataSource
		if err2 := json.Unmarshal(raw, &ds); err2 == nil {
			dsList = append(dsList, ds)
		}
	}
	return appsyncJSONResponse(http.StatusOK, map[string]any{"dataSources": dsList})
}

func (p *AppSyncPlugin) getDataSource(reqCtx *RequestContext, req *AWSRequest, apiID, name string) (*AWSResponse, error) {
	ds, err := p.loadDataSource(reqCtx.AccountID, reqCtx.Region, apiID, name)
	if err != nil {
		return nil, err
	}
	return appsyncJSONResponse(http.StatusOK, map[string]any{"dataSource": ds})
}

func (p *AppSyncPlugin) updateDataSource(reqCtx *RequestContext, req *AWSRequest, apiID, name string) (*AWSResponse, error) {
	ds, err := p.loadDataSource(reqCtx.AccountID, reqCtx.Region, apiID, name)
	if err != nil {
		return nil, err
	}
	var input struct {
		Type        string `json:"type"`
		Description string `json:"description"`
	}
	if err2 := json.Unmarshal(req.Body, &input); err2 == nil {
		if input.Type != "" {
			ds.Type = input.Type
		}
		if input.Description != "" {
			ds.Description = input.Description
		}
	}
	data, _ := json.Marshal(ds)
	goCtx := context.Background()
	if err3 := p.state.Put(goCtx, appSyncNamespace, appSyncDataSourceKey(reqCtx.AccountID, reqCtx.Region, apiID, name), data); err3 != nil {
		return nil, fmt.Errorf("update appsync datasource: %w", err3)
	}
	return appsyncJSONResponse(http.StatusOK, map[string]any{"dataSource": ds})
}

func (p *AppSyncPlugin) deleteDataSource(reqCtx *RequestContext, req *AWSRequest, apiID, name string) (*AWSResponse, error) {
	if _, err := p.loadDataSource(reqCtx.AccountID, reqCtx.Region, apiID, name); err != nil {
		return nil, err
	}
	goCtx := context.Background()
	acct, region := reqCtx.AccountID, reqCtx.Region
	if err := p.state.Delete(goCtx, appSyncNamespace, appSyncDataSourceKey(acct, region, apiID, name)); err != nil {
		return nil, fmt.Errorf("delete appsync datasource: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, appSyncNamespace, appSyncDataSourceNamesKey(acct, region, apiID), name)
	return &AWSResponse{StatusCode: http.StatusNoContent}, nil
}

// --- Resolver operations ---

func (p *AppSyncPlugin) createResolver(reqCtx *RequestContext, req *AWSRequest, apiID, typeName string) (*AWSResponse, error) {
	if _, err := p.loadAPI(reqCtx.AccountID, reqCtx.Region, apiID); err != nil {
		return nil, err
	}
	var input struct {
		FieldName               string `json:"fieldName"`
		DataSourceName          string `json:"dataSourceName"`
		Kind                    string `json:"kind"`
		RequestMappingTemplate  string `json:"requestMappingTemplate"`
		ResponseMappingTemplate string `json:"responseMappingTemplate"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil || input.FieldName == "" {
		return nil, &AWSError{Code: "BadRequestException", Message: "fieldName is required", HTTPStatus: http.StatusBadRequest}
	}
	if input.Kind == "" {
		input.Kind = "UNIT"
	}
	acct, region := reqCtx.AccountID, reqCtx.Region
	res := AppSyncResolver{
		APIID:                   apiID,
		TypeName:                typeName,
		FieldName:               input.FieldName,
		DataSourceName:          input.DataSourceName,
		Kind:                    input.Kind,
		RequestMappingTemplate:  input.RequestMappingTemplate,
		ResponseMappingTemplate: input.ResponseMappingTemplate,
		ResolverARN:             fmt.Sprintf("arn:aws:appsync:%s:%s:apis/%s/types/%s/resolvers/%s", region, acct, apiID, typeName, input.FieldName),
	}
	data, _ := json.Marshal(res)
	goCtx := context.Background()
	if err := p.state.Put(goCtx, appSyncNamespace, appSyncResolverKey(acct, region, apiID, typeName, input.FieldName), data); err != nil {
		return nil, fmt.Errorf("put appsync resolver: %w", err)
	}
	updateStringIndex(goCtx, p.state, appSyncNamespace, appSyncResolverKeysKey(acct, region, apiID), typeName+"/"+input.FieldName)
	return appsyncJSONResponse(http.StatusOK, map[string]any{"resolver": res})
}

func (p *AppSyncPlugin) listResolvers(reqCtx *RequestContext, req *AWSRequest, apiID, typeName string) (*AWSResponse, error) {
	goCtx := context.Background()
	acct, region := reqCtx.AccountID, reqCtx.Region
	keys, _ := loadStringIndex(goCtx, p.state, appSyncNamespace, appSyncResolverKeysKey(acct, region, apiID))
	resolvers := make([]AppSyncResolver, 0)
	for _, k := range keys {
		parts := strings.SplitN(k, "/", 2)
		if len(parts) != 2 || parts[0] != typeName {
			continue
		}
		raw, err := p.state.Get(goCtx, appSyncNamespace, appSyncResolverKey(acct, region, apiID, parts[0], parts[1]))
		if err != nil || raw == nil {
			continue
		}
		var res AppSyncResolver
		if err2 := json.Unmarshal(raw, &res); err2 == nil {
			resolvers = append(resolvers, res)
		}
	}
	return appsyncJSONResponse(http.StatusOK, map[string]any{"resolvers": resolvers})
}

func (p *AppSyncPlugin) getResolver(reqCtx *RequestContext, req *AWSRequest, apiID, typeName, fieldName string) (*AWSResponse, error) {
	res, err := p.loadResolver(reqCtx.AccountID, reqCtx.Region, apiID, typeName, fieldName)
	if err != nil {
		return nil, err
	}
	return appsyncJSONResponse(http.StatusOK, map[string]any{"resolver": res})
}

func (p *AppSyncPlugin) updateResolver(reqCtx *RequestContext, req *AWSRequest, apiID, typeName, fieldName string) (*AWSResponse, error) {
	res, err := p.loadResolver(reqCtx.AccountID, reqCtx.Region, apiID, typeName, fieldName)
	if err != nil {
		return nil, err
	}
	var input struct {
		DataSourceName          string `json:"dataSourceName"`
		RequestMappingTemplate  string `json:"requestMappingTemplate"`
		ResponseMappingTemplate string `json:"responseMappingTemplate"`
	}
	if err2 := json.Unmarshal(req.Body, &input); err2 == nil {
		if input.DataSourceName != "" {
			res.DataSourceName = input.DataSourceName
		}
		if input.RequestMappingTemplate != "" {
			res.RequestMappingTemplate = input.RequestMappingTemplate
		}
		if input.ResponseMappingTemplate != "" {
			res.ResponseMappingTemplate = input.ResponseMappingTemplate
		}
	}
	data, _ := json.Marshal(res)
	goCtx := context.Background()
	if err3 := p.state.Put(goCtx, appSyncNamespace, appSyncResolverKey(reqCtx.AccountID, reqCtx.Region, apiID, typeName, fieldName), data); err3 != nil {
		return nil, fmt.Errorf("update appsync resolver: %w", err3)
	}
	return appsyncJSONResponse(http.StatusOK, map[string]any{"resolver": res})
}

func (p *AppSyncPlugin) deleteResolver(reqCtx *RequestContext, req *AWSRequest, apiID, typeName, fieldName string) (*AWSResponse, error) {
	if _, err := p.loadResolver(reqCtx.AccountID, reqCtx.Region, apiID, typeName, fieldName); err != nil {
		return nil, err
	}
	goCtx := context.Background()
	acct, region := reqCtx.AccountID, reqCtx.Region
	if err := p.state.Delete(goCtx, appSyncNamespace, appSyncResolverKey(acct, region, apiID, typeName, fieldName)); err != nil {
		return nil, fmt.Errorf("delete appsync resolver: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, appSyncNamespace, appSyncResolverKeysKey(acct, region, apiID), typeName+"/"+fieldName)
	return &AWSResponse{StatusCode: http.StatusNoContent}, nil
}

// --- Function operations ---

func (p *AppSyncPlugin) createFunction(reqCtx *RequestContext, req *AWSRequest, apiID string) (*AWSResponse, error) {
	if _, err := p.loadAPI(reqCtx.AccountID, reqCtx.Region, apiID); err != nil {
		return nil, err
	}
	var input struct {
		Name           string `json:"name"`
		DataSourceName string `json:"dataSourceName"`
		Description    string `json:"description"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil || input.Name == "" {
		return nil, &AWSError{Code: "BadRequestException", Message: "name is required", HTTPStatus: http.StatusBadRequest}
	}
	acct, region := reqCtx.AccountID, reqCtx.Region
	funcID := generateAppSyncFunctionID()
	fn := AppSyncFunction{
		APIID:          apiID,
		FunctionID:     funcID,
		Name:           input.Name,
		DataSourceName: input.DataSourceName,
		Description:    input.Description,
		FunctionARN:    fmt.Sprintf("arn:aws:appsync:%s:%s:apis/%s/functions/%s", region, acct, apiID, funcID),
	}
	data, _ := json.Marshal(fn)
	goCtx := context.Background()
	if err := p.state.Put(goCtx, appSyncNamespace, appSyncFunctionKey(acct, region, apiID, funcID), data); err != nil {
		return nil, fmt.Errorf("put appsync function: %w", err)
	}
	updateStringIndex(goCtx, p.state, appSyncNamespace, appSyncFunctionIDsKey(acct, region, apiID), funcID)
	return appsyncJSONResponse(http.StatusOK, map[string]any{"functionConfiguration": fn})
}

func (p *AppSyncPlugin) listFunctions(reqCtx *RequestContext, req *AWSRequest, apiID string) (*AWSResponse, error) {
	goCtx := context.Background()
	acct, region := reqCtx.AccountID, reqCtx.Region
	ids, _ := loadStringIndex(goCtx, p.state, appSyncNamespace, appSyncFunctionIDsKey(acct, region, apiID))
	fns := make([]AppSyncFunction, 0, len(ids))
	for _, id := range ids {
		raw, err := p.state.Get(goCtx, appSyncNamespace, appSyncFunctionKey(acct, region, apiID, id))
		if err != nil || raw == nil {
			continue
		}
		var fn AppSyncFunction
		if err2 := json.Unmarshal(raw, &fn); err2 == nil {
			fns = append(fns, fn)
		}
	}
	return appsyncJSONResponse(http.StatusOK, map[string]any{"functions": fns})
}

func (p *AppSyncPlugin) getFunction(reqCtx *RequestContext, req *AWSRequest, apiID, funcID string) (*AWSResponse, error) {
	fn, err := p.loadFunction(reqCtx.AccountID, reqCtx.Region, apiID, funcID)
	if err != nil {
		return nil, err
	}
	return appsyncJSONResponse(http.StatusOK, map[string]any{"functionConfiguration": fn})
}

func (p *AppSyncPlugin) deleteFunction(reqCtx *RequestContext, req *AWSRequest, apiID, funcID string) (*AWSResponse, error) {
	if _, err := p.loadFunction(reqCtx.AccountID, reqCtx.Region, apiID, funcID); err != nil {
		return nil, err
	}
	goCtx := context.Background()
	acct, region := reqCtx.AccountID, reqCtx.Region
	if err := p.state.Delete(goCtx, appSyncNamespace, appSyncFunctionKey(acct, region, apiID, funcID)); err != nil {
		return nil, fmt.Errorf("delete appsync function: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, appSyncNamespace, appSyncFunctionIDsKey(acct, region, apiID), funcID)
	return &AWSResponse{StatusCode: http.StatusNoContent}, nil
}

// --- Schema operations ---

func (p *AppSyncPlugin) startSchemaCreation(reqCtx *RequestContext, req *AWSRequest, apiID string) (*AWSResponse, error) {
	if _, err := p.loadAPI(reqCtx.AccountID, reqCtx.Region, apiID); err != nil {
		return nil, err
	}
	var input struct {
		Definition string `json:"definition"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "BadRequestException", Message: "invalid JSON", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	data, _ := json.Marshal(input.Definition)
	if err := p.state.Put(goCtx, appSyncNamespace, appSyncSchemaKey(reqCtx.AccountID, reqCtx.Region, apiID), data); err != nil {
		return nil, fmt.Errorf("put appsync schema: %w", err)
	}
	return appsyncJSONResponse(http.StatusOK, map[string]any{"status": "PROCESSING"})
}

func (p *AppSyncPlugin) getIntrospectionSchema(_ *RequestContext, _ *AWSRequest, _ string) (*AWSResponse, error) {
	return appsyncJSONResponse(http.StatusOK, map[string]any{
		"schema": "type Query { placeholder: String }",
	})
}

// --- GraphQL execution ---

func (p *AppSyncPlugin) executeGraphQL(_ *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	return appsyncJSONResponse(http.StatusOK, map[string]any{
		"data":   map[string]any{},
		"errors": nil,
	})
}

// --- Load helpers ---

func (p *AppSyncPlugin) loadAPI(acct, region, apiID string) (AppSyncGraphQLApi, error) {
	raw, err := p.state.Get(context.Background(), appSyncNamespace, appSyncAPIKey(acct, region, apiID))
	if err != nil || raw == nil {
		return AppSyncGraphQLApi{}, &AWSError{
			Code:       "NotFoundException",
			Message:    "GraphQL API not found: " + apiID,
			HTTPStatus: http.StatusNotFound,
		}
	}
	var api AppSyncGraphQLApi
	if err2 := json.Unmarshal(raw, &api); err2 != nil {
		return AppSyncGraphQLApi{}, fmt.Errorf("unmarshal appsync api: %w", err2)
	}
	return api, nil
}

func (p *AppSyncPlugin) loadDataSource(acct, region, apiID, name string) (AppSyncDataSource, error) {
	raw, err := p.state.Get(context.Background(), appSyncNamespace, appSyncDataSourceKey(acct, region, apiID, name))
	if err != nil || raw == nil {
		return AppSyncDataSource{}, &AWSError{
			Code:       "NotFoundException",
			Message:    "DataSource not found: " + name,
			HTTPStatus: http.StatusNotFound,
		}
	}
	var ds AppSyncDataSource
	if err2 := json.Unmarshal(raw, &ds); err2 != nil {
		return AppSyncDataSource{}, fmt.Errorf("unmarshal appsync datasource: %w", err2)
	}
	return ds, nil
}

func (p *AppSyncPlugin) loadResolver(acct, region, apiID, typeName, fieldName string) (AppSyncResolver, error) {
	raw, err := p.state.Get(context.Background(), appSyncNamespace, appSyncResolverKey(acct, region, apiID, typeName, fieldName))
	if err != nil || raw == nil {
		return AppSyncResolver{}, &AWSError{
			Code:       "NotFoundException",
			Message:    fmt.Sprintf("Resolver not found: %s.%s", typeName, fieldName),
			HTTPStatus: http.StatusNotFound,
		}
	}
	var res AppSyncResolver
	if err2 := json.Unmarshal(raw, &res); err2 != nil {
		return AppSyncResolver{}, fmt.Errorf("unmarshal appsync resolver: %w", err2)
	}
	return res, nil
}

func (p *AppSyncPlugin) loadFunction(acct, region, apiID, funcID string) (AppSyncFunction, error) {
	raw, err := p.state.Get(context.Background(), appSyncNamespace, appSyncFunctionKey(acct, region, apiID, funcID))
	if err != nil || raw == nil {
		return AppSyncFunction{}, &AWSError{
			Code:       "NotFoundException",
			Message:    "Function not found: " + funcID,
			HTTPStatus: http.StatusNotFound,
		}
	}
	var fn AppSyncFunction
	if err2 := json.Unmarshal(raw, &fn); err2 != nil {
		return AppSyncFunction{}, fmt.Errorf("unmarshal appsync function: %w", err2)
	}
	return fn, nil
}
