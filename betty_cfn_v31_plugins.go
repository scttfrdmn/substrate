package substrate

import (
	"context"
	"encoding/json"
	"fmt"
)

// ----- v0.31.0 — AppSync ------------------------------------------------------

// deployAppSyncGraphQLApi creates an AppSync GraphQL API for the given CFN resource.
// The Ref value is the API ID.
func (d *StackDeployer) deployAppSyncGraphQLApi(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)
	authType := resolveStringProp(props, "AuthenticationType", "API_KEY", cctx)

	body := map[string]interface{}{
		"name":               name,
		"authenticationType": authType,
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal appsync graphqlapi body: %w", err)
	}

	req := &AWSRequest{
		Service:   "appsync",
		Operation: "POST",
		Path:      "/v1/apis",
		Body:      bodyBytes,
		Headers:   map[string]string{"Content-Type": "application/json"},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID: logicalID,
		Type:      "AWS::AppSync::GraphQLApi",
		Metadata:  make(map[string]interface{}),
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			GraphQLAPI struct {
				APIID  string `json:"apiId"`
				APIARN string `json:"apiArn"`
			} `json:"graphqlApi"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil {
			dr.PhysicalID = result.GraphQLAPI.APIID
			dr.ARN = result.GraphQLAPI.APIARN
		}
	}
	return dr, cost, nil
}

// deployAppSyncDataSource creates an AppSync data source for the given CFN resource.
// The Ref value is the data source name.
func (d *StackDeployer) deployAppSyncDataSource(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	apiID := resolveStringProp(props, "ApiId", "", cctx)
	if apiID == "" {
		return DeployedResource{LogicalID: logicalID, Type: "AWS::AppSync::DataSource", Error: "ApiId is required"}, 0, nil
	}
	name := resolveStringProp(props, "Name", logicalID, cctx)
	dsType := resolveStringProp(props, "Type", "NONE", cctx)

	body := map[string]interface{}{
		"name": name,
		"type": dsType,
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal appsync datasource body: %w", err)
	}

	req := &AWSRequest{
		Service:   "appsync",
		Operation: "POST",
		Path:      "/v1/apis/" + apiID + "/DataSources",
		Body:      bodyBytes,
		Headers:   map[string]string{"Content-Type": "application/json"},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::AppSync::DataSource",
		PhysicalID: name,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployAppSyncResolver creates an AppSync resolver for the given CFN resource.
// The Ref value is the resolver ARN.
func (d *StackDeployer) deployAppSyncResolver(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	apiID := resolveStringProp(props, "ApiId", "", cctx)
	typeName := resolveStringProp(props, "TypeName", "", cctx)
	fieldName := resolveStringProp(props, "FieldName", "", cctx)
	if apiID == "" || typeName == "" || fieldName == "" {
		return DeployedResource{LogicalID: logicalID, Type: "AWS::AppSync::Resolver", Error: "ApiId, TypeName, and FieldName are required"}, 0, nil
	}

	body := map[string]interface{}{
		"fieldName":      fieldName,
		"dataSourceName": resolveStringProp(props, "DataSourceName", "", cctx),
		"kind":           resolveStringProp(props, "Kind", "UNIT", cctx),
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal appsync resolver body: %w", err)
	}

	req := &AWSRequest{
		Service:   "appsync",
		Operation: "POST",
		Path:      fmt.Sprintf("/v1/apis/%s/types/%s/resolvers", apiID, typeName),
		Body:      bodyBytes,
		Headers:   map[string]string{"Content-Type": "application/json"},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::AppSync::Resolver",
		PhysicalID: fmt.Sprintf("%s.%s", typeName, fieldName),
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployAppSyncFunction creates an AppSync pipeline function for the given CFN resource.
// The Ref value is the function ID.
func (d *StackDeployer) deployAppSyncFunction(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	apiID := resolveStringProp(props, "ApiId", "", cctx)
	if apiID == "" {
		return DeployedResource{LogicalID: logicalID, Type: "AWS::AppSync::FunctionConfiguration", Error: "ApiId is required"}, 0, nil
	}
	name := resolveStringProp(props, "Name", logicalID, cctx)
	dsName := resolveStringProp(props, "DataSourceName", "", cctx)

	body := map[string]interface{}{
		"name":           name,
		"dataSourceName": dsName,
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal appsync function body: %w", err)
	}

	req := &AWSRequest{
		Service:   "appsync",
		Operation: "POST",
		Path:      "/v1/apis/" + apiID + "/functions",
		Body:      bodyBytes,
		Headers:   map[string]string{"Content-Type": "application/json"},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID: logicalID,
		Type:      "AWS::AppSync::FunctionConfiguration",
		Metadata:  make(map[string]interface{}),
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			FunctionConfiguration struct {
				FunctionID string `json:"functionId"`
			} `json:"functionConfiguration"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil {
			dr.PhysicalID = result.FunctionConfiguration.FunctionID
		}
	}
	return dr, cost, nil
}
