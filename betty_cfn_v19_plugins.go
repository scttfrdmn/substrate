package substrate

// betty_cfn_v19_plugins.go contains StackDeployer.deployResource helpers for
// plugins added in v0.19.0 (ACM, API Gateway v1, API Gateway v2). The functions
// in this file follow the same pattern as the deploy helpers in betty_cfn.go.

import (
	"context"
	"encoding/json"
	"fmt"
)

// ----- v0.19.0 — ACM -------------------------------------------------------

// deployACMCertificate requests an ACM certificate stub for the given CFN resource.
func (d *StackDeployer) deployACMCertificate(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	domain := resolveStringProp(props, "DomainName", logicalID, cctx)

	body := map[string]interface{}{
		"DomainName": domain,
	}
	if sans, ok := props["SubjectAlternativeNames"]; ok {
		body["SubjectAlternativeNames"] = sans
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal acm body: %w", err)
	}

	req := &AWSRequest{
		Service:   "acm",
		Operation: "RequestCertificate",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::CertificateManager::Certificate",
		PhysicalID: domain,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			CertificateArn string `json:"CertificateArn"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil && result.CertificateArn != "" {
			dr.ARN = result.CertificateArn
			dr.PhysicalID = result.CertificateArn
		}
	}
	return dr, cost, nil
}

// ----- v0.19.0 — API Gateway v1 --------------------------------------------

// deployAPIGatewayRestAPI creates an API Gateway REST API for the given CFN resource.
func (d *StackDeployer) deployAPIGatewayRestAPI(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)

	body := map[string]interface{}{
		"name":        name,
		"description": resolveStringProp(props, "Description", "", cctx),
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal restapi body: %w", err)
	}

	req := &AWSRequest{
		Service:   "apigateway",
		Operation: "POST",
		Path:      "/restapis",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::ApiGateway::RestApi",
		PhysicalID: name,
		Metadata:   make(map[string]interface{}),
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			ID             string `json:"id"`
			RootResourceID string `json:"rootResourceId"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil {
			if result.ID != "" {
				dr.PhysicalID = result.ID
				dr.ARN = "arn:aws:apigateway:" + cctx.region + "::/restapis/" + result.ID
			}
			if result.RootResourceID != "" {
				dr.Metadata["RootResourceId"] = result.RootResourceID
			}
		}
	}
	return dr, cost, nil
}

// deployAPIGatewayAuthorizer creates an API Gateway authorizer for the given CFN resource.
func (d *StackDeployer) deployAPIGatewayAuthorizer(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	restAPIID := resolveStringProp(props, "RestApiId", "", cctx)
	name := resolveStringProp(props, "Name", logicalID, cctx)

	body := map[string]interface{}{
		"name":           name,
		"type":           resolveStringProp(props, "Type", "TOKEN", cctx),
		"identitySource": resolveStringProp(props, "IdentitySource", "method.request.header.Authorization", cctx),
	}
	if uri, ok := props["AuthorizerUri"]; ok {
		body["authorizerUri"] = resolveValue(uri, cctx)
	}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "apigateway",
		Operation: "POST",
		Path:      "/restapis/" + restAPIID + "/authorizers",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ApiGateway::Authorizer", PhysicalID: name}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			ID string `json:"id"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil && result.ID != "" {
			dr.PhysicalID = result.ID
		}
	}
	return dr, cost, nil
}

// deployAPIGatewayResource creates an API Gateway resource for the given CFN resource.
func (d *StackDeployer) deployAPIGatewayResource(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	restAPIID := resolveStringProp(props, "RestApiId", "", cctx)
	parentID := resolveStringProp(props, "ParentId", "", cctx)
	pathPart := resolveStringProp(props, "PathPart", logicalID, cctx)

	body := map[string]interface{}{
		"pathPart": pathPart,
	}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "apigateway",
		Operation: "POST",
		Path:      "/restapis/" + restAPIID + "/resources/" + parentID,
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ApiGateway::Resource", PhysicalID: pathPart}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			ID string `json:"id"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil && result.ID != "" {
			dr.PhysicalID = result.ID
		}
	}
	return dr, cost, nil
}

// deployAPIGatewayMethod creates an API Gateway method for the given CFN resource.
func (d *StackDeployer) deployAPIGatewayMethod(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	restAPIID := resolveStringProp(props, "RestApiId", "", cctx)
	resourceID := resolveStringProp(props, "ResourceId", "", cctx)
	httpMethod := resolveStringProp(props, "HttpMethod", "GET", cctx)

	body := map[string]interface{}{
		"authorizationType": resolveStringProp(props, "AuthorizationType", "NONE", cctx),
	}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "apigateway",
		Operation: "PUT",
		Path:      "/restapis/" + restAPIID + "/resources/" + resourceID + "/methods/" + httpMethod,
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ApiGateway::Method", PhysicalID: httpMethod}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployAPIGatewayDeployment creates an API Gateway deployment for the given CFN resource.
func (d *StackDeployer) deployAPIGatewayDeployment(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	restAPIID := resolveStringProp(props, "RestApiId", "", cctx)

	body := map[string]interface{}{
		"description": resolveStringProp(props, "Description", "", cctx),
	}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "apigateway",
		Operation: "POST",
		Path:      "/restapis/" + restAPIID + "/deployments",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ApiGateway::Deployment", PhysicalID: logicalID}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			ID string `json:"id"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil && result.ID != "" {
			dr.PhysicalID = result.ID
		}
	}
	return dr, cost, nil
}

// deployAPIGatewayStage creates an API Gateway stage for the given CFN resource.
func (d *StackDeployer) deployAPIGatewayStage(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	restAPIID := resolveStringProp(props, "RestApiId", "", cctx)
	stageName := resolveStringProp(props, "StageName", "prod", cctx)

	body := map[string]interface{}{
		"stageName":    stageName,
		"deploymentId": resolveStringProp(props, "DeploymentId", "", cctx),
		"description":  resolveStringProp(props, "Description", "", cctx),
	}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "apigateway",
		Operation: "POST",
		Path:      "/restapis/" + restAPIID + "/stages",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	invokeURL := "https://" + restAPIID + ".execute-api." + cctx.region + ".amazonaws.com/" + stageName
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::ApiGateway::Stage",
		PhysicalID: stageName,
		ARN:        "arn:aws:apigateway:" + cctx.region + "::/restapis/" + restAPIID + "/stages/" + stageName,
		Metadata:   map[string]interface{}{"InvokeURL": invokeURL},
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployAPIGatewayAPIKey creates an API Gateway API key for the given CFN resource.
func (d *StackDeployer) deployAPIGatewayAPIKey(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)

	body := map[string]interface{}{
		"name":    name,
		"enabled": true,
	}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "apigateway",
		Operation: "POST",
		Path:      "/apikeys",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ApiGateway::ApiKey", PhysicalID: name}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			ID string `json:"id"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil && result.ID != "" {
			dr.PhysicalID = result.ID
		}
	}
	return dr, cost, nil
}

// deployAPIGatewayUsagePlan creates an API Gateway usage plan for the given CFN resource.
func (d *StackDeployer) deployAPIGatewayUsagePlan(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "UsagePlanName", logicalID, cctx)

	body := map[string]interface{}{
		"name":        name,
		"description": resolveStringProp(props, "Description", "", cctx),
	}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "apigateway",
		Operation: "POST",
		Path:      "/usageplans",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ApiGateway::UsagePlan", PhysicalID: name}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			ID string `json:"id"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil && result.ID != "" {
			dr.PhysicalID = result.ID
		}
	}
	return dr, cost, nil
}

// deployAPIGatewayUsagePlanKey associates an API key with a usage plan.
func (d *StackDeployer) deployAPIGatewayUsagePlanKey(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	planID := resolveStringProp(props, "UsagePlanId", "", cctx)
	keyID := resolveStringProp(props, "KeyId", "", cctx)

	body := map[string]interface{}{
		"keyId":   keyID,
		"keyType": "API_KEY",
	}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "apigateway",
		Operation: "POST",
		Path:      "/usageplans/" + planID + "/keys",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ApiGateway::UsagePlanKey", PhysicalID: keyID}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// ----- v0.19.0 — API Gateway v2 --------------------------------------------

// deployAPIGatewayV2Api creates an API Gateway v2 (HTTP/WebSocket) API.
func (d *StackDeployer) deployAPIGatewayV2Api(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "Name", logicalID, cctx)

	body := map[string]interface{}{
		"Name":         name,
		"ProtocolType": resolveStringProp(props, "ProtocolType", "HTTP", cctx),
		"Description":  resolveStringProp(props, "Description", "", cctx),
	}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "apigatewayv2",
		Operation: "POST",
		Path:      "/v2/apis",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ApiGatewayV2::Api", PhysicalID: name}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			APIID string `json:"ApiId"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil && result.APIID != "" {
			dr.PhysicalID = result.APIID
			dr.ARN = "arn:aws:apigateway:" + cctx.region + "::/apis/" + result.APIID
		}
	}
	return dr, cost, nil
}

// deployAPIGatewayV2Route creates an API Gateway v2 route.
func (d *StackDeployer) deployAPIGatewayV2Route(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	apiID := resolveStringProp(props, "ApiId", "", cctx)
	routeKey := resolveStringProp(props, "RouteKey", "ANY /{proxy+}", cctx)

	body := map[string]interface{}{
		"RouteKey": routeKey,
		"Target":   resolveStringProp(props, "Target", "", cctx),
	}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "apigatewayv2",
		Operation: "POST",
		Path:      "/v2/apis/" + apiID + "/routes",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ApiGatewayV2::Route", PhysicalID: routeKey}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			RouteID string `json:"RouteId"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil && result.RouteID != "" {
			dr.PhysicalID = result.RouteID
		}
	}
	return dr, cost, nil
}

// deployAPIGatewayV2Integration creates an API Gateway v2 integration.
func (d *StackDeployer) deployAPIGatewayV2Integration(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	apiID := resolveStringProp(props, "ApiId", "", cctx)

	body := map[string]interface{}{
		"IntegrationType": resolveStringProp(props, "IntegrationType", "AWS_PROXY", cctx),
		"IntegrationUri":  resolveStringProp(props, "IntegrationUri", "", cctx),
	}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "apigatewayv2",
		Operation: "POST",
		Path:      "/v2/apis/" + apiID + "/integrations",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ApiGatewayV2::Integration", PhysicalID: logicalID}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			IntegrationID string `json:"IntegrationId"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil && result.IntegrationID != "" {
			dr.PhysicalID = result.IntegrationID
		}
	}
	return dr, cost, nil
}

// deployAPIGatewayV2Stage creates an API Gateway v2 stage.
func (d *StackDeployer) deployAPIGatewayV2Stage(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	apiID := resolveStringProp(props, "ApiId", "", cctx)
	stageName := resolveStringProp(props, "StageName", "$default", cctx)

	body := map[string]interface{}{
		"StageName": stageName,
	}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "apigatewayv2",
		Operation: "POST",
		Path:      "/v2/apis/" + apiID + "/stages",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ApiGatewayV2::Stage", PhysicalID: stageName}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployAPIGatewayV2Authorizer creates an API Gateway v2 authorizer.
func (d *StackDeployer) deployAPIGatewayV2Authorizer(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	apiID := resolveStringProp(props, "ApiId", "", cctx)
	name := resolveStringProp(props, "Name", logicalID, cctx)

	body := map[string]interface{}{
		"Name":           name,
		"AuthorizerType": resolveStringProp(props, "AuthorizerType", "JWT", cctx),
	}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "apigatewayv2",
		Operation: "POST",
		Path:      "/v2/apis/" + apiID + "/authorizers",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::ApiGatewayV2::Authorizer", PhysicalID: name}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			AuthorizerID string `json:"AuthorizerId"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil && result.AuthorizerID != "" {
			dr.PhysicalID = result.AuthorizerID
		}
	}
	return dr, cost, nil
}

