package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// UpdateRoute and UpdateStage (#1279), the two convergence operations a deployer calls on a second
// deploy: retarget a route whose integration was replaced, and bring a stage's auto-deploy and
// access-logging settings to the desired state.
//
// # A member absent from the body is left as it was
//
// Every member of UpdateRouteInput and UpdateStageInput is Required: No, and an update names only
// what it changes. So each patch member below is a pointer: nil means the caller did not send it and
// the stored value stands; non-nil replaces the stored value, even with an empty string or false,
// which is how a caller clears AuthorizerId or turns AutoDeploy off. updateAPI's older rule, which
// applies a member only when it is non-empty, cannot express either, and is not followed here.
//
// Members the published input declares and the record does not model are not read: for a route,
// authorizationScopes, apiKeyRequired, requestParameters, requestModels, modelSelectionExpression,
// operationName and routeResponseSelectionExpression; for a stage, clientCertificateId and
// routeSettings. docs/services.md names them.

// apigwv2RouteUpdate is UpdateRouteInput's members the route record models.
type apigwv2RouteUpdate struct {
	RouteKey          *string `json:"routeKey"`
	Target            *string `json:"target"`
	AuthorizationType *string `json:"authorizationType"`
	AuthorizerID      *string `json:"authorizerId"`
}

// apigwv2StageUpdate is UpdateStageInput's members the stage record models.
type apigwv2StageUpdate struct {
	DeploymentID         *string              `json:"deploymentId"`
	Description          *string              `json:"description"`
	StageVariables       *map[string]string   `json:"stageVariables"`
	AutoDeploy           *bool                `json:"autoDeploy"`
	AccessLogSettings    *V2AccessLogSettings `json:"accessLogSettings"`
	DefaultRouteSettings *V2RouteSettings     `json:"defaultRouteSettings"`
}

// requireAPI answers NotFoundException for an API the account and Region hold no record of, so a
// route or stage update names the thing that is actually absent.
func (p *APIGatewayV2Plugin) requireAPI(ctx *RequestContext, apiID string) error {
	data, err := p.state.Get(context.Background(), apigatewayv2Namespace, apigwv2APIKey(ctx.AccountID, ctx.Region, apiID))
	if err != nil {
		return fmt.Errorf("apigatewayv2 requireAPI state.Get: %w", err)
	}
	if data == nil {
		return &AWSError{Code: "NotFoundException", Message: "API not found: " + apiID, HTTPStatus: http.StatusNotFound}
	}
	return nil
}

// updateRoute handles UpdateRoute: PATCH /v2/apis/{apiId}/routes/{routeId}.
func (p *APIGatewayV2Plugin) updateRoute(ctx *RequestContext, req *AWSRequest, apiID, routeID string) (*AWSResponse, error) {
	if err := p.requireAPI(ctx, apiID); err != nil {
		return nil, err
	}
	goCtx := context.Background()
	key := apigwv2RouteKey(ctx.AccountID, ctx.Region, apiID, routeID)
	data, err := p.state.Get(goCtx, apigatewayv2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 updateRoute state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Route not found: " + routeID, HTTPStatus: http.StatusNotFound}
	}
	var route V2RouteState
	if err := json.Unmarshal(data, &route); err != nil {
		return nil, fmt.Errorf("apigatewayv2 updateRoute unmarshal: %w", err)
	}

	var patch apigwv2RouteUpdate
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &patch); err != nil {
			return nil, apigwv2InvalidBody()
		}
	}
	if patch.RouteKey != nil {
		route.RouteKey = *patch.RouteKey
	}
	if patch.Target != nil {
		route.Target = *patch.Target
	}
	if patch.AuthorizationType != nil {
		route.AuthorizationType = *patch.AuthorizationType
	}
	if patch.AuthorizerID != nil {
		route.AuthorizerID = *patch.AuthorizerID
	}

	updated, err := json.Marshal(route)
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 updateRoute marshal: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayv2Namespace, key, updated); err != nil {
		return nil, fmt.Errorf("apigatewayv2 updateRoute state.Put: %w", err)
	}
	return apigwJSONResponse(http.StatusOK, v2RouteWire(route))
}

// updateStageV2 handles UpdateStage: PATCH /v2/apis/{apiId}/stages/{stageName}.
//
// The stage's lastUpdatedDate, which the Stage shape publishes, is set from the simulated clock on
// every update that reaches the store.
func (p *APIGatewayV2Plugin) updateStageV2(ctx *RequestContext, req *AWSRequest, apiID, stageName string) (*AWSResponse, error) {
	if err := p.requireAPI(ctx, apiID); err != nil {
		return nil, err
	}
	goCtx := context.Background()
	key := apigwv2StageKey(ctx.AccountID, ctx.Region, apiID, stageName)
	data, err := p.state.Get(goCtx, apigatewayv2Namespace, key)
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 updateStage state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Stage not found: " + stageName, HTTPStatus: http.StatusNotFound}
	}
	var stage V2StageState
	if err := json.Unmarshal(data, &stage); err != nil {
		return nil, fmt.Errorf("apigatewayv2 updateStage unmarshal: %w", err)
	}

	var patch apigwv2StageUpdate
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &patch); err != nil {
			return nil, apigwv2InvalidBody()
		}
	}
	if patch.DeploymentID != nil {
		stage.DeploymentID = *patch.DeploymentID
	}
	if patch.Description != nil {
		stage.Description = *patch.Description
	}
	if patch.StageVariables != nil {
		stage.StageVariables = *patch.StageVariables
	}
	if patch.AutoDeploy != nil {
		stage.AutoDeploy = *patch.AutoDeploy
	}
	if patch.AccessLogSettings != nil {
		stage.AccessLogSettings = patch.AccessLogSettings
	}
	if patch.DefaultRouteSettings != nil {
		stage.DefaultRouteSettings = patch.DefaultRouteSettings
	}
	now := p.tc.Now()
	stage.LastUpdatedDate = &now

	updated, err := json.Marshal(stage)
	if err != nil {
		return nil, fmt.Errorf("apigatewayv2 updateStage marshal: %w", err)
	}
	if err := p.state.Put(goCtx, apigatewayv2Namespace, key, updated); err != nil {
		return nil, fmt.Errorf("apigatewayv2 updateStage state.Put: %w", err)
	}
	return apigwJSONResponse(http.StatusOK, v2StageWire(stage))
}
