package substrate

import (
	"context"
	"encoding/json"
)

// ----- v0.22.0 — Cognito ---------------------------------------------------

// deployCognitoUserPool creates a Cognito User Pool for the given CFN resource.
func (d *StackDeployer) deployCognitoUserPool(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	poolName := resolveStringProp(props, "UserPoolName", logicalID, cctx)

	body := map[string]interface{}{"PoolName": poolName}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "cognito-idp",
		Operation: "CreateUserPool",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "AWSCognitoIdentityProviderService.CreateUserPool"},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Cognito::UserPool",
		PhysicalID: poolName,
		Metadata:   make(map[string]interface{}),
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			UserPool struct {
				ID           string `json:"UserPoolId"`
				Arn          string `json:"Arn"`
				ProviderName string `json:"ProviderName"`
			} `json:"UserPool"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil {
			if result.UserPool.ID != "" {
				dr.PhysicalID = result.UserPool.ID
			}
			if result.UserPool.Arn != "" {
				dr.ARN = result.UserPool.Arn
			}
			if result.UserPool.ProviderName != "" {
				dr.Metadata["ProviderName"] = result.UserPool.ProviderName
				dr.Metadata["ProviderURL"] = "https://" + result.UserPool.ProviderName
			}
		}
	}
	return dr, cost, nil
}

// deployCognitoUserPoolClient creates a Cognito User Pool App Client.
func (d *StackDeployer) deployCognitoUserPoolClient(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	poolID := resolveStringProp(props, "UserPoolId", "", cctx)
	clientName := resolveStringProp(props, "ClientName", logicalID, cctx)

	body := map[string]interface{}{
		"UserPoolId": poolID,
		"ClientName": clientName,
	}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "cognito-idp",
		Operation: "CreateUserPoolClient",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "AWSCognitoIdentityProviderService.CreateUserPoolClient"},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::Cognito::UserPoolClient", PhysicalID: clientName}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			UserPoolClient struct {
				ClientID string `json:"ClientId"`
			} `json:"UserPoolClient"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil && result.UserPoolClient.ClientID != "" {
			dr.PhysicalID = result.UserPoolClient.ClientID
		}
	}
	return dr, cost, nil
}

// deployCognitoUserPoolGroup creates a group in a Cognito User Pool.
func (d *StackDeployer) deployCognitoUserPoolGroup(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	poolID := resolveStringProp(props, "UserPoolId", "", cctx)
	groupName := resolveStringProp(props, "GroupName", logicalID, cctx)

	body := map[string]interface{}{
		"UserPoolId": poolID,
		"GroupName":  groupName,
	}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "cognito-idp",
		Operation: "CreateGroup",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "AWSCognitoIdentityProviderService.CreateGroup"},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::Cognito::UserPoolGroup", PhysicalID: groupName}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployCognitoUserPoolDomain creates a domain for a Cognito User Pool.
func (d *StackDeployer) deployCognitoUserPoolDomain(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	poolID := resolveStringProp(props, "UserPoolId", "", cctx)
	domain := resolveStringProp(props, "Domain", logicalID, cctx)

	body := map[string]interface{}{
		"UserPoolId": poolID,
		"Domain":     domain,
	}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "cognito-idp",
		Operation: "CreateUserPoolDomain",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "AWSCognitoIdentityProviderService.CreateUserPoolDomain"},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::Cognito::UserPoolDomain", PhysicalID: domain}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployCognitoIdentityPool creates a Cognito Identity Pool.
func (d *StackDeployer) deployCognitoIdentityPool(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	poolName := resolveStringProp(props, "IdentityPoolName", logicalID, cctx)

	body := map[string]interface{}{
		"IdentityPoolName":               poolName,
		"AllowUnauthenticatedIdentities": false,
	}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "cognito-identity",
		Operation: "CreateIdentityPool",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "AWSCognitoIdentityService.CreateIdentityPool"},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::Cognito::IdentityPool", PhysicalID: poolName}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			IdentityPoolID string `json:"IdentityPoolId"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil && result.IdentityPoolID != "" {
			dr.PhysicalID = result.IdentityPoolID
			dr.ARN = "arn:aws:cognito-identity:" + cctx.region + ":" + cctx.accountID + ":identitypool/" + result.IdentityPoolID
		}
	}
	return dr, cost, nil
}

// deployCognitoIdentityPoolRoleAttachment is a stub for role attachment (no-op).
func (d *StackDeployer) deployCognitoIdentityPoolRoleAttachment(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	poolID := resolveStringProp(props, "IdentityPoolId", "", cctx)

	body := map[string]interface{}{
		"IdentityPoolId": poolID,
		"Roles":          props["Roles"],
	}
	bodyBytes, _ := json.Marshal(body)

	req := &AWSRequest{
		Service:   "cognito-identity",
		Operation: "SetIdentityPoolRoles",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "AWSCognitoIdentityService.SetIdentityPoolRoles"},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::Cognito::IdentityPoolRoleAttachment", PhysicalID: poolID}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

