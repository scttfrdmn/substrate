package emulator

// cfn_resources_v22.go holds the StackDeployer.deployResource helpers for Cognito.
// The name records the substrate release that added them (v0.22.0) rather than the
// services, because several releases touched overlapping services; the helpers here
// follow the same pattern as those in cfn_deployer.go.

import (
	"context"
	"encoding/json"
	"fmt"
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
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployCognitoUserPool marshal: %w", err)
	}

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
		// Id, not UserPoolId: API_UserPoolType publishes the pool's identifier as Id, and #756 stopped
		// CreateUserPool answering it under the unpublished name as well — see cognito_idp_wire.go.
		var result struct {
			UserPool struct {
				ID  string `json:"Id"`
				Arn string `json:"Arn"`
			} `json:"UserPool"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil {
			if result.UserPool.ID != "" {
				dr.PhysicalID = result.UserPool.ID
				// ProviderName and ProviderURL are derived rather than read off the response, because
				// UserPoolType publishes no provider member for substrate to read. That is also what
				// real CloudFormation does: both are Fn::GetAtt attributes of AWS::Cognito::UserPool
				// computed from the pool's Region and ID, not members of the CreateUserPool result.
				// The expression matches the one createUserPool mints the record's value with.
				providerName := fmt.Sprintf("cognito-idp.%s.amazonaws.com/%s", cctx.region, result.UserPool.ID)
				dr.Metadata["ProviderName"] = providerName
				dr.Metadata["ProviderURL"] = "https://" + providerName
			}
			if result.UserPool.Arn != "" {
				dr.ARN = result.UserPool.Arn
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
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployCognitoUserPoolClient marshal: %w", err)
	}

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
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployCognitoUserPoolGroup marshal: %w", err)
	}

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
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployCognitoUserPoolDomain marshal: %w", err)
	}

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
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployCognitoIdentityPool marshal: %w", err)
	}

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
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("cfn deployCognitoIdentityPoolRoleAttachment marshal: %w", err)
	}

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
