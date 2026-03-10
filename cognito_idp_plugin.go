package substrate

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// CognitoIDPPlugin emulates the Amazon Cognito User Pools (IDP) JSON-protocol API.
// It handles user pool lifecycle, app clients, groups, users, and authentication stubs.
type CognitoIDPPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "cognito-idp".
func (p *CognitoIDPPlugin) Name() string { return "cognito-idp" }

// Initialize sets up the CognitoIDPPlugin with the provided configuration.
func (p *CognitoIDPPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for CognitoIDPPlugin.
func (p *CognitoIDPPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a Cognito IDP JSON-protocol request to the appropriate handler.
// The operation is derived from the X-Amz-Target header suffix after the last dot.
func (p *CognitoIDPPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op := req.Operation
	if target := req.Headers["X-Amz-Target"]; target != "" {
		if idx := strings.LastIndex(target, "."); idx >= 0 {
			op = target[idx+1:]
		}
	}

	switch op {
	case "CreateUserPool":
		return p.createUserPool(ctx, req)
	case "DescribeUserPool":
		return p.describeUserPool(ctx, req)
	case "UpdateUserPool":
		return p.updateUserPool(ctx, req)
	case "DeleteUserPool":
		return p.deleteUserPool(ctx, req)
	case "ListUserPools":
		return p.listUserPools(ctx, req)
	case "CreateUserPoolClient":
		return p.createUserPoolClient(ctx, req)
	case "DescribeUserPoolClient":
		return p.describeUserPoolClient(ctx, req)
	case "UpdateUserPoolClient":
		return p.updateUserPoolClient(ctx, req)
	case "DeleteUserPoolClient":
		return p.deleteUserPoolClient(ctx, req)
	case "ListUserPoolClients":
		return p.listUserPoolClients(ctx, req)
	case "CreateUserPoolDomain":
		return p.createUserPoolDomain(ctx, req)
	case "DescribeUserPoolDomain":
		return p.describeUserPoolDomain(ctx, req)
	case "DeleteUserPoolDomain":
		return p.deleteUserPoolDomain(ctx, req)
	case "CreateGroup":
		return p.createGroup(ctx, req)
	case "GetGroup":
		return p.getGroup(ctx, req)
	case "DeleteGroup":
		return p.deleteGroup(ctx, req)
	case "ListGroups":
		return p.listGroups(ctx, req)
	case "AdminCreateUser":
		return p.adminCreateUser(ctx, req)
	case "AdminSetUserPassword":
		return p.adminSetUserPassword(ctx, req)
	case "AdminGetUser":
		return p.adminGetUser(ctx, req)
	case "AdminDeleteUser":
		return p.adminDeleteUser(ctx, req)
	case "ListUsers":
		return p.listUsers(ctx, req)
	case "AdminAddUserToGroup":
		return p.adminAddUserToGroup(ctx, req)
	case "AdminRemoveUserFromGroup":
		return p.adminRemoveUserFromGroup(ctx, req)
	case "AdminListGroupsForUser":
		return p.adminListGroupsForUser(ctx, req)
	case "InitiateAuth":
		return p.initiateAuth(ctx, req)
	case "RespondToAuthChallenge":
		return p.respondToAuthChallenge(ctx, req)
	case "SignUp":
		return p.signUp(ctx, req)
	case "ConfirmSignUp":
		return p.confirmSignUp(ctx, req)
	case "GetUserPoolMfaConfig":
		return p.getUserPoolMfaConfig(ctx, req)
	case "SetUserPoolMfaConfig":
		return p.setUserPoolMfaConfig(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    fmt.Sprintf("CognitoIDPPlugin: unknown operation %q", op),
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// --- User Pool operations ----------------------------------------------------

func (p *CognitoIDPPlugin) createUserPool(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		PoolName         string            `json:"PoolName"`
		Policies         interface{}       `json:"Policies"`
		LambdaConfig     interface{}       `json:"LambdaConfig"`
		Schema           []interface{}     `json:"Schema"`
		MfaConfiguration string            `json:"MfaConfiguration"`
		UserPoolTags     map[string]string `json:"UserPoolTags"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.PoolName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "PoolName is required", HTTPStatus: http.StatusBadRequest}
	}

	poolID := ctx.Region + "_" + generateCognitoID()
	arn := fmt.Sprintf("arn:aws:cognito-idp:%s:%s:userpool/%s", ctx.Region, ctx.AccountID, poolID)
	providerName := fmt.Sprintf("cognito-idp.%s.amazonaws.com/%s", ctx.Region, poolID)
	mfa := body.MfaConfiguration
	if mfa == "" {
		mfa = "OFF"
	}

	now := p.tc.Now()
	pool := CognitoUserPool{
		UserPoolID:       poolID,
		Name:             body.PoolName,
		Arn:              arn,
		ProviderName:     providerName,
		Status:           "Enabled",
		Policies:         body.Policies,
		LambdaConfig:     body.LambdaConfig,
		SchemaAttributes: body.Schema,
		MfaConfiguration: mfa,
		Tags:             body.UserPoolTags,
		CreationDate:     now,
		LastModifiedDate: now,
		AccountID:        ctx.AccountID,
		Region:           ctx.Region,
	}

	data, err := json.Marshal(pool)
	if err != nil {
		return nil, fmt.Errorf("cognito-idp createUserPool marshal: %w", err)
	}

	goCtx := context.Background()
	stateKey := cognitoUserPoolKey(ctx.AccountID, ctx.Region, poolID)
	if err := p.state.Put(goCtx, cognitoIDPNamespace, stateKey, data); err != nil {
		return nil, fmt.Errorf("cognito-idp createUserPool state.Put: %w", err)
	}
	updateStringIndex(goCtx, p.state, cognitoIDPNamespace, cognitoUserPoolIDsKey(ctx.AccountID, ctx.Region), poolID)

	type response struct {
		UserPool CognitoUserPool `json:"UserPool"`
	}
	return cognitoIDPJSONResponse(http.StatusOK, response{UserPool: pool})
}

func (p *CognitoIDPPlugin) describeUserPool(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		UserPoolID string `json:"UserPoolId"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	pool, err := p.loadUserPool(ctx, body.UserPoolID)
	if err != nil {
		return nil, err
	}
	type response struct {
		UserPool CognitoUserPool `json:"UserPool"`
	}
	return cognitoIDPJSONResponse(http.StatusOK, response{UserPool: *pool})
}

func (p *CognitoIDPPlugin) updateUserPool(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		UserPoolID       string      `json:"UserPoolId"`
		Policies         interface{} `json:"Policies"`
		LambdaConfig     interface{} `json:"LambdaConfig"`
		MfaConfiguration string      `json:"MfaConfiguration"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	pool, err := p.loadUserPool(ctx, body.UserPoolID)
	if err != nil {
		return nil, err
	}
	if body.Policies != nil {
		pool.Policies = body.Policies
	}
	if body.LambdaConfig != nil {
		pool.LambdaConfig = body.LambdaConfig
	}
	if body.MfaConfiguration != "" {
		pool.MfaConfiguration = body.MfaConfiguration
	}
	pool.LastModifiedDate = p.tc.Now()

	data, err := json.Marshal(pool)
	if err != nil {
		return nil, fmt.Errorf("cognito-idp updateUserPool marshal: %w", err)
	}
	goCtx := context.Background()
	stateKey := cognitoUserPoolKey(ctx.AccountID, ctx.Region, pool.UserPoolID)
	if err := p.state.Put(goCtx, cognitoIDPNamespace, stateKey, data); err != nil {
		return nil, fmt.Errorf("cognito-idp updateUserPool state.Put: %w", err)
	}
	type response struct {
		UserPool CognitoUserPool `json:"UserPool"`
	}
	return cognitoIDPJSONResponse(http.StatusOK, response{UserPool: *pool})
}

func (p *CognitoIDPPlugin) deleteUserPool(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		UserPoolID string `json:"UserPoolId"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if _, err := p.loadUserPool(ctx, body.UserPoolID); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	poolID := body.UserPoolID

	// Remove pool record.
	_ = p.state.Delete(goCtx, cognitoIDPNamespace, cognitoUserPoolKey(ctx.AccountID, ctx.Region, poolID))
	removeFromStringIndex(goCtx, p.state, cognitoIDPNamespace, cognitoUserPoolIDsKey(ctx.AccountID, ctx.Region), poolID)

	// Remove all clients.
	clientIDs, _ := loadStringIndex(goCtx, p.state, cognitoIDPNamespace, cognitoUserPoolClientIDsKey(ctx.AccountID, ctx.Region, poolID))
	for _, cid := range clientIDs {
		_ = p.state.Delete(goCtx, cognitoIDPNamespace, cognitoUserPoolClientKey(ctx.AccountID, ctx.Region, poolID, cid))
	}
	_ = p.state.Delete(goCtx, cognitoIDPNamespace, cognitoUserPoolClientIDsKey(ctx.AccountID, ctx.Region, poolID))

	// Remove all groups.
	groupNames, _ := loadStringIndex(goCtx, p.state, cognitoIDPNamespace, cognitoGroupNamesKey(ctx.AccountID, ctx.Region, poolID))
	for _, g := range groupNames {
		_ = p.state.Delete(goCtx, cognitoIDPNamespace, cognitoGroupKey(ctx.AccountID, ctx.Region, poolID, g))
	}
	_ = p.state.Delete(goCtx, cognitoIDPNamespace, cognitoGroupNamesKey(ctx.AccountID, ctx.Region, poolID))

	// Remove all users.
	userNames, _ := loadStringIndex(goCtx, p.state, cognitoIDPNamespace, cognitoUserNamesKey(ctx.AccountID, ctx.Region, poolID))
	for _, u := range userNames {
		_ = p.state.Delete(goCtx, cognitoIDPNamespace, cognitoUserKey(ctx.AccountID, ctx.Region, poolID, u))
	}
	_ = p.state.Delete(goCtx, cognitoIDPNamespace, cognitoUserNamesKey(ctx.AccountID, ctx.Region, poolID))

	return cognitoIDPJSONResponse(http.StatusOK, struct{}{})
}

func (p *CognitoIDPPlugin) listUserPools(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		MaxResults int    `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
	}
	_ = json.Unmarshal(req.Body, &body)
	if body.MaxResults <= 0 {
		body.MaxResults = 60
	}

	goCtx := context.Background()
	poolIDs, err := loadStringIndex(goCtx, p.state, cognitoIDPNamespace, cognitoUserPoolIDsKey(ctx.AccountID, ctx.Region))
	if err != nil {
		return nil, fmt.Errorf("cognito-idp listUserPools loadStringIndex: %w", err)
	}

	type poolSummary struct {
		ID               string    `json:"Id"`
		Name             string    `json:"Name"`
		Status           string    `json:"Status"`
		CreationDate     time.Time `json:"CreationDate"`
		LastModifiedDate time.Time `json:"LastModifiedDate"`
	}
	summaries := make([]poolSummary, 0, len(poolIDs))
	for _, id := range poolIDs {
		data, err := p.state.Get(goCtx, cognitoIDPNamespace, cognitoUserPoolKey(ctx.AccountID, ctx.Region, id))
		if err != nil || data == nil {
			continue
		}
		var pool CognitoUserPool
		if json.Unmarshal(data, &pool) != nil {
			continue
		}
		summaries = append(summaries, poolSummary{
			ID:               pool.UserPoolID,
			Name:             pool.Name,
			Status:           pool.Status,
			CreationDate:     pool.CreationDate,
			LastModifiedDate: pool.LastModifiedDate,
		})
	}

	var nextToken string
	if len(summaries) > body.MaxResults {
		nextToken = summaries[body.MaxResults].ID
		summaries = summaries[:body.MaxResults]
	}

	type response struct {
		UserPools []poolSummary `json:"UserPools"`
		NextToken string        `json:"NextToken,omitempty"`
	}
	return cognitoIDPJSONResponse(http.StatusOK, response{UserPools: summaries, NextToken: nextToken})
}

// --- User Pool Client operations ---------------------------------------------

func (p *CognitoIDPPlugin) createUserPoolClient(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		UserPoolID        string   `json:"UserPoolId"`
		ClientName        string   `json:"ClientName"`
		GenerateSecret    bool     `json:"GenerateSecret"`
		ExplicitAuthFlows []string `json:"ExplicitAuthFlows"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if _, err := p.loadUserPool(ctx, body.UserPoolID); err != nil {
		return nil, err
	}

	clientID := generateCognitoID()
	var secret string
	if body.GenerateSecret {
		secret = generateCognitoID() + generateCognitoID()
	}

	now := p.tc.Now()
	client := CognitoUserPoolClient{
		ClientID:          clientID,
		ClientName:        body.ClientName,
		UserPoolID:        body.UserPoolID,
		ClientSecret:      secret,
		ExplicitAuthFlows: body.ExplicitAuthFlows,
		CreationDate:      now,
		AccountID:         ctx.AccountID,
		Region:            ctx.Region,
	}

	data, err := json.Marshal(client)
	if err != nil {
		return nil, fmt.Errorf("cognito-idp createUserPoolClient marshal: %w", err)
	}

	goCtx := context.Background()
	stateKey := cognitoUserPoolClientKey(ctx.AccountID, ctx.Region, body.UserPoolID, clientID)
	if err := p.state.Put(goCtx, cognitoIDPNamespace, stateKey, data); err != nil {
		return nil, fmt.Errorf("cognito-idp createUserPoolClient state.Put: %w", err)
	}
	updateStringIndex(goCtx, p.state, cognitoIDPNamespace, cognitoUserPoolClientIDsKey(ctx.AccountID, ctx.Region, body.UserPoolID), clientID)

	type response struct {
		UserPoolClient CognitoUserPoolClient `json:"UserPoolClient"`
	}
	return cognitoIDPJSONResponse(http.StatusOK, response{UserPoolClient: client})
}

func (p *CognitoIDPPlugin) describeUserPoolClient(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		UserPoolID string `json:"UserPoolId"`
		ClientID   string `json:"ClientId"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	client, err := p.loadUserPoolClient(ctx, body.UserPoolID, body.ClientID)
	if err != nil {
		return nil, err
	}
	type response struct {
		UserPoolClient CognitoUserPoolClient `json:"UserPoolClient"`
	}
	return cognitoIDPJSONResponse(http.StatusOK, response{UserPoolClient: *client})
}

func (p *CognitoIDPPlugin) updateUserPoolClient(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		UserPoolID        string   `json:"UserPoolId"`
		ClientID          string   `json:"ClientId"`
		ClientName        string   `json:"ClientName"`
		ExplicitAuthFlows []string `json:"ExplicitAuthFlows"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	client, err := p.loadUserPoolClient(ctx, body.UserPoolID, body.ClientID)
	if err != nil {
		return nil, err
	}
	if body.ClientName != "" {
		client.ClientName = body.ClientName
	}
	if len(body.ExplicitAuthFlows) > 0 {
		client.ExplicitAuthFlows = body.ExplicitAuthFlows
	}

	data, err := json.Marshal(client)
	if err != nil {
		return nil, fmt.Errorf("cognito-idp updateUserPoolClient marshal: %w", err)
	}
	goCtx := context.Background()
	stateKey := cognitoUserPoolClientKey(ctx.AccountID, ctx.Region, body.UserPoolID, body.ClientID)
	if err := p.state.Put(goCtx, cognitoIDPNamespace, stateKey, data); err != nil {
		return nil, fmt.Errorf("cognito-idp updateUserPoolClient state.Put: %w", err)
	}
	type response struct {
		UserPoolClient CognitoUserPoolClient `json:"UserPoolClient"`
	}
	return cognitoIDPJSONResponse(http.StatusOK, response{UserPoolClient: *client})
}

func (p *CognitoIDPPlugin) deleteUserPoolClient(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		UserPoolID string `json:"UserPoolId"`
		ClientID   string `json:"ClientId"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if _, err := p.loadUserPoolClient(ctx, body.UserPoolID, body.ClientID); err != nil {
		return nil, err
	}
	goCtx := context.Background()
	_ = p.state.Delete(goCtx, cognitoIDPNamespace, cognitoUserPoolClientKey(ctx.AccountID, ctx.Region, body.UserPoolID, body.ClientID))
	removeFromStringIndex(goCtx, p.state, cognitoIDPNamespace, cognitoUserPoolClientIDsKey(ctx.AccountID, ctx.Region, body.UserPoolID), body.ClientID)
	return cognitoIDPJSONResponse(http.StatusOK, struct{}{})
}

func (p *CognitoIDPPlugin) listUserPoolClients(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		UserPoolID string `json:"UserPoolId"`
		MaxResults int    `json:"MaxResults"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if body.MaxResults <= 0 {
		body.MaxResults = 60
	}

	goCtx := context.Background()
	clientIDs, err := loadStringIndex(goCtx, p.state, cognitoIDPNamespace, cognitoUserPoolClientIDsKey(ctx.AccountID, ctx.Region, body.UserPoolID))
	if err != nil {
		return nil, fmt.Errorf("cognito-idp listUserPoolClients loadStringIndex: %w", err)
	}

	type clientSummary struct {
		ClientID   string `json:"ClientId"`
		ClientName string `json:"ClientName"`
		UserPoolID string `json:"UserPoolId"`
	}
	summaries := make([]clientSummary, 0, len(clientIDs))
	for _, cid := range clientIDs {
		data, err := p.state.Get(goCtx, cognitoIDPNamespace, cognitoUserPoolClientKey(ctx.AccountID, ctx.Region, body.UserPoolID, cid))
		if err != nil || data == nil {
			continue
		}
		var c CognitoUserPoolClient
		if json.Unmarshal(data, &c) != nil {
			continue
		}
		summaries = append(summaries, clientSummary{
			ClientID:   c.ClientID,
			ClientName: c.ClientName,
			UserPoolID: c.UserPoolID,
		})
	}

	type response struct {
		UserPoolClients []clientSummary `json:"UserPoolClients"`
	}
	return cognitoIDPJSONResponse(http.StatusOK, response{UserPoolClients: summaries})
}

// --- User Pool Domain operations ---------------------------------------------

func (p *CognitoIDPPlugin) createUserPoolDomain(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Domain     string `json:"Domain"`
		UserPoolID string `json:"UserPoolId"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if _, err := p.loadUserPool(ctx, body.UserPoolID); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	domainKey := cognitoUserPoolClientKey(ctx.AccountID, ctx.Region, body.UserPoolID, "domain")
	domainData, _ := json.Marshal(map[string]string{"Domain": body.Domain, "UserPoolId": body.UserPoolID})
	_ = p.state.Put(goCtx, cognitoIDPNamespace, domainKey, domainData)

	type response struct {
		CloudFrontDomain string `json:"CloudFrontDomain"`
	}
	return cognitoIDPJSONResponse(http.StatusOK, response{CloudFrontDomain: ""})
}

func (p *CognitoIDPPlugin) describeUserPoolDomain(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Domain string `json:"Domain"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	type domainDescription struct {
		Domain                 string `json:"Domain"`
		UserPoolID             string `json:"UserPoolId,omitempty"`
		Status                 string `json:"Status"`
		S3Bucket               string `json:"S3Bucket,omitempty"`
		CloudFrontDistribution string `json:"CloudFrontDistribution,omitempty"`
	}
	type response struct {
		DomainDescription domainDescription `json:"DomainDescription"`
	}
	return cognitoIDPJSONResponse(http.StatusOK, response{
		DomainDescription: domainDescription{
			Domain: body.Domain,
			Status: "ACTIVE",
		},
	})
}

func (p *CognitoIDPPlugin) deleteUserPoolDomain(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		Domain     string `json:"Domain"`
		UserPoolID string `json:"UserPoolId"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	domainKey := cognitoUserPoolClientKey(ctx.AccountID, ctx.Region, body.UserPoolID, "domain")
	_ = p.state.Delete(goCtx, cognitoIDPNamespace, domainKey)
	return cognitoIDPJSONResponse(http.StatusOK, struct{}{})
}

// --- Group operations --------------------------------------------------------

func (p *CognitoIDPPlugin) createGroup(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		GroupName   string `json:"GroupName"`
		UserPoolID  string `json:"UserPoolId"`
		Description string `json:"Description"`
		RoleArn     string `json:"RoleArn"`
		Precedence  int    `json:"Precedence"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if _, err := p.loadUserPool(ctx, body.UserPoolID); err != nil {
		return nil, err
	}

	group := CognitoGroup{
		GroupName:    body.GroupName,
		UserPoolID:   body.UserPoolID,
		Description:  body.Description,
		RoleArn:      body.RoleArn,
		Precedence:   body.Precedence,
		CreationDate: p.tc.Now(),
		AccountID:    ctx.AccountID,
		Region:       ctx.Region,
	}

	data, err := json.Marshal(group)
	if err != nil {
		return nil, fmt.Errorf("cognito-idp createGroup marshal: %w", err)
	}
	goCtx := context.Background()
	if err := p.state.Put(goCtx, cognitoIDPNamespace, cognitoGroupKey(ctx.AccountID, ctx.Region, body.UserPoolID, body.GroupName), data); err != nil {
		return nil, fmt.Errorf("cognito-idp createGroup state.Put: %w", err)
	}
	updateStringIndex(goCtx, p.state, cognitoIDPNamespace, cognitoGroupNamesKey(ctx.AccountID, ctx.Region, body.UserPoolID), body.GroupName)

	type response struct {
		Group CognitoGroup `json:"Group"`
	}
	return cognitoIDPJSONResponse(http.StatusOK, response{Group: group})
}

func (p *CognitoIDPPlugin) getGroup(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		GroupName  string `json:"GroupName"`
		UserPoolID string `json:"UserPoolId"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	group, err := p.loadGroup(ctx, body.UserPoolID, body.GroupName)
	if err != nil {
		return nil, err
	}
	type response struct {
		Group CognitoGroup `json:"Group"`
	}
	return cognitoIDPJSONResponse(http.StatusOK, response{Group: *group})
}

func (p *CognitoIDPPlugin) deleteGroup(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		GroupName  string `json:"GroupName"`
		UserPoolID string `json:"UserPoolId"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	_ = p.state.Delete(goCtx, cognitoIDPNamespace, cognitoGroupKey(ctx.AccountID, ctx.Region, body.UserPoolID, body.GroupName))
	removeFromStringIndex(goCtx, p.state, cognitoIDPNamespace, cognitoGroupNamesKey(ctx.AccountID, ctx.Region, body.UserPoolID), body.GroupName)
	return cognitoIDPJSONResponse(http.StatusOK, struct{}{})
}

func (p *CognitoIDPPlugin) listGroups(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		UserPoolID string `json:"UserPoolId"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	groupNames, err := loadStringIndex(goCtx, p.state, cognitoIDPNamespace, cognitoGroupNamesKey(ctx.AccountID, ctx.Region, body.UserPoolID))
	if err != nil {
		return nil, fmt.Errorf("cognito-idp listGroups loadStringIndex: %w", err)
	}

	groups := make([]CognitoGroup, 0, len(groupNames))
	for _, name := range groupNames {
		data, err := p.state.Get(goCtx, cognitoIDPNamespace, cognitoGroupKey(ctx.AccountID, ctx.Region, body.UserPoolID, name))
		if err != nil || data == nil {
			continue
		}
		var g CognitoGroup
		if json.Unmarshal(data, &g) != nil {
			continue
		}
		groups = append(groups, g)
	}

	type response struct {
		Groups []CognitoGroup `json:"Groups"`
	}
	return cognitoIDPJSONResponse(http.StatusOK, response{Groups: groups})
}

// --- User operations ---------------------------------------------------------

func (p *CognitoIDPPlugin) adminCreateUser(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		UserPoolID        string             `json:"UserPoolId"`
		Username          string             `json:"Username"`
		TemporaryPassword string             `json:"TemporaryPassword"`
		UserAttributes    []CognitoAttribute `json:"UserAttributes"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	if _, err := p.loadUserPool(ctx, body.UserPoolID); err != nil {
		return nil, err
	}

	now := p.tc.Now()
	user := CognitoUser{
		Username:             body.Username,
		UserStatus:           "FORCE_CHANGE_PASSWORD",
		Enabled:              true,
		Attributes:           body.UserAttributes,
		UserCreateDate:       now,
		UserLastModifiedDate: now,
		UserPoolID:           body.UserPoolID,
		AccountID:            ctx.AccountID,
		Region:               ctx.Region,
	}

	data, err := json.Marshal(user)
	if err != nil {
		return nil, fmt.Errorf("cognito-idp adminCreateUser marshal: %w", err)
	}
	goCtx := context.Background()
	if err := p.state.Put(goCtx, cognitoIDPNamespace, cognitoUserKey(ctx.AccountID, ctx.Region, body.UserPoolID, body.Username), data); err != nil {
		return nil, fmt.Errorf("cognito-idp adminCreateUser state.Put: %w", err)
	}
	updateStringIndex(goCtx, p.state, cognitoIDPNamespace, cognitoUserNamesKey(ctx.AccountID, ctx.Region, body.UserPoolID), body.Username)

	type response struct {
		User CognitoUser `json:"User"`
	}
	return cognitoIDPJSONResponse(http.StatusOK, response{User: user})
}

func (p *CognitoIDPPlugin) adminSetUserPassword(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		UserPoolID string `json:"UserPoolId"`
		Username   string `json:"Username"`
		Password   string `json:"Password"`
		Permanent  bool   `json:"Permanent"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	user, err := p.loadUser(ctx, body.UserPoolID, body.Username)
	if err != nil {
		return nil, err
	}
	if body.Permanent {
		user.UserStatus = "CONFIRMED"
	}
	user.UserLastModifiedDate = p.tc.Now()

	data, err := json.Marshal(user)
	if err != nil {
		return nil, fmt.Errorf("cognito-idp adminSetUserPassword marshal: %w", err)
	}
	goCtx := context.Background()
	if err := p.state.Put(goCtx, cognitoIDPNamespace, cognitoUserKey(ctx.AccountID, ctx.Region, body.UserPoolID, body.Username), data); err != nil {
		return nil, fmt.Errorf("cognito-idp adminSetUserPassword state.Put: %w", err)
	}
	return cognitoIDPJSONResponse(http.StatusOK, struct{}{})
}

func (p *CognitoIDPPlugin) adminGetUser(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		UserPoolID string `json:"UserPoolId"`
		Username   string `json:"Username"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	user, err := p.loadUser(ctx, body.UserPoolID, body.Username)
	if err != nil {
		return nil, err
	}

	// AdminGetUser returns user fields at top level (not nested).
	type response struct {
		Username             string             `json:"Username"`
		UserAttributes       []CognitoAttribute `json:"UserAttributes"`
		UserCreateDate       time.Time          `json:"UserCreateDate"`
		UserLastModifiedDate time.Time          `json:"UserLastModifiedDate"`
		Enabled              bool               `json:"Enabled"`
		UserStatus           string             `json:"UserStatus"`
	}
	return cognitoIDPJSONResponse(http.StatusOK, response{
		Username:             user.Username,
		UserAttributes:       user.Attributes,
		UserCreateDate:       user.UserCreateDate,
		UserLastModifiedDate: user.UserLastModifiedDate,
		Enabled:              user.Enabled,
		UserStatus:           user.UserStatus,
	})
}

func (p *CognitoIDPPlugin) adminDeleteUser(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		UserPoolID string `json:"UserPoolId"`
		Username   string `json:"Username"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	_ = p.state.Delete(goCtx, cognitoIDPNamespace, cognitoUserKey(ctx.AccountID, ctx.Region, body.UserPoolID, body.Username))
	removeFromStringIndex(goCtx, p.state, cognitoIDPNamespace, cognitoUserNamesKey(ctx.AccountID, ctx.Region, body.UserPoolID), body.Username)
	return cognitoIDPJSONResponse(http.StatusOK, struct{}{})
}

func (p *CognitoIDPPlugin) listUsers(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		UserPoolID string `json:"UserPoolId"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	userNames, err := loadStringIndex(goCtx, p.state, cognitoIDPNamespace, cognitoUserNamesKey(ctx.AccountID, ctx.Region, body.UserPoolID))
	if err != nil {
		return nil, fmt.Errorf("cognito-idp listUsers loadStringIndex: %w", err)
	}

	users := make([]CognitoUser, 0, len(userNames))
	for _, name := range userNames {
		data, err := p.state.Get(goCtx, cognitoIDPNamespace, cognitoUserKey(ctx.AccountID, ctx.Region, body.UserPoolID, name))
		if err != nil || data == nil {
			continue
		}
		var u CognitoUser
		if json.Unmarshal(data, &u) != nil {
			continue
		}
		users = append(users, u)
	}

	type response struct {
		Users []CognitoUser `json:"Users"`
	}
	return cognitoIDPJSONResponse(http.StatusOK, response{Users: users})
}

func (p *CognitoIDPPlugin) adminAddUserToGroup(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		UserPoolID string `json:"UserPoolId"`
		Username   string `json:"Username"`
		GroupName  string `json:"GroupName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	user, err := p.loadUser(ctx, body.UserPoolID, body.Username)
	if err != nil {
		return nil, err
	}
	for _, g := range user.Groups {
		if g == body.GroupName {
			return cognitoIDPJSONResponse(http.StatusOK, struct{}{})
		}
	}
	user.Groups = append(user.Groups, body.GroupName)

	data, err := json.Marshal(user)
	if err != nil {
		return nil, fmt.Errorf("cognito-idp adminAddUserToGroup marshal: %w", err)
	}
	goCtx := context.Background()
	if err := p.state.Put(goCtx, cognitoIDPNamespace, cognitoUserKey(ctx.AccountID, ctx.Region, body.UserPoolID, body.Username), data); err != nil {
		return nil, fmt.Errorf("cognito-idp adminAddUserToGroup state.Put: %w", err)
	}
	return cognitoIDPJSONResponse(http.StatusOK, struct{}{})
}

func (p *CognitoIDPPlugin) adminRemoveUserFromGroup(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		UserPoolID string `json:"UserPoolId"`
		Username   string `json:"Username"`
		GroupName  string `json:"GroupName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	user, err := p.loadUser(ctx, body.UserPoolID, body.Username)
	if err != nil {
		return nil, err
	}
	filtered := make([]string, 0, len(user.Groups))
	for _, g := range user.Groups {
		if g != body.GroupName {
			filtered = append(filtered, g)
		}
	}
	user.Groups = filtered

	data, err := json.Marshal(user)
	if err != nil {
		return nil, fmt.Errorf("cognito-idp adminRemoveUserFromGroup marshal: %w", err)
	}
	goCtx := context.Background()
	if err := p.state.Put(goCtx, cognitoIDPNamespace, cognitoUserKey(ctx.AccountID, ctx.Region, body.UserPoolID, body.Username), data); err != nil {
		return nil, fmt.Errorf("cognito-idp adminRemoveUserFromGroup state.Put: %w", err)
	}
	return cognitoIDPJSONResponse(http.StatusOK, struct{}{})
}

func (p *CognitoIDPPlugin) adminListGroupsForUser(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		UserPoolID string `json:"UserPoolId"`
		Username   string `json:"Username"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	user, err := p.loadUser(ctx, body.UserPoolID, body.Username)
	if err != nil {
		return nil, err
	}

	goCtx := context.Background()
	groups := make([]CognitoGroup, 0, len(user.Groups))
	for _, gName := range user.Groups {
		data, err := p.state.Get(goCtx, cognitoIDPNamespace, cognitoGroupKey(ctx.AccountID, ctx.Region, body.UserPoolID, gName))
		if err != nil || data == nil {
			continue
		}
		var g CognitoGroup
		if json.Unmarshal(data, &g) != nil {
			continue
		}
		groups = append(groups, g)
	}

	type response struct {
		Groups []CognitoGroup `json:"Groups"`
	}
	return cognitoIDPJSONResponse(http.StatusOK, response{Groups: groups})
}

// --- Auth stubs --------------------------------------------------------------

func (p *CognitoIDPPlugin) initiateAuth(_ *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	type authResult struct {
		AccessToken  string `json:"AccessToken"`
		IDToken      string `json:"IdToken"`
		RefreshToken string `json:"RefreshToken"`
		ExpiresIn    int    `json:"ExpiresIn"`
		TokenType    string `json:"TokenType"`
	}
	type response struct {
		AuthenticationResult authResult `json:"AuthenticationResult"`
	}
	return cognitoIDPJSONResponse(http.StatusOK, response{
		AuthenticationResult: authResult{
			AccessToken:  "fakeaccesstoken.fakepayload.fakesig",
			IDToken:      "fakeidtoken.fakepayload.fakesig",
			RefreshToken: "fakerefreshtoken",
			ExpiresIn:    3600,
			TokenType:    "Bearer",
		},
	})
}

func (p *CognitoIDPPlugin) respondToAuthChallenge(_ *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	type authResult struct {
		AccessToken  string `json:"AccessToken"`
		IDToken      string `json:"IdToken"`
		RefreshToken string `json:"RefreshToken"`
		ExpiresIn    int    `json:"ExpiresIn"`
		TokenType    string `json:"TokenType"`
	}
	type response struct {
		AuthenticationResult authResult `json:"AuthenticationResult"`
	}
	return cognitoIDPJSONResponse(http.StatusOK, response{
		AuthenticationResult: authResult{
			AccessToken:  "fakeaccesstoken.fakepayload.fakesig",
			IDToken:      "fakeidtoken.fakepayload.fakesig",
			RefreshToken: "fakerefreshtoken",
			ExpiresIn:    3600,
			TokenType:    "Bearer",
		},
	})
}

func (p *CognitoIDPPlugin) signUp(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ClientID       string             `json:"ClientId"`
		Username       string             `json:"Username"`
		Password       string             `json:"Password"`
		UserAttributes []CognitoAttribute `json:"UserAttributes"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	// Try to find the pool by client ID.
	goCtx := context.Background()
	poolIDs, _ := loadStringIndex(goCtx, p.state, cognitoIDPNamespace, cognitoUserPoolIDsKey(ctx.AccountID, ctx.Region))
	var poolID string
	for _, pid := range poolIDs {
		clientData, err := p.state.Get(goCtx, cognitoIDPNamespace, cognitoUserPoolClientKey(ctx.AccountID, ctx.Region, pid, body.ClientID))
		if err == nil && clientData != nil {
			poolID = pid
			break
		}
	}
	if poolID == "" {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Client not found: " + body.ClientID, HTTPStatus: http.StatusNotFound}
	}

	userSub := generateCognitoID()
	now := p.tc.Now()
	attrs := body.UserAttributes
	attrs = append(attrs, CognitoAttribute{Name: "sub", Value: userSub})

	user := CognitoUser{
		Username:             body.Username,
		UserStatus:           "UNCONFIRMED",
		Enabled:              true,
		Attributes:           attrs,
		UserCreateDate:       now,
		UserLastModifiedDate: now,
		UserPoolID:           poolID,
		AccountID:            ctx.AccountID,
		Region:               ctx.Region,
	}

	data, err := json.Marshal(user)
	if err != nil {
		return nil, fmt.Errorf("cognito-idp signUp marshal: %w", err)
	}
	if err := p.state.Put(goCtx, cognitoIDPNamespace, cognitoUserKey(ctx.AccountID, ctx.Region, poolID, body.Username), data); err != nil {
		return nil, fmt.Errorf("cognito-idp signUp state.Put: %w", err)
	}
	updateStringIndex(goCtx, p.state, cognitoIDPNamespace, cognitoUserNamesKey(ctx.AccountID, ctx.Region, poolID), body.Username)

	type response struct {
		UserConfirmed bool   `json:"UserConfirmed"`
		UserSub       string `json:"UserSub"`
	}
	return cognitoIDPJSONResponse(http.StatusOK, response{UserConfirmed: false, UserSub: userSub})
}

func (p *CognitoIDPPlugin) confirmSignUp(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		ClientID         string `json:"ClientId"`
		Username         string `json:"Username"`
		ConfirmationCode string `json:"ConfirmationCode"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	poolIDs, _ := loadStringIndex(goCtx, p.state, cognitoIDPNamespace, cognitoUserPoolIDsKey(ctx.AccountID, ctx.Region))
	var poolID string
	for _, pid := range poolIDs {
		clientData, err := p.state.Get(goCtx, cognitoIDPNamespace, cognitoUserPoolClientKey(ctx.AccountID, ctx.Region, pid, body.ClientID))
		if err == nil && clientData != nil {
			poolID = pid
			break
		}
	}
	if poolID == "" {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Client not found: " + body.ClientID, HTTPStatus: http.StatusNotFound}
	}

	user, err := p.loadUser(ctx, poolID, body.Username)
	if err != nil {
		return nil, err
	}
	user.UserStatus = "CONFIRMED"
	user.UserLastModifiedDate = p.tc.Now()

	data, err := json.Marshal(user)
	if err != nil {
		return nil, fmt.Errorf("cognito-idp confirmSignUp marshal: %w", err)
	}
	if err := p.state.Put(goCtx, cognitoIDPNamespace, cognitoUserKey(ctx.AccountID, ctx.Region, poolID, body.Username), data); err != nil {
		return nil, fmt.Errorf("cognito-idp confirmSignUp state.Put: %w", err)
	}
	return cognitoIDPJSONResponse(http.StatusOK, struct{}{})
}

// --- MFA config --------------------------------------------------------------

func (p *CognitoIDPPlugin) getUserPoolMfaConfig(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		UserPoolID string `json:"UserPoolId"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	pool, err := p.loadUserPool(ctx, body.UserPoolID)
	if err != nil {
		return nil, err
	}
	type response struct {
		MfaConfiguration string `json:"MfaConfiguration"`
	}
	return cognitoIDPJSONResponse(http.StatusOK, response{MfaConfiguration: pool.MfaConfiguration})
}

func (p *CognitoIDPPlugin) setUserPoolMfaConfig(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		UserPoolID       string `json:"UserPoolId"`
		MfaConfiguration string `json:"MfaConfiguration"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}
	pool, err := p.loadUserPool(ctx, body.UserPoolID)
	if err != nil {
		return nil, err
	}
	pool.MfaConfiguration = body.MfaConfiguration
	pool.LastModifiedDate = p.tc.Now()

	data, err := json.Marshal(pool)
	if err != nil {
		return nil, fmt.Errorf("cognito-idp setUserPoolMfaConfig marshal: %w", err)
	}
	goCtx := context.Background()
	if err := p.state.Put(goCtx, cognitoIDPNamespace, cognitoUserPoolKey(ctx.AccountID, ctx.Region, pool.UserPoolID), data); err != nil {
		return nil, fmt.Errorf("cognito-idp setUserPoolMfaConfig state.Put: %w", err)
	}
	type response struct {
		MfaConfiguration string `json:"MfaConfiguration"`
	}
	return cognitoIDPJSONResponse(http.StatusOK, response{MfaConfiguration: pool.MfaConfiguration})
}

// --- Helpers -----------------------------------------------------------------

func (p *CognitoIDPPlugin) loadUserPool(ctx *RequestContext, poolID string) (*CognitoUserPool, error) {
	if poolID == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "UserPoolId is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, cognitoIDPNamespace, cognitoUserPoolKey(ctx.AccountID, ctx.Region, poolID))
	if err != nil {
		return nil, fmt.Errorf("cognito-idp loadUserPool state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "User pool not found: " + poolID, HTTPStatus: http.StatusNotFound}
	}
	var pool CognitoUserPool
	if err := json.Unmarshal(data, &pool); err != nil {
		return nil, fmt.Errorf("cognito-idp loadUserPool unmarshal: %w", err)
	}
	return &pool, nil
}

func (p *CognitoIDPPlugin) loadUserPoolClient(ctx *RequestContext, poolID, clientID string) (*CognitoUserPoolClient, error) {
	if clientID == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "ClientId is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, cognitoIDPNamespace, cognitoUserPoolClientKey(ctx.AccountID, ctx.Region, poolID, clientID))
	if err != nil {
		return nil, fmt.Errorf("cognito-idp loadUserPoolClient state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Client not found: " + clientID, HTTPStatus: http.StatusNotFound}
	}
	var client CognitoUserPoolClient
	if err := json.Unmarshal(data, &client); err != nil {
		return nil, fmt.Errorf("cognito-idp loadUserPoolClient unmarshal: %w", err)
	}
	return &client, nil
}

func (p *CognitoIDPPlugin) loadGroup(ctx *RequestContext, poolID, groupName string) (*CognitoGroup, error) {
	if groupName == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "GroupName is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, cognitoIDPNamespace, cognitoGroupKey(ctx.AccountID, ctx.Region, poolID, groupName))
	if err != nil {
		return nil, fmt.Errorf("cognito-idp loadGroup state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Group not found: " + groupName, HTTPStatus: http.StatusNotFound}
	}
	var group CognitoGroup
	if err := json.Unmarshal(data, &group); err != nil {
		return nil, fmt.Errorf("cognito-idp loadGroup unmarshal: %w", err)
	}
	return &group, nil
}

func (p *CognitoIDPPlugin) loadUser(ctx *RequestContext, poolID, username string) (*CognitoUser, error) {
	if username == "" {
		return nil, &AWSError{Code: "InvalidParameterException", Message: "Username is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, cognitoIDPNamespace, cognitoUserKey(ctx.AccountID, ctx.Region, poolID, username))
	if err != nil {
		return nil, fmt.Errorf("cognito-idp loadUser state.Get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "UserNotFoundException", Message: "User not found: " + username, HTTPStatus: http.StatusNotFound}
	}
	var user CognitoUser
	if err := json.Unmarshal(data, &user); err != nil {
		return nil, fmt.Errorf("cognito-idp loadUser unmarshal: %w", err)
	}
	return &user, nil
}

// generateCognitoID generates a 12-character uppercase alphanumeric ID using
// crypto/rand for use as Cognito pool IDs, client IDs, and user sub values.
func generateCognitoID() string {
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	out := make([]byte, 12)
	for i, ch := range b {
		out[i] = chars[int(ch)%len(chars)]
	}
	return string(out)
}

// cognitoIDPJSONResponse serialises v as JSON and returns an AWSResponse with the
// given HTTP status code and application/json content type.
func cognitoIDPJSONResponse(status int, v any) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("cognitoIDPJSONResponse marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}
