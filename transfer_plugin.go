package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// TransferPlugin emulates the AWS Transfer Family service.
// It handles SFTP/FTP server and user CRUD operations using the
// Transfer Family JSON-target protocol (X-Amz-Target: TransferService.{Op}).
type TransferPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "transfer".
func (p *TransferPlugin) Name() string { return transferNamespace }

// Initialize sets up the TransferPlugin with the provided configuration.
func (p *TransferPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for TransferPlugin.
func (p *TransferPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a Transfer Family JSON-target request to the appropriate handler.
func (p *TransferPlugin) HandleRequest(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "CreateServer":
		return p.createServer(reqCtx, req)
	case "DescribeServer":
		return p.describeServer(reqCtx, req)
	case "UpdateServer":
		return p.updateServer(reqCtx, req)
	case "DeleteServer":
		return p.deleteServer(reqCtx, req)
	case "ListServers":
		return p.listServers(reqCtx, req)
	case "CreateUser":
		return p.createUser(reqCtx, req)
	case "DescribeUser":
		return p.describeUser(reqCtx, req)
	case "UpdateUser":
		return p.updateUser(reqCtx, req)
	case "DeleteUser":
		return p.deleteUser(reqCtx, req)
	case "ListUsers":
		return p.listUsers(reqCtx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "TransferPlugin: unsupported operation " + req.Operation,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

func (p *TransferPlugin) createServer(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Domain               string        `json:"Domain"`
		EndpointType         string        `json:"EndpointType"`
		IdentityProviderType string        `json:"IdentityProviderType"`
		Tags                 []TransferTag `json:"Tags"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.Domain == "" {
		input.Domain = "SFTP"
	}
	if input.EndpointType == "" {
		input.EndpointType = "PUBLIC"
	}

	serverID := generateTransferServerID()
	server := TransferServer{
		ServerId:             serverID,
		Arn:                  fmt.Sprintf("arn:aws:transfer:%s:%s:server/%s", reqCtx.Region, reqCtx.AccountID, serverID),
		Domain:               input.Domain,
		EndpointType:         input.EndpointType,
		IdentityProviderType: input.IdentityProviderType,
		State:                "ONLINE",
		Tags:                 input.Tags,
		CreatedAt:            p.tc.Now(),
		AccountID:            reqCtx.AccountID,
		Region:               reqCtx.Region,
	}

	goCtx := context.Background()
	data, err := json.Marshal(server)
	if err != nil {
		return nil, fmt.Errorf("transfer createServer marshal: %w", err)
	}
	key := transferServerKey(reqCtx.AccountID, reqCtx.Region, serverID)
	if err := p.state.Put(goCtx, transferNamespace, key, data); err != nil {
		return nil, fmt.Errorf("transfer createServer put: %w", err)
	}
	updateStringIndex(goCtx, p.state, transferNamespace, transferServerIDsKey(reqCtx.AccountID, reqCtx.Region), serverID)

	return transferJSONResponse(http.StatusOK, map[string]interface{}{
		"ServerId": serverID,
	})
}

func (p *TransferPlugin) describeServer(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ServerId string `json:"ServerId"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	server, err := p.loadServer(reqCtx.AccountID, reqCtx.Region, input.ServerId)
	if err != nil {
		return nil, err
	}
	return transferJSONResponse(http.StatusOK, map[string]interface{}{
		"Server": server,
	})
}

func (p *TransferPlugin) updateServer(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ServerId     string        `json:"ServerId"`
		EndpointType string        `json:"EndpointType"`
		Tags         []TransferTag `json:"Tags"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	server, err := p.loadServer(reqCtx.AccountID, reqCtx.Region, input.ServerId)
	if err != nil {
		return nil, err
	}

	if input.EndpointType != "" {
		server.EndpointType = input.EndpointType
	}
	if input.Tags != nil {
		server.Tags = input.Tags
	}

	goCtx := context.Background()
	data, err := json.Marshal(server)
	if err != nil {
		return nil, fmt.Errorf("transfer updateServer marshal: %w", err)
	}
	key := transferServerKey(reqCtx.AccountID, reqCtx.Region, server.ServerId)
	if err := p.state.Put(goCtx, transferNamespace, key, data); err != nil {
		return nil, fmt.Errorf("transfer updateServer put: %w", err)
	}

	return transferJSONResponse(http.StatusOK, map[string]interface{}{
		"ServerId": server.ServerId,
	})
}

func (p *TransferPlugin) deleteServer(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ServerId string `json:"ServerId"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if _, err := p.loadServer(reqCtx.AccountID, reqCtx.Region, input.ServerId); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	// Cascade-delete all users for this server.
	userNames, _ := loadStringIndex(goCtx, p.state, transferNamespace, transferUserNamesKey(reqCtx.AccountID, reqCtx.Region, input.ServerId))
	for _, userName := range userNames {
		userKey := transferUserKey(reqCtx.AccountID, reqCtx.Region, input.ServerId, userName)
		_ = p.state.Delete(goCtx, transferNamespace, userKey)
	}
	// Clear the user names index.
	_ = p.state.Delete(goCtx, transferNamespace, transferUserNamesKey(reqCtx.AccountID, reqCtx.Region, input.ServerId))

	// Delete the server.
	key := transferServerKey(reqCtx.AccountID, reqCtx.Region, input.ServerId)
	if err := p.state.Delete(goCtx, transferNamespace, key); err != nil {
		return nil, fmt.Errorf("transfer deleteServer delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, transferNamespace, transferServerIDsKey(reqCtx.AccountID, reqCtx.Region), input.ServerId)

	return transferJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *TransferPlugin) listServers(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	ids, err := loadStringIndex(goCtx, p.state, transferNamespace, transferServerIDsKey(reqCtx.AccountID, reqCtx.Region))
	if err != nil {
		return nil, fmt.Errorf("transfer listServers load index: %w", err)
	}
	summaries := make([]map[string]interface{}, 0, len(ids))
	for _, id := range ids {
		server, err := p.loadServer(reqCtx.AccountID, reqCtx.Region, id)
		if err != nil {
			continue
		}
		summaries = append(summaries, map[string]interface{}{
			"Arn":      server.Arn,
			"ServerId": server.ServerId,
			"Domain":   server.Domain,
			"State":    server.State,
		})
	}
	return transferJSONResponse(http.StatusOK, map[string]interface{}{
		"Servers":   summaries,
		"NextToken": "",
	})
}

func (p *TransferPlugin) createUser(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ServerId      string        `json:"ServerId"`
		UserName      string        `json:"UserName"`
		HomeDirectory string        `json:"HomeDirectory"`
		Role          string        `json:"Role"`
		Tags          []TransferTag `json:"Tags"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.ServerId == "" || input.UserName == "" {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "ServerId and UserName are required", HTTPStatus: http.StatusBadRequest}
	}

	// Verify server exists.
	if _, err := p.loadServer(reqCtx.AccountID, reqCtx.Region, input.ServerId); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	userKey := transferUserKey(reqCtx.AccountID, reqCtx.Region, input.ServerId, input.UserName)
	existing, err := p.state.Get(goCtx, transferNamespace, userKey)
	if err != nil {
		return nil, fmt.Errorf("transfer createUser get: %w", err)
	}
	if existing != nil {
		return nil, &AWSError{Code: "ConflictException", Message: "User " + input.UserName + " already exists on server " + input.ServerId + ".", HTTPStatus: http.StatusConflict}
	}

	user := TransferUser{
		UserName:      input.UserName,
		Arn:           fmt.Sprintf("arn:aws:transfer:%s:%s:user/%s/%s", reqCtx.Region, reqCtx.AccountID, input.ServerId, input.UserName),
		ServerId:      input.ServerId,
		HomeDirectory: input.HomeDirectory,
		Role:          input.Role,
		Tags:          input.Tags,
		AccountID:     reqCtx.AccountID,
		Region:        reqCtx.Region,
	}

	data, err := json.Marshal(user)
	if err != nil {
		return nil, fmt.Errorf("transfer createUser marshal: %w", err)
	}
	if err := p.state.Put(goCtx, transferNamespace, userKey, data); err != nil {
		return nil, fmt.Errorf("transfer createUser put: %w", err)
	}
	updateStringIndex(goCtx, p.state, transferNamespace, transferUserNamesKey(reqCtx.AccountID, reqCtx.Region, input.ServerId), input.UserName)

	return transferJSONResponse(http.StatusOK, map[string]interface{}{
		"ServerId": input.ServerId,
		"UserName": input.UserName,
	})
}

func (p *TransferPlugin) describeUser(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ServerId string `json:"ServerId"`
		UserName string `json:"UserName"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	user, err := p.loadUser(reqCtx.AccountID, reqCtx.Region, input.ServerId, input.UserName)
	if err != nil {
		return nil, err
	}
	return transferJSONResponse(http.StatusOK, map[string]interface{}{
		"ServerId": input.ServerId,
		"User":     user,
	})
}

func (p *TransferPlugin) updateUser(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ServerId      string `json:"ServerId"`
		UserName      string `json:"UserName"`
		HomeDirectory string `json:"HomeDirectory"`
		Role          string `json:"Role"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	user, err := p.loadUser(reqCtx.AccountID, reqCtx.Region, input.ServerId, input.UserName)
	if err != nil {
		return nil, err
	}

	if input.HomeDirectory != "" {
		user.HomeDirectory = input.HomeDirectory
	}
	if input.Role != "" {
		user.Role = input.Role
	}

	goCtx := context.Background()
	data, err := json.Marshal(user)
	if err != nil {
		return nil, fmt.Errorf("transfer updateUser marshal: %w", err)
	}
	key := transferUserKey(reqCtx.AccountID, reqCtx.Region, input.ServerId, input.UserName)
	if err := p.state.Put(goCtx, transferNamespace, key, data); err != nil {
		return nil, fmt.Errorf("transfer updateUser put: %w", err)
	}

	return transferJSONResponse(http.StatusOK, map[string]interface{}{
		"ServerId": input.ServerId,
		"UserName": input.UserName,
	})
}

func (p *TransferPlugin) deleteUser(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ServerId string `json:"ServerId"`
		UserName string `json:"UserName"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if _, err := p.loadUser(reqCtx.AccountID, reqCtx.Region, input.ServerId, input.UserName); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	key := transferUserKey(reqCtx.AccountID, reqCtx.Region, input.ServerId, input.UserName)
	if err := p.state.Delete(goCtx, transferNamespace, key); err != nil {
		return nil, fmt.Errorf("transfer deleteUser delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, transferNamespace, transferUserNamesKey(reqCtx.AccountID, reqCtx.Region, input.ServerId), input.UserName)

	return transferJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *TransferPlugin) listUsers(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ServerId string `json:"ServerId"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidRequestException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.ServerId == "" {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "ServerId is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	names, err := loadStringIndex(goCtx, p.state, transferNamespace, transferUserNamesKey(reqCtx.AccountID, reqCtx.Region, input.ServerId))
	if err != nil {
		return nil, fmt.Errorf("transfer listUsers load index: %w", err)
	}
	summaries := make([]map[string]interface{}, 0, len(names))
	for _, name := range names {
		user, err := p.loadUser(reqCtx.AccountID, reqCtx.Region, input.ServerId, name)
		if err != nil {
			continue
		}
		summaries = append(summaries, map[string]interface{}{
			"Arn":           user.Arn,
			"UserName":      user.UserName,
			"HomeDirectory": user.HomeDirectory,
			"Role":          user.Role,
		})
	}
	return transferJSONResponse(http.StatusOK, map[string]interface{}{
		"ServerId":  input.ServerId,
		"Users":     summaries,
		"NextToken": "",
	})
}

// loadServer loads a TransferServer from state or returns a not-found error.
func (p *TransferPlugin) loadServer(acct, region, serverID string) (*TransferServer, error) {
	if serverID == "" {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "ServerId is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	key := transferServerKey(acct, region, serverID)
	data, err := p.state.Get(goCtx, transferNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("transfer loadServer get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Server " + serverID + " does not exist.", HTTPStatus: http.StatusNotFound}
	}
	var server TransferServer
	if err := json.Unmarshal(data, &server); err != nil {
		return nil, fmt.Errorf("transfer loadServer unmarshal: %w", err)
	}
	return &server, nil
}

// loadUser loads a TransferUser from state or returns a not-found error.
func (p *TransferPlugin) loadUser(acct, region, serverID, userName string) (*TransferUser, error) {
	if serverID == "" || userName == "" {
		return nil, &AWSError{Code: "InvalidRequestException", Message: "ServerId and UserName are required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	key := transferUserKey(acct, region, serverID, userName)
	data, err := p.state.Get(goCtx, transferNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("transfer loadUser get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "User " + userName + " does not exist.", HTTPStatus: http.StatusNotFound}
	}
	var user TransferUser
	if err := json.Unmarshal(data, &user); err != nil {
		return nil, fmt.Errorf("transfer loadUser unmarshal: %w", err)
	}
	return &user, nil
}

// transferJSONResponse serializes v to JSON and returns an AWSResponse with
// Content-Type application/json.
func transferJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("transfer json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       body,
	}, nil
}
