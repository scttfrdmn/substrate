package substrate

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// CodeDeployPlugin emulates the AWS CodeDeploy service.
// It handles application, deployment group, and deployment CRUD operations using
// the CodeDeploy JSON-target protocol (X-Amz-Target: CodeDeploy_20141006.{Op}).
type CodeDeployPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "codedeploy".
func (p *CodeDeployPlugin) Name() string { return codedeployNamespace }

// Initialize sets up the CodeDeployPlugin with the provided configuration.
func (p *CodeDeployPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for CodeDeployPlugin.
func (p *CodeDeployPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a CodeDeploy JSON-target request to the appropriate handler.
func (p *CodeDeployPlugin) HandleRequest(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "CreateApplication":
		return p.createApplication(reqCtx, req)
	case "GetApplication":
		return p.getApplication(reqCtx, req)
	case "DeleteApplication":
		return p.deleteApplication(reqCtx, req)
	case "ListApplications":
		return p.listApplications(reqCtx, req)
	case "CreateDeploymentGroup":
		return p.createDeploymentGroup(reqCtx, req)
	case "GetDeploymentGroup":
		return p.getDeploymentGroup(reqCtx, req)
	case "DeleteDeploymentGroup":
		return p.deleteDeploymentGroup(reqCtx, req)
	case "CreateDeployment":
		return p.createDeployment(reqCtx, req)
	case "GetDeployment":
		return p.getDeployment(reqCtx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "CodeDeployPlugin: unsupported operation " + req.Operation,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

func (p *CodeDeployPlugin) createApplication(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ApplicationName string `json:"applicationName"`
		ComputePlatform string `json:"computePlatform"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidInputException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.ApplicationName == "" {
		return nil, &AWSError{Code: "InvalidInputException", Message: "applicationName is required", HTTPStatus: http.StatusBadRequest}
	}
	if input.ComputePlatform == "" {
		input.ComputePlatform = "Server"
	}

	goCtx := context.Background()
	key := codedeployAppKey(reqCtx.AccountID, reqCtx.Region, input.ApplicationName)
	existing, err := p.state.Get(goCtx, codedeployNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("codedeploy createApplication get: %w", err)
	}
	if existing != nil {
		return nil, &AWSError{Code: "ApplicationAlreadyExistsException", Message: "Application " + input.ApplicationName + " already exists.", HTTPStatus: http.StatusBadRequest}
	}

	appID := generateCodeDeployAppID()
	app := CodeDeployApp{
		ApplicationID:   appID,
		ApplicationName: input.ApplicationName,
		ComputePlatform: input.ComputePlatform,
		CreateTime:      p.tc.Now(),
		AccountID:       reqCtx.AccountID,
		Region:          reqCtx.Region,
	}

	data, err := json.Marshal(app)
	if err != nil {
		return nil, fmt.Errorf("codedeploy createApplication marshal: %w", err)
	}
	if err := p.state.Put(goCtx, codedeployNamespace, key, data); err != nil {
		return nil, fmt.Errorf("codedeploy createApplication put: %w", err)
	}
	updateStringIndex(goCtx, p.state, codedeployNamespace, codedeployAppNamesKey(reqCtx.AccountID, reqCtx.Region), input.ApplicationName)

	return codedeployJSONResponse(http.StatusOK, map[string]interface{}{
		"applicationId": appID,
	})
}

func (p *CodeDeployPlugin) getApplication(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ApplicationName string `json:"applicationName"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidInputException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	app, err := p.loadApp(reqCtx.AccountID, reqCtx.Region, input.ApplicationName)
	if err != nil {
		return nil, err
	}

	return codedeployJSONResponse(http.StatusOK, map[string]interface{}{
		"application": app,
	})
}

func (p *CodeDeployPlugin) deleteApplication(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ApplicationName string `json:"applicationName"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidInputException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	if _, err := p.loadApp(reqCtx.AccountID, reqCtx.Region, input.ApplicationName); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	key := codedeployAppKey(reqCtx.AccountID, reqCtx.Region, input.ApplicationName)
	if err := p.state.Delete(goCtx, codedeployNamespace, key); err != nil {
		return nil, fmt.Errorf("codedeploy deleteApplication delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, codedeployNamespace, codedeployAppNamesKey(reqCtx.AccountID, reqCtx.Region), input.ApplicationName)

	return codedeployJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *CodeDeployPlugin) listApplications(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	names, err := loadStringIndex(goCtx, p.state, codedeployNamespace, codedeployAppNamesKey(reqCtx.AccountID, reqCtx.Region))
	if err != nil {
		return nil, fmt.Errorf("codedeploy listApplications load index: %w", err)
	}
	if names == nil {
		names = []string{}
	}
	return codedeployJSONResponse(http.StatusOK, map[string]interface{}{
		"applications": names,
	})
}

func (p *CodeDeployPlugin) createDeploymentGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ApplicationName     string `json:"applicationName"`
		DeploymentGroupName string `json:"deploymentGroupName"`
		ServiceRoleArn      string `json:"serviceRoleArn"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidInputException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.ApplicationName == "" || input.DeploymentGroupName == "" {
		return nil, &AWSError{Code: "InvalidInputException", Message: "applicationName and deploymentGroupName are required", HTTPStatus: http.StatusBadRequest}
	}

	// Verify the application exists.
	if _, err := p.loadApp(reqCtx.AccountID, reqCtx.Region, input.ApplicationName); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	key := codedeployGroupKey(reqCtx.AccountID, reqCtx.Region, input.ApplicationName, input.DeploymentGroupName)
	existing, err := p.state.Get(goCtx, codedeployNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("codedeploy createDeploymentGroup get: %w", err)
	}
	if existing != nil {
		return nil, &AWSError{Code: "DeploymentGroupAlreadyExistsException", Message: "Deployment group " + input.DeploymentGroupName + " already exists.", HTTPStatus: http.StatusBadRequest}
	}

	groupID := generateCodeDeployGroupID()
	group := CodeDeployGroup{
		DeploymentGroupID:   groupID,
		DeploymentGroupName: input.DeploymentGroupName,
		ApplicationName:     input.ApplicationName,
		ServiceRoleArn:      input.ServiceRoleArn,
		AccountID:           reqCtx.AccountID,
		Region:              reqCtx.Region,
	}

	data, err := json.Marshal(group)
	if err != nil {
		return nil, fmt.Errorf("codedeploy createDeploymentGroup marshal: %w", err)
	}
	if err := p.state.Put(goCtx, codedeployNamespace, key, data); err != nil {
		return nil, fmt.Errorf("codedeploy createDeploymentGroup put: %w", err)
	}
	updateStringIndex(goCtx, p.state, codedeployNamespace, codedeployGroupNamesKey(reqCtx.AccountID, reqCtx.Region, input.ApplicationName), input.DeploymentGroupName)

	return codedeployJSONResponse(http.StatusOK, map[string]interface{}{
		"deploymentGroupId": groupID,
	})
}

func (p *CodeDeployPlugin) getDeploymentGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ApplicationName     string `json:"applicationName"`
		DeploymentGroupName string `json:"deploymentGroupName"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidInputException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	group, err := p.loadGroup(reqCtx.AccountID, reqCtx.Region, input.ApplicationName, input.DeploymentGroupName)
	if err != nil {
		return nil, err
	}

	return codedeployJSONResponse(http.StatusOK, map[string]interface{}{
		"deploymentGroupInfo": group,
	})
}

func (p *CodeDeployPlugin) deleteDeploymentGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ApplicationName     string `json:"applicationName"`
		DeploymentGroupName string `json:"deploymentGroupName"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidInputException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	if _, err := p.loadGroup(reqCtx.AccountID, reqCtx.Region, input.ApplicationName, input.DeploymentGroupName); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	key := codedeployGroupKey(reqCtx.AccountID, reqCtx.Region, input.ApplicationName, input.DeploymentGroupName)
	if err := p.state.Delete(goCtx, codedeployNamespace, key); err != nil {
		return nil, fmt.Errorf("codedeploy deleteDeploymentGroup delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, codedeployNamespace, codedeployGroupNamesKey(reqCtx.AccountID, reqCtx.Region, input.ApplicationName), input.DeploymentGroupName)

	return codedeployJSONResponse(http.StatusOK, map[string]interface{}{
		"hooksNotCleanedUp": []interface{}{},
	})
}

func (p *CodeDeployPlugin) createDeployment(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ApplicationName     string `json:"applicationName"`
		DeploymentGroupName string `json:"deploymentGroupName"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidInputException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.ApplicationName == "" {
		return nil, &AWSError{Code: "InvalidInputException", Message: "applicationName is required", HTTPStatus: http.StatusBadRequest}
	}

	// Verify both application and deployment group exist.
	if _, err := p.loadApp(reqCtx.AccountID, reqCtx.Region, input.ApplicationName); err != nil {
		return nil, err
	}
	if input.DeploymentGroupName != "" {
		if _, err := p.loadGroup(reqCtx.AccountID, reqCtx.Region, input.ApplicationName, input.DeploymentGroupName); err != nil {
			return nil, err
		}
	}

	deploymentID := generateCodeDeployDeploymentID()
	now := p.tc.Now()
	deployment := CodeDeployDeployment{
		DeploymentID:        deploymentID,
		ApplicationName:     input.ApplicationName,
		DeploymentGroupName: input.DeploymentGroupName,
		Status:              "Succeeded",
		CreateTime:          now,
		CompleteTime:        now,
		AccountID:           reqCtx.AccountID,
		Region:              reqCtx.Region,
	}

	data, err := json.Marshal(deployment)
	if err != nil {
		return nil, fmt.Errorf("codedeploy createDeployment marshal: %w", err)
	}

	goCtx := context.Background()
	key := codedeployDeploymentKey(reqCtx.AccountID, reqCtx.Region, deploymentID)
	if err := p.state.Put(goCtx, codedeployNamespace, key, data); err != nil {
		return nil, fmt.Errorf("codedeploy createDeployment put: %w", err)
	}

	return codedeployJSONResponse(http.StatusOK, map[string]interface{}{
		"deploymentId": deploymentID,
	})
}

func (p *CodeDeployPlugin) getDeployment(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		DeploymentID string `json:"deploymentId"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidInputException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	goCtx := context.Background()
	key := codedeployDeploymentKey(reqCtx.AccountID, reqCtx.Region, input.DeploymentID)
	data, err := p.state.Get(goCtx, codedeployNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("codedeploy getDeployment get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "DeploymentDoesNotExistException", Message: "Deployment " + input.DeploymentID + " does not exist.", HTTPStatus: http.StatusBadRequest}
	}

	var deployment CodeDeployDeployment
	if err := json.Unmarshal(data, &deployment); err != nil {
		return nil, fmt.Errorf("codedeploy getDeployment unmarshal: %w", err)
	}

	return codedeployJSONResponse(http.StatusOK, map[string]interface{}{
		"deploymentInfo": deployment,
	})
}

// loadApp loads a CodeDeployApp from state by name or returns a not-found error.
func (p *CodeDeployPlugin) loadApp(acct, region, name string) (*CodeDeployApp, error) {
	if name == "" {
		return nil, &AWSError{Code: "InvalidInputException", Message: "applicationName is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	key := codedeployAppKey(acct, region, name)
	data, err := p.state.Get(goCtx, codedeployNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("codedeploy loadApp get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ApplicationDoesNotExistException", Message: "Application " + name + " does not exist.", HTTPStatus: http.StatusBadRequest}
	}
	var app CodeDeployApp
	if err := json.Unmarshal(data, &app); err != nil {
		return nil, fmt.Errorf("codedeploy loadApp unmarshal: %w", err)
	}
	return &app, nil
}

// loadGroup loads a CodeDeployGroup from state or returns a not-found error.
func (p *CodeDeployPlugin) loadGroup(acct, region, appName, groupName string) (*CodeDeployGroup, error) {
	if groupName == "" {
		return nil, &AWSError{Code: "InvalidInputException", Message: "deploymentGroupName is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	key := codedeployGroupKey(acct, region, appName, groupName)
	data, err := p.state.Get(goCtx, codedeployNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("codedeploy loadGroup get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "DeploymentGroupDoesNotExistException", Message: "Deployment group " + groupName + " does not exist.", HTTPStatus: http.StatusBadRequest}
	}
	var group CodeDeployGroup
	if err := json.Unmarshal(data, &group); err != nil {
		return nil, fmt.Errorf("codedeploy loadGroup unmarshal: %w", err)
	}
	return &group, nil
}

// State key helpers.
func codedeployAppKey(acct, region, name string) string {
	return "app:" + acct + "/" + region + "/" + name
}

func codedeployAppNamesKey(acct, region string) string {
	return "app_names:" + acct + "/" + region
}

func codedeployGroupKey(acct, region, appName, groupName string) string {
	return "group:" + acct + "/" + region + "/" + appName + "/" + groupName
}

func codedeployGroupNamesKey(acct, region, appName string) string {
	return "group_names:" + acct + "/" + region + "/" + appName
}

func codedeployDeploymentKey(acct, region, deployID string) string {
	return "deployment:" + acct + "/" + region + "/" + deployID
}

// generateCodeDeployAppID generates a UUID-style ID for CodeDeploy applications.
func generateCodeDeployAppID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b[0:4]) + "-" +
		hex.EncodeToString(b[4:6]) + "-" +
		hex.EncodeToString(b[6:8]) + "-" +
		hex.EncodeToString(b[8:10]) + "-" +
		hex.EncodeToString(b[10:16])
}

// generateCodeDeployGroupID generates a UUID-style ID for CodeDeploy deployment groups.
func generateCodeDeployGroupID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b[0:4]) + "-" +
		hex.EncodeToString(b[4:6]) + "-" +
		hex.EncodeToString(b[6:8]) + "-" +
		hex.EncodeToString(b[8:10]) + "-" +
		hex.EncodeToString(b[10:16])
}

// codedeployJSONResponse serializes v to JSON and returns an AWSResponse with
// Content-Type application/x-amz-json-1.1.
func codedeployJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("codedeploy json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}
