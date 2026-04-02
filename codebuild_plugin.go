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

// CodeBuildPlugin emulates the AWS CodeBuild service.
// It handles build project CRUD and build execution operations using the
// CodeBuild JSON-target protocol (X-Amz-Target: CodeBuild_20161006.{Op}).
type CodeBuildPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "codebuild".
func (p *CodeBuildPlugin) Name() string { return codebuildNamespace }

// Initialize sets up the CodeBuildPlugin with the provided configuration.
func (p *CodeBuildPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for CodeBuildPlugin.
func (p *CodeBuildPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a CodeBuild JSON-target request to the appropriate handler.
func (p *CodeBuildPlugin) HandleRequest(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "CreateProject":
		return p.createProject(reqCtx, req)
	case "BatchGetProjects":
		return p.batchGetProjects(reqCtx, req)
	case "UpdateProject":
		return p.updateProject(reqCtx, req)
	case "DeleteProject":
		return p.deleteProject(reqCtx, req)
	case "ListProjects":
		return p.listProjects(reqCtx, req)
	case "StartBuild":
		return p.startBuild(reqCtx, req)
	case "BatchGetBuilds":
		return p.batchGetBuilds(reqCtx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "CodeBuildPlugin: unsupported operation " + req.Operation,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

func (p *CodeBuildPlugin) createProject(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name        string                 `json:"name"`
		Description string                 `json:"description"`
		Source      map[string]interface{} `json:"source"`
		Artifacts   map[string]interface{} `json:"artifacts"`
		Environment map[string]interface{} `json:"environment"`
		ServiceRole string                 `json:"serviceRole"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidInputException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.Name == "" {
		return nil, &AWSError{Code: "InvalidInputException", Message: "name is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	key := codebuildProjectKey(reqCtx.AccountID, reqCtx.Region, input.Name)
	existing, err := p.state.Get(goCtx, codebuildNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("codebuild createProject get: %w", err)
	}
	if existing != nil {
		return nil, &AWSError{Code: "ResourceAlreadyExistsException", Message: "Project " + input.Name + " already exists.", HTTPStatus: http.StatusBadRequest}
	}

	now := p.tc.Now()
	project := CodeBuildProject{
		Name:        input.Name,
		ARN:         fmt.Sprintf("arn:aws:codebuild:%s:%s:project/%s", reqCtx.Region, reqCtx.AccountID, input.Name),
		Description: input.Description,
		Source:      input.Source,
		Artifacts:   input.Artifacts,
		Environment: input.Environment,
		ServiceRole: input.ServiceRole,
		Created:     now,
		LastModified: now,
		AccountID:   reqCtx.AccountID,
		Region:      reqCtx.Region,
	}

	data, err := json.Marshal(project)
	if err != nil {
		return nil, fmt.Errorf("codebuild createProject marshal: %w", err)
	}
	if err := p.state.Put(goCtx, codebuildNamespace, key, data); err != nil {
		return nil, fmt.Errorf("codebuild createProject put: %w", err)
	}
	updateStringIndex(goCtx, p.state, codebuildNamespace, codebuildProjectNamesKey(reqCtx.AccountID, reqCtx.Region), input.Name)

	return codebuildJSONResponse(http.StatusOK, map[string]interface{}{
		"project": project,
	})
}

func (p *CodeBuildPlugin) batchGetProjects(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Names []string `json:"names"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidInputException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	projects := make([]CodeBuildProject, 0)
	notFound := make([]string, 0)
	for _, name := range input.Names {
		proj, err := p.loadProject(reqCtx.AccountID, reqCtx.Region, name)
		if err != nil {
			notFound = append(notFound, name)
			continue
		}
		projects = append(projects, *proj)
	}

	return codebuildJSONResponse(http.StatusOK, map[string]interface{}{
		"projects":         projects,
		"projectsNotFound": notFound,
	})
}

func (p *CodeBuildPlugin) updateProject(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Project struct {
			Name        string                 `json:"name"`
			Description string                 `json:"description"`
			Source      map[string]interface{} `json:"source"`
			Artifacts   map[string]interface{} `json:"artifacts"`
			Environment map[string]interface{} `json:"environment"`
			ServiceRole string                 `json:"serviceRole"`
		} `json:"project"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidInputException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	proj, err := p.loadProject(reqCtx.AccountID, reqCtx.Region, input.Project.Name)
	if err != nil {
		return nil, err
	}

	if input.Project.Description != "" {
		proj.Description = input.Project.Description
	}
	if input.Project.Source != nil {
		proj.Source = input.Project.Source
	}
	if input.Project.Artifacts != nil {
		proj.Artifacts = input.Project.Artifacts
	}
	if input.Project.Environment != nil {
		proj.Environment = input.Project.Environment
	}
	if input.Project.ServiceRole != "" {
		proj.ServiceRole = input.Project.ServiceRole
	}
	proj.LastModified = p.tc.Now()

	data, err := json.Marshal(proj)
	if err != nil {
		return nil, fmt.Errorf("codebuild updateProject marshal: %w", err)
	}

	goCtx := context.Background()
	key := codebuildProjectKey(reqCtx.AccountID, reqCtx.Region, proj.Name)
	if err := p.state.Put(goCtx, codebuildNamespace, key, data); err != nil {
		return nil, fmt.Errorf("codebuild updateProject put: %w", err)
	}

	return codebuildJSONResponse(http.StatusOK, map[string]interface{}{
		"project": proj,
	})
}

func (p *CodeBuildPlugin) deleteProject(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name string `json:"name"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidInputException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	if _, err := p.loadProject(reqCtx.AccountID, reqCtx.Region, input.Name); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	key := codebuildProjectKey(reqCtx.AccountID, reqCtx.Region, input.Name)
	if err := p.state.Delete(goCtx, codebuildNamespace, key); err != nil {
		return nil, fmt.Errorf("codebuild deleteProject delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, codebuildNamespace, codebuildProjectNamesKey(reqCtx.AccountID, reqCtx.Region), input.Name)

	return codebuildJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *CodeBuildPlugin) listProjects(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	names, err := loadStringIndex(goCtx, p.state, codebuildNamespace, codebuildProjectNamesKey(reqCtx.AccountID, reqCtx.Region))
	if err != nil {
		return nil, fmt.Errorf("codebuild listProjects load index: %w", err)
	}
	if names == nil {
		names = []string{}
	}
	return codebuildJSONResponse(http.StatusOK, map[string]interface{}{
		"projects": names,
	})
}

func (p *CodeBuildPlugin) startBuild(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ProjectName string `json:"projectName"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidInputException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.ProjectName == "" {
		return nil, &AWSError{Code: "InvalidInputException", Message: "projectName is required", HTTPStatus: http.StatusBadRequest}
	}

	if _, err := p.loadProject(reqCtx.AccountID, reqCtx.Region, input.ProjectName); err != nil {
		return nil, err
	}

	buildUUID := generateCodeBuildUUID()
	buildID := input.ProjectName + ":" + buildUUID
	now := p.tc.Now()

	build := CodeBuildBuild{
		ID:           buildID,
		ARN:          fmt.Sprintf("arn:aws:codebuild:%s:%s:build/%s", reqCtx.Region, reqCtx.AccountID, buildID),
		ProjectName:  input.ProjectName,
		BuildStatus:  "SUCCEEDED",
		StartTime:    now,
		EndTime:      now,
		CurrentPhase: "COMPLETED",
		AccountID:    reqCtx.AccountID,
		Region:       reqCtx.Region,
	}

	data, err := json.Marshal(build)
	if err != nil {
		return nil, fmt.Errorf("codebuild startBuild marshal: %w", err)
	}

	goCtx := context.Background()
	key := codebuildBuildKey(reqCtx.AccountID, reqCtx.Region, buildID)
	if err := p.state.Put(goCtx, codebuildNamespace, key, data); err != nil {
		return nil, fmt.Errorf("codebuild startBuild put: %w", err)
	}
	updateStringIndex(goCtx, p.state, codebuildNamespace, codebuildBuildIDsKey(reqCtx.AccountID, reqCtx.Region), buildID)

	return codebuildJSONResponse(http.StatusOK, map[string]interface{}{
		"build": build,
	})
}

func (p *CodeBuildPlugin) batchGetBuilds(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		IDs []string `json:"ids"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidInputException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	builds := make([]CodeBuildBuild, 0)
	notFound := make([]string, 0)
	goCtx := context.Background()
	for _, id := range input.IDs {
		key := codebuildBuildKey(reqCtx.AccountID, reqCtx.Region, id)
		data, err := p.state.Get(goCtx, codebuildNamespace, key)
		if err != nil || data == nil {
			notFound = append(notFound, id)
			continue
		}
		var b CodeBuildBuild
		if err := json.Unmarshal(data, &b); err != nil {
			notFound = append(notFound, id)
			continue
		}
		builds = append(builds, b)
	}

	return codebuildJSONResponse(http.StatusOK, map[string]interface{}{
		"builds":         builds,
		"buildsNotFound": notFound,
	})
}

// loadProject loads a CodeBuildProject from state by name or returns a not-found error.
func (p *CodeBuildPlugin) loadProject(acct, region, name string) (*CodeBuildProject, error) {
	if name == "" {
		return nil, &AWSError{Code: "InvalidInputException", Message: "name is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	key := codebuildProjectKey(acct, region, name)
	data, err := p.state.Get(goCtx, codebuildNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("codebuild loadProject get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ResourceNotFoundException", Message: "Project " + name + " does not exist.", HTTPStatus: http.StatusBadRequest}
	}
	var proj CodeBuildProject
	if err := json.Unmarshal(data, &proj); err != nil {
		return nil, fmt.Errorf("codebuild loadProject unmarshal: %w", err)
	}
	return &proj, nil
}

// State key helpers.
func codebuildProjectKey(acct, region, name string) string {
	return "project:" + acct + "/" + region + "/" + name
}

func codebuildProjectNamesKey(acct, region string) string {
	return "project_names:" + acct + "/" + region
}

func codebuildBuildKey(acct, region, buildID string) string {
	return "build:" + acct + "/" + region + "/" + buildID
}

func codebuildBuildIDsKey(acct, region string) string {
	return "build_ids:" + acct + "/" + region
}

// generateCodeBuildUUID generates a UUID-style string for build IDs.
func generateCodeBuildUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b[0:4]) + "-" +
		hex.EncodeToString(b[4:6]) + "-" +
		hex.EncodeToString(b[6:8]) + "-" +
		hex.EncodeToString(b[8:10]) + "-" +
		hex.EncodeToString(b[10:16])
}

// codebuildJSONResponse serializes v to JSON and returns an AWSResponse with
// Content-Type application/x-amz-json-1.1.
func codebuildJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("codebuild json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}
