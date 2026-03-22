package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// sagemakerNamespace is the state namespace for Amazon SageMaker.
const sagemakerNamespace = "sagemaker"

// SageMakerPlugin emulates the Amazon SageMaker service.
// It handles Studio app lifecycle (CreateApp, DeleteApp, DescribeApp, ListApps,
// ListDomains, CreatePresignedDomainUrl) and training job operations
// (CreateTrainingJob, DescribeTrainingJob, StopTrainingJob, ListTrainingJobs)
// using the SageMaker JSON-target protocol (X-Amz-Target: SageMaker.{Op}).
type SageMakerPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "sagemaker".
func (p *SageMakerPlugin) Name() string { return sagemakerNamespace }

// Initialize sets up the SageMakerPlugin with the provided configuration.
func (p *SageMakerPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for SageMakerPlugin.
func (p *SageMakerPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a SageMaker JSON-target request to the appropriate handler.
func (p *SageMakerPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "ListDomains":
		return p.listDomains()
	case "ListApps":
		return p.listApps(ctx, req)
	case "CreateApp":
		return p.createApp(ctx, req)
	case "DeleteApp":
		return p.deleteApp(ctx, req)
	case "DescribeApp":
		return p.describeApp(ctx, req)
	case "CreatePresignedDomainUrl":
		return p.createPresignedDomainURL()
	case "CreateTrainingJob":
		return p.createTrainingJob(ctx, req)
	case "DescribeTrainingJob":
		return p.describeTrainingJob(ctx, req)
	case "StopTrainingJob":
		return p.stopTrainingJob(ctx, req)
	case "ListTrainingJobs":
		return p.listTrainingJobs(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "SageMakerPlugin: unsupported operation " + req.Operation,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// SageMakerApp holds persisted state for a SageMaker Studio app.
type SageMakerApp struct {
	// AppArn is the ARN of the app.
	AppArn string `json:"AppArn"`

	// AppName is the user-supplied name.
	AppName string `json:"AppName"`

	// AppType is the type of app (e.g. "JupyterServer", "KernelGateway").
	AppType string `json:"AppType"`

	// DomainId is the domain the app belongs to.
	DomainId string `json:"DomainId"`

	// UserProfileName is the user profile that owns the app.
	UserProfileName string `json:"UserProfileName"`

	// Status is the app status (InService, Deleted).
	Status string `json:"Status"`

	// AccountID is the AWS account that owns the app.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where the app runs.
	Region string `json:"Region"`
}

// SageMakerTrainingJob holds persisted state for a SageMaker training job.
type SageMakerTrainingJob struct {
	// TrainingJobName is the user-supplied name.
	TrainingJobName string `json:"TrainingJobName"`

	// TrainingJobArn is the ARN for the training job.
	TrainingJobArn string `json:"TrainingJobArn"`

	// TrainingJobStatus is the job status (InProgress, Completed, Stopped, Failed).
	TrainingJobStatus string `json:"TrainingJobStatus"`

	// CreationTime is the epoch-seconds timestamp when the job was created.
	CreationTime float64 `json:"CreationTime"`

	// AccountID is the AWS account that owns the job.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where the job runs.
	Region string `json:"Region"`
}

func (p *SageMakerPlugin) listDomains() (*AWSResponse, error) {
	return sagemakerJSONResponse(http.StatusOK, map[string]interface{}{"Domains": []interface{}{}})
}

func (p *SageMakerPlugin) listApps(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		DomainIdEquals      string `json:"DomainIdEquals"`
		UserProfileNameEquals string `json:"UserProfileNameEquals"`
	}
	_ = json.Unmarshal(req.Body, &body)

	goCtx := context.Background()
	keysKey := "app_keys:" + ctx.AccountID + "/" + ctx.Region
	keys, _ := loadStringIndex(goCtx, p.state, sagemakerNamespace, keysKey)

	apps := make([]SageMakerApp, 0)
	for _, k := range keys {
		data, err := p.state.Get(goCtx, sagemakerNamespace, k)
		if err != nil || data == nil {
			continue
		}
		var app SageMakerApp
		if json.Unmarshal(data, &app) != nil {
			continue
		}
		if body.DomainIdEquals != "" && app.DomainId != body.DomainIdEquals {
			continue
		}
		if body.UserProfileNameEquals != "" && app.UserProfileName != body.UserProfileNameEquals {
			continue
		}
		apps = append(apps, app)
	}
	return sagemakerJSONResponse(http.StatusOK, map[string]interface{}{"Apps": apps})
}

func (p *SageMakerPlugin) createApp(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		AppName         string `json:"AppName"`
		AppType         string `json:"AppType"`
		DomainId        string `json:"DomainId"`
		UserProfileName string `json:"UserProfileName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil || body.AppName == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "AppName is required", HTTPStatus: http.StatusBadRequest}
	}

	appArn := fmt.Sprintf("arn:aws:sagemaker:%s:%s:app/%s/%s/%s/%s",
		ctx.Region, ctx.AccountID, body.DomainId, body.UserProfileName, body.AppType, body.AppName)
	app := SageMakerApp{
		AppArn:          appArn,
		AppName:         body.AppName,
		AppType:         body.AppType,
		DomainId:        body.DomainId,
		UserProfileName: body.UserProfileName,
		Status:          "InService",
		AccountID:       ctx.AccountID,
		Region:          ctx.Region,
	}

	goCtx := context.Background()
	appKey := fmt.Sprintf("app:%s/%s/%s/%s/%s/%s",
		ctx.AccountID, ctx.Region, body.DomainId, body.UserProfileName, body.AppType, body.AppName)
	data, err := json.Marshal(app)
	if err != nil {
		return nil, fmt.Errorf("createApp: marshal: %w", err)
	}
	if err := p.state.Put(goCtx, sagemakerNamespace, appKey, data); err != nil {
		return nil, fmt.Errorf("createApp: put: %w", err)
	}
	keysKey := "app_keys:" + ctx.AccountID + "/" + ctx.Region
	updateStringIndex(goCtx, p.state, sagemakerNamespace, keysKey, appKey)
	return sagemakerJSONResponse(http.StatusOK, map[string]string{"AppArn": appArn})
}

func (p *SageMakerPlugin) deleteApp(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		AppName         string `json:"AppName"`
		AppType         string `json:"AppType"`
		DomainId        string `json:"DomainId"`
		UserProfileName string `json:"UserProfileName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	appKey := fmt.Sprintf("app:%s/%s/%s/%s/%s/%s",
		ctx.AccountID, ctx.Region, body.DomainId, body.UserProfileName, body.AppType, body.AppName)
	if err := p.state.Delete(goCtx, sagemakerNamespace, appKey); err != nil {
		return nil, fmt.Errorf("deleteApp: %w", err)
	}
	keysKey := "app_keys:" + ctx.AccountID + "/" + ctx.Region
	removeFromStringIndex(goCtx, p.state, sagemakerNamespace, keysKey, appKey)
	return sagemakerJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *SageMakerPlugin) describeApp(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		AppName         string `json:"AppName"`
		AppType         string `json:"AppType"`
		DomainId        string `json:"DomainId"`
		UserProfileName string `json:"UserProfileName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "invalid request body", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	appKey := fmt.Sprintf("app:%s/%s/%s/%s/%s/%s",
		ctx.AccountID, ctx.Region, body.DomainId, body.UserProfileName, body.AppType, body.AppName)
	data, err := p.state.Get(goCtx, sagemakerNamespace, appKey)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "ResourceNotFound", Message: "app " + body.AppName + " not found", HTTPStatus: http.StatusBadRequest}
	}
	var app SageMakerApp
	if err := json.Unmarshal(data, &app); err != nil {
		return nil, fmt.Errorf("describeApp: unmarshal: %w", err)
	}
	return sagemakerJSONResponse(http.StatusOK, app)
}

func (p *SageMakerPlugin) createPresignedDomainURL() (*AWSResponse, error) {
	return sagemakerJSONResponse(http.StatusOK, map[string]string{
		"AuthorizedUrl": "https://stub.studio.sagemaker.aws/auth?token=stub",
	})
}

func (p *SageMakerPlugin) createTrainingJob(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		TrainingJobName string `json:"TrainingJobName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil || body.TrainingJobName == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "TrainingJobName is required", HTTPStatus: http.StatusBadRequest}
	}

	jobArn := fmt.Sprintf("arn:aws:sagemaker:%s:%s:training-job/%s", ctx.Region, ctx.AccountID, body.TrainingJobName)
	job := SageMakerTrainingJob{
		TrainingJobName:   body.TrainingJobName,
		TrainingJobArn:    jobArn,
		TrainingJobStatus: "Completed",
		CreationTime:      float64(p.tc.Now().UnixNano()) / 1e9,
		AccountID:         ctx.AccountID,
		Region:            ctx.Region,
	}

	goCtx := context.Background()
	jobKey := "trainingjob:" + ctx.AccountID + "/" + ctx.Region + "/" + body.TrainingJobName
	data, err := json.Marshal(job)
	if err != nil {
		return nil, fmt.Errorf("createTrainingJob: marshal: %w", err)
	}
	if err := p.state.Put(goCtx, sagemakerNamespace, jobKey, data); err != nil {
		return nil, fmt.Errorf("createTrainingJob: put: %w", err)
	}
	namesKey := "trainingjob_names:" + ctx.AccountID + "/" + ctx.Region
	updateStringIndex(goCtx, p.state, sagemakerNamespace, namesKey, body.TrainingJobName)
	return sagemakerJSONResponse(http.StatusOK, map[string]string{"TrainingJobArn": jobArn})
}

func (p *SageMakerPlugin) describeTrainingJob(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		TrainingJobName string `json:"TrainingJobName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil || body.TrainingJobName == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "TrainingJobName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	jobKey := "trainingjob:" + ctx.AccountID + "/" + ctx.Region + "/" + body.TrainingJobName
	data, err := p.state.Get(goCtx, sagemakerNamespace, jobKey)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "ResourceNotFound", Message: "training job " + body.TrainingJobName + " not found", HTTPStatus: http.StatusBadRequest}
	}
	var job SageMakerTrainingJob
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, fmt.Errorf("describeTrainingJob: unmarshal: %w", err)
	}
	return sagemakerJSONResponse(http.StatusOK, job)
}

func (p *SageMakerPlugin) stopTrainingJob(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var body struct {
		TrainingJobName string `json:"TrainingJobName"`
	}
	if err := json.Unmarshal(req.Body, &body); err != nil || body.TrainingJobName == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "TrainingJobName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	jobKey := "trainingjob:" + ctx.AccountID + "/" + ctx.Region + "/" + body.TrainingJobName
	data, err := p.state.Get(goCtx, sagemakerNamespace, jobKey)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "ResourceNotFound", Message: "training job " + body.TrainingJobName + " not found", HTTPStatus: http.StatusBadRequest}
	}
	var job SageMakerTrainingJob
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, fmt.Errorf("stopTrainingJob: unmarshal: %w", err)
	}
	job.TrainingJobStatus = "Stopped"
	updated, _ := json.Marshal(job)
	if err := p.state.Put(goCtx, sagemakerNamespace, jobKey, updated); err != nil {
		return nil, fmt.Errorf("stopTrainingJob: put: %w", err)
	}
	return sagemakerJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *SageMakerPlugin) listTrainingJobs(ctx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	namesKey := "trainingjob_names:" + ctx.AccountID + "/" + ctx.Region
	names, _ := loadStringIndex(goCtx, p.state, sagemakerNamespace, namesKey)

	type summary struct {
		TrainingJobName   string  `json:"TrainingJobName"`
		TrainingJobArn    string  `json:"TrainingJobArn"`
		TrainingJobStatus string  `json:"TrainingJobStatus"`
		CreationTime      float64 `json:"CreationTime"`
	}
	summaries := make([]summary, 0, len(names))
	for _, name := range names {
		key := "trainingjob:" + ctx.AccountID + "/" + ctx.Region + "/" + name
		data, err := p.state.Get(goCtx, sagemakerNamespace, key)
		if err != nil || data == nil {
			continue
		}
		var job SageMakerTrainingJob
		if json.Unmarshal(data, &job) == nil {
			summaries = append(summaries, summary{
				TrainingJobName:   job.TrainingJobName,
				TrainingJobArn:    job.TrainingJobArn,
				TrainingJobStatus: job.TrainingJobStatus,
				CreationTime:      job.CreationTime,
			})
		}
	}
	return sagemakerJSONResponse(http.StatusOK, map[string]interface{}{"TrainingJobSummaries": summaries})
}

// sagemakerJSONResponse serializes v to JSON and returns an AWSResponse.
func sagemakerJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("sagemaker json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}
