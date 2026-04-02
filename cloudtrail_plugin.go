package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// CloudTrailPlugin emulates the AWS CloudTrail service.
// It handles trail CRUD operations, logging control, and trail status queries.
// Protocol: JSON-target CloudTrail_20131101.{Op} — parser strips the version
// suffix so no targetServiceAliases entry is needed.
type CloudTrailPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "cloudtrail".
func (p *CloudTrailPlugin) Name() string { return cloudtrailNamespace }

// Initialize sets up the CloudTrailPlugin with the provided configuration.
func (p *CloudTrailPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for CloudTrailPlugin.
func (p *CloudTrailPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a CloudTrail JSON-target request to the appropriate handler.
func (p *CloudTrailPlugin) HandleRequest(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "CreateTrail":
		return p.createTrail(reqCtx, req)
	case "GetTrail":
		return p.getTrail(reqCtx, req)
	case "GetTrailStatus":
		return p.getTrailStatus(reqCtx, req)
	case "UpdateTrail":
		return p.updateTrail(reqCtx, req)
	case "DeleteTrail":
		return p.deleteTrail(reqCtx, req)
	case "DescribeTrails":
		return p.describeTrails(reqCtx, req)
	case "StartLogging":
		return p.startLogging(reqCtx, req)
	case "StopLogging":
		return p.stopLogging(reqCtx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "CloudTrailPlugin: unsupported operation " + req.Operation,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// State key helpers.
func cloudtrailTrailKey(acct, region, name string) string {
	return "trail:" + acct + "/" + region + "/" + name
}

func cloudtrailTrailNamesKey(acct, region string) string {
	return "trail_names:" + acct + "/" + region
}

func (p *CloudTrailPlugin) createTrail(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name                       string `json:"Name"`
		S3BucketName               string `json:"S3BucketName"`
		S3KeyPrefix                string `json:"S3KeyPrefix"`
		IncludeGlobalServiceEvents bool   `json:"IncludeGlobalServiceEvents"`
		IsMultiRegionTrail         bool   `json:"IsMultiRegionTrail"`
		EnableLogFileValidation    bool   `json:"EnableLogFileValidation"`
		CloudWatchLogsLogGroupArn  string `json:"CloudWatchLogsLogGroupArn"`
		CloudWatchLogsRoleArn      string `json:"CloudWatchLogsRoleArn"`
		KMSKeyID                   string `json:"KmsKeyId"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidParameterCombinationException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.Name == "" {
		return nil, &AWSError{Code: "InvalidTrailNameException", Message: "Trail name is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	key := cloudtrailTrailKey(reqCtx.AccountID, reqCtx.Region, input.Name)
	existing, err := p.state.Get(goCtx, cloudtrailNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("cloudtrail createTrail get: %w", err)
	}
	if existing != nil {
		return nil, &AWSError{Code: "TrailAlreadyExistsException", Message: "Trail " + input.Name + " already exists.", HTTPStatus: http.StatusBadRequest}
	}

	arn := fmt.Sprintf("arn:aws:cloudtrail:%s:%s:trail/%s", reqCtx.Region, reqCtx.AccountID, input.Name)

	trail := CloudTrailTrail{
		Name:                       input.Name,
		S3BucketName:               input.S3BucketName,
		S3KeyPrefix:                input.S3KeyPrefix,
		IncludeGlobalServiceEvents: input.IncludeGlobalServiceEvents,
		IsMultiRegionTrail:         input.IsMultiRegionTrail,
		EnableLogFileValidation:    input.EnableLogFileValidation,
		CloudWatchLogsLogGroupArn:  input.CloudWatchLogsLogGroupArn,
		CloudWatchLogsRoleArn:      input.CloudWatchLogsRoleArn,
		KMSKeyID:                   input.KMSKeyID,
		TrailARN:                   arn,
		HomeRegion:                 reqCtx.Region,
		IsLogging:                  true,
		CreatedAt:                  p.tc.Now(),
		AccountID:                  reqCtx.AccountID,
		Region:                     reqCtx.Region,
	}

	data, err := json.Marshal(trail)
	if err != nil {
		return nil, fmt.Errorf("cloudtrail createTrail marshal: %w", err)
	}
	if err := p.state.Put(goCtx, cloudtrailNamespace, key, data); err != nil {
		return nil, fmt.Errorf("cloudtrail createTrail put: %w", err)
	}
	updateStringIndex(goCtx, p.state, cloudtrailNamespace, cloudtrailTrailNamesKey(reqCtx.AccountID, reqCtx.Region), input.Name)

	return cloudtrailJSONResponse(http.StatusOK, trail)
}

func (p *CloudTrailPlugin) getTrail(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name string `json:"Name"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidParameterCombinationException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	trail, err := p.loadTrail(reqCtx.AccountID, reqCtx.Region, input.Name)
	if err != nil {
		return nil, err
	}

	return cloudtrailJSONResponse(http.StatusOK, map[string]interface{}{
		"Trail": trail,
	})
}

func (p *CloudTrailPlugin) getTrailStatus(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name string `json:"Name"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidParameterCombinationException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	// Verify the trail exists.
	if _, err := p.loadTrail(reqCtx.AccountID, reqCtx.Region, input.Name); err != nil {
		return nil, err
	}

	now := p.tc.Now().Unix()
	return cloudtrailJSONResponse(http.StatusOK, map[string]interface{}{
		"IsLogging":                          true,
		"LatestDeliveryTime":                 now,
		"LatestDeliveryAttemptTime":          "",
		"LatestDeliveryAttemptSucceeded":     "",
		"LatestNotificationAttemptTime":      "",
		"LatestNotificationAttemptSucceeded": "",
		"TimeLoggingStarted":                 "",
		"TimeLoggingStopped":                 "",
	})
}

func (p *CloudTrailPlugin) updateTrail(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name                       string `json:"Name"`
		S3BucketName               string `json:"S3BucketName"`
		S3KeyPrefix                string `json:"S3KeyPrefix"`
		IncludeGlobalServiceEvents *bool  `json:"IncludeGlobalServiceEvents"`
		IsMultiRegionTrail         *bool  `json:"IsMultiRegionTrail"`
		EnableLogFileValidation    *bool  `json:"EnableLogFileValidation"`
		CloudWatchLogsLogGroupArn  string `json:"CloudWatchLogsLogGroupArn"`
		CloudWatchLogsRoleArn      string `json:"CloudWatchLogsRoleArn"`
		KMSKeyID                   string `json:"KmsKeyId"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidParameterCombinationException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	trail, err := p.loadTrail(reqCtx.AccountID, reqCtx.Region, input.Name)
	if err != nil {
		return nil, err
	}

	if input.S3BucketName != "" {
		trail.S3BucketName = input.S3BucketName
	}
	if input.S3KeyPrefix != "" {
		trail.S3KeyPrefix = input.S3KeyPrefix
	}
	if input.IncludeGlobalServiceEvents != nil {
		trail.IncludeGlobalServiceEvents = *input.IncludeGlobalServiceEvents
	}
	if input.IsMultiRegionTrail != nil {
		trail.IsMultiRegionTrail = *input.IsMultiRegionTrail
	}
	if input.EnableLogFileValidation != nil {
		trail.EnableLogFileValidation = *input.EnableLogFileValidation
	}
	if input.CloudWatchLogsLogGroupArn != "" {
		trail.CloudWatchLogsLogGroupArn = input.CloudWatchLogsLogGroupArn
	}
	if input.CloudWatchLogsRoleArn != "" {
		trail.CloudWatchLogsRoleArn = input.CloudWatchLogsRoleArn
	}
	if input.KMSKeyID != "" {
		trail.KMSKeyID = input.KMSKeyID
	}

	data, err := json.Marshal(trail)
	if err != nil {
		return nil, fmt.Errorf("cloudtrail updateTrail marshal: %w", err)
	}

	goCtx := context.Background()
	key := cloudtrailTrailKey(reqCtx.AccountID, reqCtx.Region, trail.Name)
	if err := p.state.Put(goCtx, cloudtrailNamespace, key, data); err != nil {
		return nil, fmt.Errorf("cloudtrail updateTrail put: %w", err)
	}

	return cloudtrailJSONResponse(http.StatusOK, trail)
}

func (p *CloudTrailPlugin) deleteTrail(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		Name string `json:"Name"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidParameterCombinationException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	// Verify the trail exists before deleting.
	if _, err := p.loadTrail(reqCtx.AccountID, reqCtx.Region, input.Name); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	key := cloudtrailTrailKey(reqCtx.AccountID, reqCtx.Region, input.Name)
	if err := p.state.Delete(goCtx, cloudtrailNamespace, key); err != nil {
		return nil, fmt.Errorf("cloudtrail deleteTrail delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, cloudtrailNamespace, cloudtrailTrailNamesKey(reqCtx.AccountID, reqCtx.Region), input.Name)

	return cloudtrailJSONResponse(http.StatusOK, map[string]interface{}{})
}

func (p *CloudTrailPlugin) describeTrails(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		IncludeShadowTrails bool     `json:"includeShadowTrails"`
		TrailNameList       []string `json:"trailNameList"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
	}

	goCtx := context.Background()
	names, err := loadStringIndex(goCtx, p.state, cloudtrailNamespace, cloudtrailTrailNamesKey(reqCtx.AccountID, reqCtx.Region))
	if err != nil {
		return nil, fmt.Errorf("cloudtrail describeTrails load index: %w", err)
	}

	// Filter by trailNameList if provided.
	nameSet := make(map[string]bool, len(input.TrailNameList))
	for _, n := range input.TrailNameList {
		nameSet[n] = true
	}

	trails := make([]CloudTrailTrail, 0, len(names))
	for _, name := range names {
		if len(nameSet) > 0 && !nameSet[name] {
			continue
		}
		trail, err := p.loadTrail(reqCtx.AccountID, reqCtx.Region, name)
		if err != nil {
			continue
		}
		trails = append(trails, *trail)
	}

	return cloudtrailJSONResponse(http.StatusOK, map[string]interface{}{
		"trailList": trails,
	})
}

func (p *CloudTrailPlugin) startLogging(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.setLogging(reqCtx, req, true)
}

func (p *CloudTrailPlugin) stopLogging(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.setLogging(reqCtx, req, false)
}

func (p *CloudTrailPlugin) setLogging(reqCtx *RequestContext, req *AWSRequest, enabled bool) (*AWSResponse, error) {
	var input struct {
		Name string `json:"Name"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "InvalidParameterCombinationException", Message: "invalid JSON: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	trail, err := p.loadTrail(reqCtx.AccountID, reqCtx.Region, input.Name)
	if err != nil {
		return nil, err
	}

	trail.IsLogging = enabled

	data, err := json.Marshal(trail)
	if err != nil {
		return nil, fmt.Errorf("cloudtrail setLogging marshal: %w", err)
	}

	goCtx := context.Background()
	key := cloudtrailTrailKey(reqCtx.AccountID, reqCtx.Region, trail.Name)
	if err := p.state.Put(goCtx, cloudtrailNamespace, key, data); err != nil {
		return nil, fmt.Errorf("cloudtrail setLogging put: %w", err)
	}

	return cloudtrailJSONResponse(http.StatusOK, map[string]interface{}{})
}

// loadTrail loads a CloudTrailTrail from state by name or returns a not-found error.
func (p *CloudTrailPlugin) loadTrail(acct, region, name string) (*CloudTrailTrail, error) {
	if name == "" {
		return nil, &AWSError{Code: "InvalidTrailNameException", Message: "Trail name is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	key := cloudtrailTrailKey(acct, region, name)
	data, err := p.state.Get(goCtx, cloudtrailNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("cloudtrail loadTrail get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "TrailNotFoundException", Message: "Trail " + name + " does not exist for account " + acct, HTTPStatus: http.StatusNotFound}
	}
	var trail CloudTrailTrail
	if err := json.Unmarshal(data, &trail); err != nil {
		return nil, fmt.Errorf("cloudtrail loadTrail unmarshal: %w", err)
	}
	return &trail, nil
}

// cloudtrailJSONResponse serializes v to JSON and returns an AWSResponse with
// Content-Type application/x-amz-json-1.1.
func cloudtrailJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("cloudtrail json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}
