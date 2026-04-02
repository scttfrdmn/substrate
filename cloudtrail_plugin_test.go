package substrate_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

func setupCloudTrailPlugin(t *testing.T) (*substrate.CloudTrailPlugin, *substrate.RequestContext) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := &substrate.CloudTrailPlugin{}
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  substrate.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("CloudTrailPlugin.Initialize: %v", err)
	}
	return p, &substrate.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "req-cloudtrail-1",
	}
}

func cloudtrailRequest(t *testing.T, op string, body map[string]any) *substrate.AWSRequest {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal cloudtrail request body: %v", err)
	}
	return &substrate.AWSRequest{
		Service:   "cloudtrail",
		Operation: op,
		Headers:   map[string]string{"X-Amz-Target": "CloudTrail_20131101." + op},
		Body:      b,
		Params:    map[string]string{},
	}
}

func TestCloudTrailPlugin_CreateGetDeleteTrail(t *testing.T) {
	p, ctx := setupCloudTrailPlugin(t)

	// Create trail.
	resp, err := p.HandleRequest(ctx, cloudtrailRequest(t, "CreateTrail", map[string]any{
		"Name":                       "my-trail",
		"S3BucketName":               "my-cloudtrail-bucket",
		"IncludeGlobalServiceEvents": true,
		"IsMultiRegionTrail":         false,
		"EnableLogFileValidation":    true,
	}))
	if err != nil {
		t.Fatalf("CreateTrail: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", resp.StatusCode, resp.Body)
	}

	var trail substrate.CloudTrailTrail
	if err := json.Unmarshal(resp.Body, &trail); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}
	if trail.Name != "my-trail" {
		t.Errorf("want Name=my-trail, got %q", trail.Name)
	}
	if trail.TrailARN == "" {
		t.Error("want non-empty TrailARN")
	}
	if trail.S3BucketName != "my-cloudtrail-bucket" {
		t.Errorf("want S3BucketName=my-cloudtrail-bucket, got %q", trail.S3BucketName)
	}
	if !trail.IsLogging {
		t.Error("want IsLogging=true after create")
	}

	// Duplicate create should fail.
	_, err = p.HandleRequest(ctx, cloudtrailRequest(t, "CreateTrail", map[string]any{
		"Name":         "my-trail",
		"S3BucketName": "other-bucket",
	}))
	if err == nil {
		t.Fatal("want error for duplicate trail, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok {
		t.Fatalf("want *AWSError, got %T", err)
	}
	if awsErr.Code != "TrailAlreadyExistsException" {
		t.Errorf("want TrailAlreadyExistsException, got %q", awsErr.Code)
	}

	// GetTrail.
	resp, err = p.HandleRequest(ctx, cloudtrailRequest(t, "GetTrail", map[string]any{
		"Name": "my-trail",
	}))
	if err != nil {
		t.Fatalf("GetTrail: %v", err)
	}
	var getResult struct {
		Trail substrate.CloudTrailTrail `json:"Trail"`
	}
	if err := json.Unmarshal(resp.Body, &getResult); err != nil {
		t.Fatalf("unmarshal get: %v", err)
	}
	if getResult.Trail.Name != "my-trail" {
		t.Errorf("want Name=my-trail, got %q", getResult.Trail.Name)
	}

	// DeleteTrail.
	resp, err = p.HandleRequest(ctx, cloudtrailRequest(t, "DeleteTrail", map[string]any{
		"Name": "my-trail",
	}))
	if err != nil {
		t.Fatalf("DeleteTrail: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}

	// Get after delete should fail.
	_, err = p.HandleRequest(ctx, cloudtrailRequest(t, "GetTrail", map[string]any{
		"Name": "my-trail",
	}))
	if err == nil {
		t.Fatal("want error after delete, got nil")
	}
	awsErr, ok = err.(*substrate.AWSError)
	if !ok {
		t.Fatalf("want *AWSError, got %T", err)
	}
	if awsErr.Code != "TrailNotFoundException" {
		t.Errorf("want TrailNotFoundException, got %q", awsErr.Code)
	}
}

func TestCloudTrailPlugin_UpdateTrail(t *testing.T) {
	p, ctx := setupCloudTrailPlugin(t)

	_, err := p.HandleRequest(ctx, cloudtrailRequest(t, "CreateTrail", map[string]any{
		"Name":         "update-trail",
		"S3BucketName": "original-bucket",
	}))
	if err != nil {
		t.Fatalf("CreateTrail: %v", err)
	}

	resp, err := p.HandleRequest(ctx, cloudtrailRequest(t, "UpdateTrail", map[string]any{
		"Name":         "update-trail",
		"S3BucketName": "new-bucket",
	}))
	if err != nil {
		t.Fatalf("UpdateTrail: %v", err)
	}

	var trail substrate.CloudTrailTrail
	if err := json.Unmarshal(resp.Body, &trail); err != nil {
		t.Fatalf("unmarshal update: %v", err)
	}
	if trail.S3BucketName != "new-bucket" {
		t.Errorf("want S3BucketName=new-bucket after update, got %q", trail.S3BucketName)
	}
}

func TestCloudTrailPlugin_DescribeTrails(t *testing.T) {
	p, ctx := setupCloudTrailPlugin(t)

	for _, name := range []string{"trail-a", "trail-b"} {
		_, err := p.HandleRequest(ctx, cloudtrailRequest(t, "CreateTrail", map[string]any{
			"Name":         name,
			"S3BucketName": "bucket-" + name,
		}))
		if err != nil {
			t.Fatalf("CreateTrail %s: %v", name, err)
		}
	}

	resp, err := p.HandleRequest(ctx, cloudtrailRequest(t, "DescribeTrails", map[string]any{}))
	if err != nil {
		t.Fatalf("DescribeTrails: %v", err)
	}

	var result struct {
		TrailList []substrate.CloudTrailTrail `json:"trailList"`
	}
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		t.Fatalf("unmarshal describe: %v", err)
	}
	if len(result.TrailList) != 2 {
		t.Errorf("want 2 trails, got %d", len(result.TrailList))
	}
}

func TestCloudTrailPlugin_StartStopLogging(t *testing.T) {
	p, ctx := setupCloudTrailPlugin(t)

	_, err := p.HandleRequest(ctx, cloudtrailRequest(t, "CreateTrail", map[string]any{
		"Name":         "logging-trail",
		"S3BucketName": "logging-bucket",
	}))
	if err != nil {
		t.Fatalf("CreateTrail: %v", err)
	}

	// Stop logging.
	resp, err := p.HandleRequest(ctx, cloudtrailRequest(t, "StopLogging", map[string]any{
		"Name": "logging-trail",
	}))
	if err != nil {
		t.Fatalf("StopLogging: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}

	// Verify IsLogging=false.
	resp, err = p.HandleRequest(ctx, cloudtrailRequest(t, "GetTrail", map[string]any{
		"Name": "logging-trail",
	}))
	if err != nil {
		t.Fatalf("GetTrail after stop: %v", err)
	}
	var getResult struct {
		Trail substrate.CloudTrailTrail `json:"Trail"`
	}
	if err := json.Unmarshal(resp.Body, &getResult); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if getResult.Trail.IsLogging {
		t.Error("want IsLogging=false after StopLogging")
	}

	// Start logging.
	_, err = p.HandleRequest(ctx, cloudtrailRequest(t, "StartLogging", map[string]any{
		"Name": "logging-trail",
	}))
	if err != nil {
		t.Fatalf("StartLogging: %v", err)
	}

	// Verify IsLogging=true.
	resp, err = p.HandleRequest(ctx, cloudtrailRequest(t, "GetTrail", map[string]any{
		"Name": "logging-trail",
	}))
	if err != nil {
		t.Fatalf("GetTrail after start: %v", err)
	}
	if err := json.Unmarshal(resp.Body, &getResult); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !getResult.Trail.IsLogging {
		t.Error("want IsLogging=true after StartLogging")
	}
}

func TestCloudTrailPlugin_GetTrailStatus(t *testing.T) {
	p, ctx := setupCloudTrailPlugin(t)

	_, err := p.HandleRequest(ctx, cloudtrailRequest(t, "CreateTrail", map[string]any{
		"Name":         "status-trail",
		"S3BucketName": "status-bucket",
	}))
	if err != nil {
		t.Fatalf("CreateTrail: %v", err)
	}

	resp, err := p.HandleRequest(ctx, cloudtrailRequest(t, "GetTrailStatus", map[string]any{
		"Name": "status-trail",
	}))
	if err != nil {
		t.Fatalf("GetTrailStatus: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}

	var status struct {
		IsLogging          bool  `json:"IsLogging"`
		LatestDeliveryTime int64 `json:"LatestDeliveryTime"`
	}
	if err := json.Unmarshal(resp.Body, &status); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	if !status.IsLogging {
		t.Error("want IsLogging=true from GetTrailStatus")
	}
	if status.LatestDeliveryTime == 0 {
		t.Error("want non-zero LatestDeliveryTime")
	}

	// GetTrailStatus for non-existent trail.
	_, err = p.HandleRequest(ctx, cloudtrailRequest(t, "GetTrailStatus", map[string]any{
		"Name": "nonexistent",
	}))
	if err == nil {
		t.Fatal("want error for missing trail, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok {
		t.Fatalf("want *AWSError, got %T", err)
	}
	if awsErr.Code != "TrailNotFoundException" {
		t.Errorf("want TrailNotFoundException, got %q", awsErr.Code)
	}
}
