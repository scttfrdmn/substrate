package substrate

import (
	"context"
	"encoding/json"
	"fmt"
)

// ----- v0.26.0 — EFS --------------------------------------------------------

// deployEFSFileSystem creates an EFS file system for the given CFN resource.
func (d *StackDeployer) deployEFSFileSystem(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	body := map[string]interface{}{
		"CreationToken":   resolveStringProp(props, "CreationToken", logicalID, cctx),
		"PerformanceMode": resolveStringProp(props, "PerformanceMode", "generalPurpose", cctx),
		"ThroughputMode":  resolveStringProp(props, "ThroughputMode", "bursting", cctx),
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal efs filesystem body: %w", err)
	}

	req := &AWSRequest{
		Service:   "efs",
		Operation: "POST",
		Path:      "/2015-02-01/file-systems",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID: logicalID,
		Type:      "AWS::EFS::FileSystem",
		Metadata:  make(map[string]interface{}),
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			FileSystemID  string `json:"FileSystemId"`
			FileSystemArn string `json:"FileSystemArn"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil {
			dr.PhysicalID = result.FileSystemID
			dr.ARN = result.FileSystemArn
		}
	}
	return dr, cost, nil
}

// deployEFSAccessPoint creates an EFS access point for the given CFN resource.
func (d *StackDeployer) deployEFSAccessPoint(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	fsID := resolveStringProp(props, "FileSystemId", "", cctx)

	body := map[string]interface{}{
		"FileSystemId": fsID,
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal efs access point body: %w", err)
	}

	req := &AWSRequest{
		Service:   "efs",
		Operation: "POST",
		Path:      "/2015-02-01/access-points",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID: logicalID,
		Type:      "AWS::EFS::AccessPoint",
		Metadata:  make(map[string]interface{}),
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			AccessPointID  string `json:"AccessPointId"`
			AccessPointArn string `json:"AccessPointArn"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil {
			dr.PhysicalID = result.AccessPointID
			dr.ARN = result.AccessPointArn
		}
	}
	return dr, cost, nil
}

// deployEFSMountTarget creates an EFS mount target for the given CFN resource.
func (d *StackDeployer) deployEFSMountTarget(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	body := map[string]interface{}{
		"FileSystemId": resolveStringProp(props, "FileSystemId", "", cctx),
		"SubnetId":     resolveStringProp(props, "SubnetId", "", cctx),
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal efs mount target body: %w", err)
	}

	req := &AWSRequest{
		Service:   "efs",
		Operation: "POST",
		Path:      "/2015-02-01/mount-targets",
		Body:      bodyBytes,
		Headers:   map[string]string{},
		Params:    map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID: logicalID,
		Type:      "AWS::EFS::MountTarget",
		Metadata:  make(map[string]interface{}),
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		var result struct {
			MountTargetID string `json:"MountTargetId"`
			IPAddress     string `json:"IpAddress"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &result); jsonErr == nil {
			dr.PhysicalID = result.MountTargetID
			if result.IPAddress != "" {
				dr.Metadata["IpAddress"] = result.IPAddress
			}
		}
	}
	return dr, cost, nil
}

// ----- v0.26.0 — Glue -------------------------------------------------------

// deployGlueDatabase creates a Glue database for the given CFN resource.
func (d *StackDeployer) deployGlueDatabase(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "DatabaseName", logicalID, cctx)

	body := map[string]interface{}{
		"DatabaseInput": map[string]interface{}{
			"Name":        name,
			"Description": resolveStringProp(props, "Description", "", cctx),
		},
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal glue database body: %w", err)
	}

	req := &AWSRequest{
		Service:   "glue",
		Operation: "CreateDatabase",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "AWSGlue.CreateDatabase"},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Glue::Database",
		PhysicalID: name,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployGlueConnection creates a Glue connection for the given CFN resource.
func (d *StackDeployer) deployGlueConnection(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "ConnectionName", logicalID, cctx)
	connType := resolveStringProp(props, "ConnectionType", "JDBC", cctx)

	body := map[string]interface{}{
		"ConnectionInput": map[string]interface{}{
			"Name":           name,
			"ConnectionType": connType,
		},
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal glue connection body: %w", err)
	}

	req := &AWSRequest{
		Service:   "glue",
		Operation: "CreateConnection",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "AWSGlue.CreateConnection"},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Glue::Connection",
		PhysicalID: name,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployGlueTable creates a Glue table for the given CFN resource.
func (d *StackDeployer) deployGlueTable(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	tableName := resolveStringProp(props, "TableName", logicalID, cctx)
	dbName := resolveStringProp(props, "DatabaseName", "", cctx)

	body := map[string]interface{}{
		"DatabaseName": dbName,
		"TableInput": map[string]interface{}{
			"Name": tableName,
		},
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal glue table body: %w", err)
	}

	req := &AWSRequest{
		Service:   "glue",
		Operation: "CreateTable",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "AWSGlue.CreateTable"},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Glue::Table",
		PhysicalID: tableName,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployGlueCrawler creates a Glue crawler for the given CFN resource.
func (d *StackDeployer) deployGlueCrawler(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "CrawlerName", logicalID, cctx)

	body := map[string]interface{}{
		"Name":         name,
		"Role":         resolveStringProp(props, "Role", "", cctx),
		"DatabaseName": resolveStringProp(props, "DatabaseName", "", cctx),
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal glue crawler body: %w", err)
	}

	req := &AWSRequest{
		Service:   "glue",
		Operation: "CreateCrawler",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "AWSGlue.CreateCrawler"},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Glue::Crawler",
		PhysicalID: name,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployGlueJob creates a Glue ETL job for the given CFN resource.
func (d *StackDeployer) deployGlueJob(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "JobName", logicalID, cctx)

	body := map[string]interface{}{
		"Name": name,
		"Role": resolveStringProp(props, "Role", "", cctx),
		"Command": map[string]interface{}{
			"Name":           resolveStringProp(props, "Command.Name", "glueetl", cctx),
			"ScriptLocation": resolveStringProp(props, "Command.ScriptLocation", "", cctx),
		},
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal glue job body: %w", err)
	}

	req := &AWSRequest{
		Service:   "glue",
		Operation: "CreateJob",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "AWSGlue.CreateJob"},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Glue::Job",
		PhysicalID: name,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}
