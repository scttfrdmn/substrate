package emulator

// StackDeployer helpers for CloudFormation's EFS resource types.

import (
	"context"
	"encoding/json"
	"fmt"
)

// ----- EFS ------------------------------------------------------------------

// deployEFSFileSystem creates an EFS file system for the given CFN resource.
func (d *StackDeployer) deployEFSFileSystem(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	body := map[string]interface{}{
		// CloudFormation supplies the creation token itself, the type declaring no CreationToken
		// property, so it is generated per stack as a name is (#1605).
		"CreationToken":   cfnGeneratedName(cctx, "AWS::EFS::FileSystem", logicalID),
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
