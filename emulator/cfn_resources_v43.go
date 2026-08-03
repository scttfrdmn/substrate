package emulator

// cfn_resources_v43.go holds the StackDeployer.deployResource helpers for FSx.
// The name records the substrate release that added them (v0.43.0) rather than the
// services, because several releases touched overlapping services; the helpers here
// follow the same pattern as those in cfn_deployer.go.

import (
	"context"
	"encoding/json"
)

// ----- v0.43.0 — FSx ----------------------------------------------------------

// deployFSxFileSystem creates an FSx file system for the given CFN resource.
func (d *StackDeployer) deployFSxFileSystem(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	dr := DeployedResource{LogicalID: logicalID, Type: "AWS::FSx::FileSystem"}

	fileSystemType := resolveStringProp(props, "FileSystemType", "LUSTRE", cctx)
	storageCapacity := int32(1200)
	if sc, ok := props["StorageCapacity"]; ok {
		switch v := sc.(type) {
		case int:
			storageCapacity = int32(v)
		case int32:
			storageCapacity = v
		case int64:
			storageCapacity = int32(v)
		case float64:
			storageCapacity = int32(v)
		}
	}
	storageType := resolveStringProp(props, "StorageType", "SSD", cctx)

	// resolveStringList rather than a hand-rolled loop, so the whole property is
	// list-aware: a Ref to a List<AWS::EC2::Subnet::Id> parameter or a nested
	// Fn::Split contributes every subnet rather than one (#521, #526).
	subnetIDs := resolveStringList(props["SubnetIds"], cctx)

	var tags []FSxTag
	if t, ok := props["Tags"]; ok {
		if arr, ok2 := t.([]interface{}); ok2 {
			for _, item := range arr {
				if m, ok3 := item.(map[string]interface{}); ok3 {
					// resolveValue rather than a string assertion: a tag value
					// is very often `{"Fn::Sub": "${AWS::StackName}-data"}`, and
					// asserting on string dropped it silently (#526).
					tags = append(tags, FSxTag{
						Key:   resolveValue(m["Key"], cctx),
						Value: resolveValue(m["Value"], cctx),
					})
				}
			}
		}
	}

	body := map[string]interface{}{
		"FileSystemType":  fileSystemType,
		"StorageCapacity": storageCapacity,
		"StorageType":     storageType,
		"SubnetIds":       subnetIDs,
		"Tags":            tags,
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		dr.Error = "marshal request body: " + err.Error()
		return dr, 0, nil //nolint:nilerr
	}

	req := &AWSRequest{
		Service:   "fsx",
		Operation: "CreateFileSystem",
		Headers:   map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:      bodyBytes,
	}
	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	if routeErr != nil {
		dr.Error = routeErr.Error()
		return dr, 0, nil //nolint:nilerr
	}
	if resp != nil {
		var out struct {
			FileSystem struct {
				FileSystemID string `json:"FileSystemId"`
				ResourceARN  string `json:"ResourceARN"`
				DNSName      string `json:"DNSName"`
			} `json:"FileSystem"`
		}
		if jsonErr := json.Unmarshal(resp.Body, &out); jsonErr == nil {
			dr.PhysicalID = out.FileSystem.FileSystemID
			dr.ARN = out.FileSystem.ResourceARN
			dr.Metadata = map[string]interface{}{
				"DNSName": out.FileSystem.DNSName,
			}
		}
	}
	return dr, cost, nil
}
