package substrate

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

	var subnetIDs []string
	if si, ok := props["SubnetIds"]; ok {
		if arr, ok2 := si.([]interface{}); ok2 {
			for _, item := range arr {
				if s := resolveValue(item, cctx); s != "" {
					subnetIDs = append(subnetIDs, s)
				}
			}
		}
	}

	var tags []FSxTag
	if t, ok := props["Tags"]; ok {
		if arr, ok2 := t.([]interface{}); ok2 {
			for _, item := range arr {
				if m, ok3 := item.(map[string]interface{}); ok3 {
					k, _ := m["Key"].(string)
					v, _ := m["Value"].(string)
					tags = append(tags, FSxTag{Key: k, Value: v})
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
