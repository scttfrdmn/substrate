package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"strings"
	"time"
)

// fsxNamespace is the state namespace used by FSxPlugin.
const fsxNamespace = "fsx"

// FSxFileSystem represents a stored Amazon FSx file system.
type FSxFileSystem struct {
	// FileSystemID is the unique identifier for the file system.
	FileSystemID string `json:"file_system_id"`
	// FileSystemType is the type of file system: LUSTRE, WINDOWS, ONTAP, or OPENZFS.
	FileSystemType string `json:"file_system_type"`
	// StorageCapacity is the storage capacity in GiB.
	StorageCapacity int32 `json:"storage_capacity"`
	// StorageType is the storage type: SSD or HDD.
	StorageType string `json:"storage_type"`
	// VpcID is the VPC where the file system is deployed.
	VpcID string `json:"vpc_id"`
	// SubnetIDs lists the subnets in which the file system is deployed.
	SubnetIDs []string `json:"subnet_ids"`
	// DNSName is the DNS name for the file system.
	DNSName string `json:"dns_name"`
	// ResourceARN is the Amazon Resource Name of the file system.
	ResourceARN string `json:"resource_arn"`
	// Lifecycle indicates the current state of the file system.
	Lifecycle string `json:"lifecycle"`
	// Tags contains the tags applied to the file system.
	Tags []FSxTag `json:"tags,omitempty"`
	// CreationTime is the Unix epoch timestamp when the file system was created.
	CreationTime float64 `json:"creation_time"`
	// LustreMountName is the mount name used for LUSTRE file systems.
	// For SCRATCH_2 deployments this is always "fsx"; other types use a random value.
	LustreMountName string `json:"lustre_mount_name,omitempty"`
	// LustreDeploymentType is the Lustre deployment type (e.g. SCRATCH_2, PERSISTENT_1).
	LustreDeploymentType string `json:"lustre_deployment_type,omitempty"`
	// AccountID is the AWS account that owns the file system.
	AccountID string `json:"account_id"`
	// Region is the AWS region where the file system resides.
	Region string `json:"region"`
}

// FSxTag represents a key-value tag on an FSx resource.
type FSxTag struct {
	// Key is the tag key.
	Key string `json:"Key"`
	// Value is the tag value.
	Value string `json:"Value"`
}

// generateFSxFileSystemID returns a unique FSx file system ID of the form
// "fs-" followed by 8 lowercase hex digits.
func generateFSxFileSystemID() string {
	return "fs-" + randomHex(8)
}

// fsxDNSName derives a DNS name for a file system based on its ID and region.
// The format follows the real AWS pattern: {id}.fsx.{region}.amazonaws.com.
func fsxDNSName(id, region string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return fmt.Sprintf("%s.fsx.%s.amazonaws.com", id, region)
}

// FSxPlugin emulates the Amazon FSx service.
// It supports CreateFileSystem, DescribeFileSystems, and DeleteFileSystem
// using the FSx JSON API (X-Amz-Target: AmazonFSx.<Op>).
type FSxPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "fsx".
func (p *FSxPlugin) Name() string { return fsxNamespace }

// Initialize sets up the FSxPlugin with the provided configuration.
func (p *FSxPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for FSxPlugin.
func (p *FSxPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an FSx JSON-protocol request to the appropriate handler.
func (p *FSxPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "CreateFileSystem":
		return p.createFileSystem(ctx, req)
	case "DescribeFileSystems":
		return p.describeFileSystems(ctx, req)
	case "DeleteFileSystem":
		return p.deleteFileSystem(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    fmt.Sprintf("FSxPlugin: unknown operation %q", req.Operation),
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// --- Operations --------------------------------------------------------------

func (p *FSxPlugin) createFileSystem(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		FileSystemType      string   `json:"FileSystemType"`
		StorageCapacity     int32    `json:"StorageCapacity"`
		StorageType         string   `json:"StorageType"`
		SubnetIDs           []string `json:"SubnetIds"`
		Tags                []FSxTag `json:"Tags"`
		LustreConfiguration struct {
			DeploymentType string `json:"DeploymentType"`
		} `json:"LustreConfiguration"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{
				Code:       "InvalidRequest",
				Message:    "invalid JSON body: " + err.Error(),
				HTTPStatus: http.StatusBadRequest,
			}
		}
	}
	if input.FileSystemType == "" {
		input.FileSystemType = "LUSTRE"
	}
	if input.StorageType == "" {
		input.StorageType = "SSD"
	}

	fsID := generateFSxFileSystemID()
	arn := fmt.Sprintf("arn:aws:fsx:%s:%s:file-system/%s", ctx.Region, ctx.AccountID, fsID)

	// Derive VPC ID from the first subnet when available (simplified).
	vpcID := ""
	if len(input.SubnetIDs) > 0 {
		// Look up the subnet in EC2 state to find its VPC.
		goCtx := context.Background()
		subnetData, _ := p.state.Get(goCtx, "ec2", "subnet:"+ctx.AccountID+"/"+ctx.Region+"/"+input.SubnetIDs[0])
		if subnetData != nil {
			var sn struct {
				VPCID string `json:"vpc_id"`
			}
			if unmarshalErr := json.Unmarshal(subnetData, &sn); unmarshalErr == nil {
				vpcID = sn.VPCID
			}
		}
	}

	// Determine Lustre-specific fields.
	lustreDeploymentType := input.LustreConfiguration.DeploymentType
	if strings.ToUpper(input.FileSystemType) == "LUSTRE" && lustreDeploymentType == "" {
		lustreDeploymentType = "SCRATCH_2"
	}
	// MountName for SCRATCH_2 is always "fsx"; other Lustre types use a random value.
	lustreMountName := ""
	if strings.ToUpper(input.FileSystemType) == "LUSTRE" {
		if lustreDeploymentType == "SCRATCH_2" || lustreDeploymentType == "" {
			lustreMountName = "fsx"
		} else {
			lustreMountName = randomHex(8)
		}
	}

	fs := FSxFileSystem{
		FileSystemID:         fsID,
		FileSystemType:       strings.ToUpper(input.FileSystemType),
		StorageCapacity:      input.StorageCapacity,
		StorageType:          input.StorageType,
		VpcID:                vpcID,
		SubnetIDs:            input.SubnetIDs,
		DNSName:              fsxDNSName(fsID, ctx.Region),
		ResourceARN:          arn,
		Lifecycle:            "AVAILABLE",
		Tags:                 input.Tags,
		CreationTime:         float64(p.tc.Now().Unix()),
		LustreMountName:      lustreMountName,
		LustreDeploymentType: lustreDeploymentType,
		AccountID:            ctx.AccountID,
		Region:               ctx.Region,
	}

	data, err := json.Marshal(fs)
	if err != nil {
		return nil, fmt.Errorf("fsx createFileSystem marshal: %w", err)
	}
	goCtx := context.Background()
	if err := p.state.Put(goCtx, fsxNamespace, fsxKey(ctx.AccountID, ctx.Region, fsID), data); err != nil {
		return nil, fmt.Errorf("fsx createFileSystem put: %w", err)
	}
	updateStringIndex(goCtx, p.state, fsxNamespace, fsxIDsKey(ctx.AccountID, ctx.Region), fsID)

	return fsxJSONResponse(http.StatusOK, map[string]interface{}{
		"FileSystem": fsxToWire(fs),
	})
}

func (p *FSxPlugin) describeFileSystems(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		FileSystemIDs []string `json:"FileSystemIds"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
	}

	goCtx := context.Background()

	// If specific IDs are requested, look them up directly.
	if len(input.FileSystemIDs) > 0 {
		result := make([]map[string]interface{}, 0, len(input.FileSystemIDs))
		for _, id := range input.FileSystemIDs {
			data, err := p.state.Get(goCtx, fsxNamespace, fsxKey(ctx.AccountID, ctx.Region, id))
			if err != nil {
				return nil, fmt.Errorf("fsx describeFileSystems get: %w", err)
			}
			if data == nil {
				return nil, &AWSError{
					Code:       "FileSystemNotFound",
					Message:    fmt.Sprintf("File system '%s' does not exist.", id),
					HTTPStatus: http.StatusBadRequest,
				}
			}
			var fs FSxFileSystem
			if err := json.Unmarshal(data, &fs); err != nil {
				return nil, fmt.Errorf("fsx describeFileSystems unmarshal: %w", err)
			}
			// Treat DELETED the same as not-found so that SDK delete waiters
			// (NewFileSystemDeletedWaiter) receive FileSystemNotFound and
			// consider the delete complete on the first poll.
			if fs.Lifecycle == "DELETED" {
				return nil, &AWSError{
					Code:       "FileSystemNotFound",
					Message:    fmt.Sprintf("File system '%s' does not exist.", id),
					HTTPStatus: http.StatusBadRequest,
				}
			}
			result = append(result, fsxToWire(fs))
		}
		return fsxJSONResponse(http.StatusOK, map[string]interface{}{
			"FileSystems": result,
		})
	}

	// Otherwise list all non-deleted file systems.
	ids, err := loadStringIndex(goCtx, p.state, fsxNamespace, fsxIDsKey(ctx.AccountID, ctx.Region))
	if err != nil {
		return nil, fmt.Errorf("fsx describeFileSystems list: %w", err)
	}
	result := make([]map[string]interface{}, 0, len(ids))
	for _, id := range ids {
		data, getErr := p.state.Get(goCtx, fsxNamespace, fsxKey(ctx.AccountID, ctx.Region, id))
		if getErr != nil || data == nil {
			continue
		}
		var fs FSxFileSystem
		if unmarshalErr := json.Unmarshal(data, &fs); unmarshalErr != nil {
			continue
		}
		if fs.Lifecycle != "DELETED" {
			result = append(result, fsxToWire(fs))
		}
	}
	return fsxJSONResponse(http.StatusOK, map[string]interface{}{
		"FileSystems": result,
	})
}

func (p *FSxPlugin) deleteFileSystem(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		FileSystemID string `json:"FileSystemId"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{
				Code:       "InvalidRequest",
				Message:    "invalid JSON body: " + err.Error(),
				HTTPStatus: http.StatusBadRequest,
			}
		}
	}
	if input.FileSystemID == "" {
		return nil, &AWSError{
			Code:       "InvalidRequest",
			Message:    "FileSystemId is required",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	goCtx := context.Background()
	data, err := p.state.Get(goCtx, fsxNamespace, fsxKey(ctx.AccountID, ctx.Region, input.FileSystemID))
	if err != nil {
		return nil, fmt.Errorf("fsx deleteFileSystem get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{
			Code:       "FileSystemNotFound",
			Message:    fmt.Sprintf("File system '%s' does not exist.", input.FileSystemID),
			HTTPStatus: http.StatusBadRequest,
		}
	}

	var fs FSxFileSystem
	if err := json.Unmarshal(data, &fs); err != nil {
		return nil, fmt.Errorf("fsx deleteFileSystem unmarshal: %w", err)
	}

	// Soft-delete: mark as DELETED.
	fs.Lifecycle = "DELETED"
	updated, err := json.Marshal(fs)
	if err != nil {
		return nil, fmt.Errorf("fsx deleteFileSystem marshal: %w", err)
	}
	if err := p.state.Put(goCtx, fsxNamespace, fsxKey(ctx.AccountID, ctx.Region, input.FileSystemID), updated); err != nil {
		return nil, fmt.Errorf("fsx deleteFileSystem put: %w", err)
	}

	return fsxJSONResponse(http.StatusOK, map[string]interface{}{
		"FileSystem": fsxToWire(fs),
	})
}

// --- Wire format helpers -----------------------------------------------------

// fsxToWire converts an FSxFileSystem to the AWS wire-format map.
func fsxToWire(fs FSxFileSystem) map[string]interface{} {
	m := map[string]interface{}{
		"FileSystemId":    fs.FileSystemID,
		"FileSystemType":  fs.FileSystemType,
		"StorageCapacity": fs.StorageCapacity,
		"StorageType":     fs.StorageType,
		"VpcId":           fs.VpcID,
		"SubnetIds":       fs.SubnetIDs,
		"DNSName":         fs.DNSName,
		"ResourceARN":     fs.ResourceARN,
		"Lifecycle":       fs.Lifecycle,
		"CreationTime":    fs.CreationTime,
		"Tags":            fs.Tags,
		"OwnerId":         fs.AccountID,
	}
	if fs.SubnetIDs == nil {
		m["SubnetIds"] = []string{}
	}
	if fs.Tags == nil {
		m["Tags"] = []FSxTag{}
	}
	// Include LustreConfiguration for LUSTRE file systems so that SDK consumers
	// can safely dereference LustreConfiguration.MountName without a nil panic.
	if fs.FileSystemType == "LUSTRE" {
		m["LustreConfiguration"] = map[string]interface{}{
			"MountName":      fs.LustreMountName,
			"DeploymentType": fs.LustreDeploymentType,
		}
	}
	return m
}

// --- State key helpers -------------------------------------------------------

// fsxKey returns the state key for a single FSx file system record.
func fsxKey(accountID, region, id string) string {
	return "fs:" + accountID + "/" + region + "/" + id
}

// fsxIDsKey returns the state key for the file system ID index.
func fsxIDsKey(accountID, region string) string {
	return "fs_ids:" + accountID + "/" + region
}

// --- Response helper ---------------------------------------------------------

// fsxJSONResponse serializes v to JSON and returns an AWSResponse with the
// given HTTP status code.
func fsxJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("fsxJSONResponse marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}
