package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// EFSPlugin emulates the Amazon Elastic File System service.
// It supports file system, access point, and mount target CRUD operations
// using the EFS REST/JSON API at /2015-02-01/... paths.
type EFSPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "efs".
func (p *EFSPlugin) Name() string { return efsNamespace }

// Initialize sets up the EFSPlugin with the provided configuration.
func (p *EFSPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for EFSPlugin.
func (p *EFSPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an EFS REST/JSON request to the appropriate handler.
func (p *EFSPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op, resourceID := parseEFSOperation(req.Operation, req.Path)
	switch op {
	case "CreateFileSystem":
		return p.createFileSystem(ctx, req)
	case "DescribeFileSystems":
		return p.describeFileSystems(ctx, req, resourceID)
	case "UpdateFileSystem":
		return p.updateFileSystem(ctx, req, resourceID)
	case "DeleteFileSystem":
		return p.deleteFileSystem(ctx, req, resourceID)
	case "CreateAccessPoint":
		return p.createAccessPoint(ctx, req)
	case "DescribeAccessPoints":
		return p.describeAccessPoints(ctx, req, resourceID)
	case "DeleteAccessPoint":
		return p.deleteAccessPoint(ctx, req, resourceID)
	case "CreateMountTarget":
		return p.createMountTarget(ctx, req)
	case "DescribeMountTargets":
		return p.describeMountTargets(ctx, req, resourceID)
	case "DeleteMountTarget":
		return p.deleteMountTarget(ctx, req, resourceID)
	case "TagResource":
		return p.tagResource(ctx, req, resourceID)
	case "ListTagsForResource":
		return p.listTagsForResource(ctx, req, resourceID)
	case "UntagResource":
		return p.untagResource(ctx, req, resourceID)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "EFSPlugin: unsupported path " + req.Path,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// --- FileSystem operations ---

func (p *EFSPlugin) createFileSystem(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		CreationToken   string   `json:"CreationToken"`
		PerformanceMode string   `json:"PerformanceMode"`
		ThroughputMode  string   `json:"ThroughputMode"`
		Encrypted       bool     `json:"Encrypted"`
		Tags            []EFSTag `json:"Tags"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "MalformedData", Message: "invalid JSON body: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}

	if input.PerformanceMode == "" {
		input.PerformanceMode = "generalPurpose"
	}
	if input.ThroughputMode == "" {
		input.ThroughputMode = "bursting"
	}

	fsID := generateEFSFileSystemID()
	arn := fmt.Sprintf("arn:aws:elasticfilesystem:%s:%s:file-system/%s",
		reqCtx.Region, reqCtx.AccountID, fsID)

	var name string
	for _, t := range input.Tags {
		if t.Key == "Name" {
			name = t.Value
			break
		}
	}

	fs := EFSFileSystem{
		FileSystemID:         fsID,
		FileSystemArn:        arn,
		OwnerID:              reqCtx.AccountID,
		CreationToken:        input.CreationToken,
		LifeCycleState:       "available",
		Name:                 name,
		NumberOfMountTargets: 0,
		PerformanceMode:      input.PerformanceMode,
		ThroughputMode:       input.ThroughputMode,
		Encrypted:            input.Encrypted,
		Tags:                 input.Tags,
		CreatedAt:            p.tc.Now(),
		AccountID:            reqCtx.AccountID,
		Region:               reqCtx.Region,
	}

	data, err := json.Marshal(fs)
	if err != nil {
		return nil, fmt.Errorf("efs createFileSystem marshal: %w", err)
	}
	goCtx := context.Background()
	key := "filesystem:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + fsID
	if err := p.state.Put(goCtx, efsNamespace, key, data); err != nil {
		return nil, fmt.Errorf("efs createFileSystem put: %w", err)
	}
	updateStringIndex(goCtx, p.state, efsNamespace, "filesystem_ids:"+reqCtx.AccountID+"/"+reqCtx.Region, fsID)

	return efsJSONResponse(http.StatusCreated, fs)
}

func (p *EFSPlugin) describeFileSystems(reqCtx *RequestContext, req *AWSRequest, resourceID string) (*AWSResponse, error) {
	goCtx := context.Background()
	prefix := "filesystem:" + reqCtx.AccountID + "/" + reqCtx.Region + "/"

	fsIDFilter := resourceID
	if fsIDFilter == "" {
		fsIDFilter = req.Params["FileSystemId"]
	}

	if fsIDFilter != "" {
		data, err := p.state.Get(goCtx, efsNamespace, prefix+fsIDFilter)
		if err != nil {
			return nil, fmt.Errorf("efs describeFileSystems get: %w", err)
		}
		if data == nil {
			return nil, &AWSError{Code: "FileSystemNotFound", Message: "File system " + fsIDFilter + " does not exist.", HTTPStatus: http.StatusNotFound}
		}
		var fs EFSFileSystem
		if err := json.Unmarshal(data, &fs); err != nil {
			return nil, fmt.Errorf("efs describeFileSystems unmarshal: %w", err)
		}
		return efsJSONResponse(http.StatusOK, map[string]interface{}{
			"FileSystems": []EFSFileSystem{fs},
		})
	}

	ids, err := loadStringIndex(goCtx, p.state, efsNamespace, "filesystem_ids:"+reqCtx.AccountID+"/"+reqCtx.Region)
	if err != nil {
		return nil, fmt.Errorf("efs describeFileSystems list: %w", err)
	}
	var filesystems []EFSFileSystem
	for _, id := range ids {
		data, err := p.state.Get(goCtx, efsNamespace, prefix+id)
		if err != nil || data == nil {
			continue
		}
		var fs EFSFileSystem
		if json.Unmarshal(data, &fs) != nil {
			continue
		}
		filesystems = append(filesystems, fs)
	}
	if filesystems == nil {
		filesystems = []EFSFileSystem{}
	}
	return efsJSONResponse(http.StatusOK, map[string]interface{}{
		"FileSystems": filesystems,
	})
}

func (p *EFSPlugin) updateFileSystem(reqCtx *RequestContext, req *AWSRequest, fsID string) (*AWSResponse, error) {
	if fsID == "" {
		return nil, &AWSError{Code: "BadRequest", Message: "FileSystemId is required", HTTPStatus: http.StatusBadRequest}
	}

	var input struct {
		ThroughputMode               string  `json:"ThroughputMode"`
		ProvisionedThroughputInMibps float64 `json:"ProvisionedThroughputInMibps"`
	}
	if len(req.Body) > 0 {
		_ = json.Unmarshal(req.Body, &input)
	}

	goCtx := context.Background()
	key := "filesystem:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + fsID
	data, err := p.state.Get(goCtx, efsNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("efs updateFileSystem get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "FileSystemNotFound", Message: "File system " + fsID + " does not exist.", HTTPStatus: http.StatusNotFound}
	}
	var fs EFSFileSystem
	if err := json.Unmarshal(data, &fs); err != nil {
		return nil, fmt.Errorf("efs updateFileSystem unmarshal: %w", err)
	}
	if input.ThroughputMode != "" {
		fs.ThroughputMode = input.ThroughputMode
	}
	updated, _ := json.Marshal(fs)
	if err := p.state.Put(goCtx, efsNamespace, key, updated); err != nil {
		return nil, fmt.Errorf("efs updateFileSystem put: %w", err)
	}
	return efsJSONResponse(http.StatusAccepted, fs)
}

func (p *EFSPlugin) deleteFileSystem(reqCtx *RequestContext, _ *AWSRequest, fsID string) (*AWSResponse, error) {
	if fsID == "" {
		return nil, &AWSError{Code: "BadRequest", Message: "FileSystemId is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	key := "filesystem:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + fsID
	if err := p.state.Delete(goCtx, efsNamespace, key); err != nil {
		return nil, fmt.Errorf("efs deleteFileSystem delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, efsNamespace, "filesystem_ids:"+reqCtx.AccountID+"/"+reqCtx.Region, fsID)
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}, Body: nil}, nil
}

// --- AccessPoint operations ---

func (p *EFSPlugin) createAccessPoint(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		FileSystemID  string            `json:"FileSystemId"`
		PosixUser     *EFSPosixUser     `json:"PosixUser"`
		RootDirectory *EFSRootDirectory `json:"RootDirectory"`
		Tags          []EFSTag          `json:"Tags"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "MalformedData", Message: "invalid JSON body: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.FileSystemID == "" {
		return nil, &AWSError{Code: "BadRequest", Message: "FileSystemId is required", HTTPStatus: http.StatusBadRequest}
	}

	apID := generateEFSAccessPointID()
	arn := fmt.Sprintf("arn:aws:elasticfilesystem:%s:%s:access-point/%s",
		reqCtx.Region, reqCtx.AccountID, apID)

	var name string
	for _, t := range input.Tags {
		if t.Key == "Name" {
			name = t.Value
			break
		}
	}

	ap := EFSAccessPoint{
		AccessPointID:  apID,
		AccessPointArn: arn,
		FileSystemID:   input.FileSystemID,
		LifeCycleState: "available",
		Name:           name,
		PosixUser:      input.PosixUser,
		RootDirectory:  input.RootDirectory,
		Tags:           input.Tags,
		AccountID:      reqCtx.AccountID,
		Region:         reqCtx.Region,
	}

	data, err := json.Marshal(ap)
	if err != nil {
		return nil, fmt.Errorf("efs createAccessPoint marshal: %w", err)
	}
	goCtx := context.Background()
	key := "accesspoint:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + apID
	if err := p.state.Put(goCtx, efsNamespace, key, data); err != nil {
		return nil, fmt.Errorf("efs createAccessPoint put: %w", err)
	}
	updateStringIndex(goCtx, p.state, efsNamespace, "accesspoint_ids:"+reqCtx.AccountID+"/"+reqCtx.Region, apID)

	return efsJSONResponse(http.StatusOK, ap)
}

func (p *EFSPlugin) describeAccessPoints(reqCtx *RequestContext, req *AWSRequest, resourceID string) (*AWSResponse, error) {
	goCtx := context.Background()
	prefix := "accesspoint:" + reqCtx.AccountID + "/" + reqCtx.Region + "/"

	apIDFilter := resourceID
	if apIDFilter == "" {
		apIDFilter = req.Params["AccessPointId"]
	}

	if apIDFilter != "" {
		data, err := p.state.Get(goCtx, efsNamespace, prefix+apIDFilter)
		if err != nil {
			return nil, fmt.Errorf("efs describeAccessPoints get: %w", err)
		}
		if data == nil {
			return nil, &AWSError{Code: "AccessPointNotFound", Message: "Access point " + apIDFilter + " does not exist.", HTTPStatus: http.StatusNotFound}
		}
		var ap EFSAccessPoint
		if err := json.Unmarshal(data, &ap); err != nil {
			return nil, fmt.Errorf("efs describeAccessPoints unmarshal: %w", err)
		}
		return efsJSONResponse(http.StatusOK, map[string]interface{}{
			"AccessPoints": []EFSAccessPoint{ap},
		})
	}

	fsIDFilter := req.Params["FileSystemId"]

	ids, err := loadStringIndex(goCtx, p.state, efsNamespace, "accesspoint_ids:"+reqCtx.AccountID+"/"+reqCtx.Region)
	if err != nil {
		return nil, fmt.Errorf("efs describeAccessPoints list: %w", err)
	}
	var accessPoints []EFSAccessPoint
	for _, id := range ids {
		data, err := p.state.Get(goCtx, efsNamespace, prefix+id)
		if err != nil || data == nil {
			continue
		}
		var ap EFSAccessPoint
		if json.Unmarshal(data, &ap) != nil {
			continue
		}
		if fsIDFilter != "" && ap.FileSystemID != fsIDFilter {
			continue
		}
		accessPoints = append(accessPoints, ap)
	}
	if accessPoints == nil {
		accessPoints = []EFSAccessPoint{}
	}
	return efsJSONResponse(http.StatusOK, map[string]interface{}{
		"AccessPoints": accessPoints,
	})
}

func (p *EFSPlugin) deleteAccessPoint(reqCtx *RequestContext, _ *AWSRequest, apID string) (*AWSResponse, error) {
	if apID == "" {
		return nil, &AWSError{Code: "BadRequest", Message: "AccessPointId is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	key := "accesspoint:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + apID
	if err := p.state.Delete(goCtx, efsNamespace, key); err != nil {
		return nil, fmt.Errorf("efs deleteAccessPoint delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, efsNamespace, "accesspoint_ids:"+reqCtx.AccountID+"/"+reqCtx.Region, apID)
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}, Body: nil}, nil
}

// --- MountTarget operations ---

func (p *EFSPlugin) createMountTarget(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		FileSystemID string `json:"FileSystemId"`
		SubnetID     string `json:"SubnetId"`
		IPAddress    string `json:"IpAddress"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "MalformedData", Message: "invalid JSON body: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.FileSystemID == "" {
		return nil, &AWSError{Code: "BadRequest", Message: "FileSystemId is required", HTTPStatus: http.StatusBadRequest}
	}

	mtID := generateEFSMountTargetID()
	mt := EFSMountTarget{
		MountTargetID:  mtID,
		FileSystemID:   input.FileSystemID,
		SubnetID:       input.SubnetID,
		LifeCycleState: "available",
		IPAddress:      input.IPAddress,
		AccountID:      reqCtx.AccountID,
		Region:         reqCtx.Region,
	}

	data, err := json.Marshal(mt)
	if err != nil {
		return nil, fmt.Errorf("efs createMountTarget marshal: %w", err)
	}
	goCtx := context.Background()
	key := "mounttarget:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + mtID
	if err := p.state.Put(goCtx, efsNamespace, key, data); err != nil {
		return nil, fmt.Errorf("efs createMountTarget put: %w", err)
	}
	updateStringIndex(goCtx, p.state, efsNamespace, "mounttarget_ids:"+reqCtx.AccountID+"/"+reqCtx.Region, mtID)
	updateStringIndex(goCtx, p.state, efsNamespace, "mounttarget_by_fs:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+input.FileSystemID, mtID)
	p.incrementMountTargetCount(goCtx, reqCtx, input.FileSystemID)

	return efsJSONResponse(http.StatusOK, mt)
}

func (p *EFSPlugin) describeMountTargets(reqCtx *RequestContext, req *AWSRequest, resourceID string) (*AWSResponse, error) {
	goCtx := context.Background()
	prefix := "mounttarget:" + reqCtx.AccountID + "/" + reqCtx.Region + "/"

	mtIDFilter := resourceID
	if mtIDFilter == "" {
		mtIDFilter = req.Params["MountTargetId"]
	}
	fsIDFilter := req.Params["FileSystemId"]

	if mtIDFilter != "" {
		data, err := p.state.Get(goCtx, efsNamespace, prefix+mtIDFilter)
		if err != nil {
			return nil, fmt.Errorf("efs describeMountTargets get: %w", err)
		}
		if data == nil {
			return nil, &AWSError{Code: "MountTargetNotFound", Message: "Mount target " + mtIDFilter + " does not exist.", HTTPStatus: http.StatusNotFound}
		}
		var mt EFSMountTarget
		if err := json.Unmarshal(data, &mt); err != nil {
			return nil, fmt.Errorf("efs describeMountTargets unmarshal: %w", err)
		}
		return efsJSONResponse(http.StatusOK, map[string]interface{}{
			"MountTargets": []EFSMountTarget{mt},
		})
	}

	var (
		ids []string
		err error
	)
	if fsIDFilter != "" {
		ids, err = loadStringIndex(goCtx, p.state, efsNamespace, "mounttarget_by_fs:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+fsIDFilter)
	} else {
		ids, err = loadStringIndex(goCtx, p.state, efsNamespace, "mounttarget_ids:"+reqCtx.AccountID+"/"+reqCtx.Region)
	}
	if err != nil {
		return nil, fmt.Errorf("efs describeMountTargets list: %w", err)
	}

	var mountTargets []EFSMountTarget
	for _, id := range ids {
		data, err := p.state.Get(goCtx, efsNamespace, prefix+id)
		if err != nil || data == nil {
			continue
		}
		var mt EFSMountTarget
		if json.Unmarshal(data, &mt) != nil {
			continue
		}
		mountTargets = append(mountTargets, mt)
	}
	if mountTargets == nil {
		mountTargets = []EFSMountTarget{}
	}
	return efsJSONResponse(http.StatusOK, map[string]interface{}{
		"MountTargets": mountTargets,
	})
}

func (p *EFSPlugin) deleteMountTarget(reqCtx *RequestContext, _ *AWSRequest, mtID string) (*AWSResponse, error) {
	if mtID == "" {
		return nil, &AWSError{Code: "BadRequest", Message: "MountTargetId is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	key := "mounttarget:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + mtID

	data, err := p.state.Get(goCtx, efsNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("efs deleteMountTarget get: %w", err)
	}
	var fsID string
	if data != nil {
		var mt EFSMountTarget
		if json.Unmarshal(data, &mt) == nil {
			fsID = mt.FileSystemID
		}
	}

	if err := p.state.Delete(goCtx, efsNamespace, key); err != nil {
		return nil, fmt.Errorf("efs deleteMountTarget delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, efsNamespace, "mounttarget_ids:"+reqCtx.AccountID+"/"+reqCtx.Region, mtID)
	if fsID != "" {
		removeFromStringIndex(goCtx, p.state, efsNamespace, "mounttarget_by_fs:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+fsID, mtID)
		p.decrementMountTargetCount(goCtx, reqCtx, fsID)
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}, Body: nil}, nil
}

// --- Tagging operations ---

func (p *EFSPlugin) tagResource(reqCtx *RequestContext, req *AWSRequest, resourceID string) (*AWSResponse, error) {
	if resourceID == "" {
		return nil, &AWSError{Code: "BadRequest", Message: "ResourceId is required", HTTPStatus: http.StatusBadRequest}
	}
	var input struct {
		Tags []EFSTag `json:"Tags"`
	}
	if err := json.Unmarshal(req.Body, &input); err != nil {
		return nil, &AWSError{Code: "MalformedData", Message: "invalid JSON body: " + err.Error(), HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	if err := p.mergeEFSTags(goCtx, reqCtx, resourceID, input.Tags, nil); err != nil {
		return nil, err
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}, Body: nil}, nil
}

func (p *EFSPlugin) listTagsForResource(reqCtx *RequestContext, _ *AWSRequest, resourceID string) (*AWSResponse, error) {
	if resourceID == "" {
		return nil, &AWSError{Code: "BadRequest", Message: "ResourceId is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	tags, err := p.loadEFSTags(goCtx, reqCtx, resourceID)
	if err != nil {
		return nil, err
	}
	if tags == nil {
		tags = []EFSTag{}
	}
	return efsJSONResponse(http.StatusOK, map[string]interface{}{"Tags": tags})
}

func (p *EFSPlugin) untagResource(reqCtx *RequestContext, req *AWSRequest, resourceID string) (*AWSResponse, error) {
	if resourceID == "" {
		return nil, &AWSError{Code: "BadRequest", Message: "ResourceId is required", HTTPStatus: http.StatusBadRequest}
	}
	tagKeysParam := req.Params["tagKeys"]
	var keys []string
	if tagKeysParam != "" {
		keys = strings.Split(tagKeysParam, ",")
	}

	goCtx := context.Background()
	if err := p.mergeEFSTags(goCtx, reqCtx, resourceID, nil, keys); err != nil {
		return nil, err
	}
	return &AWSResponse{StatusCode: http.StatusNoContent, Headers: map[string]string{}, Body: nil}, nil
}

// --- Internal helpers ---

func (p *EFSPlugin) mergeEFSTags(goCtx context.Context, reqCtx *RequestContext, resourceID string, addTags []EFSTag, removeKeys []string) error {
	if strings.HasPrefix(resourceID, "fs-") {
		key := "filesystem:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + resourceID
		raw, err := p.state.Get(goCtx, efsNamespace, key)
		if err != nil {
			return fmt.Errorf("efs mergeEFSTags get fs: %w", err)
		}
		if raw == nil {
			return &AWSError{Code: "FileSystemNotFound", Message: "File system " + resourceID + " does not exist.", HTTPStatus: http.StatusNotFound}
		}
		var fs EFSFileSystem
		if err := json.Unmarshal(raw, &fs); err != nil {
			return fmt.Errorf("efs mergeEFSTags unmarshal fs: %w", err)
		}
		fs.Tags = mergeEFSTagSlice(fs.Tags, addTags, removeKeys)
		updated, _ := json.Marshal(fs)
		return p.state.Put(goCtx, efsNamespace, key, updated)
	}

	if strings.HasPrefix(resourceID, "fsap-") {
		key := "accesspoint:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + resourceID
		raw, err := p.state.Get(goCtx, efsNamespace, key)
		if err != nil {
			return fmt.Errorf("efs mergeEFSTags get ap: %w", err)
		}
		if raw == nil {
			return &AWSError{Code: "AccessPointNotFound", Message: "Access point " + resourceID + " does not exist.", HTTPStatus: http.StatusNotFound}
		}
		var ap EFSAccessPoint
		if err := json.Unmarshal(raw, &ap); err != nil {
			return fmt.Errorf("efs mergeEFSTags unmarshal ap: %w", err)
		}
		ap.Tags = mergeEFSTagSlice(ap.Tags, addTags, removeKeys)
		updated, _ := json.Marshal(ap)
		return p.state.Put(goCtx, efsNamespace, key, updated)
	}

	return &AWSError{Code: "BadRequest", Message: "Unknown resource ID prefix for " + resourceID, HTTPStatus: http.StatusBadRequest}
}

func (p *EFSPlugin) loadEFSTags(goCtx context.Context, reqCtx *RequestContext, resourceID string) ([]EFSTag, error) {
	if strings.HasPrefix(resourceID, "fs-") {
		key := "filesystem:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + resourceID
		raw, err := p.state.Get(goCtx, efsNamespace, key)
		if err != nil {
			return nil, fmt.Errorf("efs loadEFSTags get fs: %w", err)
		}
		if raw == nil {
			return nil, &AWSError{Code: "FileSystemNotFound", Message: "File system " + resourceID + " does not exist.", HTTPStatus: http.StatusNotFound}
		}
		var fs EFSFileSystem
		if err := json.Unmarshal(raw, &fs); err != nil {
			return nil, fmt.Errorf("efs loadEFSTags unmarshal: %w", err)
		}
		return fs.Tags, nil
	}

	if strings.HasPrefix(resourceID, "fsap-") {
		key := "accesspoint:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + resourceID
		raw, err := p.state.Get(goCtx, efsNamespace, key)
		if err != nil {
			return nil, fmt.Errorf("efs loadEFSTags get ap: %w", err)
		}
		if raw == nil {
			return nil, &AWSError{Code: "AccessPointNotFound", Message: "Access point " + resourceID + " does not exist.", HTTPStatus: http.StatusNotFound}
		}
		var ap EFSAccessPoint
		if err := json.Unmarshal(raw, &ap); err != nil {
			return nil, fmt.Errorf("efs loadEFSTags unmarshal: %w", err)
		}
		return ap.Tags, nil
	}

	return nil, &AWSError{Code: "BadRequest", Message: "Unknown resource ID prefix for " + resourceID, HTTPStatus: http.StatusBadRequest}
}

// mergeEFSTagSlice applies add and remove operations to an []EFSTag slice.
func mergeEFSTagSlice(existing []EFSTag, add []EFSTag, removeKeys []string) []EFSTag {
	m := make(map[string]string, len(existing))
	for _, t := range existing {
		m[t.Key] = t.Value
	}
	for _, t := range add {
		m[t.Key] = t.Value
	}
	for _, k := range removeKeys {
		delete(m, k)
	}
	out := make([]EFSTag, 0, len(m))
	for k, v := range m {
		out = append(out, EFSTag{Key: k, Value: v})
	}
	return out
}

// incrementMountTargetCount increments NumberOfMountTargets on a file system.
func (p *EFSPlugin) incrementMountTargetCount(goCtx context.Context, reqCtx *RequestContext, fsID string) {
	key := "filesystem:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + fsID
	raw, err := p.state.Get(goCtx, efsNamespace, key)
	if err != nil || raw == nil {
		return
	}
	var fs EFSFileSystem
	if json.Unmarshal(raw, &fs) != nil {
		return
	}
	fs.NumberOfMountTargets++
	updated, _ := json.Marshal(fs)
	_ = p.state.Put(goCtx, efsNamespace, key, updated)
}

// decrementMountTargetCount decrements NumberOfMountTargets on a file system.
func (p *EFSPlugin) decrementMountTargetCount(goCtx context.Context, reqCtx *RequestContext, fsID string) {
	key := "filesystem:" + reqCtx.AccountID + "/" + reqCtx.Region + "/" + fsID
	raw, err := p.state.Get(goCtx, efsNamespace, key)
	if err != nil || raw == nil {
		return
	}
	var fs EFSFileSystem
	if json.Unmarshal(raw, &fs) != nil {
		return
	}
	if fs.NumberOfMountTargets > 0 {
		fs.NumberOfMountTargets--
	}
	updated, _ := json.Marshal(fs)
	_ = p.state.Put(goCtx, efsNamespace, key, updated)
}

// efsJSONResponse serializes v to JSON and returns an AWSResponse.
func efsJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("efs json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       body,
	}, nil
}

// generateEFSFileSystemID generates an EFS file system ID (fs- + 8 hex chars).
func generateEFSFileSystemID() string {
	return "fs-" + randomHex(8)
}

// generateEFSAccessPointID generates an EFS access point ID (fsap- + 8 hex chars).
func generateEFSAccessPointID() string {
	return "fsap-" + randomHex(8)
}

// generateEFSMountTargetID generates an EFS mount target ID (fsmt- + 8 hex chars).
func generateEFSMountTargetID() string {
	return "fsmt-" + randomHex(8)
}
