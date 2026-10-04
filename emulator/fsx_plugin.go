package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
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
	// CreationTime is when the file system was created, in fractional Unix epoch seconds. Records
	// written before #1373 hold whole seconds in the same field.
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

// generateFSxFileSystemID mints a unique FSx file system ID from m, of the form
// "fs-" followed by 16 lowercase hex digits.
func generateFSxFileSystemID(m *IDMint) string {
	return "fs-" + m.Hex(8)
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
// using the FSx JSON API (X-Amz-Target: AWSSimbaAPIService_v20180301.<Op>).
//
// The prefix is that, not AmazonFSx — it names an internal service codename rather than the public
// one, which is why it is worth stating here and why stating it wrongly was worth fixing (#1234).
// The authority is routing.go's FSx entry, sourced from botocore's fsx service-2.json, and parser.go
// splits an incoming target on it.
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
		return nil, unknownActionError(p.Name(), req.Operation)
	}
}

// --- Operations --------------------------------------------------------------

func (p *FSxPlugin) createFileSystem(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ClientRequestToken  string   `json:"ClientRequestToken"`
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
			return nil, fsxInvalidBody()
		}
	}
	if err := fsxValidateCreate(input.FileSystemType, input.StorageType, input.SubnetIDs); err != nil {
		return nil, err
	}
	// API_CreateFileSystem: with a token already used and the parameters the same, the call "returns
	// the description of the existing file system"; with the parameters different, it returns
	// IncompatibleParameterError.
	var fingerprint string
	if input.ClientRequestToken != "" {
		fp, err := fsxRequestFingerprint(req.Body)
		if err != nil {
			return nil, err
		}
		fingerprint = fp
		rec, found, err := p.fsxCheckToken(ctx, "CreateFileSystem", input.ClientRequestToken, fingerprint)
		if err != nil {
			return nil, err
		}
		if found {
			existing, err := p.loadFileSystem(ctx, rec.FileSystemID)
			if err != nil {
				return nil, err
			}
			return fsxJSONResponse(http.StatusOK, map[string]interface{}{"FileSystem": fsxToWire(*existing)})
		}
	}
	if input.StorageType == "" {
		input.StorageType = "SSD"
	}

	fsID := generateFSxFileSystemID(ctx.IDs)
	arn := fmt.Sprintf("arn:aws:fsx:%s:%s:file-system/%s", ctx.Region, ctx.AccountID, fsID)

	// Derive VPC ID from the first subnet when available (simplified).
	vpcID := ""
	if len(input.SubnetIDs) > 0 {
		// Look up the subnet in EC2 state to find its VPC.
		goCtx := context.Background()
		subnetData, err := p.state.Get(goCtx, "ec2", "subnet:"+ctx.AccountID+"/"+ctx.Region+"/"+input.SubnetIDs[0])
		if err != nil {
			return nil, fmt.Errorf("fsx createFileSystem subnet lookup: %w", err)
		}
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
	// API_CreateFileSystemLustreConfiguration: "(Default = SCRATCH_1)". API_LustreFileSystemConfiguration:
	// "For the SCRATCH_1 deployment type, this value is always "fsx". For SCRATCH_2, PERSISTENT_1, and
	// PERSISTENT_2 deployment types, this value is a string that is unique within an AWS Region",
	// with `Pattern: ^([A-Za-z0-9_-]{1,8})$`. Substrate defaulted to SCRATCH_2, gave *that* type
	// "fsx", and minted sixteen hex characters for the others, twice the published maximum (#1204's
	// audit). The minted name is now eight, still derived from the request id (#856).
	lustreDeploymentType := input.LustreConfiguration.DeploymentType
	if input.FileSystemType == "LUSTRE" && lustreDeploymentType == "" {
		lustreDeploymentType = "SCRATCH_1"
	}
	lustreMountName := ""
	if input.FileSystemType == "LUSTRE" {
		if lustreDeploymentType == "SCRATCH_1" {
			lustreMountName = "fsx"
		} else {
			lustreMountName = ctx.IDs.Hex(4)
		}
	}

	fs := FSxFileSystem{
		FileSystemID:         fsID,
		FileSystemType:       input.FileSystemType,
		StorageCapacity:      input.StorageCapacity,
		StorageType:          input.StorageType,
		VpcID:                vpcID,
		SubnetIDs:            input.SubnetIDs,
		DNSName:              fsxDNSName(fsID, ctx.Region),
		ResourceARN:          arn,
		Lifecycle:            "AVAILABLE",
		Tags:                 input.Tags,
		CreationTime:         fsxEpochNow(p.tc),
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
	if input.ClientRequestToken != "" {
		if err := p.fsxRecordToken(ctx, "CreateFileSystem", input.ClientRequestToken, fsxTokenRecord{FileSystemID: fsID, Fingerprint: fingerprint}); err != nil {
			return nil, err
		}
	}

	return fsxJSONResponse(http.StatusOK, map[string]interface{}{
		"FileSystem": fsxToWire(fs),
	})
}

// describeFileSystems handles DescribeFileSystems.
//
// # Pagination (#1195)
//
// API_DescribeFileSystems publishes MaxResults ("must be greater than 0", Valid Range 1–2147483647) and
// an opaque base64 NextToken (1–255 characters), and says a response "has no NextToken" once the list
// is done. Both were unread, so every account's file systems came back in one page and a paginator's
// loop body ran once. Both are read now:
//
//   - A MaxResults below 1 is BadRequest/400, the page's own generic client-error code; the page
//     publishes no more specific one. Above the published maximum cannot be sent as an Integer.
//   - The page size is min(MaxResults, 50): the page says FSx returns "the minimum of the MaxResults
//     parameter … and the service's internal maximum number of items per page", and the response's
//     FileSystems array publishes "Maximum number of 50 items", which is that internal maximum.
//   - The token is [encodeOffsetPaginationToken]'s base64 offset, which satisfies the published
//     pattern. One substrate did not issue is BadRequest/400 — again the page's only client code —
//     rather than read as page one (#915). It is omitted, never emitted empty, on the last page.
//   - The order is the account's ID index, which updateStringIndex keeps sorted, so it is stable
//     across pages; the page calls the order "unspecified", so any stable order is faithful.
//
// FileSystemIds is a lookup, not a listing, and the page scopes MaxResults and NextToken to
// "retrieving all file system descriptions", so neither applies when IDs are named. The published
// 50-item maximum on FileSystemIds is enforced; its pattern is not, for the reason
// [fsxValidateCreate] gives for SubnetIds — an ID that names nothing already answers the page's
// FileSystemNotFound.
//
// The operation publishes no filter member, so there is no filter to apply or refuse.
func (p *FSxPlugin) describeFileSystems(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		FileSystemIDs []string `json:"FileSystemIds"`
		MaxResults    *int64   `json:"MaxResults"`
		NextToken     string   `json:"NextToken"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, fsxInvalidBody()
		}
	}
	if len(input.FileSystemIDs) > fsxMaxListItems {
		return nil, fsxBadRequest(fmt.Sprintf("FileSystemIds may name at most %d file systems.", fsxMaxListItems))
	}
	pageSize := fsxMaxListItems
	if input.MaxResults != nil {
		if *input.MaxResults < 1 {
			return nil, fsxBadRequest("MaxResults must be greater than 0.")
		}
		if *input.MaxResults < int64(pageSize) {
			pageSize = int(*input.MaxResults)
		}
	}
	offset, ok := decodeOffsetPaginationToken(input.NextToken)
	if !ok {
		return nil, fsxBadRequest("The NextToken is not valid.")
	}

	goCtx := context.Background()

	// If specific IDs are requested, look them up directly.
	if len(input.FileSystemIDs) > 0 {
		result := make([]map[string]interface{}, 0, len(input.FileSystemIDs))
		for _, id := range input.FileSystemIDs {
			// loadFileSystem answers FileSystemNotFound for a deleted ID, as the page says, so an
			// SDK deletion waiter completes on the first poll.
			fs, err := p.loadFileSystem(ctx, id)
			if err != nil {
				return nil, err
			}
			result = append(result, fsxToWire(*fs))
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
	// A store error is returned rather than skipped: a skipped record would shorten the listing and
	// shift every later offset, which a caller paging through cannot detect.
	live := make([]FSxFileSystem, 0, len(ids))
	for _, id := range ids {
		data, getErr := p.state.Get(goCtx, fsxNamespace, fsxKey(ctx.AccountID, ctx.Region, id))
		if getErr != nil {
			return nil, fmt.Errorf("fsx describeFileSystems get %s: %w", id, getErr)
		}
		if data == nil {
			continue
		}
		var fs FSxFileSystem
		if unmarshalErr := json.Unmarshal(data, &fs); unmarshalErr != nil {
			return nil, fmt.Errorf("fsx describeFileSystems unmarshal %s: %w", id, unmarshalErr)
		}
		if fs.Lifecycle != fsxLifecycleLegacyDeleted {
			live = append(live, fs)
		}
	}
	page, next := pageByOffsetToken(live, offset, pageSize)
	result := make([]map[string]interface{}, 0, len(page))
	for _, fs := range page {
		result = append(result, fsxToWire(fs))
	}
	out := map[string]interface{}{"FileSystems": result}
	if next != "" {
		out["NextToken"] = next
	}
	return fsxJSONResponse(http.StatusOK, out)
}

// fsxMaxListItems is the published "Maximum number of 50 items" on both DescribeFileSystems'
// FileSystemIds request member and its FileSystems response member, and so the page size.
const fsxMaxListItems = 50

// fsxFileSystemTypes and fsxStorageTypes are the Valid Values API_CreateFileSystem publishes for
// FileSystemType and StorageType.
var (
	fsxFileSystemTypes = map[string]bool{"WINDOWS": true, "LUSTRE": true, "ONTAP": true, "OPENZFS": true}
	fsxStorageTypes    = map[string]bool{"SSD": true, "HDD": true, "INTELLIGENT_TIERING": true}
)

// fsxValidateCreate checks the members API_CreateFileSystem marks Required: Yes, and the published
// constraints on the ones it reads, before any of them is used (#1197).
//
// FileSystemType and SubnetIds are the two Required: Yes members. Both were defaulted or accepted
// absent: FileSystemType became LUSTRE, and a file system was created in no subnet. Each absence now
// answers BadRequest/400, the operation's published generic client-error code (fsx_errors.go), as
// the rest of the plugin does.
//
//   - FileSystemType must be one of `WINDOWS | LUSTRE | ONTAP | OPENZFS`, exactly. The handler used
//     to upper-case it, which accepted `lustre`; an enum on the wire is case-sensitive.
//   - StorageType, Required: No and defaulted to SSD as the page says, must be one of
//     `SSD | HDD | INTELLIGENT_TIERING` when present.
//   - SubnetIds publishes "Maximum number of 50 items", which is enforced.
//
// Declined, with the reason: SubnetIds' `^(subnet-[0-9a-f]{8,})$` pattern. Substrate does not
// require a subnet to exist in EC2 state — the VPC lookup is best effort — and fixtures and
// templates throughout the tree name symbolic subnets (`subnet-a`). Enforcing the pattern would
// refuse those without making the subnet any more real; the presence check is what catches the
// omission a generated template makes. The per-type StorageCapacity rules are likewise not
// enforced: the valid values depend on the deployment type and storage type in ways the page
// states only in prose.
func fsxValidateCreate(fileSystemType, storageType string, subnetIDs []string) error {
	if fileSystemType == "" {
		return fsxBadRequest("FileSystemType is required.")
	}
	if !fsxFileSystemTypes[fileSystemType] {
		return fsxBadRequest(fmt.Sprintf("FileSystemType %q is not one of WINDOWS, LUSTRE, ONTAP or OPENZFS.", fileSystemType))
	}
	if storageType != "" && !fsxStorageTypes[storageType] {
		return fsxBadRequest(fmt.Sprintf("StorageType %q is not one of SSD, HDD or INTELLIGENT_TIERING.", storageType))
	}
	if len(subnetIDs) == 0 {
		return fsxBadRequest("SubnetIds is required.")
	}
	if len(subnetIDs) > fsxMaxListItems {
		return fsxBadRequest(fmt.Sprintf("SubnetIds may name at most %d subnets.", fsxMaxListItems))
	}
	return nil
}

func (p *FSxPlugin) deleteFileSystem(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ClientRequestToken   string           `json:"ClientRequestToken"`
		FileSystemID         string           `json:"FileSystemId"`
		LustreConfiguration  *fsxDeleteConfig `json:"LustreConfiguration"`
		OpenZFSConfiguration *fsxDeleteConfig `json:"OpenZFSConfiguration"`
		WindowsConfiguration *fsxDeleteConfig `json:"WindowsConfiguration"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, fsxInvalidBody()
		}
	}
	if input.FileSystemID == "" {
		return nil, fsxBadRequest("FileSystemId is required")
	}

	// A token already used for this delete answers the delete's first response: the file system is
	// gone by now, and "idempotent deletion" is the token's whole published purpose.
	var fingerprint string
	if input.ClientRequestToken != "" {
		fp, err := fsxRequestFingerprint(req.Body)
		if err != nil {
			return nil, err
		}
		fingerprint = fp
		rec, found, err := p.fsxCheckToken(ctx, "DeleteFileSystem", input.ClientRequestToken, fingerprint)
		if err != nil {
			return nil, err
		}
		if found {
			return &AWSResponse{
				StatusCode: http.StatusOK,
				Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
				Body:       rec.Response,
			}, nil
		}
	}

	fs, err := p.loadFileSystem(ctx, input.FileSystemID)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(fsxDeleteToWire(*fs, input.LustreConfiguration, input.OpenZFSConfiguration, input.WindowsConfiguration))
	if err != nil {
		return nil, fmt.Errorf("fsx deleteFileSystem marshal: %w", err)
	}

	goCtx := context.Background()
	if err := p.state.Delete(goCtx, fsxNamespace, fsxKey(ctx.AccountID, ctx.Region, input.FileSystemID)); err != nil {
		return nil, fmt.Errorf("fsx deleteFileSystem delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, fsxNamespace, fsxIDsKey(ctx.AccountID, ctx.Region), input.FileSystemID)
	if input.ClientRequestToken != "" {
		rec := fsxTokenRecord{FileSystemID: input.FileSystemID, Fingerprint: fingerprint, Response: body}
		if err := p.fsxRecordToken(ctx, "DeleteFileSystem", input.ClientRequestToken, rec); err != nil {
			return nil, err
		}
	}

	return &AWSResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"Content-Type": "application/x-amz-json-1.1"},
		Body:       body,
	}, nil
}

// loadFileSystem reads one file system, answering FileSystemNotFound when it does not exist or was
// soft-deleted by a recording made before #1210.
func (p *FSxPlugin) loadFileSystem(ctx *RequestContext, id string) (*FSxFileSystem, error) {
	data, err := p.state.Get(context.Background(), fsxNamespace, fsxKey(ctx.AccountID, ctx.Region, id))
	if err != nil {
		return nil, fmt.Errorf("fsx loadFileSystem get: %w", err)
	}
	notFound := &AWSError{
		Code:       "FileSystemNotFound",
		Message:    fmt.Sprintf("File system '%s' does not exist.", id),
		HTTPStatus: http.StatusBadRequest,
	}
	if data == nil {
		return nil, notFound
	}
	var fs FSxFileSystem
	if err := json.Unmarshal(data, &fs); err != nil {
		return nil, fmt.Errorf("fsx loadFileSystem unmarshal: %w", err)
	}
	if fs.Lifecycle == fsxLifecycleLegacyDeleted {
		return nil, notFound
	}
	return &fs, nil
}

// --- Wire format helpers -----------------------------------------------------

// fsxToWire converts an FSxFileSystem to the AWS wire-format map.
//
// It answers thirteen of API_FileSystem's twenty-two members (twelve for a file system that is not
// LUSTRE), every one Required: No (#1199):
// CreationTime, DNSName, FileSystemId, FileSystemType, Lifecycle, LustreConfiguration (for LUSTRE),
// OwnerId, ResourceARN, StorageCapacity, StorageType, SubnetIds, Tags and VpcId. The nine it does not
// answer are absent because the record holds no value for them, not dropped on the way out:
//
//   - AdministrativeActions and FailureDetails: no UpdateFileSystem is routed and no file system
//     fails, so there is never an action in flight or a failure to describe.
//   - FileSystemTypeVersion, KmsKeyId and NetworkType: CreateFileSystem does not record them. Each
//     has a published default the page states only conditionally (the Lustre version depends on the
//     deployment type and metadata mode), so answering one would be a guess presented as a fact.
//   - NetworkInterfaceIds: no elastic network interface is created.
//   - WindowsConfiguration, OntapConfiguration and OpenZFSConfiguration: the per-type configuration
//     is not modeled beyond Lustre's DeploymentType and MountName.
//
// Of AWS::FSx::FileSystem's four published Fn::GetAtt attributes, DNSName, ResourceARN and
// LustreMountName resolve. RootVolumeId — "the root volume ID of the FSx for OpenZFS file system" —
// does not, because OpenZFS volumes are not modeled and minting an ID for a volume that does not
// exist would be an invented value; it resolves empty, as an attribute substrate retains nothing for.
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
		"CreationTime":    fsxCreationTime(fs.CreationTime),
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
