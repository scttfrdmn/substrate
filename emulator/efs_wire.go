package emulator

// EFS answered its persisted records straight to the caller. The types below are the
// published shapes, projected from those records, on the pattern emulator/ecr_wire.go
// established (#1090) and apigateway_wire.go before it (#529).
//
// All three EFS records — EFSFileSystem, EFSAccessPoint and EFSMountTarget (efs_types.go)
// — were handed to efsJSONResponse directly at every one of the ten sites that answer a
// resource, so every one of those responses carried AccountID and Region unconditionally
// (neither has omitempty), `ever_tagged` from whichever record had been tagged, and, on a
// file system, CreatedAt as an RFC3339 string. AWS publishes none of the four.
//
// The last is the reason EFS is the first service fixed after ECR: it is not merely an
// extra member but a near-miss of a real one. API_FileSystemDescription publishes
// **CreationTime**, "the time that the file system was created, in seconds (since
// 1970-01-01T00:00:00Z)", Required: Yes. A consumer reading CreationTime off a substrate
// response got nothing, while a member one letter-group away held the value in the wrong
// spelling and the wrong type.
//
// Projecting also closes three members substrate published nowhere, all of which the
// reference names:
//
//   - CreationTime and SizeInBytes on a file system, both Required: Yes.
//   - OwnerId on an access point and on a mount target (Required: No on both
//     API_AccessPointDescription and API_MountTargetDescription). The file system already
//     reported it from its own OwnerID field; the other two records held the account only
//     in the AccountID they were leaking, so the fix is the same value under its published
//     name rather than a deletion.
//
// The records keep AccountID, Region and EverTagged, untouched. The state key already
// scopes by account and Region so nothing reads those two back, but a persisted member is
// not free to remove: ecr_wire.go's rule is that the record's encoding is what
// MemoryStateManager snapshots and a replay reads, so a projection changes the response
// and leaves the record alone. EverTagged is read across a service boundary besides —
// TaggingPlugin.scanEFSFileSystems reports it and the efsNamespace merge arm writes it.
// The nine baseline lines therefore stay in scripts/wire-bookkeeping-baseline.txt and are
// discharged in scripts/wire-bookkeeping-projected.txt instead.
//
// # One provenance note, so the shape is not "fixed" backwards
//
// AWS's own API_CreateFileSystem page disagrees with itself. Its Response Syntax publishes
// `"OwnerId"`, `"FileSystemId"`, `"NumberOfMountTargets"` and `"CreationTime": number`;
// both of its sample responses render `"ownerId"`, `"fileSystemId"`,
// `"numberOfMountTargets"` and `"CreationTime":"1403301078"` — lower camel, and the
// timestamp quoted as a string. The Response Syntax is what the SDK decoder follows, so it
// is what these types match. The samples are stale, and the disagreement is recorded here
// because someone comparing substrate against the example would otherwise read the match
// as the defect.

// efsFileSystemSizeOut is the SizeInBytes member of a file-system response.
//
// API_FileSystemSize publishes Value (Long, minimum 0, Required: Yes) and Timestamp
// (Required: No), plus ValueInArchive, ValueInIA and ValueInStandard. The three
// storage-class breakdowns are absent from this type rather than present and zero:
// substrate models no storage classes, and #1013's rule is to report nothing AWS would
// not rather than to report a zero that reads as a measurement.
//
// Value is always 0 because substrate stores no file data — the metered size of a file
// system nothing has ever written to. Timestamp is the file system's own creation time for
// the same reason: 0 was determined when the empty file system was created and nothing has
// modified it since, so an advancing timestamp would claim a re-measurement that never
// happened.
type efsFileSystemSizeOut struct {
	Value     int64        `json:"Value"`
	Timestamp EpochSeconds `json:"Timestamp"`
}

// efsFileSystemOut is the FileSystemDescription of the EFS file-system responses:
// CreateFileSystem, UpdateFileSystem, and — under the name FileSystems —
// DescribeFileSystems.
//
// Member names follow API_FileSystemDescription. The members substrate does not model —
// AvailabilityZoneId, AvailabilityZoneName, FileSystemProtection, KmsKeyId and
// ProvisionedThroughputInMibps, all Required: No — are absent from the type rather than
// present and empty.
//
// Tags carries no omitempty, and is projected through a non-nil slice, because Tags is
// Required: Yes: a file system created without tags must answer `[]` and not `null`.
type efsFileSystemOut struct {
	FileSystemID         string               `json:"FileSystemId"`
	FileSystemArn        string               `json:"FileSystemArn"`
	OwnerID              string               `json:"OwnerId"`
	CreationToken        string               `json:"CreationToken"`
	CreationTime         EpochSeconds         `json:"CreationTime"`
	LifeCycleState       string               `json:"LifeCycleState"`
	Name                 string               `json:"Name,omitempty"`
	NumberOfMountTargets int                  `json:"NumberOfMountTargets"`
	PerformanceMode      string               `json:"PerformanceMode"`
	ThroughputMode       string               `json:"ThroughputMode"`
	Encrypted            bool                 `json:"Encrypted"`
	SizeInBytes          efsFileSystemSizeOut `json:"SizeInBytes"`
	Tags                 []EFSTag             `json:"Tags"`
}

// efsFileSystemToWire projects a persisted file system onto the published shape.
func efsFileSystemToWire(fs EFSFileSystem) efsFileSystemOut {
	return efsFileSystemOut{
		FileSystemID:         fs.FileSystemID,
		FileSystemArn:        fs.FileSystemArn,
		OwnerID:              fs.OwnerID,
		CreationToken:        fs.CreationToken,
		CreationTime:         EpochSeconds(fs.CreatedAt),
		LifeCycleState:       fs.LifeCycleState,
		Name:                 fs.Name,
		NumberOfMountTargets: fs.NumberOfMountTargets,
		PerformanceMode:      fs.PerformanceMode,
		ThroughputMode:       fs.ThroughputMode,
		Encrypted:            fs.Encrypted,
		SizeInBytes: efsFileSystemSizeOut{
			Value:     0,
			Timestamp: EpochSeconds(fs.CreatedAt),
		},
		Tags: efsTagsOrEmpty(fs.Tags),
	}
}

// efsFileSystemsToWire projects a slice of persisted file systems, for
// DescribeFileSystems.
func efsFileSystemsToWire(filesystems []EFSFileSystem) []efsFileSystemOut {
	out := make([]efsFileSystemOut, 0, len(filesystems))
	for _, fs := range filesystems {
		out = append(out, efsFileSystemToWire(fs))
	}
	return out
}

// efsAccessPointOut is the AccessPointDescription of CreateAccessPoint and — under the name
// AccessPoints — DescribeAccessPoints.
//
// Every member of API_AccessPointDescription is Required: No. ClientToken is absent:
// CreateAccessPoint decodes no idempotency token, so substrate has none to report, and
// reporting an empty one would claim the request carried it.
type efsAccessPointOut struct {
	AccessPointID  string            `json:"AccessPointId"`
	AccessPointArn string            `json:"AccessPointArn"`
	FileSystemID   string            `json:"FileSystemId"`
	OwnerID        string            `json:"OwnerId"`
	LifeCycleState string            `json:"LifeCycleState"`
	Name           string            `json:"Name,omitempty"`
	PosixUser      *EFSPosixUser     `json:"PosixUser,omitempty"`
	RootDirectory  *EFSRootDirectory `json:"RootDirectory,omitempty"`
	Tags           []EFSTag          `json:"Tags"`
}

// efsAccessPointToWire projects a persisted access point onto the published shape. OwnerId
// comes from the record's AccountID, which is the account that created it — the field was
// reaching the caller already, under a name AWS does not publish.
func efsAccessPointToWire(ap EFSAccessPoint) efsAccessPointOut {
	return efsAccessPointOut{
		AccessPointID:  ap.AccessPointID,
		AccessPointArn: ap.AccessPointArn,
		FileSystemID:   ap.FileSystemID,
		OwnerID:        ap.AccountID,
		LifeCycleState: ap.LifeCycleState,
		Name:           ap.Name,
		PosixUser:      ap.PosixUser,
		RootDirectory:  ap.RootDirectory,
		Tags:           efsTagsOrEmpty(ap.Tags),
	}
}

// efsAccessPointsToWire projects a slice of persisted access points, for
// DescribeAccessPoints.
func efsAccessPointsToWire(accessPoints []EFSAccessPoint) []efsAccessPointOut {
	out := make([]efsAccessPointOut, 0, len(accessPoints))
	for _, ap := range accessPoints {
		out = append(out, efsAccessPointToWire(ap))
	}
	return out
}

// efsMountTargetOut is the MountTargetDescription of CreateMountTarget and — under the name
// MountTargets — DescribeMountTargets.
//
// FileSystemId, LifeCycleState, MountTargetId and SubnetId are Required: Yes on
// API_MountTargetDescription and carry no omitempty for that reason. AvailabilityZoneId,
// AvailabilityZoneName, Ipv6Address and NetworkInterfaceId are Required: No and absent:
// substrate models no subnet topology and creates no network interface, so it has no value
// for any of them.
type efsMountTargetOut struct {
	MountTargetID  string `json:"MountTargetId"`
	FileSystemID   string `json:"FileSystemId"`
	SubnetID       string `json:"SubnetId"`
	OwnerID        string `json:"OwnerId"`
	LifeCycleState string `json:"LifeCycleState"`
	IPAddress      string `json:"IpAddress,omitempty"`
	VpcID          string `json:"VpcId,omitempty"`
}

// efsMountTargetToWire projects a persisted mount target onto the published shape.
func efsMountTargetToWire(mt EFSMountTarget) efsMountTargetOut {
	return efsMountTargetOut{
		MountTargetID:  mt.MountTargetID,
		FileSystemID:   mt.FileSystemID,
		SubnetID:       mt.SubnetID,
		OwnerID:        mt.AccountID,
		LifeCycleState: mt.LifeCycleState,
		IPAddress:      mt.IPAddress,
		VpcID:          mt.VpcID,
	}
}

// efsMountTargetsToWire projects a slice of persisted mount targets, for
// DescribeMountTargets.
func efsMountTargetsToWire(mountTargets []EFSMountTarget) []efsMountTargetOut {
	out := make([]efsMountTargetOut, 0, len(mountTargets))
	for _, mt := range mountTargets {
		out = append(out, efsMountTargetToWire(mt))
	}
	return out
}

// efsTagsOrEmpty returns a non-nil tag slice, so a resource with no tags answers `[]`
// rather than `null`. Tags is Required: Yes on API_FileSystemDescription, and a null there
// is a member AWS never reports.
func efsTagsOrEmpty(tags []EFSTag) []EFSTag {
	if tags == nil {
		return []EFSTag{}
	}
	return tags
}
