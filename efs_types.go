package substrate

import (
	"strings"
	"time"
)

// efsNamespace is the state namespace used by EFSPlugin.
const efsNamespace = "efs"

// EFSFileSystem represents an Amazon EFS file system.
type EFSFileSystem struct {
	// FileSystemID is the EFS file system identifier (e.g. fs-a1b2c3d4).
	FileSystemID string `json:"FileSystemId"`

	// FileSystemArn is the full ARN of the file system.
	FileSystemArn string `json:"FileSystemArn"`

	// OwnerID is the AWS account ID of the file system owner.
	OwnerID string `json:"OwnerId"`

	// CreationToken is the idempotency token used on creation.
	CreationToken string `json:"CreationToken"`

	// LifeCycleState is the lifecycle state of the file system.
	LifeCycleState string `json:"LifeCycleState"`

	// Name is an optional display name for the file system.
	Name string `json:"Name,omitempty"`

	// NumberOfMountTargets is the number of mount targets for this file system.
	NumberOfMountTargets int `json:"NumberOfMountTargets"`

	// PerformanceMode is the performance mode of the file system.
	PerformanceMode string `json:"PerformanceMode"`

	// ThroughputMode is the throughput mode of the file system.
	ThroughputMode string `json:"ThroughputMode"`

	// Encrypted indicates whether the file system is encrypted.
	Encrypted bool `json:"Encrypted"`

	// Tags is the list of tags for the file system.
	Tags []EFSTag `json:"Tags"`

	// CreatedAt is the time the file system was created.
	CreatedAt time.Time `json:"CreatedAt"`

	// AccountID is the AWS account ID owning this resource.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where this file system resides.
	Region string `json:"Region"`
}

// EFSAccessPoint represents an Amazon EFS access point.
type EFSAccessPoint struct {
	// AccessPointID is the access point identifier (e.g. fsap-a1b2c3d4).
	AccessPointID string `json:"AccessPointId"`

	// AccessPointArn is the full ARN of the access point.
	AccessPointArn string `json:"AccessPointArn"`

	// FileSystemID is the ID of the parent file system.
	FileSystemID string `json:"FileSystemId"`

	// LifeCycleState is the lifecycle state of the access point.
	LifeCycleState string `json:"LifeCycleState"`

	// Name is an optional display name for the access point.
	Name string `json:"Name,omitempty"`

	// PosixUser is the POSIX user identity for the access point.
	PosixUser *EFSPosixUser `json:"PosixUser,omitempty"`

	// RootDirectory is the root directory configuration for the access point.
	RootDirectory *EFSRootDirectory `json:"RootDirectory,omitempty"`

	// Tags is the list of tags for the access point.
	Tags []EFSTag `json:"Tags"`

	// AccountID is the AWS account ID owning this resource.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where this access point resides.
	Region string `json:"Region"`
}

// EFSMountTarget represents an Amazon EFS mount target.
type EFSMountTarget struct {
	// MountTargetID is the mount target identifier (e.g. fsmt-a1b2c3d4).
	MountTargetID string `json:"MountTargetId"`

	// FileSystemID is the ID of the parent file system.
	FileSystemID string `json:"FileSystemId"`

	// SubnetID is the VPC subnet in which the mount target resides.
	SubnetID string `json:"SubnetId"`

	// LifeCycleState is the lifecycle state of the mount target.
	LifeCycleState string `json:"LifeCycleState"`

	// IPAddress is the IP address of the mount target.
	IPAddress string `json:"IpAddress,omitempty"`

	// VpcID is the VPC containing the mount target.
	VpcID string `json:"VpcId,omitempty"`

	// AccountID is the AWS account ID owning this resource.
	AccountID string `json:"AccountID"`

	// Region is the AWS region where this mount target resides.
	Region string `json:"Region"`
}

// EFSTag is a key-value tag for an EFS resource.
type EFSTag struct {
	// Key is the tag key.
	Key string `json:"Key"`

	// Value is the tag value.
	Value string `json:"Value"`
}

// EFSPosixUser describes the POSIX user identity for an EFS access point.
type EFSPosixUser struct {
	// UID is the POSIX user ID.
	UID int64 `json:"Uid"`

	// GID is the POSIX group ID.
	GID int64 `json:"Gid"`
}

// EFSRootDirectory describes the root directory configuration for an EFS access point.
type EFSRootDirectory struct {
	// Path is the path on the EFS file system to use as the root directory.
	Path string `json:"Path"`
}

// parseEFSOperation maps an HTTP method and URL path to an EFS operation name
// and optional resource ID. It is the EFS equivalent of parseRoute53Operation.
func parseEFSOperation(method, path string) (op, resourceID string) {
	// Strip the version prefix /2015-02-01.
	const versionPrefix = "/2015-02-01"
	rest := strings.TrimPrefix(path, versionPrefix)

	// /file-systems[/{id}]
	if strings.HasPrefix(rest, "/file-systems") {
		suffix := strings.TrimPrefix(rest, "/file-systems")
		suffix = strings.TrimPrefix(suffix, "/")
		// /file-systems/{id}/resource-tags is handled below.
		switch method {
		case "POST":
			return "CreateFileSystem", ""
		case "GET":
			return "DescribeFileSystems", suffix
		case "PUT":
			return "UpdateFileSystem", suffix
		case "DELETE":
			return "DeleteFileSystem", suffix
		}
	}

	// /access-points[/{id}]
	if strings.HasPrefix(rest, "/access-points") {
		suffix := strings.TrimPrefix(rest, "/access-points")
		suffix = strings.TrimPrefix(suffix, "/")
		switch method {
		case "POST":
			return "CreateAccessPoint", ""
		case "GET":
			return "DescribeAccessPoints", suffix
		case "DELETE":
			return "DeleteAccessPoint", suffix
		}
	}

	// /mount-targets[/{id}]
	if strings.HasPrefix(rest, "/mount-targets") {
		suffix := strings.TrimPrefix(rest, "/mount-targets")
		suffix = strings.TrimPrefix(suffix, "/")
		switch method {
		case "POST":
			return "CreateMountTarget", ""
		case "GET":
			return "DescribeMountTargets", suffix
		case "DELETE":
			return "DeleteMountTarget", suffix
		}
	}

	// /resource-tags/{resourceID}
	if strings.HasPrefix(rest, "/resource-tags/") {
		id := strings.TrimPrefix(rest, "/resource-tags/")
		switch method {
		case "POST":
			return "TagResource", id
		case "GET":
			return "ListTagsForResource", id
		case "DELETE":
			return "UntagResource", id
		}
	}

	return method, ""
}
