package emulator

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
)

// ec2Namespace is the service name used in state keys.
const ec2Namespace = "ec2"

// EC2InstanceState holds the code and name of an EC2 instance state.
type EC2InstanceState struct {
	// Code is the numeric state code (0=pending, 16=running, 32=shutting-down,
	// 48=terminated, 64=stopping, 80=stopped).
	Code int `json:"code"`

	// Name is the human-readable state name.
	Name string `json:"name"`
}

// EC2Tag is a key-value tag attached to an EC2 resource.
type EC2Tag struct {
	// Key is the tag key.
	Key string `json:"key"`

	// Value is the tag value.
	Value string `json:"value"`
}

// EC2Instance represents an Amazon EC2 instance.
type EC2Instance struct {
	// InstanceID is the unique identifier for the instance (e.g. "i-0123456789abcdef0").
	InstanceID string `json:"instance_id"`

	// ReservationID groups instances launched together.
	ReservationID string `json:"reservation_id"`

	// ImageID is the AMI used to launch the instance.
	ImageID string `json:"image_id"`

	// InstanceType is the EC2 instance type (e.g. "t3.micro").
	InstanceType string `json:"instance_type"`

	// State is the current instance lifecycle state.
	State EC2InstanceState `json:"state"`

	// SubnetID is the VPC subnet the instance was launched into.
	SubnetID string `json:"subnet_id"`

	// VPCID is the VPC the instance belongs to.
	VPCID string `json:"vpc_id"`

	// PrivateIPAddress is the primary private IPv4 address.
	PrivateIPAddress string `json:"private_ip_address"`

	// PublicIPAddress is the public IPv4 address (empty for VPC-only instances).
	PublicIPAddress string `json:"public_ip_address"`

	// PublicDNSName is the public DNS hostname for the instance.
	PublicDNSName string `json:"public_dns_name,omitempty"`

	// PrivateDNSName is the private DNS hostname for the instance.
	PrivateDNSName string `json:"private_dns_name,omitempty"`

	// SecurityGroupIDs holds the security groups attached to the instance.
	SecurityGroupIDs []string `json:"security_group_ids"`

	// Tags holds key-value metadata tags.
	Tags []EC2Tag `json:"tags,omitempty"`

	// LaunchTime is the UTC time at which the instance was launched.
	LaunchTime string `json:"launch_time"`

	// TerminatedTime is the UTC time at which the instance was terminated,
	// or empty if the instance has not been terminated.
	TerminatedTime string `json:"terminated_time,omitempty"`

	// AccountID is the AWS account that owns the instance.
	AccountID string `json:"account_id"`

	// Region is the AWS region in which the instance runs.
	Region string `json:"region"`

	// KeyName is the name of the key pair used to launch the instance.
	KeyName string `json:"key_name,omitempty"`

	// IamInstanceProfile is the ARN or name of the IAM instance profile attached
	// at launch (echoed back so callers can verify it was applied).
	IamInstanceProfile string `json:"iam_instance_profile,omitempty"`

	// UserData is the base64-encoded user-data supplied at launch (stored so
	// callers can verify it was accepted; not executed).
	UserData string `json:"user_data,omitempty"`

	// DisableAPITermination reports whether termination protection is enabled.
	// AWS documents the default as false, which is the zero value, so an instance
	// unmarshalled from an event log predating this field reads back correctly.
	//
	// TerminateInstances honors it (#489), refusing with OperationNotPermitted.
	// Because the refusal is scoped to the Availability Zone rather than to the
	// request or the instance, it is read together with AvailabilityZone; see
	// [ec2TerminationProtectionBlocked].
	DisableAPITermination bool `json:"disable_api_termination,omitempty"`

	// AvailabilityZone is the zone the instance runs in, taken from
	// Placement.AvailabilityZone or from the subnet's zone when the launch named
	// neither. An instance unmarshalled from an event log predating this field
	// reads back as the empty string, which groups all such instances into one
	// zone — the conservative reading, since it is what a single-zone account
	// looks like.
	AvailabilityZone string `json:"availability_zone,omitempty"`

	// NetworkInterfaces holds every interface the launch declared, in DeviceIndex
	// order, with the primary (DeviceIndex 0) first.
	//
	// The flat SubnetID/PrivateIPAddress/SecurityGroupIDs fields above continue to
	// report the *primary* interface's values, which is what real EC2 puts at the
	// top level of an instance — they are not superseded by this slice. An instance
	// unmarshalled from an event log predating this field reads back with an empty
	// slice, and its flat fields still describe its one interface, so a replayed log
	// still describes correctly (#455).
	NetworkInterfaces []EC2NetworkInterface `json:"network_interfaces,omitempty"`
}

// EC2NetworkInterface is one interface attached to an instance at launch, from a
// RunInstances NetworkInterface.N specification or a launch template's.
//
// Only the members substrate can observe something about are held. AWS's
// InstanceNetworkInterfaceSpecification has some forty members; recording a value
// substrate never reports would be a write with no observable, and reporting one it
// never received would be a claim nothing backs.
type EC2NetworkInterface struct {
	// NetworkInterfaceID is the interface's ID, either the one the request attached
	// by NetworkInterfaceId or one substrate minted for an interface the launch
	// created.
	NetworkInterfaceID string `json:"network_interface_id"`

	// DeviceIndex is "the position of the network interface in the attachment
	// order. A primary network interface has a device index of 0."
	//
	// This is the interface's identity, not its position in the request: AWS keys on
	// DeviceIndex and the two need not agree, so NetworkInterface.1 may perfectly
	// well declare DeviceIndex 3.
	DeviceIndex int `json:"device_index"`

	// SubnetID is the subnet the interface is in.
	SubnetID string `json:"subnet_id,omitempty"`

	// PrivateIPAddress is the interface's primary private IPv4 address, either the
	// one the request named or one substrate assigned from the subnet's range.
	PrivateIPAddress string `json:"private_ip_address,omitempty"`

	// PrivateDNSName is the interface's private DNS hostname.
	PrivateDNSName string `json:"private_dns_name,omitempty"`

	// SecurityGroupIDs holds the interface's security groups.
	SecurityGroupIDs []string `json:"security_group_ids,omitempty"`

	// AssociatePublicIPAddress is the interface's public-IP preference, stored
	// verbatim as a string for the same three-state reason
	// [EC2LaunchTemplateData.AssociatePublicIPAddress] gives: absent, "true" and
	// "false" are three distinguishable requests and a bool collapses two of them.
	AssociatePublicIPAddress string `json:"associate_public_ip_address,omitempty"`

	// Description is the interface's description, which "applies only if creating a
	// network interface when launching an instance".
	Description string `json:"description,omitempty"`

	// DeleteOnTermination reports whether the interface is deleted with the
	// instance. AWS defaults a launch-created interface to true; an attached
	// existing one to false.
	DeleteOnTermination bool `json:"delete_on_termination"`

	// InterfaceType is "the type of network interface": interface, efa or efa-only.
	// Absent reads back as "interface", the documented default.
	InterfaceType string `json:"interface_type,omitempty"`

	// NetworkCardIndex is "the index of the network card", which defaults to 0 and
	// which "the primary network interface must be assigned to".
	NetworkCardIndex int `json:"network_card_index,omitempty"`
}

// EC2KeyPair represents an EC2 key pair (public/private key used for SSH access).
type EC2KeyPair struct {
	// KeyPairID is the AWS-assigned identifier for the key pair.
	KeyPairID string `json:"keyPairId"`

	// KeyName is the user-supplied name for the key pair.
	KeyName string `json:"keyName"`

	// Fingerprint is the SHA-256 fingerprint of the public key.
	Fingerprint string `json:"fingerprint"`

	// KeyType is the type of key pair (e.g. "rsa" or "ed25519").
	KeyType string `json:"keyType"`

	// CreatedAt is the RFC3339 timestamp when the key pair was created.
	CreatedAt string `json:"createdAt,omitempty"`

	// Tags holds key-value metadata tags.
	//
	// Spelled "tags" like every other EC2 record's, rather than following this struct's
	// camelCase members: that is the member [EC2Plugin.applyTagsToResource] and
	// [EC2Plugin.scanTags] read and write on every type, and a key pair spelling it
	// differently would be tagged into a member nothing reports (#708).
	Tags []EC2Tag `json:"tags,omitempty"`

	// AccountID is the AWS account that owns the key pair.
	AccountID string `json:"accountId"`

	// Region is the AWS region where the key pair is stored.
	Region string `json:"region"`
}

// EC2VPC represents an Amazon Virtual Private Cloud.
type EC2VPC struct {
	// VPCID is the unique identifier for the VPC.
	VPCID string `json:"vpc_id"`

	// CIDRBlock is the primary IPv4 CIDR block.
	CIDRBlock string `json:"cidr_block"`

	// IsDefault indicates whether this is the account's default VPC.
	IsDefault bool `json:"is_default"`

	// State is the VPC state: "pending" or "available".
	State string `json:"state"`

	// EnableDNSSupport indicates whether DNS resolution is enabled for the VPC.
	EnableDNSSupport bool `json:"enable_dns_support"`

	// EnableDNSHostnames indicates whether instances receive public DNS hostnames.
	EnableDNSHostnames bool `json:"enable_dns_hostnames"`

	// Tags holds key-value metadata tags.
	Tags []EC2Tag `json:"tags,omitempty"`

	// AccountID is the AWS account that owns the VPC.
	AccountID string `json:"account_id"`

	// Region is the AWS region in which the VPC resides.
	Region string `json:"region"`
}

// EC2Subnet represents a VPC subnet.
type EC2Subnet struct {
	// SubnetID is the unique identifier for the subnet.
	SubnetID string `json:"subnet_id"`

	// VPCID is the VPC this subnet belongs to.
	VPCID string `json:"vpc_id"`

	// CIDRBlock is the IPv4 CIDR block for the subnet.
	CIDRBlock string `json:"cidr_block"`

	// AvailabilityZone is the availability zone for the subnet.
	AvailabilityZone string `json:"availability_zone"`

	// IsDefault indicates whether this is the account's default subnet.
	IsDefault bool `json:"is_default"`

	// MapPublicIPOnLaunch indicates whether instances launched into this subnet
	// automatically receive a public IPv4 address.
	MapPublicIPOnLaunch bool `json:"map_public_ip_on_launch"`

	// State is the subnet state: "pending" or "available".
	State string `json:"state"`

	// Tags holds key-value metadata tags.
	Tags []EC2Tag `json:"tags,omitempty"`

	// AccountID is the AWS account that owns the subnet.
	AccountID string `json:"account_id"`

	// Region is the AWS region in which the subnet resides.
	Region string `json:"region"`
}

// EC2IPPermission represents an inbound or outbound IP permission rule in a
// security group.
type EC2IPPermission struct {
	// IPProtocol is the IP protocol name ("tcp", "udp", "icmp") or number,
	// or "-1" for all traffic.
	IPProtocol string `json:"ip_protocol"`

	// FromPort is the start of the port range (inclusive).
	FromPort int `json:"from_port"`

	// ToPort is the end of the port range (inclusive).
	ToPort int `json:"to_port"`

	// IPRanges holds the IPv4 CIDR ranges for this permission.
	IPRanges []string `json:"ip_ranges,omitempty"`

	// UserIDGroupPairs holds the source/destination security groups for this
	// permission. A rule whose source is another security group (including the
	// group itself) has no CIDR at all, so it cannot be represented by IPRanges
	// alone (#388).
	UserIDGroupPairs []EC2UserIDGroupPair `json:"user_id_group_pairs,omitempty"`
}

// EC2UserIDGroupPair identifies a security group referenced as the source or
// destination of a rule.
type EC2UserIDGroupPair struct {
	// GroupID is the referenced security group's ID.
	GroupID string `json:"group_id"`

	// GroupName is the referenced security group's name, used for EC2-Classic
	// style and default-VPC references.
	GroupName string `json:"group_name,omitempty"`

	// UserID is the AWS account that owns the referenced group.
	UserID string `json:"user_id,omitempty"`

	// Description is the optional rule description.
	Description string `json:"description,omitempty"`
}

// EC2SecurityGroup represents a VPC security group.
type EC2SecurityGroup struct {
	// GroupID is the unique identifier for the security group.
	GroupID string `json:"group_id"`

	// GroupName is the name of the security group.
	GroupName string `json:"group_name"`

	// Description is a description of the security group.
	Description string `json:"description"`

	// VPCID is the VPC this security group is associated with.
	VPCID string `json:"vpc_id"`

	// IngressRules holds the ingress (inbound) permission rules.
	IngressRules []EC2IPPermission `json:"ingress_rules,omitempty"`

	// EgressRules holds the egress (outbound) permission rules.
	EgressRules []EC2IPPermission `json:"egress_rules,omitempty"`

	// Tags holds key-value metadata tags.
	Tags []EC2Tag `json:"tags,omitempty"`

	// AccountID is the AWS account that owns the security group.
	AccountID string `json:"account_id"`

	// Region is the AWS region in which the security group resides.
	Region string `json:"region"`
}

// EC2IGWAttachment represents the attachment of an internet gateway to a VPC.
type EC2IGWAttachment struct {
	// VPCID is the ID of the VPC the gateway is attached to.
	VPCID string `json:"vpc_id"`

	// State is the attachment state (e.g. "available").
	State string `json:"state"`
}

// EC2InternetGateway represents an Amazon VPC internet gateway.
type EC2InternetGateway struct {
	// InternetGatewayID is the unique identifier for the internet gateway.
	InternetGatewayID string `json:"internet_gateway_id"`

	// Attachments lists the VPCs this gateway is attached to.
	Attachments []EC2IGWAttachment `json:"attachments,omitempty"`

	// Tags holds key-value metadata tags.
	Tags []EC2Tag `json:"tags,omitempty"`

	// AccountID is the AWS account that owns the internet gateway.
	AccountID string `json:"account_id"`

	// Region is the AWS region in which the internet gateway resides.
	Region string `json:"region"`
}

// EC2Route represents a single route in a route table.
type EC2Route struct {
	// DestinationCIDR is the IPv4 destination CIDR block.
	DestinationCIDR string `json:"destination_cidr"`

	// GatewayID is the target gateway ID (e.g. "igw-..." or "local").
	GatewayID string `json:"gateway_id"`

	// State is the route state: "active" or "blackhole".
	State string `json:"state"`
}

// EC2RTAssociation represents an association between a route table and a subnet
// or gateway.
type EC2RTAssociation struct {
	// AssociationID is the unique identifier for this association.
	AssociationID string `json:"association_id"`

	// SubnetID is the subnet associated with the route table (may be empty for
	// gateway associations).
	SubnetID string `json:"subnet_id"`

	// Main indicates whether this is the main (default) route table association.
	Main bool `json:"main"`
}

// EC2RouteTable represents a VPC route table.
type EC2RouteTable struct {
	// RouteTableID is the unique identifier for the route table.
	RouteTableID string `json:"route_table_id"`

	// VPCID is the VPC this route table is associated with.
	VPCID string `json:"vpc_id"`

	// Routes holds the routes in this table.
	Routes []EC2Route `json:"routes,omitempty"`

	// Associations holds subnet and gateway associations.
	Associations []EC2RTAssociation `json:"associations,omitempty"`

	// Tags holds key-value metadata tags.
	Tags []EC2Tag `json:"tags,omitempty"`

	// AccountID is the AWS account that owns the route table.
	AccountID string `json:"account_id"`

	// Region is the AWS region in which the route table resides.
	Region string `json:"region"`
}

// generateEC2InstanceID generates a random EC2 instance ID in the format
// "i-" followed by 17 hex characters.
func generateEC2InstanceID() string {
	return "i-" + randomHex(8)
}

// generateENIID generates a random elastic network interface ID in the format
// "eni-" followed by 8 hex characters.
func generateENIID() string {
	return "eni-" + randomHex(8)
}

// generateVPCID generates a random VPC ID in the format "vpc-" followed by
// 8 hex characters.
func generateVPCID() string {
	return "vpc-" + randomHex(8)
}

// generateSubnetID generates a random subnet ID in the format "subnet-"
// followed by 8 hex characters.
func generateSubnetID() string {
	return "subnet-" + randomHex(8)
}

// generateSGID generates a random security group ID in the format "sg-"
// followed by 8 hex characters.
func generateSGID() string {
	return "sg-" + randomHex(8)
}

// generateIGWID generates a random internet gateway ID in the format "igw-"
// followed by 8 hex characters.
func generateIGWID() string {
	return "igw-" + randomHex(8)
}

// generateRTBID generates a random route table ID in the format "rtb-"
// followed by 8 hex characters.
func generateRTBID() string {
	return "rtb-" + randomHex(8)
}

// generateKeyPairID generates a random EC2 key pair ID in the format "key-"
// followed by 17 hex characters.
func generateKeyPairID() string {
	return "key-" + randomHex(17)
}

// generateReservationID generates a random reservation ID in the format
// "r-" followed by 8 hex characters.
func generateReservationID() string {
	return "r-" + randomHex(8)
}

// generateAssociationID generates a random route table association ID.
func generateAssociationID() string {
	return "rtbassoc-" + randomHex(8)
}

// randomHex generates n random bytes returned as a lowercase hex string.
func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("ec2_types: rand.Read failed: %v", err))
	}
	return hex.EncodeToString(b)
}

// EC2Image represents an Amazon Machine Image (AMI) registered in Substrate.
type EC2Image struct {
	// ImageID is the AMI identifier (e.g. "ami-0123456789abcdef0").
	ImageID string `json:"image_id"`

	// Name is the user-supplied name for the AMI.
	Name string `json:"name"`

	// Description is the optional description for the AMI.
	Description string `json:"description"`

	// InstanceID is the source instance used to create the AMI.
	InstanceID string `json:"instance_id,omitempty"`

	// State is the image state: always "available" in Substrate.
	State string `json:"state"`

	// CreationDate is the RFC3339 timestamp when the AMI was registered.
	CreationDate string `json:"creation_date,omitempty"`

	// SnapshotID is the EBS snapshot backing the root device of this AMI, if any.
	// CreateImage materializes one so snapshot-retention logic can be tested;
	// RegisterImage takes it from the mapping RootDeviceName names, or from the first
	// mapping naming a snapshot when it names none — see [ec2RootMappingSnapshot].
	//
	// It stays a scalar beside BlockDeviceMappings rather than being derived from them
	// on every read because DeleteSnapshot's in-use rule is scoped to the *root*
	// device's snapshot (#710), so the distinction is load-bearing rather than
	// bookkeeping, and because it is the only snapshot member an AMI recorded before
	// #711 has.
	SnapshotID string `json:"snapshot_id,omitempty"`

	// BlockDeviceMappings holds the mapping entries a RegisterImage request sent, in
	// request order, so DescribeImages can render what the caller asked for rather than
	// the single fabricated /dev/sda1 item it rendered through v0.106.0 (#711).
	//
	// It is empty for every CreateImage-minted AMI — that path materializes one root
	// snapshot and has no caller mapping to record — and for any AMI registered before
	// #711. DescribeImages renders those from SnapshotID, which is why both members
	// exist; see [EC2Plugin.describeImages].
	BlockDeviceMappings []EC2BlockDeviceMapping `json:"block_device_mappings,omitempty"`

	// Tags holds key-value metadata tags.
	Tags []EC2Tag `json:"tags,omitempty"`

	// AccountID is the AWS account that owns the AMI.
	AccountID string `json:"account_id"`

	// Region is the AWS region in which the AMI is registered.
	Region string `json:"region"`
}

// EC2Snapshot represents an Amazon EBS snapshot.
type EC2Snapshot struct {
	// SnapshotID is the snapshot identifier (e.g. "snap-0123456789abcdef0").
	SnapshotID string `json:"snapshot_id"`

	// VolumeID is the source volume, if the snapshot was created from one.
	VolumeID string `json:"volume_id,omitempty"`

	// VolumeSize is the size of the volume, in GiB.
	VolumeSize int64 `json:"volume_size"`

	// State is the snapshot state, one of [ec2SnapshotStates]. Every snapshot Substrate
	// writes is "completed": nothing advances a snapshot asynchronously, so there is no
	// transition for the record to hold.
	//
	// A seeded progression (#715) does not change that. It overlays what an *observation*
	// reports — see [EC2Plugin.observeSnapshotStatus] — leaving this member "completed", so
	// clearing the seed makes the snapshot read completed again and a snapshot with no seed
	// against it is untouched.
	State string `json:"state"`

	// StartTime is the RFC3339 timestamp when the snapshot was started.
	StartTime string `json:"start_time"`

	// Encrypted reports whether the snapshot is encrypted.
	Encrypted bool `json:"encrypted"`

	// Description is the optional snapshot description.
	Description string `json:"description,omitempty"`

	// Tags holds key-value metadata tags.
	Tags []EC2Tag `json:"tags,omitempty"`

	// CreateVolumePermissions is the list of accounts and groups that may create volumes
	// from this snapshot, as ModifySnapshotAttribute sets it and
	// DescribeSnapshotAttribute reports it (#709).
	//
	// It is empty for a newly created or copied snapshot, which is what AWS describes a
	// reset snapshot as being — "a private snapshot that can only be used by the account
	// that created it". Substrate is single-account, so a permission here grants nothing;
	// it is recorded intent that a caller can read back, which is the observable half of
	// sharing.
	CreateVolumePermissions []EC2CreateVolumePermission `json:"create_volume_permissions,omitempty"`

	// AccountID is the AWS account that owns the snapshot.
	AccountID string `json:"account_id"`

	// Region is the AWS region in which the snapshot resides.
	Region string `json:"region"`
}

// EC2CreateVolumePermission is one entry in a snapshot's create-volume permission list.
//
// Exactly one member is set per entry, per AWS's CreateVolumePermission: a permission names
// either an account or the group "all". The type is comparable so that adding a permission
// already present, or removing one that is not, needs no key function — see
// [ec2ApplyVolumePermissions].
type EC2CreateVolumePermission struct {
	// UserID is the AWS account the permission is granted to.
	UserID string `json:"user_id,omitempty"`

	// Group is the group the permission is granted to. AWS's only valid value is "all",
	// which makes the snapshot public.
	Group string `json:"group,omitempty"`
}

// generateImageID generates a random AMI ID in the format "ami-" followed
// by 17 hex characters.
func generateImageID() string {
	return "ami-" + randomHex(8)
}

// generateEBSSnapshotID generates a random EBS snapshot ID in the format "snap-"
// followed by 17 hex characters.
func generateEBSSnapshotID() string {
	return "snap-" + randomHex(8)
}

// ec2SnapshotStateKey returns the state key for an EBS snapshot.
func ec2SnapshotStateKey(accountID, region, snapshotID string) string {
	return "snapshot:" + accountID + "/" + region + "/" + snapshotID
}

// ec2SnapshotStatePrefix is the key prefix every snapshot in one account and region
// shares, for a List that has no particular snapshot in mind.
//
// Declared beside the key for the reason [ec2VolumeStateKey]'s own doc comment gives:
// CreateSnapshot now writes records CreateImage did not, and a writer that spells the
// prefix differently from the readers would produce snapshots DescribeSnapshots cannot
// see.
func ec2SnapshotStatePrefix(accountID, region string) string {
	return "snapshot:" + accountID + "/" + region + "/"
}

// EC2PlacementGroup represents an Amazon EC2 placement group.
type EC2PlacementGroup struct {
	// GroupName is the user-supplied placement group name.
	GroupName string `json:"group_name"`

	// GroupID is the AWS-assigned identifier (e.g. "pg-0123456789abcdef0").
	GroupID string `json:"group_id"`

	// Strategy is the placement strategy: "cluster", "spread", or "partition".
	Strategy string `json:"strategy"`

	// State is the placement group state: always "available" in Substrate.
	State string `json:"state"`

	// Tags holds key-value metadata tags.
	Tags []EC2Tag `json:"tags,omitempty"`

	// AccountID is the AWS account that owns the placement group.
	AccountID string `json:"account_id"`

	// Region is the AWS region in which the placement group resides.
	Region string `json:"region"`
}

// generatePlacementGroupID generates a random placement group ID in the format
// "pg-" followed by 17 hex characters.
func generatePlacementGroupID() string {
	return "pg-" + randomHex(8)
}

// ec2PlacementGroupStateKey returns the state key for a placement group (keyed
// by name, which is unique per account/region).
//
// Keyed by name rather than by ID because that is what every placement-group operation
// takes: CreatePlacementGroup, DeletePlacementGroup and DescribePlacementGroups all name
// a GroupName and none accepts a group ID. AWS's ARN for the type is by name too. Only
// CreateTags names a pg- ID, and it resolves through [ec2NameKeyedResource] rather than
// changing the key — a rekey would orphan every record an earlier substrate wrote.
func ec2PlacementGroupStateKey(accountID, region, groupName string) string {
	return "placement_group:" + accountID + "/" + region + "/" + groupName
}

// ec2KeyPairStateKey returns the state key for a key pair (keyed by name, which is unique
// per account/region).
//
// The same shape a placement group's key has, for the same reason: KeyName is what
// CreateKeyPair, ImportKeyPair and DescribeKeyPairs take, and AWS's ARN for the type is
// arn:${Partition}:ec2:${Region}:${Account}:key-pair/${KeyPairName}. DeleteKeyPair accepts
// either, and scans for a KeyPairId.
func ec2KeyPairStateKey(accountID, region, keyName string) string {
	return "keypair:" + accountID + "/" + region + "/" + keyName
}

// ec2LaunchTemplateStateKey returns the state key for a launch template.
//
// Extracted from six inline copies by #708, which needed a seventh for CreateTags' lt-
// arm. The name index is a separate key ("lt_by_name:…") holding the ID, so a template is
// reachable by either and stored once.
func ec2LaunchTemplateStateKey(accountID, region, launchTemplateID string) string {
	return "lt:" + accountID + "/" + region + "/" + launchTemplateID
}

// EC2ElasticIP represents an Amazon EC2 Elastic IP address.
type EC2ElasticIP struct {
	// AllocationID is the unique identifier for the Elastic IP allocation.
	AllocationID string `json:"allocation_id"`

	// PublicIP is the public IPv4 address.
	PublicIP string `json:"public_ip"`

	// AssociationID is the identifier for the current association, if any.
	AssociationID string `json:"association_id,omitempty"`

	// InstanceID is the instance associated with this address, if any.
	InstanceID string `json:"instance_id,omitempty"`

	// NetworkInterfaceID is the network interface associated with this address, if any.
	NetworkInterfaceID string `json:"network_interface_id,omitempty"`

	// PrivateIPAddress is the private IP address associated with the Elastic IP.
	PrivateIPAddress string `json:"private_ip_address,omitempty"`

	// Domain is the domain of the allocation ("vpc" or "standard").
	Domain string `json:"domain"`

	// Tags holds key-value metadata tags.
	Tags []EC2Tag `json:"tags,omitempty"`

	// AccountID is the AWS account that owns the Elastic IP.
	AccountID string `json:"account_id"`

	// Region is the AWS region in which the Elastic IP is allocated.
	Region string `json:"region"`
}

// EC2NATGateway represents an Amazon VPC NAT gateway.
type EC2NATGateway struct {
	// NatGatewayID is the unique identifier for the NAT gateway.
	NatGatewayID string `json:"nat_gateway_id"`

	// SubnetID is the subnet in which the NAT gateway resides.
	SubnetID string `json:"subnet_id"`

	// VPCID is the VPC containing the NAT gateway.
	VPCID string `json:"vpc_id"`

	// AllocationID is the Elastic IP allocation ID for public NAT gateways.
	AllocationID string `json:"allocation_id,omitempty"`

	// PublicIP is the public IPv4 address for public NAT gateways.
	PublicIP string `json:"public_ip,omitempty"`

	// PrivateIP is the private IPv4 address of the NAT gateway.
	PrivateIP string `json:"private_ip"`

	// State is the NAT gateway state: "pending", "available", "deleting", "deleted".
	State string `json:"state"`

	// ConnectivityType is "public" or "private".
	ConnectivityType string `json:"connectivity_type"`

	// Tags holds key-value metadata tags.
	Tags []EC2Tag `json:"tags,omitempty"`

	// CreateTime is the RFC3339 time at which the NAT gateway was created.
	CreateTime string `json:"create_time"`

	// AccountID is the AWS account that owns the NAT gateway.
	AccountID string `json:"account_id"`

	// Region is the AWS region in which the NAT gateway resides.
	Region string `json:"region"`
}

// generateAllocationID generates a random Elastic IP allocation ID.
func generateAllocationID() string {
	return "eipalloc-" + randomHex(8)
}

// generateEIPAssociationID generates a random Elastic IP association ID.
func generateEIPAssociationID() string {
	return "eipassoc-" + randomHex(8)
}

// generateNATGatewayID generates a random NAT gateway ID.
func generateNATGatewayID() string {
	return "nat-" + randomHex(8)
}

// EC2LaunchTemplateData holds the launch parameters stored in an EC2 launch template.
type EC2LaunchTemplateData struct {
	// ImageID is the AMI ID to use when launching instances.
	ImageID string `json:"imageId,omitempty"`

	// InstanceType is the EC2 instance type (e.g. "t3.micro").
	InstanceType string `json:"instanceType,omitempty"`

	// KeyName is the name of the key pair to use.
	KeyName string `json:"keyName,omitempty"`

	// SecurityGroupIDs is the list of security group IDs, from the template's
	// top-level SecurityGroupId.N.
	SecurityGroupIDs []string `json:"securityGroupIds,omitempty"`

	// UserData is the base64-encoded user data script.
	UserData string `json:"userData,omitempty"`

	// SubnetID is the subnet named in the template's primary network interface —
	// the one whose LaunchTemplateData.NetworkInterface.N.DeviceIndex is lowest.
	//
	// This is not a mirror of RunInstances' top-level SubnetId: AWS's
	// RequestLaunchTemplateData has no top-level SubnetId member at all, so a
	// network interface is the only place a template can name a subnet — and the
	// only place AssociatePublicIpAddress exists (#444).
	SubnetID string `json:"subnetId,omitempty"`

	// AssociatePublicIPAddress is the primary network interface's public-IP
	// preference, stored verbatim as a string rather than a bool because three
	// states are observable: absent (use the subnet's own default), "true" (force
	// a public IP even on a non-default subnet), and "false" (suppress one). A
	// bool would collapse the first two.
	AssociatePublicIPAddress string `json:"associatePublicIpAddress,omitempty"`

	// NetworkInterfaceGroups is the security groups named in the template's primary
	// network interface, kept separate from SecurityGroupIDs so a template
	// carrying both is not silently merged. AWS rejects that combination; see
	// [EC2LaunchTemplateData.NetworkSecurityGroupIDs] for how substrate resolves
	// which list applies.
	NetworkInterfaceGroups []string `json:"networkInterfaceGroups,omitempty"`

	// NetworkInterfaces holds every interface the template declares, in DeviceIndex
	// order (#455).
	//
	// SubnetID, AssociatePublicIPAddress and NetworkInterfaceGroups above hold the
	// *primary* interface's values and are not superseded by this slice: they are
	// what a template stored before this field existed carries, so a replayed event
	// log still launches into the right subnet with the right groups. A launch reads
	// this slice when it has one and those fields otherwise.
	NetworkInterfaces []EC2NetworkInterface `json:"networkInterfaces,omitempty"`

	// TagSpecifications is the template's instance-scoped tag-on-create tags, from
	// LaunchTemplateData.TagSpecification.N with ResourceType=instance.
	//
	// Instance-scoped only, and deliberately still so. Widening this field to carry a
	// resource-type discriminator would unmarshal every template already in an event
	// log without error into an element with an empty ResourceType and no tags, so
	// every stored template would silently start launching untagged instances — the
	// replay guarantee broken by a change that compiles. Other scopes get their own
	// field instead; see VolumeTagSpecifications and the same split at
	// NetworkInterfaces above.
	//
	// Note that these tags land on the *instance*, not on the template itself — the
	// reference is explicit: "These tags are not applied to the launch template."
	TagSpecifications []EC2Tag `json:"tagSpecifications,omitempty"`

	// VolumeTagSpecifications is the template's volume-scoped tag-on-create tags, from
	// LaunchTemplateData.TagSpecification.N with ResourceType=volume (#670). They land
	// on every volume the launch materializes, including the root volume substrate
	// synthesizes when no mapping declares one.
	//
	// AWS's LaunchTemplateTagSpecificationRequest also allows network-interface and
	// spot-instances-request; those scopes are still recorded nowhere, because a
	// recorded tag that no read surfaces is indistinguishable from a discarded one.
	VolumeTagSpecifications []EC2Tag `json:"volumeTagSpecifications,omitempty"`

	// IamInstanceProfile is the instance profile the template names, stored as
	// whichever of Name or Arn the request supplied — the same single-string shape
	// [EC2Instance.IamInstanceProfile] holds, so the merge is a plain fallback.
	//
	// Name is preferred when both are given, mirroring RunInstances' own
	// precedence. AWS's LaunchTemplateIamInstanceProfileSpecificationRequest has
	// exactly those two members and documents neither as authoritative over the
	// other, so the choice is substrate's, made for consistency with the call-level
	// path rather than from the reference.
	IamInstanceProfile string `json:"iamInstanceProfile,omitempty"`

	// BlockDeviceMappings holds every BlockDeviceMapping.N the template declares.
	//
	// A template is the only way block device mappings reach a fleet launch:
	// CreateFleet and RequestSpotFleet build their RunInstances parameters from the
	// fleet request, forwarding the launch template reference rather than the
	// caller's own mappings.
	BlockDeviceMappings []EC2BlockDeviceMapping `json:"blockDeviceMappings,omitempty"`
}

// EC2BlockDeviceMapping is one BlockDeviceMapping.N entry of a launch request or a
// launch template, as AWS's BlockDeviceMapping and EbsBlockDevice shapes define it.
type EC2BlockDeviceMapping struct {
	// DeviceName is the device the volume is exposed as (e.g. "/dev/sdh").
	DeviceName string `json:"device_name,omitempty"`

	// VirtualName names an instance store volume ("ephemeral0"…"ephemeral23"). No
	// EBS volume is created for such an entry: an instance store device is not an
	// EBS volume, and manufacturing one would be an observation nothing backs.
	VirtualName string `json:"virtual_name,omitempty"`

	// NoDevice suppresses a device the AMI's own mapping would otherwise include.
	NoDevice bool `json:"no_device,omitempty"`

	// SnapshotID is the snapshot the volume is restored from, if any.
	SnapshotID string `json:"snapshot_id,omitempty"`

	// VolumeSize is the size in GiB. AWS documents it as required unless a snapshot
	// is named, in which case the snapshot's size is the default.
	VolumeSize int `json:"volume_size,omitempty"`

	// VolumeType is the EBS volume type. An absent value resolves to gp2 —
	// substrate's own choice, matching CreateVolume's documented default;
	// EbsBlockDevice documents none. See [ec2LaunchVolumesFor].
	VolumeType string `json:"volume_type,omitempty"`

	// IOPS is the provisioned IOPS, for io1/io2/gp3.
	IOPS int `json:"iops,omitempty"`

	// Throughput is the provisioned throughput in MiB/s, for gp3.
	Throughput int `json:"throughput,omitempty"`

	// Encrypted reports whether the volume is encrypted.
	Encrypted bool `json:"encrypted,omitempty"`

	// DeleteOnTermination is the raw parameter value rather than a bool, because
	// three states are observable and a bool collapses two of them: absent (take
	// the launch default, which splits by device role — see [ec2LaunchVolumesFor]),
	// "true", and "false". Storing a bool here would make a mapping that explicitly
	// preserves its volume indistinguishable from one that said nothing — the same
	// reason [EC2LaunchTemplateData.AssociatePublicIPAddress] keeps its string.
	DeleteOnTermination string `json:"delete_on_termination,omitempty"`

	// VolumeSizeRaw, IOPSRaw and ThroughputRaw are the numeric members' parameter
	// values exactly as they arrived, kept beside the parsed ints for two reasons
	// that the ints alone cannot serve (#671).
	//
	// First, a value that does not parse is otherwise indistinguishable from an
	// absent one: Ebs.VolumeSize=60GB left VolumeSize at 0, so the mapping silently
	// produced substrate's 8 GiB default and a consumer asserted against a value the
	// request never asked for. Second, an *absent* size is what
	// EbsBlockDevice.VolumeSize's "You must specify either a snapshot ID or a volume
	// size" is about, and a literal 0 is not the same thing.
	//
	// They are persisted rather than derived because a launch template carries its
	// mappings into state, and by the time a RunInstances names that template the
	// original parameters are long gone — so the refusal has to be reconstructable
	// from the record. Adding a field to a persisted struct is replay-safe; an older
	// event log simply unmarshals them empty, which reads as "nothing unparseable",
	// and that is the behavior those logs already had.
	VolumeSizeRaw string `json:"volume_size_raw,omitempty"`
	IOPSRaw       string `json:"iops_raw,omitempty"`
	ThroughputRaw string `json:"throughput_raw,omitempty"`
}

// NetworkSecurityGroupIDs returns the security groups a launch from this template
// should use: the top-level list when present, otherwise the first network
// interface's.
//
// The precedence only matters for a template AWS would have rejected — it refuses
// a template that sets both — so substrate prefers the top-level list rather than
// erroring, which keeps a hand-built template working while still honoring the
// interface-scoped form the SDK emits.
func (d EC2LaunchTemplateData) NetworkSecurityGroupIDs() []string {
	if len(d.SecurityGroupIDs) > 0 {
		return d.SecurityGroupIDs
	}
	return d.NetworkInterfaceGroups
}

// EC2LaunchTemplate represents an Amazon EC2 launch template.
type EC2LaunchTemplate struct {
	// LaunchTemplateID is the unique identifier (e.g. "lt-0abc1234def56789a").
	LaunchTemplateID string `json:"launchTemplateId"`

	// LaunchTemplateName is the user-supplied name.
	LaunchTemplateName string `json:"launchTemplateName"`

	// DefaultVersionNum is the default version number.
	DefaultVersionNum int64 `json:"defaultVersionNumber"`

	// LatestVersionNum is the latest version number.
	LatestVersionNum int64 `json:"latestVersionNumber"`

	// CreatedBy is the principal that created the template.
	CreatedBy string `json:"createdBy"`

	// CreateTime is the RFC3339 timestamp when the template was created.
	CreateTime string `json:"createTime"`

	// Tags holds key-value metadata tags.
	Tags []EC2Tag `json:"tags,omitempty"`

	// LatestData holds the launch template parameters for the latest version.
	//
	// Retained alongside Versions rather than derived from it, for two reasons. It
	// is the field a template stored before versioning existed (#456) carries, so
	// [EC2LaunchTemplate.TemplateVersions] can synthesize version 1 from it — that
	// synthesis is the whole migration, with no event rewriting. And several call
	// sites read it directly, so removing it would turn a storage change into a
	// rewrite of the RunInstances merge block. It is kept in sync on every version
	// append; do not delete it as redundant.
	LatestData EC2LaunchTemplateData `json:"latestData"`

	// Versions holds every version of the template, ordered by version number.
	//
	// Nil for a template stored before #456; see
	// [EC2LaunchTemplate.TemplateVersions], which is how every reader must access
	// this field.
	Versions []EC2LaunchTemplateVersion `json:"versions,omitempty"`

	// AccountID is the AWS account that owns the launch template.
	AccountID string `json:"accountID"`

	// Region is the AWS region in which the launch template resides.
	Region string `json:"region"`
}

// EC2LaunchTemplateVersion represents one version of an EC2 launch template.
type EC2LaunchTemplateVersion struct {
	// VersionNumber is the version's number, starting at 1 for the version created
	// alongside the template itself.
	VersionNumber int64 `json:"versionNumber"`

	// VersionDescription is the caller-supplied description, if any.
	VersionDescription string `json:"versionDescription,omitempty"`

	// CreateTime is the RFC3339 timestamp when the version was created.
	CreateTime string `json:"createTime"`

	// CreatedBy is the principal that created the version.
	CreatedBy string `json:"createdBy"`

	// Data holds the launch parameters this version carries.
	Data EC2LaunchTemplateData `json:"data"`
}

// TemplateVersions returns the template's versions, synthesizing version 1 from
// LatestData when the stored template predates version tracking.
//
// Every reader must go through this rather than reading Versions directly. A
// template written to the event log before #456 has no versions array at all, and
// replaying such a log must still launch instances and read back as a
// single-version template — which it does, because that template's LatestData *is*
// its version 1.
func (t EC2LaunchTemplate) TemplateVersions() []EC2LaunchTemplateVersion {
	if len(t.Versions) > 0 {
		return t.Versions
	}
	return []EC2LaunchTemplateVersion{{
		VersionNumber: 1,
		CreateTime:    t.CreateTime,
		CreatedBy:     t.CreatedBy,
		Data:          t.LatestData,
	}}
}

// generateLaunchTemplateID generates a random launch template ID.
func generateLaunchTemplateID() string {
	return "lt-" + randomHex(8)
}

// EC2VolumeAttachment represents the attachment of an EBS volume to an instance.
type EC2VolumeAttachment struct {
	// InstanceID is the ID of the attached instance.
	InstanceID string `json:"instance_id"`

	// Device is the device name on the instance (e.g. "/dev/xvdf").
	Device string `json:"device"`

	// State is the attachment state: "attaching", "attached", "detaching", "detached".
	State string `json:"state"`

	// AttachTime is the RFC3339 time the volume was attached.
	AttachTime string `json:"attach_time"`

	// DeleteOnTermination reports whether terminating the instance deletes the
	// volume. It lives on the attachment rather than on the volume because that is
	// where AWS renders it — DescribeVolumes' attachmentSet>item carries a
	// deleteOnTermination member, and the volume item itself does not — and because
	// the value describes this attachment: detaching and reattaching a volume
	// resets it to the post-launch default.
	//
	// Anything attached after launch preserves on termination, which both AWS pages
	// that document a default agree on, so AttachVolume defaults this to false. For
	// a volume a launch creates the two pages disagree, and substrate resolves the
	// conflict by device role: true for the root volume, false for a data volume
	// (#675). See [ec2LaunchVolumesFor] for which pages say what and why the split
	// wins.
	DeleteOnTermination bool `json:"delete_on_termination"`
}

// EC2Volume represents an Amazon EBS volume.
type EC2Volume struct {
	// VolumeID is the unique identifier (e.g. "vol-0123456789abcdef0").
	VolumeID string `json:"volume_id"`

	// Size is the volume size in GiB.
	Size int `json:"size"`

	// VolumeType is the EBS volume type (e.g. "gp2", "gp3", "io1").
	VolumeType string `json:"volume_type"`

	// AvailabilityZone is the AZ in which the volume resides.
	AvailabilityZone string `json:"availability_zone"`

	// State is the volume state: "creating", "available", "in-use", "deleting", "deleted".
	State string `json:"state"`

	// SnapshotID is the ID of the snapshot from which the volume was created, if any.
	SnapshotID string `json:"snapshot_id,omitempty"`

	// Encrypted indicates whether the volume is encrypted.
	Encrypted bool `json:"encrypted"`

	// IOPS is the provisioned IOPS (for io1/io2/gp3 volumes).
	IOPS int `json:"iops,omitempty"`

	// Throughput is the provisioned throughput in MiB/s. AWS supports it on gp3
	// alone; substrate records whatever a request supplies rather than refusing it
	// elsewhere, since no throughput validation is modeled.
	Throughput int `json:"throughput,omitempty"`

	// Attachments holds the current instance attachments.
	Attachments []EC2VolumeAttachment `json:"attachments,omitempty"`

	// Tags holds key-value metadata tags.
	Tags []EC2Tag `json:"tags,omitempty"`

	// CreateTime is the RFC3339 timestamp when the volume was created.
	CreateTime string `json:"create_time"`

	// AccountID is the AWS account that owns the volume.
	AccountID string `json:"account_id"`

	// Region is the AWS region in which the volume resides.
	Region string `json:"region"`
}

// generateVolumeID generates a random EBS volume ID.
func generateVolumeID() string {
	return "vol-" + randomHex(8)
}

// SecurityGroupAllowed checks if (protocol, port, sourceCIDR) is permitted by
// any rule in the given set. Protocol "-1" matches all traffic. CIDR
// "0.0.0.0/0" matches all sources.
func SecurityGroupAllowed(rules []EC2IPPermission, protocol string, port int, sourceCIDR string) bool {
	for _, rule := range rules {
		if !sgProtocolMatches(rule.IPProtocol, protocol) {
			continue
		}
		if rule.IPProtocol != "-1" && (port < rule.FromPort || port > rule.ToPort) {
			continue
		}
		if sgCIDRMatches(rule.IPRanges, sourceCIDR) {
			return true
		}
	}
	return false
}

func sgProtocolMatches(ruleProto, queryProto string) bool {
	if ruleProto == "-1" {
		return true
	}
	return strings.EqualFold(ruleProto, queryProto)
}

func sgCIDRMatches(ruleCIDRs []string, source string) bool {
	if len(ruleCIDRs) == 0 {
		return true // no CIDR restriction
	}
	sourceIP := net.ParseIP(source)
	for _, cidr := range ruleCIDRs {
		if cidr == "0.0.0.0/0" || cidr == "::/0" {
			return true
		}
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			if cidr == source {
				return true // exact match fallback
			}
			continue
		}
		if sourceIP != nil && network.Contains(sourceIP) {
			return true
		}
	}
	return false
}
