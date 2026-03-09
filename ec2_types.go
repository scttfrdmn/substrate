package substrate

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
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

	// SecurityGroupIDs holds the security groups attached to the instance.
	SecurityGroupIDs []string `json:"security_group_ids"`

	// Tags holds key-value metadata tags.
	Tags []EC2Tag `json:"tags,omitempty"`

	// LaunchTime is the UTC time at which the instance was launched.
	LaunchTime string `json:"launch_time"`

	// AccountID is the AWS account that owns the instance.
	AccountID string `json:"account_id"`

	// Region is the AWS region in which the instance runs.
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
