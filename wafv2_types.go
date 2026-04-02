package substrate

import "time"

// wafv2Namespace is the state namespace for AWS WAFv2 resources.
const wafv2Namespace = "wafv2"

// WAFv2WebACL represents an AWS WAFv2 Web Access Control List.
type WAFv2WebACL struct {
	// ID is the unique identifier of the Web ACL.
	ID string `json:"Id"`
	// Name is the name of the Web ACL.
	Name string `json:"Name"`
	// ARN is the Amazon Resource Name of the Web ACL.
	ARN string `json:"ARN"`
	// Description is an optional human-readable description.
	Description string `json:"Description,omitempty"`
	// Scope is the deployment scope: REGIONAL or CLOUDFRONT.
	Scope string `json:"Scope"`
	// LockToken is a CAS token required for update and delete operations.
	LockToken string `json:"LockToken"`
	// Rules is the list of rules contained in the Web ACL.
	Rules []WAFv2Rule `json:"Rules,omitempty"`
	// DefaultAction is the action to perform on requests that do not match any rule.
	DefaultAction map[string]interface{} `json:"DefaultAction,omitempty"`
	// VisibilityConfig holds CloudWatch metrics settings for the Web ACL.
	VisibilityConfig map[string]interface{} `json:"VisibilityConfig,omitempty"`
	// CreatedAt is the time the Web ACL was created.
	CreatedAt time.Time `json:"CreatedAt"`
	// AccountID is the AWS account that owns this Web ACL.
	AccountID string `json:"AccountID"`
	// Region is the AWS region where the Web ACL exists.
	Region string `json:"Region"`
}

// WAFv2IPSet represents an AWS WAFv2 IP set.
type WAFv2IPSet struct {
	// ID is the unique identifier of the IP set.
	ID string `json:"Id"`
	// Name is the name of the IP set.
	Name string `json:"Name"`
	// ARN is the Amazon Resource Name of the IP set.
	ARN string `json:"ARN"`
	// Description is an optional human-readable description.
	Description string `json:"Description,omitempty"`
	// Scope is the deployment scope: REGIONAL or CLOUDFRONT.
	Scope string `json:"Scope"`
	// LockToken is a CAS token required for update and delete operations.
	LockToken string `json:"LockToken"`
	// IPVersion is the IP address version: IPV4 or IPV6.
	IPVersion string `json:"IPVersion"`
	// Addresses is the list of IP addresses or CIDR ranges in the set.
	Addresses []string `json:"Addresses"`
	// AccountID is the AWS account that owns this IP set.
	AccountID string `json:"AccountID"`
	// Region is the AWS region where the IP set exists.
	Region string `json:"Region"`
}

// WAFv2Rule represents a single rule within a WAFv2 Web ACL.
type WAFv2Rule struct {
	// Name is the name of the rule.
	Name string `json:"Name"`
	// Priority is the processing order of the rule within the Web ACL.
	Priority int `json:"Priority"`
	// Action is the action to take when the rule matches (Allow, Block, Count).
	Action map[string]interface{} `json:"Action,omitempty"`
	// Statement describes the conditions under which the rule applies.
	Statement map[string]interface{} `json:"Statement,omitempty"`
	// VisibilityConfig holds CloudWatch metrics settings for the rule.
	VisibilityConfig map[string]interface{} `json:"VisibilityConfig,omitempty"`
}
