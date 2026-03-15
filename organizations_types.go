package substrate

import "time"

// organizationsNamespace is the service name used by OrganizationsPlugin.
const organizationsNamespace = "organizations"

// Organization represents an AWS Organization.
type Organization struct {
	// ID is the organization identifier (e.g. "o-" + 10 lowercase alphanum).
	ID string `json:"Id"`

	// Arn is the ARN of the organization.
	Arn string `json:"Arn"`

	// FeatureSet is the set of features enabled for the organization (e.g. "ALL").
	FeatureSet string `json:"FeatureSet"`

	// MasterAccountArn is the ARN of the master (management) account.
	MasterAccountArn string `json:"MasterAccountArn"`

	// MasterAccountID is the AWS account ID of the master account.
	MasterAccountID string `json:"MasterAccountId"`

	// MasterAccountEmail is the email address of the master account.
	MasterAccountEmail string `json:"MasterAccountEmail"`
}

// OrgAccount represents an AWS account that is a member of an organization.
type OrgAccount struct {
	// ID is the AWS account ID.
	ID string `json:"Id"`

	// Arn is the ARN of the account within the organization.
	Arn string `json:"Arn"`

	// Name is the display name of the account.
	Name string `json:"Name"`

	// Email is the email address of the account root user.
	Email string `json:"Email"`

	// Status is the account status (e.g. "ACTIVE").
	Status string `json:"Status"`

	// JoinedAt is the time the account joined the organization.
	JoinedAt time.Time `json:"JoinedTimestamp"`
}

// OrgRoot is the root container of the organization hierarchy.
type OrgRoot struct {
	// ID is the root identifier (e.g. "r-" + 4 lowercase hex chars).
	ID string `json:"Id"`

	// Arn is the ARN of the root.
	Arn string `json:"Arn"`

	// Name is the display name of the root (always "Root").
	Name string `json:"Name"`

	// PolicyTypes lists policy types attached to this root.
	PolicyTypes []OrgPolicyTypeSummary `json:"PolicyTypes"`
}

// OrgPolicyTypeSummary describes a policy type attached to an organization root.
type OrgPolicyTypeSummary struct {
	// Type is the policy type name (e.g. "SERVICE_CONTROL_POLICY").
	Type string `json:"Type"`

	// Status is the enablement status (e.g. "ENABLED").
	Status string `json:"Status"`
}
