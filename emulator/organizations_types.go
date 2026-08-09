package emulator

import "time"

// organizationsNamespace is the service name used by OrganizationsPlugin.
const organizationsNamespace = "organizations"

// Organization represents an AWS Organization.
type Organization struct {
	// ID is the organization identifier (e.g. "o-" + 10 lowercase alphanum).
	ID string `json:"Id"`

	// Arn is the ARN of the organization.
	Arn string `json:"Arn"`

	// FeatureSet is the set of features enabled for the organization,
	// either "ALL" or "CONSOLIDATED_BILLING". Service control policies exist
	// only under "ALL"; see OrganizationsPlugin.effectiveFeatureSet.
	FeatureSet string `json:"FeatureSet"`

	// MasterAccountArn is the ARN of the master (management) account.
	MasterAccountArn string `json:"MasterAccountArn"`

	// MasterAccountID is the AWS account ID of the master account.
	MasterAccountID string `json:"MasterAccountId"`

	// MasterAccountEmail is the email address of the master account.
	MasterAccountEmail string `json:"MasterAccountEmail"`

	// AvailablePolicyTypes lists the policy types available in the organization.
	// AWS marks this member deprecated in favor of the root's PolicyTypes and
	// documents that it reports only SERVICE_CONTROL_POLICY, but still returns it;
	// it is omitted under CONSOLIDATED_BILLING, where no policy type is available.
	AvailablePolicyTypes []OrgPolicyTypeSummary `json:"AvailablePolicyTypes,omitempty"`
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

	// JoinedMethod is how the account became a member, "CREATED" for an account
	// CreateAccount made and "INVITED" otherwise. The enum admits only those two
	// values, and the management account is not created by Organizations.
	JoinedMethod string `json:"JoinedMethod"`

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

// OrgOrganizationalUnit is a container for accounts within an organization root.
type OrgOrganizationalUnit struct {
	// ID is the OU identifier, "ou-{root suffix}-{8 lowercase alphanum}".
	ID string `json:"Id"`

	// Arn is the ARN of the OU.
	Arn string `json:"Arn"`

	// Name is the friendly name of the OU, unique among its siblings.
	Name string `json:"Name"`
}

// OrgPolicySummary describes a policy without its content.
type OrgPolicySummary struct {
	// ID is the policy identifier, "p-" followed by 8-128 alphanumerics or "_".
	ID string `json:"Id"`

	// Arn is the ARN of the policy. AWS-managed policies are owned by the "aws"
	// account rather than the organization's management account.
	Arn string `json:"Arn"`

	// Name is the friendly name of the policy.
	Name string `json:"Name"`

	// Description is the policy description.
	Description string `json:"Description"`

	// Type is the policy type (e.g. "SERVICE_CONTROL_POLICY").
	Type string `json:"Type"`

	// AwsManaged reports whether AWS owns the policy. An AWS-managed policy
	// cannot be updated, deleted, or detached below the minimum attachment count.
	AwsManaged bool `json:"AwsManaged"`
}

// OrgPolicy is a policy and its content, as DescribePolicy returns it.
type OrgPolicy struct {
	// PolicySummary describes the policy's identity and type.
	PolicySummary OrgPolicySummary `json:"PolicySummary"`

	// Content is the text of the policy document.
	Content string `json:"Content"`
}

// OrgParent identifies the root or OU that directly contains an entity.
type OrgParent struct {
	// ID is the parent identifier (a root or OU ID).
	ID string `json:"Id"`

	// Type is "ROOT" or "ORGANIZATIONAL_UNIT".
	Type string `json:"Type"`
}

// OrgChild identifies an account or OU directly contained by a parent.
type OrgChild struct {
	// ID is the child identifier (an account ID or OU ID).
	ID string `json:"Id"`

	// Type is "ACCOUNT" or "ORGANIZATIONAL_UNIT".
	Type string `json:"Type"`
}

// OrgPolicyTargetSummary identifies an entity a policy is attached to.
type OrgPolicyTargetSummary struct {
	// TargetID is the root, OU, or account identifier.
	TargetID string `json:"TargetId"`

	// Arn is the ARN of the target.
	Arn string `json:"Arn"`

	// Name is the friendly name of the target.
	Name string `json:"Name"`

	// Type is "ROOT", "ORGANIZATIONAL_UNIT", or "ACCOUNT".
	Type string `json:"Type"`
}

// OrgCreateAccountStatus is the observable state of an asynchronous CreateAccount
// request. CreateAccount returns it as IN_PROGRESS; DescribeCreateAccountStatus
// reports the resolved outcome.
type OrgCreateAccountStatus struct {
	// ID is the request identifier, "car-" followed by 8-32 lowercase alphanum.
	ID string `json:"Id"`

	// AccountName is the name given in the CreateAccount request.
	AccountName string `json:"AccountName"`

	// State is "IN_PROGRESS", "SUCCEEDED", or "FAILED".
	State string `json:"State"`

	// RequestedTimestamp is when CreateAccount was called, on the simulated clock.
	RequestedTimestamp time.Time `json:"RequestedTimestamp"`

	// CompletedTimestamp is when the request reached a terminal state; the zero
	// value while IN_PROGRESS.
	CompletedTimestamp *time.Time `json:"CompletedTimestamp,omitempty"`

	// AccountID is the created account, set only when State is SUCCEEDED.
	AccountID string `json:"AccountId,omitempty"`

	// FailureReason is why the request failed, set only when State is FAILED.
	FailureReason string `json:"FailureReason,omitempty"`
}

// OrgTag is a key-value pair attached to an account, OU, root, or policy.
type OrgTag struct {
	// Key is the tag name.
	Key string `json:"Key"`

	// Value is the tag value; the empty string is permitted, null is not.
	Value string `json:"Value"`
}
