package emulator

import (
	"time"
)

// ssoNamespace is the state namespace for AWS IAM Identity Center (SSO) resources.
const ssoNamespace = "sso"

// SSOInstance represents an AWS IAM Identity Center instance (singleton per account).
type SSOInstance struct {
	// InstanceArn is the ARN of the IAM Identity Center instance.
	InstanceArn string `json:"InstanceArn"`
	// IdentityStoreID is the ID of the Identity Store associated with this instance.
	IdentityStoreID string `json:"IdentityStoreId"`
	// Status is the status of the instance.
	Status string `json:"Status"` // ACTIVE
	// CreatedDate is when the instance was created.
	CreatedDate time.Time `json:"CreatedDate"`
	// AccountID is the AWS account that owns this instance.
	AccountID string `json:"AccountID"`
}

// SSOPermissionSet represents a permission set in AWS IAM Identity Center.
type SSOPermissionSet struct {
	// PermissionSetArn is the ARN of the permission set.
	PermissionSetArn string `json:"PermissionSetArn"`
	// Name is the name of the permission set.
	Name string `json:"Name"`
	// Description is the description of the permission set.
	Description string `json:"Description,omitempty"`
	// SessionDuration specifies how long the session lasts (ISO 8601 duration).
	SessionDuration string `json:"SessionDuration,omitempty"`
	// RelayState is the relay state URL used for deep links in the SSO portal.
	RelayState string `json:"RelayState,omitempty"`
	// CreatedDate is when the permission set was created.
	CreatedDate time.Time `json:"CreatedDate"`
	// AccountID is the AWS account that owns this permission set.
	AccountID string `json:"AccountID"`
	// InstanceArn is the ARN of the instance this permission set belongs to.
	InstanceArn string `json:"InstanceArn"`
}

// SSOAccountAssignment represents an account assignment in AWS IAM Identity Center.
type SSOAccountAssignment struct {
	// PermissionSetArn is the ARN of the permission set.
	PermissionSetArn string `json:"PermissionSetArn"`
	// TargetID is the account ID being assigned to.
	TargetID string `json:"TargetId"`
	// TargetType specifies the type of target (AWS_ACCOUNT).
	TargetType string `json:"TargetType"`
	// PrincipalType specifies whether the principal is a USER or GROUP.
	PrincipalType string `json:"PrincipalType"`
	// PrincipalID is the ID of the user or group.
	PrincipalID string `json:"PrincipalId"`
	// AccountID is the AWS account that owns this assignment.
	AccountID string `json:"AccountID"`
	// InstanceArn is the ARN of the instance this assignment belongs to.
	InstanceArn string `json:"InstanceArn"`
}

// The four SSO minters take the request's [IDMint] as a parameter rather than reading it off a
// [RequestContext], because two of them are reached from [SSOPlugin.ensureInstance], which takes an
// account ID and a Go context and not a request context (#856's established shape for a helper that
// mints without a request in scope — see buildSNSEnvelope).
//
// The instance is minted **lazily, by whichever request touches SSO first**, including a read:
// `ListInstances` creates it as readily as `CreatePermissionSet` does. That is replay-stable rather
// than in spite of the laziness — a replay re-issues the recorded requests in their recorded order,
// so the same request creates the instance and mints the same ARN — but it does mean the instance
// ARN belongs to the ordinal stream of a request that did not ask for one.

// generateSSOInstanceArn mints an IAM Identity Center instance ARN from m.
//
// Thirteen bytes is 26 hex characters, so the `[:26]` the crypto/rand form applied truncated
// nothing; the width is unchanged.
func generateSSOInstanceArn(m *IDMint) string {
	return "arn:aws:sso:::instance/" + m.Hex(13)
}

// generateSSOIdentityStoreID mints an identity store ID from m, in the `d-`-prefixed form.
func generateSSOIdentityStoreID(m *IDMint) string {
	return "d-" + m.Hex(5)
}

// generateSSOPermissionSetArn mints a permission set ARN from m, as a child of instanceArn.
func generateSSOPermissionSetArn(m *IDMint, instanceArn string) string {
	return instanceArn + "/ps-" + m.Hex(8)
}

// generateSSORequestID mints a UUID-shaped request ID from m, for the async SSO operations that
// report one.
func generateSSORequestID(m *IDMint) string {
	return m.HexUUID()
}

// State key helpers.

func ssoInstanceKey(acct string) string {
	return "instance:" + acct
}

func ssoPermSetKey(acct, permSetArn string) string {
	return "permset:" + acct + "/" + permSetArn
}

func ssoPermSetArnsKey(acct string) string {
	return "permset_arns:" + acct
}

func ssoManagedPoliciesKey(acct, permSetArn string) string {
	return "managed_policies:" + acct + "/" + permSetArn
}

func ssoAssignmentKey(acct, permSetArn, targetID, principalType, principalID string) string {
	return "assignment:" + acct + "/" + permSetArn + "/" + targetID + "/" + principalType + "/" + principalID
}

func ssoAssignmentKeysKey(acct, permSetArn string) string {
	return "assignment_keys:" + acct + "/" + permSetArn
}
