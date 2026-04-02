package substrate

import (
	"crypto/rand"
	"encoding/hex"
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

// generateSSOInstanceArn generates a random IAM Identity Center instance ARN.
func generateSSOInstanceArn() string {
	b := make([]byte, 13)
	_, _ = rand.Read(b)
	return "arn:aws:sso:::instance/" + hex.EncodeToString(b)[:26]
}

// generateSSOIdentityStoreID generates a random identity store ID.
func generateSSOIdentityStoreID() string {
	b := make([]byte, 5)
	_, _ = rand.Read(b)
	return "d-" + hex.EncodeToString(b)[:10]
}

// generateSSOPermissionSetArn generates an ARN for a permission set.
func generateSSOPermissionSetArn(instanceArn string) string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return instanceArn + "/ps-" + hex.EncodeToString(b)
}

// generateSSORequestID generates a UUID-style request ID for async SSO operations.
func generateSSORequestID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b[0:4]) + "-" +
		hex.EncodeToString(b[4:6]) + "-" +
		hex.EncodeToString(b[6:8]) + "-" +
		hex.EncodeToString(b[8:10]) + "-" +
		hex.EncodeToString(b[10:16])
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
