package emulator

import (
	"encoding/json"
	"time"
)

// transferNamespace is the state namespace for AWS Transfer Family resources.
const transferNamespace = "transfer"

// TransferServer represents an AWS Transfer Family server.
type TransferServer struct {
	// ServerID is the unique identifier for the server (e.g. s-01234567890abcdef).
	ServerID string `json:"ServerId"`
	// Arn is the ARN of the server.
	Arn string `json:"Arn"`
	// Domain is the type of file-transfer protocol (SFTP, FTP, FTPS).
	Domain string `json:"Domain,omitempty"`
	// EndpointType is the type of endpoint (PUBLIC, VPC, VPC_ENDPOINT).
	EndpointType string `json:"EndpointType,omitempty"`
	// IdentityProviderType specifies the mode of authentication for users.
	IdentityProviderType string `json:"IdentityProviderType,omitempty"`
	// State is the condition of the server (ONLINE, OFFLINE, STARTING, STOPPING, START_FAILED, STOP_FAILED).
	State string `json:"State"`
	// Tags is the list of key-value tags for the server.
	Tags []TransferTag `json:"Tags,omitempty"`
	// UserCount is retained so records written before #1199 still decode, and is never read:
	// it was declared and never assigned, so it always held 0. DescribeServer and ListServers
	// count the server's users from its user index at read time instead.
	UserCount int `json:"UserCount"`
	// Protocols is the file transfer protocols the server accepts (SFTP, FTP, FTPS, AS2).
	Protocols []string `json:"Protocols,omitempty"`
	// Certificate is the ACM certificate ARN, required by AWS when Protocols includes FTPS.
	Certificate string `json:"Certificate,omitempty"`
	// LoggingRole is the IAM role ARN the server logs to CloudWatch with.
	LoggingRole string `json:"LoggingRole,omitempty"`
	// SecurityPolicyName is the server's security policy.
	SecurityPolicyName string `json:"SecurityPolicyName,omitempty"`
	// IPAddressType is IPV4 or DUALSTACK.
	IPAddressType string `json:"IpAddressType,omitempty"`
	// PreAuthenticationLoginBanner is shown before a user authenticates.
	PreAuthenticationLoginBanner string `json:"PreAuthenticationLoginBanner,omitempty"`
	// PostAuthenticationLoginBanner is shown after a user authenticates.
	PostAuthenticationLoginBanner string `json:"PostAuthenticationLoginBanner,omitempty"`
	// StructuredLogDestinations is the log-group ARNs the server's logs are sent to.
	StructuredLogDestinations []string `json:"StructuredLogDestinations,omitempty"`
	// EndpointDetails is the request's EndpointDetails object, recorded as sent.
	EndpointDetails json.RawMessage `json:"EndpointDetails,omitempty"`
	// IdentityProviderDetails is the request's IdentityProviderDetails object, recorded as sent.
	IdentityProviderDetails json.RawMessage `json:"IdentityProviderDetails,omitempty"`
	// ProtocolDetails is the request's ProtocolDetails object, recorded as sent.
	ProtocolDetails json.RawMessage `json:"ProtocolDetails,omitempty"`
	// S3StorageOptions is the request's S3StorageOptions object, recorded as sent.
	S3StorageOptions json.RawMessage `json:"S3StorageOptions,omitempty"`
	// WorkflowDetails is the request's WorkflowDetails object, recorded as sent.
	WorkflowDetails json.RawMessage `json:"WorkflowDetails,omitempty"`
	// CreatedAt is when the server was created.
	CreatedAt time.Time `json:"CreatedAt"`
	// AccountID is the AWS account that owns this server.
	AccountID string `json:"AccountID"`
	// Region is the AWS region where the server exists.
	Region string `json:"Region"`
}

// TransferUser represents a user on an AWS Transfer Family server.
type TransferUser struct {
	// UserName is the name of the user.
	UserName string `json:"UserName"`
	// Arn is the ARN of the user.
	Arn string `json:"Arn"`
	// ServerID is the ID of the server this user belongs to.
	ServerID string `json:"ServerId"`
	// HomeDirectory is the landing directory for the user.
	HomeDirectory string `json:"HomeDirectory,omitempty"`
	// Role is the IAM role ARN that controls user access.
	Role string `json:"Role,omitempty"`
	// Tags is the list of key-value tags for the user.
	Tags []TransferTag `json:"Tags,omitempty"`
	// HomeDirectoryType is PATH or LOGICAL, recorded only when the request sent it.
	HomeDirectoryType string `json:"HomeDirectoryType,omitempty"`
	// HomeDirectoryMappings is the request's HomeDirectoryMappings array, recorded as sent.
	HomeDirectoryMappings json.RawMessage `json:"HomeDirectoryMappings,omitempty"`
	// Policy is the user's session policy, a JSON document held as a string.
	Policy string `json:"Policy,omitempty"`
	// PosixProfile is the request's PosixProfile object, recorded as sent.
	PosixProfile json.RawMessage `json:"PosixProfile,omitempty"`
	// SSHPublicKeys is the user's SSH public keys; CreateUser stores at most the one it is sent.
	SSHPublicKeys []TransferSSHPublicKey `json:"SshPublicKeys,omitempty"`
	// AccountID is the AWS account that owns this user.
	AccountID string `json:"AccountID"`
	// Region is the AWS region where the user exists.
	Region string `json:"Region"`
}

// TransferTag is a key-value tag for an AWS Transfer Family resource.
type TransferTag struct {
	// Key is the tag key.
	Key string `json:"Key"`
	// Value is the tag value.
	Value string `json:"Value"`
}

// TransferSSHPublicKey is one SSH public key stored for a Transfer Family user.
type TransferSSHPublicKey struct {
	// SSHPublicKeyBody is the public key as the caller sent it.
	SSHPublicKeyBody string `json:"SshPublicKeyBody"`
	// SSHPublicKeyID is the key's identifier, key- followed by 17 hex characters.
	SSHPublicKeyID string `json:"SshPublicKeyId"`
	// DateImported is when the key was stored.
	DateImported time.Time `json:"DateImported"`
}

// generateTransferSSHKeyID mints an SSH public key ID from m in the form key-{17 hex chars},
// the published SshPublicKeyId pattern key-[0-9a-f]{17}.
func generateTransferSSHKeyID(m *IDMint) string {
	return "key-" + m.Hex(9)[:17]
}

// generateTransferServerID mints a server ID from m in the form s-{17 hex chars},
// matching the real AWS Transfer Family server ID format.
func generateTransferServerID(m *IDMint) string {
	return "s-" + m.Hex(9)[:17] // 9 bytes → 18 hex chars; we use 17.
}

// State key helpers.

func transferServerKey(acct, region, serverID string) string {
	return "server:" + acct + "/" + region + "/" + serverID
}

func transferServerIDsKey(acct, region string) string {
	return "server_ids:" + acct + "/" + region
}

func transferUserKey(acct, region, serverID, userName string) string {
	return "user:" + acct + "/" + region + "/" + serverID + "/" + userName
}

func transferUserNamesKey(acct, region, serverID string) string {
	return "user_names:" + acct + "/" + region + "/" + serverID
}
