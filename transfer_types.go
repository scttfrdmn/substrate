package substrate

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

// transferNamespace is the state namespace for AWS Transfer Family resources.
const transferNamespace = "transfer"

// TransferServer represents an AWS Transfer Family server.
type TransferServer struct {
	// ServerId is the unique identifier for the server (e.g. s-01234567890abcdef).
	ServerId string `json:"ServerId"`
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
	// UserCount is the number of users configured on the server.
	UserCount int `json:"UserCount"`
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
	// ServerId is the ID of the server this user belongs to.
	ServerId string `json:"ServerId"`
	// HomeDirectory is the landing directory for the user.
	HomeDirectory string `json:"HomeDirectory,omitempty"`
	// Role is the IAM role ARN that controls user access.
	Role string `json:"Role,omitempty"`
	// Tags is the list of key-value tags for the user.
	Tags []TransferTag `json:"Tags,omitempty"`
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

// generateTransferServerID generates a server ID in the form s-{17 hex chars},
// matching the real AWS Transfer Family server ID format.
func generateTransferServerID() string {
	b := make([]byte, 9) // 9 bytes → 18 hex chars; we use 17
	_, _ = rand.Read(b)
	return "s-" + hex.EncodeToString(b)[:17]
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
