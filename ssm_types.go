package substrate

import (
	"fmt"
	"time"
)

const ssmNamespace = "ssm"

// SSMParameter represents an SSM Parameter Store parameter.
type SSMParameter struct {
	// Name is the parameter path/name.
	Name string `json:"Name"`

	// Type is the parameter type: String, StringList, or SecureString.
	Type string `json:"Type"`

	// Value is the parameter value.
	Value string `json:"Value"`

	// Version is the parameter version number.
	Version int64 `json:"Version"`

	// Description is an optional human-readable description.
	Description string `json:"Description,omitempty"`

	// KeyID is the KMS key used to encrypt SecureString parameters.
	KeyID string `json:"KeyId,omitempty"`

	// LastModifiedDate is when the parameter was last modified.
	LastModifiedDate time.Time `json:"LastModifiedDate"`

	// Tags holds resource tags.
	Tags []SSMTag `json:"Tags,omitempty"`

	// AccountID is the owning AWS account.
	AccountID string `json:"AccountID"`

	// Region is the AWS region.
	Region string `json:"Region"`

	// ARN is the Amazon Resource Name of the parameter.
	ARN string `json:"ARN"`
}

// SSMTag is a key-value tag for SSM resources.
type SSMTag struct {
	// Key is the tag key.
	Key string `json:"Key"`

	// Value is the tag value.
	Value string `json:"Value"`
}

// ssmParameterARN constructs an SSM parameter ARN.
func ssmParameterARN(region, accountID, name string) string {
	return fmt.Sprintf("arn:aws:ssm:%s:%s:parameter%s", region, accountID, name)
}
