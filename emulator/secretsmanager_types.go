package emulator

import (
	"fmt"
	"strings"
	"time"
)

const secretsManagerNamespace = "secretsmanager"

// SecretState holds the state of a Secrets Manager secret.
type SecretState struct {
	// ARN is the Amazon Resource Name of the secret.
	ARN string `json:"ARN"`

	// Name is the secret name.
	Name string `json:"Name"`

	// Description is an optional human-readable description.
	Description string `json:"Description,omitempty"`

	// KMSKeyID is the KMS key used to encrypt the secret.
	KMSKeyID string `json:"KmsKeyId,omitempty"`

	// Tags holds resource tags.
	Tags []SMTag `json:"Tags,omitempty"`

	// CurrentVersionID is the current version identifier.
	CurrentVersionID string `json:"CurrentVersionId"`

	// AccountID is the owning AWS account.
	AccountID string `json:"AccountID"`

	// Region is the AWS region.
	Region string `json:"Region"`

	// CreatedDate is when the secret was created.
	CreatedDate time.Time `json:"CreatedDate"`

	// LastChangedDate is when the secret value was last changed.
	LastChangedDate time.Time `json:"LastChangedDate"`

	// RotationEnabled indicates whether rotation is enabled.
	RotationEnabled bool `json:"RotationEnabled"`

	// DeletionDate is the end of the recovery window a DeleteSecret call opened, and the zero time when
	// no deletion is scheduled. A non-zero value is the whole of what makes a secret "scheduled for
	// deletion": DeleteSecret answers it, DescribeSecret reports it as DeletedDate, GetSecretValue
	// refuses while it is set, and RestoreSecret clears it (#953). It is never a wall-clock time — it is
	// the simulated clock at the request plus RecoveryWindowInDays.
	DeletionDate time.Time `json:"DeletionDate,omitempty"`
}

// SMTag is a key-value tag for Secrets Manager resources.
type SMTag struct {
	// Key is the tag key.
	Key string `json:"Key"`

	// Value is the tag value.
	Value string `json:"Value"`
}

// generateSecretARN constructs a Secrets Manager secret ARN.
func generateSecretARN(region, accountID, name string) string {
	return fmt.Sprintf("arn:aws:secretsmanager:%s:%s:secret:%s", region, accountID, name)
}

// generateVersionID returns a new uppercase hex version ID.
func generateVersionID() string {
	return strings.ToUpper(randomHex(8))
}
