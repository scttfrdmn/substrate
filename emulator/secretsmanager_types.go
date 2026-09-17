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

	// RotationLambdaARN is the ARN of the Lambda rotation function RotateSecret configured, and the
	// empty string when no rotation function has ever been configured (#952).
	//
	// It is the whole of what makes RotateSecret's second published InvalidRequestException cause
	// decidable — "you tried to enable rotation on a secret that doesn't already have a Lambda function
	// ARN configured and you didn't include such an ARN as a parameter in this call" — so an empty value
	// is not merely an unreported member, it is the condition a rotation is refused on. Substrate records
	// the ARN and never invokes what it names, which is the boundary CLAUDE.md draws.
	RotationLambdaARN string `json:"RotationLambdaARN,omitempty"`

	// RotationRules is the rotation schedule RotateSecret configured, and nil when rotation has never
	// been turned on.
	//
	// A pointer rather than a value, because DescribeSecret's own documentation for this member is "if
	// the secret never had rotation turned on, this field is omitted" — and a zero-valued struct and an
	// absent one are the same observation once the member is emitted, which is the distinction the
	// omission is about.
	RotationRules *SMRotationRules `json:"RotationRules,omitempty"`

	// RotateImmediately is the last RotateSecret call's RotateImmediately, defaulting to true — "the
	// default for RotateImmediately is true. If you don't specify this value, Secrets Manager rotates
	// the secret immediately."
	//
	// No API response member turns on it, and that is not an omission: the difference AWS describes
	// between true and false is whether the rotation function runs now or at the next window, and
	// substrate executes no rotation function at all. It is recorded rather than discarded because the
	// state snapshot is itself an observable here — a replay's state hash covers it, and time-travel
	// inspection reports it — so the intent a caller expressed survives the request that expressed it.
	RotateImmediately bool `json:"RotateImmediately"`

	// DeletionDate is the end of the recovery window a DeleteSecret call opened, and the zero time when
	// no deletion is scheduled. A non-zero value is the whole of what makes a secret "scheduled for
	// deletion": DeleteSecret answers it, DescribeSecret reports it as DeletedDate, GetSecretValue
	// refuses while it is set, and RestoreSecret clears it (#953). It is never a wall-clock time — it is
	// the simulated clock at the request plus RecoveryWindowInDays.
	DeletionDate time.Time `json:"DeletionDate,omitempty"`

	// EverTagged records that this secret has carried a tag; see [taggingEverTagged] (#938).
	EverTagged bool `json:"ever_tagged,omitempty"`
}

// SMRotationRules is a secret's rotation schedule, as RotateSecret configures it and DescribeSecret
// reports it — AWS's RotationRulesType.
//
// Every member is optional and every one is emitted only when set, which matches the shape AWS's own
// samples send: a schedule is written as either AutomaticallyAfterDays or ScheduleExpression, with
// Duration optionally narrowing the window a ScheduleExpression opens.
type SMRotationRules struct {
	// AutomaticallyAfterDays is the number of days between rotations, which AWS constrains to a
	// "minimum value of 1, maximum value of 1000". It cannot be set alongside ScheduleExpression: "in
	// RotateSecret, you can set the rotation schedule in RotationRules with AutomaticallyAfterDays or
	// ScheduleExpression, but not both."
	AutomaticallyAfterDays int64 `json:"AutomaticallyAfterDays,omitempty"`

	// Duration is the length of the rotation window in hours, "for example 3h for a three hour window".
	// AWS publishes the pattern [0-9]+h and a length of 2 to 3, which substrate records rather than
	// enforces — see the file preamble of secretsmanager_rotation.go.
	Duration string `json:"Duration,omitempty"`

	// ScheduleExpression is "a cron() or rate() expression that defines the schedule for rotating your
	// secret". Substrate records it and schedules nothing: no rotation function runs here, so the
	// expression is the recorded intent a DescribeSecret caller reads back.
	ScheduleExpression string `json:"ScheduleExpression,omitempty"`
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
