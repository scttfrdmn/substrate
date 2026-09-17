package emulator

import (
	"encoding/base64"
	"fmt"
	"strings"
	"time"
)

const kmsNamespace = "kms"

// kmsKeyStatePendingDeletion is the KeyState a key scheduled for deletion carries.
//
// It is a constant, where "Enabled" and "Disabled" remain literals at the sites that write them,
// because it is the one key state substrate *compares* against: ScheduleKeyDeletion writes it and
// #949's rotation guard reads it, so a typo in either would silently stop the guard from firing
// rather than failing to compile. The Enabled/Disabled literals are only ever written.
const kmsKeyStatePendingDeletion = "PendingDeletion"

// kmsKeyStateDisabled is the KeyState CancelKeyDeletion leaves behind, and the state DisableKey
// writes.
//
// It became a constant with #963 for the same reason [kmsKeyStatePendingDeletion] is one: it is now
// compared as well as written. API_KeyMetadata states the invariant that makes the comparison worth
// having — "Enabled: when KeyState is Enabled this value is true, otherwise it is false" — so the
// state and the boolean cannot be set independently, and a test asserts they agree.
const kmsKeyStateDisabled = "Disabled"

// kmsMinPendingWindowInDays and kmsMaxPendingWindowInDays bound ScheduleKeyDeletion's waiting period.
//
// API_ScheduleKeyDeletion publishes PendingWindowInDays with a Valid Range of 7 to 30 inclusive. The
// bounds are named rather than inlined because three places need the same pair — the guard, the
// message [kmsInvalidPendingWindow] builds, and the default below — and a range enforced against one
// number and reported as another is worse than no range at all.
const (
	kmsMinPendingWindowInDays = 7
	kmsMaxPendingWindowInDays = 30
)

// kmsDefaultPendingWindowInDays is the waiting period ScheduleKeyDeletion applies when the caller
// sends none.
//
// API_ScheduleKeyDeletion: "By default, AWS KMS applies a waiting period of 30 days" and "if you do
// not include a value, it defaults to 30". It coincides with [kmsMaxPendingWindowInDays] and is a
// separate constant regardless, because the two are the same number for no reason a caller can rely
// on: AWS could widen the range without moving the default.
const kmsDefaultPendingWindowInDays = 30

// KMSKey represents a KMS customer master key.
type KMSKey struct {
	// KeyID is the unique identifier for the key.
	KeyID string `json:"KeyId"`

	// ARN is the Amazon Resource Name of the key.
	ARN string `json:"Arn"`

	// Description is a human-readable description.
	Description string `json:"Description,omitempty"`

	// KeyUsage is the cryptographic usage: ENCRYPT_DECRYPT, SIGN_VERIFY, etc.
	KeyUsage string `json:"KeyUsage"`

	// KeySpec is the key spec: SYMMETRIC_DEFAULT, RSA_2048, etc.
	KeySpec string `json:"KeySpec"`

	// KeyState is the state: Enabled, [kmsKeyStateDisabled] or [kmsKeyStatePendingDeletion]. AWS
	// publishes five more — PendingImport, PendingReplicaDeletion, Unavailable, Creating and
	// Updating — that substrate never writes.
	KeyState string `json:"KeyState"`

	// DeletionDate is when a key scheduled for deletion is deleted, zero for a key that is not.
	//
	// Stored by #963 so that the date ScheduleKeyDeletion reports is the date DescribeKey reports.
	// Before it the value was computed inside the handler and discarded, so a caller could learn the
	// deletion date exactly once — from the response to the call that set it — and any later
	// observation had lost it. That also left the refusal #963 adds unassertable: a ScheduleKeyDeletion
	// declined against an already-pending key must leave the existing date alone, and a date nothing
	// reads back cannot be shown to be unchanged.
	//
	// API_KeyMetadata makes it observable and bounds when: "the date and time after which AWS KMS
	// deletes this KMS key. This value is present only when the KMS key is scheduled for deletion,
	// that is, when its KeyState is PendingDeletion." Rendered on that condition alone, so the zero
	// value is never emitted as an epoch timestamp.
	DeletionDate time.Time `json:"DeletionDate,omitempty"`

	// Enabled indicates whether the key is enabled.
	Enabled bool `json:"Enabled"`

	// MultiRegion indicates whether this is a multi-region key.
	MultiRegion bool `json:"MultiRegion"`

	// RotationEnabled indicates whether automatic rotation is enabled.
	RotationEnabled bool `json:"RotationEnabled"`

	// Tags holds resource tags.
	Tags []KMSTag `json:"Tags,omitempty"`

	// AccountID is the owning AWS account.
	AccountID string `json:"AccountID"`

	// Region is the AWS region.
	Region string `json:"Region"`

	// CreationDate is when the key was created.
	CreationDate time.Time `json:"CreationDate"`
}

// KMSTag is a key-value tag for KMS resources.
type KMSTag struct {
	// TagKey is the tag key.
	TagKey string `json:"TagKey"`

	// TagValue is the tag value.
	TagValue string `json:"TagValue"`
}

// generateKMSKeyID generates a UUID-like KMS key ID.
func generateKMSKeyID() string {
	h := randomHex(32)
	return fmt.Sprintf("%s-%s-%s-%s-%s", h[0:8], h[8:12], h[12:16], h[16:20], h[20:32])
}

// kmsKeyARN constructs a KMS key ARN.
func kmsKeyARN(region, accountID, keyID string) string {
	return fmt.Sprintf("arn:aws:kms:%s:%s:key/%s", region, accountID, keyID)
}

// kmsEncryptStub produces a deterministic stub ciphertext for testing.
// Format: base64(kms:{keyID}:{base64(plaintext)}).
func kmsEncryptStub(keyID string, plaintext []byte) []byte {
	inner := base64.StdEncoding.EncodeToString(plaintext)
	raw := fmt.Sprintf("kms:%s:%s", keyID, inner)
	return []byte(base64.StdEncoding.EncodeToString([]byte(raw)))
}

// kmsDecryptStub reverses kmsEncryptStub.
func kmsDecryptStub(ciphertext []byte) (keyID string, plaintext []byte, err error) {
	outer, err := base64.StdEncoding.DecodeString(string(ciphertext))
	if err != nil {
		return "", nil, fmt.Errorf("kms decrypt stub: decode outer: %w", err)
	}
	parts := strings.SplitN(string(outer), ":", 3)
	if len(parts) != 3 || parts[0] != "kms" {
		return "", nil, fmt.Errorf("kms decrypt stub: invalid format")
	}
	pt, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil {
		return "", nil, fmt.Errorf("kms decrypt stub: decode inner: %w", err)
	}
	return parts[1], pt, nil
}
