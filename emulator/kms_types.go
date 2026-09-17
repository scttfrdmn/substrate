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

// kmsSymmetricDefaultKeySpec is the one key spec that supports symmetric encryption, and the only one
// that supports automatic rotation.
//
// It has the same value as [kmsSymmetricDefaultAlgorithm] and is a separate constant because the two are
// different kinds of thing: one is a KeySpec on a key, the other an EncryptionAlgorithm in a request, and
// they coincide only because AWS reused the name. Sharing one identifier would read as though a key spec
// and an algorithm were interchangeable, which is exactly the confusion
// [kmsEncryptionAlgorithmsByKeySpec] exists to resolve — every other spec maps to algorithms with
// different names.
const kmsSymmetricDefaultKeySpec = "SYMMETRIC_DEFAULT"

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

// kmsMinRotationPeriodInDays and kmsMaxRotationPeriodInDays bound EnableKeyRotation's rotation period.
//
// API_EnableKeyRotation publishes RotationPeriodInDays with a Valid Range of 90 to 2560 inclusive, and
// API_GetKeyRotationStatus republishes the identical range on the response element that reports it — which
// is the fact #964 turned on, because a range stated on both ends of a round trip is a value AWS expects a
// caller to write and read back rather than one it merely accepts.
//
// Named for the reason the pending-window pair above is named: the guard, the message
// [kmsInvalidRotationPeriod] builds, and the default below must all use one pair of numbers.
const (
	kmsMinRotationPeriodInDays = 90
	kmsMaxRotationPeriodInDays = 2560
)

// kmsDefaultRotationPeriodInDays is the rotation period EnableKeyRotation applies when the caller sends
// none.
//
// API_EnableKeyRotation: "If no value is specified, the default value is 365 days", repeated on
// API_GetKeyRotationStatus's response element as "The default value is 365 days". Unlike
// [kmsDefaultPendingWindowInDays] this does not coincide with either bound, so it cannot be confused for
// one.
//
// The default applies on *every* successful EnableKeyRotation that omits the member, not only the first.
// AWS states the rule unconditionally and also documents the parameter as able to "modify the rotation
// period of a key that you previously enabled automatic key rotation on", so a second call omitting it
// resets the period to 365 rather than preserving the value the first call set. That reading is recorded
// in [KMSPlugin.enableKeyRotation] and pinned by a test, because the opposite reading is the more
// intuitive one and would otherwise be an easy "fix".
const kmsDefaultRotationPeriodInDays = 365

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
	//
	// Reported by GetKeyRotationStatus as KeyRotationEnabled, and deliberately *not* by DescribeKey:
	// API_KeyMetadata publishes no RotationEnabled member, so #971 removed the one substrate used to
	// render there. Rotation state is observable through GetKeyRotationStatus alone.
	RotationEnabled bool `json:"RotationEnabled"`

	// RotationPeriodInDays is the number of days between automatic rotations, as set by
	// EnableKeyRotation and reported by GetKeyRotationStatus.
	//
	// Zero means the key has never had rotation enabled. It is not a legal period — the range is
	// [kmsMinRotationPeriodInDays] to [kmsMaxRotationPeriodInDays] — so the zero value is unambiguous
	// and needs no separate "was it ever set" flag. Every successful EnableKeyRotation writes a value in
	// range, so a non-zero reading here is always one AWS would have accepted.
	//
	// DisableKeyRotation leaves it alone. AWS documents no clearing, and keeping it means a caller that
	// disables and re-enables with an explicit period never sees a stale one; a re-enable that omits the
	// period overwrites it with [kmsDefaultRotationPeriodInDays] regardless, per that constant's note.
	RotationPeriodInDays int `json:"RotationPeriodInDays,omitempty"`

	// RotationEnabledDate is when the last successful EnableKeyRotation ran, zero if none has.
	//
	// Stored by #973 for one reason: it is the other half of NextRotationDate, which
	// API_EnableKeyRotation defines against it — "the rotation period defines the number of days after
	// you enable automatic key rotation that AWS KMS will rotate your key material, and the number of
	// days between each automatic rotation thereafter." So the next date is this date plus
	// [KMSKey.RotationPeriodInDays], and neither half alone yields it.
	//
	// [KMSKey.CreationDate] is not it. A key can be created long before rotation is turned on, and the
	// page ties the schedule to the enable date rather than the creation date, so reusing CreationDate
	// would report a rotation in the past for any key whose rotation was enabled later than a period ago.
	//
	// Every successful EnableKeyRotation overwrites it, including one that only changes the period. AWS
	// documents no answer for what a period change does to an existing schedule, so this is substrate's
	// reading, and it is the one that cannot report a date already past: keeping the first enable date and
	// shortening the period would do exactly that. DisableKeyRotation leaves it alone, matching what
	// RotationPeriodInDays above does and for the same reason — what changes is whether the schedule is
	// reported, not whether it is remembered.
	//
	// Nothing rotates. Substrate models no key material and ListKeyRotations is unimplemented, so this is
	// a schedule reported to a caller rather than an event that fires; the clock never advances it and no
	// rotation is recorded when it passes. AWS's own re-enable rules — "if the key material in the
	// re-enabled KMS key hasn't been rotated in one year, AWS KMS rotates it immediately" — describe
	// rotations substrate does not perform, and are out of scope for the same reason.
	RotationEnabledDate time.Time `json:"RotationEnabledDate,omitempty"`

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
