package substrate

import (
	"encoding/base64"
	"fmt"
	"strings"
	"time"
)

const kmsNamespace = "kms"

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

	// KeyState is the state: Enabled, Disabled, PendingDeletion, etc.
	KeyState string `json:"KeyState"`

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
