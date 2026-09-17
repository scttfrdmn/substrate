package emulator_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// #985: CustomerMasterKeySpec is a CreateKey *request* parameter and substrate decoded only the response
// member of that name.
//
// A caller on an SDK old enough to still send the deprecated name asked for RSA_4096 and got a symmetric
// key. That is the quietest possible failure — a 200 with a complete, well-formed KeyMetadata — and #974
// made it quieter still by adding the response member, so substrate echoed the deprecated name back with
// SYMMETRIC_DEFAULT, contradicting the value it had just been sent.
//
// This is #977's request-side twin: the same handler, the same validation surface, and deliberately the
// same code for a malformed member, because two codes for one class of defect on one operation is the
// thing that issue's own analysis warned against. What is new here is the *narrower* enum — thirteen
// values against KeySpec's seventeen — and the conflict between two members that AWS documents as
// carrying one value.
//
// Five things are asserted:
//
//  1. **The deprecated member is honored**, and the key it produces is asymmetric all the way down: the
//     spec is reported back under both names, and the algorithm list is the signing one.
//  2. **Its own enum is enforced**, so a spec valid under KeySpec and not under this member is refused
//     rather than quietly accepted — the request-side half of the omission #974 chose when reporting it.
//  3. **A conflict is refused**, and equal values are not. AWS documents no answer for the conflict; what
//     it documents is that the two members have the same value.
//  4. **The pairing rules #977 established apply to a spec that arrived under the deprecated name**, since
//     they are keyed on the resolved spec and not on which member carried it.
//  5. **A refused request creates no key**, asserted over the wire through ListKeys, because a validation
//     that refuses and writes anyway is worse than no validation.
//
// Every call goes over the wire (#765) and every refusal asserts the status beside the code (#923).

// kmsDeprecatedKeySpecs is the thirteen values API_CreateKey publishes for CustomerMasterKeySpec, in its
// order — transcribed rather than read from the production list, so the two disagree unless both match the
// page.
var kmsDeprecatedKeySpecs = []string{
	"RSA_2048", "RSA_3072", "RSA_4096",
	"ECC_NIST_P256", "ECC_NIST_P384", "ECC_NIST_P521", "ECC_SECG_P256K1",
	"SYMMETRIC_DEFAULT",
	"HMAC_224", "HMAC_256", "HMAC_384", "HMAC_512",
	"SM2",
}

// kmsKeySpecsOutsideDeprecatedEnum is the four specs KeySpec carries and CustomerMasterKeySpec does not.
//
// They are the four added after the member was deprecated, which is why the enums differ at all, and they
// are the whole subject of assertion 2.
var kmsKeySpecsOutsideDeprecatedEnum = []string{
	"ML_DSA_44", "ML_DSA_65", "ML_DSA_87", "ECC_NIST_EDWARDS25519",
}

// TestKMSCreateKey_TheDeprecatedKeySpecMemberIsHonored is assertion 1, and it is the defect itself: every
// row below produced a symmetric key before #985.
//
// The rows walk the three shapes of key the deprecated enum can name — asymmetric encryption-or-signing,
// signing-only, and HMAC — because the failure was one substitution and a single row could pass against an
// implementation that special-cased RSA. Each asserts the metadata rather than the echo alone: the point is
// not that substrate repeats the string back but that the key it created is of that type, which the
// algorithm member is the observable proof of.
func TestKMSCreateKey_TheDeprecatedKeySpecMemberIsHonored(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		keySpec   string
		keyUsage  string
		algorithm string
	}{
		{"an RSA key for signing", "RSA_4096", "SIGN_VERIFY", "SigningAlgorithms"},
		{"an RSA key for encryption", "RSA_2048", "ENCRYPT_DECRYPT", "EncryptionAlgorithms"},
		{"an ECC key for signing", "ECC_NIST_P384", "SIGN_VERIFY", "SigningAlgorithms"},
		{"an HMAC key", "HMAC_512", "GENERATE_VERIFY_MAC", "MacAlgorithms"},
		{"a key agreement key", "ECC_NIST_P256", "KEY_AGREEMENT", "KeyAgreementAlgorithms"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := arnGuardServer(t)

			meta := kmsCreateKeyMetadata(t, ts, map[string]any{
				"CustomerMasterKeySpec": tc.keySpec,
				"KeyUsage":              tc.keyUsage,
			})
			assert.Equal(t, tc.keySpec, kmsMetadataString(t, meta, "KeySpec"),
				"the key spec the deprecated member asked for is the key's own")
			assert.Equal(t, tc.keySpec, kmsMetadataString(t, meta, "CustomerMasterKeySpec"),
				"the deprecated member reports the same value, per AWS's own sentence")
			assert.Equal(t, tc.keyUsage, kmsMetadataString(t, meta, "KeyUsage"),
				"the usage is unaffected by which member carried the spec")
			assert.Contains(t, meta, tc.algorithm,
				"the key is of the type asked for, not a symmetric one wearing the label")
		})
	}
}

// TestKMSCreateKey_TheDeprecatedMemberKeepsItsNarrowerEnum is assertion 2.
//
// The four specs added since the deprecation are valid under KeySpec and not under this member. Refusing
// them is substrate's reading — AWS publishes the narrower Valid Values list and no code for a value
// outside it — and it is the same decision #974 made when it chose to omit the member for such a key
// rather than report a value outside the member's own set. The alternative would let a request name a spec
// through a member AWS does not publish it under.
//
// The message must name both the member and its set: the value is a perfectly good key spec, so a bare
// refusal would read as "no such spec" and send the caller looking for a typo that is not there.
func TestKMSCreateKey_TheDeprecatedMemberKeepsItsNarrowerEnum(t *testing.T) {
	t.Parallel()

	for _, keySpec := range kmsKeySpecsOutsideDeprecatedEnum {
		t.Run(keySpec, func(t *testing.T) {
			t.Parallel()
			ts := arnGuardServer(t)

			status, code, message := kmsRefusal(t, ts, "CreateKey", map[string]any{
				"CustomerMasterKeySpec": keySpec,
				"KeyUsage":              "SIGN_VERIFY",
			})
			assert.Equal(t, "ValidationError", code, "%s under the deprecated member", keySpec)
			assert.Equal(t, http.StatusBadRequest, status, "%s under the deprecated member", keySpec)
			assert.Contains(t, message, "CustomerMasterKeySpec", "the message names the member")
			assert.Contains(t, message, "KeySpec", "the message names the member that does carry it")

			// The same value under the newer member is accepted, which is what makes this a narrowing
			// rather than a rejection of the spec.
			meta := kmsCreateKeyMetadata(t, ts, map[string]any{
				"KeySpec": keySpec, "KeyUsage": "SIGN_VERIFY",
			})
			assert.Equal(t, keySpec, kmsMetadataString(t, meta, "KeySpec"),
				"KeySpec carries the value the deprecated member cannot")
		})
	}

	// Every value the member *does* publish is accepted through it, so the test above cannot pass against
	// an implementation that refuses the deprecated member outright.
	for _, keySpec := range kmsDeprecatedKeySpecs {
		t.Run("accepted/"+keySpec, func(t *testing.T) {
			t.Parallel()
			ts := arnGuardServer(t)

			meta := kmsCreateKeyMetadata(t, ts, map[string]any{
				"CustomerMasterKeySpec": keySpec,
				"KeyUsage":              kmsDefaultKeyUsageFor(t, keySpec),
			})
			assert.Equal(t, keySpec, kmsMetadataString(t, meta, "KeySpec"),
				"%s is one of the thirteen the deprecated member publishes", keySpec)
		})
	}
}

// TestKMSCreateKey_TheTwoKeySpecMembersMustAgree is assertion 3, and it is the one decision here that AWS
// does not document at all.
//
// The published fact is that the two members "have the same value", so a request in which they disagree is
// not a request the page describes and substrate refuses it. The two rejected alternatives are worth naming
// because both look reasonable: a precedence rule discards half of a contradictory request silently, which
// is the failure mode this issue exists to remove rather than relocate, and resolving by JSON member order
// makes the answer depend on something no caller controls meaningfully.
//
// The equal-value row is the counterweight. An SDK migrating between the two names may well send both, and
// that is a caller agreeing with itself rather than contradicting itself.
func TestKMSCreateKey_TheTwoKeySpecMembersMustAgree(t *testing.T) {
	t.Parallel()

	t.Run("different values are refused", func(t *testing.T) {
		t.Parallel()
		ts := arnGuardServer(t)

		status, code, message := kmsRefusal(t, ts, "CreateKey", map[string]any{
			"KeySpec":               "RSA_4096",
			"CustomerMasterKeySpec": "RSA_2048",
			"KeyUsage":              "SIGN_VERIFY",
		})
		assert.Equal(t, "ValidationError", code, "the two members disagree")
		assert.Equal(t, http.StatusBadRequest, status, "the two members disagree")
		assert.Contains(t, message, "RSA_4096", "the message names both values, since either could be meant")
		assert.Contains(t, message, "RSA_2048", "the message names both values, since either could be meant")
	})

	t.Run("equal values are accepted", func(t *testing.T) {
		t.Parallel()
		ts := arnGuardServer(t)

		meta := kmsCreateKeyMetadata(t, ts, map[string]any{
			"KeySpec":               "RSA_4096",
			"CustomerMasterKeySpec": "RSA_4096",
			"KeyUsage":              "SIGN_VERIFY",
		})
		assert.Equal(t, "RSA_4096", kmsMetadataString(t, meta, "KeySpec"),
			"agreeing with itself is not a defect")
	})

	// A conflict is reported even when the deprecated value is the one outside its own enum, because the
	// member's enum is checked before the two are compared: the value is wrong on its own terms, and saying
	// so is more useful than reporting a disagreement between one good value and one bad one.
	t.Run("a value outside the deprecated enum is reported as such", func(t *testing.T) {
		t.Parallel()
		ts := arnGuardServer(t)

		_, code, message := kmsRefusal(t, ts, "CreateKey", map[string]any{
			"KeySpec":               "RSA_4096",
			"CustomerMasterKeySpec": "ML_DSA_44",
			"KeyUsage":              "SIGN_VERIFY",
		})
		assert.Equal(t, "ValidationError", code, "both readings answer one code")
		assert.Contains(t, message, "not one of",
			"the enum is reported, not the disagreement")
	})
}

// TestKMSCreateKey_TheDeprecatedMemberObeysThePairingRules is assertion 4.
//
// #977's rules are keyed on the resolved key spec, so they must apply identically however the spec arrived.
// An implementation that resolved the deprecated member after the pairing check — or beside it — would
// accept an HMAC key with no usage, or an ECC key that encrypts, through the older name only. Both rows
// are the same refusals #977 pins for KeySpec, asserted through the other member.
func TestKMSCreateKey_TheDeprecatedMemberObeysThePairingRules(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		body  map[string]any
		code  string
		names string
	}{
		{
			name:  "a key usage is still required",
			body:  map[string]any{"CustomerMasterKeySpec": "HMAC_256"},
			code:  "ValidationError",
			names: "HMAC_256",
		},
		{
			name: "an inadmissible pair is still refused",
			body: map[string]any{
				"CustomerMasterKeySpec": "ECC_NIST_P256", "KeyUsage": "ENCRYPT_DECRYPT",
			},
			code:  "UnsupportedOperationException",
			names: "ECC_NIST_P256",
		},
		{
			name: "an unpublished key usage is still refused",
			body: map[string]any{
				"CustomerMasterKeySpec": "RSA_2048", "KeyUsage": "TRANSMOGRIFY",
			},
			code:  "ValidationError",
			names: "TRANSMOGRIFY",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := arnGuardServer(t)

			status, code, message := kmsRefusal(t, ts, "CreateKey", tc.body)
			assert.Equal(t, tc.code, code, "%s", tc.name)
			assert.Equal(t, http.StatusBadRequest, status, "%s", tc.name)
			assert.Contains(t, message, tc.names, "the message names what was wrong")
		})
	}
}

// TestKMSCreateKey_ADeprecatedMemberRefusalCreatesNoKey is assertion 5.
//
// The check runs before the key ID is minted and before anything is written, which is the only order that
// makes sense for a member that cannot be changed afterwards — but "runs before" is a property of the code
// and this asserts the property of the emulator. A validation that refuses and writes anyway leaves a key
// AWS would not have created, which is exactly the state #985 set out to make unreachable.
func TestKMSCreateKey_ADeprecatedMemberRefusalCreatesNoKey(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	require.Equal(t, 0, kmsListKeyCount(t, ts), "the account starts with no keys")

	for _, body := range []map[string]any{
		{"CustomerMasterKeySpec": "ML_DSA_87", "KeyUsage": "SIGN_VERIFY"},
		{"KeySpec": "RSA_2048", "CustomerMasterKeySpec": "RSA_4096", "KeyUsage": "SIGN_VERIFY"},
		{"CustomerMasterKeySpec": "NOT_A_SPEC"},
	} {
		status, _, _ := kmsRefusal(t, ts, "CreateKey", body)
		require.Equal(t, http.StatusBadRequest, status, "the request is refused: %v", body)
	}

	assert.Equal(t, 0, kmsListKeyCount(t, ts), "no refused request left a key behind")
}
