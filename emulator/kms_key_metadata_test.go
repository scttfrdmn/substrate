package emulator_test

import (
	"encoding/json"
	"net/http"
	"slices"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file covers #974: DescribeKey answered 10 of API_KeyMetadata's 26 members, and CreateKey answered
// 9 of them from a second map of its own.
//
// Every assertion here is on **raw JSON**, because the defect and its fix are both about a member being
// present at all. A decoded struct cannot tell an absent member from a zero-valued one — the miss
// iam_shape_members_test.go:88 records — and this file's whole subject is presence: five members that
// must now appear, four that must appear only for their own KeyUsage, one that must vanish for four key
// specs, and nine that must stay away.
//
// Four things are asserted that a smaller test could not:
//
//   - CreateKey and DescribeKey report the *same* member set with the *same* values for one key. That is
//     the invariant the shared builder exists for, and it failed before #974 in a way neither operation's
//     own test could see: DescribeKey emitted DeletionDate and CreateKey did not.
//   - An algorithm list appears only for the KeyUsage that names it as its condition, and the other three
//     members are asserted **absent** in the same call. Asserting only the expected member would pass
//     against a builder that emitted all four.
//   - A key whose usage and spec AWS would never pair reports no algorithm member at all, rather than an
//     empty array. That is a claim-nothing answer against a claim-something one, and only raw JSON
//     separates them.
//   - The members that stay absent are asserted absent, so a later change that starts reporting one has
//     to argue for it here rather than acquire it silently.
//
// Every non-symmetric key below is created through the gap #977 records — CreateKey validates KeySpec and
// KeyUsage against nothing — which is why #977 must re-verify this file when it closes it. Every call
// goes over the wire, per #765.

// kmsCreateKeyMetadata posts CreateKey and returns its KeyMetadata member, undecoded.
//
// The sibling of [kmsKeyMetadata], which does the same for DescribeKey. Both exist because #974's central
// assertion is that the two agree, and that cannot be written if one side is only reachable through a
// helper that decodes into a struct.
func kmsCreateKeyMetadata(t *testing.T, ts *emulator.TestServer, body map[string]any) map[string]json.RawMessage {
	t.Helper()
	out := kmsRawBody(t, ts, "CreateKey", body)
	raw, ok := out["KeyMetadata"]
	require.True(t, ok, "CreateKey should report KeyMetadata")
	var meta map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &meta))
	return meta
}

// kmsMetadataMembers returns the member names present in a KeyMetadata element, sorted.
func kmsMetadataMembers(meta map[string]json.RawMessage) []string {
	names := make([]string, 0, len(meta))
	for name := range meta {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// kmsMetadataString reads a string member, requiring it to be present.
func kmsMetadataString(t *testing.T, meta map[string]json.RawMessage, member string) string {
	t.Helper()
	raw, ok := meta[member]
	require.True(t, ok, "KeyMetadata should carry %s: %v", member, kmsMetadataMembers(meta))
	var value string
	require.NoError(t, json.Unmarshal(raw, &value), "decode %s", member)
	return value
}

// kmsMetadataStrings reads a string-list member, requiring it to be present.
func kmsMetadataStrings(t *testing.T, meta map[string]json.RawMessage, member string) []string {
	t.Helper()
	raw, ok := meta[member]
	require.True(t, ok, "KeyMetadata should carry %s: %v", member, kmsMetadataMembers(meta))
	var value []string
	require.NoError(t, json.Unmarshal(raw, &value), "decode %s", member)
	return value
}

// kmsAlgorithmMembers is the four mutually exclusive algorithm members, one per published KeyUsage.
//
// Named as a set rather than written out per case so that each test asserting one of them present can
// assert the other three absent from the same response without listing them, which is what makes the
// exclusivity assertable rather than merely intended.
var kmsAlgorithmMembers = []string{
	"EncryptionAlgorithms",
	"SigningAlgorithms",
	"MacAlgorithms",
	"KeyAgreementAlgorithms",
}

// kmsUnreachableMetadataMembers is every KeyMetadata member substrate cannot reach, plus the one it
// reported and AWS does not publish.
//
// Each is absent for a reason recorded in kms_key_metadata.go — three need a custom or external key
// store, two need an EXTERNAL origin, one needs a replica, one needs the PendingReplicaDeletion state,
// one needs a key material identity (#978) — and RotationEnabled is here because API_KeyMetadata does not
// publish it at all, which is #971. Adding sixteen members from that page is exactly when someone
// working from the struct rather than the page would put it back, so the guard against that sits beside
// the guard for the eight.
var kmsUnreachableMetadataMembers = []string{
	"CloudHsmClusterId",
	"CustomKeyStoreId",
	"XksKeyConfiguration",
	"ExpirationModel",
	"ValidTo",
	"MultiRegionConfiguration",
	"PendingDeletionWindowInDays",
	"CurrentKeyMaterialId",
	"RotationEnabled",
}

// TestKMSKeyMetadata_CreateKeyAndDescribeKeyReportOneShape is the assertion #974 is really about.
//
// API_KeyMetadata "is used as a response element for the CreateKey, DescribeKey, and ReplicateKey
// operations" — one shape, three operations — and substrate built it twice. The two maps had already
// drifted: #963 added DeletionDate to DescribeKey's and left CreateKey's alone, so the two operations
// disagreed about a member of one documented type. Neither operation's own test could see that, because
// each asserted its own response against the page rather than against the other.
//
// Both member names and values are compared, on raw bytes. Comparing names alone would pass against a
// CreateKey that reported a different account ID under the same member name.
func TestKMSKeyMetadata_CreateKeyAndDescribeKeyReportOneShape(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	created := kmsCreateKeyMetadata(t, ts, map[string]any{"Description": "one shape, two operations"})
	keyID := kmsMetadataString(t, created, "KeyId")
	described := kmsKeyMetadata(t, ts, keyID)

	assert.Equal(t, kmsMetadataMembers(created), kmsMetadataMembers(described),
		"CreateKey and DescribeKey report the same KeyMetadata members")
	for _, member := range kmsMetadataMembers(created) {
		assert.JSONEq(t, string(created[member]), string(described[member]),
			"CreateKey and DescribeKey agree on %s", member)
	}
}

// TestKMSKeyMetadata_ReportsTheMembersAWSAlwaysSends covers the five members a caller got nothing back
// for where AWS sends a value on every call for a plain customer symmetric key.
//
// Each answers a decision a consumer legitimately makes: which account a cross-account grant would name,
// whether the key is theirs to administer, whether its material can expire or be re-imported, which
// algorithms Encrypt will accept, and the same spec again under the deprecated member an older SDK reads.
// Substrate knew all five and reported none.
func TestKMSKeyMetadata_ReportsTheMembersAWSAlwaysSends(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	_, keyID := createKMSKey(t, ts)
	meta := kmsKeyMetadata(t, ts, keyID)

	assert.Equal(t, taggingTestAccount, kmsMetadataString(t, meta, "AWSAccountId"),
		"AWSAccountId is the twelve-digit account that owns the key")
	assert.Equal(t, "CUSTOMER", kmsMetadataString(t, meta, "KeyManager"),
		"every key substrate can create is customer managed")
	assert.Equal(t, "AWS_KMS", kmsMetadataString(t, meta, "Origin"),
		"substrate implements neither ImportKeyMaterial nor a custom key store")
	assert.Equal(t, "SYMMETRIC_DEFAULT", kmsMetadataString(t, meta, "CustomerMasterKeySpec"),
		"the deprecated member carries the same value as KeySpec")
	assert.Equal(t, []string{"SYMMETRIC_DEFAULT"}, kmsMetadataStrings(t, meta, "EncryptionAlgorithms"),
		"a symmetric encryption key admits exactly one algorithm")
}

// TestKMSKeyMetadata_TheAlgorithmListFollowsTheKeyUsage walks one key per algorithm member and asserts
// the other three are absent from the same response.
//
// The four members are mutually exclusive because each publishes a KeyUsage as its condition and a key
// has exactly one usage, so a response carrying two of them would describe a key AWS cannot create. The
// per-spec contents come from the developer guide's key spec reference, which tabulates them for the
// reason it gives twice: "you cannot configure a KMS key to use a particular encryption algorithm" and
// "you cannot configure a KMS key to use particular signing algorithms."
//
// ECC_NIST_EDWARDS25519 and the ML-DSA specs are here because they are the two ends of the interesting
// range: the first is the only spec admitting two signing algorithms, and the three ML-DSA specs share
// one that AWS states in prose rather than in the table.
func TestKMSKeyMetadata_TheAlgorithmListFollowsTheKeyUsage(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		keySpec  string
		keyUsage string
		member   string
		want     []string
	}{
		{
			name:     "symmetric encryption",
			keySpec:  "SYMMETRIC_DEFAULT",
			keyUsage: "ENCRYPT_DECRYPT",
			member:   "EncryptionAlgorithms",
			want:     []string{"SYMMETRIC_DEFAULT"},
		},
		{
			name:     "RSA encryption",
			keySpec:  "RSA_2048",
			keyUsage: "ENCRYPT_DECRYPT",
			member:   "EncryptionAlgorithms",
			want:     []string{"RSAES_OAEP_SHA_1", "RSAES_OAEP_SHA_256"},
		},
		{
			name:     "RSA signing",
			keySpec:  "RSA_3072",
			keyUsage: "SIGN_VERIFY",
			member:   "SigningAlgorithms",
			want: []string{
				"RSASSA_PSS_SHA_256", "RSASSA_PSS_SHA_384", "RSASSA_PSS_SHA_512",
				"RSASSA_PKCS1_V1_5_SHA_256", "RSASSA_PKCS1_V1_5_SHA_384", "RSASSA_PKCS1_V1_5_SHA_512",
			},
		},
		{
			name:     "ECC signing",
			keySpec:  "ECC_NIST_P384",
			keyUsage: "SIGN_VERIFY",
			member:   "SigningAlgorithms",
			want:     []string{"ECDSA_SHA_384"},
		},
		{
			name:     "Edwards signing admits two algorithms",
			keySpec:  "ECC_NIST_EDWARDS25519",
			keyUsage: "SIGN_VERIFY",
			member:   "SigningAlgorithms",
			want:     []string{"ED25519_SHA_512", "ED25519_PH_SHA_512"},
		},
		{
			name:     "ML-DSA signing",
			keySpec:  "ML_DSA_65",
			keyUsage: "SIGN_VERIFY",
			member:   "SigningAlgorithms",
			want:     []string{"ML_DSA_SHAKE_256"},
		},
		{
			name:     "SM2 signing",
			keySpec:  "SM2",
			keyUsage: "SIGN_VERIFY",
			member:   "SigningAlgorithms",
			want:     []string{"SM2DSA"},
		},
		{
			name:     "HMAC",
			keySpec:  "HMAC_384",
			keyUsage: "GENERATE_VERIFY_MAC",
			member:   "MacAlgorithms",
			want:     []string{"HMAC_SHA_384"},
		},
		{
			name:     "key agreement",
			keySpec:  "ECC_NIST_P521",
			keyUsage: "KEY_AGREEMENT",
			member:   "KeyAgreementAlgorithms",
			want:     []string{"ECDH"},
		},
		{
			name:     "SM2 key agreement",
			keySpec:  "SM2",
			keyUsage: "KEY_AGREEMENT",
			member:   "KeyAgreementAlgorithms",
			want:     []string{"ECDH"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := arnGuardServer(t)

			meta := kmsCreateKeyMetadata(t, ts, map[string]any{
				"KeySpec": tc.keySpec, "KeyUsage": tc.keyUsage,
			})
			assert.Equal(t, tc.want, kmsMetadataStrings(t, meta, tc.member),
				"%s with KeyUsage %s reports %s", tc.keySpec, tc.keyUsage, tc.member)
			for _, other := range kmsAlgorithmMembers {
				if other == tc.member {
					continue
				}
				assert.NotContains(t, meta, other,
					"a key with KeyUsage %s reports no %s", tc.keyUsage, other)
			}
		})
	}
}

// TestKMSKeyMetadata_KeyAgreementIsNotReportedForASigningKey pins the one presence condition of the four
// that is substrate's reading rather than AWS's.
//
// API_KeyMetadata publishes a condition for the other three — "this value is present only when the
// KeyUsage of the KMS key is ENCRYPT_DECRYPT", and its two counterparts — and publishes none at all for
// KeyAgreementAlgorithms, whose entire description is "the key agreement algorithm used to derive a
// shared secret". So the narrowest reading is the pattern the other three follow.
//
// The case that separates the readings is a NIST-curve key with KeyUsage SIGN_VERIFY: its spec admits
// ECDH, and only the usage condition keeps the member away. Reporting it would tell a caller
// DeriveSharedSecret was available on a key AWS reserves for Sign.
func TestKMSKeyMetadata_KeyAgreementIsNotReportedForASigningKey(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	meta := kmsCreateKeyMetadata(t, ts, map[string]any{
		"KeySpec": "ECC_NIST_P256", "KeyUsage": "SIGN_VERIFY",
	})
	assert.NotContains(t, meta, "KeyAgreementAlgorithms",
		"a spec that admits ECDH still reports no key agreement algorithm under SIGN_VERIFY")
	assert.Equal(t, []string{"ECDSA_SHA_256"}, kmsMetadataStrings(t, meta, "SigningAlgorithms"),
		"the same key reports the signing algorithm its usage does name")
}

// TestKMSKeyMetadata_CustomerMasterKeySpecIsOmittedOutsideItsOwnEnum covers the deprecated member's
// narrower Valid Values list.
//
// AWS still sends it — "the KeySpec and CustomerMasterKeySpec fields have the same value. We recommend
// that you use the KeySpec field in your code. However, to avoid breaking changes, AWS KMS supports both
// fields" — so substrate answers it. But API_KeyMetadata gives CustomerMasterKeySpec thirteen valid
// values and KeySpec seventeen: the four specs added since the deprecation have no admissible value for
// the older member. Substrate omits it there rather than putting a value outside the member's own
// published set on the wire, and AWS documents no answer for that case, so the omission is a reading and
// this test is where it is recorded.
func TestKMSKeyMetadata_CustomerMasterKeySpecIsOmittedOutsideItsOwnEnum(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		keySpec string
		present bool
	}{
		{keySpec: "SYMMETRIC_DEFAULT", present: true},
		{keySpec: "RSA_4096", present: true},
		{keySpec: "ECC_SECG_P256K1", present: true},
		{keySpec: "HMAC_512", present: true},
		{keySpec: "SM2", present: true},
		{keySpec: "ML_DSA_44", present: false},
		{keySpec: "ML_DSA_65", present: false},
		{keySpec: "ML_DSA_87", present: false},
		{keySpec: "ECC_NIST_EDWARDS25519", present: false},
	} {
		t.Run(tc.keySpec, func(t *testing.T) {
			t.Parallel()
			ts := arnGuardServer(t)

			meta := kmsCreateKeyMetadata(t, ts, map[string]any{"KeySpec": tc.keySpec})
			if tc.present {
				assert.Equal(t, tc.keySpec, kmsMetadataString(t, meta, "CustomerMasterKeySpec"),
					"%s is one of the deprecated member's own valid values", tc.keySpec)
				return
			}
			assert.NotContains(t, meta, "CustomerMasterKeySpec",
				"%s is a KeySpec value the deprecated member's enum does not carry", tc.keySpec)
			assert.Equal(t, tc.keySpec, kmsMetadataString(t, meta, "KeySpec"),
				"the spec is still reported under the member whose enum does carry it")
		})
	}
}

// TestKMSKeyMetadata_AnImpossiblePairReportsNoAlgorithmListAtAll is the honest-empty case #827
// established, reached through #977's gap.
//
// CreateKey validates neither KeySpec nor KeyUsage, so a caller can create an ECC key with KeyUsage
// ENCRYPT_DECRYPT — a pair AWS would refuse. That key admits no encryption algorithm, and the two
// available answers are an empty array and no member. An empty array says "this key supports no
// encryption algorithms", a claim AWS never makes about a key it accepted; the absent member says
// nothing, which is the only honest answer available for a key AWS would not have created.
//
// Asserted on raw JSON because that is the only place the two answers differ: both decode to a nil slice.
func TestKMSKeyMetadata_AnImpossiblePairReportsNoAlgorithmListAtAll(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		keySpec  string
		keyUsage string
	}{
		{name: "ECC key asked to encrypt", keySpec: "ECC_NIST_P256", keyUsage: "ENCRYPT_DECRYPT"},
		{name: "symmetric key asked to sign", keySpec: "SYMMETRIC_DEFAULT", keyUsage: "SIGN_VERIFY"},
		{name: "symmetric key asked to MAC", keySpec: "SYMMETRIC_DEFAULT", keyUsage: "GENERATE_VERIFY_MAC"},
		{name: "HMAC key asked to derive a shared secret", keySpec: "HMAC_256", keyUsage: "KEY_AGREEMENT"},
		{name: "a key usage AWS does not publish", keySpec: "SYMMETRIC_DEFAULT", keyUsage: "TRANSMOGRIFY"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := arnGuardServer(t)

			meta := kmsCreateKeyMetadata(t, ts, map[string]any{
				"KeySpec": tc.keySpec, "KeyUsage": tc.keyUsage,
			})
			for _, member := range kmsAlgorithmMembers {
				assert.NotContains(t, meta, member,
					"%s with KeyUsage %s reports no %s", tc.keySpec, tc.keyUsage, member)
			}
			assert.Equal(t, tc.keyUsage, kmsMetadataString(t, meta, "KeyUsage"),
				"the usage the key was created with is still reported")
		})
	}
}

// TestKMSKeyMetadata_TheUnreachableMembersStayAbsent asserts the nine members substrate does not report,
// and why each would need something substrate does not model.
//
// This is the inverse of the rest of the file and it is deliberate: #974 adds sixteen members from a page
// that publishes twenty-six, and without this test the remaining ten would be indistinguishable from an
// oversight. Eight are unreachable — a custom or external key store, an EXTERNAL origin, a replica, the
// PendingReplicaDeletion state, a key material identity (#978) — and the ninth, RotationEnabled, is
// absent because API_KeyMetadata does not publish it, which is #971.
//
// The MultiRegion case is here for MultiRegionConfiguration specifically: AWS publishes it "only when the
// value of the MultiRegion field is True", so a key with the flag set is the one call where reporting it
// would look correct. Substrate models no replica, so it would have to answer a primary with an empty
// ReplicaKeys list — a shape describing a multi-Region key nothing can replicate.
func TestKMSKeyMetadata_TheUnreachableMembersStayAbsent(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{name: "a plain symmetric key", body: map[string]any{}},
		{name: "a multi-Region key", body: map[string]any{"MultiRegion": true}},
		{name: "an asymmetric key", body: map[string]any{"KeySpec": "RSA_2048", "KeyUsage": "SIGN_VERIFY"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := arnGuardServer(t)

			created := kmsCreateKeyMetadata(t, ts, tc.body)
			described := kmsKeyMetadata(t, ts, kmsMetadataString(t, created, "KeyId"))
			for _, member := range kmsUnreachableMetadataMembers {
				assert.NotContains(t, created, member, "CreateKey reports no %s", member)
				assert.NotContains(t, described, member, "DescribeKey reports no %s", member)
			}
		})
	}
}

// TestKMSKeyMetadata_DeletionDateAppearsOnlyOnceTheKeyIsPendingDeletion pins the one conditional member
// the two operations had already drifted apart on.
//
// #963 added it to DescribeKey's map and CreateKey's map did not have it, which was invisible while the
// two were separate: a key is Enabled the instant it is created, so CreateKey never had an opportunity to
// report it and no test could tell whether it would. Sharing the builder settles that by construction,
// and this asserts the condition AWS publishes still holds on both sides — "this value is present only
// when the KMS key is scheduled for deletion, that is, when its KeyState is PendingDeletion".
func TestKMSKeyMetadata_DeletionDateAppearsOnlyOnceTheKeyIsPendingDeletion(t *testing.T) {
	t.Parallel()
	ts := arnGuardServer(t)

	created := kmsCreateKeyMetadata(t, ts, map[string]any{})
	keyID := kmsMetadataString(t, created, "KeyId")
	assert.NotContains(t, created, "DeletionDate", "a key is Enabled the instant it is created")
	assert.NotContains(t, kmsKeyMetadata(t, ts, keyID), "DeletionDate",
		"DescribeKey reports no deletion date for an enabled key")

	kmsScheduleDeletion(t, ts, keyID, 0)
	meta := kmsKeyMetadata(t, ts, keyID)
	assert.Equal(t, "PendingDeletion", kmsMetadataString(t, meta, "KeyState"))
	assert.Contains(t, meta, "DeletionDate", "a key pending deletion reports when it is due to go")
	assert.NotContains(t, meta, "PendingDeletionWindowInDays",
		"the adjacent member belongs to PendingReplicaDeletion and has a different range")
}

// TestKMSKeyMetadata_TheReportedAccountIsTheKeysOwnAccount checks AWSAccountId against a second account
// rather than only against the one every other KMS test uses.
//
// The member is glossed "the twelve-digit account ID of the AWS account that owns the KMS key", and the
// failure worth excluding is a builder that reports the *caller's* account: with one account in play the
// two are the same string, so a cross-account read is the only call that separates them. #737 established
// that an IAM entity belongs to an account rather than to the emulator, and this is the same distinction
// aimed at a KMS response member.
func TestKMSKeyMetadata_TheReportedAccountIsTheKeysOwnAccount(t *testing.T) {
	t.Parallel()
	const otherAccount = "210987654321"
	ts := emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount, otherAccount))

	var created struct {
		KeyMetadata struct {
			Arn string `json:"Arn"`
		} `json:"KeyMetadata"`
	}
	status, code := decodeAWSResponse(t,
		signedRequest(t, ts, kmsTarget, otherAccount, "CreateKey", map[string]any{}), &created)
	require.Empty(t, code)
	require.Equal(t, http.StatusOK, status)

	meta := kmsKeyMetadata(t, ts, created.KeyMetadata.Arn)
	assert.Equal(t, otherAccount, kmsMetadataString(t, meta, "AWSAccountId"),
		"the account that owns the key, not the account that asked about it")
	assert.Equal(t, created.KeyMetadata.Arn, kmsMetadataString(t, meta, "Arn"),
		"and the ARN names the same account")
}
