package emulator

import (
	"slices"
	"strings"
)

// KMS key specs and key usages: which values CreateKey accepts, which pairs of them describe a key AWS
// would create, and which key usage a cryptographic operation requires.
//
// Until #977 substrate validated none of this. CreateKey decoded both members, defaulted each to its
// symmetric value and stored whatever string it was handed, so `KeySpec: "MADEUP"` answered 200 and
// DescribeKey reported it back. Three separate defects sat behind that one missing check, and they are
// separate because a caller can tell them apart:
//
//   - **A value outside the published enum.** A misspelling, or a spec from a newer API version than the
//     caller's SDK. Refused with ValidationError, for the reason kms_errors.go's preamble gives: no
//     operation-specific code on API_CreateKey describes a malformed member, so it comes from
//     CommonErrors.html. Substrate's reading, and the same one #969 took for a misspelled algorithm.
//   - **A missing KeyUsage where AWS requires one.** KeyUsage's own description is "this parameter is
//     optional when you are creating a symmetric encryption KMS key; otherwise, it is required", and the
//     operation's HMAC guidance says it twice over: "you must set the key usage even though
//     GENERATE_VERIFY_MAC is the only valid key usage value for HMAC KMS keys". Substrate's unconditional
//     ENCRYPT_DECRYPT default therefore silently turned an HMAC or ML-DSA request into an encryption key.
//     Also ValidationError, because a required parameter that is absent is a defect of the request.
//   - **A well-formed pair AWS would never create.** HMAC_256 with KEY_AGREEMENT, say. Refused with
//     UnsupportedOperationException, published on API_CreateKey and glossed "a specified parameter is not
//     supported or a specified resource is not valid for this operation" — the only published 400 on the
//     operation that describes an inadmissible parameter. Substrate's reading too: AWS states the pairing
//     rules and attaches no code to them.
//
// The distinction between the first and the third is the one worth keeping. One says *that is not a key
// spec*; the other says *that is a key spec, and not with that usage*. Collapsing them would leave a
// caller unable to tell a typo from a misunderstanding.
//
// # The pairing table is derived, not written
//
// AWS publishes the rules as seven bullets under KeyUsage, and they turn out to say exactly what the four
// per-key-spec algorithm tables in kms_key_metadata.go already say: a key spec admits a key usage if and
// only if it has at least one algorithm for it. Symmetric encryption and RSA admit ENCRYPT_DECRYPT and
// have encryption algorithms; the NIST curves and SM2 admit KEY_AGREEMENT and have ECDH; the HMAC specs
// admit GENERATE_VERIFY_MAC alone and have exactly one MAC algorithm each; ECC_NIST_EDWARDS25519 is
// "signing and verification only" and appears in the signing table and nowhere else.
//
// So [kmsKeySpecAdmitsKeyUsage] reads those tables rather than a fifth table of its own. That is a
// deliberate choice against the defect class #952 records and #974 hit inside this very plugin: two
// hand-written structures describing one published fact drift, and the drift is invisible to any test
// that checks each against the page instead of against the other. A pairing table written out here could
// disagree with the algorithm lists; a derivation cannot.
//
// It also settles something #974 left as defense-in-depth. That issue had to decide what KeyMetadata
// reports for a key whose usage admits no algorithm, and chose an absent member over an empty array
// because #977's gap let such a key exist. With the gap closed the case is unreachable through CreateKey
// — every key it accepts has a non-empty algorithm list, by the same equivalence this table rests on —
// and [kmsPutAlgorithms]' empty-list branch becomes a guard rather than a path.
//
// # KeyUsage and KeySpec at the cryptographic operations
//
// InvalidKeyUsageException's gloss has two bullets. #969 implemented the second, an algorithm the key's
// spec does not admit. [kmsKeyUsageError] is the first — "for encrypting, decrypting, re-encrypting, and
// generating data keys, the KeyUsage must be ENCRYPT_DECRYPT" — and it is not covered by the second, which
// is why the two are separate issues: RSA_2048 admits RSAES_OAEP_SHA_256 whatever the key's usage is, so
// an Encrypt against an RSA *signing* key passed every check substrate had.
//
// [kmsDataKeyKeySpecError] is #988, and it is a third thing that code answers, at two of the five
// operations only. The two GenerateDataKey* operations require a symmetric *encryption* key, so a key that
// passes both bullets — ENCRYPT_DECRYPT usage, no algorithm member to be incompatible with — is still
// refused there for its spec alone. That the same code carries all three is AWS's design and not a
// collision; what it costs is that a caller cannot tell them apart by code, which is why each of the three
// messages leads with the member the refusal is about.

// kmsKeySpecs is the published set of key specs, in the order API_CreateKey lists them.
//
// Seventeen values, four more than the deprecated CustomerMasterKeySpec member's own enum carries — the
// difference #974 records on the response side and #985 on the request side.
var kmsKeySpecs = []string{
	"RSA_2048", "RSA_3072", "RSA_4096",
	"ECC_NIST_P256", "ECC_NIST_P384", "ECC_NIST_P521", "ECC_SECG_P256K1",
	kmsSymmetricDefaultKeySpec,
	"HMAC_224", "HMAC_256", "HMAC_384", "HMAC_512",
	"SM2",
	"ML_DSA_44", "ML_DSA_65", "ML_DSA_87",
	"ECC_NIST_EDWARDS25519",
}

// kmsKeyUsages is the published set of key usages, in the order API_CreateKey lists them.
//
// The order is load-bearing rather than cosmetic: [kmsKeyUsagesForKeySpec] walks this slice to build the
// admissible set it names in a refusal, so the message a caller reads is AWS's order and is the same on
// every run. Ranging a map for it would make a refusal message vary between runs, which is the
// determinism failure #862 and the ordering work before it exist to prevent.
var kmsKeyUsages = []string{
	kmsKeyUsageSignVerify,
	kmsKeyUsageEncryptDecrypt,
	kmsKeyUsageGenerateVerifyMAC,
	kmsKeyUsageKeyAgreement,
}

// kmsKeySpecAdmitsKeyUsage reports whether AWS would create a key with this spec and this usage.
//
// The test is that the usage's algorithm table has a non-empty entry for the spec, which is the same fact
// AWS's seven pairing bullets state — see this file's preamble for why the equivalence holds and why it is
// read from those tables rather than written out again here. A usage outside the published four has no
// table and so admits nothing, which is correct but never reached: [kmsResolveKeySpecAndUsage] refuses an
// unpublished usage first, with a different code.
func kmsKeySpecAdmitsKeyUsage(keySpec, keyUsage string) bool {
	algorithms, ok := kmsKeyUsageAlgorithms[keyUsage]
	if !ok {
		return false
	}
	return len(algorithms.byKeySpec[keySpec]) > 0
}

// kmsKeyUsagesForKeySpec returns the key usages a key spec admits, in AWS's published order.
//
// Used only to compose refusal messages, and it exists because a bare refusal is unactionable: a caller
// that sent ECC_NIST_P256 with ENCRYPT_DECRYPT needs to be told the spec signs or derives, not merely
// that its request was wrong. An unrecognized spec yields an empty slice, and the two callers each say so
// in words rather than printing an empty list.
func kmsKeyUsagesForKeySpec(keySpec string) []string {
	usages := make([]string, 0, len(kmsKeyUsages))
	for _, usage := range kmsKeyUsages {
		if kmsKeySpecAdmitsKeyUsage(keySpec, usage) {
			usages = append(usages, usage)
		}
	}
	return usages
}

// kmsAdmissibleKeyUsageList renders a key spec's admissible usages for a refusal message.
//
// Shared by the two refusals that name the set so they cannot describe one key spec two ways. The empty
// case is reachable only for a spec [kmsKeySpecs] does not carry, which [kmsResolveKeySpecAndUsage]
// refuses before either caller runs — so it is a guard, and it says "no key usage" rather than nothing at
// all for the reason [kmsIncompatibleEncryptionAlgorithm] does the same: a caller reading an empty list
// cannot tell it from a rendering bug.
func kmsAdmissibleKeyUsageList(keySpec string) string {
	usages := kmsKeyUsagesForKeySpec(keySpec)
	if len(usages) == 0 {
		return "no key usage"
	}
	return strings.Join(usages, ", ")
}

// kmsResolveKeySpecAndUsage defaults, validates and pairs CreateKey's KeySpec and KeyUsage.
//
// It answers the two values the key is created with, so a key's stored spec and usage are ones AWS
// publishes and a pair AWS would accept — which is what lets every table keyed by them be total over the
// keys substrate can hold, [kmsEncryptionAlgorithmsByKeySpec] included.
//
// The order of the four checks is not arbitrary. KeySpec is validated first because it decides both of
// the questions that follow — whether KeyUsage may be omitted, and which usages are admissible — so a
// request wrong in both members is told about the spec, which is the one that makes sense of the other.
// Then KeyUsage's required-ness, before its enum, because an absent member and a misspelled one are
// different failures and "" is not a misspelling. Then the enum. Then the pair, which is the only check
// that needs both values to be individually valid, and the only one that answers a different code.
//
// The default for each member is AWS's own and is applied where AWS applies it. KeySpec defaults to
// SYMMETRIC_DEFAULT unconditionally, as the page's "the default value, SYMMETRIC_DEFAULT" states. KeyUsage
// defaults to ENCRYPT_DECRYPT **only for that spec**, which is the narrow reading of "this parameter is
// optional when you are creating a symmetric encryption KMS key; otherwise, it is required" — an HMAC key
// is symmetric but is not a symmetric *encryption* key, and AWS closes that gap explicitly: "you must set
// the key usage even though GENERATE_VERIFY_MAC is the only valid key usage value for HMAC KMS keys". A
// defaulted SYMMETRIC_DEFAULT counts, so a CreateKey with an empty body still creates the key the page's
// first guidance section describes.
//
// Both values are permanent once the key exists — "you can't change the KeySpec after the KMS key is
// created" and "you can't change the KeyUsage value after the KMS key is created" — which is why this runs
// before anything is written and why no other operation revalidates. It is also the reason
// [kmsKeyUsageError] is ordered ahead of a key-state check at its call sites.
func kmsResolveKeySpecAndUsage(keySpec, keyUsage string) (string, string, *AWSError) {
	if keySpec == "" {
		keySpec = kmsSymmetricDefaultKeySpec
	}
	if !slices.Contains(kmsKeySpecs, keySpec) {
		return "", "", kmsUnknownKeySpec(keySpec)
	}
	if keyUsage == "" {
		if keySpec != kmsSymmetricDefaultKeySpec {
			return "", "", kmsKeyUsageRequired(keySpec)
		}
		keyUsage = kmsKeyUsageEncryptDecrypt
	}
	if !slices.Contains(kmsKeyUsages, keyUsage) {
		return "", "", kmsUnknownKeyUsage(keyUsage)
	}
	if !kmsKeySpecAdmitsKeyUsage(keySpec, keyUsage) {
		return "", "", kmsInadmissibleKeyUsage(keySpec, keyUsage)
	}
	return keySpec, keyUsage, nil
}

// kmsResolveRequestKeySpec answers the key spec a CreateKey asked for, from either of the two members that
// can carry it.
//
// CustomerMasterKeySpec is a deprecated *request* parameter as well as a response member, and substrate
// decoded only the response half (#985). So a caller on an SDK old enough to still send the deprecated name
// asked for RSA_4096 and got a symmetric key — the worst shape a failure can take, a 200 with a complete
// KeyMetadata whose own CustomerMasterKeySpec read SYMMETRIC_DEFAULT, contradicting the value it was sent.
// Reading it is not a courtesy: AWS still accepts it — "the KeySpec and CustomerMasterKeySpec fields have
// the same value. We recommend that you use the KeySpec field in your code. However, to avoid breaking
// changes, AWS KMS supports both fields" — so a caller sending it is doing nothing wrong.
//
// Its enum is the narrower one, [kmsCustomerMasterKeySpecs]' thirteen against [kmsKeySpecs]' seventeen, and
// this is where that matters most. A request naming ML_DSA_44 under the deprecated member is refused rather
// than accepted as a key spec, which is the request-side twin of the omission #974 chose on the response
// side: neither direction puts a value outside the member's own published set on the wire. The list is
// shared with the renderer for the reason this file's preamble gives about the pairing table — one
// published fact, one structure.
//
// **Two different values are refused rather than reconciled.** AWS documents no answer for the conflict, so
// this is substrate's reading, and it rests on what AWS does say: the two members "have the same value". A
// request in which they do not is therefore not a request AWS describes, and the alternatives are worse. A
// precedence rule — newer member wins — silently discards half of a contradictory request, which is the
// same silent substitution #985 exists to remove; and resolving by JSON member order would make the answer
// depend on something no caller controls meaningfully.
//
// Equal values are accepted, because that is a caller belting and bracing rather than contradicting itself,
// and it is exactly what AWS's sentence describes. The code is ValidationError, the same one every other
// malformed-member refusal on this operation answers — #977 settled that and this member joins it rather
// than introducing a second code for one class of defect on one operation.
func kmsResolveRequestKeySpec(keySpec, deprecated string) (string, *AWSError) {
	if deprecated == "" {
		return keySpec, nil
	}
	if !slices.Contains(kmsCustomerMasterKeySpecs, deprecated) {
		return "", kmsUnknownCustomerMasterKeySpec(deprecated)
	}
	if keySpec == "" {
		return deprecated, nil
	}
	if keySpec != deprecated {
		return "", kmsConflictingKeySpecMembers(keySpec, deprecated)
	}
	return keySpec, nil
}

// kmsKeyUsageError reports the refusal a cryptographic operation owes a key that is not for encryption,
// or nil when the key's usage permits the call.
//
// The five operations that call it are the five that publish InvalidKeyUsageException — Encrypt, Decrypt,
// ReEncrypt, GenerateDataKey and GenerateDataKeyWithoutPlaintext — and the requirement is the sentence
// under that code's own gloss: "for encrypting, decrypting, re-encrypting, and generating data keys, the
// KeyUsage must be ENCRYPT_DECRYPT". One comparison covers all five, and it has to be shared rather than
// written per site: GenerateDataKeyWithoutPlaintext is the operation #961 found refusing *nothing*, which
// is what a per-site check invites.
//
// **It runs before [kmsKeyStateError]** at every call site, following the precedent
// [kmsRotationKeySpecError] set and for the same reason: a key usage is permanent — AWS states it twice,
// "you can't change the KeyUsage value after the KMS key is created" — while a key state is transient and
// has a remedy. Answering the state first would tell a caller that enabling the key makes the call
// succeed, which for a signing key is false however many times it retries.
//
// It also runs before [kmsCheckEncryptionAlgorithmForKey], and there the ordering decides which of two
// messages a caller reads for one condition rather than which code. Usage first is what makes the answer
// uniform: an ECC signing key and an RSA signing key are both simply not encryption keys, and checking the
// algorithm first would explain the ECC one in terms of its key spec and the RSA one in terms of its
// usage. A consequence worth naming, because it looks like dead code and is one of this issue's own
// results: [kmsIncompatibleEncryptionAlgorithm]'s "no encryption algorithm" branch is now unreachable,
// since every spec with an empty encryption list also fails this check first.
//
// Decrypt is in the set although substrate cannot reach it through its own API — a ciphertext is only
// minted by an operation this refuses — because a stub ciphertext is constructible by hand (see
// [kmsEncryptStub]) and because AWS publishes the code there. It is the same reasoning that keeps
// GetKeyRotationStatus out of [kmsRotationKeySpecError]: the operation's own page decides, not
// reachability.
func kmsKeyUsageError(key *KMSKey) *AWSError {
	if key.KeyUsage != kmsKeyUsageEncryptDecrypt {
		return kmsInvalidKeyUsageForOperation(key)
	}
	return nil
}

// kmsDataKeyKeySpecError reports the refusal GenerateDataKey and GenerateDataKeyWithoutPlaintext owe a key
// that is not a symmetric encryption key, or nil when the key can wrap a data key.
//
// It exists because [kmsKeyUsageError] is not this check and cannot be made into it. That one tests the
// usage, and an RSA_2048 key with KeyUsage ENCRYPT_DECRYPT is a pair AWS publishes and CreateKey must
// accept — so until #988 both operations handed such a caller a wrapped data key and a 200, where AWS
// states the requirement four times over. API_GenerateDataKey's description: "to generate a data key,
// specify the symmetric encryption KMS key that will be used to encrypt the data key. You cannot use an
// asymmetric KMS key to encrypt data keys." Its KeyId gloss, and API_GenerateDataKeyWithoutPlaintext's
// verbatim: "specifies the symmetric encryption KMS key that encrypts the data key. You cannot specify an
// asymmetric KMS key or a KMS key in a custom key store."
//
// The plural in "data **keys**" is load-bearing and is why this is not a condition on Encrypt as well. The
// corresponding sentence there is about *data*, and an RSA encryption key encrypts data perfectly well —
// [kmsCheckEncryptionAlgorithmForKey] is the only thing Encrypt owes such a key. The two operations that
// call this take no EncryptionAlgorithm member at all, which is the observable form of the same rule: AWS
// gives the caller no way to name an asymmetric algorithm because no asymmetric key belongs here.
//
// **Both operations call it, and the helper is shared for that reason rather than for brevity.** #961
// found GenerateDataKeyWithoutPlaintext refusing *nothing* while its four siblings each refused
// something, which is precisely what a per-site check invites; the two pages publish the identical
// restriction and the identical nine errors, so one guard on one of them would be a new instance of that
// defect.
//
// **It runs after [kmsKeyUsageError] and before [kmsKeyStateError]**, and both halves of that are
// deliberate. Usage first, because a request naming a signing key is wrong about the operation rather
// than about the key material, and that refusal is uniform across all five cryptographic operations
// where this one is uniform across two. Key state after, following [kmsRotationKeySpecError]'s argument
// exactly: a key spec is permanent — "you can't change the KeySpec after the KMS key is created" — while
// a key state is transient and has a remedy, so answering the state first would tell a caller that
// enabling the key makes the call succeed, which for an RSA key is false however many times it retries.
//
// [kmsIsSymmetricEncryptionKey] is the predicate rather than a `!isAsymmetric` test, because "symmetric
// encryption key" is narrower than "symmetric key" and the four HMAC specs are the difference. Those four
// are unreachable here — an HMAC key's usage is GENERATE_VERIFY_MAC, so [kmsKeyUsageError] refuses it
// first, and #977 will not pair the spec with ENCRYPT_DECRYPT at all — so sharing the predicate buys no
// behavior here and is still right: it keeps one published condition in one place, which is the argument
// [kmsReportsKeyMaterialID] makes one level up about the same predicate. Four specs are reachable, being
// the four that admit ENCRYPT_DECRYPT: SYMMETRIC_DEFAULT, which passes, and RSA_2048, RSA_3072, RSA_4096
// and SM2, which do not.
//
// The custom-key-store half of AWS's sentence needs no code of its own: since #984 no stored key can have
// an origin other than [kmsKeyOriginAWSKMS], because CreateKey refuses the request that would produce one.
// It would be a second condition here the day a custom key store becomes modelable.
func kmsDataKeyKeySpecError(key *KMSKey) *AWSError {
	if !kmsIsSymmetricEncryptionKey(key) {
		return kmsInvalidKeySpecForDataKey(key)
	}
	return nil
}
