package emulator

import "slices"

// KMS encryption algorithms: which algorithm a cryptographic operation used, and which ones a key
// admits.
//
// Four operations — Encrypt, Decrypt and ReEncrypt twice over — take an encryption algorithm and
// report one back, and until #969 substrate decoded none of them and answered none of them. The
// absence was defensible while it lasted: substrate models no cryptography, so echoing
// SYMMETRIC_DEFAULT unconditionally would have reported a value derived from nothing, which is the
// defect class two whole releases were spent on.
//
// What makes it derivable is that AWS does not let a key choose its algorithm. The developer guide's
// key spec reference is explicit — "you cannot configure a KMS key to use a particular encryption
// algorithm" — and instead fixes the admissible set per key spec. So the algorithm a request may use
// is a function of the key it names plus the member the caller sent, and both are things substrate
// holds. The value in a response is then traceable to the request and to the key, not invented.
//
// Substrate performs no encryption either way. What it models is the *refusal*: an algorithm the key
// cannot use is rejected before any stub ciphertext is produced, which is the observation a consumer's
// error path is written against.
//
// One consequence of following AWS's default rather than the key is worth naming, because it looks
// like an oversight and is not. AWS states the default unconditionally — "the default value,
// SYMMETRIC_DEFAULT, is the algorithm used for symmetric encryption KMS keys" — and separately says
// the member "is required only for asymmetric KMS keys". Defaulting to SYMMETRIC_DEFAULT and then
// checking it against the key spec produces that requirement for free: an omitted algorithm on an RSA
// key defaults to a value RSA does not admit and is refused, so the member is required exactly where
// AWS says it is, with no separate required-ness branch to keep in step.

// kmsSymmetricDefaultAlgorithm is the encryption algorithm AWS applies when a caller sends none.
//
// Named because it is both the default and a member of the published set below, and the two uses would
// otherwise be one string literal doing two jobs.
const kmsSymmetricDefaultAlgorithm = "SYMMETRIC_DEFAULT"

// kmsEncryptionAlgorithms is the published set of encryption algorithms, in the order AWS lists them.
//
// API_Encrypt, API_Decrypt and both of API_ReEncrypt's algorithm members carry the identical Valid
// Values line — SYMMETRIC_DEFAULT | RSAES_OAEP_SHA_1 | RSAES_OAEP_SHA_256 | SM2PKE — so one set serves
// all four. A value outside it is a malformed member rather than an incompatible algorithm, and
// [kmsUnknownEncryptionAlgorithm] answers it with a different code for that reason.
var kmsEncryptionAlgorithms = []string{
	kmsSymmetricDefaultAlgorithm,
	"RSAES_OAEP_SHA_1",
	"RSAES_OAEP_SHA_256",
	"SM2PKE",
}

// kmsEncryptionAlgorithmsByKeySpec is the encryption algorithm each key spec admits.
//
// Verified against the developer guide's key spec reference. SYMMETRIC_DEFAULT admits only itself —
// API_Decrypt calls it "the only supported algorithm that is valid for symmetric encryption KMS keys".
// All three RSA specs admit the same two RSAES_OAEP algorithms, which differ only in the hash they use
// internally and are per-request rather than per-key. SM2, a China-Regions-only spec, admits SM2PKE.
//
// Every other key spec AWS publishes is absent deliberately, not by omission: the ECC specs, the four
// HMAC specs and the three ML-DSA specs support signing, MAC generation or key agreement and **no
// encryption algorithm at all**. A missing entry and an empty entry therefore mean the same thing here
// — no encryption algorithm is admissible — so the lookup needs no "is this spec known" branch, and a
// key spec substrate has never heard of lands in the same place. That last case is reachable only
// because CreateKey validates KeySpec against nothing (#977), which is its own defect; when it is
// fixed this map becomes total over the specs a key can carry.
var kmsEncryptionAlgorithmsByKeySpec = map[string][]string{
	kmsSymmetricDefaultKeySpec: {kmsSymmetricDefaultAlgorithm},
	"RSA_2048":                 {"RSAES_OAEP_SHA_1", "RSAES_OAEP_SHA_256"},
	"RSA_3072":                 {"RSAES_OAEP_SHA_1", "RSAES_OAEP_SHA_256"},
	"RSA_4096":                 {"RSAES_OAEP_SHA_1", "RSAES_OAEP_SHA_256"},
	"SM2":                      {"SM2PKE"},
}

// kmsResolveEncryptionAlgorithm defaults an absent encryption algorithm and refuses one outside the
// published set.
//
// It answers the value the operation reports back, so a caller that sent an algorithm reads its own
// value and a caller that sent none reads the default AWS documents — never a value derived from
// nothing. The member name is passed in because the four call sites send three different ones
// (EncryptionAlgorithm, SourceEncryptionAlgorithm, DestinationEncryptionAlgorithm) and a caller with a
// bad value on a ReEncrypt needs to know which end of the operation it belongs to.
//
// It takes no key, and that is the point of it being separate from
// [kmsCheckEncryptionAlgorithmForKey]: a value outside the enum is a malformed request that AWS's
// protocol layer refuses before any key is looked up, so a caller who both misspells the algorithm and
// names a nonexistent key learns about the misspelling. That ordering is the one #964 established for a
// number out of range, and the two halves are split here so each call site can put them where they
// belong rather than performing both at whichever point the key happens to be loaded.
func kmsResolveEncryptionAlgorithm(member, requested string) (string, *AWSError) {
	algorithm := requested
	if algorithm == "" {
		algorithm = kmsSymmetricDefaultAlgorithm
	}
	if !slices.Contains(kmsEncryptionAlgorithms, algorithm) {
		return "", kmsUnknownEncryptionAlgorithm(member, algorithm)
	}
	return algorithm, nil
}

// kmsCheckEncryptionAlgorithmForKey refuses a published encryption algorithm that the key cannot use.
//
// The algorithm must already have been through [kmsResolveEncryptionAlgorithm], which is what makes
// the two refusals distinguishable: by the time this runs the value is one AWS publishes, so a failure
// here is a statement about the *key* rather than about the member. That is what earns
// InvalidKeyUsageException, whose second gloss bullet is exactly "the encryption algorithm or signing
// algorithm specified for the operation is incompatible with the type of key material in the KMS key
// (KeySpec)" — a published code for a published condition, unlike the ValidationError its sibling
// answers on substrate's reading.
//
// It runs after the key-state check at every call site, because a key state is a condition of the key
// that AWS's own table orders first, and a caller holding a key pending deletion needs to hear about
// the deletion rather than about an algorithm it will not get to use.
func kmsCheckEncryptionAlgorithmForKey(key *KMSKey, member, algorithm string) *AWSError {
	if !slices.Contains(kmsEncryptionAlgorithmsByKeySpec[key.KeySpec], algorithm) {
		return kmsIncompatibleEncryptionAlgorithm(key, member, algorithm)
	}
	return nil
}
