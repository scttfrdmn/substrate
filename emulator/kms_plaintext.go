package emulator

import (
	"encoding/base64"
	"fmt"
	"net/http"
)

// KMS plaintext size: the published length range on Encrypt's Plaintext member, and the smaller
// maxima a key spec and encryption algorithm impose on it (#991).
//
// Encrypt's Plaintext member carries two constraints, at two different levels, and until #991
// substrate enforced neither. The member's own Length Constraints are "Minimum length of 1. Maximum
// length of 4096", which is key-independent — every request violating it is malformed whatever key
// it names, and AWS's protocol layer refuses it before looking one up. Below that, the page states
// per-key maxima that are smaller by an order of magnitude and depend on the key spec *and* the
// encryption algorithm together: "the maximum size of the data that you can encrypt varies with the
// type of KMS key and the encryption algorithm that you choose."
//
// **The two are separate refusals and they belong at different points in the handler**, which is
// the finding that made #991 more than a code correction. The range and the base64 decode are
// facts about the request; the per-key maximum is a fact about the request *and* the key, so it
// cannot be answered until the key is loaded and the algorithm resolved. Substrate had the whole
// check in one place — after the key, the key usage, the key state and the algorithm — and answered
// a code the page does not publish, so a caller sending 5 KB of data to a key that does not exist
// heard about the key, and a caller sending unusable base64 heard InvalidCiphertextException on an
// operation that produces ciphertext rather than consuming it. This is the split
// [kmsResolveEncryptionAlgorithm] and [kmsCheckEncryptionAlgorithmForKey] already make for the
// algorithm member, applied to the one other member of this operation that has anything to check.
//
// Neither refusal has a published code. API_Encrypt lists nine errors and not one of them is about
// a member being out of range: the closest is InvalidKeyUsageException, whose two gloss bullets are
// about a KeyUsage and about an algorithm "incompatible with the type of key material in the KMS key
// (KeySpec)". Both refusals here are therefore ValidationError at 400 from CommonErrors.html, the
// reading [kmsInvalidPendingWindow] records and [kmsUnknownEncryptionAlgorithm] reached the same way.
// InvalidKeyUsageException is deliberately not reused for the per-key maximum even though that
// condition does involve the key spec and the algorithm: nothing about the pairing is incompatible —
// the key admits the algorithm, and a shorter plaintext would succeed — so a caller reading that code
// would change its algorithm when what it must change is how much data it sends in one call.

// kmsPlaintextMinBytes and kmsPlaintextMaxBytes are the published length range of Encrypt's
// Plaintext member, in bytes of decoded data.
//
// API_Encrypt states them on the member as "Length Constraints: Minimum length of 1. Maximum length
// of 4096" and repeats the maximum in its opening sentence — "encrypts plaintext of up to 4,096
// bytes using a KMS key" — which is what settles that the constraint counts *decoded* bytes rather
// than base64 characters: the member's type is a blob, and the encoding is the transport.
//
// The minimum is as load-bearing as the maximum and less obvious: an empty Plaintext is a malformed
// request rather than an encryption of nothing, and Required: Yes does not cover it, since a member
// present and empty satisfies presence.
const (
	kmsPlaintextMinBytes = 1
	kmsPlaintextMaxBytes = 4096
)

// kmsPlaintextMaxByKeySpec is the maximum plaintext each key spec accepts under each encryption
// algorithm, in bytes.
//
// Published on API_Encrypt itself rather than in the developer guide, as the list under "the maximum
// size of the data that you can encrypt varies with the type of KMS key and the encryption algorithm
// that you choose". The numbers are RSA's OAEP padding overhead made observable: the same key spec
// accepts 24 fewer bytes under RSAES_OAEP_SHA_256 than under RSAES_OAEP_SHA_1, because the padding
// carries a longer hash.
//
// **The key is (key spec, algorithm) although AWS's list mixes the two levels**, and the mixture is
// worth naming because it looks like a transcription error here. AWS heads its RSA entries with a
// key spec and its last entry with an *algorithm*, SM2PKE, while heading the symmetric entry with
// both ("Symmetric encryption KMS keys" then SYMMETRIC_DEFAULT). Keying on the pair loses nothing:
// [kmsEncryptionAlgorithmsByKeySpec] admits exactly one algorithm for SYMMETRIC_DEFAULT and exactly
// one for SM2, so those two rows are total either way, and only the RSA specs genuinely need both
// coordinates.
//
// A spec absent from this map, or an algorithm absent from its row, cannot reach
// [kmsPlaintextSizeError] at all: every call site runs [kmsCheckEncryptionAlgorithmForKey] first,
// and that helper admits only the pairs [kmsEncryptionAlgorithmsByKeySpec] lists — which are exactly
// this map's keys. The lookup therefore needs no fallback, and a missing entry would be a
// disagreement between two tables rather than a request substrate has to answer; a test asserts the
// two agree.
var kmsPlaintextMaxByKeySpec = map[string]map[string]int{
	kmsSymmetricDefaultKeySpec: {kmsSymmetricDefaultAlgorithm: kmsPlaintextMaxBytes},
	"RSA_2048":                 {"RSAES_OAEP_SHA_1": 214, "RSAES_OAEP_SHA_256": 190},
	"RSA_3072":                 {"RSAES_OAEP_SHA_1": 342, "RSAES_OAEP_SHA_256": 318},
	"RSA_4096":                 {"RSAES_OAEP_SHA_1": 470, "RSAES_OAEP_SHA_256": 446},
	"SM2":                      {"SM2PKE": 1024},
}

// kmsDecodePlaintext decodes Encrypt's Plaintext member and refuses it for the reasons that do not
// depend on which key the request names.
//
// Two conditions, one code: a value that is not base64 at all, and a decoded length outside the
// published 1-4096. Both are ValidationError at 400 for the reason the file's preamble records, and
// both are answered before the key is resolved, so a request that is malformed and also names a key
// that does not exist hears about the member it can fix on its own.
//
// The member name is a parameter because Encrypt is the only operation that reaches here today and
// that is not something to bake in: Sign, GenerateMac and VerifyMac each publish a Message member
// with a length range of its own, and a later issue extending this to them must not have to rewrite
// the message. It is not applied to Decrypt's or ReEncrypt's CiphertextBlob, whose undecodable case
// is what InvalidCiphertextException is actually published for.
//
// The message names the range rather than the value's length alone, since a caller that has just
// been told "4097 is too long" still has to find the limit somewhere.
func kmsDecodePlaintext(member, raw string) ([]byte, *AWSError) {
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, &AWSError{
			Code:       "ValidationError",
			Message:    fmt.Sprintf("%s is not valid base64-encoded data", member),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if len(decoded) < kmsPlaintextMinBytes || len(decoded) > kmsPlaintextMaxBytes {
		return nil, &AWSError{
			Code: "ValidationError",
			Message: fmt.Sprintf(
				"%s is %d bytes, which is outside the supported range of %d to %d",
				member, len(decoded), kmsPlaintextMinBytes, kmsPlaintextMaxBytes),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	return decoded, nil
}

// kmsPlaintextSizeError refuses a plaintext longer than the named key admits under the resolved
// encryption algorithm, and reports nil when it fits.
//
// This is the half of the Plaintext constraint that cannot be answered before the key is loaded, and
// it is where the whole point of the split shows: 300 bytes is a perfectly good Plaintext by the
// member's own 1-4096 range and is 110 bytes too long for an RSA_2048 key under
// RSAES_OAEP_SHA_256. A consumer that encrypts a database password under a symmetric key and later
// switches to an asymmetric one crosses this boundary without changing its request shape at all,
// which is exactly the divergence an emulator that accepted everything hid.
//
// The message names all four things the caller needs to act — the key spec, the algorithm, the size
// it sent and the maximum — because no two of them determine the answer: the same plaintext is
// accepted or refused depending on the algorithm, and the same algorithm gives three different
// maxima across the RSA specs.
//
// The lookup is total over what can reach it, per [kmsPlaintextMaxByKeySpec]: an unlisted pair has
// already been refused by [kmsCheckEncryptionAlgorithmForKey]. A zero from the map is therefore not
// treated as "no limit" — it cannot occur — and is not special-cased, because a special case would
// silently absorb a future disagreement between the two tables that the assertion on them is there
// to surface.
func kmsPlaintextSizeError(key *KMSKey, algorithm string, plaintext []byte) *AWSError {
	maximum, ok := kmsPlaintextMaxByKeySpec[key.KeySpec][algorithm]
	if !ok || len(plaintext) <= maximum {
		return nil
	}
	return &AWSError{
		Code: "ValidationError",
		Message: fmt.Sprintf(
			"Plaintext is %d bytes, and a %s key using %s encrypts at most %d bytes",
			len(plaintext), key.KeySpec, algorithm, maximum),
		HTTPStatus: http.StatusBadRequest,
	}
}
