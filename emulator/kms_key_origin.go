package emulator

import "slices"

// KMS key material origin: which of API_CreateKey's four Origin values substrate accepts, and why the
// other three are refused rather than stored.
//
// Until #984 CreateKey decoded none of Origin, CustomKeyStoreId or XksKeyId. All three were accepted and
// discarded, so `CreateKey` with `Origin: "EXTERNAL"` answered 200 with `Origin: "AWS_KMS"`,
// `KeyState: "Enabled"` and `Enabled: true` — a fully usable key where AWS answers one in PendingImport
// that no cryptographic operation will touch until ImportKeyMaterial runs. A consumer's import workflow
// therefore passed at step one and failed at step two, which is #765's failure mode spread across three
// calls: the emulator agreed with a request it did not understand.
//
// # Why a refusal rather than a model
//
// Substrate implements neither ImportKeyMaterial nor any custom key store, and per CLAUDE.md's boundary
// that is defensible: imported key material and an HSM cluster's contents are resource-internal, not
// observable through an API call. But the three *request parameters* are observable, and accepting one
// and discarding it is the worst of the three available answers — worse than modeling it and worse than
// refusing it, because it is the only one a caller cannot detect. So the parameters are decoded and what
// substrate does not model is refused with a code API_CreateKey publishes. Modeling Origin: EXTERNAL as
// a PendingImport key state, and modeling a custom key store, remain available and are each larger than
// this issue; a refusal now does not foreclose either, and it makes three of #974's absent KeyMetadata
// members unreachable **by construction** rather than by omission — see [kmsKeyOriginAWSKMS].
//
// # Two refusals, two codes
//
// The split is the same one [kmsUnknownKeySpec] and [kmsInadmissibleKeyUsage] already draw for the
// adjacent pair, and it is load-bearing here for a sharper reason: the two refusals answer different
// questions about whether the caller did anything wrong.
//
//   - **A refusal AWS itself would make.** An Origin outside the published four is a malformed member,
//     refused with ValidationError from CommonErrors — the reading #977 recorded for a misspelled
//     KeySpec. And an XksKeyId sent with any Origin other than EXTERNAL_KEY_STORE is refused on AWS's
//     own sentence, "it is not valid for KMS keys with any other Origin value", which holds against real
//     KMS and says nothing about what substrate models. A caller reading either of these has a request
//     AWS would also reject.
//   - **A refusal substrate makes because it models nothing behind the parameter.** A published Origin
//     that is not AWS_KMS, and any CustomKeyStoreId, are refused with UnsupportedOperationException —
//     published on API_CreateKey and glossed "the request was rejected because a specified parameter is
//     not supported or a specified resource is not valid for this operation". A caller reading one of
//     these sent a request real KMS would honor, and needs to know that this is a boundary of the
//     emulator rather than a defect in their code.
//
// Collapsing the two into one code would leave a consumer unable to tell a typo from a scope boundary,
// which is the distinction the whole seed-and-refuse model rests on: one is fixed by editing the
// request, the other by not testing that path here.
//
// # What becomes unreachable, and is therefore not implemented
//
// AWS publishes constraints on both store parameters — CustomKeyStoreId is length 1–64, XksKeyId is
// length 1–128 against `^[a-zA-Z0-9-_.]+$` — and no request can reach either, because any non-empty
// value is refused before a length is measured. They are recorded here rather than implemented, for the
// reason #971 gives about the opposite direction: a constraint no request can reach is not enforcement,
// and writing one invites a later reader to assume the parameter is modeled. The same holds for the
// spec-dependent conditions on Origin itself — "the EXTERNAL origin value is valid only for symmetric
// KMS keys", and the SYMMETRIC_DEFAULT requirement for AWS_CLOUDHSM and EXTERNAL_KEY_STORE — since
// every Origin those sentences constrain is already refused. [kmsResolveKeyOrigin] is nevertheless
// ordered after the key spec resolves, so that whichever of those conditions is ever modeled has a
// resolved spec to read rather than needing the call moved.
//
// The five errors API_CreateKey publishes for these paths and substrate does not construct —
// CustomKeyStoreNotFoundException, CustomKeyStoreInvalidStateException,
// CloudHsmClusterInvalidConfigurationException, XksKeyAlreadyInUseException, XksKeyNotFoundException —
// are unreachable for the same reason: each presupposes a store or an external key that would have to be
// modeled first. XksKeyInvalidConfigurationException is the near miss and is deliberately not used for
// the XksKeyId refusal below, because its gloss is about an external key's *configuration* in a store
// substrate has none of, where the refusal is about the parameter not belonging in the request at all.

// kmsKeyOrigins is the published set of key material origins, in the order API_CreateKey lists them.
//
// Only [kmsKeyOriginAWSKMS] is accepted; the set exists so that a misspelling can be told apart from a
// value substrate does not model, and so the refusal message can name what a caller should have sent.
var kmsKeyOrigins = []string{
	kmsKeyOriginAWSKMS,
	"EXTERNAL",
	"AWS_CLOUDHSM",
	"EXTERNAL_KEY_STORE",
}

// kmsKeyOriginExternalKeyStore is the one origin for which AWS publishes XksKeyId as valid.
//
// Named rather than written inline because it appears in the one condition AWS states about that member —
// "this parameter is required for a KMS key with an Origin value of EXTERNAL_KEY_STORE. It is not valid
// for KMS keys with any other Origin value" — and both halves of that sentence are read below.
const kmsKeyOriginExternalKeyStore = "EXTERNAL_KEY_STORE"

// kmsResolveKeyOrigin reports the refusal a CreateKey request's key-material parameters owe the caller,
// or nil when the request asks for the only origin substrate creates.
//
// The default is AWS's own — "the default is AWS_KMS, which means that AWS KMS creates the key material"
// — so an absent Origin is the accepted case and a CreateKey with an empty body still creates the key
// the page's first guidance section describes.
//
// The order of the four checks is deliberate and decides which of two codes a caller reads:
//
//  1. An unpublished Origin is a spelling problem and is answered as one, before anything else is
//     considered.
//  2. An XksKeyId is checked against the *requested* origin next, so that a caller sending it with
//     AWS_KMS reads AWS's own "not valid for any other Origin value" refusal rather than a message about
//     what substrate models. It is checked before the origin's own support so the one request that
//     satisfies AWS's condition — EXTERNAL_KEY_STORE with an XksKeyId — falls through to step 3 and is
//     told the truth about the store instead.
//  3. A published origin substrate does not create is the scope refusal.
//  4. A CustomKeyStoreId is last, because by here the origin is AWS_KMS, for which AWS states the
//     parameter is not used at all: "this parameter is valid only for symmetric encryption KMS keys in a
//     single Region. You cannot create any other type of KMS key in a custom key store." Substrate has
//     no store to place even that key in, so the refusal is the scope one rather than AWS's.
//
// It returns only an error. Nothing is resolved onto the key, because there is nothing to store: every
// key substrate creates has origin AWS_KMS, which is what lets [kmsKeyOriginAWSKMS] stay a constant.
func kmsResolveKeyOrigin(origin, customKeyStoreID, xksKeyID string) *AWSError {
	if origin == "" {
		origin = kmsKeyOriginAWSKMS
	}
	if !slices.Contains(kmsKeyOrigins, origin) {
		return kmsUnknownKeyOrigin(origin)
	}
	if xksKeyID != "" && origin != kmsKeyOriginExternalKeyStore {
		return kmsXksKeyIDNotValidForOrigin(origin)
	}
	if origin != kmsKeyOriginAWSKMS {
		return kmsUnsupportedKeyOrigin(origin)
	}
	if customKeyStoreID != "" {
		return kmsUnsupportedCustomKeyStore(customKeyStoreID)
	}
	return nil
}
