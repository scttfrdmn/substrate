package emulator

import "slices"

// KMS KeyMetadata: the shape CreateKey, DescribeKey and ReplicateKey all answer, and the sixteen
// members substrate withheld.
//
// API_KeyMetadata publishes 26 members and substrate answered 10 of them, plus DeletionDate on the one
// condition the page states for it (#963). Five of the sixteen absent members are ones AWS sends on
// every DescribeKey call for a plain customer symmetric key — AWSAccountId, KeyManager, Origin,
// EncryptionAlgorithms and the deprecated CustomerMasterKeySpec — so a consumer reading any of them got
// a missing key from substrate where AWS always sends a value (#974). That is #765's failure mode aimed
// at a response shape, and it is the opposite direction from #971, which removed the one member
// substrate emitted and AWS does not publish.
//
// Nothing here is a simulation. Three of the five are constants for every key substrate can create and
// the other two are functions of KeySpec and KeyUsage, both of which the key already carries — so every
// member added is a value substrate already knew and withheld, which is the distinction that made #971's
// removal and this addition the same finding read from two sides.
//
// The builder is shared rather than duplicated because AWS shares the shape: the type's own page says it
// "is used as a response element for the CreateKey, DescribeKey, and ReplicateKey operations", and
// substrate had two hand-built maps that already disagreed — DescribeKey emitted DeletionDate and
// CreateKey did not. Two operations disagreeing about one shape is the defect class #952 recorded, so
// the two call sites now build from here. ReplicateKey is the third caller AWS names and substrate does
// not implement it; when it arrives it must build from here too rather than growing a third map.

// kmsKeyManagerCustomer is the only key manager substrate reports.
//
// API_KeyMetadata's Valid Values are AWS | CUSTOMER, and substrate mints no AWS managed key: every key
// in state came from a CreateKey request in the caller's own account. The value is therefore a constant
// rather than a stored field, and it stays one until something can create the other kind — which is also
// why EnableKeyRotation's "you cannot enable or disable automatic rotation of AWS managed KMS keys"
// (#972) is unreachable rather than unenforced.
const kmsKeyManagerCustomer = "CUSTOMER"

// kmsKeyOriginAWSKMS is the only key material origin substrate reports.
//
// AWS glosses it "when this value is AWS_KMS, AWS KMS created the key material", against EXTERNAL for
// imported material and AWS_CLOUDHSM / EXTERNAL_KEY_STORE for a custom key store. Substrate implements
// neither ImportKeyMaterial nor any custom key store, so no request can produce a key with another
// origin. Three further members hang off that: ExpirationModel and ValidTo are published "only when
// Origin is EXTERNAL", and XksKeyConfiguration only for an external key store, so all three are
// unreachable for the same reason rather than three separate omissions.
const kmsKeyOriginAWSKMS = "AWS_KMS"

// The four key usages AWS publishes, which select which algorithm list a key's metadata carries.
//
// API_KeyMetadata gives KeyUsage the Valid Values SIGN_VERIFY | ENCRYPT_DECRYPT | GENERATE_VERIFY_MAC |
// KEY_AGREEMENT, and each of the four algorithm members names one of them as its condition. Named
// because the condition is what makes the members mutually exclusive: a key has one usage, so it carries
// exactly one algorithm list, and the four are not four independent branches.
//
// Since #977 no key can carry a value outside this set: CreateKey refuses one, and it refuses a spec and
// usage AWS would never pair, so every stored key has exactly one non-empty algorithm list.
// [kmsKeyUsages] is the same four as an ordered slice, for the refusals that have to name them.
const (
	kmsKeyUsageEncryptDecrypt    = "ENCRYPT_DECRYPT"
	kmsKeyUsageSignVerify        = "SIGN_VERIFY"
	kmsKeyUsageGenerateVerifyMAC = "GENERATE_VERIFY_MAC"
	kmsKeyUsageKeyAgreement      = "KEY_AGREEMENT"
)

// kmsSigningAlgorithmsByKeySpec is the signing algorithms each key spec admits.
//
// Verified against the developer guide's key spec reference, which tabulates them per spec for the same
// reason it tabulates encryption algorithms: "you cannot configure a KMS key to use particular signing
// algorithms." All three RSA specs admit the same six — three RSASSA-PSS and three RSASSA-PKCS1-v1_5,
// differing only in hash — and the ECC specs admit one each except ECC_NIST_EDWARDS25519, whose two
// differ in the MessageType they require rather than in the curve. The three ML-DSA specs share
// ML_DSA_SHAKE_256, stated in prose rather than a table: "AWS KMS supports the ML_DSA_SHAKE_256 signing
// algorithm for all of the ML-DSA key specs."
//
// SYMMETRIC_DEFAULT and the four HMAC specs are absent deliberately: neither signs. As with
// [kmsEncryptionAlgorithmsByKeySpec], a missing entry and an empty one mean the same thing, so the
// lookup needs no is-this-spec-known branch and a spec substrate has never heard of lands in the same
// place — which since #977 no stored key can be, because CreateKey refuses an unpublished spec.
//
// Absence carries a second meaning since #977, and it is why these four maps are now read for more than
// rendering: a spec absent here does not admit SIGN_VERIFY at all. [kmsKeySpecAdmitsKeyUsage] derives
// AWS's pairing rules from that, so the rules cannot drift from the algorithm lists that imply them.
var kmsSigningAlgorithmsByKeySpec = map[string][]string{
	"RSA_2048": kmsRSASigningAlgorithms,
	"RSA_3072": kmsRSASigningAlgorithms,
	"RSA_4096": kmsRSASigningAlgorithms,

	"ECC_NIST_P256":         {"ECDSA_SHA_256"},
	"ECC_NIST_P384":         {"ECDSA_SHA_384"},
	"ECC_NIST_P521":         {"ECDSA_SHA_512"},
	"ECC_SECG_P256K1":       {"ECDSA_SHA_256"},
	"ECC_NIST_EDWARDS25519": {"ED25519_SHA_512", "ED25519_PH_SHA_512"},

	"ML_DSA_44": {"ML_DSA_SHAKE_256"},
	"ML_DSA_65": {"ML_DSA_SHAKE_256"},
	"ML_DSA_87": {"ML_DSA_SHAKE_256"},

	"SM2": {"SM2DSA"},
}

// kmsRSASigningAlgorithms is the six signing algorithms every RSA key spec admits, in the order AWS
// tabulates them.
//
// Named once and shared by the three RSA rows above, because the page's table is per *spec set* rather
// than per spec — the three differ in key length and not in admissible algorithm — and three copies of
// one list would invite them to drift apart.
var kmsRSASigningAlgorithms = []string{
	"RSASSA_PSS_SHA_256",
	"RSASSA_PSS_SHA_384",
	"RSASSA_PSS_SHA_512",
	"RSASSA_PKCS1_V1_5_SHA_256",
	"RSASSA_PKCS1_V1_5_SHA_384",
	"RSASSA_PKCS1_V1_5_SHA_512",
}

// kmsMACAlgorithmsByKeySpec is the MAC algorithm each HMAC key spec admits.
//
// One algorithm per spec, and the developer guide says why the map is a bijection rather than a choice:
// "the length of the key determines the MAC algorithm that is used in GenerateMac and VerifyMac
// operations." So the list AWS reports has exactly one element for every key that reports one at all.
var kmsMACAlgorithmsByKeySpec = map[string][]string{
	"HMAC_224": {"HMAC_SHA_224"},
	"HMAC_256": {"HMAC_SHA_256"},
	"HMAC_384": {"HMAC_SHA_384"},
	"HMAC_512": {"HMAC_SHA_512"},
}

// kmsKeyAgreementAlgorithmsByKeySpec is the key agreement algorithm each key spec admits.
//
// ECDH is the only value API_KeyMetadata publishes for the member, and the developer guide fixes which
// specs reach it: "if you're creating an asymmetric KMS key to derive shared secrets, use one of the
// NIST-standard elliptic curve key specs (except ECC_SECG_P256K1 and ECC_NIST_EDWARDS25519). The only
// supported key agreement algorithm for deriving shared secrets is the Elliptic Curve Cryptography
// Cofactor Diffie-Hellman Primitive (ECDH)." SM2's own section names deriving shared secrets as one of
// its three usages, so it is here too; the two excepted ECC specs are absent for the reason the sentence
// gives.
//
// This is the one of the four algorithm members whose *condition* is substrate's reading rather than
// AWS's. The other three publish one ("this value is present only when the KeyUsage of the KMS key is
// ENCRYPT_DECRYPT", and its two counterparts) and this member publishes none at all — its entire
// description is "the key agreement algorithm used to derive a shared secret". KEY_AGREEMENT is a
// published KeyUsage and a key has exactly one usage, so following the pattern of the three that do
// state a condition is the narrowest available reading; the alternative, reporting ECDH on a
// NIST-curve signing key, would tell a caller DeriveSharedSecret was available on a key AWS reserves
// for Sign.
var kmsKeyAgreementAlgorithmsByKeySpec = map[string][]string{
	"ECC_NIST_P256": {"ECDH"},
	"ECC_NIST_P384": {"ECDH"},
	"ECC_NIST_P521": {"ECDH"},
	"SM2":           {"ECDH"},
}

// kmsKeyUsageAlgorithmList is the KeyMetadata member a key usage reports, and the algorithms it reports
// there for each key spec.
type kmsKeyUsageAlgorithmList struct {
	// member is the KeyMetadata member name — one of the four algorithm members, each of which names
	// this usage as its own condition.
	member string
	// byKeySpec is the algorithms the usage admits per key spec. An absent or empty entry means the spec
	// does not admit the usage at all, which is what [kmsKeySpecAdmitsKeyUsage] reads.
	byKeySpec map[string][]string
}

// kmsKeyUsageAlgorithms joins each published key usage to the algorithm member it reports and the specs
// that admit it.
//
// It replaced a four-arm switch in [kmsKeyMetadata], and the reason is that #977 needed the same
// correspondence for something else: which usages a key spec admits, in order to refuse a pair AWS would
// not create. Two structures over one published fact are the drift #952 records and #974 hit inside this
// plugin, so there is one — the renderer and the validator now read the same table, and a spec added to an
// algorithm map becomes admissible for that usage in the same commit.
//
// Nothing outside this map decides which usages exist. [kmsKeyUsages] is the ordered slice of the same
// four keys, kept separate only because a map has no order and a refusal message needs one.
var kmsKeyUsageAlgorithms = map[string]kmsKeyUsageAlgorithmList{
	kmsKeyUsageEncryptDecrypt:    {member: "EncryptionAlgorithms", byKeySpec: kmsEncryptionAlgorithmsByKeySpec},
	kmsKeyUsageSignVerify:        {member: "SigningAlgorithms", byKeySpec: kmsSigningAlgorithmsByKeySpec},
	kmsKeyUsageGenerateVerifyMAC: {member: "MacAlgorithms", byKeySpec: kmsMACAlgorithmsByKeySpec},
	kmsKeyUsageKeyAgreement:      {member: "KeyAgreementAlgorithms", byKeySpec: kmsKeyAgreementAlgorithmsByKeySpec},
}

// kmsCustomerMasterKeySpecs is the key specs the deprecated CustomerMasterKeySpec member can carry.
//
// AWS still sends the member — "the KeySpec and CustomerMasterKeySpec fields have the same value. We
// recommend that you use the KeySpec field in your code. However, to avoid breaking changes, AWS KMS
// supports both fields" — so substrate answers it rather than dropping it as obsolete. A consumer
// written against an older SDK reads it, and omitting a member AWS sends is the defect this issue is
// about.
//
// It is a *narrower* enum than KeySpec, and that is the reason this list exists rather than the member
// simply echoing KeySpec. API_KeyMetadata gives CustomerMasterKeySpec thirteen Valid Values and KeySpec
// seventeen: the four specs added since the deprecation — ML_DSA_44, ML_DSA_65, ML_DSA_87 and
// ECC_NIST_EDWARDS25519 — appear only under KeySpec. So a key with one of those four has no admissible
// value for the deprecated member, and substrate omits it there rather than rendering a value outside
// the member's own published set. AWS documents no answer for that case; omitting is substrate's
// reading, and it is the one that cannot put an unpublished value on the wire.
var kmsCustomerMasterKeySpecs = []string{
	"RSA_2048",
	"RSA_3072",
	"RSA_4096",
	"ECC_NIST_P256",
	"ECC_NIST_P384",
	"ECC_NIST_P521",
	"ECC_SECG_P256K1",
	kmsSymmetricDefaultKeySpec,
	"HMAC_224",
	"HMAC_256",
	"HMAC_384",
	"HMAC_512",
	"SM2",
}

// kmsKeyMetadata builds the KeyMetadata element for a key.
//
// Shared by CreateKey and DescribeKey — see this file's preamble for why that is a requirement rather
// than a tidy-up — and it is the only place a KeyMetadata member is decided, so the two cannot disagree
// about one shape.
//
// Every member is either stored on the key or derived from a member that is. Nothing here reads state,
// takes a context or can fail, which is what lets both callers use it unconditionally.
func kmsKeyMetadata(key *KMSKey) map[string]interface{} {
	// The eleven unconditional members. AWSAccountId, KeyManager and Origin are the three a consumer is
	// most likely to branch on and the three that were most plainly withheld: each answers a decision a
	// caller legitimately makes — which account a cross-account grant would name, whether the key is
	// theirs to administer, and whether its material can expire or be re-imported — and all three were
	// already determined by substrate's own model.
	//
	// RotationEnabled is not among them, and #971 is why: API_KeyMetadata does not publish it. Adding
	// sixteen members from the page is the moment that omission is most likely to be "fixed" by someone
	// working from the struct rather than from the page, so it is named here as well as at its field.
	metadata := map[string]interface{}{
		"KeyId":        key.KeyID,
		"Arn":          key.ARN,
		"AWSAccountId": key.AccountID,
		"Description":  key.Description,
		"KeyUsage":     key.KeyUsage,
		"KeySpec":      key.KeySpec,
		"KeyState":     key.KeyState,
		"Enabled":      key.Enabled,
		"MultiRegion":  key.MultiRegion,
		"KeyManager":   kmsKeyManagerCustomer,
		"Origin":       kmsKeyOriginAWSKMS,
		"CreationDate": key.CreationDate.Unix(),
	}
	if slices.Contains(kmsCustomerMasterKeySpecs, key.KeySpec) {
		metadata["CustomerMasterKeySpec"] = key.KeySpec
	}
	// One algorithm list at most, selected by the key usage each member names as its own condition. The
	// single lookup is what enforces mutual exclusivity: a key has exactly one usage, so reporting two
	// lists would describe a key AWS cannot create.
	//
	// This was four switch arms until #977, which needed the same usage-to-spec correspondence to validate
	// CreateKey's arguments; [kmsKeyUsageAlgorithms] records why the two now read one table. The behavior
	// is unchanged, including for a usage the map does not carry — no member, rather than an invented one.
	//
	// A list is emitted only when it is non-empty. That mattered before #977, when a key could carry a
	// usage and a spec AWS would never pair — an ECC key with KeyUsage ENCRYPT_DECRYPT admits no encryption
	// algorithm at all — and an empty array would have said "this key supports no encryption algorithms", a
	// claim AWS never makes about a key it accepted. CreateKey now refuses that pair, so the branch is
	// unreachable through substrate's own API and kept as a guard: it is what keeps a spec added to one
	// algorithm map, without the pairing being thought through, from putting an empty array on the wire.
	if algorithms, ok := kmsKeyUsageAlgorithms[key.KeyUsage]; ok {
		kmsPutAlgorithms(metadata, algorithms.member, algorithms.byKeySpec[key.KeySpec])
	}
	// Emitted on the key state rather than on the field being non-zero, because that is the condition
	// API_KeyMetadata publishes: "this value is present only when the KMS key is scheduled for deletion,
	// that is, when its KeyState is PendingDeletion". Added by #963 — before it the deletion date
	// ScheduleKeyDeletion computed reached the caller once and was never stored, so DescribeKey could not
	// report when a pending key was due to go. It reaches CreateKey's response too now that the builder is
	// shared, which changes nothing: a key is Enabled the instant it is created.
	//
	// PendingDeletionWindowInDays, the adjacent member, is deliberately absent: the page confines it to
	// KeyState PendingReplicaDeletion, which only a multi-Region primary that still has replicas reaches
	// and substrate never writes. Its range is 1-365, not ScheduleKeyDeletion's 7-30, so the two are
	// different members and reporting the waiting period under it would be wrong twice over.
	//
	// Five more members stay absent for reasons of the same kind, and they are listed here rather than
	// left to be rediscovered. CloudHsmClusterId, CustomKeyStoreId and XksKeyConfiguration need a custom
	// or external key store; ExpirationModel and ValidTo are published only for an EXTERNAL origin, which
	// [kmsKeyOriginAWSKMS] records as unreachable. MultiRegionConfiguration is published "only when the
	// value of the MultiRegion field is True" and substrate stores that flag but models no replica, so it
	// would have to report a primary with an empty ReplicaKeys list — a shape that describes a
	// multi-Region key nothing can replicate. CurrentKeyMaterialId needs a key material identity, which is
	// #978.
	if key.KeyState == kmsKeyStatePendingDeletion {
		metadata["DeletionDate"] = key.DeletionDate.Unix()
	}
	return metadata
}

// kmsPutAlgorithms sets an algorithm-list member, or leaves it absent when the list is empty.
//
// A one-line helper, kept after #977 folded the four switch arms above into one lookup, because the
// emptiness check is a rule about the wire rather than a detail of the branching: an empty array claims a
// key admits no algorithm for its own usage, which is a claim AWS never makes about a key it accepted.
// Omitting says nothing instead, the honest-empty reading #827 established.
func kmsPutAlgorithms(metadata map[string]interface{}, member string, algorithms []string) {
	if len(algorithms) == 0 {
		return
	}
	metadata[member] = algorithms
}
