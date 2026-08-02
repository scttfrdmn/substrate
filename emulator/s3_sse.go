package emulator

import "strings"

// The three server-side-encryption headers substrate records. Named as constants
// because each is read on a write path and written on a read path, and a typo in one
// of the two halves would produce a header that is recorded and never echoed —
// silently, since an absent SSE header is a legal observation.
const (
	s3SSEAlgorithmHeader        = "x-amz-server-side-encryption"
	s3SSEKMSKeyIDHeader         = "x-amz-server-side-encryption-aws-kms-key-id"
	s3SSEBucketKeyEnabledHeader = "x-amz-server-side-encryption-bucket-key-enabled"
)

// s3SSEBucketKeyEnabledValue is the only value that enables an S3 Bucket Key. The
// header is a boolean, and S3 documents no meaning for any other token: "Setting this
// header to true causes Amazon S3 to use an S3 Bucket Key for object encryption with
// SSE-KMS". Substrate treats anything else as not-enabled and records nothing;
// rejecting a malformed token is one of #493's four validations.
const s3SSEBucketKeyEnabledValue = "true"

// S3ServerSideEncryption holds the server-side-encryption headers S3 records on an
// object and returns on every read: x-amz-server-side-encryption, the KMS key ID, and
// the S3 Bucket Key flag (#492).
//
// **No cryptography is performed.** Substrate records what the request asked for and
// echoes it back; the object body is stored exactly as it arrived. That is the whole
// of the model, and it is the right boundary: encryption at rest is not observable
// through an API call, but *the encryption S3 reports for an object* is — and that
// report is the assertion a consumer is trying to make. With SSE unmodeled an object
// written with the header read back byte-identical to one written without it, so a
// test could only assert on what the request carried, which is one inference away
// from the defect #475 was filed for.
//
// It is embedded in both [S3Object] and [S3MultipartUpload] rather than declared
// twice, following [S3SystemMetadata] for the same reason: the two write paths cannot
// disagree about a family declared once. #406 was exactly that divergence for
// Content-Encoding, and CompleteMultipartUpload copies the whole struct in one
// assignment, so a field added here is carried by both paths or by neither.
//
// Deliberately *not* in [S3SystemMetadata], though it would fit the shape: that family
// is defined by being recorded verbatim and echoed verbatim with no rules of its own,
// and SSE has resolution rules coming in #493 — a bucket default, a CopyObject
// asymmetry, and four rejected combinations. Content-Type and the storage class stayed
// out for the same reason. #475 raised the question explicitly; this is the answer.
//
// Out of scope here, all of it #493: bucket default encryption, CopyObject's
// non-inheritance of the source's encryption, and the invalid-combination 400s. SSE-C
// (x-amz-server-side-encryption-customer-*) is out of scope entirely — no consumer has
// asked for it, and its key material would have to be discarded rather than recorded.
type S3ServerSideEncryption struct {
	// Algorithm is the x-amz-server-side-encryption value recorded on write and
	// echoed on read — AES256 for SSE-S3, aws:kms for SSE-KMS. Empty when the write
	// named no encryption, which stays a distinct observation from any value: see
	// [S3ServerSideEncryption.emitSSE].
	//
	// Recorded verbatim and not validated against the documented set
	// (AES256, aws:fsx, aws:kms, aws:kms:dsse); rejecting an unrecognized token is
	// #493's.
	Algorithm string `json:"sse_algorithm,omitempty"`

	// KMSKeyID is the x-amz-server-side-encryption-aws-kms-key-id value, recorded and
	// echoed **verbatim**.
	//
	// This is a deliberate divergence from real S3, which echoes the *resolved key
	// ARN* regardless of which of the accepted forms was sent — the header takes a
	// "Key ID, Key ARN, or Key Alias". Substrate echoes the string the caller sent,
	// because that is the string the consumer's configuration produced and therefore
	// the one their assertion is about; #475's reporter confirms their tests assert
	// the sent form. Resolving it would require modeling KMS key aliases and
	// cross-account ARNs to answer a question no consumer asked.
	//
	// The divergence is observable and is documented in docs/services.md beside the
	// behavior, in the same spirit as the CopyObject MetadataDirective asymmetry in
	// s3_copy_metadata.go. If key resolution is ever modeled, this decision has to
	// be revisited rather than silently overtaken.
	//
	// Recorded whatever the algorithm is, including the AES256 combination real S3
	// rejects with InvalidArgument — that rejection is #493's, and recording keeps the
	// value observable until it exists.
	KMSKeyID string `json:"sse_kms_key_id,omitempty"`

	// BucketKeyEnabled is true when the write set
	// x-amz-server-side-encryption-bucket-key-enabled to [s3SSEBucketKeyEnabledValue].
	//
	// A bool rather than a string because only the enabled case is echoed, so the
	// three states a string would carry — "true", "false", absent — collapse to two
	// observations on the wire. See [S3ServerSideEncryption.emitSSE].
	BucketKeyEnabled bool `json:"sse_bucket_key_enabled,omitempty"`
}

// resolveServerSideEncryption reads the encryption family from request headers.
//
// Headers are matched case-insensitively via [headerValueFold], like every other
// header the S3 plugin reads: an [AWSRequest] reaching a plugin has not always been
// through net/http's canonicalization, since substrate builds requests in-process too
// (see betty_cfn.go), and RFC 9110 header names are case-insensitive regardless.
//
// The header *values* are not folded. An algorithm token and a KMS key ID are both
// case-sensitive identifiers — aws:kms and AWS:KMS are not the same token, and a KMS
// alias is case-sensitive — so normalizing either would corrupt the verbatim
// round-trip [S3ServerSideEncryption.KMSKeyID] exists to provide.
//
// The Bucket Key flag is the exception, being a boolean rather than an identifier: its
// value is compared case-insensitively, the way s3ChecksumModeEnabled reads
// x-amz-checksum-mode.
func resolveServerSideEncryption(headers map[string]string) S3ServerSideEncryption {
	bucketKey := headerValueFold(headers, s3SSEBucketKeyEnabledHeader)
	return S3ServerSideEncryption{
		Algorithm:        headerValueFold(headers, s3SSEAlgorithmHeader),
		KMSKeyID:         headerValueFold(headers, s3SSEKMSKeyIDHeader),
		BucketKeyEnabled: strings.EqualFold(bucketKey, s3SSEBucketKeyEnabledValue),
	}
}

// emitSSE adds the recorded encryption headers to a response header map, skipping
// the ones the object does not carry.
//
// Named emitSSE rather than emit because [S3Object] embeds this struct alongside
// [S3SystemMetadata], and two embedded types with a method of the same name at the
// same depth make the selector ambiguous.
//
// Every header is conditional, and each condition is a distinct observation rather
// than a tidiness measure:
//
//   - No algorithm means no header. An object written without SSE must read back with
//     none, so "no header sent" stays distinguishable from "header sent" — the
//     property that makes recording worth anything at all. Real S3 has applied SSE-S3
//     to every new object unconditionally since January 2023, so a real bucket never
//     stores one; modeling that default is #493's item and would remove this
//     distinction, which is why it is #493's decision to make deliberately.
//   - An empty KMS key ID means no header, matching S3's own "if present, indicates
//     the ID of the KMS key that was used for object encryption" — an SSE-KMS write
//     that named no key gets the AWS managed key and no ID to report.
//   - The Bucket Key header appears only when it was enabled. Absent and false are
//     different observations, and an SDK that distinguishes a nil *bool from a false
//     one would otherwise report the wrong answer for a write that never mentioned it.
func (e *S3ServerSideEncryption) emitSSE(dst map[string]string) {
	if e.Algorithm == "" {
		return
	}
	dst[s3SSEAlgorithmHeader] = e.Algorithm
	if e.KMSKeyID != "" {
		dst[s3SSEKMSKeyIDHeader] = e.KMSKeyID
	}
	if e.BucketKeyEnabled {
		dst[s3SSEBucketKeyEnabledHeader] = s3SSEBucketKeyEnabledValue
	}
}
