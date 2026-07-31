package emulator

import (
	"crypto/md5"  //nolint:gosec // nosemgrep // MD5 is one of the checksum algorithms S3 offers; it is not used here as a security primitive.
	"crypto/sha1" //nolint:gosec // nosemgrep // SHA-1 is one of the checksum algorithms S3 offers; it is not used here as a security primitive.
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/xml"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// S3 checksum types, as reported in x-amz-checksum-type. A single-part PutObject
// is always FULL_OBJECT: "Checksums of objects that are uploaded in a single part
// (using PutObject) are treated as full object checksums".
const (
	// S3ChecksumTypeFullObject is a checksum computed over every byte of the
	// object, "covering all data from the first byte of the first part to the last
	// byte of the last part".
	S3ChecksumTypeFullObject = "FULL_OBJECT"

	// S3ChecksumTypeComposite is a checksum computed over the concatenated
	// part-level digests of a multipart upload, suffixed with "-<part count>".
	S3ChecksumTypeComposite = "COMPOSITE"
)

// crc64NVMEPolynomial is the CRC-64/NVME generator polynomial in the bit-reversed
// (reflected) form that [crc64.MakeTable] expects.
//
// The catalog lists CRC-64/NVME as poly=0xad93d23594c93659 with refin/refout
// true; hash/crc64 takes the reversed polynomial, which is bits.Reverse64 of that
// value. Verified against the catalog check value: the CRC of "123456789" under
// this table is 0xae8b14860a799888.
const crc64NVMEPolynomial = 0x9a6c9329ac4bc9b5

// crc64NVMETable is the CRC-64/NVME table, built once. [crc64.MakeTable] allocates
// and fills 256 entries per call, and every checksummed request would otherwise
// rebuild it.
var crc64NVMETable = sync.OnceValue(func() *crc64.Table {
	return crc64.MakeTable(crc64NVMEPolynomial)
})

// s3ChecksumAlgorithm describes one algorithm in the x-amz-checksum-* family.
type s3ChecksumAlgorithm struct {
	// Name is the algorithm as it appears in x-amz-checksum-algorithm and
	// x-amz-sdk-checksum-algorithm, e.g. "CRC32C".
	Name string

	// New returns a fresh hash for this algorithm. Nil means substrate cannot
	// compute the digest; see [s3ChecksumAlgorithms].
	New func() hash.Hash

	// FullObject reports whether the algorithm supports the FULL_OBJECT checksum
	// type in a multipart upload. "Full object checksums in multipart uploads are
	// only available for CRC-based checksums because they can linearize into a
	// full object checksum."
	FullObject bool

	// Composite reports whether the algorithm supports the COMPOSITE checksum type.
	// CRC-64/NVME is the sole algorithm that does not.
	Composite bool
}

// s3ChecksumHeaderPrefix is the common prefix of every per-algorithm checksum
// header.
const s3ChecksumHeaderPrefix = "x-amz-checksum-"

// s3ChecksumAlgorithms is the set of checksum algorithms S3 documents, in the
// order the API reference lists them.
//
// Ten algorithms are accepted, because rejecting one real S3 takes would make a
// consumer's working code fail against substrate. Only seven can be *verified*
// here: XXHASH64, XXHASH3 and XXHASH128 have a nil New, because substrate has no
// implementation of them to check a caller's value against. Those three are
// answered with 501 NotImplemented rather than silently stored — accepting a
// checksum substrate cannot verify is the exact defect #399 exists to remove, and
// is worse than not offering the algorithm, since it makes a test pass while the
// real service would have caught the corruption.
//
// The full-object/composite columns are the documented multipart support matrix.
var s3ChecksumAlgorithms = []s3ChecksumAlgorithm{
	{Name: "CRC32", New: func() hash.Hash { return crc32.NewIEEE() }, FullObject: true, Composite: true},
	{Name: "CRC32C", New: func() hash.Hash { return crc32.New(crc32.MakeTable(crc32.Castagnoli)) }, FullObject: true, Composite: true},
	{Name: "CRC64NVME", New: func() hash.Hash { return crc64.New(crc64NVMETable()) }, FullObject: true, Composite: false},
	{Name: "SHA1", New: func() hash.Hash { return sha1.New() }, Composite: true}, //nolint:gosec // nosemgrep
	{Name: "SHA256", New: sha256.New, Composite: true},
	{Name: "SHA512", New: sha512.New, Composite: true},
	{Name: "MD5", New: md5.New, Composite: true}, //nolint:gosec // nosemgrep
	{Name: "XXHASH64", Composite: true},
	{Name: "XXHASH3", Composite: true},
	{Name: "XXHASH128", Composite: true},
}

// s3ChecksumByName resolves an algorithm by its documented name, case-insensitively.
func s3ChecksumByName(name string) (s3ChecksumAlgorithm, bool) {
	for _, alg := range s3ChecksumAlgorithms {
		if strings.EqualFold(alg.Name, name) {
			return alg, true
		}
	}
	return s3ChecksumAlgorithm{}, false
}

// s3ChecksumHeaderOf returns the per-algorithm header name for alg.
func s3ChecksumHeaderOf(name string) string {
	return s3ChecksumHeaderPrefix + strings.ToLower(name)
}

// s3ChecksumDigest returns the base64-encoded big-endian digest of body under alg.
// "The value field in the trailer chunk includes a base64 encoding of the
// big-endian checksum value" — hash.Sum already yields big-endian bytes.
func s3ChecksumDigest(alg s3ChecksumAlgorithm, body []byte) string {
	return base64.StdEncoding.EncodeToString(s3ChecksumRawDigest(alg, body))
}

// s3ChecksumRawDigest returns the unencoded digest of body under alg. Composite
// checksums hash the concatenation of these raw digests, so they need the bytes
// rather than their base64 form.
func s3ChecksumRawDigest(alg s3ChecksumAlgorithm, body []byte) []byte {
	h := alg.New()
	h.Write(body)
	return h.Sum(nil)
}

// s3Checksum is the checksum recorded for an object or part.
type s3Checksum struct {
	// Algorithm is the documented algorithm name, e.g. "SHA256". Empty when the
	// object carries no checksum.
	Algorithm string `json:"algorithm,omitempty"`

	// Value is the base64-encoded digest, with a "-<part count>" suffix when Type
	// is COMPOSITE.
	Value string `json:"value,omitempty"`

	// Type is FULL_OBJECT or COMPOSITE.
	Type string `json:"type,omitempty"`
}

// present reports whether a checksum was recorded.
func (c s3Checksum) present() bool {
	return c.Algorithm != "" && c.Value != ""
}

// s3ChecksumNotImplemented is the response for an algorithm S3 supports but
// substrate cannot compute.
//
// This is deliberately an error rather than an unverified success. A caller that
// gets a 501 learns immediately that substrate will not check this algorithm; one
// that gets a 200 learns nothing, and ships a test that passes on a checksum
// nobody validated.
func s3ChecksumNotImplemented(name string) *AWSResponse {
	return s3ErrorResponseWith(s3Error{
		Code: "NotImplemented",
		Message: "The " + name + " checksum algorithm is not implemented by substrate. " +
			"Substrate rejects it rather than storing a value it cannot verify. " +
			"Use CRC32, CRC32C, CRC64NVME, SHA1, SHA256, SHA512 or MD5.",
		Status:  http.StatusNotImplemented,
		Details: []s3ErrorDetail{{Name: "ChecksumAlgorithm", Value: name}},
	})
}

// s3BadDigestMessage is the message S3 pairs with a BadDigest code.
const s3BadDigestMessage = "The Content-MD5 or checksum value that you specified did not match what the server received."

// s3BadDigestResponse builds the 400 BadDigest served when a supplied checksum
// does not match the digest of the received body.
//
// "If the checksum value calculated by S3 matches your provided value, the request
// is accepted. If the values don't match, the request is rejected." The supplied
// and computed values are surfaced as detail elements so a failing test says which
// value was wrong rather than only that something was.
func s3BadDigestResponse(alg, supplied, computed string) *AWSResponse {
	return s3ErrorResponseWith(s3Error{
		Code:    "BadDigest",
		Message: s3BadDigestMessage,
		Status:  http.StatusBadRequest,
		Details: []s3ErrorDetail{
			{Name: "ChecksumAlgorithm", Value: alg},
			{Name: "ClientComputedChecksum", Value: supplied},
			{Name: "S3ComputedChecksum", Value: computed},
		},
	})
}

// resolveChecksum determines the checksum to record for a body written by
// PutObject or UploadPart, verifying any value the caller supplied.
//
// Three request shapes are honored, per the PutObject reference:
//
//   - A per-algorithm header (x-amz-checksum-sha256: <base64>) supplies a
//     precomputed value. It is verified against the body; a mismatch is 400
//     BadDigest and nothing is written.
//   - x-amz-sdk-checksum-algorithm: <NAME> names an algorithm without a value, so
//     substrate computes and stores the digest. "When you send this header, there
//     must be a corresponding x-amz-checksum-<algorithm> or x-amz-trailer header
//     sent. Otherwise, Amazon S3 fails the request with the HTTP status code 400
//     Bad Request."
//   - Neither: no checksum is recorded. Real S3 attaches a default CRC-64/NVME
//     checksum in this case, but substrate does not — see the note below.
//
// Both headers together must agree: "If the individual checksum value you provide
// through x-amz-checksum-<algorithm> doesn't match the checksum algorithm you set
// through x-amz-sdk-checksum-algorithm, Amazon S3 fails the request with a
// BadDigest error."
//
// The default-checksum divergence is deliberate. S3 now attaches CRC-64/NVME to
// every object uploaded without a checksum, so a GetObject with checksum-mode
// ENABLED returns a value even for an object written with no checksum headers at
// all. Substrate does not synthesize that, because doing so would make the
// round-trip assertion in every consumer's test pass whether or not their upload
// path actually sends a checksum — which is the failure mode this issue was filed
// about. An absent checksum here means "your writer sent none".
//
// A non-nil *AWSResponse is the error to return; the caller must not write.
func resolveChecksum(headers map[string]string, body []byte) (s3Checksum, *AWSResponse) {
	named, resp := requestedChecksumAlgorithm(headers)
	if resp != nil {
		return s3Checksum{}, resp
	}

	supplied, alg, resp := suppliedChecksum(headers)
	if resp != nil {
		return s3Checksum{}, resp
	}

	switch {
	case supplied != "":
		// Both headers present must name the same algorithm.
		if named != "" && !strings.EqualFold(named, alg.Name) {
			return s3Checksum{}, s3ErrorResponseWith(s3Error{
				Code:    "BadDigest",
				Message: s3BadDigestMessage,
				Status:  http.StatusBadRequest,
				Details: []s3ErrorDetail{
					{Name: "ChecksumAlgorithm", Value: alg.Name},
					{Name: "SDKChecksumAlgorithm", Value: named},
				},
			})
		}
		computed := s3ChecksumDigest(alg, body)
		if supplied != computed {
			return s3Checksum{}, s3BadDigestResponse(alg.Name, supplied, computed)
		}
		return s3Checksum{Algorithm: alg.Name, Value: computed, Type: S3ChecksumTypeFullObject}, nil

	case named != "":
		// The algorithm was named with no value and no trailer to carry one.
		sdkAlg, ok := s3ChecksumByName(named)
		if !ok {
			return s3Checksum{}, s3InvalidChecksumAlgorithmResponse(named)
		}
		if sdkAlg.New == nil {
			return s3Checksum{}, s3ChecksumNotImplemented(sdkAlg.Name)
		}
		return s3Checksum{
			Algorithm: sdkAlg.Name,
			Value:     s3ChecksumDigest(sdkAlg, body),
			Type:      S3ChecksumTypeFullObject,
		}, nil

	default:
		return s3Checksum{}, nil
	}
}

// requestedChecksumAlgorithm reads x-amz-sdk-checksum-algorithm, returning the
// name it holds. An empty name means the header was absent.
//
// The value is not resolved to an algorithm here: whether it must be one substrate
// can compute depends on whether a value accompanies it, which only
// [resolveChecksum] knows.
func requestedChecksumAlgorithm(headers map[string]string) (string, *AWSResponse) {
	name, ok := headerLookupFold(headers, "x-amz-sdk-checksum-algorithm")
	if !ok {
		return "", nil
	}
	if strings.TrimSpace(name) == "" {
		return "", s3InvalidChecksumAlgorithmResponse(name)
	}
	if _, known := s3ChecksumByName(name); !known {
		return "", s3InvalidChecksumAlgorithmResponse(name)
	}
	return name, nil
}

// suppliedChecksum finds the x-amz-checksum-<algorithm> header on a request and
// returns its base64 value alongside the algorithm it names.
//
// An empty value with a non-nil error response means the request named an
// algorithm substrate cannot verify, or named more than one.
func suppliedChecksum(headers map[string]string) (string, s3ChecksumAlgorithm, *AWSResponse) {
	var found s3ChecksumAlgorithm
	var value string

	for _, alg := range s3ChecksumAlgorithms {
		v, ok := headerLookupFold(headers, s3ChecksumHeaderOf(alg.Name))
		if !ok {
			continue
		}
		if found.Name != "" {
			// "Expecting a single x-amz-checksum- header. Multiple checksum Types
			// are not allowed."
			return "", s3ChecksumAlgorithm{}, s3ErrorResponseWith(s3Error{
				Code:    "InvalidRequest",
				Message: "Expecting a single x-amz-checksum- header. Multiple checksum Types are not allowed.",
				Status:  http.StatusBadRequest,
			})
		}
		found, value = alg, v
	}

	if found.Name == "" {
		return "", s3ChecksumAlgorithm{}, nil
	}
	if found.New == nil {
		return "", s3ChecksumAlgorithm{}, s3ChecksumNotImplemented(found.Name)
	}
	if !validBase64Checksum(value, found) {
		return "", s3ChecksumAlgorithm{}, s3ErrorResponseWith(s3Error{
			Code:    "InvalidRequest",
			Message: "Value for " + s3ChecksumHeaderOf(found.Name) + " header is invalid.",
			Status:  http.StatusBadRequest,
			Details: []s3ErrorDetail{{Name: "ChecksumAlgorithm", Value: found.Name}},
		})
	}
	return value, found, nil
}

// validBase64Checksum reports whether value is a base64 digest of the width alg
// produces. A well-formed but wrong-length value is a malformed request rather
// than a digest mismatch, so it is separated from BadDigest.
func validBase64Checksum(value string, alg s3ChecksumAlgorithm) bool {
	raw, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return false
	}
	return len(raw) == alg.New().Size()
}

// s3InvalidChecksumAlgorithmResponse is the 400 served for an algorithm name
// outside the documented set.
func s3InvalidChecksumAlgorithmResponse(name string) *AWSResponse {
	return s3ErrorResponseWith(s3Error{
		Code:    "InvalidRequest",
		Message: "Checksum algorithm provided is unsupported. Please try again with any of the valid types: [CRC32, CRC32C, CRC64NVME, SHA1, SHA256, SHA512, MD5, XXHASH64, XXHASH3, XXHASH128]",
		Status:  http.StatusBadRequest,
		Details: []s3ErrorDetail{{Name: "ChecksumAlgorithm", Value: name}},
	})
}

// resolveChecksumMode reads x-amz-checksum-mode and reports whether the caller
// asked for the stored checksum to be returned.
//
// "To retrieve the checksum, this mode must be enabled." The only documented value
// is ENABLED; anything else is treated as not enabled, matching S3, which returns
// no checksum rather than an error for an unrecognized mode.
func resolveChecksumMode(headers map[string]string) bool {
	return strings.EqualFold(headerValueFold(headers, "x-amz-checksum-mode"), "ENABLED")
}

// s3ChecksumXML carries one dynamically named checksum element, such as the
// <ChecksumSHA256> of a CompleteMultipartUploadResult. It works the same way
// [s3ErrorDetailXML] does: encoding/xml takes the element name from the XMLName
// field's value ahead of the parent field's tag.
type s3ChecksumXML struct {
	XMLName xml.Name
	Value   string `xml:",chardata"`
}

// checksumXMLElements renders a checksum as the <Checksum<ALGORITHM>> element
// CompleteMultipartUpload returns in its result body, or nil when there is none.
//
// The element name is the algorithm in the API's own casing — <ChecksumCRC64NVME>,
// <ChecksumSHA256> — which is why it is taken from the algorithm table rather than
// derived from the lowercased header name.
func checksumXMLElements(c s3Checksum) []s3ChecksumXML {
	if !c.present() {
		return nil
	}
	return []s3ChecksumXML{{
		XMLName: xml.Name{Local: "Checksum" + c.Algorithm},
		Value:   c.Value,
	}}
}

// applyChecksumHeaders adds the stored checksum and its type to a GetObject or
// HeadObject response, when the object has one and the request enabled
// checksum-mode. It is a no-op otherwise, which is what makes the absence of the
// header on a plain GET observable.
func applyChecksumHeaders(headers map[string]string, c s3Checksum, enabled bool) {
	if !enabled || !c.present() {
		return
	}
	headers[s3ChecksumHeaderOf(c.Algorithm)] = c.Value
	if c.Type != "" {
		headers["x-amz-checksum-type"] = c.Type
	}
}

// resolveUploadChecksum determines the checksum configuration for a multipart
// upload from a CreateMultipartUpload request.
//
// CreateMultipartUpload names the algorithm in x-amz-checksum-algorithm — not the
// x-amz-sdk-checksum-algorithm form PutObject uses — and optionally the type in
// x-amz-checksum-type. An absent type defaults to COMPOSITE for every algorithm
// that supports it, since "if you're using a multipart upload to upload your
// object, then you must specify the composite checksum type"; CRC-64/NVME, which
// supports no composite form, defaults to FULL_OBJECT instead.
//
// The requested combination is validated against the documented support matrix, so
// an upload that could never complete against real S3 is rejected here, at
// creation, rather than after every part has been uploaded.
func resolveUploadChecksum(headers map[string]string) (algorithm, checksumType string, resp *AWSResponse) {
	name, ok := headerLookupFold(headers, "x-amz-checksum-algorithm")
	requestedType := headerValueFold(headers, "x-amz-checksum-type")

	if !ok || strings.TrimSpace(name) == "" {
		if requestedType != "" {
			// A type with no algorithm has nothing to apply to.
			return "", "", s3ErrorResponseWith(s3Error{
				Code:    "InvalidRequest",
				Message: "x-amz-checksum-type cannot be used without x-amz-checksum-algorithm.",
				Status:  http.StatusBadRequest,
			})
		}
		return "", "", nil
	}

	alg, known := s3ChecksumByName(name)
	if !known {
		return "", "", s3InvalidChecksumAlgorithmResponse(name)
	}
	if alg.New == nil {
		return "", "", s3ChecksumNotImplemented(alg.Name)
	}

	switch {
	case requestedType == "":
		if alg.Composite {
			checksumType = S3ChecksumTypeComposite
		} else {
			checksumType = S3ChecksumTypeFullObject
		}
	case strings.EqualFold(requestedType, S3ChecksumTypeComposite):
		checksumType = S3ChecksumTypeComposite
	case strings.EqualFold(requestedType, S3ChecksumTypeFullObject):
		checksumType = S3ChecksumTypeFullObject
	default:
		return "", "", s3ErrorResponseWith(s3Error{
			Code:    "InvalidRequest",
			Message: "Checksum type provided is unsupported. Please try again with any of the valid types: [COMPOSITE, FULL_OBJECT]",
			Status:  http.StatusBadRequest,
			Details: []s3ErrorDetail{{Name: "ChecksumType", Value: requestedType}},
		})
	}

	if resp := checkChecksumTypeSupported(alg, checksumType); resp != nil {
		return "", "", resp
	}
	return alg.Name, checksumType, nil
}

// checkChecksumTypeSupported rejects an algorithm/type pairing the documented
// support matrix does not allow, such as a FULL_OBJECT SHA-256.
func checkChecksumTypeSupported(alg s3ChecksumAlgorithm, checksumType string) *AWSResponse {
	supported := alg.Composite
	if checksumType == S3ChecksumTypeFullObject {
		supported = alg.FullObject
	}
	if supported {
		return nil
	}
	return s3ErrorResponseWith(s3Error{
		Code: "InvalidRequest",
		Message: "The " + checksumType + " checksum type is not supported for the " + alg.Name +
			" checksum algorithm.",
		Status: http.StatusBadRequest,
		Details: []s3ErrorDetail{
			{Name: "ChecksumAlgorithm", Value: alg.Name},
			{Name: "ChecksumType", Value: checksumType},
		},
	})
}

// resolveCopyChecksumAlgorithm determines the algorithm a CopyObject destination
// should be checksummed under.
//
// "You can copy the object and specify whether you want to use the existing
// checksum algorithm or a new one. If you don't specify an algorithm, S3 uses the
// existing algorithm." An unchecksummed source with no algorithm named yields no
// checksum here, rather than the CRC-64/NVME real S3 would default to — the same
// deliberate divergence [resolveChecksum] documents, for the same reason.
//
// CopyObject names the algorithm in x-amz-checksum-algorithm, as
// CreateMultipartUpload does; there is no body for the caller to have precomputed a
// value over, so no per-algorithm value header is honored.
func resolveCopyChecksumAlgorithm(headers map[string]string, src *S3Object) (string, *AWSResponse) {
	name, ok := headerLookupFold(headers, "x-amz-checksum-algorithm")
	if !ok || strings.TrimSpace(name) == "" {
		return src.Checksum.Algorithm, nil
	}
	alg, known := s3ChecksumByName(name)
	if !known {
		return "", s3InvalidChecksumAlgorithmResponse(name)
	}
	if alg.New == nil {
		return "", s3ChecksumNotImplemented(alg.Name)
	}
	return alg.Name, nil
}

// copyChecksum computes the destination checksum of a CopyObject under algorithm,
// which is always a full-object checksum of the copied bytes.
func copyChecksum(algorithm string, body []byte) s3Checksum {
	if algorithm == "" {
		return s3Checksum{}
	}
	alg, known := s3ChecksumByName(algorithm)
	if !known || alg.New == nil {
		// Unreachable: resolveCopyChecksumAlgorithm rejects both cases. Returning
		// no checksum rather than panicking keeps a stored object whose algorithm
		// substrate no longer computes readable.
		return s3Checksum{}
	}
	return s3Checksum{
		Algorithm: alg.Name,
		Value:     s3ChecksumDigest(alg, body),
		Type:      S3ChecksumTypeFullObject,
	}
}

// resolvePartChecksum determines the checksum to record for one uploaded part.
//
// A part's algorithm is fixed by the upload: uploadAlgorithm is what
// CreateMultipartUpload recorded. A part that supplies a value under a *different*
// algorithm is rejected, mirroring the trailer rule — "if a request contains
// x-amz-trailer: x-amz-checksum-crc32 and the trailer chunk has the header name
// x-amz-checksum-sha1, the request fails" — because a part digest computed under
// another algorithm cannot contribute to the object's checksum.
//
// When the upload has an algorithm and the part supplies no value, the digest is
// computed here. That is what lets substrate assemble the object checksum on
// Complete regardless of whether the caller sent per-part values.
func resolvePartChecksum(headers map[string]string, body []byte, uploadAlgorithm string) (s3Checksum, *AWSResponse) {
	supplied, alg, resp := suppliedChecksum(headers)
	if resp != nil {
		return s3Checksum{}, resp
	}
	named, resp := requestedChecksumAlgorithm(headers)
	if resp != nil {
		return s3Checksum{}, resp
	}

	// Reconcile the algorithm the part names against the upload's.
	partAlgorithm := alg.Name
	if partAlgorithm == "" {
		partAlgorithm = named
	}
	if partAlgorithm != "" && uploadAlgorithm != "" && !strings.EqualFold(partAlgorithm, uploadAlgorithm) {
		return s3Checksum{}, s3ErrorResponseWith(s3Error{
			Code: "InvalidRequest",
			Message: "Checksum algorithm " + partAlgorithm + " does not match the " + uploadAlgorithm +
				" algorithm specified for this multipart upload.",
			Status: http.StatusBadRequest,
			Details: []s3ErrorDetail{
				{Name: "ChecksumAlgorithm", Value: partAlgorithm},
				{Name: "UploadChecksumAlgorithm", Value: uploadAlgorithm},
			},
		})
	}

	effective := uploadAlgorithm
	if effective == "" {
		effective = partAlgorithm
	}
	if effective == "" {
		return s3Checksum{}, nil
	}

	effAlg, known := s3ChecksumByName(effective)
	if !known {
		return s3Checksum{}, s3InvalidChecksumAlgorithmResponse(effective)
	}
	if effAlg.New == nil {
		return s3Checksum{}, s3ChecksumNotImplemented(effAlg.Name)
	}

	computed := s3ChecksumDigest(effAlg, body)
	if supplied != "" && supplied != computed {
		return s3Checksum{}, s3BadDigestResponse(effAlg.Name, supplied, computed)
	}
	// A part checksum is a checksum of that part's bytes in full, so its type is
	// FULL_OBJECT relative to the part. The object-level type is decided on
	// Complete, from the upload's configuration.
	return s3Checksum{Algorithm: effAlg.Name, Value: computed, Type: S3ChecksumTypeFullObject}, nil
}

// assembleObjectChecksum computes the object-level checksum of a completed
// multipart upload from the assembled body and the individual part bodies.
//
// The two checksum types are computed differently, and the difference is the whole
// point of distinguishing them:
//
//   - COMPOSITE hashes the concatenation of the raw part digests and appends
//     "-<part count>", exactly as substrate already derives the multipart ETag:
//     "this approach aggregates the part-level checksums (from the first part to
//     the last) to produce a single, combined checksum for the complete object."
//   - FULL_OBJECT hashes every byte of the assembled object, "covering all data
//     from the first byte of the first part to the last byte of the last part",
//     and carries no suffix.
//
// A caller whose single-part and multipart writers hash different byte streams gets
// two different values here, which is the class of bug the reporter of #399 hit.
func assembleObjectChecksum(algorithm, checksumType string, combined []byte, partBodies [][]byte) (s3Checksum, *AWSResponse) {
	if algorithm == "" {
		return s3Checksum{}, nil
	}
	alg, known := s3ChecksumByName(algorithm)
	if !known {
		return s3Checksum{}, s3InvalidChecksumAlgorithmResponse(algorithm)
	}
	if alg.New == nil {
		return s3Checksum{}, s3ChecksumNotImplemented(alg.Name)
	}
	if resp := checkChecksumTypeSupported(alg, checksumType); resp != nil {
		return s3Checksum{}, resp
	}

	if checksumType == S3ChecksumTypeFullObject {
		return s3Checksum{
			Algorithm: alg.Name,
			Value:     s3ChecksumDigest(alg, combined),
			Type:      S3ChecksumTypeFullObject,
		}, nil
	}

	var digests []byte
	for _, part := range partBodies {
		digests = append(digests, s3ChecksumRawDigest(alg, part)...)
	}
	return s3Checksum{
		Algorithm: alg.Name,
		Value:     s3ChecksumDigest(alg, digests) + "-" + strconv.Itoa(len(partBodies)),
		Type:      S3ChecksumTypeComposite,
	}, nil
}

// verifyCompleteChecksum checks a checksum supplied on CompleteMultipartUpload
// against the one substrate assembled.
//
// Complete accepts a full-object checksum for the whole upload: "You can provide
// the checksum of the whole object in the CompleteMultipartUpload request... If the
// two values don't match, S3 fails the request with a BadDigest error." A supplied
// x-amz-checksum-type that disagrees with what CreateMultipartUpload specified is
// also a BadDigest, since the value was computed under different rules.
func verifyCompleteChecksum(headers map[string]string, assembled s3Checksum) *AWSResponse {
	if requested := headerValueFold(headers, "x-amz-checksum-type"); requested != "" {
		if !strings.EqualFold(requested, assembled.Type) {
			return s3ErrorResponseWith(s3Error{
				Code:    "BadDigest",
				Message: s3BadDigestMessage,
				Status:  http.StatusBadRequest,
				Details: []s3ErrorDetail{
					{Name: "ChecksumType", Value: requested},
					{Name: "UploadChecksumType", Value: assembled.Type},
				},
			})
		}
	}

	supplied, alg, resp := suppliedChecksum(headers)
	if resp != nil {
		return resp
	}
	if supplied == "" {
		return nil
	}
	if assembled.Algorithm != "" && !strings.EqualFold(alg.Name, assembled.Algorithm) {
		return s3ErrorResponseWith(s3Error{
			Code: "InvalidRequest",
			Message: "Checksum algorithm " + alg.Name + " does not match the " + assembled.Algorithm +
				" algorithm specified for this multipart upload.",
			Status: http.StatusBadRequest,
			Details: []s3ErrorDetail{
				{Name: "ChecksumAlgorithm", Value: alg.Name},
				{Name: "UploadChecksumAlgorithm", Value: assembled.Algorithm},
			},
		})
	}
	if supplied != assembled.Value {
		return s3BadDigestResponse(alg.Name, supplied, assembled.Value)
	}
	return nil
}
