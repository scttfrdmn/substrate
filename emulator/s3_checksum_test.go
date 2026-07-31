package emulator_test

import (
	"bytes"
	"crypto/md5" //nolint:gosec // MD5 is one of the checksum algorithms under test.
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"net/http"
	"strings"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// checksumAlgorithms enumerates the algorithms substrate verifies, paired with an
// independent implementation of each.
//
// The hash constructors here are deliberately declared in the test rather than
// reached for from the emulator package: a test that computes its expected value by
// calling the same code under test would pass even if that code hashed the wrong
// algorithm. These are wired straight to the stdlib.
var checksumAlgorithms = []struct {
	Name       string
	Header     string
	New        func() hash.Hash
	FullObject bool // supports FULL_OBJECT in a multipart upload
	Composite  bool // supports COMPOSITE in a multipart upload
}{
	{Name: "CRC32", Header: "x-amz-checksum-crc32", New: func() hash.Hash { return crc32.NewIEEE() }, FullObject: true, Composite: true},
	{Name: "CRC32C", Header: "x-amz-checksum-crc32c", New: func() hash.Hash { return crc32.New(crc32.MakeTable(crc32.Castagnoli)) }, FullObject: true, Composite: true},
	{Name: "CRC64NVME", Header: "x-amz-checksum-crc64nvme", New: newCRC64NVME, FullObject: true},
	{Name: "SHA1", Header: "x-amz-checksum-sha1", New: sha1.New, Composite: true},
	{Name: "SHA256", Header: "x-amz-checksum-sha256", New: sha256.New, Composite: true},
	{Name: "SHA512", Header: "x-amz-checksum-sha512", New: sha512.New, Composite: true},
	{Name: "MD5", Header: "x-amz-checksum-md5", New: md5.New, Composite: true},
}

// newCRC64NVME builds a CRC-64/NVME hash from the reversed generator polynomial.
// TestS3Checksum_CRC64NVMEPolynomial pins this against the published check value,
// so a wrong polynomial here fails loudly rather than agreeing with a wrong one in
// the emulator.
func newCRC64NVME() hash.Hash {
	return crc64.New(crc64.MakeTable(0x9a6c9329ac4bc9b5))
}

// digestFor returns the base64 digest of body under the named algorithm.
func digestFor(t *testing.T, name string, body []byte) string {
	t.Helper()
	for _, alg := range checksumAlgorithms {
		if alg.Name == name {
			h := alg.New()
			h.Write(body)
			return base64.StdEncoding.EncodeToString(h.Sum(nil))
		}
	}
	t.Fatalf("no test implementation for algorithm %q", name)
	return ""
}

// corruptDigest returns a valid base64 digest of the right width for alg that is
// not the digest of body — a wrong checksum, as distinct from a malformed one.
func corruptDigest(t *testing.T, name string, body []byte) string {
	t.Helper()
	return digestFor(t, name, append([]byte("not-"), body...))
}

// putBucket creates a bucket, failing the test if it cannot.
func putBucket(t *testing.T, srv *emulator.Server, bucket string) {
	t.Helper()
	w := s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil)
	require.Equal(t, http.StatusOK, w.Code, "create bucket")
}

// TestS3Checksum_CRC64NVMEPolynomial pins the CRC-64/NVME table against the
// published catalog check value, so a transcription error in the polynomial is
// caught here rather than showing up as a plausible-looking wrong digest.
func TestS3Checksum_CRC64NVMEPolynomial(t *testing.T) {
	h := newCRC64NVME()
	h.Write([]byte("123456789"))
	assert.Equal(t, "ae8b14860a799888", fmt.Sprintf("%x", h.Sum(nil)),
		"CRC-64/NVME check value for the string 123456789")
}

// TestS3Checksum_PutObject_RoundTrip covers, per algorithm, the two halves of the
// acceptance criterion: a supplied checksum is echoed on PUT and comes back on GET
// and HEAD under checksum-mode ENABLED, and is absent from both without it.
func TestS3Checksum_PutObject_RoundTrip(t *testing.T) {
	body := []byte("substrate checksum round trip")

	for _, alg := range checksumAlgorithms {
		t.Run(alg.Name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			putBucket(t, srv, "cks")

			want := digestFor(t, alg.Name, body)

			w := s3Request(t, srv, http.MethodPut, "/cks/obj", body, map[string]string{
				alg.Header: want,
			})
			require.Equal(t, http.StatusOK, w.Code, "PUT body: %s", w.Body.String())
			assert.Equal(t, want, w.Header().Get(alg.Header), "PutObject echoes the checksum")
			assert.Equal(t, "FULL_OBJECT", w.Header().Get("x-amz-checksum-type"),
				"a single-part PUT is always a full-object checksum")

			// GET with the mode enabled returns it.
			w = s3Request(t, srv, http.MethodGet, "/cks/obj", nil, map[string]string{
				"x-amz-checksum-mode": "ENABLED",
			})
			require.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, want, w.Header().Get(alg.Header))
			assert.Equal(t, "FULL_OBJECT", w.Header().Get("x-amz-checksum-type"))
			assert.Equal(t, body, w.Body.Bytes())

			// GET without it does not.
			w = s3Request(t, srv, http.MethodGet, "/cks/obj", nil, nil)
			require.Equal(t, http.StatusOK, w.Code)
			assert.Empty(t, w.Header().Get(alg.Header),
				"checksum must be absent without x-amz-checksum-mode: ENABLED")
			assert.Empty(t, w.Header().Get("x-amz-checksum-type"))

			// HEAD honors the mode identically.
			w = s3Request(t, srv, http.MethodHead, "/cks/obj", nil, map[string]string{
				"x-amz-checksum-mode": "ENABLED",
			})
			require.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, want, w.Header().Get(alg.Header))

			w = s3Request(t, srv, http.MethodHead, "/cks/obj", nil, nil)
			require.Equal(t, http.StatusOK, w.Code)
			assert.Empty(t, w.Header().Get(alg.Header))
		})
	}
}

// TestS3Checksum_PutObject_BadDigest is the behavior the issue most wanted: a
// wrong checksum is a 400 BadDigest, and the object is not written.
func TestS3Checksum_PutObject_BadDigest(t *testing.T) {
	body := []byte("payload that will be rejected")

	for _, alg := range checksumAlgorithms {
		t.Run(alg.Name, func(t *testing.T) {
			srv, fs := newS3TestServer(t)
			putBucket(t, srv, "cks")

			w := s3Request(t, srv, http.MethodPut, "/cks/rejected", body, map[string]string{
				alg.Header: corruptDigest(t, alg.Name, body),
			})
			require.Equal(t, http.StatusBadRequest, w.Code)
			assert.Contains(t, w.Body.String(), "<Code>BadDigest</Code>")
			assert.Contains(t, w.Body.String(), "<ChecksumAlgorithm>"+alg.Name+"</ChecksumAlgorithm>")

			// The object must not exist: neither its metadata nor its body.
			w = s3Request(t, srv, http.MethodHead, "/cks/rejected", nil, nil)
			assert.Equal(t, http.StatusNotFound, w.Code, "rejected object must not be readable")

			exists, err := afero.Exists(fs, "/cks/rejected")
			require.NoError(t, err)
			assert.False(t, exists, "rejected object body must not be on the filesystem")
		})
	}
}

// TestS3Checksum_PutObject_BadDigestDoesNotOverwrite asserts the stronger property:
// a rejected PUT over an existing object leaves the original bytes and checksum
// intact, rather than truncating or partially replacing them.
func TestS3Checksum_PutObject_BadDigestDoesNotOverwrite(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "cks")

	original := []byte("the original contents")
	originalDigest := digestFor(t, "SHA256", original)

	w := s3Request(t, srv, http.MethodPut, "/cks/obj", original, map[string]string{
		"x-amz-checksum-sha256": originalDigest,
	})
	require.Equal(t, http.StatusOK, w.Code)
	originalETag := w.Header().Get("ETag")

	replacement := []byte("a replacement that fails its checksum")
	w = s3Request(t, srv, http.MethodPut, "/cks/obj", replacement, map[string]string{
		"x-amz-checksum-sha256": corruptDigest(t, "SHA256", replacement),
	})
	require.Equal(t, http.StatusBadRequest, w.Code)

	w = s3Request(t, srv, http.MethodGet, "/cks/obj", nil, map[string]string{
		"x-amz-checksum-mode": "ENABLED",
	})
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, original, w.Body.Bytes(), "body must be unchanged")
	assert.Equal(t, originalETag, w.Header().Get("ETag"), "ETag must be unchanged")
	assert.Equal(t, originalDigest, w.Header().Get("x-amz-checksum-sha256"),
		"stored checksum must be unchanged")
}

// TestS3Checksum_SDKChecksumAlgorithm covers the second write shape: the caller
// names an algorithm and substrate computes the digest.
func TestS3Checksum_SDKChecksumAlgorithm(t *testing.T) {
	body := []byte("service-computed digest")

	for _, alg := range checksumAlgorithms {
		t.Run(alg.Name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			putBucket(t, srv, "cks")

			w := s3Request(t, srv, http.MethodPut, "/cks/obj", body, map[string]string{
				"x-amz-sdk-checksum-algorithm": alg.Name,
			})
			require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
			assert.Equal(t, digestFor(t, alg.Name, body), w.Header().Get(alg.Header))

			w = s3Request(t, srv, http.MethodGet, "/cks/obj", nil, map[string]string{
				"x-amz-checksum-mode": "ENABLED",
			})
			require.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, digestFor(t, alg.Name, body), w.Header().Get(alg.Header))
		})
	}
}

// TestS3Checksum_NoChecksumHeaders asserts that an object written with no checksum
// headers records none.
//
// This is a deliberate divergence from current S3, which attaches a default
// CRC-64/NVME checksum to every upload. Synthesizing one here would make a
// consumer's "the checksum came back" assertion pass whether or not their writer
// sends a checksum at all — the precise failure #399 was filed about.
func TestS3Checksum_NoChecksumHeaders(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "cks")

	w := s3Request(t, srv, http.MethodPut, "/cks/plain", []byte("no checksum"), nil)
	require.Equal(t, http.StatusOK, w.Code)
	for _, alg := range checksumAlgorithms {
		assert.Empty(t, w.Header().Get(alg.Header))
	}

	w = s3Request(t, srv, http.MethodGet, "/cks/plain", nil, map[string]string{
		"x-amz-checksum-mode": "ENABLED",
	})
	require.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, w.Header().Get("x-amz-checksum-crc64nvme"),
		"substrate does not synthesize a default checksum")
	assert.Empty(t, w.Header().Get("x-amz-checksum-type"))
}

// TestS3Checksum_ChecksumModeValues checks the gate itself: only ENABLED opens it,
// and an unrecognized value is not an error.
func TestS3Checksum_ChecksumModeValues(t *testing.T) {
	body := []byte("mode gating")
	want := digestFor(t, "SHA256", body)

	tests := []struct {
		name    string
		mode    string
		present bool
	}{
		{name: "enabled", mode: "ENABLED", present: true},
		{name: "lowercase enabled", mode: "enabled", present: true},
		{name: "mixed case enabled", mode: "Enabled", present: true},
		{name: "absent", mode: "", present: false},
		{name: "disabled", mode: "DISABLED", present: false},
		{name: "unrecognized", mode: "sometimes", present: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			putBucket(t, srv, "cks")

			w := s3Request(t, srv, http.MethodPut, "/cks/obj", body, map[string]string{
				"x-amz-checksum-sha256": want,
			})
			require.Equal(t, http.StatusOK, w.Code)

			headers := map[string]string{}
			if tc.mode != "" {
				headers["x-amz-checksum-mode"] = tc.mode
			}
			w = s3Request(t, srv, http.MethodGet, "/cks/obj", nil, headers)
			// An unrecognized mode is never an error; it just returns no checksum.
			require.Equal(t, http.StatusOK, w.Code)
			if tc.present {
				assert.Equal(t, want, w.Header().Get("x-amz-checksum-sha256"))
			} else {
				assert.Empty(t, w.Header().Get("x-amz-checksum-sha256"))
			}
		})
	}
}

// TestS3Checksum_PutObject_MalformedValue distinguishes a malformed checksum from a
// wrong one: a value that is not base64, or is the wrong width for its algorithm,
// is an InvalidRequest rather than a BadDigest.
func TestS3Checksum_PutObject_MalformedValue(t *testing.T) {
	body := []byte("malformed checksum values")

	tests := []struct {
		name  string
		value string
	}{
		{name: "not base64", value: "!!!not-base64!!!"},
		{name: "too short", value: base64.StdEncoding.EncodeToString([]byte("short"))},
		{name: "too long", value: base64.StdEncoding.EncodeToString(make([]byte, 64))},
		{name: "empty", value: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			putBucket(t, srv, "cks")

			w := s3Request(t, srv, http.MethodPut, "/cks/obj", body, map[string]string{
				"x-amz-checksum-sha256": tc.value,
			})
			require.Equal(t, http.StatusBadRequest, w.Code)
			assert.Contains(t, w.Body.String(), "<Code>InvalidRequest</Code>")

			w = s3Request(t, srv, http.MethodHead, "/cks/obj", nil, nil)
			assert.Equal(t, http.StatusNotFound, w.Code, "object must not be written")
		})
	}
}

// TestS3Checksum_PutObject_AlgorithmDisagreement covers the documented BadDigest
// case where the two write headers name different algorithms: "if the individual
// checksum value you provide through x-amz-checksum-<algorithm> doesn't match the
// checksum algorithm you set through x-amz-sdk-checksum-algorithm, Amazon S3 fails
// the request with a BadDigest error".
func TestS3Checksum_PutObject_AlgorithmDisagreement(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "cks")

	body := []byte("disagreeing algorithms")
	w := s3Request(t, srv, http.MethodPut, "/cks/obj", body, map[string]string{
		"x-amz-checksum-sha256":        digestFor(t, "SHA256", body),
		"x-amz-sdk-checksum-algorithm": "CRC32",
	})
	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "<Code>BadDigest</Code>")

	w = s3Request(t, srv, http.MethodHead, "/cks/obj", nil, nil)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

// TestS3Checksum_PutObject_AlgorithmAgreement is the converse: the same algorithm
// named in both headers is accepted.
func TestS3Checksum_PutObject_AlgorithmAgreement(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "cks")

	body := []byte("agreeing algorithms")
	want := digestFor(t, "SHA256", body)
	w := s3Request(t, srv, http.MethodPut, "/cks/obj", body, map[string]string{
		"x-amz-checksum-sha256":        want,
		"x-amz-sdk-checksum-algorithm": "sha256", // case-insensitive
	})
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
	assert.Equal(t, want, w.Header().Get("x-amz-checksum-sha256"))
}

// TestS3Checksum_PutObject_MultipleChecksumHeaders asserts that two different
// per-algorithm headers on one request is an InvalidRequest, not a silent pick.
func TestS3Checksum_PutObject_MultipleChecksumHeaders(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "cks")

	body := []byte("two checksums")
	w := s3Request(t, srv, http.MethodPut, "/cks/obj", body, map[string]string{
		"x-amz-checksum-sha256": digestFor(t, "SHA256", body),
		"x-amz-checksum-crc32":  digestFor(t, "CRC32", body),
	})
	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "<Code>InvalidRequest</Code>")
	assert.Contains(t, w.Body.String(), "Multiple checksum Types are not allowed")
}

// TestS3Checksum_UnsupportedAlgorithms asserts that the three algorithms substrate
// cannot compute are refused with 501 rather than accepted unverified.
//
// This is the property that keeps substrate from being worse than no mock at all
// for these algorithms: a caller learns immediately, instead of shipping a test
// that passes on a checksum nothing ever checked.
func TestS3Checksum_UnsupportedAlgorithms(t *testing.T) {
	body := []byte("xxhash family")

	for _, alg := range []struct{ Name, Header string }{
		{"XXHASH64", "x-amz-checksum-xxhash64"},
		{"XXHASH3", "x-amz-checksum-xxhash3"},
		{"XXHASH128", "x-amz-checksum-xxhash128"},
	} {
		t.Run(alg.Name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			putBucket(t, srv, "cks")

			// Supplied as a value...
			w := s3Request(t, srv, http.MethodPut, "/cks/obj", body, map[string]string{
				alg.Header: base64.StdEncoding.EncodeToString([]byte("00000000")),
			})
			assert.Equal(t, http.StatusNotImplemented, w.Code)
			assert.Contains(t, w.Body.String(), "<Code>NotImplemented</Code>")
			assert.Contains(t, w.Body.String(), "<ChecksumAlgorithm>"+alg.Name+"</ChecksumAlgorithm>")

			// ...and named for the service to compute.
			w = s3Request(t, srv, http.MethodPut, "/cks/obj", body, map[string]string{
				"x-amz-sdk-checksum-algorithm": alg.Name,
			})
			assert.Equal(t, http.StatusNotImplemented, w.Code)

			w = s3Request(t, srv, http.MethodHead, "/cks/obj", nil, nil)
			assert.Equal(t, http.StatusNotFound, w.Code, "object must not be written")
		})
	}
}

// TestS3Checksum_UnknownAlgorithm asserts an algorithm outside the documented set
// is an InvalidRequest.
func TestS3Checksum_UnknownAlgorithm(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "cks")

	w := s3Request(t, srv, http.MethodPut, "/cks/obj", []byte("x"), map[string]string{
		"x-amz-sdk-checksum-algorithm": "SHA3",
	})
	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "<Code>InvalidRequest</Code>")
	assert.Contains(t, w.Body.String(), "<ChecksumAlgorithm>SHA3</ChecksumAlgorithm>")
}

// TestS3Checksum_EmptyBody asserts a zero-byte object gets the digest of the empty
// string, not an absent checksum.
func TestS3Checksum_EmptyBody(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "cks")

	want := digestFor(t, "CRC32", nil)
	require.Equal(t, "AAAAAA==", want, "CRC32 of the empty string")

	w := s3Request(t, srv, http.MethodPut, "/cks/empty", []byte{}, map[string]string{
		"x-amz-checksum-crc32": want,
	})
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())

	w = s3Request(t, srv, http.MethodGet, "/cks/empty", nil, map[string]string{
		"x-amz-checksum-mode": "ENABLED",
	})
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, want, w.Header().Get("x-amz-checksum-crc32"))
}

// TestS3Checksum_RangedGetReturnsObjectChecksum documents how a checksum composes
// with a ranged read: the header is the whole object's checksum, so it must not be
// mistaken for a checksum of the bytes actually served.
func TestS3Checksum_RangedGetReturnsObjectChecksum(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "cks")

	body := []byte("0123456789abcdefghij")
	want := digestFor(t, "SHA256", body)
	w := s3Request(t, srv, http.MethodPut, "/cks/obj", body, map[string]string{
		"x-amz-checksum-sha256": want,
	})
	require.Equal(t, http.StatusOK, w.Code)

	w = s3Request(t, srv, http.MethodGet, "/cks/obj", nil, map[string]string{
		"Range":               "bytes=0-3",
		"x-amz-checksum-mode": "ENABLED",
	})
	require.Equal(t, http.StatusPartialContent, w.Code)
	assert.Equal(t, []byte("0123"), w.Body.Bytes())
	assert.Equal(t, want, w.Header().Get("x-amz-checksum-sha256"),
		"the checksum is the full object's, not the range's")
}

// TestS3Checksum_Versioning asserts each version keeps its own checksum.
func TestS3Checksum_Versioning(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "cks")

	w := s3Request(t, srv, http.MethodPut, "/cks?versioning", []byte(
		`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`), nil)
	require.Equal(t, http.StatusOK, w.Code)

	first := []byte("version one")
	w = s3Request(t, srv, http.MethodPut, "/cks/obj", first, map[string]string{
		"x-amz-checksum-sha256": digestFor(t, "SHA256", first),
	})
	require.Equal(t, http.StatusOK, w.Code)
	v1 := w.Header().Get("x-amz-version-id")
	require.NotEmpty(t, v1)

	second := []byte("version two, which differs")
	w = s3Request(t, srv, http.MethodPut, "/cks/obj", second, map[string]string{
		"x-amz-checksum-sha256": digestFor(t, "SHA256", second),
	})
	require.Equal(t, http.StatusOK, w.Code)

	w = s3Request(t, srv, http.MethodGet, "/cks/obj?versionId="+v1, nil, map[string]string{
		"x-amz-checksum-mode": "ENABLED",
	})
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, digestFor(t, "SHA256", first), w.Header().Get("x-amz-checksum-sha256"),
		"the old version keeps its own checksum")

	w = s3Request(t, srv, http.MethodGet, "/cks/obj", nil, map[string]string{
		"x-amz-checksum-mode": "ENABLED",
	})
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, digestFor(t, "SHA256", second), w.Header().Get("x-amz-checksum-sha256"))
}

// --- aws-chunked trailing checksums ---

// awsChunkedBody frames body as a single aws-chunked object chunk followed by a
// completion chunk and a trailer, in the format the S3 user guide documents:
//
//	x-amz-checksum-<alg>:<base64>\n\r\n\r\n
func awsChunkedBody(body []byte, trailerName, trailerValue string) []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "%x\r\n", len(body))
	b.Write(body)
	b.WriteString("\r\n0\r\n")
	if trailerName != "" {
		fmt.Fprintf(&b, "%s:%s\n\r\n", trailerName, trailerValue)
	}
	b.WriteString("\r\n")
	return []byte(b.String())
}

// awsChunkedHeaders builds the header set that marks a body as aws-chunked with a
// trailing checksum.
func awsChunkedHeaders(decodedLen int, trailerName string) map[string]string {
	h := map[string]string{
		"Content-Encoding":             "aws-chunked",
		"x-amz-content-sha256":         "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
		"x-amz-decoded-content-length": fmt.Sprintf("%d", decodedLen),
	}
	if trailerName != "" {
		h["x-amz-trailer"] = trailerName
	}
	return h
}

// TestS3Checksum_TrailingChecksum_Accepted asserts that a checksum arriving in an
// aws-chunked trailer is verified, not discarded.
//
// This is the path every real SDK upload takes when given a ChecksumAlgorithm, so a
// decoder that dropped trailers would make checksum verification silently
// unreachable in exactly the case consumers care about.
func TestS3Checksum_TrailingChecksum_Accepted(t *testing.T) {
	body := []byte("streamed with a trailing checksum")

	for _, alg := range checksumAlgorithms {
		t.Run(alg.Name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			putBucket(t, srv, "cks")

			want := digestFor(t, alg.Name, body)
			framed := awsChunkedBody(body, alg.Header, want)

			w := s3Request(t, srv, http.MethodPut, "/cks/streamed", framed,
				awsChunkedHeaders(len(body), alg.Header))
			require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
			assert.Equal(t, want, w.Header().Get(alg.Header))

			// The stored body must be the decoded content, not the framing.
			w = s3Request(t, srv, http.MethodGet, "/cks/streamed", nil, map[string]string{
				"x-amz-checksum-mode": "ENABLED",
			})
			require.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, body, w.Body.Bytes())
			assert.Equal(t, want, w.Header().Get(alg.Header))
		})
	}
}

// TestS3Checksum_TrailingChecksum_BadDigest asserts a wrong trailing checksum is
// rejected and writes nothing — the same guarantee as the header form.
func TestS3Checksum_TrailingChecksum_BadDigest(t *testing.T) {
	srv, fs := newS3TestServer(t)
	putBucket(t, srv, "cks")

	body := []byte("streamed but corrupt")
	framed := awsChunkedBody(body, "x-amz-checksum-crc32", corruptDigest(t, "CRC32", body))

	w := s3Request(t, srv, http.MethodPut, "/cks/streamed", framed,
		awsChunkedHeaders(len(body), "x-amz-checksum-crc32"))
	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "<Code>BadDigest</Code>")

	exists, err := afero.Exists(fs, "/cks/streamed")
	require.NoError(t, err)
	assert.False(t, exists)
}

// TestS3Checksum_TrailingChecksum_SignedFraming asserts the trailer is still found
// when the chunks carry SigV4 signatures and a trailer signature line follows.
func TestS3Checksum_TrailingChecksum_SignedFraming(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "cks")

	body := []byte("signed chunks with a trailing checksum")
	want := digestFor(t, "CRC32C", body)

	var b strings.Builder
	fmt.Fprintf(&b, "%x;chunk-signature=%s\r\n", len(body), strings.Repeat("a", 64))
	b.Write(body)
	fmt.Fprintf(&b, "\r\n0;chunk-signature=%s\r\n", strings.Repeat("b", 64))
	fmt.Fprintf(&b, "x-amz-checksum-crc32c:%s\n\r\n", want)
	fmt.Fprintf(&b, "%s\r\n\r\n", strings.Repeat("c", 64)) // trailer signature

	headers := awsChunkedHeaders(len(body), "x-amz-checksum-crc32c")
	headers["x-amz-content-sha256"] = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"

	w := s3Request(t, srv, http.MethodPut, "/cks/signed", []byte(b.String()), headers)
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
	assert.Equal(t, want, w.Header().Get("x-amz-checksum-crc32c"))

	w = s3Request(t, srv, http.MethodGet, "/cks/signed", nil, nil)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, body, w.Body.Bytes())
}

// TestS3Checksum_TrailingChecksum_NameMismatch asserts the documented rule that the
// trailer's header name must match what x-amz-trailer declared: "if a request
// contains x-amz-trailer: x-amz-checksum-crc32 and the trailer chunk has the header
// name x-amz-checksum-sha1, the request fails".
func TestS3Checksum_TrailingChecksum_NameMismatch(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "cks")

	body := []byte("mismatched trailer name")
	framed := awsChunkedBody(body, "x-amz-checksum-sha1", digestFor(t, "SHA1", body))

	w := s3Request(t, srv, http.MethodPut, "/cks/obj", framed,
		awsChunkedHeaders(len(body), "x-amz-checksum-crc32"))
	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "<Code>MalformedTrailerError</Code>")

	w = s3Request(t, srv, http.MethodHead, "/cks/obj", nil, nil)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

// TestS3Checksum_NonChunkedBodyUnaffected guards the decoder's no-op path: a plain
// body whose content happens to resemble chunk framing must be stored verbatim.
func TestS3Checksum_NonChunkedBodyUnaffected(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "cks")

	body := []byte("5\r\nhello\r\n0\r\n\r\n")
	want := digestFor(t, "SHA256", body)

	w := s3Request(t, srv, http.MethodPut, "/cks/plain", body, map[string]string{
		"x-amz-checksum-sha256": want,
	})
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())

	w = s3Request(t, srv, http.MethodGet, "/cks/plain", nil, nil)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, body, w.Body.Bytes())
}

// --- multipart ---

// uploadIDOf extracts the UploadId from an InitiateMultipartUploadResult body.
func uploadIDOf(t *testing.T, body string) string {
	t.Helper()
	var res struct {
		UploadID string `xml:"UploadId"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &res))
	require.NotEmpty(t, res.UploadID)
	return res.UploadID
}

// multipartPartBodies is a two-part fixture. The parts differ in content so that a
// composite checksum computed over the wrong concatenation cannot coincidentally
// match, and they differ in length so that a digest taken over the wrong slice
// boundary cannot either.
//
// The first part is exactly the minimum part size: anything smaller is rejected by
// CompleteMultipartUpload with EntityTooSmall (#400) before checksum assembly runs.
func multipartPartBodies() [][]byte {
	return [][]byte{
		bytes.Repeat([]byte("a"), s3MinPartSize),
		bytes.Repeat([]byte("b"), 32),
	}
}

// compositeChecksum computes the expected COMPOSITE object checksum independently:
// the digest of the concatenated raw part digests, suffixed with the part count.
func compositeChecksum(t *testing.T, name string, parts [][]byte) string {
	t.Helper()
	var alg struct {
		New func() hash.Hash
	}
	for _, a := range checksumAlgorithms {
		if a.Name == name {
			alg.New = a.New
		}
	}
	require.NotNil(t, alg.New, "no test implementation for %q", name)

	var digests []byte
	for _, part := range parts {
		h := alg.New()
		h.Write(part)
		digests = append(digests, h.Sum(nil)...)
	}
	outer := alg.New()
	outer.Write(digests)
	return fmt.Sprintf("%s-%d", base64.StdEncoding.EncodeToString(outer.Sum(nil)), len(parts))
}

// TestS3Checksum_Multipart_Composite is the acceptance criterion's multipart case:
// a COMPOSITE upload's object checksum is base64(digest-of-concatenated-part-digests)
// with the "-N" suffix, returned as an XML element in the Complete result and as a
// header on a subsequent GET.
func TestS3Checksum_Multipart_Composite(t *testing.T) {
	parts := multipartPartBodies()

	for _, alg := range checksumAlgorithms {
		if !alg.Composite {
			continue
		}
		t.Run(alg.Name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			putBucket(t, srv, "cks")

			w := s3Request(t, srv, http.MethodPost, "/cks/big?uploads", nil, map[string]string{
				"x-amz-checksum-algorithm": alg.Name,
			})
			require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
			assert.Equal(t, alg.Name, w.Header().Get("x-amz-checksum-algorithm"))
			assert.Equal(t, "COMPOSITE", w.Header().Get("x-amz-checksum-type"),
				"an absent x-amz-checksum-type defaults to COMPOSITE")
			uploadID := uploadIDOf(t, w.Body.String())

			var etags []string
			for i, part := range parts {
				num := i + 1
				w = s3Request(t, srv, http.MethodPut,
					fmt.Sprintf("/cks/big?partNumber=%d&uploadId=%s", num, uploadID), part, nil)
				require.Equal(t, http.StatusOK, w.Code, "upload part %d: %s", num, w.Body.String())
				assert.Equal(t, digestFor(t, alg.Name, part), w.Header().Get(alg.Header),
					"UploadPart echoes the part checksum")
				etags = append(etags, w.Header().Get("ETag"))
			}

			want := compositeChecksum(t, alg.Name, parts)

			w = s3Request(t, srv, http.MethodPost, "/cks/big?uploadId="+uploadID,
				completeBody(etags...), nil)
			require.Equal(t, http.StatusOK, w.Code, "complete: %s", w.Body.String())
			assert.Contains(t, w.Body.String(), "<Checksum"+alg.Name+">"+want+"</Checksum"+alg.Name+">",
				"Complete returns the checksum as an XML element, not a header")
			assert.Contains(t, w.Body.String(), "<ChecksumType>COMPOSITE</ChecksumType>")

			// The "-N" suffix form is the property the reporter asked for explicitly.
			assert.True(t, strings.HasSuffix(want, "-2"), "fixture sanity: two parts")

			w = s3Request(t, srv, http.MethodGet, "/cks/big", nil, map[string]string{
				"x-amz-checksum-mode": "ENABLED",
			})
			require.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, want, w.Header().Get(alg.Header))
			assert.Equal(t, "COMPOSITE", w.Header().Get("x-amz-checksum-type"))
		})
	}
}

// TestS3Checksum_Multipart_FullObject asserts a FULL_OBJECT multipart checksum is
// the digest of every byte of the assembled object, with no "-N" suffix — and that
// it equals what a single-part PUT of the same bytes would produce.
//
// That equality is the invariant the reporter's second bug violated: a single-part
// and a multipart writer must agree on the checksum of identical content.
func TestS3Checksum_Multipart_FullObject(t *testing.T) {
	parts := multipartPartBodies()
	var combined []byte
	for _, p := range parts {
		combined = append(combined, p...)
	}

	for _, alg := range checksumAlgorithms {
		if !alg.FullObject {
			continue
		}
		t.Run(alg.Name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			putBucket(t, srv, "cks")

			w := s3Request(t, srv, http.MethodPost, "/cks/big?uploads", nil, map[string]string{
				"x-amz-checksum-algorithm": alg.Name,
				"x-amz-checksum-type":      "FULL_OBJECT",
			})
			require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
			assert.Equal(t, "FULL_OBJECT", w.Header().Get("x-amz-checksum-type"))
			uploadID := uploadIDOf(t, w.Body.String())

			var etags []string
			for i, part := range parts {
				num := i + 1
				w = s3Request(t, srv, http.MethodPut,
					fmt.Sprintf("/cks/big?partNumber=%d&uploadId=%s", num, uploadID), part, nil)
				require.Equal(t, http.StatusOK, w.Code)
				etags = append(etags, w.Header().Get("ETag"))
			}

			want := digestFor(t, alg.Name, combined)

			w = s3Request(t, srv, http.MethodPost, "/cks/big?uploadId="+uploadID,
				completeBody(etags...), nil)
			require.Equal(t, http.StatusOK, w.Code, "complete: %s", w.Body.String())
			assert.Contains(t, w.Body.String(), "<Checksum"+alg.Name+">"+want+"</Checksum"+alg.Name+">")
			assert.Contains(t, w.Body.String(), "<ChecksumType>FULL_OBJECT</ChecksumType>")
			assert.NotContains(t, want, "-", "a full-object checksum carries no part-count suffix")

			// A single-part PUT of identical bytes must produce an identical value.
			w = s3Request(t, srv, http.MethodPut, "/cks/single", combined, map[string]string{
				"x-amz-sdk-checksum-algorithm": alg.Name,
			})
			require.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, want, w.Header().Get(alg.Header),
				"single-part and full-object multipart checksums of the same bytes must agree")
		})
	}
}

// TestS3Checksum_Multipart_CompositeDiffersFromFullObject pins the distinction the
// two types exist to express: for the same bytes they are different values, so a
// consumer that mixes them up gets a mismatch rather than an accidental pass.
func TestS3Checksum_Multipart_CompositeDiffersFromFullObject(t *testing.T) {
	parts := multipartPartBodies()
	var combined []byte
	for _, p := range parts {
		combined = append(combined, p...)
	}

	composite := compositeChecksum(t, "CRC32", parts)
	fullObject := digestFor(t, "CRC32", combined)
	assert.NotEqual(t, composite, fullObject,
		"COMPOSITE and FULL_OBJECT are computed differently and must not coincide")
}

// TestS3Checksum_Multipart_UnsupportedType asserts the documented support matrix is
// enforced at CreateMultipartUpload — before any part is uploaded, since an upload
// that could never complete against real S3 should fail at creation.
func TestS3Checksum_Multipart_UnsupportedType(t *testing.T) {
	tests := []struct {
		name      string
		algorithm string
		cksType   string
	}{
		{name: "SHA256 full object", algorithm: "SHA256", cksType: "FULL_OBJECT"},
		{name: "SHA1 full object", algorithm: "SHA1", cksType: "FULL_OBJECT"},
		{name: "SHA512 full object", algorithm: "SHA512", cksType: "FULL_OBJECT"},
		{name: "MD5 full object", algorithm: "MD5", cksType: "FULL_OBJECT"},
		{name: "CRC64NVME composite", algorithm: "CRC64NVME", cksType: "COMPOSITE"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			putBucket(t, srv, "cks")

			w := s3Request(t, srv, http.MethodPost, "/cks/big?uploads", nil, map[string]string{
				"x-amz-checksum-algorithm": tc.algorithm,
				"x-amz-checksum-type":      tc.cksType,
			})
			require.Equal(t, http.StatusBadRequest, w.Code)
			assert.Contains(t, w.Body.String(), "<Code>InvalidRequest</Code>")
			assert.Contains(t, w.Body.String(), "<ChecksumAlgorithm>"+tc.algorithm+"</ChecksumAlgorithm>")
			assert.Contains(t, w.Body.String(), "<ChecksumType>"+tc.cksType+"</ChecksumType>")
		})
	}
}

// TestS3Checksum_Multipart_CRC64NVMEDefaultsToFullObject covers the one algorithm
// with no composite form: an absent x-amz-checksum-type must default to FULL_OBJECT
// rather than to the COMPOSITE every other algorithm gets.
func TestS3Checksum_Multipart_CRC64NVMEDefaultsToFullObject(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "cks")

	w := s3Request(t, srv, http.MethodPost, "/cks/big?uploads", nil, map[string]string{
		"x-amz-checksum-algorithm": "CRC64NVME",
	})
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
	assert.Equal(t, "FULL_OBJECT", w.Header().Get("x-amz-checksum-type"))
}

// TestS3Checksum_Multipart_PartAlgorithmMismatch asserts a part supplying a
// checksum under a different algorithm than the upload's is rejected — its digest
// could not contribute to the object's checksum.
func TestS3Checksum_Multipart_PartAlgorithmMismatch(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "cks")

	w := s3Request(t, srv, http.MethodPost, "/cks/big?uploads", nil, map[string]string{
		"x-amz-checksum-algorithm": "SHA256",
	})
	require.Equal(t, http.StatusOK, w.Code)
	uploadID := uploadIDOf(t, w.Body.String())

	part := []byte("a part checksummed the wrong way")
	w = s3Request(t, srv, http.MethodPut, "/cks/big?partNumber=1&uploadId="+uploadID, part,
		map[string]string{"x-amz-checksum-crc32": digestFor(t, "CRC32", part)})
	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "<Code>InvalidRequest</Code>")
	assert.Contains(t, w.Body.String(), "<UploadChecksumAlgorithm>SHA256</UploadChecksumAlgorithm>")
}

// TestS3Checksum_Multipart_PartBadDigest asserts a part whose supplied checksum is
// wrong is rejected and not stored, so a later Complete cannot pick it up.
func TestS3Checksum_Multipart_PartBadDigest(t *testing.T) {
	srv, fs := newS3TestServer(t)
	putBucket(t, srv, "cks")

	w := s3Request(t, srv, http.MethodPost, "/cks/big?uploads", nil, map[string]string{
		"x-amz-checksum-algorithm": "SHA256",
	})
	require.Equal(t, http.StatusOK, w.Code)
	uploadID := uploadIDOf(t, w.Body.String())

	part := []byte("a corrupt part")
	w = s3Request(t, srv, http.MethodPut, "/cks/big?partNumber=1&uploadId="+uploadID, part,
		map[string]string{"x-amz-checksum-sha256": corruptDigest(t, "SHA256", part)})
	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "<Code>BadDigest</Code>")

	exists, err := afero.Exists(fs, fmt.Sprintf("/.multipart/%s/1", uploadID))
	require.NoError(t, err)
	assert.False(t, exists, "rejected part must not be stored")
}

// TestS3Checksum_Multipart_CompleteFullObjectVerified asserts Complete verifies a
// full-object checksum the caller supplies for the whole upload, and rejects a
// wrong one without writing the object.
func TestS3Checksum_Multipart_CompleteFullObjectVerified(t *testing.T) {
	parts := multipartPartBodies()
	var combined []byte
	for _, p := range parts {
		combined = append(combined, p...)
	}

	startUpload := func(t *testing.T, srv *emulator.Server) (string, []string) {
		t.Helper()
		w := s3Request(t, srv, http.MethodPost, "/cks/big?uploads", nil, map[string]string{
			"x-amz-checksum-algorithm": "CRC32",
			"x-amz-checksum-type":      "FULL_OBJECT",
		})
		require.Equal(t, http.StatusOK, w.Code)
		uploadID := uploadIDOf(t, w.Body.String())

		var etags []string
		for i, part := range parts {
			num := i + 1
			w = s3Request(t, srv, http.MethodPut,
				fmt.Sprintf("/cks/big?partNumber=%d&uploadId=%s", num, uploadID), part, nil)
			require.Equal(t, http.StatusOK, w.Code)
			etags = append(etags, w.Header().Get("ETag"))
		}
		return uploadID, etags
	}

	t.Run("correct value accepted", func(t *testing.T) {
		srv, _ := newS3TestServer(t)
		putBucket(t, srv, "cks")
		uploadID, etags := startUpload(t, srv)

		w := s3Request(t, srv, http.MethodPost, "/cks/big?uploadId="+uploadID,
			completeBody(etags...), map[string]string{
				"x-amz-checksum-crc32": digestFor(t, "CRC32", combined),
			})
		require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
	})

	t.Run("wrong value rejected", func(t *testing.T) {
		srv, _ := newS3TestServer(t)
		putBucket(t, srv, "cks")
		uploadID, etags := startUpload(t, srv)

		w := s3Request(t, srv, http.MethodPost, "/cks/big?uploadId="+uploadID,
			completeBody(etags...), map[string]string{
				"x-amz-checksum-crc32": corruptDigest(t, "CRC32", combined),
			})
		require.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "<Code>BadDigest</Code>")

		w = s3Request(t, srv, http.MethodHead, "/cks/big", nil, nil)
		assert.Equal(t, http.StatusNotFound, w.Code, "object must not be written")
	})

	t.Run("disagreeing type rejected", func(t *testing.T) {
		srv, _ := newS3TestServer(t)
		putBucket(t, srv, "cks")
		uploadID, etags := startUpload(t, srv)

		w := s3Request(t, srv, http.MethodPost, "/cks/big?uploadId="+uploadID,
			completeBody(etags...), map[string]string{
				"x-amz-checksum-type": "COMPOSITE",
			})
		require.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "<Code>BadDigest</Code>")
		assert.Contains(t, w.Body.String(), "<UploadChecksumType>FULL_OBJECT</UploadChecksumType>")
	})
}

// TestS3Checksum_Multipart_NoAlgorithm asserts an upload created without a checksum
// algorithm completes without one, and reports no checksum elements.
func TestS3Checksum_Multipart_NoAlgorithm(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "cks")

	w := s3Request(t, srv, http.MethodPost, "/cks/big?uploads", nil, nil)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, w.Header().Get("x-amz-checksum-algorithm"))
	assert.Empty(t, w.Header().Get("x-amz-checksum-type"))
	uploadID := uploadIDOf(t, w.Body.String())

	var etags []string
	for i, part := range multipartPartBodies() {
		num := i + 1
		w = s3Request(t, srv, http.MethodPut,
			fmt.Sprintf("/cks/big?partNumber=%d&uploadId=%s", num, uploadID), part, nil)
		require.Equal(t, http.StatusOK, w.Code)
		assert.Empty(t, w.Header().Get("x-amz-checksum-sha256"))
		etags = append(etags, w.Header().Get("ETag"))
	}

	w = s3Request(t, srv, http.MethodPost, "/cks/big?uploadId="+uploadID, completeBody(etags...), nil)
	require.Equal(t, http.StatusOK, w.Code)
	assert.NotContains(t, w.Body.String(), "<ChecksumType>")
	assert.NotContains(t, w.Body.String(), "<ChecksumCRC64NVME>")
}

// TestS3Checksum_Multipart_TypeWithoutAlgorithm asserts a checksum type with no
// algorithm to apply to is an InvalidRequest.
func TestS3Checksum_Multipart_TypeWithoutAlgorithm(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "cks")

	w := s3Request(t, srv, http.MethodPost, "/cks/big?uploads", nil, map[string]string{
		"x-amz-checksum-type": "FULL_OBJECT",
	})
	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "<Code>InvalidRequest</Code>")
}

// TestS3Checksum_Multipart_InvalidType asserts a type outside the documented pair
// is an InvalidRequest.
func TestS3Checksum_Multipart_InvalidType(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "cks")

	w := s3Request(t, srv, http.MethodPost, "/cks/big?uploads", nil, map[string]string{
		"x-amz-checksum-algorithm": "CRC32",
		"x-amz-checksum-type":      "PARTIAL",
	})
	require.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "<Code>InvalidRequest</Code>")
	assert.Contains(t, w.Body.String(), "<ChecksumType>PARTIAL</ChecksumType>")
}

// --- CopyObject ---

// TestS3Checksum_CopyObject_RecomputesFullObject asserts a copy carries the source's
// algorithm but recomputes the value as a direct full-object checksum: "with a copy
// command, the checksum of the object is a direct checksum of the full object. If
// the object was originally uploaded using a multipart upload, the checksum value
// changes even though the data doesn't".
func TestS3Checksum_CopyObject_RecomputesFullObject(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "cks")

	body := []byte("copied with its checksum")
	w := s3Request(t, srv, http.MethodPut, "/cks/src", body, map[string]string{
		"x-amz-checksum-sha256": digestFor(t, "SHA256", body),
	})
	require.Equal(t, http.StatusOK, w.Code)

	w = s3Request(t, srv, http.MethodPut, "/cks/dst", nil, map[string]string{
		"x-amz-copy-source": "/cks/src",
	})
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())

	w = s3Request(t, srv, http.MethodGet, "/cks/dst", nil, map[string]string{
		"x-amz-checksum-mode": "ENABLED",
	})
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, digestFor(t, "SHA256", body), w.Header().Get("x-amz-checksum-sha256"),
		"the source's algorithm is kept when none is named")
	assert.Equal(t, "FULL_OBJECT", w.Header().Get("x-amz-checksum-type"))
}

// TestS3Checksum_CopyObject_NewAlgorithm asserts a copy can switch algorithms:
// "you can copy the object and specify whether you want to use the existing
// checksum algorithm or a new one".
func TestS3Checksum_CopyObject_NewAlgorithm(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "cks")

	body := []byte("re-checksummed on copy")
	w := s3Request(t, srv, http.MethodPut, "/cks/src", body, map[string]string{
		"x-amz-checksum-sha256": digestFor(t, "SHA256", body),
	})
	require.Equal(t, http.StatusOK, w.Code)

	w = s3Request(t, srv, http.MethodPut, "/cks/dst", nil, map[string]string{
		"x-amz-copy-source":        "/cks/src",
		"x-amz-checksum-algorithm": "CRC32C",
	})
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())

	w = s3Request(t, srv, http.MethodGet, "/cks/dst", nil, map[string]string{
		"x-amz-checksum-mode": "ENABLED",
	})
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, digestFor(t, "CRC32C", body), w.Header().Get("x-amz-checksum-crc32c"))
	assert.Empty(t, w.Header().Get("x-amz-checksum-sha256"), "the old algorithm is replaced")
}

// TestS3Checksum_CopyObject_CompositeBecomesFullObject asserts the documented
// consequence for a multipart source: copying it changes the checksum value and its
// type, even though the bytes are identical.
func TestS3Checksum_CopyObject_CompositeBecomesFullObject(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "cks")

	parts := multipartPartBodies()
	var combined []byte
	for _, p := range parts {
		combined = append(combined, p...)
	}

	w := s3Request(t, srv, http.MethodPost, "/cks/src?uploads", nil, map[string]string{
		"x-amz-checksum-algorithm": "CRC32",
	})
	require.Equal(t, http.StatusOK, w.Code)
	uploadID := uploadIDOf(t, w.Body.String())

	var etags []string
	for i, part := range parts {
		num := i + 1
		w = s3Request(t, srv, http.MethodPut,
			fmt.Sprintf("/cks/src?partNumber=%d&uploadId=%s", num, uploadID), part, nil)
		require.Equal(t, http.StatusOK, w.Code)
		etags = append(etags, w.Header().Get("ETag"))
	}
	w = s3Request(t, srv, http.MethodPost, "/cks/src?uploadId="+uploadID, completeBody(etags...), nil)
	require.Equal(t, http.StatusOK, w.Code)

	composite := compositeChecksum(t, "CRC32", parts)

	w = s3Request(t, srv, http.MethodPut, "/cks/dst", nil, map[string]string{
		"x-amz-copy-source": "/cks/src",
	})
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())

	w = s3Request(t, srv, http.MethodGet, "/cks/dst", nil, map[string]string{
		"x-amz-checksum-mode": "ENABLED",
	})
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, digestFor(t, "CRC32", combined), w.Header().Get("x-amz-checksum-crc32"))
	assert.NotEqual(t, composite, w.Header().Get("x-amz-checksum-crc32"),
		"the value changes even though the data does not")
	assert.Equal(t, "FULL_OBJECT", w.Header().Get("x-amz-checksum-type"))
}

// TestS3Checksum_CopyObject_UnchecksummedSource asserts a source with no checksum
// yields a destination with none, unless the copy names an algorithm.
func TestS3Checksum_CopyObject_UnchecksummedSource(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "cks")

	body := []byte("no checksum on the source")
	w := s3Request(t, srv, http.MethodPut, "/cks/src", body, nil)
	require.Equal(t, http.StatusOK, w.Code)

	w = s3Request(t, srv, http.MethodPut, "/cks/dst", nil, map[string]string{
		"x-amz-copy-source": "/cks/src",
	})
	require.Equal(t, http.StatusOK, w.Code)

	w = s3Request(t, srv, http.MethodGet, "/cks/dst", nil, map[string]string{
		"x-amz-checksum-mode": "ENABLED",
	})
	require.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, w.Header().Get("x-amz-checksum-type"))

	// Naming an algorithm on the copy adds one.
	w = s3Request(t, srv, http.MethodPut, "/cks/dst2", nil, map[string]string{
		"x-amz-copy-source":        "/cks/src",
		"x-amz-checksum-algorithm": "SHA1",
	})
	require.Equal(t, http.StatusOK, w.Code)

	w = s3Request(t, srv, http.MethodGet, "/cks/dst2", nil, map[string]string{
		"x-amz-checksum-mode": "ENABLED",
	})
	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, digestFor(t, "SHA1", body), w.Header().Get("x-amz-checksum-sha1"))
}
