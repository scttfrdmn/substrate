package emulator_test

import (
	"encoding/json"
	"encoding/xml"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The three headers under test (#492), named here so a test asserts on the same
// strings the plugin emits rather than on a retyped copy of them.
const (
	sseAlgorithmHeader = "x-amz-server-side-encryption"
	sseKeyIDHeader     = "x-amz-server-side-encryption-aws-kms-key-id"
	sseBucketKeyHeader = "x-amz-server-side-encryption-bucket-key-enabled"
)

// assertNoSSE asserts the response carries none of the family. Absence is checked on
// the header map rather than through Get, because Get cannot tell an absent header
// from an empty one and "no encryption" versus "encryption with an empty value" is
// exactly the distinction #492's fourth criterion exists to preserve.
func assertNoSSE(t *testing.T, got http.Header, context string) {
	t.Helper()
	for _, h := range []string{sseAlgorithmHeader, sseKeyIDHeader, sseBucketKeyHeader} {
		_, present := got[http.CanonicalHeaderKey(h)]
		assert.False(t, present, "%s: %s must be absent, not empty", context, h)
	}
}

// getStoredObject reads an object's stored metadata straight out of the state store,
// bypassing every response path.
//
// This is what makes the storage criterion an actual assertion: a header round-trip
// passes if the plugin echoes the request it was handed, whether or not anything was
// persisted. The namespace literal is "s3" because s3Namespace is unexported, and the
// key layout matches what putObject writes.
func getStoredObject(t *testing.T, state emulator.StateManager, bucket, key string) emulator.S3Object {
	t.Helper()
	data, err := state.Get(t.Context(), "s3", "object:"+bucket+"/"+key)
	require.NoError(t, err)
	var obj emulator.S3Object
	require.NoError(t, json.Unmarshal(data, &obj))
	return obj
}

// TestS3_SSE_PutObjectRoundTrip asserts SSE-S3 recorded on a write is echoed by both
// reads. Substrate previously accepted these headers and discarded them, so an object
// written with encryption read back byte-identical to one written without it and a
// consumer could only assert on what their own request carried (#492).
func TestS3_SSE_PutObjectRoundTrip(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sse", nil, nil).Code)

	pw := s3Request(t, srv, http.MethodPut, "/sse/doc", []byte("payload"),
		map[string]string{sseAlgorithmHeader: "AES256"})
	require.Equal(t, http.StatusOK, pw.Code)
	assert.Equal(t, "AES256", pw.Header().Get(sseAlgorithmHeader),
		"PutObject reports the encryption it recorded")

	gw := s3Request(t, srv, http.MethodGet, "/sse/doc", nil, nil)
	require.Equal(t, http.StatusOK, gw.Code)
	assert.Equal(t, "AES256", gw.Header().Get(sseAlgorithmHeader), "GetObject")

	hw := s3Request(t, srv, http.MethodHead, "/sse/doc", nil, nil)
	require.Equal(t, http.StatusOK, hw.Code)
	assert.Equal(t, "AES256", hw.Header().Get(sseAlgorithmHeader), "HeadObject")
}

// TestS3_SSE_KMSKeyIDVerbatim asserts the key ID survives byte-identically for each
// form KMS accepts.
//
// The verbatim echo is a deliberate divergence: real S3 resolves any of these to the
// key ARN. Substrate returns the string the caller sent, because that is the string
// their configuration produced and therefore the assertion they are making — and this
// table is what pins it, since three of the four forms would silently pass under a
// resolver that normalized to an ARN.
func TestS3_SSE_KMSKeyIDVerbatim(t *testing.T) {
	for _, tc := range []struct {
		name  string
		keyID string
	}{
		{"bare UUID", "1234abcd-12ab-34cd-56ef-1234567890ab"},
		{"alias name", "alias/my-substrate-key"},
		{"key ARN", "arn:aws:kms:us-east-1:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab"},
		{"alias ARN", "arn:aws:kms:us-east-1:123456789012:alias/my-substrate-key"},
		// A KMS alias is case-sensitive, so the recorded value must not be folded on
		// the way in or out. Every other row here is lowercase and would survive a
		// strings.ToLower on the capture path.
		{"mixed-case alias name", "alias/Prod-Data-Key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/sse-kms", nil, nil).Code)
			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/sse-kms/obj", []byte("x"), map[string]string{
					sseAlgorithmHeader: "aws:kms",
					sseKeyIDHeader:     tc.keyID,
				}).Code)

			for _, read := range []struct {
				name   string
				method string
			}{{"GetObject", http.MethodGet}, {"HeadObject", http.MethodHead}} {
				w := s3Request(t, srv, read.method, "/sse-kms/obj", nil, nil)
				require.Equal(t, http.StatusOK, w.Code)
				assert.Equal(t, "aws:kms", w.Header().Get(sseAlgorithmHeader), read.name)
				assert.Equal(t, tc.keyID, w.Header().Get(sseKeyIDHeader),
					"%s: the key ID round-trips verbatim, unresolved", read.name)
			}
		})
	}
}

// TestS3_SSE_AlgorithmValuesRecordedVerbatim asserts each documented algorithm token
// is recorded as sent. The valid values are AES256, aws:fsx, aws:kms and aws:kms:dsse;
// substrate validates none of them, since rejecting an unrecognized token is #493's,
// and the two colon-bearing tokens are the ones a naive normalizer would mangle.
func TestS3_SSE_AlgorithmValuesRecordedVerbatim(t *testing.T) {
	for _, algorithm := range []string{"AES256", "aws:fsx", "aws:kms", "aws:kms:dsse"} {
		t.Run(algorithm, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/sse-alg", nil, nil).Code)
			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/sse-alg/obj", []byte("x"),
					map[string]string{sseAlgorithmHeader: algorithm}).Code)

			hw := s3Request(t, srv, http.MethodHead, "/sse-alg/obj", nil, nil)
			require.Equal(t, http.StatusOK, hw.Code)
			assert.Equal(t, algorithm, hw.Header().Get(sseAlgorithmHeader))
		})
	}
}

// TestS3_SSE_BucketKeyEnabled asserts the bucket-key header is echoed only when the
// write enabled it.
//
// Absent and false are different observations, so a write that never mentioned the
// header must produce no header rather than "false" — an SDK that distinguishes a nil
// *bool from a false one would otherwise report the wrong answer. The false and
// garbage rows pin that only the documented value enables it.
func TestS3_SSE_BucketKeyEnabled(t *testing.T) {
	for _, tc := range []struct {
		name string
		sent string
		want string // "" means the header must be absent
	}{
		{"enabled", "true", "true"},
		{"enabled uppercase", "TRUE", "true"},
		{"disabled", "false", ""},
		{"unset", "", ""},
		{"unrecognized", "yes", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/sse-bk", nil, nil).Code)

			headers := map[string]string{sseAlgorithmHeader: "aws:kms"}
			if tc.sent != "" {
				headers[sseBucketKeyHeader] = tc.sent
			}
			require.Equal(t, http.StatusOK,
				s3Request(t, srv, http.MethodPut, "/sse-bk/obj", []byte("x"), headers).Code)

			hw := s3Request(t, srv, http.MethodHead, "/sse-bk/obj", nil, nil)
			require.Equal(t, http.StatusOK, hw.Code)
			if tc.want == "" {
				_, present := hw.Header()[http.CanonicalHeaderKey(sseBucketKeyHeader)]
				assert.False(t, present, "the bucket-key header must be absent, not false")
				return
			}
			assert.Equal(t, tc.want, hw.Header().Get(sseBucketKeyHeader))
		})
	}
}

// TestS3_SSE_AbsentNotEmitted asserts an object written with no SSE headers reads back
// with none. This is the regression guard on every object written before #492 existed,
// and the property that makes recording worth anything: "no header sent" has to stay
// distinguishable from "header sent". A default applied here would erase it, which is
// why bucket default encryption is #493's decision to make deliberately.
func TestS3_SSE_AbsentNotEmitted(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sse-bare", nil, nil).Code)

	pw := s3Request(t, srv, http.MethodPut, "/sse-bare/obj", []byte("plain"), nil)
	require.Equal(t, http.StatusOK, pw.Code)
	assertNoSSE(t, pw.Header(), "PutObject")

	gw := s3Request(t, srv, http.MethodGet, "/sse-bare/obj", nil, nil)
	require.Equal(t, http.StatusOK, gw.Code)
	assertNoSSE(t, gw.Header(), "GetObject")

	hw := s3Request(t, srv, http.MethodHead, "/sse-bare/obj", nil, nil)
	require.Equal(t, http.StatusOK, hw.Code)
	assertNoSSE(t, hw.Header(), "HeadObject")
}

// TestS3_SSE_KeyIDWithoutAlgorithmNotEmitted asserts a key ID sent with no algorithm
// emits nothing. There is no such thing as a KMS key ID on an unencrypted object, and
// #493 rejects the combination outright; until then the write must not report an
// encryption the caller never named.
func TestS3_SSE_KeyIDWithoutAlgorithmNotEmitted(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sse-orphan", nil, nil).Code)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sse-orphan/obj", []byte("x"), map[string]string{
			sseKeyIDHeader:     "alias/orphan",
			sseBucketKeyHeader: "true",
		}).Code)

	hw := s3Request(t, srv, http.MethodHead, "/sse-orphan/obj", nil, nil)
	require.Equal(t, http.StatusOK, hw.Code)
	assertNoSSE(t, hw.Header(), "HeadObject")
}

// TestS3_SSE_AlgorithmWithoutKeyIDEmitsAlgorithmOnly asserts an SSE-KMS write naming
// no key emits the algorithm and no key-ID header. Real S3 protects such an object
// with the AWS managed key and reports the ID only "if present", so there is nothing
// for substrate to echo — and inventing an aws/s3 ARN would be fabricating an
// observation.
func TestS3_SSE_AlgorithmWithoutKeyIDEmitsAlgorithmOnly(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sse-nokey", nil, nil).Code)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sse-nokey/obj", []byte("x"),
			map[string]string{sseAlgorithmHeader: "aws:kms"}).Code)

	hw := s3Request(t, srv, http.MethodHead, "/sse-nokey/obj", nil, nil)
	require.Equal(t, http.StatusOK, hw.Code)
	assert.Equal(t, "aws:kms", hw.Header().Get(sseAlgorithmHeader))
	_, present := hw.Header()[http.CanonicalHeaderKey(sseKeyIDHeader)]
	assert.False(t, present, "no key was named, so there is no key ID to report")
}

// TestS3_SSE_StoredOnObject asserts the values are persisted on the object record,
// read out of the state store rather than off a response.
//
// A header round-trip alone would pass if the plugin echoed the request it was handed
// without storing anything, which is precisely the failure #492's storage criterion
// guards against. Reading state also pins the JSON field names, so an object written
// by this release still reads back after one that renames them.
func TestS3_SSE_StoredOnObject(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	srv, _ := newS3TestServerWithFS(t, state)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sse-state", nil, nil).Code)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sse-state/obj", []byte("x"), map[string]string{
			sseAlgorithmHeader: "aws:kms",
			sseKeyIDHeader:     "alias/stored",
			sseBucketKeyHeader: "true",
		}).Code)

	obj := getStoredObject(t, state, "sse-state", "obj")
	assert.Equal(t, "aws:kms", obj.Algorithm)
	assert.Equal(t, "alias/stored", obj.KMSKeyID)
	assert.True(t, obj.BucketKeyEnabled)
}

// TestS3_SSE_StoredAbsentOnUnencryptedObject asserts an unencrypted write stores the
// zero value, so an object written before this field existed reads back with no
// encryption rather than a fabricated default. That is the state half of
// TestS3_SSE_AbsentNotEmitted, and the reason the fields are omitempty.
func TestS3_SSE_StoredAbsentOnUnencryptedObject(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	srv, _ := newS3TestServerWithFS(t, state)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sse-state-bare", nil, nil).Code)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sse-state-bare/obj", []byte("x"), nil).Code)

	obj := getStoredObject(t, state, "sse-state-bare", "obj")
	assert.Empty(t, obj.Algorithm)
	assert.Empty(t, obj.KMSKeyID)
	assert.False(t, obj.BucketKeyEnabled)
}

// TestS3_SSE_NonCanonicalHeaderNames asserts the headers are read case-insensitively.
//
// Requests reaching a plugin have not always been through net/http's canonicalization
// — substrate builds them in-process too — so the plugin is exercised directly here.
// Sending lowercase names through the server would prove nothing, because Go would
// canonicalize them on the way in.
func TestS3_SSE_NonCanonicalHeaderNames(t *testing.T) {
	p := newS3PluginDirect(t)
	rctx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1"}

	// Operation carries the HTTP verb on entry; the plugin resolves the semantic
	// operation from it and the path, exactly as the server pipeline does.
	_, err := p.HandleRequest(rctx, &emulator.AWSRequest{
		Service: "s3", Operation: http.MethodPut, Path: "/sse-fold",
		Headers: map[string]string{}, Params: map[string]string{},
	})
	require.NoError(t, err)

	_, err = p.HandleRequest(rctx, &emulator.AWSRequest{
		Service:   "s3",
		Operation: http.MethodPut,
		Path:      "/sse-fold/obj",
		Body:      []byte("x"),
		Params:    map[string]string{},
		Headers: map[string]string{
			"X-AMZ-SERVER-SIDE-ENCRYPTION":                    "aws:kms",
			"x-amz-server-side-encryption-aws-kms-key-id":     "alias/folded",
			"X-Amz-Server-Side-Encryption-Bucket-Key-Enabled": "true",
		},
	})
	require.NoError(t, err)

	resp, err := p.HandleRequest(rctx, &emulator.AWSRequest{
		Service: "s3", Operation: http.MethodHead, Path: "/sse-fold/obj",
		Headers: map[string]string{}, Params: map[string]string{},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "aws:kms", resp.Headers[sseAlgorithmHeader])
	assert.Equal(t, "alias/folded", resp.Headers[sseKeyIDHeader])
	assert.Equal(t, "true", resp.Headers[sseBucketKeyHeader])
}

// TestS3_SSE_MultipartRoundTrip asserts the encryption named at CreateMultipartUpload
// reaches the assembled object.
//
// Create is the only place it can be supplied: Complete's request accepts only the
// SSE-C headers, so encryption not carried on the upload record is lost for good —
// the #406 failure shape, which is why the family is one embedded declaration shared
// by both records.
func TestS3_SSE_MultipartRoundTrip(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sse-mpu", nil, nil).Code)

	iw := s3Request(t, srv, http.MethodPost, "/sse-mpu/big?uploads", nil, map[string]string{
		sseAlgorithmHeader: "aws:kms",
		sseKeyIDHeader:     "alias/mpu",
		sseBucketKeyHeader: "true",
	})
	require.Equal(t, http.StatusOK, iw.Code)
	assert.Equal(t, "aws:kms", iw.Header().Get(sseAlgorithmHeader),
		"Create echoes it: Complete accepts no SSE header, so this is the only confirmation")
	assert.Equal(t, "alias/mpu", iw.Header().Get(sseKeyIDHeader))
	assert.Equal(t, "true", iw.Header().Get(sseBucketKeyHeader))

	var ir struct {
		UploadID string `xml:"UploadId"`
	}
	require.NoError(t, xml.Unmarshal(iw.Body.Bytes(), &ir))

	etag := uploadPart(t, srv, "sse-mpu", "big", ir.UploadID, 1, []byte("a single part is exempt from the floor"))
	cw := s3Request(t, srv, http.MethodPost, "/sse-mpu/big?uploadId="+ir.UploadID, completeBody(etag), nil)
	require.Equal(t, http.StatusOK, cw.Code)
	assert.Equal(t, "aws:kms", cw.Header().Get(sseAlgorithmHeader), "Complete")
	assert.Equal(t, "alias/mpu", cw.Header().Get(sseKeyIDHeader), "Complete")
	assert.Equal(t, "true", cw.Header().Get(sseBucketKeyHeader), "Complete")

	hw := s3Request(t, srv, http.MethodHead, "/sse-mpu/big", nil, nil)
	require.Equal(t, http.StatusOK, hw.Code)
	assert.Equal(t, "aws:kms", hw.Header().Get(sseAlgorithmHeader), "HeadObject after Complete")
	assert.Equal(t, "alias/mpu", hw.Header().Get(sseKeyIDHeader), "HeadObject after Complete")
	assert.Equal(t, "true", hw.Header().Get(sseBucketKeyHeader), "HeadObject after Complete")
}

// TestS3_SSE_MultipartAbsentNotEmitted asserts a multipart object created without
// encryption emits none, so the carry through Complete cannot smuggle in empty
// headers.
func TestS3_SSE_MultipartAbsentNotEmitted(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sse-mpu-bare", nil, nil).Code)

	iw := s3Request(t, srv, http.MethodPost, "/sse-mpu-bare/big?uploads", nil, nil)
	require.Equal(t, http.StatusOK, iw.Code)
	assertNoSSE(t, iw.Header(), "CreateMultipartUpload")
	var ir struct {
		UploadID string `xml:"UploadId"`
	}
	require.NoError(t, xml.Unmarshal(iw.Body.Bytes(), &ir))

	etag := uploadPart(t, srv, "sse-mpu-bare", "big", ir.UploadID, 1, []byte("body"))
	cw := s3Request(t, srv, http.MethodPost, "/sse-mpu-bare/big?uploadId="+ir.UploadID, completeBody(etag), nil)
	require.Equal(t, http.StatusOK, cw.Code)
	assertNoSSE(t, cw.Header(), "CompleteMultipartUpload")

	hw := s3Request(t, srv, http.MethodHead, "/sse-mpu-bare/big", nil, nil)
	require.Equal(t, http.StatusOK, hw.Code)
	assertNoSSE(t, hw.Header(), "HeadObject after Complete")
}

// TestS3_SSE_CopyObjectDoesNotInherit pins the scope boundary against #493: a copy
// records no encryption, not the source's.
//
// A copy's encryption comes from the request and, failing that, from the bucket
// default — never from the source. Neither of those exists yet, so a copy reports
// none, which is a stated gap rather than a wrong answer. The test is here so #493
// changes this deliberately: silently inheriting is the live bug #475 found twice, and
// it would otherwise be the easiest thing for a later change to introduce.
func TestS3_SSE_CopyObjectDoesNotInherit(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sse-copy", nil, nil).Code)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sse-copy/src", []byte("x"), map[string]string{
			sseAlgorithmHeader: "aws:kms",
			sseKeyIDHeader:     "alias/source",
		}).Code)

	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sse-copy/dst", nil, map[string]string{
			"x-amz-copy-source": "/sse-copy/src",
		}).Code)

	hw := s3Request(t, srv, http.MethodHead, "/sse-copy/dst", nil, nil)
	require.Equal(t, http.StatusOK, hw.Code)
	assertNoSSE(t, hw.Header(), "CopyObject destination")

	sw := s3Request(t, srv, http.MethodHead, "/sse-copy/src", nil, nil)
	require.Equal(t, http.StatusOK, sw.Code)
	assert.Equal(t, "aws:kms", sw.Header().Get(sseAlgorithmHeader),
		"the source keeps its own encryption")
}

// TestS3_SSE_VersionedObjectRoundTrip asserts the family survives on a versioned
// write, which stores a second copy of the metadata under its own key. A field set on
// the object after the versioned copy is marshaled would be missing from it.
func TestS3_SSE_VersionedObjectRoundTrip(t *testing.T) {
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sse-ver", nil, nil).Code)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/sse-ver?versioning", []byte(
			`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`), nil).Code)

	pw := s3Request(t, srv, http.MethodPut, "/sse-ver/obj", []byte("x"), map[string]string{
		sseAlgorithmHeader: "AES256",
	})
	require.Equal(t, http.StatusOK, pw.Code)
	versionID := pw.Header().Get("x-amz-version-id")
	require.NotEmpty(t, versionID)

	hw := s3Request(t, srv, http.MethodHead, "/sse-ver/obj?versionId="+versionID, nil, nil)
	require.Equal(t, http.StatusOK, hw.Code)
	assert.Equal(t, "AES256", hw.Header().Get(sseAlgorithmHeader))
}
