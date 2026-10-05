package emulator_test

import (
	"encoding/xml"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// S3 server-side-encryption resolution (#493): bucket default encryption, CopyObject's
// non-inheritance, multipart, and the four refused combinations. Assertions are on the raw
// XML and on raw headers, so a member renamed or dropped on the way through is visible.

// sseKMSDefaultBody is a PutBucketEncryption body setting SSE-KMS with a key and S3 Bucket Keys.
const sseKMSDefaultBody = `<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
	`<Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>aws:kms</SSEAlgorithm>` +
	`<KMSMasterKeyID>arn:aws:kms:us-east-1:123456789012:key/default</KMSMasterKeyID>` +
	`</ApplyServerSideEncryptionByDefault><BucketKeyEnabled>true</BucketKeyEnabled></Rule>` +
	`</ServerSideEncryptionConfiguration>`

// sseKMSDefaultKey is the key sseKMSDefaultBody configures.
const sseKMSDefaultKey = "arn:aws:kms:us-east-1:123456789012:key/default"

// sseBucket creates bucket on srv, optionally setting its default encryption to body.
func sseBucket(t *testing.T, srv *emulator.Server, bucket, body string) {
	t.Helper()
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil).Code)
	if body != "" {
		w := s3Request(t, srv, http.MethodPut, "/"+bucket+"?encryption", []byte(body), nil)
		require.Equal(t, http.StatusOK, w.Code, "PutBucketEncryption: %s", w.Body.String())
	}
}

// assertSSE asserts the three encryption headers, "" meaning absent.
func assertSSE(t *testing.T, got http.Header, context, algorithm, keyID, bucketKey string) {
	t.Helper()
	assert.Equal(t, algorithm, got.Get(sseAlgorithmHeader), "%s: algorithm", context)
	for h, want := range map[string]string{sseKeyIDHeader: keyID, sseBucketKeyHeader: bucketKey} {
		v, present := got[http.CanonicalHeaderKey(h)]
		if want == "" {
			assert.Falsef(t, present, "%s: %s must be absent, got %v", context, h, v)
			continue
		}
		assert.Equalf(t, want, got.Get(h), "%s: %s", context, h)
	}
}

// requireInvalidArgument requires an InvalidArgument/400 refusal.
func requireInvalidArgument(t *testing.T, context string, code int, body string) {
	t.Helper()
	require.Equalf(t, http.StatusBadRequest, code, "%s: %s", context, body)
	require.Containsf(t, body, "<Code>InvalidArgument</Code>", "%s", context)
}

func TestS3BucketEncryption_RoundTripsAndResetsToTheSSES3Default(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	sseBucket(t, srv, "enc-rt", "")

	// Every bucket has a default, so an unconfigured one answers SSE-S3, not a not-found.
	w := s3Request(t, srv, http.MethodGet, "/enc-rt?encryption", nil, nil)
	require.Equal(t, http.StatusOK, w.Code, "%s", w.Body.String())
	assert.Contains(t, w.Body.String(), "<SSEAlgorithm>AES256</SSEAlgorithm>")
	assert.Contains(t, w.Body.String(), "<BucketKeyEnabled>false</BucketKeyEnabled>")
	assert.NotContains(t, w.Body.String(), "ServerSideEncryptionConfigurationNotFoundError")

	w = s3Request(t, srv, http.MethodPut, "/enc-rt?encryption", []byte(sseKMSDefaultBody), nil)
	require.Equal(t, http.StatusOK, w.Code, "%s", w.Body.String())

	w = s3Request(t, srv, http.MethodGet, "/enc-rt?encryption", nil, nil)
	require.Equal(t, http.StatusOK, w.Code)
	for _, want := range []string{
		"<ServerSideEncryptionConfiguration", "<Rule>", "<SSEAlgorithm>aws:kms</SSEAlgorithm>",
		"<KMSMasterKeyID>" + sseKMSDefaultKey + "</KMSMasterKeyID>", "<BucketKeyEnabled>true</BucketKeyEnabled>",
	} {
		assert.Containsf(t, w.Body.String(), want, "GetBucketEncryption answers what was put")
	}

	// DeleteBucketEncryption "resets the default encryption for the bucket as ... SSE-S3", and is
	// idempotent.
	for i := range 2 {
		w = s3Request(t, srv, http.MethodDelete, "/enc-rt?encryption", nil, nil)
		require.Equalf(t, http.StatusNoContent, w.Code, "DeleteBucketEncryption #%d: %s", i+1, w.Body.String())
	}
	w = s3Request(t, srv, http.MethodGet, "/enc-rt?encryption", nil, nil)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "<SSEAlgorithm>AES256</SSEAlgorithm>", "reset to SSE-S3")
	assert.NotContains(t, w.Body.String(), "KMSMasterKeyID")
}

func TestS3BucketEncryption_AMissingBucketIsNoSuchBucket(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	for _, method := range []string{http.MethodPut, http.MethodGet, http.MethodDelete} {
		w := s3Request(t, srv, method, "/enc-missing?encryption", []byte(sseKMSDefaultBody), nil)
		assert.Equalf(t, http.StatusNotFound, w.Code, "%s ?encryption", method)
		assert.Containsf(t, w.Body.String(), "NoSuchBucket", "%s ?encryption", method)
	}
}

func TestS3BucketEncryption_DeleteBucketRemovesTheConfiguration(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	sseBucket(t, srv, "enc-inherit", sseKMSDefaultBody)
	require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete, "/enc-inherit", nil, nil).Code)
	sseBucket(t, srv, "enc-inherit", "")

	w := s3Request(t, srv, http.MethodGet, "/enc-inherit?encryption", nil, nil)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "<SSEAlgorithm>AES256</SSEAlgorithm>",
		"a re-created bucket must not inherit its predecessor's default")
}

func TestS3BucketEncryption_PutRefusesWhatThePagesRefuse(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, body, code string
	}{
		{"a body that does not parse", "<ServerSideEncryptionConfiguration><Rule>", "MalformedXML"},
		{"no Rule", "<ServerSideEncryptionConfiguration></ServerSideEncryptionConfiguration>", "MalformedXML"},
		{"an unpublished algorithm", `<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES-256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`, "InvalidArgument"},
		{"a key beside AES256", `<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm><KMSMasterKeyID>alias/x</KMSMasterKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`, "InvalidArgument"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, _ := newS3TestServer(t)
			sseBucket(t, srv, "enc-refuse", "")
			w := s3Request(t, srv, http.MethodPut, "/enc-refuse?encryption", []byte(tc.body), nil)
			require.Equal(t, http.StatusBadRequest, w.Code, "%s", w.Body.String())
			assert.Contains(t, w.Body.String(), "<Code>"+tc.code+"</Code>")

			got := s3Request(t, srv, http.MethodGet, "/enc-refuse?encryption", nil, nil)
			assert.Contains(t, got.Body.String(), "<SSEAlgorithm>AES256</SSEAlgorithm>", "a refused put stores nothing")
		})
	}
}

// The examples on API_PutBucketEncryption spell the key member KMSKeyID; the member is
// KMSMasterKeyID. Both are accepted, and the member's own name is answered.
func TestS3BucketEncryption_AcceptsTheExamplesKMSKeyIDSpelling(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	sseBucket(t, srv, "enc-spelling", `<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault>`+
		`<SSEAlgorithm>aws:kms</SSEAlgorithm><KMSKeyID>alias/example</KMSKeyID></ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>`)
	w := s3Request(t, srv, http.MethodGet, "/enc-spelling?encryption", nil, nil)
	assert.Contains(t, w.Body.String(), "<KMSMasterKeyID>alias/example</KMSMasterKeyID>")
}

func TestS3BucketEncryption_AWriteNamingNothingTakesTheBucketDefault(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	sseBucket(t, srv, "enc-default", sseKMSDefaultBody)

	pw := s3Request(t, srv, http.MethodPut, "/enc-default/obj", []byte("x"), nil)
	require.Equal(t, http.StatusOK, pw.Code, "%s", pw.Body.String())
	assertSSE(t, pw.Header(), "PutObject", "aws:kms", sseKMSDefaultKey, "true")
	hw := s3Request(t, srv, http.MethodHead, "/enc-default/obj", nil, nil)
	assertSSE(t, hw.Header(), "HeadObject", "aws:kms", sseKMSDefaultKey, "true")

	// A request naming an algorithm takes nothing from the default.
	ow := s3Request(t, srv, http.MethodPut, "/enc-default/own", []byte("x"), map[string]string{sseAlgorithmHeader: "AES256"})
	require.Equal(t, http.StatusOK, ow.Code)
	assertSSE(t, ow.Header(), "PutObject naming AES256", "AES256", "", "")

	// aws:kms with no key is the AWS managed key, not the bucket default's key.
	kw := s3Request(t, srv, http.MethodPut, "/enc-default/managed", []byte("x"), map[string]string{sseAlgorithmHeader: "aws:kms"})
	require.Equal(t, http.StatusOK, kw.Code)
	assert.Empty(t, kw.Header().Get(sseKeyIDHeader), "the managed key reports no ID")

	// Changing the default later does not rewrite objects already written.
	require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete, "/enc-default?encryption", nil, nil).Code)
	hw = s3Request(t, srv, http.MethodHead, "/enc-default/obj", nil, nil)
	assertSSE(t, hw.Header(), "HeadObject after the default changed", "aws:kms", sseKMSDefaultKey, "true")
}

// An in-place CopyObject for a metadata change, the case #475's reporter hit: the source is
// SSE-KMS under a customer managed key, the copy names no encryption, and the destination is
// the source's own key. The copy takes the bucket default, SSE-S3, and the object leaves its key.
func TestS3BucketEncryption_AnInPlaceMetadataCopyTakesTheBucketDefaultNotTheSources(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	sseBucket(t, srv, "enc-inplace", "")
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/enc-inplace/obj", []byte("x"), map[string]string{
		sseAlgorithmHeader: "aws:kms", sseKeyIDHeader: "alias/customer",
	}).Code)

	cw := s3Request(t, srv, http.MethodPut, "/enc-inplace/obj", nil, map[string]string{
		"x-amz-copy-source":        "/enc-inplace/obj",
		"x-amz-metadata-directive": "REPLACE",
		"x-amz-meta-reviewed":      "yes",
	})
	require.Equal(t, http.StatusOK, cw.Code, "%s", cw.Body.String())
	assertSSE(t, cw.Header(), "CopyObject response", "AES256", "", "")
	hw := s3Request(t, srv, http.MethodHead, "/enc-inplace/obj", nil, nil)
	assertSSE(t, hw.Header(), "HeadObject after the in-place copy", "AES256", "", "")

	// Restating the key keeps it.
	cw = s3Request(t, srv, http.MethodPut, "/enc-inplace/obj", nil, map[string]string{
		"x-amz-copy-source": "/enc-inplace/obj", "x-amz-metadata-directive": "REPLACE",
		sseAlgorithmHeader: "aws:kms", sseKeyIDHeader: "alias/customer",
	})
	require.Equal(t, http.StatusOK, cw.Code, "%s", cw.Body.String())
	assertSSE(t, cw.Header(), "CopyObject restating the key", "aws:kms", "alias/customer", "")
}

func TestS3BucketEncryption_ACopyTakesTheDestinationBucketsDefault(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	sseBucket(t, srv, "enc-src", "")
	sseBucket(t, srv, "enc-dst", sseKMSDefaultBody)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/enc-src/obj", []byte("x"), nil).Code)

	cw := s3Request(t, srv, http.MethodPut, "/enc-dst/obj", nil, map[string]string{"x-amz-copy-source": "/enc-src/obj"})
	require.Equal(t, http.StatusOK, cw.Code, "%s", cw.Body.String())
	assertSSE(t, cw.Header(), "CopyObject into a KMS-default bucket", "aws:kms", sseKMSDefaultKey, "true")
}

func TestS3BucketEncryption_MultipartRecordsItOnCreateOnly(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	sseBucket(t, srv, "enc-mpu", sseKMSDefaultBody)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/enc-mpu/src", []byte("copied part"), nil).Code)

	iw := s3Request(t, srv, http.MethodPost, "/enc-mpu/big?uploads", nil, nil)
	require.Equal(t, http.StatusOK, iw.Code, "%s", iw.Body.String())
	assertSSE(t, iw.Header(), "CreateMultipartUpload", "aws:kms", sseKMSDefaultKey, "true")
	var ir struct {
		UploadID string `xml:"UploadId"`
	}
	require.NoError(t, xml.Unmarshal(iw.Body.Bytes(), &ir))
	partPath := "/enc-mpu/big?partNumber=1&uploadId=" + ir.UploadID

	// A part restating encryption is refused, even when it restates the upload's own.
	for _, h := range []map[string]string{
		{sseAlgorithmHeader: "aws:kms"},
		{sseKeyIDHeader: sseKMSDefaultKey},
		{sseBucketKeyHeader: "true"},
	} {
		w := s3Request(t, srv, http.MethodPut, partPath, []byte("part"), h)
		requireInvalidArgument(t, "UploadPart restating encryption", w.Code, w.Body.String())
		h["x-amz-copy-source"] = "/enc-mpu/src"
		w = s3Request(t, srv, http.MethodPut, partPath, nil, h)
		requireInvalidArgument(t, "UploadPartCopy restating encryption", w.Code, w.Body.String())
	}

	pw := s3Request(t, srv, http.MethodPut, partPath, []byte("a single part is exempt from the floor"), nil)
	require.Equal(t, http.StatusOK, pw.Code, "%s", pw.Body.String())
	assertSSE(t, pw.Header(), "UploadPart", "aws:kms", sseKMSDefaultKey, "true")

	cw := s3Request(t, srv, http.MethodPost, "/enc-mpu/big?uploadId="+ir.UploadID, completeBody(pw.Header().Get("ETag")), nil)
	require.Equal(t, http.StatusOK, cw.Code, "%s", cw.Body.String())
	assertSSE(t, cw.Header(), "CompleteMultipartUpload", "aws:kms", sseKMSDefaultKey, "true")
	hw := s3Request(t, srv, http.MethodHead, "/enc-mpu/big", nil, nil)
	assertSSE(t, hw.Header(), "HeadObject after Complete", "aws:kms", sseKMSDefaultKey, "true")
}

func TestS3_SSE_RefusesTheFourInvalidCombinations(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		headers map[string]string
	}{
		{"a key ID with AES256", map[string]string{sseAlgorithmHeader: "AES256", sseKeyIDHeader: "alias/x"}},
		{"a Bucket Key without aws:kms", map[string]string{sseAlgorithmHeader: "AES256", sseBucketKeyHeader: "true"}},
		{"a Bucket Key with aws:kms:dsse", map[string]string{sseAlgorithmHeader: "aws:kms:dsse", sseBucketKeyHeader: "true"}},
		{"an unrecognized algorithm", map[string]string{sseAlgorithmHeader: "AES-256"}},
		{"a lower-case algorithm", map[string]string{sseAlgorithmHeader: "aes256"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, _ := newS3TestServer(t)
			sseBucket(t, srv, "enc-combos", "")
			require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/enc-combos/src", []byte("x"), nil).Code)

			w := s3Request(t, srv, http.MethodPut, "/enc-combos/put", []byte("x"), tc.headers)
			requireInvalidArgument(t, "PutObject", w.Code, w.Body.String())
			assert.Equal(t, http.StatusNotFound, s3Request(t, srv, http.MethodHead, "/enc-combos/put", nil, nil).Code, "a refused put writes nothing")

			copyHeaders := map[string]string{"x-amz-copy-source": "/enc-combos/src"}
			for k, v := range tc.headers {
				copyHeaders[k] = v
			}
			w = s3Request(t, srv, http.MethodPut, "/enc-combos/copy", nil, copyHeaders)
			requireInvalidArgument(t, "CopyObject", w.Code, w.Body.String())
			assert.Equal(t, http.StatusNotFound, s3Request(t, srv, http.MethodHead, "/enc-combos/copy", nil, nil).Code, "a refused copy writes nothing")

			w = s3Request(t, srv, http.MethodPost, "/enc-combos/mpu?uploads", nil, tc.headers)
			requireInvalidArgument(t, "CreateMultipartUpload", w.Code, w.Body.String())
		})
	}
}

// The key-ID and Bucket Key rules are evaluated against the algorithm the write resolves to, so
// a request naming only those headers is accepted where the bucket default is SSE-KMS.
func TestS3_SSE_KeyIDAloneIsAcceptedUnderAKMSDefault(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)
	sseBucket(t, srv, "enc-keyonly", sseKMSDefaultBody)
	w := s3Request(t, srv, http.MethodPut, "/enc-keyonly/obj", []byte("x"), map[string]string{sseKeyIDHeader: "alias/override"})
	require.Equal(t, http.StatusOK, w.Code, "%s", w.Body.String())
	assertSSE(t, w.Header(), "PutObject naming only a key under a KMS default", "aws:kms", "alias/override", "true")
}

// A store fault at any read or write the encryption paths make is an error, never answered as
// the default or as a success.
func TestS3BucketEncryption_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		arm    func(*cfFaultStateManager)
		method string
		path   string
		body   []byte
	}{
		{"GetBucketEncryption, read", func(m *cfFaultStateManager) { m.failGet = "bucket_encryption:" }, http.MethodGet, "/enc-fault?encryption", nil},
		{"GetBucketEncryption, corrupt record", func(m *cfFaultStateManager) { m.corruptGet = "bucket_encryption:" }, http.MethodGet, "/enc-fault?encryption", nil},
		{"PutBucketEncryption, write", func(m *cfFaultStateManager) { m.failPut = "bucket_encryption:" }, http.MethodPut, "/enc-fault?encryption", []byte(sseKMSDefaultBody)},
		{"PutBucketEncryption, bucket read", func(m *cfFaultStateManager) { m.failGet = "bucket:enc-fault" }, http.MethodPut, "/enc-fault?encryption", []byte(sseKMSDefaultBody)},
		{"DeleteBucketEncryption, delete", func(m *cfFaultStateManager) { m.failDelete = "bucket_encryption:" }, http.MethodDelete, "/enc-fault?encryption", nil},
		{"PutObject, default read", func(m *cfFaultStateManager) { m.failGet = "bucket_encryption:" }, http.MethodPut, "/enc-fault/obj", []byte("x")},
		{"CreateMultipartUpload, default read", func(m *cfFaultStateManager) { m.failGet = "bucket_encryption:" }, http.MethodPost, "/enc-fault/big?uploads", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			srv := newS3TestServerWithState(t, fault)
			sseBucket(t, srv, "enc-fault", sseKMSDefaultBody)

			tc.arm(fault)
			w := s3Request(t, srv, tc.method, tc.path, tc.body, nil)
			require.GreaterOrEqualf(t, w.Code, http.StatusInternalServerError, "%s must fail on a store fault: %s", tc.name, w.Body.String())
			assert.False(t, strings.Contains(w.Body.String(), "<SSEAlgorithm>AES256</SSEAlgorithm>"), "%s must not answer the default over a fault", tc.name)
		})
	}
}
