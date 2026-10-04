package emulator_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// An unrouted S3 sub-resource is refused rather than reinterpreted as the method's default operation
// (#1349). Before the fix, five ordinary teardown calls deleted the bucket, two object-lock calls
// overwrote the object, and ?location answered a listing.

// s3UnroutedBucketDeletes are the published bucket-level DELETE sub-resources substrate does not
// route. Each one used to reach DeleteBucket.
var s3UnroutedBucketDeletes = map[string]string{
	"analytics":             "DeleteBucketAnalyticsConfiguration",
	"encryption":            "DeleteBucketEncryption",
	"intelligent-tiering":   "DeleteBucketIntelligentTieringConfiguration",
	"inventory":             "DeleteBucketInventoryConfiguration",
	"metadataConfiguration": "DeleteBucketMetadataConfiguration",
	"metadataTable":         "DeleteBucketMetadataTableConfiguration",
	"metrics":               "DeleteBucketMetricsConfiguration",
	"ownershipControls":     "DeleteBucketOwnershipControls",
	"replication":           "DeleteBucketReplication",
	"website":               "DeleteBucketWebsite",
}

// s3RequireNotImplemented requires that w is S3's NotImplemented/501 refusal.
func s3RequireNotImplemented(t *testing.T, what string, w *httptest.ResponseRecorder) {
	t.Helper()
	require.Equalf(t, http.StatusNotImplemented, w.Code, "%s must be refused as unimplemented, not reinterpreted: %s", what, w.Body.String())
	require.Containsf(t, w.Body.String(), "NotImplemented", "%s must answer S3's NotImplemented code: %s", what, w.Body.String())
}

// TestS3Subresource_UnroutedDeleteDoesNotDeleteTheBucket is the data-loss regression: every unrouted
// bucket-level DELETE is refused, and the bucket survives it.
func TestS3Subresource_UnroutedDeleteDoesNotDeleteTheBucket(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)

	for sub, op := range s3UnroutedBucketDeletes {
		t.Run(op, func(t *testing.T) {
			// Bucket names are lowercase; several sub-resource keys are not.
			bucket := "survives-" + strings.ToLower(sub)
			if len(bucket) > 63 {
				bucket = bucket[:63]
			}
			created := s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil)
			require.Equal(t, http.StatusOK, created.Code, "CreateBucket: %s", created.Body.String())

			s3RequireNotImplemented(t, op, s3Request(t, srv, http.MethodDelete, "/"+bucket+"?"+sub, nil, nil))

			head := s3Request(t, srv, http.MethodHead, "/"+bucket, nil, nil)
			require.Equalf(t, http.StatusOK, head.Code, "%s deleted the bucket", op)
		})
	}
}

// TestS3Subresource_UnroutedPutDoesNotOverwriteTheObject pins the object-level half: PutObjectRetention
// and PutObjectLegalHold used to reach PutObject and replace the object's bytes with their XML body.
func TestS3Subresource_UnroutedPutDoesNotOverwriteTheObject(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)

	const bucket = "overwrite-probe"
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil).Code)
	original := []byte("the object's own bytes")
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/"+bucket+"/key", original, nil).Code)

	for sub, body := range map[string]string{
		"retention":  `<Retention><Mode>GOVERNANCE</Mode><RetainUntilDate>2030-01-01T00:00:00Z</RetainUntilDate></Retention>`,
		"legal-hold": `<LegalHold><Status>ON</Status></LegalHold>`,
	} {
		t.Run(sub, func(t *testing.T) {
			s3RequireNotImplemented(t, "PUT ?"+sub, s3Request(t, srv, http.MethodPut, "/"+bucket+"/key?"+sub, []byte(body), nil))
			got := s3Request(t, srv, http.MethodGet, "/"+bucket+"/key", nil, nil)
			require.Equal(t, http.StatusOK, got.Code, "GetObject: %s", got.Body.String())
			require.Equalf(t, original, got.Body.Bytes(), "PUT ?%s overwrote the object", sub)
		})
	}
}

// TestS3Subresource_UnroutedRequestsAreRefused covers the other methods: no unrouted sub-resource
// answers a 200 or a listing in place of what it asked for.
func TestS3Subresource_UnroutedRequestsAreRefused(t *testing.T) {
	t.Parallel()
	srv, _ := newS3TestServer(t)

	const bucket = "refusal-probe"
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/"+bucket, nil, nil).Code)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/"+bucket+"/key", []byte("x"), nil).Code)

	for _, tc := range []struct {
		op, method, path string
	}{
		{"GetBucketLocation", http.MethodGet, "/" + bucket + "?location"},
		{"GetBucketEncryption", http.MethodGet, "/" + bucket + "?encryption"},
		{"ListBucketInventoryConfigurations", http.MethodGet, "/" + bucket + "?inventory"},
		{"GetBucketInventoryConfiguration", http.MethodGet, "/" + bucket + "?inventory&id=one"},
		{"CreateSession", http.MethodGet, "/" + bucket + "?session"},
		{"PutBucketEncryption", http.MethodPut, "/" + bucket + "?encryption"},
		{"PutBucketWebsite", http.MethodPut, "/" + bucket + "?website"},
		{"CreateBucketMetadataTableConfiguration", http.MethodPost, "/" + bucket + "?metadataTable"},
		{"GetObjectAttributes", http.MethodGet, "/" + bucket + "/key?attributes"},
		{"GetObjectRetention", http.MethodGet, "/" + bucket + "/key?retention"},
		{"RestoreObject", http.MethodPost, "/" + bucket + "/key?restore"},
	} {
		t.Run(tc.op, func(t *testing.T) {
			s3RequireNotImplemented(t, tc.op, s3Request(t, srv, tc.method, tc.path, nil, nil))
		})
	}
}

// TestS3Subresource_ResolvesTheRealOperationName pins what the pipeline sees. ParseAWSRequest resolves
// the name the request is authorized, metered and recorded under; an unrouted DELETE used to resolve
// to DeleteBucket, so a policy granting s3:DeleteBucketEncryption alone was refused, and one granting
// s3:DeleteBucket alone let the call through.
//
// It also pins that no routed operation changed name: the table is consulted only after every routed
// test.
func TestS3Subresource_ResolvesTheRealOperationName(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		method, target, want string
	}{
		// Unrouted: named for what they are.
		{http.MethodDelete, "/b?encryption", "DeleteBucketEncryption"},
		{http.MethodGet, "/b?location", "GetBucketLocation"},
		{http.MethodGet, "/b?metrics", "ListBucketMetricsConfigurations"},
		{http.MethodGet, "/b?metrics&id=m1", "GetBucketMetricsConfiguration"},
		{http.MethodPut, "/b/k?retention", "PutObjectRetention"},
		{http.MethodPost, "/b/k?restore", "RestoreObject"},
		// Routed: unchanged. ?cors is routed since #1278, and an SDK's "?cors=" resolves the same.
		{http.MethodDelete, "/b?cors=", "DeleteBucketCors"},
		{http.MethodGet, "/b?cors", "GetBucketCors"},
		{http.MethodPut, "/b?cors", "PutBucketCors"},
		{http.MethodDelete, "/b", "DeleteBucket"},
		{http.MethodDelete, "/b?policy", "DeleteBucketPolicy"},
		{http.MethodDelete, "/b?publicAccessBlock", "DeletePublicAccessBlock"},
		{http.MethodPut, "/b", "CreateBucket"},
		{http.MethodPut, "/b?tagging", "PutBucketTagging"},
		{http.MethodGet, "/b", "ListObjects"},
		{http.MethodGet, "/b?prefix=a/&delimiter=/", "ListObjects"},
		{http.MethodGet, "/b?list-type=2&start-after=k", "ListObjectsV2"},
		{http.MethodGet, "/b?versioning", "GetBucketVersioning"},
		{http.MethodGet, "/b/k", "GetObject"},
		{http.MethodPut, "/b/k", "PutObject"},
		{http.MethodPut, "/b/k?tagging", "PutObjectTagging"},
		{http.MethodDelete, "/b/k", "DeleteObject"},
	} {
		t.Run(tc.method+" "+tc.target, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.target, bytes.NewReader(nil))
			r.Host = "s3.us-east-1.amazonaws.com"
			req, _, err := emulator.ParseAWSRequest(r)
			require.NoError(t, err, "ParseAWSRequest %s %s", tc.method, tc.target)
			require.Equal(t, tc.want, req.Operation, "%s %s", tc.method, tc.target)
		})
	}
}
