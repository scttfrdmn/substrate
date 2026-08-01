package emulator_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// pabConfig is the PublicAccessBlockConfiguration a test writes and reads back.
type pabConfig struct {
	XMLName               xml.Name `xml:"PublicAccessBlockConfiguration"`
	BlockPublicAcls       bool     `xml:"BlockPublicAcls"`
	IgnorePublicAcls      bool     `xml:"IgnorePublicAcls"`
	BlockPublicPolicy     bool     `xml:"BlockPublicPolicy"`
	RestrictPublicBuckets bool     `xml:"RestrictPublicBuckets"`
}

// pabAllTrue is the body every SDK sends to lock a bucket down, and what
// parsl-ephemeral-aws sends immediately after CreateBucket (#446).
const pabAllTrue = `<?xml version="1.0" encoding="UTF-8"?>
<PublicAccessBlockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <BlockPublicAcls>true</BlockPublicAcls>
  <IgnorePublicAcls>true</IgnorePublicAcls>
  <BlockPublicPolicy>true</BlockPublicPolicy>
  <RestrictPublicBuckets>true</RestrictPublicBuckets>
</PublicAccessBlockConfiguration>`

// newPABBucket creates a bucket for a public-access-block test and returns its name.
func newPABBucket(t *testing.T, srv *emulator.Server, name string) string {
	t.Helper()
	w := s3Request(t, srv, http.MethodPut, "/"+name, nil, nil)
	require.Equal(t, http.StatusOK, w.Code, "CreateBucket %s: %s", name, w.Body.String())
	return name
}

// putPAB sends PutPublicAccessBlock and returns the recorder.
func putPAB(t *testing.T, srv *emulator.Server, bucket, body string) *httptest.ResponseRecorder {
	t.Helper()
	return s3Request(t, srv, http.MethodPut, "/"+bucket+"?publicAccessBlock", []byte(body), nil)
}

// getPAB reads a bucket's configuration back, requiring a 200.
func getPAB(t *testing.T, srv *emulator.Server, bucket string) pabConfig {
	t.Helper()
	w := s3Request(t, srv, http.MethodGet, "/"+bucket+"?publicAccessBlock", nil, nil)
	require.Equal(t, http.StatusOK, w.Code, "GetPublicAccessBlock: %s", w.Body.String())

	var cfg pabConfig
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &cfg), "decode config: %s", w.Body.String())
	return cfg
}

// bucketExists reports whether ListBuckets still names the bucket. #446's whole
// severity is that DELETE ?publicAccessBlock destroyed it, so the assertion has
// to be made against the same surface the reporter used.
func bucketExists(t *testing.T, srv *emulator.Server, bucket string) bool {
	t.Helper()
	w := s3Request(t, srv, http.MethodGet, "/", nil, nil)
	require.Equal(t, http.StatusOK, w.Code, "ListBuckets: %s", w.Body.String())

	var out struct {
		Names []string `xml:"Buckets>Bucket>Name"`
	}
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &out), "decode ListBuckets: %s", w.Body.String())

	for _, n := range out.Names {
		if n == bucket {
			return true
		}
	}
	return false
}

// TestS3_PublicAccessBlock_RoundTrip is the core of #446: the subresource was
// unrouted, so nothing could be written and nothing could be read back.
//
// The GET assertion is what proves PUT was routed rather than merely accepted. A
// PUT that reached CreateBucket also "succeeded" in the reporter's sense of not
// crashing, so asserting the PUT's status alone would still pass against the bug
// on a bucket that did not already exist.
func TestS3_PublicAccessBlock_RoundTrip(t *testing.T) {
	srv, _ := newS3TestServer(t)
	bucket := newPABBucket(t, srv, "pab-roundtrip")

	resp := putPAB(t, srv, bucket, pabAllTrue)
	// 200 with an empty body, per the PutPublicAccessBlock reference — the
	// reporter saw 409 BucketAlreadyExists here, CreateBucket's answer.
	assert.Equal(t, http.StatusOK, resp.Code, "body: %s", resp.Body.String())
	assert.Empty(t, resp.Body.String(), "PutPublicAccessBlock returns no body")

	assert.Equal(t, pabConfig{
		XMLName:               xml.Name{Space: "http://s3.amazonaws.com/doc/2006-03-01/", Local: "PublicAccessBlockConfiguration"},
		BlockPublicAcls:       true,
		IgnorePublicAcls:      true,
		BlockPublicPolicy:     true,
		RestrictPublicBuckets: true,
	}, getPAB(t, srv, bucket))
}

// TestS3_PublicAccessBlock_DeleteKeepsBucket is the regression guard for the
// data-loss shape. Both the empty and the non-empty case are covered because the
// fall-through produced two different wrong answers: a silent 204 that took the
// bucket with it, and a 409 BucketNotEmpty naming an operation the caller never
// issued.
func TestS3_PublicAccessBlock_DeleteKeepsBucket(t *testing.T) {
	tests := []struct {
		name    string
		bucket  string
		withObj bool
	}{
		{
			name:   "empty bucket survives (was: silently deleted)",
			bucket: "pab-del-empty",
		},
		{
			name:    "non-empty bucket survives (was: 409 BucketNotEmpty)",
			bucket:  "pab-del-nonempty",
			withObj: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			newPABBucket(t, srv, tt.bucket)

			if tt.withObj {
				w := s3Request(t, srv, http.MethodPut, "/"+tt.bucket+"/important.txt", []byte("payload"), nil)
				require.Equal(t, http.StatusOK, w.Code, "PutObject: %s", w.Body.String())
			}

			require.Equal(t, http.StatusOK, putPAB(t, srv, tt.bucket, pabAllTrue).Code)

			w := s3Request(t, srv, http.MethodDelete, "/"+tt.bucket+"?publicAccessBlock", nil, nil)
			assert.Equal(t, http.StatusNoContent, w.Code, "DeletePublicAccessBlock: %s", w.Body.String())

			assert.True(t, bucketExists(t, srv, tt.bucket),
				"the bucket was destroyed by DELETE ?publicAccessBlock")

			// The object has to survive too: reaching DeleteBucket on an empty
			// bucket is the loss, but a bucket that still lists while its contents
			// are gone would be the same defect one layer down.
			if tt.withObj {
				got := s3Request(t, srv, http.MethodGet, "/"+tt.bucket+"/important.txt", nil, nil)
				assert.Equal(t, http.StatusOK, got.Code, "the object did not survive")
				assert.Equal(t, "payload", got.Body.String())
			}

			// And the configuration itself is gone, which is what was asked for.
			after := s3Request(t, srv, http.MethodGet, "/"+tt.bucket+"?publicAccessBlock", nil, nil)
			assert.Equal(t, http.StatusNotFound, after.Code,
				"the configuration outlived its DELETE")
		})
	}
}

// TestS3_PublicAccessBlock_GetUnset pins the 404. Before the fix GET fell through
// to ListObjects and returned 200 with a ListBucketResult body, which the
// reporter's SDK parsed into an empty configuration — a confident wrong answer
// that reads as "public access is not blocked" rather than "nothing is
// configured".
//
// A newly created bucket has no configuration: substrate does not apply S3's
// April 2023 default of enabling all four settings, because that default comes
// from AWS-managed account state substrate does not model, and seeding it would
// make this 404 unreachable.
func TestS3_PublicAccessBlock_GetUnset(t *testing.T) {
	srv, _ := newS3TestServer(t)
	bucket := newPABBucket(t, srv, "pab-unset")

	w := s3Request(t, srv, http.MethodGet, "/"+bucket+"?publicAccessBlock", nil, nil)
	require.Equal(t, http.StatusNotFound, w.Code, "body: %s", w.Body.String())
	assert.Equal(t, "NoSuchPublicAccessBlockConfiguration", parseS3Error(t, w.Body.Bytes()).Code)
}

// TestS3_PublicAccessBlock_PartialBody covers the members being `Required: No`
// individually. A body naming one setting must report the other three as false
// rather than dropping them — S3 always returns all four elements.
func TestS3_PublicAccessBlock_PartialBody(t *testing.T) {
	srv, _ := newS3TestServer(t)
	bucket := newPABBucket(t, srv, "pab-partial")

	require.Equal(t, http.StatusOK, putPAB(t, srv, bucket, `<PublicAccessBlockConfiguration>
  <BlockPublicPolicy>true</BlockPublicPolicy>
</PublicAccessBlockConfiguration>`).Code)

	got := getPAB(t, srv, bucket)
	assert.True(t, got.BlockPublicPolicy, "the named setting was not recorded")
	assert.False(t, got.BlockPublicAcls, "an omitted setting must read back as false")
	assert.False(t, got.IgnorePublicAcls, "an omitted setting must read back as false")
	assert.False(t, got.RestrictPublicBuckets, "an omitted setting must read back as false")
}

// TestS3_PublicAccessBlock_AllFalseIsNotUnset separates the two states a single
// bool field could not distinguish: a configuration whose settings are all false
// is a 200 carrying four false elements, not the 404 an unconfigured bucket gets.
// A consumer relaxing a block writes exactly this body.
func TestS3_PublicAccessBlock_AllFalseIsNotUnset(t *testing.T) {
	srv, _ := newS3TestServer(t)
	bucket := newPABBucket(t, srv, "pab-all-false")

	require.Equal(t, http.StatusOK, putPAB(t, srv, bucket, `<PublicAccessBlockConfiguration>
  <BlockPublicAcls>false</BlockPublicAcls>
  <IgnorePublicAcls>false</IgnorePublicAcls>
  <BlockPublicPolicy>false</BlockPublicPolicy>
  <RestrictPublicBuckets>false</RestrictPublicBuckets>
</PublicAccessBlockConfiguration>`).Code)

	w := s3Request(t, srv, http.MethodGet, "/"+bucket+"?publicAccessBlock", nil, nil)
	require.Equal(t, http.StatusOK, w.Code,
		"an all-false configuration must be a 200, not the unset 404: %s", w.Body.String())

	got := getPAB(t, srv, bucket)
	assert.False(t, got.BlockPublicAcls)
	assert.False(t, got.IgnorePublicAcls)
	assert.False(t, got.BlockPublicPolicy)
	assert.False(t, got.RestrictPublicBuckets)
}

// TestS3_PublicAccessBlock_Overwrite pins that PutPublicAccessBlock replaces the
// configuration rather than merging into it — the operation "creates or modifies"
// the whole document, so a second call naming fewer settings clears the rest.
func TestS3_PublicAccessBlock_Overwrite(t *testing.T) {
	srv, _ := newS3TestServer(t)
	bucket := newPABBucket(t, srv, "pab-overwrite")

	require.Equal(t, http.StatusOK, putPAB(t, srv, bucket, pabAllTrue).Code)
	require.True(t, getPAB(t, srv, bucket).BlockPublicAcls)

	require.Equal(t, http.StatusOK, putPAB(t, srv, bucket, `<PublicAccessBlockConfiguration>
  <IgnorePublicAcls>true</IgnorePublicAcls>
</PublicAccessBlockConfiguration>`).Code)

	got := getPAB(t, srv, bucket)
	assert.True(t, got.IgnorePublicAcls)
	assert.False(t, got.BlockPublicAcls, "PutPublicAccessBlock merged instead of replacing")
	assert.False(t, got.BlockPublicPolicy, "PutPublicAccessBlock merged instead of replacing")
	assert.False(t, got.RestrictPublicBuckets, "PutPublicAccessBlock merged instead of replacing")
}

// TestS3_PublicAccessBlock_DeleteIsIdempotent pins that deleting a configuration
// that was never written is a 204. DeletePublicAccessBlock documents no errors,
// and a teardown path that deletes unconditionally depends on it.
func TestS3_PublicAccessBlock_DeleteIsIdempotent(t *testing.T) {
	srv, _ := newS3TestServer(t)
	bucket := newPABBucket(t, srv, "pab-del-idempotent")

	for i := range 2 {
		w := s3Request(t, srv, http.MethodDelete, "/"+bucket+"?publicAccessBlock", nil, nil)
		assert.Equal(t, http.StatusNoContent, w.Code, "delete %d: %s", i+1, w.Body.String())
	}
	assert.True(t, bucketExists(t, srv, bucket), "the bucket was destroyed")
}

// TestS3_PublicAccessBlock_NoSuchBucket covers all three verbs against a bucket
// that does not exist. This is the assertion that would catch the routing being
// half-applied: an unrouted PUT on a missing bucket would *create* it and return
// 200, which is the most damaging form of the original bug.
func TestS3_PublicAccessBlock_NoSuchBucket(t *testing.T) {
	tests := []struct {
		method string
		body   []byte
	}{
		{method: http.MethodPut, body: []byte(pabAllTrue)},
		{method: http.MethodGet},
		{method: http.MethodDelete},
	}
	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			srv, _ := newS3TestServer(t)

			w := s3Request(t, srv, tt.method, "/pab-absent?publicAccessBlock", tt.body, nil)
			require.Equal(t, http.StatusNotFound, w.Code, "body: %s", w.Body.String())
			assert.Equal(t, "NoSuchBucket", parseS3Error(t, w.Body.Bytes()).Code)

			assert.False(t, bucketExists(t, srv, "pab-absent"),
				"the request created the bucket it named")
		})
	}
}

// TestS3_PublicAccessBlock_MalformedBody covers the two bad-body shapes. An empty
// body is not a request to clear the settings — that is DeletePublicAccessBlock —
// because the body is `Required: Yes`.
func TestS3_PublicAccessBlock_MalformedBody(t *testing.T) {
	tests := []struct {
		name string
		body []byte
	}{
		{name: "empty body", body: nil},
		{name: "not XML", body: []byte("{\"BlockPublicAcls\": true}")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			bucket := newPABBucket(t, srv, "pab-malformed")

			w := s3Request(t, srv, http.MethodPut, "/"+bucket+"?publicAccessBlock", tt.body, nil)
			require.Equal(t, http.StatusBadRequest, w.Code, "body: %s", w.Body.String())
			assert.Equal(t, "MalformedXML", parseS3Error(t, w.Body.Bytes()).Code)

			// A rejected body must leave the bucket unconfigured rather than
			// half-written.
			after := s3Request(t, srv, http.MethodGet, "/"+bucket+"?publicAccessBlock", nil, nil)
			assert.Equal(t, http.StatusNotFound, after.Code, "a rejected PUT wrote a configuration")
		})
	}
}

// TestS3_PublicAccessBlock_VirtualHostStyle covers the addressing form SDKs
// actually use. parseS3Operation resolves the bucket from the path or the Host
// header before the subresource switch, so routing that works path-style could
// still miss here.
func TestS3_PublicAccessBlock_VirtualHostStyle(t *testing.T) {
	srv, _ := newS3TestServer(t)
	bucket := newPABBucket(t, srv, "pab-vhost")

	w := s3VirtualHostRequest(t, srv, http.MethodPut, bucket, "/?publicAccessBlock", []byte(pabAllTrue), nil)
	require.Equal(t, http.StatusOK, w.Code, "PutPublicAccessBlock: %s", w.Body.String())

	get := s3VirtualHostRequest(t, srv, http.MethodGet, bucket, "/?publicAccessBlock", nil, nil)
	require.Equal(t, http.StatusOK, get.Code, "GetPublicAccessBlock: %s", get.Body.String())

	var cfg pabConfig
	require.NoError(t, xml.Unmarshal(get.Body.Bytes(), &cfg))
	assert.True(t, cfg.BlockPublicAcls)

	del := s3VirtualHostRequest(t, srv, http.MethodDelete, bucket, "/?publicAccessBlock", nil, nil)
	assert.Equal(t, http.StatusNoContent, del.Code)
	assert.True(t, bucketExists(t, srv, bucket), "the bucket was destroyed")
}

// TestS3_PublicAccessBlock_SiblingSubresourcesUnaffected is the regression guard
// on the routing change itself. Three new checks were inserted ahead of the
// CreateBucket, DeleteBucket and ListObjects fall-throughs, so the operations
// that reach those fall-throughs legitimately have to keep working.
func TestS3_PublicAccessBlock_SiblingSubresourcesUnaffected(t *testing.T) {
	srv, _ := newS3TestServer(t)
	bucket := newPABBucket(t, srv, "pab-siblings")

	// ListObjects still reaches the GET fall-through.
	w := s3Request(t, srv, http.MethodGet, "/"+bucket, nil, nil)
	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "ListBucketResult")

	// A configured public access block does not block DeleteBucket.
	require.Equal(t, http.StatusOK, putPAB(t, srv, bucket, pabAllTrue).Code)
	del := s3Request(t, srv, http.MethodDelete, "/"+bucket, nil, nil)
	require.Equal(t, http.StatusNoContent, del.Code, "DeleteBucket: %s", del.Body.String())
	assert.False(t, bucketExists(t, srv, bucket), "DeleteBucket left the bucket behind")
}
