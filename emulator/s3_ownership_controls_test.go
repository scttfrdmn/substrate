package emulator_test

import (
	"encoding/xml"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ownershipControlsBody builds a PutBucketOwnershipControls body with one rule per value.
func ownershipControlsBody(values ...string) []byte {
	body := `<OwnershipControls xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`
	for _, v := range values {
		body += "<Rule><ObjectOwnership>" + v + "</ObjectOwnership></Rule>"
	}
	return []byte(body + "</OwnershipControls>")
}

// objectOwnershipOf reads a bucket's ObjectOwnership values back through
// GetBucketOwnershipControls, requiring a 200.
func objectOwnershipOf(t *testing.T, srv *emulator.Server, bucket string) []string {
	t.Helper()
	w := s3Request(t, srv, http.MethodGet, "/"+bucket+"?ownershipControls", nil, nil)
	require.Equal(t, http.StatusOK, w.Code, "GetBucketOwnershipControls: %s", w.Body.String())
	var doc struct {
		XMLName xml.Name `xml:"OwnershipControls"`
		Rules   []struct {
			ObjectOwnership string `xml:"ObjectOwnership"`
		} `xml:"Rule"`
	}
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &doc))
	out := make([]string, 0, len(doc.Rules))
	for _, r := range doc.Rules {
		out = append(out, r.ObjectOwnership)
	}
	return out
}

// TestS3OwnershipControls_Lifecycle is #1445: Put, Get and Delete are routed, a new bucket
// carries the documented BucketOwnerEnforced default, and Delete leaves the bucket
// standing with no controls.
func TestS3OwnershipControls_Lifecycle(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "owned")

	assert.Equal(t, []string{"BucketOwnerEnforced"}, objectOwnershipOf(t, srv, "owned"),
		"S3 sets OwnershipControls for all newly created buckets")

	for _, v := range []string{"BucketOwnerPreferred", "ObjectWriter", "BucketOwnerEnforced"} {
		w := s3Request(t, srv, http.MethodPut, "/owned?ownershipControls", ownershipControlsBody(v), nil)
		require.Equal(t, http.StatusOK, w.Code, "Put %s: %s", v, w.Body.String())
		assert.Empty(t, w.Body.String(), "200 with an empty body")
		assert.Equal(t, []string{v}, objectOwnershipOf(t, srv, "owned"))
	}

	w := s3Request(t, srv, http.MethodDelete, "/owned?ownershipControls", nil, nil)
	require.Equal(t, http.StatusNoContent, w.Code, "Delete: %s", w.Body.String())

	w = s3Request(t, srv, http.MethodGet, "/owned?ownershipControls", nil, nil)
	s3RequireCode(t, w, http.StatusNotFound, "OwnershipControlsNotFoundError", w.Body.String())

	w = s3Request(t, srv, http.MethodDelete, "/owned?ownershipControls", nil, nil)
	assert.Equal(t, http.StatusNoContent, w.Code, "deleting absent controls is still a 204")

	head := s3Request(t, srv, http.MethodHead, "/owned", nil, nil)
	assert.Equal(t, http.StatusOK, head.Code, "DeleteBucketOwnershipControls must not delete the bucket")
}

// TestS3OwnershipControls_PutRefused asserts a body that does not validate against the
// published schema is MalformedXML and leaves the stored setting unchanged.
func TestS3OwnershipControls_PutRefused(t *testing.T) {
	cases := []struct {
		name string
		body []byte
	}{
		{name: "unknown ObjectOwnership", body: ownershipControlsBody("BucketOwnerFull")},
		{name: "lowercase value", body: ownershipControlsBody("bucketownerenforced")},
		{name: "no rules", body: ownershipControlsBody()},
		{name: "empty body", body: nil},
		{name: "not xml", body: []byte("nope")},
		{name: "wrong root element", body: []byte("<Tagging><Rule><ObjectOwnership>ObjectWriter</ObjectOwnership></Rule></Tagging>")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			putBucket(t, srv, "refused")
			w := s3Request(t, srv, http.MethodPut, "/refused?ownershipControls", tc.body, nil)
			s3RequireCode(t, w, http.StatusBadRequest, "MalformedXML", w.Body.String())
			assert.Equal(t, []string{"BucketOwnerEnforced"}, objectOwnershipOf(t, srv, "refused"))
		})
	}
}

// TestS3OwnershipControls_NoSuchBucket asserts all three operations answer NoSuchBucket
// for a bucket that does not exist, rather than creating or listing anything.
func TestS3OwnershipControls_NoSuchBucket(t *testing.T) {
	srv, _ := newS3TestServer(t)
	for _, tc := range []struct {
		method string
		body   []byte
	}{
		{method: http.MethodPut, body: ownershipControlsBody("ObjectWriter")},
		{method: http.MethodGet},
		{method: http.MethodDelete},
	} {
		t.Run(tc.method, func(t *testing.T) {
			w := s3Request(t, srv, tc.method, "/absent-bucket?ownershipControls", tc.body, nil)
			s3RequireCode(t, w, http.StatusNotFound, "NoSuchBucket", w.Body.String())
		})
	}
	head := s3Request(t, srv, http.MethodHead, "/absent-bucket", nil, nil)
	assert.Equal(t, http.StatusNotFound, head.Code, "PutBucketOwnershipControls must not create the bucket")
}

// TestS3CreateBucket_ObjectOwnershipHeader asserts CreateBucket's x-amz-object-ownership
// sets the bucket's initial setting, and that a value outside the enum refuses the
// create before the bucket exists.
func TestS3CreateBucket_ObjectOwnershipHeader(t *testing.T) {
	for _, v := range []string{"BucketOwnerPreferred", "ObjectWriter", "BucketOwnerEnforced"} {
		t.Run(v, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			w := s3Request(t, srv, http.MethodPut, "/hdr", nil, map[string]string{"x-amz-object-ownership": v})
			require.Equal(t, http.StatusOK, w.Code, "CreateBucket: %s", w.Body.String())
			assert.Equal(t, []string{v}, objectOwnershipOf(t, srv, "hdr"))
		})
	}

	t.Run("invalid", func(t *testing.T) {
		srv, _ := newS3TestServer(t)
		w := s3Request(t, srv, http.MethodPut, "/hdr", nil, map[string]string{"x-amz-object-ownership": "Nobody"})
		s3RequireCode(t, w, http.StatusBadRequest, "InvalidArgument", w.Body.String())
		assert.Equal(t, http.StatusNotFound, s3Request(t, srv, http.MethodHead, "/hdr", nil, nil).Code)
	})
}

// TestS3OwnershipControls_DoNotOutliveTheBucket asserts DeleteBucket clears the
// setting, so a bucket re-created under the same name starts from the default rather
// than inheriting the old one (#508).
func TestS3OwnershipControls_DoNotOutliveTheBucket(t *testing.T) {
	srv, _ := newS3TestServer(t)
	w := s3Request(t, srv, http.MethodPut, "/reborn", nil, map[string]string{"x-amz-object-ownership": "ObjectWriter"})
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, []string{"ObjectWriter"}, objectOwnershipOf(t, srv, "reborn"))

	require.Equal(t, http.StatusNoContent, s3Request(t, srv, http.MethodDelete, "/reborn", nil, nil).Code)
	putBucket(t, srv, "reborn")
	assert.Equal(t, []string{"BucketOwnerEnforced"}, objectOwnershipOf(t, srv, "reborn"))
}
