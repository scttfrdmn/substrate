package emulator_test

import (
	"bytes"
	"encoding/xml"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// One x-amz-copy-source parser for authorization and both copy handlers (#1489 review).
//
// The version suffix begins at the first unencoded "?", because the header "must be URL-encoded" and
// a "?" belonging to the key therefore arrives as %3F. Each case is read twice — by the
// s3:GetObject authorization pass and by CopyObject — and the two must name the same object.

// xmlElementText returns the text of the first element named name in body.
func xmlElementText(t *testing.T, body []byte, name string) string {
	t.Helper()
	dec := xml.NewDecoder(bytes.NewReader(body))
	for {
		tok, err := dec.Token()
		require.NoError(t, err, "no <%s> in %s", name, body)
		if start, ok := tok.(xml.StartElement); ok && start.Name.Local == name {
			var text string
			require.NoError(t, dec.DecodeElement(&text, &start))
			return text
		}
	}
}

// s3CopySourceFixture is a versioned source bucket holding key "k" at two versions, and an object
// whose key contains a literal "?versionId=".
type s3CopySourceFixture struct {
	srv      *emulator.Server
	firstVID string
}

func newS3CopySourceFixture(t *testing.T) s3CopySourceFixture {
	t.Helper()
	srv, _ := newS3TestServer(t)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/src", nil, nil).Code)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/src?versioning",
		[]byte(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`), nil).Code)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/dst", nil, nil).Code)

	first := s3Request(t, srv, http.MethodPut, "/src/k", []byte("first"), nil)
	require.Equal(t, http.StatusOK, first.Code)
	vid := first.Header().Get("x-amz-version-id")
	require.NotEmpty(t, vid)
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/src/k", []byte("second"), nil).Code)
	require.Equal(t, http.StatusOK,
		s3Request(t, srv, http.MethodPut, "/src/k%3FversionId%3D"+vid, []byte("literal"), nil).Code)
	return s3CopySourceFixture{srv: srv, firstVID: vid}
}

// TestS3CopySource_AuthorizationAndTheHandlerReadTheSameObject is the review's case: a key spelled
// with a "?versionId=" must not be authorized as one object and read as another.
func TestS3CopySource_AuthorizationAndTheHandlerReadTheSameObject(t *testing.T) {
	t.Parallel()
	f := newS3CopySourceFixture(t)

	for _, tc := range []struct {
		name     string
		header   func(vid string) string
		wantARN  func(vid string) string
		wantBody string
	}{
		{
			name:     "no suffix is the current version",
			header:   func(string) string { return "/src/k" },
			wantARN:  func(string) string { return "arn:aws:s3:::src/k" },
			wantBody: "second",
		},
		{
			name:     "an unencoded ?versionId= names a version of k",
			header:   func(vid string) string { return "/src/k?versionId=" + vid },
			wantARN:  func(string) string { return "arn:aws:s3:::src/k" },
			wantBody: "first",
		},
		{
			name:     "an encoded ?versionId= is part of the key",
			header:   func(vid string) string { return "/src/k%3FversionId%3D" + vid },
			wantARN:  func(vid string) string { return "arn:aws:s3:::src/k?versionId=" + vid },
			wantBody: "literal",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			header := tc.header(f.firstVID)

			// Authorization: s3:GetObject granted on exactly the expected ARN admits the copy, and
			// the same grant on the other reading of the header does not.
			grant := func(resource string) emulator.PolicyDocument {
				return s3AuthzPolicy([]string{"s3:PutObject"}, emulator.PolicyStatement{
					Effect: "Allow", Action: emulator.StringOrSlice{"s3:GetObject"},
					Resource: emulator.StringOrSlice{resource},
				})
			}
			req := &emulator.AWSRequest{Service: "s3", Operation: "CopyObject", Path: "/dst/out",
				Headers: map[string]string{"X-Amz-Copy-Source": header}}
			assert.NoError(t, s3AuthzCheck(t, "copier", grant(tc.wantARN(f.firstVID)), req))
			requireS3Denied(t, s3AuthzCheck(t, "copier", grant("arn:aws:s3:::src/nothing"), req), "s3:GetObject")

			// The handler: the copy reads the object the ARN names.
			w := s3Request(t, f.srv, http.MethodPut, "/dst/out", nil, map[string]string{"X-Amz-Copy-Source": header})
			require.Equal(t, http.StatusOK, w.Code, "%s", w.Body.String())
			got := s3Request(t, f.srv, http.MethodGet, "/dst/out", nil, nil)
			require.Equal(t, http.StatusOK, got.Code)
			assert.Equal(t, tc.wantBody, got.Body.String())
		})
	}
}

// TestS3CopySource_UploadPartCopyReadsTheNamedVersion asserts UploadPartCopy takes the version from
// the same parser.
func TestS3CopySource_UploadPartCopyReadsTheNamedVersion(t *testing.T) {
	t.Parallel()
	f := newS3CopySourceFixture(t)
	create := s3Request(t, f.srv, http.MethodPost, "/dst/mpu?uploads", nil, nil)
	require.Equal(t, http.StatusOK, create.Code)
	uploadID := xmlElementText(t, create.Body.Bytes(), "UploadId")

	w := s3Request(t, f.srv, http.MethodPut, "/dst/mpu?partNumber=1&uploadId="+uploadID, nil,
		map[string]string{"X-Amz-Copy-Source": "/src/k?versionId=" + f.firstVID})
	require.Equal(t, http.StatusOK, w.Code, "%s", w.Body.String())
	etag := xmlElementText(t, w.Body.Bytes(), "ETag")

	done := s3Request(t, f.srv, http.MethodPost, "/dst/mpu?uploadId="+uploadID, []byte(
		`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>`+etag+`</ETag></Part></CompleteMultipartUpload>`), nil)
	require.Equal(t, http.StatusOK, done.Code, "%s", done.Body.String())
	got := s3Request(t, f.srv, http.MethodGet, "/dst/mpu", nil, nil)
	assert.Equal(t, "first", got.Body.String())
}

// TestS3CopySource_Refusals covers the copy sources both handlers refuse, each refused the same way
// by CopyObject and UploadPartCopy, and needing no authorization pass because nothing is read.
func TestS3CopySource_Refusals(t *testing.T) {
	t.Parallel()
	f := newS3CopySourceFixture(t)
	marker := s3Request(t, f.srv, http.MethodDelete, "/src/k", nil, nil)
	require.Equal(t, http.StatusNoContent, marker.Code)
	markerVID := marker.Header().Get("x-amz-version-id")
	require.NotEmpty(t, markerVID)

	create := s3Request(t, f.srv, http.MethodPost, "/dst/mpu?uploads", nil, nil)
	require.Equal(t, http.StatusOK, create.Code)
	uploadID := xmlElementText(t, create.Body.Bytes(), "UploadId")

	for _, tc := range []struct {
		name   string
		header string
		status int
		code   string
	}{
		{"an access-point ARN", "arn%3Aaws%3As3%3Aus-west-2%3A123456789012%3Aaccesspoint%2Fap%2Fobject%2Fk",
			http.StatusNotImplemented, "NotImplemented"},
		{"no key", "/src", http.StatusBadRequest, "InvalidArgument"},
		{"a current version that is a delete marker", "/src/k", http.StatusNotFound, "NoSuchKey"},
		{"a version ID naming a delete marker", "/src/k?versionId=" + markerVID, http.StatusBadRequest, "InvalidRequest"},
		{"a version that does not exist", "/src/k?versionId=nope", http.StatusNotFound, "NoSuchKey"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, path := range []string{"/dst/out", "/dst/mpu?partNumber=1&uploadId=" + uploadID} {
				w := s3Request(t, f.srv, http.MethodPut, path, nil, map[string]string{"X-Amz-Copy-Source": tc.header})
				require.Equal(t, tc.status, w.Code, "%s: %s", path, w.Body.String())
				assert.Equal(t, tc.code, s3ErrorCode(t, w.Body.Bytes()), path)
			}
			for _, op := range []string{"CopyObject", "UploadPartCopy"} {
				req := &emulator.AWSRequest{Service: "s3", Operation: op, Path: "/dst/out",
					Headers: map[string]string{"X-Amz-Copy-Source": tc.header}}
				if tc.status == http.StatusNotImplemented || tc.code == "InvalidArgument" {
					assert.NoError(t, s3AuthzCheck(t, "writer", s3AuthzPolicy([]string{"s3:PutObject"}), req),
						"%s: an unparseable source is refused by the handler, not authorized", op)
				}
			}
		})
	}
}
