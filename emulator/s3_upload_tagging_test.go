package emulator_test

import (
	"encoding/xml"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// s3TagSetOf reads an object's or bucket's tag set back through GetObjectTagging or
// GetBucketTagging and returns it as a map.
func s3TagSetOf(t *testing.T, srv *emulator.Server, path string) map[string]string {
	t.Helper()
	w := s3Request(t, srv, http.MethodGet, path+"?tagging", nil, nil)
	require.Equal(t, http.StatusOK, w.Code, "get tagging: %s", w.Body.String())
	var doc struct {
		Tags []struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		} `xml:"TagSet>Tag"`
	}
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &doc))
	out := make(map[string]string, len(doc.Tags))
	for _, tg := range doc.Tags {
		out[tg.Key] = tg.Value
	}
	return out
}

// s3RequireCode requires w to be an S3 error with the given status and code.
func s3RequireCode(t *testing.T, w interface {
	Result() *http.Response
}, status int, code string, body string) {
	t.Helper()
	require.Equal(t, status, w.Result().StatusCode, "body: %s", body)
	require.Contains(t, body, "<Code>"+code+"</Code>")
}

// TestS3PutObject_TaggingHeaderIsStored is #1434: PutObject's x-amz-tagging header
// becomes the object's tag set, read back by GetObjectTagging and counted by the
// x-amz-tagging-count header GetObject and HeadObject carry.
func TestS3PutObject_TaggingHeaderIsStored(t *testing.T) {
	cases := []struct {
		name   string
		header string
		want   map[string]string
	}{
		{name: "two tags", header: "A=1&B=2", want: map[string]string{"A": "1", "B": "2"}},
		{name: "url-encoded key and value", header: "team%20name=a%26b&k=v%3Dw", want: map[string]string{"team name": "a&b", "k": "v=w"}},
		{name: "empty value", header: "flag=", want: map[string]string{"flag": ""}},
		{name: "key without equals", header: "flag", want: map[string]string{"flag": ""}},
		{name: "ten tags is the limit", header: s3TaggingHeaderOf(10), want: s3TagMapOf(10)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			putBucket(t, srv, "tagged")
			w := s3Request(t, srv, http.MethodPut, "/tagged/k", []byte("x"), map[string]string{"x-amz-tagging": tc.header})
			require.Equal(t, http.StatusOK, w.Code, "PutObject: %s", w.Body.String())

			assert.Equal(t, tc.want, s3TagSetOf(t, srv, "/tagged/k"))
			for _, method := range []string{http.MethodGet, http.MethodHead} {
				w = s3Request(t, srv, method, "/tagged/k", nil, nil)
				require.Equal(t, http.StatusOK, w.Code)
				assert.Equal(t, fmt.Sprint(len(tc.want)), w.Header().Get("x-amz-tagging-count"), method)
			}
		})
	}
}

// TestS3PutObject_UntaggedObjectHasNoTagCount asserts the count header is absent, not
// zero, on an object with no tags.
func TestS3PutObject_UntaggedObjectHasNoTagCount(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "plain")
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/plain/k", []byte("x"), nil).Code)
	w := s3Request(t, srv, http.MethodGet, "/plain/k", nil, nil)
	require.Equal(t, http.StatusOK, w.Code)
	_, present := w.Header()["X-Amz-Tagging-Count"]
	assert.False(t, present)
}

// s3TaggingHeaderOf builds an x-amz-tagging value carrying n distinct tags.
func s3TaggingHeaderOf(n int) string {
	pairs := make([]string, n)
	for i := range n {
		pairs[i] = fmt.Sprintf("k%d=v%d", i, i)
	}
	return strings.Join(pairs, "&")
}

// s3TagMapOf is the tag set s3TaggingHeaderOf(n) names.
func s3TagMapOf(n int) map[string]string {
	out := make(map[string]string, n)
	for i := range n {
		out[fmt.Sprintf("k%d", i)] = fmt.Sprintf("v%d", i)
	}
	return out
}

// s3InvalidTaggingHeaders are x-amz-tagging values PutObject and CreateMultipartUpload
// must refuse, with the code each is refused with.
var s3InvalidTaggingHeaders = []struct {
	name   string
	header string
	code   string
}{
	{name: "eleven tags", header: s3TaggingHeaderOf(11), code: "InvalidTag"},
	{name: "key too long", header: strings.Repeat("k", 129) + "=v", code: "InvalidTag"},
	{name: "value too long", header: "k=" + strings.Repeat("v", 257), code: "InvalidTag"},
	{name: "surrogate pairs count twice", header: "k=" + strings.Repeat("\U0001F600", 129), code: "InvalidTag"},
	{name: "duplicate key", header: "A=1&A=2", code: "InvalidTag"},
	{name: "empty key", header: "=v", code: "InvalidTag"},
	{name: "not query-encoded", header: "%zz=1", code: "InvalidArgument"},
}

// TestS3PutObject_InvalidTaggingRefused asserts a tag set breaking a published limit
// is refused before anything is written, so the key stays absent.
func TestS3PutObject_InvalidTaggingRefused(t *testing.T) {
	for _, tc := range s3InvalidTaggingHeaders {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			putBucket(t, srv, "refused")
			w := s3Request(t, srv, http.MethodPut, "/refused/k", []byte("x"), map[string]string{"x-amz-tagging": tc.header})
			s3RequireCode(t, w, http.StatusBadRequest, tc.code, w.Body.String())

			head := s3Request(t, srv, http.MethodHead, "/refused/k", nil, nil)
			assert.Equal(t, http.StatusNotFound, head.Code, "a refused PutObject must leave no object")
		})
	}
}

// TestS3PutObject_TwoTagsAtTheLengthLimits asserts the limits are inclusive: a key of
// 128 and a value of 256 UTF-16 code units are accepted.
func TestS3PutObject_TwoTagsAtTheLengthLimits(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "limits")
	key, value := strings.Repeat("k", 128), strings.Repeat("\U0001F600", 128)
	w := s3Request(t, srv, http.MethodPut, "/limits/k", []byte("x"), map[string]string{"x-amz-tagging": key + "=" + value})
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
	assert.Equal(t, map[string]string{key: value}, s3TagSetOf(t, srv, "/limits/k"))
}

// TestS3Multipart_TaggingHeaderCarriedToObject is #1434's multipart case: the tag set
// CreateMultipartUpload names is the one the completed object carries, since Complete
// accepts no tagging header of its own.
func TestS3Multipart_TaggingHeaderCarriedToObject(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "mpu")

	w := s3Request(t, srv, http.MethodPost, "/mpu/big?uploads", nil, map[string]string{"x-amz-tagging": "A=1&B=2"})
	require.Equal(t, http.StatusOK, w.Code, "create: %s", w.Body.String())
	uploadID := uploadIDOf(t, w.Body.String())

	w = s3Request(t, srv, http.MethodPut, "/mpu/big?partNumber=1&uploadId="+uploadID, []byte("part one"), nil)
	require.Equal(t, http.StatusOK, w.Code)
	w = s3Request(t, srv, http.MethodPost, "/mpu/big?uploadId="+uploadID, completeBody(w.Header().Get("ETag")), nil)
	require.Equal(t, http.StatusOK, w.Code, "complete: %s", w.Body.String())

	assert.Equal(t, map[string]string{"A": "1", "B": "2"}, s3TagSetOf(t, srv, "/mpu/big"))
}

// TestS3Multipart_InvalidTaggingRefusedAtCreate asserts a bad tag set is refused when
// the upload is created, so no upload ID is issued for parts that could never complete
// with their tags.
func TestS3Multipart_InvalidTaggingRefusedAtCreate(t *testing.T) {
	for _, tc := range s3InvalidTaggingHeaders {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			putBucket(t, srv, "mpu")
			w := s3Request(t, srv, http.MethodPost, "/mpu/big?uploads", nil, map[string]string{"x-amz-tagging": tc.header})
			s3RequireCode(t, w, http.StatusBadRequest, tc.code, w.Body.String())

			w = s3Request(t, srv, http.MethodGet, "/mpu?uploads", nil, nil)
			require.Equal(t, http.StatusOK, w.Code)
			assert.NotContains(t, w.Body.String(), "<UploadId>", "a refused create must leave no upload")
		})
	}
}

// TestS3CopyObject_ReplaceTaggingValidated asserts CopyObject's REPLACE directive holds
// x-amz-tagging to the same limits PutObject does.
func TestS3CopyObject_ReplaceTaggingValidated(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "copy")
	require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/copy/src", []byte("x"), nil).Code)

	w := s3Request(t, srv, http.MethodPut, "/copy/dst", nil, map[string]string{
		"x-amz-copy-source":       "/copy/src",
		"x-amz-tagging-directive": "REPLACE",
		"x-amz-tagging":           s3TaggingHeaderOf(11),
	})
	s3RequireCode(t, w, http.StatusBadRequest, "InvalidTag", w.Body.String())

	w = s3Request(t, srv, http.MethodPut, "/copy/dst", nil, map[string]string{
		"x-amz-copy-source":       "/copy/src",
		"x-amz-tagging-directive": "REPLACE",
		"x-amz-tagging":           "A=1",
	})
	require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
	assert.Equal(t, map[string]string{"A": "1"}, s3TagSetOf(t, srv, "/copy/dst"))
}

// s3TaggingBody builds a PutObjectTagging/PutBucketTagging body from key/value pairs.
func s3TaggingBody(pairs ...[2]string) []byte {
	var b strings.Builder
	b.WriteString(`<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet>`)
	for _, p := range pairs {
		fmt.Fprintf(&b, "<Tag><Key>%s</Key><Value>%s</Value></Tag>", p[0], p[1])
	}
	b.WriteString(`</TagSet></Tagging>`)
	return []byte(b.String())
}

// s3TagPairs returns n distinct key/value pairs.
func s3TagPairs(n int) [][2]string {
	out := make([][2]string, n)
	for i := range n {
		out[i] = [2]string{fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)}
	}
	return out
}

// TestS3PutObjectTagging_Validated asserts PutObjectTagging, previously unvalidated,
// refuses a set breaking the object limits with InvalidTag and leaves the stored set
// untouched.
func TestS3PutObjectTagging_Validated(t *testing.T) {
	cases := []struct {
		name string
		body []byte
	}{
		{name: "eleven tags", body: s3TaggingBody(s3TagPairs(11)...)},
		{name: "duplicate key", body: s3TaggingBody([2]string{"A", "1"}, [2]string{"A", "2"})},
		{name: "key too long", body: s3TaggingBody([2]string{strings.Repeat("k", 129), "v"})},
		{name: "value too long", body: s3TaggingBody([2]string{"k", strings.Repeat("v", 257)})},
		{name: "empty key", body: s3TaggingBody([2]string{"", "v"})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			putBucket(t, srv, "pot")
			require.Equal(t, http.StatusOK, s3Request(t, srv, http.MethodPut, "/pot/k", []byte("x"),
				map[string]string{"x-amz-tagging": "keep=me"}).Code)

			w := s3Request(t, srv, http.MethodPut, "/pot/k?tagging", tc.body, nil)
			s3RequireCode(t, w, http.StatusBadRequest, "InvalidTag", w.Body.String())
			assert.Equal(t, map[string]string{"keep": "me"}, s3TagSetOf(t, srv, "/pot/k"))
		})
	}
}

// TestS3PutBucketTagging_Validated asserts PutBucketTagging holds a bucket to its own,
// larger, limit of 50 tags, shared with CreateBucket's Tags.
func TestS3PutBucketTagging_Validated(t *testing.T) {
	srv, _ := newS3TestServer(t)
	putBucket(t, srv, "pbt")

	w := s3Request(t, srv, http.MethodPut, "/pbt?tagging", s3TaggingBody(s3TagPairs(50)...), nil)
	require.Equal(t, http.StatusNoContent, w.Code, "fifty is the limit: %s", w.Body.String())
	assert.Len(t, s3TagSetOf(t, srv, "/pbt"), 50)

	w = s3Request(t, srv, http.MethodPut, "/pbt?tagging", s3TaggingBody(s3TagPairs(51)...), nil)
	s3RequireCode(t, w, http.StatusBadRequest, "InvalidTag", w.Body.String())
	assert.Len(t, s3TagSetOf(t, srv, "/pbt"), 50, "a refused set leaves the stored one")
}

// TestABAC_RequestTag_S3TaggingHeaderCaseInsensitive asserts aws:RequestTag reads
// x-amz-tagging whatever its case: an in-process request carries the header as the
// caller spelled it, and the canonical X-Amz-Tagging was invisible to the condition
// before #1434.
func TestABAC_RequestTag_S3TaggingHeaderCaseInsensitive(t *testing.T) {
	policyDoc := newABACPolicy("Allow", "s3:PutObject", "*", "aws:RequestTag/Env", "prod")
	policyARN := "arn:aws:iam::123456789012:policy/ReqTagProdCase"
	state := newAuthTestState(t, "frank", policyARN, policyDoc)
	auth := emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false))
	reqCtx := newAuthTestReqCtx("arn:aws:iam::123456789012:user/frank")

	req := &emulator.AWSRequest{
		Service:   "s3",
		Operation: "PutObject",
		Path:      "/my-bucket/key",
		Headers:   map[string]string{"X-Amz-Tagging": "Env=prod"},
	}
	assert.NoError(t, auth.CheckAccess(reqCtx, req))
}

// TestABAC_RequestTag_CreateBucketConfigurationTags asserts CreateBucket's body tags
// reach aws:RequestTag, so a tag-on-create condition is evaluated against what the
// bucket will carry (#1463).
func TestABAC_RequestTag_CreateBucketConfigurationTags(t *testing.T) {
	policyDoc := newABACPolicy("Allow", "s3:CreateBucket", "*", "aws:RequestTag/Env", "prod")
	policyARN := "arn:aws:iam::123456789012:policy/ReqTagCreateBucket"
	state := newAuthTestState(t, "gina", policyARN, policyDoc)
	auth := emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false))
	reqCtx := newAuthTestReqCtx("arn:aws:iam::123456789012:user/gina")

	for _, tc := range []struct {
		env   string
		allow bool
	}{{env: "prod", allow: true}, {env: "dev", allow: false}} {
		t.Run(tc.env, func(t *testing.T) {
			req := &emulator.AWSRequest{
				Service:   "s3",
				Operation: "CreateBucket",
				Path:      "/abac-bucket",
				Headers:   map[string]string{},
				Body:      createBucketBody("", [2]string{"Env", tc.env}),
			}
			err := auth.CheckAccess(reqCtx, req)
			if tc.allow {
				assert.NoError(t, err)
				return
			}
			var awsErr *emulator.AWSError
			require.ErrorAs(t, err, &awsErr)
			assert.Equal(t, "AccessDenied", awsErr.Code)
		})
	}
}
