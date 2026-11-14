package emulator_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createBucketBody builds a CreateBucketConfiguration document with an optional
// LocationConstraint and the given tags.
func createBucketBody(location string, tags ...[2]string) []byte {
	var b strings.Builder
	b.WriteString(`<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
	if location != "" {
		fmt.Fprintf(&b, "<LocationConstraint>%s</LocationConstraint>", location)
	}
	if len(tags) > 0 {
		b.WriteString("<Tags>")
		for _, t := range tags {
			fmt.Fprintf(&b, "<Tag><Key>%s</Key><Value>%s</Value></Tag>", t[0], t[1])
		}
		b.WriteString("</Tags>")
	}
	b.WriteString("</CreateBucketConfiguration>")
	return []byte(b.String())
}

// TestS3CreateBucket_ConfigurationTags is #1463: CreateBucketConfiguration's Tags tag
// the bucket at creation, read back by GetBucketTagging.
func TestS3CreateBucket_ConfigurationTags(t *testing.T) {
	cases := []struct {
		name string
		body []byte
		want map[string]string
	}{
		{name: "tags only", body: createBucketBody("", [2]string{"aegc:managed", "true"}), want: map[string]string{"aegc:managed": "true"}},
		{name: "with location constraint", body: createBucketBody("us-west-2", [2]string{"a", "1"}, [2]string{"b", "2"}), want: map[string]string{"a": "1", "b": "2"}},
		{name: "fifty tags is the limit", body: createBucketBody("", s3TagPairs(50)...), want: s3TagMapOf(50)},
		{name: "location only", body: createBucketBody("us-west-2"), want: map[string]string{}},
		{name: "no body", body: nil, want: map[string]string{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			w := s3Request(t, srv, http.MethodPut, "/created-tagged", tc.body, nil)
			require.Equal(t, http.StatusOK, w.Code, "CreateBucket: %s", w.Body.String())
			assert.Equal(t, tc.want, s3TagSetOf(t, srv, "/created-tagged"))
		})
	}
}

// TestS3CreateBucket_ConfigurationRefused asserts a malformed configuration or a tag
// set breaking the bucket limits is refused before the bucket exists.
func TestS3CreateBucket_ConfigurationRefused(t *testing.T) {
	cases := []struct {
		name string
		body []byte
		code string
	}{
		{name: "not xml", body: []byte("not xml"), code: "MalformedXML"},
		{name: "wrong root element", body: []byte("<Tagging><TagSet/></Tagging>"), code: "MalformedXML"},
		{name: "fifty-one tags", body: createBucketBody("", s3TagPairs(51)...), code: "InvalidTag"},
		{name: "duplicate key", body: createBucketBody("", [2]string{"a", "1"}, [2]string{"a", "2"}), code: "InvalidTag"},
		{name: "empty key", body: createBucketBody("", [2]string{"", "1"}), code: "InvalidTag"},
		{name: "reserved aws: prefix", body: createBucketBody("", [2]string{"aws:cloudformation:stack-name", "s"}), code: "InvalidTag"},
		{name: "disallowed character", body: createBucketBody("", [2]string{"cost*center", "1"}), code: "InvalidTag"},
		{name: "key too long", body: createBucketBody("", [2]string{strings.Repeat("k", 129), "1"}), code: "InvalidTag"},
		{name: "value too long", body: createBucketBody("", [2]string{"k", strings.Repeat("v", 257)}), code: "InvalidTag"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newS3TestServer(t)
			w := s3Request(t, srv, http.MethodPut, "/refused-create", tc.body, nil)
			s3RequireCode(t, w, http.StatusBadRequest, tc.code, w.Body.String())
			head := s3Request(t, srv, http.MethodHead, "/refused-create", nil, nil)
			assert.Equal(t, http.StatusNotFound, head.Code, "a refused CreateBucket must leave no bucket")
		})
	}
}

// TestS3CreateBucket_TaggedBucketEverTagged asserts a bucket tagged at creation counts
// as tagged for the Resource Groups Tagging API exactly as one tagged afterwards by
// PutBucketTagging does: reported while it holds the tag, and still reported with an
// empty set once DeleteBucketTagging removes it (#938, #1463).
func TestS3CreateBucket_TaggedBucketEverTagged(t *testing.T) {
	t.Parallel()
	ts := everTaggedServer(t)

	const bucket = "ever-tagged-at-create"
	raw, status := scanScopeSigned(t, ts, taggingTestAccount, http.MethodPut,
		"s3."+scanScopeEast+".amazonaws.com", "s3", scanScopeEast, "/"+bucket,
		createBucketBody("", [2]string{"env", "prod"}), "application/xml")
	require.Equalf(t, http.StatusOK, status, "CreateBucket: %s", raw)

	arns := everTaggedReported(t, ts, map[string]any{})
	matches := scanScopeMatches(arns, bucket)
	require.Lenf(t, matches, 1, "the bucket tagged at creation is reported: %v", arns)

	everTaggedDeleteBucketTagging(t, ts, bucket)

	tags, _ := everTaggedRawTags(t, ts, map[string]any{})
	require.Containsf(t, tags, matches[0], "still reported once its tags are removed: %v", tags)
	assert.Equal(t, "[]", tags[matches[0]], "with an empty tag set")
}
