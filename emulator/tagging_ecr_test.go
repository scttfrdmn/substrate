package emulator_test

// ECR's tag wire shape (#1017).
//
// ECR publishes `tags` as an **array of Tag objects** with capitalized `Key`/`Value` members, on
// CreateRepository's and TagResource's requests and on ListTagsForResource's response. Substrate
// decoded and rendered a JSON *object* at all three sites, so the body a real SDK sends failed to
// unmarshal into a map[string]string and `aws ecr create-repository --tags Key=env,Value=prod`
// answered InvalidParameterException/400. ECR tagging was unusable from any SDK.
//
// The existing test could not catch it, because it wrote the same wrong shape it read: a round-trip
// assertion is blind to a shape error both ends share, which is the limit of what #765 can prove.
// Only the published Request and Response Syntax settles it, so the assertions here are against
// **raw JSON**. A decoded []struct{Key, Value string} would accept `key` and `value` as well under
// Go's case-insensitive field matching — the trap iam_shape_members_test.go recorded for XML — and
// a decoded map cannot tell an absent member from an empty one.
//
// Both directions of #765 are still asserted, because the shape fix has to leave the record alone:
// a tag written through ECR's own TagResource must reach GetResources, and one written through
// TagResources must be readable through ListTagsForResource. Those two paths disagreed about the
// wire while agreeing about the record, and they must now agree about both.

import (
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ecrTarget is the wire detail a real SDK would send. The X-Amz-Target prefix carries the 2015 API
// date, unlike the "_V1_1_0" spelling substrate's own in-process tests use.
var ecrTarget = signedRequestTarget{
	host: "ecr.us-east-1.amazonaws.com", target: "AmazonEC2ContainerRegistry_V20150921", signingName: "ecr",
}

// ecrPublishedTags renders a key/value pair list in the published array shape.
//
// It takes pairs rather than a map so a test can control the order it *sends* — the order the
// response comes back in is the assertion, and a map would decide it here.
func ecrPublishedTags(pairs ...[2]string) []map[string]string {
	tags := make([]map[string]string, 0, len(pairs))
	for _, p := range pairs {
		tags = append(tags, map[string]string{"Key": p[0], "Value": p[1]})
	}
	return tags
}

// createECRRepository creates a repository, optionally with tags, and returns the ARN ECR minted.
func createECRRepository(t *testing.T, ts *emulator.TestServer, name string, tags []map[string]string) string {
	t.Helper()
	body := map[string]any{"repositoryName": name}
	if tags != nil {
		body["tags"] = tags
	}
	var out struct {
		Repository struct {
			RepositoryArn string `json:"repositoryArn"`
		} `json:"repository"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, ecrTarget, taggingTestAccount, "CreateRepository", body), &out)
	require.Empty(t, errCode, "CreateRepository %s", name)
	require.Equal(t, http.StatusOK, status, "CreateRepository %s", name)
	require.NotEmpty(t, out.Repository.RepositoryArn, "CreateRepository %s reports an ARN", name)
	return out.Repository.RepositoryArn
}

// ecrRawTagBody returns ListTagsForResource's body verbatim.
func ecrRawTagBody(t *testing.T, ts *emulator.TestServer, arn string) string {
	t.Helper()
	resp := signedRequest(t, ts, ecrTarget, taggingTestAccount, "ListTagsForResource",
		map[string]any{"resourceArn": arn})
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))
	return string(raw)
}

// ecrTagResource tags a repository through ECR's own operation, returning the status and error code.
func ecrTagResource(t *testing.T, ts *emulator.TestServer, arn string, tags []map[string]string) (int, string) {
	t.Helper()
	return decodeAWSResponse(t,
		signedRequest(t, ts, ecrTarget, taggingTestAccount, "TagResource",
			map[string]any{"resourceArn": arn, "tags": tags}), nil)
}

// TestTaggingECR_CreateRepositoryDecodesThePublishedArray pins the shape end to end: the array a real
// SDK sends is accepted at create time, and it comes back as the array AWS publishes.
//
// The raw body is compared whole rather than searched, so a member substrate adds is a failure as
// much as one it drops.
//
// The ordering is asserted over repeated reads rather than once, because one read cannot distinguish
// a sorted renderer from a lucky one. Go shuffles iteration of a small map as a rotation of the
// bucket's own order, so an unsorted renderer answers one of four orders per read for four keys, and
// a single expectation is met by chance about a quarter of the time — a two-key expectation about
// half. Eight reads of four keys leave a dropped sort a chance of roughly one in 65 000 of surviving,
// and byte-identical repeated reads are the property the sort exists for in the first place: a
// recorded run has to replay identically (#862).
func TestTaggingECR_CreateRepositoryDecodesThePublishedArray(t *testing.T) {
	ts := arnGuardServer(t)
	arn := createECRRepository(t, ts, "created-with-tags",
		ecrPublishedTags([2]string{"owner", "platform"}, [2]string{"env", "prod"},
			[2]string{"cost-code", "42"}, [2]string{"app", "registry"}))

	const published = `{"tags":[{"Key":"app","Value":"registry"},{"Key":"cost-code","Value":"42"},` +
		`{"Key":"env","Value":"prod"},{"Key":"owner","Value":"platform"}]}`
	for range 8 {
		assert.JSONEq(t, published, ecrRawTagBody(t, ts, arn))
	}
	assert.Contains(t, ecrRawTagBody(t, ts, arn), `"Key":"env"`,
		"the member names are capitalized, as API_Tag publishes them")
}

// TestTaggingECR_TagResourceDecodesThePublishedArray is the same shape on the other request site.
func TestTaggingECR_TagResourceDecodesThePublishedArray(t *testing.T) {
	ts := arnGuardServer(t)
	arn := createECRRepository(t, ts, "tagged-after-create", nil)

	status, errCode := ecrTagResource(t, ts, arn, ecrPublishedTags([2]string{"team", "sre"}))
	require.Empty(t, errCode, "TagResource")
	require.Equal(t, http.StatusOK, status, "TagResource")

	assert.JSONEq(t, `{"tags":[{"Key":"team","Value":"sre"}]}`, ecrRawTagBody(t, ts, arn))
}

// TestTaggingECR_AnUntaggedRepositoryReportsAnEmptyArray is #938's rule on this operation: an empty
// tag set is reported as `[]`, not as `null` and not by omitting the member.
//
// An SDK decoding `null` into a list cannot tell "this resource has no tags" from "the service did
// not answer the member", which is why the distinction is worth a test of its own. It is also the one
// place ECR's renderer must not follow mapToTaggingTags, which returns nil for an empty map.
func TestTaggingECR_AnUntaggedRepositoryReportsAnEmptyArray(t *testing.T) {
	ts := arnGuardServer(t)
	arn := createECRRepository(t, ts, "never-tagged", nil)

	assert.Equal(t, `{"tags":[]}`, ecrRawTagBody(t, ts, arn),
		"an empty tag set is an empty array, not null and not absent")
}

// TestTaggingECR_ATagsObjectIsRefused pins the shape from the other side: the JSON object substrate
// used to accept is no longer a valid body.
//
// Without this, a future change could restore the map decoding and every other test here would still
// pass — an array unmarshals into a map[string]string exactly as badly as an object unmarshals into a
// slice, so the two shapes are mutually exclusive rather than merely different.
func TestTaggingECR_ATagsObjectIsRefused(t *testing.T) {
	ts := arnGuardServer(t)
	arn := createECRRepository(t, ts, "object-shape", nil)

	status, code, _ := rawSignedCall(t, ts, ecrTarget, taggingTestAccount, "TagResource",
		[]byte(`{"resourceArn":"`+arn+`","tags":{"env":"prod"}}`))
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterException", code)
}

// TestTaggingECR_ATagEntryWithNoKeyIsRefused pins the one refusal the shape brings with it: `Key` is
// Required: Yes on API_Tag, and InvalidParameterException/400 is the code all three operations
// carrying `tags` publish.
//
// An empty *value* is accepted in the same call, because API_CreateRepository's own description of
// `tags` calls the value optional where API_Tag marks it required — see ecrTagsToMap.
func TestTaggingECR_ATagEntryWithNoKeyIsRefused(t *testing.T) {
	ts := arnGuardServer(t)
	arn := createECRRepository(t, ts, "keyless-entry", nil)

	status, errCode := ecrTagResource(t, ts, arn, ecrPublishedTags([2]string{"", "orphan"}))
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterException", errCode)

	status, errCode = ecrTagResource(t, ts, arn, ecrPublishedTags([2]string{"present", ""}))
	require.Empty(t, errCode, "an empty value is accepted")
	require.Equal(t, http.StatusOK, status)
	assert.JSONEq(t, `{"tags":[{"Key":"present","Value":""}]}`, ecrRawTagBody(t, ts, arn))
}

// TestTaggingECR_ATagWrittenThroughECRIsReportedByGetResources is #765's first direction: the shape
// change must not move where the tag is stored.
func TestTaggingECR_ATagWrittenThroughECRIsReportedByGetResources(t *testing.T) {
	ts := arnGuardServer(t)
	arn := createECRRepository(t, ts, "ecr-then-rgta", nil)

	status, errCode := ecrTagResource(t, ts, arn, ecrPublishedTags([2]string{"env", "prod"}))
	require.Empty(t, errCode, "TagResource")
	require.Equal(t, http.StatusOK, status, "TagResource")

	assert.Equal(t, map[string]string{"env": "prod"}, getResourcesTags(t, ts, arn),
		"GetResources reports the tag ECR's own TagResource wrote")
}

// TestTaggingECR_ATagWrittenThroughTagResourcesIsReportedByListTagsForResource is the other
// direction, and the one the old shape actually broke: the tagging API writes a map to the record,
// and ECR's own reader now has to project that map into the published array.
func TestTaggingECR_ATagWrittenThroughTagResourcesIsReportedByListTagsForResource(t *testing.T) {
	ts := arnGuardServer(t)
	arn := createECRRepository(t, ts, "rgta-then-ecr", nil)

	tagResourcesWith(t, ts, arn, map[string]string{"env": "prod"})

	assert.JSONEq(t, `{"tags":[{"Key":"env","Value":"prod"}]}`, ecrRawTagBody(t, ts, arn),
		"ListTagsForResource reports what the tagging API wrote, in ECR's published shape")
}

// TestTaggingECR_ARepositoryCreatedWithTagsSurvivesUntagging pins #938's rule on the path the shape
// fix opened: a repository born tagged, then untagged, is still reported with an empty tag set.
//
// Before #1017 a create carrying tags was a 400, so this sequence could not be run at all. What
// carries the flag here is UntagResource, which recomputes it from the count it saw before deleting
// the keys — not the create-time stamp in createRepository, which is why removing that stamp does
// not fail this test. The path is worth pinning regardless: it is the only one where a repository's
// whole tag history is written by ECR's own operations rather than by the tagging API.
func TestTaggingECR_ARepositoryCreatedWithTagsSurvivesUntagging(t *testing.T) {
	ts := arnGuardServer(t)
	arn := createECRRepository(t, ts, "born-tagged", ecrPublishedTags([2]string{"env", "prod"}))

	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, ecrTarget, taggingTestAccount, "UntagResource",
			map[string]any{"resourceArn": arn, "tagKeys": []string{"env"}}), nil)
	require.Empty(t, errCode, "UntagResource")
	require.Equal(t, http.StatusOK, status, "UntagResource")

	// getResourcesTags fails the test if the ARN is not reported at all, which is the assertion here;
	// the empty map is what a resource that has been tagged and untagged must report.
	assert.Empty(t, getResourcesTags(t, ts, arn),
		"a repository created with tags is still reported after the last tag is removed")
}
