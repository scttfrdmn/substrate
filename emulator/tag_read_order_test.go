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

// The order of the Tags member in the five tag-read operations that flatten a map into a list — #946.
//
// Each of the five built its response by ranging a Go map[string]string and appending, so the order on
// the wire came from the map hash seed: two identical calls in one process could report one resource's
// tags in a different order. Renderings of a map *as a JSON object* are not affected and are not
// covered here, because encoding/json sorts map keys on its own; the defect is specifically a map
// flattened into an ordered array or a repeated XML element, where nothing sorted.
//
// Two assertions per operation, and neither substitutes for the other:
//
//  1. **Two reads are byte-identical.** This is the property the emulator's replay promise rests on,
//     and it is asserted on the raw body rather than a decoded value — decoding into a map would throw
//     away the very thing under test, and decoding into a slice would still hide a difference in
//     whitespace or member spelling.
//
//  2. **The keys come back sorted.** Byte-identity alone is satisfied by any stable order, including
//     one that happens to be stable for six keys in one run; only the sorted assertion says which
//     order was chosen. The six keys are written in reverse-sorted order for this reason — a rotation
//     of an unsorted insertion order is not sorted for any offset, so an unsorted handler fails this
//     rather than passing on a lucky iteration.
//
// The provenance differs across the five and is recorded per site rather than blanket. ACM's
// ListTagsForCertificate, CloudFront's ListTagsForResource and both S3 tagging reads publish no
// pagination and no ordering statement at all, so lexicographic is **substrate's reading**, taken for
// the reason [sortTagsByKey] records. Kinesis is the exception: ListTagsForStream publishes
// ExclusiveStartTagKey, "the key to use as the starting point for the list of tags … gets all tags
// that occur after ExclusiveStartTagKey", so an order over keys is **implied by AWS's own cursor**
// even though no sentence names one. Substrate implements neither that cursor nor Limit yet (#954);
// this sort is its prerequisite, because a cursor over an unstable order can skip or repeat a tag.
//
// Every call goes over the wire against a real server, and every tag is written through the owning
// service's own tag call — #765's standing rule, and the only way an assertion about a response body's
// order can be about the response body.

// tagOrderWritten is the six keys every case below writes, in reverse-sorted order deliberately: see
// the second assertion in this file's preamble for why the insertion order matters.
var tagOrderWritten = []string{"zulu", "yankee", "victor", "sierra", "romeo", "quebec"}

// tagOrderSorted is what each of the five operations must report, spelled out rather than computed
// from [tagOrderWritten] so the expectation cannot drift with the same sort the code under test uses.
var tagOrderSorted = []string{"quebec", "romeo", "sierra", "victor", "yankee", "zulu"}

// tagOrderPairs renders [tagOrderWritten] as a key/value map, each value derived from its key so a
// misordered response cannot be mistaken for a correctly ordered one with shuffled values.
func tagOrderPairs() map[string]string {
	pairs := make(map[string]string, len(tagOrderWritten))
	for _, k := range tagOrderWritten {
		pairs[k] = "v-" + k
	}
	return pairs
}

// tagOrderJSONKeys reads the Key members out of a {"Tags": [{"Key":…,"Value":…}]} body, in the order
// the document carries them, and requires each value to be the one its key was written with.
func tagOrderJSONKeys(t *testing.T, raw string) []string {
	t.Helper()
	var doc struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal([]byte(raw), &doc), "decode tags: %s", raw)

	keys := make([]string, 0, len(doc.Tags))
	for _, tag := range doc.Tags {
		assert.Equal(t, "v-"+tag.Key, tag.Value, "each tag keeps its own value")
		keys = append(keys, tag.Key)
	}
	return keys
}

// tagOrderXMLKeys reads the Key members out of an XML tag list, in document order. The path differs
// between the two XML shapes in play — CloudFront wraps its items in <Tags><Items>, S3 in
// <Tagging><TagSet> — so the caller supplies it.
func tagOrderXMLKeys(t *testing.T, raw, path string) []string {
	t.Helper()
	type tagElem struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	// The member path is not a constant, so it cannot be an xml struct tag: the document is walked
	// with a decoder configured by the caller's path instead.
	var doc struct {
		Tags []tagElem
	}
	switch path {
	case "Items>Tag":
		var cf struct {
			Tags []tagElem `xml:"Items>Tag"`
		}
		require.NoError(t, xml.Unmarshal([]byte(raw), &cf), "decode tags: %s", raw)
		doc.Tags = cf.Tags
	case "TagSet>Tag":
		var s3 struct {
			Tags []tagElem `xml:"TagSet>Tag"`
		}
		require.NoError(t, xml.Unmarshal([]byte(raw), &s3), "decode tags: %s", raw)
		doc.Tags = s3.Tags
	default:
		t.Fatalf("unknown tag member path %q", path)
	}

	keys := make([]string, 0, len(doc.Tags))
	for _, tag := range doc.Tags {
		assert.Equal(t, "v-"+tag.Key, tag.Value, "each tag keeps its own value")
		keys = append(keys, tag.Key)
	}
	return keys
}

// tagOrderS3Body is a PutBucketTagging / PutObjectTagging document carrying [tagOrderWritten].
func tagOrderS3Body() []byte {
	body := `<Tagging><TagSet>`
	for _, k := range tagOrderWritten {
		body += `<Tag><Key>` + k + `</Key><Value>v-` + k + `</Value></Tag>`
	}
	return []byte(body + `</TagSet></Tagging>`)
}

// tagOrderS3Read performs one GET ?tagging and returns the raw document.
//
// It signs through scanScopeSigned rather than adding a sixth signing primitive to the package; that
// helper's only S3-specific knowledge is the host and the path, both of which are passed in.
func tagOrderS3Read(t *testing.T, ts *emulator.TestServer, path string) string {
	t.Helper()
	raw, status := scanScopeSigned(t, ts, taggingTestAccount, http.MethodGet,
		"s3."+scanScopeEast+".amazonaws.com", "s3", scanScopeEast, path, nil, "")
	require.Equal(t, http.StatusOK, status, "GET %s: %s", path, raw)
	return string(raw)
}

func TestTagReadOrder_ACMListTagsForCertificateIsSortedAndStable(t *testing.T) {
	t.Parallel()

	ts := emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount))
	arn := requestACMCertificate(t, ts, "order.example.com")

	tags := make([]map[string]string, 0, len(tagOrderWritten))
	for _, k := range tagOrderWritten {
		tags = append(tags, map[string]string{"Key": k, "Value": "v-" + k})
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, acmTarget, taggingTestAccount, "AddTagsToCertificate",
			map[string]any{"CertificateArn": arn, "Tags": tags}), nil)
	require.Empty(t, errCode, "AddTagsToCertificate")
	require.Equal(t, http.StatusOK, status, "AddTagsToCertificate")

	first := acmListTagsRaw(t, ts, arn)
	second := acmListTagsRaw(t, ts, arn)
	assert.Equal(t, first, second, "two identical reads answer byte-identical bodies")
	assert.Equal(t, tagOrderSorted, tagOrderJSONKeys(t, first),
		"API_ListTagsForCertificate documents no order, so sorted by key is substrate's reading (#946)")
}

// acmListTagsRaw returns the raw ListTagsForCertificate document, which acmTags cannot: it decodes
// into a map and so destroys the order under test.
func acmListTagsRaw(t *testing.T, ts *emulator.TestServer, arn string) string {
	t.Helper()
	raw, status := scanScopeSignedTarget(t, ts, taggingTestAccount,
		"acm."+scanScopeEast+".amazonaws.com", "acm", scanScopeEast, "CertificateManager",
		"ListTagsForCertificate", []byte(`{"CertificateArn":"`+arn+`"}`))
	require.Equal(t, http.StatusOK, status, "ListTagsForCertificate: %s", raw)
	return string(raw)
}

func TestTagReadOrder_CloudFrontListTagsForResourceIsSortedAndStable(t *testing.T) {
	t.Parallel()

	ts, arn := cloudfrontTagServer(t)
	pairs := make([]string, 0, 2*len(tagOrderWritten))
	for _, k := range tagOrderWritten {
		pairs = append(pairs, k, "v-"+k)
	}
	cloudfrontTag(t, ts, arn, cloudfrontTagBody(pairs...))

	first := cloudfrontListTagsXML(t, ts, arn)
	second := cloudfrontListTagsXML(t, ts, arn)
	assert.Equal(t, first, second, "two identical reads answer byte-identical documents")
	assert.Equal(t, tagOrderSorted, tagOrderXMLKeys(t, first, "Items>Tag"),
		"CloudFront's ListTagsForResource documents no order, so this is substrate's reading (#946)")
}

func TestTagReadOrder_KinesisListTagsForStreamIsSortedAndStable(t *testing.T) {
	t.Parallel()

	ts := emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount))
	const stream = "order-stream"

	scanScopeJSON(t, ts, taggingTestAccount, "kinesis", scanScopeEast, "Kinesis_20131202",
		"CreateStream", map[string]any{"StreamName": stream, "ShardCount": 1}, nil)
	scanScopeJSON(t, ts, taggingTestAccount, "kinesis", scanScopeEast, "Kinesis_20131202",
		"AddTagsToStream", map[string]any{"StreamName": stream, "Tags": tagOrderPairs()}, nil)

	read := func() string {
		t.Helper()
		raw, status := scanScopeSignedTarget(t, ts, taggingTestAccount,
			"kinesis."+scanScopeEast+".amazonaws.com", "kinesis", scanScopeEast, "Kinesis_20131202",
			"ListTagsForStream", []byte(`{"StreamName":"`+stream+`"}`))
		require.Equal(t, http.StatusOK, status, "ListTagsForStream: %s", raw)
		return string(raw)
	}

	first, second := read(), read()
	assert.Equal(t, first, second, "two identical reads answer byte-identical bodies")
	// The one of the five whose order AWS's own cursor implies: ExclusiveStartTagKey selects the tags
	// that "occur after" a key, which needs an order over keys. Which order is still substrate's
	// reading — AWS's sample response is not sorted — and the cursor itself is #954.
	assert.Equal(t, tagOrderSorted, tagOrderJSONKeys(t, first),
		"ListTagsForStream's ExclusiveStartTagKey implies an order over keys (#946)")
}

func TestTagReadOrder_S3GetBucketTaggingIsSortedAndStable(t *testing.T) {
	t.Parallel()

	ts := emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount))
	const bucket = "tag-order-bucket"

	raw, status := scanScopeSigned(t, ts, taggingTestAccount, http.MethodPut,
		"s3."+scanScopeEast+".amazonaws.com", "s3", scanScopeEast, "/"+bucket, nil, "")
	require.Equal(t, http.StatusOK, status, "CreateBucket: %s", raw)

	raw, status = scanScopeSigned(t, ts, taggingTestAccount, http.MethodPut,
		"s3."+scanScopeEast+".amazonaws.com", "s3", scanScopeEast, "/"+bucket+"?tagging",
		tagOrderS3Body(), "application/xml")
	require.Equal(t, http.StatusNoContent, status, "PutBucketTagging: %s", raw)

	first := tagOrderS3Read(t, ts, "/"+bucket+"?tagging")
	second := tagOrderS3Read(t, ts, "/"+bucket+"?tagging")
	assert.Equal(t, first, second, "two identical reads answer byte-identical documents")
	assert.Equal(t, tagOrderSorted, tagOrderXMLKeys(t, first, "TagSet>Tag"),
		"API_GetBucketTagging documents no order, so sorted by key is substrate's reading (#946)")
}

func TestTagReadOrder_S3GetObjectTaggingIsSortedAndStable(t *testing.T) {
	t.Parallel()

	ts := emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount))
	const (
		bucket = "tag-order-object-bucket"
		key    = "report.txt"
	)

	raw, status := scanScopeSigned(t, ts, taggingTestAccount, http.MethodPut,
		"s3."+scanScopeEast+".amazonaws.com", "s3", scanScopeEast, "/"+bucket, nil, "")
	require.Equal(t, http.StatusOK, status, "CreateBucket: %s", raw)

	raw, status = scanScopeSigned(t, ts, taggingTestAccount, http.MethodPut,
		"s3."+scanScopeEast+".amazonaws.com", "s3", scanScopeEast, "/"+bucket+"/"+key,
		[]byte("hello"), "text/plain")
	require.Equal(t, http.StatusOK, status, "PutObject: %s", raw)

	raw, status = scanScopeSigned(t, ts, taggingTestAccount, http.MethodPut,
		"s3."+scanScopeEast+".amazonaws.com", "s3", scanScopeEast, "/"+bucket+"/"+key+"?tagging",
		tagOrderS3Body(), "application/xml")
	require.Equal(t, http.StatusOK, status, "PutObjectTagging: %s", raw)

	// The object's tag set is a second map on a second record, so it is asserted separately rather
	// than assumed to follow the bucket's: the two are different sites and only one shared helper.
	first := tagOrderS3Read(t, ts, "/"+bucket+"/"+key+"?tagging")
	second := tagOrderS3Read(t, ts, "/"+bucket+"/"+key+"?tagging")
	assert.Equal(t, first, second, "two identical reads answer byte-identical documents")
	assert.Equal(t, tagOrderSorted, tagOrderXMLKeys(t, first, "TagSet>Tag"),
		"API_GetObjectTagging documents no order, so sorted by key is substrate's reading (#946)")
}
