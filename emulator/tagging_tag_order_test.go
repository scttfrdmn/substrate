package emulator_test

import (
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A tag set was reported in Go map iteration order, so two identical calls disagreed (#1011).
//
// Two operations rendered a list of tags by ranging a map[string]string and returning: the Resource
// Groups Tagging API's GetResources, through mapToTaggingTags and the three sibling converters, and
// ElastiCache's ListTagsForResource, which builds its TagList inline. Go randomizes where a map range
// begins, so neither answered one question with one answer — twelve identical GetResources calls
// against one eight-tag resource produced eight distinct bodies.
//
// The write path was already ordered, which is what made this easy to miss. #862 sorted all four merge
// helpers, so the pairs on the record are in key order; the disorder was introduced on the way out,
// when they were re-collected into a map and ranged. A reader checking that tags are written
// deterministically found that they are.
//
// AWS documents no order for either operation. GetResources says nothing about the order of Tags within
// a ResourceTagMapping, and API_ResourceTagMapping describes Tags only as "the tags that have been
// applied to one or more AWS resources"; ElastiCache's API_ListTagsForResource describes TagList.Tag.N
// only as "A list of tags as key-value pairs" and publishes no cursor. Lexicographic by key is
// therefore substrate's reading, resting on the replay promise and on the in-tree precedents #764,
// #862 and #946 — not on a match with AWS. Sorting by key needs no tie-break, because a tag set cannot
// carry a duplicate key.
//
// Both assertions here are made on the raw response bytes. That is the point: the existing tagging
// suite decodes ResourceTagMappingList and then collects ResourceARN only, discarding Tags
// (tagging_scan_scope_test.go, tagging_acm_cloudfront_test.go), so it was structurally incapable of
// seeing this — the same way #950's suite could not see a parse guard. A decoded set compares equal
// whatever order it arrived in, so only the bytes can carry the claim.

// tagOrderKeys is the ascending key order both operations are asserted to report.
//
// Eight keys rather than two or three, because the defect is a rotation of the stored order: with two
// keys an unsorted range comes out ascending half the time, and a test that passes half the time is
// worse than none. Eight leaves one chance in eight per call, which the repeat count below compounds.
var tagOrderKeys = []string{"alpha", "beta", "delta", "epsilon", "eta", "gamma", "theta", "zeta"}

// tagOrderDescending is tagOrderKeys reversed, which is the order the tags are written in.
//
// Written descending so that insertion order is not the answer either: if a reader ever preserved the
// order the request supplied, an ascending assertion would catch it rather than agreeing with it.
func tagOrderDescending() map[string]string {
	tags := make(map[string]string, len(tagOrderKeys))
	for i, key := range tagOrderKeys {
		tags[key] = strconv.Itoa(len(tagOrderKeys) - i)
	}
	return tags
}

// tagOrderRepeats is how many times each listing is called.
//
// Eleven repeats past the first give roughly one chance in 8^11 that an unsorted range agrees with
// itself throughout, which is small enough that a green run means the sort and not luck. The ascending
// assertion is the deterministic half; this is what makes a *stable but wrong* order fail loudly too.
const tagOrderRepeats = 12

// getResourcesRawBody calls GetResources and returns the response body verbatim.
func getResourcesRawBody(t *testing.T, ts *emulator.TestServer) []byte {
	t.Helper()
	resp := signedRequest(t, ts, taggingTarget, taggingTestAccount, "GetResources", map[string]any{})
	defer resp.Body.Close() //nolint:errcheck

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read GetResources body")
	require.Equalf(t, http.StatusOK, resp.StatusCode, "GetResources: %s", raw)
	return raw
}

// TestGetResources_ATagSetIsReportedInOneOrder is #1011 reached through the converter every scanner
// shares.
//
// The resource is a DynamoDB table because CreateTable reports its own ARN, so the ARN handed to
// TagResources comes from the owning service rather than being composed by the test — a composed ARN
// would let the assertion agree with a scanner that disagreed with what the service hands a caller,
// which is the divergence #826 through #932 spent nine issues on.
func TestGetResources_ATagSetIsReportedInOneOrder(t *testing.T) {
	ts := arnGuardServer(t)

	var created struct {
		TableDescription struct {
			TableArn string `json:"TableArn"`
		} `json:"TableDescription"`
	}
	status, errCode := decodeAWSResponse(t, signedRequest(t, ts, dynamodbTarget, taggingTestAccount,
		"CreateTable", map[string]any{
			"TableName": "tag-order-table",
			"KeySchema": []map[string]string{{"AttributeName": "id", "KeyType": "HASH"}},
			"AttributeDefinitions": []map[string]string{
				{"AttributeName": "id", "AttributeType": "S"},
			},
			"BillingMode": "PAY_PER_REQUEST",
		}), &created)
	require.Empty(t, errCode, "CreateTable")
	require.Equal(t, http.StatusOK, status, "CreateTable")

	arn := created.TableDescription.TableArn
	require.NotEmpty(t, arn, "CreateTable reports an ARN")

	tagResourcesWith(t, ts, arn, tagOrderDescending())

	first := getResourcesRawBody(t, ts)

	t.Run("the reported order is ascending by key", func(t *testing.T) {
		var out struct {
			ResourceTagMappingList []struct {
				ResourceARN string `json:"ResourceARN"`
				Tags        []struct {
					Key string `json:"Key"`
				} `json:"Tags"`
			} `json:"ResourceTagMappingList"`
		}
		require.NoErrorf(t, json.Unmarshal(first, &out), "decode GetResources: %s", first)
		require.Lenf(t, out.ResourceTagMappingList, 1, "one resource reported: %s", first)
		require.Equal(t, arn, out.ResourceTagMappingList[0].ResourceARN, "it is the table")

		got := make([]string, 0, len(out.ResourceTagMappingList[0].Tags))
		for _, tag := range out.ResourceTagMappingList[0].Tags {
			got = append(got, tag.Key)
		}
		assert.Equal(t, tagOrderKeys, got, "mapToTaggingTags reports tags in key order")
	})

	t.Run("repeated calls return byte-identical bodies", func(t *testing.T) {
		for i := 2; i <= tagOrderRepeats; i++ {
			assert.Equalf(t, string(first), string(getResourcesRawBody(t, ts)),
				"GetResources call %d differs from the first", i)
		}
	})
}

// TestElastiCache_ATagSetIsReportedInOneOrder is the same defect at the site #946 missed.
//
// #946 swept the per-service tag listings that render a map — Kinesis, S3 and three more — but every
// one of those answers JSON. ElastiCache stores its tags as a map[string]string like they do and
// renders them into XML, so it fell outside that sweep's shape and kept ranging the map.
func TestElastiCache_ATagSetIsReportedInOneOrder(t *testing.T) {
	ts := newElastiCacheTestServer(t)

	resp := ecRequest(t, ts, map[string]string{
		"Action":         "CreateCacheCluster",
		"CacheClusterId": "tag-order-cluster",
		"CacheNodeType":  "cache.t3.micro",
		"Engine":         "redis",
	})
	body := ecBody(t, resp)
	require.Equalf(t, http.StatusOK, resp.StatusCode, "CreateCacheCluster: %s", body)

	// ElastiCache reports no ARN for a cluster it creates, and its own AddTagsToResource takes one, so
	// the ARN is composed here from the format API_ListTagsForResource publishes for exactly this case:
	// "arn:aws:elasticache:us-west-2:0123456789:cluster:myCluster".
	arn := "arn:aws:elasticache:us-east-1:123456789012:cluster:tag-order-cluster"

	// Written in descending key order, so a reader preserving the request's order fails the ascending
	// assertion rather than passing it.
	add := map[string]string{"Action": "AddTagsToResource", "ResourceName": arn}
	for i := range tagOrderKeys {
		key := tagOrderKeys[len(tagOrderKeys)-1-i]
		n := strconv.Itoa(i + 1)
		add["Tags.member."+n+".Key"] = key
		add["Tags.member."+n+".Value"] = strconv.Itoa(i)
	}
	resp = ecRequest(t, ts, add)
	body = ecBody(t, resp)
	require.Equalf(t, http.StatusOK, resp.StatusCode, "AddTagsToResource: %s", body)

	keys, first := ecTagKeyOrder(t, ts, arn)
	assert.Equal(t, tagOrderKeys, keys, "ListTagsForResource reports tags in key order")

	for i := 2; i <= tagOrderRepeats; i++ {
		_, again := ecTagKeyOrder(t, ts, arn)
		assert.Equalf(t, first, again, "ListTagsForResource call %d differs from the first", i)
	}
}

// ecTagKeyOrder reads one ElastiCache resource's TagList and returns the keys in the order reported,
// alongside the raw body so a second call can be compared to it byte for byte.
func ecTagKeyOrder(t *testing.T, ts *httptest.Server, arn string) ([]string, string) {
	t.Helper()
	resp := ecRequest(t, ts, map[string]string{
		"Action":       "ListTagsForResource",
		"ResourceName": arn,
	})
	body := ecBody(t, resp)
	require.Equalf(t, http.StatusOK, resp.StatusCode, "ListTagsForResource: %s", body)

	var out struct {
		Tags []struct {
			Key string `xml:"Key"`
		} `xml:"ListTagsForResourceResult>TagList>Tag"`
	}
	require.NoErrorf(t, xml.Unmarshal([]byte(body), &out), "decode ListTagsForResource: %s", body)

	keys := make([]string, 0, len(out.Tags))
	for _, tag := range out.Tags {
		keys = append(keys, tag.Key)
	}
	return keys, body
}
