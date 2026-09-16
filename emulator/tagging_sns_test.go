package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// SNS had one defect reached from ten directions and one row of #835 that could not be added without
// fixing it first.
//
// snsNameFromARN split a TopicArn on ":" and returned the LAST segment, and each of its ten callers
// then keyed loadTopic/saveTopic by the *caller's* account and Region. So a topic ARN naming another
// account or Region addressed the caller's own topic of that name; a subscription ARN — which is the
// topic's ARN with an identifier appended — resolved to that identifier as if it were a topic name;
// and a string that was not an ARN at all fell through the length guard and was looked up verbatim
// (#925). That is the rule #826 established for SQS, #845 carried into the tagging API's resolver,
// #910 applied to Step Functions, #918 to CloudFront and #922 to KMS.
//
// On top of that fix, the SNS topic row of #835: the Resource Groups Tagging API had no sns arm, so a
// topic ARN fell to resolveARN's default and answered an InternalServiceException FailedResourcesMap
// entry, and GetResources reported no topics at all.
//
// The two ship together because they are the same change seen from two sides. Both need one
// context-free key builder — [snsParseTopicARN] plus the five free-function key builders — and a
// resolver that cannot reach for the request context does not have to remember not to. Unlike KMS
// (#922) there is no cross-account refusal to add: no cross-account statement appears on any of SNS's
// three tagging pages, so isolation here is emergent — a foreign-account ARN builds a state key
// nothing is stored at — and not-found is the honest answer.
//
// Every topic is created through CreateTopic and every tag read back through SNS's own
// ListTagsForResource, per #765. A helper writing the record directly would write the very key the
// assertion has to trust, which is the one thing that distinguishes a tag that landed on the right
// record from a tag that was reported as landed.

// The wire details of an SNS query-protocol request. The parser routes on the Host and takes the
// Region from it, so a second Region needs nothing but a second host name.
const (
	snsTagSigningName = "sns"
	snsEastRegion     = "us-east-1"
	snsWestRegion     = "us-west-2"
)

// snsTagHost returns the SNS endpoint host for a Region.
func snsTagHost(region string) string { return "sns." + region + ".amazonaws.com" }

// snsTagServer starts a server callable as [taggingTestAccount], with SNS and the tagging API both
// registered — most of these tests exist to show the two agree.
func snsTagServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	return emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount))
}

// snsQuery posts one SNS query-protocol action to a Region's endpoint as [taggingTestAccount] and
// returns the status, the body, and the error code a refusal carries.
//
// The code is returned separately for the reason [decodeAWSResponse] gives for the JSON protocol: a
// refusal and a success are different documents, and a test asserting a refusal cares which code came
// back rather than what the message says.
func snsQuery(t *testing.T, ts *emulator.TestServer, region string, params map[string]string) (int, string, string) {
	t.Helper()

	creds, ok := ts.CredentialsFor(taggingTestAccount)
	if !ok {
		t.Fatalf("no credential registered for account %s", taggingTestAccount)
	}

	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	encoded := form.Encode()
	host := snsTagHost(region)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", strings.NewReader(encoded))
	if err != nil {
		t.Fatalf("build %s request: %v", params["Action"], err)
	}
	req.Host = host
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Amz-Date", sigV4TestDateTime)
	req.Header.Set("Authorization", sigV4Header(
		http.MethodPost, "/", host, snsTagSigningName, region, sigV4TestDateTime,
		[]byte(encoded), creds.AccessKeyID, creds.SecretAccessKey))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s in %s: %v", params["Action"], region, err)
	}
	defer resp.Body.Close() //nolint:errcheck

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s body: %v", params["Action"], err)
	}

	var errDoc struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Code    string   `xml:"Error>Code"`
	}
	if unmarshalErr := xml.Unmarshal(raw, &errDoc); unmarshalErr == nil && errDoc.Code != "" {
		return resp.StatusCode, string(raw), errDoc.Code
	}
	return resp.StatusCode, string(raw), ""
}

// snsQueryOK posts an action to a Region's endpoint and fails the test unless it answers 200.
func snsQueryOK(t *testing.T, ts *emulator.TestServer, region string, params map[string]string) string {
	t.Helper()
	status, body, errCode := snsQuery(t, ts, region, params)
	if errCode != "" || status != http.StatusOK {
		t.Fatalf("%s in %s: status %d, error %q, body %s", params["Action"], region, status, errCode, body)
	}
	return body
}

// snsCreateTopicIn creates a topic in a Region and returns the ARN SNS itself minted, rather than one
// built here — so no assertion below can pass by agreeing with an ARN the test happened to construct.
//
// extra carries any additional parameters, which is how the CreateTopic-tags cases send theirs.
func snsCreateTopicIn(t *testing.T, ts *emulator.TestServer, region, name string, extra map[string]string) string {
	t.Helper()
	params := map[string]string{"Action": "CreateTopic", "Name": name}
	for k, v := range extra {
		params[k] = v
	}
	body := snsQueryOK(t, ts, region, params)

	var doc struct {
		ARN string `xml:"CreateTopicResult>TopicArn"`
	}
	if err := xml.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("decode CreateTopic %s: %v (body %s)", name, err, body)
	}
	require.NotEmpty(t, doc.ARN, "CreateTopic reports an ARN")
	return doc.ARN
}

// snsCreateTopic creates a topic in us-east-1.
func snsCreateTopic(t *testing.T, ts *emulator.TestServer, name string) string {
	t.Helper()
	return snsCreateTopicIn(t, ts, snsEastRegion, name, nil)
}

// snsXMLTag is one member of SNS's Tags list as ListTagsForResource renders it.
type snsXMLTag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

// snsTagList returns a topic's tags as SNS's own ListTagsForResource reports them, in document order.
//
// Order is preserved rather than collapsed into a map because it is one of the things under test:
// SNS's own arm used to keep insertion order while the tagging API's arm emits key-sorted order, so
// one topic's tags came back in two different orders depending on which API was asked.
func snsTagList(t *testing.T, ts *emulator.TestServer, region, arn string) []snsXMLTag {
	t.Helper()
	body := snsQueryOK(t, ts, region, map[string]string{
		"Action":      "ListTagsForResource",
		"ResourceArn": arn,
	})
	var doc struct {
		Tags []snsXMLTag `xml:"ListTagsForResourceResult>Tags>member"`
	}
	if err := xml.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("decode ListTagsForResource %s: %v (body %s)", arn, err, body)
	}
	return doc.Tags
}

// snsTagMap returns a topic's tags as SNS reports them, keyed by tag key.
func snsTagMap(t *testing.T, ts *emulator.TestServer, arn string) map[string]string {
	t.Helper()
	tags := map[string]string{}
	for _, tag := range snsTagList(t, ts, snsEastRegion, arn) {
		tags[tag.Key] = tag.Value
	}
	return tags
}

// snsTagKeyOrder returns the tag keys in the order ListTagsForResource reported them.
func snsTagKeyOrder(t *testing.T, ts *emulator.TestServer, arn string) []string {
	t.Helper()
	list := snsTagList(t, ts, snsEastRegion, arn)
	keys := make([]string, 0, len(list))
	for _, tag := range list {
		keys = append(keys, tag.Key)
	}
	return keys
}

// snsTopicAttributes returns the attribute map GetTopicAttributes reports, or the error code it
// refused with.
//
// GetTopicAttributes is the resolution probe rather than the state store, because the whole question
// is which topic an ARN addresses over the wire — and the reported TopicArn names it.
func snsTopicAttributes(t *testing.T, ts *emulator.TestServer, region, arn string) (map[string]string, string) {
	t.Helper()
	status, body, errCode := snsQuery(t, ts, region, map[string]string{
		"Action":   "GetTopicAttributes",
		"TopicArn": arn,
	})
	if errCode != "" {
		return nil, errCode
	}
	require.Equal(t, http.StatusOK, status, "GetTopicAttributes %s", arn)

	var doc struct {
		Entries []struct {
			Key   string `xml:"key"`
			Value string `xml:"value"`
		} `xml:"GetTopicAttributesResult>Attributes>entry"`
	}
	if err := xml.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("decode GetTopicAttributes %s: %v (body %s)", arn, err, body)
	}
	attrs := make(map[string]string, len(doc.Entries))
	for _, e := range doc.Entries {
		attrs[e.Key] = e.Value
	}
	return attrs, ""
}

// snsSubscribe subscribes an endpoint to a topic and returns the subscription ARN SNS minted.
func snsSubscribe(t *testing.T, ts *emulator.TestServer, region, topicARN string) string {
	t.Helper()
	body := snsQueryOK(t, ts, region, map[string]string{
		"Action":   "Subscribe",
		"TopicArn": topicARN,
		"Protocol": "sqs",
		"Endpoint": "arn:aws:sqs:" + region + ":" + taggingTestAccount + ":inbox",
	})
	var doc struct {
		ARN string `xml:"SubscribeResult>SubscriptionArn"`
	}
	if err := xml.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("decode Subscribe %s: %v (body %s)", topicARN, err, body)
	}
	require.NotEmpty(t, doc.ARN, "Subscribe reports a subscription ARN")
	return doc.ARN
}

// snsListTopicARNs returns the topic ARNs ListTopics reports in a Region.
func snsListTopicARNs(t *testing.T, ts *emulator.TestServer, region string) []string {
	t.Helper()
	body := snsQueryOK(t, ts, region, map[string]string{"Action": "ListTopics"})
	var doc struct {
		ARNs []string `xml:"ListTopicsResult>Topics>member>TopicArn"`
	}
	if err := xml.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("decode ListTopics in %s: %v (body %s)", region, err, body)
	}
	return doc.ARNs
}

// ----- The resolution fix --------------------------------------------------

// TestSNSResolution_ATopicARNFromAnotherRegionDoesNotReachTheLocalTopic is the substituted-record
// assertion. Two Regions each hold a topic of the same name, and the us-west-2 topic's ARN is
// presented to the us-east-1 endpoint. Keying by the caller's Region answered with the *local*
// topic under the foreign topic's ARN, and a caller cannot tell one from the other.
func TestSNSResolution_ATopicARNFromAnotherRegionDoesNotReachTheLocalTopic(t *testing.T) {
	ts := snsTagServer(t)
	eastARN := snsCreateTopicIn(t, ts, snsEastRegion, "orders", nil)
	westARN := snsCreateTopicIn(t, ts, snsWestRegion, "orders", nil)
	require.NotEqual(t, eastARN, westARN, "the two topics differ only by Region")

	attrs, errCode := snsTopicAttributes(t, ts, snsEastRegion, westARN)
	require.Empty(t, errCode, "GetTopicAttributes on a us-west-2 topic ARN")
	assert.Equal(t, westARN, attrs["TopicArn"], "a topic ARN resolves to the topic the ARN names")
	assert.NotEqual(t, eastARN, attrs["TopicArn"],
		"never to the caller's own us-east-1 topic, which is what keying by the request Region did")
}

// TestSNSResolution_AForeignAccountARNDoesNotReachTheCallersTopic is the account half of the same
// defect, and the direction that mattered: UntagResource against a foreign-account ARN answered 200
// while stripping tags from the caller's own same-named topic.
func TestSNSResolution_AForeignAccountARNDoesNotReachTheCallersTopic(t *testing.T) {
	ts := snsTagServer(t)
	arn := snsCreateTopic(t, ts, "billing")
	foreign := strings.Replace(arn, taggingTestAccount, taggingForeignAccount, 1)
	require.NotEqual(t, arn, foreign, "the foreign ARN differs only by account")

	snsQueryOK(t, ts, snsEastRegion, map[string]string{
		"Action":              "TagResource",
		"ResourceArn":         arn,
		"Tags.member.1.Key":   "env",
		"Tags.member.1.Value": "prod",
		"Tags.member.2.Key":   "owner",
		"Tags.member.2.Value": "platform",
	})

	status, _, errCode := snsQuery(t, ts, snsEastRegion, map[string]string{
		"Action":           "UntagResource",
		"ResourceArn":      foreign,
		"TagKeys.member.1": "env",
	})
	assert.Equal(t, "ResourceNotFound", errCode, "a foreign-account ARN names no topic here")
	assert.Equal(t, http.StatusNotFound, status, "SNS publishes ResourceNotFound at HTTP 404")

	assert.Equal(t, map[string]string{"env": "prod", "owner": "platform"}, snsTagMap(t, ts, arn),
		"and the caller's own topic of that name keeps every tag")
}

// TestSNSResolution_ASubscriptionARNIsNotATopic is the anchored-segment assertion, in the form SNS's
// ARN shape permits. A topic ARN carries no type keyword and no separator — the resource portion *is*
// the name — so the only discriminator available is the colon a subscription ARN appends. Taking the
// last segment handed back the subscription's own identifier as a topic name; refusing a resource
// portion containing a colon is what stops that.
func TestSNSResolution_ASubscriptionARNIsNotATopic(t *testing.T) {
	ts := snsTagServer(t)
	topicARN := snsCreateTopic(t, ts, "events")
	subARN := snsSubscribe(t, ts, snsEastRegion, topicARN)
	require.True(t, strings.HasPrefix(subARN, topicARN+":"),
		"a subscription ARN is the topic's ARN with an identifier appended, got %q", subARN)

	snsQueryOK(t, ts, snsEastRegion, map[string]string{
		"Action":              "TagResource",
		"ResourceArn":         topicARN,
		"Tags.member.1.Key":   "env",
		"Tags.member.1.Value": "prod",
	})

	for _, op := range []string{"TagResource", "UntagResource", "ListTagsForResource"} {
		t.Run(op, func(t *testing.T) {
			params := map[string]string{"Action": op, "ResourceArn": subARN}
			switch op {
			case "TagResource":
				params["Tags.member.1.Key"] = "env"
				params["Tags.member.1.Value"] = "staging"
			case "UntagResource":
				params["TagKeys.member.1"] = "env"
			}
			status, _, errCode := snsQuery(t, ts, snsEastRegion, params)
			assert.Equal(t, "InvalidParameter", errCode, "%s refuses a subscription ARN", op)
			assert.Equal(t, http.StatusBadRequest, status, "InvalidParameter is HTTP 400")
		})
	}

	assert.Equal(t, map[string]string{"env": "prod"}, snsTagMap(t, ts, topicARN),
		"and none of the three reached the topic the subscription ARN names")
}

// TestSNSResolution_AMalformedARNIsRefused pins the third defect: the len(parts) >= 6 guard fell
// through to returning its argument, so any string at all became a topic name and was looked up
// rather than refused. A caller then saw ResourceNotFound — the resource is absent — when what was
// wrong was the input.
func TestSNSResolution_AMalformedARNIsRefused(t *testing.T) {
	ts := snsTagServer(t)

	for _, tc := range []struct{ name, arn string }{
		{"too few ARN segments", "arn:aws:sns"},
		{"a bare topic name", "orders"},
		{"another service", "arn:aws:sqs:us-east-1:" + taggingTestAccount + ":orders"},
		{"no Region", "arn:aws:sns::" + taggingTestAccount + ":orders"},
		{"no account", "arn:aws:sns:us-east-1::orders"},
		{"no topic", "arn:aws:sns:us-east-1:" + taggingTestAccount + ":"},
		{"not an ARN at all", "https://sns.us-east-1.amazonaws.com/orders"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, _, errCode := snsQuery(t, ts, snsEastRegion, map[string]string{
				"Action":              "TagResource",
				"ResourceArn":         tc.arn,
				"Tags.member.1.Key":   "env",
				"Tags.member.1.Value": "prod",
			})
			assert.Equal(t, "InvalidParameter", errCode, "%q is refused as a malformed ARN", tc.arn)
			assert.Equal(t, http.StatusBadRequest, status, "InvalidParameter is HTTP 400")
		})
	}
}

// TestSNSResolution_ThreeTagOperationsAnswerResourceNotFound asserts the code name, not the status.
// All three handlers answered Code "NotFound" at 404; SNS publishes ResourceNotFound at 404 on each
// of the three pages, so the status was right and the name was something no SDK models.
func TestSNSResolution_ThreeTagOperationsAnswerResourceNotFound(t *testing.T) {
	ts := snsTagServer(t)
	absent := "arn:aws:sns:" + snsEastRegion + ":" + taggingTestAccount + ":no-such-topic"

	for _, op := range []string{"TagResource", "UntagResource", "ListTagsForResource"} {
		t.Run(op, func(t *testing.T) {
			params := map[string]string{"Action": op, "ResourceArn": absent}
			switch op {
			case "TagResource":
				params["Tags.member.1.Key"] = "env"
				params["Tags.member.1.Value"] = "prod"
			case "UntagResource":
				params["TagKeys.member.1"] = "env"
			}
			status, _, errCode := snsQuery(t, ts, snsEastRegion, params)
			assert.Equal(t, "ResourceNotFound", errCode, "%s on an absent topic", op)
			assert.Equal(t, http.StatusNotFound, status, "SNS publishes ResourceNotFound at HTTP 404")
		})
	}
}

// TestSNSResolution_ASubscriptionARNIsMintedUnderTheTopicsAccountAndRegion asserts the minted side of
// the same rule. A subscription ARN's account and Region segments are the *topic's*, because the ARN
// is the topic's ARN with an identifier appended — so subscribing to a us-west-2 topic from the
// us-east-1 endpoint must not mint a us-east-1 subscription ARN.
func TestSNSResolution_ASubscriptionARNIsMintedUnderTheTopicsAccountAndRegion(t *testing.T) {
	ts := snsTagServer(t)
	westARN := snsCreateTopicIn(t, ts, snsWestRegion, "cross-region", nil)

	subARN := snsSubscribe(t, ts, snsEastRegion, westARN)
	assert.True(t, strings.HasPrefix(subARN, westARN+":"),
		"the subscription ARN extends the topic's own ARN, got %q for topic %q", subARN, westARN)
}

// TestSNSResolution_DeleteTopicRemovesTheOwningRegionsIndexEntry is the store half of the defect,
// which the load half hides. Before the fix DeleteTopic loaded by the ARN's account and Region but
// removed the name from the *caller's* index, so the record vanished while the owning Region's
// ListTopics went on reporting an ARN that resolved to nothing.
func TestSNSResolution_DeleteTopicRemovesTheOwningRegionsIndexEntry(t *testing.T) {
	ts := snsTagServer(t)
	westARN := snsCreateTopicIn(t, ts, snsWestRegion, "transient", nil)
	require.Contains(t, snsListTopicARNs(t, ts, snsWestRegion), westARN, "ListTopics reports it first")

	snsQueryOK(t, ts, snsEastRegion, map[string]string{"Action": "DeleteTopic", "TopicArn": westARN})

	assert.NotContains(t, snsListTopicARNs(t, ts, snsWestRegion), westARN,
		"the owning Region's topic-name index no longer reports it")
}

// ----- Tag parameter decoding ----------------------------------------------

// TestSNSTags_BothParameterSpellingsAreDecoded is the "tag parameter decoded by nobody" case. AWS's
// reference names the members Tags.member.N and TagKeys.member.N, but AWS's own request examples on
// the same pages wire Tags.Tag.1.Key, Tags.Tag.1.Value and TagKeys.TagKey.1. Substrate decoded only
// the first form, so a caller that followed the example got 200 with nothing written or removed.
// Both are accepted because the reference and its example disagree.
func TestSNSTags_BothParameterSpellingsAreDecoded(t *testing.T) {
	for _, tc := range []struct {
		name      string
		tagParams map[string]string
		keyParam  string
	}{
		{
			name: "the documented member spelling",
			tagParams: map[string]string{
				"Tags.member.1.Key": "env", "Tags.member.1.Value": "prod",
				"Tags.member.2.Key": "owner", "Tags.member.2.Value": "platform",
			},
			keyParam: "TagKeys.member.1",
		},
		{
			name: "the spelling AWS's own examples wire",
			tagParams: map[string]string{
				"Tags.Tag.1.Key": "env", "Tags.Tag.1.Value": "prod",
				"Tags.Tag.2.Key": "owner", "Tags.Tag.2.Value": "platform",
			},
			keyParam: "TagKeys.TagKey.1",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := snsTagServer(t)
			arn := snsCreateTopic(t, ts, "spellings")

			params := map[string]string{"Action": "TagResource", "ResourceArn": arn}
			for k, v := range tc.tagParams {
				params[k] = v
			}
			snsQueryOK(t, ts, snsEastRegion, params)
			require.Equal(t, map[string]string{"env": "prod", "owner": "platform"}, snsTagMap(t, ts, arn),
				"TagResource decodes this spelling")

			snsQueryOK(t, ts, snsEastRegion, map[string]string{
				"Action":      "UntagResource",
				"ResourceArn": arn,
				tc.keyParam:   "env",
			})
			assert.Equal(t, map[string]string{"owner": "platform"}, snsTagMap(t, ts, arn),
				"UntagResource decodes this spelling")
		})
	}
}

// TestSNSTags_CreateTopicDecodesTags asserts the parameter CreateTopic publishes and substrate read
// not at all, so a topic created with tags in one call reported none through either API.
func TestSNSTags_CreateTopicDecodesTags(t *testing.T) {
	ts := snsTagServer(t)
	arn := snsCreateTopicIn(t, ts, snsEastRegion, "born-tagged", map[string]string{
		"Tags.member.1.Key":   "env",
		"Tags.member.1.Value": "prod",
		"Tags.member.2.Key":   "cost-center",
		"Tags.member.2.Value": "1234",
	})

	assert.Equal(t, map[string]string{"env": "prod", "cost-center": "1234"}, snsTagMap(t, ts, arn),
		"SNS reports the tags CreateTopic was given")
	assert.Equal(t, map[string]string{"env": "prod", "cost-center": "1234"}, getResourcesTags(t, ts, arn),
		"and so does the tagging API")
}

// TestSNSTags_AnUntaggedTopicReportsNoTags is the empty case, which the two merge paths make easy to
// get wrong in opposite directions: an omitted member and a member holding an empty array both have
// to read back as no tags.
func TestSNSTags_AnUntaggedTopicReportsNoTags(t *testing.T) {
	ts := snsTagServer(t)
	arn := snsCreateTopic(t, ts, "bare")

	require.Empty(t, snsTagList(t, ts, snsEastRegion, arn), "a topic created without tags reports none")

	tagResourcesWith(t, ts, arn, map[string]string{"env": "prod"})
	untagResourcesWith(t, ts, arn, "env")
	assert.Empty(t, snsTagList(t, ts, snsEastRegion, arn),
		"and a topic whose last tag was removed reports none rather than one empty tag")
}

// ----- The #835 row --------------------------------------------------------

// TestTaggingSNS_ATopicTagIsReadBackThroughSNS is the cross-readability assertion #765 requires of
// every row: the tagging API and the owning service must agree where one resource's tags live. Both
// sides build the key through [snsTopicStateKey], which is what makes that true by construction
// rather than by two call sites happening to agree.
func TestTaggingSNS_ATopicTagIsReadBackThroughSNS(t *testing.T) {
	ts := snsTagServer(t)
	arn := snsCreateTopic(t, ts, "alerts")

	tagResourcesWith(t, ts, arn, map[string]string{"env": "prod", "owner": "platform"})

	assert.Equal(t, map[string]string{"env": "prod", "owner": "platform"}, snsTagMap(t, ts, arn),
		"SNS reports the tags the tagging API wrote")
}

// TestTaggingSNS_UntagResourcesRemovesOnlyTheNamedKeys asserts the damaging direction. A removal that
// reaches the wrong record, or removes more than it was asked to, turns an aws:ResourceTag Deny into
// an allow.
func TestTaggingSNS_UntagResourcesRemovesOnlyTheNamedKeys(t *testing.T) {
	ts := snsTagServer(t)
	arn := snsCreateTopic(t, ts, "alerts")

	tagResourcesWith(t, ts, arn, map[string]string{"env": "prod", "owner": "platform"})
	untagResourcesWith(t, ts, arn, "env")

	assert.Equal(t, map[string]string{"owner": "platform"}, snsTagMap(t, ts, arn),
		"only the named key is removed")
}

// TestTaggingSNS_ATagWrittenThroughSNSIsReportedByGetResources is the other direction of the same
// invariant: the scanner must read the member SNS's own TagResource writes. It is the assertion that
// catches a scanner reading the wrong field names out of a record that stores Key/Value.
func TestTaggingSNS_ATagWrittenThroughSNSIsReportedByGetResources(t *testing.T) {
	ts := snsTagServer(t)
	arn := snsCreateTopic(t, ts, "alerts")

	snsQueryOK(t, ts, snsEastRegion, map[string]string{
		"Action":              "TagResource",
		"ResourceArn":         arn,
		"Tags.member.1.Key":   "team",
		"Tags.member.1.Value": "sre",
	})

	assert.Equal(t, map[string]string{"team": "sre"}, getResourcesTags(t, ts, arn),
		"GetResources reports the tag SNS's own TagResource wrote")
}

// TestTaggingSNS_GetResourcesReportsTheTopicAndNothingElse is the scanner half, and the
// colon-terminated-prefix assertion with it.
//
// A resolver arm alone leaves the resource writable and invisible — a caller can tag it and then
// cannot find it. And this namespace also holds "topic_names:{acct}/{region}", whose value is a JSON
// array of names, so scanning a bare "topic" prefix would list it too; asserting the exact set rather
// than membership is what catches that.
func TestTaggingSNS_GetResourcesReportsTheTopicAndNothingElse(t *testing.T) {
	ts := snsTagServer(t)
	first := snsCreateTopic(t, ts, "alpha")
	second := snsCreateTopic(t, ts, "beta")

	assert.ElementsMatch(t, []string{first, second}, getResourcesARNs(t, ts, "sns"),
		"GetResources reports the two topics under an sns filter, and not the topic-name index")
}

// TestTaggingSNS_GetResourcesDoesNotReportATopicFromAnotherRegion asserts the scanner's Region
// scoping. TagResources states "you can only tag resources that are located in the specified AWS
// Region for the AWS account", so a us-west-2 topic must not appear in a us-east-1 GetResources.
func TestTaggingSNS_GetResourcesDoesNotReportATopicFromAnotherRegion(t *testing.T) {
	ts := snsTagServer(t)
	westARN := snsCreateTopicIn(t, ts, snsWestRegion, "elsewhere", nil)

	assert.NotContains(t, getResourcesARNs(t, ts, "sns"), westARN,
		"a us-west-2 topic is not reported by a us-east-1 GetResources")
}

// TestTaggingSNS_AForeignAccountARNCannotBeTagged is the account-isolation assertion at the tagging
// arm. It is emergent rather than guarded, as ACM's is: [snsResolveARN] builds an account-qualified
// state key, so a foreign-account ARN builds a key nothing is stored at, the merge fails "resource
// not found", and the caller gets a FailedResourcesMap entry instead of a silent success.
//
// Unlike KMS (#922) there is no explicit refusal to assert, because no cross-account statement
// appears on any of SNS's three tagging pages and inventing one would be substrate's invention
// rather than its reading.
func TestTaggingSNS_AForeignAccountARNCannotBeTagged(t *testing.T) {
	ts := snsTagServer(t)
	arn := snsCreateTopic(t, ts, "isolated")
	foreign := strings.Replace(arn, taggingTestAccount, taggingForeignAccount, 1)
	require.NotEqual(t, arn, foreign, "the foreign ARN differs only by account")

	failures := tagResourcesFailures(t, ts, "TagResources", foreign)
	require.Contains(t, failures, foreign, "a foreign-account topic ARN is refused")

	assert.Empty(t, snsTagMap(t, ts, arn),
		"and the caller's own topic of that name is untouched")
}

// TestTaggingSNS_ASubscriptionARNIsNotTaggableThroughTheTaggingAPI asserts the tagging API draws the
// same boundary SNS's own operations do. Both arms go through [snsParseTopicARN], which is what makes
// them unable to disagree about which ARNs name a topic.
func TestTaggingSNS_ASubscriptionARNIsNotTaggableThroughTheTaggingAPI(t *testing.T) {
	ts := snsTagServer(t)
	topicARN := snsCreateTopic(t, ts, "guarded")
	subARN := snsSubscribe(t, ts, snsEastRegion, topicARN)

	for _, op := range []string{"TagResources", "UntagResources"} {
		t.Run(op, func(t *testing.T) {
			failures := tagResourcesFailures(t, ts, op, subARN)
			assert.Contains(t, failures, subARN, "%s refuses a subscription ARN", op)
		})
	}

	assert.Empty(t, snsTagMap(t, ts, topicARN),
		"and nothing was written to the topic the subscription ARN names")
}

// ----- Determinism ---------------------------------------------------------

// TestTaggingSNS_TagOrderIsDeterministic is the #862 rule reached from both sides. SNS's own merge
// was already deterministic — it walks indexed request parameters, not a Go map — but it preserved
// insertion order while the tagging API's arm emits key-sorted order, so one topic's tags came back
// in two different orders depending on which API was asked. Both arms sort, and the read sorts too,
// so a record written by an earlier substrate reads back in one order as well.
func TestTaggingSNS_TagOrderIsDeterministic(t *testing.T) {
	ts := snsTagServer(t)
	arn := snsCreateTopic(t, ts, "ordered")

	// Enough keys that an unsorted map range is overwhelmingly unlikely to come out ascending, and
	// written in descending order so insertion order is not the answer either.
	want := []string{"alpha", "beta", "delta", "epsilon", "gamma", "zeta"}

	t.Run("through SNS's own TagResource", func(t *testing.T) {
		params := map[string]string{"Action": "TagResource", "ResourceArn": arn}
		for i, key := range []string{"zeta", "gamma", "epsilon", "delta", "beta", "alpha"} {
			n := strconv.Itoa(i + 1)
			params["Tags.member."+n+".Key"] = key
			params["Tags.member."+n+".Value"] = key
		}
		snsQueryOK(t, ts, snsEastRegion, params)
		assert.Equal(t, want, snsTagKeyOrder(t, ts, arn), "SNS stores tags in key order")
	})

	t.Run("through the tagging API", func(t *testing.T) {
		untagResourcesWith(t, ts, arn, want...)
		tagResourcesWith(t, ts, arn, map[string]string{
			"zeta": "5", "epsilon": "4", "delta": "3", "gamma": "2", "beta": "1", "alpha": "0",
		})
		assert.Equal(t, want, snsTagKeyOrder(t, ts, arn),
			"mergeRecordTagListTags stores tags in key order")
	})
}
