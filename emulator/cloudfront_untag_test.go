package emulator_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// CloudFront tagging reachability (#883).
//
// CloudFront implemented TagResource and ListTagsForResource and not UntagResource, and
// parseCloudFrontOperation mapped *any* Resource-bearing POST to TagResource — so an
// Operation=Untag request ran the tag path, where a <TagKeys> body decoded as <Tags> into an
// empty item list, the decode error was discarded, and the distribution was written back
// byte-identical with the tagging success. A caller removing a tag was told it worked and the
// tag was still there.
//
// Every case here goes over the wire against a real server, and every tag assertion is read
// back through CloudFront's own ListTagsForResource rather than out of the state store: a
// helper that wrote or read state directly could not tell a removal that happened from a
// removal that was reported as happening, which is the whole of this defect. The assertions
// are on the raw XML for the same reason a decoded struct cannot carry — an absent <Items>
// member and an empty one decode to the same Go value.

// cloudfrontTaggingPath is the single path all three CloudFront tagging operations are
// published under; they are told apart by the query string alone.
const cloudfrontTaggingPath = "/2020-05-31/tagging"

// cloudfrontRequest sends one CloudFront REST/XML request and returns the status and body.
//
// The request is signed with the built-in test key so the caller resolves to account
// 123456789012; an unsigned one would land in 000000000000 and the ARN the distribution
// reports would name a different account than the one these calls are made as. Signature
// verification is off by default, so the signature itself need not be computed — the
// credential scope is what the parser reads. The scope's region is us-east-1 because
// CloudFront is global and signs there, which is also where its records are stored.
func cloudfrontRequest(t *testing.T, ts *emulator.TestServer, method, path, query, body string) (int, string) {
	t.Helper()

	url := ts.URL + path
	if query != "" {
		url += "?" + query
	}
	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	require.NoError(t, err, "build %s %s", method, path)
	req.Host = "cloudfront.amazonaws.com"
	req.Header.Set("Content-Type", "application/xml")
	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential=AKIATEST12345678901/20260101/us-east-1"+
			"/cloudfront/aws4_request, SignedHeaders=host, Signature=fake")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "%s %s", method, path)
	defer resp.Body.Close() //nolint:errcheck // the body is drained immediately below.

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read %s %s body", method, path)
	return resp.StatusCode, string(raw)
}

// cloudfrontErrorCode reads the Code out of a REST-XML refusal, whose document is
// <ErrorResponse><Error><Code>…</Code></Error></ErrorResponse>.
func cloudfrontErrorCode(t *testing.T, body string) string {
	t.Helper()
	var doc struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Code    string   `xml:"Error>Code"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &doc), "decode error document: %s", body)
	return doc.Code
}

// cloudfrontTagServer starts a server holding one distribution and returns its ARN, read out
// of the CreateDistribution response rather than assembled here.
func cloudfrontTagServer(t *testing.T) (*emulator.TestServer, string) {
	t.Helper()

	ts := emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount))

	status, body := cloudfrontRequest(t, ts, http.MethodPost, "/2020-05-31/distribution", "",
		`<DistributionConfig><Comment>tagging</Comment><Enabled>true</Enabled></DistributionConfig>`)
	require.Equal(t, http.StatusCreated, status, "CreateDistribution: %s", body)

	var created struct {
		XMLName xml.Name `xml:"Distribution"`
		ARN     string   `xml:"ARN"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &created), "decode CreateDistribution: %s", body)
	require.NotEmpty(t, created.ARN, "CreateDistribution reports an ARN")
	return ts, created.ARN
}

// cloudfrontTagBody is a TagResource body in the shape the API reference publishes, namespace
// included, because that is what an SDK sends and a decoder that matched on the namespaced
// name would reject it.
func cloudfrontTagBody(pairs ...string) string {
	var b strings.Builder
	b.WriteString(`<Tags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/"><Items>`)
	for i := 0; i+1 < len(pairs); i += 2 {
		b.WriteString("<Tag><Key>" + pairs[i] + "</Key><Value>" + pairs[i+1] + "</Value></Tag>")
	}
	b.WriteString(`</Items></Tags>`)
	return b.String()
}

// cloudfrontUntagBody is an UntagResource body in the published shape.
func cloudfrontUntagBody(keys ...string) string {
	var b strings.Builder
	b.WriteString(`<TagKeys xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/"><Items>`)
	for _, k := range keys {
		b.WriteString("<Key>" + k + "</Key>")
	}
	b.WriteString(`</Items></TagKeys>`)
	return b.String()
}

// cloudfrontTag calls TagResource and requires the documented 204.
func cloudfrontTag(t *testing.T, ts *emulator.TestServer, arn, body string) {
	t.Helper()
	status, respBody := cloudfrontRequest(t, ts, http.MethodPost, cloudfrontTaggingPath,
		"Operation=Tag&Resource="+arn, body)
	require.Equal(t, http.StatusNoContent, status, "TagResource: %s", respBody)
}

// cloudfrontListTagsXML returns the raw ListTagsForResource document.
func cloudfrontListTagsXML(t *testing.T, ts *emulator.TestServer, arn string) string {
	t.Helper()
	status, body := cloudfrontRequest(t, ts, http.MethodGet, cloudfrontTaggingPath,
		"Resource="+arn, "")
	require.Equal(t, http.StatusOK, status, "ListTagsForResource: %s", body)
	return body
}

// TestCloudFrontUntagResource_IsNotAnsweredAsTheTaggingSuccess is #883's gate: an
// Operation=Untag request must not answer the tagging success while leaving the tags
// unchanged. It fails on the parent commit, where the request was routed to TagResource,
// answered 204, and removed nothing.
func TestCloudFrontUntagResource_IsNotAnsweredAsTheTaggingSuccess(t *testing.T) {
	t.Parallel()

	ts, arn := cloudfrontTagServer(t)
	cloudfrontTag(t, ts, arn, cloudfrontTagBody("env", "prod"))
	require.Contains(t, cloudfrontListTagsXML(t, ts, arn), "<Key>env</Key>",
		"the tag has to be there for its removal to mean anything")

	status, body := cloudfrontRequest(t, ts, http.MethodPost, cloudfrontTaggingPath,
		"Operation=Untag&Resource="+arn, cloudfrontUntagBody("env"))
	require.Equal(t, http.StatusNoContent, status,
		"UntagResource answers the documented HTTP/1.1 204 with an empty body: %s", body)
	assert.Empty(t, body, "the 204 carries no body")

	after := cloudfrontListTagsXML(t, ts, arn)
	assert.NotContains(t, after, "<Key>env</Key>",
		"a 204 from an untag request that left the tag in place is the #883 misroute: "+
			"the request was answered as TagResource, which decoded <TagKeys> as <Tags>, "+
			"added nothing and reported success")
	// Asserted on the raw document rather than a decoded struct: an absent member and an empty
	// one are the same value once decoded, so only the document can say the member set is
	// empty. The assertion is on the absence of any <Tag> member and not on the <Items>
	// wrapper, because the reference does not say whether a resource with no tags answers
	// <Items/> or omits the wrapper, and substrate emits the empty wrapper.
	assert.NotContains(t, after, "<Tag>", "the document carries no Tag member at all")
}

// TestCloudFrontUntagResource_RemovesOnlyTheNamedKeys pins that the removal is scoped to the
// keys in the body. A handler that cleared the map, or one that removed the first key only,
// passes the gate above and loses a tag a caller never named.
func TestCloudFrontUntagResource_RemovesOnlyTheNamedKeys(t *testing.T) {
	t.Parallel()

	ts, arn := cloudfrontTagServer(t)
	cloudfrontTag(t, ts, arn, cloudfrontTagBody("env", "prod", "owner", "platform", "cost", "eng"))

	status, body := cloudfrontRequest(t, ts, http.MethodPost, cloudfrontTaggingPath,
		"Operation=Untag&Resource="+arn, cloudfrontUntagBody("env", "cost"))
	require.Equal(t, http.StatusNoContent, status, "UntagResource: %s", body)

	after := cloudfrontListTagsXML(t, ts, arn)
	assert.NotContains(t, after, "<Key>env</Key>", "env was named")
	assert.NotContains(t, after, "<Key>cost</Key>", "cost was named")
	assert.Contains(t, after, "<Tag><Key>owner</Key><Value>platform</Value></Tag>",
		"owner was not named and keeps its value")
}

// TestCloudFrontUntagResource_AnAbsentKeySucceeds covers the case AWS does not document.
//
// The reference publishes an unconditional 204 for the operation and lists no error for a key
// the resource does not carry, but it does not address the case either way — so success here
// is substrate's reading, recorded on [CloudFrontPlugin.untagResource] and pinned by this test
// so a later change cannot make a consumer's second teardown pass fail silently.
func TestCloudFrontUntagResource_AnAbsentKeySucceeds(t *testing.T) {
	t.Parallel()

	ts, arn := cloudfrontTagServer(t)
	cloudfrontTag(t, ts, arn, cloudfrontTagBody("env", "prod"))

	status, body := cloudfrontRequest(t, ts, http.MethodPost, cloudfrontTaggingPath,
		"Operation=Untag&Resource="+arn, cloudfrontUntagBody("never-set"))
	require.Equal(t, http.StatusNoContent, status,
		"removing a key that is not present succeeds: %s", body)

	assert.Contains(t, cloudfrontListTagsXML(t, ts, arn), "<Key>env</Key>",
		"and it removes nothing else")
}

// TestCloudFrontUntagResource_AMissingARNIsRefused pins that the Resource parameter is
// required, which the reference documents for ListTagsForResource and omits for the other two.
func TestCloudFrontUntagResource_AMissingARNIsRefused(t *testing.T) {
	t.Parallel()

	ts, _ := cloudfrontTagServer(t)
	status, body := cloudfrontRequest(t, ts, http.MethodPost, cloudfrontTaggingPath,
		"Operation=Untag", cloudfrontUntagBody("env"))
	assert.Equal(t, http.StatusBadRequest, status, "body: %s", body)
	assert.Equal(t, "InvalidArgument", cloudfrontErrorCode(t, body))
}

// TestCloudFrontTagging_ARequestThatNamesNoOperationNeverWrites is the second half of #883's
// first acceptance criterion: the dispatch must stop mapping every Resource-bearing POST to
// TagResource, so a request whose Operation substrate cannot name is refused rather than
// performing whichever tagging write happens to be first.
//
// Each case asserts both halves — the refusal, and that the tag the distribution already
// carries is untouched afterwards. The refusal alone would pass on a handler that answered an
// error after writing.
func TestCloudFrontTagging_ARequestThatNamesNoOperationNeverWrites(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		query    string
		body     string
		wantCode string
		why      string
	}{{
		name:     "no Operation at all",
		query:    "Resource=%s",
		body:     cloudfrontUntagBody("env"),
		wantCode: "InvalidAction",
		why:      "a bare Resource-bearing POST used to be TagResource, which is a write",
	}, {
		name:     "an Operation value neither Tag nor Untag",
		query:    "Operation=Sideways&Resource=%s",
		body:     cloudfrontUntagBody("env"),
		wantCode: "InvalidAction",
		why:      "an unrecognized value must not fall through to a write",
	}, {
		name:     "a Tags body sent to Untag",
		query:    "Operation=Untag&Resource=%s",
		body:     cloudfrontTagBody("env", "staging"),
		wantCode: "InvalidArgument",
		why:      "the root element is part of the request shape, so the wrong document is refused",
	}, {
		name:     "a TagKeys body sent to Tag",
		query:    "Operation=Tag&Resource=%s",
		body:     cloudfrontUntagBody("env"),
		wantCode: "InvalidArgument",
		why:      "this is the exact body the misroute used to swallow, answering 204 having written nothing",
	}, {
		name:     "an untag with no body",
		query:    "Operation=Untag&Resource=%s",
		body:     "",
		wantCode: "InvalidArgument",
		why:      "TagKeys is documented Required: Yes, and a 204 removing nothing is the defect itself",
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ts, arn := cloudfrontTagServer(t)
			cloudfrontTag(t, ts, arn, cloudfrontTagBody("env", "prod"))

			status, body := cloudfrontRequest(t, ts, http.MethodPost, cloudfrontTaggingPath,
				strings.Replace(tc.query, "%s", arn, 1), tc.body)
			assert.Equal(t, http.StatusBadRequest, status, "%s — body: %s", tc.why, body)
			assert.Equal(t, tc.wantCode, cloudfrontErrorCode(t, body), tc.why)

			assert.Contains(t, cloudfrontListTagsXML(t, ts, arn),
				"<Tag><Key>env</Key><Value>prod</Value></Tag>",
				"a refused request must not have written: %s", tc.why)
		})
	}
}

// TestCloudFrontTagging_OperationNamesComeFromTheQueryString asserts the name the *pipeline*
// resolves, which is what authorization, metering and the event log record. The plugin's
// dispatch and this resolution are the same function (operationResolvers["cloudfront"]), so a
// request named TagResource here would be authorized as cloudfront:TagResource even when the
// caller asked to untag — the #572 class of defect layered on top of #883's.
func TestCloudFrontTagging_OperationNamesComeFromTheQueryString(t *testing.T) {
	t.Parallel()

	const arn = "arn:aws:cloudfront::123456789012:distribution/E1AAAAAAAAAAAA"
	cases := []struct {
		name, method, query, wantOp string
	}{
		{"tag", http.MethodPost, "?Operation=Tag&Resource=" + arn, "TagResource"},
		{"untag", http.MethodPost, "?Operation=Untag&Resource=" + arn, "UntagResource"},
		{"untag is case-insensitive, as Tag already was", http.MethodPost,
			"?Operation=untag&Resource=" + arn, "UntagResource"},
		{"list", http.MethodGet, "?Resource=" + arn, "ListTagsForResource"},
		// An unresolved operation keeps its verb: a verb is a more honest answer than another
		// operation's name, and it is what makes the plugin refuse instead of write.
		{"an unrecognized Operation resolves to nothing", http.MethodPost,
			"?Operation=Sideways&Resource=" + arn, http.MethodPost},
		{"no Operation resolves to nothing", http.MethodPost, "?Resource=" + arn, http.MethodPost},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := parseWire(t, tc.method, "cloudfront.amazonaws.com",
				cloudfrontTaggingPath+tc.query, "")
			assert.Equal(t, tc.wantOp, req.Operation)
		})
	}
}
