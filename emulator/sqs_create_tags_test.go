package emulator_test

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CreateQueue's published tag member, and the two query spellings (#1087).
//
// Every assertion here is made over the wire and read back through a *different* door than the one
// that wrote — ListQueueTags for the service's own view, GetResources for the tagging API's — because
// #765's rule is that a tag which landed and a tag which was reported as landed are only
// distinguishable that way. The defect these cover was exactly a call that answered 200 with the tag
// set discarded.

// sqsCreateTagsTarget is CreateQueue's and ListQueueTags' JSON door.
var sqsCreateTagsTarget = signedRequestTarget{
	host:        "sqs.us-east-1.amazonaws.com",
	target:      "AmazonSQS",
	signingName: "sqs",
}

// sqsCreateTagsQueue posts a JSON CreateQueue and returns the queue URL, the status and any error
// code, so a caller can assert a refusal as well as a success.
func sqsCreateTagsQueue(t *testing.T, ts *emulator.TestServer, body map[string]any) (string, int, string) {
	t.Helper()
	var out struct {
		QueueURL string `json:"QueueUrl"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, sqsCreateTagsTarget, taggingTestAccount, "CreateQueue", body), &out)
	return out.QueueURL, status, errCode
}

// sqsCreateTagsList reads a queue's tags back through ListQueueTags' JSON door.
func sqsCreateTagsList(t *testing.T, ts *emulator.TestServer, queueURL string) map[string]string {
	t.Helper()
	var out struct {
		Tags map[string]string `json:"Tags"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, sqsCreateTagsTarget, taggingTestAccount, "ListQueueTags",
			map[string]any{"QueueUrl": queueURL}), &out)
	require.Emptyf(t, errCode, "ListQueueTags %s", queueURL)
	require.Equalf(t, http.StatusOK, status, "ListQueueTags %s", queueURL)
	return out.Tags
}

// sqsCreateTagsQuery posts a form-encoded SQS request as [taggingTestAccount] and returns the status
// and raw body.
//
// signedRequest cannot serve this: the query protocol is the whole point of the spelling assertions
// below, and it marshals JSON. The body is a caller-supplied string rather than url.Values so that a
// test can write the exact parameter names AWS's sample publishes.
func sqsCreateTagsQuery(t *testing.T, ts *emulator.TestServer, body string) (int, string) {
	t.Helper()
	creds, ok := ts.CredentialsFor(taggingTestAccount)
	require.Truef(t, ok, "no credential registered for account %s", taggingTestAccount)

	host := sqsCreateTagsTarget.host
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", strings.NewReader(body))
	require.NoError(t, err, "build query request")
	req.Host = host
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Amz-Date", sigV4TestDateTime)
	req.Header.Set("Authorization", sigV4Header(http.MethodPost, "/", host, "sqs", "us-east-1",
		sigV4TestDateTime, []byte(body), creds.AccessKeyID, creds.SecretAccessKey))

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "send query request")
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read query response")
	return resp.StatusCode, string(raw)
}

// TestSQSCreateQueue_JSONTagsAreStoredAndReadableBothWays asserts the round trip AWS's own
// CreateQueue sample describes.
//
// The member is spelled lowercase here, which is how API_CreateQueue publishes it and how the
// sample sends it — not the capitalised Tags of TagQueue. That is the assertion, not a detail: a
// handler decoding only the capitalised spelling would drop the published request, and this passes
// only because encoding/json falls back to a case-insensitive member match.
func TestSQSCreateQueue_JSONTagsAreStoredAndReadableBothWays(t *testing.T) {
	ts := arnGuardServer(t)

	queueURL, status, errCode := sqsCreateTagsQueue(t, ts, map[string]any{
		"QueueName":  "create-tags-json",
		"Attributes": map[string]string{"VisibilityTimeout": "40"},
		"tags":       map[string]string{"QueueType": "Production", "env": "staging"},
	})
	require.Empty(t, errCode, "CreateQueue with tags")
	require.Equal(t, http.StatusOK, status, "CreateQueue with tags")
	require.NotEmpty(t, queueURL, "CreateQueue reports a URL")

	assert.Equal(t, map[string]string{"QueueType": "Production", "env": "staging"},
		sqsCreateTagsList(t, ts, queueURL),
		"ListQueueTags reports what CreateQueue was given")

	// And the tagging API's own view, which reaches the record through a different scanner. Before
	// #1087 the queue appeared here with no tags at all — or not at all, since GetResources omits a
	// resource that has never carried a tag.
	var found bool
	for _, rm := range getResourcesMappings(t, ts, "sqs") {
		if !strings.HasSuffix(rm.ResourceARN, ":create-tags-json") {
			continue
		}
		found = true
		reported := make(map[string]string, len(rm.Tags))
		for _, tag := range rm.Tags {
			reported[tag.Key] = tag.Value
		}
		assert.Equal(t, map[string]string{"QueueType": "Production", "env": "staging"}, reported,
			"GetResources reports the create-time tags")
	}
	assert.True(t, found, "GetResources reports the queue at all")
}

// TestSQSCreateQueue_QueryAcceptsBothPublishedAndSerialisedSpellings covers the provenance finding
// this issue turned on: AWS's query sample for CreateQueue is *unindexed*
// (&Tag.Key=QueueType&Tag.Value=Production) and publishes no Tag.N.Key form anywhere, while a
// query-protocol SDK serialiser emits the indexed form for a map member. Both arrive in practice, so
// both are read; see [sqsParseQueryTags].
func TestSQSCreateQueue_QueryAcceptsBothPublishedAndSerialisedSpellings(t *testing.T) {
	tests := []struct {
		name   string
		queue  string
		params string
		want   map[string]string
	}{
		{
			name:   "the unindexed form AWS publishes",
			queue:  "create-tags-unindexed",
			params: "&Tag.Key=QueueType&Tag.Value=Production",
			want:   map[string]string{"QueueType": "Production"},
		},
		{
			name:   "the indexed form a serialiser emits",
			queue:  "create-tags-indexed",
			params: "&Tag.1.Key=QueueType&Tag.1.Value=Production&Tag.2.Key=env&Tag.2.Value=staging",
			want:   map[string]string{"QueueType": "Production", "env": "staging"},
		},
		{
			name:   "no tag member at all",
			queue:  "create-tags-absent",
			params: "",
			want:   map[string]string{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts := arnGuardServer(t)
			status, body := sqsCreateTagsQuery(t, ts,
				"Action=CreateQueue&Version=2012-11-05&QueueName="+tc.queue+tc.params)
			require.Equalf(t, http.StatusOK, status, "CreateQueue: %s", body)

			assert.Equal(t, tc.want, sqsCreateTagsList(t, ts, sqsCreateTagsURLFrom(t, body)),
				"ListQueueTags reports the query tags")
		})
	}
}

// sqsCreateTagsURLFrom pulls the QueueUrl out of a query-protocol CreateQueue response, so the tests
// read the URL the server reported rather than rebuilding it from the URL-shaping helper they would
// then be unable to catch a change in.
func sqsCreateTagsURLFrom(t *testing.T, body string) string {
	t.Helper()
	const open, close_ = "<QueueUrl>", "</QueueUrl>"
	start := strings.Index(body, open)
	end := strings.Index(body, close_)
	require.Truef(t, start >= 0 && end > start, "no QueueUrl in %s", body)
	return body[start+len(open) : end]
}

// TestSQSTagQueue_AcceptsThePublishedUnindexedForm pins the consequence of factoring the parser out:
// TagQueue reads the unindexed form too, which is the one AWS's TagQueue sample publishes and the one
// substrate dropped. It is a widening — every request TagQueue accepted before is still accepted —
// which is why it lands with #1087 rather than needing its own reversal.
func TestSQSTagQueue_AcceptsThePublishedUnindexedForm(t *testing.T) {
	ts := arnGuardServer(t)

	queueURL, status, errCode := sqsCreateTagsQueue(t, ts, map[string]any{"QueueName": "tag-queue-unindexed"})
	require.Empty(t, errCode, "CreateQueue")
	require.Equal(t, http.StatusOK, status, "CreateQueue")

	status, body := sqsCreateTagsQuery(t, ts,
		"Action=TagQueue&Version=2012-11-05&QueueUrl="+queueURL+"&Tag.Key=QueueType&Tag.Value=Production")
	require.Equalf(t, http.StatusOK, status, "TagQueue: %s", body)

	assert.Equal(t, map[string]string{"QueueType": "Production"}, sqsCreateTagsList(t, ts, queueURL),
		"TagQueue stored the unindexed pair")

	// The indexed form still works, which is what makes this a widening rather than a swap.
	status, body = sqsCreateTagsQuery(t, ts,
		"Action=TagQueue&Version=2012-11-05&QueueUrl="+queueURL+"&Tag.1.Key=env&Tag.1.Value=staging")
	require.Equalf(t, http.StatusOK, status, "TagQueue indexed: %s", body)
	assert.Equal(t, map[string]string{"QueueType": "Production", "env": "staging"},
		sqsCreateTagsList(t, ts, queueURL), "both spellings merge into one set")
}

// TestSQSCreateQueue_AnIdempotentHitDoesNotRetag records substrate's reading of a case AWS publishes
// nothing about: a second CreateQueue for an existing queue is idempotent, and a tag set on that
// request is not applied. A call that created nothing tagged nothing; TagQueue is the door that
// publishes retagging.
func TestSQSCreateQueue_AnIdempotentHitDoesNotRetag(t *testing.T) {
	ts := arnGuardServer(t)

	queueURL, status, errCode := sqsCreateTagsQueue(t, ts, map[string]any{
		"QueueName": "create-tags-idempotent",
		"tags":      map[string]string{"QueueType": "Production"},
	})
	require.Empty(t, errCode, "first CreateQueue")
	require.Equal(t, http.StatusOK, status, "first CreateQueue")

	again, status, errCode := sqsCreateTagsQueue(t, ts, map[string]any{
		"QueueName": "create-tags-idempotent",
		"tags":      map[string]string{"QueueType": "Development", "added": "later"},
	})
	require.Empty(t, errCode, "second CreateQueue is idempotent")
	require.Equal(t, http.StatusOK, status, "second CreateQueue is idempotent")
	require.Equal(t, queueURL, again, "and reports the same URL")

	assert.Equal(t, map[string]string{"QueueType": "Production"}, sqsCreateTagsList(t, ts, queueURL),
		"the existing queue's tags are untouched")
}

// TestSQSCreateQueue_FiftyTagsIsARecommendationNotABound is AC4 as a test rather than only as prose.
//
// API_CreateQueue's `tags` member publishes no Map Entries and no Length constraint, the fifty-tag
// figure is worded as a recommendation ("Adding more than 50 tags to a queue isn't recommended") on
// both this page and TagQueue's, and neither Errors list carries a too-many-tags code. So there is
// nothing to refuse with, and sixty tags are stored. The test exists so that a later reader who sees
// Kinesis refuse at fifty-one does not "fix" SQS by analogy.
func TestSQSCreateQueue_FiftyTagsIsARecommendationNotABound(t *testing.T) {
	ts := arnGuardServer(t)

	tags := make(map[string]string, 60)
	for i := range 60 {
		tags[fmt.Sprintf("key-%02d", i)] = fmt.Sprintf("value-%02d", i)
	}
	queueURL, status, errCode := sqsCreateTagsQueue(t, ts, map[string]any{
		"QueueName": "create-tags-sixty",
		"tags":      tags,
	})
	require.Empty(t, errCode, "CreateQueue with sixty tags is not refused")
	require.Equal(t, http.StatusOK, status, "CreateQueue with sixty tags is not refused")

	assert.Len(t, sqsCreateTagsList(t, ts, queueURL), 60, "all sixty are stored")
}
