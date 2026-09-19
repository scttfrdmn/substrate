package emulator_test

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// One SQS state key, addressed the same way by every reader (#826).
//
// The SQS plugin stored a queue under "queue:<account>/<name>" — its own sqsURLKey took the last
// two components of a queue URL — while the Resource Groups Tagging API and the authorizer both
// addressed "queue:<name>". Neither reader ever saw a real queue: TagResources created a phantom
// record and answered 200, and every aws:ResourceTag condition on an SQS request was
// unsatisfiable. The key is "queue:<account>/<region>/<name>" since #1088; every reader here now
// derives it from sqsQueueStateKey, so the three cannot disagree about its shape again.
//
// Every assertion here goes through an API call rather than through state, because the defect was
// precisely state agreeing with a test that no caller agreed with: the fixtures that covered both
// paths seeded the wrong key themselves, so both passed. A queue in this file is created by
// CreateQueue, and a tag is read back through the other service's own call.

const sqsKeyOtherAccount = "999999999999"

// sqsQueryRequest sends an SQS query-protocol request and returns the XML body. The query protocol
// puts the operation in Action, which is how the AWS CLI still calls SQS.
func sqsQueryRequest(t *testing.T, ts *httptest.Server, action string, params map[string]string) string {
	t.Helper()
	form := url.Values{}
	form.Set("Action", action)
	form.Set("Version", "2012-11-05")
	for k, v := range params {
		form.Set(k, v)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		ts.URL+"/", strings.NewReader(form.Encode()))
	require.NoError(t, err)
	req.Host = "sqs.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", action, body)
	return string(body)
}

// sqsCreateQueue creates a queue through CreateQueue and returns its URL, so no test here has to
// know which key the plugin chose.
func sqsCreateQueue(t *testing.T, ts *httptest.Server, name string) string {
	t.Helper()
	body := sqsQueryRequest(t, ts, "CreateQueue", map[string]string{"QueueName": name})
	var out struct {
		QueueURL string `xml:"CreateQueueResult>QueueUrl"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &out))
	require.NotEmpty(t, out.QueueURL, "CreateQueue returned no URL: %s", body)
	return out.QueueURL
}

// sqsListQueueTags reads a queue's tags back through SQS's own ListQueueTags.
func sqsListQueueTags(t *testing.T, ts *httptest.Server, queueURL string) map[string]string {
	t.Helper()
	body := sqsQueryRequest(t, ts, "ListQueueTags", map[string]string{"QueueUrl": queueURL})
	var out struct {
		Tags []struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		} `xml:"ListQueueTagsResult>Tag"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &out))
	tags := make(map[string]string, len(out.Tags))
	for _, tag := range out.Tags {
		tags[tag.Key] = tag.Value
	}
	return tags
}

// TestSQS_ATagSetThroughTheTaggingAPIIsReadableThroughSQS is #826's first criterion, and the whole
// issue in one sequence: create the queue the way a caller creates one, tag it through the Resource
// Groups Tagging API, and read it back through both services.
func TestSQS_ATagSetThroughTheTaggingAPIIsReadableThroughSQS(t *testing.T) {
	ts, _ := newTaggingTestServer(t)

	queueURL := sqsCreateQueue(t, ts, "round-trip-queue")
	arn := "arn:aws:sqs:us-east-1:" + taggingTestAccountID + ":round-trip-queue"

	resp := taggingRequest(t, ts, "TagResources", map[string]any{
		"ResourceARNList": []string{arn},
		"Tags":            map[string]string{"Team": "platform"},
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Empty(t, out["FailedResourcesMap"],
		"TagResources should resolve a queue that CreateQueue just created")

	assert.Equal(t, map[string]string{"Team": "platform"}, sqsListQueueTags(t, ts, queueURL),
		"the tag reaches the record SQS itself reads")

	// And the other direction: GetResources reports the tag it wrote, rather than reporting the
	// real queue with no tags beside a phantom record carrying them.
	getResp := taggingRequest(t, ts, "GetResources", map[string]any{
		"ResourceTypeFilters": []string{"sqs"},
	})
	defer getResp.Body.Close() //nolint:errcheck
	var got map[string]any
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&got))
	list, _ := got["ResourceTagMappingList"].([]any)
	require.Len(t, list, 1, "one queue exists, so one mapping should come back")
	mapping := list[0].(map[string]any)
	assert.Equal(t, arn, mapping["ResourceARN"])
	tagList, _ := mapping["Tags"].([]any)
	require.Len(t, tagList, 1, "GetResources should report the tag TagResources wrote")
	assert.Equal(t, "platform", tagList[0].(map[string]any)["Value"])
}

// TestSQS_ATagSetThroughSQSIsReportedByTheTaggingAPI is the same round trip written the other way
// round, since the two writers reached different records and so failed independently.
func TestSQS_ATagSetThroughSQSIsReportedByTheTaggingAPI(t *testing.T) {
	ts, _ := newTaggingTestServer(t)

	queueURL := sqsCreateQueue(t, ts, "sqs-tagged-queue")
	sqsQueryRequest(t, ts, "TagQueue", map[string]string{
		"QueueUrl":    queueURL,
		"Tag.1.Key":   "Env",
		"Tag.1.Value": "prod",
	})

	resp := taggingRequest(t, ts, "GetResources", map[string]any{
		"TagFilters": []map[string]any{{"Key": "Env", "Values": []string{"prod"}}},
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	list, _ := out["ResourceTagMappingList"].([]any)
	require.Len(t, list, 1, "a tag filter should match the queue SQS tagged")
	assert.Equal(t, "arn:aws:sqs:us-east-1:"+taggingTestAccountID+":sqs-tagged-queue",
		list[0].(map[string]any)["ResourceARN"])
}

// TestSQS_AnARNNamingAnotherAccountDoesNotResolveThisAccountsQueue pins the half of #826's fix that
// is about *which* account, not whether one is present at all.
//
// The account comes from the ARN, as it does for the IAM and EC2 arms. Taking it from the request
// context instead would make a cross-account ARN silently tag the caller's own same-named queue,
// which is a worse failure than the phantom write: it succeeds, against the wrong resource.
func TestSQS_AnARNNamingAnotherAccountDoesNotResolveThisAccountsQueue(t *testing.T) {
	ts, _ := newTaggingTestServer(t)

	queueURL := sqsCreateQueue(t, ts, "shared-name")

	resp := taggingRequest(t, ts, "TagResources", map[string]any{
		"ResourceARNList": []string{
			"arn:aws:sqs:us-east-1:" + sqsKeyOtherAccount + ":shared-name",
		},
		"Tags": map[string]string{"Team": "theirs"},
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.NotEmpty(t, out["FailedResourcesMap"],
		"an ARN naming another account must not resolve this account's queue")
	assert.Empty(t, sqsListQueueTags(t, ts, queueURL),
		"and the caller's own queue is untouched")
}

// TestSQS_AResourceTagConditionMatchesAQueueCreatedThroughTheAPI is #826's other half. The
// authorizer read the same wrong key, so a Deny scoped by aws:ResourceTag against a real queue
// published no tag to match and silently allowed.
//
// The Deny is what carries the assertion: a condition that cannot see the tag turns an explicit
// Deny into an Allow, which is the direction that matters.
func TestSQS_AResourceTagConditionMatchesAQueueCreatedThroughTheAPI(t *testing.T) {
	ts, state := newTaggingTestServer(t)

	queueURL := sqsCreateQueue(t, ts, "guarded-queue")
	sqsQueryRequest(t, ts, "TagQueue", map[string]string{
		"QueueUrl":    queueURL,
		"Tag.1.Key":   "Confidential",
		"Tag.1.Value": "yes",
	})

	seedSQSGuardUser(t, state, "nadia")
	auth := emulator.NewAuthController(state, emulator.NewDefaultLogger(slog.LevelError, false))
	reqCtx := newAuthTestReqCtx("arn:aws:iam::" + taggingTestAccountID + ":user/nadia")

	sendTo := func(qurl string) error {
		return auth.CheckAccess(reqCtx, &emulator.AWSRequest{
			Service:   "sqs",
			Operation: "SendMessage",
			Path:      "/",
			Params:    map[string]string{"QueueUrl": qurl},
		})
	}

	require.Error(t, sendTo(queueURL),
		"the Deny must match a queue tagged through SQS's own TagQueue")

	// The control: the same policy against an untagged queue is allowed, so the refusal above is
	// the condition matching rather than the Deny matching everything.
	assert.NoError(t, sendTo(sqsCreateQueue(t, ts, "open-queue")))
}

// seedSQSGuardUser attaches an Allow-everything plus a Deny-on-Confidential policy to userName, so
// the Deny's condition is the only thing that can refuse a request.
func seedSQSGuardUser(t *testing.T, state emulator.StateManager, userName string) {
	t.Helper()
	ctx := context.Background()
	policyARN := "arn:aws:iam::" + taggingTestAccountID + ":policy/SQSGuard"

	user := emulator.IAMUser{
		UserName: userName,
		UserID:   "AIDASQSGUARD",
		ARN:      "arn:aws:iam::" + taggingTestAccountID + ":user/" + userName,
		Path:     "/",
	}
	userRaw, err := json.Marshal(user)
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam",
		emulator.IAMUserKeyForTest(taggingTestAccountID, userName), userRaw))

	arnsRaw, err := json.Marshal([]string{policyARN})
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam",
		emulator.IAMAttachedPoliciesKeyForTest(taggingTestAccountID, "user", userName), arnsRaw))

	pol := emulator.IAMPolicy{
		PolicyName:       "SQSGuard",
		PolicyID:         "ANPASQSGUARD",
		ARN:              policyARN,
		Path:             "/",
		DefaultVersionID: "v1",
		IsAttachable:     true,
		Document: emulator.PolicyDocument{
			Version: "2012-10-17",
			Statement: []emulator.PolicyStatement{
				{
					Effect:   "Allow",
					Action:   emulator.StringOrSlice{"sqs:*"},
					Resource: emulator.StringOrSlice{"*"},
				},
				{
					Effect:   "Deny",
					Action:   emulator.StringOrSlice{"sqs:SendMessage"},
					Resource: emulator.StringOrSlice{"*"},
					Condition: map[string]map[string]emulator.StringOrSlice{
						"StringEquals": {"aws:ResourceTag/Confidential": {"yes"}},
					},
				},
			},
		},
	}
	polRaw, err := json.Marshal(pol)
	require.NoError(t, err)
	require.NoError(t, state.Put(ctx, "iam", emulator.IAMPolicyKeyForTest(policyARN), polRaw))
}
