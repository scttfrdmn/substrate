package emulator_test

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The Resource Groups Tagging API bypassed every per-resource tag quota — #1000.
//
// Four services enforce a published per-resource tag quota on their own tagging operations, and all
// four are reachable through the tagging API's resolver, whose merge consulted none of them. So
// `TagResources` was the one way in substrate to put a resource over its own service's quota, after
// which that service's own operation refused every further add — a resource in a state substrate's own
// reference says cannot exist, reached through substrate's own API.
//
// Every case here writes through `TagResources` and reads back through **the owning service's own
// call**, which is #765's rule and the only assertion that distinguishes a quota honored from a
// refusal that answered a code and wrote the tags anyway. Nothing writes state directly, for the
// reason `tagging_arn_guards_test.go` gives.
//
// The four codes are deliberately not unified. IAM answers `LimitExceeded` at **409** where the other
// three answer 400, and all four codes are different strings, so the table below is the record of what
// each service publishes rather than a single shared refusal — which is also why each arm calls that
// service's own checker instead of a shared count.

// tagQuotaLimit is the per-resource tag quota all four services publish.
//
// One constant because the four agree on the number, spelled here rather than read from the emulator
// so the test pins it: `API_TagResources` publishes "Each resource can have up to 50 tags" itself, and
// EC2, ELBv2, IAM and Kinesis each publish 50 on their own tagging pages.
const tagQuotaLimit = 50

// tagQuotaService is one service's row: how to create a taggable resource, how to read its tags back
// through the service's own call, and the refusal that service publishes.
type tagQuotaService struct {
	// create makes one taggable resource and returns the ARN the tagging API addresses it by.
	create func(t *testing.T, ts *emulator.TestServer) string

	// readTags reads the resource's tags back through the owning service's own operation.
	readTags func(t *testing.T, ts *emulator.TestServer, arn string) map[string]string

	// code and status are what the service's own quota refusal answers.
	code   string
	status int
}

// tagQuotaServices is the four services that publish a per-resource tag quota and have a tagging-API
// arm. A fifth — AWS Config, at 50 — enforces one on its own operations and has no arm, so no
// `TagResources` call can reach it.
//
//nolint:gochecknoglobals // one table shared by every test in this file
var tagQuotaServices = map[string]tagQuotaService{
	"ec2": {
		create:   tagQuotaCreateInstance,
		readTags: tagQuotaInstanceTags,
		code:     "TagLimitExceeded",
		status:   http.StatusBadRequest,
	},
	"elasticloadbalancing": {
		create: func(t *testing.T, ts *emulator.TestServer) string {
			t.Helper()
			return elbCreateLB(t, ts.URL, "quota-alb", nil)
		},
		readTags: func(t *testing.T, ts *emulator.TestServer, arn string) map[string]string {
			t.Helper()
			return elbDescribeTags(t, ts.URL, arn)[arn]
		},
		code:   "TooManyTags",
		status: http.StatusBadRequest,
	},
	"iam": {
		create:   tagQuotaCreateRole,
		readTags: tagQuotaRoleTags,
		code:     "LimitExceeded",
		status:   http.StatusConflict,
	},
	"kinesis": {
		create:   tagQuotaCreateStream,
		readTags: tagQuotaStreamTags,
		code:     "LimitExceededException",
		status:   http.StatusBadRequest,
	},
}

// tagQuotaServer starts a server carrying every default plugin, so all five services share one state
// store. [emulator.WithAccounts] because [signedRequest] needs a registered credential to sign the
// tagging call with.
func tagQuotaServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	return emulator.StartTestServer(t, emulator.WithAccounts(taggingTestAccount))
}

// tagQuotaTags builds n distinct tags, keyed so the order they are generated in cannot matter.
func tagQuotaTags(prefix string, n int) map[string]string {
	tags := make(map[string]string, n)
	for i := 0; i < n; i++ {
		tags[fmt.Sprintf("%s-%02d", prefix, i)] = "v"
	}
	return tags
}

// tagQuotaTag posts TagResources for one ARN and returns the FailedResourcesMap, without asserting it
// is empty — which is the point of every test here.
func tagQuotaTag(t *testing.T, ts *emulator.TestServer, arn string, tags map[string]string) map[string]taggingFailure {
	t.Helper()
	return tagQuotaCall(t, ts, "TagResources", map[string]any{
		"ResourceARNList": []string{arn}, "Tags": tags,
	})
}

// tagQuotaUntag posts UntagResources for one ARN and returns the FailedResourcesMap.
func tagQuotaUntag(t *testing.T, ts *emulator.TestServer, arn string, keys []string) map[string]taggingFailure {
	t.Helper()
	return tagQuotaCall(t, ts, "UntagResources", map[string]any{
		"ResourceARNList": []string{arn}, "TagKeys": keys,
	})
}

// tagQuotaCall posts one tagging-API operation and returns its FailedResourcesMap, asserting that the
// request itself answered 200 — a per-resource failure is reported in the map, never as a top-level
// error, which is the shape `TagResources` publishes.
func tagQuotaCall(t *testing.T, ts *emulator.TestServer, op string, body map[string]any) map[string]taggingFailure {
	t.Helper()
	var out struct {
		FailedResourcesMap map[string]taggingFailure `json:"FailedResourcesMap"`
	}
	status, errCode := decodeAWSResponse(t,
		signedRequest(t, ts, taggingTarget, taggingTestAccount, op, body), &out)
	require.Empty(t, errCode, "%s answers a FailedResourcesMap, not a top-level error", op)
	require.Equal(t, http.StatusOK, status, "%s answers 200 whatever the per-resource outcome", op)
	return out.FailedResourcesMap
}

// tagQuotaQueryRequest posts a Query-protocol request to one service's endpoint and returns the body.
//
// A local helper because the per-service ones in the tree take a *httptest.Server or an
// *emulator.Server, and a cross-API assertion needs the [emulator.TestServer] both halves share.
func tagQuotaQueryRequest(t *testing.T, ts *emulator.TestServer, host string, params map[string]string) string {
	t.Helper()
	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(form.Encode()))
	require.NoError(t, err)
	req.Host = host
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equalf(t, http.StatusOK, resp.StatusCode, "%s %s: %s", host, params["Action"], raw)
	return string(raw)
}

// tagQuotaCreateInstance runs one instance and returns its ARN.
func tagQuotaCreateInstance(t *testing.T, ts *emulator.TestServer) string {
	t.Helper()
	body := tagQuotaQueryRequest(t, ts, "ec2.us-east-1.amazonaws.com", map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage,
		"InstanceType": "t3.micro", "MinCount": "1", "MaxCount": "1",
	})
	var out struct {
		Instances []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"instancesSet>item"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &out))
	require.Len(t, out.Instances, 1)
	return "arn:aws:ec2:us-east-1:" + taggingTestAccount + ":instance/" + out.Instances[0].InstanceID
}

// tagQuotaInstanceTags reads an instance's tags back through EC2's own DescribeTags.
func tagQuotaInstanceTags(t *testing.T, ts *emulator.TestServer, arn string) map[string]string {
	t.Helper()
	id := arn[strings.LastIndex(arn, "/")+1:]
	body := tagQuotaQueryRequest(t, ts, "ec2.us-east-1.amazonaws.com", map[string]string{
		"Action": "DescribeTags", "Filter.1.Name": "resource-id", "Filter.1.Value.1": id,
	})
	var out struct {
		Tags []struct {
			Key   string `xml:"key"`
			Value string `xml:"value"`
		} `xml:"tagSet>item"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &out))
	tags := make(map[string]string, len(out.Tags))
	for _, tag := range out.Tags {
		tags[tag.Key] = tag.Value
	}
	return tags
}

// tagQuotaCreateRole creates a role and returns the ARN IAM itself minted.
func tagQuotaCreateRole(t *testing.T, ts *emulator.TestServer) string {
	t.Helper()
	var out struct {
		Role struct {
			Arn string `xml:"Arn"`
		} `xml:"CreateRoleResult>Role"`
	}
	require.NoError(t, xml.Unmarshal([]byte(tagQuotaIAMRequest(t, ts, "CreateRole", map[string]any{
		"RoleName": "quota-role",
	})), &out))
	require.NotEmpty(t, out.Role.Arn, "CreateRole reports an ARN")
	return out.Role.Arn
}

// tagQuotaRoleTags reads a role's tags back through IAM's own ListRoleTags.
func tagQuotaRoleTags(t *testing.T, ts *emulator.TestServer, arn string) map[string]string {
	t.Helper()
	var out struct {
		Tags []struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		} `xml:"ListRoleTagsResult>Tags>member"`
	}
	name := arn[strings.LastIndex(arn, "/")+1:]
	require.NoError(t, xml.Unmarshal([]byte(tagQuotaIAMRequest(t, ts, "ListRoleTags", map[string]any{
		"RoleName": name,
	})), &out))
	tags := make(map[string]string, len(out.Tags))
	for _, tag := range out.Tags {
		tags[tag.Key] = tag.Value
	}
	return tags
}

// tagQuotaIAMRequest posts one IAM operation. IAM takes a JSON body under an X-Amz-Target and answers
// XML, which is why it needs its own helper rather than either of the two above.
func tagQuotaIAMRequest(t *testing.T, ts *emulator.TestServer, op string, body map[string]any) string {
	t.Helper()
	encoded, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", strings.NewReader(string(encoded)))
	require.NoError(t, err)
	req.Host = "iam.amazonaws.com"
	req.Header.Set("X-Amz-Target", "AmazonIdentityManagementService."+op)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equalf(t, http.StatusOK, resp.StatusCode, "IAM %s: %s", op, raw)
	return string(raw)
}

// tagQuotaCreateStream creates a stream and returns its ARN.
func tagQuotaCreateStream(t *testing.T, ts *emulator.TestServer) string {
	t.Helper()
	kinesisARNCreate(t, ts, taggingTestAccount, kinesisARNEastRegion, "quota-stream", 1)
	return kinesisStreamARNFor(taggingTestAccount, kinesisARNEastRegion, "quota-stream")
}

// tagQuotaStreamTags reads a stream's tags back through Kinesis's own ListTagsForStream.
func tagQuotaStreamTags(t *testing.T, ts *emulator.TestServer, _ string) map[string]string {
	t.Helper()
	raw := kinesisARNOK(t, ts, taggingTestAccount, kinesisARNEastRegion, "ListTagsForStream",
		map[string]any{"StreamName": "quota-stream"})
	var out struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	require.NoErrorf(t, json.Unmarshal([]byte(raw), &out), "decode ListTagsForStream: %s", raw)
	tags := make(map[string]string, len(out.Tags))
	for _, tag := range out.Tags {
		tags[tag.Key] = tag.Value
	}
	return tags
}

// TestTagQuota_TheFourServicesRefuseAnOverQuotaTagResources is the criterion the issue leads with.
//
// The fifty-first tag is refused with the owning service's own code and status, and the read-back
// through that service's own call still reports fifty — so the refusal is a refusal and not a code
// answered beside a completed write, which is the #965 rule.
func TestTagQuota_TheFourServicesRefuseAnOverQuotaTagResources(t *testing.T) {
	t.Parallel()

	for name, svc := range tagQuotaServices {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ts := tagQuotaServer(t)
			arn := svc.create(t, ts)

			require.Empty(t, tagQuotaTag(t, ts, arn, tagQuotaTags("k", tagQuotaLimit)),
				"%s accepts exactly the quota", name)
			require.Len(t, svc.readTags(t, ts, arn), tagQuotaLimit,
				"%s reports the quota's worth of tags through its own call", name)

			failures := tagQuotaTag(t, ts, arn, map[string]string{"one-too-many": "v"})
			require.Contains(t, failures, arn, "%s must refuse the fifty-first tag", name)
			assert.Equal(t, svc.code, failures[arn].ErrorCode,
				"%s reports its own published code, not one of FailureInfo's two", name)
			assert.Equal(t, svc.status, failures[arn].StatusCode,
				"%s reports its own published status", name)
			assert.NotEmpty(t, failures[arn].ErrorMessage, "%s names what went wrong", name)

			after := svc.readTags(t, ts, arn)
			assert.Len(t, after, tagQuotaLimit, "%s wrote nothing for the refused request", name)
			assert.NotContains(t, after, "one-too-many",
				"%s must not have written the tag it refused", name)
		})
	}
}

// TestTagQuota_ARewriteOfAnExistingKeyIsNotAnAddition is the direction a len(existing)+len(incoming)
// count gets wrong, and all four checkers were written to get right.
//
// A resource at its quota can still have an existing tag's value changed, because the count is over
// the post-merge key set. Reading the new value back is what separates "accepted" from "accepted and
// silently dropped".
func TestTagQuota_ARewriteOfAnExistingKeyIsNotAnAddition(t *testing.T) {
	t.Parallel()

	for name, svc := range tagQuotaServices {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ts := tagQuotaServer(t)
			arn := svc.create(t, ts)
			require.Empty(t, tagQuotaTag(t, ts, arn, tagQuotaTags("k", tagQuotaLimit)))

			failures := tagQuotaTag(t, ts, arn, map[string]string{"k-00": "rewritten"})
			require.Empty(t, failures, "%s accepts a rewrite of a key it already carries", name)

			after := svc.readTags(t, ts, arn)
			assert.Len(t, after, tagQuotaLimit, "%s is still at the quota, not over it", name)
			assert.Equal(t, "rewritten", after["k-00"], "%s stored the new value", name)
		})
	}
}

// TestTagQuota_UntagResourcesIsNeverRefusedAndFreesRoom pins the other half of the merge semantics.
//
// A removal only shrinks the key set, so it cannot exceed a quota and is never checked; and the slot
// it frees is usable, which is what makes a resource that reached the quota recoverable rather than
// permanently full.
func TestTagQuota_UntagResourcesIsNeverRefusedAndFreesRoom(t *testing.T) {
	t.Parallel()

	for name, svc := range tagQuotaServices {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ts := tagQuotaServer(t)
			arn := svc.create(t, ts)
			require.Empty(t, tagQuotaTag(t, ts, arn, tagQuotaTags("k", tagQuotaLimit)))

			require.Empty(t, tagQuotaUntag(t, ts, arn, []string{"k-00"}),
				"%s never refuses a removal", name)
			require.Len(t, svc.readTags(t, ts, arn), tagQuotaLimit-1)

			require.Empty(t, tagQuotaTag(t, ts, arn, map[string]string{"replacement": "v"}),
				"%s accepts an add into the slot the removal freed", name)
			assert.Contains(t, svc.readTags(t, ts, arn), "replacement")
		})
	}
}

// TestTagQuota_OneRefusalDoesNotStopTheOtherResources is the per-resource shape `TagResources`
// publishes, and the reason the refusal is a FailedResourcesMap entry rather than an error.
//
// Two ARNs of the same service in one request, one already at its quota and one empty: the request
// answers 200, names only the full one, and tags the other.
func TestTagQuota_OneRefusalDoesNotStopTheOtherResources(t *testing.T) {
	t.Parallel()
	ts := tagQuotaServer(t)

	full := tagQuotaCreateInstance(t, ts)
	room := tagQuotaCreateInstance(t, ts)
	require.Empty(t, tagQuotaTag(t, ts, full, tagQuotaTags("k", tagQuotaLimit)))

	failures := tagQuotaCall(t, ts, "TagResources", map[string]any{
		"ResourceARNList": []string{full, room},
		"Tags":            map[string]string{"shared": "v"},
	})
	require.Contains(t, failures, full, "the resource at its quota is named")
	assert.NotContains(t, failures, room, "the resource with room is not")
	assert.Equal(t, "TagLimitExceeded", failures[full].ErrorCode)

	assert.NotContains(t, tagQuotaInstanceTags(t, ts, full), "shared",
		"the refused resource got nothing")
	assert.Contains(t, tagQuotaInstanceTags(t, ts, room), "shared",
		"the other resource in the same request got its tag")
}

// TestTagQuota_AServiceWithNoPublishedQuotaIsUnaffected is the honesty half of the change: nineteen of
// the 23 namespace arms publish no quota substrate models, and the gate must not invent one for them.
//
// SQS is the case in point — `API_TagQueue` publishes a 50-tag limit in its own prose, but substrate
// models no SQS tag quota on `TagQueue` either, and inventing one on the tagging-API path alone would
// make the two APIs disagree in the opposite direction from the defect being fixed.
func TestTagQuota_AServiceWithNoPublishedQuotaIsUnaffected(t *testing.T) {
	t.Parallel()
	ts := tagQuotaServer(t)

	var created struct {
		QueueURL string `json:"QueueUrl"`
	}
	status, errCode := decodeAWSResponse(t, signedRequest(t, ts,
		signedRequestTarget{host: "sqs.us-east-1.amazonaws.com", target: "AmazonSQS", signingName: "sqs"},
		taggingTestAccount, "CreateQueue", map[string]any{"QueueName": "quota-queue"}), &created)
	require.Empty(t, errCode)
	require.Equal(t, http.StatusOK, status)

	arn := "arn:aws:sqs:us-east-1:" + taggingTestAccount + ":quota-queue"
	require.Empty(t, tagQuotaTag(t, ts, arn, tagQuotaTags("k", tagQuotaLimit+10)),
		"a namespace with no modeled quota accepts more than fifty")
}

// TestTagQuota_TheCloudFormationStampIsNotCounted pins reading 2 of the change: both deployer writers
// go through the same merge and neither enforces the quota.
//
// Asserted on a Kinesis stream because Kinesis is one of the two services that counts `aws:`-prefixed
// keys, so it is the case where the two modes give different answers. On EC2 and ELBv2 the stamp is
// free either way, which would make this test pass against a gate that had no mode at all.
func TestTagQuota_TheCloudFormationStampIsNotCounted(t *testing.T) {
	t.Parallel()
	ts := tagQuotaServer(t)
	arn := tagQuotaCreateStream(t, ts)
	require.Empty(t, tagQuotaTag(t, ts, arn, tagQuotaTags("k", tagQuotaLimit)))

	stamp := map[string]string{"aws:cloudformation:stack-name": "s"}
	key := "stream:" + taggingTestAccount + "/" + kinesisARNEastRegion + "/quota-stream"

	require.Error(t, emulator.MergeResourceTagsForTest(
		ts.StateManager(), "kinesis", key, stamp, nil, true),
		"the tagging API's mode refuses it, since Kinesis counts a reserved key")
	require.NoError(t, emulator.MergeResourceTagsForTest(
		ts.StateManager(), "kinesis", key, stamp, nil, false),
		"the deployer's mode writes it")

	assert.Contains(t, tagQuotaStreamTags(t, ts, arn), "aws:cloudformation:stack-name",
		"the stamp is readable through Kinesis's own call")
}
