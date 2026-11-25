package emulator_test

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The three pilots of the tagging provider interfaces (#1609): CloudWatch, EC2 and S3 answer the
// Resource Groups Tagging API for their own resources, and a write through it runs under the lock
// the service's own tag operation holds.

// providerEnv is a server holding the three pilot plugins and a tagging plugin.
type providerEnv struct {
	t        *testing.T
	srv      http.Handler
	registry *emulator.PluginRegistry
	tagging  *emulator.TaggingPlugin
	state    emulator.StateManager
}

// newProviderEnv builds a providerEnv. With reach false the tagging plugin is given no registry,
// so it reads and writes every service's state itself.
func newProviderEnv(t *testing.T, reach bool) *providerEnv {
	t.Helper()
	cfg := emulator.DefaultConfig()
	registry := emulator.NewPluginRegistry()
	state := emulator.NewMemoryStateManager()
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	store := emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig())
	tc := emulator.NewTimeController(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	opts := map[string]any{"time_controller": tc, "registry": registry}
	for _, p := range []emulator.Plugin{&emulator.EC2Plugin{}, &emulator.S3Plugin{}, &emulator.CloudWatchPlugin{}} {
		require.NoError(t, p.Initialize(context.Background(), emulator.PluginConfig{State: state, Logger: logger, Options: opts}))
		registry.Register(p)
	}
	taggingOpts := map[string]any{"time_controller": tc}
	if reach {
		taggingOpts["registry"] = registry
	}
	tagging := &emulator.TaggingPlugin{}
	require.NoError(t, tagging.Initialize(context.Background(), emulator.PluginConfig{State: state, Logger: logger, Options: taggingOpts}))
	registry.Register(tagging)
	return &providerEnv{
		t: t, srv: emulator.NewServer(*cfg, registry, store, state, tc, logger),
		registry: registry, tagging: tagging, state: state,
	}
}

// do sends one request on host and returns its status and body.
func (e *providerEnv) do(method, host, path, contentType, body string, headers map[string]string) (int, string) {
	e.t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Host = host
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	e.srv.ServeHTTP(w, r)
	return w.Code, w.Body.String()
}

// rgta sends one tagging API operation, which must answer 200.
func (e *providerEnv) rgta(op string, body any) string {
	e.t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(e.t, err)
	status, out := e.do(http.MethodPost, "tagging.us-east-1.amazonaws.com", "/", "application/x-amz-json-1.1",
		string(raw), map[string]string{"X-Amz-Target": "ResourceGroupsTaggingAPI_20170126." + op})
	require.Equal(e.t, http.StatusOK, status, "%s: %s", op, out)
	return out
}

// ec2 sends one EC2 query operation, which must answer 200.
func (e *providerEnv) ec2(params url.Values) string {
	e.t.Helper()
	params.Set("Version", "2016-11-15")
	status, out := e.do(http.MethodPost, "ec2.us-east-1.amazonaws.com", "/",
		"application/x-www-form-urlencoded", params.Encode(), nil)
	require.Equal(e.t, http.StatusOK, status, "%s: %s", params.Get("Action"), out)
	return out
}

// cw sends one CloudWatch operation over awsJson1_0, which must answer 200.
func (e *providerEnv) cw(op string, body any) {
	e.t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(e.t, err)
	status, out := e.do(http.MethodPost, "monitoring.us-east-1.amazonaws.com", "/", "application/x-amz-json-1.0",
		string(raw), map[string]string{"X-Amz-Target": "GraniteServiceVersion20100801." + op})
	require.Equal(e.t, http.StatusOK, status, "%s: %s", op, out)
}

// tagsOf returns the tags GetResources reports for arn, failing if it is not reported.
func (e *providerEnv) tagsOf(arn string) map[string]string {
	e.t.Helper()
	var out struct {
		ResourceTagMappingList []struct {
			ResourceARN string
			Tags        []struct{ Key, Value string }
		}
	}
	require.NoError(e.t, json.Unmarshal([]byte(e.rgta("GetResources", map[string]any{"ResourceARNList": []string{arn}})), &out))
	require.Len(e.t, out.ResourceTagMappingList, 1, "GetResources reports %s", arn)
	tags := map[string]string{}
	for _, tag := range out.ResourceTagMappingList[0].Tags {
		tags[tag.Key] = tag.Value
	}
	return tags
}

// createSecurityGroup creates a security group in the default VPC and returns its ARN and ID.
func (e *providerEnv) createSecurityGroup(name string) (arn, id string) {
	e.t.Helper()
	out := e.ec2(url.Values{"Action": {"CreateSecurityGroup"}, "GroupName": {name}, "GroupDescription": {name}})
	var resp struct {
		GroupID string `xml:"groupId"`
	}
	require.NoError(e.t, xml.Unmarshal([]byte(out), &resp))
	require.NotEmpty(e.t, resp.GroupID)
	return "arn:aws:ec2:us-east-1:123456789012:security-group/" + resp.GroupID, resp.GroupID
}

// createBucket creates bucket and returns its ARN.
func (e *providerEnv) createBucket(bucket string) string {
	e.t.Helper()
	status, out := e.do(http.MethodPut, "s3.us-east-1.amazonaws.com", "/"+bucket, "", "", nil)
	require.Equal(e.t, http.StatusOK, status, "CreateBucket: %s", out)
	return "arn:aws:s3:::" + bucket
}

// createAlarm creates an alarm and returns its ARN.
func (e *providerEnv) createAlarm(name string) string {
	e.t.Helper()
	e.cw("PutMetricAlarm", basicAlarm(name, nil))
	return "arn:aws:cloudwatch:us-east-1:123456789012:alarm:" + name
}

// providerFailure returns the FailedResourcesMap entry a TagResources or UntagResources body holds for arn.
func providerFailure(t *testing.T, body, arn string) (code string, status int) {
	t.Helper()
	var out struct {
		FailedResourcesMap map[string]struct {
			ErrorCode  string
			StatusCode int
		}
	}
	require.NoError(t, json.Unmarshal([]byte(body), &out))
	f, ok := out.FailedResourcesMap[arn]
	require.True(t, ok, "%s is reported failed: %s", arn, body)
	return f.ErrorCode, f.StatusCode
}

func TestTaggingProviders_ThePilotsImplementBothInterfaces(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	listers, taggers := emulator.TaggingProvidersForTest(ts.Registry())
	pilots := []string{"ec2", "eventbridge", "monitoring", "s3"}
	assert.Equal(t, pilots, listers)
	// IAM writes its own records' tags, under the lock a user rename holds, and leaves listing to
	// the tagging API's direct read.
	assert.Equal(t, []string{"ec2", "eventbridge", "iam", "monitoring", "s3"}, taggers)
}

func TestTaggingProviders_EachPilotAnswersItsScanOnceAndNoDirectReadRuns(t *testing.T) {
	t.Parallel()
	for _, reach := range []bool{true, false} {
		t.Run(fmt.Sprintf("registry=%v", reach), func(t *testing.T) {
			t.Parallel()
			e := newProviderEnv(t, reach)
			steps := map[string][]string{}
			for _, step := range emulator.TaggingScanPlanForTest(e.tagging) {
				owner, how, _ := strings.Cut(step, ":")
				steps[owner] = append(steps[owner], how)
			}
			for _, pilot := range []string{"ec2", "monitoring", "s3"} {
				if reach {
					assert.Equal(t, []string{"lister"}, steps[pilot], "%s is asked once and not read directly", pilot)
				} else {
					assert.Equal(t, []string{"direct"}, steps[pilot], "%s is read directly without a registry", pilot)
				}
			}
			assert.Equal(t, []string{"direct"}, steps["lambda"], "a service with no lister is read directly")
		})
	}
}

func TestTaggingProviders_EachPilotWritesItsOwnTags(t *testing.T) {
	t.Parallel()
	e := newProviderEnv(t, true)
	direct := newProviderEnv(t, false)
	for _, tc := range []struct{ arn, owner string }{
		{"arn:aws:ec2:us-east-1:123456789012:instance/i-1", "ec2"},
		{"arn:aws:s3:::bucket", "s3"},
		{"arn:aws:cloudwatch:us-east-1:123456789012:alarm:a", "monitoring"},
		{"arn:aws:lambda:us-east-1:123456789012:function:f", ""},
		{"not-an-arn", ""},
	} {
		assert.Equal(t, tc.owner, emulator.TaggingWriterForTest(e.tagging, tc.arn), tc.arn)
		assert.Empty(t, emulator.TaggingWriterForTest(direct.tagging, tc.arn), "%s without a registry", tc.arn)
	}
}

// providerParityScript runs one fixed sequence of tagging API requests and returns every response
// body, with the environment's minted security-group ID replaced so two environments compare.
func providerParityScript(e *providerEnv) []string {
	e.t.Helper()
	sgARN, sgID := e.createSecurityGroup("web")
	bucketARN := e.createBucket("orders")
	alarmARN := e.createAlarm("cpu")
	e.createAlarm("never-tagged")
	arns := []string{sgARN, bucketARN, alarmARN}
	var bodies []string
	call := func(op string, body any) string {
		out := e.rgta(op, body)
		bodies = append(bodies, op+" "+strings.ReplaceAll(out, sgID, "sg-ID"))
		return out
	}

	call("TagResources", map[string]any{"ResourceARNList": arns, "Tags": map[string]string{"team": "a", "env": "dev"}})
	call("UntagResources", map[string]any{"ResourceARNList": arns, "TagKeys": []string{"env"}})
	// Every page, one resource at a time, so the order and the tokens are compared too.
	token := ""
	for range 10 {
		var page struct{ PaginationToken string }
		body := map[string]any{"ResourcesPerPage": 1}
		if token != "" {
			body["PaginationToken"] = token
		}
		require.NoError(e.t, json.Unmarshal([]byte(call("GetResources", body)), &page))
		if token = page.PaginationToken; token == "" {
			break
		}
	}
	for _, filter := range []string{"ec2", "s3", "cloudwatch:alarm"} {
		call("GetResources", map[string]any{"ResourceTypeFilters": []string{filter}})
	}
	// Absent resources of each pilot, then types the pilot cannot tag.
	call("TagResources", map[string]any{"ResourceARNList": []string{
		"arn:aws:ec2:us-east-1:123456789012:security-group/sg-0000000000000000",
		"arn:aws:s3:::no-such-bucket",
		"arn:aws:cloudwatch:us-east-1:123456789012:alarm:none",
	}, "Tags": map[string]string{"k": "v"}})
	call("TagResources", map[string]any{"ResourceARNList": []string{
		"arn:aws:ec2:us-east-1:123456789012:vpc-endpoint/vpce-1",
		"arn:aws:s3:::orders/key",
		"arn:aws:cloudwatch:us-east-1:123456789012:dashboard/d",
	}, "Tags": map[string]string{"k": "v"}})
	call("UntagResources", map[string]any{"ResourceARNList": []string{alarmARN}, "TagKeys": []string{}})

	// Key rules: a reserved key added or removed, an uppercase AWS: key (an ordinary tag), a key
	// too long, and a reserved key on a missing resource (the key rule answers first).
	call("TagResources", map[string]any{"ResourceARNList": arns, "Tags": map[string]string{"aws:owner": "me"}})
	call("UntagResources", map[string]any{"ResourceARNList": arns, "TagKeys": []string{"aws:cloudformation:stack-name"}})
	call("TagResources", map[string]any{"ResourceARNList": arns, "Tags": map[string]string{"AWS:owner": "me"}})
	call("TagResources", map[string]any{"ResourceARNList": arns, "Tags": map[string]string{strings.Repeat("k", 129): "v"}})
	call("TagResources", map[string]any{"ResourceARNList": []string{
		"arn:aws:ec2:us-east-1:123456789012:security-group/sg-0000000000000000",
		"arn:aws:s3:::no-such-bucket",
		"arn:aws:cloudwatch:us-east-1:123456789012:alarm:none",
	}, "Tags": map[string]string{"aws:owner": "me"}})

	// Quotas: 48 more fit beside the two already there, and the 51st is refused.
	full := map[string]string{}
	for i := range 48 {
		full[fmt.Sprintf("q%02d", i)] = "v"
	}
	call("TagResources", map[string]any{"ResourceARNList": arns, "Tags": full})
	call("TagResources", map[string]any{"ResourceARNList": arns, "Tags": map[string]string{"one-more": "v"}})
	call("GetResources", map[string]any{"ResourceARNList": arns})
	return bodies
}

// TestTaggingProviders_AnswersMatchTheDirectPath runs the same requests against a tagging plugin
// that reaches the pilots and one that does not, and requires byte-identical answers from both:
// every FailedResourcesMap, every GetResources page and its token.
func TestTaggingProviders_AnswersMatchTheDirectPath(t *testing.T) {
	t.Parallel()
	viaOwners := providerParityScript(newProviderEnv(t, true))
	direct := providerParityScript(newProviderEnv(t, false))
	assert.Equal(t, direct, viaOwners)

	// And the answers are the ones the tagging API publishes.
	joined := strings.Join(viaOwners, "\n")
	assert.Contains(t, joined, `"team"`, "the merge and the removal landed")
	assert.NotContains(t, joined, `"env"`)
	assert.NotContains(t, joined, "never-tagged", "a never-tagged alarm is not reported")
	assert.Contains(t, joined, `"ErrorCode":"InvalidParameterException","ErrorMessage":"the target ID is invalid, unsupported, or doesn't exist","StatusCode":400`)
	assert.Contains(t, joined, `"ErrorCode":"InternalServiceException","ErrorMessage":"the resource type in the request is not supported by the Resource Groups Tagging API","StatusCode":500`)
	assert.Contains(t, joined, `"ErrorCode":"MissingParameter"`)
	assert.Contains(t, joined, `"ErrorCode":"InvalidTag"`, "S3's own refusals, on both paths")
	assert.Contains(t, joined, `"ErrorCode":"TagLimitExceeded"`, "EC2's own 50-tag refusal, on both paths")
	assert.Contains(t, joined, `"AWS:owner"`, "an uppercase AWS: key is an ordinary tag")
	assert.NotContains(t, joined, `"aws:owner"`, "no reserved key was written")
	assert.NotContains(t, joined, `"one-more"`, "the 51st tag was refused everywhere")
}

// TestTaggingProviders_ReservedKeysAreRefused pins that a caller can neither write nor strip an
// `aws:` key through the tagging API, while CloudFormation's internal stamp path still writes one.
// EC2 and S3 answer with their own service's refusal whether or not their plugin is registered,
// and CloudWatch with the tagging API's.
func TestTaggingProviders_ReservedKeysAreRefused(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		reach           bool
		ec2, s3, alarms string
	}{
		{true, "InvalidParameterValue", "InvalidTag", "InvalidParameterException"},
		{false, "InvalidParameterValue", "InvalidTag", "InvalidParameterException"},
	} {
		t.Run(fmt.Sprintf("registry=%v", tc.reach), func(t *testing.T) {
			t.Parallel()
			e := newProviderEnv(t, tc.reach)
			sgARN, _ := e.createSecurityGroup("web")
			bucketARN := e.createBucket("orders")
			alarmARN := e.createAlarm("cpu")
			arns := []string{sgARN, bucketARN, alarmARN}
			want := map[string]string{sgARN: tc.ec2, bucketARN: tc.s3, alarmARN: tc.alarms}

			stamp := map[string]string{"aws:cloudformation:stack-name": "s", "team": "a"}
			out := e.rgta("TagResources", map[string]any{"ResourceARNList": arns, "Tags": stamp})
			for _, arn := range arns {
				code, status := providerFailure(t, out, arn)
				assert.Equal(t, want[arn], code, arn)
				assert.Equal(t, http.StatusBadRequest, status, arn)
			}

			// The internal path writes the stamp, and the caller cannot strip it.
			require.NoError(t, emulator.MergeResourceTagsForTest(e.state, "s3", "bucket:orders",
				map[string]string{"aws:cloudformation:stack-name": "s"}, nil, false))
			out = e.rgta("UntagResources", map[string]any{"ResourceARNList": []string{bucketARN},
				"TagKeys": []string{"aws:cloudformation:stack-name"}})
			code, _ := providerFailure(t, out, bucketARN)
			assert.Equal(t, tc.s3, code)
			assert.Equal(t, map[string]string{"aws:cloudformation:stack-name": "s"}, e.tagsOf(bucketARN))
		})
	}
}

// TestTaggingProviders_OwnerKeyRulesApply pins that what a pilot is asked to add is held to its own
// service's key and value rules, not only to the reserved prefix.
func TestTaggingProviders_OwnerKeyRulesApply(t *testing.T) {
	t.Parallel()
	e := newProviderEnv(t, true)
	sgARN, _ := e.createSecurityGroup("web")
	bucketARN := e.createBucket("orders")
	long := strings.Repeat("k", 129)
	out := e.rgta("TagResources", map[string]any{"ResourceARNList": []string{sgARN, bucketARN},
		"Tags": map[string]string{long: "v"}})
	code, _ := providerFailure(t, out, sgARN)
	assert.Equal(t, "InvalidParameterValue", code, "CreateTags' key length rule")
	code, _ = providerFailure(t, out, bucketARN)
	assert.Equal(t, "InvalidTag", code, "PutBucketTagging's key length rule")
	out = e.rgta("TagResources", map[string]any{"ResourceARNList": []string{bucketARN},
		"Tags": map[string]string{"bad#key": "v"}})
	code, _ = providerFailure(t, out, bucketARN)
	assert.Equal(t, "InvalidTag", code, "PutBucketTagging's character rule")
}

// TestTaggingProviders_AlarmMergesAddAndRemoveTogether drives the alarm writer with both halves in
// one call, the shape only an internal caller can send.
func TestTaggingProviders_AlarmMergesAddAndRemoveTogether(t *testing.T) {
	t.Parallel()
	e := newProviderEnv(t, false)
	arn := e.createAlarm("both")
	e.rgta("TagResources", map[string]any{"ResourceARNList": []string{arn}, "Tags": map[string]string{"a": "1", "b": "2"}})
	require.NoError(t, emulator.MergeResourceTagsForTest(e.state, "monitoring", "alarm:123456789012/us-east-1/both",
		map[string]string{"c": "3"}, []string{"a"}, true))
	assert.Equal(t, map[string]string{"b": "2", "c": "3"}, e.tagsOf(arn))

	many := map[string]string{}
	for i := range 49 {
		many[fmt.Sprintf("k%02d", i)] = "v"
	}
	err := emulator.MergeResourceTagsForTest(e.state, "monitoring", "alarm:123456789012/us-east-1/both", many, []string{"b"}, true)
	assert.NoError(t, err, "the removal is counted before the quota: 1 + 49 tags fit")
	err = emulator.MergeResourceTagsForTest(e.state, "monitoring", "alarm:123456789012/us-east-1/both",
		map[string]string{"x": "1"}, nil, true)
	assert.Error(t, err, "a 51st tag is refused")
}

func TestTaggingProviders_QuotasAreTheOwnersAnswer(t *testing.T) {
	t.Parallel()
	e := newProviderEnv(t, true)
	sgARN, _ := e.createSecurityGroup("full")
	alarmARN := e.createAlarm("full")
	bucketARN := e.createBucket("full")
	seed := map[string]string{"seed": "x"}
	e.rgta("TagResources", map[string]any{"ResourceARNList": []string{sgARN, alarmARN, bucketARN}, "Tags": seed})
	many := map[string]string{}
	for i := range 51 {
		many[fmt.Sprintf("k%02d", i)] = "v"
	}
	out := e.rgta("TagResources", map[string]any{"ResourceARNList": []string{sgARN, alarmARN, bucketARN}, "Tags": many})
	code, _ := providerFailure(t, out, sgARN)
	assert.Equal(t, "TagLimitExceeded", code, "EC2's own code for its 50-tag quota")
	code, _ = providerFailure(t, out, alarmARN)
	assert.Equal(t, "InvalidParameterValue", code, "CloudWatch's own code for its 50-tag quota")
	assert.Equal(t, seed, e.tagsOf(sgARN), "a refused merge writes nothing")
	assert.Equal(t, seed, e.tagsOf(alarmARN), "a refused merge writes nothing")
	code, _ = providerFailure(t, out, bucketARN)
	assert.Equal(t, "InvalidTag", code, "S3's own code for its 50-tag bucket limit")
	assert.Equal(t, seed, e.tagsOf(bucketARN), "a refused merge writes nothing")

	// 50 user tags fit, counting the one already there; the 51st is refused by the merged count.
	fifty := map[string]string{}
	for i := range 49 {
		fifty[fmt.Sprintf("k%02d", i)] = "v"
	}
	assert.NotContains(t, e.rgta("TagResources", map[string]any{"ResourceARNList": []string{bucketARN}, "Tags": fifty}), "FailedResourcesMap")
	out = e.rgta("TagResources", map[string]any{"ResourceARNList": []string{bucketARN}, "Tags": map[string]string{"one-more": "v"}})
	code, _ = providerFailure(t, out, bucketARN)
	assert.Equal(t, "InvalidTag", code, "the merged set is counted")

	// An alarm named in a write that adds and removes nothing answers what CloudWatch's
	// UntagResource answers for an absent TagKeys.
	out = e.rgta("UntagResources", map[string]any{"ResourceARNList": []string{alarmARN}, "TagKeys": []string{}})
	code, _ = providerFailure(t, out, alarmARN)
	assert.Equal(t, "MissingParameter", code)
}

// The race tests below write one resource's tags concurrently through the tagging API and through
// the owning service's own tag operation. Both hold the owner's lock, so no write is lost.

const providerRaceWriters = 10

func TestTaggingProviders_EC2TagWritesDoNotRace(t *testing.T) {
	t.Parallel()
	e := newProviderEnv(t, true)
	arn, id := e.createSecurityGroup("race")
	var wg sync.WaitGroup
	for i := range providerRaceWriters {
		wg.Go(func() {
			e.rgta("TagResources", map[string]any{"ResourceARNList": []string{arn}, "Tags": map[string]string{fmt.Sprintf("t%d", i): "v"}})
		})
		wg.Go(func() {
			e.ec2(url.Values{"Action": {"CreateTags"}, "ResourceId.1": {id},
				"Tag.1.Key": {fmt.Sprintf("c%d", i)}, "Tag.1.Value": {"v"}})
		})
	}
	wg.Wait()
	tags := e.tagsOf(arn)
	for i := range providerRaceWriters {
		assert.Contains(t, tags, fmt.Sprintf("t%d", i), "a TagResources write survives")
		assert.Contains(t, tags, fmt.Sprintf("c%d", i), "a CreateTags write survives")
	}
}

func TestTaggingProviders_CloudWatchTagWritesDoNotRace(t *testing.T) {
	t.Parallel()
	e := newProviderEnv(t, true)
	arn := e.createAlarm("race")
	var wg sync.WaitGroup
	for i := range providerRaceWriters {
		wg.Go(func() {
			e.rgta("TagResources", map[string]any{"ResourceARNList": []string{arn}, "Tags": map[string]string{fmt.Sprintf("t%d", i): "v"}})
		})
		wg.Go(func() {
			e.cw("TagResource", map[string]any{"ResourceARN": arn,
				"Tags": []map[string]string{{"Key": fmt.Sprintf("c%d", i), "Value": "v"}}})
		})
	}
	wg.Wait()
	tags := e.tagsOf(arn)
	for i := range providerRaceWriters {
		assert.Contains(t, tags, fmt.Sprintf("t%d", i), "a TagResources write survives")
		assert.Contains(t, tags, fmt.Sprintf("c%d", i), "a CloudWatch TagResource write survives")
	}
}

// TestTaggingProviders_S3TagWritesDoNotRace writes a bucket's tags concurrently in two rounds.
//
// The first round is TagResources merges alone, each adding its own key, so every key survives.
// The second interleaves merges with PutBucketTagging, which replaces the whole set with
// {owner: s3}. Serialized, the last write is either that replacement or a merge onto a set that
// already holds owner, so owner is there at the end and every other key is a merge's.
func TestTaggingProviders_S3TagWritesDoNotRace(t *testing.T) {
	t.Parallel()
	e := newProviderEnv(t, true)
	arn := e.createBucket("race")
	merge := func(key string) {
		e.rgta("TagResources", map[string]any{"ResourceARNList": []string{arn}, "Tags": map[string]string{key: "v"}})
	}

	var wg sync.WaitGroup
	for i := range providerRaceWriters {
		wg.Go(func() { merge(fmt.Sprintf("m%d", i)) })
	}
	wg.Wait()
	tags := e.tagsOf(arn)
	for i := range providerRaceWriters {
		assert.Contains(t, tags, fmt.Sprintf("m%d", i), "a concurrent TagResources write survives")
	}

	const tagging = `<Tagging><TagSet><Tag><Key>owner</Key><Value>s3</Value></Tag></TagSet></Tagging>`
	for i := range providerRaceWriters {
		wg.Go(func() { merge(fmt.Sprintf("t%d", i)) })
		wg.Go(func() {
			status, out := e.do(http.MethodPut, "s3.us-east-1.amazonaws.com", "/race?tagging", "application/xml", tagging, nil)
			assert.Equal(t, http.StatusNoContent, status, "PutBucketTagging: %s", out)
		})
	}
	wg.Wait()
	tags = e.tagsOf(arn)
	assert.Equal(t, "s3", tags["owner"], "no merge overwrote a replacement it had read before")
	for k, v := range tags {
		if k != "owner" {
			assert.True(t, strings.HasPrefix(k, "t"), "unexpected tag %s", k)
			assert.Equal(t, "v", v)
		}
	}
}

// TestTaggingProviders_CFNStackTagsDoNotRaceCreateTags propagates a stack's changing tags onto a
// security group while CreateTags writes other keys on it. Both go through EC2's tag lock, so every
// CreateTags key survives and the group ends with the last stack tag.
func TestTaggingProviders_CFNStackTagsDoNotRaceCreateTags(t *testing.T) {
	t.Parallel()
	f := newCFNStampFixture(t, nil)
	const template = `{"Resources": {
		"Vpc": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.9.0.0/16"}},
		"Sg":  {"Type": "AWS::EC2::SecurityGroup", "Properties": {
			"GroupDescription": "raced", "VpcId": {"Ref": "Vpc"}}}}}`
	created, err := f.deployer.DeployWithOptions(context.Background(), template, "raced", nil,
		emulator.CFNDeployOptions{Tags: map[string]string{"round": "0"}})
	require.NoError(t, err)
	sg := physicalIDs(t, created)["Sg"]

	var wg sync.WaitGroup
	wg.Go(func() {
		for i := 1; i <= providerRaceWriters; i++ {
			_, err := f.deployer.UpdateStack(context.Background(), template, "raced", nil,
				emulator.CFNDeployOptions{Tags: map[string]string{"round": strconv.Itoa(i)}})
			assert.NoError(t, err)
		}
	})
	for i := range providerRaceWriters {
		wg.Go(func() {
			_, err := f.ec2.HandleRequest(&emulator.RequestContext{
				AccountID: cfnStampAccount, Region: cfnStampRegion, Metadata: map[string]interface{}{},
			}, &emulator.AWSRequest{Service: "ec2", Operation: "CreateTags", Params: map[string]string{
				"Action": "CreateTags", "ResourceId.1": sg,
				"Tag.1.Key": fmt.Sprintf("c%d", i), "Tag.1.Value": "v",
			}})
			assert.NoError(t, err)
		})
	}
	wg.Wait()
	tags := f.tagsFor(t, sg)
	for i := range providerRaceWriters {
		assert.Contains(t, tags, fmt.Sprintf("c%d=v", i), "a CreateTags write survives the stack's propagation")
	}
	assert.Contains(t, tags, "round="+strconv.Itoa(providerRaceWriters), "the last stack tag is on the group")
	assert.Contains(t, tags, "aws:cloudformation:logical-id=Sg", "the stamp is still there")
}
