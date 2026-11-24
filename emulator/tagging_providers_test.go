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
		registry: registry, tagging: tagging,
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
	pilots := []string{"ec2", "monitoring", "s3"}
	assert.Equal(t, pilots, listers)
	assert.Equal(t, pilots, taggers)
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

// TestTaggingProviders_AnswersMatchTheDirectPath runs the same requests against a tagging plugin
// that reaches the pilots and one that does not, and requires the same answers from both.
func TestTaggingProviders_AnswersMatchTheDirectPath(t *testing.T) {
	t.Parallel()
	for _, reach := range []bool{true, false} {
		t.Run(fmt.Sprintf("registry=%v", reach), func(t *testing.T) {
			t.Parallel()
			e := newProviderEnv(t, reach)
			sgARN, _ := e.createSecurityGroup("web")
			bucketARN := e.createBucket("orders")
			alarmARN := e.createAlarm("cpu")
			arns := []string{sgARN, bucketARN}
			if reach {
				// Without a registry the tagging plugin has never been able to write an alarm's
				// tags: CloudWatch's record is only ever written by CloudWatch.
				arns = append(arns, alarmARN)
			}

			e.rgta("TagResources", map[string]any{"ResourceARNList": arns, "Tags": map[string]string{"team": "a", "env": "dev"}})
			e.rgta("UntagResources", map[string]any{"ResourceARNList": arns, "TagKeys": []string{"env"}})
			for _, arn := range arns {
				assert.Equal(t, map[string]string{"team": "a"}, e.tagsOf(arn), arn)
			}

			// Absent resources of each pilot are the tagging API's not-found.
			missing := []string{
				"arn:aws:ec2:us-east-1:123456789012:security-group/sg-0000000000000000",
				"arn:aws:s3:::no-such-bucket",
				"arn:aws:cloudwatch:us-east-1:123456789012:alarm:none",
			}
			out := e.rgta("TagResources", map[string]any{"ResourceARNList": missing, "Tags": map[string]string{"k": "v"}})
			for _, arn := range missing {
				code, status := providerFailure(t, out, arn)
				assert.Equal(t, "InvalidParameterException", code, arn)
				assert.Equal(t, http.StatusBadRequest, status, arn)
			}

			// An ARN of a type the pilot cannot tag is the unsupported-type answer.
			unsupported := []string{
				"arn:aws:ec2:us-east-1:123456789012:vpc-endpoint/vpce-1",
				"arn:aws:s3:::orders/key",
				"arn:aws:cloudwatch:us-east-1:123456789012:dashboard/d",
			}
			out = e.rgta("TagResources", map[string]any{"ResourceARNList": unsupported, "Tags": map[string]string{"k": "v"}})
			for _, arn := range unsupported {
				code, status := providerFailure(t, out, arn)
				assert.Equal(t, "InternalServiceException", code, arn)
				assert.Equal(t, http.StatusInternalServerError, status, arn)
			}
		})
	}
}

func TestTaggingProviders_QuotasAreTheOwnersAnswer(t *testing.T) {
	t.Parallel()
	e := newProviderEnv(t, true)
	sgARN, _ := e.createSecurityGroup("full")
	alarmARN := e.createAlarm("full")
	seed := map[string]string{"seed": "x"}
	e.rgta("TagResources", map[string]any{"ResourceARNList": []string{sgARN, alarmARN}, "Tags": seed})
	many := map[string]string{}
	for i := range 51 {
		many[fmt.Sprintf("k%02d", i)] = "v"
	}
	out := e.rgta("TagResources", map[string]any{"ResourceARNList": []string{sgARN, alarmARN}, "Tags": many})
	code, _ := providerFailure(t, out, sgARN)
	assert.Equal(t, "TagLimitExceeded", code, "EC2's own code for its 50-tag quota")
	code, _ = providerFailure(t, out, alarmARN)
	assert.Equal(t, "InvalidParameterValue", code, "CloudWatch's own code for its 50-tag quota")
	assert.Equal(t, seed, e.tagsOf(sgARN), "a refused merge writes nothing")
	assert.Equal(t, seed, e.tagsOf(alarmARN), "a refused merge writes nothing")

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
