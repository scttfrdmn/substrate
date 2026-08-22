package emulator_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The configuration-recorder cluster (#580).
//
// Every case here goes through the full server rather than calling the plugin
// directly, because the thing most likely to be wrong about a new JSON-target
// service is whether a request reaches it at all — that is what #561 and #580's
// own routing gap were, and a direct plugin call cannot see it.
//
// The headline assertion is TestConfigRecorder_ExistsButIsNotRecording: a recorder
// that exists and is not recording is the misconfiguration the whole plugin is for,
// and the two operations that look like they answer the same question must disagree
// about it.

// configRequest posts an AWS Config JSON-target request in us-east-1 and returns the
// response.
func configRequest(t *testing.T, ts *emulator.TestServer, operation string, body any) *http.Response {
	t.Helper()
	return configRequestIn(t, ts, "us-east-1", operation, body)
}

// configRequestIn posts an AWS Config JSON-target request against a named Region.
//
// The request is signed with the built-in test key, which resolves the caller to
// account 123456789012; an unsigned one would land in 000000000000 and the ARNs
// these cases assert would silently be a different account's. The Region rides in
// both the host and the credential scope, as it does from a real client, so the
// regional-isolation cases exercise the same resolution path as the rest.
func configRequestIn(t *testing.T, ts *emulator.TestServer, region, operation string, body any) *http.Response {
	t.Helper()

	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal %s: %v", operation, err)
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("build %s request: %v", operation, err)
	}
	req.Host = "config." + region + ".amazonaws.com"
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "StarlingDoveService."+operation)
	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential=AKIATEST12345678901/20260101/"+region+
			"/config/aws4_request, SignedHeaders=host, Signature=fake")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", operation, err)
	}
	return resp
}

// decodeConfigResponse decodes a JSON 1.1 response into out and returns the status
// and the error code a refusal carries.
//
// The code comes from "__type", which is where the JSON-RPC protocols put it and
// what an SDK matches on. Every Config exception is HTTP 400 — including the
// not-found ones — so the status alone distinguishes nothing and the code is what
// every refusal case asserts.
func decodeConfigResponse(t *testing.T, resp *http.Response, out any) (status int, code, message string) {
	t.Helper()
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Fatalf("close body: %v", err)
		}
	}()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		var errBody struct {
			Type    string `json:"__type"`
			Message string `json:"message"`
			Msg     string `json:"Message"`
		}
		if err := json.Unmarshal(raw, &errBody); err != nil {
			t.Fatalf("unmarshal error body %q: %v", raw, err)
		}
		msg := errBody.Message
		if msg == "" {
			msg = errBody.Msg
		}
		return resp.StatusCode, errBody.Type, msg
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("unmarshal body %q: %v", raw, err)
		}
	}
	return resp.StatusCode, "", ""
}

// configRecorderPayload is the ConfigurationRecorder a Put sends. Its members are
// lowerCamel because that is what the API model declares for this shape, unlike the
// request that wraps it.
func configRecorderPayload(name string) map[string]any {
	return map[string]any{
		"name":    name,
		"roleARN": "arn:aws:iam::123456789012:role/aws-service-role/config.amazonaws.com/AWSServiceRoleForConfig",
	}
}

// configPutRecorder creates a recorder and requires that it succeed, for the many
// cases whose subject is something else.
func configPutRecorder(t *testing.T, ts *emulator.TestServer, name string) {
	t.Helper()
	configPutRecorderIn(t, ts, "us-east-1", name)
}

// configPutRecorderIn creates a recorder in a named Region, for the cases that need a
// second Region to have one before it can create anything there.
func configPutRecorderIn(t *testing.T, ts *emulator.TestServer, region, name string) {
	t.Helper()
	resp := configRequestIn(t, ts, region, "PutConfigurationRecorder", map[string]any{
		"ConfigurationRecorder": configRecorderPayload(name),
	})
	status, code, message := decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusOK, status, "%s in %s: %s", code, region, message)
}

// configDescribeRecorders returns the recorders DescribeConfigurationRecorders
// reports.
func configDescribeRecorders(t *testing.T, ts *emulator.TestServer) []map[string]any {
	t.Helper()
	resp := configRequest(t, ts, "DescribeConfigurationRecorders", map[string]any{})
	var out struct {
		ConfigurationRecorders []map[string]any `json:"ConfigurationRecorders"`
	}
	status, code, message := decodeConfigResponse(t, resp, &out)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	return out.ConfigurationRecorders
}

// configRecorderARN returns the ARN of the account's recorder in a Region, read back
// from the API rather than assembled in the test.
//
// The ARN's trailing RecorderId component is minted from a hash — the API model has
// no member that carries it, yet the Service Authorization Reference's template
// requires it — so a test that spelled the whole ARN out would be asserting the hash
// rather than the behavior it cares about. TestConfigRecorder_NameDefaultsToDefault is
// the one place the ARN's documented *shape* is asserted.
func configRecorderARN(t *testing.T, ts *emulator.TestServer, region string) string {
	t.Helper()
	resp := configRequestIn(t, ts, region, "DescribeConfigurationRecorders", map[string]any{})
	var out struct {
		ConfigurationRecorders []map[string]any `json:"ConfigurationRecorders"`
	}
	status, code, message := decodeConfigResponse(t, resp, &out)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	require.Len(t, out.ConfigurationRecorders, 1)
	arn, ok := out.ConfigurationRecorders[0]["arn"].(string)
	require.True(t, ok, "the recorder reports an arn")
	return arn
}

// configDescribeRecorderStatus returns the statuses
// DescribeConfigurationRecorderStatus reports.
func configDescribeRecorderStatus(t *testing.T, ts *emulator.TestServer) []map[string]any {
	t.Helper()
	resp := configRequest(t, ts, "DescribeConfigurationRecorderStatus", map[string]any{})
	var out struct {
		ConfigurationRecordersStatus []map[string]any `json:"ConfigurationRecordersStatus"`
	}
	status, code, message := decodeConfigResponse(t, resp, &out)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	return out.ConfigurationRecordersStatus
}

// configStartRecorder starts the named recorder, requiring success.
func configStartRecorder(t *testing.T, ts *emulator.TestServer, name string) {
	t.Helper()
	resp := configRequest(t, ts, "StartConfigurationRecorder", map[string]any{
		"ConfigurationRecorderName": name,
	})
	status, code, message := decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
}

// configPutChannel creates a delivery channel with a bucket whose policy admits
// Config, which is the setup StartConfigurationRecorder needs.
func configPutChannel(t *testing.T, ts *emulator.TestServer, name, bucket string) {
	t.Helper()
	if bucket != "" {
		configSeedDeliveryPolicy(t, ts, bucket, "ok")
	}
	channel := map[string]any{"name": name}
	if bucket != "" {
		channel["s3BucketName"] = bucket
	}
	resp := configRequest(t, ts, "PutDeliveryChannel", map[string]any{"DeliveryChannel": channel})
	status, code, message := decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
}

func TestConfigRecorder_ExistsButIsNotRecording(t *testing.T) {
	// The release's headline behavior. DescribeConfigurationRecorders reporting a
	// recorder says nothing about whether it is recording, and a consumer that checks
	// only that call reports an account as covered when it is not. The two operations
	// must disagree here, and after a Start they must agree.
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")

	recorders := configDescribeRecorders(t, ts)
	require.Len(t, recorders, 1, "the recorder exists and Describe reports it in full")
	assert.Equal(t, "default", recorders[0]["name"])

	statuses := configDescribeRecorderStatus(t, ts)
	require.Len(t, statuses, 1)
	assert.Equal(t, false, statuses[0]["recording"],
		"a recorder that was created and never started records nothing — this is the whole point")

	// And only Start changes it, which is what makes the first assertion a behavior
	// rather than an omission.
	configPutChannel(t, ts, "default", "cfg-logs")
	configStartRecorder(t, ts, "default")

	statuses = configDescribeRecorderStatus(t, ts)
	require.Len(t, statuses, 1)
	assert.Equal(t, true, statuses[0]["recording"])
	assert.Equal(t, "Success", statuses[0]["lastStatus"])
	assert.NotNil(t, statuses[0]["lastStartTime"], "a started recorder reports when it started")
}

func TestConfigRecorder_DescribeIsEmptyBeforeAnyPut(t *testing.T) {
	// A fresh account has no recorder, and that is an empty list rather than a
	// refusal: an emulator answering NoSuchConfigurationRecorder to an unfiltered
	// Describe would make a consumer's "am I set up?" check fail where AWS answers it.
	ts := emulator.StartTestServer(t)

	assert.Empty(t, configDescribeRecorders(t, ts))
	assert.Empty(t, configDescribeRecorderStatus(t, ts))
}

func TestConfigRecorder_PutIsIdempotentAndDoesNotRetag(t *testing.T) {
	// "Subsequent requests won't create a duplicate resource" — and the tags "are
	// added at creation and are not updated with configuration recorder updates". The
	// second half is the surprising one, so a consumer that expects a Put to retag has
	// a bug substrate must report rather than hide.
	ts := emulator.StartTestServer(t)

	resp := configRequest(t, ts, "PutConfigurationRecorder", map[string]any{
		"ConfigurationRecorder": configRecorderPayload("default"),
		"Tags":                  []map[string]string{{"Key": "env", "Value": "prod"}},
	})
	status, code, message := decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	// A second Put updates the role, and must neither create a second recorder nor
	// replace the tags.
	updated := configRecorderPayload("default")
	updated["roleARN"] = "arn:aws:iam::123456789012:role/second"
	resp = configRequest(t, ts, "PutConfigurationRecorder", map[string]any{
		"ConfigurationRecorder": updated,
		"Tags":                  []map[string]string{{"Key": "env", "Value": "dev"}},
	})
	status, code, message = decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	recorders := configDescribeRecorders(t, ts)
	require.Len(t, recorders, 1, "a second Put with the same name updates rather than duplicates")
	assert.Equal(t, "arn:aws:iam::123456789012:role/second", recorders[0]["roleARN"],
		"the role is updated")

	tags := configStoredTags(t, ts, configRecorderARN(t, ts, "us-east-1"))
	assert.Equal(t, map[string]string{"env": "prod"}, tags,
		"tags are set at creation and a later Put does not update them")
}

// configStoredTags reads the tags substrate stored for a resource ARN.
//
// ListTagsForResource reads the same tags over the wire, and
// TestConfigTags_CreationTimeTagsAreVisibleToTheTagAPI asserts the two agree. This
// helper stays because a creation-time tag is stored before any tag operation is
// called, so the state read is what tells a Put's own behavior apart from the tag
// cluster's.
func configStoredTags(t *testing.T, ts *emulator.TestServer, arn string) map[string]string {
	t.Helper()
	raw, err := ts.StateManager().Get(t.Context(), "config", "tags:"+arn)
	require.NoError(t, err)
	if raw == nil {
		return nil
	}
	var tags map[string]string
	require.NoError(t, json.Unmarshal(raw, &tags))
	return tags
}

func TestConfigRecorder_ASecondNameIsRefused(t *testing.T) {
	// One recorder per account per Region. AWS documents this on the operation rather
	// than on the service-limits page, and it is what makes the exception reachable at
	// a count of two rather than at some larger ceiling.
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")

	resp := configRequest(t, ts, "PutConfigurationRecorder", map[string]any{
		"ConfigurationRecorder": configRecorderPayload("second"),
	})
	status, code, message := decodeConfigResponse(t, resp, nil)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "MaxNumberOfConfigurationRecordersExceededException", code)
	assert.Contains(t, message, "reached the limit")

	assert.Len(t, configDescribeRecorders(t, ts), 1, "the refusal stored nothing")
}

func TestConfigRecorder_NameDefaultsToDefault(t *testing.T) {
	// "If you do not specify a name, the default is used." A consumer that omits the
	// name and then starts "default" — which is what every console-derived script does
	// — must find a recorder there.
	ts := emulator.StartTestServer(t)

	resp := configRequest(t, ts, "PutConfigurationRecorder", map[string]any{
		"ConfigurationRecorder": map[string]any{
			"roleARN": "arn:aws:iam::123456789012:role/config",
		},
	})
	status, code, message := decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	recorders := configDescribeRecorders(t, ts)
	require.Len(t, recorders, 1)
	assert.Equal(t, "default", recorders[0]["name"])
	// The Service Authorization Reference's template is
	// configuration-recorder/${RecorderName}/${RecorderId} — the segment is
	// "configuration-recorder", not "config-recorder" or "recorder", and it carries two
	// components. RecorderId has no member anywhere in the API model, so substrate mints
	// it; only its shape is asserted.
	assert.Regexp(t,
		`^arn:aws:config:us-east-1:123456789012:configuration-recorder/default/[a-z0-9_-]{8}$`,
		recorders[0]["arn"])
}

func TestConfigRecorder_RefusesAnEmptyRoleARN(t *testing.T) {
	// InvalidRoleException is a presence check and nothing more — "You have provided a
	// null or empty Amazon Resource Name (ARN)". #580 described it as an assumability
	// check; modeling it that way would refuse requests AWS accepts, so a role that
	// does not exist is deliberately fine here and only an absent one is refused.
	cases := []struct {
		name     string
		recorder map[string]any
	}{
		{"omitted", map[string]any{"name": "default"}},
		{"empty", map[string]any{"name": "default", "roleARN": ""}},
		{"whitespace", map[string]any{"name": "default", "roleARN": "   "}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts := emulator.StartTestServer(t)
			resp := configRequest(t, ts, "PutConfigurationRecorder", map[string]any{
				"ConfigurationRecorder": tc.recorder,
			})
			status, code, message := decodeConfigResponse(t, resp, nil)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidRoleException", code)
			assert.Contains(t, message, "null or empty")
		})
	}

	t.Run("a role that does not exist is accepted", func(t *testing.T) {
		ts := emulator.StartTestServer(t)
		resp := configRequest(t, ts, "PutConfigurationRecorder", map[string]any{
			"ConfigurationRecorder": map[string]any{
				"name":    "default",
				"roleARN": "arn:aws:iam::123456789012:role/never-created",
			},
		})
		status, code, message := decodeConfigResponse(t, resp, nil)
		assert.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	})
}

func TestConfigRecorder_RefusesTheReservedNamePrefix(t *testing.T) {
	// AWS reserves AWSConfigurationRecorderFor for service-linked recorders, which are
	// created through PutServiceLinkedConfigurationRecorder and which substrate does
	// not model. The refusal still matters: a consumer that picks the prefix gets the
	// error AWS gives rather than a recorder AWS would not have made.
	ts := emulator.StartTestServer(t)

	resp := configRequest(t, ts, "PutConfigurationRecorder", map[string]any{
		"ConfigurationRecorder": configRecorderPayload("AWSConfigurationRecorderForSecurityHub"),
	})
	status, code, message := decodeConfigResponse(t, resp, nil)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidConfigurationRecorderNameException", code)
	assert.Contains(t, message, "AWSConfigurationRecorderFor")
}

func TestConfigRecorder_RefusesAnInvalidRecordingGroup(t *testing.T) {
	// The five cases the reference enumerates, minus "too many resource types", for
	// which no ceiling is documented anywhere — inventing one would refuse a request
	// AWS accepts. wantMessage is asserted rather than only the code, because all of
	// these share one exception and a test checking only the code would pass with any
	// single arm deleted.
	cases := []struct {
		name        string
		group       map[string]any
		wantMessage string
	}{
		{
			"allSupported with resourceTypes",
			map[string]any{"allSupported": true, "resourceTypes": []string{"AWS::S3::Bucket"}},
			"resourceTypes is not empty",
		},
		{
			"allSupported with an exclusion strategy",
			map[string]any{
				"allSupported":      true,
				"recordingStrategy": map[string]any{"useOnly": "EXCLUSION_BY_RESOURCE_TYPES"},
			},
			"These settings conflict",
		},
		{
			"every parameter null false or empty",
			map[string]any{"allSupported": false},
			"null, false, or empty",
		},
		{
			"an invalid strategy",
			map[string]any{
				"resourceTypes":     []string{"AWS::S3::Bucket"},
				"recordingStrategy": map[string]any{"useOnly": "SOME_OTHER_STRATEGY"},
			},
			"not a valid recording strategy",
		},
		{
			"an invalid resource type",
			map[string]any{"resourceTypes": []string{"S3Bucket"}},
			"not a valid resource type",
		},
		{
			"an invalid excluded resource type",
			map[string]any{
				"exclusionByResourceTypes": map[string]any{"resourceTypes": []string{"nonsense"}},
				"recordingStrategy":        map[string]any{"useOnly": "EXCLUSION_BY_RESOURCE_TYPES"},
			},
			"not a valid resource type",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts := emulator.StartTestServer(t)
			recorder := configRecorderPayload("default")
			recorder["recordingGroup"] = tc.group

			resp := configRequest(t, ts, "PutConfigurationRecorder", map[string]any{
				"ConfigurationRecorder": recorder,
			})
			status, code, message := decodeConfigResponse(t, resp, nil)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidRecordingGroupException", code)
			assert.Contains(t, message, tc.wantMessage)
		})
	}
}

func TestConfigRecorder_AcceptsTheRecordingGroupsAWSDoes(t *testing.T) {
	// The shapes a real template sends. Each must be accepted: a group refused here
	// that AWS accepts would fail a working stack, which is the worse of the two
	// failure directions.
	cases := []struct {
		name  string
		group map[string]any
	}{
		{"allSupported alone", map[string]any{"allSupported": true}},
		{
			"allSupported with global types",
			map[string]any{"allSupported": true, "includeGlobalResourceTypes": true},
		},
		{
			"an inclusion list",
			map[string]any{
				"resourceTypes":     []string{"AWS::S3::Bucket", "AWS::EC2::Instance"},
				"recordingStrategy": map[string]any{"useOnly": "INCLUSION_BY_RESOURCE_TYPES"},
			},
		},
		{
			"an exclusion list",
			map[string]any{
				"exclusionByResourceTypes": map[string]any{"resourceTypes": []string{"AWS::IAM::Role"}},
				"recordingStrategy":        map[string]any{"useOnly": "EXCLUSION_BY_RESOURCE_TYPES"},
			},
		},
		{
			"a resource type from a service substrate does not emulate",
			map[string]any{
				"resourceTypes": []string{"AWS::Panorama::Package"},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts := emulator.StartTestServer(t)
			recorder := configRecorderPayload("default")
			recorder["recordingGroup"] = tc.group

			resp := configRequest(t, ts, "PutConfigurationRecorder", map[string]any{
				"ConfigurationRecorder": recorder,
			})
			status, code, message := decodeConfigResponse(t, resp, nil)
			assert.Equal(t, http.StatusOK, status, "%s: %s", code, message)
		})
	}
}

func TestConfigRecorder_TheDefaultGroupExcludesGlobalIAMTypes(t *testing.T) {
	// "All supported resource types, excluding the global IAM resource types" — and the
	// way AWS says so is includeGlobalResourceTypes false, that field being "a bundle
	// which only applies to the global IAM resource types: IAM users, groups, roles, and
	// customer managed policies".
	//
	// It is specifically not said by synthesizing an exclusionByResourceTypes list of
	// those four types. AWS returns no such list for an allSupported group, and an
	// exclusion list alongside ALL_SUPPORTED_RESOURCE_TYPES is the combination
	// InvalidRecordingGroupException exists to refuse — so emitting one would have
	// substrate answering with a group it would itself reject on input.
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")

	recorders := configDescribeRecorders(t, ts)
	require.Len(t, recorders, 1)
	group, ok := recorders[0]["recordingGroup"].(map[string]any)
	require.True(t, ok, "a Put with no group is reported with the default one")
	assert.Equal(t, true, group["allSupported"])
	assert.Equal(t, false, group["includeGlobalResourceTypes"],
		"the global IAM types are the bundle this field turns off")
	assert.NotContains(t, group, "exclusionByResourceTypes",
		"an allSupported group carries no exclusion list")

	strategy, ok := group["recordingStrategy"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "ALL_SUPPORTED_RESOURCE_TYPES", strategy["useOnly"])
}

func TestConfigRecorder_StartRefusesWithoutADeliveryChannel(t *testing.T) {
	// "You must have created a delivery channel to successfully start the customer
	// managed configuration recorder." #580 omitted this refusal; without it the
	// emulator would report a recording recorder that AWS would not have started,
	// which is precisely the wrong answer to give a consumer with an ordering bug.
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")

	resp := configRequest(t, ts, "StartConfigurationRecorder", map[string]any{
		"ConfigurationRecorderName": "default",
	})
	status, code, message := decodeConfigResponse(t, resp, nil)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NoAvailableDeliveryChannelException", code)
	assert.Contains(t, message, "no delivery channel available")

	statuses := configDescribeRecorderStatus(t, ts)
	require.Len(t, statuses, 1)
	assert.Equal(t, false, statuses[0]["recording"], "the refusal did not start anything")
}

func TestConfigRecorder_StartAndStopAreIdempotent(t *testing.T) {
	// No refusal is documented for starting a running recorder or stopping a stopped
	// one, so both are no-ops at 200. Inventing a refusal would fail a retry that AWS
	// accepts — and a retry is exactly what a consumer whose first call timed out
	// sends.
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")
	configPutChannel(t, ts, "default", "cfg-logs")

	configStartRecorder(t, ts, "default")
	configStartRecorder(t, ts, "default")
	statuses := configDescribeRecorderStatus(t, ts)
	require.Len(t, statuses, 1)
	assert.Equal(t, true, statuses[0]["recording"])

	for range 2 {
		resp := configRequest(t, ts, "StopConfigurationRecorder", map[string]any{
			"ConfigurationRecorderName": "default",
		})
		status, code, message := decodeConfigResponse(t, resp, nil)
		require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	}
	statuses = configDescribeRecorderStatus(t, ts)
	require.Len(t, statuses, 1)
	assert.Equal(t, false, statuses[0]["recording"])
	assert.NotNil(t, statuses[0]["lastStopTime"])
}

func TestConfigRecorder_AnUnknownNameIsRefused(t *testing.T) {
	// Every operation that names a recorder refuses a name that is not the account's,
	// and every one of them does so at HTTP 400 — this exception reports a missing
	// entity and its reference page still says 400, so a 404 here would be a shape a
	// consumer cannot reproduce against AWS.
	operations := []string{
		"StartConfigurationRecorder",
		"StopConfigurationRecorder",
		"DeleteConfigurationRecorder",
	}
	for _, op := range operations {
		t.Run(op+"/no recorder at all", func(t *testing.T) {
			ts := emulator.StartTestServer(t)
			resp := configRequest(t, ts, op, map[string]any{"ConfigurationRecorderName": "ghost"})
			status, code, message := decodeConfigResponse(t, resp, nil)
			assert.Equal(t, http.StatusBadRequest, status,
				"every Config exception is 400, including the not-found ones")
			assert.Equal(t, "NoSuchConfigurationRecorderException", code)
			assert.Contains(t, message, "does not exist")
		})

		t.Run(op+"/a different recorder exists", func(t *testing.T) {
			ts := emulator.StartTestServer(t)
			configPutRecorder(t, ts, "default")
			resp := configRequest(t, ts, op, map[string]any{"ConfigurationRecorderName": "ghost"})
			status, code, _ := decodeConfigResponse(t, resp, nil)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "NoSuchConfigurationRecorderException", code)
		})
	}

	// The describe operations refuse a named recorder that does not exist too — the
	// caller asserted the name — while an *unfiltered* describe answers with an empty
	// list, which the fresh-account test pins.
	for _, op := range []string{"DescribeConfigurationRecorders", "DescribeConfigurationRecorderStatus"} {
		t.Run(op+"/by name", func(t *testing.T) {
			ts := emulator.StartTestServer(t)
			resp := configRequest(t, ts, op, map[string]any{
				"ConfigurationRecorderNames": []string{"ghost"},
			})
			status, code, _ := decodeConfigResponse(t, resp, nil)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "NoSuchConfigurationRecorderException", code)
		})

		t.Run(op+"/by arn", func(t *testing.T) {
			ts := emulator.StartTestServer(t)
			configPutRecorder(t, ts, "default")
			resp := configRequest(t, ts, op, map[string]any{
				"Arn": "arn:aws:config:us-east-1:123456789012:configuration-recorder/ghost/deadbeef",
			})
			status, code, _ := decodeConfigResponse(t, resp, nil)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "NoSuchConfigurationRecorderException", code)
		})
	}
}

func TestConfigRecorder_DescribeAcceptsTheRecordersOwnNameAndARN(t *testing.T) {
	// The filters must select as well as refuse: a describe naming the recorder that
	// does exist has to find it, or the refusal above would be indistinguishable from
	// a filter that matches nothing.
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")
	arn := configRecorderARN(t, ts, "us-east-1")

	for _, body := range []map[string]any{
		{"ConfigurationRecorderNames": []string{"default"}},
		{"Arn": arn},
		{"ConfigurationRecorderNames": []string{"default"}, "Arn": arn},
	} {
		resp := configRequest(t, ts, "DescribeConfigurationRecorders", body)
		var out struct {
			ConfigurationRecorders []map[string]any `json:"ConfigurationRecorders"`
		}
		status, code, message := decodeConfigResponse(t, resp, &out)
		require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
		require.Len(t, out.ConfigurationRecorders, 1)
		assert.Equal(t, "default", out.ConfigurationRecorders[0]["name"])
	}
}

func TestConfigRecorder_MoreThanOneNameIsAValidationException(t *testing.T) {
	// "You have specified more than one configuration recorder." An account has at
	// most one, so a list of two is a request that cannot be satisfied — and reporting
	// an empty list instead would leave a consumer to conclude it has no recorder.
	for _, op := range []string{"DescribeConfigurationRecorders", "DescribeConfigurationRecorderStatus"} {
		t.Run(op, func(t *testing.T) {
			ts := emulator.StartTestServer(t)
			configPutRecorder(t, ts, "default")

			resp := configRequest(t, ts, op, map[string]any{
				"ConfigurationRecorderNames": []string{"default", "other"},
			})
			status, code, message := decodeConfigResponse(t, resp, nil)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "ValidationException", code)
			assert.Contains(t, message, "more than one configuration recorder")
		})
	}
}

func TestConfigRecorder_AServicePrincipalFilterMatchesNothing(t *testing.T) {
	// A service principal selects a service-linked recorder, and substrate models
	// none. Reporting the customer managed recorder for such a filter would answer a
	// question the caller did not ask; an invalid principal is refused with the
	// exception's own wording.
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")

	resp := configRequest(t, ts, "DescribeConfigurationRecorders", map[string]any{
		"ServicePrincipal": "securityhub.amazonaws.com",
	})
	var out struct {
		ConfigurationRecorders []map[string]any `json:"ConfigurationRecorders"`
	}
	status, code, message := decodeConfigResponse(t, resp, &out)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	assert.Empty(t, out.ConfigurationRecorders,
		"substrate models no service-linked recorders, so this filter selects nothing")

	resp = configRequest(t, ts, "DescribeConfigurationRecorders", map[string]any{
		"ServicePrincipal": "not a principal!",
	})
	status, code, message = decodeConfigResponse(t, resp, nil)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ValidationException", code)
	assert.Contains(t, message, "service principal")
}

func TestConfigRecorder_IsIsolatedByRegion(t *testing.T) {
	// "Recording in one Region only" is a real and common misconfiguration, and a
	// state key that dropped the Region would make it unreachable: the recorder created
	// in us-east-1 would appear in eu-west-1 and the consumer's multi-Region check
	// would pass everywhere.
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")

	resp := configRequestIn(t, ts, "eu-west-1", "DescribeConfigurationRecorders", map[string]any{})
	var out struct {
		ConfigurationRecorders []map[string]any `json:"ConfigurationRecorders"`
	}
	status, code, message := decodeConfigResponse(t, resp, &out)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	assert.Empty(t, out.ConfigurationRecorders, "a recorder in us-east-1 is not in eu-west-1")

	// And a recorder can be created there without colliding with the first, which the
	// max-recorders refusal would otherwise prevent.
	resp = configRequestIn(t, ts, "eu-west-1", "PutConfigurationRecorder", map[string]any{
		"ConfigurationRecorder": configRecorderPayload("default"),
	})
	status, code, message = decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	resp = configRequestIn(t, ts, "eu-west-1", "DescribeConfigurationRecorders", map[string]any{})
	status, code, message = decodeConfigResponse(t, resp, &out)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	require.Len(t, out.ConfigurationRecorders, 1)
	assert.Regexp(t, `^arn:aws:config:eu-west-1:123456789012:configuration-recorder/default/`,
		out.ConfigurationRecorders[0]["arn"], "the ARN carries the Region it was created in")
	assert.NotEqual(t, configRecorderARN(t, ts, "us-east-1"),
		out.ConfigurationRecorders[0]["arn"],
		"the minted RecorderId is derived from the Region, so the two recorders differ by more "+
			"than the ARN's region field")
}

func TestConfigRecorder_DeleteRemovesEverythingAboutIt(t *testing.T) {
	// A rebuilt recorder must not inherit its predecessor's tags or status: "if you
	// delete a resource, tags for the resource are deleted as well", and a fixture that
	// tears down and rebuilds is just the same test run twice.
	ts := emulator.StartTestServer(t)

	resp := configRequest(t, ts, "PutConfigurationRecorder", map[string]any{
		"ConfigurationRecorder": configRecorderPayload("default"),
		"Tags":                  []map[string]string{{"Key": "env", "Value": "prod"}},
	})
	status, code, message := decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	arn := configRecorderARN(t, ts, "us-east-1")
	configPutChannel(t, ts, "default", "cfg-logs")
	configStartRecorder(t, ts, "default")

	resp = configRequest(t, ts, "DeleteConfigurationRecorder", map[string]any{
		"ConfigurationRecorderName": "default",
	})
	status, code, message = decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	assert.Empty(t, configDescribeRecorders(t, ts))
	assert.Empty(t, configDescribeRecorderStatus(t, ts))
	assert.Nil(t, configStoredTags(t, ts, arn), "tags go with the resource")

	// A rebuilt recorder starts from nothing: not recording, and untagged.
	configPutRecorder(t, ts, "default")
	statuses := configDescribeRecorderStatus(t, ts)
	require.Len(t, statuses, 1)
	assert.Equal(t, false, statuses[0]["recording"],
		"a rebuilt recorder is not recording, whatever its predecessor was doing")
	assert.Nil(t, configStoredTags(t, ts, arn))
}

func TestConfigRecorder_RefusesTagsAWSWouldRefuse(t *testing.T) {
	// Creation-time tags go through the same validation the tag operations will, so a
	// tag AWS refuses cannot enter through the Put and be refused at TagResource.
	cases := []struct {
		name        string
		tags        []map[string]string
		wantMessage string
	}{
		{"the reserved prefix", []map[string]string{{"Key": "aws:internal", "Value": "x"}}, "reserved"},
		{"an empty key", []map[string]string{{"Key": "", "Value": "x"}}, "cannot be empty"},
		{
			"a key outside the charset",
			[]map[string]string{{"Key": "bad!key", "Value": "x"}},
			"Unicode letters",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts := emulator.StartTestServer(t)
			resp := configRequest(t, ts, "PutConfigurationRecorder", map[string]any{
				"ConfigurationRecorder": configRecorderPayload("default"),
				"Tags":                  tc.tags,
			})
			status, code, message := decodeConfigResponse(t, resp, nil)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "ValidationException", code)
			assert.Contains(t, message, tc.wantMessage)
		})
	}

	t.Run("an empty value is accepted", func(t *testing.T) {
		// "You can set the value of a tag to an empty string, but you can't set the value
		// of a tag to null."
		ts := emulator.StartTestServer(t)
		resp := configRequest(t, ts, "PutConfigurationRecorder", map[string]any{
			"ConfigurationRecorder": configRecorderPayload("default"),
			"Tags":                  []map[string]string{{"Key": "env", "Value": ""}},
		})
		status, code, message := decodeConfigResponse(t, resp, nil)
		require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
		assert.Equal(t, map[string]string{"env": ""},
			configStoredTags(t, ts, configRecorderARN(t, ts, "us-east-1")))
	})
}

func TestConfigRecorder_RefusesAMissingRecorderMember(t *testing.T) {
	// ConfigurationRecorder is the one required member, and a request without it is a
	// ValidationException rather than a recorder named "default" conjured from nothing.
	ts := emulator.StartTestServer(t)

	resp := configRequest(t, ts, "PutConfigurationRecorder", map[string]any{})
	status, code, message := decodeConfigResponse(t, resp, nil)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ValidationException", code)
	assert.Contains(t, message, "ConfigurationRecorder is required")
}

func TestConfigService_AnUnimplementedOperationNamesItself(t *testing.T) {
	// AWS Config has 97 operations and substrate implements the detective-controls
	// subset. An unimplemented one must say which, so a consumer discovers the gap from
	// the response rather than by reading the plugin. Config is JSON-RPC, so the code
	// and status are the UnknownOperationException/404 AWS publishes for that protocol
	// (#716).
	ts := emulator.StartTestServer(t)

	resp := configRequest(t, ts, "SelectResourceConfig", map[string]any{})
	status, code, message := decodeConfigResponse(t, resp, nil)
	assert.Equal(t, http.StatusNotFound, status)
	assert.Equal(t, "UnknownOperationException", code)
	assert.Contains(t, message, "SelectResourceConfig")
}
