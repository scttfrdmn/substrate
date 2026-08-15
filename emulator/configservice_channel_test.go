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

// The delivery-channel cluster, the S3 bucket-policy check, and the seeds (#580).
//
// The bucket-policy cases are the interesting ones. PutDeliveryChannel is the only
// Config operation whose success depends on another service's state, and the check
// is deliberately permissive: the tests below assert both halves of that decision —
// what is refused (no bucket, no policy at all, a policy that admits nothing) and
// what is accepted (every plausible variant, and anything substrate cannot parse).
// A test suite that only asserted the refusals would pass with a matcher that
// refuses everything, which is the failure direction that breaks working consumers.

// configSeedDeliveryPolicy pins the delivery-policy outcome for a bucket.
func configSeedDeliveryPolicy(t *testing.T, ts *emulator.TestServer, bucket, outcome string) {
	t.Helper()
	configSeed(t, ts, "/v1/config/delivery-policy", map[string]any{
		"bucket": bucket, "outcome": outcome,
	})
}

// configSeed posts a control-plane seed and requires that it be accepted.
func configSeed(t *testing.T, ts *emulator.TestServer, path string, body any) {
	t.Helper()
	status, response := configSeedRaw(t, ts, path, body)
	require.Equal(t, http.StatusOK, status, "seed %s: %s", path, response)
}

// configSeedRaw posts a control-plane seed and returns the status and body, for the
// cases whose subject is the refusal.
func configSeedRaw(t *testing.T, ts *emulator.TestServer, path string, body any) (int, string) {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+path, bytes.NewReader(data))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(raw)
}

// configClearSeed deletes a control-plane seed.
func configClearSeed(t *testing.T, ts *emulator.TestServer, path string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, ts.URL+path, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// configPutBucket creates an S3 bucket through the S3 plugin, so the delivery check
// reads real state rather than state a test wrote by hand.
func configPutBucket(t *testing.T, ts *emulator.TestServer, bucket string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, ts.URL+"/"+bucket, nil)
	require.NoError(t, err)
	req.Host = "s3.us-east-1.amazonaws.com"

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, "create bucket %s", bucket)
}

// configPutBucketPolicy sets a bucket policy through the S3 plugin.
func configPutBucketPolicy(t *testing.T, ts *emulator.TestServer, bucket, policy string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut,
		ts.URL+"/"+bucket+"?policy", bytes.NewReader([]byte(policy)))
	require.NoError(t, err)
	req.Host = "s3.us-east-1.amazonaws.com"

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	// PutBucketPolicy answers 204, as real S3 does.
	require.Equal(t, http.StatusNoContent, resp.StatusCode, "put policy on %s", bucket)
}

// configPutChannelWithTopic updates the channel to carry an SNS topic, which is what
// switches the notification stream's status on.
func configPutChannelWithTopic(t *testing.T, ts *emulator.TestServer, name, bucket, topic string) {
	t.Helper()
	status, code, message := configPutChannelRaw(t, ts, map[string]any{
		"name": name, "s3BucketName": bucket, "snsTopicARN": topic,
	})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
}

// configDeliveryPolicyAdmittingConfig is the policy the AWS documentation gives for
// a Config delivery bucket, trimmed to the statement the check is about.
const configDeliveryPolicyAdmittingConfig = `{"Version":"2012-10-17","Statement":[{
	"Sid":"AWSConfigBucketDelivery","Effect":"Allow",
	"Principal":{"Service":"config.amazonaws.com"},
	"Action":"s3:PutObject","Resource":"arn:aws:s3:::cfg-logs/*"}]}`

// configPutChannelRaw posts a PutDeliveryChannel and returns the refusal, if any.
func configPutChannelRaw(t *testing.T, ts *emulator.TestServer, channel map[string]any) (int, string, string) {
	t.Helper()
	resp := configRequest(t, ts, "PutDeliveryChannel", map[string]any{"DeliveryChannel": channel})
	return decodeConfigResponse(t, resp, nil)
}

// configDescribeChannels returns the channels DescribeDeliveryChannels reports.
func configDescribeChannels(t *testing.T, ts *emulator.TestServer) []map[string]any {
	t.Helper()
	resp := configRequest(t, ts, "DescribeDeliveryChannels", map[string]any{})
	var out struct {
		DeliveryChannels []map[string]any `json:"DeliveryChannels"`
	}
	status, code, message := decodeConfigResponse(t, resp, &out)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	return out.DeliveryChannels
}

// configDescribeChannelStatus returns the statuses DescribeDeliveryChannelStatus
// reports.
func configDescribeChannelStatus(t *testing.T, ts *emulator.TestServer) []map[string]any {
	t.Helper()
	resp := configRequest(t, ts, "DescribeDeliveryChannelStatus", map[string]any{})
	var out struct {
		DeliveryChannelsStatus []map[string]any `json:"DeliveryChannelsStatus"`
	}
	status, code, message := decodeConfigResponse(t, resp, &out)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	return out.DeliveryChannelsStatus
}

func TestConfigChannel_RefusesWithoutARecorder(t *testing.T) {
	// "There are no customer managed configuration recorders available to record your
	// resources. Use the PutConfigurationRecorder operation…" — checked before the
	// bucket, per the operation's own ordering, so a consumer with neither is told to
	// fix the recorder first, which is the thing it must fix first.
	ts := emulator.StartTestServer(t)

	status, code, message := configPutChannelRaw(t, ts, map[string]any{
		"name": "default", "s3BucketName": "does-not-exist-either",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NoAvailableConfigurationRecorderException", code)
	assert.Contains(t, message, "PutConfigurationRecorder")
}

func TestConfigChannel_TheBucketCheckReadsRealS3State(t *testing.T) {
	// The whole progression, in the order a consumer hits it: no bucket, then a bucket
	// with no policy, then a bucket with the documented policy. Each step is a distinct
	// refusal, and only the last succeeds.
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")

	status, code, message := configPutChannelRaw(t, ts, map[string]any{
		"name": "default", "s3BucketName": "cfg-logs",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NoSuchBucketException", code)
	assert.Contains(t, message, "does not exist")

	configPutBucket(t, ts, "cfg-logs")
	status, code, message = configPutChannelRaw(t, ts, map[string]any{
		"name": "default", "s3BucketName": "cfg-logs",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InsufficientDeliveryPolicyException", code,
		"a bucket with no policy at all cannot admit Config, and that is unambiguous")
	assert.Contains(t, message, "does not allow Config to write")

	configPutBucketPolicy(t, ts, "cfg-logs", configDeliveryPolicyAdmittingConfig)
	status, code, message = configPutChannelRaw(t, ts, map[string]any{
		"name": "default", "s3BucketName": "cfg-logs",
	})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	channels := configDescribeChannels(t, ts)
	require.Len(t, channels, 1)
	assert.Equal(t, "cfg-logs", channels[0]["s3BucketName"])
}

func TestConfigChannel_ThePolicyMatcherAcceptsWhatAWSAccepts(t *testing.T) {
	// The permissive half of the decision, and the one that matters more: a refusal
	// here would fail a consumer whose policy is perfectly valid. Every case is a form
	// a real bucket policy takes.
	cases := []struct {
		name   string
		policy string
	}{
		{"the documented policy", configDeliveryPolicyAdmittingConfig},
		{
			"a wildcard action",
			`{"Statement":[{"Effect":"Allow","Principal":{"Service":"config.amazonaws.com"},
			  "Action":"s3:*","Resource":"*"}]}`,
		},
		{
			"a prefix wildcard action",
			`{"Statement":[{"Effect":"Allow","Principal":{"Service":"config.amazonaws.com"},
			  "Action":"s3:Put*","Resource":"*"}]}`,
		},
		{
			"a bare wildcard action",
			`{"Statement":[{"Effect":"Allow","Principal":{"Service":"config.amazonaws.com"},
			  "Action":"*","Resource":"*"}]}`,
		},
		{
			"a wildcard principal",
			`{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"*"}]}`,
		},
		{
			"a list of service principals",
			`{"Statement":[{"Effect":"Allow",
			  "Principal":{"Service":["cloudtrail.amazonaws.com","config.amazonaws.com"]},
			  "Action":["s3:GetBucketAcl","s3:PutObject"],"Resource":"*"}]}`,
		},
		{
			"a lowercase effect",
			`{"Statement":[{"Effect":"allow","Principal":{"Service":"config.amazonaws.com"},
			  "Action":"s3:PutObject","Resource":"*"}]}`,
		},
		{
			"a deny statement alongside the allow",
			`{"Statement":[
			  {"Effect":"Deny","Principal":"*","Action":"s3:DeleteObject","Resource":"*"},
			  {"Effect":"Allow","Principal":{"Service":"config.amazonaws.com"},
			   "Action":"s3:PutObject","Resource":"*"}]}`,
		},
		{
			"a resource ARN naming a different bucket",
			// Resource is deliberately not matched: getting the prefix wrong is a real bug,
			// but it is not one this check can tell apart from a valid variant, and guessing
			// would refuse working policies.
			`{"Statement":[{"Effect":"Allow","Principal":{"Service":"config.amazonaws.com"},
			  "Action":"s3:PutObject","Resource":"arn:aws:s3:::some-other-bucket/*"}]}`,
		},
		{
			"a shape substrate's parser cannot decode",
			// IAM grammar permits a single Statement as an object rather than a one-element
			// array, and S3 stores it, but substrate decodes Statement into a slice and so
			// cannot read this one. It passes: refusing here would be substrate blaming the
			// consumer for its own parser's narrowness, which is the failure direction that
			// breaks working code. This is the case the permissive rule exists for.
			`{"Version":"2012-10-17","Statement":{"Effect":"Allow",
			  "Principal":{"Service":"config.amazonaws.com"},
			  "Action":"s3:PutObject","Resource":"*"}}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts := emulator.StartTestServer(t)
			configPutRecorder(t, ts, "default")
			configPutBucket(t, ts, "cfg-logs")
			configPutBucketPolicy(t, ts, "cfg-logs", tc.policy)

			status, code, message := configPutChannelRaw(t, ts, map[string]any{
				"name": "default", "s3BucketName": "cfg-logs",
			})
			assert.Equal(t, http.StatusOK, status, "%s: %s", code, message)
		})
	}
}

func TestConfigChannel_ThePolicyMatcherRefusesWhatAdmitsNothing(t *testing.T) {
	// The other half. A policy that parses and plainly does not let Config write is
	// refused, because accepting it would make the consumer's bucket-policy bug
	// invisible here and fatal at AWS.
	cases := []struct {
		name   string
		policy string
	}{
		{
			"a policy with no statements",
			`{"Version":"2012-10-17","Statement":[]}`,
		},
		{
			"another service's principal",
			`{"Statement":[{"Effect":"Allow","Principal":{"Service":"cloudtrail.amazonaws.com"},
			  "Action":"s3:PutObject","Resource":"*"}]}`,
		},
		{
			"a read-only action",
			`{"Statement":[{"Effect":"Allow","Principal":{"Service":"config.amazonaws.com"},
			  "Action":["s3:GetObject","s3:GetBucketAcl"],"Resource":"*"}]}`,
		},
		{
			"an action for another service",
			`{"Statement":[{"Effect":"Allow","Principal":{"Service":"config.amazonaws.com"},
			  "Action":"sns:Publish","Resource":"*"}]}`,
		},
		{
			"the right pairing under a Deny",
			`{"Statement":[{"Effect":"Deny","Principal":{"Service":"config.amazonaws.com"},
			  "Action":"s3:PutObject","Resource":"*"}]}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts := emulator.StartTestServer(t)
			configPutRecorder(t, ts, "default")
			configPutBucket(t, ts, "cfg-logs")
			configPutBucketPolicy(t, ts, "cfg-logs", tc.policy)

			status, code, _ := configPutChannelRaw(t, ts, map[string]any{
				"name": "default", "s3BucketName": "cfg-logs",
			})
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InsufficientDeliveryPolicyException", code)
		})
	}
}

func TestConfigChannel_TheDeliveryPolicySeedWorksBothWays(t *testing.T) {
	// The seed exists in both directions on purpose: "insufficient" for a consumer with
	// no S3 fixture at all, and "ok" for one whose valid policy the permissive matcher
	// still cannot read. Both override real S3 state, and clearing restores it.
	t.Run("insufficient without any bucket", func(t *testing.T) {
		ts := emulator.StartTestServer(t)
		configPutRecorder(t, ts, "default")
		configSeedDeliveryPolicy(t, ts, "no-such-bucket", "insufficient")

		status, code, _ := configPutChannelRaw(t, ts, map[string]any{
			"name": "default", "s3BucketName": "no-such-bucket",
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InsufficientDeliveryPolicyException", code,
			"the seed outranks the missing bucket, which would otherwise be NoSuchBucket")
	})

	t.Run("ok despite a policy that admits nothing", func(t *testing.T) {
		ts := emulator.StartTestServer(t)
		configPutRecorder(t, ts, "default")
		configPutBucket(t, ts, "cfg-logs")
		configPutBucketPolicy(t, ts, "cfg-logs",
			`{"Statement":[{"Effect":"Deny","Principal":"*","Action":"*","Resource":"*"}]}`)
		configSeedDeliveryPolicy(t, ts, "cfg-logs", "ok")

		status, code, message := configPutChannelRaw(t, ts, map[string]any{
			"name": "default", "s3BucketName": "cfg-logs",
		})
		require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	})

	t.Run("the wildcard bucket applies to any bucket", func(t *testing.T) {
		ts := emulator.StartTestServer(t)
		configPutRecorder(t, ts, "default")
		configSeed(t, ts, "/v1/config/delivery-policy", map[string]any{"outcome": "insufficient"})

		status, code, _ := configPutChannelRaw(t, ts, map[string]any{
			"name": "default", "s3BucketName": "any-bucket-at-all",
		})
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Equal(t, "InsufficientDeliveryPolicyException", code)
	})

	t.Run("clearing restores the computed check", func(t *testing.T) {
		ts := emulator.StartTestServer(t)
		configPutRecorder(t, ts, "default")
		configPutBucket(t, ts, "cfg-logs")
		configPutBucketPolicy(t, ts, "cfg-logs", configDeliveryPolicyAdmittingConfig)
		configSeedDeliveryPolicy(t, ts, "cfg-logs", "insufficient")

		status, _, _ := configPutChannelRaw(t, ts, map[string]any{
			"name": "default", "s3BucketName": "cfg-logs",
		})
		require.Equal(t, http.StatusBadRequest, status)

		configClearSeed(t, ts, "/v1/config/delivery-policy?bucket=cfg-logs")
		status, code, message := configPutChannelRaw(t, ts, map[string]any{
			"name": "default", "s3BucketName": "cfg-logs",
		})
		assert.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	})
}

func TestConfigChannel_TheDeliveryPolicySeedRefusesAnUnknownOutcome(t *testing.T) {
	// A seed value the reader will not recognize would be silently ignored, and the
	// test that set it would pass while asserting nothing.
	ts := emulator.StartTestServer(t)

	status, body := configSeedRaw(t, ts, "/v1/config/delivery-policy",
		map[string]any{"bucket": "cfg-logs", "outcome": "maybe"})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, body, "insufficient")

	status, body = configSeedRaw(t, ts, "/v1/config/delivery-policy",
		map[string]any{"bucket": "cfg-logs"})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, body, "outcome is required")
}

func TestConfigChannel_AChannelWithNoBucketIsAccepted(t *testing.T) {
	// DeliveryS3Bucket is min-length 0 and the operation does not require it, so
	// refusing a channel without one would refuse a request AWS accepts.
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")

	status, code, message := configPutChannelRaw(t, ts, map[string]any{"name": "default"})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	assert.Len(t, configDescribeChannels(t, ts), 1)
}

func TestConfigChannel_PutIsIdempotentAndASecondNameIsRefused(t *testing.T) {
	// One channel per account per Region, same as the recorder, and from the same kind
	// of per-operation note rather than from the limits page.
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")
	configSeedDeliveryPolicy(t, ts, "*", "ok")

	for _, prefix := range []string{"first", "second"} {
		status, code, message := configPutChannelRaw(t, ts, map[string]any{
			"name": "default", "s3BucketName": "cfg-logs", "s3KeyPrefix": prefix,
		})
		require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	}
	channels := configDescribeChannels(t, ts)
	require.Len(t, channels, 1, "a second Put with the same name updates rather than duplicates")
	assert.Equal(t, "second", channels[0]["s3KeyPrefix"], "and the update took effect")

	status, code, message := configPutChannelRaw(t, ts, map[string]any{
		"name": "other", "s3BucketName": "cfg-logs",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "MaxNumberOfDeliveryChannelsExceededException", code)
	assert.Contains(t, message, "reached the limit")
}

func TestConfigChannel_NameDefaultsToDefault(t *testing.T) {
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")

	status, code, message := configPutChannelRaw(t, ts, map[string]any{})
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	channels := configDescribeChannels(t, ts)
	require.Len(t, channels, 1)
	assert.Equal(t, "default", channels[0]["name"])
}

func TestConfigChannel_StatusIsNotApplicableUntilTheRecorderStarts(t *testing.T) {
	// Nothing has been delivered before the first start, and reporting Success would
	// tell a consumer its pipeline works when nothing has gone through it. Note the
	// spelling: the DeliveryStatus enum writes this member Not_Applicable, with an
	// underscore, unlike RecorderStatus's NotApplicable.
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")
	configPutChannel(t, ts, "default", "cfg-logs")

	statuses := configDescribeChannelStatus(t, ts)
	require.Len(t, statuses, 1)
	for _, stream := range []string{
		"configHistoryDeliveryInfo", "configSnapshotDeliveryInfo", "configStreamDeliveryInfo",
	} {
		info, ok := statuses[0][stream].(map[string]any)
		require.True(t, ok, "%s is reported", stream)
		assert.Equal(t, "Not_Applicable", info["lastStatus"])
	}

	configStartRecorder(t, ts, "default")
	statuses = configDescribeChannelStatus(t, ts)
	require.Len(t, statuses, 1)
	for _, stream := range []string{"configHistoryDeliveryInfo", "configSnapshotDeliveryInfo"} {
		info, ok := statuses[0][stream].(map[string]any)
		require.True(t, ok, "%s is reported", stream)
		assert.Equal(t, "Success", info["lastStatus"])
		assert.NotNil(t, info["lastSuccessfulTime"], "%s carries the export shape's time member", stream)
	}
}

func TestConfigChannel_TheThreeStreamsAreNotTheSameShape(t *testing.T) {
	// The two S3 deliveries are ConfigExportDeliveryInfo and the SNS notification is
	// ConfigStreamDeliveryInfo: the first has lastSuccessfulTime/lastAttemptTime and no
	// lastStatusChangeTime, the second the reverse. Emitting one shape for all three
	// would put members in the response an SDK decoding the stream shape cannot read,
	// and this is the case that catches it.
	//
	// The stream status is also independent of the S3 ones: with no SNS topic on the
	// channel it stays Not_Applicable, per the shape's own note that "If the SNS
	// delivery is turned off, the last status will be Not_Applicable" — reporting
	// Success would tell a consumer its alerting works when no topic exists.
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")
	configPutChannel(t, ts, "default", "cfg-logs")
	configStartRecorder(t, ts, "default")

	statuses := configDescribeChannelStatus(t, ts)
	require.Len(t, statuses, 1)

	export, ok := statuses[0]["configSnapshotDeliveryInfo"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Success", export["lastStatus"])
	assert.NotContains(t, export, "lastStatusChangeTime",
		"ConfigExportDeliveryInfo has no lastStatusChangeTime member")
	assert.NotContains(t, export, "nextDeliveryTime",
		"substrate schedules no delivery, so it promises no next one")

	stream, ok := statuses[0]["configStreamDeliveryInfo"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Not_Applicable", stream["lastStatus"], "no SNS topic is configured")
	assert.NotContains(t, stream, "lastSuccessfulTime",
		"ConfigStreamDeliveryInfo has no lastSuccessfulTime member")

	// Give the channel a topic and the stream joins the other two, carrying its own
	// time member rather than theirs.
	configPutChannelWithTopic(t, ts, "default", "cfg-logs",
		"arn:aws:sns:us-east-1:123456789012:config-changes")
	statuses = configDescribeChannelStatus(t, ts)
	require.Len(t, statuses, 1)
	stream, ok = statuses[0]["configStreamDeliveryInfo"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Success", stream["lastStatus"])
	assert.NotNil(t, stream["lastStatusChangeTime"])
	assert.NotContains(t, stream, "lastSuccessfulTime")
}

func TestConfigChannel_TheDeliveryStatusSeedReachesFailure(t *testing.T) {
	// Substrate delivers nothing to S3, so no sequence of API calls can produce a
	// failed delivery — and a consumer's delivery-failure branch is code that exists
	// precisely for it. The seed is the only way in.
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")
	configPutChannel(t, ts, "default", "cfg-logs")
	configStartRecorder(t, ts, "default")

	configSeed(t, ts, "/v1/config/delivery-status", map[string]any{
		"status":           "Failure",
		"lastErrorCode":    "AccessDenied",
		"lastErrorMessage": "Insufficient delivery policy to s3 bucket",
	})

	statuses := configDescribeChannelStatus(t, ts)
	require.Len(t, statuses, 1)
	info, ok := statuses[0]["configHistoryDeliveryInfo"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Failure", info["lastStatus"])
	assert.Equal(t, "AccessDenied", info["lastErrorCode"])
	assert.Equal(t, "Insufficient delivery policy to s3 bucket", info["lastErrorMessage"])

	configClearSeed(t, ts, "/v1/config/delivery-status")
	statuses = configDescribeChannelStatus(t, ts)
	require.Len(t, statuses, 1)
	info, ok = statuses[0]["configHistoryDeliveryInfo"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Success", info["lastStatus"], "clearing the seed restores the real status")
}

func TestConfigChannel_TheDeliveryStatusSeedIsValidated(t *testing.T) {
	// Not_Applicable carries an underscore in this enum and NotApplicable does not
	// exist in it, so the near-miss spelling is refused rather than stored as a value
	// no SDK enum member matches.
	ts := emulator.StartTestServer(t)

	status, body := configSeedRaw(t, ts, "/v1/config/delivery-status",
		map[string]any{"status": "NotApplicable"})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, body, "underscore")

	// An error code on a non-Failure status would be dropped by every consumer that
	// reads it only on failure, so the seed is refused rather than half-applied.
	status, body = configSeedRaw(t, ts, "/v1/config/delivery-status",
		map[string]any{"status": "Success", "lastErrorCode": "AccessDenied"})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, body, "only to status Failure")
}

func TestConfigRecorder_TheStatusSeedReachesFailure(t *testing.T) {
	// The recorder's own lastStatus, for the same reason: substrate delivers nothing,
	// so a recorder it started always reports Success and the Failure branch is
	// otherwise unreachable.
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")
	configPutChannel(t, ts, "default", "cfg-logs")
	configStartRecorder(t, ts, "default")

	configSeed(t, ts, "/v1/config/recorder-status", map[string]any{
		"lastStatus":       "Failure",
		"lastErrorCode":    "InsufficientDeliveryPolicy",
		"lastErrorMessage": "Config cannot write to the bucket",
	})

	statuses := configDescribeRecorderStatus(t, ts)
	require.Len(t, statuses, 1)
	assert.Equal(t, "Failure", statuses[0]["lastStatus"])
	assert.Equal(t, "InsufficientDeliveryPolicy", statuses[0]["lastErrorCode"])
	assert.Equal(t, true, statuses[0]["recording"],
		"the seed pins the delivery status, not whether the recorder is on")

	configClearSeed(t, ts, "/v1/config/recorder-status")
	statuses = configDescribeRecorderStatus(t, ts)
	require.Len(t, statuses, 1)
	assert.Equal(t, "Success", statuses[0]["lastStatus"])
	assert.Empty(t, statuses[0]["lastErrorCode"], "clearing the seed leaves nothing behind")
}

func TestConfigRecorder_TheStatusSeedIsValidated(t *testing.T) {
	ts := emulator.StartTestServer(t)

	status, body := configSeedRaw(t, ts, "/v1/config/recorder-status",
		map[string]any{"lastStatus": "Broken"})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, body, "RecorderStatus")

	status, body = configSeedRaw(t, ts, "/v1/config/recorder-status", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, body, "lastStatus is required")
}

func TestConfigChannel_SeedsAreScopedByAccountAndRegion(t *testing.T) {
	// A seed for one Region must not answer for another, or a multi-Region fixture
	// could not distinguish a healthy Region from a failing one — which is the whole
	// reason to run one.
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")
	configPutChannel(t, ts, "default", "cfg-logs")
	configStartRecorder(t, ts, "default")

	configSeed(t, ts, "/v1/config/recorder-status", map[string]any{
		"region": "eu-west-1", "lastStatus": "Failure",
	})

	statuses := configDescribeRecorderStatus(t, ts)
	require.Len(t, statuses, 1)
	assert.Equal(t, "Success", statuses[0]["lastStatus"],
		"a seed scoped to eu-west-1 does not answer for us-east-1")
}

func TestConfigChannel_DeleteRefusesWhileTheRecorderRuns(t *testing.T) {
	// "Before you can delete the delivery channel, you must stop the customer managed
	// configuration recorder." This is the refusal that justifies the deletes being in
	// scope at all: it is what makes a teardown-and-rebuild fixture — the same test run
	// twice — express an ordering requirement rather than silently succeed on a sequence
	// AWS rejects. Asserted as a sequence rather than as an isolated refusal, because
	// the ordering is the behavior.
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")
	configPutChannel(t, ts, "default", "cfg-logs")
	configStartRecorder(t, ts, "default")

	resp := configRequest(t, ts, "DeleteDeliveryChannel", map[string]any{
		"DeliveryChannelName": "default",
	})
	status, code, message := decodeConfigResponse(t, resp, nil)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "LastDeliveryChannelDeleteFailedException", code)
	assert.Contains(t, message, "is running")
	assert.Len(t, configDescribeChannels(t, ts), 1, "the refusal deleted nothing")

	// Stop, and the same call succeeds.
	resp = configRequest(t, ts, "StopConfigurationRecorder", map[string]any{
		"ConfigurationRecorderName": "default",
	})
	status, code, message = decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)

	resp = configRequest(t, ts, "DeleteDeliveryChannel", map[string]any{
		"DeliveryChannelName": "default",
	})
	status, code, message = decodeConfigResponse(t, resp, nil)
	require.Equal(t, http.StatusOK, status, "%s: %s", code, message)
	assert.Empty(t, configDescribeChannels(t, ts))

	// And the recorder cannot be restarted with the channel gone, which closes the loop:
	// the state after a correct teardown is the state before a correct setup.
	resp = configRequest(t, ts, "StartConfigurationRecorder", map[string]any{
		"ConfigurationRecorderName": "default",
	})
	status, code, _ = decodeConfigResponse(t, resp, nil)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NoAvailableDeliveryChannelException", code)
}

func TestConfigChannel_DeleteRefusesAnUnknownName(t *testing.T) {
	ts := emulator.StartTestServer(t)

	resp := configRequest(t, ts, "DeleteDeliveryChannel", map[string]any{
		"DeliveryChannelName": "ghost",
	})
	status, code, message := decodeConfigResponse(t, resp, nil)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NoSuchDeliveryChannelException", code)
	assert.Contains(t, message, "does not exist")
}

func TestConfigChannel_DescribeRefusesAnUnknownNameAndMoreThanOne(t *testing.T) {
	for _, op := range []string{"DescribeDeliveryChannels", "DescribeDeliveryChannelStatus"} {
		t.Run(op+"/unknown name", func(t *testing.T) {
			ts := emulator.StartTestServer(t)
			resp := configRequest(t, ts, op, map[string]any{"DeliveryChannelNames": []string{"ghost"}})
			status, code, _ := decodeConfigResponse(t, resp, nil)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "NoSuchDeliveryChannelException", code)
		})

		t.Run(op+"/more than one", func(t *testing.T) {
			ts := emulator.StartTestServer(t)
			resp := configRequest(t, ts, op, map[string]any{
				"DeliveryChannelNames": []string{"default", "other"},
			})
			status, code, message := decodeConfigResponse(t, resp, nil)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "ValidationException", code)
			assert.Contains(t, message, "more than one delivery channel")
		})
	}
}

func TestConfigChannel_DescribeIsEmptyBeforeAnyPut(t *testing.T) {
	ts := emulator.StartTestServer(t)

	assert.Empty(t, configDescribeChannels(t, ts))
	assert.Empty(t, configDescribeChannelStatus(t, ts))
}

func TestConfigChannel_RefusesAMissingChannelMember(t *testing.T) {
	ts := emulator.StartTestServer(t)
	configPutRecorder(t, ts, "default")

	resp := configRequest(t, ts, "PutDeliveryChannel", map[string]any{})
	status, code, message := decodeConfigResponse(t, resp, nil)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ValidationException", code)
	assert.Contains(t, message, "DeliveryChannel is required")
}
