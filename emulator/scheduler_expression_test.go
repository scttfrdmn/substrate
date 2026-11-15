package emulator_test

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The three schedule expressions, evaluated as schedule-types.html publishes them (#1501).
func TestSchedulerExpression_Occurrences(t *testing.T) {
	t.Parallel()
	utc := func(y int, m time.Month, d, h, mi int) time.Time { return time.Date(y, m, d, h, mi, 0, 0, time.UTC) }
	ptr := func(v time.Time) *time.Time { return &v }
	armed := utc(2026, 1, 1, 0, 0)

	for _, tc := range []struct {
		name       string
		expr, tz   string
		armed      time.Time
		start, end *time.Time
		n          int
		want       []time.Time
	}{
		{name: "at, UTC", expr: "at(2026-11-16T12:00:00)", armed: armed, n: 3,
			want: []time.Time{utc(2026, 11, 16, 12, 0)}},
		{name: "at, in the published example's zone", expr: "at(2022-11-20T13:00:00)", tz: "America/Los_Angeles",
			armed: armed, n: 1, want: []time.Time{utc(2022, 11, 20, 21, 0)}},
		{name: "at ignores StartDate and EndDate", expr: "at(2026-11-16T12:00:00)", armed: armed,
			start: ptr(utc(2027, 1, 1, 0, 0)), end: ptr(utc(2026, 2, 1, 0, 0)), n: 1,
			want: []time.Time{utc(2026, 11, 16, 12, 0)}},
		{name: "rate with no StartDate starts at once", expr: "rate(5 minutes)", armed: armed, n: 3,
			want: []time.Time{armed, utc(2026, 1, 1, 0, 5), utc(2026, 1, 1, 0, 10)}},
		{name: "rate's singular unit", expr: "rate(1 day)", armed: armed, n: 2,
			want: []time.Time{armed, utc(2026, 1, 2, 0, 0)}},
		{name: "rate from a future StartDate", expr: "rate(1 hour)", armed: armed, start: ptr(utc(2026, 1, 1, 2, 0)), n: 2,
			want: []time.Time{utc(2026, 1, 1, 2, 0), utc(2026, 1, 1, 3, 0)}},
		{name: "rate from a past StartDate keeps its phase", expr: "rate(1 hour)", armed: armed,
			start: ptr(utc(2025, 12, 31, 23, 30)), n: 2,
			want: []time.Time{utc(2026, 1, 1, 0, 30), utc(2026, 1, 1, 1, 30)}},
		{name: "rate stops before EndDate", expr: "rate(1 hour)", armed: armed, end: ptr(utc(2026, 1, 1, 2, 30)), n: 10,
			want: []time.Time{armed, utc(2026, 1, 1, 1, 0), utc(2026, 1, 1, 2, 0)}},
		{name: "rate with EndDate exactly on an occurrence excludes it", expr: "rate(1 hour)", armed: armed,
			end: ptr(utc(2026, 1, 1, 2, 0)), n: 10, want: []time.Time{armed, utc(2026, 1, 1, 1, 0)}},
		{name: "cron, the published last-Friday example", expr: "cron(15 10 ? * 6L 2022-2023)",
			armed: utc(2022, 1, 1, 0, 0), n: 2, want: []time.Time{utc(2022, 1, 28, 10, 15), utc(2022, 2, 25, 10, 15)}},
		{name: "cron in a zone, winter and summer", expr: "cron(30 8 1 1,7 ? *)", tz: "America/New_York",
			armed: armed, n: 2, want: []time.Time{utc(2026, 1, 1, 13, 30), utc(2026, 7, 1, 12, 30)}},
		{name: "cron skips a spring-forward time", expr: "cron(30 2 * * ? *)", tz: "America/Los_Angeles",
			armed: utc(2026, 3, 7, 8, 0), n: 2, want: []time.Time{utc(2026, 3, 7, 10, 30), utc(2026, 3, 9, 9, 30)}},
		{name: "cron runs a fall-back time once, at its second occurrence", expr: "cron(30 1 1 11 ? 2026)",
			tz: "America/Los_Angeles", armed: armed, n: 3, want: []time.Time{utc(2026, 11, 1, 9, 30)}},
		{name: "cron nth weekday", expr: "cron(0 12 ? * 3#2 *)", armed: utc(2026, 12, 1, 0, 0), n: 1,
			want: []time.Time{utc(2026, 12, 8, 12, 0)}},
		{name: "cron nearest weekday", expr: "cron(0 0 15W 8 ? 2026)", armed: armed, n: 1,
			want: []time.Time{utc(2026, 8, 14, 0, 0)}},
		{name: "cron nearest weekday to a Saturday the 1st stays in the month", expr: "cron(0 0 1W 8 ? 2026)", armed: armed, n: 1,
			want: []time.Time{utc(2026, 8, 3, 0, 0)}},
		{name: "cron nearest weekday to a Sunday the last stays in the month", expr: "cron(0 0 31W 5 ? 2026)", armed: armed, n: 1,
			want: []time.Time{utc(2026, 5, 29, 0, 0)}},
		{name: "cron nearest weekday past a short month's end", expr: "cron(0 0 31W 4 ? 2026)", armed: armed, n: 1,
			want: []time.Time{utc(2026, 4, 30, 0, 0)}},
		{name: "cron nearest weekday to a Sunday mid-month is the Monday after", expr: "cron(0 0 2W 8 ? 2026)", armed: armed, n: 1,
			want: []time.Time{utc(2026, 8, 3, 0, 0)}},
		{name: "cron L in day-of-week is Saturday", expr: "cron(0 0 ? 1 L 2026)", armed: armed, n: 1,
			want: []time.Time{utc(2026, 1, 3, 0, 0)}},
		{name: "cron with no occurrence left before the Year field ends", expr: "cron(0 0 1 1 ? 2020)", armed: armed, n: 1,
			want: []time.Time{}},
		{name: "cron last day of month", expr: "cron(0 0 L 2 ? 2028)", armed: armed, n: 1,
			want: []time.Time{utc(2028, 2, 29, 0, 0)}},
		{name: "cron increments and names", expr: "cron(0/20 9 ? JAN MON 2026)", armed: armed, n: 4,
			want: []time.Time{utc(2026, 1, 5, 9, 0), utc(2026, 1, 5, 9, 20), utc(2026, 1, 5, 9, 40), utc(2026, 1, 12, 9, 0)}},
		{name: "cron honors StartDate and EndDate", expr: "cron(0 0 * * ? *)", armed: armed,
			start: ptr(utc(2026, 1, 3, 0, 0)), end: ptr(utc(2026, 1, 5, 0, 0)), n: 10,
			want: []time.Time{utc(2026, 1, 3, 0, 0), utc(2026, 1, 4, 0, 0)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := emulator.SchedOccurrencesForTest(tc.expr, tc.tz, tc.armed, tc.start, tc.end, tc.n)
			require.NoError(t, err)
			require.Len(t, got, len(tc.want))
			for i := range tc.want {
				assert.True(t, tc.want[i].Equal(got[i]), "occurrence %d: want %s, got %s", i, tc.want[i], got[i].UTC())
			}
		})
	}
}

func TestSchedulerExpression_Unevaluable(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ expr, tz string }{
		{"rate(0 minutes)", ""},
		{"rate(5 weeks)", ""},
		{"rate(5)", ""},
		{"at(tomorrow)", ""},
		{"cron(* * * * * *)", ""},
		{"cron(0 0 ? * ? *)", ""},
		{"cron(0 0 * *)", ""},
		{"cron(61 0 * * ? *)", ""},
		{"cron(0 25 * * ? *)", ""},
		{"cron(0 0 32 * ? *)", ""},
		{"cron(0 0 ? 13 * *)", ""},
		{"cron(0 0 ? * 8 *)", ""},
		{"cron(0 0 * * ? 1969)", ""},
		{"cron(0/0 0 * * ? *)", ""},
		{"cron(0 0 ? * FUNDAY *)", ""},
		{"cron(0 0 32W * ? *)", ""},
		{"cron(0 0 ? * 9L *)", ""},
		{"cron(0 0 ? * 9#1 *)", ""},
		{"cron(0 0 ? * 2#6 *)", ""},
		{"cron(0 0 ? * 2#x *)", ""},
		{"cron(5-1 0 * * ? *)", ""},
		{"cron(a-5 0 * * ? *)", ""},
		{"cron(0-a 0 * * ? *)", ""},
		{"every(5 minutes)", ""},
		{"rate(5 minutes)", "Mars/Olympus_Mons"},
	} {
		_, err := emulator.SchedOccurrencesForTest(tc.expr, tc.tz, time.Unix(0, 0), nil, nil, 1)
		assert.Error(t, err, "%s in %q", tc.expr, tc.tz)
	}
}

// A universal target's JSON Input becomes Query form parameters by the generic rule, not by a
// per-operation table (#1501).
func TestSchedulerUniversalTarget_QueryFlattening(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		sdkID, op string
		input     string
		want      map[string]string
	}{
		{
			name: "the lease's updateAutoScalingGroup", sdkID: "autoscaling", op: "UpdateAutoScalingGroup",
			input: `{"AutoScalingGroupName": "aegc-dev-p", "MinSize": 0, "DesiredCapacity": 0}`,
			want:  map[string]string{"AutoScalingGroupName": "aegc-dev-p", "MinSize": "0", "DesiredCapacity": "0"},
		},
		{
			name:  "nested structures, lists of scalars and of structures, booleans, an empty list",
			sdkID: "autoscaling", op: "CreateAutoScalingGroup",
			input: `{"AutoScalingGroupName": "g", "TerminationPolicies": ["OldestInstance", "Default"],
				"Tags": [{"Key": "k", "Value": "v", "PropagateAtLaunch": true}],
				"MixedInstancesPolicy": {"InstancesDistribution": {"OnDemandBaseCapacity": 1.5}},
				"AvailabilityZones": [], "Ignored": null}`,
			want: map[string]string{
				"AutoScalingGroupName":            "g",
				"TerminationPolicies.member.1":    "OldestInstance",
				"TerminationPolicies.member.2":    "Default",
				"Tags.member.1.Key":               "k",
				"Tags.member.1.Value":             "v",
				"Tags.member.1.PropagateAtLaunch": "true",
				"MixedInstancesPolicy.InstancesDistribution.OnDemandBaseCapacity": "1.5",
				"AvailabilityZones": "",
			},
		},
		{
			name: "a renamed list with a named member, from the model", sdkID: "autoscaling", op: "CreateAutoScalingGroup",
			input: `{"MixedInstancesPolicy": {"LaunchTemplate": {"Overrides": [{"InstanceRequirements":
				{"BaselinePerformanceFactors": {"Cpu": {"References": [{"InstanceFamily": "c6i"}]}}}}]}}}`,
			want: map[string]string{
				"MixedInstancesPolicy.LaunchTemplate.Overrides.member.1.InstanceRequirements.BaselinePerformanceFactors.Cpu.Reference.item.1.InstanceFamily": "c6i",
			},
		},
		{
			name: "an SNS map is entry.N.key/value, a list of tags is member.N", sdkID: "sns", op: "CreateTopic",
			input: `{"Name": "t", "Attributes": {"DisplayName": "Lease", "FifoTopic": "false"},
				"Tags": [{"Key": "aegc:managed", "Value": "true"}]}`,
			want: map[string]string{
				"Name":                     "t",
				"Attributes.entry.1.key":   "DisplayName",
				"Attributes.entry.1.value": "Lease",
				"Attributes.entry.2.key":   "FifoTopic",
				"Attributes.entry.2.value": "false",
				"Tags.member.1.Key":        "aegc:managed",
				"Tags.member.1.Value":      "true",
			},
		},
		{
			name: "a map of structures with model-named key and value", sdkID: "sns", op: "Publish",
			input: `{"TopicArn": "arn", "Message": "m", "MessageAttributes": {"kind": {"DataType": "String", "StringValue": "lease"}}}`,
			want: map[string]string{
				"TopicArn": "arn", "Message": "m",
				"MessageAttributes.entry.1.Name":              "kind",
				"MessageAttributes.entry.1.Value.DataType":    "String",
				"MessageAttributes.entry.1.Value.StringValue": "lease",
			},
		},
		{
			name: "SetTopicAttributes takes scalars, so nothing is a map", sdkID: "sns", op: "SetTopicAttributes",
			input: `{"TopicArn": "arn", "AttributeName": "DisplayName", "AttributeValue": "Lease"}`,
			want:  map[string]string{"TopicArn": "arn", "AttributeName": "DisplayName", "AttributeValue": "Lease"},
		},
		{
			name: "the ec2 protocol's list form and member names", sdkID: "ec2", op: "DescribeInstances",
			input: `{"InstanceIds": ["i-1", "i-2"], "DryRun": false, "Filters": [{"Name": "tag:a", "Values": ["b"]}]}`,
			want: map[string]string{"InstanceId.1": "i-1", "InstanceId.2": "i-2", "DryRun": "false",
				"Filter.1.Name": "tag:a", "Filter.1.Value.1": "b"},
		},
		{
			name: "ec2 TagSpecifications", sdkID: "ec2", op: "CreateVolume",
			input: `{"AvailabilityZone": "us-east-1a", "TagSpecifications": [{"ResourceType": "volume", "Tags": [{"Key": "k", "Value": "v"}]}]}`,
			want: map[string]string{"AvailabilityZone": "us-east-1a", "TagSpecification.1.ResourceType": "volume",
				"TagSpecification.1.Tag.1.Key": "k", "TagSpecification.1.Tag.1.Value": "v"},
		},
		{
			name: "an operation the model table does not hold gets the generic rule", sdkID: "sns", op: "NotAnOperation",
			input: `{"Attributes": {"a": "b"}}`,
			want:  map[string]string{"Attributes.a": "b"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := emulator.SchedQueryParamsForTest(tc.input, tc.sdkID, tc.op)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}

	for _, bad := range []string{`not json`, `[1, 2]`, `"a string"`} {
		_, err := emulator.SchedQueryParamsForTest(bad, "sns", "Publish")
		assert.Error(t, err, "%s is not a request object", bad)
	}
}

// A target's answer is read whichever way its plugin reports a failure, and classified as the
// dead-letter guide's retryable/permanent split (#1501).
func TestSchedulerTargetFailure_Classification(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		resp      *emulator.AWSResponse
		err       error
		code, msg string
		retryable bool
		failed    bool
	}{
		{name: "a 2xx answer succeeded", resp: &emulator.AWSResponse{StatusCode: http.StatusAccepted}},
		{name: "no answer and no error succeeded"},
		{name: "an AWSError", err: &emulator.AWSError{Code: "ParameterAlreadyExists", Message: "exists", HTTPStatus: 400},
			code: "ParameterAlreadyExists", msg: "exists", failed: true},
		{name: "a throttle is retryable", err: &emulator.AWSError{Code: "ThrottlingException", HTTPStatus: 400},
			code: "ThrottlingException", retryable: true, failed: true},
		{name: "a wrapped AWSError keeps its code and retryability", err: fmt.Errorf("dispatch: %w",
			&emulator.AWSError{Code: "ThrottlingException", Message: "slow down", HTTPStatus: 400}),
			code: "ThrottlingException", msg: "slow down", retryable: true, failed: true},
		{name: "an error that is not an AWSError", err: errors.New("boom"),
			code: "AWS.Scheduler.InternalServerError", msg: "Unexpected error occurred while processing the request", retryable: true, failed: true},
		{name: "a JSON body with a qualified __type", resp: &emulator.AWSResponse{StatusCode: 404,
			Body: []byte(`{"__type":"com.amazonaws.lambda#ResourceNotFoundException","message":"Function not found"}`)},
			code: "ResourceNotFoundException", msg: "Function not found", failed: true},
		{name: "an XML body", resp: &emulator.AWSResponse{StatusCode: 503,
			Body: []byte(`<ErrorResponse><Error><Code>ServiceUnavailable</Code><Message>later</Message></Error></ErrorResponse>`)},
			code: "ServiceUnavailable", msg: "later", retryable: true, failed: true},
		{name: "a code only in the error-type header", resp: &emulator.AWSResponse{StatusCode: 429,
			Headers: map[string]string{"X-Amzn-Errortype": "TooManyRequestsException:http://internal"}, Body: []byte(`{}`)},
			code: "TooManyRequestsException", retryable: true, failed: true},
		{name: "no code at all", resp: &emulator.AWSResponse{StatusCode: 403, Body: []byte(`{}`)}, code: "Forbidden", failed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			code, msg, retryable, failed := emulator.SchedTargetFailureForTest(tc.resp, tc.err)
			assert.Equal(t, tc.failed, failed)
			assert.Equal(t, tc.code, code)
			assert.Equal(t, tc.msg, msg)
			assert.Equal(t, tc.retryable, retryable)
		})
	}
}

// A universal lambda:invoke keeps a qualified FunctionName's version or alias as the Qualifier,
// and refuses one that contradicts an explicit Qualifier as Lambda does (#1501).
func TestSchedulerUniversalTarget_LambdaInvokeQualifier(t *testing.T) {
	t.Parallel()
	const fn = "arn:aws:lambda:us-west-2:210987654321:function:HelloWorld"
	for _, tc := range []struct {
		name, input          string
		path, qualifier, err string
	}{
		{name: "an unqualified ARN", input: `{"FunctionName":"` + fn + `"}`,
			path: "/2015-03-31/functions/HelloWorld/invocations"},
		{name: "an alias in the ARN", input: `{"FunctionName":"` + fn + `:live"}`,
			path: "/2015-03-31/functions/HelloWorld/invocations", qualifier: "live"},
		{name: "a version in a bare name", input: `{"FunctionName":"HelloWorld:2"}`,
			path: "/2015-03-31/functions/HelloWorld/invocations", qualifier: "2"},
		{name: "an explicit Qualifier agreeing", input: `{"FunctionName":"` + fn + `:live","Qualifier":"live"}`,
			path: "/2015-03-31/functions/HelloWorld/invocations", qualifier: "live"},
		{name: "an explicit Qualifier alone", input: `{"FunctionName":"HelloWorld","Qualifier":"3"}`,
			path: "/2015-03-31/functions/HelloWorld/invocations", qualifier: "3"},
		{name: "a contradicting Qualifier", input: `{"FunctionName":"MyFunction:2","Qualifier":"1"}`,
			err: "InvalidParameterValueException"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req, code, intent := emulator.SchedTargetRequestForTest("arn:aws:scheduler:::aws-sdk:lambda:invoke", tc.input)
			assert.Empty(t, intent)
			assert.Equal(t, tc.err, code)
			if tc.err != "" {
				return
			}
			require.NotNil(t, req)
			assert.Equal(t, tc.path, req.Path)
			assert.Equal(t, tc.qualifier, req.Params["Qualifier"])
			assert.Equal(t, "Event", req.Headers["X-Amz-Invocation-Type"])
		})
	}
}
