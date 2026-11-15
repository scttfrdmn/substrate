package emulator_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A schedule invokes its target on the simulated clock (#1501), and a finished one with
// ActionAfterCompletion DELETE deletes itself (#1475). Every test freezes the clock and moves it by
// hand, so none depends on real elapsed time.

// schedClockInstant is the frozen instant each test starts at: an hour before the lease expires.
var schedClockInstant = time.Date(2026, 11, 16, 11, 0, 0, 0, time.UTC)

const (
	schedLeaseExpiry = "at(2026-11-16T12:00:00)"
	schedRoleARN     = "arn:aws:iam::123456789012:role/aegc/aegc-dev-scheduler-us-west-2"
)

type schedEnv struct {
	t  *testing.T
	ts *emulator.TestServer
}

func newSchedEnv(t *testing.T, opts ...emulator.TestServerOption) *schedEnv {
	t.Helper()
	ts := emulator.StartTestServer(t, opts...)
	ts.FreezeTimeAt(schedClockInstant)
	return &schedEnv{t: t, ts: ts}
}

// do sends one request and returns its status and body.
func (e *schedEnv) do(method, host, path, contentType, target string, body []byte) (int, []byte) {
	e.t.Helper()
	req, err := http.NewRequestWithContext(e.t.Context(), method, e.ts.URL+path, bytes.NewReader(body))
	require.NoError(e.t, err)
	req.Host = host
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if target != "" {
		req.Header.Set("X-Amz-Target", target)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(e.t, err)
	defer resp.Body.Close() //nolint:errcheck
	out, err := io.ReadAll(resp.Body)
	require.NoError(e.t, err)
	return resp.StatusCode, out
}

func (e *schedEnv) sched(method, path string, body any) (int, []byte) {
	e.t.Helper()
	var b []byte
	if body != nil {
		var err error
		b, err = json.Marshal(body)
		require.NoError(e.t, err)
	}
	return e.do(method, "scheduler.us-east-1.amazonaws.com", path, "application/json", "", b)
}

func (e *schedEnv) mustSched(method, path string, body any) []byte {
	e.t.Helper()
	status, out := e.sched(method, path, body)
	require.Equal(e.t, http.StatusOK, status, "%s %s: %s", method, path, out)
	return out
}

func (e *schedEnv) json(host, target string, body any) (int, []byte) {
	e.t.Helper()
	b, err := json.Marshal(body)
	require.NoError(e.t, err)
	return e.do(http.MethodPost, host, "/", "application/x-amz-json-1.1", target, b)
}

func (e *schedEnv) query(host string, params map[string]string) (int, []byte) {
	e.t.Helper()
	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	return e.do(http.MethodPost, host, "/", "application/x-www-form-urlencoded", "", []byte(form.Encode()))
}

// parameter reads an SSM parameter's value, and whether it exists.
func (e *schedEnv) parameter(name string) (string, bool) {
	e.t.Helper()
	status, out := e.json("ssm.us-east-1.amazonaws.com", "AmazonSSM.GetParameter", map[string]any{"Name": name})
	if status >= http.StatusBadRequest && strings.Contains(string(out), "ParameterNotFound") {
		return "", false
	}
	require.Equal(e.t, http.StatusOK, status, "GetParameter: %s", out)
	var got struct {
		Parameter struct{ Value string } `json:"Parameter"`
	}
	require.NoError(e.t, json.Unmarshal(out, &got))
	return got.Parameter.Value, true
}

// createQueue creates an SQS queue and returns its URL and ARN.
func (e *schedEnv) createQueue(name string) (string, string) {
	e.t.Helper()
	status, out := e.json("sqs.us-east-1.amazonaws.com", "AmazonSQS.CreateQueue", map[string]any{"QueueName": name})
	require.Equal(e.t, http.StatusOK, status, "CreateQueue: %s", out)
	var got struct{ QueueUrl string } //nolint:revive // the wire member's spelling
	require.NoError(e.t, json.Unmarshal(out, &got))
	return got.QueueUrl, "arn:aws:sqs:us-east-1:123456789012:" + name
}

type schedDLQMessage struct {
	Body              string
	MessageAttributes map[string]struct{ StringValue string }
}

// receive reads every visible message on a queue, with its attributes, leaving each in place.
func (e *schedEnv) receive(queueURL string) []schedDLQMessage {
	e.t.Helper()
	status, out := e.json("sqs.us-east-1.amazonaws.com", "AmazonSQS.ReceiveMessage", map[string]any{
		"QueueUrl": queueURL, "MaxNumberOfMessages": 10, "VisibilityTimeout": 0,
		"MessageAttributeNames": []string{"All"},
	})
	require.Equal(e.t, http.StatusOK, status, "ReceiveMessage: %s", out)
	var got struct{ Messages []schedDLQMessage }
	require.NoError(e.t, json.Unmarshal(out, &got))
	return got.Messages
}

// clockDispatches lists the service:operation of each recorded clock-driven dispatch.
func (e *schedEnv) clockDispatches() []string {
	e.t.Helper()
	events, err := e.ts.Store().GetStream(e.t.Context(), "default")
	require.NoError(e.t, err)
	var ops []string
	for _, ev := range events {
		if strings.HasPrefix(ev.RequestID, "req-clock-") {
			ops = append(ops, ev.Service+":"+ev.Operation)
		}
	}
	return ops
}

// leaseSchedule is the consumer's lease-state schedule, with overrides.
func leaseSchedule(overrides map[string]any) map[string]any {
	s := map[string]any{
		"GroupName":             "aegc-dev",
		"ScheduleExpression":    schedLeaseExpiry,
		"FlexibleTimeWindow":    map[string]any{"Mode": "OFF"},
		"ActionAfterCompletion": "NONE",
		"Target": map[string]any{
			"Arn":     "arn:aws:scheduler:::aws-sdk:ssm:putParameter",
			"RoleArn": schedRoleARN,
			"Input":   `{"Name": "/aegc/dev/state", "Value": "{\"mode\": \"expired\"}", "Type": "String", "Overwrite": true}`,
		},
	}
	for k, v := range overrides {
		s[k] = v
	}
	return s
}

func (e *schedEnv) createLeaseGroup() {
	e.t.Helper()
	e.mustSched(http.MethodPost, "/schedule-groups/aegc-dev", map[string]any{
		"Tags": []map[string]string{{"Key": "aegc:managed", "Value": "true"}},
	})
}

func TestSchedulerClock_TheLeaseExpiresOnceWhenTheClockPassesIt(t *testing.T) {
	t.Parallel()
	e := newSchedEnv(t)
	e.createLeaseGroup()
	e.mustSched(http.MethodPost, "/schedules/lease-state", leaseSchedule(nil))

	_, found := e.parameter("/aegc/dev/state")
	require.False(t, found, "before the instant nothing is written")

	e.ts.AdvanceTime(59*time.Minute + 59*time.Second)
	_, found = e.parameter("/aegc/dev/state")
	require.False(t, found, "one second short of the instant nothing is due")

	e.ts.AdvanceTime(time.Second)
	value, found := e.parameter("/aegc/dev/state")
	require.True(t, found, "the putParameter universal target ran before the observing request")
	assert.JSONEq(t, `{"mode": "expired"}`, value)

	e.ts.AdvanceTime(24 * time.Hour)
	_, _ = e.parameter("/aegc/dev/state")
	assert.Equal(t, []string{"ssm:PutParameter"}, e.clockDispatches(), "an at() schedule fires exactly once")

	got := e.mustSched(http.MethodGet, "/schedules/lease-state?groupName=aegc-dev", nil)
	assert.Contains(t, string(got), `"State":"ENABLED"`, "ActionAfterCompletion NONE keeps the schedule, State unchanged")
}

func TestSchedulerClock_UpdateAutoScalingGroupUniversalTarget(t *testing.T) {
	t.Parallel()
	a := newASGEnv(t)
	a.createGroup("aegc-dev-p", 2, 4, 2, nil)
	e := &schedEnv{t: t, ts: a.ts}
	e.createLeaseGroup()
	at := a.ts.TimeController().Now().Add(time.Hour).UTC().Format("2006-01-02T15:04:05")
	e.mustSched(http.MethodPost, "/schedules/lease-pool-p", leaseSchedule(map[string]any{
		"ScheduleExpression": "at(" + at + ")",
		"Target": map[string]any{
			"Arn":     "arn:aws:scheduler:::aws-sdk:autoscaling:updateAutoScalingGroup",
			"RoleArn": schedRoleARN,
			"Input":   `{"AutoScalingGroupName": "aegc-dev-p", "MinSize": 0, "DesiredCapacity": 0}`,
		},
	}))
	require.Equal(t, 2, a.group("aegc-dev-p").Desired)

	a.ts.AdvanceTime(time.Hour)
	g := a.group("aegc-dev-p")
	assert.Equal(t, 0, g.Min, "MinSize came from the Input, converted to Query parameters")
	assert.Equal(t, 0, g.Desired)
	assert.Equal(t, 4, g.Max, "a member the Input does not send is unchanged")
}

func TestSchedulerClock_ActionAfterCompletion(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		action   any
		expr     string
		end      any
		advance  time.Duration
		deleted  bool
		dispatch int
	}{
		{name: "at() with DELETE is deleted after its invocation", action: "DELETE", expr: schedLeaseExpiry,
			advance: time.Hour, deleted: true, dispatch: 1},
		{name: "at() with NONE stays", action: "NONE", expr: schedLeaseExpiry, advance: time.Hour, dispatch: 1},
		{name: "at() with none named stays", action: nil, expr: schedLeaseExpiry, advance: time.Hour, dispatch: 1},
		{name: "rate() with DELETE is deleted after the invocation closest to EndDate", action: "DELETE",
			expr: "rate(20 minutes)", end: float64(schedClockInstant.Add(time.Hour).Unix()), advance: 2 * time.Hour,
			deleted: true, dispatch: 3},
		{name: "rate() with DELETE and no EndDate is never deleted", action: "DELETE", expr: "rate(30 minutes)",
			advance: 2 * time.Hour, dispatch: 5},
		{name: "cron() with DELETE is deleted after its last occurrence", action: "DELETE",
			expr: "cron(0 * * * ? *)", end: float64(schedClockInstant.Add(150 * time.Minute).Unix()),
			advance: 4 * time.Hour, deleted: true, dispatch: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newSchedEnv(t)
			e.createLeaseGroup()
			body := leaseSchedule(map[string]any{"ScheduleExpression": tc.expr, "ActionAfterCompletion": tc.action})
			if tc.action == nil {
				delete(body, "ActionAfterCompletion")
			}
			if tc.end != nil {
				body["EndDate"] = tc.end
			}
			e.mustSched(http.MethodPost, "/schedules/lease-state", body)

			e.mustSched(http.MethodGet, "/schedules/lease-state?groupName=aegc-dev", nil)
			e.ts.AdvanceTime(tc.advance)
			status, out := e.sched(http.MethodGet, "/schedules/lease-state?groupName=aegc-dev", nil)
			list := e.mustSched(http.MethodGet, "/schedules?ScheduleGroup=aegc-dev", nil)
			if tc.deleted {
				assert.Equal(t, http.StatusNotFound, status, "%s", out)
				assert.Contains(t, string(out), "ResourceNotFoundException")
				assert.NotContains(t, string(list), "lease-state", "ListSchedules drops it")
			} else {
				assert.Equal(t, http.StatusOK, status, "%s", out)
				assert.Contains(t, string(list), "lease-state")
			}
			assert.Len(t, e.clockDispatches(), tc.dispatch)
		})
	}
}

func TestSchedulerClock_ADisabledScheduleFiresNothingUntilReEnabled(t *testing.T) {
	t.Parallel()
	e := newSchedEnv(t)
	e.createLeaseGroup()
	e.mustSched(http.MethodPost, "/schedules/lease-state", leaseSchedule(map[string]any{"State": "DISABLED"}))
	e.ts.AdvanceTime(2 * time.Hour)
	_, found := e.parameter("/aegc/dev/state")
	require.False(t, found, "a disabled schedule does not fire")

	// Re-enabling a one-time schedule after its instant fires it at once (troubleshooting.html).
	e.mustSched(http.MethodPut, "/schedules/lease-state", leaseSchedule(nil))
	_, found = e.parameter("/aegc/dev/state")
	require.True(t, found)

	// An update that keeps the expression does not fire it again.
	e.mustSched(http.MethodPut, "/schedules/lease-state", leaseSchedule(map[string]any{"Description": "renewed"}))
	_, _ = e.parameter("/aegc/dev/state")
	assert.Equal(t, []string{"ssm:PutParameter"}, e.clockDispatches())
}

func TestSchedulerClock_DeletedSchedulesAndGroupsDoNotFire(t *testing.T) {
	t.Parallel()
	e := newSchedEnv(t)
	e.createLeaseGroup()
	e.mustSched(http.MethodPost, "/schedules/lease-state", leaseSchedule(nil))
	e.mustSched(http.MethodPost, "/schedules/other", leaseSchedule(map[string]any{"GroupName": nil}))
	e.mustSched(http.MethodDelete, "/schedule-groups/aegc-dev", nil)
	e.mustSched(http.MethodDelete, "/schedules/other", nil)
	e.ts.AdvanceTime(2 * time.Hour)
	_, found := e.parameter("/aegc/dev/state")
	assert.False(t, found)
	assert.Empty(t, e.clockDispatches())
}

func TestSchedulerClock_RetriesThenTheDeadLetterQueue(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		seed       map[string]any
		retry      map[string]any
		steps      []time.Duration
		wantFound  bool
		condition  string // "" asserts the attribute absent
		code       string
		retries    string
		dlqLetters int
	}{
		{
			name:  "a retryable error retries MaximumRetryAttempts times, then dead-letters",
			seed:  map[string]any{"scheduleName": "lease-state", "groupName": "aegc-dev", "errorCode": "ThrottlingException", "errorMessage": "Rate exceeded"},
			retry: map[string]any{"MaximumRetryAttempts": 2, "MaximumEventAgeInSeconds": 3600},
			// The first attempt at the instant, retries 60 s and then 120 s later.
			steps:     []time.Duration{time.Hour, 60 * time.Second, 120 * time.Second},
			condition: "MaximumRetryAttempts", code: "ThrottlingException", retries: "2", dlqLetters: 1,
		},
		{
			name:  "a retryable error that outlives MaximumEventAgeInSeconds dead-letters on age",
			seed:  map[string]any{"scheduleName": "*", "errorCode": "InternalServerException", "retryable": true},
			retry: map[string]any{"MaximumRetryAttempts": 10, "MaximumEventAgeInSeconds": 120},
			// The retry at 60 s fails, and the next would be at 180 s, past the 120 s age.
			steps:     []time.Duration{time.Hour, 60 * time.Second},
			condition: "MaximumEventAgeInSeconds", code: "InternalServerException", retries: "1", dlqLetters: 1,
		},
		{
			name:  "a permanent error is not retried",
			seed:  map[string]any{"scheduleName": "lease-state", "errorCode": "ValidationException", "errorMessage": "bad input"},
			retry: map[string]any{"MaximumRetryAttempts": 5, "MaximumEventAgeInSeconds": 3600},
			steps: []time.Duration{time.Hour}, code: "ValidationException", retries: "0", dlqLetters: 1,
		},
		{
			name:      "failAttempts bounds the failures; the next attempt calls the target",
			seed:      map[string]any{"scheduleName": "lease-state", "errorCode": "ThrottlingException", "failAttempts": 1},
			retry:     map[string]any{"MaximumRetryAttempts": 3, "MaximumEventAgeInSeconds": 3600},
			steps:     []time.Duration{time.Hour, 60 * time.Second},
			wantFound: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newSchedEnv(t)
			e.createLeaseGroup()
			queueURL, queueARN := e.createQueue("lease-dlq")
			seed, err := json.Marshal(tc.seed)
			require.NoError(t, err)
			status, out := e.do(http.MethodPost, "localhost", "/v1/scheduler/invocation-outcome", "application/json", "", seed)
			require.Equal(t, http.StatusOK, status, "%s", out)

			target := leaseSchedule(nil)["Target"].(map[string]any)
			target["RetryPolicy"] = tc.retry
			target["DeadLetterConfig"] = map[string]any{"Arn": queueARN}
			e.mustSched(http.MethodPost, "/schedules/lease-state", leaseSchedule(map[string]any{"Target": target}))

			for _, step := range tc.steps[:len(tc.steps)-1] {
				e.ts.AdvanceTime(step)
				_, _ = e.parameter("/aegc/dev/state")
				assert.Empty(t, e.receive(queueURL), "nothing is dead-lettered while retries remain")
			}
			e.ts.AdvanceTime(tc.steps[len(tc.steps)-1])
			_, found := e.parameter("/aegc/dev/state")
			assert.Equal(t, tc.wantFound, found)

			letters := e.receive(queueURL)
			require.Len(t, letters, tc.dlqLetters)
			if tc.dlqLetters == 0 {
				return
			}
			m := letters[0]
			assert.JSONEq(t, leaseSchedule(nil)["Target"].(map[string]any)["Input"].(string), m.Body, "the body is the Input")
			attr := func(k string) string { return m.MessageAttributes[k].StringValue }
			assert.Equal(t, tc.code, attr("ERROR_CODE"))
			assert.Equal(t, tc.retries, attr("RETRY_ATTEMPTS"))
			assert.Equal(t, "arn:aws:scheduler:us-east-1:123456789012:schedule/aegc-dev/lease-state", attr("SCHEDULE_ARN"))
			assert.Equal(t, "arn:aws:scheduler:::aws-sdk:ssm:putParameter", attr("TARGET_ARN"))
			assert.Equal(t, "2026-11-16T12:00:00Z", attr("SCHEDULED_TIME"))
			assert.Equal(t, "false", attr("IS_PAYLOAD_TRUNCATED"))
			assert.Len(t, attr("EXECUTION_ID"), 16)
			if tc.condition == "" {
				assert.NotContains(t, m.MessageAttributes, "EXHAUSTED_RETRY_CONDITION",
					"present only for a retryable error (configuring-schedule-dlq.html)")
			} else {
				assert.Equal(t, tc.condition, attr("EXHAUSTED_RETRY_CONDITION"))
			}
		})
	}
}

func TestSchedulerClock_TheTargetRunsAsItsRole(t *testing.T) {
	t.Parallel()
	e := newSchedEnv(t)
	iam := func(params map[string]string) {
		t.Helper()
		params["Version"] = "2010-05-08"
		status, out := e.query("iam.amazonaws.com", params)
		require.Equal(t, http.StatusOK, status, "%s: %s", params["Action"], out)
	}
	iam(map[string]string{
		"Action": "CreateRole", "RoleName": "aegc-dev-scheduler-us-west-2", "Path": "/aegc/",
		"AssumeRolePolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]}`,
	})
	allow := func(actions ...string) {
		doc, err := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": []map[string]any{
			{"Effect": "Allow", "Action": actions, "Resource": "*"},
		}})
		require.NoError(t, err)
		iam(map[string]string{"Action": "PutRolePolicy", "RoleName": "aegc-dev-scheduler-us-west-2",
			"PolicyName": "scheduler", "PolicyDocument": string(doc)})
	}
	allow("sqs:SendMessage")

	e.createLeaseGroup()
	queueURL, queueARN := e.createQueue("lease-dlq")
	target := leaseSchedule(nil)["Target"].(map[string]any)
	target["DeadLetterConfig"] = map[string]any{"Arn": queueARN}
	e.mustSched(http.MethodPost, "/schedules/lease-state", leaseSchedule(map[string]any{"Target": target}))

	e.ts.AdvanceTime(time.Hour)
	_, found := e.parameter("/aegc/dev/state")
	require.False(t, found, "the role's policy does not grant ssm:PutParameter")
	letters := e.receive(queueURL)
	require.Len(t, letters, 1)
	assert.Equal(t, "AccessDeniedException", letters[0].MessageAttributes["ERROR_CODE"].StringValue)
	assert.NotContains(t, letters[0].MessageAttributes, "EXHAUSTED_RETRY_CONDITION", "a denial is permanent")

	allow("sqs:SendMessage", "ssm:PutParameter")
	e.mustSched(http.MethodPut, "/schedules/lease-state", leaseSchedule(map[string]any{
		"ScheduleExpression": "at(2026-11-16T12:30:00)", "Target": target,
	}))
	e.ts.AdvanceTime(30 * time.Minute)
	_, found = e.parameter("/aegc/dev/state")
	assert.True(t, found, "granted, the same target succeeds")
}

func TestSchedulerClock_TemplatedTargets(t *testing.T) {
	t.Parallel()
	e := newSchedEnv(t)
	queueURL, queueARN := e.createQueue("templated")
	e.mustSched(http.MethodPost, "/schedules/to-sqs", map[string]any{
		"ScheduleExpression": schedLeaseExpiry,
		"FlexibleTimeWindow": map[string]any{"Mode": "FLEXIBLE", "MaximumWindowInMinutes": 15},
		"Target": map[string]any{
			"Arn": queueARN, "RoleArn": schedRoleARN,
			"Input": "scheduled for <aws.scheduler.scheduled-time> by <aws.scheduler.schedule-arn>, attempt <aws.scheduler.attempt-number>",
		},
	})
	e.ts.AdvanceTime(time.Hour)
	msgs := e.receive(queueURL)
	require.Len(t, msgs, 1, "a FLEXIBLE window fires at its start")
	assert.Equal(t, "scheduled for 2026-11-16T12:00:00Z by arn:aws:scheduler:us-east-1:123456789012:schedule/default/to-sqs, attempt 1",
		msgs[0].Body, "the context attributes are replaced")
	assert.Equal(t, []string{"sqs:SendMessage"}, e.clockDispatches())
}

// Each target kind reaches the operation its page names, in that service's protocol, or is recorded
// as intent and dispatches nothing.
func TestSchedulerClock_EachTargetKindDispatchesItsOperation(t *testing.T) {
	t.Parallel()
	const fn = "arn:aws:lambda:us-east-1:123456789012:function:fn"
	for _, tc := range []struct {
		name   string
		target map[string]any
		want   []string
	}{
		{"templated Lambda", map[string]any{"Arn": fn}, []string{"lambda:Invoke"}},
		{"templated SNS", map[string]any{"Arn": "arn:aws:sns:us-east-1:123456789012:topic"}, []string{"sns:Publish"}},
		{"templated Step Functions", map[string]any{"Arn": "arn:aws:states:us-east-1:123456789012:stateMachine:sm", "Input": "{}"},
			[]string{"states:StartExecution"}},
		{"templated EventBridge", map[string]any{"Arn": "arn:aws:events:us-east-1:123456789012:event-bus/default", "Input": "{}",
			"EventBridgeParameters": map[string]any{"DetailType": "Lease", "Source": "aegc"}}, []string{"eventbridge:PutEvents"}},
		{"universal JSON", map[string]any{"Arn": "arn:aws:scheduler:::aws-sdk:sfn:startExecution",
			"Input": `{"stateMachineArn":"arn:aws:states:us-east-1:123456789012:stateMachine:sm"}`}, []string{"states:StartExecution"}},
		{"universal Lambda invoke", map[string]any{"Arn": "arn:aws:scheduler:::aws-sdk:lambda:invoke",
			"Input": `{"FunctionName":"` + fn + `","InvocationType":"Event","Payload":"{\"message\":\"testing\"}"}`}, []string{"lambda:Invoke"}},
		{"universal Query", map[string]any{"Arn": "arn:aws:scheduler:::aws-sdk:sts:getCallerIdentity"},
			[]string{"sts:GetCallerIdentity"}},
		{"templated Kinesis is intent", map[string]any{"Arn": "arn:aws:kinesis:us-east-1:123456789012:stream/s"}, nil},
		{"a universal REST action other than Lambda invoke is intent", map[string]any{"Arn": "arn:aws:scheduler:::aws-sdk:s3:putObject",
			"Input": `{"Bucket":"b","Key":"k"}`}, nil},
		{"a universal service substrate does not emulate is intent", map[string]any{"Arn": "arn:aws:scheduler:::aws-sdk:glacier:createVault"}, nil},
		{"a universal Query Input that is not an object fails before dispatch", map[string]any{
			"Arn": "arn:aws:scheduler:::aws-sdk:autoscaling:updateAutoScalingGroup", "Input": `["not", "a", "request"]`}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newSchedEnv(t)
			tc.target["RoleArn"] = schedRoleARN
			e.mustSched(http.MethodPost, "/schedules/target", map[string]any{
				"ScheduleExpression": schedLeaseExpiry,
				"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
				"Target":             tc.target,
			})
			e.ts.AdvanceTime(time.Hour)
			e.mustSched(http.MethodGet, "/schedules/target", nil)
			assert.Equal(t, tc.want, e.clockDispatches())
		})
	}
}

func TestSchedulerClock_ARateScheduleCatchesUpAfterAGap(t *testing.T) {
	t.Parallel()
	e := newSchedEnv(t)
	queueURL, queueARN := e.createQueue("rate")
	e.mustSched(http.MethodPost, "/schedules/every-ten", map[string]any{
		"ScheduleExpression": "rate(10 minutes)",
		"StartDate":          float64(schedClockInstant.Add(10 * time.Minute).Unix()),
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
		"Target":             map[string]any{"Arn": queueARN, "RoleArn": schedRoleARN, "Input": "<aws.scheduler.scheduled-time>"},
	})
	require.Empty(t, e.receive(queueURL), "the first occurrence is at StartDate")
	e.ts.AdvanceTime(35 * time.Minute)
	var bodies []string
	for _, m := range e.receive(queueURL) {
		bodies = append(bodies, m.Body)
	}
	assert.ElementsMatch(t, []string{"2026-11-16T11:10:00Z", "2026-11-16T11:20:00Z", "2026-11-16T11:30:00Z"}, bodies,
		"every occurrence in the gap fires, each with its own scheduled time")
}

// A recorded session replays identically: the clock-driven dispatches are skipped and re-derived
// from the requests that triggered them, so the state they left matches.
func TestSchedulerClock_AReplayReproducesTheSchedulesEffects(t *testing.T) {
	t.Parallel()
	e := newSchedEnv(t, emulator.WithRecordedBodies(), emulator.WithRecordedStateHashes())
	e.createLeaseGroup()
	queueURL, queueARN := e.createQueue("lease-dlq")
	e.mustSched(http.MethodPost, "/schedules/lease-state", leaseSchedule(nil))
	failing := leaseSchedule(nil)["Target"].(map[string]any)
	failing["DeadLetterConfig"] = map[string]any{"Arn": queueARN}
	failing["RetryPolicy"] = map[string]any{"MaximumRetryAttempts": 1, "MaximumEventAgeInSeconds": 600}
	e.mustSched(http.MethodPost, "/schedules/lease-fail", leaseSchedule(map[string]any{
		"Target": failing, "ActionAfterCompletion": "DELETE",
	}))
	seed := []byte(`{"scheduleName":"lease-fail","errorCode":"ThrottlingException"}`)
	status, out := e.do(http.MethodPost, "localhost", "/v1/scheduler/invocation-outcome", "application/json", "", seed)
	require.Equal(t, http.StatusOK, status, "%s", out)

	e.ts.AdvanceTime(time.Hour)
	_, found := e.parameter("/aegc/dev/state")
	require.True(t, found)
	e.ts.AdvanceTime(time.Minute)
	require.Len(t, e.receive(queueURL), 1)
	status, _ = e.sched(http.MethodGet, "/schedules/lease-fail?groupName=aegc-dev", nil)
	require.Equal(t, http.StatusNotFound, status, "the DELETE schedule went once its retries ended")

	dispatched := len(e.clockDispatches())
	require.Equal(t, 2, dispatched, "the PutParameter and the dead-letter SendMessage")
	// The control-plane handler re-applies the seed in position, as a replay of a live server's
	// stream does (#1140); without it lease-fail would succeed on replay.
	engine := emulator.NewReplayEngine(e.ts.Store(), e.ts.StateManager(), e.ts.TimeController(), e.ts.Registry(),
		emulator.ReplayConfig{ValidateState: true}, emulator.NewDefaultLogger(slog.LevelError, false),
		emulator.WithControlPlaneHandler(e.ts.ControlPlaneHandler()))
	results, err := engine.Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	assert.Empty(t, results.Differences, "%s", replayDifferenceSummary(results))
	assert.True(t, results.StateValid)
	assert.Equal(t, dispatched, results.SkippedEvents, "only the recorded clock-driven dispatches are skipped")
}

func TestSchedulerClock_SeedEndpointRefusals(t *testing.T) {
	t.Parallel()
	e := newSchedEnv(t)
	for _, body := range []string{`not json`, `{"errorCode":"X"}`, `{"scheduleName":"s"}`, `{"scheduleName":"s","errorCode":"X","failAttempts":-1}`} {
		status, out := e.do(http.MethodPost, "localhost", "/v1/scheduler/invocation-outcome", "application/json", "", []byte(body))
		assert.Equal(t, http.StatusBadRequest, status, "%s: %s", body, out)
	}
	for _, q := range []string{"?scheduleName=s&groupName=g", ""} {
		status, out := e.do(http.MethodDelete, "localhost", "/v1/scheduler/invocation-outcome"+q, "", "", nil)
		assert.Equal(t, http.StatusOK, status, "%s", out)
	}
}
