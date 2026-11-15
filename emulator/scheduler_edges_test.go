package emulator_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The less-traveled paths of the Scheduler's clock, targets, flattener and refusals (#1501).

// The flattener's shape facts that no routed Query operation happens to use today — flattened and
// named lists, flattened maps, lists of lists — driven with injected hints.
func TestSchedulerQuery_FlattenerShapeFacts(t *testing.T) {
	t.Parallel()
	type hint = emulator.SchedQueryHintForTest
	for _, tc := range []struct {
		name  string
		input string
		ec2   bool
		hints map[string]hint
		want  map[string]string
	}{
		{"a flattened list", `{"Items": ["a", "b"]}`, false,
			map[string]hint{"Items": {Flat: true}}, map[string]string{"Items.1": "a", "Items.2": "b"}},
		{"a flattened list with a named member replaces the last segment", `{"Outer": {"Things": ["x"]}}`, false,
			map[string]hint{"Outer.Things": {Flat: true, List: "Thing"}}, map[string]string{"Outer.Thing.1": "x"}},
		{"a top-level flattened named list", `{"Things": ["x"]}`, false,
			map[string]hint{"Things": {Flat: true, List: "Thing"}}, map[string]string{"Thing.1": "x"}},
		{"a flattened map", `{"M": {"k": "v"}}`, false,
			map[string]hint{"M": {Map: true, Flat: true, Key: "key", Value: "value"}}, map[string]string{"M.1.key": "k", "M.1.value": "v"}},
		{"a renamed member", `{"Renamed": "v"}`, false,
			map[string]hint{"Renamed": {Name: "WireName"}}, map[string]string{"WireName": "v"}},
		{"a list of lists with a named inner member", `{"Nested": [["a"]]}`, false,
			map[string]hint{"Nested[]": {List: "item"}}, map[string]string{"Nested.member.1.item.1": "a"}},
		{"an ec2 empty list sends nothing", `{"Ids": []}`, true, nil, map[string]string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := emulator.SchedQueryFlattenForTest(tc.input, tc.ec2, tc.hints)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestSchedulerTargets_RequestEdges(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		rec     *emulator.SchedulerRecord
		input   string
		intent  bool
		failure string
		check   func(t *testing.T, req *emulator.AWSRequest)
	}{
		{name: "a universal ARN with no action", rec: schedTargetRec("arn:aws:scheduler:::aws-sdk:ssm"), intent: true},
		{name: "a lambda:invoke Input that is not JSON", rec: schedTargetRec("arn:aws:scheduler:::aws-sdk:lambda:invoke"),
			input: "not json", failure: "ValidationException"},
		{name: "a REST action other than lambda:invoke", rec: schedTargetRec("arn:aws:scheduler:::aws-sdk:lambda:createFunction"),
			input: "{}", intent: true},
		{name: "an action substrate does not route is sent with its first letter raised",
			rec: schedTargetRec("arn:aws:scheduler:::aws-sdk:ssm:fooBar"), input: "{}",
			check: func(t *testing.T, req *emulator.AWSRequest) {
				assert.Equal(t, "FooBar", req.Operation)
				assert.Equal(t, "AmazonSSM.FooBar", req.Headers["X-Amz-Target"])
			}},
		{name: "a templated target that is not an ARN", rec: schedTargetRec("not-an-arn"), intent: true},
		{name: "a Lambda ARN naming no function", rec: schedTargetRec("arn:aws:lambda:us-east-1:123456789012:layer:x"), intent: true},
		{name: "a templated Lambda ARN with a qualifier", rec: schedTargetRec("arn:aws:lambda:us-east-1:123456789012:function:fn:live"),
			check: func(t *testing.T, req *emulator.AWSRequest) {
				assert.Equal(t, "/2015-03-31/functions/fn/invocations", req.Path)
				assert.Equal(t, "live", req.Params["Qualifier"])
				assert.Equal(t, "{}", string(req.Body), "no Input sends an empty event")
			}},
		{name: "a templated FIFO SQS target carries its message group", rec: func() *emulator.SchedulerRecord {
			r := schedTargetRec("arn:aws:sqs:us-east-1:123456789012:jobs.fifo")
			r.Target.SqsParameters = &emulator.SchedulerSqsParameters{MessageGroupID: "g1"}
			return r
		}(), input: "hello", check: func(t *testing.T, req *emulator.AWSRequest) {
			assert.JSONEq(t, `{"QueueUrl":"http://sqs.us-east-1.localhost/123456789012/jobs.fifo","MessageBody":"hello","MessageGroupId":"g1"}`,
				string(req.Body))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req, failure, intent := emulator.SchedTargetRequestForRecordForTest(tc.rec, tc.input)
			assert.Equal(t, tc.intent, intent != "", "intent %q", intent)
			assert.Equal(t, tc.failure, failure)
			if tc.check != nil {
				require.NotNil(t, req)
				tc.check(t, req)
			}
		})
	}
}

func schedTargetRec(arn string) *emulator.SchedulerRecord {
	return &emulator.SchedulerRecord{Target: emulator.SchedulerTarget{ARN: arn}}
}

// A recurring DELETE schedule that never fires (its StartDate is after its EndDate) is deleted once
// the clock passes EndDate, and one with no future occurrence and no EndDate is never due. An
// expression substrate cannot evaluate is refused at create (#1514), so it stores nothing to come due.
func TestSchedulerClock_SchedulesThatNeverFire(t *testing.T) {
	t.Parallel()
	e := newSchedEnv(t)
	wake := func() string {
		data, err := e.ts.StateManager().Get(t.Context(), "scheduler", "sched_wake")
		require.NoError(t, err)
		return string(data)
	}
	base := func(expr string) map[string]any {
		return map[string]any{
			"ScheduleExpression": expr, "FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
			"ActionAfterCompletion": "DELETE",
			"Target":                map[string]any{"Arn": "arn:aws:sqs:us-east-1:123456789012:q", "RoleArn": schedRoleARN},
		}
	}
	e.mustSched(http.MethodPost, "/schedules/past-cron", base("cron(0 0 1 1 ? 2020)"))
	status, _ := e.sched(http.MethodPost, "/schedules/unevaluable", base("rate(0 minutes)"))
	assert.Equal(t, http.StatusBadRequest, status, "an unevaluable expression is refused")
	assert.Empty(t, wake(), "nothing will come due")

	inverted := base("rate(1 hour)")
	inverted["StartDate"] = float64(schedClockInstant.Add(3 * time.Hour).Unix())
	inverted["EndDate"] = float64(schedClockInstant.Add(2 * time.Hour).Unix())
	e.mustSched(http.MethodPost, "/schedules/inverted", inverted)
	assert.JSONEq(t, `{"at":"2026-11-16T13:00:00Z"}`, wake(), "due at EndDate, to be found finished")

	e.ts.AdvanceTime(time.Hour)
	e.mustSched(http.MethodGet, "/schedules/inverted", nil)
	e.ts.AdvanceTime(time.Hour)
	status, _ = e.sched(http.MethodGet, "/schedules/inverted", nil)
	assert.Equal(t, http.StatusNotFound, status, "finished without firing once EndDate passed")
	status, _ = e.sched(http.MethodGet, "/schedules/past-cron", nil)
	assert.Equal(t, http.StatusOK, status, "past-cron is kept: it never finishes")
	assert.Empty(t, e.clockDispatches())
}

// Re-enabling a recurring schedule resumes its series after the update, whether or not it had fired,
// and an update keeping an explicit StartDate keeps the series too.
func TestSchedulerClock_ReEnablingResumesTheSeries(t *testing.T) {
	t.Parallel()
	e := newSchedEnv(t)
	queueURL, queueARN := e.createQueue("resume")
	body := func(state string, start *time.Time) map[string]any {
		b := map[string]any{
			"ScheduleExpression": "rate(10 minutes)", "State": state,
			"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
			"Target":             map[string]any{"Arn": queueARN, "RoleArn": schedRoleARN, "Input": "<aws.scheduler.scheduled-time>"},
		}
		if start != nil {
			b["StartDate"] = float64(start.Unix())
		}
		return b
	}
	bodies := func() []string {
		var out []string
		for _, m := range e.receive(queueURL) {
			out = append(out, m.Body)
		}
		return out
	}

	e.mustSched(http.MethodPost, "/schedules/never-fired", body("DISABLED", nil))
	e.ts.AdvanceTime(25 * time.Minute)
	e.mustSched(http.MethodPut, "/schedules/never-fired", body("ENABLED", nil))
	assert.Empty(t, bodies(), "re-enabled at 11:25, nothing is due until 11:30")
	e.ts.AdvanceTime(5 * time.Minute)
	assert.Equal(t, []string{"2026-11-16T11:30:00Z"}, bodies(), "the series kept its 11:00 anchor")

	e.mustSched(http.MethodPut, "/schedules/never-fired", body("DISABLED", nil))
	e.ts.AdvanceTime(25 * time.Minute) // 11:55
	e.mustSched(http.MethodPut, "/schedules/never-fired", body("ENABLED", nil))
	assert.Len(t, bodies(), 1, "the 11:40 and 11:50 occurrences missed while disabled are not made up")
	e.ts.AdvanceTime(5 * time.Minute)
	assert.ElementsMatch(t, []string{"2026-11-16T11:30:00Z", "2026-11-16T12:00:00Z"}, bodies())

	start := schedClockInstant.Add(2 * time.Hour) // 13:00
	e.mustSched(http.MethodPost, "/schedules/with-start", body("ENABLED", &start))
	e.mustSched(http.MethodPut, "/schedules/with-start", body("ENABLED", &start))
	e.ts.AdvanceTime(time.Hour) // 13:00
	assert.Contains(t, bodies(), "2026-11-16T13:00:00Z", "an update keeping StartDate keeps the series")
}

// Store inconsistencies set the affected schedule aside without stopping the others: a timer that
// will not decode, a timer whose record is gone, a record rewritten DISABLED without a re-arm, and an
// unreadable seed (which is ignored).
func TestSchedulerClock_StoreInconsistenciesAreSetAside(t *testing.T) {
	t.Parallel()
	e := newSchedEnv(t)
	state := e.ts.StateManager()
	for _, name := range []string{"a-timer-corrupt", "b-orphan", "c-disabled-raw", "d-good"} {
		e.mustSched(http.MethodPost, "/schedules/"+name, leaseSchedule(map[string]any{
			"GroupName": nil,
			"Target": map[string]any{"Arn": "arn:aws:scheduler:::aws-sdk:ssm:putParameter", "RoleArn": schedRoleARN,
				"Input": `{"Name": "/` + name + `", "Value": "v", "Type": "String"}`},
		}))
	}
	const prefix = "123456789012/us-east-1/default/"
	require.NoError(t, state.Put(t.Context(), "scheduler", "sched_timer:"+prefix+"a-timer-corrupt", []byte("{bad")))
	require.NoError(t, state.Delete(t.Context(), "scheduler", "sched:"+prefix+"b-orphan"))
	raw, err := state.Get(t.Context(), "scheduler", "sched:"+prefix+"c-disabled-raw")
	require.NoError(t, err)
	require.NoError(t, state.Put(t.Context(), "scheduler", "sched:"+prefix+"c-disabled-raw",
		[]byte(strings.Replace(string(raw), `"state":"ENABLED"`, `"state":"DISABLED"`, 1))))
	require.NoError(t, state.Put(t.Context(), "scheduler-ctrl", "outcome:*/*", []byte("{bad")))

	e.ts.AdvanceTime(time.Hour)
	_, found := e.parameter("/d-good")
	assert.True(t, found)
	assert.Equal(t, []string{"ssm:PutParameter"}, e.clockDispatches(), "only d-good fired")
	orphan, err := state.Get(t.Context(), "scheduler", "sched_timer:"+prefix+"b-orphan")
	require.NoError(t, err)
	assert.Nil(t, orphan, "the orphan timer was removed")
}

// A dead-letter event for a target with no Input carries an empty JSON object, and a dead-letter
// queue that does not exist drops the event without failing the request that triggered it.
func TestSchedulerClock_DeadLetterEdges(t *testing.T) {
	t.Parallel()
	e := newSchedEnv(t)
	queueURL, queueARN := e.createQueue("dlq")
	e.seed(`{"scheduleName":"*","errorCode":"ValidationException"}`)
	for name, dlq := range map[string]string{
		"no-input":    queueARN,
		"missing-dlq": "arn:aws:sqs:us-east-1:123456789012:does-not-exist",
	} {
		e.mustSched(http.MethodPost, "/schedules/"+name, map[string]any{
			"ScheduleExpression": schedLeaseExpiry, "FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
			"Target": map[string]any{"Arn": "arn:aws:sqs:us-east-1:123456789012:target", "RoleArn": schedRoleARN,
				"DeadLetterConfig": map[string]any{"Arn": dlq}},
		})
	}
	e.ts.AdvanceTime(time.Hour)
	letters := e.receive(queueURL)
	require.Len(t, letters, 1)
	assert.Equal(t, "{}", letters[0].Body)
	assert.Equal(t, []string{"sqs:SendMessage", "sqs:SendMessage"}, e.clockDispatches(),
		"both dead-letter sends were made; the one to the missing queue failed")
}

func TestSchedulerRefusals_Edges(t *testing.T) {
	t.Parallel()
	e := newSchedEnv(t)
	e.createLeaseGroup()
	arn := url.PathEscape(schedGroupARNPrefix + "aegc-dev")
	tags := func(n int) []map[string]string {
		out := make([]map[string]string, n)
		for i := range out {
			out[i] = map[string]string{"Key": "k" + strings.Repeat("x", i%5) + string(rune('a'+i%26)), "Value": "v"}
		}
		return out
	}
	keys := make([]string, 201)
	for i := range keys {
		keys[i] = "TagKeys=k" + string(rune('a'+i%26))
	}
	target := func(extra map[string]any) map[string]any {
		t := map[string]any{"Arn": "arn:aws:events:us-east-1:123456789012:event-bus/default", "RoleArn": schedRoleARN}
		for k, v := range extra {
			t[k] = v
		}
		return leaseSchedule(map[string]any{"GroupName": nil, "Target": t})
	}
	for _, tc := range []struct {
		name, method, path, raw string
		body                    any
		status                  int
	}{
		{name: "more than 200 tags", method: http.MethodPost, path: "/schedule-groups/many", body: map[string]any{"Tags": tags(201)}, status: 400},
		{name: "a tag with no key", method: http.MethodPost, path: "/schedule-groups/nokey", body: map[string]any{"Tags": []map[string]string{{"Value": "v"}}}, status: 400},
		{name: "a group body that is not JSON", method: http.MethodPost, path: "/schedule-groups/badjson", raw: "{not json", status: 400},
		{name: "a tag body that is not JSON", method: http.MethodPost, path: "/tags/" + arn, raw: "{not json", status: 400},
		{name: "a tag route naming no ARN", method: http.MethodGet, path: "/tags/", status: 400},
		{name: "more than 200 tag keys", method: http.MethodDelete, path: "/tags/" + arn + "?" + strings.Join(keys, "&"), status: 400},
		{name: "an empty tag key", method: http.MethodDelete, path: "/tags/" + arn + "?TagKeys=", status: 400},
		{name: "EventBridgeParameters with no DetailType", method: http.MethodPost, path: "/schedules/eb",
			body: target(map[string]any{"EventBridgeParameters": map[string]any{"Source": "s"}}), status: 400},
		{name: "a DetailType over 128", method: http.MethodPost, path: "/schedules/eb",
			body: target(map[string]any{"EventBridgeParameters": map[string]any{"DetailType": strings.Repeat("d", 129), "Source": "s"}}), status: 400},
		{name: "a Source over 256", method: http.MethodPost, path: "/schedules/eb",
			body: target(map[string]any{"EventBridgeParameters": map[string]any{"DetailType": "d", "Source": strings.Repeat("s", 257)}}), status: 400},
		{name: "a MessageGroupId over 128", method: http.MethodPost, path: "/schedules/eb",
			body: target(map[string]any{"SqsParameters": map[string]any{"MessageGroupId": strings.Repeat("g", 129)}}), status: 400},
		{name: "a dead-letter ARN over 1600", method: http.MethodPost, path: "/schedules/eb",
			body: target(map[string]any{"DeadLetterConfig": map[string]any{"Arn": strings.Repeat("a", 1601)}}), status: 400},
		{name: "PUT on a schedule group", method: http.MethodPut, path: "/schedule-groups/x", status: 404},
		{name: "POST on the group collection", method: http.MethodPost, path: "/schedule-groups", status: 404},
		{name: "a nested group path", method: http.MethodGet, path: "/schedule-groups/a/b", status: 404},
		{name: "PUT on a tag route", method: http.MethodPut, path: "/tags/" + arn, status: 404},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var status int
			var out []byte
			if tc.raw != "" {
				status, out = e.do(tc.method, "scheduler.us-east-1.amazonaws.com", tc.path, "application/json", "", []byte(tc.raw))
			} else {
				status, out = e.sched(tc.method, tc.path, tc.body)
			}
			if tc.status == 404 {
				assert.GreaterOrEqual(t, status, 400, "%s", out)
				return
			}
			assert.Equal(t, tc.status, status, "%s", out)
			assert.Contains(t, string(out), "ValidationException")
		})
	}
}
