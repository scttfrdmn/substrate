package emulator_test

import (
	"net/http"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestSchedulerWire_ScheduleResponsesCarryNoBookkeepingMember is the raw-bytes assertion
// scripts/wire-bookkeeping-projected.txt cites for SchedulerRecord (#756).
//
// SchedulerRecord declares its account as `account_id` and its Region as `region`, neither under
// omitempty. Neither reaches a body: GetSchedule answers schedRecordToWire, and the other operations
// answer an ARN or a summary list. The snake_case spelling is listed in its own right because a fold
// does not reach it. All five routed operations are driven.
func TestSchedulerWire_ScheduleResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p := &emulator.SchedulerPlugin{}
	ctx, state := wireSetup(t, p, "req-scheduler-wire")
	call := func(method, path string, body map[string]any) []byte {
		return wireREST(t, p, ctx, "scheduler", method, path, body)
	}

	schedule := map[string]any{
		"ScheduleExpression": "rate(5 minutes)",
		"Target":             map[string]any{"Arn": "arn:aws:lambda:us-east-1:123456789012:function:wire", "RoleArn": "arn:aws:iam::123456789012:role/wire"},
		"FlexibleTimeWindow": map[string]any{"Mode": "OFF"},
	}
	created := call(http.MethodPost, "/schedules/wire-schedule", schedule)
	wireRequireHeld(t, state, "scheduler", "sched:123456789012/us-east-1/default/wire-schedule", "account_id", "region")

	wireRunJSON(t, []string{"AccountID", "account_id", "Region"}, []wireCase{
		{op: "CreateSchedule", held: created, anchor: `"ScheduleArn"`},
		{op: "GetSchedule", call: func() []byte { return call(http.MethodGet, "/schedules/wire-schedule", nil) }, anchor: `"Name":"wire-schedule"`},
		{op: "ListSchedules", call: func() []byte { return call(http.MethodGet, "/schedules", nil) }, anchor: "wire-schedule"},
		{op: "UpdateSchedule", call: func() []byte { return call(http.MethodPut, "/schedules/wire-schedule", schedule) }, anchor: `"ScheduleArn"`},
		{op: "DeleteSchedule", call: func() []byte { return call(http.MethodDelete, "/schedules/wire-schedule", nil) }, anchor: "{"},
	})
}
