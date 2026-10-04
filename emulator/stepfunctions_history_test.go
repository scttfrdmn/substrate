package emulator_test

import (
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// GetExecutionHistory's events in API_HistoryEvent's published form (#1323).
//
// Two divergences, both invisible to a test that decodes into HistoryEvent: `timestamp` answered an
// RFC3339 string where the page publishes a Timestamp, which the sfn client decodes as a number; and
// every state answered the generic types StateEntered and StateExited, neither of which is in the
// page's closed Valid Values list. So every assertion here is on the raw bytes.

// sfnHistoryClock is the instant the history fixtures run at, on a frozen clock, so every timestamp
// is known exactly.
var sfnHistoryClock = time.Unix(1700000000, 0).UTC()

// sfnPublishedHistoryEventTypes is API_HistoryEvent's `type` Valid Values list, verbatim.
var sfnPublishedHistoryEventTypes = strings.Fields(`ActivityFailed ActivityScheduled ActivityScheduleFailed
	ActivityStarted ActivitySucceeded ActivityTimedOut ChoiceStateEntered ChoiceStateExited ExecutionAborted
	ExecutionFailed ExecutionStarted ExecutionSucceeded ExecutionTimedOut FailStateEntered LambdaFunctionFailed
	LambdaFunctionScheduled LambdaFunctionScheduleFailed LambdaFunctionStarted LambdaFunctionStartFailed
	LambdaFunctionSucceeded LambdaFunctionTimedOut MapIterationAborted MapIterationFailed MapIterationStarted
	MapIterationSucceeded MapStateAborted MapStateEntered MapStateExited MapStateFailed MapStateStarted
	MapStateSucceeded ParallelStateAborted ParallelStateEntered ParallelStateExited ParallelStateFailed
	ParallelStateStarted ParallelStateSucceeded PassStateEntered PassStateExited SucceedStateEntered
	SucceedStateExited TaskFailed TaskScheduled TaskStarted TaskStartFailed TaskStateAborted TaskStateEntered
	TaskStateExited TaskSubmitFailed TaskSubmitted TaskSucceeded TaskTimedOut WaitStateAborted WaitStateEntered
	WaitStateExited MapRunAborted MapRunFailed MapRunStarted MapRunSucceeded ExecutionRedriven MapRunRedriven
	EvaluationFailed`)

// setupSFNHistoryPlugin returns the Step Functions plugin on a frozen clock, a request context and
// the state manager behind it. Freeze then SetTime, in the order TimeController.Freeze documents.
func setupSFNHistoryPlugin(t *testing.T) (*emulator.StepFunctionsPlugin, *emulator.RequestContext, emulator.StateManager) {
	t.Helper()
	tc := emulator.NewTimeController(sfnHistoryClock)
	tc.Freeze()
	tc.SetTime(sfnHistoryClock)
	state := emulator.NewMemoryStateManager()
	p := &emulator.StepFunctionsPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}), "emulator.StepFunctionsPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "req-sfn-history",
		IDs:       emulator.NewIDMint("req-sfn-history"),
	}, state
}

// sfnHistoryEvents runs def to completion and returns GetExecutionHistory's raw events.
func sfnHistoryEvents(t *testing.T, p *emulator.StepFunctionsPlugin, ctx *emulator.RequestContext, name, def string) []map[string]json.RawMessage {
	t.Helper()
	smArn := sfnCreate(t, p, ctx, name, def, "STANDARD")
	started := sfnWireRaw(t, p, ctx, "StartExecution", map[string]any{"stateMachineArn": smArn, "name": name + "-exec", "input": `{"n":1}`})
	var exec struct {
		ExecutionArn string `json:"executionArn"`
	}
	require.NoError(t, json.Unmarshal(started, &exec), "decode StartExecution: %s", started)
	body := sfnWireRaw(t, p, ctx, "GetExecutionHistory", map[string]any{"executionArn": exec.ExecutionArn})
	var out struct {
		Events []map[string]json.RawMessage `json:"events"`
	}
	require.NoError(t, json.Unmarshal(body, &out), "decode GetExecutionHistory: %s", body)
	require.NotEmpty(t, out.Events, "GetExecutionHistory: %s", body)
	return out.Events
}

// sfnHistoryTypes returns each event's type, requiring every one to be a published value and every
// timestamp to be the frozen clock as a bare JSON number.
func sfnHistoryTypes(t *testing.T, events []map[string]json.RawMessage) []string {
	t.Helper()
	types := make([]string, 0, len(events))
	for i, ev := range events {
		var typ string
		require.NoErrorf(t, json.Unmarshal(ev["type"], &typ), "event %d type: %s", i, ev["type"])
		require.Containsf(t, sfnPublishedHistoryEventTypes, typ, "event %d answered type %q, which API_HistoryEvent does not publish", i, typ)
		types = append(types, typ)

		raw := strings.TrimSpace(string(ev["timestamp"]))
		require.NotEmptyf(t, raw, "event %d (%s) has no timestamp, which API_HistoryEvent marks Required", i, typ)
		require.NotEqualf(t, byte('"'), raw[0], "event %d (%s) answered timestamp %s as a string; the page publishes a Timestamp, epoch seconds", i, typ, raw)
		var secs float64
		require.NoErrorf(t, json.Unmarshal(ev["timestamp"], &secs), "event %d timestamp: %s", i, raw)
		require.InDeltaf(t, float64(sfnHistoryClock.Unix()), secs, 1e-6, "event %d (%s) timestamp", i, typ)
	}
	return types
}

func TestSFNHistory_EventsAnswerPublishedTypesAndEpochTimestamps(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		def  string
		want []string
	}{
		{
			name: "pass, choice, wait, parallel, map and succeed",
			def: `{"StartAt":"P","States":{
				"P":{"Type":"Pass","Next":"C"},
				"C":{"Type":"Choice","Choices":[{"Variable":"$.n","NumericEquals":1,"Next":"W"}],"Default":"W"},
				"W":{"Type":"Wait","Seconds":0,"Next":"Par"},
				"Par":{"Type":"Parallel","Branches":[{"StartAt":"B","States":{"B":{"Type":"Pass","End":true}}}],"Next":"M"},
				"M":{"Type":"Map","ItemsPath":"$","Iterator":{"StartAt":"I","States":{"I":{"Type":"Pass","End":true}}},"Next":"S"},
				"S":{"Type":"Succeed"}}}`,
			want: []string{
				"ExecutionStarted",
				"PassStateEntered", "PassStateExited",
				"ChoiceStateEntered", "ChoiceStateExited",
				"WaitStateEntered", "WaitStateExited",
				"ParallelStateEntered", "ParallelStateExited",
				"MapStateEntered", "MapStateExited",
				"SucceedStateEntered", "SucceedStateExited",
				"ExecutionSucceeded",
			},
		},
		{
			// A Fail state publishes an entered event and no exited one: it ends the execution.
			name: "fail",
			def:  `{"StartAt":"P","States":{"P":{"Type":"Pass","Next":"F"},"F":{"Type":"Fail","Error":"E","Cause":"c"}}}`,
			want: []string{"ExecutionStarted", "PassStateEntered", "PassStateExited", "FailStateEntered", "ExecutionFailed"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, ctx, _ := setupSFNHistoryPlugin(t)
			name := "hist" + strings.ReplaceAll(strings.ReplaceAll(tc.name, " ", ""), ",", "")
			got := sfnHistoryTypes(t, sfnHistoryEvents(t, p, ctx, name, tc.def))
			require.Equal(t, tc.want, got, "the event sequence")
		})
	}
}

// The persisted record keeps its encoding: HistoryEvent.Timestamp is still stored as an RFC3339
// string, which is what every run recorded before #1323 holds, and GetExecutionHistory answers that
// stored history as epoch seconds. So a replay of an older recording reads back.
func TestSFNHistory_TheStoredEncodingIsUnchangedAndStillAnswersEpochSeconds(t *testing.T) {
	t.Parallel()
	p, ctx, state := setupSFNHistoryPlugin(t)
	events := sfnHistoryEvents(t, p, ctx, "histstored", `{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`)
	sfnHistoryTypes(t, events)

	data, err := state.Get(t.Context(), "states", "execution:123456789012/us-east-1/histstored/histstored-exec")
	require.NoError(t, err, "state.Get execution")
	require.NotNil(t, data, "no execution record stored")
	var record struct {
		History []map[string]json.RawMessage `json:"History"`
	}
	require.NoError(t, json.Unmarshal(data, &record), "decode the stored execution: %s", data)
	require.NotEmpty(t, record.History, "the stored execution holds no history: %s", data)
	for i, ev := range record.History {
		require.JSONEqf(t, `"2023-11-14T22:13:20Z"`, string(ev["timestamp"]),
			"stored event %d: the persisted encoding must stay RFC3339, as every earlier recording holds it", i)
	}
}
