package emulator_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// An SQS event-source mapping polls on the simulated clock (#1292). Every test here runs on a
// frozen clock and moves it by hand, so none depends on real elapsed time.

// esmClockInstant is the frozen instant each test starts at.
var esmClockInstant = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// esmPeekCount counts the visible messages on the queue without hiding any: a receive with a
// visibility timeout of zero leaves each message where it was.
func esmPeekCount(t *testing.T, ts *emulator.TestServer, queueURL string) int {
	t.Helper()
	body := "Action=ReceiveMessage&QueueUrl=" + queueURL +
		"&MaxNumberOfMessages=10&VisibilityTimeout=0&WaitTimeSeconds=0&Version=2012-11-05"
	resp := esmDoRequest(t, ts, http.MethodPost, "/", "sqs.us-east-1.amazonaws.com",
		"application/x-www-form-urlencoded", strings.NewReader(body))
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "peek: %s", b)
	return strings.Count(string(b), "<MessageId>")
}

// esmClockFixture is a frozen server holding a queue with two messages, a function, and an
// enabled mapping between them.
type esmClockFixture struct {
	ts       *emulator.TestServer
	queueURL string
	esmID    string
}

func newESMClockFixture(t *testing.T, opts ...emulator.TestServerOption) esmClockFixture {
	t.Helper()
	ts := emulator.StartTestServer(t, opts...)
	ts.FreezeTimeAt(esmClockInstant)

	name := "esm-clock-" + strings.ReplaceAll(strings.ToLower(t.Name()), "/", "-")
	esmCreateSQSQueue(t, ts, name)
	queueURL := fmt.Sprintf("http://sqs.us-east-1.amazonaws.com/123456789012/%s", name)
	esmSendSQSMessage(t, ts, queueURL, "one")
	esmSendSQSMessage(t, ts, queueURL, "two")
	esmCreateLambdaFunction(t, ts, name+"-fn")
	id := esmCreateESM(t, ts, name+"-fn", "arn:aws:sqs:us-east-1:123456789012:"+name)
	return esmClockFixture{ts: ts, queueURL: queueURL, esmID: id}
}

func TestESMClock_AFrozenClockMakesNoProgress(t *testing.T) {
	t.Parallel()
	f := newESMClockFixture(t)

	for range 5 {
		require.Equal(t, 2, esmPeekCount(t, f.ts, f.queueURL),
			"no simulated time has passed, so no poll is due however many requests arrive")
	}
}

func TestESMClock_APollIsDueOneSimulatedIntervalAfterTheMappingIsCreated(t *testing.T) {
	t.Parallel()
	f := newESMClockFixture(t)

	f.ts.AdvanceTime(999 * time.Millisecond)
	require.Equal(t, 2, esmPeekCount(t, f.ts, f.queueURL), "one millisecond short of the interval, nothing is due")

	f.ts.AdvanceTime(time.Millisecond)
	assert.Zero(t, esmPeekCount(t, f.ts, f.queueURL),
		"the poll due at one simulated second consumed both messages before the observing request")
}

func TestESMClock_ADisabledMappingDoesNotPollAndReEnablingRestartsTheCadence(t *testing.T) {
	t.Parallel()
	f := newESMClockFixture(t)

	esmSetEnabled(t, f.ts, f.esmID, false)
	f.ts.AdvanceTime(10 * time.Second)
	require.Equal(t, 2, esmPeekCount(t, f.ts, f.queueURL), "a disabled mapping polls nothing")

	esmSetEnabled(t, f.ts, f.esmID, true)
	require.Equal(t, 2, esmPeekCount(t, f.ts, f.queueURL),
		"re-enabling starts the cadence from now, so the time passed while disabled does not count")
	f.ts.AdvanceTime(time.Second)
	assert.Zero(t, esmPeekCount(t, f.ts, f.queueURL))
}

func TestESMClock_ADeletedMappingDoesNotPoll(t *testing.T) {
	t.Parallel()
	f := newESMClockFixture(t)

	esmDeleteESM(t, f.ts, f.esmID)
	f.ts.AdvanceTime(10 * time.Second)
	assert.Equal(t, 2, esmPeekCount(t, f.ts, f.queueURL))
}

func TestESMClock_EachDispatchIsRecorded(t *testing.T) {
	t.Parallel()
	f := newESMClockFixture(t)
	f.ts.AdvanceTime(time.Second)
	require.Zero(t, esmPeekCount(t, f.ts, f.queueURL))

	events, err := f.ts.Store().GetStream(t.Context(), "default")
	require.NoError(t, err)
	var ops []string
	for _, e := range events {
		if strings.HasPrefix(e.RequestID, "req-clock-") {
			ops = append(ops, e.Service+":"+e.Operation)
		}
	}
	assert.Equal(t, []string{"sqs:ReceiveMessage", "lambda:Invoke", "sqs:DeleteMessage", "sqs:DeleteMessage"}, ops,
		"the poll's receive, invoke and two deletes are in the event log")
}

// A replay pins the clock to each recorded event and re-derives the poll from the request
// that triggered it, so the consumed queue replays consumed, with no state-hash difference.
func TestESMClock_AReplayReproducesTheMappingsEffectOnState(t *testing.T) {
	t.Parallel()
	f := newESMClockFixture(t, emulator.WithRecordedBodies(), emulator.WithRecordedStateHashes())
	f.ts.AdvanceTime(time.Second)
	require.Zero(t, esmPeekCount(t, f.ts, f.queueURL))
	f.ts.AdvanceTime(time.Second)
	require.Zero(t, esmPeekCount(t, f.ts, f.queueURL))

	results, err := replayEngineFor(f.ts, emulator.ReplayConfig{ValidateState: true}).
		Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	assert.Empty(t, results.Differences, "the mapping's polls replay with the state they left")
	assert.True(t, results.StateValid)
	assert.Equal(t, 5, results.SkippedEvents,
		"the five recorded dispatches (the first poll's receive, invoke and two deletes, and the second poll's empty receive) are skipped; replaying their triggering requests re-derives them")
}

func esmSetEnabled(t *testing.T, ts *emulator.TestServer, uuid string, enabled bool) {
	t.Helper()
	b, err := json.Marshal(map[string]any{"Enabled": enabled})
	require.NoError(t, err)
	resp := esmDoRequest(t, ts, http.MethodPut, "/2015-03-31/event-source-mappings/"+uuid,
		"lambda.us-east-1.amazonaws.com", "application/json", strings.NewReader(string(b)))
	defer func() { _ = resp.Body.Close() }()
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Less(t, resp.StatusCode, 300, "UpdateEventSourceMapping: %s", out)
}

// A store fault on the mapping's cursor or record is an error from RunDue, never a silent
// skip or a poll run twice.
func TestESMClock_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
	}{
		{"cursor read", func(m *cfFaultStateManager) { m.failGet = "esm_poll:" }},
		{"cursor corrupt", func(m *cfFaultStateManager) { m.corruptGet = "esm_poll:" }},
		{"cursor write", func(m *cfFaultStateManager) { m.failPut = "esm_poll:" }},
		{"mapping read", func(m *cfFaultStateManager) { m.failGet = "esm:" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			clock := emulator.NewTimeController(esmClockInstant)
			clock.Freeze()
			clock.SetTime(esmClockInstant)
			registry := emulator.NewPluginRegistry()
			lambda := &emulator.LambdaPlugin{}
			sqs := &emulator.SQSPlugin{}
			for _, p := range []emulator.Plugin{lambda, sqs} {
				require.NoError(t, p.Initialize(context.Background(), emulator.PluginConfig{
					State:   fault,
					Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
					Options: map[string]any{"time_controller": clock, "registry": registry},
				}))
				registry.Register(p)
			}
			body, err := json.Marshal(map[string]any{
				"FunctionName": "fault-fn", "EventSourceArn": "arn:aws:sqs:us-east-1:123456789012:fault-q",
			})
			require.NoError(t, err)
			ctx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: "req-fault", IDs: emulator.NewIDMint("req-fault")}
			_, err = lambda.HandleRequest(ctx, &emulator.AWSRequest{
				Service: "lambda", HTTPMethod: http.MethodPost, Path: "/2015-03-31/event-source-mappings",
				Body: body, Headers: map[string]string{}, Params: map[string]string{},
			})
			require.NoError(t, err, "CreateEventSourceMapping")
			require.Equal(t, 1, lambda.ESMPollerCountForTest())

			clock.SetTime(esmClockInstant.Add(time.Second))
			tc.arm(fault)
			err = lambda.RunDue(ctx, registry.RouteRequest)
			require.Error(t, err, "%s must be an error", tc.name)
		})
	}
}
