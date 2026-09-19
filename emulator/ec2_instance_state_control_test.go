package emulator_test

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	emulator "github.com/scttfrdmn/substrate/emulator"
)

// #514's surface: an instance's state transition is observable, so a consumer's
// wait-until-running loop has something to wait for.
//
// Every assertion is at HTTP level, because the whole of the defect was observable only
// through a response. Substrate applied every transition in the same request that asked for it,
// so `aws ec2 wait instance-running` — and Terraform's `aws_instance`, and CDK's own custom
// resources, and every hand-written poll — exited on its first iteration, and the retry,
// timeout and give-up branches those loops carry were never taken. `pending`, `stopping` and
// `shutting-down` were three of the six codes [emulator.EC2InstanceState]'s doc comment
// enumerates and no code path could produce.

// ec2SeedInstanceState POSTs an instance-progression seed to the control plane.
func ec2SeedInstanceState(t *testing.T, ts *httptest.Server, body string) {
	t.Helper()
	ec2SeedInstanceStateAt(t, ts.URL, body)
}

// ec2SeedInstanceStateAt is [ec2SeedInstanceState] against a base URL, for the replay test, whose
// server is an [emulator.TestServer] rather than an [httptest.Server].
func ec2SeedInstanceStateAt(t *testing.T, baseURL, body string) {
	t.Helper()
	resp, err := http.Post(baseURL+"/v1/ec2/instance-state", "application/json", strings.NewReader(body))
	require.NoError(t, err, "seed instance state")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		got, _ := io.ReadAll(resp.Body)
		t.Fatalf("seed instance state = %d: %s", resp.StatusCode, got)
	}
}

// ec2ClearInstanceState DELETEs a progression seed; an empty instanceID clears every seed.
func ec2ClearInstanceState(t *testing.T, ts *httptest.Server, instanceID string) {
	t.Helper()
	u := ts.URL + "/v1/ec2/instance-state"
	if instanceID != "" {
		u += "?instanceId=" + instanceID
	}
	req, err := http.NewRequest(http.MethodDelete, u, nil)
	require.NoError(t, err, "build clear request")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "clear instance state")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		got, _ := io.ReadAll(resp.Body)
		t.Fatalf("clear instance state = %d: %s", resp.StatusCode, got)
	}
}

// ec2StateChange is the currentState/previousState pair every state-changing operation answers
// with, read out of the operation's own response rather than out of a later describe.
//
// The two are asserted together because only the pair says a transition happened: a
// `currentState` of `stopping` beside a `previousState` of `stopping` would be substrate
// reporting the transient state twice rather than reporting a transition into it.
type ec2StateChange struct {
	Current  string
	Previous string
}

// ec2ChangeStateOf sends one of the three state-changing operations for a single instance and
// returns the pair its response reports.
func ec2ChangeStateOf(t *testing.T, ts *httptest.Server, action, instanceID string) ec2StateChange {
	t.Helper()
	resp := ec2Request(t, ts, map[string]string{"Action": action, "InstanceId.1": instanceID})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode, action)

	var out struct {
		Items []struct {
			InstanceID   string `xml:"instanceId"`
			CurrentState struct {
				Code int    `xml:"code"`
				Name string `xml:"name"`
			} `xml:"currentState"`
			PreviousState struct {
				Name string `xml:"name"`
			} `xml:"previousState"`
		} `xml:"instancesSet>item"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&out))
	require.Len(t, out.Items, 1, "%s must report one instance", action)
	item := out.Items[0]
	require.Equal(t, instanceID, item.InstanceID)
	assert.Equal(t, ec2StateCodes[item.CurrentState.Name], item.CurrentState.Code,
		"%s reports the code AWS publishes beside the name", action)
	return ec2StateChange{Current: item.CurrentState.Name, Previous: item.PreviousState.Name}
}

// ec2StateCodes is the name → code mapping AWS publishes in [EC2InstanceState]'s own Valid
// Values and in the lifecycle page's state table, asserted alongside every name so a rename
// cannot silently ship a name with the wrong code.
var ec2StateCodes = map[string]int{
	"pending": 0, "running": 16, "shutting-down": 32,
	"terminated": 48, "stopping": 64, "stopped": 80,
}

// TestEC2_InstanceState_TransitionIsPublishedInTheOperationsOwnResponse is the half of #514
// that needs no seed, because AWS publishes it in the sample responses of the operations
// themselves: `API_StartInstances` shows `currentState` 0/`pending` beside `previousState`
// 80/`stopped`, and `API_StopInstances` shows 64/`stopping` beside 16/`running`.
//
// Substrate reported the *settled* state as `currentState` at all four write sites, so a
// consumer reading the transition out of the call it just made — which is what a waiter's first
// observation is — saw a transition that had already finished. No test asserted `currentState`
// anywhere, which is how three of the six published codes stayed unreachable.
func TestEC2_InstanceState_TransitionIsPublishedInTheOperationsOwnResponse(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	id := runInstance(t, ts, map[string]string{"ImageId": ec2TestImage, "InstanceType": "t3.micro"})

	assert.Equal(t, ec2StateChange{Current: "stopping", Previous: "running"},
		ec2ChangeStateOf(t, ts, "StopInstances", id),
		"a stop reports the state it is entering, not the one it will settle in")
	require.Equal(t, "stopped", instanceState(t, ts, id),
		"and the record settled, because a transient state is reported and never stored")

	assert.Equal(t, ec2StateChange{Current: "pending", Previous: "stopped"},
		ec2ChangeStateOf(t, ts, "StartInstances", id),
		"the published previousState is the state the record held, which is why it settles")
	require.Equal(t, "running", instanceState(t, ts, id))

	assert.Equal(t, ec2StateChange{Current: "shutting-down", Previous: "running"},
		ec2ChangeStateOf(t, ts, "TerminateInstances", id))
	require.Equal(t, "terminated", instanceState(t, ts, id))
}

// TestEC2_InstanceState_ALaunchIsBornPending pins the same rule at the fourth write site, which
// answers through `instancesSet` rather than through a `currentState` pair.
//
// The lifecycle page states it as prose — "When you launch an instance, it enters the pending
// state" — and `API_RunInstances`' sample response shows code 0. The describe that follows is
// what makes the point: the record holds `running` throughout, so an unseeded launch settles on
// the first observation and no existing fixture changes.
func TestEC2_InstanceState_ALaunchIsBornPending(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	resp := ec2Request(t, ts, map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "InstanceType": "t3.micro",
		"MinCount": "1", "MaxCount": "1",
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out struct {
		Instances []struct {
			InstanceID string `xml:"instanceId"`
			State      struct {
				Code int    `xml:"code"`
				Name string `xml:"name"`
			} `xml:"instanceState"`
		} `xml:"instancesSet>item"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&out))
	require.Len(t, out.Instances, 1)
	assert.Equal(t, "pending", out.Instances[0].State.Name)
	assert.Equal(t, 0, out.Instances[0].State.Code)

	assert.Equal(t, "running", instanceState(t, ts, out.Instances[0].InstanceID),
		"an unseeded launch settles on the first observation, so every existing fixture is unchanged")
}

// TestEC2_InstanceState_ProgressionPollsToRunning is the operation's fail-before: without a seed
// the first observation already reports the settled state, so the whole sequence below collapses
// to one line.
//
// Two observations rather than one, because one cannot distinguish a countdown from an
// off-by-one — a seed that reported the transient state forever, or exactly once regardless of
// the count, would both satisfy a single-poll assertion. `DescribeInstanceStatus` is asserted in
// the same walk because it is the other operation a waiter polls (`instance-status-ok` reads it)
// and it shares the observation, so the two views cannot disagree about which poll it is.
func TestEC2_InstanceState_ProgressionPollsToRunning(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	ec2SeedInstanceState(t, ts, `{"instanceId":"*","transientObservations":2}`)
	id := runInstance(t, ts, map[string]string{"ImageId": ec2TestImage, "InstanceType": "t3.micro"})

	assert.Equal(t, "pending", instanceState(t, ts, id), "poll 1")
	assert.Equal(t, "pending", instanceState(t, ts, id), "poll 2")
	assert.Equal(t, "running", instanceState(t, ts, id), "poll 3 settles")
	assert.Equal(t, "running", instanceState(t, ts, id),
		"and stays settled — the countdown is spent, not restarted by a further poll")

	// The countdown restarts at the transition, not at seed time, so the same seed covers the
	// next transition of the same instance. A seed that ran from when it was written would make
	// this stop instantaneous, which is the opposite of what a stop/start waiter test needs.
	assert.Equal(t, "stopping", ec2ChangeStateOf(t, ts, "StopInstances", id).Current)
	assert.Equal(t, "stopping", instanceState(t, ts, id), "poll 1 after the stop")
	assert.Equal(t, "stopping", instanceState(t, ts, id), "poll 2")
	assert.Equal(t, "stopped", instanceState(t, ts, id), "poll 3 settles")
}

// TestEC2_InstanceState_ProgressionIsVisibleToDescribeInstanceStatusAndItsFilter is the
// consistency property the substitution point exists for.
//
// The observed state is written onto the local copy before the filters run, so
// `instance-state-name` selects on the state the body reports. Substituting at render time
// instead would have produced an answer no caller could act on: a filter for `running` would
// match an instance whose rendered `instanceState` said `pending`, and a filter for `pending`
// would match nothing at all while every body reported it.
func TestEC2_InstanceState_ProgressionIsVisibleToDescribeInstanceStatusAndItsFilter(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	ec2SeedInstanceState(t, ts, `{"instanceId":"*","transientObservations":1}`)
	id := runInstance(t, ts, map[string]string{"ImageId": ec2TestImage, "InstanceType": "t3.micro"})

	assert.Equal(t, []string{id}, ec2InstanceIDsMatchingState(t, ts, "pending"),
		"the filter selects the state the caller is being told about")
	assert.Equal(t, "running", instanceState(t, ts, id),
		"the filtering describe spent the one observation, so this one settles")
	assert.Empty(t, ec2InstanceIDsMatchingState(t, ts, "pending"),
		"and the transient state is no longer selectable once the countdown is spent")
}

// ec2InstanceIDsMatchingState runs DescribeInstanceStatus filtered on one state name and returns
// the instance IDs it answers with.
func ec2InstanceIDsMatchingState(t *testing.T, ts *httptest.Server, state string) []string {
	t.Helper()
	resp := ec2Request(t, ts, map[string]string{
		"Action":        "DescribeInstanceStatus",
		"Filter.1.Name": "instance-state-name", "Filter.1.Value.1": state,
	})
	defer resp.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var out struct {
		Items []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"instanceStatusSet>item"`
	}
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&out))
	ids := make([]string, 0, len(out.Items))
	for _, item := range out.Items {
		ids = append(ids, item.InstanceID)
	}
	return ids
}

// TestEC2_InstanceState_SeedsAreScopedAndClearable pins the two control-plane behaviors a test
// harness depends on: an ID-scoped seed governs that instance alone, and clearing a seed
// restores the instantaneous transition the rest of the suite assumes.
func TestEC2_InstanceState_SeedsAreScopedAndClearable(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	seeded := runInstance(t, ts, map[string]string{"ImageId": ec2TestImage, "InstanceType": "t3.micro"})
	other := runInstance(t, ts, map[string]string{"ImageId": ec2TestImage, "InstanceType": "t3.micro"})

	ec2SeedInstanceState(t, ts, `{"instanceId":"`+seeded+`","transientObservations":1}`)
	assert.Equal(t, "stopping", ec2ChangeStateOf(t, ts, "StopInstances", seeded).Current)
	assert.Equal(t, "stopping", instanceState(t, ts, seeded))

	// A describe of the unseeded instance does not draw on the seeded one's countdown, which is
	// what keeps "the next poll is stopping" from depending on how many instances exist.
	assert.Equal(t, "stopping", ec2ChangeStateOf(t, ts, "StopInstances", other).Current,
		"the immediate response is published, so it is transient with or without a seed")
	assert.Equal(t, "stopped", instanceState(t, ts, other),
		"but no seed governs this instance, so its first observation settles")
	assert.Equal(t, "stopped", instanceState(t, ts, seeded),
		"and the seeded instance's own second observation settles it")

	ec2ClearInstanceState(t, ts, "")
	assert.Equal(t, "pending", ec2ChangeStateOf(t, ts, "StartInstances", seeded).Current)
	assert.Equal(t, "running", instanceState(t, ts, seeded),
		"with the seed cleared the transition is instantaneous again")

	// An ID-scoped clear takes effect mid-progression, which is the property a test relies on when
	// it seeds a slow transition, asserts the waiting, and then wants the loop to finish.
	ec2SeedInstanceState(t, ts, `{"instanceId":"`+seeded+`","transientObservations":5}`)
	require.Equal(t, "stopping", ec2ChangeStateOf(t, ts, "StopInstances", seeded).Current)
	require.Equal(t, "stopping", instanceState(t, ts, seeded), "one observation of five")
	ec2ClearInstanceState(t, ts, seeded)
	assert.Equal(t, "stopped", instanceState(t, ts, seeded),
		"the next observation settles, with four of the five observations unspent")
}

// TestEC2_InstanceState_SeedEndpointRefusesANegativeCount keeps the control plane from storing a
// count no observation can be compared against, which would look accepted and change nothing.
func TestEC2_InstanceState_SeedEndpointRefusesANegativeCount(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)

	for _, body := range []string{
		`{"instanceId":"*","transientObservations":-1}`,
		`{not-json`,
	} {
		resp, err := http.Post(ts.URL+"/v1/ec2/instance-state", "application/json",
			strings.NewReader(body))
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode, body)
		require.NoError(t, resp.Body.Close())
	}

	// And neither refusal stored anything, so a test that ignored the status does not then see a
	// progression it did not ask for.
	id := runInstance(t, ts, map[string]string{"ImageId": ec2TestImage, "InstanceType": "t3.micro"})
	assert.Equal(t, "running", instanceState(t, ts, id))
}

// TestEC2_InstanceState_ATerminatedInstanceCannotBeStartedOrStopped is #514's refusal criterion,
// scoped to the one case AWS publishes.
//
// The lifecycle page's state table says a `terminated` instance "has been permanently deleted
// and cannot be started", and `errors-overview.html` publishes `IncorrectInstanceState` with the
// general description this is an instance of. Before the fix both operations wrote the instance
// back to `stopped` or `running`, resurrecting a deleted instance into a state from which a
// consumer could keep using it — a divergence no test could see, because neither operation
// looked at the state it was leaving.
//
// The stop half rests on "permanently deleted" alone, since no page found states a stop
// precondition; see [ec2CannotTransitionTerminated] for why refusing is still the reading taken.
func TestEC2_InstanceState_ATerminatedInstanceCannotBeStartedOrStopped(t *testing.T) {
	t.Parallel()
	ts := newEC2TestServer(t)
	id := runInstance(t, ts, map[string]string{"ImageId": ec2TestImage, "InstanceType": "t3.micro"})
	require.Equal(t, "shutting-down", ec2ChangeStateOf(t, ts, "TerminateInstances", id).Current)
	require.Equal(t, "terminated", instanceState(t, ts, id))

	for _, action := range []string{"StartInstances", "StopInstances"} {
		t.Run(action, func(t *testing.T) {
			resp := ec2Request(t, ts, map[string]string{"Action": action, "InstanceId.1": id})
			defer resp.Body.Close() //nolint:errcheck
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "body was %s", body)
			assert.Equal(t, "IncorrectInstanceState", ec2ErrorCodeOf(t, string(body)))
			assert.Equal(t, "terminated", instanceState(t, ts, id),
				"and the refusal wrote nothing, so the instance stays deleted")
		})
	}
}

// TestEC2_InstanceState_TheSameSeedProducesTheSameSequenceTwice is #514's reproducibility
// criterion: same inputs, seed included, same observations.
//
// It runs the sequence twice over two independent servers rather than replaying one stream, and
// that is a finding rather than a shortcut. **A seed is a control-plane input, not an event**:
// [Server] records only AWS requests, so `POST /v1/ec2/instance-state` never enters the stream,
// and [ReplayEngine.Replay] resets the whole [StateManager] before re-executing — which clears
// the seed along with the resource state. A replay of a seeded stream therefore answers the
// *unseeded* sequence, which is what the test below this one relies on. That gap is general to
// every seed in substrate, not specific to this one, and it is filed as #1140 rather than worked
// around here.
//
// What this does assert is the property the seed exists for: a poll loop written against
// substrate sees the same states in the same order on every run, so a failure is a real signal
// and not timing noise.
func TestEC2_InstanceState_TheSameSeedProducesTheSameSequenceTwice(t *testing.T) {
	t.Parallel()

	observe := func() []string {
		ts := newEC2TestServer(t)
		ec2SeedInstanceState(t, ts, `{"instanceId":"*","transientObservations":2}`)
		id := runInstance(t, ts, map[string]string{"ImageId": ec2TestImage, "InstanceType": "t3.micro"})
		seen := []string{ec2ChangeStateOf(t, ts, "StopInstances", id).Current}
		for range 4 {
			seen = append(seen, instanceState(t, ts, id))
		}
		return seen
	}

	want := []string{"stopping", "stopping", "stopping", "stopped", "stopped"}
	assert.Equal(t, want, observe(), "run 1")
	assert.Equal(t, want, observe(), "run 2, on a server that shares nothing with the first")
}

// TestEC2_InstanceState_AReplayReproducesTheRecordsOwnStates is the replay half, asserting the
// rule that makes the whole design safe: **a seed governs what an observation reports and never
// rewrites the record.**
//
// A replay is the sharpest available test of that rule, because [ReplayEngine.Replay] resets the
// state manager — seed included — and then re-executes the recorded AWS requests alone. The
// recording below observes `pending` twice under a seed while its record holds `running`; had
// either observation been written back, the record would have held `pending` and the replay, which
// has no seed, would have re-derived `running` — a difference. Both hold `running`, so neither
// observation touched the record.
//
// **Why no recorded call names the instance.** A replayed `RunInstances` mints a *new* random
// instance ID, so a recorded `StopInstances` naming the recording's ID answers
// `InvalidInstanceID.NotFound` on replay and the event fails. That is #856's second debt — the
// replay engine has no ID source, so any recorded request referring to a minted ID is
// unreplayable — and it is neither introduced nor fixed here. The stream is therefore kept
// ID-free, and the assertion reads whichever single instance the replay minted rather than a name
// from the recording.
func TestEC2_InstanceState_AReplayReproducesTheRecordsOwnStates(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())

	ec2SeedInstanceStateAt(t, ts.URL, `{"instanceId":"*","transientObservations":2}`)
	id := ec2ReplayRunInstance(t, ts)
	for i := range 2 {
		require.Equal(t, "pending", ec2ReplayDescribeSoleState(t, ts),
			"observation %d reports the seeded transient state", i+1)
	}
	require.Equal(t, "running", ec2ReplaySoleRecordedState(t, ts),
		"while the record itself settled, which is what the replay has to re-derive")

	engine := emulator.NewReplayEngine(
		ts.Store(), ts.StateManager(), ts.TimeController(), ts.Registry(),
		emulator.ReplayConfig{}, emulator.NewDefaultLogger(slog.LevelError, false),
	)
	results, err := engine.Replay(t.Context(), "default")
	require.NoError(t, err)
	require.Positive(t, results.TotalEvents, "nothing was recorded")
	require.Zero(t, results.SkippedEvents, "every recorded event must be re-executed")
	require.Zero(t, results.FailedEvents)

	assert.Equal(t, "running", ec2ReplaySoleRecordedState(t, ts),
		"the replay re-derived the same settled state the recording's record held")
	assert.NotEmpty(t, id, "the recording did mint an ID, which the replay is free to differ on")
}

// ec2ReplayCall issues one EC2 query-protocol call against a [emulator.TestServer] and returns
// the body, so the call is recorded into the stream a replay will walk.
//
// It cannot use [resetDoRequest], which labels every body `application/json`: the query protocol
// is form-encoded, and a JSON content type makes the parser find no Action at all.
func ec2ReplayCall(t *testing.T, ts *emulator.TestServer, action string, extra url.Values) []byte {
	t.Helper()
	form := url.Values{"Action": {action}, "Version": {"2016-11-15"}}
	for k, vs := range extra {
		form[k] = vs
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/",
		strings.NewReader(form.Encode()))
	require.NoError(t, err)
	req.Host = "ec2.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", action, body)
	return body
}

// ec2ReplayRunInstance launches one instance through a [emulator.TestServer] and returns its ID.
func ec2ReplayRunInstance(t *testing.T, ts *emulator.TestServer) string {
	t.Helper()
	body := ec2ReplayCall(t, ts, "RunInstances", url.Values{
		"ImageId": {ec2TestImage}, "InstanceType": {"t3.micro"},
		"MinCount": {"1"}, "MaxCount": {"1"},
	})

	var out struct {
		Instances []struct {
			InstanceID string `xml:"instanceId"`
		} `xml:"instancesSet>item"`
	}
	require.NoError(t, xml.Unmarshal(body, &out))
	require.Len(t, out.Instances, 1)
	return out.Instances[0].InstanceID
}

// ec2ReplayDescribeSoleState reports the state a `DescribeInstances` over no filters answers for
// the one instance the server holds, and is one recorded observation.
//
// No `InstanceId.N`, which is what keeps the recorded stream free of a minted ID — see the replay
// test's doc comment and #856.
func ec2ReplayDescribeSoleState(t *testing.T, ts *emulator.TestServer) string {
	t.Helper()
	body := ec2ReplayCall(t, ts, "DescribeInstances", nil)
	var out struct {
		Items []struct {
			State struct {
				Name string `xml:"name"`
			} `xml:"instanceState"`
		} `xml:"reservationSet>item>instancesSet>item"`
	}
	require.NoError(t, xml.Unmarshal(body, &out))
	require.Len(t, out.Items, 1, "one instance: %s", body)
	return out.Items[0].State.Name
}

// ec2ReplaySoleRecordedState reads the stored state of the one instance in the `ec2` namespace,
// rather than asking for it over the wire.
//
// Two reasons, and both are load-bearing. An AWS call would be *recorded*, so asserting through
// one would lengthen the very stream the replay walks; and a describe reports what an observation
// sees, where the whole point of this assertion is what the *record* holds. It scans for the key
// rather than composing one from an ID because the replay mints its own (#856).
func ec2ReplaySoleRecordedState(t *testing.T, ts *emulator.TestServer) string {
	t.Helper()
	keys, err := ts.StateManager().List(t.Context(), "ec2", "instance:")
	require.NoError(t, err)
	require.Len(t, keys, 1, "exactly one instance record: %v", keys)

	raw, err := ts.StateManager().Get(t.Context(), "ec2", keys[0])
	require.NoError(t, err)
	require.NotNil(t, raw, "%s is not in state", keys[0])
	var inst struct {
		State struct {
			Name string `json:"name"`
		} `json:"state"`
	}
	require.NoError(t, json.Unmarshal(raw, &inst))
	return inst.State.Name
}

// The state namespace and the two key prefixes the control plane writes, spelled here because
// [ec2InstStateFaultState] must fault one prefix without faulting the other. They are
// `ec2InstStateNamespace`, `ec2InstStateKeyPrefix` and `ec2InstObservedPrefix` in
// `ec2_instance_state_control.go`; a rename there fails the tests below rather than passing them
// vacuously, because every one of them asserts an observable consequence of the fault.
const (
	ec2InstStateNS       = "ec2-inst-state-ctrl"
	ec2InstStateSeedKeys = "status:"
	ec2InstStateCountKey = "observed:"
)

// ec2InstStateFaultState fails or corrupts one operation on the instance-state control namespace,
// leaving every other namespace — the instance records included — working.
//
// Keyed by key prefix as well as by namespace, because the seed and the observation counter live
// in one namespace and the interesting faults are the ones that reach only one of them: a readable
// seed beside an unwritable counter is what exercises the progression's own write path, and
// faulting the whole namespace would stop at the seed read instead.
type ec2InstStateFaultState struct {
	emulator.StateManager
	// failPrefix is the key prefix the fault applies to; empty means both.
	failPrefix string
	getErr     error
	putErr     error
	deleteErr  error
	listErr    error
	// corrupt is stored verbatim in place of whatever a matching Get would have returned, when
	// non-empty, so an unmarshal failure is reachable without an unmarshalable seed ever being
	// accepted by the endpoint.
	corrupt string
}

// faults reports whether a key in the control namespace is one this fault applies to.
func (m *ec2InstStateFaultState) faults(namespace, key string) bool {
	return namespace == ec2InstStateNS && strings.HasPrefix(key, m.failPrefix)
}

func (m *ec2InstStateFaultState) Get(ctx context.Context, namespace, key string) ([]byte, error) {
	if m.faults(namespace, key) {
		if m.getErr != nil {
			return nil, m.getErr
		}
		if m.corrupt != "" {
			return []byte(m.corrupt), nil
		}
	}
	return m.StateManager.Get(ctx, namespace, key)
}

func (m *ec2InstStateFaultState) Put(ctx context.Context, namespace, key string, value []byte) error {
	if m.faults(namespace, key) && m.putErr != nil {
		return m.putErr
	}
	return m.StateManager.Put(ctx, namespace, key, value)
}

func (m *ec2InstStateFaultState) Delete(ctx context.Context, namespace, key string) error {
	if m.faults(namespace, key) && m.deleteErr != nil {
		return m.deleteErr
	}
	return m.StateManager.Delete(ctx, namespace, key)
}

func (m *ec2InstStateFaultState) List(ctx context.Context, namespace, prefix string) ([]string, error) {
	if m.faults(namespace, prefix) && m.listErr != nil {
		return nil, m.listErr
	}
	return m.StateManager.List(ctx, namespace, prefix)
}

// TestEC2_InstanceState_AnUnreadableSeedLeavesTheDescribeAlone is the deliberate divergence from
// the snapshot precedent, asserted rather than left to the doc comment.
//
// `observeSnapshotStatus` propagates a state error; `observeInstanceState` returns the instance's
// own state instead, because it sits inside two describe loops that already skip an unreadable
// record. A seed store that cannot be read is substrate's own fault and not the caller's, and
// turning it into an error about every instance would lose the hundred instances the describe was
// asked for in order to report a control-plane problem the caller never asked about.
//
// Each case is a different point in the read: the seed itself unreadable, the seed unparseable,
// the counter unreadable, the counter unparseable. All four answer the settled state, which is
// also the answer an unseeded instance gets — so the failure mode is "the seed did not apply",
// never "the describe failed".
func TestEC2_InstanceState_AnUnreadableSeedLeavesTheDescribeAlone(t *testing.T) {
	t.Parallel()
	boom := errors.New("state unavailable")

	for _, tc := range []struct {
		name  string
		state *ec2InstStateFaultState
	}{
		{"the seed cannot be read", &ec2InstStateFaultState{
			failPrefix: ec2InstStateSeedKeys, getErr: boom}},
		{"the seed is not JSON", &ec2InstStateFaultState{
			failPrefix: ec2InstStateSeedKeys, corrupt: "{not-json"}},
		{"the counter cannot be read", &ec2InstStateFaultState{
			failPrefix: ec2InstStateCountKey, getErr: boom}},
		{"the counter is not JSON", &ec2InstStateFaultState{
			failPrefix: ec2InstStateCountKey, corrupt: "{not-json"}},
		{"the counter cannot be written", &ec2InstStateFaultState{
			failPrefix: ec2InstStateCountKey, putErr: boom}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.state.StateManager = emulator.NewMemoryStateManager()
			ts := newEC2TestServerWithState(t, tc.state)
			ec2SeedInstanceState(t, ts, `{"instanceId":"*","transientObservations":3}`)
			id := runInstance(t, ts, map[string]string{
				"ImageId": ec2TestImage, "InstanceType": "t3.micro",
			})

			assert.Equal(t, "running", instanceState(t, ts, id),
				"the describe answers the record's own state rather than failing")
			assert.Equal(t, "running", instanceState(t, ts, id),
				"and keeps answering it, so a poll loop terminates rather than hanging")
		})
	}
}

// TestEC2_InstanceState_AStateChangeThatCannotResetTheCountdownFails is the other half of that
// decision, and it goes the other way.
//
// [EC2Plugin.resetInstanceObservations] returns its error and all three state-changing operations
// propagate it, because a countdown that silently failed to restart would report a *settled* state
// for a transition the caller had seeded to be observable — a wrong answer, where an unreadable
// seed only costs the seed. So the operation fails rather than lying about the transition.
func TestEC2_InstanceState_AStateChangeThatCannotResetTheCountdownFails(t *testing.T) {
	t.Parallel()
	boom := errors.New("state unavailable")

	for _, action := range []string{"StopInstances", "StartInstances", "TerminateInstances"} {
		t.Run(action, func(t *testing.T) {
			t.Parallel()
			state := &ec2InstStateFaultState{
				StateManager: emulator.NewMemoryStateManager(),
				failPrefix:   ec2InstStateCountKey,
			}
			ts := newEC2TestServerWithState(t, state)
			id := runInstance(t, ts, map[string]string{
				"ImageId": ec2TestImage, "InstanceType": "t3.micro",
			})
			if action == "StartInstances" {
				resp := ec2Request(t, ts, map[string]string{"Action": "StopInstances", "InstanceId.1": id})
				require.Equal(t, http.StatusOK, resp.StatusCode)
				require.NoError(t, resp.Body.Close())
			}

			state.deleteErr = boom
			resp := ec2Request(t, ts, map[string]string{"Action": action, "InstanceId.1": id})
			defer resp.Body.Close() //nolint:errcheck
			assert.Equal(t, http.StatusInternalServerError, resp.StatusCode,
				"%s reports the failure rather than reporting a transition it cannot make observable", action)
		})
	}
}

// TestEC2_InstanceState_TheSeedEndpointReportsAStateFailure covers the control plane's own error
// paths, which are the endpoint's whole contract with a test author: a seed that was not stored
// must not answer 200, or the test that seeded it fails later and somewhere else.
//
// Five cases, because the two handlers touch state at five points: the seed write, the countdown
// reset that follows it (one key for an ID-scoped seed, a prefix sweep for the wildcard), the
// ID-scoped delete, and the two halves of the sweep a bare DELETE does.
func TestEC2_InstanceState_TheSeedEndpointReportsAStateFailure(t *testing.T) {
	t.Parallel()
	boom := errors.New("state unavailable")

	for _, tc := range []struct {
		name   string
		fault  ec2InstStateFaultState
		method string
		query  string
		body   string
	}{
		{
			name:   "the seed cannot be written",
			fault:  ec2InstStateFaultState{failPrefix: ec2InstStateSeedKeys, putErr: boom},
			method: http.MethodPost,
			body:   `{"instanceId":"i-0abc123","transientObservations":2}`,
		},
		{
			name:   "the countdown of one instance cannot be cleared",
			fault:  ec2InstStateFaultState{failPrefix: ec2InstStateCountKey, deleteErr: boom},
			method: http.MethodPost,
			body:   `{"instanceId":"i-0abc123","transientObservations":2}`,
		},
		{
			name:   "the countdowns a wildcard seed governs cannot be listed",
			fault:  ec2InstStateFaultState{failPrefix: ec2InstStateCountKey, listErr: boom},
			method: http.MethodPost,
			body:   `{"transientObservations":2}`,
		},
		{
			name:   "one seed cannot be deleted",
			fault:  ec2InstStateFaultState{failPrefix: ec2InstStateSeedKeys, deleteErr: boom},
			method: http.MethodDelete,
			query:  "?instanceId=i-0abc123",
		},
		{
			name:   "the countdown of the deleted seed cannot be cleared",
			fault:  ec2InstStateFaultState{failPrefix: ec2InstStateCountKey, deleteErr: boom},
			method: http.MethodDelete,
			query:  "?instanceId=i-0abc123",
		},
		{
			name:   "the seeds cannot be listed for a sweep",
			fault:  ec2InstStateFaultState{failPrefix: ec2InstStateSeedKeys, listErr: boom},
			method: http.MethodDelete,
		},
		{
			name:   "a listed seed cannot be deleted by the sweep",
			fault:  ec2InstStateFaultState{failPrefix: ec2InstStateSeedKeys, deleteErr: boom},
			method: http.MethodDelete,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := tc.fault
			fault.StateManager = emulator.NewMemoryStateManager()
			ts := newEC2TestServerWithState(t, &fault)

			// A seed to sweep, written before the fault is armed, so the sweep has something to
			// find — a List over an empty prefix returns no keys and reaches neither Delete.
			if tc.method == http.MethodDelete {
				armed := fault.deleteErr
				fault.deleteErr = nil
				ec2SeedInstanceState(t, ts, `{"instanceId":"i-0abc123","transientObservations":2}`)
				fault.deleteErr = armed
			}

			req, err := http.NewRequestWithContext(t.Context(), tc.method,
				ts.URL+"/v1/ec2/instance-state"+tc.query, strings.NewReader(tc.body))
			require.NoError(t, err)
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close() //nolint:errcheck
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			assert.Equal(t, http.StatusInternalServerError, resp.StatusCode,
				"a seed that was not stored must not answer 200: %s", body)
			assert.Contains(t, string(body), boom.Error(),
				"and the reason reaches the test author, who has no other way to see it")
		})
	}
}

// TestEC2_InstanceState_ASweepThatCannotDeleteACountdownIsReported is the one control-plane error
// path the table above cannot reach, because it needs a countdown to already exist: a wildcard
// seed clears the countdown of *every* instance by sweeping the prefix — no single key holds a
// wildcard seed's progress — so the delete it fails on is one the sweep itself found.
func TestEC2_InstanceState_ASweepThatCannotDeleteACountdownIsReported(t *testing.T) {
	t.Parallel()
	state := &ec2InstStateFaultState{
		StateManager: emulator.NewMemoryStateManager(),
		failPrefix:   ec2InstStateCountKey,
	}
	ts := newEC2TestServerWithState(t, state)

	ec2SeedInstanceState(t, ts, `{"instanceId":"*","transientObservations":2}`)
	id := runInstance(t, ts, map[string]string{"ImageId": ec2TestImage, "InstanceType": "t3.micro"})
	require.Equal(t, "pending", instanceState(t, ts, id), "one observation spent, so a counter exists")

	state.deleteErr = errors.New("state unavailable")
	resp, err := http.Post(ts.URL+"/v1/ec2/instance-state", "application/json",
		strings.NewReader(`{"instanceId":"*","transientObservations":4}`))
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode,
		"a re-seed that could not restart the countdowns must not answer 200: %s", body)
	assert.Contains(t, string(body), "state unavailable")
}

// TestEC2_InstanceState_ATransientRecordIsReportedAsItStands pins the guard that makes the
// "a seed never rewrites the record" rule safe against a record that breaks it.
//
// A stored state of `pending` is unreachable through the API — every write site settles — so this
// seeds the record directly. The point is what happens if the rule were ever broken: `pending` is
// not a *target*, so no published transition leads through it, and mapping it again would report
// `pending` for an instance whose record says it is already there while spending an observation on
// a countdown that could never end. It is reported as it stands instead, and the seed does not
// apply, so a describe loop still terminates.
func TestEC2_InstanceState_ATransientRecordIsReportedAsItStands(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	ts := newEC2TestServerWithState(t, state)

	ec2SeedInstanceState(t, ts, `{"instanceId":"*","transientObservations":3}`)
	id := runInstance(t, ts, map[string]string{"ImageId": ec2TestImage, "InstanceType": "t3.micro"})

	key := "instance:123456789012/us-east-1/" + id
	raw, err := state.Get(t.Context(), "ec2", key)
	require.NoError(t, err)
	require.NotNil(t, raw)
	var record map[string]any
	require.NoError(t, json.Unmarshal(raw, &record))
	record["state"] = map[string]any{"code": 0, "name": "pending"}
	patched, err := json.Marshal(record)
	require.NoError(t, err)
	require.NoError(t, state.Put(t.Context(), "ec2", key, patched))

	assert.Equal(t, "pending", instanceState(t, ts, id),
		"reported as stored, not mapped again")
	assert.Equal(t, "pending", instanceState(t, ts, id),
		"and stably so, rather than counting down toward a state it is already in")
}
