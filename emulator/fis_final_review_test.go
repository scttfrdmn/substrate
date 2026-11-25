package emulator_test

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The behaviors the #1619 final review pinned: a retried termination repairs a half-finished
// settlement, an FIS stop never writes back an instance a concurrent termination ended, an
// interruption's stop disables the Spot request and a start re-activates it, and a stop action
// leaves an instance that was already stopped alone.

// volumeListFaultState fails a List of EC2 volume records while armed.
type volumeListFaultState struct {
	emulator.StateManager
	armed atomic.Bool
}

func (s *volumeListFaultState) List(ctx context.Context, namespace, prefix string) ([]string, error) {
	if s.armed.Load() && namespace == "ec2" && strings.HasPrefix(prefix, "volume:") {
		return nil, errInjectedStore
	}
	return s.StateManager.List(ctx, namespace, prefix)
}

// ec2Query sends one EC2 request through the registry and returns its body.
func (e *fisFaultEnv) ec2Query(params map[string]string) string {
	e.t.Helper()
	resp, err := e.registry.RouteRequest(e.ctx(), &emulator.AWSRequest{Service: "ec2", Operation: params["Action"],
		Params: params, Headers: map[string]string{}})
	require.NoError(e.t, err, "%s", params["Action"])
	return string(resp.Body)
}

// xmlAfter returns body from the first element named element on, so a lookup inside it skips
// same-named elements before it.
func xmlAfter(body, element string) string {
	if i := strings.Index(body, "<"+element+">"); i >= 0 {
		return body[i:]
	}
	return ""
}

// xmlValue returns the first value of an XML element in body.
func xmlValue(body, element string) string {
	i := strings.Index(body, "<"+element+">")
	if i < 0 {
		return ""
	}
	rest := body[i+len(element)+2:]
	return rest[:strings.Index(rest, "<")]
}

// A Spot interruption whose volume settlement fails leaves the instance terminated and the rest
// undone; a later TerminateInstances settles the volumes and the Spot request, and keeps the
// interruption's reason and status.
func TestFIS_ARetriedTerminationRepairsAHalfSettledInterruption(t *testing.T) {
	t.Parallel()
	for _, cancel := range []bool{false, true} {
		t.Run(map[bool]string{false: "fulfilled", true: "cancelled"}[cancel], func(t *testing.T) {
			t.Parallel()
			fisRetriedTermination(t, cancel)
		})
	}
}

// fisRetriedTermination runs the half-settled interruption, its request cancelled first (and so
// cancelled with the instance running) when cancel is set.
func fisRetriedTermination(t *testing.T, cancel bool) {
	state := &volumeListFaultState{StateManager: emulator.NewMemoryStateManager()}
	e := newFISFaultEnvOver(t, state)
	id := e.ec2Run(true, false)
	describe := e.ec2Query(map[string]string{"Action": "DescribeInstances", "InstanceId.1": id})
	request := xmlValue(describe, "spotInstanceRequestId")
	require.NotEmpty(t, request)
	wantUnsettled, wantSettled := "active", "closed"
	if cancel {
		e.ec2Query(map[string]string{"Action": "CancelSpotInstanceRequests", "SpotInstanceRequestId.1": request})
		wantUnsettled, wantSettled = "cancelled", "cancelled"
	}
	volumes := func() int {
		body := e.ec2Query(map[string]string{"Action": "DescribeVolumes", "Filter.1.Name": "attachment.instance-id",
			"Filter.1.Value.1": id})
		return strings.Count(body, "<createTime>")
	}
	require.Equal(t, 1, volumes(), "the launch attached a root volume")

	spot := map[string]any{"resourceType": "aws:ec2:spot-instance", "resourceTags": map[string]string{"fis": "target"},
		"selectionMode": "ALL"}
	var created struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}
	e.must(http.MethodPost, "/experimentTemplates", faultTemplate(spot, map[string]any{
		"actionId": "aws:ec2:send-spot-instance-interruptions", "targets": map[string]string{"SpotInstances": "t"},
		"parameters": map[string]string{"durationBeforeInterruption": "PT2M"}}), &created)
	e.must(http.MethodPost, "/experiments", map[string]any{"experimentTemplateId": created.ExperimentTemplate.ID}, nil)
	require.NoError(t, e.fis.RunDue(e.ctx(), nil))
	e.tc.SetTime(fisClockInstant.Add(2 * time.Minute))
	state.armed.Store(true)
	require.NoError(t, e.fis.RunDue(e.ctx(), nil))
	state.armed.Store(false)

	describe = e.ec2Query(map[string]string{"Action": "DescribeInstances", "InstanceId.1": id})
	require.Equal(t, "terminated", xmlValue(describe, "name"), "the record was written before the failure")
	require.Equal(t, "Server.SpotInstanceTermination", xmlValue(xmlAfter(describe, "stateReason"), "code"))
	spotState := e.ec2Query(map[string]string{"Action": "DescribeSpotInstanceRequests", "SpotInstanceRequestId.1": request})
	require.Equal(t, wantUnsettled, xmlValue(spotState, "state"), "the failure left the request unsettled")
	require.Equal(t, 1, volumes(), "the failure left the volume attached")

	e.tc.SetTime(fisClockInstant.Add(5 * time.Minute))
	e.ec2Query(map[string]string{"Action": "TerminateInstances", "InstanceId.1": id})
	assert.Zero(t, volumes(), "the retry settled the volume")
	spotState = e.ec2Query(map[string]string{"Action": "DescribeSpotInstanceRequests", "SpotInstanceRequestId.1": request})
	assert.Equal(t, wantSettled, xmlValue(spotState, "state"))
	assert.Equal(t, "instance-terminated-by-experiment", xmlValue(xmlAfter(spotState, "status"), "code"), "the interruption's status")
	assert.Equal(t, "2026-11-24T10:02:00Z", xmlValue(spotState, "updateTime"), "at the termination's time")
	describe = e.ec2Query(map[string]string{"Action": "DescribeInstances", "InstanceId.1": id})
	assert.Equal(t, "Server.SpotInstanceTermination", xmlValue(xmlAfter(describe, "stateReason"), "code"), "the interruption's reason is intact")

	// A settled request is not settled again.
	e.ec2Query(map[string]string{"Action": "TerminateInstances", "InstanceId.1": id})
	again := e.ec2Query(map[string]string{"Action": "DescribeSpotInstanceRequests", "SpotInstanceRequestId.1": request})
	assert.Equal(t, xmlAfter(spotState, "spotInstanceRequestSet"), xmlAfter(again, "spotInstanceRequestSet"))
}

// An FIS stop racing a caller's TerminateInstances never leaves the instance anything but
// terminated: the stop reads the record under the lock the termination holds.
func TestFIS_AStopRacingATerminationNeverResurrects(t *testing.T) {
	t.Parallel()
	for i := range 20 {
		e := newFISFaultEnvOver(t, emulator.NewMemoryStateManager())
		id := e.ec2Run(false, false)
		target := map[string]any{"resourceType": "aws:ec2:instance", "resourceTags": map[string]string{"fis": "target"},
			"selectionMode": "ALL"}
		var created struct {
			ExperimentTemplate struct {
				ID string `json:"id"`
			} `json:"experimentTemplate"`
		}
		e.must(http.MethodPost, "/experimentTemplates", faultTemplate(target, map[string]any{
			"actionId": "aws:ec2:stop-instances", "targets": map[string]string{"Instances": "t"}}), &created)
		e.must(http.MethodPost, "/experiments", map[string]any{"experimentTemplateId": created.ExperimentTemplate.ID}, nil)
		runCtx, termCtx := e.ctx(), e.ctx()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			assert.NoError(t, e.fis.RunDue(runCtx, nil))
		}()
		go func() {
			defer wg.Done()
			_, err := e.registry.RouteRequest(termCtx, &emulator.AWSRequest{Service: "ec2", Operation: "TerminateInstances",
				Params: map[string]string{"Action": "TerminateInstances", "InstanceId.1": id}, Headers: map[string]string{}})
			assert.NoError(t, err)
		}()
		wg.Wait()
		describe := e.ec2Query(map[string]string{"Action": "DescribeInstances", "InstanceId.1": id})
		require.Equal(t, "terminated", xmlValue(describe, "name"), "iteration %d", i)
	}
}

// An interruption that stops a persistent request's instance disables the request, as the Spot
// request lifecycle lists instance-stopped-by-experiment; starting the instance re-activates it.
func TestFIS_AStopInterruptionDisablesTheSpotRequest(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t)
	id := e.run(true, "persistent", "stop", map[string]string{"fis": "target"})
	e.start(e.spotInterruptTemplate("ALL"))
	e.ts.AdvanceTime(2 * time.Minute)
	inst := e.instance(id)
	require.Equal(t, "stopped", inst.State)
	state, status := e.spotRequest(inst.SpotRequestID)
	assert.Equal(t, "disabled", state)
	assert.Equal(t, "instance-stopped-by-experiment", status)

	e.ec2(map[string]string{"Action": "StartInstances", "InstanceId.1": id}, nil)
	state, status = e.spotRequest(inst.SpotRequestID)
	assert.Equal(t, "active", state)
	assert.Equal(t, "fulfilled", status)
}

// A stop action passes over an instance that was already stopped: its reasons are not rewritten,
// and the action's restart does not start an instance a caller stopped.
func TestFIS_AStopActionLeavesAnAlreadyStoppedInstanceAlone(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t)
	tagged := map[string]string{"fis": "target"}
	parked := e.run(false, "", "", tagged)
	live := e.run(false, "", "", tagged)
	e.ec2(map[string]string{"Action": "StopInstances", "InstanceId.1": parked}, nil)
	id := e.template(map[string]any{
		"targets": map[string]any{"all": instanceTarget("ALL")},
		"actions": map[string]any{"stop": map[string]any{
			"actionId": "aws:ec2:stop-instances", "targets": map[string]string{"Instances": "all"},
			"parameters": map[string]string{"startInstancesAfterDuration": "PT1M"},
		}},
	})
	x := e.start(id)
	e.experiment(x.ID)
	assert.Empty(t, e.instance(parked).ReasonCode, "the caller's stop is the most recent transition")
	assert.Equal(t, "Client.UserInitiatedShutdown", e.instance(live).ReasonCode)
	e.ts.AdvanceTime(time.Minute)
	assert.Equal(t, "completed", e.experiment(x.ID).State.Status)
	assert.Equal(t, "stopped", e.instance(parked).State, "the restart does not start what FIS did not stop")
	assert.Equal(t, "running", e.instance(live).State)
}
