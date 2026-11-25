package emulator_test

import (
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The behaviors the #1619 re-review pinned: a re-termination reports the observed state without
// spending an observation, target state is the observed state, a delayed stop passes over an
// ended instance, an unreadable alarm fails the experiment, and a change set previews an
// ExperimentOptions change as a conditional replacement.

// seedInstanceState seeds an instance's transient-state countdown.
func (e *fisEnv) seedInstanceState(id string, observations int) {
	e.t.Helper()
	body, err := json.Marshal(map[string]any{"instanceId": id, "transientObservations": observations})
	require.NoError(e.t, err)
	status, out := e.do(http.MethodPost, "localhost", "/v1/ec2/instance-state", "application/json", body)
	require.Equal(e.t, http.StatusOK, status, "%s", out)
}

// terminate sends TerminateInstances and returns the current and previous states it reports.
func (e *fisEnv) terminate(id string) (string, string) {
	e.t.Helper()
	var out struct {
		Items []struct {
			Current  string `xml:"currentState>name"`
			Previous string `xml:"previousState>name"`
		} `xml:"instancesSet>item"`
	}
	e.ec2(map[string]string{"Action": "TerminateInstances", "InstanceId.1": id}, &out)
	require.Len(e.t, out.Items, 1)
	return out.Items[0].Current, out.Items[0].Previous
}

// A second TerminateInstances during a seeded shutting-down countdown reports shutting-down for
// both states, as a describe would, and spends none of the countdown.
func TestFIS_AReTerminationReportsTheObservedState(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t)
	id := e.run(false, "", "", nil)
	e.seedInstanceState(id, 2)
	cur, prev := e.terminate(id)
	assert.Equal(t, "shutting-down", cur)
	assert.Equal(t, "running", prev)

	cur, prev = e.terminate(id)
	assert.Equal(t, "shutting-down", cur, "the countdown is still running")
	assert.Equal(t, "shutting-down", prev)

	assert.Equal(t, "shutting-down", e.instance(id).State, "the re-termination spent no observation")
	assert.Equal(t, "shutting-down", e.instance(id).State)
	assert.Equal(t, "terminated", e.instance(id).State)
	cur, prev = e.terminate(id)
	assert.Equal(t, "terminated", cur)
	assert.Equal(t, "terminated", prev)
}

// A State.Name filter matches the state a describe would report: shutting-down during a seeded
// countdown, which is also an ended state no other target selects.
func TestFIS_TargetStateIsTheObservedState(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t)
	tagged := map[string]string{"fis": "target"}
	ending := e.run(false, "", "", tagged)
	live := e.run(false, "", "", tagged)
	e.seedInstanceState(ending, 5)
	e.terminate(ending)

	resolve := func(filters []any) []string {
		t.Helper()
		tg := instanceTarget("ALL")
		if filters != nil {
			tg["filters"] = filters
		}
		id := e.template(map[string]any{
			"targets":           map[string]any{"t": tg},
			"experimentOptions": map[string]string{"emptyTargetResolutionMode": "skip"},
			"actions": map[string]any{"reboot": map[string]any{
				"actionId": "aws:ec2:reboot-instances", "targets": map[string]string{"Instances": "t"},
			}},
		})
		x := e.start(id)
		var out struct {
			ResolvedTargets []struct {
				TargetInformation map[string]string `json:"targetInformation"`
			} `json:"resolvedTargets"`
		}
		e.mustFIS(http.MethodGet, "/experiments/"+x.ID+"/resolvedTargets", nil, &out)
		var arns []string
		for _, r := range out.ResolvedTargets {
			arns = append(arns, r.TargetInformation["arn"])
		}
		return arns
	}
	arn := func(id string) string { return "arn:aws:ec2:us-east-1:123456789012:instance/" + id }
	assert.Equal(t, []string{arn(live)}, resolve(nil), "a shutting-down instance is not a candidate")
	assert.Equal(t, []string{arn(ending)},
		resolve([]any{map[string]any{"path": "State.Name", "values": []string{"shutting-down"}}}))
	assert.Equal(t, "shutting-down", e.instance(ending).State, "resolution spent none of the countdown")
}

// An instance terminated between resolution and a delayed stop action is passed over; the
// action stops the others and completes.
func TestFIS_ADelayedStopPassesOverAnEndedInstance(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t)
	tagged := map[string]string{"fis": "target"}
	gone := e.run(false, "", "", tagged)
	kept := e.run(false, "", "", tagged)
	id := e.template(map[string]any{
		"targets": map[string]any{"all": instanceTarget("ALL")},
		"actions": map[string]any{
			"wait": map[string]any{"actionId": "aws:fis:wait", "parameters": map[string]string{"duration": "PT1M"}},
			"stop": map[string]any{"actionId": "aws:ec2:stop-instances", "targets": map[string]string{"Instances": "all"},
				"startAfter": []string{"wait"}},
		},
	})
	x := e.start(id)
	e.experiment(x.ID)
	e.terminate(gone)
	e.ts.AdvanceTime(time.Minute)
	got := e.experiment(x.ID)
	assert.Equal(t, "completed", got.State.Status, "%s", got.State.Reason)
	assert.Equal(t, "stopped", e.instance(kept).State)
	assert.Equal(t, "terminated", e.instance(gone).State)
}

// An alarm substrate cannot read at all — here, no CloudWatch to read it through — fails the
// experiment with a reason, rather than stalling every later observation.
func TestFIS_AnUnreadableAlarmFailsTheExperiment(t *testing.T) {
	t.Parallel()
	p := &emulator.FISPlugin{}
	ctx, _ := wireSetup(t, p, "req-fis-alarm")
	call := func(method, path string, body map[string]any) []byte {
		return wireREST(t, p, ctx, "fis", method, path, body)
	}
	var tmpl struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}
	require.NoError(t, json.Unmarshal(call(http.MethodPost, "/experimentTemplates", map[string]any{
		"description": "d", "roleArn": fisRoleARN,
		"stopConditions": []any{map[string]any{"source": "aws:cloudwatch:alarm",
			"value": "arn:aws:cloudwatch:us-east-1:123456789012:alarm:guard"}},
		"actions": map[string]any{"w": fisWaitAction()},
	}), &tmpl))
	var started struct {
		Experiment fisExperimentOut `json:"experiment"`
	}
	require.NoError(t, json.Unmarshal(call(http.MethodPost, "/experiments",
		map[string]any{"experimentTemplateId": tmpl.ExperimentTemplate.ID}), &started))
	require.NoError(t, p.RunDue(ctx, nil))
	var got struct {
		Experiment fisExperimentOut `json:"experiment"`
	}
	require.NoError(t, json.Unmarshal(call(http.MethodGet, "/experiments/"+started.Experiment.ID, nil), &got))
	assert.Equal(t, "failed", got.Experiment.State.Status)
	assert.Contains(t, got.Experiment.State.Reason, "could not be read")
	require.NoError(t, p.RunDue(ctx, nil), "a failed experiment leaves nothing due")
}

// A change set previews an ExperimentOptions change as a conditional replacement, since only an
// AccountTargeting change replaces the template.
func TestCFNFISExperimentTemplate_ChangeSetPreviewsAConditionalReplacement(t *testing.T) {
	tmpl := func(mode string) string {
		return `{"Resources":{"Template":{"Type":"AWS::FIS::ExperimentTemplate","Properties":{
			"Description":"d","RoleArn":"arn:aws:iam::123456789012:role/aegc-fis",
			"StopConditions":[{"Source":"none"}],"Targets":{},
			"ExperimentOptions":{"AccountTargeting":"single-account","EmptyTargetResolutionMode":"` + mode + `"}}}}}`
	}
	t.Parallel()
	e := newFISEnv(t)
	cfn := func(params map[string]string) []byte {
		t.Helper()
		form := url.Values{"Version": {"2010-05-15"}}
		for k, v := range params {
			form.Set(k, v)
		}
		status, out := e.do(http.MethodPost, "cloudformation.us-east-1.amazonaws.com", "/",
			"application/x-www-form-urlencoded", []byte(form.Encode()))
		require.Equal(t, http.StatusOK, status, "%s: %s", params["Action"], out)
		return out
	}
	cfn(map[string]string{"Action": "CreateStack", "StackName": "fis-cs", "TemplateBody": tmpl("fail")})
	cfn(map[string]string{"Action": "CreateChangeSet", "StackName": "fis-cs", "ChangeSetName": "cs",
		"ChangeSetType": "UPDATE", "TemplateBody": tmpl("skip")})
	var doc struct {
		Changes []cfnDescribedChange `xml:"DescribeChangeSetResult>Changes>member"`
	}
	body := cfn(map[string]string{"Action": "DescribeChangeSet", "StackName": "fis-cs", "ChangeSetName": "cs"})
	require.NoError(t, xml.Unmarshal(body, &doc), "%s", body)
	require.Len(t, doc.Changes, 1, "%s", body)
	change := doc.Changes[0]
	assert.Equal(t, "Modify", change.Action)
	assert.Equal(t, "Conditional", change.Replacement)
	require.NotEmpty(t, change.Details)
	assert.Equal(t, "ExperimentOptions", change.Details[0].Name)
	assert.Equal(t, "Conditionally", change.Details[0].RequiresRecreation)
}
