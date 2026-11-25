package emulator_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The behaviors the #1619 review pinned: a re-termination keeps the interruption's record,
// ended instances are not candidates, stop-condition alarms stay in the template's account and
// Region, and CloudFormation clears removed properties and replaces on an AccountTargeting change.

// spotInterruptTemplate is a template interrupting every tagged running Spot Instance after PT2M.
func (e *fisEnv) spotInterruptTemplate(selection string) string {
	e.t.Helper()
	return e.template(map[string]any{
		"targets": map[string]any{"spot": spotTarget(selection)},
		"actions": map[string]any{"interrupt": map[string]any{
			"actionId":   "aws:ec2:send-spot-instance-interruptions",
			"parameters": map[string]string{"durationBeforeInterruption": "PT2M"},
			"targets":    map[string]string{"SpotInstances": "spot"},
		}},
	})
}

// A later TerminateInstances of an instance an interruption terminated changes nothing, and the
// interruption's times are its due time, not the observation that applied it.
func TestFIS_AReTerminationKeepsTheInterruptionsRecord(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t)
	id := e.run(true, "one-time", "terminate", map[string]string{"fis": "target"})
	x := e.start(e.spotInterruptTemplate("ALL"))
	e.experiment(x.ID)
	// The clock jumps well past the interruption before anything observes it.
	e.ts.AdvanceTime(10 * time.Minute)
	require.Equal(t, "completed", e.experiment(x.ID).State.Status)
	before := e.instance(id)
	require.Equal(t, "Server.SpotInstanceTermination", before.ReasonCode)
	assert.Equal(t, "Service initiated (2026-11-24 10:02:00 GMT)", before.Reason, "the reason names the due time")
	var req struct {
		Requests []struct {
			Status     string `xml:"status>code"`
			UpdateTime string `xml:"status>updateTime"`
		} `xml:"spotInstanceRequestSet>item"`
	}
	e.ec2(map[string]string{"Action": "DescribeSpotInstanceRequests", "SpotInstanceRequestId.1": before.SpotRequestID}, &req)
	require.Len(t, req.Requests, 1)
	assert.Equal(t, "2026-11-24T10:02:00Z", req.Requests[0].UpdateTime, "the request's status time is the due time")

	var out struct {
		Items []struct {
			Current  string `xml:"currentState>name"`
			Previous string `xml:"previousState>name"`
		} `xml:"instancesSet>item"`
	}
	e.ec2(map[string]string{"Action": "TerminateInstances", "InstanceId.1": id}, &out)
	require.Len(t, out.Items, 1)
	assert.Equal(t, "terminated", out.Items[0].Current)
	assert.Equal(t, "terminated", out.Items[0].Previous)
	after := e.instance(id)
	assert.Equal(t, before, after, "the re-termination leaves the record as the interruption settled it")
	e.ec2(map[string]string{"Action": "DescribeSpotInstanceRequests", "SpotInstanceRequestId.1": before.SpotRequestID}, &req)
	assert.Equal(t, "instance-terminated-by-experiment", req.Requests[0].Status)
}

// An instance a first experiment terminated is not a candidate for the next: the next selects
// the live instance, unless a State.Name filter asks for terminated ones.
func TestFIS_AnEndedInstanceIsNotACandidate(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t)
	tagged := map[string]string{"fis": "target"}
	first := e.run(false, "", "", tagged)
	terminate := e.template(map[string]any{
		"targets": map[string]any{"one": instanceTarget("COUNT(1)")},
		"actions": map[string]any{"terminate": map[string]any{
			"actionId": "aws:ec2:terminate-instances", "targets": map[string]string{"Instances": "one"},
		}},
	})
	e.experiment(e.start(terminate).ID)
	require.Equal(t, "terminated", e.instance(first).State)

	second := e.run(false, "", "", tagged)
	x := e.start(terminate)
	assert.Equal(t, "completed", e.experiment(x.ID).State.Status)
	assert.Equal(t, "terminated", e.instance(second).State, "the second run selected the live instance")

	tg := instanceTarget("ALL")
	tg["filters"] = []any{map[string]any{"path": "State.Name", "values": []string{"terminated"}}}
	asked := e.template(map[string]any{
		"targets": map[string]any{"dead": tg},
		"actions": map[string]any{"reboot": map[string]any{
			"actionId": "aws:ec2:reboot-instances", "targets": map[string]string{"Instances": "dead"},
		}},
	})
	y := e.start(asked)
	var resolved struct {
		ResolvedTargets []struct{} `json:"resolvedTargets"`
	}
	e.mustFIS(http.MethodGet, "/experiments/"+y.ID+"/resolvedTargets", nil, &resolved)
	assert.Len(t, resolved.ResolvedTargets, 2, "a State.Name filter naming terminated selects both")
}

// A stop-condition alarm must be a well-formed alarm ARN in the template's account and Region.
func TestFIS_StopConditionAlarmsStayInTheTemplatesAccountAndRegion(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t)
	for _, arn := range []string{
		"arn:aws:cloudwatch:us-west-2:123456789012:alarm:guard",
		"arn:aws:cloudwatch:us-east-1:210987654321:alarm:guard",
		"arn:aws:cloudwatch:us-east-1:123456789012:alarm:",
		"arn:aws:cloudwatch:nowhere:123456789012:alarm:guard",
		"arn:aws:cloudwatch:us-east-1:1234:alarm:guard",
	} {
		body := fisValidTemplate()
		delete(body, "clientToken")
		body["stopConditions"] = []any{map[string]any{"source": "aws:cloudwatch:alarm", "value": arn}}
		status, out := e.fis(http.MethodPost, "/experimentTemplates", body)
		assert.Equal(t, http.StatusBadRequest, status, "%s: %s", arn, out)
		assert.Contains(t, string(out), "ValidationException")
	}
	id := e.template(map[string]any{"actions": map[string]any{"w": fisWaitAction()}})
	status, out := e.fis(http.MethodPatch, "/experimentTemplates/"+id, map[string]any{
		"stopConditions": []any{map[string]any{"source": "aws:cloudwatch:alarm",
			"value": "arn:aws:cloudwatch:us-west-2:123456789012:alarm:guard"}},
	})
	assert.Equal(t, http.StatusBadRequest, status, "%s", out)
	assert.Contains(t, string(out), "account and Region")

	// An alarm that does not exist never stops the experiment, and never stalls it.
	missing := e.template(map[string]any{
		"stopConditions": []any{map[string]any{"source": "aws:cloudwatch:alarm",
			"value": "arn:aws:cloudwatch:us-east-1:123456789012:alarm:never-created"}},
		"actions": map[string]any{"w": fisWaitAction()},
	})
	x := e.start(missing)
	assert.Equal(t, "running", e.experiment(x.ID).State.Status)
	e.ts.AdvanceTime(time.Minute)
	assert.Equal(t, "completed", e.experiment(x.ID).State.Status)
}

// A template with no actions is accepted, and its experiments complete at once.
func TestFIS_ATemplateWithNoActions(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t)
	id := e.template(map[string]any{"actions": map[string]any{}})
	assert.Equal(t, "completed", e.experiment(e.start(id).ID).State.Status)
}

// CloudFormation: a removed optional property is cleared, an undeclared Actions is {}, and an
// AccountTargeting change replaces the template.
func TestCFNFISExperimentTemplate_RemovalsAndReplacement(t *testing.T) {
	t.Parallel()
	d, _, _, _ := newSweepDeployer(t)
	const base = `"Description":"d","RoleArn":"arn:aws:iam::123456789012:role/aegc-fis",
		"StopConditions":[{"Source":"none"}],"Targets":{}`
	tmpl := func(extra string) string {
		return `{"Resources":{"Template":{"Type":"AWS::FIS::ExperimentTemplate","Properties":{` + base + extra + `}}}}`
	}
	type got struct {
		ExperimentTemplate struct {
			Actions           map[string]any    `json:"actions"`
			LogConfiguration  map[string]any    `json:"logConfiguration"`
			ExperimentOptions map[string]string `json:"experimentOptions"`
		} `json:"experimentTemplate"`
	}
	read := func(id string) got {
		var out got
		require.Equal(t, http.StatusOK, v129REST(t, d, "fis", http.MethodGet, "/experimentTemplates/"+id, nil, &out))
		return out
	}

	withAll := tmpl(`,"LogConfiguration":{"LogSchemaVersion":2,
		"CloudWatchLogsConfiguration":{"LogGroupArn":"arn:aws:logs:us-east-1:123456789012:log-group:fis:*"}},
		"ExperimentOptions":{"AccountTargeting":"single-account","EmptyTargetResolutionMode":"skip"}`)
	created, updated := v129Update(t, d, "fis-removals", withAll, tmpl(`,"ExperimentOptions":{"AccountTargeting":"single-account"}`))
	id := resourceByLogicalID(t, created, "Template").PhysicalID
	require.NotEmpty(t, id)
	assert.Empty(t, resourceByLogicalID(t, created, "Template").Error)
	assert.Equal(t, id, resourceByLogicalID(t, updated, "Template").PhysicalID, "updated in place")
	cur := read(id)
	assert.Equal(t, map[string]any{}, cur.ExperimentTemplate.Actions, "an undeclared Actions is {}")
	assert.Empty(t, cur.ExperimentTemplate.LogConfiguration, "the removed LogConfiguration is cleared")
	assert.Empty(t, cur.ExperimentTemplate.ExperimentOptions["emptyTargetResolutionMode"], "the removed mode is cleared")

	_, replaced := v129Update(t, d, "fis-replace", tmpl(`,"ExperimentOptions":{"AccountTargeting":"single-account"}`),
		tmpl(`,"ExperimentOptions":{"AccountTargeting":"multi-account"}`))
	newID := resourceByLogicalID(t, replaced, "Template").PhysicalID
	require.NotEmpty(t, newID)
	assert.Empty(t, resourceByLogicalID(t, replaced, "Template").Error)
	assert.Equal(t, "multi-account", read(newID).ExperimentTemplate.ExperimentOptions["accountTargeting"])
	list := struct {
		ExperimentTemplates []struct {
			ID string `json:"id"`
		} `json:"experimentTemplates"`
	}{}
	v129REST(t, d, "fis", http.MethodGet, "/experimentTemplates", nil, &list)
	var ids []string
	for _, tpl := range list.ExperimentTemplates {
		ids = append(ids, tpl.ID)
	}
	assert.ElementsMatch(t, []string{id, newID}, ids, "the replaced template is deleted")
}
