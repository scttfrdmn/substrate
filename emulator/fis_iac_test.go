package emulator_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/scttfrdmn/substrate/emulator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Terraform's aws_fis_experiment_template and CloudFormation's AWS::FIS::ExperimentTemplate
// (#1619).

// TestFIS_TerraformExperimentTemplateLifecycle sends the requests the provider sends for create,
// read, an in-place update with a tag change, and destroy, with the members the provider sets.
func TestFIS_TerraformExperimentTemplateLifecycle(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t)
	create := map[string]any{
		"clientToken": "terraform-20261124100000000000000001",
		"description": "aegc spot interruption",
		"roleArn":     fisRoleARN,
		"stopConditions": []any{
			map[string]any{"source": "none"},
		},
		"targets": map[string]any{"spot-workers": map[string]any{
			"resourceType": "aws:ec2:spot-instance", "selectionMode": "PERCENT(50)",
			"resourceTags": map[string]string{"aegc:role": "worker"},
		}},
		"actions": map[string]any{"interrupt": map[string]any{
			"actionId": "aws:ec2:send-spot-instance-interruptions", "description": "interrupt",
			"parameters": map[string]string{"durationBeforeInterruption": "PT2M"},
			"targets":    map[string]string{"SpotInstances": "spot-workers"},
		}},
		"experimentOptions": map[string]string{"accountTargeting": "single-account", "emptyTargetResolutionMode": "fail"},
		"logConfiguration": map[string]any{
			"logSchemaVersion":            2,
			"cloudWatchLogsConfiguration": map[string]string{"logGroupArn": "arn:aws:logs:us-east-1:123456789012:log-group:fis:*"},
		},
		"tags": map[string]string{"Name": "aegc"},
	}
	var created struct {
		ExperimentTemplate map[string]any `json:"experimentTemplate"`
	}
	e.mustFIS(http.MethodPost, "/experimentTemplates", create, &created)
	id := created.ExperimentTemplate["id"].(string)
	arn := created.ExperimentTemplate["arn"].(string)

	// Read: every member the provider sets in state comes back as sent.
	var read struct {
		ExperimentTemplate struct {
			Description string `json:"description"`
			RoleArn     string `json:"roleArn"`
			Targets     map[string]struct {
				ResourceType  string            `json:"resourceType"`
				ResourceTags  map[string]string `json:"resourceTags"`
				SelectionMode string            `json:"selectionMode"`
			} `json:"targets"`
			Actions map[string]struct {
				ActionID    string            `json:"actionId"`
				Description string            `json:"description"`
				Parameters  map[string]string `json:"parameters"`
				Targets     map[string]string `json:"targets"`
			} `json:"actions"`
			StopConditions []struct {
				Source string `json:"source"`
			} `json:"stopConditions"`
			LogConfiguration struct {
				LogSchemaVersion int `json:"logSchemaVersion"`
			} `json:"logConfiguration"`
			ExperimentOptions map[string]string `json:"experimentOptions"`
			Tags              map[string]string `json:"tags"`
		} `json:"experimentTemplate"`
	}
	e.mustFIS(http.MethodGet, "/experimentTemplates/"+id, nil, &read)
	got := read.ExperimentTemplate
	assert.Equal(t, "aegc spot interruption", got.Description)
	assert.Equal(t, "PERCENT(50)", got.Targets["spot-workers"].SelectionMode)
	assert.Equal(t, map[string]string{"aegc:role": "worker"}, got.Targets["spot-workers"].ResourceTags)
	assert.Equal(t, "PT2M", got.Actions["interrupt"].Parameters["durationBeforeInterruption"])
	assert.Equal(t, "spot-workers", got.Actions["interrupt"].Targets["SpotInstances"])
	assert.Equal(t, "none", got.StopConditions[0].Source)
	assert.Equal(t, 2, got.LogConfiguration.LogSchemaVersion)
	assert.Equal(t, "fail", got.ExperimentOptions["emptyTargetResolutionMode"])
	assert.Equal(t, map[string]string{"Name": "aegc"}, got.Tags)

	// Update: the provider sends the whole configuration, and the tag change separately.
	update := map[string]any{
		"description": "aegc spot interruption v2", "roleArn": fisRoleARN, "stopConditions": create["stopConditions"],
		"targets": create["targets"], "actions": create["actions"],
		"experimentOptions": map[string]string{"emptyTargetResolutionMode": "skip"},
		"logConfiguration":  create["logConfiguration"],
	}
	e.mustFIS(http.MethodPatch, "/experimentTemplates/"+id, update, nil)
	e.mustFIS(http.MethodDelete, "/tags/"+arn+"?tagKeys=Name", nil, nil)
	e.mustFIS(http.MethodPost, "/tags/"+arn, map[string]any{"tags": map[string]string{"Owner": "aegc"}}, nil)
	// A fresh read: decoding into the maps the first read filled would merge into them.
	read.ExperimentTemplate.Tags, read.ExperimentTemplate.ExperimentOptions = nil, nil
	e.mustFIS(http.MethodGet, "/experimentTemplates/"+id, nil, &read)
	assert.Equal(t, "aegc spot interruption v2", read.ExperimentTemplate.Description)
	assert.Equal(t, "skip", read.ExperimentTemplate.ExperimentOptions["emptyTargetResolutionMode"])
	assert.Equal(t, map[string]string{"Owner": "aegc"}, read.ExperimentTemplate.Tags)

	// Destroy, then the post-destroy read the provider treats as gone.
	e.mustFIS(http.MethodDelete, "/experimentTemplates/"+id, nil, nil)
	status, out := e.fis(http.MethodGet, "/experimentTemplates/"+id, nil)
	assert.Equal(t, http.StatusNotFound, status)
	assert.Contains(t, string(out), "ResourceNotFoundException")
}

// TestCFNFISExperimentTemplate_DeployUpdateDelete covers AWS::FIS::ExperimentTemplate's create,
// its in-place update with a tag change, and the stack delete.
func TestCFNFISExperimentTemplate_DeployUpdateDelete(t *testing.T) {
	t.Parallel()
	d, _, _, _ := newSweepDeployer(t)
	ctx := context.Background()
	tmpl := func(description, tag string) string {
		return `{"Resources":{"Template":{"Type":"AWS::FIS::ExperimentTemplate","Properties":{
			"Description":"` + description + `",
			"RoleArn":"arn:aws:iam::123456789012:role/aegc-fis",
			"StopConditions":[{"Source":"none"}],
			"Targets":{"SpotWorkers":{"ResourceType":"aws:ec2:spot-instance","SelectionMode":"COUNT(1)",
				"ResourceTags":{"aegc:Role":"worker"},
				"Filters":[{"Path":"State.Name","Values":["running"]}]}},
			"Actions":{"Interrupt":{"ActionId":"aws:ec2:send-spot-instance-interruptions",
				"Parameters":{"durationBeforeInterruption":"PT2M"},
				"Targets":{"SpotInstances":"SpotWorkers"}}},
			"ExperimentOptions":{"AccountTargeting":"single-account","EmptyTargetResolutionMode":"skip"},
			"Tags":{"` + tag + `":"v"}}}},
			"Outputs":{"Id":{"Value":{"Fn::GetAtt":["Template","Id"]}}}}`
	}
	created, updated := v129Update(t, d, "fis", tmpl("first", "Team"), tmpl("second", "Owner"))
	res := resourceByLogicalID(t, created, "Template")
	require.Empty(t, res.Error)
	require.True(t, strings.HasPrefix(res.PhysicalID, "EXT"), res.PhysicalID)
	assert.Equal(t, res.PhysicalID, resourceByLogicalID(t, updated, "Template").PhysicalID, "updated in place")

	var got struct {
		ExperimentTemplate struct {
			Description string `json:"description"`
			Targets     map[string]struct {
				ResourceTags map[string]string `json:"resourceTags"`
				Filters      []struct {
					Path string `json:"path"`
				} `json:"filters"`
			} `json:"targets"`
			Actions map[string]struct {
				Parameters map[string]string `json:"parameters"`
				Targets    map[string]string `json:"targets"`
			} `json:"actions"`
			ExperimentOptions map[string]string `json:"experimentOptions"`
			Tags              map[string]string `json:"tags"`
		} `json:"experimentTemplate"`
	}
	require.Equal(t, http.StatusOK, v129REST(t, d, "fis", http.MethodGet, "/experimentTemplates/"+res.PhysicalID, nil, &got))
	tg := got.ExperimentTemplate
	assert.Equal(t, "second", tg.Description)
	assert.Equal(t, map[string]string{"aegc:Role": "worker"}, tg.Targets["SpotWorkers"].ResourceTags, "a caller's keys are kept")
	assert.Equal(t, "State.Name", tg.Targets["SpotWorkers"].Filters[0].Path)
	assert.Equal(t, "PT2M", tg.Actions["Interrupt"].Parameters["durationBeforeInterruption"])
	assert.Equal(t, "SpotWorkers", tg.Actions["Interrupt"].Targets["SpotInstances"])
	assert.Equal(t, "skip", tg.ExperimentOptions["emptyTargetResolutionMode"])
	assert.Equal(t, map[string]string{"Owner": "v"}, tg.Tags)

	bad, err := d.Deploy(ctx, `{"Resources":{"Bad":{"Type":"AWS::FIS::ExperimentTemplate","Properties":{
		"Description":"bad","RoleArn":"arn:aws:iam::123456789012:role/aegc-fis","StopConditions":[{"Source":"none"}],
		"Targets":{},"Actions":{"X":{"ActionId":"aws:ec2:explode"}}}}}}`, "fis-bad", nil)
	require.NoError(t, err)
	assert.Contains(t, resourceByLogicalID(t, bad, "Bad").Error, "ValidationException")

	for _, del := range d.DeleteStackResourcesForTest(ctx, "fis") {
		assert.Empty(t, del.Reason, "%s: %s", del.LogicalID, del.Status)
	}
	_, err = d.DispatchForTest(ctx, &emulator.AWSRequest{Service: "fis", Operation: http.MethodGet,
		Path: "/experimentTemplates/" + res.PhysicalID, Headers: map[string]string{}, Params: map[string]string{}}, "probe")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ResourceNotFoundException")
}

// A persistent Spot request whose instance is interrupted is re-fulfilled with a replacement, and
// a replay mints the same replacement ID from the request that observed the interruption.
func TestFIS_APersistentSpotRequestIsReFulfilledAndReplays(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t, withFISReplayRecording()...)
	var req struct {
		Requests []struct {
			ID         string `xml:"spotInstanceRequestId"`
			InstanceID string `xml:"instanceId"`
		} `xml:"spotInstanceRequestSet>item"`
	}
	e.ec2(map[string]string{
		"Action": "RequestSpotInstances", "Type": "persistent", "InstanceInterruptionBehavior": "terminate",
		"LaunchSpecification.ImageId": ec2TestImage, "LaunchSpecification.InstanceType": "t3.micro",
	}, &req)
	require.Len(t, req.Requests, 1)
	instance := req.Requests[0].InstanceID
	id := e.template(map[string]any{
		"targets": map[string]any{"one": map[string]any{
			"resourceType": "aws:ec2:spot-instance", "selectionMode": "ALL",
			"resourceArns": []string{"arn:aws:ec2:us-east-1:123456789012:instance/" + instance},
		}},
		"actions": map[string]any{"interrupt": map[string]any{
			"actionId":   "aws:ec2:send-spot-instance-interruptions",
			"parameters": map[string]string{"durationBeforeInterruption": "PT2M"},
			"targets":    map[string]string{"SpotInstances": "one"},
		}},
	})
	x := e.start(id)
	e.ts.AdvanceTime(2 * time.Minute)
	require.Equal(t, "completed", e.experiment(x.ID).State.Status)
	assert.Equal(t, "terminated", e.instance(instance).State)
	var after struct {
		Requests []struct {
			State      string `xml:"state"`
			InstanceID string `xml:"instanceId"`
		} `xml:"spotInstanceRequestSet>item"`
	}
	e.ec2(map[string]string{"Action": "DescribeSpotInstanceRequests", "SpotInstanceRequestId.1": req.Requests[0].ID}, &after)
	require.Len(t, after.Requests, 1)
	assert.Equal(t, "active", after.Requests[0].State, "a persistent request is fulfilled again")
	assert.NotEqual(t, instance, after.Requests[0].InstanceID)
	assertFISReplays(t, e)
}

// A caller's own stop clears the reason an experiment's stop recorded.
func TestFIS_ACallersOwnTransitionClearsTheStateReason(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t)
	instance := e.run(false, "", "", map[string]string{"fis": "target"})
	id := e.template(map[string]any{
		"targets": map[string]any{"all": instanceTarget("ALL")},
		"actions": map[string]any{"stop": map[string]any{
			"actionId": "aws:ec2:stop-instances", "targets": map[string]string{"Instances": "all"},
		}},
	})
	x := e.start(id)
	assert.Equal(t, "completed", e.experiment(x.ID).State.Status, "a stop without a restart completes at once")
	assert.Equal(t, "Client.UserInitiatedShutdown", e.instance(instance).ReasonCode)
	e.ec2(map[string]string{"Action": "StartInstances", "InstanceId.1": instance}, nil)
	got := e.instance(instance)
	assert.Empty(t, got.ReasonCode)
	assert.Empty(t, got.Reason)
}
