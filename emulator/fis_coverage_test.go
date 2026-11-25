package emulator_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The FIS plugin's remaining edges (#1619): seed-store and EC2-list failures, a plugin with no
// EC2 to resolve targets through, the filter paths that match nothing, and CloudFormation updates
// FIS refuses.

// A failing seed store fails the clock-driven work rather than ending an experiment, and a
// failing EC2 list fails StartExperiment for an EC2 target.
func TestFIS_SeedAndEC2ListFailuresAreReported(t *testing.T) {
	t.Parallel()
	wait := map[string]any{"actionId": "aws:fis:wait", "parameters": map[string]string{"duration": "PT1M"}}
	for _, kind := range []string{"get", "list"} {
		e := newFISFaultEnv(t, "fis-ctrl")
		var created struct {
			ExperimentTemplate struct {
				ID string `json:"id"`
			} `json:"experimentTemplate"`
		}
		e.must(http.MethodPost, "/experimentTemplates", faultTemplate(nil, wait), &created)
		e.must(http.MethodPost, "/experiments", map[string]any{"experimentTemplateId": created.ExperimentTemplate.ID}, nil)
		e.state.fail = kind
		assert.ErrorIs(t, e.fis.RunDue(e.ctx(), nil), errInjectedStore, kind)
	}

	e := newFISFaultEnv(t, "ec2")
	e.ec2Run(false, false)
	var created struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}
	target := map[string]any{"resourceType": "aws:ec2:instance", "resourceTags": map[string]string{"fis": "target"},
		"selectionMode": "ALL"}
	e.must(http.MethodPost, "/experimentTemplates", faultTemplate(target,
		map[string]any{"actionId": "aws:ec2:reboot-instances", "targets": map[string]string{"Instances": "t"}}), &created)
	for _, kind := range []string{"list", "get"} {
		e.state.fail = kind
		_, err := e.call(http.MethodPost, "/experiments", map[string]any{"experimentTemplateId": created.ExperimentTemplate.ID})
		assert.ErrorIs(t, err, errInjectedStore, kind)
	}
}

// A plugin with no registry cannot resolve an EC2 target, and says so; it still serves every
// other operation, and its Shutdown is a no-op.
func TestFIS_APluginWithNoEC2(t *testing.T) {
	t.Parallel()
	p := &emulator.FISPlugin{}
	require.NoError(t, p.Initialize(context.Background(), emulator.PluginConfig{
		State: emulator.NewMemoryStateManager(), Logger: emulator.NewDefaultLogger(slog.LevelError, false),
	}))
	ctx := &emulator.RequestContext{RequestID: "req-fis-noec2", AccountID: "123456789012", Region: "us-east-1",
		IDs: emulator.NewIDMint("req-fis-noec2")}
	created := wireREST(t, p, ctx, "fis", http.MethodPost, "/experimentTemplates", map[string]any{
		"description": "d", "roleArn": fisRoleARN, "stopConditions": []any{map[string]any{"source": "none"}},
		"targets": map[string]any{"t": instanceTarget("ALL")},
		"actions": map[string]any{"r": map[string]any{"actionId": "aws:ec2:reboot-instances",
			"targets": map[string]string{"Instances": "t"}}},
	})
	id := created[strings.Index(string(created), `"id":"`)+6:]
	id = id[:strings.Index(string(id), `"`)]
	_, err := p.HandleRequest(ctx, &emulator.AWSRequest{Service: "fis", HTTPMethod: http.MethodPost, Path: "/experiments",
		Body: []byte(`{"experimentTemplateId":"` + string(id) + `"}`), Headers: map[string]string{}, Params: map[string]string{}})
	require.Error(t, err)
	var awsErr *emulator.AWSError
	assert.False(t, errors.As(err, &awsErr), "a missing EC2 is substrate's failure, not a refusal: %v", err)
	assert.NoError(t, p.Shutdown(context.Background()))
}

// Filter paths on members an instance does not carry match nothing, an instance missing a tag is
// not selected, and a resourceArns target ignores instances it does not name.
func TestFIS_FilterPathsOnAbsentMembers(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t)
	tagged := e.run(false, "", "", map[string]string{"fis": "target"})
	e.run(false, "", "", map[string]string{"other": "x"})
	e.run(false, "", "", nil)
	resolve := func(tg map[string]any) int {
		t.Helper()
		id := e.template(map[string]any{
			"targets":           map[string]any{"t": tg},
			"experimentOptions": map[string]string{"emptyTargetResolutionMode": "skip"},
			"actions": map[string]any{"r": map[string]any{"actionId": "aws:ec2:reboot-instances",
				"targets": map[string]string{"Instances": "t"}}},
		})
		var out struct {
			ResolvedTargets []struct{} `json:"resolvedTargets"`
		}
		e.mustFIS(http.MethodGet, "/experiments/"+e.start(id).ID+"/resolvedTargets", nil, &out)
		return len(out.ResolvedTargets)
	}
	for _, path := range []string{"VpcId", "PrivateIpAddress", "PublicIpAddress", "PrivateDnsName",
		"SecurityGroups.GroupId", "NetworkInterfaces.Groups.GroupId", "SubnetId"} {
		tg := instanceTarget("ALL")
		tg["filters"] = []any{map[string]any{"path": path, "values": []string{"no-such-value"}}}
		assert.Zero(t, resolve(tg), path)
	}
	assert.Equal(t, 1, resolve(instanceTarget("ALL")), "only the tagged instance carries fis=target")
	assert.Equal(t, 1, resolve(map[string]any{"resourceType": "aws:ec2:instance", "selectionMode": "ALL",
		"resourceArns": []string{"arn:aws:ec2:us-east-1:123456789012:instance/" + tagged,
			"arn:aws:ec2:us-west-2:123456789012:instance/" + tagged}}), "an ARN in another Region names nothing")
}

// CloudFormation reports FIS's refusal of an update and of a tag change on the resource.
func TestCFNFISExperimentTemplate_RefusedUpdates(t *testing.T) {
	t.Parallel()
	d, _, _, _ := newSweepDeployer(t)
	tmpl := func(action, tags string) string {
		return `{"Resources":{"Template":{"Type":"AWS::FIS::ExperimentTemplate","Properties":{
			"Description":"d","RoleArn":"arn:aws:iam::123456789012:role/aegc-fis",
			"StopConditions":[{"Source":"none"}],"Targets":{},
			"Actions":{"A":{"ActionId":"` + action + `","Parameters":{"duration":"PT1M"}}},"Tags":` + tags + `}}}}`
	}
	refused := func(stack, after string) {
		t.Helper()
		ctx := context.Background()
		created, err := d.Deploy(ctx, tmpl("aws:fis:wait", `{"a":"b"}`), stack, nil)
		require.NoError(t, err)
		requireNoResourceErrors(t, created)
		updated, err := d.UpdateStack(ctx, after, stack, nil, emulator.CFNDeployOptions{})
		require.NoError(t, err)
		assert.Equal(t, "UPDATE_ROLLBACK_COMPLETE", updated.Status)
		assert.Contains(t, updated.StatusReason, "ValidationException")
	}
	refused("fis-refused", tmpl("aws:ec2:explode", `{"a":"b"}`))
	refused("fis-retag", tmpl("aws:fis:wait", `{"aws:x":"y"}`))

	_, untagged := v129Update(t, d, "fis-untag", tmpl("aws:fis:wait", `{"a":"b"}`), tmpl("aws:fis:wait", `{}`))
	res := resourceByLogicalID(t, untagged, "Template")
	require.Empty(t, res.Error)
	var got struct {
		ExperimentTemplate struct {
			Tags map[string]string `json:"tags"`
		} `json:"experimentTemplate"`
	}
	require.Equal(t, http.StatusOK, v129REST(t, d, "fis", http.MethodGet, "/experimentTemplates/"+res.PhysicalID, nil, &got))
	assert.Empty(t, got.ExperimentTemplate.Tags, "removing every tag untags the template")
}

// The seed endpoint answers 500 when its store fails.
func TestFIS_SeedEndpointReportsAFailingStore(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t)
	for _, tc := range []struct {
		kind, method, path, body string
	}{
		{"put", http.MethodPost, "/v1/fis/experiment-result", `{"id":"*","status":"failed"}`},
		{"list", http.MethodDelete, "/v1/fis/experiment-result", ""},
		{"delete", http.MethodDelete, "/v1/fis/experiment-result?id=*", ""},
	} {
		e.ts.ControlPlaneStateForTest(&nsFaultState{StateManager: e.ts.StateManager(), namespace: "fis-ctrl", fail: tc.kind})
		status, out := e.do(tc.method, "localhost", tc.path, "application/json", []byte(tc.body))
		assert.Equal(t, http.StatusInternalServerError, status, "%s: %s", tc.kind, out)
	}
	e.ts.ControlPlaneStateForTest(e.ts.StateManager())
}
