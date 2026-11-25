package emulator_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The FIS template, catalog and tagging operations (#1619).

// fisWaitAction is a valid action that needs no target.
func fisWaitAction() map[string]any {
	return map[string]any{"actionId": "aws:fis:wait", "parameters": map[string]string{"duration": "PT1M"}}
}

// fisValidTemplate is a template body every validation case starts from.
func fisValidTemplate() map[string]any {
	return map[string]any{
		"clientToken": "tok", "description": "d", "roleArn": fisRoleARN,
		"stopConditions": []any{map[string]any{"source": "none"}},
		"targets":        map[string]any{"all": instanceTarget("ALL")},
		"actions": map[string]any{
			"stop": map[string]any{"actionId": "aws:ec2:stop-instances", "targets": map[string]string{"Instances": "all"}},
		},
	}
}

func TestFIS_CreateExperimentTemplateValidation(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t)
	for _, tc := range []struct {
		name   string
		edit   func(b map[string]any)
		substr string
	}{
		{"no description", func(b map[string]any) { delete(b, "description") }, "'description'"},
		{"no roleArn", func(b map[string]any) { delete(b, "roleArn") }, "'roleArn'"},
		{"short roleArn", func(b map[string]any) { b["roleArn"] = "arn:short" }, "roleArn"},
		{"no stopConditions", func(b map[string]any) { delete(b, "stopConditions") }, "'stopConditions'"},
		{"no actions", func(b map[string]any) { delete(b, "actions") }, "'actions'"},
		{"unknown action", func(b map[string]any) {
			b["actions"] = map[string]any{"x": map[string]any{"actionId": "aws:ec2:explode"}}
		}, "aws:ec2:explode"},
		{"unknown parameter", func(b map[string]any) {
			b["actions"] = map[string]any{"w": map[string]any{"actionId": "aws:fis:wait",
				"parameters": map[string]string{"duration": "PT1M", "speed": "fast"}}}
		}, "speed"},
		{"missing required parameter", func(b map[string]any) {
			b["actions"] = map[string]any{"w": map[string]any{"actionId": "aws:fis:wait"}}
		}, "requires the parameter duration"},
		{"duration out of range", func(b map[string]any) {
			b["targets"] = map[string]any{"spot": spotTarget("ALL")}
			b["actions"] = map[string]any{"i": map[string]any{"actionId": "aws:ec2:send-spot-instance-interruptions",
				"parameters": map[string]string{"durationBeforeInterruption": "PT1M"},
				"targets":    map[string]string{"SpotInstances": "spot"}}}
		}, "durationBeforeInterruption"},
		{"not a duration", func(b map[string]any) {
			b["actions"] = map[string]any{"w": map[string]any{"actionId": "aws:fis:wait",
				"parameters": map[string]string{"duration": "five minutes"}}}
		}, "ISO 8601"},
		{"wrong target key", func(b map[string]any) {
			b["actions"] = map[string]any{"s": map[string]any{"actionId": "aws:ec2:stop-instances",
				"targets": map[string]string{"SpotInstances": "all"}}}
		}, "does not take the target"},
		{"undefined target", func(b map[string]any) {
			b["actions"] = map[string]any{"s": map[string]any{"actionId": "aws:ec2:stop-instances",
				"targets": map[string]string{"Instances": "nope"}}}
		}, "not defined"},
		{"wrong resource type", func(b map[string]any) {
			b["targets"] = map[string]any{"all": spotTarget("ALL")}
		}, "resource type"},
		{"missing target", func(b map[string]any) {
			b["actions"] = map[string]any{"s": map[string]any{"actionId": "aws:ec2:stop-instances"}}
		}, "requires the target Instances"},
		{"startAfter self", func(b map[string]any) {
			a := fisWaitAction()
			a["startAfter"] = []string{"w"}
			b["actions"] = map[string]any{"w": a}
		}, "after itself"},
		{"startAfter unknown", func(b map[string]any) {
			a := fisWaitAction()
			a["startAfter"] = []string{"nope"}
			b["actions"] = map[string]any{"w": a}
		}, "not an action"},
		{"startAfter cycle", func(b map[string]any) {
			x, y := fisWaitAction(), fisWaitAction()
			x["startAfter"], y["startAfter"] = []string{"y"}, []string{"x"}
			b["actions"] = map[string]any{"x": x, "y": y}
		}, "circular"},
		{"bad selection mode", func(b map[string]any) { b["targets"] = map[string]any{"all": instanceTarget("SOME")} }, "selection mode"},
		{"COUNT(0)", func(b map[string]any) { b["targets"] = map[string]any{"all": instanceTarget("COUNT(0)")} }, "selection mode"},
		{"PERCENT(101)", func(b map[string]any) { b["targets"] = map[string]any{"all": instanceTarget("PERCENT(101)")} }, "selection mode"},
		{"unknown resource type", func(b map[string]any) {
			b["targets"] = map[string]any{"all": map[string]any{"resourceType": "aws:s3:bucket",
				"resourceArns": []string{"arn:aws:s3:::b"}, "selectionMode": "ALL"}}
		}, "aws:s3:bucket"},
		{"arns and tags", func(b map[string]any) {
			tg := instanceTarget("ALL")
			tg["resourceArns"] = []string{"arn:aws:ec2:us-east-1:123456789012:instance/i-0123456789abcdef0"}
			b["targets"] = map[string]any{"all": tg}
		}, "together"},
		{"neither arns nor tags", func(b map[string]any) {
			b["targets"] = map[string]any{"all": map[string]any{"resourceType": "aws:ec2:instance", "selectionMode": "ALL"}}
		}, "at least one"},
		{"too many arns", func(b map[string]any) {
			arns := make([]string, 6)
			for i := range arns {
				arns[i] = fmt.Sprintf("arn:aws:ec2:us-east-1:123456789012:instance/i-%017d", i)
			}
			b["targets"] = map[string]any{"all": map[string]any{"resourceType": "aws:ec2:instance",
				"resourceArns": arns, "selectionMode": "ALL"}}
		}, "at most 5"},
		{"empty filter", func(b map[string]any) {
			tg := instanceTarget("ALL")
			tg["filters"] = []any{map[string]any{"path": "State.Name"}}
			b["targets"] = map[string]any{"all": tg}
		}, "filter"},
		{"bad stop source", func(b map[string]any) {
			b["stopConditions"] = []any{map[string]any{"source": "aws:sns:topic"}}
		}, "aws:sns:topic"},
		{"alarm without ARN", func(b map[string]any) {
			b["stopConditions"] = []any{map[string]any{"source": "aws:cloudwatch:alarm"}}
		}, "CloudWatch alarm"},
		{"bad empty mode", func(b map[string]any) {
			b["experimentOptions"] = map[string]string{"emptyTargetResolutionMode": "ignore"}
		}, "emptyTargetResolutionMode"},
		{"bad account targeting", func(b map[string]any) {
			b["experimentOptions"] = map[string]string{"accountTargeting": "every-account"}
		}, "accountTargeting"},
		{"actionsMode on a template", func(b map[string]any) {
			b["experimentOptions"] = map[string]string{"actionsMode": "skip-all"}
		}, "actionsMode"},
		{"long description", func(b map[string]any) { b["description"] = strings.Repeat("d", 513) }, "512"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := fisValidTemplate()
			delete(body, "clientToken")
			tc.edit(body)
			status, out := e.fis(http.MethodPost, "/experimentTemplates", body)
			assert.Equal(t, http.StatusBadRequest, status, "%s", out)
			assert.Contains(t, string(out), "ValidationException")
			assert.Contains(t, string(out), tc.substr)
		})
	}
	status, out := e.do(http.MethodPost, fisHost, "/experimentTemplates", "application/json", []byte("{not json"))
	assert.Equal(t, http.StatusBadRequest, status, "%s", out)
}

func TestFIS_TemplateLifecycleAndPaging(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t)
	var created struct {
		ExperimentTemplate map[string]any `json:"experimentTemplate"`
	}
	e.mustFIS(http.MethodPost, "/experimentTemplates", fisValidTemplate(), &created)
	id := created.ExperimentTemplate["id"].(string)
	assert.Equal(t, "arn:aws:fis:us-east-1:123456789012:experiment-template/"+id, created.ExperimentTemplate["arn"])
	assert.Equal(t, map[string]any{}, created.ExperimentTemplate["experimentOptions"], "options are reported as sent")
	assert.Equal(t, map[string]any{}, created.ExperimentTemplate["tags"])
	assert.EqualValues(t, 0, created.ExperimentTemplate["targetAccountConfigurationsCount"])
	assert.NotContains(t, created.ExperimentTemplate, "account_id", "no bookkeeping reaches the wire")
	assert.NotContains(t, created.ExperimentTemplate, "client_token")

	// The same client token answers the same template.
	var again struct {
		ExperimentTemplate map[string]any `json:"experimentTemplate"`
	}
	e.mustFIS(http.MethodPost, "/experimentTemplates", fisValidTemplate(), &again)
	assert.Equal(t, id, again.ExperimentTemplate["id"])

	// Update replaces what it is sent and keeps the rest.
	e.ts.AdvanceTime(1e9)
	var updated struct {
		ExperimentTemplate map[string]any `json:"experimentTemplate"`
	}
	e.mustFIS(http.MethodPatch, "/experimentTemplates/"+id, map[string]any{
		"description": "updated", "experimentOptions": map[string]string{"emptyTargetResolutionMode": "skip"},
	}, &updated)
	assert.Equal(t, "updated", updated.ExperimentTemplate["description"])
	assert.Equal(t, fisRoleARN, updated.ExperimentTemplate["roleArn"])
	assert.Equal(t, "skip", updated.ExperimentTemplate["experimentOptions"].(map[string]any)["emptyTargetResolutionMode"])
	assert.Greater(t, updated.ExperimentTemplate["lastUpdateTime"], updated.ExperimentTemplate["creationTime"])
	for _, body := range []map[string]any{
		{"tags": map[string]string{"a": "b"}},
		{"experimentOptions": map[string]string{"accountTargeting": "multi-account"}},
		{"actions": map[string]any{"x": map[string]any{"actionId": "aws:ec2:explode"}}},
	} {
		status, out := e.fis(http.MethodPatch, "/experimentTemplates/"+id, body)
		assert.Equal(t, http.StatusBadRequest, status, "%v: %s", body, out)
	}

	// Paging over three templates, one at a time.
	for range 2 {
		body := fisValidTemplate()
		delete(body, "clientToken")
		e.template(body)
	}
	var ids []string
	token := ""
	for page := 0; page < 5; page++ {
		path := "/experimentTemplates?maxResults=1"
		if token != "" {
			path += "&nextToken=" + url.QueryEscape(token)
		}
		var out struct {
			ExperimentTemplates []struct {
				ID string `json:"id"`
			} `json:"experimentTemplates"`
			NextToken string `json:"nextToken"`
		}
		e.mustFIS(http.MethodGet, path, nil, &out)
		require.Len(t, out.ExperimentTemplates, 1)
		ids = append(ids, out.ExperimentTemplates[0].ID)
		if token = out.NextToken; token == "" {
			break
		}
	}
	assert.Len(t, ids, 3)
	for _, path := range []string{"/experimentTemplates?maxResults=0", "/experimentTemplates?maxResults=101",
		"/experimentTemplates?nextToken=bogus"} {
		status, out := e.fis(http.MethodGet, path, nil)
		assert.Equal(t, http.StatusBadRequest, status, "%s: %s", path, out)
	}

	// Delete answers the template; then it is gone.
	var deleted struct {
		ExperimentTemplate map[string]any `json:"experimentTemplate"`
	}
	e.mustFIS(http.MethodDelete, "/experimentTemplates/"+id, nil, &deleted)
	assert.Equal(t, id, deleted.ExperimentTemplate["id"])
	for _, method := range []string{http.MethodGet, http.MethodDelete, http.MethodPatch} {
		status, out := e.fis(method, "/experimentTemplates/"+id, map[string]any{})
		assert.Equal(t, http.StatusNotFound, status, "%s: %s", method, out)
		assert.Contains(t, string(out), "ResourceNotFoundException")
	}
	status, out := e.fis(http.MethodPost, "/experiments", map[string]any{"experimentTemplateId": id})
	assert.Equal(t, http.StatusNotFound, status, "%s", out)
}

func TestFIS_ExperimentListingIdempotencyAndRefusals(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t)
	a := e.template(map[string]any{"actions": map[string]any{"w": fisWaitAction()}})
	b := e.template(map[string]any{"actions": map[string]any{"w": fisWaitAction()}})
	var first, second struct {
		Experiment fisExperimentOut `json:"experiment"`
	}
	start := map[string]any{"experimentTemplateId": a, "clientToken": "once", "tags": map[string]string{"k": "v"}}
	e.mustFIS(http.MethodPost, "/experiments", start, &first)
	e.mustFIS(http.MethodPost, "/experiments", start, &second)
	assert.Equal(t, first.Experiment.ID, second.Experiment.ID, "a repeated client token answers the same experiment")
	assert.True(t, strings.HasPrefix(first.Experiment.ID, "EXP"))
	status, out := e.fis(http.MethodPost, "/experiments", map[string]any{"experimentTemplateId": b, "clientToken": "once"})
	assert.Equal(t, http.StatusConflict, status, "%s", out)
	e.start(b)

	var list struct {
		Experiments []struct {
			ID                   string `json:"id"`
			ExperimentTemplateID string `json:"experimentTemplateId"`
		} `json:"experiments"`
	}
	e.mustFIS(http.MethodGet, "/experiments?experimentTemplateId="+a, nil, &list)
	require.Len(t, list.Experiments, 1)
	assert.Equal(t, first.Experiment.ID, list.Experiments[0].ID)
	e.mustFIS(http.MethodGet, "/experiments", nil, &list)
	assert.Len(t, list.Experiments, 2)

	for _, tc := range []struct {
		method, path string
		body         any
		want         int
	}{
		{http.MethodPost, "/experiments", map[string]any{}, http.StatusBadRequest},
		{http.MethodPost, "/experiments", map[string]any{"experimentTemplateId": a,
			"experimentOptions": map[string]string{"actionsMode": "some"}}, http.StatusBadRequest},
		{http.MethodGet, "/experiments/EXPnope", nil, http.StatusNotFound},
		{http.MethodDelete, "/experiments/EXPnope", nil, http.StatusNotFound},
		{http.MethodGet, "/experiments/EXPnope/resolvedTargets", nil, http.StatusNotFound},
		{http.MethodGet, "/experiments/" + first.Experiment.ID + "/resolvedTargets?targetName=nope", nil, http.StatusNotFound},
		{http.MethodGet, "/experiments/" + first.Experiment.ID + "/resolvedTargets?maxResults=500", nil, http.StatusBadRequest},
		{http.MethodPut, "/experiments", nil, http.StatusNotFound},
		{http.MethodGet, "/nowhere", nil, http.StatusNotFound},
	} {
		status, out := e.fis(tc.method, tc.path, tc.body)
		assert.Equal(t, tc.want, status, "%s %s: %s", tc.method, tc.path, out)
	}
}

func TestFIS_ActionsAndTargetResourceTypes(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t)
	var list struct {
		Actions []struct {
			ID      string                       `json:"id"`
			Arn     string                       `json:"arn"`
			Targets map[string]map[string]string `json:"targets"`
		} `json:"actions"`
	}
	e.mustFIS(http.MethodGet, "/actions", nil, &list)
	require.Len(t, list.Actions, 10)
	byID := map[string]map[string]map[string]string{}
	for _, a := range list.Actions {
		byID[a.ID] = a.Targets
	}
	assert.Equal(t, "aws:ec2:spot-instance", byID["aws:ec2:send-spot-instance-interruptions"]["SpotInstances"]["resourceType"])
	assert.Empty(t, byID["aws:fis:wait"])

	var got struct {
		Action struct {
			ID          string `json:"id"`
			Description string `json:"description"`
			Parameters  map[string]struct {
				Description string `json:"description"`
				Required    bool   `json:"required"`
			} `json:"parameters"`
		} `json:"action"`
	}
	e.mustFIS(http.MethodGet, "/actions/aws:ec2:stop-instances", nil, &got)
	assert.Equal(t, "Stop the specified EC2 instances.", got.Action.Description)
	assert.Equal(t, "The time to wait before restarting the instances (ISO 8601 duration).",
		got.Action.Parameters["startInstancesAfterDuration"].Description)
	assert.False(t, got.Action.Parameters["startInstancesAfterDuration"].Required)
	status, out := e.fis(http.MethodGet, "/actions/aws:ec2:explode", nil)
	assert.Equal(t, http.StatusNotFound, status, "%s", out)

	var types struct {
		TargetResourceTypes []struct {
			ResourceType string `json:"resourceType"`
		} `json:"targetResourceTypes"`
		NextToken string `json:"nextToken"`
	}
	e.mustFIS(http.MethodGet, "/targetResourceTypes?maxResults=2", nil, &types)
	assert.Len(t, types.TargetResourceTypes, 2)
	assert.NotEmpty(t, types.NextToken)
	var one struct {
		TargetResourceType struct {
			ResourceType string `json:"resourceType"`
			Description  string `json:"description"`
		} `json:"targetResourceType"`
	}
	e.mustFIS(http.MethodGet, "/targetResourceTypes/aws:ec2:spot-instance", nil, &one)
	assert.Equal(t, "An Amazon EC2 Spot Instance", one.TargetResourceType.Description)
	status, out = e.fis(http.MethodGet, "/targetResourceTypes/aws:s3:bucket", nil)
	assert.Equal(t, http.StatusNotFound, status, "%s", out)
}

func TestFIS_Tagging(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t)
	id := e.template(map[string]any{"actions": map[string]any{"w": fisWaitAction()}, "tags": map[string]string{"team": "aegc"}})
	arn := "arn:aws:fis:us-east-1:123456789012:experiment-template/" + id
	x := e.start(id)

	tags := func(resource string) map[string]string {
		var out struct {
			Tags map[string]string `json:"tags"`
		}
		e.mustFIS(http.MethodGet, "/tags/"+resource, nil, &out)
		return out.Tags
	}
	assert.Equal(t, map[string]string{"team": "aegc"}, tags(arn))
	e.mustFIS(http.MethodPost, "/tags/"+arn, map[string]any{"tags": map[string]string{"env": "dev"}}, nil)
	assert.Equal(t, map[string]string{"team": "aegc", "env": "dev"}, tags(arn))
	e.mustFIS(http.MethodDelete, "/tags/"+arn+"?tagKeys=team&tagKeys=nope", nil, nil)
	assert.Equal(t, map[string]string{"env": "dev"}, tags(arn))

	e.mustFIS(http.MethodPost, "/tags/"+x.Arn, map[string]any{"tags": map[string]string{"run": "1"}}, nil)
	assert.Equal(t, map[string]string{"run": "1"}, tags(x.Arn))

	// The Resource Groups Tagging API reads both through the provider.
	var resources struct {
		ResourceTagMappingList []struct {
			ResourceARN string `json:"ResourceARN"`
		} `json:"ResourceTagMappingList"`
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, e.ts.URL+"/", strings.NewReader(`{}`))
	require.NoError(t, err)
	req.Host = "tagging.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "ResourceGroupsTaggingAPI_20170126.GetResources")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&resources))
	var arns []string
	for _, r := range resources.ResourceTagMappingList {
		arns = append(arns, r.ResourceARN)
	}
	assert.Contains(t, arns, arn)
	assert.Contains(t, arns, x.Arn)

	for _, tc := range []struct {
		method, path string
		body         any
		want         int
	}{
		{http.MethodPost, "/tags/" + arn, map[string]any{}, http.StatusBadRequest},
		{http.MethodPost, "/tags/" + arn, map[string]any{"tags": map[string]string{"aws:x": "y"}}, http.StatusBadRequest},
		{http.MethodDelete, "/tags/" + arn + "?tagKeys=aws:x", nil, http.StatusBadRequest},
		{http.MethodGet, "/tags/arn:aws:fis:us-east-1:123456789012:experiment-template/EXTnope", nil, http.StatusNotFound},
		{http.MethodGet, "/tags/arn:aws:fis:us-west-2:123456789012:experiment-template/" + id, nil, http.StatusNotFound},
		{http.MethodPost, "/tags/arn:aws:fis:us-east-1:123456789012:action/aws:fis:wait",
			map[string]any{"tags": map[string]string{"a": "b"}}, http.StatusNotFound},
	} {
		status, out := e.fis(tc.method, tc.path, tc.body)
		assert.Equal(t, tc.want, status, "%s %s: %s", tc.method, tc.path, out)
	}
	many := map[string]string{}
	for i := range 50 {
		many[fmt.Sprintf("k%02d", i)] = "v"
	}
	status, out := e.fis(http.MethodPost, "/tags/"+arn, map[string]any{"tags": many})
	assert.Equal(t, http.StatusBadRequest, status, "51 tags: %s", out)
}
