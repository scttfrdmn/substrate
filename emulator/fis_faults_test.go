package emulator_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Store failures in the FIS plugin (#1619): a failing read or write is reported as the failure,
// never as success or as an AWS refusal, and a failing EC2 store fails the action rather than
// stalling the experiment.

// fisFaultEnv is a registry of every plugin over a state manager that fails on demand.
type fisFaultEnv struct {
	t        *testing.T
	state    *nsFaultState
	registry *emulator.PluginRegistry
	fis      *emulator.FISPlugin
	tc       *emulator.TimeController
	seq      int
}

func newFISFaultEnv(t *testing.T, namespace string) *fisFaultEnv {
	t.Helper()
	state := &nsFaultState{StateManager: emulator.NewMemoryStateManager(), namespace: namespace}
	tc := emulator.NewTimeController(fisClockInstant)
	tc.Freeze()
	tc.SetTime(fisClockInstant)
	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: false})
	require.NoError(t, emulator.RegisterDefaultPlugins(context.Background(), registry, state, tc,
		emulator.NewDefaultLogger(slog.LevelError, false), store, nil))
	p, ok := registry.Plugin("fis")
	require.True(t, ok)
	return &fisFaultEnv{t: t, state: state, registry: registry, fis: p.(*emulator.FISPlugin), tc: tc}
}

// ctx is a fresh request context.
func (e *fisFaultEnv) ctx() *emulator.RequestContext {
	e.seq++
	id := "req-fis-fault-" + string(rune('a'+e.seq%26)) + strings.Repeat("x", e.seq/26)
	return &emulator.RequestContext{RequestID: id, AccountID: "123456789012", Region: "us-east-1",
		Timestamp: fisClockInstant, IDs: emulator.NewIDMint(id)}
}

// call sends one FIS request through the registry.
func (e *fisFaultEnv) call(method, path string, body any) ([]byte, error) {
	e.t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		require.NoError(e.t, err)
	}
	params := map[string]string{}
	multi := map[string][]string{}
	if i := strings.Index(path, "?"); i >= 0 {
		for _, kv := range strings.Split(path[i+1:], "&") {
			k, v, _ := strings.Cut(kv, "=")
			params[k] = v
			multi[k] = append(multi[k], v)
		}
		path = path[:i]
	}
	resp, err := e.registry.RouteRequest(e.ctx(), &emulator.AWSRequest{Service: "fis", HTTPMethod: method,
		Operation: method, Path: path, Body: raw, Headers: map[string]string{}, Params: params, MultiValueParams: multi})
	if resp == nil {
		return nil, err
	}
	return resp.Body, err
}

// must sends one FIS request that must succeed and decodes its answer into out.
func (e *fisFaultEnv) must(method, path string, body, out any) {
	e.t.Helper()
	raw, err := e.call(method, path, body)
	require.NoError(e.t, err, "%s %s", method, path)
	if out != nil {
		require.NoError(e.t, json.Unmarshal(raw, out), "%s", raw)
	}
}

// ec2Run launches one tagged instance, a Spot one when spot is set (persistent with a stop
// interruption behavior when stop is set), and returns its ID.
func (e *fisFaultEnv) ec2Run(spot, stop bool) string {
	e.t.Helper()
	params := map[string]string{"Action": "RunInstances", "ImageId": ec2TestImage, "InstanceType": "t3.micro",
		"MinCount": "1", "MaxCount": "1", "TagSpecification.1.ResourceType": "instance",
		"TagSpecification.1.Tag.1.Key": "fis", "TagSpecification.1.Tag.1.Value": "target"}
	if spot {
		params["InstanceMarketOptions.MarketType"] = "spot"
		params["InstanceMarketOptions.SpotOptions.SpotInstanceType"] = "one-time"
		if stop {
			params["InstanceMarketOptions.SpotOptions.SpotInstanceType"] = "persistent"
			params["InstanceMarketOptions.SpotOptions.InstanceInterruptionBehavior"] = "stop"
		}
	}
	resp, err := e.registry.RouteRequest(e.ctx(), &emulator.AWSRequest{Service: "ec2", Operation: "RunInstances",
		Params: params, Headers: map[string]string{}})
	require.NoError(e.t, err)
	i := strings.Index(string(resp.Body), "<instanceId>")
	require.GreaterOrEqual(e.t, i, 0, "%s", resp.Body)
	rest := string(resp.Body)[i+len("<instanceId>"):]
	return rest[:strings.Index(rest, "<")]
}

// faultTemplate is a template with a client token, a tag and one action.
func faultTemplate(target map[string]any, action map[string]any) map[string]any {
	body := map[string]any{
		"clientToken": "tok", "description": "d", "roleArn": fisRoleARN, "tags": map[string]string{"k": "v"},
		"stopConditions": []any{map[string]any{"source": "none"}},
		"actions":        map[string]any{"a": action},
	}
	if target != nil {
		body["targets"] = map[string]any{"t": target}
	}
	return body
}

// TestFIS_StoreFailuresAreReported drives every FIS operation that reads or writes the store, and
// the clock-driven work, against a store failing in the fis namespace.
func TestFIS_StoreFailuresAreReported(t *testing.T) {
	t.Parallel()
	type op struct {
		name string
		run  func(e *fisFaultEnv, tmpl, exp string) error
	}
	path := func(method, p string, body any) func(e *fisFaultEnv, tmpl, exp string) error {
		return func(e *fisFaultEnv, tmpl, exp string) error {
			full := strings.NewReplacer("{tmpl}", tmpl, "{exp}", exp).Replace(p)
			_, err := e.call(method, full, body)
			return err
		}
	}
	tmplARN := "/tags/arn:aws:fis:us-east-1:123456789012:experiment-template/{tmpl}"
	expARN := "/tags/arn:aws:fis:us-east-1:123456789012:experiment/{exp}"
	wait := map[string]any{"actionId": "aws:fis:wait", "parameters": map[string]string{"duration": "PT1M"}}
	ops := []op{
		{"CreateExperimentTemplate", path(http.MethodPost, "/experimentTemplates", faultTemplate(nil, wait))},
		{"CreateExperimentTemplateNoToken", func(e *fisFaultEnv, _, _ string) error {
			body := faultTemplate(nil, wait)
			delete(body, "clientToken")
			_, err := e.call(http.MethodPost, "/experimentTemplates", body)
			return err
		}},
		{"GetExperimentTemplate", path(http.MethodGet, "/experimentTemplates/{tmpl}", nil)},
		{"ListExperimentTemplates", path(http.MethodGet, "/experimentTemplates", nil)},
		{"UpdateExperimentTemplate", path(http.MethodPatch, "/experimentTemplates/{tmpl}", map[string]any{"description": "u"})},
		{"DeleteExperimentTemplate", path(http.MethodDelete, "/experimentTemplates/{tmpl}", nil)},
		{"StartExperiment", path(http.MethodPost, "/experiments", map[string]any{"experimentTemplateId": "{tmpl}", "clientToken": "e"})},
		{"StartExperimentFresh", func(e *fisFaultEnv, tmpl, _ string) error {
			_, err := e.call(http.MethodPost, "/experiments", map[string]any{"experimentTemplateId": tmpl, "clientToken": "new"})
			return err
		}},
		{"GetExperiment", path(http.MethodGet, "/experiments/{exp}", nil)},
		{"ListExperiments", path(http.MethodGet, "/experiments", nil)},
		{"StopExperiment", path(http.MethodDelete, "/experiments/{exp}", nil)},
		{"ListExperimentResolvedTargets", path(http.MethodGet, "/experiments/{exp}/resolvedTargets", nil)},
		{"TagTemplate", path(http.MethodPost, tmplARN, map[string]any{"tags": map[string]string{"a": "b"}})},
		{"UntagExperiment", path(http.MethodDelete, expARN+"?tagKeys=k", nil)},
		{"ListTags", path(http.MethodGet, tmplARN, nil)},
		{"RunDue", func(e *fisFaultEnv, _, _ string) error { return e.fis.RunDue(e.ctx(), nil) }},
	}
	for _, o := range ops {
		t.Run(o.name, func(t *testing.T) {
			t.Parallel()
			failed := false
			for _, kind := range []string{"get", "put", "delete", "list"} {
				e := newFISFaultEnv(t, "fis")
				var created struct {
					ExperimentTemplate struct {
						ID string `json:"id"`
					} `json:"experimentTemplate"`
				}
				e.must(http.MethodPost, "/experimentTemplates", faultTemplate(nil, wait), &created)
				var started struct {
					Experiment struct {
						ID string `json:"id"`
					} `json:"experiment"`
				}
				e.must(http.MethodPost, "/experiments", map[string]any{"experimentTemplateId": created.ExperimentTemplate.ID,
					"clientToken": "e", "tags": map[string]string{"k": "v"}}, &started)
				e.state.fail = kind
				err := o.run(e, created.ExperimentTemplate.ID, started.Experiment.ID)
				var awsErr *emulator.AWSError
				if err == nil || errors.As(err, &awsErr) {
					continue
				}
				failed = true
				assert.ErrorIs(t, err, errInjectedStore, "%s under a %s failure", o.name, kind)
			}
			assert.True(t, failed, "%s reports a store failure under some fault kind", o.name)
		})
	}
}

// TestFIS_AFailingEC2StoreFailsTheAction drives each EC2 action against a store failing in the
// ec2 namespace: the action and the experiment fail with the failure as the reason, and the
// experiment does not stall.
func TestFIS_AFailingEC2StoreFailsTheAction(t *testing.T) {
	t.Parallel()
	instance := map[string]any{"resourceType": "aws:ec2:instance", "resourceTags": map[string]string{"fis": "target"},
		"selectionMode": "ALL"}
	spot := map[string]any{"resourceType": "aws:ec2:spot-instance", "resourceTags": map[string]string{"fis": "target"},
		"selectionMode": "ALL"}
	for _, tc := range []struct {
		name    string
		spot    bool
		stop    bool
		target  map[string]any
		action  map[string]any
		advance time.Duration
	}{
		{"stop", false, false, instance, map[string]any{"actionId": "aws:ec2:stop-instances", "targets": map[string]string{"Instances": "t"}}, 0},
		{"terminate", false, false, instance, map[string]any{"actionId": "aws:ec2:terminate-instances", "targets": map[string]string{"Instances": "t"}}, 0},
		{"restart", false, false, instance, map[string]any{"actionId": "aws:ec2:stop-instances", "targets": map[string]string{"Instances": "t"},
			"parameters": map[string]string{"startInstancesAfterDuration": "PT1M"}}, time.Minute},
		{"interrupt", true, false, spot, map[string]any{"actionId": "aws:ec2:send-spot-instance-interruptions",
			"targets": map[string]string{"SpotInstances": "t"}, "parameters": map[string]string{"durationBeforeInterruption": "PT2M"}}, 2 * time.Minute},
		{"interrupt-stop", true, true, spot, map[string]any{"actionId": "aws:ec2:send-spot-instance-interruptions",
			"targets": map[string]string{"SpotInstances": "t"}, "parameters": map[string]string{"durationBeforeInterruption": "PT2M"}}, 2 * time.Minute},
	} {
		for _, kind := range []string{"get", "put"} {
			t.Run(tc.name+"/"+kind, func(t *testing.T) {
				t.Parallel()
				e := newFISFaultEnv(t, "ec2")
				e.ec2Run(tc.spot, tc.stop)
				var created struct {
					ExperimentTemplate struct {
						ID string `json:"id"`
					} `json:"experimentTemplate"`
				}
				e.must(http.MethodPost, "/experimentTemplates", faultTemplate(tc.target, tc.action), &created)
				var started struct {
					Experiment struct {
						ID string `json:"id"`
					} `json:"experiment"`
				}
				e.must(http.MethodPost, "/experiments", map[string]any{"experimentTemplateId": created.ExperimentTemplate.ID}, &started)
				if tc.advance > 0 {
					require.NoError(t, e.fis.RunDue(e.ctx(), nil))
					e.tc.SetTime(fisClockInstant.Add(tc.advance))
				}
				e.state.fail = kind
				require.NoError(t, e.fis.RunDue(e.ctx(), nil), "an action's failure does not fail RunDue")
				e.state.fail = ""
				var got struct {
					Experiment fisExperimentOut `json:"experiment"`
				}
				e.must(http.MethodGet, "/experiments/"+started.Experiment.ID, nil, &got)
				if got.Experiment.State.Status == "failed" {
					assert.Contains(t, got.Experiment.State.Reason, errInjectedStore.Error())
				} else {
					assert.Equal(t, "completed", got.Experiment.State.Status, "%s", got.Experiment.State.Reason)
				}
			})
		}
	}
}
