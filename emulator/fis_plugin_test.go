package emulator_test

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// AWS Fault Injection Service (#1619). Every test freezes the clock and moves it by hand, so an
// experiment's progress never depends on real elapsed time.

var fisClockInstant = time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)

const (
	fisHost    = "fis.us-east-1.amazonaws.com"
	fisRoleARN = "arn:aws:iam::123456789012:role/aegc-fis"
)

type fisEnv struct {
	t  *testing.T
	ts *emulator.TestServer
}

func newFISEnv(t *testing.T, opts ...emulator.TestServerOption) *fisEnv {
	t.Helper()
	ts := emulator.StartTestServer(t, opts...)
	ts.FreezeTimeAt(fisClockInstant)
	return &fisEnv{t: t, ts: ts}
}

// do sends one request and returns its status and body.
func (e *fisEnv) do(method, host, path, contentType string, body []byte) (int, []byte) {
	e.t.Helper()
	req, err := http.NewRequestWithContext(e.t.Context(), method, e.ts.URL+path, bytes.NewReader(body))
	require.NoError(e.t, err)
	req.Host = host
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(e.t, err)
	defer resp.Body.Close() //nolint:errcheck
	out, err := io.ReadAll(resp.Body)
	require.NoError(e.t, err)
	return resp.StatusCode, out
}

// fis sends one FIS request with a JSON body.
func (e *fisEnv) fis(method, path string, body any) (int, []byte) {
	e.t.Helper()
	var b []byte
	if body != nil {
		var err error
		b, err = json.Marshal(body)
		require.NoError(e.t, err)
	}
	return e.do(method, fisHost, path, "application/json", b)
}

// mustFIS sends one FIS request that must succeed, decoding the answer into out when it is set.
func (e *fisEnv) mustFIS(method, path string, body, out any) {
	e.t.Helper()
	status, raw := e.fis(method, path, body)
	require.Equal(e.t, http.StatusOK, status, "%s %s: %s", method, path, raw)
	if out != nil {
		require.NoError(e.t, json.Unmarshal(raw, out), "%s", raw)
	}
}

// ec2 sends one EC2 Query request, decoding the XML answer into out when it is set.
func (e *fisEnv) ec2(params map[string]string, out any) {
	e.t.Helper()
	form := url.Values{}
	for k, v := range params {
		form.Set(k, v)
	}
	status, raw := e.do(http.MethodPost, "ec2.us-east-1.amazonaws.com", "/", "application/x-www-form-urlencoded", []byte(form.Encode()))
	require.Equal(e.t, http.StatusOK, status, "%v: %s", params["Action"], raw)
	if out != nil {
		require.NoError(e.t, xml.Unmarshal(raw, out), "%s", raw)
	}
}

// run launches one instance and returns its ID: a Spot Instance when spot is set, with the
// Spot request type and interruption behavior given, and the tags given either way.
func (e *fisEnv) run(spot bool, requestType, behavior string, tags map[string]string) string {
	e.t.Helper()
	params := map[string]string{
		"Action": "RunInstances", "ImageId": ec2TestImage, "InstanceType": "t3.micro", "MinCount": "1", "MaxCount": "1",
	}
	if spot {
		params["InstanceMarketOptions.MarketType"] = "spot"
		params["InstanceMarketOptions.SpotOptions.SpotInstanceType"] = requestType
		params["InstanceMarketOptions.SpotOptions.InstanceInterruptionBehavior"] = behavior
	}
	n := 1
	for k, v := range tags {
		params["TagSpecification.1.ResourceType"] = "instance"
		params["TagSpecification.1.Tag."+strconv.Itoa(n)+".Key"] = k
		params["TagSpecification.1.Tag."+strconv.Itoa(n)+".Value"] = v
		n++
	}
	var out struct {
		IDs []string `xml:"instancesSet>item>instanceId"`
	}
	e.ec2(params, &out)
	require.Len(e.t, out.IDs, 1)
	return out.IDs[0]
}

// fisInstance is the part of a described instance the tests read.
type fisInstance struct {
	ID                string `xml:"instanceId"`
	State             string `xml:"instanceState>name"`
	Reason            string `xml:"reason"`
	ReasonCode        string `xml:"stateReason>code"`
	ReasonMessage     string `xml:"stateReason>message"`
	SpotRequestID     string `xml:"spotInstanceRequestId"`
	InstanceLifecycle string `xml:"instanceLifecycle"`
}

// instance describes one instance.
func (e *fisEnv) instance(id string) fisInstance {
	e.t.Helper()
	var out struct {
		Instances []fisInstance `xml:"reservationSet>item>instancesSet>item"`
	}
	e.ec2(map[string]string{"Action": "DescribeInstances", "InstanceId.1": id}, &out)
	require.Len(e.t, out.Instances, 1)
	return out.Instances[0]
}

// spotRequest describes the Spot request behind an instance: its state and status code.
func (e *fisEnv) spotRequest(id string) (string, string) {
	e.t.Helper()
	var out struct {
		Requests []struct {
			State  string `xml:"state"`
			Status string `xml:"status>code"`
		} `xml:"spotInstanceRequestSet>item"`
	}
	e.ec2(map[string]string{"Action": "DescribeSpotInstanceRequests", "SpotInstanceRequestId.1": id}, &out)
	require.Len(e.t, out.Requests, 1)
	return out.Requests[0].State, out.Requests[0].Status
}

// fisExperimentOut is the part of an Experiment the tests read.
type fisExperimentOut struct {
	ID                   string `json:"id"`
	Arn                  string `json:"arn"`
	ExperimentTemplateID string `json:"experimentTemplateId"`
	State                struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	} `json:"state"`
	Actions map[string]struct {
		ActionID string `json:"actionId"`
		State    struct {
			Status string `json:"status"`
			Reason string `json:"reason"`
		} `json:"state"`
		StartTime *float64 `json:"startTime"`
		EndTime   *float64 `json:"endTime"`
	} `json:"actions"`
	StartTime float64  `json:"startTime"`
	EndTime   *float64 `json:"endTime"`
}

// template creates an experiment template and returns its ID.
func (e *fisEnv) template(body map[string]any) string {
	e.t.Helper()
	if _, ok := body["description"]; !ok {
		body["description"] = "test"
	}
	if _, ok := body["roleArn"]; !ok {
		body["roleArn"] = fisRoleARN
	}
	if _, ok := body["stopConditions"]; !ok {
		body["stopConditions"] = []any{map[string]any{"source": "none"}}
	}
	var out struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}
	e.mustFIS(http.MethodPost, "/experimentTemplates", body, &out)
	require.True(e.t, strings.HasPrefix(out.ExperimentTemplate.ID, "EXT"), out.ExperimentTemplate.ID)
	return out.ExperimentTemplate.ID
}

// start starts an experiment from a template.
func (e *fisEnv) start(templateID string) fisExperimentOut {
	e.t.Helper()
	var out struct {
		Experiment fisExperimentOut `json:"experiment"`
	}
	e.mustFIS(http.MethodPost, "/experiments", map[string]any{"experimentTemplateId": templateID}, &out)
	return out.Experiment
}

// experiment reads an experiment.
func (e *fisEnv) experiment(id string) fisExperimentOut {
	e.t.Helper()
	var out struct {
		Experiment fisExperimentOut `json:"experiment"`
	}
	e.mustFIS(http.MethodGet, "/experiments/"+id, nil, &out)
	return out.Experiment
}

// seed posts an experiment-result seed.
func (e *fisEnv) seed(body string) {
	e.t.Helper()
	status, out := e.do(http.MethodPost, "localhost", "/v1/fis/experiment-result", "application/json", []byte(body))
	require.Equal(e.t, http.StatusOK, status, "%s", out)
}

// spotTarget is a target of the tagged, running Spot Instances.
func spotTarget(selection string) map[string]any {
	return map[string]any{
		"resourceType": "aws:ec2:spot-instance", "resourceTags": map[string]string{"fis": "target"},
		"filters":       []any{map[string]any{"path": "State.Name", "values": []string{"running"}}},
		"selectionMode": selection,
	}
}

// instanceTarget is a target of the tagged instances.
func instanceTarget(selection string) map[string]any {
	return map[string]any{
		"resourceType": "aws:ec2:instance", "resourceTags": map[string]string{"fis": "target"}, "selectionMode": selection,
	}
}

func fisEpochTime(v float64) time.Time { return time.UnixMilli(int64(v * 1000)).UTC() }

// TestFIS_ExperimentsOnTheSimulatedClock runs one template per case and checks what each
// observation reports as the clock moves.
func TestFIS_ExperimentsOnTheSimulatedClock(t *testing.T) {
	t.Parallel()
	tagged := map[string]string{"fis": "target"}
	for _, tc := range []struct {
		name string
		run  func(t *testing.T, e *fisEnv)
	}{{
		// aegc's Spot-interruption tutorial: COUNT(1) of the tagged Spot Instances, PT2M.
		name: "spot interruption terminates the first instance by ID after PT2M",
		run: func(t *testing.T, e *fisEnv) {
			a := e.run(true, "one-time", "terminate", tagged)
			b := e.run(true, "one-time", "terminate", tagged)
			first, second := a, b
			if b < a {
				first, second = b, a
			}
			e.run(false, "", "", tagged) // On-Demand: never a Spot target.
			id := e.template(map[string]any{
				"targets": map[string]any{"oneSpotInstance": spotTarget("COUNT(1)")},
				"actions": map[string]any{"interruptSpotInstance": map[string]any{
					"actionId":   "aws:ec2:send-spot-instance-interruptions",
					"parameters": map[string]string{"durationBeforeInterruption": "PT2M"},
					"targets":    map[string]string{"SpotInstances": "oneSpotInstance"},
				}},
			})
			x := e.start(id)
			assert.Equal(t, "initiating", x.State.Status)
			assert.Equal(t, "Experiment is initiating.", x.State.Reason)
			assert.Equal(t, "pending", x.Actions["interruptSpotInstance"].State.Status)
			assert.Equal(t, "Initial state.", x.Actions["interruptSpotInstance"].State.Reason)

			var resolved struct {
				ResolvedTargets []struct {
					ResourceType      string            `json:"resourceType"`
					TargetName        string            `json:"targetName"`
					TargetInformation map[string]string `json:"targetInformation"`
				} `json:"resolvedTargets"`
			}
			e.mustFIS(http.MethodGet, "/experiments/"+x.ID+"/resolvedTargets", nil, &resolved)
			require.Len(t, resolved.ResolvedTargets, 1)
			assert.Equal(t, "arn:aws:ec2:us-east-1:123456789012:instance/"+first, resolved.ResolvedTargets[0].TargetInformation["arn"])

			got := e.experiment(x.ID)
			assert.Equal(t, "running", got.State.Status)
			assert.Equal(t, "running", got.Actions["interruptSpotInstance"].State.Status)

			e.ts.AdvanceTime(2*time.Minute - time.Second)
			got = e.experiment(x.ID)
			assert.Equal(t, "running", got.State.Status, "before PT2M the interruption has not happened")
			assert.Equal(t, "running", e.instance(first).State)

			e.ts.AdvanceTime(time.Second)
			got = e.experiment(x.ID)
			assert.Equal(t, "completed", got.State.Status)
			assert.Equal(t, "Experiment completed.", got.State.Reason)
			act := got.Actions["interruptSpotInstance"]
			assert.Equal(t, "completed", act.State.Status)
			assert.Equal(t, "Action was completed.", act.State.Reason)
			require.NotNil(t, act.StartTime)
			require.NotNil(t, act.EndTime)
			assert.Equal(t, fisClockInstant, fisEpochTime(*act.StartTime))
			assert.Equal(t, fisClockInstant.Add(2*time.Minute), fisEpochTime(*act.EndTime))
			require.NotNil(t, got.EndTime)
			assert.Equal(t, fisClockInstant.Add(2*time.Minute), fisEpochTime(*got.EndTime))

			inst := e.instance(first)
			assert.Equal(t, "terminated", inst.State)
			assert.Equal(t, "Server.SpotInstanceTermination", inst.ReasonCode)
			assert.True(t, strings.HasPrefix(inst.ReasonMessage, "Server.SpotInstanceTermination: "), inst.ReasonMessage)
			assert.Equal(t, "Service initiated (2026-11-24 10:02:00 GMT)", inst.Reason)
			state, status := e.spotRequest(inst.SpotRequestID)
			assert.Equal(t, "closed", state)
			assert.Equal(t, "instance-terminated-by-experiment", status)

			other := e.instance(second)
			assert.Equal(t, "running", other.State)
			assert.Empty(t, other.ReasonCode)
		},
	}, {
		name: "stop-instances restarts the instances after startInstancesAfterDuration",
		run: func(t *testing.T, e *fisEnv) {
			id1 := e.run(false, "", "", tagged)
			id := e.template(map[string]any{
				"targets": map[string]any{"all": instanceTarget("ALL")},
				"actions": map[string]any{"stop": map[string]any{
					"actionId":   "aws:ec2:stop-instances",
					"parameters": map[string]string{"startInstancesAfterDuration": "PT5M"},
					"targets":    map[string]string{"Instances": "all"},
				}},
			})
			x := e.start(id)
			assert.Equal(t, "running", e.experiment(x.ID).State.Status)
			inst := e.instance(id1)
			assert.Equal(t, "stopped", inst.State)
			assert.Equal(t, "Client.UserInitiatedShutdown", inst.ReasonCode)
			assert.Equal(t, "User initiated (2026-11-24 10:00:00 GMT)", inst.Reason)

			e.ts.AdvanceTime(5 * time.Minute)
			assert.Equal(t, "completed", e.experiment(x.ID).State.Status)
			inst = e.instance(id1)
			assert.Equal(t, "running", inst.State)
			assert.Empty(t, inst.ReasonCode, "the restart is the most recent transition")
		},
	}, {
		name: "terminate-instances terminates at once and a later action waits for it",
		run: func(t *testing.T, e *fisEnv) {
			id1 := e.run(false, "", "", tagged)
			id := e.template(map[string]any{
				"targets": map[string]any{"all": instanceTarget("ALL")},
				"actions": map[string]any{
					"wait": map[string]any{"actionId": "aws:fis:wait", "parameters": map[string]string{"duration": "PT1M"}},
					"terminate": map[string]any{
						"actionId": "aws:ec2:terminate-instances", "targets": map[string]string{"Instances": "all"},
						"startAfter": []string{"wait"},
					},
				},
			})
			x := e.start(id)
			got := e.experiment(x.ID)
			assert.Equal(t, "running", got.Actions["wait"].State.Status)
			assert.Equal(t, "pending", got.Actions["terminate"].State.Status)
			assert.Equal(t, "running", e.instance(id1).State)

			e.ts.AdvanceTime(time.Minute)
			got = e.experiment(x.ID)
			assert.Equal(t, "completed", got.State.Status)
			assert.Equal(t, fisClockInstant.Add(time.Minute), fisEpochTime(*got.Actions["terminate"].StartTime))
			inst := e.instance(id1)
			assert.Equal(t, "terminated", inst.State)
			assert.Equal(t, "Client.UserInitiatedShutdown", inst.ReasonCode)
		},
	}, {
		name: "a seeded failure ends the experiment with the seeded reason",
		run: func(t *testing.T, e *fisEnv) {
			id1 := e.run(true, "one-time", "terminate", tagged)
			id := e.template(map[string]any{
				"targets": map[string]any{"spot": spotTarget("ALL")},
				"actions": map[string]any{"interrupt": map[string]any{
					"actionId":   "aws:ec2:send-spot-instance-interruptions",
					"parameters": map[string]string{"durationBeforeInterruption": "PT2M"},
					"targets":    map[string]string{"SpotInstances": "spot"},
				}},
			})
			e.seed(`{"id":"` + id + `","status":"failed","reason":"Insufficient permissions."}`)
			x := e.start(id)
			got := e.experiment(x.ID)
			assert.Equal(t, "failed", got.State.Status)
			assert.Equal(t, "Insufficient permissions.", got.State.Reason)
			assert.Equal(t, "cancelled", got.Actions["interrupt"].State.Status)
			e.ts.AdvanceTime(5 * time.Minute)
			assert.Equal(t, "running", e.instance(id1).State, "a failed experiment interrupts nothing")
		},
	}, {
		name: "StopExperiment stops the running actions and cancels the pending ones",
		run: func(t *testing.T, e *fisEnv) {
			id1 := e.run(false, "", "", tagged)
			id := e.template(map[string]any{
				"targets": map[string]any{"all": instanceTarget("ALL")},
				"actions": map[string]any{
					"wait": map[string]any{"actionId": "aws:fis:wait", "parameters": map[string]string{"duration": "PT5M"}},
					"reboot": map[string]any{
						"actionId": "aws:ec2:reboot-instances", "targets": map[string]string{"Instances": "all"},
						"startAfter": []string{"wait"},
					},
				},
			})
			x := e.start(id)
			e.ts.AdvanceTime(time.Minute)
			var stopped struct {
				Experiment fisExperimentOut `json:"experiment"`
			}
			e.mustFIS(http.MethodDelete, "/experiments/"+x.ID, nil, &stopped)
			assert.Equal(t, "stopping", stopped.Experiment.State.Status)
			assert.Equal(t, "Stopping Experiment.", stopped.Experiment.State.Reason)

			got := e.experiment(x.ID)
			assert.Equal(t, "stopped", got.State.Status)
			assert.Equal(t, "stopped", got.Actions["wait"].State.Status)
			assert.Equal(t, "cancelled", got.Actions["reboot"].State.Status)
			assert.Equal(t, fisClockInstant.Add(time.Minute), fisEpochTime(*got.EndTime))

			status, out := e.fis(http.MethodDelete, "/experiments/"+x.ID, nil)
			assert.Equal(t, http.StatusBadRequest, status, "%s", out)
			assert.Contains(t, string(out), "ValidationException")
			assert.Equal(t, "running", e.instance(id1).State)
		},
	}, {
		name: "no resolved target fails the experiment with AWS's reason",
		run: func(t *testing.T, e *fisEnv) {
			e.run(false, "", "", tagged) // On-Demand only: the Spot target resolves to nothing.
			id := e.template(map[string]any{
				"targets": map[string]any{"spot": spotTarget("ALL")},
				"actions": map[string]any{"interrupt": map[string]any{
					"actionId":   "aws:ec2:send-spot-instance-interruptions",
					"parameters": map[string]string{"durationBeforeInterruption": "PT2M"},
					"targets":    map[string]string{"SpotInstances": "spot"},
				}},
			})
			x := e.start(id)
			got := e.experiment(x.ID)
			assert.Equal(t, "failed", got.State.Status)
			assert.Equal(t, "Target resolution returned empty set.", got.State.Reason)
			assert.Equal(t, "cancelled", got.Actions["interrupt"].State.Status)
		},
	}, {
		name: "no resolved target with emptyTargetResolutionMode skip skips the action",
		run: func(t *testing.T, e *fisEnv) {
			id := e.template(map[string]any{
				"targets":           map[string]any{"spot": spotTarget("ALL")},
				"experimentOptions": map[string]string{"emptyTargetResolutionMode": "skip"},
				"actions": map[string]any{"interrupt": map[string]any{
					"actionId":   "aws:ec2:send-spot-instance-interruptions",
					"parameters": map[string]string{"durationBeforeInterruption": "PT2M"},
					"targets":    map[string]string{"SpotInstances": "spot"},
				}},
			})
			x := e.start(id)
			got := e.experiment(x.ID)
			assert.Equal(t, "completed", got.State.Status)
			assert.Equal(t, "skipped", got.Actions["interrupt"].State.Status)
		},
	}, {
		name: "PERCENT rounds down and a stop-behavior Spot Instance is stopped",
		run: func(t *testing.T, e *fisEnv) {
			ids := []string{
				e.run(true, "persistent", "stop", tagged), e.run(true, "persistent", "stop", tagged),
				e.run(true, "persistent", "stop", tagged),
			}
			id := e.template(map[string]any{
				"targets": map[string]any{"spot": spotTarget("PERCENT(50)")},
				"actions": map[string]any{"interrupt": map[string]any{
					"actionId":   "aws:ec2:send-spot-instance-interruptions",
					"parameters": map[string]string{"durationBeforeInterruption": "PT2M"},
					"targets":    map[string]string{"SpotInstances": "spot"},
				}},
			})
			e.start(id)
			e.ts.AdvanceTime(2 * time.Minute)
			stopped := 0
			for _, i := range ids {
				inst := e.instance(i)
				if inst.State == "stopped" {
					stopped++
					assert.Equal(t, "Server.SpotInstanceShutdown", inst.ReasonCode)
					_, status := e.spotRequest(inst.SpotRequestID)
					assert.Equal(t, "instance-stopped-by-experiment", status)
				}
			}
			assert.Equal(t, 1, stopped, "50% of three rounds down to one")
		},
	}, {
		name: "skip-all skips every action and completes",
		run: func(t *testing.T, e *fisEnv) {
			id1 := e.run(false, "", "", tagged)
			id := e.template(map[string]any{
				"targets": map[string]any{"all": instanceTarget("ALL")},
				"actions": map[string]any{"terminate": map[string]any{
					"actionId": "aws:ec2:terminate-instances", "targets": map[string]string{"Instances": "all"},
				}},
			})
			var out struct {
				Experiment fisExperimentOut `json:"experiment"`
			}
			e.mustFIS(http.MethodPost, "/experiments", map[string]any{
				"experimentTemplateId": id, "experimentOptions": map[string]string{"actionsMode": "skip-all"},
			}, &out)
			got := e.experiment(out.Experiment.ID)
			assert.Equal(t, "completed", got.State.Status)
			assert.Equal(t, "skipped", got.Actions["terminate"].State.Status)
			assert.Equal(t, "running", e.instance(id1).State)
		},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t, newFISEnv(t))
		})
	}
}

// A recorded run replays identically: the experiment's EC2 effects are re-derived from the
// requests that observed them, so the state they left matches.
func TestFIS_AReplayReproducesTheExperimentsEffects(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t, withFISReplayRecording()...)
	tagged := map[string]string{"fis": "target"}
	e.run(true, "persistent", "stop", tagged)
	e.run(true, "one-time", "terminate", tagged)
	id := e.template(map[string]any{
		"targets": map[string]any{"spot": spotTarget("ALL")},
		"actions": map[string]any{"interrupt": map[string]any{
			"actionId":   "aws:ec2:send-spot-instance-interruptions",
			"parameters": map[string]string{"durationBeforeInterruption": "PT3M"},
			"targets":    map[string]string{"SpotInstances": "spot"},
		}},
	})
	x := e.start(id)
	e.ts.AdvanceTime(3 * time.Minute)
	require.Equal(t, "completed", e.experiment(x.ID).State.Status)
	assertFISReplays(t, e)
}

// withFISReplayRecording are the options a replay test's server records with.
func withFISReplayRecording() []emulator.TestServerOption {
	return []emulator.TestServerOption{emulator.WithRecordedBodies(), emulator.WithRecordedStateHashes()}
}

// assertFISReplays replays the server's stream and asserts it reproduced every answer and the
// state, with no dispatch to skip: an experiment's work is re-derived from the requests.
func assertFISReplays(t *testing.T, e *fisEnv) {
	t.Helper()
	engine := emulator.NewReplayEngine(e.ts.Store(), e.ts.StateManager(), e.ts.TimeController(), e.ts.Registry(),
		emulator.ReplayConfig{ValidateState: true}, emulator.NewDefaultLogger(slog.LevelError, false),
		emulator.WithControlPlaneHandler(e.ts.ControlPlaneHandler()))
	results, err := engine.Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	assert.Empty(t, results.Differences, "%s", replayDifferenceSummary(results))
	assert.True(t, results.StateValid)
	assert.Zero(t, results.SkippedEvents, "an experiment makes no recorded dispatch")
}

// A CloudWatch alarm stop condition stops a running experiment once the alarm is in ALARM.
func TestFIS_AnAlarmStopConditionStopsTheExperiment(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t)
	form := url.Values{
		"Action": {"PutMetricAlarm"}, "AlarmName": {"fis-guard"}, "MetricName": {"Errors"}, "Namespace": {"App"},
		"Statistic": {"Sum"}, "Period": {"60"}, "EvaluationPeriods": {"1"}, "Threshold": {"1"},
		"ComparisonOperator": {"GreaterThanThreshold"},
	}
	status, out := e.do(http.MethodPost, "monitoring.us-east-1.amazonaws.com", "/", "application/x-www-form-urlencoded", []byte(form.Encode()))
	require.Equal(t, http.StatusOK, status, "%s", out)
	id := e.template(map[string]any{
		"stopConditions": []any{map[string]any{
			"source": "aws:cloudwatch:alarm", "value": "arn:aws:cloudwatch:us-east-1:123456789012:alarm:fis-guard",
		}},
		"actions": map[string]any{"wait": map[string]any{"actionId": "aws:fis:wait", "parameters": map[string]string{"duration": "PT10M"}}},
	})
	x := e.start(id)
	assert.Equal(t, "running", e.experiment(x.ID).State.Status)
	e.ts.AdvanceTime(time.Minute)
	assert.Equal(t, "running", e.experiment(x.ID).State.Status, "the alarm is not in ALARM")

	form = url.Values{"Action": {"SetAlarmState"}, "AlarmName": {"fis-guard"}, "StateValue": {"ALARM"}, "StateReason": {"test"}}
	status, out = e.do(http.MethodPost, "monitoring.us-east-1.amazonaws.com", "/", "application/x-www-form-urlencoded", []byte(form.Encode()))
	require.Equal(t, http.StatusOK, status, "%s", out)
	got := e.experiment(x.ID)
	assert.Equal(t, "stopped", got.State.Status)
	assert.Equal(t, "Experiment stopped by a stop condition.", got.State.Reason)
	assert.Equal(t, "stopped", got.Actions["wait"].State.Status)
}

func TestFIS_SeedEndpoint(t *testing.T) {
	t.Parallel()
	e := newFISEnv(t)
	for _, body := range []string{`not json`, `{"status":"failed"}`, `{"id":"*","status":"running"}`} {
		status, out := e.do(http.MethodPost, "localhost", "/v1/fis/experiment-result", "application/json", []byte(body))
		assert.Equal(t, http.StatusBadRequest, status, "%s: %s", body, out)
	}
	id := e.template(map[string]any{
		"actions": map[string]any{"wait": map[string]any{"actionId": "aws:fis:wait", "parameters": map[string]string{"duration": "PT10M"}}},
	})
	// A wildcard completion ends the experiment at once; clearing it lets the next one run.
	e.seed(`{"id":"*","status":"completed"}`)
	x := e.start(id)
	got := e.experiment(x.ID)
	assert.Equal(t, "completed", got.State.Status)
	assert.Equal(t, "Experiment completed.", got.State.Reason)
	assert.Equal(t, "completed", got.Actions["wait"].State.Status)

	status, out := e.do(http.MethodDelete, "localhost", "/v1/fis/experiment-result?id=*", "", nil)
	require.Equal(t, http.StatusOK, status, "%s", out)
	y := e.start(id)
	assert.Equal(t, "running", e.experiment(y.ID).State.Status)

	// An experiment's own seed applies mid-run, with the default reason.
	e.seed(`{"id":"` + y.ID + `","status":"failed"}`)
	got = e.experiment(y.ID)
	assert.Equal(t, "failed", got.State.Status)
	assert.Equal(t, "Experiment failed.", got.State.Reason)
	assert.Equal(t, "failed", got.Actions["wait"].State.Status)
	status, out = e.do(http.MethodDelete, "localhost", "/v1/fis/experiment-result", "", nil)
	require.Equal(t, http.StatusOK, status, "%s", out)
}
