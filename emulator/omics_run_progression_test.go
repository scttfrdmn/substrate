package emulator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A HealthOmics run's status progression (#1371) and CancelRun's published contract (#1165). Every
// assertion is on the raw response bytes and status, over the wire, on a frozen clock.

// omicsWire issues one HealthOmics request against baseURL and returns its status and body.
func omicsWire(t *testing.T, baseURL, method, path string, body any) (int, []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		require.NoError(t, err)
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, baseURL+path, reader)
	require.NoError(t, err)
	req.Host = "omics.us-east-1.amazonaws.com"
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, out
}

// omicsSeedRunStatus posts one run-status seed and requires it accepted.
func omicsSeedRunStatus(t *testing.T, baseURL, body string) {
	t.Helper()
	resp, err := http.Post(baseURL+"/v1/omics/run-status", "application/json", strings.NewReader(body)) //nolint:noctx
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	msg, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "seed %s: %s", body, msg)
}

// omicsStart starts one run and returns its ID and the StartRun body.
func omicsStart(t *testing.T, baseURL, name string) (string, []byte) {
	t.Helper()
	status, body := omicsWire(t, baseURL, http.MethodPost, "/run", map[string]any{
		"workflowId": "1234567", "name": name, "requestId": "req-" + name,
		"roleArn": "arn:aws:iam::123456789012:role/omics", "outputUri": "s3://omics-out/" + name,
	})
	require.Equal(t, http.StatusCreated, status, "StartRun: %s", body)
	var out struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(body, &out), "%s", body)
	require.NotEmpty(t, out.ID)
	return out.ID, body
}

// omicsGetStatus returns GetRun's status and failureReason for runID, from the raw body.
func omicsGetStatus(t *testing.T, baseURL, runID string) (status, reason string, body []byte) {
	t.Helper()
	code, body := omicsWire(t, baseURL, http.MethodGet, "/run/"+runID, nil)
	require.Equal(t, http.StatusOK, code, "GetRun %s: %s", runID, body)
	var out struct {
		Status        string `json:"status"`
		FailureReason string `json:"failureReason"`
	}
	require.NoError(t, json.Unmarshal(body, &out), "%s", body)
	return out.Status, out.FailureReason, body
}

// omicsErrorCode returns the code a HealthOmics REST-JSON refusal carries.
func omicsErrorCode(t *testing.T, body []byte) string {
	t.Helper()
	var out struct {
		Code    string `json:"code"`
		Type    string `json:"__type"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(body, &out), "%s", body)
	if out.Code != "" {
		return out.Code
	}
	return out.Type
}

func TestOmicsRun_AnUnseededRunIsCompletedAndCannotBeCancelled(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	runID, started := omicsStart(t, ts.URL, "unseeded")

	require.Contains(t, string(started), `"status":"COMPLETED"`,
		"API_StartRun publishes status in the create response, and an unseeded run is COMPLETED at once: %s", started)
	status, _, _ := omicsGetStatus(t, ts.URL, runID)
	require.Equal(t, "COMPLETED", status, "the nominal success path, unchanged by #1371")

	code, body := omicsWire(t, ts.URL, http.MethodPost, "/run/"+runID+"/cancel", nil)
	require.Equal(t, http.StatusConflict, code, "a completed run is in no state a cancel applies to: %s", body)
	require.Contains(t, omicsErrorCode(t, body), "ConflictException", "%s", body)
	status, _, _ = omicsGetStatus(t, ts.URL, runID)
	require.Equal(t, "COMPLETED", status, "a refused cancel changes nothing")
}

func TestOmicsRun_ASeededRunWalksThePublishedStatusesAndSettles(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		seed   string
		want   []string
		reason string
	}{
		{"the published order, then COMPLETED", `{"runId":"*","pendingObservations":4}`,
			[]string{"PENDING", "STARTING", "RUNNING", "RUNNING", "COMPLETED", "COMPLETED"}, ""},
		{"FAILED with a seeded reason", `{"runId":"*","pendingObservations":2,"finalState":"FAILED","failureReason":"seeded OOM"}`,
			[]string{"PENDING", "STARTING", "FAILED", "FAILED"}, "seeded OOM"},
		{"a pinned start-up state", `{"runId":"*","pendingObservations":2,"state":"RUNNING"}`,
			[]string{"RUNNING", "RUNNING", "COMPLETED"}, ""},
		{"zero observations settles at once", `{"runId":"*","finalState":"FAILED"}`,
			[]string{"FAILED"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := emulator.StartTestServer(t)
			omicsSeedRunStatus(t, ts.URL, tc.seed)
			runID, started := omicsStart(t, ts.URL, "walk")
			require.Containsf(t, string(started), `"status":"`+tc.want[0]+`"`,
				"StartRun reports the first observation's status without spending it: %s", started)

			for i, want := range tc.want {
				status, reason, body := omicsGetStatus(t, ts.URL, runID)
				require.Equalf(t, want, status, "observation %d: %s", i+1, body)
				if want == "FAILED" {
					require.Equal(t, tc.reason, reason, "failureReason once FAILED: %s", body)
				} else {
					require.NotContains(t, string(body), "failureReason", "no failureReason before FAILED: %s", body)
				}
			}
		})
	}
}

func TestOmicsRun_ListAndGetAgreeAndEachCountsPerRun(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	omicsSeedRunStatus(t, ts.URL, `{"runId":"*","pendingObservations":2}`)
	a, _ := omicsStart(t, ts.URL, "a")
	b, _ := omicsStart(t, ts.URL, "b")

	// One list observes each run once (#582): neither run spends the other's countdown.
	code, list := omicsWire(t, ts.URL, http.MethodGet, "/run", nil)
	require.Equal(t, http.StatusOK, code, "%s", list)
	require.Equal(t, 2, strings.Count(string(list), `"status":"PENDING"`), "both runs' first observation: %s", list)

	status, _, _ := omicsGetStatus(t, ts.URL, a)
	require.Equal(t, "STARTING", status, "the list was a's first observation, so this is its second")
	status, _, _ = omicsGetStatus(t, ts.URL, b)
	require.Equal(t, "STARTING", status, "and b's")
	status, _, _ = omicsGetStatus(t, ts.URL, a)
	require.Equal(t, "COMPLETED", status)
}

func TestOmicsRun_AnIDScopedSeedBeatsTheWildcard(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	omicsSeedRunStatus(t, ts.URL, `{"runId":"*","pendingObservations":5}`)
	runID, _ := omicsStart(t, ts.URL, "scoped")
	omicsSeedRunStatus(t, ts.URL, `{"runId":"`+runID+`","finalState":"FAILED","failureReason":"scoped"}`)

	status, reason, _ := omicsGetStatus(t, ts.URL, runID)
	require.Equal(t, "FAILED", status, "the run's own seed governs")
	require.Equal(t, "scoped", reason)

	other, _ := omicsStart(t, ts.URL, "other")
	status, _, _ = omicsGetStatus(t, ts.URL, other)
	require.Equal(t, "PENDING", status, "and every other run keeps the wildcard")
}

func TestOmicsRun_CancelPassesThroughStoppingToCancelled(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		stopping string
		want     []string
	}{
		{"one STOPPING by default", ``, []string{"STOPPING", "CANCELLED", "CANCELLED"}},
		{"two when seeded", `,"stoppingObservations":2`, []string{"STOPPING", "STOPPING", "CANCELLED"}},
		{"none when seeded zero", `,"stoppingObservations":0`, []string{"CANCELLED", "CANCELLED"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := emulator.StartTestServer(t)
			omicsSeedRunStatus(t, ts.URL, `{"runId":"*","pendingObservations":5`+tc.stopping+`}`)
			runID, _ := omicsStart(t, ts.URL, "cancel")
			status, _, _ := omicsGetStatus(t, ts.URL, runID)
			require.Equal(t, "PENDING", status, "the run is active")

			code, body := omicsWire(t, ts.URL, http.MethodPost, "/run/"+runID+"/cancel", nil)
			require.Equal(t, http.StatusAccepted, code, "API_CancelRun publishes HTTP 202 (#1165): %s", body)
			require.Empty(t, body, "with an empty body")

			for i, want := range tc.want {
				status, _, body := omicsGetStatus(t, ts.URL, runID)
				require.Equalf(t, want, status, "observation %d after the cancel: %s", i+1, body)
				require.NotContains(t, string(body), `"CANCELED"`, "the published spelling has two Ls")
			}

			code, body = omicsWire(t, ts.URL, http.MethodPost, "/run/"+runID+"/cancel", nil)
			require.Equal(t, http.StatusConflict, code, "a cancelled run cannot be cancelled again: %s", body)
			require.Contains(t, omicsErrorCode(t, body), "ConflictException")

			code, body = omicsWire(t, ts.URL, http.MethodDelete, "/run/"+runID, nil)
			require.Equal(t, http.StatusAccepted, code, "and is CANCELLED, which DeleteRun accepts: %s", body)
		})
	}
}

func TestOmicsRun_DeleteRunRequiresASettledRunAndRemovesIt(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	omicsSeedRunStatus(t, ts.URL, `{"runId":"*","pendingObservations":1}`)
	runID, _ := omicsStart(t, ts.URL, "delete")

	code, body := omicsWire(t, ts.URL, http.MethodDelete, "/run/"+runID, nil)
	require.Equal(t, http.StatusConflict, code, "API_DeleteRun accepts only COMPLETED, FAILED or CANCELLED: %s", body)
	require.Contains(t, omicsErrorCode(t, body), "ConflictException")

	status, _, _ := omicsGetStatus(t, ts.URL, runID)
	require.Equal(t, "PENDING", status, "the refused delete spent no observation; this is the first")
	status, _, _ = omicsGetStatus(t, ts.URL, runID)
	require.Equal(t, "COMPLETED", status)

	code, body = omicsWire(t, ts.URL, http.MethodDelete, "/run/"+runID, nil)
	require.Equal(t, http.StatusAccepted, code, "%s", body)
	require.Empty(t, body)

	code, body = omicsWire(t, ts.URL, http.MethodGet, "/run/"+runID, nil)
	require.Equal(t, http.StatusNotFound, code, "GetRun cannot find a deleted run, as the page says: %s", body)
	code, list := omicsWire(t, ts.URL, http.MethodGet, "/run", nil)
	require.Equal(t, http.StatusOK, code)
	require.NotContains(t, string(list), runID, "and ListRuns no longer lists it: %s", list)
}

func TestOmicsRun_RoutesByWholeSegment(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	runID, _ := omicsStart(t, ts.URL, "routes")
	for _, tc := range []struct {
		method, path string
	}{
		{http.MethodPost, "/run/" + runID},
		{http.MethodPost, "/run/" + runID + "/cancel/x"},
		{http.MethodPost, "/run/" + runID + "/cancelled"},
		{http.MethodGet, "/run/" + runID + "/cancel"},
		{http.MethodDelete, "/run"},
		{http.MethodGet, "/runs"},
	} {
		code, body := omicsWire(t, ts.URL, tc.method, tc.path, nil)
		require.Equalf(t, http.StatusNotFound, code, "%s %s is no published HealthOmics URI: %s", tc.method, tc.path, body)
	}
}

func TestOmicsRun_TheSeedEndpointRefusesWhatThePagesDoNotPublish(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	for _, body := range []string{
		`{"pendingObservations":-1}`,
		`{"state":"STOPPING"}`,
		`{"state":"COMPLETED"}`,
		`{"finalState":"CANCELLED"}`,
		`{"finalState":"DONE"}`,
		`{"stoppingObservations":-1}`,
		`{"finalState":"FAILED","failureReason":"` + strings.Repeat("x", 65) + `"}`,
	} {
		resp, err := http.Post(ts.URL+"/v1/omics/run-status", "application/json", strings.NewReader(body)) //nolint:noctx
		require.NoError(t, err)
		msg, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		require.Equalf(t, http.StatusBadRequest, resp.StatusCode, "seed %s: %s", body, msg)
	}

	// And DELETE clears: a run started after the clear is COMPLETED at once.
	omicsSeedRunStatus(t, ts.URL, `{"runId":"*","pendingObservations":3}`)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, ts.URL+"/v1/omics/run-status", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	runID, _ := omicsStart(t, ts.URL, "cleared")
	status, _, _ := omicsGetStatus(t, ts.URL, runID)
	require.Equal(t, "COMPLETED", status)
}

// TestOmicsRun_AProgressionReplaysIdentically records a seeded run's whole life — seed, start,
// polls, cancel, polls — and replays it with the seed re-issued in position (#1140). Every replayed
// response must equal the recorded one.
func TestOmicsRun_AProgressionReplaysIdentically(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())
	omicsSeedRunStatus(t, ts.URL, `{"runId":"*","pendingObservations":3,"stoppingObservations":1}`)
	runID, _ := omicsStart(t, ts.URL, "replay")

	var recorded []string
	for range 2 {
		status, _, _ := omicsGetStatus(t, ts.URL, runID)
		recorded = append(recorded, status)
	}
	code, _ := omicsWire(t, ts.URL, http.MethodPost, "/run/"+runID+"/cancel", nil)
	require.Equal(t, http.StatusAccepted, code)
	for range 2 {
		status, _, _ := omicsGetStatus(t, ts.URL, runID)
		recorded = append(recorded, status)
	}
	require.Equal(t, []string{"PENDING", "STARTING", "STOPPING", "CANCELLED"}, recorded,
		"the recording has to walk the progression, or the replay has nothing to reproduce")

	results, err := pipelineReplayEngine(ts, emulator.ReplayPipeline{}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	require.Zero(t, results.SkippedEvents, "the seed is re-executed, not skipped")
	require.Zero(t, results.FailedEvents)
	require.Empty(t, results.Differences, "every replayed status matches the recording: %s", replayDifferenceSummary(results))
}

// TestOmicsRun_AStoreFaultIsAnError requires every read and write the progression adds to answer an
// error, never a published refusal or a status it did not compute.
func TestOmicsRun_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		arm    func(*cfFaultStateManager)
		method string
		suffix string
	}{
		{"GetRun, run read", func(m *cfFaultStateManager) { m.failGet = "run:" }, http.MethodGet, ""},
		{"GetRun, seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, http.MethodGet, ""},
		{"GetRun, counter read", func(m *cfFaultStateManager) { m.failGet = "observed:" }, http.MethodGet, ""},
		{"GetRun, counter write", func(m *cfFaultStateManager) { m.failPut = "observed:" }, http.MethodGet, ""},
		{"ListRuns, run read", func(m *cfFaultStateManager) { m.failGet = "run:" }, http.MethodGet, "list"},
		{"ListRuns, index read", func(m *cfFaultStateManager) { m.failGet = "run_ids:" }, http.MethodGet, "list"},
		{"CancelRun, seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, http.MethodPost, "/cancel"},
		{"CancelRun, record write", func(m *cfFaultStateManager) { m.failPut = "run:" }, http.MethodPost, "/cancel"},
		{"CancelRun, countdown reset", func(m *cfFaultStateManager) { m.failDelete = "observed:" }, http.MethodPost, "/cancel"},
		{"DeleteRun, record delete", func(m *cfFaultStateManager) { m.failDelete = "run:" }, http.MethodDelete, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			p := &emulator.OmicsPlugin{}
			ctx, _ := wireSetup(t, p, "req-omics-fault")
			require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
				State: fault, Logger: emulator.NewDefaultLogger(0, false),
				Options: map[string]any{"time_controller": emulator.NewTimeController(omicsFrozenInstant)},
			}))
			// An active run: a wildcard seed written where the endpoint would write it.
			seed, err := json.Marshal(map[string]any{"runId": "*", "pendingObservations": 3})
			require.NoError(t, err)
			require.NoError(t, fault.Put(context.Background(), "omics-run-ctrl", "status:*", seed))
			started := omicsDirect(t, p, ctx, http.MethodPost, "/run", `{"name":"fault"}`)
			var out struct {
				ID string `json:"id"`
			}
			require.NoError(t, json.Unmarshal(started, &out))
			if tc.method == http.MethodDelete {
				// DeleteRun needs a settled run: clear the seed so the run reads COMPLETED.
				require.NoError(t, fault.Delete(context.Background(), "omics-run-ctrl", "status:*"))
			}

			path := "/run/" + out.ID + tc.suffix
			if tc.suffix == "list" {
				path = "/run"
			}
			tc.arm(fault)
			_, err = p.HandleRequest(ctx, &emulator.AWSRequest{
				Service: "omics", HTTPMethod: tc.method, Path: path,
				Headers: map[string]string{}, Params: map[string]string{},
			})
			require.Error(t, err, "%s must fail on a store fault", tc.name)
			var awsErr *emulator.AWSError
			require.Falsef(t, errors.As(err, &awsErr), "%s answered a store fault as the published %v", tc.name, awsErr)
		})
	}
}

// omicsDirect issues one request to the plugin directly and requires a 2xx.
func omicsDirect(t *testing.T, p *emulator.OmicsPlugin, ctx *emulator.RequestContext, method, path, body string) []byte {
	t.Helper()
	resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
		Service: "omics", HTTPMethod: method, Path: path, Body: []byte(body),
		Headers: map[string]string{"Content-Type": "application/json"}, Params: map[string]string{},
	})
	require.NoError(t, err, "%s %s", method, path)
	require.Less(t, resp.StatusCode, 300, "%s %s: %s", method, path, resp.Body)
	return resp.Body
}
