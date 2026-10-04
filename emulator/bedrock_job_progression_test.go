package emulator_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A Bedrock batch-inference job's lifecycle (#1174): the five Required members of
// CreateModelInvocationJob, the seeded status countdown, StopModelInvocationJob through Stopping
// to Stopped, the published ConflictException for a terminal job, and a replay of all of it.
// Every assertion is on the raw response, over the wire, on a frozen clock.

// bedrockJobWireClock is the instant every test here freezes the clock at.
var bedrockJobWireClock = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// bedrockJobBody is a CreateModelInvocationJob body carrying all five Required members.
func bedrockJobBody(name string) map[string]any {
	return map[string]any{
		"jobName":          name,
		"modelId":          "anthropic.claude-3-haiku-20240307-v1:0",
		"roleArn":          "arn:aws:iam::123456789012:role/BedrockBatch",
		"inputDataConfig":  map[string]any{"s3InputDataConfig": map[string]string{"s3Uri": "s3://in/"}},
		"outputDataConfig": map[string]any{"s3OutputDataConfig": map[string]string{"s3Uri": "s3://out/"}},
	}
}

// bedrockJobCall issues one request to baseURL on the bedrock host and returns its status and body.
func bedrockJobCall(t *testing.T, baseURL, method, path string, body any) (int, []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		reader = strings.NewReader(string(raw))
	}
	req, err := http.NewRequestWithContext(t.Context(), method, baseURL+path, reader)
	require.NoError(t, err)
	req.Host = "bedrock.us-east-1.amazonaws.com"
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

// bedrockJobOK issues one request and requires a 2xx.
func bedrockJobOK(t *testing.T, baseURL, method, path string, body any) []byte {
	t.Helper()
	status, out := bedrockJobCall(t, baseURL, method, path, body)
	require.Lessf(t, status, 300, "%s %s: %s", method, path, out)
	return out
}

// bedrockJobCreate creates a job and returns its ID.
func bedrockJobCreate(t *testing.T, baseURL, name string) string {
	t.Helper()
	var out struct {
		JobArn string `json:"jobArn"`
	}
	require.NoError(t, json.Unmarshal(bedrockJobOK(t, baseURL, http.MethodPost, "/model-invocation-job", bedrockJobBody(name)), &out))
	require.NotEmpty(t, out.JobArn)
	return out.JobArn[strings.LastIndexByte(out.JobArn, '/')+1:]
}

// bedrockJobRead answers the status and message one GetModelInvocationJob reports.
func bedrockJobRead(t *testing.T, baseURL, jobID string) (status, message string) {
	t.Helper()
	var got struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(bedrockJobOK(t, baseURL, http.MethodGet, "/model-invocation-job/"+jobID, nil), &got))
	return got.Status, got.Message
}

// bedrockJobListStatuses answers every job's status one ListModelInvocationJobs reports, by job ARN suffix.
func bedrockJobListStatuses(t *testing.T, baseURL string) map[string]string {
	t.Helper()
	var listed struct {
		Summaries []struct {
			JobArn string `json:"jobArn"`
			Status string `json:"status"`
		} `json:"invocationJobSummaries"`
	}
	require.NoError(t, json.Unmarshal(bedrockJobOK(t, baseURL, http.MethodGet, "/model-invocation-jobs", nil), &listed))
	out := map[string]string{}
	for _, s := range listed.Summaries {
		out[s.JobArn[strings.LastIndexByte(s.JobArn, '/')+1:]] = s.Status
	}
	return out
}

// bedrockJobErrorCode answers the code of a refusal body.
func bedrockJobErrorCode(t *testing.T, body []byte) string {
	t.Helper()
	var e struct {
		Type    string `json:"__type"`
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(body, &e), "%s", body)
	if e.Type != "" {
		return e.Type
	}
	return e.Code
}

// bedrockJobServer starts a frozen-clock TestServer with recorded bodies.
func bedrockJobServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())
	ts.FreezeTimeAt(bedrockJobWireClock)
	return ts
}

func TestBedrockJob_CreateRefusesEachMissingRequiredMember(t *testing.T) {
	t.Parallel()
	ts := bedrockJobServer(t)
	for _, member := range []string{"jobName", "modelId", "roleArn", "inputDataConfig", "outputDataConfig"} {
		t.Run(member, func(t *testing.T) {
			body := bedrockJobBody("req-" + member)
			delete(body, member)
			status, out := bedrockJobCall(t, ts.URL, http.MethodPost, "/model-invocation-job", body)
			require.Equal(t, http.StatusBadRequest, status, "%s", out)
			assert.Contains(t, bedrockJobErrorCode(t, out), "ValidationException", "%s", out)
			assert.Contains(t, string(out), member+" is required")
		})
	}
	t.Run("a JSON null is not a value", func(t *testing.T) {
		body := bedrockJobBody("req-null")
		body["inputDataConfig"] = nil
		status, out := bedrockJobCall(t, ts.URL, http.MethodPost, "/model-invocation-job", body)
		require.Equal(t, http.StatusBadRequest, status, "%s", out)
		assert.Contains(t, string(out), "inputDataConfig is required")
	})
	assert.Empty(t, bedrockJobListStatuses(t, ts.URL), "no refused create left a job behind")
}

func TestBedrockJob_AnUnseededJobStaysSubmitted(t *testing.T) {
	t.Parallel()
	ts := bedrockJobServer(t)
	id := bedrockJobCreate(t, ts.URL, "unseeded")
	for range 3 {
		status, _ := bedrockJobRead(t, ts.URL, id)
		require.Equal(t, "Submitted", status, "substrate runs no inference, so nothing moves an unseeded job")
	}
}

func TestBedrockJob_ASeededStatusCountsDownThenSettles(t *testing.T) {
	t.Parallel()
	ts := bedrockJobServer(t)
	id := bedrockJobCreate(t, ts.URL, "countdown")
	bedrockJobOK(t, ts.URL, http.MethodPost, "/v1/bedrock/model-invocation-job-status", map[string]any{
		"jobId": id, "pendingObservations": 2, "status": "Failed", "message": "input not found",
	})

	for i, want := range []string{"InProgress", "InProgress", "Failed", "Failed"} {
		status, message := bedrockJobRead(t, ts.URL, id)
		require.Equalf(t, want, status, "observation %d", i+1)
		if want == "Failed" {
			assert.Equal(t, "input not found", message, "the message arrives with the terminal status")
		} else {
			assert.Empty(t, message, "no failure message while the job is in progress")
		}
	}

	t.Run("a custom pending status", func(t *testing.T) {
		other := bedrockJobCreate(t, ts.URL, "validating")
		bedrockJobOK(t, ts.URL, http.MethodPost, "/v1/bedrock/model-invocation-job-status", map[string]any{
			"jobId": other, "pendingObservations": 1, "pendingStatus": "Validating", "status": "Completed",
		})
		first, _ := bedrockJobRead(t, ts.URL, other)
		second, _ := bedrockJobRead(t, ts.URL, other)
		assert.Equal(t, []string{"Validating", "Completed"}, []string{first, second})
	})
}

// A seed with no pendingObservations settles at once, which is the static override the endpoint
// was before #1174, so a seed body written for it reads the same.
func TestBedrockJob_ASeedWithoutACountReadsAsTheOldOverride(t *testing.T) {
	t.Parallel()
	ts := bedrockJobServer(t)
	id := bedrockJobCreate(t, ts.URL, "legacy")
	bedrockJobOK(t, ts.URL, http.MethodPost, "/v1/bedrock/model-invocation-job-status", map[string]any{
		"jobId": id, "status": "Completed", "message": "all done",
	})
	status, message := bedrockJobRead(t, ts.URL, id)
	assert.Equal(t, "Completed", status)
	assert.Equal(t, "all done", message)
}

// One List over N jobs under a wildcard spends one observation of each, not N of one (#582).
func TestBedrockJob_AListObservesEachJobOnce(t *testing.T) {
	t.Parallel()
	ts := bedrockJobServer(t)
	a, b := bedrockJobCreate(t, ts.URL, "list-a"), bedrockJobCreate(t, ts.URL, "list-b")
	bedrockJobOK(t, ts.URL, http.MethodPost, "/v1/bedrock/model-invocation-job-status", map[string]any{
		"pendingObservations": 1, "status": "Completed",
	})
	assert.Equal(t, map[string]string{a: "InProgress", b: "InProgress"}, bedrockJobListStatuses(t, ts.URL))
	assert.Equal(t, map[string]string{a: "Completed", b: "Completed"}, bedrockJobListStatuses(t, ts.URL))
}

func TestBedrockJob_StopPassesThroughStopping(t *testing.T) {
	t.Parallel()
	ts := bedrockJobServer(t)

	t.Run("unseeded: one Stopping read, then Stopped", func(t *testing.T) {
		id := bedrockJobCreate(t, ts.URL, "stop-default")
		body := bedrockJobOK(t, ts.URL, http.MethodPost, "/model-invocation-job/"+id+"/stop", nil)
		assert.JSONEq(t, `{}`, string(body))
		for _, want := range []string{"Stopping", "Stopped", "Stopped"} {
			status, _ := bedrockJobRead(t, ts.URL, id)
			require.Equal(t, want, status)
		}
	})

	t.Run("a repeated stop while stopping succeeds and does not restart the countdown", func(t *testing.T) {
		id := bedrockJobCreate(t, ts.URL, "stop-twice")
		bedrockJobOK(t, ts.URL, http.MethodPost, "/v1/bedrock/model-invocation-job-stop", map[string]any{
			"jobId": id, "stoppingObservations": 2,
		})
		bedrockJobOK(t, ts.URL, http.MethodPost, "/model-invocation-job/"+id+"/stop", nil)
		status, _ := bedrockJobRead(t, ts.URL, id)
		require.Equal(t, "Stopping", status)
		bedrockJobOK(t, ts.URL, http.MethodPost, "/model-invocation-job/"+id+"/stop", nil)
		first, _ := bedrockJobRead(t, ts.URL, id)
		second, _ := bedrockJobRead(t, ts.URL, id)
		assert.Equal(t, []string{"Stopping", "Stopped"}, []string{first, second},
			"the second stop left the countdown where it was rather than restarting it")
	})

	t.Run("a seeded stop countdown", func(t *testing.T) {
		id := bedrockJobCreate(t, ts.URL, "stop-seeded")
		bedrockJobOK(t, ts.URL, http.MethodPost, "/v1/bedrock/model-invocation-job-stop", map[string]any{
			"jobId": id, "stoppingObservations": 3,
		})
		bedrockJobOK(t, ts.URL, http.MethodPost, "/model-invocation-job/"+id+"/stop", nil)
		var seen []string
		for range 4 {
			status, _ := bedrockJobRead(t, ts.URL, id)
			seen = append(seen, status)
		}
		assert.Equal(t, []string{"Stopping", "Stopping", "Stopping", "Stopped"}, seen)
	})

	t.Run("a zero stop countdown settles at Stopped at once", func(t *testing.T) {
		id := bedrockJobCreate(t, ts.URL, "stop-zero")
		bedrockJobOK(t, ts.URL, http.MethodPost, "/v1/bedrock/model-invocation-job-stop", map[string]any{
			"jobId": id, "stoppingObservations": 0,
		})
		bedrockJobOK(t, ts.URL, http.MethodPost, "/model-invocation-job/"+id+"/stop", nil)
		status, _ := bedrockJobRead(t, ts.URL, id)
		assert.Equal(t, "Stopped", status)
	})

	t.Run("a stop overrides a pending status seed", func(t *testing.T) {
		id := bedrockJobCreate(t, ts.URL, "stop-over-seed")
		bedrockJobOK(t, ts.URL, http.MethodPost, "/v1/bedrock/model-invocation-job-status", map[string]any{
			"jobId": id, "pendingObservations": 5, "status": "Completed",
		})
		status, _ := bedrockJobRead(t, ts.URL, id)
		require.Equal(t, "InProgress", status)
		bedrockJobOK(t, ts.URL, http.MethodPost, "/model-invocation-job/"+id+"/stop", nil)
		first, _ := bedrockJobRead(t, ts.URL, id)
		second, _ := bedrockJobRead(t, ts.URL, id)
		assert.Equal(t, []string{"Stopping", "Stopped"}, []string{first, second})
	})
}

func TestBedrockJob_StoppingATerminalJobIsAConflict(t *testing.T) {
	t.Parallel()
	ts := bedrockJobServer(t)
	for _, terminal := range []string{"Completed", "Failed", "PartiallyCompleted", "Expired"} {
		t.Run(terminal, func(t *testing.T) {
			id := bedrockJobCreate(t, ts.URL, "terminal-"+strings.ToLower(terminal))
			bedrockJobOK(t, ts.URL, http.MethodPost, "/v1/bedrock/model-invocation-job-status", map[string]any{
				"jobId": id, "status": terminal,
			})
			status, out := bedrockJobCall(t, ts.URL, http.MethodPost, "/model-invocation-job/"+id+"/stop", nil)
			require.Equal(t, http.StatusBadRequest, status, "%s", out)
			assert.Contains(t, bedrockJobErrorCode(t, out), "ConflictException", "%s", out)
			got, _ := bedrockJobRead(t, ts.URL, id)
			assert.Equal(t, terminal, got, "a refused stop does not overwrite the terminal status")
		})
	}
	t.Run("Stopped", func(t *testing.T) {
		id := bedrockJobCreate(t, ts.URL, "terminal-stopped")
		bedrockJobOK(t, ts.URL, http.MethodPost, "/model-invocation-job/"+id+"/stop", nil)
		bedrockJobRead(t, ts.URL, id) // Stopping
		bedrockJobRead(t, ts.URL, id) // Stopped
		status, out := bedrockJobCall(t, ts.URL, http.MethodPost, "/model-invocation-job/"+id+"/stop", nil)
		require.Equal(t, http.StatusBadRequest, status, "%s", out)
		assert.Contains(t, bedrockJobErrorCode(t, out), "ConflictException")
	})
	t.Run("the precondition peeks, so a refused stop spends no observation", func(t *testing.T) {
		id := bedrockJobCreate(t, ts.URL, "terminal-peek")
		bedrockJobOK(t, ts.URL, http.MethodPost, "/v1/bedrock/model-invocation-job-status", map[string]any{
			"jobId": id, "pendingObservations": 1, "status": "Completed",
		})
		// The stop is allowed (the job is InProgress), so use a fresh job for the peek check: a
		// stop's precondition must not spend the job's one pending observation.
		other := bedrockJobCreate(t, ts.URL, "terminal-peek-other")
		bedrockJobOK(t, ts.URL, http.MethodPost, "/v1/bedrock/model-invocation-job-status", map[string]any{
			"jobId": other, "pendingObservations": 1, "status": "Completed",
		})
		status, _ := bedrockJobRead(t, ts.URL, other) // spends the one pending observation
		require.Equal(t, "InProgress", status)
		code, out := bedrockJobCall(t, ts.URL, http.MethodPost, "/model-invocation-job/"+other+"/stop", nil)
		require.Equal(t, http.StatusBadRequest, code, "%s", out)
		status, _ = bedrockJobRead(t, ts.URL, other)
		assert.Equal(t, "Completed", status)

		bedrockJobOK(t, ts.URL, http.MethodPost, "/model-invocation-job/"+id+"/stop", nil)
		status, _ = bedrockJobRead(t, ts.URL, id)
		assert.Equal(t, "Stopping", status, "stopping an in-progress job peeked rather than spending its countdown")
	})
}

func TestBedrockJob_SeedsAreValidatedAgainstThePublishedEnum(t *testing.T) {
	t.Parallel()
	ts := bedrockJobServer(t)
	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{"missing status", map[string]any{"jobId": "x"}},
		{"unpublished status", map[string]any{"jobId": "x", "status": "Done"}},
		{"unpublished pending status", map[string]any{"jobId": "x", "status": "Completed", "pendingStatus": "Running"}},
		{"negative count", map[string]any{"jobId": "x", "status": "Completed", "pendingObservations": -1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, out := bedrockJobCall(t, ts.URL, http.MethodPost, "/v1/bedrock/model-invocation-job-status", tc.body)
			assert.Equal(t, http.StatusBadRequest, status, "%s", out)
		})
	}
	status, out := bedrockJobCall(t, ts.URL, http.MethodPost, "/v1/bedrock/model-invocation-job-stop",
		map[string]any{"jobId": "x", "stoppingObservations": -1})
	assert.Equal(t, http.StatusBadRequest, status, "%s", out)
}

func TestBedrockJob_ClearingSeedsRestoresTheUnseededLifecycle(t *testing.T) {
	t.Parallel()
	ts := bedrockJobServer(t)
	id := bedrockJobCreate(t, ts.URL, "clear")
	bedrockJobOK(t, ts.URL, http.MethodPost, "/v1/bedrock/model-invocation-job-status", map[string]any{"status": "Failed"})
	status, _ := bedrockJobRead(t, ts.URL, id)
	require.Equal(t, "Failed", status)

	bedrockJobOK(t, ts.URL, http.MethodDelete, "/v1/bedrock/model-invocation-job-status?jobId=*", nil)
	status, _ = bedrockJobRead(t, ts.URL, id)
	assert.Equal(t, "Submitted", status)

	bedrockJobOK(t, ts.URL, http.MethodPost, "/v1/bedrock/model-invocation-job-stop", map[string]any{"stoppingObservations": 0})
	bedrockJobOK(t, ts.URL, http.MethodDelete, "/v1/bedrock/model-invocation-job-stop", nil)
	bedrockJobOK(t, ts.URL, http.MethodPost, "/model-invocation-job/"+id+"/stop", nil)
	status, _ = bedrockJobRead(t, ts.URL, id)
	assert.Equal(t, "Stopping", status, "with the stop seed cleared, the default one Stopping read applies")
}

// A recording of the whole lifecycle — create, seeded countdown, a refused stop, an accepted stop —
// replays with no difference, because both seeds are control-plane events re-applied in position
// and every counter lives in the StateManager (#1140).
func TestBedrockJob_AReplayReproducesTheLifecycle(t *testing.T) {
	t.Parallel()
	ts := bedrockJobServer(t)

	a := bedrockJobCreate(t, ts.URL, "replay-a")
	b := bedrockJobCreate(t, ts.URL, "replay-b")
	bedrockJobOK(t, ts.URL, http.MethodPost, "/v1/bedrock/model-invocation-job-status", map[string]any{
		"jobId": a, "pendingObservations": 1, "status": "Completed",
	})
	bedrockJobOK(t, ts.URL, http.MethodPost, "/v1/bedrock/model-invocation-job-stop", map[string]any{
		"jobId": b, "stoppingObservations": 2,
	})
	bedrockJobRead(t, ts.URL, a) // InProgress
	bedrockJobRead(t, ts.URL, a) // Completed
	status, _ := bedrockJobCall(t, ts.URL, http.MethodPost, "/model-invocation-job/"+a+"/stop", nil)
	require.Equal(t, http.StatusBadRequest, status)
	bedrockJobOK(t, ts.URL, http.MethodPost, "/model-invocation-job/"+b+"/stop", nil)
	for range 3 {
		bedrockJobRead(t, ts.URL, b) // Stopping, Stopping, Stopped
	}
	bedrockJobListStatuses(t, ts.URL)

	results, err := pipelineReplayEngine(ts, emulator.ReplayPipeline{}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	require.Positive(t, results.TotalEvents)
	assert.Equal(t, 1, results.FailedEvents,
		"exactly one event replays as a refusal: the refused stop, which refused in the recording too")
	assert.Empty(t, results.Differences, "%s", replayDifferenceSummary(results))
}

// bedrockJobFaultServer starts a server whose plugin and control plane share fault.
func bedrockJobFaultServer(t *testing.T, fault *cfFaultStateManager) string {
	t.Helper()
	registry := emulator.NewPluginRegistry()
	store := emulator.NewEventStore(emulator.EventStoreConfig{Enabled: true, Backend: "memory"})
	tc := emulator.NewTimeController(bedrockJobWireClock)
	tc.Freeze()
	tc.SetTime(bedrockJobWireClock)
	logger := emulator.NewDefaultLogger(0, false)
	p := &emulator.BedrockRuntimePlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{ //nolint:contextcheck
		State: fault, Logger: logger, Options: map[string]any{"time_controller": tc},
	}))
	registry.Register(p)
	srv := httptest.NewServer(emulator.NewServer(*emulator.DefaultConfig(), registry, store, fault, tc, logger))
	t.Cleanup(srv.Close)
	return srv.URL
}

// A store fault on any read or write the lifecycle makes is an error, never a published refusal
// or a 2xx over a countdown that was not read or written.
func TestBedrockJob_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		arm    func(*cfFaultStateManager)
		stop   bool // drive StopModelInvocationJob rather than GetModelInvocationJob
		list   bool // drive ListModelInvocationJobs
		stoppd bool // stop the job before arming
	}{
		{"Get, record read", func(m *cfFaultStateManager) { m.failGet = "job:" }, false, false, false},
		{"Get, status seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, false, false, false},
		{"Get, counter write", func(m *cfFaultStateManager) { m.failPut = "observed:" }, false, false, false},
		{"Get, stopping counter read", func(m *cfFaultStateManager) { m.failGet = "observed:" }, false, false, true},
		{"Get, stop seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, false, false, true},
		{"Get, stopping counter write", func(m *cfFaultStateManager) { m.failPut = "observed:" }, false, false, true},
		{"Stop, record write", func(m *cfFaultStateManager) { m.failPut = "job:" }, true, false, false},
		{"Stop, counter reset", func(m *cfFaultStateManager) { m.failDelete = "observed:" }, true, false, false},
		{"Stop, status seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, true, false, false},
		{"List, index read", func(m *cfFaultStateManager) { m.failGet = "job_ids:" }, false, true, false},
		{"List, record read", func(m *cfFaultStateManager) { m.failGet = "job:" }, false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			url := bedrockJobFaultServer(t, fault)
			id := bedrockJobCreate(t, url, "fault")
			// A pending seed, so the status-seed path and its counter write run on the read.
			bedrockJobOK(t, url, http.MethodPost, "/v1/bedrock/model-invocation-job-status", map[string]any{
				"jobId": id, "pendingObservations": 3, "status": "Completed",
			})
			if tc.stoppd {
				bedrockJobOK(t, url, http.MethodPost, "/v1/bedrock/model-invocation-job-stop", map[string]any{
					"jobId": id, "stoppingObservations": 3,
				})
				bedrockJobOK(t, url, http.MethodPost, "/model-invocation-job/"+id+"/stop", nil)
			}

			tc.arm(fault)
			method, path := http.MethodGet, "/model-invocation-job/"+id
			switch {
			case tc.stop:
				method, path = http.MethodPost, "/model-invocation-job/"+id+"/stop"
			case tc.list:
				path = "/model-invocation-jobs"
			}
			status, out := bedrockJobCall(t, url, method, path, nil)
			require.GreaterOrEqualf(t, status, http.StatusInternalServerError, "%s must fail on a store fault: %d %s", tc.name, status, out)
		})
	}
}
