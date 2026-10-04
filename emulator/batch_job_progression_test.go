package emulator_test

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A Batch job progresses through its published statuses instead of being born SUCCEEDED (#1248).
//
// Every assertion here is on the raw body a DescribeJobs or ListJobs answers, because the status an
// observation reports is the thing that changed; the record behind it is checked only to show it is
// never rewritten by a seed.

// batchProgressionHost is the Host header every request below carries.
const batchProgressionHost = "batch.us-east-1.amazonaws.com"

// batchProgressionServer starts a test server on a frozen clock, recording bodies so it can be
// replayed.
func batchProgressionServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())
	ts.FreezeTimeAt(time.Unix(1700000000, 0).UTC())
	return ts
}

// batchCall issues one Batch request and returns its status and body.
func batchCall(t *testing.T, ts *emulator.TestServer, path string, body any) (int, []byte) {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	status, _, out := resetDoRequest(t, http.MethodPost, ts.URL+path, batchProgressionHost, raw)
	return status, out
}

// batchSubmit submits a job and returns its ID.
func batchSubmit(t *testing.T, ts *emulator.TestServer, name string) string {
	t.Helper()
	status, body := batchCall(t, ts, "/v1/submitjob", map[string]any{
		"jobName": name, "jobQueue": "progress-queue", "jobDefinition": "progress-def",
	})
	require.Equal(t, http.StatusOK, status, "SubmitJob: %s", body)
	var out struct {
		JobID string `json:"jobId"`
	}
	require.NoError(t, json.Unmarshal(body, &out))
	require.NotEmpty(t, out.JobID)
	return out.JobID
}

// batchDescribeStatus answers the status and statusReason one DescribeJobs reports for id, which is
// one observation.
func batchDescribeStatus(t *testing.T, ts *emulator.TestServer, id string) (status, reason string) {
	t.Helper()
	code, body := batchCall(t, ts, "/v1/describejobs", map[string]any{"jobs": []string{id}})
	require.Equal(t, http.StatusOK, code, "DescribeJobs: %s", body)
	var out struct {
		Jobs []map[string]json.RawMessage `json:"jobs"`
	}
	require.NoError(t, json.Unmarshal(body, &out), "%s", body)
	require.Len(t, out.Jobs, 1, "%s", body)
	require.NoError(t, json.Unmarshal(out.Jobs[0]["status"], &status), "%s", body)
	if raw, ok := out.Jobs[0]["statusReason"]; ok {
		require.NoError(t, json.Unmarshal(raw, &reason))
	}
	return status, reason
}

// batchListStatuses answers the jobId → status pairs one ListJobs reports for body.
func batchListStatuses(t *testing.T, ts *emulator.TestServer, body map[string]any) map[string]string {
	t.Helper()
	code, raw := batchCall(t, ts, "/v1/listjobs", body)
	require.Equal(t, http.StatusOK, code, "ListJobs: %s", raw)
	var out struct {
		JobSummaryList []struct {
			JobID  string `json:"jobId"`
			Status string `json:"status"`
		} `json:"jobSummaryList"`
	}
	require.NoError(t, json.Unmarshal(raw, &out), "%s", raw)
	got := map[string]string{}
	for _, s := range out.JobSummaryList {
		got[s.JobID] = s.Status
	}
	return got
}

// batchSeedJobStatus seeds a job progression through the control plane and requires a 200.
func batchSeedJobStatus(t *testing.T, ts *emulator.TestServer, body string) {
	t.Helper()
	status, got := cpControlPlaneRequest(t, ts, http.MethodPost, "/v1/batch/job-status", []byte(body))
	require.Equal(t, http.StatusOK, status, "seed = %s", got)
}

// batchRecordStatus reads the stored record's status, bypassing every observation.
func batchRecordStatus(t *testing.T, ts *emulator.TestServer, id string) string {
	t.Helper()
	data, err := ts.StateManager().Get(t.Context(), "batch", "job:123456789012/us-east-1/"+id)
	require.NoError(t, err)
	require.NotNil(t, data, "no record for %s", id)
	var job struct {
		Status string `json:"status"`
	}
	require.NoError(t, json.Unmarshal(data, &job))
	return job.Status
}

func TestBatchJobProgression_AnUnseededJobIsSubmittedThenSucceedsOnItsFirstObservation(t *testing.T) {
	t.Parallel()
	ts := batchProgressionServer(t)
	id := batchSubmit(t, ts, "unseeded")

	require.Equal(t, "SUBMITTED", batchRecordStatus(t, ts, id),
		"the Job states page: a submitted job enters the SUBMITTED state")
	for range 2 {
		status, reason := batchDescribeStatus(t, ts, id)
		assert.Equal(t, "SUCCEEDED", status, "an unsized progression settles on the first observation")
		assert.Empty(t, reason)
	}
	assert.Empty(t, batchListStatuses(t, ts, map[string]any{"jobQueue": "progress-queue"}),
		"so ListJobs' RUNNING default still answers nothing for an unseeded job")
	assert.Equal(t, map[string]string{id: "SUCCEEDED"},
		batchListStatuses(t, ts, map[string]any{"jobQueue": "progress-queue", "jobStatus": "SUCCEEDED"}))
}

func TestBatchJobProgression_ASeededJobWalksThePublishedStatusesThenFails(t *testing.T) {
	t.Parallel()
	ts := batchProgressionServer(t)
	batchSeedJobStatus(t, ts, `{"jobId":"*","transientObservations":{"SUBMITTED":1,"PENDING":1,`+
		`"RUNNABLE":1,"STARTING":1,"RUNNING":2},"finalState":"FAILED","statusReason":"Essential container exited"}`)
	id := batchSubmit(t, ts, "seeded")

	type observation struct{ status, reason string }
	want := []observation{
		{"SUBMITTED", ""}, {"PENDING", ""}, {"RUNNABLE", ""}, {"STARTING", ""},
		{"RUNNING", ""}, {"RUNNING", ""},
		{"FAILED", "Essential container exited"}, {"FAILED", "Essential container exited"},
	}
	for i, w := range want {
		status, reason := batchDescribeStatus(t, ts, id)
		assert.Equalf(t, w, observation{status, reason}, "observation %d", i)
	}
	assert.Equal(t, "SUBMITTED", batchRecordStatus(t, ts, id),
		"a seed governs what an observation reports; it never rewrites the record")
}

func TestBatchJobProgression_ListJobsRunningDefaultFindsARunningJob(t *testing.T) {
	t.Parallel()
	ts := batchProgressionServer(t)
	batchSeedJobStatus(t, ts, `{"transientObservations":{"RUNNING":100}}`)
	id := batchSubmit(t, ts, "running")

	assert.Equal(t, map[string]string{id: "RUNNING"},
		batchListStatuses(t, ts, map[string]any{"jobQueue": "progress-queue"}),
		"the assertion #1236 could not make: the default listing of a running job is non-empty")
}

// DescribeJobs and ListJobs report a job's status through one projection, so at the same point in
// a progression they agree. Two jobs share one wildcard seed; one is observed only by DescribeJobs
// and the other only by ListJobs, and the two sequences must be identical.
func TestBatchJobProgression_DescribeAndListReportTheSameStatusAtTheSamePoint(t *testing.T) {
	t.Parallel()
	ts := batchProgressionServer(t)
	batchSeedJobStatus(t, ts, `{"transientObservations":{"RUNNABLE":1,"RUNNING":2}}`)
	byDescribe := batchSubmit(t, ts, "by-describe")
	byList := batchSubmit(t, ts, "by-list")

	listOne := map[string]any{"filters": []map[string]any{{"name": "JOB_NAME", "values": []string{"by-list"}}}}
	for i := range 5 {
		described, _ := batchDescribeStatus(t, ts, byDescribe)
		listed := batchListStatuses(t, ts, listOne)
		assert.Equalf(t, map[string]string{byList: described}, listed, "observation %d", i)
	}
}

func TestBatchJobProgression_TerminateAndCancelActOnTheObservedState(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		seed       string // "" leaves the job unseeded
		path       string
		wantStatus string
	}{
		{"unseeded job terminated", "", "/v1/terminatejob", "FAILED"},
		{"unseeded job cancelled", "", "/v1/canceljob", "FAILED"},
		{"running job terminated", `{"transientObservations":{"RUNNING":9}}`, "/v1/terminatejob", "FAILED"},
		{"runnable job cancelled", `{"transientObservations":{"RUNNABLE":9}}`, "/v1/canceljob", "FAILED"},
		// API_CancelJob: jobs that progressed to STARTING or RUNNING "aren't cancelled. However,
		// the API operation still succeeds".
		{"running job not cancelled", `{"transientObservations":{"RUNNING":9}}`, "/v1/canceljob", "RUNNING"},
		{"starting job not cancelled", `{"transientObservations":{"STARTING":9}}`, "/v1/canceljob", "STARTING"},
		// A job that has settled is left as it is.
		{"settled job terminated", `{"transientObservations":{}}`, "/v1/terminatejob", "SUCCEEDED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := batchProgressionServer(t)
			if tc.seed != "" {
				batchSeedJobStatus(t, ts, tc.seed)
			}
			id := batchSubmit(t, ts, "ended")
			status, body := batchCall(t, ts, tc.path, map[string]any{"jobId": id, "reason": "by the test"})
			require.Equal(t, http.StatusOK, status, "%s: %s", tc.path, body)

			got, reason := batchDescribeStatus(t, ts, id)
			assert.Equal(t, tc.wantStatus, got)
			if tc.wantStatus == "FAILED" {
				assert.Equal(t, "by the test", reason, "the reason is the one future DescribeJobs return")
			}
		})
	}
}

func TestBatchJobProgression_CancelOfAMissingJobIsAClientException(t *testing.T) {
	t.Parallel()
	ts := batchProgressionServer(t)
	status, body := batchCall(t, ts, "/v1/canceljob", map[string]any{"jobId": "no-such-job", "reason": "r"})
	assert.Equal(t, http.StatusBadRequest, status, "%s", body)
	assert.Contains(t, string(body), "ClientException")
}

func TestBatchJobProgression_TheSeedEndpointRefusesWhatThePagesDoNotPublish(t *testing.T) {
	t.Parallel()
	ts := batchProgressionServer(t)
	for _, body := range []string{
		`{"transientObservations":{"QUEUED":1}}`,
		`{"transientObservations":{"SUCCEEDED":1}}`,
		`{"transientObservations":{"RUNNING":-1}}`,
		`{"transientObservations":{"":1}}`,
		`{"finalState":"RUNNING"}`,
		`not json`,
	} {
		status, got := cpControlPlaneRequest(t, ts, http.MethodPost, "/v1/batch/job-status", []byte(body))
		assert.Equalf(t, http.StatusBadRequest, status, "seed %s answered %s", body, got)
	}
}

func TestBatchJobProgression_ClearingTheSeedRestoresTheUnseededAnswer(t *testing.T) {
	t.Parallel()
	ts := batchProgressionServer(t)
	batchSeedJobStatus(t, ts, `{"transientObservations":{"RUNNING":9}}`)
	id := batchSubmit(t, ts, "cleared")
	status, _ := batchDescribeStatus(t, ts, id)
	require.Equal(t, "RUNNING", status)

	code, got := cpControlPlaneRequest(t, ts, http.MethodDelete, "/v1/batch/job-status", nil)
	require.Equal(t, http.StatusOK, code, "%s", got)
	status, _ = batchDescribeStatus(t, ts, id)
	assert.Equal(t, "SUCCEEDED", status)
}

// A recorded seeded run replays to the same observations: the seed is re-issued in position and
// the countdown restarts from zero at the same point in the stream.
func TestBatchJobProgression_AReplayReproducesTheSameObservations(t *testing.T) {
	t.Parallel()
	ts := batchProgressionServer(t)
	batchSeedJobStatus(t, ts, `{"transientObservations":{"RUNNABLE":1,"RUNNING":1},"finalState":"FAILED","statusReason":"replayed"}`)
	id := batchSubmit(t, ts, "replayed")
	var recorded []string
	for range 4 {
		status, _ := batchDescribeStatus(t, ts, id)
		recorded = append(recorded, status)
	}
	require.Equal(t, []string{"RUNNABLE", "RUNNING", "FAILED", "FAILED"}, recorded,
		"the recording has to progress, or the replay has nothing to reproduce")

	results, err := pipelineReplayEngine(ts, emulator.ReplayPipeline{}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	assert.Zero(t, results.SkippedEvents, "the seed is re-executed, not skipped")
	assert.Zero(t, results.FailedEvents)
	assert.Empty(t, results.Differences,
		"every replayed DescribeJobs reports the recorded status: %s", replayDifferenceSummary(results))
}

// A store fault in the progression's seed or counter read or write is an error, never answered as
// a status. The plugin is driven directly over a faulting store; the seed is written into the
// store's inner copy under the key the progression reads.
func TestBatchJobProgression_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
		path string
		body func(id string) map[string]any
	}{
		{"DescribeJobs, seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, "/v1/describejobs",
			func(id string) map[string]any { return map[string]any{"jobs": []string{id}} }},
		{"DescribeJobs, corrupt seed", func(m *cfFaultStateManager) { m.corruptGet = "status:" }, "/v1/describejobs",
			func(id string) map[string]any { return map[string]any{"jobs": []string{id}} }},
		{"DescribeJobs, counter read", func(m *cfFaultStateManager) { m.failGet = "observed:" }, "/v1/describejobs",
			func(id string) map[string]any { return map[string]any{"jobs": []string{id}} }},
		{"DescribeJobs, counter write", func(m *cfFaultStateManager) { m.failPut = "observed:" }, "/v1/describejobs",
			func(id string) map[string]any { return map[string]any{"jobs": []string{id}} }},
		{"ListJobs, seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, "/v1/listjobs",
			func(string) map[string]any { return map[string]any{"jobQueue": "q"} }},
		{"TerminateJob, seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, "/v1/terminatejob",
			func(id string) map[string]any { return map[string]any{"jobId": id, "reason": "r"} }},
		{"TerminateJob, record write", func(m *cfFaultStateManager) { m.failPut = "job:" }, "/v1/terminatejob",
			func(id string) map[string]any { return map[string]any{"jobId": id, "reason": "r"} }},
		{"CancelJob, record read", func(m *cfFaultStateManager) { m.failGet = "job:" }, "/v1/canceljob",
			func(id string) map[string]any { return map[string]any{"jobId": id, "reason": "r"} }},
		{"CancelJob, seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, "/v1/canceljob",
			func(id string) map[string]any { return map[string]any{"jobId": id, "reason": "r"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inner := emulator.NewMemoryStateManager()
			fault := &cfFaultStateManager{inner: inner}
			p := &emulator.BatchPlugin{}
			require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
				State:   fault,
				Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
				Options: map[string]any{"time_controller": emulator.NewTimeController(time.Unix(1700000000, 0).UTC())},
			}))
			ctx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1",
				RequestID: "req-batch-fault", IDs: emulator.NewIDMint("req-batch-fault")}
			call := func(path string, body map[string]any) (*emulator.AWSResponse, error) {
				raw, err := json.Marshal(body)
				require.NoError(t, err)
				return p.HandleRequest(ctx, &emulator.AWSRequest{
					Service: "batch", HTTPMethod: http.MethodPost, Path: path, Body: raw,
					Headers: map[string]string{"Content-Type": "application/json"}, Params: map[string]string{},
				})
			}
			resp, err := call("/v1/submitjob", map[string]any{"jobName": "f", "jobQueue": "q", "jobDefinition": "d"})
			require.NoError(t, err)
			var sub struct {
				JobID string `json:"jobId"`
			}
			require.NoError(t, json.Unmarshal(resp.Body, &sub))
			require.NoError(t, inner.Put(t.Context(), "batch-job-ctrl", "status:*",
				[]byte(`{"jobId":"*","transientObservations":{"RUNNING":3}}`)))

			tc.arm(fault)
			_, err = call(tc.path, tc.body(sub.JobID))
			require.Error(t, err, "%s must fail on a store fault", tc.name)
			var awsErr *emulator.AWSError
			assert.Falsef(t, errors.As(err, &awsErr), "%s answered a store fault as the published %v", tc.name, awsErr)
		})
	}
}
