package emulator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// SageMaker training jobs, Redshift Data statements, and QuickSight ingestions and data sources
// progress on the shared seeded-progression helper (#1155, #1162, #1163, #1168).
//
// Every assertion is on what a caller observes over the wire, on a frozen clock, because a
// progression is counted in observations and the clock must not move between them unless a test
// moves it.

// sjClock is the instant every test here freezes the simulated clock at.
var sjClock = time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)

// sjServer starts a recording test server frozen at sjClock.
func sjServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())
	ts.FreezeTimeAt(sjClock)
	return ts
}

// sjJSON issues one awsJson1_1 operation and returns its status and body.
func sjJSON(t *testing.T, ts *emulator.TestServer, host, target string, body any) (int, []byte) {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", bytes.NewReader(raw))
	require.NoError(t, err)
	req.Host = host
	req.Header.Set("X-Amz-Target", target)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	return sjDo(t, req)
}

// sjREST issues one QuickSight REST request and returns its status and body.
func sjREST(t *testing.T, ts *emulator.TestServer, method, path string, body any) (int, []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, reader)
	require.NoError(t, err)
	req.Host = "quicksight.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/json")
	return sjDo(t, req)
}

func sjDo(t *testing.T, req *http.Request) (int, []byte) {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, got
}

// sjSeed posts a seed to a control-plane endpoint and returns its status and body.
func sjSeed(t *testing.T, ts *emulator.TestServer, method, path, body string) (int, string) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, reader)
	require.NoError(t, err)
	status, got := sjDo(t, req)
	return status, string(got)
}

func sjMustSeed(t *testing.T, ts *emulator.TestServer, path, body string) {
	t.Helper()
	status, got := sjSeed(t, ts, http.MethodPost, path, body)
	require.Equal(t, http.StatusOK, status, "POST %s %s: %s", path, body, got)
}

// --- SageMaker ------------------------------------------------------------------------------

const sjSageMakerHost = "sagemaker.us-east-1.amazonaws.com"

func sjSageMaker(t *testing.T, ts *emulator.TestServer, op string, body any) (int, []byte) {
	t.Helper()
	return sjJSON(t, ts, sjSageMakerHost, "SageMaker."+op, body)
}

// sjTrainingJob returns what DescribeTrainingJob reports for name: status and failure reason.
func sjTrainingJob(t *testing.T, ts *emulator.TestServer, name string) (string, string) {
	t.Helper()
	status, body := sjSageMaker(t, ts, "DescribeTrainingJob", map[string]string{"TrainingJobName": name})
	require.Equal(t, http.StatusOK, status, "%s", body)
	var out struct {
		TrainingJobStatus string `json:"TrainingJobStatus"`
		FailureReason     string `json:"FailureReason"`
	}
	require.NoError(t, json.Unmarshal(body, &out), "%s", body)
	return out.TrainingJobStatus, out.FailureReason
}

// sjListedTrainingJob returns what ListTrainingJobs reports as name's status.
func sjListedTrainingJob(t *testing.T, ts *emulator.TestServer, name string) string {
	t.Helper()
	status, body := sjSageMaker(t, ts, "ListTrainingJobs", map[string]any{})
	require.Equal(t, http.StatusOK, status, "%s", body)
	var out struct {
		TrainingJobSummaries []struct {
			TrainingJobName   string `json:"TrainingJobName"`
			TrainingJobStatus string `json:"TrainingJobStatus"`
		} `json:"TrainingJobSummaries"`
	}
	require.NoError(t, json.Unmarshal(body, &out), "%s", body)
	for _, s := range out.TrainingJobSummaries {
		if s.TrainingJobName == name {
			return s.TrainingJobStatus
		}
	}
	t.Fatalf("ListTrainingJobs did not list %s: %s", name, body)
	return ""
}

// #1162's first criterion: a seeded Failed is reported the same by Describe and List.
func TestSageMakerProgression_DescribeAndListAgreeOnASeededFailure(t *testing.T) {
	t.Parallel()
	ts := sjServer(t)
	_, _ = sjSageMaker(t, ts, "CreateTrainingJob", map[string]string{"TrainingJobName": "agree"})
	sjMustSeed(t, ts, "/v1/sagemaker/training-job-status", `{"trainingJobName":"agree","status":"Failed","failureReason":"CapacityError: none"}`)

	status, reason := sjTrainingJob(t, ts, "agree")
	assert.Equal(t, "Failed", status)
	assert.Equal(t, "CapacityError: none", reason)
	assert.Equal(t, "Failed", sjListedTrainingJob(t, ts, "agree"), "ListTrainingJobs used to report the record's Completed")
}

// A counted run: InProgress for two observations, whether Describe or List makes them, then the
// seeded Failed; a stop restarts the countdown through Stopping to the recorded Stopped (#1155).
func TestSageMakerProgression_ACountedRunSettlesAndAStopRestartsIt(t *testing.T) {
	t.Parallel()
	ts := sjServer(t)
	_, _ = sjSageMaker(t, ts, "CreateTrainingJob", map[string]string{"TrainingJobName": "count"})
	sjMustSeed(t, ts, "/v1/sagemaker/training-job-status", `{"pendingObservations":2,"status":"Failed","failureReason":"AlgorithmError"}`)

	status, reason := sjTrainingJob(t, ts, "count")
	assert.Equal(t, "InProgress", status)
	assert.Empty(t, reason, "no failure reason while the job runs")
	assert.Equal(t, "InProgress", sjListedTrainingJob(t, ts, "count"), "a list observation spends the second")
	status, reason = sjTrainingJob(t, ts, "count")
	assert.Equal(t, "Failed", status)
	assert.Equal(t, "AlgorithmError", reason)

	code, body := sjSageMaker(t, ts, "StopTrainingJob", map[string]string{"TrainingJobName": "count"})
	require.Equal(t, http.StatusOK, code, "%s", body)
	status, reason = sjTrainingJob(t, ts, "count")
	assert.Equal(t, "Stopping", status, "a stop starts a new transition")
	assert.Empty(t, reason)
	_, _ = sjTrainingJob(t, ts, "count")
	status, reason = sjTrainingJob(t, ts, "count")
	assert.Equal(t, "Stopped", status, "a stopped job settles to its own Stopped, not the seed's Failed")
	assert.Empty(t, reason)
}

// #1162: DeleteApp refuses an app that does not exist with ResourceNotFound/400.
func TestSageMakerProgression_DeleteAppRefusesAnAbsentApp(t *testing.T) {
	t.Parallel()
	ts := sjServer(t)
	status, body := sjSageMaker(t, ts, "DeleteApp", map[string]string{
		"AppName": "nope", "AppType": "JupyterServer", "DomainId": "d-1", "UserProfileName": "u",
	})
	assert.Equal(t, http.StatusBadRequest, status, "%s", body)
	assert.Contains(t, string(body), "ResourceNotFound")
}

// --- Redshift Data ---------------------------------------------------------------------------

const sjRedshiftDataHost = "redshift-data.us-east-1.amazonaws.com"

func sjRedshiftData(t *testing.T, ts *emulator.TestServer, op string, body any) (int, []byte) {
	t.Helper()
	return sjJSON(t, ts, sjRedshiftDataHost, "RedshiftData_20191217."+op, body)
}

func sjExecute(t *testing.T, ts *emulator.TestServer, sql string) string {
	t.Helper()
	status, body := sjRedshiftData(t, ts, "ExecuteStatement", map[string]string{"Sql": sql, "Database": "db", "WorkgroupName": "wg"})
	require.Equal(t, http.StatusOK, status, "%s", body)
	var out struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(body, &out), "%s", body)
	return out.ID
}

type sjStatement struct {
	Status       string `json:"Status"`
	Error        string `json:"Error"`
	CreatedAt    int64  `json:"CreatedAt"`
	UpdatedAt    int64  `json:"UpdatedAt"`
	HasResultSet *bool  `json:"HasResultSet"`
}

func sjDescribeStatement(t *testing.T, ts *emulator.TestServer, id string) sjStatement {
	t.Helper()
	status, body := sjRedshiftData(t, ts, "DescribeStatement", map[string]string{"Id": id})
	require.Equal(t, http.StatusOK, status, "%s", body)
	var out sjStatement
	require.NoError(t, json.Unmarshal(body, &out), "%s", body)
	require.NotNil(t, out.HasResultSet, "DescribeStatement reports HasResultSet (#1163): %s", body)
	return out
}

// #1163: a seed written after ExecuteStatement governs that statement; STARTED for two describes,
// then FINISHED; GetStatementResult is refused until then; UpdatedAt moves once it settles.
func TestRedshiftDataProgression_ASeedAfterExecuteIsReadAtObservation(t *testing.T) {
	t.Parallel()
	ts := sjServer(t)
	id := sjExecute(t, ts, "SELECT 1")
	other := sjExecute(t, ts, "SELECT 2")
	sjMustSeed(t, ts, "/v1/redshift-data/status", `{"statementId":"`+id+`","pendingObservations":2}`)

	first := sjDescribeStatement(t, ts, id)
	assert.Equal(t, "STARTED", first.Status, "seeded after execute, and visible")
	assert.False(t, *first.HasResultSet)
	status, body := sjRedshiftData(t, ts, "GetStatementResult", map[string]string{"Id": id})
	assert.Equal(t, http.StatusBadRequest, status, "no result for a statement still STARTED: %s", body)
	assert.Contains(t, string(body), "ValidationException")

	assert.Equal(t, "FINISHED", sjDescribeStatement(t, ts, other).Status, "the seed is keyed to one statement")

	assert.Equal(t, "STARTED", sjDescribeStatement(t, ts, id).Status)
	ts.TimeController().SetTime(sjClock.Add(90 * time.Second))
	settled := sjDescribeStatement(t, ts, id)
	assert.Equal(t, "FINISHED", settled.Status)
	assert.True(t, *settled.HasResultSet)
	assert.Equal(t, sjClock.Unix(), settled.CreatedAt)
	assert.Equal(t, sjClock.Add(90*time.Second).Unix(), settled.UpdatedAt, "UpdatedAt is when it was first observed settled")
	assert.Equal(t, settled.UpdatedAt, sjDescribeStatement(t, ts, id).UpdatedAt, "and stays there")

	status, body = sjRedshiftData(t, ts, "GetStatementResult", map[string]string{"Id": id})
	assert.Equal(t, http.StatusOK, status, "%s", body)
}

// #1163: FAILED reports its error and refuses a result; DELETE clears the seed.
func TestRedshiftDataProgression_FailedRefusesAResultAndDeleteClears(t *testing.T) {
	t.Parallel()
	ts := sjServer(t)
	id := sjExecute(t, ts, "SELECT 1")
	sjMustSeed(t, ts, "/v1/redshift-data/status", `{"status":"FAILED","errorMessage":"syntax error"}`)
	failed := sjDescribeStatement(t, ts, id)
	assert.Equal(t, "FAILED", failed.Status)
	assert.Equal(t, "syntax error", failed.Error)
	status, _ := sjRedshiftData(t, ts, "GetStatementResult", map[string]string{"Id": id})
	assert.Equal(t, http.StatusBadRequest, status, "a FAILED statement used to answer the seeded rows")

	code, got := sjSeed(t, ts, http.MethodDelete, "/v1/redshift-data/status", "")
	require.Equal(t, http.StatusOK, code, got)
	assert.Equal(t, "FINISHED", sjDescribeStatement(t, ts, id).Status)
}

// #1163: responses carry awsJson1_1's content type.
func TestRedshiftDataProgression_ContentTypeIsAwsJson11(t *testing.T) {
	t.Parallel()
	ts := sjServer(t)
	raw, err := json.Marshal(map[string]string{"Sql": "SELECT 1", "Database": "db"})
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", bytes.NewReader(raw))
	require.NoError(t, err)
	req.Host = sjRedshiftDataHost
	req.Header.Set("X-Amz-Target", "RedshiftData_20191217.ExecuteStatement")
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	assert.Equal(t, "application/x-amz-json-1.1", resp.Header.Get("Content-Type"))
}

// --- QuickSight ------------------------------------------------------------------------------

const sjQuickSightAccount = "123456789012"

func sjCreateDataSet(t *testing.T, ts *emulator.TestServer, id string) string {
	t.Helper()
	status, body := sjREST(t, ts, http.MethodPost, "/accounts/"+sjQuickSightAccount+"/data-sets", map[string]string{"DataSetId": id, "Name": id})
	require.Equal(t, http.StatusCreated, status, "%s", body)
	var out struct {
		IngestionID string `json:"IngestionId"`
	}
	require.NoError(t, json.Unmarshal(body, &out), "%s", body)
	return out.IngestionID
}

func sjIngestion(t *testing.T, ts *emulator.TestServer, dataSet, ingestion string) (int, map[string]json.RawMessage) {
	t.Helper()
	status, body := sjREST(t, ts, http.MethodGet, "/accounts/"+sjQuickSightAccount+"/data-sets/"+dataSet+"/ingestions/"+ingestion, nil)
	if status != http.StatusOK {
		return status, nil
	}
	var out struct {
		Ingestion map[string]json.RawMessage `json:"Ingestion"`
	}
	require.NoError(t, json.Unmarshal(body, &out), "%s", body)
	return status, out.Ingestion
}

// #1168: an ingestion ID that is not the dataset's is ResourceNotFoundException/404.
func TestQuickSightProgression_AnUnknownIngestionIsNotFound(t *testing.T) {
	t.Parallel()
	ts := sjServer(t)
	real := sjCreateDataSet(t, ts, "ds-unknown")
	status, _ := sjIngestion(t, ts, "ds-unknown", "not-"+real)
	assert.Equal(t, http.StatusNotFound, status, "any ID used to be COMPLETED with 1000 rows")
	status, got := sjIngestion(t, ts, "ds-unknown", real)
	assert.Equal(t, http.StatusOK, status)
	assert.JSONEq(t, `"COMPLETED"`, string(got["IngestionStatus"]))
	assert.NotContains(t, got, "RowInfo", "an unseeded ingestion has no row count to report")
	assert.JSONEq(t, strconv.FormatInt(sjClock.Unix(), 10), string(got["CreatedTime"]), "CreatedTime is when the ingestion started")
}

// #1168: a counted ingestion is QUEUED, then settles FAILED with ErrorInfo, or COMPLETED with the
// seeded RowInfo.
func TestQuickSightProgression_ASeededIngestionProgresses(t *testing.T) {
	t.Parallel()
	ts := sjServer(t)
	failing := sjCreateDataSet(t, ts, "ds-fail")
	ok := sjCreateDataSet(t, ts, "ds-ok")
	sjMustSeed(t, ts, "/v1/quicksight/ingestion-status", `{"ingestionId":"`+failing+`","pendingObservations":1,"transientStatus":"QUEUED","status":"FAILED","errorType":"S3_MANIFEST_ERROR","errorMessage":"bad manifest"}`)
	sjMustSeed(t, ts, "/v1/quicksight/ingestion-status", `{"ingestionId":"`+ok+`","pendingObservations":1,"rowsIngested":42,"rowsDropped":1}`)

	_, got := sjIngestion(t, ts, "ds-fail", failing)
	assert.JSONEq(t, `"QUEUED"`, string(got["IngestionStatus"]))
	assert.NotContains(t, got, "ErrorInfo")
	_, got = sjIngestion(t, ts, "ds-fail", failing)
	assert.JSONEq(t, `"FAILED"`, string(got["IngestionStatus"]))
	assert.JSONEq(t, `{"Type":"S3_MANIFEST_ERROR","Message":"bad manifest"}`, string(got["ErrorInfo"]))

	_, got = sjIngestion(t, ts, "ds-ok", ok)
	assert.JSONEq(t, `"RUNNING"`, string(got["IngestionStatus"]))
	assert.NotContains(t, got, "RowInfo", "no row count while the ingestion runs")
	_, got = sjIngestion(t, ts, "ds-ok", ok)
	assert.JSONEq(t, `"COMPLETED"`, string(got["IngestionStatus"]))
	assert.JSONEq(t, `{"RowsIngested":42,"RowsDropped":1}`, string(got["RowInfo"]))
}

// #1155: a data source can be held CREATION_IN_PROGRESS — by the create's own answer, which peeks,
// and the first describe — then settle CREATION_FAILED with ErrorInfo.
func TestQuickSightProgression_ADataSourceCreationProgresses(t *testing.T) {
	t.Parallel()
	ts := sjServer(t)
	sjMustSeed(t, ts, "/v1/quicksight/data-source-status", `{"dataSourceId":"src","pendingObservations":1,"status":"CREATION_FAILED","errorType":"ACCESS_DENIED","errorMessage":"no"}`)
	status, body := sjREST(t, ts, http.MethodPost, "/accounts/"+sjQuickSightAccount+"/data-sources", map[string]string{"DataSourceId": "src", "Name": "src", "Type": "S3"})
	require.Equal(t, http.StatusCreated, status, "%s", body)
	assert.Contains(t, string(body), `"CreationStatus":"CREATION_IN_PROGRESS"`)

	describe := func() map[string]json.RawMessage {
		status, body := sjREST(t, ts, http.MethodGet, "/accounts/"+sjQuickSightAccount+"/data-sources/src", nil)
		require.Equal(t, http.StatusOK, status, "%s", body)
		var out struct {
			DataSource map[string]json.RawMessage `json:"DataSource"`
		}
		require.NoError(t, json.Unmarshal(body, &out), "%s", body)
		return out.DataSource
	}
	got := describe()
	assert.JSONEq(t, `"CREATION_IN_PROGRESS"`, string(got["Status"]), "the create did not spend the first describe's observation")
	got = describe()
	assert.JSONEq(t, `"CREATION_FAILED"`, string(got["Status"]))
	assert.JSONEq(t, `{"Type":"ACCESS_DENIED","Message":"no"}`, string(got["ErrorInfo"]))
}

// --- Seed refusals ---------------------------------------------------------------------------

// Every seed is validated against the published enumerations (#1162, #1163, #1168).
func TestSeededJobsProgression_SeedsOutsideThePublishedEnumAreRefused(t *testing.T) {
	t.Parallel()
	ts := sjServer(t)
	for _, tc := range []struct {
		path, body string
	}{
		{"/v1/sagemaker/training-job-status", `{"status":"compelted"}`},
		{"/v1/sagemaker/training-job-status", `{"status":"Completed","transientStatus":"Completed","pendingObservations":1}`},
		{"/v1/sagemaker/training-job-status", `{"status":"Completed","failureReason":"x"}`},
		{"/v1/sagemaker/training-job-status", `{"trainingJobName":"x"}`},
		{"/v1/sagemaker/training-job-status", `{"status":"Failed","pendingObservations":-1}`},
		{"/v1/redshift-data/status", `{"status":"DONE"}`},
		{"/v1/redshift-data/status", `{"status":"FINISHED","transientStatus":"FINISHED","pendingObservations":1}`},
		{"/v1/redshift-data/status", `{"status":"FINISHED","errorMessage":"x"}`},
		{"/v1/quicksight/ingestion-status", `{"status":"DONE"}`},
		{"/v1/quicksight/ingestion-status", `{"status":"FAILED","errorType":"NOPE"}`},
		{"/v1/quicksight/ingestion-status", `{"status":"COMPLETED","errorType":"S3_MANIFEST_ERROR"}`},
		{"/v1/quicksight/ingestion-status", `{"rowsIngested":-1}`},
		{"/v1/quicksight/data-source-status", `{"status":"UPDATE_SUCCESSFUL"}`},
		{"/v1/quicksight/data-source-status", `{"status":"CREATION_SUCCESSFUL","errorType":"TIMEOUT"}`},
	} {
		status, got := sjSeed(t, ts, http.MethodPost, tc.path, tc.body)
		assert.Equalf(t, http.StatusBadRequest, status, "POST %s %s: %s", tc.path, tc.body, got)
	}
	// SUBMITTED and PICKED are published, and the pre-#1163 handler refused them.
	sjMustSeed(t, ts, "/v1/redshift-data/status", `{"status":"FINISHED","transientStatus":"PICKED","pendingObservations":1}`)
	sjMustSeed(t, ts, "/v1/redshift-data/status", `{"status":"SUBMITTED"}`)
}

// --- Replay ----------------------------------------------------------------------------------

// A recording of all three seeded countdowns replays with the same observations (#1155, #1163).
func TestSeededJobsProgression_AReplayReproducesEveryCountdown(t *testing.T) {
	t.Parallel()
	ts := sjServer(t)

	_, _ = sjSageMaker(t, ts, "CreateTrainingJob", map[string]string{"TrainingJobName": "replay"})
	sjMustSeed(t, ts, "/v1/sagemaker/training-job-status", `{"trainingJobName":"replay","pendingObservations":2,"status":"Failed"}`)
	stmt := sjExecute(t, ts, "SELECT 1")
	sjMustSeed(t, ts, "/v1/redshift-data/status", `{"statementId":"`+stmt+`","pendingObservations":2}`)
	ing := sjCreateDataSet(t, ts, "ds-replay")
	sjMustSeed(t, ts, "/v1/quicksight/ingestion-status", `{"pendingObservations":2,"status":"CANCELLED"}`)

	var sm, rd []string
	var qs []string
	for range 3 {
		s, _ := sjTrainingJob(t, ts, "replay")
		sm = append(sm, s)
		rd = append(rd, sjDescribeStatement(t, ts, stmt).Status)
		_, got := sjIngestion(t, ts, "ds-replay", ing)
		qs = append(qs, string(got["IngestionStatus"]))
	}
	require.Equal(t, []string{"InProgress", "InProgress", "Failed"}, sm)
	require.Equal(t, []string{"STARTED", "STARTED", "FINISHED"}, rd)
	require.Equal(t, []string{`"RUNNING"`, `"RUNNING"`, `"CANCELLED"`}, qs)

	results, err := pipelineReplayEngine(ts, emulator.ReplayPipeline{}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	assert.Zero(t, results.FailedEvents)
	assert.Empty(t, results.Differences, "every replayed observation matches its recording: %s", replayDifferenceSummary(results))
}

// --- Store faults ----------------------------------------------------------------------------

// A store fault reading a seed or a counter is an error, never an unseeded answer.
func TestSeededJobsProgression_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
		run  func(t *testing.T, srv http.Handler) (int, string)
	}{
		{"SageMaker describe, seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, sjFaultSageMakerDescribe},
		{"SageMaker list, counter read", func(m *cfFaultStateManager) { m.failGet = "observed:" }, sjFaultSageMakerList},
		{"SageMaker stop, counter reset", func(m *cfFaultStateManager) { m.failDelete = "observed:" }, sjFaultSageMakerStop},
		{"Redshift Data describe, counter write", func(m *cfFaultStateManager) { m.failPut = "observed:" }, sjFaultRedshiftDescribe},
		{"Redshift Data describe, settled write", func(m *cfFaultStateManager) { m.failPut = "settled:" }, sjFaultRedshiftSettle},
		{"Redshift Data describe, corrupt settled", func(m *cfFaultStateManager) { m.corruptGet = "settled:" }, sjFaultRedshiftSettle},
		{"Redshift Data result, seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, sjFaultRedshiftResult},
		{"QuickSight ingestion, seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, sjFaultQuickSightIngestion},
		{"QuickSight ingestion, dataset read", func(m *cfFaultStateManager) { m.failGet = "dataset:" }, sjFaultQuickSightIngestion},
		{"QuickSight data source, seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, sjFaultQuickSightDataSource},
		{"QuickSight create data source, seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, sjFaultQuickSightCreateDataSource},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			srv := sjFaultServer(t, fault)
			sjFaultArrange(t, srv)
			tc.arm(fault)
			status, body := tc.run(t, srv)
			assert.GreaterOrEqualf(t, status, http.StatusInternalServerError, "%s must fail on a store fault: %s", tc.name, body)
		})
	}
}

// sjFaultServer is a server over fault with the three plugins registered and the clock frozen.
func sjFaultServer(t *testing.T, fault *cfFaultStateManager) http.Handler {
	t.Helper()
	tc := emulator.NewTimeController(sjClock)
	tc.Freeze()
	tc.SetTime(sjClock)
	logger := emulator.NewDefaultLogger(slog.LevelError, false)
	registry := emulator.NewPluginRegistry()
	for _, p := range []emulator.Plugin{&emulator.SageMakerPlugin{}, &emulator.RedshiftDataPlugin{}, &emulator.QuickSightPlugin{}} {
		require.NoError(t, p.Initialize(context.Background(), emulator.PluginConfig{State: fault, Logger: logger, Options: map[string]any{"time_controller": tc}}))
		registry.Register(p)
	}
	cfg := emulator.DefaultConfig()
	return emulator.NewServer(*cfg, registry, emulator.NewEventStore(cfg.EventStore.ToEventStoreConfig()), fault, tc, logger)
}

func sjFaultCall(t *testing.T, srv http.Handler, method, path, host, target, body string) (int, string) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Host = host
	if target != "" {
		req.Header.Set("X-Amz-Target", target)
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	} else {
		req.Header.Set("Content-Type", "application/json")
	}
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	return rr.Code, rr.Body.String()
}

// sjFaultArrange creates one resource of each kind and arms a counted seed on each, before any
// fault is armed.
func sjFaultArrange(t *testing.T, srv http.Handler) {
	t.Helper()
	ok := func(code int, body string) {
		t.Helper()
		require.Less(t, code, 300, "%s", body)
	}
	ok(sjFaultCall(t, srv, http.MethodPost, "/", sjSageMakerHost, "SageMaker.CreateTrainingJob", `{"TrainingJobName":"f"}`))
	ok(sjFaultCall(t, srv, http.MethodPost, "/v1/sagemaker/training-job-status", "", "", `{"pendingObservations":1,"status":"Failed"}`))
	ok(sjFaultCall(t, srv, http.MethodPost, "/v1/redshift-data/status", "", "", `{"pendingObservations":1}`))
	ok(sjFaultCall(t, srv, http.MethodPost, "/v1/quicksight/ingestion-status", "", "", `{"pendingObservations":1}`))
	ok(sjFaultCall(t, srv, http.MethodPost, "/v1/quicksight/data-source-status", "", "", `{"pendingObservations":1}`))
	ok(sjFaultCall(t, srv, http.MethodPost, "/accounts/"+sjQuickSightAccount+"/data-sources", "quicksight.us-east-1.amazonaws.com", "", `{"DataSourceId":"src","Name":"s","Type":"S3"}`))
}

func sjFaultSageMakerDescribe(t *testing.T, srv http.Handler) (int, string) {
	return sjFaultCall(t, srv, http.MethodPost, "/", sjSageMakerHost, "SageMaker.DescribeTrainingJob", `{"TrainingJobName":"f"}`)
}

func sjFaultSageMakerList(t *testing.T, srv http.Handler) (int, string) {
	return sjFaultCall(t, srv, http.MethodPost, "/", sjSageMakerHost, "SageMaker.ListTrainingJobs", `{}`)
}

func sjFaultSageMakerStop(t *testing.T, srv http.Handler) (int, string) {
	return sjFaultCall(t, srv, http.MethodPost, "/", sjSageMakerHost, "SageMaker.StopTrainingJob", `{"TrainingJobName":"f"}`)
}

// sjFaultExecute runs a statement with no fault armed on the record path and returns its ID.
func sjFaultExecute(t *testing.T, srv http.Handler) string {
	t.Helper()
	code, body := sjFaultCall(t, srv, http.MethodPost, "/", sjRedshiftDataHost, "RedshiftData_20191217.ExecuteStatement", `{"Sql":"SELECT 1","Database":"db"}`)
	require.Equal(t, http.StatusOK, code, "%s", body)
	var out struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &out))
	return out.ID
}

func sjFaultRedshiftDescribe(t *testing.T, srv http.Handler) (int, string) {
	id := sjFaultExecute(t, srv)
	return sjFaultCall(t, srv, http.MethodPost, "/", sjRedshiftDataHost, "RedshiftData_20191217.DescribeStatement", `{"Id":"`+id+`"}`)
}

// sjFaultRedshiftSettle reaches the settled-instant read or write: the first describe spends the
// countdown's one observation (the counter write is not faulted here), the second settles.
func sjFaultRedshiftSettle(t *testing.T, srv http.Handler) (int, string) {
	id := sjFaultExecute(t, srv)
	_, _ = sjFaultCall(t, srv, http.MethodPost, "/", sjRedshiftDataHost, "RedshiftData_20191217.DescribeStatement", `{"Id":"`+id+`"}`)
	return sjFaultCall(t, srv, http.MethodPost, "/", sjRedshiftDataHost, "RedshiftData_20191217.DescribeStatement", `{"Id":"`+id+`"}`)
}

func sjFaultRedshiftResult(t *testing.T, srv http.Handler) (int, string) {
	id := sjFaultExecute(t, srv)
	return sjFaultCall(t, srv, http.MethodPost, "/", sjRedshiftDataHost, "RedshiftData_20191217.GetStatementResult", `{"Id":"`+id+`"}`)
}

// sjFaultQuickSightIngestion creates a dataset, a write the armed faults leave alone, and describes
// its ingestion.
func sjFaultQuickSightIngestion(t *testing.T, srv http.Handler) (int, string) {
	t.Helper()
	code, body := sjFaultCall(t, srv, http.MethodPost, "/accounts/"+sjQuickSightAccount+"/data-sets", "quicksight.us-east-1.amazonaws.com", "", `{"DataSetId":"ds","Name":"ds"}`)
	require.Equal(t, http.StatusCreated, code, "%s", body)
	var out struct {
		IngestionID string `json:"IngestionId"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &out))
	return sjFaultCall(t, srv, http.MethodGet, "/accounts/"+sjQuickSightAccount+"/data-sets/ds/ingestions/"+out.IngestionID, "quicksight.us-east-1.amazonaws.com", "", "")
}

func sjFaultQuickSightDataSource(t *testing.T, srv http.Handler) (int, string) {
	return sjFaultCall(t, srv, http.MethodGet, "/accounts/"+sjQuickSightAccount+"/data-sources/src", "quicksight.us-east-1.amazonaws.com", "", "")
}

func sjFaultQuickSightCreateDataSource(t *testing.T, srv http.Handler) (int, string) {
	return sjFaultCall(t, srv, http.MethodPost, "/accounts/"+sjQuickSightAccount+"/data-sources", "quicksight.us-east-1.amazonaws.com", "", `{"DataSourceId":"src2","Name":"s","Type":"S3"}`)
}
