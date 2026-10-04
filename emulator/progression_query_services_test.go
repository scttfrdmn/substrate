package emulator_test

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Athena query executions, CodeBuild builds and CodePipeline executions progress through their
// published transient states under a seed (#1155), and Athena reports a cancelled query as the
// enumeration spells it (#1154).
//
// Every assertion is on the raw response bytes a consumer's SDK decodes, on a frozen clock, and the
// countdown is counted in observations — so nothing here depends on how long the test takes.

// pqsClock is the instant every server below is frozen at.
var pqsClock = time.Unix(1700000000, 0).UTC()

// pqsServer starts a test server with recorded bodies on a frozen clock.
func pqsServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())
	ts.FreezeTimeAt(pqsClock)
	return ts
}

// pqsCall issues one JSON-target AWS request and returns its status and body.
func pqsCall(t *testing.T, ts *emulator.TestServer, host, target string, body any) (int, []byte) {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", strings.NewReader(string(raw)))
	require.NoError(t, err)
	req.Host = host
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", target)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return resp.StatusCode, out
}

// pqsOK issues a request that must succeed and returns its body.
func pqsOK(t *testing.T, ts *emulator.TestServer, host, target string, body any) []byte {
	t.Helper()
	status, out := pqsCall(t, ts, host, target, body)
	require.Equal(t, http.StatusOK, status, "%s: %s", target, out)
	return out
}

// pqsSeed POSTs a seed and requires it accepted.
func pqsSeed(t *testing.T, ts *emulator.TestServer, path, body string) {
	t.Helper()
	status, out := cpControlPlaneRequest(t, ts, http.MethodPost, path, []byte(body))
	require.Equal(t, http.StatusOK, status, "seed %s: %s", path, out)
}

// pqsMember decodes one member path out of a JSON body, for assertions on exact values.
func pqsMember(t *testing.T, body []byte, path ...string) json.RawMessage {
	t.Helper()
	var node json.RawMessage = body
	for _, key := range path {
		var m map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(node, &m), "decode at %q: %s", key, node)
		node = m[key]
	}
	return node
}

// --- Athena ---------------------------------------------------------------------------------

const pqsAthenaHost = "athena.us-east-1.amazonaws.com"

func pqsAthena(t *testing.T, ts *emulator.TestServer, op string, body any) []byte {
	t.Helper()
	return pqsOK(t, ts, pqsAthenaHost, "AmazonAthena."+op, body)
}

func pqsAthenaStart(t *testing.T, ts *emulator.TestServer) string {
	t.Helper()
	var out struct {
		QueryExecutionID string `json:"QueryExecutionId"`
	}
	require.NoError(t, json.Unmarshal(pqsAthena(t, ts, "StartQueryExecution", map[string]any{"QueryString": "SELECT 1"}), &out))
	return out.QueryExecutionID
}

func pqsAthenaStatus(t *testing.T, ts *emulator.TestServer, id string) json.RawMessage {
	t.Helper()
	return pqsMember(t, pqsAthena(t, ts, "GetQueryExecution", map[string]any{"QueryExecutionId": id}), "QueryExecution", "Status")
}

func pqsAthenaState(t *testing.T, ts *emulator.TestServer, id string) string {
	t.Helper()
	var state string
	require.NoError(t, json.Unmarshal(pqsMember(t, pqsAthenaStatus(t, ts, id), "State"), &state))
	return state
}

// An unseeded query reads exactly as it did before progressions existed: SUCCEEDED at once, with a
// CompletionDateTime and no reason members.
func TestAthenaProgression_AnUnseededQueryIsSucceededAtOnce(t *testing.T) {
	t.Parallel()
	ts := pqsServer(t)
	id := pqsAthenaStart(t, ts)

	status := pqsAthenaStatus(t, ts, id)
	assert.JSONEq(t, `"SUCCEEDED"`, string(pqsMember(t, status, "State")))
	assert.NotEmpty(t, pqsMember(t, status, "CompletionDateTime"), "a completed query has a CompletionDateTime: %s", status)
	assert.NotContains(t, string(status), "StateChangeReason")
	assert.NotContains(t, string(status), "AthenaError")
}

// A seeded query reports its transient state for the seeded number of observations — without a
// CompletionDateTime — then its seeded failure with the published reason members; GetQueryResults
// refuses until it has SUCCEEDED, and does not spend the countdown doing so.
func TestAthenaProgression_ASeededQueryIsObservedQueuedThenFailed(t *testing.T) {
	t.Parallel()
	ts := pqsServer(t)
	pqsSeed(t, ts, "/v1/athena/query-status", `{"queryExecutionId":"*","pendingObservations":2,"state":"QUEUED",
		"finalState":"FAILED","stateChangeReason":"Table not found",
		"athenaError":{"ErrorCategory":2,"ErrorType":1006,"Retryable":false,"ErrorMessage":"TABLE_NOT_FOUND"}}`)
	id := pqsAthenaStart(t, ts)

	for i := range 2 {
		// Results are refused while queued, and the refusal is a peek, not a poll. A peek reports
		// what the next observation will, so it is made before the poll it precedes.
		code, body := pqsCall(t, ts, pqsAthenaHost, "AmazonAthena.GetQueryResults", map[string]any{"QueryExecutionId": id})
		require.Equal(t, http.StatusBadRequest, code, "%s", body)
		assert.Contains(t, string(body), "InvalidRequestException")
		assert.Contains(t, string(body), "Query has not yet finished. Current state: QUEUED")

		status := pqsAthenaStatus(t, ts, id)
		assert.JSONEq(t, `"QUEUED"`, string(pqsMember(t, status, "State")), "observation %d", i+1)
		assert.NotContains(t, string(status), "CompletionDateTime", "a query still queued has not completed: %s", status)
	}

	status := pqsAthenaStatus(t, ts, id)
	assert.JSONEq(t, `"FAILED"`, string(pqsMember(t, status, "State")),
		"the third observation settles: two GetQueryResults calls between polls spent nothing")
	assert.JSONEq(t, `"Table not found"`, string(pqsMember(t, status, "StateChangeReason")))
	assert.JSONEq(t, `{"ErrorCategory":2,"ErrorType":1006,"Retryable":false,"ErrorMessage":"TABLE_NOT_FOUND"}`,
		string(pqsMember(t, status, "AthenaError")))
	assert.NotEmpty(t, pqsMember(t, status, "CompletionDateTime"))
	assert.Equal(t, "FAILED", pqsAthenaState(t, ts, id), "and stays settled")

	code, body := pqsCall(t, ts, pqsAthenaHost, "AmazonAthena.GetQueryResults", map[string]any{"QueryExecutionId": id})
	require.Equal(t, http.StatusBadRequest, code)
	assert.Contains(t, string(body), "Query did not finish successfully. Final query state: FAILED")
}

// #1154: StopQueryExecution leaves the query CANCELLED, two Ls, and a stop outranks a seed — it is
// the caller's own action, so a query stopped mid-countdown reads as stopped from then on.
func TestAthenaProgression_AStoppedQueryIsCancelledWithTwoLs(t *testing.T) {
	t.Parallel()
	ts := pqsServer(t)
	pqsSeed(t, ts, "/v1/athena/query-status", `{"queryExecutionId":"*","pendingObservations":5,"state":"RUNNING"}`)
	id := pqsAthenaStart(t, ts)
	require.Equal(t, "RUNNING", pqsAthenaState(t, ts, id))

	pqsAthena(t, ts, "StopQueryExecution", map[string]any{"QueryExecutionId": id})
	body := pqsAthena(t, ts, "GetQueryExecution", map[string]any{"QueryExecutionId": id})
	assert.JSONEq(t, `"CANCELLED"`, string(pqsMember(t, body, "QueryExecution", "Status", "State")),
		"QueryExecutionStatus.State publishes CANCELLED: %s", body)
	assert.NotContains(t, string(body), `"CANCELED"`)
}

// Clearing a seed makes a query read its own record again; the seed never rewrote it.
func TestAthenaProgression_ClearingTheSeedRestoresTheRecord(t *testing.T) {
	t.Parallel()
	ts := pqsServer(t)
	pqsSeed(t, ts, "/v1/athena/query-status", `{"queryExecutionId":"*","pendingObservations":9}`)
	id := pqsAthenaStart(t, ts)
	require.Equal(t, "RUNNING", pqsAthenaState(t, ts, id), "the default transient state is RUNNING")

	status, out := cpControlPlaneRequest(t, ts, http.MethodDelete, "/v1/athena/query-status", nil)
	require.Equal(t, http.StatusOK, status, "%s", out)
	assert.Equal(t, "SUCCEEDED", pqsAthenaState(t, ts, id))
}

// --- CodeBuild ------------------------------------------------------------------------------

const pqsCodeBuildHost = "codebuild.us-east-1.amazonaws.com"

func pqsCodeBuild(t *testing.T, ts *emulator.TestServer, op string, body any) []byte {
	t.Helper()
	return pqsOK(t, ts, pqsCodeBuildHost, "CodeBuild_20161006."+op, body)
}

func pqsCodeBuildProject(t *testing.T, ts *emulator.TestServer) {
	t.Helper()
	pqsCodeBuild(t, ts, "CreateProject", map[string]any{
		"name": "pqs", "serviceRole": "arn:aws:iam::123456789012:role/codebuild-role",
		"source": map[string]any{"type": "NO_SOURCE", "buildspec": "version: 0.2"}, "artifacts": map[string]any{"type": "NO_ARTIFACTS"},
		"environment": map[string]any{"type": "LINUX_CONTAINER", "image": "aws/codebuild/standard:7.0", "computeType": "BUILD_GENERAL1_SMALL"},
	})
}

func pqsCodeBuildStart(t *testing.T, ts *emulator.TestServer) (id string, build json.RawMessage) {
	t.Helper()
	build = pqsMember(t, pqsCodeBuild(t, ts, "StartBuild", map[string]any{"projectName": "pqs"}), "build")
	require.NoError(t, json.Unmarshal(pqsMember(t, build, "id"), &id))
	return id, build
}

func pqsCodeBuildGet(t *testing.T, ts *emulator.TestServer, ids ...string) []json.RawMessage {
	t.Helper()
	var out struct {
		Builds []json.RawMessage `json:"builds"`
	}
	require.NoError(t, json.Unmarshal(pqsCodeBuild(t, ts, "BatchGetBuilds", map[string]any{"ids": ids}), &out))
	require.Len(t, out.Builds, len(ids))
	return out.Builds
}

// An unseeded build reads as before: SUCCEEDED, COMPLETED, with an endTime and no phases.
func TestCodeBuildProgression_AnUnseededBuildIsSucceededAtOnce(t *testing.T) {
	t.Parallel()
	ts := pqsServer(t)
	pqsCodeBuildProject(t, ts)
	id, started := pqsCodeBuildStart(t, ts)

	for _, build := range []json.RawMessage{started, pqsCodeBuildGet(t, ts, id)[0]} {
		assert.JSONEq(t, `"SUCCEEDED"`, string(pqsMember(t, build, "buildStatus")))
		assert.JSONEq(t, `"COMPLETED"`, string(pqsMember(t, build, "currentPhase")))
		assert.NotEmpty(t, pqsMember(t, build, "endTime"))
		assert.NotContains(t, string(build), "phases")
	}
}

// A seeded build is IN_PROGRESS — in the seeded phase, with no endTime — for the seeded number of
// BatchGetBuilds observations, then reports its seeded failure with the diagnostic in the failed
// phase's contexts. StartBuild's own response peeks, so it reports IN_PROGRESS and spends nothing.
func TestCodeBuildProgression_ASeededBuildIsInProgressThenFailed(t *testing.T) {
	t.Parallel()
	ts := pqsServer(t)
	pqsCodeBuildProject(t, ts)
	pqsSeed(t, ts, "/v1/codebuild/build-status", `{"buildId":"*","pendingObservations":2,"currentPhase":"PRE_BUILD",
		"finalStatus":"FAILED","statusCode":"COMMAND_EXECUTION_ERROR","message":"Error while executing command: make. Reason: exit status 2"}`)
	id, started := pqsCodeBuildStart(t, ts)
	assert.JSONEq(t, `"IN_PROGRESS"`, string(pqsMember(t, started, "buildStatus")), "StartBuild reports the first state")
	assert.NotContains(t, string(started), "endTime")

	for i := range 2 {
		build := pqsCodeBuildGet(t, ts, id)[0]
		assert.JSONEq(t, `"IN_PROGRESS"`, string(pqsMember(t, build, "buildStatus")), "observation %d", i+1)
		assert.JSONEq(t, `"PRE_BUILD"`, string(pqsMember(t, build, "currentPhase")))
		assert.NotContains(t, string(build), "endTime", "a build still in progress has not ended: %s", build)
	}

	build := pqsCodeBuildGet(t, ts, id)[0]
	assert.JSONEq(t, `"FAILED"`, string(pqsMember(t, build, "buildStatus")),
		"the third BatchGetBuilds settles: StartBuild spent nothing")
	assert.JSONEq(t, `"COMPLETED"`, string(pqsMember(t, build, "currentPhase")))
	assert.NotEmpty(t, pqsMember(t, build, "endTime"))
	assert.JSONEq(t, `[{"phaseType":"PRE_BUILD","phaseStatus":"FAILED","contexts":[
		{"statusCode":"COMMAND_EXECUTION_ERROR","message":"Error while executing command: make. Reason: exit status 2"}]}]`,
		string(pqsMember(t, build, "phases")))
}

// One BatchGetBuilds over two builds under a wildcard seed spends one observation from each build's
// own countdown, never two from a shared one (#582).
func TestCodeBuildProgression_OneCallOverTwoBuildsSpendsOneEach(t *testing.T) {
	t.Parallel()
	ts := pqsServer(t)
	pqsCodeBuildProject(t, ts)
	pqsSeed(t, ts, "/v1/codebuild/build-status", `{"buildId":"*","pendingObservations":1}`)
	a, _ := pqsCodeBuildStart(t, ts)
	b, _ := pqsCodeBuildStart(t, ts)

	for _, build := range pqsCodeBuildGet(t, ts, a, b) {
		assert.JSONEq(t, `"IN_PROGRESS"`, string(pqsMember(t, build, "buildStatus")))
	}
	for _, build := range pqsCodeBuildGet(t, ts, a, b) {
		assert.JSONEq(t, `"SUCCEEDED"`, string(pqsMember(t, build, "buildStatus")), "the final state defaults to the record's")
	}
}

// --- CodePipeline ---------------------------------------------------------------------------

const pqsCodePipelineHost = "codepipeline.us-east-1.amazonaws.com"

func pqsCodePipeline(t *testing.T, ts *emulator.TestServer, op string, body any) []byte {
	t.Helper()
	return pqsOK(t, ts, pqsCodePipelineHost, "CodePipeline_20150709."+op, body)
}

func pqsCodePipelineStart(t *testing.T, ts *emulator.TestServer) string {
	t.Helper()
	pqsCodePipeline(t, ts, "CreatePipeline", map[string]any{"pipeline": map[string]any{
		"name": "pqs", "roleArn": "arn:aws:iam::123456789012:role/codepipeline-role",
		"stages": []map[string]any{{"name": "Source", "actions": []any{}}},
	}})
	var out struct {
		ID string `json:"pipelineExecutionId"`
	}
	require.NoError(t, json.Unmarshal(pqsCodePipeline(t, ts, "StartPipelineExecution", map[string]any{"name": "pqs"}), &out))
	return out.ID
}

func pqsCodePipelineGet(t *testing.T, ts *emulator.TestServer, id string) json.RawMessage {
	t.Helper()
	return pqsMember(t, pqsCodePipeline(t, ts, "GetPipelineExecution", map[string]any{"pipelineName": "pqs", "pipelineExecutionId": id}), "pipelineExecution")
}

// An unseeded execution is Succeeded at once, with no statusSummary; a seeded one is Stopping for
// the seeded number of observations, then Stopped with the seeded summary.
func TestCodePipelineProgression_AnExecutionProgressesUnderASeed(t *testing.T) {
	t.Parallel()
	ts := pqsServer(t)
	unseeded := pqsCodePipelineStart(t, ts)
	exec := pqsCodePipelineGet(t, ts, unseeded)
	assert.JSONEq(t, `"Succeeded"`, string(pqsMember(t, exec, "status")))
	assert.NotContains(t, string(exec), "statusSummary")

	pqsSeed(t, ts, "/v1/codepipeline/execution-status", `{"pipelineExecutionId":"*","pendingObservations":2,
		"status":"Stopping","finalStatus":"Stopped","statusSummary":"Stopped by user"}`)
	var out struct {
		ID string `json:"pipelineExecutionId"`
	}
	require.NoError(t, json.Unmarshal(pqsCodePipeline(t, ts, "StartPipelineExecution", map[string]any{"name": "pqs"}), &out))

	for i := range 2 {
		exec := pqsCodePipelineGet(t, ts, out.ID)
		assert.JSONEq(t, `"Stopping"`, string(pqsMember(t, exec, "status")), "observation %d", i+1)
		assert.NotContains(t, string(exec), "statusSummary")
	}
	exec = pqsCodePipelineGet(t, ts, out.ID)
	assert.JSONEq(t, `"Stopped"`, string(pqsMember(t, exec, "status")))
	assert.JSONEq(t, `"Stopped by user"`, string(pqsMember(t, exec, "statusSummary")))
}

// --- Shared ---------------------------------------------------------------------------------

// Each endpoint refuses a state outside its half of the published enumeration, and Athena an
// AthenaError outside API_AthenaError's ranges. A refused seed is not stored.
func TestQueryServiceProgressions_RefuseAnUnpublishedState(t *testing.T) {
	t.Parallel()
	ts := pqsServer(t)
	for _, tc := range []struct{ path, body string }{
		{"/v1/athena/query-status", `{"state":"SUCCEEDED"}`},
		{"/v1/athena/query-status", `{"finalState":"CANCELED"}`},
		{"/v1/athena/query-status", `{"athenaError":{"ErrorCategory":4}}`},
		{"/v1/athena/query-status", `{"athenaError":{"ErrorType":10000}}`},
		{"/v1/athena/query-status", `{"pendingObservations":-1}`},
		{"/v1/codebuild/build-status", `{"currentPhase":"COMPLETED"}`},
		{"/v1/codebuild/build-status", `{"finalStatus":"IN_PROGRESS"}`},
		{"/v1/codepipeline/execution-status", `{"status":"Succeeded"}`},
		{"/v1/codepipeline/execution-status", `{"finalStatus":"Canceled"}`},
	} {
		status, out := cpControlPlaneRequest(t, ts, http.MethodPost, tc.path, []byte(tc.body))
		assert.Equal(t, http.StatusBadRequest, status, "%s %s: %s", tc.path, tc.body, out)
	}
	assert.Empty(t, cpControlPlaneEvents(t, ts), "a refused seed is not recorded")
}

// A recording of all three progressions — seeds, creates and every observation — replays with no
// difference: the seeds are re-issued in position (#1140) and the countdowns restart from zero with
// them, so the same observations report the same states.
func TestQueryServiceProgressions_AReplayReproducesTheObservations(t *testing.T) {
	t.Parallel()
	ts := pqsServer(t)
	pqsCodeBuildProject(t, ts)
	pqsSeed(t, ts, "/v1/athena/query-status", `{"queryExecutionId":"*","pendingObservations":2,"finalState":"FAILED","stateChangeReason":"boom"}`)
	pqsSeed(t, ts, "/v1/codebuild/build-status", `{"buildId":"*","pendingObservations":1,"finalStatus":"TIMED_OUT"}`)
	pqsSeed(t, ts, "/v1/codepipeline/execution-status", `{"pipelineExecutionId":"*","pendingObservations":1,"finalStatus":"Failed"}`)

	query := pqsAthenaStart(t, ts)
	build, _ := pqsCodeBuildStart(t, ts)
	exec := pqsCodePipelineStart(t, ts)
	var athena, codebuild, codepipeline []string
	for range 3 {
		athena = append(athena, pqsAthenaState(t, ts, query))
		var status string
		require.NoError(t, json.Unmarshal(pqsMember(t, pqsCodeBuildGet(t, ts, build)[0], "buildStatus"), &status))
		codebuild = append(codebuild, status)
		require.NoError(t, json.Unmarshal(pqsMember(t, pqsCodePipelineGet(t, ts, exec), "status"), &status))
		codepipeline = append(codepipeline, status)
	}
	require.Equal(t, []string{"RUNNING", "RUNNING", "FAILED"}, athena, "the recording has to progress")
	require.Equal(t, []string{"IN_PROGRESS", "TIMED_OUT", "TIMED_OUT"}, codebuild)
	require.Equal(t, []string{"InProgress", "Failed", "Failed"}, codepipeline)

	results, err := pipelineReplayEngine(ts, emulator.ReplayPipeline{}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	assert.Zero(t, results.SkippedEvents, "every seed is re-executed")
	assert.Zero(t, results.FailedEvents)
	assert.Empty(t, results.Differences, "the replayed observations report the recorded states: %s", replayDifferenceSummary(results))
}

// A store fault reading a seed or a counter is an error, never an observation of the record.
func TestQueryServiceProgressions_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
	}{
		{"seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }},
		{"corrupt seed", func(m *cfFaultStateManager) { m.corruptGet = "status:" }},
		{"counter read", func(m *cfFaultStateManager) { m.failGet = "observed:" }},
		{"counter write", func(m *cfFaultStateManager) { m.failPut = "observed:" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, svc := range pqsFaultServices() {
				fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
				p, id := svc.setup(t, fault)
				require.NoError(t, fault.inner.Put(t.Context(), svc.namespace, "status:*", []byte(`{"pendingObservations":3}`)))
				tc.arm(fault)
				_, err := p.HandleRequest(pqsFaultContext(), svc.observe(t, id))
				assert.Errorf(t, err, "%s %s", svc.name, tc.name)
				var awsErr *emulator.AWSError
				assert.Falsef(t, errors.As(err, &awsErr), "%s %s answered a store fault as %v", svc.name, tc.name, awsErr)
			}
		})
	}
}

// pqsFaultService is one plugin under the store-fault test.
type pqsFaultService struct {
	name, namespace string
	setup           func(t *testing.T, state emulator.StateManager) (emulator.Plugin, string)
	observe         func(t *testing.T, id string) *emulator.AWSRequest
}

func pqsFaultContext() *emulator.RequestContext {
	return &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: "req-pqs-fault", IDs: emulator.NewIDMint("req-pqs-fault")}
}

// pqsFaultPlugin initializes p over state on a frozen clock.
func pqsFaultPlugin(t *testing.T, p emulator.Plugin, state emulator.StateManager) {
	t.Helper()
	tc := emulator.NewTimeController(pqsClock)
	tc.Freeze()
	tc.SetTime(pqsClock)
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State: state, Logger: emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}))
}

// pqsFaultCall issues one request directly and requires it to succeed, for setup.
func pqsFaultCall(t *testing.T, p emulator.Plugin, req *emulator.AWSRequest) []byte {
	t.Helper()
	resp, err := p.HandleRequest(pqsFaultContext(), req)
	require.NoError(t, err, "%s", req.Operation)
	return resp.Body
}

func pqsJSONRequest(t *testing.T, service, target, op string, body any) *emulator.AWSRequest {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	return &emulator.AWSRequest{
		Service: service, Operation: op, Path: "/", Body: raw, Params: map[string]string{},
		Headers: map[string]string{"X-Amz-Target": target + "." + op, "Content-Type": "application/x-amz-json-1.1"},
	}
}

func pqsFaultServices() []pqsFaultService {
	return []pqsFaultService{
		{
			name: "athena", namespace: "athena-query-ctrl",
			setup: func(t *testing.T, state emulator.StateManager) (emulator.Plugin, string) {
				p := &emulator.AthenaPlugin{}
				pqsFaultPlugin(t, p, state)
				var out struct {
					ID string `json:"QueryExecutionId"`
				}
				require.NoError(t, json.Unmarshal(pqsFaultCall(t, p, pqsJSONRequest(t, "athena", "AmazonAthena", "StartQueryExecution", map[string]any{"QueryString": "SELECT 1"})), &out))
				return p, out.ID
			},
			observe: func(t *testing.T, id string) *emulator.AWSRequest {
				return pqsJSONRequest(t, "athena", "AmazonAthena", "GetQueryExecution", map[string]any{"QueryExecutionId": id})
			},
		},
		{
			name: "codebuild", namespace: "codebuild-build-ctrl",
			setup: func(t *testing.T, state emulator.StateManager) (emulator.Plugin, string) {
				p := &emulator.CodeBuildPlugin{}
				pqsFaultPlugin(t, p, state)
				pqsFaultCall(t, p, pqsJSONRequest(t, "codebuild", "CodeBuild_20161006", "CreateProject", map[string]any{
					"name": "pqs", "serviceRole": "arn:aws:iam::123456789012:role/r",
					"source": map[string]any{"type": "NO_SOURCE", "buildspec": "version: 0.2"}, "artifacts": map[string]any{"type": "NO_ARTIFACTS"},
					"environment": map[string]any{"type": "LINUX_CONTAINER", "image": "aws/codebuild/standard:7.0", "computeType": "BUILD_GENERAL1_SMALL"},
				}))
				// StartBuild peeks the seed, which is written after it so the setup itself cannot fault.
				body := pqsFaultCall(t, p, pqsJSONRequest(t, "codebuild", "CodeBuild_20161006", "StartBuild", map[string]any{"projectName": "pqs"}))
				var id string
				require.NoError(t, json.Unmarshal(pqsMember(t, body, "build", "id"), &id))
				return p, id
			},
			observe: func(t *testing.T, id string) *emulator.AWSRequest {
				return pqsJSONRequest(t, "codebuild", "CodeBuild_20161006", "BatchGetBuilds", map[string]any{"ids": []string{id}})
			},
		},
		{
			name: "codepipeline", namespace: "codepipeline-exec-ctrl",
			setup: func(t *testing.T, state emulator.StateManager) (emulator.Plugin, string) {
				p := &emulator.CodePipelinePlugin{}
				pqsFaultPlugin(t, p, state)
				pqsFaultCall(t, p, pqsJSONRequest(t, "codepipeline", "CodePipeline_20150709", "CreatePipeline", map[string]any{"pipeline": map[string]any{
					"name": "pqs", "roleArn": "arn:aws:iam::123456789012:role/r", "stages": []map[string]any{{"name": "Source", "actions": []any{}}},
				}}))
				var out struct {
					ID string `json:"pipelineExecutionId"`
				}
				require.NoError(t, json.Unmarshal(pqsFaultCall(t, p, pqsJSONRequest(t, "codepipeline", "CodePipeline_20150709", "StartPipelineExecution", map[string]any{"name": "pqs"})), &out))
				return p, out.ID
			},
			observe: func(t *testing.T, id string) *emulator.AWSRequest {
				return pqsJSONRequest(t, "codepipeline", "CodePipeline_20150709", "GetPipelineExecution", map[string]any{"pipelineName": "pqs", "pipelineExecutionId": id})
			},
		},
	}
}
