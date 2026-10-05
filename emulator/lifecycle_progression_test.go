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

// #1196's CodeDeploy, EMR Serverless, FSx and Timestream sites: each resource is born in its settled
// state, and a seeded count of observations — the shared progression in emulator/progression.go —
// makes the published transient, failure and deleting states observable to the service's own
// describe. Every sequence below is asserted through the describe operation and then replayed, which
// is the property the seed exists for: the recording's observations must come back identically.

// progressionServer starts a recording test server on a frozen clock.
//
// Frozen because the replay assertion compares every response byte for byte, and a running clock
// lets a millisecond pass between the instant a handler stamps a date and the instant the event is
// recorded, so a replay pinned to the recorded timestamp can answer a date one millisecond off.
func progressionServer(t *testing.T) *emulator.TestServer {
	t.Helper()
	ts := emulator.StartTestServer(t, emulator.WithRecordedBodies())
	clock := time.Unix(1700000000, 0).UTC()
	ts.TimeController().Freeze()
	ts.TimeController().SetTime(clock)
	return ts
}

// progressionSeed POSTs a seed to a progression endpoint and requires it accepted.
func progressionSeed(t *testing.T, ts *emulator.TestServer, path, body string) {
	t.Helper()
	status, got := cpControlPlaneRequest(t, ts, http.MethodPost, path, []byte(body))
	require.Equal(t, http.StatusOK, status, "seed %s = %s", path, got)
}

// progressionReplays replays the recorded stream and requires it to reproduce every response.
//
// refusals is how many recorded requests were refused — a describe after a deleting window ends. A
// replayed refusal is counted in FailedEvents by design, and a refusal that changed would be a
// Difference, so the count pins that exactly those, and no others, were refused.
func progressionReplays(t *testing.T, ts *emulator.TestServer, refusals int) {
	t.Helper()
	results, err := pipelineReplayEngine(ts, emulator.ReplayPipeline{}).Replay(t.Context(), replayStreamID)
	require.NoError(t, err)
	require.Equal(t, refusals, results.FailedEvents, "only the recorded refusals replay as refusals")
	assert.Zero(t, results.SkippedEvents, "the seed is re-executed, not skipped")
	assert.Empty(t, results.Differences, "the replay reproduces the observed sequence: %s", replayDifferenceSummary(results))
}

// progressionJSONTarget issues one JSON-target request and returns its status and decoded body.
func progressionJSONTarget(t *testing.T, ts *emulator.TestServer, host, target string, body any) (int, map[string]any) {
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
	defer func() { _ = resp.Body.Close() }()
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, json.Unmarshal(got, &out), "%s: %s", target, got)
	return resp.StatusCode, out
}

// progressionMember walks a decoded body by member names.
func progressionMember(t *testing.T, body map[string]any, path ...string) any {
	t.Helper()
	var cur any = body
	for _, name := range path {
		m, ok := cur.(map[string]any)
		require.Truef(t, ok, "%v: %s is not an object in %v", path, name, body)
		cur = m[name]
	}
	return cur
}

func TestProgression1196_CodeDeployDeploymentReportsItsSeededStatuses(t *testing.T) {
	t.Parallel()
	ts := progressionServer(t)
	const target = "CodeDeploy_20141006."
	idsJSONTargetCall(t, ts, idsCodeDeployHost, target+"CreateApplication", map[string]any{"applicationName": "prog-app"})

	var unseeded struct {
		DeploymentID string `json:"deploymentId"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsCodeDeployHost, target+"CreateDeployment",
		map[string]any{"applicationName": "prog-app"}), &unseeded))
	_, got := progressionJSONTarget(t, ts, idsCodeDeployHost, target+"GetDeployment", map[string]any{"deploymentId": unseeded.DeploymentID})
	assert.Equal(t, "Succeeded", progressionMember(t, got, "deploymentInfo", "status"), "an unseeded deployment reads as before")
	assert.NotNil(t, progressionMember(t, got, "deploymentInfo", "completeTime"))

	progressionSeed(t, ts, "/v1/codedeploy/deployment-status",
		`{"deploymentId":"*","pendingObservations":2,"finalState":"Failed","errorCode":"HEALTH_CONSTRAINTS","errorMessage":"too few healthy instances"}`)
	var seeded struct {
		DeploymentID string `json:"deploymentId"`
	}
	require.NoError(t, json.Unmarshal(idsJSONTargetCall(t, ts, idsCodeDeployHost, target+"CreateDeployment",
		map[string]any{"applicationName": "prog-app"}), &seeded))
	for i := range 2 {
		_, got := progressionJSONTarget(t, ts, idsCodeDeployHost, target+"GetDeployment", map[string]any{"deploymentId": seeded.DeploymentID})
		assert.Equalf(t, "InProgress", progressionMember(t, got, "deploymentInfo", "status"), "observation %d", i+1)
		assert.Nil(t, progressionMember(t, got, "deploymentInfo", "completeTime"), "an incomplete deployment has no completeTime")
		assert.Nil(t, progressionMember(t, got, "deploymentInfo", "errorInformation"))
	}
	_, got = progressionJSONTarget(t, ts, idsCodeDeployHost, target+"GetDeployment", map[string]any{"deploymentId": seeded.DeploymentID})
	assert.Equal(t, "Failed", progressionMember(t, got, "deploymentInfo", "status"))
	assert.Equal(t, map[string]any{"code": "HEALTH_CONSTRAINTS", "message": "too few healthy instances"},
		progressionMember(t, got, "deploymentInfo", "errorInformation"))
	assert.NotNil(t, progressionMember(t, got, "deploymentInfo", "completeTime"))

	progressionReplays(t, ts, 0)
}

func TestProgression1196_EMRServerlessJobRunAndApplicationReportTheirSeededStates(t *testing.T) {
	t.Parallel()
	ts := progressionServer(t)
	rest := func(method, path string, body any) map[string]any {
		t.Helper()
		var out map[string]any
		require.NoError(t, json.Unmarshal(idsRESTCall(t, ts, idsEMRServerlessHost, method, path, body), &out))
		return out
	}

	progressionSeed(t, ts, "/v1/emr-serverless/application-status", `{"applicationId":"*","pendingObservations":1}`)
	app := rest(http.MethodPost, "/applications", map[string]any{
		"name": "prog", "type": "SPARK", "releaseLabel": "emr-6.9.0", "clientToken": "prog-app",
	})
	appID := app["applicationId"].(string)
	assert.Equal(t, "CREATING", progressionMember(t, rest(http.MethodGet, "/applications/"+appID, nil), "application", "state"))
	assert.Equal(t, "CREATED", progressionMember(t, rest(http.MethodGet, "/applications/"+appID, nil), "application", "state"))

	progressionSeed(t, ts, "/v1/emr-serverless/job-run-status",
		`{"jobRunId":"*","pendingObservations":2,"state":"PENDING","finalState":"FAILED","stateDetails":"The driver exited with status 1."}`)
	start := func(token string) string {
		t.Helper()
		run := rest(http.MethodPost, "/applications/"+appID+"/jobruns", map[string]any{
			"name": token, "clientToken": token, "executionRoleArn": "arn:aws:iam::123456789012:role/prog",
		})
		return run["jobRunId"].(string)
	}
	failing := start("prog-fail")
	runPath := "/applications/" + appID + "/jobruns/" + failing
	for i := range 2 {
		assert.Equalf(t, "PENDING", progressionMember(t, rest(http.MethodGet, runPath, nil), "jobRun", "state"), "observation %d", i+1)
	}
	got := rest(http.MethodGet, runPath, nil)
	assert.Equal(t, "FAILED", progressionMember(t, got, "jobRun", "state"))
	assert.Equal(t, "The driver exited with status 1.", progressionMember(t, got, "jobRun", "stateDetails"))

	// A cancel restarts the countdown: CANCELLING for the seeded observations, then CANCELLED. The run's
	// countdown is spent first, so CANCELLING is reported only because the cancel restarted it.
	cancelled := start("prog-cancel")
	cancelPath := "/applications/" + appID + "/jobruns/" + cancelled
	for range 3 {
		rest(http.MethodGet, cancelPath, nil)
	}
	rest(http.MethodDelete, cancelPath, nil)
	for i := range 2 {
		assert.Equalf(t, "CANCELLING", progressionMember(t, rest(http.MethodGet, cancelPath, nil), "jobRun", "state"), "observation %d", i+1)
	}
	assert.Equal(t, "CANCELLED", progressionMember(t, rest(http.MethodGet, cancelPath, nil), "jobRun", "state"))

	// ListJobRuns observes too, so it agrees with GetJobRun: a fresh run is PENDING on the list and on
	// the GetJobRun after it, and the list's states filter narrows by the state the caller is shown.
	listed := start("prog-list")
	list := rest(http.MethodGet, "/applications/"+appID+"/jobruns?states=PENDING", nil)
	runs := progressionMember(t, list, "jobRuns").([]any)
	require.Len(t, runs, 1, "only the fresh run is PENDING: %v", list)
	assert.Equal(t, listed, runs[0].(map[string]any)["id"])
	assert.Equal(t, "PENDING", progressionMember(t, rest(http.MethodGet, "/applications/"+appID+"/jobruns/"+listed, nil), "jobRun", "state"))

	progressionReplays(t, ts, 0)
}

func TestProgression1196_FSxFileSystemIsCreatingThenSeededAndDeletesThroughAWindow(t *testing.T) {
	t.Parallel()
	ts := progressionServer(t)
	const host, target = "fsx.us-east-1.amazonaws.com", "AWSSimbaAPIService_v20180301."
	create := func() string {
		t.Helper()
		status, got := progressionJSONTarget(t, ts, host, target+"CreateFileSystem", map[string]any{
			"FileSystemType": "LUSTRE", "StorageCapacity": 1200, "SubnetIds": []string{"subnet-0123456789abcdef0"},
		})
		require.Equal(t, http.StatusOK, status, "%v", got)
		assert.Equal(t, "CREATING", progressionMember(t, got, "FileSystem", "Lifecycle"),
			"API_CreateFileSystem returns while the file system is still CREATING")
		return progressionMember(t, got, "FileSystem", "FileSystemId").(string)
	}
	describe := func(id string) (int, map[string]any) {
		t.Helper()
		return progressionJSONTarget(t, ts, host, target+"DescribeFileSystems", map[string]any{"FileSystemIds": []string{id}})
	}
	lifecycle := func(id string) any {
		t.Helper()
		status, got := describe(id)
		require.Equal(t, http.StatusOK, status, "%v", got)
		fss := progressionMember(t, got, "FileSystems").([]any)
		require.Len(t, fss, 1)
		return fss[0].(map[string]any)["Lifecycle"]
	}

	unseeded := create()
	assert.Equal(t, "AVAILABLE", lifecycle(unseeded), "unseeded, the first describe reads AVAILABLE, as before")

	progressionSeed(t, ts, "/v1/fsx/file-system-status",
		`{"fileSystemId":"*","pendingObservations":2,"finalState":"FAILED","failureMessage":"The subnet has no free IP addresses."}`)
	seeded := create()
	assert.Equal(t, "CREATING", lifecycle(seeded))
	assert.Equal(t, "CREATING", lifecycle(seeded))
	_, got := describe(seeded)
	fs := progressionMember(t, got, "FileSystems").([]any)[0].(map[string]any)
	assert.Equal(t, "FAILED", fs["Lifecycle"])
	assert.Equal(t, map[string]any{"Message": "The subnet has no free IP addresses."}, fs["FailureDetails"])

	// The delete answers DELETING, and under the seed the next two describes do too; the third ends
	// the window, and the file system is gone.
	status, deleted := progressionJSONTarget(t, ts, host, target+"DeleteFileSystem", map[string]any{"FileSystemId": seeded})
	require.Equal(t, http.StatusOK, status, "%v", deleted)
	assert.Equal(t, "DELETING", deleted["Lifecycle"])
	assert.Equal(t, "DELETING", lifecycle(seeded))
	assert.Equal(t, "DELETING", lifecycle(seeded))
	status, got = describe(seeded)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "FileSystemNotFound", got["__type"], "%v", got)

	progressionReplays(t, ts, 1)
}

func TestProgression1196_TimestreamTableRestoresAndDeletesThroughAWindow(t *testing.T) {
	t.Parallel()
	ts := progressionServer(t)
	const target = "Timestream_20181101."
	call := func(op string, body map[string]any) (int, map[string]any) {
		t.Helper()
		return progressionJSONTarget(t, ts, idsTimestreamWriteHost, target+op, body)
	}
	table := map[string]any{"DatabaseName": "progdb", "TableName": "progtbl"}
	status := func() any {
		t.Helper()
		code, got := call("DescribeTable", table)
		require.Equal(t, http.StatusOK, code, "%v", got)
		return progressionMember(t, got, "Table", "TableStatus")
	}
	idsJSONTargetCall(t, ts, idsTimestreamWriteHost, target+"CreateDatabase", map[string]any{"DatabaseName": "progdb"})
	_, created := call("CreateTable", table)
	assert.Equal(t, "ACTIVE", progressionMember(t, created, "Table", "TableStatus"),
		"API_Table publishes no creating state, so a table is created ACTIVE")
	assert.Equal(t, "ACTIVE", status(), "unseeded, a table reads ACTIVE")

	progressionSeed(t, ts, "/v1/timestream-write/table-status",
		`{"databaseName":"progdb","tableName":"progtbl","pendingObservations":2,"state":"RESTORING"}`)
	assert.Equal(t, "RESTORING", status())
	assert.Equal(t, "RESTORING", status())
	assert.Equal(t, "ACTIVE", status())

	code, got := call("DeleteTable", table)
	require.Equal(t, http.StatusOK, code, "%v", got)
	assert.Equal(t, "DELETING", status())
	_, listed := call("ListTables", map[string]any{"DatabaseName": "progdb"})
	tables := progressionMember(t, listed, "Tables").([]any)
	require.Len(t, tables, 1, "ListTables reports the deleting table: %v", listed)
	assert.Equal(t, "DELETING", tables[0].(map[string]any)["TableStatus"])
	code, got = call("DescribeTable", table)
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Equal(t, "ResourceNotFoundException", got["__type"], "%v", got)

	progressionReplays(t, ts, 1)
}

// Each endpoint refuses a seed outside the published enumeration, or otherwise malformed, before
// storing it.
func TestProgression1196_ASeedOutsideThePublishedStatesIsRefused(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	for _, tc := range []struct {
		name, path, body string
	}{
		{"codedeploy unknown status", "/v1/codedeploy/deployment-status", `{"state":"Running"}`},
		{"codedeploy non-terminal final", "/v1/codedeploy/deployment-status", `{"finalState":"InProgress"}`},
		{"codedeploy terminal transient", "/v1/codedeploy/deployment-status", `{"state":"Succeeded"}`},
		{"codedeploy error beside success", "/v1/codedeploy/deployment-status", `{"errorCode":"TIMEOUT"}`},
		{"codedeploy unknown error code", "/v1/codedeploy/deployment-status", `{"finalState":"Failed","errorCode":"BROKEN"}`},
		{"codedeploy negative count", "/v1/codedeploy/deployment-status", `{"pendingObservations":-1}`},
		{"emr unknown run state", "/v1/emr-serverless/job-run-status", `{"state":"COMPLETED"}`},
		{"emr non-terminal final", "/v1/emr-serverless/job-run-status", `{"finalState":"RUNNING"}`},
		{"emr terminal transient", "/v1/emr-serverless/job-run-status", `{"state":"SUCCESS"}`},
		{"emr blank stateDetails", "/v1/emr-serverless/job-run-status", `{"finalState":"FAILED","stateDetails":"   "}`},
		{"emr unknown application state", "/v1/emr-serverless/application-status", `{"state":"RUNNING"}`},
		{"fsx unknown lifecycle", "/v1/fsx/file-system-status", `{"state":"PENDING"}`},
		{"fsx settled transient", "/v1/fsx/file-system-status", `{"state":"AVAILABLE"}`},
		{"fsx transient final", "/v1/fsx/file-system-status", `{"finalState":"CREATING"}`},
		{"fsx failure beside success", "/v1/fsx/file-system-status", `{"failureMessage":"x"}`},
		{"timestream table without database", "/v1/timestream-write/table-status", `{"tableName":"t"}`},
		{"timestream non-restoring state", "/v1/timestream-write/table-status", `{"state":"DELETING"}`},
		{"timestream unknown state", "/v1/timestream-write/table-status", `{"state":"CREATING"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, got := cpControlPlaneRequest(t, ts, http.MethodPost, tc.path, []byte(tc.body))
			assert.Equal(t, http.StatusBadRequest, status, "%s: %s", tc.body, got)
		})
	}
}

// progressionFaultPlugin initializes p over a faulting store on a frozen clock.
func progressionFaultPlugin(t *testing.T, p emulator.Plugin, fault *cfFaultStateManager) *emulator.RequestContext {
	t.Helper()
	clock := time.Unix(1700000000, 0).UTC()
	tc := emulator.NewTimeController(clock)
	tc.Freeze()
	tc.SetTime(clock)
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   fault,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}))
	return &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: "req-prog-fault", IDs: emulator.NewIDMint("req-prog-fault")}
}

// progressionCall dispatches one JSON-target operation straight to a plugin.
func progressionCall(t *testing.T, p emulator.Plugin, ctx *emulator.RequestContext, service, op string, body any) ([]byte, error) {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{Service: service, Operation: op, Body: raw, Headers: map[string]string{}, Params: map[string]string{}})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// requireStoreFault requires err to be a store fault, never answered as a published refusal.
func requireStoreFault(t *testing.T, err error, what string) {
	t.Helper()
	require.Errorf(t, err, "%s must fail on a store fault", what)
	var awsErr *emulator.AWSError
	require.Falsef(t, errors.As(err, &awsErr), "%s answered a store fault as the published %v", what, awsErr)
}

// A store fault on the progression's seed read, its counter write, or a windowed delete's record write
// is returned as an error, not answered as a status.
func TestProgression1196_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	seedAll := func(t *testing.T, fault *cfFaultStateManager, namespace, seed string) {
		t.Helper()
		require.NoError(t, fault.inner.Put(t.Context(), namespace, "status:*", []byte(seed)))
	}

	t.Run("codedeploy seed read", func(t *testing.T) {
		t.Parallel()
		fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
		p := &emulator.CodeDeployPlugin{}
		ctx := progressionFaultPlugin(t, p, fault)
		_, err := progressionCall(t, p, ctx, "codedeploy", "CreateApplication", map[string]any{"applicationName": "f"})
		require.NoError(t, err)
		body, err := progressionCall(t, p, ctx, "codedeploy", "CreateDeployment", map[string]any{"applicationName": "f"})
		require.NoError(t, err)
		var dep struct {
			DeploymentID string `json:"deploymentId"`
		}
		require.NoError(t, json.Unmarshal(body, &dep))
		fault.failGet = "status:"
		_, err = progressionCall(t, p, ctx, "codedeploy", "GetDeployment", map[string]any{"deploymentId": dep.DeploymentID})
		requireStoreFault(t, err, "GetDeployment")
	})

	t.Run("fsx counter write and windowed delete", func(t *testing.T) {
		t.Parallel()
		fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
		p := &emulator.FSxPlugin{}
		ctx := progressionFaultPlugin(t, p, fault)
		body, err := progressionCall(t, p, ctx, "fsx", "CreateFileSystem", map[string]any{
			"FileSystemType": "LUSTRE", "StorageCapacity": 1200, "SubnetIds": []string{"subnet-0123456789abcdef0"},
		})
		require.NoError(t, err)
		var out struct {
			FileSystem struct {
				FileSystemID string `json:"FileSystemId"`
			} `json:"FileSystem"`
		}
		require.NoError(t, json.Unmarshal(body, &out))
		id := out.FileSystem.FileSystemID
		seedAll(t, fault, "fsx-fs-ctrl", `{"fileSystemId":"*","pendingObservations":2}`)

		fault.failPut = "observed:"
		_, err = progressionCall(t, p, ctx, "fsx", "DescribeFileSystems", map[string]any{"FileSystemIds": []string{id}})
		requireStoreFault(t, err, "DescribeFileSystems")

		fault.failPut = "fs:"
		_, err = progressionCall(t, p, ctx, "fsx", "DeleteFileSystem", map[string]any{"FileSystemId": id})
		requireStoreFault(t, err, "DeleteFileSystem")
	})

	t.Run("timestream windowed delete reset", func(t *testing.T) {
		t.Parallel()
		fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
		p := &emulator.TimestreamPlugin{}
		ctx := progressionFaultPlugin(t, p, fault)
		_, err := progressionCall(t, p, ctx, "timestream", "CreateDatabase", map[string]any{"DatabaseName": "fdb"})
		require.NoError(t, err)
		table := map[string]any{"DatabaseName": "fdb", "TableName": "ftbl"}
		_, err = progressionCall(t, p, ctx, "timestream", "CreateTable", table)
		require.NoError(t, err)
		seedAll(t, fault, "timestream-table-ctrl", `{"pendingObservations":1}`)

		fault.failDelete = "observed:"
		_, err = progressionCall(t, p, ctx, "timestream", "DeleteTable", table)
		requireStoreFault(t, err, "DeleteTable")

		fault.failDelete = ""
		fault.failGet = "status:"
		_, err = progressionCall(t, p, ctx, "timestream", "DescribeTable", table)
		requireStoreFault(t, err, "DescribeTable")
	})

	t.Run("emr cancel reset", func(t *testing.T) {
		t.Parallel()
		fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
		p := &emulator.EMRServerlessPlugin{}
		ctx := progressionFaultPlugin(t, p, fault)
		rest := func(method, path string, body any) ([]byte, error) {
			var raw []byte
			if body != nil {
				var err error
				raw, err = json.Marshal(body)
				require.NoError(t, err)
			}
			resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{Service: "emr-serverless", HTTPMethod: method, Path: path, Body: raw, Headers: map[string]string{}, Params: map[string]string{}})
			if err != nil {
				return nil, err
			}
			return resp.Body, nil
		}
		body, err := rest(http.MethodPost, "/applications", map[string]any{"type": "SPARK", "releaseLabel": "emr-6.9.0", "clientToken": "f"})
		require.NoError(t, err)
		var app struct {
			ApplicationID string `json:"applicationId"`
		}
		require.NoError(t, json.Unmarshal(body, &app))
		body, err = rest(http.MethodPost, "/applications/"+app.ApplicationID+"/jobruns", map[string]any{
			"clientToken": "f", "executionRoleArn": "arn:aws:iam::123456789012:role/f",
		})
		require.NoError(t, err)
		var run struct {
			JobRunID string `json:"jobRunId"`
		}
		require.NoError(t, json.Unmarshal(body, &run))

		fault.failDelete = "observed:"
		_, err = rest(http.MethodDelete, "/applications/"+app.ApplicationID+"/jobruns/"+run.JobRunID, nil)
		requireStoreFault(t, err, "CancelJobRun")

		fault.failDelete = ""
		fault.failGet = "status:"
		_, err = rest(http.MethodGet, "/applications/"+app.ApplicationID, nil)
		requireStoreFault(t, err, "GetApplication")
	})
}
