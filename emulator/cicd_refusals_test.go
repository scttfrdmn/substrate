package emulator_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// CodePipeline's not-found refusals at the status its pages publish (#1156), and CodeBuild's
// DeleteProject, BatchGetProjects and BatchGetBuilds as their pages publish them (#1159, #1186).
// Every assertion is over the wire, on the code and the status, because a test that checks only
// the code is how a 404 where the page says 400 survived.

// cicdCall sends one JSON-target operation through a full test server and returns the status and
// raw body.
func cicdCall(t *testing.T, ts *emulator.TestServer, host, target, op string, body any) (int, []byte) {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/", bytes.NewReader(data))
	require.NoError(t, err)
	req.Host = host
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", target+"."+op)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return resp.StatusCode, out
}

// cicdCode returns a JSON error body's code, without any namespace prefix.
func cicdCode(t *testing.T, body []byte) string {
	t.Helper()
	var doc struct {
		Type string `json:"__type"`
		Code string `json:"code"`
	}
	require.NoError(t, json.Unmarshal(body, &doc), "%s", body)
	code := doc.Type
	if code == "" {
		code = doc.Code
	}
	if _, after, ok := strings.Cut(code, "#"); ok {
		code = after
	}
	return code
}

const (
	cicdPipelineHost = "codepipeline.us-east-1.amazonaws.com"
	cicdPipelineTgt  = "CodePipeline_20150709"
	cicdBuildHost    = "codebuild.us-east-1.amazonaws.com"
	cicdBuildTgt     = "CodeBuild_20161006"
)

func TestCodePipelineRefusals_ANotFoundAnswers400(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	call := func(op string, body any) (int, []byte) {
		return cicdCall(t, ts, cicdPipelineHost, cicdPipelineTgt, op, body)
	}
	pipeline := func(name string) map[string]any {
		return map[string]any{"pipeline": map[string]any{
			"name": name, "roleArn": "arn:aws:iam::123456789012:role/cp",
			"stages": []map[string]any{{"name": "Source", "actions": []any{}}, {"name": "Deploy", "actions": []any{}}},
		}}
	}
	for _, name := range []string{"cp-one", "cp-two"} {
		status, body := call("CreatePipeline", pipeline(name))
		require.Equal(t, http.StatusOK, status, "CreatePipeline %s: %s", name, body)
	}
	status, body := call("StartPipelineExecution", map[string]any{"name": "cp-one"})
	require.Equal(t, http.StatusOK, status, "StartPipelineExecution: %s", body)
	var started struct {
		ID string `json:"pipelineExecutionId"`
	}
	require.NoError(t, json.Unmarshal(body, &started), "%s", body)

	// Every operation that loads a pipeline propagates loadPipeline's refusal.
	missing := map[string]any{"name": "cp-missing"}
	for _, tc := range []struct {
		op   string
		body any
		code string
	}{
		{"GetPipeline", missing, "PipelineNotFoundException"},
		{"GetPipelineState", missing, "PipelineNotFoundException"},
		{"StartPipelineExecution", missing, "PipelineNotFoundException"},
		{"DeletePipeline", missing, "PipelineNotFoundException"},
		{"UpdatePipeline", pipeline("cp-missing"), "PipelineNotFoundException"},
		{"GetPipelineExecution", map[string]any{"pipelineName": "cp-one", "pipelineExecutionId": "00000000-0000-4000-8000-000000000000"}, "PipelineExecutionNotFoundException"},
		// API_GetPipelineExecution: the code covers "an execution ID does not belong to the
		// specified pipeline".
		{"GetPipelineExecution/another pipeline's execution", map[string]any{"pipelineName": "cp-two", "pipelineExecutionId": started.ID}, "PipelineExecutionNotFoundException"},
	} {
		t.Run(tc.op, func(t *testing.T) {
			op, _, _ := strings.Cut(tc.op, "/")
			status, body := call(op, tc.body)
			require.Equal(t, http.StatusBadRequest, status, "%s: %s", tc.op, body)
			require.Equal(t, tc.code, cicdCode(t, body), "%s: %s", tc.op, body)
		})
	}

	// The execution's own pipeline still reads it.
	status, body = call("GetPipelineExecution", map[string]any{"pipelineName": "cp-one", "pipelineExecutionId": started.ID})
	require.Equal(t, http.StatusOK, status, "GetPipelineExecution on its own pipeline: %s", body)

	// ListPipelines still answers 200 and lists what exists.
	status, body = call("ListPipelines", map[string]any{})
	require.Equal(t, http.StatusOK, status, "ListPipelines: %s", body)
	require.Contains(t, string(body), `"cp-one"`, "%s", body)
}

func TestCodeBuildDeleteProject_IsIdempotent(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	call := func(op string, body any) (int, []byte) {
		return cicdCall(t, ts, cicdBuildHost, cicdBuildTgt, op, body)
	}
	status, body := call("CreateProject", map[string]any{
		"name": "cb-del", "serviceRole": "arn:aws:iam::123456789012:role/cb",
		"source":      map[string]any{"type": "NO_SOURCE", "buildspec": "version: 0.2"},
		"artifacts":   map[string]any{"type": "NO_ARTIFACTS"},
		"environment": map[string]any{"type": "LINUX_CONTAINER", "image": "aws/codebuild/standard:7.0", "computeType": "BUILD_GENERAL1_SMALL"},
	})
	require.Equal(t, http.StatusOK, status, "CreateProject: %s", body)

	// The double delete a teardown path makes: both succeed, as API_DeleteProject publishes.
	for i := range 2 {
		status, body = call("DeleteProject", map[string]any{"name": "cb-del"})
		require.Equalf(t, http.StatusOK, status, "DeleteProject #%d: %s", i+1, body)
		require.JSONEq(t, `{}`, string(body))
	}
	status, body = call("DeleteProject", map[string]any{"name": "cb-never-existed"})
	require.Equal(t, http.StatusOK, status, "DeleteProject of a project that never existed: %s", body)

	status, body = call("BatchGetProjects", map[string]any{"names": []string{"cb-del"}})
	require.Equal(t, http.StatusOK, status, "%s", body)
	require.Contains(t, string(body), `"projectsNotFound":["cb-del"]`, "the deleted project is not found: %s", body)

	// The operation's one published error still answers an absent or empty name.
	for _, b := range []any{map[string]any{}, map[string]any{"name": ""}} {
		status, body = call("DeleteProject", b)
		require.Equal(t, http.StatusBadRequest, status, "%s", body)
		require.Equal(t, "InvalidInputException", cicdCode(t, body), "%s", body)
	}
}

func TestCodeBuildBatchReads_HoldTheirListToThePublishedBounds(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	call := func(op string, body any) (int, []byte) {
		return cicdCall(t, ts, cicdBuildHost, cicdBuildTgt, op, body)
	}
	tooMany := make([]string, 101)
	for i := range tooMany {
		tooMany[i] = "x"
	}
	for _, rd := range []struct{ op, member string }{{"BatchGetBuilds", "ids"}, {"BatchGetProjects", "names"}} {
		for _, tc := range []struct {
			name string
			body any
		}{
			{"absent", map[string]any{}},
			{"empty", map[string]any{rd.member: []string{}}},
			{"more than 100", map[string]any{rd.member: tooMany}},
			{"an empty string", map[string]any{rd.member: []string{"real-looking", ""}}},
		} {
			t.Run(rd.op+"/"+tc.name, func(t *testing.T) {
				status, body := call(rd.op, tc.body)
				require.Equal(t, http.StatusBadRequest, status, "%s", body)
				require.Equal(t, "InvalidInputException", cicdCode(t, body), "%s", body)
			})
		}
	}

	// A well-formed value naming nothing is what the not-found member is for.
	status, body := call("BatchGetBuilds", map[string]any{"ids": []string{"cb-none:00000000-0000-4000-8000-000000000000"}})
	require.Equal(t, http.StatusOK, status, "%s", body)
	require.Contains(t, string(body), `"buildsNotFound":["cb-none:00000000-0000-4000-8000-000000000000"]`, "%s", body)
	status, body = call("BatchGetProjects", map[string]any{"names": []string{"cb-none"}})
	require.Equal(t, http.StatusOK, status, "%s", body)
	require.Contains(t, string(body), `"projectsNotFound":["cb-none"]`, "%s", body)
}

// A store fault is substrate's failure, never a resource "for which information could not be
// found" (#1186): each answers an error rather than a 200 naming the ID as missing.
func TestCodeBuildReads_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
		op   string
		body map[string]any
	}{
		{"BatchGetBuilds, read", func(m *cfFaultStateManager) { m.failGet = "build:" }, "BatchGetBuilds", map[string]any{"ids": []string{"cb-f:1"}}},
		{"BatchGetBuilds, corrupt record", func(m *cfFaultStateManager) { m.corruptGet = "build:" }, "BatchGetBuilds", map[string]any{"ids": []string{"cb-f:1"}}},
		{"BatchGetProjects, read", func(m *cfFaultStateManager) { m.failGet = "project:" }, "BatchGetProjects", map[string]any{"names": []string{"cb-f"}}},
		{"DeleteProject, read", func(m *cfFaultStateManager) { m.failGet = "project:" }, "DeleteProject", map[string]any{"name": "cb-f"}},
		{"DeleteProject, delete", func(m *cfFaultStateManager) { m.failDelete = "project:" }, "DeleteProject", map[string]any{"name": "cb-f"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			p := &emulator.CodeBuildPlugin{}
			clock := time.Unix(1700000000, 0).UTC()
			tcl := emulator.NewTimeController(clock)
			tcl.Freeze()
			tcl.SetTime(clock)
			require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
				State: fault, Logger: emulator.NewDefaultLogger(slog.LevelError, false),
				Options: map[string]any{"time_controller": tcl},
			}))
			ctx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: "req-cb-fault", IDs: emulator.NewIDMint("req-cb-fault")}
			// A project and a build to read, so DeleteProject's delete and the corrupt build are reached.
			wireJSONTarget(t, p, ctx, "codebuild", cicdBuildTgt, "CreateProject", map[string]any{
				"name": "cb-f", "serviceRole": "arn:aws:iam::123456789012:role/cb",
				"source":      map[string]any{"type": "NO_SOURCE", "buildspec": "version: 0.2"},
				"artifacts":   map[string]any{"type": "NO_ARTIFACTS"},
				"environment": map[string]any{"type": "LINUX_CONTAINER", "image": "aws/codebuild/standard:7.0", "computeType": "BUILD_GENERAL1_SMALL"},
			})
			started := wireJSONTarget(t, p, ctx, "codebuild", cicdBuildTgt, "StartBuild", map[string]any{"projectName": "cb-f"})
			var sb struct {
				Build struct {
					ID string `json:"id"`
				} `json:"build"`
			}
			require.NoError(t, json.Unmarshal(started, &sb), "%s", started)
			if ids, ok := tc.body["ids"]; ok && ids != nil {
				tc.body["ids"] = []string{sb.Build.ID}
			}

			tc.arm(fault)
			raw, err := json.Marshal(tc.body)
			require.NoError(t, err)
			_, err = p.HandleRequest(ctx, &emulator.AWSRequest{
				Service: "codebuild", Operation: tc.op, Path: "/", Body: raw,
				Headers: map[string]string{"X-Amz-Target": cicdBuildTgt + "." + tc.op, "Content-Type": "application/x-amz-json-1.1"},
				Params:  map[string]string{},
			})
			require.Error(t, err, "%s must fail on a store fault", tc.name)
			var awsErr *emulator.AWSError
			require.Falsef(t, errors.As(err, &awsErr), "%s answered a store fault as the published %v", tc.name, awsErr)
		})
	}
}
