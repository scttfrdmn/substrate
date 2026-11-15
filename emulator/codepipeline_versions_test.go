package emulator_test

// GetPipeline honors `version` from a kept version history, and GetPipelineExecution refuses a
// request that omits a required member (#1160). Over the wire.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

func cpVersionsPipeline(name, role string) map[string]any {
	return map[string]any{"pipeline": map[string]any{
		"name": name, "roleArn": role,
		"stages": []map[string]any{{"name": "Source", "actions": []any{}}},
	}}
}

func TestCodePipelineVersions_GetPipelineAnswersTheVersionAskedFor(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	call := func(op string, body any) (int, []byte) {
		return cicdCall(t, ts, cicdPipelineHost, cicdPipelineTgt, op, body)
	}
	roles := []string{"arn:aws:iam::123456789012:role/v1", "arn:aws:iam::123456789012:role/v2", "arn:aws:iam::123456789012:role/v3"}
	status, body := call("CreatePipeline", cpVersionsPipeline("versioned", roles[0]))
	require.Equal(t, http.StatusOK, status, "%s", body)
	for _, role := range roles[1:] {
		status, body := call("UpdatePipeline", cpVersionsPipeline("versioned", role))
		require.Equal(t, http.StatusOK, status, "%s", body)
	}

	type got struct {
		Pipeline struct {
			RoleArn string `json:"roleArn"`
			Version int    `json:"version"`
		} `json:"pipeline"`
	}
	get := func(body map[string]any) got {
		status, raw := call("GetPipeline", body)
		require.Equal(t, http.StatusOK, status, "%v: %s", body, raw)
		var g got
		require.NoError(t, json.Unmarshal(raw, &g))
		return g
	}

	current := get(map[string]any{"name": "versioned"})
	assert.Equal(t, 3, current.Pipeline.Version, "no version is the current one")
	assert.Equal(t, roles[2], current.Pipeline.RoleArn)
	for v := 1; v <= 3; v++ {
		g := get(map[string]any{"name": "versioned", "version": v})
		assert.Equal(t, v, g.Pipeline.Version)
		assert.Equal(t, roles[v-1], g.Pipeline.RoleArn, "version %d answers its own structure", v)
	}

	for _, v := range []int{0, -1, 4} {
		status, raw := call("GetPipeline", map[string]any{"name": "versioned", "version": v})
		assert.Equal(t, http.StatusBadRequest, status, "version %d: %s", v, raw)
		assert.Equal(t, "PipelineVersionNotFoundException", cicdCode(t, raw), "version %d: %s", v, raw)
	}

	// A pipeline re-created under the same name does not inherit its predecessor's versions.
	status, body = call("DeletePipeline", map[string]any{"name": "versioned"})
	require.Equal(t, http.StatusOK, status, "%s", body)
	status, body = call("CreatePipeline", cpVersionsPipeline("versioned", roles[0]))
	require.Equal(t, http.StatusOK, status, "%s", body)
	status, body = call("GetPipeline", map[string]any{"name": "versioned", "version": 2})
	assert.Equal(t, http.StatusBadRequest, status, "%s", body)
	assert.Equal(t, "PipelineVersionNotFoundException", cicdCode(t, body), "%s", body)
}

func TestCodePipelineVersions_GetPipelineExecutionRequiresBothMembers(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	call := func(op string, body any) (int, []byte) {
		return cicdCall(t, ts, cicdPipelineHost, cicdPipelineTgt, op, body)
	}
	status, body := call("CreatePipeline", cpVersionsPipeline("owner", "arn:aws:iam::123456789012:role/cp"))
	require.Equal(t, http.StatusOK, status, "%s", body)
	status, body = call("StartPipelineExecution", map[string]any{"name": "owner"})
	require.Equal(t, http.StatusOK, status, "%s", body)
	var started struct {
		ID string `json:"pipelineExecutionId"`
	}
	require.NoError(t, json.Unmarshal(body, &started))

	for name, req := range map[string]map[string]any{
		"no pipelineName":        {"pipelineExecutionId": started.ID},
		"no pipelineExecutionId": {"pipelineName": "owner"},
		"neither":                {},
	} {
		status, raw := call("GetPipelineExecution", req)
		assert.Equal(t, http.StatusBadRequest, status, "%s: %s", name, raw)
		assert.Equal(t, "ValidationException", cicdCode(t, raw), "%s: %s", name, raw)
	}

	status, body = call("GetPipelineExecution", map[string]any{"pipelineName": "owner", "pipelineExecutionId": started.ID})
	assert.Equal(t, http.StatusOK, status, "%s", body)
}

// DeletePipeline removes exactly the deleted pipeline's stored versions — not those of a pipeline whose
// name it prefixes — and leaves no snapshot behind.
func TestCodePipelineVersions_DeleteRemovesOnlyItsOwnHistory(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	call := func(op string, body any) (int, []byte) {
		return cicdCall(t, ts, cicdPipelineHost, cicdPipelineTgt, op, body)
	}
	for _, name := range []string{"a", "ab"} {
		status, body := call("CreatePipeline", cpVersionsPipeline(name, "arn:aws:iam::123456789012:role/v1"))
		require.Equal(t, http.StatusOK, status, "%s", body)
		status, body = call("UpdatePipeline", cpVersionsPipeline(name, "arn:aws:iam::123456789012:role/v2"))
		require.Equal(t, http.StatusOK, status, "%s", body)
	}
	status, body := call("DeletePipeline", map[string]any{"name": "a"})
	require.Equal(t, http.StatusOK, status, "%s", body)

	keys, err := ts.StateManager().List(t.Context(), "codepipeline", "pipeline_version:")
	require.NoError(t, err)
	assert.Len(t, keys, 2, "ab's two versions remain: %v", keys)
	for _, k := range keys {
		assert.Contains(t, k, "/ab/", "only ab's history remains")
	}
	status, body = call("GetPipeline", map[string]any{"name": "ab", "version": 1})
	assert.Equal(t, http.StatusOK, status, "%s", body)
}
