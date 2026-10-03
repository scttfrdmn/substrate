package emulator_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestBedrockWire_InvocationJobResponsesCarryNoBookkeepingMember is the raw-bytes assertion
// scripts/wire-bookkeeping-projected.txt cites for BedrockModelInvocationJob (#756).
//
// BedrockModelInvocationJob declares accountID and region under wire-visible `json` tags, never under
// omitempty, so a record that exists holds both and no absence below is vacuous. GetModelInvocationJob
// used to answer the record whole; it and ListModelInvocationJobs answer emulator/bedrock_wire.go's
// projection now. All four routed job operations are driven.
func TestBedrockWire_InvocationJobResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p := &emulator.BedrockRuntimePlugin{}
	ctx, state := wireSetup(t, p, "req-bedrock-wire")
	call := func(method, path string, body map[string]any) []byte {
		return wireREST(t, p, ctx, "bedrock", method, path, body)
	}

	created := call(http.MethodPost, "/model-invocation-job", map[string]any{
		"jobName": "wire-job", "modelId": "anthropic.claude-3-sonnet",
		"roleArn":          "arn:aws:iam::123456789012:role/wire",
		"inputDataConfig":  map[string]any{"s3InputDataConfig": map[string]string{"s3Uri": "s3://wire-in/"}},
		"outputDataConfig": map[string]any{"s3OutputDataConfig": map[string]string{"s3Uri": "s3://wire-out/"}},
	})
	var out struct {
		JobArn string `json:"jobArn"`
	}
	require.NoError(t, json.Unmarshal(created, &out), "decode CreateModelInvocationJob: %s", created)
	jobID := out.JobArn[strings.LastIndexByte(out.JobArn, '/')+1:]
	wireRequireHeld(t, state, "bedrock-runtime", "job:123456789012/us-east-1/"+jobID, "accountID", "region")

	job := "/model-invocation-job/" + jobID
	got := call(http.MethodGet, job, nil)
	listed := call(http.MethodGet, "/model-invocation-jobs", nil)
	wireRunJSON(t, []string{"AccountID", "Region"}, []wireCase{
		{op: "CreateModelInvocationJob", held: created, anchor: `"jobArn":"` + out.JobArn + `"`},
		{op: "GetModelInvocationJob", held: got, anchor: `"jobName":"wire-job"`},
		{op: "ListModelInvocationJobs", held: listed, anchor: `"jobName":"wire-job"`},
		{op: "StopModelInvocationJob", call: func() []byte { return call(http.MethodPost, job+"/stop", nil) }, anchor: "{}"},
	})

	// After the walk. Bedrock publishes submitTime as a date-time string, not the record's epoch
	// float, and the clock is frozen at 1700000000. The summary carries the members
	// API_ModelInvocationJobSummary marks Required, which the list used to drop.
	require.Contains(t, string(got), `"submitTime":"2023-11-14T22:13:20.000Z"`, "GetModelInvocationJob: %s", got)
	var list struct {
		Summaries []map[string]json.RawMessage `json:"invocationJobSummaries"`
	}
	require.NoError(t, json.Unmarshal(listed, &list), "decode ListModelInvocationJobs: %s", listed)
	require.Len(t, list.Summaries, 1, "ListModelInvocationJobs: %s", listed)
	require.JSONEq(t, `"2023-11-14T22:13:20.000Z"`, string(list.Summaries[0]["submitTime"]), "ListModelInvocationJobs: %s", listed)
	for _, member := range []string{"jobArn", "jobName", "modelId", "roleArn", "inputDataConfig", "outputDataConfig"} {
		require.NotEmptyf(t, list.Summaries[0][member], "ModelInvocationJobSummary requires %s: %s", member, listed)
	}
}
