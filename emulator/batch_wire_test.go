package emulator_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// TestBatchWire_JobResponsesCarryNoBookkeepingMember is the raw-bytes assertion
// scripts/wire-bookkeeping-projected.txt cites for BatchJob (#756).
//
// BatchJob declares its account and Region under wire-visible `json` tags, never under omitempty, so
// a record that exists holds both and no absence below is vacuous. DescribeJobs used to answer the
// record whole; it answers emulator/batch_wire.go's projection now. SubmitJob, ListJobs and
// TerminateJob build their own shapes and are driven anyway.
func TestBatchWire_JobResponsesCarryNoBookkeepingMember(t *testing.T) {
	t.Parallel()
	p := &emulator.BatchPlugin{}
	ctx, state := wireSetup(t, p, "req-batch-wire")
	call := func(path string, body map[string]any) []byte {
		return wireREST(t, p, ctx, "batch", http.MethodPost, path, body)
	}

	submitted := call("/v1/submitjob", map[string]any{"jobName": "wire-job", "jobQueue": "wire-queue", "jobDefinition": "wire-def"})
	var job struct {
		JobID string `json:"jobId"`
	}
	require.NoError(t, json.Unmarshal(submitted, &job), "decode SubmitJob: %s", submitted)
	require.NotEmpty(t, job.JobID, "SubmitJob must report a job ID: %s", submitted)
	wireRequireHeld(t, state, "batch", "job:123456789012/us-east-1/"+job.JobID, "accountID", "region")

	described := call("/v1/describejobs", map[string]any{"jobs": []string{job.JobID}})
	wireRunJSON(t, []string{"AccountID", "Region"}, []wireCase{
		{op: "SubmitJob", held: submitted, anchor: `"jobId":"` + job.JobID + `"`},
		{op: "DescribeJobs", held: described, anchor: `"jobId":"` + job.JobID + `"`},
		{op: "ListJobs", anchor: `"jobId":"` + job.JobID + `"`, call: func() []byte {
			return call("/v1/listjobs", map[string]any{"jobQueue": "wire-queue", "jobStatus": "SUCCEEDED"})
		}},
		{op: "TerminateJob", anchor: "{}", call: func() []byte {
			return call("/v1/terminatejob", map[string]any{"jobId": job.JobID, "reason": "wire"})
		}},
	})

	// After the walk. API_JobDetail's createdAt is a Long of epoch milliseconds, and its jobArn is the
	// one SubmitJob answered.
	var out struct {
		Jobs []map[string]json.RawMessage `json:"jobs"`
	}
	require.NoError(t, json.Unmarshal(described, &out), "decode DescribeJobs: %s", described)
	require.Len(t, out.Jobs, 1, "DescribeJobs: %s", described)
	require.Equal(t,
		[]string{"createdAt", "jobArn", "jobDefinition", "jobId", "jobName", "jobQueue", "status"},
		slices.Sorted(maps.Keys(out.Jobs[0])), "DescribeJobs must answer only API_JobDetail's members the record models: %s", described)
	require.JSONEq(t, "1700000000000", string(out.Jobs[0]["createdAt"]), "createdAt is epoch milliseconds: %s", described)
	require.JSONEq(t, `"arn:aws:batch:us-east-1:123456789012:job/`+job.JobID+`"`, string(out.Jobs[0]["jobArn"]), "jobArn: %s", described)
}
