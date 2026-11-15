package emulator_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// SubmitJob requires its three members and a usable queue and definition, and deleting a queue
// fails its unfinished jobs (#1422).

// batchPoster posts one Batch REST request and answers the status and body, so the fixture helper
// below serves every way the Batch tests reach the plugin.
type batchPoster func(path string, body map[string]any) (int, []byte)

// batchHTTPPoster posts through an httptest server.
func batchHTTPPoster(t *testing.T, ts *httptest.Server) batchPoster {
	t.Helper()
	return func(path string, body map[string]any) (int, []byte) {
		resp := batchRequest(t, ts, http.MethodPost, path, body)
		return resp.StatusCode, batchBody(t, resp)
	}
}

// batchTestServerPoster posts through an emulator test server.
func batchTestServerPoster(t *testing.T, ts *emulator.TestServer) batchPoster {
	t.Helper()
	return func(path string, body map[string]any) (int, []byte) {
		return batchCall(t, ts, path, body)
	}
}

// batchPluginPoster posts straight to a plugin, answering a refusal's status and message.
func batchPluginPoster(t *testing.T, p *emulator.BatchPlugin, ctx *emulator.RequestContext) batchPoster {
	t.Helper()
	return func(path string, body map[string]any) (int, []byte) {
		raw, err := json.Marshal(body)
		require.NoError(t, err)
		resp, err := p.HandleRequest(ctx, &emulator.AWSRequest{
			Service: "batch", HTTPMethod: http.MethodPost, Path: path, Body: raw,
			Headers: map[string]string{"Content-Type": "application/json"}, Params: map[string]string{},
		})
		if err != nil {
			var awsErr *emulator.AWSError
			require.Truef(t, errors.As(err, &awsErr), "%s failed with a non-AWS error: %v", path, err)
			return awsErr.HTTPStatus, []byte(awsErr.Code + ": " + awsErr.Message)
		}
		return resp.StatusCode, resp.Body
	}
}

// batchEnsureSubmittable creates what a SubmitJob naming queue and definition needs: an ENABLED
// queue, and enough ACTIVE revisions of the definition that a named revision exists. Either may be a
// name or an ARN. It is idempotent, so a test may call it before every submission.
func batchEnsureSubmittable(t *testing.T, post batchPoster, queue, definition string) {
	t.Helper()
	queueName := queue[strings.LastIndexByte(queue, '/')+1:]
	status, body := post("/v1/describejobqueues", map[string]any{"jobQueues": []string{queueName}})
	require.Equalf(t, http.StatusOK, status, "DescribeJobQueues: %s", body)
	if !strings.Contains(string(body), `"jobQueueName":"`+queueName+`"`) {
		status, body = post("/v1/createjobqueue", map[string]any{
			"jobQueueName": queueName, "priority": 1, "state": "ENABLED",
			"computeEnvironmentOrder": []map[string]any{{"order": 1, "computeEnvironment": "ce"}},
		})
		require.Equalf(t, http.StatusOK, status, "CreateJobQueue %s: %s", queueName, body)
	}

	defName := definition[strings.LastIndexByte(definition, '/')+1:]
	want := 1
	if colon := strings.LastIndexByte(defName, ':'); colon >= 0 {
		n, err := strconv.Atoi(defName[colon+1:])
		require.NoErrorf(t, err, "definition %s names no revision", definition)
		defName, want = defName[:colon], n
	}
	status, body = post("/v1/describejobdefinitions", map[string]any{"jobDefinitionName": defName})
	require.Equalf(t, http.StatusOK, status, "DescribeJobDefinitions: %s", body)
	for have := strings.Count(string(body), `"revision":`); have < want; have++ {
		status, body = post("/v1/registerjobdefinition", map[string]any{
			"jobDefinitionName": defName, "type": "container",
			"containerProperties": map[string]any{"image": "busybox"},
		})
		require.Equalf(t, http.StatusOK, status, "RegisterJobDefinition %s: %s", defName, body)
	}
}

// batchSubmitStatus submits one job and answers the status and body.
func batchSubmitStatus(post batchPoster, queue, definition string) (int, string) {
	status, body := post("/v1/submitjob", map[string]any{"jobName": "j", "jobQueue": queue, "jobDefinition": definition})
	return status, string(body)
}

func TestBatchSubmit_RequiredMembersAreClientException(t *testing.T) {
	t.Parallel()
	ts := newBatchTestServer(t)
	post := batchHTTPPoster(t, ts)
	batchEnsureSubmittable(t, post, "q", "d")
	for _, missing := range []string{"jobName", "jobQueue", "jobDefinition"} {
		t.Run(missing, func(t *testing.T) {
			body := map[string]any{"jobName": "j", "jobQueue": "q", "jobDefinition": "d"}
			delete(body, missing)
			status, raw := post("/v1/submitjob", body)
			assert.Equal(t, http.StatusBadRequest, status, "%s", raw)
			assert.Contains(t, string(raw), "ClientException")
			assert.NotContains(t, string(raw), "MissingParameter", "Batch publishes no MissingParameter")
		})
	}
}

func TestBatchSubmit_TheQueueMustExistAndBeEnabled(t *testing.T) {
	t.Parallel()
	ts := newBatchTestServer(t)
	post := batchHTTPPoster(t, ts)
	batchEnsureSubmittable(t, post, "open", "d")
	batchEnsureSubmittable(t, post, "closed", "d")
	status, body := post("/v1/updatejobqueue", map[string]any{"jobQueue": "closed", "state": "DISABLED"})
	require.Equal(t, http.StatusOK, status, "%s", body)

	cases := []struct {
		name, queue string
		want        int
	}{
		{"an ENABLED queue by name", "open", http.StatusOK},
		{"an ENABLED queue by ARN", "arn:aws:batch:us-east-1:123456789012:job-queue/open", http.StatusOK},
		{"a queue that does not exist", "absent", http.StatusBadRequest},
		{"a DISABLED queue", "closed", http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, body := batchSubmitStatus(post, tc.queue, "d")
			assert.Equal(t, tc.want, status, body)
			if tc.want != http.StatusOK {
				assert.Contains(t, body, "ClientException")
			}
		})
	}
}

func TestBatchSubmit_TheDefinitionMustHaveAnActiveRevision(t *testing.T) {
	t.Parallel()
	ts := newBatchTestServer(t)
	post := batchHTTPPoster(t, ts)
	batchEnsureSubmittable(t, post, "q", "two:2")
	batchEnsureSubmittable(t, post, "q", "gone:1")
	// gone's only revision is deregistered, and two's second, so two resolves to its first.
	for _, def := range []string{"gone:1", "two:2"} {
		status, body := post("/v1/deregisterjobdefinition", map[string]any{"jobDefinition": def})
		require.Equal(t, http.StatusOK, status, "%s", body)
	}

	cases := []struct {
		name, definition string
		want             int
	}{
		{"a bare name resolves to its latest ACTIVE revision", "two", http.StatusOK},
		{"an ACTIVE revision", "two:1", http.StatusOK},
		{"an ARN with a revision", "arn:aws:batch:us-east-1:123456789012:job-definition/two:1", http.StatusOK},
		{"an ARN without a revision", "arn:aws:batch:us-east-1:123456789012:job-definition/two", http.StatusOK},
		{"an INACTIVE revision", "two:2", http.StatusBadRequest},
		{"a revision never registered", "two:9", http.StatusBadRequest},
		{"a name with no ACTIVE revision", "gone", http.StatusBadRequest},
		{"a name never registered", "absent", http.StatusBadRequest},
		{"a revision that is not a number", "two:latest", http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, body := batchSubmitStatus(post, "q", tc.definition)
			assert.Equal(t, tc.want, status, body)
			if tc.want != http.StatusOK {
				assert.Contains(t, body, "ClientException")
			}
		})
	}
}

func TestBatchSubmit_DeletingAQueueFailsItsUnfinishedJobs(t *testing.T) {
	t.Parallel()
	ts := batchProgressionServer(t)
	post := batchTestServerPoster(t, ts)
	batchEnsureSubmittable(t, post, "doomed", "d")
	batchEnsureSubmittable(t, post, "kept", "d")

	submit := func(queue string) string {
		status, body := post("/v1/submitjob", map[string]any{"jobName": "j", "jobQueue": queue, "jobDefinition": "d"})
		require.Equal(t, http.StatusOK, status, "%s", body)
		var out struct {
			JobID string `json:"jobId"`
		}
		require.NoError(t, json.Unmarshal(body, &out))
		return out.JobID
	}
	running := submit("doomed")
	byARN := submit("arn:aws:batch:us-east-1:123456789012:job-queue/doomed")
	finished := submit("doomed")
	elsewhere := submit("kept")

	// Every job but finished is seeded to hold RUNNING; finished is unseeded, so its first
	// observation reports SUCCEEDED, which the delete must leave alone.
	for _, id := range []string{running, byARN, elsewhere} {
		batchSeedJobStatus(t, ts, fmt.Sprintf(`{"jobId":%q,"transientObservations":{"RUNNING":100}}`, id))
	}
	status, reason := batchDescribeStatus(t, ts, finished)
	require.Equal(t, "SUCCEEDED", status, reason)

	code, body := post("/v1/updatejobqueue", map[string]any{"jobQueue": "doomed", "state": "DISABLED"})
	require.Equal(t, http.StatusOK, code, "%s", body)
	code, body = post("/v1/deletejobqueue", map[string]any{"jobQueue": "doomed"})
	require.Equal(t, http.StatusOK, code, "%s", body)

	for _, tc := range []struct {
		id, wantStatus, wantReason string
	}{
		{running, "FAILED", "Job terminated because its job queue was deleted."},
		{byARN, "FAILED", "Job terminated because its job queue was deleted."},
		{finished, "SUCCEEDED", ""},
		{elsewhere, "RUNNING", ""},
	} {
		status, reason := batchDescribeStatus(t, ts, tc.id)
		assert.Equal(t, tc.wantStatus, status, "job %s", tc.id)
		assert.Equal(t, tc.wantReason, reason, "job %s", tc.id)
	}

	// The queue is gone, so a further submission to it is refused.
	code, raw := batchSubmitStatus(post, "doomed", "d")
	assert.Equal(t, http.StatusBadRequest, code, raw)
}
