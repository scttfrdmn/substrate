package emulator_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests in this file are #555's gate: a Batch resource can be taken out of service and
// changed, not only created and described.

// TestBatchLifecycle_Deregister asserts the assertion #530 could not make: a deregistered
// revision is reported INACTIVE, selected by status INACTIVE and not by ACTIVE, and still
// describable by name.
func TestBatchLifecycle_Deregister(t *testing.T) {
	ts := newBatchTestServer(t)
	for range 2 {
		code, body := batchPost(t, ts, "/v1/registerjobdefinition", map[string]interface{}{
			"jobDefinitionName":   "jd",
			"type":                "container",
			"containerProperties": map[string]interface{}{"image": "busybox"},
		})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
	}

	code, body := batchPost(t, ts, "/v1/deregisterjobdefinition", map[string]interface{}{"jobDefinition": "jd:1"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.JSONEq(t, `{}`, body, "the page's sample response is {}")

	code, body = batchPost(t, ts, "/v1/describejobdefinitions", map[string]interface{}{"status": "INACTIVE"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	inactive := batchMembers(t, body, "jobDefinitions")
	require.Len(t, inactive, 1)
	assert.EqualValues(t, 1, inactive[0]["revision"])
	assert.Equal(t, "INACTIVE", inactive[0]["status"])

	code, body = batchPost(t, ts, "/v1/describejobdefinitions", map[string]interface{}{"status": "ACTIVE"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	active := batchMembers(t, body, "jobDefinitions")
	require.Len(t, active, 1)
	assert.EqualValues(t, 2, active[0]["revision"])

	// The deregistered revision stays describable by bare name: it is kept, not deleted.
	code, body = batchPost(t, ts, "/v1/describejobdefinitions", map[string]interface{}{"jobDefinitionName": "jd"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Len(t, batchMembers(t, body, "jobDefinitions"), 2)

	// By ARN as well as by name:revision.
	code, body = batchPost(t, ts, "/v1/deregisterjobdefinition", map[string]interface{}{
		"jobDefinition": "arn:aws:batch:us-east-1:123456789012:job-definition/jd:2",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	code, body = batchPost(t, ts, "/v1/describejobdefinitions", map[string]interface{}{"status": "ACTIVE"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Empty(t, batchMembers(t, body, "jobDefinitions"))
}

// TestBatchLifecycle_Refusals covers every ClientException the five operations raise. Each
// is the one error code Batch publishes for a client fault.
func TestBatchLifecycle_Refusals(t *testing.T) {
	ts := newBatchTestServer(t)
	code, body := batchPost(t, ts, "/v1/registerjobdefinition", map[string]interface{}{
		"jobDefinitionName": "jd", "type": "container",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	code, body = batchPost(t, ts, "/v1/createcomputeenvironment", map[string]interface{}{
		"computeEnvironmentName": "ce", "type": "UNMANAGED",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	code, body = batchPost(t, ts, "/v1/createjobqueue", map[string]interface{}{
		"jobQueueName": "q", "priority": 1,
		"computeEnvironmentOrder": []map[string]interface{}{{"order": 1, "computeEnvironment": "ce"}},
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	cases := []struct {
		name string
		path string
		body map[string]interface{}
		want string
	}{
		{"deregister without jobDefinition", "/v1/deregisterjobdefinition", map[string]interface{}{}, "jobDefinition is required"},
		{"deregister a bare name", "/v1/deregisterjobdefinition", map[string]interface{}{"jobDefinition": "jd"}, "name:revision"},
		{"deregister an absent revision", "/v1/deregisterjobdefinition", map[string]interface{}{"jobDefinition": "jd:9"}, "does not exist"},
		{"update an absent queue", "/v1/updatejobqueue", map[string]interface{}{"jobQueue": "absent"}, "does not exist"},
		{"update a queue without jobQueue", "/v1/updatejobqueue", map[string]interface{}{"state": "DISABLED"}, "jobQueue is required"},
		{"update a queue to an unpublished state", "/v1/updatejobqueue", map[string]interface{}{"jobQueue": "q", "state": "PAUSED"}, "ENABLED | DISABLED"},
		{"update an absent environment", "/v1/updatecomputeenvironment", map[string]interface{}{"computeEnvironment": "absent"}, "does not exist"},
		{"update an environment to an unpublished state", "/v1/updatecomputeenvironment", map[string]interface{}{"computeEnvironment": "ce", "state": "off"}, "ENABLED | DISABLED"},
		{"delete an enabled queue", "/v1/deletejobqueue", map[string]interface{}{"jobQueue": "q"}, "must be DISABLED"},
		{"delete an absent queue", "/v1/deletejobqueue", map[string]interface{}{"jobQueue": "absent"}, "does not exist"},
		{"delete an enabled environment", "/v1/deletecomputeenvironment", map[string]interface{}{"computeEnvironment": "ce"}, "must be DISABLED"},
		{"delete an absent environment", "/v1/deletecomputeenvironment", map[string]interface{}{"computeEnvironment": "absent"}, "does not exist"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, body := batchPost(t, ts, tc.path, tc.body)
			assert.Equal(t, http.StatusBadRequest, code, "body was %s", body)
			assert.Contains(t, body, "ClientException")
			assert.Contains(t, body, tc.want)
		})
	}
}

// TestBatchLifecycle_Teardown is the documented teardown order: disable the queue and the
// environment, disassociate the environment, then delete both. Each precondition is asserted
// as refused until it is met.
func TestBatchLifecycle_Teardown(t *testing.T) {
	ts := newBatchTestServer(t)
	code, body := batchPost(t, ts, "/v1/createcomputeenvironment", map[string]interface{}{
		"computeEnvironmentName": "ce", "type": "MANAGED",
		"computeResources": map[string]interface{}{"type": "EC2", "maxvCpus": 16, "minvCpus": 0},
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	code, body = batchPost(t, ts, "/v1/createjobqueue", map[string]interface{}{
		"jobQueueName": "q", "priority": 1,
		"computeEnvironmentOrder": []map[string]interface{}{
			{"order": 1, "computeEnvironment": "arn:aws:batch:us-east-1:123456789012:compute-environment/ce"},
		},
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	// Disable the environment; its compute resources merge rather than being replaced.
	code, body = batchPost(t, ts, "/v1/updatecomputeenvironment", map[string]interface{}{
		"computeEnvironment": "ce", "state": "DISABLED",
		"computeResources": map[string]interface{}{"maxvCpus": 32},
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, `"computeEnvironmentName":"ce"`)
	assert.Contains(t, body, `"computeEnvironmentArn":"arn:aws:batch:us-east-1:`)
	code, body = batchPost(t, ts, "/v1/describecomputeenvironments", map[string]interface{}{})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, `"state":"DISABLED"`)
	assert.Contains(t, body, `"maxvCpus":32`)
	assert.Contains(t, body, `"type":"EC2"`, "an update names only what changes")

	// Still associated with q, by ARN, so the delete is refused.
	code, body = batchPost(t, ts, "/v1/deletecomputeenvironment", map[string]interface{}{"computeEnvironment": "ce"})
	assert.Equal(t, http.StatusBadRequest, code, "body was %s", body)
	assert.Contains(t, body, "associated with job queue q")

	// Disable the queue and disassociate the environment in one update.
	code, body = batchPost(t, ts, "/v1/updatejobqueue", map[string]interface{}{
		"jobQueue": "q", "state": "DISABLED", "priority": 5,
		"computeEnvironmentOrder": []map[string]interface{}{},
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, `"jobQueueName":"q"`)
	code, body = batchPost(t, ts, "/v1/describejobqueues", map[string]interface{}{})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.Contains(t, body, `"state":"DISABLED"`)
	assert.Contains(t, body, `"priority":5`)

	code, body = batchPost(t, ts, "/v1/deletecomputeenvironment", map[string]interface{}{"computeEnvironment": "ce"})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.JSONEq(t, `{}`, body)
	code, body = batchPost(t, ts, "/v1/deletejobqueue", map[string]interface{}{
		"jobQueue": "arn:aws:batch:us-east-1:123456789012:job-queue/q",
	})
	require.Equal(t, http.StatusOK, code, "body was %s", body)
	assert.JSONEq(t, `{}`, body)

	for path, member := range map[string]string{
		"/v1/describecomputeenvironments": "computeEnvironments",
		"/v1/describejobqueues":           "jobQueues",
	} {
		code, body = batchPost(t, ts, path, map[string]interface{}{})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
		assert.Empty(t, batchMembers(t, body, member), "%s still reports a deleted resource", path)
	}

	// Deleting again is an identifier that is no longer valid.
	code, body = batchPost(t, ts, "/v1/deletejobqueue", map[string]interface{}{"jobQueue": "q"})
	assert.Equal(t, http.StatusBadRequest, code, "body was %s", body)
}

// TestBatchLifecycle_ReEnable moves a queue DISABLED and back, the ENABLED↔DISABLED pair the
// deletes depend on, and checks an absent member leaves its value alone.
func TestBatchLifecycle_ReEnable(t *testing.T) {
	ts := newBatchTestServer(t)
	code, body := batchPost(t, ts, "/v1/createjobqueue", map[string]interface{}{"jobQueueName": "q", "priority": 7})
	require.Equal(t, http.StatusOK, code, "body was %s", body)

	for _, state := range []string{"DISABLED", "ENABLED"} {
		code, body = batchPost(t, ts, "/v1/updatejobqueue", map[string]interface{}{"jobQueue": "q", "state": state})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
		code, body = batchPost(t, ts, "/v1/describejobqueues", map[string]interface{}{"jobQueues": []string{"q"}})
		require.Equal(t, http.StatusOK, code, "body was %s", body)
		queues := batchMembers(t, body, "jobQueues")
		require.Len(t, queues, 1)
		assert.Equal(t, state, queues[0]["state"])
		assert.EqualValues(t, 7, queues[0]["priority"], "an absent priority leaves it unchanged")
	}
}
