package emulator_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// jsonErrorType reads the error code out of an AWS JSON-protocol refusal.
//
// botocore reads the body's `__type`, so that is what an assertion has to check: a code that reached
// only the status line or a header would be invisible to the caller. The prefix some services qualify
// the type with is stripped, as the CloudWatch Logs helper does.
//
// Named for the protocol rather than for Glue because #1098's WAFv2 half reads the same field the same
// way; [cwLogsErrorType] predates it and stays where it is.
func jsonErrorType(t *testing.T, raw []byte) string {
	t.Helper()
	var body map[string]any
	require.NoError(t, json.Unmarshal(raw, &body), string(raw))
	code, _ := body["__type"].(string)
	for i := len(code) - 1; i >= 0; i-- {
		if code[i] == '#' {
			return code[i+1:]
		}
	}
	return code
}

// TestGlue_EntityNotFoundAnswersTheStatusItsPagesPublish is #1098's Glue half, asserted over the wire.
//
// All twelve sites answered 404. Glue publishes `EntityNotFoundException` at **400** on every page that
// lists it, and publishes no 404 anywhere, so a consumer branching on status rather than on the code saw
// a shape AWS never sends — #910 is the precedent for both the correction and the reasoning.
//
// Every condition is driven through the server rather than through HandleRequest, because the status is
// exactly what the plugin-level tests do not pin: that is how twelve sites drifted together.
func TestGlue_EntityNotFoundAnswersTheStatusItsPagesPublish(t *testing.T) {
	ts := newGlueTestServer(t)

	for _, tc := range []struct {
		op     string
		body   map[string]any
		entity string
		name   string
	}{
		{"GetDatabase", map[string]any{"Name": "no-db"}, "Database", "no-db"},
		{"UpdateDatabase", map[string]any{
			"Name": "no-db", "DatabaseInput": map[string]any{"Description": "d"},
		}, "Database", "no-db"},
		{"GetTable", map[string]any{"DatabaseName": "no-db", "Name": "no-table"}, "Table", "no-table"},
		{"UpdateTable", map[string]any{
			"DatabaseName": "no-db", "TableInput": map[string]any{"Name": "no-table"},
		}, "Table", "no-table"},
		{"GetConnection", map[string]any{"Name": "no-conn"}, "Connection", "no-conn"},
		{"UpdateConnection", map[string]any{
			"Name": "no-conn", "ConnectionInput": map[string]any{"Description": "d"},
		}, "Connection", "no-conn"},
		{"GetCrawler", map[string]any{"Name": "no-crawler"}, "Crawler", "no-crawler"},
		{"UpdateCrawler", map[string]any{"Name": "no-crawler", "Role": "r"}, "Crawler", "no-crawler"},
		{"GetJob", map[string]any{"JobName": "no-job"}, "Job", "no-job"},
		{"UpdateJob", map[string]any{
			"JobName": "no-job", "JobUpdate": map[string]any{"Description": "d"},
		}, "Job", "no-job"},
		{"StartJobRun", map[string]any{"JobName": "no-job"}, "Job", "no-job"},
		{"GetJobRun", map[string]any{"JobName": "no-job", "RunId": "no-run"}, "Job run", "no-run"},
	} {
		t.Run(tc.op, func(t *testing.T) {
			resp := glueRequest(t, ts, tc.op, tc.body)
			raw := glueBody(t, resp)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode,
				"Glue publishes EntityNotFoundException at 400, not 404")
			assert.Equal(t, "EntityNotFoundException", jsonErrorType(t, raw))
			assert.Contains(t, string(raw), tc.entity+" "+tc.name+" not found.",
				"the message names which entity and which name, since the published gloss names neither")
		})
	}
}

// TestGlue_ExistingEntitiesStillRead is the other half: the correction moved a status and nothing else,
// so an entity that does exist is still answered rather than refused.
func TestGlue_ExistingEntitiesStillRead(t *testing.T) {
	ts := newGlueTestServer(t)

	resp := glueRequest(t, ts, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "present-db"},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_ = glueBody(t, resp)

	resp = glueRequest(t, ts, "GetDatabase", map[string]any{"Name": "present-db"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(glueBody(t, resp)), "present-db")
}
