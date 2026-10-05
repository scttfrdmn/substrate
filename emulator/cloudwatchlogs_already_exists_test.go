package emulator_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCWLogs_ACreateOfAnExistingResourceIsRefusedAt400 is #1251: both creates answer
// ResourceAlreadyExistsException at the 400 API_CreateLogGroup and API_CreateLogStream publish, where
// they answered 409. Each case is built through published operations (#765), and each asserts the
// status and the __type code, per #1224's pattern, and that the refused create left the original in
// place.
func TestCWLogs_ACreateOfAnExistingResourceIsRefusedAt400(t *testing.T) {
	for _, tc := range []struct {
		name  string
		op    string
		body  map[string]any
		names string
	}{
		{"CreateLogGroup", "CreateLogGroup", map[string]any{"logGroupName": "/dup/group"}, "/dup/group"},
		{"CreateLogStream", "CreateLogStream", map[string]any{"logGroupName": "/dup/group", "logStreamName": "dup-stream"}, "dup-stream"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newCWLogsTestServer(t)
			cwLogsSeedGroupWithEvent(t, srv, "/dup/group", "dup-stream", "kept")

			resp := cwLogsRequest(t, srv, tc.op, tc.body)
			raw := cwLogsReadBody(t, resp)
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode,
				"%s publishes ResourceAlreadyExistsException at 400, not 409: %s", tc.op, raw)
			assert.Equal(t, "ResourceAlreadyExistsException", cwLogsErrorTypeFrom(t, raw))
			assert.Contains(t, string(raw), tc.names, "the message names the resource that collided")

			// The refused create changed nothing: the original stream still holds its event.
			resp = cwLogsRequest(t, srv, "GetLogEvents", map[string]any{"logGroupName": "/dup/group", "logStreamName": "dup-stream"})
			got := cwLogsReadBody(t, resp)
			require.Equal(t, http.StatusOK, resp.StatusCode, "GetLogEvents: %s", got)
			assert.Contains(t, string(got), "kept", "a refused %s must not reset the existing resource", tc.op)
		})
	}
}
