package emulator_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// QuickSight binds Status to the HTTP status line: every Response Syntax opens with HTTP/1.1 {Status},
// and no Response Elements list carries Status in the body (#1179). The raw body is read, because an
// SDK overwrites the member from the status line and would hide one written into the JSON.
func TestQuickSight_StatusIsBoundToTheStatusLineNotTheBody(t *testing.T) {
	ts := newQuickSightTestServer(t)
	qsBody(t, qsRequest(t, ts, http.MethodPost, "/accounts/"+qsAccountID+"/data-sources",
		map[string]string{"DataSourceId": "ds-status", "Name": "S", "Type": "ATHENA"}))
	var created struct {
		IngestionID string `json:"IngestionId"`
	}
	require.NoError(t, json.Unmarshal(qsBody(t, qsRequest(t, ts, http.MethodPost, "/accounts/"+qsAccountID+"/data-sets",
		map[string]string{"DataSetId": "set-status", "Name": "S"})), &created))
	require.NotEmpty(t, created.IngestionID)

	for name, path := range map[string]string{
		"DescribeDataSource": "/accounts/" + qsAccountID + "/data-sources/ds-status",
		"DescribeIngestion":  "/accounts/" + qsAccountID + "/data-sets/set-status/ingestions/" + created.IngestionID,
	} {
		t.Run(name, func(t *testing.T) {
			resp := qsRequest(t, ts, http.MethodGet, path, nil)
			body := qsBody(t, resp)
			require.Equal(t, http.StatusOK, resp.StatusCode, "%s", body)
			var members map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(body, &members))
			assert.NotContains(t, members, "Status", "Status is the status line, not a body member: %s", body)
			assert.Contains(t, members, "RequestId")
		})
	}
}
