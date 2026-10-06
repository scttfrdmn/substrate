package emulator_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// The two Backup plan creates are routed on the verb their pages publish (#1172), and
// ListBackupSelections is routed so a teardown can find the selections DeleteBackupPlan requires it
// to delete first (#1408).
//
// API_CreateBackupPlan publishes `PUT /backup/plans/`, API_CreateBackupSelection
// `PUT /backup/plans/{backupPlanId}/selections/`, API_UpdateBackupPlan
// `POST /backup/plans/{backupPlanId}`, and API_ListBackupSelections
// `GET /backup/plans/{backupPlanId}/selections/?maxResults=…&nextToken=…`.

// backupListedSelections decodes one ListBackupSelections page.
type backupListedSelections struct {
	BackupSelectionsList []map[string]json.RawMessage `json:"BackupSelectionsList"`
	NextToken            string                       `json:"NextToken"`
}

func TestBackupRouting_EachPlanOperationAnswersOnItsPublishedVerb(t *testing.T) {
	t.Parallel()
	h := newBackupHarness(t)
	planPath, selectionID := backupPlanWithSelection(t, h, "verbs")

	for _, tc := range []struct {
		name, method, path string
		body               map[string]any
		wantCode           string
	}{
		{"CreateBackupPlan on PUT", http.MethodPut, "/backup/plans/",
			map[string]any{"BackupPlan": map[string]any{"BackupPlanName": "via-put"}}, ""},
		{"CreateBackupSelection on PUT", http.MethodPut, planPath + "/selections/",
			map[string]any{"BackupSelection": map[string]any{"SelectionName": "via-put"}}, ""},
		{"UpdateBackupPlan on POST", http.MethodPost, planPath,
			map[string]any{"BackupPlan": map[string]any{"BackupPlanName": "renamed"}}, ""},
		{"ListBackupSelections on GET", http.MethodGet, planPath + "/selections/", nil, ""},
		{"GetBackupSelection on GET", http.MethodGet, planPath + "/selections/" + selectionID, nil, ""},
		// The POST spellings the router used to accept are not published, so they are unknown routes.
		{"POST on the plan collection", http.MethodPost, "/backup/plans/",
			map[string]any{"BackupPlan": map[string]any{"BackupPlanName": "via-post"}}, "UnknownOperationException"},
		{"POST on the selection collection", http.MethodPost, planPath + "/selections/",
			map[string]any{"BackupSelection": map[string]any{"SelectionName": "via-post"}}, "UnknownOperationException"},
		{"PUT on a plan", http.MethodPut, planPath,
			map[string]any{"BackupPlan": map[string]any{"BackupPlanName": "via-put"}}, "UnknownOperationException"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code, body, err := h.call(tc.method, tc.path, nil, tc.body)
			require.NoError(t, err)
			require.Equal(t, tc.wantCode, code, "%s %s: %s", tc.method, tc.path, body)
			if tc.wantCode == "" {
				require.Equal(t, http.StatusOK, status)
			}
		})
	}
}

func TestBackupListSelections_AnswersThePublishedMembers(t *testing.T) {
	t.Parallel()
	h := newBackupHarness(t)
	created := h.ok(http.MethodPut, "/backup/plans/", nil, map[string]any{"BackupPlan": map[string]any{"BackupPlanName": "listed"}})
	planID := backupRefusalsMember(t, created, "BackupPlanId")
	selections := "/backup/plans/" + planID + "/selections/"
	sel := h.ok(http.MethodPut, selections, nil, map[string]any{
		"BackupSelection":  map[string]any{"SelectionName": "with-role", "IamRoleArn": "arn:aws:iam::123456789012:role/backup"},
		"CreatorRequestId": "req-sel-1",
	})
	selectionID := backupRefusalsMember(t, sel, "SelectionId")

	var page backupListedSelections
	raw := h.ok(http.MethodGet, selections, nil, nil)
	require.NoError(t, json.Unmarshal(raw, &page), "%s", raw)
	require.Len(t, page.BackupSelectionsList, 1)
	assert.Empty(t, page.NextToken, "one page holds every selection")

	member := page.BackupSelectionsList[0]
	assert.JSONEq(t, `"`+selectionID+`"`, string(member["SelectionId"]))
	assert.JSONEq(t, `"with-role"`, string(member["SelectionName"]))
	assert.JSONEq(t, `"`+planID+`"`, string(member["BackupPlanId"]))
	assert.JSONEq(t, `"arn:aws:iam::123456789012:role/backup"`, string(member["IamRoleArn"]))
	assert.JSONEq(t, `"req-sel-1"`, string(member["CreatorRequestId"]))
	assert.Equal(t, "1700000000.000", string(member["CreationDate"]), "CreationDate is a Unix timestamp")
	published := map[string]bool{
		"BackupPlanId": true, "CreationDate": true, "CreatorRequestId": true,
		"IamRoleArn": true, "SelectionId": true, "SelectionName": true,
	}
	for name := range member {
		assert.Truef(t, published[name], "%s is not a BackupSelectionsListMember member", name)
	}
}

func TestBackupListSelections_PagesByMaxResults(t *testing.T) {
	t.Parallel()
	h := newBackupHarness(t)
	created := h.ok(http.MethodPut, "/backup/plans/", nil, map[string]any{"BackupPlan": map[string]any{"BackupPlanName": "paged"}})
	selections := "/backup/plans/" + backupRefusalsMember(t, created, "BackupPlanId") + "/selections/"
	for _, name := range []string{"s1", "s2", "s3"} {
		h.ok(http.MethodPut, selections, nil, map[string]any{"BackupSelection": map[string]any{"SelectionName": name}})
	}

	seen := map[string]bool{}
	params := map[string]string{"maxResults": "2"}
	for pages := 1; ; pages++ {
		require.LessOrEqual(t, pages, 2, "three selections at two a page is two pages")
		var page backupListedSelections
		raw := h.ok(http.MethodGet, selections, params, nil)
		require.NoError(t, json.Unmarshal(raw, &page), "%s", raw)
		require.LessOrEqual(t, len(page.BackupSelectionsList), 2)
		for _, m := range page.BackupSelectionsList {
			var name string
			require.NoError(t, json.Unmarshal(m["SelectionName"], &name))
			require.False(t, seen[name], "%s listed twice", name)
			seen[name] = true
		}
		if page.NextToken == "" {
			break
		}
		params = map[string]string{"maxResults": "2", "nextToken": page.NextToken}
	}
	assert.Len(t, seen, 3)
}

func TestBackupListSelections_Refusals(t *testing.T) {
	t.Parallel()
	h := newBackupHarness(t)
	created := h.ok(http.MethodPut, "/backup/plans/", nil, map[string]any{"BackupPlan": map[string]any{"BackupPlanName": "refused"}})
	selections := "/backup/plans/" + backupRefusalsMember(t, created, "BackupPlanId") + "/selections/"

	for _, tc := range []struct {
		name, path string
		params     map[string]string
		code       string
	}{
		{"unknown plan", "/backup/plans/no-such-plan/selections/", nil, "ResourceNotFoundException"},
		{"maxResults 0", selections, map[string]string{"maxResults": "0"}, "InvalidParameterValueException"},
		{"maxResults 1001", selections, map[string]string{"maxResults": "1001"}, "InvalidParameterValueException"},
		{"maxResults not a number", selections, map[string]string{"maxResults": "ten"}, "InvalidParameterValueException"},
		{"unreadable nextToken", selections, map[string]string{"nextToken": "!!"}, "InvalidParameterValueException"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code, _, err := h.call(http.MethodGet, tc.path, tc.params, nil)
			require.NoError(t, err)
			assert.Equal(t, tc.code, code)
			assert.Equal(t, http.StatusBadRequest, status, "every ListBackupSelections refusal is published at 400")
		})
	}
}

// A teardown that recorded no selection IDs lists them, deletes each, then deletes the plan — the
// order API_DeleteBackupPlan requires — through the published verbs on a running server.
func TestBackupListSelections_ATeardownFindsAndDeletesEverySelectionEndToEnd(t *testing.T) {
	ts := emulator.StartTestServer(t)
	const host = "backup.us-east-1.amazonaws.com"

	status, raw := rawUnsignedRawBody(t, ts, host, "/backup/plans/", http.MethodPut,
		[]byte(`{"BackupPlan":{"BackupPlanName":"e2e-teardown"}}`))
	require.Equal(t, http.StatusOK, status, "CreateBackupPlan: %s", raw)
	planPath := "/backup/plans/" + jsonMemberAnyDepth(t, raw, "BackupPlanId")
	for _, name := range []string{"a", "b"} {
		status, raw = rawUnsignedRawBody(t, ts, host, planPath+"/selections/", http.MethodPut,
			[]byte(`{"BackupSelection":{"SelectionName":"`+name+`"}}`))
		require.Equal(t, http.StatusOK, status, "CreateBackupSelection %s: %s", name, raw)
	}

	status, raw = rawUnsignedRawBody(t, ts, host, planPath+"/selections/", http.MethodGet, nil)
	require.Equal(t, http.StatusOK, status, "ListBackupSelections: %s", raw)
	var page backupListedSelections
	require.NoError(t, json.Unmarshal([]byte(raw), &page), "%s", raw)
	require.Len(t, page.BackupSelectionsList, 2)
	for _, m := range page.BackupSelectionsList {
		var id string
		require.NoError(t, json.Unmarshal(m["SelectionId"], &id))
		status, raw = rawUnsignedRawBody(t, ts, host, planPath+"/selections/"+id, http.MethodDelete, nil)
		require.Equal(t, http.StatusOK, status, "DeleteBackupSelection %s: %s", id, raw)
	}

	status, raw = rawUnsignedRawBody(t, ts, host, planPath, http.MethodDelete, nil)
	require.Equal(t, http.StatusOK, status, "DeleteBackupPlan after its selections: %s", raw)
}
