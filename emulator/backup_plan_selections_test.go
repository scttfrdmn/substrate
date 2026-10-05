package emulator_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// A backup plan cannot be deleted while it has selections, and a selection is unreachable once its
// plan is gone (#1178).
//
// API_DeleteBackupPlan: "A backup plan can only be deleted after all associated selections of
// resources have been deleted." DeleteBackupPlan used to delete the plan regardless, and
// GetBackupSelection kept answering 200 for the orphaned selections, reporting a plan ID no other
// operation would accept.

// backupPlanWithSelection creates a plan and one selection of it, returning the plan's path and the
// selection's ID.
func backupPlanWithSelection(t *testing.T, h *backupHarness, name string) (planPath, selectionID string) {
	t.Helper()
	created := h.ok(http.MethodPost, "/backup/plans", nil, map[string]any{"BackupPlan": map[string]any{"BackupPlanName": name}})
	var plan struct {
		BackupPlanID string `json:"BackupPlanId"`
	}
	require.NoError(t, json.Unmarshal(created, &plan), "decode CreateBackupPlan: %s", created)
	planPath = "/backup/plans/" + plan.BackupPlanID
	sel := h.ok(http.MethodPost, planPath+"/selections", nil, map[string]any{
		"BackupSelection": map[string]any{"SelectionName": name + "-sel", "IamRoleArn": "arn:aws:iam::123456789012:role/backup"},
	})
	var out struct {
		SelectionID string `json:"SelectionId"`
	}
	require.NoError(t, json.Unmarshal(sel, &out), "decode CreateBackupSelection: %s", sel)
	require.NotEmpty(t, out.SelectionID, "CreateBackupSelection must report a SelectionId: %s", sel)
	return planPath, out.SelectionID
}

func TestBackupPlan_ADeleteIsRefusedWhileItHasSelections(t *testing.T) {
	t.Parallel()
	h := newBackupHarness(t)
	planPath, selectionID := backupPlanWithSelection(t, h, "held")

	status, code, body, err := h.call(http.MethodDelete, planPath, nil, nil)
	require.NoError(t, err)
	require.Equal(t, "InvalidRequestException", code, "a plan with a selection must refuse deletion: %s", body)
	require.Equal(t, http.StatusBadRequest, status)

	// The refused delete changed nothing: the plan and its selection both still answer.
	h.ok(http.MethodGet, planPath, nil, nil)
	h.ok(http.MethodGet, planPath+"/selections/"+selectionID, nil, nil)
}

// The published teardown order: delete the selections, then the plan. Afterwards the selection is
// unreachable through its plan's path.
func TestBackupPlan_TheTeardownOrderSucceedsEndToEnd(t *testing.T) {
	t.Parallel()
	h := newBackupHarness(t)
	planPath, selectionID := backupPlanWithSelection(t, h, "teardown")

	h.ok(http.MethodDelete, planPath+"/selections/"+selectionID, nil, nil)
	deleted := h.ok(http.MethodDelete, planPath, nil, nil)
	require.Contains(t, string(deleted), `"BackupPlanId"`, "DeleteBackupPlan answers its published body: %s", deleted)

	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, planPath},
		{http.MethodGet, planPath + "/selections/" + selectionID},
	} {
		status, code, body, err := h.call(tc.method, tc.path, nil, nil)
		require.NoError(t, err)
		require.Equalf(t, "ResourceNotFoundException", code, "%s %s after teardown: %s", tc.method, tc.path, body)
		require.Equal(t, http.StatusBadRequest, status)
	}
}

// A selection orphaned by an older Substrate — its plan deleted while it still existed — must not
// answer 200 now. The plan record is removed directly, leaving the selection record behind, which is
// exactly the state such a Substrate wrote.
func TestBackupPlan_ASelectionWhosePlanIsGoneIsNotFound(t *testing.T) {
	t.Parallel()
	h := newBackupHarness(t)
	planPath, selectionID := backupPlanWithSelection(t, h, "orphan")
	planID := planPath[len("/backup/plans/"):]

	require.NoError(t, h.state.inner.Delete(t.Context(), "backup", "plan:"+h.ctx.AccountID+"/"+h.ctx.Region+"/"+planID))

	for _, method := range []string{http.MethodGet, http.MethodDelete} {
		status, code, body, err := h.call(method, planPath+"/selections/"+selectionID, nil, nil)
		require.NoError(t, err)
		require.Equalf(t, "ResourceNotFoundException", code, "%s on an orphaned selection: %s", method, body)
		require.Equal(t, http.StatusBadRequest, status)
	}
}

// A store fault while checking the plan's selections is an error, never the published refusal and
// never a delete.
func TestBackupPlan_ASelectionCheckFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
	}{
		{"selection index read", func(m *cfFaultStateManager) { m.failGet = "selection_ids:" }},
		{"selection record read", func(m *cfFaultStateManager) { m.failGet = "selection:" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newBackupHarness(t)
			planPath, _ := backupPlanWithSelection(t, h, "fault")
			tc.arm(h.state)
			_, code, _, err := h.call(http.MethodDelete, planPath, nil, nil)
			require.Error(t, err, "a store fault must be an error")
			require.Empty(t, code, "a store fault must not be answered as a published refusal")
		})
	}
}
