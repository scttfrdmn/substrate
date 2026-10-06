package emulator_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// AWS Backup refuses with the codes and statuses its pages publish (#1390).
//
// Every routed operation's page — CreateBackupVault, DescribeBackupVault, DeleteBackupVault,
// ListBackupVaults, CreateBackupPlan, GetBackupPlan, UpdateBackupPlan, DeleteBackupPlan,
// ListBackupPlans, CreateBackupSelection, GetBackupSelection and DeleteBackupSelection, each fetched
// for this change — publishes ResourceNotFoundException and MissingParameterValueException, both at
// HTTP 400. Substrate answered a missing resource at 404 and a missing member as
// InvalidRequestException, which only the two plan and vault delete pages list, glossed as input of
// the wrong type rather than input that is absent.

func TestBackupRefusals_AnswerThePublishedCodeAndStatus(t *testing.T) {
	t.Parallel()
	h := newBackupHarness(t)
	h.ok(http.MethodPut, "/backup-vaults/refusal-vault", nil, nil)
	created := h.ok(http.MethodPut, "/backup/plans", nil, map[string]any{"BackupPlan": map[string]any{"BackupPlanName": "refusal-plan"}})
	var plan struct {
		BackupPlanID string `json:"BackupPlanId"`
	}
	require.NoError(t, json.Unmarshal(created, &plan), "decode CreateBackupPlan: %s", created)
	planPath := "/backup/plans/" + plan.BackupPlanID

	const (
		notFound = "ResourceNotFoundException"
		missing  = "MissingParameterValueException"
	)
	for _, tc := range []struct {
		name   string
		method string
		path   string
		body   map[string]any
		code   string
	}{
		// A resource that does not exist, on every operation that reads one.
		{"DescribeBackupVault/absent", http.MethodGet, "/backup-vaults/no-such-vault", nil, notFound},
		{"DeleteBackupVault/absent", http.MethodDelete, "/backup-vaults/no-such-vault", nil, notFound},
		{"GetBackupPlan/absent", http.MethodGet, "/backup/plans/no-such-plan", nil, notFound},
		{"UpdateBackupPlan/absent", http.MethodPost, "/backup/plans/no-such-plan",
			map[string]any{"BackupPlan": map[string]any{"BackupPlanName": "x"}}, notFound},
		{"DeleteBackupPlan/absent", http.MethodDelete, "/backup/plans/no-such-plan", nil, notFound},
		{"CreateBackupSelection/absent plan", http.MethodPut, "/backup/plans/no-such-plan/selections",
			map[string]any{"BackupSelection": map[string]any{"SelectionName": "s"}}, notFound},
		{"GetBackupSelection/absent", http.MethodGet, planPath + "/selections/no-such-selection", nil, notFound},
		{"DeleteBackupSelection/absent", http.MethodDelete, planPath + "/selections/no-such-selection", nil, notFound},

		// A required member that is absent.
		{"CreateBackupVault/no name", http.MethodPut, "/backup-vaults", nil, missing},
		{"CreateBackupPlan/no BackupPlanName", http.MethodPut, "/backup/plans", map[string]any{"BackupPlan": map[string]any{}}, missing},
		{"CreateBackupSelection/no SelectionName", http.MethodPut, planPath + "/selections",
			map[string]any{"BackupSelection": map[string]any{}}, missing},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, code, body, err := h.call(tc.method, tc.path, nil, tc.body)
			require.NoError(t, err)
			require.Equalf(t, tc.code, code, "%s %s answered %d %s: %s", tc.method, tc.path, status, code, body)
			require.Equal(t, http.StatusBadRequest, status, "%s: every Backup page publishes %s at 400", tc.name, tc.code)
		})
	}
}

// #1177: UpdateBackupPlan answers the members API_UpdateBackupPlan publishes, from the stored plan,
// and not the UpdatedAt no AWS Backup page publishes.
func TestBackupRefusals_UpdateBackupPlanAnswersItsPublishedMembers(t *testing.T) {
	t.Parallel()
	h := newBackupHarness(t)
	created := h.ok(http.MethodPut, "/backup/plans", nil, map[string]any{"BackupPlan": map[string]any{"BackupPlanName": "members-plan"}})
	var plan map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(created, &plan), "decode CreateBackupPlan: %s", created)
	var id string
	require.NoError(t, json.Unmarshal(plan["BackupPlanId"], &id))

	updated := h.ok(http.MethodPost, "/backup/plans/"+id, nil, map[string]any{"BackupPlan": map[string]any{"BackupPlanName": "members-plan-2"}})
	var got map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(updated, &got), "decode UpdateBackupPlan: %s", updated)
	keys := make([]string, 0, len(got))
	for k := range got {
		keys = append(keys, k)
	}
	require.ElementsMatch(t, []string{"BackupPlanArn", "BackupPlanId", "CreationDate", "VersionId"}, keys,
		"UpdateBackupPlan answers the four published members the record holds: %s", updated)
	require.JSONEq(t, string(plan["CreationDate"]), string(got["CreationDate"]),
		"CreationDate is the plan's own; an update does not move it: %s", updated)
	require.NotEqual(t, string(plan["VersionId"]), string(got["VersionId"]), "an update mints a new VersionId: %s", updated)
}

// backupRefusalsMember decodes one string member out of a JSON body.
func backupRefusalsMember(t *testing.T, body []byte, member string) string {
	t.Helper()
	var m map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &m), "decode: %s", body)
	var v string
	require.NoError(t, json.Unmarshal(m[member], &v), "%s in %s", member, body)
	return v
}

// #1173: a create repeating a recorded CreatorRequestId answers the resource it first made.
// API_CreateBackupPlan: "If the request includes a CreatorRequestId that matches an existing backup
// plan, that plan is returned." That sentence is the contract.
func TestBackupRefusals_ACreateRepeatingItsCreatorRequestIDReturnsTheExistingResource(t *testing.T) {
	t.Parallel()
	h := newBackupHarness(t)

	planBody := map[string]any{"BackupPlan": map[string]any{"BackupPlanName": "idem-plan"}, "CreatorRequestId": "req-plan-1"}
	first := h.ok(http.MethodPut, "/backup/plans", nil, planBody)
	second := h.ok(http.MethodPut, "/backup/plans", nil, planBody)
	firstID := backupRefusalsMember(t, first, "BackupPlanId")
	require.Equal(t, firstID, backupRefusalsMember(t, second, "BackupPlanId"), "a retried plan create returns the first plan")
	require.Equal(t, backupRefusalsMember(t, first, "VersionId"), backupRefusalsMember(t, second, "VersionId"), "and its version")

	list := h.ok(http.MethodGet, "/backup/plans", nil, nil)
	var plans struct {
		BackupPlansList []map[string]any `json:"BackupPlansList"`
	}
	require.NoError(t, json.Unmarshal(list, &plans))
	require.Len(t, plans.BackupPlansList, 1, "one plan, not two: %s", list)
	require.Equal(t, "req-plan-1", plans.BackupPlansList[0]["CreatorRequestId"], "ListBackupPlans answers the recorded CreatorRequestId")
	got := h.ok(http.MethodGet, "/backup/plans/"+firstID, nil, nil)
	require.Contains(t, string(got), `"CreatorRequestId":"req-plan-1"`, "GetBackupPlan answers it: %s", got)
	other := h.ok(http.MethodPut, "/backup/plans", nil, map[string]any{"BackupPlan": map[string]any{"BackupPlanName": "idem-plan"}, "CreatorRequestId": "req-plan-2"})
	require.NotEqual(t, firstID, backupRefusalsMember(t, other, "BackupPlanId"), "a different CreatorRequestId is a new plan")

	selPath := "/backup/plans/" + firstID + "/selections"
	selBody := map[string]any{"BackupSelection": map[string]any{"SelectionName": "idem-sel", "IamRoleArn": "arn:aws:iam::123456789012:role/b"}, "CreatorRequestId": "req-sel-1"}
	s1 := h.ok(http.MethodPut, selPath, nil, selBody)
	s2 := h.ok(http.MethodPut, selPath, nil, selBody)
	selID := backupRefusalsMember(t, s1, "SelectionId")
	require.Equal(t, selID, backupRefusalsMember(t, s2, "SelectionId"), "a retried selection create returns the first selection")
	gotSel := h.ok(http.MethodGet, selPath+"/"+selID, nil, nil)
	require.Contains(t, string(gotSel), `"CreatorRequestId":"req-sel-1"`, "GetBackupSelection answers it: %s", gotSel)

	v1 := h.ok(http.MethodPut, "/backup-vaults/idem-vault", nil, map[string]any{"CreatorRequestId": "req-vault-1"})
	v2 := h.ok(http.MethodPut, "/backup-vaults/idem-vault", nil, map[string]any{"CreatorRequestId": "req-vault-1"})
	require.JSONEq(t, string(v1), string(v2), "a retried vault create answers the first create's response")
	for _, body := range []map[string]any{nil, {"CreatorRequestId": "req-vault-2"}} {
		status, code, _, err := h.call(http.MethodPut, "/backup-vaults/idem-vault", nil, body)
		require.NoError(t, err)
		require.Equal(t, "AlreadyExistsException", code, "a vault create without the recorded CreatorRequestId is a collision (%v)", body)
		require.Equal(t, http.StatusBadRequest, status)
	}
}

func TestBackupRefusals_ACreatorRequestIDOutsideItsPatternIsRefused(t *testing.T) {
	t.Parallel()
	h := newBackupHarness(t)
	created := h.ok(http.MethodPut, "/backup/plans", nil, map[string]any{"BackupPlan": map[string]any{"BackupPlanName": "pattern-plan"}})
	planID := backupRefusalsMember(t, created, "BackupPlanId")
	const bad = "has a space"
	for _, tc := range []struct {
		name, path string
		body       map[string]any
	}{
		{"CreateBackupPlan", "/backup/plans", map[string]any{"BackupPlan": map[string]any{"BackupPlanName": "p"}, "CreatorRequestId": bad}},
		{"CreateBackupSelection", "/backup/plans/" + planID + "/selections",
			map[string]any{"BackupSelection": map[string]any{"SelectionName": "s"}, "CreatorRequestId": bad}},
	} {
		status, code, _, err := h.call(http.MethodPut, tc.path, nil, tc.body)
		require.NoError(t, err)
		require.Equal(t, "InvalidParameterValueException", code, tc.name)
		require.Equal(t, http.StatusBadRequest, status, tc.name)
	}
}

// A store fault in the CreatorRequestId lookups is an error, never a second resource or a refusal.
func TestBackupRefusals_AStoreFaultInTheCreatorRequestIDLookupIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
		plan bool
	}{
		{"plan index read", func(m *cfFaultStateManager) { m.failGet = "plan_ids:" }, true},
		{"plan record read", func(m *cfFaultStateManager) { m.failGet = "plan:123" }, true},
		{"selection index read", func(m *cfFaultStateManager) { m.failGet = "selection_ids:" }, false},
		{"selection record corrupt", func(m *cfFaultStateManager) { m.corruptGet = "selection:123" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newBackupHarness(t)
			created := h.ok(http.MethodPut, "/backup/plans", nil, map[string]any{"BackupPlan": map[string]any{"BackupPlanName": "fault-plan"}, "CreatorRequestId": "req-f"})
			selPath := "/backup/plans/" + backupRefusalsMember(t, created, "BackupPlanId") + "/selections"
			h.ok(http.MethodPut, selPath, nil, map[string]any{"BackupSelection": map[string]any{"SelectionName": "s"}, "CreatorRequestId": "req-s"})

			tc.arm(h.state)
			path, body := selPath, map[string]any{"BackupSelection": map[string]any{"SelectionName": "s"}, "CreatorRequestId": "req-s"}
			if tc.plan {
				path, body = "/backup/plans", map[string]any{"BackupPlan": map[string]any{"BackupPlanName": "fault-plan"}, "CreatorRequestId": "req-f"}
			}
			_, code, _, err := h.call(http.MethodPut, path, nil, body)
			require.Error(t, err, "%s must fail on a store fault", tc.name)
			require.Empty(t, code, "a store fault is not a published refusal")
		})
	}
}
