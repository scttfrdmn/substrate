package emulator_test

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Backup's share of #1199: the vault record answered 5 of API_DescribeBackupVault's 17 members and
// of API_BackupVaultListMember's 13, and ListBackupVaults was one page with both filters ignored.
// The assertions are on raw JSON, because a typed decode cannot tell an absent Locked from a false
// one.

// backupHarness drives the Backup plugin directly over a store a test may fault.
type backupHarness struct {
	t     *testing.T
	p     *emulator.BackupPlugin
	ctx   *emulator.RequestContext
	state *cfFaultStateManager
}

func newBackupHarness(t *testing.T) *backupHarness {
	t.Helper()
	clock := time.Unix(1700000000, 0).UTC()
	tc := emulator.NewTimeController(clock)
	tc.Freeze()
	tc.SetTime(clock)
	h := &backupHarness{t: t, p: &emulator.BackupPlugin{}, state: &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}}
	require.NoError(t, h.p.Initialize(t.Context(), emulator.PluginConfig{
		State:   h.state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}), "Initialize")
	h.ctx = &emulator.RequestContext{
		AccountID: "123456789012", Region: "us-east-1", RequestID: "req-backup-vault", IDs: emulator.NewIDMint("req-backup-vault"),
	}
	return h
}

// call issues one REST request and returns the status and body, or the refusal's code and status.
func (h *backupHarness) call(method, path string, params map[string]string, body map[string]any) (int, string, []byte, error) {
	h.t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		require.NoError(h.t, err)
	}
	if params == nil {
		params = map[string]string{}
	}
	resp, err := h.p.HandleRequest(h.ctx, &emulator.AWSRequest{
		Service: "backup", HTTPMethod: method, Path: path, Body: raw, Params: params,
		Headers: map[string]string{"Content-Type": "application/json"},
	})
	var awsErr *emulator.AWSError
	if errors.As(err, &awsErr) {
		return awsErr.HTTPStatus, awsErr.Code, nil, nil
	}
	if err != nil {
		return 0, "", nil, err
	}
	return resp.StatusCode, "", resp.Body, nil
}

func (h *backupHarness) ok(method, path string, params map[string]string, body map[string]any) []byte {
	h.t.Helper()
	status, code, out, err := h.call(method, path, params, body)
	require.NoError(h.t, err)
	require.Empty(h.t, code, "%s %s was refused", method, path)
	require.Equal(h.t, http.StatusOK, status)
	return out
}

func TestBackupVault_DescribeAndListAnswerThePublishedStateMembers(t *testing.T) {
	t.Parallel()
	h := newBackupHarness(t)
	h.ok(http.MethodPut, "/backup-vaults/vault-a", nil, map[string]any{"CreatorRequestId": "req-1.a_b"})

	described := h.ok(http.MethodGet, "/backup-vaults/vault-a", nil, nil)
	listed := h.ok(http.MethodGet, "/backup-vaults/", nil, nil)
	var list struct {
		BackupVaultList []json.RawMessage `json:"BackupVaultList"`
	}
	require.NoError(t, json.Unmarshal(listed, &list), "%s", listed)
	require.Len(t, list.BackupVaultList, 1)

	for op, raw := range map[string][]byte{"DescribeBackupVault": described, "ListBackupVaults": list.BackupVaultList[0]} {
		var vault map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(raw, &vault), "%s: %s", op, raw)
		require.JSONEqf(t, `"AVAILABLE"`, string(vault["VaultState"]), "%s answers VaultState: %s", op, raw)
		require.JSONEqf(t, `"BACKUP_VAULT"`, string(vault["VaultType"]), "%s answers VaultType: %s", op, raw)
		require.JSONEqf(t, `false`, string(vault["Locked"]), "%s answers Locked as false, not absent: %s", op, raw)
		require.JSONEqf(t, `"req-1.a_b"`, string(vault["CreatorRequestId"]), "%s answers the create's CreatorRequestId: %s", op, raw)
		for _, absent := range []string{"LockDate", "MinRetentionDays", "MaxRetentionDays", "EncryptionKeyType", "AccountID", "Region"} {
			_, present := vault[absent]
			require.Falsef(t, present, "%s answers %s, which no substrate vault has a value for: %s", op, absent, raw)
		}
	}
}

func TestBackupVault_ACreatorRequestIdOutsideItsPatternIsRefused(t *testing.T) {
	t.Parallel()
	for _, id := range []string{"has space", "x/slash", fmt.Sprintf("%051d", 0)} {
		h := newBackupHarness(t)
		status, code, _, err := h.call(http.MethodPut, "/backup-vaults/vault-bad", nil, map[string]any{"CreatorRequestId": id})
		require.NoError(t, err)
		require.Equal(t, "InvalidParameterValueException", code, "%q", id)
		require.Equal(t, http.StatusBadRequest, status)
	}
}

func TestBackupVault_ListBackupVaultsPagesAndFilters(t *testing.T) {
	t.Parallel()
	h := newBackupHarness(t)
	names := []string{"vault-e", "vault-a", "vault-c", "vault-b", "vault-d"}
	for _, n := range names {
		h.ok(http.MethodPut, "/backup-vaults/"+n, nil, nil)
	}
	listNames := func(raw []byte) ([]string, string, bool) {
		var doc map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(raw, &doc), "%s", raw)
		var list []struct {
			Name string `json:"BackupVaultName"`
		}
		require.NoError(t, json.Unmarshal(doc["BackupVaultList"], &list))
		out := make([]string, 0, len(list))
		for _, v := range list {
			out = append(out, v.Name)
		}
		tok, present := doc["NextToken"]
		var s string
		if present {
			require.NoError(t, json.Unmarshal(tok, &s))
		}
		return out, s, present
	}

	var seen []string
	params := map[string]string{"maxResults": "2"}
	pages := 0
	for {
		pages++
		require.LessOrEqual(t, pages, 10, "the walk must terminate")
		got, tok, present := listNames(h.ok(http.MethodGet, "/backup-vaults/", params, nil))
		require.LessOrEqual(t, len(got), 2, "maxResults bounds the page")
		seen = append(seen, got...)
		if !present {
			break
		}
		require.NotEmpty(t, tok)
		params = map[string]string{"maxResults": "2", "nextToken": tok}
	}
	require.Equal(t, 3, pages, "five vaults at two per page is three pages")
	want := slices.Clone(names)
	slices.Sort(want)
	require.Equal(t, want, seen, "a walk sees every vault once, in name order")

	all, _, present := listNames(h.ok(http.MethodGet, "/backup-vaults/", nil, nil))
	require.Len(t, all, 5)
	require.False(t, present, "a final page omits NextToken")

	// The filters narrow: every substrate vault is a BACKUP_VAULT, and none is shared.
	for _, tc := range []struct {
		params map[string]string
		want   int
	}{
		{map[string]string{"vaultType": "BACKUP_VAULT"}, 5},
		{map[string]string{"vaultType": "LOGICALLY_AIR_GAPPED_BACKUP_VAULT"}, 0},
		{map[string]string{"vaultType": "RESTORE_ACCESS_BACKUP_VAULT"}, 0},
		{map[string]string{"shared": "true"}, 0},
		{map[string]string{"shared": "false"}, 5},
	} {
		got, _, _ := listNames(h.ok(http.MethodGet, "/backup-vaults/", tc.params, nil))
		require.Lenf(t, got, tc.want, "%v", tc.params)
	}

	// Refusals, each the page's InvalidParameterValueException/400.
	for _, params := range []map[string]string{
		{"maxResults": "0"},
		{"maxResults": "1001"},
		{"maxResults": "two"},
		{"nextToken": "not-a-token"},
		{"nextToken": base64.StdEncoding.EncodeToString([]byte("05"))},
		{"vaultType": "SOME_VAULT"},
		{"shared": "maybe"},
	} {
		status, code, _, err := h.call(http.MethodGet, "/backup-vaults/", params, nil)
		require.NoError(t, err)
		require.Equalf(t, "InvalidParameterValueException", code, "%v", params)
		require.Equal(t, http.StatusBadRequest, status)
	}
}

// API_DeleteBackupPlan publishes a four-member body, and the handler answered `{}` (#1206's survey,
// #1177).
func TestBackupPlan_DeleteAnswersThePublishedBody(t *testing.T) {
	t.Parallel()
	h := newBackupHarness(t)
	var created struct {
		BackupPlanID  string `json:"BackupPlanId"`
		BackupPlanArn string `json:"BackupPlanArn"`
		VersionID     string `json:"VersionId"`
	}
	raw := h.ok(http.MethodPut, "/backup/plans", nil, map[string]any{"BackupPlan": map[string]any{"BackupPlanName": "del-plan"}})
	require.NoError(t, json.Unmarshal(raw, &created), "%s", raw)

	deleted := h.ok(http.MethodDelete, "/backup/plans/"+created.BackupPlanID, nil, nil)
	require.JSONEq(t, fmt.Sprintf(`{"BackupPlanArn":%q,"BackupPlanId":%q,"DeletionDate":1700000000.000,"VersionId":%q}`,
		created.BackupPlanArn, created.BackupPlanID, created.VersionID), string(deleted))
}

func TestBackupVault_AStoreFaultInTheListIsAnError(t *testing.T) {
	t.Parallel()
	for _, arm := range []func(*cfFaultStateManager){
		func(m *cfFaultStateManager) { m.failGet = "vault:" },
		func(m *cfFaultStateManager) { m.corruptGet = "vault:" },
	} {
		h := newBackupHarness(t)
		h.ok(http.MethodPut, "/backup-vaults/vault-f", nil, nil)
		arm(h.state)
		_, code, _, err := h.call(http.MethodGet, "/backup-vaults/", nil, nil)
		require.Errorf(t, err, "a store fault must be an error, not a shorter list (answered %q)", code)
	}
}
