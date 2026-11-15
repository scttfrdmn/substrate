package emulator_test

// ListBackupPlans reads its three published query parameters (#1184): maxResults bounds a page,
// nextToken resumes it, includeDeleted is accepted as a boolean, and a plan whose record cannot be read
// fails the listing rather than vanishing from it.

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// backupPlanListPage decodes one ListBackupPlans body into its plan IDs and NextToken.
func backupPlanListPage(t *testing.T, raw []byte) (ids []string, token string, present bool) {
	t.Helper()
	var doc map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &doc), "%s", raw)
	var list []struct {
		ID string `json:"BackupPlanId"`
	}
	require.NoError(t, json.Unmarshal(doc["BackupPlansList"], &list))
	for _, p := range list {
		ids = append(ids, p.ID)
	}
	if tok, ok := doc["NextToken"]; ok {
		require.NoError(t, json.Unmarshal(tok, &token))
		present = true
	}
	return ids, token, present
}

func TestBackupPlan_ListBackupPlansPages(t *testing.T) {
	t.Parallel()
	h := newBackupHarness(t)
	var created []string
	for i := range 5 {
		raw := h.ok(http.MethodPut, "/backup/plans", nil, map[string]any{
			"BackupPlan": map[string]any{"BackupPlanName": fmt.Sprintf("plan-%d", i)},
		})
		var out struct {
			ID string `json:"BackupPlanId"`
		}
		require.NoError(t, json.Unmarshal(raw, &out))
		created = append(created, out.ID)
	}

	var seen []string
	params := map[string]string{"maxResults": "2"}
	pages := 0
	for {
		pages++
		require.LessOrEqual(t, pages, 10, "the walk must terminate")
		got, tok, present := backupPlanListPage(t, h.ok(http.MethodGet, "/backup/plans/", params, nil))
		require.LessOrEqual(t, len(got), 2, "maxResults bounds the page")
		seen = append(seen, got...)
		if !present {
			break
		}
		require.NotEmpty(t, tok, "a truncated page's token is never empty")
		params = map[string]string{"maxResults": "2", "nextToken": tok}
	}
	require.Equal(t, 3, pages, "five plans at two per page is three pages")
	want := slices.Clone(created)
	slices.Sort(want)
	require.Equal(t, want, seen, "a walk sees every plan once, in the index's order")

	// No pagination parameters is one page with no NextToken, as before.
	all, _, present := backupPlanListPage(t, h.ok(http.MethodGet, "/backup/plans/", nil, nil))
	require.Len(t, all, 5)
	require.False(t, present, "a final page omits NextToken")

	// includeDeleted is accepted; substrate keeps no deleted plan, so both values list the same plans.
	for _, v := range []string{"true", "false", "TRUE"} {
		got, _, _ := backupPlanListPage(t, h.ok(http.MethodGet, "/backup/plans/", map[string]string{"includeDeleted": v}, nil))
		require.Lenf(t, got, 5, "includeDeleted=%s", v)
	}

	for _, params := range []map[string]string{
		{"maxResults": "0"},
		{"maxResults": "-1"},
		{"maxResults": "1001"},
		{"maxResults": "two"},
		{"nextToken": "not-a-token"},
		{"nextToken": base64.StdEncoding.EncodeToString([]byte("05"))},
		{"includeDeleted": "maybe"},
		{"includeDeleted": "1"},
		{"includeDeleted": "t"},
	} {
		status, code, _, err := h.call(http.MethodGet, "/backup/plans/", params, nil)
		require.NoError(t, err)
		require.Equalf(t, "InvalidParameterValueException", code, "%v", params)
		require.Equal(t, http.StatusBadRequest, status)
	}
}

// A plan the store cannot read fails the listing; it used to be skipped, which shortened the list and
// answered 200 over a store failure.
func TestBackupPlan_ListBackupPlansSurfacesAReadError(t *testing.T) {
	t.Parallel()
	h := newBackupHarness(t)
	raw := h.ok(http.MethodPut, "/backup/plans", nil, map[string]any{
		"BackupPlan": map[string]any{"BackupPlanName": "unreadable"},
	})
	var out struct {
		ID string `json:"BackupPlanId"`
	}
	require.NoError(t, json.Unmarshal(raw, &out))

	h.state.failGet = "plan:123456789012/us-east-1/" + out.ID
	_, code, _, err := h.call(http.MethodGet, "/backup/plans/", nil, nil)
	require.Error(t, err, "a store failure is an error, not a shorter list")
	require.Empty(t, code)

	h.state.failGet = ""
	h.state.corruptGet = "plan:123456789012/us-east-1/" + out.ID
	_, _, _, err = h.call(http.MethodGet, "/backup/plans/", nil, nil)
	require.Error(t, err, "an undecodable record is an error, not a shorter list")
}
