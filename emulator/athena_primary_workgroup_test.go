package emulator_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Wire-level tests for #1222: the primary workgroup Athena gives every account is reported by every
// reader that reports workgroups, and refuses to be deleted.
//
// Everything below drives CreateWorkGroup / ListWorkGroups / GetWorkGroup / DeleteWorkGroup over HTTP
// rather than seeding the workgroup-names index, per #765: the index is exactly what the defect was
// about — the primary workgroup was never in it — so a test that wrote to it would have asserted the
// bug's own premise.

// athenaWorkGroupSummary is the shape of one entry in either reader's answer. Description is a *string
// so an absent member and an empty one are distinguishable, which is the point of
// TestAthenaWorkGroupReadersAgree.
type athenaWorkGroupSummary struct {
	Name        string  `json:"Name"`
	State       string  `json:"State"`
	Description *string `json:"Description"`
}

// athenaListWorkGroups returns one page of ListWorkGroups plus its NextToken.
func athenaListWorkGroups(t *testing.T, ts *httptest.Server, body map[string]interface{}) ([]athenaWorkGroupSummary, string) {
	t.Helper()
	resp := athenaRequest(t, ts, "ListWorkGroups", body)
	raw := athenaBody(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode, "ListWorkGroups: %s", raw)
	var out struct {
		WorkGroups []athenaWorkGroupSummary `json:"WorkGroups"`
		NextToken  string                   `json:"NextToken"`
	}
	require.NoError(t, json.Unmarshal(raw, &out), "decode ListWorkGroups: %s", raw)
	return out.WorkGroups, out.NextToken
}

// athenaWorkGroupNames reduces a page to the names it reported, in the order it reported them.
func athenaWorkGroupNames(page []athenaWorkGroupSummary) []string {
	names := make([]string, len(page))
	for i, wg := range page {
		names[i] = wg.Name
	}
	return names
}

// athenaGetWorkGroup returns the workgroup GetWorkGroup reports, or the error code it refuses with.
func athenaGetWorkGroup(t *testing.T, ts *httptest.Server, name string) (athenaWorkGroupSummary, string) {
	t.Helper()
	resp := athenaRequest(t, ts, "GetWorkGroup", map[string]string{"WorkGroup": name})
	raw := athenaBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		var errShape struct {
			Type string `json:"__type"`
		}
		require.NoError(t, json.Unmarshal(raw, &errShape), "decode GetWorkGroup refusal: %s", raw)
		return athenaWorkGroupSummary{}, awsErrorCode(errShape.Type)
	}
	var out struct {
		WorkGroup athenaWorkGroupSummary `json:"WorkGroup"`
	}
	require.NoError(t, json.Unmarshal(raw, &out), "decode GetWorkGroup: %s", raw)
	return out.WorkGroup, ""
}

// athenaCreateWorkGroup creates a workgroup and requires that it was created.
func athenaCreateWorkGroup(t *testing.T, ts *httptest.Server, name, description string) {
	t.Helper()
	body := map[string]interface{}{"Name": name}
	if description != "" {
		body["Description"] = description
	}
	resp := athenaRequest(t, ts, "CreateWorkGroup", body)
	raw := athenaBody(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode, "CreateWorkGroup %s: %s", name, raw)
}

// athenaDeleteWorkGroup returns the status and error code DeleteWorkGroup answered with.
func athenaDeleteWorkGroup(t *testing.T, ts *httptest.Server, name string) (int, string, string) {
	t.Helper()
	resp := athenaRequest(t, ts, "DeleteWorkGroup", map[string]string{"WorkGroup": name})
	raw := athenaBody(t, resp)
	if resp.StatusCode == http.StatusOK {
		return resp.StatusCode, "", ""
	}
	var errShape struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(raw, &errShape), "decode DeleteWorkGroup refusal: %s", raw)
	return resp.StatusCode, awsErrorCode(errShape.Type), errShape.Message
}

// TestAthenaPrimaryWorkGroupIsListedForAFreshAccount is #1222's central assertion: an account that has
// created no workgroup still has one, so ListWorkGroups reports it rather than an empty list.
//
// The published fact is that the primary workgroup exists without being created — API_DeleteWorkGroup
// states "The primary workgroup cannot be deleted", which is only meaningful of a workgroup that is
// there. Before this, GetWorkGroup("primary") answered 200 while ListWorkGroups answered [], so the
// two readers disagreed about whether the same workgroup existed.
func TestAthenaPrimaryWorkGroupIsListedForAFreshAccount(t *testing.T) {
	ts := newAthenaTestServer(t)

	page, nextToken := athenaListWorkGroups(t, ts, map[string]interface{}{})
	assert.Empty(t, nextToken, "one workgroup does not need a second page")
	require.Len(t, page, 1, "a fresh account has exactly the primary workgroup")
	assert.Equal(t, "primary", page[0].Name)
	assert.Equal(t, "ENABLED", page[0].State, "the workgroup an unqualified query runs in is usable")
}

// TestAthenaPrimaryWorkGroupIsListedFirst asserts the position, which is not cosmetic: ListWorkGroups
// pages by an offset into this order (#1086), so where the synthesized entry sits decides whether a
// token issued mid-walk still points at the element it pointed at when it was issued.
//
// Prepending is the only position that keeps that true. The primary workgroup exists before any
// workgroup a caller creates, so creation order puts it first, and appending it would move it on every
// CreateWorkGroup.
func TestAthenaPrimaryWorkGroupIsListedFirst(t *testing.T) {
	ts := newAthenaTestServer(t)

	athenaCreateWorkGroup(t, ts, "wg-alpha", "")
	athenaCreateWorkGroup(t, ts, "wg-beta", "")

	page, _ := athenaListWorkGroups(t, ts, map[string]interface{}{})
	assert.Equal(t, []string{"primary", "wg-alpha", "wg-beta"}, athenaWorkGroupNames(page),
		"the primary workgroup precedes the created ones, in creation order")
}

// TestAthenaWorkGroupReadersAgree asserts the two readers render the same workgroup identically, for
// the synthesized one and for created ones with and without a description.
//
// This is the assertion the issue title names, and it is stronger than "both report the workgroup":
// the members are compared as decoded values, so a Description that one reader sends as "" and the
// other omits fails here. Both pages give Description "Required: No", so the absent member is the
// faithful rendering of a workgroup that has none, and GetWorkGroup's empty string was the divergence.
func TestAthenaWorkGroupReadersAgree(t *testing.T) {
	ts := newAthenaTestServer(t)

	athenaCreateWorkGroup(t, ts, "wg-described", "analytics workgroup")
	athenaCreateWorkGroup(t, ts, "wg-bare", "")

	page, _ := athenaListWorkGroups(t, ts, map[string]interface{}{})
	require.Len(t, page, 3, "primary plus the two created")

	for _, summary := range page {
		t.Run(summary.Name, func(t *testing.T) {
			got, errCode := athenaGetWorkGroup(t, ts, summary.Name)
			require.Empty(t, errCode, "GetWorkGroup %s", summary.Name)
			assert.Equal(t, summary, got, "ListWorkGroups and GetWorkGroup describe %s the same way", summary.Name)
		})
	}

	bare, _ := athenaGetWorkGroup(t, ts, "wg-bare")
	assert.Nil(t, bare.Description, "a workgroup created without a description has none, not an empty one")
	described, _ := athenaGetWorkGroup(t, ts, "wg-described")
	require.NotNil(t, described.Description)
	assert.Equal(t, "analytics workgroup", *described.Description)
}

// TestAthenaDeletePrimaryWorkGroupIsRefused asserts the refusal API_DeleteWorkGroup publishes in its
// own description — "The primary workgroup cannot be deleted" — and that the workgroup survives it.
//
// Before #1222 the answer was InvalidRequestException with "WorkGroup primary not found", which is the
// right code for the wrong reason: the same server reported the workgroup as existing through
// GetWorkGroup in the same breath. The code is unchanged because InvalidRequestException/400 is the
// only 400-class error the page publishes; the message and the reason are what changed.
func TestAthenaDeletePrimaryWorkGroupIsRefused(t *testing.T) {
	ts := newAthenaTestServer(t)

	status, code, message := athenaDeleteWorkGroup(t, ts, "primary")
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidRequestException", code)
	assert.Equal(t, "The primary workgroup cannot be deleted", message)

	got, errCode := athenaGetWorkGroup(t, ts, "primary")
	require.Empty(t, errCode, "a refused delete leaves the workgroup readable")
	assert.Equal(t, "primary", got.Name)

	page, _ := athenaListWorkGroups(t, ts, map[string]interface{}{})
	assert.Equal(t, []string{"primary"}, athenaWorkGroupNames(page), "and listed")
}

// TestAthenaCreatedPrimaryWorkGroupIsNotDuplicated covers the collision the synthesized entry makes
// possible: a caller creating its own workgroup named `primary`.
//
// Two things must hold. The listing reports it once — the guard is against the names index rather than
// against the record, so the entry CreateWorkGroup appended is not joined by a synthesized twin. And
// the stored record wins both readers, because answering the synthesized one over a real record would
// discard a write.
//
// It still cannot be deleted. The refusal is on the name, not on the absence of a record, because
// AWS's statement is flat: the primary workgroup cannot be deleted.
func TestAthenaCreatedPrimaryWorkGroupIsNotDuplicated(t *testing.T) {
	ts := newAthenaTestServer(t)

	athenaCreateWorkGroup(t, ts, "primary", "the caller's own")

	page, _ := athenaListWorkGroups(t, ts, map[string]interface{}{})
	assert.Equal(t, []string{"primary"}, athenaWorkGroupNames(page), "listed once, not twice")
	require.NotNil(t, page[0].Description)
	assert.Equal(t, "the caller's own", *page[0].Description, "the stored record, not the synthesized one")

	got, errCode := athenaGetWorkGroup(t, ts, "primary")
	require.Empty(t, errCode)
	assert.Equal(t, page[0], got, "and both readers read the stored record")

	status, code, message := athenaDeleteWorkGroup(t, ts, "primary")
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidRequestException", code)
	assert.Equal(t, "The primary workgroup cannot be deleted", message)
}

// TestAthenaListWorkGroupsPagesOverThePrependedPrimary walks the listing a page at a time and asserts
// the prepended entry participates in the pagination rather than being added to each page.
//
// The second half is why the position was argued rather than picked: a workgroup created after a token
// was issued must not move an entry the caller has not reached yet. Because the new name is appended
// and `primary` is already at offset 0, the offset the token carries still indexes the element it
// indexed when it was issued — so resuming reports the remaining entries once each, and the late
// arrival shows up after them rather than displacing one.
func TestAthenaListWorkGroupsPagesOverThePrependedPrimary(t *testing.T) {
	ts := newAthenaTestServer(t)

	for _, name := range []string{"wg-1", "wg-2", "wg-3"} {
		athenaCreateWorkGroup(t, ts, name, "")
	}

	first, token := athenaListWorkGroups(t, ts, map[string]interface{}{"MaxResults": 2})
	assert.Equal(t, []string{"primary", "wg-1"}, athenaWorkGroupNames(first),
		"the primary workgroup occupies a slot in the first page rather than being added to every page")
	require.NotEmpty(t, token, "four workgroups do not fit in a page of two")

	athenaCreateWorkGroup(t, ts, "wg-late", "")

	var seen []string
	seen = append(seen, athenaWorkGroupNames(first)...)
	for token != "" {
		var page []athenaWorkGroupSummary
		page, token = athenaListWorkGroups(t, ts, map[string]interface{}{"MaxResults": 2, "NextToken": token})
		seen = append(seen, athenaWorkGroupNames(page)...)
	}
	assert.Equal(t, []string{"primary", "wg-1", "wg-2", "wg-3", "wg-late"}, seen,
		"every workgroup once, in order, with the one created mid-walk last")
}

// TestAthenaUnqualifiedQueryRunsInAListedWorkGroup closes the loop the defect left open: a query that
// names no workgroup is attributed to `primary`, and ListQueryExecutions reports it under that name,
// so the listing that omitted `primary` was claiming the query ran in a workgroup that did not exist.
func TestAthenaUnqualifiedQueryRunsInAListedWorkGroup(t *testing.T) {
	ts := newAthenaTestServer(t)

	resp := athenaRequest(t, ts, "StartQueryExecution", map[string]interface{}{
		"QueryString": "SELECT 1",
	})
	raw := athenaBody(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode, "StartQueryExecution: %s", raw)
	var started struct {
		QueryExecutionID string `json:"QueryExecutionId"`
	}
	require.NoError(t, json.Unmarshal(raw, &started))

	getResp := athenaRequest(t, ts, "GetQueryExecution", map[string]string{
		"QueryExecutionId": started.QueryExecutionID,
	})
	getRaw := athenaBody(t, getResp)
	require.Equal(t, http.StatusOK, getResp.StatusCode, "GetQueryExecution: %s", getRaw)
	var execution struct {
		QueryExecution struct {
			WorkGroup string `json:"WorkGroup"`
		} `json:"QueryExecution"`
	}
	require.NoError(t, json.Unmarshal(getRaw, &execution))
	require.Equal(t, "primary", execution.QueryExecution.WorkGroup)

	page, _ := athenaListWorkGroups(t, ts, map[string]interface{}{})
	assert.Contains(t, athenaWorkGroupNames(page), execution.QueryExecution.WorkGroup,
		"the workgroup a query ran in is one the listing reports")
}
