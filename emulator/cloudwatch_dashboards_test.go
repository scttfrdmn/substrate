package emulator_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// CloudWatch dashboards (#1613): a body recorded and answered back, global per account, paged by
// name, and the not-found error spelled the way each of CloudWatch's three protocols spells it.

const cwDashboardBody = `{"widgets":[{"type":"text","x":0,"y":0,"width":3,"height":3,"properties":{"markdown":"Hello world"}}]}`

func TestCWDashboards_PutGetListDelete(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, nil)

	out := e.ok("PutDashboard", map[string]any{"DashboardName": "ops-main", "DashboardBody": cwDashboardBody})
	assert.Equal(t, []any{}, out["DashboardValidationMessages"], "a valid body answers an empty list, present")

	got := e.ok("GetDashboard", map[string]any{"DashboardName": "ops-main"})
	arn, _ := got["DashboardArn"].(string)
	assert.Equal(t, "ops-main", got["DashboardName"])
	assert.Equal(t, cwDashboardBody, got["DashboardBody"], "the body is answered byte for byte")
	assert.True(t, strings.HasPrefix(arn, "arn:aws:cloudwatch::"), "a dashboard is global, so its ARN has no Region: %s", arn)
	assert.True(t, strings.HasSuffix(arn, ":dashboard/ops-main"), "%s", arn)

	// A put replaces the body and moves LastModified with the simulated clock.
	e.tc.SetTime(cwStoreNow.Add(time.Hour))
	replaced := `{"widgets":[]}`
	e.ok("PutDashboard", map[string]any{"DashboardName": "ops-main", "DashboardBody": replaced})
	e.ok("PutDashboard", map[string]any{"DashboardName": "ops-other", "DashboardBody": cwDashboardBody})
	e.ok("PutDashboard", map[string]any{"DashboardName": "billing", "DashboardBody": cwDashboardBody})

	list := e.ok("ListDashboards", map[string]any{"DashboardNamePrefix": "ops-"})
	entries, _ := list["DashboardEntries"].([]any)
	require.Len(t, entries, 2)
	first, _ := entries[0].(map[string]any)
	assert.Equal(t, "ops-main", first["DashboardName"])
	assert.Equal(t, arn, first["DashboardArn"])
	assert.EqualValues(t, len(replaced), first["Size"], "Size is the body's length in bytes")
	assert.EqualValues(t, cwStoreNow.Add(time.Hour).Unix(), first["LastModified"])
	assert.Nil(t, list["NextToken"], "a complete listing carries no token")

	all := e.ok("ListDashboards", map[string]any{})
	require.Len(t, all["DashboardEntries"], 3)

	e.ok("DeleteDashboards", map[string]any{"DashboardNames": []string{"ops-main", "billing"}})
	status, body := e.do("GetDashboard", map[string]any{"DashboardName": "ops-main"})
	assert.Equal(t, http.StatusNotFound, status)
	assert.Equal(t, "com.amazonaws.cloudwatch#DashboardNotFoundError", body["__type"])
	assert.Equal(t, "Dashboard ops-main does not exist", body["message"])
	remaining := e.ok("ListDashboards", map[string]any{})
	require.Len(t, remaining["DashboardEntries"], 1)
}

// TestCWDashboards_DeleteDeletesWhatExists pins "the operation attempts to delete as many
// dashboards as possible": the missing name is reported after the existing one is gone.
func TestCWDashboards_DeleteDeletesWhatExists(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, nil)
	e.ok("PutDashboard", map[string]any{"DashboardName": "keep-going", "DashboardBody": cwDashboardBody})

	status, body := e.do("DeleteDashboards", map[string]any{"DashboardNames": []string{"never-existed", "keep-going"}})
	assert.Equal(t, http.StatusNotFound, status)
	assert.Equal(t, "Dashboard never-existed does not exist", body["message"])
	list := e.ok("ListDashboards", map[string]any{})
	assert.Empty(t, list["DashboardEntries"], "the existing dashboard is deleted despite the error")
}

func TestCWDashboards_Refusals(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, nil)

	for _, tc := range []struct {
		name, op, code, message string
		body                    map[string]any
	}{
		{"body not JSON", "PutDashboard", "DashboardInvalidInputError", "The field DashboardBody must be a valid JSON object",
			map[string]any{"DashboardName": "d", "DashboardBody": "{not json"}},
		{"body a JSON array", "PutDashboard", "DashboardInvalidInputError", "The field DashboardBody must be a valid JSON object",
			map[string]any{"DashboardName": "d", "DashboardBody": "[]"}},
		{"body null", "PutDashboard", "DashboardInvalidInputError", "The field DashboardBody must be a valid JSON object",
			map[string]any{"DashboardName": "d", "DashboardBody": "null"}},
		{"no body", "PutDashboard", "DashboardInvalidInputError", "", map[string]any{"DashboardName": "d"}},
		{"no name", "PutDashboard", "DashboardInvalidInputError", "", map[string]any{"DashboardBody": cwDashboardBody}},
		{"name with a space", "PutDashboard", "DashboardInvalidInputError", "",
			map[string]any{"DashboardName": "has space", "DashboardBody": cwDashboardBody}},
		{"name too long", "PutDashboard", "DashboardInvalidInputError", "",
			map[string]any{"DashboardName": strings.Repeat("a", 256), "DashboardBody": cwDashboardBody}},
		{"get without a name", "GetDashboard", "InvalidParameterValueException", "", map[string]any{}},
		{"delete without names", "DeleteDashboards", "InvalidParameterValueException", "", map[string]any{}},
		{"list with a foreign token", "ListDashboards", "InvalidParameterValueException", "", map[string]any{"NextToken": "bm9wZQ=="}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := e.refused(tc.op, tc.body, tc.code)
			if tc.message != "" {
				assert.Equal(t, tc.message, msg)
			}
		})
	}

	names := make([]string, 101)
	for i := range names {
		names[i] = fmt.Sprintf("d%d", i)
	}
	e.refused("DeleteDashboards", map[string]any{"DashboardNames": names}, "InvalidParameterValueException")

	// A name at the 255-character limit, of every permitted character class, is accepted.
	long := strings.Repeat("A", 250) + "z9-_x"
	e.ok("PutDashboard", map[string]any{"DashboardName": long, "DashboardBody": cwDashboardBody})
}

// TestCWDashboards_ListPages walks a listing past one page; the token names the last dashboard
// returned and is refused under a different prefix.
func TestCWDashboards_ListPages(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, nil)
	const n = 1003
	for i := range n {
		e.ok("PutDashboard", map[string]any{"DashboardName": fmt.Sprintf("p-%04d", i), "DashboardBody": `{}`})
	}
	page1 := e.ok("ListDashboards", map[string]any{"DashboardNamePrefix": "p-"})
	require.Len(t, page1["DashboardEntries"], 1000)
	token, _ := page1["NextToken"].(string)
	require.NotEmpty(t, token)

	// A dashboard created ahead of the cursor between pages does not shift the walk.
	e.ok("PutDashboard", map[string]any{"DashboardName": "p-0000a", "DashboardBody": `{}`})
	page2 := e.ok("ListDashboards", map[string]any{"DashboardNamePrefix": "p-", "NextToken": token})
	entries, _ := page2["DashboardEntries"].([]any)
	require.Len(t, entries, 3)
	firstOfPage2, _ := entries[0].(map[string]any)
	assert.Equal(t, "p-1000", firstOfPage2["DashboardName"])
	assert.Nil(t, page2["NextToken"])

	e.refused("ListDashboards", map[string]any{"DashboardNamePrefix": "q-", "NextToken": token}, "InvalidParameterValueException")
}

// TestCWDashboards_TagsOnCreateOnly pins "If you specify Tags when updating an existing dashboard,
// the tag updates are ignored": an update with bad tags is not refused for them.
func TestCWDashboards_TagsOnCreateOnly(t *testing.T) {
	t.Parallel()
	e := newCWEnv(t, nil)
	e.refused("PutDashboard", map[string]any{"DashboardName": "tagged", "DashboardBody": `{}`,
		"Tags": []map[string]string{{"Key": strings.Repeat("k", 129), "Value": "v"}}}, "DashboardInvalidInputError")
	e.ok("PutDashboard", map[string]any{"DashboardName": "tagged", "DashboardBody": `{}`,
		"Tags": []map[string]string{{"Key": "team", "Value": "ops"}}})
	e.ok("PutDashboard", map[string]any{"DashboardName": "tagged", "DashboardBody": `{}`,
		"Tags": []map[string]string{{"Key": strings.Repeat("k", 129), "Value": "v"}}})
}

// TestCWDashboards_NotFoundAcrossProtocols pins the wire spelling of a missing dashboard: Query
// says ResourceNotFound, JSON and CBOR name the DashboardNotFoundError shape, and the
// query-compatibility header carries the Query code a Terraform-style caller matches.
func TestCWDashboards_NotFoundAcrossProtocols(t *testing.T) {
	t.Parallel()
	srv := newCWAlarmTestServer(t)

	query := cwRequest(t, srv, map[string]string{"Action": "GetDashboard", "Version": "2010-08-01", "DashboardName": "gone"})
	defer query.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusNotFound, query.StatusCode)
	qbody := cwReadBody(t, query)
	assert.Contains(t, qbody, "<Code>ResourceNotFound</Code>")
	assert.Contains(t, qbody, "<Message>Dashboard gone does not exist</Message>")

	js := cwPost(t, srv, "/", "application/x-amz-json-1.0", `{"DashboardName":"gone"}`, map[string]string{
		"X-Amz-Target":      "GraniteServiceVersion20100801.GetDashboard",
		"X-Amzn-Query-Mode": "true",
	})
	defer js.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusNotFound, js.StatusCode)
	assert.Contains(t, string(cwReadBytes(t, js)), `"__type":"com.amazonaws.cloudwatch#DashboardNotFoundError"`)
	assert.Equal(t, "ResourceNotFound;Sender", js.Header.Get("x-amzn-query-error"))

	req, err := emulator.CBOREncodeForTest(emulator.CBORMapForTest(emulator.CBORPairForTest{Key: "DashboardName", Value: "gone"}))
	require.NoError(t, err)
	cb := cwPost(t, srv, "/service/GraniteServiceVersion20100801/operation/GetDashboard", "application/cbor", string(req),
		map[string]string{"Smithy-Protocol": "rpc-v2-cbor", "Accept": "application/cbor", "X-Amzn-Query-Mode": "true"})
	defer cb.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusNotFound, cb.StatusCode)
	decoded, err := emulator.CBORDecodeForTest(cwReadBytes(t, cb))
	require.NoError(t, err)
	doc, _ := decoded.(map[string]any)
	assert.Equal(t, "com.amazonaws.cloudwatch#DashboardNotFoundError", doc["__type"])
	assert.Equal(t, "Dashboard gone does not exist", doc["message"])
	assert.Equal(t, "ResourceNotFound;Sender", cb.Header.Get("x-amzn-query-error"))
}

// TestCWDashboards_QueryProtocol drives a put and a read over the query protocol, where the body
// travels as a form value and the result is wrapped in <{Op}Result>.
func TestCWDashboards_QueryProtocol(t *testing.T) {
	t.Parallel()
	srv := newCWAlarmTestServer(t)

	put := cwRequest(t, srv, map[string]string{"Action": "PutDashboard", "Version": "2010-08-01",
		"DashboardName": "q", "DashboardBody": cwDashboardBody})
	defer put.Body.Close() //nolint:errcheck
	require.Equal(t, http.StatusOK, put.StatusCode)
	assert.Contains(t, cwReadBody(t, put), "<PutDashboardResult><DashboardValidationMessages></DashboardValidationMessages></PutDashboardResult>")

	list := cwRequest(t, srv, map[string]string{"Action": "ListDashboards", "Version": "2010-08-01"})
	defer list.Body.Close() //nolint:errcheck
	body := cwReadBody(t, list)
	assert.Contains(t, body, "<DashboardEntries><member><DashboardName>q</DashboardName>")
	assert.Contains(t, body, fmt.Sprintf("<Size>%d</Size>", len(cwDashboardBody)))

	del := cwRequest(t, srv, map[string]string{"Action": "DeleteDashboards", "Version": "2010-08-01", "DashboardNames.member.1": "q"})
	defer del.Body.Close() //nolint:errcheck
	assert.Equal(t, http.StatusOK, del.StatusCode)
	assert.Contains(t, cwReadBody(t, del), "<DeleteDashboardsResponse")
}

// TestCWDashboards_AStoreFaultIsAnError pins that a store fault is an error, never a dashboard, an
// absence or a success.
func TestCWDashboards_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, op string
		body     map[string]any
		arm      func(*cfFaultStateManager)
	}{
		{"PutDashboard, read", "PutDashboard", map[string]any{"DashboardName": "f", "DashboardBody": `{}`}, func(m *cfFaultStateManager) { m.failGet = "dashboard:" }},
		{"PutDashboard, write", "PutDashboard", map[string]any{"DashboardName": "f", "DashboardBody": `{}`}, func(m *cfFaultStateManager) { m.failPut = "dashboard:" }},
		{"GetDashboard, corrupt record", "GetDashboard", map[string]any{"DashboardName": "f"}, func(m *cfFaultStateManager) { m.corruptGet = "dashboard:" }},
		{"ListDashboards, read", "ListDashboards", map[string]any{}, func(m *cfFaultStateManager) { m.failGet = "dashboard:" }},
		{"DeleteDashboards, read", "DeleteDashboards", map[string]any{"DashboardNames": []string{"f"}}, func(m *cfFaultStateManager) { m.failGet = "dashboard:" }},
		{"DeleteDashboards, delete", "DeleteDashboards", map[string]any{"DashboardNames": []string{"f"}}, func(m *cfFaultStateManager) { m.failDelete = "dashboard:" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			e := newCWEnvWithState(t, fault, nil)
			e.ok("PutDashboard", map[string]any{"DashboardName": "f", "DashboardBody": `{}`})
			tc.arm(fault)
			status, out := e.do(tc.op, tc.body)
			require.GreaterOrEqualf(t, status, http.StatusInternalServerError, "%s: %v", tc.name, out)
		})
	}
}
