package emulator_test

import (
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// EMR Serverless's share of the batch audits: required members (#1197), pagination and filters
// (#1195), the published state spelling (#1198) and the published members (#1199). Every assertion
// is on the raw response bytes, because a typed decoder hides an absent or extra member.

// emrAuditRole is an execution role ARN in the published pattern.
const emrAuditRole = "arn:aws:iam::123456789012:role/emr-audit"

// emrAuditClock is the instant every fixture starts at; the clock is frozen.
var emrAuditClock = time.Unix(1700000000, 0).UTC()

// emrAudit is an EMR Serverless plugin over a given store, on a frozen clock a test can step.
type emrAudit struct {
	t   *testing.T
	p   *emulator.EMRServerlessPlugin
	ctx *emulator.RequestContext
	tc  *emulator.TimeController
}

func newEMRAudit(t *testing.T, state emulator.StateManager) *emrAudit {
	t.Helper()
	tc := emulator.NewTimeController(emrAuditClock)
	tc.Freeze()
	tc.SetTime(emrAuditClock)
	p := &emulator.EMRServerlessPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}))
	return &emrAudit{t: t, p: p, tc: tc, ctx: &emulator.RequestContext{
		AccountID: "123456789012", Region: "us-east-1", RequestID: "req-emr-audit", IDs: emulator.NewIDMint("req-emr-audit"),
	}}
}

// call issues one request and answers its status and body, or the refusal's status and code.
func (a *emrAudit) call(method, path string, body any, params map[string]string, multi map[string][]string) (int, string) {
	a.t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		require.NoError(a.t, err)
	}
	if params == nil {
		params = map[string]string{}
	}
	resp, err := a.p.HandleRequest(a.ctx, &emulator.AWSRequest{
		Service: "emr-serverless", HTTPMethod: method, Path: path, Body: raw,
		Headers: map[string]string{"Content-Type": "application/json"}, Params: params, MultiValueParams: multi,
	})
	var awsErr *emulator.AWSError
	if errors.As(err, &awsErr) {
		return awsErr.HTTPStatus, awsErr.Code
	}
	require.NoError(a.t, err, "%s %s", method, path)
	return resp.StatusCode, string(resp.Body)
}

// ok issues one request and requires a 200.
func (a *emrAudit) ok(method, path string, body any, params map[string]string) map[string]json.RawMessage {
	a.t.Helper()
	status, out := a.call(method, path, body, params, nil)
	require.Equal(a.t, http.StatusOK, status, "%s %s: %s", method, path, out)
	var doc map[string]json.RawMessage
	require.NoError(a.t, json.Unmarshal([]byte(out), &doc), "%s %s: %s", method, path, out)
	return doc
}

func (a *emrAudit) createApp(token string) string {
	a.t.Helper()
	doc := a.ok(http.MethodPost, "/applications", map[string]any{
		"clientToken": token, "name": "audit-" + token, "releaseLabel": "emr-7.0.0", "type": "SPARK",
	}, nil)
	return emrAuditString(a.t, doc["applicationId"])
}

func (a *emrAudit) startRun(appID, token string, extra map[string]any) string {
	a.t.Helper()
	body := map[string]any{"clientToken": token, "executionRoleArn": emrAuditRole}
	maps.Copy(body, extra)
	doc := a.ok(http.MethodPost, "/applications/"+appID+"/jobruns", body, nil)
	return emrAuditString(a.t, doc["jobRunId"])
}

func emrAuditString(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var s string
	require.NoError(t, json.Unmarshal(raw, &s), "%s", raw)
	return s
}

// emrAuditKeys answers the sorted member names of a raw JSON object.
func emrAuditKeys(t *testing.T, raw json.RawMessage) []string {
	t.Helper()
	var obj map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &obj), "%s", raw)
	return slices.Sorted(maps.Keys(obj))
}

func TestEMRServerlessAudit_RequiredMembersAreRefusedNotDefaulted(t *testing.T) {
	t.Parallel()
	valid := func(over map[string]any) map[string]any {
		b := map[string]any{"clientToken": "t", "releaseLabel": "emr-7.0.0", "type": "SPARK"}
		for k, v := range over {
			if v == nil {
				delete(b, k)
			} else {
				b[k] = v
			}
		}
		return b
	}
	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{"no clientToken", valid(map[string]any{"clientToken": nil})},
		{"clientToken outside its pattern", valid(map[string]any{"clientToken": "has space"})},
		{"clientToken over 64", valid(map[string]any{"clientToken": strings.Repeat("a", 65)})},
		{"no releaseLabel", valid(map[string]any{"releaseLabel": nil})},
		{"releaseLabel outside its pattern", valid(map[string]any{"releaseLabel": "emr 7"})},
		{"no type", valid(map[string]any{"type": nil})},
		{"type over 64", valid(map[string]any{"type": strings.Repeat("S", 65)})},
		{"name outside its pattern", valid(map[string]any{"name": "has space"})},
		{"architecture outside its values", valid(map[string]any{"architecture": "RISCV"})},
	} {
		t.Run("CreateApplication/"+tc.name, func(t *testing.T) {
			t.Parallel()
			a := newEMRAudit(t, emulator.NewMemoryStateManager())
			status, code := a.call(http.MethodPost, "/applications", tc.body, nil, nil)
			require.Equal(t, http.StatusBadRequest, status, "%s", code)
			require.Equal(t, "ValidationException", code)
		})
	}

	run := func(over map[string]any) map[string]any {
		b := map[string]any{"clientToken": "t", "executionRoleArn": emrAuditRole}
		for k, v := range over {
			if v == nil {
				delete(b, k)
			} else {
				b[k] = v
			}
		}
		return b
	}
	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{"no clientToken", run(map[string]any{"clientToken": nil})},
		{"no executionRoleArn", run(map[string]any{"executionRoleArn": nil})},
		{"executionRoleArn not a role", run(map[string]any{"executionRoleArn": "arn:aws:iam::123456789012:user/someone"})},
		{"blank name", run(map[string]any{"name": "   "})},
		{"mode outside its values", run(map[string]any{"mode": "INTERACTIVE"})},
	} {
		t.Run("StartJobRun/"+tc.name, func(t *testing.T) {
			t.Parallel()
			a := newEMRAudit(t, emulator.NewMemoryStateManager())
			appID := a.createApp("app")
			status, code := a.call(http.MethodPost, "/applications/"+appID+"/jobruns", tc.body, nil, nil)
			require.Equal(t, http.StatusBadRequest, status, "%s", code)
			require.Equal(t, "ValidationException", code)
		})
	}

	t.Run("StartJobRun on an application that does not exist", func(t *testing.T) {
		t.Parallel()
		a := newEMRAudit(t, emulator.NewMemoryStateManager())
		status, code := a.call(http.MethodPost, "/applications/00deadbeef/jobruns", run(nil), nil, nil)
		require.Equal(t, http.StatusNotFound, status, "%s", code)
		require.Equal(t, "ResourceNotFoundException", code)
	})
}

func TestEMRServerlessAudit_AClientTokenIsIdempotent(t *testing.T) {
	t.Parallel()
	a := newEMRAudit(t, emulator.NewMemoryStateManager())
	create := map[string]any{"clientToken": "once", "name": "idem", "releaseLabel": "emr-7.0.0", "type": "SPARK"}

	first := a.ok(http.MethodPost, "/applications", create, nil)
	again := a.ok(http.MethodPost, "/applications", create, nil)
	require.JSONEq(t, string(first["applicationId"]), string(again["applicationId"]),
		"the same token and parameters must answer the application they created")
	require.JSONEq(t, `"idem"`, string(first["name"]), "CreateApplication publishes name in its response")

	changed := maps.Clone(create)
	changed["releaseLabel"] = "emr-6.9.0"
	status, code := a.call(http.MethodPost, "/applications", changed, nil, nil)
	require.Equal(t, http.StatusConflict, status, "%s", code)
	require.Equal(t, "ConflictException", code)

	appID := emrAuditString(t, first["applicationId"])
	start := map[string]any{"clientToken": "run-once", "executionRoleArn": emrAuditRole, "name": "r"}
	r1 := a.ok(http.MethodPost, "/applications/"+appID+"/jobruns", start, nil)
	r2 := a.ok(http.MethodPost, "/applications/"+appID+"/jobruns", start, nil)
	require.JSONEq(t, string(r1["jobRunId"]), string(r2["jobRunId"]))
	listed := a.ok(http.MethodGet, "/applications/"+appID+"/jobruns", nil, nil)
	var runs []json.RawMessage
	require.NoError(t, json.Unmarshal(listed["jobRuns"], &runs))
	require.Len(t, runs, 1, "a resubmitted StartJobRun must not start a second run")

	start["name"] = "different"
	status, code = a.call(http.MethodPost, "/applications/"+appID+"/jobruns", start, nil, nil)
	require.Equal(t, http.StatusConflict, status, "%s", code)
	require.Equal(t, "ConflictException", code)
}

func TestEMRServerlessAudit_ListJobRunsPagesWithATokenThatRoundTrips(t *testing.T) {
	t.Parallel()
	a := newEMRAudit(t, emulator.NewMemoryStateManager())
	appID := a.createApp("pages")
	var want []string
	for i := range 7 {
		// One second apart, so the order the list answers is the order of creation.
		a.tc.SetTime(emrAuditClock.Add(time.Duration(i) * time.Second))
		want = append(want, a.startRun(appID, "run-"+strconv.Itoa(i), nil))
	}

	var got []string
	params := map[string]string{"maxResults": "3"}
	var sizes []int
	for pages := 0; pages < 10; pages++ {
		doc := a.ok(http.MethodGet, "/applications/"+appID+"/jobruns", nil, maps.Clone(params))
		var runs []struct {
			ID string `json:"id"`
		}
		require.NoError(t, json.Unmarshal(doc["jobRuns"], &runs))
		sizes = append(sizes, len(runs))
		for _, r := range runs {
			got = append(got, r.ID)
		}
		tok, ok := doc["nextToken"]
		if !ok {
			break
		}
		token := emrAuditString(t, tok)
		require.NotEmpty(t, token, "a nextToken is never empty: the page publishes a minimum length of 1")
		params["nextToken"] = token
	}
	require.Equal(t, []int{3, 3, 1}, sizes, "seven runs at three a page")
	require.Equal(t, want, got, "every run once, in creation order")

	for _, bad := range []map[string]string{
		{"maxResults": "0"}, {"maxResults": "51"}, {"maxResults": "x"},
		{"nextToken": "bm90LWlzc3VlZA=="}, {"nextToken": "has+plus"},
	} {
		status, code := a.call(http.MethodGet, "/applications/"+appID+"/jobruns", nil, bad, nil)
		require.Equalf(t, http.StatusBadRequest, status, "%v: %s", bad, code)
		require.Equalf(t, "ValidationException", code, "%v", bad)
	}
}

func TestEMRServerlessAudit_ListJobRunsFiltersNarrow(t *testing.T) {
	t.Parallel()
	a := newEMRAudit(t, emulator.NewMemoryStateManager())
	appID := a.createApp("filters")
	batch := a.startRun(appID, "batch", nil)
	a.tc.SetTime(emrAuditClock.Add(time.Hour))
	streaming := a.startRun(appID, "streaming", map[string]any{"mode": "STREAMING"})
	a.tc.SetTime(emrAuditClock.Add(2 * time.Hour))
	cancelled := a.startRun(appID, "cancelled", nil)
	a.ok(http.MethodDelete, "/applications/"+appID+"/jobruns/"+cancelled, nil, nil)

	ids := func(params map[string]string, multi map[string][]string) []string {
		t.Helper()
		status, out := a.call(http.MethodGet, "/applications/"+appID+"/jobruns", nil, params, multi)
		require.Equal(t, http.StatusOK, status, "%s", out)
		var doc struct {
			JobRuns []struct {
				ID string `json:"id"`
			} `json:"jobRuns"`
		}
		require.NoError(t, json.Unmarshal([]byte(out), &doc))
		var got []string
		for _, r := range doc.JobRuns {
			got = append(got, r.ID)
		}
		return got
	}

	require.Equal(t, []string{cancelled}, ids(map[string]string{"states": "CANCELLED"}, nil))
	require.Equal(t, []string{cancelled, batch, streaming},
		ids(map[string]string{"states": "CANCELLED"}, map[string][]string{"states": {"CANCELLED", "SUCCESS"}}),
		"several states group the list by state, in the filter's order")
	require.Equal(t, []string{streaming}, ids(map[string]string{"mode": "STREAMING"}, nil))
	require.Equal(t, []string{batch, cancelled}, ids(map[string]string{"mode": "BATCH"}, nil),
		"a run that named no mode is BATCH")
	require.Equal(t, []string{streaming, cancelled},
		ids(map[string]string{"createdAtAfter": emrAuditClock.Add(30 * time.Minute).Format(time.RFC3339)}, nil))
	require.Equal(t, []string{batch, streaming},
		ids(map[string]string{"createdAtBefore": emrAuditClock.Add(90 * time.Minute).Format(time.RFC3339)}, nil))

	for _, bad := range []map[string]string{
		{"states": "CANCELED"}, {"mode": "INTERACTIVE"}, {"createdAtAfter": "yesterday"},
	} {
		status, code := a.call(http.MethodGet, "/applications/"+appID+"/jobruns", nil, bad, nil)
		require.Equalf(t, http.StatusBadRequest, status, "%v: %s", bad, code)
		require.Equalf(t, "ValidationException", code, "%v", bad)
	}
	status, code := a.call(http.MethodGet, "/applications/"+appID+"/jobruns", nil, nil,
		map[string][]string{"states": slices.Repeat([]string{"SUCCESS"}, 9)})
	require.Equal(t, http.StatusBadRequest, status, "nine states exceed the published eight: %s", code)
}

// The repeated `states` key a typed SDK sends survives the HTTP parser, which used to keep only the
// first value of every query parameter.
func TestEMRServerlessAudit_ARepeatedStatesKeyReachesTheHandlerOverTheWire(t *testing.T) {
	t.Parallel()
	ts := newEMRServerlessTestServer(t)
	resp := emrRequest(t, ts, http.MethodPost, "/applications", map[string]string{
		"clientToken": "wire", "releaseLabel": "emr-7.0.0", "type": "SPARK",
	})
	var app struct {
		ApplicationID string `json:"applicationId"`
	}
	require.NoError(t, json.Unmarshal(emrBody(t, resp), &app))
	for i, token := range []string{"a", "b"} {
		r := emrRequest(t, ts, http.MethodPost, "/applications/"+app.ApplicationID+"/jobruns", map[string]string{
			"clientToken": token, "executionRoleArn": emrAuditRole,
		})
		var run struct {
			JobRunID string `json:"jobRunId"`
		}
		require.NoError(t, json.Unmarshal(emrBody(t, r), &run))
		if i == 0 {
			emrBody(t, emrRequest(t, ts, http.MethodDelete, "/applications/"+app.ApplicationID+"/jobruns/"+run.JobRunID, nil))
		}
	}

	r := emrRequest(t, ts, http.MethodGet, "/applications/"+app.ApplicationID+"/jobruns?states=SUCCESS&states=CANCELLED", nil)
	body := emrBody(t, r)
	require.Equal(t, http.StatusOK, r.StatusCode, "%s", body)
	var list struct {
		JobRuns []struct {
			State string `json:"state"`
		} `json:"jobRuns"`
	}
	require.NoError(t, json.Unmarshal(body, &list))
	var states []string
	for _, jr := range list.JobRuns {
		states = append(states, jr.State)
	}
	require.Equal(t, []string{"SUCCESS", "CANCELLED"}, states,
		"both states must be applied, grouped in the order sent: %s", body)
}

func TestEMRServerlessAudit_ACancelledRunReportsThePublishedState(t *testing.T) {
	t.Parallel()
	a := newEMRAudit(t, emulator.NewMemoryStateManager())
	appID := a.createApp("cancel")
	runID := a.startRun(appID, "c", nil)
	a.ok(http.MethodDelete, "/applications/"+appID+"/jobruns/"+runID, nil, nil)

	for op, path := range map[string]string{
		"GetJobRun":   "/applications/" + appID + "/jobruns/" + runID,
		"ListJobRuns": "/applications/" + appID + "/jobruns",
	} {
		_, body := a.call(http.MethodGet, path, nil, nil, nil)
		require.Containsf(t, body, `"state":"CANCELLED"`, "%s must answer the published CANCELLED: %s", op, body)
		require.NotContainsf(t, body, "CANCELED\"", "%s answered the unpublished one-L spelling: %s", op, body)
	}
}

func TestEMRServerlessAudit_EveryRequiredMemberIsAnswered(t *testing.T) {
	t.Parallel()
	a := newEMRAudit(t, emulator.NewMemoryStateManager())
	appID := a.createApp("members")
	runID := a.startRun(appID, "m", map[string]any{
		"name": "m", "jobDriver": map[string]any{"sparkSubmit": map[string]any{"entryPoint": "s3://bucket/main.py"}},
	})

	app := a.ok(http.MethodGet, "/applications/"+appID, nil, nil)
	require.Subset(t, emrAuditKeys(t, app["application"]),
		[]string{"applicationId", "arn", "createdAt", "releaseLabel", "state", "type", "updatedAt"},
		"API_Application's seven Required members: %s", app["application"])

	run := a.ok(http.MethodGet, "/applications/"+appID+"/jobruns/"+runID, nil, nil)
	require.Subset(t, emrAuditKeys(t, run["jobRun"]),
		[]string{"applicationId", "arn", "createdAt", "createdBy", "executionRole", "jobDriver", "jobRunId",
			"releaseLabel", "state", "stateDetails", "updatedAt"},
		"API_JobRun's eleven Required members: %s", run["jobRun"])
	var jr map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(run["jobRun"], &jr))
	require.JSONEq(t, `{"sparkSubmit":{"entryPoint":"s3://bucket/main.py"}}`, string(jr["jobDriver"]),
		"jobDriver is the record of what was submitted")
	require.JSONEq(t, `"`+emrAuditRole+`"`, string(jr["executionRole"]))
	require.JSONEq(t, `"emr-7.0.0"`, string(jr["releaseLabel"]), "a run's release is its application's")
	require.JSONEq(t, `"arn:aws:iam::123456789012:root"`, string(jr["createdBy"]))
	for _, member := range []string{"createdAt", "updatedAt"} {
		require.JSONEqf(t, "1700000000.000", string(jr[member]), "%s is epoch seconds, a JSON number", member)
	}

	list := a.ok(http.MethodGet, "/applications/"+appID+"/jobruns", nil, nil)
	var summaries []json.RawMessage
	require.NoError(t, json.Unmarshal(list["jobRuns"], &summaries))
	require.Len(t, summaries, 1)
	require.Subset(t, emrAuditKeys(t, summaries[0]),
		[]string{"applicationId", "arn", "createdAt", "createdBy", "executionRole", "id", "releaseLabel",
			"state", "stateDetails", "updatedAt"},
		"API_JobRunSummary's ten Required members: %s", summaries[0])
}

// A store fault at any read or write the audited handlers make is an error, never a published
// refusal or a 2xx over a record that was not written.
func TestEMRServerlessAudit_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		arm    func(*cfFaultStateManager)
		method string
		path   string
		body   map[string]any
	}{
		{"CreateApplication token read", func(m *cfFaultStateManager) { m.failGet = "app_token:" }, http.MethodPost, "/applications",
			map[string]any{"clientToken": "new", "releaseLabel": "emr-7.0.0", "type": "SPARK"}},
		{"CreateApplication token write", func(m *cfFaultStateManager) { m.failPut = "app_token:" }, http.MethodPost, "/applications",
			map[string]any{"clientToken": "new", "releaseLabel": "emr-7.0.0", "type": "SPARK"}},
		{"CreateApplication resubmission read", func(m *cfFaultStateManager) { m.failGet = "app:" }, http.MethodPost, "/applications",
			map[string]any{"clientToken": "first", "name": "audit-first", "releaseLabel": "emr-7.0.0", "type": "SPARK"}},
		{"GetApplication read", func(m *cfFaultStateManager) { m.failGet = "app:" }, http.MethodGet, "/applications/{app}", nil},
		{"GetApplication corrupt record", func(m *cfFaultStateManager) { m.corruptGet = "app:" }, http.MethodGet, "/applications/{app}", nil},
		{"StartJobRun application read", func(m *cfFaultStateManager) { m.failGet = "app:" }, http.MethodPost, "/applications/{app}/jobruns",
			map[string]any{"clientToken": "new-run", "executionRoleArn": emrAuditRole}},
		{"StartJobRun token read", func(m *cfFaultStateManager) { m.failGet = "jobrun_token:" }, http.MethodPost, "/applications/{app}/jobruns",
			map[string]any{"clientToken": "new-run", "executionRoleArn": emrAuditRole}},
		{"StartJobRun token write", func(m *cfFaultStateManager) { m.failPut = "jobrun_token:" }, http.MethodPost, "/applications/{app}/jobruns",
			map[string]any{"clientToken": "new-run", "executionRoleArn": emrAuditRole}},
		{"StartJobRun resubmission read", func(m *cfFaultStateManager) { m.failGet = "jobrun:" }, http.MethodPost, "/applications/{app}/jobruns",
			map[string]any{"clientToken": "first-run", "executionRoleArn": emrAuditRole}},
		{"GetJobRun read", func(m *cfFaultStateManager) { m.failGet = "jobrun:" }, http.MethodGet, "/applications/{app}/jobruns/{run}", nil},
		{"GetJobRun corrupt record", func(m *cfFaultStateManager) { m.corruptGet = "jobrun:" }, http.MethodGet, "/applications/{app}/jobruns/{run}", nil},
		{"CancelJobRun read", func(m *cfFaultStateManager) { m.failGet = "jobrun:" }, http.MethodDelete, "/applications/{app}/jobruns/{run}", nil},
		{"CancelJobRun write", func(m *cfFaultStateManager) { m.failPut = "jobrun:" }, http.MethodDelete, "/applications/{app}/jobruns/{run}", nil},
		{"ListJobRuns index read", func(m *cfFaultStateManager) { m.failGet = "jobrun_ids:" }, http.MethodGet, "/applications/{app}/jobruns", nil},
		{"ListJobRuns record read", func(m *cfFaultStateManager) { m.failGet = "jobrun:" }, http.MethodGet, "/applications/{app}/jobruns", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			a := newEMRAudit(t, fault)
			appID := a.createApp("first")
			runID := a.startRun(appID, "first-run", nil)
			path := strings.NewReplacer("{app}", appID, "{run}", runID).Replace(tc.path)

			tc.arm(fault)
			var raw []byte
			if tc.body != nil {
				var err error
				raw, err = json.Marshal(tc.body)
				require.NoError(t, err)
			}
			_, err := a.p.HandleRequest(a.ctx, &emulator.AWSRequest{
				Service: "emr-serverless", HTTPMethod: tc.method, Path: path, Body: raw,
				Headers: map[string]string{}, Params: map[string]string{},
			})
			require.Error(t, err, "%s must fail on a store fault", tc.name)
			var awsErr *emulator.AWSError
			require.Falsef(t, errors.As(err, &awsErr), "%s answered a store fault as the published %v", tc.name, awsErr)
		})
	}
}
