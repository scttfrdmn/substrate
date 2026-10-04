package emulator_test

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// UpdateRoute and UpdateStage (#1279). The assertions read the raw response members, because what a
// converging caller reads back is the published shape, and a decode into substrate's record would
// hide a member renamed or dropped on the way out.

// apigwv2Update is a harness over one API with one route and one stage.
type apigwv2Update struct {
	t       *testing.T
	p       *emulator.APIGatewayV2Plugin
	ctx     *emulator.RequestContext
	apiID   string
	routeID string
}

// call issues one request and answers its status and body, or the refusal's status and code.
func (h *apigwv2Update) call(method, path, body string) (int, string) {
	h.t.Helper()
	resp, err := h.p.HandleRequest(h.ctx, &emulator.AWSRequest{
		Service: "apigatewayv2", HTTPMethod: method, Path: path, Body: []byte(body),
		Headers: map[string]string{"Content-Type": "application/json"}, Params: map[string]string{},
	})
	var awsErr *emulator.AWSError
	if errors.As(err, &awsErr) {
		return awsErr.HTTPStatus, awsErr.Code
	}
	require.NoError(h.t, err, "%s %s", method, path)
	return resp.StatusCode, string(resp.Body)
}

// members decodes a response body into its top-level members, as raw JSON.
func (h *apigwv2Update) members(body string) map[string]json.RawMessage {
	h.t.Helper()
	var m map[string]json.RawMessage
	require.NoError(h.t, json.Unmarshal([]byte(body), &m), "decode %s", body)
	return m
}

func newAPIGWv2Update(t *testing.T, state emulator.StateManager) *apigwv2Update {
	t.Helper()
	clock := time.Unix(1700000000, 0).UTC()
	tc := emulator.NewTimeController(clock)
	tc.Freeze()
	tc.SetTime(clock)
	h := &apigwv2Update{t: t, p: &emulator.APIGatewayV2Plugin{}}
	require.NoError(t, h.p.Initialize(t.Context(), emulator.PluginConfig{
		State: state, Logger: emulator.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}), "Initialize")
	h.ctx = &emulator.RequestContext{
		AccountID: "123456789012", Region: "us-east-1", RequestID: "req-apigwv2-update", IDs: emulator.NewIDMint("req-apigwv2-update"),
	}

	status, body := h.call(http.MethodPost, "/v2/apis", `{"name":"converge","protocolType":"HTTP"}`)
	require.Equal(t, http.StatusCreated, status, "CreateApi: %s", body)
	require.NoError(t, json.Unmarshal(h.members(body)["apiId"], &h.apiID))

	status, body = h.call(http.MethodPost, "/v2/apis/"+h.apiID+"/routes",
		`{"routeKey":"GET /pets","target":"integrations/old","authorizationType":"JWT","authorizerId":"auth1"}`)
	require.Equal(t, http.StatusCreated, status, "CreateRoute: %s", body)
	require.NoError(t, json.Unmarshal(h.members(body)["routeId"], &h.routeID))

	status, body = h.call(http.MethodPost, "/v2/apis/"+h.apiID+"/stages",
		`{"stageName":"$default","description":"first","stageVariables":{"env":"dev"}}`)
	require.Equal(t, http.StatusCreated, status, "CreateStage: %s", body)
	return h
}

func (h *apigwv2Update) routePath() string { return "/v2/apis/" + h.apiID + "/routes/" + h.routeID }
func (h *apigwv2Update) stagePath() string { return "/v2/apis/" + h.apiID + "/stages/$default" }

func TestAPIGatewayV2_UpdateRoute_PatchesWhatTheBodyNames(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		body string
		want map[string]string // member → raw JSON value; "" asserts the member is absent
	}{
		{"retarget only", `{"target":"integrations/new"}`, map[string]string{
			"target": `"integrations/new"`, "routeKey": `"GET /pets"`, "authorizationType": `"JWT"`, "authorizerId": `"auth1"`}},
		{"every modeled member", `{"routeKey":"POST /pets","target":"integrations/b","authorizationType":"NONE","authorizerId":"auth2"}`, map[string]string{
			"target": `"integrations/b"`, "routeKey": `"POST /pets"`, "authorizationType": `"NONE"`, "authorizerId": `"auth2"`}},
		// A present empty value replaces: this is how a caller detaches an authorizer.
		{"clear the authorizer", `{"authorizationType":"NONE","authorizerId":""}`, map[string]string{
			"authorizationType": `"NONE"`, "authorizerId": "", "target": `"integrations/old"`}},
		{"an empty body changes nothing", ``, map[string]string{
			"target": `"integrations/old"`, "routeKey": `"GET /pets"`, "authorizerId": `"auth1"`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newAPIGWv2Update(t, emulator.NewMemoryStateManager())
			status, body := h.call(http.MethodPatch, h.routePath(), tc.body)
			require.Equal(t, http.StatusOK, status, "UpdateRoute: %s", body)

			// The response is the full updated shape, and GetRoute reads back the same.
			_, got := h.call(http.MethodGet, h.routePath(), "")
			for name, body := range map[string]string{"UpdateRoute": body, "GetRoute": got} {
				m := h.members(body)
				require.JSONEqf(t, `"`+h.routeID+`"`, string(m["routeId"]), "%s: %s", name, body)
				for member, want := range tc.want {
					if want == "" {
						require.NotContainsf(t, m, member, "%s must not answer %s: %s", name, member, body)
						continue
					}
					require.JSONEqf(t, want, string(m[member]), "%s %s: %s", name, member, body)
				}
			}
		})
	}
}

func TestAPIGatewayV2_UpdateStage_PatchesWhatTheBodyNames(t *testing.T) {
	t.Parallel()
	h := newAPIGWv2Update(t, emulator.NewMemoryStateManager())

	status, body := h.call(http.MethodPatch, h.stagePath(), `{
		"autoDeploy": true,
		"accessLogSettings": {"destinationArn": "arn:aws:logs:us-east-1:123456789012:log-group:access", "format": "$context.requestId"},
		"defaultRouteSettings": {"detailedMetricsEnabled": true, "throttlingBurstLimit": 50, "throttlingRateLimit": 12.5}
	}`)
	require.Equal(t, http.StatusOK, status, "UpdateStage: %s", body)
	_, got := h.call(http.MethodGet, h.stagePath(), "")
	for name, body := range map[string]string{"UpdateStage": body, "GetStage": got} {
		m := h.members(body)
		require.JSONEqf(t, `"$default"`, string(m["stageName"]), "%s: %s", name, body)
		require.JSONEqf(t, "true", string(m["autoDeploy"]), "%s: %s", name, body)
		require.JSONEqf(t, `{"destinationArn":"arn:aws:logs:us-east-1:123456789012:log-group:access","format":"$context.requestId"}`,
			string(m["accessLogSettings"]), "%s: %s", name, body)
		require.JSONEqf(t, `{"detailedMetricsEnabled":true,"throttlingBurstLimit":50,"throttlingRateLimit":12.5}`,
			string(m["defaultRouteSettings"]), "%s: %s", name, body)
		// Members the body did not name are left as they were.
		require.JSONEqf(t, `"first"`, string(m["description"]), "%s: %s", name, body)
		require.JSONEqf(t, `{"env":"dev"}`, string(m["stageVariables"]), "%s: %s", name, body)
		require.JSONEqf(t, `"2023-11-14T22:13:20Z"`, string(m["lastUpdatedDate"]), "%s: %s", name, body)
	}

	// A second update names the description and variables and turns auto-deploy off: present false
	// replaces, and the log settings it does not name stay.
	status, body = h.call(http.MethodPatch, h.stagePath(), `{"description":"second","stageVariables":{"env":"prod"},"autoDeploy":false}`)
	require.Equal(t, http.StatusOK, status, "UpdateStage: %s", body)
	m := h.members(body)
	require.JSONEq(t, `"second"`, string(m["description"]), "%s", body)
	require.JSONEq(t, `{"env":"prod"}`, string(m["stageVariables"]), "%s", body)
	require.NotContains(t, m, "autoDeploy", "autoDeploy false is the default and is not rendered: %s", body)
	require.Contains(t, string(m["accessLogSettings"]), "log-group:access", "%s", body)
}

func TestAPIGatewayV2_Update_RefusesWhatThePagePublishes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, path, body, code string
		status                 int
	}{
		{"UpdateRoute, unknown API", "/v2/apis/nope/routes/{route}", `{"target":"x"}`, "NotFoundException", http.StatusNotFound},
		{"UpdateRoute, unknown route", "/v2/apis/{api}/routes/nope", `{"target":"x"}`, "NotFoundException", http.StatusNotFound},
		{"UpdateRoute, malformed body", "/v2/apis/{api}/routes/{route}", `{"target":`, "BadRequestException", http.StatusBadRequest},
		{"UpdateStage, unknown API", "/v2/apis/nope/stages/$default", `{"autoDeploy":true}`, "NotFoundException", http.StatusNotFound},
		{"UpdateStage, unknown stage", "/v2/apis/{api}/stages/nope", `{"autoDeploy":true}`, "NotFoundException", http.StatusNotFound},
		{"UpdateStage, malformed body", "/v2/apis/{api}/stages/$default", `{"autoDeploy":"yes"}`, "BadRequestException", http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newAPIGWv2Update(t, emulator.NewMemoryStateManager())
			path := strings.NewReplacer("{api}", h.apiID, "{route}", h.routeID).Replace(tc.path)
			status, code := h.call(http.MethodPatch, path, tc.body)
			require.Equal(t, tc.status, status, "%s: %s", tc.name, code)
			require.Equal(t, tc.code, code, "%s", tc.name)
		})
	}
}

// A store fault at any read or write an update makes is an error, never answered as an update.
func TestAPIGatewayV2_Update_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		arm   func(m *cfFaultStateManager)
		route bool
	}{
		{"UpdateRoute, API read", func(m *cfFaultStateManager) { m.failGet = "apiv2:" }, true},
		{"UpdateRoute, route read", func(m *cfFaultStateManager) { m.failGet = "routev2:" }, true},
		{"UpdateRoute, corrupt route", func(m *cfFaultStateManager) { m.corruptGet = "routev2:" }, true},
		{"UpdateRoute, write", func(m *cfFaultStateManager) { m.failPut = "routev2:" }, true},
		{"UpdateStage, API read", func(m *cfFaultStateManager) { m.failGet = "apiv2:" }, false},
		{"UpdateStage, stage read", func(m *cfFaultStateManager) { m.failGet = "stagev2:" }, false},
		{"UpdateStage, corrupt stage", func(m *cfFaultStateManager) { m.corruptGet = "stagev2:" }, false},
		{"UpdateStage, write", func(m *cfFaultStateManager) { m.failPut = "stagev2:" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			h := newAPIGWv2Update(t, fault)
			path := h.stagePath()
			if tc.route {
				path = h.routePath()
			}
			tc.arm(fault)
			_, err := h.p.HandleRequest(h.ctx, &emulator.AWSRequest{
				Service: "apigatewayv2", HTTPMethod: http.MethodPatch, Path: path, Body: []byte(`{"description":"x","target":"y"}`),
				Headers: map[string]string{}, Params: map[string]string{},
			})
			require.Error(t, err, "%s must fail on a store fault", tc.name)
			var awsErr *emulator.AWSError
			require.Falsef(t, errors.As(err, &awsErr), "%s answered a store fault as the published %v", tc.name, awsErr)
		})
	}
}
