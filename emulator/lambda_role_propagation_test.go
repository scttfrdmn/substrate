package emulator_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// A seedable IAM role propagation window, and AddPermission's string Statement (#1274 item 5, #1382).
//
// Every request goes over the wire through a TestServer, so the seed is the recorded control-plane
// write a replay re-issues, and the refusal is asserted on the bytes a typed SDK would decode.

const (
	lrpRoleA = "arn:aws:iam::123456789012:role/deployer-a"
	lrpRoleB = "arn:aws:iam::123456789012:role/deployer-b"
)

// lrpServer starts a TestServer on a frozen clock.
func lrpServer(t *testing.T, opts ...emulator.TestServerOption) *emulator.TestServer {
	t.Helper()
	ts := emulator.StartTestServer(t, opts...)
	ts.FreezeTimeAt(time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC))
	return ts
}

// lrpCall issues one Lambda REST request and answers its status and body.
func lrpCall(t *testing.T, ts *emulator.TestServer, method, path string, body any) (int, []byte) {
	t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		require.NoError(t, err)
	}
	req, err := http.NewRequestWithContext(t.Context(), method, ts.URL+path, bytes.NewReader(raw))
	require.NoError(t, err)
	req.Host = "lambda.us-east-1.amazonaws.com"
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, out
}

// lrpSeed POSTs a role-propagation seed.
func lrpSeed(t *testing.T, ts *emulator.TestServer, body string) (int, []byte) {
	t.Helper()
	resp, err := http.Post(ts.URL+"/v1/lambda/role-propagation", "application/json", strings.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close() //nolint:errcheck
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, out
}

// lrpCreate attempts CreateFunction naming role.
func lrpCreate(t *testing.T, ts *emulator.TestServer, name, role string) (int, []byte) {
	t.Helper()
	return lrpCall(t, ts, http.MethodPost, "/2015-03-31/functions", map[string]any{
		"FunctionName": name, "Runtime": "python3.12", "Handler": "index.handler", "Role": role,
		"Code": map[string]string{"ZipFile": "UEsDBA=="},
	})
}

// lrpRequireNotAssumable requires the observed refusal, on the raw bytes.
func lrpRequireNotAssumable(t *testing.T, status int, out []byte, what string) {
	t.Helper()
	require.Equal(t, http.StatusBadRequest, status, "%s: %s", what, out)
	assert.Contains(t, string(out), "InvalidParameterValueException", "%s: %s", what, out)
	assert.Contains(t, string(out), "The role defined for the function cannot be assumed by Lambda.", "%s: %s", what, out)
}

func TestLambdaRolePropagation_AnUnseededRoleIsAssumableAtOnce(t *testing.T) {
	t.Parallel()
	ts := lrpServer(t)
	status, out := lrpCreate(t, ts, "plain", lrpRoleA)
	require.Equal(t, http.StatusCreated, status, "%s", out)
}

// A seed refuses its count of attempts naming the role, then admits the next.
func TestLambdaRolePropagation_ASeededWindowRefusesItsCountThenAdmits(t *testing.T) {
	t.Parallel()
	ts := lrpServer(t)
	status, out := lrpSeed(t, ts, `{"roleArn":"`+lrpRoleA+`","refusedAttempts":2}`)
	require.Equal(t, http.StatusOK, status, "%s", out)

	status, out = lrpCreate(t, ts, "fn", lrpRoleA)
	lrpRequireNotAssumable(t, status, out, "first attempt")
	status, out = lrpCreate(t, ts, "fn", lrpRoleA)
	lrpRequireNotAssumable(t, status, out, "second attempt")
	status, out = lrpCreate(t, ts, "fn", lrpRoleA)
	require.Equal(t, http.StatusCreated, status, "the third attempt is past the window: %s", out)

	// Another role is not governed by an ARN-keyed seed.
	status, out = lrpCreate(t, ts, "other", lrpRoleB)
	require.Equal(t, http.StatusCreated, status, "%s", out)
}

// A wildcard seed gives every role its own window (#582), so one role's attempts do not spend
// another's.
func TestLambdaRolePropagation_AWildcardCountsEachRoleSeparately(t *testing.T) {
	t.Parallel()
	ts := lrpServer(t)
	status, out := lrpSeed(t, ts, `{"refusedAttempts":1}`)
	require.Equal(t, http.StatusOK, status, "%s", out)

	status, out = lrpCreate(t, ts, "a", lrpRoleA)
	lrpRequireNotAssumable(t, status, out, "role A's first attempt")
	status, out = lrpCreate(t, ts, "b", lrpRoleB)
	lrpRequireNotAssumable(t, status, out, "role B's first attempt is its own")
	status, out = lrpCreate(t, ts, "a", lrpRoleA)
	require.Equal(t, http.StatusCreated, status, "%s", out)
	status, out = lrpCreate(t, ts, "b", lrpRoleB)
	require.Equal(t, http.StatusCreated, status, "%s", out)
}

// UpdateFunctionConfiguration naming a new Role meets the same window; one naming no Role spends
// nothing.
func TestLambdaRolePropagation_AnUpdateToANewRoleMeetsTheWindow(t *testing.T) {
	t.Parallel()
	ts := lrpServer(t)
	status, out := lrpCreate(t, ts, "fn", lrpRoleA)
	require.Equal(t, http.StatusCreated, status, "%s", out)
	status, out = lrpSeed(t, ts, `{"roleArn":"`+lrpRoleB+`","refusedAttempts":1}`)
	require.Equal(t, http.StatusOK, status, "%s", out)

	status, out = lrpCall(t, ts, http.MethodPut, "/2015-03-31/functions/fn/configuration", map[string]any{"Description": "no role"})
	require.Equal(t, http.StatusOK, status, "an update naming no Role assumes nothing: %s", out)
	status, out = lrpCall(t, ts, http.MethodPut, "/2015-03-31/functions/fn/configuration", map[string]any{"Role": lrpRoleB})
	lrpRequireNotAssumable(t, status, out, "an update to a role in its window")
	status, out = lrpCall(t, ts, http.MethodPut, "/2015-03-31/functions/fn/configuration", map[string]any{"Role": lrpRoleB})
	require.Equal(t, http.StatusOK, status, "%s", out)
	assert.Contains(t, string(out), `"Role":"`+lrpRoleB+`"`, "%s", out)
}

// A create refused for another reason spends no attempt of the window.
func TestLambdaRolePropagation_AnotherRefusalSpendsNoAttempt(t *testing.T) {
	t.Parallel()
	ts := lrpServer(t)
	status, out := lrpCreate(t, ts, "taken", lrpRoleB)
	require.Equal(t, http.StatusCreated, status, "%s", out)
	status, out = lrpSeed(t, ts, `{"roleArn":"`+lrpRoleB+`","refusedAttempts":1}`)
	require.Equal(t, http.StatusOK, status, "%s", out)

	status, out = lrpCreate(t, ts, "taken", lrpRoleB)
	require.Equal(t, http.StatusConflict, status, "a duplicate name is the conflict, not the window: %s", out)
	status, out = lrpCreate(t, ts, "fresh", lrpRoleB)
	lrpRequireNotAssumable(t, status, out, "the window's one attempt is still unspent")
}

func TestLambdaRolePropagation_TheSeedEndpointValidatesAndClears(t *testing.T) {
	t.Parallel()
	ts := lrpServer(t)
	for _, body := range []string{
		`{"roleArn":"arn:aws:s3:::bucket","refusedAttempts":1}`,
		`{"roleArn":"not-an-arn","refusedAttempts":1}`,
		`{"refusedAttempts":-1}`,
		`{`,
	} {
		status, out := lrpSeed(t, ts, body)
		assert.Equal(t, http.StatusBadRequest, status, "%s must be refused: %s", body, out)
	}

	status, out := lrpSeed(t, ts, `{"roleArn":"`+lrpRoleA+`","refusedAttempts":3}`)
	require.Equal(t, http.StatusOK, status, "%s", out)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, ts.URL+"/v1/lambda/role-propagation?roleArn="+lrpRoleA, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode)
	status, out = lrpCreate(t, ts, "cleared", lrpRoleA)
	require.Equal(t, http.StatusCreated, status, "a cleared seed opens no window: %s", out)
}

// API_AddPermission publishes Statement as a string holding the statement's JSON, and API_GetPolicy
// publishes Policy the same way (#1382).
func TestLambdaPermission_StatementAndPolicyAreJSONStrings(t *testing.T) {
	t.Parallel()
	ts := lrpServer(t)
	status, out := lrpCreate(t, ts, "perm", lrpRoleA)
	require.Equal(t, http.StatusCreated, status, "%s", out)

	status, out = lrpCall(t, ts, http.MethodPost, "/2015-03-31/functions/perm/policy", map[string]any{
		"StatementId": "s1", "Action": "lambda:InvokeFunction", "Principal": "s3.amazonaws.com",
	})
	require.Equal(t, http.StatusCreated, status, "AddPermission: %s", out)
	var added map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out, &added), "%s", out)
	var statement string
	require.NoErrorf(t, json.Unmarshal(added["Statement"], &statement), "Statement must be a JSON string: %s", out)
	var stmt map[string]any
	require.NoErrorf(t, json.Unmarshal([]byte(statement), &stmt), "the string must hold the statement document: %s", statement)
	assert.Equal(t, "s1", stmt["Sid"], "%s", statement)

	status, out = lrpCall(t, ts, http.MethodGet, "/2015-03-31/functions/perm/policy", nil)
	require.Equal(t, http.StatusOK, status, "GetPolicy: %s", out)
	var got map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out, &got), "%s", out)
	var policy string
	require.NoErrorf(t, json.Unmarshal(got["Policy"], &policy), "Policy must be a JSON string: %s", out)
	assert.Contains(t, policy, `"Sid":"s1"`, "%s", policy)
}

// The seed is a control-plane write recorded in the stream; a replay that re-applies it reproduces
// the recorded refusals, and one that withholds it does not.
func TestLambdaRolePropagation_AReplayReproducesTheWindow(t *testing.T) {
	t.Parallel()
	ts := lrpServer(t, emulator.WithRecordedBodies())
	status, out := lrpSeed(t, ts, `{"roleArn":"`+lrpRoleA+`","refusedAttempts":1}`)
	require.Equal(t, http.StatusOK, status, "%s", out)
	status, out = lrpCreate(t, ts, "rep", lrpRoleA)
	lrpRequireNotAssumable(t, status, out, "the recorded refusal")
	status, out = lrpCreate(t, ts, "rep", lrpRoleA)
	require.Equal(t, http.StatusCreated, status, "%s", out)

	withSeed := emulator.NewReplayEngine(ts.Store(), ts.StateManager(), ts.TimeController(), ts.Registry(),
		emulator.ReplayConfig{}, emulator.NewDefaultLogger(slog.LevelError, false),
		emulator.WithControlPlaneHandler(ts.ControlPlaneHandler()))
	results, err := withSeed.Replay(t.Context(), "default")
	require.NoError(t, err)
	assert.Equal(t, 1, results.FailedEvents, "the refused create refused in the recording too")
	assert.Empty(t, results.Differences, "the replay reproduces the window: %s", replayDifferenceSummary(results))

	withoutSeed := emulator.NewReplayEngine(ts.Store(), ts.StateManager(), ts.TimeController(), ts.Registry(),
		emulator.ReplayConfig{}, emulator.NewDefaultLogger(slog.LevelError, false))
	results, err = withoutSeed.Replay(t.Context(), "default")
	require.NoError(t, err)
	assert.Equal(t, 1, results.SkippedEvents, "the seed is the one event withheld")
	assert.NotEmpty(t, results.Differences, "without the seed the first create is not refused")
}

// A store fault on the window's seed or counter keys is an error, never the published refusal and
// never an admission.
func TestLambdaRolePropagation_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
	}{
		{"seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }},
		{"corrupt seed", func(m *cfFaultStateManager) { m.corruptGet = "status:" }},
		{"counter read", func(m *cfFaultStateManager) { m.failGet = "observed:" }},
		{"counter write", func(m *cfFaultStateManager) { m.failPut = "observed:" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			p := &emulator.LambdaPlugin{}
			clock := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
			clk := emulator.NewTimeController(clock)
			clk.Freeze()
			clk.SetTime(clock)
			require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
				State: fault, Logger: emulator.NewDefaultLogger(slog.LevelError, false),
				Options: map[string]any{"time_controller": clk},
			}))
			ctx := &emulator.RequestContext{
				AccountID: "123456789012", Region: "us-east-1", RequestID: "req-lrp-fault", IDs: emulator.NewIDMint("req-lrp-fault"),
			}
			seed, err := json.Marshal(map[string]any{"roleArn": lrpRoleA, "refusedAttempts": 1})
			require.NoError(t, err)
			require.NoError(t, fault.Put(t.Context(), "lambda-role-propagation-ctrl", "status:"+lrpRoleA, seed))

			tc.arm(fault)
			body, err := json.Marshal(map[string]any{
				"FunctionName": "f", "Runtime": "python3.12", "Handler": "index.handler", "Role": lrpRoleA,
				"Code": map[string]string{"ZipFile": "UEsDBA=="},
			})
			require.NoError(t, err)
			_, err = p.HandleRequest(ctx, &emulator.AWSRequest{
				Service: "lambda", HTTPMethod: http.MethodPost, Path: "/2015-03-31/functions", Body: body,
				Headers: map[string]string{"Content-Type": "application/json"}, Params: map[string]string{},
			})
			require.Error(t, err, "%s must fail", tc.name)
			var awsErr *emulator.AWSError
			assert.Falsef(t, errors.As(err, &awsErr), "%s answered the published %v", tc.name, awsErr)
		})
	}
}
