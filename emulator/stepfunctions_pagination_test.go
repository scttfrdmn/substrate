package emulator_test

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// ListStateMachines, ListExecutions and ListActivities refuse a nextToken they never issued with the
// published InvalidToken/400, and the token they do issue walks the listing (#1540).

const sfnPageHost = "states.us-east-1.amazonaws.com"

func sfnPageCall(t *testing.T, ts *emulator.TestServer, op string, body map[string]any) (int, map[string]json.RawMessage) {
	t.Helper()
	status, raw := cicdCall(t, ts, sfnPageHost, "AWSStepFunctions", op, body)
	var out map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &out), "%s: %s", op, raw)
	return status, out
}

// sfnPageFixture is three state machines, three activities and three executions of the first state
// machine, each named so the name order is the creation order.
func sfnPageFixture(t *testing.T, ts *emulator.TestServer) (smArn string) {
	t.Helper()
	for i := range 3 {
		status, out := sfnPageCall(t, ts, "CreateStateMachine", map[string]any{
			"name":       fmt.Sprintf("sm-%d", i),
			"definition": `{"StartAt":"A","States":{"A":{"Type":"Pass","End":true}}}`,
			"roleArn":    "arn:aws:iam::123456789012:role/StepFunctionsRole",
		})
		require.Equal(t, http.StatusOK, status, "%v", out)
		if i == 0 {
			require.NoError(t, json.Unmarshal(out["stateMachineArn"], &smArn))
		}
		status, out = sfnPageCall(t, ts, "CreateActivity", map[string]any{"name": fmt.Sprintf("act-%d", i)})
		require.Equal(t, http.StatusOK, status, "%v", out)
	}
	for i := range 3 {
		status, out := sfnPageCall(t, ts, "StartExecution", map[string]any{"stateMachineArn": smArn, "name": fmt.Sprintf("ex-%d", i)})
		require.Equal(t, http.StatusOK, status, "%v", out)
	}
	return smArn
}

// sfnPageListing is one list operation: its request base, and the member its items arrive in.
type sfnPageListing struct {
	op, items string
	base      map[string]any
}

func sfnPageListings(smArn string) []sfnPageListing {
	return []sfnPageListing{
		{"ListStateMachines", "stateMachines", map[string]any{}},
		{"ListActivities", "activities", map[string]any{}},
		{"ListExecutions", "executions", map[string]any{"stateMachineArn": smArn}},
	}
}

func sfnPageWith(base map[string]any, extra map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range base {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

func sfnPageNames(t *testing.T, out map[string]json.RawMessage, member string) []string {
	t.Helper()
	var items []struct {
		Name string `json:"name"`
	}
	require.NoError(t, json.Unmarshal(out[member], &items))
	names := make([]string, 0, len(items))
	for _, it := range items {
		names = append(names, it.Name)
	}
	return names
}

func sfnPageToken(t *testing.T, out map[string]json.RawMessage) string {
	t.Helper()
	raw, ok := out["nextToken"]
	if !ok {
		return ""
	}
	var token string
	require.NoError(t, json.Unmarshal(raw, &token))
	return token
}

func TestStepFunctionsPaging_AnUnissuedTokenIsInvalidToken(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	smArn := sfnPageFixture(t, ts)

	// A token issued by the state machine listing, to hand to the other two.
	_, first := sfnPageCall(t, ts, "ListStateMachines", map[string]any{"maxResults": 1})
	foreign := sfnPageToken(t, first)
	require.NotEmpty(t, foreign)

	for _, l := range sfnPageListings(smArn) {
		tokens := map[string]string{
			// The pre-#1540 wire form: the next item's bare name.
			"a bare name":         "sm-1",
			"not base64":          "%%%",
			"base64 of nothing":   base64.StdEncoding.EncodeToString([]byte("garbage")),
			"another listing's":   foreign,
			"an empty cursor tag": base64.StdEncoding.EncodeToString([]byte("statemachines\x00")),
		}
		if l.op == "ListStateMachines" {
			delete(tokens, "another listing's")
		}
		for name, token := range tokens {
			t.Run(l.op+", "+name, func(t *testing.T) {
				status, out := sfnPageCall(t, ts, l.op, sfnPageWith(l.base, map[string]any{"nextToken": token}))
				require.Equal(t, http.StatusBadRequest, status, "%v", out)
				var code string
				require.NoError(t, json.Unmarshal(out["__type"], &code))
				assert.Contains(t, code, "InvalidToken")
				assert.JSONEq(t, `"The provided token is not valid."`, string(out["message"]))
			})
		}
	}
}

// The token's shape is checked before any state is read (#887's ordering): with every read faulting,
// an unissued token is still the published InvalidToken rather than a store error.
func TestStepFunctionsPaging_TheTokenIsRefusedBeforeAnyRead(t *testing.T) {
	t.Parallel()
	fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager(), failGet: "/"}
	p := &emulator.StepFunctionsPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State: fault, Logger: emulator.NewDefaultLogger(0, false),
	}))
	ctx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: "req-sfn-token"}
	for _, l := range sfnPageListings("arn:aws:states:us-east-1:123456789012:stateMachine:sm-0") {
		_, err := p.HandleRequest(ctx, sfnRequest(l.op, sfnPageWith(l.base, map[string]any{"nextToken": "sm-1"})))
		var awsErr *emulator.AWSError
		require.Truef(t, errors.As(err, &awsErr), "%s: %v", l.op, err)
		assert.Equal(t, "InvalidToken", awsErr.Code, "%s", l.op)
	}
}

// A ListExecutions token is the listing of one state machine's executions; another state machine's
// listing refuses it ("Keep all other arguments unchanged").
func TestStepFunctionsPaging_AnExecutionTokenIsBoundToItsStateMachine(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	smArn := sfnPageFixture(t, ts)
	_, out := sfnPageCall(t, ts, "ListExecutions", map[string]any{"stateMachineArn": smArn, "maxResults": 1})
	token := sfnPageToken(t, out)
	require.NotEmpty(t, token)

	other := smArn[:len(smArn)-len("sm-0")] + "sm-1"
	status, out := sfnPageCall(t, ts, "ListExecutions", map[string]any{"stateMachineArn": other, "nextToken": token})
	require.Equal(t, http.StatusBadRequest, status, "%v", out)
	assert.Contains(t, string(out["__type"]), "InvalidToken")
}

func TestStepFunctionsPaging_TheIssuedTokenWalksEveryListing(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	smArn := sfnPageFixture(t, ts)
	want := map[string][]string{
		"ListStateMachines": {"sm-0", "sm-1", "sm-2"},
		"ListActivities":    {"act-0", "act-1", "act-2"},
		"ListExecutions":    {"ex-0", "ex-1", "ex-2"},
	}
	for _, l := range sfnPageListings(smArn) {
		t.Run(l.op, func(t *testing.T) {
			status, out := sfnPageCall(t, ts, l.op, sfnPageWith(l.base, map[string]any{"maxResults": 2}))
			require.Equal(t, http.StatusOK, status, "%v", out)
			page1 := sfnPageNames(t, out, l.items)
			token := sfnPageToken(t, out)
			require.NotEmpty(t, token, "a listing longer than the page issues a token")

			status, out = sfnPageCall(t, ts, l.op, sfnPageWith(l.base, map[string]any{"maxResults": 2, "nextToken": token}))
			require.Equal(t, http.StatusOK, status, "%v", out)
			page2 := sfnPageNames(t, out, l.items)
			assert.Empty(t, sfnPageToken(t, out), "the last page issues no token")
			assert.Equal(t, want[l.op], append(page1, page2...), "two pages cover the listing once each")
		})
	}
}

// The cursor is a value: deleting the item it names between two pages resumes after it rather than
// restarting the listing or skipping the next item.
// statusFilter is applied before the page is cut: each page is full of matching executions, and a
// token is issued only when another matching one remains.
func TestStepFunctionsPaging_StatusFilterIsAppliedBeforeThePage(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	smArn := sfnPageFixture(t, ts)
	execPrefix := "arn:aws:states:us-east-1:123456789012:execution:sm-0:"
	for _, name := range []string{"ex-0", "ex-2"} {
		status, out := sfnPageCall(t, ts, "StopExecution", map[string]any{"executionArn": execPrefix + name})
		require.Equal(t, http.StatusOK, status, "%v", out)
	}
	base := map[string]any{"stateMachineArn": smArn, "statusFilter": "ABORTED", "maxResults": 1}

	status, out := sfnPageCall(t, ts, "ListExecutions", base)
	require.Equal(t, http.StatusOK, status, "%v", out)
	assert.Equal(t, []string{"ex-0"}, sfnPageNames(t, out, "executions"))
	token := sfnPageToken(t, out)
	require.NotEmpty(t, token, "another ABORTED execution remains")

	status, out = sfnPageCall(t, ts, "ListExecutions", sfnPageWith(base, map[string]any{"nextToken": token}))
	require.Equal(t, http.StatusOK, status, "%v", out)
	assert.Equal(t, []string{"ex-2"}, sfnPageNames(t, out, "executions"), "the non-matching ex-1 does not use the page")
	assert.Empty(t, sfnPageToken(t, out), "no matching execution remains")

	status, out = sfnPageCall(t, ts, "ListExecutions", sfnPageWith(base, map[string]any{"statusFilter": "TIMED_OUT"}))
	require.Equal(t, http.StatusOK, status, "%v", out)
	assert.Empty(t, sfnPageNames(t, out, "executions"))
	assert.Empty(t, sfnPageToken(t, out), "an empty filtered listing issues no token")
}

func TestStepFunctionsPaging_ADeleteMidWalkResumes(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	smArn := sfnPageFixture(t, ts)
	prefix := smArn[:len(smArn)-len("sm-0")]
	actPrefix := "arn:aws:states:us-east-1:123456789012:activity:"
	for _, tc := range []struct {
		list, items, deleteOp, deleteMember, deleteArn string
		want                                           []string
	}{
		{"ListStateMachines", "stateMachines", "DeleteStateMachine", "stateMachineArn", prefix + "sm-1", []string{"sm-2"}},
		{"ListActivities", "activities", "DeleteActivity", "activityArn", actPrefix + "act-1", []string{"act-2"}},
	} {
		t.Run(tc.list, func(t *testing.T) {
			_, out := sfnPageCall(t, ts, tc.list, map[string]any{"maxResults": 2})
			require.Len(t, sfnPageNames(t, out, tc.items), 2)
			token := sfnPageToken(t, out)
			require.NotEmpty(t, token)

			status, del := sfnPageCall(t, ts, tc.deleteOp, map[string]any{tc.deleteMember: tc.deleteArn})
			require.Equal(t, http.StatusOK, status, "%v", del)

			status, out = sfnPageCall(t, ts, tc.list, map[string]any{"maxResults": 2, "nextToken": token})
			require.Equal(t, http.StatusOK, status, "%v", out)
			assert.Equal(t, tc.want, sfnPageNames(t, out, tc.items),
				"the walk resumes after the deleted item the token named")
		})
	}
}
