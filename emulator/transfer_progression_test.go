package emulator_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Transfer Family's seeded server lifecycle and StartServer/StopServer (#1196). See
// emulator/transfer_progression.go.

const transferLifecycleHost = "transfer.us-east-1.amazonaws.com"

// transferLifecycleOp sends one Transfer operation and returns its status and body.
func transferLifecycleOp(t *testing.T, ts *emulator.TestServer, op, body string) (int, []byte) {
	t.Helper()
	return lifecycleTarget(t, ts, transferLifecycleHost, "TransferService."+op, body)
}

// transferLifecycleCreate creates a server and returns its ID.
func transferLifecycleCreate(t *testing.T, ts *emulator.TestServer) string {
	t.Helper()
	status, raw := transferLifecycleOp(t, ts, "CreateServer", `{}`)
	require.Equal(t, http.StatusOK, status, "CreateServer: %s", raw)
	var out struct {
		ServerID string `json:"ServerId"`
	}
	require.NoError(t, json.Unmarshal(raw, &out), "%s", raw)
	return out.ServerID
}

// transferLifecycleState describes a server and returns its State.
func transferLifecycleState(t *testing.T, ts *emulator.TestServer, id string) string {
	t.Helper()
	status, raw := transferLifecycleOp(t, ts, "DescribeServer", `{"ServerId":"`+id+`"}`)
	require.Equal(t, http.StatusOK, status, "DescribeServer: %s", raw)
	var out struct {
		Server struct {
			State string `json:"State"`
		} `json:"Server"`
	}
	require.NoError(t, json.Unmarshal(raw, &out), "%s", raw)
	return out.Server.State
}

func TestTransferLifecycle_AnUnseededServerIsOnlineAndStopsAndStartsAtOnce(t *testing.T) {
	t.Parallel()
	ts := lifecycleServer(t)
	id := transferLifecycleCreate(t, ts)
	require.Equal(t, "ONLINE", transferLifecycleState(t, ts, id))

	status, raw := transferLifecycleOp(t, ts, "StopServer", `{"ServerId":"`+id+`"}`)
	require.Equal(t, http.StatusOK, status, "%s", raw)
	require.JSONEq(t, `{}`, string(raw), "the pages say the response body is empty")
	require.Equal(t, "OFFLINE", transferLifecycleState(t, ts, id))

	status, raw = transferLifecycleOp(t, ts, "StartServer", `{"ServerId":"`+id+`"}`)
	require.Equal(t, http.StatusOK, status, "%s", raw)
	require.Equal(t, "ONLINE", transferLifecycleState(t, ts, id))
}

func TestTransferLifecycle_ASeedCountsDownEachTransition(t *testing.T) {
	t.Parallel()
	ts := lifecycleServer(t)
	lifecycleSeed(t, ts, "/v1/transfer/server-status", `{"serverId":"*","pendingObservations":2}`)
	id := transferLifecycleCreate(t, ts)
	for _, want := range []string{"STARTING", "STARTING", "ONLINE"} {
		require.Equal(t, want, transferLifecycleState(t, ts, id), "a new server comes up (unverified ordering)")
	}

	status, raw := transferLifecycleOp(t, ts, "StopServer", `{"ServerId":"`+id+`"}`)
	require.Equal(t, http.StatusOK, status, "%s", raw)
	for _, want := range []string{"STOPPING", "STOPPING", "OFFLINE"} {
		require.Equal(t, want, transferLifecycleState(t, ts, id), "a stop restarts the countdown")
	}

	status, raw = transferLifecycleOp(t, ts, "ListServers", `{}`)
	require.Equal(t, http.StatusOK, status, "%s", raw)
	require.Contains(t, string(raw), `"State":"OFFLINE"`, "a list reports the same observed state")
}

func TestTransferLifecycle_ASeededFailureIsReachable(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, final, move string }{
		{"START_FAILED", "START_FAILED", ""},
		{"STOP_FAILED", "STOP_FAILED", "StopServer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := lifecycleServer(t)
			lifecycleSeed(t, ts, "/v1/transfer/server-status", `{"pendingObservations":1,"finalState":"`+tc.final+`"}`)
			id := transferLifecycleCreate(t, ts)
			if tc.move != "" {
				status, raw := transferLifecycleOp(t, ts, tc.move, `{"ServerId":"`+id+`"}`)
				require.Equal(t, http.StatusOK, status, "%s", raw)
			}
			transferLifecycleState(t, ts, id)
			require.Equal(t, tc.final, transferLifecycleState(t, ts, id))
		})
	}
}

func TestTransferLifecycle_StartingAnOnlineServerHasNoImpact(t *testing.T) {
	t.Parallel()
	ts := lifecycleServer(t)
	id := transferLifecycleCreate(t, ts)
	lifecycleSeed(t, ts, "/v1/transfer/server-status", `{"serverId":"`+id+`","pendingObservations":1}`)
	transferLifecycleState(t, ts, id) // spend the create's countdown
	status, raw := transferLifecycleOp(t, ts, "StartServer", `{"ServerId":"`+id+`"}`)
	require.Equal(t, http.StatusOK, status, "%s", raw)
	require.Equal(t, "ONLINE", transferLifecycleState(t, ts, id), "API_StartServer: no impact on a server already ONLINE")
}

func TestTransferLifecycle_StartAndStopRefuseAMissingServer(t *testing.T) {
	t.Parallel()
	ts := lifecycleServer(t)
	for _, op := range []string{"StartServer", "StopServer"} {
		status, raw := transferLifecycleOp(t, ts, op, `{"ServerId":"s-0123456789abcdef0"}`)
		require.Equalf(t, http.StatusBadRequest, status, "%s: %s", op, raw)
		require.Containsf(t, string(raw), "ResourceNotFoundException", "%s: %s", op, raw)
	}
}

func TestTransferLifecycle_TheSeedEndpointRefusesANonFailureFinalState(t *testing.T) {
	t.Parallel()
	ts := lifecycleServer(t)
	for _, body := range []string{
		`{"pendingObservations":1,"finalState":"ONLINE"}`,
		`{"pendingObservations":1,"state":"BOOTING"}`,
		`{"pendingObservations":-1}`,
	} {
		require.Equal(t, http.StatusBadRequest, lifecycleSeedStatus(t, ts, "/v1/transfer/server-status", body), body)
	}
}

func TestTransferLifecycle_TheSequenceReplaysIdentically(t *testing.T) {
	ts := lifecycleServer(t, emulator.WithRecordedBodies())
	lifecycleSeed(t, ts, "/v1/transfer/server-status", `{"pendingObservations":2}`)
	id := transferLifecycleCreate(t, ts)
	for _, want := range []string{"STARTING", "STARTING", "ONLINE"} {
		require.Equal(t, want, transferLifecycleState(t, ts, id))
	}
	lifecycleReplayIdentically(t, ts)
}

func TestTransferLifecycle_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		arm  func(*cfFaultStateManager)
		op   string
	}{
		{"describe, seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, "DescribeServer"},
		{"stop, seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, "StopServer"},
		{"stop, record write", func(m *cfFaultStateManager) { m.failPut = "server:" }, "StopServer"},
		{"stop, counter reset", func(m *cfFaultStateManager) { m.failDelete = "observed:" }, "StopServer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			p := &emulator.TransferPlugin{}
			ctx, _ := wireSetup(t, p, "req-transfer-lifecycle-fault")
			require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{State: fault, Logger: emulator.NewDefaultLogger(0, false),
				Options: map[string]any{"time_controller": frozenLifecycleClock()}}))
			created := wireJSONTarget(t, p, ctx, "transfer", "TransferService", "CreateServer", map[string]any{})
			var out struct {
				ServerID string `json:"ServerId"`
			}
			require.NoError(t, json.Unmarshal(created, &out))
			tc.arm(fault)
			_, err := p.HandleRequest(ctx, &emulator.AWSRequest{
				Service: "transfer", Operation: tc.op, Path: "/", Params: map[string]string{},
				Body: []byte(`{"ServerId":"` + out.ServerID + `"}`),
			})
			require.Error(t, err)
			var awsErr *emulator.AWSError
			require.NotErrorAs(t, err, &awsErr, "a store fault is not answered as a published refusal")
		})
	}
}
