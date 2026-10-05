package emulator_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// MSK's seeded cluster lifecycle (#1196). See emulator/msk_progression.go for the model.

const mskLifecycleHost = "kafka.us-east-1.amazonaws.com"

// mskLifecycleCreate creates a provisioned cluster over the wire and returns the create response.
func mskLifecycleCreate(t *testing.T, ts *emulator.TestServer, name string) map[string]any {
	t.Helper()
	status, raw := lifecycleCall(t, ts, http.MethodPost, "/v1/clusters", mskLifecycleHost,
		map[string]string{"Content-Type": "application/json"},
		[]byte(`{"clusterName":"`+name+`","kafkaVersion":"3.5.1","numberOfBrokerNodes":2,`+
			`"brokerNodeGroupInfo":{"instanceType":"kafka.m5.large","clientSubnets":["subnet-a","subnet-b"]}}`))
	require.Equal(t, http.StatusOK, status, "CreateCluster: %s", raw)
	var out map[string]any
	require.NoError(t, json.Unmarshal(raw, &out), "%s", raw)
	return out
}

// mskLifecycleDescribe describes a cluster over the wire and returns the status and clusterInfo.
func mskLifecycleDescribe(t *testing.T, ts *emulator.TestServer, arn string) (int, map[string]any) {
	t.Helper()
	status, raw := lifecycleCall(t, ts, http.MethodGet, "/v1/clusters/"+url.PathEscape(arn), mskLifecycleHost, nil, nil)
	var out struct {
		ClusterInfo map[string]any `json:"clusterInfo"`
	}
	if status == http.StatusOK {
		require.NoError(t, json.Unmarshal(raw, &out), "%s", raw)
	}
	return status, out.ClusterInfo
}

func TestMSKLifecycle_UnseededClusterIsActiveAtOnceAndCarriesItsCurrentVersion(t *testing.T) {
	t.Parallel()
	ts := lifecycleServer(t)
	created := mskLifecycleCreate(t, ts, "unseeded")
	require.Equal(t, "ACTIVE", created["state"], "no MSK page publishes a create's state, so unseeded it is the record's")

	status, info := mskLifecycleDescribe(t, ts, created["clusterArn"].(string))
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "ACTIVE", info["state"])
	require.Regexp(t, `^[A-Z0-9]{13}$`, info["currentVersion"], "currentVersion has the page's example shape, KTVPDKIKX0DER")
	require.NotContains(t, info, "stateInfo", "stateInfo is answered only for a seeded FAILED cluster")
}

func TestMSKLifecycle_ASeededCreateCountsDownThroughCreating(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, seed, final string
		stateInfo         map[string]any
	}{
		{"to ACTIVE", `{"clusterArn":"*","pendingObservations":2}`, "ACTIVE", nil},
		{"to FAILED with stateInfo", `{"pendingObservations":2,"finalState":"FAILED","stateInfo":{"code":"InsufficientCapacity","message":"no capacity"}}`, "FAILED",
			map[string]any{"code": "InsufficientCapacity", "message": "no capacity"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := lifecycleServer(t)
			lifecycleSeed(t, ts, "/v1/msk/cluster-status", tc.seed)
			created := mskLifecycleCreate(t, ts, "seeded")
			require.Equal(t, "CREATING", created["state"], "a seeded create answers the countdown's state")
			arn := created["clusterArn"].(string)

			for i := range 2 {
				_, info := mskLifecycleDescribe(t, ts, arn)
				require.Equalf(t, "CREATING", info["state"], "observation %d", i+1)
			}
			_, info := mskLifecycleDescribe(t, ts, arn)
			require.Equal(t, tc.final, info["state"])
			if tc.stateInfo == nil {
				require.NotContains(t, info, "stateInfo")
			} else {
				require.Equal(t, tc.stateInfo, info["stateInfo"])
			}
		})
	}
}

func TestMSKLifecycle_ASeededDeleteReportsDeletingThenNotFound(t *testing.T) {
	t.Parallel()
	ts := lifecycleServer(t)
	created := mskLifecycleCreate(t, ts, "doomed")
	arn := created["clusterArn"].(string)
	lifecycleSeed(t, ts, "/v1/msk/cluster-status", `{"clusterArn":"`+arn+`","pendingObservations":2}`)

	status, raw := lifecycleCall(t, ts, http.MethodDelete, "/v1/clusters/"+url.PathEscape(arn), mskLifecycleHost, nil, nil)
	require.Equal(t, http.StatusOK, status, "%s", raw)
	require.JSONEq(t, `{"clusterArn":"`+arn+`","state":"DELETING"}`, string(raw))

	for i := range 2 {
		status, info := mskLifecycleDescribe(t, ts, arn)
		require.Equalf(t, http.StatusOK, status, "observation %d still sees the deleting cluster", i+1)
		require.Equalf(t, "DELETING", info["state"], "observation %d", i+1)
	}
	status, _ = mskLifecycleDescribe(t, ts, arn)
	require.Equal(t, http.StatusNotFound, status, "once the countdown is spent the cluster is gone")

	status, raw = lifecycleCall(t, ts, http.MethodGet, "/v1/clusters", mskLifecycleHost, nil, nil)
	require.Equal(t, http.StatusOK, status)
	require.JSONEq(t, `{"clusterInfoList":[]}`, string(raw), "and a list no longer reports it")
}

func TestMSKLifecycle_AnUnseededDeleteRemovesTheClusterAtOnce(t *testing.T) {
	t.Parallel()
	ts := lifecycleServer(t)
	arn := mskLifecycleCreate(t, ts, "quick")["clusterArn"].(string)
	status, raw := lifecycleCall(t, ts, http.MethodDelete, "/v1/clusters/"+url.PathEscape(arn), mskLifecycleHost, nil, nil)
	require.Equal(t, http.StatusOK, status, "%s", raw)
	require.Contains(t, string(raw), `"state":"DELETING"`)
	status, _ = mskLifecycleDescribe(t, ts, arn)
	require.Equal(t, http.StatusNotFound, status)
}

func TestMSKLifecycle_DeleteChecksCurrentVersion(t *testing.T) {
	t.Parallel()
	ts := lifecycleServer(t)
	arn := mskLifecycleCreate(t, ts, "versioned")["clusterArn"].(string)
	_, info := mskLifecycleDescribe(t, ts, arn)
	version := info["currentVersion"].(string)

	status, raw := lifecycleCall(t, ts, http.MethodDelete, "/v1/clusters/"+url.PathEscape(arn)+"?currentVersion=WRONGVERSION0", mskLifecycleHost, nil, nil)
	require.Equal(t, http.StatusBadRequest, status, "%s", raw)
	require.Contains(t, string(raw), `"invalidParameter":"currentVersion"`)

	status, raw = lifecycleCall(t, ts, http.MethodDelete, "/v1/clusters/"+url.PathEscape(arn)+"?currentVersion="+version, mskLifecycleHost, nil, nil)
	require.Equal(t, http.StatusOK, status, "the cluster's own version is accepted: %s", raw)
}

func TestMSKLifecycle_TheSeedEndpointRefusesAnOffEnumOrTransitionalState(t *testing.T) {
	t.Parallel()
	ts := lifecycleServer(t)
	for _, body := range []string{
		`{"pendingObservations":1,"state":"BOOTING"}`,
		`{"pendingObservations":1,"finalState":"CREATING"}`,
		`{"pendingObservations":1,"finalState":"DELETING"}`,
		`{"pendingObservations":-1}`,
	} {
		require.Equal(t, http.StatusBadRequest, lifecycleSeedStatus(t, ts, "/v1/msk/cluster-status", body), body)
	}
}

func TestMSKLifecycle_ClearingTheSeedSettlesTheCluster(t *testing.T) {
	t.Parallel()
	ts := lifecycleServer(t)
	lifecycleSeed(t, ts, "/v1/msk/cluster-status", `{"pendingObservations":5}`)
	arn := mskLifecycleCreate(t, ts, "cleared")["clusterArn"].(string)
	_, info := mskLifecycleDescribe(t, ts, arn)
	require.Equal(t, "CREATING", info["state"])
	lifecycleClear(t, ts, "/v1/msk/cluster-status", "")
	_, info = mskLifecycleDescribe(t, ts, arn)
	require.Equal(t, "ACTIVE", info["state"], "a seed governs observations and never rewrote the record")
}

func TestMSKLifecycle_TheSequenceReplaysIdentically(t *testing.T) {
	ts := lifecycleServer(t, emulator.WithRecordedBodies())
	lifecycleSeed(t, ts, "/v1/msk/cluster-status", `{"pendingObservations":2}`)
	arn := mskLifecycleCreate(t, ts, "replayed")["clusterArn"].(string)
	for _, want := range []string{"CREATING", "CREATING", "ACTIVE"} {
		_, info := mskLifecycleDescribe(t, ts, arn)
		require.Equal(t, want, info["state"])
	}
	lifecycleReplayIdentically(t, ts)
}

func TestMSKLifecycle_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
	p := &emulator.MSKPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State: fault, Logger: emulator.NewDefaultLogger(0, false), Options: map[string]any{"time_controller": frozenLifecycleClock()},
	}))
	ctx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: "req-msk-lifecycle-fault", IDs: emulator.NewIDMint("req-msk-lifecycle-fault")}
	body := map[string]any{
		"clusterName": "fault", "kafkaVersion": "3.5.1", "numberOfBrokerNodes": 2,
		"brokerNodeGroupInfo": map[string]any{"instanceType": "kafka.m5.large", "clientSubnets": []string{"a", "b"}},
	}
	created := wireREST(t, p, ctx, "kafka", http.MethodPost, "/v1/clusters", body)
	var out struct {
		ClusterArn string `json:"clusterArn"`
	}
	require.NoError(t, json.Unmarshal(created, &out))

	for _, tc := range []struct {
		name   string
		arm    func(*cfFaultStateManager)
		method string
	}{
		{"describe, seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, http.MethodGet},
		// The counter is read only under a seed, so this case writes one first.
		{"describe, counter read", func(m *cfFaultStateManager) {
			require.NoError(t, m.inner.Put(t.Context(), "msk-cluster-ctrl", "status:*", []byte(`{"pendingObservations":2}`)))
			m.failGet = "observed:"
		}, http.MethodGet},
		{"delete, seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, http.MethodDelete},
	} {
		t.Run(tc.name, func(t *testing.T) {
			*fault = cfFaultStateManager{inner: fault.inner}
			tc.arm(fault)
			_, err := p.HandleRequest(ctx, &emulator.AWSRequest{
				Service: "kafka", HTTPMethod: tc.method, Path: "/v1/clusters/" + out.ClusterArn, Params: map[string]string{},
			})
			require.Error(t, err)
			var awsErr *emulator.AWSError
			require.NotErrorAs(t, err, &awsErr, "a store fault is not answered as a published refusal")
		})
	}
}
