package emulator_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// Redshift's seeded cluster and snapshot lifecycles (#1196). See emulator/redshift_progression.go.

const redshiftLifecycleHost = "redshift.us-east-1.amazonaws.com"

// redshiftLifecycleCluster is the subset of a Cluster element these tests read.
type redshiftLifecycleCluster struct {
	ClusterIdentifier     string `xml:"ClusterIdentifier"`
	ClusterStatus         string `xml:"ClusterStatus"`
	NodeType              string `xml:"NodeType"`
	NumberOfNodes         int    `xml:"NumberOfNodes"`
	PendingModifiedValues *struct {
		NodeType      string `xml:"NodeType"`
		NumberOfNodes int    `xml:"NumberOfNodes"`
	} `xml:"PendingModifiedValues"`
}

// redshiftLifecycleAction sends one Redshift action and returns its status and body.
func redshiftLifecycleAction(t *testing.T, ts *emulator.TestServer, action string, params map[string]string) (int, []byte) {
	t.Helper()
	form := url.Values{"Action": {action}, "Version": {"2012-12-01"}}
	for k, v := range params {
		form.Set(k, v)
	}
	return lifecycleQuery(t, ts, redshiftLifecycleHost, form)
}

// redshiftLifecycleCreate creates a cluster and returns the create response's Cluster element.
func redshiftLifecycleCreate(t *testing.T, ts *emulator.TestServer, id string) redshiftLifecycleCluster {
	t.Helper()
	status, raw := redshiftLifecycleAction(t, ts, "CreateCluster", map[string]string{
		"ClusterIdentifier": id, "NodeType": "ra3.xlplus", "MasterUsername": "admin", "NumberOfNodes": "2",
	})
	require.Equal(t, http.StatusOK, status, "CreateCluster: %s", raw)
	var out struct {
		Cluster redshiftLifecycleCluster `xml:"CreateClusterResult>Cluster"`
	}
	require.NoError(t, xml.Unmarshal(raw, &out), "%s", raw)
	return out.Cluster
}

// redshiftLifecycleDescribe describes one cluster and returns the status and its Cluster element.
func redshiftLifecycleDescribe(t *testing.T, ts *emulator.TestServer, id string) (int, redshiftLifecycleCluster) {
	t.Helper()
	status, raw := redshiftLifecycleAction(t, ts, "DescribeClusters", map[string]string{"ClusterIdentifier": id})
	var out struct {
		Clusters []redshiftLifecycleCluster `xml:"DescribeClustersResult>Clusters>Cluster"`
	}
	if status != http.StatusOK {
		return status, redshiftLifecycleCluster{}
	}
	require.NoError(t, xml.Unmarshal(raw, &out), "%s", raw)
	require.Len(t, out.Clusters, 1, "%s", raw)
	return status, out.Clusters[0]
}

func TestRedshiftLifecycle_CreateAnswersCreatingAndAnUnseededDescribeIsAvailable(t *testing.T) {
	t.Parallel()
	ts := lifecycleServer(t)
	require.Equal(t, "creating", redshiftLifecycleCreate(t, ts, "plain").ClusterStatus,
		"API_CreateCluster's sample answers creating, seeded or not")
	_, c := redshiftLifecycleDescribe(t, ts, "plain")
	require.Equal(t, "available", c.ClusterStatus, "the zero default: settled from the first describe")
}

func TestRedshiftLifecycle_ASeededCreateCountsDown(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, seed, final string }{
		{"to available", `{"clusterIdentifier":"*","pendingObservations":2}`, "available"},
		{"to hardware-failure", `{"pendingObservations":2,"finalState":"hardware-failure"}`, "hardware-failure"},
		{"to incompatible-network", `{"pendingObservations":2,"finalState":"incompatible-network"}`, "incompatible-network"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := lifecycleServer(t)
			lifecycleSeed(t, ts, "/v1/redshift/cluster-status", tc.seed)
			redshiftLifecycleCreate(t, ts, "seeded")
			for i := range 2 {
				_, c := redshiftLifecycleDescribe(t, ts, "seeded")
				require.Equalf(t, "creating", c.ClusterStatus, "observation %d", i+1)
			}
			_, c := redshiftLifecycleDescribe(t, ts, "seeded")
			require.Equal(t, tc.final, c.ClusterStatus)
		})
	}
}

func TestRedshiftLifecycle_ASeededResizeReportsResizingWithPendingModifiedValues(t *testing.T) {
	t.Parallel()
	ts := lifecycleServer(t)
	redshiftLifecycleCreate(t, ts, "resized")
	lifecycleSeed(t, ts, "/v1/redshift/cluster-status", `{"clusterIdentifier":"resized","pendingObservations":2}`)

	status, raw := redshiftLifecycleAction(t, ts, "ModifyCluster", map[string]string{
		"ClusterIdentifier": "resized", "NodeType": "ra3.4xlarge", "NumberOfNodes": "4",
	})
	require.Equal(t, http.StatusOK, status, "%s", raw)
	require.Contains(t, string(raw), "<ClusterStatus>resizing</ClusterStatus>")

	for i := range 2 {
		_, c := redshiftLifecycleDescribe(t, ts, "resized")
		require.Equalf(t, "resizing", c.ClusterStatus, "observation %d", i+1)
		require.Equal(t, "ra3.xlplus", c.NodeType, "the cluster shows its previous node type while resizing")
		require.Equal(t, 2, c.NumberOfNodes)
		require.NotNil(t, c.PendingModifiedValues, "the new values are pending")
		require.Equal(t, "ra3.4xlarge", c.PendingModifiedValues.NodeType)
		require.Equal(t, 4, c.PendingModifiedValues.NumberOfNodes)
	}
	_, c := redshiftLifecycleDescribe(t, ts, "resized")
	require.Equal(t, "available", c.ClusterStatus)
	require.Equal(t, "ra3.4xlarge", c.NodeType)
	require.Equal(t, 4, c.NumberOfNodes)
	require.Nil(t, c.PendingModifiedValues, "nothing is pending once the resize is done")
}

func TestRedshiftLifecycle_AnUnseededResizeIsAppliedInPlace(t *testing.T) {
	t.Parallel()
	ts := lifecycleServer(t)
	redshiftLifecycleCreate(t, ts, "inplace")
	status, raw := redshiftLifecycleAction(t, ts, "ModifyCluster", map[string]string{
		"ClusterIdentifier": "inplace", "NodeType": "ra3.4xlarge", "NumberOfNodes": "4",
	})
	require.Equal(t, http.StatusOK, status, "%s", raw)
	require.NotContains(t, string(raw), "PendingModifiedValues")
	_, c := redshiftLifecycleDescribe(t, ts, "inplace")
	require.Equal(t, "available", c.ClusterStatus)
	require.Equal(t, "ra3.4xlarge", c.NodeType)
}

func TestRedshiftLifecycle_ASeededDeleteReportsItsTransitionThenNotFound(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		params map[string]string
		want   []string
	}{
		{"skipping the final snapshot", map[string]string{"SkipFinalClusterSnapshot": "true"}, []string{"deleting", "deleting"}},
		{"with a final snapshot", map[string]string{"FinalClusterSnapshotIdentifier": "final-snap"}, []string{"final-snapshot", "deleting"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := lifecycleServer(t)
			redshiftLifecycleCreate(t, ts, "doomed")
			lifecycleSeed(t, ts, "/v1/redshift/cluster-status", `{"clusterIdentifier":"doomed","pendingObservations":2}`)
			params := map[string]string{"ClusterIdentifier": "doomed"}
			for k, v := range tc.params {
				params[k] = v
			}
			status, raw := redshiftLifecycleAction(t, ts, "DeleteCluster", params)
			require.Equal(t, http.StatusOK, status, "%s", raw)
			require.Contains(t, string(raw), "<ClusterStatus>"+tc.want[0]+"</ClusterStatus>")

			for i, want := range tc.want {
				_, c := redshiftLifecycleDescribe(t, ts, "doomed")
				require.Equalf(t, want, c.ClusterStatus, "observation %d", i+1)
			}
			status, _ = redshiftLifecycleDescribe(t, ts, "doomed")
			require.Equal(t, http.StatusNotFound, status, "the countdown spent, the cluster is ClusterNotFound")
		})
	}
}

func TestRedshiftLifecycle_DeleteReadsTheFinalSnapshotParameters(t *testing.T) {
	t.Parallel()
	ts := lifecycleServer(t)
	for _, id := range []string{"c1", "c2", "c3", "c4", "c5", "c6"} {
		redshiftLifecycleCreate(t, ts, id)
	}
	for _, tc := range []struct {
		name   string
		params map[string]string
		status int
		code   string
	}{
		{"no final snapshot named and none skipped", map[string]string{"ClusterIdentifier": "c1"}, http.StatusBadRequest, "InvalidParameterCombination"},
		{"a final snapshot named while skipping it", map[string]string{"ClusterIdentifier": "c1", "SkipFinalClusterSnapshot": "true", "FinalClusterSnapshotIdentifier": "s"}, http.StatusBadRequest, "InvalidParameterCombination"},
		{"a malformed skip flag", map[string]string{"ClusterIdentifier": "c1", "SkipFinalClusterSnapshot": "maybe"}, http.StatusBadRequest, "InvalidParameterValue"},
		{"a malformed final snapshot identifier", map[string]string{"ClusterIdentifier": "c1", "FinalClusterSnapshotIdentifier": "1bad"}, http.StatusBadRequest, "InvalidParameterValue"},
		{"a retention period out of range", map[string]string{"ClusterIdentifier": "c1", "FinalClusterSnapshotIdentifier": "keep", "FinalClusterSnapshotRetentionPeriod": "0"}, http.StatusBadRequest, "InvalidRetentionPeriodFault"},
		{"a final snapshot", map[string]string{"ClusterIdentifier": "c2", "FinalClusterSnapshotIdentifier": "taken", "FinalClusterSnapshotRetentionPeriod": "7"}, http.StatusOK, ""},
		{"a final snapshot whose identifier is taken", map[string]string{"ClusterIdentifier": "c3", "FinalClusterSnapshotIdentifier": "taken"}, http.StatusBadRequest, "ClusterSnapshotAlreadyExists"},
		{"skipping it", map[string]string{"ClusterIdentifier": "c4", "SkipFinalClusterSnapshot": "true"}, http.StatusOK, ""},
	} {
		status, raw := redshiftLifecycleAction(t, ts, "DeleteCluster", tc.params)
		require.Equalf(t, tc.status, status, "%s: %s", tc.name, raw)
		if tc.code != "" {
			require.Containsf(t, string(raw), "<Code>"+tc.code+"</Code>", "%s: %s", tc.name, raw)
		}
	}
	status, raw := redshiftLifecycleAction(t, ts, "DescribeClusterSnapshots", map[string]string{"SnapshotIdentifier": "taken"})
	require.Equal(t, http.StatusOK, status, "%s", raw)
	require.Contains(t, string(raw), "<ClusterIdentifier>c2</ClusterIdentifier>", "the final snapshot is a snapshot of the deleted cluster")
	require.Contains(t, string(raw), "<SnapshotType>manual</SnapshotType>")
}

// A resize counting down refuses a further modify and a delete. A create counting down does not: a
// seed posted after the create restarts its countdown too, and is usually meant for the next
// transition.
func TestRedshiftLifecycle_AResizeInProgressRefusesModifyAndDelete(t *testing.T) {
	t.Parallel()
	ts := lifecycleServer(t)
	lifecycleSeed(t, ts, "/v1/redshift/cluster-status", `{"pendingObservations":3}`)
	redshiftLifecycleCreate(t, ts, "busy")
	status, raw := redshiftLifecycleAction(t, ts, "ModifyCluster", map[string]string{"ClusterIdentifier": "busy", "NodeType": "ra3.4xlarge", "NumberOfNodes": "4"})
	require.Equal(t, http.StatusOK, status, "a create counting down does not refuse the resize: %s", raw)
	for _, tc := range []struct {
		action string
		params map[string]string
	}{
		{"ModifyCluster", map[string]string{"ClusterIdentifier": "busy", "NodeType": "ra3.4xlarge", "NumberOfNodes": "2"}},
		{"DeleteCluster", map[string]string{"ClusterIdentifier": "busy", "SkipFinalClusterSnapshot": "true"}},
	} {
		status, raw := redshiftLifecycleAction(t, ts, tc.action, tc.params)
		require.Equalf(t, http.StatusBadRequest, status, "%s: %s", tc.action, raw)
		require.Containsf(t, string(raw), "<Code>InvalidClusterState</Code>", "%s: %s", tc.action, raw)
	}
}

func TestRedshiftLifecycle_ASnapshotCountsDownFromCreating(t *testing.T) {
	t.Parallel()
	ts := lifecycleServer(t)
	redshiftLifecycleCreate(t, ts, "src")
	lifecycleSeed(t, ts, "/v1/redshift/snapshot-status", `{"snapshotIdentifier":"snap","pendingObservations":1,"finalState":"failed"}`)
	status, raw := redshiftLifecycleAction(t, ts, "CreateClusterSnapshot", map[string]string{"ClusterIdentifier": "src", "SnapshotIdentifier": "snap"})
	require.Equal(t, http.StatusOK, status, "%s", raw)
	require.Contains(t, string(raw), "<Status>creating</Status>", "API_Snapshot: CreateClusterSnapshot returns creating")
	for _, want := range []string{"creating", "failed"} {
		_, raw := redshiftLifecycleAction(t, ts, "DescribeClusterSnapshots", map[string]string{"SnapshotIdentifier": "snap"})
		require.Contains(t, string(raw), "<Status>"+want+"</Status>")
	}
}

func TestRedshiftLifecycle_TheSeedEndpointsRefuseAnOffEnumOrTransitionalState(t *testing.T) {
	t.Parallel()
	ts := lifecycleServer(t)
	for _, tc := range []struct{ path, body string }{
		{"/v1/redshift/cluster-status", `{"pendingObservations":1,"state":"booting"}`},
		{"/v1/redshift/cluster-status", `{"pendingObservations":1,"finalState":"creating"}`},
		{"/v1/redshift/cluster-status", `{"pendingObservations":1,"finalState":"deleting"}`},
		{"/v1/redshift/snapshot-status", `{"pendingObservations":1,"finalState":"deleted"}`},
		{"/v1/redshift/snapshot-status", `{"pendingObservations":-1}`},
	} {
		require.Equal(t, http.StatusBadRequest, lifecycleSeedStatus(t, ts, tc.path, tc.body), "%s %s", tc.path, tc.body)
	}
}

func TestRedshiftLifecycle_TheSequenceReplaysIdentically(t *testing.T) {
	ts := lifecycleServer(t, emulator.WithRecordedBodies())
	lifecycleSeed(t, ts, "/v1/redshift/cluster-status", `{"pendingObservations":2}`)
	redshiftLifecycleCreate(t, ts, "replayed")
	for _, want := range []string{"creating", "creating", "available"} {
		_, c := redshiftLifecycleDescribe(t, ts, "replayed")
		require.Equal(t, want, c.ClusterStatus)
	}
	lifecycleReplayIdentically(t, ts)
}

func TestRedshiftLifecycle_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		arm    func(*cfFaultStateManager)
		action string
		params map[string]string
	}{
		{"describe, seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, "DescribeClusters", map[string]string{"ClusterIdentifier": "f"}},
		{"modify, seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, "ModifyCluster", map[string]string{"ClusterIdentifier": "f", "NodeType": "ra3.4xlarge", "NumberOfNodes": "2"}},
		{"delete, seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, "DeleteCluster", map[string]string{"ClusterIdentifier": "f", "SkipFinalClusterSnapshot": "true"}},
		{"delete, final snapshot read", func(m *cfFaultStateManager) { m.failGet = "snapshot:" }, "DeleteCluster", map[string]string{"ClusterIdentifier": "f", "FinalClusterSnapshotIdentifier": "fs"}},
		{"delete, final snapshot write", func(m *cfFaultStateManager) { m.failPut = "snapshot:" }, "DeleteCluster", map[string]string{"ClusterIdentifier": "f", "FinalClusterSnapshotIdentifier": "fs"}},
		{"delete, record delete", func(m *cfFaultStateManager) { m.failDelete = "cluster:" }, "DeleteCluster", map[string]string{"ClusterIdentifier": "f", "SkipFinalClusterSnapshot": "true"}},
		{"snapshot describe, seed read", func(m *cfFaultStateManager) { m.failGet = "status:" }, "DescribeClusterSnapshots", map[string]string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			p := &emulator.RedshiftPlugin{}
			ctx, _ := wireSetup(t, p, "req-redshift-lifecycle-fault")
			require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{State: fault, Logger: emulator.NewDefaultLogger(0, false),
				Options: map[string]any{"time_controller": frozenLifecycleClock()}}))
			call := func(action string, params map[string]string) error {
				full := map[string]string{"Action": action, "Version": "2012-12-01"}
				for k, v := range params {
					full[k] = v
				}
				_, err := p.HandleRequest(ctx, &emulator.AWSRequest{Service: "redshift", Operation: action, Path: "/", Params: full})
				return err
			}
			require.NoError(t, call("CreateCluster", map[string]string{"ClusterIdentifier": "f", "NodeType": "ra3.xlplus", "MasterUsername": "admin"}))
			require.NoError(t, call("CreateClusterSnapshot", map[string]string{"ClusterIdentifier": "f", "SnapshotIdentifier": "s"}))
			tc.arm(fault)
			err := call(tc.action, tc.params)
			require.Error(t, err)
			var awsErr *emulator.AWSError
			require.NotErrorAs(t, err, &awsErr, "a store fault is not answered as a published refusal")
		})
	}
}
