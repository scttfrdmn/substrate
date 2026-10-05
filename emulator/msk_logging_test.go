package emulator_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// MSK keeps and answers loggingInfo, openMonitoring, rebalancing and brokerNodeGroupInfo's
// connectivityInfo (#1386). Both create pages accept all four, and both describe shapes publish them.
// Until #1386 the create decoded none of them, so a cluster created with logging or monitoring
// described with neither. The assertions are on raw bytes, in the published lowerCamel member names.

// mskLoggingMembers is one create's worth of the four members, in the published request shape.
func mskLoggingMembers() map[string]any {
	return map[string]any{
		"loggingInfo": map[string]any{
			"brokerLogs": map[string]any{
				"cloudWatchLogs": map[string]any{"enabled": true, "logGroup": "msk-broker"},
				"firehose":       map[string]any{"enabled": false},
				"s3":             map[string]any{"enabled": true, "bucket": "msk-logs", "prefix": "broker/"},
			},
			"authorizerLogs": map[string]any{
				"cloudWatchLogs": map[string]any{"enabled": true, "logGroup": "msk-authz"},
			},
		},
		"openMonitoring": map[string]any{
			"prometheus": map[string]any{
				"jmxExporter":  map[string]any{"enabledInBroker": true},
				"nodeExporter": map[string]any{"enabledInBroker": false},
			},
		},
		"rebalancing": map[string]any{"status": "ACTIVE"},
	}
}

// mskConnectivity is brokerNodeGroupInfo.connectivityInfo in the published request shape.
func mskConnectivity() map[string]any {
	return map[string]any{
		"publicAccess": map[string]any{"type": "SERVICE_PROVIDED_EIPS"},
		"vpcConnectivity": map[string]any{
			"clientAuthentication": map[string]any{
				"sasl": map[string]any{"scram": map[string]any{"enabled": true}, "iam": map[string]any{"enabled": false}},
				"tls":  map[string]any{"enabled": true},
			},
		},
		"networkType": "DUAL",
	}
}

// The members each describe must answer exactly, as raw JSON.
const (
	mskWantLogging = `{"brokerLogs":{"cloudWatchLogs":{"enabled":true,"logGroup":"msk-broker"},` +
		`"firehose":{"enabled":false},"s3":{"bucket":"msk-logs","enabled":true,"prefix":"broker/"}},` +
		`"authorizerLogs":{"cloudWatchLogs":{"enabled":true,"logGroup":"msk-authz"}}}`
	mskWantMonitoring   = `{"prometheus":{"jmxExporter":{"enabledInBroker":true},"nodeExporter":{"enabledInBroker":false}}}`
	mskWantRebalancing  = `{"status":"ACTIVE"}`
	mskWantConnectivity = `{"publicAccess":{"type":"SERVICE_PROVIDED_EIPS"},"vpcConnectivity":{"clientAuthentication":` +
		`{"sasl":{"scram":{"enabled":true},"iam":{"enabled":false}},"tls":{"enabled":true}}},"networkType":"DUAL"}`
)

// mskWithLogging returns the minimal v1 body carrying all four members.
func mskWithLogging(name string) map[string]any {
	b := mskMinimalCluster(name)
	for k, v := range mskLoggingMembers() {
		b[k] = v
	}
	b["brokerNodeGroupInfo"].(map[string]any)["connectivityInfo"] = mskConnectivity()
	return b
}

// mskRequireMembers decodes one cluster document and requires the four members, as raw JSON.
func mskRequireMembers(t *testing.T, op string, cluster map[string]json.RawMessage) {
	t.Helper()
	require.JSONEqf(t, mskWantLogging, string(cluster["loggingInfo"]), "%s loggingInfo", op)
	require.JSONEqf(t, mskWantMonitoring, string(cluster["openMonitoring"]), "%s openMonitoring", op)
	require.JSONEqf(t, mskWantRebalancing, string(cluster["rebalancing"]), "%s rebalancing", op)
	var bng map[string]json.RawMessage
	require.NoErrorf(t, json.Unmarshal(cluster["brokerNodeGroupInfo"], &bng), "%s brokerNodeGroupInfo", op)
	require.JSONEqf(t, mskWantConnectivity, string(bng["connectivityInfo"]), "%s brokerNodeGroupInfo.connectivityInfo", op)
}

func TestMSKLogging_V2RoundTripsTheFourMembers(t *testing.T) {
	t.Parallel()
	p, ctx := mskAuditPlugin(t, emulator.NewMemoryStateManager())
	v1 := mskWithLogging("v2-logged")
	provisioned := map[string]any{}
	for k, v := range v1 {
		if k != "clusterName" {
			provisioned[k] = v
		}
	}
	arn := mskAuditARN(t, p, ctx, "/api/v2/clusters", map[string]any{"clusterName": "v2-logged", "provisioned": provisioned})

	var described struct {
		ClusterInfo struct {
			Provisioned map[string]json.RawMessage `json:"provisioned"`
		} `json:"clusterInfo"`
	}
	raw := mskAuditOK(t, p, ctx, http.MethodGet, "/api/v2/clusters/"+arn, nil, nil)
	require.NoError(t, json.Unmarshal(raw, &described), "%s", raw)
	mskRequireMembers(t, "DescribeClusterV2", described.ClusterInfo.Provisioned)

	var listed struct {
		ClusterInfoList []struct {
			Provisioned map[string]json.RawMessage `json:"provisioned"`
		} `json:"clusterInfoList"`
	}
	raw = mskAuditOK(t, p, ctx, http.MethodGet, "/api/v2/clusters", nil, nil)
	require.NoError(t, json.Unmarshal(raw, &listed), "%s", raw)
	require.Len(t, listed.ClusterInfoList, 1, "%s", raw)
	mskRequireMembers(t, "ListClustersV2", listed.ClusterInfoList[0].Provisioned)
}

func TestMSKLogging_V1RoundTripsTheFourMembers(t *testing.T) {
	t.Parallel()
	p, ctx := mskAuditPlugin(t, emulator.NewMemoryStateManager())
	arn := mskAuditARN(t, p, ctx, "/v1/clusters", mskWithLogging("v1-logged"))

	var described struct {
		ClusterInfo map[string]json.RawMessage `json:"clusterInfo"`
	}
	raw := mskAuditOK(t, p, ctx, http.MethodGet, "/v1/clusters/"+arn, nil, nil)
	require.NoError(t, json.Unmarshal(raw, &described), "%s", raw)
	mskRequireMembers(t, "DescribeCluster", described.ClusterInfo)

	var listed struct {
		ClusterInfoList []map[string]json.RawMessage `json:"clusterInfoList"`
	}
	raw = mskAuditOK(t, p, ctx, http.MethodGet, "/v1/clusters", nil, nil)
	require.NoError(t, json.Unmarshal(raw, &listed), "%s", raw)
	require.Len(t, listed.ClusterInfoList, 1, "%s", raw)
	mskRequireMembers(t, "ListClusters", listed.ClusterInfoList[0])
}

// A cluster created without the four answers without them: neither page states a default.
func TestMSKLogging_AnUnsentMemberIsAbsent(t *testing.T) {
	t.Parallel()
	p, ctx := mskAuditPlugin(t, emulator.NewMemoryStateManager())
	arn := mskAuditARN(t, p, ctx, "/v1/clusters", mskMinimalCluster("plain"))
	raw := mskAuditOK(t, p, ctx, http.MethodGet, "/v1/clusters/"+arn, nil, nil)
	for _, member := range []string{`"loggingInfo"`, `"openMonitoring"`, `"rebalancing"`, `"connectivityInfo"`} {
		require.NotContainsf(t, string(raw), member, "an unsent %s is omitted, not invented: %s", member, raw)
	}
}

func TestMSKLogging_RefusesWhatThePagesRequire(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func(b map[string]any)
		param  string
	}{
		{"loggingInfo without brokerLogs", func(b map[string]any) {
			b["loggingInfo"] = map[string]any{"authorizerLogs": map[string]any{}}
		}, "brokerLogs"},
		{"a cloudWatchLogs target without enabled", func(b map[string]any) {
			b["loggingInfo"] = map[string]any{"brokerLogs": map[string]any{"cloudWatchLogs": map[string]any{"logGroup": "g"}}}
		}, "enabled"},
		{"a firehose target without enabled", func(b map[string]any) {
			b["loggingInfo"] = map[string]any{"brokerLogs": map[string]any{"firehose": map[string]any{"deliveryStream": "s"}}}
		}, "enabled"},
		{"an authorizerLogs s3 target without enabled", func(b map[string]any) {
			b["loggingInfo"] = map[string]any{"brokerLogs": map[string]any{},
				"authorizerLogs": map[string]any{"s3": map[string]any{"bucket": "b"}}}
		}, "enabled"},
		{"openMonitoring without prometheus", func(b map[string]any) {
			b["openMonitoring"] = map[string]any{}
		}, "prometheus"},
		{"an exporter without enabledInBroker", func(b map[string]any) {
			b["openMonitoring"] = map[string]any{"prometheus": map[string]any{"nodeExporter": map[string]any{}}}
		}, "enabledInBroker"},
		{"an unpublished rebalancing status", func(b map[string]any) {
			b["rebalancing"] = map[string]any{"status": "RUNNING"}
		}, "status"},
		{"an unpublished networkType", func(b map[string]any) {
			b["brokerNodeGroupInfo"].(map[string]any)["connectivityInfo"] = map[string]any{"networkType": "IPV6"}
		}, "networkType"},
		{"an unpublished publicAccess type", func(b map[string]any) {
			b["brokerNodeGroupInfo"].(map[string]any)["connectivityInfo"] = map[string]any{"publicAccess": map[string]any{"type": "ENABLED"}}
		}, "type"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, api := range []string{"v1", "v2"} {
				p, ctx := mskAuditPlugin(t, emulator.NewMemoryStateManager())
				b := mskMinimalCluster("refused")
				tc.mutate(b)
				path, body := "/v1/clusters", b
				if api == "v2" {
					delete(b, "clusterName")
					path, body = "/api/v2/clusters", map[string]any{"clusterName": "refused", "provisioned": b}
				}
				_, err := mskAuditCall(p, ctx, http.MethodPost, path, body, nil)
				mskAuditRefusal(t, err, "BadRequestException", http.StatusBadRequest, tc.param)
			}
		})
	}
}

// A record written before #1386 carries none of the four, decodes unchanged, and describes without
// them: every new state member is omitempty.
func TestMSKLogging_ARecordWrittenBeforeTheFixDecodes(t *testing.T) {
	t.Parallel()
	state := emulator.NewMemoryStateManager()
	p, ctx := mskAuditPlugin(t, state)
	arn := mskAuditARN(t, p, ctx, "/v1/clusters", mskMinimalCluster("legacy"))

	keys, err := state.List(t.Context(), "msk", "cluster:")
	require.NoError(t, err)
	require.Len(t, keys, 1)
	stored, err := state.Get(t.Context(), "msk", keys[0])
	require.NoError(t, err)
	for _, member := range []string{"LoggingInfo", "OpenMonitoring", "Rebalancing", "ConnectivityInfo"} {
		require.NotContainsf(t, string(stored), member, "a cluster sent no %s stores none, so its bytes are unchanged: %s", member, stored)
	}
	mskAuditOK(t, p, ctx, http.MethodGet, "/api/v2/clusters/"+arn, nil, nil)
}
