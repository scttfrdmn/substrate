package emulator_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scttfrdmn/substrate/emulator"
)

// MSK's share of the #1195/#1197/#1198/#1199 audits and of #1211, asserted on raw bytes and on the
// AWSError a handler returns. MSK's pages publish an Error model of {message, invalidParameter} and no
// code string, so each refusal is asserted on the shape-name code (substrate's reading, recorded in
// msk_errors.go), the published status, and the invalidParameter the Error model glosses as "The
// parameter that caused the error."

// mskAuditPlugin is an MSK plugin over the given store, on a frozen clock.
func mskAuditPlugin(t *testing.T, state emulator.StateManager) (*emulator.MSKPlugin, *emulator.RequestContext) {
	t.Helper()
	clock := time.Unix(1700000000, 0).UTC()
	tc := emulator.NewTimeController(clock)
	tc.Freeze()
	tc.SetTime(clock)
	p := &emulator.MSKPlugin{}
	require.NoError(t, p.Initialize(t.Context(), emulator.PluginConfig{
		State:   state,
		Logger:  emulator.NewDefaultLogger(0, false),
		Options: map[string]any{"time_controller": tc},
	}), "MSKPlugin.Initialize")
	return p, &emulator.RequestContext{
		AccountID: "123456789012", Region: "us-east-1",
		RequestID: "req-msk-audit", IDs: emulator.NewIDMint("req-msk-audit"),
	}
}

// mskAuditCall issues one request with optional query parameters and returns the response or error.
func mskAuditCall(p *emulator.MSKPlugin, ctx *emulator.RequestContext, method, path string, body any, query map[string]string) (*emulator.AWSResponse, error) {
	var raw []byte
	switch b := body.(type) {
	case nil:
	case string:
		raw = []byte(b)
	default:
		raw, _ = json.Marshal(b)
	}
	if query == nil {
		query = map[string]string{}
	}
	return p.HandleRequest(ctx, &emulator.AWSRequest{
		Service: "msk", HTTPMethod: method, Path: path, Body: raw,
		Headers: map[string]string{"Content-Type": "application/json"}, Params: query,
	})
}

// mskAuditOK issues a request that must succeed and returns its raw body.
func mskAuditOK(t *testing.T, p *emulator.MSKPlugin, ctx *emulator.RequestContext, method, path string, body any, query map[string]string) []byte {
	t.Helper()
	resp, err := mskAuditCall(p, ctx, method, path, body, query)
	require.NoError(t, err, "%s %s", method, path)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s %s: %s", method, path, resp.Body)
	return resp.Body
}

// mskAuditRefusal requires the call to be refused with code, status and invalidParameter.
func mskAuditRefusal(t *testing.T, err error, code string, status int, param string) {
	t.Helper()
	require.Error(t, err)
	var awsErr *emulator.AWSError
	require.Truef(t, errors.As(err, &awsErr), "want an AWSError, got %T: %v", err, err)
	require.Equal(t, code, awsErr.Code, "%v", awsErr)
	require.Equal(t, status, awsErr.HTTPStatus, "%v", awsErr)
	require.Equalf(t, param, awsErr.Members["invalidParameter"], "invalidParameter names the member that caused %v", awsErr)
}

// mskAuditARN creates a cluster from body and returns its ARN.
func mskAuditARN(t *testing.T, p *emulator.MSKPlugin, ctx *emulator.RequestContext, path string, body map[string]any) string {
	t.Helper()
	var out struct {
		ClusterARN string `json:"clusterArn"`
	}
	raw := mskAuditOK(t, p, ctx, http.MethodPost, path, body, nil)
	require.NoError(t, json.Unmarshal(raw, &out), "%s", raw)
	require.NotEmpty(t, out.ClusterARN, "%s", raw)
	return out.ClusterARN
}

// with returns a copy of the minimal create body with one member changed, or removed when v is nil.
func mskAuditWith(name, member string, v any) map[string]any {
	b := mskMinimalCluster(name)
	if strings.HasPrefix(member, "brokerNodeGroupInfo.") {
		bng := b["brokerNodeGroupInfo"].(map[string]any)
		key := strings.TrimPrefix(member, "brokerNodeGroupInfo.")
		if v == nil {
			delete(bng, key)
		} else {
			bng[key] = v
		}
		return b
	}
	if v == nil {
		delete(b, member)
	} else {
		b[member] = v
	}
	return b
}

func TestMSKCreate_RefusesWhatThePageRequires(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("a", 65)
	for _, tc := range []struct {
		name  string
		path  string
		body  any
		param string
	}{
		// CreateCluster: clusters.html marks clusterName, kafkaVersion, numberOfBrokerNodes and
		// brokerNodeGroupInfo Required: True; clientSubnets and instanceType are True inside it.
		{"v1 no clusterName", "/v1/clusters", mskAuditWith("x", "clusterName", nil), "clusterName"},
		{"v1 clusterName over 64", "/v1/clusters", mskAuditWith(long, "clusterName", long), "clusterName"},
		{"v1 no kafkaVersion", "/v1/clusters", mskAuditWith("x", "kafkaVersion", nil), "kafkaVersion"},
		{"v1 empty kafkaVersion", "/v1/clusters", mskAuditWith("x", "kafkaVersion", ""), "kafkaVersion"},
		{"v1 no numberOfBrokerNodes", "/v1/clusters", mskAuditWith("x", "numberOfBrokerNodes", nil), "numberOfBrokerNodes"},
		{"v1 zero brokers", "/v1/clusters", mskAuditWith("x", "numberOfBrokerNodes", 0), "numberOfBrokerNodes"},
		{"v1 no brokerNodeGroupInfo", "/v1/clusters", mskAuditWith("x", "brokerNodeGroupInfo", nil), "brokerNodeGroupInfo"},
		{"v1 no clientSubnets", "/v1/clusters", mskAuditWith("x", "brokerNodeGroupInfo.clientSubnets", nil), "clientSubnets"},
		{"v1 no instanceType", "/v1/clusters", mskAuditWith("x", "brokerNodeGroupInfo.instanceType", nil), "instanceType"},
		{"v1 instanceType under 5", "/v1/clusters", mskAuditWith("x", "brokerNodeGroupInfo.instanceType", "m5"), "instanceType"},
		{"v1 volumeSize over 16384", "/v1/clusters", mskAuditWith("x", "brokerNodeGroupInfo.storageInfo",
			map[string]any{"ebsStorageInfo": map[string]any{"volumeSize": 20000}}), "volumeSize"},
		{"v1 encryptionAtRest without a key", "/v1/clusters", mskAuditWith("x", "encryptionInfo",
			map[string]any{"encryptionAtRest": map[string]any{}}), "dataVolumeKMSKeyId"},
		{"v1 clientBroker off-enum", "/v1/clusters", mskAuditWith("x", "encryptionInfo",
			map[string]any{"encryptionInTransit": map[string]any{"clientBroker": "SSL"}}), "clientBroker"},
		{"v1 configurationInfo without arn", "/v1/clusters", mskAuditWith("x", "configurationInfo",
			map[string]any{"revision": 1}), "arn"},
		{"v1 configurationInfo revision 0", "/v1/clusters", mskAuditWith("x", "configurationInfo",
			map[string]any{"arn": "arn:aws:kafka:us-east-1:123456789012:configuration/c/1"}), "revision"},
		// CreateClusterV2: clusterName is the request's only Required: True member.
		{"v2 no clusterName", "/api/v2/clusters", map[string]any{"provisioned": map[string]any{}}, "clusterName"},
		{"v2 neither kind", "/api/v2/clusters", map[string]any{"clusterName": "x"}, "provisioned"},
		{"v2 both kinds", "/api/v2/clusters", map[string]any{"clusterName": "x", "provisioned": map[string]any{},
			"serverless": map[string]any{}}, "serverless"},
		{"v2 provisioned brokerNodeGroupInfo without clientSubnets", "/api/v2/clusters", map[string]any{"clusterName": "x",
			"provisioned": map[string]any{"brokerNodeGroupInfo": map[string]any{"instanceType": "kafka.m5.large"}}}, "clientSubnets"},
		{"v2 serverless without vpcConfigs", "/api/v2/clusters", map[string]any{"clusterName": "x",
			"serverless": map[string]any{"clientAuthentication": map[string]any{}}}, "vpcConfigs"},
		{"v2 serverless vpcConfig without subnetIds", "/api/v2/clusters", map[string]any{"clusterName": "x",
			"serverless": map[string]any{"vpcConfigs": []any{map[string]any{}}, "clientAuthentication": map[string]any{}}}, "subnetIds"},
		{"v2 serverless without clientAuthentication", "/api/v2/clusters", map[string]any{"clusterName": "x",
			"serverless": map[string]any{"vpcConfigs": []any{map[string]any{"subnetIds": []string{"subnet-1"}}}}}, "clientAuthentication"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, ctx := mskAuditPlugin(t, emulator.NewMemoryStateManager())
			_, err := mskAuditCall(p, ctx, http.MethodPost, tc.path, tc.body, nil)
			mskAuditRefusal(t, err, "BadRequestException", http.StatusBadRequest, tc.param)
			// Nothing was created by a refused create.
			list := mskAuditOK(t, p, ctx, http.MethodGet, "/api/v2/clusters", nil, nil)
			require.JSONEq(t, `{"clusterInfoList":[]}`, string(list))
		})
	}
}

// Every routed MSK operation that can refuse names the parameter that caused it (#1211's last
// criterion).
func TestMSK_EveryRoutedRefusalNamesItsParameter(t *testing.T) {
	t.Parallel()
	p, ctx := mskAuditPlugin(t, emulator.NewMemoryStateManager())
	arn := mskAuditARN(t, p, ctx, "/v1/clusters", mskMinimalCluster("named"))
	missing := arn[:strings.LastIndex(arn, "/")+1] + "00000000-0000-4000-8000-000000000000-1"
	for _, tc := range []struct {
		op, method, path string
		body             any
		query            map[string]string
		code             string
		status           int
		param            string
	}{
		{"CreateCluster", http.MethodPost, "/v1/clusters", mskMinimalCluster("named"), nil, "ConflictException", http.StatusConflict, "clusterName"},
		{"CreateClusterV2", http.MethodPost, "/api/v2/clusters", map[string]any{"clusterName": "named", "provisioned": map[string]any{}}, nil, "ConflictException", http.StatusConflict, "clusterName"},
		{"DescribeCluster", http.MethodGet, "/v1/clusters/" + missing, nil, nil, "NotFoundException", http.StatusNotFound, "clusterArn"},
		{"DescribeClusterV2", http.MethodGet, "/api/v2/clusters/notanarn", nil, nil, "BadRequestException", http.StatusBadRequest, "clusterArn"},
		{"GetBootstrapBrokers", http.MethodGet, "/v1/clusters/" + missing + "/bootstrap-brokers", nil, nil, "NotFoundException", http.StatusNotFound, "clusterArn"},
		{"ListNodes", http.MethodGet, "/v1/clusters/" + arn + "/nodes", nil, map[string]string{"nextToken": "not-issued"}, "BadRequestException", http.StatusBadRequest, "nextToken"},
		{"ListClusters", http.MethodGet, "/v1/clusters", nil, map[string]string{"maxResults": "0"}, "BadRequestException", http.StatusBadRequest, "maxResults"},
		{"ListClustersV2", http.MethodGet, "/api/v2/clusters", nil, map[string]string{"clusterTypeFilter": "EXPRESS"}, "BadRequestException", http.StatusBadRequest, "clusterTypeFilter"},
		{"DeleteCluster", http.MethodDelete, "/v1/clusters/", nil, nil, "BadRequestException", http.StatusBadRequest, "clusterArn"},
	} {
		t.Run(tc.op, func(t *testing.T) {
			_, err := mskAuditCall(p, ctx, tc.method, tc.path, tc.body, tc.query)
			mskAuditRefusal(t, err, tc.code, tc.status, tc.param)
		})
	}
	t.Run("an undecodable body names no parameter", func(t *testing.T) {
		_, err := mskAuditCall(p, ctx, http.MethodPost, "/v1/clusters", "{not json", nil)
		mskAuditRefusal(t, err, "BadRequestException", http.StatusBadRequest, "")
	})
}

// The wire carries invalidParameter beside message, and the shape name in x-amzn-ErrorType, which is
// what an SDK's REST-JSON deserializer matches on.
func TestMSK_ARefusalOnTheWireCarriesInvalidParameter(t *testing.T) {
	t.Parallel()
	ts := emulator.StartTestServer(t)
	send := func(body string) (*http.Response, map[string]any) {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, ts.URL+"/v1/clusters", strings.NewReader(body))
		require.NoError(t, err)
		req.Host = "kafka.us-east-1.amazonaws.com"
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		raw, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		var doc map[string]any
		require.NoError(t, json.Unmarshal(raw, &doc), "%s", raw)
		return resp, doc
	}

	resp, doc := send(`{"clusterName":"wire","numberOfBrokerNodes":2,"brokerNodeGroupInfo":{"instanceType":"kafka.m5.large","clientSubnets":["s"]}}`)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "BadRequestException", resp.Header.Get("x-amzn-ErrorType"))
	require.Equal(t, "kafkaVersion", doc["invalidParameter"], "%v", doc)
	require.Equal(t, "kafkaVersion is required", doc["message"], "%v", doc)

	_, doc = send(`{not json`)
	_, present := doc["invalidParameter"]
	require.False(t, present, "a body that will not decode has no parameter to blame, so the member is omitted: %v", doc)
}

func TestMSKV2_ProvisionedMembersAreOptionalAndServerlessIsModeled(t *testing.T) {
	t.Parallel()
	p, ctx := mskAuditPlugin(t, emulator.NewMemoryStateManager())

	// ProvisionedRequest marks every member Required: False, so this is accepted, and nothing is
	// defaulted into the members the request left out.
	bare := mskAuditOK(t, p, ctx, http.MethodPost, "/api/v2/clusters", map[string]any{"clusterName": "bare", "provisioned": map[string]any{}}, nil)
	require.Contains(t, string(bare), `"clusterType":"PROVISIONED"`, "CreateClusterV2Response publishes clusterType: %s", bare)
	var bareOut struct {
		ClusterARN string `json:"clusterArn"`
	}
	require.NoError(t, json.Unmarshal(bare, &bareOut))
	described := mskAuditOK(t, p, ctx, http.MethodGet, "/api/v2/clusters/"+bareOut.ClusterARN, nil, nil)
	require.NotContains(t, string(described), "kafkaVersion", "no kafkaVersion is invented: %s", described)
	require.NotContains(t, string(described), "numberOfBrokerNodes", "no broker count is invented: %s", described)

	serverless := mskAuditOK(t, p, ctx, http.MethodPost, "/api/v2/clusters", map[string]any{
		"clusterName": "sls",
		"serverless": map[string]any{
			"vpcConfigs":           []any{map[string]any{"subnetIds": []string{"subnet-a", "subnet-b"}, "securityGroupIds": []string{"sg-1"}}},
			"clientAuthentication": map[string]any{"sasl": map[string]any{"iam": map[string]any{"enabled": true}}},
		},
	}, nil)
	require.Contains(t, string(serverless), `"clusterType":"SERVERLESS"`, "%s", serverless)
	var slsOut struct {
		ClusterARN string `json:"clusterArn"`
	}
	require.NoError(t, json.Unmarshal(serverless, &slsOut))

	got := mskAuditOK(t, p, ctx, http.MethodGet, "/api/v2/clusters/"+slsOut.ClusterARN, nil, nil)
	var doc struct {
		ClusterInfo map[string]json.RawMessage `json:"clusterInfo"`
	}
	require.NoError(t, json.Unmarshal(got, &doc), "%s", got)
	require.JSONEq(t, `"SERVERLESS"`, string(doc.ClusterInfo["clusterType"]))
	require.JSONEq(t, `{"vpcConfigs":[{"subnetIds":["subnet-a","subnet-b"],"securityGroupIds":["sg-1"]}],"clientAuthentication":{"sasl":{"iam":{"enabled":true}}}}`,
		string(doc.ClusterInfo["serverless"]))
	_, provisioned := doc.ClusterInfo["provisioned"]
	require.False(t, provisioned, "a serverless cluster answers no provisioned member: %s", got)

	// ListClusters (v1) lists provisioned clusters; ListClustersV2 lists both, and filters by type.
	v1 := mskAuditOK(t, p, ctx, http.MethodGet, "/v1/clusters", nil, nil)
	require.NotContains(t, string(v1), `"sls"`, "%s", v1)
	only := mskAuditOK(t, p, ctx, http.MethodGet, "/api/v2/clusters", nil, map[string]string{"clusterTypeFilter": "SERVERLESS"})
	require.Contains(t, string(only), `"clusterName":"sls"`, "%s", only)
	require.NotContains(t, string(only), `"bare"`, "%s", only)

	brokers := mskAuditOK(t, p, ctx, http.MethodGet, "/v1/clusters/"+slsOut.ClusterARN+"/bootstrap-brokers", nil, nil)
	require.Regexp(t, `^\{"bootstrapBrokerStringSaslIam":"boot-[0-9a-f]{6}\.c2\.kafka-serverless\.us-east-1\.amazonaws\.com:9098"\}$`, string(brokers))
	nodes := mskAuditOK(t, p, ctx, http.MethodGet, "/v1/clusters/"+slsOut.ClusterARN+"/nodes", nil, nil)
	require.JSONEq(t, `{"nodeInfoList":[]}`, string(nodes))
}

func TestMSKLists_PageAndFilter(t *testing.T) {
	t.Parallel()
	p, ctx := mskAuditPlugin(t, emulator.NewMemoryStateManager())
	for i := 1; i <= 5; i++ {
		mskAuditARN(t, p, ctx, "/v1/clusters", mskMinimalCluster(fmt.Sprintf("page-%d", i)))
	}
	mskAuditARN(t, p, ctx, "/v1/clusters", mskMinimalCluster("other"))

	for _, path := range []string{"/v1/clusters", "/api/v2/clusters"} {
		t.Run(path, func(t *testing.T) {
			var names []string
			token, pages := "", 0
			for {
				q := map[string]string{"maxResults": "2", "clusterNameFilter": "page-"}
				if token != "" {
					q["nextToken"] = token
				}
				raw := mskAuditOK(t, p, ctx, http.MethodGet, path, nil, q)
				var doc struct {
					List []struct {
						Name string `json:"clusterName"`
					} `json:"clusterInfoList"`
					Next *string `json:"nextToken"`
				}
				require.NoError(t, json.Unmarshal(raw, &doc), "%s", raw)
				require.LessOrEqual(t, len(doc.List), 2, "%s", raw)
				for _, c := range doc.List {
					names = append(names, c.Name)
				}
				pages++
				if doc.Next == nil {
					break
				}
				require.NotEmpty(t, *doc.Next, "a nextToken is never sent empty: %s", raw)
				token = *doc.Next
			}
			require.Equal(t, 3, pages, "five clusters at two per page")
			require.Equal(t, []string{"page-1", "page-2", "page-3", "page-4", "page-5"}, names,
				"the token round-trips and the filter narrows: other is not listed")
		})
	}
	for _, bad := range []string{"0", "101", "two"} {
		_, err := mskAuditCall(p, ctx, http.MethodGet, "/v1/clusters", nil, map[string]string{"maxResults": bad})
		mskAuditRefusal(t, err, "BadRequestException", http.StatusBadRequest, "maxResults")
	}
	_, err := mskAuditCall(p, ctx, http.MethodGet, "/api/v2/clusters", nil, map[string]string{"nextToken": "bm90LWlzc3VlZA=="})
	mskAuditRefusal(t, err, "BadRequestException", http.StatusBadRequest, "nextToken")
}

func TestMSKListNodes_PagesAndReportsBrokerEndpoints(t *testing.T) {
	t.Parallel()
	p, ctx := mskAuditPlugin(t, emulator.NewMemoryStateManager())
	body := mskMinimalCluster("nodes")
	body["numberOfBrokerNodes"] = 3
	arn := mskAuditARN(t, p, ctx, "/v1/clusters", body)

	first := mskAuditOK(t, p, ctx, http.MethodGet, "/v1/clusters/"+arn+"/nodes", nil, map[string]string{"maxResults": "2"})
	var page struct {
		Nodes []struct {
			BrokerNodeInfo struct {
				BrokerID     float64  `json:"brokerId"`
				ClientSubnet string   `json:"clientSubnet"`
				Endpoints    []string `json:"endpoints"`
			} `json:"brokerNodeInfo"`
		} `json:"nodeInfoList"`
		Next string `json:"nextToken"`
	}
	require.NoError(t, json.Unmarshal(first, &page), "%s", first)
	require.Len(t, page.Nodes, 2)
	require.NotEmpty(t, page.Next)
	require.Equal(t, "subnet-1", page.Nodes[0].BrokerNodeInfo.ClientSubnet)
	require.Equal(t, "subnet-2", page.Nodes[1].BrokerNodeInfo.ClientSubnet, "brokers are spread over the subnets")
	require.Regexp(t, `^b-1\.nodes\.[0-9a-f]{6}\.c2\.kafka\.us-east-1\.amazonaws\.com$`, page.Nodes[0].BrokerNodeInfo.Endpoints[0])

	rest := mskAuditOK(t, p, ctx, http.MethodGet, "/v1/clusters/"+arn+"/nodes", nil, map[string]string{"nextToken": page.Next})
	require.NotContains(t, string(rest), "nextToken", "the last page omits nextToken: %s", rest)
	require.Contains(t, string(rest), `"brokerId":3`, "%s", rest)

	// The endpoints are the hosts GetBootstrapBrokers names.
	brokers := mskAuditOK(t, p, ctx, http.MethodGet, "/v1/clusters/"+arn+"/bootstrap-brokers", nil, nil)
	require.Contains(t, string(brokers), page.Nodes[0].BrokerNodeInfo.Endpoints[0]+":9094")
}

func TestMSKBootstrapBrokers_AnswerTheStringsTheConfigurationImplies(t *testing.T) {
	t.Parallel()
	host := `b-[12]\.bb\.[0-9a-f]{6}\.c2\.kafka\.us-east-1\.amazonaws\.com`
	for _, tc := range []struct {
		name string
		set  map[string]any
		want map[string]string // member → port
	}{
		{"default encryption is TLS", nil, map[string]string{"bootstrapBrokerStringTls": "9094"}},
		{"PLAINTEXT", map[string]any{"encryptionInfo": map[string]any{"encryptionInTransit": map[string]any{"clientBroker": "PLAINTEXT"}}},
			map[string]string{"bootstrapBrokerString": "9092"}},
		{"TLS_PLAINTEXT", map[string]any{"encryptionInfo": map[string]any{"encryptionInTransit": map[string]any{"clientBroker": "TLS_PLAINTEXT"}}},
			map[string]string{"bootstrapBrokerString": "9092", "bootstrapBrokerStringTls": "9094"}},
		{"SASL IAM and SCRAM", map[string]any{"clientAuthentication": map[string]any{"sasl": map[string]any{
			"iam": map[string]any{"enabled": true}, "scram": map[string]any{"enabled": true}}}},
			map[string]string{"bootstrapBrokerStringSaslIam": "9098", "bootstrapBrokerStringSaslScram": "9096"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, ctx := mskAuditPlugin(t, emulator.NewMemoryStateManager())
			body := mskMinimalCluster("bb")
			for k, v := range tc.set {
				body[k] = v
			}
			arn := mskAuditARN(t, p, ctx, "/v1/clusters", body)
			raw := mskAuditOK(t, p, ctx, http.MethodGet, "/v1/clusters/"+arn+"/bootstrap-brokers", nil, nil)
			var got map[string]string
			require.NoError(t, json.Unmarshal(raw, &got), "%s", raw)
			require.Len(t, got, len(tc.want), "%s", raw)
			for member, port := range tc.want {
				require.Regexpf(t, "^"+host+":"+port+","+host+":"+port+"$", got[member], "%s: %s", member, raw)
			}
		})
	}
}

func TestMSKDescribe_EchoesTheConfigurationACreateSent(t *testing.T) {
	t.Parallel()
	p, ctx := mskAuditPlugin(t, emulator.NewMemoryStateManager())
	body := mskMinimalCluster("echo")
	body["enhancedMonitoring"] = "PER_BROKER"
	body["storageMode"] = "TIERED"
	body["configurationInfo"] = map[string]any{"arn": "arn:aws:kafka:us-east-1:123456789012:configuration/cfg/1", "revision": 2}
	body["clientAuthentication"] = map[string]any{"tls": map[string]any{"enabled": true, "certificateAuthorityArnList": []string{"arn:ca"}}}
	body["encryptionInfo"] = map[string]any{"encryptionAtRest": map[string]any{"dataVolumeKMSKeyId": "arn:aws:kms:us-east-1:123456789012:key/k"}}
	arn := mskAuditARN(t, p, ctx, "/v1/clusters", body)

	for _, path := range []string{"/v1/clusters/" + arn, "/api/v2/clusters/" + arn} {
		raw := mskAuditOK(t, p, ctx, http.MethodGet, path, nil, nil)
		for _, want := range []string{
			`"enhancedMonitoring":"PER_BROKER"`,
			`"storageMode":"TIERED"`,
			`"configurationArn":"arn:aws:kafka:us-east-1:123456789012:configuration/cfg/1"`,
			`"configurationRevision":2`,
			`"clientAuthentication":{"tls":{"certificateAuthorityArnList":["arn:ca"],"enabled":true}}`,
			// The request named no in-transit settings, so the page's stated defaults are answered.
			`"encryptionInTransit":{"clientBroker":"TLS","inCluster":true}`,
			`"encryptionAtRest":{"dataVolumeKMSKeyId":"arn:aws:kms:us-east-1:123456789012:key/k"}`,
		} {
			require.Containsf(t, string(raw), want, "%s answers %s: %s", path, want, raw)
		}
		require.False(t, bytes.Contains(raw, []byte("null")), "no member is sent null: %s", raw)
	}
}

// A store fault in the create, list and load paths is an error, never a refusal or a shorter list.
func TestMSK_AStoreFaultIsAnError(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		arm          func(*cfFaultStateManager)
		method, path string
		body         any
	}{
		{"CreateCluster index read", func(m *cfFaultStateManager) { m.failGet = "cluster_ids:" }, http.MethodPost, "/v1/clusters", mskMinimalCluster("second")},
		{"CreateCluster record write", func(m *cfFaultStateManager) { m.failPut = "cluster:" }, http.MethodPost, "/v1/clusters", mskMinimalCluster("second")},
		{"ListClusters record read", func(m *cfFaultStateManager) { m.failGet = "cluster:" }, http.MethodGet, "/v1/clusters", nil},
		{"ListClustersV2 corrupt record", func(m *cfFaultStateManager) { m.corruptGet = "cluster:" }, http.MethodGet, "/api/v2/clusters", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fault := &cfFaultStateManager{inner: emulator.NewMemoryStateManager()}
			p, ctx := mskAuditPlugin(t, fault)
			mskAuditARN(t, p, ctx, "/v1/clusters", mskMinimalCluster("first"))
			tc.arm(fault)
			_, err := mskAuditCall(p, ctx, tc.method, tc.path, tc.body, nil)
			require.Error(t, err, "%s must fail on a store fault", tc.name)
			var awsErr *emulator.AWSError
			require.Falsef(t, errors.As(err, &awsErr), "%s answered a store fault as %v", tc.name, awsErr)
		})
	}
}
