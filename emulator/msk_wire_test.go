package emulator_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/scttfrdmn/substrate/emulator"
)

// These tests assert on the raw response bytes rather than round-tripping through
// a Go struct. That distinction is the whole point: a struct marshaled and
// unmarshaled by its own definition agrees with itself whatever its tags say, and
// Go's json.Unmarshal matches keys case-insensitively on top of that — so the
// pre-#529 tests were green while `aws kafka list-clusters` returned nothing at
// all. Only a literal-key assertion sees the difference.

// mskWireBody sends one request and decodes the response into a generic map, so
// every assertion is against the key a real SDK would look for.
func mskWireBody(t *testing.T, p *emulator.MSKPlugin, ctx *emulator.RequestContext,
	method, path string, body map[string]any,
) (map[string]any, []byte) {
	t.Helper()
	resp, err := p.HandleRequest(ctx, mskRequest(method, path, body))
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s %s: want 200, got %d", method, path, resp.StatusCode)
	}
	var m map[string]any
	if err := json.Unmarshal(resp.Body, &m); err != nil {
		t.Fatalf("%s %s: unmarshal body: %v", method, path, err)
	}
	return m, resp.Body
}

// mskWireCreate creates one cluster and returns the decoded create response plus
// its raw bytes.
func mskWireCreate(t *testing.T, p *emulator.MSKPlugin, ctx *emulator.RequestContext, name string) (map[string]any, []byte) {
	t.Helper()
	return mskWireBody(t, p, ctx, "POST", "/v1/clusters", map[string]any{
		"clusterName":         name,
		"kafkaVersion":        "3.5.1",
		"numberOfBrokerNodes": 3,
		"brokerNodeGroupInfo": map[string]any{
			"instanceType":  "kafka.m5.large",
			"clientSubnets": []string{"subnet-1", "subnet-2"},
			"storageInfo": map[string]any{
				"ebsStorageInfo": map[string]any{"volumeSize": 100},
			},
		},
		"tags": map[string]string{"env": "test"},
	})
}

// mskWireCluster creates one cluster and returns its ARN.
func mskWireCluster(t *testing.T, p *emulator.MSKPlugin, ctx *emulator.RequestContext, name string) string {
	t.Helper()
	m, _ := mskWireCreate(t, p, ctx, name)
	arn, ok := m["clusterArn"].(string)
	if !ok || arn == "" {
		t.Fatalf("create: want a clusterArn, got %v", m)
	}
	return arn
}

// requireKeys fails unless every named key is present in m.
func requireKeys(t *testing.T, what string, m map[string]any, keys ...string) {
	t.Helper()
	for _, k := range keys {
		if _, ok := m[k]; !ok {
			present := make([]string, 0, len(m))
			for kk := range m {
				present = append(present, kk)
			}
			t.Errorf("%s: missing wire key %q; present keys: %v", what, k, present)
		}
	}
}

// requireValues asserts each key's value, not merely its presence. Presence alone
// does not prove a projection carried a member: a Wire function that forgets a
// field still emits its key, holding Go's zero value, because the wire type's tag
// has no omitempty. A dropped numberOfBrokerNodes reads as a cluster with no
// brokers — so every member a projection copies is checked by value here.
func requireValues(t *testing.T, what string, m map[string]any, want map[string]any) {
	t.Helper()
	for k, w := range want {
		got, ok := m[k]
		if !ok {
			t.Errorf("%s: missing wire key %q", what, k)
			continue
		}
		if got != w {
			t.Errorf("%s: %s = %#v, want %#v", what, k, got, w)
		}
	}
}

func TestMSKWire_CreateCluster(t *testing.T) {
	p, ctx := setupMSKPlugin(t)
	m, raw := mskWireCreate(t, p, ctx, "wire-create")

	// CreateClusterResponse declares exactly these three members.
	requireKeys(t, "CreateCluster", m, "clusterArn", "clusterName", "state")
	if got := m["clusterName"]; got != "wire-create" {
		t.Errorf("clusterName: want wire-create, got %v", got)
	}
	if got := m["state"]; got != "ACTIVE" {
		t.Errorf("state: want ACTIVE, got %v", got)
	}
	mskAssertNoInternalFields(t, "CreateCluster", raw)
}

func TestMSKWire_DescribeCluster(t *testing.T) {
	p, ctx := setupMSKPlugin(t)
	arn := mskWireCluster(t, p, ctx, "wire-describe")

	m, raw := mskWireBody(t, p, ctx, "GET", "/v1/clusters/"+arn, nil)
	requireKeys(t, "DescribeCluster", m, "clusterInfo")
	info, ok := m["clusterInfo"].(map[string]any)
	if !ok {
		t.Fatalf("clusterInfo: want an object, got %T", m["clusterInfo"])
	}
	requireKeys(t, "ClusterInfo", info,
		"clusterArn", "clusterName", "state", "brokerNodeGroupInfo",
		"currentBrokerSoftwareInfo", "numberOfBrokerNodes", "tags", "creationTime")
	requireValues(t, "ClusterInfo", info, map[string]any{
		"clusterArn":          arn,
		"clusterName":         "wire-describe",
		"state":               "ACTIVE",
		"numberOfBrokerNodes": float64(3),
	})
	if tags, ok := info["tags"].(map[string]any); !ok || tags["env"] != "test" {
		t.Errorf("tags: want env=test, got %#v", info["tags"])
	}
	if ct, ok := info["creationTime"].(string); !ok || ct == "" {
		t.Errorf("creationTime: want a timestamp, got %#v", info["creationTime"])
	}
	mskAssertNoInternalFields(t, "DescribeCluster", raw)
}

func TestMSKWire_ListClusters(t *testing.T) {
	p, ctx := setupMSKPlugin(t)
	mskWireCluster(t, p, ctx, "wire-list")

	m, raw := mskWireBody(t, p, ctx, "GET", "/v1/clusters", nil)
	// The envelope is clusterInfoList, not ClusterInfoList: botocore parses
	// {"ClusterInfoList": […]} to an empty result with no error.
	requireKeys(t, "ListClusters", m, "clusterInfoList")
	list, ok := m["clusterInfoList"].([]any)
	if !ok || len(list) != 1 {
		t.Fatalf("clusterInfoList: want one element, got %v", m["clusterInfoList"])
	}
	elem, ok := list[0].(map[string]any)
	if !ok {
		t.Fatalf("clusterInfoList[0]: want an object, got %T", list[0])
	}
	requireKeys(t, "ListClusters element", elem,
		"clusterArn", "clusterName", "state", "numberOfBrokerNodes", "creationTime")
	requireValues(t, "ListClusters element", elem, map[string]any{
		"clusterName":         "wire-list",
		"state":               "ACTIVE",
		"numberOfBrokerNodes": float64(3),
	})
	mskAssertNoInternalFields(t, "ListClusters", raw)
}

// TestMSKWire_ListClustersEmpty pins the empty case as a list rather than null: a
// caller iterating the result must not have to handle a JSON null.
func TestMSKWire_ListClustersEmpty(t *testing.T) {
	p, ctx := setupMSKPlugin(t)
	m, _ := mskWireBody(t, p, ctx, "GET", "/v1/clusters", nil)
	list, ok := m["clusterInfoList"].([]any)
	if !ok {
		t.Fatalf("clusterInfoList: want an empty list, got %#v", m["clusterInfoList"])
	}
	if len(list) != 0 {
		t.Errorf("clusterInfoList: want 0 elements, got %d", len(list))
	}
}

func TestMSKWire_DeleteCluster(t *testing.T) {
	p, ctx := setupMSKPlugin(t)
	arn := mskWireCluster(t, p, ctx, "wire-delete")

	m, raw := mskWireBody(t, p, ctx, "DELETE", "/v1/clusters/"+arn, nil)
	requireKeys(t, "DeleteCluster", m, "clusterArn", "state")
	if got := m["state"]; got != "DELETING" {
		t.Errorf("state: want DELETING, got %v", got)
	}
	// DeleteClusterResponse declares only clusterArn and state.
	if _, ok := m["clusterName"]; ok {
		t.Error("DeleteCluster: clusterName is not a member of DeleteClusterResponse")
	}
	mskAssertNoInternalFields(t, "DeleteCluster", raw)
}

func TestMSKWire_GetBootstrapBrokers(t *testing.T) {
	p, ctx := setupMSKPlugin(t)
	arn := mskWireCluster(t, p, ctx, "wire-brokers")

	m, raw := mskWireBody(t, p, ctx, "GET", "/v1/clusters/"+arn+"/bootstrap-brokers", nil)
	requireKeys(t, "GetBootstrapBrokers", m, "bootstrapBrokerString")
	mskAssertNoInternalFields(t, "GetBootstrapBrokers", raw)
}

func TestMSKWire_ListNodes(t *testing.T) {
	p, ctx := setupMSKPlugin(t)
	arn := mskWireCluster(t, p, ctx, "wire-nodes")

	m, raw := mskWireBody(t, p, ctx, "GET", "/v1/clusters/"+arn+"/nodes", nil)
	requireKeys(t, "ListNodes", m, "nodeInfoList")
	list, ok := m["nodeInfoList"].([]any)
	if !ok || len(list) != 3 {
		t.Fatalf("nodeInfoList: want three elements, got %v", m["nodeInfoList"])
	}
	node, ok := list[0].(map[string]any)
	if !ok {
		t.Fatalf("nodeInfoList[0]: want an object, got %T", list[0])
	}
	// nodeARN: the acronym stays capitalised in this one member.
	requireKeys(t, "NodeInfo", node, "nodeARN", "nodeType", "instanceType", "brokerNodeInfo")
	requireValues(t, "NodeInfo", node, map[string]any{
		"nodeType":     "BROKER",
		"instanceType": "kafka.m5.large",
	})
	if s, ok := node["nodeARN"].(string); !ok || s == "" {
		t.Errorf("nodeARN: want a non-empty ARN, got %#v", node["nodeARN"])
	}
	if _, ok := node["nodeArn"]; ok {
		t.Error("NodeInfo: the member is nodeARN, not nodeArn")
	}
	bni, ok := node["brokerNodeInfo"].(map[string]any)
	if !ok {
		t.Fatalf("brokerNodeInfo: want an object, got %T", node["brokerNodeInfo"])
	}
	requireKeys(t, "BrokerNodeInfo", bni, "brokerId", "clientSubnet", "currentBrokerSoftwareInfo")
	requireValues(t, "BrokerNodeInfo", bni, map[string]any{
		"brokerId":     float64(1),
		"clientSubnet": "subnet-1",
	})
	sw, ok := bni["currentBrokerSoftwareInfo"].(map[string]any)
	if !ok {
		t.Fatalf("currentBrokerSoftwareInfo: want an object, got %T", bni["currentBrokerSoftwareInfo"])
	}
	requireValues(t, "BrokerSoftwareInfo", sw, map[string]any{"kafkaVersion": "3.5.1"})
	mskAssertNoInternalFields(t, "ListNodes", raw)
}

// TestMSKWire_NestedProjection walks the deepest nesting a cluster response has —
// clusterInfo → brokerNodeGroupInfo → storageInfo → ebsStorageInfo — because a
// projection that converts only the top level leaves the nested value PascalCase
// and a caller parses a cluster whose broker configuration is empty.
func TestMSKWire_NestedProjection(t *testing.T) {
	p, ctx := setupMSKPlugin(t)
	arn := mskWireCluster(t, p, ctx, "wire-nested")

	m, _ := mskWireBody(t, p, ctx, "GET", "/v1/clusters/"+arn, nil)
	info := m["clusterInfo"].(map[string]any)

	bng, ok := info["brokerNodeGroupInfo"].(map[string]any)
	if !ok {
		t.Fatalf("brokerNodeGroupInfo: want an object, got %T", info["brokerNodeGroupInfo"])
	}
	requireKeys(t, "BrokerNodeGroupInfo", bng, "instanceType", "clientSubnets", "storageInfo")
	requireValues(t, "BrokerNodeGroupInfo", bng, map[string]any{"instanceType": "kafka.m5.large"})
	subnets, ok := bng["clientSubnets"].([]any)
	if !ok || len(subnets) != 2 || subnets[0] != "subnet-1" || subnets[1] != "subnet-2" {
		t.Errorf("clientSubnets: want [subnet-1 subnet-2], got %#v", bng["clientSubnets"])
	}

	si, ok := bng["storageInfo"].(map[string]any)
	if !ok {
		t.Fatalf("storageInfo: want an object, got %T", bng["storageInfo"])
	}
	requireKeys(t, "StorageInfo", si, "ebsStorageInfo")

	ebs, ok := si["ebsStorageInfo"].(map[string]any)
	if !ok {
		t.Fatalf("ebsStorageInfo: want an object, got %T", si["ebsStorageInfo"])
	}
	requireKeys(t, "EBSStorageInfo", ebs, "volumeSize")
	if got := ebs["volumeSize"]; got != float64(100) {
		t.Errorf("volumeSize: want 100, got %v", got)
	}

	sw, ok := info["currentBrokerSoftwareInfo"].(map[string]any)
	if !ok {
		t.Fatalf("currentBrokerSoftwareInfo: want an object, got %T", info["currentBrokerSoftwareInfo"])
	}
	requireKeys(t, "BrokerSoftwareInfo", sw, "kafkaVersion")
	if got := sw["kafkaVersion"]; got != "3.5.1" {
		t.Errorf("kafkaVersion: want 3.5.1, got %v", got)
	}
}

// TestMSKWire_UnsetOptionalsAreOmitted pins absent-versus-null. Real MSK omits an
// unset member; substrate used to send "securityGroups": null and
// "volumeSize": 0 because the state struct it marshaled had no omitempty, and a
// caller distinguishing the two reads a real observable.
func TestMSKWire_UnsetOptionalsAreOmitted(t *testing.T) {
	p, ctx := setupMSKPlugin(t)
	// No securityGroups, no storageInfo, no tags.
	m, _ := mskWireBody(t, p, ctx, "POST", "/v1/clusters", map[string]any{
		"clusterName":         "wire-sparse",
		"numberOfBrokerNodes": 2,
		"brokerNodeGroupInfo": map[string]any{
			"instanceType":  "kafka.m5.large",
			"clientSubnets": []string{"subnet-1"},
		},
	})
	arn := m["clusterArn"].(string)

	body, raw := mskWireBody(t, p, ctx, "GET", "/v1/clusters/"+arn, nil)
	info := body["clusterInfo"].(map[string]any)
	if _, ok := info["tags"]; ok {
		t.Error("tags: an unset map must be omitted, not sent empty")
	}
	bng := info["brokerNodeGroupInfo"].(map[string]any)
	if _, ok := bng["securityGroups"]; ok {
		t.Error("securityGroups: an unset list must be omitted, not sent as null")
	}
	if _, ok := bng["storageInfo"]; ok {
		t.Error("storageInfo: unset storage must be omitted rather than reporting volumeSize 0")
	}
	if bytes.Contains(raw, []byte("null")) {
		t.Errorf("response contains a JSON null: %s", raw)
	}
}

func TestMSKWire_DescribeClusterV2(t *testing.T) {
	p, ctx := setupMSKPlugin(t)
	m, _ := mskWireBody(t, p, ctx, "POST", "/api/v2/clusters", map[string]any{
		"clusterName": "wire-v2",
		"provisioned": map[string]any{
			"kafkaVersion":        "3.6.0",
			"numberOfBrokerNodes": 2,
			"brokerNodeGroupInfo": map[string]any{
				"instanceType":  "kafka.m5.xlarge",
				"clientSubnets": []string{"subnet-a"},
			},
		},
	})
	arn := m["clusterArn"].(string)

	body, raw := mskWireBody(t, p, ctx, "GET", "/api/v2/clusters/"+arn, nil)
	requireKeys(t, "DescribeClusterV2", body, "clusterInfo")
	info, ok := body["clusterInfo"].(map[string]any)
	if !ok {
		t.Fatalf("clusterInfo: want an object, got %T", body["clusterInfo"])
	}
	requireKeys(t, "Cluster", info,
		"clusterArn", "clusterName", "clusterType", "state", "creationTime", "provisioned")
	requireValues(t, "Cluster", info, map[string]any{
		"clusterArn":  arn,
		"clusterName": "wire-v2",
		"clusterType": "PROVISIONED",
		"state":       "ACTIVE",
	})
	prov, ok := info["provisioned"].(map[string]any)
	if !ok {
		t.Fatalf("provisioned: want an object, got %T", info["provisioned"])
	}
	requireKeys(t, "Provisioned", prov,
		"brokerNodeGroupInfo", "currentBrokerSoftwareInfo", "numberOfBrokerNodes")
	requireValues(t, "Provisioned", prov, map[string]any{"numberOfBrokerNodes": float64(2)})
	if sw, ok := prov["currentBrokerSoftwareInfo"].(map[string]any); !ok || sw["kafkaVersion"] != "3.6.0" {
		t.Errorf("provisioned.currentBrokerSoftwareInfo: want kafkaVersion 3.6.0, got %#v", prov["currentBrokerSoftwareInfo"])
	}
	// The v2 projection must reach the same depth as v1's, not stop at Provisioned.
	bng, ok := prov["brokerNodeGroupInfo"].(map[string]any)
	if !ok {
		t.Fatalf("provisioned.brokerNodeGroupInfo: want an object, got %T", prov["brokerNodeGroupInfo"])
	}
	requireKeys(t, "Provisioned.BrokerNodeGroupInfo", bng, "instanceType", "clientSubnets")
	requireValues(t, "Provisioned.BrokerNodeGroupInfo", bng, map[string]any{"instanceType": "kafka.m5.xlarge"})
	mskAssertNoInternalFields(t, "DescribeClusterV2", raw)
}

func TestMSKWire_ListClustersV2(t *testing.T) {
	p, ctx := setupMSKPlugin(t)
	mskWireBody(t, p, ctx, "POST", "/api/v2/clusters", map[string]any{
		"clusterName": "wire-v2-list",
		"provisioned": map[string]any{
			"kafkaVersion":        "3.6.0",
			"numberOfBrokerNodes": 2,
			"brokerNodeGroupInfo": map[string]any{
				"instanceType":  "kafka.m5.xlarge",
				"clientSubnets": []string{"subnet-a"},
			},
		},
	})

	m, raw := mskWireBody(t, p, ctx, "GET", "/api/v2/clusters", nil)
	requireKeys(t, "ListClustersV2", m, "clusterInfoList")
	list, ok := m["clusterInfoList"].([]any)
	if !ok || len(list) != 1 {
		t.Fatalf("clusterInfoList: want one element, got %v", m["clusterInfoList"])
	}
	elem, ok := list[0].(map[string]any)
	if !ok {
		t.Fatalf("clusterInfoList[0]: want an object, got %T", list[0])
	}
	requireKeys(t, "ListClustersV2 element", elem,
		"clusterArn", "clusterName", "clusterType", "state", "provisioned")
	requireValues(t, "ListClustersV2 element", elem, map[string]any{
		"clusterName": "wire-v2-list",
		"clusterType": "PROVISIONED",
		"state":       "ACTIVE",
	})
	if prov, ok := elem["provisioned"].(map[string]any); !ok || prov["numberOfBrokerNodes"] != float64(2) {
		t.Errorf("provisioned: want numberOfBrokerNodes 2, got %#v", elem["provisioned"])
	}
	// An empty nextToken would invite a caller to page on nothing.
	if _, ok := m["nextToken"]; ok {
		t.Error("nextToken: substrate returns one page, so the token must be omitted")
	}
	mskAssertNoInternalFields(t, "ListClustersV2", raw)
}

// mskAssertNoInternalFields checks the raw bytes, not a decoded top-level map: a
// nested occurrence of AccountID or Region would pass a top-level key check while
// still reaching the caller. #529 requires no response carry either.
func mskAssertNoInternalFields(t *testing.T, what string, raw []byte) {
	t.Helper()
	for _, bad := range []string{`"AccountID"`, `"Region"`, `"accountId"`, `"region"`} {
		if bytes.Contains(raw, []byte(bad)) {
			t.Errorf("%s: response carries substrate's internal %s: %s", what, bad, raw)
		}
	}
}

// TestMSKWire_StateEncodingUnchanged makes "state encoding is unchanged" a fact
// rather than a claim. The stored bytes are a persisted format that recorded runs
// replay from, so they must keep their PascalCase keys even though the wire is
// lowerCamel. This is the test that fails if someone later fixes a casing bug by
// retagging the state struct instead of the wire type.
func TestMSKWire_StateEncodingUnchanged(t *testing.T) {
	state := emulator.NewMemoryStateManager()
	p := &emulator.MSKPlugin{}
	if err := p.Initialize(context.Background(), emulator.PluginConfig{
		State:  state,
		Logger: emulator.NewDefaultLogger(0, false),
	}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	ctx := &emulator.RequestContext{AccountID: "123456789012", Region: "us-east-1", RequestID: "req-1"}
	if _, err := p.HandleRequest(ctx, mskRequest("POST", "/v1/clusters", map[string]any{
		"clusterName":         "state-shape",
		"numberOfBrokerNodes": 2,
		"brokerNodeGroupInfo": map[string]any{
			"instanceType":  "kafka.m5.large",
			"clientSubnets": []string{"subnet-1"},
		},
	})); err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}

	data, err := state.Get(context.Background(), "msk", "cluster:123456789012/us-east-1/state-shape")
	if err != nil || data == nil {
		t.Fatalf("state.Get: %v (data=%v)", err, data)
	}
	var stored map[string]any
	if err := json.Unmarshal(data, &stored); err != nil {
		t.Fatalf("unmarshal stored cluster: %v", err)
	}
	for _, k := range []string{
		"ClusterName", "ClusterArn", "State", "BrokerNodeGroupInfo",
		"NumberOfBrokerNodes", "KafkaVersion", "AccountID", "Region", "CreatedAt",
	} {
		if _, ok := stored[k]; !ok {
			t.Errorf("stored cluster: key %q is missing; the persisted format must not change", k)
		}
	}
	// And the wire spelling must NOT appear in state, or the two have been conflated.
	if _, ok := stored["clusterArn"]; ok {
		t.Error("stored cluster: wire spelling clusterArn found in the persisted format")
	}
}
