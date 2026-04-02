package substrate_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"
	"time"

	substrate "github.com/scttfrdmn/substrate"
)

func setupMSKPlugin(t *testing.T) (*substrate.MSKPlugin, *substrate.RequestContext) {
	t.Helper()
	state := substrate.NewMemoryStateManager()
	tc := substrate.NewTimeController(time.Now())
	p := &substrate.MSKPlugin{}
	if err := p.Initialize(context.Background(), substrate.PluginConfig{
		State:   state,
		Logger:  substrate.NewDefaultLogger(slog.LevelError, false),
		Options: map[string]any{"time_controller": tc},
	}); err != nil {
		t.Fatalf("MSKPlugin.Initialize: %v", err)
	}
	return p, &substrate.RequestContext{
		AccountID: "123456789012",
		Region:    "us-east-1",
		RequestID: "req-msk-1",
	}
}

func mskRequest(method, path string, body map[string]any) *substrate.AWSRequest {
	var b []byte
	if body != nil {
		b, _ = json.Marshal(body)
	}
	return &substrate.AWSRequest{
		Service:   "msk",
		Operation: method,
		Path:      path,
		Headers:   map[string]string{"Content-Type": "application/json"},
		Body:      b,
		Params:    map[string]string{},
	}
}

func TestMSKPlugin_CreateListDescribeDeleteCluster(t *testing.T) {
	p, ctx := setupMSKPlugin(t)

	// CreateCluster
	resp, err := p.HandleRequest(ctx, mskRequest("POST", "/v1/clusters", map[string]any{
		"ClusterName":         "my-kafka",
		"KafkaVersion":        "3.5.1",
		"NumberOfBrokerNodes": 3,
		"BrokerNodeGroupInfo": map[string]any{
			"InstanceType":  "kafka.m5.large",
			"ClientSubnets": []string{"subnet-1"},
		},
	}))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
	var created struct {
		ClusterARN  string `json:"ClusterArn"`
		ClusterName string `json:"ClusterName"`
		State       string `json:"State"`
	}
	if err := json.Unmarshal(resp.Body, &created); err != nil {
		t.Fatalf("unmarshal create response: %v", err)
	}
	if created.ClusterARN == "" {
		t.Error("want non-empty ClusterArn")
	}
	if created.ClusterName != "my-kafka" {
		t.Errorf("want ClusterName=my-kafka, got %q", created.ClusterName)
	}
	if created.State != "ACTIVE" {
		t.Errorf("want State=ACTIVE, got %q", created.State)
	}

	// CreateCluster — duplicate
	_, err = p.HandleRequest(ctx, mskRequest("POST", "/v1/clusters", map[string]any{
		"ClusterName":         "my-kafka",
		"KafkaVersion":        "3.5.1",
		"NumberOfBrokerNodes": 3,
	}))
	if err == nil {
		t.Fatal("want error for duplicate cluster, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok {
		t.Fatalf("want *AWSError, got %T", err)
	}
	if awsErr.Code != "ConflictException" {
		t.Errorf("want ConflictException, got %q", awsErr.Code)
	}
	if awsErr.HTTPStatus != http.StatusConflict {
		t.Errorf("want 409, got %d", awsErr.HTTPStatus)
	}

	// ListClusters
	resp, err = p.HandleRequest(ctx, mskRequest("GET", "/v1/clusters", nil))
	if err != nil {
		t.Fatalf("ListClusters: %v", err)
	}
	var listed struct {
		ClusterInfoList []substrate.MSKCluster `json:"ClusterInfoList"`
	}
	if err := json.Unmarshal(resp.Body, &listed); err != nil {
		t.Fatalf("unmarshal list: %v", err)
	}
	if len(listed.ClusterInfoList) != 1 {
		t.Errorf("want 1 cluster, got %d", len(listed.ClusterInfoList))
	}
	if listed.ClusterInfoList[0].ClusterName != "my-kafka" {
		t.Errorf("want ClusterName=my-kafka, got %q", listed.ClusterInfoList[0].ClusterName)
	}

	// DescribeCluster
	resp, err = p.HandleRequest(ctx, mskRequest("GET", "/v1/clusters/"+created.ClusterARN, nil))
	if err != nil {
		t.Fatalf("DescribeCluster: %v", err)
	}
	var described struct {
		ClusterInfo substrate.MSKCluster `json:"ClusterInfo"`
	}
	if err := json.Unmarshal(resp.Body, &described); err != nil {
		t.Fatalf("unmarshal describe: %v", err)
	}
	if described.ClusterInfo.ClusterName != "my-kafka" {
		t.Errorf("want ClusterName=my-kafka, got %q", described.ClusterInfo.ClusterName)
	}

	// DescribeCluster — not found
	_, err = p.HandleRequest(ctx, mskRequest("GET", "/v1/clusters/arn:aws:kafka:us-east-1:123456789012:cluster/no-such/abc", nil))
	if err == nil {
		t.Fatal("want error for missing cluster, got nil")
	}
	awsErr, ok = err.(*substrate.AWSError)
	if !ok {
		t.Fatalf("want *AWSError, got %T", err)
	}
	if awsErr.Code != "NotFoundException" {
		t.Errorf("want NotFoundException, got %q", awsErr.Code)
	}

	// GetBootstrapBrokers
	resp, err = p.HandleRequest(ctx, mskRequest("GET", "/v1/clusters/"+created.ClusterARN+"/bootstrap-brokers", nil))
	if err != nil {
		t.Fatalf("GetBootstrapBrokers: %v", err)
	}
	var brokers struct {
		BootstrapBrokerString string `json:"BootstrapBrokerString"`
	}
	if err := json.Unmarshal(resp.Body, &brokers); err != nil {
		t.Fatalf("unmarshal brokers: %v", err)
	}
	if brokers.BootstrapBrokerString == "" {
		t.Error("want non-empty BootstrapBrokerString")
	}

	// DeleteCluster
	resp, err = p.HandleRequest(ctx, mskRequest("DELETE", "/v1/clusters/"+created.ClusterARN, nil))
	if err != nil {
		t.Fatalf("DeleteCluster: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
	var deleted struct {
		State string `json:"State"`
	}
	if err := json.Unmarshal(resp.Body, &deleted); err != nil {
		t.Fatalf("unmarshal delete: %v", err)
	}
	if deleted.State != "DELETING" {
		t.Errorf("want State=DELETING, got %q", deleted.State)
	}

	// DescribeCluster — not found after delete
	_, err = p.HandleRequest(ctx, mskRequest("GET", "/v1/clusters/"+created.ClusterARN, nil))
	if err == nil {
		t.Fatal("want error after delete, got nil")
	}
}

func TestMSKPlugin_MissingClusterName(t *testing.T) {
	p, ctx := setupMSKPlugin(t)
	_, err := p.HandleRequest(ctx, mskRequest("POST", "/v1/clusters", map[string]any{
		"KafkaVersion": "3.5.1",
	}))
	if err == nil {
		t.Fatal("want error for missing ClusterName, got nil")
	}
	awsErr, ok := err.(*substrate.AWSError)
	if !ok {
		t.Fatalf("want *AWSError, got %T", err)
	}
	if awsErr.HTTPStatus != http.StatusBadRequest {
		t.Errorf("want 400, got %d", awsErr.HTTPStatus)
	}
}

func TestMSKPlugin_UnknownPath(t *testing.T) {
	p, ctx := setupMSKPlugin(t)
	_, err := p.HandleRequest(ctx, mskRequest("PUT", "/v1/unknown", nil))
	if err == nil {
		t.Fatal("want error for unknown path, got nil")
	}
}

func TestMSKPlugin_CreateClusterV2_DescribeListV2(t *testing.T) {
	p, ctx := setupMSKPlugin(t)

	// CreateClusterV2 using Provisioned shape
	resp, err := p.HandleRequest(ctx, mskRequest("POST", "/api/v2/clusters", map[string]any{
		"ClusterName": "v2-kafka",
		"Provisioned": map[string]any{
			"BrokerNodeGroupInfo": map[string]any{
				"InstanceType":  "kafka.m5.xlarge",
				"ClientSubnets": []string{"subnet-a", "subnet-b"},
			},
			"KafkaVersion":        "3.6.0",
			"NumberOfBrokerNodes": 2,
		},
	}))
	if err != nil {
		t.Fatalf("CreateClusterV2: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode)
	}
	var created struct {
		ClusterARN  string `json:"ClusterArn"`
		ClusterName string `json:"ClusterName"`
		State       string `json:"State"`
	}
	if err := json.Unmarshal(resp.Body, &created); err != nil {
		t.Fatalf("unmarshal create v2 response: %v", err)
	}
	if created.ClusterARN == "" {
		t.Error("want non-empty ClusterArn")
	}
	if created.ClusterName != "v2-kafka" {
		t.Errorf("want ClusterName=v2-kafka, got %q", created.ClusterName)
	}
	if created.State != "ACTIVE" {
		t.Errorf("want State=ACTIVE, got %q", created.State)
	}

	// DescribeClusterV2
	resp, err = p.HandleRequest(ctx, mskRequest("GET", "/api/v2/clusters/"+created.ClusterARN, nil))
	if err != nil {
		t.Fatalf("DescribeClusterV2: %v", err)
	}
	var described struct {
		ClusterInfo struct {
			ClusterARN  string `json:"ClusterArn"`
			ClusterName string `json:"ClusterName"`
			ClusterType string `json:"ClusterType"`
			State       string `json:"State"`
			Provisioned struct {
				NumberOfBrokerNodes int `json:"NumberOfBrokerNodes"`
			} `json:"Provisioned"`
		} `json:"ClusterInfo"`
	}
	if err := json.Unmarshal(resp.Body, &described); err != nil {
		t.Fatalf("unmarshal describe v2: %v", err)
	}
	if described.ClusterInfo.ClusterType != "PROVISIONED" {
		t.Errorf("want ClusterType=PROVISIONED, got %q", described.ClusterInfo.ClusterType)
	}
	if described.ClusterInfo.ClusterName != "v2-kafka" {
		t.Errorf("want ClusterName=v2-kafka, got %q", described.ClusterInfo.ClusterName)
	}
	if described.ClusterInfo.Provisioned.NumberOfBrokerNodes != 2 {
		t.Errorf("want NumberOfBrokerNodes=2, got %d", described.ClusterInfo.Provisioned.NumberOfBrokerNodes)
	}

	// ListClustersV2
	resp, err = p.HandleRequest(ctx, mskRequest("GET", "/api/v2/clusters", nil))
	if err != nil {
		t.Fatalf("ListClustersV2: %v", err)
	}
	var listed struct {
		ClusterInfoList []struct {
			ClusterName string `json:"ClusterName"`
			ClusterType string `json:"ClusterType"`
		} `json:"ClusterInfoList"`
	}
	if err := json.Unmarshal(resp.Body, &listed); err != nil {
		t.Fatalf("unmarshal list v2: %v", err)
	}
	if len(listed.ClusterInfoList) != 1 {
		t.Errorf("want 1 cluster, got %d", len(listed.ClusterInfoList))
	}
	if listed.ClusterInfoList[0].ClusterType != "PROVISIONED" {
		t.Errorf("want ClusterType=PROVISIONED, got %q", listed.ClusterInfoList[0].ClusterType)
	}
}

func TestMSKPlugin_ListNodes(t *testing.T) {
	p, ctx := setupMSKPlugin(t)

	// CreateCluster with 3 broker nodes
	resp, err := p.HandleRequest(ctx, mskRequest("POST", "/v1/clusters", map[string]any{
		"ClusterName":         "node-kafka",
		"KafkaVersion":        "3.5.1",
		"NumberOfBrokerNodes": 3,
		"BrokerNodeGroupInfo": map[string]any{
			"InstanceType":  "kafka.m5.large",
			"ClientSubnets": []string{"subnet-x"},
		},
	}))
	if err != nil {
		t.Fatalf("CreateCluster: %v", err)
	}
	var created struct {
		ClusterARN string `json:"ClusterArn"`
	}
	if err := json.Unmarshal(resp.Body, &created); err != nil {
		t.Fatalf("unmarshal create: %v", err)
	}

	// ListNodes
	resp, err = p.HandleRequest(ctx, mskRequest("GET", "/v1/clusters/"+created.ClusterARN+"/nodes", nil))
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	var nodes struct {
		NodeInfoList []substrate.MSKNodeInfo `json:"NodeInfoList"`
	}
	if err := json.Unmarshal(resp.Body, &nodes); err != nil {
		t.Fatalf("unmarshal nodes: %v", err)
	}
	if len(nodes.NodeInfoList) != 3 {
		t.Fatalf("want 3 nodes, got %d", len(nodes.NodeInfoList))
	}
	for i, n := range nodes.NodeInfoList {
		if n.NodeType != "BROKER" {
			t.Errorf("node[%d] want NodeType=BROKER, got %q", i, n.NodeType)
		}
		if n.NodeARN == "" {
			t.Errorf("node[%d] want non-empty NodeArn", i)
		}
		if n.BrokerNodeInfo.BrokerId == 0 {
			t.Errorf("node[%d] want non-zero BrokerId", i)
		}
		if n.BrokerNodeInfo.CurrentBrokerSoftwareInfo.KafkaVersion == "" {
			t.Errorf("node[%d] want non-empty KafkaVersion", i)
		}
	}
}
