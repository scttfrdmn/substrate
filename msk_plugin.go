package substrate

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// MSKPlugin emulates the Amazon Managed Streaming for Apache Kafka (MSK) service.
// It supports cluster lifecycle operations using the MSK REST/JSON API.
type MSKPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "msk".
func (p *MSKPlugin) Name() string { return mskNamespace }

// Initialize sets up the MSKPlugin with the provided configuration.
func (p *MSKPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for MSKPlugin.
func (p *MSKPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an MSK REST/JSON request to the appropriate handler.
func (p *MSKPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	op, clusterARN := parseKafkaOperation(req.Operation, req.Path)
	switch op {
	case "CreateCluster":
		return p.createCluster(ctx, req)
	case "DescribeCluster":
		return p.describeCluster(ctx, req, clusterARN)
	case "GetBootstrapBrokers":
		return p.getBootstrapBrokers(ctx, req, clusterARN)
	case "ListClusters":
		return p.listClusters(ctx, req)
	case "DeleteCluster":
		return p.deleteCluster(ctx, req, clusterARN)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "MSKPlugin: unsupported path " + req.Path,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// parseKafkaOperation derives the MSK operation and optional cluster ARN from
// the HTTP method and request path.
func parseKafkaOperation(method, path string) (op, clusterARN string) {
	path = strings.TrimRight(path, "/")

	switch {
	case path == "/v1/clusters" && method == "POST":
		return "CreateCluster", ""
	case path == "/v1/clusters" && method == "GET":
		return "ListClusters", ""
	case strings.HasSuffix(path, "/bootstrap-brokers") && method == "GET":
		arn := strings.TrimSuffix(strings.TrimPrefix(path, "/v1/clusters/"), "/bootstrap-brokers")
		return "GetBootstrapBrokers", arn
	case strings.HasPrefix(path, "/v1/clusters/") && method == "GET":
		arn := strings.TrimPrefix(path, "/v1/clusters/")
		return "DescribeCluster", arn
	case strings.HasPrefix(path, "/v1/clusters/") && method == "DELETE":
		arn := strings.TrimPrefix(path, "/v1/clusters/")
		return "DeleteCluster", arn
	}
	return "", ""
}

func (p *MSKPlugin) createCluster(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	var input struct {
		ClusterName         string                 `json:"ClusterName"`
		KafkaVersion        string                 `json:"KafkaVersion"`
		NumberOfBrokerNodes int                    `json:"NumberOfBrokerNodes"`
		BrokerNodeGroupInfo MSKBrokerNodeGroupInfo `json:"BrokerNodeGroupInfo"`
		Tags                map[string]string      `json:"Tags"`
	}
	if len(req.Body) > 0 {
		if err := json.Unmarshal(req.Body, &input); err != nil {
			return nil, &AWSError{Code: "BadRequest", Message: "invalid JSON body: " + err.Error(), HTTPStatus: http.StatusBadRequest}
		}
	}
	if input.ClusterName == "" {
		return nil, &AWSError{Code: "BadRequest", Message: "ClusterName is required", HTTPStatus: http.StatusBadRequest}
	}

	scope := reqCtx.AccountID + "/" + reqCtx.Region
	indexKey := "cluster_ids:" + scope

	// Check for duplicate by name.
	names, _ := loadStringIndex(context.Background(), p.state, mskNamespace, indexKey)
	for _, n := range names {
		if n == input.ClusterName {
			return nil, &AWSError{Code: "ConflictException", Message: "Cluster " + input.ClusterName + " already exists.", HTTPStatus: http.StatusConflict}
		}
	}

	if input.KafkaVersion == "" {
		input.KafkaVersion = "3.5.1"
	}
	if input.NumberOfBrokerNodes == 0 {
		input.NumberOfBrokerNodes = 2
	}

	// Generate a deterministic UUID-like suffix from name + timestamp.
	uuid := mskGenerateUUID(input.ClusterName, p.tc.Now().UnixNano())
	clusterARN := "arn:aws:kafka:" + reqCtx.Region + ":" + reqCtx.AccountID + ":cluster/" + input.ClusterName + "/" + uuid

	cluster := MSKCluster{
		ClusterName:         input.ClusterName,
		ClusterARN:          clusterARN,
		State:               "ACTIVE",
		BrokerNodeGroupInfo: input.BrokerNodeGroupInfo,
		NumberOfBrokerNodes: input.NumberOfBrokerNodes,
		KafkaVersion:        input.KafkaVersion,
		Tags:                input.Tags,
		AccountID:           reqCtx.AccountID,
		Region:              reqCtx.Region,
		CreatedAt:           p.tc.Now(),
	}

	data, err := json.Marshal(cluster)
	if err != nil {
		return nil, fmt.Errorf("msk createCluster marshal: %w", err)
	}
	stateKey := "cluster:" + scope + "/" + input.ClusterName
	if err := p.state.Put(context.Background(), mskNamespace, stateKey, data); err != nil {
		return nil, fmt.Errorf("msk createCluster put: %w", err)
	}
	updateStringIndex(context.Background(), p.state, mskNamespace, indexKey, input.ClusterName)

	return mskJSONResponse(http.StatusOK, map[string]interface{}{
		"ClusterArn":  clusterARN,
		"ClusterName": cluster.ClusterName,
		"State":       cluster.State,
	})
}

func (p *MSKPlugin) describeCluster(_ *RequestContext, _ *AWSRequest, clusterARN string) (*AWSResponse, error) {
	if clusterARN == "" {
		return nil, &AWSError{Code: "BadRequest", Message: "cluster ARN is required", HTTPStatus: http.StatusBadRequest}
	}
	cluster, err := p.loadClusterByARN(clusterARN)
	if err != nil {
		return nil, err
	}
	return mskJSONResponse(http.StatusOK, map[string]interface{}{
		"ClusterInfo": cluster,
	})
}

func (p *MSKPlugin) getBootstrapBrokers(_ *RequestContext, _ *AWSRequest, clusterARN string) (*AWSResponse, error) {
	if clusterARN == "" {
		return nil, &AWSError{Code: "BadRequest", Message: "cluster ARN is required", HTTPStatus: http.StatusBadRequest}
	}
	cluster, err := p.loadClusterByARN(clusterARN)
	if err != nil {
		return nil, err
	}
	name := cluster.ClusterName
	region := cluster.Region
	brokers := fmt.Sprintf(
		"broker1.%s.%s.kafka.amazonaws.com:9092,broker2.%s.%s.kafka.amazonaws.com:9092",
		name, region, name, region,
	)
	return mskJSONResponse(http.StatusOK, map[string]string{
		"BootstrapBrokerString": brokers,
	})
}

func (p *MSKPlugin) listClusters(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	names, err := loadStringIndex(context.Background(), p.state, mskNamespace, "cluster_ids:"+scope)
	if err != nil {
		return nil, fmt.Errorf("msk listClusters load index: %w", err)
	}

	var clusters []MSKCluster
	for _, name := range names {
		data, getErr := p.state.Get(context.Background(), mskNamespace, "cluster:"+scope+"/"+name)
		if getErr != nil || data == nil {
			continue
		}
		var c MSKCluster
		if json.Unmarshal(data, &c) != nil {
			continue
		}
		clusters = append(clusters, c)
	}
	if clusters == nil {
		clusters = []MSKCluster{}
	}

	return mskJSONResponse(http.StatusOK, map[string]interface{}{
		"ClusterInfoList": clusters,
	})
}

func (p *MSKPlugin) deleteCluster(reqCtx *RequestContext, _ *AWSRequest, clusterARN string) (*AWSResponse, error) {
	if clusterARN == "" {
		return nil, &AWSError{Code: "BadRequest", Message: "cluster ARN is required", HTTPStatus: http.StatusBadRequest}
	}
	cluster, err := p.loadClusterByARN(clusterARN)
	if err != nil {
		return nil, err
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	stateKey := "cluster:" + scope + "/" + cluster.ClusterName

	if err := p.state.Delete(context.Background(), mskNamespace, stateKey); err != nil {
		return nil, fmt.Errorf("msk deleteCluster delete: %w", err)
	}
	removeFromStringIndex(context.Background(), p.state, mskNamespace, "cluster_ids:"+scope, cluster.ClusterName)

	return mskJSONResponse(http.StatusOK, map[string]interface{}{
		"ClusterArn":  cluster.ClusterARN,
		"ClusterName": cluster.ClusterName,
		"State":       "DELETING",
	})
}

// loadClusterByARN finds and deserializes a cluster by its ARN.
// ARN format: arn:aws:kafka:{region}:{acct}:cluster/{name}/{uuid}.
func (p *MSKPlugin) loadClusterByARN(clusterARN string) (*MSKCluster, error) {
	// Parse region and account from ARN.
	parts := strings.SplitN(clusterARN, ":", 7)
	if len(parts) < 6 || parts[2] != "kafka" {
		return nil, &AWSError{Code: "BadRequest", Message: "invalid MSK cluster ARN: " + clusterARN, HTTPStatus: http.StatusBadRequest}
	}
	region := parts[3]
	acct := parts[4]
	// parts[5] is "cluster/{name}/{uuid}"
	resParts := strings.SplitN(parts[5], "/", 3)
	if len(resParts) < 2 {
		return nil, &AWSError{Code: "BadRequest", Message: "invalid MSK cluster ARN resource: " + clusterARN, HTTPStatus: http.StatusBadRequest}
	}
	name := resParts[1]

	scope := acct + "/" + region
	data, err := p.state.Get(context.Background(), mskNamespace, "cluster:"+scope+"/"+name)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "NotFoundException", Message: "Cluster not found: " + clusterARN, HTTPStatus: http.StatusNotFound}
	}
	var cluster MSKCluster
	if err := json.Unmarshal(data, &cluster); err != nil {
		return nil, fmt.Errorf("msk loadClusterByARN unmarshal: %w", err)
	}
	return &cluster, nil
}

// mskGenerateUUID produces a short deterministic hex string for cluster ARNs.
func mskGenerateUUID(name string, nano int64) string {
	h := fmt.Sprintf("%x", nano)
	if len(h) > 8 {
		h = h[:8]
	}
	nh := fmt.Sprintf("%x", len(name))
	return nh + h + "-0001-0001-0001-000000000001"
}

// mskJSONResponse serializes v to JSON and returns an AWSResponse.
func mskJSONResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("msk json marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "application/json"},
		Body:       body,
	}, nil
}
