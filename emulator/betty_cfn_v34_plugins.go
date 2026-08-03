package emulator

import (
	"context"
	"encoding/json"
	"fmt"
)

// ----- v0.34.0 — RDS Aurora cluster ----------------------------------------

// deployRDSDBCluster creates an RDS Aurora DB cluster for the given CFN resource.
func (d *StackDeployer) deployRDSDBCluster(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	id := resolveStringProp(props, "DBClusterIdentifier", logicalID, cctx)
	engine := resolveStringProp(props, "Engine", "aurora-mysql", cctx)

	params := map[string]string{
		"Action":              "CreateDBCluster",
		"DBClusterIdentifier": id,
		"Engine":              engine,
		"MasterUsername":      resolveStringProp(props, "MasterUsername", "admin", cctx),
	}
	if ev := resolveStringProp(props, "EngineVersion", "", cctx); ev != "" {
		params["EngineVersion"] = ev
	}
	if sg := resolveStringProp(props, "DBSubnetGroupName", "", cctx); sg != "" {
		params["DBSubnetGroupName"] = sg
	}

	req := &AWSRequest{
		Service:   "rds",
		Operation: "CreateDBCluster",
		Params:    params,
		Headers:   map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	arn := fmt.Sprintf("arn:aws:rds:%s:%s:cluster:%s", cctx.region, cctx.accountID, id)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::RDS::DBCluster",
		PhysicalID: id,
		ARN:        arn,
		Metadata:   make(map[string]interface{}),
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		if addr := extractXMLField(resp.Body, "Endpoint"); addr != "" {
			dr.Metadata["Endpoint.Address"] = addr
		}
		if port := extractXMLField(resp.Body, "Port"); port != "" {
			dr.Metadata["Endpoint.Port"] = port
		}
	}
	return dr, cost, nil
}

// ----- v0.34.0 — MSK --------------------------------------------------------

// deployMSKCluster creates an Amazon MSK cluster for the given CFN resource.
func (d *StackDeployer) deployMSKCluster(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "ClusterName", logicalID, cctx)
	kafkaVersion := resolveStringProp(props, "KafkaVersion", "3.5.1", cctx)

	brokerInfo := map[string]interface{}{
		"InstanceType":  resolveStringProp(props, "InstanceType", "kafka.m5.large", cctx),
		"ClientSubnets": []string{},
	}
	if bi, ok := props["BrokerNodeGroupInfo"].(map[string]interface{}); ok {
		// Resolved rather than asserted on string: a template names its subnets
		// with a Ref far more often than as literals, and a string assertion
		// dropped every one of them (#526). MSK's API is natively PascalCase, so
		// only the values need attention.
		if it := resolveValue(bi["InstanceType"], cctx); it != "" {
			brokerInfo["InstanceType"] = it
		}
		if subnets := resolveStringList(bi["ClientSubnets"], cctx); len(subnets) > 0 {
			// Only overwritten when the property resolves to something, so an
			// absent or empty ClientSubnets keeps the empty-slice default rather
			// than becoming a JSON null.
			brokerInfo["ClientSubnets"] = subnets
		}
	}

	bodyMap := map[string]interface{}{
		"ClusterName":         name,
		"KafkaVersion":        kafkaVersion,
		"NumberOfBrokerNodes": 2,
		"BrokerNodeGroupInfo": brokerInfo,
	}
	body, err := json.Marshal(bodyMap)
	if err != nil {
		return DeployedResource{LogicalID: logicalID, Type: "AWS::MSK::Cluster", Error: err.Error()}, 0, fmt.Errorf("msk deployMSKCluster marshal: %w", err)
	}

	req := &AWSRequest{
		Service:   "msk",
		Operation: "POST",
		Path:      "/v1/clusters",
		Body:      body,
		Headers:   map[string]string{"Content-Type": "application/json"},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID: logicalID,
		Type:      "AWS::MSK::Cluster",
		Metadata:  make(map[string]interface{}),
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
		dr.PhysicalID = name
	} else if resp != nil {
		var result struct {
			ClusterARN  string `json:"ClusterArn"`
			ClusterName string `json:"ClusterName"`
		}
		if json.Unmarshal(resp.Body, &result) == nil {
			dr.PhysicalID = result.ClusterARN
			dr.ARN = result.ClusterARN
		} else {
			dr.PhysicalID = name
		}
	}
	return dr, cost, nil
}
