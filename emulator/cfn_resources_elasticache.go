package emulator

// StackDeployer helpers for CloudFormation's ElastiCache resource types.

import (
	"context"
	"fmt"
)

// ----- ElastiCache ----------------------------------------------------------

// deployElastiCacheSubnetGroup creates an ElastiCache cache subnet group.
func (d *StackDeployer) deployElastiCacheSubnetGroup(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "CacheSubnetGroupName", logicalID, cctx)

	req := &AWSRequest{
		Service:   "elasticache",
		Operation: "CreateCacheSubnetGroup",
		Params: map[string]string{
			"Action":                      "CreateCacheSubnetGroup",
			"CacheSubnetGroupName":        name,
			"CacheSubnetGroupDescription": resolveStringProp(props, "Description", "substrate", cctx),
		},
		Headers: map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::ElastiCache::SubnetGroup",
		PhysicalID: name,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployElastiCacheParameterGroup creates an ElastiCache cache parameter group.
func (d *StackDeployer) deployElastiCacheParameterGroup(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	// The type declares no name property, so CloudFormation names the group, as
	// cfnGeneratedName does: {stack}-{logical}-{suffix} (#1605).
	name := cfnGeneratedName(cctx, "AWS::ElastiCache::ParameterGroup", logicalID)
	family := resolveStringProp(props, "CacheParameterGroupFamily", "redis7", cctx)

	req := &AWSRequest{
		Service:   "elasticache",
		Operation: "CreateCacheParameterGroup",
		Params: map[string]string{
			"Action":                    "CreateCacheParameterGroup",
			"CacheParameterGroupName":   name,
			"CacheParameterGroupFamily": family,
			"Description":               resolveStringProp(props, "Description", "substrate", cctx),
		},
		Headers: map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::ElastiCache::ParameterGroup",
		PhysicalID: name,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployElastiCacheCacheCluster creates an ElastiCache cache cluster.
func (d *StackDeployer) deployElastiCacheCacheCluster(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	// "ClusterName": the type declares no ClusterId (#1605).
	id := resolveStringProp(props, "ClusterName", logicalID, cctx)
	engine := resolveStringProp(props, "Engine", "redis", cctx)

	params := map[string]string{
		"Action":         "CreateCacheCluster",
		"CacheClusterId": id,
		"Engine":         engine,
		"CacheNodeType":  resolveStringProp(props, "CacheNodeType", "cache.t3.micro", cctx),
		"NumCacheNodes":  resolveStringProp(props, "NumCacheNodes", "1", cctx),
	}

	req := &AWSRequest{
		Service:   "elasticache",
		Operation: "CreateCacheCluster",
		Params:    params,
		Headers:   map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	arn := fmt.Sprintf("arn:aws:elasticache:%s:%s:cluster:%s", cctx.region, cctx.accountID, id)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::ElastiCache::CacheCluster",
		PhysicalID: id,
		ARN:        arn,
		Metadata:   make(map[string]interface{}),
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		if addr := extractXMLField(resp.Body, "Address"); addr != "" {
			dr.Metadata["ConfigurationEndpoint.Address"] = addr
			dr.Metadata["RedisEndPoint.Address"] = addr
		}
		if port := extractXMLField(resp.Body, "Port"); port != "" {
			dr.Metadata["ConfigurationEndpoint.Port"] = port
			dr.Metadata["RedisEndPoint.Port"] = port
		}
	}
	return dr, cost, nil
}

// deployElastiCacheReplicationGroup creates an ElastiCache replication group.
func (d *StackDeployer) deployElastiCacheReplicationGroup(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	id := resolveStringProp(props, "ReplicationGroupId", logicalID, cctx)

	params := map[string]string{
		"Action":                      "CreateReplicationGroup",
		"ReplicationGroupId":          id,
		"ReplicationGroupDescription": resolveStringProp(props, "ReplicationGroupDescription", "substrate", cctx),
	}
	if af := resolveStringProp(props, "AutomaticFailoverEnabled", "", cctx); af != "" {
		params["AutomaticFailoverEnabled"] = af
	}
	if maz := resolveStringProp(props, "MultiAZEnabled", "", cctx); maz != "" {
		params["MultiAZEnabled"] = maz
	}

	req := &AWSRequest{
		Service:   "elasticache",
		Operation: "CreateReplicationGroup",
		Params:    params,
		Headers:   map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	arn := fmt.Sprintf("arn:aws:elasticache:%s:%s:replicationgroup:%s", cctx.region, cctx.accountID, id)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::ElastiCache::ReplicationGroup",
		PhysicalID: id,
		ARN:        arn,
		Metadata:   make(map[string]interface{}),
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		if addr := extractXMLField(resp.Body, "Address"); addr != "" {
			dr.Metadata["PrimaryEndPoint.Address"] = addr
		}
		if port := extractXMLField(resp.Body, "Port"); port != "" {
			dr.Metadata["PrimaryEndPoint.Port"] = port
		}
	}
	return dr, cost, nil
}
