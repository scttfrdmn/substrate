package substrate

import (
	"context"
	"fmt"
)

// ----- v0.25.0 — RDS --------------------------------------------------------

// deployRDSDBSubnetGroup creates an RDS DB subnet group for the given CFN resource.
func (d *StackDeployer) deployRDSDBSubnetGroup(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "DBSubnetGroupName", logicalID, cctx)

	req := &AWSRequest{
		Service:   "rds",
		Operation: "CreateDBSubnetGroup",
		Params: map[string]string{
			"Action":                   "CreateDBSubnetGroup",
			"DBSubnetGroupName":        name,
			"DBSubnetGroupDescription": resolveStringProp(props, "DBSubnetGroupDescription", "substrate", cctx),
		},
		Headers: map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::RDS::DBSubnetGroup",
		PhysicalID: name,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployRDSDBParameterGroup creates an RDS DB parameter group for the given CFN resource.
func (d *StackDeployer) deployRDSDBParameterGroup(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	name := resolveStringProp(props, "DBParameterGroupName", logicalID, cctx)
	family := resolveStringProp(props, "Family", "mysql8.0", cctx)

	req := &AWSRequest{
		Service:   "rds",
		Operation: "CreateDBParameterGroup",
		Params: map[string]string{
			"Action":                 "CreateDBParameterGroup",
			"DBParameterGroupName":   name,
			"DBParameterGroupFamily": family,
			"Description":            resolveStringProp(props, "Description", "substrate", cctx),
		},
		Headers: map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::RDS::DBParameterGroup",
		PhysicalID: name,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployRDSDBInstance creates an RDS DB instance for the given CFN resource.
func (d *StackDeployer) deployRDSDBInstance(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	dbID := resolveStringProp(props, "DBInstanceIdentifier", logicalID, cctx)
	engine := resolveStringProp(props, "Engine", "mysql", cctx)

	params := map[string]string{
		"Action":               "CreateDBInstance",
		"DBInstanceIdentifier": dbID,
		"Engine":               engine,
		"DBInstanceClass":      resolveStringProp(props, "DBInstanceClass", "db.t3.micro", cctx),
		"MasterUsername":       resolveStringProp(props, "MasterUsername", "admin", cctx),
	}
	if sg := resolveStringProp(props, "DBSubnetGroupName", "", cctx); sg != "" {
		params["DBSubnetGroupName"] = sg
	}
	if stor := resolveStringProp(props, "AllocatedStorage", "", cctx); stor != "" {
		params["AllocatedStorage"] = stor
	}

	req := &AWSRequest{
		Service:   "rds",
		Operation: "CreateDBInstance",
		Params:    params,
		Headers:   map[string]string{},
	}

	resp, cost, routeErr := d.dispatch(ctx, req, streamID)
	arn := fmt.Sprintf("arn:aws:rds:%s:%s:db:%s", cctx.region, cctx.accountID, dbID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::RDS::DBInstance",
		PhysicalID: dbID,
		ARN:        arn,
		Metadata:   make(map[string]interface{}),
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	} else if resp != nil {
		// Extract endpoint address and port from XML response for GetAtt.
		if addr := extractXMLField(resp.Body, "Address"); addr != "" {
			dr.Metadata["Endpoint.Address"] = addr
		}
		if port := extractXMLField(resp.Body, "Port"); port != "" {
			dr.Metadata["Endpoint.Port"] = port
		}
	}
	return dr, cost, nil
}

// ----- v0.25.0 — ElastiCache ------------------------------------------------

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
	name := resolveStringProp(props, "CacheParameterGroupName", logicalID, cctx)
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
	id := resolveStringProp(props, "ClusterId", logicalID, cctx)
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
