package emulator

// StackDeployer helpers for CloudFormation's RDS resource types, including Aurora clusters.

import (
	"context"
	"fmt"
)

// ----- RDS ------------------------------------------------------------------

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

// ----- RDS Aurora cluster --------------------------------------------------

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
