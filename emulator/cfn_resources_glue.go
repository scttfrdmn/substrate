package emulator

// StackDeployer helpers for CloudFormation's Glue resource types.

import (
	"context"
	"encoding/json"
	"fmt"
)

// ----- Glue -----------------------------------------------------------------

// deployGlueDatabase creates a Glue database for the given CFN resource.
func (d *StackDeployer) deployGlueDatabase(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	// The type's own properties are CatalogId, DatabaseInput and DatabaseName; the name and
	// description are members of DatabaseInput. Description was read as a top-level property,
	// which the type does not declare (#1605).
	name := resolveStringProp(props, "DatabaseName",
		resolveNestedStringProp(props, "DatabaseInput", "Name", logicalID, cctx), cctx)

	body := map[string]interface{}{
		"DatabaseInput": map[string]interface{}{
			"Name":        name,
			"Description": resolveNestedStringProp(props, "DatabaseInput", "Description", "", cctx),
		},
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal glue database body: %w", err)
	}

	req := &AWSRequest{
		Service:   "glue",
		Operation: "CreateDatabase",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "AWSGlue.CreateDatabase"},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Glue::Database",
		PhysicalID: name,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployGlueConnection creates a Glue connection for the given CFN resource.
func (d *StackDeployer) deployGlueConnection(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	// The connection's name and type are members of ConnectionInput, the type's one required
	// property; ConnectionName and ConnectionType were read as top-level properties, which the
	// type does not declare (#1605).
	name := resolveNestedStringProp(props, "ConnectionInput", "Name", logicalID, cctx)
	connType := resolveNestedStringProp(props, "ConnectionInput", "ConnectionType", "JDBC", cctx)

	body := map[string]interface{}{
		"ConnectionInput": map[string]interface{}{
			"Name":           name,
			"ConnectionType": connType,
		},
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal glue connection body: %w", err)
	}

	req := &AWSRequest{
		Service:   "glue",
		Operation: "CreateConnection",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "AWSGlue.CreateConnection"},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Glue::Connection",
		PhysicalID: name,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployGlueTable creates a Glue table for the given CFN resource.
func (d *StackDeployer) deployGlueTable(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	// The table's name is TableInput's Name member; TableName is not a property of the type
	// (#1605).
	tableName := resolveNestedStringProp(props, "TableInput", "Name", logicalID, cctx)
	dbName := resolveStringProp(props, "DatabaseName", "", cctx)

	body := map[string]interface{}{
		"DatabaseName": dbName,
		"TableInput": map[string]interface{}{
			"Name": tableName,
		},
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal glue table body: %w", err)
	}

	req := &AWSRequest{
		Service:   "glue",
		Operation: "CreateTable",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "AWSGlue.CreateTable"},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Glue::Table",
		PhysicalID: tableName,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployGlueCrawler creates a Glue crawler for the given CFN resource.
func (d *StackDeployer) deployGlueCrawler(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	// "Name": the type declares no CrawlerName (#1605).
	name := resolveStringProp(props, "Name", logicalID, cctx)

	body := map[string]interface{}{
		"Name":         name,
		"Role":         resolveStringProp(props, "Role", "", cctx),
		"DatabaseName": resolveStringProp(props, "DatabaseName", "", cctx),
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal glue crawler body: %w", err)
	}

	req := &AWSRequest{
		Service:   "glue",
		Operation: "CreateCrawler",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "AWSGlue.CreateCrawler"},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Glue::Crawler",
		PhysicalID: name,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}

// deployGlueJob creates a Glue ETL job for the given CFN resource.
func (d *StackDeployer) deployGlueJob(
	ctx context.Context,
	logicalID string,
	props map[string]interface{},
	streamID string,
	cctx *cfnContext,
) (DeployedResource, float64, error) {
	// "Name": the type declares no JobName (#1605).
	name := resolveStringProp(props, "Name", logicalID, cctx)

	body := map[string]interface{}{
		"Name": name,
		"Role": resolveStringProp(props, "Role", "", cctx),
		// Command is a nested object in the template, so both members are read through the
		// nested walk. Indexed flat by "Command.Name" and "Command.ScriptLocation" they matched
		// nothing and took their fallbacks unconditionally, so every CFN Glue job shipped an
		// empty ScriptLocation and a pythonshell job was created as a Spark ETL one (#877).
		"Command": map[string]interface{}{
			"Name":           resolveNestedStringProp(props, "Command", "Name", "glueetl", cctx),
			"ScriptLocation": resolveNestedStringProp(props, "Command", "ScriptLocation", "", cctx),
		},
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return DeployedResource{}, 0, fmt.Errorf("marshal glue job body: %w", err)
	}

	req := &AWSRequest{
		Service:   "glue",
		Operation: "CreateJob",
		Body:      bodyBytes,
		Headers:   map[string]string{"x-amz-target": "AWSGlue.CreateJob"},
		Params:    map[string]string{},
	}

	_, cost, routeErr := d.dispatch(ctx, req, streamID)
	dr := DeployedResource{
		LogicalID:  logicalID,
		Type:       "AWS::Glue::Job",
		PhysicalID: name,
	}
	if routeErr != nil {
		dr.Error = routeErr.Error()
	}
	return dr, cost, nil
}
