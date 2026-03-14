package substrate

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// RDSPlugin emulates the Amazon RDS API using the AWS Query protocol.
// It supports DB instances, snapshots, subnet groups, and parameter groups.
type RDSPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "rds".
func (p *RDSPlugin) Name() string { return rdsNamespace }

// Initialize sets up the RDSPlugin with the provided configuration.
func (p *RDSPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for RDSPlugin.
func (p *RDSPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an RDS query-protocol request to the appropriate handler.
func (p *RDSPlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	action := req.Operation
	if action == "" {
		action = req.Params["Action"]
	}
	switch action {
	// DB Instance operations.
	case "CreateDBInstance":
		return p.createDBInstance(ctx, req)
	case "DescribeDBInstances":
		return p.describeDBInstances(ctx, req)
	case "ModifyDBInstance":
		return p.modifyDBInstance(ctx, req)
	case "DeleteDBInstance":
		return p.deleteDBInstance(ctx, req)
	case "StartDBInstance":
		return p.startDBInstance(ctx, req)
	case "StopDBInstance":
		return p.stopDBInstance(ctx, req)
	case "RebootDBInstance":
		return p.rebootDBInstance(ctx, req)
	// DB Snapshot operations.
	case "CreateDBSnapshot":
		return p.createDBSnapshot(ctx, req)
	case "DescribeDBSnapshots":
		return p.describeDBSnapshots(ctx, req)
	case "DeleteDBSnapshot":
		return p.deleteDBSnapshot(ctx, req)
	// DB Subnet Group operations.
	case "CreateDBSubnetGroup":
		return p.createDBSubnetGroup(ctx, req)
	case "DescribeDBSubnetGroups":
		return p.describeDBSubnetGroups(ctx, req)
	case "DeleteDBSubnetGroup":
		return p.deleteDBSubnetGroup(ctx, req)
	// DB Parameter Group operations.
	case "CreateDBParameterGroup":
		return p.createDBParameterGroup(ctx, req)
	case "DescribeDBParameterGroups":
		return p.describeDBParameterGroups(ctx, req)
	case "DeleteDBParameterGroup":
		return p.deleteDBParameterGroup(ctx, req)
	// Tagging operations.
	case "ListTagsForResource":
		return p.listTagsForResource(ctx, req)
	case "AddTagsToResource":
		return p.addTagsToResource(ctx, req)
	case "RemoveTagsFromResource":
		return p.removeTagsFromResource(ctx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "RDSPlugin: unknown action " + action,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// --- DB Instance operations ---

func (p *RDSPlugin) createDBInstance(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id := req.Params["DBInstanceIdentifier"]
	if id == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "DBInstanceIdentifier is required", HTTPStatus: http.StatusBadRequest}
	}
	engine := req.Params["Engine"]
	if engine == "" {
		engine = "mysql"
	}
	port := rdsDefaultPort(engine)
	inst := RDSDBInstance{
		DBInstanceIdentifier: id,
		DBInstanceClass:      req.Params["DBInstanceClass"],
		Engine:               engine,
		EngineVersion:        req.Params["EngineVersion"],
		DBInstanceStatus:     "available",
		MasterUsername:       req.Params["MasterUsername"],
		DBSubnetGroupName:    req.Params["DBSubnetGroupName"],
		MultiAZ:              req.Params["MultiAZ"] == "true",
		DBInstanceArn:        rdsDBInstanceARN(reqCtx.Region, reqCtx.AccountID, id),
		Endpoint: RDSEndpoint{
			Address: id + ".rds." + reqCtx.Region + ".amazonaws.com",
			Port:    port,
		},
		Tags:      rdsTagsFromParams(req.Params),
		AccountID: reqCtx.AccountID,
		Region:    reqCtx.Region,
		CreatedAt: p.tc.Now(),
	}
	if s := req.Params["AllocatedStorage"]; s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			inst.AllocatedStorage = n
		}
	}

	data, err := json.Marshal(inst)
	if err != nil {
		return nil, fmt.Errorf("rds createDBInstance marshal: %w", err)
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	stateKey := "dbinstance:" + scope + "/" + id
	if err := p.state.Put(context.Background(), rdsNamespace, stateKey, data); err != nil {
		return nil, fmt.Errorf("rds createDBInstance put: %w", err)
	}
	if err := p.appendToIndex(scope, "dbinstance_ids", id); err != nil {
		return nil, err
	}

	type result struct {
		DBInstance xmlDBInstanceItem `xml:"DBInstance"`
	}
	type response struct {
		XMLName xml.Name `xml:"CreateDBInstanceResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result   `xml:"CreateDBInstanceResult"`
	}
	return rdsXMLResponse(http.StatusOK, response{
		XMLNS:  rdsXMLNS,
		Result: result{DBInstance: dbInstanceToXML(inst)},
	})
}

func (p *RDSPlugin) describeDBInstances(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	filterID := req.Params["DBInstanceIdentifier"]

	keys, err := p.state.List(context.Background(), rdsNamespace, "dbinstance:"+scope+"/")
	if err != nil {
		return nil, fmt.Errorf("rds describeDBInstances list: %w", err)
	}

	var items []xmlDBInstanceItem
	for _, k := range keys {
		data, getErr := p.state.Get(context.Background(), rdsNamespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var inst RDSDBInstance
		if json.Unmarshal(data, &inst) != nil {
			continue
		}
		if filterID != "" && inst.DBInstanceIdentifier != filterID {
			continue
		}
		items = append(items, dbInstanceToXML(inst))
	}

	if filterID != "" && len(items) == 0 {
		return nil, &AWSError{
			Code:       "DBInstanceNotFound",
			Message:    "DBInstance " + filterID + " not found.",
			HTTPStatus: http.StatusNotFound,
		}
	}

	// Pagination.
	maxRecords := 100
	if s := req.Params["MaxRecords"]; s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			maxRecords = n
		}
	}
	marker := req.Params["Marker"]
	offset := 0
	if marker != "" {
		if n, err := strconv.Atoi(marker); err == nil {
			offset = n
		}
	}
	if offset > len(items) {
		offset = len(items)
	}
	page := items[offset:]
	var nextMarker string
	if len(page) > maxRecords {
		page = page[:maxRecords]
		nextMarker = strconv.Itoa(offset + maxRecords)
	}

	type result struct {
		DBInstances []xmlDBInstanceItem `xml:"DBInstances>DBInstance"`
		Marker      string              `xml:"Marker,omitempty"`
	}
	type response struct {
		XMLName xml.Name `xml:"DescribeDBInstancesResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result   `xml:"DescribeDBInstancesResult"`
	}
	return rdsXMLResponse(http.StatusOK, response{
		XMLNS: rdsXMLNS,
		Result: result{
			DBInstances: page,
			Marker:      nextMarker,
		},
	})
}

func (p *RDSPlugin) modifyDBInstance(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id := req.Params["DBInstanceIdentifier"]
	if id == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "DBInstanceIdentifier is required", HTTPStatus: http.StatusBadRequest}
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	stateKey := "dbinstance:" + scope + "/" + id

	data, err := p.state.Get(context.Background(), rdsNamespace, stateKey)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "DBInstanceNotFound", Message: "DBInstance " + id + " not found.", HTTPStatus: http.StatusNotFound}
	}
	var inst RDSDBInstance
	if err := json.Unmarshal(data, &inst); err != nil {
		return nil, fmt.Errorf("rds modifyDBInstance unmarshal: %w", err)
	}

	if v := req.Params["DBInstanceClass"]; v != "" {
		inst.DBInstanceClass = v
	}
	if v := req.Params["EngineVersion"]; v != "" {
		inst.EngineVersion = v
	}
	if s := req.Params["AllocatedStorage"]; s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			inst.AllocatedStorage = n
		}
	}
	if v := req.Params["MultiAZ"]; v != "" {
		inst.MultiAZ = v == "true"
	}

	updated, _ := json.Marshal(inst)
	if err := p.state.Put(context.Background(), rdsNamespace, stateKey, updated); err != nil {
		return nil, fmt.Errorf("rds modifyDBInstance put: %w", err)
	}

	type result struct {
		DBInstance xmlDBInstanceItem `xml:"DBInstance"`
	}
	type response struct {
		XMLName xml.Name `xml:"ModifyDBInstanceResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result   `xml:"ModifyDBInstanceResult"`
	}
	return rdsXMLResponse(http.StatusOK, response{
		XMLNS:  rdsXMLNS,
		Result: result{DBInstance: dbInstanceToXML(inst)},
	})
}

func (p *RDSPlugin) deleteDBInstance(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id := req.Params["DBInstanceIdentifier"]
	if id == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "DBInstanceIdentifier is required", HTTPStatus: http.StatusBadRequest}
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	stateKey := "dbinstance:" + scope + "/" + id

	data, err := p.state.Get(context.Background(), rdsNamespace, stateKey)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "DBInstanceNotFound", Message: "DBInstance " + id + " not found.", HTTPStatus: http.StatusNotFound}
	}
	var inst RDSDBInstance
	if json.Unmarshal(data, &inst) != nil {
		inst.DBInstanceIdentifier = id
	}
	inst.DBInstanceStatus = "deleting"

	if err := p.state.Delete(context.Background(), rdsNamespace, stateKey); err != nil {
		return nil, fmt.Errorf("rds deleteDBInstance delete: %w", err)
	}
	p.removeFromIndex(scope, "dbinstance_ids", id)

	type result struct {
		DBInstance xmlDBInstanceItem `xml:"DBInstance"`
	}
	type response struct {
		XMLName xml.Name `xml:"DeleteDBInstanceResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result   `xml:"DeleteDBInstanceResult"`
	}
	return rdsXMLResponse(http.StatusOK, response{
		XMLNS:  rdsXMLNS,
		Result: result{DBInstance: dbInstanceToXML(inst)},
	})
}

func (p *RDSPlugin) startDBInstance(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.setDBInstanceStatus(reqCtx, req, "available", "StartDBInstanceResponse", "StartDBInstanceResult")
}

func (p *RDSPlugin) stopDBInstance(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.setDBInstanceStatus(reqCtx, req, "stopped", "StopDBInstanceResponse", "StopDBInstanceResult")
}

func (p *RDSPlugin) rebootDBInstance(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.setDBInstanceStatus(reqCtx, req, "available", "RebootDBInstanceResponse", "RebootDBInstanceResult")
}

func (p *RDSPlugin) setDBInstanceStatus(reqCtx *RequestContext, req *AWSRequest, status, responseName, resultName string) (*AWSResponse, error) {
	id := req.Params["DBInstanceIdentifier"]
	if id == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "DBInstanceIdentifier is required", HTTPStatus: http.StatusBadRequest}
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	stateKey := "dbinstance:" + scope + "/" + id

	data, err := p.state.Get(context.Background(), rdsNamespace, stateKey)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "DBInstanceNotFound", Message: "DBInstance " + id + " not found.", HTTPStatus: http.StatusNotFound}
	}
	var inst RDSDBInstance
	if err := json.Unmarshal(data, &inst); err != nil {
		return nil, fmt.Errorf("rds setDBInstanceStatus unmarshal: %w", err)
	}
	inst.DBInstanceStatus = status

	updated, _ := json.Marshal(inst)
	if err := p.state.Put(context.Background(), rdsNamespace, stateKey, updated); err != nil {
		return nil, fmt.Errorf("rds setDBInstanceStatus put: %w", err)
	}

	type result struct {
		DBInstance xmlDBInstanceItem `xml:"DBInstance"`
	}
	type xmlResp struct {
		XMLName xml.Name `xml:"placeholder"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result
	}
	resp := xmlResp{XMLNS: rdsXMLNS, Result: result{DBInstance: dbInstanceToXML(inst)}}
	// Build raw XML with dynamic element names.
	body, err := xml.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("rds setDBInstanceStatus xml marshal: %w", err)
	}
	// Replace placeholder element names with dynamic names.
	body = []byte(strings.ReplaceAll(string(body), "placeholder", responseName))
	body = []byte(strings.ReplaceAll(string(body), "XMLResult", resultName))
	return &AWSResponse{
		StatusCode: http.StatusOK,
		Headers:    map[string]string{"Content-Type": "text/xml; charset=UTF-8"},
		Body:       append([]byte(xml.Header), body...),
	}, nil
}

// --- DB Snapshot operations ---

func (p *RDSPlugin) createDBSnapshot(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	snapshotID := req.Params["DBSnapshotIdentifier"]
	instanceID := req.Params["DBInstanceIdentifier"]
	if snapshotID == "" || instanceID == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "DBSnapshotIdentifier and DBInstanceIdentifier are required", HTTPStatus: http.StatusBadRequest}
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region

	// Load source instance.
	instData, err := p.state.Get(context.Background(), rdsNamespace, "dbinstance:"+scope+"/"+instanceID)
	if err != nil || instData == nil {
		return nil, &AWSError{Code: "DBInstanceNotFound", Message: "DBInstance " + instanceID + " not found.", HTTPStatus: http.StatusNotFound}
	}
	var inst RDSDBInstance
	if json.Unmarshal(instData, &inst) != nil {
		inst.Engine = "mysql"
	}

	snap := RDSDBSnapshot{
		DBSnapshotIdentifier: snapshotID,
		DBInstanceIdentifier: instanceID,
		SnapshotType:         "manual",
		Status:               "available",
		Engine:               inst.Engine,
		AllocatedStorage:     inst.AllocatedStorage,
		DBSnapshotArn:        rdsSnapshotARN(reqCtx.Region, reqCtx.AccountID, snapshotID),
		Tags:                 rdsTagsFromParams(req.Params),
		AccountID:            reqCtx.AccountID,
		Region:               reqCtx.Region,
		CreatedAt:            p.tc.Now(),
	}
	data, err := json.Marshal(snap)
	if err != nil {
		return nil, fmt.Errorf("rds createDBSnapshot marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), rdsNamespace, "dbsnapshot:"+scope+"/"+snapshotID, data); err != nil {
		return nil, fmt.Errorf("rds createDBSnapshot put: %w", err)
	}
	if err := p.appendToIndex(scope, "dbsnapshot_ids", snapshotID); err != nil {
		return nil, err
	}

	type result struct {
		DBSnapshot xmlDBSnapshotItem `xml:"DBSnapshot"`
	}
	type response struct {
		XMLName xml.Name `xml:"CreateDBSnapshotResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result   `xml:"CreateDBSnapshotResult"`
	}
	return rdsXMLResponse(http.StatusOK, response{
		XMLNS:  rdsXMLNS,
		Result: result{DBSnapshot: dbSnapshotToXML(snap)},
	})
}

func (p *RDSPlugin) describeDBSnapshots(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	filterSnap := req.Params["DBSnapshotIdentifier"]
	filterInst := req.Params["DBInstanceIdentifier"]

	keys, err := p.state.List(context.Background(), rdsNamespace, "dbsnapshot:"+scope+"/")
	if err != nil {
		return nil, fmt.Errorf("rds describeDBSnapshots list: %w", err)
	}

	var items []xmlDBSnapshotItem
	for _, k := range keys {
		data, getErr := p.state.Get(context.Background(), rdsNamespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var snap RDSDBSnapshot
		if json.Unmarshal(data, &snap) != nil {
			continue
		}
		if filterSnap != "" && snap.DBSnapshotIdentifier != filterSnap {
			continue
		}
		if filterInst != "" && snap.DBInstanceIdentifier != filterInst {
			continue
		}
		items = append(items, dbSnapshotToXML(snap))
	}

	type result struct {
		DBSnapshots []xmlDBSnapshotItem `xml:"DBSnapshots>DBSnapshot"`
	}
	type response struct {
		XMLName xml.Name `xml:"DescribeDBSnapshotsResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result   `xml:"DescribeDBSnapshotsResult"`
	}
	return rdsXMLResponse(http.StatusOK, response{
		XMLNS:  rdsXMLNS,
		Result: result{DBSnapshots: items},
	})
}

func (p *RDSPlugin) deleteDBSnapshot(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id := req.Params["DBSnapshotIdentifier"]
	if id == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "DBSnapshotIdentifier is required", HTTPStatus: http.StatusBadRequest}
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	stateKey := "dbsnapshot:" + scope + "/" + id

	data, err := p.state.Get(context.Background(), rdsNamespace, stateKey)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "DBSnapshotNotFound", Message: "DBSnapshot " + id + " not found.", HTTPStatus: http.StatusNotFound}
	}
	var snap RDSDBSnapshot
	if json.Unmarshal(data, &snap) != nil {
		snap.DBSnapshotIdentifier = id
	}

	if err := p.state.Delete(context.Background(), rdsNamespace, stateKey); err != nil {
		return nil, fmt.Errorf("rds deleteDBSnapshot delete: %w", err)
	}
	p.removeFromIndex(scope, "dbsnapshot_ids", id)

	type result struct {
		DBSnapshot xmlDBSnapshotItem `xml:"DBSnapshot"`
	}
	type response struct {
		XMLName xml.Name `xml:"DeleteDBSnapshotResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result   `xml:"DeleteDBSnapshotResult"`
	}
	return rdsXMLResponse(http.StatusOK, response{
		XMLNS:  rdsXMLNS,
		Result: result{DBSnapshot: dbSnapshotToXML(snap)},
	})
}

// --- DB Subnet Group operations ---

func (p *RDSPlugin) createDBSubnetGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["DBSubnetGroupName"]
	if name == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "DBSubnetGroupName is required", HTTPStatus: http.StatusBadRequest}
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	sg := RDSDBSubnetGroup{
		DBSubnetGroupName:        name,
		DBSubnetGroupDescription: req.Params["DBSubnetGroupDescription"],
		SubnetGroupStatus:        "Complete",
		VpcID:                    req.Params["VpcId"],
		DBSubnetGroupArn:         rdsSubnetGroupARN(reqCtx.Region, reqCtx.AccountID, name),
		Tags:                     rdsTagsFromParams(req.Params),
		AccountID:                reqCtx.AccountID,
		Region:                   reqCtx.Region,
	}
	data, err := json.Marshal(sg)
	if err != nil {
		return nil, fmt.Errorf("rds createDBSubnetGroup marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), rdsNamespace, "dbsubnetgroup:"+scope+"/"+name, data); err != nil {
		return nil, fmt.Errorf("rds createDBSubnetGroup put: %w", err)
	}
	if err := p.appendToIndex(scope, "dbsubnetgroup_names", name); err != nil {
		return nil, err
	}

	type result struct {
		DBSubnetGroup xmlDBSubnetGroupItem `xml:"DBSubnetGroup"`
	}
	type response struct {
		XMLName xml.Name `xml:"CreateDBSubnetGroupResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result   `xml:"CreateDBSubnetGroupResult"`
	}
	return rdsXMLResponse(http.StatusOK, response{
		XMLNS:  rdsXMLNS,
		Result: result{DBSubnetGroup: dbSubnetGroupToXML(sg)},
	})
}

func (p *RDSPlugin) describeDBSubnetGroups(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	filterName := req.Params["DBSubnetGroupName"]

	keys, err := p.state.List(context.Background(), rdsNamespace, "dbsubnetgroup:"+scope+"/")
	if err != nil {
		return nil, fmt.Errorf("rds describeDBSubnetGroups list: %w", err)
	}

	var items []xmlDBSubnetGroupItem
	for _, k := range keys {
		data, getErr := p.state.Get(context.Background(), rdsNamespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var sg RDSDBSubnetGroup
		if json.Unmarshal(data, &sg) != nil {
			continue
		}
		if filterName != "" && sg.DBSubnetGroupName != filterName {
			continue
		}
		items = append(items, dbSubnetGroupToXML(sg))
	}

	type result struct {
		DBSubnetGroups []xmlDBSubnetGroupItem `xml:"DBSubnetGroups>DBSubnetGroup"`
	}
	type response struct {
		XMLName xml.Name `xml:"DescribeDBSubnetGroupsResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result   `xml:"DescribeDBSubnetGroupsResult"`
	}
	return rdsXMLResponse(http.StatusOK, response{
		XMLNS:  rdsXMLNS,
		Result: result{DBSubnetGroups: items},
	})
}

func (p *RDSPlugin) deleteDBSubnetGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["DBSubnetGroupName"]
	if name == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "DBSubnetGroupName is required", HTTPStatus: http.StatusBadRequest}
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	if err := p.state.Delete(context.Background(), rdsNamespace, "dbsubnetgroup:"+scope+"/"+name); err != nil {
		return nil, fmt.Errorf("rds deleteDBSubnetGroup delete: %w", err)
	}
	p.removeFromIndex(scope, "dbsubnetgroup_names", name)

	type response struct {
		XMLName xml.Name `xml:"DeleteDBSubnetGroupResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
	}
	return rdsXMLResponse(http.StatusOK, response{XMLNS: rdsXMLNS})
}

// --- DB Parameter Group operations ---

func (p *RDSPlugin) createDBParameterGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["DBParameterGroupName"]
	if name == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "DBParameterGroupName is required", HTTPStatus: http.StatusBadRequest}
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	pg := RDSDBParameterGroup{
		DBParameterGroupName:   name,
		DBParameterGroupFamily: req.Params["DBParameterGroupFamily"],
		Description:            req.Params["Description"],
		DBParameterGroupArn:    rdsParameterGroupARN(reqCtx.Region, reqCtx.AccountID, name),
		Tags:                   rdsTagsFromParams(req.Params),
		AccountID:              reqCtx.AccountID,
		Region:                 reqCtx.Region,
	}
	data, err := json.Marshal(pg)
	if err != nil {
		return nil, fmt.Errorf("rds createDBParameterGroup marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), rdsNamespace, "dbparamgroup:"+scope+"/"+name, data); err != nil {
		return nil, fmt.Errorf("rds createDBParameterGroup put: %w", err)
	}
	if err := p.appendToIndex(scope, "dbparamgroup_names", name); err != nil {
		return nil, err
	}

	type result struct {
		DBParameterGroup xmlDBParamGroupItem `xml:"DBParameterGroup"`
	}
	type response struct {
		XMLName xml.Name `xml:"CreateDBParameterGroupResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result   `xml:"CreateDBParameterGroupResult"`
	}
	return rdsXMLResponse(http.StatusOK, response{
		XMLNS:  rdsXMLNS,
		Result: result{DBParameterGroup: dbParamGroupToXML(pg)},
	})
}

func (p *RDSPlugin) describeDBParameterGroups(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	filterName := req.Params["DBParameterGroupName"]

	keys, err := p.state.List(context.Background(), rdsNamespace, "dbparamgroup:"+scope+"/")
	if err != nil {
		return nil, fmt.Errorf("rds describeDBParameterGroups list: %w", err)
	}

	var items []xmlDBParamGroupItem
	for _, k := range keys {
		data, getErr := p.state.Get(context.Background(), rdsNamespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var pg RDSDBParameterGroup
		if json.Unmarshal(data, &pg) != nil {
			continue
		}
		if filterName != "" && pg.DBParameterGroupName != filterName {
			continue
		}
		items = append(items, dbParamGroupToXML(pg))
	}

	type result struct {
		DBParameterGroups []xmlDBParamGroupItem `xml:"DBParameterGroups>DBParameterGroup"`
	}
	type response struct {
		XMLName xml.Name `xml:"DescribeDBParameterGroupsResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result   `xml:"DescribeDBParameterGroupsResult"`
	}
	return rdsXMLResponse(http.StatusOK, response{
		XMLNS:  rdsXMLNS,
		Result: result{DBParameterGroups: items},
	})
}

func (p *RDSPlugin) deleteDBParameterGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["DBParameterGroupName"]
	if name == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "DBParameterGroupName is required", HTTPStatus: http.StatusBadRequest}
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	if err := p.state.Delete(context.Background(), rdsNamespace, "dbparamgroup:"+scope+"/"+name); err != nil {
		return nil, fmt.Errorf("rds deleteDBParameterGroup delete: %w", err)
	}
	p.removeFromIndex(scope, "dbparamgroup_names", name)

	type response struct {
		XMLName xml.Name `xml:"DeleteDBParameterGroupResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
	}
	return rdsXMLResponse(http.StatusOK, response{XMLNS: rdsXMLNS})
}

// --- Tagging operations ---

func (p *RDSPlugin) listTagsForResource(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	resourceARN := req.Params["ResourceName"]
	if resourceARN == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "ResourceName is required", HTTPStatus: http.StatusBadRequest}
	}
	tags, err := p.loadTagsByARN(resourceARN)
	if err != nil {
		return nil, err
	}

	type xmlTag struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	type result struct {
		TagList []xmlTag `xml:"TagList>Tag"`
	}
	type response struct {
		XMLName xml.Name `xml:"ListTagsForResourceResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result   `xml:"ListTagsForResourceResult"`
	}
	xmlTags := make([]xmlTag, 0, len(tags))
	for k, v := range tags {
		xmlTags = append(xmlTags, xmlTag{Key: k, Value: v})
	}
	return rdsXMLResponse(http.StatusOK, response{
		XMLNS:  rdsXMLNS,
		Result: result{TagList: xmlTags},
	})
}

func (p *RDSPlugin) addTagsToResource(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	resourceARN := req.Params["ResourceName"]
	if resourceARN == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "ResourceName is required", HTTPStatus: http.StatusBadRequest}
	}
	newTags := rdsTagsFromParams(req.Params)
	if err := p.updateTagsByARN(resourceARN, newTags, nil); err != nil {
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"AddTagsToResourceResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
	}
	return rdsXMLResponse(http.StatusOK, response{XMLNS: rdsXMLNS})
}

func (p *RDSPlugin) removeTagsFromResource(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	resourceARN := req.Params["ResourceName"]
	if resourceARN == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "ResourceName is required", HTTPStatus: http.StatusBadRequest}
	}
	keys := extractIndexedParams(req.Params, "TagKeys.member")
	if err := p.updateTagsByARN(resourceARN, nil, keys); err != nil {
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"RemoveTagsFromResourceResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
	}
	return rdsXMLResponse(http.StatusOK, response{XMLNS: rdsXMLNS})
}

// loadTagsByARN resolves an RDS ARN and returns the resource's tags.
func (p *RDSPlugin) loadTagsByARN(arn string) (map[string]string, error) {
	ns, key, err := rdsResolveARN(arn)
	if err != nil {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	data, err := p.state.Get(context.Background(), ns, key)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "DBInstanceNotFound", Message: "Resource not found: " + arn, HTTPStatus: http.StatusNotFound}
	}
	var res struct {
		Tags map[string]string `json:"Tags"`
	}
	if json.Unmarshal(data, &res) != nil {
		return nil, nil //nolint:nilerr
	}
	return res.Tags, nil
}

// updateTagsByARN merges or removes tags on the resource identified by arn.
func (p *RDSPlugin) updateTagsByARN(arn string, add map[string]string, removeKeys []string) error {
	_, key, err := rdsResolveARN(arn)
	if err != nil {
		return &AWSError{Code: "InvalidParameterValue", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	// Determine the resource type from the key prefix.
	var instKey string
	switch {
	case strings.HasPrefix(key, "dbinstance:"):
		instKey = key
	case strings.HasPrefix(key, "dbsnapshot:"):
		instKey = key
	default:
		instKey = key
	}
	data, err := p.state.Get(context.Background(), rdsNamespace, instKey)
	if err != nil || data == nil {
		return &AWSError{Code: "DBInstanceNotFound", Message: "Resource not found: " + arn, HTTPStatus: http.StatusNotFound}
	}

	// Generic tag merge via raw JSON.
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("rds updateTagsByARN unmarshal: %w", err)
	}
	existingTags := map[string]string{}
	if t, ok := m["Tags"]; ok && t != nil {
		if tagMap, ok := t.(map[string]interface{}); ok {
			for k, v := range tagMap {
				if sv, ok := v.(string); ok {
					existingTags[k] = sv
				}
			}
		}
	}
	merged := mergeStringMap(existingTags, add, removeKeys)
	m["Tags"] = merged
	updated, _ := json.Marshal(m)
	return p.state.Put(context.Background(), rdsNamespace, instKey, updated)
}

// --- XML types ---

// xmlDBInstanceItem is the XML representation of an RDS DB instance.
type xmlDBInstanceItem struct {
	DBInstanceIdentifier string      `xml:"DBInstanceIdentifier"`
	DBInstanceClass      string      `xml:"DBInstanceClass"`
	Engine               string      `xml:"Engine"`
	EngineVersion        string      `xml:"EngineVersion"`
	DBInstanceStatus     string      `xml:"DBInstanceStatus"`
	MasterUsername       string      `xml:"MasterUsername"`
	AllocatedStorage     int         `xml:"AllocatedStorage"`
	DBInstanceArn        string      `xml:"DBInstanceArn"`
	MultiAZ              bool        `xml:"MultiAZ"`
	DBSubnetGroupName    string      `xml:"DBSubnetGroup>DBSubnetGroupName,omitempty"`
	Endpoint             xmlEndpoint `xml:"Endpoint"`
}

// xmlEndpoint is the XML representation of an RDS endpoint.
type xmlEndpoint struct {
	Address string `xml:"Address"`
	Port    int    `xml:"Port"`
}

// xmlDBSnapshotItem is the XML representation of an RDS DB snapshot.
type xmlDBSnapshotItem struct {
	DBSnapshotIdentifier string `xml:"DBSnapshotIdentifier"`
	DBInstanceIdentifier string `xml:"DBInstanceIdentifier"`
	SnapshotType         string `xml:"SnapshotType"`
	Status               string `xml:"Status"`
	Engine               string `xml:"Engine"`
	AllocatedStorage     int    `xml:"AllocatedStorage"`
	DBSnapshotArn        string `xml:"DBSnapshotArn"`
}

// xmlDBSubnetGroupItem is the XML representation of an RDS DB subnet group.
type xmlDBSubnetGroupItem struct {
	DBSubnetGroupName        string `xml:"DBSubnetGroupName"`
	DBSubnetGroupDescription string `xml:"DBSubnetGroupDescription"`
	SubnetGroupStatus        string `xml:"SubnetGroupStatus"`
	VpcID                    string `xml:"VpcId"`
	DBSubnetGroupArn         string `xml:"DBSubnetGroupArn"`
}

// xmlDBParamGroupItem is the XML representation of an RDS DB parameter group.
type xmlDBParamGroupItem struct {
	DBParameterGroupName   string `xml:"DBParameterGroupName"`
	DBParameterGroupFamily string `xml:"DBParameterGroupFamily"`
	Description            string `xml:"Description"`
	DBParameterGroupArn    string `xml:"DBParameterGroupArn"`
}

func dbInstanceToXML(inst RDSDBInstance) xmlDBInstanceItem {
	return xmlDBInstanceItem{
		DBInstanceIdentifier: inst.DBInstanceIdentifier,
		DBInstanceClass:      inst.DBInstanceClass,
		Engine:               inst.Engine,
		EngineVersion:        inst.EngineVersion,
		DBInstanceStatus:     inst.DBInstanceStatus,
		MasterUsername:       inst.MasterUsername,
		AllocatedStorage:     inst.AllocatedStorage,
		DBInstanceArn:        inst.DBInstanceArn,
		MultiAZ:              inst.MultiAZ,
		DBSubnetGroupName:    inst.DBSubnetGroupName,
		Endpoint:             xmlEndpoint{Address: inst.Endpoint.Address, Port: inst.Endpoint.Port},
	}
}

func dbSnapshotToXML(snap RDSDBSnapshot) xmlDBSnapshotItem {
	return xmlDBSnapshotItem{
		DBSnapshotIdentifier: snap.DBSnapshotIdentifier,
		DBInstanceIdentifier: snap.DBInstanceIdentifier,
		SnapshotType:         snap.SnapshotType,
		Status:               snap.Status,
		Engine:               snap.Engine,
		AllocatedStorage:     snap.AllocatedStorage,
		DBSnapshotArn:        snap.DBSnapshotArn,
	}
}

func dbSubnetGroupToXML(sg RDSDBSubnetGroup) xmlDBSubnetGroupItem {
	return xmlDBSubnetGroupItem{
		DBSubnetGroupName:        sg.DBSubnetGroupName,
		DBSubnetGroupDescription: sg.DBSubnetGroupDescription,
		SubnetGroupStatus:        sg.SubnetGroupStatus,
		VpcID:                    sg.VpcID,
		DBSubnetGroupArn:         sg.DBSubnetGroupArn,
	}
}

func dbParamGroupToXML(pg RDSDBParameterGroup) xmlDBParamGroupItem {
	return xmlDBParamGroupItem{
		DBParameterGroupName:   pg.DBParameterGroupName,
		DBParameterGroupFamily: pg.DBParameterGroupFamily,
		Description:            pg.Description,
		DBParameterGroupArn:    pg.DBParameterGroupArn,
	}
}

// --- State helpers ---

func (p *RDSPlugin) appendToIndex(scope, indexName, id string) error {
	key := indexName + ":" + scope
	data, err := p.state.Get(context.Background(), rdsNamespace, key)
	if err != nil {
		return fmt.Errorf("rds appendToIndex get %s: %w", key, err)
	}
	var ids []string
	if data != nil {
		_ = json.Unmarshal(data, &ids)
	}
	ids = append(ids, id)
	newData, _ := json.Marshal(ids)
	return p.state.Put(context.Background(), rdsNamespace, key, newData)
}

func (p *RDSPlugin) removeFromIndex(scope, indexName, id string) {
	key := indexName + ":" + scope
	data, err := p.state.Get(context.Background(), rdsNamespace, key)
	if err != nil || data == nil {
		return
	}
	var ids []string
	if json.Unmarshal(data, &ids) != nil {
		return
	}
	filtered := ids[:0]
	for _, v := range ids {
		if v != id {
			filtered = append(filtered, v)
		}
	}
	newData, _ := json.Marshal(filtered)
	_ = p.state.Put(context.Background(), rdsNamespace, key, newData)
}

// --- ARN helpers ---

func rdsDBInstanceARN(region, acct, id string) string {
	return "arn:aws:rds:" + region + ":" + acct + ":db:" + id
}

func rdsSnapshotARN(region, acct, id string) string {
	return "arn:aws:rds:" + region + ":" + acct + ":snapshot:" + id
}

func rdsSubnetGroupARN(region, acct, name string) string {
	return "arn:aws:rds:" + region + ":" + acct + ":subgrp:" + name
}

func rdsParameterGroupARN(region, acct, name string) string {
	return "arn:aws:rds:" + region + ":" + acct + ":pg:" + name
}

// rdsResolveARN parses an RDS ARN and returns (namespace, stateKey).
// Supported ARN resource types: db (instance), snapshot.
func rdsResolveARN(arn string) (ns, key string, err error) {
	// arn:aws:rds:{region}:{acct}:{type}:{id}
	parts := strings.SplitN(arn, ":", 7)
	if len(parts) < 7 || parts[0] != "arn" || parts[2] != "rds" {
		return "", "", fmt.Errorf("invalid RDS ARN: %q", arn)
	}
	region := parts[3]
	acct := parts[4]
	resType := parts[5]
	resID := parts[6]
	scope := acct + "/" + region

	switch resType {
	case "db":
		return rdsNamespace, "dbinstance:" + scope + "/" + resID, nil
	case "snapshot":
		return rdsNamespace, "dbsnapshot:" + scope + "/" + resID, nil
	default:
		return "", "", fmt.Errorf("unsupported RDS ARN resource type: %q", resType)
	}
}

// rdsTagsFromParams extracts Tags.member.N.Key/Value pairs from query params.
func rdsTagsFromParams(params map[string]string) map[string]string {
	tags := make(map[string]string)
	for i := 1; ; i++ {
		prefix := fmt.Sprintf("Tags.member.%d.", i)
		k := params[prefix+"Key"]
		v := params[prefix+"Value"]
		if k == "" {
			break
		}
		tags[k] = v
	}
	if len(tags) == 0 {
		return nil
	}
	return tags
}

// rdsXMLResponse serialises v to XML and wraps it in an AWSResponse.
func rdsXMLResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := xml.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("rds xml marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "text/xml; charset=UTF-8"},
		Body:       append([]byte(xml.Header), body...),
	}, nil
}
