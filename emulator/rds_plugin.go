package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// RDSPlugin emulates the Amazon RDS API using the AWS Query protocol.
// It supports DB instances, snapshots, subnet groups, and parameter groups.
// No database engine runs behind an instance: its endpoint is a stub address.
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

// Shutdown is a no-op for the RDS plugin.
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
	// DB Cluster operations.
	case "CreateDBCluster":
		return p.createDBCluster(ctx, req)
	case "DescribeDBClusters":
		return p.describeDBClusters(ctx, req)
	case "DeleteDBCluster":
		return p.deleteDBCluster(ctx, req)
	// Restore operations.
	case "RestoreDBInstanceFromDBSnapshot":
		return p.restoreDBInstanceFromSnapshot(ctx, req)
	// Tagging operations.
	case "ListTagsForResource":
		return p.listTagsForResource(ctx, req)
	case "AddTagsToResource":
		return p.addTagsToResource(ctx, req)
	case "RemoveTagsFromResource":
		return p.removeTagsFromResource(ctx, req)
	default:
		return nil, unknownActionError(p.Name(), action)
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
	return rdsOKResponse(reqCtx, "CreateDBInstance", result{DBInstance: dbInstanceToXML(inst)})
}

func (p *RDSPlugin) describeDBInstances(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	filterID := req.Params["DBInstanceIdentifier"]

	// Both pagination parameters are validated before any state is read, so a request
	// substrate cannot serve is refused rather than answered with page one.
	cursor, cursorErr := parseQueryMarker(req.Params["Marker"])
	if cursorErr != nil {
		return nil, cursorErr
	}
	maxRecords, maxErr := queryMaxRecords(req.Params["MaxRecords"])
	if maxErr != nil {
		return nil, maxErr
	}

	prefix := "dbinstance:" + scope + "/"
	keys, err := p.state.List(context.Background(), rdsNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("rds describeDBInstances list: %w", err)
	}

	page, nextMarker := queryMarkerPage(keys, prefix, cursor, maxRecords,
		func(key, _ string) (xmlDBInstanceItem, bool) {
			data, getErr := p.state.Get(context.Background(), rdsNamespace, key)
			if getErr != nil || data == nil {
				return xmlDBInstanceItem{}, false
			}
			var inst RDSDBInstance
			if json.Unmarshal(data, &inst) != nil {
				return xmlDBInstanceItem{}, false
			}
			if filterID != "" && inst.DBInstanceIdentifier != filterID {
				return xmlDBInstanceItem{}, false
			}
			return dbInstanceToXML(inst), true
		})

	if filterID != "" && len(page) == 0 {
		return nil, &AWSError{
			Code:       "DBInstanceNotFound",
			Message:    "DBInstance " + filterID + " not found.",
			HTTPStatus: http.StatusNotFound,
		}
	}

	type result struct {
		DBInstances []xmlDBInstanceItem `xml:"DBInstances>DBInstance"`
		Marker      string              `xml:"Marker,omitempty"`
	}
	return rdsOKResponse(reqCtx, "DescribeDBInstances", result{
		DBInstances: page,
		Marker:      nextMarker,
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

	updated, err := json.Marshal(inst)
	if err != nil {
		return nil, fmt.Errorf("rds modifyDBInstance marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), rdsNamespace, stateKey, updated); err != nil {
		return nil, fmt.Errorf("rds modifyDBInstance put: %w", err)
	}

	type result struct {
		DBInstance xmlDBInstanceItem `xml:"DBInstance"`
	}
	return rdsOKResponse(reqCtx, "ModifyDBInstance", result{DBInstance: dbInstanceToXML(inst)})
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
	if err := p.removeFromIndex(scope, "dbinstance_ids", id); err != nil {
		return nil, fmt.Errorf("rds deleteDBInstance removeFromIndex: %w", err)
	}

	type result struct {
		DBInstance xmlDBInstanceItem `xml:"DBInstance"`
	}
	return rdsOKResponse(reqCtx, "DeleteDBInstance", result{DBInstance: dbInstanceToXML(inst)})
}

func (p *RDSPlugin) startDBInstance(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.setDBInstanceStatus(reqCtx, req, "available", "StartDBInstance")
}

func (p *RDSPlugin) stopDBInstance(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.setDBInstanceStatus(reqCtx, req, "stopped", "StopDBInstance")
}

func (p *RDSPlugin) rebootDBInstance(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	return p.setDBInstanceStatus(reqCtx, req, "available", "RebootDBInstance")
}

// setDBInstanceStatus sets an instance's status and answers operation's document with the instance.
//
// Start, Stop and RebootDBInstance share it and differ only in the status written and the operation
// name, from which [rdsOKResponse] derives both element names. Until #1242 the names were spliced in
// by substituting placeholder strings in the marshaled bytes — after #756 found the result element
// had been missed entirely — which also rewrote any instance attribute that happened to contain the
// placeholder text.
func (p *RDSPlugin) setDBInstanceStatus(reqCtx *RequestContext, req *AWSRequest, status, operation string) (*AWSResponse, error) {
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

	updated, err := json.Marshal(inst)
	if err != nil {
		return nil, fmt.Errorf("rds setDBInstanceStatus marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), rdsNamespace, stateKey, updated); err != nil {
		return nil, fmt.Errorf("rds setDBInstanceStatus put: %w", err)
	}

	type result struct {
		DBInstance xmlDBInstanceItem `xml:"DBInstance"`
	}
	return rdsOKResponse(reqCtx, operation, result{DBInstance: dbInstanceToXML(inst)})
}

// --- DB Cluster operations ---

func (p *RDSPlugin) createDBCluster(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id := req.Params["DBClusterIdentifier"]
	if id == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "DBClusterIdentifier is required", HTTPStatus: http.StatusBadRequest}
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	stateKey := "dbcluster:" + scope + "/" + id

	existing, err := p.state.Get(context.Background(), rdsNamespace, stateKey)
	if err != nil {
		return nil, fmt.Errorf("rds createDBCluster get: %w", err)
	}
	if existing != nil {
		return nil, &AWSError{Code: "DBClusterAlreadyExistsFault", Message: "DB Cluster already exists: " + id, HTTPStatus: http.StatusBadRequest}
	}

	engine := req.Params["Engine"]
	if engine == "" {
		engine = "aurora-mysql"
	}
	port := rdsDefaultPort(engine)

	cluster := RDSDBCluster{
		DBClusterIdentifier: id,
		Engine:              engine,
		EngineVersion:       req.Params["EngineVersion"],
		Status:              "available",
		Endpoint:            id + ".cluster-xxx." + reqCtx.Region + ".rds.amazonaws.com",
		ReaderEndpoint:      id + ".cluster-ro-xxx." + reqCtx.Region + ".rds.amazonaws.com",
		Port:                port,
		MasterUsername:      req.Params["MasterUsername"],
		DBSubnetGroupName:   req.Params["DBSubnetGroupName"],
		MultiAZ:             req.Params["MultiAZ"] == "true",
		DBClusterArn:        rdsDBClusterARN(reqCtx.Region, reqCtx.AccountID, id),
		Tags:                rdsTagsFromParams(req.Params),
		AccountID:           reqCtx.AccountID,
		Region:              reqCtx.Region,
		CreatedAt:           p.tc.Now(),
	}

	data, err := json.Marshal(cluster)
	if err != nil {
		return nil, fmt.Errorf("rds createDBCluster marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), rdsNamespace, stateKey, data); err != nil {
		return nil, fmt.Errorf("rds createDBCluster put: %w", err)
	}
	if err := p.appendToIndex(scope, "dbcluster_ids", id); err != nil {
		return nil, err
	}

	type result struct {
		DBCluster xmlDBClusterItem `xml:"DBCluster"`
	}
	return rdsOKResponse(reqCtx, "CreateDBCluster", result{DBCluster: dbClusterToXML(cluster)})
}

func (p *RDSPlugin) describeDBClusters(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	filterID := req.Params["DBClusterIdentifier"]

	// Both pagination parameters are validated before any state is read, so a request
	// substrate cannot serve is refused rather than answered with page one.
	cursor, cursorErr := parseQueryMarker(req.Params["Marker"])
	if cursorErr != nil {
		return nil, cursorErr
	}
	maxRecords, maxErr := queryMaxRecords(req.Params["MaxRecords"])
	if maxErr != nil {
		return nil, maxErr
	}

	prefix := "dbcluster:" + scope + "/"
	keys, err := p.state.List(context.Background(), rdsNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("rds describeDBClusters list: %w", err)
	}

	page, nextMarker := queryMarkerPage(keys, prefix, cursor, maxRecords,
		func(key, _ string) (xmlDBClusterItem, bool) {
			data, getErr := p.state.Get(context.Background(), rdsNamespace, key)
			if getErr != nil || data == nil {
				return xmlDBClusterItem{}, false
			}
			var c RDSDBCluster
			if json.Unmarshal(data, &c) != nil {
				return xmlDBClusterItem{}, false
			}
			if filterID != "" && c.DBClusterIdentifier != filterID {
				return xmlDBClusterItem{}, false
			}
			return dbClusterToXML(c), true
		})

	if filterID != "" && len(page) == 0 {
		return nil, &AWSError{
			Code:       "DBClusterNotFoundFault",
			Message:    "DBCluster " + filterID + " not found.",
			HTTPStatus: http.StatusNotFound,
		}
	}

	type result struct {
		DBClusters []xmlDBClusterItem `xml:"DBClusters>DBCluster"`
		Marker     string             `xml:"Marker,omitempty"`
	}
	return rdsOKResponse(reqCtx, "DescribeDBClusters", result{
		DBClusters: page,
		Marker:     nextMarker,
	})
}

func (p *RDSPlugin) deleteDBCluster(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id := req.Params["DBClusterIdentifier"]
	if id == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "DBClusterIdentifier is required", HTTPStatus: http.StatusBadRequest}
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	stateKey := "dbcluster:" + scope + "/" + id

	data, err := p.state.Get(context.Background(), rdsNamespace, stateKey)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "DBClusterNotFoundFault", Message: "DBCluster " + id + " not found.", HTTPStatus: http.StatusNotFound}
	}
	var cluster RDSDBCluster
	if json.Unmarshal(data, &cluster) != nil {
		cluster.DBClusterIdentifier = id
	}
	cluster.Status = "deleting"

	if err := p.state.Delete(context.Background(), rdsNamespace, stateKey); err != nil {
		return nil, fmt.Errorf("rds deleteDBCluster delete: %w", err)
	}
	if err := p.removeFromIndex(scope, "dbcluster_ids", id); err != nil {
		return nil, fmt.Errorf("rds deleteDBCluster removeFromIndex: %w", err)
	}

	// Through the same projection the other two cluster sites use, so DeleteDBCluster answers the
	// full published DBCluster rather than the three members its own inline struct used to declare
	// (#756). The record it renders is the one just read out of state, with Status moved to
	// "deleting", so every member holds the value the cluster had.
	type result struct {
		DBCluster xmlDBClusterItem `xml:"DBCluster"`
	}
	return rdsOKResponse(reqCtx, "DeleteDBCluster", result{DBCluster: dbClusterToXML(cluster)})
}

// --- Restore operations ---

func (p *RDSPlugin) restoreDBInstanceFromSnapshot(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	snapshotID := req.Params["DBSnapshotIdentifier"]
	newInstanceID := req.Params["DBInstanceIdentifier"]
	if snapshotID == "" || newInstanceID == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "DBSnapshotIdentifier and DBInstanceIdentifier are required", HTTPStatus: http.StatusBadRequest}
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region

	snapData, err := p.state.Get(context.Background(), rdsNamespace, "dbsnapshot:"+scope+"/"+snapshotID)
	if err != nil || snapData == nil {
		return nil, &AWSError{Code: "DBSnapshotNotFound", Message: "DBSnapshot " + snapshotID + " not found.", HTTPStatus: http.StatusNotFound}
	}
	var snap RDSDBSnapshot
	if json.Unmarshal(snapData, &snap) != nil {
		snap.Engine = "mysql"
	}

	engine := snap.Engine
	if engine == "" {
		engine = "mysql"
	}
	port := rdsDefaultPort(engine)

	inst := RDSDBInstance{
		DBInstanceIdentifier: newInstanceID,
		DBInstanceClass:      req.Params["DBInstanceClass"],
		Engine:               engine,
		DBInstanceStatus:     "available",
		AllocatedStorage:     snap.AllocatedStorage,
		DBInstanceArn:        rdsDBInstanceARN(reqCtx.Region, reqCtx.AccountID, newInstanceID),
		Endpoint: RDSEndpoint{
			Address: newInstanceID + ".rds." + reqCtx.Region + ".amazonaws.com",
			Port:    port,
		},
		DBSubnetGroupName: req.Params["DBSubnetGroupName"],
		Tags:              rdsTagsFromParams(req.Params),
		AccountID:         reqCtx.AccountID,
		Region:            reqCtx.Region,
		CreatedAt:         p.tc.Now(),
	}

	data, err := json.Marshal(inst)
	if err != nil {
		return nil, fmt.Errorf("rds restoreDBInstanceFromSnapshot marshal: %w", err)
	}
	stateKey := "dbinstance:" + scope + "/" + newInstanceID
	if err := p.state.Put(context.Background(), rdsNamespace, stateKey, data); err != nil {
		return nil, fmt.Errorf("rds restoreDBInstanceFromSnapshot put: %w", err)
	}
	if err := p.appendToIndex(scope, "dbinstance_ids", newInstanceID); err != nil {
		return nil, err
	}

	type result struct {
		DBInstance xmlDBInstanceItem `xml:"DBInstance"`
	}
	return rdsOKResponse(reqCtx, "RestoreDBInstanceFromDBSnapshot", result{DBInstance: dbInstanceToXML(inst)})
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
	return rdsOKResponse(reqCtx, "CreateDBSnapshot", result{DBSnapshot: dbSnapshotToXML(snap)})
}

func (p *RDSPlugin) describeDBSnapshots(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	filterSnap := req.Params["DBSnapshotIdentifier"]
	filterInst := req.Params["DBInstanceIdentifier"]
	// Set when a snapshot with the requested identifier exists, whatever the instance filter says
	// about it, because DBSnapshotNotFound's gloss is about DBSnapshotIdentifier alone (#1020). A
	// snapshot that exists but belongs to another instance is an empty 200, not a claim that it
	// does not exist. See [queryMarkerFilterNotFound].
	var snapshotExists bool

	// Both pagination parameters are validated before any state is read, so a request
	// substrate cannot serve is refused rather than answered with page one (#916).
	cursor, cursorErr := parseQueryMarker(req.Params["Marker"])
	if cursorErr != nil {
		return nil, cursorErr
	}
	maxRecords, maxErr := queryMaxRecords(req.Params["MaxRecords"])
	if maxErr != nil {
		return nil, maxErr
	}

	prefix := "dbsnapshot:" + scope + "/"
	keys, err := p.state.List(context.Background(), rdsNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("rds describeDBSnapshots list: %w", err)
	}

	page, nextMarker := queryMarkerPage(keys, prefix, cursor, maxRecords,
		func(key, _ string) (xmlDBSnapshotItem, bool) {
			data, getErr := p.state.Get(context.Background(), rdsNamespace, key)
			if getErr != nil || data == nil {
				return xmlDBSnapshotItem{}, false
			}
			var snap RDSDBSnapshot
			if json.Unmarshal(data, &snap) != nil {
				return xmlDBSnapshotItem{}, false
			}
			if filterSnap != "" && snap.DBSnapshotIdentifier != filterSnap {
				return xmlDBSnapshotItem{}, false
			}
			snapshotExists = true
			if filterInst != "" && snap.DBInstanceIdentifier != filterInst {
				return xmlDBSnapshotItem{}, false
			}
			return dbSnapshotToXML(snap), true
		})

	// API_DescribeDBSnapshots publishes DBSnapshotNotFound/404 as its only error, glossed
	// "DBSnapshotIdentifier doesn't refer to an existing DB snapshot"; substrate answered an empty
	// 200 (#1020).
	if fault := queryMarkerFilterNotFound(filterSnap, snapshotExists, queryMarkerFault{
		Code:       "DBSnapshotNotFound",
		Kind:       "DBSnapshot",
		HTTPStatus: http.StatusNotFound,
	}); fault != nil {
		return nil, fault
	}

	type result struct {
		DBSnapshots []xmlDBSnapshotItem `xml:"DBSnapshots>DBSnapshot"`
		Marker      string              `xml:"Marker,omitempty"`
	}
	return rdsOKResponse(reqCtx, "DescribeDBSnapshots", result{
		DBSnapshots: page,
		Marker:      nextMarker,
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
	if err := p.removeFromIndex(scope, "dbsnapshot_ids", id); err != nil {
		return nil, fmt.Errorf("rds deleteDBSnapshot removeFromIndex: %w", err)
	}

	type result struct {
		DBSnapshot xmlDBSnapshotItem `xml:"DBSnapshot"`
	}
	return rdsOKResponse(reqCtx, "DeleteDBSnapshot", result{DBSnapshot: dbSnapshotToXML(snap)})
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
	return rdsOKResponse(reqCtx, "CreateDBSubnetGroup", result{DBSubnetGroup: dbSubnetGroupToXML(sg)})
}

func (p *RDSPlugin) describeDBSubnetGroups(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	filterName := req.Params["DBSubnetGroupName"]

	cursor, cursorErr := parseQueryMarker(req.Params["Marker"])
	if cursorErr != nil {
		return nil, cursorErr
	}
	maxRecords, maxErr := queryMaxRecords(req.Params["MaxRecords"])
	if maxErr != nil {
		return nil, maxErr
	}

	prefix := "dbsubnetgroup:" + scope + "/"
	keys, err := p.state.List(context.Background(), rdsNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("rds describeDBSubnetGroups list: %w", err)
	}

	page, nextMarker := queryMarkerPage(keys, prefix, cursor, maxRecords,
		func(key, _ string) (xmlDBSubnetGroupItem, bool) {
			data, getErr := p.state.Get(context.Background(), rdsNamespace, key)
			if getErr != nil || data == nil {
				return xmlDBSubnetGroupItem{}, false
			}
			var sg RDSDBSubnetGroup
			if json.Unmarshal(data, &sg) != nil {
				return xmlDBSubnetGroupItem{}, false
			}
			if filterName != "" && sg.DBSubnetGroupName != filterName {
				return xmlDBSubnetGroupItem{}, false
			}
			return dbSubnetGroupToXML(sg), true
		})

	// API_DescribeDBSubnetGroups publishes DBSubnetGroupNotFoundFault/404 as its only error,
	// glossed "DBSubnetGroupName doesn't refer to an existing DB subnet group" (#1020).
	if fault := queryMarkerFilterNotFound(filterName, len(page) > 0, queryMarkerFault{
		Code:       "DBSubnetGroupNotFoundFault",
		Kind:       "DBSubnetGroup",
		HTTPStatus: http.StatusNotFound,
	}); fault != nil {
		return nil, fault
	}

	type result struct {
		DBSubnetGroups []xmlDBSubnetGroupItem `xml:"DBSubnetGroups>DBSubnetGroup"`
		Marker         string                 `xml:"Marker,omitempty"`
	}
	return rdsOKResponse(reqCtx, "DescribeDBSubnetGroups", result{
		DBSubnetGroups: page,
		Marker:         nextMarker,
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
	if err := p.removeFromIndex(scope, "dbsubnetgroup_names", name); err != nil {
		return nil, fmt.Errorf("rds deleteDBSubnetGroup removeFromIndex: %w", err)
	}

	return rdsOKResponse(reqCtx, "DeleteDBSubnetGroup", nil)
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
		DBParameterGroup xmlDBParameterGroupItem `xml:"DBParameterGroup"`
	}
	return rdsOKResponse(reqCtx, "CreateDBParameterGroup", result{DBParameterGroup: dbParamGroupToXML(pg)})
}

func (p *RDSPlugin) describeDBParameterGroups(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	filterName := req.Params["DBParameterGroupName"]

	cursor, cursorErr := parseQueryMarker(req.Params["Marker"])
	if cursorErr != nil {
		return nil, cursorErr
	}
	maxRecords, maxErr := queryMaxRecords(req.Params["MaxRecords"])
	if maxErr != nil {
		return nil, maxErr
	}

	prefix := "dbparamgroup:" + scope + "/"
	keys, err := p.state.List(context.Background(), rdsNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("rds describeDBParameterGroups list: %w", err)
	}

	page, nextMarker := queryMarkerPage(keys, prefix, cursor, maxRecords,
		func(key, _ string) (xmlDBParameterGroupItem, bool) {
			data, getErr := p.state.Get(context.Background(), rdsNamespace, key)
			if getErr != nil || data == nil {
				return xmlDBParameterGroupItem{}, false
			}
			var pg RDSDBParameterGroup
			if json.Unmarshal(data, &pg) != nil {
				return xmlDBParameterGroupItem{}, false
			}
			if filterName != "" && pg.DBParameterGroupName != filterName {
				return xmlDBParameterGroupItem{}, false
			}
			return dbParamGroupToXML(pg), true
		})

	// API_DescribeDBParameterGroups publishes DBParameterGroupNotFound/404 as its only error,
	// glossed "DBParameterGroupName doesn't refer to an existing DB parameter group" (#1020). Note
	// that the same page's *constraint* on the parameter says "must match the name of an existing
	// DBClusterParameterGroup" — an AWS slip, since this operation describes DB parameter groups
	// and DescribeDBClusterParameterGroups is a different one; the Errors gloss is the statement
	// this follows.
	if fault := queryMarkerFilterNotFound(filterName, len(page) > 0, queryMarkerFault{
		Code:       "DBParameterGroupNotFound",
		Kind:       "DBParameterGroup",
		HTTPStatus: http.StatusNotFound,
	}); fault != nil {
		return nil, fault
	}

	type result struct {
		DBParameterGroups []xmlDBParameterGroupItem `xml:"DBParameterGroups>DBParameterGroup"`
		Marker            string                    `xml:"Marker,omitempty"`
	}
	return rdsOKResponse(reqCtx, "DescribeDBParameterGroups", result{
		DBParameterGroups: page,
		Marker:            nextMarker,
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
	if err := p.removeFromIndex(scope, "dbparamgroup_names", name); err != nil {
		return nil, fmt.Errorf("rds deleteDBParameterGroup removeFromIndex: %w", err)
	}

	return rdsOKResponse(reqCtx, "DeleteDBParameterGroup", nil)
}

// --- Tagging operations ---
//
// AddTagsToResource, RemoveTagsFromResource, ListTagsForResource and the shared ARN resolver
// live in rds_tags.go.

// --- XML types ---
//
// The item structs every handler above answers through, and the projections onto them, live in
// rds_wire.go.

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
	newData, err := json.Marshal(ids)
	if err != nil {
		return fmt.Errorf("rds appendToIndex marshal: %w", err)
	}
	return p.state.Put(context.Background(), rdsNamespace, key, newData)
}

func (p *RDSPlugin) removeFromIndex(scope, indexName, id string) error {
	key := indexName + ":" + scope
	data, err := p.state.Get(context.Background(), rdsNamespace, key)
	if err != nil {
		return fmt.Errorf("rds removeFromIndex %s state.Get: %w", key, err)
	}
	if data == nil {
		return nil
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		return fmt.Errorf("rds removeFromIndex %s unmarshal: %w", key, err)
	}
	filtered := ids[:0]
	for _, v := range ids {
		if v != id {
			filtered = append(filtered, v)
		}
	}
	newData, err := json.Marshal(filtered)
	if err != nil {
		return fmt.Errorf("rds removeFromIndex %s marshal: %w", key, err)
	}
	if err := p.state.Put(context.Background(), rdsNamespace, key, newData); err != nil {
		return fmt.Errorf("rds removeFromIndex %s state.Put: %w", key, err)
	}
	return nil
}

// --- ARN helpers ---

func rdsDBInstanceARN(region, acct, id string) string {
	return "arn:aws:rds:" + region + ":" + acct + ":db:" + id
}

func rdsDBClusterARN(region, acct, id string) string {
	return "arn:aws:rds:" + region + ":" + acct + ":cluster:" + id
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

// rdsResolveARN lives in rds_tags.go, alongside the tag operations that call it.

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

// rdsOKResponse answers an RDS operation in the awsQuery success document, under [rdsXMLNS] with the
// published `ResponseMetadata` carrying the request's own ID (#1242).
//
// result is the operation's result struct. Pass nil for an operation whose page publishes no result
// element — `DeleteDBSubnetGroup`, `DeleteDBParameterGroup`, `AddTagsToResource` and
// `RemoveTagsFromResource`, whose sample responses are the root and the metadata alone; see
// `query_envelope.go` for why that is not the same as an empty one.
func rdsOKResponse(reqCtx *RequestContext, operation string, result any) (*AWSResponse, error) {
	return queryOKResponse(reqCtx, operation, rdsXMLNS, result)
}
