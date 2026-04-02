package substrate

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"time"
)

// RedshiftPlugin emulates the AWS Redshift service.
// It handles cluster and related resource CRUD operations using the AWS Query
// (Action= parameter) protocol with XML responses.
type RedshiftPlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "redshift".
func (p *RedshiftPlugin) Name() string { return redshiftNamespace }

// Initialize sets up the RedshiftPlugin with the provided configuration.
func (p *RedshiftPlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for RedshiftPlugin.
func (p *RedshiftPlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches a Redshift query-protocol request to the appropriate handler.
func (p *RedshiftPlugin) HandleRequest(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	switch req.Operation {
	case "CreateCluster":
		return p.createCluster(reqCtx, req)
	case "DescribeClusters":
		return p.describeClusters(reqCtx, req)
	case "ModifyCluster":
		return p.modifyCluster(reqCtx, req)
	case "DeleteCluster":
		return p.deleteCluster(reqCtx, req)
	case "CreateClusterParameterGroup":
		return p.createClusterParameterGroup(reqCtx, req)
	case "DescribeClusterParameterGroups":
		return p.describeClusterParameterGroups(reqCtx, req)
	case "CreateClusterSubnetGroup":
		return p.createClusterSubnetGroup(reqCtx, req)
	case "DescribeClusterSubnetGroups":
		return p.describeClusterSubnetGroups(reqCtx, req)
	case "CreateClusterSnapshot":
		return p.createClusterSnapshot(reqCtx, req)
	case "DescribeClusterSnapshots":
		return p.describeClusterSnapshots(reqCtx, req)
	default:
		return nil, &AWSError{
			Code:       "InvalidAction",
			Message:    "RedshiftPlugin: unsupported action " + req.Operation,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// --- Cluster operations ------------------------------------------------------

// redshiftEndpointXML represents a Redshift cluster endpoint in XML.
type redshiftEndpointXML struct {
	Address string `xml:"Address"`
	Port    int    `xml:"Port"`
}

// redshiftClusterXML represents a Redshift cluster in XML responses.
type redshiftClusterXML struct {
	XMLName             xml.Name            `xml:"member"`
	ClusterIdentifier   string              `xml:"ClusterIdentifier"`
	ClusterStatus       string              `xml:"ClusterStatus"`
	NodeType            string              `xml:"NodeType"`
	MasterUsername      string              `xml:"MasterUsername"`
	DBName              string              `xml:"DBName"`
	NumberOfNodes       int                 `xml:"NumberOfNodes"`
	ClusterCreateTime   time.Time           `xml:"ClusterCreateTime"`
	VpcID               string              `xml:"VpcId,omitempty"`
	AvailabilityZone    string              `xml:"AvailabilityZone,omitempty"`
	ClusterNamespaceArn string              `xml:"ClusterNamespaceArn,omitempty"`
	Endpoint            redshiftEndpointXML `xml:"Endpoint"`
}

// redshiftClusterListXML wraps a list of clusters for DescribeClusters.
type redshiftClusterListXML struct {
	XMLName  xml.Name             `xml:"DescribeClustersResult"`
	Clusters []redshiftClusterXML `xml:"Clusters>member"`
}

// redshiftCreateClusterResultXML wraps the CreateCluster result.
type redshiftCreateClusterResultXML struct {
	XMLName xml.Name           `xml:"CreateClusterResult"`
	Cluster redshiftClusterXML `xml:"Cluster>member"`
}

// redshiftModifyClusterResultXML wraps the ModifyCluster result.
type redshiftModifyClusterResultXML struct {
	XMLName xml.Name           `xml:"ModifyClusterResult"`
	Cluster redshiftClusterXML `xml:"Cluster>member"`
}

// redshiftDeleteClusterResultXML wraps the DeleteCluster result.
type redshiftDeleteClusterResultXML struct {
	XMLName xml.Name           `xml:"DeleteClusterResult"`
	Cluster redshiftClusterXML `xml:"Cluster>member"`
}

func clusterToXML(c RedshiftCluster) redshiftClusterXML {
	return redshiftClusterXML{
		ClusterIdentifier:   c.ClusterIdentifier,
		ClusterStatus:       c.ClusterStatus,
		NodeType:            c.NodeType,
		MasterUsername:      c.MasterUsername,
		DBName:              c.DBName,
		NumberOfNodes:       c.NumberOfNodes,
		ClusterCreateTime:   c.ClusterCreateTime,
		VpcID:               c.VpcID,
		AvailabilityZone:    c.AvailabilityZone,
		ClusterNamespaceArn: c.ClusterArn,
		Endpoint: redshiftEndpointXML{
			Address: c.EndpointAddress,
			Port:    c.EndpointPort,
		},
	}
}

func (p *RedshiftPlugin) createCluster(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id := req.Params["ClusterIdentifier"]
	if id == "" {
		return nil, &AWSError{Code: "MissingParameter", Message: "ClusterIdentifier is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	existing, _ := p.state.Get(goCtx, redshiftNamespace, redshiftClusterKey(reqCtx.AccountID, reqCtx.Region, id))
	if existing != nil {
		return nil, &AWSError{Code: "ClusterAlreadyExistsFault", Message: "Cluster " + id + " already exists.", HTTPStatus: http.StatusBadRequest}
	}

	nodeType := req.Params["NodeType"]
	if nodeType == "" {
		nodeType = "dc2.large"
	}
	masterUsername := req.Params["MasterUsername"]
	dbName := req.Params["DBName"]
	if dbName == "" {
		dbName = "dev"
	}
	numberOfNodes := 1
	if n := req.Params["NumberOfNodes"]; n != "" {
		_, _ = fmt.Sscanf(n, "%d", &numberOfNodes)
	}

	clusterArn := fmt.Sprintf("arn:aws:redshift:%s:%s:cluster:%s", reqCtx.Region, reqCtx.AccountID, id)
	cluster := RedshiftCluster{
		ClusterIdentifier: id,
		ClusterStatus:     "available",
		NodeType:          nodeType,
		MasterUsername:    masterUsername,
		DBName:            dbName,
		EndpointAddress:   fmt.Sprintf("%s.%s.%s.redshift.amazonaws.com", id, reqCtx.AccountID, reqCtx.Region),
		EndpointPort:      5439,
		ClusterCreateTime: p.tc.Now(),
		NumberOfNodes:     numberOfNodes,
		ClusterArn:        clusterArn,
		AccountID:         reqCtx.AccountID,
		Region:            reqCtx.Region,
	}

	d, err := json.Marshal(cluster)
	if err != nil {
		return nil, fmt.Errorf("redshift createCluster marshal: %w", err)
	}
	if err := p.state.Put(goCtx, redshiftNamespace, redshiftClusterKey(reqCtx.AccountID, reqCtx.Region, id), d); err != nil {
		return nil, fmt.Errorf("redshift createCluster put: %w", err)
	}
	updateStringIndex(goCtx, p.state, redshiftNamespace, redshiftClusterIDsKey(reqCtx.AccountID, reqCtx.Region), id)

	return redshiftXMLResponse(http.StatusOK, redshiftCreateClusterResultXML{
		Cluster: clusterToXML(cluster),
	}, reqCtx.RequestID)
}

func (p *RedshiftPlugin) describeClusters(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	filterID := req.Params["ClusterIdentifier"]

	ids, err := loadStringIndex(goCtx, p.state, redshiftNamespace, redshiftClusterIDsKey(reqCtx.AccountID, reqCtx.Region))
	if err != nil {
		return nil, fmt.Errorf("redshift describeClusters load index: %w", err)
	}

	clusters := make([]redshiftClusterXML, 0)
	for _, id := range ids {
		if filterID != "" && id != filterID {
			continue
		}
		data, err := p.state.Get(goCtx, redshiftNamespace, redshiftClusterKey(reqCtx.AccountID, reqCtx.Region, id))
		if err != nil || data == nil {
			continue
		}
		var c RedshiftCluster
		if err := json.Unmarshal(data, &c); err != nil {
			continue
		}
		clusters = append(clusters, clusterToXML(c))
	}

	if filterID != "" && len(clusters) == 0 {
		return nil, &AWSError{Code: "ClusterNotFoundFault", Message: "Cluster " + filterID + " not found.", HTTPStatus: http.StatusNotFound}
	}

	return redshiftXMLResponse(http.StatusOK, redshiftClusterListXML{Clusters: clusters}, reqCtx.RequestID)
}

func (p *RedshiftPlugin) modifyCluster(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id := req.Params["ClusterIdentifier"]
	cluster, err := p.loadCluster(reqCtx.AccountID, reqCtx.Region, id)
	if err != nil {
		return nil, err
	}

	if nodeType := req.Params["NodeType"]; nodeType != "" {
		cluster.NodeType = nodeType
	}
	if n := req.Params["NumberOfNodes"]; n != "" {
		var num int
		if _, err := fmt.Sscanf(n, "%d", &num); err == nil && num > 0 {
			cluster.NumberOfNodes = num
		}
	}

	goCtx := context.Background()
	d, err := json.Marshal(cluster)
	if err != nil {
		return nil, fmt.Errorf("redshift modifyCluster marshal: %w", err)
	}
	if err := p.state.Put(goCtx, redshiftNamespace, redshiftClusterKey(reqCtx.AccountID, reqCtx.Region, id), d); err != nil {
		return nil, fmt.Errorf("redshift modifyCluster put: %w", err)
	}
	return redshiftXMLResponse(http.StatusOK, redshiftModifyClusterResultXML{
		Cluster: clusterToXML(*cluster),
	}, reqCtx.RequestID)
}

func (p *RedshiftPlugin) deleteCluster(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id := req.Params["ClusterIdentifier"]
	cluster, err := p.loadCluster(reqCtx.AccountID, reqCtx.Region, id)
	if err != nil {
		return nil, err
	}

	goCtx := context.Background()
	if err := p.state.Delete(goCtx, redshiftNamespace, redshiftClusterKey(reqCtx.AccountID, reqCtx.Region, id)); err != nil {
		return nil, fmt.Errorf("redshift deleteCluster delete: %w", err)
	}
	removeFromStringIndex(goCtx, p.state, redshiftNamespace, redshiftClusterIDsKey(reqCtx.AccountID, reqCtx.Region), id)

	return redshiftXMLResponse(http.StatusOK, redshiftDeleteClusterResultXML{
		Cluster: clusterToXML(*cluster),
	}, reqCtx.RequestID)
}

// --- Parameter group operations ----------------------------------------------

// redshiftParamGroupData holds cluster parameter group fields for XML encoding.
type redshiftParamGroupData struct {
	ParameterGroupName   string `xml:"ParameterGroupName"`
	ParameterGroupFamily string `xml:"ParameterGroupFamily"`
	Description          string `xml:"Description,omitempty"`
}

// redshiftParamGroupListXML wraps a list of parameter groups.
type redshiftParamGroupListXML struct {
	XMLName         xml.Name                 `xml:"DescribeClusterParameterGroupsResult"`
	ParameterGroups []redshiftParamGroupData `xml:"ParameterGroups>member"`
}

// redshiftCreateParamGroupResultXML wraps the CreateClusterParameterGroup result.
type redshiftCreateParamGroupResultXML struct {
	XMLName        xml.Name               `xml:"CreateClusterParameterGroupResult"`
	ParameterGroup redshiftParamGroupData `xml:"ClusterParameterGroup"`
}

func (p *RedshiftPlugin) createClusterParameterGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["ParameterGroupName"]
	family := req.Params["ParameterGroupFamily"]
	description := req.Params["Description"]
	if name == "" {
		return nil, &AWSError{Code: "MissingParameter", Message: "ParameterGroupName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	pg := RedshiftClusterParameterGroup{
		ParameterGroupName:   name,
		ParameterGroupFamily: family,
		Description:          description,
		AccountID:            reqCtx.AccountID,
		Region:               reqCtx.Region,
	}
	d, err := json.Marshal(pg)
	if err != nil {
		return nil, fmt.Errorf("redshift createClusterParameterGroup marshal: %w", err)
	}
	if err := p.state.Put(goCtx, redshiftNamespace, redshiftParamGroupKey(reqCtx.AccountID, reqCtx.Region, name), d); err != nil {
		return nil, fmt.Errorf("redshift createClusterParameterGroup put: %w", err)
	}
	updateStringIndex(goCtx, p.state, redshiftNamespace, redshiftParamGroupNamesKey(reqCtx.AccountID, reqCtx.Region), name)

	return redshiftXMLResponse(http.StatusOK, redshiftCreateParamGroupResultXML{
		ParameterGroup: redshiftParamGroupData{
			ParameterGroupName:   pg.ParameterGroupName,
			ParameterGroupFamily: pg.ParameterGroupFamily,
			Description:          pg.Description,
		},
	}, reqCtx.RequestID)
}

func (p *RedshiftPlugin) describeClusterParameterGroups(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	names, err := loadStringIndex(goCtx, p.state, redshiftNamespace, redshiftParamGroupNamesKey(reqCtx.AccountID, reqCtx.Region))
	if err != nil {
		return nil, fmt.Errorf("redshift describeClusterParameterGroups load index: %w", err)
	}

	groups := make([]redshiftParamGroupData, 0)
	for _, name := range names {
		data, err := p.state.Get(goCtx, redshiftNamespace, redshiftParamGroupKey(reqCtx.AccountID, reqCtx.Region, name))
		if err != nil || data == nil {
			continue
		}
		var pg RedshiftClusterParameterGroup
		if err := json.Unmarshal(data, &pg); err != nil {
			continue
		}
		groups = append(groups, redshiftParamGroupData{
			ParameterGroupName:   pg.ParameterGroupName,
			ParameterGroupFamily: pg.ParameterGroupFamily,
			Description:          pg.Description,
		})
	}
	return redshiftXMLResponse(http.StatusOK, redshiftParamGroupListXML{ParameterGroups: groups}, reqCtx.RequestID)
}

// --- Subnet group operations -------------------------------------------------

// redshiftSubnetGroupData holds cluster subnet group fields for XML encoding.
type redshiftSubnetGroupData struct {
	ClusterSubnetGroupName string `xml:"ClusterSubnetGroupName"`
	Description            string `xml:"Description,omitempty"`
	VpcID                  string `xml:"VpcId,omitempty"`
}

// redshiftSubnetGroupListXML wraps a list of subnet groups.
type redshiftSubnetGroupListXML struct {
	XMLName      xml.Name                  `xml:"DescribeClusterSubnetGroupsResult"`
	SubnetGroups []redshiftSubnetGroupData `xml:"ClusterSubnetGroups>member"`
}

// redshiftCreateSubnetGroupResultXML wraps the CreateClusterSubnetGroup result.
type redshiftCreateSubnetGroupResultXML struct {
	XMLName     xml.Name                `xml:"CreateClusterSubnetGroupResult"`
	SubnetGroup redshiftSubnetGroupData `xml:"ClusterSubnetGroup"`
}

func (p *RedshiftPlugin) createClusterSubnetGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["ClusterSubnetGroupName"]
	description := req.Params["Description"]
	vpcID := req.Params["VpcId"]
	if name == "" {
		return nil, &AWSError{Code: "MissingParameter", Message: "ClusterSubnetGroupName is required", HTTPStatus: http.StatusBadRequest}
	}

	goCtx := context.Background()
	sg := RedshiftClusterSubnetGroup{
		ClusterSubnetGroupName: name,
		Description:            description,
		VpcID:                  vpcID,
		AccountID:              reqCtx.AccountID,
		Region:                 reqCtx.Region,
	}
	d, err := json.Marshal(sg)
	if err != nil {
		return nil, fmt.Errorf("redshift createClusterSubnetGroup marshal: %w", err)
	}
	if err := p.state.Put(goCtx, redshiftNamespace, redshiftSubnetGroupKey(reqCtx.AccountID, reqCtx.Region, name), d); err != nil {
		return nil, fmt.Errorf("redshift createClusterSubnetGroup put: %w", err)
	}
	updateStringIndex(goCtx, p.state, redshiftNamespace, redshiftSubnetGroupNamesKey(reqCtx.AccountID, reqCtx.Region), name)

	return redshiftXMLResponse(http.StatusOK, redshiftCreateSubnetGroupResultXML{
		SubnetGroup: redshiftSubnetGroupData{
			ClusterSubnetGroupName: sg.ClusterSubnetGroupName,
			Description:            sg.Description,
			VpcID:                  sg.VpcID,
		},
	}, reqCtx.RequestID)
}

func (p *RedshiftPlugin) describeClusterSubnetGroups(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	names, err := loadStringIndex(goCtx, p.state, redshiftNamespace, redshiftSubnetGroupNamesKey(reqCtx.AccountID, reqCtx.Region))
	if err != nil {
		return nil, fmt.Errorf("redshift describeClusterSubnetGroups load index: %w", err)
	}

	groups := make([]redshiftSubnetGroupData, 0)
	for _, name := range names {
		data, err := p.state.Get(goCtx, redshiftNamespace, redshiftSubnetGroupKey(reqCtx.AccountID, reqCtx.Region, name))
		if err != nil || data == nil {
			continue
		}
		var sg RedshiftClusterSubnetGroup
		if err := json.Unmarshal(data, &sg); err != nil {
			continue
		}
		groups = append(groups, redshiftSubnetGroupData{
			ClusterSubnetGroupName: sg.ClusterSubnetGroupName,
			Description:            sg.Description,
			VpcID:                  sg.VpcID,
		})
	}
	return redshiftXMLResponse(http.StatusOK, redshiftSubnetGroupListXML{SubnetGroups: groups}, reqCtx.RequestID)
}

// --- Snapshot operations -----------------------------------------------------

// redshiftSnapshotData holds cluster snapshot fields for XML encoding.
type redshiftSnapshotData struct {
	SnapshotIdentifier string    `xml:"SnapshotIdentifier"`
	ClusterIdentifier  string    `xml:"ClusterIdentifier"`
	SnapshotType       string    `xml:"SnapshotType"`
	Status             string    `xml:"Status"`
	SnapshotCreateTime time.Time `xml:"SnapshotCreateTime"`
}

// redshiftSnapshotListXML wraps a list of snapshots.
type redshiftSnapshotListXML struct {
	XMLName   xml.Name               `xml:"DescribeClusterSnapshotsResult"`
	Snapshots []redshiftSnapshotData `xml:"Snapshots>member"`
}

// redshiftCreateSnapshotResultXML wraps the CreateClusterSnapshot result.
type redshiftCreateSnapshotResultXML struct {
	XMLName  xml.Name             `xml:"CreateClusterSnapshotResult"`
	Snapshot redshiftSnapshotData `xml:"Snapshot"`
}

func (p *RedshiftPlugin) createClusterSnapshot(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	clusterID := req.Params["ClusterIdentifier"]
	snapshotID := req.Params["SnapshotIdentifier"]
	if clusterID == "" || snapshotID == "" {
		return nil, &AWSError{Code: "MissingParameter", Message: "ClusterIdentifier and SnapshotIdentifier are required", HTTPStatus: http.StatusBadRequest}
	}
	if _, err := p.loadCluster(reqCtx.AccountID, reqCtx.Region, clusterID); err != nil {
		return nil, err
	}

	goCtx := context.Background()
	snapshot := RedshiftSnapshot{
		SnapshotIdentifier: snapshotID,
		ClusterIdentifier:  clusterID,
		SnapshotType:       "manual",
		Status:             "available",
		SnapshotCreateTime: p.tc.Now(),
		AccountID:          reqCtx.AccountID,
		Region:             reqCtx.Region,
	}
	d, err := json.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf("redshift createClusterSnapshot marshal: %w", err)
	}
	if err := p.state.Put(goCtx, redshiftNamespace, redshiftSnapshotKey(reqCtx.AccountID, reqCtx.Region, snapshotID), d); err != nil {
		return nil, fmt.Errorf("redshift createClusterSnapshot put: %w", err)
	}
	updateStringIndex(goCtx, p.state, redshiftNamespace, redshiftSnapshotIDsKey(reqCtx.AccountID, reqCtx.Region), snapshotID)

	return redshiftXMLResponse(http.StatusOK, redshiftCreateSnapshotResultXML{
		Snapshot: redshiftSnapshotData{
			SnapshotIdentifier: snapshot.SnapshotIdentifier,
			ClusterIdentifier:  snapshot.ClusterIdentifier,
			SnapshotType:       snapshot.SnapshotType,
			Status:             snapshot.Status,
			SnapshotCreateTime: snapshot.SnapshotCreateTime,
		},
	}, reqCtx.RequestID)
}

func (p *RedshiftPlugin) describeClusterSnapshots(reqCtx *RequestContext, _ *AWSRequest) (*AWSResponse, error) {
	goCtx := context.Background()
	ids, err := loadStringIndex(goCtx, p.state, redshiftNamespace, redshiftSnapshotIDsKey(reqCtx.AccountID, reqCtx.Region))
	if err != nil {
		return nil, fmt.Errorf("redshift describeClusterSnapshots load index: %w", err)
	}

	snapshots := make([]redshiftSnapshotData, 0)
	for _, id := range ids {
		data, err := p.state.Get(goCtx, redshiftNamespace, redshiftSnapshotKey(reqCtx.AccountID, reqCtx.Region, id))
		if err != nil || data == nil {
			continue
		}
		var snap RedshiftSnapshot
		if err := json.Unmarshal(data, &snap); err != nil {
			continue
		}
		snapshots = append(snapshots, redshiftSnapshotData{
			SnapshotIdentifier: snap.SnapshotIdentifier,
			ClusterIdentifier:  snap.ClusterIdentifier,
			SnapshotType:       snap.SnapshotType,
			Status:             snap.Status,
			SnapshotCreateTime: snap.SnapshotCreateTime,
		})
	}
	return redshiftXMLResponse(http.StatusOK, redshiftSnapshotListXML{Snapshots: snapshots}, reqCtx.RequestID)
}

// --- Helpers -----------------------------------------------------------------

// loadCluster loads a RedshiftCluster from state or returns a not-found error.
func (p *RedshiftPlugin) loadCluster(acct, region, id string) (*RedshiftCluster, error) {
	if id == "" {
		return nil, &AWSError{Code: "MissingParameter", Message: "ClusterIdentifier is required", HTTPStatus: http.StatusBadRequest}
	}
	goCtx := context.Background()
	data, err := p.state.Get(goCtx, redshiftNamespace, redshiftClusterKey(acct, region, id))
	if err != nil {
		return nil, fmt.Errorf("redshift loadCluster get: %w", err)
	}
	if data == nil {
		return nil, &AWSError{Code: "ClusterNotFoundFault", Message: "Cluster " + id + " not found.", HTTPStatus: http.StatusNotFound}
	}
	var c RedshiftCluster
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("redshift loadCluster unmarshal: %w", err)
	}
	return &c, nil
}

// redshiftXMLResponse marshals result to XML and returns an AWSResponse with
// Content-Type text/xml as required by the Redshift query protocol.
func redshiftXMLResponse(status int, result interface{}, _ string) (*AWSResponse, error) {
	body, err := xml.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("redshift xml.Marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "text/xml; charset=UTF-8"},
		Body:       append([]byte(xml.Header), body...),
	}, nil
}
