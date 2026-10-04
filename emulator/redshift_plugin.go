package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
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
		return nil, redshiftUnroutedAction(req.Operation)
	}
}

// --- Cluster operations ------------------------------------------------------

// redshiftEndpointXML represents a Redshift cluster endpoint in XML.
type redshiftEndpointXML struct {
	Address string `xml:"Address"`
	Port    int    `xml:"Port"`
}

// redshiftClusterXML represents a Redshift cluster in XML responses.
//
// It carries no `XMLName`: the element name belongs to the field that holds the value, which is
// `<Cluster>` for a single cluster and `<Clusters><Cluster>` for a list, both as API_CreateCluster and
// API_DescribeClusters publish. Until #1208 the type forced `<member>`, so a single cluster rendered
// `<Cluster><member>` and a list rendered `<Clusters><member>`, neither of which an SDK reads.
//
// # The members it does not send (#1199)
//
// API_Cluster publishes 63 members, every one `Required: No`, and this answers the 10 the record
// models: ClusterIdentifier, ClusterStatus, NodeType, MasterUsername, DBName, NumberOfNodes,
// ClusterCreateTime, VpcId, AvailabilityZone and Endpoint. The other 53 are absent rather than
// present and empty (#1013's rule), because the record holds no value for any of them:
//
//   - **What CreateCluster accepts and the record does not keep**: AllowVersionUpgrade,
//     AutomatedSnapshotRetentionPeriod, ManualSnapshotRetentionPeriod, ClusterVersion,
//     ClusterSubnetGroupName, ClusterParameterGroups, ClusterSecurityGroups, VpcSecurityGroups,
//     Encrypted, KmsKeyId, EnhancedVpcRouting, PubliclyAccessible, IamRoles, DefaultIamRoleArn,
//     IpAddressType, MaintenanceTrackName, PreferredMaintenanceWindow, MultiAZ,
//     SnapshotScheduleIdentifier, MasterPasswordSecretArn, MasterPasswordSecretKmsKeyId, CatalogArn,
//     ExtraComputeForAutomaticOptimization and Tags. Rendering a published default here would assert
//     a value the caller may have set otherwise.
//   - **Service-side state substrate does not model**: ClusterAvailabilityStatus, ClusterNodes,
//     ClusterPublicKey, ClusterRevisionNumber, ClusterSnapshotCopyStatus, DataTransferProgress,
//     DeferredMaintenanceWindows, ElasticIpStatus, ElasticResizeNumberOfNodeOptions,
//     ExpectedNextSnapshotScheduleTime and its status, HsmStatus, ModifyStatus, MultiAZSecondary,
//     NextMaintenanceWindowStartTime, PendingActions, PendingModifiedValues,
//     ReservedNodeExchangeStatus, ResizeInfo, RestoreStatus, SnapshotScheduleState,
//     TotalStorageCapacityInMegaBytes, AvailabilityZoneRelocationStatus, LakehouseRegistrationStatus,
//     AquaConfiguration and the three CustomDomain members.
//   - **ClusterNamespaceArn**, which API_Cluster glosses as "the namespace Amazon Resource Name (ARN)
//     of the cluster". Until #1199 substrate filled it with the cluster's own `:cluster:` ARN. A
//     namespace ARN names a namespace by its UUID, which substrate does not mint, so the member is
//     omitted rather than answered with an ARN of the wrong resource type.
type redshiftClusterXML struct {
	ClusterIdentifier string              `xml:"ClusterIdentifier"`
	ClusterStatus     string              `xml:"ClusterStatus"`
	NodeType          string              `xml:"NodeType"`
	MasterUsername    string              `xml:"MasterUsername"`
	DBName            string              `xml:"DBName"`
	NumberOfNodes     int                 `xml:"NumberOfNodes"`
	ClusterCreateTime time.Time           `xml:"ClusterCreateTime"`
	VpcID             string              `xml:"VpcId,omitempty"`
	AvailabilityZone  string              `xml:"AvailabilityZone,omitempty"`
	Endpoint          redshiftEndpointXML `xml:"Endpoint"`
}

// redshiftClusterListXML is DescribeClusters' result: `Clusters.Cluster.N` and the `Marker` that
// resumes it, omitted on the last page.
type redshiftClusterListXML struct {
	Clusters []redshiftClusterXML `xml:"Clusters>Cluster"`
	Marker   string               `xml:"Marker,omitempty"`
}

// redshiftClusterResultXML is the result of CreateCluster, ModifyCluster and DeleteCluster, each of
// which publishes one `Cluster` element.
type redshiftClusterResultXML struct {
	Cluster redshiftClusterXML `xml:"Cluster"`
}

func clusterToXML(c RedshiftCluster) redshiftClusterXML {
	return redshiftClusterXML{
		ClusterIdentifier: c.ClusterIdentifier,
		ClusterStatus:     c.ClusterStatus,
		NodeType:          c.NodeType,
		MasterUsername:    c.MasterUsername,
		DBName:            c.DBName,
		NumberOfNodes:     c.NumberOfNodes,
		ClusterCreateTime: c.ClusterCreateTime,
		VpcID:             c.VpcID,
		AvailabilityZone:  c.AvailabilityZone,
		Endpoint: redshiftEndpointXML{
			Address: c.EndpointAddress,
			Port:    c.EndpointPort,
		},
	}
}

// createCluster handles CreateCluster.
//
// API_CreateCluster marks three members `Required: Yes` — ClusterIdentifier, MasterUsername and
// NodeType — and until #1197 only the first was checked: an absent NodeType was defaulted to
// dc2.large and an absent MasterUsername stored empty, so a template omitting either validated here
// and failed against AWS. Each is now refused with `MissingParameter`/400, from Redshift's own Common
// Errors page, in the order the page lists them. See [redshiftCheckCreateCluster] for the values each
// checked member must also satisfy.
func (p *RedshiftPlugin) createCluster(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	if awsErr := redshiftCheckCreateCluster(req.Params); awsErr != nil {
		return nil, awsErr
	}
	id := req.Params["ClusterIdentifier"]

	goCtx := context.Background()
	existing, err := p.state.Get(goCtx, redshiftNamespace, redshiftClusterKey(reqCtx.AccountID, reqCtx.Region, id))
	if err != nil {
		return nil, fmt.Errorf("redshift createCluster get: %w", err)
	}
	if existing != nil {
		return nil, &AWSError{Code: "ClusterAlreadyExists", Message: "Cluster " + id + " already exists.", HTTPStatus: http.StatusBadRequest}
	}

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
		NodeType:          req.Params["NodeType"],
		MasterUsername:    req.Params["MasterUsername"],
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

	return redshiftOKResponse(reqCtx, "CreateCluster", redshiftClusterResultXML{Cluster: clusterToXML(cluster)})
}

// describeClusters handles DescribeClusters.
//
// It pages with [queryMarkerPage], the value-based cursor the RDS and ElastiCache describes share,
// over the cluster keys in lexicographic order. API_DescribeClusters publishes the same MaxRecords
// contract they do (default 100, minimum 20, maximum 100) and an opaque Marker, so one cursor serves
// all three families (#1195). Its page publishes no code for an unusable Marker or an out-of-range
// MaxRecords; both answer `InvalidParameterValue`/400 from Redshift's Common Errors page, which the
// operation page links to, and which the shared helpers already answer.
//
// The page constrains Marker: "You can specify either the ClusterIdentifier parameter or the Marker
// parameter, but not both." Sending both is `InvalidParameterCombination`/400, the Common Errors code
// for "parameters that must not be used together".
//
// **TagKeys and TagValues are not applied.** Substrate records no tags on a cluster (CreateCluster's
// `Tags.Tag.N` is not stored), so there is nothing for a tag filter to match against; applying one
// would answer an empty list for a cluster AWS would return. They are named here, and in
// docs/services.md, rather than silently accepted as if applied.
func (p *RedshiftPlugin) describeClusters(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	filterID := req.Params["ClusterIdentifier"]
	if filterID != "" && req.Params["Marker"] != "" {
		return nil, &AWSError{
			Code:       "InvalidParameterCombination",
			Message:    "You can specify either the ClusterIdentifier parameter or the Marker parameter, but not both.",
			HTTPStatus: http.StatusBadRequest,
		}
	}
	cursor, maxRecords, awsErr := redshiftPagination(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}

	var readErr error
	page, next, err := redshiftPage(p, redshiftClusterKey(reqCtx.AccountID, reqCtx.Region, ""), cursor, maxRecords, &readErr,
		func(id string, c RedshiftCluster) (redshiftClusterXML, bool) {
			if filterID != "" && id != filterID {
				return redshiftClusterXML{}, false
			}
			return clusterToXML(c), true
		})
	if err != nil {
		return nil, fmt.Errorf("redshift describeClusters: %w", err)
	}
	if filterID != "" && len(page) == 0 {
		return nil, redshiftClusterNotFound(filterID)
	}
	return redshiftOKResponse(reqCtx, "DescribeClusters", redshiftClusterListXML{Clusters: page, Marker: next})
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
	return redshiftOKResponse(reqCtx, "ModifyCluster", redshiftClusterResultXML{Cluster: clusterToXML(*cluster)})
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

	return redshiftOKResponse(reqCtx, "DeleteCluster", redshiftClusterResultXML{Cluster: clusterToXML(*cluster)})
}

// --- Parameter group operations ----------------------------------------------

// redshiftParamGroupData holds cluster parameter group fields for XML encoding.
type redshiftParamGroupData struct {
	ParameterGroupName   string `xml:"ParameterGroupName"`
	ParameterGroupFamily string `xml:"ParameterGroupFamily"`
	Description          string `xml:"Description,omitempty"`
}

// redshiftParamGroupListXML is DescribeClusterParameterGroups' result:
// `ParameterGroups.ClusterParameterGroup.N` and its Marker.
type redshiftParamGroupListXML struct {
	ParameterGroups []redshiftParamGroupData `xml:"ParameterGroups>ClusterParameterGroup"`
	Marker          string                   `xml:"Marker,omitempty"`
}

// redshiftCreateParamGroupResultXML is CreateClusterParameterGroup's result.
type redshiftCreateParamGroupResultXML struct {
	ParameterGroup redshiftParamGroupData `xml:"ClusterParameterGroup"`
}

// createClusterParameterGroup handles CreateClusterParameterGroup.
//
// API_CreateClusterParameterGroup marks all three of its members `Required: Yes` —
// ParameterGroupName, ParameterGroupFamily and Description — and until #1197 only the name was
// checked, so a group was recorded with no family. Each absent one is `MissingParameter`/400.
//
// The name's published constraints are enforced (1 to 255 alphanumeric characters or hyphens, a
// letter first, no trailing hyphen and no two consecutive hyphens), and the name is stored lower-case,
// which the page states ("This value is stored as a lower-case string"). A name already in use is
// `ClusterParameterGroupAlreadyExists`/400, which the page publishes and which had no site: a second
// create overwrote the first. ParameterGroupFamily's valid values are not enforced: the page defers
// them to whatever DescribeClusterParameterGroups lists, and substrate lists no default groups.
func (p *RedshiftPlugin) createClusterParameterGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	for _, member := range []string{"ParameterGroupName", "ParameterGroupFamily", "Description"} {
		if req.Params[member] == "" {
			return nil, redshiftMissingParameter(member)
		}
	}
	if awsErr := redshiftCheckGroupName("ParameterGroupName", req.Params["ParameterGroupName"]); awsErr != nil {
		return nil, awsErr
	}
	name := strings.ToLower(req.Params["ParameterGroupName"])

	goCtx := context.Background()
	key := redshiftParamGroupKey(reqCtx.AccountID, reqCtx.Region, name)
	existing, err := p.state.Get(goCtx, redshiftNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("redshift createClusterParameterGroup get: %w", err)
	}
	if existing != nil {
		return nil, &AWSError{
			Code:       "ClusterParameterGroupAlreadyExists",
			Message:    "A cluster parameter group with the name " + name + " already exists.",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	pg := RedshiftClusterParameterGroup{
		ParameterGroupName:   name,
		ParameterGroupFamily: req.Params["ParameterGroupFamily"],
		Description:          req.Params["Description"],
		AccountID:            reqCtx.AccountID,
		Region:               reqCtx.Region,
	}
	d, err := json.Marshal(pg)
	if err != nil {
		return nil, fmt.Errorf("redshift createClusterParameterGroup marshal: %w", err)
	}
	if err := p.state.Put(goCtx, redshiftNamespace, key, d); err != nil {
		return nil, fmt.Errorf("redshift createClusterParameterGroup put: %w", err)
	}
	updateStringIndex(goCtx, p.state, redshiftNamespace, redshiftParamGroupNamesKey(reqCtx.AccountID, reqCtx.Region), name)

	return redshiftOKResponse(reqCtx, "CreateClusterParameterGroup", redshiftCreateParamGroupResultXML{
		ParameterGroup: paramGroupToXML(pg),
	})
}

func paramGroupToXML(pg RedshiftClusterParameterGroup) redshiftParamGroupData {
	return redshiftParamGroupData{
		ParameterGroupName:   pg.ParameterGroupName,
		ParameterGroupFamily: pg.ParameterGroupFamily,
		Description:          pg.Description,
	}
}

// describeClusterParameterGroups handles DescribeClusterParameterGroups.
//
// Until #1195 it took `_ *AWSRequest`, so Marker, MaxRecords and ParameterGroupName were all unread:
// every call answered every group in one page, and a request naming one group got the whole list.
// It now pages as [RedshiftPlugin.describeClusters] does, and ParameterGroupName narrows to that group
// or answers `ClusterParameterGroupNotFound`/404, the page's own code. The name is compared
// lower-cased, as it is stored. TagKeys and TagValues are not applied, for the reason
// [RedshiftPlugin.describeClusters] gives. The page also says the listing includes "the default
// parameter group"; substrate models none, so a listing holds only groups the caller created.
func (p *RedshiftPlugin) describeClusterParameterGroups(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	cursor, maxRecords, awsErr := redshiftPagination(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}
	filter := strings.ToLower(req.Params["ParameterGroupName"])

	var readErr error
	page, next, err := redshiftPage(p, redshiftParamGroupKey(reqCtx.AccountID, reqCtx.Region, ""), cursor, maxRecords, &readErr,
		func(id string, pg RedshiftClusterParameterGroup) (redshiftParamGroupData, bool) {
			if filter != "" && id != filter {
				return redshiftParamGroupData{}, false
			}
			return paramGroupToXML(pg), true
		})
	if err != nil {
		return nil, fmt.Errorf("redshift describeClusterParameterGroups: %w", err)
	}
	if fault := queryMarkerFilterNotFound(filter, len(page) > 0, queryMarkerFault{
		Code: "ClusterParameterGroupNotFound", Kind: "Cluster parameter group", HTTPStatus: http.StatusNotFound,
	}); fault != nil {
		return nil, fault
	}
	return redshiftOKResponse(reqCtx, "DescribeClusterParameterGroups", redshiftParamGroupListXML{ParameterGroups: page, Marker: next})
}

// --- Subnet group operations -------------------------------------------------

// redshiftSubnetXML is one `Subnets.Subnet` element of a ClusterSubnetGroup.
//
// API_Subnet publishes SubnetIdentifier, SubnetAvailabilityZone and SubnetStatus. The zone is absent:
// substrate does not resolve a subnet ID to the zone EC2 placed it in.
type redshiftSubnetXML struct {
	SubnetIdentifier string `xml:"SubnetIdentifier"`
	SubnetStatus     string `xml:"SubnetStatus"`
}

// redshiftSubnetGroupData holds cluster subnet group fields for XML encoding.
//
// VpcId is answered only for a record stored before #1197, which took it from the request (see
// [RedshiftPlugin.createClusterSubnetGroup]); a group created since carries none, because substrate
// does not resolve its subnets to their VPC.
type redshiftSubnetGroupData struct {
	ClusterSubnetGroupName string              `xml:"ClusterSubnetGroupName"`
	Description            string              `xml:"Description,omitempty"`
	VpcID                  string              `xml:"VpcId,omitempty"`
	SubnetGroupStatus      string              `xml:"SubnetGroupStatus,omitempty"`
	Subnets                []redshiftSubnetXML `xml:"Subnets>Subnet,omitempty"`
}

// redshiftSubnetGroupListXML is DescribeClusterSubnetGroups' result:
// `ClusterSubnetGroups.ClusterSubnetGroup.N` and its Marker.
type redshiftSubnetGroupListXML struct {
	SubnetGroups []redshiftSubnetGroupData `xml:"ClusterSubnetGroups>ClusterSubnetGroup"`
	Marker       string                    `xml:"Marker,omitempty"`
}

// redshiftCreateSubnetGroupResultXML is CreateClusterSubnetGroup's result.
type redshiftCreateSubnetGroupResultXML struct {
	SubnetGroup redshiftSubnetGroupData `xml:"ClusterSubnetGroup"`
}

// redshiftMaxSubnetsPerRequest is the published ceiling on `SubnetIds.SubnetIdentifier.N`, which
// API_CreateClusterSubnetGroup states as "A maximum of 20 subnets can be modified in a single request".
const redshiftMaxSubnetsPerRequest = 20

// createClusterSubnetGroup handles CreateClusterSubnetGroup.
//
// API_CreateClusterSubnetGroup marks ClusterSubnetGroupName, Description and
// `SubnetIds.SubnetIdentifier.N` `Required: Yes`. Until #1197 only the name was checked, and the
// handler read `VpcId` off the request, which is a **response** member of ClusterSubnetGroup and never
// a request parameter, while the subnets the page requires were not read at all. It now refuses each
// absent member with `MissingParameter`/400, records the subnets, and answers them as `Subnets` with
// `SubnetGroupStatus` Complete, as the page's sample does.
//
// The name's published constraints are enforced (no more than 255 alphanumeric characters or hyphens,
// and not "Default"), and it is stored lower-case, as the page states. A name in use is
// `ClusterSubnetGroupAlreadyExists`/400 and more than twenty subnets is
// `ClusterSubnetQuotaExceededFault`/400, both from this page; neither had a site.
func (p *RedshiftPlugin) createClusterSubnetGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	for _, member := range []string{"ClusterSubnetGroupName", "Description"} {
		if req.Params[member] == "" {
			return nil, redshiftMissingParameter(member)
		}
	}
	subnets := redshiftIndexedParams(req.Params, "SubnetIds.SubnetIdentifier.")
	if len(subnets) == 0 {
		return nil, redshiftMissingParameter("SubnetIds")
	}
	if len(subnets) > redshiftMaxSubnetsPerRequest {
		return nil, &AWSError{
			Code:       "ClusterSubnetQuotaExceededFault",
			Message:    fmt.Sprintf("A cluster subnet group can hold at most %d subnets; the request names %d.", redshiftMaxSubnetsPerRequest, len(subnets)),
			HTTPStatus: http.StatusBadRequest,
		}
	}
	if awsErr := redshiftCheckSubnetGroupName(req.Params["ClusterSubnetGroupName"]); awsErr != nil {
		return nil, awsErr
	}
	name := strings.ToLower(req.Params["ClusterSubnetGroupName"])

	goCtx := context.Background()
	key := redshiftSubnetGroupKey(reqCtx.AccountID, reqCtx.Region, name)
	existing, err := p.state.Get(goCtx, redshiftNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("redshift createClusterSubnetGroup get: %w", err)
	}
	if existing != nil {
		return nil, &AWSError{
			Code:       "ClusterSubnetGroupAlreadyExists",
			Message:    "The cluster subnet group name " + name + " is already in use.",
			HTTPStatus: http.StatusBadRequest,
		}
	}

	sg := RedshiftClusterSubnetGroup{
		ClusterSubnetGroupName: name,
		Description:            req.Params["Description"],
		SubnetIDs:              subnets,
		AccountID:              reqCtx.AccountID,
		Region:                 reqCtx.Region,
	}
	d, err := json.Marshal(sg)
	if err != nil {
		return nil, fmt.Errorf("redshift createClusterSubnetGroup marshal: %w", err)
	}
	if err := p.state.Put(goCtx, redshiftNamespace, key, d); err != nil {
		return nil, fmt.Errorf("redshift createClusterSubnetGroup put: %w", err)
	}
	updateStringIndex(goCtx, p.state, redshiftNamespace, redshiftSubnetGroupNamesKey(reqCtx.AccountID, reqCtx.Region), name)

	return redshiftOKResponse(reqCtx, "CreateClusterSubnetGroup", redshiftCreateSubnetGroupResultXML{
		SubnetGroup: subnetGroupToXML(sg),
	})
}

func subnetGroupToXML(sg RedshiftClusterSubnetGroup) redshiftSubnetGroupData {
	out := redshiftSubnetGroupData{
		ClusterSubnetGroupName: sg.ClusterSubnetGroupName,
		Description:            sg.Description,
		VpcID:                  sg.VpcID,
	}
	if len(sg.SubnetIDs) > 0 {
		out.SubnetGroupStatus = "Complete"
		for _, id := range sg.SubnetIDs {
			out.Subnets = append(out.Subnets, redshiftSubnetXML{SubnetIdentifier: id, SubnetStatus: "Active"})
		}
	}
	return out
}

// describeClusterSubnetGroups handles DescribeClusterSubnetGroups.
//
// Until #1195 it took `_ *AWSRequest`. It now pages as [RedshiftPlugin.describeClusters] does, and
// ClusterSubnetGroupName narrows to that group or answers `ClusterSubnetGroupNotFoundFault`/**400** —
// the page's own code and status, which carry the `Fault` suffix and a 400 where the parameter-group
// and snapshot faults do not. TagKeys and TagValues are not applied, for the reason
// [RedshiftPlugin.describeClusters] gives.
func (p *RedshiftPlugin) describeClusterSubnetGroups(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	cursor, maxRecords, awsErr := redshiftPagination(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}
	filter := strings.ToLower(req.Params["ClusterSubnetGroupName"])

	var readErr error
	page, next, err := redshiftPage(p, redshiftSubnetGroupKey(reqCtx.AccountID, reqCtx.Region, ""), cursor, maxRecords, &readErr,
		func(id string, sg RedshiftClusterSubnetGroup) (redshiftSubnetGroupData, bool) {
			if filter != "" && id != filter {
				return redshiftSubnetGroupData{}, false
			}
			return subnetGroupToXML(sg), true
		})
	if err != nil {
		return nil, fmt.Errorf("redshift describeClusterSubnetGroups: %w", err)
	}
	if fault := queryMarkerFilterNotFound(filter, len(page) > 0, queryMarkerFault{
		Code: "ClusterSubnetGroupNotFoundFault", Kind: "Cluster subnet group", HTTPStatus: http.StatusBadRequest,
	}); fault != nil {
		return nil, fault
	}
	return redshiftOKResponse(reqCtx, "DescribeClusterSubnetGroups", redshiftSubnetGroupListXML{SubnetGroups: page, Marker: next})
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

// redshiftSnapshotListXML is DescribeClusterSnapshots' result: `Snapshots.Snapshot.N` and its Marker.
type redshiftSnapshotListXML struct {
	Snapshots []redshiftSnapshotData `xml:"Snapshots>Snapshot"`
	Marker    string                 `xml:"Marker,omitempty"`
}

// redshiftCreateSnapshotResultXML is CreateClusterSnapshot's result.
type redshiftCreateSnapshotResultXML struct {
	Snapshot redshiftSnapshotData `xml:"Snapshot"`
}

func snapshotToXML(s RedshiftSnapshot) redshiftSnapshotData {
	return redshiftSnapshotData{
		SnapshotIdentifier: s.SnapshotIdentifier,
		ClusterIdentifier:  s.ClusterIdentifier,
		SnapshotType:       s.SnapshotType,
		Status:             s.Status,
		SnapshotCreateTime: s.SnapshotCreateTime,
	}
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

	return redshiftOKResponse(reqCtx, "CreateClusterSnapshot", redshiftCreateSnapshotResultXML{Snapshot: snapshotToXML(snapshot)})
}

// describeClusterSnapshots handles DescribeClusterSnapshots.
//
// Until #1195 it took `_ *AWSRequest`, so its pagination and all of its filters were unread and a
// request for one cluster's snapshots, or one snapshot, answered every snapshot. It now pages as
// [RedshiftPlugin.describeClusters] does, and applies each filter API_DescribeClusterSnapshots
// publishes that substrate holds a value for. See [redshiftSnapshotFilter] for each one's reading.
// SnapshotIdentifier naming nothing is `ClusterSnapshotNotFound`/404, the page's own code.
func (p *RedshiftPlugin) describeClusterSnapshots(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	cursor, maxRecords, awsErr := redshiftPagination(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}
	filter, awsErr := parseRedshiftSnapshotFilter(req.Params)
	if awsErr != nil {
		return nil, awsErr
	}

	exists := map[string]bool{}
	var readErr error
	page, next, err := redshiftPage(p, redshiftSnapshotKey(reqCtx.AccountID, reqCtx.Region, ""), cursor, maxRecords, &readErr,
		func(_ string, snap RedshiftSnapshot) (redshiftSnapshotData, bool) {
			if filter.clusterExists != nil {
				present, seen := exists[snap.ClusterIdentifier]
				if !seen {
					data, getErr := p.state.Get(context.Background(), redshiftNamespace,
						redshiftClusterKey(reqCtx.AccountID, reqCtx.Region, snap.ClusterIdentifier))
					if getErr != nil {
						readErr = fmt.Errorf("cluster %s: %w", snap.ClusterIdentifier, getErr)
						return redshiftSnapshotData{}, false
					}
					present = data != nil
					exists[snap.ClusterIdentifier] = present
				}
				if present != *filter.clusterExists {
					return redshiftSnapshotData{}, false
				}
			}
			if !filter.matches(reqCtx, snap) {
				return redshiftSnapshotData{}, false
			}
			return snapshotToXML(snap), true
		})
	if err != nil {
		return nil, fmt.Errorf("redshift describeClusterSnapshots: %w", err)
	}
	if fault := queryMarkerFilterNotFound(filter.snapshotID, len(page) > 0, queryMarkerFault{
		Code: "ClusterSnapshotNotFound", Kind: "Snapshot", HTTPStatus: http.StatusNotFound,
	}); fault != nil {
		return nil, fault
	}
	return redshiftOKResponse(reqCtx, "DescribeClusterSnapshots", redshiftSnapshotListXML{Snapshots: page, Marker: next})
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
		return nil, redshiftClusterNotFound(id)
	}
	var c RedshiftCluster
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("redshift loadCluster unmarshal: %w", err)
	}
	return &c, nil
}

// redshiftXMLNS is the namespace every Redshift response document is in, as each operation page's
// sample response shows.
const redshiftXMLNS = "http://redshift.amazonaws.com/doc/2012-12-01/"

// redshiftOKResponse answers a Redshift operation in the Query protocol's three-level document,
// `<{Operation}Response xmlns=…><{Operation}Result>…</…Result><ResponseMetadata><RequestId>`, which
// every Redshift operation page's sample response shows.
//
// Until #1208 the result was marshaled as the document root, with no namespace and the request ID
// accepted and discarded, so an SDK looking for `{Operation}Response` found nothing and every routed
// Redshift operation was undecodable. The envelope is ELB's (#1149): [elbResponseEnvelope] and
// [elbResultElement] are protocol-generic, deriving both element names from the operation, and a
// second copy of them would be a second place for the shape to drift. The request ID is the
// context's, as [elbOKResponse] explains, so a replayed body is byte-identical to the recorded one.
func redshiftOKResponse(reqCtx *RequestContext, operation string, result any) (*AWSResponse, error) {
	return redshiftXMLResponse(http.StatusOK, elbResponseEnvelope{
		XMLName:          xml.Name{Local: operation + "Response"},
		XMLNS:            redshiftXMLNS,
		Result:           &elbResultElement{name: operation + "Result", value: result},
		ResponseMetadata: responseMetadata{RequestID: reqCtx.RequestID},
	})
}

// redshiftClusterNotFound is the refusal for a cluster identifier that names no cluster.
//
// API_DescribeClusters and API_DescribeClusterSnapshots publish `ClusterNotFound` at HTTP 404, and the
// cluster operations that take an identifier publish the same code. Until #1208 substrate answered
// `ClusterNotFoundFault`, the shape's name in the service model rather than its wire code, which a
// consumer matching on the published code never matched. #1208's text gives the status as 400; the
// pages give 404, which is what this answers.
func redshiftClusterNotFound(id string) *AWSError {
	return &AWSError{Code: "ClusterNotFound", Message: "Cluster " + id + " not found.", HTTPStatus: http.StatusNotFound}
}

// redshiftUnroutedAction refuses an action substrate does not route, with a code from Redshift's own
// Common Errors page.
//
// [unknownActionError] answers the Query family's `InvalidAction`, which Redshift's page does not
// publish: its consolidated list carries no unknown-action code (#1208). SQS keeps `InvalidAction`
// correctly, because SQS's Common Errors page is the one Query-family list AWS has not regenerated and
// still publishes it (#1064); the two pages are different generations and the divergence between them
// is real. So Redshift reads its answer off its own list:
//
//   - no Action at all is `MissingAction`/400, "The request is missing the Action parameter";
//   - an Action naming no operation is `InvalidParameterValue`/400, "A value that you provided for a
//     parameter isn't valid". `Action` is one of the page's Common Parameters, so an unrecognized one
//     is a parameter value that isn't valid. `ValidationError` was the other candidate; it describes
//     input that fails a format or constraint, which a well-formed but unknown name does not.
func redshiftUnroutedAction(action string) *AWSError {
	if action == "" {
		return &AWSError{Code: "MissingAction", Message: "The request is missing the Action parameter.", HTTPStatus: http.StatusBadRequest}
	}
	return &AWSError{
		Code:       "InvalidParameterValue",
		Message:    fmt.Sprintf("The action %s is not valid for this endpoint.", action),
		HTTPStatus: http.StatusBadRequest,
	}
}

// redshiftXMLResponse marshals a response document to XML and returns an AWSResponse with
// Content-Type text/xml as required by the Redshift query protocol.
func redshiftXMLResponse(status int, document any) (*AWSResponse, error) {
	body, err := xml.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("redshift xml.Marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "text/xml; charset=UTF-8"},
		Body:       append([]byte(xml.Header), body...),
	}, nil
}
