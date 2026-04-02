package substrate

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// ElastiCachePlugin emulates the Amazon ElastiCache API using the AWS Query
// protocol. It supports cache clusters, replication groups, subnet groups,
// and parameter groups.
type ElastiCachePlugin struct {
	state  StateManager
	logger Logger
	tc     *TimeController
}

// Name returns the service name "elasticache".
func (p *ElastiCachePlugin) Name() string { return elasticacheNamespace }

// Initialize sets up the ElastiCachePlugin with the provided configuration.
func (p *ElastiCachePlugin) Initialize(_ context.Context, cfg PluginConfig) error {
	p.state = cfg.State
	p.logger = cfg.Logger
	if tc, ok := cfg.Options["time_controller"].(*TimeController); ok {
		p.tc = tc
	} else {
		p.tc = NewTimeController(time.Now())
	}
	return nil
}

// Shutdown is a no-op for ElastiCachePlugin.
func (p *ElastiCachePlugin) Shutdown(_ context.Context) error { return nil }

// HandleRequest dispatches an ElastiCache query-protocol request to the
// appropriate handler.
func (p *ElastiCachePlugin) HandleRequest(ctx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	action := req.Operation
	if action == "" {
		action = req.Params["Action"]
	}
	switch action {
	// Cache Cluster operations.
	case "CreateCacheCluster":
		return p.createCacheCluster(ctx, req)
	case "DescribeCacheClusters":
		return p.describeCacheClusters(ctx, req)
	case "ModifyCacheCluster":
		return p.modifyCacheCluster(ctx, req)
	case "DeleteCacheCluster":
		return p.deleteCacheCluster(ctx, req)
	// Replication Group operations.
	case "CreateReplicationGroup":
		return p.createReplicationGroup(ctx, req)
	case "DescribeReplicationGroups":
		return p.describeReplicationGroups(ctx, req)
	case "ModifyReplicationGroup":
		return p.modifyReplicationGroup(ctx, req)
	case "DeleteReplicationGroup":
		return p.deleteReplicationGroup(ctx, req)
	// Cache Subnet Group operations.
	case "CreateCacheSubnetGroup":
		return p.createCacheSubnetGroup(ctx, req)
	case "DescribeCacheSubnetGroups":
		return p.describeCacheSubnetGroups(ctx, req)
	case "DeleteCacheSubnetGroup":
		return p.deleteCacheSubnetGroup(ctx, req)
	// Cache Parameter Group operations.
	case "CreateCacheParameterGroup":
		return p.createCacheParameterGroup(ctx, req)
	case "DescribeCacheParameterGroups":
		return p.describeCacheParameterGroups(ctx, req)
	case "DeleteCacheParameterGroup":
		return p.deleteCacheParameterGroup(ctx, req)
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
			Message:    "ElastiCachePlugin: unknown action " + action,
			HTTPStatus: http.StatusBadRequest,
		}
	}
}

// --- Cache Cluster operations ---

func (p *ElastiCachePlugin) createCacheCluster(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id := req.Params["CacheClusterId"]
	if id == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "CacheClusterId is required", HTTPStatus: http.StatusBadRequest}
	}
	engine := req.Params["Engine"]
	if engine == "" {
		engine = "redis"
	}
	port := elasticacheDefaultPort(engine)
	numNodes := 1
	if s := req.Params["NumCacheNodes"]; s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			numNodes = n
		}
	}

	cluster := ElastiCacheCacheCluster{
		CacheClusterID:     id,
		CacheNodeType:      req.Params["CacheNodeType"],
		Engine:             engine,
		EngineVersion:      req.Params["EngineVersion"],
		CacheClusterStatus: "available",
		NumCacheNodes:      numNodes,
		CacheClusterARN:    elasticacheClusterARN(reqCtx.Region, reqCtx.AccountID, id),
		ReplicationGroupID: req.Params["ReplicationGroupId"],
		Tags:               elasticacheTagsFromParams(req.Params),
		AccountID:          reqCtx.AccountID,
		Region:             reqCtx.Region,
		CreatedAt:          p.tc.Now(),
	}
	// Memcached uses a configuration endpoint; Redis uses a primary endpoint
	// (omitted here for simplicity — callers read ConfigurationEndpoint).
	cluster.ConfigurationEndpoint = &ElastiCacheEndpoint{
		Address: id + "." + elasticacheClusterHash(id) + ".cfg." + reqCtx.Region + ".cache.amazonaws.com",
		Port:    port,
	}

	data, err := json.Marshal(cluster)
	if err != nil {
		return nil, fmt.Errorf("elasticache createCacheCluster marshal: %w", err)
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	stateKey := "cachecluster:" + scope + "/" + id
	if err := p.state.Put(context.Background(), elasticacheNamespace, stateKey, data); err != nil {
		return nil, fmt.Errorf("elasticache createCacheCluster put: %w", err)
	}
	if err := p.appendToIndex(scope, "cachecluster_ids", id); err != nil {
		return nil, err
	}

	type result struct {
		CacheCluster xmlCacheClusterItem `xml:"CacheCluster"`
	}
	type response struct {
		XMLName xml.Name `xml:"CreateCacheClusterResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result   `xml:"CreateCacheClusterResult"`
	}
	return elasticacheXMLResponse(http.StatusOK, response{
		XMLNS:  elasticacheXMLNS,
		Result: result{CacheCluster: cacheClusterToXML(cluster)},
	})
}

func (p *ElastiCachePlugin) describeCacheClusters(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	filterID := req.Params["CacheClusterId"]

	keys, err := p.state.List(context.Background(), elasticacheNamespace, "cachecluster:"+scope+"/")
	if err != nil {
		return nil, fmt.Errorf("elasticache describeCacheClusters list: %w", err)
	}

	var items []xmlCacheClusterItem
	for _, k := range keys {
		data, getErr := p.state.Get(context.Background(), elasticacheNamespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var cluster ElastiCacheCacheCluster
		if json.Unmarshal(data, &cluster) != nil {
			continue
		}
		if filterID != "" && cluster.CacheClusterID != filterID {
			continue
		}
		items = append(items, cacheClusterToXML(cluster))
	}

	if filterID != "" && len(items) == 0 {
		return nil, &AWSError{
			Code:       "CacheClusterNotFound",
			Message:    "CacheCluster " + filterID + " not found.",
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
		CacheClusters []xmlCacheClusterItem `xml:"CacheClusters>CacheCluster"`
		Marker        string                `xml:"Marker,omitempty"`
	}
	type response struct {
		XMLName xml.Name `xml:"DescribeCacheClustersResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result   `xml:"DescribeCacheClustersResult"`
	}
	return elasticacheXMLResponse(http.StatusOK, response{
		XMLNS: elasticacheXMLNS,
		Result: result{
			CacheClusters: page,
			Marker:        nextMarker,
		},
	})
}

func (p *ElastiCachePlugin) modifyCacheCluster(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id := req.Params["CacheClusterId"]
	if id == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "CacheClusterId is required", HTTPStatus: http.StatusBadRequest}
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	stateKey := "cachecluster:" + scope + "/" + id

	data, err := p.state.Get(context.Background(), elasticacheNamespace, stateKey)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "CacheClusterNotFound", Message: "CacheCluster " + id + " not found.", HTTPStatus: http.StatusNotFound}
	}
	var cluster ElastiCacheCacheCluster
	if err := json.Unmarshal(data, &cluster); err != nil {
		return nil, fmt.Errorf("elasticache modifyCacheCluster unmarshal: %w", err)
	}

	if v := req.Params["CacheNodeType"]; v != "" {
		cluster.CacheNodeType = v
	}
	if v := req.Params["EngineVersion"]; v != "" {
		cluster.EngineVersion = v
	}
	if s := req.Params["NumCacheNodes"]; s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			cluster.NumCacheNodes = n
		}
	}

	updated, _ := json.Marshal(cluster)
	if err := p.state.Put(context.Background(), elasticacheNamespace, stateKey, updated); err != nil {
		return nil, fmt.Errorf("elasticache modifyCacheCluster put: %w", err)
	}

	type result struct {
		CacheCluster xmlCacheClusterItem `xml:"CacheCluster"`
	}
	type response struct {
		XMLName xml.Name `xml:"ModifyCacheClusterResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result   `xml:"ModifyCacheClusterResult"`
	}
	return elasticacheXMLResponse(http.StatusOK, response{
		XMLNS:  elasticacheXMLNS,
		Result: result{CacheCluster: cacheClusterToXML(cluster)},
	})
}

func (p *ElastiCachePlugin) deleteCacheCluster(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id := req.Params["CacheClusterId"]
	if id == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "CacheClusterId is required", HTTPStatus: http.StatusBadRequest}
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	stateKey := "cachecluster:" + scope + "/" + id

	data, err := p.state.Get(context.Background(), elasticacheNamespace, stateKey)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "CacheClusterNotFound", Message: "CacheCluster " + id + " not found.", HTTPStatus: http.StatusNotFound}
	}
	var cluster ElastiCacheCacheCluster
	if json.Unmarshal(data, &cluster) != nil {
		cluster.CacheClusterID = id
	}
	cluster.CacheClusterStatus = "deleting"

	if err := p.state.Delete(context.Background(), elasticacheNamespace, stateKey); err != nil {
		return nil, fmt.Errorf("elasticache deleteCacheCluster delete: %w", err)
	}
	p.removeFromIndex(scope, "cachecluster_ids", id)

	type result struct {
		CacheCluster xmlCacheClusterItem `xml:"CacheCluster"`
	}
	type response struct {
		XMLName xml.Name `xml:"DeleteCacheClusterResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result   `xml:"DeleteCacheClusterResult"`
	}
	return elasticacheXMLResponse(http.StatusOK, response{
		XMLNS:  elasticacheXMLNS,
		Result: result{CacheCluster: cacheClusterToXML(cluster)},
	})
}

// --- Replication Group operations ---

func (p *ElastiCachePlugin) createReplicationGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id := req.Params["ReplicationGroupId"]
	if id == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "ReplicationGroupId is required", HTTPStatus: http.StatusBadRequest}
	}
	autoFailover := req.Params["AutomaticFailoverEnabled"]
	if autoFailover == "true" {
		autoFailover = "enabled"
	} else {
		autoFailover = "disabled"
	}
	multiAZ := req.Params["MultiAZEnabled"]
	if multiAZ == "true" {
		multiAZ = "enabled"
	} else {
		multiAZ = "disabled"
	}

	rg := ElastiCacheReplicationGroup{
		ReplicationGroupID: id,
		Description:        req.Params["ReplicationGroupDescription"],
		Status:             "available",
		AutomaticFailover:  autoFailover,
		MultiAZ:            multiAZ,
		ARN:                elasticacheReplicationGroupARN(reqCtx.Region, reqCtx.AccountID, id),
		Tags:               elasticacheTagsFromParams(req.Params),
		AccountID:          reqCtx.AccountID,
		Region:             reqCtx.Region,
		CreatedAt:          p.tc.Now(),
	}

	data, err := json.Marshal(rg)
	if err != nil {
		return nil, fmt.Errorf("elasticache createReplicationGroup marshal: %w", err)
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	stateKey := "replgroup:" + scope + "/" + id
	if err := p.state.Put(context.Background(), elasticacheNamespace, stateKey, data); err != nil {
		return nil, fmt.Errorf("elasticache createReplicationGroup put: %w", err)
	}
	if err := p.appendToIndex(scope, "replgroup_ids", id); err != nil {
		return nil, err
	}

	type result struct {
		ReplicationGroup xmlReplicationGroupItem `xml:"ReplicationGroup"`
	}
	type response struct {
		XMLName xml.Name `xml:"CreateReplicationGroupResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result   `xml:"CreateReplicationGroupResult"`
	}
	return elasticacheXMLResponse(http.StatusOK, response{
		XMLNS:  elasticacheXMLNS,
		Result: result{ReplicationGroup: replicationGroupToXML(rg)},
	})
}

func (p *ElastiCachePlugin) describeReplicationGroups(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	filterID := req.Params["ReplicationGroupId"]

	keys, err := p.state.List(context.Background(), elasticacheNamespace, "replgroup:"+scope+"/")
	if err != nil {
		return nil, fmt.Errorf("elasticache describeReplicationGroups list: %w", err)
	}

	var items []xmlReplicationGroupItem
	for _, k := range keys {
		data, getErr := p.state.Get(context.Background(), elasticacheNamespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var rg ElastiCacheReplicationGroup
		if json.Unmarshal(data, &rg) != nil {
			continue
		}
		if filterID != "" && rg.ReplicationGroupID != filterID {
			continue
		}
		items = append(items, replicationGroupToXML(rg))
	}

	if filterID != "" && len(items) == 0 {
		return nil, &AWSError{
			Code:       "ReplicationGroupNotFoundFault",
			Message:    "ReplicationGroup " + filterID + " not found.",
			HTTPStatus: http.StatusNotFound,
		}
	}

	type result struct {
		ReplicationGroups []xmlReplicationGroupItem `xml:"ReplicationGroups>ReplicationGroup"`
	}
	type response struct {
		XMLName xml.Name `xml:"DescribeReplicationGroupsResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result   `xml:"DescribeReplicationGroupsResult"`
	}
	return elasticacheXMLResponse(http.StatusOK, response{
		XMLNS:  elasticacheXMLNS,
		Result: result{ReplicationGroups: items},
	})
}

func (p *ElastiCachePlugin) modifyReplicationGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id := req.Params["ReplicationGroupId"]
	if id == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "ReplicationGroupId is required", HTTPStatus: http.StatusBadRequest}
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	stateKey := "replgroup:" + scope + "/" + id

	data, err := p.state.Get(context.Background(), elasticacheNamespace, stateKey)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "ReplicationGroupNotFoundFault", Message: "ReplicationGroup " + id + " not found.", HTTPStatus: http.StatusNotFound}
	}
	var rg ElastiCacheReplicationGroup
	if err := json.Unmarshal(data, &rg); err != nil {
		return nil, fmt.Errorf("elasticache modifyReplicationGroup unmarshal: %w", err)
	}

	if v := req.Params["ReplicationGroupDescription"]; v != "" {
		rg.Description = v
	}
	if v := req.Params["AutomaticFailoverEnabled"]; v != "" {
		if v == "true" {
			rg.AutomaticFailover = "enabled"
		} else {
			rg.AutomaticFailover = "disabled"
		}
	}
	if v := req.Params["MultiAZEnabled"]; v != "" {
		if v == "true" {
			rg.MultiAZ = "enabled"
		} else {
			rg.MultiAZ = "disabled"
		}
	}

	updated, _ := json.Marshal(rg)
	if err := p.state.Put(context.Background(), elasticacheNamespace, stateKey, updated); err != nil {
		return nil, fmt.Errorf("elasticache modifyReplicationGroup put: %w", err)
	}

	type result struct {
		ReplicationGroup xmlReplicationGroupItem `xml:"ReplicationGroup"`
	}
	type response struct {
		XMLName xml.Name `xml:"ModifyReplicationGroupResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result   `xml:"ModifyReplicationGroupResult"`
	}
	return elasticacheXMLResponse(http.StatusOK, response{
		XMLNS:  elasticacheXMLNS,
		Result: result{ReplicationGroup: replicationGroupToXML(rg)},
	})
}

func (p *ElastiCachePlugin) deleteReplicationGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id := req.Params["ReplicationGroupId"]
	if id == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "ReplicationGroupId is required", HTTPStatus: http.StatusBadRequest}
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	stateKey := "replgroup:" + scope + "/" + id

	data, err := p.state.Get(context.Background(), elasticacheNamespace, stateKey)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "ReplicationGroupNotFoundFault", Message: "ReplicationGroup " + id + " not found.", HTTPStatus: http.StatusNotFound}
	}
	var rg ElastiCacheReplicationGroup
	if json.Unmarshal(data, &rg) != nil {
		rg.ReplicationGroupID = id
	}
	rg.Status = "deleting"

	if err := p.state.Delete(context.Background(), elasticacheNamespace, stateKey); err != nil {
		return nil, fmt.Errorf("elasticache deleteReplicationGroup delete: %w", err)
	}
	p.removeFromIndex(scope, "replgroup_ids", id)

	type result struct {
		ReplicationGroup xmlReplicationGroupItem `xml:"ReplicationGroup"`
	}
	type response struct {
		XMLName xml.Name `xml:"DeleteReplicationGroupResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result   `xml:"DeleteReplicationGroupResult"`
	}
	return elasticacheXMLResponse(http.StatusOK, response{
		XMLNS:  elasticacheXMLNS,
		Result: result{ReplicationGroup: replicationGroupToXML(rg)},
	})
}

// --- Cache Subnet Group operations ---

func (p *ElastiCachePlugin) createCacheSubnetGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["CacheSubnetGroupName"]
	if name == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "CacheSubnetGroupName is required", HTTPStatus: http.StatusBadRequest}
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	sg := ElastiCacheCacheSubnetGroup{
		CacheSubnetGroupName:        name,
		CacheSubnetGroupDescription: req.Params["CacheSubnetGroupDescription"],
		VpcID:                       req.Params["VpcId"],
		ARN:                         elasticacheSubnetGroupARN(reqCtx.Region, reqCtx.AccountID, name),
		Tags:                        elasticacheTagsFromParams(req.Params),
		AccountID:                   reqCtx.AccountID,
		Region:                      reqCtx.Region,
	}
	data, err := json.Marshal(sg)
	if err != nil {
		return nil, fmt.Errorf("elasticache createCacheSubnetGroup marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), elasticacheNamespace, "cachesubnetgroup:"+scope+"/"+name, data); err != nil {
		return nil, fmt.Errorf("elasticache createCacheSubnetGroup put: %w", err)
	}
	if err := p.appendToIndex(scope, "cachesubnetgroup_names", name); err != nil {
		return nil, err
	}

	type result struct {
		CacheSubnetGroup xmlCacheSubnetGroupItem `xml:"CacheSubnetGroup"`
	}
	type response struct {
		XMLName xml.Name `xml:"CreateCacheSubnetGroupResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result   `xml:"CreateCacheSubnetGroupResult"`
	}
	return elasticacheXMLResponse(http.StatusOK, response{
		XMLNS:  elasticacheXMLNS,
		Result: result{CacheSubnetGroup: cacheSubnetGroupToXML(sg)},
	})
}

func (p *ElastiCachePlugin) describeCacheSubnetGroups(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	filterName := req.Params["CacheSubnetGroupName"]

	keys, err := p.state.List(context.Background(), elasticacheNamespace, "cachesubnetgroup:"+scope+"/")
	if err != nil {
		return nil, fmt.Errorf("elasticache describeCacheSubnetGroups list: %w", err)
	}

	var items []xmlCacheSubnetGroupItem
	for _, k := range keys {
		data, getErr := p.state.Get(context.Background(), elasticacheNamespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var sg ElastiCacheCacheSubnetGroup
		if json.Unmarshal(data, &sg) != nil {
			continue
		}
		if filterName != "" && sg.CacheSubnetGroupName != filterName {
			continue
		}
		items = append(items, cacheSubnetGroupToXML(sg))
	}

	type result struct {
		CacheSubnetGroups []xmlCacheSubnetGroupItem `xml:"CacheSubnetGroups>CacheSubnetGroup"`
	}
	type response struct {
		XMLName xml.Name `xml:"DescribeCacheSubnetGroupsResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result   `xml:"DescribeCacheSubnetGroupsResult"`
	}
	return elasticacheXMLResponse(http.StatusOK, response{
		XMLNS:  elasticacheXMLNS,
		Result: result{CacheSubnetGroups: items},
	})
}

func (p *ElastiCachePlugin) deleteCacheSubnetGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["CacheSubnetGroupName"]
	if name == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "CacheSubnetGroupName is required", HTTPStatus: http.StatusBadRequest}
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	if err := p.state.Delete(context.Background(), elasticacheNamespace, "cachesubnetgroup:"+scope+"/"+name); err != nil {
		return nil, fmt.Errorf("elasticache deleteCacheSubnetGroup delete: %w", err)
	}
	p.removeFromIndex(scope, "cachesubnetgroup_names", name)

	type response struct {
		XMLName xml.Name `xml:"DeleteCacheSubnetGroupResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
	}
	return elasticacheXMLResponse(http.StatusOK, response{XMLNS: elasticacheXMLNS})
}

// --- Cache Parameter Group operations ---

func (p *ElastiCachePlugin) createCacheParameterGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["CacheParameterGroupName"]
	if name == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "CacheParameterGroupName is required", HTTPStatus: http.StatusBadRequest}
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	pg := ElastiCacheCacheParameterGroup{
		CacheParameterGroupName:   name,
		CacheParameterGroupFamily: req.Params["CacheParameterGroupFamily"],
		Description:               req.Params["Description"],
		ARN:                       elasticacheParameterGroupARN(reqCtx.Region, reqCtx.AccountID, name),
		Tags:                      elasticacheTagsFromParams(req.Params),
		AccountID:                 reqCtx.AccountID,
		Region:                    reqCtx.Region,
	}
	data, err := json.Marshal(pg)
	if err != nil {
		return nil, fmt.Errorf("elasticache createCacheParameterGroup marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), elasticacheNamespace, "cacheparamgroup:"+scope+"/"+name, data); err != nil {
		return nil, fmt.Errorf("elasticache createCacheParameterGroup put: %w", err)
	}
	if err := p.appendToIndex(scope, "cacheparamgroup_names", name); err != nil {
		return nil, err
	}

	type result struct {
		CacheParameterGroup xmlCacheParamGroupItem `xml:"CacheParameterGroup"`
	}
	type response struct {
		XMLName xml.Name `xml:"CreateCacheParameterGroupResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result   `xml:"CreateCacheParameterGroupResult"`
	}
	return elasticacheXMLResponse(http.StatusOK, response{
		XMLNS:  elasticacheXMLNS,
		Result: result{CacheParameterGroup: cacheParamGroupToXML(pg)},
	})
}

func (p *ElastiCachePlugin) describeCacheParameterGroups(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	filterName := req.Params["CacheParameterGroupName"]

	keys, err := p.state.List(context.Background(), elasticacheNamespace, "cacheparamgroup:"+scope+"/")
	if err != nil {
		return nil, fmt.Errorf("elasticache describeCacheParameterGroups list: %w", err)
	}

	var items []xmlCacheParamGroupItem
	for _, k := range keys {
		data, getErr := p.state.Get(context.Background(), elasticacheNamespace, k)
		if getErr != nil || data == nil {
			continue
		}
		var pg ElastiCacheCacheParameterGroup
		if json.Unmarshal(data, &pg) != nil {
			continue
		}
		if filterName != "" && pg.CacheParameterGroupName != filterName {
			continue
		}
		items = append(items, cacheParamGroupToXML(pg))
	}

	type result struct {
		CacheParameterGroups []xmlCacheParamGroupItem `xml:"CacheParameterGroups>CacheParameterGroup"`
	}
	type response struct {
		XMLName xml.Name `xml:"DescribeCacheParameterGroupsResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
		Result  result   `xml:"DescribeCacheParameterGroupsResult"`
	}
	return elasticacheXMLResponse(http.StatusOK, response{
		XMLNS:  elasticacheXMLNS,
		Result: result{CacheParameterGroups: items},
	})
}

func (p *ElastiCachePlugin) deleteCacheParameterGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["CacheParameterGroupName"]
	if name == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "CacheParameterGroupName is required", HTTPStatus: http.StatusBadRequest}
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	if err := p.state.Delete(context.Background(), elasticacheNamespace, "cacheparamgroup:"+scope+"/"+name); err != nil {
		return nil, fmt.Errorf("elasticache deleteCacheParameterGroup delete: %w", err)
	}
	p.removeFromIndex(scope, "cacheparamgroup_names", name)

	type response struct {
		XMLName xml.Name `xml:"DeleteCacheParameterGroupResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
	}
	return elasticacheXMLResponse(http.StatusOK, response{XMLNS: elasticacheXMLNS})
}

// --- Tagging operations ---

func (p *ElastiCachePlugin) listTagsForResource(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
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
	return elasticacheXMLResponse(http.StatusOK, response{
		XMLNS:  elasticacheXMLNS,
		Result: result{TagList: xmlTags},
	})
}

func (p *ElastiCachePlugin) addTagsToResource(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	resourceARN := req.Params["ResourceName"]
	if resourceARN == "" {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: "ResourceName is required", HTTPStatus: http.StatusBadRequest}
	}
	newTags := elasticacheTagsFromParams(req.Params)
	if err := p.updateTagsByARN(resourceARN, newTags, nil); err != nil {
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"AddTagsToResourceResponse"`
		XMLNS   string   `xml:"xmlns,attr"`
	}
	return elasticacheXMLResponse(http.StatusOK, response{XMLNS: elasticacheXMLNS})
}

func (p *ElastiCachePlugin) removeTagsFromResource(_ *RequestContext, req *AWSRequest) (*AWSResponse, error) {
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
	return elasticacheXMLResponse(http.StatusOK, response{XMLNS: elasticacheXMLNS})
}

// loadTagsByARN resolves an ElastiCache ARN and returns the resource's tags.
func (p *ElastiCachePlugin) loadTagsByARN(arn string) (map[string]string, error) {
	_, key, err := elasticacheResolveARN(arn)
	if err != nil {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	data, err := p.state.Get(context.Background(), elasticacheNamespace, key)
	if err != nil || data == nil {
		return nil, &AWSError{Code: "CacheClusterNotFound", Message: "Resource not found: " + arn, HTTPStatus: http.StatusNotFound}
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
func (p *ElastiCachePlugin) updateTagsByARN(arn string, add map[string]string, removeKeys []string) error {
	_, key, err := elasticacheResolveARN(arn)
	if err != nil {
		return &AWSError{Code: "InvalidParameterValue", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	data, err := p.state.Get(context.Background(), elasticacheNamespace, key)
	if err != nil || data == nil {
		return &AWSError{Code: "CacheClusterNotFound", Message: "Resource not found: " + arn, HTTPStatus: http.StatusNotFound}
	}

	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("elasticache updateTagsByARN unmarshal: %w", err)
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
	return p.state.Put(context.Background(), elasticacheNamespace, key, updated)
}

// --- XML types ---

// xmlEndpointItem is the XML representation of an ElastiCache endpoint.
type xmlEndpointItem struct {
	Address string `xml:"Address"`
	Port    int    `xml:"Port"`
}

// xmlCacheClusterItem is the XML representation of an ElastiCache cache cluster.
type xmlCacheClusterItem struct {
	CacheClusterID        string           `xml:"CacheClusterId"`
	CacheNodeType         string           `xml:"CacheNodeType"`
	Engine                string           `xml:"Engine"`
	EngineVersion         string           `xml:"EngineVersion"`
	CacheClusterStatus    string           `xml:"CacheClusterStatus"`
	NumCacheNodes         int              `xml:"NumCacheNodes"`
	ARN                   string           `xml:"ARN"`
	ReplicationGroupID    string           `xml:"ReplicationGroupId,omitempty"`
	ConfigurationEndpoint *xmlEndpointItem `xml:"ConfigurationEndpoint,omitempty"`
}

// xmlReplicationGroupItem is the XML representation of an ElastiCache replication group.
type xmlReplicationGroupItem struct {
	ReplicationGroupID string `xml:"ReplicationGroupId"`
	Description        string `xml:"Description"`
	Status             string `xml:"Status"`
	AutomaticFailover  string `xml:"AutomaticFailover"`
	MultiAZ            string `xml:"MultiAZ"`
	ARN                string `xml:"ARN"`
}

// xmlCacheSubnetGroupItem is the XML representation of an ElastiCache cache subnet group.
type xmlCacheSubnetGroupItem struct {
	CacheSubnetGroupName        string `xml:"CacheSubnetGroupName"`
	CacheSubnetGroupDescription string `xml:"CacheSubnetGroupDescription"`
	VpcID                       string `xml:"VpcId"`
	ARN                         string `xml:"ARN"`
}

// xmlCacheParamGroupItem is the XML representation of an ElastiCache cache parameter group.
type xmlCacheParamGroupItem struct {
	CacheParameterGroupName   string `xml:"CacheParameterGroupName"`
	CacheParameterGroupFamily string `xml:"CacheParameterGroupFamily"`
	Description               string `xml:"Description"`
	ARN                       string `xml:"ARN"`
}

func cacheClusterToXML(c ElastiCacheCacheCluster) xmlCacheClusterItem {
	item := xmlCacheClusterItem{
		CacheClusterID:     c.CacheClusterID,
		CacheNodeType:      c.CacheNodeType,
		Engine:             c.Engine,
		EngineVersion:      c.EngineVersion,
		CacheClusterStatus: c.CacheClusterStatus,
		NumCacheNodes:      c.NumCacheNodes,
		ARN:                c.CacheClusterARN,
		ReplicationGroupID: c.ReplicationGroupID,
	}
	if c.ConfigurationEndpoint != nil {
		item.ConfigurationEndpoint = &xmlEndpointItem{
			Address: c.ConfigurationEndpoint.Address,
			Port:    c.ConfigurationEndpoint.Port,
		}
	}
	return item
}

func replicationGroupToXML(rg ElastiCacheReplicationGroup) xmlReplicationGroupItem {
	return xmlReplicationGroupItem{
		ReplicationGroupID: rg.ReplicationGroupID,
		Description:        rg.Description,
		Status:             rg.Status,
		AutomaticFailover:  rg.AutomaticFailover,
		MultiAZ:            rg.MultiAZ,
		ARN:                rg.ARN,
	}
}

func cacheSubnetGroupToXML(sg ElastiCacheCacheSubnetGroup) xmlCacheSubnetGroupItem {
	return xmlCacheSubnetGroupItem{
		CacheSubnetGroupName:        sg.CacheSubnetGroupName,
		CacheSubnetGroupDescription: sg.CacheSubnetGroupDescription,
		VpcID:                       sg.VpcID,
		ARN:                         sg.ARN,
	}
}

func cacheParamGroupToXML(pg ElastiCacheCacheParameterGroup) xmlCacheParamGroupItem {
	return xmlCacheParamGroupItem{
		CacheParameterGroupName:   pg.CacheParameterGroupName,
		CacheParameterGroupFamily: pg.CacheParameterGroupFamily,
		Description:               pg.Description,
		ARN:                       pg.ARN,
	}
}

// --- State helpers ---

func (p *ElastiCachePlugin) appendToIndex(scope, indexName, id string) error {
	key := indexName + ":" + scope
	data, err := p.state.Get(context.Background(), elasticacheNamespace, key)
	if err != nil {
		return fmt.Errorf("elasticache appendToIndex get %s: %w", key, err)
	}
	var ids []string
	if data != nil {
		_ = json.Unmarshal(data, &ids)
	}
	ids = append(ids, id)
	newData, _ := json.Marshal(ids)
	return p.state.Put(context.Background(), elasticacheNamespace, key, newData)
}

func (p *ElastiCachePlugin) removeFromIndex(scope, indexName, id string) {
	key := indexName + ":" + scope
	data, err := p.state.Get(context.Background(), elasticacheNamespace, key)
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
	_ = p.state.Put(context.Background(), elasticacheNamespace, key, newData)
}

// --- ARN and endpoint helpers ---

// elasticacheClusterHash returns a deterministic 6-char lowercase hex string
// derived from the cluster ID. AWS embeds a similar short hash in the real
// endpoint address format: {id}.{hash}.cfg.{region}.cache.amazonaws.com.
func elasticacheClusterHash(id string) string {
	var h uint32
	for _, b := range []byte(id) {
		h = h*31 + uint32(b)
	}
	return fmt.Sprintf("%06x", h&0xFFFFFF)
}

func elasticacheClusterARN(region, acct, id string) string {
	return "arn:aws:elasticache:" + region + ":" + acct + ":cluster:" + id
}

func elasticacheReplicationGroupARN(region, acct, id string) string {
	return "arn:aws:elasticache:" + region + ":" + acct + ":replicationgroup:" + id
}

func elasticacheSubnetGroupARN(region, acct, name string) string {
	return "arn:aws:elasticache:" + region + ":" + acct + ":subnetgroup:" + name
}

func elasticacheParameterGroupARN(region, acct, name string) string {
	return "arn:aws:elasticache:" + region + ":" + acct + ":parametergroup:" + name
}

// elasticacheResolveARN parses an ElastiCache ARN and returns (namespace, stateKey).
func elasticacheResolveARN(arn string) (ns, key string, err error) {
	// arn:aws:elasticache:{region}:{acct}:{type}:{id}
	parts := splitARN(arn)
	if len(parts) < 7 || parts[2] != "elasticache" {
		return "", "", fmt.Errorf("invalid ElastiCache ARN: %q", arn)
	}
	region := parts[3]
	acct := parts[4]
	resType := parts[5]
	resID := parts[6]
	scope := acct + "/" + region

	switch resType {
	case "cluster":
		return elasticacheNamespace, "cachecluster:" + scope + "/" + resID, nil
	case "replicationgroup":
		return elasticacheNamespace, "replgroup:" + scope + "/" + resID, nil
	default:
		return "", "", fmt.Errorf("unsupported ElastiCache ARN resource type: %q", resType)
	}
}

// splitARN splits an ARN on ":" returning up to 7 parts. The last element
// (resource ID) may itself contain colons in some services; for ElastiCache
// the format is always arn:aws:elasticache:{r}:{a}:{type}:{id}.
func splitARN(arn string) []string {
	const maxParts = 7
	out := make([]string, 0, maxParts)
	rest := arn
	for i := 0; i < maxParts-1; i++ {
		idx := -1
		for j := 0; j < len(rest); j++ {
			if rest[j] == ':' {
				idx = j
				break
			}
		}
		if idx < 0 {
			break
		}
		out = append(out, rest[:idx])
		rest = rest[idx+1:]
	}
	out = append(out, rest)
	return out
}

// elasticacheTagsFromParams extracts Tags.member.N.Key/Value pairs from query params.
func elasticacheTagsFromParams(params map[string]string) map[string]string {
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

// elasticacheXMLResponse serializes v to XML and wraps it in an AWSResponse.
func elasticacheXMLResponse(status int, v interface{}) (*AWSResponse, error) {
	body, err := xml.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("elasticache xml marshal: %w", err)
	}
	return &AWSResponse{
		StatusCode: status,
		Headers:    map[string]string{"Content-Type": "text/xml; charset=UTF-8"},
		Body:       append([]byte(xml.Header), body...),
	}, nil
}
