package emulator

import (
	"context"
	"encoding/json"
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
		return nil, unknownActionError(p.Name(), action)
	}
}

// --- Cache Cluster operations ---

func (p *ElastiCachePlugin) createCacheCluster(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id := req.Params["CacheClusterId"]
	if id == "" {
		return nil, elasticacheMissing("CreateCacheCluster", req, "CacheClusterId")
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
	return elasticacheOKResponse(reqCtx, "CreateCacheCluster", result{CacheCluster: cacheClusterToXML(cluster)})
}

func (p *ElastiCachePlugin) describeCacheClusters(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	filterID := req.Params["CacheClusterId"]

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

	prefix := "cachecluster:" + scope + "/"
	keys, err := p.state.List(context.Background(), elasticacheNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("elasticache describeCacheClusters list: %w", err)
	}

	page, nextMarker := queryMarkerPage(keys, prefix, cursor, maxRecords,
		func(key, _ string) (xmlCacheClusterItem, bool) {
			data, getErr := p.state.Get(context.Background(), elasticacheNamespace, key)
			if getErr != nil || data == nil {
				return xmlCacheClusterItem{}, false
			}
			var cluster ElastiCacheCacheCluster
			if json.Unmarshal(data, &cluster) != nil {
				return xmlCacheClusterItem{}, false
			}
			if filterID != "" && cluster.CacheClusterID != filterID {
				return xmlCacheClusterItem{}, false
			}
			return cacheClusterToXML(cluster), true
		})

	if filterID != "" && len(page) == 0 {
		return nil, &AWSError{
			Code:       "CacheClusterNotFound",
			Message:    "CacheCluster " + filterID + " not found.",
			HTTPStatus: http.StatusNotFound,
		}
	}

	type result struct {
		CacheClusters []xmlCacheClusterItem `xml:"CacheClusters>CacheCluster"`
		Marker        string                `xml:"Marker,omitempty"`
	}
	return elasticacheOKResponse(reqCtx, "DescribeCacheClusters", result{
		CacheClusters: page,
		Marker:        nextMarker,
	})
}

func (p *ElastiCachePlugin) modifyCacheCluster(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id := req.Params["CacheClusterId"]
	if id == "" {
		return nil, elasticacheMissing("ModifyCacheCluster", req, "CacheClusterId")
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

	updated, err := json.Marshal(cluster)
	if err != nil {
		return nil, fmt.Errorf("elasticache modifyCacheCluster marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), elasticacheNamespace, stateKey, updated); err != nil {
		return nil, fmt.Errorf("elasticache modifyCacheCluster put: %w", err)
	}

	type result struct {
		CacheCluster xmlCacheClusterItem `xml:"CacheCluster"`
	}
	return elasticacheOKResponse(reqCtx, "ModifyCacheCluster", result{CacheCluster: cacheClusterToXML(cluster)})
}

func (p *ElastiCachePlugin) deleteCacheCluster(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id := req.Params["CacheClusterId"]
	if id == "" {
		return nil, elasticacheMissing("DeleteCacheCluster", req, "CacheClusterId")
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
	if err := p.removeFromIndex(scope, "cachecluster_ids", id); err != nil {
		return nil, fmt.Errorf("elasticache deleteCacheCluster removeFromIndex: %w", err)
	}

	type result struct {
		CacheCluster xmlCacheClusterItem `xml:"CacheCluster"`
	}
	return elasticacheOKResponse(reqCtx, "DeleteCacheCluster", result{CacheCluster: cacheClusterToXML(cluster)})
}

// --- Replication Group operations ---

func (p *ElastiCachePlugin) createReplicationGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id := req.Params["ReplicationGroupId"]
	if id == "" {
		return nil, elasticacheMissing("CreateReplicationGroup", req, "ReplicationGroupId")
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
	return elasticacheOKResponse(reqCtx, "CreateReplicationGroup", result{ReplicationGroup: replicationGroupToXML(rg)})
}

func (p *ElastiCachePlugin) describeReplicationGroups(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	filterID := req.Params["ReplicationGroupId"]

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

	prefix := "replgroup:" + scope + "/"
	keys, err := p.state.List(context.Background(), elasticacheNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("elasticache describeReplicationGroups list: %w", err)
	}

	page, nextMarker := queryMarkerPage(keys, prefix, cursor, maxRecords,
		func(key, _ string) (xmlReplicationGroupItem, bool) {
			data, getErr := p.state.Get(context.Background(), elasticacheNamespace, key)
			if getErr != nil || data == nil {
				return xmlReplicationGroupItem{}, false
			}
			var rg ElastiCacheReplicationGroup
			if json.Unmarshal(data, &rg) != nil {
				return xmlReplicationGroupItem{}, false
			}
			if filterID != "" && rg.ReplicationGroupID != filterID {
				return xmlReplicationGroupItem{}, false
			}
			return replicationGroupToXML(rg), true
		})

	// API_DescribeReplicationGroups publishes ReplicationGroupNotFoundFault/404. This was the only
	// one of the six describes to answer its published fault until #1020 gave the other five theirs;
	// it now goes through the shared [queryMarkerFilterNotFound], which carries the argument that
	// pagination cannot make the fault fire for a group that exists, and which keeps six operations
	// from answering one condition six ways.
	if fault := queryMarkerFilterNotFound(filterID, len(page) > 0, queryMarkerFault{
		Code:       "ReplicationGroupNotFoundFault",
		Kind:       "ReplicationGroup",
		HTTPStatus: http.StatusNotFound,
	}); fault != nil {
		return nil, fault
	}

	type result struct {
		ReplicationGroups []xmlReplicationGroupItem `xml:"ReplicationGroups>ReplicationGroup"`
		Marker            string                    `xml:"Marker,omitempty"`
	}
	return elasticacheOKResponse(reqCtx, "DescribeReplicationGroups", result{
		ReplicationGroups: page,
		Marker:            nextMarker,
	})
}

func (p *ElastiCachePlugin) modifyReplicationGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id := req.Params["ReplicationGroupId"]
	if id == "" {
		return nil, elasticacheMissing("ModifyReplicationGroup", req, "ReplicationGroupId")
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

	updated, err := json.Marshal(rg)
	if err != nil {
		return nil, fmt.Errorf("elasticache modifyReplicationGroup marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), elasticacheNamespace, stateKey, updated); err != nil {
		return nil, fmt.Errorf("elasticache modifyReplicationGroup put: %w", err)
	}

	type result struct {
		ReplicationGroup xmlReplicationGroupItem `xml:"ReplicationGroup"`
	}
	return elasticacheOKResponse(reqCtx, "ModifyReplicationGroup", result{ReplicationGroup: replicationGroupToXML(rg)})
}

func (p *ElastiCachePlugin) deleteReplicationGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	id := req.Params["ReplicationGroupId"]
	if id == "" {
		return nil, elasticacheMissing("DeleteReplicationGroup", req, "ReplicationGroupId")
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
	if err := p.removeFromIndex(scope, "replgroup_ids", id); err != nil {
		return nil, fmt.Errorf("elasticache deleteReplicationGroup removeFromIndex: %w", err)
	}

	type result struct {
		ReplicationGroup xmlReplicationGroupItem `xml:"ReplicationGroup"`
	}
	return elasticacheOKResponse(reqCtx, "DeleteReplicationGroup", result{ReplicationGroup: replicationGroupToXML(rg)})
}

// --- Cache Subnet Group operations ---

func (p *ElastiCachePlugin) createCacheSubnetGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["CacheSubnetGroupName"]
	if name == "" {
		return nil, elasticacheMissing("CreateCacheSubnetGroup", req, "CacheSubnetGroupName")
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
	return elasticacheOKResponse(reqCtx, "CreateCacheSubnetGroup", result{CacheSubnetGroup: cacheSubnetGroupToXML(sg)})
}

func (p *ElastiCachePlugin) describeCacheSubnetGroups(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	filterName := req.Params["CacheSubnetGroupName"]

	cursor, cursorErr := parseQueryMarker(req.Params["Marker"])
	if cursorErr != nil {
		return nil, cursorErr
	}
	maxRecords, maxErr := queryMaxRecords(req.Params["MaxRecords"])
	if maxErr != nil {
		return nil, maxErr
	}

	prefix := "cachesubnetgroup:" + scope + "/"
	keys, err := p.state.List(context.Background(), elasticacheNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("elasticache describeCacheSubnetGroups list: %w", err)
	}

	page, nextMarker := queryMarkerPage(keys, prefix, cursor, maxRecords,
		func(key, _ string) (xmlCacheSubnetGroupItem, bool) {
			data, getErr := p.state.Get(context.Background(), elasticacheNamespace, key)
			if getErr != nil || data == nil {
				return xmlCacheSubnetGroupItem{}, false
			}
			var sg ElastiCacheCacheSubnetGroup
			if json.Unmarshal(data, &sg) != nil {
				return xmlCacheSubnetGroupItem{}, false
			}
			if filterName != "" && sg.CacheSubnetGroupName != filterName {
				return xmlCacheSubnetGroupItem{}, false
			}
			return cacheSubnetGroupToXML(sg), true
		})

	// API_DescribeCacheSubnetGroups publishes CacheSubnetGroupNotFoundFault as its only error, and
	// at **400** rather than the 404 the other four #1020 sites use — the one status outlier among
	// the six, which is why [queryMarkerFault] carries a status at all. This is the same page whose
	// silence about a malformed cursor [parseQueryMarker] records: it publishes this fault and
	// nothing else.
	if fault := queryMarkerFilterNotFound(filterName, len(page) > 0, queryMarkerFault{
		Code:       "CacheSubnetGroupNotFoundFault",
		Kind:       "CacheSubnetGroup",
		HTTPStatus: http.StatusBadRequest,
	}); fault != nil {
		return nil, fault
	}

	type result struct {
		CacheSubnetGroups []xmlCacheSubnetGroupItem `xml:"CacheSubnetGroups>CacheSubnetGroup"`
		Marker            string                    `xml:"Marker,omitempty"`
	}
	return elasticacheOKResponse(reqCtx, "DescribeCacheSubnetGroups", result{
		CacheSubnetGroups: page,
		Marker:            nextMarker,
	})
}

func (p *ElastiCachePlugin) deleteCacheSubnetGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["CacheSubnetGroupName"]
	if name == "" {
		return nil, elasticacheMissing("DeleteCacheSubnetGroup", req, "CacheSubnetGroupName")
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	if err := p.state.Delete(context.Background(), elasticacheNamespace, "cachesubnetgroup:"+scope+"/"+name); err != nil {
		return nil, fmt.Errorf("elasticache deleteCacheSubnetGroup delete: %w", err)
	}
	if err := p.removeFromIndex(scope, "cachesubnetgroup_names", name); err != nil {
		return nil, fmt.Errorf("elasticache deleteCacheSubnetGroup removeFromIndex: %w", err)
	}

	return elasticacheOKResponse(reqCtx, "DeleteCacheSubnetGroup", nil)
}

// --- Cache Parameter Group operations ---

func (p *ElastiCachePlugin) createCacheParameterGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["CacheParameterGroupName"]
	if name == "" {
		return nil, elasticacheMissing("CreateCacheParameterGroup", req, "CacheParameterGroupName")
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
		CacheParameterGroup xmlCacheParameterGroupItem `xml:"CacheParameterGroup"`
	}
	return elasticacheOKResponse(reqCtx, "CreateCacheParameterGroup", result{CacheParameterGroup: cacheParameterGroupToXML(pg)})
}

func (p *ElastiCachePlugin) describeCacheParameterGroups(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	filterName := req.Params["CacheParameterGroupName"]

	cursor, cursorErr := parseQueryMarker(req.Params["Marker"])
	if cursorErr != nil {
		return nil, cursorErr
	}
	maxRecords, maxErr := queryMaxRecords(req.Params["MaxRecords"])
	if maxErr != nil {
		return nil, maxErr
	}

	prefix := "cacheparamgroup:" + scope + "/"
	keys, err := p.state.List(context.Background(), elasticacheNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("elasticache describeCacheParameterGroups list: %w", err)
	}

	page, nextMarker := queryMarkerPage(keys, prefix, cursor, maxRecords,
		func(key, _ string) (xmlCacheParameterGroupItem, bool) {
			data, getErr := p.state.Get(context.Background(), elasticacheNamespace, key)
			if getErr != nil || data == nil {
				return xmlCacheParameterGroupItem{}, false
			}
			var pg ElastiCacheCacheParameterGroup
			if json.Unmarshal(data, &pg) != nil {
				return xmlCacheParameterGroupItem{}, false
			}
			if filterName != "" && pg.CacheParameterGroupName != filterName {
				return xmlCacheParameterGroupItem{}, false
			}
			return cacheParameterGroupToXML(pg), true
		})

	// API_DescribeCacheParameterGroups publishes CacheParameterGroupNotFound/404, glossed "The
	// requested cache parameter group name does not refer to an existing cache parameter group"
	// (#1020). It also publishes InvalidParameterValue/400, which is the code
	// [parseQueryMarker] and [queryMaxRecords] already answer here for an unusable cursor — so this
	// operation is one of the three whose refusals are all sourced from its own page.
	if fault := queryMarkerFilterNotFound(filterName, len(page) > 0, queryMarkerFault{
		Code:       "CacheParameterGroupNotFound",
		Kind:       "CacheParameterGroup",
		HTTPStatus: http.StatusNotFound,
	}); fault != nil {
		return nil, fault
	}

	type result struct {
		CacheParameterGroups []xmlCacheParameterGroupItem `xml:"CacheParameterGroups>CacheParameterGroup"`
		Marker               string                       `xml:"Marker,omitempty"`
	}
	return elasticacheOKResponse(reqCtx, "DescribeCacheParameterGroups", result{
		CacheParameterGroups: page,
		Marker:               nextMarker,
	})
}

func (p *ElastiCachePlugin) deleteCacheParameterGroup(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	name := req.Params["CacheParameterGroupName"]
	if name == "" {
		return nil, elasticacheMissing("DeleteCacheParameterGroup", req, "CacheParameterGroupName")
	}
	scope := reqCtx.AccountID + "/" + reqCtx.Region
	if err := p.state.Delete(context.Background(), elasticacheNamespace, "cacheparamgroup:"+scope+"/"+name); err != nil {
		return nil, fmt.Errorf("elasticache deleteCacheParameterGroup delete: %w", err)
	}
	if err := p.removeFromIndex(scope, "cacheparamgroup_names", name); err != nil {
		return nil, fmt.Errorf("elasticache deleteCacheParameterGroup removeFromIndex: %w", err)
	}

	return elasticacheOKResponse(reqCtx, "DeleteCacheParameterGroup", nil)
}

// --- Tagging operations ---

func (p *ElastiCachePlugin) listTagsForResource(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	resourceARN := req.Params["ResourceName"]
	if resourceARN == "" {
		return nil, elasticacheMissing("ListTagsForResource", req, "ResourceName")
	}
	tags, err := p.loadTagsByARN(resourceARN)
	if err != nil {
		return nil, err
	}
	return elasticacheOKResponse(reqCtx, "ListTagsForResource", elasticacheTagListResult(tags))
}

// elasticacheTagListXML is one member of a `TagListMessage`'s `TagList`.
type elasticacheTagListXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

// elasticacheTagListMessage is the published `TagListMessage`: the output of ListTagsForResource,
// AddTagsToResource and RemoveTagsFromResource alike.
type elasticacheTagListMessage struct {
	TagList []elasticacheTagListXML `xml:"TagList>Tag"`
}

// elasticacheTagListResult renders a resource's tags as a `TagListMessage`.
//
// Ranging the map put Go's map order into TagList, so two identical calls could report one
// cluster's tags in a different order (#1011). This is the site #946 missed when it swept the
// per-service tag listings: ElastiCache stores its tags as a map[string]string like Kinesis and
// S3 do, but it renders them through XML rather than JSON, so it fell outside that sweep's shape.
//
// API_ListTagsForResource describes TagList.Tag.N only as "A list of tags as key-value pairs" and
// publishes no cursor, so unlike Kinesis's ExclusiveStartTagKey there is nothing here implying an
// order. Lexicographic by key is substrate's reading, resting on the replay promise — see
// docs/reference/elasticache.md.
func elasticacheTagListResult(tags map[string]string) elasticacheTagListMessage {
	xmlTags := make([]elasticacheTagListXML, 0, len(tags))
	for k, v := range tags {
		xmlTags = append(xmlTags, elasticacheTagListXML{Key: k, Value: v})
	}
	sortTagsByKey(xmlTags, func(t elasticacheTagListXML) string { return t.Key })
	return elasticacheTagListMessage{TagList: xmlTags}
}

// addTagsToResource merges the request's tags into the resource's and answers the resulting list.
//
// API_AddTagsToResource publishes one response element, `TagList.Tag.N` — the output shape is
// `TagListMessage`, the same as ListTagsForResource's — so unlike RDS's operation of the same name,
// whose sample response carries no result element, ElastiCache's answers the resource's tags after
// the change. Until #1242 it answered an empty document.
func (p *ElastiCachePlugin) addTagsToResource(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	resourceARN := req.Params["ResourceName"]
	if resourceARN == "" {
		return nil, elasticacheMissing("AddTagsToResource", req, "ResourceName")
	}
	newTags := elasticacheTagsFromParams(req.Params)
	tags, err := p.updateTagsByARN(resourceARN, newTags, nil)
	if err != nil {
		return nil, err
	}
	return elasticacheOKResponse(reqCtx, "AddTagsToResource", elasticacheTagListResult(tags))
}

// removeTagsFromResource removes the named keys and answers the resource's remaining tags, which is
// what API_RemoveTagsFromResource's one response element, `TagList.Tag.N`, publishes.
func (p *ElastiCachePlugin) removeTagsFromResource(reqCtx *RequestContext, req *AWSRequest) (*AWSResponse, error) {
	resourceARN := req.Params["ResourceName"]
	if resourceARN == "" {
		return nil, elasticacheMissing("RemoveTagsFromResource", req, "ResourceName")
	}
	keys := extractIndexedParams(req.Params, "TagKeys.member")
	tags, err := p.updateTagsByARN(resourceARN, nil, keys)
	if err != nil {
		return nil, err
	}
	return elasticacheOKResponse(reqCtx, "RemoveTagsFromResource", elasticacheTagListResult(tags))
}

// loadTagsByARN resolves an ElastiCache ARN and returns the resource's tags.
func (p *ElastiCachePlugin) loadTagsByARN(arn string) (map[string]string, error) {
	_, key, err := elasticacheResolveARN(arn)
	if err != nil {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	data, err := p.state.Get(context.Background(), elasticacheNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("elasticache loadTagsByARN get %s: %w", key, err)
	}
	if data == nil {
		return nil, &AWSError{Code: "CacheClusterNotFound", Message: "Resource not found: " + arn, HTTPStatus: http.StatusNotFound}
	}
	var res struct {
		Tags map[string]string `json:"Tags"`
	}
	// Reported rather than swallowed, as RDS's twin does: a store fault answered 404 and a record that
	// would not decode answered an empty TagList, each indistinguishable from a real answer.
	if err := json.Unmarshal(data, &res); err != nil {
		return nil, fmt.Errorf("elasticache loadTagsByARN unmarshal %s: %w", key, err)
	}
	return res.Tags, nil
}

// updateTagsByARN merges or removes tags on the resource identified by arn and returns the tag set
// it wrote.
//
// The set is returned rather than re-read so a tag mutation's TagList is the one this request wrote:
// a read after the Put could report tags another request wrote in between (#1242).
func (p *ElastiCachePlugin) updateTagsByARN(arn string, add map[string]string, removeKeys []string) (map[string]string, error) {
	_, key, err := elasticacheResolveARN(arn)
	if err != nil {
		return nil, &AWSError{Code: "InvalidParameterValue", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	data, err := p.state.Get(context.Background(), elasticacheNamespace, key)
	if err != nil {
		return nil, fmt.Errorf("elasticache updateTagsByARN get %s: %w", key, err)
	}
	if data == nil {
		return nil, &AWSError{Code: "CacheClusterNotFound", Message: "Resource not found: " + arn, HTTPStatus: http.StatusNotFound}
	}

	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("elasticache updateTagsByARN unmarshal: %w", err)
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
	if err := taggingStampRecordAnyEverTagged(m, len(existingTags), len(add)); err != nil {
		return nil, fmt.Errorf("elasticache updateTagsByARN: %w", err)
	}
	merged := mergeStringMap(existingTags, add, removeKeys)
	m["Tags"] = merged
	updated, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("elasticache updateTagsByARN marshal: %w", err)
	}
	if err := p.state.Put(context.Background(), elasticacheNamespace, key, updated); err != nil {
		return nil, fmt.Errorf("elasticache updateTagsByARN put %s: %w", key, err)
	}
	return merged, nil
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
	newData, err := json.Marshal(ids)
	if err != nil {
		return fmt.Errorf("elasticache appendToIndex marshal: %w", err)
	}
	return p.state.Put(context.Background(), elasticacheNamespace, key, newData)
}

func (p *ElastiCachePlugin) removeFromIndex(scope, indexName, id string) error {
	key := indexName + ":" + scope
	data, err := p.state.Get(context.Background(), elasticacheNamespace, key)
	if err != nil {
		return fmt.Errorf("elasticache removeFromIndex %s state.Get: %w", key, err)
	}
	if data == nil {
		return nil
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		return fmt.Errorf("elasticache removeFromIndex %s unmarshal: %w", key, err)
	}
	filtered := ids[:0]
	for _, v := range ids {
		if v != id {
			filtered = append(filtered, v)
		}
	}
	newData, err := json.Marshal(filtered)
	if err != nil {
		return fmt.Errorf("elasticache removeFromIndex %s marshal: %w", key, err)
	}
	if err := p.state.Put(context.Background(), elasticacheNamespace, key, newData); err != nil {
		return fmt.Errorf("elasticache removeFromIndex %s state.Put: %w", key, err)
	}
	return nil
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

// elasticacheOKResponse answers an ElastiCache operation in the awsQuery success document, under
// [elasticacheXMLNS] with the published `ResponseMetadata` carrying the request's own ID (#1242).
//
// result is the operation's result struct. Pass nil for an operation whose page publishes no result
// element — `DeleteCacheSubnetGroup` and `DeleteCacheParameterGroup`, whose sample responses are the
// root and the metadata alone; see `query_envelope.go` for why that is not the same as an empty one.
func elasticacheOKResponse(reqCtx *RequestContext, operation string, result any) (*AWSResponse, error) {
	return queryOKResponse(reqCtx, operation, elasticacheXMLNS, result)
}

// elasticacheMissing refuses a request whose required parameter param is absent or blank, in
// ElastiCache's own sentence (see validatedServices), through [wireRequireMember]. Request validation
// answers an absent one for a client's request first; a blank one, and any request a stack or a
// schedule sends without the gate, reaches this and is answered the same way.
func elasticacheMissing(operation string, req *AWSRequest, param string) *AWSError {
	return wireRequireMember("elasticache", operation, req, param, &AWSError{
		Code: "InvalidParameterValue", Message: param + " is required", HTTPStatus: http.StatusBadRequest})
}
