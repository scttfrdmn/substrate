package substrate

import "time"

// elasticacheNamespace is the state namespace used by the ElastiCache plugin.
const elasticacheNamespace = "elasticache"

// elasticacheXMLNS is the XML namespace used in ElastiCache API responses.
const elasticacheXMLNS = "https://elasticache.amazonaws.com/doc/2015-02-02/"

// ElastiCacheCacheCluster represents an Amazon ElastiCache cache cluster.
type ElastiCacheCacheCluster struct {
	// CacheClusterID is the unique identifier for the cache cluster.
	CacheClusterID string `json:"CacheClusterId"`
	// CacheNodeType is the compute/memory class of the cache nodes.
	CacheNodeType string `json:"CacheNodeType"`
	// Engine is the name of the cache engine ("redis" or "memcached").
	Engine string `json:"Engine"`
	// EngineVersion is the version of the cache engine.
	EngineVersion string `json:"EngineVersion"`
	// CacheClusterStatus is the current status of the cache cluster.
	CacheClusterStatus string `json:"CacheClusterStatus"`
	// NumCacheNodes is the number of cache nodes in the cluster.
	NumCacheNodes int `json:"NumCacheNodes"`
	// CacheClusterARN is the Amazon Resource Name for the cache cluster.
	CacheClusterARN string `json:"ARN"`
	// ConfigurationEndpoint holds the configuration endpoint for memcached clusters.
	ConfigurationEndpoint *ElastiCacheEndpoint `json:"ConfigurationEndpoint,omitempty"`
	// ReplicationGroupID is the ID of the replication group this cluster belongs to.
	ReplicationGroupID string `json:"ReplicationGroupId,omitempty"`
	// Tags holds resource tags as key-value pairs.
	Tags map[string]string `json:"Tags"`
	// AccountID is the AWS account that owns the cluster.
	AccountID string `json:"AccountID"`
	// Region is the AWS region where the cluster resides.
	Region string `json:"Region"`
	// CreatedAt is the time the cluster was created.
	CreatedAt time.Time `json:"CreatedAt"`
}

// ElastiCacheReplicationGroup represents an Amazon ElastiCache replication group.
type ElastiCacheReplicationGroup struct {
	// ReplicationGroupID is the unique identifier for the replication group.
	ReplicationGroupID string `json:"ReplicationGroupId"`
	// Description is a user-supplied description for the replication group.
	Description string `json:"Description"`
	// Status is the current status of the replication group.
	Status string `json:"Status"`
	// AutomaticFailover indicates whether automatic failover is enabled.
	AutomaticFailover string `json:"AutomaticFailover"`
	// MultiAZ indicates whether Multi-AZ support is enabled.
	MultiAZ string `json:"MultiAZ"`
	// ARN is the Amazon Resource Name for the replication group.
	ARN string `json:"ARN"`
	// Tags holds resource tags as key-value pairs.
	Tags map[string]string `json:"Tags"`
	// AccountID is the AWS account that owns the replication group.
	AccountID string `json:"AccountID"`
	// Region is the AWS region where the replication group resides.
	Region string `json:"Region"`
	// CreatedAt is the time the replication group was created.
	CreatedAt time.Time `json:"CreatedAt"`
}

// ElastiCacheEndpoint holds the endpoint address and port for a cache cluster.
type ElastiCacheEndpoint struct {
	// Address is the DNS hostname of the endpoint.
	Address string `json:"Address"`
	// Port is the port number for the endpoint.
	Port int `json:"Port"`
}

// ElastiCacheCacheSubnetGroup represents an ElastiCache cache subnet group.
type ElastiCacheCacheSubnetGroup struct {
	// CacheSubnetGroupName is the name of the subnet group.
	CacheSubnetGroupName string `json:"CacheSubnetGroupName"`
	// CacheSubnetGroupDescription is the description of the subnet group.
	CacheSubnetGroupDescription string `json:"CacheSubnetGroupDescription"`
	// VpcID is the ID of the VPC for the subnet group.
	VpcID string `json:"VpcId"`
	// ARN is the Amazon Resource Name of the subnet group.
	ARN string `json:"ARN"`
	// Tags holds resource tags as key-value pairs.
	Tags map[string]string `json:"Tags"`
	// AccountID is the AWS account that owns the subnet group.
	AccountID string `json:"AccountID"`
	// Region is the AWS region where the subnet group resides.
	Region string `json:"Region"`
}

// ElastiCacheCacheParameterGroup represents an ElastiCache cache parameter group.
type ElastiCacheCacheParameterGroup struct {
	// CacheParameterGroupName is the name of the parameter group.
	CacheParameterGroupName string `json:"CacheParameterGroupName"`
	// CacheParameterGroupFamily is the name of the parameter group family.
	CacheParameterGroupFamily string `json:"CacheParameterGroupFamily"`
	// Description is a user-supplied description for the parameter group.
	Description string `json:"Description"`
	// ARN is the Amazon Resource Name of the parameter group.
	ARN string `json:"ARN"`
	// Tags holds resource tags as key-value pairs.
	Tags map[string]string `json:"Tags"`
	// AccountID is the AWS account that owns the parameter group.
	AccountID string `json:"AccountID"`
	// Region is the AWS region where the parameter group resides.
	Region string `json:"Region"`
}

// elasticacheDefaultPort returns the default port for the given cache engine.
func elasticacheDefaultPort(engine string) int {
	if engine == "memcached" {
		return 11211
	}
	// redis (default)
	return 6379
}
