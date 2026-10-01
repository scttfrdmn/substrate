package emulator

import "time"

// ElastiCache, like RDS before it, was already answering from a projection when #756 reached it. The
// types below are the published shapes; they were lifted out of elasticache_plugin.go unchanged
// except where noted, on the pattern emulator/ecr_wire.go established (#1090) and
// emulator/efs_wire.go (#1304), emulator/ecs_wire.go (#1309), emulator/glue_wire.go (#1310) and
// emulator/rds_wire.go (#1311) followed.
//
// # Why this file is a pin rather than a fix
//
// All four ElastiCache records — ElastiCacheCacheCluster, ElastiCacheReplicationGroup,
// ElastiCacheCacheSubnetGroup and ElastiCacheCacheParameterGroup (elasticache_types.go) — carry
// AccountID and Region, two of them carry CreatedAt and one carries EverTagged: eleven baseline
// lines. But `grep 'ElastiCache[A-Za-z]* .*`xml:'` matches nothing in the tree. Not one record is
// XML-encoded directly; every one of the twelve sites that answers a resource hands a *…ToXML item
// struct to elasticacheXMLResponse, so no bookkeeping member could reach a body even before this
// change.
//
// What was missing was therefore not a projection but the assertion that one exists: the eleven
// lines were reachable-by-declaration with nothing pinning them shut, the position ECS's four
// already-projecting records were in at #1308 and RDS's five were in at #1311.
// emulator/elasticache_wire_test.go is that pin and scripts/wire-bookkeeping-projected.txt cites it.
// The lines stay in scripts/wire-bookkeeping-baseline.txt because ecr_wire.go's rule forbids
// removing them: `json:"-"` and retyping a field in place both change the format of every recorded
// run, since MemoryStateManager snapshots those bytes and a replay reads them back.
//
// # The leaked account and Region have no published home here
//
// Unlike EFS, where the leaked AccountID became the published OwnerId (#1304), and Glue, where it
// became CatalogId (#1310), no ElastiCache shape publishes an account or a Region member. All four
// publish the resource's ARN instead — API_CacheCluster.ARN, API_ReplicationGroup.ARN,
// API_CacheSubnetGroup.ARN and API_CacheParameterGroup.ARN, each spelled exactly ARN — and both
// values are recoverable from it, which is what the real API requires of a caller. Checked
// member-by-member against all four reference pages: there is no candidate to move either value to.
//
// # Two creation times were persisted and reported nowhere
//
// Two records store CreatedAt from the simulated clock, and neither item struct carried it. AWS
// publishes a creation timestamp on both of those shapes, under a different name on each:
//
//	record                       substrate   published                     authority
//	ElastiCacheCacheCluster      CreatedAt   CacheClusterCreateTime        API_CacheCluster
//	ElastiCacheReplicationGroup  CreatedAt   ReplicationGroupCreateTime    API_ReplicationGroup
//
// So this is a gap rather than a #1013 "substrate has not measured it" absence: substrate holds the
// value and a consumer asking the question got nothing. Both are Type: Timestamp, Required: No, so
// each is a pointer here and is omitted when the record's time is zero — see elasticacheTimeOrNil.
//
// API_CacheSubnetGroup and API_CacheParameterGroup publish no creation-time member at all, which is
// why those two records store no CreatedAt and why nothing is added to their item structs. The
// declaration surface and the published surface already agreed there.
//
// The format needs no fix, as with RDS and unlike Glue's. The AWS Query protocol's default timestamp
// format is ISO8601 and encoding/xml renders a time.Time as RFC3339Nano, which satisfies it — so
// ElastiCache is not an instance of #1305.
//
// # Nothing else needed correcting on the way through
//
// RDS's move turned up a truncated delete response and a member nested one element too deep.
// Neither recurs here: each item struct is declared once at file scope and all twelve sites
// share it, so the four deletes report the same membership their creates do, and every member sits
// at the depth its reference page publishes. ConfigurationEndpoint is the one nested member and
// API_CacheCluster.ConfigurationEndpoint is an API_Endpoint object, so the nesting is the published
// form.
//
// # What stays on the records
//
// AccountID, Region, CreatedAt, Tags and EverTagged stay in elasticache_types.go, untouched. The
// state keys already scope every record by account and Region so nothing reads those two back, but a
// persisted member is not free to remove, per ecr_wire.go's rule above — and Tags and EverTagged are
// read off the stored record to answer the Resource Groups Tagging API's GetResources.
//
// One inert side-effect, recorded so it is not re-discovered as a defect: tagging edits the record
// as raw JSON through updateTagsByARN, whose taggingStampRecordAnyEverTagged
// (tagging_ever_tagged.go) writes an `ever_tagged` member to whatever map it is handed. So tagging a
// replication group adds that member to the stored JSON even though ElastiCacheReplicationGroup
// declares no field for it — the RDSDBSnapshot situation exactly. It is not a baseline line, the
// baseline counting Go declarations, and no item struct has the element, so it cannot reach a body.
// elasticacheResolveARN reaches only cluster and replicationgroup, so the subnet group and the
// parameter group cannot be tagged through ElastiCache at all and the question does not arise for
// them.
//
// # What substrate does not model
//
// Each type below lists the published members it omits. They are absent rather than present and
// zero, per #1013: a count, a status or a timestamp reported as its zero value reads as a
// measurement, and substrate has measured nothing. Three are worth naming because a consumer is
// likely to look for them, and all three are absent for want of a value rather than by choice:
// createCacheCluster never reads the request's CacheSubnetGroupName, createCacheSubnetGroup never
// reads its SubnetIds (so Subnets and SupportedNetworkTypes have nothing to report), and IsGlobal is
// a Global datastore's flag, which substrate does not model at all.

// elasticacheTimeOrNil returns nil for the zero time, so an unset optional timestamp is omitted from
// the body rather than rendered as the year-one instant encoding/xml would otherwise write.
//
// A pointer is necessary rather than decorative: `omitempty` has no effect on a struct type in
// encoding/xml any more than in encoding/json, so a plain field would report
// `<CacheClusterCreateTime>0001-01-01T00:00:00Z</CacheClusterCreateTime>` where AWS omits the
// member. The zero time is reachable — deleteCacheCluster and deleteReplicationGroup both fall back
// to a record holding only the identifier when the stored JSON will not decode.
func elasticacheTimeOrNil(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// xmlEndpointItem is the Endpoint of API_CacheCluster.ConfigurationEndpoint. Those two members are
// the whole published shape.
type xmlEndpointItem struct {
	Address string `xml:"Address"`
	Port    int    `xml:"Port"`
}

// xmlCacheClusterItem is the CacheCluster of CreateCacheCluster, DescribeCacheClusters,
// ModifyCacheCluster and DeleteCacheCluster.
//
// Member names follow API_CacheCluster, on which every member is Required: No. The members substrate
// does not model are absent rather than present and zero, and there are many, API_CacheCluster being
// the widest shape in the service: AtRestEncryptionEnabled, AuthTokenEnabled,
// AuthTokenLastModifiedDate, AutoMinorVersionUpgrade, CacheNodes, CacheParameterGroup,
// CacheSecurityGroups, CacheSubnetGroupName, ClientDownloadLandingPage, IpDiscovery,
// LogDeliveryConfigurations, NetworkType, NotificationConfiguration, PendingModifiedValues,
// PreferredAvailabilityZone, PreferredMaintenanceWindow, PreferredOutpostArn,
// ReplicationGroupLogDeliveryEnabled, SecurityGroups, SnapshotRetentionLimit, SnapshotWindow,
// TransitEncryptionEnabled and TransitEncryptionMode.
type xmlCacheClusterItem struct {
	CacheClusterID         string           `xml:"CacheClusterId"`
	CacheNodeType          string           `xml:"CacheNodeType"`
	Engine                 string           `xml:"Engine"`
	EngineVersion          string           `xml:"EngineVersion"`
	CacheClusterStatus     string           `xml:"CacheClusterStatus"`
	NumCacheNodes          int              `xml:"NumCacheNodes"`
	ARN                    string           `xml:"ARN"`
	ReplicationGroupID     string           `xml:"ReplicationGroupId,omitempty"`
	CacheClusterCreateTime *time.Time       `xml:"CacheClusterCreateTime,omitempty"`
	ConfigurationEndpoint  *xmlEndpointItem `xml:"ConfigurationEndpoint,omitempty"`
}

// cacheClusterToXML projects a persisted cache cluster onto the published shape.
func cacheClusterToXML(c ElastiCacheCacheCluster) xmlCacheClusterItem {
	item := xmlCacheClusterItem{
		CacheClusterID:         c.CacheClusterID,
		CacheNodeType:          c.CacheNodeType,
		Engine:                 c.Engine,
		EngineVersion:          c.EngineVersion,
		CacheClusterStatus:     c.CacheClusterStatus,
		NumCacheNodes:          c.NumCacheNodes,
		ARN:                    c.CacheClusterARN,
		ReplicationGroupID:     c.ReplicationGroupID,
		CacheClusterCreateTime: elasticacheTimeOrNil(c.CreatedAt),
	}
	if c.ConfigurationEndpoint != nil {
		item.ConfigurationEndpoint = &xmlEndpointItem{
			Address: c.ConfigurationEndpoint.Address,
			Port:    c.ConfigurationEndpoint.Port,
		}
	}
	return item
}

// xmlReplicationGroupItem is the ReplicationGroup of CreateReplicationGroup,
// DescribeReplicationGroups, ModifyReplicationGroup and DeleteReplicationGroup.
//
// Member names follow API_ReplicationGroup, on which every member is Required: No. AutomaticFailover
// and MultiAZ are Type: String there rather than Boolean — "enabled" or "disabled" — which is why
// the record stores them as strings and they are projected unchanged. The members substrate does not
// model are absent rather than present and zero: ARN's siblings AtRestEncryptionEnabled,
// AuthTokenEnabled, AuthTokenLastModifiedDate, AutoMinorVersionUpgrade, CacheNodeType,
// ClusterEnabled, ClusterMode, ConfigurationEndpoint, DataTiering, Durability, EffectiveDurability,
// Engine, GlobalReplicationGroupInfo, IpDiscovery, KmsKeyId, LogDeliveryConfigurations,
// MemberClusters, MemberClustersOutpostArns, NetworkType, NodeGroups, PendingModifiedValues,
// SnapshotRetentionLimit, SnapshottingClusterId, SnapshotWindow, StorageEncryptionType,
// TransitEncryptionEnabled, TransitEncryptionMode and UserGroupIds.
type xmlReplicationGroupItem struct {
	ReplicationGroupID         string     `xml:"ReplicationGroupId"`
	Description                string     `xml:"Description"`
	Status                     string     `xml:"Status"`
	AutomaticFailover          string     `xml:"AutomaticFailover"`
	MultiAZ                    string     `xml:"MultiAZ"`
	ARN                        string     `xml:"ARN"`
	ReplicationGroupCreateTime *time.Time `xml:"ReplicationGroupCreateTime,omitempty"`
}

// replicationGroupToXML projects a persisted replication group onto the published shape.
func replicationGroupToXML(rg ElastiCacheReplicationGroup) xmlReplicationGroupItem {
	return xmlReplicationGroupItem{
		ReplicationGroupID:         rg.ReplicationGroupID,
		Description:                rg.Description,
		Status:                     rg.Status,
		AutomaticFailover:          rg.AutomaticFailover,
		MultiAZ:                    rg.MultiAZ,
		ARN:                        rg.ARN,
		ReplicationGroupCreateTime: elasticacheTimeOrNil(rg.CreatedAt),
	}
}

// xmlCacheSubnetGroupItem is the CacheSubnetGroup of CreateCacheSubnetGroup and
// DescribeCacheSubnetGroups.
//
// Member names follow API_CacheSubnetGroup, which publishes no creation-time member — so unlike the
// two shapes above, nothing here reports a timestamp. Subnets and SupportedNetworkTypes are its
// other two members; createCacheSubnetGroup never reads the request's SubnetIds, so both are absent
// for want of a value rather than by choice.
type xmlCacheSubnetGroupItem struct {
	CacheSubnetGroupName        string `xml:"CacheSubnetGroupName"`
	CacheSubnetGroupDescription string `xml:"CacheSubnetGroupDescription"`
	VpcID                       string `xml:"VpcId"`
	ARN                         string `xml:"ARN"`
}

// cacheSubnetGroupToXML projects a persisted cache subnet group onto the published shape.
func cacheSubnetGroupToXML(sg ElastiCacheCacheSubnetGroup) xmlCacheSubnetGroupItem {
	return xmlCacheSubnetGroupItem{
		CacheSubnetGroupName:        sg.CacheSubnetGroupName,
		CacheSubnetGroupDescription: sg.CacheSubnetGroupDescription,
		VpcID:                       sg.VpcID,
		ARN:                         sg.ARN,
	}
}

// xmlCacheParameterGroupItem is the CacheParameterGroup of CreateCacheParameterGroup and
// DescribeCacheParameterGroups.
//
// Member names follow API_CacheParameterGroup, which publishes five members and no creation time.
// IsGlobal, the fifth, flags a parameter group attached to a Global datastore, which substrate does
// not model, so it is absent rather than reported false.
type xmlCacheParameterGroupItem struct {
	CacheParameterGroupName   string `xml:"CacheParameterGroupName"`
	CacheParameterGroupFamily string `xml:"CacheParameterGroupFamily"`
	Description               string `xml:"Description"`
	ARN                       string `xml:"ARN"`
}

// cacheParameterGroupToXML projects a persisted cache parameter group onto the published shape.
func cacheParameterGroupToXML(pg ElastiCacheCacheParameterGroup) xmlCacheParameterGroupItem {
	return xmlCacheParameterGroupItem{
		CacheParameterGroupName:   pg.CacheParameterGroupName,
		CacheParameterGroupFamily: pg.CacheParameterGroupFamily,
		Description:               pg.Description,
		ARN:                       pg.ARN,
	}
}
