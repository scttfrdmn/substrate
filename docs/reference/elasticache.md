# ElastiCache

**Endpoint:** `elasticache.{region}.amazonaws.com`
**Protocol:** AWS Query (form-encoded, `Action=` parameter)

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateCacheCluster | Answers the `CacheCluster` of `API_CacheCluster`, including `CacheClusterCreateTime`; `CacheSubnetGroupName` is absent because the request's value is not read. Carries no account or Region member — `ARN` is where both are recoverable from |
| DescribeCacheClusters | Same cluster shape as CreateCacheCluster. Paginates on `Marker`/`MaxRecords`; refuses a `MaxRecords` outside the published 20–100 with `InvalidParameterValue`, which this service's page publishes — see [A page size outside the documented range](/reference/concepts/listing-order#a-page-size-outside-the-documented-range-is-refused-not-honored-or-rewritten) |
| ModifyCacheCluster | Same cluster shape as CreateCacheCluster; `CacheClusterCreateTime` survives the modify |
| DeleteCacheCluster | Same cluster shape as CreateCacheCluster, reported with `CacheClusterStatus` `deleting` |
| CreateReplicationGroup | Answers the `ReplicationGroup` of `API_ReplicationGroup`, including `ReplicationGroupCreateTime`. `AutomaticFailover` and `MultiAZ` are reported as the published strings `enabled`/`disabled`, not booleans. Carries no account or Region member |
| DescribeReplicationGroups | Paginates on `Marker`/`MaxRecords`; a `Marker` it did not issue and a `MaxRecords` outside the published 20–100 are refused with `InvalidParameterValue`, which this page publishes — see [Six describes published a cursor](/reference/concepts/listing-order#six-describes-published-a-cursor-and-implemented-none-of-it). Filtering by `ReplicationGroupId` for a group that does not exist answers `ReplicationGroupNotFoundFault` / 404, now through the same helper as the other five — see [A single-resource filter that names nothing](/reference/concepts/listing-order#a-single-resource-filter-that-names-nothing-answers-the-published-fault) |
| ModifyReplicationGroup | Same replication-group shape as CreateReplicationGroup; `ReplicationGroupCreateTime` survives the modify |
| DeleteReplicationGroup | Same replication-group shape as CreateReplicationGroup, reported with `Status` `deleting` |
| CreateCacheSubnetGroup | Answers the `CacheSubnetGroup` of `API_CacheSubnetGroup`, which publishes no creation time, so none is reported. `Subnets` and `SupportedNetworkTypes` are absent because the request's `SubnetIds` is not read. Carries no account or Region member |
| DescribeCacheSubnetGroups | Same subnet-group shape as CreateCacheSubnetGroup. Paginates on `Marker`/`MaxRecords`; a `Marker` it did not issue and a `MaxRecords` outside the published 20–100 are refused with `InvalidParameterValue`, which this page does **not** publish although its two ElastiCache siblings do — see [Six describes published a cursor](/reference/concepts/listing-order#six-describes-published-a-cursor-and-implemented-none-of-it). Filtering by `CacheSubnetGroupName` for a group that does not exist answers `CacheSubnetGroupNotFoundFault` / **400**, the one status outlier among the six — see [A single-resource filter that names nothing](/reference/concepts/listing-order#a-single-resource-filter-that-names-nothing-answers-the-published-fault) |
| DeleteCacheSubnetGroup | Answers the root and `ResponseMetadata` with no result element, as the sample response shows |
| CreateCacheParameterGroup | Answers the `CacheParameterGroup` of `API_CacheParameterGroup`, which publishes no creation time, so none is reported. `IsGlobal` is absent rather than reported `false`, substrate modelling no Global datastore. Carries no account or Region member |
| DescribeCacheParameterGroups | Same parameter-group shape as CreateCacheParameterGroup. Paginates on `Marker`/`MaxRecords`; a `Marker` it did not issue and a `MaxRecords` outside the published 20–100 are refused with `InvalidParameterValue`, which this page publishes — see [Six describes published a cursor](/reference/concepts/listing-order#six-describes-published-a-cursor-and-implemented-none-of-it). Filtering by `CacheParameterGroupName` for a group that does not exist answers `CacheParameterGroupNotFound` / 404 — see [A single-resource filter that names nothing](/reference/concepts/listing-order#a-single-resource-filter-that-names-nothing-answers-the-published-fault) |
| DeleteCacheParameterGroup | Answers the root and `ResponseMetadata` with no result element, as the sample response shows |
| ListTagsForResource | Resolves a `cluster` or `replicationgroup` ARN; a `subnetgroup` or `parametergroup` ARN is refused with `InvalidParameterValue` |
| AddTagsToResource | Same ARN resolution as ListTagsForResource. Answers the resource's `TagList` after the change, sorted by key — the page's one response element; it answered an empty document until #1242 |
| RemoveTagsFromResource | Same ARN resolution as ListTagsForResource. Answers the resource's remaining `TagList`, sorted by key — same fix |

## The ElastiCache response document

Every ElastiCache success response is the awsQuery document its page's sample shows: an
`{Operation}Response` root in the `http://elasticache.amazonaws.com/doc/2015-02-02/` namespace, the
`{Operation}Result` element when the operation has an output, and `ResponseMetadata` carrying the
request's own `RequestId`. The namespace was spelled `https://` until #1238 and no response carried
`ResponseMetadata` until #1242, exactly as on RDS (see [The RDS response document](/reference/rds#the-rds-response-document)).

The result element is read off each page rather than carried over from RDS, and the two services
differ on the tag operations: RDS's `AddTagsToResource` and `RemoveTagsFromResource` have no output,
while ElastiCache's both publish `TagListMessage`, the same output as `ListTagsForResource`. So only
`DeleteCacheSubnetGroup` and `DeleteCacheParameterGroup` answer the root and the metadata alone.

**Responses are rendered from the published shape.** All four ElastiCache records — cache cluster,
replication group, cache subnet group and cache parameter group — already were, as RDS's five were
and unlike ECS's and Glue's: every one of the twelve operations that reports a resource has always
answered through a separate item struct, so substrate's own `AccountID`, `Region` and `ever_tagged`
could not reach an ElastiCache body even before
[#756](https://github.com/scttfrdmn/substrate/issues/756). What that issue added here is the
assertion that this stays true, and none of the three has a published home to move to: no
ElastiCache shape publishes an account or a Region member, and all four publish the resource's `ARN`,
from which a caller recovers both.

Writing that assertion settled one divergence, noted in the table above. Substrate stored a creation
timestamp on the cache cluster and the replication group and reported it on neither, while AWS
publishes one on each of those two shapes under a different name — `CacheClusterCreateTime` and
`ReplicationGroupCreateTime`. Both are now reported, as the ISO8601 instant the Query protocol
publishes. `API_CacheSubnetGroup` and `API_CacheParameterGroup` publish no creation time at all,
which is why those two records store none and why neither reports one. The stored fields themselves
stay on all four records, so the state a recorded run replays from is unchanged and the Resource
Groups Tagging API — which reads those records rather than these responses — still reports an
ElastiCache cluster's tags and whether it has ever been tagged.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::ElastiCache::CacheCluster | CacheClusterId | |
| AWS::ElastiCache::ReplicationGroup | ReplicationGroupId | |

## Request validation

ElastiCache is validated against its service model before any plugin sees a client's request
(#1612; see [Request validation](/reference/concepts/validation)). A missing required parameter
is `InvalidParameterValue`/400 in ElastiCache's own sentence, `The parameter SubnetIds must be
provided and must not be blank.`, which is AWS's observed answer and which the plugin also gives
for a blank one. Any other failure is `InvalidParameterValue` in the validator's quoted form, `1
validation error detected: Value '12345' at 'nodeGroupConfiguration.1.member.nodeGroupId' …`;
no source shows ElastiCache's wording for it, so that message is substrate's own.

`CreateCacheSubnetGroup` now requires `SubnetIds`, as the model does.

## Cost

ElastiCache cache.t3.micro: $0.017 per node-hour (approximate).
