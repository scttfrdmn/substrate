# RDS

**Endpoint:** `rds.{region}.amazonaws.com`
**Protocol:** AWS Query (form-encoded, `Action=` parameter)

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateDBInstance | Reports `InstanceCreateTime` from the simulated clock. `DBSubnetGroup` is the published object, so the group's name is one element inside it |
| DescribeDBInstances | Same instance shape as CreateDBInstance. Paginates on `Marker`/`MaxRecords`; refuses a `MaxRecords` outside the published 20–100 with `InvalidParameterValue` — see [A page size outside the documented range](/reference/concepts/listing-order#a-page-size-outside-the-documented-range-is-refused-not-honored-or-rewritten) |
| DeleteDBInstance | Same instance shape as CreateDBInstance, with `DBInstanceStatus` `deleting` |
| ModifyDBInstance | Same instance shape as CreateDBInstance |
| StartDBInstance | Same instance shape as CreateDBInstance, `available`. Answers `StartDBInstanceResult`; it answered an unnamed `Result` element until #756, which no SDK could decode |
| StopDBInstance | Same instance shape as CreateDBInstance, `stopped`. Answers `StopDBInstanceResult` — same fix |
| RebootDBInstance | Same instance shape as CreateDBInstance, `available`. Answers `RebootDBInstanceResult` — same fix |
| CreateDBSnapshot | Reports `SnapshotCreateTime` from the simulated clock. `OriginalSnapshotCreateTime` and `SourceRegion` are absent: substrate copies no snapshot, so either would claim a copy had happened |
| DescribeDBSnapshots | Same snapshot shape as CreateDBSnapshot. Paginates on `Marker`/`MaxRecords`; a `Marker` it did not issue and a `MaxRecords` outside the published 20–100 are refused with `InvalidParameterValue`, which this page does not publish — see [Six describes published a cursor](/reference/concepts/listing-order#six-describes-published-a-cursor-and-implemented-none-of-it). Filtering by `DBSnapshotIdentifier` for a snapshot that does not exist answers `DBSnapshotNotFound` / 404; a `DBInstanceIdentifier` that names no instance stays an empty `200`, because only the snapshot filter has a published fault — see [A single-resource filter that names nothing](/reference/concepts/listing-order#a-single-resource-filter-that-names-nothing-answers-the-published-fault) |
| DeleteDBSnapshot | Same snapshot shape as CreateDBSnapshot |
| RestoreDBInstanceFromDBSnapshot | Same instance shape as CreateDBInstance. The restored instance's `InstanceCreateTime` is its own, not the source instance's — substrate does not carry a creation time across a restore |
| CreateDBCluster | Reports `ClusterCreateTime` from the simulated clock. `DBSubnetGroup` is a flat string here — `API_DBCluster` publishes the group's name, not the object `API_DBInstance` publishes |
| DescribeDBClusters | Same cluster shape as CreateDBCluster. Paginates on `Marker`/`MaxRecords`; refuses a `MaxRecords` outside the published 20–100 with `InvalidParameterValue` — see [A page size outside the documented range](/reference/concepts/listing-order#a-page-size-outside-the-documented-range-is-refused-not-honored-or-rewritten) |
| DeleteDBCluster | Same cluster shape as CreateDBCluster, with `Status` `deleting`. It reported only `DBClusterIdentifier`, `Status` and `DBClusterArn` until #756, where `API_DeleteDBCluster`'s response element is the whole `DBCluster` |
| CreateDBSubnetGroup | No creation time: `API_DBSubnetGroup` publishes none. `Subnets` and `SupportedNetworkTypes` are absent because substrate reads no `SubnetIds` and so has no subnet to report |
| DescribeDBSubnetGroups | Same subnet-group shape as CreateDBSubnetGroup. Paginates on `Marker`/`MaxRecords`; a `Marker` it did not issue and a `MaxRecords` outside the published 20–100 are refused with `InvalidParameterValue`, which this page does not publish — see [Six describes published a cursor](/reference/concepts/listing-order#six-describes-published-a-cursor-and-implemented-none-of-it). Filtering by `DBSubnetGroupName` for a group that does not exist answers `DBSubnetGroupNotFoundFault` / 404 — see [A single-resource filter that names nothing](/reference/concepts/listing-order#a-single-resource-filter-that-names-nothing-answers-the-published-fault) |
| DeleteDBSubnetGroup | Answers the root and `ResponseMetadata` with no result element, as the sample response shows |
| CreateDBParameterGroup | All four of `API_DBParameterGroup`'s members, which is the one RDS shape substrate models completely. No creation time: the shape publishes none |
| DescribeDBParameterGroups | Same parameter-group shape as CreateDBParameterGroup. Paginates on `Marker`/`MaxRecords`; a `Marker` it did not issue and a `MaxRecords` outside the published 20–100 are refused with `InvalidParameterValue`, which this page does not publish — see [Six describes published a cursor](/reference/concepts/listing-order#six-describes-published-a-cursor-and-implemented-none-of-it). Filtering by `DBParameterGroupName` for a group that does not exist answers `DBParameterGroupNotFound` / 404 — see [A single-resource filter that names nothing](/reference/concepts/listing-order#a-single-resource-filter-that-names-nothing-answers-the-published-fault) |
| DeleteDBParameterGroup | Answers the root and `ResponseMetadata` with no result element, as the sample response shows |
| ListTagsForResource | `TagList` sorted by key — see below |
| AddTagsToResource | No result element, as the sample response shows |
| RemoveTagsFromResource | No result element, as the sample response shows |

## The RDS response document

Every RDS success response is the awsQuery document its page's sample shows: an
`{Operation}Response` root in the `http://rds.amazonaws.com/doc/2014-10-31/` namespace, the
`{Operation}Result` element when the operation has an output, and `ResponseMetadata` carrying the
request's own `RequestId` — the value `Event.RequestID` records and a replay reproduces.

Two things were wrong with it. The namespace was spelled `https://` (#1238); an XML
namespace is an opaque string, so no SDK noticed, but an XSD step, a prefix-bound XPath or a golden
file recorded from AWS did. And no RDS response carried `ResponseMetadata` at all (#1242), so a
logging wrapper reading `ResponseMetadata.RequestId` off a successful call read the empty string.
Both are now built in one place for every routed action (see
[The response envelope](/reference/elasticloadbalancing#the-response-envelope-and-which-plugins-still-lack-it)). Four operations
have no output shape and their pages show no result element — `DeleteDBSubnetGroup`,
`DeleteDBParameterGroup`, `AddTagsToResource` and `RemoveTagsFromResource` — so they answer the root
and the metadata alone. An error response is the shared Query `<ErrorResponse>`, which carries no
namespace.

## An RDS ARN addresses the resource it names

The three tag operations take a `ResourceName` ARN, and so does the Resource
Groups Tagging API. Both resolve it through one function, for the reason #826
established: two derivations of one key drift, and where they drift a tag is
written to a record the other side does not read.

Four of AWS's RDS resource-type segments resolve, and they are the four whose
records substrate stores tags on:

| Segment | Example | Resource |
|---------|---------|----------|
| `db` | `arn:aws:rds:{region}:{account}:db:{name}` | DB instance |
| `cluster` | `arn:aws:rds:{region}:{account}:cluster:{name}` | DB cluster |
| `snapshot` | `arn:aws:rds:{region}:{account}:snapshot:{name}` | DB snapshot |
| `subgrp` | `arn:aws:rds:{region}:{account}:subgrp:{name}` | DB subnet group |

`cluster:` and `subgrp:` were absent until #835, and their absence was a
self-contradiction rather than a gap: `CreateDBCluster` and `CreateDBSubnetGroup`
report those ARNs, and substrate's own tag operations then refused them as an
unsupported resource type. That breaks #765's rule — a value substrate reports has
to be usable against the API that reported it — and it also put the tagging API's
`rds` arm out of reach of both resources entirely.

An **automated** snapshot needs no separate handling. AWS writes its ARN as
`snapshot:rds:{name}`, and the extra segment belongs to the identifier: such a
snapshot really is named `rds:mydb-2019-07-22-07-23`. The parse keeps the whole
remainder, so the ARN addresses that identifier rather than a truncated one. For
the other three types an identifier containing `/` or `:` is refused, because RDS
accepts neither in a name and a key built from one addresses nothing — reported as
a malformed ARN rather than as an absent resource, which is the difference between
a caller fixing its ARN and a caller waiting for a resource to appear.

The account and Region come from the ARN and never from the calling request, so an
ARN naming another account's cluster resolves that account's cluster or none at
all. It cannot reach the caller's own same-named one.

`cluster-pg` and `cluster-snapshot` are their own segments in AWS's ARN table, not
prefixes of `cluster`, and substrate stores neither — so both are refused, as are
`pg` and `es`.

## A missing resource names its own kind

Each of the three tag operations answers the 404 AWS publishes for the kind of
resource the ARN named, rather than one code for all four. Reporting
`DBInstanceNotFound` for a cluster tells a caller polling for a cluster that it
asked about the wrong sort of thing, and a consumer branching on the code to
decide whether to keep waiting branches wrong.

| Resource | Code | Status |
|----------|------|--------|
| DB instance | `DBInstanceNotFound` | 404 |
| DB cluster | `DBClusterNotFoundFault` | 404 |
| DB snapshot | `DBSnapshotNotFound` | 404 |
| DB subnet group | `DBSubnetGroupNotFoundFault` | 404 |

The first three are on `AddTagsToResource`' and `ListTagsForResource`' own
published error lists verbatim. The fourth is **substrate's reading** in one
respect only: a DB subnet group is a taggable type in AWS's ARN table, but neither
tagging operation's published error list names a subnet-group fault. The code and
the status are still AWS's — `DescribeDBSubnetGroups` publishes
`DBSubnetGroupNotFoundFault`/404, "`DBSubnetGroupName` doesn't refer to an existing
DB subnet group." — so what substrate decides is where to answer it, not what it
is. Keeping `DBInstanceNotFound` for a subnet group is wrong under any reading.

`TagList` is sorted by key. **AWS documents no order for it** — its own sample
response renders `owner` before `environment` — so this is substrate's reading,
taken for the reason #862 records: the list was built by ranging a Go map, so two
identical calls answered in different orders and a caller asserting on the body
could not replay a recorded run.

**Responses are rendered from the published shape.** All five RDS records — DB instance, cluster,
snapshot, subnet group and parameter group — already were, unlike ECS's and Glue's: every one of the
eighteen operations that reports a resource has always answered through a separate item struct, so
substrate's own `AccountID`, `Region` and `ever_tagged` could not reach an RDS body even before
[#756](https://github.com/scttfrdmn/substrate/issues/756). What that issue added here is the
assertion that this stays true, and none of the three has a published home to move to: no RDS shape
publishes an account or a Region member, and every one publishes the resource's ARN, from which a
caller recovers both. `API_DBSnapshot.SourceRegion` is not that home — it carries a value only for a
cross-account or cross-Region copy, which substrate does not perform, so reporting the request's
Region there would tell a caller the snapshot had been copied.

Writing that assertion settled three divergences, each noted in the table above. The first is the
creation timestamp: substrate stored one on the instance, the cluster and the snapshot and reported
it on none of them, while AWS publishes one on each of those three shapes under a different name —
`InstanceCreateTime`, `ClusterCreateTime` and `SnapshotCreateTime`. All three are now reported, as
ISO8601, which is the Query protocol's own default format; an unset one is omitted rather than
rendered as the year-one instant. The second is `DeleteDBCluster`, which reported three members of
the eleven it held. The third is the cluster's `DBSubnetGroup`, which was nested as the instance's
object where `API_DBCluster` publishes a flat string, so an SDK reading `DBCluster.DBSubnetGroup`
got `""` off a response that held the name one element deeper. The fields themselves stay on all
five stored records, so the state a recorded run replays from is unchanged and the Resource Groups
Tagging API — which reads those records rather than these responses — still reports an RDS
resource's tags and whether it has ever been tagged.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::RDS::DBInstance | DBInstanceIdentifier | |

## Cost

RDS db.t3.micro on-demand: $0.017 per hour (approximate for testing purposes).
