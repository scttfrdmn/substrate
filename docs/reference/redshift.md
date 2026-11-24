# Redshift

**Endpoint:** `redshift.{region}.amazonaws.com`
**Protocol:** Query (API version 2012-12-01), XML responses
**Routing:** `Action` form parameter

Substrate models the Redshift cluster control plane: creating, describing,
modifying and deleting a cluster, and recording a parameter group, a subnet group
and a manual snapshot. Ten of the API's 141 operations are routed. Nothing about
the data plane is here — for `ExecuteStatement` and the rest of the statement API
see [Redshift Data API](/reference/redshift-data), which is a separate plugin on a
separate endpoint. Every response is the Query protocol's three-level document in the
`http://redshift.amazonaws.com/doc/2012-12-01/` namespace, as the reference's samples show, so an
SDK decodes it. Until #1208 none was, and the ten operations were reachable only by a caller that
read the body itself.

## Supported operations

| Operation | `Action` |
|---|---|
| CreateCluster | `CreateCluster` |
| DescribeClusters | `DescribeClusters` |
| ModifyCluster | `ModifyCluster` |
| DeleteCluster | `DeleteCluster` |
| CreateClusterParameterGroup | `CreateClusterParameterGroup` |
| DescribeClusterParameterGroups | `DescribeClusterParameterGroups` |
| CreateClusterSubnetGroup | `CreateClusterSubnetGroup` |
| DescribeClusterSubnetGroups | `DescribeClusterSubnetGroups` |
| CreateClusterSnapshot | `CreateClusterSnapshot` |
| DescribeClusterSnapshots | `DescribeClusterSnapshots` |

The other 131 operations are not routed. The whole of snapshot restore and copy,
resize and `DescribeNodeConfigurationOptions`, pause and resume, `RebootCluster`,
`GetClusterCredentials`, the event, HSM, usage-limit, reserved-node and
scheduled-action families, every tagging operation, and the parameter-level
operations `DescribeClusterParameters` and `ModifyClusterParameterGroup` reach the
dispatch switch's default arm.

## An unrouted action answers a code Redshift's own page publishes

Redshift's Common Errors page publishes no code for an unrecognized action: its list carries no
`InvalidAction`. So a Redshift refusal reads its answer off that page rather than borrowing the Query
family's code (#1208):

| Request | Code | Status |
|---|---|---|
| No `Action` at all | `MissingAction` | 400 |
| An `Action` substrate does not route | `InvalidParameterValue` | 400 |

`Action` is one of the page's Common Parameters, so an unrecognized one is "a value that you provided
for a parameter isn't valid". `ValidationError` was the other candidate, but it describes input that
fails a format or constraint, which a well-formed but unknown name does not. SQS keeps `InvalidAction`
correctly: its Common Errors page is the one Query-family list AWS has not regenerated, and it still
publishes the code (#1064). The two pages are different generations, and the divergence between them
is real.

## Every response is the published three-level document

Each of the ten operations answers

```xml
<CreateClusterResponse xmlns="http://redshift.amazonaws.com/doc/2012-12-01/">
  <CreateClusterResult>
    <Cluster>…</Cluster>
  </CreateClusterResult>
  <ResponseMetadata><RequestId>…</RequestId></ResponseMetadata>
</CreateClusterResponse>
```

with both element names derived from the operation, the shape every Redshift page's sample shows.
The `RequestId` is the request's own ID, the one the event log records, so a replayed response is
byte-identical to the recorded one. The envelope is the one ELB's responses use (#1149), shared
rather than copied. Until #1208 the result was the document root, with no namespace, and the request
ID was accepted and discarded, so a Query-protocol SDK found neither the `…Response` nor the
`…Result` it locates a result by
([#1208](https://github.com/scttfrdmn/substrate/issues/1208)). `TestRedshiftEnvelope_*` in
`emulator/redshift_envelope_test.go` assert this on the raw bytes of every operation and of the
refusals. A refusal answers the shared Query `<ErrorResponse>` document, which carries the same
request ID in its `<RequestId>`; see [What an error response carries](/reference/concepts/errors#what-an-error-response-carries)
(#1241).

## Each element is named for what the reference publishes

A single cluster is `<Cluster>`, and each list names its elements for its member type:
`<Clusters><Cluster>`, `<ParameterGroups><ClusterParameterGroup>`,
`<ClusterSubnetGroups><ClusterSubnetGroup>` and `<Snapshots><Snapshot>`, as the four describes'
Response Elements (`Clusters.Cluster.N` and the rest) and samples publish. The element name is per
list, not a convention: `Cluster` does publish two `member`-named lists, `ClusterNodes.member.N` and
`PendingActions.member.N`, neither of which substrate models. Until #1208 the cluster type forced
`member` on itself, so a single cluster rendered `<Cluster><member>` and every list `<…><member>`
([#1208](https://github.com/scttfrdmn/substrate/issues/1208)).

## The two refusal codes are spelled as published

A duplicate `CreateCluster` answers `ClusterAlreadyExists` at 400. A `ClusterIdentifier` naming no
cluster answers `ClusterNotFound` at 404, at `DescribeClusters`, `ModifyCluster`, `DeleteCluster` and
`CreateClusterSnapshot`. Both are the codes and statuses those pages publish. Until #1208 they were
`ClusterAlreadyExistsFault` and `ClusterNotFoundFault`, the shape names in the service model rather
than the wire codes, so a consumer branching on the published code never took the branch. #1208's
text gave the not-found status as 400; the pages give 404, which is what substrate answers
([#1208](https://github.com/scttfrdmn/substrate/issues/1208)).

## A cluster's and a snapshot's lifecycles are seeded

`Cluster` publishes twenty `ClusterStatus` values, from `creating` and `modifying` through `resizing`,
`paused`, `storage-full` and `incompatible-parameters`. Until [#1196](https://github.com/scttfrdmn/substrate/issues/1196) substrate emitted one of
them, `available`: a cluster and a snapshot were born available, a resize took effect in place, and a
delete erased the record. So a consumer's wait-until-available loop exited on its first poll, a
modify-then-poll loop had nothing to poll, and a wait-until-deleted loop had no deleting cluster.
Both lifecycles are now [seeded progressions](/reference/concepts/seeds-and-replay#how-a-progression-is-seeded):

```
POST   /v1/redshift/cluster-status    {"clusterIdentifier":"*","pendingObservations":2,"state":"…","finalState":"hardware-failure"}
DELETE /v1/redshift/cluster-status    (all seeds; ?clusterIdentifier=… for one)
POST   /v1/redshift/snapshot-status   {"snapshotIdentifier":"*","pendingObservations":2,"finalState":"failed"}
DELETE /v1/redshift/snapshot-status   (all seeds; ?snapshotIdentifier=… for one)
```

**What the create answers is published, and does not depend on a seed.** `API_CreateCluster`'s
sample answers `<ClusterStatus>creating</ClusterStatus>`, and `API_Snapshot` states that
`CreateClusterSnapshot` "returns status as 'creating'", so both creates answer `creating`. A describe
then reports the countdown. Unseeded, it reports the record's own `available` from the first
describe, the zero default every existing describe relies on.

One seed covers each transition of a cluster, because every transition restarts its countdown:

| Transition | While the countdown runs, `DescribeClusters` reports | Once it is spent |
|------------|------------------------------------------------------|------------------|
| `CreateCluster` | `creating` (or the seed's `state`) | the seed's `finalState`, default `available`. `hardware-failure`, `incompatible-network` and the other settled statuses are reachable this way; `creating`, `deleting` and `final-snapshot` are refused as final states |
| `ModifyCluster` changing `NodeType` or `NumberOfNodes` | `resizing` (or the seed's `state`), with the previous `NodeType` and `NumberOfNodes` and the new ones under `PendingModifiedValues` | the new values, and no `PendingModifiedValues` |
| `DeleteCluster` | `final-snapshot` for the first observation when a final snapshot was requested, `deleting` otherwise | the record is removed and the describe answers `ClusterNotFound`/404 |

A snapshot reports `creating` (or the seed's `state`) for the countdown, then the seed's `finalState`,
default `available`. `failed` reaches a failed snapshot. The four statuses `API_Snapshot` names are
accepted.

Unseeded, `ModifyCluster` applies a resize in place and `DeleteCluster` removes the record at once,
as before. `ModifyCluster` and `DeleteCluster` refuse a cluster whose resize or delete is still
counting down with the pages' `InvalidClusterState`/400, "The specified cluster is not in the available
state". A create counting down is not refused, because a seed posted after a create restarts its
countdown too (a POST resets every counter it governs) and is usually meant for the next transition.
Nor is a cluster that settled in a failure status, so a test that seeded `hardware-failure` can still
delete it.

## Deleting a cluster reads its final-snapshot parameters

`API_DeleteCluster`'s response answers `deleting`, as its sample does, or `final-snapshot` when a final
snapshot was requested, as its text says ("the status of the cluster will be 'final-snapshot' while the
snapshot is being taken, then it's 'deleting'"). Until #1196 all three of its final-snapshot parameters
were discarded:

| Parameter | Published rule | What substrate answers |
|-----------|----------------|------------------------|
| `SkipFinalClusterSnapshot` | Default `false`; `FinalClusterSnapshotIdentifier` "must be specified if SkipFinalClusterSnapshot is false" | Neither sent is `InvalidParameterCombination`/400; not `true` or `false` is `InvalidParameterValue`/400 |
| `FinalClusterSnapshotIdentifier` | "If this parameter is provided, SkipFinalClusterSnapshot must be false"; 1 to 255 alphanumeric characters, a letter first, no two consecutive hyphens or trailing hyphen | Sent with skip `true` is `InvalidParameterCombination`/400; malformed is `InvalidParameterValue`/400; taken is the page's `ClusterSnapshotAlreadyExists`/400. Otherwise a manual snapshot of the cluster is written, and `DescribeClusterSnapshots` reports it |
| `FinalClusterSnapshotRetentionPeriod` | -1, or 1 to 3,653 | Otherwise the page's `InvalidRetentionPeriodFault`/400. The period is not stored |

Neither combination rule has a code on the page, so both answer `InvalidParameterCombination`, Redshift's
Common Errors code for parameters that must not be used together — substrate's reading. A consumer
that deleted a cluster with only `ClusterIdentifier` must now send `SkipFinalClusterSnapshot=true` or
name a final snapshot, as AWS requires.

## The cluster record carries ten of sixty-three members

`redshiftClusterXML` answers `ClusterIdentifier`, `ClusterStatus`, `NodeType`, `MasterUsername`,
`DBName`, `NumberOfNodes`, `ClusterCreateTime`, `VpcId`, `AvailabilityZone` and `Endpoint`. The
reference's `Cluster` type publishes sixty-three, every one `Required: No`, so the ten are a legal
subset. The other fifty-three are absent rather than present and empty, and the type's doc comment
names each with its reason ([#1199](https://github.com/scttfrdmn/substrate/issues/1199)). They fall
into two groups:

- **Members `CreateCluster` accepts and the record does not keep.** Among them are
  `AllowVersionUpgrade`, `AutomatedSnapshotRetentionPeriod`, `ClusterVersion`,
  `ClusterSubnetGroupName`, `ClusterParameterGroups`, `Encrypted`, `KmsKeyId`, `PubliclyAccessible`,
  `IamRoles`, `MaintenanceTrackName`, `PreferredMaintenanceWindow` and `Tags`. Rendering the published
  default would assert a value the caller may have set otherwise.
- **Service-side state substrate does not model.** Among them are `ClusterAvailabilityStatus`,
  `ClusterNodes`, `PendingModifiedValues`, `NextMaintenanceWindowStartTime`, `ResizeInfo`,
  `RestoreStatus` and `TotalStorageCapacityInMegaBytes`.

`ClusterNamespaceArn` is omitted. API_Cluster glosses it as "The namespace Amazon Resource Name (ARN)
of the cluster", a `:namespace:` ARN that names a namespace by its UUID. Until #1199 substrate filled
it with the cluster's own `arn:aws:redshift:{region}:{account}:cluster:{id}`, an ARN of the wrong
resource type. Substrate mints no namespace UUID, and the issue allows the member to be omitted.

## The account and Region a record carries reach no response

Each of the four persisted records — `RedshiftCluster`,
`RedshiftClusterParameterGroup`, `RedshiftClusterSubnetGroup` and
`RedshiftSnapshot` — declares `AccountID` and `Region` under wire-visible `json`
tags, because the record is what `MemoryStateManager` snapshots and a replay reads
back. Neither member is published by any of the four reference shapes, and neither
reaches a body: every response is rendered through a projection struct that
declares `xml` tags of its own (`redshiftClusterXML`, `redshiftParamGroupData`,
`redshiftSubnetGroupData`, `redshiftSnapshotData`), and
`emulator/redshift_types.go` carries no `xml` tag at all. All ten operations are
driven by `TestRedshiftWire_ClusterResponsesCarryNoBookkeepingMember` and its
three siblings in `emulator/redshift_wire_test.go`, which walk the raw XML and
fail on an element named for either member at any depth, so the eight entries this
discharges in `scripts/wire-bookkeeping-baseline.txt` stay listed as declarations
rather than as leaks ([#756](https://github.com/scttfrdmn/substrate/issues/756)).
The assertion is on the element *name*: the account ID and the Region both appear
in a cluster body as substrings of the endpoint address, which is published.

## Every describe pages, and applies the filters it publishes

All four describes publish the same pagination contract as RDS and ElastiCache: `MaxRecords`
"Default: `100`", "Constraints: minimum 20, maximum 100", and an opaque `Marker` on request and
response. So they share those families' value-based cursor (`queryMarkerPage`,
`emulator/query_marker_pagination.go`). Records are listed in identifier order, a page holds at most
`MaxRecords`, and `Marker` is omitted on the last page rather than answered empty. Until #1195 three of
the four did not read the request at all, and none paginated, so a consumer's paginator ran once
([#1195](https://github.com/scttfrdmn/substrate/issues/1195)).

| Request | Answer |
|---|---|
| `MaxRecords` absent | 100 |
| `MaxRecords` outside 20–100, or not an integer | `InvalidParameterValue`/400 |
| A `Marker` substrate did not issue | `InvalidParameterValue`/400 |
| `ClusterIdentifier` and `Marker` together, on `DescribeClusters` | `InvalidParameterCombination`/400 |

None of the four pages publishes a code for a bad `Marker` or `MaxRecords`. Both answer
`InvalidParameterValue` from Redshift's own Common Errors page, which each page links to, and the
"either `ClusterIdentifier` or `Marker`, but not both" constraint answers that page's
`InvalidParameterCombination`.

The filters each page publishes are applied:

| Operation | Filters applied | Naming nothing |
|---|---|---|
| `DescribeClusters` | `ClusterIdentifier` | `ClusterNotFound`/404 |
| `DescribeClusterParameterGroups` | `ParameterGroupName`, compared lower-case as it is stored | `ClusterParameterGroupNotFound`/404 |
| `DescribeClusterSubnetGroups` | `ClusterSubnetGroupName`, likewise | `ClusterSubnetGroupNotFoundFault`/400 |
| `DescribeClusterSnapshots` | `ClusterIdentifier`, `SnapshotIdentifier`, `SnapshotArn`, `SnapshotType`, `StartTime`, `EndTime`, `OwnerAccount`, `ClusterExists` | `ClusterSnapshotNotFound`/404 for `SnapshotIdentifier` |

The snapshot filters read as follows:

- `SnapshotArn` is the `arn:aws:redshift:{region}:{account}:snapshot:{cluster}/{snapshot}` form.
- `SnapshotType` must be `automated` or `manual`, and `StartTime` and `EndTime` must be ISO 8601;
  otherwise the answer is `InvalidParameterValue`.
- `OwnerAccount` naming any account but the caller's matches nothing, since substrate stores only the
  caller's snapshots.
- `ClusterExists` follows the page's four rules. `true` without `ClusterIdentifier` is
  `MissingParameter`, as the page requires the identifier then.
- A `ClusterIdentifier` naming no cluster narrows to an empty list rather than failing, because a
  snapshot outlives its cluster.

Three published filters are **not** applied, each named in its handler's doc comment:

- `TagKeys` and `TagValues`, on all four. Substrate records no Redshift tags, since `Tags.Tag.N` is not
  stored on any create, so a tag filter has nothing to match. Applying one would answer an empty list
  where AWS returns the tagged record.
- `SortingEntities` on `DescribeClusterSnapshots`. The page publishes only its type, and the cursor
  requires identifier order.

`DescribeClusterParameterGroups` also lists only the groups a caller created. The page says the listing
includes "the default parameter group", which substrate does not model.

## What ModifyCluster applies

A change of `NodeType` or `NumberOfNodes` is a resize: applied in place unseeded, and reported as
`resizing` with `PendingModifiedValues` under a seed (see
[A cluster's and a snapshot's lifecycles are seeded](#a-cluster-s-and-a-snapshot-s-lifecycles-are-seeded)).
The page's rule that a resize names both members "even if one of the parameters does not change" is
not enforced. The other modifiable parameters the page publishes — among them `ClusterType`, `MasterUserPassword`,
`ClusterVersion`, `AllowVersionUpgrade`, `PreferredMaintenanceWindow`,
`AutomatedSnapshotRetentionPeriod`, `Encrypted` and `PubliclyAccessible` — are
discarded without a refusal, so a call that modifies only those appears to
succeed and changes nothing
([#1197](https://github.com/scttfrdmn/substrate/issues/1197)).

## A required member is refused rather than defaulted

Every member the three create pages mark `Required: Yes` is checked before use, and an absent one is
`MissingParameter`/400 from Redshift's Common Errors page, naming the member
([#1197](https://github.com/scttfrdmn/substrate/issues/1197)). Until #1197 `CreateCluster` checked
only `ClusterIdentifier`: it defaulted `NodeType` to `dc2.large` and stored an absent `MasterUsername`
empty. The two group creates checked only their names. So a template omitting any of these validated
here and failed against AWS.

| Operation | Required members |
|---|---|
| `CreateCluster` | `ClusterIdentifier`, `MasterUsername`, `NodeType` |
| `CreateClusterParameterGroup` | `ParameterGroupName`, `ParameterGroupFamily`, `Description` |
| `CreateClusterSubnetGroup` | `ClusterSubnetGroupName`, `Description`, `SubnetIds.SubnetIdentifier.N` |

A newly checked member's published constraints are enforced, with `InvalidParameterValue`/400:

- `NodeType` must be one of its eight `Valid Values`.
- `MasterUsername` must be 1 to 128 characters, start with a lowercase letter, use only lowercase
  letters, digits, `_`, `+`, `.`, `@` and `-`, and not be `PUBLIC`.
- A parameter-group name must be 1 to 255 letters, digits or hyphens, start with a letter, and have
  no trailing hyphen and no two consecutive hyphens.
- A subnet-group name must be at most 255 letters, digits or hyphens, and not `Default`.

Both group names are stored lower-case, as both pages state. Two constraints are declined, each in its
handler's doc comment:

- `MasterUsername`'s reserved-word list, a separate document of several hundred SQL keywords; part
  of it would answer a different set than AWS does.
- `ParameterGroupFamily`'s valid values, which the page defers to the default groups substrate does
  not model.

`createClusterSubnetGroup` used to read `VpcId` off the request. `VpcId` is a member of the
`ClusterSubnetGroup` response, not a request parameter. The handler now reads the subnets the page
requires, records them, and answers them as `Subnets` with `SubnetGroupStatus` `Complete`, as the
page's sample does. More than twenty is `ClusterSubnetQuotaExceededFault`/400. A group created now
carries no `VpcId`, because substrate does not resolve a subnet to its VPC; a group stored before
#1197 still answers the `VpcId` it recorded.

A name already in use is refused with the code its page publishes: `ClusterParameterGroupAlreadyExists`
or `ClusterSubnetGroupAlreadyExists`, both at 400. Until now a second create silently overwrote the
first. A request that supplies every required member with valid values answers as it did before.

## What a refusal reports

| Condition | Code | Status |
|---|---|---|
| `ClusterIdentifier` absent on `CreateCluster` | `MissingParameter` | 400 |
| `ClusterIdentifier` absent on `ModifyCluster` or `DeleteCluster` | `MissingParameter` | 400 |
| `ClusterIdentifier` or `SnapshotIdentifier` absent on `CreateClusterSnapshot` | `MissingParameter` | 400 |
| `ParameterGroupName` absent on `CreateClusterParameterGroup` | `MissingParameter` | 400 |
| `ClusterSubnetGroupName` absent on `CreateClusterSubnetGroup` | `MissingParameter` | 400 |
| Cluster identifier already recorded | `ClusterAlreadyExists` | 400 |
| Named cluster not recorded | `ClusterNotFound` | 404 |
| No `Action` | `MissingAction` | 400 |
| Any of the other 131 operations | `InvalidParameterValue` | 400 |

`MissingParameter`, `MissingAction` and `InvalidParameterValue` at 400 are Redshift's own Common
Errors entries, and `ClusterAlreadyExists` and `ClusterNotFound` are the codes and statuses the
operation pages publish (#1208). Several published codes have no site in
substrate at all: `ClusterParameterGroupAlreadyExists` (400),
`ClusterSubnetGroupAlreadyExists` (400), `ClusterSnapshotAlreadyExists` (400),
`ClusterSnapshotNotFound` (404), `ClusterParameterGroupNotFound` (404),
`ClusterSubnetGroupNotFound` (400), `InvalidClusterState` (400),
`NumberOfNodesQuotaExceeded` (400), `ClusterQuotaExceeded` (400),
`InsufficientClusterCapacity` (400), `UnsupportedOperation` (400) and
`InvalidSubnet` (400). Because substrate never records a non-`available` status,
`InvalidClusterState` in particular is unreachable by construction rather than
merely unimplemented.

## CloudFormation resource types

Substrate has no Redshift provisioner. `AWS::Redshift::Cluster` appears only in
`cfnSnapshotCapableTypes` (`emulator/cfn_deployer.go:182`), which records that the
reference lists the type as supporting the `Snapshot` deletion policy; nothing
dispatches it. A template declaring one is refused in
[strict mode](/reference/cloudformation#strict-mode-and-template-validation). In lenient mode it falls to
`dispatchResource`'s generic stub, which logs "unknown CloudFormation resource type;
using generic stub", so the stack reaches `CREATE_COMPLETE` and no cluster exists for
`DescribeClusters` to find. No `AWS::Timestream::*` or other Redshift
type is dispatched.

## Cost

| Operation | Cost per call (USD) |
|---|---|
| `CreateCluster` | 0.0002 |
| `CreateClusterSnapshot` | 0.00002 |

Both keys are `redshift/{Operation}` and both name routed operations, so both
attribute on every matching call. The other eight routed operations are free.
