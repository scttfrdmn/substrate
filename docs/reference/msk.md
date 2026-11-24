# MSK

**Endpoint:** `kafka.{region}.amazonaws.com`

**Protocol:** REST-JSON over versioned paths — `POST /v1/clusters` rather than an `X-Amz-Target`
header — with error codes carried in the `x-amzn-errortype` header. API version 2018-11-14. The
signing name and service key are `kafka`, not `msk`.

**Routing:** on the HTTP method and path together, resolved by a single ordered switch. Since #1009 an
empty path parameter routes to the single-cluster operation rather than folding onto the list
operation, so `GET /v1/clusters/` reaches `DescribeCluster` with an empty ARN and is refused for the
reason it is wrong. Every arm is [anchored at its published prefix](#the-nodes-and-bootstrap-brokers-arms-are-anchored-at-v1-clusters),
so a path that matches no published URI answers `UnknownOperationException`/404.

Nine operations over one resource: a cluster, addressed by ARN, in both the v1 and the v2 shapes,
provisioned or (through `CreateClusterV2`) serverless. No Kafka runs. A cluster is a record, and brokers
are synthesised from the stored count on each `ListNodes` call rather than stored, so a bootstrap-broker
string here resolves to nothing and a producer cannot connect.

## Supported operations

| Operation | Route, and what it answers |
|-----------|----------------------------|
| CreateCluster | `POST /v1/clusters` → `{clusterArn, clusterName, state}` as published. [Every member the page marks required is checked](#what-a-create-requires); [`ACTIVE` unseeded, `CREATING` under a seed](#a-cluster-s-lifecycle-is-seeded) |
| ListClusters | `GET /v1/clusters` → `{clusterInfoList, nextToken}`. [Pages, and applies `clusterNameFilter`](#the-list-operations-page-and-filter); provisioned clusters only |
| DescribeCluster | `GET /v1/clusters/{clusterArn}` → `{clusterInfo}`. [The ARN must be the cluster's own, UUID included](#a-cluster-arn-resolves-by-name-and-uuid); [fourteen of twenty-one members are reported](#what-a-cluster-response-reports); [observes the seeded lifecycle](#a-cluster-s-lifecycle-is-seeded) |
| DeleteCluster | `DELETE /v1/clusters/{clusterArn}` → `{clusterArn, state}` with state `DELETING`. Reads `currentVersion`; [removed at once unseeded, `DELETING` for the seeded observations otherwise](#a-cluster-s-lifecycle-is-seeded) |
| GetBootstrapBrokers | `GET /v1/clusters/{clusterArn}/bootstrap-brokers` → [the broker strings the cluster's encryption and authentication imply](#what-a-cluster-response-reports), in the page's host form. [Anchored at `/v1/clusters/`](#the-nodes-and-bootstrap-brokers-arms-are-anchored-at-v1-clusters) |
| ListNodes | `GET /v1/clusters/{clusterArn}/nodes` → `{nodeInfoList, nextToken}`, one node per broker with its endpoint. [Anchored at `/v1/clusters/`](#the-nodes-and-bootstrap-brokers-arms-are-anchored-at-v1-clusters); [pages](#the-list-operations-page-and-filter) |
| CreateClusterV2 | `POST /api/v2/clusters` → `{clusterArn, clusterName, clusterType, state}`. [Provisioned or serverless, verified against its page](#the-v2-surface-verified-against-its-pages) |
| DescribeClusterV2 | `GET /api/v2/clusters/{clusterArn}` → `{clusterInfo}` in the v2 `Cluster` shape, under `provisioned` or `serverless` |
| ListClustersV2 | `GET /api/v2/clusters` → `{clusterInfoList, nextToken}`, both cluster types. [Pages, and applies `clusterNameFilter` and `clusterTypeFilter`](#the-list-operations-page-and-filter) |

There is no v2 delete arm, so `DELETE /api/v2/clusters/{clusterArn}` is refused as an unrecognised
route. The rest of the `kafka` API is unrouted: the whole update surface (`UpdateBrokerCount`,
`UpdateBrokerStorage`, `UpdateBrokerType`, `UpdateClusterConfiguration`, `UpdateClusterKafkaVersion`,
`UpdateMonitoring`, `UpdateSecurity`, `UpdateConnectivity`, `UpdateStorage`, `RebootBroker`), the
configuration surface, the cluster-operation history, the SCRAM-secret operations, the cluster-policy
operations, VPC connections, the Kafka-version listings, replicators and the three tag operations. A
cluster here can therefore be created, read, listed and deleted and nothing else: it cannot be scaled,
reconfigured, upgraded, or tagged after creation, and because `ListClusterOperations` is unrouted
there is no history to read either.

## The nodes and bootstrap-brokers arms are anchored at /v1/clusters/

The published URIs are exactly `/v1/clusters/{clusterArn}/bootstrap-brokers` and
`/v1/clusters/{clusterArn}/nodes`. A cluster ARN contains slashes and arrives decoded, so the ARN
cannot be taken as one segment. Each arm is anchored at both ends instead: the `/v1/clusters/` prefix,
the ARN, then the suffix. Until [#1205](https://github.com/scttfrdmn/substrate/issues/1205) the two
arms tested only the suffix, so `GET /anything/at/all/nodes` was taken for `ListNodes` and refused as
a bad cluster ARN (400), sending a caller to look at its ARN instead of its URL.
`GET /api/v2/clusters/{arn}/nodes` answered 200, serving a path AWS does not publish. MSK publishes
both sub-resources under `/v1` only, so the same suffixes under `/v2/` or `/api/v2/clusters/` are not
routed. Every path that matches no published URI now answers `UnknownOperationException`/404.
`emulator/rest_routing_near_miss_test.go` enumerates the near-misses.

## A cluster's lifecycle is seeded

The published `ClusterState` is `ACTIVE`, `CREATING`, `UPDATING`, `DELETING`, `FAILED`, `MAINTENANCE`,
`REBOOTING_BROKER` and `HEALING`, and a real cluster takes tens of minutes to leave `CREATING`. Until
[#1196](https://github.com/scttfrdmn/substrate/issues/1196) substrate wrote `ACTIVE` at create and nothing else, so a consumer's wait-for-active loop
returned on its first poll. A cluster's lifecycle is now a [seeded progression](/reference/concepts/seeds-and-replay#how-a-progression-is-seeded):

```
POST   /v1/msk/cluster-status   {"clusterArn":"*","pendingObservations":2,"state":"CREATING","finalState":"FAILED","stateInfo":{"code":"…","message":"…"}}
DELETE /v1/msk/cluster-status   (all seeds; ?clusterArn=… for one)
```

- **Unseeded, nothing changes.** A create answers and describes `ACTIVE`, and `DeleteCluster` removes
  the record at once, answering `DELETING`. No MSK page publishes the state a fresh create answers, so
  the record's own state is kept rather than guessed.
- **A create** under a seed answers `CREATING` (or the seed's `state`), and `DescribeCluster`,
  `DescribeClusterV2`, `ListClusters` and `ListClustersV2` report it for `pendingObservations` observations
  of that cluster. It then settles in the seed's `finalState` (default the record's `ACTIVE`). A
  `FAILED` final state answers the seed's `stateInfo` (`code`, `message`), which is how a test reaches a
  failed cluster. `finalState` may be any published state but `CREATING` and `DELETING`.
- **A delete** under a seed with pending observations keeps the record, marked deleting, and restarts
  the countdown. That many describes and lists report `DELETING`, and the observation after them
  removes the record and answers `NotFoundException`/404, as a describe of a deleted cluster does. A
  second delete of a deleting cluster answers `DELETING` again without restarting it.
- `GetBootstrapBrokers`, `ListNodes`, the create and the delete peek rather than observe: none is a
  poll of the cluster's state.

`DeleteCluster` reads its published `currentVersion` query parameter. Every cluster carries a
`currentVersion`, minted at create in the form of the page's one example (`KTVPDKIKX0DER`: thirteen
upper-case letters and digits, derived from the request ID as every minted value is). A delete naming
another version is `BadRequestException`/400 with `invalidParameter` `currentVersion`, substrate's
reading, since MSK publishes no codes (#671). A record from before #1196 carries none and accepts any.

## What a create requires

`CreateClusterRequest` (`clusters.html`) marks four members `Required: True`, and each is checked
before use and refused when absent rather than defaulted
([#1197](https://github.com/scttfrdmn/substrate/issues/1197)):

- `clusterName`, 1 to 64 characters;
- `kafkaVersion`, 1 to 128 characters;
- `numberOfBrokerNodes`. The page states no minimum, so refusing a count below 1 is substrate's reading;
- `brokerNodeGroupInfo`, whose own `clientSubnets` and `instanceType` (5 to 32 characters) are
  required in turn.

An empty `clientSubnets` list counts as present. The page's two-or-three-subnet rule is prose, not a
constraint the model states, and CloudFormation sends `[]` when a template names none. A member
that is present is held to its published constraint:
- `volumeSize` must be 1 to 16384;
- `clientBroker` must be `TLS`, `TLS_PLAINTEXT` or `PLAINTEXT`;
- `encryptionAtRest.dataVolumeKMSKeyId` is required;
- `configurationInfo` must carry an `arn` and a `revision` of at least 1.

`kafkaVersion` used to default to `3.5.1` and `numberOfBrokerNodes` to `2`, and an absent
`brokerNodeGroupInfo` was accepted, so a cluster could be created from an empty body.

`CreateClusterV2` differs because its page differs ([#1211](https://github.com/scttfrdmn/substrate/issues/1211)). Its request
marks only `clusterName` `Required: True`. `provisioned` and `serverless` are both `Required: False`,
and every member inside `ProvisionedRequest` is too, so a v2 provisioned create may omit
`kafkaVersion`, `numberOfBrokerNodes` and `brokerNodeGroupInfo`, and nothing is defaulted in their
place. A request naming neither cluster kind, or both, is refused: the page publishes no rule for
either, so this is substrate's reading. Inside `serverless`, `vpcConfigs` (each with `subnetIds`) and
`clientAuthentication` are `Required: True` and checked.

A create naming an existing cluster answers `ConflictException`/409, the status both create pages give
"This cluster name already exists."

## A cluster ARN resolves by name and UUID

The published ARN form is
`arn:aws:kafka:us-east-1:0123456789019:cluster/SalesCluster/abcd1234-abcd-cafe-abab-9876543210ab-4`,
and the trailing UUID is what distinguishes one cluster named `SalesCluster` from the next. Substrate
finds the record by the ARN's Region, account and name, then requires the ARN to be that record's own,
UUID included, so an ARN naming a deleted cluster answers `NotFoundException`/404 rather than
resolving to its replacement. Until [#1204](https://github.com/scttfrdmn/substrate/issues/1204) the
UUID was never read, so a deploy-destroy-redeploy test that reused the old ARN got 200 where AWS
answers not-found.

The UUID is minted as a version-4 UUID and a one-digit suffix (`…/{uuid}-{n}`), the form every MSK
page uses; MSK publishes no pattern for it. It is derived from the request ID
([#856](https://github.com/scttfrdmn/substrate/issues/856)), so a replay mints the ARN its recording
minted. It used to be built from the name's length and the clock, padded with
`-0001-0001-0001-000000000001`, so on a frozen clock a re-created cluster reused its predecessor's ARN.
A store read failure while resolving an ARN is now returned as an error rather than answered as
not-found.

## The list operations page and filter

`ListClusters`, `ListClustersV2` and `ListNodes` read `maxResults` and `nextToken`, and the two cluster
lists read `clusterNameFilter`, "a prefix of the name of the clusters"
([#1195](https://github.com/scttfrdmn/substrate/issues/1195)).

- **`maxResults`.** Each page describes it as "default maximum 100 results per API call" and states no
  range, so substrate defaults to 100 and refuses anything outside 1 to 100. That range is its reading.
- **`nextToken`.** An offset token; a token substrate did not issue is refused rather than answered as
  page one. `nextToken` is omitted on the last page, never sent empty.
- **`clusterTypeFilter`.** `ListClustersV2` also reads it, as `PROVISIONED` or `SERVERLESS`; any other
  value is refused.

MSK publishes no code for a bad pagination or filter value, so each refusal is `BadRequestException`/400
with `invalidParameter` naming the parameter. `ListClusters` lists provisioned clusters only. Its page
says it returns "all the MSK clusters", while `ListClustersV2`'s says "all serverless and provisioned
clusters", and v1's `ClusterInfo` has no member to describe a serverless cluster with, so this is
substrate's reading of the contrast.

## DeleteCluster's response

`DeleteClusterResponse` publishes exactly `clusterArn` and `state`, and `clusterName` is deliberately
not reported because it is not a member. The state is `DELETING`. Whether the deleting cluster stays
observable afterwards is the seed's to say; see [A cluster's lifecycle is seeded](#a-cluster-s-lifecycle-is-seeded).

## The v2 surface, verified against its pages

Until [#1211](https://github.com/scttfrdmn/substrate/issues/1211) this section said the v2 surface had no page in the API reference.
It does: `v2-clusters.html` (`ListClustersV2`, `CreateClusterV2`) and `v2-clusters-clusterarn.html`
(`DescribeClusterV2`). What each publishes, against substrate:

| Operation | Verified against the page | Not modelled |
|-----------|---------------------------|--------------|
| CreateClusterV2 | `POST /api/v2/clusters`. Request: `clusterName` (required, 1–64), `provisioned`, `serverless`, `tags`. Response: `clusterArn`, `clusterName`, `clusterType`, `state`, all four answered. A serverless create used to be ignored and stored as a provisioned cluster, and `clusterType` was not answered | — `loggingInfo`, `openMonitoring`, `rebalancing` and `brokerNodeGroupInfo.connectivityInfo` are stored and answered since [#1386](https://github.com/scttfrdmn/substrate/issues/1386); see [Logging, monitoring, rebalancing and connectivity](#logging-monitoring-rebalancing-and-connectivity) |
| DescribeClusterV2 | `GET /api/v2/clusters/{clusterArn}` → `{clusterInfo}`, the `Cluster` shape: `clusterArn`, `clusterName`, `clusterType`, `creationTime`, `state`, `tags`, and `provisioned` or `serverless` | `activeOperationArn`; `serverless.kafkaVersion`, since a serverless create takes none. `currentVersion` and `stateInfo` are answered as v1's are (#1196) |
| ListClustersV2 | `GET /api/v2/clusters`, query `clusterNameFilter`, `clusterTypeFilter`, `maxResults`, `nextToken`; response `{clusterInfoList, nextToken}` | — |

Request members arrive in the published lowerCamel spelling and are decoded case-insensitively, so the
PascalCase CloudFormation sends decodes too. Responses use the published lowerCamel names only.

## What a cluster response reports

`ClusterInfo` publishes twenty-one members. Substrate reports seventeen:
- always: `clusterArn`, `clusterName`, `state`, `creationTime`, `currentVersion` (#1196),
  `brokerNodeGroupInfo`,
  `numberOfBrokerNodes` and `currentBrokerSoftwareInfo` (`kafkaVersion`, plus `configurationArn` and
  `configurationRevision` when the create named a configuration);
- `encryptionInfo`, answered with `encryptionInTransit`'s stated defaults filled in (`clientBroker`
  "The default value is `TLS`", `inCluster` "The default value is true");
- when the create sent them: `tags`, `clientAuthentication`, `enhancedMonitoring`, `storageMode`,
  `loggingInfo`, `openMonitoring` and `rebalancing` (#1386), and `brokerNodeGroupInfo.connectivityInfo`;
- `stateInfo`, when a seeded cluster settles `FAILED` (#1196).

Absent, because nothing in substrate holds them:
- `activeOperationArn` and `customerActionStatus`, since no cluster operation is modelled;
- the two ZooKeeper connect strings.

`DescribeClusterV2`'s `provisioned` carries the same set ([#1199](https://github.com/scttfrdmn/substrate/issues/1199)).

## Logging, monitoring, rebalancing and connectivity

Both create pages accept `loggingInfo`, `openMonitoring`, `rebalancing` and, inside
`brokerNodeGroupInfo`, `connectivityInfo`, and both describe shapes publish them back. Until
[#1386](https://github.com/scttfrdmn/substrate/issues/1386) substrate decoded none of the four, so a
cluster created with logging or monitoring described with neither. They are now stored as sent and
answered by `DescribeCluster`, `ListClusters`, `DescribeClusterV2` and `ListClustersV2`. `connectivityInfo`
belongs to the broker node group, not to `provisioned`, on both pages.

Neither page states a default for any of the four, so a cluster created without one answers without
it rather than with an invented value. The constraints the model states are enforced, on v1 and v2
alike, each refusal `BadRequestException`/400 with `invalidParameter` naming the member:

| Member | Constraint | `invalidParameter` |
|--------|------------|--------------------|
| `loggingInfo.brokerLogs` | Required when `loggingInfo` is sent | `brokerLogs` |
| `cloudWatchLogs`/`firehose`/`s3` `.enabled` | Required when the target is sent, in `brokerLogs` or `authorizerLogs` | `enabled` |
| `openMonitoring.prometheus` | Required when `openMonitoring` is sent | `prometheus` |
| `jmxExporter`/`nodeExporter` `.enabledInBroker` | Required when the exporter is sent | `enabledInBroker` |
| `rebalancing.status` | `PAUSED` or `ACTIVE` | `status` |
| `connectivityInfo.networkType` | `IPV4` or `DUAL` | `networkType` |
| `connectivityInfo.publicAccess.type` | `DISABLED` or `SERVICE_PROVIDED_EIPS`, the two values its description names | `type` |

A delivery target's `logGroup`, `deliveryStream` or `bucket` is not checked: the pages mark each
optional, and what an enabled target without one does is AWS's behaviour, not something the model
states. `v2-clusters.html` would not render for verification, so these shapes and constraints are
taken from the boto3 reference for `create_cluster_v2`, `create_cluster` and `describe_cluster_v2`,
which is generated from the same service model.

`GetBootstrapBrokers` answers the strings the cluster's configuration implies, in the page's host form
`b-{n}.{clusterName}.{id}.c2.kafka.{region}.amazonaws.com`, where `{id}` is six hex digits of the ARN's
UUID. Each string uses the port the developer guide's port-information page gives:

| Member | Answered when |
|--------|---------------|
| `bootstrapBrokerString` (9092) | encryption `PLAINTEXT` or `TLS_PLAINTEXT`, with unauthenticated access |
| `bootstrapBrokerStringTls` (9094) | encryption `TLS` or `TLS_PLAINTEXT`, with unauthenticated or TLS client authentication |
| `bootstrapBrokerStringSaslScram` (9096) | encryption `TLS` or `TLS_PLAINTEXT`, with SASL/SCRAM enabled |
| `bootstrapBrokerStringSaslIam` (9098) | encryption `TLS` or `TLS_PLAINTEXT`, with SASL/IAM enabled |

The encryption default is `TLS`, so a cluster created without `encryptionInfo` answers
`bootstrapBrokerStringTls` and **not** `bootstrapBrokerString`, which is what it used to answer, in a
`broker1.{name}.{region}.kafka.amazonaws.com` form no page uses. A serverless cluster answers
`bootstrapBrokerStringSaslIam` alone, at `boot-{id}.c2.kafka-serverless.{region}.amazonaws.com:9098`.
The other eight members (the public, VPC-connectivity and IPv6 variants) are not answered, since no
connectivity or network type is modelled.

`ListNodes` reports each broker's `endpoints`: its host, the same one `GetBootstrapBrokers` names.
Brokers are spread over the client subnets in order. `addedToClusterTime`, `attachedENIId`,
`clientVpcIpAddress`, `controllerNodeInfo` and `zookeeperNodeInfo` are absent. A serverless cluster
lists no nodes; the page does not address serverless clusters, so this is substrate's reading.

## What a refusal reports

| Condition | Code | Status | `invalidParameter` |
|-----------|------|--------|--------------------|
| a body that will not parse | `BadRequestException` | 400 | omitted |
| a required create member absent, or one outside its published constraint | `BadRequestException` | 400 | the member, e.g. `kafkaVersion`, `clientSubnets` |
| `maxResults` outside 1–100, an unissued `nextToken`, an unknown `clusterTypeFilter` | `BadRequestException` | 400 | the parameter |
| an empty cluster ARN, or one that is not a kafka cluster ARN | `BadRequestException` | 400 | `clusterArn` |
| a cluster ARN that resolves to no cluster | `NotFoundException` | 404 | `clusterArn` |
| a cluster name that already exists | `ConflictException` | 409 | `clusterName` |
| a method and path Substrate does not route | `UnknownOperationException` | 404 | — |

MSK's pages publish an `Error` model of `{"message", "invalidParameter"}`, with **no code member and
no code string** on any page. They do name the error shape each status maps to in the service
model: `BadRequestException` (400), `NotFoundException` (404), `ConflictException` (409). A shape name
is what an SDK matches on, through the `x-amzn-errortype` header, so substrate answers the shape names.
That is its reading, recorded in `emulator/msk_errors.go` ([#1198](https://github.com/scttfrdmn/substrate/issues/1198),
[#1211](https://github.com/scttfrdmn/substrate/issues/1211)). The 400 used to be `BadRequest`, which no shape is called, so a typed SDK
fell through to a generic error.

`invalidParameter`, glossed "The parameter that caused the error.", is the only machine-readable field
an MSK error carries. Every refusal with an offending member now names it in the published lowerCamel
spelling.

The statuses are published rows. The remaining published statuses (401, 403, 429, 500 and 503) have
no site: no credential is validated inside the plugin, no request is throttled, and nothing fails
internally.

## The account and Region a record carries reach no response

`MSKCluster` declares `AccountID`, `Region` and `CreatedAt` under wire-visible `json` tags, because the
record is what `MemoryStateManager` snapshots and a replay reads back. None of them reaches a body.
Every cluster response is rendered through `mskClusterInfoWire` or its V2 counterpart, which copy
published members only, and `CreatedAt` reaches the wire only as the published `creationTime`. That
member is an ISO-8601 string, which is the form the MSK reference documents.
`TestMSKWire_ClusterResponsesCarryNoBookkeepingMember` in `emulator/msk_wire_test.go` drives all nine
routed operations across both API generations
([#756](https://github.com/scttfrdmn/substrate/issues/756)).

## CloudFormation resource types

`AWS::MSK::Cluster` deploys through `POST /v1/clusters` and deletes by path. Five template properties
reach the plugin: `ClusterName` (defaulting to the logical ID), `KafkaVersion` (defaulting to `3.5.1`),
and `BrokerNodeGroupInfo`'s `InstanceType` (defaulting to `kafka.m5.large`) and `ClientSubnets`.
`NumberOfBrokerNodes`, which the resource publishes as "*Required*: Yes", has been read from the
template since #1203; it was hard-coded to `2`. A number or a numeric string is accepted. A template
without it fails the resource. `ClientAuthentication`,
`ConfigurationInfo`, `EncryptionInfo`, `EnhancedMonitoring`, `LoggingInfo`, `OpenMonitoring`,
`Rebalancing`, `StorageMode`, `Tags` and `ZookeeperAccess` are all dropped.

`Ref` returns the cluster ARN, which is what the resource publishes. Of the two published `Fn::GetAtt`
attributes, `Arn` resolves; `CurrentVersion` does not, so a template that reads it to drive an update
gets nothing. Since #1196 the cluster record holds a `currentVersion`, but the deployer does not resolve
the attribute from it.
[#1203](https://github.com/scttfrdmn/substrate/issues/1203).

On `UpdateStack` an unchanged cluster keeps its ARN and no create is sent
([#1427](https://github.com/scttfrdmn/substrate/issues/1427)). A changed `ClusterName`, the one property
the resource marks *Update requires: Replacement*, creates a new cluster and deletes the old one once
the update succeeds. A change to any other property **fails the resource** with a reason naming it, and
the update rolls back: AWS applies those in place through MSK's `Update*` cluster operations ([#1546](https://github.com/scttfrdmn/substrate/issues/1546)), none of
which substrate routes.

## Cost

`CreateCluster` and `CreateClusterV2` are each attributed $0.0002, and `GetBootstrapBrokers`
$0.000001, a broker hour and a request respectively, prorated across a single API call. The two
creates are charged alike because MSK pricing is per broker-hour and does not depend on which API
version created the cluster; before #1202 only the v1 create had an entry, so a cluster created
through the v2 path was free. No other MSK operation is charged, and there is no per-service
fallback. A loaded price list can supply MSK figures, since `AmazonMSK` maps onto this service
([#1202](https://github.com/scttfrdmn/substrate/issues/1202)).
