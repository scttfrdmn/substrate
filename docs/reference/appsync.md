# AppSync

**Endpoint:** `appsync.{region}.amazonaws.com`
**Protocol:** REST/JSON (path and HTTP method, no `X-Amz-Target`)

**Routing:** every operation is identified by its verb and its path, and Substrate matches the path
segment by segment in the spelling the operation's own Request Syntax publishes. That is worth stating
because it was not true until [#1065](https://github.com/scttfrdmn/substrate/issues/1065): the
data-source segment was matched as `DataSources` and the api-key segment as `ApiKeys`, where AWS
publishes `datasources` and `apikeys`. Path matching is case-sensitive, so seven implemented
operations — the five data-source ones and the two api-key ones — were unreachable through the URIs
every AWS SDK builds, and answered `UnknownOperationException`/404 instead.

## Supported operations

Twenty-four operations are routed. The `Route` column is the published requestUri; nothing in the
table is a Substrate spelling.

| Operation | Route | Notes |
|-----------|-------|-------|
| CreateGraphqlApi | `POST /v1/apis` | `name` is required; `authenticationType` defaults to `API_KEY` |
| ListGraphqlApis | `GET /v1/apis` | one page, no cursor |
| GetGraphqlApi | `GET /v1/apis/{apiId}` | |
| UpdateGraphqlApi | `POST /v1/apis/{apiId}` | |
| DeleteGraphqlApi | `DELETE /v1/apis/{apiId}` | **200** with an empty body — see below |
| CreateDataSource | `POST /v1/apis/{apiId}/datasources` | `name` and `type` are required |
| ListDataSources | `GET /v1/apis/{apiId}/datasources` | one page, no cursor |
| GetDataSource | `GET /v1/apis/{apiId}/datasources/{name}` | |
| UpdateDataSource | `POST /v1/apis/{apiId}/datasources/{name}` | merges the members it decodes |
| DeleteDataSource | `DELETE /v1/apis/{apiId}/datasources/{name}` | **200** with an empty body |
| CreateResolver | `POST /v1/apis/{apiId}/types/{typeName}/resolvers` | `fieldName` is required; `kind` defaults to `UNIT` |
| ListResolvers | `GET /v1/apis/{apiId}/types/{typeName}/resolvers` | filtered to the type in the path |
| GetResolver | `GET /v1/apis/{apiId}/types/{typeName}/resolvers/{fieldName}` | |
| UpdateResolver | `POST /v1/apis/{apiId}/types/{typeName}/resolvers/{fieldName}` | |
| DeleteResolver | `DELETE /v1/apis/{apiId}/types/{typeName}/resolvers/{fieldName}` | **200** with an empty body |
| CreateFunction | `POST /v1/apis/{apiId}/functions` | |
| ListFunctions | `GET /v1/apis/{apiId}/functions` | one page, no cursor |
| GetFunction | `GET /v1/apis/{apiId}/functions/{functionId}` | |
| DeleteFunction | `DELETE /v1/apis/{apiId}/functions/{functionId}` | **200** with an empty body |
| CreateApiKey | `POST /v1/apis/{apiId}/apikeys` | `expires` is read and bounded — see *An api key's expiry* |
| ListApiKeys | `GET /v1/apis/{apiId}/apikeys` | one page, no cursor |
| StartSchemaCreation | `POST /v1/apis/{apiId}/schemacreation` | stores the definition, answers `PROCESSING` |
| GetIntrospectionSchema | `GET /v1/apis/{apiId}/schema` | a fixed placeholder schema |
| ExecuteGraphQL | `POST /graphql` | a stub; see *The execution endpoint answers a stub* below |

`UpdateFunction` is the one operation with a routed sibling under the same segment that Substrate does
not implement: `POST /v1/apis/{apiId}/functions/{functionId}` resolves to nothing and is refused. The
same is true of `UpdateApiKey` and `DeleteApiKey` under `apikeys/{id}`, and of every operation AppSync
publishes outside these five segments — the merged-API operations, `AssociateApi`, the
`SourceApiAssociation` family, the tagging trio, `EvaluateMappingTemplate`, and the Event API
operations. All of them answer `UnknownOperationException`/**404**, per *An operation substrate does
not implement* above.

`ExecuteGraphQL` is reachable two ways, because AWS gives it its own host. A request to
`POST /graphql` on the control-plane endpoint resolves to it, and so does any request to a host
containing `.appsync-api.` — the per-API hostname `CreateGraphqlApi` reports back under
`uris.GRAPHQL`. A consumer that takes the URI from the create response and posts to it therefore
reaches the same handler without configuring anything.

## A segment carries its verb and its tail, not just its name

Three arms of the router used to match on the segment name alone, and each of the three was a hazard
of a different size:

- **`apikeys` ignored its tail**, so any `POST` under the segment resolved to `CreateApiKey`. That was
  invisible while the segment was misspelled, and lowercasing it without a gate would have been *worse
  than the defect being fixed*: `UpdateApiKey` is `POST /v1/apis/{apiId}/apikeys/{id}`, so a consumer
  extending a key's expiry would have been answered **200 carrying a second, newly minted
  credential** instead of the 404 an unimplemented operation owes. Only the bare segment reaches
  `CreateApiKey` and `ListApiKeys`.
- **`schemacreation` ignored its verb**, so a `GET` performed a write — it stored the request's
  `definition` and reported `PROCESSING`. AWS publishes `POST` only, and only `POST` reaches it now.
- **`schema` ignored its verb** the same way; AWS publishes `GET`.

The `datasources`, `types` and `functions` arms already checked both, which is why only these three
moved.

## A delete answers 200, not 204

All four delete operations — `DeleteGraphqlApi`, `DeleteDataSource`, `DeleteResolver`,
`DeleteFunction` — open their Response Syntax `HTTP/1.1 200` and state it in words: *"If the action is
successful, the service sends back an HTTP 200 response with an empty HTTP body."* Substrate answered
**204** at all four, on the reasonable but unpublished reading that an empty body is a No Content. The
body stays empty rather than becoming `{}`, because an empty body is what the pages promise and a
member-less object is not the same thing.

Each of those four operations had a test, and each test asserted the 204 — so the tests that existed
to check the operations were pinning the divergence. That is the same shape as CloudFront's `Id` leak
(#1091) and Kinesis's shared describe builder (#1076): a defect survives precisely where a test
records it.

## What a refusal reports

| Condition | Code | Status |
|---|---|---|
| A `Required: Yes` member absent on a create, or `ClusterIdentifier` absent on `ModifyCluster`, `DeleteCluster` or `CreateClusterSnapshot` | `MissingParameter` | 400 |
| `ClusterExists=true` without `ClusterIdentifier` | `MissingParameter` | 400 |
| A value outside its published constraints, a bad `Marker` or an out-of-range `MaxRecords` | `InvalidParameterValue` | 400 |
| `ClusterIdentifier` and `Marker` together on `DescribeClusters` | `InvalidParameterCombination` | 400 |
| Cluster identifier already recorded | `ClusterAlreadyExists` | 400 |
| Parameter-group name already recorded | `ClusterParameterGroupAlreadyExists` | 400 |
| Subnet-group name already recorded | `ClusterSubnetGroupAlreadyExists` | 400 |
| More than twenty subnets | `ClusterSubnetQuotaExceededFault` | 400 |
| Named cluster not recorded | `ClusterNotFound` | 404 |
| Named parameter group not recorded | `ClusterParameterGroupNotFound` | 404 |
| Named subnet group not recorded | `ClusterSubnetGroupNotFoundFault` | 400 |
| Named snapshot not recorded | `ClusterSnapshotNotFound` | 404 |
| No `Action` | `MissingAction` | 400 |
| Any of the other 131 operations | `InvalidParameterValue` | 400 |

`MissingParameter`, `MissingAction`, `InvalidParameterValue` and `InvalidParameterCombination` at 400
are Redshift's own Common Errors entries. Every other row is the code and status its operation page
publishes, spelled as the page spells it: `ClusterSubnetGroupNotFoundFault` carries the `Fault`
suffix and a 400, where the parameter-group, snapshot and cluster faults do not (#1198, #1208).

Published codes with no site in substrate are:

- `ClusterSnapshotAlreadyExists` (400), `InvalidClusterState` (400) and `InvalidTagFault` (400);
- `NumberOfNodesQuotaExceeded` (400), `ClusterQuotaExceeded` (400), `ClusterParameterGroupQuotaExceeded`
  (400) and `ClusterSubnetGroupQuotaExceeded` (400);
- `InsufficientClusterCapacity` (400), `UnsupportedOperation` (400) and `InvalidSubnet` (400).

Because substrate never records a non-`available` status, `InvalidClusterState` in particular is
unreachable by construction rather than merely unimplemented.

## The execution endpoint answers a stub

`POST /graphql` answers `{"data": {}, "errors": null}` for any query. Executing a GraphQL document
against a schema and a resolver chain is running the workload behind the API, not observing the API,
so it sits on the far side of the boundary `doc.go` draws — the same reading that keeps a Lambda's
handler from being executed. The schema a caller uploads through `StartSchemaCreation` is stored as
recorded intent and is not parsed; `GetIntrospectionSchema` answers a fixed placeholder rather than an
introspection of it.

## ARNs are deterministic

| Resource | ARN |
|----------|-----|
| GraphQL API | `arn:aws:appsync:{region}:{account}:apis/{apiId}` |
| Data source | `arn:aws:appsync:{region}:{account}:apis/{apiId}/datasources/{name}` |
| Resolver | `arn:aws:appsync:{region}:{account}:apis/{apiId}/types/{typeName}/resolvers/{fieldName}` |
| Function | `arn:aws:appsync:{region}:{account}:apis/{apiId}/functions/{functionId}` |

An API ID is 13 hex characters and a function or api-key ID is 26, both from the shared random source,
so they differ between runs but are recorded in the event log and reproduce on replay.

## The wire is projected from the state, not handed over

Four AppSync records used to be answered straight out of state, and the persisted shape is not the
published one ([#1121](https://github.com/scttfrdmn/substrate/issues/1121)):

- The `graphqlApi` object spelled its ARN **`apiArn`**, where `API_GraphqlApi` publishes **`arn`** —
  *"The Amazon Resource Name (ARN)"*. `aws.ToString(out.GraphqlApi.Arn)` was therefore `""` with no
  error at `CreateGraphqlApi`, `GetGraphqlApi`, `UpdateGraphqlApi` and `ListGraphqlApis`: a silent
  wrong answer rather than a refusal. The ARN substrate computes was always right; only the member
  name was wrong, and the value is unchanged.
- The same object carried `region` and `accountId`, which the page publishes nowhere. That is
  [#756](https://github.com/scttfrdmn/substrate/issues/756)'s class, whose worked instance is ECR.
- A data source, resolver and function each carried an `apiId`. `API_DataSource`, `API_Resolver` and
  `API_FunctionConfiguration` publish no such member: the API is the path segment the request was
  addressed to, not data the shape carries — the same reading API Gateway v2's `Route` shape records.

Each response is now rendered from a type tagged from the API model and projected from the record
(`appsync_wire.go`), which is the pattern API Gateway v1 (#529), DynamoDB (#1013) and ECR (#1090)
already follow. The records themselves are unchanged, deliberately: they are what `state.Put` writes
and what a replay reads back, so retagging a persisted field in place would make an already-recorded
run decode differently. Members AppSync publishes but substrate does not model are absent from the
projection rather than present and empty.

Because `AWS::AppSync::GraphQLApi`'s `Ref` and `Fn::GetAtt Arn` are read out of the plugin's own
response rather than rebuilt in the deployer, the CloudFormation reader moved with the rename.

## An api key's expiry

`API_CreateApiKey` publishes `expires` as an optional request member and states the default in words:
"From the creation time, the time after which the API key expires. The date is represented as seconds
since the epoch, rounded down to the nearest hour. The default value for this parameter is 7 days from
creation time."

| Request | Answer |
|---------|--------|
| No `expires` | 7 days from the simulated clock, rounded down to the hour |
| `expires` between 1 and 365 days out | that instant, rounded down to the hour |
| `expires` under 1 day or over 365 days out | `ApiKeyValidityOutOfBoundsException`/**400** |

The bound is the one the exception's own message states: "The API key expiration must be set to a
value between 1 and 365 days from creation (for `CreateApiKey`) or from update (for `UpdateApiKey`)."
It is checked against the value the caller sent, with the hour rounding applied afterwards — rounding
first would refuse an `expires` exactly one day out, which the sentence admits, because the rounding
is how the timestamp is represented rather than part of the constraint. AWS publishes no order for the
two.

`deletes` is answered on every key, derived as 60 days past `expires` and rounded the same way, from
`API_ApiKey`'s "Expired API keys are kept for 60 days after the expiration time." It is derived in the
projection rather than persisted, so a key recorded before this behaviour existed still reports the
published value instead of a zero.

Until [#1122](https://github.com/scttfrdmn/substrate/issues/1122), every key expired 365 days out and a
caller's `expires` was decoded nowhere — so a request for a 30-day key was answered with a year-long
one and told it succeeded, and the published refusal had no site to fire from. The `from update` half
of the bound arrives with `UpdateApiKey`, which is unrouted: a `POST` under `apikeys/{id}` answers
`UnknownOperationException`/**404** rather than minting a second credential.

## Known divergences in the wire shape

These are recorded rather than fixed, each with the issue that owns it, so that a consumer reading
this page is not surprised by a member:

- **Tagging is not modelled.** `CreateGraphqlApi` stores a `tags` map and reports it back, but
  `TagResource`, `UntagResource` and `ListTagsForResource` are unrouted and an AppSync ARN is not a
  Resource Groups Tagging resource here — see *Resource Groups Tagging* for the services that are.
- **No listing pages.** Every collection above answers all of its members and no `nextToken`, so a
  caller's pagination loop terminates on the first response. See *The order a listing returns its
  members in* for the tiering that governs which listings are ordered.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::AppSync::GraphQLApi | the API ARN | `Fn::GetAtt ApiId` is the ID; `GraphQLEndpointArn` is empty |
| AWS::AppSync::DataSource | the data source ARN | |
| AWS::AppSync::Resolver | the resolver ARN | physical ID is `TypeName.FieldName` |
| AWS::AppSync::FunctionConfiguration | the function ARN | |

All four `Ref`s are the resource's ARN, which is what each type's Return values section publishes, so a
child resource must name its API by `Fn::GetAtt ["Api", "ApiId"]` and **not** by `Ref` — the `Ref` form
builds `/v1/apis/arn:aws:appsync:…/datasources` and reaches no operation. Substrate answers both
correctly, and its own CloudFormation tests now use the `Fn::GetAtt` form and assert every deployed
resource's error, physical ID and ARN (#1123 — they previously used `Ref`, deployed three resources
that all failed, and passed, because `Deploy` reports a refusal on the resource rather than as a
returned error).

## Cost

AppSync query and mutation operations: **$4.00 per million**, charged on `ExecuteGraphQL` and on
`CreateGraphqlApi`. No other AppSync operation is priced.
