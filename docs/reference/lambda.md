# Lambda

**Endpoint:** `lambda.{region}.amazonaws.com`
**Protocol:** REST/JSON

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateFunction | Stores function metadata; no actual execution; records [`CodeSize` and `CodeSha256`](#what-codesize-and-codesha256-report) from the deployment package, and `TracingConfig.Mode` (`Active` or `PassThrough`) as recorded intent. `Publish: true` publishes version 1 and answers its configuration (#1613) |
| GetFunction | Reports `Code.ImageUri` with `RepositoryType: ECR` for an [image-packaged function](#an-image-packaged-function-reports-its-image), and the function's `Tags` beside `Configuration`, which reports `TracingConfig.Mode` (#1519) and `LastUpdateStatus: Successful`, since every update completes within its request (#1660). Takes a [`Qualifier`](#versions-and-aliases), or a qualified `FunctionName` |
| GetFunctionConfiguration | The `Configuration` member of `GetFunction`: `$LATEST`, or with a [`Qualifier`](#versions-and-aliases) the version's, or the version an alias points at, with the qualified `FunctionArn` (#1660) |
| UpdateFunctionCode | Re-derives [`CodeSize` and `CodeSha256`](#what-codesize-and-codesha256-report) from the new package; an update carrying no package changes neither. `Publish: true` publishes a version from the result |
| UpdateFunctionConfiguration | Also updates `TracingConfig.Mode`. Changes `$LATEST` only; a published version keeps its configuration |
| DeleteFunction | Without a `Qualifier`, deletes the function with all its versions, aliases and their policies. With one, deletes that version; a version an alias routes to is `ResourceConflictException`/409, and an alias or `$LATEST` is `InvalidParameterValueException` |
| ListFunctions | |
| PublishVersion | Snapshots `$LATEST` as the next version number; numbers are never reused. Answers the newest version again when the code and configuration are unchanged since it was published. A stale `RevisionId` is `PreconditionFailedException`/412, and a stale `CodeSha256` `InvalidParameterValueException` (#1613) |
| ListVersionsByFunction | `$LATEST`, with the qualified `FunctionArn`, then the published versions in numeric order. At most 50 per page, as published; `MaxItems` is checked against 1–10000, and a `Marker` this call did not issue for this function is refused (#1613) |
| CreateAlias | `Name`, `FunctionVersion`, `Description` and `RoutingConfig.AdditionalVersionWeights`, validated as the [weighted-alias rules](#versions-and-aliases) publish. An existing alias is `ResourceConflictException`/409, and a version never published `ResourceNotFoundException`/404 |
| GetAlias | `ResourceNotFoundException`/404 for an alias the function does not have |
| UpdateAlias | Each member replaces only what it names; an empty `AdditionalVersionWeights` clears the routing. A stale `RevisionId` is `PreconditionFailedException`/412 |
| DeleteAlias | `204`, also for an alias that does not exist: the page publishes no not-found refusal. Deletes the alias's resource policy too |
| ListAliases | In name order, filtered on `FunctionVersion` (an alias's own version or its weighted one); pages on `MaxItems`/`Marker` as `ListVersionsByFunction` does |
| GetFunctionCodeSigningConfig | An empty `CodeSigningConfigArn`, since substrate models no code-signing configurations. Published under `2020-06-30` (#1519) |
| Invoke | Never runs the function's code. Answers a [seeded error, a seeded result, or the stub](#invoke-answers-a-seed-or-a-stub) `{"statusCode":200,"body":"null"}`, always with `200`, per the reference's *"the status code in the API response doesn't reflect function errors"*. A `Qualifier` or qualified `FunctionName` names a version or alias, and `X-Amz-Executed-Version` reports the version that ran, chosen [deterministically](#versions-and-aliases) for a weighted alias |
| InvokeAsync | Always `202` with `{"Status":202}`; the payload is not stored and nothing is queued. The operation is deprecated in AWS's own reference, and is published under [its own API version date](#each-operation-is-published-under-its-own-api-version-date), `2014-11-13` |
| AddPermission | Adds a statement to the function's resource policy and answers it as `Statement`, a string holding the statement's JSON, as published (#1382); the body is [parsed before the function is looked up](/reference/concepts/errors#whether-a-body-is-parsed-before-the-resource-is-looked-up). `Principal` takes AddPermission's output forms — a service as `{"Service": …}`, an account ID as `{"AWS": "arn:aws:iam::{account}:root"}`, an ARN as `{"AWS": …}`, `*` as `"*"` — and `SourceArn`, `SourceAccount`, `PrincipalOrgID`, `EventSourceToken`, `FunctionUrlAuthType` and `InvokedViaFunctionUrl` become the statement's `Condition` with the keys the developer guide's resource-based policy examples use. A `Qualifier` must name an existing version or alias; the statement goes in that version's or alias's own policy, with the qualified ARN as its `Resource` (#1605, #1613) |
| RemovePermission | Removes the statement by `StatementId`, from the qualifier's policy when a `Qualifier` is given; `204` with no body. An absent function, an absent policy and an unmatched `StatementId` are all `ResourceNotFoundException`/404 |
| GetPolicy | Reports the stored policy as a JSON **string** in `Policy`, as published; with a `Qualifier`, the version's or alias's. A function with no policy is `ResourceNotFoundException`/404, not an empty document |
| PutFunctionEventInvokeConfig | Records `MaximumRetryAttempts` and `MaximumEventAgeInSeconds` as intent; nothing retries, because nothing is invoked asynchronously. The body is parsed before the lookup. Published under [`2019-09-25`](#each-operation-is-published-under-its-own-api-version-date) |
| CreateEventSourceMapping | |
| GetEventSourceMapping | By UUID |
| UpdateEventSourceMapping | `BatchSize` and `Enabled` only; both optional, so an **absent** body is a no-op update rather than a refusal and only a present-but-unparseable body is refused. Toggling `Enabled` starts or stops polling; re-enabling restarts the cadence from now (#1292) |
| DeleteEventSourceMapping | |
| ListEventSourceMappings | Paginates on `MaxItems`/`Marker`; an absent `MaxItems` answers the published per-response cap of 100, a value outside 1–10000 is refused, and a `Marker` substrate did not issue is refused with `InvalidParameterValueException` — see [Two more cursors published and unread](/reference/concepts/listing-order#two-more-cursors-published-and-unread-outside-ec2) |
| TagResource | `204` with no body. Published under [`2017-03-31`](#each-operation-is-published-under-its-own-api-version-date), as the other two tag operations are |
| UntagResource | Takes `tagKeys` as repeated query parameters; `204` with no body |
| ListTags | Reports `Tags` as a map, empty for an untagged function |
| PublishLayerVersion | `201` with the published shape; `Content` is a `ZipFile` or an `S3Bucket`/`S3Key`, sized and hashed like a function package. Version numbers start at 1 and are never reused after a delete. Published under `2018-10-31`, as every layer operation is |
| ListLayerVersions | Newest first; filters on `CompatibleRuntime`/`CompatibleArchitecture`; `MaxItems` 1–50 and a `Marker` substrate issued. **Another account's layer is always `AccessDeniedException`**, since no layer policy can grant this action — see [Another account's layer](#another-account-s-layer) |
| ListLayers | The caller's own layers in its Region, each with its latest version matching the filters; a layer with none matching is not listed |
| GetLayerVersion | The owner gets `ResourceNotFoundException`/404 for a version that does not exist. Another account gets the version only when its policy grants it, and **`AccessDeniedException` otherwise, including for a version that does not exist** |
| GetLayerVersionByArn | `GET /2018-10-31/layers?find=LayerVersion&Arn=…`; the same rules as `GetLayerVersion` |
| DeleteLayerVersion | `204`, also for a version that does not exist: the page publishes no not-found refusal |
| AddLayerVersionPermission | `Action` must be `lambda:GetLayerVersion`, the one value its pattern allows; `Principal` an account ID, a root ARN or `*`. A duplicate `StatementId` is `ResourceConflictException`/409 and a stale `RevisionId` `PreconditionFailedException`/412. `Statement` is a JSON document in a string |
| GetLayerVersionPolicy | The policy document as a string, with its `RevisionId`; a version with no statement is `ResourceNotFoundException`/404 |
| RemoveLayerVersionPermission | `204`; an unmatched `StatementId` is `ResourceNotFoundException`/404, and a stale `RevisionId` is `PreconditionFailedException`/412 |

The row that used to sit here read `InvokeFunction`, which **is not a Lambda API
operation** — the operation is `Invoke`, and `InvokeFunction` is the IAM action name. So
the one `Invoke`-shaped row in the table named something no caller can call, while the
eight operations the plugin actually routes had no row at all (#1015). Rows are now
listed in the router's own order, which is the order a reader checking one against the
other needs.

## Versions and aliases

`PublishVersion` stores an immutable snapshot of `$LATEST`'s code and configuration under the next
number of a per-function counter
([#1613](https://github.com/scttfrdmn/substrate/issues/1613)). The counter never goes backwards, so
a deleted version's number is not reused; deleting the function deletes the counter with its
versions and aliases, and a function recreated under the name starts at 1 again. AWS *"doesn't
publish a version if the function's configuration and code haven't changed since the last
version"*; substrate answers the newest version in that case, which is what a deploy that publishes
on every apply expects. `CreateFunction` and `UpdateFunctionCode` honour `Publish: true` the same
way.

Every operation that takes a `Qualifier` (`GetFunction`, `GetFunctionConfiguration`, `Invoke`,
`AddPermission`, `RemovePermission`, `GetPolicy`, `DeleteFunction`) also accepts the qualifier
appended to `FunctionName` (`my-fn:2`, or a full ARN ending `:live`); the two must agree, or the
request is the `InvalidParameterValueException` the developer guide shows for that case. A number is
a version, `$LATEST` the unpublished code, and any other name an alias, which resolves to its
`FunctionVersion`. A version or alias the function does not have is `ResourceNotFoundException`/404.
Each version and alias has a resource policy of its own, separate from the function's. Both forms
are authorized against the same qualified ARN, so a policy on `function:my-fn:prod` governs
`?Qualifier=prod` and `my-fn:prod` alike. That holds only for the operations that act on the version
or alias a `Qualifier` names (`GetFunction`, `GetFunctionConfiguration`, `Invoke`, `DeleteFunction`
and the three permission operations). Every other operation acts on the function and is authorized
against it whatever `Qualifier` a request carries; `PutFunctionEventInvokeConfig`'s published
`Qualifier` is not modelled. `PublishVersion`, `ListVersionsByFunction` and the alias operations
take the function itself, and refuse a qualified `FunctionName` with `InvalidParameterValueException`.

**A function outside the caller's account and Region is not reached.** A full or partial ARN naming
another account's function is `AccessDeniedException`/403, because substrate does not evaluate a
function's resource policy for a cross-account caller. Cross-account invoke through a resource
policy is not modelled yet ([#1710](https://github.com/scttfrdmn/substrate/issues/1710)). A full ARN
in another Region is `ResourceNotFoundException`/404,
`Functions from 'us-west-2' are not reachable in this region ('us-east-1')` for a `us-west-2` ARN
sent to `us-east-1`, the wording Lambda is observed to answer. The same refusals apply to the tag
operations, to an event source mapping's `FunctionName` on create and update, and to an EventBridge
Scheduler target naming a function, templated or through `aws-sdk:lambda:invoke`, on
`CreateSchedule` and `UpdateSchedule`.

SNS subscriptions, S3 notifications, API Gateway integrations, event source mappings and Step
Functions tasks that name a function ARN invoke that function, under its own account and Region, and
the version or alias it names. One naming another account's function is refused, not redirected to
a same-named function of the caller's.

A weighted alias follows the guide's rules: one additional version at most, and not the alias's own
`FunctionVersion`, a weight from 0.0 to 1.0, both versions published (the alias cannot point at `$LATEST`) and both with the same execution
role. The dead-letter-queue rule is not checked, because substrate models no dead-letter
configuration.

**Weighted routing is deterministic.** AWS *"uses a simple probabilistic model to distribute the
traffic"*. Substrate draws the same proportion from the invoking request's ID mint rather than from
a random source: four bytes derived from the request ID, read as a fraction of 2^32, route to the
additional version when they fall below its weight. Replaying a recorded invocation therefore routes
it to the same version, and over many invocations the shares approach the configured weights. A
weight of `0` never routes to the additional version and a weight of `1` always does, which is how a
test pins one branch.

CloudFormation deploys `AWS::Lambda::Version` through `PublishVersion` (`Ref` is the version ARN,
`Fn::GetAtt` `Version` its number) and `AWS::Lambda::Alias` through `CreateAlias` (`Ref` and
`AliasArn` are the alias ARN), and updates an alias's `FunctionVersion`, `Description` and
`RoutingConfig` in place: the shape the CDK emits for `currentVersion` and an alias over it. A Version
whose function has not changed since its last version cannot adopt that version: the resource fails
with *"A version for this Lambda function exists ( N ). Modify the function to create a new
version."*, the wording CloudFormation is observed to use
([aws/aws-cdk#14428](https://github.com/aws/aws-cdk/issues/14428)), and a replaced Version rolls back
rather than deleting the version its predecessor names.

## Another account's layer

A layer belongs to one account in one Region: the account in the layer ARN when the request names
the layer by ARN, and the caller's own when it names it by name. The owner can do everything.
Another account can do only what the layer version's resource policy grants it, and
`API_AddLayerVersionPermission` constrains `Action` to the single value `lambda:GetLayerVersion`, so
that is all any policy can grant ([#1272](https://github.com/scttfrdmn/substrate/issues/1272)):

| Another account's request | Answer |
|---|---|
| `ListLayerVersions` | Always `AccessDeniedException`/403, even for a public layer |
| `GetLayerVersion`, `GetLayerVersionByArn` on a version the policy grants (to `*`, the caller's account, or its root ARN) | The version |
| The same, on a version the policy does not grant, **or that does not exist** | `AccessDeniedException`/403, not `ResourceNotFoundException` |
| `PublishLayerVersion`, `DeleteLayerVersion`, `GetLayerVersionPolicy` and both permission operations | `AccessDeniedException`/403 |

The refusal reads *"User: {caller} is not authorized to perform: {action} on resource: {ARN} because
no resource-based policy allows the {action} action"*. The caller is the request's principal ARN, or
the account's root ARN when none is resolved.

**Provenance: observed.** Neither `AccessDeniedException` answer is in the operations' published
Errors lists. Both were observed against a real account (#1272): a cross-account `ListLayerVersions`
of `arn:aws:lambda:us-west-2:753240598075:layer:LambdaAdapterLayerArm64` was refused with that
sentence, and `GetLayerVersion` answered versions 1–30 and `AccessDeniedException` for 31–50, a range
that was never published. That is what lets a consumer probe for the newest version, doubling upward
and then binary-searching the boundary, since absent and forbidden are one answer. The code and the
403 are the JSON protocol's authorization answer, the same ones an identity-policy denial gives
(#595). The rule is a resource-policy decision, so it holds whether or not IAM enforcement is on.

A statement scoped to an organization (`OrganizationId`) grants nothing here. Substrate does not
resolve which organization a caller's account belongs to, and refusing is the reading that cannot
over-grant.

### Seeding another account's public layer

`POST /v1/lambda/layer-versions` with `{"layerArn": "arn:aws:lambda:us-west-2:753240598075:layer:LambdaAdapterLayerArm64", "publishedVersions": 30}`
makes versions 1 through 30 of that layer readable, each with a statement granting
`lambda:GetLayerVersion` to `*`, the way a public layer is published. So a consumer's search loop
runs deterministically, without substrate asserting facts about a real account that may age. The
seed is applied at read time, like every other seed. A version the layer's own account published
takes precedence over a seeded one of the same number, and a publish continues the numbering past
the seeded count. `DELETE /v1/lambda/layer-versions?layerArn=…` removes one seed; without the
parameter it removes all of them. The write is recorded, so a replay re-applies it in position
(#1140).

## Each operation is published under its own API version date

A query-protocol service carries one `Version` parameter for the whole API. A REST service
puts the version in the path, and Lambda dates each operation's URI at the version that
operation was introduced — it has never renumbered. Four dates appear among the operations
substrate routes, each read from that operation's own published Request Syntax:

| API version | Operations |
|---|---|
| `2014-11-13` | `InvokeAsync` |
| `2015-03-31` | the function CRUD, `Invoke`, the resource policy, the event source mappings |
| `2017-03-31` | `TagResource`, `UntagResource`, `ListTags` |
| `2019-09-25` | `PutFunctionEventInvokeConfig` |
| `2020-06-30` | `GetFunctionCodeSigningConfig` |

Until #1142 substrate routed one of the four. The parser reached its arms by trimming the
literal prefix `/2015-03-31`, so a path under any other date kept its version segment,
matched nothing, and answered `UnknownOperationException`/404 — which is what
`lambda.TagResource` got from an SDK against a function substrate had just created, even
though the handler behind it merges tags and saves the function. Three operations were
unreachable in a plugin that implements them, and `invoke-async` and `event-invoke-config`
were reachable only at a date AWS does not serve.

**A request under an undocumented date is still refused.** The version is compared against
the one the operation publishes rather than stripped, so `/2015-03-31/tags/{Resource}` — a
path no AWS SDK emits and AWS itself does not serve — answers the same 404 it always did.
Accepting it would make substrate the only implementation that does, which hides the defect
rather than reporting it: a consumer hand-building the URI would pass here and fail at
deployment.

The same resolution decides more than routing, because the parser is also what names the
operation for authorization, metering and fault injection. `lambda:TagResource` resolved to
`Unknown`, so an IAM policy naming the action could neither allow nor deny it and a seeded
fault on it could not fire. The resource half moved with it: a tags request names its
resource as a whole ARN in the path, and that ARN is what the request is authorized against
— reassembling one from the caller's own account and Region, which a function-path request
must do because it carries only a name, would silently retarget a cross-account ARN at the
caller's own function of that name.

## What CodeSize and CodeSha256 report

`CodeSize` is "the size of the function's deployment package, in bytes" and
`CodeSha256` is "the SHA256 hash of the function's deployment package". What
substrate can report depends on whether it holds the package:

| Code source | `CodeSize` | `CodeSha256` |
|---|---|---|
| `Code.ZipFile` (inline, base64) | the decoded package's length | the real SHA256 of those bytes |
| `Code.S3Bucket` + `Code.S3Key` | the S3 object's recorded length | the object's **ETag**, not a SHA256 — see below |
| `Code.S3ObjectVersion` | that version's length | that version's ETag |
| `Code.ImageUri` | 0 — an image is not a package substrate holds | a digest of the image URI |
| no `Code` at all | 0 | empty |

Two of these are substrate's own decisions rather than the API model:

**The digest of an S3-sourced package is the object's ETag.** Substrate does not
fetch the object's bytes — nothing executes them unless they arrived inline — so it
cannot compute the SHA256 real Lambda would report. It reports the ETag instead,
unquoted. For a single-part upload that ETag is the MD5 of the body, so it changes
exactly when the package changes, which is what a caller comparing digests across
deploys is asking. Do not assert that it equals a SHA256 you computed yourself; do
assert that it changes when you upload different bytes and does not when you do not.
An image-packaged function's digest is likewise derived from its URI.

**An absent S3 object does not fail the create.** Real Lambda refuses a
`CreateFunction` naming an object that is not there. Substrate accepts it, logs a
warning, and reports `CodeSize: 0` with no digest. The reason is that substrate's S3
and Lambda state are independent and a template may legitimately name an object a
test never uploaded; failing the create would make a stack undeployable for a reason
unrelated to what the test is checking. A deleted object — one hidden by a delete
marker — counts as absent, not as a zero-length package.

`CodeSize` and `CodeSha256` describe the package. `RevisionId` does not: it is not a
digest of anything and advances on every `UpdateFunctionCode`, including one that
changes no code.

## An image-packaged function reports its image

`Code.ImageUri` is the documented spelling and implies `PackageType: Image`, which a
request need not state — real Lambda rejects an `ImageUri` alongside
`PackageType: Zip`, so inferring it cannot contradict a valid request. A top-level
`ImageUri` is also accepted, for compatibility with what substrate took before
`Code.ImageUri` worked; `Code.ImageUri` wins when both are sent.

`GetFunction` reports such a function through `Code.RepositoryType: ECR` with
`ImageUri` and `ResolvedImageUri`, and no `Location` — `RepositoryType` is "the
service that's hosting the file", and reporting a presigned S3 URL for an image is a
claim a caller can act on and be wrong about.

## An event-source mapping polls on the simulated clock

An enabled SQS event-source mapping consumes its queue: it receives a batch, invokes the function
with it, and deletes the batch if the invocation succeeded. A failed invocation leaves the batch on
the queue until its visibility timeout passes, which is how a real mapping retries it. Substrate does
not run the function's code (see [Invoke answers a seed or a stub](#invoke-answers-a-seed-or-a-stub)); the
observable effect is the queue's.

Until #1292 a goroutine drove each mapping from a one-second **wall-clock** ticker. A frozen or scaled
clock did not affect it, an ESM's effect depended on how long a test happened to run, and nothing it
did was recorded, so a replay of a stream in which a mapping consumed a queue replayed with the queue
still full.

Polling is now evaluated at observation, on the simulated clock:

- **When a poll runs.** A mapping's first poll is due one simulated second after it is created or
  enabled, then one per simulated second. A poll that is due runs before the next top-level request
  reaches its plugin, whatever that request is. A test observes the queue, and the poll that came
  due ran first. AWS publishes no polling cadence; one second is the interval the old poller used.
- **A frozen clock polls never.** Nothing comes due, however many requests arrive. A test that
  freezes the clock moves it with `AdvanceTime` to let a poll run, and the cadence follows `SetScale`,
  because it is measured in simulated time.
- **A gap runs its due polls in order** at the one clock reading the request carries, until one
  receives nothing. Every later poll in the gap would see the same queue at the same instant. At most
  100 polls of one mapping run per request; any beyond that stay due for the next.
- **Disabling or deleting a mapping stops it.** Re-enabling restarts the cadence from now, so time
  that passed while it was disabled does not count.

**The decision on replay: the dispatches are recorded, and a replay re-derives them.** Each poll's
receive, invoke and deletes are recorded as events with request IDs prefixed `req-clock-`, so the event
log shows what a mapping consumed and invoked. Each ID is derived from the triggering request's ID,
the mapping and the poll's index. A replay pins the clock to each recorded event's timestamp and runs
the same step before routing it, so the same polls come due at the same request, under the same IDs,
and leave the same state. The recorded copies are then skipped (`SkippedEvents`), because executing
them as well would apply every dispatch twice. Exact reproduction requires what every recorded
timestamp requires: a frozen clock during the recording.

The polls run on the server's request path and on replay, not on internal dispatch paths (the
CloudFormation deployer's and an API Gateway proxy integration's). Those are nested inside a
top-level request that already ran them.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::Lambda::Function | FunctionName | `Code` is deployed in every form; [inline `ZipFile` is zipped](#an-inline-zipfile-is-zipped-into-a-package). `Fn::GetAtt Arn` answers; `SnapStartResponse` and its members are refused with the reason, because SnapStart is not modeled ([#1591](https://github.com/scttfrdmn/substrate/issues/1591)) |
| AWS::Lambda::EventSourceMapping | — | |
| AWS::Lambda::Permission | statement ID | `AddPermission` on the function `FunctionName` names (a name or an ARN, with a version or alias qualifier sent as `Qualifier`); the statement ID is generated per stack, as CloudFormation generates it, and every property is Replacement, so a replacement adds a statement with a new ID before the old one is removed. `Fn::GetAtt Id` answers. Deleted through `RemovePermission`, with the same `Qualifier` (#1605) |

## A function name is unique per account per Region

Two accounts can each hold a function named `orders`, and one account can hold one in
`us-east-1` and another in `us-west-2`. Until #943 the second create in either pair answered
`ResourceConflictException`/409, because the function's state key carried only its name. See
[A Lambda function and a DynamoDB table belong to one account in one Region](/reference/tagging#a-lambda-function-and-a-dynamodb-table-belong-to-one-account-in-one-region)
for what AWS does and does not publish about that scope, and for the three sibling keys — the
resource policy, the stored zip and the event-invoke configuration — that moved with it.

## An inline ZipFile is zipped into a package

The resource type's `Code.ZipFile` and the API's `Code.ZipFile` are not the same
thing, which is the one place the deployer does more than forward a property.

The resource type's is "the source code of your Lambda function": "CloudFormation
places it in a file named `index` and zips it to create a deployment package". The
API's is "the base64-encoded contents of the deployment package". So substrate builds
the archive CloudFormation would have built — a single entry named `index` with the
extension the runtime reads (`.js` for `nodejs*`, `.py` for `python*`, bare `index`
otherwise) — and sends that. `CodeSize` is therefore the **archive's** length, not the
source's, and `CodeSha256` is a digest of real bytes.

The archive carries no timestamps, so the same template deployed twice produces the
same package and the same digest. A digest that changed on every deploy would not be
worth comparing.

`Code.S3Bucket`, `Code.S3Key`, `Code.S3ObjectVersion` and `Code.ImageUri` are
forwarded as the API spells them, and each goes through the template's `Ref` and
pseudo-parameter resolution like every other property.

## Invoke answers a seed or a stub

Substrate never runs a function's code. Running a handler is the workload behind the API rather
than an observation through it, so per the scope rule it is out of scope. `Invoke` instead answers
a seed or the stub:

- **A seeded error**, through `POST`/`DELETE /v1/lambda/invoke-error`. The response carries
  `X-Amz-Function-Error` (`Handled` or `Unhandled`, default `Unhandled`) and an error payload, or the
  seed's own `payload` verbatim.
- **A seeded result**, through `POST`/`DELETE /v1/lambda/invoke-result`. The seed's `payload`, any
  JSON value, is returned verbatim with no `X-Amz-Function-Error`.
- **The stub** `{"statusCode":200,"body":"null"}`, with no `X-Amz-Function-Error`, when no seed
  applies.

A seed for the function's own name outranks a `"*"` seed of either kind, and at the same scope an
error outranks a result. The first that exists answers, in this order:

1. the error seeded for the function;
2. the result seeded for the function;
3. the `"*"` error;
4. the `"*"` result.

So a wildcard failure does not mask a result seeded for one function.

```
POST   /v1/lambda/invoke-result   {"functionName","payload"}
DELETE /v1/lambda/invoke-result?functionName=<name>   (or with no query, clear all)
```

Both seeds are keyed by function name, or by `"*"` for any function. `functionName` defaults to
`"*"`, and in a POST or DELETE it takes the forms `Invoke` does: a name, a qualified name
(`resize:v1`), or a full or partial ARN, all keyed by the bare name. `payload` is required. Clearing result seeds
leaves error seeds in place, and the reverse. Every path that invokes a function reads the same
seeds, including an event-source mapping's poll, a Step Functions task, a Scheduler target and an
API Gateway proxy integration.

```console
$ curl -X POST http://localhost:4566/v1/lambda/invoke-result \
    -d '{"functionName":"resize","payload":{"statusCode":200,"body":"{\"width\":640}"}}'
```

The deployment package is still stored. Its bytes are what `GetFunction`'s `CodeSha256` and
`CodeSize` report, and an `UpdateFunctionCode` changes them. A timeout firing, a handler exception or
an `Environment` variable reaching the code are not modeled; seed the outcome each would produce.

## The tag bookkeeping a function carries reaches no response

`LambdaFunction` declares `ever_tagged`, the flag that records a function was once tagged (#938), because the record is what `MemoryStateManager` snapshots and a replay reads back. It does not reach a body: every response is a configuration built member by member.
`TestLambdaWire_FunctionResponsesCarryNoBookkeepingMember` in `emulator/lambda_wire_test.go` tags the function and reads the flag back first, since an unset `omitempty` member is absent for free, then drives every operation that answers the function and walks each decoded document for it at any depth
([#756](https://github.com/scttfrdmn/substrate/issues/756)). `TagResource` answers 204 with no body, so it has nothing to walk.

## A new role is not assumable for a seeded number of attempts

IAM is eventually consistent, so a role is not assumable by Lambda for a short while after
`CreateRole` returns. A `CreateFunction` naming it in that window is refused
`InvalidParameterValueException`/400 with "The role defined for the function cannot be assumed by
Lambda." The sentence is observed on a real account ([#1274](https://github.com/scttfrdmn/substrate/issues/1274));
no page publishes it, though `API_CreateFunction` publishes the code at 400. A deployer that creates
its own roles has to retry exactly that error, so the window is seedable, on the shared
[progression](/reference/concepts/seeds-and-replay#how-a-progression-is-seeded):

```
POST   /v1/lambda/role-propagation   {"roleArn": "arn:aws:iam::123456789012:role/r", "refusedAttempts": 2}
DELETE /v1/lambda/role-propagation   (all seeds; ?roleArn=… for one)
```

- **What counts as an attempt:** each `CreateFunction` naming the role, and each
  `UpdateFunctionConfiguration` naming it as the new `Role`. Each spends one attempt of that role's
  own counter. With `refusedAttempts` *n*, the first *n* attempts are refused and the next is admitted.
- **The wildcard:** no `roleArn` (or `"*"`) governs every role, each with its own counter (#582).
- **When counting starts:** at the seed, not at `CreateRole`, because the IAM and Lambda plugins share
  no record of when a role was created. A test posts the seed, creates the role, and retries. A seed's
  POST restarts the counters it governs.
- **What spends nothing:** a request refused for another reason first (a body that does not parse, a
  missing name, an existing function), and an update that names no `Role`.
- **The default:** unseeded, a role is assumable the moment it exists, as before. The seed is a
  control-plane write, so a replay re-issues it and reproduces the refusals.

## Cost

Lambda invocations: $0.0000002 per request.
