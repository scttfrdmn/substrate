# Request routing

How substrate decides which plugin serves a request, and what it answers for an operation
no plugin routes.

## How a request reaches a plugin

A plugin that is registered is not necessarily reachable. Substrate resolves which
plugin serves a request by reducing what the client sent to one lowercase service
name, using four signals in priority order:

| # | Signal | Example | Notes |
|---|--------|---------|-------|
| 1 | `X-Amz-Target` namespace | `DynamoDB_20120810.GetItem` | The only signal a `--endpoint-url` caller supplies beyond the credential scope |
| 2 | `Host` | `dynamodb.us-east-1.amazonaws.com` | Useless against substrate, where the host is `localhost` |
| 3 | URL path | `/service/GraniteServiceVersion20100801/operation/GetMetricData` | Smithy RPC v2 CBOR transport |
| 4 | SigV4 credential scope | `…/us-east-1/dynamodb/aws4_request` | The signing name, which is not always the endpoint prefix |

The order is what makes a routing bug hard to see. The target is checked first, so an
unrecognized namespace **short-circuits** signals 2 and 4, both of which would often
have answered correctly. Substrate is reached by pointing a client at
`--endpoint-url http://localhost:4566`, where signal 2 is `localhost` and signal 3 is
absent — so for a JSON service the target is effectively the only signal, and a
namespace substrate does not recognize answers `ServiceNotAvailable` for every
operation of that service while the plugin's own tests stay green.

That has happened repeatedly: `sso` (#561), `organizations` and `config` (#580),
`eventbridge` (#734), and then `monitoring`, `health`, `cloudtrail` and `timestream`
(#739). Each was found by pointing a real client at a running server, not by the test
suite.

### The routing table

`emulator/routing.go` records, for every registered plugin, the identifiers a real AWS
client sends: the target namespace, one example endpoint host per distinct shape, and
the SigV4 signing names. Every row cites the source it was read from, and the citation
names *which* source — see CloudTrail below for why that matters.

The table has three consumers, so it cannot rot quietly:

- the generated [coverage matrix](/services#coverage-matrix) reads its display names and protocols;
- `make docs-reference-check` fails if a registered plugin has no row, or a row names
  no registered plugin;
- a sweep test drives every identifier in every row through the parser and asserts the
  result is a **registered** plugin — not merely the expected string.

Adding a plugin therefore means recording how a client addresses it. That is the check
whose absence let four plugins ship unreachable.

### The four that were unreachable

| Plugin | What the client sends | What it reduced to | Who saw the failure |
|---|---|---|---|
| `monitoring` | `X-Amz-Target: GraniteServiceVersion20100801.{Op}` | nothing — the name was in the Smithy path table and absent from the target table | every AWS CLI and boto3 caller |
| `health` | `X-Amz-Target: AWSHealth_20160804.{Op}` | nothing — the table held an invented `healthservice` instead | every SDK |
| `health` | `Host: global.health.amazonaws.com` | `global` | a caller using the real endpoint |
| `cloudtrail` | `X-Amz-Target: com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.{Op}` | `com` | the AWS CLI and boto3 only |
| `timestream` | `Host: ingest.` / `query.timestream.{region}.amazonaws.com` | `ingest` / `query` | a caller using endpoint discovery |

Two of these are worth understanding rather than just recording.

**CloudTrail is why a source citation must name the SDK.** botocore sends the model's
fully-qualified namespace; `aws-sdk-go-v2` sends the terse `CloudTrail_20131101`. The
Go SDK's form always worked, so substrate's suite and its real-SDK end-to-end tier —
both of which drive `aws-sdk-go-v2` — were green while every CLI and boto3 CloudTrail
call answered `ServiceNotAvailable`. An alias could not fix the long form, because its
first label is `com`, which prefixes every fully-qualified namespace and would hijack
any other service that adopted one. Substrate now reduces a dot-qualified namespace to
its **last** segment, mirroring what operation extraction already did. Of the 430
botocore models only three carry a dotted prefix — `cloudtrail`, `codeconnections` and
`codestar-connections`, the latter two not substrate plugins — and their last segments
do not collide.

**CloudWatch is now drivable three ways, and that closes the gap this section used to
carve out.** Its service shape declares `awsQuery`, `awsJson1_0`, `rpcv2Cbor` **and**
`awsQueryCompatible` at once, and its clients disagree about which to use: `aws-sdk-go-v2`
posts Smithy RPC v2 CBOR, the AWS CLI and boto3 post `awsJson1_0` under an `X-Amz-Target`,
and a hand-rolled client posts a query form. Substrate serves all three (#785) — a request
is normalized into query parameters before dispatch and the reply rendered from one ordered
document — so the ten operations answer bytes each client can deserialize, and since #757
a *refusal* is shaped the same way: a CBOR caller gets a CBOR error naming the modeled
shape in `__type`, a JSON-RPC caller gets a JSON one, a Query caller gets `<ErrorResponse>`,
and a caller that sent `X-Amzn-Query-Mode: true` also gets the Query code back in an
`x-amzn-query-error` header. The plugin previously answered XML on every protocol, so
`aws-sdk-go-v2` reported `deserialization failed, expected map for struct, got major type 1`
— `0x3C`, the leading `<`, read as a CBOR major type — and the AWS CLI printed nothing at
all. Substrate's own suite was green over it, because every CloudWatch test posted a form
body and read the XML back as a string; see [Protocols](/reference/monitoring#protocols) under CloudWatch for
what each client now gets.

### Plugins that are deliberately not addressable three ways

Three rows cannot assert all three signals, and say so in prose rather than being
skipped:

- **`apigatewayv2`** shares `apigateway`'s endpoint host *and* signing name, and neither
  client sends a target. The two are separated by the `/v2/` path segment instead (#529).
- **`execute-api`** is addressed by a host whose first label is the API's ID — data, not
  a service name — so the match is on the `execute-api` label. There is no service model;
  this is the runtime endpoint, which no SDK-generated client calls.
- **`opensearch`** is data-plane only. A managed domain's host and a Serverless
  collection's host both carry the resource name ahead of the `es`/`aoss` label, and the
  OpenSearch Serverless control-plane target prefix names operations substrate does not
  implement, so it is absent on purpose. The name in the host is not a routing key, but it
  does scope the plugin's state, and a control-plane path (`/2021-01-01/…`,
  `/2015-01-01/…`) that arrives on an `es` host is refused `UnknownOperationException`
  rather than routed (#1200, #1212).

### Coverage limits

All 67 rows were read from the botocore models bundled in a locally installed AWS CLI
v2. The JSON ones were cross-checked against `aws-sdk-go-v2` serializers. The Java,
JavaScript and .NET SDKs were **not** checked, so a namespace only they send would not
have been caught here. Where a per-service section below states a target prefix, that
prefix is what the model declares; substrate also accepts the older spellings it used
to document, so correcting one of them cannot break a caller.

## An operation substrate does not implement

No plugin covers its whole service, so every one of the 67 has an answer for a call it
does not serve. That answer is decided by the **Protocol** column above and by nothing
else — not by which plugin the call reached, and not by who wrote it.

| Protocol | Code | HTTP | Message |
|---|---|---|---|
| JSON, REST/JSON | `UnknownOperationException` | **404** | `The action <name> is not recognized.` |
| Query, `ec2`, REST/XML | `InvalidAction` | 400 | `The action <name> is not valid for this endpoint.` |

`<name>` is the operation name for a service that dispatches on one, and the HTTP verb
and path — `POST /substrate-no-such-path` — for a REST service whose router matched
nothing. A REST operation is identified by the pair, so both halves are reported:
`DELETE` and `POST` on one path are different operations, and the same verb on a
mistyped path is the more common mistake.

The name is always present. It is what lets a consumer tell *substrate has not
implemented this call* from *this call was rejected*, which is the difference that
decides whether to change the test or the code under test.

### Where the two rows come from

`UnknownOperationException` at 404 is AWS's, quoted from its Common Errors pages:
"The action or operation isn't recognized. Verify that the action name is spelled
correctly and that it's supported by the API version you're using." Both JSON families
publish it and both publish the 404 — checked on
[DynamoDB's](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html)
for JSON and
[Lambda's](https://docs.aws.amazon.com/lambda/latest/api/CommonErrors.html)
for REST/JSON.

`InvalidAction` at 400 **is** a citation, which corrects the reading this row carried
when #716 wrote it. The pages AWS has regenerated publish no unknown-action code for the
Query, `ec2` and REST/XML families — the eighteen-entry Query list is byte-identical on
EC2, RDS, IAM, ELB and CloudFormation, and the `InvalidAction` entry it once carried is
gone from all five. But [#1064](https://github.com/scttfrdmn/substrate/issues/1064) found
it is not gone from the reference: **SQS's Common Errors page is the one Query-family list
AWS has not regenerated, and it still publishes `InvalidAction` at exactly this 400.**

The citation is one step removed, and the row says so rather than overclaiming: substrate
routes `sqs` itself as a JSON-RPC service, so no substrate endpoint reaches this arm for
SQS. What SQS's page establishes is that the Query family's code for this condition is
still published, not that any one service answers it. See
[the three generations](/reference/concepts/errors#the-reference-has-three-common-errors-generations-not-two).

### Two things this is not

It is **not** a Go type name. Until
[#716](https://github.com/scttfrdmn/substrate/issues/716) every plugin wrote this
refusal itself — 59 sites in eight wordings, each leading with the plugin's Go type
("`SSMPlugin: unknown operation "Foo"`"). No AWS endpoint emits that, so a consumer's
error branch was matching on a substrate internal. A tripwire test now sweeps every
plugin and fails if any answer contains the word `Plugin`.

It is **not** `InvalidAction` on a JSON service. Forty-four of those 59 sites answered the
Query protocol's code at 400 on a service whose protocol is JSON, which is what the
table above corrects. **A test asserting `InvalidAction` or a 400 for an unimplemented
call on a JSON or REST/JSON service needs both the code and the status from the first
row** — see the release that landed #716 in `CHANGELOG.md` for the upgrade note. The
change is visible for every service in the [matrix](/services#coverage-matrix) except the nine Query, `ec2` and
REST/XML ones.

One shape difference survives on purpose: IAM answers a refusal as a 4xx response
document rather than as an error object. Both conventions are in use across the tree
([#516](https://github.com/scttfrdmn/substrate/issues/516) is about that), so #716
changed where IAM's code, message and status come from and left the shape alone.
