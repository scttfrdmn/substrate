# SSM Parameter Store

**Endpoint:** `ssm.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AmazonSSM.{Op}`)

## Supported operations

| Operation | Notes |
|-----------|-------|
| PutParameter | Supports String, StringList, SecureString types; normalizes `Name` to a leading `/`. A name prefixed with `aws` or `ssm`, in any case and with or without the slash, is `ValidationException`: *"A parameter name can't be prefixed with "aws" or "ssm" (case-insensitive)"* (#1606) |
| GetParameter | Supports WithDecryption; reports the parameter's ARN |
| GetParameters | Batch get |
| GetParametersByPath | Recursive path traversal; paginates on `MaxResults`/`NextToken` (`MaxResults` 1–10, default 10, refused outside [the published range](/reference/concepts/listing-order#eight-json-list-operations-refuse-a-page-size-outside-the-published-range)) and refuses a `NextToken` substrate did not issue with `InvalidNextToken` — see [A pagination token substrate never issued](/reference/concepts/listing-order#a-pagination-token-substrate-never-issued-is-refused-not-answered-with-page-one) |
| GetParameterHistory | |
| DeleteParameter | |
| DeleteParameters | |
| DescribeParameters | Paginates on `MaxResults`/`NextToken` (`MaxResults` 1–50, default 10, refused outside [the published range](/reference/concepts/listing-order#eight-json-list-operations-refuse-a-page-size-outside-the-published-range)); refuses a `NextToken` substrate did not issue with `InvalidNextToken` — see [A pagination token substrate never issued](/reference/concepts/listing-order#a-pagination-token-substrate-never-issued-is-refused-not-answered-with-page-one) |
| AddTagsToResource | `ResourceType` is required and is resolved — see below |
| RemoveTagsFromResource | Removes only the named keys |
| ListTagsForResource | Reports `TagList` sorted by key; an empty list, never `null` |
| LabelParameterVersion | Accepted; always reports `ParameterVersion: 1` |
| SendCommand | Run Command; records the intent — substrate does not execute the command. The `CommandId` is the published fixed length of 36, [derived from the request ID](/reference/concepts/seeds-and-replay#an-identifier-a-replay-mints-is-the-one-it-recorded) ([#856](https://github.com/scttfrdmn/substrate/issues/856)) |
| GetCommandInvocation | |
| DescribeInstanceInformation | |

## How a `(ResourceType, ResourceId)` pair resolves

`ResourceType` is `Required: Yes` at all three tag operations, over the ten-value enum
AWS publishes. Substrate reads it (#932); it previously decoded the member and ignored it,
so any type resolved to the same-named Parameter Store parameter:

| Request | Answer |
|---|---|
| `ResourceType: "Parameter"`, `ResourceId: "/db/password"` | The parameter `/db/password` |
| `ResourceType: "Parameter"`, `ResourceId: "db/password"` | The same parameter — the leading `/` is supplied |
| `ResourceType: "Parameter"`, `ResourceId: "arn:aws:ssm:…"` | `InvalidResourceId`/400, not normalized into a name (#928) |
| `ResourceType: "Document"` and the eight other published types | `InvalidResourceId`/400 — the type is real, the resource is not modelled here |
| `ResourceType: "parameter"`, `"Parameters"`, or anything outside the enum | `InvalidResourceType`/400, listing the ten valid values |
| `ResourceType` absent | `ValidationException`/400 |

`ResourceId` is the parameter **name**, not an ARN: AWS states *"For the `Document` and
`Parameter` values, use the name of the resource"*. It therefore carries no account or
Region, and the caller's own supply them — the one arm in the tagging set where that is
correct rather than a defect. The Resource Groups Tagging API's arm for the same parameter
takes an ARN and takes account and Region from it; both arms build the same state key, so
the two cannot disagree about which parameter an identifier names. See
[Resource Groups Tagging](/reference/tagging).

The leading-slash tolerance is substrate's reading, not AWS's: AWS documents it only for
`OpsMetadata`. Substrate needs it because `PutParameter` normalizes `Name` to a leading
`/`, so a caller who created `MyParam` must be able to tag `MyParam`.

`InvalidResourceId` is answered at HTTP **400**, the status all three reference pages give
it: *"The resource ID isn't valid. Verify that you entered the correct ID and try again.
HTTP Status Code: 400"*. Systems Manager publishes no distinct not-found code for these
operations — `InvalidResourceId` *is* how a nonexistent resource is reported — and no 404
at all, so the status is not a thing a caller can branch on and the code is the whole
signal. Substrate models no tag-count cap here, where AWS caps most resources at 50 tags
and Automations at 5.

**A body that will not parse answers `ValidationError`/400**, at all twelve operations
that decode one. Two of the twelve — `SendCommand` and `GetCommandInvocation` — answered
`SerializationException` with `encoding/json`'s own error text as the message, so a caller
was told about the emulator's decoder and the Go type it was unmarshalling into; the other
ten answered `InvalidRequest`. Neither code appears anywhere in Systems Manager's
documentation. All twelve operation pages were read and none publishes an error for a
request that could not be read at all, so the code comes from the common-errors page — see
*A request body that will not parse* above for why, and for the two published near misses
that are not the answer (#950).

## AWS's public AMI parameters are answered

A `/aws/service/…` path whose shape says "AMI" is answered without anyone having
put it there, because that is how AWS documents AMI discovery — a template or a
script reads `/aws/service/ami-amazon-linux-latest/…` rather than hardcoding an ID.
The value is the AMI ID substrate resolves for that parameter in the request's
region, and it is the *same* ID `RunInstances` accepts: the two answers come from
one table, so `GetParameter` cannot hand out an AMI the launch then refuses. See
[Which AMIs resolve](/reference/ec2#which-amis-resolve) for the paths and the derivation.

A parameter the caller wrote wins over a managed one at the same path, so
`PutParameter` can override any of these.

## The account and Region a record carries reach no response

`SSMParameter` declares `AccountID` and `Region` under wire-visible `json` tags, plus `EverTagged` as
`ever_tagged,omitempty`, because the record is what `MemoryStateManager` snapshots and a replay reads
back. None of them reaches a body. The parameter reads answer items built member by member, and the
writes answer maps of published members.
`TestSSMWire_ParameterResponsesCarryNoBookkeepingMember` in `emulator/ssm_wire_test.go` drives every
operation that answers a parameter: the reads, the history, labels, the tag operations and both
deletes ([#756](https://github.com/scttfrdmn/substrate/issues/756)). The parameter is tagged first
and `ever_tagged` is read back, since only a tag write sets it (#938).

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::SSM::Parameter | ParameterName | |

## Cost

SSM standard parameters are free. Advanced parameters: $0.05 per 10,000 API interactions.
