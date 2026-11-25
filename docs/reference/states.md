# Step Functions

**Endpoint:** `states.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AWSStepFunctions.{Op}`)

`AmazonStates` was documented here previously and is not what any client sends;
the Step Functions model's target prefix is `AWSStepFunctions` (#739). Substrate
routes both.

**A custom endpoint needs host-prefix injection off for `StartSyncExecution`.** The SDKs prepend `sync-` to the host, so turn it off in the client (boto3: `Config(inject_host_prefix=False)`); see [Operations whose SDK call adds a host prefix](concepts/routing.md#operations-whose-sdk-call-adds-a-host-prefix).

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateStateMachine | `tags` is an array of `{key, value}` objects; the ARN is minted from the caller's account and Region; the definition must be a JSON object and a structurally valid state machine — see below (#996, #1073); `name`, `roleArn` and `type` are checked against their published constraints, and a repeat is **idempotent** — see below (#1072) |
| DescribeStateMachine | Addressed by ARN — see below |
| UpdateStateMachine | Addressed by ARN; a supplied definition is checked the same way `CreateStateMachine` checks one (#996, #1073) |
| DeleteStateMachine | Addressed by ARN; **idempotent** — an ARN naming nothing is a `200`; synchronous, so no `DELETING` status is observable (#995). Answers `{}`; the page publishes an empty body (#1206) |
| ListStateMachines | Scoped to the caller's own account and Region; `maxResults` 0 is the default of 100, and outside [the published range](/reference/concepts/listing-order#eight-json-list-operations-refuse-a-page-size-outside-the-published-range) is refused. [A `nextToken` it did not issue is `InvalidToken`](#list-tokens-are-issued-not-names) |
| StartExecution | Returns RUNNING status immediately; the execution ARN is minted in the **state machine's** account and Region |
| StartSyncExecution | EXPRESS only — a `STANDARD` state machine is `StateMachineTypeNotSupported`/400 (#996); the express execution ARN is minted in the state machine's account and Region, and no record is stored for it; twelve of the fourteen published response members are answered — see below (#1071) |
| DescribeExecution | Addressed by ARN. The execution runs to a terminal status at `StartExecution`, so this reports rather than advances it; `error` and `cause` are answered on a failed execution — see below (#1071) |
| StopExecution | Addressed by ARN |
| ListExecutions | Exactly one of `stateMachineArn` or `mapRunArn` — see below; `maxResults` 0 is the default of 100, and outside [the published range](/reference/concepts/listing-order#eight-json-list-operations-refuse-a-page-size-outside-the-published-range) is refused. [A `nextToken` it did not issue is `InvalidToken`](#list-tokens-are-issued-not-names) |
| GetExecutionHistory | Addressed by ARN. Each event's `timestamp` is epoch seconds and its `type` a published per-state value (#1323) |
| CreateActivity | `tags` is an array of `{key, value}` objects; the ARN is minted from the caller's account and Region; `name` is checked against the same published constraints as `CreateStateMachine`'s, and a repeat is **idempotent** on the name alone — see below (#1072) |
| DescribeActivity | Addressed by ARN — see below |
| ListActivities | Scoped to the caller's own account and Region; `maxResults` 0 is the default of 100, and outside [the published range](/reference/concepts/listing-order#eight-json-list-operations-refuse-a-page-size-outside-the-published-range) is refused. [A `nextToken` it did not issue is `InvalidToken`](#list-tokens-are-issued-not-names) |
| DeleteActivity | Addressed by ARN; **idempotent** — an ARN naming nothing is a `200` (#995). Answers `{}`; the page publishes an empty body (#1206) |
| TagResource | State machine or activity — see below. Answers `{}`, the published empty body (#1206) |
| UntagResource | State machine or activity — see below. Answers `{}`, the published empty body (#1206) |
| ListTagsForResource | `tags` sorted by key — see below |

## An ARN addresses the resource it names, at every operation

**The account, the Region, the resource type and the name all come from the ARN,
never from the calling request.** Every operation that takes an ARN resolves it
through one parser, which takes no request context at all — so the guarantee is
structural rather than something each of the fourteen call sites has to remember.
That is the arrangement ECS has had since #826, and it is the rule #826
established for SQS and DynamoDB and #845 carried across the tagging API's
resolver.

| Resource | ARN | State key |
|----------|-----|-----------|
| State machine | `arn:aws:states:{region}:{account}:stateMachine:{name}` | `statemachine:{account}/{region}/{name}` |
| Activity | `arn:aws:states:{region}:{account}:activity:{name}` | `activity:{account}/{region}/{name}` |
| Execution | `arn:aws:states:{region}:{account}:execution:{stateMachine}:{execution}` | `execution:{account}/{region}/{stateMachine}/{execution}` |
| Express execution | `arn:aws:states:{region}:{account}:express:{stateMachine}:{execution}` | none — no record is kept |

The tagging operations were audited against the rule in #910 and the other eleven
in #912. `TagResource`, `UntagResource` and `ListTagsForResource` each describe
`resourceArn` as "the Amazon Resource Name (ARN) for the Step Functions state
machine or activity", so those two are the whole **taggable** set; the execution
rows above are addressable by the execution operations only.

Before #910 and #912 every one of the fourteen took the resource *name* from the
ARN's last colon-separated segment and the account and Region from the caller's
own request context. Three separate things followed:

1. **An ARN naming another account's or another Region's resource reached the
   caller's own same-named one.** `DescribeStateMachine` disclosed it,
   `UpdateStateMachine` rewrote it, `DeleteStateMachine` and `DeleteActivity`
   removed it, `StopExecution` aborted it, `UntagResource` stripped its tags —
   every one answering `200`. `UntagResource` and `StopExecution` are the
   damaging directions: stripping a tag can turn an `aws:ResourceTag` `Deny` into
   an allow, and aborting the wrong execution destroys work.
2. **The resource-type segment was never read.** The tagging check was
   `strings.Contains(arn, ":stateMachine:")`, a substring test over the whole ARN,
   so a *name* carrying that text satisfied it as readily as a type segment did:
   `arn:aws:states:{region}:{account}:activity:x:stateMachine:y` took the
   state-machine branch. The other eleven did not check the type at all, so an
   activity ARN at `DescribeStateMachine` looked for a state machine named after
   the activity, and an execution ARN there looked for one named after the
   *execution*.
3. **An execution ARN's two names were reconstructed by stripping one segment**,
   which is right only for an ARN of exactly that arity.

The type comparison is exact and **case-sensitive**, because AWS distinguishes
these resources by the literal segment alone — `stateMachine` with a capital M
against `activity` — so `statemachine:orders` is refused rather than treated as
the same resource.

**A well-formed ARN of the wrong type answers `InvalidArn`.** An execution ARN at
a tagging or state-machine operation, an activity ARN at a state-machine
operation, a state-machine ARN at an execution operation: the resource may well
exist, and it is the ARN that does not belong at this operation. An execution ARN
answered `InvalidArn` at the tagging operations before #910 too, but by falling
off the end of the `strings.Contains` chain rather than by a decision.

**A version or alias ARN is refused.** `arn:aws:states:{region}:{account}:stateMachine:orders:1`
and `…:stateMachine:orders:live` are both well-formed at AWS and both name
something substrate keeps no record of — state machine versions and aliases are
not modelled. Resolving either to the unqualified state machine would hand a
caller a different resource from the one it asked for, which is the whole defect
this rule exists to end, so both answer `InvalidArn`. This is substrate's
reading: AWS refuses neither shape.

**An express execution ARN resolves to nothing rather than being refused for its
shape.** `StartSyncExecution` mints one and stores no record, since an express
execution completes within the call, so such an ARN answers
`ExecutionDoesNotExist` at the execution operations. Refusing it for its shape
would claim AWS rejects an ARN it mints. A trailing extra segment is tolerated in
an express ARN for the same reason.

**A Task state's `Resource` is not a Step Functions ARN and is not parsed as
one.** It is Lambda's ARN, and the segment wanted is the one after `function:`,
which is a different job — so it has its own reader (#912). The same
last-segment extraction used to run here: a qualified ARN such as
`arn:aws:lambda:{region}:{account}:function:score:PROD` invoked a function named
after the *alias*, and `arn:aws:states:::lambda:invoke` — the optimized
integration, which also contains `:lambda:` and so passed the old dispatch test —
invoked one named `invoke`. A qualifier is now dropped rather than honored,
because the executor invokes through Lambda's unqualified path; invoking a
specific version or alias from a Task state is not modelled. An optimized
integration is not dispatched to Lambda at all and returns the empty-object stub.

## Every refusal answers its own code at 400

| Code | Status | When |
|------|--------|------|
| InvalidArn | 400 | The ARN is malformed, names another service, names a type the operation does not accept, or carries a version or alias qualifier |
| StateMachineDoesNotExist | 400 | A well-formed state-machine ARN names a state machine that does not exist |
| ActivityDoesNotExist | 400 | A well-formed activity ARN names an activity that does not exist |
| ExecutionDoesNotExist | 400 | A well-formed execution ARN names an execution that does not exist, including any express execution ARN |
| ResourceNotFound | 400 | The tagging operations' code for a state machine or activity that does not exist, and `ListExecutions`' answer for a `mapRunArn` |
| StateMachineTypeNotSupported | 400 | `StartSyncExecution` against a `STANDARD` state machine (#996), or a `CreateStateMachine` `type` outside the published `STANDARD`/`EXPRESS` (#1072) |
| InvalidDefinition | 400 | `CreateStateMachine` or `UpdateStateMachine` was given a definition substrate could not read back (#996), or one that reads back and is not a structurally valid state machine (#1073) — see below |
| InvalidName | 400 | A `CreateStateMachine` or `CreateActivity` `name` that is absent or breaks the published constraints — see below (#1072) |
| StateMachineAlreadyExists | 400 | `CreateStateMachine` against a name held by a state machine with a different definition or type — see below (#1072) |
| ValidationError | 400 | The request body is not valid JSON, at all fifteen operations that decode one — the common error, for the reasons in *A request body that will not parse* above (#950) |

**Every one of these is 400, not 404.** All eleven Step Functions API reference
pages consulted for #910 and #912 publish every error at "HTTP Status Code: 400",
including the three `*DoesNotExist` codes — unusual enough to be worth stating,
because substrate answered 404 for all four before #910 and #912, which no Step
Functions endpoint returns. A consumer branching on the status rather than the
code saw something AWS never sends.

**And not 409 either.** Two create handlers survived those sweeps at 409 rather
than 404 and were corrected in #1072: `CreateStateMachine`'s
`StateMachineAlreadyExists`, which `API_CreateStateMachine` publishes at 400, and
`CreateActivity`'s `ActivityAlreadyExists`, which is now answered only for its
published condition (see below). Both handlers also answered `InvalidParameterException` for an
absent `name` — a code on neither page's Errors list, where
`API_CreateStateMachine` publishes fifteen and `API_CreateActivity` seven.

Two codes those pages publish at a status other than 400 are **not** in the table
above and are not answered anywhere, which is deliberate rather than an omission:
`API_CreateStateMachine` publishes `ConflictException` at 409 *and* at 400 — it is
listed twice, at two statuses, on the same page — and `API_UpdateStateMachine`
publishes `ServiceQuotaExceededException` at 402. Both describe concurrency and
quota conditions substrate does not model, so neither has a site to be answered
from.

The code a resource's absence carries is the one **its own operation's page
publishes**, which is why there are four rather than one:
`StateMachineDoesNotExist` at `DescribeStateMachine`, `UpdateStateMachine`,
`StartExecution`, `StartSyncExecution` and `ListExecutions`;
`ActivityDoesNotExist` at `DescribeActivity`; `ExecutionDoesNotExist` at
`DescribeExecution`, `StopExecution` and `GetExecutionHistory`; and
`ResourceNotFound` at the three tagging operations. The two deletes used to answer
a code their own pages do **not** publish, and no longer do — see *The two deletes
are idempotent* below. `StartSyncExecution`'s refusal of a `STANDARD` state
machine used to answer an unpublished code too; see *StartSyncExecution refuses a
workflow type, not a definition* (#996).

## The two creates are idempotent, and check what their pages constrain

Both create operations are published as idempotent, and substrate refused a repeat
unconditionally until #1072. The published Notes differ in what they key on, and
substrate follows each page rather than generalising one to both.

`CreateStateMachine`:

> `CreateStateMachine` is an idempotent API. Subsequent requests won't create a
> duplicate resource if it was already created. `CreateStateMachine`'s idempotency
> check is based on the state machine `name`, `definition`, `type`,
> `LoggingConfiguration`, `TracingConfiguration`, and `EncryptionConfiguration`
> The check is also based on the `publish` and `versionDescription` parameters. If
> a following request has a different `roleArn` or `tags`, Step Functions will
> ignore these differences and treat it as an idempotent request of the previous.
> In this case, `roleArn` and `tags` will not be updated, even if they are
> different.

`CreateActivity`:

> `CreateActivity` is an idempotent API. Subsequent requests won't create a
> duplicate resource if it was already created. `CreateActivity`'s idempotency
> check is based on the activity `name`. If a following request has different
> `tags` values, Step Functions will ignore these differences and treat it as an
> idempotent request of the previous. In this case, `tags` will not be updated,
> even if they are different.

So a repeat answers the stored record's own ARN and `creationDate`, byte for byte
with the first call's response, and leaves `roleArn` and `tags` alone. Of the
eight inputs to `CreateStateMachine`'s check, substrate compares the three it
models — `name`, `definition` and `type`. The other five are request members no
handler in this plugin decodes, so they cannot differ between two requests
substrate has seen.

**Where the page contradicts itself, the Note governs.**
`StateMachineAlreadyExists` is glossed *"A state machine with the same name but a
different definition **or role ARN** already exists"*, which would make a differing
`roleArn` a refusal — while the Note excludes `roleArn` from the check twice and
says outright that it "will not be updated, even if [it is] different". The Note is
the more specific statement, so substrate treats a repeat differing only in
`roleArn` or `tags` as the idempotent success and refuses only a differing
`definition`, `type`, `loggingConfiguration`, `tracingConfiguration` or
`encryptionConfiguration` — the last three since #1199, which made them recorded
rather than discarded. They are compared as JSON values, so reordered keys are not
a difference. That choice is substrate's reading of a page that states
both things.

**`ActivityAlreadyExists` answers only its published condition.** That condition
is its own gloss — *"Activity already exists. `EncryptionConfiguration` may not be
updated."* — so a repeat `CreateActivity` that keeps the activity's
`encryptionConfiguration` is the idempotent success, and one that changes it is
`ActivityAlreadyExists`/400. Until [#1199](https://github.com/scttfrdmn/substrate/issues/1199) `encryptionConfiguration` was a
request member substrate did not decode, so no input reached the refusal at all.
Substrate earlier still answered it at 409 for a plain duplicate name, which was
neither the published status nor the published condition.

Three published constraints are now checked, each answering a code its own page
publishes:

| Member | Published constraint | Refusal |
|--------|----------------------|---------|
| `name` (both operations) | `Required: Yes`; 1–80 characters; no white space; none of the brackets, wildcards and special characters the page lists — `<>{}[]`, `?*`, and `"#%\^~$&,;:/` together with the pipe and the backtick; no control characters (`U+0000-001F`, `U+007F-009F`, `U+FFFE-FFFF`); no surrogates (`U+D800-DFFF`); not `U+10FFFF` | `InvalidName`/400 |
| `roleArn` (`CreateStateMachine`) | `Required: Yes`; 1–256 characters | `InvalidArn`/400 |
| `type` (`CreateStateMachine`) | Valid Values `STANDARD` or `EXPRESS`, defaulting to `STANDARD` | `StateMachineTypeNotSupported`/400 |

`InvalidName` rather than `ValidationException` for a bad name, because it is the
only one of the two published on **both** pages: `CreateStateMachine` publishes
`ValidationException` and `CreateActivity` does not, so answering that would report
a code `CreateActivity`'s page does not publish and would make one plugin answer
two codes for one failure. `InvalidName` is also the narrower fit — every
constraint checked is a name constraint.

Two clauses of the name list are recorded rather than enforced, both for stated
reasons. The surrogate range `U+D800-DFFF` cannot be reached: Go's JSON decoder
substitutes `U+FFFD` for an unpaired surrogate escape and for any byte sequence
that is not valid UTF-8, so no request can carry one as a surrogate. And the
page's further sentence — *"To enable logging with CloudWatch Logs, the name should
only contain 0-9, A-Z, a-z, - and \_"* — is a condition on logging stated with
"should", not a constraint on the name, and substrate models no logging
configuration for it to interact with; enforcing it would refuse names AWS accepts.

`roleArn` is checked for presence, for the published length, and for an `arn:`
prefix — and no further. The page publishes **no Pattern** for the member, so
splitting the ARN into its six fields and refusing a value that does not name an
IAM role would invent a validation AWS does not document; that is the same line
CloudFormation's own `RoleARN` draws. A role that does not exist is deliberately
not an error either: no code is published for it and substrate does not resolve the
role at create time.

## The two deletes are idempotent

`DeleteStateMachine` on a state machine that is not there, and `DeleteActivity` on
an activity that is not there, both answer `200` with an empty body. Neither
refuses (#995).

Until then both answered a `*DoesNotExist` code, because both went through the
same lookup helper as their siblings and the code came along with the lookup. The
codes are real — `StateMachineDoesNotExist` is published at
`DescribeStateMachine`, `UpdateStateMachine`, `StartExecution`,
`StartSyncExecution` and `ListExecutions`, and `ActivityDoesNotExist` at
`DescribeActivity` — just not at these two operations.
`API_DeleteStateMachine` publishes exactly two errors, `InvalidArn`/400 and
`ValidationException`/400. `API_DeleteActivity` publishes exactly one,
`InvalidArn`/400.

**Reading that omission as idempotence is substrate's reading.** It is weaker
evidence than SNS's `DeleteTopic`, whose page states the property outright —
here neither page says anything in either direction. Two things carry it. The
error list is the only thing either page says on the matter, and `CommonErrors`
frames a page's list as the errors the operation returns. And the page's own
description makes an idempotent delete the behaviour a caller needs:

> Deletes a state machine. This is an asynchronous operation. It sets the state
> machine's status to `DELETING` and begins the deletion process. A state machine
> is deleted only when all its executions are completed.

A caller that has issued a delete and retries cannot distinguish *already gone*
from *still `DELETING`*, which is the situation an idempotent delete exists for.
The contrary reading — that the list is incomplete and AWS does refuse — rests on
nothing either page says, only on the code existing elsewhere in the service. The
two operations answer alike because an absent state machine and an absent activity
cannot sensibly disagree about whether a delete is idempotent.

**Only the deletes changed.** `DescribeStateMachine` and `DescribeActivity` still
refuse an absent resource with the code their own pages publish, so the change is
about which operations may be idempotent, not about whether absence is observable.

**Idempotence licenses an ARN that names nothing, not a string that is not the
right kind of ARN.** The parse stays ahead of the load in both handlers, so a
malformed ARN, a non-`states` ARN, an ARN of the wrong resource type, and a
version- or alias-qualified ARN are all still `InvalidArn`/400 at both deletes.

The cost of the `200` is that the status no longer proves a delete declined to
act: a cross-account or cross-Region ARN now answers `200` whether or not it
touched anything. So the tests assert the caller's own same-named resource
survives, which is the #912 guarantee restated where it is no longer implied by a
refusal.

## The DELETING status is not modelled

`API_DescribeStateMachine` publishes two status values, `ACTIVE` and `DELETING`.
Substrate reports `ACTIVE` and nothing else, and `StateMachineState.Status`
claimed both until #995 looked for the writer that set `DELETING` and found none.

Nothing can set it: the delete removes the record synchronously, so there is no
observation between `ACTIVE` and gone for a `DELETING` to occupy. Two consequences
follow, and both are stated rather than hidden. A consumer cannot exercise a poll
loop that waits for a delete to finish — the second `DescribeStateMachine` answers
`StateMachineDoesNotExist` rather than reporting `DELETING`. And
`StateMachineDeleting`/400 — *"The specified state machine is being deleted.
Execution will not be started or updated."*, published at `UpdateStateMachine`,
`StartExecution` and `StartSyncExecution` — is unreachable for the same reason.

Modelling the transition is a separate piece of work, and per substrate's scope it
would be driven by the simulated clock or by a countdown of observations rather
than by wall-clock time. A test pins the current answer, so whoever models it
finds a failing assertion naming this decision rather than a silent widening.

## StartSyncExecution refuses a workflow type, not a definition

`StartSyncExecution` against a `STANDARD` state machine answers
`StateMachineTypeNotSupported`/400, *"State machine type is not supported."*, with
the rejected type appended. It answered `InvalidDefinition` before #996 —
a real Step Functions code, published at `CreateStateMachine` and
`UpdateStateMachine` where a definition arrives in the request, but not on this
page and not about this fact. `API_StartSyncExecution` publishes nine errors, all
400, and states the restriction outright: *"`StartSyncExecution` is not available
for `STANDARD` workflows."* A consumer branching on the code was told the ASL
document was wrong when what was wrong was the workflow type.

The page says **nothing** about an endpoint host. At AWS this operation is served
on a `sync-` prefixed host, and that fact comes from the endpoints reference rather
than from this page; **substrate does not model it** and serves
`StartSyncExecution` on the same `states.{region}.amazonaws.com` endpoint as every
other operation. A consumer whose client is configured against the AWS `sync-`
host will not reach substrate.

## DescribeStateMachine and DescribeActivity answer their configurations

`DescribeStateMachine` answered seven of the fourteen members `API_DescribeStateMachine`
publishes, and `DescribeActivity` three of `API_DescribeActivity`'s four. Since
[#1199](https://github.com/scttfrdmn/substrate/issues/1199) `CreateStateMachine`, `UpdateStateMachine` and `CreateActivity` record the
configuration objects they are sent, and the describes answer them:

| Member | Answered |
|--------|----------|
| `loggingConfiguration` | As sent, or the published default `{"level":"OFF","includeExecutionData":false}` ("By default, the `level` is set to `OFF`") |
| `tracingConfiguration` | As sent, or `{"enabled":false}` |
| `encryptionConfiguration` | As sent, or `{"type":"AWS_OWNED_KEY"}`, the AWS-owned key a resource created with none is encrypted with. On `DescribeActivity` too |
| `revisionId` | A new value at every create and every `UpdateStateMachine`, which also answers it, as its page publishes. Two describes with no update between them answer the same one, so a caller can compare configurations "without performing a diff", which is what the page says the member is for. A state machine recorded before #1199 has none until its next update |

A configuration that is not a JSON object is refused with its own published code at 400:
`InvalidLoggingConfiguration`, `InvalidTracingConfiguration` or
`InvalidEncryptionConfiguration`. The objects' members are not validated further: nothing logs,
traces or encrypts, so the configuration is recorded intent, reported back verbatim.

Three members stay absent, because no state machine substrate holds has a value for them:
- **`description`:** "the description of the state machine version", and versions are not modeled.
- **`label`:** identifies a Distributed Map state named by a qualified ARN, which is not modeled
  either.
- **`variableReferences`:** maps each state to the variables it references. Substrate does not
  derive it, and a definition with no `Assign` or JSONata variables has none.

## A failed execution reports why it failed

`StartSyncExecution` answered five of the fourteen members its Response Syntax
publishes, and `DescribeExecution` seven of its twenty-one. Four of the gaps were
the same four on both pages — `error`, `cause`, `inputDetails` and `outputDetails`
— and the first two are the ones that mattered, because a failed execution's
reason was recorded and then reported nowhere (#1071).

On `StartSyncExecution` that is the whole of the observable failure, since AWS
publishes the operation's contract as *"`StartSyncExecution` will return a `200 OK`
response, even if your execution fails, because the status code in the API response
doesn't reflect function errors."* A consumer testing a failure path saw
`"status":"FAILED"` and nothing else.

| Member | When it is answered |
|--------|---------------------|
| `error` | Only on a failed execution — the ASL error code, from a `Fail` state's `Error` or from the runtime |
| `cause` | Only on a failed execution — the explanation, from a `Fail` state's `Cause` |
| `input` / `inputDetails` | Whenever the execution carries input |
| `output` / `outputDetails` | Only on a succeeded execution, per the published rule that `output` *"is set only if the execution succeeds"* |
| `name`, `stateMachineArn` | Always, on `StartSyncExecution`, which omitted both |

`error` and `cause` are **absent** rather than empty on a succeeded execution: the
page gives no meaning to an empty `error`. `inputDetails` and `outputDetails` are
`CloudWatchEventsExecutionDataDetails` objects carrying `included: true`, which is
the page's own value — *"Always `true` for API calls."* — and not a derivation.
**Pairing each details member with the payload member it describes is substrate's
reading**: neither page states when they are present, and details about a payload
the body does not carry would describe nothing.

`status` on `StartSyncExecution` is one of the narrower three the operation
publishes, `SUCCEEDED | FAILED | TIMED_OUT`, where `DescribeExecution` publishes
six.

**Two published members stay unreported, deliberately.** `billingDetails` reports
the metering of a workload substrate does not run: `billedMemoryUsedInMB` is memory
consumed inside the execution, which is resource-internal, and
`billedDurationInMilliseconds` measured on the simulated clock would be `0` for a
sync execution that completes inside one handler — a duration AWS would never
return. `traceHeader` echoes a request member no path decodes, and the page
publishes a precedence rule for it (the `X-Amzn-Trace-Id` header wins over the
body), so answering it means modeling X-Ray's header rather than adding a field.

## A definition that cannot be read back, or cannot run, is refused when it is stored

`CreateStateMachine` and `UpdateStateMachine` answer `InvalidDefinition`/400,
*"The provided Amazon States Language definition is not valid."*, for a definition
that is empty, is not valid JSON, or does not describe a JSON object — and, since
#1073, for one that reads back perfectly and still does not name a runnable state
machine. Every message names the offending state and field, because a caller
fixing a generated document cannot act on *"the definition is not valid"*.

Neither operation checked the definition at all before #996: both stored whatever
string arrived. So substrate could accept a definition, report `200`, and then be
unable to execute it — and both execution operations reported that as a caller
error. `StartSyncExecution` answered `InvalidDefinition` for it, which is the same
unpublished code as above at the same operation, and `StartExecution` failed the
execution with the error name `InvalidDefinition`, which is an API error code and
not an Amazon States Language error name at all.

The CloudFormation deployer was a concrete producer of exactly that, not a
hypothetical one. `DefinitionString` is a CloudFormation **string** property, and
the deployer marshalled it to JSON, which quotes and escapes a string that is
already the document — so every `AWS::StepFunctions::StateMachine` deployed from a
`DefinitionString` stored a JSON string literal rather than an ASL object, and its
executions failed for a reason the template author could do nothing about. Fixed in
the same change; the sibling object-valued `Definition` property was still not read
at all until #1074, which is below.

Both residual paths are now unreachable, and both are stated rather than removed.
In `StartSyncExecution` an unreadable stored definition is a `500` through Go's
error return rather than an `AWSError`, because it means substrate wrote something
it cannot read — or replayed an event log written before the validation — and that
is not the caller's fault. In `StartExecution` it stays an execution-level failure,
which is the shape an asynchronous start has to use, but the error name is now
`States.Runtime`, the published ASL name for an execution that failed due to an
exception that could not be processed.

**The structural rules: what a definition has to name before it is stored.** #996
asked only whether substrate could read the document back, so `{}` created a state
machine and answered `200`. #1073 added the rules below, each one a sentence AWS
publishes on `amazon-states-language-state-machine-structure.html`, the `Choice`
page, the `Parallel` page or the inline-`Map` page:

| Rule | AWS's words |
|------|-------------|
| `States` is present, and is not an empty object | *"States (Required) An object containing a comma-delimited set of states"* — absent and `{}` answer different messages |
| `StartAt` is present and names a member of `States` | *"StartAt (Required) A string that must exactly match (is case sensitive) the name of one of the state objects"* |
| Every state has a `Type`, and it is one of the published eight | `Pass`, `Task`, `Choice`, `Wait`, `Succeed`, `Fail`, `Parallel`, `Map` |
| `End` is refused on `Choice`, `Succeed` and `Fail` | *"Some state types, such as Choice, or terminal states, such as Succeed workflow state and Fail workflow state, don't support or use the End field"* |
| Every other type carries exactly one of `Next` or `End` | *"Only one of Next or End can be used in a state"* |
| A `Choice` state carries no `Next` of its own | *"Choice states do not support the End field. In addition, they use Next only inside their Choices field"* |
| A `Choice` state has at least one `Choices` rule | *"Choices (Required) … You must define at least one rule in the Choice state"* |
| Every top-level Choice Rule has a `Next`, and `Default` resolves when present | *"Default (Optional, Recommended) The name of the state to transition to if no Choice Rule evaluates to true"* |
| A `Next` nested inside `And`, `Or` or `Not` is refused where it stands | *"the Next field can appear only in a top-level Choice Rule"* — not resolved and accepted; refused for being nested |
| `"And": []` and `"Or": []` are refused, while an absent operator is fine | *"The values of the And and Or operators must be non-empty arrays of Choice Rules"* |
| Every `Catch` entry has a `Next` | a Catcher with no `Next` has nowhere to send the error |
| A `Parallel` state has `Branches`, and each branch is checked by these same rules | *"Each such state machine object must have fields named States and StartAt, whose meanings are exactly like those in the top level of a state machine"* |
| A `Map` state carries exactly one of `ItemProcessor` or `Iterator` | `ItemProcessor` is marked *"(Required)"*; `Iterator` is under *"Deprecated fields"* and still accepted |
| Every `Next`, `Catch[].Next`, Choice Rule `Next` and `Default` names a state in **its own** `States` object | *"Each branch must be self-contained"* (`Parallel`); *"States within the ItemProcessor field can only transition to each other"* (`Map`) |

A definition with two faults reports the same one on every run: state names are
walked in sorted order, because a map range would make the refusal a coin flip and
the event log unreplayable.

**Both spellings of a `Map` state's sub-state-machine are accepted, and both now
run.** AWS says *"The ItemProcessor field replaces the now deprecated Iterator
field"* and, on the same page, *"Step Functions Local doesn't currently support the
ItemProcessor field. We recommend that you use the Iterator field with Step
Functions Local."* Refusing either spelling would reject a document AWS accepts
from exactly the class of tool substrate is, so the rule is *exactly one of the
two*. Carrying both is refused: they name one field and nothing publishes a
precedence. Before #1073 the executor read `Iterator` alone, so a `Map` written with
`ItemProcessor` — the spelling AWS marks Required — iterated zero times and returned
an empty array instead of failing.

**What is still not checked, and a `200` here is still not ASL approval.** The rules
above are structural: a rule exists only where a published AWS sentence makes the
document malformed regardless of any input. Left unvalidated, deliberately:

- **A field a state type does not support.** `ASLState` is one flat struct carrying
  every type's members at once, so an `InputPath` on a `Succeed` state, or a
  `Seconds` on a `Pass` state, is invisible to the validator. The three cases the
  pages name outright — `End` on `Choice`, `Succeed` and `Fail` — are checked,
  because those are the ones a generator actually emits.
- **A `Next` on a `Succeed` or `Fail` state.** Both are terminal, so the transition
  can never be taken, but the published sentence covers only `End`. Substrate
  accepts it rather than borrowing a rule AWS does not state (#671).
- **Everything that depends on the execution.** Whether a Choice Rule's comparison
  can ever be true, whether a `Task`'s `Resource` ARN names anything, whether a
  JSONPath resolves against the state's input, whether a `Retry` interval is
  sensible — all of those are answers about a run, not about the document, and sit
  on the workload-internal side of substrate's scope boundary.
- **Field-level rules inside a state.** Each state type's own page carries
  requirements about that type's own members — a `Wait` state's alternative timing
  fields, a `Fail` state's `Error`/`Cause` pair, a `Map` state's `MaxConcurrency`.
  Those are validation of a field's value rather than of the state machine's shape,
  and none of them is enforced.
- **The definition-size quota.** The service-quotas page publishes *"Maximum size of
  state machine definition — 1 MB — Hard quota"*; substrate does not measure it, and
  neither page publishes an error code for exceeding it.

This is also where `API_StartSyncExecution` itself draws the line, which is worth
recording because it is the only guidance either page gives: *"Error codes are
reserved for errors that prevent your execution from running, such as permissions
errors, limit errors, or issues with your state machine code and configuration."*
An unreadable definition is an issue with the state machine's code, so AWS puts it
on the error-code side — but publishes no code for it, because AWS would never have
stored such a definition in the first place. Checking on the way in is the only
reading that leaves both sides consistent.

`ListTagsForResource` returns `tags` sorted by key. AWS documents no order for
it; lexicographic is substrate's reading, justified by the replay promise — a
member order that followed Go's map iteration would differ between two identical
calls in one run and could not replay from the event log (#862).

## ListExecutions takes exactly one of its two ARNs

`stateMachineArn` is "Required: No" at `ListExecutions`, and the page states:
"You can specify either a `mapRunArn` or a `stateMachineArn`, but not both."
Substrate answers all four cases:

| Input | Answer |
|-------|--------|
| `stateMachineArn` alone | The state machine's executions |
| `mapRunArn` alone | `ResourceNotFound` / 400 |
| Both | `ValidationException` / 400 |
| Neither | `ValidationException` / 400 |

The `mapRunArn` row is **substrate's reading**: a Map Run is not modelled — no
operation mints one — so the ARN names a resource substrate keeps no record of,
and `ResourceNotFound` is published on this page. The alternative, refusing it as
unsupported, would need a code the page does not carry.

**An absent state machine is a refusal, not an empty list.** `ListExecutions` had
no existence check before #912, so a `stateMachineArn` naming nothing answered
`200` with `executions: []` — indistinguishable from a state machine that exists
and has never run, which is the one pair a consumer polling for executions cannot
tell apart. It now answers `StateMachineDoesNotExist` / 400, which the page
publishes. A state machine that exists with no executions still answers `200`
with an empty list.

## Tags are an array of objects, not an object

AWS's `Tag` shape is `{"key": …, "value": …}`, and both `TagResource`'s request
and `ListTagsForResource`'s response carry an **array** of them:

```json
{"tags": [{"key": "env", "value": "test"}]}
```

Substrate rendered and accepted an *object* at those two operations while
`CreateStateMachine` and `CreateActivity` in the same plugin already took the
array — so the plugin disagreed with itself about the wire shape of its own tags:
a tag set at create time could not be read back in a shape any SDK decodes, and
`TagResource` could not be called by one at all. Both now use AWS's array (#910).

## The account and Region a record carries reach no response

Each of the three persisted records — `StateMachineState`, `ExecutionState` and
`ActivityState` — declares `AccountID` and `Region` under wire-visible `json`
tags, and the two taggable ones also declare `EverTagged` as `ever_tagged`,
because the record is what `MemoryStateManager` snapshots and a replay reads back.
No Step Functions shape publishes any of the three, and none reaches a body: every
one of the eighteen routed operations answers through `statesJSONResponse` with
either a hand-built map, one of the two named projections `smToMap` and
`execToMap`, or a list entry struct declared inside its own handler
(`ListStateMachines`, `ListExecutions` and `ListActivities` each have one), and
`ListTagsForResource` decodes the stored record into a one-member struct and
answers only its tags. Every `json.Marshal` of a record in the plugin is in a
`save*` helper writing to state.

All eighteen are driven by
`TestStepFunctionsWire_StateMachineResponsesCarryNoBookkeepingMember` and its two
siblings in `emulator/stepfunctions_wire_test.go`, which walk the decoded document
and fail on a member named for any of the three at any depth — so the eight
entries this discharges in `scripts/wire-bookkeeping-baseline.txt` stay listed as
declarations rather than as leaks
([#756](https://github.com/scttfrdmn/substrate/issues/756)). Two details make the
walk worth making. The assertion is on the member *name*, because the account and
the Region appear in every body as segments of a `states` ARN, which is published.
And `ever_tagged` is `,omitempty` and is set by `TagResource` alone — never by
create-with-tags (#938) — so the state-machine and activity tests tag the resource
and assert the flag is set on the stored record *before* asserting it is absent
from a response; without that step the absence would hold of a record that never
carried the member, which is the vacuous form #1304 shipped on EFS.

Six of the eighteen carry nothing a projection could leak and are driven anyway:
`UpdateStateMachine` answers only `updateDate`, `StopExecution` only `stopDate`,
and `DeleteStateMachine`, `DeleteActivity`, `TagResource` and `UntagResource` an
empty object — none reads a member off the record it addresses. They are listed so
the set reads as complete rather than as a sample.

## Every date is epoch seconds, history events included

Step Functions speaks `awsJson1_0`, where a `Timestamp` is a JSON number of epoch seconds, and the sfn
client's decoder expects a number. `creationDate`, `startDate`, `stopDate` and `updateDate` have
always answered that way, through `sfnEpoch`. `GetExecutionHistory`'s per-event `timestamp`, which
`API_HistoryEvent` publishes as a `Timestamp`, Required: Yes, did not until #1323: it answered the
stored `time.Time` as an RFC3339 string, so the whole response failed to decode. It now answers
through `sfnHistoryToWire` (`emulator/stepfunctions_wire.go`), and the stored history keeps its
encoding, so an execution recorded before the fix reads back and answers the number too.

Each event's `type` is a value of `API_HistoryEvent`'s closed Valid Values list, derived from the ASL
state's own type: `PassStateEntered`/`PassStateExited`, and likewise for `Task`, `Choice`, `Wait`,
`Succeed`, `Parallel` and `Map`. A `Fail` state answers `FailStateEntered` and no exited event, because
the page publishes none. Until #1323 every state answered the generic `StateEntered`/`StateExited`,
which the list does not contain. `TestSFNHistory_EventsAnswerPublishedTypesAndEpochTimestamps` in
`emulator/stepfunctions_history_test.go` asserts both on the raw bytes.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::StepFunctions::StateMachine | StateMachineArn | `DefinitionString`, `Definition` and `DefinitionSubstitutions` are all read and all resolved through the intrinsic context; `DefinitionS3Location` is declined — see below (#1074) |
| AWS::StepFunctions::Activity | ActivityArn | `Name` only |

### The definition resolves through the intrinsic context

Every property of `AWS::StepFunctions::StateMachine` was resolved through the
intrinsic context except the one carrying the definition, which is close to the
only place a real template *has* to put an intrinsic: an ASL `Task` state's
`Resource` is a Lambda function ARN, and a template that creates the function
cannot know the ARN at authoring time. An `Fn::Sub` arrived at the deployer as a
map and was stored as `{"Fn::Sub":"…"}` — a document that parses, describes an
object, and has neither `StartAt` nor `States` (#1074).

The three properties substrate reads, and the order it reads them in:

| Property | Type | How it resolves |
|----------|------|-----------------|
| `DefinitionString` | String | Resolved through the intrinsic context, so an `Fn::Join` or `Fn::Sub` becomes the document. A literal string passes through untouched and is **not** re-marshalled — doing that once stored a JSON string literal rather than an ASL object (#996) |
| `Definition` | Json | The object form, resolved at every depth and marshalled **once**. A literal number stays a number; only a resolved intrinsic becomes a string |
| `DefinitionSubstitutions` | Object | Applied last to whichever of the two supplied the document. Each value is resolved first, so `HelloFunction: !GetAtt Hello.Arn` injects the deployed ARN |

`DefinitionString` wins when both it and `Definition` are present. **This is
substrate's reading**: both are `Required: No` and no sentence on the resource
page covers supplying both, and this is the one order that changes nothing for a
template that already deployed.

An undeclared `${key}` in the document is left **exactly as written**, which is
why substitution does not reuse `Fn::Sub`'s resolver: that one falls back to
resolving an unknown name as a `Ref`, which returns the bare name, so an
unrelated `${…}` would lose its braces instead of being left alone. AWS's second
published substitution form, `${variable_1,variable_2,…}`, addresses a key-value
map variable rather than naming a substitution key, so no key can match it and it
falls through that same untouched case.

`DefinitionS3Location` is declined: it names an S3 object holding the document,
and fetching it would make a deploy depend on a bucket's contents, so a template
using it gets the stub definition instead. `Fn::Sub`'s `${LogicalId.Attribute}`
form works inside a definition as it does in every other property: the shared
resolver answers `${MyFunction.Arn}` with what `Fn::GetAtt` answers (#1103).

A template that supplies none of the three gets a stub definition, and the stub
is a runnable state machine rather than a placeholder — which is what lets it
survive the structural validation above.

## Cost

Step Functions state transitions: $0.025 per 1,000 transitions.


## List tokens are issued, not names

`ListStateMachines`, `ListExecutions` and `ListActivities` each describe `nextToken` as *"a unique
pagination token for each page"* and publish `InvalidToken`/400, *"The provided token is not valid."*
The token used to be the next item's bare name, and a name that matched nothing was read as page one
([#1540](https://github.com/scttfrdmn/substrate/issues/1540)). It is now an opaque cursor naming the last
item the previous page returned and the listing that issued it, checked before any state is read: a
token no call to that operation issued — a bare name, another listing's token, another state machine's
execution token — is `InvalidToken`/400.

`ListExecutions` applies `statusFilter` before it cuts the page, so a page holds up to `maxResults`
matching executions and a `nextToken` means another matching one remains. The cursor is resumed by the
named execution's place in the whole listing, so it stays valid when that execution's status changes
between pages.

State machines and activities resume at the first name after the cursor, so deleting one between two
pages, including the one the cursor names, costs no other its place. Executions are listed in start
order and are never removed from their state machine, so their cursor resumes after the named
execution. The pages' 24-hour expiry is not modeled. Tokens recorded before #1540 are bare names and
are now refused.
