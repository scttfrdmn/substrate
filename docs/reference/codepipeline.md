# CodePipeline

**Endpoint:** `codepipeline.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: CodePipeline_20150709.{Op}`)

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreatePipeline | `pipeline.name` required; `stages` are stored as opaque objects and never validated |
| GetPipeline | `version` [answers that stored version](#getpipelineexecution-requires-and-checks-its-pipeline-and-getpipeline-answers-the-version-asked-for) |
| UpdatePipeline | Merges `roleArn` and `stages`, increments `version`; reports no `metadata` |
| DeletePipeline | |
| ListPipelines | Reports name, version and timestamps; `maxResults` and `nextToken` are not read. A pipeline whose record cannot be loaded is skipped |
| StartPipelineExecution | The execution is `Succeeded` before the call returns unless [a seed makes it progress](#an-execution-progresses-under-a-seed); `clientRequestToken` and `variables` are not read |
| GetPipelineState | Reports every stage as `Succeeded` with an [empty `pipelineExecutionId`](#getpipelinestate-reports-a-shape-no-execution-produced) |
| GetPipelineExecution | Both members are required (`ValidationException`); an execution belonging to another pipeline is `PipelineExecutionNotFoundException`, as the page glosses the code ([#1156](https://github.com/scttfrdmn/substrate/issues/1156), [#1160](https://github.com/scttfrdmn/substrate/issues/1160)) |

## An execution progresses under a seed

`StartPipelineExecution` stores the execution with `Status: "Succeeded"`, which is what an unseeded
`GetPipelineExecution` reports. No stage action runs — that is workload-internal — but the execution's
status is observable, and a seed makes the published in-flight states observable first
([#1155](https://github.com/scttfrdmn/substrate/issues/1155)), under the rules
[every progression shares](/reference/concepts/seeds-and-replay#how-a-progression-is-seeded):

```
POST   /v1/codepipeline/execution-status   {"pipelineExecutionId": "…" | "*", "pendingObservations": 2,
                                            "status": "InProgress", "finalStatus": "Failed",
                                            "statusSummary": "…"}
DELETE /v1/codepipeline/execution-status   (all seeds; ?pipelineExecutionId=… for one)
```

`GetPipelineExecution` reports `status` — `InProgress` or `Stopping`, default `InProgress` — for
`pendingObservations` calls, then `finalStatus`, which is `Succeeded`, `Failed`, `Stopped`,
`Superseded` or `Cancelled` (default the record's). The final observation also carries the seed's
`statusSummary`, the member `API_PipelineExecution` publishes to describe a status. Together the two
halves are the seven published values, and a status outside its half is refused with 400 and not
stored. `ListPipelineExecutions` is not routed, so `GetPipelineExecution` is the only observation.
`GetPipelineState` still reports every stage `Succeeded` and is not governed by the seed.

`clientRequestToken` is the published idempotency member and is not read, so a retried start mints a
second execution where AWS would return the first.

## GetPipelineExecution requires and checks its pipeline, and GetPipeline answers the version asked for

Executions are keyed by ID alone, so `getPipelineExecution` compares the execution's pipeline with
the request's `pipelineName`. An execution that belongs to another pipeline answers
`PipelineExecutionNotFoundException`/400, as the page's own gloss of the code states: *"…or an
execution ID does not belong to the specified pipeline."* Both `pipelineName` and `pipelineExecutionId`
are `Required: Yes`, and a request that omits either is refused with `ValidationException`/400, which
the page publishes ([#1160](https://github.com/scttfrdmn/substrate/issues/1160)). Before, an absent
`pipelineName` skipped the ownership check and any execution answered.

`GetPipeline` honours `version` ([#1160](https://github.com/scttfrdmn/substrate/issues/1160)). Every
write of a pipeline — `CreatePipeline`'s version 1 and each `UpdatePipeline` — also stores that
version's snapshot at `pipeline_version:{account}/{region}/{name}/{version}`, and `GetPipeline` with a
`version` answers that snapshot: its `stages`, `roleArn` and `version`, with `metadata.updated` the time
that version was written. A version that does not exist, or one below the published minimum of 1, is
`PipelineVersionNotFoundException`/400. `DeletePipeline` removes the history with the pipeline.

A pipeline created before this change has no history: its current version still answers, and any
earlier version is `PipelineVersionNotFoundException`.

## The account and Region a record carries reach no response

`CodePipelineState` and `CodePipelineExecution` declare `accountID` and `region` under wire-visible
`json` tags, because the record is what `MemoryStateManager` snapshots and a replay reads back.

- **The pipeline's responses** have always been built member by member, so neither member reached
  them.
- **`GetPipelineExecution`** answered the execution record whole until
  [#756](https://github.com/scttfrdmn/substrate/issues/756). That put both members on the wire, plus
  `startTime`. `API_PipelineExecution` publishes no date member at all, so `startTime` is dropped
  rather than converted. The response now answers `codepipelineExecutionOut`
  (`emulator/codepipeline_wire.go`).

All eight routed operations are driven by
`TestCodePipelineWire_PipelineResponsesCarryNoBookkeepingMember` and its sibling in
`emulator/codepipeline_wire_test.go`.

`created` and `updated` rendered as RFC3339 strings everywhere they appear: `CreatePipeline`'s and
`GetPipeline`'s `metadata`, `ListPipelines`' summaries, and `GetPipelineState`. CodePipeline speaks
`awsJson1_1`, where `API_GetPipeline` publishes both as numbers and its own sample is
`"created": 1501626591.112`. A typed SDK therefore could not decode a pipeline response
([#1338](https://github.com/scttfrdmn/substrate/issues/1338)). They are now epoch seconds, and the
stored record keeps its `time.Time`.

## GetPipelineState reports a shape no execution produced

`GetPipelineState` derives one stage state per stored stage definition, each with
`latestExecution.status: "Succeeded"` and `latestExecution.pipelineExecutionId: ""` — an empty string
where the member is published as an execution ID, and a success regardless of whether any execution
has ever run. A pipeline created and never started reports every stage succeeded. `actionStates`,
`inboundTransitionState` and `beforeEntryConditionState` are not emitted.

## What a refusal reports

| Condition | Code | Status |
|-----------|------|--------|
| a body that will not parse | `InvalidStructureException` | 400 |
| a required name absent or empty | `InvalidStructureException` | 400 |
| a pipeline name already in use | `PipelineNameInUseException` | 400 |
| a pipeline that does not exist | `PipelineNotFoundException` | 400 |
| a pipeline execution that does not exist, or belongs to another pipeline | `PipelineExecutionNotFoundException` | 400 |

Every row is 400, the status every CodePipeline page publishes. Until
[#1156](https://github.com/scttfrdmn/substrate/issues/1156) both not-found codes answered 404, which no
CodePipeline page publishes, across six operations: `GetPipeline`, `UpdatePipeline`, `DeletePipeline`,
`StartPipelineExecution`, `GetPipelineState` and `GetPipelineExecution`. `ListPipelines` still swallows
the refusal and reports a short list, as its page publishes.

`InvalidStructureException` is published, glossed *"The structure was specified in an invalid format"* —
but `GetPipeline` does not publish it at all (its three errors are `PipelineNotFoundException`,
`PipelineVersionNotFoundException` and `ValidationException`), so on the four operations that reach it
through the shared name check it is Substrate's reading rather than that page's vocabulary.
`ValidationException`, `PipelineVersionNotFoundException`, `ConcurrentModificationException` and
`LimitExceededException` are published and have no site.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::CodePipeline::Pipeline | Name | A stub: properties are recorded in the CloudFormation stub store, which the CodePipeline plugin does not read, so a pipeline deployed from a template is invisible to `GetPipeline` and `ListPipelines` and cannot be started. The ARN carries no resource-type prefix (`arn:aws:codepipeline:{region}:{account}:{name}`), which is the form AWS publishes |

## Cost

`StartPipelineExecution` is attributed $0.000001 per call. Real CodePipeline charges $1.00 per active
pipeline per month rather than per execution, so the attribution is a proxy: Substrate has no month.
