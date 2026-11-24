# EMR Serverless

**Endpoint:** `emr-serverless.{region}.amazonaws.com`
**Protocol:** REST/JSON, API version 2021-07-13. The SigV4 signing name is `emr-serverless`, which
`parser.go` maps to the `emrserverless` namespace
**Routing:** method and path, resolved by `parseEMRServerlessOperation`, which matches the path
[segment by segment](#the-jobruns-route-is-matched-by-segment). A path under `/applications` that
matches no published URI answers `UnknownOperationException`/404

Seven of the twenty-three published operations, over two resources: an application and its job runs.
Every record is keyed by account and Region. No Spark or Hive work is executed — that is the boundary
in `doc.go` — so a job run reports [`SUCCESS` from the instant it is
submitted](#a-job-run-s-state-progresses-under-a-seed). The `jobDriver` that says what would have
run is recorded and answered back, so a test can confirm what was submitted.

## Supported operations

| Operation | Published path | Notes |
|-----------|----------------|-------|
| CreateApplication | `POST /applications` | `clientToken`, `releaseLabel` and `type` are [`Required: Yes` and checked](#the-required-inputs-are-checked-and-clienttoken-is-idempotent), with their published patterns; a resubmitted `clientToken` answers the application it created. The application is `CREATED` and [stays there](#an-application-s-state-progresses-under-a-seed). Answers `applicationId`, `arn` and `name`, the three published members |
| GetApplication | `GET /applications/{applicationId}` | [All seven published `Required: Yes` `Application` members](#the-records-answer-every-required-member), plus the optional members the create sent |
| DeleteApplication | `DELETE /applications/{applicationId}` | Answers `{}` where the page publishes an empty body, and [cannot fail](#deleteapplication-cannot-fail) |
| StartJobRun | `POST /applications/{applicationId}/jobruns` | `clientToken` and `executionRoleArn` are [`Required: Yes` and checked](#the-required-inputs-are-checked-and-clienttoken-is-idempotent); `jobDriver`, `mode`, `name` and `tags` are recorded. The application must exist (`ResourceNotFoundException`/404). The run is [`SUCCESS` immediately](#a-job-run-s-state-progresses-under-a-seed). The `jobRunId` it mints [satisfies its published pattern](#an-emr-serverless-job-run-id-is-sixteen-lowercase-hex-characters). `configurationOverrides`, `retryPolicy` and `executionTimeoutMinutes` are not read |
| GetJobRun | `GET /applications/{applicationId}/jobruns/{jobRunId}` | [All eleven published `Required: Yes` `JobRun` members](#the-records-answer-every-required-member); `attempt` is not read |
| CancelJobRun | `DELETE /applications/{applicationId}/jobruns/{jobRunId}` | Answers the two published members, and leaves the run [`CANCELLED`](#a-cancelled-job-run-reports-cancelling-then-cancelled); `shutdownGracePeriodInSeconds` is not read |
| ListJobRuns | `GET /applications/{applicationId}/jobruns` | [Every published member is read](#listjobruns-pages-and-filters): `maxResults`, `nextToken`, `states`, `mode`, `createdAtAfter` and `createdAtBefore`. Each element is the ten-required-member `JobRunSummary`, [the ID as `id`](#listjobruns-names-the-job-run-id-id) |

The sixteen unrouted operations include the whole application lifecycle beyond create and delete —
`StartApplication`, `StopApplication`, `UpdateApplication` and `ListApplications`, so an application
cannot be started, stopped, modified or enumerated — the entire interactive-session surface
(`StartSession`, `GetSession`, `GetSessionEndpoint`, `ListSessions`, `TerminateSession`), the two
dashboard reads (`GetDashboardForJobRun`, `GetResourceDashboard`), `ListJobRunAttempts`, and the three
tag operations. Each answers `UnknownOperationException` / 404. Tags sent on a create are stored and
answered by `GetApplication` and `GetJobRun`, but no tag operation can change them.

## A job run's state progresses under a seed

`StartJobRun` records the run `SUCCESS` — executing the Spark or Hive work is out of scope — and answers
no state. `GetJobRun` and `ListJobRuns` report the run's state through a seed ([How a progression is seeded](/reference/concepts/seeds-and-replay#how-a-progression-is-seeded)):

```
POST   /v1/emr-serverless/job-run-status
       {"jobRunId":"…"|"*","pendingObservations":2,"state":"PENDING","finalState":"FAILED",
        "stateDetails":"…"}
DELETE /v1/emr-serverless/job-run-status           (all; ?jobRunId=… for one)
```

For `pendingObservations` observations the run reports `state` (default `RUNNING`; `SUBMITTED`,
`PENDING`, `SCHEDULED`, `QUEUED` may be named), then `finalState` (default the record's `SUCCESS`, or
`FAILED` or `CANCELLED`) with the seed's `stateDetails`, held to `JobRun`'s 1–256-character, non-blank
constraint. `GetJobRun` and `ListJobRuns` both observe, so they agree: a run a list shows `PENDING` is
`PENDING` on the next `GetJobRun` too, and a `states` filter narrows by the state the caller is
shown. Unseeded, a run reads `SUCCESS` from its first observation, as before.
`billedResourceUtilization`, `totalResourceUtilization`, `totalExecutionDurationSeconds`,
`queuedDurationMilliseconds`, `startedAt` and `endedAt` describe a workload substrate does not run,
and are absent ([#1196](https://github.com/scttfrdmn/substrate/issues/1196)).

## A cancelled job run reports CANCELLING, then CANCELLED

`CancelJobRun` records the run `CANCELLED`, with two Ls, the spelling both places EMR Serverless
publishes the enum use: `JobRun`'s `state` member and `ListJobRuns`'s `states` filter. Until
[#1198](https://github.com/scttfrdmn/substrate/issues/1198) it wrote `CANCELED`, which a typed-enum SDK
resolved to an unknown value. The cancel restarts the run's countdown, so under a job-run seed the run
reports `CANCELLING`, the published transitional state, for the seeded observations before
`CANCELLED`, whatever the seed's own transient state; a cancelled run never ends `SUCCESS` or
`FAILED`. Unseeded, it reports `CANCELLED` at once
([#1196](https://github.com/scttfrdmn/substrate/issues/1196)).

## ListJobRuns names the job run id id

`ListJobRuns` publishes its `jobRuns` elements as `JobRunSummary`, whose ID member is `id`. `jobRunId`
is a member of `JobRun`, which `GetJobRun` returns, not of the summary. Until
[#1204](https://github.com/scttfrdmn/substrate/issues/1204) the list answered `jobRunId`, so an SDK
deserializing it found no `id` and left it empty on every element. `jobRunId` is not also answered for
compatibility: it is not a member of the published shape, and an unpublished member is a defect by
#1013's rule.

## ListJobRuns pages and filters

Until [#1195](https://github.com/scttfrdmn/substrate/issues/1195) the handler discarded its request, so
every list was one page with no `nextToken`, and every filter matched everything: `states=FAILED`
returned every run, all `SUCCESS`, which read as a passing assertion. Every member the page publishes
is now read.

| Member | Published constraint | What substrate does |
|--------|----------------------|---------------------|
| `maxResults` | 1–50 | A page holds at most that many, 50 when none is named (the page publishes no default). Outside the range, or not an integer, is `ValidationException`/400 |
| `nextToken` | 1–1024 of `[A-Za-z0-9_=-]` | Resumes after the previous page. It is omitted on the last page, never answered empty. A token substrate did not issue is `ValidationException`/400 |
| `states` | up to 8 of the nine state values | Narrows the list, grouped by state in the order the filter names them, as the page says. More than eight, or an unpublished value, is `ValidationException` |
| `mode` | `BATCH` or `STREAMING` | Narrows the list. A run started without a mode is `BATCH` |
| `createdAtAfter`, `createdAtBefore` | Timestamps | Narrow by `createdAt`. They are query-string timestamps, ISO 8601; one that does not parse is `ValidationException` |

`ListJobRuns` publishes no `InvalidNextTokenException`, only `ValidationException`/400 and
`InternalServerException`/500, so an unissued token answers `ValidationException`, the page's own code
for input that fails a constraint. #1195 named `InvalidNextTokenException` for EMR Serverless; the page
does not carry it.

`states` is a list bound to the query string, so a typed SDK sends it as a repeated key,
`states=FAILED&states=CANCELLED`. The HTTP parser used to keep only the first value of every query
parameter, so a two-state filter narrowed to its first state. It now keeps every value of a repeated key
in `AWSRequest.MultiValueParams`, which a request without a repeated key leaves unset, so its recorded
bytes are unchanged.

The list is ordered by `createdAt`, then by job run ID for runs created at one instant. The page
publishes no order beyond the grouping, and the order is stable, which is what the offset token relies
on. A list for an application that does not exist answers empty: the page publishes no
`ResourceNotFoundException`.

## StartJobRun checks that the application exists

`StartJobRun` loads the application before it writes a run, and an application that does not exist is
`ResourceNotFoundException`/404, *"The specified resource was not found."*, which the page publishes.
Until [#1197](https://github.com/scttfrdmn/substrate/issues/1197) a typo'd or deleted application ID
yielded a run that `GetJobRun` then reported as `SUCCESS`. The run's `releaseLabel` is its
application's.

## DeleteApplication cannot fail

The handler deletes the state key without reading it first, removes the id from the index and answers
`{}`. `ResourceNotFoundException`/404 is published on that page and has no site, so deleting an
application that was never created succeeds. The page also states the precondition — *"An application
has to be in a stopped or created state in order to be deleted."* — which cannot be violated here
because an application never leaves `CREATED`; and there is no published code on that page for a
precondition failure to land on, since it publishes only `InternalServerException`/500,
`ResourceNotFoundException`/404 and `ValidationException`/400, with no `ConflictException`. The `{}`
body is a third small divergence: the page publishes *"an HTTP 200 response with an empty HTTP body"*.
[#1196](https://github.com/scttfrdmn/substrate/issues/1196).

## An EMR Serverless job run id is sixteen lowercase hex characters

`JobRun`'s `jobRunId` publishes `Pattern: [0-9a-z]+` with a maximum length of 64, the URI parameters
on `GetJobRun` and `CancelJobRun` publish the same, and `StartJobRun`'s `arn` publishes
`Pattern: arn:(aws[a-zA-Z0-9-]*):emr-serverless:.+:(\d{12}):\/applications\/[0-9a-zA-Z]+\/jobruns\/[0-9a-zA-Z]+`.
Substrate mints a job run ID as `00` and fourteen lowercase hex digits, sixteen characters, the form
the application ID takes (`00` and eight hex digits). Until
[#1204](https://github.com/scttfrdmn/substrate/issues/1204) it was a dashed UUID, which satisfied none
of the three, so code that validated an ID or parsed one out of an ARN rejected every job run substrate
minted. Both IDs are derived from the request ID ([#856](https://github.com/scttfrdmn/substrate/issues/856)),
so a replay mints what its recording minted, and both are asserted against their published patterns
in `emulator/minted_id_patterns_test.go`.

## The records answer every required member

Until [#1199](https://github.com/scttfrdmn/substrate/issues/1199) `GetApplication` answered five of
`Application`'s seven `Required: Yes` members, `GetJobRun` four of `JobRun`'s eleven, and `ListJobRuns`
four of `JobRunSummary`'s ten. Each now answers all of them.

| Shape | Required members | Optional members answered | Notes |
|-------|------------------|---------------------------|-------|
| `Application` | `applicationId`, `arn`, `createdAt`, `releaseLabel`, `state`, `type`, `updatedAt` | `name`, `architecture`, `tags`, `initialCapacity`, `maximumCapacity`, `networkConfiguration`, as the create sent them | The configuration objects are recorded verbatim; nothing acts on them |
| `JobRun` | `applicationId`, `arn`, `createdAt`, `createdBy`, `executionRole`, `jobDriver`, `jobRunId`, `releaseLabel`, `state`, `stateDetails`, `updatedAt` | `mode`, `name`, `tags` | `jobDriver` is answered when the request sent one: `StartJobRun` marks it `Required: No`, so a run started without one has none, and none is invented |
| `JobRunSummary` | `applicationId`, `arn`, `createdAt`, `createdBy`, `executionRole`, `id`, `releaseLabel`, `state`, `stateDetails`, `updatedAt` | `mode`, `name` | `type` and the attempt members are not answered |

`createdAt` and `updatedAt` are epoch seconds, a JSON number, as the pages' Response Syntax renders a
Timestamp. `CancelJobRun` moves `updatedAt`. A record written before #1199 holds neither date and
answers both as `null`, rather than an invented instant. `createdBy` is the caller's principal ARN,
or the account root when the request carries no principal. `stateDetails` is `Required` and publishes
no wording, so its text is substrate's: *"The job run completed."*, *"The job run was cancelled."*, and
so on.

## The account and Region a record carries reach no response

`EMRServerlessApp` and `EMRServerlessJobRun` declare `accountID` and `region` under wire-visible
`json` tags, because the record is what `MemoryStateManager` snapshots and a replay reads back. Until
[#756](https://github.com/scttfrdmn/substrate/issues/756), `GetApplication` and `GetJobRun` answered
each record whole, so both members reached the wire, and neither published shape has them. They now
answer through `emrServerlessAppOut` and `emrServerlessJobRunOut`
(`emulator/emrserverless_wire.go`). The stored records are unchanged.
`TestEMRServerlessWire_ResponsesCarryNoBookkeepingMember` drives all seven routed operations and walks
each decoded document for either member at any depth.

## The required inputs are checked, and clientToken is idempotent

Until [#1197](https://github.com/scttfrdmn/substrate/issues/1197) no `Required: Yes` member of either
create was checked, and `clientToken` was not even decoded. Each is now refused when absent, and its
published constraint is enforced:

| Operation | Member | Published constraint |
|-----------|--------|----------------------|
| `CreateApplication` | `clientToken` | Required; 1–64 of `[A-Za-z0-9._-]` |
| `CreateApplication` | `releaseLabel` | Required; 1–64 of `[A-Za-z0-9._/-]` |
| `CreateApplication` | `type` | Required; 1–64 characters |
| `CreateApplication` | `name` | 1–64 of `[A-Za-z0-9._/#-]` |
| `CreateApplication` | `architecture` | `ARM64` or `X86_64` |
| `StartJobRun` | `clientToken` | Required; 1–64 of `[A-Za-z0-9._-]` |
| `StartJobRun` | `executionRoleArn` | Required; 20–2048, an IAM role ARN in the published pattern |
| `StartJobRun` | `name` | 1–256, not blank |
| `StartJobRun` | `mode` | `BATCH` or `STREAMING` |

A failure is `ValidationException`/400, the code every EMR Serverless page publishes for input that
fails a constraint. The `tags` maps' key and value patterns are not enforced; the members are recorded as
sent.

Both pages say only that `clientToken`'s *"value must be unique for each request"*, and both publish
`ConflictException`/409. Substrate applies the AWS idempotency-token contract. The same token with the
same parameters answers the resource it first created and creates nothing: `CreateApplication`
answers the same `applicationId`, and `StartJobRun` the same `jobRunId`, which `ListJobRuns` then lists
once. The same token with different parameters is `ConflictException`/409. A `StartJobRun` token is
scoped to its application.

## An application's state progresses under a seed

`CreateApplication` records `state: "CREATED"` and answers no state. `GetApplication` reports it
through a seed ([How a progression is seeded](/reference/concepts/seeds-and-replay#how-a-progression-is-seeded)):

```
POST   /v1/emr-serverless/application-status
       {"applicationId":"…"|"*","pendingObservations":1,"state":"CREATING","finalState":"CREATED"}
DELETE /v1/emr-serverless/application-status       (all; ?applicationId=… for one)
```

The application reports `state` (default `CREATING`) for `pendingObservations` observations, then
`finalState` (default `CREATED`). Any of `Application`'s seven published states may be named, so
`STARTED`, `STOPPED` and `TERMINATED` are reachable as a final state. `StartApplication`,
`StopApplication` and `UpdateApplication` are still not routed, so no request moves an application,
and `autoStartConfiguration` and `autoStopConfiguration` are stored nowhere; `StartJobRun` succeeds
against an application in any state. The application publishes no failure state
([#1196](https://github.com/scttfrdmn/substrate/issues/1196)).

## The jobruns route is matched by segment

`parseEMRServerlessOperation` splits the path into segments and matches each published URI
positionally: `applications` first, then one application-ID segment, then `jobruns` as a whole
segment, then one job-run-ID segment. Until [#1205](https://github.com/scttfrdmn/substrate/issues/1205)
it searched for the substring `/jobruns`, so `/applications/ab/jobrunsbad` routed as `ListJobRuns`,
`/applications/ab/cd/jobruns` read the application ID as `ab/cd`, and `/applications/jobruns` routed
with an empty application ID. Each now answers `UnknownOperationException`/404, the common list's code
for a path that matches nothing. So does an empty ID segment, since both IDs publish a minimum length
of 1. One shape answers differently, and correctly: `GET /applications/jobruns` is a well-formed
`GetApplication` for an application named `jobruns`, so it answers that operation's
`ResourceNotFoundException`/404. `emulator/rest_routing_near_miss_test.go` enumerates the near-misses.

## What a refusal reports

| Condition | Code | Status |
|-----------|------|--------|
| a `CreateApplication` body that will not parse, or no body at all | `ValidationException` | 400 |
| a `StartJobRun` body that will not parse | `ValidationException` | 400 |
| a `Required: Yes` member absent, or a member outside its published constraint | `ValidationException` | 400 |
| a `ListJobRuns` `maxResults`, `nextToken`, `states`, `mode` or date outside its constraint | `ValidationException` | 400 |
| a `clientToken` reused with different parameters | `ConflictException` | 409 |
| an application that does not exist, on `GetApplication` or `StartJobRun` | `ResourceNotFoundException` | 404 |
| a job run that does not exist, on `GetJobRun` or `CancelJobRun` | `ResourceNotFoundException` | 404 |
| a method and path that match no operation | `UnknownOperationException` | 404 |

Each code is published at that status on every page that carries it: `ValidationException` at 400,
`ConflictException` at 409 and `ResourceNotFoundException` at 404, which is the REST-JSON shape and not
the 400 the JSON-RPC services in this reference use. The messages are substrate's. Both body-decode
refusals go through the shared `emrInvalidBody` constructor.

`ResourceNotFoundException`/404 still has no site on `DeleteApplication`, which publishes it. No state
precondition is enforced, since an application never leaves `CREATED`. `InternalServerException`/500
is published on all seven routed operations and has no site, which is correct: an internal failure is
not something a request can ask for. A store failure is returned as an error, never as a published
refusal.

## Cost

`StartJobRun` is attributed $0.0001 per call and `CreateApplication` $0.00001. Both keys can match:
the cost table is keyed on the lowercased service name and the resolved operation, and the
`emrserverless` operation resolver derives the operation from the method and path. The figures stand
in for nothing AWS actually meters — EMR Serverless bills worker vCPU-hours, memory-GB-hours and
storage, and Substrate runs no workers, which is also why `billedResourceUtilization` and
`totalResourceUtilization` are never emitted on a job run. Creating an application is free at AWS;
the $0.00001 exists so that an application appears in a cost summary at all.
