# Batch

**Endpoint:** `batch.{region}.amazonaws.com`
**Protocol:** REST/JSON (`POST /v1/{operation}`)

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateComputeEnvironment | `computeEnvironmentName` and `type` required; an omitted `state` is `ENABLED` |
| DescribeComputeEnvironments | `computeEnvironments` filter takes [names or full ARNs](#a-describe-filter-takes-a-name-or-an-arn); reports `ecsClusterArn`; a `nextToken` no previous call returned answers [`ClientException` / 400](/reference/concepts/listing-order#batch-s-three-describes-shared-one-paginator-so-the-decode-had-to-leave-it) rather than page one |
| CreateJobQueue | `jobQueueName` required; an omitted `state` is `ENABLED` |
| DescribeJobQueues | `jobQueues` filter takes names or full ARNs; a `nextToken` no previous call returned answers [`ClientException` / 400](/reference/concepts/listing-order#batch-s-three-describes-shared-one-paginator-so-the-decode-had-to-leave-it) rather than page one |
| RegisterJobDefinition | `jobDefinitionName` and `type` required; each registration is [the next revision](#a-job-definition-is-versioned) |
| DescribeJobDefinitions | `jobDefinitions` (`${name}:${revision}` or full ARN), `jobDefinitionName` (every revision), and `status`; a `nextToken` no previous call returned answers [`ClientException` / 400](/reference/concepts/listing-order#batch-s-three-describes-shared-one-paginator-so-the-decode-had-to-leave-it) rather than page one |
| SubmitJob | Returns `jobId`. `jobName`, `jobQueue` and `jobDefinition` are required, and the queue and definition must be usable — see [What a submitted job needs](#what-a-submitted-job-needs). The job is recorded `SUBMITTED`; what a describe reports is its [progression](#a-job-s-status-progresses-through-the-seven-published-values) (#1248) |
| DescribeJobs | Each job reported is one [observation](#a-job-s-status-progresses-through-the-seven-published-values) of its progression |
| ListJobs | `POST /v1/listjobs`; [`RUNNING` by default](#listjobs-reads-its-request), `jobQueue` scopes, all five `filters` match by their published rules, `maxResults`/`nextToken` paginate, and `arrayJobId`/`multiNodeJobId` are empty listings |
| TerminateJob | A job not yet settled is reported `FAILED` with the supplied `reason`, as the page publishes for `STARTING`/`RUNNING` (terminated) and earlier states (cancelled). A settled job is left as it is. Answers `{}`: the page publishes an empty body, and its own sample response is `{}` (#1206) |
| CancelJob | `POST /v1/canceljob`. A `SUBMITTED`, `PENDING` or `RUNNABLE` job is reported `FAILED` with the `reason`; a `STARTING` or `RUNNING` one is not cancelled, and the call still succeeds, as the page publishes (#1248). Answers `{}` |
| DeregisterJobDefinition | `jobDefinition` is `name:revision` or a full ARN; a bare name, which addresses no single revision, and a revision that does not exist are `ClientException`. The revision is [kept and reported `INACTIVE`](#a-resource-leaves-service-through-five-lifecycle-operations), so `status: INACTIVE` selects it. Answers `{}` (#555) |
| UpdateJobQueue | Changes `state`, `priority`, `schedulingPolicyArn` and `computeEnvironmentOrder`; an absent member is left as it was. `state` outside `ENABLED \| DISABLED`, or a queue that does not exist, is `ClientException`. `jobStateTimeLimitActions` and `serviceEnvironmentOrder` are not read |
| UpdateComputeEnvironment | Changes `state`, `serviceRole` and `unmanagedvCpus`, and merges `computeResources` member by member; an absent member is left as it was. Same refusals as `UpdateJobQueue`. `updatePolicy`, `ecsSettings`, `eksConfiguration` and `context` are not read |
| DeleteJobQueue | Refused with `ClientException` unless the queue is `DISABLED`, as the page requires; its compute environments need not be disassociated first. The queue [is gone at once](#a-resource-leaves-service-through-five-lifecycle-operations), and its unfinished jobs [are failed](#what-a-submitted-job-needs). Answers `{}` |
| DeleteComputeEnvironment | Refused with `ClientException` unless the environment is `DISABLED` and no job queue's `computeEnvironmentOrder` names it, by name or ARN. Answers `{}` |

## A resource leaves service through five lifecycle operations

Until #555 a compute environment, a job queue or a job definition could be created and described and
never changed or removed, so `DescribeJobDefinitions`' `INACTIVE` filter could not select anything:
deregistration is the only writer of `INACTIVE`. The documented teardown order is now the one that
works — disable the queue and the environment, disassociate the environment from the queue, then
delete both — and each step taken out of order is refused with `ClientException`, the only client
error Batch publishes.

A deregistered job definition **is kept**: "Job definitions are permanently deleted after 180 days",
and `INACTIVE` is a published filter value, so it stays describable by name and by revision. A deleted
queue or environment **is not**: AWS reports one as `DELETING` and then `DELETED` before it stops being
listed, but the pages publish no count or interval for that transition, so substrate removes the record
at once rather than invent one.

Every operation reports a bad request as **`ClientException`** at HTTP 400. The API
reference declares exactly two errors for each Batch operation, `ClientException` and
`ServerException`, so substrate does not use the `MissingParameter` or
`InvalidParameterValue` codes other services return for the same shape of complaint.

## What a submitted job needs

Until [#1422](https://github.com/scttfrdmn/substrate/issues/1422) `SubmitJob` answered a missing
`jobName` with `MissingParameter`, which Batch publishes nowhere, and checked neither the queue nor the
definition, so a job could be submitted to a queue that was disabled, deleted or never created.
`API_SubmitJob` publishes `jobName`, `jobQueue` and `jobDefinition` as `Required: Yes` and one client
error, `ClientException`/400 (*"Another cause is specifying an identifier that's not valid"*). Each of
these is now that refusal:

| Request | Why |
|---------|-----|
| `jobName`, `jobQueue` or `jobDefinition` missing | `Required: Yes`; checked before any state is read |
| A queue that does not exist, by name or ARN | *"You can specify either the name or the Amazon Resource Name (ARN) of the queue"* |
| A queue that is not `ENABLED` and `VALID` | `UpdateJobQueue`: a `DISABLED` queue *"can't accept new jobs"* |
| A `name:revision`, or an ARN with one, naming no revision or an `INACTIVE` one | The definition must be usable |
| A bare name, or an ARN without a revision, with no `ACTIVE` revision | *"If the revision is not specified, then the latest active revision is used"* |

The job keeps the queue and definition identifiers as they were sent. The messages are substrate's own.

`API_DeleteJobQueue` says *"All jobs in the queue are eventually terminated when you delete a job
queue."* Substrate ends them **at delete time**, since the page publishes no interval. Every job of the
queue, submitted by name or by ARN, is ended by the rule `TerminateJob` uses, so the two agree: a job
that would report `SUBMITTED`, `PENDING`, `RUNNABLE`, `STARTING` or `RUNNING` is recorded `FAILED`, and
one `TerminateJob` or `CancelJob` already ended keeps its status and reason. As with `TerminateJob`, an
unseeded job's record is still `SUBMITTED`, so it is ended too. The state is peeked rather than
observed, so the delete spends no observation. No page publishes a `statusReason` for such a job, so the
one it reports, *"Job terminated because its job queue was deleted."*, is substrate's.

The jobs are failed before the queue is deleted, so a delete that fails partway leaves the queue for a
retry to finish. `SubmitJob` and `DeleteJobQueue` serialize on the queue, so no job is accepted between
the delete's job sweep and the queue's removal. A bare definition name resolves through an index of the
name's `ACTIVE` revisions, which `RegisterJobDefinition` and `DeregisterJobDefinition` maintain.

## A describe filter takes a name or an ARN

All three resource describes document their filter as "a list of up to 100 … names or
full Amazon Resource Name (ARN) entries", and both forms resolve. An **absent** filter
reports every resource of that type in the caller's account and Region.

A filter entry naming a resource that does not exist is **skipped, not refused**: the
operations describe "one or more of your compute environments" and document no
not-found error, so an absent name yields an absent result rather than an error. A
filter in which nothing matches is an empty list at HTTP 200.

`maxResults` and `nextToken` paginate. An absent or out-of-range `maxResults` reports up
to 100 results, per the reference's "if this parameter isn't used, then Describe…
returns up to 100 results". `nextToken` is **omitted** once the results are exhausted —
"this value is null when there are no more results to return" — including when the last
page is exactly full.

## A job definition is versioned

Each `RegisterJobDefinition` of a name is a distinct revision, numbered from 1, and each
revision is its own record. `DescribeJobDefinitions` addresses one revision through
`jobDefinitions` (`${name}:${revision}` or the full ARN) and every revision of a
definition through `jobDefinitionName`.

`jobDefinitions` **wins outright** when both are sent, rather than being intersected or
unioned: the reference states it "can't be used with other parameters".

A newly registered definition is `ACTIVE`, and the `status` filter selects on that.
Nothing yet reports a definition `INACTIVE` — `DeregisterJobDefinition` is not
implemented (tracked as issue #555) — but the status is recorded on the resource rather
than synthesised at read time, so a deregistration can set it.

## ListJobs reads its request

[#1236](https://github.com/scttfrdmn/substrate/issues/1236). The handler took its request as
`_ *AWSRequest`, so none of the seven published members was read: it answered every job in the
account and Region, in insertion order, with no cursor. The pagination gap it was filed under was
the smallest of the four things that were wrong.

**The published default is `RUNNING` only.** *"If you don't specify a status, only `RUNNING` jobs
are returned."* A `SUCCEEDED` job in a list a consumer reads as "still running" inverts the meaning
of the call, and it was the **default** path — both of the page's own examples take it, and there is
no parameter to blame it on. `jobStatus` selects any of the seven published values and a value
outside them is refused; that a value outside a published `Valid Values` list is one of the
*"identifier[s] that's not valid"* the `ClientException` gloss covers is substrate's reading, on the
same footing as the token refusal above.

An unseeded job settles to `SUCCEEDED` on its first observation, so **an unseeded run's default
listing is still empty**: no unseeded job is ever `RUNNING`. A job seeded to sit in `RUNNING` is
listed by the default, which is the case the faithful default exists for; see
[A job's status progresses through the seven published values](#a-job-s-status-progresses-through-the-seven-published-values)
([#1248](https://github.com/scttfrdmn/substrate/issues/1248)). A listing observes each job in the
listing's scope (the queue and the filter) once before its status filter applies, so ListJobs and
DescribeJobs report the same status at the same point in a job's progression.

**One selector, or none.** *"You must specify only one of the following items: A job queue ID … A
multi-node parallel job ID … An array job ID"* — all three are `Required: No` individually, so the
rule lives in that prose. More than one is refused. **None** of the three stays the account-wide
listing, because the sentence forbids naming two rather than naming none and the page publishes no
error for an empty request; requiring exactly one would be substrate's reading. `jobQueue` matches a
name or a full ARN, so it reaches a job whichever form `SubmitJob` recorded.

`arrayJobId` and `multiNodeJobId` answer an **empty** list: `SubmitJob` records no
`arrayProperties` and no `nodeProperties`, so substrate mints no children and no nodes and there is
nothing for either listing to contain. Ignoring the member answered the account-wide listing
instead, reporting a parent's children as though they existed — the worse of the two wrong answers,
because it is not empty.

**All five filters, by their own rules.** `JOB_NAME` is a case-insensitive match with a
trailing-asterisk prefix form; `JOB_DEFINITION` is case-sensitive, matches every revision of a bare
name, supports the same asterisk on a name but not on an ARN; `BEFORE_CREATED_AT` and
`AFTER_CREATED_AT` take milliseconds since the epoch, and a value that is not a number is refused
rather than matching nothing. `SHARE_IDENTIFIER` is implemented and matches nothing, because no
recorded job carries a share identifier — a real empty answer rather than an ignored filter. More
than one filter is refused (*"Only one filter can be used at a time"*), a filter switches `jobStatus`
selection off except for `SHARE_IDENTIFIER`, and the filter path sorts by `createdAt` with the most
recent first, as the page publishes. The unfiltered path keeps insertion order, because the page
states no order for it.

Two published properties of the filter path are **not** modeled: `JOB_NAME`'s *"the results are
grouped by the job name and version"*, because a group is a property of the listing's shape rather
than of any job and the page does not say what the grouping does to the order it publishes in the
same paragraph; and *"The filter doesn't apply to child jobs in an array or multi-node parallel (MNP)
jobs"*, which is vacuous while both of those listings are empty.

**The summary carries seven of the eighteen published `JobSummary` members** — `jobArn` (derived as
`SubmitJob` derives the one it returns), `jobId`, `jobName`, `jobDefinition`, `createdAt`, `status`
and `statusReason`. It used to carry three. The other eleven describe the workload running inside
the job rather than an API observation of it and have no record behind them, so they are declined
rather than half-filled: a caller cannot tell a zero `container.exitCode` from a job that exited 0.
The page's own two sample responses emit only `jobId` and `jobName`, so a reader of the examples
alone would conclude nothing was missing; the Response Elements section is the authority.

`maxResults` clamps to the applicable published cap — 100 with `filters`, 1000 otherwise — and an
absent, zero or negative value takes the cap too, which is the reading the three describes already
record. `nextToken` is the same offset cursor, refused when no previous `ListJobs` returned it.

ListJobs also gained its published route, `POST /v1/listjobs`. Its members live in a body, which the
legacy `GET /v1/jobs` route cannot carry, so that is the path an SDK call arrives on; the legacy
route still answers, with every member absent.

## A job's status progresses through the seven published values

The Batch user guide's *Job states* page: *"When you submit a job to an AWS Batch job queue, the job
enters the `SUBMITTED` state. It then passes through the following states until it succeeds … or
fails"*, and `API_ListJobs`' `jobStatus` publishes `SUBMITTED | PENDING | RUNNABLE | STARTING |
RUNNING | SUCCEEDED | FAILED`. Substrate used to record a job `SUCCEEDED` in the request that
submitted it, so six of the seven were unreachable and a consumer's waiter exited on its first poll
([#1248](https://github.com/scttfrdmn/substrate/issues/1248)).

A submitted job's record now holds `SUBMITTED`, and what `DescribeJobs` and `ListJobs` report is
the job's [progression](/reference/concepts/seeds-and-replay#how-a-progression-is-seeded):

- **Unseeded**, the first observation reports `SUCCEEDED`. How many observations a default
  progression spends in each state is unpublished; substrate's choice is none, so a fixture written
  before the fix still reads one describe, one success.
- **Seeded**, the job spends the seed's count of observations in each transient state it names,
  walked in the published order, then reports the seed's terminal state: `SUCCEEDED`, or `FAILED`
  with the seed's `statusReason`, which makes a waiter's failure branch reachable.
- A job `TerminateJob` or `CancelJob` ended reports its record, `FAILED` with the caller's
  `reason`, whatever the seed says. A seed never rewrites the record.

```
POST   /v1/batch/job-status  {"jobId":"*","transientObservations":{"RUNNABLE":1,"RUNNING":2},
                              "finalState":"FAILED","statusReason":"Essential container exited"}
DELETE /v1/batch/job-status  (all seeds; ?jobId=… for one)
```

`jobId` is a job's ID or `"*"`, resolved ID first. `transientObservations` names only transient
states, and `finalState` only `SUCCEEDED` or `FAILED`; anything else, or a negative count, is
refused 400. Terminating and cancelling decide on the state the job is in now, peeked rather than
observed, so ending a job spends none of the caller's polling budget. A seed is a control-plane
write, so it is recorded and a replay re-issues it in position.

## Resources are scoped to the caller

A compute environment, job queue and job definition is recorded against the account and
Region of the request that created it, and reported only to a caller in that scope, as
every other partitioned plugin does. The ARN a create returns therefore names the
caller's own account and Region, and is an identifier that caller's `SubmitJob` and
`computeEnvironmentOrder` can use. A job definition's revision counter is scoped the
same way, so two accounts each registering one definition of the same name both see
revision 1.

## The account and Region a job carries reach no response

`BatchJob` declares its account and Region under wire-visible `json` tags, because the record is what
`MemoryStateManager` snapshots and a replay reads back. Neither reaches a body: `DescribeJobs` used to answer the record whole, and answers `batchJobToWire`'s projection onto `API_JobDetail` now, with the published `jobArn` added. `ListJobs` answers `batchJobSummary`, and the rest answer maps.
`TestBatchWire_JobResponsesCarryNoBookkeepingMember` in `emulator/batch_wire_test.go` drives all four job operations and walks each decoded document for either member at any
depth ([#756](https://github.com/scttfrdmn/substrate/issues/756)). `createdAt` stays epoch milliseconds, the published Long. `startedAt` is published as required and absent, because the record does not hold it.

## Cost

Batch itself is free; the compute it launches is not, and substrate launches none.
