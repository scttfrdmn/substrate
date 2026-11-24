# HealthOmics

**Endpoint:** `omics.{region}.amazonaws.com`

**Protocol:** REST-JSON, path-routed. API version 2022-11-28.

Five operations, all on workflow runs. A run's state is keyed by account and Region.

No workflow is executed. `StartRun` records the workflow ID, role and output URI as intent. Unseeded,
the run is `COMPLETED` the moment it is created, the nominal success path. Seeded, it passes through
the published start-up statuses first; see [A run's status progresses only when
seeded](#a-run-s-status-progresses-only-when-seeded).

## Supported operations

| Operation | Notes |
|-----------|-------|
| StartRun | `POST /run`. Answers HTTP 201 with seven of the eight published members, refuses a request missing `outputUri`, `requestId` or `roleArn`, and starts a run once per `requestId` — see [StartRun's members, refusals and requestId](#startrun-s-members-refusals-and-requestid) (#1166) |
| GetRun | `GET /run/{id}`. Answers the thirteen published members the record models (`arn`, `uuid`, `runOutputUri`, `networkingMode` and `tags` joined in #1166) of the roughly forty-four published; `creationTime`, `startTime`, `stopTime`, `outputUriPrefixMode` and the whole resource-usage set are absent |
| ListRuns | `GET /run`. `items` of `id`, `status` and `name` only, where `RunListItem` publishes ten members. Each run listed is observed, as `GetRun` observes it. `maxResults`, `startingToken`, `name`, `runGroupId` and `status` are ignored and no `nextToken` is emitted |
| CancelRun | `POST /run/{id}/cancel`, 202 with an empty body. An active run passes through `STOPPING` to `CANCELLED`; a settled one is `ConflictException`/409. See [Cancelling and deleting a run](#cancelling-and-deleting-a-run) |
| DeleteRun | `DELETE /run/{id}`, 202 with an empty body. Only a `COMPLETED`, `FAILED` or `CANCELLED` run; any other is `ConflictException`/409 |

Nothing else is routed: `ListRunTasks`, `GetRunTask`, the workflow surface
(`CreateWorkflow`, `GetWorkflow`, `ListWorkflows`), run groups, sequence and reference stores, the
read-set and annotation-store import jobs, and the three tag operations are all absent. HealthOmics
resources are not scanned by the Resource Groups Tagging API either.

## StartRun's members, refusals and requestId

`API_StartRun` publishes eight response members — `arn`, `configuration`, `id`, `networkingMode`,
`runOutputUri`, `status`, `tags` and `uuid`. Until
[#1166](https://github.com/scttfrdmn/substrate/issues/1166) Substrate answered `id` and `status`; it
now answers seven:

| Member | What Substrate answers |
|--------|------------------------|
| `arn` | `arn:aws:omics:{region}:{account}:run/{id}`, the run resource's form in the Service Authorization Reference |
| `id` | a ten-digit run ID — see [Run IDs](#run-ids) |
| `uuid` | an RFC 4122 version-4 UUID, minted at start and persisted |
| `status` | what the run's first observation would report, peeked rather than observed, so a `"*"` seed's countdown is not spent by the start (#1371) |
| `networkingMode` | the request's, or `RESTRICTED`, the default the request member publishes |
| `runOutputUri` | `{outputUri}/{id}`, or `outputUri` itself under `outputUriPrefixMode` `NONE_WITH_OVERWRITE` |
| `tags` | the request's tags, when it sends any |

**`configuration` is omitted.** It is a `ConfigurationDetails` (`arn`, `name`, `uuid`) resolved from
the request's `configurationName`, and Substrate models no HealthOmics configuration resource to
resolve one against (#1569).

`runOutputUri` follows the user guide's *HealthOmics run outputs* page: under the default
`SERVICE_DEFINED` mode the outputs go "under a prefix named after the run ID", and under
`NONE_WITH_OVERWRITE` "directly to the output location"; a trailing slash on `outputUri` is
insignificant. WDL and CWL runs nest one level further, under an `out` prefix. Substrate models no
workflow, so it knows no engine, and answers the run-ID prefix both engines share. `GetRun` answers
`arn`, `uuid`, `runOutputUri`, `networkingMode` and `tags` as `StartRun` does.

**Refusals.** The page marks `outputUri`, `requestId` and `roleArn` `Required: Yes`. A request is
`ValidationException`/400, the page's code, when it misses any of them, names a `roleArn` outside the
published pattern `arn:.+`, or sends a `networkingMode` or `outputUriPrefixMode` outside its
published values. The messages are Substrate's own. `requestId` is an idempotency token in the API
model, so an AWS SDK fills it in when the caller does not; only a hand-built request is refused for
it.

**`requestId`.** *"If you provide request IDs, AWS HealthOmics identifies duplicate requests and starts
the run only once."* A `StartRun` whose `requestId` names a run already started in the account and
Region answers 201 with that run and starts nothing. The page publishes neither the token's lifetime
nor what a retry with a different body gets, so both readings are Substrate's:

- the token lives as long as its run, so `DeleteRun` releases it;
- the first run is answered whatever the retry's body says, since the page describes a duplicate as
  starting "the run only once" and publishes no conflict code for it.

## A run's status progresses only when seeded

`API_GetRun` publishes `PENDING | STARTING | RUNNING | STOPPING | COMPLETED | DELETED | CANCELLED |
FAILED`. Unseeded, a run is `COMPLETED` from its first observation. A seed makes the start-up
observable, using the countdown every progressing resource shares (see [How a progression is
seeded](/reference/concepts/seeds-and-replay#how-a-progression-is-seeded)):

```
POST   /v1/omics/run-status   {"runId": "…" | "*", "pendingObservations": N,
                               "state": "…", "finalState": "COMPLETED" | "FAILED",
                               "failureReason": "…", "stoppingObservations": N}
DELETE /v1/omics/run-status   (every seed; ?runId=… for one)
```

- While the countdown runs, the n-th observation reports the n-th published start-up status:
  `PENDING`, `STARTING`, then `RUNNING` for every observation after the third. `state` pins a single
  one instead.
- Once it is spent, the run settles at `finalState`, `COMPLETED` by default. A `FAILED` run answers
  the seed's `failureReason`, which has no default and is at most 64 characters, as `API_GetRun`
  constrains it.
- `GetRun` and `ListRuns` observe; `StartRun`, `CancelRun` and `DeleteRun` only peek. Each run keeps
  its own count, so one `ListRuns` over five runs spends one observation of each.
- A seed refused for a state outside the published start-up or final statuses is 400. `STOPPING` is
  not seedable as a start-up state, and `CANCELLED` not as a final one: both are what `CancelRun`
  produces.
- The seed is a control-plane write, so a replay re-issues it in position, and a recorded run's
  statuses replay identically (`TestOmicsRun_AProgressionReplaysIdentically`).

## Cancelling and deleting a run

`API_CancelRun` is `POST /run/{id}/cancel`, answering HTTP **202** with an empty body; it answered
204 until [#1165](https://github.com/scttfrdmn/substrate/issues/1165). It applies only to a run
that has not settled: `ConflictException` (409), "the request cannot be applied to the target
resource in its current state", is published, and a `COMPLETED`, `FAILED` or `CANCELLED` run is
in no state a cancel applies to. An unseeded run is `COMPLETED` at once, so cancelling one is
refused.

A cancel of an active run writes `CANCELLED` and restarts the run's countdown. The next
`stoppingObservations` observations (one by default, so a cancel always passes through it) report
`STOPPING`, and the run then reports `CANCELLED`, spelled as `API_GetRun` and `API_RunListItem`
publish it (#1364).

`DELETE /run/{id}` is `API_DeleteRun`, which until #1165 was routed to `CancelRun`. It answers 202
with an empty body and accepts only a run "that has reached a `COMPLETED`, `FAILED`, or `CANCELLED`
stage"; any other is `ConflictException`/409. It removes the run's record, its list entry and its
countdown, so `GetRun` then answers `ResourceNotFoundException`, as the page says it will.
`DELETED`, which the enum also publishes, is therefore never observed: a deleted run is not found.

## What a refusal reports

| Condition | Code | Status |
|-----------|------|--------|
| a body that will not parse, or a `StartRun` missing a required member or outside a published constraint | `ValidationException` | 400 |
| a run that does not exist | `ResourceNotFoundException` | 404 |
| `CancelRun` of a settled run, or `DeleteRun` of an active one | `ConflictException` | 409 |

All three match the published code and status on the pages that carry them.
`AccessDeniedException`, `InternalServerException`,
`RequestTimeoutException`, `ServiceQuotaExceededException` and `ThrottlingException` are published
and have no site: no quota or transient failure is modelled.

## Run IDs

A run ID is a ten-digit number with no leading zero, and the run's `uuid` is minted with it. Both are
derived from the request's own ID (the per-request mint every plugin shares), so a recorded run replays
with the run ID and `uuid` it was recorded with, in any process. Until #1166 the run ID came from a
per-process pseudo-random source that `ResetForRun` rewound: a replay in the recording process
reproduced it, a replay in another process did not. Nothing checks that a freshly minted ID is
unused; the derivation makes a collision vanishingly unlikely rather than impossible.

## The account and Region a run carries reach no response

`OmicsRun` declares its account and Region under wire-visible `json` tags, because the record is what
`MemoryStateManager` snapshots and a replay reads back, and since #1166 the `requestId` it was started
with. None of the three reaches a body: `GetRun` used to answer the record whole, and answers `omicsRunToWire` now. `StartRun` and `ListRuns` answer members built one by one.
`TestOmicsWire_RunResponsesCarryNoBookkeepingMember` in `emulator/omics_wire_test.go` drives the three routed operations that answer a body and walks each decoded document for either member at any
depth ([#756](https://github.com/scttfrdmn/substrate/issues/756)). `CancelRun` and `DeleteRun` answer 202 with no body, so they have nothing to walk.

## CloudFormation resource types

None. AWS publishes `AWS::Omics::*` types for workflows, run groups and stores, and Substrate deploys
none of them, so a run exists only if `StartRun` creates it.

## Cost

`StartRun` is attributed $0.001 per call. Real HealthOmics bills a run by the compute and storage it
consumes for as long as it runs; Substrate runs nothing, so the attribution is a flat per-run proxy
and no run is more expensive than another.
