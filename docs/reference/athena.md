# Athena

**Endpoint:** `athena.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AmazonAthena.{Op}`)

**Routing:** Athena's target prefix carries no API version date — `AmazonAthena` is what every SDK
sends, and `parser.go` aliases `amazonathena` to the `athena` plugin. The service model's API version
is `2017-05-18` and appears in no wire field.

## Supported operations

| Operation | Notes |
|-----------|-------|
| StartQueryExecution | `QueryString` required; the query is `SUCCEEDED` when the call returns unless [a seed makes it progress](#a-query-progresses-under-a-seed) |
| GetQueryExecution | Reports `QueryExecutionId`, `Query`, `WorkGroup`, `Status` and `ResultConfiguration.OutputLocation`, and nothing else; each call is [one observation of a seeded progression](#a-query-progresses-under-a-seed) |
| GetQueryResults | Returns [the result set seeded for the execution's SQL](#the-result-set-is-seeded-by-sql-text) once the query has `SUCCEEDED`, and refuses it with `InvalidRequestException`/400 before; `MaxResults` and `NextToken` are not read |
| StopQueryExecution | Writes `CANCELLED` ([#1154](https://github.com/scttfrdmn/substrate/issues/1154)); accepted for any stored execution, including one already `SUCCEEDED` |
| ListQueryExecutions | `MaxResults` defaults to 50; 0–50 is honoured, 0 as a final empty page with no `NextToken`, and anything else is `InvalidRequestException` (#1185) |
| CreateWorkGroup | `Name` and `Description` are read; `Configuration` and `Tags` are discarded |
| GetWorkGroup | Reports `Name`, `State` and `Description`, and nothing else; [synthesises the `primary` workgroup](#the-primary-workgroup-and-what-is-synthesised-about-it) when no record was written for it |
| DeleteWorkGroup | Refuses `primary` with `InvalidRequestException`/400 and *"The primary workgroup cannot be deleted"*, which `API_DeleteWorkGroup` itself states; `RecursiveDeleteOption` is not read |
| ListWorkGroups | `MaxResults` defaults to 50; outside 1–50 is `InvalidRequestException` (#1185); [the `primary` workgroup is prepended](#the-primary-workgroup-and-what-is-synthesised-about-it) and pages like any other entry |

## A query progresses under a seed

`StartQueryExecution` stores the execution with `State: "SUCCEEDED"` and `SubmissionDateTime` equal to
`CompletionDateTime`, both read from the simulated clock. Unseeded, that is what every
`GetQueryExecution` reports, so a poll loop exits on its first observation. A seed makes the published
transient states observable first ([#1155](https://github.com/scttfrdmn/substrate/issues/1155)), under
the rules [every progression shares](/reference/concepts/seeds-and-replay#how-a-progression-is-seeded):

```
POST   /v1/athena/query-status   {"queryExecutionId": "…" | "*", "pendingObservations": 2,
                                  "state": "QUEUED", "finalState": "FAILED",
                                  "stateChangeReason": "…",
                                  "athenaError": {"ErrorCategory": 2, "ErrorType": 1006,
                                                  "Retryable": false, "ErrorMessage": "…"}}
DELETE /v1/athena/query-status   (all seeds; ?queryExecutionId=… for one)
```

- `GetQueryExecution` reports `state` — `QUEUED` or `RUNNING`, default `RUNNING` — for
  `pendingObservations` calls, with no `CompletionDateTime`, since the query has not completed. It then
  reports `finalState` (`SUCCEEDED`, `FAILED` or `CANCELLED`; default the record's own) with
  `CompletionDateTime`, and with the seed's `StateChangeReason` and `AthenaError` — the two members
  `QueryExecutionStatus` publishes for a failure. A state outside `QueryExecutionStatus.State`'s Valid
  Values, or an `AthenaError` outside `API_AthenaError`'s published ranges (`ErrorCategory` 1–3,
  `ErrorType` 0–9999), is refused with 400 and not stored.
- `GetQueryResults` answers only a query that has `SUCCEEDED`; before then it is
  `InvalidRequestException`/400, the code `API_GetQueryResults` publishes for a bad request. The page
  gives no sentence for the case, so substrate's two — *"Query has not yet finished. Current state:
  RUNNING"* and *"Query did not finish successfully. Final query state: FAILED"* — are its own reading.
  The check peeks rather than observes, so fetching results never spends the countdown a
  `GetQueryExecution` loop is waiting through.
- `StopQueryExecution` writes `CANCELLED`, two `L`s, as `QueryExecutionStatus.State` spells it
  ([#1154](https://github.com/scttfrdmn/substrate/issues/1154); it wrote `CANCELED` before). The record's
  `CANCELLED` outranks a seed: a stop is the caller's own action, so a query stopped mid-countdown reads
  as stopped from then on.
- `ListQueryExecutions` answers identifiers only, so it has no status to disagree with
  `GetQueryExecution` about.

A stop is still accepted for a query that has already `SUCCEEDED`, which it cancels; real Athena leaves
a finished query as it finished. That divergence predates the seed and is unchanged.

## The result set is seeded by SQL text

`GetQueryResults` returns rows from a seeded result set rather than executing anything — executing the
SQL is workload-internal and out of scope. Seeds are written over the control plane:

```
POST   /v1/athena/results     {"sql": "SELECT 1", "columnMetadata": [...], "rows": [...]}
DELETE /v1/athena/results     (all seeds; ?sql=… for one)
```

Lookup order is exact SQL, then the `"*"` wildcard, then an empty result set. The SQL match is exact
string equality, so a difference in whitespace or case misses the seed and reports zero rows rather
than refusing. Seeds live in the `athena-ctrl` namespace keyed `result:{sql}` and are **not** scoped by
account or Region: one seed serves every caller of the emulator.

## The primary workgroup, and what is synthesised about it

Every AWS account has a `primary` workgroup. `API_DeleteWorkGroup`'s own description states *"The
primary workgroup cannot be deleted"*, and the user guide's *Manage workgroups* page repeats the
sentence verbatim, so both the existence and the refusal are published by the API reference rather than
inferred from the guide.

Substrate still writes no record for it — there is nothing to write it in response to — and instead
synthesises one on read. One producer and one reader, which is the whole of
[#1222](https://github.com/scttfrdmn/substrate/issues/1222): until it was fixed, `GetWorkGroup`
synthesised the record while `ListWorkGroups` read only the workgroup-names index that `CreateWorkGroup`
alone appends to, so `GetWorkGroup("primary")` answered 200, `ListWorkGroups` answered `[]`, and
`ListQueryExecutions` reported every unqualified query under a workgroup the listing said did not
exist — three answers that cannot all be true of one account. `DeleteWorkGroup` compounded it by
answering `WorkGroup primary not found`: the right code for the wrong reason.

What is read and what is substrate's own:

| Member | Where it comes from |
|--------|---------------------|
| `Name` | `primary`, published by `API_DeleteWorkGroup` and by the user guide |
| `State` | `ENABLED` — the workgroup is usable (an unqualified `StartQueryExecution` is attributed to it and runs), and `API_WorkGroupSummary` publishes only `ENABLED` and `DISABLED`, so a usable workgroup has exactly one available value |
| `Description` | `Primary workgroup` — **substrate's own placeholder.** AWS publishes no description for it, and a real primary workgroup has none. Assert on `Name` and `State`; treat this string as arbitrary |

The synthesised entry is **prepended** to the listing, not appended, and the position is part of the
contract rather than cosmetic. `ListWorkGroups` pages by an offset into this order
([#1086](https://github.com/scttfrdmn/substrate/issues/1086)); the primary workgroup exists before any
workgroup a caller creates, so creation order puts it first, and prepending is the only position that
leaves every other entry's offset unchanged — appending would move it on each `CreateWorkGroup` and
leave a token issued mid-walk pointing at a different element.

A caller may create its own workgroup named `primary`. Then the stored record wins both readers, the
listing reports it once (the duplicate guard is against the names index, which is what the walk orders),
and `DeleteWorkGroup` still refuses it — the refusal is on the name, not on the absence of a record,
because AWS's statement is flat.

`Description` is omitted from both readers' answers when it is empty, rather than sent as `""`: both
`API_WorkGroup` and `API_WorkGroupSummary` give it *Required: No* with a minimum length of 0, and a
workgroup created without a description has none rather than an empty one. Before #1222 `GetWorkGroup`
sent the empty string while `ListWorkGroups` omitted the member, so the two readers differed over a
workgroup neither was wrong about. `EngineVersion` and `IdentityCenterApplicationArn` are published
`WorkGroupSummary` members that substrate does not carry, so neither reader reports them.

## What a refusal reports

Athena has one refusal code, and it covers every condition:

| Condition | Code | Status |
|-----------|------|--------|
| a body that will not parse, or is absent where a member is required | `InvalidRequestException` | 400 |
| a required member absent or empty | `InvalidRequestException` | 400 |
| a query execution or workgroup that does not exist | `InvalidRequestException` | 400 |
| a workgroup name already in use | `InvalidRequestException` | 400 |
| `DeleteWorkGroup` naming the `primary` workgroup | `InvalidRequestException` | 400 |

The code is published — `GetQueryResults` declares `InternalServerException`/500,
`InvalidRequestException`/400 and `TooManyRequestsException`/400, and Athena's consolidated
*Common Error Types* list adds fifteen more — but the **conditions** Substrate attaches to it are
broader than any page's, and in particular a not-found reports the same code as a missing parameter.
Nothing in the plugin answers 404 or 500. `MetadataException`, `ResourceNotFoundException` and
`TooManyRequestsException` are published and have no site.

Both paginators now **refuse** a `NextToken` neither of them issued, with `InvalidRequestException`/400
and a message naming the operation, rather than reading it as page one
([#1086](https://github.com/scttfrdmn/substrate/issues/1086)). What each page publishes about the token
and what the refusal takes as its own reading are set out under
[Athena's two listings refuse a token](/reference/concepts/listing-order#athena-s-two-listings-refuse-a-token-and-the-page-says-where-a-token-comes-from)
above. Neither operation enforces the published `MaxResults` range, which is a separate defect: a value
above the maximum of 50 is honored and one at or below zero is silently rewritten to 50.

## The account and Region a record carries reach no response

`AthenaQuery` declares its account and Region under wire-visible `json` tags, because the record is what
`MemoryStateManager` snapshots and a replay reads back. Neither reaches a body: every response is a map built member by member.
`TestAthenaWire_QueryResponsesCarryNoBookkeepingMember` in `emulator/athena_wire_test.go` drives all nine routed operations and walks each decoded document for either member at any
depth ([#756](https://github.com/scttfrdmn/substrate/issues/756)).

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::Athena::WorkGroup | Name | A stub: properties are recorded in the CloudFormation stub store, which the Athena plugin does not read, so a workgroup deployed from a template is invisible to `GetWorkGroup` and `ListWorkGroups` |

## The owning account a record carries reaches no response, and dates are epoch seconds

`SSOInstance`, `SSOPermissionSet` and `SSOAccountAssignment` each declare the owning account as
`AccountID`, because the record is what `MemoryStateManager` snapshots and a replay reads back. Until
[#756](https://github.com/scttfrdmn/substrate/issues/756), `CreatePermissionSet` and
`DescribePermissionSet` answered the permission set whole. That put `AccountID` on the wire, plus
`InstanceArn`, which `API_PermissionSet` does not publish: the request names the instance. They now
answer `ssoPermissionSetOut` (`emulator/sso_wire.go`), which carries all six published members.

`CreatedDate` rendered as an RFC3339 string on the permission set and in `ListInstances`. The service
speaks `awsJson1_1`, where both pages publish `"CreatedDate": number`, so a typed SDK could decode
neither response ([#1345](https://github.com/scttfrdmn/substrate/issues/1345)). Both are now epoch
seconds. The stored records are unchanged.

`TestSSOWire_ResponsesCarryNoBookkeepingMember` drives all twelve routed operations. It compares
member names **exactly**, not folded: the assignment operations publish `AccountId`, the target
account, which differs from the bookkeeping `AccountID` only in case.

## Cost

`StartQueryExecution` is attributed $0.000005 per call, standing in for Athena's $5.00 per TB scanned.
Substrate models no scan volume, so every query costs the same regardless of its SQL or its seeded
result set.
