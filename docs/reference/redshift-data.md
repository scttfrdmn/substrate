# Redshift Data API

**Endpoint:** `redshift-data.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: RedshiftData.{Op}`)

## Supported operations

| Operation | Notes |
|-----------|-------|
| ExecuteStatement | `Sql` required; the statement is recorded `FINISHED`, and [a seed governs what a describe reports](#a-statement-s-status-is-seeded-and-read-when-it-is-described). `SecretArn` is decoded and not stored; `Parameters`, `StatementName`, `WithEvent` and `ClientToken` are not read |
| DescribeStatement | Reports `Id`, `Status`, `QueryString`, `CreatedAt`, `UpdatedAt`, `HasResultSet` and `Error`, and [nothing else](#describestatement-reports-seven-members). Each describe is one observation of a seeded statement |
| GetStatementResult | Returns [the result set seeded for the statement's SQL](#the-result-set-is-seeded-by-the-statement-s-sql) for a `FINISHED` statement, and refuses any other with `ValidationException`; `NextToken` is not read and no `NextToken` is emitted |

`BatchExecuteStatement`, `CancelStatement`, `DescribeTable`, `ListDatabases`, `ListSchemas`,
`ListStatements`, `ListTables` and `GetStatementResultV2` are not routed.

## A statement's status is seeded, and read when it is described

A statement is recorded `FINISHED`: substrate runs no SQL, so the work is done in the request that
starts it. A seed governs what each `DescribeStatement` *reports*, read at that describe rather than
frozen onto the statement when it ran, so a seed written after `ExecuteStatement` governs that
statement too ([#1163](https://github.com/scttfrdmn/substrate/issues/1163),
[#1155](https://github.com/scttfrdmn/substrate/issues/1155)). It follows
[the rules every progression shares](/reference/concepts/seeds-and-replay#how-a-progression-is-seeded).

```
POST   /v1/redshift-data/status   {"statementId": "…", "pendingObservations": 2, "transientStatus": "STARTED", "status": "FAILED", "errorMessage": "query timed out"}
DELETE /v1/redshift-data/status   (all seeds; ?statementId=… for one)
```

- **Keyed by statement:** `statementId` names one statement, and empty or `"*"` governs every
  statement, the exact ID winning. It used to be one global value under the literal key `status`.
- **Counted:** `pendingObservations` describes report `transientStatus` (`SUBMITTED`, `PICKED` or
  `STARTED`, default `STARTED`) before the statement settles to `status`. `status` may be any of the
  six published values (`SUBMITTED | PICKED | STARTED | FINISHED | ABORTED | FAILED`), defaulting to the
  recorded `FINISHED`. The handler used to refuse `SUBMITTED` and `PICKED`.
- **`errorMessage`** is reported as `Error` alongside a settled `FAILED`, and refused with any other
  status.
- **Backward compatible:** the pre-#1163 body, `{"status": "FAILED", "errorMessage": "…"}`, still
  parses, as the `"*"` wildcard with no countdown. Statuses are accepted in any case and stored
  upper-cased, as before.
- A status outside its enumeration, or a negative count, is refused with 400.
- A store failure reading the seed is an error, not an absent seed.

`GetStatementResult` *peeks*: it spends no observation, and answers a result only for a statement whose
next describe would report `FINISHED`. Any other statement, including one seeded `FAILED` or `ABORTED`,
which used to answer its seeded rows, is `ValidationException`/400, the client-error code
`API_GetStatementResult` publishes.

## The result set is seeded by the statement's SQL

```
POST   /v1/redshift-data/results   {"sql": "SELECT 1", "columnMetadata": [...], "records": [...]}
DELETE /v1/redshift-data/results   (all seeds; ?sql=… for one)
```

Lookup order is a Go-level in-memory map (exact SQL, then `"*"`), then the control-plane state (exact
SQL, then `"*"`), then an empty result set. The match is exact string equality on the SQL the statement
was created with. `TotalNumRows` is the number of seeded records.

## DescribeStatement reports seven members

`Id`, `Status`, `QueryString`, `CreatedAt`, `UpdatedAt`, `HasResultSet` and, for a failed statement,
`Error` ([#1163](https://github.com/scttfrdmn/substrate/issues/1163)).
- **`HasResultSet`** is `true` exactly when the reported status is `FINISHED`, the statements for which
  `GetStatementResult` answers a result. The page says the result set "can be empty", and an unseeded
  one is.
- **`UpdatedAt`** is the simulated instant a seeded statement was first observed in its settled status
  after a countdown, so it is later than `CreatedAt` when the clock moved while the statement ran. A
  statement nothing seeds is created settled and never changes, so its `UpdatedAt` is its `CreatedAt`.
- **Absent:** `Duration`, `ResultRows`, `ResultSize`, `RedshiftPid`, `RedshiftQueryId`,
  `WorkgroupName`, `ClusterIdentifier`, `Database`, `DbUser`, `SecretArn`, `SessionId`,
  `SubStatements` and `QueryParameters`.

The two timestamps are emitted as epoch seconds, which is what the protocol's JSON version specifies.
Responses carry `Content-Type: application/x-amz-json-1.1`, the Redshift Data API's protocol; they
carried `application/json` until #1163.

## What a refusal reports

| Condition | Code | Status |
|-----------|------|--------|
| a body that will not parse | `ValidationException` | 400 |
| `Sql` or `Id` absent or empty | `ValidationException` | 400 |
| a statement that does not exist | `ResourceNotFoundException` | 400 |
| `GetStatementResult` for a statement that is not `FINISHED` | `ValidationException` | 400 |

Both codes and both statuses are what `API_DescribeStatement` publishes.
`ActiveStatementsExceededException`, `ActiveWaitingRequestsExceededException`,
`BatchExecuteStatementException`, `ExecuteStatementException`, `DatabaseConnectionException` and
`InternalServerException` are published and have no site: Substrate holds no connection, enforces no
concurrency ceiling, and has no internal failure to report.

## The account and Region a record carries reach no response

`RedshiftDataStatement` declares its account and Region under wire-visible `json` tags, because the record is what
`MemoryStateManager` snapshots and a replay reads back. Neither reaches a body: `DescribeStatement` answers a map built member by member.
`TestRedshiftDataWire_StatementResponsesCarryNoBookkeepingMember` in `emulator/redshiftdata_wire_test.go` drives all three routed operations and walks each decoded document for either member at any
depth ([#756](https://github.com/scttfrdmn/substrate/issues/756)). The same test pins `CreatedAt` and `UpdatedAt` as the numbers `API_DescribeStatement` publishes.

## CloudFormation resource types

None. AWS publishes no CloudFormation resource type for the Redshift Data API — a statement is an
action, not a resource.

## Cost

Nothing is attributed. The Redshift Data API itself is free; a query's cost falls on the cluster or
Serverless workgroup that runs it, which Substrate does not model.
