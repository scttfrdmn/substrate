# Timestream

**Endpoint:** `ingest.timestream.{region}.amazonaws.com` (write) and
`query.timestream.{region}.amazonaws.com` (query)
**Protocol:** JSON 1.0
**Routing:** `X-Amz-Target: Timestream_20181101.{Operation}`

Substrate models Timestream's database and table control plane, accepts records
without interpreting them, and answers `Query` from a **seeded result set**.
Twelve of the API's thirty-three operations are routed, spanning two API versions
that AWS publishes as separate references — `timestream-write-2018-11-01` and
`timestream-query-2018-11-01` — behind one plugin and one dispatch switch. The
single most important thing to know before writing a test is that a seeded query
result is keyed by account, Region and query string, with `*` a wildcard in each,
so a seed installed without `accountId` and `region` serves every caller of the
emulator, and a wildcard seed left behind by an earlier test will answer a later
one.

## Supported operations

| Operation | `X-Amz-Target` | Notes |
|---|---|---|
| CreateDatabase | `Timestream_20181101.CreateDatabase` | A duplicate is `ConflictException`/400 |
| DescribeDatabase | `Timestream_20181101.DescribeDatabase` | |
| DeleteDatabase | `Timestream_20181101.DeleteDatabase` | Answers `{}`; see *The two deletes answer an empty object* |
| ListDatabases | `Timestream_20181101.ListDatabases` | Pages by `MaxResults` (1–20) and `NextToken` |
| CreateTable | `Timestream_20181101.CreateTable` | A duplicate is `ConflictException`/400 |
| DescribeTable | `Timestream_20181101.DescribeTable` | `TableStatus` [progresses under a seed](#a-table-s-status-progresses-under-a-seed-and-tablecount-never-moves) |
| DeleteTable | `Timestream_20181101.DeleteTable` | Answers `{}`; see *The two deletes answer an empty object*. [Under a seed the table is `DELETING` for a counted window first](#a-table-s-status-progresses-under-a-seed-and-tablecount-never-moves) |
| ListTables | `Timestream_20181101.ListTables` | Pages by `MaxResults` (1–20) and `NextToken`; `DatabaseName` narrows, and absent lists every database's tables |
| WriteRecords | `Timestream_20181101.WriteRecords` | `Records` must hold 1–100 records |
| DescribeEndpoints | `Timestream_20181101.DescribeEndpoints` | |
| Query | `Timestream_20181101.Query` | `QueryString` required; pages by `MaxRows` (1–1000) and `NextToken` |
| CancelQuery | `Timestream_20181101.CancelQuery` | `QueryId` required; answers `CancellationMessage`; see *CancelQuery answers which cancellation happened* |

The other twenty-one operations are not routed and refuse with
`UnknownOperationException` at 404, which is what both Timestream Common Errors
pages publish for an unrecognised action — a live citation rather than a
substituted code. That covers `UpdateDatabase`, `UpdateTable`, the three tagging
operations, the whole batch-load family (`CreateBatchLoadTask`,
`DescribeBatchLoadTask`, `ResumeBatchLoadTask`, `ListBatchLoadTasks`), the account
settings pair, the six scheduled-query operations, and `PrepareQuery`. Which
endpoint an operation arrives on is enforced; see
[Endpoint discovery is enforced](#endpoint-discovery-is-enforced) below.

## A seeded query result is scoped by account and Region

`Query` returns a seeded result when one applies, or — for `SELECT * FROM db.table`
alone — the table's records in its logical shape. Any other unseeded query is
refused; see [What an unseeded Query answers](#what-an-unseeded-query-answers).
Seeds are installed and cleared through the control plane:

```
POST   /v1/timestream-query/results   {"queryString": "…", "accountId": "…", "region": "…", "result": {…}}
DELETE /v1/timestream-query/results   ?queryString=…&accountId=…&region=…
```

Seeds live in the `timestream-ctrl` namespace keyed
`result:{accountId}/{region}/{queryString}`, scoped like every other Timestream key
— `db:{acct}/{region}/{name}`, `table:{acct}/{region}/{db}/{name}` and the records
key. `accountId` and `region` are optional and each defaults to `*`, which matches
any account or any Region, so a single-account fixture can seed without knowing its
own account ID; the AWS Config seeds follow the same convention. `queryString` is
**required**, and the wildcard query is the explicit value `"*"`: an omitted or empty
`queryString` is refused at 400, where it once silently became the wildcard and a
seed intended for one query came to answer all of them
([#1200](https://github.com/scttfrdmn/substrate/issues/1200)).

A `Query` looks for the exact query string first and the `"*"` query second, and
within each tries the caller's account and Region, then its account in any Region,
then its Region in any account, then any. So a seed for the exact query in any scope
outranks a wildcard seed in the caller's own scope.

A `DELETE` narrows by each of `queryString`, `accountId` and `region` it is given, and
an omitted one matches every value: `?accountId=A` removes account A's seeds in every
Region and for every query, `?region=R` every account's seeds in Region R,
`?queryString=Q` the seeds for Q in every scope, and no parameter at all every seed. A
value given is matched literally against the scope a seed was installed under, so
`?accountId=*` removes only the seeds installed for any account.

The seed is otherwise barely validated: the handler refuses a body that fails to
decode, a missing `queryString`, and a null `result`, so a result whose row arity
disagrees with its `ColumnInfo` is stored and returned verbatim. A state-layer
failure reading a seed is an error, not an absent seed ([#1209](https://github.com/scttfrdmn/substrate/issues/1209)).
Seeds installed before this change were keyed `result:{queryString}` and are not read;
re-seed, or reset state, after upgrading.

## Every Timestream timestamp is epoch seconds

`API_Database` and `API_Table` publish `CreationTime` and `LastUpdatedTime` as `Type: Timestamp`,
which Timestream Write's JSON protocol renders as a number of epoch seconds with a fraction. Every
database and table response (`CreateDatabase`, `DescribeDatabase`, `ListDatabases`, `CreateTable`,
`DescribeTable`, `ListTables`) answers both that way, to three decimals, through
`emulator/timestream_wire.go`.

Until [#1207](https://github.com/scttfrdmn/substrate/issues/1207) the records declared both as
strings and wrote whole-second RFC3339, which an SDK deserializing a timestamp refuses, so every one
of those calls failed inside the client. The records hold `time.Time` now, so a create keeps its
sub-second instant and two in one second are orderable. A record written before the fix holds the
old RFC3339 string, which still decodes and answers the same instant as epoch seconds
(`TestTimestreamWire_ARecordWrittenBeforeTheFixStillDecodes`). A run recorded before the fix and
replayed after it diverges on a create made at a sub-second instant, because the record now keeps
the fraction the old one dropped.

## Every refusal answers 400

Every published Timestream error but `InternalServerException` (500) is a 400, across both Common
Errors pages and every routed operation's page: "ConflictException … HTTP Status Code: 400" on
`API_CreateDatabase`, and "ResourceNotFoundException … HTTP Status Code: 400" on every operation that
names a resource. Substrate answered a duplicate database or table `ConflictException` at 409, and an
absent one `ResourceNotFoundException` at 404, so a retry policy keyed on status (retry a 409, fail
fast on a 400) took the wrong branch. Both answer 400 now, with the same codes
([#1198](https://github.com/scttfrdmn/substrate/issues/1198)). `TestTimestreamAudit_ConflictAndNotFoundAnswer400` asserts the code and the
status at each site.

The codes substrate answers are `ValidationException`, `ConflictException`, `ResourceNotFoundException`
and `InvalidEndpointException`, all at 400. These published codes have no site:
- `AccessDeniedException`: authorization is answered by the cross-service IAM gate's own code.
- `ThrottlingException` and `ServiceQuotaExceededException`: substrate models no quota.
- `RejectedRecordsException`: record content is not evaluated.
- `QueryExecutionException`: no query runs.
- `InternalServerException`: a store fault is answered as a 500 by the server, not under this code.

## The two deletes answer an empty object

`DeleteDatabase` and `DeleteTable` answer `{}`. Their pages say "If the action is successful, the
service sends back an HTTP 200 response with an empty HTTP body" and publish no example, so the
literal reading is zero bytes. Substrate keeps `{}` as a recorded decision rather than an accident.
An empty JSON object is how a JSON-protocol response with no members serializes, and every SDK reads it
and an empty body alike. Transfer Family's pages also print `{ }` as the example body for the same
prose, which is the inconsistency on AWS's own pages that makes the choice worth stating
([#1206](https://github.com/scttfrdmn/substrate/issues/1206)). `TestTimestreamAudit_DeletesAnswerAnEmptyObject` pins it. Of the routed Timestream
operations, `CancelQuery` was the one answering `{}` where its page publishes a member; it is
described below.

## CancelQuery answers which cancellation happened

`API_query_CancelQuery` publishes `QueryId` "Required: Yes", 1–64 characters matching `[a-zA-Z0-9]+`,
and a response of `{"CancellationMessage": "string"}`. Before [#1206](https://github.com/scttfrdmn/substrate/issues/1206) the handler read neither
its context nor its body, so every input, including an ID that was never issued, answered `{}`. Every
`Query` is now recorded under its `QueryId`, and `CancelQuery` reads it back:

| Request | Answer |
|---|---|
| No `QueryId`, or one outside 1–64 characters of `[a-zA-Z0-9]` | `ValidationException`/400 |
| A well-formed `QueryId` this account never ran in this Region | `ValidationException`/400 |
| A query with pages still unread, the first time | `"CancellationMessage": "Query cancelled successfully"` |
| A query already cancelled, or one that completed | `"CancellationMessage": "Cancellation message is posted"` |

The page publishes no `ResourceNotFoundException`, only `AccessDeniedException`,
`InternalServerException`, `InvalidEndpointException`, `ThrottlingException` and
`ValidationException`. So an unknown ID is `ValidationException`, not a not-found code borrowed from
another page (#671). The page says "Cancellation is provided only if the query has not completed
running", and substrate's queries complete synchronously, so the one query that has not completed is
a paginated one with pages unread. Its next page after a cancellation is `ConflictException`/400,
"Unable to poll results for a cancelled query.", which `API_query_Query` publishes. The two message
strings are the values #1206 quotes. Neither `API_query_CancelQuery` nor the boto3 reference prints
them, so they are substrate's reading. `TestTimestreamAudit_CancelQuery` pins every row.

## Pagination

**The lists.** `ListDatabases` and `ListTables` read `MaxResults` and `NextToken`. A page size
outside the published "Minimum value of 1. Maximum value of 20" is `ValidationException`/400. So is a
token substrate did not issue: neither page publishes a dedicated bad-token code, so none is borrowed
(#671). An absent `MaxResults` answers the whole listing, as before, since neither page states a
default page size. Names are listed in lexicographic order and paged by offset, and `NextToken` is
omitted on the last page. `ListTables`' `DatabaseName` is "Required: No". Given, it narrows the
listing, and an absent database is `ResourceNotFoundException`/400. Absent, every database's tables
are listed; before, an absent name listed a database called `""`, which has none.

**Query.** `MaxRows` (1–1000) and `NextToken` (1–2048 characters) are read, and the sentence
`API_query_Query` publishes is modeled on the row count: "The initial run of Query with a MaxRows value
specified will return the result set of the query in two cases: … The number of rows in the result set
is less than the value of maxRows. Otherwise, the initial invocation of Query only returns a
NextToken". So a first call whose result has at least `MaxRows` rows answers no rows, its `ColumnInfo`
and a `NextToken`. Each call with the token answers the next `MaxRows` rows under the same `QueryId`,
and the last page carries no token. The result is snapshotted when the query runs, so a `WriteRecords`
between pages does not shift what the next page holds. The 1 MB response limit is not modeled.

A `Query` token is refused with `ValidationException` "Invalid pagination token", the error the page
names, in four cases: substrate did not issue it; it names a query this account and Region did not
run; the request's `QueryString` is not that query's; or the query's last page has been read. The
page's five-use and one-hour token lifetimes are not modeled; a token answers the same page each time,
as the page also says it does. `ClientToken` is checked for its published 32–128 length. Its
idempotency window is not modeled, because substrate's `Query` already answers a repeated query the
same result. `QueryString` is "Required: Yes" and at most 262,144 characters, and an absent one is
`ValidationException`/400 ([#1195](https://github.com/scttfrdmn/substrate/issues/1195), [#1197](https://github.com/scttfrdmn/substrate/issues/1197)).
`TestTimestreamAudit_ListDatabasesPagesByMaxResultsAndOmitsTheLastToken`,
`TestTimestreamAudit_ListTablesDatabaseNameNarrows` and `TestTimestreamAudit_QueryMaxRowsPagesTheResult`
walk each collection past one page.

## A table's status progresses under a seed, and TableCount never moves

`API_Table` publishes `TableStatus` as `ACTIVE | DELETING | RESTORING` and no creating state, so
`CreateTable` answers and records `ACTIVE`. `DescribeTable` and each table `ListTables` reports
observe the status through a seed ([How a progression is seeded](/reference/concepts/seeds-and-replay#how-a-progression-is-seeded)):

```
POST   /v1/timestream-write/table-status
       {"databaseName":"…","tableName":"…","pendingObservations":2,"state":"RESTORING"}
DELETE /v1/timestream-write/table-status           (all; ?table=database/table for one)
```

Naming both `databaseName` and `tableName` targets one table, and naming neither targets every table.
A live table reports `state` — `RESTORING`, the one transient status — for `pendingObservations`
observations, then `ACTIVE`; with no `state` it reads `ACTIVE`. Under a seed with observations to
spend, `DeleteTable` keeps the table marked `DELETING` and restarts its countdown: the next
`pendingObservations` observations report `DELETING`, and the one after completes the delete and
answers `ResourceNotFoundException`. Unseeded, `DeleteTable` removes the table at once, as before. The
table publishes no failure state ([#1196](https://github.com/scttfrdmn/substrate/issues/1196)).

Separately, `createDatabase` stores
`TableCount: 0` (`:110`) and nothing increments it, so after creating three
tables `DescribeDatabase` still reports zero against a member the reference
defines as "The total number of tables found within a Timestream database".
`DeleteDatabase` compounds this by not requiring the database to be empty
(`:136`): the page states "All tables in the database must be deleted first, or a
ValidationException error will be thrown", and substrate deletes the database
record while leaving every table in the store, still reachable by
`DescribeTable`, under a database that no longer exists. `ValidationException` at
400 is published on that page, so the refusal has a site. Neither is a status progression, and both
remain open.

## WriteRecords accepts any batch and rejects nothing

`Records` must hold "Minimum number of 1 item. Maximum number of 100 items". A batch outside
that is `ValidationException`/400 ([#1197](https://github.com/scttfrdmn/substrate/issues/1197)), not `RejectedRecordsException`, which the page
reserves for records rejected on their content. The response reports `RecordsIngested` with `Total` and
`MemoryStore` both set to the record count and `MagneticStore` fixed at zero.
`CommonAttributes` is merged into every stored record, as the page defines it ("will
be merged with the measure and dimension attributes in the records object"): the
common dimensions come first, and any other attribute a record sets wins over the
common one ([#1209](https://github.com/scttfrdmn/substrate/issues/1209)). Nothing in substrate emits `RejectedRecordsException` at 400 or
its `RejectedRecords` list of per-record `Reason` and `ExistingVersion`, so the
duplicate-record, out-of-retention and schema-mismatch surface a consumer's
partial-failure handler exists for is unreachable, by construction and without a
seed
([#1197](https://github.com/scttfrdmn/substrate/issues/1197)).

## What an unseeded Query answers

Substrate does not evaluate Timestream SQL. A query's result is a seeded value, per
the scope rule in `CLAUDE.md`, and **seeding is the mechanism for any query whose
columns, column order or row count matter**. Before
[#1209](https://github.com/scttfrdmn/substrate/issues/1209) an unseeded query answered a plausible wrong result: every column
`VARCHAR`, alphabetized, every record a row, any `WHERE`, `GROUP BY` or `LIMIT`
ignored. A consumer who forgot to seed got rows to assert against.

Now an unseeded query is answered only when substrate can answer it correctly:
`SELECT * FROM db.table` (quotes and a trailing `;` allowed), with nothing after the
table. Every other unseeded query — a column list, an aggregate, a `WHERE`, an
`ORDER BY`, a `LIMIT`, a table without its database — is `ValidationException` at 400,
which `API_query_Query` publishes, with a message naming the seed endpoint. A
`SELECT *` of a table that does not exist is the same code. Of the issue's two
options this takes the refusal: `QueryStatus` publishes only `ProgressPercentage`,
`CumulativeBytesScanned` and `CumulativeBytesMetered`, so no member could carry
"this was reconstructed", and the `SELECT` list is not parsed at all.

`SELECT *` answers the table in the logical shape the developer guide's *Writes*
page draws:

| Column | `ScalarType` | `ScalarValue` |
|---|---|---|
| `time` | `TIMESTAMP` | The record's `Time` in its `TimeUnit` (`MILLISECONDS` when it names none), rendered `YYYY-MM-DD hh:mm:ss.sssssssss` in UTC, the form the *Supported data types* page publishes |
| Each dimension, in the order first written | `VARCHAR` | The dimension's value |
| `measure_name` | `VARCHAR` | The record's measure name |
| `measure_value::<type>`, one per single-measure type | The record's `MeasureValueType` (`DOUBLE` when it names none, as `API_Record` publishes) | The value as written |
| Each name in a multi-measure record's `MeasureValues` | That value's `Type` | The value as written; a `TIMESTAMP` value is rendered like `time` |

A record with no value in a column answers `{"NullValue": true}`, as `API_query_Datum`
publishes. `VARCHAR`, `DOUBLE` and `BIGINT` values are answered as the record wrote
them, since `WriteRecords` takes every value as a string and substrate does not
re-render a number it was handed. `BOOLEAN` is the lowercase literal.

`QueryStatus` is answered on every `Query`. `ProgressPercentage` is 100, because
substrate's `Query` is synchronous. `CumulativeBytesScanned` and
`CumulativeBytesMetered` are both the size in bytes of the `Rows` member as answered.
Neither is Timestream's own accounting, which meters a 10 MB minimum per query. A
seed may supply its own `QueryStatus`, which is answered verbatim.

A worked seed, for a query whose shape matters:

```
POST /v1/timestream-query/results
{"queryString": "SELECT count(*) AS n FROM qdb.metrics",
 "result": {"ColumnInfo": [{"Name": "n", "Type": {"ScalarType": "BIGINT"}}],
            "Rows": [{"Data": [{"ScalarValue": "3"}]}],
            "QueryStatus": {"ProgressPercentage": 100,
                            "CumulativeBytesScanned": 10485760,
                            "CumulativeBytesMetered": 10485760}}}
```

`Query` with that exact `QueryString` answers those columns, rows and status as
written, whether or not the table exists. `TestTimestreamQuery_SelectAllAnswersTheTableInItsLogicalShape`,
`TestTimestreamQuery_AnUnseededQueryItCannotAnswerIsRefused` and
`TestTimestreamQuery_ASeededResultIsAnsweredAsSeeded` in
`emulator/timestream_query_test.go` pin all three paths on the raw bytes.

## Endpoint discovery is enforced

Timestream is two APIs, Write and Query, sharing one target prefix and one signing
name, and both require endpoint discovery. A client sends `DescribeEndpoints` to a
regional host, `ingest.timestream.{region}.amazonaws.com` for Write or
`query.timestream.{region}.amazonaws.com` for Query, and calls the address that
returns. The developer guide's usage notes say "The DescribeEndpoints action is the
only action that Timestream Live Analytics regional endpoints recognize", and each
operation page publishes `InvalidEndpointException` at 400. Substrate classifies a
request by its `Host`:

| Host | Serves | Refuses, with `InvalidEndpointException`/400 |
|---|---|---|
| `ingest.timestream.{region}` or `query.timestream.{region}` (regional) | `DescribeEndpoints` | Everything else |
| `ingest-cellN.timestream.{region}` (Write cell) | The Write operations and `DescribeEndpoints` | `Query`, `CancelQuery` |
| `query-cellN.timestream.{region}` (Query cell) | `Query`, `CancelQuery` and `DescribeEndpoints` | The Write operations |
| Any other host (localhost, an `--endpoint-url`) | Everything | Nothing, since the host names neither API |

The refusal's message opens with the sentence its API publishes: "The requested
endpoint was not valid." for Write, "The requested endpoint is invalid." for Query.
`DescribeEndpoints` answers `ingest-cell1.timestream.{region}.amazonaws.com` or
`query-cell1.timestream.{region}.amazonaws.com` for the API its host names, and the
parser routes both cell hosts to the plugin, so a discovering client's next call
lands here. The guide says the returned addresses are cell-specific and publishes no
form for them; `-cellN` is substrate's reading of the form real responses carry. On a
host naming neither API, `DescribeEndpoints` answers that host itself, since a caller
that reached substrate through an `--endpoint-url` has no other address to use.
`CachePeriodInMinutes` is 1; the member is required, but its value is not published.
`TestTimestreamEndpoints_DescribeEndpointsAnswersTheCellOfTheAPIItWasCalledOn` and
`TestTimestreamEndpoints_ARequestAtAnEndpointThatDoesNotServeItIsRefused` assert both
directions ([#1209](https://github.com/scttfrdmn/substrate/issues/1209)).

## What a refusal reports

| Condition | Code | Status |
|---|---|---|
| A request body that is not valid JSON | `ValidationException` | 400 |
| `DatabaseName` absent on any database or table operation | `ValidationException` | 400 |
| `TableName` absent on `CreateTable`, `DescribeTable`, `DeleteTable` or `WriteRecords` | `ValidationException` | 400 |
| Database name already recorded | `ConflictException` | 409 |
| Table name already recorded in the database | `ConflictException` | 409 |
| Named database not recorded | `ResourceNotFoundException` | 404 |
| Named table not recorded | `ResourceNotFoundException` | 404 |
| Any of the other twenty-one operations | `UnknownOperationException` | 404 |
| An operation at an endpoint that does not serve it | `InvalidEndpointException` | 400 |
| An unseeded `Query` other than `SELECT * FROM db.table`, or of a table that does not exist | `ValidationException` | 400 |

`ValidationException` at 400 is published on every routed operation's page and is
the right code for both a malformed body and a missing required member, so
substrate reuses it for both and a consumer cannot distinguish them.
`UnknownOperationException` at 404 is published on both Common Errors pages at
exactly that status. `ConflictException` and `ResourceNotFoundException` are
published at 400, not at the 409 and 404 substrate returns. Published codes with
no site in substrate at all are `RejectedRecordsException` (400),
`AccessDeniedException` (400), `ThrottlingException` (400),
`ServiceQuotaExceededException` (400), `QueryExecutionException` (400) and `InternalServerException` (500) — so
throttling and quota retries, which are the paths a
Timestream client's retry policy is written for, cannot be exercised here and
have no seed.

## Cost

| Operation | Cost per call (USD) |
|---|---|
| `WriteRecords` | 0.0000005 |
| `Query` | 0.000001 |

Both keys are `timestream/{Operation}` and both name routed operations, so both
attribute on every matching call. The cost is per call and does not scale with the
record count or the bytes scanned, which is how the service prices both.
