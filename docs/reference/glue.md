# Glue

**Endpoint:** `glue.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AWSGlue.{Op}`)

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateDatabase | |
| GetDatabase | Reports `Database` with `CreateTime` — the epoch-seconds number `API_Database` publishes — and `CatalogId`, the Data Catalog the database resides in, which is the caller's account |
| UpdateDatabase | Applies `DatabaseInput`'s `Description`, `LocationUri` and `Parameters` when each is non-empty, so a member sent empty leaves the stored one alone. The database is addressed by the request's `Name`, which is therefore not renameable |
| DeleteDatabase | |
| GetDatabases | The same database shape as `GetDatabase`, under `DatabaseList`; `MaxResults`, `NextToken` and `ResourceShareType` are unread, so every database comes back in one page |
| CreateTable | |
| GetTable | Reports `Table` with `CreateTime` and `CatalogId`, as `GetDatabase` does. `UpdateTime` is absent: substrate stores no update timestamp, so it has no value to report rather than a zero one |
| UpdateTable | Applies `TableInput`'s `Description`, `StorageDescriptor`, `PartitionKeys` and `Parameters` when each is non-empty. The table is addressed by `DatabaseName` and `TableInput.Name` together, so it is not renameable either, and `VersionId` is not read — substrate keeps one version of a table |
| DeleteTable | |
| GetTables | The same table shape as `GetTable`, under `TableList`; `Expression`, `MaxResults` and `NextToken` are unread |
| CreateConnection | Records the `ConnectionInput` and `Tags`, and mints `arn:aws:glue:{region}:{account}:connection/{name}`. A connection is keyed by name, so a second create with the same name replaces the first rather than being refused |
| GetConnection | Reports `Connection` with its `ConnectionProperties` verbatim and `CreationTime` as epoch seconds; `HidePassword` is not read, so a password-like property is reported as stored. No `CatalogId` is reported, because `API_Connection` publishes none, and `Status` is absent — it reports whether Glue could reach the data source, which substrate does not attempt |
| GetConnections | The same connection shape as `GetConnection`, under `ConnectionList`, in ascending connection name; `Filter`, `MaxResults` and `NextToken` are unread, so every connection comes back in one page |
| UpdateConnection | Applies `ConnectionInput`'s `Description`, `ConnectionType` and `ConnectionProperties` when each is non-empty |
| DeleteConnection | Keyed by `ConnectionName`; a name with no connection behind it is not refused |
| CreateCrawler | Records the crawler with `State` `READY` and mints `arn:aws:glue:{region}:{account}:crawler/{name}`; `Schedule`, `Classifiers`, `SchemaChangePolicy` and the rest of the request are not recorded |
| GetCrawler | Reports `Crawler` with `CreationTime`. `LastCrawl` and `CrawlElapsedTime` are absent, and stay absent after `StartCrawler`: substrate runs no crawl, so it has no crawl to describe |
| GetCrawlers | The same crawler shape as `GetCrawler`, under `Crawlers`, in ascending crawler name; `MaxResults` and `NextToken` are unread |
| StartCrawler | A deterministic no-op at `200`: the crawler stays `READY`, and **no** table, partition or schema is ever discovered, because crawling a data store is workload-internal rather than an API observation. A name with no crawler behind it is not refused |
| StopCrawler | A deterministic no-op at `200`, for the same reason — nothing is ever running to stop |
| UpdateCrawler | Applies `Description` and `Targets` when each is non-empty; `Role`, `DatabaseName` and `Schedule` are not applied |
| DeleteCrawler | A crawler name with nothing behind it is not refused |
| CreateJob | |
| GetJob | Reports `Job` with `CreatedOn` — a third published spelling of the one creation timestamp substrate stores. The sizing and execution members (`MaxCapacity`, `NumberOfWorkers`, `Timeout`, `DefaultArguments`) are absent rather than zero: nothing configured them |
| UpdateJob | Applies `JobUpdate`'s `Description` and `Command` and reports `JobName`; every other `JobUpdate` member, including `Role`, `MaxRetries` and `DefaultArguments`, is accepted and not applied |
| DeleteJob | |
| GetJobs | The same job shape as `GetJob`, under `Jobs`; `MaxResults` and `NextToken` are unread |
| StartJobRun | Returns a `jr_`-prefixed JobRunId — 32 hex characters after the prefix, derived from the request ID (#856). The prefix is the service's own convention, not a published pattern |
| GetJobRun | A run is `SUCCEEDED` from the moment `StartJobRun` creates it, with `StartedOn` and `CompletedOn` both set to that instant, so a wait loop passes on its first observation and never sees `RUNNING`. Substrate does not execute the job, so there is no progression to observe — the same position the EMR Serverless section takes. `ErrorMessage` and `ExecutionTime` are absent for the same reason |
| GetJobRuns | The same run shape as `GetJobRun`, under `JobRuns`; `MaxResults` and `NextToken` are unread |
| TagResource | Merges `TagsToAdd` into the tags on the resource the ARN names. The ARN is resolved to state, so it addresses the account and Region *in the ARN* rather than the caller's, and the five resource types it resolves are `database`, `table`, `connection`, `crawler` and `job` |
| UntagResource | Deletes each key in `TagsToRemove`; a key that is not there is a no-op |
| GetTags | Reports `Tags` as a JSON object, empty for a resource with none |

**What the three tagging operations refuse.** An ARN without the `arn:aws:glue:` prefix, with fewer
than three colon-separated fields, with no slash in its resource segment, or naming a resource type
outside the five above is `InvalidInputException`/400 — the code every one of the three pages
publishes, where earlier releases answered `InvalidParameterValueException`, a string Glue's
reference does not contain ([#1063](https://github.com/scttfrdmn/substrate/issues/1063)). An ARN
that *parses* and then names no resource is a different condition, and it is the one AWS answers
with `EntityNotFoundException`: substrate fails it as `InternalFailure`/500 instead, which #1063
recorded as a divergence rather than fixing, since the fix changes what `GetTags` answers for an
untagged resource.

## What a refusal reports

| Condition | Code | Status |
|-----------|------|--------|
| a database, table, connection, crawler, job or job run that does not exist | `EntityNotFoundException` | 400 |

All twelve of those sites answered 404 until
[#1098](https://github.com/scttfrdmn/substrate/issues/1098). Glue publishes
`EntityNotFoundException` at 400 on every page that lists it — `GetTable`'s Errors section gives the
gloss, *"A specified entity does not exist"* — and publishes no 404 anywhere, so a consumer branching on
the status rather than on the code saw a shape AWS never sends. The correction follows
[#910](https://github.com/scttfrdmn/substrate/issues/910), which made the same argument for the
statuses it moved; [#1063](https://github.com/scttfrdmn/substrate/issues/1063) had already corrected
these codes and left their statuses behind.

The messages are substrate's own — `"<Entity> <name> not found."`, naming which entity and which name —
because the published gloss names neither, and a caller reading a message rather than a code needs to
know which lookup failed.

**Responses are rendered from the published shape.** All six Glue records — database, table,
connection, crawler, job and job run — used to be answered to the caller as stored, so each of the
twelve operations that reports a resource carried substrate's own `AccountID` and `Region`, and
`ever_tagged` once the resource had been tagged. None of the six is a member of `API_Database`,
`API_Table`, `API_Connection`, `API_Crawler`, `API_Job` or `API_JobRun`. Each record is now
projected onto its published shape before it is answered
([#756](https://github.com/scttfrdmn/substrate/issues/756)), which is what settles three further
divergences. The first is the creation timestamp: substrate stores one field on every record, and
Glue publishes that timestamp under four different names — `CreateTime` on a database and a table,
`CreationTime` on a connection and a crawler, `CreatedOn` on a job — so on five records out of six
the stored name was a near-miss of a real member, and a caller reading `CreateTime` found nothing
while the value sat beside it. The second is its format: every one of those members is published as
an epoch-seconds number and was rendered as an RFC3339 string
([#1305](https://github.com/scttfrdmn/substrate/issues/1305)), and an unset optional one is now
omitted rather than reported `null`. The third is `Arn` and `Tags`, which substrate reported on
every record and no Glue shape publishes at all — tags are observable through `GetTags` alone, and a
consumer that needs an ARN builds it from the account, the Region and the name, which is what the
real API requires of it. The fields themselves stay on all six stored records, so the state a
recorded run replays from is unchanged and the Resource Groups Tagging API — which reads those
records rather than these responses — still reports a Glue resource's tags and whether it has ever
been tagged. The account is still observable where AWS publishes it, as `CatalogId` on a database
and a table.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::Glue::Database | DatabaseName | |
| AWS::Glue::Table | TableName | |
| AWS::Glue::Job | JobName | |

## Cost

Glue ETL jobs: $0.44 per DPU-hour. Crawlers: $0.44 per DPU-hour.
