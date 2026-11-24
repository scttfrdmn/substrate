# QuickSight

**Endpoint:** `quicksight.{region}.amazonaws.com`

**Protocol:** REST-JSON, path-routed. API version 2018-04-01.

Four operations over two resources: data sources and data sets, plus a data set's ingestion.

No data is ever read from a source and no ingestion runs. A data source is recorded
`CREATION_SUCCESSFUL` and a data set's ingestion `COMPLETED`; seeds make the creation and the
ingestion progress, observation by observation, and settle to a failure where a test needs one
([#1155](https://github.com/scttfrdmn/substrate/issues/1155),
[#1168](https://github.com/scttfrdmn/substrate/issues/1168)).

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateDataSource | `POST /accounts/{AwsAccountId}/data-sources`. Answers HTTP 201 with the four published members; `CreationStatus` is `CREATION_SUCCESSFUL`, or `CREATION_IN_PROGRESS` while [a seed's countdown runs](#a-data-source-s-creation-and-an-ingestion-are-seeded-progressions). `Name` and `Type` are `Required: Yes` and unchecked, so a data source can have neither |
| DescribeDataSource | `GET /accounts/{AwsAccountId}/data-sources/{DataSourceId}`. Answers five of `API_DataSource`'s members, plus `ErrorInfo` for a seeded `CREATION_FAILED`, and none it does not publish. Each describe is one observation of [the seeded creation](#a-data-source-s-creation-and-an-ingestion-are-seeded-progressions). [`Status` is the status line, not a body member](#status-is-bound-to-the-status-line-not-the-body) |
| CreateDataSet | `POST /accounts/{AwsAccountId}/data-sets`. Answers HTTP 201 with `DataSetId`, `Arn`, `IngestionId` and `RequestId`; `PhysicalTableMap`, `ImportMode` and the rest of the definition are not read |
| DescribeIngestion | `GET /accounts/{AwsAccountId}/data-sets/{DataSetId}/ingestions/{IngestionId}`. Answers the data set's one ingestion, the one `CreateDataSet` minted, and refuses any other ID; each describe is one observation of [the seeded ingestion](#a-data-source-s-creation-and-an-ingestion-are-seeded-progressions) |

Every other QuickSight operation is unrouted: the updates and deletes
(`UpdateDataSource`, `DeleteDataSource`, `UpdateDataSet`, `DeleteDataSet`), the lists
(`ListDataSources`, `ListDataSets`, `ListIngestions`), `CreateIngestion` and `CancelIngestion`, and
the whole analysis, dashboard, template, namespace, user and group surface. QuickSight resources are
not scanned by the Resource Groups Tagging API either, so `Tags` on a create is dropped.

## The account in the path is the caller's, and the Region is in the key

[#1167](https://github.com/scttfrdmn/substrate/issues/1167). `AwsAccountId` is `Required: Yes` in every
QuickSight URI, "Fixed length of 12", Pattern `^[0-9]{12}$`. Every handler used to discard it and use
the caller's account, so a call naming account B wrote account A's data sources and answered 201. Now:

- A path account that does not match the pattern is `InvalidParameterValueException`/400.
- A well-formed path account that is not the caller's is `AccessDeniedException` at **401**, the status
  every QuickSight page publishes for that code (not the 403 most services use). Substrate models no
  cross-account QuickSight access, so every mismatch is refused. The message is the page's gloss.

The state keys carry the Region: `datasource:{account}/{region}/{DataSourceId}` and
`dataset:{account}/{region}/{DataSetId}`. The pages gloss both IDs "unique per AWS Region for each AWS
account", so the same ID in two Regions is two resources, and one Region's data source is not visible
from another.

**Compatibility.** State written before this change sits at the old keys, which omit the Region, and is
unreachable after it. A recording made before it replays to `ResourceNotFoundException` at the first
describe of something it created. Nothing migrates the old keys, since a key without a Region cannot say
which Region it belonged to.

## A data source's creation and an ingestion are seeded progressions

Both follow [the rules every progression shares](/reference/concepts/seeds-and-replay#how-a-progression-is-seeded).

**An ingestion.** `DescribeIngestion` answers the data set's one ingestion, the initial ingestion
`CreateDataSet` minted, and refuses any other ID with `ResourceNotFoundException`/404, as
`API_DescribeIngestion` publishes. It used to load only the data set and fabricate the ingestion from
the path, so any ID at all was `COMPLETED` with exactly 1,000 rows
([#1168](https://github.com/scttfrdmn/substrate/issues/1168)).
- **Unseeded:** the ingestion reports `COMPLETED` and **no `RowInfo`**, since substrate ingests no
  rows to count.
- **`CreatedTime`** is when the ingestion started, recorded at `CreateDataSet`. A data set recorded
  before #1168 reports the time of the describe, as it always did.

```
POST   /v1/quicksight/ingestion-status   {"ingestionId": "…", "pendingObservations": 2,
                                          "transientStatus": "QUEUED", "status": "FAILED",
                                          "errorType": "S3_MANIFEST_ERROR", "errorMessage": "…",
                                          "rowsIngested": 0, "rowsDropped": 0, "totalRowsInDataset": 0}
DELETE /v1/quicksight/ingestion-status   (all seeds; ?ingestionId=… for one)
```

- **Counted:** `pendingObservations` describes report `transientStatus` (`INITIALIZED`, `QUEUED` or
  `RUNNING`, default `RUNNING`), then the ingestion settles to `status`, `COMPLETED` when empty.
- **On settling,** `errorType`/`errorMessage` are reported as `ErrorInfo`, only with `FAILED` or
  `CANCELLED`. The row counts are reported as `RowInfo`, and only when seeded.
- **Validated:** every value is checked against its published enumeration (`IngestionStatus`,
  `ErrorInfo.Type`), and a negative row count is refused with 400.

**A data source's creation.**

```
POST   /v1/quicksight/data-source-status   {"dataSourceId": "…", "pendingObservations": 1,
                                            "status": "CREATION_FAILED",
                                            "errorType": "ACCESS_DENIED", "errorMessage": "…"}
DELETE /v1/quicksight/data-source-status   (all seeds; ?dataSourceId=… for one)
```

- **Counted:** `pendingObservations` observations report `CREATION_IN_PROGRESS`, then the data source
  settles to `CREATION_SUCCESSFUL` (the recorded value, and the default) or `CREATION_FAILED`, with
  `errorType`/`errorMessage` as `ErrorInfo` (`DataSourceErrorInfo`).
- **The create peeks:** `CreateDataSource`'s own `CreationStatus` reports what the first describe
  will, without spending its observation. `API_CreateDataSource` does not say which value a create
  returns.
- **Validated:** the update and deletion values (`UPDATE_*`, `DELETED`) are published but refused as
  a seeded status, because substrate routes no `UpdateDataSource` or `DeleteDataSource` for them to
  end.

## Status is bound to the status line, not the body

Every QuickSight Response Syntax opens with `HTTP/1.1 {Status}` rather than a literal code, and each
page lists `Status`, *"The HTTP status of the request"*, under the HTTP response rather than among the
members *"returned in JSON format"*: an SDK populates the field from the HTTP status it already received.
`DescribeDataSource` and `DescribeIngestion` used to emit `Status` as a JSON body member as well, which
was invisible through an SDK and visible as an unpublished extra member to anything reading the raw
body — a recorded event log, a fixture diff. Neither does now
([#1179](https://github.com/scttfrdmn/substrate/issues/1179)), and the creates never did. A new handler
answers its status through the response's status code, never through a `Status` member.

## What a refusal reports

| Condition | Code | Status |
|-----------|------|--------|
| a body that will not parse | `InvalidParameterValueException` | 400 |
| `DataSourceId`/`DataSetId` absent | `InvalidParameterValueException` | 400 |
| a data source, data set or ingestion that does not exist, including an ingestion ID that is not the data set's | `ResourceNotFoundException` | 404 |

`ResourceNotFoundException`/404 is what the pages publish, and so is `InvalidParameterValueException`/400,
which `API_CreateDataSource` and `API_CreateDataSet` both list. The two creates used to answer the
suffixless `InvalidParameterValue`, which no QuickSight page publishes, so an SDK raised it as a
generic error rather than the modeled exception. They also reported an unparseable body with the
message *"DataSourceId is required"*. The two conditions now share the published code and carry
their own messages ([#1169](https://github.com/scttfrdmn/substrate/issues/1169)).

A store fault while reading a data source is an error, not `ResourceNotFoundException`.
`DescribeDataSource` used to answer not-found for it, which told the caller to recreate a data source
that exists ([#1400](https://github.com/scttfrdmn/substrate/issues/1400)).

`AccessDeniedException`/**401**, `ConflictException`/409, `LimitExceededException`/409,
`ResourceExistsException`/409 and `ThrottlingException`/429 are published and have no site, so
creating the same data source twice succeeds. QuickSight publishes no `ValidationException` anywhere.

## The account and Region a record carries reach no response

`QuickSightDataSource` and `QuickSightDataSet` declare `AccountID` and `Region` under wire-visible
`json` tags, because the record is what `MemoryStateManager` snapshots and a replay reads back. Until
[#756](https://github.com/scttfrdmn/substrate/issues/756), `DescribeDataSource` answered the data
source whole, so both members reached the wire, and `API_DataSource` publishes neither. It now answers
`quicksightDataSourceOut` (`emulator/quicksight_wire.go`), and the stored record is unchanged.

The data set has never been rendered. `CreateDataSet` answers a map of published members, and
`DescribeIngestion` reads the record only to confirm the dataset exists. All four routed operations
are driven by `TestQuickSightWire_DataSourceResponsesCarryNoBookkeepingMember` and its data-set
sibling in `emulator/quicksight_wire_test.go`.

## CloudFormation resource types

None. AWS publishes `AWS::QuickSight::DataSource`, `AWS::QuickSight::DataSet` and the analysis,
dashboard and template types; Substrate deploys none of them.

## Cost

`CreateDataSource` and `CreateDataSet` are each attributed $0.000025 per call. Real QuickSight bills
per user per month, and SPICE capacity by the gigabyte; neither has a per-call analogue, so these are
flat proxies for authoring activity.
