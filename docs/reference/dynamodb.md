# DynamoDB

**Endpoint:** `dynamodb.{region}.amazonaws.com`
**Protocol:** JSON (`application/x-amz-json-1.0`, `X-Amz-Target: DynamoDB_20120810.{Op}`)

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateTable | Supports GSI, LSI, billing mode; mints `TableId`, a UUID derived from the request's IDs; stores `SSESpecification` and `WarmThroughput` (table and GSI) — see [Warm throughput and encryption](#warm-throughput-and-encryption); [reports only `TableDescription`'s own members](#a-table-description-reports-only-its-own-members) |
| DescribeTable | [reports only `TableDescription`'s own members](#a-table-description-reports-only-its-own-members) |
| DeleteTable | [reports only `TableDescription`'s own members](#a-table-description-reports-only-its-own-members) |
| ListTables | |
| UpdateTable | Applies `BillingMode`, `ProvisionedThroughput`, `SSESpecification` and the table's `WarmThroughput` ([Warm throughput and encryption](#warm-throughput-and-encryption)); every other member — `GlobalSecondaryIndexUpdates`, the stream specification, `TableClass`, `DeletionProtectionEnabled` — is accepted and not applied; [reports only `TableDescription`'s own members](#a-table-description-reports-only-its-own-members) |
| PutItem | Supports ConditionExpression; a key whose type differs from the table's `AttributeDefinitions` is `ValidationException`; conditional writes to one item [serialize](#conditional-writes-serialize-per-item) |
| GetItem | Supports ProjectionExpression |
| UpdateItem | Supports UpdateExpression (SET/REMOVE/ADD/DELETE) with `if_not_exists`, `list_append`, `+`/`-`, document paths (`a.b[0]`) and DynamoDB's `ValidationException` messages; every action reads the item as it was before the update. The ConditionExpression is evaluated against the stored item — none when the update creates it, so `attribute_not_exists(pk)` holds and `attribute_exists(pk)` does not. `ALL_OLD`/`UPDATED_OLD` return nothing for a created item; `UPDATED_*` return only the updated top-level attributes. Conditional writes to one item [serialize](#conditional-writes-serialize-per-item) |
| DeleteItem | Supports ConditionExpression |
| Query | Supports FilterExpression, GSI/LSI via IndexName; key conditions and sort order use [typed comparison](#comparisons-are-typed) — a Number sort key sorts numerically, a Binary one by unsigned bytes |
| Scan | Supports FilterExpression, GSI/LSI via IndexName |
| BatchGetItem | |
| BatchWriteItem | |
| TransactGetItems | |
| TransactWriteItems | Conditions are evaluated against the stored items (none for an absent item); every resulting item is computed before any is written, so an update expression that cannot be applied writes nothing; every touched item is locked for the transaction |
| UpdateTimeToLive | |
| DescribeTimeToLive | `ENABLED` with the attribute name once `UpdateTimeToLive` set one, `DISABLED` otherwise. No item ever expires — the sweep that deletes expired items is background service work, not an API observation |
| DescribeContinuousBackups | `ContinuousBackupsStatus` is always `ENABLED`, as the page states for every table. `PointInTimeRecoveryStatus` is `DISABLED` until `UpdateContinuousBackups` enables it; an enabled table reports `RecoveryPeriodInDays` (default 35) and a restorable window from the simulated clock: `LatestRestorableDateTime` five minutes before now and `EarliestRestorableDateTime` the start of the recovery period, neither earlier than when recovery was enabled (that bound is substrate's reading). `TableName` may be the table's ARN. An absent table is `TableNotFoundException`/400. No restore is modelled |
| UpdateContinuousBackups | `PointInTimeRecoverySpecification.PointInTimeRecoveryEnabled` turns recovery on or off; `RecoveryPeriodInDays` outside 1–35 is `ValidationException`/400, and an omitted one is the default 35, not the previous value. Re-enabling an enabled table keeps its window. The record is updated under the table's lock, as `TagResource`, `UntagResource`, `UpdateTable` and `UpdateTimeToLive` now update it too. Answers the same description as `DescribeContinuousBackups` |
| TagResource | Merges into the table's tags by key. The table name comes from the ARN's `:table/` segment with any `/index/…` or `/stream/…` suffix dropped, but the ARN's account and Region are **not** read — the name is resolved in the caller's account and Region. An ARN with no `:table/` segment, and a name with no table behind it, are both `ResourceNotFoundException` at **400**, which is the status DynamoDB's JSON protocol uses throughout |
| UntagResource | Deletes each key in `TagKeys`; a key that is not there is a no-op. Resolves its ARN exactly as `TagResource` does |
| ListTagsOfResource | Reports `Tags` as the API's list of `{Key, Value}` sorted by key — see [A table description reports only its own members](#a-table-description-reports-only-its-own-members) for why this is the only place a table's tags are readable. `NextToken` is always `null`: every tag comes back in one page |
| ExecuteStatement | A deliberately small PartiQL subset: `SELECT * FROM "table" [WHERE …]`, `INSERT INTO "table" VALUE {…}` (the `VALUE` clause is a JSON item in `AttributeValue` shape), `UPDATE "table" SET … WHERE …` and `DELETE FROM "table" WHERE …`. `WHERE` and the `SET` values are translated to expression form — a `'single-quoted'` string (`''` escapes a quote), a number, `true`/`false` and each `?` become expression attribute values, a `"double-quoted"` name an expression attribute name, `!=` becomes `<>` — and evaluated by the same code as `Scan`'s `FilterExpression` and `UpdateItem`'s `UpdateExpression`, over every item in the table. `?` placeholders take `Parameters` in statement order (`SET`'s, then `WHERE`'s); too few is `ValidationException`. A `WHERE` that does not parse is refused rather than matching nothing. A statement that does not parse, or names a verb outside those four, is `ValidationException`/400; an unknown table is `ResourceNotFoundException`/400. No `NextToken` is ever issued |
| BatchExecuteStatement | Runs each `Statements` entry through the same evaluator as `ExecuteStatement`, in order, and reports one `Responses` entry per statement. A statement that fails contributes an `Error` entry in place and does not abort the batch; a `SELECT` contributes only its first item as `Item` |
| ListStreams | Reports the stream of the named `TableName`, or of every table in the caller's account and Region when no name is given; a table whose `CreateTable` did not set `StreamSpecification.StreamEnabled` is skipped, and `UpdateTable` cannot turn a stream on afterwards. `StreamLabel` is the trailing segment of the stream ARN |
| DescribeStream | Reports one shard, `shardId-00000000000000000000-00000001`, with `StreamStatus` `ENABLED`. The shard is synthesized from the ARN rather than looked up, so the description does not depend on the table existing |
| GetShardIterator | Encodes the table, account, Region and starting sequence number into the iterator itself, so an iterator is self-describing and survives a replay. `TRIM_HORIZON` starts at zero, `LATEST` after the newest record, and `AT_SEQUENCE_NUMBER` / `AFTER_SEQUENCE_NUMBER` at and past the given one; an unrecognized `ShardIteratorType` is treated as `TRIM_HORIZON` |
| GetRecords | Returns the records at or after the iterator's sequence number a page holds 1000 unless a smaller `Limit` asks for fewer — and a `NextShardIterator` advanced past the last one returned. An **absent** `ShardIterator` is `ValidationError`/400, the JSON protocol's common-list code rather than the `ValidationException` this plugin's control-plane operations answer, because no DynamoDB Streams page publishes a validation error at all ([#1062](https://github.com/scttfrdmn/substrate/issues/1062)). An iterator that is present but does not decode still answers `200` with an empty list. Substrate routes the four Streams operations through DynamoDB's own host and `DynamoDB_20120810` target prefix, not the `streams.dynamodb` endpoint |

## Comparisons are typed

Condition, filter and key-condition expressions compare values the way the `Condition` API
reference states. `=`, `<>` and `IN` are defined for every type: values of different types are
never equal (`{"S":"1"}` does not equal `{"N":"1"}`), sets compare as unordered collections, and
lists and maps compare deeply. `<`, `<=`, `>`, `>=` and `BETWEEN` hold only between two Strings,
two Numbers or two Binaries — Numbers numerically and exactly (38 digits survive), Strings and
Binaries by unsigned byte order — and any other pairing does not match.

A Number must be decimal — an optional sign, digits with an optional fraction, an optional
exponent — with at most 38 significant digits and a magnitude from `1E-130` to
`9.9999999999999999999999999999999999999E+125`, wherever it appears in a request or results from
arithmetic; otherwise the request is `ValidationException`/400 with DynamoDB's message. A Number
key is stored in canonical form — `1.0`, `1E0` and `1` are one key, as DynamoDB trims leading and
trailing zeros. Until #1454 every Binary
compared equal to every other, and every Binary key of a table addressed the same item; a Binary
key is now stored as `B:` followed by its base64, while String and Number keys keep their
existing state keys.

## Conditional writes serialize per item

Of N concurrent conditional writes to one item whose conditions are mutually exclusive, exactly
one succeeds and the rest get `ConditionalCheckFailedException`, as on DynamoDB. Substrate holds a
striped per-item lock across load, condition and write in PutItem, UpdateItem, DeleteItem,
BatchWriteItem, TransactWriteItems (every touched item, in one global order) and PartiQL, and a
per-table lock around the item list and stream records concurrent creates share (#1449). The
guarantee is process-local, like S3's conditional writes.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::DynamoDB::Table | TableName | GSI, LSI, TTL supported |

## A table name is unique per Region, not per account

`CreateTable` states it outright: "In an AWS account, table names must be unique within each
Region. That is, you can have two tables with same name if you create the tables in different
Regions." Until #943 substrate keyed a table as `table:{account}/{name}`, so one account could
hold one table name once across every Region and the second create answered
`ResourceInUseException`/400 — which is what a consumer deploying one stack to two Regions hit.
The key is now `table:{account}/{region}/{name}`; see
[A Lambda function and a DynamoDB table belong to one account in one Region](/reference/tagging#a-lambda-function-and-a-dynamodb-table-belong-to-one-account-in-one-region).

## A table description reports only its own members

`CreateTable`, `UpdateTable`, `DeleteTable` and `DescribeTable` project the stored table onto
`TableDescription` rather than handing back the record. Until #1013 they handed back the record,
and it carries two fields substrate needs that `TableDescription` does not publish: a
`CreateTable` with tags answered `"Tags":{"env":"test"}` inside its `TableDescription`, and a
`DescribeTable` on a table with TTL enabled answered `"TTLAttribute":"expiresAt"`. `Tags` was
additionally rendered as a JSON object, where AWS's `Tags` is everywhere a list of
`{Key, Value}` — so the invented member was also in a shape the API does not use anywhere.

Both values remain readable where AWS publishes them: a table's tags through
`ListTagsOfResource`, and its TTL attribute through `DescribeTimeToLive`. Only the member
`TableDescription` does not have is gone.

The projection is the point rather than the two names: a state record accumulates fields for
the emulator's own bookkeeping, and one marshalled straight onto the wire turns each of them
into a response member. This is the pattern #529 established for API Gateway v1, where the
opposite failure — the state struct's PascalCase members against a lowerCamel model — made an
AWS SDK parse a populated response to an empty result. Twenty-eight of the twenty-nine record
types the Resource Groups Tagging API scans already had such a projection; `DynamoDBTable` was
the twenty-ninth, which is why `AccountID` and `Region` were never at risk of appearing but
`Tags` was.

Members substrate does not model are absent from the projection rather than present and empty,
which is the honest-empty reading #827 established, applied to a shape: nothing reports
`Replicas` or the eleven others AWS publishes as optional. `TableId` is reported for every table
created since #1608; a table recorded before it has none, and omits the member.

## Warm throughput and encryption

Every table description reports `WarmThroughput`, on the table and on each global secondary index,
with `Status` `ACTIVE` ([#1608](https://github.com/scttfrdmn/substrate/issues/1608)). Terraform's
provider waits for that status after every create. The value is the larger of three things:

- the developer guide's on-demand default of 12,000 read and 4,000 write units per second;
- the value `CreateTable` or `UpdateTable` requested;
- the provisioned capacity, since the guide says warm throughput rises with it.

The guide publishes no separate default for a provisioned table, so using the on-demand default
there is substrate's reading. A lower request leaves the value unchanged, because "the values can't
be decreased". A request below 1 is `ValidationException`/400 with substrate's own message.

`SSESpecification` with `Enabled` true is KMS encryption. The table then reports `SSEDescription`
with `Status` `ENABLED`, `SSEType` `KMS` and `KMSMasterKeyArn`. With `Enabled` false or absent, the
table uses an AWS owned key and reports no `SSEDescription`. A key ID is reported as its key ARN in
the caller's account and Region, and a key ARN verbatim. An alias is reported as its alias ARN,
not resolved to its key as AWS would. Substrate mints no AWS managed key, so `Enabled` true with no
`KMSMasterKeyId` reports no `KMSMasterKeyArn`. An unknown `SSEType` is `ValidationException`/400.
`UpdateTable` with an `SSESpecification` replaces the encryption, and one without leaves it alone.

## A stream iterator carries its account and Region only inside the token

`DynamoDBStreamCursor` holds the table, account, Region and position a shard iterator reads from, and it does reach a body: `GetShardIterator` and `GetRecords` answer it base64-encoded as the opaque `ShardIterator` and `NextShardIterator` strings. A real iterator is opaque too, so carrying the account and Region inside it is faithful, and it is what keeps an iterator from being redeemed against another account's or Region's table ([#943](https://github.com/scttfrdmn/substrate/issues/943)).
`TestDynamoDBStreamsWire_CursorReachesResponsesOnlyAsAToken` in `emulator/dynamodb_stream_cursor_wire_test.go` decodes each token first, requiring both members inside it, then walks each document for either member at any depth, so it fails the day the cursor is rendered as an object
([#756](https://github.com/scttfrdmn/substrate/issues/756)).

## Cost

DynamoDB write operations: $0.00000125 per WCU. Read operations: $0.00000025 per RCU.
