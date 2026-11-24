# Kinesis Data Firehose

**Endpoint:** `firehose.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: Firehose_20150804.{Op}`)

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateDeliveryStream | |
| DescribeDeliveryStream | |
| DeleteDeliveryStream | |
| ListDeliveryStreams | |
| PutRecord | |
| PutRecordBatch | |

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::KinesisFirehose::DeliveryStream | DeliveryStreamName | |

## The account and Region a delivery stream carries reach no response

`FirehoseDeliveryStream` declares its account and Region under wire-visible `json` tags, because the record is what
`MemoryStateManager` snapshots and a replay reads back. Neither reaches a body: `DescribeDeliveryStream` answers `firehoseDeliveryStreamToWire`, the five members of `API_DeliveryStreamDescription` the record models, and the rest answer members built one by one.
`CreateTimestamp` is epoch seconds, a JSON number, as awsJson1_1 publishes a Timestamp ([#1305](https://github.com/scttfrdmn/substrate/issues/1305)), and a stream's tags are not in the description, which publishes none.
`TestFirehoseWire_DeliveryStreamResponsesCarryNoBookkeepingMember` in `emulator/firehose_wire_test.go` drives all six routed operations and walks each decoded document for either member at any
depth ([#756](https://github.com/scttfrdmn/substrate/issues/756)). `Destinations`, `HasMoreDestinations` and `VersionId` are published as required and absent, because `CreateDeliveryStream` keeps no destination.

## Cost

Firehose data ingestion: $0.029 per GB.
