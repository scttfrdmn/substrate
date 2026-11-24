# CloudTrail

**Endpoint:** `cloudtrail.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: CloudTrail_20131101.{Op}`)

**Routing:** two target spellings are accepted — the short `CloudTrail_20131101.{Op}` and the fully
qualified `com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.{Op}`, both of which SDKs have been
observed to send. Neither is reached through a service alias, because the long form's first label is
`com`; the router matches the prefix itself.

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateTrail | `Name` required; `S3BucketName` is `Required: Yes` and unchecked; the trail is [not logging until `StartLogging`](#a-trail-logs-only-once-started-and-gettrailstatus-reports-it) |
| GetTrail | |
| GetTrailStatus | Reports the trail's [`IsLogging` and its logging times](#a-trail-logs-only-once-started-and-gettrailstatus-reports-it) |
| UpdateTrail | Merges the supplied members into the stored trail |
| DeleteTrail | |
| DescribeTrails | `trailNameList` filters; a name that matches nothing is skipped rather than refused. `IncludeShadowTrails` is parsed and not read |
| StartLogging | Sets the flag `GetTrailStatus` reports, and records `StartLoggingTime` on a transition |
| StopLogging | Clears the flag `GetTrailStatus` reports, and records `StopLoggingTime` on a transition |

## A trail logs only once started, and GetTrailStatus reports it

A new trail is not logging. `API_StartLogging` "starts the recording of AWS API calls and log file
delivery for a trail", and `CreateTrail`'s response publishes no `IsLogging`, so `CreateTrail`
stores the trail not logging, and `GetTrailStatus` reports `IsLogging: false` until `StartLogging`.
Until [#1157](https://github.com/scttfrdmn/substrate/issues/1157) a trail was stored logging, and
`GetTrailStatus` answered `IsLogging: true` hardcoded, so a `StopLogging` changed state that no
response could show.

`GetTrailStatus` answers the stored flag, plus the logging times the trail has:

| Member | Answered |
|--------|----------|
| `IsLogging` | always: the stored flag |
| `StartLoggingTime` | once the trail has started logging: when it last went from not logging to logging, as epoch seconds |
| `StopLoggingTime` | once the trail has stopped logging: when it last went from logging to not logging, as epoch seconds |

A `StartLogging` on a trail already logging, or a `StopLogging` on one already stopped, succeeds and
moves neither time. That is substrate's reading: the page describes `StartLoggingTime` as "the most
recent date and time when CloudTrail started recording", and a repeated start does not start it again.

The page's delivery members (`LatestDeliveryTime`, `LatestDigestDeliveryTime`,
`LatestNotificationTime`, `LatestCloudWatchLogsDeliveryTime` and their errors) are not answered.
Substrate delivers no log, digest, notification or CloudWatch Logs record, so none has anything true
to report. Until #1157, `LatestDeliveryTime` answered the request's own time, claiming a delivery
that never happened. The six members the page marks "no longer in use" (`TimeLoggingStarted`,
`TimeLoggingStopped`, `LatestDeliveryAttemptTime`, `LatestDeliveryAttemptSucceeded`,
`LatestNotificationAttemptTime`, `LatestNotificationAttemptSucceeded`) are omitted rather than
answered as empty strings. `TestCloudTrailLogging_GetTrailStatusReportsTheWalk`
(`emulator/cloudtrail_logging_test.go`) walks create, status, `StartLogging`, status, `StopLogging`
and status over the wire.

An `AWS::CloudTrail::Trail` CloudFormation resource is still a stub that records no CloudTrail trail,
so its `IsLogging` property neither starts nor stops anything.

## The account, Region and creation time a record carries reach no response

`CloudTrailTrail` declares `AccountID`, `Region` and `CreatedAt` under wire-visible `json` tags,
because the record is what `MemoryStateManager` snapshots and a replay reads back. Until
[#756](https://github.com/scttfrdmn/substrate/issues/756), four sites answered the record whole:

- `CreateTrail` and `UpdateTrail` returned it as the body.
- `GetTrail` returned it under `Trail`, and `DescribeTrails` in `trailList`.

That put all three members on the wire, plus `IsLogging`, which only `GetTrailStatus` publishes. The
two writes also answered `HomeRegion` and `HasCustomEventSelectors`, which `Trail` publishes and
`CreateTrailResponse`/`UpdateTrailResponse` do not.

The reads now answer `cloudtrailTrailOut` and the writes `cloudtrailTrailWriteOut`
(`emulator/cloudtrail_wire.go`), and the stored record is unchanged.
`TestCloudTrailWire_TrailResponsesCarryNoBookkeepingMember` drives all eight routed operations.

With the leak gone, `GetTrailStatus` is the one response that reports whether a trail is logging, as
its page publishes; since #1157 it reports the stored flag.

## A trail's ARN is always in the aws partition

`CreateTrail` builds the trail ARN with a literal `aws` partition, so a Region in `aws-us-gov` or
`aws-cn` reports an ARN AWS would not issue. Substrate's Region handling is otherwise
partition-agnostic.

## What a refusal reports

| Condition | Code | Status |
|-----------|------|--------|
| a body that will not parse | `ValidationError` | 400 |
| `Name` absent or empty | `InvalidTrailNameException` | 400 |
| a trail name already in use | `TrailAlreadyExistsException` | 400 |
| a trail that does not exist | `TrailNotFoundException` | 400 |

Every status is the one CloudTrail publishes: each page lists every error at HTTP 400, and no
CloudTrail page publishes a 404 anywhere. `TrailNotFoundException` answered 404 until
[#1156](https://github.com/scttfrdmn/substrate/issues/1156). Six operations raise it: `GetTrail`,
`GetTrailStatus`, `UpdateTrail`, `DeleteTrail`, `StartLogging` and `StopLogging`.

`DescribeTrails` publishes no not-found refusal, so a `trailNameList` entry that names nothing is
left out of the list, and the call answers 200. A store fault while it reads a listed trail is an
error, not a shorter list.

`ValidationError`, which an unparseable body answers, is substrate's shared invalid-body code. No
CloudTrail page publishes it; its status, 400, matches every page.

`InvalidTrailNameException` is published, and its gloss covers a name that violates the published
pattern — which Substrate does not check, so the code fires only for an absent name and an unparseable
body. `S3BucketDoesNotExistException`, `InsufficientS3BucketPolicyException`,
`TrailNotProvidedException` and the rest of the page's twenty-odd errors have no site: Substrate
validates nothing about the destination bucket.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::CloudTrail::Trail | TrailName | `Ref` returns the resource name where Substrate's physical ID is the ARN, so the name is recorded in the resource's metadata and read back from there (#827). A stub: the trail is invisible to `GetTrail` and `DescribeTrails` |

## Cost

`CreateTrail` is attributed $0.000002 per call. Real CloudTrail charges nothing for the first copy of
management events and $2.00 per 100,000 events for additional copies; Substrate counts no events.
