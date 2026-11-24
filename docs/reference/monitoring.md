# CloudWatch

**Endpoint:** `monitoring.{region}.amazonaws.com`
**Protocol:** Smithy RPC v2 CBOR, `awsJson1_0` and AWS Query — see below

## Protocols

CloudWatch is the one service whose model declares three wire protocols at once. Its
service shape, `com.amazonaws.cloudwatch#GraniteServiceVersion20100801`, carries
`aws.protocols#awsQuery`, `aws.protocols#awsJson1_0`, `smithy.protocols#rpcv2Cbor` **and**
`aws.protocols#awsQueryCompatible`, and its clients pick differently: `aws-sdk-go-v2` sends
CBOR, the AWS CLI and boto3 send `awsJson1_0`, and a hand-rolled client sends a query form.
All three are served, so which one you use is your choice and not a constraint (#785).

| Client | Request | Response |
|--------|---------|----------|
| `aws-sdk-go-v2` | `POST /service/GraniteServiceVersion20100801/operation/{Op}`, `Content-Type: application/cbor`, `Smithy-Protocol: rpc-v2-cbor` | CBOR, `Smithy-Protocol: rpc-v2-cbor` |
| AWS CLI, boto3 | `POST /`, `X-Amz-Target: GraniteServiceVersion20100801.{Op}`, `Content-Type: application/x-amz-json-1.0` | JSON, `application/x-amz-json-1.0` |
| query client | `POST /` with `Action={Op}` in a form body | `<{Op}Response><{Op}Result>…`, `text/xml` |

The Query path is byte-for-byte what earlier releases served, with two deliberate
exceptions. `DescribeAlarmsForMetric` now answers `<DescribeAlarmsForMetricResponse>` /
`<DescribeAlarmsForMetricResult>`; it previously borrowed `DescribeAlarms`' element names,
which the query protocol does not permit. `EnableAlarmActions` and `DisableAlarmActions`
previously emitted a document whose opening tag was literally `<placeholder>` and whose
closing tag was the operation's — not well-formed XML at all. `MetricAlarms` and
`GetMetricData`'s `Messages` are also now always present, where an empty one used to be
omitted.

**An operation with a `smithy.api#Unit` output answers with no body and no
`Content-Type`.** Eight of the fourteen do: `PutMetricAlarm`, `DeleteAlarms`, `SetAlarmState`,
`EnableAlarmActions`, `DisableAlarmActions`, `PutMetricData`, `TagResource` and
`UntagResource`. On the CBOR path they
return HTTP 200 carrying only `Smithy-Protocol: rpc-v2-cbor` — the rule comes from the
`no_output` protocol test in `smithy-protocol-tests/model/rpcv2Cbor/empty-input-output.smithy`,
which lists `Content-Type` in its `forbidHeaders`, rather than from the protocol spec page.
An empty CBOR map is merely *tolerated* by the companion `NoOutputClientAllowsEmptyCbor`
test, and `GetMetricData` used to send exactly that one byte for any caller whose
`Content-Type` mentioned CBOR — a response no client could tell from "not implemented". On
the JSON path the same operations answer `{}`, because `awsJson1_0` has no equivalent rule
and botocore reads a zero-length JSON body as a parse failure. On the Query path they answer
a response wrapper with no result element.

**Absent and present-but-empty are different answers.** A member that was never set is
omitted, so a typed client reads it as nil and can tell it was never set: an alarm created
without OK actions omits `OKActions` rather than returning an empty list, and one created
without `TreatMissingData` omits it rather than reporting the default. A member substrate
models as empty is present: `DescribeAlarms` always returns `MetricAlarms`, a metric
published without dimensions returns an empty `Dimensions`, and a `GetMetricData` result
whose window holds no datapoint has empty `Timestamps` and `Values`.

## Stored datapoints

`PutMetricData` stores every datapoint it is given, and `GetMetricData` and
`GetMetricStatistics` compute their statistics from the stored points
([#1595](https://github.com/scttfrdmn/substrate/issues/1595)). Earlier releases kept only a
metric's name and discarded its values, on the reading that datapoints are workload output.
That reading was wrong for this operation. The caller supplies the values through an API
call, so reading them back is an API observation. What stays out of scope is a workload that
produces metrics on its own, such as an instance's `CPUUtilization`, because substrate runs no
workload. Two derived exceptions report values that are facts about API-observable state:
[EC2 vCPU usage](#ec2-vcpu-usage-in-aws-usage) and the
[`AWS/Scheduler` invocation metrics](/reference/scheduler#cloudwatch-metrics).

- **What a series is.** A metric's identity is its namespace, name and whole dimension set.
  Dimension order does not matter. Each point keeps its `Value`, its `Values`/`Counts` pair or
  its `StatisticValues` set, with its `Unit` and `StorageResolution`. A datum that omits
  `Timestamp` is stamped with the simulated clock.
- **How a window is cut.** `StartTime` is rounded down as CloudWatch rounds it: to the minute,
  or to 5 minutes or an hour for a start 15 or 63 days back. A `Period` under 60 seconds rounds
  the start to that period instead. Periods are laid end to end from there, and `EndTime` is
  exclusive. A regular-resolution point counts at the start of its minute, and a point
  published with `StorageResolution` 1 counts at its own second. A period with no point
  reports no datapoint.
- **Which statistics.** `SampleCount`, `Sum`, `Average`, `Minimum` and `Maximum` come from every
  point. A `Values` element counts its `Counts` entry, or 1 when `Counts` is omitted. A
  `StatisticValues` set contributes its own four members. A percentile `pNN` (`p0` to `p100`)
  is computed by **nearest rank**: the values are ordered, each weighted by its count, and the
  answer is the smallest value whose cumulative weight reaches rank `ceil(NN × n / 100)`. AWS
  does not publish its method, so this one is substrate's, chosen because it always answers a
  value that was published. A `StatisticValues` set joins a percentile only in the two cases
  `PutMetricData` names. A period holding a negative value has no percentile: "Percentile
  statistics are not available for metrics when any of the metric values are negative numbers."
  Other extended statistics (trimmed and winsorized means, `IQM`, `PR`, `TC`, `TS`) are not
  computed, and a query naming one gets no datapoint. `GetMetricStatistics` refuses an
  `ExtendedStatistics` name that is not a percentile from `p0.0` to `p100`, and omits a period
  for which none of the requested statistics could be computed. A statistic that is not a
  finite number, such as a `Sum` past the double range, is omitted rather than rendered.
- **Units.** A query naming a `Unit` reads only the points published with it. A datum that
  named none was published with `None`. A `GetMetricStatistics` datapoint reports the unit its
  points share, or `None` when they differ. AWS aggregates different units separately, and
  substrate does not.
- **Order and paging.** `GetMetricData` orders values by `ScanBy`, `TimestampDescending` when
  omitted. Paging counts datapoints across the returned results in query order. A page carries
  at most `MaxDatapoints` of them (default 100,800). A result with datapoints beyond the page
  reports `StatusCode` `PartialData`, and `NextToken` is an offset token for the next page.
  `GetMetricStatistics` answers its datapoints in ascending order.
- **Expressions.** `GetMetricData` evaluates the arithmetic core of metric math: numbers, the
  `Id`s of the request's other queries, `+ - * /`, unary minus and parentheses. Two series
  combine at the timestamps both carry. A point divided by zero, or whose result is not a
  finite number, is dropped. An `Expression` over the published 2,048 characters is refused
  with `InvalidParameterValue`. Anything else is not evaluated: functions such as
  `SUM(METRICS())`, `SEARCH`, Metrics Insights `SELECT`, a constant (`1/0` included), an
  unknown `Id`, a cycle, or nesting deeper than 64 levels. Such a result carries no values and a `Messages`
  entry with `Code` `Unsupported`. That code and its text are substrate's own.
- **Retention.** Each series keeps at most 10,080 points, a week at one-minute resolution, and
  drops its oldest when a write takes it past that. `cloudwatch.max_datapoints_per_series` in
  `substrate.yaml` (or `SUBSTRATE_CLOUDWATCH_MAX_DATAPOINTS_PER_SERIES`) sets another cap.
  CloudWatch's age-based roll-up to coarser periods is not modelled: a point reads back at full
  resolution for as long as it is kept.
- **Validation.** `PutMetricData` stores nothing from a request that fails any of these checks.
  It refuses a missing `Namespace` or `MetricName`. It refuses a value outside -2^360 to 2^360,
  or a `NaN` or infinity. It refuses more than 30 dimensions, a dimension outside its published
  lengths, or a dimension named twice. It refuses more than 150 `Values`, a `Counts` array of a
  different length, or `Counts` without `Values`. It refuses `Value` with `Values` or with
  `StatisticValues`, and a `StatisticValues` set missing a member. It refuses a `Timestamp` more
  than two weeks before the simulated clock or more than two hours after it. It refuses a `Unit`
  outside its valid values, a `StorageResolution` other than 1 or 60, and more than 1,000 metrics
  in one request. The codes are the operation's published `MissingParameter`,
  `InvalidParameterValue` and `InvalidParameterCombination`; the messages are substrate's own.
  `EntityMetricData` datums are stored like `MetricData` ones, and the entity is not.

## What `DescribeAlarms` renders

`DescribeAlarms` and `DescribeAlarmsForMetric` render every `PutMetricAlarm` member the alarm
was put with ([#1620](https://github.com/scttfrdmn/substrate/issues/1620)): `Dimensions`,
`ExtendedStatistic`, `Unit`, `DatapointsToAlarm`, `TreatMissingData`,
`EvaluateLowSampleCountPercentile`, `ThresholdMetricId` and the `Metrics` of a metric-math
alarm, beside the members earlier releases rendered. They also render the three timestamps
CloudWatch keeps. `AlarmConfigurationUpdatedTimestamp` is the last `PutMetricAlarm`.
`StateUpdatedTimestamp` is the last state write, and `StateTransitionedTimestamp` is the last
change of `StateValue`. A metric-math alarm omits `MetricName`, `Namespace` and `Period`, and an
anomaly-detection alarm omits `Threshold`. `ActionConfiguration`, `EvaluationCriteria`,
`EvaluationInterval`, `EvaluationWindow` and `WarmUpConfiguration` are accepted and not
rendered.

An update keeps the alarm's state and its tags and replaces the rest of the configuration, as
the reference says: "the update completely overwrites the previous configuration of the alarm."
`PutMetricAlarm` refuses what its page publishes as invalid. That covers a `Statistic`,
`ComparisonOperator`, `TreatMissingData`, `EvaluateLowSampleCountPercentile` or `Unit` outside
its valid values, and `Statistic` with `ExtendedStatistic`. It covers `Metrics` with any of
`Namespace`, `MetricName`, `Dimensions`, `Period`, `Unit`, `Statistic` or `ExtendedStatistic`. It
covers a `Period` other than 10, 20, 30 or a multiple of 60, and an evaluation window longer than
a week, or longer than a day for a period under an hour. It also covers more than five actions in
a list and more than 30 dimensions. The messages are substrate's own.

Storing the configuration is not evaluating it. An alarm's `StateValue` moves only by
`SetAlarmState`, and evaluating alarms against stored datapoints is
[#1637](https://github.com/scttfrdmn/substrate/issues/1637).

## Alarm tags

`TagResource`, `UntagResource` and `ListTagsForResource` take an alarm ARN,
`arn:aws:cloudwatch:{region}:{account}:alarm:{name}`
([#1618](https://github.com/scttfrdmn/substrate/issues/1618),
[#1607](https://github.com/scttfrdmn/substrate/issues/1607)). `PutMetricAlarm`'s `Tags` are set
when it creates an alarm. An update ignores them, as the reference says: "any tags you specify in
this parameter are ignored." The account and Region come from the ARN, never from the caller.

- An alarm holds at most 50 tags, counted over the merged set. A tag `Key` is 1 to 128
  characters and a `Value` at most 256. The pages publish no code for exceeding the quota, so
  the refusal is the common `InvalidParameterValue`, with substrate's own message.
- A well-formed dashboard, metric-stream or Contributor Insights rule ARN names a type AWS tags
  and substrate does not model. It answers `ResourceNotFoundException` (404). Any other ARN is
  `InvalidParameterValue`.
- `ListTagsForResource` always returns `Tags`, in key order, and empty for an alarm with none.
- The [tagging API](/reference/tagging) reports alarms, and its writes route through
  these operations.
  `DeleteAlarms` removes an alarm's tag history with it, so an alarm re-created under the name
  starts never-tagged.

## EC2 vCPU usage in `AWS/Usage`

`ResourceCount` in the `AWS/Usage` namespace, with dimensions `Service=EC2`,
`Type=Resource`, `Resource=vCPU` and a `Class`, reports the vCPUs of the account's running
instances in that quota class (#1498). It is what a quota preflight compares against the
quota's value: each EC2 vCPU quota in [Service Quotas](/reference/servicequotas) names this metric
as its `UsageMetric`. `GetMetricData`, `GetMetricStatistics` and `ListMetrics` all answer
it, derived at request time from EC2's stored instances. Nothing is stored on the
CloudWatch side, and a replay that rebuilds the same instances and clock reads back the
same values.

| Class | Counts |
|---|---|
| `Standard/OnDemand`, `Standard/Spot` | Types starting A, C, D, H, I, M, R, T or Z, except `inf` |
| `F/OnDemand`, `F/Spot` | F |
| `G/OnDemand`, `G/Spot` | G and VT |
| `Inf/OnDemand`, `Inf/Spot` | Inf |
| `P/OnDemand`, `P/Spot` | P |
| `X/OnDemand`, `X/Spot` | X |

- **Which instances count.** An instance counts while it reports `running`, and under
  `…/Spot` when its `InstanceLifecycle` is `spot`. Pending, stopping, stopped,
  shutting-down and terminated instances do not count, following the On-Demand quota page
  ("Instances that are in the pending, stopping, stopped, and hibernated states do not
  count"). What an instance reports is what `DescribeInstances` would report at that
  moment, so an instance an [instance-state seed](/reference/ec2) still has reporting `pending` does
  not count. The seed's countdown is peeked, never spent: reading usage does not move a
  poll loop along. An instance's vCPUs are its type's default vCPU count from the
  [instance-type catalog](/reference/ec2). A type outside the catalog counts nowhere, and so do
  Trn, DL, HPC and High Memory types, which have no `Class` value.
- **Open Spot requests count.** Spot quotas cover the vCPUs "your running Spot Instances
  are either using or will use pending the fulfillment of open Spot Instance requests",
  and the quota's usage metric is the one that "reflects quota usage". So an `open` Spot
  request with no instance adds its instance type's vCPUs to its `…/Spot` class. Substrate
  holds one after a `/v1/ec2/spot-request-outcomes` seed leaves it unfulfilled, or after a
  persistent request's replacement launch is refused. Once a request is fulfilled its
  instance counts instead, so nothing is counted twice.
- **Which classes have a metric.** A class has a metric once any instance or Spot request
  in the account and region has counted against it, and keeps it after that instance
  stops or terminates, so its usage then reads 0. A class nothing has launched into has no
  metric: `ListMetrics` omits it and a query for it gets no datapoint.
- **Which datapoint is reported.** Substrate keeps no usage history, so it knows the usage
  only as of the request, and only the period holding the simulated clock can carry it.
  The window's `StartTime` is rounded down the way CloudWatch rounds it (to the minute, or
  to 5 minutes or an hour for a start 15 or 63 days back), and periods are laid end to end
  from there. The current period is the one holding the clock. A query gets that period's
  datapoint when the window reaches it: the window opens no later than the clock, and its
  exclusive `EndTime` falls after the period's start. Any other window gets no datapoint.
  That includes a window wholly in the past, which would otherwise be stamped with usage
  it was never measured at. `EndTime` only has to reach the current period, not pass the
  clock, because a caller's `EndTime=now` is computed a moment before substrate reads its
  clock.
- **Which statistic.** The usage is treated as sampled every minute of the period at its
  current value. `Maximum`, `Minimum`, `Average`, `IQM` and percentile, trimmed-mean and
  winsorized-mean statistics are the value itself; `SampleCount` is the period's minute
  count (`Period`/60, at least 1); `Sum` is the value times that count. Any other
  statistic gets no datapoint. The unit is `None`, so a query naming another `Unit` gets
  none either.

**Errors name the modeled shape, not the query code.** A CBOR or JSON refusal sets `__type`
to the absolute shape ID — `com.amazonaws.cloudwatch#InvalidParameterValueException`, not
`InvalidParameterValue` — with the HTTP status from the shape's `@httpError`, and per the
protocol the `Code` body member must not be what a client keys on. A caller that sent
`X-Amzn-Query-Mode: true` (both reference clients do) additionally gets
`x-amzn-query-error: <QueryCode>;Sender|Receiver`, which is how a query-compatible client
recovers the code its older error handling branches on. A body substrate cannot decode is
refused with `SerializationException` and HTTP 400; that choice is substrate's, since
neither the protocol nor the CloudWatch model names a shape for it.

**Substrate's CBOR writer is definite-length and minimal; its reader is not.** The protocol
spec never mentions indefinite-length encoding, yet the normative test vectors use it almost
everywhere and the reference implementation emits it, so a reader must accept
indefinite-length maps, arrays and strings, non-minimal length arguments, a `double` arriving
as a float16, float32, float64 **or** an integer, tag 1 as either an integer or a float, and
`0xf7` as null — substrate's does, and skips an unrecognized member's value recursively. Its
*writer* chooses definite lengths with minimal arguments so that the same response always
produces the same bytes, which is what makes a recorded event replayable byte-for-byte. That
is a documented divergence from the reference writer's output and conformant either way.

## Supported operations

| Operation | Notes |
|-----------|-------|
| PutMetricData | Stores every datapoint — see [Stored datapoints](#stored-datapoints); validates the published limits and stores nothing from a refused request |
| ListMetrics | Filters on `Namespace`, `MetricName`, `Dimensions` and `RecentlyActive`; returns each metric's dimensions; pages 500 at a time; also lists the [`AWS/Scheduler` metrics](/reference/scheduler#cloudwatch-metrics) and the [EC2 vCPU usage metrics](#ec2-vcpu-usage-in-aws-usage) the account has used |
| GetMetricData | Computes each `MetricStat` query from the stored datapoints; `ReturnData=false` gets only `Id` and `StatusCode`; evaluates arithmetic expressions only; pages on `MaxDatapoints`/`NextToken`; orders by `ScanBy` |
| GetMetricStatistics | Validates required members, `Statistics` against `ExtendedStatistics`, `Period` and the 1,440-datapoint ceiling; computes `Statistics` and percentile `ExtendedStatistics` from the stored datapoints |
| PutMetricAlarm | Stores every member — see [What DescribeAlarms renders](#what-describealarms-renders); preserves an existing alarm's state and tags on re-put; `Tags` set on create |
| DescribeAlarms | Filters on `AlarmNames` and `StateValue`; paginates on `MaxRecords`/`NextToken`; refuses a `NextToken` substrate did not issue with `InvalidNextToken` — see [A pagination token substrate never issued](/reference/concepts/listing-order#a-pagination-token-substrate-never-issued-is-refused-not-answered-with-page-one) |
| DescribeAlarmsForMetric | Filters on `MetricName`, `Namespace`, `Dimensions` (as a set), `Statistic`, `ExtendedStatistic`, `Period` and `Unit`; does not paginate |
| DeleteAlarms | Removes the alarm's tag history with it |
| SetAlarmState | `ResourceNotFoundException` for an unknown alarm; moves the state timestamps |
| EnableAlarmActions | |
| DisableAlarmActions | |
| TagResource | Alarm ARNs; 50-tag quota over the merged set — see [Alarm tags](#alarm-tags) |
| UntagResource | Alarm ARNs; a key the alarm does not carry is not an error |
| ListTagsForResource | Alarm ARNs; `Tags` always present, in key order |

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::CloudWatch::Alarm | AlarmName | |

## Cost

CloudWatch metrics: $0.30 per metric per month. Alarms: $0.10 per alarm per month.
