# EventBridge Scheduler

**Endpoint:** `scheduler.{region}.amazonaws.com`
**Protocol:** REST/JSON (path and HTTP method, no `X-Amz-Target`)

## Supported operations

| Operation | Route | Notes |
|-----------|-------|-------|
| CreateSchedule | `POST /schedules/{Name}` | Answers **200**, not 201 — see below. A `GroupName` that does not exist is `ResourceNotFoundException`/404 — see [Schedule groups and tags](#schedule-groups-and-tags). Arms the schedule's timer — see [A due schedule invokes its target](#a-due-schedule-invokes-its-target) |
| GetSchedule | `GET /schedules/{Name}` | Reports every member the create or update set, including `ActionAfterCompletion`, `StartDate`, `EndDate` and `KmsKeyArn` — see [The completion and window members are kept](#the-completion-and-window-members-are-kept) — and `Target.DeadLetterConfig`, `Target.SqsParameters` and `Target.EventBridgeParameters` |
| UpdateSchedule | `PUT /schedules/{Name}` | Replaces the whole configuration, as the page publishes — see below. Re-arms the timer |
| DeleteSchedule | `DELETE /schedules/{Name}` | Removes the timer with the record, so a deleted schedule never fires. A schedule that does not exist is `ResourceNotFoundException`/404, as the page publishes; it answered 200 before [#1501](https://github.com/scttfrdmn/substrate/issues/1501) |
| ListSchedules | `GET /schedules` | Filters and cursor read from the published `ScheduleGroup`, `NamePrefix`, `State`, `MaxResults` and `NextToken` keys — [not the lowerCamel ones the sibling operations use](#the-query-string-is-read-in-the-published-spelling-which-differs-per-operation). An absent `ScheduleGroup` lists **every group's** schedules, ordered by group then name; a group that does not exist lists none. A `NextToken` no previous call returned answers [`ValidationException` / 400](/reference/concepts/listing-order#eventbridge-scheduler-s-one-listing-refuses-a-token-the-parameter-is-not-documented-to-accept) rather than page one. All three filters are applied [before the page is cut](/reference/concepts/listing-order#a-published-filter-selects-what-the-page-is-cut-from), so a page carries `min(MaxResults, remaining matches)` and never an empty array alongside a cursor |
| CreateScheduleGroup | `POST /schedule-groups/{Name}` | `Tags` (Key 1–128, Value 1–256, at most 200). A taken name, including `default`, is `ConflictException`/409. Answers `ScheduleGroupArn` |
| GetScheduleGroup | `GET /schedule-groups/{Name}` | `Arn`, `Name`, `State` (`ACTIVE`), `CreationDate`, `LastModificationDate`. `default` exists without a create. A missing group is `ResourceNotFoundException`/404, "Schedule group *name* does not exist." |
| ListScheduleGroups | `GET /schedule-groups` | `NamePrefix`, `MaxResults` (default 20, clamped to 100) and `NextToken`; includes `default` |
| DeleteScheduleGroup | `DELETE /schedule-groups/{Name}` | Deletes the group's schedules, then the group, in the one request. `default` is `ValidationException`/400 |
| TagResource | `POST /tags/{ResourceArn}` | Schedule groups only: any other ARN is `ValidationException`/400. Replaces a key's value |
| UntagResource | `DELETE /tags/{ResourceArn}?TagKeys=…` | `TagKeys` repeated; required |
| ListTagsForResource | `GET /tags/{ResourceArn}` | `Tags`, sorted by key |

## What a create or update is refused for

`API_CreateSchedule` and `API_UpdateSchedule` publish exactly three `Required: Yes` body members, plus
a required `Name` in the URI, and substrate checked none of them before
[#1008](https://github.com/scttfrdmn/substrate/issues/1008): a `POST` with an empty body created a
schedule with no expression and no target, which `GetSchedule` then reported as a live resource. Every
constraint below is checked before any state is read, and every failure is `ValidationException`/**400**
— the only refusal either page publishes for an input that fails a constraint, which is why the message
rather than the code carries which constraint failed.

| Member | Published constraint | Refusal |
|--------|----------------------|---------|
| `Name` (URI) | `Required: Yes`, length 1–64, pattern `[0-9a-zA-Z-_.]+` | names `'name'` |
| `ScheduleExpression` | `Required: Yes`, length 1–256 | names `'scheduleExpression'` |
| `Target` | `Required: Yes` | names `'target'` |
| `Target.Arn` | `Required: Yes`, length 1–1600 | names `'target.arn'` |
| `Target.RoleArn` | `Required: Yes`, length 1–1600 | names `'target.roleArn'` |
| `FlexibleTimeWindow` | `Required: Yes` | names `'flexibleTimeWindow'` |
| `FlexibleTimeWindow.Mode` | `Required: Yes`, `Valid Values` OFF \| FLEXIBLE | names `'flexibleTimeWindow.mode'` |
| `FlexibleTimeWindow.MaximumWindowInMinutes` | `Required: No`, `Valid Range` 1–1440 | 0 and 1441 refused; **absent accepted** |
| `GroupName` | `Required: No`, length 1–64, pattern `[0-9a-zA-Z-_.]+` | names `'groupName'` |
| `Description` | length 0–512 | names `'description'` |
| `State` | `Valid Values` ENABLED \| DISABLED | names `'state'` |
| `ScheduleExpressionTimezone` | length 1–50 | names `'scheduleExpressionTimezone'` |
| `ActionAfterCompletion` | `Valid Values` NONE \| DELETE | names `'actionAfterCompletion'` |
| `KmsKeyArn` | length 1–2048, pattern `arn:aws[a-z-]*:kms:[a-z0-9\-]+:\d{12}:(key\|alias)\/[0-9a-zA-Z-_]*` | names `'kmsKeyArn'` |
| `StartDate`, `EndDate` | Timestamp (epoch seconds) | a non-number is refused as an invalid body |

`MaximumWindowInMinutes` is the one member whose *absence* is observably different from a zero, which is
why the request type holds it as a pointer: an explicit `0` is outside the published range and is
refused, where an absent member is accepted. **AWS does not publish that a `FLEXIBLE` window requires
the bound**, so substrate does not invent that rule — a `FLEXIBLE` window with no maximum is accepted.

Two published constraints are deliberately **not** checked, and the omissions are recorded rather
than silently taken:

- `Target.RoleArn`'s IAM-role ARN **pattern**. Substrate does not model the role, so refusing a string
  that is not an ARN would refuse a call this emulator otherwise serves without ever needing the value
  to resolve. Its `Required: Yes` and its length are checked.
- The templated-target objects for targets substrate records rather than dispatches (`EcsParameters`,
  `EventBridgePutEventsV2Parameters`, `EventBridgePutRawEventsParameters`, `KinesisParameters`,
  `SageMakerPipelineParameters`) are likewise unmodelled, so they are neither decoded nor reported.
  Since [#1501](https://github.com/scttfrdmn/substrate/issues/1501) the three a due schedule acts on are
  decoded, checked and reported: `DeadLetterConfig.Arn` (length 1–1600, the published SQS-ARN pattern),
  `EventBridgeParameters` (`DetailType` 1–128 and `Source` 1–256, both `Required: Yes`; `Source`'s
  pattern is not checked) and `SqsParameters.MessageGroupId` (1–128).

Two checks were added in [#1514](https://github.com/scttfrdmn/substrate/issues/1514), on both
`CreateSchedule` and `UpdateSchedule`:

- `RetryPolicy`'s published Valid Ranges, `MaximumRetryAttempts` 0–185 and `MaximumEventAgeInSeconds`
  60–86400, are checked when a member is sent, in the same *"1 validation error detected"* form as
  `MaximumWindowInMinutes`.
- A `ScheduleExpression` substrate cannot evaluate — a malformed `at()`, `rate()` or `cron()` — or a
  `ScheduleExpressionTimezone` that is not an IANA zone is refused with `ValidationException`/400. It used
  to be stored, reported as sent, and never fire. The expression is parsed by the same parser the clock
  fires schedules with, which accepts the three forms and every wildcard the User Guide's
  `schedule-types` page publishes (`, - * / ? L W #`), so a refusal is an expression the clock could never
  fire. AWS refuses such input with `ValidationException` but publishes neither which inputs nor the
  message, so which inputs are refused is substrate's reading and the messages are its own. A zone must
  be named exactly, case included, in the copy of the IANA database (release 2026c) embedded in the
  binary, which the clock also evaluates it with. The host's zoneinfo is never consulted and `Local` is
  refused, so acceptance and firing times do not depend on the host.

## `UpdateSchedule` replaces the schedule, it does not merge into it

`API_UpdateSchedule` opens with the property verbatim:

> Updates the specified schedule. When you call `UpdateSchedule`, EventBridge Scheduler uses all the
> information that you have provided and replaces your schedule. You will lose any information that you
> haven't provided, such as a description.

Substrate assigned each optional member only when the request supplied a non-empty one, so an omitted
`Description`, `ScheduleExpressionTimezone`, `State`, `Target.Input` or `Target.RetryPolicy` survived the
update. A caller following AWS's own advice — send the whole configuration, or accept losing what you
omit — saw a stale value instead of a reset. Since
[#1089](https://github.com/scttfrdmn/substrate/issues/1089) the create and the update resolve a body
through one function, so the two doors cannot disagree about what an omitted member means.

`State`'s default is the one part of this with no API Reference citation. The `State` entry is
byte-identical on all four pages that carry it (`API_UpdateSchedule`, `API_CreateSchedule`,
`API_GetSchedule`, `API_ScheduleSummary`) and none publishes a `Default:` line; the CLI and
CloudFormation references are equally silent, and `API_GetSchedule` has no Examples section. `ENABLED` is
documented in the **User Guide** instead — *"By default, the EventBridge Scheduler enables your
schedule"* (`scheduler/latest/UserGuide/getting-started.html`) — and that is the citation substrate
applies at both doors.

`ClientToken` came off the `GetSchedule` response with the same change. It is not among that page's
fifteen published response elements — it is a request-only idempotency token, published on the create and
the update and on no read. It was removed *with* the full-replace fix rather than on its own because an
omitted member now reverts: leaving it would have made an unpublished field start changing under callers
who never named it. The record keeps it as recorded intent.

## The completion and window members are kept

`ActionAfterCompletion`, `StartDate`, `EndDate` and `KmsKeyArn` are published on `CreateSchedule`,
`UpdateSchedule` and `GetSchedule`, and until [#1442](https://github.com/scttfrdmn/substrate/issues/1442)
substrate decoded none of them: a schedule created with `ActionAfterCompletion: DELETE` and an `EndDate`
read back with neither, so a consumer relying on finished schedules deleting themselves could only
assert on its own outgoing request. They are now decoded, validated (see the table above), stored and
reported. The dates are epoch seconds on the wire, as `CreationDate` is, and a fractional second sent is
a fractional second read back.

Each follows the replace rule: an `UpdateSchedule` naming new values replaces them, and one omitting a
member clears it. None of the three pages publishes a default for `ActionAfterCompletion`, so a schedule
created without it reports no `ActionAfterCompletion` member rather than an invented `NONE`; the dates
and the key are absent the same way.

Since [#1501](https://github.com/scttfrdmn/substrate/issues/1501) and
[#1475](https://github.com/scttfrdmn/substrate/issues/1475) three of them are **acted on**: `StartDate`
and `EndDate` bound a recurring schedule's occurrences, and `ActionAfterCompletion: DELETE` deletes a
finished schedule — see [A due schedule invokes its target](#a-due-schedule-invokes-its-target).
Nothing is encrypted under `KmsKeyArn`. No relation between the two dates is checked, because no page
publishes one.

## Schedule groups and tags

`API_CreateScheduleGroup`, `API_GetScheduleGroup`, `API_ListScheduleGroups` and
`API_DeleteScheduleGroup` publish the four group routes, and `API_TagResource`, `API_UntagResource` and
`API_ListTagsForResource` the three tag routes. Their `ResourceArn` pattern admits only a
`schedule-group/` ARN — *"You can only assign tags to schedule groups"* — so a schedule's ARN, or any
other, is `ValidationException`/400, and a group in another account or Region is
`ResourceNotFoundException`/404.

- **The `default` group.** *"Your AWS account comes with a default scheduler group"*
  (`managing-schedule-group.html`). Substrate materializes it the first time a request needs it in an
  account and Region, at that request's simulated time, so its `CreationDate` replays. It is listed,
  readable and taggable. *"You can't delete, or edit, the default group"*: no page publishes the
  refusal's code, and substrate answers `ValidationException`/400.
- **A schedule needs its group.** `CreateSchedule` with a `GroupName` that does not exist is
  `ResourceNotFoundException`/404, one of the page's published errors; before
  [#1501](https://github.com/scttfrdmn/substrate/issues/1501) any name was accepted. Existing
  default-group schedules keep their state keys (`sched:{account}/{region}/default/{name}`).
- **Deletion.** *"Deleting a schedule group results in EventBridge Scheduler deleting all schedules
  associated with the group. When you delete a group, it remains in a DELETING state until all of its
  associated schedules are deleted"* (`API_DeleteScheduleGroup`). Substrate deletes the schedules and then
  the group inside the one request, so `DELETING` lasts no time and no later request observes it: the
  next `GetScheduleGroup` answers *"Schedule group TestGroup does not exist."* in the form the user guide
  shows, and none of the group's schedules fires again.
- **Not modelled:** the quota of 500 groups, and `ClientToken` idempotency on either the create or
  the delete (the token is accepted and ignored).

## A due schedule invokes its target

Before [#1501](https://github.com/scttfrdmn/substrate/issues/1501) substrate stored schedules and fired
none. A schedule now invokes its target when the simulated clock passes an occurrence. This is
clock-driven work in the sense of `emulator/clock_driven.go`: before every top-level request, live and on
replay, the Scheduler plugin fires whatever is due at the clock's current reading. A frozen clock fires
nothing; `AdvanceTime` past an occurrence fires it before the observing request is answered. Each
schedule keeps a timer at `sched_timer:{account}/{region}/{group}/{name}`, armed by `CreateSchedule`,
re-armed by `UpdateSchedule` and removed by `DeleteSchedule` and `DeleteScheduleGroup`.

**When an occurrence is due** (`schedule-types.html`):

| Expression | Occurrences |
|------------|-------------|
| `at(yyyy-mm-ddThh:mm:ss)` | Once, at that wall-clock time in `ScheduleExpressionTimezone` (UTC when absent). `StartDate` and `EndDate` are ignored, as the guide states. An instant already past when the schedule is armed fires at once |
| `rate(value unit)` | `minute(s)`, `hour(s)` or `day(s)`, a fixed duration in which no zone applies (*"days represents a 24-hour duration"*). The first occurrence is `StartDate`, or the moment the schedule is armed when there is none (*"your schedule starts invoking the target immediately"*); a `StartDate` already past keeps its phase |
| `cron(minutes hours day-of-month month day-of-week year)` | Evaluated in `ScheduleExpressionTimezone` with the published wildcards `, - * / ? L W #`, at or after `StartDate`. A time a spring-forward skips does not occur; a time a fall-back repeats occurs once, at its second occurrence, as the guide's daylight-saving example describes |

A recurring occurrence must fall strictly **before** `EndDate` (*"the date, in UTC, before which the
schedule can invoke its target"*). Every occurrence in a gap fires, each with its own scheduled time, up
to 100 per schedule per request; the rest fire on the next request.

- **`FlexibleTimeWindow`.** A `FLEXIBLE` window spreads invocations across the window, and the guide adds
  60-second precision; both are nondeterministic by nature. Substrate fires every occurrence at its
  scheduled time exactly, the window's start.
- **`State: DISABLED`.** A disabled schedule fires nothing, and its missed occurrences are not made up:
  re-enabling (an `UpdateSchedule`) resumes the series at its first occurrence after the update. A
  one-time schedule re-enabled after its instant fires at once: *"When re-enabling a one-time schedule
  after its original scheduled time has passed, the schedule may immediately invoke its target"*
  (`troubleshooting.html`).
- **`UpdateSchedule`** replaces the configuration, and `API_UpdateSchedule` publishes nothing about the
  series, so substrate's reading is that an update keeping the expression, `ScheduleExpressionTimezone`
  and `StartDate` keeps the series: a `rate()` schedule with no `StartDate` keeps its anchor (it neither
  restarts nor fires at the update), a one-time schedule that has fired is not fired again, and an
  invocation being retried keeps being retried. A new `EndDate` re-clips the series. An update that
  changes any of the three starts a new series from the update. An update setting
  `ActionAfterCompletion: DELETE` on a schedule that has already finished deletes it on the next request.
- **Cost of a request.** A request with nothing due reads one key, `sched_wake`, the earliest time any
  timer needs attention. When something is due, each timer is read and only the due ones load their
  schedule. A finished schedule kept under `NONE` keeps its timer — it is how an update knows a
  one-time schedule has fired — but never comes due again. A schedule whose stored record will not
  decode is set aside with a warning rather than stopping the others.

### Universal targets

A universal target's ARN is `arn:aws:scheduler:::aws-sdk:service:apiAction`, and its `Input` is *"A
well-formed JSON you specify with the request parameters that EventBridge Scheduler sends to the target
API"* (`managing-targets-universal.html`). The service is the AWS SDK service identifier — *"for
Amazon Cognito Identity Provider, use cognitoidentityprovider (not cognito-idp)"* — which substrate maps
to its plugin (`ssm`, `autoscaling`, `sfn` → Step Functions, `cloudwatch`, `cloudwatchlogs`,
`elasticloadbalancingv2`, and the rest of `schedulerSDKServices` in `emulator/scheduler_targets.go`). The
`apiAction` is matched case-insensitively against the plugin's routed operations. The `Input` becomes a
request in the plugin's own protocol, by one generic rule per protocol plus, for Query and EC2, the
shape facts a JSON object cannot carry:

| Protocol | Request |
|----------|---------|
| JSON | The `Input` is the body, under the plugin's `X-Amz-Target` — `ssm:putParameter` sends `AmazonSSM.PutParameter` |
| Query | The `Input` is flattened into form parameters with the operation's `Action` and the service's API `Version`: a structure member is `Parent.Member`, a list element `Parent.member.N`, a boolean `true`/`false`, an empty list the bare name. `autoscaling:updateAutoScalingGroup` with `{"AutoScalingGroupName": "g", "MinSize": 0}` sends `AutoScalingGroupName=g&MinSize=0`. A **map** member is `Parent.entry.N.key`/`.value` with the model's key and value names — `sns:createTopic`'s `Attributes` is `Attributes.entry.1.key=DisplayName`, `sns:publish`'s `MessageAttributes` is `MessageAttributes.entry.1.Name`/`.Value.StringValue` — and flattened and renamed lists and renamed members follow the model |
| EC2 | The same, with `Parent.N` for a list element and the ec2 protocol's member names: `TagSpecifications` is sent as `TagSpecification.1.Tag.1.Key`, `InstanceIds` as `InstanceId.1` |
| REST | Only `lambda:invoke` is bound: `FunctionName` and `Qualifier` to the URI, `InvocationType` to its header (`Event` when absent), `Payload` to the body. A qualified `FunctionName` (`…:function:name:alias`, `name:2`) sends its version or alias as the `Qualifier`; one that contradicts an explicit `Qualifier` fails with `InvalidParameterValueException`, *"The derived qualifier from the function name does not match the specified qualifier."*, as the guide's troubleshooting section shows Lambda answering. Any other REST action is **recorded as intent** — logged, counted as delivered and not dispatched |

The shape facts come from `emulator/scheduler_query_model_gen.go`, which `make scheduler-query-model`
generates from the botocore models bundled with the locally installed AWS CLI v2 (the header records the
model versions). It walks the input shape of every operation substrate routes for the twelve Query and
EC2 SDK services and records only the paths whose serialization departs from the generic rule — a map, a
flattened or renamed list, a renamed member. `go run ./cmd/gen-scheduler-query-model -check` reports
a stale table, but CI has no AWS CLI to run it against, so regenerate it when a Query plugin routes a new
operation. The emitter itself is tested in CI against fixture models in
`cmd/gen-scheduler-query-model/testdata`.

A universal target naming a service substrate does not emulate is recorded as intent too. An `Input`
that is not a JSON object fails the invocation with `ValidationException`, as the guide's
troubleshooting section describes for an `Input` the target rejects. The guide's list of unsupported
read-only prefixes (`get`, `describe`, `list`, …) is not enforced.

### Templated targets

A templated target names a resource ARN, and *"EventBridge Scheduler automatically assumes that you want
to call the supported API operation for that service"* (`managing-targets-templated.html`). Substrate
dispatches the five whose services it emulates; the other seven (CodeBuild, CodePipeline, ECS, Inspector,
Kinesis, Firehose, SageMaker) are recorded as intent.

| Target ARN | Operation | The `Input` is |
|------------|-----------|----------------|
| `arn:aws:lambda:…:function:name[:qualifier]` | Lambda `Invoke`, with `X-Amz-Invocation-Type: Event` — *"EventBridge Scheduler invokes your Lambda function asynchronously"* (Lambda developer guide) | the payload |
| `arn:aws:sqs:…:queue` | SQS `SendMessage`, with `SqsParameters.MessageGroupId` | `MessageBody` |
| `arn:aws:sns:…:topic` | SNS `Publish` | `Message` |
| `arn:aws:states:…:stateMachine:name` | Step Functions `StartExecution` | `input` |
| `arn:aws:events:…:event-bus/name` | EventBridge `PutEvents`, with `EventBridgeParameters`' `DetailType` and `Source` | `Detail` |

In both kinds the four context attributes of `managing-schedule-context-attributes.html` —
`<aws.scheduler.schedule-arn>`, `<aws.scheduler.scheduled-time>`, `<aws.scheduler.execution-id>` and
`<aws.scheduler.attempt-number>` — are replaced in the `Input` before it is sent. The execution ID is a
deterministic 16-hex-digit digest of the schedule, the occurrence and the attempt.

### Authorization

*"The execution role you specify must have the permissions to call the API operation you want your
schedule to target."* Each target request, and each dead-letter send, is authorized as `Target.RoleArn`
through the same `AuthController.CheckAccess` the CloudFormation deployer uses for a stack's role. A role
substrate's IAM holds has its policies evaluated; one it does not hold is not enforced, as for every other
caller. A denial (`AccessDeniedException`) is a permanent failure. The role's trust policy is not
consulted.

### Retries, the dead-letter queue and the seed

`API_RetryPolicy`: *"Retry attempts with exponential backoff continue until either the maximum number of
attempts is made or until the duration of the MaximumEventAgeInSeconds is reached."* AWS publishes
neither the backoff's intervals nor a default — `API_RetryPolicy`, `API_Target`, the CLI reference and
`AWS::Scheduler::Schedule RetryPolicy` list both members as optional with a range and nothing more — so
three readings are substrate's own: each member a target omits, whether it omits the whole
`RetryPolicy` or one member, takes its published maximum (185 retries, 86,400 seconds), and an explicit
`MaximumRetryAttempts: 0` means no retries; retry *n* is due 60·2^(n−1) seconds after the attempt
before it, capped at 15 minutes; and event age is measured from the occurrence's scheduled time.
`GetSchedule` reports the members as sent, so an omitted one is absent rather than an invented default.

A throttle, a timeout and any 5xx answer are **retryable**; any other answer — a validation error, a
missing resource, a denial — is **permanent** and is not retried. `configuring-schedule-dlq.html`
draws that line and publishes no list, so the classification is substrate's.

An invocation that fails for good is sent to `Target.DeadLetterConfig`'s queue as the role, with the
published dead-letter event: the `Input` as the body, and `ERROR_CODE`, `ERROR_MESSAGE`, `EXECUTION_ID`,
`IS_PAYLOAD_TRUNCATED`, `RETRY_ATTEMPTS`, `SCHEDULED_TIME`, `SCHEDULE_ARN` and `TARGET_ARN` as `String`
message attributes, plus `EXHAUSTED_RETRY_CONDITION` (`MaximumRetryAttempts` or
`MaximumEventAgeInSeconds`) — which *"will be present if the error from the target API is a retryable
error, and not a permanent error"*. With no dead-letter queue the event is dropped.

The failure path is seedable, so a consumer's handling of it can be tested without breaking a target:

```
POST /v1/scheduler/invocation-outcome
{"groupName": "aegc-dev", "scheduleName": "lease-state", "errorCode": "ThrottlingException",
 "errorMessage": "Rate exceeded", "retryable": true, "failAttempts": 2}
```

`scheduleName` is a schedule's name or `"*"`; `groupName` is optional and, absent, matches any group,
so `{"groupName": "aegc-dev", "scheduleName": "*"}` affects only that group's schedules. The most
specific seed applies: the schedule's own, its name in any group, its group's wildcard, the global
wildcard. Each matching attempt fails with the seeded error without calling the target. `failAttempts`
bounds how many attempts of each invocation fail (zero or absent fails every one), and `retryable`
overrides the classification of `errorCode`. `DELETE /v1/scheduler/invocation-outcome` clears one seed by
`scheduleName` and `groupName`, a group's seeds by `groupName` alone, or all of them. Seeds are
control-plane writes, so a replay re-applies them in position.

### Completion and `ActionAfterCompletion`

*"For one-time schedules, this occurs after the schedule has invoked its target once. For recurring
schedules … your schedule is deleted after its last invocation. A recurring schedule's last invocation
is the invocation that occurs closest to the EndDate you specify. If you configure a schedule with
automatic deletion but do not specify a value for EndDate, EventBridge Scheduler does not automatically
delete the schedule"* (`managing-schedule-delete.html`). A schedule is finished once it has no next
occurrence and no invocation awaiting a retry — and, for a recurring schedule that never fired, once the
clock has passed its `EndDate`. A finished schedule with `ActionAfterCompletion: DELETE` is deleted:
`GetSchedule` answers `ResourceNotFoundException`/404 and `ListSchedules` drops it. With `NONE`, or with
none named, it stays, and its `State` is unchanged.

### Replay

The timer, the retries and the seeds are all state, and every dispatch carries a request ID derived from
the triggering request, the schedule, the occurrence and the attempt. A replay pins the clock to each
recorded event, so the same occurrences come due at the same request and mint the same identifiers; the
recorded copies of the dispatches are skipped rather than executed twice, as for an event-source
mapping's polls.

### CloudWatch metrics

Each invocation counts toward the `AWS/Scheduler` metrics the user guide's *Monitoring* page
publishes ([#1513](https://github.com/scttfrdmn/substrate/issues/1513)). They are reported both
with a `ScheduleGroup` dimension and as account metrics with no dimensions, which sum every group.
`GetMetricData`, `GetMetricStatistics` and `ListMetrics` answer them.

| Metric | Counted |
|---|---|
| `InvocationAttemptCount` | Every attempt, the first and each retry |
| `TargetErrorCount` | Every failed attempt: a target's error, a seeded failure, a denial of the role, or an input substrate could not build |
| `TargetErrorThrottledCount` | A failed attempt that is a throttle: HTTP 429, or a code naming a throttle |
| `InvocationDroppedCount` | Every invocation that fails for good, whether or not a dead-letter queue then receives it |
| `InvocationsSentToDeadLetterCount` | Every dead-letter event the queue accepts |
| `InvocationsFailedToBeSentToDeadLetterCount`, and the same name suffixed `_<error_code>` | Every dead-letter event the queue refuses; the suffix is the refusal's code |

Each event counts in the minute its attempt was due, even when the clock reaches that minute in
one jump. Each event is one sample of value 1 with unit `Count`, so a period's `Sum` and
`SampleCount` are the event count, and its `Average`, `Minimum` and `Maximum` are 1.

Nothing is sent from the Scheduler to CloudWatch. The Scheduler keeps a counter per metric per
minute per group beside its timers, and CloudWatch reads those counters when it is asked, as it
derives [EC2 vCPU usage](/reference/monitoring#ec2-vcpu-usage-in-aws-usage) from EC2's instances. So no extra event is
recorded, and a replay rebuilds the same counters. The counters are kept one record per group
per hour, and a run writes each hour it counted in once. A group keeps the last 168 hours that
saw an invocation, a week of hours.

Two metrics are never counted. `InvocationThrottleCount` would need a Scheduler invocation quota
to exceed, and substrate models none. `InvocationsSentToDeadLetterCount_Truncated_MessageSizeExceeded`
would need a truncated dead-letter payload, and substrate truncates none.

### What is not modelled

- The REST universal-target bindings noted above
  ([#1511](https://github.com/scttfrdmn/substrate/issues/1511)), and the templated targets recorded as
  intent ([#1512](https://github.com/scttfrdmn/substrate/issues/1512)).
- An observable `DELETING` window, `ClientToken` idempotency and the quotas
  ([#1515](https://github.com/scttfrdmn/substrate/issues/1515)).
- The read-only-action restriction on universal targets, and the role's trust policy.

## `CreateSchedule` answers 200, not 201

`API_CreateSchedule`'s Response Syntax opens `HTTP/1.1 200`. Substrate answered 201, on the reasonable
but unpublished reading that a create is a creation; the page is the contract and it says 200.

## The request body is decoded in the published spelling

Both handlers used to unmarshal a caller's body straight into substrate's *storage* types, whose JSON
tags are snake_case (`role_arn`, `retry_policy`, `maximum_window_in_minutes`). A caller's
`Target.RoleArn`, `Target.RetryPolicy` and `FlexibleTimeWindow.MaximumWindowInMinutes` therefore never
survived the request — `GetSchedule` reported them empty however they were sent. The handlers now decode
into request types carrying the published names and fold those into the stored shape, which is also what
makes `RoleArn`'s `Required: Yes` checkable at all: a member the decode drops cannot be found missing.

## The query string is read in the published spelling, which differs per operation

The body's spelling was only half of it. `ListSchedules` read all five of its query parameters in the
lowerCamel form — `groupName`, `namePrefix`, `state`, `nextToken`, `maxResults` — and **no SDK sends
those names**, so every call arrived with none of the five set. The observable result was the worst
available: the operation answered the `default` group's first twenty schedules to every request,
attached a `NextToken` the next request then ignored, and applied no filter. A paginating loop either
spun or reread the same page, and nothing in any response said so. Every test in the tree spoke
substrate's dialect rather than AWS's, which is why it survived
([#1226](https://github.com/scttfrdmn/substrate/issues/1226)).

**The split is the API Reference's own, not substrate's**, which is why the fix is confined to one
operation. `API_ListSchedules` publishes

```
GET /schedules?MaxResults={MaxResults}&NamePrefix={NamePrefix}&NextToken={NextToken}&ScheduleGroup={GroupName}&State={State}
```

— PascalCase throughout, and the group is bound to **`ScheduleGroup`** even though the parameter list
calls it `GroupName`, so `GroupName` never appears on the wire. `API_GetSchedule` publishes
`GET /schedules/{Name}?groupName={GroupName}` and `API_DeleteSchedule` publishes
`DELETE /schedules/{Name}?clientToken={ClientToken}&groupName={GroupName}` — lowerCamel, and a
different key for the same concept. Those two handlers were already right and were deliberately left
alone; `emulator/scheduler_query_keys.go` names both spellings in one place so neither can later be
"corrected" into the other.

The lowerCamel names are **not** accepted as aliases for `ListSchedules`. AWS ignores a query
parameter its model does not carry, so honoring one would be the same defect facing the other way: a
call that filters against substrate and silently does not against AWS. `?maxResults=1` now reads the
default page, which is what AWS answers.

Two readings of substrate's own remain, and are recorded rather than fixed here. `MaxResults` has
**no published default** — the page publishes only a Valid Range of 1–100 — so the page size of 20 an
absent `MaxResults` gets is substrate's choice. And a `MaxResults` above the published maximum is
**clamped to 100 rather than refused**, as is a value of zero or below, which is silently ignored;
refusing an out-of-range page size is a class of its own and is not this operation's alone.

## An empty `Name` reaches the operation the caller named

`parseSchedulerOperation` used to normalise `/schedules/` to `/schedules`, so a caller that built the
path from an empty variable got `ListSchedules` — every schedule in the group, answered 200, to a
request for one schedule. All four empty-name guards below the router were therefore dead code. Since
[#1009](https://github.com/scttfrdmn/substrate/issues/1009) only `/schedules` is the collection route,
and `/schedules/` reaches `GetSchedule`, `CreateSchedule`, `UpdateSchedule` or `DeleteSchedule` on its
own verb and is refused there naming `'name'`. See *A request body that will not parse* above for the
whole-tree inventory of that fold.

## The account and Region a record carries reach no response

`SchedulerRecord` declares its account and Region under wire-visible `json` tags, because the record is what
`MemoryStateManager` snapshots and a replay reads back. Neither reaches a body: `GetSchedule` answers `schedRecordToWire`, and the rest answer an ARN or a summary list.
`TestSchedulerWire_ScheduleResponsesCarryNoBookkeepingMember` in `emulator/scheduler_wire_test.go` drives all five routed operations and walks each decoded document for either member at any
depth ([#756](https://github.com/scttfrdmn/substrate/issues/756)). The record spells the account `account_id`, which a case fold does not reach, so the walk names that spelling too.

## Cost

Not modelled.
