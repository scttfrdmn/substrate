# EventBridge

**Endpoint:** `events.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AWSEvents.{Op}`)

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateEventBus | A custom bus with `Description`, `KmsKeyIdentifier`, `DeadLetterConfig`, `LogConfig` and `Tags`; `ResourceAlreadyExistsException` for a taken name or `default`, `LimitExceededException` past 100 buses — see [Event buses](#event-buses) |
| DescribeEventBus | Name or ARN, the default bus when omitted; reports the bus `Policy`; `ResourceNotFoundException` for an absent bus |
| UpdateEventBus | A member the request carries replaces the stored one, an omitted one is kept, an empty one (including `LogConfig: {}`) clears it |
| DeleteEventBus | Refuses the default bus and a bus that still has rules (`ValidationException`); an absent bus answers `200` |
| ListEventBuses | The default bus and every custom bus, by name, with `NamePrefix`, `Limit` and `NextToken` |
| PutPermission | Sets the bus `Policy` from `Policy`, or adds the statement `StatementId`, `Action`, `Principal` and `Condition` describe — see [Bus policy](#bus-policy) |
| RemovePermission | Removes one statement by `StatementId`, or the whole policy with `RemoveAllPermissions` |
| PutRule | On the bus `EventBusName` names (name or ARN); stores `RoleArn` and, when it creates the rule, `Tags`; an update replaces every member but keeps the tags, as the page states; `LimitExceededException` past 300 rules on a bus (100 in `af-south-1` and `eu-south-1`) |
| DescribeRule | Reports `RoleArn` and `CreatedBy`; `ResourceNotFoundException`/400 for an absent rule |
| DeleteRule | `ValidationException` while the rule has targets |
| EnableRule | Sets `State` to `ENABLED` |
| DisableRule | Sets `State` to `DISABLED` |
| ListRules | The rules on one bus; reports `RoleArn` |
| PutTargets | Merges by target `Id`; `ResourceNotFoundException` if the rule does not exist |
| RemoveTargets | `ResourceNotFoundException` if the rule does not exist |
| ListTargetsByRule | |
| ListRuleNamesByTarget | The rules on one bus with a target whose `Arn` is `TargetArn`, by name, with `Limit` and `NextToken` |
| PutEvents | Stores last 100 events in ring buffer |
| TagResource | Merges onto a rule's or bus's tags, up to 50 — see [Tags](#tags) |
| UntagResource | Removes the named keys; an absent key is not an error |
| ListTagsForResource | A rule's or bus's tags sorted by key, and `"Tags": []` for an untagged one |

Every rule operation takes `EventBusName` and addresses the rule on that bus, so two buses can each
hold a rule of the same name. A rule on the default bus has the ARN `rule/{name}` and one on a custom
bus `rule/{bus}/{name}`.

Wherever a bus can be named by ARN — `EventBusName` on the rule operations, `PutPermission`,
`RemovePermission` and `PutEvents` entries, and `Name` on `DescribeEventBus` and `UpdateEventBus` —
the ARN names the bus in its own account and Region. Substrate models no cross-account or cross-Region
bus access, so an ARN naming another account or Region answers `ResourceNotFoundException`/400 (a
per-entry failure on `PutEvents`), and an ARN that is not an event-bus ARN is `ValidationError`/400.
`ListRules` and `ListRuleNamesByTarget` on a bus that does not exist answer `ResourceNotFoundException`,
and a `PutEvents` entry naming one fails with `ResourceNotFoundException` and `Event bus {name} does not
exist.`. `PutEventsResultEntry` lists no code for an absent bus, so that one is substrate's reading.

## Event buses

Every account has a `default` bus in every Region, which no call creates or deletes. It is answered
without having been written; it has no `CreationTime` or `LastModifiedTime`. A custom bus reports
both, on the simulated clock.

| Request | Answer |
|---|---|
| `CreateEventBus` with `EventSourceName` | `ResourceNotFoundException`/400: substrate models no partner event sources |
| A custom bus name containing `/` | `ValidationError`/400 |
| A 100th bus in an account and Region, counting `default` | `LimitExceededException`/400 |
| `DeleteEventBus` of `default` | `ValidationException`/400 |
| `DeleteEventBus` of a bus with rules | `ValidationException`/400, `EventBus can't be deleted since it has rules.` |
| `PutRule` on a bus that does not exist | `ResourceNotFoundException`/400, `Event bus {name} does not exist.` |

`ConcurrentModificationException` and `InvalidStateException`, which `CreateEventBus` also
publishes, are never answered: substrate serializes every bus and rule write, and the second code
concerns partner event sources.

## Bus policy

`PutPermission` with `StatementId`, `Action` and `Principal` adds a statement, replacing one with the
same `Sid`:

```json
{"Sid":"MyStatement","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::111122223333:root"},
 "Action":"events:PutEvents","Resource":"arn:aws:events:us-east-1:123456789012:event-bus/default"}
```

`Principal: "*"` is written as `"*"`, and a `Condition` as `{"{Type}": {"{Key}": "{Value}"}}`. That is
the shape `aws_cloudwatch_event_permission` reads back. `PutPermission` with `Policy` replaces the whole
policy, as `aws_cloudwatch_event_bus_policy` uses it, and cannot be combined with the other four. It
must be a JSON object whose `Statement` is an object or a list of objects, or it is
`ValidationError`/400. A policy over 10,240 characters is `PolicyLengthExceededException`/400. Substrate records the policy and
does not evaluate it.

## Tags

A rule and a bus each store their tags, which Terraform reads through `ListTagsForResource` on every
refresh. The Resource Groups Tagging API reads and writes them too: `GetResources` reports a tagged or
previously tagged rule or bus, and `TagResources` and `UntagResources` write through the same code,
refusing an `aws:` key with `InvalidParameterException`.

| Request | Answer |
|---|---|
| A rule ARN, `rule/{name}` or `rule/{bus}/{name}` | The rule on that bus in the ARN's own account and Region |
| An event-bus ARN, `event-bus/{name}` | The bus; tagging the `default` bus gives it a record |
| A rule or bus ARN in another account or Region | `ResourceNotFoundException`/400, and `InvalidParameterException` in the tagging API's `FailedResourcesMap` ("You can only tag resources that are located in the specified AWS Region for the AWS account"); with no EventBridge plugin registered, the tagging API checks the account only |
| Any other ARN | `ValidationError`/400 |
| More than 50 tags after the merge, on `TagResource`, a creating `PutRule` or `CreateEventBus` | `LimitExceededException`/400 |
| A key outside 1–128 characters or a value over 256 | `ValidationError`/400 |

The 50-tag ceiling is published on `TagResource`'s page without a code; `LimitExceededException` is
the code `PutRule` publishes for a service quota, so using it on `TagResource` is substrate's reading.

Where no page or observed report gives AWS's text, the messages are substrate's own.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::Events::EventBus | Name | `Fn::GetAtt` `Arn` and `Name`; `Policy` is set through `PutPermission`; every property but `Name` and `EventSourceName` updates in place, and a dropped `Policy`, `DeadLetterConfig` or `LogConfig` is cleared |
| AWS::Events::Rule | Name | Created on the bus `EventBusName` names; `Fn::GetAtt` `Arn` and `RuleName`; `Targets` are put with their `Id`, `Arn` and `RoleArn`; every property but `Name` updates in place, and changed `Tags` and dropped `Targets` are applied. A changed `EventBusName` moves the rule: it is created on the new bus with its declared targets and any target added outside the stack, then the old rule's targets are removed and it is deleted, and a failed step puts both buses back. On a rollback the rule is moved back from wherever the failed update left it. A stack delete removes the declared targets before `DeleteRule`; a target added outside the stack still blocks it |

## Cost

EventBridge custom events: $1.00 per million events.
