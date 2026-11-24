# EventBridge

**Endpoint:** `events.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AWSEvents.{Op}`)

## Supported operations

| Operation | Notes |
|-----------|-------|
| ListEventBuses | Returns the default bus only, with the calling account's and Region's ARN |
| PutRule | Stores `RoleArn` and, when it creates the rule, `Tags`; an update replaces every member but keeps the tags, as the page states — see [Rules carry tags](#rules-carry-tags) |
| DescribeRule | Reports `RoleArn` and `CreatedBy`; `ResourceNotFoundException`/400 for an absent rule |
| DeleteRule | |
| ListRules | Reports `RoleArn` |
| PutTargets | Merges by target `Id`; `ResourceNotFoundException` if the rule does not exist |
| RemoveTargets | `ResourceNotFoundException` if the rule does not exist; it answered `200` and stored an empty target list until #1641 |
| ListTargetsByRule | |
| PutEvents | Stores last 100 events in ring buffer |
| TagResource | Merges onto a rule's tags, up to 50 — see [Rules carry tags](#rules-carry-tags) |
| UntagResource | Removes the named keys; an absent key is not an error |
| ListTagsForResource | A rule's tags sorted by key, and `"Tags": []` for an untagged rule |

**Not modelled: `CreateEventBus`, `DescribeEventBus`, `DeleteEventBus`.** This table listed
all three until #1015, which is what the operation-docs check was written to catch — the
heading says *Supported*, and none of the three is in the dispatch. Substrate models the
default bus and no other, so there is no custom bus for them to act on. A rule's
`EventBusName` is recorded (defaulting to `default`) and echoed back, but it is not part of
the rule's key, so two rules of one name on different buses are one rule here and not two.
`PutTargets` and `RemoveTargets`, meanwhile, were routed all along and had no row.

## Rules carry tags

Since [#1607](https://github.com/scttfrdmn/substrate/issues/1607) a rule stores its tags, which
Terraform's `aws_cloudwatch_event_rule` reads through `ListTagsForResource` on every refresh, and the
Resource Groups Tagging API's `GetResources` reports a tagged or previously tagged rule.

| Request | Answer |
|---|---|
| A rule ARN, `rule/{name}` or `rule/{bus}/{name}` | The rule in the ARN's own account and Region; a `{bus}` other than the rule's is `ResourceNotFoundException`/400 |
| An event-bus ARN | `ResourceNotFoundException`/400: substrate stores no event-bus record to hold tags, so bus tags are not modelled |
| Any other ARN | `ValidationError`/400 |
| More than 50 tags after the merge, on `TagResource` or a creating `PutRule` | `LimitExceededException`/400 |
| A key outside 1–128 characters or a value over 256 | `ValidationError`/400 |

The 50-tag ceiling is published on `TagResource`'s page without a code; `LimitExceededException` is
the code `PutRule` publishes for a service quota, so using it on `TagResource` is substrate's reading.
The messages are substrate's own. `DescribeRule`, `DeleteRule`, `PutTargets` and `ListTargetsByRule`
answered `ResourceNotFoundException` at 404 before #1607; every EventBridge page that publishes the
code gives it 400.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::Events::Rule | RuleArn | |

## Cost

EventBridge custom events: $1.00 per million events.
