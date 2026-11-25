# Application Auto Scaling

**Endpoint:** `application-autoscaling.{region}.amazonaws.com`
**Protocol:** JSON 1.1 (`X-Amz-Target: AnyScaleFrontendService.{Op}`)

## Supported operations

| Operation | Notes |
|-----------|-------|
| RegisterScalableTarget | Creates or updates. A new target needs `MinCapacity` and `MaxCapacity`; an update changes only the members it names and ignores `Tags` (tag an existing target with `TagResource`). `RoleARN` defaults to the namespace's service-linked role ARN; EMR has none, so an EMR target must name one. A new DynamoDB or ECS target's resource must exist (see below). Answers `ScalableTargetARN` |
| DescribeScalableTargets | `ServiceNamespace` (required), `ResourceIds` (up to 50), `ScalableDimension`. Resource ID order; pages on `NextToken`/`MaxResults` (1–50, default 50) |
| DeregisterScalableTarget | Deletes the target's scaling policies, their alarms, and its scheduled actions |
| PutScalingPolicy | Creates or updates `StepScaling`, `TargetTrackingScaling` or `PredictiveScaling`. A target tracking policy creates two CloudWatch alarms and answers them in `Alarms` (see below) |
| DescribeScalingPolicies | `PolicyNames`, `ResourceId`, `ScalableDimension` filters; pages on `NextToken`/`MaxResults` (1–10, default 10). `Alarms` is always present |
| DeleteScalingPolicy | Deletes a target tracking policy's alarms with it |
| PutScheduledAction | Creates or updates. A new action needs `Schedule` in `at(...)`, `rate(...)` or `cron(...)` form. An update without `StartTime`/`EndTime` clears them, as the page states. Recorded only |
| DescribeScheduledActions | `ScheduledActionNames`, `ResourceId`, `ScalableDimension` filters; pages on `NextToken`/`MaxResults` (1–50, default 50) |
| DeleteScheduledAction | |
| DescribeScalingActivities | Always an empty list: substrate scales nothing |
| TagResource | Scalable targets only. At most 50 tags (`TooManyTagsException`); an `aws:` key is `ValidationException` |
| UntagResource | |
| ListTagsForResource | `Tags` is always present |

## Targets, policies and scheduled actions are recorded intent

A scalable target, its scaling policies and its scheduled actions are stored and answered back, and
none of them changes the resource's capacity. Registering a DynamoDB target does not touch the
table's provisioned throughput, a policy is never evaluated, and a scheduled action never runs:
what Application Auto Scaling does to a resource is the workload, which is outside substrate's
[scope](/scope). `DescribeScalingActivities` therefore always answers an empty list.

A policy's configuration is stored as it was sent (compacted) and answered back by
`DescribeScalingPolicies`, which reports only the configuration the policy's type uses.

## A target tracking policy creates its CloudWatch alarms

AWS creates CloudWatch alarms for a target tracking policy and names them in the response:
`TargetTracking-{ResourceId}-AlarmHigh-{uuid}` and `TargetTracking-{ResourceId}-AlarmLow-{uuid}`,
and for a DynamoDB target also `-ProvisionedCapacityHigh-` and `-ProvisionedCapacityLow-` ("four
CloudWatch alarms are created for each read and write auto-scaling event", the DynamoDB developer
guide). Substrate creates them through the CloudWatch plugin with `PutMetricAlarm`, so
`DescribeAlarms` lists exactly the alarms the policy's `Alarms` member names, and deleting the
policy or deregistering its target deletes them once the record no longer names them. A re-put of
the policy keeps the names and redefines the alarms; changing the policy to another type deletes
them. A `PutScalingPolicy` that fails after creating alarms deletes the ones it created.

The alarms' definitions are substrate's approximation, because AWS publishes their names and not
their metric, threshold or evaluation settings. The high alarm breaches above `TargetValue` over 3
one-minute periods and the low alarm below 90% of it over 15; a DynamoDB target's provisioned-capacity
alarms watch `ProvisionedReadCapacityUnits` or `ProvisionedWriteCapacityUnits` above `MaxCapacity`
and below `MinCapacity`. All act on the policy ARN. A
`CustomizedMetricSpecification` with a `MetricName` supplies the metric. The predefined metric types
`DynamoDBReadCapacityUtilization`, `DynamoDBWriteCapacityUtilization`,
`ECSServiceAverageCPUUtilization` and `ECSServiceAverageMemoryUtilization` map to their `AWS/DynamoDB`
or `AWS/ECS` metric, with the target's `TableName` (and `GlobalSecondaryIndexName`) or `ClusterName`
and `ServiceName` dimensions. Any other metric is alarmed on the namespace
`Substrate/ApplicationAutoScaling`, which is deliberately not an `AWS/` namespace.

## Validation

Every check answers `ValidationException` at HTTP 400, with substrate's own message:

- `ServiceNamespace` and `ScalableDimension` must be values the model lists, and the dimension must
  belong to the namespace (`dynamodb:table:ReadCapacityUnits` with `dynamodb`).
- `ResourceId` must have the dimension's published form: `table/{name}` for a DynamoDB table
  dimension, `table/{name}/index/{index}` for an index dimension, `service/{cluster}/{service}` for
  ECS, and so on through the `ResourceId` list on the API reference. A custom resource's ID is not
  checked.
- `StepScaling` is refused for DynamoDB, Comprehend, Lambda, Keyspaces, MSK, ElastiCache and
  Neptune; `TargetTrackingScaling` for EMR; `PredictiveScaling` for anything but ECS. The
  configuration for the policy's type is required, and a target tracking configuration needs
  `TargetValue` and a metric specification.

A policy or scheduled action on an unregistered target, and a deregistration of one, is
`ObjectNotFoundException` with AWS's observed wording: `No scalable target registered for service
namespace: dynamodb, resource ID: table/orders, scalable dimension: dynamodb:table:ReadCapacityUnits`.
A delete naming a policy or scheduled action the target does not hold is `ObjectNotFoundException`
with substrate's own message. A `NextToken` this listing did not issue is `InvalidNextTokenException`.

## A DynamoDB or ECS target's resource must exist

AWS checks the resource behind some namespaces when a target is registered. Substrate checks two,
through the owning plugin: a new DynamoDB target's table (and index) must exist, or the request is
`ValidationException` `DynamoDB resource doesn't exist: {ResourceId}`, and a new ECS target's service
must be active, or it is `ValidationException` `ECS service doesn't exist: {ResourceId}`. The ECS
message contains the phrase the Terraform AWS provider retries on; the rest of both messages is
substrate's own. Targets in other namespaces register whatever they name.

## A scalable target is reachable through the tagging API

The Resource Groups Tagging API's `GetResources`, `TagResources` and `UntagResources` reach scalable
targets, under the lock the plugin's own tag operations hold, so a tag written through either API is
read back by the other. A target whose tags were all removed is still reported, as a previously
tagged resource. Scaling policies and scheduled actions carry no tags, as the `TagResource` page
states. An ARN naming another account's or Region's target is reported failed and changes nothing.

## CloudFormation resource types

`AWS::ApplicationAutoScaling::ScalableTarget` registers the target and puts its `ScheduledActions`.
An update deletes the scheduled actions the previous declaration named and this one does not; an
action made outside the stack is left alone. A create whose scheduled actions fail deregisters the
target, so the rollback leaves nothing behind. `Ref` returns the ID CloudFormation generates,
`{ResourceId}|{ScalableDimension}|{ServiceNamespace}`.

`AWS::ApplicationAutoScaling::ScalingPolicy` names its target either by `ScalingTargetId` or by
`ResourceId`, `ScalableDimension` and `ServiceNamespace`, never both. `Ref` and `Fn::GetAtt Arn`
return the policy ARN.

Both underlying operations create or update. A stack's create refuses a target or policy that
already exists rather than adopting it, so deleting the stack cannot deregister another owner's
target (the reason is substrate's own). A change to a No interruption property updates the target or
policy in place under the same physical ID, and a change to a Replacement property
(`ResourceId`, `ScalableDimension`, `ServiceNamespace`, and a policy's `PolicyName` or
`ScalingTargetId`) replaces it. Deleting a stack deregisters the target, which deletes its policies.
The CDK's DynamoDB `autoScaleReadCapacity`/`autoScaleWriteCapacity` output deploys in strict mode.
