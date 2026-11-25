# FIS

**Endpoint:** `fis.{region}.amazonaws.com`
**Protocol:** REST/JSON (botocore `fis` 2020-12-01)

AWS Fault Injection Service: experiment templates, and experiments that run on the simulated clock
and change EC2 state as their actions come due ([#1619](https://github.com/scttfrdmn/substrate/issues/1619)).

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateExperimentTemplate | `description`, `roleArn`, `stopConditions` and `actions` are required. Each action's `actionId` must be one of the known actions below, its parameters that action's own (each required one present, each duration an ISO 8601 duration inside the published range), its target key the action's, and the target it names defined and of the action's resource type. `startAfter` must name other actions and must not be circular. A target needs a known `resourceType`, a `selectionMode` of `ALL`, `COUNT(n)` or `PERCENT(n)`, and `resourceArns` (at most 5) or `resourceTags`, not both, and not `resourceArns` with `filters`. A stop condition is `aws:cloudwatch:alarm` with an alarm ARN, or `none`. Every refusal is `ValidationException`/400. `clientToken` is idempotent: a repeat answers the template it created. `experimentOptions` is reported as sent; an experiment from a template naming none behaves as `single-account` and `fail`. A template may have no actions; its experiments complete at once. `logConfiguration` and `experimentReportConfiguration` are recorded and reported; no log or report is written |
| GetExperimentTemplate | An unknown ID is `ResourceNotFoundException`/404 |
| ListExperimentTemplates | By template ID. Pages on `nextToken`/`maxResults` (1 to 100, default 100) |
| UpdateExperimentTemplate | A member sent replaces the stored one; a member not sent is kept. The result is validated as a create is. `experimentOptions` takes `emptyTargetResolutionMode` only and replaces it (an omitted mode reverts to `fail`), and `tags` is refused (use TagResource) |
| DeleteExperimentTemplate | Answers the deleted template. Experiments started from it are kept |
| StartExperiment | Resolves the targets (see below) and answers `initiating`, "Experiment is initiating.", with each action `pending`, "Initial state.", as the CLI's sample does. `clientToken` is idempotent; reusing one for a different template is `ConflictException`/409. `experimentOptions.actionsMode` `skip-all` skips every action. An unknown template is `ResourceNotFoundException`/404 |
| GetExperiment | The experiment's state and each action's state, `startTime` and `endTime` on the simulated clock (see below). `targetAccountConfigurationsCount` is 0 |
| ListExperiments | By experiment ID; `experimentTemplateId` filters. Pages on `nextToken`/`maxResults` |
| StopExperiment | Answers `stopping`, "Stopping Experiment.". At the next observation the experiment is `stopped`, its running actions `stopped` and its pending ones `cancelled`. An experiment that has ended is `ValidationException`/400 |
| ListExperimentResolvedTargets | One entry per resolved resource, `targetInformation` carrying its ARN under `arn` (the key is substrate's; no page names one). `targetName` filters; an unknown one is `ResourceNotFoundException`/404 |
| ListActions | The known actions below, by ID |
| GetAction | A known action with its parameters; another ID is `ResourceNotFoundException`/404 |
| ListTargetResourceTypes | `aws:ec2:autoscaling-group`, `aws:ec2:instance`, `aws:ec2:spot-instance`, `aws:iam:role` |
| GetTargetResourceType | |
| TagResource | An experiment template's or an experiment's ARN in the caller's account and Region. At most 50 tags; an `aws:` key is refused. Anything else is `ResourceNotFoundException`/404 |
| UntagResource | `tagKeys` |
| ListTagsForResource | |

The messages are substrate's own; the API reference publishes none.

### Known actions

From the [AWS FIS actions reference](https://docs.aws.amazon.com/fis/latest/userguide/fis-actions-reference.html):

| Action | Target | What substrate does |
|--------|--------|---------------------|
| `aws:ec2:stop-instances` | `Instances` (`aws:ec2:instance`) | Stops the instances when the action starts. With `startInstancesAfterDuration` (1 minute to 12 hours) starts them again that long afterwards, when the action completes; a targeted instance terminated meanwhile fails the action unless `completeIfInstancesTerminated` is `true` |
| `aws:ec2:terminate-instances` | `Instances` | Terminates the instances when the action starts. Termination protection refuses it, failing the action, since FIS calls TerminateInstances on the caller's behalf |
| `aws:ec2:reboot-instances` | `Instances` | Completes at once; a reboot is not observable through DescribeInstances |
| `aws:ec2:send-spot-instance-interruptions` | `SpotInstances` (`aws:ec2:spot-instance`) | Interrupts the Spot Instances `durationBeforeInterruption` (2 to 15 minutes) after the action starts, when the action completes (see below) |
| `aws:fis:wait` | none | Completes after `duration` |
| `aws:fis:inject-api-internal-error`, `aws:fis:inject-api-throttle-error`, `aws:fis:inject-api-unavailable-error`, `aws:ec2:api-insufficient-instance-capacity-error` | `Roles` (`aws:iam:role`) | Recorded; complete after `duration` and inject nothing |
| `aws:ec2:asg-insufficient-instance-capacity-error` | `AutoScalingGroups` (`aws:ec2:autoscaling-group`) | Recorded; completes after `duration` and injects nothing |

## How an experiment runs

**On the simulated clock.** An experiment starts at the clock's reading when StartExperiment is
sent. At the next request after that, it fails if a target resolved to nothing, with the reason AWS
reports, "Target resolution returned empty set.", unless the template's `emptyTargetResolutionMode`
is `skip`, which skips the actions on that target instead. Otherwise it is `running`. An action
with no `startAfter` starts at the experiment's start time; one with `startAfter` starts when the
last of the actions it names ends. Each action ends after its duration, and the experiment
`completed`, "Experiment completed.", at the latest action's end. Every start and end time is
computed from the experiment's start time and the actions' durations, never from when a request
observed it, so polling GetExperiment more or less often reports the same times. Nothing happens
while the clock is frozen; `POST /v1/control/time` moves it (see the [Testing Guide](/testing-guide)).

**Deterministic target selection.** Targets are resolved once, at StartExperiment, as AWS resolves
them before any action starts. An EC2 target takes the account's and Region's instances its
`resourceArns` name, or those carrying every one of its `resourceTags`; each filter must match, and
any of a filter's values matches. A filter path is a DescribeInstances member in Pascal case:
`State.Name`, `State.Code`, `InstanceId`, `InstanceType`, `ImageId`, `SubnetId`, `VpcId`,
`KeyName`, `PrivateIpAddress`, `PublicIpAddress`, `PrivateDnsName`, `Architecture`,
`InstanceLifecycle`, `SpotInstanceRequestId`, `Placement.AvailabilityZone`, `Placement.GroupName`,
`SecurityGroups.GroupId` and `NetworkInterfaces.Groups.GroupId`; any other path matches nothing. An
`aws:ec2:spot-instance` target takes Spot Instances only — instances launched by RunInstances with
`InstanceMarketOptions.MarketType=spot`, by RequestSpotInstances, by a Spot fleet or by an Auto
Scaling group's Spot capacity. A `terminated` or `shutting-down` instance is not a candidate unless
a `State.Name` filter names its state: the Targets page says only that DescribeInstances "might"
report recently terminated instances, no EC2 action can act on one, and a `COUNT` or `PERCENT`
selection would otherwise pick dead instances first, so that default is substrate's reading. An
instance's state, for this rule and for a `State.Name` or `State.Code` filter, is the one
DescribeInstances would report now — `shutting-down` while a
[seeded](/reference/ec2#seeding-an-instance-state-progression) termination countdown runs — read
without spending an observation. A stop or terminate action passes over a resolved instance that
has ended by the time the action starts. The
candidates are sorted by instance ID, and `COUNT(n)` takes the
first n, `PERCENT(p)` the first p percent rounded down (as AWS rounds), `ALL` every one. AWS
chooses at random; substrate takes the first of the sorted list, so the same state selects the same
instances on every run and on replay. A target of another resource type resolves to its
`resourceArns`.

**The Spot interruption.** When `aws:ec2:send-spot-instance-interruptions` comes due, each
targeted Spot Instance whose request's interruption behavior is `terminate` (or that has no
request, as an Auto Scaling group's Spot capacity has none) is terminated with `stateReason`
`Server.SpotInstanceTermination` and the message API_StateReason publishes, and `reason`
(StateTransitionReason) `Service initiated (YYYY-MM-DD HH:MM:SS GMT)`. Its Spot request reports
status `instance-terminated-by-experiment`, the status the FIS Spot tutorial names, and is closed,
or for a persistent request re-fulfilled with a replacement, as for any terminated Spot Instance.
An instance whose interruption behavior is `stop` or `hibernate` is stopped instead, with
`Server.SpotInstanceShutdown`, and its request reports `instance-stopped-by-experiment`. Stop and
terminate actions record `Client.UserInitiatedShutdown` and `User initiated (…)`. A caller's own
StopInstances, StartInstances or TerminateInstances clears both reasons; terminating an instance
that is already terminated changes nothing, so its reasons, `TerminatedTime` and Spot request stay
as the interruption left them. These are EC2's own
transitions, so volumes, the Spot request and Auto Scaling membership settle as they do for a
caller's termination; no StopInstances or TerminateInstances request is recorded. The interruption
notice and rebalance recommendation, delivered through instance metadata and EventBridge, are not
modeled ([#1722](https://github.com/scttfrdmn/substrate/issues/1722)).

**Stop conditions.** A running experiment with an `aws:cloudwatch:alarm` stop condition reads the
alarm through CloudWatch on every request, and stops with "Experiment stopped by a stop
condition." when it is in `ALARM`. The alarm must be in the template's account and Region —
CreateExperimentTemplate and UpdateExperimentTemplate refuse any other ARN with
`ValidationException` — and is read there. An alarm that does not exist, or one CloudWatch refuses
with a client error, is not in `ALARM` and never stops the experiment (the refusal is logged). A read
substrate cannot make at all — no CloudWatch registered, a store failure — fails the experiment with
a reason naming the alarm. Both are substrate's readings. A stop
does not roll an action back: instances a stop-instances action stopped stay stopped.

**A seeded outcome.** `POST /v1/fis/experiment-result` with
`{"id": "EXT…", "status": "failed", "reason": "…"}` ends every experiment started from that template
at its next observation with that state; `id` may also be an experiment's ID, or `"*"` for every
experiment (the most specific seed applies), and `status` may be `completed`. A seeded failure
fails the running actions and cancels the pending ones; a seeded completion completes every action
that has not ended. Effects actions already had stay. `DELETE /v1/fis/experiment-result?id=…`
clears one seed, and without `id` clears them all. The seed is a control-plane write, so a replay
re-applies it in position.

**Replay.** The experiment's progress runs before each request, live and on replay, at the same
step and with the clock pinned to the recorded reading, so a replay of the recorded requests
re-derives every EC2 change the experiment made. See
[Seeds and replay](/reference/concepts/seeds-and-replay).

### Worked example: testing a Spot-interruption handler

```python
import boto3, requests

SUBSTRATE = "http://localhost:4566"
ec2 = boto3.client("ec2", endpoint_url=SUBSTRATE, region_name="us-east-1")
fis = boto3.client("fis", endpoint_url=SUBSTRATE, region_name="us-east-1")

def set_clock(at):  # freeze the clock at an instant; nothing moves until the test says so
    requests.post(f"{SUBSTRATE}/v1/control/time", json={"time": at, "frozen": True})

set_clock("2026-11-24T10:00:00Z")

# Two tagged Spot workers.
for _ in range(2):
    ec2.run_instances(
        ImageId="resolve:ssm:/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64",
        InstanceType="t3.micro", MinCount=1, MaxCount=1,
        InstanceMarketOptions={"MarketType": "spot",
                               "SpotOptions": {"SpotInstanceType": "one-time"}},
        TagSpecifications=[{"ResourceType": "instance",
                            "Tags": [{"Key": "Name", "Value": "interruptMe"}]}])

template = fis.create_experiment_template(
    description="Test Spot Instance interruptions",
    roleArn="arn:aws:iam::123456789012:role/AllowFISSpotInterruptionActions",
    stopConditions=[{"source": "none"}],
    targets={"oneSpotInstance": {
        "resourceType": "aws:ec2:spot-instance",
        "resourceTags": {"Name": "interruptMe"},
        "filters": [{"path": "State.Name", "values": ["running"]}],
        "selectionMode": "COUNT(1)"}},
    actions={"interruptSpotInstance": {
        "actionId": "aws:ec2:send-spot-instance-interruptions",
        "parameters": {"durationBeforeInterruption": "PT2M"},
        "targets": {"SpotInstances": "oneSpotInstance"}}},
)["experimentTemplate"]

experiment = fis.start_experiment(experimentTemplateId=template["id"])["experiment"]
victim = fis.list_experiment_resolved_targets(experimentId=experiment["id"])[
    "resolvedTargets"][0]["targetInformation"]["arn"].split("/")[-1]  # the lowest instance ID

set_clock("2026-11-24T10:01:59Z")
assert fis.get_experiment(id=experiment["id"])["experiment"]["state"]["status"] == "running"
set_clock("2026-11-24T10:02:00Z")  # durationBeforeInterruption has passed

state = fis.get_experiment(id=experiment["id"])["experiment"]["state"]["status"]
assert state == "completed"
instance = ec2.describe_instances(InstanceIds=[victim])["Reservations"][0]["Instances"][0]
assert instance["State"]["Name"] == "terminated"
assert instance["StateReason"]["Code"] == "Server.SpotInstanceTermination"
# ...and assert that your handler drained and replaced the worker.
```

To test the failure path instead, seed it before starting:

```bash
curl -X POST localhost:4566/v1/fis/experiment-result \
  -d '{"id":"*","status":"failed","reason":"Insufficient permissions."}'
```

## Terraform and CloudFormation

Terraform's `aws_fis_experiment_template` creates, reads, updates and deletes through the
operations above. CloudFormation's `AWS::FIS::ExperimentTemplate` deploys through
CreateExperimentTemplate (an undeclared `Actions` is sent as `{}`), updates every property but
`Tags` in place through UpdateExperimentTemplate — sending a removed optional property as empty, so
it is cleared — and `Tags` through TagResource and UntagResource. A change to
`ExperimentOptions.AccountTargeting`, whose "Update requires" is Replacement, creates a new
template and deletes the old one. It deletes through DeleteExperimentTemplate. `Ref` returns the template ID, and `Id` is its one attribute.

## Not emulated

Multi-account experiments (`accountTargeting` `multi-account` is recorded and targets the caller's
account; the target-account-configuration operations are not routed), safety levers, experiment
reports and logs, and target resolution of resource types other than EC2 instances by tag.

**The experiment role is not evaluated.** An experiment's EC2 actions run without authorizing the
template's `roleArn`, so a role missing `ec2:StopInstances` or `ec2:SendSpotInstanceInterruptions`
does not fail the action as it would in AWS. To test a consumer's handling of that denial, seed a
`failed` result (see above) with the reason your code expects
([#1744](https://github.com/scttfrdmn/substrate/issues/1744)).

## Cost

FIS experiments are not costed.
