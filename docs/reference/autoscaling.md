# EC2 Auto Scaling

**Endpoint:** `autoscaling.{region}.amazonaws.com`
**Protocol:** AWS Query (form-encoded, `Action=` parameter), namespace `http://autoscaling.amazonaws.com/doc/2011-01-01/`

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateAutoScalingGroup | `LaunchTemplate` or `MixedInstancesPolicy` (exactly one), `MinSize`/`MaxSize`/`DesiredCapacity` (default `MinSize`) in the units `DesiredCapacityType` names (see below), `VPCZoneIdentifier` or `AvailabilityZones`, `Tags.member.N` with `PropagateAtLaunch`, `CapacityRebalance`, `NewInstancesProtectedFromScaleIn`, `TerminationPolicies`. Launches the desired capacity before answering. Refuses `AlreadyExists`/400, and `ValidationError`/400 for contradictory sizes, a missing or unknown launch template or version, an unknown subnet, a reserved `aws:` tag key and a launch configuration. More than 50 tags is `LimitExceeded`/400. With IAM enforced, the caller's `ec2:RunInstances` is checked first (see below) |
| DescribeAutoScalingGroups | `AutoScalingGroupNames.member.N`, `Filters` `tag-key`/`tag-value`/`tag:<key>` (another name is `ValidationError`), `IncludeInstances`. Pages on `NextToken`/`MaxRecords` (default 50, max 100). An observation: reconciles each group it returns first |
| UpdateAutoScalingGroup | Any subset of Create's members; a member not sent is unchanged, inside `MixedInstancesPolicy` too (overrides are replaced as a list when any is sent, distribution members one by one, an empty `SpotMaxPrice` clears it, a `LaunchTemplateSpecification` carrying only `Version` re-points the existing template). A new `MinSize` above, or `MaxSize` below, the current desired capacity moves it there when no `DesiredCapacity` is named, as the reference states. Reconciles before answering |
| DeleteAutoScalingGroup | `ResourceInUse`/400 while instances remain unless `ForceDelete`, which terminates them through EC2. A `ForceDelete` whose termination fails fails the request and leaves the group, still owning its instances |
| SetDesiredCapacity | Outside `MinSize`–`MaxSize` is `ValidationError`/400. `HonorCooldown=true` inside `DefaultCooldown` of the group's last activity is `ScalingActivityInProgress`/400. Scales out or in before answering |
| TerminateInstanceInAutoScalingGroup | `InstanceId`, or `InstanceIds.member.N` with `AutoScalingGroupName` (answering `Activities`). An `InstanceId` belongs to whichever group owns it; an `AutoScalingGroupName` naming a different group is `ValidationError`/400, as is a repeated ID. Every instance is validated before any is terminated, and the batch terminates whole. `ShouldDecrementDesiredCapacity=false` launches the replacement before answering; `true` lowers desired by the instance's weight, held at `MinSize` and never below (#1526). When every instance terminated weighs one, desired less the full decrement below `MinSize` is `ValidationError`/400, as before; a weighted decrement is refused only when desired already equals `MinSize` (both observed wording). AWS was observed clamping a weighted decrement at a `MinSize` of 0 and terminating rather than refusing; that it clamps the same way above 0 is substrate's reading. The answered activity is `InProgress`, as the published sample shows; the recorded one is `Successful` |
| SuspendProcesses | The nine published processes, all of them when `ScalingProcesses` is omitted. `Launch`, `Terminate`, `HealthCheck` and `ReplaceUnhealthy` change what a reconciliation does; the others are recorded and echoed |
| ResumeProcesses | |
| DescribeScalingActivities | Most recent first; `AutoScalingGroupName`, `ActivityIds`, `IncludeDeletedGroups` (which adds `AutoScalingGroupState`), `Filters` `Status`/`StartTimeLowerBound`/`StartTimeUpperBound`. Pages on `NextToken`/`MaxRecords` (default and max 100). An observation |
| SetInstanceProtection | An instance outside the group is `ValidationError`/400; more than 50 is `LimitExceeded`/400 |
| CreateOrUpdateTags | Overwrites an existing key. A group that does not exist is `ValidationError`/400; a group past 50 tags is `LimitExceeded`/400 and the request writes nothing. Existing instances are not retagged |
| DeleteTags | |
| DescribeTags | `Filters` `auto-scaling-group`/`key`/`value`/`propagate-at-launch`. Pages on `NextToken`/`MaxRecords` (default 50, max 100) |
| DescribeAutoScalingInstances | `InstanceIds.member.N` (unknown IDs ignored, as published). Pages on `NextToken`/`MaxRecords` (default and max 50). `HealthStatus` is `HEALTHY`, the spelling this operation's published sample uses. An observation |
| EnableMetricsCollection | `Granularity` is required and must be `1Minute`, the only published value. `Metrics.member.N` names group metrics, and all twenty are enabled when it is omitted. An unknown metric or granularity is `ValidationError`/400 with substrate's own message. The enabled set is reported as `DescribeAutoScalingGroups`' `EnabledMetrics`, in the published order. No metric is published to CloudWatch ([#1594](https://github.com/scttfrdmn/substrate/issues/1594)) |
| DisableMetricsCollection | Disables the named metrics, or all of them when `Metrics` is omitted |
| DescribeMetricCollectionTypes | The published catalog: granularity `1Minute` and the twenty group metrics |

**A group's instances are EC2's own.** Every launch runs EC2's RunInstances launch path and every
termination EC2's termination path, so `DescribeInstances` reports a group's instances like
any others: launched from the template version the group resolves (`$Default` unless named),
tagged with the reserved `aws:autoscaling:groupName`, the template's instance tags and the group's
`PropagateAtLaunch` tags (the group's value winning on a shared key, as the tagging guide states),
and moving `pending → running` through EC2's own [seeded progression](/reference/concepts/seeds-and-replay#how-a-progression-is-seeded).
A member reports `LifecycleState` `InService`; the group adds no clock of its own
([#1460](https://github.com/scttfrdmn/substrate/issues/1460)).

**Reconciliation is synchronous, and also happens on observation.** A request that changes
capacity — Create, Update, SetDesiredCapacity, TerminateInstanceInAutoScalingGroup — launches or
terminates before it answers. The three describes are observations: each first checks every member
against EC2's instance record, so an instance terminated directly through EC2 (a worker scaling
itself in) is removed with a scaling activity and replaced, unless the desired capacity was lowered
first. EC2's record is read rather than described, so a group's check does not spend a test's seeded
`pending` observations. Both moments are requests, so a recorded session replays identically.

**Mixed instances.** Overrides are taken in request order. An `InstanceRequirements` override
matches EC2's seeded instance-type catalog on `VCpuCount`, `MemoryMiB`, `MemoryGiBPerVCpu`,
`AcceleratorCount`, `AcceleratorTypes`, `CpuManufacturers`, `BurstablePerformance`, `BareMetal`,
`LocalStorage`, `InstanceGenerations`, `AllowedInstanceTypes` and `ExcludedInstanceTypes`, with the
published defaults; the price-protection thresholds are echoed and not evaluated, and the bandwidth,
interface-count, accelerator-name and hibernation members are accepted and ignored, the catalog
holding nothing to evaluate them against. Substrate models no capacity pools or prices, so every
allocation strategy launches the **first** candidate: the first override's type, or for
requirements the smallest match (vCPU, then memory, then name). Requirements narrow by the AMI's
architecture, a `resolve:ssm:` AMI's resolved one included, and a group selecting by requirements from
a template whose `ImageId` is `resolve:ssm:` is refused at create or update — see [An ImageId may name a
Systems Manager parameter](/reference/ec2#an-imageid-may-name-a-systems-manager-parameter). An override type the AMI
cannot run fails the launch with EC2's [architecture refusal](/reference/ec2#an-ami-the-instance-type-cannot-run-is-refused).
On-Demand and Spot are split as the
setup overview publishes — base capacity first, then the percentage above it rounded up in favor of
On-Demand — and a Spot launch is a launch with `InstanceMarketOptions.MarketType=spot`, which EC2
reports as `instanceLifecycle` `spot`. As for a CreateFleet Spot pool, no `sir-` Spot Instance
request is written: the group is the record that tracks the capacity.

**Capacity units and weights.** `MinSize`, `MaxSize`, `DesiredCapacity` and `OnDemandBaseCapacity`
are in the group's units, and each instance contributes its weight
([#1502](https://github.com/scttfrdmn/substrate/issues/1502)). With `DesiredCapacityType` `vcpu` or
`memory-mib`, the weight is the instance type's vCPUs or MiB from EC2's catalog. Both need an
`InstanceRequirements` override, since API_CreateAutoScalingGroup supports `DesiredCapacityType`
"for attribute-based instance type selection only"; without one, Create refuses with
`ValidationError`/400 (AWS's observed wording), as does an Update that sends `DesiredCapacityType`
or changes the launch configuration. A group recorded before this rule with `vcpu` or `memory-mib`
and no `InstanceRequirements` override keeps counting instances. With `units`, an override's
`WeightedCapacity` is the weight of every instance launched from it: an `InstanceRequirements`
override's weight covers each type it matches, and an override naming no type covers the template's.
It must be an integer from 1 to 999 and present on every override or on none, else
`ValidationError`/400. A spelling such as `+4` or `007` is accepted as the live service accepts it,
and stored as `4` or `7`. Without weights, every instance counts as one. A scale-out launches until the
summed weight reaches desired, and the last launch may overshoot: weights "do not change which instance
types Amazon EC2 Auto Scaling chooses to launch", so the first candidate still launches. Launching stops
at desired, which is at most `MaxSize`, so capacity stays below `MaxSize` plus one instance's weight.
The On-Demand/Spot split and zone balance are computed in units, for the size the launch or
termination leaves the group at. A scale-in terminates only an instance whose weight fits in the
excess over desired, so the group never drops below it (AWS publishes no finer rule). A decrementing
`TerminateInstanceInAutoScalingGroup` lowers desired by the instance's weight. When every instance it
terminates weighs one, it is refused, as before, when desired less the full decrement would fall below
`MinSize`. A weighted decrement is held at `MinSize` instead, and refused only when desired already
equals `MinSize` (#1526).
Changing an override's weight reconciles to desired, and an instance whose type no override names any
more keeps its last weight, as the instance-weighting guide states. A weighted group's members report
`WeightedCapacity` (a string) in `DescribeAutoScalingGroups` and `DescribeAutoScalingInstances`; the
published shapes constrain it to 1–999, and a `memory-mib` group reports the MiB anyway, since no page
says what AWS reports instead.

**Requirements select by the AMI's architecture.** API_InstanceRequirements' `CpuManufacturers` note
says "Instances will be launched with a compatible CPU architecture based on the Amazon Machine Image
(AMI) that you specify in your launch template", so an `InstanceRequirements` override first resolves
its AMI — the override's `ImageId`, or the template version's — and considers only the catalog types
that report its architecture: an `arm64` AMI selects only the AWS Graviton families, an `x86_64` AMI
only the Intel and AMD ones ([#1497](https://github.com/scttfrdmn/substrate/issues/1497)). So
`CpuManufacturers=["amazon-web-services"]` launches Graviton types from an arm64 AMI, `t4g` counting
as burstable, and `intel` on an arm64 AMI matches nothing and records the Failed activity AWS would.
Any other architecture narrows as `x86_64`: an AMI that does not resolve (the launch then fails on
the AMI itself), and one reporting an architecture no catalog type does — `i386`, which an AMI
registered without `Architecture` reports, or the `mac` values. So Graviton types are chosen only for
an AMI that says `arm64`, and a group that chose an x86 type before the Graviton families existed
chooses the same one now, rather than a 1-vCPU Graviton `.medium`. The manufacturer is the catalog
family's processor as the guide pages publish it: `intel`, `amd`, or `amazon-web-services`.

**A zone-only group uses the default VPC.** A group naming `AvailabilityZones` and no
`VPCZoneIdentifier` launches "into the default VPC subnet in each Availability Zone", as
API_CreateAutoScalingGroup states, through EC2's own default-subnet resolution; an Auto Scaling
request seeds the account's default VPC first, as an EC2 request does
([#1464](https://github.com/scttfrdmn/substrate/issues/1464)).

**A group's terminations ignore EC2 termination protection.** The EC2 user guide: "Enabling
termination protection does not prevent Amazon EC2 Auto Scaling from terminating an instance when
the instance is unhealthy or during scale-in events." Substrate applies that to every termination a
group makes — scale-in, health replacement, `TerminateInstanceInAutoScalingGroup` and `ForceDelete`
— since all run as the group's service-linked role; scale-in protection (`SetInstanceProtection`)
is what stops a scale-in.

**Template tags are held to EC2's rules.** A launch's tags are the template version's instance
tags, then the group's `PropagateAtLaunch` tags, then `aws:autoscaling:groupName`. The merged set is
checked as EC2 checks a template's tags — no reserved key, at most 50 counting everything but the
group-name tag — and a launch that fails it is a `Failed` activity carrying EC2's message.

**Scale-in follows the default termination policy as far as it is observable.** Protected
instances are never chosen. A mixed group first picks the purchase option that keeps it nearest its
ratio; then the zone with the most instances; then instances from a template other than the current
one, then the oldest version of the current one, then the instance closest to its next billing hour;
where AWS would choose at random, the lowest instance ID. `OldestInstance`, `NewestInstance`,
`OldestLaunchTemplate` and `ClosestToNextInstanceHour` apply in the order listed;
`AllocationStrategy`, `OldestLaunchConfiguration` and a Lambda ARN narrow nothing.

**A launch failure is seeded.** `POST /v1/autoscaling/launch-failure` with
`{"autoScalingGroupName":"pool","statusMessage":"…"}` (the name defaulting to `"*"`, the message to
EC2's insufficient-capacity text) makes every launch attempt for the group fail with that
`StatusMessage` and launch nothing, leaving the capacity unmet; `DELETE` (with
`?autoScalingGroupName=` or without, for all) clears it, and the next attempt succeeds. Every
observation retries, but one that fails exactly as the last recorded failure did — same desired
capacity, same message — records nothing new: AWS publishes no retry cadence, so the rule is one
`Failed` activity per desired-capacity change, and a poll loop sees one failure rather than one per
poll. The seed is a control-plane write, so a replay re-applies it in position. A launch EC2 itself
refuses — an AMI that does not exist, say — is reported the same way. An observation of a group with
nothing to reconcile writes no state.

**Authorization.** A mutation is decided against the group's ARN
(`arn:aws:autoscaling:{region}:{account}:autoScalingGroup:{uuid}:autoScalingGroupName/{name}`; a
create's, with the UUID as the literal `*` the user guide prescribes), and a describe against `*`,
as the Service Authorization Reference (vendored as `emulator/authzref/autoscaling.json`) publishes.
The group's tags are reported as both `autoscaling:ResourceTag/<key>` and `aws:ResourceTag/<key>`;
`aws:RequestTag/<key>` and `aws:TagKeys` are populated for CreateAutoScalingGroup, CreateOrUpdateTags
and DeleteTags. A tagging request naming several groups is decided once per group, each with only
the tags aimed at it, and one denial denies the request. TerminateInstanceInAutoScalingGroup by
`InstanceId` is decided against the group that owns the instance. Create and Update also check the caller's `ec2:RunInstances` against the request the
group would launch — the user guide's "RunInstances dry run" — before writing anything, and a
denial is Auto Scaling's own `AccessDenied`/403
`You are not authorized to use launch template: <template>` (no page publishes it; it is the response SDK users report verbatim); later launches
run as the group's service-linked role and are not checked against the caller.

**Not emulated.** Launch configurations (refused with `ValidationError`), scaling policies, lifecycle
hooks, warm pools, instance refresh, load-balancer and target-group registration (the names are
echoed), Capacity Rebalancing (echoed; no rebalance recommendation is ever sent),
`MaxInstanceLifetime` (echoed, not enforced), and the CloudFormation
`AWS::AutoScaling::AutoScalingGroup` resource type.

## Cost

Auto Scaling itself is free; the EC2 instances a group launches are costed as EC2 instances.
