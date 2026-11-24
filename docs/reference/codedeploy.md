# CodeDeploy

**Endpoint:** `codedeploy.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: CodeDeploy_20141006.{Op}`), API version 2014-10-06

Nine of the forty-eight published operations, over three resources: an application, its deployment
groups, and a deployment. Every record is keyed by account and Region, so two accounts, or one account
in two Regions, never see each other's applications. Nothing is ever deployed. The revision, the tag
filters, the traffic-shifting configuration and the alarms a request names are **recorded as sent and
answered back**, but no instance is targeted and no hook is installed, and a deployment is
[`Succeeded` before `CreateDeployment` returns](#a-deployment-s-status-progresses-under-a-seed).
Each of the three records is projected onto its published shape before it is answered, so no response
carries a field of Substrate's own and [every date is epoch
seconds](#every-published-date-is-epoch-seconds) rather than an RFC3339 string.

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateApplication | `applicationName` must satisfy its published length and pattern, and `computePlatform` its Valid Values; an absent platform is `Server` ([what each member requires](#what-each-codedeploy-request-requires)). `tags` are not read. Answers the published `applicationId` |
| GetApplication | Five of the six published `ApplicationInfo` members, `linkedToGitHub` always `false` ([which members](#what-each-codedeploy-record-answers)). `createTime` is [epoch seconds](#every-published-date-is-epoch-seconds) |
| DeleteApplication | Answers the published empty 200 body, takes the application's deployment groups with it, and [succeeds for an application that does not exist](#a-delete-of-something-absent-succeeds) |
| ListApplications | Names only, [paged by `nextToken`](#listapplications-pages-by-nexttoken) |
| CreateDeploymentGroup | `serviceRoleArn` is `Required: Yes` and must be a role ARN; the thirteen members `DeploymentGroupInfo` answers back are recorded as sent, and two published tag-form combinations are refused ([what each member requires](#what-each-codedeploy-request-requires)) |
| GetDeploymentGroup | All twenty-three published `deploymentGroupInfo` members once they have values; `lastAttemptedDeployment`, `lastSuccessfulDeployment` and `targetRevision` follow the group's deployments ([which members](#what-each-codedeploy-record-answers)) |
| DeleteDeploymentGroup | Answers the published `hooksNotCleanedUp` as an empty array, and [succeeds for a group that does not exist](#a-delete-of-something-absent-succeeds) |
| CreateDeployment | Verifies the application, and the deployment group when one is named. The request members `DeploymentInfo` answers back are recorded; `fileExistsBehavior` and `deploymentMode` are checked against their Valid Values. Answers the published `deploymentId` in the `d-XXXXXXXXX` shape AWS's own sample response shows — the page publishes no pattern for it — derived from the request ID (#856) |
| GetDeployment | Twenty-one of the thirty-one published `deploymentInfo` members ([which members](#what-each-codedeploy-record-answers)); an absent `deploymentId` is `DeploymentIdRequiredException`. `status` [progresses under a seed](#a-deployment-s-status-progresses-under-a-seed) |

The thirty-nine unrouted operations include everything that would let a consumer observe a deployment
in progress or intervene in one: `ListDeployments`, `StopDeployment`, `ContinueDeployment`,
`GetDeploymentTarget`, `ListDeploymentTargets`, `GetDeploymentInstance`,
`PutLifecycleEventHookExecutionStatus` and `SkipWaitTimeForInstanceTermination`. Also unrouted are
`UpdateApplication` and `UpdateDeploymentGroup` — nothing created here can be modified — the five
`BatchGet*` reads, `ListDeploymentGroups`, the revision surface
(`RegisterApplicationRevision`, `GetApplicationRevision`, `ListApplicationRevisions`), the whole
deployment-configuration surface (`CreateDeploymentConfig`, `GetDeploymentConfig`,
`ListDeploymentConfigs`, `DeleteDeploymentConfig`), the on-premises-instance surface, the GitHub-token
operations and the three tag operations. Each answers `UnknownOperationException` / 404.

## The account and Region a record carries reach no response

Each of the three persisted records — `CodeDeployApp`, `CodeDeployGroup` and `CodeDeployDeployment` —
declares `AccountID` and `Region` under wire-visible `json` tags, because the record is what
`MemoryStateManager` snapshots and a replay reads back. The tags are the lowercase-initial
`accountID`/`region` rather than the capitalized spelling the other services in this family use,
CodeDeploy's own members being lowerCamelCase; only the `ID` gives them away. No CodeDeploy shape
publishes either.

CodeDeploy is the one service in this family where all three records genuinely reached a body.
`GetApplication` answered the stored `CodeDeployApp` whole under `application`, `GetDeploymentGroup`
the `CodeDeployGroup` under `deploymentGroupInfo`, and `GetDeployment` the `CodeDeployDeployment`
under `deploymentInfo`, so each of the six declarations was a live leak until
[#1327](https://github.com/scttfrdmn/substrate/pull/1327) introduced `codedeployAppOut`,
`codedeployGroupOut` and `codedeployDeploymentOut` (`emulator/codedeploy_wire.go`). The other six
routed operations build a map of one or two published members and render no record at all.

All **nine** routed operations are driven by
`TestCodeDeployWire_ApplicationResponsesCarryNoBookkeepingMember` and its two siblings in
`emulator/codedeploy_wire_test.go`, which walk the decoded document and fail on a member named for
either at any depth — so the six entries this discharges in
`scripts/wire-bookkeeping-baseline.txt` stay listed as declarations rather than as leaks
([#756](https://github.com/scttfrdmn/substrate/issues/756)). The comparison is a case-insensitive
equality on the member *name*: a fold covers both the record's spelling and the `AccountID` a
projection written in the house style of the other services might introduce, and an equality rather
than a substring test means the account appearing inside a `serviceRoleArn` value is not a collision.
Each case asserts a published member is *present* before asserting a bookkeeping one is absent, so a
response that failed to render the record at all fails as a missing anchor rather than passing as an
absence, and the stored record is read back as raw JSON to prove it held both members in the first
place.

Neither member carries `,omitempty` on any of the three records and no CodeDeploy record declares
`EverTagged` — the three tag operations are unrouted — so the vacuous-absence trap that governs the
taggable services (#1304, #938) does not arise here. The six operations that never rendered a record
are driven anyway, because the set of nine should read as complete rather than as a sample, and a
control confirms the walk catches a member added to any of their hand-built maps.

## Every published date is epoch seconds

CodeDeploy speaks `awsJson1_1`, where a `Timestamp` is published as epoch seconds with fractional
precision rather than an RFC3339 string. `API_ApplicationInfo` types `createTime` as `Timestamp`,
`API_GetApplication` renders it `"createTime": number` and its own sample response carries
`"createTime": 1446229001.211`; `API_GetDeployment` renders `"createTime": number` and
`"completeTime": number` and samples them as `1446232639.487` and `1446232681.319`.

`CodeDeployApp.CreateTime` and `CodeDeployDeployment.CreateTime`/`CompleteTime` stay Go `time.Time`
in the persisted record and are converted where the response is built, by `codedeployAppOut` and
`codedeployDeploymentOut` in `emulator/codedeploy_wire.go`. Converting on projection rather than
retyping the field is what leaves every recorded run byte-identical, since `MemoryStateManager`
snapshots the record and a replay reads it back.

`GetDeploymentGroup` has no top-level date — `API_GetDeploymentGroup` publishes none — and its two
nested ones, `createTime` and `endTime` inside `lastAttemptedDeployment` and
`lastSuccessfulDeployment`, are epoch seconds too. `GetDeployment`'s `startTime` is the third
deployment date, and is the same instant as `createTime` (see below).

Until [#1207](https://github.com/scttfrdmn/substrate/issues/1207) both dates were RFC3339 strings,
which was the service's hardest divergence to work around: an `awsJson1_1` timestamp deserializer
expects a number, so `GetApplication` and `GetDeployment` did not return a wrong value but failed to
decode at all, inside the SDK, before any assertion of the caller's own ran. A hand-rolled client
reading raw JSON was unaffected, which is why it survived — Substrate's own tests read raw JSON.
`emulator/codedeploy_dates_test.go` now asserts on the raw bytes that each member is a bare number and
that no response anywhere renders an RFC3339 date.

`GetDeployment` answers `startTime` and `completeTime` equal to `createTime`, because all three are
written from one clock read. That is a separate defect and belongs to
[#1196](https://github.com/scttfrdmn/substrate/issues/1196): the fractional precision is preserved, so
two genuinely distinct instants would be orderable.

## A deployment's status progresses under a seed

`CreateDeployment` records the deployment `Succeeded`, because running a deployment is
workload-internal and out of scope, and it answers only `deploymentId`, so there is no status on
create to report. What a consumer's wait loop observes is `GetDeployment`'s `status`, and that can be
seeded ([How a progression is seeded](/reference/concepts/seeds-and-replay#how-a-progression-is-seeded)):

```
POST   /v1/codedeploy/deployment-status
       {"deploymentId":"d-…"|"*","pendingObservations":2,"state":"InProgress","finalState":"Failed",
        "errorCode":"HEALTH_CONSTRAINTS","errorMessage":"…"}
DELETE /v1/codedeploy/deployment-status            (all; ?deploymentId=… for one)
```

For `pendingObservations` observations `GetDeployment` reports `state` (default `InProgress`) and
answers no `completeTime`, "when the deployment was complete". It then reports `finalState` (default
the record's `Succeeded`). Any of `DeploymentInfo`'s eight published statuses is seedable: `Created`,
`Queued`, `InProgress`, `Baking` and `Ready` as the transient state, and `Succeeded`, `Failed` and
`Stopped` as the final one. A `Failed` or `Stopped` final status may carry `errorInformation` — the
code held to `API_ErrorInformation`'s Valid Values — which is how a consumer's failure and rollback
handling is exercised. Unseeded, a deployment reads `Succeeded` from the first `GetDeployment`, as
before. `deploymentOverview` is not emitted, because it counts the targets a deployment ran on and
Substrate runs on none: assert on `status` instead
([#1196](https://github.com/scttfrdmn/substrate/issues/1196)).

The deployment group's `lastAttemptedDeployment` and `lastSuccessfulDeployment` reflect the seeded
status too ([#1400](https://github.com/scttfrdmn/substrate/issues/1400)). They used to keep the
record's `Succeeded`, so a group reported a deployment seeded to fail as its last successful one.

- **Same status as `GetDeployment`.** `GetDeploymentGroup` reports each deployment with the status
  the next `GetDeployment` of it would answer. It answers no `endTime` while that status is not
  terminal; the page glosses `endTime` as "when the most recent deployment … was complete".
- **Reading spends nothing.** `GetDeploymentGroup` peeks rather than observes, so it does not advance
  a countdown a `GetDeployment` loop is counting on.
- **Which deployment each member names.** `lastAttemptedDeployment` is the group's newest deployment.
  `lastSuccessfulDeployment` is the newest that reports `Succeeded`, matching `DeploymentGroupInfo`'s
  "the most recent successful deployment". A later failure therefore leaves an earlier success in
  place, and a group with no successful deployment answers no `lastSuccessfulDeployment`.
- **Older groups.** A group records its deployments from #1400 on. A group recorded earlier has no
  such history and answers the two references it stored, unchanged.
- **`targetRevision` is unchanged.** It is still the last created deployment's revision, whatever that
  deployment reports.

## What each CodeDeploy request requires

CodeDeploy publishes a separate refusal for each required member rather than one generic code, and
each operation's page lists exactly the ones that apply to it. Every check runs before any state is
read, so a malformed request is refused for what it is rather than for a resource lookup it never
reached ([#1197](https://github.com/scttfrdmn/substrate/issues/1197),
[#1198](https://github.com/scttfrdmn/substrate/issues/1198)).

| Member | Absent | Malformed |
|--------|--------|-----------|
| `applicationName` (every operation that takes it) | `ApplicationNameRequiredException` | `InvalidApplicationNameException`: outside 1–100 characters or the published pattern `[A-Za-z0-9+=,.@_-]*` |
| `deploymentGroupName` (the group operations; `CreateDeployment` when named) | `DeploymentGroupNameRequiredException` | `InvalidDeploymentGroupNameException`, the same constraints |
| `serviceRoleArn` (`CreateDeploymentGroup`) | `RoleRequiredException` | `InvalidRoleException`: not `arn:<partition>:iam::<account>:role/<name>` — the page gives no format, so that is Substrate's reading of "an IAM role ARN" |
| `computePlatform` (`CreateApplication`) | `Server` | `InvalidComputePlatformException`: outside `Server \| Lambda \| ECS \| Kubernetes` |
| `deploymentConfigName` | the group's, else `CodeDeployDefault.OneAtATime` for a Server application | `InvalidDeploymentConfigNameException`, the name constraints |
| `deploymentId` (`GetDeployment`) | `DeploymentIdRequiredException` | — |

`CreateDeploymentGroup` also refuses `ec2TagFilters` with `ec2TagSet`
(`InvalidEC2TagCombinationException`), `onPremisesInstanceTagFilters` with `onPremisesTagSet`
(`InvalidOnPremisesTagCombinationException`), and an `outdatedInstancesStrategy` outside `UPDATE |
IGNORE` (`InvalidInputException`, the page's own code for input in an invalid format; nothing narrower
is published). `CreateDeployment` refuses a `fileExistsBehavior` outside `DISALLOW | OVERWRITE |
RETAIN` (`InvalidFileExistsBehaviorException`) and a `deploymentMode` outside `STANDARD | RESTART`
(`InvalidInputException`).

`InvalidInputException` is no longer answered for an absent member anywhere. It is published on only
`CreateDeploymentGroup` and `CreateDeployment`, and is not on CodeDeploy's Common Errors page, so a
caller catching the published per-member exception caught nothing.

Two defaults are deliberate. `OneAtATime` is applied only to a Server application: both pages name it
as the default without qualifying the platform, but it is an EC2/on-premises configuration, and a
Lambda or ECS group's default is a predefined configuration the API Reference does not name, so for
those platforms an unnamed configuration stays unnamed. And `InvalidDeploymentIdException` has no site:
the page gives `deploymentId` no pattern, and the `d-` shape Substrate mints is observed rather than
published, so no ID is malformed by any rule the page states.

## A delete of something absent succeeds

`API_DeleteApplication` publishes `ApplicationNameRequiredException`, `InvalidApplicationNameException`
and `InvalidRoleException`, and on success *"an HTTP 200 response with an empty HTTP body"*.
`API_DeleteDeploymentGroup` publishes the two name-required codes, the two invalid-name codes and
`InvalidRoleException`. Neither publishes a not-found code, so deleting an application or a group that
does not exist succeeds, with the published empty body or `{"hooksNotCleanedUp":[]}`. Until
[#1198](https://github.com/scttfrdmn/substrate/issues/1198) both raised the `*DoesNotExist*` code of
another page, so a teardown that runs twice failed here where it succeeds against AWS.

Deleting an application deletes its deployment groups too, since a group is addressed only through its
application; re-creating the application does not resurrect them.

## What each CodeDeploy record answers

Each record is projected onto its published shape in `emulator/codedeploy_wire.go`
([#1199](https://github.com/scttfrdmn/substrate/issues/1199)).

- **`application`** — five of `ApplicationInfo`'s six: `applicationId`, `applicationName`,
  `computePlatform`, `createTime`, and `linkedToGitHub`, always `false`. No GitHub connection is
  modeled, and AWS's own sample answers `false` for an unlinked application rather than omitting the
  member. `gitHubAccountName` is absent, because there is no connection to name.
- **`deploymentGroupInfo`** — all twenty-three of `DeploymentGroupInfo`'s members once each has a value.
  `applicationName`, `deploymentGroupId`, `deploymentGroupName`, `serviceRoleArn`, `computePlatform`
  (the application's), `deploymentConfigName` and `autoScalingGroups` are always answered.
  `autoScalingGroups` answers `{"name": …}` objects for the names sent, with no `hook`, because none is
  installed. The thirteen request members the shape answers back unchanged are recorded as sent and
  appear when sent: `alarmConfiguration`, `autoRollbackConfiguration`,
  `blueGreenDeploymentConfiguration`, `deploymentStyle`, `ec2TagFilters`, `ec2TagSet`, `ecsServices`,
  `loadBalancerInfo`, `onPremisesInstanceTagFilters`, `onPremisesTagSet`, `outdatedInstancesStrategy`,
  `terminationHookEnabled` and `triggerConfigurations`. `lastAttemptedDeployment`,
  `lastSuccessfulDeployment` (each `LastDeploymentInfo`, dates in epoch seconds, `endTime` only once
  terminal) and `targetRevision` appear once a deployment has run in the group. They follow
  [the seeded status](#a-deployment-s-status-progresses-under-a-seed).
- **`deploymentInfo`** — twenty-one of `DeploymentInfo`'s thirty-one: `applicationName`,
  `deploymentGroupName`, `deploymentId`, `status`, `createTime`, `startTime`, `completeTime`,
  `creator` (`user`), `computePlatform`, `deploymentConfigName`, and, when sent, the request's
  `revision`, `description`, `ignoreApplicationStopFailures`, `fileExistsBehavior`,
  `updateOutdatedInstancesOnly`, `autoRollbackConfiguration`, `overrideAlarmConfiguration` and
  `targetInstances`, plus the group's `deploymentStyle` and `loadBalancerInfo`. `deploymentMode` is
  answered only for `RESTART`: the page says it is absent for `STANDARD` deployments and must not be
  read as `STANDARD` when absent. The ten absent members are `deploymentOverview`,
  `deploymentStatusMessages` and `instanceTerminationWaitTimeStarted`, which describe targets
  Substrate does not run on; `errorInformation` and `rollbackInfo`, which describe failures that do not
  happen; `previousRevision`, `relatedDeployments`, `blueGreenDeploymentConfiguration` and
  `externalId`, which are not modeled; and the deprecated `additionalDeploymentStatusInfo`.

A deployment or group stored before these members existed decodes unchanged and simply answers fewer of
them.

## ListApplications pages by nextToken

`nextToken` is the only member `API_ListApplications` publishes on either side, and
`InvalidNextTokenException`/400 is its only published error
([#1195](https://github.com/scttfrdmn/substrate/issues/1195)). A page is 100 names in creation order.
The page states no size, so 100 is Substrate's reading: large enough that an ordinary account fits on
one page, small enough that a test can force a second. The token is the next page's offset, issued only
when a further name exists, so the last page **omits** `nextToken` rather than answering it empty. A
token Substrate did not issue is refused with `InvalidNextTokenException` rather than read as page
one. The operation publishes no filter, so there is none to apply.

## What a refusal reports

| Condition | Code | Status |
|-----------|------|--------|
| a body that will not parse | `ValidationError` | 400 |
| a required member absent or malformed | [the member's own code](#what-each-codedeploy-request-requires) | 400 |
| an application that does not exist (get, deployment-group create and get, deployment create) | `ApplicationDoesNotExistException` | 400 |
| an application name already in use | `ApplicationAlreadyExistsException` | 400 |
| a deployment group that does not exist (get, deployment create) | `DeploymentGroupDoesNotExistException` | 400 |
| a deployment group name already in use | `DeploymentGroupAlreadyExistsException` | 400 |
| a deployment that does not exist | `DeploymentDoesNotExistException` | 400 |
| a `nextToken` Substrate did not issue | `InvalidNextTokenException` | 400 |
| an unrecognised `X-Amz-Target` suffix | `UnknownOperationException` | 404 |

`ValidationError`/400 is the spelling CodeDeploy's consolidated common-errors list publishes, and it
is the only code that covers all eight body-decode sites; the reasoning is recorded on
`codedeployInvalidBody`. A delete of something absent is not a refusal: see
[above](#a-delete-of-something-absent-succeeds).

Beyond the codes named here and in [what each request
requires](#what-each-codedeploy-request-requires), CodeDeploy publishes and Substrate never answers:
`InvalidDeploymentIdException`, `InvalidTagsToAddException`, `RevisionRequiredException`,
`RevisionDoesNotExistException`, `InvalidRevisionException`, `DeploymentConfigDoesNotExistException`,
`InvalidAlarmConfigException`, `InvalidAutoRollbackConfigException`, `InvalidDeploymentStyleException`,
`InvalidLoadBalancerInfoException`, `InvalidTriggerConfigException`, `InvalidTargetInstancesException`,
`InvalidAutoScalingGroupException`, `InvalidECSServiceException`, `DescriptionTooLongException` and
`ThrottlingException`, all at 400, plus the five limit codes (`ApplicationLimitExceededException`,
`DeploymentGroupLimitExceededException`, `DeploymentLimitExceededException`,
`AlarmsLimitExceededException`, `TriggerTargetsLimitExceededException`). Each checks something not
modeled — a revision's contents, a configuration's existence, the resources a member names, a quota —
so none has a site.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::CodeDeploy::Application | ApplicationName | Deployed through `CreateApplication`, and deleted through `DeleteApplication`. `ApplicationName` falls back to the logical ID. `ComputePlatform` is sent, and `Tags` is dropped. No `Fn::GetAtt` attribute is published |
| AWS::CodeDeploy::DeploymentGroup | DeploymentGroupName | Deployed through `CreateDeploymentGroup`, so `GetDeploymentGroup` answers the group a stack created. Deleted through `DeleteDeploymentGroup`. Every property is renamed from PascalCase to the API's lowerCamel at every depth. `Ec2TagSet` and `OnPremisesTagSet` are also reshaped from `{…TagGroup: [filter…]}` objects to the API's filter lists. Two properties are dropped. `Deployment` would be a `CreateDeployment`, which a stack does not issue. `Tags` is not read by the plugin. No `Fn::GetAtt` attribute is published ([#1203](https://github.com/scttfrdmn/substrate/issues/1203)) |

AWS publishes three types in the namespace. `AWS::CodeDeploy::DeploymentConfig` is declined: the
plugin routes no `CreateDeploymentConfig`, so the type is refused in
[strict mode](/reference/cloudformation#strict-mode-and-template-validation) and in lenient mode falls through to the generic
stub, whose `Ref` is the logical ID. A deployment group naming it deploys, because the plugin does not look configurations up.

## Cost

`CreateDeployment` is attributed $0.000001 per call. The table's own comment records the rationale as
"free for EC2/on-premises, approximated per deployment"; AWS's published price structure for CodeDeploy
could not be read off its pricing page during this pass, so treat the figure as a placeholder rather
than a derivation. Substrate does not model on-premises instances at all — none of the four
`*OnPremisesInstance*` operations is routed — so the entry fires identically for every deployment
whatever platform it names.
