# SageMaker

**Endpoint:** `api.sagemaker.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: SageMaker.{Op}`)

Two unrelated slices of SageMaker are modelled: the Studio app lifecycle and training jobs.

## Supported operations

| Operation | Notes |
|-----------|-------|
| ListDomains | Always an empty list — Substrate has no domain records |
| ListApps | [Pages and sorts by its published members](#listapps-and-listtrainingjobs-page-by-their-published-members); `DomainIdEquals` and `UserProfileNameEquals` filter, and `SpaceNameEquals` matches no app |
| CreateApp | Only `AppName` is required; `AppType` and `DomainId` are `Required: Yes` and unchecked |
| DeleteApp | [Refuses an app that does not exist](#deleteapp-refuses-an-absent-app) with `ResourceNotFound`/400 |
| DescribeApp | Seven published members, `CreationTime` among them for an app created since #1400 |
| CreatePresignedDomainUrl | A fixed stub URL; no domain, user profile or expiry is read |
| CreateTrainingJob | Only `TrainingJobName` is read; the job is recorded `Completed`, and [a seed governs what is observed](#a-training-job-s-status-is-a-seeded-progression) |
| DescribeTrainingJob | One observation of [the seeded progression](#a-training-job-s-status-is-a-seeded-progression), answering [`SecondaryStatus` beside it](#secondarystatus-follows-the-observed-status) |
| StopTrainingJob | Writes `Stopped` and restarts the job's countdown, so a seed can report `Stopping` first; a `Completed` job is stopped without complaint |
| ListTrainingJobs | Each listed job is one observation of [the same progression](#a-training-job-s-status-is-a-seeded-progression), so List and Describe agree (#1162), and each summary carries the same `SecondaryStatus`. [Pages, sorts and filters by its published members](#listapps-and-listtrainingjobs-page-by-their-published-members) |

## A training job's status is a seeded progression

`CreateTrainingJob` records `TrainingJobStatus: "Completed"`. Nothing in `CreateTrainingJob`'s large
published input is read beyond the name: the algorithm, the resource configuration, the
hyper-parameters and the input data are neither stored nor validated. Running the training is out of
scope; a seed is what makes the *transition and the outcome* observable
([#1155](https://github.com/scttfrdmn/substrate/issues/1155),
[#1162](https://github.com/scttfrdmn/substrate/issues/1162)), following
[the rules every progression shares](/reference/concepts/seeds-and-replay#how-a-progression-is-seeded):

```
POST   /v1/sagemaker/training-job-status   {"trainingJobName": "job-1", "pendingObservations": 2,
                                            "transientStatus": "InProgress", "status": "Failed",
                                            "failureReason": "CapacityError: …"}
DELETE /v1/sagemaker/training-job-status    (all seeds; ?trainingJobName=… for one)
```

- **Keyed by job:** `trainingJobName` defaults to the `"*"` wildcard, and the exact name wins.
- **Counted:** `pendingObservations` observations report `transientStatus` (`InProgress` or
  `Stopping`), then the job settles to `status`, the recorded status when empty. `DescribeTrainingJob`
  and every `ListTrainingJobs` that lists the job each spend one, so the two answer the same status for
  the same job: `ListTrainingJobs` used to report the record while `DescribeTrainingJob` reported the
  seed.
- **`failureReason`** is reported with a settled `Failed`, and refused with any other status.
- **Validated:** `status` must be one of `TrainingJobStatus`' published values
  (`InProgress | Completed | Failed | Stopping | Stopped`); a misspelling used to be accepted and
  reported. `{"trainingJobName": …, "status": …, "failureReason": …}`, the pre-#1155 body, still means
  what it meant: no countdown, settled from the first observation.
- **A stop restarts the countdown.** After `StopTrainingJob`, the countdown reports `Stopping` by
  default and the job settles to its recorded `Stopped`. The seed's settled status and reason do not
  override a stop the caller made, because the stop is itself the transition being observed.

The POST answers `{"ok": true, "trainingJobName": "status:<name|*>"}`, the key the seed was stored
under, as every progression's seed endpoint does.

## SecondaryStatus follows the observed status

`API_DescribeTrainingJob` publishes `SecondaryStatus` as Required, and `DescribeTrainingJob` used to
omit it ([#1400](https://github.com/scttfrdmn/substrate/issues/1400)). It is now derived from the
`TrainingJobStatus` the same observation reports, so a seeded countdown moves the two together:

| `TrainingJobStatus` | `SecondaryStatus` |
|---------------------|-------------------|
| `InProgress` | `Training` |
| `Completed` | `Completed` |
| `Failed` | `Failed` |
| `Stopping` | `Stopping` |
| `Stopped` | `Stopped` |

The page groups the secondary values under the primary status each belongs to. Four groups map one
to one. For `InProgress`, `Training` is substrate's choice among the six the page lists. The other
five (`Starting`, `Pending`, `Downloading`, `Interrupted`, `Uploading`) describe stages of a workload
substrate does not run. `MaxRuntimeExceeded` and `MaxWaitTimeExceeded` are never reported, because no
runtime limit is modelled. `ListTrainingJobs` summaries carry the same value.

## ListApps and ListTrainingJobs page by their published members

Neither operation used to read a pagination member. Both answered every record in one response, and
both discarded the error from reading their index, so a store fault answered an empty list
([#1400](https://github.com/scttfrdmn/substrate/issues/1400)). Both now page with `MaxResults` and
`NextToken`, and return a store fault as an error:

| | ListApps | ListTrainingJobs |
|---|---|---|
| `MaxResults` | 1–100, default 10 (published) | 1–100, default 100 (the page publishes no default) |
| `SortBy` | `CreationTime` (the only value) | `Name`, `CreationTime` or `Status`, default `CreationTime` |
| `SortOrder` | `Ascending` or `Descending`, default `Ascending` | the same |
| filters | `DomainIdEquals`, `UserProfileNameEquals`; `SpaceNameEquals` matches nothing, since no space is modelled | `NameContains`, `StatusEquals`; `TrainingPlanArnEquals` and `WarmPoolStatusEquals` match nothing, since neither is modelled |

- **Ties.** Equal sort keys are broken by name for `ListTrainingJobs` and by state key for `ListApps`,
  so every offset token is stable.
- **`StatusEquals` is applied after the cut.** The page's Note says that with both members set, "the
  `MaxResults` number of training jobs are first retrieved ignoring the `StatusEquals` parameter and
  then they are filtered". A page can therefore hold fewer summaries than `MaxResults`, or none, and
  still carry a `NextToken`.
- **The time filters are not read.** `ListTrainingJobs`' `CreationTimeAfter`/`Before` and
  `LastModifiedTimeAfter`/`Before` are ignored.
- **Old apps.** An app records its `CreationTime` from #1400 on, which `DescribeApp` and each
  `ListApps` element now answer. An app recorded earlier has none, sorts first and answers no
  `CreationTime`.
- **Observations.** Every job the listing visits is observed, including jobs outside the returned
  page. A `Status` sort has to observe each job before it can order them.

A token neither operation issued, a `MaxResults` outside 1–100, an enum outside its published values,
a `NameContains` outside `[a-zA-Z0-9\-]+` or longer than 63 characters, and a `SpaceNameEquals` sent
with `UserProfileNameEquals` are all refused. The page states that last pair cannot both be set.

## DeleteApp refuses an absent app

`DeleteApp` refuses an app that does not exist with `ResourceNotFound`/400, which `API_DeleteApp`
publishes and `DescribeApp` already answered for the same condition. It used to delete the state key
unconditionally and answer 200 ([#1162](https://github.com/scttfrdmn/substrate/issues/1162)).

An app's state key is the account, Region, domain ID, user profile name, app type and app name joined —
all six, so two apps differing only in type are distinct records, which is what AWS's four-part
identity implies.

## What a refusal reports

| Condition | Code | Status |
|-----------|------|--------|
| a body that will not parse | `ValidationError` | 400 |
| `AppName` or `TrainingJobName` absent or empty | `ValidationError` | 400 |
| a list member outside its published range, values or pattern, or an unissued `NextToken` | `ValidationError` | 400 |
| an app or training job that does not exist, including on `DeleteApp` | `ResourceNotFound` | 400 |

`ValidationError` comes from SageMaker's Common Errors page. None of these operations publishes an
input-validation error of its own (`emulator/sagemaker_errors.go`).

`ResourceNotFound` is spelled without the `Exception` suffix because that is how SageMaker publishes
it, and at 400, which is the status its pages publish. A store fault while reading an app or a
training job is an error, not `ResourceNotFound`. `DescribeApp`, `DescribeTrainingJob` and
`StopTrainingJob` used to report a store fault as the resource being absent (#1400). SageMaker's `ResourceInUse`,
`ResourceLimitExceeded` and `ConflictException` are published and have no site.

## The account and Region a record carries reach no response

`SageMakerApp` and `SageMakerTrainingJob` declare `AccountID` and `Region` under wire-visible `json`
tags, because the record is what `MemoryStateManager` snapshots and a replay reads back. Until
[#756](https://github.com/scttfrdmn/substrate/issues/756), three sites answered the records whole:

- `DescribeApp` and `DescribeTrainingJob` answered the record as the whole body.
- `ListApps` answered every element as a record. That also carried `AppArn`, which `API_AppDetails`,
  the element's shape, does not publish. `DescribeApp` does publish it.

They now answer `sagemakerAppOut`, `sagemakerAppDetailsOut` and `sagemakerTrainingJobOut`
(`emulator/sagemaker_wire.go`), and the stored records are unchanged. All ten routed operations are
driven by `TestSageMakerWire_AppResponsesCarryNoBookkeepingMember` and its training-job sibling in
`emulator/sagemaker_wire_test.go`.

## CloudFormation resource types

None are handled specifically. A `AWS::SageMaker::*` resource in a template is refused in
[strict mode](/reference/cloudformation#strict-mode-and-template-validation); in lenient mode it falls to the generic stub,
whose `Ref` is the logical ID, and is invisible to the SageMaker plugin.

## Cost

`CreateTrainingJob` is attributed $0.001 per call and `CreateApp` $0.0001. Real SageMaker bills
training by instance-second and a Studio app by the instance behind it; Substrate's jobs and apps
consume no time, so the attribution is a per-call proxy rather than a rate.
