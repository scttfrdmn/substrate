# CodeBuild

**Endpoint:** `codebuild.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: CodeBuild_20161006.{Op}`)

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateProject | `name` required; `source`, `artifacts` and `environment` are stored as opaque objects and never inspected |
| BatchGetProjects | `names` must hold 1–100 non-empty names, else `InvalidInputException`/400; a well-formed name naming nothing is reported in `projectsNotFound` ([#1159](https://github.com/scttfrdmn/substrate/issues/1159)) |
| UpdateProject | Reads the flat published `UpdateProjectInput`; [a member the request carries replaces the stored one](#updateproject-replaces-the-members-it-names), and the response is wrapped in `project` |
| DeleteProject | [Idempotent](#deleteproject-is-idempotent): a project that does not exist is deleted successfully |
| ListProjects | Reports names only; `sortBy`, `sortOrder` and `nextToken` are not read |
| StartBuild | Only `projectName` is read; the build is `SUCCEEDED` before the call returns unless [a seed makes it progress](#a-build-progresses-under-a-seed) |
| BatchGetBuilds | Each build is [one observation of its own seeded progression](#a-build-progresses-under-a-seed); `ids` must hold 1–100 non-empty IDs, else `InvalidInputException`/400; [`buildsNotFound` means absence only](#what-buildsnotfound-and-projectsnotfound-mean) ([#1186](https://github.com/scttfrdmn/substrate/issues/1186)) |

## UpdateProject replaces the members it names

`UpdateProjectInput` is flat: `name`, its one `Required: Yes` member, beside the members to change.
Only `UpdateProjectOutput` is wrapped, in a single `project` member. Until
[#1158](https://github.com/scttfrdmn/substrate/issues/1158) the handler read a `project` wrapper on
the request too, so every SDK request was refused with `InvalidInputException` / *"name is required"*.

The page describes each optional member as "a new or replacement" value. So a member the request
carries replaces the stored one, and an empty `description` clears it. A member the request omits is
left as it was. Of the published members, `description`, `source`, `artifacts`, `environment` and
`serviceRole` are read; the others are not stored by `CreateProject` either. A name that resolves to
no project is refused with `ResourceNotFoundException`/400, which the page publishes.

The other six routed handlers read flat members, as their pages publish.

## DeleteProject is idempotent

`API_DeleteProject` publishes exactly one error, `InvalidInputException`/400, and no not-found at
all, with "an HTTP 200 response with an empty HTTP body": the shape of a delete that succeeds whether
or not the project existed. So a delete of a project that does not exist answers 200, and a teardown
path that runs twice succeeds, as it does against AWS. An absent or empty `name` is still
`InvalidInputException`/400, the operation's one published error. The body is `{}`, the form
substrate's JSON-protocol deletes answer
([#1206](https://github.com/scttfrdmn/substrate/issues/1206)'s recorded decision), which every SDK
decodes as an empty response. Until
[#1159](https://github.com/scttfrdmn/substrate/issues/1159) a missing project was refused with
`ResourceNotFoundException`, a code the operation's page does not publish.

## What buildsNotFound and projectsNotFound mean

`API_BatchGetBuilds` glosses `buildsNotFound` as "the IDs of builds for which information could not
be found", and `projectsNotFound` means the same for projects. Both report **absence only**: a
well-formed ID or name that names nothing. A request whose list is absent, empty, longer than 100 or
holds an empty string is refused with `InvalidInputException`/400, the one error both pages publish.
A failure of substrate's own state store, or a stored record that will not decode, is substrate's
failure, not an AWS condition: it answers an error rather than reporting the ID as missing. Until
[#1186](https://github.com/scttfrdmn/substrate/issues/1186) both were folded into `buildsNotFound`
with a 200, which made a broken store indistinguishable from a build that was never created.

## A build progresses under a seed

`StartBuild` stores the build with `buildStatus: "SUCCEEDED"`, `currentPhase: "COMPLETED"` and
`startTime` equal to `endTime`, which is what an unseeded build reports. Running the build is
workload-internal and out of scope, but its status is observable, and a seed makes the published
in-progress state observable before the final one
([#1155](https://github.com/scttfrdmn/substrate/issues/1155)), under the rules
[every progression shares](/reference/concepts/seeds-and-replay#how-a-progression-is-seeded):

```
POST   /v1/codebuild/build-status   {"buildId": "project:uuid" | "*", "pendingObservations": 2,
                                     "currentPhase": "PRE_BUILD", "finalStatus": "FAILED",
                                     "statusCode": "COMMAND_EXECUTION_ERROR", "message": "…"}
DELETE /v1/codebuild/build-status   (all seeds; ?buildId=… for one)
```

- `BatchGetBuilds` reports `buildStatus: "IN_PROGRESS"`, with `currentPhase` the seed's phase (default
  `BUILD`) and no `endTime`, for `pendingObservations` observations. It then reports `finalStatus` —
  `SUCCEEDED`, `FAILED`, `FAULT`, `TIMED_OUT` or `STOPPED`, default the record's — with
  `currentPhase: "COMPLETED"` and `endTime`. Each build in one call is one observation of its own
  countdown, so a call over several builds spends one from each.
- `Build` publishes no top-level failure reason: the diagnostic lives in a phase's `contexts`. So a
  failed final status with a seeded `statusCode` or `message` also reports
  `phases: [{"phaseType": <the seed's phase>, "phaseStatus": <finalStatus>, "contexts": [{"statusCode", "message"}]}]`.
  Otherwise `phases` stays absent, as it is for every unseeded build.
- `StartBuild`'s own response is a create-time read. It reports the seed's first state without
  spending an observation, so `pendingObservations` counts `BatchGetBuilds` polls only.
- A phase outside `API_BuildPhase`'s `phaseType` values (or `COMPLETED`, which ends a build rather than
  naming a phase in it), or a final status outside `API_Build`'s `buildStatus` values, is refused with
  400 and not stored.

`logs`, `artifacts` and `buildComplete` are still not emitted.

`StartBuild` also reads only `projectName`: the twenty-odd `*Override` members AWS publishes, and
`idempotencyToken`, are neither stored nor refused, so two identical calls mint two builds.

## What a refusal reports

| Condition | Code | Status |
|-----------|------|--------|
| a body that will not parse | `InvalidInputException` | 400 |
| a required member absent or empty | `InvalidInputException` | 400 |
| a project name already in use | `ResourceAlreadyExistsException` | 400 |
| a project that does not exist | `ResourceNotFoundException` | 400 |

All four are 400, which is what every CodeBuild page publishes — `StartBuild`'s
`ResourceNotFoundException` included. `DeleteProject` no longer reaches the not-found row (#1159). `AccountLimitExceededException` and `OAuthProviderException` are published and
have no site.

## The account and Region a record carries reach no response

`CodeBuildProject` and `CodeBuildBuild` declare `accountID` and `region` under wire-visible `json`
tags, because the record is what `MemoryStateManager` snapshots and a replay reads back. Until
[#756](https://github.com/scttfrdmn/substrate/issues/756), five sites answered the records whole:
`CreateProject`, `UpdateProject`, `BatchGetProjects`, `StartBuild` and `BatchGetBuilds`. So every
project and build carried both members, which neither published shape has. All five now answer
through `codebuildProjectOut` and `codebuildBuildOut` (`emulator/codebuild_wire.go`). All seven
routed operations are driven by `TestCodeBuildWire_ProjectResponsesCarryNoBookkeepingMember` and its
sibling in `emulator/codebuild_wire_test.go`, which walk each decoded document for either member at
any depth.

The same five sites rendered `created`, `lastModified`, `startTime` and `endTime` as RFC3339 strings.
CodeBuild speaks `awsJson1_1`, where `API_BatchGetProjects` and `API_BatchGetBuilds` publish each as
a number, so a typed SDK could not decode any project or build response
([#1338](https://github.com/scttfrdmn/substrate/issues/1338)). They are now epoch seconds. The stored
records keep their `time.Time`, so a recorded run replays identically.

`emulator/codebuild_plugin_test.go` used to decode these responses into the stored record types,
which typed the dates as `time.Time`. It therefore read the RFC3339 strings without complaint, which
is how the decode failure survived. It now decodes the published members, typed as the API publishes
them.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::CodeBuild::Project | Name | A stub: properties are recorded in the CloudFormation stub store, which the CodeBuild plugin does not read, so a project deployed from a template is invisible to `BatchGetProjects` and `ListProjects` and cannot be built |

## Cost

`StartBuild` is attributed $0.0001 per call, standing in for CodeBuild's per-build-minute charge
($0.005/minute for `general1.small` on Linux). Substrate's builds take no time, so the duration term
has nothing to multiply.
