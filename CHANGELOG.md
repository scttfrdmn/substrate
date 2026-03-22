# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [v0.45.0] - 2026-03-21

### Added

- **AWS Batch plugin** (`batch_plugin.go`): New `BatchPlugin` for the AWS Batch service.
  Handles `SubmitJob`, `DescribeJobs`, `TerminateJob`, `ListJobs`,
  `CreateComputeEnvironment`, `CreateJobQueue`, and `RegisterJobDefinition` via REST/JSON
  path routing on `batch.{region}.amazonaws.com`. Jobs are immediately stored with
  `SUCCEEDED` status (deterministic). State namespace `"batch"`, keys `job:`, `job_ids:`.
  Costs: `batch/SubmitJob = $0.00001`. Closes #237.
- **Amazon SageMaker plugin** (`sagemaker_plugin.go`): New `SageMakerPlugin` for the
  SageMaker service. Handles Studio app operations (`CreateApp`, `DeleteApp`, `DescribeApp`,
  `ListApps`, `ListDomains`, `CreatePresignedDomainUrl`) and training job operations
  (`CreateTrainingJob`, `DescribeTrainingJob`, `StopTrainingJob`, `ListTrainingJobs`) via
  JSON-target protocol (`X-Amz-Target: SageMaker.{Op}`). Training jobs are immediately
  `Completed` (deterministic). State namespace `"sagemaker"`. Costs:
  `sagemaker/CreateTrainingJob = $0.001`, `sagemaker/CreateApp = $0.0001`. Closes #238.
- **Amazon EMR Serverless plugin** (`emrserverless_plugin.go`): New `EMRServerlessPlugin`
  for the EMR Serverless service. Handles `CreateApplication`, `GetApplication`,
  `DeleteApplication`, `StartJobRun`, `GetJobRun`, `CancelJobRun`, and `ListJobRuns` via
  REST/JSON path routing on `emrserverless.{region}.amazonaws.com`. Job runs immediately
  have state `SUCCESS` (deterministic). State namespace `"emrserverless"`. Costs:
  `emrserverless/StartJobRun = $0.0001`, `emrserverless/CreateApplication = $0.00001`.
  Closes #239.
- **Amazon HealthOmics plugin** (`omics_plugin.go`): New `OmicsPlugin` for the HealthOmics
  service. Handles `StartRun`, `GetRun`, `CancelRun`, and `ListRuns` via REST/JSON path
  routing on `omics.{region}.amazonaws.com`. Runs are immediately `COMPLETED`
  (deterministic). Run IDs are 10-digit numeric strings matching real HealthOmics format.
  State namespace `"omics"`. Costs: `omics/StartRun = $0.001`. Closes #240.

## [v0.44.4] - 2026-03-21

### Added

- **EC2 launch templates** (`ec2_plugin.go`, `ec2_types.go`): Added `CreateLaunchTemplate`,
  `DescribeLaunchTemplates`, and `DeleteLaunchTemplate` operations. `RunInstances` now
  resolves `ImageId`, `InstanceType`, `KeyName`, and `SecurityGroupId` from a referenced
  launch template when those parameters are not supplied directly. New types:
  `EC2LaunchTemplate`, `EC2LaunchTemplateData`, `generateLaunchTemplateID`. State keys use
  namespace `"ec2"` with prefix `lt:`, `lt_by_name:`, `lt_ids:`. Closes #243.

## [v0.44.3] - 2026-03-22

### Fixed

- **StepFunctions routing** (`parser.go`): Added `"awsstepfunctions": "states"` alias to
  `targetServiceAliases`. The AWS SDK v2 sfn client sends `X-Amz-Target: AWSStepFunctions.*`
  which was not matched by the existing `"Amazon"` prefix stripping, causing every SDK call
  to return `ServiceNotAvailable: service not emulated: awsstepfunctions`. Closes #242.
- **ECS timestamp serialization** (`ecs_types.go`, `epochseconds.go`): Replaced `time.Time`
  fields `ECSTaskDefinition.RegisteredAt`, `ECSService.CreatedAt`, `ECSTask.StartedAt`, and
  `ECSTask.StoppedAt` with the new `EpochSeconds` type, which marshals as a JSON float64
  (Unix epoch seconds). The AWS SDK v2 ECS client uses `smithytime.ParseEpochSeconds` and
  expects a JSON number, not an RFC3339 string. Closes #241.
- **Step Functions timestamp serialization** (`stepfunctions_plugin.go`): All response maps
  (`creationDate`, `startDate`, `stopDate`, `updateDate`) now emit `float64` epoch seconds
  via the new `sfnEpoch` helper instead of RFC3339 strings. Struct fields in list-response
  entry types (`smEntry.CreationDate`, `execEntry.StartDate`/`StopDate`,
  `actEntry.CreationDate`) changed from `string` to `float64`. Fixes the companion timestamp
  issue noted in #242.

### Added

- **`EpochSeconds` type** (`epochseconds.go`): New package-level type that wraps `time.Time`
  and marshals/unmarshals as JSON float64 epoch seconds, with RFC3339 string fallback on
  unmarshal for backward-compatible state reads.

## [v0.44.2] - 2026-03-19

### Added

- **SQS JSON protocol support** (`sqs_plugin.go`): `SQSPlugin` now handles both the
  query protocol (`application/x-www-form-urlencoded`) and the AWS JSON protocol
  (`application/x-amz-json-1.0` with `X-Amz-Target: AmazonSQS.<Op>`) for all 16
  operations. Existing query-protocol behaviour is completely unchanged. New helpers:
  `sqsIsJSONProtocol`, `sqsJSONResponse`, `sqsQueueURLFromRequest`. JSON `ReceiveMessage`
  always returns `"Messages":[]` (never `null`) when the queue is empty. Closes #236.
- **12 new SQS JSON protocol tests** (`sqs_plugin_test.go`): cover create/get/delete queue,
  attributes, send/receive/delete messages, batch operations, tags, visibility changes,
  purge, error on non-existent queue, and a cross-protocol test (create via JSON, send via
  query, receive via JSON).

### Note

v0.44.0 and v0.44.1 are tagged at `e11f4b2` (servicequotas routing fix). v0.43.4 is a
later commit on the same main branch that added EC2 `DescribeRegions`; it supersedes
v0.44.0/v0.44.1 in functionality. v0.44.2 is built on top of v0.43.4 and consolidates
all changes onto the v0.44.x line.

## [v0.43.4] - 2026-03-19

### Added

- **EC2 `DescribeRegions`** (`ec2_plugin.go`): Returns a pre-seeded list of three enabled
  regions (`us-east-1`, `us-west-2`, `eu-west-1`) with `opt-in-not-required` opt-in status
  and the canonical regional EC2 endpoint. Supports `RegionName.N` filters. Enables testing
  of code that fans out EC2 calls across regions (e.g. `GetEnabledRegions` in truffle).
  Closes #235.

## [v0.43.3] - 2026-03-19

### Added

- **EC2 instance type and spot price operations** (`ec2_plugin.go`): Three new read-only
  operations enable testing of instance-type discovery and Spot pricing without real AWS
  credentials.
  - `DescribeInstanceTypes`: returns a pre-seeded catalog of 8 instance types
    (`t3.micro`, `c5.xlarge`, `c5.2xlarge`, `m5.large`, `r5.xlarge`, `p3.2xlarge`,
    `g4dn.xlarge`, `inf1.xlarge`) with vCPU, memory, architecture, and usage-class details.
    Supports `InstanceType.N` filters.
  - `DescribeInstanceTypeOfferings`: returns the same catalog for all three AZs in the
    request region. Supports `location` filter.
  - `DescribeSpotPriceHistory`: returns fixed stub prices for each seeded instance type.
    Supports `InstanceType.N`, `AvailabilityZone`, and `ProductDescriptions` filters.
  Closes #234.

## [v0.43.2] - 2026-03-19

### Fixed

- **FSx LustreConfiguration in responses** (`fsx_plugin.go`): `CreateFileSystem` and
  `DescribeFileSystems` now include a `LustreConfiguration` object (`MountName`,
  `DeploymentType`) in the wire response for LUSTRE file systems. Previously this field was
  absent, causing nil-pointer panics in any code that dereferenced
  `fs.LustreConfiguration.MountName`. `DeploymentType` defaults to `SCRATCH_2` when not
  supplied by the caller; `MountName` is `"fsx"` for SCRATCH_2 and a random hex string for
  other deployment types. Closes #233.

## [v0.43.1] - 2026-03-19

### Fixed

- **FSx SDK routing** (`parser.go`): Added `"awssimbaapiservice": "fsx"` to
  `targetServiceAliases`. The AWS SDK v2 FSx client sends
  `X-Amz-Target: AWSSimbaAPIService_v20180301.<Op>` — the parser stripped the version
  suffix (`_v20180301`) leaving `AWSSimbaAPIService` (lowercase: `awssimbaapiservice`), which
  had no alias and caused a `501 ServiceNotAvailable` error on every FSx SDK call. Closes #232.

## [v0.43.0] - 2026-03-19

### Added

- **FSx plugin** (`fsx_plugin.go`): JSON-protocol plugin on `fsx.{region}.amazonaws.com`
  (target: `AmazonFSx.<Op>`). Implements `CreateFileSystem`, `DescribeFileSystems`, and
  `DeleteFileSystem`. Supports `LUSTRE`, `WINDOWS`, `ONTAP`, and `OPENZFS` file system types.
  State keys: `fs:{acct}/{region}/{id}`, `fs_ids:{acct}/{region}`. File systems transition
  immediately to `AVAILABLE`; delete soft-marks as `DELETED`. `CreationTime` is returned as
  a Unix epoch `float64` per AWS SDK requirements. Cost: `fsx/CreateFileSystem = $0.00013`.
  Betty CFN: `AWS::FSx::FileSystem` at priority 3; `Ref` = `FileSystemId`. Closes #230.

### Fixed

- **Scheduler timestamp format** (`scheduler_plugin.go`): `GetSchedule` and `ListSchedules`
  now return `CreationDate` and `LastModificationDate` as Unix epoch `float64` values (e.g.
  `1711929600.0`) instead of RFC3339 strings. The AWS SDK v2 `scheduler` client deserializes
  these fields as `*time.Time` via a `float64` JSON path and previously panicked or returned
  a zero time when it received a quoted string. Closes #231.

## [v0.42.1] - 2026-03-19

### Fixed

- **Spurious SDK warning** (`server.go`): `writeResponse` now sets `Content-Length` on every
  response (unless the plugin already supplied one, as S3 HEAD does). This allows the AWS SDK
  v2 transport to drain and close response bodies cleanly, eliminating the
  "failed to close HTTP response body, this may affect connection reuse" warning that appeared
  in `go test -v` output when using `StartTestServer`. Closes #229.

## [v0.42.0] - 2026-03-19

### Added

- **EventBridge Scheduler plugin** (`scheduler_plugin.go`): REST-JSON plugin on
  `scheduler.{region}.amazonaws.com`. Implements `CreateSchedule` (HTTP 201),
  `GetSchedule`, `UpdateSchedule`, `DeleteSchedule`, `ListSchedules`. State keys:
  `sched:{acct}/{region}/{groupName}/{name}`. Supports `namePrefix`, `state`, `maxResults`,
  and `nextToken` (base64 integer offset) pagination on `ListSchedules`. Errors:
  `ConflictException` (409) on duplicate create, `ResourceNotFoundException` (404) on
  missing schedule. Cost: `scheduler/CreateSchedule = $0.0000001`. Closes #228.

## [v0.41.3] - 2026-03-18

### Fixed

- **`revive` naming** (`ec2_types.go`, `ec2_plugin.go`, `iam_managed.go`): Renamed struct fields
  to comply with Go naming conventions — `PublicDnsName`→`PublicDNSName`,
  `PrivateDnsName`→`PrivateDNSName`, `MapPublicIpOnLaunch`→`MapPublicIPOnLaunch`,
  `EnableDnsSupport`→`EnableDNSSupport`, `EnableDnsHostnames`→`EnableDNSHostnames`.
- **`nilerr`** (`ec2_plugin.go`, `stepfunctions_asl.go`): Added `//nolint:nilerr` on intentional
  nil returns following a non-nil error (resource-not-found pass-through and non-JSON response
  stub).
- **`staticcheck` S1016** (`ec2_plugin.go`): Added `//nolint:staticcheck` on `tagItem` struct
  literals where xml tags differ from the source `EC2Tag` json tags.

## [v0.41.2] - 2026-03-18

### Fixed

- **gofmt / linter fixes** (`ec2_plugin.go`, `ce_plugin.go`, `ce_plugin_test.go`, `ce_types.go`,
  `s3_plugin.go`, `parser.go`, and several test files): Applied `gofmt`; fixed all
  `golangci-lint` issues — misspellings (`behaviour`→`behavior`, `synthesising`→`synthesizing`,
  `Synthesise`→`Synthesize`, `modelled`→`modeled`), `ineffassign` on unused `name` reassignment
  in `deleteKeyPair`, and `nilerr` in `deleteObjects`.

## [v0.41.1] - 2026-03-18

### Fixed

- **gofmt formatting** (`ec2_plugin.go`, `ec2_plugin_test.go`, `cloudwatch_plugin.go`,
  `cloudwatch_plugin_test.go`, `parser.go`, `plugin_lifecycle_test.go`, `s3_plugin_test.go`,
  `testing.go`): Applied `gofmt` to all files that were flagged by the CI lint step.

## [v0.41.0] - 2026-03-18

### Added

- **`DescribeAvailabilityZones`** (`ec2_plugin.go`): Returns 3 synthetic AZs (`{region}a/b/c`,
  state `available`) for any region. No state required; deterministic for replay.
- **`ModifySubnetAttribute`** (`ec2_plugin.go`): Toggles `MapPublicIpOnLaunch` on a subnet and
  persists the change. Returns `InvalidSubnetID.NotFound` (HTTP 400) for unknown subnets.
- **`ModifyVpcAttribute`** (`ec2_plugin.go`): Toggles `EnableDnsSupport` and `EnableDnsHostnames`
  on a VPC and persists the changes. Returns `InvalidVpcID.NotFound` (HTTP 400) for unknown VPCs.
- **`EnableDnsSupport` / `EnableDnsHostnames` fields** (`ec2_types.go`): Added to `EC2VPC`.
  `createVPC` now sets `EnableDnsSupport: true` (AWS default). `ensureDefaultVPC` additionally
  sets `EnableDnsHostnames: true` (default VPC default).
- **Elastic IP operations** (`ec2_plugin.go`, `ec2_types.go`): `AllocateAddress`,
  `AssociateAddress`, `DisassociateAddress`, `ReleaseAddress`, `DescribeAddresses`. EIPs use
  `eipalloc-` prefixed IDs. Associating with an instance updates the instance's public IP and
  DNS. Releasing an associated EIP returns `InvalidIPAddress.InUse` (HTTP 400). State keys:
  `eip:{acct}/{region}/{allocationID}`. Costs: `ec2/AllocateAddress = $0.005`.
- **NAT Gateway operations** (`ec2_plugin.go`, `ec2_types.go`): `CreateNatGateway`,
  `DescribeNatGateways`, `DeleteNatGateway`. Private IP derived deterministically via FNV-32a
  on the NAT gateway ID. State immediately `available`. `DeleteNatGateway` soft-deletes (state
  set to `deleted`). `DescribeNatGateways` supports `NatGatewayId.N` and `Filter.N` (`state`,
  `vpc-id`). State keys: `nat:{acct}/{region}/{natGatewayID}`. Costs:
  `ec2/CreateNatGateway = $0.045`.
- **`AWS::EC2::EIP`** (`betty_cfn.go`, `betty_cfn_v41_plugins.go`): CFN support; priority 2.
  `Ref` / `PhysicalID` = allocationID (`eipalloc-…`). `Fn::GetAtt AllocationId` and `PublicIp`
  available via `Metadata`.
- **`AWS::EC2::NatGateway`** (`betty_cfn.go`, `betty_cfn_v41_plugins.go`): CFN support;
  priority 4 (after subnets and EIPs). `Ref` / `PhysicalID` = natGatewayID (`nat-…`).

## [v0.40.0] - 2026-03-18

### Added

- **EC2 public IP / DNS assignment** (`ec2_plugin.go`, `ec2_types.go`): Instances launched
  into the default VPC subnet (or any subnet with `MapPublicIpOnLaunch=true`) now receive a
  deterministic synthetic public IPv4 address in Amazon's `54.0.0.0/8` range, a
  `dnsName` (public DNS), and a `privateDnsName` (private DNS) in both `RunInstances` and
  `DescribeInstances` XML responses. The public IP is derived from the instance ID via
  FNV-32a hash, ensuring stable values across replays. DNS name format matches real AWS:
  `ec2-<a>-<b>-<c>-<d>.compute-1.amazonaws.com` (us-east-1) /
  `ec2-<a>-<b>-<c>-<d>.<region>.compute.amazonaws.com` (other regions) /
  `ip-<a>-<b>-<c>-<d>.<region>.compute.internal` (private). Closes #N.
- `EC2Subnet.MapPublicIpOnLaunch` field persisted in state and surfaced in `DescribeSubnets`
  XML responses (`mapPublicIpOnLaunch` element). Default subnets created by `ensureDefaultVPC`
  now set this field to `true`, mirroring real AWS behaviour. Closes #226.

## [v0.39.0] - 2026-03-18

### Added

- **ASL execution engine** (`stepfunctions_asl.go`): Real in-process execution of Amazon
  States Language definitions replacing the previous stub that auto-flipped executions to
  SUCCEEDED. Supports all seven state types — Task, Pass, Wait, Choice, Succeed, Fail,
  Parallel, Map — with synchronous deterministic execution. Closes #151, #152.
- **Task state Lambda invocation** (`stepfunctions_asl.go`): Task states whose `Resource`
  is a Lambda ARN dispatch to the Lambda plugin via the plugin registry (same pattern as
  S3 notifications). Non-Lambda resources return a stub `{}` output. Closes #153.
- **`StartSyncExecution`** (`stepfunctions_plugin.go`): Express workflows (`type=EXPRESS`)
  support `StartSyncExecution`, which executes the state machine synchronously and returns
  `{executionArn, startDate, stopDate, status, output}` in a single response. Attempting
  `StartSyncExecution` on a STANDARD state machine returns `InvalidDefinition`. Closes #154.
- **Catch/Retry with configurable backoff** (`stepfunctions_asl.go`): Task states honour
  `Retry` configs (`MaxAttempts`, `IntervalSeconds`, `BackoffRate`) and `Catch` configs
  (`ErrorEquals`, `Next`, `ResultPath`). The `TimeController` is advanced by the computed
  back-off on each retry so tests remain deterministic. `States.ALL` wildcard supported.
  Closes #155.
- **Real execution history** (`stepfunctions_plugin.go`): `GetExecutionHistory` now returns
  the complete ordered event list recorded during execution — `ExecutionStarted`,
  `StateEntered`, `TaskScheduled`, `TaskSucceeded`/`TaskFailed`, `StateExited`,
  `ExecutionSucceeded`/`ExecutionFailed` — instead of the previous hardcoded two-event stub.
- **New ASL types** (`stepfunctions_types.go`): `StateMachineDefinition`, `ASLState`,
  `RetryConfig`, `CatchConfig`, `ChoiceRule`, `HistoryEvent`; `ExecutionState` gains
  `History []HistoryEvent` and `ErrorDetails string`.

### Changed

- `StepFunctionsPlugin` now accepts `registry` and `time_controller` options; both are
  wired by `RegisterDefaultPlugins` (`plugins.go`).
- `StartExecution` executes the state machine synchronously before returning; the stored
  execution has a terminal status (`SUCCEEDED` or `FAILED`) by the time the response is sent.
- `DescribeExecution` returns the stored state as-is (no longer auto-flips RUNNING to
  SUCCEEDED on first describe).

## [v0.38.0] - 2026-03-18

### Added

- **RDS Aurora clusters** (`rds_plugin.go`, `rds_types.go`): `CreateDBCluster`,
  `DescribeDBClusters`, `DeleteDBCluster` — cluster ARNs, endpoints, and status.
  Closes #133.
- **MSK plugin** (`msk_plugin.go`, `msk_types.go`): Kafka cluster lifecycle —
  `CreateCluster`, `DescribeCluster`, `ListClusters`, `DeleteCluster`. Cluster ARN
  format: `arn:aws:kafka:{region}:{acct}:cluster/{name}/{uuid}`. Closes #138.
- **Email capture endpoint** (`debug_ui.go`): `GET /v1/emails` returns all SES messages
  captured in the emulator as JSON, enabling test assertions without a real mail server.
  Closes #140.
- **`RestoreDBInstanceFromDBSnapshot`** (`rds_plugin.go`): Restores an RDS DB instance
  from a DB snapshot, creating a new instance with the snapshot's engine/storage settings.
  Closes #188.
- **Betty CFN resources for ElastiCache and MSK** (`betty_cfn_*.go`): CloudFormation
  resource types `AWS::ElastiCache::ReplicationGroup` and `AWS::MSK::Cluster` now
  deployable via Betty. Closes #189.

## [v0.37.1] - 2026-03-18

### Added

- **CloudWatch `PutMetricData` and `ListMetrics`** (`cloudwatch_plugin.go`): `PutMetricData`
  now records metric names by namespace so that `ListMetrics` can return them. Actual
  data-point values continue to be discarded (no time-series storage); `GetMetricData` still
  returns an empty result set. `ListMetrics` supports `Namespace` and `MetricName` filter
  parameters. Three unit tests added. Closes #221.

### Fixed

- **S3 `HeadObject` / `GetObject` omit `Content-Encoding` header** (`s3_plugin.go`,
  `s3_types.go`): `S3Object` was missing a `ContentEncoding` field; the header set on
  `PutObject` was silently discarded and never returned by subsequent reads. Added
  `ContentEncoding string` to `S3Object`, capture it in `putObject`, and emit
  `Content-Encoding` in both `headObject` and `getObject` when non-empty. Fixes #222.
- **S3 `PutObject` to non-existent bucket returns `NoSuchBucket`** (`s3_plugin.go`):
  the bucket-existence check was already in place; regression tests added to confirm the
  behaviour and prevent future regressions. Fixes #223.
- **S3 `ListObjectsV2` object size is correct** (`s3_plugin.go`): `Size` is stored as
  `int64(len(body))` at `PutObject` time and round-trips correctly through JSON state
  storage; regression test added. Fixes #224.

## [v0.37.0] - 2026-03-18

### Added

- **HTTP time-control endpoints** (`debug_ui.go`, `server.go`, `types.go`): three new REST
  endpoints let any external test harness drive Substrate's simulated clock over HTTP without
  a Go client.
  - `GET /v1/control/time` — returns `{"simulated_time":"<RFC3339Nano>","scale":<float>}`.
  - `POST /v1/control/time` — body `{"time":"<RFC3339>"}` jumps the clock to an absolute
    instant; response is the same shape as GET.
  - `POST /v1/control/scale` — body `{"scale":<positive float>}` sets the time acceleration
    factor (1.0 = real-time, 3600.0 = one real second equals one simulated hour); response
    is the same shape as GET. Returns 400 if scale ≤ 0.
  - `TimeController.Scale() float64` accessor added to `types.go` (thread-safe, RWMutex).
  - Nine unit tests in `time_control_test.go` cover GET, POST valid/invalid inputs, and the
    accelerated-clock property test (`TestTimeScale_AcceleratesTime`). Closes #220.

### Fixed

- **`TestLoadConfig_EnvOverride` test isolation** (`.gitignore`): viper's config discovery
  was picking up the `substrate` build artifact in the project root and attempting to parse
  it as YAML, causing the test to fail. Added `/substrate` and `/substratelocal` to
  `.gitignore` so accidental root-level builds are excluded from version control and do not
  interfere with config-loading tests.

## [v0.36.21] - 2026-03-18

### Fixed

- **EC2 `CreateSecurityGroup` now reads description from `GroupDescription` parameter** (`ec2_plugin.go`): the handler was reading `req.Params["Description"]` but the AWS EC2 query protocol wire format sends `GroupDescription`. Security groups created via any AWS SDK always had an empty description. The fix changes the key to `"GroupDescription"`. Existing tests that passed the wrong parameter name were also corrected. Fixes #219.

### Added

- **Regression test for `CreateSecurityGroup` description** (`ec2_plugin_test.go`): `TestEC2_SecurityGroup_GroupDescription` creates a group with `GroupDescription=my group description` and asserts that `DescribeSecurityGroups` returns the same value in `groupDescription`.

## [v0.36.20] - 2026-03-18

### Fixed

- **EC2 `DescribeKeyPairs` now includes `createTime` field** (`ec2_plugin.go`, `ec2_types.go`): `EC2KeyPair` was missing a `CreatedAt` field; `CreateKeyPair` and `ImportKeyPair` did not record the creation timestamp and `DescribeKeyPairs` omitted `<createTime>` from the XML response. AWS SDKs that read `KeyPairInfo.CreateTime` would receive `nil`, causing a zero `time.Time` on dereference. The fix adds `CreatedAt string` to `EC2KeyPair`, stamps it with the simulated clock at creation, and emits `<createTime>` in `DescribeKeyPairs`. Fixes #218.

### Added

- **Regression test for EC2 key pair `createTime`** (`ec2_plugin_test.go`): `TestEC2_KeyPair_CreateTime` verifies that `DescribeKeyPairs` returns a non-empty `createTime` after `CreateKeyPair`.

## [v0.36.19] - 2026-03-18

### Fixed

- **S3 multipart uploads now preserve user metadata** (`s3_plugin.go`, `s3_types.go`): `x-amz-meta-*` headers supplied to `CreateMultipartUpload` were silently discarded — `S3MultipartUpload` had no `UserMetadata` field, so `completeMultipartUpload` always assembled the final object with an empty metadata map. `HeadObject`/`GetObject` on a completed multipart object would return no metadata regardless of what was provided at initiation. The fix adds `UserMetadata map[string]string` to `S3MultipartUpload`, captures `extractUserMetadata(req.Headers)` in `createMultipartUpload`, and copies it to the assembled `S3Object` in `completeMultipartUpload`. `PutObject` (single-part) was not affected. Fixes #217.

### Added

- **Regression test for multipart user metadata** (`s3_plugin_test.go`): `TestS3_MultipartUpload_UserMetadata` verifies that `X-Amz-Meta-*` headers supplied to `CreateMultipartUpload` are returned by both `HeadObject` and `GetObject` after `CompleteMultipartUpload`.

## [v0.36.18] - 2026-03-18

### Fixed

- **EC2 `DescribeImages` now includes `creationDate` field** (`ec2_plugin.go`, `ec2_types.go`): `EC2Image` was missing a `CreationDate` field; `CreateImage` did not record the timestamp and `DescribeImages` omitted `<creationDate>` from the XML response. AWS SDKs that parse `creationDate` to sort or filter AMIs would see an empty value. The fix adds `CreationDate string` to `EC2Image`, stamps it with the current simulated time at `CreateImage` time, and emits `<creationDate>` in `DescribeImages`. Fixes #214.

- **EC2 `DescribeKeyPairs` now includes `keyType` field** (`ec2_plugin.go`, `ec2_types.go`): `EC2KeyPair` was missing a `KeyType` field; `CreateKeyPair` and `ImportKeyPair` did not store the key type and `DescribeKeyPairs` omitted `<keyType>` from the XML response. `CreateKeyPair` now defaults to `"rsa"` and honours an explicit `KeyType` parameter. `ImportKeyPair` infers the type from the OpenSSH public key prefix (`ssh-ed25519` → `"ed25519"`, else `"rsa"`). All three operations now echo `<keyType>` in their XML response. Fixes #215.

- **S3 `CreateMultipartUpload` and `ListMultipartUploads` now recognised with bare or empty `?uploads` query parameter** (`s3_plugin.go`): `parseS3Operation` checked `req.Params["uploads"] == "1"` to detect `?uploads`, but both the bare form (`?uploads`) and the AWS SDK form (`?uploads=`) store an empty string (`""`) in `Params`, never `"1"`. Both operations were silently mis-routed. The fix uses a map presence check (`_, ok := req.Params["uploads"]; ok`) for both routing decisions. Fixes #216.

### Added

- **Regression tests for EC2 `keyType` and `creationDate` fields** (`ec2_plugin_test.go`): `TestEC2_KeyPair_KeyType_Default` verifies `CreateKeyPair` defaults to `"rsa"` and that `DescribeKeyPairs` echoes `keyType`. `TestEC2_KeyPair_KeyType_Ed25519` verifies an explicit `KeyType=ed25519` is stored and returned. `TestEC2_Image_CreationDate` verifies `DescribeImages` returns a non-empty `creationDate`.

- **Regression test for S3 multipart `?uploads=` routing** (`s3_plugin_test.go`): `TestS3_MultipartUpload_ExplicitEmptyUploadsParam` sends `?uploads=` (explicit empty value, AWS SDK style) and verifies `CreateMultipartUpload` and `ListMultipartUploads` are both correctly routed.

## [v0.36.17] - 2026-03-18

### Fixed

- **S3 path-style requests to IPv4/IPv6 addresses now work correctly** (`parser.go`): `normalizeS3CustomEndpointVirtualHost` treated any host containing a dot as a virtual-hosted-style request and extracted the first octet of an IPv4 address (e.g. `"127"` from `"127.0.0.1"`) as the bucket name. Requests to `127.0.0.1:<port>` would incorrectly route to a non-existent bucket and return `NoSuchBucket`. The fix adds a `net.ParseIP` check before the dot-presence check; IPv4 and IPv6 bracket-notation addresses are now correctly treated as path-style hosts. Fixes #213.

### Changed

- **`StartTestServer` now uses `localhost` instead of `127.0.0.1`** (`testing.go`): the listener address and `TestServer.URL` are now `http://localhost:<port>`. This avoids the IP-address virtual-hosting misparse for callers that use `TestServer.URL` as an S3 base endpoint with path-style requests (the `UsePathStyle = true` pattern). The fix in `parser.go` is the authoritative resolution; this change adds defence-in-depth and simplifies the `URL` field for humans reading test output.

## [v0.36.16] - 2026-03-18

### Fixed

- **S3 `PutObject` now preserves trailing-slash keys (directory markers)** (`s3_plugin.go`): `parseS3Operation` was unconditionally stripping the trailing `/` from all object keys. Keys like `"newdir/"` became `"newdir"`, breaking the common S3 directory-marker pattern. The fix narrows the trim to the degenerate `"/"` case (from `"/bucket//"` style URLs) and leaves all other keys intact. Additionally, `putObject` and `getObject` now bypass the afero filesystem entirely for directory-marker keys (key ends with `/`) because `filepath.Clean` inside `MemMapFs` would corrupt the path — state metadata is sufficient for zero-body markers. Fixes #212.

### Added

- **Regression tests for S3 directory markers** (`s3_plugin_test.go`): `TestS3_DirectoryMarker_KeyPreserved` verifies that `PutObject` / `HeadObject` / `GetObject` / `ListObjectsV2` all preserve the trailing slash in the stored key. `TestS3_DirectoryMarker_AppearsAsPrefix` verifies that a directory-marker object is correctly grouped into `CommonPrefixes` (not `Contents`) when `ListObjectsV2` is called with `delimiter="/"`.

## [v0.36.15] - 2026-03-18

### Fixed

- **`DescribeSecurityGroups` now applies `Filters` parameter** (`ec2_plugin.go`): `group-name`, `vpc-id`, and `group-id` filters are now respected. Previously the `Filters` parameter was silently ignored and all security groups in the account/region were returned. This caused `ensureCanopyDefaultSG`-style idempotency checks to see stale or mismatched groups. Added `extractEC2Filters` helper to parse EC2 query-protocol `Filter.N.Name` / `Filter.N.Value.M` parameters into a name→values map, reusable by other describe operations. Fixes #211.

## [v0.36.14] - 2026-03-18

### Fixed

- **`GetCostAndUsage` GroupBy TAG type check is now case-insensitive** (`ce_plugin.go`): the `Type` field comparison now uses `strings.EqualFold` so that callers sending `"tag"` or `"Tag"` are handled identically to `"TAG"`, matching the robustness of other case-insensitive checks in the codebase. Fixes #210.

### Added

- **Regression tests for GroupBy TAG** (`ce_plugin_test.go`): `TestCE_GetCostAndUsage_CreateTagsAfterLaunch` verifies that tags applied via `CreateTags` after `RunInstances` (the common consumer pattern) are visible in GroupBy TAG cost queries. `TestCE_GetCostAndUsage_GroupByTag_NoEventStoreLeakage` verifies that EventStore service records (`"ec2"`, `"iam"`, `"ce"`, etc.) do not appear in TAG-grouped responses — only `"TagKey$TagValue"` entries are returned.

## [v0.36.13] - 2026-03-18

### Added

- **`GetCostAndUsage` GroupBy TAG support** (`ce_plugin.go`, `ce_types.go`): `GroupBy [{Type: "TAG", Key: "Name"}]` now returns one group per unique tag value using the AWS CE `"TagKey$TagValue"` key format. An optional `Filter.Dimensions` service filter (e.g. `Key=SERVICE, Values=["Amazon Elastic Compute Cloud - Compute"]`) restricts results to the matching service. Instances without the requested tag are grouped under `"TagKey$"`, matching real AWS behaviour. Fixes #209.

### Changed

- Extracted `ec2InstanceCostInWindow` package-level helper and `clampedQueryEnd` method to eliminate duplicate logic between `computeEC2UsageCost` and the new `computeEC2UsageCostByTag`.

## [v0.36.12] - 2026-03-18

### Fixed

- **`GetCostAndUsage` metric key now mirrors the request** (`ce_plugin.go`): groups and the total bucket previously always used `"UnblendedCost"` regardless of the `Metrics` field sent by the caller. Callers that request `"BlendedCost"` (the AWS SDK default) now receive `BlendedCost` keys with non-nil `Amount` values. Fixes #208.

## [v0.36.11] - 2026-03-18

### Fixed

- **EC2 `TerminateInstances` now records termination time** (`ec2_plugin.go`, `ec2_types.go`): `EC2Instance` gains a `TerminatedTime` field (RFC3339) set via `p.tc.Now()` when an instance is terminated, ensuring simulated-clock accuracy.

### Added

- **Cost Explorer EC2 usage costs** (`ce_plugin.go`): `GetCostAndUsage` now reflects simulated EC2 compute spend. For each instance in the account, hours overlapping the query window are multiplied by a per-type on-demand rate (17 instance types defined; unknown types fall back to $0.096/hr). Cost accrues from `LaunchTime` to `TerminatedTime` (or `tc.Now()` for running instances), so simulated time advances directly drive `GetCostAndUsage` results. Result appears under the `"Amazon Elastic Compute Cloud - Compute"` service group. Fixes #207.
- **`CEPlugin` accepts `time_controller` option** (`ce_plugin.go`, `plugins.go`): the `TimeController` is now passed to `CEPlugin` via `RegisterDefaultPlugins` so cost accrual uses simulated time rather than wall-clock time.

## [v0.36.10] - 2026-03-18

### Added

- **HTTP time-control endpoints** (`server.go`, `debug_ui.go`): three REST endpoints allow any test harness to drive the simulated clock over HTTP without a Go client. `GET /v1/control/time` returns the current simulated time and scale factor; `POST /v1/control/time` jumps the clock to an arbitrary RFC3339 instant; `POST /v1/control/scale` sets the acceleration factor (e.g. `{"scale":3600}` makes 1 real second equal 1 simulated hour).
- **`TimeController.Scale()`** (`types.go`): new read-only accessor returns the current time acceleration factor.
- **`TestServer` time-control methods** (`testing.go`): `AdvanceTime(d time.Duration)`, `SetTime(t time.Time)`, and `SetScale(scale float64)` allow in-process integration tests to drive the simulated clock directly without HTTP calls. Fixes #206.

## [v0.36.9] - 2026-03-18

### Fixed

- **`TimeController` live accelerated clock** (`types.go`): `SetScale` stored the multiplier but `Now()` ignored it, so `SetScale(86400)` had no observable effect. The implementation now uses a (simulated baseline, wall baseline) pair: `Now()` returns `simBaseline + (wall_now − wallBaseline) × scale`. `SetTime` and `SetScale` both reset the wall baseline atomically so changes take effect immediately without a discontinuous jump. A scale of 3600 makes one real second equal one simulated hour; 86400 makes one real second equal one simulated day. Manual `SetTime` (for deterministic replay) is unaffected.

## [v0.36.8] - 2026-03-18

### Fixed

- **EC2 `RunInstances` `TagSpecifications`** (`ec2_plugin.go`): tags specified in `TagSpecifications` with `ResourceType=instance` at launch time were silently dropped. They are now parsed from `TagSpecification.N.Tag.M.Key/Value` params and stored on the instance, so `DescribeInstances` returns them immediately after launch. Fixes issue #205.

## [v0.36.7] - 2026-03-18

### Fixed

- **CloudWatch `GetMetricData` Smithy RPC v2 CBOR protocol** (`parser.go`, `cloudwatch_plugin.go`): AWS SDK Go v2 cloudwatch v1.55+ sends `GetMetricData` via the Smithy RPC v2 CBOR transport (`POST /service/GraniteServiceVersion20100801/operation/GetMetricData`, `Content-Type: application/cbor`). Two fixes: (1) the parser now maps the Smithy service ID `GraniteServiceVersion20100801` to `monitoring` via a new `smithyServiceAliases` table; (2) the operation is extracted from the `/operation/<Name>` URL segment as a new 3rd strategy in `extractOperation`; (3) `getMetricData` detects `Content-Type: application/cbor` and returns an empty CBOR map `{}` (`0xa0`) instead of XML. Fixes issue #204.

## [v0.36.6] - 2026-03-18

### Added

- **EC2: `CreateImage` / `DescribeImages` (AMIs) / `DeregisterImage`** (`ec2_plugin.go`, `ec2_types.go`): full AMI lifecycle — `CreateImage` stores an AMI in state (state immediately `"available"`, tags supported via `TagSpecification.N.Tag.M.*`); `DescribeImages` lists AMIs by account/region with `tag:<key>` filter support; `DeregisterImage` removes the AMI. Fixes issue #203.
- **CloudWatch: `GetMetricData`** (`cloudwatch_plugin.go`): returns a valid empty `MetricDataResults` response. Callers that degrade gracefully on zero values work correctly. Fixes issue #202.
- **IAM: `ListInstanceProfiles`** (`iam_plugin.go`): returns an empty `InstanceProfiles` list with `IsTruncated: false`. Fixes issue #201.

## [v0.36.5] - 2026-03-17

### Fixed

- **Parser: empty-value query params** (`parser.go`): bare keys (e.g. `?uploads`, `?versions`) were correctly stored as `"1"`, but keys with an explicit empty value (e.g. `?prefix=`) were incorrectly also stored as `"1"`. The fix inspects the raw query string to distinguish the two cases, so `ListObjectsV2` with an empty `Prefix` now receives `""` as intended (issue #200).

## [v0.36.4] - 2026-03-17

### Added

- **EC2: `RebootInstances`** — no-op returning 200 OK (issue #193)
- **EC2: `CreateTags` / `DeleteTags`** — applies/removes tags on instances, VPCs, subnets, security groups, internet gateways, and route tables; `DescribeInstances` now includes tags in the `tagSet` XML element (issue #194)
- **EC2: `ModifyInstanceAttribute`** — supports `InstanceType.Value` changes; updated instance type is reflected in subsequent `DescribeInstances` responses (issue #195)
- **S3: `DeleteObjects`** — `POST /<bucket>?delete` multi-object delete; supports `<Quiet>true</Quiet>` to suppress the `<Deleted>` list in the response (issue #197)

### Fixed

- **Presigned URL service identification** (`parser.go`): when no `Authorization` header is present (presigned requests), the `X-Amz-Credential` query parameter is used to synthesise a credential scope, enabling correct service and region identification for all presigned S3 (and other) URLs (issue #196)
- **Budgets `CreateBudget`** (`parser.go`): the `AWSBudgetServiceGateway` X-Amz-Target prefix (used by mutation operations) was not in the alias table; only `AmazonBudgetServiceGateway` (used by `DescribeBudgets`) was. Added `"awsbudgetservicegateway": "budgets"` alias (issue #199)
- **`ListObjectsV2` `KeyCount`** (`s3_plugin.go`): `KeyCount` now correctly equals `len(Contents) + len(CommonPrefixes)` rather than `len(Contents)` only; the existing `CommonPrefixes` logic was correct (issue #198)

## [v0.36.3] - 2026-03-17

### Added

- **EC2 key pair operations** (`ec2_plugin.go`, `ec2_types.go`): `CreateKeyPair` (generates an EC P-256 key pair, returns PEM `KeyMaterial` and SHA-256 fingerprint), `DescribeKeyPairs` (with optional `KeyName` filter), `DeleteKeyPair` (by name or `KeyPairId`), and `ImportKeyPair` (accepts base64-encoded public key material). `RunInstances` now records `KeyName` on the instance and includes it in the `RunInstancesResponse` XML. Fixes issue #192.

## [v0.36.2] - 2026-03-17

### Fixed

- **S3 virtual-hosted style with `config.WithBaseEndpoint`** (`parser.go`): AWS SDK v2 prepends the bucket name to the custom base-endpoint host (e.g. `my-bucket.localhost:4566`). `normalizeS3VirtualHost` only handled `.amazonaws.com` hosts, so the bucket was never prepended to the request path and Substrate returned a 501. A new helper `normalizeS3CustomEndpointVirtualHost` fires after the service is identified as `"s3"` (via SigV4 credential scope from v0.36.1) and strips the first DNS label as the bucket name, normalising the path for all S3 plugins. Path-style requests (`localhost:4566/bucket/key`) already worked after v0.36.1. Fixes issue #191.

## [v0.36.1] - 2026-03-17

### Fixed

- **Service identification with `config.WithBaseEndpoint`** (`parser.go`): query-protocol services (STS, EC2, IAM, CloudWatch, ELB, …) now resolve correctly when the SDK is configured with a single base endpoint URL (e.g. `http://localhost:8080`) instead of per-service hostnames. `extractService` gains a 4th strategy: reads the service name from the SigV4 `Authorization` credential scope (`…/<region>/<service>/aws4_request`). Added `"ses"→"sesv2"` alias to cover SES v2 SigV4 scope. Fixes issue #190.

## [v0.36.0] - 2026-03-16

### Added

- **Lambda Docker execution** (`lambda_exec.go`): `LambdaExecutor` manages warm Lambda RIE containers via Docker CLI; supports ZIP-deployed and container-image functions; warm pool with configurable TTL; gracefully falls back to stub response `{"statusCode":200,"body":"null"}` when Docker is unavailable (issues #111, #112)
- **Lambda replay cache** (`lambda_exec.go`): `saveReplay`/`loadReplay` persist invocation results to state keyed by `sha256(functionARN|payload)`; `ReplayMode: "recorded"` returns cached responses without Docker (issue #113)
- **SQS ESM polling** (`lambda_plugin.go`): `createEventSourceMapping` starts a `sqsPollerLoop` goroutine when the ESM is `Enabled` and targets SQS; polls via `registry.RouteRequest`, invokes Lambda with an SQS records event, and deletes messages on 2xx; `Shutdown` stops all pollers (issue #115)
- **API Gateway proxy plugin** (`apigateway_plugin.go`): `APIGatewayProxyPlugin` (service `"execute-api"`) handles runtime invocations at `{apiId}.execute-api.{region}.amazonaws.com`; resolves v1 REST APIs and v2 HTTP APIs to their `AWS_PROXY` Lambda integrations; builds v1 and v2 proxy event payloads and parses proxy response shapes (issue #114)
- **RDS Postgres container executor** (`rds_exec.go`): `RDSExecutor.StartPostgres` launches `postgres:latest` via Docker, polls `pg_isready`, and stores the handle; `StopAll` / `StopContainer` clean up containers; `rds_plugin.go` wires the executor when `cfg.RDS.Engine == "container"` (issue #136)
- `LambdaCfg` and `RDSCfg` config structs in `config.go`; viper defaults (`lambda.docker_enabled=false`, `lambda.replay_mode=live`, `lambda.warm_pool_ttl=5m`, `rds.engine=stub`)
- `parser.go`: `extractServiceFromHost` now recognises `{apiId}.execute-api.{region}.amazonaws.com` and routes to the `execute-api` plugin; `extractRegionFromHost` correctly parses region from execute-api hosts; `ParseAWSRequest` injects `Host` header into the headers map so plugins can access it
- `RegisterDefaultPlugins` gains a `cfg *Config` parameter (nil-safe) for Docker feature wiring; callers updated

### Changed

- `LambdaPlugin.Initialize` accepts `"lambda_exec"` and `"registry"` options to enable Docker execution and ESM polling
- `RDSPlugin.Initialize` accepts `"rds_executor"` option to enable container-backed instances

## [v0.35.0] - 2026-03-16

### Added

- **Embedded debug web UI** (`ui.html`, `debug_ui.go`): single-file vanilla-JS SPA served at `GET /ui`; tabs for Events, State, Diff, Costs, and Export; left-sidebar service filter; clicking an event row loads state at that sequence point (issue #156)
- **`GET /v1/debug/events`**: returns a filtered event list (query params: `?service=`, `?stream=`, `?limit=`, `?after=`); bodies are stripped to keep the payload small (issue #156)
- **`GET /v1/debug/events/{seq}/state`**: replays all events up to sequence N into a fresh in-memory state and returns the snapshot as JSON; powered by the new `stateAtSequence` private helper (issue #157)
- **`GET /v1/debug/state/diff`**: computes a symmetric JSON diff between two sequence points (`?from=`, `?to=`); returns `{"added":…,"removed":…,"changed":…}` (issue #157)
- **`GET /v1/debug/costs`**: returns `CostSummary` JSON for an account or stream (`?account=`, `?stream=`) (issue #158)
- **`GET /v1/debug/export`**: generates and returns a standalone Go `*_test.go` file that replays recorded requests via `StartTestServer` (`?stream=`, `?package=`, `?test=`) (issue #159)
- **`GenerateTestFixture`** (`replay_export.go`): exported function that converts a `[]*Event` slice into a gofmt-formatted Go test file using `text/template` + `go/format` (issue #159)

## [v0.34.0] - 2026-03-15

### Added

- **RDS Aurora cluster support** (`rds_plugin.go`, `rds_types.go`): `CreateDBCluster`, `DescribeDBClusters`, `DeleteDBCluster` operations; `RDSDBCluster` type with writer/reader endpoints; `DBClusterAlreadyExistsFault` and `DBClusterNotFoundFault` error codes; state key `dbcluster:{acct}/{region}/{id}` (issue #133)
- **RDS `RestoreDBInstanceFromDBSnapshot`** (`rds_plugin.go`): restores a DB instance from an existing snapshot, copying engine and allocated storage; returns HTTP 200 with new instance details (issue #188)
- **`MSKPlugin`** — 39th built-in plugin; supports `CreateCluster`, `DescribeCluster`, `GetBootstrapBrokers`, `ListClusters`, `DeleteCluster` via MSK REST/JSON API at `/v1/clusters/...`; `ConflictException` (409) on duplicate cluster; `NotFoundException` (404) on missing cluster; synthetic broker endpoints for `GetBootstrapBrokers` (issue #138)
- `msk_types.go` — `MSKCluster`, `MSKBrokerNodeGroupInfo`, `MSKStorageInfo`, `MSKEBSStorageInfo` types; `mskNamespace = "msk"` constant; state key `cluster:{acct}/{region}/{name}`, index `cluster_ids:{acct}/{region}`
- `parser.go`: `"kafka": "msk"` alias so `Kafka_20181101.{Op}` target routes to the MSK plugin
- **SESv2 email capture** (`sesv2_plugin.go`, `sesv2_types.go`): `sendEmail` now persists a `SESv2CapturedEmail` to state (`captured_email:{acct}/{region}/{msgID}`) with To, From, Subject, and Body fields for test assertions (issue #140)
- **`GET /v1/emails` assertion endpoint** (`server.go`): lists all captured SESv2 outbound emails as JSON; accepts optional `?to=` and `?subject=` substring filters; returns `{"Emails": [...], "Count": N}`
- `betty_cfn_v34_plugins.go`: `deployRDSDBCluster` helper for `AWS::RDS::DBCluster` (priority 3, Ref = DBClusterIdentifier, GetAtt `Endpoint.Address`); `deployMSKCluster` helper for `AWS::MSK::Cluster` (priority 3, Ref = ClusterARN)
- `betty_cfn.go`: `AWS::RDS::DBCluster` at priority 3 and `AWS::MSK::Cluster` at priority 3 added to `typePriority` map and dispatch switch
- `costs.go`: `rds/CreateDBCluster` ($0.0001), `rds/RestoreDBInstanceFromDBSnapshot` ($0.0001), `msk/CreateCluster` ($0.0002), `msk/GetBootstrapBrokers` ($0.000001)

## [v0.32.0] - 2026-03-15

### Added

- **Generic CFN fallback** (`betty_cfn.go`): unknown `AWS::*` resource types now produce a synthetic ARN and store their properties in the `cfn_stub` state namespace instead of being silently skipped; `Ref` resolves to the logical ID (issue #146)
- `betty_cfn_v32_plugins.go`: eleven stub deploy helpers for new CFN resource types — `deployOpenSearchDomain` (`AWS::OpenSearchService::Domain`, priority 2), `deployWAFv2WebACL` (`AWS::WAFv2::WebACL`, priority 2), `deployBackupBackupPlan` (`AWS::Backup::BackupPlan`, priority 2), `deployCodeBuildProject` (`AWS::CodeBuild::Project`, priority 2), `deployCodePipelinePipeline` (`AWS::CodePipeline::Pipeline`, priority 3), `deployCodeDeployDeploymentGroup` (`AWS::CodeDeploy::DeploymentGroup`, priority 3), `deployCloudTrailTrail` (`AWS::CloudTrail::Trail`, priority 2), `deployConfigConfigRule` (`AWS::Config::ConfigRule`, priority 3), `deployConfigConfigurationRecorder` (`AWS::Config::ConfigurationRecorder`, priority 2), `deployTransferServer` (`AWS::Transfer::Server`, priority 2), `deployAthenaWorkGroup` (`AWS::Athena::WorkGroup`, priority 2) (issues #147–#150)
- `betty_cfn_v32_test.go`: 13 table-driven tests covering the generic fallback, all 11 new stub types, and a Glue regression guard
- `cfnStubNamespace = "cfn_stub"` constant for generic resource property storage

## [v0.31.0] - 2026-03-15

### Added

- `AppSyncPlugin` — 39th built-in plugin; supports `CreateGraphqlApi`, `ListGraphqlApis`, `GetGraphqlApi`, `UpdateGraphqlApi`, `DeleteGraphqlApi`, `CreateDataSource`, `ListDataSources`, `GetDataSource`, `UpdateDataSource`, `DeleteDataSource`, `CreateResolver`, `ListResolvers`, `GetResolver`, `UpdateResolver`, `DeleteResolver`, `CreateFunction`, `ListFunctions`, `GetFunction`, `DeleteFunction`, `StartSchemaCreation`, `GetIntrospectionSchema`, and `ExecuteGraphQL` (stub) (issues #142–#145)
- `appsync_types.go` — `AppSyncGraphQLApi`, `AppSyncDataSource`, `AppSyncResolver`, `AppSyncFunction` types; `parseAppSyncOperation` path router; state key helpers; `generateAppSyncAPIID` / `generateAppSyncFunctionID`
- `parser.go`: `extractServiceFromHost` now recognises AppSync execution endpoints (`{apiId}.appsync-api.{region}.amazonaws.com`) and routes them to the `appsync` plugin
- `betty_cfn_v31_plugins.go`: `deployAppSyncGraphQLApi`, `deployAppSyncDataSource`, `deployAppSyncResolver`, `deployAppSyncFunction` deploy helpers for `AWS::AppSync::GraphQLApi` (priority 2), `AWS::AppSync::DataSource` (priority 3), `AWS::AppSync::Resolver` / `AWS::AppSync::FunctionConfiguration` (priority 4)
- `costs.go`: AppSync pricing — `appsync/ExecuteGraphQL` and `appsync/CreateGraphqlApi` at $0.000004 each ($4.00 per million operations)
- `doc.go`: updated plugin count from 38 to 39 and added AppSync to service description

## [v0.30.0] - 2026-03-15

### Added

- `ServiceQuotasPlugin` — 38th built-in plugin; supports `ListServices`, `ListServiceQuotas`, `GetServiceQuota`, `GetAWSDefaultServiceQuota`, `RequestServiceQuotaIncrease`, `ListRequestedServiceQuotaChangesByService`, and `GetRequestedServiceQuotaChange`; covers Lambda, S3, DynamoDB, SQS, and nine other AWS services (issue #119)
- `servicequotas_types.go` — `ServiceQuota` and `QuotaIncrease` types with built-in default quota table
- **S3 versioning** (`s3_plugin.go`): `PutBucketVersioning`, `GetBucketVersioning`, `ListObjectVersions`; version-aware `PutObject` (generates `x-amz-version-id`), `GetObject` (accepts `?versionId`), and `DeleteObject` (delete markers + permanent version deletion) (issue #126)
- **S3 lifecycle** (`s3_plugin.go`): `PutBucketLifecycleConfiguration`, `GetBucketLifecycleConfiguration`, `DeleteBucketLifecycle` — config round-trip storage (issue #127)
- **SQS FIFO** (`sqs_plugin.go`): `MessageGroupId` enforcement, 5-minute deduplication window via `MessageDeduplicationId` or content-based SHA-256, `sqsFIFODedupEntry` state type (issue #128)
- **DynamoDB Streams** (`dynamodb_plugin.go`): ring-buffer stream records (max 1000) with `appendStreamRecord` hooks in `putItem`/`updateItem`/`deleteItem`; real implementations of `DescribeStream`, `GetShardIterator` (supports TRIM_HORIZON/LATEST/AT_SEQUENCE_NUMBER/AFTER_SEQUENCE_NUMBER), and `GetRecords` replacing previous stubs; `DynamoDBStreamRecord` and `DynamoDBStreamCursor` types (issue #129)
- **DynamoDB PartiQL** (`dynamodb_plugin.go`): `ExecuteStatement` and `BatchExecuteStatement` with `tokenizePartiQL` supporting `SELECT * FROM`, `INSERT INTO … VALUE`, `UPDATE … SET`, and `DELETE FROM` (issue #130)
- **Lambda ESM** (`lambda_plugin.go`): `CreateEventSourceMapping`, `ListEventSourceMappings`, `GetEventSourceMapping`, `UpdateEventSourceMapping`, `DeleteEventSourceMapping`; `ESMConfig` type with `esm:{uuid}` and `esm_ids:{functionARN}` state keys (issue #131)
- `betty_cfn.go`: `deployLambdaEventSourceMapping` helper for `AWS::Lambda::EventSourceMapping` CFN resources (priority 5); `deployS3Bucket` extended to call `PutBucketVersioning` when `VersioningConfiguration.Status = Enabled`
- `doc.go`: updated plugin count from 37 to 38 and expanded service description

## [v0.29.0] - 2026-03-15

### Added

- `docker-compose.yml` — turnkey local development deployment; SQLite state persisted in a named Docker volume (issue #187)
- `configs/substrate-local.yaml` — ready-to-use Substrate config mounted into the Compose container (issue #187)
- `deploy/ecs/task-definition.json` — ECS Fargate task definition template with EFS volume and CloudWatch logging (issue #187)
- `deploy/ecs/README.md` — step-by-step ECS Fargate + ALB deployment guide (issue #187)
- `deploy/k8s/deployment.yaml` — Kubernetes Deployment + ClusterIP Service (issue #187)
- `deploy/k8s/configmap.yaml` — Substrate config as a Kubernetes ConfigMap (issue #187)
- `deploy/k8s/pvc.yaml` — PersistentVolumeClaim for SQLite data (issue #187)
- `deploy/README.md` — comparison table and quickstarts for all three deployment options (issue #187)
- `Makefile`: `compose-up`, `compose-down`, `compose-logs` targets (issue #187)
- `docs/getting-started.md`: Docker Compose quickstart added as first Install option (issue #187)

### Fixed

- `cmd/substrate/main.go`: `TimeController` is now constructed before `EventStore` and passed via `WithTimeController`; the server clock and event-store clock are now the same instance (issue #187)

## [v0.28.0] - 2026-03-15

### Added

- SES v2 plugin (issue #180): CreateEmailIdentity, ListEmailIdentities, GetEmailIdentity, SendEmail, DeleteEmailIdentity
- Kinesis Data Firehose plugin (issue #181): CreateDeliveryStream, DescribeDeliveryStream, PutRecord, PutRecordBatch, ListDeliveryStreams, DeleteDeliveryStream
- Betty CFN: AWS::SES::EmailIdentity, AWS::KinesisFirehose::DeliveryStream (issue #182)
- Documentation overhaul: README service matrix updated to all 37 plugins (issue #175)
- docs/getting-started.md: new first-user tutorial (issue #176)
- docs/services.md: complete service reference for all 37 plugins (issue #177)
- docs/testing-guide.md: Go testing patterns guide (issue #178)
- `Server.Serve(ctx, net.Listener)` — accepts an already-bound listener, eliminating the port TOCTOU race in `StartTestServer` (issue #183)
- `WithTimeController(tc)` EventStoreOption — event timestamps and cost-forecast windows now use the simulated clock rather than `time.Now()` (issue #185)

### Fixed

- `StartTestServer` now passes the open `net.Listener` directly to `srv.Serve`, eliminating the TOCTOU race between port reservation and bind (issue #183)
- IAMPlugin and Route53Plugin now use `TimeController.Now()` for all business-visible timestamps (CreateDate, SubmittedAt) instead of `time.Now()` (issue #184)
- `GetCostForecast` now uses `EventStore.now()` (respects the controlled clock) for the observation window and `ComputedAt` field (issue #185)
- `authz.go` SQS ARN builder: guard against empty `name` segment after splitting a trailing-slash `QueueUrl`, preventing a spurious `""` queue name lookup (issue #186)

## [v0.27.2] - 2026-03-14

### Fixed

- **CI lint:** Pinned golangci-lint to v2.11.3 in `.github/workflows/ci.yml` and removed `install-mode: goinstall`. The v2 pre-built binary supports Go 1.26 natively and correctly validates the v2 config schema (`version`, `formatters`, `linters.default`) used in `.golangci.yml`.

## [v0.27.1] - 2026-03-14

### Fixed

- **CI lint:** `golangci-lint` pre-built binary v1.64.8 (compiled with Go 1.24) rejected `go 1.26` in `go.mod`. Fixed by setting `install-mode: goinstall` in `.github/workflows/ci.yml` so golangci-lint is compiled from source with the installed Go 1.26.
- **CI e2e:** `test/e2e/go.mod` was missing OTel and gRPC transitive dependencies introduced since v0.17.0; `go test` failed with `go: updates to go.mod needed`. Fixed by running `go mod tidy` and committing the result; added an explicit tidy step to the e2e CI job to prevent future drift.

## [v0.27.0] - 2026-03-14

### Added

- **Cost Explorer plugin:** `CEPlugin` handles JSON-target requests (`X-Amz-Target: AWSInsightsIndexService.{Op}`) on `ce.us-east-1.amazonaws.com`. Parser alias `"awsinsightsindexservice" → "ce"`. Operations: `GetCostAndUsage` (derives per-service cost buckets from `EventStore.GetCostSummary`), `GetCostForecast` (uses linear-regression projection from `EventStore.GetCostForecast`), `GetDimensionValues` (scans EventStore for unique service names). No persistent state — all data derived from EventStore. `RegisterDefaultPlugins` now accepts an optional `store *EventStore` parameter passed to CEPlugin; passing `nil` returns valid empty responses (#121).

- **Budgets plugin:** `BudgetsPlugin` handles JSON-target requests (`X-Amz-Target: AmazonBudgetServiceGateway.{Op}`) on `budgets.amazonaws.com`. Parser alias `"budgetservicegateway" → "budgets"`. Operations: `CreateBudget`, `DescribeBudgets`, `DescribeBudget`, `UpdateBudget`, `DeleteBudget`. State keys: `budget:{acct}/{name}`, `budget_names:{acct}`. Error code for not-found: `NotFoundException`; duplicate: `DuplicateRecordException`. Cost entry: `budgets/CreateBudget = $0.00001` (#122).

- **Health plugin:** `HealthPlugin` provides a stub of the AWS Health API. Parser alias `"healthservice" → "health"`. Operations: `DescribeEvents`, `DescribeEventDetails`, `DescribeAffectedEntities`, `DescribeEventAggregates` — all return valid empty responses satisfying the SDK shape. No persistent state, no cost entries (#123).

- **Organizations plugin:** `OrganizationsPlugin` handles JSON-target requests (`X-Amz-Target: Organizations_20161128.{Op}`) on `organizations.*.amazonaws.com`. Operations: `DescribeOrganization` (auto-creates org + master account on first call), `ListAccounts`, `DescribeAccount`, `ListRoots` (returns single root with SCP enabled), `CreateAccount` (status `SUCCEEDED`). State keys: `org:{acct}`, `account:{id}`, `account_ids:{acct}`. Error code for not-found: `AccountNotFoundException` (#124).

- **Betty CFN: AWS::Budgets::Budget:** `deployBudgetsBudget` helper in `betty_cfn_v27_plugins.go`. CFN type priority 3. `Ref` resolves to `BudgetName` (#125).

## [v0.26.0] - 2026-03-14

### Added

- **EFS plugin:** `EFSPlugin` handles REST/JSON requests on `elasticfilesystem.{region}.amazonaws.com` at `/2015-02-01/...` paths. Operations: file system CRUD (`CreateFileSystem` returns HTTP 201, `DescribeFileSystems`, `UpdateFileSystem`, `DeleteFileSystem` returns 204), access point CRUD (`CreateAccessPoint`, `DescribeAccessPoints`, `DeleteAccessPoint` returns 204), mount target CRUD (`CreateMountTarget`, `DescribeMountTargets`, `DeleteMountTarget` returns 204), tagging (`TagResource`, `ListTagsForResource`, `UntagResource`). File systems start in `available` state immediately. `parseEFSOperation` maps HTTP method + path to operation names. Tags use `[]EFSTag{Key, Value}` slice. Cost entries: `elasticfilesystem/CreateFileSystem = $0.00003`, `elasticfilesystem/CreateAccessPoint = $0.00001`, `elasticfilesystem/CreateMountTarget = $0.00001`.

- **Glue plugin:** `GluePlugin` handles JSON-target requests (`X-Amz-Target: AWSGlue.{Op}`) on `glue.{region}.amazonaws.com`. Operations: database CRUD (`CreateDatabase`, `GetDatabase`, `GetDatabases`, `UpdateDatabase`, `DeleteDatabase`), table CRUD (`CreateTable`, `GetTable`, `GetTables`, `UpdateTable`, `DeleteTable`), connection CRUD (`CreateConnection`, `GetConnection`, `GetConnections`, `UpdateConnection`, `DeleteConnection`), crawler CRUD + start/stop (`CreateCrawler`, `GetCrawler`, `GetCrawlers`, `StartCrawler`, `StopCrawler`, `UpdateCrawler`, `DeleteCrawler`), job CRUD + runs (`CreateJob`, `GetJob`, `GetJobs`, `UpdateJob`, `DeleteJob`, `StartJobRun`, `GetJobRun`, `GetJobRuns`), tagging (`TagResource`, `UntagResource`, `GetTags`). All resources created immediately in final state (crawlers in `READY`, job runs in `SUCCEEDED`). Tags use `map[string]string`. Error code for not-found: `EntityNotFoundException`. Cost entries: `glue/CreateDatabase = $0.00002`, `glue/CreateJob = $0.0001`, `glue/StartJobRun = $0.0001`, `glue/CreateCrawler = $0.0001`.

- **Betty CFN: EFS and Glue resource types:** `deployResource` switch extended with `AWS::EFS::FileSystem` (priority 2), `AWS::EFS::AccessPoint` (priority 3), `AWS::EFS::MountTarget` (priority 4), `AWS::Glue::Database` (priority 2), `AWS::Glue::Connection` (priority 2), `AWS::Glue::Table` (priority 3), `AWS::Glue::Crawler` (priority 3), `AWS::Glue::Job` (priority 3). EFS resources set `PhysicalID` and `ARN` from response body.

- **Tagging API:** Extended `GetResources`, `TagResources`, `UntagResources` with scan functions for EFS file systems (`elasticfilesystem:file-system`) and Glue databases (`glue:database`). `resolveARN` handles `elasticfilesystem` (file-system and access-point) and `glue` (database, job, crawler, connection) ARN formats. `mergeTags` handles EFS (uses `[]EFSTag` slice) and Glue (uses `map[string]string`).

- **ABAC `buildResourceARN`:** Added cases for `elasticfilesystem` (extracts resource ID from path `/2015-02-01/file-systems/{id}`) and `glue` (uses `req.Params["Name"]`).

## [v0.25.0] - 2026-03-14

### Added

- **RDS plugin:** `RDSPlugin` handles query-protocol requests on `rds.{region}.amazonaws.com`. Operations: DB instance CRUD (`CreateDBInstance`, `DescribeDBInstances`, `ModifyDBInstance`, `DeleteDBInstance`, `StartDBInstance`, `StopDBInstance`, `RebootDBInstance`), DB snapshot CRUD (`CreateDBSnapshot`, `DescribeDBSnapshots`, `DeleteDBSnapshot`), subnet group CRUD (`CreateDBSubnetGroup`, `DescribeDBSubnetGroups`, `DeleteDBSubnetGroup`), parameter group CRUD (`CreateDBParameterGroup`, `DescribeDBParameterGroups`, `DeleteDBParameterGroup`), tagging (`ListTagsForResource`, `AddTagsToResource`, `RemoveTagsFromResource`). Instances start in `available` status immediately. Engine-appropriate port stubs (3306 MySQL, 5432 Postgres, 1433 MSSQL, etc.). Cost entries: `rds/CreateDBInstance = $0.0001`, `rds/CreateDBSnapshot = $0.00002`, `rds/ModifyDBInstance = $0.0001` (#160, #161).

- **ElastiCache plugin:** `ElastiCachePlugin` handles query-protocol requests on `elasticache.{region}.amazonaws.com`. Operations: cache cluster CRUD (`CreateCacheCluster`, `DescribeCacheClusters`, `ModifyCacheCluster`, `DeleteCacheCluster`), replication group CRUD (`CreateReplicationGroup`, `DescribeReplicationGroups`, `ModifyReplicationGroup`, `DeleteReplicationGroup`), subnet group CRUD, parameter group CRUD, tagging. Clusters start in `available` status. Redis (port 6379) and Memcached (port 11211) endpoints. Cost entries: `elasticache/CreateCacheCluster = $0.0001`, `elasticache/CreateReplicationGroup = $0.0001` (#163, #164).

- **Betty CFN: RDS and ElastiCache resource types:** `deployResource` switch extended with `AWS::RDS::DBSubnetGroup` (priority 2), `AWS::RDS::DBParameterGroup` (priority 2), `AWS::RDS::DBInstance` (priority 3), `AWS::ElastiCache::SubnetGroup` (priority 2), `AWS::ElastiCache::ParameterGroup` (priority 2), `AWS::ElastiCache::CacheCluster` (priority 3), `AWS::ElastiCache::ReplicationGroup` (priority 3). GetAtt support: `DBInstance.Endpoint.Address`, `DBInstance.Endpoint.Port`, `CacheCluster.ConfigurationEndpoint.Address`, `CacheCluster.ConfigurationEndpoint.Port`, `CacheCluster.RedisEndPoint.Address`, `CacheCluster.RedisEndPoint.Port`, `ReplicationGroup.PrimaryEndPoint.Address`, `ReplicationGroup.PrimaryEndPoint.Port` (#162, #165).

- **Tagging API:** Extended `GetResources`, `TagResources`, `UntagResources` with scan functions for RDS DB instances (`rds:db`) and ElastiCache clusters (`elasticache:cluster`). ARN resolution and tag merge/remove for both services (#166).

- **ABAC:** `buildResourceARN` in `authz.go` extended with `rds` and `elasticache` cases for attribute-based access control on DB instances and cache clusters (#167).

## [v0.24.0] - 2026-03-14

### Added

- `/_localstack/health` and `/_localstack/info` endpoints returning LocalStack-compatible service status JSON — enables Prism and other tools that poll for service readiness (#109).
- `POST /v1/state/reset` HTTP endpoint for wiping all emulator state between tests (#108).
- `StartTestServer(t *testing.T) *TestServer` Go helper for integration tests — starts an in-process server on a random port, registers all plugins, and registers `t.Cleanup` for automatic shutdown (#108).
- `RegisterDefaultPlugins` exported function extracted from the server binary so testing helpers and custom embeddings can initialise the same plugin set (#108).
- `substratelocal` CLI wrapper binary — injects `AWS_ENDPOINT_URL`, `LOCALSTACK_ENDPOINT`, and stub credentials into child process environment (#107).
- Multi-arch Docker image (`linux/amd64` + `linux/arm64`) build support via `Dockerfile` and `.github/workflows/docker.yml`, published to `ghcr.io/scttfrdmn/substrate` on tag push (#106).
- `docs/endpoint-configuration.md` — endpoint configuration reference for AWS CLI, Go SDK v2, boto3, Terraform, CDK, Prism, and Docker Compose (#110).

## [v0.23.0] - 2026-03-09

### Added

- **Kinesis Data Streams plugin:** `KinesisPlugin` handles JSON-protocol requests (`X-Amz-Target: Kinesis_20131202.{Op}`). Operations: stream CRUD (`CreateStream`, `DeleteStream`, `DescribeStream`, `DescribeStreamSummary`, `ListStreams`), shard iteration (`GetShardIterator`, `GetRecords`), producer operations (`PutRecord`, `PutRecords`), enhanced fan-out (`RegisterStreamConsumer`, `DeregisterStreamConsumer`, `DescribeStreamConsumer`), tagging. Cost entries: `kinesis/PutRecord = $0.000000014`, `kinesis/PutRecords = $0.000000014`.

- **CloudFront plugin:** `CloudFrontPlugin` handles REST/XML requests on `cloudfront.amazonaws.com` (global service). Operations: distribution CRUD (`CreateDistribution` → HTTP 201, `GetDistribution`, `UpdateDistribution`, `DeleteDistribution`), `ListDistributions`, CloudFront Origin Access Identity (OAI) CRUD, tagging. Distributions start in `InProgress` state. GetAtt `Distribution.DomainName` supported.

- **Betty CFN: Kinesis and CloudFront resource types:** `deployResource` switch extended with `AWS::Kinesis::Stream` (priority 2), `AWS::CloudFront::CloudFrontOriginAccessIdentity` (priority 2), `AWS::CloudFront::Distribution` (priority 3). GetAtt `Distribution.DomainName` and `Stream.StreamArn` supported.

- **Tagging API: scan and resolve for Kinesis:** `TaggingPlugin` now scans Kinesis streams. `resolveARN` and `mergeTags` extended to handle `kinesis` namespace.

- **ABAC: `buildResourceARN` for Kinesis:** `authz.go` `buildResourceARN` extended with case for `kinesis`.

- **Kinesis and CloudFront plugins registered in `cmd/substrate/main.go`.**

## [v0.22.0] - 2026-03-09

### Added

- **Cognito User Pools plugin:** `CognitoIDPPlugin` handles JSON-protocol requests (`X-Amz-Target: AWSCognitoIdentityProviderService.{Op}`). Operations: user pool CRUD, user pool client CRUD, domain, groups, admin user management, `ListUsers`, `InitiateAuth` (stub JWT tokens), `RespondToAuthChallenge`, `SignUp`/`ConfirmSignUp`, MFA config. Pool IDs use format `{region}_{12-char alphanum}`. Cost entry: `cognito-idp/InitiateAuth = $0.000055`.

- **Cognito Identity Pools plugin:** `CognitoIdentityPlugin` handles JSON-protocol requests (`X-Amz-Target: AWSCognitoIdentityService.{Op}`). Operations: identity pool CRUD, `GetId`, `GetCredentialsForIdentity` (stub AWS credentials), `SetIdentityPoolRoles`/`GetIdentityPoolRoles`.

- **Betty CFN: Cognito resource types:** `deployResource` switch extended with `AWS::Cognito::UserPool`, `AWS::Cognito::UserPoolClient`, `AWS::Cognito::UserPoolGroup`, `AWS::Cognito::UserPoolDomain`, `AWS::Cognito::IdentityPool`, `AWS::Cognito::IdentityPoolRoleAttachment`. GetAtt `UserPool.ProviderName` and `UserPool.ProviderURL` supported.

- **Tagging API: scan and resolve for Cognito:** `TaggingPlugin` now scans Cognito user pools. `resolveARN` and `mergeTags` extended to handle `cognito-idp` namespace.

- **ABAC: `buildResourceARN` for Cognito:** `authz.go` `buildResourceARN` extended with cases for `cognito-idp` and `cognito-identity`. Parser alias `"awscognitoidentityproviderservice" → "cognito-idp"` added.

- **CognitoIDP and CognitoIdentity plugins registered in `cmd/substrate/main.go`.**

## [v0.21.0] - 2026-03-09

### Added

- **ECR plugin:** `ECRPlugin` handles JSON-protocol requests (`X-Amz-Target: AmazonEC2ContainerRegistry_V1_1_0.{Op}`). Operations: repository CRUD, `PutImage`, `BatchGetImage`, `DescribeImages`, `BatchDeleteImage`, `ListImages`, `GetAuthorizationToken`, lifecycle policy, repository policy, tagging. Cost entry: `ecr/PutImage = $0.000001`.

- **ECS plugin:** `ECSPlugin` handles JSON-protocol requests (`X-Amz-Target: AmazonEC2ContainerServiceV20141113.{Op}`). Operations: cluster CRUD, task definition register/deregister/describe/list, service create/update/describe/delete/list, `RunTask`/`StopTask`/`DescribeTasks`/`ListTasks`, tagging. Cost entry: `ecs/RunTask = $0.000025`.

- **Betty CFN: ECR and ECS resource types:** `deployResource` switch extended with `AWS::ECR::Repository`, `AWS::ECR::LifecyclePolicy`, `AWS::ECS::Cluster`, `AWS::ECS::TaskDefinition`, `AWS::ECS::Service`, `AWS::ECS::CapacityProvider`. GetAtt `ECRRepository.RepositoryUri` supported.

- **Tagging API: scan and resolve for ECR and ECS:** `TaggingPlugin` now scans ECR repositories and ECS clusters. `resolveARN` and `mergeTags` extended to handle `ecr` and `ecs` namespaces. Helper functions `ecsTagsToTaggingTags` and `mergeECSTags` added.

- **ABAC: `buildResourceARN` for ECR and ECS:** `authz.go` `buildResourceARN` extended with cases for `ecr` and `ecs`. Parser aliases `"ec2containerservicev20141113" → "ecs"` and `"ec2containerregistry" → "ecr"` added.

- **ECR and ECS plugins registered in `cmd/substrate/main.go`.**

## [v0.20.0] - 2026-03-09

### Added

- **Step Functions plugin:** `StepFunctionsPlugin` handles JSON-protocol requests (`X-Amz-Target: AmazonStates.{Op}`). Operations: state machine CRUD, `StartExecution` (returns RUNNING), `StopExecution`, `DescribeExecution` (RUNNING → SUCCEEDED on first call), `ListExecutions`, `GetExecutionHistory` (stub events), Activity CRUD, `TagResource`/`UntagResource`/`ListTagsForResource`. Cost entry: `states/StartExecution = $0.000025`.

- **Betty CFN: Step Functions resource types:** `deployResource` switch extended with `AWS::StepFunctions::StateMachine` (priority 4) and `AWS::StepFunctions::Activity` (priority 3). GetAtt `StateMachine.Name` supported.

- **Tagging API: scan and resolve for Step Functions:** `TaggingPlugin` now scans Step Functions state machines. `resolveARN` and `mergeTags` extended to handle `states` namespace.

- **ABAC: `buildResourceARN` for Step Functions:** `authz.go` `buildResourceARN` extended with case for `states`.

- **StepFunctions plugin registered in `cmd/substrate/main.go`.**

## [v0.19.0] - 2026-03-09

### Added

- **ACM plugin:** `ACMPlugin` handles JSON-protocol requests (`X-Amz-Target: CertificateManager.{Op}`). Operations: `RequestCertificate` (immediately sets status `ISSUED`), `DescribeCertificate`, `DeleteCertificate`, `ListCertificates`, `AddTagsToCertificate`, `RemoveTagsFromCertificate`, `ListTagsForCertificate`, `RenewCertificate` (no-op). Parser alias `"certificatemanager" → "acm"` added.

- **API Gateway v1 plugin:** `APIGatewayPlugin` handles path-based REST API requests on `apigateway.{region}.amazonaws.com`. Operations cover RestApis, Resources, Methods, Integrations, Deployments, Stages, Authorizers, ApiKeys, UsagePlans, DomainNames, and BasePathMappings. `CreateRestApi` auto-creates a root resource `/` and returns `RootResourceId`. `CreateStage` returns `InvokeURL` via Betty CFN GetAtt. Cost entry: `apigateway/CreateDeployment = $0.0000035`.

- **API Gateway v2 plugin:** `APIGatewayV2Plugin` handles HTTP/WebSocket API requests on `apigatewayv2.{region}.amazonaws.com` at `/v2/apis/...`. Operations cover Apis, Routes, Integrations, Stages, Authorizers, Deployments, DomainNames, and ApiMappings. Cost entry: `apigatewayv2/CreateApi = $0.000001`.

- **Betty CFN: API Gateway and ACM resource types:** `deployResource` switch extended with 15 new resource types for `AWS::CertificateManager::Certificate`, `AWS::ApiGateway::*`, and `AWS::ApiGatewayV2::*`. Key GetAtts: `RestApi.RootResourceId`, `Stage.InvokeURL`. `DeployedResource` gains `Metadata map[string]interface{}` field for GetAtt-resolvable attributes.

- **Tagging API: scan and resolve for API Gateway:** `TaggingPlugin` now scans API Gateway REST APIs. `resolveARN` and `mergeTags` extended to handle `apigateway` namespace.

- **ABAC: `buildResourceARN` for new services:** `authz.go` `buildResourceARN` extended with cases for `apigateway`, `apigatewayv2`, and `acm`.

- **ACM, APIGateway, APIGatewayV2 plugins registered in `cmd/substrate/main.go`:** registered in dependency order after CloudWatch.

## [v0.18.0] - 2026-03-09

### Added

- **(#67) CloudWatch Logs plugin:** New `CloudWatchLogsPlugin` handles JSON-protocol (`application/x-amz-json-1.1`) requests identified by `X-Amz-Target: Logs_20140328.{Op}`. Operations: `CreateLogGroup`, `DeleteLogGroup`, `DescribeLogGroups` (prefix filter + base64 pagination), `CreateLogStream`, `DeleteLogStream`, `DescribeLogStreams`, `PutLogEvents`, `GetLogEvents` (time-range filter + pagination), `FilterLogEvents` (multi-stream substring pattern match). State keys: `loggroup:{acct}/{region}/{name}`, `logstream:{acct}/{region}/{group}/{stream}`, `logevents:{acct}/{region}/{group}/{stream}`. Package-level helpers `updateStringIndex`, `removeFromStringIndex`, `loadStringIndex` manage sorted `[]string` JSON indexes used by all three observability plugins.

- **(#68) EventBridge plugin:** New `EventBridgePlugin` handles JSON-protocol requests identified by `X-Amz-Target: AmazonEventBridge.{Op}`. Parser alias `"events" → "eventbridge"` added to `targetServiceAliases` (applied in both `extractServiceFromTarget` and `extractServiceFromHost`). Operations: `PutRule`, `DeleteRule` (validates no targets attached), `ListRules` (prefix filter + pagination), `DescribeRule`, `PutTargets` (merge by ID), `RemoveTargets`, `ListTargetsByRule`, `PutEvents` (validates Source/DetailType/Detail; ring buffer of last 100 events), `ListEventBuses` (returns default bus).

- **(#69) CloudWatch Alarms plugin:** New `CloudWatchPlugin` handles query-protocol (`Action=` param) requests on `monitoring.{region}.amazonaws.com`. Service name: `"monitoring"`. `"monitoring"` added to the server query→JSON rewrite condition. XML responses under `http://monitoring.amazonaws.com/doc/2010-08-01/` namespace. Operations: `PutMetricAlarm` (initial state `INSUFFICIENT_DATA`; preserves existing state on update), `DeleteAlarms` (bulk by `AlarmNames.member.N`), `DescribeAlarms` (filter by name list and/or `StateValue`), `DescribeAlarmsForMetric` (filter by `MetricName`+`Namespace`), `SetAlarmState`, `EnableAlarmActions`, `DisableAlarmActions`. Helper `parseMemberList` extracts query-style `.member.N` arrays.

- **(#70) Lambda auto-creates CloudWatch Logs log group:** `LambdaPlugin.createFunction` now calls `autoCreateLambdaLogGroup` after storing the function. This writes the `/aws/lambda/{name}` log group directly to state (bypassing the registry to avoid circular dependency), matching real AWS behaviour.

- **(#71) Betty CFN: Logs, Events, CloudWatch resource types:** `deployResource` switch extended with `AWS::Logs::LogGroup` (priority 2, dispatches `CreateLogGroup`), `AWS::Logs::LogStream` (priority 3), `AWS::Events::Rule` (priority 4, dispatches `PutRule`), `AWS::CloudWatch::Alarm` (priority 4, dispatches `PutMetricAlarm`). All four set `PhysicalID` and `ARN` so `Ref` and `GetAtt Arn` resolve correctly in subsequent resources.

- **(#72) Cost entries for observability services:** `logs/PutLogEvents` $0.0000005 (~$0.50/GB approximate), `eventbridge/PutEvents` $0.000001 ($1.00/M events), `monitoring/PutMetricAlarm` $0.10 ($0.10/alarm/month charged on creation).

## [v0.17.0] - 2026-03-09

### Added

- **(#65) Request latency histogram:** `MetricsCollector` now tracks `substrate_request_duration_seconds` as a Prometheus histogram with 12 default buckets (1 ms–10 s). New `RecordLatency(service, operation string, d time.Duration)` method; wired in `server.go` after every request completes. Histogram output follows the standard Prometheus text format with cumulative `_bucket`, `_sum`, and `_count` lines sorted by service/operation.

- **(#66) Enriched OTel spans:** `Tracer.StartRequest` now returns `(context.Context, trace.Span)` instead of `(context.Context, func())`, exposing the live span to `server.go` for attribute decoration and error recording. `server.go` sets `aws.region` and `aws.account_id` attributes immediately after span creation. New package-level `RecordSpanError(span trace.Span, err error)` helper (nil-safe) marks the span with `codes.Error` and calls `span.RecordError`; called on any non-nil `routeErr`.

## [v0.16.0] - 2026-03-09

### Added

- **(v0.16.0) SNS plugin:** New `SNSPlugin` handles query-protocol requests on `sns.{region}.amazonaws.com`. Operations: `CreateTopic` (idempotent), `DeleteTopic`, `ListTopics`, `Subscribe`, `Unsubscribe`, `ListSubscriptions`, `ListSubscriptionsByTopic`, `Publish` (fan-out to SQS `SendMessage` and Lambda POST `/invocations`), `SetTopicAttributes`, `GetTopicAttributes`, `TagResource`, `UntagResource`, `ListTagsForResource`. S3 `fireNotifications` extended to dispatch `TopicConfigurations` to SNS. Betty CFN support: `AWS::SNS::Topic` (priority 3), `AWS::SNS::Subscription` (priority 4), `AWS::SNS::TopicPolicy` (priority 4). Cost entry added for `sns/Publish`.

- **(v0.16.0) Secrets Manager plugin:** New `SecretsManagerPlugin` handles JSON-protocol (`application/x-amz-json-1.1`) requests identified by `X-Amz-Target: secretsmanager.{Op}`. Operations: `CreateSecret` (409 on duplicate), `GetSecretValue` (404 on missing), `PutSecretValue` (generates new versionID each call), `DeleteSecret`, `ListSecrets`, `DescribeSecret`, `UpdateSecret`, `RotateSecret` (sets `RotationEnabled: true`), `TagResource`, `UntagResource`. Betty CFN support: `AWS::SecretsManager::Secret` (priority 2), `AWS::SecretsManager::RotationSchedule` (priority 5), `AWS::SecretsManager::SecretTargetAttachment` (priority 5, stub). Cost entries added for `CreateSecret`, `GetSecretValue`, `PutSecretValue`.

- **(v0.16.0) SSM Parameter Store plugin:** New `SSMPlugin` handles JSON-protocol requests identified by `X-Amz-Target: AmazonSSM.{Op}`. Operations: `PutParameter` (versioning, 409 `ParameterAlreadyExists` without `Overwrite`), `GetParameter` (404 on missing), `GetParameters` (returns `Parameters` + `InvalidParameters`), `DeleteParameter`, `DeleteParameters`, `GetParametersByPath` (recursive/non-recursive, base64 pagination), `DescribeParameters`, `AddTagsToResource`, `RemoveTagsFromResource`, `ListTagsForResource`. Betty CFN support: `AWS::SSM::Parameter` (priority 2), `AWS::SSM::Association` (priority 5, stub). `resolveDynamicRef` added to `StackDeployer` for `{{resolve:ssm:/path}}` and `{{resolve:ssm-secure:/path}}` dynamic references. Cost entries added for `GetParameter`, `GetParameters`, `GetParametersByPath`, `PutParameter`.

- **(v0.16.0) KMS plugin:** New `KMSPlugin` handles JSON-protocol requests identified by `X-Amz-Target: TrentService.{Op}`. Parser alias `"trentservice" → "kms"` added to `targetServiceAliases`. Operations: `CreateKey`, `DescribeKey`, `ListKeys`, `EnableKey`, `DisableKey`, `ScheduleKeyDeletion`, `CancelKeyDeletion`, `Encrypt`, `Decrypt`, `GenerateDataKey`, `GenerateDataKeyWithoutPlaintext`, `ReEncrypt`, `CreateAlias`, `DeleteAlias`, `ListAliases`, `UpdateAlias`, `PutKeyPolicy`, `GetKeyPolicy`, `EnableKeyRotation`, `DisableKeyRotation`, `GetKeyRotationStatus`, `TagResource`, `UntagResource`, `ListResourceTags`. Stub crypto: `kmsEncryptStub`/`kmsDecryptStub` use `base64(kms:{keyID}:{base64(plaintext)})` for deterministic testing. Betty CFN support: `AWS::KMS::Key` (priority 1), `AWS::KMS::Alias` (priority 2), `AWS::KMS::ReplicaKey` (priority 2, stub). Cost entries added for `CreateKey`, `Encrypt`, `Decrypt`, `GenerateDataKey`, `ReEncrypt`.

- **(v0.16.0) `writeError` protocol fix:** `server.go` `writeError` now uses `strings.HasPrefix(ct, "application/x-amz-json")` to match both `1.0` and `1.1` content types, ensuring error responses are JSON for all JSON-protocol services (KMS, SSM, Secrets Manager). SNS added to the query→JSON rewrite condition in `server.go`.

- **(v0.16.0) Betty CFN `resolveFnGetAtt` extended:** `GetAtt` attribute routing for `AWS::SNS::Topic` → `TopicArn`, `AWS::KMS::Key` → `KeyArn`/`Arn`, `AWS::SSM::Parameter` → `Value` (returns physical parameter name). `resolveDynamicRef` added for SSM dynamic references.

- **(#59) ELBv2 plugin:** New `ELBPlugin` handling query-protocol requests on `elasticloadbalancing.{region}.amazonaws.com`. Load balancer operations: `CreateLoadBalancer`, `DescribeLoadBalancers`, `DeleteLoadBalancer`, `DescribeLoadBalancerAttributes`, `ModifyLoadBalancerAttributes`. Target group operations: `CreateTargetGroup`, `DescribeTargetGroups`, `DeleteTargetGroup`, `ModifyTargetGroup`. Target registration: `RegisterTargets`, `DeregisterTargets`, `DescribeTargetHealth` (always returns healthy). Listener operations: `CreateListener`, `DescribeListeners`, `DeleteListener`, `ModifyListener`. Rule operations: `CreateRule`, `DescribeRules`, `DeleteRule`, `SetRulePriorities`. Supports both `application` and `network` load balancer types. Betty CFN support added for `AWS::ElasticLoadBalancingV2::TargetGroup`, `AWS::ElasticLoadBalancingV2::LoadBalancer`, `AWS::ElasticLoadBalancingV2::Listener`, `AWS::ElasticLoadBalancingV2::ListenerRule`. Cost entries added for `CreateLoadBalancer` and `RegisterTargets`.

- **(#60) Route 53 plugin:** New `Route53Plugin` handling REST/XML requests on `route53.amazonaws.com`. Hosted zone operations: `CreateHostedZone` (returns 201), `ListHostedZones`, `GetHostedZone`, `DeleteHostedZone`. Record set operations: `ChangeResourceRecordSets` (CREATE/UPSERT/DELETE actions, returns `INSYNC` immediately), `ListResourceRecordSets`. Supports A, AAAA, CNAME, MX, NS, SOA, TXT record types and alias records. Operation routing via `parseRoute53Operation(method, path)` for path-based REST dispatch. Betty CFN support added for `AWS::Route53::HostedZone`, `AWS::Route53::RecordSet`, `AWS::Route53::RecordSetGroup`. Cost entries added for `CreateHostedZone` and `ChangeResourceRecordSets`.

- **(#56) ABAC condition keys:** `AuthController.CheckAccess` now populates `aws:ResourceTag/*` and `aws:RequestTag/*` IAM condition keys so policies can allow or deny based on resource tags or request-time tags. Resource tags are loaded from state for S3, Lambda, SQS, DynamoDB, EC2 (instances), and IAM (users/roles). Request tags are parsed from `x-amz-tagging` headers (S3), JSON bodies (IAM, Lambda), and query params (EC2 `TagSpecification.*`). `buildResourceARN` extended to produce full ARNs for EC2, Lambda, DynamoDB, and SQS requests. `DynamoDBTable` gains a `Tags map[string]string` field.

- **(#57) Resource Groups Tagging API:** New `TaggingPlugin` handles `tagging.{region}.amazonaws.com` requests identified by `X-Amz-Target: ResourceGroupsTaggingAPI_20170126.{Op}`. Supported operations: `GetResources` (with `TagFilters`, `ResourceTypeFilters`, and cursor-based pagination), `TagResources` (merge tags onto any supported resource), `UntagResources` (remove tag keys). Resources covered: S3 buckets, Lambda functions, SQS queues, DynamoDB tables, EC2 instances, IAM users and roles. Parser alias `"resourcegroupstaggingapi" → "tagging"` added to `targetServiceAliases`. `TaggingPlugin` registered in `cmd/substrate/main.go`.



- **(#52) Config hot-reload via SIGHUP:** `QuotaController.UpdateConfig`, `ConsistencyController.UpdateConfig`, `CostController.UpdateConfig`, and `FaultController.UpdateConfig` allow in-place config replacement without server restart. `cmd/substrate/main.go` installs a SIGHUP handler that reloads `substrate.yaml` and calls each controller's `UpdateConfig`.

- **(#53) EventStore bulk export:** `EventStore.ExportNDJSON` streams all matching events as newline-delimited JSON; `EventStore.ExportCSV` writes RFC 4180 CSV with an 11-column header. New `substrate export` CLI subcommand with `--format` (ndjson/csv), `--output`, `--stream`, `--service`, `--start`, `--end` flags.

- **(#55) Per-service tagging:** Lambda `TagResource`, `UntagResource`, `ListTags` via `/2015-03-31/tags/{arn}` paths. S3 `PutBucketTagging`, `GetBucketTagging`, `DeleteBucketTagging`, `PutObjectTagging`, `GetObjectTagging`, `DeleteObjectTagging` using XML `<Tagging><TagSet>` format; `S3Object.Tags` field added. IAM `TagUser`, `UntagUser`, `ListUserTags`, `TagRole`, `UntagRole`, `ListRoleTags`.

- **(#19) Fault injection middleware:** New `FaultController` with `FaultConfig` / `FaultRule` types. Rules match by service and/or operation, fire probabilistically (0.0–1.0), and inject either an `AWSError` (error fault) or a `time.Sleep` latency delay. Seeded per-instance PRNG for deterministic test replay. `fault` section added to `Config` and `substrate.yaml.example`. Server pipeline integrates fault injection between consistency check and plugin dispatch.

- **(#50 + #58) EC2 + VPC plugin:** New `EC2Plugin` handling query-protocol requests on `ec2.{region}.amazonaws.com`. Instance operations: `RunInstances`, `DescribeInstances` (with `Filter.N.*` support), `TerminateInstances`, `StopInstances`, `StartInstances`, `DescribeInstanceStatus`. VPC operations: `CreateVpc`, `DescribeVpcs`, `DeleteVpc`, `CreateSubnet`, `DescribeSubnets`, `DeleteSubnet`, `CreateSecurityGroup`, `DescribeSecurityGroups`, `DeleteSecurityGroup`, `AuthorizeSecurityGroupIngress/Egress`, `RevokeSecurityGroupIngress/Egress`, `CreateInternetGateway`, `DescribeInternetGateways`, `AttachInternetGateway`, `DetachInternetGateway`, `DeleteInternetGateway`, `CreateRouteTable`, `DescribeRouteTables`, `AssociateRouteTable`, `DisassociateRouteTable`, `CreateRoute`, `DeleteRoute`, `DeleteRouteTable`. Default VPC (`172.31.0.0/16`) auto-created on `RunInstances` when no `SubnetId` supplied. All state keys are region-scoped (`instance:{acct}/{region}/{id}`).

- **(#51) Multi-region routing:** `RegionCfg` added to `Config` with `default` and optional `allowed` allowlist. When `allowed` is non-empty, requests with unlisted regions receive `400 InvalidClientTokenId`. `region` section added to `substrate.yaml.example`.

- **(#54) Terraform plan validation:** `ParseTerraformPlan` decodes `terraform show -json` output; `ValidateTerraformPlan` estimates monthly cost and flags policy concerns. New `TerraformValidation` type with `EstimatedMonthlyCostUSD`, `ResourceCount`, `CreatedResources`, `DeletedResources`, `Warnings`, `Errors`. New `substrate validate-plan` CLI subcommand reads a JSON plan file and prints cost estimate and warnings.

- **Betty CFN EC2/VPC support:** `betty_cfn.go` now handles `AWS::EC2::VPC` (priority 1), `AWS::EC2::Subnet`, `AWS::EC2::SecurityGroup`, `AWS::EC2::InternetGateway` (priority 2), `AWS::EC2::RouteTable`, `AWS::EC2::Instance` (priority 3) resource types via new `deployEC2*` functions.

### Added

- **(#46) Prometheus metrics endpoint** (`/metrics`): hand-rolled Prometheus text-format v0.0.4 emitter with no external dependencies. New `MetricsCollector` type tracks `substrate_requests_total`, `substrate_request_errors_total`, `substrate_quota_hits_total`, `substrate_consistency_delays_total`, `substrate_cost_usd_total`, and `substrate_events_total`. Enabled via `metrics.enabled: true` in config; `MetricsCfg` added to `Config`; `/metrics` path registered before the `/*` catch-all.

- **(#49) Cost forecasting** via `EventStore.GetCostForecast`: linear regression on historical per-day cost buckets with a 95% confidence interval (±1.96σ), fallback to mean for fewer than 3 data points, and Z-score anomaly detection with a configurable sigma threshold. New types `CostForecast`, `DailyCost`, `CostAnomaly`. `ForecastCfg` added to `Config` with `forecast` section in `substrate.yaml.example`.

- **(#48) Plugin developer guide** in `doc_plugins.go` covering the `Plugin` interface, `PluginConfig`, state key naming conventions, `AWSRequest`/`AWSResponse`/`AWSError` shapes, unit-testing patterns, and integration-test patterns. New `examples/custom_plugin/main.go` demonstrates a minimal "weather" service plugin. `doc.go` extended with a `# Plugin Development` section cross-referencing both files.

- **(#47) OpenTelemetry distributed tracing** via `NewTracer`: supports `noop`, `stdout`, and `otlp_http` exporters. New `Tracer` type with `StartSpan` and `StartRequest` helpers. `TracingConfig` and `TracingCfg` structs added; `Tracer` field added to `ServerOptions`; `tracing` section added to `substrate.yaml.example`. Uses `go.opentelemetry.io/otel` v1.42.0.

- **DynamoDB table lifecycle (#43):** New `DynamoDBPlugin` implements the DynamoDB JSON-protocol
  API (`X-Amz-Target: DynamoDB_20120810.{Operation}`). Supports full table lifecycle:
  `CreateTable` (status `ACTIVE` immediately), `DeleteTable`, `DescribeTable`, `ListTables`
  (paginated via `ExclusiveStartTableName` + `Limit`), and `UpdateTable`. Table ARNs are generated
  as `arn:aws:dynamodb:{region}:{account}:table/{name}`. State stored under the `dynamodb`
  namespace using keys `table:{acct}/{name}`, `table_names:{acct}`, `item:{acct}/{tbl}/{key}`,
  `item_keys:{acct}/{tbl}`.

- **DynamoDB item CRUD and batch operations (#43):** `PutItem` (with `ConditionExpression` and
  `ReturnValues=ALL_OLD`), `GetItem` (with `ProjectionExpression`), `DeleteItem` (with
  `ConditionExpression` and `ReturnValues`), `UpdateItem` (SET/REMOVE/ADD/DELETE
  `UpdateExpression` clauses with arithmetic, set union/subtraction, and all `ReturnValues`
  modes), `BatchGetItem`, and `BatchWriteItem`. Item key encoding: hash-only PK uses `pkVal`;
  hash+range uses `pkVal#skVal`. `server.go` `writeError` extended to return JSON errors for
  `application/x-amz-json-1.0` requests.

- **DynamoDB Query and Scan (#44):** `Scan` and `Query` support `FilterExpression`,
  `ProjectionExpression`, `Limit`, `ExclusiveStartKey` pagination, and `IndexName` for
  GSI/LSI access. `Query` parses `KeyConditionExpression` (PK equality plus SK conditions `=`,
  `<`, `<=`, `>`, `>=`, `BETWEEN`, `begins_with`) and honours `ScanIndexForward`. A
  token-based recursive-descent expression evaluator handles comparisons (`=`, `<>`, `<`, `<=`,
  `>`, `>=`), logical operators (`AND`, `OR`, `NOT`), parentheses, `BETWEEN`, `IN`, and
  functions (`attribute_exists`, `attribute_not_exists`, `begins_with`, `contains`,
  `attribute_type`, `size`). Nested dotted-path attribute access supported (e.g.,
  `Meta.Region`).

- **DynamoDB GSI, LSI, TTL, and Streams stubs (#45):** `CreateTable` accepts
  `GlobalSecondaryIndexes`, `LocalSecondaryIndexes`, and `StreamSpecification`. `Query` and
  `Scan` route through `findIndexKeySchema` to use the correct key schema for the named index.
  `UpdateTimeToLive` / `DescribeTimeToLive` manage TTL attribute on the table. `ListStreams`,
  `DescribeStream`, `GetShardIterator`, and `GetRecords` provide stub stream support.

- **CloudFormation DynamoDB support (#43):** `betty_cfn.go` maps `AWS::DynamoDB::Table` to
  `CreateTable` (priority 2, deploys alongside S3). All CFN properties are forwarded:
  `KeySchema`, `AttributeDefinitions`, `BillingMode`, `ProvisionedThroughput`,
  `GlobalSecondaryIndexes`, `LocalSecondaryIndexes`, `StreamSpecification`. If
  `TimeToLiveSpecification` is present, `UpdateTimeToLive` is called automatically after table
  creation. `Ref` resolves to `TableName`; `Fn::GetAtt TableArn` resolves to the table ARN.

- **DynamoDB cost tracking:** `defaultCostTable` in `costs.go` now includes entries for
  `dynamodb/PutItem`, `dynamodb/UpdateItem`, `dynamodb/DeleteItem`, `dynamodb/BatchWriteItem`
  ($0.00000125 each) and `dynamodb/Query`, `dynamodb/Scan`, `dynamodb/BatchGetItem`
  ($0.00000025 each). `GetItem` was already present.

- **CLI DynamoDB registration:** `cmd/substrate/main.go` registers `DynamoDBPlugin` after SQS
  and before S3 in the server plugin chain.

## [v0.10.0] - 2026-03-08

### Added

- **Lambda function emulation (#40):** New `LambdaPlugin` implements the Lambda REST API
  (`/2015-03-31/functions/…`). Supports `CreateFunction`, `GetFunction`, `UpdateFunctionCode`,
  `UpdateFunctionConfiguration`, `DeleteFunction`, `ListFunctions` (paginated), `Invoke` (stub
  synchronous response), `InvokeAsync`, `AddPermission`, `RemovePermission`, `GetPolicy`, and
  `PutFunctionEventInvokeConfig`. State is stored under the `lambda` namespace. `LambdaPlugin` is
  registered automatically in the server CLI.

- **SQS queue emulation (#41):** New `SQSPlugin` implements the SQS query-protocol API. Supports
  `CreateQueue` (idempotent), `DeleteQueue`, `GetQueueUrl`, `GetQueueAttributes`,
  `SetQueueAttributes`, `ListQueues` (prefix-filtered), `TagQueue`, `UntagQueue`, `ListQueueTags`,
  `SendMessage`, `SendMessageBatch`, `ReceiveMessage` (with `VisibilityTimeout` and `DelaySeconds`
  via `TimeController`), `DeleteMessage`, `DeleteMessageBatch`, `ChangeMessageVisibility`, and
  `PurgeQueue`. Queue URLs use the local format `http://sqs.{region}.localhost/{accountID}/{name}`.
  `server.go` now includes `sqs` in the query-protocol → JSON body rewrite path. SQS cost entries
  (`sqs/SendMessage`, `sqs/ReceiveMessage` at $0.0000004 each) added to `defaultCostTable`.

- **S3 event notifications (#22):** `S3Plugin` gains `GetBucketNotificationConfiguration` and
  `PutBucketNotificationConfiguration` operations (via `?notification` query param). After each
  successful `PutObject` or `DeleteObject`, `fireNotifications` dispatches to configured Lambda
  functions (via `lambda/Invoke`) and SQS queues (via `sqs/SendMessage`) with an S3 event payload
  matching the AWS `2.1` schema. Prefix/suffix key filters and wildcard event patterns
  (`s3:ObjectCreated:*`) are supported. The `S3Plugin` accepts an optional `"registry"` key in
  `PluginConfig.Options` to enable dispatch; nil disables notifications without error.

- **Enhanced CloudFormation support (#42):** `cfnTemplate` now parses `Parameters`, `Conditions`,
  and `Outputs` sections. `StackDeployer.Deploy` accepts an optional `params map[string]string`
  argument that overrides template parameter defaults. Intrinsic functions supported:
  `Ref`, `Fn::Sub` (string and `[template, vars]` forms), `Fn::Join`, `Fn::Select`, `Fn::Split`,
  `Fn::Base64`, `Fn::GetAtt`, `Fn::If`. Condition operators: `Fn::Equals`, `Fn::Not`, `Fn::And`,
  `Fn::Or`. Resources with a false `Condition` field are skipped. `DeployResult.Outputs` is
  populated from the resolved Outputs section. Stack state is persisted under the `cfn` namespace
  via a new `CFNStackState` type. New methods: `StackDeployer.UpdateStack`,
  `StackDeployer.DeleteStack`, `StackDeployer.ListStacks`. `AWS::Lambda::Function` now dispatches
  to the Lambda plugin (no longer a stub). New `AWS::SQS::Queue` resource type supported.
  `NewStackDeployer` constructor added for direct instantiation in tests.

## [v0.9.0] - 2026-03-08

### Added

- **Multi-account credential registry (#36):** New `CredentialRegistry` and `CredentialEntry` types
  provide a thread-safe store mapping AWS access key IDs to accounts and secrets. A built-in test
  credential (`AKIATEST12345678901` → account `123456789012`) is pre-loaded by `NewCredentialRegistry`.
  `ServerOptions.Credentials` wires the registry into the request pipeline so the caller's account ID
  and principal ARN are resolved from the `Authorization` header on every request.

- **SigV4 request signature verification (#35):** `VerifySigV4` validates AWS4-HMAC-SHA256 signatures
  against secret keys from the `CredentialRegistry`. The server pre-reads the request body once and
  restores it before parsing; the SigV4 check runs in the pipeline after credential resolution and
  returns `InvalidClientTokenId` (403) for unknown keys or `SignatureDoesNotMatch` (403) for bad
  signatures. Passing `nil` as the registry disables verification (backward-compatible default).

- **IAM inline policies (#38):** `IAMPlugin` now handles `PutUserPolicy`, `GetUserPolicy`,
  `DeleteUserPolicy`, `ListUserPolicies`, `PutRolePolicy`, `GetRolePolicy`, `DeleteRolePolicy`, and
  `ListRolePolicies`. Inline policy documents are stored in state under
  `user_inline:{name}:{policyName}` / `role_inline:{name}:{policyName}` keys; a sorted name index
  is maintained under `user_inline_names:{name}` / `role_inline_names:{name}`.

- **IAM permission boundaries (#38):** `IAMUser` and `IAMRole` each gain a `PermissionsBoundary`
  field (`*IAMAttachedPolicy`). `IAMPlugin` handles `PutUserPermissionsBoundary`,
  `DeleteUserPermissionsBoundary`, `PutRolePermissionsBoundary`, and `DeleteRolePermissionsBoundary`.
  The `authorize` function enforces AWS boundary semantics: effective access = Allow in BOTH identity
  policies AND boundary policy. The `AdministratorAccess` fast path no longer bypasses the boundary
  check.

- **Cross-service IAM enforcement (#37):** New `AuthController` type (created via
  `NewAuthController`) inspects the caller principal on every request and evaluates attached managed
  policies, inline policies, and permission boundaries via the existing `Evaluate` engine.
  `ServerOptions.Auth` wires the controller into the pipeline before quota/consistency checks.
  `cmd/substrate/main.go` instantiates and wires `AuthController` automatically.

- **S3 bucket policies and object ACLs (#39):** `S3Plugin` now handles `GetBucketPolicy`,
  `PutBucketPolicy`, `DeleteBucketPolicy`, `GetBucketAcl`, `PutBucketAcl`, `GetObjectAcl`, and
  `PutObjectAcl`. Bucket policies are stored as raw JSON under `bucket_policy:{bucket}`; ACLs as
  `S3AccessControlList` XML under `bucket_acl:{bucket}` and `object_acl:{bucket}/{key}`. Canned ACL
  values (`private`, `public-read`, `public-read-write`, `authenticated-read`) are supported via the
  `x-amz-acl` header. New `S3BucketPolicy`, `S3AccessControlList`, `S3Owner`, `S3Grant`, and
  `S3Grantee` types added to `s3_types.go`.

## [v0.8.0] - 2026-03-07

### Added

- **TimeController race fix (#33):** Added `sync.RWMutex` to `TimeController`; `Now()` acquires
  a read-lock and `SetTime()`/`SetScale()` acquire a write-lock. New `types_test.go` verifies
  zero races under 50-goroutine concurrent access (`go test -race`).

- **PluginRegistry thread safety (#32):** Added `sync.RWMutex` to `PluginRegistry`; `Register`
  acquires a write-lock, `RouteRequest` acquires a read-lock. New `Names()` method returns a
  sorted slice of registered service names (used by `/ready`).

- **Health and readiness endpoints (#32):** `GET /health` returns
  `{"status":"ok","version":"<version>"}` always 200; `GET /ready` returns
  `{"status":"ok","plugins":["iam","s3",...]}` always 200. Both paths are configurable via
  `server.health_path` / `server.ready_path` (default `/health` / `/ready`). Neither endpoint
  is recorded in the EventStore. `server_test.go` gains four new tests. The E2E test now polls
  `/health` instead of sleeping.

- **`Version` package variable (#32):** `doc.go` exports `var Version = "dev"` set at build time
  via `-X github.com/scttfrdmn/substrate.Version=$(VERSION)` (Makefile updated).

- **EventStore in-memory service/operation indexes (#34):** `byService` and `byOperation`
  maps are populated in `RecordEvent`; `GetEvents` calls the new private `selectSource` which
  selects the narrowest index for single-field Service or Operation filters. New benchmark
  `BenchmarkEventStore_FilterByService` (10,000 events, 3 services) demonstrates the speedup.

- **EventStoreOption variadic options pattern (#34):** `NewEventStore` now accepts
  `...EventStoreOption`; `WithStateManager(sm)` attaches a `StateManager` for async snapshotting.
  All existing callers remain source-compatible.

- **Async snapshot goroutine (#30):** When `EventStoreConfig.SnapshotInterval > 0` and a
  `StateManager` is provided via `WithStateManager`, `NewEventStore` launches a `snapshotLoop`
  goroutine. `RecordEvent` sends a non-blocking hint to the goroutine every N events.
  `Close()` shuts down the goroutine. New tests: `TestEventStore_AsyncSnapshot_CreatesSnapshot`,
  `TestEventStore_Close_NoGoroutine`, `TestEventStore_SnapshotInterval_Zero_Disabled`.

- **File NDJSON backend (#31):** New `eventstore_file.go` with `fileBackend`; `Flush` appends
  only new events as NDJSON lines under `<persist_path>/events/<stream_id>.ndjson`; `Load` reads
  all `*.ndjson` files. Optional rotation when `event_store.max_file_size_mb > 0`. New config
  fields `EventStoreCfg.MaxFileSizeMB` / `EventStoreConfig.MaxFileSizeMB`. New tests:
  `TestEventStore_FilePersistence`, `TestEventStore_FilePersistence_AppendOnly`.

- **SQLite backend (#29):** New `eventstore_sqlite.go` with `sqliteBackend` using pure-Go
  `modernc.org/sqlite v1.37.0` (no CGO). Schema: `events` and `snapshots` tables with indexes
  on `(stream_id, sequence)`, `service`, and `operation`. Lazy init via `initSQLiteBackend`
  (thread-safe `sync.Mutex`). `Flush` uses `INSERT OR IGNORE` for idempotency; `Load` restores
  all events and snapshots into memory. New config fields `EventStoreCfg.DSN` / `EventStoreConfig.DSN`
  (default `"substrate.db"`). New tests: `TestEventStore_SQLitePersistence`,
  `TestEventStore_SQLite_IdempotentFlush`, `TestEventStore_SQLite_SnapshotRoundTrip`.

### Fixed

- `generateEventID` now includes the event's Sequence number to guarantee uniqueness when many
  events are recorded within the same nanosecond (previously SQLite `INSERT OR IGNORE` would
  silently drop events with duplicate IDs).

- `coverage_test.go` SQLite stub tests (`TestEventStore_Flush_NonMemory`,
  `TestEventStore_Load_NonMemory`) now use `t.TempDir()` so they exercise the real SQLite
  backend rather than failing silently.

## [v0.7.0] - 2026-03-07

### Added

- End-to-end tests in `test/e2e/` using the real `aws-sdk-go-v2`: `TestS3_CRUD`
  (CreateBucket/PutObject/GetObject/HeadObject/DeleteObject/DeleteBucket),
  `TestIAM_Lifecycle` (CreateUser/CreateRole/CreatePolicy/Attach/Detach/Delete),
  `TestCostReport_AfterS3Workload` (10× PutObject then cost aggregation check).
  Uses a `serviceTransport` that routes SDK requests to an in-process server while
  preserving the `Host` header for service extraction. Closes #26.
- Four benchmark functions in `benchmarks_test.go`: `BenchmarkEventStore_RecordThroughput`,
  `BenchmarkReplayEngine_Replay`, `BenchmarkServer_HTTPThroughput`, and
  `BenchmarkS3PutObject_Latency`. Run with `make bench`. Closes #27.
- `examples/betty_workflow/main.go`: runnable Betty.codes end-to-end example demonstrating
  plugin wiring, `BettyClient.Deploy`, recording, `StopRecording`/validation, `DebugSession`
  time-travel, and formatted JSON report output. Run with
  `go run examples/betty_workflow/main.go`. Closes #28.
- `Makefile` targets `bench` and `e2e`.
- CI jobs `e2e` and `benchmark` in `.github/workflows/ci.yml`.

### Fixed

- IAM/STS query-protocol body gap: when the real AWS SDK sends
  `application/x-www-form-urlencoded` bodies, `ParseAWSRequest` consumes the
  body via `r.ParseForm` leaving it empty. `handleAWSRequest` now reconstructs
  `req.Body` as JSON from `req.Params` for `iam` and `sts` services, enabling
  plugin JSON unmarshalling to succeed without affecting Betty in-process calls.

- `BettyClient` (`betty.go`): convenience wrapper orchestrating the full Betty.codes
  validation workflow — `Deploy`, `StartRecording`, `StopRecording`, `Validate`, and
  `NewDebugSession` — without requiring an HTTP server. Closes #25.
- `StackDeployer` / `DeployStack` (`betty_cfn.go`): parses JSON or YAML CloudFormation
  templates and creates resources via in-process plugin dispatch.  Deployment order:
  `AWS::IAM::Policy` → `AWS::IAM::Role` → `AWS::S3::Bucket`; unknown types are skipped
  with a warning; `AWS::Lambda::Function` returns `NotImplemented` while the rest of
  the template continues to deploy. New types: `cfnTemplate`, `cfnResource`,
  `DeployedResource`, `DeployResult`, `Intent`. Closes #23.
- `ValidateRecording` (`betty_report.go`): analyses a recorded event stream for cost,
  quota headroom, consistency incidents, and intent violations. New types:
  `ValidationReport`, `ValidationStatus`, `CostBreakdown`, `QuotaCheck`,
  `ConsistencyIncident`, `Suggestion`. Monthly cost projection extrapolated from
  observed request rate; suggestions generated for high cost, consistency incidents,
  and failed events. Closes #24.
- `DebugSession` (`betty_debug.go`): time-travel inspection wrapper over `ReplayEngine`
  with lazy stream loading. Exposes `JumpToEvent`, `StepBackward`, and `InspectState`.

- `S3Plugin`: REST+XML S3 emulator covering 12 core operations —
  `CreateBucket`, `HeadBucket`, `DeleteBucket`, `ListBuckets`,
  `PutObject`, `GetObject`, `HeadObject`, `DeleteObject`, `CopyObject`,
  `ListObjects`, `ListObjectsV2` (with continuation-token pagination and
  common-prefix / delimiter support). Object bodies stored in afero.MemMapFs;
  metadata in StateManager namespace `"s3"`. User-defined metadata round-tripped
  via `X-Amz-Meta-*` headers. `ETag` computed as MD5 hex digest. Closes #20.
- `S3Plugin` multipart upload: `CreateMultipartUpload`, `UploadPart`,
  `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListMultipartUploads`.
  Multi-part ETag uses `"<md5(concat(part_md5s))>-<N>"` format matching AWS.
  Part bodies stored in `/.multipart/<uploadID>/<partNum>` on the afero
  filesystem; cleaned up on complete or abort. Closes #21.
- S3 virtual-hosted-style URL normalisation in `ParseAWSRequest`:
  `mybucket.s3[.<region>].amazonaws.com` is transparently rewritten to
  service `"s3"` with path `/mybucket/…`, so path-style and
  virtual-hosted requests are handled identically by the plugin.
- `AWSRequest.Path`: new field carrying the effective URL path, with the
  bucket prepended for S3 virtual-hosted requests.
- Bare query-key sentinel `"1"` in the parameter parser — `?uploads` and
  `?versions` now map to `params["uploads"]=="1"` as expected by plugins.
- S3 quota defaults: `"s3"` 3500 rps / 5500 burst and
  `"s3/GetObject"` 5500 / 5500, replacing the prior `TODO(#22)` placeholder.
- `isMutating` recognises `Copy`, `Upload`, `Complete`, and `Abort` prefixes
  for consistency-controller tracking of S3 write operations.
- `S3Plugin` registered in `cmd/substrate/main.go` server command.
- `github.com/spf13/afero` promoted from indirect to direct dependency.
- `// TODO(#22)`: event-notification forwarding to Lambda/SQS deferred —
  placeholder comment in `S3Plugin.HandleRequest` default case.

- `QuotaController`: token-bucket rate limiter wired into the server pipeline;
  returns HTTP 429 `ThrottlingException` when a service or operation burst is
  exhausted. Time sourced from `TimeController` for deterministic tests.
  Replay requests bypass quota checks. Default rules mirror AWS service quotas
  (IAM 100 rps / 100 burst, STS 100 rps / 100 burst, AssumeRole 50/50, with
  per-operation overrides for CreateUser/DeleteUser/CreateRole/DeleteRole at
  20/20). S3 prefix-level rules deferred to TODO(#22). Closes #16.
- `ConsistencyController`: eventual-consistency simulation that rejects reads
  within a configurable `PropagationDelay` after a mutating request to the
  same resource key; returns HTTP 409 `InconsistentStateException`. Disabled
  by default. Replay requests are a no-op. Closes #17.
- `CostController`: stateless per-request USD cost estimator backed by a
  built-in pricing table (`s3/PutObject` $0.000005, `s3/GetObject`
  $0.0000004, `dynamodb/GetItem` $0.00000025, `lambda/Invoke` $0.0000002;
  IAM/STS free). Config overrides supported. Closes #18.
- `GetCostSummary`: new `EventStore` method that aggregates `Event.Cost`
  over an account and optional time range, returning a `CostSummary` with
  `TotalCost`, `ByService`, `ByOperation`, and `RequestCount`. Closes #18.
- `ServerOptions`: variadic options struct for `NewServer` that wires
  `QuotaController`, `ConsistencyController`, and `CostController` into the
  request pipeline. Nil fields disable the respective feature.
- `QuotaCfg`, `ConsistencyCfg`, `CostCfg`: YAML config sections with
  `To*Config()` bridge methods following the existing `EventStoreCfg` pattern.
- `substrate.yaml.example` extended with `quotas`, `consistency`, and `costs`
  sections and inline comments.

[Unreleased]: https://github.com/scttfrdmn/substrate/compare/v0.7.0...HEAD
[v0.7.0]: https://github.com/scttfrdmn/substrate/compare/v0.3.0-alpha...v0.7.0

## [v0.3.0-alpha] - 2026-03-08

### Added

- `IAMPlugin`: full IAM JSON-protocol plugin (CreateUser/GetUser/DeleteUser/ListUsers,
  CreateRole/GetRole/DeleteRole/ListRoles, CreateGroup/GetGroup/DeleteGroup/ListGroups,
  AttachUserPolicy/DetachUserPolicy/ListAttachedUserPolicies,
  AttachRolePolicy/DetachRolePolicy/ListAttachedRolePolicies,
  CreatePolicy/GetPolicy/DeletePolicy/ListPolicies,
  CreateAccessKey/DeleteAccessKey/ListAccessKeys) with alphabetically deterministic
  pagination (Marker/MaxItems) and JSON error format (`__type` field) (closes #14).
- `STSPlugin`: STS query-protocol plugin (GetCallerIdentity, AssumeRole,
  GetSessionToken) with TimeController-driven credential expiry and XML responses;
  temporary credentials persisted to state (closes #15).
- `IAMUser`, `IAMRole`, `IAMGroup`, `IAMPolicy`, `IAMAccessKey`, `IAMTag`,
  `IAMAttachedPolicy`: AWS-exact IAM entity types with JSON serialisation (closes #11).
- `PolicyDocument`, `PolicyStatement`, `PolicyPrincipal`, `StringOrSlice`:
  IAM policy document types; `StringOrSlice` and `PolicyPrincipal` implement
  custom JSON marshal/unmarshal to handle AWS's mixed string/array encoding (closes #11).
- `Evaluate`, `EvaluationRequest`, `EvaluationResult`: pure IAM policy evaluation
  engine implementing the AWS evaluation algorithm (explicit deny wins, then allow,
  then implicit deny); supports Action/NotAction, Resource/NotResource, and
  condition operators StringEquals, StringNotEquals, StringLike, StringNotLike,
  ArnEquals, ArnLike, ArnNotEquals, Bool, Null (closes #12).
- `ListManagedPolicies`, `GetManagedPolicy`: 47 bundled AWS managed policies with
  policy documents sourced from the official AWS managed policy reference; lazy-initialised
  lookup map via `sync.Once` (closes #13).
- `STSSessionCredentials`: JSON-persisted session credential type used by STSPlugin.
- `cmd/substrate/main.go`: IAMPlugin and STSPlugin registered in `newServerCmd()`.
- `substrate.yaml.example`: fully-commented server configuration example.
- README Getting Started section: install, run, SDK configuration examples (AWS CLI,
  Go SDK v2, Python boto3, Node.js SDK v3), supported services table, known limitations.
- `Server`: chi-based HTTP server with catch-all AWS request handler, graceful
  shutdown, and event recording on every request (closes #7).
- `ParseAWSRequest`: pure function extracting service, operation, region, and
  account ID from HTTP request headers, Host, Authorization SigV4 scope, and URL
  path (closes #8).
- `Config`, `ServerConfig`, `EventStoreCfg`, `StateCfg`, `LogCfg`: YAML-friendly
  config types with `mapstructure` tags; `LoadConfig` (viper, env overrides via
  `SUBSTRATE_` prefix), `DefaultConfig`, `Validate` (closes #9).
- `EventStoreCfg.ToEventStoreConfig`: bridge from YAML config to `EventStoreConfig`.
- CLI rewritten with cobra: `substrate server`, `substrate replay <stream>`, and
  `substrate debug <stream>` sub-commands; `--version` retained on root (closes #10).
- `Server.ServeHTTP`: exposes the chi router directly for httptest-based testing.
- `MemoryStateManager`: thread-safe in-memory `StateManager` and `SnapshotableStateManager`
  implementation with JSON snapshot/restore and atomic reset (closes #1, #5).
- `SnapshotableStateManager` interface extending `StateManager` with `Snapshot`,
  `Restore`, and `Reset` (closes #5).
- `SlogLogger` and `NewDefaultLogger`: structured logging backed by `log/slog` (closes #4).
- Seeded RNG (`ReplayConfig.RandomSeed`) using `math/rand/v2` PCG source for
  deterministic replay; `ReplayEngine.RandFloat64` and `RandInt64` helpers (closes #6).
- `EventStore`: immutable event log with stream grouping, filtering, and snapshots.
- `ReplayEngine`: deterministic stream replay with time-travel debugging (step
  forward/backward, jump-to-event, breakpoints, state inspection).
- `RecordingSession`: named test-recording sessions.
- Core types: `AWSRequest`, `AWSResponse`, `AWSError`, `RequestContext`,
  `Principal`, `StateManager`, `TimeController`, `Logger`, `Plugin`,
  `PluginConfig`, `PluginRegistry`.
- `JSONSerializer` for event persistence.
- Initial project structure, CI workflow, and tooling.

### Changed

- `cmd/substrate/main.go`: replaced `flag`-based stub with full cobra command tree.
- Managed policy names corrected to match actual AWS names: `AmazonECS_FullAccess`
  (was `AmazonECSFullAccess`), `CloudWatchFullAccess` (was `AmazonCloudWatchFullAccess`),
  `AWSCodePipeline_FullAccess` (was `AWSCodePipelineFullAccess`),
  `AWSCodePipeline_ReadOnlyAccess` (was `AWSCodePipelineReadOnlyAccess`),
  `AmazonCognitoPowerUser` (was `AmazonCognitoIdpFullAccess`).

### Fixed

- `golangci-lint` v2 config: moved `gofmt`/`goimports` to `formatters` section and
  removed `gosimple` (absorbed into `staticcheck` in v2).
- Unused `ctx` parameters renamed to `_` across `eventstore.go` and `replay.go` to
  satisfy `revive` linter.

[Unreleased]: https://github.com/scttfrdmn/substrate/compare/v0.27.2...HEAD
[v0.27.2]: https://github.com/scttfrdmn/substrate/compare/v0.27.1...v0.27.2
[v0.27.1]: https://github.com/scttfrdmn/substrate/compare/v0.27.0...v0.27.1
[v0.27.0]: https://github.com/scttfrdmn/substrate/compare/v0.26.0...v0.27.0
[v0.26.0]: https://github.com/scttfrdmn/substrate/compare/v0.25.0...v0.26.0
[v0.25.0]: https://github.com/scttfrdmn/substrate/compare/v0.24.0...v0.25.0
[v0.24.0]: https://github.com/scttfrdmn/substrate/compare/v0.23.0...v0.24.0
[v0.23.0]: https://github.com/scttfrdmn/substrate/compare/v0.22.0...v0.23.0
[v0.22.0]: https://github.com/scttfrdmn/substrate/compare/v0.21.0...v0.22.0
[v0.21.0]: https://github.com/scttfrdmn/substrate/compare/v0.20.0...v0.21.0
[v0.20.0]: https://github.com/scttfrdmn/substrate/compare/v0.19.0...v0.20.0
[v0.19.0]: https://github.com/scttfrdmn/substrate/compare/v0.18.0...v0.19.0
[v0.18.0]: https://github.com/scttfrdmn/substrate/compare/v0.17.0...v0.18.0
[v0.18.0]: https://github.com/scttfrdmn/substrate/compare/v0.17.0...v0.18.0
[v0.17.0]: https://github.com/scttfrdmn/substrate/compare/v0.16.0...v0.17.0
[v0.16.0]: https://github.com/scttfrdmn/substrate/compare/v0.7.0...v0.16.0
[v0.3.0-alpha]: https://github.com/scttfrdmn/substrate/releases/tag/v0.3.0-alpha
[v0.28.0]: https://github.com/scttfrdmn/substrate/compare/v0.27.2...v0.28.0
[v0.29.0]: https://github.com/scttfrdmn/substrate/compare/v0.28.0...v0.29.0
[v0.30.0]: https://github.com/scttfrdmn/substrate/compare/v0.29.0...v0.30.0
[v0.31.0]: https://github.com/scttfrdmn/substrate/compare/v0.30.0...v0.31.0
[v0.32.0]: https://github.com/scttfrdmn/substrate/compare/v0.31.0...v0.32.0
[v0.34.0]: https://github.com/scttfrdmn/substrate/compare/v0.32.0...v0.34.0
[v0.35.0]: https://github.com/scttfrdmn/substrate/compare/v0.34.0...v0.35.0
[v0.36.0]: https://github.com/scttfrdmn/substrate/compare/v0.35.0...v0.36.0
[v0.36.1]: https://github.com/scttfrdmn/substrate/compare/v0.36.0...v0.36.1
[v0.36.2]: https://github.com/scttfrdmn/substrate/compare/v0.36.1...v0.36.2
[v0.36.3]: https://github.com/scttfrdmn/substrate/compare/v0.36.2...v0.36.3
[v0.36.4]: https://github.com/scttfrdmn/substrate/compare/v0.36.3...v0.36.4
[v0.36.5]: https://github.com/scttfrdmn/substrate/compare/v0.36.4...v0.36.5
[v0.36.6]: https://github.com/scttfrdmn/substrate/compare/v0.36.5...v0.36.6
[v0.36.7]: https://github.com/scttfrdmn/substrate/compare/v0.36.6...v0.36.7
[v0.36.8]: https://github.com/scttfrdmn/substrate/compare/v0.36.7...v0.36.8
[v0.36.9]: https://github.com/scttfrdmn/substrate/compare/v0.36.8...v0.36.9
[v0.36.10]: https://github.com/scttfrdmn/substrate/compare/v0.36.9...v0.36.10
[v0.36.11]: https://github.com/scttfrdmn/substrate/compare/v0.36.10...v0.36.11
[v0.36.12]: https://github.com/scttfrdmn/substrate/compare/v0.36.11...v0.36.12
[v0.36.13]: https://github.com/scttfrdmn/substrate/compare/v0.36.12...v0.36.13
[v0.36.14]: https://github.com/scttfrdmn/substrate/compare/v0.36.13...v0.36.14
[v0.36.15]: https://github.com/scttfrdmn/substrate/compare/v0.36.14...v0.36.15
[v0.36.16]: https://github.com/scttfrdmn/substrate/compare/v0.36.15...v0.36.16
[v0.36.17]: https://github.com/scttfrdmn/substrate/compare/v0.36.16...v0.36.17
[v0.36.18]: https://github.com/scttfrdmn/substrate/compare/v0.36.17...v0.36.18
[v0.36.19]: https://github.com/scttfrdmn/substrate/compare/v0.36.18...v0.36.19
[v0.36.20]: https://github.com/scttfrdmn/substrate/compare/v0.36.19...v0.36.20
[v0.36.21]: https://github.com/scttfrdmn/substrate/compare/v0.36.20...v0.36.21
[v0.37.0]: https://github.com/scttfrdmn/substrate/compare/v0.36.21...v0.37.0
[v0.37.1]: https://github.com/scttfrdmn/substrate/compare/v0.37.0...v0.37.1
[v0.38.0]: https://github.com/scttfrdmn/substrate/compare/v0.37.1...v0.38.0
[v0.39.0]: https://github.com/scttfrdmn/substrate/compare/v0.38.0...v0.39.0
[v0.40.0]: https://github.com/scttfrdmn/substrate/compare/v0.39.0...v0.40.0
[v0.41.0]: https://github.com/scttfrdmn/substrate/compare/v0.40.0...v0.41.0
[v0.41.1]: https://github.com/scttfrdmn/substrate/compare/v0.41.0...v0.41.1
[v0.41.2]: https://github.com/scttfrdmn/substrate/compare/v0.41.1...v0.41.2
[v0.41.3]: https://github.com/scttfrdmn/substrate/compare/v0.41.2...v0.41.3
[v0.42.0]: https://github.com/scttfrdmn/substrate/compare/v0.41.3...v0.42.0
[v0.42.1]: https://github.com/scttfrdmn/substrate/compare/v0.42.0...v0.42.1
[v0.43.0]: https://github.com/scttfrdmn/substrate/compare/v0.42.1...v0.43.0
[v0.43.1]: https://github.com/scttfrdmn/substrate/compare/v0.43.0...v0.43.1
[v0.43.2]: https://github.com/scttfrdmn/substrate/compare/v0.43.1...v0.43.2
[v0.43.3]: https://github.com/scttfrdmn/substrate/compare/v0.43.2...v0.43.3
[v0.43.4]: https://github.com/scttfrdmn/substrate/compare/v0.43.3...v0.43.4
[v0.44.2]: https://github.com/scttfrdmn/substrate/compare/v0.43.4...v0.44.2
[v0.44.3]: https://github.com/scttfrdmn/substrate/compare/v0.44.2...v0.44.3
[v0.44.4]: https://github.com/scttfrdmn/substrate/compare/v0.44.3...v0.44.4
[v0.45.0]: https://github.com/scttfrdmn/substrate/compare/v0.44.4...v0.45.0
[Unreleased]: https://github.com/scttfrdmn/substrate/compare/v0.45.0...HEAD
