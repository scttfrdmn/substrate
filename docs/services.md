# Service Reference

## Coverage matrix

<!-- BEGIN GENERATED COVERAGE MATRIX -->
Substrate ships **65 built-in service plugins**. This section is generated
from the plugin registry (`make docs-reference`), so the count and plugin list
cannot drift from the implementation. The live count is also available from the
`/ready` endpoint (`curl http://localhost:4566/ready`). Per-service operation,
CloudFormation, and cost detail follows below the matrix.

| # | Service | Plugin name | Protocol |
|---|---------|-------------|----------|
| 1 | ACM | `acm` | JSON |
| 2 | API Gateway (REST) | `apigateway` | REST/JSON |
| 3 | API Gateway (HTTP) | `apigatewayv2` | REST/JSON |
| 4 | AppSync | `appsync` | REST/JSON |
| 5 | Athena | `athena` | JSON |
| 6 | Backup | `backup` | REST/JSON |
| 7 | Batch | `batch` | REST/JSON |
| 8 | Bedrock Runtime | `bedrock-runtime` | REST/JSON |
| 9 | Budgets | `budgets` | JSON |
| 10 | Cost Explorer | `ce` | JSON |
| 11 | CloudFormation | `cloudformation` | Query |
| 12 | CloudFront | `cloudfront` | REST/XML |
| 13 | CloudTrail | `cloudtrail` | JSON |
| 14 | CodeBuild | `codebuild` | JSON |
| 15 | CodeDeploy | `codedeploy` | JSON |
| 16 | CodePipeline | `codepipeline` | JSON |
| 17 | Cognito Identity | `cognito-identity` | JSON |
| 18 | Cognito Identity Provider | `cognito-idp` | JSON |
| 19 | DynamoDB | `dynamodb` | JSON |
| 20 | EC2 / VPC | `ec2` | Query |
| 21 | ECR | `ecr` | JSON |
| 22 | ECS | `ecs` | JSON |
| 23 | EFS | `efs` | REST/JSON |
| 24 | ElastiCache | `elasticache` | Query |
| 25 | ELBv2 | `elasticloadbalancing` | Query |
| 26 | EMR Serverless | `emrserverless` | REST/JSON |
| 27 | EventBridge | `eventbridge` | JSON |
| 28 | API Gateway (execute-api) | `execute-api` | REST/JSON |
| 29 | Kinesis Data Firehose | `firehose` | JSON |
| 30 | FSx | `fsx` | JSON |
| 31 | Glue | `glue` | JSON |
| 32 | Health | `health` | JSON |
| 33 | IAM | `iam` | Query |
| 34 | Kinesis Data Streams | `kinesis` | JSON |
| 35 | KMS | `kms` | JSON |
| 36 | Lambda | `lambda` | REST/JSON |
| 37 | CloudWatch Logs | `logs` | JSON |
| 38 | CloudWatch | `monitoring` | Query |
| 39 | MSK | `msk` | REST/JSON |
| 40 | HealthOmics | `omics` | REST/JSON |
| 41 | OpenSearch | `opensearch` | REST/JSON |
| 42 | Organizations | `organizations` | JSON |
| 43 | Price List Query API | `pricing` | JSON |
| 44 | QuickSight | `quicksight` | REST/JSON |
| 45 | RAM | `ram` | REST/JSON |
| 46 | RDS | `rds` | Query |
| 47 | Redshift | `redshift` | Query |
| 48 | Redshift Data API | `redshift-data` | JSON |
| 49 | Route 53 | `route53` | REST/XML |
| 50 | S3 | `s3` | REST/XML |
| 51 | SageMaker | `sagemaker` | JSON |
| 52 | EventBridge Scheduler | `scheduler` | REST/JSON |
| 53 | Secrets Manager | `secretsmanager` | JSON |
| 54 | Service Quotas | `servicequotas` | JSON |
| 55 | SES v2 | `sesv2` | REST/JSON |
| 56 | SNS | `sns` | Query |
| 57 | SQS | `sqs` | JSON |
| 58 | SSM | `ssm` | JSON |
| 59 | SSO / Identity Store | `sso` | REST/JSON |
| 60 | Step Functions | `states` | JSON |
| 61 | STS | `sts` | Query |
| 62 | Resource Groups Tagging | `tagging` | JSON |
| 63 | Timestream | `timestream` | JSON |
| 64 | Transfer Family | `transfer` | JSON |
| 65 | WAFv2 | `wafv2` | JSON |
<!-- END GENERATED COVERAGE MATRIX -->

---

The per-service sections below carry hand-written operation lists,
CloudFormation resource types, and cost notes for the most heavily used plugins.
They are maintained by hand and cover a subset of the plugins in the matrix
above; the remaining plugins are registered and functional but not yet detailed
here.

---

## CloudFormation

**Endpoint:** `cloudformation.{region}.amazonaws.com`
**Protocol:** AWS Query (form-encoded, `Action=` parameter)

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateStack | Deploys every resource in `TemplateBody` and returns the stack ARN; honours `RoleARN` |
| UpdateStack | Re-deploys the template; an omitted `TemplateBody` re-uses the stored one, and an omitted `RoleARN` the stored role |
| DeleteStack | Sweeps the resources the stack deployed, then removes the stack record (see below); deleting an absent stack succeeds; `RoleARN` applies to this operation only |
| DescribeStacks | One stack by `StackName`, or every stack when omitted; reports `RoleARN` when the stack has one |
| ListStacks | Summary shape; honours `StackStatusFilter.member.N` |
| DescribeStackResources | By `StackName` + optional `LogicalResourceId`, or by `PhysicalResourceId` |
| GetTemplate | Returns the stored `TemplateBody` byte-for-byte |
| CreateChangeSet | `ChangeSetType=UPDATE` only; see below |
| DescribeChangeSet | Accepts a bare change-set name or its ARN |
| ExecuteChangeSet | Applies the change and consumes the set |
| ListChangeSets | Pending change sets for a stack |
| DeleteChangeSet | Discards a pending set; deleting an absent set succeeds |
| DetectStackDrift | Returns a `StackDriftDetectionId` |
| DescribeStackDriftDetectionStatus | Resolves that ID to a completed detection |
| DescribeStackResourceDrifts | Per-resource drift; honours `StackResourceDriftStatusFilters.member.N` |
| ListExports | Every exported output value in the caller's account and Region, in one page |
| ListImports | Stack **names** importing an `ExportName`; an export nothing imports is an empty list |

`TemplateURL` is refused with `ValidationError` rather than ignored: fetching a
template is a network read substrate does not perform, and silently accepting the
parameter deployed a stack with no resources in it.

A request that names something absent — a stack, a change set, a drift detection —
is a `ValidationError` at 400, and a template body that cannot be decoded is a
`ValidationError` at 400 prefixed `Template format error:`. A resource that failed
to deploy after the template parsed is an `InternalFailure` at 500, because the
request was well-formed and the failure is substrate's. That distinction is drawn
from the classification the stack model attaches to a failure, not from its
message, so a message may be reworded without moving any consumer's error code.

### A stack ARN is accepted wherever `StackName` is

`CreateStack` reports the stack's ARN as its `StackId`, and every stack-scoped
operation takes that identifier in place of the name — "The name or the unique
stack ID that's associated with the stack", as the reference puts it. That covers
`UpdateStack`, `DeleteStack`, `DescribeStacks`, `DescribeStackResources`,
`GetTemplate`, the four change-set operations that take a `StackName`, and both
drift operations. `CreateStack` is the exception, and the API's: the ID does not
exist until that call mints it.

```
ARN=$(aws cloudformation create-stack --stack-name probe \
        --template-body file:///tmp/probe.json --query StackId --output text)
aws cloudformation describe-stacks --stack-name "$ARN"   # the stack
aws cloudformation delete-stack   --stack-name "$ARN"    # sweeps its resources
```

The ARN is **verified**, not merely parsed for the name inside it. Substrate builds
a stack ARN from the caller's partition, Region and account plus a digest over
those and the stack name, so an ARN naming another account, another Region, another
partition, or a digest that does not belong to the name attached to it is not an
identifier substrate would have issued — and is refused with the same
`ValidationError` an absent stack reports. Reporting the two cases identically is
deliberate: a stack outside the caller's account and Region is one the caller
cannot observe, so a distinct error would disclose whether some other scope holds a
stack by that name.

### Stacks share state with every other plugin

The plugin is a thin adapter over the same stack model substrate has always
exposed to in-process Go callers, and it deploys through the same plugin registry
the server routes with. A resource a template declares is therefore a **real**
resource in the corresponding plugin:

```
aws cloudformation create-stack --stack-name probe \
  --template-body '{"Resources":{"B":{"Type":"AWS::S3::Bucket",
                    "Properties":{"BucketName":"probe-data"}}}}'
aws s3api head-bucket --bucket probe-data      # 200 — a real bucket
aws s3api put-object --bucket probe-data --key k --body f
```

The reverse also holds: a stack created in process through `emulator.Client`
is visible to `DescribeStacks` over the wire, and a wire-created stack is visible
to the in-process API. There is one set of stacks.

Deleting a stack's resource outside CloudFormation is the drift substrate models
— `DescribeStackResourceDrifts` reports it as `DELETED`.

#### A stack deploys into the calling account and region

Most plugins scope a resource to the account, and some also to the region, of the
request that created it. A stack's resources are created under the identity of the
caller that created the stack, so they are visible to that caller's reads and no
one else's:

```
# unsigned, so the caller is 000000000000
aws --endpoint-url http://localhost:4566 cloudformation create-stack \
  --stack-name acct --template-body '{"Resources":{"I":{"Type":"AWS::EC2::Instance",
                     "Properties":{"ImageId":"ami-12345678","InstanceType":"t3.micro"}}}}'
aws --endpoint-url http://localhost:4566 ec2 describe-instances   # 1 reservation
aws --endpoint-url http://localhost:4566 --region eu-west-1 \
  ec2 describe-instances                                          # 0 — a different partition
```

`AWS::AccountId` and `AWS::Region` resolve to the same caller, so a physical name
built from either agrees with the stack ARN.

The in-process `emulator.Client` deploys into substrate's default partition
(`123456789012` / `us-east-1`). Its callers never sign a request, so there is no
caller identity to take; an in-process caller that needs another partition can set
one on the deployer with `emulator.WithDeployerIdentity`.

### A stack's resource calls are authorized

CloudFormation does not create resources as itself. With a service role it "always
uses this role for all future operations on the stack"; without one it uses "a
temporary session that's generated from your user credentials". Substrate models
both, so a template asking for a permission the deploying identity does not have
fails the way it fails on AWS instead of deploying cleanly.

`RoleARN` is accepted on `CreateStack`, `UpdateStack` and `DeleteStack` and reported
by `DescribeStacks`. Its lifetime differs by operation, following the reference
rather than convenience:

- `UpdateStack` **without** `RoleARN` keeps the role the stack already has, and one
  that supplies it replaces the role for that update and every operation after.
- `DeleteStack`'s `RoleARN` applies to **that delete only** and is not persisted, so
  a delete refused by its override leaves the stack's own role intact and a retry
  runs as the identity the stack was created with.
- A stack with no service role reports no `RoleARN` at all rather than an empty
  string.

Absent a service role, the calls are attributed to the principal that created the
stack — so `CreateStack` cannot be used to obtain a permission the caller does not
have. The same resolution covers teardown: a resource created by a role is deleted
by that role, not by whoever asks for the delete, and a rollback's sweep runs as the
identity that created what it is tearing down.

A refused resource call surfaces as `CREATE_FAILED` with the denial —
`AccessDeniedException`, naming the action and the resource ARN — as that resource's
`ResourceStatusReason` in `DescribeStackResources`, and the stack rolls back:

```
aws cloudformation create-stack --stack-name s --template-body file://bucket.json \
  --role-arn arn:aws:iam::123456789012:role/narrow
aws cloudformation describe-stacks --stack-name s \
  --query 'Stacks[0].[StackStatus,RoleARN]'
# ROLLBACK_COMPLETE   arn:aws:iam::123456789012:role/narrow
aws cloudformation describe-stack-resources --stack-name s \
  --query 'StackResources[0].ResourceStatusReason'
# AccessDeniedException: User: arn:aws:iam::123456789012:role/narrow is not
# authorized to perform: s3:CreateBucket on resource: arn:aws:s3:::...
```

The denial is **not** reported as a `StackEvent`: `DescribeStackEvents` is still
refused with `UnsupportedOperation`, because substrate models stack status rather
than per-resource stack events. Deriving events from the event log is tracked in
[#501](https://github.com/scttfrdmn/substrate/issues/501); until then
`DescribeStackResources` is where a per-resource reason is observable.

Enforcement is opt-in by creating the principal: a stack deployed with a credential
that resolves to no IAM user or role in state is not authorized at all, which is
every in-process `emulator.Client` caller and every credential that never touched
IAM. See the testing guide's "Testing IAM permissions".

### A resource with no name in the template gets a per-stack name

Omitting a resource's physical name is the **recommended** practice — it is what
makes a template deployable more than once, and AWS documents that naming a
resource explicitly costs you replacement updates. So substrate generates a name
for the omitted case, in the shape CloudFormation documents for its own generated
physical IDs (`MyStack-MyBucket-abcdefghijk1`):

```
{stack name}-{logical ID}-{12-character suffix}
```

The omitted case previously used the **logical ID verbatim**, which is unique
only *within* a stack. Every name below is unique across an account or a Region, so
a second stack from the same template either collided outright or — for `SQS::Queue`
and `SNS::Topic`, whose creates are idempotent — silently shared one resource with
the first, and deleting either stack destroyed the other's resource with no error
reported to anyone ([#560](https://github.com/scttfrdmn/substrate/issues/560)).

- **An explicit name still wins, verbatim.** A template that sets `BucketName` gets
  exactly that bucket, un-repeatability included, because that is what it asked for.
- **The suffix is derived, not random** — FNV-64a over the account, Region, stack
  name and logical ID, base36. This is substrate's own divergence: AWS randomizes.
  `UpdateStack` here re-deploys the whole template, so a name regenerated per deploy
  would mint a fresh resource on every update and leak the one it replaced. Deriving
  it keeps an unchanged update a no-op and every name reproducible from its inputs.
  The account and Region are in the hash because two same-named stacks in different
  Regions are different stacks.
- **The name fits the service.** The stack and logical-ID segments are truncated
  proportionally to fit the service's limit and lowercased where the service demands
  it; the suffix is never truncated, since it is the only part that makes the name
  unique. A generated name always begins with a letter and holds only ASCII letters,
  digits and hyphens, with no trailing or doubled hyphen.

The types that get a generated name, with the limit each is fitted to:

| Resource type | Name property | Limit |
|---|---|---|
| `AWS::IAM::Role` | `RoleName` | 64 |
| `AWS::IAM::InstanceProfile` | `InstanceProfileName` | 128 |
| `AWS::IAM::Policy` | `PolicyName` | 128 |
| `AWS::S3::Bucket` | `BucketName` | 63, lowercase |
| `AWS::DynamoDB::Table` | `TableName` | 255 |
| `AWS::SQS::Queue` | `QueueName` | 80 |
| `AWS::SNS::Topic` | `TopicName` | 256 |
| `AWS::Logs::LogGroup` | `LogGroupName` | 512 |
| `AWS::Lambda::Function` | `FunctionName` | 64 |

**A type absent from that table still uses its logical ID.** The list is names that
are account- or Region-unique, not every property that falls back to the logical ID:
an `AWS::ApiGateway::Resource`'s `PathPart` is a URL segment unique only within its
parent, and `Domain`, `SecretId`, `ClusterId` and `ReplicationGroupId` are
identifiers a template legitimately controls. Generating those would change the URLs
and identifiers a consumer wrote the template to get.

`Ref` and `GetAtt` resolve to the generated name and ARN, and the delete sweep
deletes by it, so a template that wires its resources together needs no change —
and two stacks from one template can now be torn down independently.

### DeleteStack deletes the stack's resources

`DeleteStack` sweeps the resources the stack deployed before removing the stack
record: a bucket a stack created is gone once its stack is, and `s3api head-bucket`
answers 404.

The sweep is the exact inverse of the deploy — resources are ordered by descending
deploy priority, ties by descending logical ID — so a resource is deleted before
whatever it was created after. Substrate deploys in priority order rather than from
a dependency graph, and inverting that order is what makes the teardown observable:
the recorded event sequence for a stack of an IAM role, a bucket, a queue and a
topic reads `CreateRole, CreateBucket, CreateQueue, CreateTopic` and then
`DeleteTopic, DeleteQueue, DeleteBucket, DeleteRole`.

`DeletionPolicy` and `UpdateReplacePolicy` are parsed from both the JSON and YAML
template paths, and a value outside `Delete`/`Retain`/`RetainExceptOnCreate`/
`Snapshot` is a template error rather than a silent default. `Retain` keeps the
resource and the stack still deletes; `RetainExceptOnCreate` retains for a
`DeleteStack` but not for the rollback of the create that made the resource. The
default is `Delete`, **except** `Snapshot` for `AWS::RDS::DBCluster` and for an
`AWS::RDS::DBInstance` that declares no `DBClusterIdentifier` — a sweep that
assumed `Delete` would destroy a database the template asked to be snapshotted.
`Snapshot` does not retain: substrate deletes the resource and records in the
per-resource reason that no snapshot was taken, since no snapshot resource is
modelled for any of the eight Snapshot-capable types.

A resource already absent — deleted out of band between the deploy and the sweep —
is a **success**, not a failure: a stack must not be wedged by a resource someone
else removed. Any other refusal is a failure, and a stack with a failed deletion
keeps its record, its resource list and its name index while reporting
`DELETE_FAILED` in `DescribeStacks`, with the offending resource and the plugin's
own error code in the reason. A stack that reported a failed delete and then
vanished would leave a caller no way to retry and no way to learn what held it.

The `delete-stack` **call** still answers 200 in that case, as the API does: real
`DeleteStack` returns success and the stack reaches `DELETE_FAILED` asynchronously,
so poll `DescribeStacks` to learn the outcome rather than relying on the call
raising. The in-process `emulator.StackDeployer.DeleteStack` returns an error
directly, since a Go caller has no status to poll.

```
aws cloudformation delete-stack --stack-name probe
aws s3api head-bucket --bucket probe-data     # 404 — deleted with its stack

aws cloudformation delete-stack --stack-name stuck          # 200
aws cloudformation describe-stacks --stack-name stuck \
  --query 'Stacks[0].StackStatus'                           # DELETE_FAILED
```

#### A resource whose delete needs a detach first gets one

Some deletes are refused while a subordinate entity still references the resource.
`AWS::IAM::InstanceProfile` is the case substrate models: the sweep dispatches one
`RemoveRoleFromInstanceProfile` for each role in the profile's declared `Roles`
before `DeleteInstanceProfile`, resolving each `!Ref` through the same context the
deploy used — so a role whose name was generated is detached by its generated name.

Without it the stack could not converge from either side
([#581](https://github.com/scttfrdmn/substrate/issues/581)): `DeleteRole` succeeded
while the profile still held the role, leaving the profile listing a role that no
longer existed, and `DeleteInstanceProfile` then refused with `DeleteConflict`. The
failure therefore landed on the resource that was still *present*, and a retry failed
identically — with no `RetainResources` escape, since retaining the profile leaves it
behind for good.

A pre-step failure fails that resource: a delete dispatched after a failed detach
would be refused anyway, and reporting the detach's own error names what went wrong.
A profile declaring no roles dispatches only its own delete.

Coverage is stated rather than implied. Of the 109 resource types the deployer
dispatches, 89 have a delete request and 11 are state-only types whose stub record
is removed. The remaining 9 sweep to a no-op and are reported as `DELETE_SKIPPED`
naming the reason:

| Type | Why the sweep is a no-op |
|---|---|
| `AWS::CloudFront::CloudFrontOriginAccessIdentity` | the deploy records no state to remove |
| `AWS::ECS::CapacityProvider` | the deploy records no state to remove |
| `AWS::SSM::Association` | the deploy records no state to remove |
| `AWS::SecretsManager::SecretTargetAttachment` | the deploy records no state to remove |
| `AWS::Route53::RecordSetGroup` | its record sets are dispatches the stack does not record individually |
| `AWS::ECR::LifecyclePolicy` | `DeleteLifecyclePolicy` is not routed; deleting the repository removes the policy |
| `AWS::ApiGateway::UsagePlanKey` | `DeleteUsagePlanKey` is not routed; the key goes with its usage plan |
| `AWS::Cognito::IdentityPoolRoleAttachment` | Cognito models no `DeleteIdentityPoolRoles` |
| `AWS::SecretsManager::RotationSchedule` | `CancelRotateSecret` is not routed; the schedule goes with the secret |

A type the deployer does not recognize at all is also `DELETE_SKIPPED`: its stub
state is removed, but substrate never created a resource for it, so reporting
`DELETE_COMPLETE` would claim a deletion that did not happen. A skip is always
reported — a claim of cleanliness that is not true is worse than a stated gap.

### A refused resource reports CREATE_FAILED

A plugin that refuses a resource — an invalid bucket name, a malformed trust
policy, a security group that does not exist — makes that resource
`CREATE_FAILED` in `DescribeStackResources`, with the plugin's **own** error code
and message as the reason:

```
aws cloudformation describe-stack-resources --stack-name badname
# ResourceStatus: CREATE_FAILED
# ResourceStatusReason: InvalidBucketName: The specified bucket is not valid.
```

**The stack rolls back.** By default the resources the create had already made are
swept — in the same reverse order a `DeleteStack` uses — and the stack reaches
`ROLLBACK_COMPLETE`, naming the resource that failed and the plugin's error code in
its reason:

```
aws cloudformation create-stack --stack-name partial --template-body file:///tmp/partial.json
aws cloudformation describe-stacks --stack-name partial \
  --query 'Stacks[0].StackStatus'                    # ROLLBACK_COMPLETE
aws sqs get-queue-url --queue-name still-here        # absent — swept with the stack
```

`CreateStack`'s two failure options both work, and are **mutually exclusive** as the
API makes them: specifying `OnFailure` and `DisableRollback` together is a
`ValidationError`, and the test is *presence* rather than value, so the CLI's
`--no-disable-rollback` counts as specifying it.

| Option | Outcome |
|---|---|
| `OnFailure=ROLLBACK` (default), `DisableRollback=false` | resources swept, stack reports `ROLLBACK_COMPLETE` |
| `OnFailure=DO_NOTHING`, `DisableRollback=true` | nothing swept, stack reports `CREATE_FAILED` |
| `OnFailure=DELETE` | resources swept and the stack record removed |

Whichever was given is reported back: `DescribeStacks` emits `DisableRollback` as
`true` for a `DO_NOTHING` stack rather than always `false`. A sweep that cannot
delete a resource gives `ROLLBACK_FAILED`, and the stack keeps its record so the
undeleted resource is discoverable. All of these answer **200** on the wire — real
`CreateStack` has returned its `StackId` before the rollback happens, so a
rolled-back stack is not a failed call; poll `DescribeStacks` for the outcome.

`RetainExceptOnCreate` interacts here: it retains a resource for a `DeleteStack`
sweep but **deletes** it for the rollback of the create that made it.

A failed stack publishes **no outputs**, and therefore exports none — an import
against a value whose resource never deployed would resolve against nothing. A
duplicate export name is still refused as an error rather than as a rolled-back
stack, so that refusal reads the same whether or not a resource beside it failed.

Two divergences are substrate's own, both deliberate:

- Substrate deploys the resources declared **after** the failure, where real
  CloudFormation stops at the first one, so a single deploy reports every refusal a
  template contains rather than only the first. The stack status is the same either
  way, and the status is what a caller keys off. Under `DO_NOTHING` those later
  resources are therefore left in place too.
- A failed `UpdateStack` reports `UPDATE_ROLLBACK_COMPLETE` by **re-deploying the
  stored previous template**, since that is the only description of the previous
  state substrate holds. It converges on the previous template's *declared* state
  rather than restoring properties field by field, so a resource the failed update
  replaced may keep a new physical ID. A previous record that cannot be read leaves
  the stack at `UPDATE_FAILED` with the reason logged rather than a rollback
  attempted against nothing.

Because an update is a re-deploy, an unchanged resource's create is re-issued and
the plugin refuses it as already existing. That refusal is **not** a failure of the
update: substrate clears it when the stack's previous deployment created that
logical ID successfully *and* the template still declares it identically. A rename
into a name another stack owns, a resource that failed the previous time, and a
record belonging to another account or region are all left standing as real
failures — without that guard every `UpdateStack` would roll back the resources it
was asked to keep.

A refused resource's follow-up configuration requests are not sent either. A bucket
whose name S3 rejected has no `PUT ?versioning` issued against it, so the event log
holds only the request a real client would have made.

### Templates

113 resource types are supported; each service section below lists the types it
backs under **CloudFormation resource types**. A template body may be JSON or YAML.

**Every intrinsic function resolves.** `Ref`, `Fn::GetAtt`, `Fn::Sub`, `Fn::Join`,
`Fn::Select`, `Fn::Split`, `Fn::Base64`, `Fn::If`, `Fn::Equals`, `Fn::And`,
`Fn::Or`, `Fn::Not`, `Fn::FindInMap`, `Fn::GetAZs`, `Fn::Cidr` and
`Fn::ImportValue`, as do every pseudo-parameter: `AWS::Region`, `AWS::AccountId`,
`AWS::StackName`, `AWS::StackId`, `AWS::Partition`, `AWS::URLSuffix`,
`AWS::NotificationARNs` and `AWS::NoValue`.

`AWS::Partition` and `AWS::URLSuffix` follow the region — `aws-cn` and
`amazonaws.com.cn` for a `cn-` region, `aws-us-gov` for a `us-gov-` one, `aws` and
`amazonaws.com` otherwise. `AWS::StackId` is the same ARN `CreateStack` returned
and `DescribeStacks` reports for the stack, so a template that writes its own
stack ID into a property and a caller that captured `StackId` agree.
`AWS::NotificationARNs` is an **empty list**, which is the accurate answer for a
stack created without any: substrate has no notification model, so there is never
an ARN to report, and `!Select ['0', !Ref 'AWS::NotificationARNs']` yields the
empty string rather than the reference string.

#### `Mappings` and `Fn::FindInMap`

A template's `Mappings` section is read and `Fn::FindInMap` resolves all three
levels — map name, top-level key, second-level key — including the nested form the
reference leads with, where the second-level key is itself a `Ref` or a
`Fn::FindInMap`. A mapping's leaf value may be a string or a **list**, so
`SecurityGroupIds: !FindInMap [SGs, Prod, Ids]` contributes several IDs; in a
scalar context the members are rejoined on commas, as `Fn::Split`'s are.

A lookup that misses **fails the resource**: it reports `CREATE_FAILED` with a
`ResourceStatusReason` naming the intrinsic and the key, and the resource is not
created. This is deliberate and it is the point of the model — the JSON-encoding
fallback would have turned a missing AMI into a nonsense `ImageId` that launched an
instance, reporting success for a template real CloudFormation rejects. The rest of
the stack still deploys; one unresolvable property does not abort it, matching the
per-resource failure reporting above.

The optional fourth argument supplies a fallback: `!FindInMap [M, K1, K2, {DefaultValue: x}]`
resolves to `x` when either key is missing, and the fallback is not consulted when
the lookup succeeds. It must be spelled exactly `DefaultValue` — a map with any
other key is not a default and the lookup fails as it would without one, rather
than substrate guessing at the intent.

#### `Fn::GetAZs` and `Fn::Cidr`

`Fn::GetAZs` resolves to the **same zone names EC2's `DescribeAvailabilityZones`
reports** for that region, from the same list, so a subnet placed with
`!Select [0, !GetAZs '']` names a zone the caller can afterwards query. An empty
string means the caller's region, as `Ref 'AWS::Region'` does.

`Fn::Cidr` splits `ipBlock` into `count` blocks whose mask is the address width
less `cidrBits`, for IPv4 and IPv6 alike — `!Cidr ['192.168.0.0/24', 6, 5]` gives
six `/27`s, and `!Cidr ['2001:db8::/56', 1, 64]` gives a `/64`. A request the block
cannot satisfy — a `count` larger than the number of blocks that fit, a `cidrBits`
that would widen the block, a `count` outside 1–256, an `ipBlock` that is not a
CIDR block — fails the resource rather than returning a short list, for the same
reason: `!Select [3, !Cidr [...]]` over a short list would read an empty string out
of it and deploy.

`Fn::Split` resolves to a **list**, and a list-valued property receives every
element. A `CommaDelimitedList` (or `List<…>`) parameter is list-valued too: a
`Ref` to one yields one member per comma, each space-trimmed, so
`!Select ['2', !Split [':', arn]]` picks the third field and
`SecurityGroupIds: !Ref SubnetIds` reaches the API as several IDs rather than one
string. Whether a `Ref` is list-valued comes from the parameter's declared type,
not from whether its value happens to contain a comma.

`Ref 'AWS::NoValue'` in a list position contributes **no** element, and as a
*property's whole value* it **removes the property**, which is what makes the
conventional `!If [HasCommand, !Split [',', !Ref Command], !Ref 'AWS::NoValue']`
idiom work: the property is absent rather than present-and-empty, and an API that
rejects an empty value sees what it would see from real CloudFormation.

Intrinsics resolve **at any depth** inside a structured property, not only where
the property's whole value is one. A `Ref` inside `KeySchema`, an `Fn::Sub` inside
a container definition's `Environment`, an `Fn::Split` nested in a list — all
resolve, and a nested list-valued intrinsic contributes its elements to the list
holding it rather than one rejoined string. Two rules bound the walk:

- Only a **single-key** map is an intrinsic. A map with several keys is user data
  even when one of them is named `Ref`, so a property whose interior is a
  caller-supplied map — a log driver's `Options`, an IAM policy `Condition` block
  — keeps its own shape.
- **Keys are never rewritten by resolution.** Where a property's member names
  differ between CloudFormation and the service's API — ECS spells a container's
  members in camelCase where CloudFormation spells them PascalCase — that mapping
  is per-service and applied separately, and it stops at any member whose keys are
  user-supplied.

A resolved intrinsic is a **string**, or a list of strings where the intrinsic is
list-valued. So `"Cpu": {"Ref": "Cpu"}` reaches the API as `"256"` where a literal
`256` would have stayed a number; a literal is never retyped.

Where the property is scalar and has nowhere to put a list — an `Outputs` value,
say — `Fn::Split`'s elements are rejoined on the delimiter, reproducing the
source string, and `Fn::GetAZs`, `Fn::Cidr` and a list-valued `Fn::FindInMap` leaf
are rejoined on commas. Real CloudFormation rejects the template instead; substrate
resolves rather than rejects, and rejoining is the spelling that loses nothing.

A parameter declared `Default: ''` is a parameter whose default is the empty
string, not a parameter without one — which is what makes the conventional
optional-parameter idiom work:

```yaml
Parameters:
  Command: {Type: String, Default: ''}
Conditions:
  HasCommand: !Not [!Equals [!Ref Command, '']]
```

A condition that references another condition by name resolves regardless of the
order the template declares them in; a reference cycle resolves to `false`. Each
condition is evaluated once, before any resource deploys, and keeps that value for
the whole deployment — as in real CloudFormation, where conditions are evaluated when
the stack is created or updated and cannot reference a resource or its attributes.

#### Cross-stack exports and `Fn::ImportValue`

An output that declares `Export: {Name: …}` publishes its value for another stack
to import; an output without one is readable through `DescribeStacks` and nowhere
else. `Fn::ImportValue` resolves against those exports, so the two-stack idiom —
a network stack exporting a subnet ID, an app stack importing it — deploys and reads
back as it does in AWS. The export name may itself be an intrinsic, which is what
makes the conventional `Export: {Name: !Sub '${AWS::StackName}-SubnetID'}` work; it
resolves before the first resource deploys, which the API permits because an export
name may not depend on a resource.

Exports are scoped **per account and Region**, matching the documented restriction
that cross-stack references are limited to the same account and Region. A caller in
another account or another Region does not see them in `ListExports` and cannot
import them — a template that would fail in AWS fails here rather than resolving
against an export it is not entitled to.

Four rules are enforced, and they are the reason exports are modelled rather than
faked:

- **An import of an unpublished name fails the resource**, with the export name in
  the `ResourceStatusReason`. It does not resolve to the empty string or to the
  intrinsic's JSON — either would launch a resource named with nonsense and report
  success.
- **Export names are unique per account and Region.** A second stack claiming a name
  another stack already exports is a `ValidationError` at 400 naming the holder.
- **A stack whose export is imported cannot be deleted.** `DeleteStack` is a
  `ValidationError` at 400 naming the export and every importing stack; the stack is
  untouched. Delete the importers first, as AWS requires.
- **An imported export's value cannot be changed** — nor dropped, which is the same
  thing from the importer's side. An `UpdateStack` that would change or remove it is
  refused on the same terms. Re-deploying the *same* value is not a change, so an
  idempotent redeploy is unaffected.

What counts as an import is decided **when the resolver walks the template**, not
from the template's text. An `Fn::ImportValue` in the branch of an `Fn::If` that was
not taken never happened, and neither does one that failed to resolve, so neither
pins an exporting stack. `DescribeStacks` reports each output's `ExportName` beside
it, and `ListImports` answers the "who is holding this" question a refused delete
raises.

The two refusals above use `ValidationError` at 400. The `DeleteStack` reference
documents only `TokenAlreadyExists`, so that code is substrate's own choice for this
case — the same code the service uses for every other unsatisfiable request on it.

#### YAML short forms

The YAML tag shorthands are expanded to their long forms before the template is
read, so a template written with `!Ref` / `!Sub` / `!If` resolves **identically**
to the same template written with `Ref` / `Fn::Sub` / `Fn::If`. All of
`!Ref`, `!Condition`, `!GetAtt`, `!Sub`, `!Join`, `!Select`, `!Split`, `!Base64`,
`!If`, `!Equals`, `!Not`, `!And`, `!Or`, `!FindInMap`, `!ImportValue`, `!Cidr`,
`!GetAZs` and `!Transform` are expanded, at any nesting depth — `!Not [!Equals
[!Ref VpcId, '']]` is three levels and works.

`!GetAtt` is the one irregular form: it takes a dotted string where `Fn::GetAtt`
takes a two-element list, and the split is on the **first** period only, so
`!GetAtt Res.Outputs.Nested` is `["Res", "Outputs.Nested"]` — an attribute name
may itself contain periods.

Expansion is not resolution, but nothing is now expanded that does not also
resolve: every tag in the list above reaches a resolver, `!Transform` excepted —
it expands to `Fn::Transform`, which substrate carries but does not apply, since a
macro is code substrate does not run.

A tag substrate does not recognize is **not** dropped: the node's value is kept
and a `WARN` naming the tag is logged, since a macro or transform may introduce a
tag substrate has never heard of, and refusing the template would reject one real
CloudFormation accepts.

A short form's value is read as a string, as the long forms' unquoted values are, so
`!Sub 12345` and `!Sub 2026-08-02` reach the resolver as written rather than as a
number and a timestamp.

One tag per node is a YAML rule, not a substrate limitation: `!Base64 !Ref P` is a
parse error, which is why AWS's own examples spell the outer function long-form —
`Fn::Base64: !Ref P`. That nesting works.

`Fn::Sub` honours the documented `${!Literal}` escape: `${!Count.Index}` renders
as the literal `${Count.Index}` with no substitution, which is how a template
passes a `${…}` through to something that interpolates it later, such as Terraform
or cloud-init.

Parameters use the Query protocol's list encoding, which is what every SDK and
the CLI send:

```
Parameters.member.1.ParameterKey=Env
Parameters.member.1.ParameterValue=staging
Parameters.member.2.ParameterKey=Size
Parameters.member.2.UsePreviousValue=true
```

`UsePreviousValue=true` resolves against the stack's stored parameters, so an
`UpdateStack` need not repeat every value.

Template **transforms** are not applied — `GetTemplate` reports
`StagesAvailable: [Original]` only, and a SAM or macro template reaches the
deployer unexpanded.

### Change sets describe, they do not stage

A change set records the template that would be applied and reports the
resource-level changes it implies. `ExecuteChangeSet` applies it and deletes the
set, so `DescribeChangeSet` afterwards reports `ChangeSetNotFound` (404 — unlike
the stack family, which reports `ValidationError` at 400).

`ChangeSetType=CREATE` is refused: it would have to produce a stack in
`REVIEW_IN_PROGRESS`, a state the stack model has no representation for. Create
the stack, then change-set the update.

### DescribeStackEvents is not supported

`DescribeStackEvents` returns `UnsupportedOperation` (400). This is deliberate,
and `UnsupportedOperation` is substrate's own signal rather than a documented
CloudFormation code — real CloudFormation has no error for an operation it
implements.

A stack carries one status string; there is no per-resource event model behind
it. Answering the call would mean synthesizing a plausible
`CREATE_IN_PROGRESS → CREATE_COMPLETE` pair per resource with invented
timestamps, which is inventing observations — the opposite of what an emulator
that exists to be trusted should do. A consumer polling events for completion
should poll `DescribeStacks` for `StackStatus` instead. Tracked in
[#501](https://github.com/scttfrdmn/substrate/issues/501).

### Stack status is terminal on return

`CreateStack` deploys synchronously and returns with the stack already
`CREATE_COMPLETE`; `UpdateStack` returns `UPDATE_COMPLETE`. There is no
`*_IN_PROGRESS` window, so a `wait stack-create-complete` succeeds immediately
rather than polling. That is the deterministic-clock trade: a stack's observable
state does not depend on how long a test waited.

### Stack and change-set ARNs are deterministic

The UUID in a stack or change-set ARN is derived from the account, region and
name, not from a clock or a PRNG, so the same call produces the same ARN on a
replay. `StackId` is stable across `CreateStack`, `DescribeStacks` and
`ListStacks`.

### Cost

CloudFormation operations are free. The resources a template deploys are costed
by their own plugins, so a stack's cost shows up under S3, EC2 and so on.

---

## IAM

**Endpoint:** `iam.amazonaws.com`
**Protocol:** AWS Query (form-encoded, `Action=` parameter)

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateUser | Returns User object |
| GetUser | |
| DeleteUser | |
| ListUsers | |
| CreateRole | Supports trust policy document |
| GetRole | |
| DeleteRole | Refuses with `DeleteConflict`/409 while a policy is attached **or** an instance profile holds the role; the message names the profiles |
| ListRoles | |
| CreateGroup | |
| GetGroup | |
| DeleteGroup | |
| ListGroups | |
| AttachUserPolicy | Does not verify the policy ARN resolves ([#499](https://github.com/scttfrdmn/substrate/issues/499)) |
| DetachUserPolicy | |
| ListAttachedUserPolicies | |
| AttachRolePolicy | Does not verify the policy ARN resolves ([#499](https://github.com/scttfrdmn/substrate/issues/499)) |
| DetachRolePolicy | |
| ListAttachedRolePolicies | |
| CreatePolicy | |
| GetPolicy | Resolves a bundled AWS managed policy or a `CreatePolicy` one; metadata only, as on AWS |
| DeletePolicy | |
| ListPolicies | Lists `CreatePolicy` policies only; `Scope` and `PathPrefix` are accepted and not applied ([#497](https://github.com/scttfrdmn/substrate/issues/497)) |
| CreateAccessKey | |
| DeleteAccessKey | |
| ListAccessKeys | |
| PutUserPolicy | Inline policy |
| GetUserPolicy | |
| DeleteUserPolicy | |
| ListUserPolicies | |
| PutRolePolicy | Inline policy |
| GetRolePolicy | |
| DeleteRolePolicy | |
| ListRolePolicies | |
| PutUserPermissionsBoundary | |
| DeleteUserPermissionsBoundary | |
| PutRolePermissionsBoundary | |
| DeleteRolePermissionsBoundary | |
| TagUser | |
| UntagUser | |
| ListUserTags | |
| TagRole | |
| UntagRole | |
| ListRoleTags | |
| CreateInstanceProfile | |
| GetInstanceProfile | |
| AddRoleToInstanceProfile | |

### AWS managed policies are a seeded catalog

Substrate bundles **52** AWS managed policies, not the ~1,200 AWS publishes. Each carries
its real ARN, policy ID, path and default version, and a policy document copied verbatim
from its page in the [AWS managed policy
reference](https://docs.aws.amazon.com/aws-managed-policy/latest/reference/). `GetPolicy`
resolves a bundled ARN exactly as it resolves one from `CreatePolicy`.

The catalog covers two populations:

| Population | Examples |
|---|---|
| Human-operator policies (47) | `AdministratorAccess`, `PowerUserAccess`, `ReadOnlyAccess`, and per-service `…FullAccess` / `…ReadOnlyAccess` pairs |
| Service-role policies (5) | `AmazonSSMManagedInstanceCore`, `AmazonEC2ContainerRegistryReadOnly`, `service-role/AmazonECSTaskExecutionRolePolicy`, `service-role/AWSLambdaBasicExecutionRole`, `service-role/AWSLambdaVPCAccessExecutionRole` |

The distinction matters because they are attached by different callers. A human-operator
policy is attached to a user or group; a service-role policy is what an **instance profile
or execution role** carries, and those are the ones IaC provisions. Substrate bundled
`AmazonSSMFullAccess` — the operator policy — but not `AmazonSSMManagedInstanceCore`, which
is the policy an SSM-managed instance actually needs.

A policy under a path reports the path in `Path` and keeps it out of `PolicyName`:
`service-role/AWSLambdaBasicExecutionRole` has `Path: /service-role/` and `PolicyName:
AWSLambdaBasicExecutionRole`, matching AWS. The full ARN includes the path component.

#### Attaching a policy substrate does not bundle

`AttachRolePolicy` and `AttachUserPolicy` accept **any** policy ARN without checking that
it resolves, so attaching one of the ~1,150 unbundled managed policies succeeds and
`GetPolicy` on the same ARN then returns `NoSuchEntity`. Real IAM refuses the attach.

This asymmetry is deliberate for now: refusing an ARN substrate cannot resolve would fail
every attach of an unbundled managed policy — breaking working consumer code — where the
current behaviour merely fails to catch a typo. Tracked in
[#499](https://github.com/scttfrdmn/substrate/issues/499). A consumer who needs the attach
verified should follow it with `GetPolicy`, which is exact.

#### Policy documents are not observable over the wire

`GetPolicy` returns metadata only — policy ID, name, ARN, path, default version,
attachment count and dates — which is what AWS returns. On AWS the document comes from
`GetPolicyVersion`, which substrate does not implement
([#498](https://github.com/scttfrdmn/substrate/issues/498)). The seeded documents are
readable in process through `emulator.GetManagedPolicy` and are what the IAM policy
evaluator reads.

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::IAM::Role | RoleName | Supports AssumeRolePolicyDocument, ManagedPolicyArns |
| AWS::IAM::Policy | PolicyName | |
| AWS::IAM::User | UserName | |
| AWS::IAM::InstanceProfile | InstanceProfileName | Attaches each entry in `Roles`; resolvable by an `AWS::EC2::Instance`'s `IamInstanceProfile` |
| AWS::IAM::Group | GroupName | |

### Cost

IAM operations are free.

---

## STS

**Endpoint:** `sts.amazonaws.com`
**Protocol:** AWS Query (form-encoded)

### Supported operations

| Operation | Notes |
|-----------|-------|
| GetCallerIdentity | Returns account 123456789012 by default |
| AssumeRole | Returns stub temporary credentials |
| GetSessionToken | Returns stub temporary credentials |

### Cost

STS operations are free.

---

## S3

**Endpoint:** `s3.amazonaws.com` / `{bucket}.s3.amazonaws.com`
**Protocol:** REST/XML

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateBucket | Stores an ACL named by `x-amz-acl` / `x-amz-grant-*` — see [Access control lists](#access-control-lists) |
| HeadBucket | |
| DeleteBucket | |
| ListBuckets | |
| PutObject | Supports Content-Type, metadata headers; `Cache-Control`, `Content-Disposition`, `Content-Language`, `Expires` — see [Object system metadata](#object-system-metadata); `Content-Encoding` less any `aws-chunked` — see [Content-Encoding and aws-chunked](#content-encoding-and-aws-chunked); `x-amz-storage-class` — see [Storage classes](#storage-classes); conditional writes, including a seedable `409 ConditionalRequestConflict` — see [Conditional requests](#conditional-requests); verifies `x-amz-checksum-*` — see [Additional checksums](#additional-checksums); records the `x-amz-server-side-encryption` family — see [Server-side encryption](#server-side-encryption); stores an ACL named by `x-amz-acl` / `x-amz-grant-*` — see [Access control lists](#access-control-lists) |
| GetObject | Echoes recorded system metadata — see [Object system metadata](#object-system-metadata); supports Range header — see [Ranged reads](#ranged-reads); preconditions — see [Conditional requests](#conditional-requests); `403 InvalidObjectState` on archived objects — see [Storage classes](#storage-classes); `x-amz-checksum-mode` — see [Additional checksums](#additional-checksums); synthesizes a seedable task-completion record — see [Task-completion records](#task-completion-records); echoes recorded encryption — see [Server-side encryption](#server-side-encryption) |
| HeadObject | Echoes recorded system metadata — see [Object system metadata](#object-system-metadata); supports Range header — see [Ranged reads](#ranged-reads); preconditions — see [Conditional requests](#conditional-requests); succeeds on archived objects — see [Storage classes](#storage-classes); `x-amz-checksum-mode` — see [Additional checksums](#additional-checksums); resolves a synthesized task-completion record exactly as `GetObject` does — see [Task-completion records](#task-completion-records); echoes recorded encryption — see [Server-side encryption](#server-side-encryption) |
| DeleteObject | Fires S3 notifications if configured |
| CopyObject | Honors both destination and `x-amz-copy-source-if-*` preconditions, including a seedable `409 ConditionalRequestConflict` on the destination — see [Conditional requests](#conditional-requests); `x-amz-metadata-directive` / `x-amz-tagging-directive` and storage-class transitions — see [Copying objects](#copying-objects); recomputes the checksum — see [Additional checksums](#additional-checksums); records **no** encryption, deliberately — see [Server-side encryption](#server-side-encryption); takes its ACL from the copy request and never from the source — see [Access control lists](#access-control-lists) |
| ListObjects | Emits `<StorageClass>` per object |
| ListObjectsV2 | Supports Prefix, Delimiter, MaxKeys, ContinuationToken; emits `<StorageClass>` per object |
| CreateMultipartUpload | Accepts `x-amz-storage-class` and the [system-metadata family](#object-system-metadata), applied to the assembled object; `Content-Encoding` less any `aws-chunked` — see [Content-Encoding and aws-chunked](#content-encoding-and-aws-chunked); `x-amz-checksum-algorithm` / `x-amz-checksum-type` — see [Additional checksums](#additional-checksums); records the encryption for the whole upload — see [Server-side encryption](#server-side-encryption); records the ACL for the whole upload — see [Access control lists](#access-control-lists) |
| UploadPart | Verifies the part checksum, including a trailing one — see [Additional checksums](#additional-checksums) |
| UploadPartCopy | Copies an existing object, or a byte range of one, into a part — see [Copying into a part](#copying-into-a-part) |
| ListParts | Lists an upload's stored parts, with `max-parts` / `part-number-marker` paging; an upload with no parts is `200` with an empty list |
| CompleteMultipartUpload | Validates part order, ETags, and part sizes — see [Multipart upload validation](#multipart-upload-validation); conditional writes, including a seedable `409 ConditionalRequestConflict` that invalidates the upload — see [Conditional requests](#conditional-requests); assembles the object checksum — see [Additional checksums](#additional-checksums); reports the upload's recorded encryption — see [Server-side encryption](#server-side-encryption); applies the upload's recorded ACL — see [Access control lists](#access-control-lists) |
| AbortMultipartUpload | |
| ListMultipartUploads | Emits `<StorageClass>` per in-progress upload |
| GetBucketPolicy | |
| PutBucketPolicy | `403 AccessDenied` for a public policy when `BlockPublicPolicy` is set — see [Block Public Access](#block-public-access) |
| DeleteBucketPolicy | |
| PutPublicAccessBlock | Records the configuration and enforces `BlockPublicAcls` / `BlockPublicPolicy`; a partial body reports omitted settings as `false` — see [Block Public Access](#block-public-access) |
| GetPublicAccessBlock | `404 NoSuchPublicAccessBlockConfiguration` when the bucket has none — see [Block Public Access](#block-public-access) |
| DeletePublicAccessBlock | Idempotent; removes only the configuration, never the bucket — see [Block Public Access](#block-public-access) |
| GetBucketAcl | Reports the stored ACL, or the default owner-only one — see [Access control lists](#access-control-lists) |
| PutBucketAcl | Accepts an XML body, a canned `x-amz-acl` or the `x-amz-grant-*` family — see [Access control lists](#access-control-lists); `403 AccessDenied` for a public ACL when `BlockPublicAcls` is set — see [Block Public Access](#block-public-access) |
| GetObjectAcl | Reports the stored ACL, or the default owner-only one — see [Access control lists](#access-control-lists) |
| PutObjectAcl | Accepts an XML body, a canned `x-amz-acl` or the `x-amz-grant-*` family — see [Access control lists](#access-control-lists); `403 AccessDenied` for a public ACL when the *bucket* has `BlockPublicAcls` set — see [Block Public Access](#block-public-access) |
| GetBucketNotificationConfiguration | Reports the stored configuration in the API's element names; an unconfigured bucket is an empty `NotificationConfiguration` — see [Event notifications](#event-notifications) |
| PutBucketNotificationConfiguration | Dispatches to Lambda, SQS and SNS on `PutObject`/`DeleteObject`; a body naming no recognized element is `400 MalformedXML` — see [Event notifications](#event-notifications) |
| PutBucketTagging | |
| GetBucketTagging | |
| DeleteBucketTagging | |
| PutObjectTagging | |
| GetObjectTagging | |
| DeleteObjectTagging | |

### Storage classes

`PutObject`, `CopyObject` and `CreateMultipartUpload` accept `x-amz-storage-class`
and record it on the object. An absent header means `STANDARD`, S3's documented
default for a newly created object. All thirteen documented values are accepted:

```
STANDARD  REDUCED_REDUNDANCY  STANDARD_IA  ONEZONE_IA  INTELLIGENT_TIERING
GLACIER  DEEP_ARCHIVE  OUTPOSTS  GLACIER_IR  SNOW  EXPRESS_ONEZONE
FSX_OPENZFS  FSX_ONTAP
```

Any other value — including a lowercase or whitespace-padded one — is `400
InvalidStorageClass`, rejected before anything is written, so the key does not
appear. The classes reachable only through Outposts, Snow, Express One Zone and the
FSx-backed tiers are accepted but carry no distinct behaviour beyond being recorded.

How the class is reported back differs between the header and the XML, which is easy
to get wrong in both directions:

| Surface | STANDARD | Every other class |
|---|---|---|
| `x-amz-storage-class` response header on `GetObject`/`HeadObject` | **Omitted** | Present |
| `<StorageClass>` in `ListObjects`, `ListObjectsV2`, `ListObjectVersions`, `ListMultipartUploads` | `STANDARD` | The class |

An absent header therefore means `STANDARD`, not "unknown". A `<DeleteMarker>` entry
in `ListObjectVersions` carries no `<StorageClass>`, matching S3's response shape.

**Archived objects.** A `GetObject` of a `GLACIER` or `DEEP_ARCHIVE` object is `403
InvalidObjectState` with the message `The action is not valid for the object's
storage class`, and so is a `CopyObject` that names one as its source — S3 requires
a restore first. The check precedes the `Range` step, so a ranged read of an
archived object is the same `403`, not a `206`.

`GLACIER_IR` is **not** archival. It is the instant-retrieval tier and reads
normally; so do `STANDARD_IA`, `ONEZONE_IA` and `INTELLIGENT_TIERING`.

`HeadObject` of an archived object is a **`200`**, not a `403`. The `HeadObject`
reference documents no `InvalidObjectState` and states that "even if the object is
stored in S3 Glacier, all object metadata is still available" — which is what makes
`HEAD` the way a consumer discovers that a `GET` would need a restore first. A test
asserting `403` on `HEAD` is asserting behaviour real S3 does not have.

`RestoreObject` and the `x-amz-restore` response header are not implemented, so an
archived object stays unreadable for the lifetime of the emulator run. Restoring is
modeled by copying the object to a non-archival class.

Intelligent-Tiering archive access tiers are not modeled, so the `InvalidObjectState`
variant carrying `<StorageClass>` and `<AccessTier>` children is never returned.

### Content-Encoding and aws-chunked

`PutObject` and `CreateMultipartUpload` record `Content-Encoding` on the object and
`GetObject`/`HeadObject` echo it back. **The `aws-chunked` token is stripped before
the value is recorded**, on both write paths:

| Request `Content-Encoding` | Recorded, and returned on a read |
|---|---|
| `gzip` | `gzip` |
| absent | absent — no header on the response |
| `aws-chunked` | absent |
| `aws-chunked, gzip` | `gzip` |
| `gzip, aws-chunked` | `gzip` |

`aws-chunked` is a *transfer* encoding: it describes the chunk-signature framing a
SigV4 streaming upload arrived in, which substrate decodes before storing the body
(see [Additional checksums](#additional-checksums) for the trailer that framing
carries). The bytes at rest are plain, and the API reference defines
`Content-Encoding` as "what content encodings have been applied to the object and
thus what decoding mechanisms must be applied" — so persisting `aws-chunked` would
hand a consumer a codec name for content that needs no decoding. `PutObject` does
not document it as persisted metadata, and `CreateMultipartUpload` scopes that value
to directory buckets.

A genuine codec alongside it is kept, in order, because an SDK streaming a
compressed body sends both tokens and dropping the header wholesale would lose the
codec that *is* applied to the stored object.

The header name is matched case-insensitively on both paths.

### Object system metadata

`Cache-Control`, `Content-Disposition`, `Content-Language` and `Expires` are recorded
on write and returned on every read, on all three write paths — `PutObject`,
`CreateMultipartUpload` → `CompleteMultipartUpload`, and `CopyObject`. Substrate
previously accepted them and discarded them, so a test asserting "the download
filename survives an upload" passed while verifying nothing.

They are stored verbatim and never interpreted: substrate does not evaluate a
`Cache-Control` lifetime, parse a `Content-Disposition` filename, or apply an
`Expires` date to anything. What is modeled is the observation — that a read reports
what the write set.

An absent header is **absent on the response, not empty**. `Cache-Control: ` and no
`Cache-Control` are different observations, and an SDK distinguishing nil from `""`
would otherwise report the wrong one.

**`Expires` is a string, never a parsed date.** A malformed value round-trips
unchanged rather than being normalized or dropped. Real S3 stores and returns what the
caller sent, and the Go SDK's own `GetObject` output deprecates its `time.Time`
`Expires` in favour of `ExpiresString` — "the unparsed value of the `Expires` field
from the service response". Parsing here would be lower fidelity, and would make a
consumer's parse-failure branch unreachable.

`Content-Type`, `Content-Encoding` and the storage class are user-controlled system
metadata too, but each has resolution rules of its own — a default of
`application/octet-stream`, `aws-chunked` filtering, and a `STANDARD`-means-absent
read rule — so they are documented in their own sections above.

### Server-side encryption

Three headers are recorded on write and returned on every read:

| Header | Recorded | Echoed |
|---|---|---|
| `x-amz-server-side-encryption` | Verbatim — `AES256`, `aws:fsx`, `aws:kms`, `aws:kms:dsse`, or any other token | Whenever set |
| `x-amz-server-side-encryption-aws-kms-key-id` | Verbatim, in whichever form was sent | Only alongside an algorithm, and only when a key was named |
| `x-amz-server-side-encryption-bucket-key-enabled` | As a boolean; only `true` (any case) enables it | Only when enabled |

**No cryptography is performed.** The object body is stored exactly as it arrived.
Encryption at rest is not observable through an API call, but *the encryption S3
reports for an object* is — and that report is the assertion a consumer is making.
Substrate previously accepted these headers and discarded them, so an object written
with encryption read back byte-identical to one written without it: a test could only
assert on what its own request carried, which proves the line that filled in the
request and nothing about the stored object.

They are recorded on `PutObject` and on `CreateMultipartUpload`, and echoed on those
two responses plus `GetObject`, `HeadObject` and `CompleteMultipartUpload`.
`CreateMultipartUpload` is the only place a multipart upload's encryption can be
supplied — Complete's request accepts only the SSE-C headers — so it is fixed for the
whole upload at creation and carried onto the assembled object.

An absent header is **absent on the response, not `false` or empty**. A write that
never mentioned the bucket-key header produces no bucket-key header, since an SDK
distinguishing a nil `*bool` from a `false` one would otherwise report the wrong
answer. The same rule keeps "no encryption named" distinguishable from "encryption
named", which is the observation that makes recording worth anything.

**The KMS key ID round-trips verbatim, which is a deliberate divergence.** KMS accepts
four forms — a bare UUID, `alias/name`, a key ARN and an alias ARN — and real S3
resolves any of them to the key ARN before reporting it. Substrate returns the string
the caller sent, because that is the string the consumer's configuration produced and
therefore the assertion they are trying to make. Resolving it would mean modeling KMS
aliases and cross-account ARNs to answer a question no consumer has asked. The
difference is observable: if key resolution is ever modeled, this decision has to be
revisited rather than silently overtaken.

Nothing is validated. A key ID sent with `AES256`, a bucket-key flag without
`aws:kms`, and an unrecognized algorithm token are all accepted and recorded, where
real S3 answers `400 InvalidArgument`; `UploadPart` restating an encryption header is
likewise accepted rather than refused. Those four rejections are
[#493](https://github.com/scttfrdmn/substrate/issues/493).

Also out of scope there, and worth knowing before relying on this:

- **Bucket default encryption.** `PutBucketEncryption` and its siblings are not
  modeled, so a write naming no encryption records none. Real S3 has applied SSE-S3 to
  every new object unconditionally since January 2023, so a real bucket never stores an
  unencrypted object — modeling that default would remove the absent-versus-set
  distinction above, which is why it is a deliberate decision rather than a side effect.
- **`CopyObject` records no encryption at all.** A copy's encryption comes from the
  request and, failing that, from the bucket default — never from the source. Neither
  exists yet, so substrate reports none for a copy rather than inheriting the source's.
  That is a stated gap, not a wrong answer: silently inheriting would hide exactly the
  bug this half exists to expose, where an in-place metadata copy or a storage-tier
  transition moves an SSE-KMS object off its customer managed key.
- **SSE-C** (`x-amz-server-side-encryption-customer-*`) is out of scope entirely; its
  key material would have to be discarded rather than recorded.

### Copying objects

`CopyObject`'s metadata behaviour is governed by two independent directives, both
defaulting to `COPY` when absent. An unrecognized value on either is `400
InvalidArgument` rather than a silent fall back to the default — a typo that quietly
preserved metadata is the kind of false success this emulator exists to surface.

| `x-amz-metadata-directive` | Destination `Content-Type`, `Content-Encoding`, `Cache-Control`, `Content-Disposition`, `Content-Language`, `Expires`, `x-amz-meta-*` |
|---|---|
| `COPY` (default) | Taken from the **source**; headers restated on the request are ignored |
| `REPLACE` | Taken from the **request**; anything not restated is dropped |

`COPY` preserving `Content-Encoding` is the documented behaviour: "when you copy an
object, user-controlled system metadata and user-defined metadata are also copied",
and `Content-Type`, `Content-Encoding`, `Content-Disposition` and `Cache-Control` are
all user-controlled. Only `x-amz-website-redirect-location` is documented as not
copied, and substrate does not model it.

The loss case is `REPLACE`: "you must explicitly specify all of the user-configurable
metadata present on the source object in your request, even if you are changing only
one of the metadata values". A `REPLACE` that omits `Content-Encoding` drops it, and
`Content-Type` falls back to `application/octet-stream`.

**The one directive governs the whole family.** S3 documents no per-header variant of
`x-amz-metadata-directive`, so a `REPLACE` restating only `Content-Type` also drops
the `Content-Disposition` download name and the `Cache-Control` lifetime the source
carried. Each header is independently restatable, but each must actually be restated.

`CopyObject` applies no `aws-chunked` filtering of its own and does not need to:
under `COPY` it inherits the source object's already-filtered value, and a copy
request carries no body, so no SDK sends a transfer encoding on it.

`x-amz-tagging-directive` works the same way for the tag-set: `COPY` (the default)
carries the source's tags, `REPLACE` takes them from `x-amz-tagging` as URL query
parameters (`stage=prod&owner=alice`), defaulting to an empty tag-set when that
header is absent.

**Storage class is never inherited.** "If the `x-amz-storage-class` header is not
used, the copied object will be stored in the `STANDARD` Storage Class by default" —
so an unqualified copy of a `STANDARD_IA` object yields a `STANDARD` one. This is
what makes an in-place `CopyObject` onto an object's own key with a new
`x-amz-storage-class` the tier-transition mechanism, and it is also the trap: a
transition that means to change only the class must restate the metadata it wants to
keep if it uses `REPLACE`.

Every request-derived value — storage class, both directives, both precondition sets
— is resolved before the first write, so a rejected copy leaves the destination
untouched.

### Ranged reads

`GetObject` and `HeadObject` honor a single-range `Range` header, returning `206
Partial Content` with `Content-Range` and a `Content-Length` equal to the range
served. Both advertise `Accept-Ranges: bytes`. `HeadObject` returns the same
status and headers with no body.

The edge cases matter more than the happy path, because S3 does **not** report an
error for most bad ranges — a caller cannot use a 416 to detect a malformed
request:

| `Range` (1000-byte object) | Result |
|---|---|
| `bytes=0-99` | `206`, `Content-Range: bytes 0-99/1000` |
| `bytes=900-` | `206`, `bytes 900-999/1000` |
| `bytes=-100` | `206`, `bytes 900-999/1000` (suffix range) |
| `bytes=0-99999` | `206`, **clamped** to `bytes 0-999/1000` — past EOF is not an error |
| `bytes=1000-1099` | `416 InvalidRange` with `Content-Range: bytes */1000` |
| `bytes=-0` | `416 InvalidRange` — a zero-length suffix is unsatisfiable |
| `bytes=abc`, `bytes=500-100` | `200` with the whole object — malformed ranges are ignored |
| `bytes=0-99,200-299` | `200` with the whole object — S3 serves only one range per GET |
| `items=0-99` | `200` with the whole object — units other than `bytes` are ignored |

Every range against a zero-byte object is unsatisfiable. A `416` body carries
`<ActualObjectSize>` and `<RangeRequested>` so a caller can correct the request
without a second round trip. Ranges compose with `versionId`.

Retrieving a specific part by `?partNumber=N` is not implemented.

### Conditional requests

#### Conditional writes

`PutObject`, `CopyObject` and `CompleteMultipartUpload` honor `If-None-Match` and
`If-Match`, evaluated against the current version of the destination key:

| Header | Destination state | Result |
|---|---|---|
| `If-None-Match: *` | Key absent | `200` — the write proceeds |
| `If-None-Match: *` | Key present | `412 PreconditionFailed` |
| `If-None-Match: *` | Current version is a delete marker | `200` — a delete marker is not an object |
| `If-None-Match: <anything but *>` | Any | `412 PreconditionFailed` — S3 expects only `*` on a write |
| `If-Match: "<etag>"` | ETag matches | `200` — the write proceeds |
| `If-Match: "<etag>"` | ETag differs | `412 PreconditionFailed` |
| `If-Match: "<etag>"` | Key absent, or the current version is a delete marker | `404 NoSuchKey` |
| either header | A conflict is [seeded](#seeding-conditionalrequestconflict) on the key | `409 ConditionalRequestConflict` |

A rejected conditional write is a no-op: the stored object is byte-identical
afterwards — body, ETag, size, `Content-Type` and user metadata all unchanged — and
a `412`-rejected `CompleteMultipartUpload` additionally leaves its upload open to be
retried or aborted. A `409`-rejected one does not; see below.

**Concurrency.** N concurrent `If-None-Match: *` writes to one key yield exactly one
`200` and N-1 `412`s; the same holds for N concurrent `If-Match` writes asserting
the same ETag, which is the compare-and-swap primitive optimistic locking needs.
This guarantee is **process-local**. Substrate implements it with a per-key mutex
held across the existence check and the write, because `StateManager` exposes no
compare-and-swap; it therefore holds for any number of goroutines or HTTP clients
against one emulator process, but would not hold across two emulator processes
sharing one state backend.

#### Seeding `ConditionalRequestConflict`

S3 returns `409 ConditionalRequestConflict` when a concurrent operation — in the
documented case, a delete — interferes with a conditional write between its
evaluation and its completion. It is a timing accident rather than a state a
request can assert, so substrate cannot derive it the way it derives a `412` from
the current object. Seeding is what makes the branch reachable.

**The branch matters because the three outcomes select different recovery paths,
and they are not interchangeable:**

| Outcome | What it means | The recovery AWS documents |
|---|---|---|
| `412 PreconditionFailed` | Another writer won the race | Re-read, recompute, retry the compare-and-swap |
| `404 NoSuchKey` on `If-Match` | The object is gone | Re-upload rather than retry the CAS |
| `409` on `PutObject` / `CopyObject` | A delete interleaved | Retry the request as-is (for `If-Match`, fetch the current ETag first) |
| `409` on `CompleteMultipartUpload` | A delete interleaved | **Abandon the upload ID** — re-do `CreateMultipartUpload` and re-upload every part |

A compare-and-swap loop that answers the last case like the first re-sends
`CompleteMultipartUpload` with an upload ID that can never complete again, and
spins until it gives up. **Substrate models that consequence: consuming a seeded
multipart conflict invalidates the upload, so a same-ID retry gets
`404 NoSuchUpload` and `ListParts` on it is gone too.** That inference is
substrate's — AWS documents the recovery advice, not the ID's fate — but without it
the broken loop passes.

```bash
# The next conditional PutObject on cond/k reports ConditionalRequestConflict.
curl -X POST http://localhost:4566/v1/s3/conditional-conflict \
  -d '{"bucket":"cond","key":"k","putConflicts":1}'

# CopyObject (evaluated against the destination key) and
# CompleteMultipartUpload have their own independent counters.
curl -X POST http://localhost:4566/v1/s3/conditional-conflict \
  -d '{"bucket":"cond","key":"dst","copyConflicts":1}'
curl -X POST http://localhost:4566/v1/s3/conditional-conflict \
  -d '{"bucket":"cond","key":"big","completeConflicts":1}'

# Apply to any key (wildcard).
curl -X POST http://localhost:4566/v1/s3/conditional-conflict -d '{"putConflicts":3}'

# Clear one, or all.
curl -X DELETE 'http://localhost:4566/v1/s3/conditional-conflict?bucket=cond&key=k'
curl -X DELETE http://localhost:4566/v1/s3/conditional-conflict
```

`bucket` and `key` must be given together; supplying one alone is a `400`, because
such a seed would be stored under a key no write can match — it would look armed
and never fire. A key-scoped seed is consulted before the wildcard, and when it is
exhausted the write falls through to the wildcard, so a spent key-scoped seed does
not mask a wildcard that still has budget.

**Conflicts are counted in occurrences, not measured as a duration**, for the same
reason as [the SQS consistency window](#seeding-the-create-then-lookup-consistency-window):
substrate's simulated clock advances with wall time from its baseline, so a
duration-based window would expire partway through a test and make assertions
wall-clock dependent. A counter is exactly reproducible.

Two ordering rules make the seed usable from a harness:

- A conflict is consumed **only after the preconditions pass**. A `412` or `404` is
  a determinate observation of the destination's current state and is reported as
  itself rather than replaced by a seeded race — and it does not spend the budget,
  so a request that was going to fail anyway cannot silently consume what the test
  armed for the conflict.
- An **unconditional** write to a seeded key is untouched and spends nothing. AWS
  documents this code only on the `If-Match` and `If-None-Match` members, so a
  plain `PutObject` never reports it.

The code and the `409` status are documented in those member docs and in the S3
user guide's conditional-writes page; **no message text is documented anywhere**,
and the API model carries no `ConditionalRequestConflict` shape, so substrate's
message is its own. Assert on the code and the status.

The `404` real S3 can return when a concurrent delete lands mid-write is still not
modeled as a race: substrate reaches that outcome deterministically, from an
`If-Match` against a key that is genuinely absent (the row above).

#### Conditional reads

`GetObject` and `HeadObject` honor all four RFC 9110 preconditions. They are
evaluated **before** the `Range` header, so a failed precondition is reported rather
than a partial response served:

| Header | Evaluation | Result |
|---|---|---|
| `If-None-Match` | Matches the object's ETag (or is `*`) | `304 Not Modified`, no body, `ETag` echoed |
| `If-None-Match` | Does not match | `200` |
| `If-Match` | Matches (or is `*`) | `200` |
| `If-Match` | Does not match | `412 PreconditionFailed` |
| `If-Modified-Since` | Object not modified since the date | `304 Not Modified` |
| `If-Unmodified-Since` | Object modified since the date | `412 PreconditionFailed` |

Two combination rules from the `GetObject` reference are implemented, both of which
stop a coarse date condition from overriding an exact entity assertion:

- `If-Match` true **and** `If-Unmodified-Since` false → `200`, not `412`.
- `If-None-Match` false **and** `If-Modified-Since` true → `304`, not `200`.

A precondition against an absent key is still `404 NoSuchKey` — there is no ETag to
compare. An unparseable date makes its condition inapplicable rather than failed
(per RFC 9110), so a malformed date never produces a spurious `412`. The three date
formats RFC 9110 requires a recipient to accept are all parsed. An empty header
value is a condition that cannot be met, distinct from an absent header.

ETag comparison ignores surrounding quotes, `W/` weak-validator prefixes, hex case
and whitespace, and a comma-separated list matches if any member does. Header names
are matched case-insensitively.

#### Conditional copies

`CopyObject` carries two independent sets: the unprefixed headers above gate
overwriting the destination, while `x-amz-copy-source-if-match`,
`x-amz-copy-source-if-none-match`, `x-amz-copy-source-if-modified-since` and
`x-amz-copy-source-if-unmodified-since` gate reading the source. Both are evaluated
before anything is written, so a rejected copy leaves the destination untouched.

Every failed copy-source condition is a `412`, including the case where the
equivalent `GetObject` would be a `304`: there is no cached entity for a
server-side copy to revalidate against.

### Multipart upload validation

`CompleteMultipartUpload` validates the parts list before assembling anything, so
the failure paths a consumer's retry and cleanup code exists to handle are
reachable:

| Condition | Result |
|---|---|
| A part other than the highest-numbered one is under 5 MB (5,242,880 bytes) | `400 EntityTooSmall` |
| A referenced part was never uploaded | `400 InvalidPart` |
| A supplied `ETag` does not match the stored part | `400 InvalidPart` |
| Part numbers not strictly ascending (including duplicates) | `400 InvalidPartOrder` |
| No `Part` elements, or a body that does not parse | `400 MalformedXML` |
| `uploadId` unknown, or already completed or aborted | `404 NoSuchUpload` |
| `uploadId` valid but for a different bucket or key | `404 NoSuchUpload` |

The final part may be any size, including zero, and a single-part upload is exempt
from the minimum entirely. Supplied ETags are compared ignoring surrounding quotes,
hex case, and whitespace, since clients differ on whether they echo back the quotes
S3 sends.

A rejected `CompleteMultipartUpload` writes nothing: no object appears at the key,
and the upload stays open — `ListMultipartUploads` still reports it until
`AbortMultipartUpload` (or a successful Complete) ends it. That makes "no orphan
upload was left behind" a property a test can assert by observing the emulator.

The `EntityTooSmall` body identifies the offending part:

```xml
<Error>
  <Code>EntityTooSmall</Code>
  <Message>Your proposed upload is smaller than the minimum allowed object size. Each part must be at least 5 MB in size, except the last part.</Message>
  <RequestId>SUBSTRATE</RequestId>
  <ETag>b6d81b360a5672d80c27430f39153e2c</ETag>
  <MinSizeAllowed>5242880</MinSizeAllowed>
  <ProposedSize>1024</ProposedSize>
  <PartNumber>1</PartNumber>
</Error>
```

#### Copying into a part

`UploadPartCopy` — a `PUT` to the destination key carrying `partNumber`, `uploadId`
**and** `x-amz-copy-source` — copies an existing object into a part of an open
upload. What distinguishes it from `CopyObject` is where the bytes land: **the
destination key stays absent until `CompleteMultipartUpload` assembles it.** A
`HeadObject` on the destination mid-upload is a `404`, and `ListParts` is the only
place the copied bytes are observable. Copied and uploaded parts mix freely in one
upload, and a copied part's checksum is computed under the upload's algorithm, so
`CompleteMultipartUpload` still assembles a `COMPOSITE` object checksum over the
mixture.

`x-amz-copy-source-range` selects a byte range of the source. Unlike a `GET`'s
`Range` header — which S3 treats as advisory, ignoring a malformed value and
clamping one that runs past the end — a copy-source range is part of the request's
meaning, so substrate refuses a range it cannot honor rather than silently copying
different bytes:

| Condition | Result |
|---|---|
| `bytes=first-last`, both offsets within the source | that range is copied |
| No range header | the whole source object is copied |
| Malformed, or missing either offset (`bytes=0-`, `bytes=-9`) | `400 InvalidArgument` |
| `last` at or beyond the source's size | `400 InvalidArgument` |
| Any range against a source of 5 MB or less | `400 InvalidRequest` — the reference's documented special error, since "you can copy a range only if the source object is greater than 5 MB" |
| `uploadId` unknown, or for a different bucket or key | `404 NoSuchUpload` |
| Copy source does not exist | `404 NoSuchKey` |

The `x-amz-copy-source-if-*` preconditions gate reading the source and answer
`412 PreconditionFailed` on failure. There are no destination preconditions, since
there is no destination object yet.

#### Metadata carried from CreateMultipartUpload

`CompleteMultipartUpload` accepts no object-metadata headers — per the AWS API
reference it takes only the checksum family, `x-amz-mp-object-size`, request-payer,
SSE-C and the conditional headers. So anything describing the finished object must
be supplied at `CreateMultipartUpload` and is carried on the upload until the object
is assembled:

| Supplied at Create | Applied to the assembled object |
|---|---|
| `Content-Type` | yes (defaults to `application/octet-stream`) |
| `Content-Encoding` | yes, less any `aws-chunked` token — see [Content-Encoding and aws-chunked](#content-encoding-and-aws-chunked) |
| `Cache-Control` | yes — see [Object system metadata](#object-system-metadata) |
| `Content-Disposition` | yes |
| `Content-Language` | yes |
| `Expires` | yes, stored verbatim |
| `x-amz-storage-class` | yes (empty means `STANDARD`) |
| `x-amz-checksum-algorithm` | yes — see [Additional checksums](#additional-checksums) |
| `x-amz-meta-*` | yes |

Setting one of these at `Complete` instead has no effect — the reference lists no
object-metadata header there, so substrate ignores them rather than applying them
late.

### Additional checksums

`PutObject`, `UploadPart`, `CopyObject`, `CreateMultipartUpload` and
`CompleteMultipartUpload` honor the `x-amz-checksum-*` family, and **verify** any
value the caller supplies. A wrong value is `400 BadDigest` and nothing is written —
the object does not appear at the key, and a rejected `UploadPart` leaves no part for
a later Complete to pick up.

All ten documented algorithms are recognized. Seven are computed and verified:

| Algorithm | Header | `FULL_OBJECT` | `COMPOSITE` |
|---|---|---|---|
| `CRC32` | `x-amz-checksum-crc32` | yes | yes |
| `CRC32C` | `x-amz-checksum-crc32c` | yes | yes |
| `CRC64NVME` | `x-amz-checksum-crc64nvme` | yes | **no** |
| `SHA1` | `x-amz-checksum-sha1` | no | yes |
| `SHA256` | `x-amz-checksum-sha256` | no | yes |
| `SHA512` | `x-amz-checksum-sha512` | no | yes |
| `MD5` | `x-amz-checksum-md5` | no | yes |

`XXHASH64`, `XXHASH3` and `XXHASH128` are recognized but answered with `501
NotImplemented`, because substrate has no implementation to check a supplied value
against. That is deliberate: storing a checksum nobody verified would make a
consumer's test pass on data real S3 would have rejected, which is the failure this
section exists to prevent. An algorithm name outside all ten is `400 InvalidRequest`.

Three request shapes are honored on a write:

| Request | Behavior |
|---|---|
| `x-amz-checksum-<alg>: <base64>` | Verified against the body; mismatch is `400 BadDigest` |
| `x-amz-sdk-checksum-algorithm: <NAME>` alone | Substrate computes and records the digest |
| Both, naming different algorithms | `400 BadDigest` |
| Two different `x-amz-checksum-*` headers | `400 InvalidRequest`, "Multiple checksum Types are not allowed" |
| A base64 value of the wrong width for the algorithm | `400 InvalidRequest`, distinct from `BadDigest` |

**Trailing checksums are read.** When the body is `aws-chunked` and `x-amz-trailer`
names a checksum header, the value is taken from the trailer that follows the
completion chunk — which is where every AWS SDK puts the checksum of a streamed
upload. A trailer whose name differs from what `x-amz-trailer` declared is `400
MalformedTrailerError`; so is a declared trailer that never arrives.

**Reading a checksum back** requires `x-amz-checksum-mode: ENABLED` on `GetObject` or
`HeadObject`. Without it the response carries no `x-amz-checksum-*` header and no
`x-amz-checksum-type`, so the absence is observable. A ranged `GET` returns the
whole object's checksum, not the range's.

**Multipart.** `CreateMultipartUpload` takes `x-amz-checksum-algorithm` (not the
`x-amz-sdk-` form) and an optional `x-amz-checksum-type`, echoing both back. An
absent type defaults to `COMPOSITE`, except for `CRC64NVME`, which has no composite
form and defaults to `FULL_OBJECT`. An unsupported algorithm/type pairing is `400
InvalidRequest` at **creation**, before any part is uploaded. A part supplying a
checksum under a different algorithm than the upload's is `400 InvalidRequest`.

`CompleteMultipartUpload` returns the object checksum as an XML **element**, not a
header:

```xml
<CompleteMultipartUploadResult>
  <Location>/bucket/key</Location>
  <Bucket>bucket</Bucket>
  <Key>key</Key>
  <ETag>"b6d81b360a5672d80c27430f39153e2c-2"</ETag>
  <ChecksumCRC32>Zm9vYmFy-2</ChecksumCRC32>
  <ChecksumType>COMPOSITE</ChecksumType>
</CompleteMultipartUploadResult>
```

A `COMPOSITE` value is the digest of the concatenated raw part digests with a
`-<part count>` suffix; a `FULL_OBJECT` value is the digest of every byte of the
assembled object, with no suffix. For the same bytes the two differ, which is the
point of distinguishing them. A `FULL_OBJECT` multipart checksum equals what a
single-part `PutObject` of the same bytes produces — a property a test can assert.
Complete also verifies a whole-object checksum supplied on the request itself, and
rejects an `x-amz-checksum-type` that disagrees with the upload's.

**`CopyObject` recomputes.** The destination's checksum is always a direct
full-object checksum of the copied bytes, under the source's algorithm unless the
copy names a new one. Copying a `COMPOSITE` multipart object therefore changes both
the value and the type even though the data is identical, matching S3.

**One deliberate divergence.** Real S3 attaches a default `CRC64NVME` checksum to
every object uploaded without one, so a checksum-mode `GET` always returns something.
Substrate records **no** checksum in that case. Synthesizing one would make a
round-trip assertion pass whether or not the consumer's writer actually sends a
checksum — the exact defect this support was added to expose. An absent checksum in
substrate means "your writer sent none".

### Task-completion records

A read of `tasks/<task_id>/completion.json` on any bucket resolves to a synthesized
spore.host task-completion record when no real object exists at that key. Substrate
does not run the task — this is the seedable completion *observation* only, so a
consumer's poll-until-done loop can be exercised instantly and reproducibly.

Absent a seed the key resolves to the nominal success record, so the happy path needs
no setup:

```console
$ aws s3api get-object --bucket results --key tasks/t1/completion.json /dev/stdout
{"task_id":"t1","exit_code":0,"state":"completed","started_at":"…","ended_at":"…"}
```

Seed an alternate outcome — a non-zero exit, a `failed` state, or a completion time in
the simulated future:

```
POST   /v1/spawn/task-completion   {"task_id","exit_code","state","started_at","ended_at"}
DELETE /v1/spawn/task-completion?taskId=<id>   (or with no query, clear all)
```

**`ended_at` gates presence on the simulated clock.** Before that time the record
reads as absent — `404 NoSuchKey` — which is the "still running" observation a poll
loop needs in order to loop at all. After it, the record is served.

**`HeadObject` resolves the record exactly as `GetObject` does**, reporting the same
`Content-Length`, `ETag`, `Content-Type` and `Last-Modified`, and honoring the same
clock gate. This matters because `aws s3 cp` and `aws s3 sync` HEAD before they GET,
so a HEAD that 404'd made the record unreadable through the CLI even though the GET
worked; an SDK `HeadObject` existence poll had the same problem in the worse
direction, since absence reads as "still running" and the loop never terminates.

**A real object always wins.** Staging an actual object at the completion key serves
it verbatim; the resolver only runs when the key is absent. A read naming an explicit
`versionId` never resolves either, since a synthesized record has no version history.
Both reads and the resolver's response path are otherwise ordinary, so ranged and
conditional reads apply to a synthesized record as to any other object.

**`ListObjectsV2` deliberately does not enumerate synthesized records.** A keyed read
works because the caller names the task, so the resolver has something to answer
about. A list is unkeyed, and substrate cannot enumerate the set of task IDs a
consumer might ask about — a list that invented entries would be a wrong answer, not a
more complete one. This asymmetry is a decision, not an oversight.

### Block Public Access

The `?publicAccessBlock` subresource is addressed as a bare query key on the bucket,
which is why it needs explicit routing: an unrouted `DELETE
/bucket?publicAccessBlock` is indistinguishable from `DeleteBucket`.

```
PUT    /bucket?publicAccessBlock   → 200, empty body
GET    /bucket?publicAccessBlock   → 200 + PublicAccessBlockConfiguration, or 404
DELETE /bucket?publicAccessBlock   → 204
```

**All four settings are always reported.** `BlockPublicAcls`, `IgnorePublicAcls`,
`BlockPublicPolicy` and `RestrictPublicBuckets` are each optional on the request, and
every one a `PUT` omits is recorded — and reported back — as `false`, matching S3.
`PutPublicAccessBlock` replaces the whole document rather than merging into it, so a
second call naming fewer settings clears the rest.

**An unconfigured bucket is a 404, not an all-false 200.** A bucket that has never
been the subject of a `PutPublicAccessBlock` returns `404
NoSuchPublicAccessBlockConfiguration`. The two states are deliberately
distinguishable: an all-false configuration a consumer wrote on purpose is a `200`
carrying four `false` elements. Reporting the unset case as all-false would tell a
caller "public access is not blocked" where AWS says "nothing is configured".

**Substrate does not apply S3's April 2023 default.** Real S3 enables all four
settings on buckets newly created through the API, CLI, SDKs or CloudFormation. In
substrate a new bucket has no configuration at all. That default is a property of
AWS-managed account and organization state substrate does not model, and seeding
every bucket with a configuration would make the `NoSuchPublicAccessBlockConfiguration`
path — the branch a consumer's error handling exists for — unreachable through the
public API. Call `PutPublicAccessBlock` to get a configured bucket, which is what the
SDKs and CloudFormation both do.

**`DeletePublicAccessBlock` is idempotent** and touches nothing but the
configuration. Deleting one that was never written is a `204`, which is what a
teardown path that deletes unconditionally relies on.

**`BlockPublicAcls` and `BlockPublicPolicy` are enforced at request time.** A bucket
carrying either setting refuses the call that would make it public:

| Setting | Refuses | Response |
|---------|---------|----------|
| `BlockPublicAcls` | `PutBucketAcl`, `PutObjectAcl` with a public ACL | `403 AccessDenied` / `Access Denied` |
| `BlockPublicAcls` | `PutObject`, `CopyObject`, `CreateMultipartUpload` whose request includes a public ACL | `403 AccessDenied` / `Access Denied` |
| `BlockPublicPolicy` | `PutBucketPolicy` with a public policy | `403 AccessDenied` / `Access Denied` |

The three create operations are their own bullet on the setting — "PUT Object calls
fail if the request includes a public ACL" — and were unenforceable until substrate
read an ACL from a create at all. **The refusal precedes every write**, so a refused
`PutObject` stores no object, a refused `CopyObject` stores no destination object, and
a refused `CreateMultipartUpload` leaves no upload ID behind; an overwrite refused this
way leaves the object that was already at the key untouched, body and ACL both. The
configuration consulted on a copy is the **destination** bucket's, since that is where
the object lands.

`CreateBucket` is the documented case substrate does **not** refuse; see below.

A rejection stores nothing: the bucket or object keeps the ACL or policy it already
had, matching "existing policies and ACLs for buckets and objects aren't modified".
Deleting the configuration re-allows what it was refusing, per "removing a block
public access setting causes a bucket or object with a public policy or ACL to again
be publicly accessible". The configuration read for `PutObjectAcl` is the *bucket's* —
"Amazon S3 doesn't support block public access settings on a per-object basis".

Neither operation documents an Errors section covering this, so the `AccessDenied` /
`Access Denied` / `403` triple comes from observed real-AWS behaviour rather than from
the API model: a blocked `PutBucketPolicy` surfaces through the CLI as `An error
occurred (AccessDenied) when calling the PutBucketPolicy operation: Access Denied`.

**A public ACL is one that grants any permission to a predefined public group.**
Substrate matches the grantee URI against
`http://acs.amazonaws.com/groups/global/AllUsers` and
`.../AuthenticatedUsers`, per "Amazon S3 considers a bucket or object ACL public if it
grants any permissions to members of the predefined `AllUsers` or `AuthenticatedUsers`
groups". `AuthenticatedUsers` is every AWS account, not every account in yours, which
is why it counts despite the name. The permission itself is not inspected — `READ`,
`WRITE`, `READ_ACP`, `WRITE_ACP` and `FULL_CONTROL` all count. All three ways a public
ACL arrives are covered: the `x-amz-acl` canned header (`public-read`,
`public-read-write`, `authenticated-read`), an XML `Grant` naming a public group URI,
and an `x-amz-grant-*` header whose grantee list contains one. See
[Access control lists](#access-control-lists) for how each form resolves.

**`CreateBucket` with a public ACL is accepted, and that is a stated gap.** The
setting's third bullet says "PUT Bucket calls fail if the request includes a public
ACL", but the configuration that refuses such a call is the **account-level** one — a
bucket-level configuration cannot exist before the bucket does. Substrate models no
account-level Block Public Access, so gating this one operation would mean modeling a
control the emulator does not otherwise have. A bucket created with `--acl public-read`
therefore succeeds and reports the public grant, and the next `PutBucketAcl` on it is
subject to whatever configuration has since been written.

**A public policy is decided by assuming public and then trying to disqualify —
not by looking for `Principal: "*"`.** This is stronger than wildcard-detection and is
the part a naive implementation gets backwards. Per "When evaluating a bucket policy,
Amazon S3 begins by assuming that the policy is public. It then evaluates the policy to
determine whether it qualifies as non-public", a statement is non-public only when it
grants access solely to *fixed* values — no `*`, no `?`, no `${...}` IAM policy
variable — either through its `Principal` or through a `Condition` on one of
`aws:SourceIp`, `aws:SourceArn`, `aws:SourceVpc`, `aws:SourceVpce`, `aws:SourceOwner`,
`aws:SourceAccount`, `aws:userid`, `aws:PrincipalOrgID`, `aws:PrincipalArn`,
`aws:PrincipalAccount`, `s3:DataAccessPointArn` or `s3:DataAccessPointAccount`.

The consequence, and AWS's own example:

| Policy | Public? |
|--------|---------|
| `Principal: "*"`, no condition | yes |
| `Principal: "*"` + `StringLike aws:SourceVpc: "vpc-*"` | **yes** — the narrowing value is itself a wildcard |
| `Principal: "*"` + `StringEquals aws:SourceVpc: "vpc-91237329"` | no |
| `Principal: {"AWS": "arn:aws:iam::123456789012:root"}` | no |
| `Principal: {"AWS": "arn:aws:iam::123456789012:user/*"}` | yes |
| `Principal: "*"` + `aws:SourceIp: "203.0.113.0/24"` | no |
| `Principal: "*"` + `aws:SourceIp: "0.0.0.0/1"` | **yes** — broader than `/8` |
| `Effect: Deny`, `Principal: "*"` | no |
| A fixed cross-account grant **plus** one public statement | yes |

Three further rules follow from the same page. Only an `Allow` can make a policy
public. A single surviving public statement makes the *whole* policy public — the
guide's worked example, where one public statement disables an otherwise-legal
cross-account grant. And an `aws:SourceIp` range pins nothing when it is "broader than
`/8` for IPv4 and `/32` for IPv6 (excluding RFC1918 private ranges)", so a bucket
policy conditioned on `0.0.0.0/0` is public even though it contains no wildcard
character; the RFC1918 exclusion is what keeps a unique-local IPv6 range from tripping
the `/32` bound.

A body that parses as JSON but not as a policy document is **not** treated as public.
`PutBucketPolicy` already rejects a non-JSON body with `400 MalformedPolicy` before
this check runs, and the public-access check is not a second validity check — a
malformed-but-JSON document keeps whatever answer it had before enforcement existed.

**`IgnorePublicAcls` and `RestrictPublicBuckets` remain recorded-only.** Both govern
how an *incoming* request is evaluated against an existing ACL or policy rather than
which write is refused, and substrate has no unauthenticated or cross-account request
path to deny — every request it serves is already the bucket owner's. Substrate also
does not model the guide's unsupported-action clause (S3 treats a statement granting an
action S3 does not support as potentially public), which would need an authoritative
list of every supported `s3:` action.

### Access control lists

An ACL named on a write is **stored**, and reported by `GetBucketAcl` /
`GetObjectAcl`. Six operations resolve one:

| Operation | ACL source | On no ACL header |
|-----------|-----------|-----------------|
| `CreateBucket` | `x-amz-acl`, `x-amz-grant-*` | nothing stored; the default owner-only ACL is reported |
| `PutObject` | `x-amz-acl`, `x-amz-grant-*` | nothing stored, and any ACL the key already had is **cleared** |
| `CopyObject` | the copy request's own headers only | as `PutObject` — a copy never inherits the source's ACL |
| `CreateMultipartUpload` | `x-amz-acl`, `x-amz-grant-*`, carried to the object `CompleteMultipartUpload` assembles | as `PutObject` |
| `PutBucketAcl` | an XML body, else the headers | an empty request resolves to `private` |
| `PutObjectAcl` | an XML body, else the headers | an empty request resolves to `private` |

Before this, `PutObject` and `CreateBucket` read no ACL header at all, so the ACL
`GetObjectAcl` reported was never the one the write set — and an ACL expressed through
`x-amz-grant-*` was stored by **no** operation, `PutBucketAcl` and `PutObjectAcl`
included: the grant headers were parsed only to decide whether Block Public Access
should refuse.

**A write replaces the whole ACL, not part of it.** "You cannot use `PutObject` to only
update a single piece of metadata for an existing object. You must put the entire object
with updated metadata" — so an overwrite naming no ACL reports owner-only afterwards
even if the key previously carried a public grant, whether that grant arrived through
the original `PutObject` or through a later `PutObjectAcl`. `CompleteMultipartUpload`
and `CopyObject` replace it the same way, and `CreateBucket` clears any ACL a
same-named bucket left behind when it was deleted.

**A copy takes its ACL from the request and nowhere else**: "When you copy an object,
the ACL metadata is not preserved and is set to `private` by default. Only the owner has
full access control. To override the default ACL setting, specify a new ACL when you
generate a copy request." That is the opposite of the metadata families, where `COPY` is
the default directive — see [Copying objects](#copying-objects).

**A multipart upload's ACL is fixed at create.** `CompleteMultipartUpload`'s request
accepts no ACL header, exactly as it accepts no encryption header, so an ACL not named
at `CreateMultipartUpload` cannot be supplied later.

**The canned names resolve from the user guide's Canned ACL table**, and three of them
mean different things on a bucket than on an object:

| `x-amz-acl` | Bucket | Object |
|-------------|--------|--------|
| `private` | owner `FULL_CONTROL` | owner `FULL_CONTROL` |
| `public-read` | + `AllUsers` `READ` | + `AllUsers` `READ` |
| `public-read-write` | + `AllUsers` `READ`, `WRITE` | + `AllUsers` `READ`, `WRITE` |
| `authenticated-read` | + `AuthenticatedUsers` `READ` | + `AuthenticatedUsers` `READ` |
| `log-delivery-write` | + `LogDelivery` `WRITE`, `READ_ACP` | owner-only — "Applies to: Bucket" |
| `aws-exec-read` | owner-only | owner-only |
| `bucket-owner-read` | owner-only — S3 "ignores it" on a bucket | owner-only |
| `bucket-owner-full-control` | owner-only | owner-only |

The owner grant is present in every row because a canned ACL is applied on top of the
ACL the resource already has: "When Amazon S3 receives a request with a canned ACL in
the request, it adds the predefined grants to the ACL of the resource". `aws-exec-read`
resolves to owner-only because it grants Amazon EC2 `READ` and AWS does not publish that
canonical user ID; the two `bucket-owner-*` names collapse because substrate has one
owner identity per bucket, so the object owner and the bucket owner are the same
principal. **`authenticated-read` is public** by Block Public Access's own definition,
which is the row that matters most: substrate resolved it to owner-only before, so the
block could be walked straight through.

A canned name substrate does not recognize resolves to owner-only rather than being
refused. The per-operation Valid Values lists differ — `CreateBucket` and `PutBucketAcl`
document four names, `PutObject`, `PutObjectAcl`, `CopyObject` and
`CreateMultipartUpload` seven — and no error code is documented for a name outside them,
so refusing would mean rejecting a request real S3 may accept.

**The five `x-amz-grant-*` headers add to the default ACL**, they do not replace it:
"you specify explicit access permissions and grantees … These permissions are then added
to the ACL on the object", so the owner keeps `FULL_CONTROL`.

| Header | Permission |
|--------|-----------|
| `x-amz-grant-full-control` | `FULL_CONTROL` |
| `x-amz-grant-read` | `READ` |
| `x-amz-grant-read-acp` | `READ_ACP` |
| `x-amz-grant-write` | `WRITE` |
| `x-amz-grant-write-acp` | `WRITE_ACP` |

Each value is a comma-separated list of `type=value` pairs — `id="abc123",
uri="http://acs.amazonaws.com/groups/global/AllUsers"` — with the quotes optional and
the type read case-insensitively. Only `id` and `uri` produce a grantee, which is the
pair the user guide's "Who is a grantee?" section lists. `PutObject`'s request syntax
omits `x-amz-grant-write` (the permissions table gives `WRITE` no object meaning);
substrate records it there anyway rather than inventing a rejection no error code is
documented for.

**An `emailAddress` grantee is skipped, not refused.** S3 ended support for it — "As of
October 1, 2025, Amazon S3 has discontinued support for Email Grantee Access Control
Lists (ACLs) … the request will receive an `HTTP 405` (Method Not Allowed) error" — and
substrate's clock is past that date, but the 405 is Region-conditional and applies to the
XML body form too, so returning it is tracked separately rather than guessed at.

**The two forms are documented mutually exclusive** — "If you use these ACL-specific
headers, you cannot use the `x-amz-acl` header to set a canned ACL" — but no error code
is documented for sending both, so substrate resolves rather than refuses and the grant
headers win, being the more specific expression. On `PutBucketAcl` and `PutObjectAcl` an
XML body wins over both.

**The owner identity is derived from the bucket name** (`<bucket>-owner`), because
substrate has no canonical user IDs: there is one owner per bucket and it owns everything
in it. An ACL's `Owner` and its `CanonicalUser` `FULL_CONTROL` grantee are therefore
always the same ID, and an object's owner is its bucket's.

### Event notifications

`PutBucketNotificationConfiguration` accepts the API's XML body and
`GetBucketNotificationConfiguration` returns it in the same shape. Note the element
names, which are not the SDK member names:

| Destination | Configuration element | Destination element |
|---|---|---|
| SNS topic | `TopicConfiguration` | `Topic` |
| SQS queue | `QueueConfiguration` | `Queue` |
| Lambda function | `CloudFunctionConfiguration` | `CloudFunction` |
| EventBridge | `EventBridgeConfiguration` | — (no members) |

Each configuration takes an optional `Id`, one or more repeated `Event` elements, and
an optional `Filter` → `S3Key` → repeated `FilterRule` with `Name` (`prefix` or
`suffix`) and `Value`. Substrate also accepts a JSON body keyed on the SDK's member
names as a convenience; the response is always XML.

A configured notification is **dispatched**: `PutObject` and `DeleteObject` invoke the
named Lambda function, send to the named SQS queue, and publish to the named SNS
topic, with the key filter applied. EventBridge delivery is recorded and reported but
not dispatched — substrate has no bus-to-target path for S3 events.

An **empty** `NotificationConfiguration` is the documented way to turn notifications
off, and an unconfigured bucket reads back as one. A non-empty body naming no
recognized element is `400 MalformedXML` rather than being accepted as a disable: an
XML decoder reports no error for a body whose elements match no field, so without that
refusal a body with the wrong element names is indistinguishable from a deliberate
disable, which is how a configuration could be accepted with a `200` and silently
never fire (#542).

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::S3::Bucket | BucketName | |

### Cost

S3 operation costs match AWS list pricing. PUT/COPY/POST/LIST operations are
$0.005 per 1,000. GET/SELECT operations are $0.0004 per 1,000.

---

## Lambda

**Endpoint:** `lambda.{region}.amazonaws.com`
**Protocol:** REST/JSON

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateFunction | Stores function metadata; no actual execution; records [`CodeSize` and `CodeSha256`](#what-codesize-and-codesha256-report) from the deployment package |
| GetFunction | Reports `Code.ImageUri` with `RepositoryType: ECR` for an [image-packaged function](#an-image-packaged-function-reports-its-image) |
| UpdateFunctionCode | Re-derives [`CodeSize` and `CodeSha256`](#what-codesize-and-codesha256-report) from the new package; an update carrying no package changes neither |
| UpdateFunctionConfiguration | |
| DeleteFunction | |
| ListFunctions | |
| InvokeFunction | Returns stub `{"statusCode":200,"body":"null"}` |
| CreateEventSourceMapping | |
| DeleteEventSourceMapping | |
| ListEventSourceMappings | |
| TagResource | |
| UntagResource | |
| ListTags | |

### What CodeSize and CodeSha256 report

`CodeSize` is "the size of the function's deployment package, in bytes" and
`CodeSha256` is "the SHA256 hash of the function's deployment package". What
substrate can report depends on whether it holds the package:

| Code source | `CodeSize` | `CodeSha256` |
|---|---|---|
| `Code.ZipFile` (inline, base64) | the decoded package's length | the real SHA256 of those bytes |
| `Code.S3Bucket` + `Code.S3Key` | the S3 object's recorded length | the object's **ETag**, not a SHA256 — see below |
| `Code.S3ObjectVersion` | that version's length | that version's ETag |
| `Code.ImageUri` | 0 — an image is not a package substrate holds | a digest of the image URI |
| no `Code` at all | 0 | empty |

Two of these are substrate's own decisions rather than the API model:

**The digest of an S3-sourced package is the object's ETag.** Substrate does not
fetch the object's bytes — nothing executes them unless they arrived inline — so it
cannot compute the SHA256 real Lambda would report. It reports the ETag instead,
unquoted. For a single-part upload that ETag is the MD5 of the body, so it changes
exactly when the package changes, which is what a caller comparing digests across
deploys is asking. Do not assert that it equals a SHA256 you computed yourself; do
assert that it changes when you upload different bytes and does not when you do not.
An image-packaged function's digest is likewise derived from its URI.

**An absent S3 object does not fail the create.** Real Lambda refuses a
`CreateFunction` naming an object that is not there. Substrate accepts it, logs a
warning, and reports `CodeSize: 0` with no digest. The reason is that substrate's S3
and Lambda state are independent and a template may legitimately name an object a
test never uploaded; failing the create would make a stack undeployable for a reason
unrelated to what the test is checking. A deleted object — one hidden by a delete
marker — counts as absent, not as a zero-length package.

`CodeSize` and `CodeSha256` describe the package. `RevisionId` does not: it is not a
digest of anything and advances on every `UpdateFunctionCode`, including one that
changes no code.

### An image-packaged function reports its image

`Code.ImageUri` is the documented spelling and implies `PackageType: Image`, which a
request need not state — real Lambda rejects an `ImageUri` alongside
`PackageType: Zip`, so inferring it cannot contradict a valid request. A top-level
`ImageUri` is also accepted, for compatibility with what substrate took before
`Code.ImageUri` worked; `Code.ImageUri` wins when both are sent.

`GetFunction` reports such a function through `Code.RepositoryType: ECR` with
`ImageUri` and `ResolvedImageUri`, and no `Location` — `RepositoryType` is "the
service that's hosting the file", and reporting a presigned S3 URL for an image is a
claim a caller can act on and be wrong about.

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::Lambda::Function | FunctionName | `Code` is deployed in every form; [inline `ZipFile` is zipped](#an-inline-zipfile-is-zipped-into-a-package) |
| AWS::Lambda::EventSourceMapping | — | |

### An inline ZipFile is zipped into a package

The resource type's `Code.ZipFile` and the API's `Code.ZipFile` are not the same
thing, which is the one place the deployer does more than forward a property.

The resource type's is "the source code of your Lambda function": "CloudFormation
places it in a file named `index` and zips it to create a deployment package". The
API's is "the base64-encoded contents of the deployment package". So substrate builds
the archive CloudFormation would have built — a single entry named `index` with the
extension the runtime reads (`.js` for `nodejs*`, `.py` for `python*`, bare `index`
otherwise) — and sends that. `CodeSize` is therefore the **archive's** length, not the
source's, and `CodeSha256` is a digest of real bytes.

The archive carries no timestamps, so the same template deployed twice produces the
same package and the same digest. A digest that changed on every deploy would not be
worth comparing.

`Code.S3Bucket`, `Code.S3Key`, `Code.S3ObjectVersion` and `Code.ImageUri` are
forwarded as the API spells them, and each goes through the template's `Ref` and
pseudo-parameter resolution like every other property.

### Cost

Lambda invocations: $0.0000002 per request.

---

## SQS

**Endpoint:** `sqs.{region}.amazonaws.com`
**Protocol:** AWS Query (form-encoded, `Action=` parameter)

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateQueue | Supports FifoQueue, VisibilityTimeout attributes; [`QueueNameExists`](#queuenameexists) when a name is reused with differing attributes; [seedable `QueueDeletedRecently`](#seeding-queuedeletedrecently) |
| GetQueueUrl | [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent; [seedable consistency window](#seeding-the-create-then-lookup-consistency-window) |
| GetQueueAttributes | [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent; [seedable consistency window](#seeding-the-create-then-lookup-consistency-window); [attribute defaults](#queue-attribute-defaults) |
| SetQueueAttributes | [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent |
| DeleteQueue | [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent |
| ListQueues | |
| SendMessage | Returns MessageId; [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent; enforces [`MaximumMessageSize`](#message-size-enforcement); stores [message attributes](#message-attributes) and returns `MD5OfMessageAttributes`; enforces the [attribute count, name, type and `Number` rules](#attribute-rules) |
| SendMessageBatch | Enforces both the [per-message and batch-total size limits](#message-size-enforcement); stores [message attributes](#message-attributes) per entry; reports an [attribute-rule violation per entry](#batch-failures-are-per-entry) in `Failed` at HTTP 200 |
| ReceiveMessage | Supports MaxNumberOfMessages, WaitTimeSeconds; [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent; returns [message attributes](#message-attributes) for the names requested |
| DeleteMessage | [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent |
| DeleteMessageBatch | |
| ChangeMessageVisibility | [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent |
| PurgeQueue | [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent |

### QueueDoesNotExist

Every operation that names a queue fails with `QueueDoesNotExist`, HTTP 400, when
that queue is absent — not the legacy `AWS.SimpleQueueService.NonExistentQueue`.

The distinction decides whether a consumer can catch the error as a typed
exception. SQS is an `awsQueryCompatible` JSON service, and the dotted form is the
query-compatibility alias AWS sends in an `x-amzn-query-error` header, not in
`__type`:

- **botocore** derives the exception class from the resolved error code, so the
  legacy string resolves to a bare `ClientError` and
  `except sqs.exceptions.QueueDoesNotExist` never matches.
- **aws-sdk-go-v2** dispatches on `strings.EqualFold("QueueDoesNotExist", …)`; the
  legacy string appears nowhere in the `sqs` module, so
  `errors.As(err, &types.QueueDoesNotExist{})` never matched either.

SQS errors are emitted as JSON regardless of the request protocol, since substrate
resolves the error protocol per service and SQS is JSON-RPC. A query-protocol
request therefore gets a JSON error document rather than the XML `<Error>` shape.

### Seeding the create-then-lookup consistency window

AWS documents that you "must wait at least one second after the queue is created
to be able to use the queue", so a real `CreateQueue` → `GetQueueUrl` → retry loop
can legitimately see `QueueDoesNotExist` for a queue that exists. Substrate
resolves a new queue instantly, which makes that retry path unreachable and any
test of it vacuous. Seeding is what makes the window observable:

```bash
# The next 2 GetQueueUrl calls on run-q report QueueDoesNotExist.
curl -X POST http://localhost:4566/v1/sqs/consistency \
  -d '{"queueName":"run-q","getUrlMisses":2}'

# GetQueueAttributes has its own independent counter.
curl -X POST http://localhost:4566/v1/sqs/consistency \
  -d '{"queueName":"run-q","getAttributesMisses":1}'

# Apply to any queue (wildcard).
curl -X POST http://localhost:4566/v1/sqs/consistency -d '{"getUrlMisses":3}'

# Clear one, or all.
curl -X DELETE 'http://localhost:4566/v1/sqs/consistency?queueName=run-q'
curl -X DELETE http://localhost:4566/v1/sqs/consistency
```

A name-scoped seed is consulted before the wildcard. When a name-scoped seed is
exhausted the lookup falls through to the wildcard, so an empty named seed does not
mask a wildcard that still has budget.

**The window is counted in misses, not measured as a duration.** Substrate's
simulated clock advances with wall time from its baseline, so a duration-based
window would expire partway through a test and make "still missing" assertions
wall-clock dependent — which no test here may be. A miss counter is exactly
reproducible.

Two ordering rules make the seed usable from a harness:

- A lookup miss is consumed **only when the queue actually exists**. Seeding before
  `CreateQueue` is safe: lookups against the genuinely absent queue still fail, but
  they do not spend the budget. (`deletedRecentlyMisses` is the deliberate
  exception — see [below](#seeding-queuedeletedrecently).)
- A seed counts down the next N misses and **does not re-arm on `CreateQueue`**.
  `CreateQueue` is idempotent here (it returns the existing URL), so "after its
  CreateQueue" would be ambiguous when create runs twice, and re-arming would mean
  the data path writes control-plane state.

All three counters — including [`deletedRecentlyMisses`](#seeding-queuedeletedrecently)
— default to 0, so an unseeded queue behaves exactly as before: instantly
resolvable. Seeds live in the state store, so `POST /v1/state/reset` clears them
along with everything else.

### Queue attribute defaults

`GetQueueAttributes` reports these for a queue created without naming them:

| Attribute | Default |
|---|---|
| `VisibilityTimeout` | `30` |
| `MaximumMessageSize` | `1048576` — 1 MiB, per the CreateQueue reference |
| `MessageRetentionPeriod` | `345600` |
| `DelaySeconds` | `0` |
| `ReceiveMessageWaitTimeSeconds` | `0` |

`262144` (256 KiB) is the historical limit rather than the current default, and is
what substrate reported until #439. An explicitly requested value is always honored
— 256 KiB is still a legal size.

These defaults also decide what counts as a
[`QueueNameExists`](#queuenameexists) conflict, since an existing queue's unset
attributes are resolved through them before comparing.

### Message size enforcement

`SendMessage` rejects a message larger than the queue's **effective**
`MaximumMessageSize` with `InvalidParameterValue`, HTTP 400:

```
One or more parameters are invalid. Reason: Message must be shorter than 1048576 bytes.
```

"Effective" means resolved through the [default](#queue-attribute-defaults) when the
attribute is unset, so a queue created with no attributes enforces 1 MiB. The limit is
read through the same default `GetQueueAttributes` reports, so the number a caller
reads back is by construction the number that is enforced. The limit named in the
message is the one that actually applied — a queue configured at 1 KiB says 1024.

The boundary is **inclusive**: a message of exactly the limit is accepted. AWS's
wording is "must be shorter than N bytes", but N is the documented maximum *size*, so
the largest legal message is N bytes.

**Message attributes count toward the size.** The measured total is the body plus, for
every attribute, its name, its data type, and its value — a binary value counted as
its raw (decoded) byte length. The developer guide is explicit that "all components of
a message attribute are included in the 1 MiB message size restriction", and the
per-component breakdown is the one AWS's own Extended Client Library uses to decide
whether a payload needs offloading to S3. Message *system* attributes are excluded,
per the `SendMessage` reference.

Attributes are also stored and returned — see [Message
attributes](#message-attributes).

`SendMessageBatch` enforces two limits, both 1 MiB, as the reference states: "the
maximum allowed individual message size and the maximum total payload size (the sum of
the individual lengths of all of the batched messages) are both 1 MiB".

| Condition | Error |
|---|---|
| Combined payload of all entries over 1 MiB | `BatchRequestTooLong` |
| One entry over the queue's per-message limit | `InvalidParameterValue` |

Because the two limits are equal on a default queue, a batch carrying a single
oversized entry breaches the total as well — and real AWS reports
`BatchRequestTooLong` for that case, not the per-message error, so the total is checked
first. The queue's `MaximumMessageSize` is a *per-message* cap and does not lower the
request payload cap: ten legal 1 KiB entries on a queue configured at 1 KiB are
accepted.

A rejected send or batch enqueues **nothing** — a partially applied batch would leave a
retry to re-send the entries that already landed. On a FIFO queue the size check runs
before the deduplication ID is recorded, so a corrected retry reusing the same
`MessageDeduplicationId` is delivered rather than swallowed as a duplicate.

An unparseable or non-positive `MaximumMessageSize` falls back to the default rather
than to zero, which would make the queue reject every message including an empty one.

**Provenance.** `SendMessage` declares no oversized-message error in the API model:
its `InvalidMessageContents` is documented as a character-set error, and
`BatchRequestTooLong` is declared only on `SendMessageBatch`. The per-message code and
both message wordings therefore come from **observed real-AWS responses** rather than
a doc citation — captured SDK errors carrying `code: 'InvalidParameterValue'` with
HTTP 400, and `BatchRequestTooLong: Batch requests cannot be longer than N bytes. You
have sent M bytes.` The same strings appear in independent reimplementations, which
corroborates them as transcribed AWS text.

### Message attributes

User-defined message attributes are stored on send and returned on receive, for both
`SendMessage` and `SendMessageBatch`, under both protocols. A consumer routing on an
attribute — a `messageType` discriminator, a trace ID, a tenant key — reads back what
it sent.

**Attributes are returned only when the receive asks for them.** This is the part that
is easy to get wrong in the permissive direction: a consumer whose production caller
never sets `MessageAttributeNames` would pass a test against an emulator that
volunteered them, then read none from real SQS.

| `MessageAttributeName` | Returned |
|---|---|
| omitted | nothing |
| `All` | every attribute |
| `.*` | every attribute |
| `messageType` | that attribute, if the message carries it |
| `trace.*` | every attribute whose name starts with `trace.` |

A named attribute the message does not carry is simply absent, not an error — the
selector says what to return, not that it must exist. The query protocol numbers the
selectors `MessageAttributeName.1`, `.2`, …; the JSON protocol sends a
`MessageAttributeNames` array.

`MD5OfMessageAttributes` is returned on `SendMessage`, on each `SendMessageBatch`
result entry, and on each received message. It is computed with the algorithm published
in the developer guide under "Calculating the MD5 message digest for message
attributes": sort by name, then per attribute append a 4-byte big-endian length and the
UTF-8 name, the same for the data type, one transport byte (`1` for String and Number,
`2` for Binary), then the value's length and bytes.

Two details of that algorithm are load-bearing, and substrate's implementation is
pinned against three real-AWS digests that fail if either is wrong:

- **A binary value is hashed raw, not base64.** Base64 is the wire form, so hashing
  what travels is the natural mistake; it produces `5ff413c9dc7bd18abea88ca05643f902`
  where AWS produces `049075255ebc53fb95f7f9f3cedf3c50` for the same input. This is
  the same raw-versus-encoded distinction [message size
  enforcement](#message-size-enforcement) makes, now with a hash to settle it.
- **A custom data-type suffix is included in full.** `Number.java.lang.Long` hashes as
  the whole 21-byte string, not as its `Number` base type.

`MD5OfMessageAttributes` is **omitted entirely** from a response for a message with no
attributes, rather than reported as the MD5 of zero bytes. A digest of nothing is a
value a caller could compare against and "successfully" verify, which is worse than no
value at all.

On a receive, the digest covers **what is being returned**, not what was sent: a
request naming a subset gets that subset's digest, since the digest exists so a caller
can checksum the attributes in hand. Attributes come back in name order, which real SQS
does not promise but determinism here does.

A deduplicated FIFO send reports the digest of *that request's* attributes rather than
the stored original's, for the same reason: the digest is a checksum of what the caller
sent.

#### Attribute rules

Every rule below is checked on `SendMessage` and on each `SendMessageBatch` entry, under
both protocols, and every rejection is `InvalidParameterValue` with HTTP 400. A rejected
send **enqueues nothing** — on a FIFO queue the check runs before the deduplication ID is
recorded, so a corrected retry reusing the same `MessageDeduplicationId` is delivered
rather than swallowed as a duplicate.

| Rule | Rejected |
|---|---|
| count | more than 10 attributes on one message |
| name length | 256 bytes or more |
| name characters | anything outside `A-Z`, `a-z`, `0-9`, `_`, `-`, `.` |
| reserved prefix | starting with `AWS.` or `Amazon.`, **any casing** |
| periods | a leading or trailing `.`, or two in sequence |
| type | a `DataType` not prefixed `String`, `Number` or `Binary`; an absent one |
| type length | 256 bytes or more |
| empty value | a `String` attribute with no value |
| `Number` value | not a decimal number, or outside −10^128 … 10^126 |

Both length bounds are **exclusive**: 255 bytes is legal, 256 is not. The developer guide
says "up to 256 characters" while the error text says "must be shorter than 256 Bytes";
the error is the more specific evidence, and the conflict is recorded rather than quietly
resolved.

A custom suffix on a type is legal and preserved — `Number.java.lang.Long`, `Binary.gif`
— and a `Number` is range-checked whatever its suffix. Scientific notation (`1e5`) is
accepted, which is the permissive reading of a detail no source settles. Uniqueness
within a message is structural rather than checked: attributes are keyed by name.

**The reserved-prefix check is case-insensitive**, so `aws.trace` and `AwS.trace` are
refused alongside `AWS.trace`. This is the **opposite** of EC2's `aws:` tag-key rule,
where [`AWS:foo` is a legal key](#reserved-tag-keys) — the two services document
different rules, and unifying them would break one of the two. A name merely beginning
with those letters and no period (`AWSfoo`) is not reserved.

A message breaking two rules always reports the same one: attributes are visited in
sorted name order, because a walk over the Go map would report one error on some runs and
the other on others.

#### Batch failures are per entry

`SendMessageBatch` reports an attribute violation as a **`BatchResultErrorEntry` in
`Failed`** with `SenderFault: true`, at HTTP **200** — the offending entry does not
enqueue while its siblings do. This is what the reference warns about: "you should check
for batch errors even when the call returns an HTTP status code of `200`". `Failed` is
always present, empty rather than absent on a fully successful batch. The ten-attribute
maximum is per message, so three entries of nine attributes each is legal.

That differs deliberately from the [size checks](#message-size-enforcement) ten lines
away in the same operation, which fail the **whole request** with `BatchRequestTooLong`:
the payload cap is a documented property of the aggregate the caller transmitted, while a
malformed attribute is a defect in one entry.

#### Provenance

Message text carries different weights, and the code says which is which:

- **Real-AWS captures**: the count rejection (an SDK exception quoting a Request ID and
  status 400), the `Number` cast failure (`Can't cast the value of message (user)
  attribute '…' to a number.`, captured from boto3 against live SQS, code and message
  together), and the empty-`String`-value message (captured twice independently).
- **Snapshot-tested reimplementation**: the name and type messages, from LocalStack's
  `check_attributes`, which snapshot-tests against real AWS. The character-class message
  is reproduced verbatim including its odd "upper and lower score characters" phrasing
  and its trailing space — a tidied string is no longer the one a consumer sees.
- **A single reimplementation, and the weakest claim here**: the `Number` **range**
  message (`Number attribute value … should be in range (-10**128..10**126)`), which only
  elasticmq supplies.

The count rejection's **code** is not in the capture; it comes from agreement across five
reimplementations. Neither moto nor LocalStack enforces the count at all, which is why
substrate accepting an eleventh attribute went unnoticed until message attributes became
observable.

### The rules apply on send, not on receive

A message written into state before the rules existed — replayed from an older event log
— is **returned as stored**, in full: same body, same attributes, nothing filtered,
nothing corrected. `ReceiveMessage` logs a warning at `WARN` naming the queue, the message
ID and the violated rule, and that is the whole of the receive-side behaviour.

Returning it is the decision, not an omission. Substrate's core property is that
replaying an event log reproduces the same observations, and the message was accepted by
the substrate that recorded it. Withholding or dropping it now would make a recorded run
unreplayable — the one property the emulator rests on — and a receive-time rejection has
no AWS behaviour to imitate: real SQS never accepted the message, so there is nothing to
copy. Rejecting on receive is deliberately not done, and the test suite fails if anyone
adds it.

The warning covers the **whole stored attribute set**, not the subset the request named.
The violation is a property of what is in state, and a request that names no attribute
names gets no attributes back, so checking only the selection would hide it from exactly
the caller most likely to be replaying an old log. It also fires on every receive of the
same message rather than once: a redelivery after the visibility timeout is the fixture
being exercised again, and remembering which messages had already warned would be
per-process state a replay could not reproduce.

Send-time rejection is unchanged, so no new run can produce this state.

### QueueNameExists

`CreateQueue` fails with `QueueNameExists`, HTTP 400, when the named queue already
exists **with attribute values differing from the ones requested**. Same name with
the same values stays idempotent and returns the existing URL, as AWS documents.

This needs no seed: the condition is entirely determined by state, so it fires on the
real mistake — two stacks or two test cases claiming one queue name with different
settings — rather than only when a harness remembers to arm it.

**Only attributes present in the request are compared.** An omitted attribute is
treated as "no opinion", not as an assertion of its default. That reading comes from
the error's own definition — "Amazon SQS returns this error only if the request
includes attributes whose values differ from those of the existing queue" — which
scopes the comparison to what the request includes. It is also what keeps
CloudFormation re-deploys working, since a template forwards only the properties it
declares.

An existing queue's unset attributes are resolved through their defaults before
comparing, so these are all idempotent:

| First create | Re-create | Result |
|---|---|---|
| no attributes | no attributes | idempotent |
| no attributes | `VisibilityTimeout=30` | idempotent — 30 is what the queue already reports |
| `VisibilityTimeout=30` | no attributes | idempotent |
| `VisibilityTimeout=45`, `DelaySeconds=5` | `VisibilityTimeout=45` | idempotent — subset |
| `VisibilityTimeout=45` | `VisibilityTimeout=90` | `QueueNameExists` |
| no attributes | `DelaySeconds=10` | `QueueNameExists` — effective value is 0 |
| `orders.fifo`, no attributes | `FifoQueue=true` | idempotent — derived from the `.fifo` suffix |
| `orders.fifo`, no attributes | `FifoQueue=false` | `QueueNameExists` |

Comparison is **exact string equality** on resolved values, so `5` and `05` differ,
and two semantically identical `Policy` documents differing in whitespace or key
order read as a conflict. Any SDK or template re-sending its own serialization
matches, which is the case that has to work; semantic JSON comparison is not
attempted.

The message carries AWS's documented wording plus the name of the offending
attribute, which AWS's own text omits — without it the error is nearly
undiagnosable for a caller holding a large attribute set. When several attributes
differ, the alphabetically first is named, so the message is reproducible across
runs.

### Seeding QueueDeletedRecently

AWS requires a **60-second wait after `DeleteQueue`** before a queue of the same name
can be created, raising `QueueDeletedRecently`, HTTP 400, in the meantime. Substrate
keeps no memory of a delete, so a consumer's delete → recreate → retry loop had no
reachable error branch.

```bash
# The next 2 CreateQueue calls naming run-q report QueueDeletedRecently.
curl -X POST http://localhost:4566/v1/sqs/consistency \
  -d '{"queueName":"run-q","deletedRecentlyMisses":2}'
```

It shares the `/v1/sqs/consistency` endpoint, the name-over-wildcard precedence, and
the `DELETE` clearing described above, and it is counted rather than timed for the
same reason: the real condition is a wall-clock window, and a wall-clock window would
make the assertion depend on how long the rest of the test took.

Unlike the two lookup counters, this one applies **only while the name is free**.
`QueueDeletedRecently` describes a name too recently freed, so an existing queue is
the one case it cannot describe — a `CreateQueue` that hits an existing queue is an
idempotent success and does not spend the budget. Substrate cannot know whether a
name was "recently deleted", which is why the condition is seeded rather than
inferred: a seeded name is refused on its next create whether or not a delete
preceded it.

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::SQS::Queue | QueueUrl | FifoQueue attribute supported |

### Cost

SQS requests: $0.0000004 per request.

---

## DynamoDB

**Endpoint:** `dynamodb.{region}.amazonaws.com`
**Protocol:** JSON (`application/x-amz-json-1.0`, `X-Amz-Target: DynamoDB_20120810.{Op}`)

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateTable | Supports GSI, LSI, billing mode |
| DescribeTable | |
| DeleteTable | |
| ListTables | |
| PutItem | Supports ConditionExpression |
| GetItem | Supports ProjectionExpression |
| UpdateItem | Supports UpdateExpression (SET/REMOVE/ADD/DELETE) |
| DeleteItem | Supports ConditionExpression |
| Query | Supports FilterExpression, GSI/LSI via IndexName |
| Scan | Supports FilterExpression, GSI/LSI via IndexName |
| BatchGetItem | |
| BatchWriteItem | |
| TransactGetItems | |
| TransactWriteItems | |
| UpdateTimeToLive | |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::DynamoDB::Table | TableName | GSI, LSI, TTL supported |

### Cost

DynamoDB write operations: $0.00000125 per WCU. Read operations: $0.00000025 per RCU.

---

## EC2

**Endpoint:** `ec2.{region}.amazonaws.com`
**Protocol:** AWS Query (form-encoded, `Action=` parameter)

### Supported operations

| Operation | Notes |
|-----------|-------|
| RunInstances | Auto-creates default VPC (172.31.0.0/16); [requires a resolvable AMI](#runinstances-requires-a-resolvable-ami); [merges a named launch template field by field](#a-launch-template-merges-with-the-request-field-by-field); [validates MinCount/MaxCount](#mincount-and-maxcount); reports [`groupSet`](#security-groups-on-an-instance) and [`placement`](#termination-protection-is-honoured-one-availability-zone-at-a-time) |
| DescribeInstances | [Explicit resource IDs](#explicit-resource-ids); reports [`groupSet`](#security-groups-on-an-instance) and [`placement`](#termination-protection-is-honoured-one-availability-zone-at-a-time); `availability-zone` filter |
| TerminateInstances | [Explicit resource IDs](#explicit-resource-ids); [honours termination protection, per Availability Zone](#termination-protection-is-honoured-one-availability-zone-at-a-time) |
| StopInstances | [Explicit resource IDs](#explicit-resource-ids) |
| StartInstances | [Explicit resource IDs](#explicit-resource-ids) |
| DescribeInstanceStatus | [Explicit resource IDs](#explicit-resource-ids) |
| DescribeInstanceAttribute | Four attributes, `<value>`-wrapped — see [Instance attributes](#instance-attributes) |
| ModifyInstanceAttribute | `InstanceType.Value`, `UserData.Value`, `DisableApiTermination.Value`; the first two [require a stopped instance](#instance-attributes) |
| CreateVpc | |
| DescribeVpcs | [Explicit resource IDs](#explicit-resource-ids) |
| DeleteVpc | [Explicit resource IDs](#explicit-resource-ids) |
| CreateSubnet | |
| DescribeSubnets | [Explicit resource IDs](#explicit-resource-ids) |
| DeleteSubnet | [Explicit resource IDs](#explicit-resource-ids) |
| CreateSecurityGroup | |
| DescribeSecurityGroups | [Explicit resource IDs](#explicit-resource-ids) |
| DeleteSecurityGroup | [Explicit resource IDs](#explicit-resource-ids) |
| AuthorizeSecurityGroupIngress | Supports source security groups (`IpPermissions.N.Groups.M.GroupId`), including self-referencing rules |
| AuthorizeSecurityGroupEgress | Supports destination security groups |
| RevokeSecurityGroupIngress | Matches on protocol, ports, **and** source |
| RevokeSecurityGroupEgress | |
| CreateInternetGateway | |
| AttachInternetGateway | |
| DescribeInternetGateways | [Explicit resource IDs](#explicit-resource-ids) |
| DeleteInternetGateway | [Explicit resource IDs](#explicit-resource-ids) |
| DescribeAvailabilityZones | Three zones per region, from the same list the offerings and spot-price operations use — see [Instance types are a seeded catalog](#instance-types-are-a-seeded-catalog) |
| DescribeRegions | |
| DescribeInstanceTypes | Answers from a [seeded catalog](#instance-types-are-a-seeded-catalog). `InstanceType.N` is an assertion: a type outside the catalog is refused with `InvalidInstanceType`. `Filter.N` is not applied |
| DescribeInstanceTypeOfferings | `instance-type` and `location` filters (both with [wildcards](#instance-types-are-a-seeded-catalog)) and the `LocationType` parameter; an unmatched filter is an empty answer, not an error |
| DescribeSpotPriceHistory | One stub price per catalog type per zone. `InstanceType.N` here is a *filter*, so an unknown type is an empty history — [see below](#instance-types-are-a-seeded-catalog) |
| CreateRouteTable | |
| AssociateRouteTable | |
| DescribeRouteTables | [Explicit resource IDs](#explicit-resource-ids) |
| DeleteRouteTable | [Explicit resource IDs](#explicit-resource-ids) |
| DescribeSnapshots | [Explicit resource IDs](#explicit-resource-ids) |
| DescribeAddresses | [Explicit resource IDs](#explicit-resource-ids) |
| DescribeNatGateways | [Explicit resource IDs](#explicit-resource-ids) |
| CreateLaunchTemplate | Creates version 1. Networking is read from every `NetworkInterface.N.*` — see [Launch template networking](#launch-template-networking) |
| DescribeLaunchTemplates | Summary only — no `launchTemplateData`, matching AWS. Use `DescribeLaunchTemplateVersions` to read a template's parameters |
| DeleteLaunchTemplate | |
| CreateLaunchTemplateVersion | `SourceVersion` inheritance — see [Launch template versions](#launch-template-versions) |
| ModifyLaunchTemplate | `SetDefaultVersion` only, which is AWS's only modifiable attribute |
| DescribeLaunchTemplateVersions | Numbers, `$Latest`, `$Default`, `MinVersion`/`MaxVersion`, `MaxResults`/`NextToken`, and the account-wide form |
| DeleteLaunchTemplateVersions | Reports per version at HTTP 200; the default version cannot be deleted |
| CreateFleet | Instances launch through the `RunInstances` path, so they are visible to `DescribeInstances`, and carry the reserved `aws:ec2:fleet-id` tag. Partial fulfillment is seedable — see below |
| DescribeFleets | An `instant` fleet is returned only when its ID is named explicitly, matching AWS |
| DeleteFleets | `TerminateInstances=true` (and any `instant` fleet) terminates the fleet's instances, [subject to termination protection](#termination-protection-is-honoured-one-availability-zone-at-a-time) |
| CreateTags | Rejects [reserved `aws:` keys](#reserved-tag-keys), [over-long keys and values](#tag-key-and-value-length-limits), and more than [50 tags per resource](#the-50-tag-per-resource-limit) |
| DeleteTags | Rejects [reserved `aws:` keys](#reserved-tag-keys) and [over-long keys](#tag-key-and-value-length-limits) |

### RunInstances requires a resolvable AMI

`RunInstances` must end up with an AMI from *some* source, or it fails with
`MissingParameter` / "The request must contain the parameter ImageId", HTTP 400.

AWS documents `ImageId` as **Required: No** only because a launch template may
supply it, so substrate checks *after* template resolution rather than on the way
in. Both of these are valid:

- `ImageId` given directly.
- `LaunchTemplate.LaunchTemplateId` (or `…Name`) naming a template whose data
  carries an `ImageId`.

The request fails when neither applies — including when the named template
resolves but carries no AMI of its own, and when the template name does not exist
at all.

Note that `ImageId` is an optional `*string` in the typed SDKs, so
`aws.String("")` serializes as **absent from the wire**: an empty AMI reaches the
service rather than being caught client-side. That is the shape this check exists
for. The AMI value itself is not format-validated — substrate accepts any
non-empty string, so fixtures like `ami-test` work.

### A launch template merges with the request, field by field

Naming a launch template does not replace the request, and the request does not
replace the template: the two are merged per field, with the request winning any
field it names. AWS's `RunInstances` reference states the rule directly — "Any
additional parameters that you specify for the new instance overwrite the
corresponding parameters included in the launch template."

| Field | Request | Template | Result |
|---|---|---|---|
| `ImageId` | `ami-request` | `ami-template` | `ami-request` |
| `ImageId` | absent | `ami-template` | `ami-template` |
| `InstanceType` | `m5.large` | `c5.xlarge` | `m5.large` |
| `InstanceType` | `t3.micro` | `m5.large` | `t3.micro` |
| `InstanceType` | absent | `m5.large` | `m5.large` |
| `InstanceType` | absent | absent | `t3.micro` (substrate's default) |
| `KeyName` | `k-request` | `k-template` | `k-request` |
| `TagSpecification` (instance-scoped) | `Env=req` | `Env=tmpl,Team=x` | `Env=req` alone — replace, not merge |
| `TagSpecification` (instance-scoped) | absent | `Env=tmpl` | `Env=tmpl` |
| `IamInstanceProfile` | `p-request` | `p-template` | `p-request` |
| `IamInstanceProfile` | absent | `p-template` | `p-template` |
| `SubnetId`, security groups, `AssociatePublicIpAddress` | see [Launch template networking](#launch-template-networking) | | |

Which *version* of the template supplies those values is resolved from
`LaunchTemplate.Version`; an absent version means the template's **default**
version, not its latest. See [Launch template versions](#launch-template-versions).

Two details are worth stating, because both were wrong before and each fails
silently rather than loudly.

Substrate used to consult the template **only when the request omitted `ImageId`**.
A request naming both an AMI and a template therefore ignored the template
entirely — its instance type, key name, user data, subnet, security groups and
public-IP preference were all dropped — and the launch still succeeded. The
instance simply was not the one that was asked for, which is the hardest kind of
infidelity to notice from a test that only checks that the call worked.

The `t3.micro` default is now applied **last**, after the template has had its
chance. It used to be applied first, and the template fallback then treated
`t3.micro` as a proxy for "the request named no instance type" — so a request
explicitly asking for `t3.micro` alongside a template naming something else got
the template's type, exactly inverting the documented precedence. An explicit
`t3.micro` is now honoured, and the default applies only when neither side names
a type.

A template's `TagSpecifications` and `IamInstanceProfile` used to be accepted and
stored nowhere, so a template that tagged its instances produced untagged ones and a
template naming a role produced an instance with none — with nothing failing to say
so. That is worse than a dropped `KeyName`, because a `tag:` filter is how IaC finds
the resources it just created: `DescribeInstances --filters tag:Env,Values=prod`
simply returned nothing, and a suite asserting on the tags it asked for had an
assertion that could not pass.

Both now participate in the merge, with two things worth stating:

- **Tags replace rather than merge.** A request naming `Env=req` against a template
  naming `Env=tmpl,Team=x` yields `Env=req` alone; `Team` is not inherited. The
  reference gives no `TagSpecifications`-specific merge semantics, only the general
  "overwrite the corresponding parameters" rule quoted above, and replacement is that
  rule applied to the whole specification.
- **Substrate's own `aws:ec2:fleet-id` stamp does not count as the request naming
  tags.** A fleet instance already carries that reserved key by the time the merge
  runs, so the fallback tests for a non-reserved key rather than for an empty set —
  otherwise a fleet launched from a tagging template would silently lose the
  template's tags. See [Reserved tag keys](#reserved-tag-keys).

**Only the instance scope is modelled.** A template may also scope tags to `volume`,
`network-interface` or `spot-instances-request`. Substrate models none of those
resources, so those specifications are recorded nowhere rather than misapplied to the
instance: they neither reach the launch nor read back from
`DescribeLaunchTemplateVersions`. Note that a template's instance-scoped tags land on
the *instance*, not on the template — the reference is explicit that "these tags are
not applied to the launch template."

A template's tags are subject to both tag rules, so a template is not a second
unrestricted tagging path: a `TagSpecifications` naming an `aws:`-prefixed key or
exceeding the 50-tag limit is rejected at `CreateLaunchTemplate` and at
`CreateLaunchTemplateVersion` (after any `SourceVersion` inheritance, so an inherited
violation is caught too). The launch checks again, because a template written
straight into state by a replayed event log can predate those checks.

The instance profile is stored as the single string the request supplied, matching
the shape an instance holds, so it is echoed back from
`DescribeLaunchTemplateVersions` in whichever member it arrived in — `arn` for an
`arn:`-prefixed value and `name` otherwise. `DescribeInstances` surfaces it as an ARN
either way, because AWS's instance response shape has no name member; for a
*template* read-back, synthesizing the other member would report the template as
naming something the caller never wrote.

### Launch template versions

Launch templates are versioned. `CreateLaunchTemplate` creates version 1, each
`CreateLaunchTemplateVersion` appends the next number, and `ModifyLaunchTemplate`
moves the default.

**An absent `LaunchTemplate.Version` means the default version, not the latest.**
aws-sdk-go-v2 documents this on `LaunchTemplateSpecification.Version` — "Default:
The default version of the launch template" — and it is the detail worth stating
loudest, because a new version does *not* become the default. A consumer that
creates version 2 and launches without naming a version still gets version 1.

| `LaunchTemplate.Version` | Resolves to |
|---|---|
| absent | the **default** version |
| `$Default` | the default version |
| `$Latest` | the highest version number |
| a number | that version, or `InvalidLaunchTemplateId.VersionNotFound` |

Both aliases are matched case-insensitively, so a hand-built `$latest` works. A
version that does not exist is an error rather than a silent fallback: a fallback
would launch instances from parameters the caller never asked for.

`CreateLaunchTemplateVersion`'s `SourceVersion` is the asymmetry to know:

- **With `SourceVersion`**, the new version inherits that version's parameters and
  the request's values overwrite the ones they name.
- **Without it**, the new version holds *only* what the request names. Nothing is
  inherited — not from version 1, not from the latest.

`DeleteLaunchTemplateVersions` reports **per version**, at HTTP 200:
`successfullyDeletedLaunchTemplateVersionSet` and
`unsuccessfullyDeletedLaunchTemplateVersionSet`. A request naming a deletable and an
undeletable version puts one entry in each set and still returns 200, so a caller
checking only the status code sees success. The default version cannot be deleted
("you must first assign a different version as the default"), and a deleted version
number is never reused.

The `responseError.code` on a failed item is `launchTemplateVersionDoesNotExist` for
a missing version. For the default-version rejection substrate emits
`unexpectedError`: `ResponseError.code` is a **closed six-value enum** in the AWS
SDK models (`launchTemplateIdDoesNotExist`, `launchTemplateIdMalformed`,
`launchTemplateNameDoesNotExist`, `launchTemplateNameMalformed`,
`launchTemplateVersionDoesNotExist`, `unexpectedError`) with no default-version
member, and a typed SDK deserializes anything outside it as an unknown variant.
AWS's real code for this case is not published and no capture of the rejection
exists — the code is the modeled catch-all and the message is the reference's own
sentence. Both are inferred, not captured.

Omitting both `LaunchTemplateId` and `LaunchTemplateName` from
`DescribeLaunchTemplateVersions` selects the **account-wide** form, which lists every
template's `$Latest` and/or `$Default`. As AWS does, it accepts only those two
aliases — a version number means nothing across templates — and rejects a request
naming neither.

**A template stored before versioning existed reads back as version 1, default.**
Its single stored parameter set *is* its version 1, synthesized on read, so a
replayed event log recorded against an earlier substrate still launches instances
and still describes correctly. No event rewriting is involved.

### Launch template networking

A launch template's subnet, security groups and public-IP preference are read from
its **primary network interface** — the one whose `DeviceIndex` is lowest:

```
LaunchTemplateData.NetworkInterface.N.SubnetId
LaunchTemplateData.NetworkInterface.N.SecurityGroupId.N
LaunchTemplateData.NetworkInterface.N.AssociatePublicIpAddress
```

That is not a stylistic choice. AWS's `RequestLaunchTemplateData` has **no
top-level `SubnetId` member** — a network interface is the only place a template
can name a subnet, and the only place `AssociatePublicIpAddress` exists at all. So
a template configured the way AWS requires is precisely the one whose networking
substrate used to discard.

Note the group parameter name: the AWS model calls that member `Groups` but gives
it the `locationName` `SecurityGroupId`, so real SDKs send `SecurityGroupId.N`.
Substrate accepts `Groups.N` as well, for hand-built requests.

Precedence when the same value is available from several sources, matching AWS:

| Source | Wins over |
|---|---|
| The request itself — `SubnetId`, or the primary `NetworkInterface.N.SubnetId` | everything below |
| A `CreateFleet` override's `SubnetId` | the template (it reaches `RunInstances` as a request-level value) |
| The launch template's network interface | the default VPC |
| The auto-created default VPC | — |

`AssociatePublicIpAddress` is three-valued, and only a non-default subnet without
`MapPublicIPOnLaunch` distinguishes them: **absent** uses the subnet's own
behavior, **`true`** forces a public IP anyway, and **`false`** suppresses one.

Every declared interface is parsed, on both `RunInstances` and launch templates — see
[Multiple network interfaces](#multiple-network-interfaces). The flat fields above
describe the **primary** interface, which is what a launch from a multi-interface
template resolves its subnet and groups from.

### Security groups on an instance

Both `RunInstances` and `DescribeInstances` report an instance's security groups as
`groupSet>item`, with `groupId` and `groupName` — the same shape as AWS's
`GroupIdentifier`. Groups appear in the order the launch resolved them, whichever
source supplied them:

- `SecurityGroupId.N` (or `SecurityGroupIds.N`) on the request, or the nested
  `NetworkInterface.N.SecurityGroupId.N` / `NetworkInterface.N.Groups.N` of the
  **primary** interface.
- The launch template's [network interface](#launch-template-networking).
- The auto-created default VPC's `default` group, when the launch names none.

`groupName` is **omitted when the group cannot be resolved** — for example after
the group is deleted while the instance it was launched with still exists. The
`groupId` is still reported, because that is what the launch actually recorded; a
name is not invented to fill the field.

The top-level `groupSet` reports the **primary** interface's groups, matching AWS.
A secondary interface's own groups appear on that interface inside
[`networkInterfaceSet`](#multiple-network-interfaces).

### Multiple network interfaces

`RunInstances` and `CreateLaunchTemplate` parse **every** declared
`NetworkInterface.N.*`, contiguously from 1 and stopping at the first missing
index — the same convention every other indexed list follows. Both `RunInstances`
and `DescribeInstances` report them as `networkInterfaceSet>item`, which is what
the `RunInstances` reference's own sample response shows:

```xml
<networkInterfaceSet>
  <item>
    <networkInterfaceId>eni-1a2b3c4d</networkInterfaceId>
    <subnetId>subnet-0123456789abcdef0</subnetId>
    <status>in-use</status>
    <privateIpAddress>172.31.1.10</privateIpAddress>
    <groupSet><item><groupId>sg-…</groupId><groupName>default</groupName></item></groupSet>
    <attachment>
      <deviceIndex>0</deviceIndex>
      <status>attached</status>
      <deleteOnTermination>true</deleteOnTermination>
    </attachment>
  </item>
</networkInterfaceSet>
```

**Identity is `DeviceIndex`, not the parameter index.** AWS documents
`DeviceIndex` as "the position of the network interface in the attachment order",
and it is not required to agree with the position the request happens to write it
at, so `NetworkInterface.1.DeviceIndex=3` and `NetworkInterface.2.DeviceIndex=0`
makes the *second* one primary. Interfaces are reported in `DeviceIndex` order.

The instance's flat `subnetId`, `privateIpAddress` and `groupSet` describe the
**primary** interface, as real EC2 does — they are not superseded by the set.
`AssociatePublicIpAddress` is honored **only on the primary**, which is what the
reference requires: it "can only be assigned to a network interface for eth0".

`DeleteOnTermination` defaults to `true` for an interface the launch creates and
`false` for an existing one it attaches by `NetworkInterfaceId` — deleting an
interface the caller brought would destroy something the launch did not make. An
explicit value wins over either default.

Substrate's own choices, where the API model does not decide:

- A **secondary** interface that names no `PrivateIpAddress` is given one derived
  from its instance index and device index, so every interface of every instance
  in a multi-`Count` launch has a distinct address a test can assert on. The
  primary's address is the instance's own.
- There are **no standalone ENI resources** — `CreateNetworkInterface` is not
  modeled, so an interface exists only as part of the instance that declared it,
  and an `eni-` ID is minted for one the request did not name. A launch declaring
  no interface reports an empty `networkInterfaceSet` rather than a synthesized
  phantom interface.
- Interfaces report `status: in-use` and attachment `status: attached`
  immediately, because substrate's instances are already `running` by the time
  `RunInstances` answers; an `attaching` attachment would contradict the instance
  state reported beside it.
- `InterfaceType` defaults to `interface`; `efa` and `efa-only` are recorded as
  given. `NetworkCardIndex` is recorded as given and defaults to 0.

### Instance attributes

`DescribeInstanceAttribute` reads one attribute off an instance. It is the only way
to read an instance's **user data** back: `RunInstances` recorded `UserData` and
nothing could observe it, so a consumer could not assert that the user data their IaC
intended reached the instance — including the value a
[launch template supplied](#a-launch-template-merges-with-the-request-field-by-field).

Four attributes are readable, being the ones that correspond to state substrate holds:

| `Attribute` | Reports |
|---|---|
| `userData` | The value as stored, still base64-encoded |
| `instanceType` | |
| `disableApiTermination` | `true` or `false`; recorded at launch and by `ModifyInstanceAttribute` |
| `groupSet` | `groupSet>item` with `groupId`/`groupName`, the same shape [`DescribeInstances` reports](#security-groups-on-an-instance) |

Scalar values are **wrapped in a `<value>` element**, which all three of the
reference's worked examples show and which matches the `AttributeValue` type the
response elements carry:

```xml
<DescribeInstanceAttributeResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
  <instanceId>i-0123456789abcdef0</instanceId>
  <instanceType><value>t3.micro</value></instanceType>
</DescribeInstanceAttributeResponse>
```

Exactly one attribute appears per response — the one asked for. `groupSet` is the
exception to the wrapper: it is an array of `GroupIdentifier`, not an `AttributeValue`.

`Attribute` is **Required: Yes**, so an absent one fails with `MissingParameter`
(`The request must contain the parameter Attribute`). An unknown instance ID fails
with `InvalidInstanceID.NotFound`, and a malformed one with `InvalidInstanceID.Malformed`,
as [everywhere else](#explicit-resource-ids).

#### Unmodelled attributes are refused, not defaulted

Every other name in the documented valid-values list — `kernel`, `ramdisk`,
`sourceDestCheck`, `blockDeviceMapping`, `productCodes`, `ebsOptimized`,
`rootDeviceName`, `sriovNetSupport`, `enaSupport`, `enclaveOptions`,
`instanceInitiatedShutdownBehavior`, `disableApiStop` — is rejected:

```
InvalidParameterValue: Value (enaSupport) for parameter attribute is invalid. Unknown attribute.
```

The status is `400`, and the offending value is interpolated. Refusing is deliberate
rather than a gap: answering `sourceDestCheck` with a default `false` would be
indistinguishable from a real instance that has it disabled, and a consumer asserting
on it would get a green test built on a value substrate invented.

This message has the strongest provenance of any in the EC2 plugin. It is **captured
from real AWS** in [aws/aws-cli#4273](https://github.com/aws/aws-cli/issues/4273),
where `aws ec2 describe-instance-attribute --attribute enaSupport` returns exactly it,
and it is byte-identical to [moto](https://github.com/getmoto/moto)'s string — a
capture and an independent reimplementation agreeing. The reference could not have
supplied it: `DescribeInstanceAttribute`'s Errors section is empty.

`enaSupport` is the case that makes the boundary concrete. It is *in* AWS's own valid-values
list, and the same reference says "Note that the `enaSupport` attribute is not supported."
Real AWS rejects a value its documentation lists, and #4273 is the capture of that
rejection — so substrate rejecting it is fidelity rather than a shortfall.

Attribute names are matched **case-sensitively**, as AWS's valid values are: `InstanceType`
is rejected, `instanceType` is not.

#### An attribute that was never set

An unset attribute is reported as a **present but empty element** — `<userData></userData>`
— rather than an omitted one. An empty `groupSet` likewise appears, empty.

This is the one shape here the reference cannot settle: all three of its worked examples
show an attribute that *has* a value. It ships from moto's `test_describe_instance_attribute`,
which asserts `response["UserData"] == {}` — an empty mapping, which is what an SDK
produces from a present element with no children. That is **weaker provenance than a
capture**, and worth stating, because the two shapes are not interchangeable to a caller:
an SDK maps a present-but-empty element to an empty struct and an omitted one to nil, so
`resp.UserData.Value` panics under one and not the other.

#### Modifying an attribute requires a stopped instance

`ModifyInstanceAttribute` writes `InstanceType.Value`, `UserData.Value` and
`DisableApiTermination.Value`. The first two require the instance to be `stopped`:

```
IncorrectInstanceState: The instance 'userData' attribute cannot be modified while the
instance is in the 'running' state; stop the instance first
```

The status is `400`. The **code** is documented — EC2's client-error table lists
`IncorrectInstanceState` as "some instance attributes, such as user data, can only be
modified if the instance is in a 'stopped' state" — while the **message text is
substrate's own**, since the table describes the condition rather than quoting the
string AWS sends, and no capture of this rejection was found.

This is a behaviour change for `instanceType`, which substrate previously changed on a
running instance. `ModifyInstanceAttribute`'s Example 1 states the requirement plainly:
"The instance must be in the `stopped` state." A test that asserted the old behaviour
will now see a `400`.

`disableApiTermination` is deliberately **exempt** from the gate, because `RunInstances`'
reference says so: "You can enable termination protection when you launch an instance,
while the instance is running, or while the instance is stopped." Gating it would refuse
a call real EC2 accepts — the same class of defect as accepting one real EC2 refuses,
just harder to notice, since it looks like extra rigor.

`UserData.Value` is read with a presence check rather than a non-empty one, so clearing
an instance's user data is expressible: `UserData.Value=` on a stopped instance empties
it, and the attribute then reads back as the empty element above.

#### Termination protection is honoured, one Availability Zone at a time

`TerminateInstances` refuses a protected instance with `OperationNotPermitted`, HTTP
`400`, and the instance stays `running`. The **code** is documented — EC2's client-error
table lists `OperationNotPermitted` as "The specified operation is not allowed" and names
this case first among its examples, "you might be trying to terminate an instance that has
termination protection enabled" — while the **message text is substrate's own**, since no
capture of the string AWS sends was found. It interpolates the instance ID and names the
attribute to clear, which is what a caller acts on:

```
OperationNotPermitted: The instance 'i-0123456789abcdef0' may not be terminated. Modify
its 'disableApiTermination' instance attribute and try again.
```

A request naming both protected and unprotected instances is where this gets
counter-intuitive, and it is worth reading the reference's own words, because the answer is
neither "the whole request is refused" nor "the unprotected instances are terminated":

> If you terminate multiple instances across multiple Availability Zones, and one or more
> of the specified instances are enabled for termination protection, the request fails with
> the following results:
>
> - The specified instances that are in the same Availability Zone as the protected
>   instance are not terminated.
> - The specified instances that are in different Availability Zones, where no other
>   specified instances are protected, are successfully terminated.

Partial failure is scoped to the **Availability Zone**. So for the reference's own worked
example — A and B unprotected in `us-east-1a`, C protected and D unprotected in
`us-east-1b`, all four named in one request:

| Instance | Zone | Protected | Outcome |
|---|---|---|---|
| A | `us-east-1a` | no | **terminated** |
| B | `us-east-1a` | no | **terminated** |
| C | `us-east-1b` | yes | still `running` |
| D | `us-east-1b` | no | still `running` — it shares C's zone |

The request itself reports `OperationNotPermitted`, naming C, **after** the terminations in
`us-east-1a` have been persisted. An unprotected instance sharing a zone with a protected
one survives; an unprotected instance in another zone does not.

Because the grouping key is the zone, every instance carries one. It is resolved at launch
from `Placement.AvailabilityZone`, or from the subnet's zone when the launch named only a
`SubnetId`, or from the region's first zone (`<region>a`) when it named neither — matching
the reference's "EC2 automatically selects an Availability Zone for you". `AvailabilityZoneId`
is not modelled. The zone is reported by `RunInstances` and `DescribeInstances` as
`<placement><availabilityZone>`, and `DescribeInstances` accepts an `availability-zone`
filter, so a caller can work out in advance which of their instances a terminate would
spare. (The filter is named `availability-zone`, not `placement.availability-zone` — the
placement family's filter names are spelled out individually in the reference's list.)

An instance unmarshalled from an **event log recorded before this field existed** reads back
with an empty zone, which groups all such instances together. That is the conservative
reading, being what a single-zone account looks like.

A bad instance ID still fails the whole request before anything is written, per "If you
specify multiple instances and the request fails (for example, because of a single incorrect
instance ID), none of the instances are terminated." The protection scan runs as a second
pre-flight pass, after every named instance resolves, so no state is written for a zone that
is about to be refused. Terminating an already-terminated instance still succeeds; the
operation is idempotent and the protection check does not change that.

`DeleteFleets --terminate-instances` goes through the same handler, so the rule applies
there too: a protected fleet instance **survives** its fleet's deletion, an unprotected
sibling in another zone does not, and `DeleteFleets` propagates the `OperationNotPermitted`
rather than folding it into `unsuccessfulFleetDeletionSet`. `DeleteFleetError`'s documented
codes are exactly `fleetIdDoesNotExist`, `fleetIdMalformed`, `fleetNotInDeletableState` and
`unexpectedError`, none of which covers termination protection, so folding it in would mean
answering `unexpectedError` and losing the code the caller acts on.

One divergence remains, unrelated to protection and pre-existing: substrate reports a
terminated instance as code `48` `terminated` immediately, where real EC2 reports code `32`
`shutting-down` first and settles to `48`. A consumer polling for `shutting-down` will never
observe it. That is tracked separately.

### MinCount and MaxCount

A count that is **present but invalid** fails with `InvalidParameterValue`,
HTTP 400. A count that is **absent** still defaults.

| Request | Result |
|---|---|
| Neither given | 1 instance |
| `MinCount=2` alone | 2 instances — an absent `MaxCount` defaults to `MinCount`, not to 1 |
| `MaxCount=4` alone | 4 instances |
| `MinCount=1&MaxCount=3` | 3 instances |
| `MinCount=0`, or either count `< 1` | `Invalid value '0' for parameter minCount. It must be at least 1.` |
| Either count unparseable | `Invalid value 'abc' for parameter minCount. It must be an integer.` |
| `MinCount=3&MaxCount=1` | `Invalid value '1' for parameter maxCount. The maxCount must be equal to or greater than the minCount '3'.` |

A successful launch always creates `MaxCount` instances. AWS "launches the largest
possible number of instances above the specified minimum count", and substrate
models no capacity ceiling, so the largest possible number is always the maximum
asked for and `MinCount` can only ever be satisfied.

Absence defaults rather than erroring even though AWS marks both **Required: Yes**,
because in every typed SDK they are required members that fail client-side — so a
consumer bug there cannot reach the wire, while requiring presence here would break
hand-built form-encoded requests. A value that is present and invalid *is*
reachable: the query protocol carries these as strings, and neither botocore's
`ParamValidator` nor `aws-sdk-go-v2` range-checks them.

The error code is the common-error `InvalidParameterValue` because the RunInstances
reference documents no action-specific error for these. `MinCount > MaxCount` uses
it too, rather than `InvalidParameterCombination` — that code is defined as
"Parameters that must not be used together were used together", which cannot
describe two parameters AWS documents as used together; the defect is the *value*.

The upper bound is a per-account, per-instance-type quota substrate does not model,
so it is not enforced: any count at or above 1 is accepted.

### Instance types are a seeded catalog

`DescribeInstanceTypes`, `DescribeInstanceTypeOfferings` and
`DescribeSpotPriceHistory` all answer from one seeded catalog. It is **not
exhaustive** — EC2 offers some 800 types — but it is **complete per family**:

| Family | Sizes |
|---|---|
| `t3`, `t3a` | `nano`, `micro`, `small`, `medium`, `large`, `xlarge`, `2xlarge` |
| `m5`, `m5a`, `r5`, `c5a` | `large`, `xlarge`, `2xlarge`, `4xlarge`, `8xlarge`, `12xlarge`, `16xlarge`, `24xlarge` |
| `c5` | `large`, `xlarge`, `2xlarge`, `4xlarge`, `9xlarge`, `12xlarge`, `18xlarge`, `24xlarge` — note the ladder is **not** the same as `c5a`'s |
| accelerated | `p3.2xlarge`, `g4dn.xlarge`, `inf1.xlarge` |

Whole families rather than a sample, because an absent type is *refused* (below) —
a catalog stopping at `c5.xlarge` would answer `InvalidInstanceType` for
`c5.large`, which is the right code for a bogus type and the wrong one for a real
one. Bare-metal sizes (`m5.metal` and friends) are deliberately excluded: they are
real types, but nothing else in the plugin models their behaviour, so returning
them would advertise fidelity that is not there. vCPU and memory figures come from
the AWS instance-type guides. `inf1`'s Inferentia accelerator is not reported
through `gpuInfo`, matching real EC2, so its GPU count is zero.

#### A type outside the catalog: refused, or empty?

Both — and which one depends on whether the parameter is an assertion or a filter.
This asymmetry is deliberate and matches real AWS; #485 diffed all three
operations against `us-east-1`.

| Request | Answer |
|---|---|
| `DescribeInstanceTypes --instance-types zz9.bogus` | `InvalidInstanceType`, HTTP 400 — `InstanceType.N` asserts the types exist |
| `DescribeInstanceTypeOfferings --filters Name=instance-type,Values=zz9.bogus` | **0 offerings, HTTP 200** — a filter that matches nothing is a legitimate empty answer |
| `DescribeSpotPriceHistory --instance-types zz9.bogus` | **Empty history, HTTP 200** — the reference describes this parameter as filtering the results |

Every unknown type in one `DescribeInstanceTypes` request is collected into a
single error, in request order:

```
InvalidInstanceType: The following supplied instance types do not exist: [zz9.bogus, aa1.nope]
```

One bad type fails the whole request; the known types are not returned. The
message is verbatim from a real `us-east-1` capture for the single-type case; the
`", "` separator for a list is substrate's choice, so dispatch on the code.

`DescribeInstanceTypes` ignores `Filter.N` entirely. The operation documents some
60 filter names, nearly all over response fields the seeded catalog does not carry,
and applying the handful that are answerable while silently dropping the rest is
the same defect as an ignored filter. Tracked in
[#495](https://github.com/scttfrdmn/substrate/issues/495).

#### Offerings filters and wildcards

`DescribeInstanceTypeOfferings` accepts exactly the two filter names its reference
documents — `instance-type` and `location`. **Any other name is refused** with
`InvalidParameterValue` rather than ignored. Multiple `Filter.N.Value.M` values are
an OR; separate `Filter.N` entries AND together.

Filter values honour EC2's documented wildcards, and are **case-sensitive**:

| Value | Matches |
|---|---|
| `c5.2xlarge` | that type |
| `c5.*` | the eight `c5` sizes |
| `c5*` | `c5` **and** `c5a` — `*` matches zero or more characters, including the `a` |
| `t3?.micro` | `t3.micro` and `t3a.micro` — `?` matches zero **or one** character |
| `m5.larg\*` | nothing; a backslash escapes a literal wildcard |
| `M5.XLarge` | nothing |

`LocationType` is a top-level **parameter**, not a filter name (`location-type` is
refused as a filter). `availability-zone` is the default and `region` returns one
offering per type located at the region. `availability-zone-id` and `outpost` are
valid AWS values that substrate does **not** model, and are refused with a message
naming substrate — treating them as `availability-zone` would return zone *names*
under a `locationType` claiming they are IDs or Outpost ARNs, which a caller
matching the two would silently mis-read.

The three zones `DescribeAvailabilityZones` reports are the same three the
offerings and spot-price operations use, so filtering an offerings query by a zone
you just enumerated always returns an answer.

#### Spot prices are stubs

The `spotPrice` values are **deterministic stubs, not AWS prices**: substrate has
no price feed, and the numbers exist so a spot-price response has a plausible,
stable figure in it. Within a family they are a fixed rate per GiB, so they stay
monotonic in size. Assert on the *shape* of a spot-price response, never on the
amount. Every catalog type has a price — the two are generated together, so a type
cannot appear in `DescribeInstanceTypes` and be missing from
`DescribeSpotPriceHistory`.

### Explicit resource IDs

Naming a resource ID explicitly is an assertion that the ID exists, and EC2
answers it with an error rather than an empty result. `DescribeVpcs()` with no
arguments legitimately returns `[]`; `DescribeVpcs(VpcIds=["vpc-…"])` where that
VPC is absent fails.

- An ID that resolves to nothing → `Invalid<Type>.NotFound`, HTTP 400.
- A syntactically invalid ID → `Invalid<Type>.Malformed`, HTTP 400. Syntax is
  checked before existence, so a request naming both a malformed and an absent ID
  reports `Malformed`.
- One present plus one absent ID fails the whole call — EC2 does not return the
  partial set.
- An ID excluded by a `Filter` rather than by absence still counts as resolved:
  an existing ID plus a non-matching filter returns 200 and an empty set.
- No explicit IDs → every resource matches, and an empty account returns 200 and
  an empty set.

AWS's casing is inconsistent across these codes and SDK callers match the literal
string, so substrate mirrors each pair exactly:

| Resource | NotFound | Malformed |
|----------|----------|-----------|
| Instance | `InvalidInstanceID.NotFound` | `InvalidInstanceID.Malformed` |
| VPC | `InvalidVpcID.NotFound` | `InvalidVpcID.Malformed` |
| Subnet | `InvalidSubnetID.NotFound` | `InvalidSubnetID.Malformed` |
| Security group | `InvalidGroup.NotFound` | `InvalidGroupId.Malformed` |
| Internet gateway | `InvalidInternetGatewayID.NotFound` | `InvalidInternetGatewayId.Malformed` |
| Route table | `InvalidRouteTableID.NotFound` | `InvalidRouteTableId.Malformed` |
| Snapshot | `InvalidSnapshot.NotFound` | `InvalidSnapshotID.Malformed` |
| Elastic IP allocation | `InvalidAllocationID.NotFound` | — |
| NAT gateway | `InvalidNatGatewayID.NotFound` | — |

EC2 publishes no `Malformed` variant for allocation IDs or NAT gateway IDs; a
malformed ID for those surfaces as the `NotFound` code.

An ID is well formed when it has the resource's prefix followed by at least one
lowercase hex digit. Length is deliberately not checked: substrate's generators
emit 16 hex characters where AWS emits 8 or 17, and AWS itself still accepts the
legacy 8-character form for several resources.

### Finding a fleet's instances

Every instance `CreateFleet` launches is tagged `aws:ec2:fleet-id` with the fleet
that created it, so the fleet's instances are reachable with an ordinary
`DescribeInstances` tag filter:

```bash
aws ec2 describe-instances \
  --filters "Name=tag:aws:ec2:fleet-id,Values=fleet-12a34b56-7890-1cde-2f34-abcdef567890"
```

For an `instant` fleet this is the only route from a fleet back to its live
instances. `DescribeFleetInstances` rejects instant fleets outright, and the
`fleetInstanceSet` in a `CreateFleet`/`DescribeFleets` response is a record of what
was launched — it never drops instances that have since terminated. Without the tag
a fully-running fleet is indistinguishable from an empty one.

This tag is modelled from observed behaviour on real AWS rather than from a
documented API contract: it appears in neither the EC2 API reference nor the fleet
tagging and describe pages. It is applied to every fleet type, and — unlike a
caller's own `TagSpecification` entries — it is not scoped by `ResourceType`.

A caller cannot delete this tag — see
[reserved tag keys](#reserved-tag-keys) — which matches the rule AWS attaches to the
`aws:` prefix.

### Reserved tag keys

Every path that assigns a tag rejects any key beginning with `aws:`, the prefix EC2
reserves for its own use — `CreateTags`, `DeleteTags`, and tag-on-create through
`RunInstances`, `CreateFleet`, `CreateImage` and `CreateNatGateway`:

```
InvalidParameterValue: Tag keys starting with 'aws:' are reserved for internal use
```

The status is `400`. The whole request is refused before any resource is modified,
so a request that mixes a legal tag with a reserved one leaves every resource it
named untouched — `CreateTags` accepts up to 1000 resource IDs, and a partial
application is a state real EC2 never produces.

The match is **case-sensitive**. AWS documents tag keys and values as
case-sensitive, so `AWS:foo` and `Aws:foo` are ordinary user tags and are accepted;
only the lowercase `aws:` prefix is reserved.

On a tag-on-create path the rejection happens **before the resource is created**, so
a refused `RunInstances` launches no instance, a refused `CreateImage` leaves behind
neither the AMI nor its backing snapshot, and a refused `CreateNatGateway` creates no
gateway. This follows the tagging documentation directly: "If tags cannot be applied
during resource creation, we roll back the resource creation process. This ensures
that resources are either created with tags or not created at all."

Provenance: the `CreateTags` API reference has an empty Errors section, so neither
the code nor the message above is derivable from the API model. Both come from
observed real-AWS responses — and both captures are in fact of `RunInstances`
tag-on-create, so that path has the strongest claim to this wording. The
`DeleteTags` rejection is a step weaker — substrate found no captured `DeleteTags`
error and inherits the wording from the same capture. What the tagging documentation
does state plainly is the outcome: such a tag "can't be edited or deleted" by a
caller.

#### How substrate's own fleet tag is exempt

Substrate stamps [`aws:ec2:fleet-id`](#finding-a-fleets-instances) on every fleet
instance, which is a reserved key on a tag-on-create path — the reason this check
was previously left off that path entirely.

It is exempt structurally rather than by a flag. `CreateFleet` parses the caller's
`TagSpecification.N` tags and checks them exactly as `RunInstances` does; the
fleet-ID tag is appended to the resulting value *after* that check, on an internal
launch entry point that takes already-parsed tags. There is no param a request could
set to reach it. A validation-skipping flag would have made the outcome depend on
internal state a consumer cannot observe, which is the opposite of the
deterministic-replay property substrate exists for.

So a caller naming `aws:` anything in a `CreateFleet` request is rejected —
instance- and fleet-scoped alike — while the fleet's own stamp is still applied, and
both coexist with the caller's legal tags on the same instance.

One limit of the current scope, stated rather than implied: only tags scoped to a
resource substrate models are checked. A `TagSpecification` naming `volume` or
`network-interface` on `RunInstances`, or inside a launch template's
`LaunchTemplateData`, is skipped, because substrate does not tag those resources at
all; real EC2 would reject a reserved key there too.

A launch template's own instance-scoped tags *are* checked, at
`CreateLaunchTemplate` and `CreateLaunchTemplateVersion` as well as at every launch
that names the template — so a template cannot serve as an unchecked second path to a
reserved key. See
[A launch template merges with the request, field by field](#a-launch-template-merges-with-the-request-field-by-field).

### The 50-tag-per-resource limit

A resource carrying more than 50 user tags is refused, on `CreateTags` and on every
tag-on-create path:

```
TagLimitExceeded: The maximum number of Tags for a resource has been reached.
```

The status is `400`. From the tagging documentation's restrictions: "Maximum number of
tags per resource – 50".

Two rules make this less arithmetic than it looks, and both are modelled.

**Tags with the `aws:` prefix do not count.** The documentation says so directly:
"Tags with the `aws:` prefix do not count against your tags per resource limit." This
is load-bearing rather than pedantry, because substrate stamps
[`aws:ec2:fleet-id`](#finding-a-fleets-instances) on every fleet instance — a counter
that included reserved keys would refuse a fleet launch whose template names the full
50 user tags, which real EC2 accepts. A fleet instance therefore holds 51 tags legally:
50 of the caller's and one of substrate's.

**Overwriting an existing key at the limit succeeds.** The count is over the
*post-merge key set*, so a key already on the resource adds nothing. `CreateTags` on a
50-tag instance changing the value of `key7` is accepted and the value changes; adding
a new `key51` to the same instance is refused. Written as
`len(existing) + len(incoming)` both would fail, and real AWS permits the first —
[getmoto/moto#8151](https://github.com/getmoto/moto/issues/8151) reports exactly that
case.

As with reserved keys, the whole request is refused before anything is modified.
`CreateTags` naming two instances — one with room, one at the limit — tags neither.
A resource ID that names nothing is not counted against, because the apply step ignores
it: checking it would refuse a request real EC2 accepts as a no-op.

Provenance is split, and the weaker half is the message. The **code** is documented:
EC2's client-error table lists `TagLimitExceeded` as "You've reached the limit on the
number of tags that you can assign to the specified resource." The wire **message** is
published nowhere, so the wording above is [moto](https://github.com/getmoto/moto)'s,
from a reimplementation rather than a captured response. That is a weaker claim than the
code's, and is a distinction worth stating: SDKs dispatch on `Error.Code`, so the code
is the part a consumer's error branch turns on.

A launch template's instance-scoped tags are counted the same way, at
`CreateLaunchTemplate` and `CreateLaunchTemplateVersion` as well as at every launch
that names the template. Exactly 50 template tags launch; 51 are refused at template
creation.

The third restriction from the same table — the key and value **length** limits — is
enforced too, with one deliberate difference in how reserved keys are treated. See
[Tag key and value length limits](#tag-key-and-value-length-limits).

### Tag key and value length limits

A tag key longer than **128 characters** or a value longer than **256** is refused, on
`CreateTags`, `DeleteTags` and every tag-on-create path:

```
InvalidParameterValue: Tag key must be no more than 128 Unicode characters in UTF-8; the supplied key is 129
```

The status is `400`. The message names which of the two limits was exceeded and by how
much, because that is the only place a caller learns it. From the same restrictions list
that gives the 50-tag count: "Maximum key length – 128 Unicode characters in UTF-8" and
"Maximum value length – 256 Unicode characters in UTF-8".

**The unit is Unicode characters, not bytes.** A key of 128 emoji is 128 characters and
512 bytes, and it is legal; a byte-counting check would refuse it, and refuse it while
reporting a length the caller never sent. The two counts agree on ASCII, so a suite that
only tests ASCII keys cannot tell them apart.

There is **no lower bound**. The documentation states that "You can set the value of a
tag to an empty string, but you can't set the value of a tag to null", so an empty value
is legal and the check is an upper bound only. That is also what makes `DeleteTags` work
unremarkably: it names keys and treats the value as optional, so a request with no
`Tag.N.Value` supplies the empty string and passes. A key is required by the query
encoding rather than by this check — the tag walk ends on an absent or empty
`Tag.N.Key` — so an empty key is not expressible in the first place.

As with the other two tag restrictions, the whole request is refused before anything is
modified, and on a tag-on-create path before the resource is created: a refused
`RunInstances` launches no instance, a refused `CreateImage` creates neither the AMI nor
its snapshot, a refused `CreateNatGateway` creates no gateway, and a refused
`CreateLaunchTemplate` creates no template. A launch template is checked at
`CreateLaunchTemplate` and `CreateLaunchTemplateVersion` as well as at every launch that
names it, so a consumer hears about an over-long template tag once, at the operation that
named it.

**Reserved keys are not exempt from the lengths, though they are from the count.** The
exemption in the restrictions list is scoped to the count alone — "Tags with the `aws:`
prefix do not count against your tags per resource limit" — and nothing in it exempts a
reserved key from either length, so substrate checks them. In practice that decides no
observation: the reserved-key check runs first, so a caller's `aws:`-prefixed key is
refused for being reserved before its length is measured. It matters for the code rather
than the wire, and it is recorded here because the adjacent count check does the opposite.
Where the length check does bite is the case-sensitive edge: `AWS:` is an ordinary user
tag, so an over-long `AWS:`-prefixed key is refused for its length.

Provenance is the weakest of the three tag restrictions, and is marked as such
deliberately. The **code** `InvalidParameterValue` with `400` is by analogy with the
reserved-key rejection — the other tag-restriction violation on the same operations, and
`CreateTags`' Errors section is empty so the API model supplies nothing. The **message
text is substrate's own**: no captured real-AWS length rejection was found, in
[moto](https://github.com/getmoto/moto) or LocalStack either. It follows the
`SendMessage` size-limit message's precedent of interpolating the limit and the actual
value. A consumer's error branch should dispatch on the code, which is the part that
rests on something.

#### Tag scoping on `CreateImage`

`CreateImage` accepts two tag scopes, and substrate now honours the distinction:
`ResourceType=image` tags the AMI, and `ResourceType=snapshot` tags the backing
snapshot substrate materializes for the AMI's root device. Per the reference, "the
same tag is applied to all of the snapshots that are created."

This is a behaviour change. `CreateImage` previously read `TagSpecification.1`'s tags
whatever they were scoped to, so a request that tagged only its snapshots put those
tags on the AMI instead. A caller asserting on `DescribeImages` tags that were
actually snapshot-scoped will now see them on `DescribeSnapshots`, which is where
real EC2 puts them.

### Seeding EC2 Fleet partial fulfillment

`CreateFleet` fulfills its whole `TotalTargetCapacity` by default. Partial
fulfillment — the case callers most often get wrong, since a fleet that asks for
12 and receives 8 still returns a fleet ID and echoes the *request* in
`TotalTargetCapacity` — is reachable by seeding a shortfall:

```bash
# Fulfill 8 instances and report the remainder as a capacity failure.
curl -X POST http://localhost:4566/v1/ec2/fleet-shortfall \
  -d '{"launchTemplate":"lt-0abc123","fulfill":8,
       "errorCode":"InsufficientInstanceCapacity","lifecycle":"spot"}'

# Clear one seed, or all of them.
curl -X DELETE 'http://localhost:4566/v1/ec2/fleet-shortfall?launchTemplate=lt-0abc123'
curl -X DELETE http://localhost:4566/v1/ec2/fleet-shortfall
```

`launchTemplate` matches a launch template ID or name, or `*` (the default) for
any. The shortfall is spread across the request's capacity pools, so `errorSet`
reports one item per pool that came up short, and `DescribeFleets` reports the
result in `fulfilledCapacity`.

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::EC2::VPC | VpcId | |
| AWS::EC2::Subnet | SubnetId | |
| AWS::EC2::SecurityGroup | GroupId | Inline `SecurityGroupIngress`/`SecurityGroupEgress` rules are authorized |
| AWS::EC2::SecurityGroupIngress | GroupId | Standalone rule; resolves `SourceSecurityGroupId` through `Ref`/`GetAtt`, so self- and mutually-referencing groups work |
| AWS::EC2::SecurityGroupEgress | GroupId | Standalone rule; supports `DestinationSecurityGroupId` |
| AWS::EC2::Instance | InstanceId | Passes through `IamInstanceProfile`, `KeyName`, and `SecurityGroupIds` |
| AWS::EC2::InternetGateway | InternetGatewayId | |
| AWS::EC2::LaunchTemplate | LaunchTemplateId | Ref is the real `lt-…` ID, usable by `CreateFleet` |

### Cost

EC2 instance costs approximate on-demand pricing for the instance type.

---

## ELB v2

**Endpoint:** `elasticloadbalancing.{region}.amazonaws.com`
**Protocol:** AWS Query (form-encoded, `Action=` parameter)

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateLoadBalancer | ALB and NLB supported |
| DescribeLoadBalancers | |
| DeleteLoadBalancer | |
| CreateTargetGroup | |
| DescribeTargetGroups | |
| DeleteTargetGroup | |
| RegisterTargets | |
| DeregisterTargets | |
| DescribeTargetHealth | |
| CreateListener | |
| DescribeListeners | |
| DeleteListener | |
| CreateRule | |
| DescribeRules | |
| DeleteRule | |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::ElasticLoadBalancingV2::LoadBalancer | LoadBalancerArn | |
| AWS::ElasticLoadBalancingV2::TargetGroup | TargetGroupArn | |
| AWS::ElasticLoadBalancingV2::Listener | ListenerArn | |
| AWS::ElasticLoadBalancingV2::ListenerRule | RuleArn | |

### Cost

ELB charges $0.008 per LCU-hour (approximated as flat per-request rate).

---

## Route 53

**Endpoint:** `route53.amazonaws.com` (global)
**Protocol:** REST/XML

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateHostedZone | Returns HTTP 201; zone IDs prefixed `/hostedzone/Z` |
| GetHostedZone | |
| DeleteHostedZone | |
| ListHostedZones | |
| ChangeResourceRecordSets | CREATE/DELETE/UPSERT actions |
| ListResourceRecordSets | |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::Route53::HostedZone | HostedZoneId | |
| AWS::Route53::RecordSet | — | |

### Cost

Route 53 hosted zone: $0.50/month per zone (tracked as flat cost on CreateHostedZone).

---

## Resource Groups Tagging

**Endpoint:** `tagging.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: ResourceGroupsTaggingAPI_20170126.{Op}`)

### Supported operations

| Operation | Notes |
|-----------|-------|
| GetResources | Supports ResourceTypeFilters, TagFilters; base64 pagination token |
| TagResources | Applies tags to existing resources by ARN |
| UntagResources | Removes tag keys from resources by ARN |

Scanned resource types: S3 buckets, Lambda functions, SQS queues, DynamoDB
tables, EC2 instances, IAM users, IAM roles.

### Cost

Resource Groups Tagging API operations are free.

---

## SNS

**Endpoint:** `sns.{region}.amazonaws.com`
**Protocol:** AWS Query (form-encoded, `Action=` parameter)

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateTopic | |
| GetTopicAttributes | |
| SetTopicAttributes | |
| DeleteTopic | |
| ListTopics | |
| Subscribe | Supports lambda, sqs, http, https, email protocols |
| Unsubscribe | |
| ListSubscriptions | |
| ListSubscriptionsByTopic | |
| Publish | Dispatches to subscribed Lambda/SQS via cross-service dispatch |
| PublishBatch | |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::SNS::Topic | TopicArn | |
| AWS::SNS::Subscription | SubscriptionArn | |

### Cost

SNS publish: $0.0000005 per message.

---

## Secrets Manager

**Endpoint:** `secretsmanager.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: secretsmanager.{Op}`)

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateSecret | |
| GetSecretValue | Returns SecretString or SecretBinary |
| PutSecretValue | Creates new version |
| UpdateSecret | |
| DeleteSecret | Supports ForceDeleteWithoutRecovery |
| ListSecrets | |
| DescribeSecret | |
| TagResource | |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::SecretsManager::Secret | SecretArn | |

### Cost

Secrets Manager API calls: $0.05 per 10,000 API calls.

---

## SSM Parameter Store

**Endpoint:** `ssm.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AmazonSSM.{Op}`)

### Supported operations

| Operation | Notes |
|-----------|-------|
| PutParameter | Supports String, StringList, SecureString types |
| GetParameter | Supports WithDecryption |
| GetParameters | Batch get |
| GetParametersByPath | Recursive path traversal |
| DeleteParameter | |
| DescribeParameters | |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::SSM::Parameter | ParameterName | |

### Cost

SSM standard parameters are free. Advanced parameters: $0.05 per 10,000 API interactions.

---

## KMS

**Endpoint:** `kms.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: TrentService.{Op}`)

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateKey | |
| DescribeKey | |
| ListKeys | |
| ScheduleKeyDeletion | |
| Encrypt | Returns ciphertext blob (base64-encoded stub) |
| Decrypt | Returns plaintext (stub pass-through) |
| GenerateDataKey | |
| GenerateDataKeyWithoutPlaintext | |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::KMS::Key | KeyId | |
| AWS::KMS::Alias | — | |

### Cost

KMS API requests: $0.03 per 10,000 requests.

---

## CloudWatch Logs

**Endpoint:** `logs.{region}.amazonaws.com`
**Protocol:** JSON (`application/x-amz-json-1.1`, `X-Amz-Target: Logs_20140328.{Op}`)

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateLogGroup | |
| DeleteLogGroup | Also removes the group's streams and their events |
| DescribeLogGroups | Reports both ARN forms — see below |
| PutRetentionPolicy | `retentionInDays` must be one of the API's 22 enumerated values |
| DeleteRetentionPolicy | The documented way to make a group's events never expire |
| CreateLogStream | |
| DeleteLogStream | |
| DescribeLogStreams | |
| PutLogEvents | Accepts up to 10,000 events per call |
| GetLogEvents | Supports nextForwardToken pagination |
| FilterLogEvents | Substring match on `filterPattern`; reports `searchedLogStreams` |

Lambda auto-creates `/aws/lambda/{name}` log groups.

### Response members are camelCase

CloudWatch Logs is a JSON-1.1 service whose members are camelCase, and an SDK
matches response members against the service model **case-sensitively** — a
PascalCase member does not fail to parse, it parses to *nothing*, so a caller
receives one empty object per resource with an HTTP 200 and no error. Earlier
releases had `DescribeLogGroups`, `DescribeLogStreams` and `GetLogEvents` doing
exactly that (`FilterLogEvents` did not), so a `len()` assertion passed while every
field read raised `KeyError`. All four now emit the API's member names.

`DescribeLogGroups` reports **both** ARN forms the reference documents as distinct
members: `logGroupArn` without a trailing `:*`, which is what a
`logGroupIdentifier` input or a tagging API wants, and `arn` with it, which is
what an IAM policy wants for most actions. They differ only in that suffix.

A group with no retention policy omits `retentionInDays` entirely rather than
reporting `0`, because the API has no value meaning "never" — the member's absence
is the signal, which is why `DeleteRetentionPolicy` exists.

`PutRetentionPolicy` accepts only the enumerated day counts (1, 3, 5, 7, 14, 30,
60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288,
3653); anything else — including a plausible 45 or 100 — is an
`InvalidParameterException`. Note that this service returns
`ResourceNotFoundException` at **HTTP 400**, not 404, as its reference documents:
the error code travels in the body's `__type`, not the status line. Substrate's
older group- and stream-level not-found responses on the other operations still
use 404 and are not changed here.

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::Logs::LogGroup | LogGroupName | |
| AWS::Logs::LogStream | LogStreamName | |

### Cost

CloudWatch Logs ingestion: $0.50 per GB. Storage: $0.03 per GB-month.

---

## EventBridge

**Endpoint:** `events.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AWSEvents.{Op}`)

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateEventBus | |
| DescribeEventBus | |
| DeleteEventBus | |
| ListEventBuses | |
| PutRule | |
| DescribeRule | |
| DeleteRule | |
| ListRules | |
| PutEvents | Stores last 100 events in ring buffer |
| ListTargetsByRule | |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::Events::Rule | RuleArn | |

### Cost

EventBridge custom events: $1.00 per million events.

---

## CloudWatch

**Endpoint:** `monitoring.{region}.amazonaws.com`
**Protocol:** AWS Query (form-encoded, `Action=` parameter)

### Supported operations

| Operation | Notes |
|-----------|-------|
| PutMetricData | |
| GetMetricData | |
| GetMetricStatistics | |
| PutMetricAlarm | |
| DescribeAlarms | |
| DeleteAlarms | |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::CloudWatch::Alarm | AlarmName | |

### Cost

CloudWatch metrics: $0.30 per metric per month. Alarms: $0.10 per alarm per month.

---

## ACM

**Endpoint:** `acm.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: CertificateManager.{Op}`)

### Supported operations

| Operation | Notes |
|-----------|-------|
| RequestCertificate | Certificate auto-transitions to ISSUED status |
| DescribeCertificate | |
| DeleteCertificate | |
| ListCertificates | |
| AddTagsToCertificate | |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::CertificateManager::Certificate | CertificateArn | |

### Cost

ACM certificates are free.

---

## API Gateway (REST)

**Endpoint:** `apigateway.{region}.amazonaws.com`
**Protocol:** REST/JSON

**Response shape:** every member is lowerCamel as the service model spells it
(`id`, `name`, `rootResourceId`, `resourceMethods`, `methodIntegration`, …), and
collection responses nest their elements under **`item`** — singular, because that
is the `locationName` of the `items` member. `GetUsage` uses a third spelling,
`values`, and is not routed. Responses carry no pagination `position`: Substrate
returns every element in one page and honours no token. Earlier releases sent
PascalCase members under an `items` envelope, which an AWS SDK parsed to an empty
result with no error (#529).

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateRestApi | Auto-creates root `/` resource |
| GetRestApi | |
| DeleteRestApi | |
| GetRestApis | |
| CreateResource | |
| GetResource | |
| DeleteResource | |
| GetResources | |
| PutMethod | |
| GetMethod | |
| DeleteMethod | |
| PutIntegration | |
| GetIntegration | |
| CreateDeployment | |
| GetDeployment | |
| CreateStage | |
| GetStage | |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::ApiGateway::RestApi | RestApiId | |
| AWS::ApiGateway::Resource | ResourceId | |
| AWS::ApiGateway::Method | — | |
| AWS::ApiGateway::Deployment | DeploymentId | |
| AWS::ApiGateway::Stage | StageName | |

### Cost

API Gateway REST API calls: $3.50 per million calls.

---

## API Gateway v2 (HTTP)

**Endpoint:** `apigateway.{region}.amazonaws.com`
**Protocol:** REST/JSON (`/v2/` prefix)

**Routing:** v1 and v2 are one endpoint and one SigV4 signing name — an
apigatewayv2 client signs as `apigateway`, uses the same hostname, and sends no
`X-Amz-Target` — so Substrate discriminates them by **path**: a request under
`/v2/` that would otherwise resolve to `apigateway` is routed to the v2 plugin.
Every requestUri in the apigatewayv2 API is under `/v2/` and none of v1's is, so
the split is exact. A consumer pointing an `apigatewayv2` client at Substrate needs
no special configuration.

**Response shape:** every member is lowerCamel as the service model spells it
(`apiId`, `routeId`, `integrationId`, `apiEndpoint`, `protocolType`, …), and
collection responses nest their elements under **`items`** — lowercase, unlike v1's
singular `item`. Substrate returns every element in one page and honours no
pagination token, so no response carries a `nextToken`. Two members v1 has are
absent here because the v2 model does not declare them: a route, integration, stage
or API mapping reports no `apiId` (the API is a path parameter of the request), and
a domain name reports no `regionalDomainName` — v2 nests that hostname as
`domainNameConfigurations[].apiGatewayDomainName`. Earlier releases sent PascalCase
members under an `Items` envelope, which an AWS SDK parsed to an empty result with
no error (#529).

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateApi | |
| GetApi | |
| DeleteApi | |
| GetApis | |
| CreateRoute | |
| GetRoute | |
| DeleteRoute | |
| CreateIntegration | |
| GetIntegration | |
| CreateStage | |
| GetStage | |
| CreateAuthorizer | |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::ApiGatewayV2::Api | ApiId | |
| AWS::ApiGatewayV2::Route | RouteId | |
| AWS::ApiGatewayV2::Integration | IntegrationId | |
| AWS::ApiGatewayV2::Stage | StageName | |

### Cost

API Gateway HTTP API calls: $1.00 per million calls.

---

## Step Functions

**Endpoint:** `states.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AmazonStates.{Op}`)

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateStateMachine | |
| DescribeStateMachine | |
| DeleteStateMachine | |
| ListStateMachines | |
| StartExecution | Returns RUNNING status immediately |
| DescribeExecution | Transitions to SUCCEEDED on describe |
| StopExecution | |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::StepFunctions::StateMachine | StateMachineArn | |

### Cost

Step Functions state transitions: $0.025 per 1,000 transitions.

---

## ECR

**Endpoint:** `ecr.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AmazonEC2ContainerRegistry_V1_1_0.{Op}`)

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateRepository | |
| DescribeRepositories | |
| DeleteRepository | |
| GetAuthorizationToken | Returns base64("AWS:password") |
| PutImage | |
| BatchGetImage | |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::ECR::Repository | RepositoryName | |

### Cost

ECR storage: $0.10 per GB-month. Data transfer is free within the same region.

---

## ECS

**Endpoint:** `ecs.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AmazonEC2ContainerServiceV20141113.{Op}`)

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateCluster | |
| DescribeClusters | |
| DeleteCluster | |
| ListClusters | |
| RegisterTaskDefinition | |
| DescribeTaskDefinition | |
| ListTaskDefinitions | |
| CreateService | |
| DescribeServices | |
| UpdateService | |
| DeleteService | |
| RunTask | |
| DescribeTasks | |
| ListTasks | |
| StopTask | |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::ECS::Cluster | ClusterName | |
| AWS::ECS::TaskDefinition | TaskDefinitionArn | |
| AWS::ECS::Service | ServiceName | |

### Cost

ECS Fargate vCPU: $0.04048 per vCPU-hour. Memory: $0.004445 per GB-hour.

---

## Cognito User Pools

**Endpoint:** `cognito-idp.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AWSCognitoIdentityProviderService.{Op}`)

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateUserPool | Pool ID format: `{region}_{12-char alphanum}` |
| DescribeUserPool | |
| DeleteUserPool | |
| ListUserPools | |
| CreateUserPoolClient | |
| DescribeUserPoolClient | |
| DeleteUserPoolClient | |
| AdminCreateUser | |
| AdminGetUser | |
| AdminDeleteUser | |
| InitiateAuth | Returns stub JWT tokens |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::Cognito::UserPool | UserPoolId | |
| AWS::Cognito::UserPoolClient | ClientId | |

### Cost

Cognito MAUs: first 50,000 free, then $0.0055 per MAU.

---

## Cognito Identity

**Endpoint:** `cognito-identity.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AWSCognitoIdentityService.{Op}`)

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateIdentityPool | |
| DescribeIdentityPool | |
| DeleteIdentityPool | |
| GetCredentialsForIdentity | Returns stub temporary credentials |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::Cognito::IdentityPool | IdentityPoolId | |

### Cost

Cognito Identity operations are free.

---

## Kinesis Data Streams

**Endpoint:** `kinesis.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: Kinesis_20131202.{Op}`)

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateStream | |
| DescribeStream | |
| DescribeStreamSummary | |
| DeleteStream | |
| ListStreams | |
| PutRecord | |
| PutRecords | Batch put |
| GetShardIterator | Returns base64-encoded cursor |
| GetRecords | Ring buffer of last 10,000 records per shard |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::Kinesis::Stream | StreamName | |

### Cost

Kinesis shard: $0.015 per shard-hour. PUT payload: $0.014 per million 25KB units.

---

## CloudFront

**Endpoint:** `cloudfront.amazonaws.com` (global)
**Protocol:** REST/XML

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateDistribution | Distribution IDs: `E{13-char upper alphanum}` |
| GetDistribution | |
| UpdateDistribution | |
| DeleteDistribution | |
| ListDistributions | |
| TagResource | |

All CloudFront resources are stored under `us-east-1` (global service).

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::CloudFront::Distribution | DistributionId | |

### Cost

CloudFront HTTPS requests: $0.0100 per 10,000 requests (approximate).

---

## RDS

**Endpoint:** `rds.{region}.amazonaws.com`
**Protocol:** AWS Query (form-encoded, `Action=` parameter)

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateDBInstance | |
| DescribeDBInstances | |
| DeleteDBInstance | |
| ModifyDBInstance | |
| CreateDBSnapshot | |
| DescribeDBSnapshots | |
| DeleteDBSnapshot | |
| RestoreDBInstanceFromDBSnapshot | |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::RDS::DBInstance | DBInstanceIdentifier | |

### Cost

RDS db.t3.micro on-demand: $0.017 per hour (approximate for testing purposes).

---

## ElastiCache

**Endpoint:** `elasticache.{region}.amazonaws.com`
**Protocol:** AWS Query (form-encoded, `Action=` parameter)

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateCacheCluster | |
| DescribeCacheClusters | |
| DeleteCacheCluster | |
| CreateReplicationGroup | |
| DescribeReplicationGroups | |
| DeleteReplicationGroup | |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::ElastiCache::CacheCluster | CacheClusterId | |
| AWS::ElastiCache::ReplicationGroup | ReplicationGroupId | |

### Cost

ElastiCache cache.t3.micro: $0.017 per node-hour (approximate).

---

## EFS

**Endpoint:** `elasticfilesystem.{region}.amazonaws.com`
**Protocol:** REST/JSON

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateFileSystem | |
| DescribeFileSystems | |
| DeleteFileSystem | |
| CreateMountTarget | |
| DescribeMountTargets | |
| DeleteMountTarget | |
| CreateAccessPoint | |
| DescribeAccessPoints | |
| DeleteAccessPoint | |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::EFS::FileSystem | FileSystemId | |
| AWS::EFS::MountTarget | MountTargetId | |
| AWS::EFS::AccessPoint | AccessPointId | |

### Cost

EFS standard storage: $0.30 per GB-month.

---

## Glue

**Endpoint:** `glue.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AWSGlue.{Op}`)

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateDatabase | |
| GetDatabase | |
| DeleteDatabase | |
| GetDatabases | |
| CreateTable | |
| GetTable | |
| DeleteTable | |
| GetTables | |
| CreateJob | |
| GetJob | |
| DeleteJob | |
| GetJobs | |
| StartJobRun | Returns JobRunId |
| GetJobRun | Transitions to SUCCEEDED after describe |
| GetJobRuns | |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::Glue::Database | DatabaseName | |
| AWS::Glue::Table | TableName | |
| AWS::Glue::Job | JobName | |

### Cost

Glue ETL jobs: $0.44 per DPU-hour. Crawlers: $0.44 per DPU-hour.

---

## Cost Explorer

**Endpoint:** `ce.us-east-1.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AWSInsightsIndexService.{Op}`)

Cost Explorer reads from the Substrate `EventStore` to return real usage data
from your test runs.

### Supported operations

| Operation | Notes |
|-----------|-------|
| GetCostAndUsage | Aggregates event costs by service/operation |
| GetCostForecast | Returns stub forecast based on recent usage |

### Cost

Cost Explorer API calls: $0.01 per request.

---

## Budgets

**Endpoint:** `budgets.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AWSBudgetServiceGateway.{Op}`)

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateBudget | `DuplicateRecordException` if name already exists |
| DescribeBudget | `NotFoundException` if missing |
| UpdateBudget | |
| DeleteBudget | |
| DescribeBudgets | Lists all budgets for account |
| DescribeBudgetActionsForBudget | |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::Budgets::Budget | BudgetName | |

### Cost

Budgets: first two budgets free, then $0.02 per budget per day.

---

## Health

**Endpoint:** `health.us-east-1.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AWSHealth_20160804.{Op}`)

The Health plugin is a stub that returns empty valid responses. It exists to
allow infrastructure code that calls the Health API to run without errors.

### Supported operations

| Operation | Notes |
|-----------|-------|
| DescribeEvents | Returns empty events list |
| DescribeEventDetails | Returns empty details |
| DescribeAffectedEntities | Returns empty entities |

### Cost

Health API calls are free.

---

## Price List Query API

**Endpoints:** `api.pricing.us-east-1.amazonaws.com`,
`api.pricing.ap-south-1.amazonaws.com`, `api.pricing.eu-central-1.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AWSPriceListService.{Op}`)

This is the *server* side of pricing — for code that queries AWS rates at
runtime. It is the inverse of Substrate's own cost-tracking pricing provider,
which consumes the public offer index to cost simulated usage (the
`/v1/pricing/refresh`, `/v1/pricing/lookup`, `/v1/pricing/discounts` and
`/v1/pricing/credits` control endpoints, and the `substrate pricing` command).

The offer corpus is seven Amazon S3 SKUs copied verbatim from the live
`AmazonS3/current/us-east-1/index.json` offer file (version `20260728131000`).
It is small on purpose: each SKU exists to reproduce a response shape that
callers get wrong, so a consumer's parser is tested against real awkwardness
rather than a tidied-up fixture.

### Supported operations

| Operation | Notes |
|-----------|-------|
| GetProducts | `ServiceCode` required; `Filters` 0–50; `MaxResults` 1–100 |
| DescribeServices | All fields optional; `MaxResults` 1–100 |
| GetAttributeValues | `AttributeName` **and** `ServiceCode` required; `MaxResults` 1–**10000** |

`FormatVersion` accepts only `aws_v1`, the sole documented value. Pagination
uses an opaque `NextToken`; a token that does not decode, or that points past the
end of the result set, is an `InvalidNextTokenException`.

`Filter.Type` supports the full documented enum — `TERM_MATCH`, `EQUALS`,
`CONTAINS`, `ANY_OF`, `NONE_OF` — not just `TERM_MATCH`. `ANY_OF` and `NONE_OF`
take a comma-separated `Value`. Filters are conjunctive, and a filter naming a
field a product does not carry never matches it, including `NONE_OF`.

### Response shapes worth knowing about

These are the traps the corpus deliberately preserves. Each is verified against
the live offer file.

- **`PriceList` elements are JSON documents encoded as strings**, not objects.
  Decoding requires a second unmarshal per element.
- **`pricePerUnit` values are strings** with trailing zeros (`"0.0230000000"`),
  never numbers. So are `beginRange` and `endRange`.
- **`productFamily` is absent from most products** — 315 of the 381 in the real
  S3 offer file omit it. A filter on `productFamily` therefore misses the
  majority of SKUs. `usagetype` is the attribute that is reliably present and
  1:1 with a SKU.
- **`TimedStorage-ByteHrs` carries three `priceDimensions`**, the last with
  `"endRange": "Inf"`. Reading only the first reports the first-50 TB rate as if
  it were the only rate.
- **`Requests-Tier1` is `"0.0000050000"` per request**, and its `unit` is
  `Requests` — that is $0.005 per 1,000. Dividing by 1,000 again is a 1,000×
  error.
- **Filtering `productFamily=Storage` with `volumeType="Glacier Deep Archive"`
  returns only `TimedStorage-GDA-Staging` at $0.021/GB-Mo** — the staging rate,
  21× the $0.00099 archive rate. No `TimedStorage-GDA-ByteHrs` SKU exists in the
  S3 offer file at all, so that filter cannot return the rate a caller expects;
  the nearest $0.00099 SKU is Intelligent-Tiering's
  `TimedStorage-INT-DAA-ByteHrs`.

An unknown `ServiceCode` is a `NotFoundException` rather than an empty
`PriceList`. Substrate's corpus is far smaller than AWS's catalog, and a loud
error is better than an empty result that reads as "AWS has no such price".

### Endpoint regions — a deliberate divergence

AWS hosts the Price List Query API in exactly three regions: `us-east-1`,
`ap-south-1` and `eu-central-1`. There is no `api.pricing.eu-west-1.amazonaws.com`
to resolve.

Substrate serves every region from one endpoint, so it cannot reproduce a name
that fails to resolve. Instead, a request signed for any other region is rejected
with **`SubstrateInvalidPricingEndpoint`** (HTTP 400). The code is deliberately
not an AWS code — this is Substrate reporting a condition AWS surfaces at the
transport layer, and naming it as such is better than silently pricing a request
against an endpoint that does not exist.

### Seeding failures

Pricing is the kind of dependency whose failure should degrade a caller, not stop
it. That property is only testable if the failure can be produced on demand:

```bash
# Fail one operation.
curl -X POST http://localhost:4566/v1/pricing/query-failures \
  -d '{"operation":"GetProducts","code":"ThrottlingException","message":"Rate exceeded"}'

# Fail every operation (wildcard).
curl -X POST http://localhost:4566/v1/pricing/query-failures \
  -d '{"code":"InternalErrorException"}'

# Clear one, or all.
curl -X DELETE 'http://localhost:4566/v1/pricing/query-failures?operation=GetProducts'
curl -X DELETE http://localhost:4566/v1/pricing/query-failures
```

An operation-specific seed takes precedence over the wildcard. `statusCode`
defaults to the status the Price List API documents for the code — 400 for every
documented code except `InternalErrorException`, which is 500 — and may be
overridden explicitly.

`code` must be one of the seven codes the Price List API documents:
`AccessDeniedException`, `ExpiredNextTokenException`, `InvalidNextTokenException`,
`InvalidParameterException`, `NotFoundException`, `ThrottlingException`,
`InternalErrorException`. Anything else is rejected with a 400, because a typo'd
code would seed an error no SDK catch branch matches — the fallback path would go
untested while the seed itself appeared to work.

### Cost

Price List API calls are free.

---

## Organizations

**Endpoint:** `organizations.us-east-1.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: Organizations_20161128.{Op}`)

On the first `DescribeOrganization` call, the plugin auto-creates an
organization and master account.

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateOrganization | |
| DescribeOrganization | Auto-creates org on first call |
| ListRoots | |
| CreateAccount | |
| DescribeAccount | `AccountNotFoundException` if missing |
| ListAccounts | |

### Cost

Organizations API calls are free.

---

## SES v2

**Endpoint:** `email.{region}.amazonaws.com`
**Protocol:** REST/JSON

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateEmailIdentity | |
| GetEmailIdentity | |
| DeleteEmailIdentity | |
| ListEmailIdentities | |
| SendEmail | Returns stub MessageId; does not deliver |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::SES::EmailIdentity | EmailIdentityName | |

### Cost

SES outbound email: $0.10 per 1,000 emails.

---

## Kinesis Data Firehose

**Endpoint:** `firehose.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: Firehose_20150804.{Op}`)

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateDeliveryStream | |
| DescribeDeliveryStream | |
| DeleteDeliveryStream | |
| ListDeliveryStreams | |
| PutRecord | |
| PutRecordBatch | |

### CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::KinesisFirehose::DeliveryStream | DeliveryStreamName | |

### Cost

Firehose data ingestion: $0.029 per GB.

---

## Batch

**Endpoint:** `batch.{region}.amazonaws.com`
**Protocol:** REST/JSON (`POST /v1/{operation}`)

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateComputeEnvironment | `computeEnvironmentName` and `type` required; an omitted `state` is `ENABLED` |
| DescribeComputeEnvironments | `computeEnvironments` filter takes [names or full ARNs](#a-describe-filter-takes-a-name-or-an-arn); reports `ecsClusterArn` |
| CreateJobQueue | `jobQueueName` required; an omitted `state` is `ENABLED` |
| DescribeJobQueues | `jobQueues` filter takes names or full ARNs |
| RegisterJobDefinition | `jobDefinitionName` and `type` required; each registration is [the next revision](#a-job-definition-is-versioned) |
| DescribeJobDefinitions | `jobDefinitions` (`${name}:${revision}` or full ARN), `jobDefinitionName` (every revision), and `status` |
| SubmitJob | Returns `jobId`; the job is immediately `SUCCEEDED` |
| DescribeJobs | |
| TerminateJob | Reports the job `FAILED` with the supplied `reason` |

Every operation reports a bad request as **`ClientException`** at HTTP 400. The API
reference declares exactly two errors for each Batch operation, `ClientException` and
`ServerException`, so substrate does not use the `MissingParameter` or
`InvalidParameterValue` codes other services return for the same shape of complaint.

### A describe filter takes a name or an ARN

All three resource describes document their filter as "a list of up to 100 … names or
full Amazon Resource Name (ARN) entries", and both forms resolve. An **absent** filter
reports every resource of that type in the caller's account and Region.

A filter entry naming a resource that does not exist is **skipped, not refused**: the
operations describe "one or more of your compute environments" and document no
not-found error, so an absent name yields an absent result rather than an error. A
filter in which nothing matches is an empty list at HTTP 200.

`maxResults` and `nextToken` paginate. An absent or out-of-range `maxResults` reports up
to 100 results, per the reference's "if this parameter isn't used, then Describe…
returns up to 100 results". `nextToken` is **omitted** once the results are exhausted —
"this value is null when there are no more results to return" — including when the last
page is exactly full.

### A job definition is versioned

Each `RegisterJobDefinition` of a name is a distinct revision, numbered from 1, and each
revision is its own record. `DescribeJobDefinitions` addresses one revision through
`jobDefinitions` (`${name}:${revision}` or the full ARN) and every revision of a
definition through `jobDefinitionName`.

`jobDefinitions` **wins outright** when both are sent, rather than being intersected or
unioned: the reference states it "can't be used with other parameters".

A newly registered definition is `ACTIVE`, and the `status` filter selects on that.
Nothing yet reports a definition `INACTIVE` — `DeregisterJobDefinition` is not
implemented (tracked as issue #555) — but the status is recorded on the resource rather
than synthesised at read time, so a deregistration can set it.

### Resources are scoped to the caller

A compute environment, job queue and job definition is recorded against the account and
Region of the request that created it, and reported only to a caller in that scope, as
every other partitioned plugin does. The ARN a create returns therefore names the
caller's own account and Region, and is an identifier that caller's `SubmitJob` and
`computeEnvironmentOrder` can use. A job definition's revision counter is scoped the
same way, so two accounts each registering one definition of the same name both see
revision 1.

### Cost

Batch itself is free; the compute it launches is not, and substrate launches none.

---

## SSO / Identity Store

**Endpoint:** `sso.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: SWBExternalService.{Op}`)

`SWBExternalService` is the `sso-admin` API's target prefix — not a name that appears
in any published SDK surface, but what every client sends. Substrate accepts the
plausible-looking `AWSSSOAdminService` as well, so a caller that constructs the target
header by hand from the service name also reaches this plugin.

### Supported operations

| Operation | Notes |
|-----------|-------|
| ListInstances | One instance per account, created on first read |
| CreatePermissionSet | |
| DescribePermissionSet | |
| UpdatePermissionSet | |
| DeletePermissionSet | |
| ListPermissionSets | |
| AttachManagedPolicyToPermissionSet | |
| DetachManagedPolicyFromPermissionSet | |
| ListManagedPoliciesInPermissionSet | |
| CreateAccountAssignment | |
| DeleteAccountAssignment | |
| ListAccountAssignments | |

---

## Fault injection

Fault injection is cross-service rather than a plugin, so it lives here rather than in a
per-service section. Rules are armed in process through `NewFaultController`, from a
configuration file's `fault:` block, or over the wire:

```
POST   /v1/fault/rules   {"enabled":true,"rules":[{…}]}
GET    /v1/fault/rules   → the live configuration, each rule carrying its fired count
DELETE /v1/fault/rules   → disable and clear
```

Faults are evaluated **before the request reaches a plugin**, so a rule fires whether or
not the operation it names is implemented, and no state is written for a request a fault
refuses. `POST /v1/state/reset` clears the rules along with the state.

A rule matches on five fields, all AND-ed, each ignored when empty:

| Matcher | Matches |
|---------|---------|
| `service` | the service name (`s3`, `ec2`, …) |
| `operation` | the **semantic** operation name (`PutObject`, `UploadPart`, …) |
| `path_suffix` | requests whose path ends with the string (`.parquet`, `/big.bin`) |
| `query_key` | requests carrying the query parameter, whatever its value (`uploads`, `uploadId`, `partNumber`) |
| `header_prefix` | requests carrying a header whose name starts with the prefix, compared case-insensitively |

**S3 operations are named semantically, like every other service's.** The request parser
resolves an S3 REST request to `PutObject`, `UploadPart`, `CompleteMultipartUpload` and
so on before faults are evaluated, so a rule naming `PutObject` fires on `PutObject` and
not on `UploadPart`, which is also a `PUT` to an object path. Previously an S3 request
carried its bare HTTP verb at this point, so `operation: PutObject` matched nothing at
all and `operation: PUT` took out both. **A rule naming a bare HTTP method therefore no
longer matches an S3 request** — a rule on `GET` still fires for a service whose
operation genuinely is its method, such as `execute-api`.

**`query_key` is what separates the multipart sub-operations.** `CreateMultipartUpload`,
`UploadPart` and `CompleteMultipartUpload` share a path and differ from each other by a
`POST`-versus-`PUT` and by a sub-resource parameter — `?uploads`, `?partNumber=&uploadId=`,
`?uploadId=`. Presence is what those parameters signal, so the value is not compared. The
three wire matchers exist for distinctions an operation name does not carry: one key
rather than every key, or one header family rather than every request.

### `times` bounds a rule, and zero means one

| `times` | Fires on |
|---------|----------|
| absent / `0` | exactly **one** matching request |
| `n > 0` | the first `n` matching requests |
| negative | every matching request |

The bound is what makes retry assertable: fail twice, then succeed, is the outcome that
distinguishes working retry from no retry, and an unbounded rule can only ever produce
failure. **Zero means one rather than unlimited** deliberately — reading a missing field
as unlimited turns a typo into a fixture that consumes a consumer's whole retry budget.

A rule that has reached its bound is skipped rather than ending evaluation, so a later
rule still gets its turn. The match, the probability roll and the increment all happen
under one lock: with `times: 1`, N concurrent requests produce exactly one failure, which
is not something a counter on the client side can arrange.

Set `times: -1` when a fixture arms a fault and then clears it to assert the retry
succeeds — with the default of one the rule would already be spent, and the assertion
would pass whether or not clearing worked.

### The fired count

Each rule reports a `fired` count through `GET /v1/fault/rules`; `FaultsFired()` sums
them for an in-process test. **A rule that matches nothing produces exactly the same
passing test as a consumer's retry working**, so a fixture that arms a fault and then
observes success has proven nothing without asserting the count. Arming rules again
replaces the configuration and resets every count, so a fixture that re-arms the same
rule between phases gets its full budget back rather than a spent one.

### An injected error is indistinguishable from a real one

An injected error is serialized in the wire shape the target service's own errors use.
That matters most for S3, whose error document is a bare `<Error>` with a `<RequestId>`
rather than the `<ErrorResponse>` wrapper the Query protocol uses: an SDK recovers no
code from the wrapped form and falls back to the HTTP status, so an injected `SlowDown`
used to arrive at the client as `ServiceUnavailable` and a consumer matching on
`SlowDown` never saw their own fault. The bytes now come from the same function the S3
plugin uses, so an injected `NoSuchKey` and a genuine one are byte-identical — which is
the one property a fault injector must not lack, since a caller who can tell the two
apart can tell a fixture from production.

`cloudfront` and `route53` are REST-XML like S3 but keep the `<ErrorResponse>` shape:
their real error documents genuinely are wrapped, so they were already correct.

### `probability` draws from a per-rule PRNG

Each rule draws from its own PRNG stream, seeded from the controller's seed and the
rule's index. A rule's outcome sequence therefore depends only on how many requests
that rule itself matched: adding an unrelated rule, or changing how often one matches,
leaves the others' rolls unchanged. Streams are re-derived whenever a configuration is
armed, so re-arming resets them exactly as it resets each rule's `fired` count.

The stream is keyed by a rule's **index**, so reordering rules does change their
outcomes. That is deliberate — two rules with identical matchers are legitimate and
useful, and keying by the matchers instead would make them share a stream and would
make editing a path suffix silently reshuffle results. Prefer `times` for a bounded
outcome regardless: it needs no roll at all.
