# CloudFormation

**Endpoint:** `cloudformation.{region}.amazonaws.com`
**Protocol:** AWS Query (form-encoded, `Action=` parameter)

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateStack | Deploys every resource in `TemplateBody`, or in the object `TemplateURL` names in substrate's S3 (see below), and returns the stack ARN; honours `RoleARN` |
| UpdateStack | Re-deploys the template; an omitted `TemplateBody` and `TemplateURL` re-use the stored one, and an omitted `RoleARN` the stored role. A stack in `REVIEW_IN_PROGRESS` is refused, as it has no template to update |
| DeleteStack | Sweeps the resources the stack deployed, then removes the stack record (see below); deleting an absent stack succeeds; `RoleARN` applies to this operation only |
| DescribeStacks | One stack by `StackName`, or every stack when omitted; reports `RoleARN` when the stack has one. A stack a `CREATE` change set made reports `REVIEW_IN_PROGRESS` until it is executed, and its `CreationTime` is when the change set made it. An unknown stack's `ValidationError` names the identifier the caller sent, name or ARN |
| ListStacks | Summary shape; honours `StackStatusFilter.member.N` |
| DescribeStackResources | By `StackName` + optional `LogicalResourceId`, or by `PhysicalResourceId` |
| ListStackResources | `StackResourceSummary` per resource, from the same derivation as `DescribeStackResources`; pages 100 on `NextToken` (the page publishes only a 1 MB bound, so the size is substrate's), an unissued token is `ValidationError`; a deleted stack is not found (#1446) |
| DescribeStackEvents | Derived from the stack record rather than stored, so it cannot disagree with `DescribeStackResources` about whether a resource failed — the two are renderings of one fact. Pages 100 events on `NextToken`, the only paging parameter the operation publishes; a stack small enough for one page gets no token |
| GetTemplate | Returns the stored `TemplateBody` byte-for-byte; `TemplateStage` is [validated and honoured](#change-set-detail-template-stages-and-filtered-drift) |
| CreateChangeSet | `ChangeSetType` `UPDATE` (the default) or `CREATE`, which creates the stack in `REVIEW_IN_PROGRESS`; `IMPORT` is refused. Takes `TemplateBody`, `TemplateURL` or `UsePreviousTemplate`, and records `Tags`, `RoleARN`, `Capabilities`, `Description` and `OnStackFailure`. An `UPDATE` that changes nothing is `FAILED` with AWS's reason; see [Change sets](#change-sets-create-update-and-the-empty-change-set) |
| DescribeChangeSet | Accepts a bare change-set name or its ARN; reports `Status`, `StatusReason`, `ExecutionStatus`, `Description`, `Capabilities` and `OnStackFailure`; each `ResourceChange` carries [`Scope`, `Details` and `PhysicalResourceId`](#change-set-detail-template-stages-and-filtered-drift) |
| ExecuteChangeSet | Applies the change and its tags — a `CREATE` change set through the create path, honouring `DisableRollback` and `OnStackFailure` — then reports `EXECUTE_COMPLETE` or `EXECUTE_FAILED` and deletes the stack's other change sets. A change set that is not `AVAILABLE` is `InvalidChangeSetStatus` |
| ListChangeSets | A stack's change sets, with `Status`, `StatusReason`, `ExecutionStatus` and `Description` |
| DeleteChangeSet | Discards a pending set; deleting an absent set succeeds |
| DetectStackDrift | Returns a `StackDriftDetectionId` — UUID-shaped within the published maximum length of 36, [derived from the request ID](/reference/concepts/seeds-and-replay#an-identifier-a-replay-mints-is-the-one-it-recorded) (#856). It used to be substrate's internal `req-…` string, a shape no AWS reference describes. Honours [`LogicalResourceIds.member.N`](#change-set-detail-template-stages-and-filtered-drift) |
| DescribeStackDriftDetectionStatus | Resolves that ID to a completed detection |
| DescribeStackResourceDrifts | Per-resource drift; honours `StackResourceDriftStatusFilters.member.N` |
| ListExports | Every exported output value in the caller's account and Region, in one page |
| ListImports | Stack **names** importing an `ExportName`; an export nothing imports is an empty list |
| ValidateTemplate | Makes the [template checks](#strict-mode-and-template-validation) CloudFormation makes — syntax, `Parameters`, unrecognized resource types, undeclared and circular references — and answers `Parameters` (`ParameterKey`, `DefaultValue`, `NoEcho`, `Description`), `Description`, `Capabilities` (`CAPABILITY_IAM`, or `CAPABILITY_NAMED_IAM` when an IAM resource sets its name property), `CapabilitiesReason` and `DeclaredTransforms`. Strict mode is not consulted, so a type substrate does not emulate is a valid template. `TemplateBody` wins over `TemplateURL`, as the reference says; a `TemplateURL` alone is read from substrate's S3, as `CreateStack` reads one (#1605) |

**`TemplateURL` is read from substrate's own S3** ([#1606](https://github.com/scttfrdmn/substrate/issues/1606)),
by `CreateStack`, `UpdateStack` and `CreateChangeSet`. It is a `GetObject` made as the requesting
caller, routed and recorded like a stack's resource calls, so a caller who cannot read the object
cannot deploy it. The URL forms are S3's HTTPS addressing styles: path-style on
`s3.amazonaws.com`, `s3.{region}.amazonaws.com` and the legacy `s3-{region}.amazonaws.com`, and
virtual-hosted `{bucket}.s3[.{region}|-{region}].amazonaws.com`, a bucket name with dots included, and
the legacy `s3-external-1.amazonaws.com`.
Each regional form's dual-stack and FIPS endpoints (`s3.dualstack.{region}`, `s3-fips.{region}`,
`s3-fips.dualstack.{region}`) are accepted too, since the page excludes only S3 static websites. A
`versionId` query parameter reads that version, a presigned URL's `x-amz-*` parameters (any case) and
the JavaScript SDK's `x-id` are ignored
(the read is made as the caller anyway), and any other query parameter is refused rather than dropped.
The read happens after every check that needs no template, so a request refused for its stack name
or its `RoleARN` makes no `GetObject`.
The parameter's page requires `https://`. A path-style URL on the host the CloudFormation request was
itself sent to is substrate's own endpoint and is read over `http` or `https`: that is the URL the
CDK builds when it is pointed at an emulator, `${endpoint}/${bucket}/${key}`. Everything else is
refused with `ValidationError`:

| TemplateURL | Message |
|-------------|---------|
| Not an S3 URL — another host, `http://` on an AWS host, `s3://`, a website (`s3-website-…`) or Transfer Acceleration endpoint (substrate's reading), a label that is no Region, an unsupported query parameter, no key | `TemplateURL must be a supported URL.` |
| An object S3 cannot serve | `S3 error: ` followed by S3's own message, e.g. `The specified key does not exist.` |
| An empty object, or one over the page's 1 MB maximum | substrate's own message |
| Both `TemplateBody` and `TemplateURL` | substrate's own message; the pages say *"You must specify only one"* |

The first two wordings are observed rather than published: an AWS re:Post question and
`aws-cloudformation/cloudformation-coverage-roadmap#1212` quote the first, and
`aws-quickstart/quickstart-boomi-atom#24` the `S3 error:` prefix. A Systems Manager document URL is
not modelled.

A request that names something absent — a stack, a change set, a drift detection —
is a `ValidationError` at 400, and a template body that cannot be decoded is a
`ValidationError` at 400 prefixed `Template format error:`. A resource that failed
to deploy after the template parsed is an `InternalFailure` at 500, because the
request was well-formed and the failure is substrate's. That distinction is drawn
from the classification the stack model attaches to a failure, not from its
message, so a message may be reworded without moving any consumer's error code.

## Strict mode and template validation

A template is checked before anything deploys, by `CreateStack`, `UpdateStack`,
`CreateChangeSet`, `ValidateTemplate` and the in-process `Client.Deploy` alike
([#1605](https://github.com/scttfrdmn/substrate/issues/1605),
[#1548](https://github.com/scttfrdmn/substrate/issues/1548)). Every refusal below is a
`ValidationError` at 400, and a refused create leaves no stack.

The resource types CloudFormation recognizes, and the properties each declares, come from
the CloudFormation resource specification for us-east-1, vendored under
`emulator/cfnschema` and generated into `emulator/cfn_schema_gen.go` by
`cmd/gen-cfn-schema` (`make cfn-schema`; `make cfn-schema-fetch` refreshes the snapshot
over the network and is never run in CI).

| Check | Message | Where the wording comes from |
|-------|---------|------------------------------|
| A parameter with no `Type` | `Template format error: Every Parameters object must contain a Type member.` | CLI reports (Stack Overflow 55412786) |
| An unrecognized parameter type | `Template format error: Unrecognized parameter type: X` | CLI and CDK reports (aws-cdk #6786). `List<String>` is accepted |
| No resources, or a resource with no `Type` | `Template format error: At least one Resources member must be defined.` / `…Every Resources object must contain a Type member.` | CLI reports (Stack Overflow 41667181) |
| A type the specification does not list | `Template format error: Unrecognized resource types: [X, Y]` | aws-cli #8032 and the re:Post template-validation article |
| A `Ref` or `Fn::Sub` name that is not a parameter, a resource or a pseudo-parameter | `Template format error: Unresolved resource dependencies [X] in the Resources block of the template` | the re:Post template-validation article |
| The same in `Outputs` (`Value` or `Export.Name`) | `Unresolved resource dependencies [X] in the Outputs block of the template` | observed: CLI output quoted in the Bugs That Bite write-up and on Server Fault, with no prefix |
| An `Fn::GetAtt` to an undeclared resource | `Template error: instance of Fn::GetAtt references undefined resource X` | observed: Stack Overflow 41855596, aws-cdk #22046 |
| A type CloudFormation recognizes but substrate does not emulate (**strict mode**) | `Template declares resource types substrate does not emulate: [AWS::EKS::Cluster (Cluster)]. …` naming the opt-out | substrate's own |

The reference walk is **condition-blind**: it does not evaluate `Fn::If`, so an undeclared
name inside an untaken branch is refused, as CloudFormation's template check refuses it. A
reference to a *declared* resource skipped by a false `Condition` is not refused by this
walk. Several names are listed sorted; the GetAtt message names the first.

**Strict mode is on by default.** A resource type with no deploy helper in
`cfnResourceDeployers` (`emulator/cfn_resource_table.go`, the one list) is refused rather
than deployed as a stub that reports `CREATE_COMPLETE` for a resource nothing created.
Custom resources (`Custom::*` and `AWS::CloudFormation::CustomResource`) are refused too:
substrate runs no provider. A resource under a false `Condition` is not refused, because it
does not deploy. `AWS::CDK::Metadata` — which the CDK adds to every stack — is a modeled
no-op. A template that declares a `Transform` may use types only its macro defines (the
`AWS::Serverless::*` types) and names only its macro declares (`ServerlessRestApi`, a
function's implicit `<Function>Role`), so neither the unrecognized-type check nor the
undeclared-name check applies to it: an unknown type is left to strict mode, which refuses
it as unemulated. A type outside the `AWS::` and `Alexa::` namespaces — a third-party or
private registry type such as `Datadog::Monitors::Monitor` — or one ending `::MODULE` is one
an account may have activated, which substrate cannot know, so it too is treated as
recognized but not emulated: refused in strict mode, stubbed in lenient mode.
`ValidateTemplate` is given no parameter values, so it evaluates no condition and orders every
declared resource.

To deploy unemulated types as stubs, as every template did before strict mode existed, opt out with any
of:

- `cloudformation.lenient: true` in `substrate.yaml` (or its inverse, `cloudformation.strict: false`), or
  `SUBSTRATE_CLOUDFORMATION_LENIENT=true` (`SUBSTRATE_CLOUDFORMATION_STRICT=false`);
- `emulator.WithLenientCloudFormation()` on `StartTestServer`;
- `emulator.WithDeployerLenient(true)` on `NewStackDeployer`, or
  `emulator.WithClientLenientCloudFormation()` on `NewClient`.

**What ordinary CDK apps emit.** Synthesized with CDK 2.1149.0 and the flags `cdk init` sets, a
default `ec2.Vpc`, a Lambda function with an SQS event source, a `LambdaRestApi`, an SNS-to-SQS
subscription and an ECS Fargate service deploy and delete in strict mode;
`emulator/testdata/cfn/cdk` holds the templates and `TestCFN_OrdinaryCDKAppsDeployInStrictMode`
pins them. Strict mode refuses these common constructs, because substrate cannot deploy what they
emit:

| Type | Emitted by |
|------|------------|
| `AWS::ApplicationAutoScaling::ScalableTarget`, `::ScalingPolicy` | `autoScaleReadCapacity` / `autoScaleWriteCapacity` on a DynamoDB table, ECS service autoscaling — substrate has no Application Auto Scaling plugin |
| `AWS::Lambda::Url` | `addFunctionUrl` — the Lambda plugin routes no function-URL operations |
| `AWS::ApiGateway::Account` | a `RestApi` with `cloudWatchRole` on, the default without the `@aws-cdk/aws-apigateway:disableCloudWatchRole` flag — the API Gateway plugin routes no account settings |
| `Custom::S3AutoDeleteObjects` | `autoDeleteObjects: true` on a bucket |
| `Custom::S3BucketNotifications` | `addEventNotification` on a bucket |
| `Custom::*` generally | any construct backed by a custom-resource provider; substrate runs none |
| `AWS::Serverless::*` | a SAM template — substrate runs no transform |

An unrecognized type and an extraneous property are refused in both modes, because
CloudFormation refuses them.

**An extraneous property fails the resource, not the request.** A top-level key the
resource's type does not declare fails that resource with `CREATE_FAILED` (`UPDATE_FAILED` on
an update) and the stack rolls back, as CloudFormation's property validation does:
`Properties validation failed for resource Q with message: #: extraneous key
[VisibilityTimeoutt] is not permitted` (several keys are joined with a space). The wording is
observed behavior, quoted on re:Post and in aws-cdk #25409 and #28984. Only top-level keys are
checked; a nested property type's members, value types and required keys are not. A key whose
value is `AWS::NoValue` is not extraneous, and a property the specification declares but
substrate ignores stays accepted. Custom resources and `AWS::CDK::Metadata` are not checked.

## Change-set detail, template stages, and filtered drift

Three gaps a consumer that proves what a change set does fell into
([#1446](https://github.com/scttfrdmn/substrate/issues/1446)).

**`DescribeChangeSet`'s `ResourceChange` carries `Scope`, `Details` and `PhysicalResourceId`.**
The diff used to compare only `Type` and `Properties`, so a template that changed nothing but a
`DeletionPolicy` produced an empty change set. The rules, from `API_ResourceChange`,
`API_ResourceChangeDetail`, `API_ResourceTargetDefinition` and the user guide's "Example change sets":

| What changed | Detail(s) |
|--------------|-----------|
| A property's template value | `Static` / `DirectModification`, `Target` `Properties` + `Name` |
| Nothing in the template, but the property `Ref`s (or `Fn::Sub`s) a parameter whose value changed | `Dynamic` / `DirectModification` **and** `Static` / `ParameterReference` with `CausingEntity` the parameter |
| The `Tags` property | as above, `Target` `Tags` with no `Name`, `RequiresRecreation` `Never` |
| `DeletionPolicy`, `UpdateReplacePolicy`, `Metadata`, `CreationPolicy`, `UpdatePolicy` | `Static` / `DirectModification`, `Target` that attribute, `RequiresRecreation` `Never` |
| A property that `Fn::GetAtt`s a resource whose `Properties` or `Tags` change, or that is replaced | `Dynamic` / `ResourceAttribute`, `CausingEntity` `Resource.Attribute` |
| A property that `Ref`s a resource that will be replaced | `Dynamic` / `ResourceReference`, `CausingEntity` the resource |

A policy- or `Metadata`-only change does not propagate to dependents: it touches nothing an
`Fn::GetAtt` can read, so a retain-only change set names exactly the resources whose policies
changed. Values are compared after normalizing numbers (a JSON template decodes `3600` as a float, a
YAML one as an integer) and map keys, so the same template in the other syntax is no change.

Dependents are followed to a fixpoint. `Replacement` is the most intrusive detail — `Always` and
`Static` is `True`, `Always` and only `Dynamic` is `Conditional`, `Conditionally` is `Conditional`,
`Never` is `False`. `Scope` lists the distinct attributes in the API reference's enum order, and
`Details` lists `Dynamic` before `Static`, as the published samples do. `Add` and `Remove` carry
neither, since they "apply only to modified resources"; `Modify` and `Remove` carry the deployed
`PhysicalResourceId`, and `Add` none.

**`RequiresRecreation` is only as good as substrate's table.** It is a property's "Update requires"
behaviour in the resource-type reference, and substrate does not carry that catalogue: a short table
lists the properties whose page says *Replacement* for the types the deployer models natively
(`BucketName` and its two siblings; `QueueName`, `FifoQueue`; `TopicName`, `FifoTopic`; `TableName`,
`ImportSourceSpecification`; `RoleName`, `Path`; `FunctionName`, `PackageType`, `DurableConfig`,
`TenancyConfig`; the EC2 `KeyName`/`InstanceType` pair from the samples; and, since #1427 and #1428,
Transfer's `Domain`, FSx's `BackupId`/`FileSystemType`/`KmsKeyId`/`SecurityGroupIds`/`SubnetIds`,
MSK's `ClusterName`, a launch template's `LaunchTemplateName` and both of a backup selection's
properties), and any other property reports `Never`. So `Replacement: False` proves an in-place
update only for a property the table covers. The same table decides whether `UpdateStack` replaces
a changed resource — see [What an update does with each resource](#what-an-update-does-with-each-resource). A `DeletionPolicy`-only change set is `Never` throughout by the rule above, whatever the
table says. The property-value fields (`Path`, `BeforeValue`, `AfterValue`, `AttributeChangeType`),
`PolicyAction`, `BeforeContext`/`AfterContext` and `ChangeSource` `Automatic` are not derived.

**`GetTemplate` honours `TemplateStage`.** `Original | Processed` is validated (`ValidationError`
otherwise). "If the template doesn't include transforms, Original and Processed return the same
template", and both are reported in `StagesAvailable` — for a stack, and for a change set, which is
complete by the time `CreateChangeSet` answers. A template that declares a `Transform` is the case
substrate cannot answer honestly, because it runs no macro: `StagesAvailable` is `Original` alone, an
explicit `TemplateStage=Processed` is `ValidationError`, and an omitted stage answers `Original`, as
every `GetTemplate` did before. That last choice is substrate's: AWS publishes no answer for a stage
that is not available, and answering the submitted body *as* `Processed` would tell a caller the
transform expanded to nothing.

**`DetectStackDrift` honours `LogicalResourceIds.member.N`.** Only the named resources are checked,
so the detection's `StackDriftStatus` and `DriftedStackResourceCount` cover them alone. More than the
published 200, or a name the stack does not declare, is `ValidationError` — the operation publishes no
error of its own, and a filter matching nothing would report `IN_SYNC` for a resource that does not
exist. `DescribeStackResourceDrifts` still recomputes every resource, because it is not tied to a
detection ID.

## A stack ARN is accepted wherever `StackName` is

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

## A stack name is unique per account per Region

A stack, its change sets and its drift detections are stored under keys that carry the
caller's account and Region (`stack:<account>/<region>/<name>`, and likewise for
`changeset:`, `changeset_names:`, `stack_names:` and `drift_detection:`), so two accounts,
or two Regions of one account, can each create a stack named `app` and each reads back
only its own. Until [#1366](https://github.com/scttfrdmn/substrate/issues/1366) the keys
carried neither: the second `CreateStack` was refused `AlreadyExistsException`, and a
`DescribeStacks` in one Region answered the other Region's stack. Every operation reads in
the request's scope, the reads the plugin makes on its own behalf included.

A state snapshot taken before #1366 (the file and sqlite event stores keep them) holds the
unscoped keys. A read falls back to such a key when the scoped one is absent, for a record
whose own `AccountID` and `Region` place it in the reader's scope (an empty field matches,
as a stack recorded before those fields existed has neither). The first write to such a
stack moves it, with its change sets and drift detections, to its scoped keys, so a
restored snapshot keeps every stack and is migrated by use. A replay of a recorded event
stream needs neither: it re-executes the requests against fresh state, and they write
scoped keys. `TestCFNScope_*` in `emulator/cfn_scope_test.go` asserts the isolation, the
fallback, the migration, and that a store fault on any of them is an error.

## Stacks share state with every other plugin

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

### A stack deploys into the calling account and region

Most plugins scope a resource to the account, and some also to the region, of the
request that created it. A stack's resources are created under the identity of the
caller that created the stack, so they are visible to that caller's reads and no
one else's:

```
# unsigned, so the caller is the default account — 123456789012
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
one on the deployer with `emulator.WithDeployerIdentity`. An unsigned wire caller
lands in the same partition — see
[Which account a request is attributed to](/reference/concepts/accounts)
— so a stack deployed in process and one deployed over the wire are visible to each
other without either side configuring anything.

## A stack's resource calls are authorized

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
`AccessDenied`, naming the action and the resource ARN — as that resource's
`ResourceStatusReason` in `DescribeStackResources`, and the stack rolls back:

```
aws cloudformation create-stack --stack-name s --template-body file://bucket.json \
  --role-arn arn:aws:iam::123456789012:role/narrow
aws cloudformation describe-stacks --stack-name s \
  --query 'Stacks[0].[StackStatus,RoleARN]'
# ROLLBACK_COMPLETE   arn:aws:iam::123456789012:role/narrow
aws cloudformation describe-stack-resources --stack-name s \
  --query 'StackResources[0].ResourceStatusReason'
# AccessDenied: User: arn:aws:iam::123456789012:role/narrow is not
# authorized to perform: s3:CreateBucket on resource: arn:aws:s3:::...
```

The denial is also reported as a `StackEvent`, which is where a deployment wrapper
conventionally reads a CloudFormation failure from:

```bash
aws cloudformation describe-stack-events --stack-name s \
  --query 'StackEvents[?ResourceStatus==`CREATE_FAILED`].ResourceStatusReason'
# AccessDenied: User: arn:aws:iam::123456789012:role/narrow is not
# authorized to perform: s3:CreateBucket on resource: arn:aws:s3:::...
```

Both views share one derivation, so they cannot disagree about whether a resource
failed. See "Stack events are derived from the stack record" below for what the
event model does and does not contain.

Enforcement is opt-in by creating the principal: a stack deployed with a credential
that resolves to no IAM user or role in state is not authorized at all, which is
every in-process `emulator.Client` caller and every credential that never touched
IAM. See the testing guide's "Testing IAM permissions".

## A resource with no name in the template gets a per-stack name

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
| `AWS::IAM::ManagedPolicy` | `ManagedPolicyName` | 128 |
| `AWS::IAM::User` | `UserName` | 64 |
| `AWS::IAM::Group` | `GroupName` | 128 |
| `AWS::IAM::Policy` | none — the generated value is the physical ID of the inline policy, whose `PolicyName` is required | 128 |
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

## `PhysicalResourceId` is the stored identifier, and is *not* per type

`Ref` is resolved [per resource type](#what-ref-returns). `PhysicalResourceId` is
not, and the asymmetry is deliberate (#837). The three operations that report it —
`DescribeStackResources`, `DescribeStackEvents` and `DescribeStackResourceDrifts` —
all report the identifier substrate stored when it deployed the resource: a bucket or
role name, a VPC or instance ID, a generated name from the table above. Nothing is
derived at render time.

The question this settles was raised by #827, which made `Ref` per-type and scoped
`PhysicalResourceId` out. The natural inference — `Ref`'s documented value differs by
type, so `PhysicalResourceId`'s probably does too — **does not survive checking the
API reference**, and that is the whole of the reason:

- `API_StackResource` publishes **one** description for every type: *"The name or
  unique identifier that corresponds to a physical instance ID of a resource
  supported by CloudFormation."* `Type: String`, `Required: No`. There is no per-type
  `PhysicalResourceId` documented anywhere in the API reference, the way each type's
  Template Reference page documents its own `Ref`.
- The one type AWS names explicitly it names on `API_DescribeStackResources`: *"for an
  Amazon Elastic Compute Cloud (EC2) instance, `PhysicalResourceId` corresponds to
  the `InstanceId`."* That is a name-or-ID, and it is what substrate already stores.
- The only worked values AWS publishes are that page's own sample response —
  `MyStack_DB1` and `MyStack_ASG1`. Generated **names**, not ARNs.

So a per-type `PhysicalResourceId` would be substrate inventing a divergence AWS does
not publish, which is the opposite of what #827 did: `Ref` was made per-type *because*
AWS documents it per type. **A per-type audit is therefore not deferred — it has no
citable source to be conducted against**, and that is recorded here rather than left
as an open question.

The round trip AWS describes does work: the value the describes report is accepted
back by `DescribeStackResources`' own `PhysicalResourceId` selector, which is the use
the page puts it to (*"You can pass the EC2 `InstanceId` to `DescribeStackResources`
to find which stack the instance belongs to"*). Naming both it and `StackName` in one
request is a `ValidationError`, as published. `DescribeStackResource` (singular) and
`ListStackResources` are **not implemented**.

Two arguments for keeping the stored value were made when this was filed and are
corrected here rather than repeated, because both were checkable and neither held
(#819):

- **A redeploy does not recognise a resource by its physical ID.** It matches on
  logical ID against the previous record, and the physical ID cannot be the key
  because a refused create returns none at all.
- **Not *every* tag state key is composed from the physical ID.** The four ELBv2
  types find their record by ARN and bypass the stamp's target resolver entirely.

What does still hold is that most `aws:cloudformation:*` tag state keys *are* composed
from it, which is why the stored value is the one thing a later change must leave
alone: derive at render time if a type ever needs a different reported value, and the
safety rule is that a stored identifier may change only if no tag state key is
composed from it and it is stable across a redeploy.

## DeleteStack deletes the stack's resources

`DeleteStack` sweeps the resources the stack deployed before removing the stack
record: a bucket a stack created is gone once its stack is, and `s3api head-bucket`
answers 404.

The sweep is the exact inverse of the deploy's [dependency order](#deploy-order-follows-references-and-dependson),
so a resource is deleted before whatever it depends on — "Resource A is deleted before
resource B" when A references B. For four independent resources the order is the
tie-break order reversed: the recorded event sequence for a stack of an IAM role, a
bucket, a queue and a topic reads `CreateRole, CreateBucket, CreateQueue, CreateTopic`
and then `DeleteTopic, DeleteQueue, DeleteBucket, DeleteRole`. A topic that a role
references is created first and deleted last, whatever the two types' priorities say.
The order is recomputed from the stack's template rather than read off its record, so a
stack recorded before [#1420](https://github.com/scttfrdmn/substrate/issues/1420) also
sweeps in dependency order.

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
modelled for any of the eight Snapshot-capable types. `UpdateReplacePolicy` governs
the delete of a resource an update replaced; see [What an update does with each
resource](#what-an-update-does-with-each-resource).

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

### A resource whose delete needs a detach first gets one

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

Coverage is stated rather than implied. Every resource type the deployer
dispatches has a delete request, or is a state-only type whose stub record is
removed (`AWS::CDK::Metadata` among them), or sweeps to a no-op reported as
`DELETE_SKIPPED` naming the reason:

| Type | Why the sweep is a no-op |
|---|---|
| `AWS::CloudFront::CloudFrontOriginAccessIdentity` | the ID is derived rather than registered with CloudFront, so the deploy records no state to remove |
| `AWS::ECS::CapacityProvider` | the deploy records no state to remove |
| `AWS::SSM::Association` | the deploy records no state to remove |
| `AWS::SecretsManager::SecretTargetAttachment` | the deploy records no state to remove |
| `AWS::ApiGateway::UsagePlanKey` | `DeleteUsagePlanKey` is not routed; the key goes with its usage plan |
| `AWS::Cognito::IdentityPoolRoleAttachment` | Cognito models no `DeleteIdentityPoolRoles` |
| `AWS::SecretsManager::RotationSchedule` | `CancelRotateSecret` is not routed; the schedule goes with the secret |

A type the deployer does not recognize at all is also `DELETE_SKIPPED`: its stub
state is removed, but substrate never created a resource for it, so reporting
`DELETE_COMPLETE` would claim a deletion that did not happen. A skip is always
reported — a claim of cleanliness that is not true is worse than a stated gap.

## A refused resource reports CREATE_FAILED

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

An Output under a `Condition` that is false is not created: it is neither resolved nor
validated, and the stack publishes no value for it.

An **Output that does not resolve fails the stack**, as a resource that does not
resolve fails itself ([#1429](https://github.com/scttfrdmn/substrate/issues/1429)):
an `Fn::ImportValue` naming no export, an `Fn::FindInMap` naming no entry, or an
`Fn::Cidr` out of range rolls a create back and an update back, with a status reason
beginning `Output {Key}:`. That text is substrate's own. Before, the Output answered an
empty string, and an exported one handed an importer an empty ID. An Output that reads,
through `Fn::GetAtt` or an `Fn::Sub` `${Resource.Attribute}`, an attribute substrate
holds no value for — such as a Transfer server's `As2ServiceManagedEgressIpAddresses` —
is refused at the request instead, before anything deploys: `CreateStack`, `UpdateStack`
and `CreateChangeSet` answer `ValidationError`/400, `Template error: resource {LogicalId}
does not support attribute type {Attribute} in Fn::GetAtt`. No AWS page publishes that
wording; it is the `ValidationError` text user reports quote from `CreateStack`. A
resource *property* reading the same attribute still fails only that resource.

Two divergences are substrate's own, both deliberate:

- Substrate deploys the resources declared **after** the failure, where real
  CloudFormation stops at the first one, so a single deploy reports every refusal a
  template contains rather than only the first. The stack status is the same either
  way, and the status is what a caller keys off. Under `DO_NOTHING` those later
  resources are therefore left in place too.
- A failed `UpdateStack` reports `UPDATE_ROLLBACK_COMPLETE` by converging on the
  **record the update started from**. A resource the update kept is untouched; one it
  changed in place has the previous declaration applied again; one it created — an
  addition, or the replacement for a resource whose change required one — is
  deleted, and the resource it replaced, which a successful update would only then
  have deleted, is the stack's again under its original physical ID. A resource changed
  in place is restored by the difference between the failed update's declaration and
  the previous one, in both directions, so a property the failed update newly declared
  returns to the value an undeclared one has, by the type's rule for a removed
  property. An in-place update refused part-way — a security group whose revoke went
  through and whose authorize did not — is restored the same way. A previous
  record that cannot be read leaves the stack at `UPDATE_FAILED` with the reason
  logged rather than a rollback attempted against nothing.

A refused resource's follow-up configuration requests are not sent either. A bucket
whose name S3 rejected has no `PUT ?versioning` issued against it, so the event log
holds only the request a real client would have made.

## What an update does with each resource

`UpdateStack` (and `ExecuteChangeSet`, which routes through it) compares each
resource with the stack's previous deployment and does what the resource's "Update
requires" lines say ([#1427](https://github.com/scttfrdmn/substrate/issues/1427)).
Until #1427 an update re-issued every resource's create, so a type whose create
mints its identifier — a Transfer server, an FSx file system, a KMS key, a
certificate, an internet gateway — came out of every update as a second resource with
a new ID, and the first stayed behind.

- **Unchanged.** A resource whose declaration resolves to the same values it resolved
  to last time keeps its physical resource, and no call is made for it. The comparison
  is on **resolved** values, property by property, so a resource whose text is
  unchanged but which references a parameter whose value changed, or a sibling that was
  replaced and so has a new ID, is treated as changed; and since the order is the
  dependency order, every dependency has been decided first.
- **Changed, and the type's update is modeled.** The change goes through the service's
  own update operation and the physical ID stands. The types are
  `AWS::Transfer::Server` (`UpdateServer`), `AWS::Backup::BackupPlan`
  (`UpdateBackupPlan`), `AWS::EC2::LaunchTemplate` (`CreateLaunchTemplateVersion`),
  and as of [#1545](https://github.com/scttfrdmn/substrate/issues/1545):

  | Type | Update operations | Properties applied |
  |------|-------------------|--------------------|
  | `AWS::Lambda::Function` | `UpdateFunctionConfiguration`, then `UpdateFunctionCode` | `Runtime`, `Role`, `Handler`, `Description`, `Timeout`, `MemorySize`, `Environment`; `Code` |
  | `AWS::DynamoDB::Table` | `UpdateTable`, `UpdateTimeToLive` | `BillingMode`, `ProvisionedThroughput`; `TimeToLiveSpecification` |
  | `AWS::SQS::Queue` | `SetQueueAttributes` | `VisibilityTimeout`, `MessageRetentionPeriod`, `DelaySeconds`, `ReceiveMessageWaitTimeSeconds`, `MaximumMessageSize` |
  | `AWS::SNS::Topic` | `SetTopicAttributes` | `DisplayName` |
  | `AWS::S3::Bucket` | `PUT ?versioning`, `PUT`/`DELETE ?encryption`, `PUT`/`DELETE ?cors`, `PUT`/`DELETE ?lifecycle`, `PUT`/`DELETE ?publicAccessBlock` | `VersioningConfiguration`, `BucketEncryption`, `CorsConfiguration`, `LifecycleConfiguration`, `PublicAccessBlockConfiguration` |
  | `AWS::S3::BucketPolicy` | `PutBucketPolicy` | `PolicyDocument`; a changed `Bucket` is Replacement |
  | `AWS::KMS::Key` | `UpdateKeyDescription`, `PutKeyPolicy`, `EnableKeyRotation`/`DisableKeyRotation`, `EnableKey`/`DisableKey` | `Description`, `KeyPolicy`, `EnableKeyRotation`, `RotationPeriodInDays`, `Enabled` |
  | `AWS::EC2::SecurityGroup` | `RevokeSecurityGroupIngress`/`Egress`, then `AuthorizeSecurityGroupIngress`/`Egress` | `SecurityGroupIngress`, `SecurityGroupEgress` |
  | `AWS::ECS::Service` | `UpdateService` | `TaskDefinition`, `DesiredCount` |
  | `AWS::IAM::Role` | `UpdateAssumeRolePolicy`, `UpdateRole`, `Attach`/`DetachRolePolicy`, `Put`/`DeleteRolePolicy`, `TagRole`/`UntagRole`, `Put`/`DeleteRolePermissionsBoundary` | `AssumeRolePolicyDocument`; `Description`, `MaxSessionDuration`; `ManagedPolicyArns`; `Policies`; `Tags`; `PermissionsBoundary` |
  | `AWS::IAM::User` | `Add`/`RemoveUserFromGroup`, `Attach`/`DetachUserPolicy`, `Put`/`DeleteUserPolicy`, `TagUser`/`UntagUser`, `Put`/`DeleteUserPermissionsBoundary` | `Groups`, `ManagedPolicyArns`, `Policies`, `Tags`, `PermissionsBoundary` |
  | `AWS::IAM::Group` | `Attach`/`DetachGroupPolicy`, `Put`/`DeleteGroupPolicy` | `ManagedPolicyArns`, `Policies` |
  | `AWS::IAM::Policy` | `Put`/`Delete{Role,User,Group}Policy` | every property |
  | `AWS::IAM::ManagedPolicy` | `CreatePolicyVersion` or `SetDefaultPolicyVersion`, `Attach`/`Detach{Role,User,Group}Policy` | `PolicyDocument`; `Roles`, `Users`, `Groups` |

  The properties applied are the ones the type's create forwards, so a create and an
  update of one declaration agree; a property the create does not forward either — most
  types' `Tags` ([#1572](https://github.com/scttfrdmn/substrate/issues/1572)), a bucket's
  `OwnershipControls` or `NotificationConfiguration` ([#1573](https://github.com/scttfrdmn/substrate/issues/1573)), a
  function's `VpcConfig`, a queue's `RedrivePolicy` — makes no call. A bucket's
  `LifecycleConfiguration` and `PublicAccessBlockConfiguration` joined the applied set in
  v0.131.0 ([#1606](https://github.com/scttfrdmn/substrate/issues/1606)), and like encryption and
  CORS are deleted when the template drops them. The
  creates gained three of these with the change: a function's `Environment`, a bucket's
  `BucketEncryption` and `CorsConfiguration`, and a key's `KeyPolicy`, `Enabled` and
  `EnableKeyRotation` (which used to be sent to `CreateKey`, which has no such member).
  A property removed from the template returns to the value the page publishes for an
  undeclared one — SQS's attribute defaults, a function's 3-second `Timeout` and 128 MB
  `MemorySize`, a table's `PROVISIONED`, a key's default policy, rotation off and
  enabled — or to what a create without it produces: a bucket's encryption and CORS
  configuration are deleted, and a topic's `DisplayName` and a key's `Description` are
  emptied. A removed `VersioningConfiguration` is left as it is, since a versioned
  bucket cannot return to unversioned and the page does not say what CloudFormation does
  instead; a removed function `Description` is left too, since substrate's
  `UpdateFunctionConfiguration` reads an empty one as not sent
  ([#1575](https://github.com/scttfrdmn/substrate/issues/1575)).

  A security group's rules are diffed **declaration against declaration**, one source at
  a time, with CIDRs compared in canonical form: each rule the previous declaration held
  and the new one does not is revoked, then each rule the new one adds is authorized, the
  order the page gives ("CloudFormation removes the existing rule and then adds a new
  rule"). A rule the group holds for another reason — an
  `AWS::EC2::SecurityGroupIngress`/`SecurityGroupEgress` resource, another stack, an
  out-of-band call — is left alone. The live rules are read only to skip a revoke of a
  rule already gone and an authorize of one already there. Declaring egress where none
  was declared revokes the default all-traffic rule, as the create does; a removed
  `SecurityGroupEgress` leaves the group with no egress rule: "After the security group
  is created, if you remove all egress rules that you added, we do not add egress rules,
  so no outbound traffic is allowed." A rollback restoring a declaration with no egress
  restores the default rule. A rule list may hold `Fn::If` elements; one choosing
  `AWS::NoValue` is no rule. This is what an update of a
  default-VPC group whose rules changed needed ([#1550](https://github.com/scttfrdmn/substrate/issues/1550)):
  re-creating it asked for its own name and was refused with `InvalidGroup.Duplicate`.
- **Changed, and a changed property is *Update requires: Replacement*.** A new resource
  is created; the references to it resolve to the new ID; and once the whole update
  has succeeded the old one is deleted — "CloudFormation creates the replacement
  resource first … By default, CloudFormation then deletes the old resource." Its
  `UpdateReplacePolicy` governs that delete, so `Retain` keeps it. The replacement
  table covers the properties each modeled type's page marks Replacement. A
  replacement whose name substrate generates gets a **new** generated name, as AWS's
  does, with a suffix minted from the request's ID so a replay mints the same one; the
  derived name belongs to the resource being replaced, which still exists, so reusing
  it would collide. Only such a replacement draws from the request's mint, so a
  template-named replacement leaves every later identifier where it was. The name a
  resource is first created under is unchanged.
- **Changed, and AWS updates the property in place through an operation substrate does
  not route.** `AWS::FSx::FileSystem` and `AWS::MSK::Cluster` fail the resource with a
  reason naming the property, and the update rolls back. So do `AWS::DynamoDB::Table`'s
  `KeySchema`, `AttributeDefinitions`, `GlobalSecondaryIndexes`, `LocalSecondaryIndexes`
  and `StreamSpecification` ([#1574](https://github.com/scttfrdmn/substrate/issues/1574)),
  and `AWS::ECS::Service`'s `LaunchType`, and a `Path` that differs from an `AWS::IAM::User`'s
  or `AWS::IAM::Group`'s own, which needs `UpdateUser` or `UpdateGroup`
  ([#1582](https://github.com/scttfrdmn/substrate/issues/1582)). A new resource would be a
  new ID where AWS keeps the old one.
- **Changed, any other type.** The resource is created again, which is what every
  update did before; when that yields a new physical ID, the old resource is deleted
  after the update succeeds, as a replacement is. Which types still re-create on
  change rather than update in place is tracked in
  [#1565](https://github.com/scttfrdmn/substrate/issues/1565).
- **Removed from the template**, or declared under a `Condition` that is now false.
  The resource is deleted once the update has succeeded
  ([#1547](https://github.com/scttfrdmn/substrate/issues/1547)), under its
  **`DeletionPolicy`** — the `UpdateReplacePolicy` page: "Use DeletionPolicy for
  resources deleted when a stack is deleted, or when the resource definition itself is
  deleted from the template as part of a stack update" — so `Retain` and
  `RetainExceptOnCreate` keep it, outside the stack. Removed and replaced resources go in
  one sweep, in the reverse of the previous template's dependency order. A failed update
  deletes none of them: its rollback re-deploys the previous template, which still
  declares them.

A delete of a replaced or removed resource that fails does not fail the update. The stack
still reports `UPDATE_COMPLETE`, with a status reason naming the resource that could not be
deleted, which is what the troubleshooting guide's "Resource removed from stack but not
deleted" describes; the per-resource outcome is on the result's `ResourceDeletions`.

A re-created resource whose plugin refuses it as already existing **fails**, and the
update rolls back — including a fixed-name resource whose text is unchanged but whose
resolved inputs changed, such as a named Secrets Manager secret whose `Description` reads
the ID of a VPC the update re-creates. Keeping the secret's previous record would leave it
naming the VPC the update is about to delete. Such a type needs an update path (#1565). The one
refusal that is not a failure is a rollback applying a resource's previous declaration
again, when the resolved inputs are identical to that deployment's.

A change to a property the resource's page says no update can change fails the resource
without a call, and the update rolls back, leaving it as it was.
`AWS::Transfer::Server`'s `CreateWithHostKey` is one: "If you change its value in a later
stack update, the update fails and the server is left unchanged." `AWS::KMS::Key`'s
`KeySpec`, `KeyUsage` and `MultiRegion` are the others — Replacement on the page, which a
change set reports, and then: "the update request fails, regardless of the value of the
UpdateReplacePolicy attribute. This prevents you from accidentally deleting a KMS key by
changing an immutable property value" — and so is its `Origin` ("You cannot change the
origin after you create the KMS key"). The reason text is substrate's own.

`UpdateReplacePolicy` must be `Delete`, `Retain` or `Snapshot`, the three options its
page lists. Any other value refuses the template at the request, as a `ValidationError`
with a template format error whose wording is substrate's own. `RetainExceptOnCreate`
is a `DeletionPolicy` value only. `DeletionPolicy` itself is still checked when the
sweep reads it.

An update is refused outright for a stack whose status admits none, as the status codes
page states: `ROLLBACK_COMPLETE` ("only a delete operation can be performed"),
`DELETE_FAILED` ("you can't work with or update the stack") and `ROLLBACK_FAILED`
("Delete the stack"). `UpdateStack` and `CreateChangeSet` answer `ValidationError`/400,
`Stack:{arn} is in ROLLBACK_COMPLETE state and can not be updated.` That text is what
CLI users report; no AWS page publishes it.

A deployment never reuses a resource a sweep removed. A stack record keeps its resource
list after a rollback or a failed delete, so the next deployment reads the record's
deletions: everything is gone after `ROLLBACK_COMPLETE`, and after `DELETE_FAILED` only
the resource whose delete failed remains. Each deletion records the physical ID it
removed, so the deleted predecessor of a replaced resource is not mistaken for the
resource that replaced it.

>>>

## Templates

114 resource types are supported; each service section below lists the types it
backs under **CloudFormation resource types**. A template body may be JSON or YAML.

### Deploy order follows references and `DependsOn`

A stack's resources deploy in **dependency order**
([#1420](https://github.com/scttfrdmn/substrate/issues/1420)). The `DependsOn` attribute
page states the rule: a resource that references another "in the form of target
properties `!Ref`, `!GetAtt`, and `!Sub`" is created after it, updated after it and
deleted before it, and `DependsOn` — "a single string or list of strings" — adds an edge
of the same kind. Every form counts: `Ref`, both forms of `Fn::GetAtt` (one whose
attribute name is itself an intrinsic still depends on its resource), and both forms of
`Fn::Sub`, including a `${Logical.Attribute}` inside the string. In the two-argument
`Fn::Sub`, a `${Name}` that is a key of the variable map is that variable, not a
reference to a resource of the same name. AWS Config's
recorder, delivery channel and rule carry the order their pages state without a
`DependsOn`: a channel follows the stack's recorder, and a rule follows both.

Until #1420 the order was the deployer's type priority and then the logical ID, and
`DependsOn` was parsed and never read. `AWS::IAM::Policy`, the one type at priority 0,
deployed before everything, so an `Fn::GetAtt` in its `PolicyDocument` to a queue, a
bucket or a table — the grant CDK emits — found nothing deployed and stored the literal
`Queue.Arn`. The priorities now only break ties between resources that are ready at the
same time, and a template whose references they already agreed with deploys exactly as
it did before.

A reference to a resource skipped by a false `Condition` is not an edge: that resource
does not deploy, and a `Ref` to it inside an `Fn::If` branch the condition does not take
deploys. A `DependsOn` naming one is refused, like an undeclared name. That is observed
behaviour — user reports trace the "Unresolved resource dependencies" refusal to such a
`DependsOn` — not a published rule.

A rollback re-deploys the stack's previous template without these refusals, ordering what
it can and placing the rest last, as a `DeleteStack` sweep does: a stack recorded before
#1420 with a `DependsOn` the validation would now refuse can still roll back.

Two templates are refused at the **request**, before anything deploys or any stack
record is written, as CloudFormation refuses them — a `CreateStack` leaves no stack, an
`UpdateStack` leaves the stack as it was, and a `CreateChangeSet` records nothing. Both
are `ValidationError` at 400, with AWS's message:

| Template | Message |
|---|---|
| A `DependsOn` naming a resource the template does not declare, or declares under a false `Condition` | `Template format error: Unresolved resource dependencies [Ghost] in the Resources block of the template` |
| Resources that depend on each other, directly or through others | `Circular dependency between resources: [A, B]` |

The first is quoted by the AWS re:Post Knowledge Center article on template validation
errors; the second by the AWS CDK reference, and CLI reports show it as the
`ValidationError` `CreateStack` and `CreateChangeSet` answer. The circular message lists
every resource that cannot be ordered — those on the cycle and those depending on one —
sorted; which resources AWS lists is not published, and that choice is substrate's.
A `Ref` to a name the template declares nowhere is not yet refused this way; see
[#1548](https://github.com/scttfrdmn/substrate/issues/1548).

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

### What `Ref` returns

**`Ref` is resolved per resource type.** CloudFormation does not return one kind of
value: each type's Template Reference "Return values" section documents its own,
which is an ARN for some types, a name for others, a service-assigned ID for others
and a URL for one. `Ref` used to answer the resource's physical ID for every type —
correct for the majority, and the wrong *kind of thing* for the rest (#827). Nothing
refuses a wrong-kind value, so a template writing `LoadBalancerArn: !Ref lb` passed a
bare load balancer name to `CreateListener` and AWS's own listener shape did not
deploy at all; where the receiving operation was more permissive the wrong value was
simply stored.

For most types the documented value **is** the physical ID — a bucket or role name, a
VPC or instance ID, an SNS topic ARN — and those are unchanged. These are the types
whose `Ref` is something else:

| Resource type | `Ref` returns |
|---|---|
| `AWS::ElasticLoadBalancingV2::LoadBalancer` | the load balancer ARN |
| `AWS::ElasticLoadBalancingV2::TargetGroup` | the target group ARN |
| `AWS::ECS::Service` | the service ARN |
| `AWS::StepFunctions::StateMachine` | the state machine ARN |
| `AWS::StepFunctions::Activity` | the activity ARN |
| `AWS::AppSync::GraphQLApi` | the API ARN |
| `AWS::AppSync::DataSource` | the data source ARN |
| `AWS::AppSync::Resolver` | the resolver ARN |
| `AWS::AppSync::FunctionConfiguration` | the function ARN |
| `AWS::Transfer::Server` | the server ARN |
| `AWS::KMS::Key`, `AWS::KMS::ReplicaKey` | the key **ID**, not the key ARN |
| `AWS::EC2::EIP` | the Elastic **IP address**, not the allocation ID |
| `AWS::CloudTrail::Trail` | the trail **name**, not the trail ARN |
| `AWS::SQS::Queue` | the queue **URL** |
| `AWS::WAFv2::WebACL` | `name\|id\|scope`, as the page's own example spells it |
| `AWS::ApiGateway::UsagePlanKey` | `keyId:usagePlanId` |
| `AWS::Route53::HostedZone` | the bare hosted zone **ID**, such as `Z23ABC4XYZL05B`, not the API's `/hostedzone/Z…` path form |

A hosted zone's `Ref`, and its `Fn::GetAtt Id`, are the bare ID the CloudFormation page publishes ("`Ref` returns the
hosted zone ID, such as `Z23ABC4XYZL05B`"), and its ARN is the `arn:aws:route53:::hostedzone/<id>` form the same page
quotes. `CreateHostedZone` answers its `Id` member in the path form, `/hostedzone/Z…`, and until
[#1256](https://github.com/scttfrdmn/substrate/issues/1256) that path form was both the `Ref` and the ARN. So a
`RecordSet` naming its zone with `{"Ref": "MyZone"}`, as AWS's own examples do, built
`/2013-04-01/hostedzone//hostedzone/Z…/rrset` and was refused, and a stack delete sent the same doubled path.
`Fn::GetAtt NameServers` is not recorded and resolves empty.

An `AWS::Route53::RecordSetGroup`'s `HostedZoneId` is declared on the group for every record set in it, and is
passed to each record set that does not name its own; each was deployed with its own properties alone and named
no zone. A refused record set fails the group, with the record set's refusal as the group's own; it used to be
discarded, so a group whose every record set was refused reported a clean resource.

The Route 53 plugin is deliberately unchanged: its `ChangeResourceRecordSets` route takes the bare ID in the path,
and a path carrying `/hostedzone/` a second time is not routed (`InvalidAction`). The SDKs strip that prefix from a
zone ID before building the path (botocore's Route 53 ID handler, for one), so a doubled path is only ever a client's
own construction, as it was the deployer's. Whether `Ref` answers the bare ID is a separate question, and it is now
answered as published. The plugin does not yet refuse a record set for a zone that does not exist.

A queue's `Ref` is the URL substrate's own SQS operations answer with and accept —
`http://sqs.{region}.localhost/{account}/{name}` rather than AWS's
`https://sqs.{region}.amazonaws.com/...` — because a URL the emulator would then
reject is useless to the caller who resolved it. Both come from one function, so the
value a template resolves and the value `CreateQueue` returns cannot drift.

Every `arn:aws:wafv2` ARN — a web ACL's or an IP set's, from the WAFv2 API or from
CloudFormation — comes from one builder, for the same reason. The CloudFormation path
derived the ARN's scope segment from the `Scope` property while the plugin hardcoded
`regional`, so one logical web ACL reported two different ARNs depending on which path
created it, and a `CLOUDFRONT` resource created through the API named a scope it did
not have. An IP-set ARN is what an `IPSetReferenceStatement` takes and a web-ACL ARN is
what `AssociateWebACL` takes, so that is an identifier another operation is expected to
accept. The segment is the lowercase of `Scope`, which is **substrate's reading**: AWS
publishes exactly one substituted web-ACL ARN anywhere, on the `AWS::WAFv2::WebACL`
Template Reference page, and it is a `REGIONAL` ACL rendering `regional`. What one
builder guarantees is that every path agrees; which answer they agree on is not cited
(#858).

`CreateIPSet` refuses an incomplete request rather than inventing one. All four of
`Addresses`, `IPAddressVersion`, `Name` and `Scope` are documented `Required: Yes`, and
substrate defaulted three of them — so a request AWS rejects created an IP set whose
scope and address family the caller never chose, and then reported them back as though
it had asked. An **omitted** required member answers `ValidationError`/400 (WAFv2's
`CommonErrors`: "Check that all required parameters are included and that values are
valid"); a **present-but-invalid value** answers `WAFInvalidParameterException`/400,
which the operation's own Errors section lists. `"Addresses": []` still **succeeds**,
because AWS lists it among the operation's valid specifications while marking
`"Addresses": [""]` INVALID — so an empty list and an absent member stay distinct, and
the `nil`-to-`[]` normalisation had to go rather than sit beside the refusal (#755).

A listener's `DefaultActions` and a listener rule's `Actions` are now **forwarded** to
`CreateListener` and `CreateRule`. They were dropped, so the resolved target group
never reached the listener and `DescribeListeners` reported a listener with no default
action; forwarding them is what makes a resolved `!Ref` on a target group observable
through an API call rather than only through a stack `Output`.

An `AWS::ApiGateway::Method`'s physical ID is a **generated method ID**,
`{stack}-{logical}-{suffix}`, which is the shape AWS's own Template Reference example
for the type has (`mysta-metho-01234b567890example`). It was the HTTP verb, so two
methods with the same verb on different resources of one API were indistinguishable —
and because a `PhysicalResourceId` lookup scans every stack in the account, so was
every `GET` method everywhere. The type has no name property, so unlike every other
generated name the bound comes from that published example rather than from a service
limit; API Gateway's REST API publishes no method identifier at all, which is why the
identifier exists only in the CloudFormation layer and the plugin is unchanged — a
method is still addressed by its verb in API Gateway's own state, and the deleter
resolves the verb from the template's properties, the same channel it already used for
the method's two parents (#843).

An `AWS::CloudFront::CloudFrontOriginAccessIdentity`'s physical ID is a **derived OAI
ID** — `E` followed by 13 uppercase alphanumerics — computed by SHA-256 over the
account, the Region, the stack name and the logical ID. It was the resource's own
**logical ID**, so `!Ref Oai` handed an S3 bucket policy or an origin-access
configuration a value AWS would never mint. That was not a stub left in place: the
deploy asked for `CloudFrontOriginAccessIdentityConfig.Comment`, a **dotted key read
from a flat map**, so the lookup matched nothing and the fallback — the logical ID —
won unconditionally (#877). The shape is **AWS's**, published twice by example
(`E15MNIMTCFKK4C` for `Ref`, `E74FTE3AJFJ256A` for `Fn::GetAtt Id`); the
**derivation is substrate's reading**, and it is derived rather than random because
`UpdateStack` re-deploys the whole template, so a `crypto/rand` ID would change on
every update and every policy naming the old one would silently stop matching. The
type still dispatches nothing and still sweeps to a no-op on delete: the identity is
derived, not registered with CloudFront (#859).

**Four other property lookups read a dotted key from a flat map, and all four took
their fallback unconditionally** (#877). A Glue job's `Command.Name` and
`Command.ScriptLocation` are nested object members, so every job created through
CloudFormation shipped an **empty `ScriptLocation`** — the property that says what the
job runs — and a `pythonshell` job was created as a Spark ETL one. An ECS task
definition's `RequiresCompatibilities.0` is a **list element**, so every task
definition registered as `EC2` and a Fargate one was never Fargate. A distribution's
`DistributionConfig.Comment` stored the logical ID; that one is cosmetic, because the
physical ID is recovered from `CreateDistribution`'s response, but a comment reporting
a logical ID is still a value from somewhere other than the template. Each site reads
its own nested member or list element rather than the shared helper learning to split
a dotted key, because a nested read and a list-indexed read are different operations
and a shared walk would have turned all five constants into template-controlled values
at once, as a side effect of a refactor rather than as a decision per site. An
AST tripwire refuses a sixth.

**When a type's documented value cannot be built, `Ref` resolves to empty** rather
than falling back to the physical ID. An empty value means the deploy did not yield
the source the value is derived from, which happens only for a resource that failed
and is about to be reported `CREATE_FAILED`; a plausible-looking wrong answer is worse
than an obviously missing one, since a stack `Output` carrying it would be asserted
against happily.

These divergences are **recorded rather than fixed**, because the value AWS documents
does not exist in substrate to return:

- `AWS::SNS::Subscription` — AWS documents "`Ref` returns the subscription's logical
  name". Substrate returns the subscription ARN, deliberately: the logical name is a
  value the template already has, and the ARN is the one an `Unsubscribe` takes.
- `AWS::EC2::SecurityGroupIngress` and `::SecurityGroupEgress` — no per-rule identity
  exists, and `Ref` on the ingress type is not documented.
- `AWS::EC2::SecurityGroup` — AWS returns the group **name** for a group created
  without a `VpcId` and the group ID otherwise. Substrate always returns the ID.
- `AWS::SecretsManager::RotationSchedule` and `::SecretTargetAttachment` —
  conditionally or self-consistently correct as they stand. (`AWS::Backup::BackupPlan`
  was listed here, and its `Ref` was the logical ID; since #1182 it is the plan ID.)

### What `Fn::GetAtt` returns

**`Fn::GetAtt` keys on the resource type *and* the attribute name.** It used to key on
the attribute name alone, which is the mirror image of the `Ref` defect above: an
attribute name two services share resolved with the wrong service's rule, and everything
unrecognised fell through to the physical ID (#827). `!GetAtt bucket.Arn` answered the
bucket **name**, because a bucket had no recorded ARN and the `Arn` arm fell back;
`!GetAtt bucket.DomainName` answered the name too, because `DomainName` was
CloudFront's arm; `!GetAtt param.Value` answered an SSM parameter's **name** where AWS
documents "returns the value of the parameter"; and `!GetAtt lb.DNSName` had no arm at
all.

**An attribute the resolver cannot answer resolves to empty**, never to the physical ID.
This generalises the rule the `AWS::Config::ConfigRule` arm already stated to the whole
resolver, for its reason: a bare name where an ARN, a URL or a stored value belongs is a
plausible-looking wrong answer, which a stack `Output` carries and a test asserts against
happily, while an empty string is distinguishable from a real value. So an attribute AWS
documents but substrate models nothing for — an ELBv2 load balancer's
`CanonicalHostedZoneID`, a DynamoDB table's `StreamArn` when the table has no stream —
reads as empty rather than as something plausible.

Resolution answers only from a fact, in three steps:

1. **A per-type rule**, for the attributes whose value is specific to the type.
2. **The attribute's own name in the resource's recorded metadata**, which is the channel
   every deploy helper already records a resolvable attribute through — `RepositoryUri`,
   `Endpoint.Address`, `InvokeURL`, `ProviderName`, `AllocationId`, `ConfigRuleId`.
3. **The resource's own ARN, when the attribute's name says it returns one** — a check on
   the attribute's spelling rather than a table of 113 resource types, which is why
   `LoadBalancerArn`, `ListenerArn`, `RuleArn`, `TopicArn`, `TaskDefinitionArn`,
   `ServiceArn` and FSx's `ResourceARN` all resolve without a rule each. A plural
   `…Arns` is a list, not a string, and is not matched; neither is a **dotted** name,
   because a nested path names a *member's* ARN rather than the resource's — an RDS
   instance's `MasterUserSecret.SecretArn` is the secret's.

The per-type rules, each carrying the value the type's own "Return values" section
documents:

| Resource type | Attributes |
|---|---|
| `AWS::S3::Bucket` | `DomainName`, `RegionalDomainName`, `DualStackDomainName`, `WebsiteURL` — built from the bucket name and the region, in AWS's own forms |
| `AWS::Logs::LogGroup` | `Arn` — with the trailing `:*` AWS's example carries, built by the same function the Logs API's own `arn` member uses |
| `AWS::ElasticLoadBalancingV2::LoadBalancer` | `LoadBalancerName`, `LoadBalancerFullName` (`app/name/id`) |
| `AWS::ElasticLoadBalancingV2::TargetGroup` | `TargetGroupName`, `TargetGroupFullName` (`targetgroup/name/id`) |
| `AWS::SNS::Topic` | `TopicName` — off the end of the ARN, since a topic's physical ID *is* its ARN |
| `AWS::SQS::Queue` | `QueueName`, `QueueUrl` — the same URL `Ref` resolves to, from the same builder |
| `AWS::SecretsManager::Secret` | `Id` — which for this type is the ARN, not an opaque identifier |
| `AWS::CloudFront::CloudFrontOriginAccessIdentity` | `Id` — the derived OAI ID, which is also what `Ref` returns; `S3CanonicalUserId` is empty, since AWS publishes no format for it (#859) |
| `AWS::Glue::Database` | `CatalogId` — the deploying account |
| `AWS::ECS::Service`, `AWS::ApiGateway::Stage` | `Name` |
| `AWS::Cognito::UserPool` | `UserPoolId` |
| `AWS::ApiGatewayV2::Api` | `ApiId`, `ApiEndpoint` (`https://{id}.execute-api.{region}.amazonaws.com`) |
| `AWS::AppSync::GraphQLApi` | `ApiId`; `GraphQLEndpointArn` is empty — it is the *endpoint's* ARN, not the API's |
| `AWS::ElastiCache::CacheCluster` | `RedisEndpoint.Address`, `RedisEndpoint.Port` — AWS's documented spelling, translated to the `RedisEndPoint` the ElastiCache API uses |
| `AWS::DynamoDB::Table` | `StreamArn` — empty unless a stream was recorded, so the table's own ARN is not returned in its place |
| `AWS::SSM::Parameter` | `Type` and `Value`, recorded at deploy time |
| `AWS::ElasticLoadBalancing::LoadBalancer` | `DNSName`, recorded at deploy time from the classic create's own response; its four other published attributes answer empty — see [the classic type](/reference/elasticloadbalancing#the-classic-type) |
| `AWS::EC2::SecurityGroup`, `AWS::EC2::VPC`, `AWS::EC2::Subnet`, `AWS::EC2::Instance`, `AWS::EC2::InternetGateway`, `AWS::EC2::RouteTable`, `AWS::EC2::NatGateway` | The ID attribute — `GroupId`, `VpcId`, `SubnetId`, `InstanceId`, `InternetGatewayId`, `RouteTableId`, `NatGatewayId` — which is the physical ID. These resolved to empty until #1545's sweep, so a standalone `AWS::EC2::SecurityGroupIngress` written `GroupId: !GetAtt SG.GroupId`, the form CDK emits, failed with `MissingParameter` |
| `AWS::EC2::SecurityGroup`, `AWS::EC2::VPC`, `AWS::EC2::Subnet`, `AWS::EC2::Instance` | Read from EC2's record once the resource deploys and recorded as metadata: a group's `VpcId` (the default VPC's for a group declared without one); a VPC's `CidrBlock` and `DefaultSecurityGroup`; a subnet's `VpcId`, `CidrBlock`, `AvailabilityZone` and `AvailabilityZoneId`; an instance's `VpcId`, `PrivateIp`, `PrivateDnsName`, `PublicIp` and `PublicDnsName`. A VPC's network-ACL, IPv6 and encryption-control attributes, a subnet's `NetworkAclAssociationId`, `Ipv6CidrBlocks` and `OutpostArn`, and a NAT gateway's `EniId` answer empty ([#1591](https://github.com/scttfrdmn/substrate/issues/1591)) |

An S3 bucket's four domain names and an HTTP API's endpoint name **AWS's** hostnames
rather than substrate's, unlike a queue's `Ref`: they are values a template hands to
another resource — a CloudFront origin, a redirect target — not endpoints the emulator
serves, so there is no request for a substrate-local form to satisfy. Where a value is
an endpoint the caller then *uses*, the emulator's own is returned instead.

An attribute of a resource the template does not declare still resolves to
`{LogicalID}.{Attribute}`, which names the mistake rather than hiding it as an empty
string.

### `Mappings` and `Fn::FindInMap`

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

### `Fn::GetAZs` and `Fn::Cidr`

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

### Cross-stack exports and `Fn::ImportValue`

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

### YAML short forms

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

`Fn::Sub`'s resource-attribute form, `${LogicalId.Attribute}`, answers what
`Fn::GetAtt` answers for the same pair, as the function's reference page states
(#1103). The body is split on its **first** dot only, because an attribute name
may itself hold one — `${Db.Endpoint.Address}`, or `Fn::GetAtt`'s published
`SourceSecurityGroup.OwnerAlias`. A key in `Fn::Sub`'s own variable map still wins,
even when it contains a dot. A dotted name that no declared resource matches
resolves to `LogicalId.Attribute`, as `Fn::GetAtt` does, so the value names the
template's mistake.

**Json-typed properties resolve at depth too** (#1153). A policy document or an
event pattern is resolved at every depth and then marshalled, so an `Fn::GetAtt` in
a statement's `Resource` stores the ARN rather than `{"Fn::GetAtt":[…]}`. The
string form, a JSON document written as a string, is passed through as the document
rather than quoted a second time. An `Fn::If` that chooses a structure — a whole `Condition`
block, or one statement of a `Statement` list, the way the CDK bootstrap template writes its optional
grants — is that structure, resolved in turn; until #1606 it was marshalled into a JSON string, which
IAM refused as `MalformedPolicyDocument`. This covers:

| Resource type | Property |
|---|---|
| `AWS::IAM::Role` | `AssumeRolePolicyDocument` (and its drift comparison) |
| `AWS::IAM::Policy` | `PolicyDocument` |
| `AWS::IAM::ManagedPolicy` | `PolicyDocument` |
| `AWS::IAM::Role`, `AWS::IAM::User`, `AWS::IAM::Group` | each `Policies` member's `PolicyDocument` |
| `AWS::SNS::TopicPolicy` | `PolicyDocument` |
| `AWS::S3::BucketPolicy` | `PolicyDocument` (#1606) |
| `AWS::Events::Rule` | `EventPattern` |
| `AWS::ECR::Repository` | `LifecyclePolicy.LifecyclePolicyText` |
| `AWS::StepFunctions::StateMachine` | `Definition` (#1074) |

A resource can only resolve an attribute of a resource deployed before it. Deploy
order follows the template's references (#1420), so an `Fn::GetAtt` inside an IAM
policy document deploys the policy after the resource it names.

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

## A stack's own tags

`CreateStack`'s and `UpdateStack`'s `Tags.member.N` is recorded on the stack and reported by
`DescribeStacks` ([#764](https://github.com/scttfrdmn/substrate/issues/764)). Before that it
was decoded by nothing at all: a caller sending `--tags Key=team,Value=platform` got a 200
and a stack that reported no tags, so a cost-allocation or policy assertion keyed on a stack
tag had nothing to read.

Tags are reported **sorted by key**, for the same reason parameters and outputs are: a
response whose member order followed Go's map iteration would differ run to run, and an
assertion on it would flake in an emulator whose whole claim is that it does not. A stack with
no tags reports an empty `<Tags></Tags>` — `Parameters` and `Outputs` render the same way, and
every SDK decodes an empty list and an absent one alike.

**The update semantics are AWS's**, and they are the reason a stack's tags are modelled as a
map that can be absent rather than as a list: "If you don't specify this parameter,
CloudFormation doesn't modify the stack's tags. If you specify an empty value, CloudFormation
removes all associated tags."

| `UpdateStack` sends | Result |
|---|---|
| no `Tags` parameter | the stack keeps the tags it had |
| `Tags=` (an empty list) | every tag is removed |
| `Tags.member.N` | the tag set is **replaced wholesale**, not merged |

The empty list is not the same wire request as no parameter, and the distinction is
observable: the query protocol serialises an empty list as a bare `Tags=`, so `--tags '[]'`
arrives with the parameter present and no members, while a request that omits it entirely
arrives without it. Substrate turns on exactly that.

A tag set is validated against the `Tag` data type's own constraints — at most 50 tags ("A
maximum number of 50 tags can be specified"), a key of 1 to 128 characters, a value of 1 to
256 — counted in characters rather than bytes, so a tag in a non-Latin script is not refused
for being multi-byte. Two points where a consumer will otherwise guess wrong:

- **A tag value may not be empty.** The model's minimum is 1 and the member is `Required:
  Yes`, where IAM's and ELBv2's tag values document a minimum of 0 and accept an empty string.
  `Key=team` with no value is therefore refused by CloudFormation and accepted by those two.
  That is AWS's own inconsistency; substrate asserts it rather than smoothing it over.
- **The console page disagrees with the model by one character.** It documents keys "up to 127
  characters" and values "up to 255"; the API model and the Template Reference both say 128 and
  256. The model wins, so a 128-character key the console would reject is accepted here.

No character set is enforced, because the `Tag` model publishes no `Pattern` for either
member — unlike IAM's `Tag` and AWS Config's `TagsList`, whose patterns substrate compiles
verbatim. The Template Reference does list a character set, but for a *template's*
resource-level `Tags` property rather than for the stack-level list an API call carries.

A key carrying the reserved `aws:` prefix is refused, matched **case-insensitively** — so
`AWS:owner` is refused too. That is CloudFormation's own rule, stated outright: "The `aws:`
prefix is reserved for AWS use. This prefix is case-insensitive." EC2, ELBv2 and IAM all match
the prefix case-sensitively, because their pages state no such thing. The prefix is refused in
a *key* only; a value beginning `aws:` is stored, since the only thing AWS says about one is
that "you can't update or delete the tag" — a consequence, not a refusal.

**Provenance for the refusal code.** `ValidationError` at 400 is substrate's choice, not
AWS's: neither `CreateStack`'s nor `UpdateStack`'s Errors list names a code for a bad tag, and
substrate's two existing precedents disagree — EC2 invents `InvalidParameterValue` and ELBv2
accepts silently. `ValidationError` is the code the plugin already answers every other
parameter refusal with, and the refusal is of the whole request: a create with one bad tag
leaves no stack behind, and a refused update changes nothing.

Two tags naming the same key collapse, last one winning. AWS publishes no error code for a
duplicate key, and the tag set is a mapping from key to value, so there is nothing a second
entry could mean other than an overwrite.

An in-process `Client` deploy is held to the same limits as an HTTP request, because both funnel
through the deployer's options, where the validation lives.

### A stack tag reaches the resources the stack creates

AWS says so on the `Tags` member itself — "CloudFormation also propagates these tags to the
resources created in the stack" — and it is the half a policy or a cost report reads: a stack tag
only `DescribeStacks` reports cannot be the subject of an `aws:ResourceTag` condition on anything
the stack built. A stack tag therefore lands on the same resources the three
`aws:cloudformation:*` keys do, through the same per-service tag stores, and is readable through
each service's own tag call — see [what the stamp
reaches](/reference/ec2#cloudformation-stamps-its-own-tags-on-the-resources-it-creates) for the table of
services and the named list of what is skipped. A resource whose service models no tags is
skipped silently here too, with no log line.

**A propagation that fails is reported, and is not a skip.** A write that finds the resource and then fails
(a state read, a state write, a marshal) is logged and recorded on `DeployResult.TagPropagationFailures`: the
resource's logical ID, physical ID, type and the error. Until
[#1138](https://github.com/scttfrdmn/substrate/issues/1138) the log line was its only report, so the stack and the
resource both reported success and the resource silently lacked the stack's tags. Three properties of the reading:
- **It is substrate's own.** Real CloudFormation writes a resource's tags in its own create or update call, so a tag
  failure there is a resource failure; "created, but its tags could not be written" is a state it cannot be in.
  Substrate propagates in a pass after the resources deploy, which is what makes the state possible.
- **It is reported in process,** to a `Client` or `StackDeployer` caller, and invents no wire observation. No stack
  event or status carries it.
- **It is not `DeployedResource.Error`.** A non-empty `Error` makes `DescribeStackResources` and
  `DescribeStackEvents` report `CREATE_FAILED` for a resource that in fact created, and takes the stack's status
  with it. The resource and the stack keep the status they earned.

**Whose tag a key is** is the question propagation actually has to answer, because two of AWS's
rules pull against each other: a tag the caller set directly on a resource must survive
propagation, and a stack tag whose value the caller *changes on the stack* must reach the
resource. Neither is decidable from the new tag set alone. What decides it is the stack's
**previous** tag set, which substrate already stores:

| The resource's stored value for the key | What propagation does |
|---|---|
| the key is absent | write it |
| equal to what the stack carried before | overwrite it — the stack's own copy |
| anything else | leave it alone — the caller's |
| the key left the stack, value still matches | remove it |
| the key left the stack, value differs | leave it alone |

That needs no per-resource bookkeeping and can never delete a key the stack did not propagate.
The one case it cannot distinguish is a caller who sets a resource tag to the **same value** the
stack propagates: removing the stack tag then removes theirs too. Substrate records that rather
than papering over it — the alternative is tracking, per resource, which keys the deployer wrote,
which is real state in the event stream for an ambiguity AWS does not resolve either, since a
propagated tag on a real resource carries no provenance a caller can read.

Two further consequences worth knowing before writing an assertion:

- **Propagation runs after the whole stack is deployed**, not per resource like the
  `aws:cloudformation:*` stamp. It has to: an update keeps an unchanged resource without a call
  ([#1427](https://github.com/scttfrdmn/substrate/issues/1427)), so the per-resource stamp never
  runs for it, and a tag added to an existing stack must reach exactly those resources. Since a
  kept resource is the same record, a tag the caller set on it directly is read and preserved.
- **A changed resource that is re-created carries the new tags; the one it replaced is deleted.**
  A VPC whose declaration changes is minted afresh (see
  [What an update does with each resource](#what-an-update-does-with-each-resource)); an
  unchanged one keeps its ID and is retagged in place.

### Propagation can leave a resource over its service's tag quota

Four of the services a stack tag reaches publish a per-resource tag quota and
enforce it on their own tagging operations — EC2, ELBv2, IAM and Kinesis, all four
at 50 ([above](/reference/tagging#a-tag-quota-belongs-to-the-service-that-owns-the-resource)).
**Propagation does not check any of them**, so a stack carrying enough tags can
leave a resource holding more tags than its own service would accept. That is
substrate's recorded reading of a case AWS does not publish
([#1077](https://github.com/scttfrdmn/substrate/issues/1077)), not an oversight.

What AWS publishes, and what it does not:

- `CreateStack` publishes exactly four errors — `AlreadyExists`,
  `InsufficientCapabilities`, `LimitExceeded` and `TokenAlreadyExists`, all 400 —
  and **none is about tags**. `LimitExceeded`'s own description scopes it to
  *"the quota for the resource … see CloudFormation quotas"*, and that quotas page
  has **no row for tags at all**.
- The resource-tagging reference publishes only that *"[t]he propagation of
  stack-level tags to resources, including tags with the `aws:` prefix, varies by
  resource type"*. Nothing there, or on the console page, addresses a target whose
  service caps tags below the stack's count.

**What settles it rather than leaving it open is that a refusal would have no
vocabulary.** CloudFormation publishes no tag error, and each service's own
`TagLimitExceeded` / `TooManyTags` / `LimitExceeded` / `LimitExceededException` is
published for *that service's own tagging operation*, not for a CloudFormation
propagation. Borrowing one is the analogy
[#671](https://github.com/scttfrdmn/substrate/issues/671) forbids, and it would make
substrate's deployer fail a template real CloudFormation deploys — a false failure in
a consumer's test, which is the worst answer this emulator can give.

The case is narrower than it sounds, for two published reasons. A caller cannot
supply a reserved key — CloudFormation's `Tag` `Key` publishes *"can't be prefixed
with `aws:`"* — and the three `aws:cloudformation:*` stamp keys do not count toward a
per-resource limit, which EC2's tag restrictions, ELBv2's, Classic ELB's and the
general Tag Editor rule (*"a maximum of 50 **user created** tags"*) all state. Since
a stack is capped at 50 tags and each quota is 50, **a resource carrying no tags of
its own can always take a full stack's worth**. Reaching the overflow needs a
resource tagged independently, through its own service or the tagging API.

Two things worth knowing if you assert on this:

- **Enforcing would be four decisions, not one flag.** Propagation dispatches to four
  arms and only the record-keyed one goes through the shared merge that takes a quota
  mode; the EC2, ELBv2 and AWS Config arms each write through their own service's
  writer and would need their own check.
- **The quota is untouched everywhere AWS publishes it.** A stream the deployer took
  to fifty-eight tags still refuses a fifty-ninth through Kinesis's own
  `AddTagsToStream`, with its published `LimitExceededException` at 400. The
  divergence is about one door, not about the limit.

## Change sets: CREATE, UPDATE and the empty change set

A change set records the template that would be applied and reports the
resource-level changes it implies
([#1606](https://github.com/scttfrdmn/substrate/issues/1606)). Creation and execution are
synchronous, so no change set is observed in `CREATE_PENDING`, `CREATE_IN_PROGRESS` or
`EXECUTE_IN_PROGRESS`.

- **`CREATE`.** *"If you create a change set for a new stack, CloudFormation creates a stack with a
  unique stack ID, but no template or resources. The stack will be in the `REVIEW_IN_PROGRESS` state
  until you execute the change set."* `DescribeStacks` and `ListStacks` report the stack,
  `DescribeStackEvents` reports its one `REVIEW_IN_PROGRESS` event (*User Initiated*), and
  `DeleteStack` removes it with its change sets. Every change is an `Add`, diffed against an empty
  template, and a resource whose `Condition` is false is not listed. For an `UPDATE` too, a resource
  whose `Condition` turns on is an `Add`, and one whose `Condition` turns off is a `Remove`. A second `CREATE` on a stack
  still in review is accepted; one on any other existing stack is refused with `ValidationError`
  *"Stack [{name}] already exists and cannot be created again with the changeSet [{changeSet}]."* —
  observed wording, quoted by `aws/copilot-cli#1376` and SAM CLI reports. Executing it runs the
  create path, so a failed create rolls back, does nothing or deletes the stack as `OnStackFailure`
  or `DisableRollback` say; the stack's `CreationTime` stays when the change set made it, and its
  `REVIEW_IN_PROGRESS` event stays the oldest, through every later update.
- **`UPDATE`**, the default, diffs against the stack's template. A stack in `REVIEW_IN_PROGRESS`, like
  one in `ROLLBACK_COMPLETE`, cannot be updated.
- **`IMPORT`** is refused with `ValidationError`; the message is substrate's own. Importing brings
  existing resources under a stack, which needs a resource-identifier model substrate does not have.
- **An `UPDATE` that changes nothing is `FAILED`**, `ExecutionStatus` `UNAVAILABLE`, with
  `StatusReason` *"The submitted information didn't contain changes. Submit different information
  to create a change set."* — AWS's text, quoted by the AWS Cloud Operations Blog. Nothing to change
  means no resource change, no changed `Outputs`, and a `Tags` parameter that is absent or equal to
  the stack's. The CDK reads this reason as a no-op deploy.
- **Execution.** `ExecuteChangeSet` refuses a change set that is not `AVAILABLE` with
  `InvalidChangeSetStatus` (400; the message is substrate's own). An executed change set remains,
  reporting `EXECUTE_COMPLETE`, or `EXECUTE_FAILED` when the stack ended failed or rolled back, and
  *"CloudFormation deletes all other change sets associated with the stack"*. Before #1606 the
  executed set was deleted too. The change set's `RoleARN` is the role the execution runs as and the
  stack's afterwards. `DisableRollback` applies to a `CREATE` change set; an `UPDATE` that fails rolls
  back, as `UpdateStack` does.
- **Recorded, not enforced.** `Capabilities` is reported back, and `InsufficientCapabilities` is not
  answered. `NotificationARNs`, `RollbackConfiguration`, `IncludeNestedStacks`,
  `ImportExistingResources`, `DeploymentMode`, `DeploymentConfig` and the two client tokens are
  accepted and not read.

### What a change set does with tags

`CreateChangeSet` records its `Tags`, `DescribeChangeSet` reports them, and
`ExecuteChangeSet` applies them to the stack and propagates them to the stack's
resources ([#824](https://github.com/scttfrdmn/substrate/issues/824)). The
parameter was decoded by nothing before, so a change set answered 200 and then
executed as though the caller had asked for no tags — the one hole left when
`CreateStack` and `UpdateStack` gained theirs.

The three-way meaning is the family's, and it is `UpdateStack`'s wording because
that is the operation an execution routes through: *"If you don't specify this
parameter, CloudFormation doesn't modify the stack's tags. If you specify an empty
value, CloudFormation removes all associated tags."* So a change set created
without `Tags` leaves the stack's own tags exactly as they are — which is how every
change set behaved before this — one created with an empty `Tags` clears them, and
one created with tags replaces them wholesale.

**The warrant for applying them on execution is `DescribeChangeSet`'s, not
`ExecuteChangeSet`'s.** `API_ExecuteChangeSet` mentions tags nowhere at all: no
request parameter, no response element, an empty result body. The only statement
AWS makes about what executing a change set does with tags is the description of
the member `DescribeChangeSet` reports — *"if you execute the change set, the tags
that will be associated with the stack"* — and that is the sentence substrate
implements.

Tags are validated at **creation**, against the same limits `CreateStack` is held
to (50 tags, a key of 1–128 characters, a value of 1–256, no case-insensitive
`aws:` key prefix), because `CreateChangeSet` publishes the same constraints. A
change set that could never execute is refused rather than recorded, and the
refusal leaves no change set behind.

`DescribeChangeSet`'s `Tags` member sits after `Parameters`, where `DescribeStacks`
puts its own; AWS's page lists response elements alphabetically, so it does not
settle wire order. A change set with no tags reports an empty `<Tags></Tags>`,
matching the `Parameters` and `Changes` members beside it.

## Stack events are derived from the stack record

`DescribeStackEvents` answers from the stack's recorded state rather than from a
separately maintained event stream, which is what keeps every value in it a real
observation. Four consequences are worth knowing before writing an assertion.

**Resource events are terminal only.** A stack's own events bracket the operation
— an opening `CREATE_IN_PROGRESS` / `UPDATE_IN_PROGRESS` / `DELETE_IN_PROGRESS`
and the terminal status — because the record says the deploy started and how it
finished. Its *resources* get one event each, the terminal one. Substrate deploys
synchronously and never observed a resource mid-create, so emitting a per-resource
`CREATE_IN_PROGRESS` would be inventing an observation. A consumer counting two
events per resource will find one.

**A deploy's events share one timestamp.** They come from the record's creation
and update times, and a synchronous deploy is one instant. Manufacturing an
interval is not available: simulated time advances with wall time, so any spacing
substrate invented would make a test's assertions wall-clock dependent. Order is
carried by position instead — newest first, as the API documents, with the
last-deployed resource above the one before it.

**`EventId` is deterministic.** A resource event's is
`{LogicalId}-{ResourceStatus}-{Timestamp}`, the form real CloudFormation is
observed to return; a stack event's is a UUID derived from the stack ARN, status
and timestamp. So a replayed stack reports byte-identical events.

**Only the most recent operation is reported.** An update overwrites the record's
status and time, so the create that preceded it left nothing to report. And a
stack deleted successfully is removed from the record entirely, so
`DescribeStackEvents` on it reports `ValidationError` — the same answer
`DescribeStacks` gives. A stack whose *delete failed* remains, and its events name
the resource holding it.

Responses paginate at 100 events with a `NextToken`. `DescribeStackEvents` has no
`MaxResults` parameter (the CLI's `--max-items` is applied client-side), so a
`NextToken` loop is the only way to page.

## Stack status is terminal on return

`CreateStack` deploys synchronously and returns with the stack already
`CREATE_COMPLETE`; `UpdateStack` returns `UPDATE_COMPLETE`. There is no
`*_IN_PROGRESS` window, so a `wait stack-create-complete` succeeds immediately
rather than polling. That is the deterministic-clock trade: a stack's observable
state does not depend on how long a test waited.

## Stack and change-set ARNs are deterministic

The UUID in a stack or change-set ARN is derived from the account, region and
name, not from a clock or a PRNG, so the same call produces the same ARN on a
replay. `StackId` is stable across `CreateStack`, `DescribeStacks` and
`ListStacks`.

This was substrate's first derived identifier and is the one the general rule was
modelled on; see [An identifier a replay mints is the one it
recorded](/reference/concepts/seeds-and-replay#an-identifier-a-replay-mints-is-the-one-it-recorded).

The **physical** IDs of the resources a stack deploys are derived too, and were
not until #856's dispatch fix: the deployer turns each resource into an internal
request, and those requests carried no mint, so a replayed stack reported
different `PhysicalResourceId` values and a different `state_hash_after` for
`CreateStack`. The internal request IDs now derive from the stack request's own
ID, in the order a deploy already uses — dependency order, ties broken by type
priority and then logical ID — so a replayed `CreateStack` re-runs the deployment
and arrives at the same IDs.

## The account, Region and dates a stack carries reach no response

`CFNStackState` declares its account and Region under wire-visible `,omitempty` `json` tags and its `CreatedAt` and `UpdatedAt` as times, and `CFNChangeSet` its `CreatedAt`, because the records are what
`MemoryStateManager` snapshots and a replay reads back. None reaches a body: CloudFormation speaks the query protocol, and every response is marshaled from an XML struct declared for its operation, which publishes `CreationTime` and `LastUpdatedTime` instead.
`TestCFNWire_StackResponsesCarryNoBookkeepingMember` and `TestCFNWire_ChangeSetResponsesCarryNoBookkeepingMember` in `emulator/cfn_wire_test.go` read each member back from the stored record first, then drive every routed stack and change-set operation and walk each response's element names
([#756](https://github.com/scttfrdmn/substrate/issues/756)). `ListImports` is not driven: it answers importing-stack names, not a stack.

## Cost

CloudFormation operations are free. The resources a template deploys are costed
by their own plugins, so a stack's cost shows up under S3, EC2 and so on.
