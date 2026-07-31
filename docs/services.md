# Service Reference

## Coverage matrix

<!-- BEGIN GENERATED COVERAGE MATRIX -->
Substrate ships **64 built-in service plugins**. This section is generated
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
| 11 | CloudFront | `cloudfront` | REST/XML |
| 12 | CloudTrail | `cloudtrail` | JSON |
| 13 | CodeBuild | `codebuild` | JSON |
| 14 | CodeDeploy | `codedeploy` | JSON |
| 15 | CodePipeline | `codepipeline` | JSON |
| 16 | Cognito Identity | `cognito-identity` | JSON |
| 17 | Cognito Identity Provider | `cognito-idp` | JSON |
| 18 | DynamoDB | `dynamodb` | JSON |
| 19 | EC2 / VPC | `ec2` | Query |
| 20 | ECR | `ecr` | JSON |
| 21 | ECS | `ecs` | JSON |
| 22 | EFS | `efs` | REST/JSON |
| 23 | ElastiCache | `elasticache` | Query |
| 24 | ELBv2 | `elasticloadbalancing` | Query |
| 25 | EMR Serverless | `emrserverless` | REST/JSON |
| 26 | EventBridge | `eventbridge` | JSON |
| 27 | API Gateway (execute-api) | `execute-api` | REST/JSON |
| 28 | Kinesis Data Firehose | `firehose` | JSON |
| 29 | FSx | `fsx` | JSON |
| 30 | Glue | `glue` | JSON |
| 31 | Health | `health` | JSON |
| 32 | IAM | `iam` | Query |
| 33 | Kinesis Data Streams | `kinesis` | JSON |
| 34 | KMS | `kms` | JSON |
| 35 | Lambda | `lambda` | REST/JSON |
| 36 | CloudWatch Logs | `logs` | JSON |
| 37 | CloudWatch | `monitoring` | Query |
| 38 | MSK | `msk` | REST/JSON |
| 39 | HealthOmics | `omics` | REST/JSON |
| 40 | OpenSearch | `opensearch` | REST/JSON |
| 41 | Organizations | `organizations` | JSON |
| 42 | Price List Query API | `pricing` | JSON |
| 43 | QuickSight | `quicksight` | REST/JSON |
| 44 | RAM | `ram` | JSON |
| 45 | RDS | `rds` | Query |
| 46 | Redshift | `redshift` | Query |
| 47 | Redshift Data API | `redshift-data` | JSON |
| 48 | Route 53 | `route53` | REST/XML |
| 49 | S3 | `s3` | REST/XML |
| 50 | SageMaker | `sagemaker` | JSON |
| 51 | EventBridge Scheduler | `scheduler` | REST/JSON |
| 52 | Secrets Manager | `secretsmanager` | JSON |
| 53 | Service Quotas | `servicequotas` | JSON |
| 54 | SES v2 | `sesv2` | REST/JSON |
| 55 | SNS | `sns` | Query |
| 56 | SQS | `sqs` | JSON |
| 57 | SSM | `ssm` | JSON |
| 58 | SSO / Identity Store | `sso` | REST/JSON |
| 59 | Step Functions | `states` | JSON |
| 60 | STS | `sts` | Query |
| 61 | Resource Groups Tagging | `tagging` | JSON |
| 62 | Timestream | `timestream` | JSON |
| 63 | Transfer Family | `transfer` | JSON |
| 64 | WAFv2 | `wafv2` | JSON |
<!-- END GENERATED COVERAGE MATRIX -->

---

The per-service sections below carry hand-written operation lists, Betty
CloudFormation resource types, and cost notes for the most heavily used plugins.
They are maintained by hand and cover a subset of the plugins in the matrix
above; the remaining plugins are registered and functional but not yet detailed
here.

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
| DeleteRole | |
| ListRoles | |
| CreateGroup | |
| GetGroup | |
| DeleteGroup | |
| ListGroups | |
| AttachUserPolicy | |
| DetachUserPolicy | |
| ListAttachedUserPolicies | |
| AttachRolePolicy | |
| DetachRolePolicy | |
| ListAttachedRolePolicies | |
| CreatePolicy | |
| GetPolicy | |
| DeletePolicy | |
| ListPolicies | |
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

### Betty CFN resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::IAM::Role | RoleName | Supports AssumeRolePolicyDocument, ManagedPolicyArns |
| AWS::IAM::Policy | PolicyName | |
| AWS::IAM::User | UserName | |
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
| CreateBucket | |
| HeadBucket | |
| DeleteBucket | |
| ListBuckets | |
| PutObject | Supports Content-Type, metadata headers; conditional writes — see [Conditional requests](#conditional-requests) |
| GetObject | Supports Range header — see [Ranged reads](#ranged-reads); preconditions — see [Conditional requests](#conditional-requests) |
| HeadObject | Supports Range header — see [Ranged reads](#ranged-reads); preconditions — see [Conditional requests](#conditional-requests) |
| DeleteObject | Fires S3 notifications if configured |
| CopyObject | Honors both destination and `x-amz-copy-source-if-*` preconditions — see [Conditional requests](#conditional-requests) |
| ListObjects | |
| ListObjectsV2 | Supports Prefix, Delimiter, MaxKeys, ContinuationToken |
| CreateMultipartUpload | |
| UploadPart | |
| CompleteMultipartUpload | Validates part order, ETags, and part sizes — see [Multipart upload validation](#multipart-upload-validation); conditional writes — see [Conditional requests](#conditional-requests) |
| AbortMultipartUpload | |
| ListMultipartUploads | |
| GetBucketPolicy | |
| PutBucketPolicy | |
| DeleteBucketPolicy | |
| GetBucketAcl | |
| PutBucketAcl | |
| GetObjectAcl | |
| PutObjectAcl | |
| GetBucketNotificationConfiguration | |
| PutBucketNotificationConfiguration | Triggers Lambda/SQS on PutObject/DeleteObject |
| PutBucketTagging | |
| GetBucketTagging | |
| DeleteBucketTagging | |
| PutObjectTagging | |
| GetObjectTagging | |
| DeleteObjectTagging | |

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

A rejected conditional write is a no-op: the stored object is byte-identical
afterwards — body, ETag, size, `Content-Type` and user metadata all unchanged — and
a rejected `CompleteMultipartUpload` additionally leaves its upload open to be
retried or aborted.

**Concurrency.** N concurrent `If-None-Match: *` writes to one key yield exactly one
`200` and N-1 `412`s; the same holds for N concurrent `If-Match` writes asserting
the same ETag, which is the compare-and-swap primitive optimistic locking needs.
This guarantee is **process-local**. Substrate implements it with a per-key mutex
held across the existence check and the write, because `StateManager` exposes no
compare-and-swap; it therefore holds for any number of goroutines or HTTP clients
against one emulator process, but would not hold across two emulator processes
sharing one state backend.

Two outcomes real S3 documents are deliberately not emulated: the `409
ConditionalRequestConflict` returned when a concurrent operation interferes, and the
`404` returned when a concurrent delete lands mid-write. Both are races against
wall-clock timing rather than states a deterministic emulator can reach, so
substrate resolves every conditional write to one of the rows above. A consumer
that must exercise its 409 handler needs a fault-injection tier, not this one.

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

### Betty CFN resource types

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
| CreateFunction | Stores function metadata; no actual execution |
| GetFunction | |
| UpdateFunctionCode | |
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

### Betty CFN resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::Lambda::Function | FunctionName | |
| AWS::Lambda::EventSourceMapping | — | |

### Cost

Lambda invocations: $0.0000002 per request.

---

## SQS

**Endpoint:** `sqs.{region}.amazonaws.com`
**Protocol:** AWS Query (form-encoded, `Action=` parameter)

### Supported operations

| Operation | Notes |
|-----------|-------|
| CreateQueue | Supports FifoQueue, VisibilityTimeout attributes |
| GetQueueUrl | |
| GetQueueAttributes | |
| SetQueueAttributes | |
| DeleteQueue | |
| ListQueues | |
| SendMessage | Returns MessageId |
| SendMessageBatch | |
| ReceiveMessage | Supports MaxNumberOfMessages, WaitTimeSeconds |
| DeleteMessage | |
| DeleteMessageBatch | |
| ChangeMessageVisibility | |
| PurgeQueue | |

### Betty CFN resource types

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

### Betty CFN resource types

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
| RunInstances | Auto-creates default VPC (172.31.0.0/16) |
| DescribeInstances | [Explicit resource IDs](#explicit-resource-ids) |
| TerminateInstances | [Explicit resource IDs](#explicit-resource-ids) |
| StopInstances | [Explicit resource IDs](#explicit-resource-ids) |
| StartInstances | [Explicit resource IDs](#explicit-resource-ids) |
| DescribeInstanceStatus | [Explicit resource IDs](#explicit-resource-ids) |
| CreateVpc | |
| DescribeVpcs | [Explicit resource IDs](#explicit-resource-ids) |
| DeleteVpc | [Explicit resource IDs](#explicit-resource-ids) |
| CreateSubnet | |
| DescribeSubnets | [Explicit resource IDs](#explicit-resource-ids) |
| DeleteSubnet | [Explicit resource IDs](#explicit-resource-ids) |
| CreateSecurityGroup | |
| DescribeSecurityGroups | [Explicit resource IDs](#explicit-resource-ids) |
| DeleteSecurityGroup | [Explicit resource IDs](#explicit-resource-ids) |
| AuthorizeSecurityGroupIngress | |
| AuthorizeSecurityGroupEgress | |
| CreateInternetGateway | |
| AttachInternetGateway | |
| DescribeInternetGateways | [Explicit resource IDs](#explicit-resource-ids) |
| DeleteInternetGateway | [Explicit resource IDs](#explicit-resource-ids) |
| DescribeAvailabilityZones | |
| DescribeRegions | |
| CreateRouteTable | |
| AssociateRouteTable | |
| DescribeRouteTables | [Explicit resource IDs](#explicit-resource-ids) |
| DeleteRouteTable | [Explicit resource IDs](#explicit-resource-ids) |
| DescribeSnapshots | [Explicit resource IDs](#explicit-resource-ids) |
| DescribeAddresses | [Explicit resource IDs](#explicit-resource-ids) |
| DescribeNatGateways | [Explicit resource IDs](#explicit-resource-ids) |

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

### Betty CFN resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::EC2::VPC | VpcId | |
| AWS::EC2::Subnet | SubnetId | |
| AWS::EC2::SecurityGroup | GroupId | |
| AWS::EC2::Instance | InstanceId | |
| AWS::EC2::InternetGateway | InternetGatewayId | |

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

### Betty CFN resource types

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

### Betty CFN resource types

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

### Betty CFN resource types

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

### Betty CFN resource types

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

### Betty CFN resource types

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

### Betty CFN resource types

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
| DeleteLogGroup | |
| DescribeLogGroups | |
| CreateLogStream | |
| DeleteLogStream | |
| DescribeLogStreams | |
| PutLogEvents | Accepts up to 10,000 events per call |
| GetLogEvents | Supports nextForwardToken pagination |

Lambda auto-creates `/aws/lambda/{name}` log groups.

### Betty CFN resource types

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

### Betty CFN resource types

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

### Betty CFN resource types

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

### Betty CFN resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::CertificateManager::Certificate | CertificateArn | |

### Cost

ACM certificates are free.

---

## API Gateway (REST)

**Endpoint:** `apigateway.{region}.amazonaws.com`
**Protocol:** REST/JSON

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

### Betty CFN resource types

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

### Betty CFN resource types

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

### Betty CFN resource types

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

### Betty CFN resource types

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

### Betty CFN resource types

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

### Betty CFN resource types

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

### Betty CFN resource types

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

### Betty CFN resource types

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

### Betty CFN resource types

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

### Betty CFN resource types

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

### Betty CFN resource types

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

### Betty CFN resource types

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

### Betty CFN resource types

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

### Betty CFN resource types

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

### Betty CFN resource types

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

### Betty CFN resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::KinesisFirehose::DeliveryStream | DeliveryStreamName | |

### Cost

Firehose data ingestion: $0.029 per GB.
