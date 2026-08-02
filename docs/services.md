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
| 44 | RAM | `ram` | REST/JSON |
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
| CreateInstanceProfile | |
| GetInstanceProfile | |
| AddRoleToInstanceProfile | |

### Betty CFN resource types

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
| CreateBucket | |
| HeadBucket | |
| DeleteBucket | |
| ListBuckets | |
| PutObject | Supports Content-Type, metadata headers; `Cache-Control`, `Content-Disposition`, `Content-Language`, `Expires` — see [Object system metadata](#object-system-metadata); `Content-Encoding` less any `aws-chunked` — see [Content-Encoding and aws-chunked](#content-encoding-and-aws-chunked); `x-amz-storage-class` — see [Storage classes](#storage-classes); conditional writes — see [Conditional requests](#conditional-requests); verifies `x-amz-checksum-*` — see [Additional checksums](#additional-checksums) |
| GetObject | Echoes recorded system metadata — see [Object system metadata](#object-system-metadata); supports Range header — see [Ranged reads](#ranged-reads); preconditions — see [Conditional requests](#conditional-requests); `403 InvalidObjectState` on archived objects — see [Storage classes](#storage-classes); `x-amz-checksum-mode` — see [Additional checksums](#additional-checksums); synthesizes a seedable task-completion record — see [Task-completion records](#task-completion-records) |
| HeadObject | Echoes recorded system metadata — see [Object system metadata](#object-system-metadata); supports Range header — see [Ranged reads](#ranged-reads); preconditions — see [Conditional requests](#conditional-requests); succeeds on archived objects — see [Storage classes](#storage-classes); `x-amz-checksum-mode` — see [Additional checksums](#additional-checksums); resolves a synthesized task-completion record exactly as `GetObject` does — see [Task-completion records](#task-completion-records) |
| DeleteObject | Fires S3 notifications if configured |
| CopyObject | Honors both destination and `x-amz-copy-source-if-*` preconditions — see [Conditional requests](#conditional-requests); `x-amz-metadata-directive` / `x-amz-tagging-directive` and storage-class transitions — see [Copying objects](#copying-objects); recomputes the checksum — see [Additional checksums](#additional-checksums) |
| ListObjects | Emits `<StorageClass>` per object |
| ListObjectsV2 | Supports Prefix, Delimiter, MaxKeys, ContinuationToken; emits `<StorageClass>` per object |
| CreateMultipartUpload | Accepts `x-amz-storage-class` and the [system-metadata family](#object-system-metadata), applied to the assembled object; `Content-Encoding` less any `aws-chunked` — see [Content-Encoding and aws-chunked](#content-encoding-and-aws-chunked); `x-amz-checksum-algorithm` / `x-amz-checksum-type` — see [Additional checksums](#additional-checksums) |
| UploadPart | Verifies the part checksum, including a trailing one — see [Additional checksums](#additional-checksums) |
| CompleteMultipartUpload | Validates part order, ETags, and part sizes — see [Multipart upload validation](#multipart-upload-validation); conditional writes — see [Conditional requests](#conditional-requests); assembles the object checksum — see [Additional checksums](#additional-checksums) |
| AbortMultipartUpload | |
| ListMultipartUploads | Emits `<StorageClass>` per in-progress upload |
| GetBucketPolicy | |
| PutBucketPolicy | `403 AccessDenied` for a public policy when `BlockPublicPolicy` is set — see [Block Public Access](#block-public-access) |
| DeleteBucketPolicy | |
| PutPublicAccessBlock | Records the configuration and enforces `BlockPublicAcls` / `BlockPublicPolicy`; a partial body reports omitted settings as `false` — see [Block Public Access](#block-public-access) |
| GetPublicAccessBlock | `404 NoSuchPublicAccessBlockConfiguration` when the bucket has none — see [Block Public Access](#block-public-access) |
| DeletePublicAccessBlock | Idempotent; removes only the configuration, never the bucket — see [Block Public Access](#block-public-access) |
| GetBucketAcl | |
| PutBucketAcl | Accepts a canned `x-amz-acl` or an XML body; `403 AccessDenied` for a public ACL when `BlockPublicAcls` is set — see [Block Public Access](#block-public-access) |
| GetObjectAcl | |
| PutObjectAcl | `403 AccessDenied` for a public ACL when the *bucket* has `BlockPublicAcls` set — see [Block Public Access](#block-public-access) |
| GetBucketNotificationConfiguration | |
| PutBucketNotificationConfiguration | Triggers Lambda/SQS on PutObject/DeleteObject |
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
| `BlockPublicPolicy` | `PutBucketPolicy` with a public policy | `403 AccessDenied` / `Access Denied` |

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
`public-read-write`), an XML `Grant` naming a public group URI, and an
`x-amz-grant-*` header whose grantee list contains one. The grant headers are read for
this check only; substrate does not otherwise model them, so an ACL set through them
is still not stored.

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

**`PutObject` and `CreateBucket` with a public ACL are not yet refused.** Real
`BlockPublicAcls` rejects those too, but neither handler reads `x-amz-acl` at all, so
covering them means first modelling ACL-on-create. Tracked separately.

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
| CreateQueue | Supports FifoQueue, VisibilityTimeout attributes; [`QueueNameExists`](#queuenameexists) when a name is reused with differing attributes; [seedable `QueueDeletedRecently`](#seeding-queuedeletedrecently) |
| GetQueueUrl | [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent; [seedable consistency window](#seeding-the-create-then-lookup-consistency-window) |
| GetQueueAttributes | [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent; [seedable consistency window](#seeding-the-create-then-lookup-consistency-window); [attribute defaults](#queue-attribute-defaults) |
| SetQueueAttributes | [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent |
| DeleteQueue | [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent |
| ListQueues | |
| SendMessage | Returns MessageId; [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent; enforces [`MaximumMessageSize`](#message-size-enforcement); stores [message attributes](#message-attributes) and returns `MD5OfMessageAttributes` |
| SendMessageBatch | Enforces both the [per-message and batch-total size limits](#message-size-enforcement); stores [message attributes](#message-attributes) per entry |
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

Two limits are **not** enforced (tracked separately): the documented maximum of 10
attributes per message, and the attribute-name character rules (no `AWS.` or `Amazon.`
prefix, no leading, trailing or sequential periods). A 10-attribute message is accepted,
which is the boundary rather than a rejection.

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
| RunInstances | Auto-creates default VPC (172.31.0.0/16); [requires a resolvable AMI](#runinstances-requires-a-resolvable-ami); [merges a named launch template field by field](#a-launch-template-merges-with-the-request-field-by-field); [validates MinCount/MaxCount](#mincount-and-maxcount); reports [`groupSet`](#security-groups-on-an-instance) |
| DescribeInstances | [Explicit resource IDs](#explicit-resource-ids); reports [`groupSet`](#security-groups-on-an-instance) |
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
| AuthorizeSecurityGroupIngress | Supports source security groups (`IpPermissions.N.Groups.M.GroupId`), including self-referencing rules |
| AuthorizeSecurityGroupEgress | Supports destination security groups |
| RevokeSecurityGroupIngress | Matches on protocol, ports, **and** source |
| RevokeSecurityGroupEgress | |
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
| CreateLaunchTemplate | Creates version 1. Networking is read from `NetworkInterface.1.*` — see [Launch template networking](#launch-template-networking) |
| DescribeLaunchTemplates | Summary only — no `launchTemplateData`, matching AWS. Use `DescribeLaunchTemplateVersions` to read a template's parameters |
| DeleteLaunchTemplate | |
| CreateLaunchTemplateVersion | `SourceVersion` inheritance — see [Launch template versions](#launch-template-versions) |
| ModifyLaunchTemplate | `SetDefaultVersion` only, which is AWS's only modifiable attribute |
| DescribeLaunchTemplateVersions | Numbers, `$Latest`, `$Default`, `MinVersion`/`MaxVersion`, `MaxResults`/`NextToken`, and the account-wide form |
| DeleteLaunchTemplateVersions | Reports per version at HTTP 200; the default version cannot be deleted |
| CreateFleet | Instances launch through the `RunInstances` path, so they are visible to `DescribeInstances`, and carry the reserved `aws:ec2:fleet-id` tag. Partial fulfillment is seedable — see below |
| DescribeFleets | An `instant` fleet is returned only when its ID is named explicitly, matching AWS |
| DeleteFleets | `TerminateInstances=true` (and any `instant` fleet) terminates the fleet's instances |
| CreateTags | Rejects [reserved `aws:` keys](#reserved-tag-keys) |
| DeleteTags | Rejects [reserved `aws:` keys](#reserved-tag-keys) |

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

A template's `TagSpecifications` and `IamInstanceProfile` are not parsed at all, so
neither participates in the merge.

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
its **first network interface**:

```
LaunchTemplateData.NetworkInterface.1.SubnetId
LaunchTemplateData.NetworkInterface.1.SecurityGroupId.N
LaunchTemplateData.NetworkInterface.1.AssociatePublicIpAddress
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
| The request itself — `SubnetId`, or `NetworkInterface.1.SubnetId` | everything below |
| A `CreateFleet` override's `SubnetId` | the template (it reaches `RunInstances` as a request-level value) |
| The launch template's network interface | the default VPC |
| The auto-created default VPC | — |

`AssociatePublicIpAddress` is three-valued, and only a non-default subnet without
`MapPublicIPOnLaunch` distinguishes them: **absent** uses the subnet's own
behavior, **`true`** forces a public IP anyway, and **`false`** suppresses one.

Only interface index **1** is modeled, on both `RunInstances` and launch templates.
A template declaring a second interface loses it silently.

### Security groups on an instance

Both `RunInstances` and `DescribeInstances` report an instance's security groups as
`groupSet>item`, with `groupId` and `groupName` — the same shape as AWS's
`GroupIdentifier`. Groups appear in the order the launch resolved them, whichever
source supplied them:

- `SecurityGroupId.N` (or `SecurityGroupIds.N`) on the request, or the nested
  `NetworkInterface.1.SecurityGroupId.N` / `NetworkInterface.1.Groups.N`.
- The launch template's [network interface](#launch-template-networking).
- The auto-created default VPC's `default` group, when the launch names none.

`groupName` is **omitted when the group cannot be resolved** — for example after
the group is deleted while the instance it was launched with still exists. The
`groupId` is still reported, because that is what the launch actually recorded; a
name is not invented to fill the field.

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

`CreateTags` and `DeleteTags` reject any tag whose key begins with `aws:`, the
prefix EC2 reserves for its own use:

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

Two caveats about the scope of this rule:

- Provenance: the `CreateTags` API reference has an empty Errors section, so neither
  the code nor the message above is derivable from the API model. Both come from
  observed real-AWS responses. The `DeleteTags` rejection is a step weaker still —
  substrate found no captured `DeleteTags` error and inherits the wording from the
  `CreateTags` capture. What the tagging documentation does state plainly is the
  outcome: such a tag "can't be edited or deleted" by a caller.
- `RunInstances` tag-on-create is **not** covered. Real EC2 rejects a reserved key
  there too, but substrate stamps [`aws:ec2:fleet-id`](#finding-a-fleets-instances)
  through exactly that path, so restricting it would reject substrate's own fleet
  tagging.

The 50-tag-per-resource limit is not modelled at all, so the companion rule that
reserved tags are exempt from it has nothing to exempt them from yet.

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

### Betty CFN resource types

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
