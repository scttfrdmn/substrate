# Resource Groups Tagging

**Endpoint:** `tagging.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: ResourceGroupsTaggingAPI_20170126.{Op}`)

## Supported operations

| Operation | Notes |
|-----------|-------|
| GetResources | All eight published request members honored; base64 pagination token, refused unless substrate issued it; reports tagged and previously tagged resources only |
| TagResources | Applies tags to existing resources by ARN |
| UntagResources | Removes tag keys from resources by ARN |

`GetResources` scans forty-two resource types: S3 buckets, Lambda functions, SQS
queues, DynamoDB tables, EC2 instances and Elastic IP addresses (`elastic-ip`, as of #1191), EC2
security groups, launch templates and EBS volumes (`security-group`, `launch-template` and
`volume`, as of #1523), IAM users, roles and instance profiles (`instance-profile`, as of #1523),
API Gateway REST
APIs, API Gateway v2 HTTP and WebSocket APIs, Step Functions state machines and activities, ECR repositories, ECS
clusters, services, tasks and task definitions, Cognito user pools, Kinesis
streams, RDS DB instances, DB clusters and DB subnet groups, ElastiCache cache
clusters, EFS file systems, Glue databases, ACM certificates, CloudFront
distributions, CloudWatch Logs log groups, CloudWatch alarms (`cloudwatch:alarm`, as of #1618), EventBridge rules (`events:rule`, as of #1607),
KMS keys, SNS topics, Secrets Manager secrets, Systems Manager parameters, and ELBv2 load
balancers, target groups, listeners and listener rules.

A CloudWatch alarm is reported under `arn:aws:cloudwatch:{region}:{account}:alarm:{name}`, the
ARN CloudWatch's own `TagResource` takes. `TagResources` and `UntagResources` route to
CloudWatch's own `TagResource` and `UntagResource` rather than editing the alarm, so CloudWatch
serializes every write to an alarm and its 50-tag quota and refusals apply unchanged. Like a log group, its previously-tagged state is a
side-car key rather than a member of its record; see [Alarm tags](/reference/monitoring#alarm-tags).

An API Gateway HTTP or WebSocket API (`/apis/{id}`) is the newest (#1378), reached by the
same `apigateway` type filter as a REST API. A log group (#1282) and a v2 API are the two
types whose previously-tagged state is a side-car key rather than an `ever_tagged` member
of their record, for the reason
[Tagging a log group takes the unsuffixed ARN](/reference/logs#tagging-a-log-group-takes-the-unsuffixed-arn)
gives. A v2 API's tags are also readable through API Gateway v2's own `GetTags`; see
[An API's tags](/reference/apigatewayv2#an-api-s-tags).

ELBv2's four are the newest and the odd ones out, and the section on the ELB side
([An ELBv2 resource is reachable through the tagging API](/reference/elasticloadbalancing#an-elbv2-resource-is-reachable-through-the-tagging-api))
says why: every other arm turns an ARN into a state key by building one, while ELB's has to
*find* the record the ARN names, because a listener's and a rule's key carries a minted
suffix that no ARN component yields.

`TagResources` and `UntagResources` reach a slightly different set, because they
address one named ARN rather than enumerating a namespace: they additionally
reach EFS access points and Glue's other three types. Those four are the whole
remaining difference between the two halves — the ECS namespace, which was the
last and largest of them, gained its scanners in #935, so a tag written to an ECS
service, task or task definition is now both readable through ECS's own
`ListTagsForResource` and reported by `GetResources`.

An ECS task definition is reported **per revision**, including a deregistered
(`INACTIVE`) one. Each revision has its own ARN and its own tags —
`RegisterTaskDefinition` accepts tags per revision, and the tag ARN parser splits
`{family}:{revision}` to reach exactly one of them — so reporting a family's newest
revision only would hide a tag the tagging API itself had written to an older one.
AWS's tagging page says nothing about deregistration, and a deregistered revision
still answers `DescribeTaskDefinition`, so it remains an observable resource.

All four ECS scanners are scoped to the caller's account **and Region**, through the
same shared prefix helper every other scanner now uses, so none of them can disagree
about scope. Before #935 the cluster scanner prefixed by account alone, so a `us-east-1`
caller was reported a `us-west-2` cluster while the same caller's services — keyed
identically — were Region-scoped. The section below is the general rule that grew out of
that one.

## `GetResources` reports what has been tagged, not what is tagged

`GetResources` publishes two rules that end at the same place — a record whose tag set is
empty — and until #938 substrate could not tell them apart. It "does not return untagged
resources"; and, on `TagFilters`, "[i]f you don't specify a `TagFilter`, the response
includes all resources that are currently tagged or ever had a tag. Resources that were
previously tagged, but do not currently have tags, are shown with an empty tag set, like
this: `"Tags": []`."

So a resource is in one of three states, and only two of them are reported:

| State | Reported | Tag set |
|---|---|---|
| Never tagged | No | — |
| Tagged | Yes | its tags |
| Previously tagged, now empty | Yes, and only when no `TagFilter` is given | `[]` |

Before #938 substrate reported all three, so a scan answered with every resource the
caller had ever created — an inventory call that cannot distinguish "tagged with nothing"
from "not tagged" is not an inventory of tags. The rendering matters as much as the
membership: `[]` rather than `null` is what AWS publishes, and a caller decoding a tag
list cannot tell the two apart, so substrate normalises an absent set to an empty list at
one place in the scan rather than in each of the scanners.

**The third state needs state the tags do not carry**, so each scanned record gains a
persisted `ever_tagged` boolean and the scan reads `len(Tags) > 0 || ever_tagged` once, in
the loop, rather than once per scanner. A side-car keyed by ARN was the alternative and was
rejected: a scanner already loads the record, so a second load per resource buys nothing,
and a side-car and a record can disagree about a resource that was deleted and recreated
under one name. Putting it on the record is safe against leaking onto the wire because
#1013 established that every scanned type either is a state-only record or projects through
a separate wire struct.

The flag is written by whichever writer *removes* a tag, not by whichever writes one: a
resource holding a tag is reported for holding it, and the first writer to empty the set is
the one looking at the set it is about to empty. A create-with-tags path therefore stamps
nothing and does not need to. Nothing ever clears the flag — the rule is about history, and
a resource that was tagged once stays previously tagged.

Both writer directions are covered: the tagging API's own `TagResources`/`UntagResources`,
and each owning service's native tag operation. **The one shape that has to read the set it
writes** is a writer that replaces a whole tag set rather than merging into it, because it
never sees a per-key removal — S3's `PutBucketTagging` and `DeleteBucketTagging` and Systems
Manager's `PutParameter` overwrite are the three in the tree, and each now carries the
stored flag forward. A writer added later that rebuilds a record from the request without
reading the stored one would make a previously tagged resource never-tagged again; that is
the gap to check for, and it is a property of the writer rather than of the scan.

## Every scanned resource is scoped to the caller's account and Region

`GetResources` "[r]eturns all the tagged or previously tagged resources that are located
in the specified AWS Region for the account", so a scan is scoped by where a resource
*is* — and an ARN is where AWS states that. Substrate therefore decides scope at one
choke point in the scan loop, off each reported ARN's own account and Region segments,
rather than once per scanner. Before #937 three scanners narrowed by nothing at all, so
an S3 bucket, a Lambda function or an SQS queue in any account was reported to any
caller, and fourteen more narrowed by account without the Region, so a `us-east-1` caller
was reported a `us-west-2` EC2 instance.

Reading the scope from the reported ARN rather than from each scanner's state-key prefix
is what makes it one rule instead of one per scanner. A scanner whose key carries no
Region cannot express the scope in a prefix at all — that was true of DynamoDB's key, and
of Lambda's and S3's, when #937 landed — and a scanner that can express it has to remember
to, which the ECS cluster scanner did not. The state-key prefixes still narrow wherever
the key can carry the scope, through one shared helper, but that is a narrowing of what
gets loaded rather than the guarantee. This is the read-side form of the rule #826
through #932 established for the write side, where every resolver takes the account and
Region from the ARN and never from the caller. Two of those keys were themselves wrong,
and #943 corrected both: a Lambda function name is unique per account per Region and a
DynamoDB table name per Region, so neither key could hold two resources AWS would let a
caller create. Both scanners now narrow by the whole scope, and S3's is the one that still
cannot, because a bucket name really is global — see
[A Lambda function and a DynamoDB table belong to one account in one Region](#a-lambda-function-and-a-dynamodb-table-belong-to-one-account-in-one-region).

An empty account or Region segment means the ARN states no such scope, and such a
resource is in scope everywhere. IAM depends on that: an IAM ARN carries no Region, so an
IAM user is reported to a caller in any Region, which is what a global service's resource
should do. It is also why this rule does not replace CloudFront's own `us-east-1` gate — a
CloudFront ARN is Region-less too, but AWS publishes CloudFront tagging through that one
Region, which is a narrower rule than an empty segment can express.

An ARN that does not parse — no `arn:` prefix, or fewer than six colon-separated
segments — is left in scope rather than dropped. A scan that silently discarded a resource
because substrate had built a malformed ARN for it would hide the ARN defect behind a
missing row, which is the opposite of what #827 settled: report what is there.

**An S3 bucket is the one resource scoped off its record rather than its ARN**, and that
part is substrate's reading. A bucket ARN is `arn:aws:s3:::{name}`: S3's bucket namespace
is global, so the ARN carries neither an account nor a Region segment to read, and the
state key cannot carry either for the same reason. The record does instead — a bucket
stores the Region it was created in, and #937 added the account that created it. A record
whose Region or account is empty stays in scope, on the same rule an absent ARN segment
gets, so a bucket written by a path that has no request context is reported with what is
known about it rather than vanishing from the listing.

Three ECS ARN shapes carry no tag, and the reason differs by shape. Each is
refused rather than accepted silently — ECS's own `TagResource`, `UntagResource`
and `ListTagsForResource` all answer `InvalidParameterException`/400, and the
tagging API's `TagResources` reports a `FailedResourcesMap` entry:

| ARN | Reason a tag cannot be written |
|---|---|
| `…:capacity-provider/{name}` | AWS lists a capacity provider among the taggable ECS resources, but substrate stores no such record — there is nothing for a tag to sit beside and nothing to read it back from |
| `…:service/{name}` | AWS's *short* service ARN. The long form `service/{cluster}/{name}` is what addresses a service; the short form names no cluster, so it identifies no record |
| `…:task-definition/{family}:{revision}` with a non-numeric revision | a revision is an integer, so `web:latest` addresses nothing. A family without a resolvable revision is not an addressable resource |

A container instance is the fourth type AWS lists as taggable
("capacity providers, tasks, services, task definitions, clusters, and container
instances") and has no ARN shape here at all, because substrate implements no
operation that registers one. That is the capacity provider's reason rather than a
separate one.

Absence is a different answer from either: a well-formed ARN of a type substrate
*can* key, naming a cluster or a revision that does not exist, answers
`ResourceNotFoundException` through ECS's own operations. The table above is about
the shape, not about a missing record.

## A `ResourceTypeFilters` type is the ARN's own segment

A filter entry is `service[:resourceType]`, and AWS pins what the type half means from
two directions: "[s]pecifying a resource type of `ec2:instance` returns **only** EC2
instances", and "[t]he string for each service name and resource type is the same as
that embedded in a resource's Amazon Resource Name (ARN)". Together those say the type
is delimited by the ARN itself.

Substrate compared it as an unanchored prefix of the ARN's whole resource portion until
#936, which broke the first sentence in both directions at once. **Too wide:** `ecs:task`
selected a task definition, whose resource portion is `task-definition/{family}:{revision}`
and so begins with the string `task` — a caller could not express "tasks only" at all.
**Too narrow:** `apigateway:restapis` selected nothing, because API Gateway's resource
portion begins with a slash (`arn:{partition}:apigateway:{region}::/restapis/{api-id}`),
which a prefix comparison against `restapis` cannot see past.

The type is now the segment the ARN delimits, with one leading slash stripped first. That
covers the four shapes in the scanned set, and an ARN embedding no type at all yields its
whole resource portion:

| ARN resource portion | Type |
|---|---|
| `instance/i-abc123` | `instance` |
| `stateMachine:hello` | `stateMachine` |
| `task-definition/sidecar:3` | `task-definition` |
| `/restapis/abc123` | `restapis` |
| `my-bucket` | `my-bucket` (S3 embeds no type) |

The last row is the one narrowing a consumer may notice: an S3 bucket ARN is
`arn:aws:s3:::{name}`, so there is no type string for `s3:bucket` to be "the same as", and
a bucket named `bucket-logs` used to match that filter by accident of its first six
characters. The service-only filter `s3` is what reaches a bucket, as it always was.

This is the anchored-segment rule of #910 and #918 applied one layer up. Those fixed
`strings.Contains(arn, ":stateMachine:")` and `strings.LastIndex(arn, "distribution/")` in
the ARN *resolvers*; the filter matcher is the one comparison of that kind whose left-hand
side comes from the caller, and it was in neither pass.

## `GetResources` honours all eight of its request parameters

`API_GetResources` publishes eight request members, all `Required: No`. Substrate decoded
four of them, and until #1004 and #1010 two of those four were not honored as published
either:

| Member | Before | Now |
|---|---|---|
| `TagFilters` | honored | honored |
| `ResourceTypeFilters` | honored | honored |
| `ResourcesPerPage` | **clamped** — any value ≤ 0 became 100, and 5000 was served | refused outside 1–100 |
| `PaginationToken` | **both decode errors discarded** — an unissued token meant page one | refused unless substrate issued it |
| `ResourceARNList` | **dropped** — and its three exclusions unenforced | selects the named ARNs |
| `TagsPerPage` | dropped | cuts the page by tag count |
| `IncludeComplianceDetails` | dropped | renders `ComplianceDetails` |
| `ExcludeCompliantResources` | dropped | refused without its companion |

Every refusal is `InvalidParameterException` at HTTP 400, the operation's only published
code for a bad request; its own gloss names the two conditions — "a provided string
parameter is malformed" and "a provided parameter value is out of range". The message is
therefore what distinguishes one refusal from another, and each one names the parameter
and the bound it violated.

**`ResourcesPerPage` is refused, not clamped.** AWS states "[y]ou can specify a minimum of
1 and a maximum value of 100" in the member's own prose rather than as a Length Constraint,
which an Integer member does not carry. Substrate's single `if in.ResourcesPerPage <= 0 {
in.ResourcesPerPage = 100 }` arm was doing two jobs — supplying the default and swallowing
an out-of-range value — and only the first was correct. The worse direction was upward:
`5000` was honored, so one call could return every resource in the account on a page size
AWS refuses, and a consumer's paging loop was never exercised. The member is now a pointer,
because `Required: No` with a published minimum above zero means an omitted integer and an
explicit `0` are different requests: absent is the default of 100, and `0` is out of range.

**`ResourceARNList` was the costliest omission, and not because it was unimplemented.** The
page publishes it as mutually exclusive with `ResourceTypeFilters`, with `TagFilters`, and
with all three pagination members — three sentences, each promising an "`Invalid Parameter`
exception". So substrate accepted three request shapes AWS refuses, and in every one of
them answered the account-wide scan: a caller asking for the tags on five ARNs was handed a
superset at HTTP 200 with nothing in the response to say the request served was not the
request sent. All three are now refused, and the third is why #1004 and #1010 are one
change: distinguishing an absent `ResourcesPerPage` from a sent one is exactly the pointer
above.

The ARN comparison is exact. `ResourceARNList` takes ARNs and an ARN identifies one
resource, so a prefix or case-folded match would return resources the caller did not name.
An ARN naming nothing is not an error — "if a resource specified by this parameter doesn't
exist, it doesn't generate an error; it simply isn't included in the response" — so a list
of entirely absent ARNs is an empty list at 200. Its published bounds are enforced: 1–100
items, each 1–1011 characters. An empty `ResourceARNList: []` is refused rather than read as
absent, because a caller that sent the member asked to filter by ARN and filtering by none
of them is not the account-wide scan.

**`TagsPerPage` is implemented rather than refused**, because the page publishes exact and
deterministic behaviour for it: the 100–500 range, "a resource with no tags is counted as
having one tag (one key and value pair)", "does not split a resource and its associated
tags across pages", and a worked example — `TagsPerPage` 100 against 22 resources of 10 tags
each yielding pages of 10, 10 and 2 — that pins the comparison as inclusive. Both page
members apply when both are sent, the page breaking at whichever limit is reached first.
Note the minimum is 100 and not 1: the member counts tags rather than resources.

One departure, unobservable and recorded anyway: **at least one resource is always taken.**
Read literally, "a `PaginationToken` is returned in place of the affected resource and its
tags" would have a first resource whose own tags exceed the whole budget yield an empty page
and a token pointing at the same resource, so a caller looping to a null token would never
terminate. The case cannot arise — the minimum `TagsPerPage` is 100 and no service substrate
models admits more than 50 tags on a resource — but progress is guaranteed rather than left
to depend on that, because a hang is a worse failure than a page one tag over budget.

**`ComplianceDetails` renders three of its four published members, and omits
`ComplianceStatus`.** It was `*struct{}`, so the only two documents it could produce were
"absent" and `{}`, neither of which is the published shape. The three key arrays —
`KeysWithNoncompliantValues`, `MissingTagKeys`, `NoncompliantKeys` — are each defined
against the *effective tag policy* by their own descriptions, and substrate models no
organization with tag policies enabled (they are "available only in an organization that has
all features enabled"), so no key can be a member of any of them. `[]` is the derived
answer, not a placeholder for one, and it is `[]` rather than `null` for #938's reason.

`ComplianceStatus` cannot be derived that way, and reporting `true` would be the tempting
answer — nothing can be noncompliant with a policy that does not exist. The Organizations
tag-policies guide settles it the other way: "[u]ntagged resources or tags that aren't
defined in the tag policy aren't evaluated for compliance with the tag policy." Not
evaluated is not compliant, so a `true` here would claim an evaluation that never happened.
The member is `Required: No`, so a document without it is still the published shape.

It follows that **`ExcludeCompliantResources: true` excludes nothing**, since no resource is
evaluated as compliant — stated here rather than left for a caller to discover from an
unexpectedly full response. The page's own constraint on it is enforced regardless and needs
no tag-policy model: it "can be used only if the `IncludeComplianceDetails` parameter is
also set to `true`". An explicit `false` is accepted, which is substrate's reading — it asks
for nothing, and AWS's own Sample Request sends the member with a non-meaningful value.

**`PaginationToken` is emitted on every response**, `omitempty` removed. The evidence is the
operation's own Sample Response, which carries `"PaginationToken": ""` on a complete result,
plus the prose "repeat the query … until you receive a `null` value" — a consumer cannot
receive a value from a member that is absent. One sample is a reading rather than a
citation, and is recorded as one.

Two things this does **not** do, so the new checks are not read as complete coverage.
`ResourceTypeFilters`' per-item 0–256 length and its 100-item array bound, and `TagFilters`'
50-key and 20-values-per-key bounds, are **not** enforced; adding a bound without walking
its citations is how an emulator starts refusing what AWS accepts. And
`PaginationTokenExpiredException` stays unreachable: the expiry it reports is wall-clock
(fifteen minutes), which no substrate behaviour may depend on, and it is not the code for an
unissued token in any case — a token substrate never issued is *malformed*, not *expired*,
and the two imply different consumer actions ("your code composed this wrong" against "start
again from page one").

Per-member checks run before the cross-member exclusions. No ordering avoids every
two-round-trip case, so the criterion is the one #991's plaintext guard and #983's policy
document already use: name what is wrong with a single member before what is wrong with the
request as a whole, because the first is fixable from that member's own documentation while
the second makes the caller decide which of two features it wanted.

One documentation curiosity, recorded because a reader searching AWS's page for the exact
string would otherwise conclude the sentence is missing: the two exclusion sentences written
on `ResourceTypeFilters` and `TagFilters` both spell the member `ResourceArnList`, where it
is `ResourceARNList` everywhere else on the same page.

## An ARN resolves to the state key its own service uses

`TagResources` and `UntagResources` take an ARN and have to reach the record the
owning service reads. That is not automatic: the ARN and the state key are
derived separately, and where they disagree a tag is written to a record nothing
reads — the call answers `200`, the service reports no tag, and an
`aws:ResourceTag` condition on the resource never matches.

That is what happened for SQS until #826. A queue was stored under
`queue:{account}/{name}`, because the SQS plugin keyed on the last two components
of a queue URL, but the ARN resolver dropped the account and addressed
`queue:{name}`. The authorizer derived the same key a third way, from the last
component of the request's `QueueUrl`, and so had the same blind spot: every
`aws:ResourceTag/*` condition on an SQS request was unsatisfiable, which turns
an explicit `Deny` into a silent allow. All three now derive the key from the SQS
plugin's own builder rather than re-deriving it, which is how the Region #1088
added ([below](/reference/sqs#one-queue-name-is-one-queue-per-region)) reached every reader at
once: the key is `queue:{account}/{region}/{name}`.

Every other service's arm was audited against its plugin's key at the same time
and they agreed *for the resource type each arm claims to name*: S3, Lambda,
DynamoDB, EC2, IAM (users and roles), API Gateway, Step Functions, ECR, ECS,
Cognito, Kinesis, RDS, ElastiCache, EFS (file systems and access points) and
Glue's four types. SQS was the only key that disagreed.

That audit was narrower than it read, and #845 found what it missed. Eight arms
stripped a resource-type prefix without first checking it was there, and
`strings.TrimPrefix` returns its input unchanged when the prefix does not match —
so an ARN naming a *different type* under the same service built a well-formed
key for the wrong kind of resource instead of failing. A Step Functions
`activity:` ARN resolved to a state-machine key, so a tag meant for an activity
landed on a same-named state machine; a Lambda layer ARN resolved to a function
key. Every arm now checks what it strips. Three checks are more than a prefix
test, because AWS's own ARN formats make them so: a Lambda version and alias ARN
are lexically identical, so any qualified function ARN is refused rather than
guessed at; a DynamoDB index and stream ARN nest under `table/`, so a remaining
`/` is not a table; and Step Functions distinguishes its two types by the capital
M in `stateMachine:` alone, so that check is case-sensitive on purpose.

The account in a key comes from the **ARN**, not from the calling request, for
SQS as for IAM and EC2. An ARN naming another account therefore resolves that
account's resource or none at all, and appears in `FailedResourcesMap` — rather
than silently tagging the caller's own same-named resource, which would succeed
against the wrong thing.

DynamoDB was the one arm that rule had missed: it built its key from the calling
request's account, so an ARN naming another account's table tagged the caller's
own same-named table and `UntagResources` stripped tags from it. The resolver no
longer receives a request context, so no arm can reach for the caller's account
again — with one exception, for an ARN that carries **no** account by specification.

An API Gateway REST API's ARN is `arn:aws:apigateway:{region}::/restapis/{id}`: the
account segment is empty in the published format. The audit above listed API Gateway
as agreeing with its plugin, and it did not. The arm read that empty segment into the
key and looked up `api:/{region}/{id}`, while API Gateway writes
`api:{account}/{region}/{id}`, so `TagResources` answered `InvalidParameterException`
for every REST API a caller had created — and `GetRestApi` found it either side of the
call (#1307). Its tests had passed because they wrote the record by hand at the key the
resolver used. The resolver now receives the caller's account as a single named value,
read only by an arm whose ARN format has no account to give: the REST API is the
caller's own, which is the only resource such an ARN can name. An API Gateway ARN that
*does* carry an account still resolves that account's record or none. The tests create
the API through `POST /restapis`, so the two keys cannot disagree again unnoticed, and
`RestAPIState`'s previously-tagged flag, which only that arm sets, is now reachable.

The other formats with an empty account segment do not repeat it: an S3 bucket ARN
(`arn:aws:s3:::{name}`) has none, but neither does its key, since a bucket name is
global; and API Gateway v2's `/apis/{id}` resolves by the same rule as `/restapis/{id}`,
through the parser API Gateway v2's own tagging operations use (#1378). ECS's own `TagResource` had the same defect and now shares one key
builder with the ARN resolver, which is also what lets the tagging API reach an
ECS service, task and task definition rather than a cluster only.

RDS follows the same shape as of #835: the `rds` arm and RDS's own three tag
operations resolve through one builder, so the tagging API now reaches a DB
cluster and a DB subnet group as well as an instance and a snapshot — see
[the RDS section](/reference/rds#an-rds-arn-addresses-the-resource-it-names) for the four
segments and the per-kind 404.

Sharing the resolver is only half of what that took. The `rds` arm merged a tag by
decoding the record into an `RDSDBInstance` and storing the result back, whatever
the key named — so tagging a cluster through the tagging API replaced the cluster
record with an instance-shaped one, and every member an instance does not carry
under the same name went missing. Nothing refused and the tag itself looked right;
`DescribeDBClusters` simply stopped reporting the endpoint, the reader endpoint and
the port. That is data loss rather than a missing feature, which is why the arm now
edits the tags member of the raw JSON and leaves every other member untouched —
ECS's pattern, generalised so the two services share it. RDS spells the member
`Tags` and ECS spells it `tags`, so the member name is a parameter, and a
case-differing member is an error rather than a second member written alongside the
real one: every way of not writing a tag has to fail.

Step Functions followed as of #910, and it needed all three parts. The `states`
arm and Step Functions' own three tag operations now resolve through one builder,
which is what lets the tagging API reach an **activity** rather than a state
machine only (part of #835); the arm's merge went through the same shared
raw-JSON helper, because it decoded a `StateMachineState` whatever the key named
and so replaced a tagged activity's record with a state-machine-shaped one, losing
its `activityArn` outright; and the merge sits behind a kind guard, because the
`states` namespace also holds executions and three index keys that no ARN
addresses and that store no tags. The guard and the raw-JSON merge fix different
failures and neither substitutes for the other. Step Functions' own operations
took the account and Region from the calling request — see
[the Step Functions section](/reference/states#an-arn-addresses-the-resource-it-names-at-every-operation) for
what that reached and for the 400 status `ResourceNotFound` carries.

AWS does not publish what a cross-account ARN does here: `TagResources` says only
that "you can only tag resources that are located in the specified AWS Region for
the AWS account", and an explicit refusal is documented for a **partition**
mismatch but not for an account mismatch. Refusing is substrate's reading,
applied uniformly.

ACM certificates and CloudFront distributions followed as of #835, and both are on
AWS's own authoritative list: the tagging guide's welcome page names **AWS
Certificate Manager** and **Amazon CloudFront** among the services
`TagResources`/`UntagResources` support. Each row is a resolver arm, a merge arm and
a scanner, and none of the three substitutes for the others — an arm without a
scanner leaves the resource writable and invisible, and a scanner without an arm
leaves it visible and unwritable.

Both resolvers share the key builder the owning service already uses, which is what
#765's cross-readability criterion asks for. ACM's is the cheaper of the two because
`acmCertKey` embeds the whole certificate ARN alongside the account and Region it
names. That redundancy is also why ACM's own three tag operations legitimately key
from the calling request rather than from the ARN, and are **not** an instance of
the #918 defect: a lookup keyed by the caller's account can only ever find a record
whose ARN names that same account, so there is no key at which the two disagree.
Changing them to key from the ARN would introduce the cross-account reach that
arrangement forecloses.

Both merges go through the shared raw-JSON helper and both sit behind a kind guard,
for the reasons the `states` arm established. The guard's prefix is tested
**colon-terminated** in each namespace, because `cert:` is a prefix of `cert_arns:`
and `cfdist:` of `cfdist_ids:` — a bare-prefix test would report an index key
taggable and merge a tags member into a JSON array of identifier strings. Each
namespace holds a second kind no ARN addresses: ACM's certificate-ARN index, and
CloudFront's distribution index plus its invalidation records and their index.

**A CloudFront distribution is reported by `GetResources` in `us-east-1` only.**
CloudFront is global and its ARNs carry an empty Region segment, but `GetResources`
is a per-Region operation, so a global resource has to be attributed to exactly one
Region or every Region's call would report it — and `TagResources` states that "you
can only tag resources that are located in the specified AWS Region for the AWS
account". Which Region that is, is AWS's: a global resource is attributed to
`us-east-1`. That the gate exists at all is substrate's reading, because AWS
publishes the attribution and not a `GetResources` rule. CloudFront's own
`ListDistributions` keeps answering from any Region, which is not an inconsistency —
it is what a global service does, and the per-Region attribution belongs to the
tagging API alone.

KMS keys followed as of #835, together with the resolution fix (#922) that had to
land with them. KMS is on the truncated part of the welcome page's list, so its
write half is unlisted rather than refused; its read half is unconditional, like
every other row's. The row is the usual three parts, and two things about it are
new.

It is the first row whose tags are an **array of two-field objects** rather than a
map, so the merge goes through a second shared helper alongside the string-map one.
The element's field names are parameters because the services disagree: KMS spells
them `TagKey` and `TagValue`, and SNS, Secrets Manager and Systems Manager spell
them `Key` and `Value`. Both helpers sort the result by key, because a slice built
by ranging the intermediate map would come out in Go's map hash order and one
recorded run would not replay byte-identically — the rule #862 settled for the four
EC2-shaped helpers.

And it is the first row where sharing the resolver *created* a refusal rather than
only fixing a reach. All three KMS tagging operations publish "Cross-account use:
No", but while KMS keyed from the calling request a foreign-account key ARN could
only ever address the caller's own key, so that prohibition had nothing to refuse.
Honouring the ARN's account without adding the refusal would have turned a
wrong-record write into a genuine cross-account one. See
[the KMS section](/reference/kms#a-keyid-is-resolved-from-its-own-arn-not-from-the-caller) for
the four accepted `KeyId` forms and for why the alias case needs an anchored cut on
the *first* slash rather than the last.

SNS topics followed as of #835, again with the resolution fix (#925) that had to land
with them, and the row is where the anchored-segment rule of #910 has nothing to
anchor on: an SNS topic ARN carries no type keyword and no separator, so the resource
portion *is* the name. The discriminator the shape offers is the colon — a resource
portion containing one names a subscription — and that is what the resolver refuses.
See [the SNS section](/reference/sns#a-topic-arn-addresses-the-topic-it-names) for the ten
operations that shared the defect and for the two tag-parameter spellings AWS's
reference and its own examples disagree about.

SNS is also the row where sharing the resolver created **no** refusal, and the
contrast with KMS is the point: no cross-account statement appears on any of SNS's
three tagging pages. The state key is account- and Region-qualified, so a
foreign-account ARN builds a key nothing is stored at and the caller gets a
`FailedResourcesMap` entry — isolation emerges from the key rather than from a guard,
as ACM's does. Inventing a prohibition AWS does not publish would be substrate's
invention rather than its reading. Its merge sits behind a colon-terminated kind
guard for the reason the earlier rows' do: `topic:` is a prefix of `topic_names:`,
whose value is a JSON array of names.

Secrets Manager secrets followed as of #835, with their own resolution fix (#928),
and the row is the first where the identifier is legitimately **either** an ARN or a
bare name. AWS documents `SecretId` as "the ARN or name of the secret", and a name
carries no account — so the caller's own account is the *correct* source for that one
case, and refusing everything that is not an ARN would have been the easy
over-correction. The two are separated structurally rather than by convention: the ARN
parser takes no account, Region or request context at all, and only the name path is
given them. Anything beginning `arn:` that does not parse is refused rather than
falling back to a name lookup, because a caller who wrote an ARN prefix meant an ARN.

The tagging arm takes an ARN only, with no name fallback, since its parameter is
`ResourceARNList` and a bare name is not an ARN. Unlike SNS this shape *does* offer a
keyword to anchor on — `secret` is the fifth colon-delimited segment — and the name is
the whole remainder after it, so a hierarchical `prod/db/password` survives the round
trip where taking the ARN's last component would truncate it to `password`. Isolation
is emergent as SNS's and ACM's are, with no published cross-account prohibition to
enforce.

Its merge sits behind a colon-terminated kind guard, which is now the **fifth**
namespace to need one: `secret:` is a prefix of both `secret_names:` and
`secret_version:`. Four of the five namespaces reached so far have collided, so the
guard is the default for a new row rather than a per-service discovery. The version
key is also the strongest argument for the guard anywhere in the set — its value is a
caller's secret payload, and not JSON at all, so a tags member merged into it would
corrupt the secret rather than merely write somewhere nothing reads.

One thing substrate deliberately does not model, because it is what makes this row's
ARN-to-name derivation exact: AWS appends a hyphen and six random characters to a
secret's name when it mints the ARN, and warns "do not end your secret name with a
hyphen followed by six characters" precisely because trimming them back off cannot be
done in general. Substrate mints no suffix, so the name is recoverable from the ARN
by construction. Adding one would make the resolver ambiguous and would change a value
CloudFormation records as a physical ID, so it is a separate decision.

Systems Manager parameters came next, and the row is the first where the confusion the
resolver exists to prevent is between resource **types** rather than between accounts
(#932). Every earlier row asked *whose* resource an identifier names; this one asks
*what kind*. AWS publishes `ResourceType` as `Required: Yes` over a ten-value enum
(`Document | ManagedInstance | MaintenanceWindow | Parameter | PatchBaseline | OpsItem |
OpsMetadata | Automation | Association | CloudConnector`), and all three of Systems
Manager's own tag operations decoded it and never read it — so
`{"ResourceType": "Document", "ResourceId": "MyRunbook"}` tagged the *parameter* named
`/MyRunbook`, and `ListTagsForResource` with the same pair read the tag back, confirming
that the wrong resource had been tagged. A value outside the enum was accepted too, which
made `InvalidResourceType` — a code AWS publishes at all three operations —
unreachable.

The two refusals are deliberately distinct, because they tell a caller two different
things. A `ResourceType` outside the enum is `InvalidResourceType`, whose own description
— *"The resource type isn't valid"* — is the true statement about it. A `ResourceType` AWS
publishes but substrate models no taggable resource for is `InvalidResourceId`: the type
is real, and it is the identifier that names nothing, which is the honest-empty behavior
of #827. Answering one code for both would tell a caller who mistyped `Parameter` that
their parameter is missing. Substrate models a taggable resource for exactly one of the
ten today; the other nine are enumerated so that a value AWS publishes can be told apart
from a value AWS does not. Both are answered at HTTP **400**, which is what all three
reference pages give every code they publish: `InvalidResourceId` and `InvalidResourceType`
at 400, `TooManyTagsError` and `TooManyUpdates` at 400, and `InternalServerError` at 500.
There is no 404 anywhere in the set, so the status carries nothing a caller can branch on
and the code is the whole signal — which is why #933 was worth fixing even though the
refusal was already distinguishable by code.

This is also the first row where the owning service's own tag operations take **no ARN at
all**. AWS is explicit — *"For the `Document` and `Parameter` values, use the name of the
resource"* — so a parameter's `ResourceId` carries neither account nor Region, and the
caller's own are the only possible source for them. "The account comes from the ARN"
(#826) is therefore inapplicable to that half and has nothing to guard; the tagging API's
half does take an ARN, and its parser accordingly takes no account, Region or request
context, so it structurally cannot reach for the caller's. Anything beginning `arn:`
arriving as a `ResourceId` is refused rather than normalized into a name, per #928: a
caller who wrote an ARN prefix meant an ARN. Previously such a value became the *name*
`/arn:aws:ssm:us-east-1:123456789012:parameter/db/password` and was looked up as one —
refused either way, but for the wrong reason and after a normalization that makes the
message nonsense.

The leading-slash tolerance in the other direction is kept, and it is substrate's reading
rather than AWS's. AWS documents that tolerance explicitly only for `OpsMetadata`, whose
`ResourceID` may be given as either `aws/ssm/MyGroup/appmanager` or
`/aws/ssm/MyGroup/appmanager`. For a `Parameter` it is *required* by substrate's own
behavior: `PutParameter` normalizes `Name` to a leading `/`, so a caller who created
`MyParam` would otherwise be unable to tag it.

The tagging arm's ARN shape has one feature none of the earlier rows do: there is no
separator between the type segment and the name, because the name carries its own. The
resource portion of `arn:aws:ssm:us-east-1:123456789012:parameter/db/password` is
`parameter/db/password`, so the first `/`-delimited segment must be exactly `parameter`
(per #910's whole-segment rule — the same namespace addresses `document/`,
`servicesetting/`, `opsmetadata/`, `maintenancewindow/`, `patchbaseline/` and
`managed-instance/`) and the name is the whole remainder with the `/` restored. Taking the
ARN's last component would truncate a hierarchical name to `password`, and a prefix match
would grow wrong the moment AWS adds a type whose name begins `parameter`. The state key
consequently holds a doubled separator, `parameter:{account}/{region}//db/password`; that
is the shape already on disk and is left alone, since changing it would orphan every
stored parameter.

Its merge sits behind a colon-terminated kind guard, the **sixth** namespace to need one:
`parameter:` is a prefix of `parameter_paths:`, whose value is a JSON array of names. Five
of the six namespaces reached so far have collided, so the guard is now unambiguously the
default for a new row. Tag order at both arms is sorted by key, because
`AddTagsToResource` appended in Go map order while `PutParameter` stored the caller's
order — so a parameter tagged at creation and one tagged afterwards reported their tags
differently, which is the defect #862 fixed in the four merge helpers, re-verified here
rather than assumed. `ListTagsForResource` also rendered an untagged parameter's `TagList`
as `null` rather than as the empty array AWS publishes.

## A Lambda function and a DynamoDB table belong to one account in one Region

Every row above is about a *resolver* addressing the wrong record. This one is about the
record itself: two of the keys the resolvers address were not qualified enough to hold the
resources AWS lets a caller create. Before #943 a Lambda function was stored under
`function:{name}` and a DynamoDB table under `table:{account}/{name}`. Both are now
`{kind}:{account}/{region}/{name}` — `function:123456789012/us-east-1/orders` and
`table:123456789012/us-east-1/orders` — which is the shape Glue, Timestream and AppSync
already used and the shape CloudFormation's tag stamper keys on.

What that changes is not a mis-addressed tag but a refusal. A key that cannot tell two
resources apart makes the second one impossible to create:

| Before #943 | Now |
|---|---|
| Two accounts each creating a function named `orders` — the second answered `ResourceConflictException`/409 | both succeed, and each `GetFunction` reports its own account's ARN |
| One account creating a function named `orders` in `us-east-1` and in `us-west-2` — the second answered 409 | both succeed |
| One account creating a table named `orders` in `us-east-1` and in `us-west-2` — the second answered `ResourceInUseException`/400 | both succeed, and each `DescribeTable` reports its own Region's ARN |
| A `TagResources` naming another account's function ARN wrote to the caller's own function of that name | it writes to the function the ARN names, or to nothing |

**The two halves rest on different provenance, and the difference is worth stating.**
DynamoDB's is quoted: `CreateTable`'s own description says "In an AWS account, table names
must be unique within each Region. That is, you can have two tables with same name if you
create the tables in different Regions." Nothing needs inferring, and the code the old key
produced — `ResourceInUseException`, whose first listed cause is "[y]ou attempted to
recreate an existing table" — is on the same page at HTTP 400.

Lambda's is **the weaker of the two, and it is an inference rather than a quotation**.
`CreateFunction` states nothing at all about the scope a function name is unique within.
The whole of the argument is the shape of the value it hands back: `FunctionArn`'s published
pattern is
`arn:(aws[a-zA-Z-]*)?:lambda:[a-z]{2}((-gov)|(-iso([a-z]?)))?-[a-z]+-\d{1}:\d{12}:function:[a-zA-Z0-9-_\.]+`,
so the identifier AWS mints for a function qualifies its name by a Region and an account —
and an identifier that carries a scope is not an identifier of something outside it. That
is substrate's reading, not AWS's sentence. It is the same reasoning the ARN-format
argument gave for SQS in #826 and for DynamoDB in #845, and it points the same way in all
three cases; recording it as an inference is what distinguishes it from the DynamoDB half
rather than a reason to doubt it. #1088 then applied the same reading to the last service in
the class — see
[One queue name is one queue per Region](/reference/sqs#one-queue-name-is-one-queue-per-region), where the
identifier carrying the scope is a queue URL's host rather than an ARN, and where the missing
Region produced a create that answered another Region's URL rather than a refusal.

**Three sibling keys moved with the function key, and not moving them would have been a new
leak of its own.** A function's resource policy, its stored zip and its event-invoke
configuration were keyed `function_policy:{name}`, `function_zip:{name}` and
`function_invoke_config:{name}`. Qualifying only `function:` would have left two accounts'
same-named functions holding one policy, one code payload and one invoke configuration
between them — so `AddPermission` in one account would have granted access on the other's
function. Every key family in both services carries the full scope.

**One behaviour changed that no key required.** A Lambda event source mapping can name a
queue in one account and a function in another, and the poller invoked the function using
the *event source's* account and Region. That was already wrong and was invisible while the
function key carried neither: the invoke now resolves the function in the account and Region
its own `FunctionARN` names.

**No stored state needs migrating, and that is a fact about substrate rather than a
decision.** The only `StateManager` implementation in non-test code is
`NewMemoryStateManager`, and the server constructs it unconditionally — a SQLite backend is
deferred to #2 — so a key exists only for the lifetime of one process and there is nothing
written under the old shape for a later run to fail to find. The rename is therefore
same-process and needs no compatibility path, which is stated here rather than left implied.
When a persistent backend does arrive, a key-shape change stops being free and this
paragraph is the note that says so: a fixture recorded against the old key would then need
re-seeding, exactly as the account default's does above.

## Which failure gets which error code

A `FailedResourcesMap` entry carries one of the two codes `FailureInfo`
enumerates, split three ways, plus one case that carries the owning service's own
code:

| Case | Code | Status |
|------|------|--------|
| A well-formed ARN naming a resource type substrate cannot key | `InternalServiceException` | 500 |
| A well-formed ARN of a type substrate keys, naming a resource that does not exist | `InvalidParameterException` | 400 |
| Not an ARN: no `arn:` prefix, or fewer than six colon-separated segments | `InvalidParameterException` | 400 |
| The merge would leave the resource over its owning service's published per-resource tag quota | that service's own code | that service's own status |

`FailureInfo` documents `InternalServiceException` as covering "the resource type
in the request is not supported by the Resource Groups Tagging API", and tells the
caller "it's safe to retry the request and then call `GetResources` to verify the
changes". Which of the two codes an unsupported *type* earns is substrate's reading
rather than AWS's: the same page's `InvalidParameterException` bullets also say
"the target ID is invalid, unsupported, or doesn't exist", so both codes can be
read to cover it. Substrate splits those two on whether the ARN parses, because
that is the only distinction a caller can act on differently.

The middle row is **not** ambiguous, and substrate answered the 500 for it until
#939. `TagResources` and `UntagResources` both list "[t]he target ID is invalid,
unsupported, or doesn't exist" among `InvalidParameterException`'s causes, and a
resource that is not there is the third of those three; a 500 telling the caller it
is "safe to retry" pointed at a request that could only fail again. Wherever the
resolver builds an account- and Region-qualified state key from the ARN, a
foreign-account or foreign-Region ARN takes this row rather than one of its own: it
addresses a key nothing is stored at, so the refusal is emergent rather than a
separate guard. Lambda's arm was the one exception until #943 — its key was
`function:{name}` with no account in it, so a foreign-account function ARN reached the
caller's own function of that name — and it now qualifies by both halves like the rest.
An S3 bucket ARN — `arn:aws:s3:::{name}` — carries no account or Region to honour in the
first place, so the question does not arise there.

AWS publishes a contradiction about this field, recorded here rather than resolved:
`FailureInfo.ErrorCode` carries "Valid Values: `InternalServiceException` |
`InvalidParameterException`", while the same member's prose says it "can also
include any valid error code returned by the AWS service that hosts the resource
that the ARN key represents" and offers `AccessDeniedException` — which is in neither
enumerated value — as its example. Substrate reads the enumeration for every failure
it can express that way, because the enumeration is the part a caller can switch on,
and the prose for the one it cannot: a quota refusal (the table's fourth row). Neither
enumerated code says "this resource is full", so reporting one would leave the caller
to retry a request that can only fail again, which is the same reasoning that moved
the "does not exist" case off `InternalServiceException` in #939.

Substrate does **not** distinguish "AWS's tagging API does not support this type"
from "AWS supports it and substrate has no arm yet". AWS publishes no list that
could support the distinction for a *write*: the `supported-services` page its own
`TagResources` reference links to does not exist, and the list on the guide's
welcome page — which is the authoritative one — is truncated alphabetically at IAM,
so a type absent from it is unlisted rather than refused.

For a *read* there is no distinction to draw. The same page states that "the
`GetResources`, `GetTagKeys`, and `GetTagValues` operations support all resource
types", so every scanner substrate lacks is a gap in substrate rather than a
boundary of AWS's, and the scanner half of every #835 row was in scope
unconditionally — the last of them, ECS's, landed in #935. The per-service list
constrains `TagResources`/`UntagResources` only.

One requirement of `TagResources` substrate models at no arm: AWS requires the
caller to hold `tag:TagResources` **and** the owning service's own tagging
permission for the type. Substrate authorizes the tagging action alone.

An `ErrorMessage` never carries a state key. The detail naming the offending
resource portion goes to the log instead, because a state-key layout is
substrate's internal business and not something an API response should publish.

## A tag quota belongs to the service that owns the resource

`API_TagResources` states the rule itself — *"Each resource can have up to 50
tags"* — and four of the twenty-three namespace arms enforce a per-resource quota on
their **own** tagging operations: EC2, ELB, IAM and Kinesis. Three are at 50, and ELB
is at 50 for an ELBv2 resource and **10** for a Classic Load Balancer — the cap is
resolved from the record's own state key, so a classic load balancer reached through
this generation-agnostic API gets 10 because of what it is rather than because of which
door the request came through
([#1148](https://github.com/scttfrdmn/substrate/issues/1148)). Until
[#1000](https://github.com/scttfrdmn/substrate/issues/1000) the shared merge consulted
none of them, so `TagResources` was the one way in substrate to put a resource over its
own service's quota — after which that service's own tagging operation refused every
further add, leaving a resource in a state substrate's own reference says cannot exist,
reached through substrate's own API. Two earlier statements in the reference and in
`CHANGELOG.md` claimed Kinesis was the first quota of any service and that the merge
covered sixteen arms; both were wrong when written and are corrected with that issue.

**Each service's own checker is called rather than a shared count, because the four
disagree in ways a shared count would have to flatten:**

| Service | Cap | Code | Status | Reserved `aws:` keys |
|---------|-----|------|--------|----------------------|
| EC2 | 50 | `TagLimitExceeded` | 400 | Excluded from the count |
| ELBv2 | 50 | `TooManyTags` | 400 | Excluded from the count |
| ELB Classic | **10** | `TooManyTags` | 400 | Excluded from the count |
| IAM | 50 | `LimitExceeded` | **409** | Counted |
| Kinesis | 50 | `LimitExceededException` | 400 | Counted |

The two ELB rows answer the same code at the same status and differ only in the number
and in the message, and the numbers come from different places. ELBv2's `API_AddTags`
(`2015-12-01`) publishes **no maximum at all** — not in its description, not as an
`Array Members` constraint, not in its Errors section beyond naming `TooManyTags` — so
the 50 is read off the user guide's restrictions list (*"Maximum number of tags per
resource—50"*). The classic 10 is API-reference text, in the first sentence of the
`2012-06-01` `API_AddTags`: *"Each load balancer can have a maximum of 10 tags."* The
`TooManyTags` **message** differs between the two pages and each generation answers its
own, so a consumer reading the message sees which generation refused it.

The reserved-key column is what each service publishes, not a choice: EC2's and ELB's
restrictions state that *"[t]ags with the aws: prefix do not count against your tags
per resource limit"* and no IAM or Kinesis page says anything of the kind. That
restrictions list is written for Elastic Load Balancing rather than for one generation,
and the classic API page publishes no reserved-prefix rule of its own, so the exclusion
is applied to both ELB caps rather than to ELBv2's alone. It is
unobservable through this API either way — a reserved key is refused upstream, so only
the CloudFormation deployer's stamp can write one — and it is recorded rather than
unified because unifying it would mean overruling one of the four pages.

The count is over the **post-merge key set**, so rewriting the value of a key a
resource already carries succeeds at the quota where adding a new key is refused, and
`UntagResources` is never checked at all: a removal only shrinks the key set, so it
cannot exceed a quota, and the slot it frees is usable. That also keeps a resource
written over a quota before #1000 reportable and removable rather than untouchable.

A refusal writes **nothing** — the check runs before the merge, per #965 — and it is
one entry in the `FailedResourcesMap`, so the other ARNs in the same request are tagged.

The nineteen remaining arms publish no quota substrate models, and this path does not
invent one for them. SQS is the case in point: `API_TagQueue` states a 50-tag limit in
its own prose, but `TagQueue` does not enforce it either, and enforcing it here alone
would make substrate's two tagging APIs disagree in the opposite direction from the
defect being fixed.

**Neither CloudFormation writer enforces the quota, and the two reasons are
different.** The stamp writes only `aws:`-prefixed keys, which two of the four
services exclude from the count by their own statement. Propagation writes the
caller's own stack tags, which every one of the four would count — and
[#1077](https://github.com/scttfrdmn/substrate/issues/1077) decided that case rather
than leaving it filed. The decision is that **propagation writes regardless, and the
resulting over-quota resource is a recorded divergence**. See [propagation and a
resource's own tag
quota](/reference/cloudformation#propagation-can-leave-a-resource-over-its-service-s-tag-quota) for the search
behind it; in short, AWS publishes no outcome for the case *and* no code a refusal
could carry, and refusing on no citation would fail a template real CloudFormation
deploys — a false failure in a consumer's test, which is worse than the divergence.
The quota is still enforced at every door AWS publishes it for, including Kinesis's
own `AddTagsToStream` on the very stream the deployer took past fifty.

## Cost

Resource Groups Tagging API operations are free.
