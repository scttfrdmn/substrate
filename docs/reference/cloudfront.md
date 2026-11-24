# CloudFront

**Endpoint:** `cloudfront.amazonaws.com` (global)
**Protocol:** REST/XML

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateDistribution | Distribution IDs: `E{13-char upper alphanum}`, derived from the request ID (#1277). An empty body is `MissingBody`/400, and one holding no readable `DistributionConfig` is `InvalidArgument`/400 — see [The `WithTags` query key selects an operation](#the-withtags-query-key-selects-an-operation). `CallerReference` is required, and a reused one is `DistributionAlreadyExists`/409 — see [A CallerReference is used once](#a-callerreference-is-used-once) (#1116). Answers the whole `Distribution` (#1115) |
| CreateDistributionWithTags | Same path as `CreateDistribution` with `?WithTags`; body is a `<DistributionConfigWithTags>`, whose nested `DistributionConfig` is applied exactly as `CreateDistribution` applies it and whose `Tags` are stored on the distribution. The same refusals (#1133, #1116) |
| GetDistribution | Answers the `ETag` header (#1271) and the `Distribution` members — see [A configuration is not a distribution](#a-configuration-is-not-a-distribution) (#1115). Observes the seeded `Status` window — see [A distribution reports InProgress for a seeded window](#a-distribution-reports-inprogress-for-a-seeded-window) (#1381) |
| GetDistributionConfig | Answers the configuration the create or last update sent, with what `CreateDistribution` defaults filled in, and the `ETag` header — see [An update replaces the configuration](#an-update-replaces-the-configuration) |
| UpdateDistribution | Shares the `/config` path with `GetDistributionConfig`, told apart by the verb. Replaces the configuration; requires `If-Match` (`InvalidIfMatchVersion`/400, `PreconditionFailed`/412); refuses `MissingBody`, `IllegalUpdate` and `InvalidArgument`/400 one member per call (#1271) |
| DeleteDistribution | Requires `If-Match` (`InvalidIfMatchVersion`/400, `PreconditionFailed`/412) and a disabled distribution (`DistributionNotDisabled`/409) (#1271). Releases the distribution's `CallerReference` (#1116) |
| ListDistributions | Each `DistributionSummary` copies its configuration members from the stored configuration and answers `LastModifiedTime` and `ETag` (#1115). Each distribution it reports spends one observation of its own seeded `Status` window (#1381). [Pages on `Marker`/`MaxItems`](#the-two-listings-page) |
| CreateInvalidation | Keeps and answers the `InvalidationBatch`; a resubmitted batch returns the first invalidation (#1360) |
| GetInvalidation | `NoSuchDistribution` and `NoSuchInvalidation` are both published and name different absences (#1091) |
| ListInvalidations | Refuses a distribution that does not exist rather than answering an empty list (#1091) |
| TagResource | Body is a `<Tags>` document; a body of another shape is refused rather than read as an empty tag set (#883) |
| UntagResource | Body is a `<TagKeys><Items><Key>` document. Removing a key the distribution does not carry succeeds — AWS documents no error for it, so that reading is substrate's (#883) |
| ListTagsForResource | Reports the `<Tags><Items>` members sorted by key — see [A tag set read back out of a map](/reference/concepts/listing-order#a-tag-set-read-back-out-of-a-map) |
| CreateOriginAccessControl | 201 with the `ETag` and `Location` headers; the four required config members are validated against their published enums — see [The origin access control family](#the-origin-access-control-family) |
| GetOriginAccessControl | 200 with the `ETag` header; absent → `NoSuchOriginAccessControl` |
| ListOriginAccessControls | An account using no origin access controls answers **no `Items` element**. [Pages on `Marker`/`MaxItems`](#the-two-listings-page) |
| DeleteOriginAccessControl | 204. `If-Match` required: missing or malformed → `InvalidIfMatchVersion`, well-formed but stale → `PreconditionFailed`. `OriginAccessControlInUse` is not answered — see below |

All three tagging operations share the `POST`/`GET /2020-05-31/tagging` path and are told apart
by the query string: `Operation=Tag`, `Operation=Untag`, and a `GET` carrying only `Resource`. A
POST whose `Operation` is neither is refused with `InvalidAction`, never treated as a tag write
(#883).

The Resource Groups Tagging API reaches a CloudFront distribution as of #835: `TagResources` and
`UntagResources` resolve a distribution ARN through the same parser these three operations use, so
a tag written either way is readable through the other. Until #835 both answered an
`InternalServiceException` `FailedResourcesMap` entry and tags here could be changed through
CloudFront's own operations only. No other CloudFront resource type is reachable — see
[the tagging section](/reference/tagging#an-arn-resolves-to-the-state-key-its-own-service-uses) for the kind guard and
for why `GetResources` reports a distribution in `us-east-1` alone.

All CloudFront resources are stored under `us-east-1` (global service).

## The `WithTags` query key selects an operation

`CreateDistribution` and `CreateDistributionWithTags` share `POST /2020-05-31/distribution`. The
reference publishes the second as `POST /2020-05-31/distribution?WithTags`: a bare query key, with no
value. That makes it the first query-string discriminator in the tree that selects an *operation*
rather than a sub-resource, so the event log and the authorization decision both name
`CreateDistributionWithTags`. The key is tested for presence, case-insensitively, because a bare key
reaches the plugin with the sentinel value `1`.

Both creates read the configuration through one function, and both refuse a body they cannot read
([#1133](https://github.com/scttfrdmn/substrate/issues/1133)). An empty body is `MissingBody`/400. A
body that is not XML, or that holds no `DistributionConfig`, is `InvalidArgument`/400. Both pages
publish both codes, and `DistributionConfig` is `Required: Yes` on both; `MalformedXML` is on neither,
so it is not used. Until #1133, `CreateDistribution` created a distribution from an empty
configuration in that case and answered 201: enabled whatever the caller sent, with an empty
`Comment`. A `<CreateDistributionRequest>` wrapping the configuration is still tolerated.

## The origin access control family

An origin access control is what lets a distribution read a private S3 bucket: CloudFront signs the
origin request with SigV4 and the bucket policy trusts the distribution rather than the world. The
four operations live on their own path — `POST`/`GET /2020-05-31/origin-access-control` for the
collection, `GET`/`DELETE …/origin-access-control/{Id}` for one member — and substrate routed none
of it before #1277, so a consumer keeping its bucket private could not run against the emulator at
all: the create reached the unknown-route refusal.

`OriginAccessControlConfig` marks four members `Required: Yes`, and three of the four publish a
closed set of values: `OriginAccessControlOriginType` is `s3|mediastore|mediapackagev2|lambda`,
`SigningBehavior` is `never|always|no-override`, and `SigningProtocol` is `sigv4`. A missing member
or a value outside its enum is `InvalidArgument`/400, which is the code the operation publishes for
both. The comparison is case-sensitive: accepting `S3` would let a request through substrate that
AWS refuses, which is the direction a consumer pays for with a failed live deploy.

The control carries an **ETag**, and it is the one version substrate models on this service. The
create answers it as a header alongside `Location`, the get answers it as a header, and
`DeleteOriginAccessControl` requires it in `If-Match`, and the published description of
`InvalidIfMatchVersion` — *"The If-Match version is missing or not valid"* — is two cases in one
sentence: a **missing** header and a value that is **not a version substrate could have issued** are
both `InvalidIfMatchVersion`/400, while a well-formed version that is not the current one is
`PreconditionFailed`/412. Three published codes for three different mistakes, so a caller that sent
no version at all is not told its version was stale.

The `ETag` *shape* is substrate's: AWS publishes only that the value identifies the current version,
so substrate mints the same `E`-prefixed form it mints IDs in, from the same per-request mint, which
is what makes a replayed create hand out the version its recording did. That is also what makes
"malformed" decidable at all — a value outside that shape was never handed out here, so it cannot be
a stale one — and it is a statement about substrate's own minting rather than a claim about what
CloudFront accepts. A quoted `If-Match` is accepted as well as a bare one, since HTTP ETags are
conventionally quoted and CloudFront's are not.

An account using no origin access controls answers **no `Items`
element at all** rather than an empty one — the page states exactly that, and it is the difference
between a caller's `len(Items) == 0` and a decode that has nothing to decode. The listing pages as
[`ListDistributions` does](#the-two-listings-page).

Two published behaviours are deliberately absent. **`OriginAccessControlInUse`/409** on delete needs
to know that some distribution names the control. Since
[#1271](https://github.com/scttfrdmn/substrate/issues/1271) a distribution records the configuration
it was sent, its origins' `OriginAccessControlId` included, so the check is answerable, but it is not
made yet. **`OriginAccessControlAlreadyExists`/409** on
create is published for a control "with the specified parameters", which parameters is not published,
and a control carries no `CallerReference` to key a duplicate on the way a distribution does; a
repeated create mints a second control, so a consumer converging by name should list first.
`UpdateOriginAccessControl` is not implemented.

## A configuration is not a distribution

`GetDistribution` returns a `Distribution` and `GetDistributionConfig` returns a
`DistributionConfig`. They are two published types, one nested in the other, and substrate
answered the same fields for both until #1091: the configuration carried `Id` and `ARN`, which
`API_DistributionConfig` publishes nowhere — they are `Distribution` members, one level up. A
caller reading a distribution's identity out of a configuration found it here and finds nothing
there against CloudFront itself.

Since #1271 the configuration is the document the create or the last update sent (see the next
section), and `Comment` and `Enabled` are always among its members: both are `Required: Yes` and
rendered unconditionally by the Response Syntax.

**The `Distribution` side.** `API_Distribution` marks seven members `Required: Yes` — `ARN`,
`DistributionConfig`, `DomainName`, `Id`, `InProgressInvalidationBatches`, `LastModifiedTime` and
`Status` — and `CreateDistribution`, `GetDistribution` and `UpdateDistribution` answer all seven,
through one renderer ([#1115](https://github.com/scttfrdmn/substrate/issues/1115)). Until #1115 there
were two renderers with two member sets. `GetDistribution` and `UpdateDistribution` answered `Comment`
and `Enabled` beside `Id`, one level too high, and no `DistributionConfig` or
`InProgressInvalidationBatches`. `CreateDistribution` answered four members, not even
`LastModifiedTime`. `Comment` and `Enabled` now appear only inside the nested `DistributionConfig`,
which is the stored configuration exactly as `GetDistributionConfig` answers it, so the two documents
cannot disagree about it. `InProgressInvalidationBatches` is always 0: Substrate completes an
invalidation when it creates one (there is no edge cache to purge), so none is ever in progress. A
seeded `InProgress` window for invalidations is
[#1581](https://github.com/scttfrdmn/substrate/issues/1581). The three optional members
(`ActiveTrustedKeyGroups`, `ActiveTrustedSigners`, `AliasICPRecordals`) are absent: they report
signer and ICP-recordal state Substrate does not model.

`ListDistributions`' `DistributionSummary` answers its six distribution members (`Id`, `ARN`,
`ETag`, `Status`, `LastModifiedTime`, `DomainName`) and copies each configuration member it publishes
from the stored configuration, in the summary's order. `Aliases`, `Comment` and `Enabled` are always
present. The summary's other eleven `Required: Yes` configuration members (`Origins`,
`DefaultCacheBehavior`, `CacheBehaviors`, `CustomErrorResponses`, `PriceClass`,
`ViewerCertificate`, `Restrictions`, `WebACLId`, `HttpVersion`, `IsIPV6Enabled`, `Staging`) are
answered when the configuration carries them and absent when it does not, for the reason below.
Until #1115 a summary answered six members, and `Comment` only when it was non-empty. A stored
configuration that does not parse falls back to the record's own `Comment` and `Enabled` and is
logged, rather than failing the list. Every summary is built before any seeded `Status` observation
is spent, so nothing about a summary can consume a countdown for a list that then fails.

**Two divergences remain, and are deliberate.**

`DistributionConfig` marks five members `Required: Yes` — `CallerReference`, `Comment`,
`DefaultCacheBehavior`, `Enabled` and `Origins` — and `CreateDistribution` requires `CallerReference`
(since #1116) but not `Origins` or `DefaultCacheBehavior`, as it never has (#1197's class). So three
of the five are always answered. A distribution created without `Origins` or `DefaultCacheBehavior`
answers a configuration without them: Substrate holds no value for them, and inventing one would
assert a shape AWS has not published. A record written before #1116 may also lack
`CallerReference`.

`API_GetDistributionConfig` publishes, on its `Id` parameter: *"The distribution's ID. If the ID is
empty, an empty distribution configuration is returned."* An empty ID is reachable — the path
`/2020-05-31/distribution//config` routes to the operation with an empty ID — and substrate answers
`NoSuchDistribution`/404 instead, for the same reason: the "empty distribution configuration" is the
document with no published example whose five required members would have to be invented. The
refusal is the code the page publishes at the status it publishes, so a caller is told something
true; it is simply not what AWS says for this one input.

A path ending in a bare slash is *not* that case. `/2020-05-31/distribution/` has its trailing
slash trimmed before routing, so it is `ListDistributions` — not a `GetDistribution` with an empty
ID, which is the natural reading of the routing arithmetic and is wrong.

## The two listings page

[#1568](https://github.com/scttfrdmn/substrate/issues/1568). `ListDistributions` and
`ListOriginAccessControls` read the `Marker` and `MaxItems` query parameters and list in **ID order**.

- Every page answers `Marker` (the request's, empty when absent), `MaxItems`, `IsTruncated` and
  `Quantity` (the page's item count); `NextMarker` is present only when `IsTruncated` is true, and is
  the ID of the page's last item, as `API_ListDistributions` publishes ("which is also the ID of the
  last distribution on that page").
- The page starts strictly after the `Marker`, so a create or delete between pages neither skips nor
  repeats an item that survives. Any `Marker` is accepted: one naming a deleted or unknown ID resumes
  after where that ID sorts ([cursor pagination](/reference/concepts/listing-order#cursor-pagination)).
- An absent `MaxItems` is **100, substrate's reading**: neither page publishes a default. A `MaxItems`
  that is not a positive integer is `InvalidArgument`/400, the one code both pages publish; the
  message is substrate's own.
- `ListDistributions` pages over the index before it reads any record, so only the distributions on
  the page spend an observation of their seeded `Status` countdown.

`ListInvalidations` reads neither member and answers every batch; its page publishes a
most-recent-first order, which needs its own cursor ([#1677](https://github.com/scttfrdmn/substrate/issues/1677)).

## A CallerReference is used once

`API_DistributionConfig` describes `CallerReference` as a behaviour: *"A unique value … that ensures
that the request can't be replayed. If the value of `CallerReference` is new (regardless of the
content of the `DistributionConfig` object), CloudFront creates a new distribution. If
`CallerReference` is a value that you already sent in a previous request to create a distribution,
CloudFront returns a `DistributionAlreadyExists` error."* Until
[#1116](https://github.com/scttfrdmn/substrate/issues/1116) neither create read the member, so a
create without one was answered 201 and a replayed create minted a second distribution.

| Create | Code | Status |
|--------|------|--------|
| no `CallerReference`, or a blank one | `InvalidArgument` | 400 |
| a `CallerReference` one of the account's distributions was created with | `DistributionAlreadyExists` | 409 |

Both creates answer both. The refusal of a reused reference holds whatever the configuration says,
an identical one included: the page states the refusal "regardless of the content", and unlike
`InvalidationBatch` and `CloudFrontOriginAccessIdentityConfig`, whose pages do describe an identical
retry returning the original, `DistributionConfig` publishes no such case. A caller retrying a create
whose answer it lost should list to find the distribution. The `DistributionAlreadyExists` message is
the code's published description. `InvalidArgument` is the code the page publishes for "An argument
is invalid", and its message is Substrate's own.

A reference belongs to the account that created the distribution. **Deleting the distribution
releases it**, so a later create may reuse it; disabling does not. AWS publishes nothing on the
point, so that reading is Substrate's. The reference is kept in the stored configuration, which
`GetDistributionConfig` answers back, and in a per-account index entry naming the distribution that
holds it. A create reads that one entry, and a delete removes it. An entry counts only when the
distribution it names exists and still holds the reference. So an entry a half-finished delete left
behind, or a record that cannot be read (which is logged), never refuses a create.

A distribution created before #1116 may hold no reference, and an update may give it one. One
another distribution holds is `IllegalUpdate`/400, because `DistributionAlreadyExists` is not on
`API_UpdateDistribution`'s Errors list; the message is Substrate's own.

Both creates answer a `Location` header,
`https://cloudfront.amazonaws.com/2020-05-31/distribution/{Id}`. The API model publishes the member
as "The fully qualified URI of the new distribution resource just created". No page publishes an
example value, so the form is the one `CreateOriginAccessControl` already answers.

A CloudFormation-deployed distribution sends a reference of `{stack}-{logicalId}-{suffix}`. The
suffix is a hash of the stack ARN, the logical ID and the physical ID the stack last recorded for the
resource. A replay of the deployment sends the same one, and a replacement sends a fresh one, so it
can be created while its predecessor still holds the old. Nothing is drawn from the deployment's
mint, so no other identifier in the stack moves. One collision remains: Substrate derives a stack's
ARN from its name, so a stack deleted with its distribution retained (`DeletionPolicy: Retain`) and
re-created under the same name gets `DistributionAlreadyExists`.

## An update replaces the configuration

`API_UpdateDistribution`: *"The new configuration replaces the existing configuration. The values that
you specify in an `UpdateDistribution` request are not merged into your existing configuration. Make
sure to include all fields."* The procedure it publishes is read-modify-write: `GetDistributionConfig`
for the configuration and its `ETag`, change your own fields, `UpdateDistribution` with the `ETag` as
`If-Match`. Until [#1271](https://github.com/scttfrdmn/substrate/issues/1271) substrate recorded two
members and merged an update into them, so a configuration written from scratch updated cleanly here
and failed against CloudFront, one field per round trip.

A distribution now keeps the configuration it was sent, every member and in order, as a document
substrate echoes rather than interprets. `CreateDistribution` fills in the three members a real
account was observed to default on create and then require on update. Those members are refused on
update, one per call, in this order:

| Missing from the update | Code (400) | Message |
|---|---|---|
| `Aliases` | `IllegalUpdate` | Aliases are missing for the resource |
| an origin's `CustomHeaders` | `IllegalUpdate` | The 'OriginCustomHeaders' field is missing |
| a cache behavior's `SmoothStreaming` (default or path) | `InvalidArgument` | The parameter SmoothStreaming flag is missing |

The codes are on `API_UpdateDistribution`'s Errors list. The messages and the order are **observed**
(#1271's report), not published; the reference marks all three members `Required: No`. The
reporter suspected more of the same family (`OriginPath`, `TrustedSigners`, `Compress`, the
connection timeouts…), and none of those is enforced, because none was observed.

Then, in substrate's own wording:
- an update without `Comment` or `Enabled`, or with an `Enabled` that is not `true`/`false`, is
  `InvalidArgument`/400, because the update must have both to replace the record's values;
- an update that changes or drops the `CallerReference` the distribution was created with is
  `IllegalUpdate`/400 (*"You can't change the value of `CallerReference`"*).

The two other `Required: Yes` members (`Origins`, `DefaultCacheBehavior`) are not required on
update, because `CreateDistribution` does not require them. Requiring them on update alone would make
a distribution substrate itself created un-updatable. `CallerReference` is required on create since
#1116, so the unchanged-reference rule above already requires it on update for every distribution
created since.

**Versions.** A create mints the distribution's `ETag`, and each update mints a new one. `CreateDistribution`,
`GetDistribution`, `GetDistributionConfig` and `UpdateDistribution` answer it as the `ETag` header.
`UpdateDistribution` and `DeleteDistribution` require it as `If-Match`:
- a missing one, or one substrate could not have issued, is `InvalidIfMatchVersion`/400;
- a well-formed stale one is `PreconditionFailed`/412.

A record written before #1271 has no `ETag`; its version is its own ID until the first update mints
one. The order of checks on an update is: the distribution exists, there is a body (`MissingBody`),
it parses (`InvalidArgument`), the version is current, then the configuration's checks.

**Deleting.** `API_DeleteDistribution`: *"Before you can delete a distribution, you must disable
it"*, under the `ETag` the disable answered. An enabled distribution is
`DistributionNotDisabled`/409, after the version check.

A disabled distribution deletes during its `InProgress` window: the page publishes no refusal for
one still propagating, so substrate applies none (#1381).

A CloudFormation stack delete performs the same sequence: read, disable under the version read,
delete under the version the disable answered. It does not poll for `Deployed` between the disable
and the delete, because nothing refuses the delete, and its `GetDistributionConfig` read spends no
observation.

## A distribution reports InProgress for a seeded window

`API_Distribution` publishes `Status`: *"When the status is `Deployed`, the distribution's
information is fully propagated to all CloudFront edge locations."* A real distribution reports
`InProgress` after `CreateDistribution` and after every `UpdateDistribution`, and `aws cloudfront
wait distribution-deployed` and the SDK waiters poll `GetDistribution` for `Deployed`. Until #1381
substrate reported `Deployed` at once, so a waiter's first poll passed and its polling and timeout
handling were never exercised.

The window is a seeded progression ([How a progression is seeded](/reference/concepts/seeds-and-replay#how-a-progression-is-seeded)):

```
POST   /v1/cloudfront/distribution-status   {"distributionId": "E… | *", "inProgressObservations": 2}
DELETE /v1/cloudfront/distribution-status   (all; ?distributionId=E… for one)
```

| Read | Effect on the window |
|------|----------------------|
| `GetDistribution`, `ListDistributions` | Observe: each spends one observation of that distribution's countdown, and reports `InProgress` until the count is spent, then the record's `Deployed` |
| `CreateDistribution`, `CreateDistributionWithTags` response | Peek: reports the first observation without spending it |
| `UpdateDistribution` | Restarts the countdown — every update is a new propagation, the disable before a delete included — and its response peeks |
| `GetDistributionConfig` | Publishes no `Status`; spends nothing |
| `DeleteDistribution` | Removes the distribution's countdown |

The default is **zero observations**: an unseeded distribution is `Deployed` from its first
observation, as before #1381. A wait loop is opt-in because every existing consumer reads
`Deployed` straight after a create, and a default window would turn each of those into a poll. The
seed names a count and no states, because the page names exactly two values and the window runs from
one to the other. The countdown is per distribution even under `"*"`, so one `ListDistributions` of
five distributions advances each once (#582). The seed is a control-plane write recorded in
position, so a replay reproduces the window exactly.

## Both invalidation codes name something

`API_GetInvalidation` publishes `NoSuchDistribution`/404 and `NoSuchInvalidation`/404, and
`API_ListInvalidations` publishes `NoSuchDistribution`/404. Neither handler looked at the
distribution record until #1091: `ListInvalidations` read the invalidation index straight out of
state and answered 200 with an empty list for any ID at all, and `GetInvalidation` answered
`NoSuchInvalidation` — telling a caller a batch was missing from a distribution that does not
exist. Both load the distribution first, so each published code reports the thing that is actually
absent.

## An invalidation keeps the batch it was sent

`API_Invalidation` marks `CreateTime`, `Id`, `InvalidationBatch` and `Status` all Required: Yes.
Until #1360 `CreateInvalidation` and `GetInvalidation` answered three of them, because
`createInvalidation` never read its body: no path and no `CallerReference` was stored, and there
was no batch to answer. Both now answer the batch as it was sent. `CreateInvalidation` refuses the
bodies its page publishes codes for, all 400:

| Body | Code |
|------|------|
| None | `MissingBody` |
| Does not parse, or omits `CallerReference` or every path (both Required: Yes) | `InvalidArgument` |
| `Paths.Quantity` disagrees with the number of `Items` | `InconsistentQuantities` |

`API_InvalidationBatch` says a resubmitted batch, with the same `CallerReference` and the same
paths, does not create an invalidation, and returns the one created before. That is modeled, and
answered 201, the operation's only success status. The same page says a reused `CallerReference`
with *different* paths returns `InvalidationBatchAlreadyExists`, but `API_CreateInvalidation`'s
Errors list does not publish that code and nothing gives its status, so it is not modeled: such a
request creates a new invalidation.

## A tagging ARN addresses the distribution it names

The three tagging operations take a `Resource` ARN in the query string, and one function resolves
it for all three, so they cannot drift on which resource an ARN addresses. Two things about that
ARN are load-bearing, and until #918 both were wrong.

**The account comes from the ARN, not from the calling request.** `ListTagsForResource` publishes
the parameter's pattern — `arn:aws(-cn)?:cloudfront::[0-9]+:.*`, required — in which `[0-9]+` is
the account and the empty segment before it is the Region, absent because CloudFront is global.
The resolver read the distribution *ID* out of the ARN and then keyed the load and the store by
the caller's own account, so `arn:aws:cloudfront::{other}:distribution/E1EXAMPLE` addressed the
caller's `E1EXAMPLE`. `UntagResource` is the damaging direction, and it answered the documented
`204` while doing it: stripping a tag can turn an `aws:ResourceTag` `Deny` into an allow. This is
the rule #826 established for SQS and DynamoDB, #845 carried through the tagging API's resolver
and #910 applied to Step Functions — CloudFront was the last plugin holding out against it, and
the resolver now takes no request context at all, so the caller's account is not in scope to
reach for.

**The resource type is matched against the ARN's own segment.** It was
`strings.LastIndex(arn, "distribution/")`, which is unanchored, so
`arn:aws:cloudfront::{account}:streaming-distribution/E1EXAMPLE` resolved to the *web*
distribution `E1EXAMPLE` — a different resource type reaching a record it does not name. That is
the same anchoring mistake #910 found one layer up. The type is now the first `/`-delimited
segment of the resource portion and is compared whole.

Only `distribution` resolves, and that is the reference's own boundary rather than substrate's
convenience: the developer guide's tagging page states *"You can tag distributions, but you can't
tag origin access identities or invalidations"*. Substrate stores invalidations in the same
namespace and CloudFormation mints origin access identities (#859), so both are
reachable-looking targets that have to be refused rather than left to a substring match.

The two refusals answer two different published codes, because they are two different failures:

| Case | Code | Status |
|------|------|--------|
| The ARN is malformed — not an ARN, not CloudFront, carrying a Region, naming no account or no resource, or naming something nested under a distribution such as an invalidation | `InvalidArgument` | 400 |
| The ARN is well formed and names a CloudFront resource type substrate does not model — a streaming distribution, a function, a cache policy, an origin access identity | `NoSuchResource` | 404 |
| The ARN names a distribution that does not exist in the account it names | `NoSuchDistribution` | 404 |

Both `InvalidArgument` and `NoSuchResource` are among the four codes all three tagging operations
publish (`AccessDenied` 403, `InvalidArgument` 400, `InvalidTagging` 400, `NoSuchResource` 404);
which of the two each case gets is substrate's reading, since AWS publishes the codes and not the
conditions. The third row is not one of the four, and that is deliberate and older than #918: it
is the code every other arm of this plugin answers for an absent distribution, and one plugin
should not report a missing distribution two ways. An ARN naming an unmodelled *type* is a
different case — there is no distribution for it to be missing — which is why it takes the
published code instead.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::CloudFront::Distribution | DistributionId | |

## The account and tag bookkeeping a distribution carries reach no response

`CloudFrontDistribution` declares `AccountID` and `ever_tagged`, the flag that records a distribution was once tagged (#938), because the record is what `MemoryStateManager` snapshots and a replay reads back. Neither reaches a body: CloudFront speaks REST-XML, and every response is marshaled from an XML struct declared for its operation.
`TestCloudFrontWire_DistributionResponsesCarryNoBookkeepingMember` in `emulator/cloudfront_wire_test.go` tags the distribution and reads both members back first, since an unset `omitempty` member is absent for free, then drives every operation that answers the distribution and walks each response's element names
([#756](https://github.com/scttfrdmn/substrate/issues/756)). `TagResource` and `UntagResource` answer 204 with no body, so they have nothing to walk.

## Cost

CloudFront HTTPS requests: $0.0100 per 10,000 requests (approximate).
