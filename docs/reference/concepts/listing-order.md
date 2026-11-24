# The order a listing returns its members in

**A listing is sorted, and the order is part of what substrate promises rather than an
incidental tidiness.** `StateManager.List` returns the keys of a namespace sorted
lexicographically, and its doc comment states that as a contract an implementation must meet.
Every listing built from it therefore answers the same way twice.

That guarantee is stated here because for most of substrate's history it did not hold. State is
held in Go maps, whose iteration order is randomised per process, and `List` returned that order
directly — so a listing rendered straight into a response body reported its members in a
different order on each run, and sometimes on two calls within one run. In an emulator whose
whole claim is that a recorded run replays byte-for-byte, that is a correctness defect and not a
cosmetic one: a response body that reorders itself between two reads of unchanged state cannot be
compared against a recording, and a cursor paged over an unstable order can omit or repeat a
resource between pages ([#865](https://github.com/scttfrdmn/substrate/issues/865)).

The fix is in the one non-test `List` implementation rather than at each call site, because the
same defect had already been fixed five separate times at five individual sites — CloudFormation
stack tags, `aws:TagKeys`, `CreateSnapshots`' `snapshotSet`, `DeleteSnapshot`'s image-ID
tie-break, and the four tag merge helpers — each with its own written rationale, and each leaving
every other caller exposed.

**Where the order comes from differs by operation, and the three cases are not equally strong.**

- **AWS documents it.** `ListMultipartUploads` publishes a "Sorting of multipart uploads in
  response" section: ascending object key, then ascending initiation time among uploads sharing a
  key. Substrate's previous order was a citable violation, not merely a nondeterminism. Where two
  uploads of one key share an initiation instant — which a controlled clock allows and a real one
  effectively does not — the upload ID breaks the tie, which AWS does not document but its
  `key-marker`/`upload-id-marker` cursor implies.
- **The operation's own cursor requires it, though the prose states none.**
  `ListObjectVersions` is the case in point: `NextKeyMarker` is "the first key not returned that
  satisfies the search criteria" and a `CommonPrefixes` entry "is filtered out from results if it
  is not lexicographically greater than the key-marker", both of which presuppose a key order.
  RDS's and ElastiCache's describes are the other: their pages state no order either, but they
  do state what the `Marker` means — "the response includes only records beyond the marker" — and
  a record cannot be *beyond* another without an order to be beyond it in. AWS also publishes a
  `Marker` and `PageSize` on the four ELBv2 describes, and substrate pages them over this key order
  ([#1244](https://github.com/scttfrdmn/substrate/issues/1244)). A cursor over
  an unstable order is the worst form of this defect, because it loses and duplicates resources
  rather than merely reordering them.
- **AWS documents no order at all, and lexicographic is substrate's reading.** `ListBuckets` says
  nothing about the order buckets come back in; neither does `DescribeRules`, nor EC2's
  `reservationSet`. The guarantee there rests on the replay promise and on the five precedents
  above, not on a published statement, and a consumer should not read it as AWS's behaviour.

**Operations whose response-body order this actually changed**, verified one by one rather than
inferred from the call sites: S3 `ListBuckets`, `ListMultipartUploads` and `ListObjectVersions`;
the four ELBv2 describes that return a list — `DescribeLoadBalancers`, `DescribeTargetGroups`,
`DescribeListeners` and `DescribeRules`; RDS's five describes; and fifteen EC2 describes, among
them `DescribeInstances`, `DescribeVolumes`, `DescribeSnapshots`, `DescribeImages`,
`DescribeVpcs`, `DescribeSubnets` and `DescribeSecurityGroups`. The remaining `List` callers are
single-key lookups or mutations that stop at the first match, and their order was never
observable.

**Two listings were never affected, and are recorded here so the claim is not overstated.** The
Resource Groups Tagging API's `GetResources` sorts its scan results by ARN before paginating, so
its `PaginationToken` was always a cursor over a stable order. EC2's `DescribeTags` sorts by
resource ID, then type, then key. Both sorts remain load-bearing after the change, because
neither wants the order of the state keys its records were read from: `DescribeTags` spans every
resource type at once, and a tag's state key does not sort by resource ID.

`DescribeInstances` needed a second fix beyond the shared one. It buckets instances into
reservations through a map and then ranges that map, so sorting `List` made the instances within
a reservation deterministic while leaving `reservationSet`'s own member order in Go's map order.
It is now ordered by reservation ID.

## Every listing audited against those three tiers

[#865](https://github.com/scttfrdmn/substrate/issues/865) fixed the ordering defect at its source
and named the three tiers above, but it established them from the listings it happened to touch.
[#887](https://github.com/scttfrdmn/substrate/issues/887) audited the rest, so that the tier a
given operation sits in is a recorded finding rather than an assumption. All **128** non-test
`List` call sites, across **46** files, were classified by whether their order can be observed:

| Class | Sites | What it means |
|---|---|---|
| A | 50 | The keys reach a response body, so the order is observable by a caller. |
| B | 21 | The keys reach one of substrate's own control endpoints, not an AWS-shaped response. |
| C | 57 | The keys are scanned internally — a single-key lookup, a mutation that stops at the first match, or a set that is re-sorted before it is rendered. |

The `tagging_plugin.go` scanners are the largest block of class C: twenty of them feed
`GetResources`, which sorts by ARN before paginating, so their own order is discarded.

**The audit found no new tier-1 operation.** Eleven further AWS pages were read — RDS
`DescribeDBInstances` and `DescribeDBClusters`; ElastiCache `DescribeCacheClusters`,
`DescribeReplicationGroups` and `DescribeCacheSubnetGroups`; API Gateway `GetRestApis`;
CloudWatch `DescribeAlarms` and `ListMetrics`; SSM `DescribeParameters` and
`GetParametersByPath`; and EC2's Query request page — and **none publishes an ordering
statement**. `ListMultipartUploads` remains the only operation in the tree with a documented
order. EC2 is settled by a blanket disclaimer on its Query request page rather than per
operation: "The order of the elements in the response, including those within nested structures,
might vary. Applications should not assume that the elements appear in a particular order." All
thirty EC2-family sites are therefore tier 3, and substrate's lexicographic order is a stronger
guarantee than AWS gives.

**It found one tier-2 defect, at exactly three sites.** RDS's `DescribeDBInstances` and
`DescribeDBClusters` and ElastiCache's `DescribeCacheClusters` implemented `Marker` as a
**decimal offset** into the sorted listing. Their pages document the parameter positionally —
"the response includes only records beyond the marker, up to the value specified by
`MaxRecords`" — and an offset diverges from that in two ways a caller can observe:

- **A record removed behind the cursor loses a record the caller never sees.** Paging five
  instances two at a time and deleting the first after page one, the offset cursor answered page
  two as `items[2:]` of a now-four-record listing, so the third record was never reported. The
  caller's loop terminated normally with four of five records and nothing to indicate the fifth
  had been skipped. A record *added* behind the cursor repeated one instead.
- **A `Marker` substrate never issued was answered with page one.** The offset was parsed with
  `strconv.Atoi` and the error discarded, so any unparseable marker became `0`. A consumer that
  persisted a marker across a restart, or truncated one, silently restarted the walk.

Both are fixed by making the `Marker` name the last record of the previous page rather than count
the records before it, so the next page is the records sorting strictly after it — the pattern
`ListBuckets`' `continuation-token` already uses. The marker is base64, so one substrate did not
issue is detectable and is refused with `InvalidParameterValue` / 400. That code is **published**
for ElastiCache, on `DescribeCacheClusters` and `DescribeReplicationGroups`; the two RDS pages
publish only their NotFound faults, so for RDS it is **substrate's reading**. Truncation is
decided on the next matching record rather than on the page filling up, so a full last page
carries no `Marker` and costs the caller no round trip to an empty page.

`MaxRecords` was a separate matter and deliberately untouched by that fix — folding a
page-*size* change into a page-*contents* change would have made the two indistinguishable in
one diff — and it is now corrected in its own right, below.

## A page size outside the documented range is refused, not honored or rewritten

[#913](https://github.com/scttfrdmn/substrate/issues/913). Both families publish the same three
facts on `MaxRecords`, and substrate honored none of them:

| Fact | Published as | Substrate before |
|---|---|---|
| Default | `Default: 100` | 100, but also applied to an unusable value |
| Minimum | `Minimum 20` (RDS) / `minimum 20` (ElastiCache) | any positive integer honored |
| Maximum | `maximum 100` | any positive integer honored |

The two halves fail in different directions, and both matter:

- **An honored out-of-range value diverges towards the caller's disadvantage.**
  `MaxRecords=5` produced a five-record page here and is refused by real RDS, so a consumer
  written against substrate broke on AWS. That is the direction an emulator must not permit.
- **A rewritten value cannot be noticed.** `MaxRecords=0`, `-1` and `abc` became 100. A caller
  asking for a small page and receiving a hundred records sees the same well-formed shape as a
  caller whose listing is short — the same argument the token refusal above rests on.

An **absent** `MaxRecords` still defaults to 100, which is the case AWS publishes a default for.
Anything else must be an integer within 20–100 inclusive; the ends are accepted, because
narrowing a published range would be substrate inventing a contract. A refusal answers
`InvalidParameterValue` / 400 with a message naming the range, following `parseSimulateRequest`
and `parseS3ListBucketsParams`. That code is **published** for ElastiCache — `API_DescribeCacheClusters`
and `API_DescribeReplicationGroups` both list `InvalidParameterValue` at 400, "The value for a
parameter is invalid." — and is **substrate's reading** for RDS, whose pages publish only their
NotFound faults (`API_DescribeDBInstances` lists `DBInstanceNotFound` / 404 and nothing else). One
code serves both families, as it does for the `Marker`, so the two parameters of one cursor cannot
be refused under different codes. Both parameters are validated before any state is read.

**Elastic Load Balancing joined this rule later, and from the other side.** Its two paginated
operations at the time published one range — `PageSize` 1–400, on both generations' pages — and answered it two
ways: the classic `DescribeLoadBalancers` refused an out-of-range value while ELBv2
`DescribeAccountLimits` substituted its default and answered 200. The fallback rested on the
operation publishing no error of its own, which
[#1064](https://github.com/scttfrdmn/substrate/issues/1064) answered, so
[#1150](https://github.com/scttfrdmn/substrate/issues/1150) gave the plugin one rule and it is this
one: absent is the published default, anything else must be within the range, and a value outside it
is refused. The code is `ValidationError`/400 rather than `InvalidParameterValue`, because that is
what ELB's own Common Errors page publishes — the rule is shared, the vocabulary is each service's.

**A finding recorded rather than quietly fixed.** Eleven request sites across eight tests paged at
`MaxRecords=2` — a page size both real services refuse — and passed only because substrate was
permissive. That is evidence of the divergence, not noise, so it is recorded here: those tests now
page at the documented minimum of twenty and create twenty-odd records to reach a second page.

## Eight JSON list operations refuse a page size outside the published range

[#1131](https://github.com/scttfrdmn/substrate/issues/1131). Eight handlers read their page size as
`if MaxResults <= 0 { MaxResults = n }` and compared it with no maximum, so an explicit 0 or -1 was
answered with substrate's default page and a value above the published maximum was honoured. The same
rule as ELB's now applies to each (`emulator/page_size_range.go`): an absent member takes the default,
a value inside the Valid Range is honoured, and a value outside it is refused. No page in the set
publishes a clamp, and none publishes a code tied to the page size, so the code is each service's own
validation code — the operation's page where it publishes one, otherwise the Common Errors page the
operation links. The messages are substrate's own.

| Operation | Member | Valid Range | Absent | Out of range |
|---|---|---|---|---|
| Secrets Manager `ListSecrets`, `ListSecretVersionIds` | `MaxResults` | 1–100 | 100 (none published) | `InvalidParameterException` / 400 |
| Step Functions `ListStateMachines`, `ListActivities` | `maxResults` | 0–1000 | 100 (published) | `ValidationError` / 400 |
| Step Functions `ListExecutions` | `maxResults` | 0–1000 | 100 (published) | `ValidationException` / 400, which the page publishes |
| Systems Manager `GetParametersByPath` | `MaxResults` | 1–10 | 10 (none published) | `ValidationError` / 400 |
| Systems Manager `DescribeParameters` | `MaxResults` | 1–50 | 10 (none published) | `ValidationError` / 400 |
| KMS `ListKeys` | `Limit` | 1–1000 | 100 (published) | `ValidationError` / 400 |
| KMS `ListAliases` | `Limit` | 1–100 | **50** (published) | `ValidationError` / 400 |

**KMS `ListAliases` defaults to 50.** Its page says "If you do not include a value, it defaults to 50";
substrate shared `ListKeys`' 100, so sixty aliases were one page here and two on KMS. The two KMS
operations now carry separate constants.

**Step Functions' 0 was already right.** Its pages publish a minimum of 0 and say "A value of 0 uses the
default", so 0 still means 100. Only a negative value, or one above 1000, is refused.

**What a refusal changes.** A request that omits the member answers exactly as before. A request that
sent an out-of-range value, which used to succeed, is now refused; a recording that contains one
replays to the refusal.

## A pagination token substrate never issued is refused, not answered with page one

[#915](https://github.com/scttfrdmn/substrate/issues/915) is the second half of the tier-2 defect
above, at the four operations the RDS and ElastiCache fix did not reach; the tagging API's
`GetResources` joined them with #1010, which is why the table below has five rows for a four-operation
issue. Each decoded its token and discarded the error:

```go
if decoded, decErr := base64.StdEncoding.DecodeString(nextToken); decErr == nil {
	if n, parseErr := strconv.Atoi(string(decoded)); parseErr == nil && n >= 0 {
		offset = n
	}
}
```

So a token substrate could not have issued — one from another operation, a truncated copy, a
hand-written string, an offset left over from an older recording — left the offset at zero and the
operation answered a well-formed **page one**. That is the one wrong answer a paginating caller
cannot detect: a loop that runs until the token comes back empty is handed the first page again, so
it either spins or processes the same records twice, and the response says nothing.

The code and the message are per operation, and their provenance differs:

| Operation | Code | Message | Provenance |
|---|---|---|---|
| CloudWatch `DescribeAlarms` | `InvalidNextToken` / 400 | The next token specified is invalid. | **Published.** It is the only error `API_DescribeAlarms` lists, and the same code appears in CloudWatch's Smithy model, which is where the JSON and CBOR protocols get the shape name. |
| SSM `DescribeParameters` | `InvalidNextToken` / 400 | The specified token isn't valid. | **Published**, on the operation's own page. |
| SSM `GetParametersByPath` | `InvalidNextToken` / 400 | The specified token isn't valid. | **Published**, identically. |
| S3 `ListObjectsV2` | `InvalidArgument` / 400 | The continuation token provided is incorrect. | **Substrate's reading.** `API_ListObjectsV2` publishes exactly one error, `NoSuchBucket` at 404, and says nothing about an unusable `continuation-token`; the S3 `ErrorResponses` page returns an empty body to automated fetches. It follows the choice already recorded for `ListBuckets`, and the two operations taking this cursor now decode it through one helper, so they cannot refuse it differently. |
| Resource Groups Tagging `GetResources` | `InvalidParameterException` / 400 | PaginationToken is not a token this API issued | **Substrate's reading**, added by #1010 as the fifth operation of this class. `API_GetResources` publishes `PaginationTokenExpiredException` as well, and it is deliberately *not* the code used here: an unissued token is malformed, not expired, and the two imply different consumer actions. `InvalidParameterException`'s own gloss covers "a provided string parameter is malformed". |

Two rules decide what counts as issuable, and both are stated once, in
`emulator/offset_pagination_token.go`, rather than agreed on at each site:

- **A token is issuable if the encoder could have produced it.** For the three offset cursors that
  means base64 whose decoded text is exactly what `strconv.Itoa` renders for the offset it names, so
  `+5`, `05` and a trailing space are refused as well as a negative offset and a non-integer — forms
  `strconv.Atoi` accepts but nothing ever emitted. For `ListObjectsV2` the cursor is a *key*, so any
  base64 is issuable and only an undecodable token is refused: a token naming an object that has
  since been deleted resumes after it, which is the same reading the RDS and ElastiCache marker
  records.
- **An offset past the end of the listing is not refused.** That token *was* issued, over a listing
  that has since shrunk, and it clamps to a final empty page. Refusing it would break a legitimate
  walk whose records were deleted mid-loop.

**The token is validated before any state is read.** Three of the four read first — CloudWatch
loaded its alarm index and Systems Manager its parameter paths — so a refusal could depend on how
much state happened to exist. This is the ordering rule `ec2_describetags.go` records for
`DescribeTags`, and it is asserted by sealing the state store against reads and requiring the
refusal to arrive anyway. `ListObjectsV2` is the one exception, deliberately: its
bucket-existence `404` keeps its precedence over the token refusal, because the bucket is the
resource the request addresses and AWS publishes nothing about which of the two wins. Only the
object listing is guaranteed unread. `GetResources` follows the same rule: its whole eight-member
validation runs ahead of the account-wide scan, so a refusal does not depend on how many resources
happen to exist.

**What this does not fix.** CloudWatch `DescribeAlarms` and both Systems Manager listings still page
by *offset*, so a record added or removed behind the cursor still shifts every later page — the
other half of the tier-2 defect, and the reason the RDS and ElastiCache describes were converted to
a value-based cursor. Refusing an unissued token and choosing a stable cursor basis are independent
defects, and only the first is #915.

## The same idiom at fifteen more sites, and why four services went first

[#1086](https://github.com/scttfrdmn/substrate/issues/1086) found the pre-#915 form at fifteen
further sites across nine services. The decode is mechanical — the helpers above already exist — but
**the refusal is not, because the services do not share a code**, and that is what decides the
order the sites move in rather than the size of each edit.

Four services, five sites, went first — three of them because they publish a code that names this
condition, and CloudFormation because the tree carried a recorded decision at its site that had to
be resolved one way or the other before the class could move at all:

| Operation | Code | Provenance |
|---|---|---|
| KMS `ListKeys` | `InvalidMarkerException` / 400 | **Published**, in the operation's own Errors section, glossed "the request was rejected because the marker that specifies where pagination should next begin is not valid". KMS is also the only one of the four that constrains the token itself — `Marker` is Length 1–1024, Pattern `[\u0020-\u00FF]*` — so a marker outside that range is refusable on the page's own terms as well. Substrate's issuability rule is the stricter test, and every string it refuses that is *inside* the Pattern is still a string KMS could not have minted. |
| KMS `ListAliases` | `InvalidMarkerException` / 400 | **Published**, identically. Asserted separately from `ListKeys` because the two decoded their markers with two copies of one block, which is how they came to carry the defect twice. |
| Secrets Manager `ListSecrets` | `InvalidNextTokenException` / 400 | **Published**, and published *separately* from `InvalidParameterException`, which is the whole point: the page's Errors list is four entries and carries a code for the token and a code for a member, so answering `InvalidParameterException` here would tell a caller that the value of some member was wrong when AWS has a code for exactly this condition. `NextToken` is Length 1–4096, which substrate's token is always inside. |
| EventBridge `ListRules` | `InvalidToken` / 400 | **Published in prose only.** The Errors section is `InternalException`/500 and `ResourceNotFoundException`/400, and `InvalidToken` is absent from EventBridge's common-errors page too — but the `NextToken` member's own description says *"Using an expired pagination token results in an HTTP 400 InvalidToken error."* The code, the status and the condition are all the page's, in the description of the very member being refused. **This corrects #950's sweep of EventBridge**, which read Errors sections and so settled for the common-errors fallback for this condition. AWS names an *expired* token and substrate's tokens do not expire; what substrate refuses is the other way a token fails to be honourable, and both are one observation for a caller — a token this service will not resume from. |
| CloudFormation `DescribeStackEvents` | `ValidationError` / 400 | **Substrate's reading.** The Errors section is literally empty, so the code comes from CloudFormation's Common Errors page, which carries `ValidationError` and `InvalidParameterValue` both at 400. `ValidationError` is what this plugin already answers for every other malformed parameter, so a caller sees one code per class of mistake rather than a second code invented for this member. |

**This reverses a decision recorded in the tree, rather than deleting it.** `cfn_events.go` argued
the silent page-one answer *from* that empty Errors section, and a test asserted it with the
rationale attached. Two things overturn the argument. The empty section is not the whole of what
CloudFormation publishes — the common page is also published, and this plugin has a house rule for
choosing between its two 400s. And a well-formed page one is the one wrong answer a paginating
caller cannot detect, which is the finding the whole class rests on. The reversal also fixes a
second defect at that site for free: its guard required the offset to be `< len(events)`, so a
token substrate itself had issued, over a listing that has since shrunk, **reset to page one**
instead of clamping to a final empty page — the exact behaviour the old comment claimed to be
preserving.

**All five validate the token before they read any state.** Four of the five loaded their index
first, so a refusal could depend on how much state happened to exist and a store failure would be
reported as a 500 for a request that was already refusable. Asserted the way #915's sites assert it:
by sealing the state store against reads and requiring the refusal to arrive anyway.

**The other ten sites, and what their pages do not say.** Thirteen operation references were read for
the remaining five services, and **not one publishes a code that AWS attributes to an invalid,
unusable or expired pagination token** — in an Errors section or in the token member's prose. So each
of those ten refuses under a code that is substrate's reading of a generic code published on the
operation's own page, the way S3 `ListObjectsV2` and `GetResources` above already do, and none by
borrowing a sibling operation's code, which
[#671](https://github.com/scttfrdmn/substrate/issues/671) forbids. All ten have landed — SNS's two
sites, Athena's two, CloudWatch Logs' four, EventBridge Scheduler's one and Batch's one, which alone
carries three operations — and each has its own section below. **The class is closed: no listing in
the tree now answers page one to a token it could not have issued.**

What the ten have in common, and what the five sections record separately, is that the *condition* is
substrate's reading in every case while the *code* is never borrowed. Four of the five services
publish the where-a-token-comes-from sentence in the token parameter's own description — Athena,
CloudWatch Logs, EventBridge Scheduler and Batch — and that sentence, not an Errors entry, is the
footing each refusal stands on. SNS is the one that publishes no such sentence, so its section carries
the weaker argument, which is why it says so.

## SNS's three listings refuse a token under the code their own pages publish

`ListTopics`, `ListSubscriptions` and `ListSubscriptionsByTopic` are the first three of those ten
sites to convert. All three answer **`InvalidParameter` / 400**, which every one of the three pages
publishes in its **own** Errors section, glossed *"Indicates that a request parameter does not comply
with the associated constraints."* The code is therefore the operation's own vocabulary, not a
sibling's. What is substrate's reading is the **condition**: each page describes `NextToken` in a
single sentence — *"Token returned by the previous `ListTopics` request."* — with no constraint, no
length, and no error attributed to it, and SNS's common-errors page says nothing about a token
either. So "a token no previous call returned" is substrate's reading of that gloss, and the message
says which parameter and which operation rather than restating it, because a caller handed *"does not
comply with the associated constraints"* cannot tell which of its parameters AWS means:

```
NextToken is not a token returned by a previous ListTopics request
```

Two things this does **not** claim. It is not an expiry refusal: SNS publishes no lifetime for a
`NextToken` and substrate's tokens do not expire, so what is refused is the other way a token fails to
be honourable — one this service could not have minted. And it does not make a token portable between
the three: all three encode an offset the same way, so `ListTopics`' token decodes cleanly under
`ListSubscriptions` and would index into the wrong listing. That is now refused, which is why the
message names the operation.

**The page size is a constant 100, not a default.** All three pages say *"Each call returns a limited
list of topics, up to 100"* and none publishes a request parameter that can change it, so the listing
in the tests is 101 records — the smallest one that makes the operation issue a token at all.

**Two of the three validate the token before they read any state; the third does so deliberately
later.** `ListSubscriptionsByTopic` resolves the topic first, so the `NotFound`/404 that
[#926](https://github.com/scttfrdmn/substrate/issues/926) added for an absent topic keeps its
precedence over a token refusal — the topic is the resource the request addresses, which is the same
reading S3 `ListObjectsV2` records for its bucket, and AWS publishes nothing about which of the two
wins. The token is still decoded before the subscription index is read, and that boundary is asserted
by sealing **that one state key** rather than every read: sealing every read would fail the topic
lookup instead and prove nothing about the token.

## Athena's two listings refuse a token, and the page says where a token comes from

`ListQueryExecutions` and `ListWorkGroups` are the next two of those ten sites to convert. Both answer
**`InvalidRequestException` / 400**, which both pages publish in their **own** Errors sections, glossed
*"Indicates that something is wrong with the input to the request. For example, a required parameter may
be missing or out of range."* — and which this plugin already answers at five other doors, so it is
Athena's own vocabulary rather than a sibling's.

**Athena has the best footing of the five services in this class, because its page says where a token
comes from.** Both operations describe `NextToken` — for the request parameter and again for the
response element — as *"A token generated by the Athena service that specifies where to continue
pagination if a previous request was truncated. To obtain the next set of pages, pass in the `NextToken`
from the response object of the previous page call."* A token no previous response carried is therefore
not the thing the parameter is documented to accept. What is still substrate's reading is only that
this input problem is the one the `InvalidRequestException` gloss covers: neither page publishes a code
AWS attributes to a pagination token, and Athena's common-errors page carries nothing about one either.

```
NextToken is not a token returned by a previous ListWorkGroups request
```

**The published Length constraint is enforced at both ends.** `NextToken` is Length 1–1024 on both the
request and the response, and the encoder emits an offset in decimal base64 — a handful of characters —
so every token longer than 1024 already fails the issuability round trip. The minimum of 1 needs a
check of its own, because the round trip reads an empty string as the start of the listing: an absent
`NextToken` is that start, and an explicit `"NextToken": ""` is refused with the same code (#1185).

**A token from the other Athena listing is issuable and is not refused.** Both encode an offset the same
way, so `ListQueryExecutions`' token decodes cleanly under `ListWorkGroups`. Nothing in the token can
distinguish them; what the refusal contributes is the operation name in the message, so a caller that
crossed the two listings can see which one a token belongs to. The same shape is still live at Batch,
whose three describes share one paginator.

**The two page sizes have two ranges, and each is enforced
([#1185](https://github.com/scttfrdmn/substrate/issues/1185)).** `ListQueryExecutions` publishes
`MaxResults` Valid Range **0–50** and `ListWorkGroups` **1–50**; the two shared one preamble that
rewrote anything at or below zero to 50 and honoured anything above. Now a value outside the operation's
own range is `InvalidRequestException`/400, which both pages gloss as "a required parameter may be
missing or out of range". An absent `MaxResults` is still 50. A `MaxResults` of **0** on
`ListQueryExecutions` is legal and is honoured as written: an empty `QueryExecutionIds` and **no**
`NextToken`, so the walk ends. A token resuming at the same position would never advance, and a
paginator following it would ask for zero items forever. The response publishes
`QueryExecutionIds` with "Array Members: Minimum number of 1 item", which a zero page cannot meet;
substrate reads that minimum as describing a non-empty page rather than forbidding the empty list it
already answers for a workgroup with no queries. An explicit empty `NextToken` (`""`) is refused for
the published minimum length of 1, while an absent one is still the start of the listing.

**Both validate the token before they read any state, and that is counted rather than sealed.** Neither
operation resolves a resource first, so neither has a `NotFound` whose precedence has to be preserved —
`ListQueryExecutions`' `WorkGroup` is a filter, not a lookup. But the sealed-store assertion the other
sites use cannot tell Athena's two orderings apart: its index loader reports a store failure as an empty
index, so a sealed read answers `200` with an empty page, which is indistinguishable from an empty
listing. The test counts the reads made in Athena's namespace instead and requires a refused request to
have made none, with a paired control — a token the operation could have issued — that must read the
index, so a count of zero cannot pass by never reaching the handler.

## CloudWatch Logs carried one block four times, and the page says where a token comes from

`DescribeLogGroups`, `DescribeLogStreams`, `GetLogEvents` and `FilterLogEvents` are the next four of
those ten sites to convert, and they are the largest concentration in the class: **four copies of one
decode block in one file**, which is why the conversion is worth more here than the count of
operations suggests — a fix applied to one of them would have left the other three answering page one.
All four answer **`InvalidParameterException` / 400**, glossed *"A parameter is specified
incorrectly."* and published in **all four** operations' own Errors sections, so it is each
operation's own vocabulary rather than a sibling's; it is also already this plugin's code at every
other door it has.

**The footing is the parenthesis in the token's own description.** All four pages describe the request
parameter identically — *"The token for the next set of items to return. (You received this token from
a previous call.)"*, with `FilterLogEvents` saying "events" where the others say "items". That aside
states where a token comes from, so a token no previous call returned is not what the parameter is
documented to accept. What is still substrate's reading is only that this input problem is the one the
`InvalidParameterException` gloss covers: no page publishes a code AWS attributes to a pagination
token.

```
nextToken is not a token returned by a previous DescribeLogGroups request
```

**Nothing in the published constraints bounds the token.** `nextToken` is Length **minimum 1 with no
maximum** and no Pattern, so unlike Athena's and KMS's 1–1024 there is no ceiling for the refusal to
subsume, and the issuability round trip is the entire rule. The minimum of 1 is again why an empty
`nextToken` is an *absent* token, the start of the listing, rather than an invalid one.

**Here the page sizes are published, and substrate's match.** The two describes publish *"If you don't
specify a value, the default is up to 50 items"* with a Valid Range of 1–50; `FilterLogEvents`
publishes *"The default is 10,000 events."* and `GetLogEvents` *"the default is as many log events as
can fit in a response size of 1 MB (up to 10,000 log events)"*, both with a Valid Range of 1–10000.
Substrate applies 50 and 10,000, which is the published figure in each case — unlike Athena, where the
default was substrate's own choice. It models no response-size ceiling, so `GetLogEvents` applies the
count alone.

**One thing this does not fix, named so the conversion is not read as having fixed it.** All four
response members publish *"The token expires after 24 hours."* and no page publishes a code for
presenting an expired one, so a token substrate issues stays valid for the life of the store —
refusing an expired one would need the token to carry its issue time and the simulated clock to judge
it, and would have to attribute the refusal to `InvalidParameterException`/400, the only 400-class code
these pages publish for a bad parameter. That attribution is a reading rather than a read, so the expiry
is **declined** rather than deferred: substrate's pagination tokens do not expire, and a test that needs
an expired-token path cannot get one here.

Two more were recorded here and have since been fixed. `ResourceNotFoundException` had no site at three
of these four doors, so a listing over a log group that does not exist was empty rather than refused —
see [a log group that does not exist is not an empty log group](#a-log-group-that-does-not-exist-is-not-an-empty-log-group)
for the refusal and for how it orders against the token. And `GetLogEvents`' pair of directional tokens
is now reported in full — see
[GetLogEvents pages by a pair of tokens](/reference/logs#getlogevents-pages-by-a-pair-of-tokens), which is also where
the one way the four now differ is set out: `GetLogEvents` prefixes its tokens with a direction, so its
tokens no longer decode under the other three and theirs no longer decode under it.

**All four validate the token before they read any listing**, asserted by sealing the store the way
#915's sites are; unlike Athena, Logs' index loader propagates a store failure, so the seal can tell
the two orderings apart. `DescribeLogGroups` has nothing ahead of its decode and seals every read; the
other three seal the *listing's* key alone — the stream-name index at two of them, the events key at
`GetLogEvents` — each with a control call proving the sealed key is reached, because sealing every read
would fail the group lookup that now precedes the token and assert nothing about it. Three of the four
require a member first — `logGroupName`, and
`logStreamName` as well at `GetLogEvents` — and that refusal keeps its precedence, because an absent
required member is the more basic failure and neither ordering is published. Both carry the same
published code, so the message is the only thing that says which one answered, which is why the
precedence is asserted rather than left to the reader. Those same three now also resolve the log group
(and the log stream, at `GetLogEvents`) ahead of the token, for the reason SNS `ListSubscriptionsByTopic`
resolves its topic first; the token is still decoded before the *listing* is read, which is what #887's
criterion asks.

## EventBridge Scheduler's one listing refuses a token the parameter is not documented to accept

`ListSchedules` is the ninth of those ten sites, and the only one whose token travels in a **query
string** rather than a request body. It answers **`ValidationException` / 400**, which the operation's
own Errors section publishes, glossed *"The input fails to satisfy the constraints specified by an AWS
service."* It is also the only refusal this service publishes for an input that fails a constraint, so
the message carries which input failed, in the shape every other Scheduler refusal renders:

```
1 validation error detected: Value at 'nextToken' failed to satisfy constraint: Member must be a token
returned by a previous ListSchedules request
```

**The footing is the request parameter's own sentence, not the Errors section.** `API_ListSchedules`
describes `NextToken` as *"The token returned by a previous call to retrieve the next set of
results."* — the page states where a token comes from, so a token no previous call returned is not the
thing the parameter is documented to accept. What remains substrate's reading is only that this input
problem is the one the `ValidationException` gloss covers: the page publishes no code AWS attributes
to a pagination token. The published Length constraint of **1–2048** is subsumed by the issuability
round trip, since a base64 encoding of a decimal offset is far shorter than 2048 bytes and a
length refusal would be unreachable behind it; the minimum of 1 is again why an empty `NextToken` is an
*absent* token, the start of the listing, rather than an invalid one.

**The token is decoded before the name index is read.** There is no required member on this operation
and no schedule to resolve, so unlike the three Scheduler operations that check `Name` first, nothing
competes with the refusal for precedence. Asserted by sealing the state store the way #915's sites
are; Scheduler's index loader propagates a store failure, so the seal can tell the two orderings
apart.

**Two things this does not claim, and two it does not fix.** It refuses a token this service could not
have minted; it does **not** make a token portable between two listings of different shape, because
the token carries an offset and nothing else, so a token issued for one `ScheduleGroup`, `NamePrefix`
or `State` decodes cleanly against another and indexes into a listing the caller never asked for —
a property every offset paginator in the tree has, recorded here because the refusal's name invites
the stronger reading. And a past-the-end offset still clamps to a final empty page rather than being
refused, because a token substrate issued over a listing that has since shrunk is still a token it
issued. The `State` filter was also left as it was — applied **after** the page is cut, so a
state-filtered request could be answered a page shorter than `MaxResults` while still carrying a
`NextToken` — and has since been fixed under
[#1229](https://github.com/scttfrdmn/substrate/issues/1229); see
[the next section](#a-published-filter-selects-what-the-page-is-cut-from). Still left as it was: a
`MaxResults` above the published maximum of 100 is clamped rather than refused, with a value of zero or
below silently ignored in favour of substrate's own default of 20. That is a separate class from the
token, and changing it changes which schedules a page contains rather than which tokens are accepted.

## A published filter selects what the page is cut from

`ListSchedules` read its three filters in three different places relative to the cut. `ScheduleGroup`
chose which name index to load and `NamePrefix` filtered that index — both ahead of the cut — but
**`State` was applied inside the render loop**, on the records the page had already selected. So a
state-filtered request was answered `MaxResults` schedules minus however many of *that page* failed the
filter, while the `NextToken` alongside had been computed from the unfiltered listing
([#1229](https://github.com/scttfrdmn/substrate/issues/1229)).

`API_ListSchedules` publishes `State` in the same URI-parameter list as the other two and in the same
words — *"If specified, only lists the schedules whose current state matches the given filter."*
against `NamePrefix`'s *"Schedule name prefix to return the filtered list of resources."* — so it is a
filter on the listing, and the listing is what a page is cut from. The response member is the ordinary
cursor: *"Indicates whether there are additional results to retrieve. If the value is null, there are
no more results."* Nothing on the page says a page may be shorter than the `MaxResults` it was asked
for.

Two observations changed, the second worse than the first. `?State=ENABLED&MaxResults=20` over a group
of 40 where half are `DISABLED` answered fewer than 20 enabled schedules with a `NextToken`, where AWS
answers 20. And when every schedule on a page failed the filter, the answer was
`{"Schedules":[],"NextToken":"…"}` with matches still to come — a shape a caller that stops at an empty
list reads as "the filter matched nothing". AWS can produce that shape too, but from server-side scan
limits, never from applying a filter it publishes.

**The cost is a `state.Get` per schedule in the group, per call**, because `State` lives in the record
and not in the name index, so a state-filtered listing cannot be assembled from names alone. That is
the deliberate reading: the alternative is to copy `State` into the name index so the filter stays
index-only, which would duplicate a field of the record into an index and leave `UpdateSchedule` with
two places to write it — the class [#756](https://github.com/scttfrdmn/substrate/issues/756) exists
for.

**What is unchanged.** The offset now counts *matching* schedules, so a token issued with a `State`
filter and replayed without one indexes into a different listing — already true of `NamePrefix`, and
already recorded above as a property of every offset paginator in the tree. And a name the index holds
with no record behind it, or a record that will not decode, is still skipped: those are store
inconsistencies rather than published filters, there is no request a caller could send to produce
either, and the page publishes no code for substrate's own index and records disagreeing, so a refusal
would be inventing one. What changed is that skipping one can no longer shorten a *page* — it shortens
the listing being cut from, exactly as a filter does, and a page is short only when the listing has run
out.

## A log group that does not exist is not an empty log group

`DescribeLogStreams`, `GetLogEvents` and `FilterLogEvents` each answered **HTTP 200 with an empty
listing** for a log group with no record, so "this group is empty" and "this group was never created"
were the same response — and a consumer waiting for a Lambda's first log line could not tell a
still-warming stream from a name it had spelled wrong. All three now answer
**`ResourceNotFoundException` / 400**:

```
The specified log group does not exist: /aws/lambda/my-function
```

**The code and the status are published on each page, and the status is not a typo.** Each of
`API_DescribeLogStreams`, `API_GetLogEvents` and `API_FilterLogEvents` lists
`ResourceNotFoundException`, glossed *"The specified resource does not exist."*, at **HTTP Status Code
400**, not the 404 the code's name suggests. `API_DescribeLogGroups` is the exception and is left
alone: its Errors section publishes `InvalidParameterException` / 400 and `ServiceUnavailableException`
/ 500 and no not-found at all, so a `logGroupNamePrefix` matching nothing is a legitimately empty
listing there, and refusing it would be inventing a code the page does not carry.

**So the fix also moved four refusals that already existed.** `DeleteLogGroup`, `CreateLogStream`,
`DeleteLogStream` and `PutLogEvents` answered the same `ResourceNotFoundException` at **404**, and each
of their pages publishes 400. One code cannot keep two statuses in one service without a consumer
matching on status seeing a difference AWS does not have, so all seven sites now go through two
helpers and cannot drift. `GetLogEvents` also resolves the **stream**, since its `logStreamName` is
Required: Yes and a stream is what it reads; the group is checked first, because a caller told the
stream is missing would create it and be refused again.

**The one status this left as it was** has since been fixed too. `CreateLogStream`'s and
`CreateLogGroup`'s `ResourceAlreadyExistsException` answered **409** where both pages publish 400, and
now answer 400 ([#1251](https://github.com/scttfrdmn/substrate/issues/1251)). That fix surveyed every
status the plugin answers; see [Every refusal answers the status its page
publishes](/reference/logs#every-refusal-answers-the-status-its-page-publishes).

**Against the token refusal above, the resource wins.** No page publishes the order, so the reading is
the one SNS `ListSubscriptionsByTopic` already records: a token is a continuation of a listing over the
resource the request addresses, and there is no listing to continue when the resource does not exist.
The token is still decoded before the *listing* is read, which is all #887's criterion asks — a refusal
must not depend on how much state happens to exist, and existence of the addressed resource is not
"how much". The absent-required-member refusal keeps the front of the sequence, since a request naming
no group names no resource to resolve. All three orderings are asserted, in both directions, because
every refusal involved answers 400 and the code is the only thing that says which one won.
`logStreamNames` on `FilterLogEvents` is a filter and not the addressed resource, so a name in it with
no stream behind it is still not refused.

## Batch's three describes shared one paginator, so the decode had to leave it

`DescribeComputeEnvironments`, `DescribeJobQueues` and `DescribeJobDefinitions` are the last of those
ten sites, and the only one where three operations paginate through **one shared helper** — which is
why one conversion covers three operations, and why the fix had to move the decode *out* of the helper
rather than correcting it in place.

All three answer **`ClientException` / 400**, glossed *"These errors are usually caused by a client
action. … Another cause is specifying an identifier that's not valid."* and published in all three
operations' own Errors sections. Each Batch page publishes exactly two errors — `ClientException`/400
and `ServerException`/500 — and Batch publishes no common-errors page, so those two are the whole
published vocabulary and there is nothing a refusal here could borrow from a sibling:

```
nextToken is not a token returned by a previous DescribeComputeEnvironments request
```

**The footing is each page's own description of the parameter, and it names the operation.** All three
say *"The `nextToken` value returned from a previous paginated `Describe…` request where `maxResults`
was used and the results exceeded the value of that parameter."*, each naming its own operation as the
source. The same paragraph adds *"Treat this token as an opaque identifier that's only used to retrieve
the next items in a list and not for other programmatic purposes."*, which is addressed to the caller
rather than to the service and so is not itself a refusal rule — it is the page's own statement that
the token's contents are not a caller-constructible value. What remains substrate's reading is only
that a token is one of the identifiers the `ClientException` gloss covers: the page never joins the two
sentences. No page publishes a Length or Pattern constraint on `nextToken`, so unlike KMS's 1–1024
there is no ceiling for the refusal to subsume and the issuability round trip is the entire rule.

**Why the decode left the shared helper.** `DescribeJobDefinitions` loads the job-definition index
*before* it reaches the helper, to expand a `jobDefinitionName` into its revisions, so a decode inside
the helper would have sat below a state read on exactly one of the three and only for one shape of
request. Moving it to each handler makes the refusal unconditional, which is #887's criterion, and the
state-store seal asserts it for all three rather than for two-and-an-argument. The body decode keeps
its precedence: a request whose JSON does not parse cannot have a token read out of it, and since both
refusals carry the same published code, the message is the only thing that distinguishes them.

**Tokens are not portable between the three, and that is asserted rather than implied.** The token
carries an offset and nothing else, so a `DescribeJobQueues` token is well-formed at
`DescribeComputeEnvironments` and indexes into a listing the caller never asked for. The message names
the operation for that reason, and a test pins the non-portability so the naming is not mistaken for
enforcement. A past-the-end offset still clamps to a final empty page, because a token substrate issued
over a listing that has since shrunk is still a token it issued.

**Two things this does not fix.** `maxResults` outside the published range of **1–100** is clamped
rather than refused — all three pages publish *"If this parameter isn't used, then `Describe…` returns
up to 100 results"*, so 100 is the published default for an absent value and applying it to a zero or
negative one as well is substrate's reading. And `DescribeJobDefinitions` applies its `status` filter
after the page is cut, which is recorded in the tree as substrate's reading of the page's ordering and
is unchanged here.

`ListJobs` was recorded here as a third, because it published `maxResults` and `nextToken` and
implemented neither. It is the fourth caller of the same decode as of
[#1236](https://github.com/scttfrdmn/substrate/issues/1236) — see
[ListJobs reads its request](/reference/batch#listjobs-reads-its-request), which is also where the rest of that
operation's members are argued.

## Six describes published a cursor and implemented none of it

[#916](https://github.com/scttfrdmn/substrate/issues/916). The RDS and ElastiCache work above
fixed the three describes that *had* a cursor. Six more publish `Marker` and `MaxRecords` and
implemented neither: RDS `DescribeDBSnapshots`, `DescribeDBSubnetGroups` and
`DescribeDBParameterGroups`, and ElastiCache `DescribeReplicationGroups`,
`DescribeCacheSubnetGroups` and `DescribeCacheParameterGroups`. Each answered its entire listing,
emitted no `Marker`, and discarded a `MaxRecords` the caller sent.

Of the three states a published parameter can be in — implemented, absent and refused, or
accepted and ignored — the third is the worst, and it is the one all six were in:

- **A paging loop is dead code until it runs against AWS.** A consumer that walks until the
  `Marker` comes back empty completes on the first response here, so the loop is never exercised;
  the first time it executes is against real AWS, over a listing long enough to page, with no test
  covering it.
- **A `Marker` from anywhere else restarted the listing.** With nothing decoding the parameter, a
  token persisted across a restart, copied from another operation, or left over from a recording
  was answered with the whole listing again rather than refused.

All six now go through the same three helpers as the first three — `parseQueryMarker`,
`queryMaxRecords` and `queryMarkerPage` — so nine operations share one cursor, and the semantics
cannot diverge between them by construction rather than by nine sites agreeing.

**The refusal code is published for three of the nine, and the split is not the family
boundary.** This is a correction to #916's own acceptance criteria, which claimed the code was
published for ElastiCache and unpublished for RDS:

| Page | `InvalidParameterValue` / 400 | Published errors |
|---|---|---|
| `API_DescribeCacheClusters` | **published** | ElastiCache's cluster faults, plus `InvalidParameterValue` and `InvalidParameterCombination` |
| `API_DescribeReplicationGroups` | **published** | `ReplicationGroupNotFoundFault` / 404, `InvalidParameterValue` / 400, `InvalidParameterCombination` / 400 |
| `API_DescribeCacheParameterGroups` | **published** | `CacheParameterGroupNotFound` / 404, `InvalidParameterValue` / 400, `InvalidParameterCombination` / 400 |
| `API_DescribeCacheSubnetGroups` | **absent** | `CacheSubnetGroupNotFoundFault` / 400 only |
| `API_DescribeDBInstances` | **absent** | `DBInstanceNotFound` / 404 only |
| `API_DescribeDBClusters` | **absent** | its own NotFound fault only |
| `API_DescribeDBSnapshots` | **absent** | `DBSnapshotNotFound` / 404 only |
| `API_DescribeDBSubnetGroups` | **absent** | `DBSubnetGroupNotFoundFault` / 404 only |
| `API_DescribeDBParameterGroups` | **absent** | `DBParameterGroupNotFound` / 404 only |

`API_DescribeCacheSubnetGroups` is the case that matters: it is an ElastiCache page that publishes
no `InvalidParameterValue`, so "ElastiCache publishes it, RDS does not" is not a rule the
provenance can rest on. Three of nine are **published** and six are **substrate's reading**, and
which is which has to be stated per page.

The `MaxRecords` range is published identically on all nine — `Default: 100` with a minimum of 20
and a maximum of 100 — though not in identical words: the RDS pages write
`Constraints: Minimum 20, maximum 100.` and the ElastiCache pages
`Constraints: minimum 20; maximum 100.` The numbers are what one shared range depends on, and they
agree. Both parameters are validated before any state is read, so a request substrate cannot serve
is refused rather than answered with page one.

**A single-resource filter that lands past page one still answers its record.** Every one of the
six publishes one — `DBSnapshotIdentifier`, `DBSubnetGroupName`, `DBParameterGroupName`,
`ReplicationGroupId`, `CacheSubnetGroupName`, `CacheParameterGroupName` — and filtering plus
paging is the ordering trap the issue names: a filter applied *after* a page is cut would answer
an empty page, or a `NotFound` fault, for a record that exists and merely sorts late. It cannot
happen here, because the filter runs inside `queryMarkerPage`'s record callback and a non-matching
record consumes no page slot: a filtered listing is one record long however deep into the
unfiltered listing the record sits. `DescribeReplicationGroups` was the one of the six answering a
`NotFound` fault when this landed — all six do since
[#1020](https://github.com/scttfrdmn/substrate/issues/1020) — and its fault is therefore decided on
a page that cannot be empty for a group that exists. Both properties are asserted rather than left
to the reasoning.

`DescribeDBSnapshots` is the one of the six with more than one filter to order against — it also
publishes `DBInstanceIdentifier`, `DbiResourceId`, `SnapshotType`, `IncludePublic`,
`IncludeShared` and `Filters.Filter.N` — and both of its sibling RDS group describes publish
`Filters.Filter.N` as **"Not currently supported"**, a parameter AWS declares and refuses to
honour, so substrate ignoring it matches the page rather than diverging from it.

**What this did not fix.** Five of the six published a NotFound fault their handler did not
answer: a filtered request naming a resource that does not exist got an empty `200` instead of
`DBSnapshotNotFound` / 404, `DBSubnetGroupNotFoundFault` / 404, `DBParameterGroupNotFound` / 404,
`CacheSubnetGroupNotFoundFault` / 400 or `CacheParameterGroupNotFound` / 404. That is a defect
about a request's *result*, not about how a listing is cut into pages, so it was
[#1020](https://github.com/scttfrdmn/substrate/issues/1020) rather than part of that change — the
same reason `MaxRecords` was kept out of the cursor fix above. It is fixed below.

## A single-resource filter that names nothing answers the published fault

[#1020](https://github.com/scttfrdmn/substrate/issues/1020). Each of the six describes above
publishes exactly one single-resource filter and a NotFound fault to go with it, and five of the
six answered an empty `200` for a filter that matched nothing. A consumer's error path for a
resource that has been deleted — the branch every retry and every "create if absent" depends on —
was therefore dead code that first executed against real AWS.

The fault belongs to the **filter**, not to the listing. Every gloss says so, naming the parameter
rather than the collection: "`DBSnapshotIdentifier` doesn't refer to an existing DB snapshot",
"`DBSubnetGroupName` doesn't refer to an existing DB subnet group", "The requested cache subnet
group name does not refer to an existing cache subnet group". So an unfiltered listing with no
records is still an empty `200`, which is asserted separately — a check written as "the page came
back empty" would refuse the first call a fresh emulator serves.

| Page | Code | Status |
|---|---|---|
| `API_DescribeDBSnapshots` | `DBSnapshotNotFound` | 404 |
| `API_DescribeDBSubnetGroups` | `DBSubnetGroupNotFoundFault` | 404 |
| `API_DescribeDBParameterGroups` | `DBParameterGroupNotFound` | 404 |
| `API_DescribeReplicationGroups` | `ReplicationGroupNotFoundFault` | 404 |
| `API_DescribeCacheSubnetGroups` | `CacheSubnetGroupNotFoundFault` | **400** |
| `API_DescribeCacheParameterGroups` | `CacheParameterGroupNotFound` | 404 |

Nothing in that table is substrate's reading; each row is its own page's Errors section. Two
disagreements in it are AWS's and are reproduced rather than tidied: three codes carry a `Fault`
suffix and three do not, and the ElastiCache subnet-group fault is **400** where the other five are
404. A sweep that made either uniform would break a consumer matching on the code, which is why
both are asserted per operation rather than derived from a shared constant.

All six now go through one helper, `queryMarkerFilterNotFound`, including
`DescribeReplicationGroups`, which already answered its fault inline. One condition in one place is
the same argument the shared cursor rests on, and it is what keeps the six from drifting into six
readings of one sentence.

**Pagination cannot make the fault fire for a record that exists**, and that is closed by
construction rather than by ordering two checks. The filter runs inside the record callback, so a
non-matching record consumes no page slot; at most one record can match a single-resource filter;
and the published minimum `MaxRecords` is 20. A filtered page therefore never truncates and never
carries a `Marker`.

One case is **substrate's reading**: a caller that sends both a `Marker` and a filter naming a
record at or before that marker gets the fault, because the cursor skipped the record. No page says
anything about combining a cursor with a single-resource filter, and substrate reads a `Marker` as
naming a position in the listing — so such a request asked about a stretch of records that does not
include its own, and the fault is the honest answer to the request as asked.

`DescribeDBSnapshots` is the one operation whose fault is narrower than "the filtered page is
empty", because it publishes two single-resource filters and a fault for only one of them.
`DBInstanceIdentifier` carries the constraint "if supplied, must match the identifier of an existing
DBInstance" and **no Errors entry**, so an instance that does not exist is an empty `200`. The case
that decides the implementation is a snapshot that exists under a *different* instance: the caller
named its snapshot correctly, so `DBSnapshotNotFound` would be a false statement, and substrate
tracks whether the snapshot identifier matched independently of the instance filter.

One AWS slip is recorded rather than followed: `API_DescribeDBParameterGroups` constrains its
`DBParameterGroupName` to "the name of an existing **DBClusterParameterGroup**", which is a
different resource described by a different operation. The Errors gloss — "`DBParameterGroupName`
doesn't refer to an existing DB parameter group" — is the statement substrate implements.

## Two more cursors published and unread, outside EC2

[#917](https://github.com/scttfrdmn/substrate/issues/917) is mostly an EC2 change (see
[One offset paginator, shared](/reference/ec2#one-offset-paginator-shared)), but two operations in other services
were in the same state — both halves of a cursor published on the URI, and neither read:

- **Lambda `ListEventSourceMappings`**, whose URI publishes
  `?EventSourceArn={EventSourceArn}&FunctionName={FunctionName}&Marker={Marker}&MaxItems={MaxItems}`.
- **API Gateway `GetBasePathMappings`**, whose URI publishes
  `?domainNameId={domainNameId}&limit={limit}&position={position}`.

Each answered its whole listing with no token whatever the caller sent, so a paging loop terminated
on its first response — the divergence a caller cannot see, described in full above.

Both now cut their page through the same helper as each other, over the **base64** offset token the
CloudWatch, Systems Manager and S3 listings use rather than EC2's decimal one. A token substrate
could not have issued is refused rather than answered with page one, and refused **before any state
is read**; an offset past the end clamps to an empty final page. Those are the rules already stated
for [a token substrate never
issued](#a-pagination-token-substrate-never-issued-is-refused-not-answered-with-page-one), and this
change adds no new ones.

**The page-size rules are per operation, and neither resembles EC2's.**

| Operation | Absent page size | Out of range | Refusal |
|---|---|---|---|
| Lambda `ListEventSourceMappings` | **100** | `MaxItems` outside 1–10000 is refused, never clamped | `InvalidParameterValueException` / 400, which the page publishes |
| API Gateway `GetBasePathMappings` | **25**, the published default | `limit` outside 1–500 is refused | `BadRequestException` / 400, which the page publishes |

`MaxItems` publishes **two bounds that are two different rules**, and substrate keeps them apart:
`Valid Range: Minimum value of 1. Maximum value of 10000.` is what the parameter accepts, and "Note
that `ListEventSourceMappings` returns a maximum of 100 items in each response, even if you set the
number higher" is what a response may carry. So `MaxItems=5000` is a valid request that answers at
most 100 mappings with a `NextMarker`, while `MaxItems=10001` is refused. An **absent** `MaxItems`
is that same 100, which is *substrate's reading*: the page publishes no default and states the cap
against every response.

API Gateway is the opposite case, and the only paginated operations in the tree with a published
default: "The maximum number of returned results per page. The default value is 25 and the maximum
value is 500." A request naming no `limit` therefore still pages, at 25, where it used to answer the
whole collection. No **minimum** is published — the floor of one is *substrate's reading*, for the
reason the EC2 section gives: a page of zero elements describes a walk that answers nothing and
hands back a position forever. The same two sentences appear on all seven paginated v1 collections,
so that reading is applied once and shared; see [Six more v1 collections read the pair they
publish](#six-more-v1-collections-read-the-pair-they-publish).

**Neither operation gained an `InvalidParameterCombination`.** EC2's service-wide rule that an ID
list and `MaxResults` may not appear together has no counterpart on either page: Lambda's
`FunctionName` and `EventSourceArn` narrow a page rather than forbidding one, and importing the EC2
rule would be substrate inventing a refusal.

**The order each cursor counts positions in is *substrate's reading*, since neither page publishes
one.** `GetBasePathMappings` walks the state keys' lexicographic order, which is by base path within
the domain. `ListEventSourceMappings` has two code paths — a scan of the mapping keys when no
function is named, and a per-function index when one is — and the index holds mappings in creation
order, so it is sorted by UUID before the page is cut. Without that the same `Marker` would name two
different positions depending on whether the caller passed `FunctionName`.

**What the same audit found still unconverted is counted, not estimated.** Nine routed EC2 describes
published `MaxResults` and `NextToken` and read neither; #1024 converted them in three parts,
grouped by the range each page publishes, and each is named with its range in
[One offset paginator, shared](/reference/ec2#one-offset-paginator-shared). Six routed API Gateway v1 collections
were in that state too; all six were converted under #1025, two at a time — see the next section.
Lambda's `ListFunctions` is a third case of the narrower defect: it pages, but
accepts a `Marker` it never issued and range-checks no `MaxItems`.

## Six more v1 collections read the pair they publish

[#1025](https://github.com/scttfrdmn/substrate/issues/1025) is the section above applied to the rest
of API Gateway v1. Six collections besides `GetBasePathMappings` publish `limit` and `position` on
their URI and read neither, answering the whole collection with no cursor: `GetRestApis`,
`GetResources`, `GetDeployments`, `GetAuthorizers`, `GetApiKeys` and `GetUsagePlans`. All six carry
AWS's two sentences byte-identically, so this is one rule six times rather than six rules, and the
bounds, the default, the absent minimum and the `BadRequestException` / 400 to refuse with are read
in one place for all seven.

**All six now page**, converted two at a time. Each gained the request parameter its signature could
not previously reach — six of the eight v1 collection handlers took no request at all — which is why
this was three changes rather than one sweep. With `GetBasePathMappings`, every v1 collection whose URI
publishes the pair now reads it.

**The order each collection is paged in is *substrate's reading*, because no page publishes one, and
it is the order the collection already had.** These six are walked in **ascending element ID**: each
is built from a string index of IDs which is kept sorted as it is written, so the order is persisted
state rather than a map walk and is therefore stable between two reads and across a replay — the
obligation an offset cursor imposes. Two consequences are worth stating plainly, because both are
things a reader may expect not to hold:

- An ID is *generated*, not chosen by the caller, so the order is not one a caller can predict from
  its own inputs. It is only one it can rely on not to change under it.
- A REST API's **root resource is not first** in `GetResources`. Its ID is generated like any other
  resource's, so `/` falls wherever that ID sorts, and a first page of a large API need not contain
  it. Sorting by `path` instead would make a nicer collection to read and a worse emulator: it is not
  the order this operation answered in before it paged, and nothing published asks for it.
- `GetDeployments` is **not** newest-first, or oldest-first. A deployment ID is generated too, so the
  order carries no relation to the `createdDate` the element publishes. The same argument applies:
  sorting by `createdDate` would read better and emulate worse.

**Five further request parameters across three of the six are still unread, and they are a different
divergence.** `GetResources` publishes `embed`, whose only accepted value is `methods`, and substrate
answers every resource with its `resourceMethods` populated regardless. `GetApiKeys` publishes
`customerId`, `includeValues`, and the parameter documented as `nameQuery` but spelled **`name`** on the
query string — worth recording, since a later reader will grep for the documented name and find
nothing. `GetUsagePlans` publishes `keyId`. Every one of the five *narrows* what is reported, so
honoring one would over-report before and under-report after — the opposite direction from the cursor
defect these conversions fix, and not the rest of it. Narrowing a response a caller may already be
reading is a compatibility break that wants its own issue and its own citation.

**`GetApiKeys` publishes a third response member, `warnings`, and it is deliberately absent rather than
reported empty.** The page says it holds "a list of warning messages logged during the import of API
keys when the `failOnWarnings` option is set to true", and `failOnWarnings` belongs to `ImportApiKeys`,
which substrate does not route. So no call that can reach this handler could produce a warning and no
state could hold one; under [#1013](https://github.com/scttfrdmn/substrate/issues/1013)'s rule an
unmodelled member is omitted rather than sent empty, because `[]` would be a claim that the import ran
and warned about nothing.

## A tag set read back out of a map

[#946](https://github.com/scttfrdmn/substrate/issues/946) is the same defect one layer down, at
five operations the audit above could not reach: they build their response from a resource's own
`map[string]string` of tags rather than from `StateManager.List`. They are ACM's
`ListTagsForCertificate`, CloudFront's `ListTagsForResource`, Kinesis's `ListTagsForStream`, and
S3's `GetBucketTagging` and `GetObjectTagging`. Each ranged that map and appended, so the order on
the wire came from the map's hash seed and two identical reads of unchanged state could disagree.
All five now report their tags **sorted by key**.

Sorting `List` could not have fixed these, and neither could the four tag merge helpers
[#862](https://github.com/scttfrdmn/substrate/issues/862) sorted: the map is loaded whole from a
single state key, so no listing is involved. A rendering of a map *as a JSON object* was never
affected, because `encoding/json` sorts map keys itself — the defect is specific to a map
flattened into an ordered array or into a repeated XML element, where nothing sorted.

**Four of the five sit in tier 3 and one in tier 2.** ACM's page, CloudFront's, and both of S3's
publish neither a cursor nor an ordering statement, so lexicographic there is **substrate's
reading**, resting on the replay promise exactly as `ListBuckets`' order does. Kinesis's is tier 2:
`ListTagsForStream` states no order in prose either, but it publishes `ExclusiveStartTagKey` — "the
key to use as the starting point for the list of tags. If this parameter is set,
`ListTagsForStream` gets all tags that occur after `ExclusiveStartTagKey`" — and a tag cannot occur
*after* a key unless the tags are walked in some order over keys. *Which* order is still
substrate's reading, and AWS's own sample response is not sorted.

That cursor is implemented as of [#954](https://github.com/scttfrdmn/substrate/issues/954), and this
sort was its prerequisite for the reason the tier-2 defect above already demonstrated: a cursor
paged over an unstable order skips and repeats. See
[Paging the tags on a stream](/reference/kinesis#paging-the-tags-on-a-stream) for what a page contains and for the two
AWS sentences about `HasMoreTags` that do not agree.

## The two readers #946 could not see

[#1011](https://github.com/scttfrdmn/substrate/issues/1011) is the same defect at the two sites that
sweep missed, and it was found the way the others were not — by measuring rather than by reading.
Twelve identical `GetResources` calls against one eight-tag resource in one process produced **eight
distinct bodies**, all rotations of the sorted order, which is the signature of Go's randomised
map-range start offset over a map whose insertion order was already sorted.

The first site is the Resource Groups Tagging API's own `GetResources`, which #946 could not have
covered because it is not a per-service tag listing: it renders **every** service's tags, through four
shared converters in `tagging_plugin.go`. `mapToTaggingTags` is the load-bearing one — it has most of
the scanners as call sites and is the only one of the four that passes through a map at all, so the
other three (`iamTagsToTaggingTags`, `ec2TagsToTaggingTags`, `efsTagsToTaggingTags`) were
order-preserving by luck and it could not be. All four sort now, including the three that were already
fed a sorted slice by #862's merge helpers, so the guarantee does not rest on every writer remembering
to sort: a raw writer that bypasses a merge helper cannot make the response non-deterministic through
them. The four scanners that build the list by hand — KMS, SNS, Secrets Manager and Systems Manager —
already sorted, so the converters were the whole gap.

`GetResources` had exactly one sort before this, on `ResourceARN`, which made the *resource* order
deterministic and left the tag order inside each resource to the map. That is why a reader checking the
write path found nothing wrong: state on disk was ordered by #862, and the disorder was introduced on
the way out.

The second site is **ElastiCache's `ListTagsForResource`**, which is exactly the shape #946 swept and
was missed for a reason worth naming: all five of #946's operations answer JSON, and ElastiCache
flattens its `map[string]string` into a repeated **XML** element instead, so it did not match that
sweep's shape. The rest of the tree was swept for this pattern at the same time; every other site that
ranges a tag map either already sorts or writes into another map, where no order is observable.

Both sit in tier 3. `API_GetResources` says nothing about the order of `Tags` within a
`ResourceTagMapping`, and `API_ResourceTagMapping` describes `Tags` only as "the tags that have been
applied to one or more AWS resources"; ElastiCache's `API_ListTagsForResource` describes
`TagList.Tag.N` only as "A list of tags as key-value pairs" and publishes no cursor at all, so unlike
Kinesis there is nothing here implying an order even indirectly. Lexicographic by key is **substrate's
reading** at both, resting on the replay promise. No tie-break is needed, because a tag set cannot
carry a duplicate key.

The assertion is made on the **raw response bytes**, and that is the finding underneath the finding:
the existing tagging suite decoded `ResourceTagMappingList` and then collected `ResourceARN` only,
discarding `Tags`, so it was structurally incapable of seeing this — the same way #950's suite could
not see a parse guard. A decoded tag set compares equal whatever order it arrived in.
