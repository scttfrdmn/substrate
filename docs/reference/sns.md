# SNS

**Endpoint:** `sns.{region}.amazonaws.com`
**Protocol:** AWS Query (form-encoded, `Action=` parameter)

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateTopic | Decodes `Tags`, in either published spelling, and `Attributes` — only the 24 published keys, anything else is `InvalidParameter`/400 |
| GetTopicAttributes | Four attributes derived, the rest passed through as stored |
| SetTopicAttributes | Only the 25 published names are settable; anything else is `InvalidParameter`/400 |
| DeleteTopic | |
| ListTopics | Base64 pagination token |
| Subscribe | Supports lambda, sqs, http, https, email protocols |
| Unsubscribe | A string that is not a subscription ARN is `InvalidParameter`/400; a well-formed ARN naming no subscription is `NotFound`/404, in that order (#1259) |
| ListSubscriptions | |
| ListSubscriptionsByTopic | |
| GetSubscriptionAttributes | Seven members derived, the rest passed through as stored; `InvalidParameter`/400 for an ARN that is not a subscription's |
| SetSubscriptionAttributes | Only the 6 published names are settable; anything else is `InvalidParameter`/400. A stored `FilterPolicy` does not filter delivery |
| Publish | Dispatches to subscribed Lambda/SQS via cross-service dispatch |
| PublishBatch | |
| AddPermission | Accepted; no policy is stored |
| RemovePermission | Accepted; no policy is stored |
| TagResource | `ResourceNotFound`/404 for an absent topic; answers an empty `<TagResourceResult>` |
| UntagResource | Answers an empty `<UntagResourceResult>` |
| ListTagsForResource | Not paginated, as AWS's reference is not |

## A topic ARN addresses the topic it names

An SNS topic ARN is `arn:aws:sns:{region}:{account}:{topic-name}`. It carries **no
type keyword and no separator** — the resource portion *is* the name — which makes
it the one ARN in the tree with no segment to anchor a resource-type match against.

Every operation taking a `TopicArn` or a `ResourceArn` used to split it on `:` and
take the **last** segment, then key the load and the store by the **caller's** own
account and Region. Three failures compounded (#925), across ten operations:

- A topic ARN naming another account or Region addressed the caller's own topic of
  that name. `UntagResource` is the damaging direction and answered `200` while
  stripping tags from it; `Publish` published to the wrong topic and `DeleteTopic`
  deleted it. A caller could not tell any of those from a correct call.
- A subscription ARN is the topic's ARN with an identifier appended —
  `arn:aws:sns:{region}:{account}:{topic}:{sub-id}` — so its last segment is the
  subscription's own identifier, handed back as a topic name.
- The length guard fell through to returning its argument, so any string at all —
  `arn:aws:sns`, a bare name, a URL — became a topic name and was looked up rather
  than refused.

The account and Region now come from the ARN, by a parser that takes no request
context at all, which is the arrangement #826 established for SQS and DynamoDB, #910
for Step Functions, #918 for CloudFront and #922 for KMS. The discriminator SNS's
shape does offer is the colon: a resource portion containing one names a
subscription, and is refused with `InvalidParameter`/400 — which is also what a
malformed ARN now answers instead of being looked up verbatim.

A subscription ARN is **minted** under the topic's account and Region rather than the
subscriber's, because the ARN is the topic's own with an identifier appended.
The subscription record and the two subscription indexes are still keyed by the
calling account, deliberately: a cross-account subscription is not modeled — nothing
mints one and no operation distinguishes a subscriber's account from a topic's — so
moving them would put the index under an account the subscriptions are not stored in
and `Publish` would silently stop delivering. The key embeds the whole subscription
ARN, which itself names an account, so a lookup keyed by the caller can only find a
record whose ARN names that same account — ACM's arrangement, and not an instance of
the #918 defect.

## An SNS tag

`Tag` is `Key`/`Value`. AWS's reference names the request members `Tags.member.N`
and `TagKeys.member.N`, but **AWS's own request examples on the same pages wire
`Tags.Tag.1.Key`, `Tags.Tag.1.Value` and `TagKeys.TagKey.1`**. Substrate decoded
only the first spelling, so a caller that followed the example got `200` with
nothing written or nothing removed. Both are accepted, because the reference and its
example disagree and a caller may reasonably have followed either.

`CreateTopic` publishes a `Tags` parameter that substrate read not at all, so a topic
created with tags in one call reported none through either API. It is decoded now.

An absent topic answers **`ResourceNotFound`** at 404, which all three tag
operations publish. Substrate answered the same status under the code name
`NotFound`, which no SDK models — the status was right and the name was not. The
plain `NotFound` the topic operations answer is correct for them: only the three tag
pages publish `ResourceNotFound`.

A topic's tags are stored and reported **ordered by key**, on the write and on the
read. SNS's own merge was already deterministic — it walks indexed request
parameters, not a Go map — but it preserved insertion order while the tagging API's
arm emits key-sorted order, so one topic's tags came back in two different orders
depending on which API was asked (#862).

Two things are deliberately not modeled. AWS's published `ListTagsForResource`
sample response emits `<Value>` before `<Key>` and substrate emits `<Key>` first;
XML member order is not significant to any SDK's parser, so no test could assert a
difference a caller can act on. And the 10 TPS limit on SNS's tagging actions is not
modeled, which is a seedable-throttle question rather than an ARN one.

## Whether the topic an ARN names has to exist

The resolution rules above settle *which* topic an ARN addresses. Whether that topic
has to exist was a separate question, and four operations did not ask it (#926).

`Subscribe`, `Publish`, `PublishBatch` and `ListSubscriptionsByTopic` parsed the
`TopicArn`, derived a state key from it, and then read only the **subscription
index** — so an absent topic was indistinguishable from a real topic with no
subscribers, and all four answered `200`. `Publish` and `PublishBatch` minted a
`MessageId` for a message no topic had accepted, `Subscribe` handed back a
subscription ARN, and `ListSubscriptionsByTopic` reported an empty list. All four
pages publish `NotFound` at 404, glossed *"Indicates that the requested resource does
not exist."*

`Subscribe` is the worst of the four, because it is the one that **writes**: a
subscription record and two index entries survived it, reported by
`ListSubscriptions` ever after and delivered to had a topic of that name later been
created.

The consequence is not only a wrong code. A `CreateTopic` → `Publish` →
`DeleteTopic` → `Publish` sequence — a producer's teardown, or a test asserting that
one surfaces an error once its topic is gone — could not be written, because the
second `Publish` succeeded. An empty subscription list and an absent topic are now
different answers rather than the same one.

All six operations that resolve a `TopicArn` to a topic record — the four above plus
`GetTopicAttributes` and `SetTopicAttributes`, which already checked — go through one
helper, on the #961/#969 precedent that a per-handler existence check drifts. The
load is keyed by the ARN's own account and Region, so the check cannot reintroduce
what #925 fixed: a same-named topic in the caller's own Region does not satisfy an
ARN naming another, which `API_Publish` requires independently — *"You can publish
messages only to topics and endpoints in the same AWS Region."* The account half
needs no guard of its own, because the state key carries the ARN's account and a
foreign one addresses nothing.

The check runs **after** the parse, so a string that is not an ARN still answers
`InvalidParameter`/400 rather than being reported as a topic that does not exist.

`PublishBatch` answers at the **top level**, not as a per-entry
`BatchResultErrorEntry` inside a `200`. `API_PublishBatch` has both shapes, and the
page does not say which applies here; `TopicArn` is a request-level parameter, so
every entry addresses the same topic and a per-entry rendering would report the
identical failure on each of them behind a success status. `NotFound` is in the
operation's top-level `Errors` list, and the batch-scoped codes it publishes —
`BatchEntryIdsNotDistinct`, `InvalidBatchEntryId`, `ParameterValueInvalid` — are each
about an individual message. That reading is **substrate's**.

`Unsubscribe` answers `NotFound`/404 for a subscription ARN naming no subscription,
where substrate answered `200` behind an unsourced *"idempotent"* comment.
`API_Unsubscribe` publishes the code and states no idempotence, and
`SubscriptionArn` is the operation's only request parameter and only named resource,
so the published code can only be about the subscription. A string that is not a
subscription ARN at all, a topic ARN included, is `InvalidParameter`/400, which the
same page publishes; it is checked first, so the two refusals are distinguishable (#1259).

`DeleteTopic` is the one topic operation that goes the other way, and substrate had
it backwards (#992). `API_DeleteTopic`'s description states that *"this action is
idempotent, so deleting a topic that does not exist does not result in an error"*,
and substrate answered `NotFound`/404 — so a teardown that deletes unconditionally,
or a retried delete, failed here and succeeded in production. The same page **also**
publishes `NotFound`/404 in its `Errors` list, alongside `ConcurrentAccess`,
`InvalidState`, `StaleTag` and `TagPolicy`, so the page contradicts itself. The
description sentence governs: it names the condition, names the outcome, and calls
the property by name, while the error-list entry names no condition at all. Resolving
it that way is **substrate's reading**, corroborated by `API_Unsubscribe` publishing
the same code and carrying no such sentence — AWS states idempotence where it means
it. A malformed ARN is still `InvalidParameter`/400: idempotence licenses a topic
that does not exist, not a string that is not an ARN.

## Which attributes GetTopicAttributes reports

`API_GetTopicAttributes` publishes seventeen attribute names, and substrate's answer
splits in two: four are **derived** from state on every read, `Policy` is defaulted, and the
rest are reported only if `CreateTopic` or `SetTopicAttributes` stored them.

| Attribute | Substrate's answer |
|-----------|--------------------|
| `TopicArn` | Derived — the ARN the record is keyed by |
| `Owner` | Derived — the account segment of that ARN, matching the page's sample |
| `SubscriptionsConfirmed` | Derived — the length of the per-topic subscription index |
| `SubscriptionsPending` | Derived — always `0`; see below |
| `SubscriptionsDeleted` | **Not reported**; see below |
| `Policy` | The stored policy, or SNS's default topic policy when none is stored (`SetTopicAttributes` refuses an empty `Policy` with `InvalidParameter`, as AWS was observed to, and deleting an `AWS::SNS::TopicPolicy` writes the default back): the AWS CLI reference's `get-topic-attributes` example document, naming the topic and its owner's account (#1608) |
| `DeliveryPolicy`, `EffectiveDeliveryPolicy`, `DisplayName`, `MaximumMessageSize`, `SignatureVersion`, `TracingConfig`, `KmsMasterKeyId` | Stored only |
| `ArchivePolicy`, `BeginningArchiveTime`, `ContentBasedDeduplication`, `FifoTopic` | Stored only (FIFO) |

Of those seventeen, **eight are read-only** — `SetTopicAttributes` publishes none of
them, so substrate refuses a write to any: `TopicArn`, `Owner`, the three
subscription counts, `EffectiveDeliveryPolicy`, `BeginningArchiveTime` and
`FifoTopic`. The next section is the other side of that arithmetic.

Until #993 none of that was true. The handler reported a **`SubscriptionsCount`**
member that appears nowhere on the page — not in the attribute list, not in the
sample response, not in any SDK's shape — hardcoded to `"0"`, and reported none of
the three subscription counts AWS does publish. So a caller reading the attribute AWS
documents got nothing back and a caller reading the invented one got a constant no
writer ever touched.

**`SubscriptionsPending` is always `0`, and that is a fact rather than a placeholder.**
Substrate implements no `ConfirmSubscription`, and `Subscribe` hands back a real
subscription ARN rather than the `"pending confirmation"` string `API_Subscribe`
documents for a subscription awaiting confirmation. Every subscription this emulator
mints is therefore confirmed the moment `Subscribe` returns, so the confirmed/pending
split is decidable rather than guesswork.

**`SubscriptionsDeleted` is omitted rather than reported as `0`.** `Unsubscribe`
deletes the subscription record and filters both indexes rather than tombstoning it,
and `SNSSubscription` carries no status, so nothing tracks a deletion. A monotonic
counter incremented in `Unsubscribe` would be cheap, but the page publishes only
*"The number of deleted subscriptions for the topic"* and says nothing about how long
a deleted subscription stays counted — so a never-decaying counter would be
substrate's invention rather than its reading of the page. Omitting the member claims
nothing, which is the honest-empty rule. That choice is **substrate's**.

**`SubscriptionsConfirmed` is keyed by the caller's account and Region**, not by the
ARN's. The topic record is keyed by the ARN's own account and Region — the rule #925
made structural — while every read and write of the per-topic subscription index is
keyed by the caller's, which `Subscribe` records deliberately: a cross-account
subscription is not modeled, and re-keying the indexes would leave `Publish` reading
an index the subscriptions are not in. The count follows the index, because that
makes `GetTopicAttributes` and `ListSubscriptionsByTopic` read the same entries under
the same key and so unable to disagree about the same topic. Keying the count by the
ARN's target instead would have made them disagree by construction: `0` from the
count while the list returned the subscriptions.

**A derived attribute cannot be shadowed by a stored one.** `SetTopicAttributes` used
to accept any `AttributeName` and store it unchecked, and the handler used to merge
the stored map *after* its own literals — so a caller could set `TopicArn` and have
`GetTopicAttributes` report it, and once the counts became derived could have set
`SubscriptionsConfirmed` to any value it liked. The derived members are written last
for that reason. Since #1067 the handler refuses all four derived names outright, so
the ordering is defence in depth rather than the only guard — but it remains the only
guard for a value that reached the record another way, which is why it stays and why
its test seeds state directly rather than through the calls the allowlist now refuses.

**`Policy` is unmodelled, not omitted by accident.** The page publishes it and AWS's
sample response carries a default policy document naming eight actions, but no SNS
operation mints one, so substrate reports a `Policy` only when something stored it.
This is the same shape as KMS's #983 — a default resource policy that no operation
creates — and is recorded here rather than answered with an invented document.
`SignatureVersion` is the one attribute whose absence the page gives a meaning:
*"If the API response does not include the `SignatureVersion` attribute, it means that
the `SignatureVersion` for the topic has value 1."* Not inventing it is what the page
asks for.

## Which attributes SetTopicAttributes accepts

`API_SetTopicAttributes` publishes **twenty-five** settable `AttributeName` values, and
substrate accepts exactly those. Any other name — including the eight
`GetTopicAttributes` publishes and this page does not — is `InvalidParameter`/400.

| Group | Names |
|-------|-------|
| General | `DeliveryPolicy`, `DisplayName`, `MaximumMessageSize`, `Policy`, `TracingConfig` |
| Delivery status, per endpoint family | `HTTP`, `Firehose`, `Lambda`, `Application` and `SQS` each × `SuccessFeedbackRoleArn`, `SuccessFeedbackSampleRate`, `FailureFeedbackRoleArn` — fifteen names |
| Server-side encryption | `KmsMasterKeyId`, `SignatureVersion` |
| FIFO topics | `ArchivePolicy`, `ContentBasedDeduplication`, `FifoThroughputScope` |

Until #1067 the handler wrote whatever `AttributeName` arrived into the topic record
with no check of any kind, so `AttributeName=Banana` was stored and
`GetTopicAttributes` reported it back as though SNS carried it — and so were the eight
read-only names, of which the merge order described above stopped only four from being
reported.

**The guard is an allowlist, not a denylist of the derived names**, because the two
pages do not partition one vocabulary. `Set` publishes 25 names, `Get` publishes 17,
only **9** appear on both, and the union is 33. Subtracting substrate's four derived
names from anything would still have accepted `EffectiveDeliveryPolicy`,
`SubscriptionsDeleted`, `BeginningArchiveTime` and `FifoTopic` — each a fact about the
topic rather than something a caller sets — and no denylist of any length refuses a
name neither page publishes.

**The code is substrate's reading of a published gloss, not a published sentence.** The
page lists `InvalidParameter`/400 with *"Indicates that a request parameter does not
comply with the associated constraints"*, and `AttributeName`'s constraint is its
published value list; the page's only prose naming the code is about a
`MaximumMessageSize` above 256 KiB on a topic that cannot carry it. The refusal
*messages* are substrate's own, because the page publishes none. `AttributeName` is
`Required: Yes`, so an absent one is the same code; `AttributeValue` is
`Required: No`, so an empty value is accepted, except for `Policy`. AWS was observed
refusing an empty `Policy` with `InvalidParameter` (go-to-k/cdkd#4610), since a topic
always has one, and substrate refuses it too. There is no `DeleteTopicPolicy`, so
deleting an `AWS::SNS::TopicPolicy` writes the topic's default policy back, as
CloudFormation's own handler does (#1608).

**Divergence: sixteen settable names are reported back where AWS would not report
them.** Those sixteen — the fifteen delivery-status names and `FifoThroughputScope` —
are absent from `GetTopicAttributes`' seventeen, so on AWS they are write-only.
Substrate stores them and `GetTopicAttributes` reports the whole stored map, so it
reports them. That is **recorded rather than filtered**: the value the caller set is
real, and hiding it would make `SetTopicAttributes` look like a no-op, which is #1067's
failure mode in the other direction. A consumer asserting on the key set
`GetTopicAttributes` publishes must take AWS's page as the authority here, not
substrate's answer.

**The name check runs before the topic is resolved**, so an unpublished name is refused
whether or not the topic exists. The page publishes both `InvalidParameter`/400 and
`NotFound`/404 and orders them nowhere, so that is substrate's choice.

## CreateTopic's Attributes map, and the one name it does not accept

`API_CreateTopic` publishes an `Attributes` map on the wire as
`Attributes.entry.N.key` / `Attributes.entry.N.value`, which is what an SDK sends for
`create_topic(Name=…, Attributes={…})`. Substrate decoded none of it until #1126: it
seeded the record from a bare `DisplayName` query parameter instead, so the ordinary SDK
call lost every attribute and `GetTopicAttributes` reported none of them, while a
parameter the page does not publish at all was the only one honored.

The map is now decoded at every index, 1-based and dense — the first absent *key* ends
it, and an empty value is stored, matching what `SetTopicAttributes` does with an empty
`AttributeValue`. The bare `DisplayName` parameter is **no longer read**: the page
publishes exactly four request parameters — `Attributes`, `DataProtectionPolicy`,
`Name` and `Tags.member.N` — and honoring a fifth let a consumer write a call that
round-tripped here and created a topic with no display name on AWS.

**`CreateTopic` publishes 24 keys where `SetTopicAttributes` publishes 25.** The lists
are otherwise identical, name for name and group for group; the single difference is the
server-side-encryption group, where `SetTopicAttributes` lists `KmsMasterKeyId` *and*
`SignatureVersion` and `CreateTopic` lists `KmsMasterKeyId` alone. So
`SignatureVersion` on a create is `InvalidParameter`/400 and on a set is accepted, and a
topic that needs one gets it from the operation whose page publishes it. That asymmetry
is AWS's, and substrate keeps two allowlists rather than one so that it cannot be
collapsed by accident.

The refusal is the same code and the same shape as the set side's, and it is decoded
**before the topic index is read** — so a refused create leaves no topic behind, and the
answer does not depend on whether a topic of that name already exists.

`AWS::SNS::Topic`'s `DisplayName` property travels through this map, as the wire form a
real `CreateTopic` carries. The type's other attribute-valued properties are not
forwarded; see the CloudFormation section for what it does send.

## Subscription attributes: six settable, seven derived, one inert

`SetSubscriptionAttributes` was a stub until #1125 — it read `SubscriptionArn` into `_`,
read neither `AttributeName` nor `AttributeValue`, touched no state and answered 200 —
and `GetSubscriptionAttributes` answered a fixed four entries built from the record, so
nothing a caller set could ever be read back. Both halves now go through one merge.

**Settable (6).** Exactly the `AttributeName` values `API_SetSubscriptionAttributes`
publishes: `DeliveryPolicy`, `FilterPolicy`, `FilterPolicyScope`, `RawMessageDelivery`,
`RedrivePolicy` and `SubscriptionRoleArn`. Anything else is `InvalidParameter`/400.

Six, not seven. `ReplayPolicy` and `ReplayStatus` appear on `API_Subscribe`, under a
heading reading "The following attributes apply only to FIFO topics", and on neither of
the two attribute pages — so substrate refuses both here. The `ReplayLimitExceeded`/403
this page *does* publish is an error shared with `Subscribe` and says nothing about which
names the operation accepts.

The Firehose-only qualification on `SubscriptionRoleArn` is recorded and not enforced:
the page states the attribute "applies only to" Firehose subscriptions but publishes no
error for setting it elsewhere, so substrate accepts it on any protocol rather than
inventing a refusal.

**Derived (7).** `ConfirmationWasAuthenticated`, `Endpoint`, `Owner`,
`PendingConfirmation`, `Protocol`, `SubscriptionArn` and `TopicArn` are projected from the
record and merged *over* the stored map, so a stored value cannot shadow one.
`PendingConfirmation` is `false` and `ConfirmationWasAuthenticated` is `true` because
substrate mints no unconfirmed subscription — there is no `ConfirmSubscription` operation,
and `Subscribe` returns a real ARN rather than the "pending confirmation" string — so a
subscription is confirmed by the authenticated `Subscribe` call itself and never by an
out-of-band token. This is the same reasoning that makes a topic's `SubscriptionsPending`
`"0"`.

`Protocol` and `Endpoint` are reported although `API_GetSubscriptionAttributes`' list
omits them. That list is explicitly open ("Attributes in this map include the
following"), both are `Subscribe` parameters a caller has no other way to read back for a
single subscription, and `ListSubscriptions` already reports both for the same record —
dropping them would make two readers disagree about one subscription.

**Not modelled.** `EffectiveDeliveryPolicy` is omitted. The page defines it as the policy
"that takes into account the topic delivery policy and account system defaults", and
substrate models neither the defaults nor the merge; reporting the subscription's own
`DeliveryPolicy` under the name would claim a computation that did not happen, so the
member is absent rather than invented (#827).

**A stored `FilterPolicy` does not filter delivery.** It round-trips through
`GetSubscriptionAttributes` as the JSON string the caller sent, and `Publish` delivers to
the subscription regardless of whether a message would match. Delivering only matching
messages is the subscription's runtime behaviour rather than an API observation, so it
stays outside the boundary — the same reading that keeps a Lambda's handler from being
executed. `SNSSubscription` carries a separate `FilterPolicy` field that `Publish` does
consult, but no operation writes it: it is reachable only by a test writing the record
directly. A consumer asserting "a non-matching message was not delivered" will therefore
see it delivered here, which is the deliberate divergence.

**ARN refusals.** Both operations answer `InvalidParameter`/400 for a string that is not a
subscription ARN and `NotFound`/404 for one that names no subscription, in that order.
Before #1125 neither had a site for the first: a topic ARN — a perfectly good SNS ARN
naming no subscription — was reported as a subscription that did not exist. `Unsubscribe`
follows the same order since #1259, through the same `requireSubscription` helper.

## An empty result element is not the same as no result element

Eight SNS operations answer with a body carrying no members, and the query protocol
spells that two different ways. Whether `<{Operation}Response>` holds an
`<{Operation}Result>` element at all is decided by the operation's modeled output:

- an output of `smithy.api#Unit` has **no** result element — `DeleteTopic`,
  `SetTopicAttributes`, `Unsubscribe`, `SetSubscriptionAttributes`, `AddPermission`
  and `RemovePermission`;
- an output that is an empty **structure** has the element, empty — `TagResource` and
  `UntagResource`.

AWS publishes both halves as sample responses: `API_TagResource` and
`API_UntagResource` show `<TagResourceResult/>` and `<UntagResourceResult/>`, while
`API_DeleteTopic`, `API_Unsubscribe` and `API_AddPermission` show
`<ResponseMetadata>` as the response's only child. Substrate emitted the second shape
for all eight, which is right for six of them (#1141).

**The two tag operations therefore could not be called through an SDK at all**, even
though they did their work. `aws-sdk-go-v2`'s generated deserializer looks the element
up by name and fails the operation when it is absent rather than treating a missing
empty element as an empty one:

```
operation error SNS: TagResource, https response error StatusCode: 200, RequestID: ,
deserialization failed, failed to decode response body, TagResourceResult node not found
```

The tag was already written when that error was returned, so state was right and only
the envelope was wrong — and every caller that checks its error treated a successful
tag as a failure. A create → converge → tag deployment sequence could not get past its
last step.

A hand-written client cannot see the difference, because the element carries nothing,
which is why the omission survived until a real SDK reached it. The sweep for further
instances found none: the SDK's SNS deserializer requires a result element for 31
operations and none of the six `Unit` ones is among them, and no other query-protocol
service in the tree omits an element its own page publishes.

## The account and Region a record carries reach no response

`SNSTopic` and `SNSSubscription` declare `AccountID` and `Region` under wire-visible `json` tags,
because the record is what `MemoryStateManager` snapshots and a replay reads back. The topic also
declares `EverTagged` as `ever_tagged`. No SNS shape publishes any of the three, and none reaches a
body:

- `emulator/sns_types.go` carries no `xml` tag.
- Every response is marshaled from a struct declared for its operation.
- The two subscription lists build their entries member by member.
- Publish's delivery envelope is a map built member by member too.

All **eighteen** routed operations are driven by `TestSNSWire_TopicResponsesCarryNoBookkeepingMember`
and `TestSNSWire_SubscriptionResponsesCarryNoBookkeepingMember` in `emulator/sns_wire_test.go`. They
walk the raw XML and fail on an element named for a bookkeeping member at any depth. As a result, the
five entries this discharges in `scripts/wire-bookkeeping-baseline.txt` stay listed as declarations
rather than as leaks ([#756](https://github.com/scttfrdmn/substrate/issues/756)).

The walk also reads the *text* of every `<key>`. `GetTopicAttributes` and `GetSubscriptionAttributes`
answer a string map rendered as `<entry><key>…</key><value>…</value></entry>`, so a member leaked
into the map would arrive as key text under an element named `key`. An element walk alone passes
that. A control that adds an `AccountID` attribute fails only with the key-text check in place.

`ever_tagged` carries `,omitempty` and is set only by a tag write (#938). An untagged topic's
responses would therefore be missing it for free. The topic test calls `TagResource` and reads
`ever_tagged: true` back from the stored record before it walks any response. Without that step, the
test fails at the read-back rather than passing vacuously, which was #1304's trap on EFS.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::SNS::Topic | TopicArn | `TopicName` and `DisplayName` are sent; other attribute-valued properties are not |
| AWS::SNS::Subscription | SubscriptionArn | |

`DisplayName` travels as `CreateTopic`'s published `Attributes.entry.1.key` /
`.value` — see *CreateTopic's Attributes map* above. It is the only one of the type's
attribute-valued properties the deployer forwards. `AWS::SNS::Topic` publishes fourteen
properties, and the deploy reads two: `TopicName` and `DisplayName`. So `ArchivePolicy`,
`ContentBasedDeduplication`, `FifoThroughputScope`, `KmsMasterKeyId`,
`MaximumMessageSize`, `SignatureVersion` and `TracingConfig` are accepted in a template
and reach no `CreateTopic` parameter — `GetTopicAttributes` reports none of them for a
topic CloudFormation created — and neither `Tags`, the inline `Subscription` array,
`DataProtectionPolicy` nor `DeliveryStatusLogging` is sent. `FifoTopic` is likewise not
read, so a `.fifo` topic declared in a template is a standard topic here.

## Cost

SNS publish: $0.0000005 per message.
