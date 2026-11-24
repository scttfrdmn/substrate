# Kinesis Data Streams

**Endpoint:** `kinesis.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: Kinesis_20131202.{Op}`)

## Supported operations

All eighteen operations accept `StreamARN`, `StreamName` or both, except the four noted below — see
[Naming a stream: by name or by ARN](#naming-a-stream-by-name-or-by-arn).

| Operation | Notes |
|-----------|-------|
| CreateStream | Names the stream by `StreamName` only — the service's one operation-wide `Required: Yes`, and the one operation minting an ARN rather than resolving one; stores a [create-time `Tags` map](#how-many-tags-a-stream-may-carry) and refuses it under the same two bounds `AddTagsToStream` enforces, before the stream is written. Records `StreamModeDetails` (an `ON_DEMAND` stream starts with four shards) and `MaxRecordSizeInKiB` (1024–10 240) — see [Capacity mode, and the reshards it refuses](#capacity-mode-and-the-reshards-it-refuses) |
| DescribeStream | Answers an `API_StreamDescription` — which carries `Shards` and `HasMoreShards` and **no** `OpenShardCount`; see [The two describe shapes, and the bounds on a reshard](#the-two-describe-shapes-and-the-bounds-on-a-reshard). Carries `StreamModeDetails` |
| DescribeStreamSummary | Answers an `API_StreamDescriptionSummary` — which carries `OpenShardCount` and **neither** `Shards` **nor** `HasMoreShards`. Carries `StreamModeDetails` and `MaxRecordSizeInKiB` |
| DeleteStream | A store fault deleting a shard's records is an error, not a stream reported deleted (#1399) |
| ListStreams | Names no single stream, so it publishes neither member and lists the caller's own account and Region. Pages by `Limit` (1–10 000, default 100, at most 100 returned) and `ExclusiveStartStreamName`, reporting `HasMoreStreams`; names are listed lexicographically; `NextToken` is not read — see [Resharding acts on the shards it names](#resharding-acts-on-the-shards-it-names) |
| UpdateShardCount | `ScalingType` and `TargetShardCount` both required and both checked: the enum, the published minimum of 1, the 10 000 ceiling and the double/half pair, all `InvalidArgumentException`/400. Reports all four published members including `StreamARN`. Reports `UPDATING` for as many observations as a seed holds it, and refuses a stream that is not `ACTIVE` with `ResourceInUseException`/400 — see [A stream's status progresses](#a-stream-s-status-progresses). Refuses an on-demand stream with `ValidationException`/400 |
| UpdateStreamMode | Names the stream by `StreamARN` only, which is `Required: Yes`, as is `StreamModeDetails`. Switches the capacity mode and reports `UPDATING` under a seed like a reshard; switching to the mode the stream already has changes nothing. `WarmThroughputMiBps` is not read — see [Capacity mode, and the reshards it refuses](#capacity-mode-and-the-reshards-it-refuses) |
| MergeShards | Merges the two named adjacent shards: both close, and one child covering their union reports them as `ParentShardId`/`AdjacentParentShardId`. Reports `UPDATING` under a seed, and refuses a stream that is not `ACTIVE` with `ResourceInUseException`/400 and an on-demand stream with `ValidationException`/400 — see [Resharding acts on the shards it names](#resharding-acts-on-the-shards-it-names) and [A stream's status progresses](#a-stream-s-status-progresses) |
| SplitShard | Splits the named shard at `NewStartingHashKey`: it closes, and two children report it as `ParentShardId`. Otherwise as `MergeShards` |
| PutRecord | `PartitionKey` is 1–256 characters, and the record is at most the stream's maximum record size — see [What a put may carry](#what-a-put-may-carry) |
| PutRecords | 1–500 entries, each checked as `PutRecord`'s, and at most 10 MiB in all |
| GetShardIterator | Returns base64-encoded cursor |
| GetRecords | Names its stream by `ShardIterator`; a `StreamARN` is optional and is checked against it. This is the one page publishing `StreamARN` and **no** `StreamName`. `ApproximateArrivalTimestamp` is epoch seconds to the millisecond, as the page publishes (#1338) |
| EnableEnhancedMonitoring | `ShardLevelMetrics` is checked against its published 1–7 range and enum; `ALL` is expanded — see [Shard-level metrics, and the ALL wildcard](#shard-level-metrics-and-the-all-wildcard) |
| DisableEnhancedMonitoring | Same shape and the same checks as its sibling |
| AddTagsToStream | `Tags` is a JSON object of key/value pairs, not a list |
| RemoveTagsFromStream | |
| ListTagsForStream | Reports `Tags` sorted by key — see [A tag set read back out of a map](/reference/concepts/listing-order#a-tag-set-read-back-out-of-a-map) — and pages them with `Limit` and `ExclusiveStartTagKey`; see [Paging the tags on a stream](#paging-the-tags-on-a-stream) |

## Naming a stream: by name or by ARN

Fifteen of the eighteen operations publish `StreamARN` beside `StreamName`, both `Required: No`, under
a Note that is byte-identical on every one of their pages: *"you must use either the `StreamARN` or the
`StreamName` parameter, or both. It is recommended that you use the `StreamARN` input parameter when you
invoke this API."* Until [#966](https://github.com/scttfrdmn/substrate/issues/966) substrate decoded
only `StreamName`, so the **recommended** form was the one form that could not work — a caller sending
only an ARN, which is what an SDK client built from one sends, and what a CloudFormation `Ref`, a Lambda
event-source mapping and an IAM policy all carry, reached an empty-`StreamName` guard and was refused.

| Request | Answer |
|---------|--------|
| `StreamARN` only | The stream the ARN names, in the ARN's own account and Region |
| `StreamName` only | The stream of that name in the **caller's** account and Region — the only reading available, since a name carries neither |
| Both, naming one stream | Accepted, per *"or both"* |
| Both, naming different streams | `InvalidArgumentException`/400 — **substrate's reading**, below |
| Neither | `InvalidArgumentException`/400, since the Note makes such a request invalid |
| A `StreamARN` not matching the published pattern | `InvalidArgumentException`/400, whose description — *"a specified parameter exceeds its restrictions, is not supported, or can't be used"* — is this case |
| `StreamId` | Not decoded, and naming a stream by it alone names it not at all: AWS publishes it as *"Not Implemented. Reserved for future use."* on all fifteen pages |

**The account and Region come from the ARN, not from the request context.** An ARN naming another
account's or another Region's stream addresses *that* stream, and one naming nothing there reports the
stream absent rather than quietly serving the caller's own same-named one. Every key a request touches —
the stream record, its records, and its entry in the `ListStreams` index — is derived from that one
resolution, so a cross-account `UpdateShardCount`, `MergeShards` or `AddTagsToStream` writes to the
target and a cross-account `DeleteStream` removes the target's index entry rather than the caller's. The
parse takes no request context at all, which is what makes the rule structural rather than a thing
fifteen call sites have to remember, following
[#826](https://github.com/scttfrdmn/substrate/issues/826) for SQS and DynamoDB and
[#912](https://github.com/scttfrdmn/substrate/issues/912) for Step Functions. The Resource Groups
Tagging API resolves a stream ARN through the same parser, so the two cannot disagree about which stream
an ARN names or where its tags live.

**What enforcement the pattern states, and no more.** The published pattern is
`arn:aws.*:kinesis:.*:\d{12}:stream/\S+`. The partition must begin `aws`, the service segment must be
`kinesis`, and the account must be exactly twelve digits. A remaining `/` in the resource portion means
the ARN names a **consumer** — `stream/{name}/consumer/{name}:{timestamp}` — which substrate does not
model, so it is refused. `StreamName`'s own `[a-zA-Z0-9_.-]+` class is deliberately **not** applied to
the name inside an ARN, because nothing applies it on `CreateStream` either: applying it only here
would make a stream substrate itself lets a caller create unaddressable by its own ARN.

**A `StreamARN` with an empty Region resolves rather than being refused.** The pattern's Region segment
is `.*`, which matches the empty string, so such an ARN is well-formed; it resolves to a key nothing is
written at and reports the stream absent. Refusing it for its shape would claim AWS rejects an ARN its
own pattern accepts — the decision [#912](https://github.com/scttfrdmn/substrate/issues/912) recorded
for an express execution ARN.

**Two members that disagree are refused, and that is substrate's reading**: no Kinesis page says what
happens when `StreamARN` and `StreamName` name different streams. Serving either of them is how a
caller's bug stays hidden, so the request is refused instead. Only the **name** segments are compared —
an ARN whose account or Region differ from the caller's is not a disagreement but the whole point of the
change, and it wins.

**Two error statuses were corrected in the same pass.** Every error on every one of the seventeen
Kinesis reference pages is published at HTTP **400** — the only 500 in the service is
`InternalFailureException`, which substrate does not raise — and substrate answered
`ResourceNotFoundException` at 404 and `ResourceInUseException` at 409. Both are now 400, as
[#910](https://github.com/scttfrdmn/substrate/issues/910) and
[#912](https://github.com/scttfrdmn/substrate/issues/912) established for Step Functions. The
`InvalidParameterException` each handler's body-decode guard answered is published by Kinesis nowhere
at all — not on an operation page, not on the common-errors page, not even in prose — and
[#950](https://github.com/scttfrdmn/substrate/issues/950) corrected all nineteen sites to
`InvalidArgumentException`/400, which **all sixteen** guarded operation pages publish: *"A specified
parameter exceeds its restrictions, is not supported, or can't be used. For more information, see the
returned message."* `ValidationException`, which four of those pages also carry, is not the answer:
the SDK model glosses it as specific to capacity mode, and since
[#1118](https://github.com/scttfrdmn/substrate/issues/1118) it is answered only for a reshard of an
on-demand stream. So nothing in the plugin answers the old code any longer.

**`GetRecords`' page contradicts itself, and the contradiction is recorded rather than resolved.** It
publishes `StreamARN` and no `StreamName`, because its stream is implied by the required
`ShardIterator` — and it carries the same boilerplate Note anyway, naming a parameter the same page does
not document. Substrate follows the shape: an iterator already carries the account and Region its stream
was resolved in, so a supplied `StreamARN` is redundant and is checked against it. One naming a
different stream is `InvalidArgumentException`/400 rather than ignored, since ignoring it would serve
records from a stream the request did not name.

## Paging the tags on a stream

`ListTagsForStream` publishes both halves of a cursor over the tag key, and until
[#954](https://github.com/scttfrdmn/substrate/issues/954) substrate read neither: every call returned
every tag and `HasMoreTags` was the literal `false`, so a caller's paging loop was told there was
nothing more by a response that had not looked. Now `Limit` caps a page, `ExclusiveStartTagKey`
positions it, and `HasMoreTags` says whether anything remains.

| Call | Answer |
|------|--------|
| No `Limit` | Every tag, `HasMoreTags: false` — `Limit` is optional and the page states no default |
| `Limit` of 1–50 | At most that many tags, taken from the front of the remaining set |
| `Limit` of 0, 51 or any other value outside 1–50 | `InvalidArgumentException`/400 — the operation's own published error, whose description is *"a specified parameter exceeds its restrictions"* |
| `ExclusiveStartTagKey` | The tags whose keys sort **strictly after** it, per *"gets all tags that occur after `ExclusiveStartTagKey`"* — so a caller that passes back the last key it received advances instead of repeating it |
| `ExclusiveStartTagKey` naming no tag | Positions the walk anyway: the cursor is over the key space, so a value between two keys starts at the later one |
| `ExclusiveStartTagKey` of `""` | Read as absent rather than as a violation of the published minimum length of 1 — it is what an omitted member decodes to and what a caller starting a walk sends |
| `ExclusiveStartTagKey` longer than 128 characters | `InvalidArgumentException`/400, the published maximum length, which is also the maximum length of a tag key |

**The walk order is substrate's reading**, as [the tag-order section](/reference/concepts/listing-order#a-tag-set-read-back-out-of-a-map)
records: AWS names no order for these tags anywhere, and its own sample response is unsorted. The
cursor *requires* one — a tag cannot occur "after" a key otherwise — so substrate walks keys
lexicographically, and that is what makes a page's contents predictable rather than a function of Go's
map seed.

**`HasMoreTags` is `true` exactly when tags were withheld, which resolves two AWS sentences that do
not agree.** Under `Limit`, the page says `HasMoreTags` is set *"if this number is less than the total
number of tags associated with the stream"*; read literally, a walk at `Limit` 2 over six tags would
report `true` on the last page too, since 2 is still less than 6, and the loop AWS itself describes —
*"to list additional tags, set `ExclusiveStartTagKey` to the last key in the response"* — would never
terminate. `HasMoreTags`' own description is the coherent one, *"if set to true, more tags are
available"*, so substrate reports whether anything remains **after this page**. A `Limit` equal to or
larger than the remaining set is therefore not a truncation.

**AWS publishes two different limits on how large a stream's tag set can get, and both are now
enforced** — see [How many tags a stream may carry](#how-many-tags-a-stream-may-carry). Because 50 is
the one that binds, a single maximum-`Limit` page holds every tag a stream may legally hold through
Kinesis's own operations, and the cursor matters only for a smaller `Limit`.

## How many tags a stream may carry

`API_AddTagsToStream` states both numbers **inside one parameter entry**. The `Tags` member's
description reads *"A set of up to 50 key-value pairs to use to create the tags. A tag consists of a
required key and an optional value. You can add up to 50 tags per resource."*, and the constraint lines
directly beneath it read *"Map Entries: Maximum number of 200 items."* The operation's lede says 50
again, and `ListTagsForStream`'s response `Tags` array publishes 0–200 while its `Limit` maxes out at
exactly 50. Until [#965](https://github.com/scttfrdmn/substrate/issues/965) substrate enforced neither,
so a stream could hold 300 tags and `ListTagsForStream` would answer an array longer than its own
published maximum — substrate emitting a response its own reference says cannot exist.

**Both figures are real, they are limits on different things, and both are enforced under different
codes.** 200 bounds one request's shape; 50 is the resource's quota.

| Request | Answer |
|---------|--------|
| More than 200 entries in one `Tags` map | `InvalidArgumentException`/400, whose description is *"a specified parameter exceeds its restrictions"*. Checked **first**, against the request alone: a request that does not satisfy its own shape is not evaluated against account state |
| A merged tag set of more than 50 | `LimitExceededException`/400 — *"The requested resource exceeds the maximum number allowed"* |
| A tag key outside 1–128 characters, or a value longer than 256 | `InvalidArgumentException`/400. An **empty value** is valid where an empty key is not, per *"a tag consists of a required key and an optional value"* and the published minimums of 0 and 1 |
| An absent `Tags` member | `InvalidArgumentException`/400 — it is the operation's one `Required: Yes` member besides the stream reference |
| An empty `Tags` map | Accepted as a no-op. **Substrate's reading**: the map publishes a maximum entry count and no minimum, where `RemoveTagsFromStream`'s `TagKeys` array publishes *"Minimum number of 1 item"* — AWS states a minimum where it means one |

**The quota is a property of the stream, so it is counted against the merged result**, not against the
incoming map: two accepted requests of thirty tags each are refused on the second. A key already on the
stream does not count twice, because *"`AddTagsToStream` overwrites any existing tags that correspond to
the specified tag keys"* — so re-tagging a stream that is already at the quota with a key it already
carries is a rewrite and succeeds, and `RemoveTagsFromStream` frees slots for a later add. A refused
request writes **nothing**, not even the tags that would have fit.

**The Resource Groups Tagging API enforces this same quota**, under this same code, since
[#1000](https://github.com/scttfrdmn/substrate/issues/1000). It did not until then: `TagResources`
merges tags for twenty-three services through one helper that consulted no quota at all, so a caller
could push a stream past fifty tags through it and `AddTagsToStream` would then refuse every further
add — the right answer for a stream over quota however it got there, but reached through substrate's
own API. That issue also corrects two claims first published here: Kinesis was **not** substrate's
first per-resource tag quota (EC2, ELBv2 and IAM each enforced one already, and all three are reachable
through the tagging API), and the merge covers twenty-three arms rather than sixteen. See
[A tag quota belongs to the service that owns the resource](/reference/tagging#a-tag-quota-belongs-to-the-service-that-owns-the-resource)
for the four services' codes, which differ.

**`CreateStream` reaches the same two checks**, since
[#1087](https://github.com/scttfrdmn/substrate/issues/1087). `API_CreateStream` publishes `Tags` with
the identical pair of numbers — prose *"A set of up to 50 key-value pairs"* over *"Map Entries: Maximum
number of 200 items"* — and the operation's lede states the interaction: *"You can add tags to the
stream when making a `CreateStream` request by setting the `Tags` parameter."* Substrate decoded
`StreamName` and `ShardCount` only, so a create-time tag set was silently dropped.

Both checks run **before any state is read**, which is what leaves no stream behind on a refusal: the
200-entry bound is a property of the request alone, and a stream that does not exist yet holds no tags
for the quota to merge against. A create that refused after writing its record would be the worse
failure, because the caller's retry would then hit `ResourceInUseException` for a stream it was told it
had not created.

Two differences from `AddTagsToStream` are worth stating.

- **`Tags` is `Required: No` here**, so an absent member is not the missing-member refusal.
  `kinesisValidateTagMap`'s nil branch belongs to `AddTagsToStream`, where the member is
  `Required: Yes`, and is gated at the create call site. An explicit `null` and an empty map are both
  accepted, the latter as the same no-op the table above records.
- **`LimitExceededException`'s attribution on this page is substrate's reading, not a citation.** The
  code *is* in `CreateStream`'s Errors list at 400, but the page attributes it to *"more than five
  streams in the `CREATING` state"* and to requesting *"more shards than are authorized"* — it says
  nothing about tag count. Using it for a fifty-first tag at create time carries the attribution over
  from the sibling door where it *is* published. That is not the borrowing
  [#671](https://github.com/scttfrdmn/substrate/issues/671) forbids, which is taking a code the
  operation's own page does not carry at all. `InvalidArgumentException` needs no such reading: its
  description covers the over-200 shape on this page exactly as it does on the other.

`EverTagged` is deliberately not stamped by the create path, for the reason recorded under
[`GetResources` reports what has been tagged](/reference/tagging#getresources-reports-what-has-been-tagged-not-what-is-tagged).

## Shard-level metrics, and the ALL wildcard

`API_EnableEnhancedMonitoring` and `API_DisableEnhancedMonitoring` publish a byte-identical
`ShardLevelMetrics` shape — `Required: Yes`, *"Array Members: Minimum number of 1 item. Maximum number
of 7 items"*, and an eight-entry `Valid Values` enum: the seven metric names plus `ALL`. Until
[#999](https://github.com/scttfrdmn/substrate/issues/999) substrate decoded the member and checked none
of it, so an empty array, a fifty-item array and a misspelled metric name were all accepted and written to
the stream.

| Request | Answer |
|---------|--------|
| An absent `ShardLevelMetrics` | `InvalidArgumentException`/400 — it is the operation's one `Required: Yes` member besides the stream reference |
| An empty array | `InvalidArgumentException`/400, per the published *"Minimum number of 1 item"*. **The opposite reading from `AddTagsToStream`'s `Tags`**, deliberately: that map publishes a maximum and no minimum, this array publishes both |
| More than 7 items | `InvalidArgumentException`/400 |
| A value outside the enum, including a correct name in the wrong case | `InvalidArgumentException`/400, naming the offending value and the enum |

**`ALL` is expanded into the seven metrics and never appears in a response.** The enum's eighth entry is
listed on the two *response* arrays as well, which would let AWS report the literal `ALL`; substrate
reads it as an artifact of one `MetricsName` enum shape reused in both directions, because *"The value
`ALL` enables every metric"* composed with `DesiredShardLevelMetrics`' own description — *"the list of
all the metrics that would be in the enhanced state after the operation"* — names seven metrics, not one
wildcard. Expanding is also the only reading under which `DisableEnhancedMonitoring` works: against a
stored `["ALL"]`, disabling `IncomingBytes` would remove nothing.

**That expansion is what makes the page's eight-value enum consistent with its seven-item maximum.** A
caller naming every metric *and* `ALL` sends eight items and cannot satisfy both bounds, so substrate
enforces the maximum AWS states — and such a caller has no reason to ask, since `ALL` alone is one item
and already means all seven.

**`CurrentShardLevelMetrics` is the set before the operation and `DesiredShardLevelMetrics` the set
after**, per their own descriptions. Substrate rendered the same slice for both, read after the write, so
`Current` reported the after-state on every call — and `DisableEnhancedMonitoring` filtered its stored
slice in place, aliasing the backing array, so the before-state was destroyed as the filter ran. The
second defect is why the first could not be fixed by swapping two renders.

**An empty set renders as `[]`, never `null`.** Both arrays publish *"Minimum number of 1 item"*, yet
`Enable`'s own Sample Response carries `"CurrentShardLevelMetrics": []` and `Disable`'s carries
`"DesiredShardLevelMetrics": []`. The samples are the authority on the shape a caller has to handle, so
the minimum is not a response guarantee — the same rule as
[#938](https://github.com/scttfrdmn/substrate/issues/938).

**The response orders the metrics as the pages bullet them**, whatever order the caller sent. Neither
page states a response ordering and a set has none, so substrate picks a canonical one, following
`ListTagsForStream`'s choice to sort an unordered map by key rather than report Go's iteration order.
Two replays of one recorded request therefore render byte-identical bodies.

Both Sample Responses also **omit the published `StreamARN`**. It is reported regardless, from the
stream the request resolved to rather than from the caller's own account and Region — so an ARN-only
request naming another account's stream reports that account's ARN. `UpdateShardCount` omitted the same
member and now reports it too. [#966](https://github.com/scttfrdmn/substrate/issues/966) was
request-side only: it taught fifteen operations to read a `StreamARN` and gave no response one.

## The two describe shapes, and the bounds on a reshard

`DescribeStream` answers an `API_StreamDescription` and `DescribeStreamSummary` an
`API_StreamDescriptionSummary`. Until [#1076](https://github.com/scttfrdmn/substrate/issues/1076) one
builder served both and emitted the **union** of their members, so each operation answered members its
own page does not publish:

| Member | `StreamDescription` | `StreamDescriptionSummary` |
|--------|---------------------|----------------------------|
| `Shards` | `Required: Yes` | not a member |
| `HasMoreShards` | `Required: Yes` | not a member |
| `OpenShardCount` | not a member | `Required: Yes` |

The other six `Required: Yes` members — `StreamName`, `StreamARN`, `StreamStatus`,
`RetentionPeriodHours`, `StreamCreationTimestamp` and `EnhancedMonitoring` — are common to both and
substrate answers all of them. Substrate closes no shard, so every shard it holds is open and
`OpenShardCount` equals the shard count; that the two coincide is a property of substrate's model, not
of the API.

**`EnhancedMonitoring` is an array of `EnhancedMetrics` objects, not an array of names.** Both pages
publish it `Required: Yes` and typed *"Array of `EnhancedMetrics` objects"*, each object carrying one
`ShardLevelMetrics` array. Substrate rendered the stored `[]string` straight through, so a response
read `["IncomingBytes"]` where AWS answers `[{"ShardLevelMetrics": ["IncomingBytes"]}]` — an SDK
decoding into the generated type gets an unmarshal error, so this was a hard failure for a real client
rather than a cosmetic difference, the same class as
[#1017](https://github.com/scttfrdmn/substrate/issues/1017)'s ECR tags.

**A stream with nothing enhanced answers `"EnhancedMonitoring": []`.** Both readings were open —
`[]` or `[{"ShardLevelMetrics": []}]` — and `API_EnhancedMetrics` settles it: `ShardLevelMetrics`
publishes *"Array Members: Minimum number of 1 item"*, so an object holding an empty list is a shape
the model does not permit, while `EnhancedMonitoring` itself publishes no array minimum. The member is
present either way, as `Required: Yes` demands, and `[]` invents no impossible inner object. `ALL` is
expanded here as it is in the two monitoring responses, so a record written before #999 holding the
literal wildcard reads back as the seven metrics it means.

**`UpdateShardCount` checks four of its published bounds and refuses each with
`InvalidArgumentException`/400.**

| Request | Answer |
|---------|--------|
| An absent `ScalingType` | `InvalidArgumentException`/400 — `Required: Yes` |
| A `ScalingType` outside the enum, including the right word in the wrong case | `InvalidArgumentException`/400; `UNIFORM_SCALING` is the only published value |
| An absent `TargetShardCount` | `InvalidArgumentException`/400 — `Required: Yes`, and reported as absent rather than as a zero, so a caller who omitted the member is not told it was too small |
| `TargetShardCount` below 1 | `InvalidArgumentException`/400, per the published `Valid Range: Minimum value of 1` |
| `TargetShardCount` above 10 000 | `InvalidArgumentException`/400 |
| More than double, or less than half, the stream's current shard count | `InvalidArgumentException`/400. The bounds are inclusive: against 4 shards, 8 and 2 are accepted and 9 and 1 are not |
| A bad shape on a stream that does not exist | `ResourceNotFoundException`/400. Forced rather than chosen — the double and half bounds are stated against *"your current shard count"*, so they cannot be evaluated before the record is loaded |

**The code is `InvalidArgumentException` and not `ValidationException`**, although the page publishes
both. `ValidationException`'s gloss there is capacity-mode-specific — *"Specifies that you tried to
invoke this API for a data stream with the on-demand capacity mode"* — so it is not a general
validation code despite the name, the reading substrate has carried since
[#950](https://github.com/scttfrdmn/substrate/issues/950). `LimitExceededException` is the other
candidate and is declined: its gloss is about a resource exceeding a maximum, and the page attributes
it explicitly to one rule only, the 10 TPS call rate. The double, half and ceiling rules are stated
inside the `TargetShardCount` parameter entry, and *"a specified parameter exceeds its restrictions"*
is `InvalidArgumentException`'s own sentence.

**Three published restrictions are deliberately unmodelled**, because each needs state substrate does
not hold rather than a value in the request:

- *"Scale more than ten times per rolling 24-hour period per stream"* — needs a request history.
- *"Scale up to more than the shard limit for your account"* — needs an account quota.
- *"Scale a stream with more than 10000 shards down unless the result is less than 10000 shards"* —
  unreachable while the 10 000 ceiling above is enforced.

The seventh, *"Make over 10 TPS"*, is a call-rate limit rather than a property of any one request; it
is the one rule the page attributes to `LimitExceededException` by name.

A reshard reports `UPDATING` before it reports `ACTIVE` when a seed holds it, which the next section
sets out ([#1119](https://github.com/scttfrdmn/substrate/issues/1119)). Capacity mode is unmodelled altogether,
which is why `ValidationException` has no site at all today;
[#1118](https://github.com/scttfrdmn/substrate/issues/1118) carries it, together with the
`StreamModeDetails` member both describe shapes publish `Required: No`.


## A stream's status progresses

`API_CreateStream` says a new stream reports `CREATING` and then `ACTIVE`, and `API_UpdateShardCount`,
`API_MergeShards` and `API_SplitShard` each say the reshard reports `UPDATING` and then `ACTIVE`. Until
[#1119](https://github.com/scttfrdmn/substrate/issues/1119) every stream was `ACTIVE` from the request
that changed it, so a wait-for-`ACTIVE` loop exited on its first poll. The transition is now a seeded
progression — see [How a progression is seeded](/reference/concepts/seeds-and-replay#how-a-progression-is-seeded) for the rules every one
shares:

```
POST   /v1/kinesis/stream-status   {"streamName": "s", "pendingObservations": 2}
POST   /v1/kinesis/stream-status   {"streamARN": "arn:aws:kinesis:…:stream/s", "pendingObservations": 2}
POST   /v1/kinesis/stream-status   {"pendingObservations": 2}            (every stream)
DELETE /v1/kinesis/stream-status   (every seed; ?stream=<name or ARN> for one)
```

- **The seed counts observations, not time.** `pendingObservations` observations of a governed stream
  report its transient status, and the next reports `ACTIVE`. The default is no seed, so an unseeded
  stream reads `ACTIVE` exactly as before.
- **The status is the stream's most recent transition, not the seed's.** It is `CREATING` after
  `CreateStream` and `UPDATING` after `UpdateShardCount`, `MergeShards` or `SplitShard`; each reshard
  restarts the stream's countdown, so one seed covers every transition in a test. The reshard itself
  is applied at once — the shard count a describe reports is the new one throughout.
- **Which seed governs a stream:** one keyed by its ARN, then one keyed by its name (which applies to
  that name in every account and Region), then the wildcard. The countdown is kept per stream, so a
  wildcard seed is spent separately by each.
- **What observes:** `DescribeStream` and `DescribeStreamSummary`, the reads the pages tell a caller to
  poll, each spend one observation.
- **What refuses:** `UpdateShardCount`, `MergeShards` and `SplitShard` answer `ResourceInUseException`/400
  for a stream that is not `ACTIVE`, as each page publishes. The check peeks, so a refused reshard does
  not spend the polls a test seeded. The message is substrate's wording; the pages publish the code.
- **`DELETING` is not modeled.** `DeleteStream` removes the stream in the request, so a stream being
  deleted answers `ResourceNotFoundException` at once, which is what a completed delete answers.
- **Replay.** The seed is a control-plane write, recorded and re-applied in position (#1140), so a
  recorded `CREATING`, `UPDATING`, `ACTIVE` sequence replays identically.

## Resharding acts on the shards it names

Until [#1399](https://github.com/scttfrdmn/substrate/issues/1399), `MergeShards` and `SplitShard`
decremented or incremented the shard count and regenerated the whole shard list, so the shards a
caller named were never the ones that changed. Each now follows its page:

- **Hash key space.** A stream's hash keys run from 0 to 2^128−1, divided evenly between the shards a
  `CreateStream` or an `UpdateShardCount` makes. Substrate used to give each shard a toy range of a
  thousand keys, against which a published split key such as 2^127 fell outside every shard.
- **MergeShards.** The two shards must be *"adjacent if the union of the hash key ranges for the two
  shards form a contiguous set with no gaps"*. Both parents close (their `SequenceNumberRange` gains an
  `EndingSequenceNumber`) and one child covering the union opens, reporting `ShardToMerge` as its
  `ParentShardId` and `AdjacentShardToMerge` as its `AdjacentParentShardId`.
- **SplitShard.** `NewStartingHashKey` *"must be in the range of hash keys being mapped into the
  shard"*. The parent closes and two children open, both reporting it as `ParentShardId`: one holds the
  keys below `NewStartingHashKey`, the other the key and everything above it.
- **UpdateShardCount.** `UNIFORM_SCALING` closes every open shard and opens the target number dividing
  the hash key space evenly.
- **IDs and closed shards.** New shards continue the stream's own sequence (`shardId-000000000000`
  upward), so an ID is never reused. A closed shard stays listed in `DescribeStream`, as it does in
  AWS until it ages out of retention, which is not modeled. `OpenShardCount` counts open shards only.
- **Records.** `PutRecord` and `PutRecords` write only to open shards. A record goes to the open shard
  whose hash key range contains its hash key, and the response's `ShardId` names it
  ([#1409](https://github.com/scttfrdmn/substrate/issues/1409)). The hash key is `ExplicitHashKey` when
  sent; otherwise it is the MD5 of `PartitionKey` read as a 128-bit integer, as API_PutRecord states
  (*"An MD5 hash function is used to map partition keys to 128-bit integer values and to map associated
  data records to shards using the hash key ranges of the shards"*). The same keys reach the same shards
  in every run and every replay. `PutRecords` checks every entry's `ExplicitHashKey` before it writes
  any record, so a refused batch stores nothing.
- **Refusals,** each at HTTP 400:

  | Condition | Code |
  |---|---|
  | A shard the stream does not hold | `ResourceNotFoundException` |
  | Two shards that are not adjacent, or one shard named twice | `InvalidArgumentException` |
  | A closed shard | `InvalidArgumentException` |
  | A split key outside the shard's range, or equal to its starting key | `InvalidArgumentException` |
  | A split key not matching `^(0\|([1-9]\d{0,38}))$` | `InvalidArgumentException` |
  | A missing `ShardToMerge`, `AdjacentShardToMerge`, `ShardToSplit` or `NewStartingHashKey` | `InvalidArgumentException` |
  | An `ExplicitHashKey` not matching `^(0\|([1-9]\d{0,38}))$`, or at or above 2^128 | `InvalidArgumentException` |

  Every code is one the pages publish. The messages are substrate's own wording, and so are two
  readings. Refusing a split key equal to the shard's starting key is one: that key is *"in the
  range"*, but it would leave one child with no hash keys at all. The other is refusing an
  `ExplicitHashKey` of 2^128 or more: the published pattern admits 39-digit values up to 10^39-1, but
  such a value falls in no shard's range.
- **ListStreams** pages by `Limit` (1–10 000, default 100, and *"at most 100 results are returned"*) and
  `ExclusiveStartStreamName`, reporting `HasMoreStreams`. The page publishes no order, so names are
  listed lexicographically, which is what makes "start after this name" well defined. A `Limit` outside
  1–10 000 is `InvalidArgumentException`. `NextToken` is not read.

## Capacity mode, and the reshards it refuses

Until [#1118](https://github.com/scttfrdmn/substrate/issues/1118) every stream was implicitly
provisioned: `CreateStream` discarded `StreamModeDetails` and `UpdateStreamMode` was not routed. A stream
now records its mode, and `API_StreamModeDetails` publishes the one member, `StreamMode`, as
`PROVISIONED | ON_DEMAND`.

| Request | Answer |
|---------|--------|
| `CreateStream` with no `StreamModeDetails` | `PROVISIONED`, with the shards `ShardCount` asks for (one if it asks for none). The record stores no mode, so it is byte-identical to one written before #1118 |
| `CreateStream` with `ON_DEMAND` | Four shards, the count the quotas page's *"new data streams created with the on-demand capacity mode have 4 MB/s of write … throughput"* implies at 1 MB/s a shard. On-demand scaling is not modelled |
| `CreateStream` with `ON_DEMAND` and a `ShardCount` | `InvalidArgumentException`/400. **Substrate's reading:** the page once called the parameter *"no longer supported"* for on-demand streams and now says nothing |
| A `StreamMode` outside the enum, or missing | `InvalidArgumentException`/400, the code [#950](https://github.com/scttfrdmn/substrate/issues/950) settled on for an unusable parameter |
| `UpdateShardCount`, `MergeShards` or `SplitShard` on an on-demand stream | `ValidationException`/400 |
| `UpdateStreamMode` to the mode the stream has | 200, nothing changes. **Substrate's reading**; no page states it |
| `UpdateStreamMode` to the other mode | The mode changes, the shards stay, and the stream reports `UPDATING` under a seed — see [A stream's status progresses](#a-stream-s-status-progresses) |

The three reshard pages each say *"This API is only supported for the data streams with the provisioned
capacity mode"*, and the SDK model glosses their `ValidationException` as *"Specifies that you tried to
invoke this API for a data stream with the on-demand capacity mode."* The API Reference pages now gloss
the same code more generally, as data validation *"for this data stream"*, which the refusal still fits.
The quota of two mode switches in 24 hours, and `WarmThroughputMiBps`, are not modelled. The messages are
substrate's own.

## What a put may carry

[#1418](https://github.com/scttfrdmn/substrate/issues/1418) added the limits `API_PutRecord` and
`API_PutRecords` publish. Every refusal is `InvalidArgumentException`/400, the only validation code
either page's Errors section carries, and the messages are substrate's own.

| Limit | Source |
|-------|--------|
| `PartitionKey` is 1–256 characters | The length constraint on both pages. The member is `Required: No` only for a stream using the `AUTO` distribution strategy; substrate models only the default `USER_PARTITION_KEY`, under which the pages say *"a partition key is required"* |
| `Records` holds 1–500 entries | *"Array Members: Minimum number of 1 item. Maximum number of 500 items."* |
| A record is at most the stream's maximum record size, partition key included | 1 MiB unless `CreateStream` set `MaxRecordSizeInKiB`. The large-records guide: *"The default maximum record size for existing, and newly created streams is set to 1 MiB."* The blob is counted before base64 encoding |
| A `PutRecords` request is at most 10 MiB, partition keys included | *"up to a limit of 10 MiB for the entire request, including partition keys"*, on `API_PutRecords` and the quotas page. The older 5 MiB figure is no longer published |

What the request alone decides — the key lengths, the entry count and the 10 MiB total — is checked
before the stream is read, so it is refused even for a stream that does not exist. The per-record check
needs the stream's own maximum, so it runs after the stream is loaded, the same order `UpdateShardCount`
uses for its bounds against the current shard count. `UpdateMaxRecordSize` is not routed.

## The account and Region a record carries reach no response

`KinesisStream` declares `AccountID`, `Region` and `CreatedAt` under wire-visible `json` tags, plus
`EverTagged` as `ever_tagged`, because the record is what `MemoryStateManager` snapshots and a replay
reads back. None of them reaches a body:

- `DescribeStream` and `DescribeStreamSummary` answer `buildStreamDescription` and
  `buildStreamDescriptionSummary` (`emulator/kinesis_stream_shapes.go`). Both build their maps member
  by member.
- Every other operation answers a map or a list of names.
- `CreatedAt` reaches the wire only as the published `StreamCreationTimestamp`.

All eighteen routed operations are driven by
`TestKinesisWire_StreamResponsesCarryNoBookkeepingMember` and its shard-topology sibling in
`emulator/kinesis_wire_test.go`. Each walks the decoded document and fails on a bookkeeping member at
any depth. `ever_tagged` carries `,omitempty` and is set only by a tag write (#938). So the test
calls `AddTagsToStream` and reads the flag back from the record before walking any response, which
keeps the absence from passing for free (#1304).

`GetRecords` used to render each record's `ApproximateArrivalTimestamp` as an RFC3339 string.
`API_GetRecords` publishes it as a number, and its own sample is `1.441215410867E9`, so a typed SDK
could not decode the response
([#1338](https://github.com/scttfrdmn/substrate/issues/1338)). It is now epoch seconds to the
millisecond. The stored record keeps its `time.Time`, so a recorded run replays identically.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::Kinesis::Stream | StreamName | Forwards `ShardCount`, `StreamModeDetails` and `MaxRecordSizeInKiB` as declared. A provisioned stream declaring no `ShardCount` gets one shard, and an `ON_DEMAND` one sends none |

## A shard iterator carries its account and Region only inside the token

`kinesisIterator` holds the stream, shard, position, account (`a`) and Region (`r`) a shard iterator reads from, and it reaches a body only base64-encoded, as `GetShardIterator`'s `ShardIterator` and `GetRecords`' `NextShardIterator`. A real iterator is opaque too, so carrying both inside it is faithful.
`TestKinesisWire_IteratorReachesResponsesOnlyAsAToken` in `emulator/kinesis_wire_test.go` decodes each token first, requiring both members inside it, then walks each document for every spelling at any depth
([#756](https://github.com/scttfrdmn/substrate/issues/756)).

## Cost

Kinesis shard: $0.015 per shard-hour. PUT payload: $0.014 per million 25KB units.
