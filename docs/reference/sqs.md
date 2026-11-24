# SQS

**Endpoint:** `sqs.{region}.amazonaws.com`
**Protocol:** AWS Query (form-encoded, `Action=` parameter)

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateQueue | Supports FifoQueue, VisibilityTimeout attributes; [`QueueNameExists`](#queuenameexists) when a name is reused with differing attributes; [seedable `QueueDeletedRecently`](#seeding-queuedeletedrecently); stores a [create-time tag set](#a-tag-set-at-create-time-and-the-query-spelling-aws-publishes) |
| GetQueueUrl | [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent; [seedable consistency window](#seeding-the-create-then-lookup-consistency-window) |
| GetQueueAttributes | [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent; [seedable consistency window](#seeding-the-create-then-lookup-consistency-window); [attribute defaults](#queue-attribute-defaults); returns only the `AttributeNames` requested (`All` or by name; none requested returns none) and `InvalidAttributeName` for an undefined name; computes `ApproximateNumberOfMessages`, `ApproximateNumberOfMessagesNotVisible` and `ApproximateNumberOfMessagesDelayed` from the queue's messages at the simulated clock |
| SetQueueAttributes | [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent |
| DeleteQueue | [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent |
| ListQueues | Scoped to the caller's account and the endpoint's Region; filters on `QueueNamePrefix`; see [One queue name is one queue per Region](#one-queue-name-is-one-queue-per-region) |
| SendMessage | Returns MessageId; [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent; enforces [`MaximumMessageSize`](#message-size-enforcement); stores [message attributes](#message-attributes) and returns `MD5OfMessageAttributes`; enforces the [attribute count, name, type and `Number` rules](#attribute-rules) |
| SendMessageBatch | Enforces both the [per-message and batch-total size limits](#message-size-enforcement); stores [message attributes](#message-attributes) per entry; reports an [attribute-rule violation per entry](#batch-failures-are-per-entry) in `Failed` at HTTP 200 |
| ReceiveMessage | Supports MaxNumberOfMessages, WaitTimeSeconds; [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent; returns [message attributes](#message-attributes) for the names requested; returns the system attributes `ApproximateReceiveCount`, `ApproximateFirstReceiveTimestamp`, `SentTimestamp`, `SenderId` (and `MessageGroupId`/`MessageDeduplicationId` on FIFO) selected by `AttributeNames` or `MessageSystemAttributeNames`; a per-receive `VisibilityTimeout` (including `0`) overrides the queue's, and one outside 0–43200 is `InvalidParameterValue` |
| DeleteMessage | [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent |
| DeleteMessageBatch | |
| ChangeMessageVisibility | [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent; `ReceiptHandleIsInvalid` for a handle naming no message, `MessageNotInflight` for a message not in flight, `InvalidParameterValue` outside 0–43200 or for a non-integer, `MissingParameter` without the required `VisibilityTimeout` |
| ChangeMessageVisibilityBatch | Up to 10 entries; an entry's optional `VisibilityTimeout` defaults to 0, and a non-integer one is `InvalidParameterValue`; `EmptyBatchRequest`, `TooManyEntriesInBatchRequest`, `BatchEntryIdsNotDistinct` and `InvalidBatchEntryId` refuse the request; `ReceiptHandleIsInvalid` and `MessageNotInflight` fail their entry in `Failed` at HTTP 200 |
| PurgeQueue | [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent |
| TagQueue | [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent; merges into the existing set rather than replacing it; reads both the indexed `Tag.N.Key` spelling an SDK emits and the [unindexed `Tag.Key`/`Tag.Value` pair AWS actually publishes](#a-tag-set-at-create-time-and-the-query-spelling-aws-publishes), and enforces no tag-count limit because the API publishes none to enforce |
| UntagQueue | [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent; an unknown key is a no-op and so is an empty `TagKeys` |
| ListQueueTags | [`QueueDoesNotExist`](#queuedoesnotexist) when the queue is absent; `Tag` members sorted by key under the query protocol, a `Tags` map under JSON |

## One queue name is one queue per Region

Two Regions can each hold a queue named `orders`, and a `ListQueues` answers for the endpoint
it was sent to. Until #1088 neither held: the queue's state key was
`queue:{account}/{name}`, built from the last two components of a queue URL — which skips the
Region, because a queue URL carries it in the **host**.

The consequence was not merely colliding state. A `CreateQueue` for a name another Region
already held found that record, took its idempotent branch and **answered the other Region's
URL** — so the caller's next call addressed the wrong endpoint for a queue it had just created,
every operation on it succeeded against a record it had not asked for, and nothing refused
anything. The key is now `queue:{account}/{region}/{name}`, and the `msg:`, `msg_ids:` and
`fifo_dedup:` keys derived from it carry the same triple.

| Before #1088 | Now |
|---|---|
| `CreateQueue` for `orders` in `eu-west-1` after one in `us-east-1` answered the **us-east-1 URL** | each Region answers its own URL, and the two hold separate attributes and messages |
| A queue URL from another Region resolved that Region's queue | it resolves nothing at this endpoint and answers [`QueueDoesNotExist`](#queuedoesnotexist) |
| `ListQueues` reported every queue in every account and every Region | it reports the caller's own account's queues in the endpoint's Region |

**The Region comes from the request, not from the queue URL**, and that is the substantive
choice. Substrate's URL does carry the Region in its host, so a host parse is available — but a
URL reaching a handler may have been built by an SDK against a custom endpoint whose host says
nothing about a Region, so parsing it out is a guess at the one value the endpoint already knows
for certain. `API_GetQueueUrl` publishes **no Region parameter** for the same reason: AWS takes
it from the endpoint too. Taking it from the request is also what gives the third row above its
answer, where a host parse would have silently served the other Region's queue.

**The provenance is structural rather than quoted, and that is worth stating plainly.** AWS
publishes no sentence anywhere scoping a queue name to a Region — unlike DynamoDB's
`CreateTable`, which says so outright. The whole of the argument is the shape of the identifier
AWS hands back: the sample queue URL carries the Region in its host on all four published
protocol variants, and two endpoints are therefore two namespaces or the URL in the response is
wrong. This is the same reading, and the same kind of reading, that #943 recorded for Lambda —
see [A Lambda function and a DynamoDB table belong to one account in one Region](/reference/tagging#a-lambda-function-and-a-dynamodb-table-belong-to-one-account-in-one-region),
which sets out when an inference from an ARN's shape is and is not enough. SQS was the last
service in that class: Budgets, Organizations and IAM are global and legitimately carry no
Region, and ELB's prefix was already account- and Region-scoped.

**`ListQueues` became scoped in the same change, and it had to.** It read one flat `queue_names`
key holding every queue URL substrate had ever created, with no account and no Region in the
key, and filtered on `QueueNamePrefix` alone — so it already crossed accounts before anything
about the Region changed. Once two Regions can hold one name, an unscoped list answers one
endpoint with two URLs for the same name, which is incoherent rather than merely over-broad. The
index is gone: the list is a prefix scan over the queue records themselves, which has no second
copy to go stale, needs no prune on delete, and takes its scope from the key rather than from a
filter written beside it.

## QueueDoesNotExist

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

## Seeding the create-then-lookup consistency window

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

**A budget is consumed exactly once per miss, including under concurrency.** That
matters most for a wildcard seed, which is shared across every queue: seeding
`{"getUrlMisses":16}` and then driving concurrent lookups on sixteen *different*
queues reports exactly sixteen misses, not more. Substrate serializes the
read-decrement-write on one lock, the same way it does for the
[S3 conditional-conflict seed](/reference/s3#seeding-conditionalrequestconflict). The guarantee
is process-local, so it covers a single `substrate server` and not two of them
sharing one state backend.

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

## Queue attribute defaults

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

## A tag set at create time, and the query spelling AWS publishes

`API_CreateQueue` publishes a `tags` member (`String to string map`, `Required: No`) and its own JSON
sample sends one. Until [#1087](https://github.com/scttfrdmn/substrate/issues/1087) `createQueue`
decoded `QueueName` and `Attributes` only, so the call answered 200, the queue existed, and
`ListQueueTags` reported nothing — a consumer that tags on create, which is the only way to tag
atomically and the shape CDK and Terraform both emit, could not tell a dropped tag set from a bug in
its own code.

**The query spelling is the finding.** AWS's query sample is **unindexed**:

```
&Tag.Key=QueueType&Tag.Value=Production
```

and that is the sample on `API_TagQueue`'s page as well. AWS publishes **no** `Tag.N.Key` form for
either operation, anywhere on either page — even though the same `CreateQueue` sample indexes its
*attributes* (`&Attribute.1.Name=&Attribute.1.Value=`). Substrate's `TagQueue` parsed `Tag.N.Key` and
nothing else, so it handled a spelling AWS publishes nowhere and dropped the only one it does. The
indexed form still has to be read, because it is what a query-protocol SDK serialiser emits for a map
member and therefore the form real calls arrive in, so **both are accepted** — indexed first, and the
unindexed pair only when the indexed scan matched nothing, so a request carrying both resolves the
same way every time. One parser serves both operations, which widened `TagQueue` to the published
form; nothing it accepted before is refused now.

The JSON member differs in case between the two operations — lowercase `tags` on `CreateQueue`,
capitalised `Tags` on `TagQueue`, AWS's own inconsistency — and needs no code, because `encoding/json`
falls back to a case-insensitive member match when no exact one is found.

**Substrate enforces no tag limit here, and that is the published contract rather than an omission.**
The `tags` member carries no `Map Entries` and no `Length` constraint at all; the fifty-tag figure is
worded as a recommendation — *"Adding more than 50 tags to a queue isn't recommended"* — on both this
page and `TagQueue`'s; and neither Errors list carries a too-many-tags code. There is nothing to refuse
with, so sixty tags are stored. Kinesis's `CreateStream` publishes constraints and therefore refuses
(see [How many tags a stream may carry](/reference/kinesis#how-many-tags-a-stream-may-carry)); the difference is in the
references, not in substrate, and is pinned by a test so the missing SQS refusal is not later "fixed"
by analogy.

**A tag set on an idempotent hit is not applied.** A second `CreateQueue` for an existing queue with
the same attributes succeeds and reports the same URL; AWS publishes nothing about what happens to
tags on that request, and substrate's reading is that a call which created nothing tagged nothing.
`TagQueue` is the door that publishes retagging.

`EverTagged` is deliberately not stamped by either create path, per the recorded decision in
`tagging_ever_tagged.go`: the flag is only consulted when a record's tag set is empty, and the writer
that empties it counts the set before removing from it. See
[GetResources reports what has been tagged](/reference/tagging#getresources-reports-what-has-been-tagged-not-what-is-tagged).

## Message size enforcement

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

## Message attributes

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

### Attribute rules

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
where [`AWS:foo` is a legal key](/reference/ec2#reserved-tag-keys) — the two services document
different rules, and unifying them would break one of the two. A name merely beginning
with those letters and no period (`AWSfoo`) is not reserved.

A message breaking two rules always reports the same one: attributes are visited in
sorted name order, because a walk over the Go map would report one error on some runs and
the other on others.

### Batch failures are per entry

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

### Provenance

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

## The rules apply on send, not on receive

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

## QueueNameExists

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

## Seeding QueueDeletedRecently

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

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::SQS::Queue | QueueUrl | FifoQueue attribute supported |
| AWS::SQS::QueuePolicy | generated ID | `SetQueueAttributes` `Policy` on each queue in `Queues`; a change updates in place, and a queue the update drops has its policy cleared. Each queue records which QueuePolicy set its policy, and a delete — or a dropped queue — clears the policy only when this resource set it and the queue's live policy is still the one it applied, so a renamed CDK construct (the new policy is set before the old is deleted) and a caller's own later policy survive. A create that fails partway clears what it set, and an update that fails partway records every queue it touched (#1605) |

## The tag bookkeeping a queue carries reaches no response

`SQSQueue` declares `ever_tagged`, the flag that records a queue was once tagged (#938), because the record is what `MemoryStateManager` snapshots and a replay reads back. It does not reach a body: every response is built member by member.
`TestSQSWire_QueueResponsesCarryNoBookkeepingMember` in `emulator/sqs_wire_test.go` tags the queue and reads the flag back first, since an unset `omitempty` member is absent for free, then drives every queue operation and walks each decoded document for it at any depth
([#756](https://github.com/scttfrdmn/substrate/issues/756)).

## Cost

SQS requests: $0.0000004 per request.
