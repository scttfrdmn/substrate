# Errors

What substrate answers when a request body will not parse, and what every error response
carries.

## A request body that will not parse

An unimplemented operation is refused before a plugin sees it. A body that arrives at a
real operation and then fails to decode is the plugin's own refusal, and it was the
largest single source of unpublished error codes in the tree. **110 sites in fourteen
services answered a code their own operation does not publish** — in most cases a code
that appears nowhere in the service's documentation at all — and all 110 are corrected
([#923](https://github.com/scttfrdmn/substrate/issues/923),
[#1003](https://github.com/scttfrdmn/substrate/issues/1003),
[#950](https://github.com/scttfrdmn/substrate/issues/950)). Three further services — MSK,
CloudWatch and Bedrock Runtime — had the right code and the wrong message, and kept the
code.

Every one of the 110 already answered HTTP **400**, and 400 is what every published
counterpart carries, so this whole class was invisible to #923's status audit. It is a
code-string and message defect throughout.

### The rule: a per-operation code where one covers every guarded site, otherwise the common page

There is no single answer, because AWS does not give one. The rule the audit settled on,
applied uniformly:

1. If **every** operation carrying a guard publishes the same input-validation code in its
   own Errors section, use that code.
2. Otherwise fall back to the service's **common-errors page**, which is where an error
   belonging to no single operation belongs.

Step 1 is preferred because a per-operation code is a stronger citation: the page that
receives the request names it. Step 2 exists because a body that will not parse belongs to
no operation — substrate cannot know which operation the caller meant beyond the
`X-Amz-Target` header or the path, and it certainly cannot know which parameter was wrong,
since nothing was decoded.

The rule is why the table below is not one column of one code, and why two services under
one reference can legitimately differ: Budgets and Cost Explorer share
`aws-cost-management/latest/APIReference/CommonErrors.html`, but Budgets publishes
`InvalidParameterException` on all five of its guarded operations and Cost Explorer
publishes no validation code on any of its three, so Budgets takes step 1 and Cost Explorer
step 2.

It is also why Firehose loses `InvalidArgumentException` **even at the one site that
publishes it**. A missing `DeliveryStreamName` is one condition, and answering it with two
different codes depending on which operation received it is the failure mode a uniform
choice exists to avoid.

### The inventory

| Service | Sites | Answered | Answers now | Provenance |
|---|---|---|---|---|
| Step Functions | 15 | `InvalidRequest` | `ValidationError` / 400 | Common page. `ValidationException` is published on only 5 of the 15 |
| Kinesis | 16 parse + 3 member | `InvalidParameterException` | `InvalidArgumentException` / 400 | All 16 operation pages |
| EventBridge | 7 parse + 6 member | `InvalidParameterException` | `ValidationError` / 400 | Common page. EventBridge publishes no validation code on any guarded operation |
| Systems Manager | 12 | `InvalidRequest` ×10, `SerializationException` ×2 | `ValidationError` / 400 | Common page. None of the 12 publishes one |
| SageMaker | 6 parse + 4 conflated | `InvalidParameterValue` | `ValidationError` / 400 | Common page. 0 of 6 publish any validation error |
| ACM | 6 | `InvalidParameterException` | `ValidationError` / 400 | Common page. `InvalidParameterException` is on 3 of 6, `ValidationException` on 5 of 6 |
| Lambda | 4 parse + 3 member | `ValidationException` | `InvalidParameterValueException` / 400 | All 4 operation pages |
| Firehose | 3 parse + 3 member | `MalformedData`, `InvalidArgumentException` | `ValidationError` / 400 | Common page. `InvalidArgumentException` is on 1 of 3 |
| EFS | 4 | `MalformedData` | `BadRequest` / 400 | All 4 operation pages, which is why the answer is unaffected by #1064 finding that EFS's API-reference common-errors page **now exists** |
| Service Quotas | 3 parse + 1 member | `SerializationException`, `ValidationException` | `IllegalArgumentException` / 400 | All 4 operation pages |
| Budgets | 5 | `MalformedData` | `InvalidParameterException` / 400 | All 5 operation pages |
| Cost Explorer | 3 | `MalformedData` | `ValidationError` / 400 | Common page. None of the 3 publishes a validation code |
| SES v2 | 1 parse + 3 member | `MalformedData`, `BadRequest` | `BadRequestException` / 400 | All 5 operation pages |
| Batch | 2 of 8 | `InvalidParameterValue` | `ClientException` / 400 | All operation pages. **No common-errors page exists** |
| Bedrock Runtime | 2 | `ValidationException` (code correct) | `ValidationException` / 400, unchanged | Both operation pages |
| MSK | 2 parse + 9 member | `BadRequest`, message leaking | `BadRequest` / 400, unchanged | **Nothing published** — see below |
| CloudWatch | 1 | `SerializationException`, message leaking | unchanged | Substrate's reading — see the CloudWatch section |

The **Sites** column counts places in the source, not operations, and the two numbers differ
wherever handlers share a decoder. Service Quotas' three parse sites are reached from **five**
operations, because `sqUnmarshal` serves three of them; MSK's eleven from **eleven**. The wire
tests are written per *operation* rather than per site, since an operation is what a caller
can reach, which is why `invalid_body_inventory_test.go` names five Service Quotas cases
against the three counted here.

Fourteen services were audited and found **already correct**, and are listed here so they
are not re-derived: Cognito Identity Provider (28 sites), Secrets Manager (11), ECR (15),
ECS (15), CloudWatch Logs (10), Athena (7), API Gateway (6), API Gateway v2 (5), Cognito
Identity (5), Resource Groups Tagging (3), Bedrock (2), EventBridge Scheduler (2), EMR
Serverless (1), HealthOmics (1).

### That the common page is AWS boilerplate is what makes step 2 transfer

Step Functions', Systems Manager's and KMS's common-errors pages are **byte-identical**:
the same fifteen entries in the same order with the same statuses. The code is spelled
**`ValidationError`**, with no `Exception` suffix — "The input doesn't meet the required
format or constraints. Check that all required parameters are included and that values are
valid." The same entry, with the same status and the same gloss, is what EventBridge's,
SageMaker's, ACM's, Firehose's and Cost Management's pages publish, so a finding at one
service transfers rather than being a coincidence to re-derive per plugin.

One page is not the boilerplate and is worth naming, because it is why a service was
verified operation by operation rather than by transfer: **Secrets Manager's common-errors
page has 24 entries, not fifteen**. Its eleven sites were each checked, and each was
already correct.

### The reference has three common-errors generations, not two

Re-verified across twenty-two pages on **2026-09-18** for
[#1064](https://github.com/scttfrdmn/substrate/issues/1064), because a code sourced from one
of these pages before AWS regenerated it may now cite a page that no longer says it. There
are three live generations, and which one a service serves decides what step 2 can be cited
for:

| Generation | Entries | Verified identical on | `ValidationError` gloss |
|---|---|---|---|
| JSON / REST-JSON | **15** | KMS, Systems Manager, Step Functions, Lambda, SageMaker, Firehose, Cost Management, Organizations, EventBridge, ACM, RAM, CloudTrail, Glue, FSx, WAFv2, DynamoDB — **sixteen services, byte-identical** | *"The input doesn't meet the required format or constraints. Check that all required parameters are included and that values are valid."* |
| Query / `ec2` | **18** | EC2, RDS, IAM, ELB, CloudFormation — **five services, byte-identical** | identical to the JSON generation's, word for word |
| Legacy Query | **18**, a different set | **SQS alone** | *"The input fails to satisfy the constraints specified by an AWS service."* |

Three consequences worth stating once rather than re-deriving per plugin:

- **The two *current* generations agree on `ValidationError`'s status and gloss**, so step 2
  transfers across the protocol boundary as well as within it. They differ on exactly one
  status: `IncompleteSignature` is **403** on the JSON list and **400** on the Query list.
- **SQS is a live counterexample to consolidation, and this is load-bearing twice over.** Its
  page is the older generation, so `sqsInvalidBody`'s gloss (*"fails to satisfy the
  constraints"*) is correct for SQS and would be wrong anywhere else — the two spellings in
  the tree are two page generations, not one stale citation. And SQS's list still carries
  `InvalidAction` at 400, which is why [the unknown-action row](/reference/concepts/routing#where-the-two-rows-come-from)
  is a citation rather than substrate's invention. SQS also inverts three statuses against the
  current Query list: `AccessDeniedException` **400** (against 403), `ThrottlingException`
  **403** (against 400), `NotAuthorized` **400** (against 401).
- **Every other citation in the tree survived the sweep.** All fifty-five non-test
  common-errors citations in `emulator/` resolve to a page that still says what they quote, so
  #1064 produced no code corrections in the #950 class — the two corrections it did produce are
  both provenance, recorded below.

### `ValidationError` and `ValidationException` are different codes, and ACM publishes both

This distinction has been re-derived more than once, so it is recorded here. **They are not
spellings of one code:**

- **`ValidationError`/400** is a *common* error. It appears only on a common-errors page, in
  all three generations, and never in an operation's own Errors section.
- **`ValidationException`/400** is a *service-specific* error, published by many services in
  the Errors section of individual operations, and it appears on **no** common-errors page in
  any generation.

So a doc comment citing "the common-errors page" for `ValidationException` is citing something
no such page says, and one citing an operation page for `ValidationError` is doing the reverse.
**ACM is the worked example, because it publishes both with different glosses.** Its
common-errors page carries `ValidationError`/400 glossed *"The input doesn't meet the required
format or constraints…"*, while `ListTagsForCertificate` and `DescribeCertificate` each publish
a three-code Errors section — `InvalidArnException`, `ResourceNotFoundException`,
`ValidationException`, all 400 — in which `ValidationException` is glossed *"The supplied input
failed to satisfy constraints of an AWS service."* Substrate answers each from its own source:
`acmValidationError` cites the operation pages for `ValidationException`, and `acmInvalidBody`
cites the common page for `ValidationError`.

### Two services publish no common-errors page, and EFS is no longer one of them

Batch and API Gateway v2 have no fifteen-entry page to fall back to: both `CommonErrors` links
resolve to a `meta refresh` onto the service's welcome page. MSK's does the same. For each of
them step 1 was the only route available, and in each it was open — Batch publishes
`ClientException`, API Gateway v2's five sites were already correct.

**#950 recorded EFS as a third such service and #1064 found that stale.** The page that
redirects is the *user guide's* `api-errors.html`; the API reference's
[`CommonErrors.html`](https://docs.aws.amazon.com/efs/latest/APIReference/CommonErrors.html) now
publishes the full fifteen-entry list. This is the one direction the sweep was not looking in:
consolidation did not only change what a page says, it **created pages that did not exist**.

EFS's answer is unchanged regardless, and by the rule rather than by luck — step 1 outranks step
2, so a common page appearing underneath a satisfied step 1 changes nothing. `BadRequest`/400
stays, still cited to the Errors section of all four guarded operations.

This remains the strongest argument for preferring step 1. A rule that depends on a page some
services do not have is a rule with holes in it — and, as EFS shows, a rule whose holes move.

### One service publishes nothing to check against

MSK is the single row above whose code could not be verified against anything, and it kept
what it had. Three sources would normally settle it and each is absent:

- There is no common-errors page.
- The operation pages carry **no Errors section** — `clusters.html` documents response codes
  only, and the `Error` schema it names has members `{message, invalidParameter}`, with no
  `Code` member, so nothing on the page states a code string a caller could match on.
- `CreateClusterV2`, one of the two operations carrying a guard, **has no documentation
  page**.

So `BadRequest` stays, recorded as substrate's reading rather than presented as modelled,
the same treatment CloudWatch's `SerializationException` gets. Inventing a code on no
evidence would be worse than keeping one that has at least been shipped. The status is not
in doubt: 400 is what every site already answered and what the response codes on
`clusters.html` give a client error. If MSK ever publishes an Errors section, this is the
one row in the table that should be revisited.

### Two near misses at Step Functions, and why neither is the answer

| Candidate | Published | Declined because |
|---|---|---|
| `ValidationException` | Step Functions publishes it at 400 on **five** of the fifteen operations that carry a parse guard | Answering it everywhere leaves ten sites reporting a code their own operation does not publish — the same defect relocated. Answering it at only five makes one failure produce two codes inside one plugin. |
| `MalformedHttpRequestException` | On the common page at 400 | Its published scope is the transport layer: "the request body can't be processed. This typically happens when the request body can't be decompressed using the specified content encoding algorithm." A body that arrived intact and then failed to parse is not that. |

ACM is the same shape one operation smaller, and it is why that plugin carries two codes
side by side on purpose. `ValidationException` is published on five of ACM's six guarded
operations and **not** on `RequestCertificate`; `InvalidParameterException` is on only
three. So a `CertificateArn` breaking a published constraint answers `ValidationException`,
because every operation taking a certificate publishes it, while a body that would not parse
belongs to no operation and takes the code from the page that belongs to no operation.

**The trap worth naming: an error mentioned in prose is not an error a shape publishes.**
Five Systems Manager pages name `ValidationException` in prose — "if the specified name
for a parameter contains spaces between characters, the request fails with a
`ValidationException` error" — and **none of the five lists it in an Errors section**.
Four Step Functions pages do the same for the Distributed Map note. A reader who greps
for the word finds a pattern the reference does not actually publish, which is how the
wrong code survives a careful reading.

**And the trap that vindicated checking every guarded operation rather than a
representative one: Firehose.** `InvalidArgumentException` at 400 is published for
`CreateDeliveryStream` — the operation anyone would check first — and for neither of the
other two. `DescribeDeliveryStream` publishes exactly one error,
`ResourceNotFoundException`, and `DeleteDeliveryStream` two, `ResourceInUseException` and
`ResourceNotFoundException`. Applying the obvious replacement from the representative page
would have been wrong at two of three sites, which is this whole defect relocated.

### The message describes the request, not the emulator

**Twenty-two sites, across eight services, passed `encoding/json`'s own error text through
as the message**, so a caller was told which Go struct field failed to unmarshal by an
endpoint that is meant to look like AWS. Service Quotas passed it **bare**, with no prefix
at all; EFS, Firehose, SES v2, Budgets, Cost Explorer, MSK and CloudWatch prefixed it with
"invalid JSON body: " or "invalid JSON: ". The message is now substrate's: "the request body
is not valid JSON". Nothing a caller can act on was lost, because the only actionable fact
is that the body was not JSON.

CloudWatch is the one place where a decoder's text is still appended, and the distinction
is deliberate. Its CBOR arm reports `cborDecode`'s message, which is substrate's own and
describes the wire — "cbor: 3 trailing byte(s) after the top-level item" — so it is useful
to the caller who wrote those bytes. Its JSON arm no longer appends anything, because
`encoding/json`'s text describes Go.

### A parse failure is not a missing member

Five sites conflated the two, testing `if err != nil || body.X == ""` and reporting the
missing member for both — four in SageMaker and one in Bedrock Runtime's
`CreateModelInvocationJob`. They are split, because the two conditions call for different
fixes by the caller: one sends different bytes, the other adds a member, and telling a
caller their `jobName` is missing when their JSON is truncated sends them to look at the
wrong thing.

The remaining conflated sites, in Athena and Secrets Manager, are recorded here rather than
changed. There the required member is the only member the handler reads, so the two answers
coincide, and splitting them would add a branch that cannot change what a caller does.

### Why a green suite held all of them

Every test that builds a request from a Go value is structurally incapable of reaching these
guards: `json.Marshal` produces valid JSON by construction. The only way in is to hand the
server bytes. Nothing in the suite did, at any of the seventeen services, which is exactly
why the whole class survived — and why, when 110 codes were corrected, **exactly one
existing test failed**: the Service Quotas case that had asserted `SerializationException`
deliberately.

The suite now sends bytes: one case per guarded operation, asserting **the status and the
code together**, since a decoded error struct carries only the code and a consumer's retry
logic branches on the status. Each table names every operation that carries a guard rather
than a representative sample, because the defect was per-site duplication of one literal and
the assertion that matters is that no site was missed.
`emulator/invalid_body_inventory_test.go` carries **every guarded operation** —
the inventory has grown since this sentence was first written, by the third slice of #1007 and
by #1066 — plus **35 member-complaint sites in nine** (below), and
`emulator/invalid_body_code_test.go` the Step Functions and Systems Manager sites #1003 fixed.

Three different **35**s appear in this section and they count three different things, which is worth
saying once rather than leaving a reader to infer it: the 35 member-complaint sites just named
([#1062](https://github.com/scttfrdmn/substrate/issues/1062)), the 35 files that already contained a
checked guard when #1007 began (next section), and the 35 guards that were checked and still leaked
`encoding/json`'s own error text ([#1066](https://github.com/scttfrdmn/substrate/issues/1066), below).
No two of the three share a site.

Two of the 66 needed a resource to exist first, which was worth recording because it is the one
way a guard can be present, correct and still untested. Lambda's `AddPermission` and
`TagResource` looked up the function **before** they parsed the body, so on an empty emulator
both answered `ResourceNotFoundException`/404 and never reached the guard at all. The two sites
were therefore only reachable with a function in place, and a test that did not create one
would have reported success while asserting nothing.

### A guard that was never there: the discarded unmarshal error

The 110 sites above answered the *wrong* code. A second, larger class answered **no** code: 95 sites
across 38 files wrote `_ = json.Unmarshal(req.Body, &input)` and carried on with a zero-valued input,
so a body that would not parse was not refused at all. That is worse than a wrong code, because a
wrong code is at least an error: a discarded one produces a plausible success, or a refusal about
something else entirely. It is filed as
[#1007](https://github.com/scttfrdmn/substrate/issues/1007) and was corrected in three slices, since
each service needed its own published code under the two-step rule above. **All 95 are now corrected**,
and one site is deliberately retained with its reachability recorded — see the third slice below for the
check that keeps it the only one.

**SQS and AWS Health are the first slice**, and they are first because they were the only files in the
inventory with no checked guard anywhere to copy a code from — the other 35 **files** already contained
one, which is why 80 of the 95 sites need no fresh archaeology. (A different 35 from #1062's sites above
and from #1066's leaks below; this one counts files.) Both resolve to step 2:

| Service | Code | Status | Provenance |
|---------|------|--------|------------|
| SQS | `ValidationError` | 400 | Common Errors: *"The input fails to satisfy the constraints specified by an AWS service"*. No SQS operation page names an undecodable body. |
| AWS Health | `ValidationError` | 400 | Common Error Types: *"The input doesn't meet the required format or constraints"*. `DescribeEventDetails` publishes only `UnsupportedLocale`. |

Nearer-looking codes on the same pages are deliberately unused, and the reason is the same each time —
a code that misdescribes the fault sends the reader to the wrong place. SQS's `InvalidParameterValue`
names *"the input parameter"*, and a body that will not parse has no parameter to name;
`MalformedQueryString` is published at **404** and describes a query string; `MissingParameter` asserts
which parameter is absent, which is unknowable when nothing decoded. Health's
`MalformedHttpRequestException`/400 is specifically about a body that cannot be *decompressed* under
the declared content encoding, so it would send a caller to check its `Content-Encoding` header over a
syntax error in its own JSON.

**Four SQS operations were answering an actively misleading code.** `GetQueueAttributes`,
`DeleteQueue`, `ListQueueTags` and `PurgeQueue` read the queue URL through one shared helper and passed
it straight to the queue lookup, so a discarded decode yielded an empty URL and the lookup answered
`QueueDoesNotExist`: the emulator reported a missing queue when the queue was fine and the body was
not. That is the most expensive kind of wrong answer, because it sends the reader to look at
infrastructure rather than at the request. The helper now returns the refusal, and a test creates the
queue first so that a passing assertion is known to come from the guard rather than from a lookup that
happened to fail for another reason — while a *well-formed* request for an absent queue still answers
`QueueDoesNotExist`, so the lookup moved rather than went away.

**Nine SQS handlers decode the body twice, and the second decode catches a different fault.** They read
the queue URL through the shared helper and then decode the same bytes into their own struct, so a body
that will not *parse* has been refused one call earlier and their own guard can never see one. It is
still reachable, by a body that parses cleanly but contradicts a member's type — `{"QueueUrl": "…",
"Attributes": "not-a-map"}` — because the first decode skips a field its target does not declare without
type-checking it. That shape answers the same `ValidationError`/400, and the offending member is
deliberately **not** named: naming it would mean answering `InvalidParameterValue` for a type mismatch
and `ValidationError` for a syntax error on the same operation, which is exactly the one-plugin-two-codes
split #950 removed. The common-errors gloss covers both.

One discard in this slice is **retained**, with its reachability recorded rather than a second refusal
invented: `sqsRequestedAttributeNames` decodes the same body a second time for `ReceiveMessage`'s
attribute selectors, and `ReceiveMessage` has already refused an unparseable body through its own guard
before that helper runs. `sqs_plugin.go`'s FIFO deduplication decode is also left alone, because it
reads *stored state* rather than a request — a corrupted state blob is not a caller error and answering
a caller-error code for one would be a new defect.

**The second slice is the twelve services that already had the answer.** Twenty sites in files whose own
`*InvalidBody()` constructor was sitting a few hundred lines away, carrying the provenance #950 and #1003
established for it — Lambda (4), Step Functions (3), KMS (2), SES v2 (2), Firehose (2), and one each in
EventBridge, EFS, Systems Manager, Service Quotas, SageMaker, Kinesis and ACM. No new AWS reading was
needed for any of them, which is the whole reason this slice is separable from the first.

**Fourteen of the twenty were hiding behind a true statement.** They sat inside
`if len(req.Body) > 0 { _ = json.Unmarshal(…) }`, and six carried the comment
`//nolint:errcheck // optional body`. The body *is* optional on all fourteen — they are list operations
whose answer to an empty request is "everything" — but that is what the length check is for. Discarding
the error from a body that is **present** is a second, separate decision, and the stated reason for the
first was covering it. Both now hold at once: an absent body still lists everything, and a present body
that will not parse is refused. `TestInvalidBodyLeavesAnAbsentBodyAlone` asserts the first, because a
guard that refused an empty body would satisfy every refusal assertion in the tables and break every
consumer that lists without filters.

Three of the twenty needed more than the error checked:

- **`ListApps` had no length check at all**, so a plain guard would have refused the empty body AWS
  accepts. It gained the check the other thirteen already had. Both of its members are filters.
- **Service Quotas' site was bypassing `sqUnmarshal`, a helper in its own file** that already handles the
  absent body and returns the refusal. It was the only site in that file decoding by hand, which is why it
  was the only one still discarding. It now goes through the helper.
- **Lambda's four update operations parsed below their resource lookup**, so adding the guard there would
  have answered `404` for an unparseable body naming an absent function while `AddPermission` and
  `TagResource` — moved above the lookup by [#1006] — answer `400` for the same request. One plugin, two
  codes, for one class of caller error is what [#950] removed, so all six now parse first. This does not
  settle the tree-wide question recorded under *"Whether a body is parsed before the resource is looked
  up"* below; it makes one plugin answer one code.

Six of the twenty are not reachable on `POST` — four Lambda updates and EFS's `UpdateFileSystem` are
routed on `PUT`, and `ListEmailIdentities` on `GET` with its filters in the body — so they carry their own
table rather than being quietly absent from the `POST` one.

Two adjacent gaps are recorded rather than folded in: `DescribeEventDetails` publishes `eventArns` as
`Required: Yes` with Array Members 1–10 and substrate answers an empty `successfulSet` for an absent
list, and SQS's own required-member checks are not part of this class. Both are separate divergences
from "a malformed body is not refused", and are filed rather than mixed into a sweep whose whole value
is being mechanical.

**The third slice is the inline-literal tail**: the remaining sixty sites, across twenty-three files
that spelled a body-parse refusal as an `&AWSError{…}` literal at every site rather than through a
constructor. Those literals are why this slice is separate and why it is the one that found wrong codes:
a code repeated inline twenty-six times is a code no reviewer ever sees twice in one screen, so a
borrowed one survives indefinitely. The sixty refusals now go through twenty-one constructors collected
in one file, `emulator/invalid_body_refusals.go`, on the reasoning that the thing under review is a single
decision repeated twenty-one times — *which code does this service publish for a body that will not
parse?* — and a reviewer reading them side by side can see a borrowed code that a reviewer reading one
plugin cannot.

**Four of the twenty-one were sourced from a service that does not publish them.** Each was verified
against the service's own Common Errors page, its operation pages, and its exception-class list before
being replaced:

| Service | Was | Is | Why the old code was wrong |
|---------|-----|----|----------------------------|
| RAM | `MalformedQueryString`/400 | `ValidationError`/400 | Absent from RAM entirely. It is published at **404** on the Query-protocol page and describes the URL query string, not a body. |
| CloudTrail | `InvalidParameterCombinationException`/400 | `ValidationError`/400 | Means two parameters that cannot be used together; not published on `LookupEvents` at all. A body that will not parse yields no parameters to combine. |
| Glue | `InvalidParameterValueException`/400 | `InvalidInputException`/400 | Absent from Glue's Common Errors page, from `CreateDatabase`/`GetTables`/`StartJobRun`, and from all thirty-six `AWSGlueException` subclasses. Glue publishes `InvalidInputException`, *"The input provided was not valid."*, on every operation page. |
| FSx | `InvalidRequest`/400 | `BadRequest`/400 | Absent from FSx's Common Errors page, from `DescribeFileSystems` and `DeleteFileSystem`, and from all thirty-five `AmazonFSxException` subclasses. `InvalidRequest` is an **Amazon S3** code — the likely provenance of the mistake. FSx's Java class is `BadRequestException`, but the wire code carries no suffix, so the file's no-suffix instinct was right and only the stem was wrong. `BadRequest` is the first of `DeleteFileSystem`'s five published errors, which is where [#1063](https://github.com/scttfrdmn/substrate/issues/1063) landed the required-member refusal below. |

Only these four constructors changed a code outright; WAFv2's is a re-reading recorded below, and the
other sixteen carry forward what their file already answered. Twenty-one constructors cover twenty-one of
the twenty-three files; the other two needed none — OpenSearch's refusal is not an `AWSError` at all (see
below), and Batch already had `batchClientError`.

But a code correction cannot stop at the sites a sweep happens to touch. Leaving the *other*
body-parse guards in those five services on the old code would have made RAM, CloudTrail, Glue, FSx and
WAFv2 each answer **two different codes for the identical caller error** — the one-plugin-two-codes split
[#950](https://github.com/scttfrdmn/substrate/issues/950) removed, and a direct violation of the
invariant `invalid_body_inventory_test.go` states, which is one code per service for this condition. So
the forty-six pre-existing body-parse guards were folded in as well (RAM 1, CloudTrail 6, Glue 26, FSx 2,
WAFv2 11), located by their `"invalid JSON"` message text rather than by hand, which also removed
forty-six `err.Error()` message leaks.

What #950 left answering the old code in those five services was the **required-member** class, not the
body-parse one: Glue's `DatabaseInput.Name` check and its three `resolveGlueARN` failures, FSx's
`FileSystemId` check, and six WAFv2 `WAFInvalidParameterException` required-member literals — the last
inconsistent with WAFv2's own #755 reading below, under which an omitted member is `ValidationError`.
Each of those is a per-operation code decision rather than a mechanical edit, so they were filed
separately as [#1063](https://github.com/scttfrdmn/substrate/issues/1063) and corrected there: Glue's
four now answer `InvalidInputException`/400, FSx's one `BadRequest`/400, and WAFv2's six
`ValidationError`/400, matching each service's body-parse constructor. Three findings came out of doing
it, each recorded where it applies:

- **FSx's site is in `DeleteFileSystem`, not `DescribeFileSystems`**, where `FileSystemId` is
  `Required: Yes`. `DescribeFileSystems` marks `FileSystemIds` `Required: No` and an absent list means
  *describe them all*, which substrate already does — so the guard was in the right place and only the
  code was wrong.
- **Glue's `resolveGlueARN` returns the refusal itself** rather than a bare error its three callers wrap.
  All four of its failures are complaints about the *shape* of the string and none reads state, which is
  why `EntityNotFoundException` — published at 400 on all three tagging pages — applies to none of them:
  nothing has been looked up. A well-formed ARN that addresses nothing is that code's condition and is
  recorded as an open divergence, since `GetTags` on an untagged-and-absent resource answers an empty
  tag set.
- **WAFv2's `Id` check could not move wholesale.** `GetWebACL` marks `ARN`, `Id`, `Name` *and* `Scope`
  all `Required: No`, while `GetWebACL`'s three siblings and all three `IPSet` readers mark theirs
  `Required: Yes`. So the shared helper's check moved out to the callers that need it, and `GetWebACL`
  reports that the request addressed nothing rather than naming a member its own page says is optional.

**WAFv2 is a judgement recorded rather than a correction.** Its other guards answer
`WAFInvalidParameterException`, which *is* published at 400 — so unlike the four above it is not
sourced from nowhere. The body-parse sites answer `ValidationError`/400 instead, because substrate has
already drawn this line and tested it: `wafv2_createipset_validation_test.go` records, from
[#755](https://github.com/scttfrdmn/substrate/issues/755), that an omitted required member is
`ValidationError` while a *present-but-invalid value* is `WAFInvalidParameterException`. A body that
will not parse yields no members at all, so it falls on the first side of a distinction already made.
AWS glosses `WAFInvalidParameterException` as *"AWS WAF didn't recognize a parameter in the request"*,
all four of its published examples concern a value that was read, and it carries `Field`, `Parameter`
and `Reason` members substrate cannot truthfully fill for a request that never deserialized.
`WAFInvalidRequestException` does not exist.

**DynamoDB's `SerializationException` is kept although no AWS reference publishes it.** It is absent
from the Common Errors page, from the developer guide's error list, from `Programming.LowLevelAPI.html`
and from DynamoDB's model; the only AWS-published uses of the name are a Lambda client-side helper and a
Smithy Kotlin serde class. On the wire it arrives under the Coral namespace
(`com.amazon.coral.service#SerializationException`) rather than DynamoDB's own
`com.amazonaws.dynamodb.v20120810#`, because the protocol layer rejects the body before the request
reaches the service — which is exactly why the service never documents it. It is retained for wire
fidelity, since an SDK's retry classifier reads the code it receives and not the one the page omits.
**Its provenance is observed behaviour rather than the API model**, which is the one code in this slice
that is not a citation.

**OpenSearch's three sites are not an AWS control-plane API at all.** They are the domain's own REST
search API, so no AWS reference publishes a code for them and the refusal is not an `AWSError`:
`openSearchInvalidBody` returns `json_parse_exception`/400 through `openSearchError`, following the
file's existing convention of the engine's own lowercased exception name
(`resource_already_exists_exception`, `illegal_argument_exception`). The provenance is engine behaviour,
recorded as such.

**A structural finding that outlives this issue: AWS has consolidated the JSON-protocol Common Errors
boilerplate.** The RAM, CloudTrail, DynamoDB, Glue, FSx and WAFv2 Common Errors pages are now
byte-identical fifteen-code lists, distinct from EC2's longer Query-protocol list. The shared list
publishes `ValidationError`/400 — note `ValidationError`, **not** `ValidationException` — and its only
body-scoped code, `MalformedHttpRequestException`/400, is published as being about decompression and
content-encoding rather than JSON syntax. Any code in the tree that was sourced from a per-service
Common Errors page before the consolidation may therefore cite a page that no longer says it.

That risk was swept in #1064 and the result is recorded under
[the three generations](#the-reference-has-three-common-errors-generations-not-two): the
consolidation is **not** sitewide, SQS being a live counterexample, and all fifty-five citations in
`emulator/` survived. The two corrections were both provenance — EFS's page now exists, and
`InvalidAction` turned out to be citable.

**The "no bare `_ =`" criterion needed a script, because `errcheck` cannot state it.** To errcheck, a
bare `_ = json.Unmarshal(req.Body, …)` and a `//nolint:errcheck` carrying a written reason are the same
construct: the assignment is explicit, so the error is "handled". Telling those two apart is the whole
point of the criterion, so it is enforced by `scripts/check-discarded-unmarshal.sh` and
`make discarded-unmarshal-check`, in the same shape as `scripts/check-doc-versions.sh` — a rule a linter
cannot express is still a rule. The script's allowlist is keyed by file and each entry must carry its
reason, because the reason is the thing being reviewed; an allowlist without one is a suppression. It
holds exactly one entry, `sqs_messageattributes.go`, the retained discard described in the first slice.

**Every one of the hundred and six changed sites is asserted over the wire.** The inventory test's service
table carries 189 operations, the forty-six folded-in guards among them — a changed code that no test reads
is a code that can drift back. Three sites a bare server cannot reach have their own test, which creates the
prerequisite first: API Gateway v2 `UpdateApi`, AppSync `CreateApiKey` and Backup `UpdateBackupPlan` each sit
below a lookup of the resource their path names, so the lookup answers before the guard runs. That is the
lookup-first convention recorded under [Whether a body is parsed before the resource is looked up](#whether-a-body-is-parsed-before-the-resource-is-looked-up)
rather than a defect in this slice — but an unreached guard is an unchecked code, and `appsyncInvalidBody`
would otherwise have had no caller any test exercises at all.

**Which tail operations must still answer 200 for an absent body was measured, not reasoned.** Every one
was called with no body at all and the ones answering 200 were listed; fourteen of those are what their
page publishes and are now pinned. Nine more answered 200 where their page marks a member `Required: Yes`,
and those were deliberately left unpinned — SSO's three account-assignment operations, `ListIdentityPools`
and `ListUserPools` (`MaxResults`), Glue `GetTables` (`DatabaseName`), WAFv2 `ListWebACLs` and `ListIPSets`
(`Scope`), and DynamoDB Streams `GetRecords` (`ShardIterator`). Asserting the tree's current answer there
would have turned a missing required-member check into a pinned requirement, so they were recorded as the
missing-required-member class and filed separately.

**[#1062](https://github.com/scttfrdmn/substrate/issues/1062) is that class, and all nine now refuse.**
Each answers the code its own page publishes, which is four different codes rather than one:
`ValidationException`/400 for SSO's three (published on all three pages, and already the plugin's answer
at its two checked guards), `InvalidParameterException`/400 for the two Cognito services,
`InvalidInputException`/400 for Glue — the code [#1063](https://github.com/scttfrdmn/substrate/issues/1063)
centralised one release earlier — and `ValidationError`/400 for WAFv2, which is
[#755](https://github.com/scttfrdmn/substrate/issues/755)'s reading of the common list, cited rather than
re-derived.

Two of the nine needed a reading recorded rather than a citation. **Cognito user pools'
`InvalidParameterException` is glossed "…encounters an invalid parameter" and does not say *missing*,**
where Cognito Identity's says "Thrown for missing or bad input parameter(s)" — so `ListUserPools`'
refusal is substrate's reading of an absent required parameter as an invalid one, and `ListIdentityPools`'
rests on the page. **DynamoDB Streams `GetRecords` publishes no validation error of any kind** — its five
published errors are about iterators, limits, resources and trimmed data, and even an out-of-range `Limit`
is assigned to `LimitExceededException` — so its refusal comes from the JSON common list.
`ValidationException` was declined there even though eighteen sites in the same plugin answer it, because
those are DynamoDB's control-plane pages and borrowing across API surfaces is what
[#671](https://github.com/scttfrdmn/substrate/issues/671) settles against.

**The measurement missed sites in one direction only, and #1062 corrected for it.** A site that already
refuses an absent body never appears as a 200, so two required-member defects were invisible to it:
Cognito `ListUserPoolClients`, which unmarshals unconditionally and so refuses an absent body while
ignoring its required `UserPoolId`, and six further WAFv2 operations that default `Scope`. `Scope` is
`Required: Yes` on eight of the nine operations that read it and `Required: No` only on `GetWebACL`, so
eight now refuse an absent `Scope` and `GetWebACL` keeps the `REGIONAL` fallback — **a recorded
divergence, because no WAFv2 page publishes any default for the member** and an optional member's lookup
still needs a value. A present-but-unrecognised `Scope` answers `WAFInvalidParameterException`/400 at all
nine, which is #755's omitted-versus-invalid split applied to a second member.

Also corrected while measuring: `MaxResults` is `Required: Yes` over 1–60 on both Cognito list pages with
no published default, so the silent rewrite of an absent or non-positive value to 60 is gone at those two
operations — absent, zero, negative and above-60 are each outside the published range and each refuse.
`ListUserPoolClients` marks the same member `Required: No`, so its rewrite is a page-size defect rather
than a required-member one and stays for that issue.

### A guard that was there all along: the decoder's own error text on the wire

The two classes above are a guard answering the wrong code and a guard that was never written. A third
is narrower and had survived both sweeps: a guard that is present, runs, and answers with
`Message: "invalid JSON: " + err.Error()`. That hands the caller `encoding/json`'s text — a Go struct
field name and offset, from an endpoint whose whole purpose is to be indistinguishable from AWS. #950
removed twenty-two such leaks and #1007's third slice forty-six more;
[#1066](https://github.com/scttfrdmn/substrate/issues/1066) is the remainder, and it is **35 sites in
eight services**:

| Service | Sites | Old code | New code |
|---------|-------|----------|----------|
| Transfer Family | 9 | `InvalidRequestException`/400 | unchanged |
| CodeDeploy | 8 | `InvalidInputException`/400 | **`ValidationError`/400** |
| CodePipeline | 7 | `InvalidStructureException`/400 | **`ValidationException`/400** |
| CodeBuild | 6 | `InvalidInputException`/400 | unchanged |
| Backup | 2 | `InvalidRequestException`/400 | unchanged |
| IAM Identity Center | 1 | `ValidationException`/400 | unchanged |
| Redshift Data | 1 | `ValidationException`/400 | unchanged |
| AppSync | 1 | `BadRequestException`/400 | unchanged |

**Why these thirty survived two sweeps is the finding, not the leak.** #1007 went looking for a
*discarded* decode error, and Transfer, CodeDeploy, CodePipeline and CodeBuild discard none: every
handler in all four decodes exactly once and checks the error every time — nine, eight, seven and six
handlers, thirty guards, thirty leaks. So the sweep passed the four services over entirely, and none of
them had a single row in `invalid_body_inventory_test.go`. `assertNoDecoderText` has been the enforcing
helper since #950, and a site it never reaches is a site it never enforced. The other five leaks are
single handlers in services that *did* have rows — each one sitting beside a sibling whose guard #1007
had routed through a constructor.

**Two codes changed, and neither was visible from inside its own plugin.** In all four `Code*` services
the same `&AWSError{…}` literal appears at every site, which reads as deliberate consistency; the
error only shows up once the four codes sit next to each other in
`emulator/invalid_body_refusals.go`, which is what that file is for.

- **CodePipeline's `InvalidStructureException` is wrong twice over.** Where it *is* published —
  `CreatePipeline` and `UpdatePipeline` — it is glossed *"The structure was specified in an invalid
  format."*, meaning the pipeline structure, a value read out of the body after the body has parsed.
  And it is published on only those two of the seven operations substrate routes: `GetPipeline`,
  `DeletePipeline`, `StartPipelineExecution`, `GetPipelineState` and `GetPipelineExecution` do not carry
  it, so five of the seven sites were borrowing a code from a sibling operation, which
  [#671](https://github.com/scttfrdmn/substrate/issues/671) settles against. `ValidationException`/400,
  *"The validation was specified in an invalid format."*, is published on **all seven**, and because it
  is on the operation pages it outranks the common list's `ValidationError` under #950's ordering.
- **CodeDeploy's `InvalidInputException` is real, well-glossed, and unpublished where six of the eight
  handlers live.** *"The input was specified in an invalid format."* fits this condition better than
  anything else in the service, but CodeDeploy publishes it on only `CreateDeployment` and
  `CreateDeploymentGroup`. The other six publish nothing generic at all — only per-field codes
  (`ApplicationNameRequiredException`, `InvalidApplicationNameException`,
  `InvalidDeploymentGroupNameException`, `DeploymentIdRequiredException`,
  `InvalidDeploymentIdException`). Using the generic code service-wide would borrow it at six sites;
  using it at two and something else at six would spell one condition two ways in one file, the split
  #950 removed. A body that will not parse names no field, so the landing place is CodeDeploy's own
  common-errors `ValidationError`/400 — the same argument as RAM's and CloudTrail's above, reached from
  the opposite direction, and the third code this class has changed.

**Transfer's and CodeBuild's codes were checked as closely and kept.** Transfer publishes
`InvalidRequestException`/400 on every page checked, glossed *"This exception is thrown when the client
submits a malformed request."* — the strongest published fit of the eight services, because the sentence
names this condition outright. Its common page also publishes `MalformedHttpRequestException`/400, which
reads like the better name and is not: the gloss is about a body whose content-encoding could not be
decompressed, a failure that happens before any JSON is seen. CodeBuild publishes
`InvalidInputException`/400, *"The input value that was provided is not valid."*, everywhere including
`BatchGetBuilds`, where it is the *only* error listed — the same shape as ECS, a service whose generic
code is the only choice on offer.

**One site needed the prerequisite created first.** `CreateBackupSelection` loads the backup plan before
it decodes, so against a bare server the loader answers and the guard never runs — the lookup-first
convention recorded below. It joins the three sites in `TestInvalidBodyBelowAResourceLookup`, which is
also why its leak outlived `createBackupPlan`'s in the same file.

**The arithmetic is a second assertion rather than an extension of #1007's.**
`TestInvalidBodyTailIsFullyCovered` pins `tailSites = 60`, the sites #1007's third slice changed — a
closed historical figure about guards with a *discarded* error. #1066's thirty-five had no error to
discard; every one was already checked. The populations are disjoint in both directions, so
`TestInvalidBodyDecoderTextLeaksAreFullyCovered` counts the thirty-five separately (30 in four new
service entries, 4 in existing ones, 1 below a lookup) and 60 stays 60.

**Also corrected: four constructor doc comments and the file header overstated their own reach.** Each
said its service's checked guards "now call this" when what it had established was only that the guard
it was written beside did. Counting what was left is what found the five leaks in services #1007 had
already touched, so the comments now say which sweep routed which site, and the file header says that
`assertNoDecoderText` rather than a claim in a comment is what keeps the rule true.

### Whether a body is parsed before the resource is looked up

[#1006](https://github.com/scttfrdmn/substrate/issues/1006) settled this for Lambda's two sites
and, deliberately, for nothing else. **Lambda now parses first**: `AddPermission` and
`TagResource` refuse an unparseable body with `InvalidParameterValueException`/400 whether or
not the function exists, and a *well-formed* request for an absent function is still a 404, so
the lookup moved rather than went away. `AddPermission`'s required-member check moved with the
parse guard, because a body that will not parse and a body with no `StatementId` are both
mistakes visible in the request alone, and answering 404 for one and 400 for the other on the
same request is exactly the one-plugin-two-codes inconsistency #950 corrected.

**Which answer AWS gives is unverified, and this is substrate's reading rather than a published
fact.** No Lambda page states the precedence; both Errors sections list the two codes without
ordering them; substrate vendors no Smithy model or SDK, so the wire order is not citable from
this repository — which is why #1006's first acceptance criterion could not be met as written.
The reading is that a request whose shape is wrong is wrong whatever state exists, so it is
refused without consulting state. The case for the other order is real and is why this is
recorded rather than asserted: a caller naming a function that does not exist arguably wants to
hear that first.

**The tree does not follow one convention, and this release does not impose one.** Lookup-first
handlers remain elsewhere — AppSync's updates and creates, Backup's `UpdateBackupPlan` and
`CreateBackupSelection`, and Lambda's own `UpdateFunctionCode`, `UpdateFunctionConfiguration`,
`PutFunctionEventInvokeConfig` and `UpdateEventSourceMapping` among them — and the opposite
order is *documented with its own reasoning* where the question arises in its cursor form:
`ListObjectsV2` states that "the bucket-existence 404 above keeps precedence over this refusal …
the bucket is the resource the request addresses" (see *A cursor substrate did not issue*). A
tree-wide flip would therefore overrule stated reasoning rather than fill a gap, so it is out of
scope here and is deliberately not filed: the honest position is that substrate holds two
orders, each argued where it applies, and that AWS's own documentation is what leaves the
question open. A single shared hook is also the wrong shape for it — `Server.handleAWSRequest`
is one of three entry points (`StackDeployer.dispatch` and `Server.stateAtSequence` bypass the
gates), and it would need a per-service code table that does not exist. The two sites #1006
moved are the ones where the old order made a guard **unreachable**, which is a coverage fact
rather than a fidelity opinion, and that is the whole of what it settles.

**The member-complaint half of the inventory is covered the same way**, in
`TestMemberComplaintAnswersThePublishedCode` — every site in the *"+ N member"* column above,
which is to say every site whose code #950 corrected that is *not* a parse guard. These are the
easier half to leave unverified: a parse guard is one literal per handler, while these are
scattered complaints about a missing member or a malformed identifier, and the whole point of
correcting them was that one plugin must not answer two codes for one class of caller error.
They send `{}` rather than a truncated body, deliberately — `{}` *parses*, so it travels past
the parse guard and reaches the member check underneath, where an unparseable body would have
stopped one line earlier — and they assert the message alongside the code, because once every
site in a service answers one code the message is the only thing distinguishing them.

**Those guards used to be unreachable, and since
[#1009](https://github.com/scttfrdmn/substrate/issues/1009) every one of them is reached.**
`parseKafkaOperation` and `parseSESv2Operation` opened by trimming a trailing slash, and
`parseSchedulerOperation` normalised the same way by testing `path == "/schedules/"` explicitly, so a
request naming an empty path parameter collapsed onto the collection route one case earlier in the
same switch: `GET /v1/clusters/` dispatched `ListClusters`, not `DescribeCluster` with an empty ARN,
and `GET /schedules/` answered every schedule in the group to a caller that had asked for one. The
inventory is every router in the tree that normalised a trailing slash, with the reachability of each
empty-parameter guard beneath it:

| Router | Normalisation | Guards below it | Now |
|---|---|---|---|
| `parseKafkaOperation` | `strings.TrimRight(path, "/")` | `describeCluster`, `deleteCluster`, `describeClusterV2` | reachable; trim removed |
| `parseSESv2Operation` | `strings.TrimRight(path, "/")` | `getEmailIdentity`, `deleteEmailIdentity` | reachable; trim removed |
| `parseSchedulerOperation` | explicit `path == "/schedules/"` arm | `getSchedule`, `createSchedule`, `updateSchedule`, `deleteSchedule` | reachable; the arm and the `name != ""` route condition removed |
| `parseCloudFrontOperation` | `strings.TrimSuffix(path, "/")` | none — the **inverse** hole, below | filed |
| `parseAccountOperation` | `strings.TrimSuffix(path, "/")` | none; the service has no path parameters | harmless, left alone |

Nine guards in three routers were dead, not the five in two originally recorded; Scheduler's four were
invisible to a `TrimRight` search. MSK's `getBootstrapBrokers` and `listNodes` escaped only because a
literal segment follows the ARN, so the empty parameter was interior rather than trailing and
`/v1/clusters//nodes` reached them. `parseEFSOperation` never trimmed, which is why all nine of EFS's
equivalent guards were always reachable, and it is the precedent the three fixed routers now follow:
an empty path parameter reaches the operation the caller named and is refused there. **AWS publishes
nothing about a trailing slash** for any of these services — whether it answers a validation error, a
404 or the collection operation is unverified, and MSK is the weakest service in the tree to settle
from documentation for the reasons given below. Substrate's reading is that a refusal is recoverable
where a wrong operation is not: a caller that built a path from an empty variable is told which
parameter was empty rather than served a listing it did not ask for.

CloudFront is the same defect inverted and is **not** fixed here: `/distribution/` reaches
`GetDistribution` with an empty ID and there is no guard below it to reach, so the fix is a guard to
add rather than a fold to remove. It is filed separately.

**One service is outside this rule by design.** CloudWatch speaks Smithy RPC v2 CBOR, and
its refusal names the modelled shape rather than a code from a common-errors page; neither
the protocol nor the CloudWatch model names a shape for an undecodable body, so it answers
`SerializationException` at 400 as substrate's own choice. See the CloudWatch section.

## What an error response carries

A refusal is the call a consumer logs and the one whose request ID a support case quotes, so an XML
error document carries the **request's own ID** — the value `Event.RequestID` records and
`replayRequestID` reproduces — exactly as a success envelope does
([#1149](https://github.com/scttfrdmn/substrate/issues/1149) for ELB, #866 for the rule). Until
[#1241](https://github.com/scttfrdmn/substrate/issues/1241) no Query error carried an ID at all, and
EC2's carried the fixed string `SUBSTRATE`. IAM's success envelopes carried the literal
`stub-request-id` until [#1413](https://github.com/scttfrdmn/substrate/issues/1413), and now carry
the request's ID.

| Family | Plugins | Document | Source |
|---|---|---|---|
| Query (and REST-XML) | CloudFormation, CloudFront, CloudWatch (Query), ElastiCache, ELB, IAM, RDS, Redshift, Route 53, SNS, STS | `<ErrorResponse><Error><Type>Sender</Type><Code>…</Code><Message>…</Message></Error><RequestId>…</RequestId></ErrorResponse>`. `Type` is `Receiver` for a 5xx or a modeled server fault ([#1413](https://github.com/scttfrdmn/substrate/issues/1413)) | smithy [awsQuery](https://smithy.io/2.0/aws/protocols/aws-query-protocol.html) and [restXml](https://smithy.io/2.0/aws/protocols/aws-restxml-protocol.html) error serialization; the SQS developer guide's [XML error response](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-api-responses.html) quotes the same document from the service |
| EC2 | EC2 | `<Response><Errors><Error><Code>…</Code><Message>…</Message></Error></Errors><RequestID>…</RequestID></Response>` — plural `Errors`, capital-D `RequestID` | [Error codes for the Amazon EC2 API](https://docs.aws.amazon.com/AWSEC2/latest/APIReference/errors-overview.html), "Example error response"; smithy [ec2Query](https://smithy.io/2.0/aws/protocols/aws-ec2-query-protocol.html) |
| S3 | S3 | bare `<Error>…<RequestId>SUBSTRATE</RequestId></Error>` | unchanged — see below |
| JSON, REST-JSON, RPC v2 CBOR | the rest | no request ID in the body | — |

`RequestId` is a **sibling** of `<Error>`, closing the document, not a child of it. No other Query
service's API reference publishes a sample error response: the Common Errors pages of CloudFormation,
CloudFront, CloudWatch, ElastiCache, IAM, RDS, Redshift, SNS and STS list codes and statuses only, and
neither ELB generation's reference publishes one either. So the per-service document is the
protocol's, not one read off each service's page.

The ID is never minted per call. A replayed refusal is dispatched under the recorded ID, so a
document built from it is byte-identical to the recording; a minted one would be reported as a
difference on every replayed refusal. IAM's handlers build their own error document rather than
raising an error for the shared writer, and the ID is stamped into it once, at the plugin's dispatch,
so the two paths answer the same element.

S3 keeps the fixed `SUBSTRATE`: its documents are built by the plugin without a request context, and a
pipeline-raised S3 error (an injected fault, a quota refusal) is built by the same function so the two
are byte-identical (#480). Threading the ID into one half only would break that. An in-process caller
with no request in hand also renders `SUBSTRATE`.
