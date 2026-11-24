# CloudWatch Logs

**Endpoint:** `logs.{region}.amazonaws.com`
**Protocol:** JSON (`application/x-amz-json-1.1`, `X-Amz-Target: Logs_20140328.{Op}`)

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateLogGroup | Records `kmsKeyId`, which `DescribeLogGroups` reports back; nothing is encrypted (#1519) |
| DeleteLogGroup | Also removes the group's streams and their events |
| DescribeLogGroups | Reports both ARN forms — see below, and `kmsKeyId` when the group has one; refuses a `nextToken` it did not issue |
| PutRetentionPolicy | `retentionInDays` must be one of the API's 22 enumerated values |
| DeleteRetentionPolicy | The documented way to make a group's events never expire |
| CreateLogStream | |
| DeleteLogStream | |
| DescribeLogStreams | Refuses a `nextToken` it did not issue |
| PutLogEvents | Accepts up to 10,000 events per call |
| GetLogEvents | Issues both `nextForwardToken` and `nextBackwardToken`; reads `startFromHead`; refuses a `nextToken` it did not issue |
| FilterLogEvents | Evaluates `filterPattern` as [a filter pattern](#filter-patterns); reports `searchedLogStreams`; refuses a `nextToken` it did not issue |
| TagResource | Takes the **unsuffixed** log-group ARN — see below; refuses the 51st tag |
| UntagResource | Same ARN rule; an absent key and an empty `tagKeys` are both no-ops |
| AssociateKmsKey | Replaces the group's `kmsKeyId` by `logGroupName`; the key is recorded intent and is not resolved against KMS. The `resourceIdentifier` form is not modelled |
| DisassociateKmsKey | Clears the group's `kmsKeyId` by `logGroupName` |
| ListTagsForResource | Same ARN rule; an untagged group answers `{"tags":{}}` |

Lambda auto-creates `/aws/lambda/{name}` log groups.

## Filter patterns

`FilterLogEvents` evaluates `filterPattern` with the grammar the user guide's filter pattern syntax
page publishes ([#1596](https://github.com/scttfrdmn/substrate/issues/1596)). Until then it was matched
as one literal substring, so a JSON pattern matched nothing.

- **Terms.** A term matches when the message contains it, case-sensitively. Several terms must all
  match, `-term` excludes, and `?term` alternatives match any one of them. A `?` term mixed with any
  other term is ignored, as the page states. A `"quoted phrase"` is one term, and `" "` matches every
  event. A term matches as a substring, not a whole word, because the page does not say which.
- **JSON.** `{ $.a.b op value }` with `=`, `!=`, `<`, `>`, `<=`, `>=`, combined with `&&` and `||`
  (`&&` binds tighter) and parentheses. Selectors take array indexes (`$.a[0]`), one wildcard
  (`$.a[*]`, `$.*`) and bracket notation for a key with a period (`$.['cluster.name']`). A value with
  `*` is a glob. `IS NULL`, `IS TRUE`, `IS FALSE` and `NOT EXISTS` test a field. Two numbers compare
  numerically (`1e+3` equals `1000`), and anything else compares as text. A numeric operator needs a
  number. A field that is missing, an object, an array or null satisfies no comparison, `!=` included.
  A message that is not JSON never matches.
- **Refused.** Every refusal is `InvalidParameterException`/400 with substrate's own message. That
  covers a pattern that does not parse, `IS NOT` and a bare `EXISTS` ("currently aren't supported",
  per the page), and more than one wildcard in a selector or three in a pattern. It also covers
  space-delimited patterns (`[ip, user, ...]`) and regular expressions (`%...%`), which are
  [#1638](https://github.com/scttfrdmn/substrate/issues/1638).

No metric filters or subscription filters exist in substrate yet, so `FilterLogEvents` is the only
operation that evaluates a pattern.

## Response members are camelCase

CloudWatch Logs is a JSON-1.1 service whose members are camelCase, and an SDK
matches response members against the service model **case-sensitively** — a
PascalCase member does not fail to parse, it parses to *nothing*, so a caller
receives one empty object per resource with an HTTP 200 and no error. Earlier
releases had `DescribeLogGroups`, `DescribeLogStreams` and `GetLogEvents` doing
exactly that (`FilterLogEvents` did not), so a `len()` assertion passed while every
field read raised `KeyError`. All four now emit the API's member names.

`DescribeLogGroups` reports **both** ARN forms the reference documents as distinct
members: `logGroupArn` without a trailing `:*`, which is what a
`logGroupIdentifier` input or a tagging API wants, and `arn` with it, which is
what an IAM policy wants for most actions. They differ only in that suffix, and
which of the two the tagging operations accept is [a rule of its
own](#tagging-a-log-group-takes-the-unsuffixed-arn).

A group with no retention policy omits `retentionInDays` entirely rather than
reporting `0`, because the API has no value meaning "never" — the member's absence
is the signal, which is why `DeleteRetentionPolicy` exists.

`PutRetentionPolicy` accepts only the enumerated day counts (1, 3, 5, 7, 14, 30,
60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288,
3653); anything else — including a plausible 45 or 100 — is an
`InvalidParameterException`. Note that this service returns
`ResourceNotFoundException` at **HTTP 400**, not 404, as its reference documents:
the error code travels in the body's `__type`, not the status line. Every
group- and stream-level not-found in the plugin answers that 400, and the three
reads that answered an empty `200` for an absent log group now refuse it too —
see [a log group that does not exist is not an empty log
group](/reference/concepts/listing-order#a-log-group-that-does-not-exist-is-not-an-empty-log-group).

## Every refusal answers the status its page publishes

CloudWatch Logs is a JSON-1.1 service. The error code travels in the body's `__type`, and every client
error its pages publish shares the status line **400**. A consumer that classifies on the status, as
an SDK's retry logic does before it reads the code, must see that 400.

[#1251](https://github.com/scttfrdmn/substrate/issues/1251) checked every refusal the plugin answers
against its operation's page:

| Operation | Code | Substrate | Page |
|-----------|------|-----------|------|
| `CreateLogGroup` | `ResourceAlreadyExistsException` | 400 (was 409) | 400 |
| `CreateLogStream` | `ResourceAlreadyExistsException` | 400 (was 409) | 400 |
| `DeleteLogGroup`, `DeleteLogStream`, `CreateLogStream`, `PutLogEvents`, `GetLogEvents`, `DescribeLogStreams`, `FilterLogEvents`, `PutRetentionPolicy`, `DeleteRetentionPolicy` | `ResourceNotFoundException` | 400 | 400 (#1224) |
| every routed operation | `InvalidParameterException` (missing member, bad body, retention days, unissued token) | 400 | 400 |
| `TagResource` | `TooManyTagsException` | 400 | 400 |
| `TagResource`, `UntagResource`, `ListTagsForResource` | `ValidationException` (the `:*` ARN form) | 400 | observed (#1273) |

Both creates refuse through one constructor, `cwLogsAlreadyExists`, so they cannot drift.
`TestCWLogs_ACreateOfAnExistingResourceIsRefusedAt400` asserts the status and the `__type` code over
the wire, and that the refused create leaves the existing resource in place. No refusal in the plugin
answers any status but 400 now.

All four paginating operations refuse a `nextToken` substrate could not have issued with
`InvalidParameterException` / 400, rather than answering a well-formed page one — see
[CloudWatch Logs carried one block four times](/reference/concepts/listing-order#cloudwatch-logs-carried-one-block-four-times-and-the-page-says-where-a-token-comes-from)
for the provenance, the one divergence the change deliberately leaves in place (the published
24-hour token expiry, which is declined rather than deferred), and the argument that the token
is validated before any listing is read.

## Tagging a log group takes the unsuffixed ARN

The three tagging operations accept the **unsuffixed** log-group ARN and refuse the
IAM-policy form:

```
arn:aws:logs:us-west-2:123456789012:log-group:/aws/lambda/foray-gateway       accepted
arn:aws:logs:us-west-2:123456789012:log-group:/aws/lambda/foray-gateway:*     ValidationException: Invalid resourceArn
```

The trap is that substrate hands a caller the refused form itself: as the section
above records, `DescribeLogGroups` reports both, and the suffixed one travels under
the shorter member name `arn`. Reaching for it is the natural mistake, since it is
also the ARN an IAM policy wants, and a hand-written fake that accepts any ARN keeps
a tagging path green offline and then fails on every group of a live deploy — which
is the report [#1273](https://github.com/scttfrdmn/substrate/issues/1273) was filed
from.

The same rule applies to all three operations, not to the write alone: a convergence
path reads before it writes, and a `ListTagsForResource` that accepted the suffixed
form would report a group as untagged rather than telling the caller which ARN to
send.

**Provenance.** `ValidationException` / `Invalid resourceArn` is **observed** real-AWS
behaviour (us-west-2), not published: the pages for all three operations list
`InvalidParameterException`/400, `ResourceNotFoundException`/400 and
`ServiceUnavailableException`/500, plus `TooManyTagsException`/400 on `TagResource`,
and none of them lists a `ValidationException` at all. What *is* published supports
refusing — `resourceArn` carries Pattern `[\w+=/:,.@-]*`, a character class with no
`*` in it, so the suffixed form violates the request model before any resource is
resolved. Only the code and the message text come from observation.

A `:log-stream:{name}` suffix is refused the same way, since a stream is not one of
the two types these operations accept. The other type that *is* accepted —
`destination:{name}` — answers `ResourceNotFoundException`, because substrate models
no destinations at all: the ARN names a taggable type and a resource that cannot
exist here, so it is the resource that is missing rather than the ARN that is wrong.

Three further readings worth knowing:

- `tags` is a **string-to-string map** on both the `TagResource` request and the
  `ListTagsForResource` response, not the array of `{key, value}` objects most
  services use. An untagged group answers `{"tags":{}}` rather than omitting the
  member, so a caller comparing maps never has to tell nil from empty.
- `CreateLogGroup`'s inline `tags` are persisted and read back by
  `ListTagsForResource`. They were decoded and discarded before #1273, so a group
  created with tags inline reported none.
- The 50-tag ceiling is enforced at `TagResource` only, against the **merged** set —
  a request of one tag against a group already holding fifty is refused with
  `TooManyTagsException`. `CreateLogGroup` accepts more, because its page publishes
  no error code for exceeding the `tags` map maximum and inventing one would assert a
  refusal AWS documents nowhere ([#671](https://github.com/scttfrdmn/substrate/issues/671)).

**The Resource Groups Tagging API reaches a log group too** (#1282). Its `logs` arm parses
the ARN through the same function these three operations use, so the two cannot disagree
about which ARN names a group: the unsuffixed form is accepted, and the `:*` policy form, a
`:log-stream:` ARN and a destination ARN are each refused (as `InternalServiceException`,
the tagging API's answer for a type it cannot handle) rather than keyed to the group. A tag
written through either side is read back through the other, and `TagResources` enforces
the same 50-tag ceiling with the same `TooManyTagsException`.

`GetResources` reports a log group under its **unsuffixed** ARN — the one a sweep can hand
straight back to `TagResources` or `UntagResources` — and follows the tagging API's rule:
a tagged group is reported, a group whose tags were all removed is reported with
`"Tags": []`, and a group that never had a tag is not. The second state is recorded in a
side-car key beside the group (`loggroup_tagged:{account}/{region}/{name}`), not in the group's
own record, so `CWLogGroup` gains no bookkeeping member; `DeleteLogGroup` removes it, so a
group re-created under the same name starts never-tagged. Every tag writer of a log group —
`TagResource`, `UntagResource` and the tagging API — writes it, which is why the
side-car is safe here where #938 found one unsafe across services.

## GetLogEvents pages by a pair of tokens

`GetLogEvents` is the one Logs paginator whose response carries **two** tokens, and whose termination
rule is stated in terms of both. The reference's overview says *"As long as the `nextBackwardToken` or
`nextForwardToken` returned is NOT equal to the `nextToken` that you passed into the API call, there
might be more log events available"*, and each member's own description adds *"If you have reached the
end of the stream, it returns the same token you passed in."* Both are published with Length Constraints
minimum 1, and the overview states flatly that *"The returned tokens are never null."*

Substrate emitted `nextForwardToken` only when a further page existed, and `nextBackwardToken` never. A
caller written from the page therefore could not terminate: it sent an empty token, was returned an empty
one, and either stopped on its first page believing the comparison had been met or spun. Both tokens are
now present on every answer, including for an empty stream, and each names a position such that
presenting it back yields the same token — so the published rule works as written in both directions
([#1223](https://github.com/scttfrdmn/substrate/issues/1223)).

**The forward token names the position after the page; the backward token the position before it.** That
is what makes equality fall out arithmetically at each end rather than needing a special case: walking
forward past the tail clamps to the same offset and returns the same token, and walking backward past the
head clamps to zero and does the same. It is also why `GetLogEvents` does **not** share
`pageByOffsetToken` with the other three — that helper omits its token on a final page, which is correct
where a token's *absence* means done, and wrong here where *equality* means done. A full final page
therefore still carries a forward token, and presenting it costs one round trip to an empty page. AWS
describes exactly that: *"Partially full or empty pages don't necessarily mean that pagination is
finished."*

**The direction is on the wire, using the prefixes the reference's own examples publish.** The page's
example responses show `"nextBackwardToken": "b/31132629274945519779805322857203735586714454643391594505"`
and `"nextForwardToken": "f/31132629323784151764587387538205132201699397759403884544"`, so substrate
prefixes `b/` and `f/` ahead of its offset encoding. The prefix is load-bearing rather than cosmetic:
without it a backward token is a bare offset indistinguishable from a forward one, and would be read as a
forward position — the same class of undetectably wrong page the token refusal above exists to prevent.
One consequence is a **wire-shape change**: `GetLogEvents` no longer accepts the bare offset token the
other three issue, and its own tokens no longer decode under them. A token presented without a valid
direction prefix is refused with the same `InvalidParameterException` / 400.

**`startFromHead` is read, and decides only the first page.** The reference gives it a default of `false`
— *"If the value is true, the earliest log events are returned first. If the value is false, the latest
log events are returned first"* — so a tokenless call now answers the **tail** of the stream, where
substrate previously always started at the head. Once a token is present the token's own direction
decides. The page also says *"If you are using a previous `nextForwardToken` value as the `nextToken` in
this operation, you must specify `true` for `startFromHead`"*; substrate does **not** enforce that,
because no Logs page publishes a code for violating it and the token already carries its direction —
inventing a refusal AWS does not document refusing is the thing
[#671](https://github.com/scttfrdmn/substrate/issues/671)'s scope decision rules out.

**The 24-hour expiry is declined here too.** Both members publish *"The token expires after 24 hours."*
and neither publishes a code for presenting an expired one; the argument is the same as for the other
three and is set out above. A token past the end of a stream that has since been trimmed is **clamped**
to a final empty page rather than refused, so a walk whose events were deleted mid-loop terminates
instead of erroring.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::Logs::LogGroup | LogGroupName | |
| AWS::Logs::LogStream | LogStreamName | |

## Cost

CloudWatch Logs ingestion: $0.50 per GB. Storage: $0.03 per GB-month.
