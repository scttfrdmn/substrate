# Fault injection

Fault injection is cross-service rather than a plugin, so it lives here rather than in a
per-service section. Rules are armed in process through `NewFaultController`, from a
configuration file's `fault:` block, or over the wire:

```
POST   /v1/fault/rules   {"enabled":true,"rules":[{…}]}
GET    /v1/fault/rules   → the live configuration, each rule carrying its fired count
DELETE /v1/fault/rules   → disable and clear
```

Faults are evaluated **before the request reaches a plugin**, so a rule fires whether or
not the operation it names is implemented, and no state is written for a request a fault
refuses. `POST /v1/state/reset` clears the rules along with the state.

A rule matches on five fields, all AND-ed, each ignored when empty:

| Matcher | Matches |
|---------|---------|
| `service` | the service name (`s3`, `ec2`, …) |
| `operation` | the **semantic** operation name (`PutObject`, `UploadPart`, …) |
| `path_suffix` | requests whose path ends with the string (`.parquet`, `/big.bin`) |
| `query_key` | requests carrying the query parameter, whatever its value (`uploads`, `uploadId`, `partNumber`) |
| `header_prefix` | requests carrying a header whose name starts with the prefix, compared case-insensitively |

**S3 operations are named semantically, like every other service's.** The request parser
resolves an S3 REST request to `PutObject`, `UploadPart`, `CompleteMultipartUpload` and
so on before faults are evaluated, so a rule naming `PutObject` fires on `PutObject` and
not on `UploadPart`, which is also a `PUT` to an object path. Previously an S3 request
carried its bare HTTP verb at this point, so `operation: PutObject` matched nothing at
all and `operation: PUT` took out both. **A rule naming a bare HTTP method therefore no
longer matches an S3 request** — a rule on `GET` still fires for a service whose
operation genuinely is its method, such as `execute-api`.

**`query_key` is what separates the multipart sub-operations.** `CreateMultipartUpload`,
`UploadPart` and `CompleteMultipartUpload` share a path and differ from each other by a
`POST`-versus-`PUT` and by a sub-resource parameter — `?uploads`, `?partNumber=&uploadId=`,
`?uploadId=`. Presence is what those parameters signal, so the value is not compared. The
three wire matchers exist for distinctions an operation name does not carry: one key
rather than every key, or one header family rather than every request.

## `times` bounds a rule, and zero means one

| `times` | Fires on |
|---------|----------|
| absent / `0` | exactly **one** matching request |
| `n > 0` | the first `n` matching requests |
| negative | every matching request |

The bound is what makes retry assertable: fail twice, then succeed, is the outcome that
distinguishes working retry from no retry, and an unbounded rule can only ever produce
failure. **Zero means one rather than unlimited** deliberately — reading a missing field
as unlimited turns a typo into a fixture that consumes a consumer's whole retry budget.

A rule that has reached its bound is skipped rather than ending evaluation, so a later
rule still gets its turn. The match, the probability roll and the increment all happen
under one lock: with `times: 1`, N concurrent requests produce exactly one failure, which
is not something a counter on the client side can arrange.

Set `times: -1` when a fixture arms a fault and then clears it to assert the retry
succeeds — with the default of one the rule would already be spent, and the assertion
would pass whether or not clearing worked.

## The fired count

Each rule reports a `fired` count through `GET /v1/fault/rules`; `FaultsFired()` sums
them for an in-process test. **A rule that matches nothing produces exactly the same
passing test as a consumer's retry working**, so a fixture that arms a fault and then
observes success has proven nothing without asserting the count. Arming rules again
replaces the configuration and resets every count, so a fixture that re-arms the same
rule between phases gets its full budget back rather than a spent one.

## An injected error is indistinguishable from a real one

An injected error is serialized in the wire shape the target service's own errors use.
That matters most for S3, whose error document is a bare `<Error>` with a `<RequestId>`
rather than the `<ErrorResponse>` wrapper the Query protocol uses: an SDK recovers no
code from the wrapped form and falls back to the HTTP status, so an injected `SlowDown`
used to arrive at the client as `ServiceUnavailable` and a consumer matching on
`SlowDown` never saw their own fault. The bytes now come from the same function the S3
plugin uses, so an injected `NoSuchKey` and a genuine one are byte-identical — which is
the one property a fault injector must not lack, since a caller who can tell the two
apart can tell a fixture from production.

EC2 is the second such service, and it was broken more widely than S3: it is the only
service on the `ec2` protocol, whose error document wraps the error in a **plural**
`<Errors>` element and spells the request id `<RequestID>` with a capital D.

```xml
<Response><Errors><Error><Code>InsufficientInstanceCapacity</Code><Message>…</Message></Error></Errors><RequestID>req-…</RequestID></Response>
```

The AWS SDKs read the code at the XPath `Errors>Error>Code`, which finds nothing in a
`<ErrorResponse><Error>` document, so every EC2 error — organic as much as injected —
arrived at an SDK caller as `UnknownError`. That made a consumer's
`InvalidInstanceID.NotFound` branch unreachable and its test vacuous, the same failure
S3 had, one service over and on both paths rather than just the injected one.

**This is also a lesson about which tests can catch a wire-shape bug.** Neither case was
caught by substrate's own coverage, because botocore is lenient where the SDKs are
strict: its EC2 parser falls back to the document root when `<Errors>` is absent, so the
AWS CLI reported the correct code out of the wrong document the entire time, and every
CLI-driven test passed. Only a real SDK client is a gate here — see
`test/e2e/journey_ec2_errors_test.go`.

`cloudfront` and `route53` are REST-XML like S3 but keep the `<ErrorResponse>` shape:
their real error documents genuinely are wrapped, so they were already correct. Every
other XML service is on the Query protocol and keeps that wrapper too; S3 and EC2 are
the only two carve-outs.

The `<RequestID>` is the request's own ID, the one its event records, rather than a value
minted per call, so a replay of one recorded run produces byte-identical error bodies; S3's
document keeps the fixed `SUBSTRATE` so a pipeline error stays byte-identical to a plugin one.
See [What an error response carries](/reference/concepts/errors#what-an-error-response-carries) (#1241).

## `probability` draws from a per-rule PRNG

Each rule draws from its own PRNG stream, seeded from the controller's seed and the
rule's index. A rule's outcome sequence therefore depends only on how many requests
that rule itself matched: adding an unrelated rule, or changing how often one matches,
leaves the others' rolls unchanged. Streams are re-derived whenever a configuration is
armed, so re-arming resets them exactly as it resets each rule's `fired` count.

The stream is keyed by a rule's **index**, so reordering rules does change their
outcomes. That is deliberate — two rules with identical matchers are legitimate and
useful, and keying by the matchers instead would make them share a stream and would
make editing a path suffix silently reshuffle results. Prefer `times` for a bounded
outcome regardless: it needs no roll at all.
