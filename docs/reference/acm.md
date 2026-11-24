# ACM

**Endpoint:** `acm.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: CertificateManager.{Op}`)

## Supported operations

| Operation | Notes |
|-----------|-------|
| RequestCertificate | Certificate auto-transitions to ISSUED status |
| DescribeCertificate | |
| DeleteCertificate | |
| ListCertificates | |
| AddTagsToCertificate | |
| RemoveTagsFromCertificate | |
| ListTagsForCertificate | Reports `Tags` sorted by key — see [A tag set read back out of a map](/reference/concepts/listing-order#a-tag-set-read-back-out-of-a-map) |
| RenewCertificate | |

## A certificate is reachable through the tagging API

The Resource Groups Tagging API reaches an ACM certificate as of #835: `TagResources`,
`UntagResources` and `GetResources` all address it, and a tag written through any of them is
readable through `ListTagsForCertificate`. All four resolve the certificate's state key through
one builder, so none can address a certificate another would not.

An ARN naming any other ACM resource type is refused rather than resolved to a certificate. AWS
scopes these three operations to one type in prose — "This action applies only to the
`certificate` resource type. For all other ACM resource types, use `TagResource` instead" — and
publishes at least one other type with its own ARN shape, the ACME endpoint. The published
`CertificateArn` pattern does not itself restrict the resource portion, so the restriction is the
prose's and the type match is anchored on the ARN's own first path segment (#910).

Only the certificate record stores tags. The `cert_arns:` index lives in the same namespace and is
a JSON array of ARN strings, so the merge sits behind a guard whose prefix is tested
colon-terminated — `cert` alone matches `cert_arns` too.

## A `CertificateArn` is refused in three tiers, all at HTTP 400

Every code ACM publishes on the five operations that take a `CertificateArn` —
`DescribeCertificate`, `DeleteCertificate`, `AddTagsToCertificate`, `RemoveTagsFromCertificate`
and `ListTagsForCertificate` — carries **HTTP 400**. Substrate answered `ResourceNotFoundException`
at 404, a status ACM publishes nowhere, and sent a malformed ARN through the state lookup so that
the answer was "not found" for a string that could not name a certificate at all (#921).

| `CertificateArn` | Answer | Whose reading |
|---|---|---|
| Absent, length outside 20–2048, or failing the published pattern | `ValidationException`/400 | **AWS's** — *"The supplied input failed to satisfy constraints of an AWS service"*, and `CertificateArn` publishes both constraints |
| Pattern satisfied but naming no certificate: empty Region, another ACM resource type, a type with no identifier, or something nested under a certificate | `InvalidArnException`/400 | **Substrate's** — AWS publishes the code but describes it as *"does not refer to an existing resource"*, which is about non-existence, not syntax |
| A well-formed certificate ARN with no record | `ResourceNotFoundException`/400 | **AWS's** — *"The specified certificate cannot be found in the caller's account or the caller's account cannot be found"* |

The first tier is answered in ACM's validation form — `1 validation error detected: Value null at
'certificateArn' failed to satisfy constraint: Member must not be null` for an absent ARN, and the
length or pattern constraint for the others; see [Request validation](#request-validation) below.
Request validation answers it for a client's request before the plugin runs, and the plugin
answers it in the same words for a request that reaches it without the gate.

ACM's own operations and the tagging API's resolver run the same validation, so neither can accept
an ARN the other refuses. The tagging API renders its refusal as a `FailedResourcesMap` entry
rather than an error response, so the shapes differ and the decision does not.

The unparseable-body case was left open by #921 and settled by
[#950](https://github.com/scttfrdmn/substrate/issues/950), which confirmed that deferral's guess: it
is a protocol-level failure whose code belongs to ACM's common errors rather than to any one
operation's published list, so it answers `ValidationError`/400 from that page. All six sites
previously answered `InvalidParameterException`, which ACM publishes on only three of the six. See
*A request body that will not parse* above for why `ValidationException` is not the answer either,
near as the name is, and why the two codes now sit side by side in this plugin on purpose.

One answer is deliberately left as it is. ACM publishes `AccessDeniedException` at **400** while
substrate answers it at **403**, from the central authorization check every service shares — moving
it for one service would have the emulator answer two statuses for one decision, so it is recorded
here rather than changed.

## Request validation

ACM is validated against its service model before any plugin sees a client's request (#1612; see
[Request validation](/reference/concepts/validation)). Every failure answers
`ValidationException`/400, the code every operation taking a certificate publishes with the
description *"The supplied input failed to satisfy constraints of an AWS service"*.
`RequestCertificate`'s own Errors section omits it, and ACM answers it there anyway.

| Request | Answer |
|---|---|
| A required member absent, or `null` | `1 validation error detected: Value null at 'domainName' failed to satisfy constraint: Member must not be null` |
| A length or a number out of range | `… Value of the input at 'domainName' failed to satisfy constraint: Member must have length less than or equal to 253` |
| A value outside an enum (`KeyAlgorithm`, `CertificateStatuses`, …) | `… Member must satisfy enum value set: [RSA_1024, RSA_2048, …]` |
| A value failing a pattern (a tag's key or value, a `CertificateArn`) | `… Member must satisfy regular expression pattern: …` |

ACM withholds the value ("Value of the input at"), and that form, the `Value null` form for a
missing member, and ACM's enforcement of enums and patterns are each from ACM's observed answers
rather than from the reference, which publishes no message text; the sources are cited in
`emulator/wire_validate_errors.go`. `DomainName`'s published pattern uses a lookahead Go cannot
compile, so a malformed domain name is not refused on its pattern.

Two of the plugin's own refusals now answer in this form too, because a request the gate does
not see still reaches them — a stack's dispatch, a schedule's target, the tagging API's resolver —
and it should get the answer a client gets:

- **`RequestCertificate` with no `DomainName`** answers `ValidationException`, not
  `InvalidParameterException`.
- **A `CertificateArn` absent, of the wrong length or off the pattern** answers the constraint it
  fails rather than substrate's own wording.

`wire.validate: false` (`SUBSTRATE_WIRE_VALIDATE=false`, or `WithoutWireValidation()` on a
`TestServer`) turns the gate off; those two refusals are then the plugin's, in the same words.

## DescribeCertificate answers the published shape, with dates as epoch seconds

`ACMCertificate` declares `AccountID` and `Region` under wire-visible `json` tags, plus `EverTagged` as
`ever_tagged,omitempty`, because the record is what `MemoryStateManager` snapshots and a replay reads
back. Until [#756](https://github.com/scttfrdmn/substrate/issues/756), `DescribeCertificate` answered
the record whole. That put all three members on the wire, plus `Tags`, which `CertificateDetail` does
not publish: a certificate's tags are read through `ListTagsForCertificate`.

The same response rendered `CreatedAt`, `IssuedAt`, `NotAfter` and `NotBefore` as RFC3339 strings.
ACM speaks `awsJson1_1`, where `CertificateDetail`'s `Timestamp` members are numbers, so a typed SDK
could not decode it ([#1305](https://github.com/scttfrdmn/substrate/issues/1305)). The response now
answers `acmCertificateOut` (`emulator/acm_wire.go`), with the four dates as epoch seconds. The stored
record is unchanged. `TestACMWire_CertificateResponsesCarryNoBookkeepingMember` drives all eight
routed operations, tagging the certificate first, and pins the dates on a frozen clock.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::CertificateManager::Certificate | CertificateArn | |

## Cost

ACM certificates are free.
