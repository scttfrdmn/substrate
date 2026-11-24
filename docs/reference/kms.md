# KMS

**Endpoint:** `kms.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: TrentService.{Op}`)

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateKey | Answers the same `KeyMetadata` shape `DescribeKey` does, from the same builder; validates `KeySpec`/`KeyUsage` and their pairing, and refuses an `Origin`, `CustomKeyStoreId` or `XksKeyId` substrate does not model rather than discarding it; records the `Policy` it was given, and refuses one outside 1–32768 bytes or not a JSON object — see below |
| DescribeKey | Accepts all four `KeyId` forms; answers 16 of `KeyMetadata`'s 26 members, including `AWSAccountId`, `KeyManager`, `Origin` and the key's algorithm list; reports `DeletionDate` while a key is pending deletion, and reports no rotation flag — see below |
| ListKeys | `Limit` defaults to 100; outside [the published range](/reference/concepts/listing-order#eight-json-list-operations-refuse-a-page-size-outside-the-published-range) is refused |
| EnableKey | Refuses a key pending deletion, so recovery stays two calls — see below |
| DisableKey | Same refusal as `EnableKey` |
| ScheduleKeyDeletion | Waiting period range-checked at 7–30; refuses a key already pending deletion — see below |
| CancelKeyDeletion | Requires a key pending deletion, and leaves it `Disabled` — see below |
| GetKeyPolicy | Answers AWS's default key policy for a key that has never been given one, naming the key's own account root; refuses a `KeyId` naming no key and a `PolicyName` other than `default` — see below |
| PutKeyPolicy | Refuses a `Policy` outside 1–32768 bytes and one that is not a JSON object, both before the key is read; refuses a `PolicyName` other than `default`; accepts a key in every state, including pending deletion — see below |
| GetKeyRotationStatus | Reports the bare key ID and, while rotation is on, `RotationPeriodInDays` and `NextRotationDate`; answers `false` for a key pending deletion and answers in every key state substrate can produce — see below |
| EnableKeyRotation | Range-checks `RotationPeriodInDays` at 90–2560 and stores it with the date it ran; refuses a key whose spec is not `SYMMETRIC_DEFAULT`, and refuses a disabled key and a key pending deletion with a different code for each — see below |
| DisableKeyRotation | Same refusals as `EnableKeyRotation`; leaves the stored rotation period and enable date alone |
| UpdateKeyDescription | Replaces the description `DescribeKey` reports; an empty one is accepted and clears it. Accepts a disabled key and refuses one pending deletion with `KMSInvalidStateException`, per the key-state table; the published 8192-character bound is not enforced, since the page names no error for it (#1545) |
| TagResource | Tags are keyed `TagKey`/`TagValue`, not `Key`/`Value` |
| UntagResource | |
| ListResourceTags | |
| CreateAlias | Refuses a `TargetKeyId` outside the caller's account and Region, one naming no key, and one pending deletion; enforces its own published `AliasName` pattern and the reserved `alias/aws/` prefix; refuses a name the account and Region already hold — see below |
| DeleteAlias | Accepts a name with or without the `alias/` prefix, because its published pattern — unlike the other two — does not require it; an alias the account and Region do not hold is `NotFoundException`/400, its page's one reachable refusal (#1107) — see below |
| UpdateAlias | Refuses an alias that does not exist, a `TargetKeyId` outside the caller's account and Region, one naming no key, and one pending deletion; refuses a move between two key types or two key usages. The alias's **current** key may be pending deletion — see below |
| ListAliases | Reports one entry per alias, so a repeated `CreateAlias` no longer duplicates a row. `Limit` defaults to the published 50; outside [the published range](/reference/concepts/listing-order#eight-json-list-operations-refuse-a-page-size-outside-the-published-range) is refused |
| Encrypt | Returns ciphertext blob (base64-encoded stub); reports the `EncryptionAlgorithm` used and refuses one the key's spec does not admit; refuses a disabled key and a key pending deletion, with a different code for each; refuses a `Plaintext` outside the published 1–4096 bytes before the key is read, and one past the smaller per-spec maximum after — see below |
| Decrypt | Returns plaintext (stub pass-through); same algorithm handling and same refusals as `Encrypt`, and refuses a `KeyId` naming a key other than the ciphertext's — see below |
| GenerateDataKey | Same refusals as `Encrypt`, minus the algorithm members, plus a refusal of any key spec but `SYMMETRIC_DEFAULT` — see below |
| GenerateDataKeyWithoutPlaintext | Same refusals as `GenerateDataKey`, through the same helper |
| ReEncrypt | Checks the key state, the named key and the encryption algorithm of **both** keys independently, and reports the source key's ARN and both algorithms — see below |

## A `KeyId` is resolved from its own ARN, not from the caller

AWS's `KeyId` parameter accepts four forms, and substrate accepts all four:

| Form | Example | Account and Region come from |
|------|---------|------------------------------|
| Key ID | `1234abcd-12ab-34cd-56ef-1234567890ab` | the request |
| Key ARN | `arn:aws:kms:us-west-2:111122223333:key/1234abcd-…` | the ARN |
| Alias name | `alias/prod` | the request |
| Alias ARN | `arn:aws:kms:us-west-2:111122223333:alias/prod` | the ARN |

The two ARN forms take their account and Region from the ARN, and the parser that
reads them takes no request context at all, so that is structural rather than
remembered at each of eighteen call sites. Before #922 every one of those sites
keyed its load *and its store* by the caller's account and Region, so a key ARN
naming another account addressed the caller's own key of that ID — and
`UntagResource` stripped its tags while answering 200.

The type is the resource portion's first `/`-delimited segment, compared whole.
That matters twice. An alias ARN's resource portion is `alias/{name}`, so taking
the last component resolved `…:alias/prod` to the key ID `prod`; and an alias
name may itself contain a slash — AWS's own example of an AWS managed key's alias
is `alias/aws/s3` — so a parser that refused a slashed identifier would be wrong
in the other direction. The cut is on the *first* slash only, and a further slash
is refused for a `key` and allowed for an `alias`.

A bare identifier is not treated as a malformed ARN. It is a key ID, and one that
names nothing is not-found; answering `InvalidArnException` would tell a caller
its ARN was wrong when it sent no ARN.

Two refusals follow from the resolution rather than preceding it. All three
tagging operations publish *"Cross-account use: No. You cannot perform this
operation on a KMS key in a different AWS account"*, and the developer guide adds
*"You cannot tag aliases, custom key stores, AWS managed keys, AWS owned keys, or
KMS keys in other AWS accounts"* — so a key outside the caller's account or
Region is refused with `NotFoundException`. While the account came from the
request context that prohibition had nothing to refuse, because a foreign ARN
could only ever reach a local record. `CreateAlias` and `UpdateAlias` refuse the
same way, because an alias is scoped to one account and one Region and a written
cross-Region pointer is a dangling one that `ListAliases` reports and every later
resolution fails on. Mapping the prohibition onto `NotFoundException` is
substrate's reading: AWS publishes the prohibition and the code but does not join
them.

One thing this deliberately leaves alone, so the residue is recorded rather than
discovered: the other fifteen `KeyId` operations do not enforce the Region, so
`DescribeKey` on a foreign-Region key ARN answers with that key — strictly better
than describing a local impostor, but real KMS would refuse, and AWS publishes no
per-operation statement to cite for the other fifteen.

## The three alias operations do not share one set of rules

Until #1085 the two alias writers verified nothing at all. `UpdateAlias` decoded
two members, prepended `alias/` if it was missing and wrote a state key — so an
alias that did not exist was *created* by the operation whose first published
sentence is *"Associates an **existing** AWS KMS alias with a different KMS
key"*, a `TargetKeyId` naming no key produced a dangling pointer that every later
resolution of the alias failed on, a key scheduled for deletion could take the
alias, and a symmetric key's alias could be moved to an RSA one. `CreateAlias`
checked the same nothing, so all four gaps existed twice in one plugin.

Closing them meant reading all three pages, and the finding is that they publish
**three different** `AliasName` rules and **three different** sets of codes:

| Operation | Pattern | Codes published for a name |
|-----------|---------|----------------------------|
| CreateAlias | `^alias/[a-zA-Z0-9/_-]+$` | `InvalidAliasNameException`, `LimitExceededException` |
| UpdateAlias | `^alias/[a-zA-Z0-9/_-]+$` | `LimitExceededException` |
| DeleteAlias | `^[a-zA-Z0-9:/_-]+$` | none |

So the `alias/` prepend is gone from the two writers whose pattern requires the
prefix and stays at `DeleteAlias`, whose pattern does not require it and admits a
colon besides. `API_DeleteAlias`'s own prose still says the name "must begin with
`alias/`", so that page contradicts itself; substrate takes the
machine-readable half. All three publish the same 1–256 length bound.

`InvalidAliasNameException` is published on `CreateAlias` alone, and that is not a
gap on `UpdateAlias`: the alias must already exist, and no alias failing
`CreateAlias`'s pattern can ever have been created, so a malformed name there is
answered by the `NotFoundException` `UpdateAlias` *does* publish. Nothing is
borrowed across pages. Over-length is `LimitExceededException` at both, by its own
gloss — *"a length constraint or quota was exceeded"* — and it is checked before
the alias is looked up, so the length code wins over the not-found one.

**Only the new target's key state is checked, and the operation pages do not say
so.** Both say merely *"The KMS key that you use for this operation must be in a
compatible key state"*, which reads as one condition over one key. The developer
guide's key-state table resolves it into two: `CreateAlias` carries the footnote
*"KMSInvalidStateException: `<key ARN>` is pending deletion"*, while `UpdateAlias`
carries *"If the source KMS key is pending deletion, the command succeeds. If the
destination KMS key is pending deletion, the command fails."* So an alias whose
current key is scheduled for deletion may still be re-pointed — which is exactly
what a caller does to rescue it — and a single "check the key state" guard would
break that. A **disabled** key is accepted at both: the table gives both a
checkmark for it and neither page publishes `DisabledException`.

**The type-match refusal is substrate's reading.** AWS publishes the restriction
twice on `API_UpdateAlias`, verbatim in both places — *"The current and new KMS key
must be the same type (both symmetric or both asymmetric or both HMAC), and they
must have the same key usage. This restriction prevents errors in code that uses
aliases."* — and none of the operation's five published errors describes it.
Substrate answers `ValidationError`/400, following the two in-tree precedents for a
malformed KMS request (`kmsUnknownKeySpec`, `kmsUnknownKeyUsage`) rather than
borrowing a code from a sibling page. The message names which half failed, because
the code cannot: a deploy that moved an alias across types needs to know whether it
was the family or the usage. The three families are derived from the algorithm
tables the rest of the plugin already reads, not listed a fourth time.

One divergence is recorded rather than fixed here. `TargetKeyId` is published as
*"Specify the key ID or key ARN"*, and substrate additionally accepts an alias
there, because the shared resolver handles all four `KeyId` forms; a caller
relying on that is relying on something AWS does not publish.

**`DeleteAlias` refuses an alias that names nothing** (#1107). `API_DeleteAlias`
publishes `NotFoundException`/400 and `KMSInvalidStateException`/400, and the
key-state table permits `DeleteAlias` in every key state, so the not-found refusal
is the only one the operation can give. It used to answer 200 for any name. The
name resolves as before, with or without the `alias/` prefix, so a bare name gets
the same refusal. A second delete of the same alias is refused too, because the
first removed it.

CloudFormation's teardown stays idempotent. A stack delete calls `DeleteAlias` for
an alias that may already be gone, deleted out of band or by a retried teardown,
and the sweep reads `NotFoundException` as "already deleted" through the shared
`cfnDeleteAbsentCodes` rule in `emulator/cfn_delete.go`. That rule already covered
every type whose delete answers `NotFoundException`, so the alias needed no
special case. `TestCFN_AStackWhoseAliasWasDeletedOutOfBandStillDeletes` pins it.

## Every KMS refusal is a 400, because KMS publishes no 404

Across every KMS operation substrate models, AWS's reference publishes exactly two
HTTP statuses: **500** for `DependencyTimeoutException`, `KMSInternalException` and
`KeyUnavailableException`, and **400** for everything else. There is no 404 for a
missing key, alias or destination key on any operation. The only 404 anywhere in
KMS's documentation is `UnknownOperationException` on the common-errors page, which
reports that the *action name* was not recognised — a different failure entirely.

So a KMS status carries nothing a caller can branch on, and the **code** is the
whole signal. Substrate answered three codes at a status KMS does not publish, and
one of the three was not a KMS code at all (#923):

| Code | Was | Is | Sites | Provenance |
|------|-----|----|-------|------------|
| `NotFoundException` | 404 | **400** | 15 | Every operation that can answer it — *"The request was rejected because the specified entity or resource could not be found"* |
| `DisabledException` | 409 | **400** | 3 | `API_Encrypt`, `API_Decrypt`, `API_GenerateDataKey` — *"The request was rejected because the specified KMS key is not enabled"* |
| `InvalidRequest` | 400 | **`ValidationError`**/400 | 21 | The string appears on no KMS page at all; `ValidationError` is the common error — *"The input doesn't meet the required format or constraints"* |

`ValidationError` rather than `TagException` for the unparseable-body guard: both
are 400, but `TagException` is glossed *"one or more tags are not valid"* — the
content of a `Tags` member — and eighteen of those twenty-one operations take no
tags. `MalformedHttpRequestException` is the other near miss and is also declined:
its published scope is the transport layer, *"when the request body can't be
decompressed using the specified content encoding algorithm"*, and a body that
decompressed and then failed to parse is not that. Note that KMS spells the common
error `ValidationError`, with no `Exception` suffix, unlike most JSON-protocol
services; `SerializationException` and `ValidationException` appear nowhere in its
documentation, so answering either would repeat the defect.

Every code is now constructed by one helper per code, so a status is chosen once
rather than at each of the thirty-nine call sites that answered it — the
arrangement Systems Manager arrived at in #933 from the other direction.

The full published surface, for a caller deciding what to branch on:

| Status | Codes |
|--------|-------|
| 400 | `NotFoundException`, `DisabledException`, `InvalidArnException`, `KMSInvalidStateException`, `InvalidCiphertextException`, `IncorrectKeyException`, `InvalidKeyUsageException`, `InvalidGrantTokenException`, `LimitExceededException`, `TagException`, `AlreadyExistsException`, `InvalidAliasNameException`, `MalformedPolicyDocumentException`, `UnsupportedOperationException`, `DryRunOperationException`, `ValidationError`, `ThrottlingException` |
| 500 | `DependencyTimeoutException`, `KMSInternalException`, `KeyUnavailableException` |

The adjacent gap #923 named rather than closed — `API_EnableKeyRotation` and
`API_DisableKeyRotation` publish `DisabledException` too, and substrate's two
handlers checked nothing — is closed by [#949](https://github.com/scttfrdmn/substrate/issues/949).
It was a missing refusal rather than a wrong status, and it is described in the
next section, because getting it right needed a second code rather than a second
call to the same helper.

The same gap remains open on three cryptographic operations, and is named here for
the same reason: `Encrypt`, `Decrypt` and `GenerateDataKey` answer
`DisabledException` for a key pending deletion, where AWS publishes
`KMSInvalidStateException`. That is
[#961](https://github.com/scttfrdmn/substrate/issues/961).

## A key state can forbid rotation, and the two forbidding states answer different codes

`API_EnableKeyRotation` and `API_DisableKeyRotation` both carry the sentence *"the
KMS key that you use for this operation must be in a compatible key state"*, and
both publish the identical seven-error list — `DisabledException` and
`KMSInvalidStateException` among them, each at 400. Substrate checked no state at
all: it wrote `RotationEnabled` and answered 200 whatever the key was.

The developer guide's *Key states of AWS KMS keys* table is what decides which of
the two codes a state gets, and for these two operations its row is:

| Key state | Substrate answers | Provenance |
|-----------|-------------------|------------|
| `Enabled` | 200 | Permitted |
| `Disabled` | `DisabledException`/400 | Footnote `[1]` — *"DisabledException: `<key ARN>` is disabled"* |
| `PendingDeletion` | `KMSInvalidStateException`/400 | Footnote `[3]` — *"KMSInvalidStateException: `<key ARN>` is pending deletion (or pending replica deletion)"* |
| `PendingImport`, `Unavailable`, `Creating`, `Updating` | — | Refused by AWS, **unreachable in substrate**: no operation writes any of the four, so nothing can be in one of these states to be answered |

Two of those rows are load-bearing.

**`PendingDeletion` is reachable, so the naive check would answer the wrong code.**
`ScheduleKeyDeletion` writes the state *and* clears the enabled flag in one go, so
a lone "is it enabled?" test would report `DisabledException` for a key pending
deletion. The two codes carry different remedies — enable the key and retry, or
cancel the deletion and retry — so collapsing them leaves a caller's error handler
unable to tell which. `PendingDeletion` is therefore checked first.

**`GetKeyRotationStatus` is deliberately not guarded.** Its row in the same table
permits `Enabled`, `Disabled` and `PendingDeletion` alike, so reading whether
rotation is on succeeds in every state substrate can produce. A test pins that, so a
later sweep guarding "every key-state-sensitive operation" cannot quietly introduce a
refusal AWS does not have.

The reason first recorded here was that the page published neither code. That was
wrong about half of it and is corrected rather than left standing:
`API_GetKeyRotationStatus` publishes no `DisabledException`, but it does publish
`KMSInvalidStateException`/400 among its six errors. The decision survives the
correction on better grounds — the page documents an *answer* for a pending-deletion
key, *"while a KMS key is pending deletion, its key rotation status is `false` … If
you cancel the deletion, the original key rotation status returns to `true`"*, and an
operation that documents an answer for a state cannot also be refusing that state. So
the published code belongs to the states substrate never writes, the same four the
table's last row names. That documented `false` was itself a wrong value rather than a
missing refusal, and it is now answered — see *A pending deletion suspends the rotation
schedule without forgetting it* below.

The refusal is checked **before** the write, so a refused call leaves
`RotationEnabled` exactly as it was — asserted by reading it back through
`GetKeyRotationStatus` rather than by inspecting state, since a guard placed after
the assignment would answer the right code while having already changed the value. For
a key pending deletion that read-back now reports a derived `false` whatever was stored,
so the test leaves the state before reading a second time; otherwise the strongest half
of the assertion would be invisible in exactly the state where the guard is newest.

The four unreachable states are recorded rather than implemented because
`ScheduleKeyDeletion` is substrate's only writer of a state other than `Enabled` or
`Disabled`: there is no operation a caller could use to reach them, so a guard for
them could only be exercised by fabricating a key record, which is the kind of test
that proves nothing about the wire.

The same guard now serves the five cryptographic operations, which reached it by a
different route and with one code chosen rather than published — see *The five
cryptographic operations refuse the same two key states* below.

## The rotation period is a value AWS expects a caller to read back

`EnableKeyRotation` decoded `KeyId` and nothing else, so `RotationPeriodInDays` was
accepted and discarded ([#964](https://github.com/scttfrdmn/substrate/issues/964)).
Discarding it would have been defensible if the value were write-only — a plugin has
no rotation to schedule and no key material to replace — but it is not:
`API_GetKeyRotationStatus` publishes `RotationPeriodInDays` as a **response** element
with the identical Valid Range of **90 to 2560**. A range stated at both ends of a
round trip is a value AWS expects a caller to write and read back, so substrate now
stores it and reports it.

The published members, and what substrate answers on each:

| Member | Answered | Provenance |
|--------|----------|------------|
| `KeyId` | The **bare key ID**, whichever of the four forms the caller addressed the key by | Glossed only *"identifies the specified symmetric encryption KMS key"* — none of the *"Amazon Resource Name (key ARN)"* wording `ScheduleKeyDeletion` and `ReEncrypt` use for theirs — and the page's sample renders `1234abcd-…`. Echoing the request would therefore be wrong for an alias or an ARN, not merely lazy |
| `KeyRotationEnabled` | Always; the stored flag in every state but `PendingDeletion`, which answers `false` | Published unconditionally, with the pending-deletion `false` published separately — see below |
| `RotationPeriodInDays` | Only while the *reported* rotation status is on | See below |
| `NextRotationDate` | Only while the *reported* rotation status is on: the enable date plus the period, as Unix seconds | See below |
| `OnDemandRotationStartDate` | Not answered | *"Identifies the date and time that an in progress on-demand rotation was initiated"*; `RotateKeyOnDemand` is not implemented, so nothing can start one. AWS's own sample response omits it too |

Three decisions here are substrate's rather than AWS's.

**The refusal for an out-of-range period is `ValidationError`/400**, reached exactly as
the waiting-period range check reaches it and deliberately reused rather than
re-argued: `API_EnableKeyRotation`'s seven errors — `DependencyTimeoutException`,
`DisabledException`, `InvalidArnException`, `KMSInternalException`,
`KMSInvalidStateException`, `NotFoundException` and `UnsupportedOperationException` —
contain none for a parameter value out of range, so it comes from `CommonErrors`. Two
range violations in one plugin answering two different codes would be the divergence
#923 exists to prevent. `UnsupportedOperationException` is the near miss and is
declined: its gloss is *"a specified parameter is not supported or a specified resource
is not valid for this operation"*, which describes an inadmissible parameter or
resource — an asymmetric key, which is where that code *is* answered, two sections
below — not an admissible
parameter carrying a number out of range. The message names 90 and 2560, because a
caller that sent 30 by analogy from the deletion window cannot discover the range from
a bare refusal. As with the waiting period, the range is checked **before** the key is
resolved, so a bad period against an absent key answers the value that is wrong on the
face of the request.

**The period decodes into a `*int`, where `PendingWindowInDays` deliberately stays an
`int`.** The asymmetry is the point rather than an inconsistency: 0 is a value a caller
can send, it is far below the published minimum of 90, and a plain `int` would make it
indistinguishable from an absent member and silently accept it as 365 — #964's defect
reintroduced one level down. The waiting period's note below explains why it does not
need the same treatment.

**An omitted period resets to 365 on every call, not only the first.**
`API_EnableKeyRotation` states *"if no value is specified, the default value is 365
days"* unconditionally, and documents the parameter as able to *"modify the rotation
period of a key that you previously enabled automatic key rotation on"* — so a second
call is a legitimate change rather than a conflict to refuse, and a second call that
omits the member takes the default rather than preserving the value the first one set.
AWS does not address the interaction directly, so the reading is recorded and pinned by
a test: the intuitive opposite reading would otherwise be an easy "fix". A caller that
wants to keep 180 has to send 180 again.

`DisableKeyRotation` leaves the stored period alone — AWS documents no clearing — but
`GetKeyRotationStatus` stops reporting it, which is the honest-empty behaviour #827
established: a key that is not rotating has no rotation period to report. A re-enable
with an explicit period therefore never surfaces a stale one, and a re-enable without
one reports 365 per the rule above.

The first of the two restrictions the page publishes — automatic rotation is
*"supported only on symmetric encryption KMS keys"* — is now enforced; see the next
section. The second, *"you cannot enable or disable automatic rotation of AWS managed
KMS keys"*, is unreachable rather than unenforced: substrate mints no AWS managed key,
so no request can name one. `EnableKeyRotation` is also documented **Cross-account use:
No** while `GetKeyRotationStatus` is **Yes**, an asymmetry substrate does not model.

## A key type can forbid rotation permanently, and that is answered before the key state

`EnableKeyRotation` and `DisableKeyRotation` checked the period's range
([#964](https://github.com/scttfrdmn/substrate/issues/964)) and the key state
([#949](https://github.com/scttfrdmn/substrate/issues/949)) and never checked what kind
of key they were pointed at
([#972](https://github.com/scttfrdmn/substrate/issues/972)). AWS states the restriction
twice on `API_EnableKeyRotation` — once in the prose and once on the `KeyId` parameter —
and repeats it verbatim on `API_DisableKeyRotation` and `API_GetKeyRotationStatus`:
*"automatic key rotation is supported only on symmetric encryption KMS keys. You cannot
enable automatic rotation of asymmetric KMS keys, HMAC KMS keys, KMS keys with imported
key material, or KMS keys in a custom key store."* `EnableKeyRotation` on an RSA key
answered 200 and wrote the flag, so `GetKeyRotationStatus` then reported a rotation
schedule for a key AWS will never rotate.

Both operations now refuse it with `UnsupportedOperationException`/400, whose gloss —
*"a specified parameter is not supported or a specified resource is not valid for this
operation"* — is the inadmissible-resource case the out-of-range period above declined
to borrow. `DisableKeyRotation` refuses it too, although turning rotation *off* on a key
that will never rotate looks harmless; the same argument was rejected for the key state
and it is rejected again here, because AWS publishes the identical seven-error list and
the identical sentence on both pages.

**The discriminator is `KeySpec`, not `KeyUsage`.** A single equality test against
`SYMMETRIC_DEFAULT` covers every family AWS names and the two it implies — asymmetric
encryption, asymmetric signing, `SM2`, HMAC and ML-DSA — where a `KeyUsage` test would
let an `ENCRYPT_DECRYPT` RSA key through. The refusal message names the spec it found,
because `UnsupportedOperationException`/400 is otherwise indistinguishable from the
AWS-managed-key refusal a caller might expect. Tests cover one spec per family rather
than all fifteen: the plugin's check is one comparison, so a second RSA size exercises
no new code, while five families guard against the check being rewritten as the
family test the sentence's own wording invites.

**The key type is answered before the key state**, which AWS does not publish a
precedence for. It is substrate's reading, and the argument is the remedy: a key state
has one — `EnableKey`, or `CancelKeyDeletion` and then `EnableKey` — and a key type has
none, so telling a caller with a disabled RSA key to enable the key sends it round a
loop that cannot terminate. It is the same request-before-resource principle #964 used
to check the period's range before resolving the key.

**`GetKeyRotationStatus` is deliberately not refused for such a key**, and a test pins
that. The sentence forbids *enabling* rotation on the key, not asking whether it
rotates; the operation's own key-state row permits the read; and `false` for a key that
will never rotate is a true answer. A sweep that guarded "every rotation operation" for
consistency would introduce a refusal AWS does not publish.

Three of AWS's five restrictions stay unenforced because they are unreachable rather
than unguarded: imported key material, a custom key store and an AWS managed key each
need a `KMSKey` member substrate does not model, so no request can produce one. Every
non-symmetric key in the tests is now created with a key usage its spec admits, because
[#977](https://github.com/scttfrdmn/substrate/issues/977) closed the gap they used to
rely on — `CreateKey` validated `KeySpec` against nothing — and that refusal leaves this
one unchanged: it fires on the key spec of a key that exists, not on a request member.

## A pending deletion suspends the rotation schedule without forgetting it

`GetKeyRotationStatus` answered two of its five published members, and one of the two
answers was wrong ([#973](https://github.com/scttfrdmn/substrate/issues/973)).

**`NextRotationDate` was missing entirely**, and it is the member rotation-monitoring
code reads — the reason to ask for a rotation status is usually to find out when the
next rotation is. AWS defines it on `API_EnableKeyRotation` rather than where it is
reported: *"the rotation period defines the number of days after you enable automatic
key rotation that AWS KMS will rotate your key material, and the number of days between
each automatic rotation thereafter."* So it is the enable date plus the period, and
substrate held only the period — `KMSKey` gained the enable date for this and nothing
else. It is reported as **Unix seconds**, matching what `DescribeKey` does with
`DeletionDate`. AWS's own page disagrees with itself here: its Response Syntax types the
member `number` while its sample response renders
`"2024-02-14T18:14:33.587000+00:00"`. Substrate keeps one KMS timestamp convention.

**A second `EnableKeyRotation` moves the date**, because it overwrites the enable date
rather than keeping the first one. AWS documents no answer — the parameter is described
as able to *"modify the rotation period"*, with nothing said about the date it counts
from — so this is substrate's reading, and the alternative is what argues for it: a key
enabled with 2560 days and re-enabled with 90 would otherwise report a next rotation
date roughly six years in the past. `DisableKeyRotation` leaves both the period and the
enable date alone, so nothing is lost by turning rotation off.

**A key pending deletion reports `false`**, whatever the stored flag says.
`API_GetKeyRotationStatus` publishes the state's answer in full: *"while a KMS key is
pending deletion, its key rotation status is `false` and AWS KMS does not rotate the key
material. If you cancel the deletion, the original key rotation status returns to
`true`."* Substrate reported the stored flag, so a caller polling a key it had scheduled
for deletion was told rotation was still on. That was a wrong value rather than a
missing refusal — the distinction that kept it out of #949, which read the same two
sentences as a reason **not** to guard the operation and stopped there.

The second sentence is what forces the shape of the fix. The reported status is
**derived** — the stored flag and the key state, combined at read time — rather than
written by `ScheduleKeyDeletion`, because there has to be something left for
`CancelKeyDeletion` to restore. An implementation that cleared the flag on the way in
would satisfy the first sentence and make the second unimplementable, and no state
inspection would tell the two apart; the test walks enable → schedule → cancel and
asserts the original date comes back, not a fresh one counted from the cancel.

The period and the date follow the **reported** status rather than the stored flag, so a
key pending deletion reports neither. Answering a period beside a `false` status would
describe a rotation AWS has just said will not happen, and the honest-empty reading #827
established says a key with no rotation schedule has no schedule to report. Every read
in the test file also asserts `OnDemandRotationStartDate`'s absence, so the one member
substrate still does not answer cannot appear by accident: it reports an in-progress
`RotateKeyOnDemand`, which is not implemented, so a zero timestamp there would report a
rotation nobody started.

## `DescribeKey` never published a rotation flag

Substrate rendered `RotationEnabled` inside `DescribeKey`'s `KeyMetadata`.
`API_KeyMetadata` publishes **26** members and that is not one of them; the string does
not occur on the page at all ([#971](https://github.com/scttfrdmn/substrate/issues/971)).

That is the failure mode #765 exists to catch, aimed at a response member rather than
at state: a consumer could branch on
`DescribeKey().KeyMetadata.RotationEnabled` against the emulator, pass, and get an
absent field from AWS. The member is removed, and the test asserts its absence against
**raw JSON** both before and after enabling rotation — a decoded struct cannot tell an
omitted member from a `false` one, which is the whole distinction — and then asserts
that rotation state is still readable through `GetKeyRotationStatus`, so the removal is
shown to have removed a duplicate route rather than the only one.

Restoring it under another name would be worse than the original defect, because the
rules the real operation carries — the symmetric-only restriction, the key-state
refusals, the documented `false` while a key is pending deletion — belong to
`GetKeyRotationStatus` and would all be bypassed. The opposite direction — the sixteen
`KeyMetadata` members substrate did **not** answer, five of which are present on every
real `DescribeKey` call for a plain customer symmetric key — was
[#974](https://github.com/scttfrdmn/substrate/issues/974), and it is the next section.

## One `KeyMetadata` builder, and the six members it stopped withholding

`API_KeyMetadata` publishes 26 members. `DescribeKey` answered 10 and `CreateKey`
answered 9 from a second map of its own
([#974](https://github.com/scttfrdmn/substrate/issues/974)). Five of the absent ones
are sent by AWS on every call for a plain customer symmetric key —
`AWSAccountId`, `KeyManager`, `Origin`, `EncryptionAlgorithms` and the deprecated
`CustomerMasterKeySpec` — so a consumer reading any of them got a missing field from
substrate where AWS always has a value. That is #765's failure mode aimed at a response
shape, and it is #971 read from the other side: that issue removed the one member
substrate emitted and AWS does not publish, and this one adds the members AWS publishes
and substrate did not emit.

**Nothing here is a simulation.** Three of the five are constants for every key
substrate can create and the other two are functions of `KeySpec` and `KeyUsage`, both
of which the key already carries. Every member added is a value substrate already knew
and withheld, which is what makes the two issues the same finding rather than two.

**The builder is shared because AWS shares the shape.** The type's own page says it
*"is used as a response element for the `CreateKey`, `DescribeKey`, and `ReplicateKey`
operations"*, and substrate's two hand-built maps had already drifted apart: #963 added
`DeletionDate` to `DescribeKey`'s and left `CreateKey`'s alone, so two operations
disagreed about one documented type. That is the defect class #952 catalogues, and
neither operation's own test could see it, because each asserted its own response
against the page rather than against the other. Both now build from one function, and a
test compares the two responses member by member on raw bytes. `ReplicateKey` is the
third caller AWS names; substrate does not implement it, and when it arrives it must
build from here rather than growing a third map.

### Three constants, and what each one records

| Member | Value | Why it is a constant rather than a stored field |
|--------|-------|-------------------------------------------------|
| `AWSAccountId` | The key's own account | Not the caller's. A cross-account `DescribeKey` is the only call that separates the two, so that is the call the test makes |
| `KeyManager` | `CUSTOMER` | Substrate mints no AWS managed key; every key in state came from a `CreateKey` request in the caller's own account |
| `Origin` | `AWS_KMS` | Substrate creates its own key material and models no custom key store, and since #984 `CreateKey` **refuses** every other published origin rather than discarding the member — so a stored key with another origin is unreachable by construction, which is what keeps this a constant |

`KeyManager` is also why `EnableKeyRotation`'s *"you cannot enable or disable automatic
rotation of AWS managed KMS keys"* is recorded as **unreachable** rather than
unenforced, and `Origin` is why three further members are: `ExpirationModel` and
`ValidTo` are published *"only when `Origin` is `EXTERNAL`"*, and
`XksKeyConfiguration` only for an external key store.

### `CreateKey`'s three key-material parameters

Until #984 `CreateKey` decoded none of `Origin`, `CustomKeyStoreId` or `XksKeyId`. All
three were accepted and thrown away, so `CreateKey` with `Origin: "EXTERNAL"` — the first
call of every key-import workflow — answered `200` with `Origin: "AWS_KMS"`, `KeyState:
"Enabled"` and `Enabled: true`. A consumer got a fully usable key where AWS answers one in
`PendingImport` that no cryptographic operation will touch, so the workflow passed at step
one and failed at step two, where `GetParametersForImport` turns out not to exist either.

Substrate models neither imported key material nor a custom key store, and per CLAUDE.md's
boundary that is defensible: the key material itself and an HSM cluster's contents are
resource-internal. **The request parameters are not** — they are observable through an API
call — so the choice was between modelling them, refusing them, and the third thing
substrate was doing. Accepting a parameter and discarding it is the worst of the three,
because it is the only one a caller cannot detect. They are now decoded, and what substrate
does not model is refused with a code `API_CreateKey` publishes.

**Two refusals, two codes.** The split is not cosmetic: the two answer different questions
about whether the caller did anything wrong, and both are `400`, so the code is the only
thing that can say which.

| Request | Substrate answers | Whose refusal it is |
|---------|-------------------|---------------------|
| `Origin` outside the published four | `ValidationError`/400, naming the value and the four | AWS's. A malformed member, from `CommonErrors.html` — the reading #977 recorded for a misspelled `KeySpec` |
| `XksKeyId` with any `Origin` but `EXTERNAL_KEY_STORE` | `ValidationError`/400, naming the origin sent and the one the member is for | AWS's. The page states it itself: *"this parameter is required for a KMS key with an `Origin` value of `EXTERNAL_KEY_STORE`. It is not valid for KMS keys with any other `Origin` value"* — so this refusal holds against real KMS. AWS attaches no code to the sentence; `ValidationError` is substrate's reading, the same one every other malformed-member refusal on this operation answers |
| `Origin` published and not `AWS_KMS` | `UnsupportedOperationException`/400, naming both origins | Substrate's. Real KMS honours all four. The message says so, because a consumer whose import workflow stops here needs to know it has reached a boundary of the emulator rather than written a bad request |
| `CustomKeyStoreId`, any value | `UnsupportedOperationException`/400, quoting the ID | Substrate's, for the same reason |

The order matters at one point and is asserted: `XksKeyId` is checked against the
*requested* origin **before** the origin's own support, so a caller sending it with
`AWS_KMS` reads AWS's own refusal, while `EXTERNAL_KEY_STORE` with an `XksKeyId` — the one
combination AWS accepts — falls through and is told the truth about the store.

`CustomKeyStoreNotFoundException` is the near miss for the fourth row and is **not** used,
although `API_CreateKey` publishes it. It says *no store has this ID*, which invites the
caller to create one, and `CreateCustomKeyStore` does not exist either — so the caller would
loop. The same reasoning keeps `CustomKeyStoreInvalidStateException`,
`CloudHsmClusterInvalidConfigurationException`, `XksKeyAlreadyInUseException`,
`XksKeyNotFoundException` and `XksKeyInvalidConfigurationException` unconstructed: each
presupposes a modelled store or external key.

Three published constraints are **unreachable by construction** and are recorded rather than
implemented, because a constraint no request can reach is not enforcement and writing one
implies the parameter is modelled: `CustomKeyStoreId`'s 1–64 length, `XksKeyId`'s 1–128
length and `^[a-zA-Z0-9-_.]+$` pattern, and the spec-dependent conditions on `Origin` itself
(*"the `EXTERNAL` origin value is valid only for symmetric KMS keys"*, and the
`SYMMETRIC_DEFAULT` requirement for `AWS_CLOUDHSM` and `EXTERNAL_KEY_STORE`). The resolver
is nevertheless ordered **after** the key spec resolves, so that whichever of those is ever
modelled has a resolved spec to read.

Modelling `Origin: EXTERNAL` as a `PendingImport` key state, and modelling a custom key
store, both remain open and are each larger than this. A refusal now forecloses neither, and
it converts five of the absent `KeyMetadata` members below from *nothing reads the
parameter* into *the request that would produce them is refused* — a stronger statement, and
the one the table's `Why it would need` column now records.

### One algorithm list, selected by the key usage

Each of the four algorithm members names a `KeyUsage` as its presence condition, and a
key has exactly one usage — so a key carries exactly one list, and the four are
mutually exclusive rather than four independent members. The contents come from the
developer guide's key spec reference, which tabulates them for the reason it states
twice: *"you cannot configure a KMS key to use a particular encryption algorithm"* and
*"you cannot configure a KMS key to use particular signing algorithms."*

| `KeyUsage` | Member | Example |
|------------|--------|---------|
| `ENCRYPT_DECRYPT` | `EncryptionAlgorithms` | `SYMMETRIC_DEFAULT` → itself; `RSA_2048` → the two `RSAES_OAEP` |
| `SIGN_VERIFY` | `SigningAlgorithms` | every RSA spec → the same six; `ECC_NIST_P384` → `ECDSA_SHA_384`; `ECC_NIST_EDWARDS25519` → **two**; the three ML-DSA specs → `ML_DSA_SHAKE_256` |
| `GENERATE_VERIFY_MAC` | `MacAlgorithms` | `HMAC_384` → `HMAC_SHA_384`, one per spec because *"the length of the key determines the MAC algorithm"* |
| `KEY_AGREEMENT` | `KeyAgreementAlgorithms` | `ECDH`, for the NIST curves and `SM2` |

**`KeyAgreementAlgorithms`' presence condition is substrate's reading.** The other three
members publish one; this member publishes none at all — its entire description is
*"the key agreement algorithm used to derive a shared secret"*. `KEY_AGREEMENT` is a
published `KeyUsage` and a key has one usage, so following the pattern of the three that
do state a condition is the narrowest available reading. The alternative — reporting
`ECDH` on a NIST-curve **signing** key, whose spec does admit it — would tell a caller
`DeriveSharedSecret` was available on a key AWS reserves for `Sign`. A test pins exactly
that case.

**An empty list is omitted rather than sent as `[]`.** When this was written `CreateKey`
validated neither `KeySpec` nor `KeyUsage`, so a caller could create an ECC key with
`KeyUsage` `ENCRYPT_DECRYPT` — a pair AWS would refuse — and that key admitted no
encryption algorithm. An empty array would say *"this key supports no encryption
algorithms"*, a claim AWS never makes about a key it accepted; the absent member says
nothing, which is the honest-empty reading #827 established.

[#977](https://github.com/scttfrdmn/substrate/issues/977) removed the premise by refusing
the pair, so no key substrate creates now reaches that branch. The branch stays, and so
does the reading behind it: it is the guard on a fifth algorithm member being added
without its pairing being thought through. The four maps it reads are also now the
**source** of the pairing rule — see *A key spec and a key usage must pair* below.

### `CustomerMasterKeySpec` is answered, and omitted outside its own enum

AWS still sends the deprecated member — *"the `KeySpec` and `CustomerMasterKeySpec`
fields have the same value. We recommend that you use the `KeySpec` field in your code.
However, to avoid breaking changes, AWS KMS supports both fields"* — so substrate
answers it rather than dropping it as obsolete: a consumer written against an older SDK
reads it, and omitting a member AWS sends is what this issue is about.

Its enum is **narrower** than `KeySpec`'s: `API_KeyMetadata` gives it 13 valid values
against `KeySpec`'s 17. The four specs added since the deprecation — `ML_DSA_44`,
`ML_DSA_65`, `ML_DSA_87` and `ECC_NIST_EDWARDS25519` — appear only under `KeySpec`, so a
key with one of those has no admissible value for the older member. **Substrate omits it
there**, which is a reading rather than a match: AWS documents no answer for the case,
and the alternative puts a value outside the member's own published set on the wire.

### The deprecated name is a request parameter too

`CustomerMasterKeySpec` appears in `API_CreateKey`'s **Request Parameters** as well, and
substrate decoded only the response half
([#985](https://github.com/scttfrdmn/substrate/issues/985)). A caller on an SDK old
enough to still send the deprecated name asked for `RSA_4096` and got a symmetric key —
the quietest failure available, a `200` with a complete `KeyMetadata` whose own
`CustomerMasterKeySpec` read `SYMMETRIC_DEFAULT`, contradicting the value it was sent.
It is now decoded and honored, and every rule of the section above applies to the spec
it names, since those rules are keyed on the resolved spec rather than on which member
carried it.

Two things AWS does not document, both substrate's reading and both refused rather than
resolved:

| Request | Substrate answers | Why |
|---------|-------------------|-----|
| `CustomerMasterKeySpec` outside its own 13 — `ML_DSA_44`, say | `ValidationError`/400, naming the member and its set, and pointing at `KeySpec` | The request-side half of the omission above: neither direction puts a value outside the member's own published set on the wire. The message must name the set, because the value *is* a good key spec and a bare refusal would send the caller hunting a typo that is not there |
| Both members sent with **different** values | `ValidationError`/400, naming both values | AWS documents no answer for the conflict; what it documents is that the two members *"have the same value"*, so a request in which they disagree is not one the page describes. Substrate cannot tell which was meant, which is why it refuses instead of choosing |

The two rejected alternatives are worth recording because both look reasonable. A
**precedence rule** — the newer member wins — discards half of a contradictory request
silently, which is the failure mode this issue exists to remove rather than relocate.
**Resolving by JSON member order** makes the answer depend on something no caller
controls meaningfully. Equal values are accepted: an SDK migrating between the two names
may send both, and that is a caller agreeing with itself.

The code is `ValidationError` in both rows, the same one every other malformed-member
refusal on this operation answers. That is deliberate rather than incidental — two codes
for one class of defect on one operation is what #977's analysis warned against, and this
member joins that decision rather than introducing a second.

### The eight members that stay absent

| Member | What it would need |
|--------|--------------------|
| `CloudHsmClusterId`, `CustomKeyStoreId`, `XksKeyConfiguration` | A custom or external key store, which substrate does not implement — and since #984 `CreateKey` refuses the request that would ask for one |
| `ExpirationModel`, `ValidTo` | An `EXTERNAL` origin, which the `Origin` section above records as unreachable — substrate's key material is always `AWS_KMS`, and since #984 by refusal rather than by omission |
| `MultiRegionConfiguration` | Published *"only when the value of the `MultiRegion` field is `True`"*. Substrate stores the flag but models no replica, so it would have to report a primary with an empty `ReplicaKeys` list — a shape describing a multi-Region key nothing can replicate |
| `PendingDeletionWindowInDays` | `KeyState` `PendingReplicaDeletion`, which only a multi-Region primary that still has replicas reaches. Its range is 1–365, not `ScheduleKeyDeletion`'s 7–30, so reporting the waiting period under it would be wrong twice over |
| `RotationEnabled` | Nothing — AWS does not publish it, per #971 above |

All eight are asserted **absent** by a test, `RotationEnabled` included. Adding sixteen
members from a page is exactly the moment someone working from the struct rather than
the page would put #971's member back, so the guard against that sits beside the guard
for the other seven.

`CurrentKeyMaterialId` was the ninth until substrate gained a key material identity;
the section below is what it needed.

## A key has a material identity, and six members report it

`API_KeyMetadata` publishes `CurrentKeyMaterialId` — *"identifies the current key
material… AWS KMS uses the current key material for both encryption and decryption, and
the non-current key material for decryption operations only"* — and five operation
responses carry the same identity under four other names. Substrate modelled no key
material at all, so all six were absent
([#978](https://github.com/scttfrdmn/substrate/issues/978)).

| Where | Member | Reported when |
|-------|--------|---------------|
| `CreateKey`, `DescribeKey` | `CurrentKeyMaterialId` | *"present for symmetric encryption keys with `AWS_KMS` or `EXTERNAL` origin"* |
| `Decrypt` | `KeyMaterialId` | *"present only when the operation uses a symmetric encryption KMS key"* |
| `ReEncrypt` | `SourceKeyMaterialId` | *"present only when the original encryption used a symmetric encryption KMS key"* |
| `ReEncrypt` | `DestinationKeyMaterialId` | *"present only when data is reencrypted using a symmetric encryption KMS key"* |
| `GenerateDataKey` | `KeyMaterialId` | The page states no key-type condition, only that it *"is omitted if the request includes the `Recipient` parameter"* |
| `GenerateDataKeyWithoutPlaintext` | `KeyMaterialId` | The page states no condition at all |

`Encrypt` publishes **none**, and that is recorded rather than left to be noticed: its
Response Syntax is exactly `CiphertextBlob`, `EncryptionAlgorithm` and `KeyId`, and a
test asserts those three exactly, so a later sweep over "the operations that report key
material" cannot add a site AWS does not have. The asymmetry is AWS's and is not
obviously deliberate — `Encrypt` names the material it used no more than `Decrypt` names
the material that produced the ciphertext it was handed.

**The value is stored on the key and read from it at all six sites, which is the point.**
AWS's member is named *Current*, and "current" is only meaningful against material that
can change; more practically, a caller compares what `Decrypt` reports against what
`DescribeKey` reports and learns that the material which decrypted its data is the
material the key holds. A value each site computed for itself would satisfy that
comparison by construction and prove nothing. `ReEncrypt` is where it shows: its two
members are the only two material identities in one response, they differ when the keys
differ, and each follows its own key — so a re-encryption out of an asymmetric key into a
symmetric one reports the destination member alone.

The identifier is derived, not drawn from `crypto/rand`: `SHA-256` over the key's own ARN
with a domain-separation prefix. AWS constrains all six members to a **fixed length of
64** with pattern `^[a-f0-9]+$`, and a `SHA-256` digest hex-encodes to exactly 64
lowercase hex characters, so no truncation or reshaping is involved. The ARN already
carries the account, the Region and the key ID, so two keys never collide and the
material ID inherits exactly the determinism the key ID has — becoming fully
deterministic once #856 reaches the key ID itself.

Two readings are substrate's rather than AWS's:

- **Rotation mints no new material.** Nothing rotates: `ListKeyRotations` and
  `RotateKeyOnDemand` are unimplemented, and the rotation section above already records
  that a rotation schedule here is a value reported to a caller rather than an event that
  fires. A second material identity nothing can list or ask for would be a value no call
  could reach, so `Current` is true in the trivial sense — it is the only material the key
  has ever had. Minting on rotation means implementing `ListKeyRotations` alongside it, so
  that the non-current identities `Decrypt` is documented to still accept are observable.
- **The two `GenerateDataKey*` operations are held to the same symmetric-encryption-key
  condition as the other four**, although their own pages state none. Both operations
  *require* a symmetric encryption key at AWS, which is why neither page needs a condition.
  Substrate did not enforce that until
  [#988](https://github.com/scttfrdmn/substrate/issues/988), because its check tested the
  key usage rather than the key spec, so an `RSA_2048` key with `KeyUsage`
  `ENCRYPT_DECRYPT` reached both responses; reporting unconditionally would have put a
  material ID on a response AWS cannot produce, and omitting said nothing about a request
  that should not have succeeded — the honest-empty reading. **#988 refuses that request, so
  at those two sites the condition is now a guard rather than a path**: every key reaching
  either response satisfies it, and no observable behaviour there depends on it. It is kept
  because one condition in one place is the whole point of the shared helper, and because a
  key written directly into state by a test can still reach those sites.

Note that *"symmetric encryption key"* is narrower than *"symmetric key"*: the four
`HMAC_*` specs are symmetric, hold key material, and encrypt nothing, so they report no
material identity. All seventeen key specs are walked by a test for exactly that reason —
an implementation reading the condition as "not asymmetric" passes every other row and
fails those four.

## A key with no policy has AWS's default policy, not an empty one

Substrate answered `{"Version":"2012-10-17","Statement":[]}` from `GetKeyPolicy` for any key
that had never had `PutKeyPolicy` called on it, and `CreateKey` decoded no `Policy` member at
all ([#983](https://github.com/scttfrdmn/substrate/issues/983)). So a caller that attached a
policy at creation time read back a document it had never sent, and every other key reported
a policy that grants nothing.

The second half is the one that matters beyond fidelity. A key policy is the only place a KMS
key's own permissions live — an IAM policy cannot grant access to a key whose key policy does
not delegate to IAM — so an empty-statement document is not a neutral placeholder. It states
the opposite of what AWS attaches.

**The document comes from `API_GetKeyPolicy`'s own Example Response**, which carries it in
full, rather than from the developer guide's *Default key policy* page that describes it in
prose. The reference page is the stronger source in one concrete way: it carries an `Id`
member, `key-default-1`, that the prose does not mention.

| Member | Value |
|--------|-------|
| `Version` | `2012-10-17` |
| `Id` | `key-default-1` |
| `Statement[0].Sid` | `Enable IAM User Permissions` |
| `Statement[0].Effect` | `Allow` |
| `Statement[0].Principal.AWS` | `arn:aws:iam::{account}:root` |
| `Statement[0].Action` | `kms:*` |
| `Statement[0].Resource` | `*` |

The account is the **key's own**, where AWS's example shows its documentation placeholder
`111122223333`: the whole content of the statement is that this key's account controls it, so a
document naming a foreign root would grant nothing to anyone who can reach the key. The
document is synthesised when it is read rather than written when the key is created, which is
observationally identical and also means a key created by an earlier release reports the
default rather than the old stand-in. Whitespace is not preserved from the example — AWS
formats the document with spaces around its colons — because a consumer parses the string.

**Three refusals, each with the code its page publishes.**

| Request | Answer |
|---------|--------|
| `Policy` outside 1–32768 bytes, on `CreateKey` or `PutKeyPolicy` | `LimitExceededException`/400 |
| `Policy` that is not JSON, or is JSON but not an object | `MalformedPolicyDocumentException`/400 |
| `PolicyName` other than `default`, on `GetKeyPolicy` or `PutKeyPolicy` | `NotFoundException`/400 |
| `KeyId` naming no key, on either operation | `NotFoundException`/400 |

`LimitExceededException` for a length violation is not an inference from that error's gloss —
`CreateKey`'s `Policy` member names the code itself: *"if the key policy exceeds the length
constraint, AWS KMS returns a `LimitExceededException`"*. The length is checked before the
document is parsed, so a caller sending 40 KB of valid JSON hears about the size. On
`PutKeyPolicy`, where `Policy` is `Required: Yes`, the range is also what refuses an empty
member, since a member present and empty satisfies presence.

`NotFoundException` for a `PolicyName` is substrate's one-step reading: neither page publishes
a code *for* that member, but both state that `default` is the only valid value and both publish
`NotFoundException` glossed *"the specified entity or resource could not be found"* — which a
name that names no policy is exactly. It keeps the refusal among the codes the page publishes
rather than reaching for `CommonErrors`.

Both member checks run **before** the key is looked up, following the same reading `Encrypt`'s
`Plaintext` guard follows: a request whose document is unusable and whose `KeyId` names no key
is told about the document, which is the half it can fix without an AWS account.

**Four things AWS publishes here that substrate does not implement**, recorded rather than
half-built:

- **The key policy lockout safety check, and therefore `BypassPolicyLockoutSafetyCheck`.** AWS
  requires that a supplied policy *"allow the calling principal to make a subsequent
  `PutKeyPolicy` request"* — a policy evaluation against the caller's principal, which is
  authorization machinery rather than this operation's business. With no lockout check,
  bypassing it is unobservable, so the member stays undecoded on both operations rather than
  becoming a parameter read by nobody.
- **Statement-level validation.** `MalformedPolicyDocumentException` is glossed *"not
  syntactically or semantically correct"*, but AWS's own `Policy` description narrows the
  semantic half almost to nothing: a statement missing `Action` or `Resource` *"has no
  effect"* while *"the `CreateKey` and `PutKeyPolicy` API requests succeed"*. There is no
  published statement-level refusal to implement, so substrate accepts such a document.
- **The `Policy` pattern**, which admits tab, line feed, carriage return and the printable
  range through U+00FF. Neither page publishes a code for a pattern violation.
- **`InvalidArnException`**, glossed *"a specified ARN, or an ARN in a key policy, is not
  valid"*. Nothing walks the principals, so it stays unreachable.

**Neither operation refuses a key for its state, and that is deliberate.** Both publish
`KMSInvalidStateException`, which reads like a missing refusal — but the developer guide's
key-state table gives `GetKeyPolicy` and `PutKeyPolicy` a checkmark in **all seven** state
columns, footnote-free, where `TagResource` two rows below is refused at pending deletion
under footnote [3]. A key pending deletion still takes a policy and still reports one, and a
test pins that so a later sweep completing the key-state checks cannot read the published code
as a gap.

## Cancelling a deletion leaves the key disabled, not enabled

`API_CancelKeyDeletion`'s first sentence is the whole of it: *"Cancels the deletion
of a KMS key. When this operation succeeds, the key state of the KMS key is
`Disabled`. To enable the KMS key, use `EnableKey`."* Recovery from a scheduled
deletion is two calls, and substrate answered `Enabled` — collapsing it to one.

That is not a cosmetic difference in a reported string. A consumer's recovery path
written against substrate would have passed with its `EnableKey` step missing, and
then failed against AWS. Substrate now leaves the key `Disabled` with `Enabled`
false, and the test asserts it by encrypting: `Encrypt` still refuses with
`DisabledException`/400 after the cancel, and succeeds only once `EnableKey` has
run. Reading `KeyState` back says the state changed; `Encrypt` refusing says the
state means what it says.

`API_KeyMetadata` states the invariant the two fields have to satisfy — *"`Enabled`:
when `KeyState` is `Enabled` this value is true, otherwise it is false"* — so the
state and the boolean are not independent, and a test asserts they agree.

## The deletion pair each permit one side of a single key state

`CancelKeyDeletion` and `ScheduleKeyDeletion` are each other's inverse, and the
*Key states of AWS KMS keys* table treats them that way. Both carry the
compatible-key-state sentence and both publish `KMSInvalidStateException`/400;
substrate checked neither.

| Operation | Permitted from | Refused from |
|-----------|----------------|--------------|
| `CancelKeyDeletion` | `PendingDeletion` **only** | Every other state — footnote `[4]`, *"KMSInvalidStateException: `<key ARN>` is not pending deletion"* |
| `ScheduleKeyDeletion` | Every state but the two at right | `PendingDeletion` — footnote `[3]`; `Updating` — footnote `[15]`, **unreachable in substrate** |

`CancelKeyDeletion` is the only operation in the table whose permitted set is a
single state, and the only one whose footnote is phrased as a **negation**. So it
gets its own refusal helper: naming the offending state, as every other refusal in
this package does, would read as though some other state were the problem when the
problem is the absence of the one state the operation needs.

Without that check the operation enabled any key it was pointed at, which is
`EnableKey` under another name — reachable by a caller holding
`kms:CancelKeyDeletion` and not `kms:EnableKey`.

On `ScheduleKeyDeletion` the missing check was quieter and no less real: a second
call against an already-pending key recomputed the deletion date and saved it, so a
caller that had scheduled a 7-day deletion and then called again silently moved a
deadline it believed was fixed. `Updating`, the row's other refused state, is
recorded as unreachable for the same reason the four rotation states are: nothing
in substrate writes it.

## `DescribeKey` reports a deletion date, so a refused schedule can be shown to have changed nothing

Substrate computed the deletion date inside `ScheduleKeyDeletion` and discarded it.
A caller could therefore learn when a key was due to be deleted exactly once — from
the response to the call that set it — and any later observation had lost it.

`API_KeyMetadata` publishes the member and bounds when it appears: *"the date and
time after which AWS KMS deletes this KMS key. This value is present only when the
KMS key is scheduled for deletion, that is, when its `KeyState` is
`PendingDeletion`."* Substrate now stores the date and renders it on that condition
— on the key state, not on the field being non-zero, so a cancelled deletion leaves
no date behind and the zero value is never emitted as an epoch timestamp.

Storing it is what makes the refusal above assertable. A re-stamped deadline is
invisible if the date is computed and thrown away, so the test reads the date back
through `DescribeKey` after the refused call rather than inspecting state.

**`PendingDeletionWindowInDays` is deliberately absent from `KeyMetadata`.** The
page confines it to `KeyState` `PendingReplicaDeletion`, which only a multi-Region
primary that still has replica keys reaches and substrate never writes — and its
range is **1–365**, not `ScheduleKeyDeletion`'s 7–30. The two are different members
measuring different things, so reporting the waiting period under it would be wrong
twice over.

## The waiting period is range-checked, at a code substrate chooses

`API_ScheduleKeyDeletion` states the range twice — *"you can specify a waiting
period of 7-30 days"* in the prose, and *"if you include a value, it must be between
7 and 30, inclusive"* on the parameter, with a published Valid Range to match.
Substrate's guard was `if days <= 0 { days = 30 }`, which got the default right and
the range not at all: `1`, `365` and `-5` were all accepted, and the last silently
became 30.

The response now echoes `PendingWindowInDays`, which AWS publishes and its own
sample response carries, so the default is observable rather than inferable from
date arithmetic. `KeyId` also became the key **ARN**, as the page says (*"the Amazon
Resource Name (key ARN)"*) and its sample shows; and `CancelKeyDeletion`, which
answered `{}`, now answers the single element the page publishes — `KeyId`, again
the key ARN — so a consumer reading `response["KeyId"]` gets something.

Two decisions here are substrate's, not AWS's:

**The refusal code.** AWS publishes no error for a range violation on this page: its
list is `DependencyTimeoutException`, `InvalidArnException`, `KMSInternalException`,
`KMSInvalidStateException` and `NotFoundException`, none of which describes a bad
parameter value. `ValidationError`/400 comes from `CommonErrors` for the same reason
an unparseable body does — a failure belonging to no operation-specific code has to
come from the common set. The bound is stated in the message, because a caller that
sent 1 has no way to discover 7–30 from a bare refusal.

**Which check runs first.** The range is checked before the key is resolved, so a
bad window against a key that does not exist answers `ValidationError` rather than
`NotFoundException`. AWS does not publish the precedence. A value wrong on the face
of the request is refused without a lookup, which is how an unparseable body already
behaves one line earlier.

One thing recorded rather than fixed: `PendingWindowInDays` decodes into an `int`, so
a body that omits the member and a body sending an explicit `0` are indistinguishable.
AWS refuses `0` and defaults an absent value. Substrate defaults both, because
telling them apart needs a `*int` and defaulting is by far the commoner intent — worth
changing only if a caller is ever shown to send an explicit zero.

`RotationPeriodInDays` **is** decoded into a `*int`, and the difference is not an
inconsistency. Here the two readings agree on the sensible answer, since AWS's default
of 30 coincides with the maximum a caller could plausibly have meant; there the default
of 365 sits in the middle of the range, so collapsing an explicit `0` into it would
accept a value AWS refuses and report a period the caller never asked for.

## The five cryptographic operations refuse the same two key states

`Encrypt`, `Decrypt`, `GenerateDataKey`, `GenerateDataKeyWithoutPlaintext` and
`ReEncrypt` share one row in the developer guide's *Key states of AWS KMS keys*
table, and each of their reference pages carries the same *"the KMS key that you use
for this operation must be in a compatible key state"* sentence. Substrate honored
that unevenly. Three of the five checked whether the key was enabled and answered
`DisabledException`; **two checked nothing at all**, so a disabled key generated a
data key or re-encrypted a ciphertext and answered `200`.

| Key state | Substrate answers | Provenance |
|-----------|-------------------|------------|
| `Enabled` | 200 | Permitted |
| `Disabled` | `DisabledException`/400 | Footnote `[1]` — *"DisabledException: `<key ARN>` is disabled"* |
| `PendingDeletion` | `KMSInvalidStateException`/400 | Cell reads `[2] or [3]`; **substrate's reading**, below |
| `PendingImport`, `Unavailable`, `Creating`, `Updating` | — | Refused by AWS, unreachable in substrate: nothing writes them |

**The missing refusal was the larger half.**
`GenerateDataKeyWithoutPlaintext` and `ReEncrypt` had no key check of any kind
between loading the record and returning a ciphertext. A caller that disabled a key
to stop it being used could still use it through either operation, which is the
defect class #923 catalogued rather than a question of which code to answer.

**The pending-deletion code is a decision, not a correction.** This is worth stating
plainly, because the neighbouring rotation section looks like the same finding and is
not. For the rotation pair the table's pending-deletion cell is footnote `[3]` alone,
so `DisabledException` there was simply the wrong one of two published codes. For
these five the cell reads **`[2] or [3]`**, and the two footnotes are the same
sentence under two different codes:

- `[2]` — *"DisabledException: `<key ARN>` is pending deletion (or pending replica deletion)"*
- `[3]` — *"KMSInvalidStateException: `<key ARN>` is pending deletion (or pending replica deletion)"*

So AWS admits either code here, and the `DisabledException` substrate answered before
was **inside** what the table publishes. `KMSInvalidStateException` is chosen anyway,
and the reason is what an emulator is for: it is the only one of the two choices under
which a consumer's error handler can tell the two states apart by code, and the two
states carry different remedies — `EnableKey` for a disabled key, `CancelKeyDeletion`
*then* `EnableKey` for one pending deletion, which is a genuine two-step recovery. It
also gives one key state one code across the whole plugin, matching what the rotation
pair and the deletion pair already answer.

**The message names the state either way**, so the distinction survives even for a
caller that matches on the code alone. Before this, both states answered *"is not
enabled"*, so neither the code nor the message told a caller which remedy applied — and
that, rather than the code, was the observable defect. A test compares the two answers
from one operation directly, so a later change cannot collapse them again by either
route: one code for both states, or two codes with one message.

**`GetKeyRotationStatus` remains deliberately unguarded**, and `EnableKey` and
`DisableKey` are guarded by neither this nor the rotation rule: their rows *permit* a
disabled key, so the enabled-check would refuse a call AWS accepts. They take the
pending-deletion arm alone — see *`EnableKey` cannot skip the cancel* below, which
completes the class.

## `EnableKey` cannot skip the cancel

`EnableKey` and `DisableKey` both went through one helper that resolved the key,
refused only a missing one, and then assigned whatever state it was handed. So
`EnableKey` against a key in `PendingDeletion` answered `200` and wrote `Enabled` — a
**one-call path from pending deletion to usable**, which AWS does not have
([#968](https://github.com/scttfrdmn/substrate/issues/968)).

That is worse than a missing refusal usually is, because it defeated a guarantee
another operation in the same file had just established: `CancelKeyDeletion` leaves a
key `Disabled` precisely so recovery is two calls, and an unguarded `EnableKey` skipped
the first one. It also abandoned the deletion silently, since the helper cleared the
deletion date on its way through, leaving no record that one had ever been scheduled.

Both operations have identical rows in the *Key states of AWS KMS keys* table:

| Key state | Answered | Provenance |
|-----------|----------|------------|
| `Enabled` | 200 | Permitted — so neither operation refuses a redundant call |
| `Disabled` | 200 | Permitted too, which is why the rotation rule cannot be reused here: an enabled-key check would refuse a call AWS accepts |
| `PendingDeletion` | `KMSInvalidStateException`/400 | Footnote `[3]`, which names the state — so the existing helper serves, with no sibling needed as footnote `[4]`'s negation required for `CancelKeyDeletion` |
| `Unavailable` | — | **Permitted, not refused.** Footnote `[12]`: *"the operation succeeds, but the key state of the KMS key does not change until it becomes available"* — a success with a deferred effect, unreachable in substrate and recorded so a later sweep does not read it as a refusal |
| `PendingImport`, `Creating`, `Updating` | — | Refused by AWS, unreachable: `ScheduleKeyDeletion` remains substrate's only writer of a state other than `Enabled` or `Disabled` |

**The guard lives in the shared helper**, and #963 is the counter-example that makes
that worth stating. Both callers here refuse the same single state, so one condition
serves both. `CancelKeyDeletion` was deliberately moved *off* that helper, because it
requires the **opposite** state and nothing else, phrases the absence of it as a
negation, and returns a body. Two callers wanting one refusal belong together; a third
wanting the inverse does not.

The refusal precedes the write, so a refused call leaves the key state, the enabled flag
**and the deletion date** exactly as they were — assertable only because #963 stores the
date and `DescribeKey` reports it. The date-clearing the helper used to do is gone
rather than kept as a safety net: the only state that carries a date can no longer reach
that line, and `CancelKeyDeletion` clears it on the one exit AWS documents, so keeping
it would be code no request can run — which this package records in a comment rather
than guards, the same disposition the unreachable states have.

With this, every key-state-sensitive operation in the package is guarded: the rotation
pair (#949), the five cryptographic operations (#961), the deletion pair (#963) and this
one. `GetKeyRotationStatus` is the only deliberate exception, for the reason given
above.

## `ReEncrypt` has two keys, and both are checked

`ReEncrypt` is the only cryptographic operation with two keys, and substrate loaded
one. The source key's identifier came out of the ciphertext and was discarded, so a
source key that was disabled, pending deletion, or absent from state entirely still
re-encrypted successfully.

Both keys are now resolved, loaded and state-checked. Nothing in AWS's table splits
the two — `ReEncrypt` has a single row whose pending-deletion cell is a plain
`[2] or [3]` — and three things point the same way: the page publishes
`DisabledException` and `KMSInvalidStateException` without qualification, its
*Required permissions* are split across the two keys (`kms:ReEncryptFrom` on the
source, `kms:ReEncryptTo` on the destination), and cross-account use is documented
for both. AWS therefore treats the source as a key the operation *uses*, not as a
value inside a blob.

One footnote would have exempted a pending-deletion source — `[10]`, *"if the source
KMS key is pending deletion, the command succeeds. If the destination KMS key is
pending deletion, the command fails"* — but it belongs to **`UpdateAlias`**'s row,
not to this one. Refusing both keys is therefore substrate's reading of an unsplit
row, and it is the conservative direction: it cannot admit a call AWS refuses.

**The source is checked first**, following the operation's own description —
*"Decrypts ciphertext and then reencrypts it entirely within AWS KMS"* — so a caller
whose source and destination are both unusable hears about the decrypt half. AWS
publishes no ordering, so this is recorded rather than matched, and a test pins it
with a bad source and an absent destination, where the two candidate answers are
visibly different.

**`SourceKeyId` is the source key's ARN.** It held the **ciphertext blob** before
#961 — a value that identifies nothing, and one a caller round-tripping it into
`DescribeKey` could only get `NotFoundException` from. `API_ReEncrypt` glosses the
element *"unique identifier of the KMS key used to originally encrypt the data"* and
its sample response renders a key ARN, so that is what substrate reports; the test
asserts the round trip through `DescribeKey` rather than string equality, because a
bare key ID would satisfy the weaker assertion while diverging from the sample.

The source key is loaded from the **caller's** account and Region, for the reason
`Decrypt` records: the key ID came out of the ciphertext, so there is no ARN to take
an account from. AWS permits a cross-account source here and substrate cannot address
one, because its stub ciphertext carries no account and no Region — #979's envelope
records what a *refusal* needs, not what a lookup needs. That is a limitation of the
stub cipher, not of the resolver.

Two of `ReEncrypt`'s nine request members are still unmodelled: `DryRun` and
`GrantTokens`. `SourceKeyId` as a *request* parameter, and both encryption-algorithm
members at both ends, arrived with #969; the two `*KeyMaterialId` response members
with #978; and the source and destination `EncryptionContext` pair with #979 — see the
sections below.

## A named key that is not the ciphertext's is refused

`Decrypt` takes a `KeyId` and `ReEncrypt` takes a `SourceKeyId`, and neither names the
key the operation will use — substrate finds that inside its own stub ciphertext. Both
members are a *constraint* on which key the ciphertext may belong to, and both were
decoded and then dropped, so naming the wrong key was indistinguishable from naming the
right one: the operation used the ciphertext's key and answered `200`
([#969](https://github.com/scttfrdmn/substrate/issues/969)).

AWS glosses the two members with one sentence and gives them one code, which is why one
check now serves both:

> Enter a key ID of the KMS key that was used to encrypt the ciphertext. If you identify
> a different KMS key, the `Decrypt` operation throws an `IncorrectKeyException`.

| Request | Substrate answers | Provenance |
|---------|-------------------|------------|
| The member is absent | 200, on the ciphertext's own key | Both members are published `Required: No` |
| The member names the ciphertext's key | 200 | The constraint is satisfied |
| The member names a different key KMS holds | `IncorrectKeyException`/400 | Published by `API_Decrypt` and by `API_ReEncrypt`, in the gloss above |
| The member names no key at all | `NotFoundException`/400 | Published; which of the two codes wins is **substrate's reading**, below |

**The comparison is between key ARNs, not between strings.** The member accepts all four
forms a `KeyId` accepts — a bare key ID, a key ARN, an alias name and an alias ARN — so it
is resolved to a key record first and the two records' ARNs are compared. Comparing the
member against the bare key ID inside the ciphertext would have refused three of those
four correct requests, and it would also have been wrong across accounts, where two keys
can share neither ARN nor much else. A test sends all four forms plus the absent case.

**A member naming nothing answers `NotFoundException`, not `IncorrectKeyException`.**
AWS publishes both codes for both operations and orders neither. Substrate reports the
absence, because the two codes ask the caller for different things: `IncorrectKeyException`
says *go find the right key*, which is useless advice when the identifier names no key —
there is a typo to fix instead.

**The wrong key is reported before the key state.** A request whose member names another
key *and* whose ciphertext key is disabled has two candidate answers, and substrate gives
the wrong-key one. AWS publishes no ordering, so this is recorded rather than matched; it
follows the direction #964 took, that a defect in the request precedes a condition of a
resource, and it avoids sending a caller after the state of a key it did not ask about. A
test pins it with the two answers visibly different.

`Encrypt`, `GenerateDataKey` and `GenerateDataKeyWithoutPlaintext` have no such member:
their `KeyId` *is* the key they use, and an absent or unresolvable one is already
`NotFoundException`.

## An encryption algorithm belongs to the key spec, not to the key

`Encrypt`, `Decrypt` and both ends of `ReEncrypt` take an encryption algorithm and report
one back. Substrate decoded none of the four members and reported none of them
([#969](https://github.com/scttfrdmn/substrate/issues/969)), so a caller could not say
which algorithm it wanted and could not read which one was used.

The absence was defensible while it lasted, because substrate performs no cryptography and
echoing `SYMMETRIC_DEFAULT` unconditionally would have reported a value derived from
nothing. What makes the value derivable is that **AWS does not let a key choose its
algorithm.** The developer guide's key spec reference is explicit — *"you cannot configure a
KMS key to use a particular encryption algorithm"* — and fixes the admissible set per key
spec instead, so the algorithm a request may use is a function of the key it names and the
member it sent, both of which substrate holds:

| Key spec | Admissible encryption algorithms |
|----------|----------------------------------|
| `SYMMETRIC_DEFAULT` | `SYMMETRIC_DEFAULT` — *"the only supported algorithm that is valid for symmetric encryption KMS keys"* |
| `RSA_2048`, `RSA_3072`, `RSA_4096` | `RSAES_OAEP_SHA_1`, `RSAES_OAEP_SHA_256` |
| `SM2` | `SM2PKE` |
| The ECC, HMAC and ML-DSA specs | **None.** They sign, generate MACs or derive shared secrets; no encryption algorithm applies |

Substrate still encrypts nothing. What it models is the **refusal**, which is the
observation a consumer's error path is written against, and it comes in two codes:

| Request | Substrate answers | Provenance |
|---------|-------------------|------------|
| The member is absent | 200, reporting `SYMMETRIC_DEFAULT` | Published: *"the default value, `SYMMETRIC_DEFAULT`, is the algorithm used for symmetric encryption KMS keys"* |
| The member is one of the published four and the key spec admits it | 200, reporting the caller's own value | The four are `SYMMETRIC_DEFAULT`, `RSAES_OAEP_SHA_1`, `RSAES_OAEP_SHA_256`, `SM2PKE` — one `Valid Values` line shared by all four members |
| The member is outside the published four | `ValidationError`/400, naming the member and listing the set | **Substrate's reading**: no operation page gives a code for a malformed enum member, so it comes from `CommonErrors.html` |
| The key spec does not admit the algorithm | `InvalidKeyUsageException`/400, naming the member, the key spec and what that spec does admit | Published — it is that code's own second gloss bullet, *"the encryption algorithm or signing algorithm specified for the operation is incompatible with the type of key material in the KMS key (`KeySpec`)"* |

**The two codes are ordered, and they are checked at different points.** A value outside
the published four says nothing about any key, so it is refused *before* the key is
resolved — a caller that misspells `RSAES_OAEP_SHA_256` and names a nonexistent key hears
about the misspelling, which is the ordering #964 established for a number out of range.
The key-spec check necessarily runs after the key is loaded, and after the key-state check,
so a caller holding a key pending deletion hears about the deletion rather than about an
algorithm it would not get to use.

**AWS's separate rule that the member is *"required only for asymmetric KMS keys"* falls
out of this with no branch of its own.** The default is `SYMMETRIC_DEFAULT`
unconditionally, and an RSA key does not admit it — so omitting the member on an RSA key is
refused for the ordinary reason, and the member is required exactly where AWS says it is.
A test asserts that, because the alternative implementation — a required-ness check keyed on
the key spec — is a second thing to keep in step with the table above.

**`ReEncrypt`'s two ends are independent.** The operation exists to move data between
keys, so the two algorithms need not agree: a symmetric source can re-encrypt to an RSA
destination, and each member is checked against its own key. The source is checked first,
matching the order #961 established for the two key states. A test re-encrypts across two
key specs and then swaps each end for the other's value, because an implementation that
validated both members against one key would pass every other assertion here.

The first bullet of `InvalidKeyUsageException`'s gloss — a `KeyUsage` incompatible with the
operation, such as an `ENCRYPT_DECRYPT` call against a `SIGN_VERIFY` key — is the next
section's subject, along with `CreateKey`'s former acceptance of any string as a `KeySpec`
([#977](https://github.com/scttfrdmn/substrate/issues/977)).

## `Plaintext` has two maximum sizes, and they are checked in two places

[#991](https://github.com/scttfrdmn/substrate/issues/991). `Encrypt`'s `Plaintext` member
carries two published constraints at two different levels, and substrate enforced neither
while answering a code the page does not publish for the one thing it did check.

| Constraint | Where AWS states it | What it depends on |
|---|---|---|
| 1–4096 bytes | The member's own **Length Constraints**, repeated in the page's opening sentence: *"encrypts plaintext of up to 4,096 bytes using a KMS key"* | Nothing but the request |
| A smaller per-key maximum | The page's own list, under *"the maximum size of the data that you can encrypt varies with the type of KMS key and the encryption algorithm that you choose"* | The key spec **and** the encryption algorithm |

| Key spec | Algorithm | Maximum |
|---|---|---|
| `SYMMETRIC_DEFAULT` | `SYMMETRIC_DEFAULT` | 4096 bytes |
| `RSA_2048` | `RSAES_OAEP_SHA_1` / `RSAES_OAEP_SHA_256` | 214 / 190 bytes |
| `RSA_3072` | `RSAES_OAEP_SHA_1` / `RSAES_OAEP_SHA_256` | 342 / 318 bytes |
| `RSA_4096` | `RSAES_OAEP_SHA_1` / `RSAES_OAEP_SHA_256` | 470 / 446 bytes |
| `SM2` | `SM2PKE` | 1024 bytes (China Regions only) |

The numbers are RSA's OAEP padding overhead made observable: the same key spec accepts 24
fewer bytes under `RSAES_OAEP_SHA_256`, because the padding carries a longer hash. Note that
AWS's list heads its RSA entries with a **key spec** and its last entry with an
**algorithm**; substrate keys the table on the pair, which loses nothing because
`SYMMETRIC_DEFAULT` and `SM2` each admit exactly one algorithm.

**The two constraints belong at different points in the handler**, and that is the finding
that made this more than a code correction. The range and the base64 decode are facts about
the request, so they are answered *before* the key is resolved — the same split the
[encryption algorithm](#an-encryption-algorithm-belongs-to-the-key-spec-not-to-the-key)
already makes between its enum check and its key-spec check. The per-key maximum cannot be
answered until the key is loaded and the algorithm resolved, so it is the last check the
operation makes:

| Request | Substrate answers |
|---|---|
| `Plaintext` is not base64 | `ValidationError`/400, before any key is loaded |
| `Plaintext` decodes to 0 bytes, or to more than 4096 | `ValidationError`/400 naming the range, before any key is loaded |
| The pair is admissible and the data fits | 200 |
| The data exceeds the pair's maximum | `ValidationError`/400 naming the key spec, the algorithm, the size sent and the maximum |

So a caller that sends 5 KB to a key that does not exist now hears about the 5 KB, and a
caller whose `Plaintext` is unusable hears about it whether the key is disabled, is a signing
key, or is absent. Tests assert all three, because no assertion on a code alone can show an
ordering.

**Neither refusal has a published code**, and `ValidationError`/400 from `CommonErrors.html`
is substrate's reading for both — the same landing place as an out-of-range waiting period
and an unpublished encryption algorithm. `InvalidKeyUsageException` is the near miss for the
per-key maximum and is deliberately not used, although the condition does involve the key
spec and the algorithm: nothing about the pairing is incompatible — the key admits the
algorithm, and a shorter plaintext would succeed — so a caller reading that code would change
its algorithm when what it has to change is how much data it sends per call.

**The code this replaces was `InvalidCiphertextException`**, which is not among `API_Encrypt`'s
nine errors and is published on `Decrypt` and `ReEncrypt`, whose `CiphertextBlob` decode is
what it exists for. Those two sites are untouched: `Encrypt` produces a ciphertext, it does
not consume one, and a sweep that unified the three would have taken a published code off the
two operations that own it.

One consequence is worth naming because it looks like a gap. `SYMMETRIC_DEFAULT`'s per-key
maximum *is* the member's 4096, so the per-key branch is unobservable for a symmetric key —
a symmetric request one byte over is refused by the range check before any key is read. The
per-key refusal is therefore an asymmetric-key observation only, which is also the direction
that matters: a consumer that encrypts a database password under a symmetric key and later
switches to an RSA key crosses a 190-byte boundary without changing its request shape at all.

## An encryption context is authenticated data, so the ciphertext has to carry it

`Encrypt`, `Decrypt`, both `GenerateDataKey*` operations and both ends of `ReEncrypt`
take an encryption context. Substrate decoded **none** of the six members
([#979](https://github.com/scttfrdmn/substrate/issues/979)), so a context sent on the
way in was accepted and lost, and the refusal AWS publishes for a mismatch on the way
out was unreachable. `API_Encrypt` states the rule as a consequence rather than a
footnote:

> If you specify an `EncryptionContext` when encrypting data, you must specify the same
> encryption context (a case-sensitive exact match) when decrypting the data. Otherwise,
> the request to decrypt fails with an `InvalidCiphertextException`.

That is the whole reason the member is worth modelling. An application that encrypts
under `{"tenant": "acme"}` and decrypts without it is broken in production and passed
here, which is exactly the class of defect an emulator exists to catch before AWS does.

**The stub ciphertext changed format to carry it,** and that is a compatibility break: a
blob written by an earlier release no longer decodes, and `Decrypt` answers
`InvalidCiphertextException`/400 for it rather than reading its fields under the new
meanings. The old format was a delimited string; the new one is a base64-wrapped JSON
envelope recording the key ID, the encryption algorithm, the encryption context and the
plaintext. **JSON rather than a delimiter is a correctness requirement, not a
preference** — an encryption context is caller-supplied text, so a key or a value may
contain any delimiter, and an escaping bug would produce a ciphertext that decrypts to
the *wrong* context, which is the failure the feature exists to detect. `json.Marshal`
sorts map keys, so one input produces one blob however the caller ordered its context; a
test asserts that two `Encrypt` calls whose contexts differ only in JSON key order return
byte-identical ciphertext, because a blob that varied with the caller's ordering could not
be replayed.

Two refusals become reachable, and they are ordered:

| Request | Substrate answers | Provenance |
|---------|-------------------|------------|
| The context matches exactly | 200, and the plaintext round-trips | Published: *"an exact case-sensitive match"* |
| No context recorded, none supplied | 200 | Both members are `Required: No` |
| An empty context against an absent one, either direction | 200 | **Substrate's reading** — AWS documents no way to tell `{}` from an omitted member |
| The recorded and supplied contexts differ in any key, value or case | `InvalidCiphertextException`/400, naming both | Published end to end: the code by `API_Encrypt`'s sentence above, the condition by the code's own gloss — *"the specified ciphertext, or additional authenticated data incorporated into the ciphertext, such as the encryption context, is corrupted, missing, or otherwise invalid"* |
| The encryption algorithm is not the one that encrypted the data | `InvalidCiphertextException`/400, naming both | The *behaviour* is published — *"if you specify a different algorithm, the `Decrypt` operation fails"* — the **code is substrate's reading**, resting on that gloss's *"or otherwise invalid"* |
| The blob is not a substrate ciphertext at all | `InvalidCiphertextException`/400 | The envelope carries a format marker, so a foreign, truncated or previous-format blob is refused rather than misread |

**The algorithm is checked before the context.** AWS orders the two nowhere, so this is
recorded rather than matched: the algorithm is what a real implementation needs in order
to attempt a decryption at all, where the context is authenticated once the decryption has
happened, so a caller wrong about both hears about the one that would have stopped it
first. Both checks run *after* the named-key, key-usage, key-state and key-spec checks — an
algorithm the key does not admit is a fact about the key, knowable without holding the
ciphertext, where a mismatch is a fact about these particular bytes.

`InvalidCiphertextException` rather than `IncorrectKeyException` for both, and the
distinction is the point: `IncorrectKeyException` is about the key the caller *named*,
which the section above answers, while these are about the request disagreeing with the
blob. Both messages render both contexts, which is licensed rather than assumed —
`API_GenerateDataKey` says *"do not include confidential or sensitive information in this
field. This field may be displayed in plaintext in CloudTrail logs and other output"*, and
a refusal a caller cannot act on is not worth answering.

**A context is recorded only under a symmetric encryption key, and that split is AWS's
own.** `API_ReEncrypt` is decisive: *"a destination encryption context is valid only when
the destination KMS key is a symmetric encryption KMS key. The standard ciphertext format
for asymmetric KMS keys does not include fields for metadata."* So an asymmetric
ciphertext has nowhere to hold a context, and AWS publishes no code for sending one —
recording it would invent a refusal AWS cannot produce. Substrate accepts the member and
ignores it, and a test asserts the *opposite* shape from everything above: two different
contexts across an `Encrypt` and a `Decrypt` under an RSA key must **succeed**.

The encryption *algorithm* is recorded for every key regardless, although AWS's asymmetric
format holds no metadata either. AWS reaches the same **observable** answer
cryptographically — decrypting RSA ciphertext with the wrong OAEP hash fails — and
substrate models the observation rather than the mechanism. The divergence is confined to
bytes inside the blob, which no caller is entitled to read; the answer, which every caller
is, agrees.

**`ReEncrypt` is where the two members are visibly different things.**
`SourceEncryptionContext` is *matched* against the incoming blob — *"enter the same
encryption context that was used to encrypt the ciphertext"* — while
`DestinationEncryptionContext` is *written* into the outgoing one. So a `ReEncrypt` is the
call that **changes** a ciphertext's context, and one consequence is worth stating because
nothing in AWS's text suggests otherwise: re-encrypting with no destination context
**strips** the context rather than inheriting the source's. A caller relying on inheritance
would find its data readable without the context it believed it had set, so a test pins it.

Both `GenerateDataKey*` operations record the context and neither publishes
`InvalidCiphertextException`, which is consistent — the recording happens there and the
refusal happens at `Decrypt`. `API_GenerateDataKey` states the round trip in its own words
and substrate's tests assert it through the only thing a caller can observe: the wrapped
data key decrypts under the recorded context and is refused without it.

**Still unmodelled, and recorded here rather than left to be discovered.** The two IAM
condition keys built on this member — `kms:EncryptionContext:<key>` and
`kms:EncryptionContextKeys` — are not evaluated, so a key policy or grant constraining a
context is accepted and has no effect on whether a request is authorized. AWS also
publishes limits substrate does not enforce: the total size of an encryption context, and
the reserved `aws:` key prefix. Neither is observable through a response substrate
produces today.

## A key spec and a key usage must pair

`CreateKey` accepted **any string** for `KeySpec` and `KeyUsage` and paired them however a
caller asked ([#977](https://github.com/scttfrdmn/substrate/issues/977)). Both members are
immutable — *"you can't change the `KeySpec` after the KMS key is created"*, and the same
sentence for `KeyUsage` — so an unvalidated value is not a member a later call can correct.
It is a key that will never behave as its metadata says, and every operation reading either
member reads a value AWS would never have stored: `KeySpec` `"rsa2048"` reports an algorithm
list for no spec at all, and `KeyUsage` `"ENCRYPT"` reports none.

Three defects, three answers:

| Request | Substrate answers | Provenance |
|---------|-------------------|------------|
| A `KeySpec` or `KeyUsage` outside its published enum | `ValidationError`/400, listing the enum | **Substrate's reading**: `API_CreateKey` publishes 13 errors and `ValidationException` is not among them, so a malformed enum member comes from `CommonErrors.html`, as it does for the encryption algorithm above |
| `KeyUsage` absent for any spec but `SYMMETRIC_DEFAULT` | `ValidationError`/400, naming the spec and what it admits | Published as a requirement: *"this parameter is optional when you are creating a symmetric encryption KMS key; otherwise, it is required"* — the code is substrate's reading, for the same reason |
| A well-formed pair the spec does not admit | `UnsupportedOperationException`/400, naming both and what the spec admits | Published for `CreateKey`, glossed *"a specified parameter is not supported or a specified resource is not valid for this operation"* |

**The two codes say different things, which is why they are not one.** `ValidationError`
says *that is not a key spec*; `UnsupportedOperationException` says *that is a key spec, and
not with that usage*. A caller that misspelled a value and one that chose an impossible
combination have different fixes, and the first is refused before the second is considered —
a `KeySpec` of `"ECC_NIST_P25"` with a `KeyUsage` of `ENCRYPT_DECRYPT` hears about the spec.

**The default fires for exactly one spec.** AWS's HMAC guidance settles it: *"you must set
the key usage even though `GENERATE_VERIFY_MAC` is the only valid key usage value for HMAC
KMS keys."* So `KeyUsage` is not defaulted to the single admissible value wherever there is
one — it is defaulted only for `SYMMETRIC_DEFAULT`, and required everywhere else.

**The pairing table is derived, not written.** A spec admits a usage exactly when its
algorithm list for that usage is non-empty, read from the four maps
[#974](https://github.com/scttfrdmn/substrate/issues/974) already built for `DescribeKey`'s
algorithm members. Writing AWS's seven pairing bullets out a second time would create the
drift class this file records elsewhere: two tables that must agree, with nothing making
them. Here there is one table, and the metadata builder and the validator read it through
the same accessor — so a key cannot be accepted for a usage whose algorithm list it would
then not carry. The **test** transcribes AWS's bullets by hand, all 17 specs × 4 usages, so
the derivation and the page disagree unless both match.

**A key usage is also checked at the five cryptographic operations**, which is
`InvalidKeyUsageException`'s *first* gloss bullet — *"the `KeyUsage` value of the KMS key is
incompatible with the API operation"* — where the encryption-algorithm section above
implements the second. AWS states the requirement: *"for encrypting, decrypting,
re-encrypting, and generating data keys, the `KeyUsage` must be `ENCRYPT_DECRYPT`."* So
`Encrypt`, `Decrypt`, `GenerateDataKey`, `GenerateDataKeyWithoutPlaintext` and both ends of
`ReEncrypt` refuse a key whose usage is anything else.

That check runs **before** the key-state check and before the encryption-algorithm check.
Before the state check because a key usage is permanent and a key state is not — telling a
caller to enable a `SIGN_VERIFY` key sends it round a loop that cannot terminate, which is
the same argument the rotation section above makes for refusing a non-symmetric key spec
ahead of that key's state. Before the algorithm
check so that one condition yields one message: a `SIGN_VERIFY` RSA key addressed by
`Encrypt` hears about its usage, not about `SYMMETRIC_DEFAULT` being inadmissible for RSA.

**What this changes for a CloudFormation template.** `AWS::KMS::Key` documents `KeySpec`'s
default as `SYMMETRIC_DEFAULT` and `KeyUsage`'s as `ENCRYPT_DECRYPT`, and substrate's
deployer sends both unconditionally — so a template naming an asymmetric or HMAC `KeySpec`
without a `KeyUsage` is now refused where it previously created a key AWS would not have.
The defaults stay, because they are the resource type's own and because that template is
invalid at AWS too: the property *"is required for asymmetric KMS keys and HMAC KMS keys"*.

## A data key needs a symmetric encryption key, and the usage check does not say so

The key-usage check above is not the same condition as *"this key can wrap a data key"*, and
[#988](https://github.com/scttfrdmn/substrate/issues/988) is the gap between them. An
`RSA_2048` key created with `KeyUsage` `ENCRYPT_DECRYPT` is a pair AWS publishes and
`CreateKey` must accept, so it passed the usage check — and `GenerateDataKey` handed such a
caller a wrapped data key and a `200`.

AWS states the restriction four times over between the two pages, twice in a description and
twice on the `KeyId` parameter itself:

- `API_GenerateDataKey` — *"to generate a data key, specify the symmetric encryption KMS key
  that will be used to encrypt the data key. You cannot use an asymmetric KMS key to encrypt
  data keys."*
- `API_GenerateDataKeyWithoutPlaintext` — *"you cannot use an asymmetric KMS key or a key in
  a custom key store to generate a data key."*
- Both, on `KeyId` — *"specifies the symmetric encryption KMS key that encrypts the data key.
  You cannot specify an asymmetric KMS key or a KMS key in a custom key store."*

**The plural in *"data keys"* is why this is not a condition on `Encrypt` as well.** The
corresponding sentence there is about *data*, and an RSA encryption key encrypts data
perfectly well — the algorithm section above is the only thing `Encrypt` owes such a key. The
observable form of the same rule is that these two operations take no `EncryptionAlgorithm`
member at all: AWS gives a caller no way to name an asymmetric algorithm here, because no
asymmetric key belongs here.

| Request | Substrate answers | Provenance |
|---------|-------------------|------------|
| `SYMMETRIC_DEFAULT` | 200, wrapping the data key | The only spec either operation accepts |
| `RSA_2048`, `RSA_3072`, `RSA_4096`, `SM2` with `KeyUsage` `ENCRYPT_DECRYPT` | `InvalidKeyUsageException`/400, naming the key spec | The restriction is published; **the code is substrate's reading** — see below |
| An `HMAC_*` spec, or any signing or key-agreement spec | `InvalidKeyUsageException`/400, naming the key **usage** | Unreachable at this check: the usage check refuses such a key first, and `CreateKey` will not pair those specs with `ENCRYPT_DECRYPT` at all |
| A key in a custom key store | — | Unreachable: no stored key can have an origin other than `AWS_KMS` since [#984](https://github.com/scttfrdmn/substrate/issues/984) |

**The code is substrate's reading of an unsplit bullet.** Neither of
`InvalidKeyUsageException`'s two published bullets describes this condition exactly: the
key's `KeyUsage` *is* `ENCRYPT_DECRYPT`, which is what the operation wants, and the operation
specifies no encryption algorithm for the second bullet to find incompatible. What is wrong
is the `KeySpec` alone — the second bullet's subject, reached by a route it does not describe.
It is still the right answer on three grounds: it is the only code either page publishes
about a key being the wrong kind for the operation (the other eight are two 500s, a grant
token, a dry run, a key state, a disabled key, a missing key and an internal error); the
restriction it enforces is published four times over; and the wrap has a fixed algorithm, so
the second bullet does fit on the reading that the operation specifies `SYMMETRIC_DEFAULT`
implicitly.

**The message names the key spec, where the usage refusal names the key usage.** One code
carries three conditions across these operations, so the message is the only thing that tells
them apart — and naming the usage here would be actively wrong, since the usage is the one
thing about such a key that is correct. It also names `GenerateDataKeyPair`, because AWS sends
an asymmetric caller there (*"to generate an asymmetric data key pair, use the
`GenerateDataKeyPair` or `GenerateDataKeyPairWithoutPlaintext` operation"*) and **substrate
implements neither**. Saying so in the refusal rather than only here is deliberate: this
refusal is where a caller is standing when it needs to know, and being redirected to an
operation that answers an unknown-action error would be worse than being told the truth.

**Ordered after the usage check and before the key-state check**, and both halves are
substrate's reading of conditions AWS states without precedence. Usage first, because a
request naming a signing key is wrong about the operation rather than about the key material,
and that refusal is uniform across all five cryptographic operations where this one covers
two. Key state after, following the rotation section's argument exactly: a key spec is
permanent — *"you can't change the `KeySpec` after the KMS key is created"* — while a key
state is transient and has a remedy, so answering the state first would tell a caller that
enabling the key makes the call succeed, which for an RSA key is false however many times it
retries. Tests assert each side through the *message*, since the neighbouring refusals are
one shared code and one shared 400.

**One helper serves both operations**, and that is a claim about this service's history
rather than a preference: [#961](https://github.com/scttfrdmn/substrate/issues/961) found
`GenerateDataKeyWithoutPlaintext` refusing *nothing* while its four siblings each refused
something. A test compares the two refusals to each other, so two sites cannot answer one
code with two explanations.

## A key is reachable through the tagging API

`TagResources`, `UntagResources` and `GetResources` all reach a KMS key. The
resolver and KMS's own three tagging operations share one key builder, so a key's
tags are at one address whichever arm writes them, and a tag written through
either is readable through the other.

An alias ARN is refused by the tagging API rather than followed to its key. The
developer guide's list of what cannot be tagged — aliases, custom key stores, AWS
managed keys, AWS owned keys, and keys in other accounts — is the boundary, and
an alias is the one of those five substrate stores in the same namespace.

The namespace holds five kinds of key and only the key record stores tags, so the
merge is guarded on a **colon-terminated** prefix. Without the colon, `key:`
would match `key_ids:` and `key_policy:` and `alias:` would match
`alias_names:` — and `key_ids` is a JSON array of identifier strings, which a
tags merge would leave looking like a record. This is the third namespace to need
that guard; `cert`/`cert_arns` and `cfdist`/`cfdist_ids` were the first two.

KMS is the first row whose tags are an **array** rather than a map, so the merge
goes through a separate helper that takes the element's two field names as
parameters. KMS spells them `TagKey` and `TagValue` — alone among the four
services #835's remaining rows cover, the other three spelling them `Key` and
`Value`. Both merge paths sort the result by key, because a slice built by
ranging a Go map comes out in the map's hash order and one recorded run would not
replay byte-identically.

A key pending deletion is still reported by `GetResources` if it has been tagged, or
once was (#938). AWS says you may not *tag* such a key, but a listing is a read and
the key exists until its waiting period elapses; refusing the write is a separate
unmodelled behaviour recorded on #922.

## The account and Region a record carries reach no response

`KMSKey` declares `AccountID` and `Region` under wire-visible `json` tags, plus `EverTagged` as
`ever_tagged,omitempty`, because the record is what `MemoryStateManager` snapshots and a replay reads
back. None of them reaches a body. `CreateKey` and `DescribeKey` answer `KeyMetadata`, which is built
member by member (`emulator/kms_key_metadata.go`). Its published `AWSAccountId` is fed from the
record's account, which is the service's own data rather than a leak. Every other operation answers a
map of published members.

All twenty-four routed operations are driven by `TestKMSWire_KeyResponsesCarryNoBookkeepingMember` in
`emulator/kms_wire_test.go` ([#756](https://github.com/scttfrdmn/substrate/issues/756)). The key is
tagged first and `ever_tagged` is read back, since only a tag write sets it (#938).

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::KMS::Key | KeyId | |
| AWS::KMS::Alias | — | |

## Cost

KMS API requests: $0.03 per 10,000 requests.
