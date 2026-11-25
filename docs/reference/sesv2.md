# SES v2

**Endpoint:** `email.{region}.amazonaws.com`
**Protocol:** REST/JSON

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateEmailIdentity | `Tags` is read as the published array of `{Key, Value}` objects (#1335). A domain gets three Easy DKIM tokens, derived like SES v1's. Answers `{}` where the page publishes `IdentityType`, `VerifiedForSendingStatus` and `DkimAttributes` |
| GetEmailIdentity | `IdentityType`, `Tags` (the published array in key order), the seeded `VerificationStatus` with `VerifiedForSendingStatus`, `FeedbackForwardingStatus`, and `DkimAttributes` and `MailFromAttributes` once [SES v1](/reference/ses) set them up. `Policies`, `ConfigurationSetName` and `VerificationInfo` are not modeled. No member the page does not publish |
| DeleteEmailIdentity | |
| ListEmailIdentities | |
| SendEmail | Returns stub MessageId; does not deliver |

## Identities are shared with SES v1

SES v2 and [SES v1](/reference/ses) read and write the same identity records, and one seed (`POST /v1/ses/identity-verification`) governs the verification status both report; see the SES page. The two share the `email` host and the `ses` signing name, so a request reaches SES v2 by its `/v2/` path.

## The account and Region a record carries reach no response

`SESv2Identity` declares `AccountId`, `Region` and `CreatedAt` under wire-visible `json` tags,
because the record is what `MemoryStateManager` snapshots and a replay reads back. Until
[#756](https://github.com/scttfrdmn/substrate/issues/756), `GetEmailIdentity` answered the record as
its whole body. That put all three members on the wire, plus `IdentityName`, which the response does
not carry because the caller named the identity in the request path. It now answers through
`sesv2IdentityOut` (`emulator/sesv2_wire.go`). All five routed operations are driven by
`TestSESv2Wire_IdentityResponsesCarryNoBookkeepingMember` in `emulator/sesv2_wire_test.go`, which
walks each decoded document for a bookkeeping member at any depth.

`Tags` was a map on both sides of the wire, where the API publishes an **array of Tag objects** on
both ([#1335](https://github.com/scttfrdmn/substrate/issues/1335)). A tagged `CreateEmailIdentity`
from an SDK sends that array, so it was refused with a 400. A tagged identity's `GetEmailIdentity`
answered an object that no typed SDK could decode. Both sides now use the array. The stored record
keeps its map, so a recorded run replays identically, and the response sorts the array by key so one
identity always answers the same bytes.

`SESv2CapturedEmail` also declares `AccountId` and `Region`, but no AWS response renders it.
`SendEmail` answers a `MessageId`, and the record reaches a body only through Substrate's own
`GET /v1/emails`, where the account and Region are the point. It is recorded as such in
`scripts/wire-bookkeeping-internal.txt`, anchored on that route.

## Request validation

SESv2 is validated against its service model before any plugin sees a client's request (#1612; see
[Request validation](/reference/concepts/validation)). Every failure answers
`BadRequestException`/400, which every routed operation publishes, in SESv2's form, which leaves
the value out: `1 validation error detected: Value at 'emailIdentity' failed to satisfy
constraint: Member must have length greater than or equal to 1`. Enum checks are on. The form
and the enum enforcement are from SESv2's observed answers, cited in
`emulator/wire_validate_errors.go`; no source shows SESv2's answer for a missing member, so its
`Value null at 'emailIdentity'` form is substrate's reading.

`SendEmail` without `Content` is refused, and `CreateEmailIdentity` without `EmailIdentity`
answers the validator's message rather than substrate's own, also from the plugin for a request
that reaches it without the gate.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::SES::EmailIdentity | EmailIdentityName | |

## Cost

SES outbound email: $0.10 per 1,000 emails.
