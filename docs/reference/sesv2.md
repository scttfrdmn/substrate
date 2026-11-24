# SES v2

**Endpoint:** `email.{region}.amazonaws.com`
**Protocol:** REST/JSON

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateEmailIdentity | `Tags` is read as the published array of `{Key, Value}` objects (#1335). Answers `{}` where the page publishes `IdentityType`, `VerifiedForSendingStatus` and `DkimAttributes` |
| GetEmailIdentity | `IdentityType` and `Tags`, the latter as the published array in key order; the other published members are not modeled. No member the page does not publish |
| DeleteEmailIdentity | |
| ListEmailIdentities | |
| SendEmail | Returns stub MessageId; does not deliver |

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

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::SES::EmailIdentity | EmailIdentityName | |

## Cost

SES outbound email: $0.10 per 1,000 emails.
