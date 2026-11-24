# WAFv2

**Endpoint:** `wafv2.{region}.amazonaws.com` (and `wafv2.us-east-1.amazonaws.com` for
`Scope: CLOUDFRONT`)
**Protocol:** JSON (`X-Amz-Target: AWSWAF_20190729.{Op}`)

**Routing:** `parser.go` aliases `awswaf` to the `wafv2` plugin. Classic WAF (`AWSWAF_20150824`) is not
routed.

**Scope is part of a resource's identity.** A Web ACL's and an IP set's state keys carry the account,
the Region and the **scope**, so the same name in `REGIONAL` and `CLOUDFRONT` is two resources, which is
what AWS's own model implies. An invalid `Scope` is refused everywhere except `GetWebACL`, where AWS
marks the member `Required: No` and Substrate defaults it to `REGIONAL` deliberately (#1062). The
`assoc:` keys that record `AssociateWebACL` omit the scope, since a regional resource ARN can only be
associated with a regional Web ACL.

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateWebACL | `Name`, `Scope` and `DefaultAction` are read; `Rules` and `VisibilityConfig` are stored as opaque objects |
| GetWebACL | `Scope` defaults to `REGIONAL`; resolvable by `ARN` or by the `Name`+`Id`+`Scope` triple. Seven of `API_WebACL`'s members and nothing it does not publish; the lock token is answered once, at the top level |
| UpdateWebACL | Requires a matching `LockToken`; [merges rather than replaces](#updatewebacl-merges-so-a-member-can-never-be-cleared) |
| DeleteWebACL | Requires a matching `LockToken`; [refuses an ACL still associated with a resource](#a-web-acl-in-use-cannot-be-deleted) with `WAFAssociatedItemException`/400 |
| ListWebACLs | [Pages by `Limit` and `NextMarker`](#both-list-operations-page-by-limit-and-nextmarker) |
| AssociateWebACL | Records the association. `WebACLArn` must name a web ACL (`WAFNonexistentItemException`/400); the resource ARN is not checked against any service's state |
| DisassociateWebACL | |
| GetWebACLForResource | [Reports the associated WebACL in full](#getwebaclforresource-reports-the-web-acl-itself), and no `WebACL` for a resource with no association |
| CreateIPSet | `Name`, `Scope`, `IPAddressVersion` and `Addresses` are read; a CIDR is not validated |
| GetIPSet | All six members `API_IPSet` publishes and nothing it does not; the lock token is answered once, at the top level |
| UpdateIPSet | Requires a matching `LockToken`; merges |
| DeleteIPSet | Requires a matching `LockToken` |
| ListIPSets | [Pages by `Limit` and `NextMarker`](#both-list-operations-page-by-limit-and-nextmarker) |

## Both list operations page by Limit and NextMarker

[#1161](https://github.com/scttfrdmn/substrate/issues/1161). `ListWebACLs` and `ListIPSets` read
neither `Limit` nor a cursor, so every call returned the whole set and a consumer's `NextMarker` loop
ran once whatever the data. Both pages publish `Limit` with a Valid Range of 1–100 and `NextMarker` on
request and response, and both are now read:

- **`Limit`** bounds the page. Neither page publishes a default; an absent `Limit` is 100, the
  published maximum, so a listing of up to 100 objects answers exactly what it did. A `Limit` outside
  1–100 is `WAFInvalidParameterException`/400, the rule [Eight JSON list operations refuse a page size
  outside the published range](/reference/concepts/listing-order#eight-json-list-operations-refuse-a-page-size-outside-the-published-range)
  records, with the code both pages publish for "a parameter name or value that isn't valid".
- **`NextMarker`** is emitted only when objects remain, and resumes after them. A marker substrate did
  not issue is the same `WAFInvalidParameterException`, rather than page one.
- A record that cannot be read fails the listing instead of being skipped.

## GetWebACLForResource reports the Web ACL itself

[#1161](https://github.com/scttfrdmn/substrate/issues/1161). The response was `{"WebACL": {"ARN": "…"}}`,
where AWS publishes the full `WebACL`, whose `DefaultAction` and `VisibilityConfig` are `Required: Yes`.
The association's ARN is now resolved to the stored record and answered through the projection
`GetWebACL` uses, so the two report one ACL identically.

A resource with no association answers 200 with no `WebACL` member: the page says *"If there is no
associated resource, AWS WAF returns a null web ACL."* It answered `WAFNonexistentItemException`/400
until [#1541](https://github.com/scttfrdmn/substrate/issues/1541), which settled the reading from the
page. An association whose Web ACL is gone would still answer that code, but the API can no longer
reach the case: see [A web ACL in use cannot be deleted](#a-web-acl-in-use-cannot-be-deleted).

## A web ACL in use cannot be deleted

`API_DeleteWebACL`: *"Before deleting any web ACL, first disassociate it from all resources."* It
publishes `WAFAssociatedItemException`/400. `DeleteWebACL` now refuses an ACL that a resource is
associated with through `AssociateWebACL`, and leaves it in place
([#1541](https://github.com/scttfrdmn/substrate/issues/1541)). The page orders no checks; the
association is checked before the lock token, so a stale token learns the same thing. The message,
which names the resource, is substrate's own.

`AssociateWebACL` now requires `WebACLArn` (`Required: Yes`) and refuses one naming no web ACL with
`WAFNonexistentItemException`/400, which the page publishes, so an association always names a live ACL.

## UpdateWebACL merges, so a member can never be cleared

`UpdateWebACL` and `UpdateIPSet` copy each supplied member over the stored record and leave the others
alone, where AWS's update operations replace the whole configuration — their inputs mark
`DefaultAction`, `VisibilityConfig` and `Addresses` `Required: Yes` precisely because the call is a
replacement. A `Description` set once cannot be removed, and a rule list cannot be emptied.

Both operations do enforce the `LockToken`, which is the part that matters for a consumer's
optimistic-concurrency handling: a stale token is refused with `WAFOptimisticLockException` and a new
token is minted on every successful write.

## What a refusal reports

| Condition | Code | Status |
|-----------|------|--------|
| a body that will not parse | `WAFInvalidParameterException` | 400 |
| an invalid `Scope` | `WAFInvalidParameterException` | 400 |
| a Web ACL or IP set that does not exist | `WAFNonexistentItemException` | 400 |
| a `LockToken` that does not match | `WAFOptimisticLockException` | 400 |

Those three answered 404 until
[#1098](https://github.com/scttfrdmn/substrate/issues/1098). Every WAFv2 page that lists
`WAFNonexistentItemException` publishes it at 400 — `GetIPSet`'s Errors section gives the gloss, *"AWS
WAF couldn't perform the operation because your resource doesn't exist. If you've just created a
resource that you're using in this operation, you might just need to wait a few minutes."* — and no
WAFv2 page publishes a 404 at all, so a consumer branching on the status rather than on the code saw a
shape AWS never sends. The messages are substrate's own, naming which Web ACL or IP set was not found,
because the published gloss names neither.

The one 404 that remains is not WAFv2's: an operation the plugin does not route answers
`UnknownOperationException`, which the JSON protocol's own Common Errors page publishes at 404.

`WAFDuplicateItemException`, `WAFLimitsExceededException`, `WAFInvalidResourceException`,
`WAFUnavailableEntityException` and `WAFInternalErrorException` are published and have no site:
Substrate enforces no capacity ceiling, validates no rule statement, and checks no association target.

## The account and Region a record carries reach no response

`WAFv2WebACL` and `WAFv2IPSet` declare `AccountID` and `Region` under wire-visible `json` tags,
because the record is what `MemoryStateManager` snapshots and a replay reads back. The web ACL also
declares `CreatedAt`. Until [#756](https://github.com/scttfrdmn/substrate/issues/756), `GetWebACL`
and `GetIPSet` answered each record whole. That put these members on the wire, together with `Scope`
and `LockToken` inside the object:

- No WAFv2 shape publishes the bookkeeping members.
- `API_WebACL` and `API_IPSet` publish neither `Scope` nor `LockToken`. `Scope` is a request
  parameter. `LockToken` is published once, at the top level of each get's response, where it is
  still answered, so the response used to carry the token twice.

Both gets now answer through `wafv2WebACLOut` and `wafv2IPSetOut` (`emulator/wafv2_wire.go`). The
stored record is unchanged, so a run recorded before the fix replays identically.

All thirteen routed operations are driven by `TestWAFv2Wire_WebACLResponsesCarryNoBookkeepingMember`
and `TestWAFv2Wire_IPSetResponsesCarryNoBookkeepingMember` in `emulator/wafv2_wire_test.go`. They walk
each decoded document and fail on a bookkeeping member at any depth. They also require that the
object carries no `Scope` or `LockToken` while the top-level `LockToken` survives.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::WAFv2::WebACL | `name\|id\|scope` | `Ref` is the composite AWS publishes, with the scope spelled as the template spells it (`REGIONAL`/`CLOUDFRONT`) rather than lowercased as the ARN segment is. The ARN comes from `wafv2ARN`, the same builder the plugin uses, so one logical Web ACL cannot report two different ARNs depending on which path created it. A stub otherwise: the Web ACL is invisible to `GetWebACL` and `ListWebACLs` |

## Cost

`CreateWebACL` is attributed $5.00 per call — WAF's real charge is $5.00 per Web ACL per *month*, taken
here once at creation — and `AssociateWebACL` $0.000001. Per-request WAF charges ($0.60 per million)
are not modelled, because Substrate sees no traffic through a Web ACL.
