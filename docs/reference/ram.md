# RAM

**Endpoint:** `ram.{region}.amazonaws.com`

**Protocol:** REST-JSON over lowercase `POST` paths — `POST /createresourceshare` rather than an
`X-Amz-Target` header. API version 2018-01-04.

**Routing:** the path is lowercased before matching, so a mixed-case path still routes; a path that
matches no operation falls through to the bare HTTP method, which matches nothing and is refused as
an unknown route. `DeleteResourceShare` is routed on `DELETE`, which is what the page publishes, and
on `POST` as well.

Eight operations over one resource: a resource share, and the principals and resources associated
with it. Nothing is actually shared — an association is a record, not access — so a principal that
RAM reports as `ASSOCIATED` gains no permission on the resource anywhere else in the emulator.

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateResourceShare | Answers HTTP 200 with `{"clientToken", "resourceShare"}`; `name` is the one `Required: Yes` member and is checked. [`clientToken` is honored and echoed](#clienttoken-is-honored-on-create-and-delete), and minted when the request omits it |
| GetResourceShares | [`resourceOwner` is required and read](#resourceowner-is-required-and-read); filters by `name`, `resourceShareArns`, `resourceShareStatus` and `tagFilters`. [Pages on `maxResults`/`nextToken`](#the-three-listings-page). `permissionArn` and `permissionVersion` are accepted and ignored: substrate attaches no managed permission to a share |
| UpdateResourceShare | Replaces `name` and `allowExternalPrincipals` and refreshes `lastUpdatedTime`; `clientToken` is not read. A `DELETED` share is `OperationNotPermittedException` |
| DeleteResourceShare | [A soft delete](#a-delete-marks-the-share-deleted), answering `{"clientToken", "returnValue": true}`; a retry under the same `clientToken` [succeeds again](#clienttoken-is-honored-on-create-and-delete) |
| AssociateResourceShare | Records each principal and resource ARN and answers `resourceShareAssociations` of five members, each `ASSOCIATED`. `clientToken` and `sources` are not read. A `DELETED` share is `InvalidStateTransitionException` |
| DisassociateResourceShare | [Reports `DISASSOCIATED`](#a-disassociation-reports-disassociated) |
| ListPrincipals | `principals` of `id` and `resourceShareArn`, where `Principal` publishes five members and no `status`. [`resourceOwner` is required and read](#resourceowner-is-required-and-read), and `principals`, `resourceArn`, `resourceShareArns` and `resourceType` filter. [Pages on `maxResults`/`nextToken`](#the-three-listings-page) |
| ListResources | `resources` of `arn`, `resourceShareArn` and `status`, all `AVAILABLE`, where `Resource` publishes eight members. [`resourceOwner` is required and read](#resourceowner-is-required-and-read), and `principal`, `resourceArns`, `resourceShareArns`, `resourceType` and `resourceRegionScope` filter. [Pages on `maxResults`/`nextToken`](#the-three-listings-page) |

The invitation surface is unrouted — `GetResourceShareInvitations`,
`AcceptResourceShareInvitation`, `RejectResourceShareInvitation` — as is the permission surface
(`ListPermissions`, `GetPermission`, `AssociateResourceSharePermission`,
`ListResourceSharePermissions`), `GetResourceShareAssociations`, `ListResourceTypes`,
`GetResourcePolicies`, `EnableSharingWithAwsOrganization` and the three tag operations. A share's
`tags` are stored but RAM is not scanned by the Resource Groups Tagging API, so they cannot be
searched for or read back except inside a share.

## The resource share's published shape

`API_ResourceShare` publishes exactly eleven members. Substrate answers eight of them —
`allowExternalPrincipals`, `creationTime`, `lastUpdatedTime`, `name`, `owningAccountId`,
`resourceShareArn`, `status`, `tags` — and omits `featureSet`, `resourceShareConfiguration` and
`statusMessage`. The share's associations, which the record keeps because `ListPrincipals` and
`ListResources` answer from them, are not on the wire: RAM reports them through those operations,
not on the share ([#1170](https://github.com/scttfrdmn/substrate/issues/1170)). Nor are the record's
`accountID` and `region` ([#756](https://github.com/scttfrdmn/substrate/issues/756)).

## clientToken is honored on create and delete

[#1170](https://github.com/scttfrdmn/substrate/issues/1170). Both pages publish `clientToken` as the
idempotency key — "If you retry the operation with the same `ClientToken`, but with different
parameters, the retry fails with an `IdempotentParameterMismatch` error" — and both responses
publish it. Substrate read it on neither and echoed it on neither, so a retried create made a second
share. Now:

- A create under a token that already made a share, with the same parameters, answers that share and
  the token and creates nothing. The parameters are compared as the decoded request body less
  `clientToken`, so key order and whitespace do not matter but every member does, including
  `permissionArns`, `sources` and `resourceShareConfiguration`, which substrate does not otherwise
  model. Spelling out a default the first request omitted is a different request.
- The same token with different parameters is `IdempotentParameterMismatchException`/400.
- A create token does not match a `DELETED` share: the retry creates a new share rather than
  answering a deleted one as the outcome of a create.
- A delete retried under the token it was made with answers `returnValue: true` again, rather than
  the `InvalidStateTransitionException` a second delete answers without it; the same delete token on
  a different share is `IdempotentParameterMismatchException`. Once the deleted share's two-hour
  window passes, the retry finds nothing and answers `UnknownResourceException`.
- A request without a token is answered with one substrate generates, in UUID form, from the
  request's ID — so a replay reproduces it — and stored like a caller's, so retrying with it is
  idempotent too.

The token lives on the share record. One index entry per token names the share's ARN, so finding it
is one read rather than a scan of every share. The share an entry names must still carry the token,
and pruning an expired share removes the entries that still name it. Every share writer — create,
delete, update, associate, disassociate — holds one lock per account and Region across its load,
check and write. So concurrent creates under one token make one share, and an associate racing a
delete cannot write the share back `ACTIVE`. `InvalidClientTokenException` is published and has no
site, because neither page publishes a length or a pattern for the token.

## The three listings page

[#1542](https://github.com/scttfrdmn/substrate/issues/1542). `GetResourceShares`, `ListPrincipals` and
`ListResources` read `maxResults` and `nextToken`, and a truncated page carries a `nextToken`; the
last page carries none.

- `maxResults` outside the published 1–500 is `InvalidParameterException`/400. An absent one answers
  the whole listing on one page: the pages give the default only as "a value that is specific to the
  operation", so any number would be invented.
- A `nextToken` substrate did not issue is `InvalidNextTokenException`/400, refused before any state
  is read.
- `tagFilters` filters `GetResourceShares`: a share matches when any filter matches one of its tags
  ("Multiple tag filters are evaluated as an OR condition"), and a filter with no `tagValues` matches
  any value of its key. A filter with no `tagKey` is `InvalidParameterException`/400.

Every token names the operation that issued it, so a token from one listing is refused by the other
two ("a value you received in the NextToken response of a previous call to this operation"). The
position inside it takes one of two forms:

- **`GetResourceShares`** lists shares in ARN order, so its position is a value cursor, as the RDS and
  ElastiCache describes use: the ARN of the page's last share, and the next page starts strictly after
  it. That matters because this listing shrinks on its own. A `DELETED` share is listed for its
  two-hour window and then removed, and a walk that spans the removal resumes after the expired ARN
  rather than skipping the share that moved into its position. A position is refused unless it names a
  resource share in the caller's own account and Region; a share that has since expired still resumes.
- **`ListPrincipals` and `ListResources`** ([#1566](https://github.com/scttfrdmn/substrate/issues/1566))
  list the principals or resources of every live share in **share-ARN order, then principal ID or
  resource ARN order**, and that pair is the position: the next page starts strictly after the pair
  the token names. A delete, associate or disassociate between two pages therefore neither skips nor
  repeats an item that survives it, and a token naming an item that has since gone resumes where it
  was. A principal or resource associated with several shares is listed once, under the first share in
  that order. The token is the shared [cursor token](/reference/concepts/listing-order#cursor-pagination),
  bound to the operation and to the request's filter members: a token reused with different filters,
  `GetResourceShares`' token, or one from the other listing is `InvalidNextTokenException`/400.

## resourceOwner is required and read

[#1171](https://github.com/scttfrdmn/substrate/issues/1171). `resourceOwner` is `Required: Yes` on
`GetResourceShares`, `ListPrincipals` and `ListResources`, with the values `SELF` and `OTHER-ACCOUNTS`.
It was decoded and discarded (the two list operations read no request at all), so every call behaved as
`SELF`. Now an absent `resourceOwner` is `ValidationError`/400 and any other value
`InvalidParameterException`/400. `SELF` answers the caller's own shares. `OTHER-ACCOUNTS` answers an
**empty list**, because nothing in substrate is shared across accounts: a share's principals are
recorded, but no other account's view is derived from them.

The published filters apply. A resource's type is the name RAM publishes on its "Shareable AWS
resources" page, looked up from the ARN's service and resource segment (`transit-gateway/tgw-…` is
`ec2:TransitGateway`), compared case-insensitively and reported as the resource's `type` by
`ListResources`. An ARN the table does not name has no type and matches no `resourceType` filter.
A resource's `resourceRegionScope` is `GLOBAL` when its ARN has no Region and `REGIONAL` otherwise.

A share whose record cannot be read or decoded fails the request; it used to be skipped.

## A disassociation reports DISASSOCIATED

[#1171](https://github.com/scttfrdmn/substrate/issues/1171). `DisassociateResourceShare` answered
through the association's helper, which hard-coded `status: "ASSOCIATED"`, so the response confirming a
disassociation said the principal was still associated. It now reports `DISASSOCIATED`, the settled
state, as an association reports the settled `ASSOCIATED`; the transitional `ASSOCIATING` and
`DISASSOCIATING` the enum also publishes are not modeled.

## A delete marks the share DELETED

[#1171](https://github.com/scttfrdmn/substrate/issues/1171). `DeleteResourceShare` used to delete the
record, so a later `GetResourceShares` reported the share absent. It now sets the share's status to
`DELETED` and `lastUpdatedTime` to the moment of deletion. The RAM User Guide says "The deleted
resource share disappears after two hours. Until then, it remains visible in the console with a deleted
status", and substrate applies that window on the simulated clock:

- For two hours, `GetResourceShares` lists the share as `DELETED` with no `resourceShareStatus` filter
  or with `DELETED`. Its principals and resources are not listed by `ListPrincipals` or
  `ListResources`, since deleting a share ends the sharing.
- In that window, a second delete, an associate or a disassociate is `InvalidStateTransitionException`/400,
  which each of those pages publishes. `UpdateResourceShare` does not publish that code and answers
  `OperationNotPermittedException`/400, which it does.
- After two hours the share is neither listed nor addressable: `UnknownResourceException`, as for a
  share that never existed.
- The first list or lookup that finds an expired share removes its record and index entry, so deleted
  shares do not accumulate. When that happens is not observable: an expired share answers the same
  whether or not it has been removed yet.

## What a refusal reports

| Condition | Code | Status |
|-----------|------|--------|
| a body that will not parse | `ValidationError` | 400 |
| `name`, `resourceShareArn` or `resourceOwner` absent | `ValidationError` | 400 |
| `resourceOwner`, `resourceShareStatus` or `resourceRegionScope` outside its published values | `InvalidParameterException` | 400 |
| a resource share that does not exist, or was deleted more than two hours ago | `UnknownResourceException` | 400 |
| delete, associate or disassociate on a `DELETED` share | `InvalidStateTransitionException` | 400 |
| update on a `DELETED` share | `OperationNotPermittedException` | 400 |
| a `clientToken` reused with different parameters | `IdempotentParameterMismatchException` | 400 |
| `maxResults` outside 1–500, or a `tagFilters` entry with no `tagKey` | `InvalidParameterException` | 400 |
| a `nextToken` substrate did not issue | `InvalidNextTokenException` | 400 |

`ValidationError`/400 is the spelling RAM's consolidated common-errors list publishes, and
`UnknownResourceException`/400 matches its own page.

An absent `name` or `resourceShareArn` used to answer `MissingRequiredParameter`. No RAM page
publishes that code. It is now `ValidationError`, which Common Errors glosses as *"Check that all
required parameters are included"* ([#1169](https://github.com/scttfrdmn/substrate/issues/1169)).
`InvalidParameterException`, which `API_CreateResourceShare` lists, was not chosen. It covers a
parameter that was sent with an invalid value, and an absent member was not sent at all. The one code
covers every share operation, because they all look the share up the same way.

`InvalidClientTokenException`, `MalformedArnException`, `ResourceShareLimitExceededException` and
`ServerInternalException`/500 are published and have no site: no token or ARN is validated for shape,
and no share quota is enforced.

## The account and Region a resource share carries reach no response

`RAMResourceShare` declares its account and Region under wire-visible `json` tags, because the record is what
`MemoryStateManager` snapshots and a replay reads back. Neither reaches a body: `CreateResourceShare`, `UpdateResourceShare` and `GetResourceShares` used to answer the record whole, along with its `principals` and `resourceArns`, which `API_ResourceShare` does not publish. They answer `ramResourceShareToWire` now, with `creationTime` and `lastUpdatedTime` as epoch seconds rather than RFC3339 strings.
`TestRAMWire_ResourceShareResponsesCarryNoBookkeepingMember` in `emulator/ram_wire_test.go` drives all eight routed operations and walks each decoded document for either member at any
depth ([#756](https://github.com/scttfrdmn/substrate/issues/756)).

## CloudFormation resource types

None. AWS publishes `AWS::RAM::ResourceShare` and Substrate does not deploy it, so a share exists
only if `CreateResourceShare` creates it.

## Cost

Nothing is attributed. AWS RAM is free of charge; what a share costs is whatever the shared resources
cost in the accounts that use them, which Substrate does not model.
