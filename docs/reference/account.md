# Account Management

**Endpoint:** `account.{region}.amazonaws.com`
**Protocol:** REST/JSON (`POST /listRegions`, `/enableRegion`, `/disableRegion`,
`/getRegionOptStatus` — the operation is in the URL; there is no `X-Amz-Target`)

Substrate emulates the **Region opt-in** half of the Account Management API: which
Regions an account may use, and the asynchronous opt in and out of the ones that
are off by default. The other eleven operations in the model — alternate contacts,
the primary contact, the account name and the primary email — are not emulated.

## Supported operations

| Operation | Notes |
|-----------|-------|
| ListRegions | All 34 Regions with their opt status; `RegionOptStatusContains` filters, `MaxResults` 1–50 |
| GetRegionOptStatus | The poll target; reports the Region name back alongside the status |
| EnableRegion | **Asynchronous**, empty 200, no output shape; idempotent against the target state |
| DisableRegion | The same, and refused for a Region that is enabled by default |

## The two Region tables

The 17 **default** Regions — those launched before 2019-03-20 — report
`ENABLED_BY_DEFAULT` and can be neither enabled nor disabled. The 17 **opt-in**
Regions report `DISABLED` until an account enables them.

| | Regions |
|---|---|
| Default (`ENABLED_BY_DEFAULT`) | `ap-northeast-1`, `ap-northeast-2`, `ap-northeast-3`, `ap-south-1`, `ap-southeast-1`, `ap-southeast-2`, `ca-central-1`, `eu-central-1`, `eu-north-1`, `eu-west-1`, `eu-west-2`, `eu-west-3`, `sa-east-1`, `us-east-1`, `us-east-2`, `us-west-1`, `us-west-2` |
| Opt-in (`DISABLED` until enabled) | `af-south-1`, `ap-east-1`, `ap-east-2`, `ap-south-2`, `ap-southeast-3`, `ap-southeast-4`, `ap-southeast-5`, `ap-southeast-6`, `ap-southeast-7`, `ca-west-1`, `eu-central-2`, `eu-south-1`, `eu-south-2`, `il-central-1`, `me-central-1`, `me-south-1`, `mx-central-1` |

Both tables come from the
[Account Management Reference Guide](https://docs.aws.amazon.com/accounts/latest/reference/manage-acct-regions.html)
rather than from the API model, which publishes the `RegionOptStatus` enum but no
Region list.

**`ec2SeededRegions` is a separate, smaller list** and stays that way. EC2's
`DescribeRegions` seeds three Regions and answers a different question — which
Regions EC2 reports, not which ones an account has opted into. Unifying them would
make every EC2 fixture's Region list depend on this table.

## An opt resolves on observation

`EnableRegion` answers an empty 200 and moves the Region to `ENABLING`. The **first**
`GetRegionOptStatus` reports `ENABLING`; the next reports `ENABLED`, and it never
moves again. `DisableRegion` behaves the same way through `DISABLING` to `DISABLED`.
`ListRegions` resolves identically, so a caller polling through the listing and one
polling a single Region cannot contradict each other.

Advancing on observation rather than after an interval of the simulated clock is
what makes the wait testable: enabling a Region takes "a few minutes to several
hours" in AWS, and a waiter here converges in two polls with no dependence on
wall-clock or simulated time. The first observation reports the in-flight status
deliberately — `EnableRegion` has no output shape, so a poll is the only place
`ENABLING` is ever visible, and resolving before the first report would leave a
consumer's in-flight branch unexecutable.

An opt is **not** an observation: a redundant `EnableRegion` neither advances the
record nor rewrites it. Enabling a Region that is already `ENABLED` or `ENABLING`
succeeds silently, which is what makes an "ensure these Regions are on" routine
safe to re-run.

## Refusals

Every 400 is `ValidationException`, the only one these operations declare, with the
reason at the front of the message (`"invalidRegionOptTarget: …"`) — the REST-JSON
error document has no `reason` member to put it in. `ValidationExceptionReason` has
exactly two members, and both are used:

| Case | Code | Reason |
|---|---|---|
| Enabling or disabling a default Region | `ValidationException` (400) | `invalidRegionOptTarget` |
| A Region code in neither table | `ValidationException` (400) | `invalidRegionOptTarget` |
| An `AccountId` that is not a member of the caller's organization, or is the caller itself | `ValidationException` (400) | `invalidRegionOptTarget` / `fieldValidationFailed` |
| `MaxResults` outside 1–50, a bad `RegionOptStatusContains` member, a missing `RegionName`, an unreadable `NextToken` | `ValidationException` (400) | `fieldValidationFailed` |
| The opposite opt is still in flight | `ConflictException` (409) | — |

Disabling a default Region is **`ValidationException`, not
`ConstraintViolationException`** — the `account/2021-02-01` model declares no such
error for any operation, so a consumer catching one would never match.

## Targeting a member account

`AccountId` names a member account of the caller's organization, resolved through
the same member→management index Organizations uses; there is no second copy of it.
The management account cannot specify its own `AccountId` — omit the parameter to
operate in standalone context — and an account outside the caller's organization is
refused. Trusted access and a delegated administrator for Account Management are
not modelled, so the caller must be the management account itself.

## Two limits are deliberately not modelled

AWS documents a limit of **6** region-opt requests in progress per account, and a
per-organization limit that the same guide gives as **50** in one section and **20**
in another. Neither is enforced here. A guessed `TooManyRequestsException` boundary
would refuse requests AWS accepts, and there is no way to pick between two
published numbers without making one of them wrong.

## Seeding

```bash
# Pin what every observation of a Region reports, by Region code or "*".
curl -X POST http://localhost:4566/v1/account/region-opt-status \
  -d '{"regionName":"af-south-1","status":"ENABLING"}'
curl -X DELETE 'http://localhost:4566/v1/account/region-opt-status?regionName=af-south-1'
curl -X DELETE http://localhost:4566/v1/account/region-opt-status
```

A Region-scoped seed takes precedence over the wildcard, and a seeded status does
not resolve — that is the point of it. Because an in-flight opt otherwise advances
on the first observation, the seed is the only way to hold a Region *stuck* in
`ENABLING`, which is what a waiter's timeout branch and the `ConflictException` an
opposite opt gets mid-flight both need.

`status` must be a member of the `RegionOptStatus` enum, and a **default** Region
cannot be seeded: its status is fixed before a seed is ever consulted, so the seed
would be silently ignored and the test using it would pass while asserting nothing.
Both are a 400.

## Cost

Account Management API calls are free.
