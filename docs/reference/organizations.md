# Organizations

**Endpoint:** `organizations.us-east-1.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AWSOrganizationsV20161128.{Op}`)

`Organizations_20161128` was documented here previously and reduces to
`organizations` by the generic version strip; the prefix every SDK actually sends is
`AWSOrganizationsV20161128`, which carries its version inline and needed an explicit
alias (#739).

On the first call, the plugin auto-creates an organization, its management
account, and its root. All three are persisted, so the root's ID and ARN are
stable for the life of the state store — an organization whose root ID moved
between calls could not be governed at all, since nothing could reference the
thing policies attach to.

**Every Organizations exception is HTTP 400.** The API model declares no 404 for
any of them, `AccountNotFoundException` included, so a consumer that branches on
the status rather than the code takes a path AWS never sends it down. The
`InvalidInputException` and `ConstraintViolationException` reasons ride at the
front of the message (`"OU_DEPTH_LIMIT_EXCEEDED: …"`), because the JSON-RPC error
document has no `Reason` member to put them in.

## Supported operations

| Operation | Notes |
|-----------|-------|
| DescribeOrganization | Auto-creates the organization, its management account and its root on first call |
| ListRoots | The same root ID on every call; `PolicyTypes` is empty under `CONSOLIDATED_BILLING` |
| ListAccounts | Reports a vending account immediately, before its status resolves |
| DescribeAccount | `AccountNotFoundException` |
| CreateAccount | **Asynchronous** — returns `IN_PROGRESS` and a `car-` request ID |
| DescribeCreateAccountStatus | Resolves the request on first observation; `CreateAccountStatusNotFoundException` |
| ListCreateAccountStatus | Filterable by `States` |
| MoveAccount | The only way an account leaves the root |
| CloseAccount | **Asynchronous**, no output shape; management-only; the account stays in the organization |
| CreateOrganizationalUnit | `Name` 1–128 characters; accepts inline `Tags` |
| DescribeOrganizationalUnit | `OrganizationalUnitNotFoundException` |
| UpdateOrganizationalUnit | Renames in place — ID, ARN, children and attachments all survive |
| DeleteOrganizationalUnit | `OrganizationalUnitNotEmptyException` while it holds anything; deletes its tags |
| ListOrganizationalUnitsForParent | |
| ListChildren | `ChildType` is required |
| ListParents | Walks up to the root `ListRoots` reports |
| ListAccountsForParent | |
| CreatePolicy | `Content`, `Description`, `Name` and `Type` are **all** required; accepts inline `Tags` |
| UpdatePolicy | Any of `Name`/`Description`/`Content`; `IMMUTABLE_POLICY` on `p-FullAWSAccess` |
| DeletePolicy | `PolicyInUseException` while attached; deletes its tags |
| DescribePolicy | `PolicyNotFoundException` |
| ListPolicies | `Filter` is required; includes `p-FullAWSAccess` |
| AttachPolicy | `DuplicatePolicyAttachmentException`; refused while the type is disabled |
| DetachPolicy | `PolicyNotAttachedException`; the last SCP on a target cannot be detached |
| ListPoliciesForTarget | `Filter` is required |
| ListTargetsForPolicy | Reports the root, OU and account targets of one policy |
| EnablePolicyType | `PolicyTypeAlreadyEnabledException`; restores only `p-FullAWSAccess` |
| DisablePolicyType | Detaches every SCP from every entity in the root |
| PutResourcePolicy | `Content` is required, 1–40,000 characters; `Tags` only on the first call |
| DescribeResourcePolicy | `ResourcePolicyNotFoundException` when none is set — the usual case |
| DeleteResourcePolicy | `ResourcePolicyNotFoundException` when none is set, so a re-run is a refusal |
| TagResource | Roots, OUs, accounts, policies and the resource policy |
| UntagResource | Validates key shape on the removal path too |
| ListTagsForResource | Paginated by `NextToken`; no `MaxResults` |

`CreateOrganization` is not implemented: the organization already exists on first
contact, so there is nothing for it to create.

## The resource policy

An organization holds **exactly one** resource policy — the delegation document
that lets a member account make a read management would otherwise have to make.
That shape is unlike everything else here: there is no list, no per-statement
update, and `PutResourcePolicy` replaces the document wholesale.

The `rp-` ID and its ARN are minted once and **survive every replacement**. A
re-mint would tell a caller holding the ARN that its policy had been replaced by a
different one, when the same single policy was updated — and with only one per
organization, there is nothing a new ID could distinguish it from.

`DescribeResourcePolicy`'s refusal is the **normal** answer, not an edge case: most
organizations have no resource policy. A caller checking whether anything was
delegated to it has to tell `ResourcePolicyNotFoundException` apart from
`AccessDeniedException`, so answering an empty policy would collapse "no
delegation" and "delegation I cannot read" into one observation.

`Tags` apply only to the initial creation, per the API model; a `Put` carrying
tags against an existing policy is refused rather than silently dropping them,
which would leave a tag-gated decision reading a tag set the caller believes it
just wrote. Deleting the policy deletes its tags.

Content must **parse as JSON**, refused as
`InvalidInputException`/`INVALID_RESOURCE_POLICY_JSON`. The evidence is that enum
member in the API model, not the shape's own pattern — which is `[\s\S]*`, any text
at all. Only parseability is checked: the enum's `INVALID_PRINCIPAL`,
`UNSUPPORTED_ACTION_IN_RESOURCE_POLICY` and the two like them are refusals about the
document's *meaning*, and the sets AWS accepts are not in the model, so emitting
them would mean guessing at their boundaries. A guessed refusal is worse than a
missing one: it fails a document AWS would have accepted.

## A member account sees its management account's organization

Organizations state is keyed by the management account, and every account
substrate knows is indexed to the organization it belongs to. So a **member**
account calling any Organizations operation reads management's organization — the
same organization ID, the same root, the same accounts — rather than a private one
of its own. A vended account signing its own requests is a real member, which is
what makes a two-account governance flow testable at all.

An account substrate has never seen still gets an organization auto-created for
it on first contact. That no-setup path is what makes a fresh emulator usable
without a `CreateOrganization` call, and it is only reachable for an account that
is not a known member of anything.

The reads are **not** uniform across the three resource-policy operations, because
AWS's own permissions are not:

| Caller | `DescribeResourcePolicy` | `Put`/`DeleteResourcePolicy` |
|---|---|---|
| The management account | The policy, or `ResourcePolicyNotFoundException` | Allowed |
| A member the policy names | The **same** policy management set | `AccessDeniedException` (403) |
| A member the policy does not name | `AccessDeniedException` (403) | `AccessDeniedException` (403) |

`DescribeOrganization` stays readable by every account in the organization, per
"can be called from any account in the organization"; `DescribeResourcePolicy` is
documented as callable from the management account *or a member account that is a
delegated administrator*, and for Organizations itself that delegation comes from
the resource policy substrate already stores. Making the two uniform would erase a
distinction a governance tool depends on.

`AccessDeniedException` is HTTP **403** here, not the 400 every declared
Organizations exception uses. It is a *common* error the service model does not
declare at all, and the API Reference's Common Errors page gives it 403 — which is
what an SDK's retry classifier reads.

A caller in **no** relationship to the organization is not reachable through this
API: none of the three operations takes an input naming an organization, so there
is nowhere to put another organization's ID. The reachable third case is a member
the policy does not name.

## `p-FullAWSAccess`

AWS attaches the managed allow-everything SCP to the root, every OU and every
account while the SCP type is enabled, and substrate does the same. It is
**synthesized rather than stored**, so it cannot be updated or deleted, and its
ARN is owned by `aws` (`arn:aws:organizations::aws:policy/service_control_policy/p-FullAWSAccess`)
rather than by the organization — which is also why it cannot be tagged, though
reading its tags answers empty.

Without it a fresh organization reports no attached policies, which is wrong, and
the minimum-one-SCP rule below has nothing to hold.

## Account vending is asynchronous

`CreateAccount` returns HTTP 200 with `State: IN_PROGRESS` and a `car-` request
ID, as AWS does. The status resolves — to `SUCCEEDED` with an `AccountId` and a
`CompletedTimestamp`, or to the seeded `FAILED` — on the **first**
`DescribeCreateAccountStatus`, so a waiter converges in one poll with no
wall-clock dependence. `ListAccounts` reports the account immediately, before the
status resolves, matching AWS.

This is advance-on-observation rather than clock-driven on purpose, and #514 has
since settled that question the same way for EC2 instance states — a count of
observations rather than a duration over the simulated clock, because the
simulated clock advances with wall time. See
[Seeding an instance-state progression](/reference/ec2#seeding-an-instance-state-progression).

New accounts land in the **root**. `MoveAccount` is the only way into an OU, and
a move to the account's current parent is `DuplicateAccountException`, not a
no-op — which is what makes a vending script's re-run testable: the second run
hits the refusal rather than silently duplicating.

Organization-wide **email uniqueness is reachable only through the seed**. AWS
enforces it and surfaces a collision asynchronously — `CreateAccount` answers 200
and `DescribeCreateAccountStatus` later reports `FAILED` /
`EMAIL_ALREADY_EXISTS` — and substrate models that shape without inferring the
collision from the accounts it holds. Inferring it would *remove* a path rather
than add one: every fixture that vends two accounts with one email would start
failing without having asked to. The cost is that a consumer wanting the collision
must seed it, which is the trade taken deliberately.

## Closing an account does not remove it

`CloseAccount` has **no output shape** — a success is an empty 200 — and the
closure is read through `DescribeAccount`, which is how AWS documents watching it:
`PENDING_CLOSURE` while the request is in flight, `SUSPENDED` when it completes.
There is no `CLOSED` status; the model's `AccountStatus` enum is exactly those
three values.

The closed account **stays in the organization**. It keeps its place in the
hierarchy, still appears in `ListAccounts` and `ListAccountsForParent`, and keeps
counting against the accounts-per-organization quota — "when an account is closed
it does not stop counting against this quota until it is permanently closed". So a
cleanup path that closes accounts to make room for new ones **gets no room**, and
`CreateAccount` still answers `ACCOUNT_NUMBER_LIMIT_EXCEEDED`. Removing the
account instead would make that broken script look correct, which is the reason
this operation is modelled at all.

The status advances **on observation**, like `DescribeCreateAccountStatus`, with
one difference: the in-flight status is reported on the *first* observation and the
terminal one from the second. `CloseAccount` returns no body, so a poll is the only
place `PENDING_CLOSURE` is ever visible — resolving on the first read would leave a
consumer's in-flight branch unexecutable. Only the operations that put an account's
`Status` on the wire advance it; the concurrency count below deliberately does not,
since counting through an observation would let closing a fourth account converge
the first three.

The operation is **management-only** (`AccessDeniedException`, 403), checked before
any state is read so a member cannot use the other refusals to probe the
organization it belongs to — and a member cannot close itself either, since the
guard is on the caller rather than on the caller's relationship to the target. It
also requires the **ALL** feature set, per "you can close an account when all
features are enabled"; under `CONSOLIDATED_BILLING` the account is left untouched,
so a retry after enabling all features does not start from half-applied state.

A closure already in flight and one already finished are **different** refusals.
The model declares both `ConflictException` and `AccountAlreadyClosedException`
without saying which applies to a `PENDING_CLOSURE` target; substrate reads
"already closed" as the terminal state and answers the conflict for the in-flight
one, so a re-run of a teardown script can tell "this is finishing" from "this was
done".

Of the three published closure quotas, only **3 concurrent closures** is enforced
— it is a count of accounts currently in `PENDING_CLOSURE`, so it is exact.
Observing all three to `SUSPENDED` frees the slots, which is the "as soon as one
finishes, you can close another" half of the quota. The rolling-30-day allowance
(250 or 20% of member accounts, capped at 1,000) and the four-day minimum age
before a created account can be removed are **not** modelled: both are bounded by a
wall-clock window, and substrate's clock is simulated and freely advanced, so such
a refusal would fire or not depending on unrelated `AdvanceTime` calls elsewhere in
a test. A limit a test can skip past is not a limit.

## The disabled-SCP state

An all-features organization whose root has had `DisablePolicyType` called on it
is the state a governance tool is most likely to get wrong, because it looks
nothing like a failure:

- `CreatePolicy` **succeeds**.
- `AttachPolicy` is refused with `PolicyTypeNotEnabledException`.
- `EnablePolicyType` restores **only** `p-FullAWSAccess`. Attachments from before
  the disable are lost, per the User Guide.

That is different from SCPs not being *available* at all, which is what a
`CONSOLIDATED_BILLING` organization has: there no SCP exists, no policy is
visible, and every operation naming one answers with its own documented not-found
code. Only `CreatePolicy` and `EnablePolicyType` name the feature set as the
reason (`PolicyTypeNotAvailableForOrganizationException`), because those are the
two operations whose model error list declares it — emitting it elsewhere would
hand a caller an exception its SDK cannot catch by type.

## Tags reach the authorization decision

Tags written through `TagResource` resolve as `aws:ResourceTag/*` on the tagged
entity, and a request's inline `Tags` as `aws:RequestTag/*`, so a tag-gated
privilege boundary — `CreatePolicy` only when `aws:RequestTag/Owner` matches,
`UpdatePolicy` on that policy only when `aws:ResourceTag/Owner` does — is
actually enforced rather than silently open.

The inline `Tags` of `CreateOrganizationalUnit`, `CreatePolicy` and
`CreateAccount` go through the same validation `TagResource` applies, so a key
that operation refuses cannot be planted through a create instead; an
`aws:`-prefixed one would otherwise be readable as `aws:ResourceTag` by a policy
condition. An invalid tag fails the whole create and leaves nothing behind, and
`CreateAccount`'s refusal is synchronous even though its success is not — the
request is malformed, so there is nothing to vend. Deleting an OU or a policy
deletes its tags, so an entity that reused the ID cannot inherit them.

## A request naming several resources is authorized against every one

`MoveAccount` names three resources — the account, the source parent and the
destination parent — and the Service Authorization Reference marks all three
required. It is the only Organizations operation that does; every other one names
a single resource, and `AttachPolicy`/`DetachPolicy` mark only the policy
required, so those authorize against the policy alone.

The caller's policies must therefore allow the action against all three ARNs. A
policy that names the account and one OU but not the root cannot move that account
out of the root or into it, which is what a delegated-admin confinement policy is
written to guarantee. A permission boundary is applied to every one of the three
as well, since a boundary checked against a subset is not a boundary.

Each ARN is matched against the tags of the resource *it* names, so a condition on
`aws:ResourceTag` written about the destination cannot be satisfied by a tag on
the account. The denial names the first resource the policies do not allow —
resolved in a fixed order of account, source parent, destination parent — which is
the only place the missing ARN surfaces, and the ARN a caller has to add.

## Refusals

| Condition | Answer |
|---|---|
| Unknown account | `AccountNotFoundException` |
| Unknown OU | `OrganizationalUnitNotFoundException` |
| Unknown parent / child / root | `ParentNotFoundException` / `ChildNotFoundException` / `RootNotFoundException` |
| Unknown policy / attachment target | `PolicyNotFoundException` / `TargetNotFoundException` |
| Describing or deleting an unset resource policy | `ResourcePolicyNotFoundException` |
| Resource-policy content outside 1–40,000 characters | `InvalidInputException`/`MIN_LENGTH_EXCEEDED` or `MAX_LENGTH_EXCEEDED` |
| Unparseable resource-policy content | `InvalidInputException`/`INVALID_RESOURCE_POLICY_JSON` |
| `PutResourcePolicy` with `Tags` on an existing policy | `InvalidInputException`, with no reason prefix — the model's enum has no member for it |
| Duplicate OU name under one parent | `DuplicateOrganizationalUnitException` |
| Duplicate policy name | `DuplicatePolicyException` |
| Already attached | `DuplicatePolicyAttachmentException` |
| Detaching something not attached | `PolicyNotAttachedException` |
| Deleting an attached policy | `PolicyInUseException` |
| Deleting a non-empty OU | `OrganizationalUnitNotEmptyException` |
| Enabling an enabled policy type | `PolicyTypeAlreadyEnabledException` |
| Attaching while the type is disabled | `PolicyTypeNotEnabledException` |
| `CreatePolicy`/`EnablePolicyType` under `CONSOLIDATED_BILLING` | `PolicyTypeNotAvailableForOrganizationException` |
| Unparseable policy content | `MalformedPolicyDocumentException` |
| Modifying `p-FullAWSAccess` | `InvalidInputException`/`IMMUTABLE_POLICY` |
| Moving to the current parent | `DuplicateAccountException` |
| Closing the management account | `ConstraintViolationException`/`CANNOT_CLOSE_MANAGEMENT_ACCOUNT` |
| Closing an account already `SUSPENDED` | `AccountAlreadyClosedException` |
| Closing an account already `PENDING_CLOSURE` | `ConflictException` |
| Closing a malformed account ID | `InvalidInputException`/`INVALID_PATTERN` — shape is checked before existence |
| `CloseAccount` under `CONSOLIDATED_BILLING` | `ConstraintViolationException`/`ORGANIZATION_NOT_IN_ALL_FEATURES_MODE` |
| A member account calling `CloseAccount` | `AccessDeniedException`, **403** |
| A member account calling `Put`/`DeleteResourcePolicy` | `AccessDeniedException`, **403** |
| A member the resource policy does not name calling `DescribeResourcePolicy` | `AccessDeniedException`, **403** |
| A source that is not the account's parent | `SourceParentNotFoundException` |
| Unknown move destination | `DestinationParentNotFoundException` |
| A move across roots | `InvalidInputException`/`MOVING_ACCOUNT_BETWEEN_DIFFERENT_ROOTS` |
| A repeated tag key in one request | `InvalidInputException`/`DUPLICATE_TAG_KEY` |
| An `aws:`-prefixed tag key | `InvalidInputException`/`INVALID_SYSTEM_TAGS_PARAMETER` |
| An unreadable pagination token | `InvalidInputException`/`INVALID_NEXT_TOKEN` |
| An unparseable request body | `InvalidInputException` |
| An unimplemented operation | `UnknownOperationException`, **404** — Organizations is JSON; see [An operation substrate does not implement](/reference/concepts/routing#an-operation-substrate-does-not-implement) |

## Quotas

Each is the value in [Quotas for AWS Organizations](https://docs.aws.amazon.com/organizations/latest/userguide/orgs_reference_limits.html),
and each is enforced rather than merely documented.

| Quota | Value | Refusal reason |
|---|---|---|
| Accounts in an organization | 10 | `ACCOUNT_NUMBER_LIMIT_EXCEEDED` |
| OU nesting levels below the root | 5 | `OU_DEPTH_LIMIT_EXCEEDED` |
| OUs in an organization | 2,000 | `OU_NUMBER_LIMIT_EXCEEDED` |
| SCPs in an organization | 10,000 | `POLICY_NUMBER_LIMIT_EXCEEDED` |
| SCPs attached to one root, OU or account | 10 max, **1 min** | `MAX_POLICY_TYPE_ATTACHMENT_LIMIT_EXCEEDED` / `MIN_POLICY_TYPE_ATTACHMENT_LIMIT_EXCEEDED` |
| Characters in an SCP | 10,240 | `POLICY_CONTENT_LIMIT_EXCEEDED` |
| Characters in the resource policy | 40,000 | `MAX_LENGTH_EXCEEDED` |
| Tags on one resource | 50 | `MAX_TAG_LIMIT_EXCEEDED` |
| Member-account closures in progress at once | 3 | `CLOSE_ACCOUNT_REQUESTS_LIMIT_EXCEEDED` |

The 5-per-target and 5,120-character figures often quoted are the **RCP** values,
not the SCP ones.

Two published closure quotas are deliberately **not** enforced — the rolling
30-day allowance (250 or 20% of member accounts, capped at 1,000) and the four-day
minimum age before a created account can be removed. Both require a wall-clock
window, which a freely advanced simulated clock makes meaningless; see
[Closing an account does not remove it](#closing-an-account-does-not-remove-it).

Three of these are also readable through Service Quotas — accounts, OUs and SCPs
per organization — at the same values, so a consumer that reads a ceiling and a
consumer that runs into one get the same number. See
[Service Quotas](/reference/servicequotas).

## Pagination

Paginated listings honor `MaxResults` — clamped to the model's 1–20, so a caller
asking for more gets a truncated page and a token rather than everything — and
`NextToken`. An unreadable token is `InvalidInputException`/`INVALID_NEXT_TOKEN`
rather than a silent restart from the beginning: a paginating caller that restarts
sees duplicates instead of an error, which is the harder failure to notice.

## Seeding

```bash
# An organization in which no service control policy can exist at all.
curl -X POST http://localhost:4566/v1/organizations/feature-set \
  -d '{"featureSet":"CONSOLIDATED_BILLING"}'
curl -X DELETE http://localhost:4566/v1/organizations/feature-set

# The asynchronous outcome of CreateAccount, by account name or "*".
curl -X POST http://localhost:4566/v1/organizations/create-account-failure \
  -d '{"accountName":"dev","failureReason":"EMAIL_ALREADY_EXISTS"}'
curl -X DELETE 'http://localhost:4566/v1/organizations/create-account-failure?accountName=dev'
curl -X DELETE http://localhost:4566/v1/organizations/create-account-failure
```

A name-scoped seed takes precedence over the wildcard. The feature-set seed wins
over the stored value, so an already observed organization can be flipped without
recreating it.

`failureReason` must be a member of the model's `CreateAccountFailureReason`
enum; anything else is a 400. A typo'd reason would seed a `FAILED` status
carrying a value no SDK catch branch matches, so the caller's fallback path would
go untested while the seed appeared to work. The seeded failure is the case worth
testing: `CreateAccount` still returns **200**, and only
`DescribeCreateAccountStatus` reveals `FAILED`, the reason, and the absence of an
`AccountId`.

## Cost

Organizations API calls are free.
