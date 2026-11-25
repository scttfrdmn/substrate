# IAM

**Endpoint:** `iam.amazonaws.com`
**Protocol:** AWS Query (form-encoded, `Action=` parameter)

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateUser | Returns User object |
| GetUser | |
| DeleteUser | Refuses with `DeleteConflict`/409 while the user has a login profile, access keys, inline or attached policies, or group memberships, each in AWS's observed wording ("Cannot delete entity, must delete access keys first.") |
| ListUsers | |
| UpdateUser | Renames and re-paths a user, moving everything keyed by its name — see [Renaming a user or group](#renaming-a-user-or-group). `EntityAlreadyExists`/409 for a taken name |
| CreateLoginProfile | Stores `CreateDate` and `PasswordResetRequired`; the password is never stored. `UserName` defaults to the caller |
| GetLoginProfile | `NoSuchEntity`/404 "Login Profile for User … cannot be found." when there is none |
| UpdateLoginProfile | Changes `PasswordResetRequired` when sent; a new password is accepted and not stored |
| DeleteLoginProfile | |
| ListMFADevices | Always empty for an existing user: substrate models no MFA device. `NoSuchEntity` for an absent one |
| ListSSHPublicKeys | Always empty, as above |
| ListSigningCertificates | Always empty, as above |
| ListServiceSpecificCredentials | Always empty, as above |
| ListVirtualMFADevices | Always empty; `AssignmentStatus` is validated |
| CreateRole | Supports trust policy document; the response carries it as submitted, URL-encoded per RFC 3986 — see [Policy documents are served as submitted](#policy-documents-are-served-as-submitted) |
| GetRole | Reports `AssumeRolePolicyDocument` as submitted, URL-encoded per RFC 3986 — see [Policy documents are served as submitted](#policy-documents-are-served-as-submitted) |
| UpdateAssumeRolePolicy | Replaces the trust policy in place, kept as submitted; takes effect on the next `AssumeRole` and does not revoke existing sessions. `PolicyDocument` is required |
| UpdateRole | Changes `Description` and `MaxSessionDuration` (3600 to 43200) in place; only a member the request carries is changed. A service-linked role is `UnmodifiableEntity`/400 |
| DeleteRole | Refuses with `DeleteConflict`/409 while a policy is attached **or** an instance profile holds the role; the message names the profiles. Refuses a service-linked role with `UnmodifiableEntity`/400 — note 400, not the 409 the conflicts use |
| ListRoles | Each role's trust policy as submitted, URL-encoded per RFC 3986 — see [Policy documents are served as submitted](#policy-documents-are-served-as-submitted) |
| CreateServiceLinkedRole | Mints the role under `/aws-service-role/<principal>/` with a trust policy naming the linked service. A duplicate name is `InvalidInput`/400, not `EntityAlreadyExists` (see below) |
| DeleteServiceLinkedRole | Returns a `DeletionTaskId`; an incomplete task for the same role is returned again rather than duplicated. A role that exists but is not service-linked is `NoSuchEntity`/404 |
| GetServiceLinkedRoleDeletionStatus | Reports `SUCCEEDED` unless [seeded](#seeding-a-service-linked-role-deletion-outcome); a `FAILED` task also reports the `Reason` and `RoleUsageList` |
| CreateGroup | |
| GetGroup | Reports the group's actual members |
| DeleteGroup | Refuses with `DeleteConflict`/409 while the group has members or policies |
| ListGroups | |
| AddUserToGroup | Writes both sides of the membership index; idempotent |
| RemoveUserFromGroup | Idempotent — removing a non-member is not an error, per the model |
| ListGroupsForUser | |
| UpdateGroup | Renames and re-paths a group, moving its policies and both sides of its memberships. `EntityAlreadyExists`/409 for a taken name |
| AttachUserPolicy | Refuses a malformed `PolicyArn` with `InvalidInput`; a well-formed ARN that resolves nowhere succeeds and logs at `WARN` (see below) |
| DetachUserPolicy | Refuses a malformed `PolicyArn` with `InvalidInput`, as the attach does; a well-formed ARN that is not attached answers `NoSuchEntity`/404 (see below) |
| ListAttachedUserPolicies | |
| AttachRolePolicy | Same `PolicyArn` check as `AttachUserPolicy` |
| DetachRolePolicy | Same `PolicyArn` check as `DetachUserPolicy` |
| ListAttachedRolePolicies | |
| AttachGroupPolicy | Same `PolicyArn` check as `AttachUserPolicy` |
| DetachGroupPolicy | Same `PolicyArn` check as `DetachUserPolicy` |
| ListAttachedGroupPolicies | |
| PutGroupPolicy | Inline policy |
| GetGroupPolicy | The document as submitted, URL-encoded per RFC 3986 |
| DeleteGroupPolicy | |
| ListGroupPolicies | |
| CreatePolicy | |
| GetPolicy | Resolves a bundled AWS managed policy or a `CreatePolicy` one; metadata only, as on AWS |
| DeletePolicy | `DeleteConflict`/409 while attached, naming the users, groups and roles; `InvalidInput` for a malformed ARN or a bundled AWS managed one — see below |
| ListEntitiesForPolicy | The users, groups and roles a managed policy is attached to, and (unless `PolicyUsageFilter=PermissionsPolicy`) the users and roles using it as a boundary. `EntityFilter` and `PathPrefix` narrow it; one `Marker` pages across all three kinds. `EntityFilter=LocalManagedPolicy`/`AWSManagedPolicy` selecting by the policy's own kind is substrate's reading, since AWS publishes none |
| ListPolicies | Applies `Scope`, `PathPrefix` and `OnlyAttached`, and includes the bundled catalog. `PolicyUsageFilter` is validated and narrows nothing (see below) |
| GetPolicyVersion | Returns any version's document as it was submitted, URL-encoded per RFC 3986. A bundled AWS managed policy has only its default version |
| ListPolicyVersions | Newest first; pages with `MaxItems` and `Marker` |
| CreatePolicyVersion | Up to five versions; a sixth is `LimitExceeded`/409. `SetAsDefault` makes it the evaluated document — see [Policy documents and versions](#policy-documents-and-versions) |
| DeletePolicyVersion | The default version is `DeleteConflict`/409 |
| SetDefaultPolicyVersion | The new default is what the next authorization decision reads |
| SimulatePrincipalPolicy | Evaluates a user's, group's or role's identity policies plus its permissions boundary. See "What the policy simulator evaluates" |
| SimulateCustomPolicy | Evaluates `PolicyInputList` only; resolves no entity, so it declares no `NoSuchEntity` |
| CreateAccessKey | |
| DeleteAccessKey | |
| ListAccessKeys | |
| PutUserPolicy | Inline policy |
| GetUserPolicy | The document as submitted, URL-encoded per RFC 3986 |
| DeleteUserPolicy | |
| ListUserPolicies | |
| PutRolePolicy | Inline policy |
| GetRolePolicy | The document as submitted, URL-encoded per RFC 3986 |
| DeleteRolePolicy | |
| ListRolePolicies | |
| PutUserPermissionsBoundary | ARN shape checked, existence not — see below |
| DeleteUserPermissionsBoundary | |
| PutRolePermissionsBoundary | Refuses a service-linked role with `UnmodifiableEntity` |
| DeleteRolePermissionsBoundary | |
| TagUser | |
| UntagUser | |
| ListUserTags | |
| TagRole | |
| UntagRole | |
| ListRoleTags | |
| TagPolicy | Customer-managed policies only |
| UntagPolicy | Customer-managed policies only |
| ListPolicyTags | Customer-managed policies only |
| CreateInstanceProfile | |
| GetInstanceProfile | |
| DeleteInstanceProfile | |
| AddRoleToInstanceProfile | |
| RemoveRoleFromInstanceProfile | |
| ListInstanceProfilesForRole | The profiles holding the role, paged by `MaxItems`/`Marker`; an absent role is `NoSuchEntity` (#1519) |
| ListInstanceProfiles | Lists the account's profiles, narrowed by `PathPrefix` and paged by `MaxItems`/`Marker`; `PathPrefix` is applied before the page is cut — see below |
| TagInstanceProfile | |
| UntagInstanceProfile | |
| ListInstanceProfileTags | |
| GetAccountAuthorizationDetails | Reports the account's users, groups, roles and managed policies with their inline and attached policies in one response. `Filter` selects populations, `MaxItems` counts across all four — see below |
| CreateOpenIDConnectProvider | `Url` must begin with `https://`; the ARN ends in the URL without it. A second provider for one URL is `EntityAlreadyExists`/409, more than 5 thumbprints or 100 client IDs `LimitExceeded`/409. No thumbprint is fetched — see [Identity providers](#identity-providers) |
| GetOpenIDConnectProvider | Reports `Url` without its scheme, as AWS's sample does |
| ListOpenIDConnectProviders | |
| DeleteOpenIDConnectProvider | A missing provider is `NoSuchEntity`/404: the page declares it and makes no idempotency statement |
| UpdateOpenIDConnectProviderThumbprint | Replaces the list |
| AddClientIDToOpenIDConnectProvider | Idempotent, per the page |
| RemoveClientIDFromOpenIDConnectProvider | Idempotent, per the page |
| TagOpenIDConnectProvider | |
| UntagOpenIDConnectProvider | |
| ListOpenIDConnectProviderTags | |
| CreateSAMLProvider | The metadata document is parsed (XML or base64 XML) for `validUntil` and its `entityID`s, which an assertion's `Issuer` must name; its keys are not read. A document declaring no `entityID` is "Could not parse metadata." |
| GetSAMLProvider | Reports `SAMLProviderUUID`, `ValidUntil` and the private keys' IDs; a key's text is never stored |
| ListSAMLProviders | |
| UpdateSAMLProvider | Metadata, `AssertionEncryptionMode` and one private key added or removed per call, refused in the troubleshooting guide's words |
| DeleteSAMLProvider | |
| TagSAMLProvider | |
| UntagSAMLProvider | |
| ListSAMLProviderTags | |

## Policy documents are served as submitted

IAM stores a policy as the text it was given. Substrate now does too
([#1452](https://github.com/scttfrdmn/substrate/issues/1452)): `CreateRole` and
`UpdateAssumeRolePolicy` keep the trust policy's text, `CreatePolicy` the managed policy's, and
`PutUserPolicy`, `PutGroupPolicy` and `PutRolePolicy` store the inline document as written. Every
read serves that text. Before, substrate kept only the parsed document and re-marshaled it, which
normalized it: a trust principal submitted as `{"AWS":"arn:aws:iam::444455556666:role/X"}` read back
as the bare string `"arn:aws:iam::444455556666:role/X"`, a form IAM accepts only for `"*"`. So the
document `GetRole` answered was not one `UpdateAssumeRolePolicy` would take, and code converging a
trust policy by comparing `GetRole`'s answer with the document it means to write rewrote one real
IAM reports unchanged. Whitespace and list-form members are kept the same way.

The parsed form is still what policy evaluation, the `AssumeRole` trust gate and CloudFormation
drift comparison read, so none of them changed. A record with no stored text (one written before
#1452, a service-linked role, a bundled AWS managed policy) is served from its parsed form.

**Every policy document is URL-encoded, in every operation.** The text is percent-encoded per
RFC 3986 wherever IAM returns a policy document: the `Role` shape's `AssumeRolePolicyDocument` from
`CreateRole`, `GetRole`, `ListRoles`, `CreateServiceLinkedRole` and the roles nested in
`GetInstanceProfile`, `ListInstanceProfiles` and `GetAccountAuthorizationDetails`; the
`Get{User,Group,Role}Policy` documents; and `PolicyVersion.Document`. Only RFC 3986's unreserved
characters are left bare, so a space is `%20`, never `+`.

Six API Reference pages state the encoding (`GetRole`, `GetRolePolicy`, `GetUserPolicy`,
`GetGroupPolicy`, `GetPolicyVersion`, `GetAccountAuthorizationDetails`); the rest say nothing either
way, and every page's sample response renders plain JSON, including the six that state the
encoding. The deciding evidence is botocore: its `json_decode_policies` handler, registered for
every IAM operation, states that "any time an IAM operation returns a policy document it is a
string that is json that has been urlencoded", and URL-decodes every member of the
`policyDocumentType` shape. In the IAM service model that shape covers `Role.AssumeRolePolicyDocument`,
`RoleDetail`, `PolicyDetail`, `PolicyVersion` and each `Get*Policy` response alike. Encoding only the
six annotated operations would hand one role's trust policy back two ways, depending on which call
read it.

botocore and the AWS CLI decode these members automatically. An aws-sdk-go-v2 caller decodes them
with `url.PathUnescape`, not `url.QueryUnescape`, which reads a literal `+` as a space.

## What the policy simulator evaluates

`SimulatePrincipalPolicy` and `SimulateCustomPolicy` answer "may this principal do X to
Y?" without doing X to Y. Both run **the same evaluator the request gate enforces with**,
which is the property worth relying on: a simulated decision that could disagree with an
enforced one would be worse than no simulator, because a consumer would trust the
preflight and then be refused.

The three decisions are reported separately and mean different things:

| `EvalDecision` | Meaning |
|---|---|
| `allowed` | A statement allows the action and nothing denies it |
| `explicitDeny` | A statement denies the action by name |
| `implicitDeny` | Nothing allows it — or a permissions boundary does not permit it |

The distinction matters to an assertion. "The policy explicitly forbids this" and "no
policy grants this" are different findings, and a test asserting the first must not pass
on the second.

**What is in the evaluated set.** For `SimulatePrincipalPolicy`: the entity's attached
managed policies, its inline policies, the managed and inline policies of every group a
user belongs to, plus any `PolicyInputList` documents supplied with the call. The boundary
is `PermissionsBoundaryPolicyInputList` when given, otherwise the entity's stored one. For
`SimulateCustomPolicy`: `PolicyInputList` and `PermissionsBoundaryPolicyInputList` only —
it resolves no entity.

`MatchedStatements` names *which* policy decided, as `SourcePolicyId` (the policy **name**,
or `PolicyInputList.N` for a string input) and `SourcePolicyType`. `MissingContextValues`
reports every condition key a statement tested that the request supplied no value for, so
a conditional grant does not read as a clean refusal. A key present with an empty value is
*set*, not missing — the `Null` operator exists precisely to test for absence.

`CallerArn` defaults to `PolicySourceArn` and populates `aws:PrincipalArn`; `ResourceOwner`
populates `aws:ResourceAccount`; an explicit `ContextEntry` wins over both. `ResourceArns`
defaults to `["*"]`, and every action is evaluated against every resource. `MaxItems`
defaults to 100, valid 1–1000 — **there is no cap on the number of actions per call**.

### What the simulator does not evaluate

Each of these is absent rather than faked, because a fabricated field in a preflight
answer is worse than a missing one:

| Not evaluated | Why |
|---|---|
| **Service control policies** | Organizations stores SCPs and `CheckAccess` never consults them either, so simulating them would report a bound substrate does not enforce. `OrganizationsDecisionDetail` is therefore *absent* from a result rather than present and `false` |
| `StartPosition` / `EndPosition` | They are byte offsets into the policy document *as submitted*. Substrate stores a parsed document, so the original text is gone and any offset would be invented. The AWS sample response shows an empty `<MatchedStatements/>`, so an absent member is a shape callers already handle |
| `EvalDecisionDetails`, `ResourceSpecificResults` | Cross-account constructs. The reference states `EvalDecisionDetails` "is returned, but the response is empty" for a same-account simulation with a resource ARN, which is what substrate models |
| `ResourceHandlingOption`, `PolicyExclusionList` | Accepted and ignored; both select alternate evaluation modes substrate does not model |

A consumer whose preflight depends on an SCP boundary cannot get that answer here, and
should not read an `allowed` as covering it.

## Condition key *names* are matched case-insensitively

AWS: "Context key **names** are not case-sensitive. For example, including the
`aws:SourceIP` context key is equivalent to testing for `AWS:SourceIp`.
Case-sensitivity of context key **values** depends on the condition operator that you
use."

Substrate follows both halves. A policy naming `ec2:createaction`, `AWS:RequestTag/Env`
or `aws:tagkeys` is evaluated identically to its canonical spelling, in `Allow` and
`Deny` position alike — and every operator still compares its *value* exactly as its own
definition says, which is why `StringEquals` and `StringEqualsIgnoreCase` remain
different operators.

The name was previously matched byte-for-byte, which failed in both directions: an
`Allow` written with a differently-cased name was an implicit deny — a false refusal —
and a `Deny` written that way was **inert**, allowing what AWS refuses.

| Folded | Not folded |
|---|---|
| The whole key name, including the tag suffix of `aws:RequestTag/<key>` and `aws:ResourceTag/<key>` | The condition **value**, per the operator's own definition |
| The `aws:` / `ec2:` / `sts:` service prefix | A tag key carried as a value, in `aws:TagKeys` — and substrate's own tag rules, which AWS documents as case-sensitive |
| Names supplied through a simulation's `ContextEntries` | The **operator** and its set qualifier: `stringequals` and `forallvalues:StringEquals` are not operators substrate evaluates, and [deny rather than being read as their canonical spellings](#multivalued-condition-keys-forallvalues-and-foranyvalue) |

**The tag suffix folds too**, which is AWS's rule stated in the same breath as the
key–value form: "Key names are not case-sensitive. This means that if you specify
`"aws:ResourceTag/TagKey1": "Value1"` in the condition element of your policy, then the
condition matches a resource tag key named either `TagKey1` or `tagkey1`, **but not
both**." A tag key compared as a *value* is a different thing and stays case-sensitive.

**When both spellings exist, substrate answers with the first in sorted order.** AWS
names this hazard without resolving it — "you might tag an Amazon EC2 instance with
`ec2=test1` and `EC2=test2` … the key name matches both tags, but only one value matches.
This can result in unexpected condition failures" — so substrate makes the choice
explicit rather than leaving it to Go's randomized map iteration, which a decision that
must replay identically from the event log cannot depend on. An **exact** hit always wins
over a folded one, whatever the folded one sorts as.

Two notes for a caller reading a simulation:

- A key the evaluator resolved by folding case is **not** reported in
  `MissingContextValues`. Reporting it would make the simulation contradict the
  enforcement it exists to predict.
- A key that genuinely is absent is reported **as the policy spelled it**, not
  canonicalized — that string is compared against the document the caller submitted.

Only `ContextEntries` can introduce two spellings of one key on the way in; every
producer inside substrate writes a canonical literal. Two entries whose names differ only
by case are one key, and the later one wins, under the earlier one's spelling. A
`ContextEntry` also overrides a derived `aws:PrincipalArn` or `aws:ResourceAccount`
however it is cased.

## Which condition operators substrate evaluates

Every operator AWS documents, in all five families, plus the `...IfExists` suffix on each
of them.

| Family | Operators | Comparison |
|---|---|---|
| String | `StringEquals`, `StringNotEquals`, `StringEqualsIgnoreCase`, `StringNotEqualsIgnoreCase`, `StringLike`, `StringNotLike` | Exact, case-folded, or globbed with `*` and `?` |
| Numeric | `NumericEquals`, `NumericNotEquals`, `NumericLessThan`, `NumericLessThanEquals`, `NumericGreaterThan`, `NumericGreaterThanEquals` | Both sides read as decimals |
| Date | `DateEquals`, `DateNotEquals`, `DateLessThan`, `DateLessThanEquals`, `DateGreaterThan`, `DateGreaterThanEquals` | W3C ISO 8601 (`2020-01-01T00:00:00Z` down to `2020`) or epoch seconds, on either side |
| Boolean | `Bool` | `true` / `false` |
| Binary | `BinaryEquals` | Base64 text, compared as AWS's own example table does |
| IP | `IpAddress`, `NotIpAddress` | CIDR containment, IPv4 and IPv6 |
| ARN | `ArnEquals`, `ArnLike`, `ArnNotEquals`, `ArnNotLike` | Six components, each globbed separately |
| Existence | `Null` | `true` matches an absent key, `false` a present one |

Three details of the comparisons are worth stating, because they are the ones a policy
written against real IAM depends on:

- **An ARN's six components are compared one at a time**, per AWS: "Each of the six
  colon-delimited components of the ARN is checked separately and each can include
  multi-character match wildcards (`*`) or single-character match wildcards (`?`)." So
  `arn:aws:sns:*:TOPIC-ID` does **not** match
  `arn:aws:sns:us-east-1:123456789012:TOPIC-ID` — a `*` cannot cross a colon. A pattern
  naming fewer than six components has the missing trailing ones filled with `*`.
  `ArnEquals`/`ArnLike` are one operator and `ArnNotEquals`/`ArnNotLike` are its negation,
  which is AWS's rule, not an approximation: "The `ArnEquals` and `ArnLike` condition
  operators behave identically."
- **A bare IP address is a host route** — `/32` for IPv4, as AWS says, and `/128` for
  IPv6, which AWS's IPv4-written sentence does not cover.
- **Unquoted numbers and Booleans are accepted**, per the grammar: "Values are enclosed in
  quotation marks. Quotation marks are optional for numeric and Boolean values." So
  `{"Bool": {"aws:SecureTransport": false}}` and
  `{"NumericLessThanEquals": {"s3:max-keys": 10}}` are read as written, and a number keeps
  the spelling the document used.

**A key the request context does not carry follows the operator's polarity**, which is
AWS's rule: "If the key that you specify in a policy condition is not present in the
request context, the values do not match and the condition is `false`. If the policy
condition requires that the key is **not** matched, such as `StringNotLike` or
`ArnNotLike`, and the right key is not present, the condition is `true`." A key present
with an empty value counts as absent — AWS's own phrase is a value that "resolves to a
null dataset, such as an empty string" — except to `Null`, which is the operator whose
whole job is to see it.

**`...IfExists` on any operator but `Null`.** AWS excludes `Null` explicitly ("any
condition operator name except the `Null` condition"), so `NullIfExists` does not match;
`Null` already answers the question the suffix asks. On a `Deny`, a negated `IfExists`
still fires on an absent key, which is AWS's documented behaviour and the reason the
suffix is safe to write in a guardrail.

**A value substrate cannot parse as the operator's type** is substrate's own choice, since
AWS documents neither side. In the *policy*, such a value is skipped, so one unparseable
element of a list does not decide the answer. In the *request context*, it fails a positive
operator and satisfies a negated one, on the reasoning that a comparison that cannot be
made has not been satisfied. Two consequences: `"2020"` is read as a year rather than as
epoch second 2020, the one place the two date notations collide; and Go's spellings of
infinity, NaN and hexadecimal floats are not numbers, so `"Inf"` cannot satisfy
`NumericGreaterThan` against every number in existence.

**An operator substrate does not recognize is refused when the document is submitted**, with
`MalformedPolicyDocument` and HTTP 400 — AWS's published pair for every one of the four
submitting operations, whose Errors sections read "The request was rejected because the
policy document was malformed. The error message describes the specific error." That last
sentence is what licenses substrate naming the offending operator in the message, which is
the only part of the refusal a caller can act on. The four doors are `CreateRole`,
`UpdateAssumeRolePolicy`, `CreatePolicy` and the three inline-policy operations
(`PutUserPolicy`, `PutRolePolicy`, `PutGroupPolicy`), which share one handler.

Three names are refused for a documented reason rather than for being unknown:
`NullIfExists`, because AWS allows the suffix on "any condition operator name except the
`Null` condition"; `ForAllValues:Null` and `ForAnyValue:Null`, because AWS defines neither.
A set qualifier over any other recognized operator is accepted, including the combinations
AWS's condition-operator page does not tabulate — it lists the String, Bool and ARN forms,
while its set-operator reference describes the qualifiers generally, so refusing
`ForAllValues:NumericLessThan` would be substrate inventing a restriction. Operator names
are matched **case-sensitively**: `stringequals` is refused, because AWS's
case-insensitivity covers condition *key* names, not operator names.

Validation runs on the submission paths only, never when a stored document is read back.
`PolicyDocument` is unmarshalled on every read from state, so validating there would make
an already-stored document — one written by an older substrate, or by a seed — permanently
unloadable. **An operator that is already in state still denies**, which is the evaluator's
own reading and unchanged: it is the only answer that cannot turn a typo into a grant,
including under a set qualifier, where an unrecognized operator over an absent key used to
be [vacuously true](#multivalued-condition-keys-forallvalues-and-foranyvalue) and *granted*
the statement.

Out of scope, and named because the asymmetry is visible: S3's `PutBucketPolicy` performs a
shallower check and answers `MalformedPolicy`, and the SNS and SQS `Policy` attribute is
stored opaquely and parsed by neither.

### Which keys have a producer

An operator is only as useful as the keys it can compare. Substrate populates these, and
nothing else:

| Key | Where it comes from |
|---|---|
| `aws:RequestTag/<key>`, `aws:TagKeys` | The tags the request asks to apply |
| `aws:ResourceTag/<key>` | The tags on each resource the request names |
| `ec2:ResourceTag/<key>` | The same tags, under EC2's service-specific duplicate of the global key — see [both prefixes](/reference/ec2#ec2-reports-a-resource-s-tags-under-both-prefixes) |
| `aws:CurrentTime`, `aws:EpochTime` | The emulator's simulated clock |
| `ec2:CreateAction` | [A tagged create's second authorization pass](/reference/ec2#a-tagged-create-is-authorized-twice) |
| `ec2:Subnet`, `ec2:Vpc` | [A launch's networking resources](/reference/ec2#a-launch-s-networking-resources-carry-ec2-subnet-and-ec2-vpc) |
| `sts:ExternalId` | An `AssumeRole` that supplied one |
| `aws:PrincipalArn` | The caller, at every gate — identity policy, permission boundary, [tag-on-create](/reference/ec2#a-tagged-create-is-authorized-twice), the IAM control plane and a role's trust policy |
| `aws:username` | The caller's IAM user name, at the same gates — **absent** for every principal that has none; see [policy variables](#policy-variables-resolve-from-the-request-context) |
| `aws:userid` | The caller's unique ID, in AWS's per-kind form — an IAM user's `AIDA…`, and `<role-id>:<session-name>` for an assumed role; see [the caller's unique ID and tags](#the-caller-s-unique-id-and-tags) |
| `aws:PrincipalTag/<key>` | One key per tag on the IAM user or role behind the request, read from its record at the same gates |
| `aws:ResourceAccount` | A **simulation** only, from `ResourceOwner` |
| Anything at all | A simulation's `ContextEntries` |

The time pair comes from the emulator's `TimeController`, not the wall clock, so a
recorded request replays to the same decision. An `AuthController` constructed without one
leaves both keys **absent** rather than falling back to `time.Now()`: a date condition then
reports a missing key and denies, which is a false refusal, but a replayable one.

`aws:PrincipalArn` is written at every gate rather than only where a condition on it is
expected, because a request that passes two gates must get one answer from both. It is also
why the key could not stay simulator-only once the negated operators arrived: AWS answers
*true* for a negated operator over an absent key, so
`"ArnNotEquals": {"aws:PrincipalArn": "arn:aws:iam::*:user/carol"}` — an exemption — was
satisfied by every caller. As a `Deny` it fired on carol herself; as an `Allow` it granted
everyone.

What has **no** producer, and therefore never matches on the enforcement path:

- `aws:SecureTransport`, `aws:MultiFactorAuthPresent`,
  `aws:MultiFactorAuthAge`, `aws:SourceIp`, `aws:SourceVpc`, `aws:PrincipalOrgID` and
  every other key not in the table above. The IP family is therefore correct but
  satisfiable only through a simulation's `ContextEntries`. (S3's
  [public-access analysis](/reference/s3#block-public-access) reads an `aws:SourceIp` condition out of a
  *bucket policy* to judge how broad it is, which is a different question from evaluating
  one against a request.)
- `aws:PrincipalAccount`, even though `aws:PrincipalArn`, `aws:username`, `aws:userid` and
  `aws:PrincipalTag/` are all populated beside it. It is the last of the four caller keys
  that needs a derivation substrate cannot make honestly, because a cross-account credential
  makes "the principal's account" ambiguous between the one in the ARN and the one the
  request resolved to — and a key guessed wrong is worse than one a policy can test for with
  `Null`. `aws:username` left this list in
  [#745](https://github.com/scttfrdmn/substrate/issues/745) and the other two in
  [#771](https://github.com/scttfrdmn/substrate/issues/771), all three by the same route,
  which is the reason worth keeping: what made each honest was *recording* the value where
  the credential is minted or *reading* it where the credential is resolved, never parsing it
  back out of an ARN. See [the caller's unique ID and
  tags](#the-caller-s-unique-id-and-tags).
- Four of the seven keys the bundled AWS managed policies condition on, each for its own
  reason rather than as one omission:

  | Key | Blocks | Why it has no producer |
  |---|---|---|
  | `iam:PassedToService` | 8 | No `PassRole` operation exists anywhere in substrate, and the value comes from the *calling* API's service principal, which substrate has no analogue for |
  | `codestar-notifications:NotificationsForResource` | 6 | No CodeStar Notifications plugin |
  | `aws:CalledViaLast` | 2 | `RequestContext` has no call-chain notion, and there is no service-to-service internal path to record one from |
  | `devops-guru:ServiceNames` | 2 | No DevOps Guru plugin |

  Three now have one: `ec2:ResourceTag/<key>` (see [both
  prefixes](/reference/ec2#ec2-reports-a-resource-s-tags-under-both-prefixes)), `iam:AWSServiceName`
  (see [service-linked roles](#service-linked-roles-and-iam-awsservicename)) and
  `elasticloadbalancing:CreateAction` (see [ELB v2](/reference/elasticloadbalancing#authorization)). Between them they
  were the largest group in the table — 11 of the 32 condition blocks turned on
  `iam:AWSServiceName` alone — and each needed the same two things rather than one: a
  producer for the key, *and* a request resource specific enough for the statement's
  `Resource` to match. The ELB key needed a third: the tagging operations it conditions did
  not exist, so the statement in `AmazonECS_FullAccess`'s `ELBTaggingPolicy` conditioned an
  action no request could carry.

  `elasticloadbalancing:CreateAction` was the one whose provenance looked thinnest and turned
  out not to be. It is **absent** from the ELB user guide's own list of ELB-specific condition
  keys, and both Service Authorization Reference pages for ELB render their key tables in
  JavaScript and were unreachable — so the bundled managed policy read like the only citable
  source. It is not: the guide's "Tag your Elastic Load Balancing resources during creation"
  page documents the key, the `AddTags` second authorization it belongs to, and the bare
  operation name as its value.

  Each remaining absence is a false deny in the safe direction: all 32 condition blocks in
  the bundled catalog sit on an `Allow`, so such a statement grants nothing rather than a
  `Deny` going inert.

## The caller's unique ID and tags

`aws:userid` and `aws:PrincipalTag/<key>` are populated
([#771](https://github.com/scttfrdmn/substrate/issues/771)). Both are caller keys, and both
arrive by the route [#745](https://github.com/scttfrdmn/substrate/issues/745) established for
`aws:username`: substrate records or reads the value where it is known, and never parses it
back out of the principal ARN.

`aws:userid` is **recorded when the credential is minted**, in AWS's own per-kind form:

| Caller | `aws:userid` |
|---|---|
| IAM user | the user's `AIDA…`, copied onto the access key by `CreateAccessKey` |
| Assumed role | `<role-id>:<session-name>`, the string `AssumeRole` already returns as `AssumedRoleId` |
| `GetSessionToken` session | the calling user's `AIDA…`, unchanged — the principal is the same user |
| Anything else | **absent** |

Recording rather than deriving is what makes the assumed-role form possible at all: a
session's ARN carries the role's *name*, so `<role-id>` cannot be recovered from the
credential afterwards — and the role it named may since have been deleted and recreated with
a new ID. The account root is the one kind AWS's table documents that substrate has no value
for, because it models no root principal: an unauthenticated caller resolves to a nil
principal, which the gate leaves unenforced and `GetCallerIdentity` reports as `…:root`.

**Absent is not empty.** A credential that resolves to no IAM entity — the documented
`AKIAIOSFODNN7EXAMPLE`, or a record written before this release — publishes no `aws:userid`
key at all, so a policy testing it with `Null` still answers, and nothing matches a guess.

`aws:PrincipalTag/<key>` is **read from the entity's record when the credential is
resolved**, one key per tag on the IAM user or role behind the request. Reading it per
request is the point: `TagUser` and `UntagUser` change tags after an access key exists, and a
snapshot taken at `CreateAccessKey` time would authorize a long-lived key against tags its
principal no longer has — so an `UntagUser` meant to revoke an exemption would have no effect
until the key was rotated. The cost is one extra state read per signed request that resolves
to an IAM entity.

**Session tags are not modelled.** Substrate's `AssumeRole` reads no `Tags` parameter, so an
assumed role's `aws:PrincipalTag/` is the *role's* own tags, where AWS would also publish
whatever the session passed. A deliberate narrowing: what is published is a subset of AWS's,
never a superset, so a statement that matches here matches there.

Both keys reach both authorization doors, because both call the same publisher — the
one-answer-per-request requirement of
[#411](https://github.com/scttfrdmn/substrate/issues/411). And `${aws:PrincipalTag/team}`
resolves as a [policy variable](#policy-variables-resolve-from-the-request-context) with no
further work, since substitution reads any single-valued key from the same context.

Upgrading inverts one thing, and it inverts the same way on AWS: a policy asserting the
*absence* of either key with `Null` stops matching for a caller that now has one.

## What an entity read reports about its tags

An entity's tags were stored and never reported
([#796](https://github.com/scttfrdmn/substrate/issues/796)): `TagRole` wrote them, `ListRoleTags`
read them back, and `GetRole` reported none — so a consumer comparing desired state against the
entity read saw the same tag change on every plan, for ever.

AWS draws the line by **shape**, not by entity, and substrate now draws it the same way. Nine
responses carry a `Tags` member when the entity has tags:

| Shape | Operations |
|---|---|
| `User` | `CreateUser`, `GetUser` |
| `Role` | `CreateRole`, `GetRole`, `CreateServiceLinkedRole` |
| `Policy` | `CreatePolicy`, `GetPolicy` |
| `InstanceProfile` | `CreateInstanceProfile`, `GetInstanceProfile` |

**The list shapes deliberately report no tags.** `ListUsers`, `ListRoles`, `ListPolicies` and
`ListInstanceProfiles` each carry the same note in the API reference, verbatim: *"IAM
resource-listing operations return a subset of the available attributes for the resource. This
operation does not return the following attributes, even though they are an attribute of the
returned object: PermissionsBoundary, RoleLastUsed, Tags. To view all of the information for a
role, see GetRole."* The roles nested inside an instance-profile shape are a list too, and carry
none for the same reason — a tagged role reports its tags through `GetRole` and not through the
profile that holds it.

**An untagged entity omits the member rather than reporting an empty list.** `Tags` is
`Required: No` on all four data types, and every untagged sample response leaves the element
out; `CreateInstanceProfile`'s sample settles the contrast by rendering its *required* empty
list as `<Roles/>` while carrying no `<Tags>` at all. `ListUserTags` and `ListRoleTags` are the
other case — there `Tags` is required, so it is always rendered, empty or not.

**Groups cannot be tagged.** There is no `Tags` member on the `Group` data type, no
`TagGroup`/`UntagGroup`/`ListGroupTags` in the Actions index, no `iam:*Group` tagging action in
the vendored service-authorization snapshot, and the User Guide says it directly: *"You can tag
most IAM resources, but not groups, assumed roles, access reports, or hardware-based MFA
devices."* A `Tags.member.N` sent to `CreateGroup` anyway is ignored rather than stored.

`Tags.member.N` at create time is accepted by all four — `CreatePolicy` and
`CreateInstanceProfile` had nowhere to put one before this release, so a `--tags` on either was
dropped silently. A record written by an earlier version reads back with no tags, which is the
same thing an untagged entity is.

## Which members each entity shape reports

Tags are one member of several that AWS reports per *shape* rather than per entity
([#807](https://github.com/scttfrdmn/substrate/issues/807)). The full picture, with what
substrate renders:

| Member | Single-entity shape | List shape | Notes |
|---|---|---|---|
| `UserId`, `UserName`, `Arn`, `Path`, `CreateDate` | yes | yes | required on `User` |
| `RoleId`, `RoleName`, `Arn`, `Path`, `CreateDate` | yes | yes | required on `Role` |
| `Description` (role) | yes | yes | `Required: No`, omitted when unset |
| `MaxSessionDuration` | yes | yes | omitted when unset |
| `AssumeRolePolicyDocument` | yes | yes | omitted when the role has no trust policy |
| `PasswordLastUsed` | yes | yes | omitted when unset, which is always — see below |
| `PermissionsBoundary` | yes | **no** | excluded from the list shapes by AWS's own note |
| `Tags` | yes | **no** | same note; omitted when the entity has none |
| `PolicyId`, `PolicyName`, `Arn`, `Path`, `AttachmentCount`, `CreateDate` | yes | yes | |
| `DefaultVersionId`, `UpdateDate` | yes | yes | omitted when unset |
| `IsAttachable` | yes | yes | always rendered, `false` included |
| `Description` (policy) | yes | **no** | `Required: No`, omitted when unset |
| `PermissionsBoundaryUsageCount` | yes | yes | computed per read, never stored — see below |
| `RoleLastUsed` | yes | **no** | same note; omitted until the role is assumed — see below |

**`PermissionsBoundary` left the list shapes**, which is a behaviour change: `ListUsers` and
`ListRoles` reported one until this release. AWS's note on both operations excludes it by name,
in the same sentence that excludes `Tags` — so substrate was more generous than the service, and
a consumer could write an assertion against a list response that AWS never satisfies. Read the
entity to see its boundary, which is what the note instructs. The users nested in a `GetGroup`
response and the roles nested in an instance-profile shape are list shapes too, and carry no
boundary for the same reason.

**`Description` on a policy is single-entity-only**, and here AWS says so about the member
directly rather than through the listing note: *"This element is included in the response to the
GetPolicy operation. It is not included in the response to the ListPolicies operation."* Both it
and `IsAttachable` were stored from `CreatePolicy` and never rendered before this release, so a
consumer setting a description could not read it back at all.

**`PasswordLastUsed` is rendered but never populated.** AWS documents it as *"returned only in
the GetUser and ListUsers operations"*, and a null value means the user never signed in with a
password. A login profile (`CreateLoginProfile`) holds a password, but substrate models no console
sign-in, so nothing assigns the field and the member is always omitted in an ordinary run. It is rendered from the record so a
consumer that seeds one directly observes it.

**`PermissionsBoundaryUsageCount` is computed on every policy read, not stored.** A boundary
lives as an ARN on the entity — `IAMUser`/`IAMRole` — and not as a back-reference on the policy,
so `CreatePolicy`, `GetPolicy` and `ListPolicies` each count it by scanning the account's users
and roles once and reading the resulting map per member. That makes a policy read
O(policies + entities) rather than the O(policies × entities) a per-policy scan would cost, and
it is why three things need no code of their own: the count reaches zero when the last boundary
is removed, `DeleteUser`/`DeleteRole` decrement it (the boundary goes with the deleted record),
and a replayed run reports the live run's count, because the count is a function of the state
replay rebuilds rather than a counter accumulated alongside it. A bundled AWS managed policy is
counted the same way, which matters because a fresh emulator's only available boundary ARNs are
bundled ones ([#815](https://github.com/scttfrdmn/substrate/issues/815)).

**No member of these shapes is left unmodelled now.** `PermissionsBoundaryUsageCount` and
`RoleLastUsed` were the last two, each recorded as a design decision rather than a field to
render, and both are answered as of #815 and #816.

**A boundary is reported under AWS's own member names**, which two of them were not until
[#852](https://github.com/scttfrdmn/substrate/issues/852). The `PermissionsBoundary` member of
`User`, `Role`, `UserDetail` and `RoleDetail` is an `AttachedPermissionsBoundary`, and its Contents
section lists exactly `PermissionsBoundaryArn` and `PermissionsBoundaryType`. Substrate rendered
`PolicyArn` and `PolicyName` instead — names that appear on no AWS shape — so an SDK decoded the
element into an empty struct and a consumer reading
`role.PermissionsBoundary.PermissionsBoundaryArn` got `""` for a boundary substrate had stored and
was reporting. `PolicyName` is still kept on the stored record, because
`ListAttachedRolePolicies` renders one and AWS's `AttachedPolicy` shape *has* that member, but a
boundary no longer carries it to the wire: there is no member on the shape to carry it.

`PermissionsBoundaryType` renders **`PermissionsBoundaryPolicy`**. AWS's page contradicts itself
here — the prose says the type "can only have a value of `Policy`" while the same page's
enumeration says "Valid Values: `PermissionsBoundaryPolicy`", and the CLI v2 reference, generated
from the service model, lists only the latter. Substrate follows the model rather than the prose,
and the contradiction is recorded rather than silently resolved so a consumer who read the prose
knows which of the two substrate chose.

**`AttachmentCount` is derived per read, exactly as `PermissionsBoundaryUsageCount` is.** Nothing
writes the field: `CreatePolicy` never set it, the bundled catalog carries no value for it, and an
attach records only the ARN on the entity's own list. `ListPolicies` derived the count and
`GetPolicy` did not, so a policy attached to three entities reported 3 in a listing and **0** when
read on its own ([#847](https://github.com/scttfrdmn/substrate/issues/847)). Both now count the
same way, from the three `<kind>_policies:` prefixes, which is also why an attach and a detach are
immediately visible in both. The count is written onto a **copy** of a bundled policy's record
rather than onto the catalog entry, because the catalog hands back shared pointers: writing through
one would leak a count into every later read of that policy, including reads from a different
emulator in the same process.

## What a role read reports about its last use

`GetRole` reports `RoleLastUsed` — AWS's structure carrying `LastUsedDate` and `Region` — once
the role has been assumed ([#816](https://github.com/scttfrdmn/substrate/issues/816)). It was
neither stored nor rendered before, so a consumer could not tell an assumed role from an
untouched one.

**`AssumeRole` is what writes it**, which makes that operation a writer of IAM state. AWS's
`RoleLastUsed` advances when the role is *used*, and an assume is the only use an emulator that
models no workload can observe, so the stamp is written by the STS operation onto the IAM
record. `AssumeRoleWithWebIdentity` and `AssumeRoleWithSAML` stamp it the same way. The role record was already read to evaluate its trust policy, so the
stamp costs one `Put` and no extra read, and it is written after the session credentials are
stored — a caller the trust policy refuses records no use.

**A projection over the recorded `AssumeRole` events was considered and rejected.** The direct
write replays identically for the reason a projection would: the replay engine re-executes each
recorded request with the simulated clock **frozen at** that event's timestamp and the request's
region taken from the event, so the re-executed `AssumeRole` derives the same date and the same
region and writes the same value. ("Frozen at" rather than "set to" since
[#1217](https://github.com/scttfrdmn/substrate/issues/1217): setting it left the clock advancing
with wall time, so a rendered date was the recorded one only to within the replay's own latency.) What a projection adds is a dependency no plugin has — the IAM plugin
would have to hold the event store and scan it on every `GetRole`, making the answer to a read
depend on the event log rather than on state.

**The date comes from the simulated clock and the region from the request.** AWS documents
`Region` as *"the name of the AWS Region in which the role was last used"*, so it is the
assuming request's region and not the emulator's configured one: assuming the same role from
`eu-west-2` and then `ap-southeast-1` reports each in turn.

**Only `GetRole` and `GetAccountAuthorizationDetails` report it.** The `RoleLastUsed` type says
so directly: *"This data type is returned as a response element in the GetRole and
GetAccountAuthorizationDetails operations."* Substrate answers both since #848, and the second
renders the member in two places — on a `RoleDetail`, and on each role nested inside that
detail's `InstanceProfileList`, which is where AWS's own sample response puts it. `ListRoles`
excludes it by name, in the same sentence that
excludes `PermissionsBoundary` and `Tags`, and the roles nested inside an instance-profile
shape are a list too. `CreateRole` and `CreateServiceLinkedRole` share the single-role wrapper
and report none, because a role created a moment ago has not been assumed.

**Omitting the member for a never-assumed role is substrate's choice, not AWS's.** AWS's page
settles neither half of the question. `LastUsedDate` is documented only as *"This field is null
if the role has not been used within the IAM tracking period"* — a statement about a role
falling out of the trailing 400 days of tracked activity, not about a role never assumed at all
— and it says nothing about `Region` in that case, nor about whether the structure itself is
present or absent. Both members are `Required: No`, so omitting the wrapper is admissible;
substrate omits it because that is the only way a consumer can distinguish "not yet assumed"
from "assumed", where a rendered wrapper holding a zero date and an empty region would report
values AWS never publishes. So do not assert on `RoleLastUsed` being present-but-null: assert on
its absence.

A role record written by an earlier version reads back with no last use, which is the same
thing a never-assumed role is.

## The account-wide authorization snapshot

`GetAccountAuthorizationDetails` reports the account's users, groups, roles and managed
policies in one response, each with its inline policies, its attached managed policies and —
for a user — its group memberships ([#848](https://github.com/scttfrdmn/substrate/issues/848)).
It answered `InvalidAction`/400 before, while the action was already authorizable, so a
consumer's permission to call it did not mean the call worked.

It is the **only** operation that reports all four entity shapes together, which is what makes
it worth having beyond the convenience: `AttachmentCount`,
`PermissionsBoundaryUsageCount` and `RoleLastUsed` are each derived per read, and nothing else
puts a derivation side by side with the operation that reports the same value singly. All three
were defects — #847, #815 and #816 — precisely because no response contained two of them at once.

**The four detail shapes are not the shapes the single-entity reads use.** `UserDetail` has no
`PasswordLastUsed` and `RoleDetail` has neither `Description` nor `MaxSessionDuration`, though
`GetUser` and `GetRole` report all three; `GroupDetail`'s scalars match `Group`'s exactly, and
`ManagedPolicyDetail` carries no `Tags`. The roles nested inside a `RoleDetail`'s
`InstanceProfileList` are `Role`, not `RoleDetail`, so `Description` and `MaxSessionDuration`
*are* admissible there. The wrapper names differ from the sibling listings too:
`AttachedManagedPolicies`, not the `AttachedPolicies` that `ListAttachedRolePolicies` sends.

**An embedded role is re-read from state, never taken from the instance-profile record.**
`AddRoleToInstanceProfile` stores a copy of the role inside the profile, and every later write
to that role — STS's `RoleLastUsed`, `UpdateRole`'s description and session duration,
`UpdateAssumeRolePolicy`'s trust policy — lands on the role's own record and never reaches the
copy. AWS's sample response renders `<RoleLastUsed>` inside `InstanceProfileList → Roles →
member`, so rendering the stored copy would report nothing there for essentially every role that
has been assumed. A role state no longer holds keeps the embedded copy, since that is all there
is to report.

**`MaxItems` counts across the four lists combined, which AWS does not document.** The page says
only *"the maximum number of items"*, and the response carries four lists plus one `Marker`.
Substrate pages one combined, ordered key space — keys composed as `user/<name>`,
`group/<name>`, `role/<name>` and `policy/<arn>` — so a `Marker` is unambiguous across the
populations: a per-list cursor would skip or repeat an entity at every page boundary. Each of the
four list elements is emitted whether or not the page holds a member of it, so a consumer decodes
one shape rather than branching. With 52 bundled managed policies against a default `MaxItems` of
100, **truncation is on the default path** for any account of moderate size — a caller that
ignores `IsTruncated` here sees a partial account, not an edge case.

**Every policy document is URL-encoded per RFC 3986**, as the operation's page states ("Policies
returned by this operation are URL-encoded compliant with RFC 3986"): the trust policy, the inline
documents and `PolicyVersionList[].Document` alike, each the submitted text and byte-identical to
what `GetRole`, `Get{User,Group,Role}Policy` and `GetPolicyVersion` send for it. The page's own
sample response renders them as plain JSON; substrate follows the note, as it does for the singular
operations ([#1452](https://github.com/scttfrdmn/substrate/issues/1452)). Before #1452 only
`PolicyVersionList[].Document` was encoded.

`SAMLProviderList` is a `Filter` value AWS's model accepts, and so does substrate. It selects
none of the four populations, because the response has no member that could carry a SAML
provider: a filter naming only it answers four empty lists. Errors come from
`CommonErrors` — the page itself declares only `ServiceFailure`/500 — so an unknown `Filter`
value is `ValidationError`/400.

## An out-of-range `MaxItems` is refused, not silently rewritten

Every IAM operation that decodes `MaxItems` refuses a value outside 1–1000
([#868](https://github.com/scttfrdmn/substrate/issues/868)). AWS's `maxItemsType` has `Min: 1`
and `Max: 1000`, and the service refuses `MaxItems=0` or `MaxItems=1001` rather than choosing a
value for the caller.

The code is `ValidationError`/400 at nineteen operations and `InvalidInput`/400 at
`SimulatePrincipalPolicy` and `SimulateCustomPolicy`, and that split is AWS's rather than
substrate's: the two simulate operations publish `InvalidInput` in their own Errors sections,
while `ListUsers`, `ListUserTags` and the rest publish only `NoSuchEntity` and `ServiceFailure`
— so for them the code can only come from `CommonErrors`, which does not list `InvalidInput` at
all. Only the code differs; the bounds and the presence rule below are the same everywhere, so
no IAM operation disagrees with another about which values are acceptable.

`ListInstanceProfiles` was the one exception, and it was a gap rather than a decision: it decoded
no request parameters at all, so it ignored `MaxItems`, `Marker` and `PathPrefix` and always
reported `IsTruncated` as `false`. There was no decoded value for the guard to range-check, so
closing the gap meant implementing the pagination the operation had never had, which
[#873](https://github.com/scttfrdmn/substrate/issues/873) did. Every IAM operation that publishes
`MaxItems` now applies the same bounds.

Substrate coerced instead: the shared paginator rewrote anything outside the range to 100, so a
caller who asked for 1001 items got 100 and a caller who asked for 0 got 100 — a page size no
part of the request named. That is the release's theme applied to a request parameter rather than
a response value. The coercion is still the **defaulting** path for an *absent* `MaxItems`, which
is what AWS's documented default of 100 means; it is no longer reachable by a value the caller
actually sent.

A parameter that is present but empty is accepted, and that is a decision rather than an
oversight: a form body carrying `MaxItems=` expressed no limit, so it takes the default. The
refusal is per operation, in the handler, rather than in the paginator, because the paginator
cannot tell an absent parameter from a zero one.

## A `Marker` IAM did not issue is refused, not answered with page one

Every IAM listing that pages on a key — `ListUsers`, `ListRoles`, `ListGroups`, `GetGroup`,
`ListGroupsForUser`, `ListPolicies`, `ListInstanceProfiles`, `ListInstanceProfilesForRole`, the
inline-policy listings, `GetAccountAuthorizationDetails` and the four tag listings — issues as its
`Marker` the base64 of the page's last key, and the next page starts strictly after that key
([cursor pagination](/reference/concepts/listing-order#cursor-pagination)). A key deleted between
pages resumes where it was. A `Marker` that is not canonical base64 of a non-empty key is
`ValidationError`/400, IAM `CommonErrors`' code for input that "doesn't meet the required format or
constraints"; none of these operations publishes a code of its own for it, and the message is
substrate's own. The tag listings' `Marker` takes the same form, so it no longer names the next
page's first tag key in the clear.

`SimulatePrincipalPolicy` and `SimulateCustomPolicy` page their results on a base64 index. The
result list holds one entry per requested action and resource pair, in request order, so an index
names the same pair on every page; `SimulatePrincipalPolicy` reads the principal's policies on each
call, so a policy changed between pages changes the decisions later pages report, not which pairs
they hold. An index that does not decode is
`InvalidInput`/400, the code both pages publish ("The request was rejected because an invalid or
out-of-range value was supplied for an input parameter."). `ListPolicyVersions`' marker is the next
version's ID, and one naming no version is `InvalidInput`/400.

## `ListInstanceProfiles` filters before it pages

`ListInstanceProfiles` applies `PathPrefix` to the whole account and *then* cuts the page
([#873](https://github.com/scttfrdmn/substrate/issues/873)). `ListUsers` and `ListRoles` do it the
other way round — `paginateIAMKeys` slices first and the prefix is checked while the page is
rendered — which under-fills a page whenever the entities outside the prefix outnumber it. Ask for
one profile under `/service-role/` with two profiles under `/` sorting ahead of it and the
filter-after order answers an empty page while `IsTruncated` says there is more; a caller cannot
tell that from an account with nothing under that path. The order here follows `ListPolicies`,
which already gets it right. The `ListUsers`/`ListRoles` order is a separate defect and is not
fixed by #873.

The cost is that every profile in the account is decoded on each call rather than one page's
worth: `PathPrefix` matches on `Path`, which lives inside the record rather than in its state key,
so nothing can be filtered until the record is read. A page that under-fills is wrong in a way a
caller cannot detect; an extra state read is only slower.

`PathPrefix` is **validated**, against the pattern `API_ListInstanceProfiles` publishes —
`\u002F[\u0021-\u007F]*`, a leading slash followed by any character from `!` to DEL —
and against its published length of 1–512. An out-of-range or malformed value answers
`ValidationError`/400. That pattern is deliberately *not* the one `ListPolicies` enforces
(`policyPathType`, which requires a trailing slash as well): the two operations publish different
patterns, and sharing one would refuse `/service-role` here, a prefix IAM accepts. Enforcing the
*leading* slash is worth doing because `service-role/` matches no profile at all, and a silent
empty result is indistinguishable from "nothing is under that path".

An empty `PathPrefix` is taken as absent rather than refused against the published minimum length
of 1, matching the `MaxItems` rule above: a form body carrying `PathPrefix=` expressed no filter,
so it takes the documented default of `/`, which selects every profile.

## An attached policy cannot be deleted

`DeletePolicy` answers `DeleteConflict`/**409** while the policy is attached to any user, group
or role, and the message **names them**. Before #853 it went straight from the `NoSuchEntity`
check to the delete, and what the silent success left behind is worse than the missing code:
nothing writes a back-reference onto a policy, so every entity kept its ARN,
`ListAttachedUserPolicies` still reported it, the authorization evaluator loaded no document for
it — so the entity silently lost the permissions the policy granted, and a test asserting a
*deny* passed for the wrong reason — and `GetPolicy` answered `NoSuchEntity` for the same ARN.

The refusal reads the same three `<kind>_policies:` prefixes, through the same loader, that the
`AttachmentCount` `GetPolicy` and `ListPolicies` report is derived from. That is deliberate: the
count and the refusal must not be able to disagree about what "attached" means, and reading the
same keys is what guarantees it rather than promises it. It also closes a hazard the derived
count made observable — a delete now succeeds only when nothing is attached, so a policy
re-created under a deleted ARN cannot inherit the previous one's attachments.

Naming the entities matters more here than in the sibling refusals. `API_DeletePolicy`'s
description tells the caller to use `ListEntitiesForPolicy` to find what to detach, and
**substrate does not implement that operation**, so the 409's message is the only way a caller
can learn it. The three refusals that already name what to remove — `DeleteUser`'s group
memberships, `DeleteRole`'s instance profiles, `DeleteGroup`'s users — set the wording; the four
that refuse over attached policies name nothing, which is why this message is written rather
than copied.

A malformed `PolicyArn` answers `InvalidInput`/400 through the same shape check the three attach
operations and both `Put*PermissionsBoundary` apply. Previously any non-empty string became a
state key that could never match, so a bare policy name was reported as a policy that does not
exist — the wrong answer to a request that was never well-formed enough to name one.

AWS's other stated precondition, deleting every non-default version first, needs no code here:
it publishes no error code — `DeleteConflict`'s own text is about attached subordinate entities —
and substrate models exactly one version per policy, so there is never a non-default version to
delete.

A **bundled AWS managed ARN** answers `InvalidInput`/400 rather than `NoSuchEntity`, and that is
substrate's reading of a constrained choice rather than a documented code. The handler read state
only, so `arn:aws:iam::aws:policy/PowerUserAccess` was reported as not existing while `GetPolicy`
resolved the same ARN from the catalog and returned the policy — one ARN, two operations,
opposite answers about whether the thing exists. `API_DeletePolicy` does not say what happens to
an AWS managed ARN, and the strongest published statement anywhere is *"You cannot change the
permissions defined in AWS managed policies"* (*Managed policies and inline policies*), which
establishes that the customer does not administer them but names no code and does not mention
deletion. So the code comes from `DeletePolicy`'s own Errors section, and of the five it publishes
`InvalidInput` is the only one describing a rejected input value: `NoSuchEntity` is false here
because the policy is readable, `DeleteConflict` means attached subordinate entities, and
`LimitExceeded` and `ServiceFailure` are unrelated. `UnmodifiableEntity` — which `DeleteRole`
answers for a service-linked role — fits the meaning better but is not published for this
operation, and answering a code AWS does not list would trade one wrong answer for another.

## The tagging operations, and what a listing reports

Twelve tagging operations answer, in four families of three. The user and role families have
existed for some time; the policy and instance-profile families answered `InvalidAction`/400
before this release ([#796](https://github.com/scttfrdmn/substrate/issues/796)), which is the
other half of the same drift — a consumer's tag aspect tags every entity it creates, and only
two of the four could accept one.

| Family | Operations | Identifier |
|---|---|---|
| User | `TagUser`, `UntagUser`, `ListUserTags` | `UserName` |
| Role | `TagRole`, `UntagRole`, `ListRoleTags` | `RoleName` |
| Policy | `TagPolicy`, `UntagPolicy`, `ListPolicyTags` | `PolicyArn` |
| Instance profile | `TagInstanceProfile`, `UntagInstanceProfile`, `ListInstanceProfileTags` | `InstanceProfileName` |

**A listing is sorted by tag key**, on all six — AWS states it on each, and it is not cosmetic:
the response `Marker` encodes a key rather than an offset ([see above](#a-marker-iam-did-not-issue-is-refused-not-answered-with-page-one)), so an unsorted underlying order would
make a second page arbitrary. All six render through one code path, so `ListUserTags` and
`ListRoleTags` now sort too, where they previously reported tags in whatever order they were
stored. `MaxItems` defaults to 100 (1–1000), and `Marker` appears in the response **only** when
`IsTruncated` is `true`. `Tags` is a *required* response member here, so an untagged resource
reports an empty list rather than omitting it — the opposite of the entity shapes above.

**Tagging is a privilege-relevant operation**, not bookkeeping: an `aws:ResourceTag`-conditioned
statement is decided on the resource's tags, so a caller who can retag an instance profile can
move it in or out of the reach of every such statement. All six are authorized at both doors
against the resource they name, which is what the six new
[`iamAuthzOperationResource`](#what-resource-an-iam-request-is-decided-against) rows are for.

**An AWS managed policy cannot be tagged.** AWS documents these operations for an "IAM customer
managed policy", and a managed policy belongs to the `aws` account; substrate's bundled catalog
is read-only for the same reason, so tagging one answers `NoSuchEntity`/404 even though
`GetPolicy` resolves the same ARN.

## What an IAM tag request is refused for

All twelve paths that accept a tag — the four `Tag*`, the four `Untag*` and the four `Create*` —
validate against one rule set ([#806](https://github.com/scttfrdmn/substrate/issues/806)). Before
this release every one of them accepted anything at all: fifty-one tags, an empty key, a key
beginning with the reserved `aws:` prefix, a character outside the set AWS publishes.

| Rule | Source | Answer |
|---|---|---|
| Key 1–128 characters | `Tag` — *"Minimum length of 1. Maximum length of 128"* | `ValidationError`/400 |
| Value 0–256 characters | `Tag` — *"Minimum length of 0. Maximum length of 256"* | `ValidationError`/400 |
| Key matches `[\p{L}\p{Z}\p{N}_.:/=+\-@]+` | `Tag` — Pattern | `ValidationError`/400 |
| Value matches `[\p{L}\p{Z}\p{N}_.:/=+\-@]*` | `Tag` — Pattern | `ValidationError`/400 |
| At most 50 members in one request | `Tags.member.N` / `TagKeys.member.N` — *"Array Members: Maximum number of 50 items"* | `ValidationError`/400 |
| Neither key nor value begins with `aws:` | *Tagging IAM resources* — *"You cannot create a tag key or value that begins with the text `aws:`"* | `InvalidInput`/400 |
| At most 50 tags on a resource, counted after the merge | the same array cap, applied to the resulting set | `LimitExceeded`/409 |

**The character set is Unicode, not ASCII.** The two rules above are AWS's own patterns compiled
verbatim, so `Abteilung=Zürich`, `部門=エンジニアリング` and a value of Arabic-Indic digits are all
accepted — where the ASCII whitelist the User Guide's prose rendering suggests ("letters, numbers,
spaces, and `_ . : / = + - @`") would refuse them. The lengths are counted in **characters** rather
than bytes for the same reason: a 128-rune key of non-Latin letters is legal. The anchors on the
patterns are substrate's — AWS publishes them unanchored, and an unanchored match would accept any
string containing one legal character.

**Which code answers which rule is substrate's mapping**, and it is worth stating plainly because
AWS publishes no per-rule code. A constraint stated **on the shape** answers `ValidationError`/400,
the code the IAM plugin already returns for every other shape violation and the only 400 the four
`Untag*` operations declare at all. A rule stated only **in prose** and inexpressible in the shape —
the reserved prefix — answers `InvalidInput`/400, which every tag-writing and create operation
documents. The over-limit total answers `LimitExceeded`/409, whose documented sentence is *"The
request was rejected because it attempted to create resources beyond the current AWS account
limits."* The *messages* are substrate's own wording throughout; no page publishes message text, and
an SDK dispatches on the code.

**The 50-tag cap is counted over the post-merge set**, the way EC2's is: rewriting the value of a
key an entity already carries adds no key, so it succeeds on an entity already holding fifty tags,
while adding a fifty-first is refused. Unlike EC2, reserved keys are **not** exempt from the count —
EC2's restrictions list states that exemption and no IAM page does, and a caller cannot create such
a key here in any case.

**`aws:` is matched case-sensitively**, so `AWS:billing` is an ordinary caller tag. The prohibition
names "the text `aws:`", the reserved keys the same page lists are all lowercase
(`aws:cloudformation:stack-name`), and folding the comparison would refuse a key real IAM accepts.

**An `Untag*` validates its keys too**, including against the reserved prefix. `TagKeys.member.N`
carries the same length, pattern and array constraints as a tag key, so the same checks apply; the
reserved-prefix check there is a choice rather than a rule, because the prohibition is on *creating*
such a tag — but no IAM entity in substrate can hold one, so a caller naming it is asking to remove
a tag that cannot exist.

**A bad tag on a create leaves no entity behind.** `CreateUser`'s `Tags.member.N` is explicit —
*"If any one of the tags is invalid or if you exceed the allowed maximum number of tags, then the
entire request fails and the resource is not created"* — so validation runs before the write on all
four creates. A create also collapses a duplicate key under its entity type's case rule (below), so
an entity cannot be born holding two keys no later `Tag*` could produce.

**Authorization is decided first.** A caller without the tagging permission on the named resource is
told `AccessDenied` whether its payload is legal or not, rather than being told which of its tags
was malformed on a resource it cannot touch.

**Tag keys are case-sensitive for some entity types and not others**, which is AWS's split rather
than substrate's: *"Tag key values for IAM users and roles are not case sensitive, but case is
preserved. This means that you cannot have separate `Department` and `department` tag keys. […] For
other IAM resource types, tag key values are case sensitive."*

| Entity | Key comparison | `Department=finance` then `department=hr` yields |
|---|---|---|
| User | not case sensitive | `Department=hr` — one tag, stored spelling, new value |
| Role | not case sensitive | `Department=hr` |
| Customer managed policy | case sensitive | both keys |
| Instance profile | case sensitive | both keys |

Case being **preserved** matters on the insensitive side: the surviving key keeps the spelling
already stored, because taking the incoming spelling would silently rename a key an
`aws:ResourceTag` condition might be matching on. The same rule reaches the removal — untagging
`DEPARTMENT` removes a user's `Department` and removes nothing from a policy — because otherwise a
user could hold a tag no spelling a caller can send would delete.

**Two rules are deliberately not enforced.** The `Tag` type marks both `Key` and `Value`
`Required: Yes`, but an omitted `Tags.member.N.Value` arrives on the query wire as the empty string,
which is indistinguishable from an explicitly empty one — and the empty value is documented as legal
(*"You can create a tag with an empty value such as `phoneNumber = `"*), so it is accepted rather
than guessed at. An empty *key* is refused, by the minimum length of 1 and the pattern's `+` alike.

**`ConcurrentModification`/409 and `ServiceFailure`/500 are unreachable by construction.** Both are
declared on all twelve operations and substrate emits neither. A tagging handler's
read-modify-write runs synchronously inside one request against a state manager that serializes its
own access, so no second request can interleave to produce the simultaneous-change condition
`ConcurrentModification` reports. A state failure is returned as a Go error from the plugin and
answered as an internal error, not as an IAM-shaped `ServiceFailure` body, so no code path
constructs one. A consumer testing a retry loop around either code cannot drive it from here.

## Service-linked roles and `iam:AWSServiceName`

The three service-linked-role operations exist principally as the producer for
`iam:AWSServiceName` ([#747](https://github.com/scttfrdmn/substrate/issues/747)). Eleven of
the 32 condition blocks in the bundled managed-policy catalog turn on that key — the largest
single group — and before this release every one of them was unevaluatable, because the
operation carrying the parameter answered `InvalidAction`/400.

Making them evaluate needed two things, not one. The key is published at both authorization
doors: the generic gate reads it off the request, and the handler passes it alongside the
action, because a request that passes two gates must get one answer from both
([#411](https://github.com/scttfrdmn/substrate/issues/411)). And the request **resource** had
to become the role's own ARN — four of those statements scope `Resource` to
`arn:aws:iam::*:role/aws-service-role/…` and two of the four to an exact ARN with no
trailing `*`, none of which a flat `arn:aws:iam::<account>:*` can match. All three operations
get a real ARN, including the status poll, whose `DeletionTaskId` embeds the service and the
role so the resource resolves without reading state — which is what makes it still resolve
after the role is gone, the normal case for the poll that finally reports `SUCCEEDED`.

The key is published for `CreateServiceLinkedRole` and `DeleteServiceLinkedRole` and **not**
for `GetServiceLinkedRoleDeletionStatus`, matching where the Service Authorization Reference
lists it. Publishing a key AWS does not is the permissive direction: it would let a
`StringEquals` succeed here and fail on AWS.

**The role name is substrate's convention, not AWS's rule.** AWS publishes no derivation from
a service principal to a role name, and the IAM User Guide warns against inferring even the
principal — *"Do not try to guess the service principal, because it is case sensitive and the
format can vary across AWS services."* So substrate carries a table of exactly the six
principals a bundled statement names inside a `Resource`:

| Service principal | Role name |
|---|---|
| `lambda.amazonaws.com` | `AWSServiceRoleForLambda` |
| `elasticache.amazonaws.com` | `AWSServiceRoleForElastiCache` |
| `events.amazonaws.com` | `AWSServiceRoleForCloudWatchEvents` |
| `ssm.amazonaws.com` | `AWSServiceRoleForAmazonSSM` |
| `cognito-idp.amazonaws.com` | `AWSServiceRoleForAmazonCognitoIdp` |
| `email.cognito-idp.amazonaws.com` | `AWSServiceRoleForAmazonCognitoIdpEmail` |

Every other principal gets a derived name: the `.amazonaws.com` suffix stripped and each
remaining `.`/`-`/`_` segment title-cased, so `someservice.amazonaws.com` yields
`AWSServiceRoleForSomeservice`. That will differ from AWS for a service whose real name is
not mechanical — `spot.amazonaws.com` is `AWSServiceRoleForEC2Spot` on AWS, which no
transformation of "spot" produces. The table is deliberately **not** padded out with the
other well-known names: a guessed row would be indistinguishable from a cited one, whereas a
derived name is documented as substrate's own. One bundled statement scopes a `Resource`
around an untabled principal — AWSBatchFullAccess pairs `batch.amazonaws.com` with
`arn:aws:iam::*:role/*Batch*` — and the derived `AWSServiceRoleForBatch` satisfies it,
because that pattern is a contains-glob rather than a path.

`CustomSuffix` is joined to the name with `_`, which is observed behaviour: AWS says only
that the suffix "is combined with the service-provided prefix to form the complete role
name". A combined name over 64 characters is refused rather than stored, so
`GetRole` and `DeleteServiceLinkedRole` can always name what the create made.

**A duplicate name is `InvalidInput`/400.** `CreateServiceLinkedRole` publishes exactly four
errors — `InvalidInput` 400, `LimitExceeded` 409, `NoSuchEntity` 404, `ServiceFailure` 500 —
and notably not `EntityAlreadyExists`, which is what `CreateRole` answers. So the refusal
cannot be a copy of `CreateRole`'s; `InvalidInput` is substrate's reading of AWS's "the
request fails with a duplicate role name error" against the four codes the operation actually
publishes.

`DeleteRole` refuses a service-linked role with `UnmodifiableEntity` at HTTP **400**. Without
that guard `DeleteServiceLinkedRole` would be decorative — a caller could delete the role
through the ordinary path and never submit a task.

### Seeding a service-linked-role deletion outcome

Deletion is asynchronous on AWS, and the failure it documents — *"If you submit a deletion
request for a service-linked role whose linked service is still accessing a resource, then
the deletion task fails"* — is unreachable in an emulator that runs no linked service. So
substrate reports `SUCCEEDED` by default and the outcome is seedable, which is the only way a
consumer's poll loop's `FAILED` branch is testable without wall-clock time:

```bash
curl -X POST http://localhost:4566/v1/iam/slr-deletion-status \
  -d '{"roleName":"AWSServiceRoleForLambda","status":"FAILED",
       "reason":"Cannot delete the role because it is still in use.",
       "roleUsageList":[{"Region":"us-east-1",
         "Resources":["arn:aws:lambda:us-east-1:123456789012:function:live"]}]}'

curl -X DELETE 'http://localhost:4566/v1/iam/slr-deletion-status?roleName=AWSServiceRoleForLambda'
```

`roleName` defaults to `"*"`, which applies to every role; an exact name wins over the
wildcard. `status` must be one of AWS's four documented values, so a typo is refused where it
is written rather than reported later as a status no SDK models. Omitting `roleName` on the
`DELETE` clears every seed.

The seed is read at **submission**, not at each poll, because the deletion is conditional on
it: only a `SUCCEEDED` task removes the role record. That is the observable difference the
two outcomes turn on — a caller who polls to `FAILED` and then reads the role must still find
it. A task held at `IN_PROGRESS` or `NOT_STARTED` also makes a resubmission return the same
`DeletionTaskId`, per AWS's "if the deletion task is not yet complete, the `DeletionTaskId` of
the existing task is returned". A `FAILED` task does not block a resubmission, or a caller who
removed the blocking resources could never retry.

## Policy variables resolve from the request context

**A `${…}` in a policy is substituted before the comparison**
([#745](https://github.com/scttfrdmn/substrate/issues/745)), so
`arn:aws:s3:::home/${aws:username}/*` grants alice her own prefix and refuses bob's. Until
that release it was compared as literal text, which matched no ARN at all — a policy of that
shape, the shape AWS's own documentation gives for "a prefix per user", granted nothing.

Substitution runs in exactly the elements AWS names, and nowhere else:

| Element | Substituted? | Rule |
|---|---|---|
| `Resource`, `NotResource` | Yes | AWS names both, in the resource portion of the ARN |
| `Condition` values under `String*` and `Arn*` | Yes | "any condition that involves the string operators or the ARN operators" |
| `Condition` values under `Numeric`, `Date`, `Bool`, `Binary`, `IpAddress`, `Null` | **No** | "You can't use a policy variable with other operators" — the text is compared as written, which for those types is a comparison that cannot be made |
| `Action`, `NotAction`, `Principal`, `Sid` | **No** | Not on AWS's list |

Four rules that decide the edge cases, each taken from AWS's *IAM policy elements: Variables
and tags* page:

- **The `Version` element gates the whole feature.** "Variables were introduced in version
  `2012-10-17`… If you don't include the `Version` element and set it to an appropriate
  version date, variables like `${aws:username}` are treated as literal strings in the
  policy." A document with an older `Version`, or none, is read exactly as it was before this
  release. All 52 bundled AWS managed policies declare `2012-10-17`, so nothing bundled
  changed meaning by accident.
- **Key names are case-insensitive**, so `${aws:userName}` and `${AWS:USERNAME}` resolve the
  same key a producer wrote as `aws:username`.
- **Defaults are honored**: `${aws:PrincipalTag/team, 'company-wide'}` resolves to
  `company-wide` when the tag has no value, so a variable with a default is never unresolved.
- **`${*}`, `${?}` and `${$}` stay literal.** They are AWS's escapes for those characters, so
  substrate tracks per byte whether a `*` was written by the policy author or arrived from a
  substitution. **A wildcard inside a resolved value is text too** — a tag value is data the
  request carried, not pattern the author wrote. AWS documents no rule for that case; reading
  it as text is substrate's choice, and it is the reading that cannot widen a statement on
  behalf of whoever set the tag.

**A variable with no value voids what contains it**, which is one AWS rule stated twice for
the two element kinds:

| Where | AWS's rule | Effect |
|---|---|---|
| `Resource` | "the resource that includes a policy variable with no value will not match any resource" | The entry grants nothing |
| `NotResource` | the same sentence names both elements | The entry excludes nothing — so the statement matches **every** resource |
| Condition value, positive operator | "the value is effectively null. There is no equal or like value" | No match |
| Condition value, negated operator | "Inverted condition operators like `StringNotEquals` or `StringNotLike` do match against a null value" | Match |

This page published the `NotResource` half of that backwards before
[#744](https://github.com/scttfrdmn/substrate/issues/744) corrected it. Substrate reached all
four answers correctly even while comparing literally, because an ARN never contains a
`${…}` — but by coincidence rather than by the rule, which is why the rule is now applied
deliberately: a resolved value can itself contain a wildcard, and coincidence does not
survive that.

`aws:username` is the producer this needed, and it is recorded rather than derived:

| Caller | `aws:username` |
|---|---|
| A long-term access key | The IAM user that holds it |
| `GetSessionToken` | The **same** user — its principal is unchanged, and AWS publishes the key |
| `AssumeRole` | **Absent.** The session name is not a user name |
| A registered credential with no IAM entity behind it | **Absent.** That ARN's last segment is the access key ID |
| A CloudFormation stack's own resource calls | The stack's creator, carried in the stack record so a rollback's deletes are authorized as the create was |
| `SimulateCustomPolicy` / `SimulatePrincipalPolicy` | Derived from `CallerArn` when it names a user, because there the ARN is the caller's own assertion of who to simulate as. It takes the **last** segment, which is the friendly name — the reading every other reader of an entity ARN adopted in [#801](https://github.com/scttfrdmn/substrate/issues/801) |

Substitution reads only the single-valued context, per AWS's "You can use any single-valued
condition key as a variable. You can't use a multivalued condition key as a variable" — so
`${aws:TagKeys}` never resolves.

For one release, substitution alone did not make the bundled `IAMUserChangePassword` grant
anything: its `Resource` resolved correctly to `arn:aws:iam::*:user/alice`, but every IAM
request's resource was built as `arn:aws:iam::<account>:*`, so there was nothing user-shaped to
match. That was a separate gap in how IAM's request resource is derived, closed by
[#770](https://github.com/scttfrdmn/substrate/issues/770) — see
[what resource an IAM request is decided against](#what-resource-an-iam-request-is-decided-against),
below. The bundled policy now grants the caller their own password and nothing else.

## What resource an IAM request is decided against

**An IAM request is authorized against the entity it names**
([#770](https://github.com/scttfrdmn/substrate/issues/770)), so a statement whose `Resource`
is `arn:aws:iam::123456789012:user/alice` grants exactly alice. Until that release every IAM
request was decided against `arn:aws:iam::<account>:*` — a literal `*` in the resource
position, which no statement naming a user, a role or a path can match. The consequence was a
false *deny*: every narrowly scoped IAM statement was inert, including AWS's own managed
policies for letting a user manage their own credentials.

Resolution is one table of 59 operations, keyed by operation name, plus the three
service-linked-role operations that are resolved separately because their resource comes from
a service principal or a deletion-task ID rather than from a name on the wire:

| Resource type | Where the name comes from |
|---|---|
| `user` | `UserName`, or the **caller's own user** when the parameter is absent — `GetUser`, `CreateAccessKey`, `DeleteAccessKey`, `ListAccessKeys` and `ChangePassword`, each because AWS documents that default for that operation |
| `role` | `RoleName` |
| `group` | `GroupName` — including `AddUserToGroup` and `RemoveUserFromGroup`, which publish `group` and only `group` |
| `policy` | `PolicyArn` as it stands, or `PolicyName` for `CreatePolicy` |
| `instance-profile` | `InstanceProfileName` |
| whichever of the three | `PolicySourceArn` for `SimulatePrincipalPolicy`, which is a finished ARN |

Two things about that table are checked rather than claimed. Every row's resource type is one
AWS publishes for that action, and every IAM action AWS publishes *no* resource types for is
**absent** from the table — both read from the vendored *Service Reference Information*
snapshot ([#797](https://github.com/scttfrdmn/substrate/issues/797)), so a row that drifts
from AWS's own data fails the build. The minted ARNs are checked the same way, against AWS's
published format strings.

**An IAM ARN embeds the entity's path**, and only the `Create*` operations carry `Path` on the
wire. So a request naming an entity costs one state read to recover the stored path: a policy
scoped to `arn:aws:iam::123456789012:role/division/engineering/worker` matches
`GetRole(RoleName=worker)` when that is where the role lives, and a policy scoped to
`…:role/worker` does not. On a state miss the request stays on the account path rather than
being decided against an ARN that only looks specific.

An operation the table does not cover is decided against `arn:aws:iam::<account>:*`, which is
also AWS's answer for the operations that name no resource: `ListUsers`, `ListRoles`,
`ListGroups`, `ListPolicies`, `ListInstanceProfiles` and `SimulateCustomPolicy`. A statement
scoped to one user grants nothing on those — the account wildcard is deliberately not a bare
`*`, which would match every statement's `Resource` and quietly satisfy a `Deny`.

**Both authorization doors call one resolver.** The generic `AuthController` gate and the IAM
plugin's own gate see the same request and derive the same ARN, which is what keeps them from
answering one request two ways — the failure behind
[#411](https://github.com/scttfrdmn/substrate/issues/411),
[#714](https://github.com/scttfrdmn/substrate/issues/714) and
[#745](https://github.com/scttfrdmn/substrate/issues/745). The plugin door passed a literal
`"*"` at 48 of its gates before this release, and the six instance-profile operations did not
call it at all: `AddRoleToInstanceProfile` — the classic privilege-escalation step, attaching a more
privileged role to a profile an instance already carries — had no plugin-side gate. All six are
gated now.

**The same rule applies to the caller, whose ARN also embeds their path**
([#801](https://github.com/scttfrdmn/substrate/issues/801)). AWS writes an entity ARN as
`arn:aws:iam::<account>:user/<UserNameWithPath>` — one component in which the friendly name is
the **last** segment — so `…:user/division/engineering/alice` names the user `alice` at
`/division/engineering/`. Substrate read the whole component as the name, and two things
followed from that:

- **A caller at a non-default path was not enforced at all.** Their ARN resolved to an entity
  called `division/engineering/alice`, which exists nowhere, and a principal that resolves to
  nothing is unenforced, because enforcement is opt-in by *existence*. Reachable two ways
  without any misconfiguration: a CloudFormation stack whose `RoleARN` is a service role at
  `/service-role/` — where AWS's console creates one — had *every* resource call allowed, and
  `sts:AssumeRole` on a role at any path answered `NoSuchEntityException` for a valid ARN.
- **A signed IAM-user call reported an ARN that named no entity.** The principal's ARN was
  built from the access key's record without the path, so `aws:PrincipalArn` conditions and
  `GetCallerIdentity` published `…:user/alice` for a user who is not there. That defect masked
  the first one for long-term keys, which is why both moved together.

The caller's ARN now carries the path, and every reader takes the friendly name from its last
segment. One interaction is worth stating because it reads like a regression and is not: **a
pathful caller's own resource ARN carries the path too**, so a statement scoped to
`arn:aws:iam::*:user/${aws:username}` does not match them. That is AWS's behavior, and it is why
AWS's own `IAMUserChangePassword` names a second resource, `arn:aws:iam::*:user/*/${aws:username}`
— which does match, so the bundled policy grants a pathful user their own password exactly as it
grants a default-path one.

**An assumed-role ARN is the one exception**, and it is deliberate:
`arn:aws:sts::<account>:assumed-role/<RoleName>/<RoleSessionName>` has exactly two segments and
**excludes** the role's path, so its role name is the *first* — which is how a session of a role
at `/service-role/` still resolves to that role's policies.

## `aws:ResourceTag/<key>` on an IAM request

**The tags published for an IAM request are the named entity's**
([#804](https://github.com/scttfrdmn/substrate/issues/804)). They ride out of the same read that
recovers the path above, so `aws:ResourceTag/<key>` costs nothing beyond it, and they are attached
to the ARN they belong to rather than merged across the request — a condition about one resource
cannot be satisfied by a tag on another.

The IAM User Guide names four tag condition keys for IAM — `aws:ResourceTag/<key>`,
`aws:RequestTag/<key>`, `aws:PrincipalTag/<key>` and `aws:TagKeys` — and no `iam:`-prefixed
resource-tag key, so unlike EC2 there is no service-specific duplicate to publish under.

| Request | What `aws:ResourceTag/<key>` reports |
|---|---|
| Names a user, role, policy or instance profile | That entity's tags, from its own record |
| Names a service-linked role | The role's tags — an SLR is a role and is tagged like one |
| Names a group | **Nothing.** AWS does not let a caller tag a group |
| Names an entity that does not exist | **Nothing** — there is no record to read tags from |
| Names no resource (the six `List*` and `SimulateCustomPolicy`) | **Nothing.** Their resource is `arn:aws:iam::<account>:*` — every IAM resource in the account, which names no one entity whose tags could describe it |

Two directions were wrong before that release, and the serious one is the second:

- **An `Allow … if aws:ResourceTag/team=platform` granted nothing**, because the key was absent
  from every entity-naming IAM request from the moment
  [#770](https://github.com/scttfrdmn/substrate/issues/770) made that the path they all take.
- **A `Deny … if aws:ResourceTag/env=prod` silently stopped biting.** A guardrail written to fence
  off production entities was inert, and the `Allow` beneath it decided the request — enforced on
  AWS, unenforced here, which is the one direction an emulated privilege boundary must not drift
  in.
- **On the six operations that name no resource, the *caller's* tags were published as the
  resource's**, so a caller tagged `team=platform` satisfied a condition written about a resource
  tagged `team=platform` whatever the resource carried. That arm dated from before either key had
  a producer for IAM; the caller's tags are `aws:PrincipalTag/<key>`, which has had
  [one of its own](#the-caller-s-unique-id-and-tags) since
  [#771](https://github.com/scttfrdmn/substrate/issues/771).

Both doors publish the same tags for the same request, for the same reason they derive the same
ARN: publishing the key at the generic gate alone would have refused, inside the handler, a
request the gate had just allowed.

## Multivalued condition keys: `ForAllValues` and `ForAnyValue`

A condition key is either **single-valued** — at most one value in the request context —
or **multivalued**. AWS's rule is that a multivalued key "requires a condition set
operator", written as a prefix on the operator itself, and that a set operator must *not*
be used on a single-valued key. Substrate mirrors that split: the evaluator carries two
context maps, and only the multivalued one is quantified over.

| In the policy | Substrate's answer |
|---|---|
| `StringEquals` and the other eight operators, unqualified | Compares the single-valued context. Unchanged by anything in this section |
| `ForAllValues:<operator>` | True when **every** value the request carries satisfies the operator — and **true when the key is absent or carries no values** |
| `ForAnyValue:<operator>` | True when **at least one** value satisfies it; **false when the key is absent or carries no values** |
| Either qualifier on an operator substrate does not [recognize](#which-condition-operators-substrate-evaluates) | Does not match, absent key or not |
| Either qualifier on `Null` | Does not match. AWS defines no set-qualified `Null`, and quantifying an existence test over the values whose existence it is testing has no meaning |
| Anything else in that position | Does not match. `ForSomeValues:StringEquals`, or a lowercase `forallvalues:`, denies rather than being read as unqualified |

Both absent-key rules are AWS's, quoted: `ForAllValues` "also returns `true` if there are no
context keys in the request", and for `ForAnyValue`, "for no matching context key or if the
key does not exist, the condition returns `false`".

**Pair `ForAllValues` with `Null`.** The vacuous truth above means a `ForAllValues` `Allow`
permits a request that names nothing at all, which is why AWS's own note says to "always
include the `Null` condition operator in your policy with a `false` value", and why all four
of its `aws:TagKeys` examples do:

```json
"Condition": {
  "ForAllValues:StringEquals": {"aws:TagKeys": ["Department", "CostCenter"]},
  "Null": {"aws:TagKeys": "false"}
}
```

Seen from the `Deny` side the same vacuity bites the other way — a `ForAllValues` `Deny`
fires on the request that carries no keys — so a guardrail is better written with
`ForAnyValue`.

The two absent-key rules are AWS's, but they are also what used to make the qualifiers
dangerous: substrate answered `true` for `ForAllValues:<anything>` over an absent key
**without consulting the operator at all**, so an `Allow` written with an operator it could
not evaluate — or with a misspelled one, which nothing rejects at write time — was granted.
The vacuous truth now applies only to an operator substrate
[actually evaluates](#which-condition-operators-substrate-evaluates); an unrecognized one,
and a qualified `Null`, deny. Treating an unrecognized qualifier or operator as a non-match
is substrate's own choice — real IAM refuses the document instead — and it is the only choice
that cannot turn a typo into a grant. Everything else here is documented behaviour.

### Which keys are multivalued

| Key | Populated from |
|---|---|
| `aws:TagKeys` | The tag keys the request asks to apply, on every service whose tags substrate reads: EC2's `TagSpecification.N.Tag.M.Key` and a direct `CreateTags`/`DeleteTags`' `Tag.N.Key`, IAM's and Organizations' `Tags` lists, Lambda's `Tags` map, Config's `Tags`, and S3's `x-amz-tagging` header |
| Any key a simulation supplies | `ContextEntries.member.N.ContextKeyValues.member.M` — every value, not just the first |

`aws:TagKeys` is derived from the `aws:RequestTag/<key>` entries substrate already read out
of the request rather than gathered separately, so the two cannot disagree about what the
request asked for, and its value is **sorted** — a `ForAllValues` denial that depended on Go
map iteration order could not replay from the event log.

Everything else substrate populates is single-valued: `aws:RequestTag/<key>` and
`aws:ResourceTag/<key>` on an authorized request, `aws:CurrentTime` and `aws:EpochTime`
from the simulated clock, `ec2:CreateAction` on [a tagged create's
second authorization pass](/reference/ec2#a-tagged-create-is-authorized-twice),
[`ec2:Subnet` and `ec2:Vpc`](/reference/ec2#a-launch-s-networking-resources-carry-ec2-subnet-and-ec2-vpc) on a
launch's networking resources, `sts:ExternalId` when a role
is assumed, `aws:PrincipalArn` at every gate, and `aws:ResourceAccount` in a simulation. A set qualifier on one of
those is evaluated over a one-element set — which is the thing AWS warns against writing,
not something substrate refuses. Over one that is **absent** it is vacuously true, which is
the other half of the same warning.

**One authorization path populates neither map**: `iam:` actions decided inside the IAM
plugin. Nothing there reads the request for tags, so a condition on `aws:RequestTag` or
`aws:TagKeys` cannot be satisfied through that door — worth knowing before writing one
against an IAM action.

## AWS managed policies are a seeded catalog

Substrate bundles **52** AWS managed policies, not the ~1,200 AWS publishes. Each carries
its real ARN, policy ID, path and default version, and a policy document copied verbatim
from its page in the [AWS managed policy
reference](https://docs.aws.amazon.com/aws-managed-policy/latest/reference/). `GetPolicy`
resolves a bundled ARN exactly as it resolves one from `CreatePolicy`.

The catalog covers two populations:

| Population | Examples |
|---|---|
| Human-operator policies (47) | `AdministratorAccess`, `PowerUserAccess`, `ReadOnlyAccess`, and per-service `…FullAccess` / `…ReadOnlyAccess` pairs |
| Service-role policies (5) | `AmazonSSMManagedInstanceCore`, `AmazonEC2ContainerRegistryReadOnly`, `service-role/AmazonECSTaskExecutionRolePolicy`, `service-role/AWSLambdaBasicExecutionRole`, `service-role/AWSLambdaVPCAccessExecutionRole` |

The distinction matters because they are attached by different callers. A human-operator
policy is attached to a user or group; a service-role policy is what an **instance profile
or execution role** carries, and those are the ones IaC provisions. Substrate bundled
`AmazonSSMFullAccess` — the operator policy — but not `AmazonSSMManagedInstanceCore`, which
is the policy an SSM-managed instance actually needs.

A policy under a path reports the path in `Path` and keeps it out of `PolicyName`:
`service-role/AWSLambdaBasicExecutionRole` has `Path: /service-role/` and `PolicyName:
AWSLambdaBasicExecutionRole`, matching AWS. The full ARN includes the path component.

### Finding a policy: `ListPolicies` scope

`Scope=AWS` means substrate's **52-policy catalog**, not the ~1,200 AWS publishes.
`Scope=Local` is the policies created through `CreatePolicy`; `All`, the default, is both.
`PathPrefix` matches against the `Path` field and must begin *and* end with a slash, as
IAM requires — `/service-role` is refused, `/service-role/` returns the three policies AWS
publishes under that path (`AWSLambdaBasicExecutionRole`,
`AWSLambdaVPCAccessExecutionRole`, `AmazonECSTaskExecutionRolePolicy`). The other two
service-role policies substrate bundles, `AmazonSSMManagedInstanceCore` and
`AmazonEC2ContainerRegistryReadOnly`, live at `/` because that is their real path — what a
policy is *for* and where it lives are different things, and the path reported here is
AWS's. `OnlyAttached` is computed from stored attachments, so a bundled policy that has
been attached to a user, group or role does appear; the catalog's own `AttachmentCount` field is
always 0 and is read by nothing — every shape that reports a count derives it, `GetPolicy`
included as of #847.

`PolicyUsageFilter` is **validated and applies no narrowing.** The reference does not say
which side of `PermissionsPolicy`/`PermissionsBoundary` an entirely-unused policy falls on,
and in a fresh substrate every bundled policy is unused — so guessing would silently drop
all 52 from a filtered listing, which is the same failure the unfiltered listing used to
have. An invalid value is still refused with `ValidationError`.

### Attaching a policy substrate does not bundle

`AttachUserPolicy`, `AttachRolePolicy` and `AttachGroupPolicy` check the **shape** of
`PolicyArn`, not that it resolves. A value that is not a well-formed policy ARN — a bare
policy name, a bucket ARN, `role/` where `policy/` belongs, an account that is not twelve
digits — is refused with `InvalidInput` (400).

A well-formed ARN that resolves in neither the catalog nor state **succeeds**, and logs at
`WARN` naming it. Requiring existence would refuse every attach of the ~1,150 unbundled
managed policies: attaching `AmazonAthenaFullAccess` would hard-fail where AWS succeeds.
That trades a confusing success for a wrong failure, and the wrong failure breaks working
consumer code rather than merely failing to catch a typo. The warning distinguishes an
unbundled AWS managed policy (expected) from a customer-managed ARN no `CreatePolicy` ever
created (likelier a real mistake).

The consequence to know: an attached-but-unresolvable policy contributes **no statements**
to any authorization decision, so `CheckAccess` and the simulator both behave as though it
were not attached. A consumer who needs the attach verified should follow it with
`GetPolicy`, which is exact.

**A permissions boundary is treated the same way, and the consequence is the reverse one.**
`PutUserPermissionsBoundary` and `PutRolePermissionsBoundary` check the boundary ARN's shape and
refuse a malformed one with `InvalidInput` (400) — the code both operations publish — while a
well-formed ARN naming a policy substrate cannot resolve **succeeds** and logs at `WARN`, for
#499's reason above: AWS says a boundary may be "an AWS managed policy or a customer managed
policy", so the unbundled ~1,150 are the common case here too, and neither operation's page states
what a nonexistent boundary policy produces. The warning is its own line rather than the attach
path's, because an unresolvable *attached* policy grants nothing while an unresolvable *boundary*
**restricts** nothing: the boundary is stored and reported, but the evaluator loads no document for
it, and no document is indistinguishable from no boundary — so the entity keeps every permission
its attached policies grant, where AWS would have clamped them
([#846](https://github.com/scttfrdmn/substrate/issues/846)).

Two further notes on that pair. `PutRolePermissionsBoundary` refuses a **service-linked role** with
`UnmodifiableEntity` (400) — its own prose says "You cannot set the boundary for a service-linked
role", and both the sentence and the code are absent from `PutUserPermissionsBoundary`, which is
AWS confirming the asymmetry rather than substrate inferring it from "there is no service-linked
user". And `PolicyNotAttachable`, which both pages publish for an AWS service-role policy attached
to anything but its own service-linked role, is deliberately **not** modelled: substrate does not
record which managed policies are service-role policies, so it cannot tell the case apart, and
refusing on a guess would be a refusal AWS's own description does not cover. A missing required
member is `ValidationError` (400), which is on IAM's `CommonErrors` page rather than either
operation's list, and is what every other IAM operation answers for one.

### Detaching a policy

`DetachUserPolicy`, `DetachRolePolicy` and `DetachGroupPolicy` apply the **same shape check as
their attach counterparts**, refusing a malformed `PolicyArn` with `InvalidInput` (400)
([#875](https://github.com/scttfrdmn/substrate/issues/875)). Before that they answered
`NoSuchEntity` (404) — "the policy is not attached to the specified entity" — which was true but
pointed at the wrong thing: a consumer handling it inspects the attachment, finds the policy
attached under its real ARN, and has been told the opposite of its actual mistake, which was the
string it typed. `API_DetachUserPolicy` publishes `InvalidInput` for "an invalid or out-of-range
value ... supplied for an input parameter", and publishes `PolicyArn`'s length range as 20-2048,
so both refusals come from the API model.

**A well-formed ARN that is not attached still answers `NoSuchEntity` (404)**, which is also
published, and which is not in tension with the attach side's refusal to require the policy to
exist. The two are about different facts. Substrate holds every attachment it was told about, in
full, so an absent attachment is a fact it can report exactly; policy *existence* is a fact it
cannot report, bundling 52 of roughly 1,200 managed policies — which is why the attach warns
instead of refusing. A detach therefore requires the attachment, never the policy.

### Policy documents and versions

`GetPolicy` returns metadata only — policy ID, name, ARN, path, default version, attachment
count and dates — which is what AWS returns. The document comes from `GetPolicyVersion`,
URL-encoded per RFC 3986, for a bundled policy and a created one alike.

**A customer managed policy holds up to five versions** (#1560). `CreatePolicyVersion` adds
one, and makes it the default when `SetAsDefault` is true. The default is the document the policy
evaluator reads. A sixth version is `LimitExceeded` (409), with the API reference's own sentence as
its message. A version's number is one more than the highest the policy has had, so a number that
`DeletePolicyVersion` freed is not reused. `UpdateDate` becomes the newest version's creation time.
`DeletePolicyVersion` refuses the default version with `DeleteConflict` (409), quoting the API
reference. `SetDefaultPolicyVersion` switches the default, and the change takes effect at the next
authorization decision. `DeletePolicy` refuses a policy that still has versions other than its
default, as `DeleteConflict` (409). `API_DeletePolicy` lists deleting them as a step, and the
message is substrate's own. `ListPolicyVersions` lists newest first, as the page's sample does, and
pages with `MaxItems` and `Marker`. `GetAccountAuthorizationDetails` lists every version.

**A bundled AWS managed policy holds only its current default**, so `GetPolicyVersion` answers
`NoSuchEntity` for any of its other `VersionId`s. This is reachable immediately: the catalog
reports real AWS defaults, so `AmazonSSMManagedInstanceCore` has `DefaultVersionId: v2` and `v1`
does not exist here. Claiming `v1` resolves when the policy reports `v3` as its default would be a
fabricated document. The three version writes refuse an AWS managed ARN with `InvalidInput`, for the
reason `DeletePolicy` does. A `VersionId` that is not version-shaped at all is `InvalidInput` (400).

The seeded documents are also readable in process through `emulator.GetManagedPolicy`, and
are what the IAM policy evaluator reads.

## Renaming a user or group

`UpdateUser` and `UpdateGroup` change a name, a path or both. Everything substrate stores under the
name moves with it:
- the record, with its ARN rebuilt from the new path and name;
- the attached and inline policies;
- both sides of each group membership;
- for a user, the access keys (each key's record names its user, and a request signed with the key
  resolves to the renamed user), the login profile, and its `GetSessionToken` sessions. AWS ties a
  session to the user rather than to its name, so a session minted before the rename keeps the
  renamed user's permissions.

**Readers outside IAM see one name or the other, whole.** These are principal resolution, the
authorization gate and STS. The new name's records are written first, then its entity record. Next
the pointers that name the user are flipped one by one, and only then are the old records deleted,
old entity record first. **A failure part way through is undone:** every write records what it
replaced, so the entity is left wholly under its old name and the rename can be retried. Writers
that could race a rename take the same lock: IAM's own operations, and the tagging API's writes to
IAM records, which go through IAM. So none of them can write a record back under a name the rename
removed.

The rename also authorizes the target name, since AWS requires "permissions on both the source
object and the target object". The `NoSuchEntity` and `EntityAlreadyExists` messages match the other
user and group operations, and the path message is substrate's own.

## Identity providers

An OpenID Connect or SAML provider is a record a role's trust policy names as a `Federated`
principal, which `AssumeRoleWithWebIdentity` and `AssumeRoleWithSAML` read (see the
[STS page](/reference/sts#federated-assumes-check-claims-not-signatures)). Substrate contacts no
provider. `CreateOpenIDConnectProvider` without a `ThumbprintList` stores none, where AWS would
fetch one, and `OpenIdIdpCommunicationError` is reachable only by seed:

```bash
curl -X POST localhost:4566/v1/iam/oidc-provider-outcome -d '{"url":"https://idp.example.com"}'
curl -X DELETE 'localhost:4566/v1/iam/oidc-provider-outcome?url=https://idp.example.com'
```

`url` defaults to `*`, which matches every URL. The seed holds until it is cleared. Most refusal
messages are AWS's: the OIDC `EntityAlreadyExists` and `NoSuchEntity` wording is observed from
the service, and the SAML key and encryption refusals are the IAM troubleshooting guide's. The
limit and validation messages, and the SAML `NoSuchEntity`, are substrate's own.

## The bookkeeping a record carries reaches no response

Four IAM records declare bookkeeping members under wire-visible `json` tags:

- `IAMRole`, `IAMUser` and `IAMInstanceProfile` each declare `EverTagged` as
  `ever_tagged,omitempty`. The instance profile's has been read since GetResources began scanning
  instance profiles (#1523).
- `IAMAccessKey` declares its owning account as `AccountId,omitempty`. An access key's ID is what
  determines the account, so the account cannot be in its state key (#737).

None of these members reaches a body: every response is marshaled from an XML struct declared for its
operation, and no XML-tagged field is typed as one of these records. Every operation that answers one
of the records is driven by `TestIAMWire_RoleResponsesCarryNoBookkeepingMember` and its siblings in
`emulator/iam_wire_test.go`. That includes the ones that render it nested: `GetGroup`, the two
instance-profile reads and `GetAccountAuthorizationDetails`. Each test walks the raw XML for a
bookkeeping element at any depth
([#756](https://github.com/scttfrdmn/substrate/issues/756)).

Every member here is `,omitempty`, so each would be absent for free from a record that did not hold
it. The role and the user are therefore tagged first, and `ever_tagged` is read back; it is set only
by a tag write (#938). The access key's account is read back too, since `CreateAccessKey` writes it on
every key.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::IAM::Role | RoleName | `CreateRole` with the trust policy, `Description`, `MaxSessionDuration` and `Tags`; then `PermissionsBoundary`, `ManagedPolicyArns` (attached) and `Policies` (embedded inline). Updated in place; `RoleName` and `Path` replace it. `Fn::GetAtt` `Arn`, `RoleId` |
| AWS::IAM::Policy | a generated ID | An **inline** policy: `Put{Role,User,Group}Policy` under `PolicyName` on every entry of `Roles`, `Users` and `Groups`. Naming none of the three fails the resource. Updated in place. `Ref` and `Fn::GetAtt Id` answer the generated physical ID |
| AWS::IAM::ManagedPolicy | the policy ARN | `CreatePolicy`, then attached to every entry of `Roles`, `Users` and `Groups`. A changed `PolicyDocument` becomes a new default version. `ManagedPolicyName`, `Path` and `Description` replace it. `Fn::GetAtt` `PolicyArn`, `PolicyId`, `DefaultVersionId`, `AttachmentCount`, `IsAttachable`, `PermissionsBoundaryUsageCount`, `CreateDate`, `UpdateDate` |
| AWS::IAM::User | UserName | `CreateUser` with `Path` and `Tags`; then `PermissionsBoundary`, `LoginProfile` (`CreateLoginProfile`), `Groups` (joined), `ManagedPolicyArns` and `Policies`. Updated in place: `Path` through `UpdateUser`, `LoginProfile` through Create/Update/`DeleteLoginProfile`. `UserName` replaces it. The delete removes the login profile first. `Fn::GetAtt Arn` |
| AWS::IAM::InstanceProfile | InstanceProfileName | Attaches each entry in `Roles`; resolvable by an `AWS::EC2::Instance`'s `IamInstanceProfile` |
| AWS::IAM::OIDCProvider | the provider ARN | `CreateOpenIDConnectProvider` with `ClientIdList`, `ThumbprintList` and `Tags`. Those three update in place against the live record; `Url` replaces it. `Fn::GetAtt Arn` |
| AWS::IAM::SAMLProvider | the provider ARN | `CreateSAMLProvider` with `SamlMetadataDocument`, `AssertionEncryptionMode` and `Tags`, updated in place; `Name` replaces it. `Fn::GetAtt Arn`, `SamlProviderUUID` |
| AWS::IAM::Group | GroupName | `CreateGroup` with `Path`; then `ManagedPolicyArns` and `Policies`. Updated in place, `Path` through `UpdateGroup`. `GroupName` replaces it. `Fn::GetAtt Arn` |

A role, user, group or policy remembers what it has in place: what its deploy and its updates
attached, embedded and joined, written as each call succeeds. An update changes only the
difference between the declaration and what is in place, and a delete undoes exactly that before
deleting the entity, in the order `DeleteUser`, `DeleteGroup` and `DeleteRole` require. A policy
attached to a stack's role from outside the stack is not the stack's to detach, so `DeleteRole`
then answers `DeleteConflict`, as it does on AWS.

What is in place is read from the stack's latest deployment, so the rollback of a failed update
detaches, deletes and untags what that update added. Four more rules apply to updates:

- An inline policy is put again only when `Policies`, or an `AWS::IAM::Policy`'s `PolicyDocument`,
  changed.
- A user's or role's tag key whose case alone changed is kept. IAM's tag keys for those two types
  are not case sensitive, so the new tag replaces the old one.
- A `Path` equal to the user's or group's own is not refused.
- An `AWS::IAM::ManagedPolicy` document that a version already holds reuses that version, through
  `SetDefaultPolicyVersion` when it is not the default, rather than creating a duplicate.
  This is substrate's reading.

**An `AWS::IAM::Policy` is an inline policy** (#1562). Its page says it "adds or updates an inline
policy document that is embedded in the specified IAM group, user or role". It used to be created
as an unattached managed policy instead, so a role the template granted permissions to had none. A
stack recorded by a release before the change still holds that managed policy. Deleting the stack,
or updating the resource, deletes it.

## Cost

IAM operations are free.
