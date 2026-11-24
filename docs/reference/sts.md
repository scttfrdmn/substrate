# STS

**Endpoint:** `sts.amazonaws.com`
**Protocol:** AWS Query (form-encoded)

## Supported operations

| Operation | Notes |
|-----------|-------|
| GetCallerIdentity | Returns account 123456789012 by default |
| AssumeRole | Evaluates the role's trust policy, then returns temporary credentials |
| GetSessionToken | Returns stub temporary credentials |

## What `GetCallerIdentity` reports as `UserId`

AWS's [identifiers reference](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_identifiers.html)
documents three forms, and names `GetCallerIdentity` as the way to read one:

| Caller | `UserId` |
|--------|----------|
| An IAM user | the user's unique ID, `AIDA…` |
| An assumed role | `AROA…:<role-session-name>` — the role's unique ID, then the session name the caller chose |
| The account root | the account ID |

Substrate reported the caller's *friendly name* until
[#805](https://github.com/scttfrdmn/substrate/issues/805) — `alice`, and `worker/sess1`
for a session, which is the ARN's last two segments rather than any documented shape.
It is the same value `aws:userid` publishes, so a policy conditioned on that key and an
assertion on `UserId` describe one caller.

A caller who resolves to no IAM entity keeps the third row's answer or, if a credential
names a user who has since been deleted, that user's name. Every caller on AWS has a
unique ID, so this case is substrate's own: a name identifies the caller, where an empty
member could not be told from a bug.

## A role's trust policy is enforced, and `sts:ExternalId` with it

`AssumeRole` evaluates the role's `AssumeRolePolicyDocument` before minting a
session, so a caller the trust policy does not admit is refused with
`AccessDenied` (403). Two gates apply, and they answer different questions: the
caller's own policies must allow `sts:AssumeRole` ("what may this caller do"), and
the role's trust policy must admit the caller ("who may become this role"). Both
report the same code, so a consumer cannot tell them apart by code alone — the
message says which one refused.

That code follows the service's **wire protocol** rather than which gate refused or
which service was called: `AccessDenied` on the XML protocols (Query, REST-XML and
EC2 — so STS, IAM, CloudFormation, SNS, S3, EC2) and `AccessDeniedException` on the
JSON ones (so SQS, DynamoDB, Lambda, SSM). That is what AWS does, and it is the
same split every AWS service model shows.

That makes the confused-deputy pattern testable. A trust policy conditioning on
`sts:ExternalId` refuses a caller who cannot present the secret:

```json
{"Version": "2012-10-17", "Statement": [{
  "Effect": "Allow",
  "Principal": {"AWS": "123456789012"},
  "Action": "sts:AssumeRole",
  "Condition": {"StringEquals": {"sts:ExternalId": "secret-123"}}
}]}
```

A `Principal` naming a bare account ID or an account-root ARN
(`arn:aws:iam::123456789012:root`) admits any principal in that account; an exact
ARN admits only that entity. `ExternalId` is validated at 2–1224 characters.

AWS distinguishes the two refusals only in the message, and substrate matches:
"because no role trust policy allows the sts:AssumeRole action" for no matching
statement, and "with an explicit deny in the role trust policy" for an explicit
`Deny`.

**Writing a trust policy is the opt-in.** A role created without one — which
substrate permits, though AWS's `CreateRole` does not — is not enforced, so a test
that never wrote a trust policy is unaffected. Enforcement is also skipped for an
unauthenticated caller, who resolves to no principal for a `Principal` element to
be true of; this is the same rule described in
[Testing IAM permissions](/testing-guide#testing-iam-permissions).

`iam:UpdateAssumeRolePolicy` replaces the document in place, which is how a test
exercises the update-in-place shape IaC emits: assume, tighten, and be refused on
the same role. The replacement takes effect on the **next** `AssumeRole`; sessions
already minted under the old policy stay valid, as on AWS. `GetRole` reports the
stored document, so a test can assert what it set.

## The account a session credential carries reaches no response

`STSSessionCredentials` declares its account under a wire-visible `json` tag, because the record is what
`MemoryStateManager` snapshots and a replay reads back, and what a request signed with the session's key is resolved from. It does not reach a body: `AssumeRole` and `GetSessionToken` answer an XML struct declared for their operation, copying the four published `Credentials` members out of the record, and `GetCallerIdentity` publishes the account as `Account`.
`TestSTSWire_SessionCredentialResponsesCarryNoBookkeepingMember` in `emulator/sts_wire_test.go` drives all three routed operations, `GetCallerIdentity` signed with an assumed-role session's key, and walks each response's element names for the member
([#756](https://github.com/scttfrdmn/substrate/issues/756)).

## Cost

STS operations are free.
