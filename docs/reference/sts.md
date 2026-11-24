# STS

**Endpoint:** `sts.amazonaws.com`
**Protocol:** AWS Query (form-encoded)

## Supported operations

| Operation | Notes |
|-----------|-------|
| GetCallerIdentity | Returns account 123456789012 by default |
| AssumeRole | Evaluates the role's trust policy, then returns temporary credentials. `Policy` and `PolicyArns` narrow the session — see [Session policies](#session-policies-narrow-the-session) |
| GetSessionToken | Returns stub temporary credentials |
| AssumeRoleWithWebIdentity | Unsigned. Checks the token's `iss`, `aud` and `exp` against a registered OIDC provider and the simulated clock, then the role's trust policy — see [below](#federated-assumes-check-claims-not-signatures) |
| AssumeRoleWithSAML | Unsigned. Checks the assertion's provider, Role attribute and `NotOnOrAfter`, then the trust policy with the `saml:` keys |

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

## Federated assumes check claims, not signatures

`AssumeRoleWithWebIdentity` and `AssumeRoleWithSAML` need no AWS credentials, and the SDKs send
them unsigned. Substrate routes such a request to STS by its `Action` when nothing else names a
service, which is what a client pointed at one base endpoint sends.

Substrate decodes the token and verifies no signature. What it checks is what a test can arrange:

| Check | Web identity | SAML | Refusal |
|-------|--------------|------|---------|
| The provider is registered in the role's account | `iss`, without `https://`, names an [OIDC provider](/reference/iam#identity-providers) exactly (AWS: "The URL is case-sensitive") | `PrincipalArn` names a SAML provider, and the assertion's `Issuer` is an `entityID` its metadata declares | `InvalidIdentityToken`/400 |
| The audience is AWS's | `aud` is in the provider's client IDs | an `Audience` is `https://signin.aws.amazon.com/saml` (or a regional one) or `urn:amazon:webservices` | `InvalidIdentityToken` "Incorrect token audience", "Response does not contain the required audience." |
| The token names a subject | `sub` (REQUIRED by OpenID Connect Core §2) | a `NameID` | `InvalidIdentityToken`, `AccessDenied` |
| The token is current on the simulated clock | `exp`, and `nbf` when present | every `NotOnOrAfter` and `NotBefore` | `ExpiredTokenException`/400 for an expiry, `InvalidIdentityToken` for a token not yet valid |
| The assertion names this role | — | a `Role` attribute pairs `RoleArn` with `PrincipalArn`, and there is a `RoleSessionName` | `AccessDenied`/403, or `InvalidIdentityToken` for the session name |
| The trust policy admits the provider | a `Federated` entry naming it, for `sts:AssumeRoleWithWebIdentity`, and `sts:TagSession`/`sts:SetSourceIdentity` when the token passes tags or a source identity | the same, for `sts:AssumeRoleWithSAML` | `AccessDenied` "Not authorized to perform sts:AssumeRoleWithWebIdentity" |
| The duration fits the role | `DurationSeconds` (default 3600) is at most the role's `MaxSessionDuration` | the same; the `SessionDuration` attribute is the console's limit, not the API default | `ValidationError` "Requested DurationSeconds exceeds MaxSessionDuration set for this role." |

The trust policy is evaluated with the provider's ARN as the principal. Only a `Federated` entry
names one, so an `"AWS": "<account>"` statement does not admit a web identity. A role with no trust
policy admits no federated caller. `AssumeRole` reads a missing policy as not enforced, but AWS
requires the provider to be "specified in the role's trust policy".

The condition keys are AWS's. For OIDC, the provider URL without its scheme is the prefix:
`token.actions.githubusercontent.com:sub` and `:aud` for GitHub Actions, and
`oidc.eks.<region>.amazonaws.com/id/<id>:sub` for EKS IRSA. `:aud` is `azp` when the token sets it,
`:oaud` is `aud`, and `:email` and `:amr` are published when the token carries them. For SAML the
keys are `saml:aud` (the `Recipient`), `saml:iss`, `saml:sub`, `saml:sub_type`, `saml:doc` and
`saml:namequalifier`. Key names match case-insensitively, so `SAML:aud` works too.

The session is the one `AssumeRole` mints: an `ASIA…` key, the
`assumed-role/<role>/<session>` ARN, and `RoleLastUsed` on the role. A SAML session ends at the
assertion's `SessionNotOnOrAfter` when that comes first.

**Session tags and source identity** come from the token. A web identity passes tags in the
`https://aws.amazon.com/tags` claim (`principal_tags`, `transitive_tag_keys`) or the flattened
`https://aws.amazon.com/tags/principal_tags/<key>` claims, and a source identity in
`https://aws.amazon.com/source_identity`. A SAML assertion uses the `PrincipalTag:<key>`,
`TransitiveTagKeys` and `SourceIdentity` attributes. At the assume they are `aws:RequestTag/<key>`,
`aws:TagKeys`, `sts:TransitiveTagKeys` and `sts:SourceIdentity`. Afterwards they are the session's
`aws:PrincipalTag/<key>` (over the role's own tags, a session tag winning regardless of case) and
`aws:SourceIdentity`. The response reports `SourceIdentity`. Role chaining is not modeled, so
transitive keys are evaluated and not carried forward.

## Session policies narrow the session

`Policy` and `PolicyArns` on `AssumeRole`, `AssumeRoleWithWebIdentity` and `AssumeRoleWithSAML` are
session policies: "The resulting session's permissions are the intersection of the role's
identity-based policy and the session policies." The documents are stored on the session, and
every request signed with it must be allowed by them after the identity policies and the
permissions boundary. A refusal says "(blocked by session policy)".

Both limits are enforced: 2,048 characters for `Policy` and 10 ARNs for `PolicyArns`, each
answering `ValidationError`. A `Policy` that is not a policy document is `MalformedPolicyDocument`,
in substrate's own wording. A `PolicyArns` entry naming no policy is `MalformedPolicyDocument`
`Policy <arn> does not exist or is not attachable.`, observed from the service. The documents are
read when the session is minted, so a later change to a managed policy does not reach an existing
session.

### Seeding what the identity provider decides

A signature or thumbprint that does not verify, a claim the provider rejects, and a provider
that cannot be reached all depend on the provider, so each is a seed:

```bash
curl -X POST localhost:4566/v1/sts/federation-outcome \
  -d '{"provider":"arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com","error":"IDPRejectedClaim","count":1}'
curl -X DELETE 'localhost:4566/v1/sts/federation-outcome?provider=*'
```

`error` is `InvalidIdentityToken`, `IDPRejectedClaim` (403) or `IDPCommunicationError`, which
only the web-identity operation documents. `provider` defaults to `*`. `count` is how many
assumes the seed answers before it clears itself, so a retry loop can be shown failing and then
succeeding. Without a count the seed holds until it is cleared. The seed applies only to a token
whose provider is registered, and it is checked before the audience and the expiry.

Most messages are AWS's. The web-identity provider, audience, expiry and refusal wording is
observed from the service. The SAML session-name, source-identity, provider, issuer, audience,
signature and `MaxSessionDuration` wording is the IAM troubleshooting guide's. These messages are
substrate's own: the malformed token, a missing `exp` or `sub`, `nbf`/`NotBefore`, the SAML expiry,
the session-tag limits, and the seeded `IDPRejectedClaim` and `IDPCommunicationError`.

## The account a session credential carries reaches no response

`STSSessionCredentials` declares its account under a wire-visible `json` tag, because the record is what
`MemoryStateManager` snapshots and a replay reads back, and what a request signed with the session's key is resolved from. It does not reach a body: `AssumeRole` and `GetSessionToken` answer an XML struct declared for their operation, copying the four published `Credentials` members out of the record, and `GetCallerIdentity` publishes the account as `Account`.
`TestSTSWire_SessionCredentialResponsesCarryNoBookkeepingMember` in `emulator/sts_wire_test.go` drives `AssumeRole`, `GetSessionToken` and `GetCallerIdentity` (signed with an assumed-role session's key) and walks each response's element names for the member
([#756](https://github.com/scttfrdmn/substrate/issues/756)). `TestSTSWire_FederatedResponsesCarryNoBookkeepingMember` does the same for the two federated assumes.

## Cost

STS operations are free.
