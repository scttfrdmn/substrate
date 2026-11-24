# Cognito Identity

**Endpoint:** `cognito-identity.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AWSCognitoIdentityService.{Op}`)

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateIdentityPool | |
| DescribeIdentityPool | Reports the six of `API_DescribeIdentityPool`'s ten members substrate models. `Roles` is **not** among them and is no longer reported (#756) — `GetIdentityPoolRoles` is the published door to a pool's role mapping |
| DeleteIdentityPool | |
| ListIdentityPools | `MaxResults` is **required** over 1–60 and an absent value is refused rather than defaulted, because the API publishes no default (#671). Pages on `NextToken`, which is the next pool's ID rather than an opaque cursor; summaries carry `IdentityPoolId` and `IdentityPoolName` only |
| GetId | Mints a `REGION:GUID` identity ID from the [request-derived minter](/reference/concepts/seeds-and-replay#an-identifier-a-replay-mints-is-the-one-it-recorded), so a replay answers with the ID its recording answered with (#856). The request is not read: no pool is looked up, nothing is persisted, and `IdentityPoolId` and `Logins` are ignored — a second call for one login returns a second ID |
| GetIdentityPoolRoles | `ResourceNotFoundException`/404 for an absent pool, `InvalidParameterException`/400 for an absent `IdentityPoolId`; `Roles` is `{}` rather than `null` when none are set |
| SetIdentityPoolRoles | Merges into the existing role map rather than replacing it, so a call omitting a previously set key leaves that key in place. Same refusals as `GetIdentityPoolRoles`; nothing validates that a role ARN exists in IAM |
| GetCredentialsForIdentity | Returns stub temporary credentials, the whole of `API_Credentials`' four members. `Expiration` is one hour past the simulated clock and is reported as epoch seconds, as the page publishes it |

**Responses are rendered from the published shape.** All four operations that report an identity pool
already built their own response struct, so substrate's `AccountID` and `Region` could not reach a body
here even before [#756](https://github.com/scttfrdmn/substrate/issues/756) — and `API_IdentityPool`
declares neither, so neither has a published home to move to. What that issue added is the assertion
that this stays true, and writing it caught the two divergences recorded in the table above: an
unpublished `Roles` on `DescribeIdentityPool`, and `Credentials.Expiration` reported as an RFC3339
string where the page publishes epoch seconds. See
[Responses are rendered from the published shape](/reference/cognito-idp) under Cognito User Pools for the
rest of that reading; the two services were fixed together, since both answer from the same records.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::Cognito::IdentityPool | IdentityPoolId | |

## Cost

Cognito Identity operations are free.
