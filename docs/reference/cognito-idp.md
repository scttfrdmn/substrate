# Cognito User Pools

**Endpoint:** `cognito-idp.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: AWSCognitoIdentityProviderService.{Op}`)

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateUserPool | Pool ID format: `{region}_{12-char alphanum}`, derived from the request ID (#856) |
| DescribeUserPool | Reports the pool as `UserPoolType` — so the identifier is `Id` and the tag set is `UserPoolTags` — and is the only door that publishes it after an update |
| UpdateUserPool | Replaces the published configuration and answers an empty body — see below |
| DeleteUserPool | Cascades: the pool's app clients, groups and users are deleted with it |
| ListUserPools | Summaries are `UserPoolDescriptionType`, `LambdaConfig` included. `MaxResults` is published `Required: Yes` over 1–60 with no default, so an absent, zero or out-of-range value is refused rather than rewritten (#1062). Summaries ascend by pool ID; `NextToken` is reported as the next pool's ID but is not read back, so a second page repeats the first |
| CreateUserPoolClient | Reads `ClientName`, `GenerateSecret` and `ExplicitAuthFlows`; a generated secret is 24 characters, two drawn IDs concatenated |
| DescribeUserPoolClient | Reports the client as `UserPoolClientType`, `ClientSecret` included |
| UpdateUserPoolClient | Replaces the published configuration — see below |
| DeleteUserPoolClient | Refuses an unknown client before deleting, unlike the group and user deletes |
| ListUserPoolClients | `UserPoolClients` summaries of `ClientId`/`ClientName`/`UserPoolId`, ascending by id. One page only: `MaxResults` is rewritten to 60 and never applied as a page size, a page-size defect recorded rather than smoothed over. An absent `UserPoolId` is refused rather than answered with an empty list (#1062) |
| CreateUserPoolDomain | Validates the pool and records the domain, answering an empty `CloudFrontDomain`. **Not modelled: the hosted UI.** No S3 bucket, distribution or sign-in page exists behind the domain |
| DescribeUserPoolDomain | Reads no state: echoes the requested `Domain` with `Status` `ACTIVE`, so a domain that was never created still describes as active |
| DeleteUserPoolDomain | Deletes the recorded domain with no existence check |
| CreateGroup | Records `Description`, `RoleArn` and `Precedence` against a pool that must exist |
| GetGroup | Reports the group as `GroupType`. `Precedence` is reported as stored, including zero, which the page's `null` default is distinguishable from in the shape but not in the record |
| ListGroups | Every group of the pool in one page, ascending by name; no paging member is read |
| DeleteGroup | `200` with no existence check, and a user's recorded membership outlives the group — see `AdminListGroupsForUser` |
| AdminCreateUser | `UserStatus` `FORCE_CHANGE_PASSWORD`; `TemporaryPassword` is decoded and not stored. The user is reported as `UserType`, whose seven members include neither `UserPoolId` nor a group list |
| AdminGetUser | Reports the user's fields at the top level rather than nested, as the operation publishes them, and under the member names `UserType` uses — so no `UserPoolId` and no `Groups` |
| AdminDeleteUser | `200` with no existence check |
| AdminSetUserPassword | `Permanent: true` moves the user to `CONFIRMED`. The password itself is never stored, and nothing later verifies one — see `InitiateAuth` |
| ListUsers | Every user of the pool in one page, ascending by username; `Filter`, `AttributesToGet` and `PaginationToken` are not read |
| AdminAddUserToGroup | Appends to the user's recorded group names; a repeat is a no-op, and the group is **not** required to exist |
| AdminRemoveUserFromGroup | Drops the name; a group the user is not in is a no-op |
| AdminListGroupsForUser | Resolves the recorded names to group records, silently skipping any group deleted since |
| SignUp | Finds the pool by scanning its pools for one holding the `ClientId`; `ResourceNotFoundException` when none does. Mints a `sub` appended to the request's attributes, `UserStatus` `UNCONFIRMED`, and answers `UserConfirmed: false`. **No password policy is enforced** |
| ConfirmSignUp | The same client scan, then `CONFIRMED`. `ConfirmationCode` is not validated, so any code confirms — there is no code to match, since substrate delivers no message |
| InitiateAuth | Returns stub JWT tokens |
| RespondToAuthChallenge | The identical stub tokens, reading neither the request nor state, so no challenge is ever verified |
| GetUserPoolMfaConfig | Reports the pool's `MfaConfiguration` and nothing else |
| SetUserPoolMfaConfig | Stores `MfaConfiguration` and echoes it; `SmsMfaConfiguration`, `SoftwareTokenMfaConfiguration` and `EmailMfaConfiguration` are accepted and not recorded |
| TagResource | Merges `Tags` into the pool's tag set, the one `DescribeUserPool` reports as `UserPoolTags`, and answers an empty body — see [Tagging](#tagging-reads-and-writes-the-pool-s-own-tag-set) |
| UntagResource | Removes the `TagKeys` named; a key the pool does not hold is a no-op. Answers an empty body |
| ListTagsForResource | Reports the pool's tag set as `Tags`, `{}` when it holds none |

**What the four loaders refuse.** Every operation that addresses an existing record keys through one
resolver per type, so the refusals are uniform: an empty `UserPoolId`, `ClientId`, `GroupName` or
`Username` is `InvalidParameterException` at 400 — the reading `cognito_errors.go` records, since this
service's gloss on that code says *invalid* and not *missing* — an absent pool, client or group is
`ResourceNotFoundException`, and an absent user is `UserNotFoundException`, the code that operation's
own page publishes rather than the generic one. An unparseable body is `InvalidParameterException`.
Both not-found codes answer HTTP **400**, which is what `API_DescribeUserPool`, `API_AdminGetUser` and
every other user pools page publish for them; until
[#1425](https://github.com/scttfrdmn/substrate/issues/1425) substrate answered **404**, a status the
service publishes for neither. A record keyed by the caller's account and Region means an identifier from
another account reaches nothing rather than the caller's same-named record.

## `UpdateUserPool` and `UpdateUserPoolClient` replace, they do not merge

`API_UpdateUserPool` and `API_UpdateUserPoolClient` carry the same Important box, word for word:

> If you don't provide a value for an attribute, Amazon Cognito sets it to its default value.

and the same recommendation above it — build the request from the current configuration, which both pages
point at `DescribeUserPool` / `DescribeUserPoolClient` to obtain. Substrate assigned each member only when
the request supplied a non-empty one, so a caller following that advice and omitting a member it did not
want to change saw the stored value survive where AWS resets it. Since
[#1089](https://github.com/scttfrdmn/substrate/issues/1089) both handlers assign every member their page
publishes, and the create shares the same resolver so the two doors cannot drift apart again.

Full replacement governs only the members an operation **publishes**. `Schema` is absent from
`API_UpdateUserPool`'s Request Syntax, so `SchemaAttributes` is *preserved* across an update rather than
cleared — an operation cannot reset a member it does not accept. `ProviderName`, `Status`, `Arn` and
`CreationDate` are preserved for the same reason.

Two defaults are applied on the reset:

| Member | Default | Citation |
|--------|---------|----------|
| `ExplicitAuthFlows` | `ALLOW_REFRESH_TOKEN_AUTH`, `ALLOW_USER_SRP_AUTH`, `ALLOW_CUSTOM_AUTH` | Published on `API_UpdateUserPoolClient` and `API_CreateUserPoolClient`: *"If you don't specify a value for `ExplicitAuthFlows`, your app client supports `ALLOW_REFRESH_TOKEN_AUTH`, `ALLOW_USER_SRP_AUTH`, and `ALLOW_CUSTOM_AUTH`."* An explicit `[]` is a value the caller specified and keeps the empty set. |
| `MfaConfiguration` | `OFF` | **Unpublished.** Neither page carries a `Default:` line; both publish `Valid Values: OFF \| ON \| OPTIONAL`. `OFF` is substrate's reading, and it predates this change — the create has always applied it. It is applied at the update so an omitted member cannot leave the pool reporting the empty string, which is not a value the page publishes (#1013). |

`UpdateUserPool` answers **200 with a byte-empty body**. Its Response Syntax is `HTTP/1.1 200` followed by
nothing, where `UpdateUserPoolClient`'s publishes a `UserPoolClient` object — so the two updates differ,
and substrate answers each as its own page publishes rather than making them symmetrical. Empty rather
than `{}` follows `AppSync`'s in-tree precedent: `{}` is a member-less object where the page promises no
object at all.

## Other `Update*` handlers are not flipped by analogy

41 `update*` handlers live in `emulator/`, and 29 of them guard an assignment on a non-empty request
member. Exactly three pages publish that an update is a full replacement — `API_UpdateSchedule`,
`API_UpdateUserPool` and `API_UpdateUserPoolClient` — and only those three lost their guards. The other
26 keep them, because [#671](https://github.com/scttfrdmn/substrate/issues/671)'s binding scope decision
is that substrate models only what an operation's own page states: a published sentence is not extended to
a sibling by analogy, however tempting the symmetry.

Step Functions is the sharpest case, because its page argues the other way rather than merely staying
silent. `API_UpdateStateMachine` publishes both `definition` and `roleArn` as `Required: No` and then
publishes `MissingRequiredParameter` — *"This error occurs if both `definition` and `roleArn` are not
specified."* A request naming only `roleArn` is therefore explicitly legal, which full replacement would
turn into a request that blanks the definition and leaves a state machine the service could not execute.
So the merge there is what the page describes, and substrate keeps it.

## Tagging reads and writes the pool's own tag set

`TagResource`, `UntagResource` and `ListTagsForResource` are routed since
[#1135](https://github.com/scttfrdmn/substrate/issues/1135). All three address the same stored map
`DescribeUserPool` reports as `UserPoolTags`, which `CreateUserPool` and `UpdateUserPool` also write;
`UpdateUserPool` still replaces it outright like every other published member.

- **Only a user-pool ARN resolves.** Each page describes `ResourceArn` as the ARN "of the user pool",
  and no page names an app client, whose `UserPoolClientType` publishes no ARN member. So the accepted
  form is `arn:aws:cognito-idp:{region}:{account}:userpool/{id}`, in the caller's own account and
  Region. Anything else, or a pool that does not exist, is `ResourceNotFoundException` at the
  published **400**, as the loaders above answer it.
- **`ResourceArn`, `Tags` and `TagKeys` are `Required: Yes`**; an absent one is
  `InvalidParameterException`/400.
- **The two writes answer an empty body**, which is their pages' Response Elements, rather than the
  `{}` `TagResource`'s Sample Response shows.
- **The 50-tag quota is not enforced.** The `TagResource` page states it, but publishes no code for
  exceeding it.
- **GetResources sees the pools.** The resource-groups-tagging scanner already listed user pools by
  reading the same map. A tag write stamps the previously-tagged flag, so a pool whose tags are all
  removed is reported with an empty set rather than vanishing.

**Responses are rendered from the published shape.** Four of the five Cognito records — the user pool,
its app client, a group and a user — were not. Each was embedded straight into a response struct, so
every persisted member marshaled into the body, across eleven operations. Since
[#756](https://github.com/scttfrdmn/substrate/issues/756) those eleven answer through the projections
in `emulator/cognito_idp_wire.go`, and the identity pool — whose four operations already built their
own response struct, as `ListUserPools`, `ListUserPoolClients`, `AdminGetUser` and `SignUp` did on this
side — is pinned by the same tests. None of substrate's `AccountID`, `Region` and `ever_tagged` has a
published home to move to: no shape in either API declares an account or a Region member, checked
member-by-member against `API_UserPoolType`, `API_UserPoolClientType`, `API_GroupType`, `API_UserType`
and `API_IdentityPool`. `UserPoolType` publishes the `Arn` instead, from which a caller recovers both,
which is what the real API requires of one.

Reading those shapes member-by-member to write the projection settled five divergences, noted in the
tables above:

- **Two members were reported under a name AWS does not publish.** The pool's identifier was
  `UserPoolId`, where `UserPoolType` publishes `Id` and declares no `UserPoolId` at all
  ([#1286](https://github.com/scttfrdmn/substrate/issues/1286)), and its tag set was `Tags`, where the
  same shape publishes `UserPoolTags`
  ([#1136](https://github.com/scttfrdmn/substrate/issues/1136)). The identifier is the one that bit: an
  SDK decoding `CreateUserPool` read `UserPool.Id` as nil and had to fall back to `ListUserPools` to
  learn the ID of the pool it had just created — and `ListUserPools` rendered `Id` correctly from its
  own summary struct, so one service answered the same identifier under two member names depending on
  which door a caller knocked on.
- **Three unpublished members reached a body.** The pool's `ProviderName` is absent from all thirty-six
  of `UserPoolType`'s members, and is derivable from the pool ID and the Region — which is how
  substrate mints it and how the `AWS::Cognito::UserPool` deployer now recovers it for the
  `ProviderName` and `ProviderURL` attributes rather than reading it off the response, as real
  CloudFormation computes it. A user's `UserPoolId` and `Groups` are likewise undeclared by
  `API_UserType`: the pool ID is how a user is keyed, and `AdminListGroupsForUser` is the operation
  that reports a user's groups.
- **`DescribeIdentityPool` reported a `Roles` member.** `API_DescribeIdentityPool` publishes ten
  response members and that is not one of them. `GetIdentityPoolRoles` is the published door to a
  pool's role mapping, substrate routes it, and the stored roles are still answered there.
- **Every Cognito timestamp was an RFC3339 string.** All nine are now epoch seconds — see below.
- **`ListUserPools` withheld a member its summary publishes.** `API_UserPoolDescriptionType` declares
  `LambdaConfig`, substrate stores it, and the summary reported it nowhere; it now does.

The stored fields themselves stay on all five records, so the state a recorded run replays from is
unchanged and the Resource Groups Tagging API — which reads those records rather than these responses —
still reports a user pool's tags and whether it has ever been tagged. `ProviderName` and a user's
`Groups` are still persisted and still answered, by the CloudFormation deployer's derivation and by
`AdminListGroupsForUser` respectively.

Published members substrate does not model are **absent** rather than reported as zero
([#1013](https://github.com/scttfrdmn/substrate/issues/1013)). Two are worth naming because a consumer
is likely to look for them: `UserPoolClientType.LastModifiedDate` and `GroupType.LastModifiedDate` are
absent because no Cognito record but the user pool persists a modification time at all, and
`UserType.MFAOptions` because substrate records no MFA enrolment for a user. `UserPoolType`'s long
configuration surface — `AccountRecoverySetting` through `VerificationMessageTemplate`, twenty-five
members — is absent for the same reason.

## Timestamps are epoch seconds, as both services publish them

Every Cognito timestamp carries the same sentence on its page: *"Amazon Cognito returns this timestamp
in UNIX epoch time format."* Both services speak AWS JSON, where a Timestamp is a JSON number, and
`API_AdminGetUser`'s Response Syntax says `"UserCreateDate": number` outright with a sample response of
`1.682955829578E9`. Substrate reported a Go `time.Time`, which marshals to an RFC3339 string, so the
SDK's epoch-seconds decoder was handed a string — a live decode failure rather than a cosmetic one
([#1305](https://github.com/scttfrdmn/substrate/issues/1305)). Nine members across seven sites moved
together in #756: `CreationDate` and `LastModifiedDate` on the pool and on `ListUserPools`' summary,
`CreationDate` on the app client and on a group, `UserCreateDate` and `UserLastModifiedDate` on a user
and on `AdminGetUser`, and `Credentials.Expiration` on `GetCredentialsForIdentity`. The three sites
that already projected correctly are included deliberately: a service answering the same published
member in two formats is worse than either end state.

The records keep their `time.Time`, and the conversion happens in the projection. That is
`emulator/ecr_wire.go`'s rule — retyping a persisted field changes the format of every recorded run,
because the state manager snapshots those bytes and a replay reads them back.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::Cognito::UserPool | UserPoolId | |
| AWS::Cognito::UserPoolClient | ClientId | |

## Cost

Cognito MAUs: first 50,000 free, then $0.0055 per MAU.
