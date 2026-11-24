# Secrets Manager

**Endpoint:** `secretsmanager.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: secretsmanager.{Op}`)

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateSecret | Tags are stored key-ordered, so two identical runs report them alike. A `ClientRequestToken` becomes the initial version's `VersionId`, and a retried create is idempotent — see [A version's identity is its `ClientRequestToken`](#a-version-s-identity-is-its-clientrequesttoken) |
| GetSecretValue | Returns `SecretString` or `SecretBinary`, whichever the version was given as, with its `VersionStages`; reads a version by `VersionId` or `VersionStage` — see [A secret's versions carry staging labels](#a-secret-s-versions-carry-staging-labels). Refuses a secret scheduled for deletion — see [A deleted secret is scheduled, not removed](#a-deleted-secret-is-scheduled-not-removed) |
| PutSecretValue | Creates a new version under the caller's `ClientRequestToken`; a resubmitted token is a no-op with the same value and `ResourceExistsException` with a different one — see [A version's identity is its `ClientRequestToken`](#a-version-s-identity-is-its-clientrequesttoken). Refuses a secret scheduled for deletion — see [A deleted secret is scheduled, not removed](#a-deleted-secret-is-scheduled-not-removed) |
| UpdateSecret | Rewrites `Description` and `KmsKeyId`; a value creates a new version under the caller's `ClientRequestToken`, and a token already in use is `ResourceExistsException` — see [A secret's versions carry staging labels](#a-secret-s-versions-carry-staging-labels). Answers `VersionId` only when it created a version. Refuses a secret scheduled for deletion — see [A deleted secret is scheduled, not removed](#a-deleted-secret-is-scheduled-not-removed) |
| DeleteSecret | Opens a 7-to-30-day recovery window, defaulting to 30, rather than removing the secret; `ForceDeleteWithoutRecovery` removes it — see [A deleted secret is scheduled, not removed](#a-deleted-secret-is-scheduled-not-removed) |
| RestoreSecret | Clears the `DeletionDate` and answers `ARN` and `Name` only |
| ListSecrets | Each entry carries the secret's metadata, including `Tags` and `SecretVersionsToStages`; honours `Filters`, `SortBy` and `SortOrder`, and leaves out secrets scheduled for deletion unless `IncludePlannedDeletion` is set — see [`ListSecrets` filters, sorts and hides pending deletions](/reference/ec2#listsecrets-filters-sorts-and-hides-pending-deletions). `NextToken` is a cursor on the last entry served, so a secret deleted or created mid-walk costs no other secret its place; `MaxResults` outside [the published range](/reference/concepts/listing-order#eight-json-list-operations-refuse-a-page-size-outside-the-published-range) is refused; scoped to the caller's account and Region |
| DescribeSecret | The read path for a secret's tags; reports only the members it has a value for, plus `DeletedDate` while a recovery window is open |
| ListSecretVersionIds | Every version with its staging labels; deprecated versions only with `IncludeDeprecated`; pages by `MaxResults`/`NextToken` — see [A secret's versions carry staging labels](#a-secret-s-versions-carry-staging-labels) |
| TagResource | Appends to the existing list rather than replacing it; refuses a secret scheduled for deletion — see [A deleted secret is scheduled, not removed](#a-deleted-secret-is-scheduled-not-removed) |
| UntagResource | Idempotent — an absent key is not an error — but a secret scheduled for deletion is refused even then, see [A deleted secret is scheduled, not removed](#a-deleted-secret-is-scheduled-not-removed) |
| RotateSecret | Records the rotation function and schedule and echoes `ClientRequestToken` as `VersionId`; no rotation function is executed, and a secret scheduled for deletion is refused — see [A rotation is configured, not run](#a-rotation-is-configured-not-run) |
| PutResourcePolicy | Validates and stores the policy; `BlockPublicPolicy` refuses a broad grant with `PublicPolicyException`. Stored, not enforced — see [A resource policy is stored, not enforced](/reference/ec2#a-resource-policy-is-stored-not-enforced) |
| GetResourcePolicy | Answers the stored text verbatim, and no `ResourcePolicy` member for a secret without one |
| DeleteResourcePolicy | Removes the policy; succeeds when none is attached |
| ValidateResourcePolicy | `PolicyValidationPassed` and `ValidationErrors` from the syntax and broad-access checks; `SecretId` is optional |

## A version's identity is its `ClientRequestToken`

`API_PutSecretValue`, `API_CreateSecret` and `API_RotateSecret` each say the request's
`ClientRequestToken` "becomes the `VersionId` of the new version", and all three publish it, like
`VersionId`, as 32 to 64 characters. Since
[#1285](https://github.com/scttfrdmn/substrate/issues/1285) all three use the token they were sent,
and `GetSecretValue` reaches the version by it. `PutSecretValue` and `CreateSecret` also honour the
idempotency contract both pages publish on the token:

| Request | Answer |
|---|---|
| A token not yet a version of the secret | A new version under that `VersionId` |
| A token naming an existing version, with the same `SecretString`/`SecretBinary` | `200` reporting that version; nothing is written and `AWSCURRENT` does not move |
| A token naming an existing version, with a different value | `ResourceExistsException`/400; the version keeps its value |
| A token shorter than 32 or longer than 64 characters | `InvalidParameterException`/400, nothing written |

For `CreateSecret` the retry case is a create of a name that already exists: the same token and
value answer the first create's `ARN`, `Name` and `VersionId`, and any other create of that name is
the name collision it always was. `ResourceExistsException` is the code each page publishes that
fits a version that "can't be modified"; `InvalidRequestException` is published too, but none of
its listed causes is this one.

With no token, substrate mints the version ID from the request ID (#856), so a replayed
`CreateSecret` or `PutSecretValue` reports the version the recording reported and the recorded
`GetSecretValue` naming it still answers its value. The minted rendering is a version-4 UUID, 36
characters: `VersionId` publishes a width and no pattern, and the UUID is what the pages recommend
for the token, what the CLI and SDKs generate, and the shape of every example version ID. Until
#1285 it was sixteen uppercase hex characters, half the published minimum.

`UpdateSecret` takes the token too, since
[#1376](https://github.com/scttfrdmn/substrate/issues/1376), but its page is stricter than the other
two: "If you call this operation with a `ClientRequestToken` that matches an existing version's
`VersionId`, the operation results in an error." So a token already in use is
`ResourceExistsException`/400 there whatever the value, with no idempotent retry. An update with no
value creates no version, its token names nothing, and its response carries no `VersionId`, as the
page's metadata-only examples answer.

A version holds either a `SecretString` or a `SecretBinary`, and the retry comparison counts the kind
as well as the bytes, so the same text sent once as each is two different values. A write naming both
is `InvalidParameterException`/400 ("Either `SecretString` or `SecretBinary` must have a value, but not
both"), and a `PutSecretValue` naming neither is too.

## A secret's versions carry staging labels

A secret holds every version it was given, and each version carries the staging labels the pages
describe ([#1376](https://github.com/scttfrdmn/substrate/issues/1376)). Until #1376 there was one
pointer, and `ListSecretVersionIds` reported only it.

| Write | Labels |
|---|---|
| `CreateSecret` with a value | The initial version gets `AWSCURRENT`. With no value there is no version |
| `PutSecretValue` without `VersionStages`, or `UpdateSecret` with a value | `AWSCURRENT` moves to the new version, and `AWSPREVIOUS` to the version it left |
| `PutSecretValue` with `VersionStages` | Each named label moves off the version that held it. Naming `AWSCURRENT` moves `AWSPREVIOUS` as above; not naming it leaves the current version current |

A version left with no label is deprecated: a third value takes `AWSPREVIOUS` from the first.
`ListSecretVersionIds` lists versions in creation order with `CreatedDate` and `VersionStages`, the
member absent for a deprecated version. It includes deprecated versions only with
`IncludeDeprecated`, as the page states. It pages by `MaxResults` (1–100; 100 when absent, substrate's
reading, since the page states no default) and `NextToken`, refusing an out-of-range size with
`InvalidParameterException` and an unissued token with `InvalidNextTokenException`, both 400.
`GetSecretValue` reads a version by `VersionStage`: a label no version carries is
`ResourceNotFoundException`, and a `VersionId` and `VersionStage` naming different versions is
`InvalidParameterException` (the page states the rule and publishes no code for it, so the code is
substrate's reading). It answers the version's own `CreatedDate`.

Not modeled: AWS's removal of versions beyond 100 and its 24-hour retention, `UpdateSecretVersionStage`,
and `KmsKeyIds` and `LastAccessedDate` on a listed version. A secret stored before #1376 carries no
version list; its one version is read from its current version ID, labeled `AWSCURRENT`, and its
first write moves the labels as above.

## A `SecretId` addresses the secret its own ARN names

`SecretId` is documented as "the ARN or name of the secret", and the two halves have
to be resolved differently. An ARN carries an account and a Region; a bare name
carries neither, so for a name — and only for a name — the caller's own account and
Region are the correct source.

Substrate read the caller's for both. The resolver split an identifier on `:` and
returned the **last** segment, and every one of the ten `SecretId`-taking operations
then keyed its load and its store by `ctx.AccountID` and `ctx.Region`. So
`arn:aws:secretsmanager:eu-west-1:999988887777:secret:db-password`, presented by a
`us-east-1` caller in another account, addressed **that caller's own** `db-password`:
`GetSecretValue` read its value, `DeleteSecret` deleted it, and `UntagResource`
answered `200` while stripping its tags. That is the rule #826 settled for SQS and
#928 applies here — the account and Region come from the ARN, never from the request
context.

Three further checks were missing and compounded it. The service segment was never
compared, so an ARN belonging to another service resolved to its own last segment as a
secret name. The type keyword was never compared either, so
`arn:aws:secretsmanager:{region}:{account}:other:foo` named a secret called `foo`;
`secret` is now matched as a whole segment rather than as a prefix, per #910, so
`secretpolicy` and any keyword AWS adds later are refused rather than accepted. And
the malformed case fell through to returning its argument, so an ARN with too few
segments became a *name* and was looked up as one. Anything beginning `arn:` that does
not parse is now `InvalidParameterException`, because a caller who wrote an ARN prefix
meant an ARN and a silent name lookup turns a typo into a `200` against the wrong
resource.

The separation is structural rather than a convention each handler has to remember:
the ARN parser takes no account, Region or request context **as parameters at all**,
which is the arrangement Step Functions (#910), CloudFront (#918), KMS (#922) and SNS
(#925) each settled on. The name is the whole remainder after the keyword, so a
hierarchical `prod/db/password` round-trips through its own ARN where taking the last
component would truncate it to `password`.

`DeleteSecret` had the same defect in its index: it removed the name from the
*caller's* account and Region index while deleting the record the ARN named, so the
owning Region went on listing a secret whose record had just been removed. Both now
address the target.

**Why the ARN-to-name derivation is exact, and what substrate deliberately does not
model.** AWS appends a hyphen and six random characters to a secret's name when it
mints the ARN, and warns "do not end your secret name with a hyphen followed by six
characters" — because a name may itself end that way, so trimming the suffix back off
cannot be done in general. Substrate mints no suffix, which is precisely what makes the
name recoverable from the ARN here. Adding one would make this resolver ambiguous by
construction and would change a value CloudFormation records as a physical ID, so it is
a separate decision rather than part of the fix.

## An absent secret is a `400`, and `DescribeSecret` reports only what it has a value for

**`ResourceNotFoundException` is `400`.** Substrate answered `404` at all nine
`SecretId`-taking operations until #930 — a status Secrets Manager publishes nowhere.
`API_DescribeSecret`'s error list is three codes long: `InternalServiceError` at `500`,
`InvalidParameterException` at `400`, and `ResourceNotFoundException` at **`400`**,
glossed "Secrets Manager can't find the resource that you asked for". Every other
operation that takes a `SecretId` publishes the same status. This is the defect ACM
carried at one site (#921) and KMS at fifteen (#923), and the fix is the same shape: one
constructor chooses the status, so a tenth operation added later cannot disagree with
the reference page by accident.

**`DescribeSecret` omits a member it has no value for — but AWS says two different
things, and the difference is observable.** The blanket rule is one sentence: "Secrets
Manager only returns fields that have a value in the response". The per-member text does
not distribute uniformly over it, so the members fall into three tiers, and substrate
keeps them apart deliberately:

| Tier | Members | Treatment |
|------|---------|-----------|
| AWS states "this field is omitted" | `DeletedDate`, `KmsKeyId`, `LastAccessedDate`, `RotationRules` | Absent — `DeletedDate` is emitted while a recovery window is open (#953), and `RotationRules` once `RotateSecret` has configured a schedule (#952) |
| AWS states "Secrets Manager returns null" | `LastRotatedDate`, `NextRotationDate`, `RotationEnabled` | Emitted as JSON `null` |
| AWS states nothing per member | `ARN`, `CreatedDate`, `Description`, `LastChangedDate`, `Name`, `Tags`, and the rest | Absent when empty — **substrate's reading**, on the blanket sentence alone |

`KmsKeyId` is omitted on AWS's own statement ("If the secret is encrypted with the AWS
managed key `aws/secretsmanager`, this field is omitted"). `Description` is omitted on
substrate's reading of the blanket sentence, which is a weaker basis and is recorded as
such rather than presented as matching AWS — #930's own criterion named it as though AWS
had stated it, and the page does not.

`RotationEnabled` needed no decision at all, because AWS published one: "If the secret
has never been configured for rotation, Secrets Manager returns null." So it is emitted
either way, as `null` before a `RotateSecret` and `true` after — never `false`, which is
a claim AWS does not make. Substrate needs no extra state to answer that correctly:
rotation is set by `RotateSecret` and by nothing else, and there is no
`CancelRotateSecret`, so "false" and "never configured" are the same condition.

Seven of the twenty-one published response members are absent because substrate models no
value for them, which the same sentence makes correct rather than a gap:
`LastAccessedDate`, `LastRotatedDate`, `NextRotationDate`,
`OwningService`, `PrimaryRegion`, `ReplicationStatus` and `VersionIdsToStages`.
`DeletedDate` left that list in #953 and `RotationLambdaARN` and `RotationRules` in #952 —
which is also what makes the count true, the list having named ten when it was nine. So
are the three managed-external-secret
members — `Type`, `ExternalSecretRotationRoleArn` and
`ExternalSecretRotationMetadata` — which belong to a partner integration substrate
models nothing of.

`LastRotatedDate` and `NextRotationDate` stay absent even now that a schedule is
recorded, and the reason is the boundary rather than the effort: both report when a
rotation *happened*, and no rotation happens here. AWS documents them as nulled rather
than omitted, so if either is ever modelled it belongs beside `RotationEnabled`.

**Tags.** `DescribeSecret` is the read path, because Secrets Manager publishes no
`ListTagsForResource`. Substrate answered one until #929 removed it: the API publishes
twenty-three operations and that is not among them, so the name now falls to
`UnknownOperationException`, which is what a caller reaching for it against real AWS
gets. An untagged secret omits the `Tags` member entirely rather than sending `null`.

## A deleted secret is scheduled, not removed

`DeleteSecret` does not delete a secret. AWS "attaches a `DeletionDate` stamp to the
secret that specifies the end of the recovery window", and only "at the end of the
recovery window" is the secret deleted permanently; the window is 7 to 30 days and
**defaults to 30**, and "at any time before recovery window ends, you can use
`RestoreSecret` to remove the `DeletionDate` and cancel the deletion of the secret".

Substrate removed the record, the version payload and the index entry on **every** call —
the one behaviour AWS reserves for `ForceDeleteWithoutRecovery: true` — and decoded
neither of the two parameters that choose between them. Three consequences followed
(#953). The destructive variant was the only variant, so a consumer could not test the
default path at all. `RestoreSecret` was not implemented, and had nothing to restore if it
had been. And a secret scheduled for deletion and a secret that never existed were the
**same observation**: both answered `ResourceNotFoundException`, so a caller's error
handling could not tell "restore this" from "this was never here".

A recovery window is in scope under the boundary in `doc.go`: it is a state transition
observable through an API call, stamped off the simulated clock, so a
schedule-then-restore path is assertable with no dependence on wall-clock time.

| Call | Answer |
|------|--------|
| `DeleteSecret` with neither parameter | `200`, `DeletionDate` 30 days out; the secret is still described, and listed when `ListSecrets` sets `IncludePlannedDeletion` |
| `DeleteSecret` with `RecoveryWindowInDays` 7–30 | `200`, `DeletionDate` that many days out |
| `DeleteSecret` with a window outside 7–30 | `InvalidParameterException`/`400`, nothing written |
| `DeleteSecret` with both parameters present | `InvalidParameterException`/`400`, nothing written |
| `DeleteSecret` on an already-scheduled secret, unforced | `InvalidRequestException`/`400` naming the scheduled-for-deletion cause |
| `DeleteSecret` with `ForceDeleteWithoutRecovery: true` | `200`; record, current version payload and index entry all removed |
| `DeleteSecret` forced, on an absent or already-deleted secret | `200` — see below |
| `GetSecretValue` on a scheduled secret | `InvalidRequestException`/`400`, distinguishable from `ResourceNotFoundException` |
| `PutSecretValue` on a scheduled secret | `InvalidRequestException`/`400`; no version is stored and `CurrentVersionID` does not move |
| `UpdateSecret` on a scheduled secret | `InvalidRequestException`/`400`; `Description`, `KmsKeyId` and the value are all unchanged |
| `TagResource` on a scheduled secret | `InvalidRequestException`/`400`; the tag list is unchanged |
| `UntagResource` on a scheduled secret | `InvalidRequestException`/`400`, **even when no named key is attached**; the tag list is unchanged |
| `RotateSecret` on a scheduled secret | `InvalidRequestException`/`400`; no rotation function or schedule is recorded |
| `PutResourcePolicy` or `DeleteResourcePolicy` on a scheduled secret | `InvalidRequestException`/`400`; the stored policy is unchanged |
| `DescribeSecret` on a scheduled secret | `200` with `DeletedDate`; the member is absent otherwise |
| `RestoreSecret` | `200` with `ARN` and `Name` only; the stamp is cleared and all nine refusals lift at once |

Four points where the reasoning is not simply AWS's prose:

**Refusing both parameters turns on their presence, which is substrate's reading.** AWS
states the exclusion twice, once under each parameter, and states it over *use*: "You
can't use both this parameter and `ForceDeleteWithoutRecovery` in the same call." So
`ForceDeleteWithoutRecovery: false` alongside a window is refused here too. The
alternative — treating an explicit `false` as an omission — would silently open a
30-day window for a caller who asked for an immediate delete, which is the more damaging
direction to guess in.

**A forced delete of a secret that is not there answers `200`, and the body is
substrate's reading.** The sentence is explicit — "if you forcibly delete an already
deleted or nonexistent secret, the operation does not return
`ResourceNotFoundException`" — but AWS publishes the suspension of the code without
publishing the body answered instead, so substrate reports the ARN the identifier names
and a `DeletionDate` at the request. This is the path a CloudFormation stack teardown
takes twice: substrate's own deleter has always sent `ForceDeleteWithoutRecovery: true`
for `AWS::SecretsManager::Secret`, citing "the default behavior of CloudFormation is to
delete the secret with the `ForceDeleteWithoutRecovery` flag" — a parameter nothing
decoded until #953, so that comment describes what the tree does only now.

**The stamp has two names, and substrate follows AWS rather than tidying.**
`DeleteSecret` answers it as `DeletionDate`; `DescribeSecret` reports the same value as
`DeletedDate`.

**The permanent deletion at the end of the window is deliberately not modelled.** That is
a decision, not a gap: AWS publishes no guarantee to model — "there is no guarantee of a
specific time after the recovery window for the permanent delete to occur" — so a secret
whose `DeletionDate` has passed is still reported, still stamped, still withholding its
value, and still restorable. A test asserting it had vanished at some simulated instant
would assert something AWS explicitly declines to promise. What is modelled is the stamp
and the refusals it causes.

`RestoreSecret` brings the `SecretId`-taking operations to ten, and it refuses an absent
secret through the same `400` constructor as the other nine, so it cannot disagree with
them. Restoring a secret that is **not** scheduled succeeds, which is substrate's
reading: `API_RestoreSecret` publishes `InvalidRequestException` but its cause list names
only the three conditions shared across the service, none of which is "not scheduled", so
there is no published code to refuse with — and inventing one would make an idempotent
restore fail here and succeed against AWS.

**Nine operations refuse the stamp, and one shared constructor builds all nine refusals.**
`GetSecretValue` and the unforced `DeleteSecret` came with the recovery window itself,
`RotateSecret` with #952, `PutSecretValue`, `UpdateSecret`, `TagResource` and
`UntagResource` with #956, and `PutResourcePolicy` and `DeleteResourcePolicy` with #1623 — each of whose pages publishes "The secret is scheduled for
deletion." as the first of `InvalidRequestException`'s three possible causes. Until then a
secret inside its recovery window could take a new version, have its description, KMS key
and value rewritten, and be retagged, so a consumer's "is this secret usable?" check got a
`200` here and a `400` from AWS at four operations, and a `RestoreSecret` afterwards
returned a secret carrying writes AWS would have refused. `RestoreSecret` is the one
operation publishing the cause that does not refuse on it, for the obvious reason: it is
what clears the stamp.

One refusal reads two AWS sentences against each other, and the resolution is substrate's.
`UntagResource` is documented idempotent — "if a requested tag is not attached to the
secret, no error is returned and the secret metadata is unchanged" — but that sentence is
about which *keys* are present, whereas `InvalidRequestException` reports "a parameter
value is not valid for the current state of the resource", which the stamp decides. So the
scheduled refusal outranks the idempotency, and an `UntagResource` naming no attached key
is still refused on a scheduled secret.

## A rotation is configured, not run

`RotateSecret` read one of its seven request parameters, omitted the response member all
four of AWS's samples return, and refused nothing. That was worse than a thin response:
the handler set rotation enabled on any secret it could load, so `DescribeSecret` reported
rotation configured for a secret with **no rotation function** — the one state AWS refuses
to create. A consumer's nominal path was substrate's nominal path, and its error path was
unreachable.

Substrate records which function a rotation would run and which schedule it would run on,
and runs neither. That is the boundary: a rotation function's execution is workload-internal
and out of scope, while *which* function and schedule a caller configured is observable
through `DescribeSecret` and so belongs here.

| Call | Answer |
|---|---|
| `RotateSecret` with a `ClientRequestToken` and a `RotationLambdaARN` | `200` with `ARN`, `Name` and `VersionId` |
| `RotateSecret` with no `ClientRequestToken` | `InvalidParameterException`/400, nothing written |
| A `ClientRequestToken` outside the published 32–64 characters | `InvalidParameterException`/400, nothing written (#1285) |
| `RotateSecret` on a secret with no rotation function, naming none | `InvalidRequestException`/400 naming the cause, nothing written |
| `RotateSecret` naming no function on a secret that already has one | `200` — the stored function is used and is not cleared |
| `RotationRules` with both `AutomaticallyAfterDays` and `ScheduleExpression` | `InvalidParameterException`/400, nothing written |
| `AutomaticallyAfterDays` outside 1–1000 | `InvalidParameterException`/400, nothing written |
| `RotateSecret` on a secret in its recovery window | `InvalidRequestException`/400 — distinguishable from `ResourceNotFoundException` |

**`VersionId` is derived from the request, not minted.** All four of AWS's examples state
it in the same words — "the `ClientRequestToken` field becomes the `VersionId` of the new
version created during the rotation" — and all four sample responses return the token that
was sent. So the value is reproducible on replay by construction rather than by a
determinism mechanism. No version *payload* is written for it: producing the new secret
value is the rotation function's job, and inventing one would be inventing a secret.

**An omitted `ClientRequestToken` is refused, and that is substrate's reading.** The
parameter is "Required: No" because "the CLI or SDK generates a random UUID for you", but
the page is explicit about the caller substrate is: "if you generate a raw HTTP request to
the Secrets Manager service endpoint, then you must generate a `ClientRequestToken` and
include it in the request." Minting one would put a nondeterministic value in a response
body, and echoing an empty one would answer a `VersionId` of `""` where AWS publishes a
minimum length of 32.

**Two of `InvalidRequestException`'s three published causes are refused.** The first, "the
secret is scheduled for deletion", became decidable when #953 modelled the recovery window.
The second, "you tried to enable rotation on a secret that doesn't already have a Lambda
function ARN configured and you didn't include such an ARN as a parameter in this call", is
decided from the record and the request together — which is why a bare call is refused on a
fresh secret and succeeds on one already configured. Note `RotationLambdaARN`'s published
length minimum of **0**: an explicit `""` is not naming an ARN, so it is refused too. The
third cause, "the secret is managed by another service", needs `OwningService`, which
substrate models nothing of, so it is not refused — recorded here rather than left silent.
The same absence is why the refusal is unconditional: a secret using AWS's *managed*
rotation legitimately has no ARN, but such a secret is an `OwningService` secret and none
can exist here.

The mutual exclusion inside `RotationRules` is AWS's rule — "in `RotateSecret`, you can set
the rotation schedule in `RotationRules` with `AutomaticallyAfterDays` or
`ScheduleExpression`, but not both" — while the **code** is substrate's reading, AWS
publishing none for the violation. It is refused on the parameters' presence, following
`DeleteSecret`'s precedent above and for the same reason: the alternative silently picks one
of two schedules the caller asked for.

Recorded rather than enforced: the length constraints on `ClientRequestToken` (32–64) and
`RotationLambdaARN` (≤2048), and the patterns on `Duration` (`[0-9]+h`) and
`ScheduleExpression`. Substrate does not validate parameter lengths or patterns as a rule,
and enforcing them at this one operation would refuse a token every sibling operation
accepts. `RotateImmediately` is recorded on the secret and reported by nothing, which is
not an omission — the difference AWS describes between `true` and `false` is whether the
function runs now or at the next window, and no function runs — but the state snapshot is
itself an observable under replay and time-travel inspection, so the intent survives.

`AWS::SecretsManager::RotationSchedule` passes the template's rotation function and
schedule through. It sent `SecretId` alone, which stopped being merely thin once the
refusal above existed: a template naming a `RotationLambdaARN` would have been refused for
naming none. The `ClientRequestToken` a deploy has no caller to supply is derived from the
account, Region, stack name and logical ID, so a redeploy sends the same token rather than a
fresh one. `HostedRotationLambda`'s `RotationLambdaName` is composed into a function ARN in
the deploying account and Region — **substrate's reading**, and the narrowest available: a
hosted rotation lambda is a function CloudFormation creates through a nested stack, which
substrate does not do, so nothing exists at that ARN. What the pass-through buys is a
deployable template instead of a failed resource.

## The account and Region a record carries reach no response

`SecretState` declares `AccountID` and `Region` under wire-visible `json` tags, plus `EverTagged` as
`ever_tagged,omitempty`, because the record is what `MemoryStateManager` snapshots and a replay reads
back. None of them reaches a body. `DescribeSecret` answers `smDescribeSecretBody`, which emits only
the members the page publishes, and every other operation answers a map of published members.
`TestSecretsManagerWire_SecretResponsesCarryNoBookkeepingMember` in
`emulator/secretsmanager_wire_test.go` drives all twelve routed operations
([#756](https://github.com/scttfrdmn/substrate/issues/756)). The secret is tagged first and
`ever_tagged` is read back, since only a tag write sets it (#938).

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::SecretsManager::Secret | SecretArn | |
| AWS::SecretsManager::RotationSchedule | the secret's ARN | Passes `RotationLambdaARN`, `RotationRules` and `RotateImmediatelyOnUpdate` through to `RotateSecret` with a derived `ClientRequestToken` |

## Cost

Secrets Manager API calls: $0.05 per 10,000 API calls.
