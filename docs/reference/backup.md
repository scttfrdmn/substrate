# AWS Backup

**Endpoint:** `backup.{region}.amazonaws.com`

**Protocol:** REST-JSON — the operation is the HTTP method plus the URL path, not an `X-Amz-Target`
header. API version 2018-11-15.

**Routing:** `parseBackupOperation` matches the path by whole segments, each literal at its published
position, and accepts one trailing slash, which several published URIs carry. Until
[#1205](https://github.com/scttfrdmn/substrate/issues/1205)'s sweep it tested prefixes and searched
for `/selections` as a substring, so `/backup-vaultsX` routed as a vault operation and
`/backup/plans/a/b/selections` read the plan ID as `a/b`. A path matching no published URI answers
`UnknownOperationException`/404. An empty identifier still reaches its operation's
"… is required" refusal.

Twelve operations over three resources: backup vaults, backup plans, and a plan's resource
selections. Every record is keyed by account and Region, so two accounts, or one account in two
Regions, never see each other's vaults or plans.

Nothing is ever backed up. A plan's `Rules` are stored verbatim as recorded intent and no schedule is
ever evaluated, so no backup job, recovery point or restore job exists and a vault's
`NumberOfRecoveryPoints` is `0` for its whole life. That is the boundary in `doc.go`: the API
observation is modelled, the work behind it is not.

The published path is given for every operation because one of them cannot be reached over it.

## Supported operations

| Operation | Published path | Notes |
|-----------|----------------|-------|
| CreateBackupVault | `PUT /backup-vaults/{backupVaultName}` | Routed on the published verb. Answers exactly the three published members. `BackupVaultTags` and `CreatorRequestId` are not read, so a create-time tag set is dropped, and `EncryptionKeyArn` is echoed without the KMS key having to exist |
| DescribeBackupVault | `GET /backup-vaults/{backupVaultName}` | [Nine of the seventeen published members](#the-backup-vault-is-projected-onto-the-published-shape), including `VaultState`, `VaultType` and `Locked`, and nothing Substrate does not publish |
| DeleteBackupVault | `DELETE /backup-vaults/{backupVaultName}` | Answers `{}`, which is the published empty body. Its published precondition [cannot fail here](#which-backup-preconditions-are-enforced) |
| ListBackupVaults | `GET /backup-vaults/` | `BackupVaultList` of [nine of the thirteen published `BackupVaultListMember` members](#the-backup-vault-is-projected-onto-the-published-shape) per vault. [Pages by `maxResults` and `nextToken`, and applies `vaultType` and `shared`](#listbackupvaults-pages-and-filters); a bad value is `InvalidParameterValueException`/400 |
| CreateBackupPlan | `PUT /backup/plans/` | [Routed on the published verb only](#the-backup-plan-operations-are-routed-on-their-published-verbs). `BackupPlanName` is required; `Rules` are stored unvalidated and `AdvancedBackupSettings` is not read. A repeated `CreatorRequestId` returns the existing plan, as published (#1173). The plan ARN uses the [published `plan` segment](#arn-shapes) |
| GetBackupPlan | `GET /backup/plans/{backupPlanId}/` | [Reachable over that path since #1176](#getbackupplan-is-reachable-over-its-published-path); `versionId` and `MaxScheduledRunsPreview` are not read |
| UpdateBackupPlan | `POST /backup/plans/{backupPlanId}` | Routed on the published verb, but [merges where AWS replaces and answers members no page publishes](#two-backup-plan-responses-carry-the-wrong-members) |
| DeleteBackupPlan | `DELETE /backup/plans/{backupPlanId}` | Answers the [four published members](#two-backup-plan-responses-carry-the-wrong-members) — `BackupPlanArn`, `BackupPlanId`, `DeletionDate`, `VersionId`. A plan that still has selections is [refused](#which-backup-preconditions-are-enforced) with `InvalidRequestException`/400 |
| ListBackupPlans | `GET /backup/plans/` | Five of the nine published `BackupPlansListMember` members per plan; [pages by `maxResults` and `nextToken`](#listbackupplans-pages), and `includeDeleted` is accepted but lists nothing more |
| CreateBackupSelection | `PUT /backup/plans/{backupPlanId}/selections/` | [Routed on the published verb only](#the-backup-plan-operations-are-routed-on-their-published-verbs); refuses an unknown plan. `SelectionName` is required; `Conditions`, `ListOfTags` and `NotResources` are not read |
| ListBackupSelections | `GET /backup/plans/{backupPlanId}/selections/` | `BackupSelectionsList` of the six published `BackupSelectionsListMember` members per selection — `IamRoleArn` and `CreatorRequestId` when the create sent them. Pages by `maxResults` (1 to 1000, the default 1000) and `nextToken`; a bad value is `InvalidParameterValueException`/400, and an unknown plan `ResourceNotFoundException`/400 (#1408) |
| GetBackupSelection | `GET /backup/plans/{backupPlanId}/selections/{selectionId}` | Answers `BackupPlanId`, `SelectionId`, `CreationDate` and a three-member `BackupSelection`. A selection whose plan is gone is `ResourceNotFoundException` |
| DeleteBackupSelection | `DELETE /backup/plans/{backupPlanId}/selections/{selectionId}` | Answers `{}`, which is the published empty body. A selection whose plan is gone is `ResourceNotFoundException` |

Every other AWS Backup operation is unrouted, including the whole job surface —
`StartBackupJob`, `DescribeBackupJob`, `ListBackupJobs`, `StartRestoreJob`,
`ListRecoveryPointsByBackupVault` — as well as `ListBackupPlanVersions`,
`PutBackupVaultAccessPolicy`, `PutBackupVaultLockConfiguration`, `GetBackupPlanFromJSON` and the
three tag operations. No Backup resource is scanned by the Resource Groups Tagging API either, so a
vault or plan cannot be found by tag.

A create writes its resource and then adds it to a name or ID index. A failed index write fails
the create, so a create that reports success is always listed by `ListBackupVaults` or
`ListBackupPlans` ([#1175](https://github.com/scttfrdmn/substrate/issues/1175)).

## GetBackupPlan is reachable over its published path

`API_GetBackupPlan` publishes `GET /backup/plans/{backupPlanId}/?…` — with a trailing slash before
the query string. Until [#1176](https://github.com/scttfrdmn/substrate/issues/1176) the router treated
everything after `/backup/plans/` as the plan ID, so an SDK built from the model asked for the ID
`abc/` and a plan that `ListBackupPlans` reported answered `ResourceNotFoundException`. The segment
matcher (#1205) drops one trailing slash, so the published form reaches the plan, the slash-less form
still does, and `GET /backup/plans/` is still `ListBackupPlans`. `TestBackupRouting_ANearMissPathIsUnknown`
sends the published form verbatim, `?versionId=` included. The two creates publish a trailing slash too,
and the slash is dropped there as well.

`versionId` is unread for a structural reason rather than an oversight: one record is kept per plan
and `UpdateBackupPlan` overwrites it, so no previous version exists to fetch.

## The backup plan operations are routed on their published verbs

`API_CreateBackupPlan` publishes `PUT /backup/plans/` and `API_CreateBackupSelection` publishes
`PUT /backup/plans/{backupPlanId}/selections/`. Until
[#1172](https://github.com/scttfrdmn/substrate/issues/1172) both were routed on `POST` and nothing
routed the published `PUT`, so every SDK's create was refused as an unknown route and no plan could
exist. Both now answer on `PUT`, as `CreateBackupVault` already did.

The `POST` spellings were dropped rather than kept as a substrate-only convenience: no SDK sends them,
and keeping them would let a test written against the wrong verb keep passing. A `POST` on either
collection is now `UnknownOperationException`. `UpdateBackupPlan` is routed from its own page,
`POST /backup/plans/{backupPlanId}`, and only on a plan ID, not on the collection; a `PUT` on a plan ID
is unknown too.

## Two backup plan responses carry the wrong members

`UpdateBackupPlan` answered `BackupPlanId`, `BackupPlanArn`, `VersionId` and an `UpdatedAt` that is on
no AWS Backup page, while `CreationDate`, published and already held on the stored record, was
absent. It now answers the four published members the record holds: `BackupPlanArn`, `BackupPlanId`,
the plan's own `CreationDate` and the new `VersionId`. `AdvancedBackupSettings` and `ScanSettings` are
absent, because the record carries neither. `DeleteBackupPlan` answered `{}` where the page publishes `BackupPlanArn`, `BackupPlanId`,
`DeletionDate` and `VersionId`. `VersionId` is the only handle on the version that was deleted, so the
member identifying what happened was the one missing. It answers all four now, from the plan it
deletes, with `DeletionDate` as Unix seconds like every Backup date. #1206's survey found it, and it
is #1177's other half. `DeleteBackupVault` and `DeleteBackupSelection` publish *"an HTTP 200 response
with an empty HTTP body"*, so their `{}` is faithful.
[#1177](https://github.com/scttfrdmn/substrate/issues/1177).

`UpdateBackupPlan` also merges where AWS replaces. `BackupPlan` is `Required: Yes` and describes the
plan in full, but an omitted `BackupPlanName` or `Rules` leaves the stored value in place, and an
empty body updates nothing while still minting a new `VersionId` — so an update that drops a rule
does not drop it here.

## Which backup preconditions are enforced

`API_DeleteBackupPlan` opens with *"A backup plan can only be deleted after all associated selections
of resources have been deleted."* That is enforced: `DeleteBackupPlan` on a plan that still has a
selection is refused, and changes nothing. The page names no code for the condition. Of the codes it
publishes, `InvalidRequestException` ("something is wrong with the input to the request", HTTP 400)
is the only one that describes it, since the request is well-formed and names a plan that exists, so
that is the code. The message is substrate's own. The published teardown order, deleting the
selections and then the plan, succeeds end to end
([#1178](https://github.com/scttfrdmn/substrate/issues/1178)).

A selection is reachable only while its plan exists. `GetBackupSelection` and `DeleteBackupSelection`
load the plan first, so a selection whose plan is gone answers `ResourceNotFoundException`, including
one an older Substrate left behind when it deleted a plan without checking. Before #1178, such a
selection answered HTTP 200, reporting a plan ID no other operation would accept.

`API_DeleteBackupVault`'s mirror precondition — *"A vault can be deleted only if it is empty"* — is
**vacuous** rather than unenforced. No operation creates a recovery point, so
`NumberOfRecoveryPoints` is `0` for a vault's whole life and the condition cannot fail.

## The backup vault is projected onto the published shape

`DescribeBackupVault` and `ListBackupVaults` answer a `backupVaultOut`
(`emulator/backup_wire.go`) rather than the persisted `BackupVault`, so `AccountID` and `Region` —
Substrate's own bookkeeping — reach no response
([#756](https://github.com/scttfrdmn/substrate/issues/756)). Both fields stay on the record: a
`json:"-"` would change the format of every run already recorded, because the state snapshot holds
those bytes and a replay reads them back.

One wire type serves both sites, because every member it answers is in both
`API_DescribeBackupVault`'s seventeen and `API_BackupVaultListMember`'s thirteen. Until
[#1199](https://github.com/scttfrdmn/substrate/issues/1199) it answered five of them. It answers nine now:

| Member | Answered |
|--------|----------|
| `BackupVaultName`, `BackupVaultArn`, `CreationDate`, `NumberOfRecoveryPoints` | Always |
| `EncryptionKeyArn` | When the create named one |
| `CreatorRequestId` | When the create sent one. `CreateBackupVault` now reads it, refusing a value outside the published "1 to 50 alphanumeric or '-_.' characters" with `InvalidParameterValueException`/400 |
| `VaultState` | Always `AVAILABLE`: a vault is usable the moment the create returns, and is never `CREATING` or `FAILED` |
| `VaultType` | Always `BACKUP_VAULT`: `CreateLogicallyAirGappedBackupVault` and `CreateRestoreAccessBackupVault` are unrouted |
| `Locked` | Always `false`: `PutBackupVaultLockConfiguration` is unrouted, so no vault is ever locked |

`VaultState` is the only published signal that a vault is anything but available, and `Locked` the
one a consumer checking Vault Lock reads, so both are worth answering as the truths they are rather
than leaving a consumer to read an absent member as `false` by accident. The rest are absent because
no vault ever has a value for them:
- **`LockDate`, `MinRetentionDays`, `MaxRetentionDays`:** the Vault Lock settings, which are unset
  when no lock is configured, and none can be.
- **`EncryptionKeyType`:** whether a key is customer-managed or AWS-owned depends on the key, which
  the record does not hold. A vault created with no key would report AWS's default backup key, which
  Substrate does not mint. Either value would be a guess.
- **`SourceBackupVaultArn` and the four multi-party-approval members:** they belong to restore-access
  vaults and to MPA, neither of which is routed.

A second create of the same name answers `AlreadyExistsException`, unless it carries the
`CreatorRequestId` the vault was created with. That retry answers the first create's response; see
*What a refusal reports* below (#1173). `CreateBackupVault` is deliberately not projected
through `backupVaultOut`: `API_CreateBackupVault` publishes three members and no more, so it keeps
its own map. The plan and selection handlers build their responses member by member, so no Backup
record reaches the wire whole.

## ListBackupVaults pages and filters

`API_ListBackupVaults` publishes four query parameters. Until #1199's audit none was read: the list
was one page with no `NextToken`, and both filters matched every vault.

- **`maxResults`** (Valid Range 1–1000) bounds the page, and with none the page size is the published
  maximum of 1000. A value outside the range, or one that is not an integer, is
  `InvalidParameterValueException`/400, which the page glosses "the value is out of range".
- **`nextToken`** is an opaque offset over the vault index, which is kept sorted by name, so a walk
  sees every vault once. It is omitted, not emitted empty, on the last page. A token Substrate did not
  issue is `InvalidParameterValueException`, rather than read as page one (#915).
- **`vaultType`** must be one of its published Valid Values. Every vault is a `BACKUP_VAULT`, so that
  value matches every vault and the other two match none.
- **`shared`** must be `true` or `false` (in any case; `1`, `t` and the like are refused). The page glosses it as sorting "the list of vaults by shared
  vaults"; the CLI and SDKs use it to list the vaults shared with the caller. Substrate shares no vault
  across accounts, so `true` answers an empty list and `false` the full one.

A vault whose record cannot be read is an error rather than skipped, since a skipped vault would
shorten the list and shift every later offset. The page is cut from the sorted name index before any
record is loaded, so a listing loads only the vaults it returns.

## ListBackupPlans pages

[#1184](https://github.com/scttfrdmn/substrate/issues/1184). `API_ListBackupPlans` publishes
`includeDeleted`, `maxResults` and `nextToken`, and the handler took no request at all. It now reads
all three, with the vault listing's rules:

- **`maxResults`** (Valid Range 1–1000) bounds the page; with none it is 1000, so a request with no
  pagination parameters answers what it did. Outside the range, or not an integer, is
  `InvalidParameterValueException`/400.
- **`nextToken`** is an offset over the plan-ID index, which is kept sorted. It is omitted on the last
  page, and one substrate did not issue is `InvalidParameterValueException` — the only validation code
  the page publishes — rather than page one.
- **`includeDeleted`** must be `true` or `false` (in any case), but **`true` lists the same plans as `false`**:
  `DeleteBackupPlan` removes the record, so substrate keeps no deleted plan to return, where AWS would
  answer one with its `DeletionDate`.

A plan whose record cannot be read or decoded fails the listing; it used to be skipped. All three Backup
listings — vaults, plans and selections — share one validation rule (`emulator/backup_list_params.go`)
and cut the page from the sorted index before loading any record.

## The account and Region a record carries reach no response

Each of the three persisted records — `BackupVault`, `BackupPlan` and `BackupSelection` — declares
`AccountID` and `Region` under wire-visible `json` tags, because the record is what
`MemoryStateManager` snapshots and a replay reads back. No Backup shape publishes either, and neither
reaches a body: the vault answers through `backupVaultOut` (`emulator/backup_wire.go`), and every
plan and selection response is a `map[string]interface{}` built member by member, so the map is the
projection. Every `json.Marshal` of a record in the plugin is in a create handler writing to state.

All **twelve** routed operations are driven by
`TestBackupWire_VaultResponsesCarryNoBookkeepingMember` and its two siblings in
`emulator/backup_wire_test.go`, which walk the decoded document and fail on a member named for either
at any depth — so the six entries this discharges in `scripts/wire-bookkeeping-baseline.txt` stay
listed as declarations rather than as leaks
([#756](https://github.com/scttfrdmn/substrate/issues/756)). The assertion is on the member *name*,
because the account and the Region appear in every body as segments of a `backup` ARN, which is
published; and each case asserts a published member is *present* before asserting a bookkeeping one is
absent, so a response that failed to render the record at all fails as a missing anchor rather than
passing as an absence.

Neither member carries `,omitempty` on any of the three records and no Backup record declares
`EverTagged` — Backup routes no tag operation — so the vacuous-absence trap that governs the taggable
services (#1304, #938) does not arise here. The three deletes answer `{}` and read nothing off the
record they address; they are driven anyway, because the set of twelve should read as complete rather
than as a sample, and a control confirms the walk catches a leak added at each of the three.

## Every published date is a Unix timestamp

Backup publishes no RFC3339 date. Each page glosses its date as *"in Unix format and Coordinated
Universal Time (UTC) … accurate to milliseconds. For example, the value 1516925490.087"*, and all
nine sites that answer one render it that way, to exactly three decimals
([#1324](https://github.com/scttfrdmn/substrate/issues/1324)). `UpdateBackupPlan` is the ninth: it
answers the plan's `CreationDate` now, where it answered an RFC3339 `UpdatedAt` that
[no Backup page publishes](#two-backup-plan-responses-carry-the-wrong-members) (#1177).

## What a refusal reports

| Condition | Code | Status |
|-----------|------|--------|
| a body that will not parse | `InvalidRequestException` | 400 |
| `BackupVaultName`, `BackupPlanName` or `SelectionName` absent | `MissingParameterValueException` | 400 |
| a `CreatorRequestId` outside 1–50 alphanumeric or `-_.` characters | `InvalidParameterValueException` | 400 |
| a vault name already in use, without the vault's own `CreatorRequestId` | `AlreadyExistsException` | 400 |
| a vault, plan or selection that does not exist | `ResourceNotFoundException` | 400 |

Every routed Backup page publishes `ResourceNotFoundException` and `MissingParameterValueException`,
both at **400**. Each of the twelve was fetched for this table. Until
[#1390](https://github.com/scttfrdmn/substrate/issues/1390) and
[#1173](https://github.com/scttfrdmn/substrate/issues/1173), a missing resource answered 404, and a
missing member answered `InvalidRequestException`, which only the vault and plan delete pages list,
for input that is wrong rather than absent. The empty-identifier branches inside the three loaders are
unreachable through the router, since a request with no vault name, plan ID or selection ID routes to
the list or create operation instead, so they are not in the table.

The required members still not checked are recorded rather than enforced:
- `BackupPlanInput`'s `Rules`, and each `BackupRuleInput`'s `RuleName` and `TargetBackupVaultName`;
- `BackupSelection`'s `IamRoleArn`.

A create without them answers 200.

**`CreatorRequestId` is the idempotency key on all three creates.** `API_CreateBackupPlan` states it:
*"If the request includes a `CreatorRequestId` that matches an existing backup plan, that plan is
returned."* The vault and selection pages gloss the member the same way, as what lets a failed request
be retried without running it twice. A retry carrying a recorded value answers the resource it first
made, and `GetBackupPlan`, `ListBackupPlans` and `GetBackupSelection` answer the recorded value.

`InvalidParameterValueException`, `LimitExceededException` and `ServiceUnavailableException`/500 are
published across these pages and have no site here: Substrate enforces no vault or plan quota and has
no transient failure to report.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| `AWS::Backup::BackupPlan` | the plan ID | Deployed through `CreateBackupPlan`, so `GetBackupPlan` and `ListBackupPlans` report the stack's plan ([#1428](https://github.com/scttfrdmn/substrate/issues/1428)). The template's `BackupPlanRule` is sent as the API's `Rules`, and a rule's `TargetBackupVault` as `TargetBackupVaultName`; every other member has the same name on both pages and is sent as declared. `BackupPlan` is `Required: Yes`, and a template without it fails the resource. `Ref` and `Fn::GetAtt BackupPlanId` return the plan ID the plugin minted, `VersionId` the version `CreateBackupPlan` answered, and `BackupPlanArn` the response's ARN, with the published `plan` segment (#1181). An `UpdateStack` keeps the ID: an unchanged plan makes no call, and a changed `BackupPlan` goes through `UpdateBackupPlan`, which answers a new `VersionId`. A change to `BackupPlanTags` alone makes no call, since Backup's `TagResource` is not routed |
| `AWS::Backup::BackupSelection` | the selection ID | Deployed through `CreateBackupSelection` on the plan `BackupPlanId` names, with `BackupSelection` sent as declared — the template's and the API's members have the same names ([#1428](https://github.com/scttfrdmn/substrate/issues/1428)). `Fn::GetAtt BackupPlanId` and `SelectionId` are the response's own members; `Id` is the same selection ID, since the API has no second identifier for a selection (substrate's reading). Both properties are *Update requires: Replacement*, so a changed selection is a new one and the old one is deleted once the update succeeds. A stack delete removes the selection before the plan — the selection's `Ref` to the plan orders it — which `DeleteBackupPlan` requires |

`AWS::Backup::BackupVault` is not deployed.

## ARN shapes

| Resource | Substrate | Published |
|----------|-----------|-----------|
| vault | `arn:aws:backup:{region}:{account}:backup-vault:{name}` | the same |
| plan | `arn:aws:backup:{region}:{account}:plan:{planId}` | the same |
| selection | none — a selection carries no ARN | AWS publishes none either |

The two segments really are spelled differently by the same service: `backup-vault` for a vault
(`API_CreateBackupVault`'s `arn:aws:backup:us-east-1:123456789012:backup-vault:aBackupVault`) and `plan`
for a plan (`API_CreateBackupPlan`'s and `AWS::Backup::BackupPlan`'s
`arn:aws:backup:us-east-1:123456789012:plan:8F81F553-…`), each published as a worked example rather than
as a format string. Substrate spelled a plan's segment `backup-plan` until
[#1181](https://github.com/scttfrdmn/substrate/issues/1181), so an IAM policy or an ARN parser written
against it matched nothing on AWS. The ARN is derived from the plan's ID when it is read, so a plan
stored before the fix, and a CloudFormation stack record holding one, report the published form. A stack
record's outputs are rewritten too, so an output that resolved `Fn::GetAtt BackupPlanArn`, an export
built from one, and an importing stack's output that resolved it all report `plan`.

The published example differs in one more way the table does not show: AWS's sample is
`arn:aws:backup:us-east-1:123456789012:plan:8F81F553-3A74-4A3F-B93D-B3360DC80C50`, an **uppercase**
UUID, where Substrate renders lowercase. `BackupPlanId` publishes no pattern and no length, so this is
observed from the example rather than required by the model, and it is a rendering question rather than
a derivation one — the plan ID is [derived from the request ID](/reference/concepts/seeds-and-replay#an-identifier-a-replay-mints-is-the-one-it-recorded) either way.

## Cost

`CreateBackupPlan` is attributed $0.000001 per call. Real AWS Backup charges for protected storage
and for restores, not for creating a plan; Substrate stores no backups, so the attribution stands in
for a plan's existence rather than for anything AWS would bill.
