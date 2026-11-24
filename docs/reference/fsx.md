# FSx

**Endpoint:** `fsx.{region}.amazonaws.com`

**Protocol:** JSON 1.1 with an `X-Amz-Target` header. The target namespace is
`AWSSimbaAPIService_v20180301`, the internal name the FSx model carries rather than anything the
public reference prints, and Substrate's parser maps that namespace onto the `fsx` service. API
version 2018-03-01.

**Routing:** on the target's operation name alone; a target Substrate does not route, and an absent
target, are both refused as an unrecognised action.

Three operations over one resource: a file system. Nothing is mounted and no storage exists — a file
system is a record with a DNS name — and the Lustre mount name a `SCRATCH_1` file system reports is
the literal `fsx`, which `API_LustreFileSystemConfiguration` publishes for that deployment type.
`SCRATCH_1` is also the published default. Every other deployment type reports a minted mount name
of eight lowercase hex characters, inside the published `^([A-Za-z0-9_-]{1,8})$`
([#1204](https://github.com/scttfrdmn/substrate/issues/1204)). Until then the default was
`SCRATCH_2`, the constant went to that type, and the minted name was sixteen characters, twice the
maximum. A file system's VPC is
derived by looking its first subnet up in EC2 state, so a file system created against a subnet
Substrate has never seen reports an empty `VpcId`.

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateFileSystem | Answers `{"FileSystem"}` as published. [Both `Required: Yes` members are required, and `FileSystemType` and `StorageType` must be published values](#createfilesystem-requires-filesystemtype-and-subnetids), each refused `BadRequest`/400; [the file system is `AVAILABLE` immediately](#a-new-file-system-is-creating-and-its-lifecycle-progresses-under-a-seed); [`ClientRequestToken` makes a retry answer the file system it first created](#clientrequesttoken-makes-create-and-delete-idempotent) |
| DescribeFileSystems | Describes the IDs given, or every non-deleted file system when `FileSystemIds` is absent, which is what the page publishes. [Pages by `MaxResults` and `NextToken`, at most 50 per page, and refuses an unissued token](#describefilesystems-pages-by-maxresults-and-nexttoken) `BadRequest`/400 |
| DeleteFileSystem | [Answers the published flat shape with `Lifecycle` `DELETING`, and removes the file system](#deletefilesystem-answers-the-published-flat-shape-and-reports-deleting); [`ClientRequestToken` makes a retry answer the first delete](#clientrequesttoken-makes-create-and-delete-idempotent) |

Everything else on the FSx API is unrouted: `CreateFileSystemFromBackup` and `UpdateFileSystem`, the
backup surface (`CreateBackup`, `CopyBackup`, `DeleteBackup`, `DescribeBackups`), the volume and
storage-virtual-machine surfaces used by ONTAP and OpenZFS, the snapshot surface, the data-repository
association and task surfaces used by Lustre, the alias operations, and the three tag operations. A
call to any of them is refused as an unrecognised action, so an FSx file system here cannot be backed
up, restored, resized, or read through a data repository — and because `ListTagsForResource` is
unrouted, a file system's tags can only be read back inside the file system record itself.

## DeleteFileSystem answers the published flat shape and reports DELETING

`API_DeleteFileSystem` publishes a flat response: `FileSystemId`, `Lifecycle`, and the per-type
`LustreResponse`, `OpenZFSResponse` or `WindowsResponse`. Until
[#1210](https://github.com/scttfrdmn/substrate/issues/1210), Substrate answered the whole record under
a `FileSystem` key, the `CreateFileSystem` shape, which an SDK's `DeleteFileSystemOutput` decodes as
empty, and reported `DELETED`, which is on no FSx page. It now answers the flat shape, with
`Lifecycle` `DELETING`, as the page publishes: "If the `DeleteFileSystem` operation is successful,
this status is `DELETING`." (#1210 named the member `LifecycleStatus`; the page names it `Lifecycle`,
and the page is what this follows.)

The per-type object carries `FinalBackupId` and `FinalBackupTags`. Substrate models no backup, so
`FinalBackupId` is never answered. `WindowsResponse` is answered for a `WINDOWS` file system, the one
type whose delete takes a final backup by default. `LustreResponse` and `OpenZFSResponse` are answered
when the request carried that type's configuration. Each echoes the `FinalBackupTags` it was sent.

**A deleted file system is removed — at once, or after a seeded deleting window.** The page says the
delete "returns while the file system has the `DELETING` status", and that `DescribeFileSystems` on a
deleted ID "returns a `FileSystemNotFound` error". Unseeded, Substrate removes the record in the
delete, so the next describe answers `FileSystemNotFound`, and an SDK's deletion waiter completes on
its first poll; a second delete answers `FileSystemNotFound` too. Under a file-system seed with
observations to spend (see the next section), the delete keeps the record marked `DELETING` and
restarts its countdown: the next `pendingObservations` describes report `DELETING`, and the one after
completes the delete and answers `FileSystemNotFound`. A delete of a file system already `DELETING`
answers `DELETING` again and leaves the window running
([#1196](https://github.com/scttfrdmn/substrate/issues/1196)).

A record a recording made before #1210 soft-deleted with `DELETED` still reads back as absent.

## A new file system is CREATING, and its lifecycle progresses under a seed

`API_CreateFileSystem` publishes that it "Creates a new, empty Amazon FSx file system with an assigned
ID, and an initial lifecycle state of `CREATING`", and that "The `CreateFileSystem` call returns while
the file system's lifecycle state is still `CREATING`." `CreateFileSystem` answers `CREATING`, whatever
is seeded; the record itself is the settled file system. `DescribeFileSystems` reports the lifecycle
through a seed ([How a progression is seeded](/reference/concepts/seeds-and-replay#how-a-progression-is-seeded)):

```
POST   /v1/fsx/file-system-status
       {"fileSystemId":"fs-…"|"*","pendingObservations":2,"state":"CREATING","finalState":"FAILED",
        "failureMessage":"…"}
DELETE /v1/fsx/file-system-status                  (all; ?fileSystemId=… for one)
```

For `pendingObservations` describes the file system reports `state` (default `CREATING`; `UPDATING`
may be named), then `finalState`: the record's `AVAILABLE` by default, or `FAILED`, `MISCONFIGURED` or
`MISCONFIGURED_UNAVAILABLE`, with `FailureDetails.Message` when the seed gives `failureMessage`. The
same seed governs the deleting window above. Every one of `FileSystem`'s seven published `Lifecycle`
values is reachable. Unseeded, a file system reads `AVAILABLE` from its first describe, as before, so
the SDK's file-system-available waiter still succeeds on its first poll. A describe listing several
file systems observes only the ones on the page it answers
([#1196](https://github.com/scttfrdmn/substrate/issues/1196)).

## CreateFileSystem requires FileSystemType and SubnetIds

`FileSystemType` is `Required: Yes` with `Valid Values: WINDOWS | LUSTRE | ONTAP | OPENZFS`, and
`SubnetIds` is `Required: Yes`. Until [#1197](https://github.com/scttfrdmn/substrate/issues/1197) Substrate defaulted the first to `LUSTRE` and
accepted an absent second, so a template or SDK call missing a required member deployed clean here and
was refused by AWS. Each is checked now, before any other member is used, and refused with
`BadRequest`/400, "A generic error indicating a failure with a client request.", the first entry in
the operation's own Errors section:

| Request | Message |
|---------|---------|
| No `FileSystemType` | `FileSystemType is required.` |
| A `FileSystemType` outside the enum, including a lower-case `lustre` (the handler used to upper-case it) | `FileSystemType "…" is not one of WINDOWS, LUSTRE, ONTAP or OPENZFS.` |
| A `StorageType` outside `SSD \| HDD \| INTELLIGENT_TIERING` | `StorageType "…" is not one of SSD, HDD or INTELLIGENT_TIERING.` |
| No `SubnetIds`, or an empty list | `SubnetIds is required.` |
| More than the published 50 `SubnetIds` | `SubnetIds may name at most 50 subnets.` |

`StorageType` is `Required: No` and still defaults to `SSD`, as the page says. A CloudFormation
template omitting `SubnetIds` now fails its `AWS::FSx::FileSystem` resource with that message.

Two published constraints are deliberately **not** enforced, recorded in `fsxValidateCreate`'s doc
comment:
- **The `SubnetIds` pattern** `^(subnet-[0-9a-f]{8,})$`. Substrate does not require a subnet to exist
  in EC2 state (the VPC lookup is best effort), and fixtures and templates name symbolic subnets such
  as `subnet-a`. Refusing those would not make the subnet any more real; the presence check is what
  catches the omission a generated template makes.
- **`StorageCapacity`'s per-type values**, such as "1200 GiB, 2400 GiB, and increments of 2400 GiB"
  for `SCRATCH_2`. They depend on the deployment type and storage type in ways the page states only
  in prose. The member is `Required: No`, and is reported as `0` when absent.

## ClientRequestToken makes create and delete idempotent

`API_CreateFileSystem` publishes the contract: "If a file system with the specified client request
token exists and the parameters match, `CreateFileSystem` returns the description of the existing file
system. If a file system with the specified client request token exists and the parameters don't
match, this call returns `IncompatibleParameterError`." `API_DeleteFileSystem` publishes the same token
"to ensure idempotent deletion", and the same `IncompatibleParameterError`/400. Until
[#1210](https://github.com/scttfrdmn/substrate/issues/1210) neither token was read, so a retried
create made a second file system.

Both are honoured now. A token is recorded per account, Region and operation, with a fingerprint of
every other request member.
- **A retried create** with the same parameters answers the file system the first create made, in its
  current state. If that file system has been deleted since, the retry answers `FileSystemNotFound`.
- **A retried delete** answers the first delete's response, although the file system is gone.
- **Different parameters** under a used token answer `IncompatibleParameterError`/400.
- **A token outside the published constraint** (1 to 63 characters matching `[A-za-z0-9_.-]`, the
  class as published) is refused `BadRequest`/400.

The tokens are state, so they replay with everything else. An SDK fills the token in on every call,
so a consumer's retry-on-timeout path, the case the token exists for, now observes the published
replay.

## DescribeFileSystems pages by MaxResults and NextToken

`MaxResults` and `NextToken` are published request members, `NextToken` is a published response
member, and the page describes the loop in full: "`DescribeFileSystems` is called first without a
`NextToken` value. Then the operation continues to be called with the `NextToken` parameter set to the
value of the last `NextToken` value until a response has no `NextToken`." Until
[#1195](https://github.com/scttfrdmn/substrate/issues/1195) Substrate decoded only `FileSystemIds` and answered every file system in one page, so
a paginator's loop ran once. Both members are read now:

- **The page size** is the smaller of `MaxResults` and 50. The page says FSx returns "the minimum of
  the `MaxResults` parameter … and the service's internal maximum number of items per page", and the
  response's `FileSystems` publishes "Maximum number of 50 items", which is that maximum. A
  `MaxResults` below 1 ("This parameter value must be greater than 0") is `BadRequest`/400.
- **The token** is an opaque base64 offset that satisfies the published pattern. It is **omitted**, not
  answered empty, on the last page; an empty string is outside its published minimum length of 1.
- **A token Substrate did not issue** is `BadRequest`/400, rather than read as page one (#915).
  `BadRequest` is the page's own generic client-error code; it publishes no more specific one.
- **The order** is the account's ID index, sorted by ID, so a walk sees every file system exactly once.
  The page calls the order "unspecified".
- **`FileSystemIds`** is a lookup, and the page scopes `MaxResults` and `NextToken` to "retrieving all
  file system descriptions", so neither applies when IDs are named. More than the published 50 IDs is
  `BadRequest`/400. An ID that names nothing answers `FileSystemNotFound`.

The operation publishes no filter member, so there is none to apply or refuse. A store read error while
listing is returned rather than skipped, since a skipped record would shift every later offset.

## The file system record carries thirteen of the published members

`API_FileSystem` is a large shape and Substrate reports `FileSystemId`, `FileSystemType`,
`StorageCapacity`, `StorageType`, `VpcId`, `SubnetIds`, `DNSName`, `ResourceARN`, `Lifecycle`,
`CreationTime`, `Tags` and `OwnerId`, plus a `LustreConfiguration` of `MountName` and `DeploymentType`
for Lustre file systems so that an SDK consumer can dereference the mount name without a nil check.
Published and never populated: `AdministrativeActions`, `FailureDetails`, `FileSystemTypeVersion`,
`KmsKeyId`, `NetworkInterfaceIds`, `NetworkType`, and the ONTAP, OpenZFS and Windows configuration
blocks. Every name Substrate does emit is a published one. `CreationTime` is a JSON number of epoch
seconds to three decimals, as `awsJson1_1` publishes a `Timestamp`. Until
[#1373](https://github.com/scttfrdmn/substrate/issues/1373) it was whole seconds, so two file systems
created in one second could not be ordered. The record holds the fraction in the same field, so one
written before the fix still decodes, and renders `.000`. Every absent member is absent because the record holds no value for it, and `fsxToWire`'s doc comment
says why for each ([#1199](https://github.com/scttfrdmn/substrate/issues/1199)). Of
`AWS::FSx::FileSystem`'s four published `Fn::GetAtt` attributes, `DNSName`, `ResourceARN` and
`LustreMountName` resolve. `LustreMountName` resolved empty until #1199's audit, though the plugin
always held it. `RootVolumeId`, "the root volume ID of the FSx for OpenZFS file system", resolves
empty, because OpenZFS volumes are not modeled and an ID for a volume that does not exist would be
invented.

## What a refusal reports

| Condition | Code | Status |
|-----------|------|--------|
| a body that will not parse | `BadRequest` | 400 |
| `FileSystemId` absent on `DeleteFileSystem` | `BadRequest` | 400 |
| a file system ID that does not exist | `FileSystemNotFound` | 400 |
| a file system already deleted, on `DescribeFileSystems` or `DeleteFileSystem` | `FileSystemNotFound` | 400 |
| a `ClientRequestToken` outside 1–63 characters of `[A-za-z0-9_.-]` | `BadRequest` | 400 |
| a used `ClientRequestToken` with different parameters | `IncompatibleParameterError` | 400 |
| a target Substrate does not route | `UnknownOperationException` | 404 |

All four FSx codes are published spellings at published statuses. `BadRequest`/400 and
`FileSystemNotFound`/400 are both in `API_DeleteFileSystem`'s own Errors section, and
`FileSystemNotFound`/400 is in `API_DescribeFileSystems`' as well — the 400 is worth stating plainly,
because a not-found at 400 rather than 404 is unusual enough to look like a bug and is not one here.
Refusing an absent `FileSystemId` is correct on the delete, where it is `Required: Yes`, and the
absence of the same check on `DescribeFileSystems` is also correct, where `FileSystemIds` is
`Required: No` and an absent list means describe them all.

`IncompatibleParameterError`/400 is in the Errors section of both `CreateFileSystem` and
`DeleteFileSystem` (#1210).

Published and with no site: `ActiveDirectoryError`/400, `InvalidExportPath`/400,
`InvalidImportPath`/400, `InvalidNetworkSettings`/400, `InvalidPerUnitStorageThroughput`/400,
`MissingFileSystemConfiguration`/400 and `ServiceLimitExceeded`/400 on `CreateFileSystem`;
`ServiceLimitExceeded`/400 on `DeleteFileSystem`; and `InternalServerError`/500 on all three. No
network setting is validated, no configuration block is required and no file-system quota is
enforced, so none of these conditions can arise.

## The account and Region a record carries reach no response

`FSxFileSystem` declares its account and Region under wire-visible `json` tags, because the record is what
`MemoryStateManager` snapshots and a replay reads back. Neither reaches a body: every file-system response goes through `fsxToWire`.
`TestFSxWire_FileSystemResponsesCarryNoBookkeepingMember` in `emulator/fsx_wire_test.go` drives all three routed operations and walks each decoded document for either member at any
depth ([#756](https://github.com/scttfrdmn/substrate/issues/756)). The record spells the account `account_id`, which a case fold does not reach, so the walk names that spelling too.

## CloudFormation resource types

`AWS::FSx::FileSystem` deploys through `CreateFileSystem` and deletes through `DeleteFileSystem`. These
template properties reach the plugin:
- `FileSystemType`, defaulting to `LUSTRE`;
- `StorageCapacity`, defaulting to `1200`;
- `StorageType`, defaulting to `SSD`;
- `SubnetIds` and `Tags`;
- since #1203, the four per-type configuration blocks (`LustreConfiguration`, `WindowsConfiguration`,
  `OpenZFSConfiguration` and `OntapConfiguration`), as declared.

`!Ref` and `!Sub` are resolved in each of them. The plugin reads `LustreConfiguration`'s
`DeploymentType`, so a `PERSISTENT_2` template now gets a `PERSISTENT_2` file system with a minted
mount name. The other three blocks are sent but not read. `FileSystemTypeVersion`, `KmsKeyId`,
`SecurityGroupIds`, `NetworkType` and `BackupId` are still dropped.

`Ref` returns the file system ID, as published. Of the four published `Fn::GetAtt` attributes:
- `DNSName` and `LustreMountName` resolve from stored metadata;
- `ResourceARN` resolves because its name ends in `ARN`;
- `RootVolumeId` is refused, failing the resource that reads it, because OpenZFS volumes are not
  modeled. An `Output` reading it still answers an empty string.

[#1203](https://github.com/scttfrdmn/substrate/issues/1203).

On `UpdateStack` an unchanged file system keeps its ID and no `CreateFileSystem` is sent
([#1427](https://github.com/scttfrdmn/substrate/issues/1427)). A change to `BackupId`,
`FileSystemType`, `KmsKeyId`, `SecurityGroupIds` or `SubnetIds` — the properties the resource
marks *Update requires: Replacement* — creates a new file system and deletes the old one once the
update succeeds. A change to any other property **fails the resource** with a reason naming the
property, and the update rolls back onto the original file system: AWS applies those in place
through `UpdateFileSystem`, which substrate does not route ([#1546](https://github.com/scttfrdmn/substrate/issues/1546)), and a new file system would be a new ID
where AWS keeps the old one.

## Cost

`CreateFileSystem` is attributed $0.00013, a file-system hour prorated across a single API call.
`DescribeFileSystems` and `DeleteFileSystem` have no entry and no per-service fallback, so they are
free — which is right for the describe and wrong in spirit for a file system that keeps existing after
the create call returns, since FSx bills for provisioned storage by the hour rather than per request.
No pricing provider maps onto `fsx`, so the figure is the static one and does not move with a loaded
price list.
