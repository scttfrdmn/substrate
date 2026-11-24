# EFS

**Endpoint:** `elasticfilesystem.{region}.amazonaws.com`
**Protocol:** REST/JSON

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateFileSystem | `CreationTime` is reported in epoch seconds, as the Response Syntax publishes it. `SizeInBytes.Value` is always **0** — substrate stores no file data, so the metered size of a file system nothing has written to is the published minimum — and `SizeInBytes.Timestamp` is the file system's own creation time, because that is when the 0 was determined. `ValueInIA`, `ValueInStandard` and `ValueInArchive` are absent rather than zero, since no storage class is modelled. `Tags` is always an array, never `null` |
| DescribeFileSystems | Same file-system shape as CreateFileSystem. An account with no file systems answers `"FileSystems": []` |
| UpdateFileSystem | `202 Accepted` with the updated file system; `FileSystemNotFound`/404 for an absent one. Only `ThroughputMode` is applied — `ProvisionedThroughputInMibps` is decoded and unread, so a switch to `provisioned` reports the mode without the throughput |
| DeleteFileSystem | |
| CreateMountTarget | Reports `OwnerId`. `AvailabilityZoneId`, `AvailabilityZoneName`, `Ipv6Address` and `NetworkInterfaceId` are absent: substrate models no subnet topology and creates no network interface, so it has no value for any of them |
| DescribeMountTargets | Same mount-target shape as CreateMountTarget |
| DeleteMountTarget | |
| CreateAccessPoint | Reports `OwnerId`. `ClientToken` is absent — the request's idempotency token is not decoded, so there is none to report |
| DescribeAccessPoints | Same access-point shape as CreateAccessPoint |
| DeleteAccessPoint | |
| TagResource | `204 No Content`; merges into the existing set. The `ResourceId` path member selects the resource by prefix: `fs-` (`FileSystemNotFound`/404 if absent) and `fsap-` (`AccessPointNotFound`/404), and any other prefix is a `BadRequest` — a mount-target ID reaches nothing |
| ListTagsForResource | `Tags` as a `Key`/`Value` array, never `null`; an untagged resource answers with an empty array |
| UntagResource | `204 No Content`; an unknown key is a no-op. Keys are read as **one comma-separated `tagKeys` value**, where AWS publishes a repeated query member — a request parameter keeps only its first value here, so `?tagKeys=a&tagKeys=b` removes `a` alone while `?tagKeys=a,b` removes both |

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::EFS::FileSystem | FileSystemId | |
| AWS::EFS::MountTarget | MountTargetId | |
| AWS::EFS::AccessPoint | AccessPointId | |

## Cost

EFS standard storage: $0.30 per GB-month. Substrate prorates it across the create calls:
`CreateFileSystem` is attributed $0.00003, and `CreateAccessPoint` and `CreateMountTarget`
$0.00001 each. The three entries are keyed `efs/{Operation}`, the plugin's name; until #1202 they
were keyed by the signing name `elasticfilesystem`, which a parsed request never carries, so no
EFS call was charged.
