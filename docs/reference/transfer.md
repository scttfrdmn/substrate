# Transfer Family

**Endpoint:** `transfer.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: TransferService.{Operation}`, API version 2018-11-05)

Substrate routes ten of the seventy-three operations the Transfer Family API
reference lists: create, describe, update and delete for servers and for users,
plus the two collections that list them. Server and user records are keyed by
account and Region, so two accounts or two Regions in one run do not see each
other's servers. Nothing behind the API is modelled — no SFTP, FTPS, FTP or AS2
endpoint listens, no file moves, and no identity provider is called — because a
file transfer is not observable through a Transfer Family API call. What a
consumer can assert is the control-plane record: that a server exists with the
endpoint type and tags it was given, that it has the users created on it, and
that deleting it takes its users with it.

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateServer | Reads every published member except `HostKey`'s value, which is validated and not stored; `Domain` defaults to `S3`, `IdentityProviderType` to `SERVICE_MANAGED`, `IpAddressType` to `IPV4`, `EndpointType` to `PUBLIC` |
| DescribeServer | Every modelled `DescribedServer` member; `UserCount` is counted; no `HostKeyFingerprint` or `As2ServiceManagedEgressIpAddresses` |
| UpdateServer | Reads every published member; a member sent replaces the server's value, one omitted is left alone; `Tags` is not an UpdateServer member and is not read |
| DeleteServer | Cascade-deletes the server's users and their index; answers `{}` |
| ListServers | Pages by `MaxResults` (1–1000, default 1000) and `NextToken`; each element is all eight `ListedServer` members; [observes the seeded lifecycle](#a-server-s-lifecycle-is-seeded) |
| StartServer | `OFFLINE` (or a failed transition) to `ONLINE`; no impact on a server already `ONLINE`, as the page states; answers `{}` ([#1196](https://github.com/scttfrdmn/substrate/issues/1196)) |
| StopServer | `ONLINE` (or a failed transition) to `OFFLINE`; answers `{}` ([#1196](https://github.com/scttfrdmn/substrate/issues/1196)) |
| CreateUser | Requires `Role`, `ServerId` and `UserName`, checked before the server is looked up; stores `SshPublicKeyBody` as the user's one key |
| DescribeUser | All ten `DescribedUser` members; `SshPublicKeys` is an array, possibly empty |
| UpdateUser | Reads every published member with patch semantics; each sent member is held to CreateUser's constraint (#1391) |
| DeleteUser | Answers `{}` |
| ListUsers | Requires `ServerId` and an existing server; pages as `ListServers` does; each element is all six `ListedUser` members |

## Defaults are the ones the pages state

`API_CreateServer` states three defaults in so many words, and those are what a server
omitting the member records: `Domain` "The default value is S3", `IdentityProviderType`
"The default value is SERVICE_MANAGED", and `IpAddressType` "The default value is IPV4".
`Domain` used to default to `SFTP`, which is a `Protocols` value rather than a storage
domain, and it was stored, so every later read reported it
([#1198](https://github.com/scttfrdmn/substrate/issues/1198)). A `Domain` outside `S3 | EFS` is now refused.

`EndpointType` defaults to `PUBLIC`, and that is a different decision: the page states no
default for it. `PUBLIC` is the first of its three Valid Values, and it was already the
value substrate recorded, so it is kept rather than changed along with `Domain`.
`Protocols` and `SecurityPolicyName` publish no default and are absent unless sent.

## A server's lifecycle is seeded

`API_DescribedServer` publishes `Valid Values: OFFLINE | ONLINE | STARTING | STOPPING | START_FAILED |
STOP_FAILED`. Until [#1196](https://github.com/scttfrdmn/substrate/issues/1196) `CreateServer` recorded `ONLINE` and nothing wrote the field again,
because `StartServer` and `StopServer` were unrouted: five of the six values were unobservable, a
wait-until-`ONLINE` loop exited on its first poll, and a wait-until-`OFFLINE` loop could not exit.

Both operations are now routed, and a server's lifecycle is a [seeded progression](/reference/concepts/seeds-and-replay#how-a-progression-is-seeded):

```
POST   /v1/transfer/server-status   {"serverId":"*","pendingObservations":2,"state":"…","finalState":"START_FAILED"}
DELETE /v1/transfer/server-status   (all seeds; ?serverId=… for one)
```

- **The operations.** `StopServer` "changes the state … from `ONLINE` to `OFFLINE`", and `StartServer`
  "from `OFFLINE` to `ONLINE`. It has no impact on a server that is already `ONLINE`." Both also move a
  server out of `START_FAILED` or `STOP_FAILED`.
  - Both pages say the response is "an HTTP 200 response with an empty HTTP body", while their examples
    show `{"ServerId": …}`. Substrate answers `{}`, for the reason the deletes do (#1206).
  - A missing server is `ResourceNotFoundException`/400, as on every server operation.
- **Unseeded**, a transition takes effect at once: a new or started server is `ONLINE`, a stopped one
  `OFFLINE`, from the first describe.
- **Under a seed**, `DescribeServer` and `ListServers` report the intermediate state for
  `pendingObservations` observations of that server: `STARTING` when it is moving to `ONLINE`,
  `STOPPING` when moving to `OFFLINE`, or the seed's `state`. Each create, start and stop restarts the
  countdown, so one seed covers them all.
  - Once the countdown is spent, the server reports the state it moved to, or the seed's `finalState`
    when that is this transition's failure: `START_FAILED` for a create or a start, `STOP_FAILED` for a
    stop. Any other `finalState` is refused.
- **What is unverified.** `CreateServer`'s response carries no state, and no page says what a new server
  reports while it comes up. A seeded new server reporting `STARTING` before `ONLINE` is therefore
  substrate's reading. No page publishes a deleting state either, so `DeleteServer` still removes the
  server and its users at once.

## Both collections page, and refuse a token substrate did not issue

`ListServers` and `ListUsers` read `MaxResults` and `NextToken`
([#1195](https://github.com/scttfrdmn/substrate/issues/1195)). A `MaxResults` outside the published Valid Range of 1–1000 is
`InvalidRequestException`/400, since neither page publishes a code specific to the range.
Neither page states a default page size either, so a request with none gets the
published maximum, 1000. A `NextToken` substrate did not issue is
`InvalidNextTokenException`/400, the code both pages publish, with the sentence they give
it. The token is the base64 offset the other offset-paginated operations use
(`offset_pagination_token.go`).

`NextToken` is **omitted** on the last page. It used to be `""` on every response, and the
published Length Constraints are 1–6144, so the empty string was an illegal value rather
than a terminator: a paginator written `while "NextToken" in response` never stopped. A
request that echoes an empty `NextToken` is read as having sent none, so a caller still
echoing the old `""` is not refused.

Servers list in server-ID order and users in user-name order: both indexes are kept
sorted, which is what makes an offset stable across pages. Neither collection publishes
a filter member, so there is no filter to apply or to refuse.

## Required members, and the constraints of the members read

`API_CreateUser` marks `Role`, `ServerId` and `UserName` Required: Yes. All three are
checked before the server is looked up, and an absent one is `InvalidRequestException`/400
— the code every Transfer page publishes for a malformed request — with a message naming
the member ([#1197](https://github.com/scttfrdmn/substrate/issues/1197)). `Role` used to go unchecked, so a user with no access
role was created and reported successful.

Every member a routed operation reads is held to its own published Valid Values, Length
Constraints and Pattern, and is refused `InvalidRequestException`/400 when it breaks them:

- **CreateUser, UpdateUser:** `Role` (20–2048, `arn:.*role/\S+`), `UserName` (`[\w][\w@.-]{2,99}`),
  `HomeDirectory` (`(|/.*)`, ≤1024), `HomeDirectoryType` (`PATH | LOGICAL`), `Policy`
  (≤2048), `SshPublicKeyBody` (the published key pattern, ≤2048, CreateUser only).
  `HomeDirectoryMappings` must be an array of 1–50,000 entries and `PosixProfile` an object.
  Both operations apply these through one function, so they cannot drift apart.
- **CreateServer, UpdateServer:** `Domain` (`S3 | EFS`, CreateServer only), `Tags` (1–50
  items, CreateServer only; `[]` is below the published minimum), `EndpointType`,
  `IdentityProviderType`, `IpAddressType`, `Protocols` (1–4 of `SFTP | FTP | FTPS | AS2`),
  `LoggingRole`, `SecurityPolicyName`, both login banners, `Certificate`, `HostKey`, and
  `StructuredLogDestinations` (0–1 entries, 20–1600, `arn:\S+`). Each nested member must be
  an object.

Two rules are declined, and recorded at the handler:

- The cross-member rules the CreateServer page states in prose are not enforced. These are
  FTP or FTPS requiring `VPC` and a non-`SERVICE_MANAGED` identity provider, FTPS requiring
  a `Certificate`, and AS2 requiring `VPC` and `S3`. The page publishes no code or message
  for a bad combination, so substrate would have to invent both.
- A `ServerId` outside `s-([0-9a-f]{17})` is not refused as malformed. It names no server,
  so it answers `ResourceNotFoundException` like any other absent ID, which keeps an
  existence probe on one code.

## Records carry their published members

`DescribeServer` answers every `DescribedServer` member the record models, which is all of
them except two ([#1199](https://github.com/scttfrdmn/substrate/issues/1199)):

- **`HostKeyFingerprint`:** substrate holds no host key to fingerprint. A `HostKey` sent to
  CreateServer or UpdateServer is a private key, and is validated and not stored.
- **`As2ServiceManagedEgressIpAddresses`:** no AS2 endpoint is modelled, so no address is
  assigned.

The nested objects (`EndpointDetails`, `IdentityProviderDetails`, `ProtocolDetails`,
`S3StorageOptions`, `WorkflowDetails`) are recorded as sent and answered back unchanged.

`UserCount` is counted from the server's user index when the response is built. It was
declared on the record and never assigned, so it reported 0 however many users existed.

`DescribeUser` answers all ten `DescribedUser` members. `SshPublicKeys` holds the key
`CreateUser` was sent as `SshPublicKeyBody`, with an `SshPublicKeyId` of `key-` and 17 hex
characters minted from the request, and a `DateImported` in epoch seconds, the awsJson1_1
form of a Timestamp. It is an empty array for a user with no key, as the page's own
`SshPublicKeys: []` note shows. `HomeDirectoryType` is reported only when it was sent,
since `API_CreateUser` states no default for it.

`ListServers` answers all eight `ListedServer` members and `ListUsers` all six
`ListedUser` members, `UserCount` and `SshPublicKeyCount` included.

## The account and Region a record carries reach no response

`TransferServer` and `TransferUser` declare `AccountID` and `Region` under wire-visible
`json` tags, because the record is what `MemoryStateManager` snapshots and a replay reads
back. The server also declares `CreatedAt`. Until
[#756](https://github.com/scttfrdmn/substrate/issues/756), `DescribeServer` and
`DescribeUser` answered each record whole. That put all of these members on the wire, plus
`ServerId` inside the user object. None of them is published: `API_DescribedUser` has no
`ServerId`, and the response carries the server's ID at the top level instead. Both now
answer through `transferServerOut` and `transferUserOut` (`emulator/transfer_wire.go`). The
stored record is unchanged, so a run recorded before the fix replays identically.

All ten routed operations are driven by
`TestTransferWire_ServerResponsesCarryNoBookkeepingMember` and its sibling in
`emulator/transfer_wire_test.go`. They walk each decoded document and fail on a member named
for a bookkeeping field at any depth. The user test also requires that `User` carries no
`ServerId`. No bookkeeping member carries `,omitempty`, so a record that exists holds every
one of them, and each test reads them back before asserting their absence.

## UpdateServer applies what it is sent

`UpdateServer` reads every member `API_UpdateServer` publishes
([#1199](https://github.com/scttfrdmn/substrate/issues/1199)). A member the request sends replaces the server's value, and one it
omits is left alone. An empty `StructuredLogDestinations` clears the destination, as the
page documents. `Tags` is no longer read, since the page publishes no `Tags` member;
tagging a server is `TagResource`, which is not routed. `Domain` is not an UpdateServer
member either: the page says the domain cannot be changed after creation.

## UpdateUser applies what it is sent

`UpdateUser` reads every member `API_UpdateUser` publishes: `HomeDirectory`,
`HomeDirectoryType`, `HomeDirectoryMappings`, `Policy`, `PosixProfile` and `Role`
([#1391](https://github.com/scttfrdmn/substrate/issues/1391)). It used to read `HomeDirectory` and `Role` only, so a policy, a POSIX
profile or a logical home directory sent in an update was answered 200 and dropped.

- A member the request omits is left as it was, and one it sends replaces the stored value.
- An empty `HomeDirectory` or `Policy` is a value inside each member's published minimum of 0,
  so it clears the stored one.
- `Role` (minimum 20) and `HomeDirectoryType` (`PATH | LOGICAL`) have no valid empty value, so an
  empty one is refused `InvalidRequestException`/400 rather than read as a clear.
- Each sent member is held to the constraint CreateUser enforces on it, and a refused update
  changes nothing. An absent user is `ResourceNotFoundException`/400.
- The prose cross-member rules (`HomeDirectoryMappings` only with `LOGICAL`, and not both
  `HomeDirectory` and `HomeDirectoryMappings`) are recorded as sent, as CreateUser records
  them, because the page publishes no code for a bad combination.

The response is `ServerId` and `UserName`, as published.

## Server IDs are minted from the request

`generateTransferServerID` draws from the request's `IDMint`, so a server ID is
`s-` followed by 17 hexadecimal characters, the published fixed length of 19 and pattern
`s-([0-9a-f]{17})`, and is the same on every run of the same requests (#856,
[#1204](https://github.com/scttfrdmn/substrate/issues/1204)). SSH public key IDs (`key-` and 17 hex characters) are minted the
same way.

## A stack-deployed server is the plugin's own

`AWS::Transfer::Server` deploys through `CreateServer`, and its properties are sent as `CreateServer`'s
members, which have the same names. It deletes through `DeleteServer`. It used to be a stub whose ID
was `s-` followed by the lower-cased logical ID, which matches neither the published fixed length of
19 nor `s-([0-9a-f]{17})`, and `DescribeServer` answered `ResourceNotFoundException` for it. Now the
plugin mints the ID, and `DescribeServer` answers it. `Ref` returns the server ARN, as AWS publishes
([#1203](https://github.com/scttfrdmn/substrate/issues/1203)).

On `UpdateStack` the server keeps its ID ([#1427](https://github.com/scttfrdmn/substrate/issues/1427)).
An unchanged server makes no call. A change to any property but `Domain` goes through `UpdateServer`,
with the template's properties sent as its members minus the three it does not take — `Domain`,
`Tags` and `CreateWithHostKey` — and `DescribeServer` reports the new values. `Domain` is
*Update requires: Replacement*, so a changed `Domain` creates a new server and deletes the old one
once the update succeeds. Two gaps: a property removed from the template is not reset, since
`UpdateServer` leaves an omitted member as it is; and a change to `Tags` alone makes no call, since
`TagResource` is not routed.

## The two delete operations answer an empty JSON object

`DeleteServer` and `DeleteUser` answer `{}` at 200. This is a recorded decision, because
the reference disagrees with itself ([#1206](https://github.com/scttfrdmn/substrate/issues/1206)). Both pages' Response Syntax is
a bare `HTTP/1.1 200`, and their prose says "an HTTP 200 response with an empty HTTP body",
while `API_DeleteServer`'s Example shows `{ }` as the body. `{}` is chosen because:

- it is what the Example shows;
- an awsJson1_1 client decodes an empty object and an empty body alike for an operation
  with no response members;
- it is what substrate has always answered, so a recorded run replays byte-identically.

Both deletes refuse an identifier that names nothing with `ResourceNotFoundException`/400.
No other routed Transfer operation answers `{}` where its page publishes a body.

## What a refusal reports

| Condition | Code | Status |
|-----------|------|--------|
| Request body that is not JSON | InvalidRequestException | 400 |
| A required member absent (`ServerId`, `UserName`, `Role`) | InvalidRequestException | 400 |
| A member outside its published Valid Values, Length Constraints or Pattern | InvalidRequestException | 400 |
| `MaxResults` outside 1–1000 | InvalidRequestException | 400 |
| `NextToken` substrate did not issue | InvalidNextTokenException | 400 |
| Server ID that does not exist | ResourceNotFoundException | 400 |
| User that does not exist on the named server | ResourceNotFoundException | 400 |
| User name already present on the server | ResourceExistsException | 400 |
| Operation outside the routed ten | UnknownOperationException | 404 |

Every code and status above is the one the operation's own page publishes. The
not-found status was 404, and the duplicate-user refusal was `ConflictException`/409,
which is published on `UpdateServer` for a concurrent update rather than on `CreateUser`
([#1198](https://github.com/scttfrdmn/substrate/issues/1198)). The refusal for an
undecodable body is `InvalidRequestException` at 400 rather than the Common Errors
code `MalformedHttpRequestException`, also 400, because that code's published
gloss is about content-encoding decompression rather than JSON syntax; the
reasoning is recorded at the call site.

Published codes with no site in substrate: `AccessDeniedException`, which
`CreateServer`, `UpdateServer` and `DeleteServer` publish at 400 and the Common
Errors page publishes at 403; `ConflictException` (400 on `UpdateServer`, for a VPC
endpoint that is not available, which substrate does not model); `ThrottlingException`
(400 per operation, 400 on Common Errors); `InternalServiceError` (500); and
`ServiceUnavailableException` (500). The rest of the Common Errors vocabulary is likewise
unemitted: `ExpiredTokenException` (403), `IncompleteSignature` (403), `InternalFailure`
(500), `MalformedHttpRequestException` (400), `NotAuthorized` (401), `OptInRequired`
(403), `RequestAbortedException` (400), `RequestEntityTooLargeException` (413),
`RequestTimeoutException` (408), `ServiceUnavailable` (503),
`UnrecognizedClientException` (403) and `ValidationError` (400).

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::Transfer::Server | server ARN | Deployed through `CreateServer`, whose minted ID is the physical ID. Deleted through `DeleteServer`. No property is dropped. `Fn::GetAtt Arn` and `ServerId` resolve. `State` resolves to `ONLINE`, the state `CreateServer`'s record moves to; a seeded start progression is observed through `DescribeServer`, and this is substrate's reading. `As2ServiceManagedEgressIpAddresses` is refused, failing the resource that reads it, because no AS2 egress addresses are assigned |

`AWS::Transfer::User` is not deployed, so a template that creates a server and its
users is refused in [strict mode](/reference/cloudformation#strict-mode-and-template-validation), and in lenient
mode gets the server and a generic stub for each user.

## Cost

Creating a server is charged $0.30, an approximation of the published
$0.30 per protocol per hour collapsed into a single per-creation charge. The key
is matched on the operation name, which a JSON-target request carries in its
`X-Amz-Target` header, so the entry is live. No other Transfer operation is
charged, and no per-hour or per-gigabyte component is modelled.
