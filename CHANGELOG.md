# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **A REST service is no longer named after its HTTP method, so a correct policy
  is no longer denied** (#572). A REST-JSON or REST-XML service carries its
  operation in the shape of its URL and nowhere else, so request parsing fell
  through to its last-resort fallback and the request entered the pipeline named
  `POST`. Authorization, fault injection, cost and consistency all read the
  operation before any plugin runs, so every one of them saw the verb. Measured on
  the parent commit: **17 of 18 REST services** were affected — S3 was the only
  exception, because #480 had wired its resolver in for exactly this reason.

  This was inert while the evaluator was unreachable and stopped being inert with
  #411. A user holding `lambda:InvokeFunction` was refused an invoke, because the
  action evaluated was `lambda:POST`; a wildcard masked it, so precisely the
  scoped policy #411 exists to let a consumer test got a false
  `AccessDeniedException`. Left unfixed, the release would have reported denials
  for policies that are correct — the inverse of its claim.

  The operation name is now resolved once, before the pipeline reads it,
  generalising #480 instead of repeating it 17 times. Each service's resolver
  already existed and is reused rather than reimplemented, so the name that is
  authorized, metered and recorded cannot drift from the name its plugin routes
  on. `AWSRequest` gains `HTTPMethod`: the verb and the operation had been sharing
  one field, and resolving the operation destroyed the verb a REST resolver needs
  — `POST` and `DELETE` on the same path are different operations.

  **The IAM action is not always the operation name**, and treating it as such
  denies a caller whose policy is right, so the mapping is now explicit and
  sourced per operation. Lambda's `Invoke` requires `lambda:InvokeFunction`. S3's
  listings authorize against the bucket: `ListObjects`/`ListObjectsV2` and
  `HeadBucket` require `s3:ListBucket`, `ListBuckets` requires
  `s3:ListAllMyBuckets`, and the versioned and multipart listings have their own
  bucket-scoped actions. API Gateway v1 is the sharper case: its IAM actions
  genuinely *are* HTTP verbs (`apigateway:POST`, and no `apigateway:CreateRestApi`
  exists), so for that service the verb remains the action — while `apigatewayv2`
  is a separate prefix that does use operation names. Conflating them would have
  broken v1 in the act of fixing the others.

  Two consequences beyond authorization, both pre-existing and now fixed: a fault
  rule can name a REST operation, closing #480's gap for the 17 services its
  S3-only fix never reached (a rule naming `POST` used to take out every call to
  the service at once); and a recorded event carries its operation, so an event log
  is greppable by operation. CloudFormation's resource calls are built in-process
  and never parsed off the wire, so they are resolved at dispatch too — a stack's
  bucket is recorded and authorized as `CreateBucket`, not `PUT`.
- **A signed request now resolves to the principal it actually is** (#411). The
  server derived a caller's principal ARN from its *access key ID*, yielding
  `arn:aws:iam::123456789012:user/AKIATEST12345678901` — a name nothing is ever
  stored under. So a user `alice` holding an inline `sqs:*` Allow was allowed when
  evaluated as `user/alice` and denied when evaluated as `user/AKIATEST…`:
  authorization could not find any real principal's policies even where it ran, and
  #411's own acceptance ("create a role with a scoped policy, make a call as that
  role") was unreachable in principle rather than merely unwired.

  A caller is now resolved from **state**: `iam:accesskey:<id>` names the user whose
  policies apply, which is the record `CreateAccessKey` has always written, and
  `sts:session:<id>` names the assumed-role ARN `AssumeRole` mints — a key substrate
  wrote from the beginning and nothing had ever read, which is why STS session
  credentials authorized as nothing in particular.

  Resolution is deliberately independent of `ServerOptions.Credentials`. That
  registry also gates SigV4 verification, and an access key absent from it is
  rejected with `InvalidClientTokenId`; identifying callers through it would
  therefore have 403'd every credential substrate documents, `AKIAIOSFODNN7EXAMPLE`
  included. Reading state instead costs one `Get` on a request that carries an
  `Authorization` header, and refuses nothing.

  A key that belongs to no IAM entity — an unregistered key, the built-in test key,
  a deleted key — resolves to **no principal** and is therefore still enforced
  against nothing. Existence in state is the opt-in, so no configuration flag turns
  this on and every caller that never touched IAM is unaffected, including
  `AKIAIOSFODNN7EXAMPLE` and `test`/`test`. `Credentials`-wired servers keep
  reporting the access-key ARN for a key with no IAM entity behind it, which is what
  `GetCallerIdentity` has always shown.

  An STS session also now carries the account it was issued for. Account resolution
  recognises only an `AKIA` prefix as substrate's test account, so an `ASIA` session
  key was filed under `000000000000` — a different partition from the resources the
  caller who assumed the role had just created.

- **An `assumed-role` principal is no longer allowed with no policies at all**
  (#411). `arn:aws:sts::123456789012:assumed-role/worker/sess1` — the principal shape
  STS itself mints, and therefore what every "call as a role" reduces to — was
  **allowed unconditionally**. The policy loader treated any entity type outside
  `user`/`role` as an error, and the evaluator failed open on an error, so the safe
  default inverted for exactly the callers a scoped role is meant to constrain. A
  session is now resolved to its **role**, as real IAM resolves one: the session name
  is split off (`assumed-role/worker/sess1` → role `worker`) so managed and inline
  role policies are found under the keys IAM writes, rather than under a per-session
  name nothing is stored beneath.

  A permission boundary resolves through the same path, else switching enforcement on
  for these principals would have switched their boundaries off.

- **The two policy loaders no longer disagree about one ARN** (#411).
  `IAMPlugin.authorize` **denied** an entity type it did not handle where
  `AuthController.CheckAccess` **allowed** it, so the same caller's permission
  depended on which service it happened to call. Both now resolve through one
  function, and an entity type neither models — an account root, a service
  principal, a federated user — is consistently *not enforced* rather than denied by
  one and allowed by the other.

  This distinguishes two cases the loader had conflated: an entity that resolves and
  exists is evaluated (no policies is an implicit deny, as on AWS) and one that
  resolves to nothing is unenforced and logged at debug, while a state read that
  genuinely *fails* still fails open with a warning. That last case is a broken
  backend, not an unknown caller, and it is the only one that should be loud.

- **`sts:GetCallerIdentity` and `sts:GetSessionToken` no longer require
  permissions** (#411). AWS documents both as needing none — `GetCallerIdentity`
  answers "even if an administrator attaches a policy to your identity that
  explicitly denies access", because a denial would return the same information, and
  `GetSessionToken`'s purpose is to authenticate a user via MFA, which "you cannot
  use policies to control". Substrate evaluated both like any other action.

  This was unreachable until the fix above: with every caller resolving to no
  principal, the evaluator never ran. The first credential that named a real IAM user
  had `aws sts get-caller-identity` answer `AccessDeniedException`. A sweep of every
  botocore service model finds the phrase on these two operations and nothing else,
  so the exemption is that pair rather than a general carve-out.

### Changed
- **A call made as an IAM user that exists is now authorized against that user's
  policies** (#411). This follows from principal resolution above and is the point of
  the release: a key minted by `CreateAccessKey` for a user with no policy, or with a
  policy that does not allow the call, now returns `AccessDeniedException` where it
  previously succeeded — which is real IAM behaviour, and the failure class
  ("forgot a permission / policy too narrow") that composition tests could not catch.

  Nothing else changes. A nil principal still passes, so every in-process `Client`
  call, every unregistered access key, and every test that does not create an IAM
  principal behaves exactly as before.

- **A call made with STS session credentials is now authorized against the assumed
  role's policies** (#411). Same consequence as above, for the principal shape
  `AssumeRole` mints: a session of a role holding no policy, or a policy that does
  not allow the call, now returns `AccessDeniedException` where it previously
  succeeded — see the fail-open under **Fixed** for why "previously succeeded" meant
  *unconditionally*.

  A role that the session names but that does not exist in state is still not
  enforced, so this reaches only sessions minted against a role substrate actually
  holds.

## [v0.91.0] - 2026-08-04

### Fixed
- **API Gateway v2 responses are the shape an SDK parses** (#529). Every
  `apigatewayv2` response used PascalCase members under an `Items` envelope, so
  `aws apigatewayv2 create-api --query ApiId` printed `None` and
  `get-apis --query 'length(Items)'` failed on a null — the same
  case-sensitive-`locationName` mechanism as v1 and MSK below, and the same total
  parse failure rather than a member missing here and there. This was invisible over
  the wire until the routing fix below made v2 reachable by an SDK at all.

  Same remedy: the seven state types keep their PascalCase tags and are
  **untouched**, so **state encoding is unchanged and recorded runs replay**; nine
  response-only element types tagged from the model are projected from them, and none
  carries an `AccountID` or `Region` member at all. The five list envelopes now emit
  `items` — lowercase, unlike v1's singular `item`; the two spellings differ and both
  are transcribed from their own model.

  v2 is far more regular than v1: all 1,545 members of its response shapes are the
  plain lowerCamel of their member names, the only irregular spellings in the whole
  model (`ResourceArn -> "resource-arn"`) being on the three tag *request* shapes
  that substrate does not route. Each tag was still transcribed from the model rather
  than lowercased programmatically, because a mechanical transform is exactly what
  gets an exception wrong.

  Three members are now absent, from the model rather than the previous code. A
  route, integration, stage and API mapping report no `apiId`: the API is a path
  parameter of the request and not a member of any of those shapes, so botocore
  discarded the key regardless. A domain name reports no `regionalDomainName` — that
  spelling is v1's, and v2 has no top-level equivalent; substrate now reports the
  same hostname where the v2 model puts it, as
  `domainNameConfigurations[].apiGatewayDomainName`, which is what makes it visible
  to an SDK instead of silently dropped. And no response carries a `nextToken`, since
  substrate returns every element in one page and honours no token. Unset optionals
  are omitted rather than sent as `""` or `null`.

  The CloudFormation v2 deploy handlers read `ApiId`, `RouteId`, `IntegrationId` and
  `AuthorizerId` out of these responses to set a resource's physical ID; they now read
  the wire spellings. They worked before only because Go's `json.Unmarshal` is
  case-insensitive, which is the same reason ~10 assertions in
  `apigatewayv2_plugin_test.go` and `apigateway_proxy_test.go` were **wrong today and
  passed anyway**. Updating them is part of the fix. Those CloudFormation tests
  asserted only a resource count, so a physical ID silently falling back to a logical
  ID or a route key would not have failed one; they now assert the ID itself, which is
  what makes every `Ref` in a stack verifiable.

  `GetApiMappings` turns out to be unrouted (`POST` on that path is the only mapping
  operation implemented). That is a missing operation rather than a wrong response
  shape, so it is filed as #566 rather than folded in here; `create-api-mapping`
  itself parses correctly.

- **API Gateway v1 responses are the shape an SDK parses** (#529). Every `apigateway`
  response substrate sent used PascalCase members under an `items` envelope, and
  botocore matches a response key against the model's `locationName`
  **case-sensitively** — so `aws apigateway create-rest-api --query id` printed
  nothing and `get-rest-apis --query 'length(items)'` failed on a null. Not a field
  missing here and there: nothing substrate returned parsed.

  Both halves were wrong, and this corrects #529 as filed, which says the envelope
  key `items` is *"already correct"*. Every v1 collection member is spelled `items`
  in the model but carries `locationName: "item"` — singular — so `{"items": […]}`
  parses to nothing and `{"item": […]}` parses. Seven of substrate's eight list
  handlers sent the plural and now send `item`. `GetStages` is the exception: its
  model member is literally named `item`, substrate already emitted `item`, and it
  is deliberately **unchanged** — a blanket rename would have broken the only v1
  list an SDK could already read. `TestAPIGatewayWire_GetStagesEnvelope` is the
  regression gate for exactly that.

  Fixed as #528 fixed CloudWatch Logs and as MSK is fixed below: the eleven state
  types keep their PascalCase tags and are **untouched**, so **state encoding is
  unchanged and recorded runs replay**; eleven response-only element types tagged
  from the model are projected from them by a `Wire` function, and none of those
  types has an `AccountID`, `Region` or `APIId` member at all, so substrate's
  internal fields cannot leak back through a field someone adds later.

  The projection recurses, which needed a second fix to be observable: `Resource`
  declares a `resourceMethods` map and `Method` a `methodIntegration`, but substrate
  stored methods and integrations under their own state keys and never joined them,
  so both members were permanently absent from every response. `GetResource` and
  `GetMethod` now report them, projected at all three depths.

  Judgement calls taken from the model rather than the previous code: `providerARNs`
  keeps its capitalised acronym, the one v1 member that is not the plain lowerCamel
  of its name; `UsagePlan` reports no `createdDate` and `BasePathMapping` no
  `domainName`, neither being a member of its shape; unset optionals are omitted
  rather than sent as `""` or `null`, because real API Gateway omits them and a
  caller distinguishing absent from null is reading a real observable; and no
  response carries a `position` token, since substrate returns every element in one
  page and honours none — an empty token would invite a caller to page on nothing.
  `GetStage`'s `invokeUrl` is substrate's own convenience that no model declares; it
  is kept, respelled from `InvokeUrl`, and cannot confuse an SDK because botocore
  drops a key the model does not declare.

  Existing assertions in `apigateway_plugin_test.go` and `apigateway_proxy_test.go`
  are updated to the real wire keys. Worth saying plainly: those assertions were
  **wrong today and passed anyway**, for the same case-insensitivity reason, and one
  even carried a comment documenting the broken spelling as intended.

- **MSK responses are the shape a Kafka SDK parses** (#529). Every `kafka` response
  substrate sent used PascalCase keys, and botocore matches a response key against
  the model's `locationName` **case-sensitively** — so `aws kafka list-clusters`
  returned HTTP 200 with an empty result and no error at all, and
  `--query 'length(ClusterInfoList)'` failed on a null. The kafka model spells all
  579 of its response members in lowerCamel, with no exceptions, so nothing
  substrate returned parsed: not a missing field here and there, nothing.

  This was invisible from every direction. The request side works, because Go's
  `json.Unmarshal` matches keys case-insensitively and so accepts the lowerCamel
  body a real SDK sends into a PascalCase-tagged struct. The state was written
  correctly. And the Go round-trip tests passed, because a struct marshalled and
  unmarshalled by its own definition agrees with itself whatever its tags say — the
  reason the new tests decode into `map[string]any` and assert on literal wire keys
  instead.

  Fixed as #528 fixed CloudWatch Logs: the state types keep their PascalCase tags,
  and response-only element types tagged from the model are projected from them.
  `MSKCluster` and its six sibling state types are **untouched**, so **state
  encoding is unchanged and recorded runs replay** — the wire types are a separate
  thing from the persisted format, and a comment in `msk_types.go` says so, because
  the tempting "fix" for a future casing bug is to retag the state struct and
  silently change the format of every recorded run.

  Corrected envelopes and members: `clusterInfoList`, `clusterInfo`,
  `bootstrapBrokerString`, `nodeInfoList`, and `CreateCluster`'s
  `clusterArn`/`clusterName`/`state`. `NodeInfo`'s ARN member is `nodeARN` — the one
  kafka response member that is not the plain lowerCamel of its name, transcribed
  from the model rather than lowercased. `DeleteCluster` no longer reports
  `clusterName`, which is not a member of `DeleteClusterResponse`. `ListClustersV2`
  no longer sends an empty `nextToken`, which invited a caller to page on nothing.
  `DescribeClusterV2`/`ListClustersV2` built a hand-written PascalCase map, which was
  wrong for the same reason the struct path was; both now use the same projection as
  v1, so the two cannot drift.

  Three consequences worth stating. Substrate's internal `AccountID` and `Region`
  are gone from every kafka response, and structurally so: no wire type has such a
  member, so a field added later cannot reintroduce the leak. An unset optional —
  `securityGroups`, `storageInfo`, `tags` — is now **omitted** rather than sent as
  `null` or as `volumeSize: 0`, matching real MSK, since a caller distinguishing
  absent from null is reading a real observable. And an empty `ListClusters` returns
  `[]`, not `null`.
- **API Gateway v2 and SSO Admin are reachable from an SDK at all** (#561, #529).
  Both plugins were registered, had test files, and could not be reached by any
  client — every unit test green, the feature absent from a caller's point of view,
  because both test files call `HandleRequest` directly and so never exercise the
  router.

  `APIGatewayV2Plugin` was unreachable because no routing signal distinguishes API
  Gateway v1 from v2: the v2 client's SigV4 signing name **is** `apigateway`, its
  hostname is the same `apigateway.<region>.amazonaws.com`, and neither client sends
  `X-Amz-Target`. So every v2 request landed on `APIGatewayPlugin` and came back
  `NotFoundException: unknown operation for POST /v2/apis`. `extractService` now
  refines a resolved `apigateway` to `apigatewayv2` when the path begins with
  `/v2/`. The two URI spaces make that exact rather than heuristic — all 103
  requestUris in the apigatewayv2 model are under `/v2/`, and none of v1's are — and
  the refinement is confined to requests that already resolved to `apigateway`, so
  another service's `/v2/` endpoint cannot be captured by it.

  `SSOPlugin` was unreachable because `targetServiceAliases` mapped
  `AWSSSOAdminService`, a prefix derived by guesswork that no client sends. The real
  `sso-admin` `targetPrefix` is `SWBExternalService`, so `aws sso-admin
  list-instances` failed with `ServiceNotAvailable: service not emulated:
  swbexternalservice`. The real prefix is now mapped **alongside** the guessed one,
  which is retained so any caller that does send it keeps working.

  API Gateway v2 responses are still PascalCase and so still parse to nothing in a
  real SDK; that is #529's separate defect and is fixed in this release's later
  changes. This change is what makes it observable.

## [v0.90.0] - 2026-08-04

### Deprecated
- **`emulator.BettyClient` and `emulator.NewBettyClient`, in favour of
  `emulator.Client` and `emulator.NewClient`** (#549). The name referred to an
  unrelated project and said nothing about what the type does, so no reader could
  learn its meaning from this repository — while it appeared in the public
  documentation of all 65 plugins, since `docs/services.md` advertised a "Betty CFN
  resource types" section per service.

  `BettyClient` is now a type **alias** for `Client`, not a defined type, so a
  consumer holding both spellings — or passing one where the other is expected —
  keeps compiling for the whole deprecation window; a defined type would have made
  the rename a breaking change wearing a deprecation's clothes. A test asserts the
  alias resolves to one type in both directions, because both spellings compile in
  isolation and nothing else would catch the difference. `NewBettyClient` is a
  one-line wrapper. Both are marked `// Deprecated:` naming the replacement, so
  `staticcheck` flags a consumer's use without breaking their build, and both will be
  removed in **v1.0.0** — the only point a Go module can drop an exported symbol
  without breaking the import path for everyone.

### Changed
- **The `Betty` naming is gone from the code, the docs and the examples** (#549).
  Thirty-five `emulator/betty*.go` files are renamed for what they contain
  (`betty.go` → `client.go`, `betty_cfn.go` → `cfn_deployer.go`, `betty_report.go` →
  `validation_report.go`, `betty_debug.go` → `debug_session.go`, `betty_cfn_*` →
  `cfn_*`), `examples/betty_workflow/` → `examples/validation_workflow/`, and
  `docs/services.md`'s repeated **Betty CFN resource types** heading is now
  **CloudFormation resource types**. Every rename is recorded as a `git mv` so
  `git log --follow` and `git blame` still reach each file's history.

  The `betty_cfn_vNN_plugins.go` files were named for the substrate release that
  added them rather than their contents, and keep that suffix as
  `cfn_resources_vNN.go`: several releases touched overlapping services (RDS appears
  in two, EC2 in two), so renaming by service would mean *merging* files, which is a
  refactor rather than a rename and does not belong in the same diff. Each now opens
  with a header naming the services it holds, which is what the file name could not.

  This is a pure rename: no behaviour changes, so the diff reads as a rename and a
  bisect over it cannot land on a mixed commit. Two references are deliberately left
  alone as historical records — this changelog, and the `"v0.6.0 — Betty
  integration"` option in the feature-request issue template, since rewriting a
  release label in a dropdown would make past issues unmatchable. The `area: betty`
  GitHub label was renamed in place to `area: cloudformation`, which preserves it on
  every issue already carrying it.

### Added
- **S3 `UploadPartCopy` and `ListParts`** (#532, #551). Both were unroutable, and
  both for the same reason: `parseS3Operation`'s object-level verb arms had drifted
  apart on when they test `uploadId`.

  A part copy — a `PUT` carrying `partNumber`, `uploadId` **and**
  `x-amz-copy-source` — was claimed by `CopyObject`, whose copy-source test came
  first. The consequence was worse than a missing operation: the request answered
  `200` with a correctly-shaped `CopyPartResult`, so nothing looked wrong, while it
  **wrote the destination object** instead of storing a part. A `HeadObject` on the
  destination mid-upload therefore reported a `ContentLength` real S3 never exposes,
  and the `CompleteMultipartUpload` that followed failed with `InvalidPart` because
  no part had been stored. `UploadPartCopy` now stores a part and leaves the
  destination key absent until Complete assembles it, honors
  `x-amz-copy-source-range` and the `x-amz-copy-source-if-*` preconditions, and
  returns the copied part's `ETag` and `LastModified` in a `CopyPartResult` body.
  Copied and uploaded parts mix in one upload, and a copied part's checksum is
  computed under the upload's algorithm so Complete still assembles a `COMPOSITE`
  object checksum over the mixture.

  `ListParts` had no handler at all, and the `GET` arm tested no `uploadId`, so
  `GET /<bucket>/<key>?uploadId=…` reached `GetObject` and answered `404 NoSuchKey`
  — for an upload whose parts substrate had stored and could list. A caller polling
  its own upload could not tell an upload with parts from a key that does not exist.
  It now returns a `ListPartsResult` with `max-parts`/`part-number-marker` paging,
  the upload's storage class and checksum configuration, and each part's number,
  `ETag`, size, `LastModified` and checksum. An upload with **no** parts is a `200`
  with an empty list, not a `404`; an unknown `uploadId`, or one addressed through
  the wrong key, is `404 NoSuchUpload` rather than the `NoSuchKey` `GetObject` was
  answering.

  The asymmetry was the tell: `PUT`, `DELETE` and `POST` all tested `uploadId`;
  `GET` alone did not. The invariant is now stated once in the router where all four
  arms are visible — *a multipart request is identified by its `uploadId`, and the
  general object-verb operation is a fall-through the router must reach only after
  every multipart test has failed* — because four arms drifting apart is what
  produced both bugs, and a comment on one arm would not have prevented the other.

  Two refusals are substrate's own choice rather than the API model's. A malformed
  or out-of-bounds `x-amz-copy-source-range` is `400 InvalidArgument`: the reference
  documents only `InvalidRequest` for an unsupported byte-range *source*, and unlike
  a `GET`'s advisory `Range` header — malformed is ignored, past-EOF is clamped — a
  copy-source range is part of the request's meaning, so a range substrate cannot
  honor is refused rather than silently turned into a different copy. And
  `bytes=0-` is refused where a `GET` would accept it, because an open-ended range
  does not describe the extent of a part.

### Fixed
- **Every network interface a launch declares is parsed and reported** (#455).
  `NetworkInterface.N.*` was read for **N=1 only**, in both `runInstances` and
  `createLaunchTemplate`, and `EC2Instance` had no interface list to hold a second
  one — so a launch or a template declaring two interfaces was answered `200` and
  quietly became a one-interface instance, with no error and nothing in the response
  to reveal the loss.

  Both sites now parse all N, following the same convention every other indexed list
  in the plugin follows: contiguously from 1, stopping at the first missing index.
  **Identity is keyed on `DeviceIndex`, not the parameter index** — the reference
  defines `DeviceIndex` as the position in the attachment order and does not require
  it to agree with the position the request writes the interface at, so
  `NetworkInterface.1.DeviceIndex=3` with `NetworkInterface.2.DeviceIndex=0` makes the
  *second* one primary. An implementation that used the parameter index would name the
  wrong interface primary and put the wrong subnet on the instance.

  `RunInstances` and `DescribeInstances` now both report `networkInterfaceSet>item` —
  emitted because the `RunInstances` reference's own sample response carries it, so a
  caller parsing the documented shape found a member missing. The instance's flat
  `subnetId`, `privateIpAddress` and `groupSet` continue to describe the **primary**
  interface, which is what real EC2 puts at the top level; they are not superseded by
  the new set, and `AssociatePublicIpAddress` is still honored only on the primary, as
  the reference requires ("can only be assigned to a network interface for eth0").

  `DeleteOnTermination` defaults per the reference's reasoning rather than to a single
  value: `true` for an interface the launch creates, `false` for an existing one it
  attaches by `NetworkInterfaceId`, since deleting an interface the caller brought
  would destroy something the launch did not make. An explicit value wins over either.

  The state and template shapes both grew a slice, so both keep their flat fields as
  the primary's values: an instance or a launch-template version stored by an earlier
  substrate reads back with an empty slice, and the single-interface synthesis from
  the flat fields still runs for it, so a replayed event log still describes
  correctly. `docs/services.md` loses its "only interface index 1 is modeled" limit
  and gains a section stating what substrate decides where the API model does not —
  how a secondary interface's address is assigned, that there are no standalone ENI
  resources so a launch declaring none reports an empty set rather than a phantom
  interface, and that interfaces report `in-use`/`attached` immediately because
  substrate's instances are already `running` when `RunInstances` answers.
- **A Lambda function reports the size and digest of the package it was deployed
  from** (#545). `CodeSize` was `0` and `CodeSha256` empty for every function whose
  code did not arrive as an inline base64 `ZipFile` on a direct `CreateFunction` — so
  a consumer polling `CodeSize` to decide whether a deploy took effect was told
  nothing had, whatever it deployed.

  **The issue's premise was confounded, and correcting it doubled the change.** It was
  filed as a CloudFormation problem. Measuring all four combinations against v0.89.0
  showed the axis is **inline vs. S3 source**, not CFN vs. direct:
  direct + `ZipFile` = **156** (correct), direct + S3 = **0**, CFN + S3 = **0**, and
  CFN + inline `ZipFile` = **0** — the row the report never tested, and the one that
  proves there were two independent gaps rather than one.

  **CloudFormation sent no `Code` at all.** `deployLambdaFunction` built a
  `CreateFunction` body carrying `FunctionName`, `Runtime`, `Role`, `Handler`,
  `Description`, `Timeout` and `MemorySize` and no `Code` key of any kind, so every
  CFN-deployed function reported size 0 and an empty digest however its template
  declared its code, a container-image function lost both its image and its
  `PackageType`, and `UpdateFunctionCode` drift was undetectable. All of `ZipFile`,
  `S3Bucket`, `S3Key`, `S3ObjectVersion` and `ImageUri` are now forwarded, each
  through the template's `Ref` and pseudo-parameter resolution like any other
  property.

  **An inline template needed more than forwarding, which is why fixing it was not a
  one-line change.** The resource type's `Code.ZipFile` is "the source code of your
  Lambda function" — "CloudFormation places it in a file named `index` and zips it to
  create a deployment package" — while the API's `Code.ZipFile` is "the base64-encoded
  contents of the deployment package". Forwarding the template's string would have
  handed Lambda something that is not a package and usually not valid base64, leaving
  `CodeSize` at 0 exactly as before. Substrate now builds the archive CloudFormation
  would have built: one entry named `index` with the extension the runtime reads
  (`.js` for `nodejs*` per the reference, `.py` for `python*`, bare `index`
  otherwise). The archive carries no timestamps, so the same template deployed twice
  yields the same package and the same digest — a digest that moved on every deploy
  would not be worth comparing.

  **An S3-sourced package is now sized.** Both `createFunction` and
  `updateFunctionCode` read the object's recorded length out of substrate's own S3
  state, honouring `S3ObjectVersion`. `ZipStored` stays `false` — the bytes are still
  not staged for execution, which is a separate and correct fact from knowing the
  size.

  **`UpdateFunctionCode` stopped randomising the digest.** It assigned `CodeSha256` a
  fresh random value on every call, so a caller asking "did the code change?" was
  always told yes. The digest is now derived from the package, and an update carrying
  no package at all changes neither the size nor the digest. `RevisionId` is not a
  digest of anything and still advances on every call. An image-packaged function's
  digest tracks its image URI, and is now reported from creation rather than only after
  the first update — appearing on one path and not the other was a difference a caller
  had no way to account for.

  Two decisions here are substrate's own rather than the API model's, and
  `docs/services.md` states both. The digest of an **S3-sourced** package is the
  object's **ETag**, not a SHA256: substrate never fetches the bytes, and for a
  single-part upload the ETag is the MD5 of the body, so it changes exactly when the
  package changes — the question a caller comparing digests is actually asking. And an
  **absent** S3 object does not fail the create the way real Lambda would; substrate's
  S3 and Lambda state are independent and a template may legitimately name an object a
  test never uploaded, so it logs and reports size 0. A deleted object — one hidden by
  a delete marker — counts as absent rather than as a zero-length package.

  Two adjacent gaps surfaced while fixing this and are fixed with it, since leaving
  either would have made the change a write with no observable — the shape this whole
  release is about. `Code.ImageUri`, the documented spelling, was accepted only at the
  top level, and now wins over the legacy field and infers `PackageType: Image`. And
  `GetFunction` reported `RepositoryType: S3` with a stub presigned URL for an
  image-packaged function, which is a claim a caller can act on and be wrong about; it
  now reports `RepositoryType: ECR` with `ImageUri` and `ResolvedImageUri` and no
  `Location`.

- **A Batch compute environment, job queue and job definition can be read back after
  it is created** (#530). `DescribeComputeEnvironments`, `DescribeJobQueues` and
  `DescribeJobDefinitions` were unrouted, so each answered an unknown-operation error
  while `DescribeJobs` — the one read that was routed — worked. A test could perform
  the write and had no way to verify it.

  **The issue's premise was wrong, and correcting it made the change larger than
  filed.** It reported these as read handlers over state the creates already
  persisted. None of the three creates persisted anything: each unmarshalled only the
  name out of the body, echoed it back, and never touched the state manager. So the
  whole request body — `type`, `state`, `priority`, `computeEnvironmentOrder`,
  `containerProperties`, every member a describe has to report — was not merely unread
  but never stored, and the creates had to start recording before there was anything
  to answer from.

  Two consequences of that came with it. All three creates took `_ *RequestContext`
  and minted a hardcoded `arn:aws:batch:us-east-1:000000000000:…` for every caller,
  which is wrong for any caller outside that account and Region and propagates,
  because that ARN is the identifier `computeEnvironmentOrder` and `SubmitJob` take;
  resources are now recorded against and reported to the caller's own account and
  Region, like every other partitioned plugin. And `RegisterJobDefinition` answered
  `"revision": 1` unconditionally, so registering one name twice was
  indistinguishable — each registration is now its own revision and its own record,
  which is what makes `${name}:${revision}` addressable at all.

  The reads honour the documented filters (`computeEnvironments`/`jobQueues` as names
  or full ARNs, `jobDefinitions` as `${name}:${revision}` or an ARN,
  `jobDefinitionName` for every revision of a definition, `status`) and paginate on
  `maxResults`/`nextToken`, omitting the token once the results are exhausted. An
  absent filter reports every resource in scope; a filter entry naming something that
  does not exist is skipped rather than refused, since the operations document no
  not-found error. `jobDefinitions` wins outright over `jobDefinitionName` when both
  are sent, per the reference's "can't be used with other parameters". A bad request
  is a `ClientException` at HTTP 400, the only 400-class error any Batch operation
  declares.

  A registered definition is `ACTIVE` and the `status` filter selects on it. Nothing
  yet reports one `INACTIVE`, because `DeregisterJobDefinition` is not routed either
  — filed separately as #555 rather than widening this — but the status is stored on
  the resource rather than synthesised at read time, so a deregistration can set it.
- **An S3 bucket notification configuration submitted the way the API documents it
  is now parsed, stored, reported back — and dispatched** (#542). This is not a
  lost-configuration bug. It is a working feature that could not be turned on: every
  S3 event notification configured through the real API was silently off.

  `PutBucketNotificationConfiguration` unmarshalled the request body into
  `S3NotificationConfiguration`, a struct carrying only `json` tags. Go's XML decoder
  falls back to matching Go field *names* when a field has no `xml` tag, and none of
  the API's element names are substrate's field names: the wire names each
  configuration in the **singular** (`TopicConfiguration`, `QueueConfiguration`,
  `CloudFunctionConfiguration`), names the destination after the service rather than
  the field (`Topic`, `Queue`, `CloudFunction` — not `TopicArn`, `QueueArn`,
  `LambdaFunctionArn`), repeats a bare `Event` per event rather than nesting under
  `Events`, and names the key filter's inner element `S3Key` rather than `Key`. So a
  real-S3 body matched **nothing**.

  What made it invisible is that `xml.Unmarshal` reports **no error** for a body
  whose elements match no field. The handler's JSON fallback therefore never ran, an
  empty configuration was persisted, and the request answered `200`. Since an empty
  configuration is also the documented way to *disable* notifications, the effect of
  configuring a notification the AWS way was to turn notifications off.

  `fireNotifications` reads that same record, which is the more serious half and was
  not in the report: with an empty configuration stored, no event was ever
  dispatched. A test asserting a message arrives failed far from its assertion, and
  one asserting no message arrives passed vacuously.

  The remedy follows the one #528 established for CloudWatch Logs, because the struct
  is dual-role — it is also the persisted encoding, so retagging it would have fixed
  the wire and changed the stored format in the same edit, stranding existing
  records. The state struct and its `json` tags are untouched; a set of wire types
  with the correct `xml` element names sits beside them, with projections both ways,
  and `EventBridgeConfiguration` is now recognized, stored and reported (delivery to
  EventBridge is still not dispatched — substrate has no bus-to-target path for S3
  events, and `docs/services.md` says so). `fireNotifications` and the API read now
  share one loader, so a read-back that passes while delivery stays broken is no
  longer reachable.

  **A non-empty body naming no recognized element is now `400 MalformedXML`.** This
  refusal is substrate's own choice, not the API model's: without it, a body with the
  wrong element names is indistinguishable from a deliberate disable, which is
  exactly how this defect stayed invisible. Substrate's pre-existing JSON body form,
  keyed on the state shape's `json` tags, is still accepted.

  Two existing tests are worth naming. The round-trip test passed throughout, because
  a struct marshalled and unmarshalled by its own definition agrees with itself
  whatever its tags say — the lesson #528/#529 already paid for, and why every new
  assertion here is against raw wire bytes. And a delete-bucket sub-test had a
  comment explaining that it wrote a substrate-shaped body *because* the AWS-shaped
  one stored nothing; it now writes the AWS shape, so what it pins is again the
  delete clearing the key.

- **The stack ARN `CreateStack` returns is now an identifier every stack-scoped
  operation accepts** (#544). Substrate looked a caller's `StackName` up verbatim, so
  the `StackId` it had just handed back resolved to no stack — even though the
  reference documents both forms for each of these operations: "The name or the unique
  stack ID that's associated with the stack, which aren't always interchangeable."

  `DeleteStack` is the case worth stating plainly, and the reason this is a
  silent-success bug rather than a lookup gap. It documents no not-found error, so
  substrate treats an unresolvable name as a stack already gone and succeeds. Handed
  an ARN, it swept nothing, answered `200`, and left the stack standing — and v0.89.0
  *widened* the gap by shipping #518's sweep, so a delete by name now really tears
  resources down while a delete by ARN did nothing. A caller had no error to catch and
  no status to poll; the next `CreateStack` answering `AlreadyExists` was the only
  symptom.

  One resolver runs at the plugin boundary, modelled on the `cfnChangeSetName` this
  repository already had, so the deployer keeps taking a name and its nine
  `"stack:"`-keyed lookups are untouched. It is applied to every operation that takes
  a `StackName` — thirteen call sites, not the two the report named, since fixing
  describe and delete alone would leave the same trap in `GetTemplate`, the change-set
  family and the drift pair, which are exactly the operations a caller reaches holding
  a `StackId`. In `DeleteStack` it runs **ahead** of the absent-stack tolerance; that
  ordering is the fix. `CreateStack` deliberately does not use it: its `StackName` has
  no unique-stack-ID alternative, because the ID does not exist until that call mints
  it.

  **The ARN is verified, not merely parsed.** Substrate builds a stack ARN from the
  caller's partition, Region and account plus a digest over those and the name, so the
  whole string is recomputable — and it is compared whole, which checks every
  component at once rather than field by field. Lifting the name out without that
  check would have let a caller reach, and `DeleteStack` tear down, a stack in another
  account, Region or partition by hand-writing an ARN: a defect worse than the silent
  no-op being fixed. An out-of-scope ARN reports the same `ValidationError` an absent
  stack does, deliberately — a stack outside the caller's scope is one the caller
  cannot observe, so a distinct error would disclose whether another scope holds a
  stack by that name.

## [v0.89.0] - 2026-08-03

### Added
- **`DeletionPolicy` and `UpdateReplacePolicy` on a template resource** (#518), which
  no template could previously declare — the attributes were dropped on parse in both
  the JSON and YAML paths. A value outside `Delete`/`Retain`/`RetainExceptOnCreate`/
  `Snapshot` is now a template error rather than a silent fall back to the default: a
  typo'd `Retian` that deletes the resource is precisely what the attribute exists to
  prevent, and CloudFormation itself rejects the template.

  The default is resolved rather than assumed, because it is not uniformly `Delete`:
  "the default policy is `Snapshot` for `AWS::RDS::DBCluster` resources and for
  `AWS::RDS::DBInstance` resources that don't specify the `DBClusterIdentifier`
  property". A sweep that assumed `Delete` would destroy a database the template asked
  to be snapshotted. `Snapshot` does **not** retain — substrate deletes the resource
  and says in the per-resource reason that no snapshot was taken, since no snapshot
  resource is modelled for any of the eight Snapshot-capable types.
  `RetainExceptOnCreate` retains for a `DeleteStack` but not for the rollback of the
  create that made the resource, so the operation is passed into the policy resolver
  rather than special-cased by a caller. `UpdateReplacePolicy` is parsed and reported
  but nothing consults it yet: `UpdateStack` is a re-deploy rather than a per-resource
  replace, so a template that declares it round-trips instead of being silently
  dropped.

### Fixed
- **`DeleteStack` now deletes the stack's resources, not just the stack record**
  (#518). Every resource a stack created outlived it: create a stack holding a bucket,
  delete the stack, and `head-bucket` still answered 200 — a test that tore down and
  asserted cleanliness passed for the wrong reason. `DeleteStack` swept nothing at
  all, and because the stack record is what *names* the resources, removing it first
  stranded them with no way left to find them.

  The sweep runs before the record is dropped, in the exact inverse of the deploy:
  descending deploy priority, ties by descending logical ID. Substrate deploys in
  priority order rather than from a dependency graph, so inverting that order is what
  makes teardown ordering observable and assertable — the recorded events for a stack
  of a role, a bucket, a queue and a topic read `CreateRole, CreateBucket,
  CreateQueue, CreateTopic` and then `DeleteTopic, DeleteQueue, DeleteBucket,
  DeleteRole`.

  Deletes are declared in a per-type table that **returns** the request rather than
  performing it, so the sweep owns ordering, error handling and event recording once
  and each entry is a data declaration. Three things the table deliberately does not
  do. It does not assume the physical ID is the delete identifier: SQS records the
  queue *name* while `DeleteQueue` requires a `QueueUrl`, a standalone security-group
  rule records its *group* because EC2's opaque `sgr-` ID is not modelled (so the
  revoke restates the permission the authorize granted, since a revoke built any other
  way silently removes nothing), and an AppSync resolver's is `TypeName.FieldName`
  while the path wants the two separately. It does not guess an operation from the
  type: KMS has no `DeleteKey` at all, so its entry dispatches `ScheduleKeyDeletion`,
  and neither an SNS topic policy nor a Route 53 record set has a delete of its own —
  the first is an attribute set back to empty, the second a change batch carrying
  `Action DELETE`, each the mirror of the call that created it. And it does not read a
  parent ID off the resource: the composite types re-resolve theirs from the stored
  template, which is where an API Gateway method's API *and* resource come from.

  A resource that is already absent is a **success**. A resource deleted out of band
  between the deploy and the sweep must not wedge its stack, so the sweep tolerates 33
  not-found codes — each one measured by dispatching that type's delete against an
  empty registry and recording what came back, rather than read off a reference and
  hoped for.

  Any other refusal is a failure, and a failed sweep **keeps** the stack: its record,
  its resource list and its name index all survive, and `DescribeStacks` reports
  `DELETE_FAILED` with the offending resource and the plugin's own error code as the
  reason. A stack that reported a failed delete and then vanished would be a worse lie
  than the leak — a caller would have no way to retry and no way to learn what held
  it. Only a fully successful sweep removes the record. Deleting a stack that does not
  exist stays a success, as it was: `DeleteStack` documents no not-found error.

  On the wire the failure is reported the way the API reports it: `DeleteStack`
  "returns success" and the stack reaches `DELETE_FAILED`, which a caller learns by
  polling `DescribeStacks`. Raising it on the call would be wrong twice over — a
  caller following AWS semantics would get an exception where the API gives it a
  status to poll, and the 500 it would have to be makes an SDK **retry** the delete,
  sweeping a stack already in `DELETE_FAILED`. The in-process deployer still returns
  an error, so a Go caller sees the failure directly.

  Coverage is **stated**, not implied. Of the 109 types the deployer dispatches, 89
  have a delete request, 11 are state-only types whose stub record is removed, and the
  remaining 9 report `DELETE_SKIPPED` with a reason distinguishing "AWS models no
  delete" from "substrate does not route the one it has" — different facts, and only
  the second is substrate's to fix. A type the deployer does not recognize at all also
  skips, but its `cfn_stub` key is now removed rather than left for a redeployed stack
  to read as the previous one's properties. Reporting `DELETE_COMPLETE` for any of
  these would claim a deletion that did not happen, and a false claim of cleanliness
  is worse than a stated gap. `docs/services.md` enumerates all nine.

- **`DeleteBucket` now clears the bucket's namespace instead of only its `bucket:`
  record** (#508). A bucket-scoped configuration is keyed by bucket name alone, so
  every one of them outlived its bucket and was inherited by the next bucket created
  with that name: create a bucket, enable versioning, delete it, create it again, and
  writes to the "new" bucket came back versioned. All six are now cleared —
  `bucket_acl:`, `bucket_policy:`, `bucket_lifecycle:`, `bucket_versioning:`,
  `notification:`, `bucket_public_access_block:`. Two corrections to the issue's
  table: the notification prefix is `notification:`, not `bucket_notification:`, and
  **bucket tagging never leaked**, because `PutBucketTagging` writes `Tags` into the
  `bucket:` record rather than under a key of its own.

  Errors **propagate** and the `bucket:` record is deleted **last**, so a failed
  clear leaves the bucket present and retryable rather than half-deleted. A
  discarded `Delete` error is how the leak went unnoticed in the first place.

  Object-scoped leftovers go too. An object ACL and a version index are stored under
  their own keys and outlive the object they describe, so an *empty* bucket could
  still own state that a recreated bucket inherited.

- **`DeleteBucket`'s emptiness check counted only live objects** (#508, found while
  fixing it and not reported in the issue). "All objects (including all object
  versions and delete markers) in the bucket must be deleted before the bucket
  itself can be deleted", but the check listed only `object:<bucket>/`, so a bucket
  holding nothing but a deleted object's history deleted "successfully" and stranded
  every version. It now counts `object_version:` too, which covers both noncurrent
  versions and delete markers. The sharpest case is a **versioning-suspended**
  bucket: substrate's `DELETE` removes the current-version pointer outright there,
  so the bucket looked empty while the version it wrote was still present — and real
  S3 keeps that version as well, since "when you suspend versioning, existing
  objects in your bucket do not change".

  In-flight multipart uploads are **aborted rather than refused**, correcting this
  release's own plan. The refusal is documented for *directory* buckets, which
  substrate does not model; for general purpose buckets the reference only
  recommends that you "remove all incomplete multipart uploads" while emptying. An
  upload whose destination bucket is gone can never be completed, so the delete
  aborts it — part records, upload record and part bodies — through the same helper
  `AbortMultipartUpload` now uses, so the two paths cannot leave different residue.
  Only uploads whose `Bucket` field names the bucket being deleted are touched; an
  upload is keyed by upload ID alone, so the scan sees every bucket's.

- **Permanently deleting an object version now updates the current-version pointer**
  (#508). Deleting a version by ID removed its record and left the `object:` pointer
  stale, which made a versioned bucket impossible to empty — so `DeleteBucket`'s
  widened emptiness check could never have been satisfied. Deleting the current
  version promotes the next one, since "if you delete the current object version,
  ... the version that is next in the version stack becomes the current version";
  deleting the last version removes the key entirely. The promoted version's **body**
  moves with its record: a versioned `PUT` writes the body both under
  `.versions/<key>/<versionID>` and at the unversioned path an unversioned `GET`
  reads, so promoting the record alone served the deleted version's bytes under the
  promoted version's ETag. A delete marker has no body to promote, which an absent
  versioned body is treated as rather than as an error, and the promotion honours
  #534's `s3ObjectHasBody` guard — afero reads a directory as an empty body with no
  error, so an unguarded promote would truncate the object at `dir` while promoting a
  version of the marker `dir/`.

- **Deleting a directory-marker object no longer strands the keys beneath it**
  (#534). `PUT dir/`, `PUT dir/a.txt`, `DELETE dir/`, `DELETE dir/a.txt` — the last
  call returned **HTTP 500**. S3 keys are opaque strings, but the afero filesystem
  the plugin mirrors object *bodies* into is hierarchical, and `filepath.Clean`
  normalizes `/bucket/dir/` to `/bucket/dir`: the same path as the directory node
  `MkdirAll` created to hold `dir/a.txt`. Removing the marker therefore removed that
  node, orphaning its children, and the child's own delete then panicked inside
  `MemMapFs` and surfaced as a 500. The multi-object `DeleteObjects` inherited it,
  since it delegates per key.

  The write path already had the right guard — a marker is never written to the
  mirror, because its body is always empty — and the delete and copy paths simply
  did not. All six sites now go through one `s3ObjectHasBody(key)` predicate rather
  than four hand-copied `strings.HasSuffix` checks, which is the drift that produced
  the bug. This also closes two adjacent cases the reproduction did not name: `dir`
  and `dir/` are **distinct S3 keys** that collide on a single mirror path, so a
  marker operation used to truncate or delete the body of the object one character
  away from it; and a marker's versioned path `.versions/dir//<versionID>`
  normalizes onto the directory holding the versions of a child key named
  `dir/<versionID>` — reachable, not theoretical, since a client reads that version
  ID off the marker's own `PUT` response. `CopyObject` over a marker now succeeds
  in both directions, yielding the empty object a marker is, where reading one as a
  source used to fail outright.

- **A create whose resource failed now rolls the stack back** (#520). A stack holding
  a resource a plugin refused still reported a terminal `CREATE_COMPLETE`, with every
  resource beside it left in place — so a consumer's failure-handling path could not
  be tested at all, because substrate never produced the failure that path exists to
  handle. The default is `ROLLBACK`, as the API's is: the resources the create had
  already made are swept through #518's dispatcher in the same reverse order a
  `DeleteStack` uses, and the stack reaches `ROLLBACK_COMPLETE` naming the failed
  resource and the plugin's own error code. The stack record stays, because a
  `ROLLBACK_COMPLETE` stack still blocks a create of the same name — which is the one
  thing that makes the failure impossible to miss.

  `CreateStack`'s two failure parameters are read and honoured for the first time.
  They are **mutually exclusive** — "you can specify either `DisableRollback` or
  `OnFailure`, but not both" — so giving both is a `ValidationError` rather than a
  precedence rule, and the test is *presence* rather than value: the CLI's
  `--no-disable-rollback` sends `DisableRollback=false` explicitly, so a rule that
  ignored a `false` would accept a combination the API rejects. `OnFailure=DO_NOTHING`
  (equivalently `DisableRollback=true`) leaves everything standing at `CREATE_FAILED`,
  which is what substrate used to do unconditionally; `OnFailure=DELETE` sweeps and
  removes the record, so the `DeployResult`'s `DELETE_COMPLETE` is the only place that
  outcome is readable. `DescribeStacks` now reports the option it was given rather
  than emitting `DisableRollback: false` for every stack. A sweep that cannot delete
  gives `ROLLBACK_FAILED` and keeps the record, so the undeleted resource is
  discoverable. All of these answer **200** on the wire: real `CreateStack` returns
  its `StackId` before any rollback happens, so a rolled-back stack is not a failed
  call, and returning a 5xx would make an SDK retry a create that already ran.

  A failed stack publishes **no outputs**, and therefore exports none — an import
  resolving against a value whose resource never deployed is the silent-literal
  failure the export model exists to prevent. A duplicate export name is still
  answered as an error rather than as a rolled-back stack, because it is a refusal of
  the *request*: a request substrate declines outright never became a stack whose
  resources could roll back, and the refusal now reads the same whether or not some
  resource beside it also failed.

  A failed `UpdateStack` reports `UPDATE_ROLLBACK_COMPLETE`, and the approximation
  behind that is stated rather than implied: `UpdateStack` is a re-`Deploy`, so the
  stored previous template is the only description of the previous state substrate
  holds, and the rollback re-deploys it. It converges on that template's *declared*
  state rather than restoring properties field by field, so a resource the failed
  update replaced may keep a new physical ID. A previous record that cannot be read
  leaves the stack at `UPDATE_FAILED` with the reason logged, rather than a rollback
  attempted against nothing.

- **An unchanged resource no longer looks like a failed create on every update**
  (#520), found while building the rollback rather than reported. Because
  `UpdateStack` is a re-`Deploy` of the whole template, every unchanged resource's
  create is re-issued and its plugin refuses it as already existing — where real
  CloudFormation issues no call at all for a resource it is not changing. Before
  rollback this was merely invisible: the refusal landed on the resource's error field
  and the stack still said `UPDATE_COMPLETE`. With rollback added it becomes data
  loss, since every `UpdateStack` would see failures and sweep the resources it was
  asked to keep.

  So an already-exists refusal is cleared when — and only when — the stack's previous
  deployment created that logical ID *successfully* and the template still declares it
  identically, compared as canonically marshalled JSON. Each of those conditions is
  load-bearing: a rename into a name another stack owns, a resource that failed the
  previous time (so this stack never owned the name), and a record belonging to
  another account or region all remain real failures. The refusal cannot be keyed on
  the physical ID, because a refused create returns an empty one.

## [v0.88.0] - 2026-08-03

### Added
- **`ListExports` and `ListImports`** (#522), which were `InvalidAction` before there
  was anything to list. `ListExports` reports every export in the caller's account
  and Region with the `ExportingStackId` ARN `DescribeStacks` reports for the same
  stack, in one page with no `NextToken`; `ListImports` reports stack **names**, and
  an export nothing imports — including one that does not exist — is an empty list
  rather than an error, since the reference documents no service-specific errors for
  it. `DescribeStacks` additionally reports each output's `ExportName` beside it,
  saving a caller a second call to learn whether an output is importable.

- **CloudWatch Logs `PutRetentionPolicy` and `DeleteRetentionPolicy`** (#528), which
  returned `InvalidAction` while `RetentionInDays` was already modeled, settable
  through CloudFormation, and reported by `DescribeLogGroups` — so a retention a
  template set could be read but never changed. `retentionInDays` accepts only the
  22 values the reference enumerates (1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180,
  365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288, 3653); anything else,
  including a plausible-looking 45 or 100, is an `InvalidParameterException`, since
  accepting one would let a template pass under test that the real service rejects.
  The error message lists the valid values in ascending order so an assertion on it
  is stable. `Delete` ships with `Put` because it is the documented way to make a
  group's events never expire — there is no `retentionInDays` value meaning
  "never", so a group with no policy omits the member rather than reporting `0`.
  Both report `ResourceNotFoundException` at **HTTP 400**, as their references
  document: this service carries the error code in the body's `__type`, not the
  status line.

### Fixed
- **CloudWatch Logs read operations put camelCase members on the wire, so an SDK
  no longer parses every field to null** (#528). `DescribeLogGroups`,
  `DescribeLogStreams` and `GetLogEvents` emitted **PascalCase** members where this
  JSON-1.1 service uses camelCase. An SDK matches response members against the
  service model case-sensitively, so a PascalCase member does not fail to parse —
  it parses to *nothing*. The caller received one empty object per resource with an
  HTTP 200 and no error: the count was right and every field read raised
  `KeyError`, which is why a `len()` assertion passed. Reported against
  v0.87.1/botocore 1.42.59 with a reproducer using a directly created group, so
  this was never CloudFormation-specific.

  `FilterLogEvents` was already correct, and that is what made the defect easy to
  miss — a smoke test using it passes — because it alone declared its own response
  element type instead of reusing the state struct. That is the pattern the fix
  copies: each read now projects `CWLogGroup`/`CWLogStream`/`CWLogEvent` onto a
  response-only type. Retagging the state structs would have been the shorter edit
  and the wrong one: those structs are also the **persisted** encoding that
  `Snapshot`/`Restore` round-trips and `betty_debug` replays, and
  `LambdaPlugin.autoCreateLambdaLogGroup` writes one directly to dodge a registry
  cycle, so a retag would have changed the wire and the on-disk format together. A
  v0.87 snapshot still restores unchanged.

  `DescribeLogGroups` now reports **both** ARN forms the reference documents as
  distinct members, differing only in a trailing `:*`: `logGroupArn` without it,
  which is what a `logGroupIdentifier` input or a tagging API wants, and `arn` with
  it, which is what an IAM policy wants for every other action. Substrate
  previously emitted its single unsuffixed ARN under `ARN`, so a caller who did
  reach the value got the form the real service rejects in a policy.
- **`Fn::Split` resolves to a list, so a split list no longer loses everything
  after its first element** (#521). `resolveValue` returns a `string`, so every
  list-valued intrinsic had nowhere to put a list: `resolveFnSplitFirst` was
  honest about it in its own name, and a container command of `python,-m,worker`
  deployed as `python` while the resource reported `CREATE_COMPLETE`. That was
  reachable from conventional YAML only from v0.87.1 onward — before #516 the
  whole `!Split` tag was dropped before the resolver saw it.

  The resolver gains a list-returning sibling, `resolveValueList`, which the
  list-valued property paths call. `resolveStringList` delegates to it, so all its
  existing call sites gain full split results with no signature change. Its
  conventions are the ones CloudFormation states: a scalar is a one-element list;
  `Ref AWS::NoValue` contributes **no** element, which is what makes
  `!If [HasCommand, !Split [',', !Ref Command], !Ref 'AWS::NoValue']` mean what it
  says; a nested list-valued intrinsic splices rather than nesting, since a list
  of lists is not a shape any AWS API member has; and empty elements are preserved,
  because `!Split ['|', 'a||c|']` is documented to return `["a", "", "c", ""]`.

- **`Fn::Select` over `Fn::Split` resolves** (#521). This is the idiom the AWS
  `Fn::Split` reference itself leads with — `!Select ['2', !Split [':', arn]]` —
  and it silently produced the empty string: `resolveFnSelect` required its second
  argument to be a literal list, so every one of the six functions the `Fn::Select`
  reference permits there (`Fn::FindInMap`, `Fn::GetAtt`, `Fn::GetAZs`, `Fn::If`,
  `Fn::Split`, `Ref`) resolved to nothing with no error reported. The list argument
  now goes through `resolveValueList`.

- **A `Ref` to a `CommaDelimitedList` or `List<…>` parameter is list-valued**
  (#521). `SecurityGroupIds: !Ref SubnetIds` reached the API as one string
  containing commas rather than as several IDs. Which parameters are list-valued
  comes from the **declared type** rather than from whether a value happens to
  contain a comma — a `String` parameter holding `a,b` is one value — and each
  member is space-trimmed, as the Parameters reference specifies.

- **`Fn::Split` in a scalar context rejoins rather than truncates** (#521). A
  scalar property has nowhere to put a list, and real CloudFormation rejects the
  template; substrate resolves rather than rejects, so of the two ways to spell a
  list as one string it now picks the one that loses nothing.

- **A multi-key map is no longer resolved as an intrinsic** (#521). Every
  intrinsic CloudFormation defines is a one-member object, but the resolver walked
  a map of any size and returned whichever recognized key Go's map iteration
  reached first — so a property holding both `Ref` and another member resolved two
  different ways across runs. Nondeterminism is the sharper half of the defect: it
  is the one outcome an emulator built on deterministic replay must never produce.
  Such a map now falls through to its JSON encoding, which also leaves user data
  that happens to carry a member named `Ref` — an ECS container definition, an IAM
  policy document — untouched.

- **Intrinsics resolve at any depth inside a structured property** (#526).
  `resolveValue` resolves a value that *is* an intrinsic; nothing walked into a map
  or a list to resolve one nested within. So a deploy path that forwarded a
  structured property whole handed the plugin `{"Ref": "PK"}` as a literal object:
  DynamoDB's typed plugin rejected it (`SerializationException: cannot unmarshal
  object into Go struct field … of type string`, the resource `CREATE_FAILED`) while
  an untyped plugin stored it and echoed it back. Which of a consumer's properties
  resolved depended on how each deploy path happened to have been written — some
  enumerate their members key by key, others forward them verbatim.

  A new `resolveNested` walks a property and returns a structurally identical value
  with every intrinsic resolved, applied at the verbatim-forwarding sites: ECS's
  `ContainerDefinitions`, DynamoDB's `KeySchema`, `AttributeDefinitions`,
  `ProvisionedThroughput`, `GlobalSecondaryIndexes`, `LocalSecondaryIndexes` and
  `StreamSpecification`, ACM's `SubjectAlternativeNames`, MSK's `BrokerNodeGroupInfo`
  and FSx's `SubnetIds` and `Tags` — the last three had asserted their property was a
  literal `string` and silently dropped anything else, so a `!Ref`-valued subnet list
  or the near-universal `Value: !Sub '${AWS::StackName}-data'` tag never reached the
  API at all. Four rules a naive walk gets wrong are pinned by tests: only a
  single-key map is an intrinsic (a multi-key map is user data even when one key is
  `Ref` — the same map-iteration nondeterminism #521 fixed a level up); a
  list-valued intrinsic in a list position splices its elements; an `Fn::If`
  yielding `AWS::NoValue` **removes** the property rather than leaving an empty
  string behind; and resolution never rewrites a key, since that is what would
  mangle a `logConfiguration.options` whose keys are user data.

- **ECS container definitions reach the ECS API under the ECS API's member names**
  (#527). CloudFormation spells a container's members in PascalCase where the ECS
  API spells them in camelCase, and the CFN deploy path forwarded them as written.
  `ContainerDefinitions` is untyped both in `RegisterTaskDefinition`'s request and
  in state, so there was nothing to reject them: the stack reached
  `CREATE_COMPLETE`, `DescribeTaskDefinition` answered `200`, and
  `--query 'taskDefinition.containerDefinitions'` returned `[{}]` — every member
  present under a name no SDK reads. A direct `RegisterTaskDefinition` call was
  never affected; the mismatch was introduced by the deploy path alone.

  The mapping is an explicit table of all 42 `ContainerDefinition` members plus the
  11 nested types (`environment`, `secrets`, `portMappings`, `logConfiguration`,
  `mountPoints`, `volumesFrom`, `healthCheck`, `ulimits`, `dependsOn`,
  `extraHosts`, `systemControls`), verified member by member against each type's own
  API reference page. It is deliberately a table rather than a first-letter-lowering
  function — which would be right for all 42 today — because only a table can tell
  "not mapped" from "mapped to itself", so a member added to the API later passes
  through verbatim instead of being renamed to something ECS does not accept. It is
  also deliberately **per-service**: DynamoDB, ACM and MSK are natively PascalCase,
  and a generic converter is precisely the change that would break them.
  `logConfiguration.options` and `dockerLabels` are named by their parent's table and
  then left whole, their keys being user data.

- **A template's `Mappings` section is read, and `Fn::FindInMap` resolves against
  it** (#522). `cfnTemplate` had no `Mappings` field, so the section every
  region→AMI template carries was discarded at parse time and the intrinsic fell
  back to its JSON encoding: the near-universal
  `ImageId: !FindInMap [RegionMap, !Ref 'AWS::Region', AMI]` reached `RunInstances`
  as the literal string `{"Fn::FindInMap":["RegionMap",…]}` and **the instance
  launched**, the resource reporting `CREATE_COMPLETE`.

  All three levels resolve, including the nested form the reference leads with,
  where the second-level key is itself a `Ref` or a `Fn::FindInMap`. A leaf may be a
  string or a **list**, since "the values can be of type String or List", so a
  list-valued lookup contributes its members to a list-valued property and is
  rejoined on commas in a scalar one. The optional fourth argument is honored — but
  only spelled exactly `{DefaultValue: …}`, because a map with any other key is not
  a default and guessing at the intent is how a template gets a value neither
  CloudFormation nor the author asked for.

  A lookup that misses now **fails the resource** — `CREATE_FAILED` with a
  `ResourceStatusReason` naming the intrinsic and the key that missed — rather than
  falling back to the literal. That is the whole point of modelling `Mappings` rather
  than tolerating it: a nonsense `ImageId` reported as success is the defect, and a
  template CloudFormation would have rejected must not deploy. The rest of the stack
  still deploys, matching the per-resource failure reporting #519 established.
  Reporting it needed a new channel — a resolver returns a `string`, in which "no
  such key" and "the key held an empty string" are the same value — so `cfnContext`
  now accumulates failures, drained per resource onto `DeployedResource.Error`, which
  is already what `DescribeStackResources` derives `CREATE_FAILED` from.

- **`Fn::GetAZs` resolves, to the same zones `DescribeAvailabilityZones` reports**
  (#522). A subnet placed with the conventional `!Select [0, !GetAZs '']` had the
  intrinsic's JSON encoding as its `AvailabilityZone`. It now derives from the same
  seeded zone list EC2's own `DescribeAvailabilityZones` answers from rather than
  carrying a second list, so the two cannot disagree — a subnet in a zone the
  emulator does not report is not something a caller can then query, and two
  independent lists is how that happens. An empty string means the caller's region,
  as the reference specifies.

- **`Fn::Cidr` resolves, for IPv4 and IPv6** (#522). `!Cidr ['192.168.0.0/24', 6, 5]`
  yields six `/27`s and `!Cidr ['2001:db8::/56', 1, 64]` a `/64`, the mask taken
  from the address family rather than assumed — both are the reference's own
  examples, and both are asserted against it. A request the block cannot satisfy
  fails the resource rather than returning a short list: a `count` larger than the
  number of blocks that fit, a `cidrBits` that would widen the block, a `count`
  outside the documented 1–256, or an `ipBlock` that is not a CIDR block. A short
  list is the worse failure, because `!Select [3, !Cidr [...]]` reads an empty string
  out of it and deploys.

- **The four remaining pseudo-parameters resolve** (#522), which closes the
  unresolved list rather than shortening it. `AWS::Partition` and `AWS::URLSuffix`
  follow the region (`aws-cn`/`amazonaws.com.cn`, `aws-us-gov`, else
  `aws`/`amazonaws.com`), so an ARN a template builds with
  `!Sub 'arn:${AWS::Partition}:s3:::${Bucket}'` is an ARN rather than a string with
  a placeholder in it. `AWS::NotificationARNs` is an **empty list** — substrate has
  no notification model, and empty is the accurate answer for a stack created
  without any, where the reference string was not.

  `AWS::StackId` resolves to the stack's ARN, and the builder is now shared with the
  wire's `CreateStack`/`DescribeStacks` rather than duplicated: a consumer that
  captures `StackId` from `CreateStack` and compares it against a resource property
  built from `AWS::StackId` must find one ARN for one stack, and two builders is
  exactly the divergence #517 fixed for the caller's account.

- **`Fn::ImportValue` resolves, against a cross-stack export registry** (#522), which
  closes the intrinsic list: every function CloudFormation defines now resolves. It
  was the last one falling through to its JSON encoding, so a template importing
  another stack's subnet ID deployed a resource whose `SubnetId` was the literal
  `{"Fn::ImportValue":"net-SubnetID"}` and reported `CREATE_COMPLETE` — the
  silent-success shape this whole release is about, and the worst instance of it,
  because the two-stack split is how every non-trivial template is organised.

  An output may now declare `Export: {Name: …}`, absent from `cfnOutput` until now.
  The name is an expression rather than a string, because the conventional form is
  `Export: {Name: !Sub '${AWS::StackName}-SubnetID'}` — an export name must be unique
  per account and Region, so a template that hard-codes one cannot be deployed twice.
  It resolves before the first resource deploys, which the API permits: an export
  name may not depend on a resource, so it is knowable from the template and its
  parameters alone.

  Exports are scoped **per account and Region**, per the documented restriction that
  cross-stack references are limited to the same account and Region. Getting this
  wrong in the permissive direction would be worse than not resolving at all: a
  template would pass under test and fail in AWS. `deleteStack` on the wire now uses
  the caller's deployer for the same reason — the stack record is unpartitioned, but
  the export visibility deciding the refusal is not.

  Four rules are enforced, and they are why exports are modelled rather than faked —
  an import that resolves against nothing enforceable is a lookup, not a reference:
  an import of an unpublished name **fails the resource** with the name in its
  `ResourceStatusReason` rather than resolving to `""` or to JSON; an export name
  already held by another stack is refused; a stack whose export is imported cannot
  be **deleted**; and an imported export's value cannot be **changed** — nor dropped,
  which is the same thing from the importer's side, so a removal is refused on the
  same terms. Re-deploying an unchanged value is not a change, so an idempotent
  redeploy still works. Both refusals are a `ValidationError` at 400 naming the
  export and every importing stack.

  What counts as an import is recorded **when the resolver walks the template**, not
  derived from the template's text, which is the one design decision here that a
  reader would otherwise get wrong: an `Fn::ImportValue` in the branch of an `Fn::If`
  that was not taken never happened, and neither did one that failed to resolve, so
  neither pins an exporting stack. Only the resolver knows which branch it walked.

  `Fn::ImportValue` was deliberately kept out of the intrinsic-name table until it
  resolved. `resolveNested` (#526) consults that table to decide what is an
  intrinsic, and admitting an unresolvable one would have resolved every nested
  import to `""` — strictly worse than the JSON fallback, which at least left the
  intrinsic visible in the request the plugin refused.

### Changed
- **A CloudFormation error code is derived from the failure's classification, not
  from its message text** (#502). `cfnMapDeployerError` recovered the AWS error code
  and HTTP status by running `strings.Contains` over the deployer's message, which
  coupled two things that have no business being coupled. Rewording
  `stack %q not found` — an ordinary copy-edit — silently turned a `ValidationError`
  at 400 into an `InternalFailure` at 500 for every consumer, with no compiler error
  and no failing test outside the plugin's own suite. And it was lossy the other
  way: `deploy resource %s: %w` wraps whatever the plugin returned, and a plugin's
  own message may well contain "not found" — an instance whose AMI does not resolve,
  say — so a genuine resource-level failure was reported as though the *request* had
  named an absent stack, at 400 rather than 500.

  `StackDeployer` now wraps one of `ErrCFNStackNotFound`,
  `ErrCFNChangeSetNotFound`, `ErrCFNDriftDetectionNotFound`,
  `ErrCFNTemplateInvalid`, `ErrCFNResourceDeployFailed` or `ErrCFNStateRequired`,
  and the plugin classifies with `errors.Is`. **Every message is unchanged byte for
  byte**, which is what makes this safe to land on its own: anything reading a
  substrate log sees exactly what it saw before, and only the classification is new.
  The classification lives in a field rather than being wrapped into the text with a
  second `%w`, precisely so that the text and the code stay independent — a test
  hands the mapping a message sharing no word with the old switch's strings and
  asserts the code still resolves.

  A failed resource additionally carries its logical ID on a typed
  `CFNResourceDeployError`, so a caller need not re-parse the message to learn which
  resource failed. `DescribeStackResources` reads a failure off the resource record
  rather than off this error (#519), but a deploy that fails outright returns before
  any record is written, so the logical ID has to travel with the error or it is
  lost.

  Landed here rather than deferred because #522's delete-refusal has no resource to
  blame and so must come back through `DeleteStack`'s error return — the trigger the
  issue's own analysis named for pulling this forward instead of widening the
  substring switch.

## [v0.87.1] - 2026-08-03

### Fixed
- **CloudFormation YAML short-form intrinsics now resolve** (#516). A template
  written with `!Ref`, `!Sub` or `!If` deployed with unresolved literals where a
  value should be: `go.yaml.in/yaml/v3` has no notion of CloudFormation's tag
  shorthands, so it dropped the tag and kept the node value. `!Sub 'x-${P}'` reached
  the resolver as the plain string `"x-${P}"`, indistinguishable from a literal the
  template really did intend, and `!If [C, a, b]` arrived as the raw array
  `["C", "a", "b"]` — which is what a reporting consumer saw stamped into an ECS
  task-definition family verbatim. The `Fn::`-prefixed long forms were unaffected
  throughout, so this was purely tag resolution.

  The template is now decoded into a `yaml.Node`, whose `.Tag` survives, and every
  recognized shorthand is rewritten into its long form before the node is decoded.
  All eighteen are handled — `!Ref`, `!Condition`, `!GetAtt`, `!Sub`, `!Join`,
  `!Select`, `!Split`, `!Base64`, `!If`, `!Equals`, `!Not`, `!And`, `!Or`,
  `!FindInMap`, `!ImportValue`, `!Cidr`, `!GetAZs`, `!Transform` — and the walk is
  depth-first, children before parents, because the tags nest: `!Not [!Equals
  [!Ref VpcId, '']]` is three levels deep and is the single most common condition
  idiom in real templates.

  `!GetAtt` is the one irregular expansion: it takes a dotted string where
  `Fn::GetAtt` takes a two-element list, and the split is on the **first** period
  only, since an attribute name may itself contain periods — AWS's own example maps
  `!GetAtt myELB.SourceSecurityGroup.OwnerAlias` to
  `["myELB", "SourceSecurityGroup.OwnerAlias"]`.

  A tag substrate does not recognize is **not** dropped: its value is kept and a
  `WARN` naming the tag is logged. Silently dropping it is the defect being fixed,
  and refusing the template would reject templates real CloudFormation accepts,
  since a macro or transform may introduce a tag substrate has never heard of.

  A short form's value is decoded as a string rather than having its YAML type
  resolved afresh, because CloudFormation values are strings and the long forms are
  written unquoted the same way. Otherwise `!Sub 12345` would arrive as an `int` and
  `!Ref 2026-08-02` as a `time.Time`, which matches none of the resolver's cases and
  would silently empty the property.

  Expansion is not resolution. `!FindInMap`, `!ImportValue`, `!Cidr` and `!GetAZs`
  now expand to long forms the resolver still ignores, and hit the same documented
  fallback the long forms already got. That is still an improvement — the two
  syntaxes now behave *identically*, which is the invariant at issue — but
  "expanded" must not be read as "resolved"; `docs/services.md` says so, and #522
  tracks the resolvers (`Fn::FindInMap` needs a `Mappings` model the template struct
  does not have).

- **`Fn::Sub` now honours the `${!Literal}` escape** (#516). `${!Count.Index}`
  renders as the literal `${Count.Index}` with no substitution, which is how a
  template passes a `${…}` through to something that interpolates it later. The
  escape was unreachable before the expander — with the whole `!Sub` discarded,
  `substituteTemplate` never saw the string — which is why it ships in the same
  change rather than separately.

- **A resource a plugin refused now reports `CREATE_FAILED`** (#519). The deployer
  set a resource's error only when the dispatched request returned a non-nil `error`,
  and never looked at the response status. But plugins signal a client error two
  ways, and both are in wide use: S3 (58 sites) and IAM (162) return a 4xx response
  with a nil error — the same shape a real endpoint puts on the wire — while EC2
  (49), ECS (27), CloudWatch Logs (20) and SQS (5) return an `*AWSError`. Only the
  second was ever inspected, so **every S3 and IAM resource failure in a stack was
  swallowed** and the stack reported `CREATE_COMPLETE` for a resource that does not
  exist. That asymmetry is also why this survived #483's review.

  One helper now derives the failure from either convention, applied centrally in
  `dispatch` so all 31 recording sites became correct without being individually
  edited. The plugin's own error code is lifted from the response body, so the
  recorded reason is `InvalidBucketName: The specified bucket is not valid.` rather
  than a bare status number. `deployS3Bucket` also stopped discarding its response
  and now returns early instead of configuring versioning on a bucket that was
  refused. The follow-up could not have corrupted the recorded reason — its own
  result was discarded too — but it did put a `PUT ?versioning` refused with
  `NoSuchBucket` into the event log, a request no real client would have sent, in the
  log substrate replays.

  **Compatibility: a resource that genuinely fails now reports `CREATE_FAILED` where
  it reported `CREATE_COMPLETE`.** A test asserting the old status was asserting a
  defect, but it will still turn red. The stack status is unchanged, and substrate
  still does not roll back (#520).

- **A stack now deploys into the calling account and region** (#517). The deployer
  synthesized its own request context from substrate's package defaults —
  `123456789012` and `us-east-1` — regardless of who created the stack. Most plugins
  embed the account, and some the region, in their state keys
  (`instance:<account>/<region>/<id>`, `table:<account>/<name>`,
  `queue:<account>/<name>`, `topic:<account>/<region>/<name>`), so a caller in any
  other partition got a stack ARN naming their own account whose resources were
  written where they could not read them. An unsigned client — which resolves to
  `000000000000` — created a stack reporting `CREATE_COMPLETE` with a
  `PhysicalResourceId` of `i-…`, and `DescribeInstances` then correctly returned
  nothing. **Every read scoped to the caller was right; the write was in the wrong
  place.**

  The reporting consumer read that as `AWS::EC2::*` being stubbed while
  `AWS::IAM::*` worked, which is worth stating plainly because the asymmetry is real
  but the diagnosis was not: S3 (`bucket:<name>`) and IAM (`role:<name>`) key their
  state *unpartitioned*, so a resource written under the wrong identity is still
  found by a read under the right one. Both families were fully implemented. That is
  also why this survived #483's review — its shared-state test asserted a bucket
  through S3 and a role through IAM, the only two services that could not see the
  bug.

  Identity now rides on the deployer as an explicit account and region, defaulting to
  today's constants, and the CloudFormation plugin builds a deployer carrying the
  requesting caller's identity for every path that deploys or reads a resource. A
  deployer is six pointer copies with no I/O, so one per deploying request is cheaper
  than making a single deployer's identity mutable and therefore racy. Two strings
  rather than a whole request context, deliberately: a deployer holding one would
  carry a single request's ID and timestamp across every resource it deploys, and
  would invite a future reader to propagate the principal, which is a separate
  decision (#411). Because the default is unchanged, no existing caller behaves
  differently.

  Six sites read that identity, and threading only the obvious one would have left a
  half-fix. `dispatch` builds each resource's request context, so it is the write
  path. Both `buildCFNContext` calls resolve `AWS::AccountId` and `AWS::Region`, so
  before this a template whose bucket name was `{"Fn::Sub": "b-${AWS::AccountId}"}`
  named substrate's account rather than the caller's — a defect neither the issue nor
  the reporter named, and one that produced a *wrong physical name* rather than a
  misplaced resource. And three drift comparators read their own partitioned state
  keys directly (DynamoDB, SQS, SNS), which drift detection needs on top of the
  existence checkers: a wrong-partition existence check reports `DELETED` for a
  resource that is fine, and a wrong-partition comparator finds nothing to compare
  and reports no difference at all — drift silently blind, which no `IN_SYNC`
  assertion can distinguish from drift working.

  `BettyClient` keeps the defaults, and that is a decision rather than an oversight:
  it is the in-process validation client, its callers never sign a request, so there
  is no caller whose identity could be threaded. Its deployer is now built through
  the constructor rather than as a struct literal, so there is one construction path
  and the default cannot drift away from it — the literal left both fields empty,
  which was harmless only for as long as nothing read them. An in-process caller that
  does need another partition can set the identity on the deployer directly.

  The comment #483 left on the stack-ARN builder described this as a discrepancy
  confined to the two pseudo-parameters. That understated it considerably and has
  been rewritten.

- **A parameter declared `Default: ''` is now a declared parameter** (#516). The
  empty string was treated as "no default at all", so such a parameter was left
  undeclared, `Ref` fell through to echoing the parameter's own name back, and the
  conventional optional-parameter test `!Not [!Equals [!Ref X, '']]` **inverted**: a
  parameter the caller never set read as set, and every template taking the wrong
  branch did so silently. `Default: ''` is how an optional parameter is spelled —
  21 occurrences across the five templates of the consumer that reported #516 — so
  the distinction is load-bearing rather than pedantic. Found by deploying one of
  those templates as a checked-in fixture rather than by reading the code.

- **A condition referencing another condition no longer depends on map order**
  (#516). `Conditions` were evaluated by iterating a Go map, so a condition
  referencing another via `{"Condition": "Other"}` read the referent's zero value
  whenever it happened to be evaluated first. The same template deployed differently
  from one run to the next — nondeterminism, which is the one outcome an emulator
  built on deterministic replay must never produce. Conditions are now resolved on
  demand, so a referent is evaluated when it is needed regardless of declaration
  order, and a reference cycle resolves to `false` (real CloudFormation rejects one
  at validation time) rather than recursing forever. Names are additionally walked in
  sorted order, which matters only for a cycle: one running through `Fn::Not`
  resolves its two members to *opposite* values, so absent the sort the answer would
  still depend on which member the map happened to yield first.

  Each condition's first answer is also cached for the rest of the deployment. AWS
  evaluates conditions once, when the stack is created or updated, and forbids a
  condition from referencing a resource's logical ID or attributes; substrate has no
  validation pass to reject such a reference, so caching is what keeps a template
  that makes one anyway from seeing one value in a property resolved early and
  another in an output resolved last.

### Changed
- **`docs/services.md` no longer claims `DeleteStack` deletes the stack's
  resources** (#518). It deletes the stack record and its name index only — a bucket
  a stack created outlives the stack, and `head-bucket` still answers 200. The
  correction is documented now, independent of the sweep itself, which is bigger than
  it looks: 113 resource types need a per-type delete dispatcher, `DeletionPolicy`
  is not modelled at all, deletion must run in reverse dependency order, and a bucket
  sweep inherits #508's leak. Tracked in #518.

  The CloudFormation section also now states that a refused resource reports
  `CREATE_FAILED` while the stack still reaches a terminal status without rolling
  back (#520), that `Fn::Split` yields only its first element (#521), and what the
  YAML short forms do and do not resolve.

## [v0.87.0] - 2026-08-02

### Fixed
- **`DescribeInstanceTypes` now refuses an instance type it does not model** (#485).
  An unknown type was answered with HTTP 200 and an empty list, so a consumer
  validating a user-supplied instance type got no signal at all and their validation
  branch was unreachable — the same shape as #391's unknown instance ID. Real
  `us-east-1` answers `InvalidInstanceType`, HTTP 400, and substrate now does too,
  collecting every unknown type in the request into one bracketed list in request
  order. One bad type fails the whole request; the known types are not returned,
  because `InstanceType.N` asserts that the types supplied exist.

  The code is documented in EC2's client-error table. The message is verbatim from
  the single real-`us-east-1` capture reported in #485, so the brackets and the
  plural phrasing are observed; the `", "` separator for a multi-type list is
  **not** corroborated and is substrate's choice. Dispatch on the code.

- **The `DescribeInstanceTypeOfferings` instance-type filter now works** (#485). It
  had never worked: the handler built its filter from an `InstanceType.N` parameter
  the operation does not have — its reference lists exactly `DryRun`, `Filter.N`,
  `LocationType`, `MaxResults` and `NextToken`, and botocore rejects `InstanceTypes`
  outright — so the filter map was always empty and every query returned the whole
  catalog in every zone. A caller asking "is `m5.xlarge` offered here?" got yes for
  any input, including nonsense. The pattern had been copied from
  `DescribeInstanceTypes`, where the parameter does exist.

  Both documented filter names are now applied — `instance-type` and `location` —
  with EC2's documented wildcards (`*` for zero or more characters, `?` for zero or
  **one**, `\` to escape a literal), case-sensitive values, all `Filter.N.Value.M`
  values as an OR, and separate `Filter.N` entries ANDed. Any other filter name is
  **refused** with `InvalidParameterValue` rather than ignored, since silently
  dropping a filter is how this defect went unnoticed for four releases.
  `location-type` is among the refused: it is not a filter name, contrary to #485's
  aside — `LocationType` is a separate top-level parameter, now honoured for
  `availability-zone` (the default) and `region`. `availability-zone-id` and
  `outpost` are real AWS values substrate does not model and are refused with a
  message naming substrate, rather than answered with zone names under a
  `locationType` claiming they are IDs or Outpost ARNs.

  An `instance-type` filter matching nothing is **zero offerings and HTTP 200**, not
  an error — deliberately the opposite of `DescribeInstanceTypes`' identically
  spelled parameter, and confirmed by #485's real-AWS diff of both. The two are
  different questions: a filter narrows a result set, so an unmatched value is a
  legitimate empty answer, while `InstanceType.N` asserts existence. Both halves are
  pinned by tests so neither can later be "tidied" into consistency with the other.
  `DescribeSpotPriceHistory`'s `InstanceType.N` is a filter too, per its reference,
  so an unknown type there is an empty history.

- **`TerminateInstances` now honours termination protection** (#489). #473 made
  `disableApiTermination` observable but nothing acted on it, which is the sharpest
  case of this tracker's recurring shape: a consumer's test asserting "my teardown
  does not destroy the protected instance" passed against a code path that ignored
  protection entirely, so the assertion *could not fail*. A protected instance is now
  refused with `OperationNotPermitted`, HTTP 400, and stays `running`. Clearing
  protection with `ModifyInstanceAttribute` and terminating again succeeds, which is
  the sequence a teardown actually performs.

  **A request naming both protected and unprotected instances fails per Availability
  Zone, not per request and not per instance.** This is the behaviour a reader will
  guess wrong in one of two directions, so it is quoted: "The specified instances that
  are in the same Availability Zone as the protected instance are not terminated. The
  specified instances that are in different Availability Zones, where no other
  specified instances are protected, are successfully terminated." So an *unprotected*
  instance sharing a zone with a protected one survives, while an unprotected instance
  in another zone is terminated — and the request reports the error **after** those
  terminations are persisted. Refusing the whole request and terminating every
  unprotected instance are both wrong, in opposite directions.

  `DeleteFleets --terminate-instances` routes through the same handler, so a protected
  fleet instance survives its fleet's deletion while an unprotected sibling in another
  zone does not, and `DeleteFleets` propagates `OperationNotPermitted` rather than
  folding it into `unsuccessfulFleetDeletionSet` — `DeleteFleetError`'s documented
  codes are exactly `fleetIdDoesNotExist`, `fleetIdMalformed`,
  `fleetNotInDeletableState` and `unexpectedError`, so folding it in would mean
  answering `unexpectedError` and losing the code a caller acts on.

  Provenance is split. The **code** is documented: EC2's client-error table lists
  `OperationNotPermitted` and names this case first among its examples, "you might be
  trying to terminate an instance that has termination protection enabled". The
  **message text is substrate's own** — #489 supplied a remembered console wording
  that no capture corroborates, so per `docs/fidelity.md` it is not dressed up as
  observed. Dispatch on the code.

  Unchanged and still divergent: a terminated instance reports code `48` `terminated`
  immediately, where real EC2 reports code `32` `shutting-down` first. That is
  pre-existing, unrelated to protection, and tracked separately.

- **S3 now stores the ACL a write names, and `BlockPublicAcls` refuses the writes it
  should** (#470). `PutObject` and `CreateBucket` read `x-amz-acl` not at all, so the
  ACL `GetObjectAcl` and `GetBucketAcl` reported was never the one the write set — a
  consumer asserting "my upload is publicly readable" saw the default owner-only ACL
  with no signal the header had been discarded. An ACL expressed through the five
  `x-amz-grant-*` headers was stored by **no** operation, `PutBucketAcl` and
  `PutObjectAcl` included: the headers were parsed only to decide whether #458's
  public-access check should refuse, so a non-public grant was silently dropped and a
  public one was refused without ever being storable. Six operations now resolve and
  store an ACL — `CreateBucket`, `PutObject`, `CopyObject`, `CreateMultipartUpload`,
  `PutBucketAcl`, `PutObjectAcl` — and the canned-ACL table is modelled per resource
  kind, so `log-delivery-write` grants the `LogDelivery` group on a bucket and nothing
  on an object, per its "Applies to: Bucket".

  **`authenticated-read` resolved to owner-only, which let a blocked bucket be walked
  straight through.** `AuthenticatedUsers` is every AWS account, not every account in
  yours, so an ACL granting it is public by Block Public Access's own definition — and
  a canned ACL that resolved to no group grant was invisible to the check. It now
  resolves to an `AuthenticatedUsers` `READ` grant and is refused where the two
  `public-*` names are.

  With an ACL to examine, `BlockPublicAcls` now enforces its other documented bullet:
  "PUT Object calls fail if the request includes a public ACL". `PutObject`,
  `CopyObject` and `CreateMultipartUpload` are refused with `403 AccessDenied`
  **before anything is written**, so a refused upload stores no object, a refused copy
  stores no destination object, a refused multipart create leaves no upload ID behind,
  and a refused overwrite leaves the object already at the key untouched — body and
  ACL both. A copy is judged against the **destination** bucket's configuration, since
  reading the source's would let a public ACL be laundered into a blocked bucket by
  copying rather than uploading.

  A write replaces the whole ACL rather than part of it, per "You cannot use
  `PutObject` to only update a single piece of metadata for an existing object. You
  must put the entire object with updated metadata". So an overwrite naming no ACL
  clears one the key already had, whether it arrived through the original write or a
  later `PutObjectAcl`; `CompleteMultipartUpload` and `CopyObject` replace it the same
  way. A copy never inherits: "When you copy an object, the ACL metadata is not
  preserved and is set to `private` by default." A multipart upload's ACL is fixed at
  create and carried on the upload record, because Complete's request accepts no ACL
  header — the same shape #492 established for encryption.

  Fixed along the way: `bucket_acl:<bucket>` outlived `DeleteBucket`, so creating a
  bucket with a public ACL, deleting it and creating it again reported the deleted
  bucket's ACL on the new one. `CreateBucket` is now authoritative and clears it. The
  six *other* bucket sub-resources still leak this way — three of them changing
  behaviour rather than just a `GET` — and that is tracked separately.

  **`CreateBucket` with a public ACL is still accepted, and that gap is now stated
  rather than left ambiguous.** The setting's third bullet refuses such a call, but the
  configuration that does so is the **account-level** one — a bucket-level
  configuration cannot exist before the bucket does — and substrate models no
  account-level Block Public Access, so gating this one operation would mean modeling a
  control the emulator does not otherwise have. A test pins the acceptance so adding
  account-level support later changes a failing test rather than passing silently.

  An `emailAddress` grantee is skipped rather than refused with the `HTTP 405` S3 has
  returned since October 1 2025: the 405 is Region-conditional and applies to the XML
  body form too, so it is filed rather than guessed at. A canned name substrate does
  not recognize resolves to owner-only rather than being refused, because the
  per-operation Valid Values lists differ (four names on `CreateBucket`, seven on
  `PutObject`) and no error code is documented for a name outside them.

- **An injected S3 fault is no longer distinguishable from a real S3 error** (#480).
  This is the one property a fault injector must not have, and it had it. Faults are
  evaluated before any plugin runs, so an injected error was serialized by the
  pipeline's generic Query arm as `<ErrorResponse><Error><Type>Sender</Type>…`, while
  a genuine S3 error is a bare `<Error>` document with an XML declaration and a
  `<RequestId>`. S3's parser recovers no code from the wrapped form and falls back to
  the HTTP status, so a fault armed as `SlowDown` arrived at the client as
  `ServiceUnavailable`: a consumer whose retry policy matches on `SlowDown` never saw
  their own fault fire, and a caller able to tell an injected error from a real one can
  tell a fixture from production. The bytes now come from the same function the S3
  plugin uses, so the two documents are byte-identical — asserted both at the
  marshaller and over the wire against a genuine `NoSuchKey`, since only comparing two
  independently produced documents proves it.

  `cloudfront` and `route53` are REST-XML like S3 and keep the `<ErrorResponse>` shape
  deliberately: their real error documents genuinely are wrapped, so they were already
  correct and are pinned by tests against being swept along. EC2's Query XML and SQS's
  JSON RPC are likewise unchanged.

- **A fault rule can now name an S3 operation, and be bounded to N occurrences**
  (#480). Three defects that compounded into fixtures that were silently wrong rather
  than merely limited:

  **An S3 rule could not name a semantic operation.** Faults are evaluated one pipeline
  step before the S3 plugin resolves a REST request to its operation name, so
  `req.Operation` was still the bare HTTP verb: `operation: PutObject` matched
  **nothing at all**, and `operation: PUT` took out `UploadPart` and every other
  object-path `PUT` alongside it. A fixture arming a `PutObject` fault therefore either
  did nothing or failed a part upload somewhere the test was not looking. The parser
  now resolves an S3 request's operation up front, so the name is canonical before
  faults, quotas, cost attribution and consistency tracking all see it — the S3 cost
  entries keyed on `s3/PutObject` and `s3/GetObject` were unreachable for any request
  that errored before reaching the plugin, and the event log recorded `PUT` where it
  now records the operation.

  **A rule could not be bounded, so recovery was unobservable.** `Times` bounds how
  many matching requests a rule fires on. Fail twice, then succeed, is the outcome that
  distinguishes working retry from no retry, and an unbounded rule can only ever
  produce failure — so the one thing a retry test needs to observe could not be
  produced. Zero means **one**, not unlimited: reading a missing field as unlimited
  turns a typo into a fixture that consumes a consumer's whole retry budget. A negative
  value is unlimited and has to be asked for. The match, the probability roll and the
  increment happen under one lock, so `Times: 1` with N concurrent requests yields
  exactly one failure — atomicity a client-side counter cannot arrange, and the reason
  the controller's locking was restructured rather than a counter bolted on.

  **A rule that matched nothing looked exactly like a consumer's retry working.** Each
  rule now reports a `fired` count through `GET /v1/fault/rules`, with `FaultsFired()`
  summing them in process. A fixture that arms a fault and observes success has proven
  nothing without asserting the count — and the first of these three defects would have
  been caught immediately by any test that did. Arming rules again replaces the
  configuration and resets the counts, so a fixture re-arming between phases gets its
  full budget rather than a spent one.

  Three wire matchers are added for distinctions an operation name does not carry:
  `PathSuffix`, `QueryKey` and `HeaderPrefix`, AND-ed with `Service` and `Operation`.
  `QueryKey` is what separates the multipart sub-operations, which share a path and
  differ by a sub-resource parameter — so #480's own previously unwritable test, "fail
  `CompleteMultipartUpload` after every part has uploaded and assert no orphaned
  upload", is now writable and is written. Presence is what those parameters signal, so
  the value is not compared.

  Fixed along the way: `NewFaultController` and `UpdateConfig` stored the caller's rule
  slice, so incrementing a fired count reached into the caller's own `FaultConfig` —
  arming the same value twice found the second arming already spent.

### Added
- **EC2 instances now carry and report an Availability Zone** (#489), which is what
  makes the zone-scoped rule above observable rather than merely internally
  consistent. It resolves at launch from `Placement.AvailabilityZone`, else the named
  subnet's zone, else the region's first zone (`<region>a`) — matching the reference's
  "EC2 automatically selects an Availability Zone for you" — and a fleet pool's
  `Overrides.N.AvailabilityZone` now reaches the launch, so a fleet spread across
  zones no longer looks single-zone. `RunInstances` and `DescribeInstances` report it
  as `<placement><availabilityZone>`, and `DescribeInstances` accepts an
  `availability-zone` filter (that is the reference's spelling; the placement family's
  filter names are listed individually, so there is no `placement.availability-zone`).
  `AvailabilityZoneId` is not modelled. An instance replayed from an event log
  predating the field reads back with an empty zone, grouping all such instances
  together — the conservative reading, being what a single-zone account looks like.


- **The instance-type catalog now covers whole families** (#485), widened from 8
  types to 57. The old catalog split families mid-way — `c5.xlarge` in and `c5.large`
  out, `m5.large` in and `m5.xlarge` out — which was incidental to #234's consumer and
  became a correctness problem the moment an absent type started being *refused*: a
  catalog stopping at `c5.xlarge` answers `InvalidInstanceType` for `c5.large`, the
  right code for a bogus type and the wrong one for a real one. `t3`, `t3a`, `m5`,
  `m5a`, `c5`, `c5a` and `r5` are now complete (note `c5`'s ladder differs from
  `c5a`'s: `9xlarge`/`18xlarge` against `8xlarge`/`16xlarge`), alongside the three
  accelerated sizes #234 seeded. All ten types #485 names are present; five were not.

  Bare-metal sizes are deliberately excluded and the exclusion is pinned by a test:
  they are real types, but nothing else in the plugin models their behaviour.

  Spot prices are now generated from the same per-family table as the specs, so a
  type cannot be in the catalog and missing from the price index — the previous pair
  of parallel maps had exactly that hazard, and `DescribeSpotPriceHistory` silently
  dropped any type absent from the price map. The eight prices #234 shipped are
  preserved verbatim so no recorded fixture moves. They remain deterministic stubs,
  not AWS prices.

  `DescribeAvailabilityZones`, `DescribeInstanceTypeOfferings` and
  `DescribeSpotPriceHistory` now derive their zone names from one list, so filtering
  an offerings query by a zone you just enumerated cannot return empty.

- **Five service-role AWS managed policies are now bundled** (#484), taking the
  catalog from 47 policies to 52: `AmazonSSMManagedInstanceCore`,
  `AmazonEC2ContainerRegistryReadOnly`,
  `service-role/AmazonECSTaskExecutionRolePolicy`,
  `service-role/AWSLambdaBasicExecutionRole` and
  `service-role/AWSLambdaVPCAccessExecutionRole`. `GetPolicy` on any of them returned
  `NoSuchEntity` before, so the sequence every consumer provisioning an SSM-managed
  instance profile performs — `CreateRole`, `AttachRolePolicy`, `GetPolicy` — failed
  at the last step on an ARN the attach had just accepted.

  The gap was a population gap rather than an oversight about any one policy. All 47
  bundled policies were human-operator policies: `AdministratorAccess`,
  `PowerUserAccess`, and per-service `FullAccess`/`ReadOnlyAccess` pairs, which are
  attached to users and groups. None was a service-role policy — what an *instance
  profile* or *execution role* carries, and therefore what IaC actually provisions.
  `AmazonSSMFullAccess` was present; `AmazonSSMManagedInstanceCore`, the policy an
  SSM-managed instance needs, was not.

  Each document is verbatim from the policy's page in the AWS managed policy
  reference, and each policy ID is real, from a recorded `get-policy` snapshot rather
  than synthesised — so a consumer asserting on an `ANPA…` ID sees the value AWS
  reports. Default versions likewise follow AWS (`v2` for
  `AmazonSSMManagedInstanceCore`, `v3` for `AmazonEC2ContainerRegistryReadOnly` and
  `AWSLambdaVPCAccessExecutionRole`) instead of a blanket `v1`. The documents are read
  by the IAM policy evaluator, so they are behaviour and not decoration.

  The catalog can now express a **path**, which three of the five need. A policy under
  a path reports it in `Path` (`/service-role/`) and keeps it out of `PolicyName`,
  matching AWS — the distinction `ListPolicies --path-prefix` reads, and getting it
  wrong would make a policy findable by ARN and invisible to a path query. Bundled
  policies are constructed through one function so the ARN and the path cannot
  disagree.

  `AttachRolePolicy` still accepts a policy ARN that no `GetPolicy` can resolve, which
  is the general asymmetry #484 also offered to fix. Not taken, deliberately:
  substrate bundles 52 of roughly 1,200 AWS managed policies, so refusing an
  unresolvable ARN would fail every attach of the other ~1,150 — breaking working
  consumer code, where the current behaviour merely fails to catch a typo. Seeding the
  catalog fixes the reported case; the asymmetry is tracked in #499 so the decision is
  recorded rather than implied. Two further gaps found while verifying this one are
  filed: `ListPolicies` applies neither `Scope` nor `PathPrefix` and never lists
  bundled policies (#497), and `GetPolicyVersion` is unimplemented, so no policy
  document is observable over the wire at all (#498).

- **CloudFormation is now reachable over the wire** (#483). Every CloudFormation
  call returned `ServiceNotAvailable` at HTTP 501 — no plugin served the service —
  even though the stack model behind it was complete and had been for many releases.
  It was simply reachable only from Go, through `emulator.BettyClient`. A consumer
  driving substrate the way CDK, CloudFormation and Terraform users actually do,
  with the AWS CLI or an SDK, could not create a stack at all, while `README.md` and
  the docs advertised CloudFormation as a first-class target. The advertisement is
  now true at both layers.

  Fifteen operations: `CreateStack`, `UpdateStack`, `DeleteStack`,
  `DescribeStacks`, `ListStacks`, `DescribeStackResources`, `GetTemplate`, the five
  change-set operations (`CreateChangeSet`, `DescribeChangeSet`, `ExecuteChangeSet`,
  `ListChangeSets`, `DeleteChangeSet`) and the three drift operations
  (`DetectStackDrift`, `DescribeStackResourceDrifts`,
  `DescribeStackDriftDetectionStatus`). 113 resource types and the `Ref`/`Fn::*`
  intrinsics the deployer already resolved are available unchanged, which is why
  this is a wire adapter of about 1,000 lines rather than a new service.

  **A stack's resources are real resources in the other plugins**, because the
  adapter deploys through the same plugin registry the server routes with. A
  template declaring an `AWS::S3::Bucket` produces a bucket `s3api head-bucket`
  finds and `s3api put-object` writes to; a stack created over the wire is visible
  to the in-process API and vice versa. There is one set of stacks and one set of
  resources, not a CloudFormation-shaped world beside the emulator.

  `DescribeStackEvents` is deliberately **not** supported and returns
  `UnsupportedOperation` — substrate's own signal, not a CloudFormation code, since
  real CloudFormation has no error for an operation it implements. A stack carries
  one status string and there is no per-resource event model behind it, so
  answering the call would mean fabricating a `CREATE_IN_PROGRESS →
  CREATE_COMPLETE` pair per resource with invented timestamps. A consumer polling
  for completion should poll `DescribeStacks`. What a real event model needs is
  filed as #501. `ChangeSetType=CREATE` is refused for the same class of reason: it
  requires a stack in `REVIEW_IN_PROGRESS`, a state the model cannot represent.

  `TemplateURL` is refused with `ValidationError` rather than ignored — fetching a
  template is a network read substrate does not perform, and accepting the
  parameter silently deployed an empty stack. Template transforms are not applied,
  so a SAM template reaches the deployer unexpanded and `GetTemplate` reports
  `StagesAvailable: [Original]`.

  Stack and change-set ARNs derive their UUID from the account, region and name
  rather than a clock or a PRNG, so a recorded response replays byte-identically.
  `CreateStack` returns with the stack already `CREATE_COMPLETE` — there is no
  `*_IN_PROGRESS` window, so `wait stack-create-complete` returns at once instead of
  polling.

  CloudFormation was also missing from the error-protocol table, which would have
  left its error-document shape to Content-Type sniffing; it is now explicitly
  Query/XML. `StackDeployer` reports its failures as `fmt.Errorf` strings, so the
  adapter maps them to AWS codes by matching sentinel prefixes — typed errors
  behind the model would remove the string matching and are filed as #502.

- **EC2 tag keys and values now have their documented length limits enforced**
  (#490). A key longer than 128 characters or a value longer than 256 was accepted
  everywhere: `CreateTags`, `DeleteTags`, and tag-on-create through `RunInstances`,
  `CreateFleet`, `CreateImage`, `CreateNatGateway` and a launch template. This was the
  third restriction in the same table #469 read for the 50-tag count and the only one
  left unenforced, so a consumer generating tag values from a resource description or a
  git ref — the case that actually overruns 256 characters — had no way to reach real
  EC2's rejection, and a test asserting it passed against substrate while the same code
  failed against AWS. The rejection is `InvalidParameterValue`, HTTP 400, and names
  which limit was exceeded and by how much.

  **The unit is Unicode characters, not bytes**, per "Maximum key length – 128 Unicode
  characters in UTF-8". A key of 128 emoji is 128 characters and 512 bytes and is
  legal; counting bytes would refuse it while reporting a length the caller never sent.
  The two counts agree on ASCII, which is why an ASCII-only suite cannot tell them
  apart, so the tests carry emoji and three-byte rows.

  There is no lower bound — "You can set the value of a tag to an empty string, but you
  can't set the value of a tag to null" — so the check is an upper bound only. That is
  also what makes `DeleteTags` correct without a special case: it names keys and treats
  the value as optional, so an absent `Tag.N.Value` is the empty string and passes.

  Reserved `aws:` keys are **not** exempt from the lengths, though they are from the
  count: the exemption in the restrictions list is scoped to the count alone ("Tags
  with the `aws:` prefix do not count against your tags per resource limit") and
  nothing in it exempts a reserved key from either length. That answers the question
  #490 raised, and it is worth being precise about what it buys — the reserved-key
  check runs first, so an `aws:`-prefixed key is refused for being reserved before its
  length is measured, and the choice therefore changes no wire behaviour today. It is
  the right code for any future path that checks lengths alone, and it is recorded
  because the adjacent count check does the opposite. Where the length check does bite
  is the case-sensitive edge: `AWS:` is an ordinary user tag, and an over-long one is
  refused for its length.

  As with the other two tag restrictions, the whole request is refused before anything
  is modified, and on a tag-on-create path before the resource exists — a refused
  `RunInstances` launches no instance, a refused `CreateImage` creates neither the AMI
  nor its snapshot, and a refused `CreateLaunchTemplate` creates no template. A launch
  template is checked at creation and at each version as well as at every launch that
  names it, following #471, so an over-long template tag is reported once at the
  operation that named it.

  The three tag checks are now applied through one function rather than at each of the
  nine call sites that had to be found by hand for this fix, so the next restriction is
  added in one place.

  Provenance is the weakest of the three tag restrictions and is marked as such. The
  **code** `InvalidParameterValue` with HTTP 400 is by analogy with the reserved-key
  rejection — the other tag-restriction violation on the same operations, and
  `CreateTags`' Errors section is empty so the API model supplies nothing. The
  **message text is substrate's own**: no captured real-AWS length rejection was found,
  in moto or LocalStack either. It interpolates the limit and the actual length,
  following the `SendMessage` size-limit message's precedent, because the message is
  the only place a caller learns which limit they hit. Dispatch on the code.

- **S3 records the server-side-encryption headers and echoes them on every read**
  (#492). `x-amz-server-side-encryption`,
  `x-amz-server-side-encryption-aws-kms-key-id` and
  `x-amz-server-side-encryption-bucket-key-enabled` are recorded on `PutObject` and
  `CreateMultipartUpload`, and returned on those responses plus `GetObject`,
  `HeadObject` and `CompleteMultipartUpload`. Substrate previously accepted them and
  discarded them, so an object written with encryption read back byte-identical to one
  written without it: a consumer could only assert on what their own request carried,
  which verifies the line that filled in the request and nothing about the stored
  object. Every such request assertion is now a stored-object assertion.

  **No cryptography is performed** — the body is stored exactly as it arrived.
  Encryption at rest is not observable through an API call, but the encryption S3
  *reports* for an object is, and that report is the assertion a consumer is making.

  An absent header stays **absent on the response, not `false` or empty**, so "no
  encryption named" remains distinguishable from "encryption named" — the property
  that makes recording worth anything. That is also why bucket default encryption is
  deliberately not modelled here: real S3 has applied SSE-S3 unconditionally since
  January 2023, so adding the default would erase the distinction, and it belongs with
  the rest of the resolution rules in #493.

  **The KMS key ID round-trips verbatim, as a stated divergence.** Real S3 resolves any
  of the four accepted forms — a bare UUID, `alias/name`, a key ARN, an alias ARN — to
  the key ARN before reporting it. Substrate returns the string that was sent, because
  that is the string the consumer's configuration produced and therefore the assertion
  they are trying to make; #475's reporter confirms their tests assert the sent form.
  The difference is observable and documented, so modelling key resolution later has to
  revisit it rather than silently overtake it.

  Encryption on a multipart upload is fixed at `CreateMultipartUpload` and carried onto
  the assembled object, because Complete's request accepts only the SSE-C headers.
  The family is one embedded declaration shared by the object and upload records, so
  the two write paths cannot drift the way #406's did.

  Nothing is validated, and `CopyObject` records no encryption at all rather than
  inheriting the source's — both deliberate, both pinned by tests so #493 changes them
  on purpose. #493 carries the four `InvalidArgument` rejections, bucket defaults, and
  the copy resolution order. SSE-C is out of scope entirely.

### Changed
- **A fault rule naming a bare HTTP method no longer matches an S3 request** (#480).
  This is the one compatibility consequence of resolving an S3 request's semantic
  operation before faults are evaluated. A rule written as `{"service": "s3",
  "operation": "PUT"}` matched every object-path `PUT` and now matches nothing; write
  the operation it meant — `PutObject`, `UploadPart`, `CopyObject` — or leave
  `operation` empty to match every S3 operation. A bare-method rule still fires for a
  service whose operation genuinely is its HTTP method, such as `execute-api`. Rules
  naming a semantic S3 operation were matching nothing before this release, so no rule
  that worked stops working; a rule that appeared to work by naming a verb was firing
  on requests it did not mean.

- **`FaultRule.Probability`'s shared PRNG is now documented rather than implied**
  (#480). One PRNG serves every rule in a `FaultConfig` and is rolled once per
  *matching* rule per request, so a fixed seed reproduces a run only while the whole
  sequence of requests is unchanged — adding a request upstream, or a retry, shifts
  every later roll, including for rules that request never touched. No behaviour
  changed; the coupling was real and undocumented, and `Times` is the bounded outcome
  to prefer since it needs no roll at all. Per-rule PRNGs would fix it and would move
  the outcome of every existing seeded run, so it is filed as #510 rather than changed
  inside a fix for something else.

- **`ReceiveMessage` now warns when a stored message's attributes violate a current
  rule, and still returns the message** (#491). v0.86.0 added SQS's message-attribute
  count, name, type and `Number` value rules on send. A message written into state
  before they existed — replayed from an event log recorded against an older substrate —
  can still carry an illegal name, an eleventh attribute or a `Number` holding `"abc"`.
  It is returned as stored, in full, and a `WARN` line names the queue, the message ID
  and the violated rule.

  **Returning it is the decision, not an omission.** Substrate's core property is that
  replaying an event log reproduces the same observations, and the message was accepted
  by the substrate that recorded it — so withholding or dropping it would make a recorded
  run unreplayable, which is the property everything else rests on. A receive-time
  rejection also has no AWS behaviour to imitate: real SQS never accepted the message, so
  there is nothing to copy. The suite now fails if anyone adds one, which is what makes
  this a recorded decision rather than an unfixed gap.

  The warning covers the whole stored attribute set rather than the subset the request
  named, because the violation is a property of state and a request naming no attributes
  gets none back — checking only the selection would hide it from exactly the caller most
  likely to be replaying an old log. It fires on every receive rather than once, since
  remembering which messages had already warned would be per-process state a replay could
  not reproduce. Send-time rejection is unchanged, so no new run can produce this state.
  A replay-time report is the more principled home for the signal and is filed as #512;
  it needs a surface in the replay machinery that does not exist, and the warning delivers
  the operator-facing value now.

## [v0.86.0] - 2026-08-02

### Added
- **Reserved `aws:` tag keys are now rejected on every tag-on-create path** (#468).
  #452 closed this for `CreateTags` and `DeleteTags` but deliberately left it off
  tag-on-create, so a key real EC2 refuses was still accepted whenever it arrived
  through a `TagSpecification` — on `RunInstances`, `CreateFleet`, `CreateImage` or
  `CreateNatGateway`. A consumer's error branch for the rejection stayed unreachable
  on exactly the path where the rejection is best evidenced: both real-AWS captures
  behind the message are of `RunInstances` tag-on-create.

  The rejection happens **before the resource is created**. A refused `RunInstances`
  launches no instance, a refused `CreateImage` leaves behind neither the AMI nor its
  backing snapshot, and a refused `CreateNatGateway` creates no gateway — per the
  tagging documentation, "If tags cannot be applied during resource creation, we roll
  back the resource creation process. This ensures that resources are either created
  with tags or not created at all."

  Substrate stamps `aws:ec2:fleet-id` on every fleet instance (#443), which is itself
  a reserved key on a checked path — the reason the check was previously left off it.
  That tag is now exempt **structurally rather than by a flag**: `CreateFleet` parses
  and checks the caller's tags exactly as `RunInstances` does, and the fleet-ID tag is
  appended afterwards, on an internal launch entry point taking already-parsed tags
  that no request can address. A validation-skipping flag would have made the
  outcome depend on internal state a consumer cannot observe, which is the opposite of
  the deterministic-replay property. So a caller naming `aws:` anything in a
  `CreateFleet` request is now rejected — instance- and fleet-scoped alike — while the
  fleet's own stamp still lands, alongside the caller's legal tags.

  The match remains **case-sensitive**, so `AWS:foo` is still an ordinary user tag.
  Note that #472's SQS attribute prefix rule is case-*insensitive*; the two services
  document different rules and the checks are deliberately not shared.
- **The 50-tag-per-resource limit is now enforced** (#469).
  Substrate accepted any number of tags, so a consumer that hit real EC2's ceiling had
  no way to reach the `TagLimitExceeded` branch — and a test asserting the rejection
  passed against substrate while the same code failed against AWS. `CreateTags` and
  every tag-on-create path now refuse a resource that would carry more than 50 user
  tags with `TagLimitExceeded`, HTTP 400, per the tagging documentation's "Maximum
  number of tags per resource – 50".

  Two rules make this less like arithmetic than it looks, and both are modelled.
  **Tags with the `aws:` prefix do not count** ("Tags with the `aws:` prefix do not
  count against your tags per resource limit"), which matters beyond pedantry: since
  substrate stamps `aws:ec2:fleet-id` on every fleet instance (#443), a counter that
  included reserved keys would refuse a fleet launch whose template names the full 50
  user tags — a launch real EC2 accepts. A fleet instance therefore holds 51 tags
  legally. And **overwriting an existing key at the limit succeeds**, because the count
  is over the post-merge key *set* rather than the sum of both sides: changing `key7`'s
  value on a 50-tag instance is accepted while adding a new `key51` is refused.
  [getmoto/moto#8151](https://github.com/getmoto/moto/issues/8151) reports real AWS
  permitting the first. This also closes the companion gap v0.85.0 recorded as vacuous:
  the reserved-tag exemption now has a limit to be exempt from.

  As with reserved keys, the whole request is refused before anything is modified — a
  `CreateTags` naming one instance with room and one at the limit tags neither.

  Provenance is split, and the weaker half is named. The **code** is a doc citation:
  EC2's client-error table lists `TagLimitExceeded` as "You've reached the limit on the
  number of tags that you can assign to the specified resource." The wire **message**
  is published nowhere, so the wording comes from moto — a reimplementation, not a
  capture. Stronger than #452's provenance on the code, weaker on the message; SDKs
  dispatch on `Error.Code`, so the documented half is the one a consumer's error branch
  turns on. The documented tag key/value *length* limits (128 and 256 Unicode
  characters) remain unenforced and are tracked separately.
- **EC2 launch templates are now versioned** (#456).
  `CreateLaunchTemplateVersion`, `ModifyLaunchTemplate`,
  `DescribeLaunchTemplateVersions` and `DeleteLaunchTemplateVersions` were all
  unregistered, so a consumer that shipped a second version of a template got
  `InvalidAction` from the create and — worse — every launch silently used the one
  stored parameter set. There was also no way to read a template's contents back at
  all: `DescribeLaunchTemplates` returns only summary fields, matching AWS, and
  `DescribeLaunchTemplateVersions` is the only operation that carries
  `launchTemplateData`.

  **An absent `LaunchTemplate.Version` resolves to the template's *default*
  version, not its latest**, per aws-sdk-go-v2's
  `LaunchTemplateSpecification.Version` ("Default: The default version of the launch
  template"). This is the detail worth checking a test against, because a new
  version does not become the default: creating version 2 and launching without
  naming a version still gets version 1, and an emulator that resolved to the latest
  instead would agree with every test that never pins a default.

  `CreateLaunchTemplateVersion`'s `SourceVersion` inherits the source version's
  parameters and lets the request overwrite the ones it names; **omitting
  `SourceVersion` inherits nothing**, so the new version holds only what the request
  names. `DeleteLaunchTemplateVersions` reports per version at HTTP 200 rather than
  failing the request — the default version cannot be deleted, and a request naming
  both a deletable and an undeletable version puts one entry in each set while still
  returning 200, so a caller checking only the status code sees success. A deleted
  version number is never reused.

  A `CreateFleet` config's `LaunchTemplateSpecification.Version` now reaches the
  launch. It was parsed and then dropped, so a fleet pinned to version 1 launched
  whatever the template held latest, with nothing reporting the substitution.

  Templates stored before this change keep working: a stored template with no
  versions array reads back as version 1, default, synthesized from the parameters
  it does carry. A recorded event log from an earlier substrate replays unchanged —
  no event rewriting is involved.

  Provenance: the default-version deletion is reported with `responseError.code`
  `unexpectedError`, because `ResponseError.code` is a closed six-value enum in the
  AWS SDK models with no default-version member and a typed SDK deserializes
  anything outside it as an unknown variant. AWS's real code for this case is not
  published and no capture of the rejection exists (searched the API reference, the
  CLI help, moto — which does not implement the operation — LocalStack, and the SDK
  models); the message is the reference's own sentence. Both are inferred, not
  captured.
- **SQS message attributes are now validated against their documented rules** (#472).
  Every attribute was stored and returned as sent, so an eleventh attribute, a name real
  SQS reserves, a `DataType` that is not one of the three, and a `Number` attribute
  holding `"abc"` were all accepted and delivered. That last one is the shape this
  tracker exists to close: a consumer whose handler parses a `Number` attribute had no
  way to reach its parse-failure branch, and a test of that branch passed against
  substrate while the same code broke against AWS.

  `SendMessage` and each `SendMessageBatch` entry now enforce, all as
  `InvalidParameterValue` / HTTP 400: the maximum of 10 attributes; a name shorter than
  256 bytes drawn from alphanumerics, `_`, `-` and `.`, with no `AWS.`/`Amazon.` prefix
  and no leading, trailing or sequential period; a `DataType` shorter than 256 bytes
  prefixed `String`, `Number` or `Binary`; a non-empty value on a `String` attribute; and
  a `Number` value that parses as a decimal number within −10^128 … 10^126. Both length
  bounds are exclusive — the guide says "up to 256 characters" while the error says "must
  be shorter than 256 Bytes", and the error is the more specific evidence.

  **The reserved-prefix check is case-insensitive**, so `aws.trace` and `AwS.trace` are
  refused too, per the guide's "or any casing variations". This is the **opposite** of
  #468's `aws:` tag-key rule, where `AWS:foo` remains a legal key. Both are correct, the
  two services document different rules, and the checks are deliberately not shared —
  which is the detail a reader arriving from one will get wrong about the other.

  A rejected send **enqueues nothing**. On a FIFO queue the check runs before the
  deduplication ID is recorded, so a corrected retry reusing the same
  `MessageDeduplicationId` is delivered rather than swallowed as a duplicate — the same
  ordering #454's size check established, for the same reason.

  `SendMessageBatch` reports a violation **per entry**: a `BatchResultErrorEntry` in
  `Failed` with `SenderFault: true` at HTTP **200**, the offending entry not enqueued and
  its siblings delivered. Substrate had no such type — both batch paths emitted an empty
  `Failed` placeholder — so a consumer's per-entry error handling had nothing to
  exercise, and the reference warns that "you should check for batch errors even when the
  call returns an HTTP status code of `200`". `Failed` is now always present, empty rather
  than absent on full success. This is deliberately unlike the batch size checks a few
  lines away, which still fail the whole request with `BatchRequestTooLong`: the payload
  cap is a property of the aggregate the caller transmitted, while a malformed attribute
  is a defect in one entry.

  Provenance, strongest first, and recorded per message in the code. **Real-AWS
  captures**: the count rejection's message (an SDK exception quoting an AWS Request ID
  and status 400), the `Number` cast failure (code *and* message, from boto3 against live
  SQS), and the empty-`String`-value message (captured twice independently). **A
  snapshot-tested reimplementation**: the name and type messages, from LocalStack, whose
  character-class string is reproduced verbatim including its "upper and lower score
  characters" phrasing and its trailing space. **A single reimplementation, the weakest
  claim here and labelled as such in the file**: the `Number` range message, which only
  elasticmq supplies. The count rejection's *code* is absent from its capture and comes
  from agreement across five reimplementations; neither moto nor LocalStack enforces the
  count at all, which is why substrate accepting an eleventh attribute went unnoticed
  until #461 made attributes observable.

  The rules apply on send, not on receive: a message written into state before they
  existed is still returned as stored, because withholding it would make a recorded run
  unreplayable. Tracked separately.
- **`DescribeInstanceAttribute`** (#473), which is the only way to read an instance's
  user data back. `RunInstances` recorded `UserData` and nothing could observe it, so a
  consumer could not assert that the user data their IaC intended reached the instance —
  and #453's launch-template `UserData` fallback could only ever be tested indirectly,
  meaning it could regress with nothing observable changing. Four attributes are
  readable, being the ones backed by state substrate holds: `userData` (returned as
  stored, still base64), `instanceType`, `disableApiTermination`, and `groupSet` (the
  same `groupSet>item` shape `DescribeInstances` reports, per #444). Scalars are wrapped
  in a `<value>` element, as all three of the reference's worked examples show, and
  exactly one attribute appears per response.

  Every other name in AWS's valid-values list is **refused rather than defaulted**, with
  `InvalidParameterValue`, HTTP 400, and `Value (enaSupport) for parameter attribute is
  invalid. Unknown attribute.` Answering `sourceDestCheck` with a default `false` would
  be indistinguishable from a real instance that has it disabled, and a consumer
  asserting on it would get a green test built on a value substrate invented. This
  message has the strongest provenance in the release: it is captured from real AWS in
  [aws/aws-cli#4273](https://github.com/aws/aws-cli/issues/4273) and is byte-identical
  to moto's string — a capture and an independent reimplementation agreeing. The
  reference could not have supplied it, its Errors section being empty. `enaSupport` is
  in AWS's own valid-values list while the same page says the attribute "is not
  supported", and #4273 captures exactly that rejection, so refusing it is fidelity.

  An attribute that was never set is reported as a **present but empty element**
  (`<userData></userData>`), not an omitted one. This is the one shape the reference
  cannot settle — all three examples show an attribute that has a value — so it ships
  from moto's `test_describe_instance_attribute` asserting `response["UserData"] == {}`.
  That is weaker provenance than a capture and is labelled as such in the code and the
  docs, because the two shapes are not interchangeable: an SDK maps a present-but-empty
  element to an empty struct and an omitted one to nil, so `resp.UserData.Value` panics
  under one and not the other.

  `ModifyInstanceAttribute` gained `UserData.Value` and `DisableApiTermination.Value`
  alongside the existing `InstanceType.Value`, and its errors now match the reference's
  wording for a missing or unknown instance instead of substrate's own. `UserData.Value`
  is presence-checked rather than non-empty-checked, so clearing an instance's user data
  is expressible.

  **`ModifyInstanceAttribute` now requires a `stopped` instance for `userData` and
  `instanceType`, where changing a running instance's type previously succeeded** — a new
  rejection on a path that worked before, so a test asserting the old behavior will now
  see `IncorrectInstanceState`, HTTP 400. The reference's Example 1 states it plainly
  ("The instance must be in the `stopped` state"), and the client-error table names user
  data as the same rule's worked example; the code is documented while the message text
  is substrate's own. `disableApiTermination` is deliberately exempt, because
  `RunInstances` documents that termination protection can be enabled "when you launch an
  instance, while the instance is running, or while the instance is stopped" — gating it
  would refuse a call real EC2 accepts, which is the same defect in the other direction.
  Termination protection is recorded and reported but not acted on: `TerminateInstances`
  still terminates a protected instance, which is tracked separately.
- `make tag-releases-check` (`scripts/check-tag-releases.sh`) fails if a published
  `vX.Y.Z` tag has no GitHub Release behind it, run daily by the new `Release Audit`
  workflow. Pushing a tag does not create a Release and nothing asserted otherwise,
  so v0.84.0 and v0.85.0 were both cut correctly and neither appeared on the
  releases page — anything watching releases rather than tags saw nothing ship. The
  previous commit documented the step; this asserts it, because a documented step is
  one a human has to remember.

  Two design points, both about not crying wolf. The check grants a **grace window**
  after a tag's commit, because the documented procedure pushes the tag before
  publishing the Release, so a tag legitimately has neither for as long as writing
  the notes takes. And it is **not part of CI**: it audits repository state rather
  than the contents of a change, so on `pull_request` it would fail a contributor's
  unrelated PR for a missing release they cannot publish. The schedule is what
  catches this class of omission anyway, since it is invisible at push time.

  Tags below v0.68.0 are exempt — 77 of them predate the convention and auditing the
  whole history would report a wall of unactionable failures. The floor's premise is
  asserted rather than trusted: the tag just below it must still lack a Release,
  which also guards the void `v0.67.0` (see `SECURITY.md`) against ever acquiring
  one.

### Changed
- `docs/fidelity.md` now states substrate's position on **error message text**, which
  the page previously left unsaid. It committed to verifying "request/response shapes,
  error codes, pagination, and IAM condition keys" and never mentioned messages — no
  false claim, but a reader could reasonably have assumed more than substrate delivers.
  Codes and HTTP statuses are verified and are the contract SDKs dispatch on; message
  text is faithful only where a capture or a parity-tested reimplementation supplies it,
  and carries its source in the code comment where so. Elsewhere it is substrate's own
  and reads as such. The page now also says why the remainder is closed incrementally
  rather than swept: the reference describes error *conditions* rather than quoting the
  strings AWS sends, so a blind rewrite would replace recognisably substrate-shaped text
  with invented text that reads as authoritative. Tracked in #487, which records the
  measured counts.
- `CLAUDE.md`'s release procedure now includes publishing the GitHub Release. The
  step was absent, so v0.84.0 and v0.85.0 were tagged and their tags pushed with no
  Release entry behind them — the tag alone does not create one, and v0.83.0 was
  the last version to appear on the releases page. Both have since been published
  retroactively against their existing signed tags. The step records the house
  style for the notes and requires `--verify-tag`, because `gh release create`
  otherwise tags the current branch tip when the named tag is missing, which would
  produce an unsigned tag at whatever commit happened to be checked out — the one
  way this step could damage a release rather than merely omit one. Deferred issues
  are also now explicitly moved to the next milestone rather than closed with the
  release.
- `CreateImage` now honours its `TagSpecification.N` `ResourceType` (#468). The
  reference gives two scopes — `image` tags the AMI, `snapshot` tags the snapshots
  created of the attached volumes — but substrate read `TagSpecification.1`'s tags
  whatever they were scoped to, so a request tagging only its snapshots put those tags
  on the AMI. Snapshot-scoped tags now land on the backing snapshot substrate
  materializes, where `DescribeSnapshots` reports them. A caller asserting on
  `DescribeImages` for tags that were actually snapshot-scoped will see them move,
  which is where real EC2 has them.

### Fixed
- **A launch template's `TagSpecifications` and `IamInstanceProfile` now reach the
  instance** (#471). Both were accepted by `CreateLaunchTemplate` and stored nowhere,
  so a template that tagged its instances produced untagged ones and a template naming
  a role produced an instance with none — with nothing failing to say so. The tag half
  is the more damaging: `DescribeInstances --filters tag:Env,Values=prod` is how IaC
  finds the resources it just created, and it simply returned nothing, so an assertion
  on the tags the template asked for could not pass however the code was written.
  Neither field could even be read back before this release, because
  `DescribeLaunchTemplates` carries no `launchTemplateData`; #456's
  `DescribeLaunchTemplateVersions` is what makes the round-trip assertable.

  **Template tags replace rather than merge.** A request naming `Env=req` against a
  template naming `Env=tmpl,Team=x` yields `Env=req` alone — `Team` is not inherited.
  The reference gives no `TagSpecifications`-specific merge semantics, only the general
  "Any additional parameters that you specify for the new instance overwrite the
  corresponding parameters included in the launch template", and replacement is that
  rule applied to the whole specification rather than a per-field citation.

  **Substrate's own `aws:ec2:fleet-id` stamp does not count as the request naming
  tags.** A fleet instance already carries that reserved key by the time the merge
  runs, so the fallback tests for a non-reserved key rather than for an empty set. Had
  it tested emptiness, a fleet launched from a tagging template would have silently
  lost the template's tags — the very defect this entry closes, on the path #443 added.
  It is the same reserved-key exclusion #469's counter uses.

  **A template's tags are subject to both tag rules**, so a template is not a second
  unrestricted tagging path: `TagSpecifications` naming an `aws:`-prefixed key or
  exceeding the 50-tag limit is rejected at `CreateLaunchTemplate` and at
  `CreateLaunchTemplateVersion` — the latter *after* any `SourceVersion` inheritance, so
  an inherited violation is caught rather than propagated. The launch checks again,
  deliberately and not redundantly: a template written straight into state by a replayed
  event log can predate these checks, and that launch is the one that would otherwise
  apply the key.

  Two scope limits, stated rather than implied. Only the **instance** scope is stored: a
  template may also scope tags to `volume`, `network-interface` or
  `spot-instances-request`, none of which substrate models, so those specifications are
  recorded nowhere rather than misapplied to the instance. And the profile is stored as
  the single string the request supplied, so `DescribeLaunchTemplateVersions` echoes it
  in whichever member it arrived in — `arn` for an `arn:`-prefixed value, `name`
  otherwise. `DescribeInstances` still surfaces it as an ARN either way, because AWS's
  instance response shape has no name member; synthesizing the missing member for a
  *template* read-back would report the template as naming something the caller never
  wrote.

## [v0.85.0] - 2026-08-02

### Added
- EC2: `CreateTags` and `DeleteTags` now reject tag keys using the reserved `aws:`
  prefix (#452). Substrate accepted them, so a consumer could tag a resource in a way
  real EC2 refuses — and a test asserting that refusal passed against substrate while
  the same call failed against AWS. The rejection is `InvalidParameterValue`, HTTP 400,
  `Tag keys starting with 'aws:' are reserved for internal use`.

  The whole request is refused before any resource is modified, so a request mixing a
  legal tag with a reserved one leaves every resource it named untouched. `CreateTags`
  accepts up to 1000 resource IDs, and rejecting partway through the apply loop would
  leave a prefix of them tagged — a state real EC2 never produces, and a worse outcome
  than either accepting or refusing cleanly.

  **The match is case-sensitive**, which corrects the assumption #452 was filed with.
  AWS documents tag keys and values as case-sensitive, and every observed rejection
  quotes the lowercase form, so `AWS:foo` and `Aws:foo` are ordinary user tags and are
  accepted. A case-folded check would have traded this infidelity for a new one in the
  opposite direction.

  **Provenance, stated plainly:** the `CreateTags` reference's Errors section is empty,
  so neither the code nor the message is derivable from the API model. Both come from
  observed real-AWS responses — two independent captures giving byte-identical text,
  one recording `Service: AmazonEC2; Status Code: 400; Error Code:
  InvalidParameterValue`. Both captures are of `RunInstances` tag-on-create rather than
  `CreateTags`; the message is evidently shared across the tagging paths, but substrate
  has not observed it on `CreateTags` directly. The `DeleteTags` rejection rests on
  weaker evidence still: no captured `DeleteTags` error was found, so its code and
  message are inherited from the `CreateTags` capture. What the tagging documentation
  does state unambiguously is the outcome — such a tag "can't be edited or deleted" by
  a caller — and a `DeleteTags` that returned success while leaving the tag in place
  would be its own infidelity, so refusing is the closer of the two available
  behaviors. `docs/services.md` records all of this rather than implying a doc
  citation.

  Two parts of AWS's rule are deliberately **not** covered. `RunInstances`
  tag-on-create is unrestricted: real EC2 rejects a reserved key there too, but
  substrate stamps `aws:ec2:fleet-id` (#443) by building `RunInstances`
  `TagSpecification` params, so restricting that path would reject substrate's own
  fleet tagging and silently undo the only route from an `instant` fleet back to its
  instances. A regression test pins that a fleet still stamps and still filters on the
  tag. And the companion rule that reserved tags do not count against the 50-tag
  per-resource limit is vacuous here, because substrate does not model that limit at
  all — `TagLimitExceeded` never fires. Both gaps are tracked separately.
- SQS: `SendMessage` and `SendMessageBatch` now enforce `MaximumMessageSize` (#454).
  No length check existed anywhere in the send path — against the queue's attribute or
  against any constant — so a body of any size was accepted and delivered. Real SQS
  rejects an oversized message, which meant a consumer's too-large-payload branch
  (chunk, compress, spill to S3) was unreachable and any test of it passed while
  verifying nothing. This is the enforcement half of #439, which deliberately
  corrected only the reported default.

  `SendMessage` fails with `InvalidParameterValue`, HTTP 400, and a message naming the
  limit that applied. The limit is the queue's **effective** `MaximumMessageSize`,
  resolved through the 1 MiB default when the attribute is unset and read via the same
  call `GetQueueAttributes` uses — so the number a caller reads back is by construction
  the number enforced. A queue created with an explicit smaller value enforces that
  value. The boundary is inclusive: a message of exactly the limit is accepted, since
  AWS's "must be shorter than N bytes" wording describes N as the maximum size.

  **Message attributes count toward the measured size**, as AWS measures it: the body
  plus each attribute's name, data type and value, with a binary value counted at its
  raw decoded length. The developer guide states that "all components of a message
  attribute are included in the 1 MiB message size restriction", and the per-component
  breakdown is the one AWS's own Extended Client Library uses to decide whether a
  payload needs offloading to S3. Measuring the body alone would be wrong in the
  direction that matters — a caller packing most of the budget into metadata is over
  the limit in AWS and under it here, so the payload shape that motivates a chunking
  branch is precisely the one substrate would have accepted. Attributes are parsed for
  measurement only; storing and returning them is tracked separately (#461).

  `SendMessageBatch` enforces both documented limits: `BatchRequestTooLong` when the
  combined payload exceeds 1 MiB, and `InvalidParameterValue` when a single entry
  exceeds the queue's per-message limit. Because the two limits are equal on a default
  queue, a batch carrying one oversized entry breaches the total as well — and real AWS
  reports `BatchRequestTooLong` for that case rather than the per-message error, so the
  total is checked first. The queue attribute is a per-message cap and does not lower
  the request payload cap, so ten legal 1 KiB entries on a 1 KiB queue are accepted.

  A rejected send or batch enqueues nothing, rather than a prefix of the batch that a
  retry would then duplicate. On a FIFO queue the check runs before the deduplication
  ID is recorded, so a corrected retry reusing the same `MessageDeduplicationId` is
  delivered instead of being swallowed as a duplicate — which would have been a worse
  failure than the original, because it looks like success.

  **Provenance, stated plainly:** the per-message error code is not derivable from the
  API model. `SendMessage` declares no oversized-message error at all, its
  `InvalidMessageContents` is documented as a character-set error, and
  `BatchRequestTooLong` is declared only on `SendMessageBatch`. The code and both
  message wordings come from observed real-AWS responses — captured SDK errors carrying
  `code: 'InvalidParameterValue'` with HTTP 400, and `BatchRequestTooLong: Batch
  requests cannot be longer than N bytes. You have sent M bytes.` — corroborated by
  independent reimplementations emitting the same strings. `docs/services.md` records
  this rather than implying a doc citation.

### Fixed
- S3: `BlockPublicAcls` and `BlockPublicPolicy` are now enforced rather than merely
  recorded (#458). #446 made the four Block Public Access settings storable and
  readable, and nothing acted on any of them — so a consumer whose test asserted "once
  we lock the bucket down, a public ACL is refused" got a green test that verified
  nothing, and the assertion would have to be written a second time before it ever ran
  against AWS. A bucket with `BlockPublicAcls` set now refuses a public ACL on
  `PutBucketAcl` and `PutObjectAcl`, and one with `BlockPublicPolicy` set refuses a
  public policy on `PutBucketPolicy`, both with `403 AccessDenied` / `Access Denied`.

  **A rejection stores nothing.** The check runs before the write, so the bucket or
  object keeps the ACL or policy it already had — "existing policies and ACLs for
  buckets and objects aren't modified". Deleting the configuration re-allows what it was
  refusing, matching the documented reversibility. `PutObjectAcl` reads the *bucket's*
  configuration: "Amazon S3 doesn't support block public access settings on a per-object
  basis".

  **A public policy is decided by assuming public and then disqualifying, not by
  looking for `Principal: "*"`.** This is the substance of the change and it is stricter
  than the wildcard check the obvious reading suggests. Per the user guide, S3 "begins by
  assuming that the policy is public" and a statement qualifies as non-public only when
  it grants access solely to *fixed* values — no `*`, no `?`, no `${...}` IAM policy
  variable — through its `Principal` or through a `Condition` on one of the documented
  keys (`aws:SourceIp`, `aws:SourceArn`, `aws:SourceVpc`, `aws:SourceVpce`,
  `aws:SourceOwner`, `aws:SourceAccount`, `aws:userid`, `aws:PrincipalOrgID`,
  `aws:PrincipalArn`, `aws:PrincipalAccount`, `s3:DataAccessPointArn`,
  `s3:DataAccessPointAccount`). So `Principal: "*"` narrowed by `StringLike
  aws:SourceVpc: "vpc-*"` **is public** — the narrowing value is itself a wildcard —
  where the same statement with `StringEquals aws:SourceVpc: "vpc-91237329"` is not.
  Only an `Allow` can make a policy public, and one surviving public statement makes the
  whole policy public: the guide's own example, where a single public statement disables
  an otherwise-legal cross-account grant.

  The `aws:SourceIp` breadth rule is implemented too, including the exclusion that makes
  it usable: a range "broader than `/8` for IPv4 and `/32` for IPv6 (excluding RFC1918
  private ranges)" pins nothing, so a policy conditioned on `0.0.0.0/0` is public
  despite containing no wildcard character, while `10.0.0.0/8` and a unique-local IPv6
  prefix are not. So is the `s3:DataAccessPointArn` carve-out, where a wildcard
  access-point name does not make a *bucket* policy public as long as the account ID is
  fixed.

  **A public ACL is one granting any permission to `AllUsers` or
  `AuthenticatedUsers`** — matched on the grantee URI, with the permission not
  inspected, so `WRITE_ACP` counts as much as `READ`. `AuthenticatedUsers` is every AWS
  account rather than every account in yours, which is why it counts despite the name and
  why it can only arrive through an XML body: the canned-ACL resolver never emits it.
  All three documented forms are covered — the `x-amz-acl` canned header, an XML `Grant`
  naming a public group, and an `x-amz-grant-*` header whose grantee list contains one.
  The grant headers were previously unread anywhere in substrate and are parsed for this
  check only; an ACL set through them is still not stored, which is a separate gap.

  **Provenance:** none of `PutBucketAcl`, `PutObjectAcl` or `PutBucketPolicy` documents
  an Errors section covering this, so the `AccessDenied` / `Access Denied` / `403` triple
  comes from observed real-AWS behaviour rather than from the API model — a blocked
  `PutBucketPolicy` surfaces through the CLI as `An error occurred (AccessDenied) when
  calling the PutBucketPolicy operation: Access Denied`. The definitions of "public" are
  quoted from the user guide's *Blocking public access → The meaning of "public"*.

  Three things stay as they were, deliberately. `IgnorePublicAcls` and
  `RestrictPublicBuckets` remain recorded-only: both govern how an incoming request is
  evaluated against an ACL or policy already in place rather than which write is refused,
  and substrate has no unauthenticated or cross-account request path to deny. `PutObject`
  and `CreateBucket` with a public ACL are not refused, because neither handler reads
  `x-amz-acl` at all — covering them means modelling ACL-on-create first. And a body that
  parses as JSON but not as a policy document is not treated as public: `PutBucketPolicy`
  already rejects non-JSON with `400 MalformedPolicy`, and the new check is not a second
  validity check. An unconfigured bucket, and one whose four settings are all `false`,
  behave exactly as they did before enforcement existed — the case that guards #446.

- SQS: message attributes are now stored on send and returned on receive (#461). #454
  parsed them in order to measure them against `MaximumMessageSize` and then discarded
  them: `SQSMessage.MessageAttributes` existed and was never populated, and
  `ReceiveMessage` had no attribute handling at all. A consumer routing on an attribute
  — a `messageType` discriminator, a trace ID, a tenant key — saw every message fall to
  its default branch, with no error to explain why, because the attribute was simply
  not there. `SendMessage`, `SendMessageBatch` and `ReceiveMessage` all carry them now,
  under both the query and JSON protocols.

  **Attributes are returned only for the names a receive asks for.** Returning them
  unconditionally is the trap here, and it fails in the permissive direction: a consumer
  whose production caller never sets `MessageAttributeNames` would pass against
  substrate and then read nothing from real SQS. All the documented selector forms work
  — `All` and `.*` for everything, a bare name for one, and the `prefix.*` form the
  guide documents as "all message attributes starting with a prefix, for example
  `bar.*`". A named attribute the message does not carry is absent rather than an error,
  matching AWS.

  `MD5OfMessageAttributes` is returned on `SendMessage`, on each `SendMessageBatch`
  result entry, and on each received message, computed with the algorithm published in
  the developer guide under "Calculating the MD5 message digest for message attributes".
  Two details of it are load-bearing and a reimplementation gets both wrong by default:
  **a binary value is hashed raw, not base64** — the same raw-versus-encoded distinction
  #454 already drew for measurement, except here there is a hash to settle it, since
  base64 yields `5ff413c9dc7bd18abea88ca05643f902` where AWS yields
  `049075255ebc53fb95f7f9f3cedf3c50` for the same input — and **a custom data-type
  suffix is hashed in full**, so `Number.java.lang.Long` is the whole string rather than
  its `Number` base type.

  The implementation is pinned against **three real-AWS digests**, which is what makes
  #461's "verified against a known-good digest" criterion satisfiable with no network
  access: the vectors ship in the test file with their provenance, and each fails on a
  different mistake — the minimal single-attribute case, the custom-suffix case, and the
  raw-binary case.

  The field is **omitted entirely** for a message with no attributes rather than
  reported as the MD5 of zero bytes, matching observed behaviour. A digest of nothing is
  a value a caller could compare against and "successfully" verify, which is worse than
  no value at all.

  On a receive the digest covers **what is being returned**, not what was sent, so a
  request naming a subset gets that subset's digest — the digest exists to let a caller
  checksum the attributes in hand, and replaying the send-time value would fail that
  check for a request that legitimately succeeded. A deduplicated FIFO send likewise
  reports the digest of that request's attributes rather than the stored original's.
  Attributes are emitted in name order; real SQS promises no order, but a Go map
  iterates randomly, and two identical requests must not produce different responses.

  Not enforced, and deliberately so for now: the documented maximum of 10 attributes per
  message, and the attribute-name character rules (no `AWS.`/`Amazon.` prefix, no
  leading, trailing or sequential periods). Both are newly reachable and tracked
  separately; a 10-attribute message is accepted, pinned as the boundary it is.
- EC2: `RunInstances` now merges a named launch template with the request field by
  field, instead of consulting the template only when the request omitted `ImageId`
  (#453). The entire template block was gated on the AMI being absent, so a request
  passing both an `ImageId` and a `LaunchTemplate` never read the template at all: its
  `InstanceType`, `KeyName`, `UserData`, security groups and — since #444 — its subnet
  and public-IP preference were all silently dropped. The launch still succeeded and
  still returned an instance ID, so nothing in the response said anything was wrong;
  the instance simply was not the one that was asked for. A consumer templating a
  `c5.xlarge` in a private subnet and pinning the AMI per environment at the call —
  which is why `ImageId` is passed alongside a template in the first place — got a
  `t3.micro` in the default VPC and a green test.

  The template is now resolved whenever one is named, and each field falls back to it
  only when the request did not supply that field. AWS's `RunInstances` reference is
  the specification: "Any additional parameters that you specify for the new instance
  overwrite the corresponding parameters included in the launch template." The
  `ImageId` check added by #412 still runs after resolution, so a template remains a
  valid sole source of the AMI, and a template that carries none still fails with
  `MissingParameter`.

  The `UserData` fallback is new rather than restored: a template's user data was
  parsed and stored but never applied to the launch. Note that no registered operation
  reads it back — substrate does not implement `DescribeInstanceAttribute` — so the
  merge is observable only in the recorded instance state, not through an API call.

  Separately, an explicit `InstanceType=t3.micro` on the request is now honoured over a
  template's type. The `t3.micro` default used to be applied *before* the template was
  read, and the template fallback then tested for that literal value as a proxy for
  "the request named no instance type" — so asking for `t3.micro` alongside a template
  naming `m5.large` yielded `m5.large`, exactly inverting the documented precedence.
  The default is now resolved last, after the template has had its chance, so it
  applies only when neither side names a type. This bug was reachable before this
  change and is far more reachable after it, which is why it is fixed here rather than
  deferred.

  A template's `TagSpecifications` and `IamInstanceProfile` are still not parsed, so
  neither participates in the merge; the request wins those two by default rather than
  by rule.
- S3: `HeadObject` now resolves a synthesized task-completion record, so `HEAD` and
  `GET` agree about whether the key exists (#457). `getObject` consulted the
  completion resolver when no real object was staged at
  `tasks/<task_id>/completion.json`; `headObject` did not, and answered `NoSuchKey`
  for a key `GET` served with a 200 and a full body. Real S3 never contradicts itself
  that way.

  The practical consequence was that `aws s3 cp` could not read a synthesized record
  at all — the CLI HEADs before it GETs, so it failed on the 404 and never reached
  the working GET. That is the exact command spawn prints for users. An SDK
  `HeadObject` existence poll broke in the worse direction: absence reads as "still
  running", so a wait loop spun forever rather than failing visibly.

  `HeadObject` now reports the same `Content-Length`, `ETag`, `Content-Type` and
  `Last-Modified` a `GET` of the same key returns, and the `ended_at` clock gate that
  makes a not-yet-complete record absent applies identically to both verbs — so a HEAD
  before the simulated completion time is still the `404` a poll loop depends on. A
  real staged object still wins, and a read naming an explicit `versionId` still does
  not resolve, since a synthesized record has no version history.

  `ListObjectsV2` deliberately continues not to enumerate synthesized records, and
  that asymmetry is now recorded in `docs/services.md` rather than left to be
  rediscovered: a keyed read works because the caller names the task, whereas a list
  is unkeyed and substrate cannot enumerate the task IDs a consumer might ask about.
  The task-completion resolver and its seeding endpoints are documented there for the
  first time.
- S3: `DELETE /bucket?publicAccessBlock` no longer destroys the bucket (#446). The
  `?publicAccessBlock` subresource was unrouted on all three verbs, and because it is
  addressed as a bare query key on the bucket itself, each request fell through to the
  bucket-level operation for its method — so a `DeletePublicAccessBlock` reached
  `DeleteBucket` and deleted the bucket and its contents. On an empty bucket that
  returned `204`, exactly as a successful `DeletePublicAccessBlock` does, so the caller
  had no signal at all: the next operation on that bucket failed with `NoSuchBucket`
  for no visible reason. On a non-empty bucket it returned `409 BucketNotEmpty`, naming
  an operation the caller never issued.

  The other two verbs were wrong in the same way. `PUT` fell through to `CreateBucket`
  and answered `409 BucketAlreadyExists` — the shape SDKs and CloudFormation send
  immediately after creating a bucket, so locking a bucket down failed on the
  already-owned bucket it was meant to configure. `GET` fell through to `ListObjects`
  and returned `200` with a `ListBucketResult` body, which an SDK parses into an empty
  `PublicAccessBlockConfiguration` — reporting "public access is not blocked" for a
  bucket whose settings substrate had never stored.

  All three are now routed and modeled. `PutPublicAccessBlock` records the
  configuration and replaces it wholesale; settings the body omits are reported back as
  `false`, as S3 does. `GetPublicAccessBlock` returns `404
  NoSuchPublicAccessBlockConfiguration` for a bucket with no configuration, which keeps
  it distinguishable from the all-false configuration a consumer may have written on
  purpose. `DeletePublicAccessBlock` returns `204`, is idempotent, and touches nothing
  but the configuration.

  One deliberate limit, documented in `docs/services.md`: substrate does not apply
  S3's April 2023 default of enabling all four settings on a newly created bucket,
  because that default comes from AWS-managed account state substrate does not model
  and seeding it would make the `NoSuchPublicAccessBlockConfiguration` path
  unreachable. Enforcement of `BlockPublicAcls` and `BlockPublicPolicy` landed in this
  same release — see the `### Fixed` entry for #458.

## [v0.84.0] - 2026-08-01

### Fixed
- EC2: `RunInstances` and `DescribeInstances` now report an instance's security
  groups as `groupSet` (#444). The groups a launch named were parsed, validated
  against the subnet's VPC and stored — and then absent from every read, so a
  consumer that had just launched an instance with an explicit security group read
  back an instance with no groups at all. Nothing errored; the fact was simply
  missing, which reads as "this instance has no security groups" rather than
  "substrate didn't report them".

  Both responses emit `groupId` and `groupName`, matching AWS's `GroupIdentifier`,
  for groups from any source: the request, the launch template's network interface,
  or the default VPC's `default` group. `groupName` is **omitted** when the group
  cannot be resolved — a group deleted after the launch keeps reporting its
  `groupId`, since that is what the launch recorded, rather than being given an
  invented name. The two response builders had each declared their own instance
  item type, which is how a field could go missing from both; they now share the
  group item, so they cannot disagree about its shape.
- EC2: a launch template's network interface is no longer discarded, so a template
  that names a subnet, security groups or a public-IP preference is honored on
  launch (#444). `CreateLaunchTemplate` accepted
  `LaunchTemplateData.NetworkInterface.1.*` and stored none of it, so an instance
  launched from such a template landed in a substrate-chosen subnet — a wrong
  answer returned confidently, with a 200 and a plausible-looking instance.

  This hit exactly the templates configured the way AWS requires: AWS's
  `RequestLaunchTemplateData` has **no top-level `SubnetId`** member, so a network
  interface is the only place a template can name a subnet, and the only place
  `AssociatePublicIpAddress` exists at all. It also produced a confusing
  asymmetry, since a `CreateFleet` override's subnet *was* honored while the same
  subnet named in the template was not.

  Precedence follows AWS: a value named in the request wins over the template's,
  and a fleet override wins over both (it reaches `RunInstances` as a
  request-level value). `AssociatePublicIpAddress` is stored as a string because
  three states are observable — absent (use the subnet default), `true` (force one
  even on a non-default subnet) and `false` (suppress one). The interface's group
  list is read from `SecurityGroupId.N`, which is what real SDKs send (the AWS
  model gives the `Groups` member the `locationName` `SecurityGroupId`), with
  `Groups.N` accepted as a secondary spelling. Only interface index 1 is modeled,
  matching `RunInstances`.
- SQS: `GetQueueAttributes` reports the documented `MaximumMessageSize` default of
  1,048,576 bytes (1 MiB) rather than 262,144 (256 KiB) (#439). 256 KiB is the
  historical limit; the current CreateQueue reference documents 1 MiB as the
  default, so a consumer sizing a payload against what substrate reported was
  working from a number a real queue would not give it. An explicitly requested
  value is still honored — 256 KiB remains a legal size, it is simply no longer the
  default.

  Both sites moved together and now share one constant: `GetQueueAttributes`'
  inline fallback, and the defaults `CreateQueue`'s conflict check (#429) resolves
  an existing queue's unset attributes through. They can no longer diverge, which
  matters because a divergence would report one number and then reject a re-create
  naming exactly that number as `QueueNameExists`.

  Substrate still does **not** enforce `MaximumMessageSize` on `SendMessage` — no
  length check exists against the attribute or any constant, so an oversized body is
  accepted. That absence is deliberate here: enforcement is a new error path with
  its own scope, and is tracked separately.

### Added
- EC2: `CreateFleet` stamps the reserved `aws:ec2:fleet-id` tag on every instance
  it launches, so a fleet's instances are reachable with a `DescribeInstances`
  `tag:aws:ec2:fleet-id` filter (#443). Without it there was no route from a fleet
  ID back to its instances at all: `DescribeFleetInstances` rejects `instant`
  fleets outright, and the `fleetInstanceSet` a fleet response echoes is a record
  of what was launched rather than what is running — it never drops terminated
  instances. A consumer enumerating an instant fleet therefore saw a
  fully-provisioned fleet as empty, which reads as "the instances are gone" rather
  than "substrate didn't record this". The tag survives alongside a caller's own
  launch-time `TagSpecification` entries and is applied per capacity pool, so a
  multi-pool fleet tags all of its instances.

  Source note: this tag is documented in neither the EC2 API reference nor the
  fleet tagging and describe pages. Substrate models it on observed real-AWS
  behaviour reported by the parsl-aws-provider consumer, whose fleet-to-instance
  lookup is built on it — not on a documented API contract. Substrate also does
  not yet enforce the rules AWS attaches to the reserved `aws:` prefix
  (`CreateTags` still accepts an `aws:`-prefixed key that real EC2 rejects); that
  is tracked separately.

## [v0.83.0] - 2026-08-01

### Added
- S3: `Cache-Control`, `Content-Disposition`, `Content-Language` and `Expires`
  persist on an object and are returned on every read (#430). `S3Object` had no
  fields for them at all, so substrate accepted the headers on write and silently
  discarded them — a consumer asserting that an `attachment; filename=` download
  name or a cache lifetime survives an upload got a green test verifying nothing,
  because the write appeared to succeed. All three write paths record them now:
  `PutObject`, `CreateMultipartUpload` → `CompleteMultipartUpload` (Complete
  accepts no object-metadata headers, so the family is carried on the upload
  record from creation), and `CopyObject` under the same
  `x-amz-metadata-directive` that already governed `Content-Type` and
  `Content-Encoding` — S3 documents no per-header variant, so a `REPLACE`
  restating only `Content-Type` drops the rest. The four share one embedded
  struct declaration used by both `S3Object` and `S3MultipartUpload`, so the two
  write paths cannot drift apart on a member the way they did on
  `Content-Encoding` in #406. An absent header stays **absent** on the response
  rather than becoming empty, since an SDK distinguishing nil from `""` would
  otherwise report the wrong one. `Expires` is stored as a string and never
  parsed: real S3 returns what the caller sent, the Go SDK deprecates its
  `time.Time` `Expires` in favour of the unparsed `ExpiresString`, and parsing
  here would make a consumer's parse-failure branch unreachable.
- SQS: `QueueDeletedRecently` is seedable on `CreateQueue` (#429). AWS requires a
  60-second wait after `DeleteQueue` before the same name can be reused, so a
  consumer's delete → recreate → retry loop has a documented error to handle;
  substrate keeps no memory of a delete, so that branch could never be exercised
  offline. `POST /v1/sqs/consistency` now accepts `deletedRecentlyMisses`
  alongside the existing lookup counters, keyed by queue name or the `"*"`
  wildcard and cleared by the same `DELETE`. It is counted rather than timed for
  the reason the lookup windows are: the real condition is a wall-clock window,
  and a wall-clock window makes the assertion depend on how long the rest of the
  test took. Unlike the lookup counters it applies only while the name is free —
  `QueueDeletedRecently` describes a name too recently freed, so a create that
  hits an existing queue is an idempotent success and does not spend the budget.
  Unseeded behaviour is unchanged: the counter defaults to 0, so a recreate is
  still instant.

### Fixed
- SQS: `CreateQueue` returns `QueueNameExists` when a name is reused with
  differing attribute values, instead of treating every repeat as idempotent
  (#429). `createQueue` returned the existing queue's URL regardless of what the
  request asked for, so two stacks or two test cases claiming one queue name with
  different settings both "succeeded" and the second silently got the first one's
  configuration — a confidently wrong answer, not a missing error, and the
  consumer's error branch was unreachable. Only attributes **present in the
  request** are compared, per the error's own definition ("only if the request
  includes attributes whose values differ from those of the existing queue"); an
  omitted attribute is no opinion, which is also what keeps a CloudFormation
  re-deploy working, since a template forwards only the properties it declares.
  An existing queue's unset attributes resolve through the values
  `GetQueueAttributes` reports before comparing, so `VisibilityTimeout=30` against
  a bare queue is not a conflict, and `FifoQueue=true` against a `.fifo` queue —
  what every SDK and template sends — stays idempotent. The message names the
  offending attribute, which AWS's own wording omits.
- S3: `PutObject` and `CreateMultipartUpload` no longer record the `aws-chunked`
  transfer encoding as the object's `Content-Encoding` (#428). A SigV4 streaming
  upload — what every AWS SDK sends for a body it does not buffer — arrives with
  `Content-Encoding: aws-chunked`; substrate decoded the chunk framing but stored
  that token verbatim, so `GetObject`/`HeadObject` reported the object as
  `aws-chunked`-encoded when the stored bytes were plain. A consumer that
  dispatches decompression on `Content-Encoding` was handed a name that is not a
  content codec, for content needing no decoding — and against real AWS the
  header is absent entirely, so nothing revealed the difference until a
  round-trip failed. A genuine codec sent alongside it (`aws-chunked, gzip`, as
  an SDK streaming a compressed body sends) is kept, in order: dropping the
  header wholesale would lose the codec that *is* applied, which is the #406
  failure. Both write paths now resolve the header through one helper, so they
  cannot drift apart on it again the way #406 did.
- EC2: `RunInstances` validates `MinCount` and `MaxCount` instead of silently
  clamping them (#431). Both were parsed with `strconv.Atoi`'s error discarded and
  anything `<= 0` raised to 1, so `MinCount=0` launched an instance where real EC2
  fails the request, and a non-numeric value launched one too. That is worse than
  a missing error: it is a confidently wrong answer, so a consumer asserting on
  the launched count got a green run for a request AWS rejects. A count that is
  present but unparseable or below 1, or a `MinCount` above `MaxCount`, is now
  `InvalidParameterValue` / HTTP 400 with the parameter named in the message.
  **Absence still defaults**, so nothing relying on today's behaviour changes: no
  counts means one instance, and an absent `MaxCount` defaults to `MinCount`
  rather than to 1 — a request asking for three no longer risks launching one.
  Validating presence would catch an unreachable bug class, since both are
  required members that fail client-side in every typed SDK; validating *values*
  catches a reachable one, because the query protocol carries them as strings and
  neither botocore nor `aws-sdk-go-v2` range-checks them.

## [v0.82.0] - 2026-08-01

EC2 Fleet support, four CloudFormation resource types that silently deployed
nothing, and the reason every published container image reported its version as
`"dev"`.

The theme is the same one that drove v0.80.0 and v0.81.0 — **substrate reporting
success while doing nothing observable** — but reached from a different
direction. Here it was not a wrong response shape: the four CFN types had working
API handlers and were simply never dispatched, so a stack deployed
"successfully" with the resource absent, and a security group came back with no
rules however the template was written. The `-X` linker flag that fell silently
on the wrong package path is the same failure in the build: it linked cleanly and
left every release image reporting `"dev"`.

**If you pin a container image tag**, this is the release where `/health` starts
reporting the version that is actually answering. Existing tags keep reporting
`"dev"` — published tags are immutable, so the fix cannot be applied
retroactively.

### Added
- EC2 Fleet: `CreateFleet`, `DescribeFleets`, and `DeleteFleets` (#387).
  Instances launch through the same `RunInstances` path as a direct launch, so
  they are visible to `DescribeInstances` with subnet/security-group validation,
  placement groups, and launch-time tag propagation intact.
  `LaunchTemplateConfigs[]` × `Overrides[]` are flattened into ordered capacity
  pools (sorted by `Priority` under a `prioritized` allocation strategy) and
  fulfilled round-robin, so `fleetInstanceSet` carries one item per pool each
  with a *list* of instance IDs. An `instant` fleet returns instances and errors
  synchronously; `request`/`maintain` fleets return only the fleet ID, matching
  AWS. `DescribeFleets` returns an `instant` fleet only when its ID is named
  explicitly; `DeleteFleets` terminates the fleet's instances when
  `TerminateInstances=true` or the fleet is `instant`.
- Seedable EC2 Fleet partial fulfillment via
  `POST`/`DELETE /v1/ec2/fleet-shortfall`, keyed by launch-template ID/name or
  `"*"` (#387). A fleet that asks for 12 and receives 8 still returns a fleet ID
  and echoes the *request* in `TotalTargetCapacity`, so this path — rare and
  hard to trigger against real AWS — is where callers most often go wrong;
  seeding makes it instant and reproducible. The unfulfilled capacity is
  reported per pool in `errorSet` with a configurable error code and lifecycle.
- Source and destination security groups in security-group rules
  (`IpPermissions.N.Groups.M.GroupId`), rendered by `DescribeSecurityGroups` as
  `groups>item` (#388). A rule whose source is another security group — the
  self-referencing rule in particular — has no CIDR at all and previously could
  not be represented.
- CloudFormation wiring for four resource types whose API handlers already
  existed but which fell through to `deployGenericStub`, so a stack deployed
  "successfully" while the resource was never created (#388):
  `AWS::EC2::LaunchTemplate` (Ref is the real `lt-…` ID, usable by
  `CreateFleet`), `AWS::IAM::InstanceProfile` (creates the profile and attaches
  each entry in `Roles`), and the standalone `AWS::EC2::SecurityGroupIngress` /
  `AWS::EC2::SecurityGroupEgress` rules, which resolve
  `SourceSecurityGroupId`/`DestinationSecurityGroupId` through `Ref`/`GetAtt` so
  self- and mutually-referencing groups work.
- `make version-check` (`scripts/check-version-stamping.sh`), run by CI, builds
  with the Dockerfile's own `-ldflags` and asserts the running server reports
  that version (#402). It also fails if the Dockerfile and Makefile stamp
  different symbols, or if any `-X` names a symbol that does not exist. A silent
  `-X` is invisible at build time, so the guard asserts the observable outcome
  rather than the spelling of the flag.
- The Docker release workflow smoke-tests the image it just pushed, asserting
  `/health` reports the tag being released (#402) — the build-arg wiring that
  only a real image can exercise.

### Changed
- `AWS::EC2::SecurityGroup` now authorizes the rules declared inline in its
  `SecurityGroupIngress`/`SecurityGroupEgress` properties (#388). They were
  parsed but never applied, so `DescribeSecurityGroups` reported a group with no
  rules regardless of how the template was written.
- `AWS::EC2::Instance` passes through `IamInstanceProfile`, `KeyName`, and
  `SecurityGroupIds` (#388) — the profile reference is only resolvable now that
  `AWS::IAM::InstanceProfile` creates a real profile.
- `AuthorizeSecurityGroup{Ingress,Egress}` parse every `IpPermissions.N` entry
  and all of each entry's `IpRanges.M`, rather than only the first rule and
  first CIDR (#388). `RevokeSecurityGroup{Ingress,Egress}` match on source as
  well as protocol and ports, so revoking a CIDR rule no longer removes a
  source-group rule on the same port.
- The generic-stub warning now reports whether the unhandled type's service
  plugin is loaded (#388). It previously read as "substrate doesn't model this",
  which is misleading when the API actions exist and only the wiring is missing.

### Fixed
- Published container images report their release version from `/health` and
  `/_localstack/{health,info}` instead of `"dev"` (#402). The Dockerfile stamped
  `github.com/scttfrdmn/substrate.Version` — the root module path, which contains
  no Go files — while the variable those endpoints serve is
  `github.com/scttfrdmn/substrate/emulator.Version`. The linker silently ignores
  `-X` against a symbol that does not exist, so the build succeeded with no
  diagnostic and every image since the endpoint was added reported `"dev"`.
  Consumers pinning an image tag for reproducible CI had no way to confirm at
  runtime which build answered, so a silently-wrong tag or a stale cached image
  went unnoticed. `substrate --version` was unaffected, which is why this
  survived so long.
- Package documentation no longer states a plugin count or per-plugin operation
  count (#389). `emulator/doc.go` claimed 39 plugins and 23 S3 operations while
  the registry had 63 and `s3_plugin.go` 53 — the counts that #364 made
  generated elsewhere had drifted here. It now points at the generated
  `docs/services.md` instead of repeating a number that can drift.
- `CLAUDE.md`'s key-files table pointed at four paths that moved to `emulator/`
  in the #310 reorg (#389). The stale next-release pin flagged in the same issue
  was already removed in v0.81.0.

## [v0.81.0] - 2026-08-01

A fidelity release driven by three consumer-filed issues (`objectfs`,
`spore-host/spawn`) that share one defect with v0.80.0: **substrate returned
success, or a confident wrong answer, where real AWS signals failure** — so the
consumer's error branch was unreachable and their suite reported green while
verifying nothing. Each was found the expensive way: one as a false
`DATA_CORRUPTION` against correct application code, one as a bug that shipped and
was caught only by a paid smoke test against real AWS, one as a retry path that
could not be exercised offline at all.

### Added
- The SQS create-then-lookup eventual-consistency window is now seedable via
  `POST`/`DELETE /v1/sqs/consistency`, keyed by queue name or the `"*"` wildcard
  (#413). AWS documents that a caller "must wait at least one second after the queue
  is created to be able to use the queue", so a real
  `CreateQueue` → `GetQueueUrl` → retry loop can see `QueueDoesNotExist` for a queue
  that exists. Substrate resolved a new queue instantly, which made that retry path
  unreachable — the consumer could not exercise it offline at all, so their suite
  reported green on a loop it never ran.
  `GetQueueUrl` and `GetQueueAttributes` have independent counters, both defaulting
  to 0, so unseeded behaviour is unchanged. The window is counted in **misses rather
  than measured as a duration**: the simulated clock advances with wall time from
  its baseline, so a duration seed would expire partway through a test and make
  "still missing" assertions wall-clock dependent, which no test here may be. A miss
  counter is exactly reproducible.
  Two ordering rules make it usable from a harness: a miss is consumed only when the
  queue actually exists, so seeding before `CreateQueue` does not silently burn
  budget on lookups against a genuinely absent queue; and a seed counts down the
  next N misses rather than re-arming on `CreateQueue`, which would otherwise be
  ambiguous given that `CreateQueue` is idempotent, and would make the data path
  write control-plane state.
  A state-store failure during the seed lookup propagates as an error rather than
  collapsing into `QueueDoesNotExist`: the two are opposite signals, since a 400
  tells a caller to stop retrying while a store failure is transient — and this
  seed exists so that retry loops can be tested, so conflating them would undercut
  the feature.

### Fixed
- SQS now raises `QueueDoesNotExist` rather than the legacy
  `AWS.SimpleQueueService.NonExistentQueue` for an operation naming a queue that
  does not exist, across all 14 sites that produced it (#413). The code decides
  whether a consumer can catch the error as a typed exception at all: botocore
  derives the exception class from the resolved error code, so the legacy string
  resolved to a bare `ClientError` and `except sqs.exceptions.QueueDoesNotExist`
  never matched, while `aws-sdk-go-v2` dispatches on
  `strings.EqualFold("QueueDoesNotExist", …)` and does not mention the dotted form
  anywhere in its `sqs` module, so `errors.As(err, &types.QueueDoesNotExist{})`
  never matched either. SQS is an `awsQueryCompatible` JSON service and the dotted
  form is the query-compatibility alias AWS sends in an `x-amzn-query-error`
  header, not in `__type`. Because `writeError` derives `x-amzn-ErrorType` from the
  code, correcting the code fixes both SDKs with no additional plumbing. The 14
  verbatim duplicates are now a single `sqsQueueDoesNotExist` helper, so future
  queue operations inherit the right code.
- `CreateMultipartUpload` now records `Content-Encoding` and
  `CompleteMultipartUpload` applies it to the assembled object, so `GetObject` and
  `HeadObject` report the codec a multipart object was uploaded with (#406).
  `PutObject` already did this, so the two upload paths disagreed about the same
  object property, and an object compressed above the multipart threshold
  round-tripped as uncompressed. That makes a *correct* application look broken — one
  that dispatches decompression on `Content-Encoding` fails closed on a read it wrote
  properly — and, symmetrically, an application that forgot to set the header on the
  multipart path (a separate input struct from `PutObjectInput`) was indistinguishable
  from one that set it, so any test asserting "the encoding survives a multipart
  upload" passed vacuously. `Complete` accepts no object-metadata headers at all, so
  carrying the value on the upload record from creation is the only place it can come
  from.
- `RunInstances` now rejects a launch that resolves no AMI, with
  `MissingParameter` / "The request must contain the parameter ImageId", HTTP 400
  (#412). It previously accepted an empty `ImageId` and launched an instance, so a
  consumer's error branch was unreachable: the bug reported here shipped and was
  caught only by a paid smoke test against real AWS. The check runs *after* launch
  template resolution, because AWS documents `ImageId` as "Required: No" precisely
  so that a template can supply it — a launch naming a template that carries an
  AMI is still valid, and one that resolves to nothing is not. `ImageId` is an
  optional `*string` in the typed SDKs, so `aws.String("")` serializes as absent
  from the wire and reaches the service rather than failing client-side, which is
  the shape that shipped the bug.
- The request parser no longer coerces an explicitly-empty form-body parameter to
  the bare-key sentinel `"1"` (#412). Bare keys such as `?uploads` and `?delete`
  map to `"1"` so a caller can detect their presence, but the set of bare keys was
  derived from the URL's raw query while the value was applied to `r.Form`, which
  merges query *and* body — the entire AWS Query protocol (EC2, IAM, STS, SQS,
  SNS, CloudWatch) travels in the body. So `ImageId=` did not arrive as empty; it
  arrived as `"1"`, and `RunInstances` launched an instance from an AMI named `1`.
  Any query-protocol parameter sent explicitly empty was affected the same way,
  which also made emptiness unverifiable by the plugin above it. The query-string
  case was already correct (#200); this is the body-side companion.

## [v0.80.0] - 2026-07-31

A fidelity release driven by two downstream consumers (`parsl-ephemeral-aws` and
`objectfs`), who filed ten issues sharing one defect: **substrate returned success,
or a confident wrong answer, where real AWS signals failure.** That makes a
consumer's error branch unreachable, so their suite reports green while verifying
nothing — worse than an incomplete emulator, and directly against this project's
premise that a red test is a real signal.

Ranged and conditional S3 reads, checksum verification, storage classes, symbolic
error codes across five services, EC2 `Invalid*.NotFound`, Lambda invoke metadata,
and a new `pricing` plugin. Closes #391, #392, #393, #396, #397, #398, #399, #400,
#401 and #403.

### Added
- New `pricing` plugin emulating the AWS Price List Query API — `GetProducts`,
  `DescribeServices` and `GetAttributeValues` — over `api.pricing.{region}` and
  `X-Amz-Target: AWSPriceListService.{Op}` (#401). This is the *server* side of
  pricing, for consumers that query AWS rates at runtime; it is the inverse of
  Substrate's existing cost-tracking provider, which consumes the public offer
  index to price simulated usage.
  The offer corpus is seven Amazon S3 SKUs copied verbatim from the live
  `AmazonS3/current/us-east-1` offer file, kept small on purpose: each SKU
  reproduces a response shape callers get wrong. `PriceList` elements are JSON
  documents encoded as *strings*, not objects. `pricePerUnit`, `beginRange` and
  `endRange` are strings with trailing zeros, never numbers. `productFamily` is
  absent from most products — 315 of the 381 in the real file omit it — so a
  filter on it misses the majority of SKUs, while `usagetype` reaches every one.
  `TimedStorage-ByteHrs` carries three `priceDimensions`, the last with
  `"endRange": "Inf"`, so a parser reading only the first reports the
  first-50 TB rate as the only rate.
  Two of those shapes are the reporter's own bugs, now reproducible: their static
  table had `Requests-Tier1` 10× low (it is `"0.0000050000"` per request — $0.005
  per 1,000, with `unit: Requests`, so dividing by 1,000 again is a 1,000× error),
  and filtering `productFamily=Storage` with `volumeType="Glacier Deep Archive"`
  returns *only* `TimedStorage-GDA-Staging` at $0.021/GB-Mo — 21× the $0.00099
  archive rate. No `TimedStorage-GDA-ByteHrs` SKU exists in the S3 offer file at
  all, so that filter cannot return the rate a caller expects; the nearest
  $0.00099 SKU is Intelligent-Tiering's `TimedStorage-INT-DAA-ByteHrs`.
  `Filter.Type` supports the full documented enum — `TERM_MATCH`, `EQUALS`,
  `CONTAINS`, `ANY_OF`, `NONE_OF` — not just `TERM_MATCH`. `MaxResults` bounds are
  per-operation (1–100 for `GetProducts`/`DescribeServices`, 1–**10000** for
  `GetAttributeValues`), out-of-range values are `InvalidParameterException`
  rather than a silent clamp, and an unknown `ServiceCode` is `NotFoundException`
  rather than an empty `PriceList` that would read as "AWS has no such price".
- Price List failures are seedable via `POST`/`DELETE
  /v1/pricing/query-failures`, keyed by operation or the `"*"` wildcard (#401).
  Pricing is the kind of dependency whose failure should degrade a caller rather
  than stop it, and that property is only testable if the failure can be produced
  on demand — otherwise the fallback branch is unreachable and a green suite says
  nothing about it. The seed endpoint rejects any code outside the seven the Price
  List API documents, because a typo'd code would seed an error no SDK catch
  branch matches: the fallback path would go untested while the seed appeared to
  work.
- Lambda function-execution failures are seedable via
  `POST`/`DELETE /v1/lambda/invoke-error`, keyed by function name or the `"*"`
  wildcard (#393). Substrate does not run handler code, so an invoke always took
  the success path and a caller's error branch was unreachable — the seed is what
  makes it testable. The body accepts `errorType` (`Handled` for an exception the
  runtime caught, `Unhandled` for a process that died; defaults to `Unhandled`),
  `errorMessage` and `exceptionType` to populate the synthesized error object in
  the shape the runtime interface clients emit
  (`errorMessage`/`errorType`/`requestId`/`stackTrace`), or a verbatim `payload`
  for a runtime-specific shape substrate does not synthesize. With
  `X-Amz-Log-Type: Tail`, the returned log reflects the failure.
- S3 `GetObject` and `HeadObject` honor a single-range `Range` header (#396),
  returning `206 Partial Content` with `Content-Range` and a `Content-Length`
  equal to the range served. `docs/services.md` had claimed "Supports Range
  header" since the operation was first documented, but the header was never
  read — a consumer whose every read is a ranged GET (a FUSE filesystem, a
  columnar reader seeking within a Parquet footer) silently received whole
  objects, so no test of its read path was verifying anything.
  Range composes with `versionId`, and the synthesized spore.host
  task-completion record is rangeable like any other object.
  The edge cases are the substance of the fix, because S3 does not report an
  error for most bad ranges: a range extending past the end of the object is
  clamped rather than rejected, and a malformed range, a multi-range request, or
  a unit other than `bytes` is *ignored* — the whole object is served with 200.
  Only a range starting at or beyond the object's end is a `416 InvalidRange`,
  whose body carries `<ActualObjectSize>` and `<RangeRequested>` alongside the
  `Content-Range: bytes */<size>` header so a caller can correct the request
  without a second round trip. Every range against a zero-byte object is
  unsatisfiable. Retrieving a part by `?partNumber=N` remains unimplemented.
- S3 `CompleteMultipartUpload` validates the parts list before assembling
  anything (#400). A non-final part below S3's 5 MB minimum is now
  `400 EntityTooSmall`, carrying the offending `<PartNumber>`, its `<ETag>`,
  `<ProposedSize>` and `<MinSizeAllowed>`; a part that was never uploaded or whose
  supplied `ETag` does not match the stored one is `400 InvalidPart`; a body naming
  no parts is `400 MalformedXML` rather than `InvalidPart`; and an upload ID
  presented against a different bucket or key is `404 NoSuchUpload`, matching the
  guard `UploadPart` and `AbortMultipartUpload` already applied.
  A consumer computing chunk sizes from a file size can land under 5 MB and have
  every upload in that band fail at Complete on real S3 — after every part has
  been uploaded and paid for. Substrate accepted those uploads, so the only thing
  holding the size floor correct was a unit test asserting the consumer's own
  arithmetic against itself.
  Equally important is what a rejected Complete does *not* do: it writes no
  object, and it leaves the upload open, so `ListMultipartUploads` still reports it
  until `AbortMultipartUpload` ends it. "No orphan upload was left behind after a
  failed Complete" is thereby a property a test can assert by observing the
  emulator, rather than by asserting the consumer called Abort — orphaned parts
  bill indefinitely.
  ETags are compared ignoring surrounding quotes, hex case, and whitespace, since
  clients differ on whether they echo back the quotes S3 sends and a false
  `InvalidPart` would send a consumer hunting a data bug that does not exist.
- S3 conditional requests: `If-Match`, `If-None-Match`, `If-Modified-Since` and
  `If-Unmodified-Since` (#397). All four were previously ignored, which is the worst
  possible outcome for the pattern they exist to serve: a consumer using
  `If-None-Match: *` to claim a lock, elect a leader, or refuse to clobber a
  checkpoint got a `200` every time, so its lost-race branch was unreachable and its
  tests passed while proving the opposite of the intended invariant.
  - **Writes.** `PutObject`, `CopyObject` and `CompleteMultipartUpload` evaluate
    `If-None-Match` and `If-Match` against the current version of the destination
    key. `If-None-Match: *` is `412 PreconditionFailed` when the key exists;
    `If-Match` is `412` on an ETag mismatch and `404 NoSuchKey` when the key is
    absent — not a `412`, since there is no ETag to compare. A delete marker counts
    as absent for both, per the conditional-writes reference. An `If-None-Match`
    value other than `*` is rejected rather than ignored, so a header sent to
    prevent an overwrite can never silently permit one.
    A rejected write is a no-op: the stored object is byte-identical afterwards, and
    a rejected `CompleteMultipartUpload` leaves its upload open to be aborted.
  - **Exactly one winner.** N concurrent `If-None-Match: *` PUTs to one key produce
    exactly one `200` and N-1 `412`s, as do N concurrent `If-Match` PUTs asserting
    the same ETag. `StateManager` has no compare-and-swap and `MemoryStateManager`
    is last-write-wins, so this required a per-key mutex held across the existence
    check and the write; without it 23 of 32 concurrent writers won. The guarantee
    is **process-local** — airtight for substrate's single-process topology, but it
    would not hold across two emulator processes sharing one state backend, and that
    limit is documented rather than papered over.
  - **Reads.** `GetObject` and `HeadObject` evaluate all four preconditions
    *before* the `Range` step, so a failed precondition is reported rather than a
    partial response served from an entity the caller did not ask for. A matching
    `If-None-Match` is `304 Not Modified` with no body and the `ETag` echoed; a
    failed `If-Match` or `If-Unmodified-Since` is `412`. Both combination rules
    from the `GetObject` reference are implemented: `If-Match` true with
    `If-Unmodified-Since` false is `200`, and `If-None-Match` false with
    `If-Modified-Since` true is `304`.
  - **Copies.** `CopyObject` additionally honors the four `x-amz-copy-source-if-*`
    headers, which gate reading the source rather than overwriting the destination;
    both sets may appear on one request and both are checked before anything is
    written. A failed copy-source condition is always `412`, including where the
    equivalent GET would be `304`.
  - ETag comparison ignores quoting, `W/` prefixes, hex case and whitespace, and a
    comma-separated list matches if any member does. An unparseable date makes its
    condition inapplicable rather than failed (per RFC 9110), so a client with a
    broken date formatter never sees a spurious `412`; all three date formats RFC
    9110 requires a recipient to accept are parsed. An empty header value is a
    condition that cannot be met, distinct from an absent header.
  - Not emulated, deliberately: the `409 ConditionalRequestConflict` and the
    concurrent-delete `404` that real S3 can return, both of which are races against
    wall-clock timing rather than states a deterministic emulator can reach.
- S3 storage classes, `InvalidObjectState`, and `CopyObject` metadata directives
  (#398). `x-amz-storage-class` had been discarded on every write and reported
  nowhere, so a consumer implementing lifecycle transitions could not observe the
  one thing a transition changes — and the classes whose objects real S3 refuses to
  serve looked exactly like the ones it serves instantly.
  - **Recorded and reported.** `PutObject`, `CopyObject` and
    `CreateMultipartUpload` accept `x-amz-storage-class`, defaulting to `STANDARD`
    when absent. All thirteen documented values are accepted, including the
    Outposts, Snow, Express One Zone and FSx tiers — rejecting a value real S3
    takes is the same class of defect as accepting one it rejects. Anything else is
    `400 InvalidStorageClass`, raised before the write, so a rejected class leaves
    no object behind. The class is reported as `x-amz-storage-class` on
    `GetObject`/`HeadObject` — **omitted for `STANDARD`**, per the response-header
    reference — and as `<StorageClass>` in `ListObjects`, `ListObjectsV2`,
    `ListObjectVersions` and `ListMultipartUploads`, where unlike the header it is
    emitted for every class including `STANDARD`. A `<DeleteMarker>` carries none,
    matching S3's response shape. A class set at `CreateMultipartUpload` survives
    to the object `CompleteMultipartUpload` assembles.
  - **Archived objects are unreadable.** `GetObject` of a `GLACIER` or
    `DEEP_ARCHIVE` object is `403 InvalidObjectState`, as is a `CopyObject` naming
    one as its source. The check precedes the `Range` step, so a ranged read of an
    archived object is the same `403` rather than a `206`. `GLACIER_IR` reads
    normally: it is the instant-retrieval tier, and conflating it with the archival
    classes would make a consumer's restore path fire where real S3 never would.
  - **`HeadObject` of an archived object is `200`, not `403`** — a deliberate
    departure from the issue as filed. The `HeadObject` reference documents no
    `InvalidObjectState` and states that "even if the object is stored in S3
    Glacier, all object metadata is still available", which is what makes `HEAD`
    the way a consumer discovers that a `GET` needs a restore first. `GetObject`'s
    reference does list the error. Emulating a `403` on `HEAD` would have made a
    test pass against substrate and fail against AWS.
  - **`CopyObject` metadata directives.** `x-amz-metadata-directive` and
    `x-amz-tagging-directive` are both honored, both defaulting to `COPY`, and an
    unrecognized value on either is `400 InvalidArgument` rather than a silent fall
    back to the default. Under `COPY` the destination takes the source's
    `Content-Type`, `Content-Encoding`, user metadata and tags, and headers
    restated on the request are ignored; under `REPLACE` it takes only what the
    request supplies and drops the rest. `Content-Encoding` had previously been
    dropped on every copy regardless of directive, so a copy of a gzipped object
    produced one a client would hand to the application still compressed.
  - **The storage class is never inherited from the source**, per "if the
    `x-amz-storage-class` header is not used, the copied object will be stored in
    the `STANDARD` Storage Class by default" — so an unqualified copy of a
    `STANDARD_IA` object yields a `STANDARD` one. This is what makes an in-place
    `CopyObject` with a new class the tier-transition mechanism, and also the trap:
    a transition using `REPLACE` must restate the metadata it means to keep.
  - `RestoreObject`, the `x-amz-restore` header and Intelligent-Tiering archive
    access tiers are not implemented, so an archived object stays unreadable for
    the run; restoring is modeled by copying to a non-archival class.
- S3 additional checksums: the `x-amz-checksum-*` family is now computed and
  **verified** on `PutObject`, `UploadPart`, `CopyObject` and
  `CompleteMultipartUpload` (#399). Checksum headers were previously ignored
  altogether, which inverts the guarantee they exist to provide: a consumer
  computing a checksum client-side and sending it to have S3 reject corrupt data
  got a `200` whether the value matched the body or not, so a test asserting "a
  corrupted upload is rejected" passed against substrate and would have passed
  against a version of the consumer that computed the checksum wrong.
  - **A mismatch is `400 BadDigest` and nothing is written.** The object does not
    appear at the key — neither its metadata nor its body — and a rejected write
    over an existing object leaves the original bytes, ETag and checksum intact
    rather than truncating them. A rejected `UploadPart` stores no part, so a
    later `CompleteMultipartUpload` cannot pick up a part that failed its
    checksum. The error body names the algorithm alongside both the supplied and
    the computed value, so a failing test says which value was wrong.
  - **Seven algorithms are verified**: `CRC32`, `CRC32C`, `CRC64NVME`, `SHA1`,
    `SHA256`, `SHA512` and `MD5`. `CRC-64/NVME` is built from the bit-reversed
    catalog polynomial and pinned by a test against the published check value, so
    a transcription error surfaces as a failing assertion rather than as a
    plausible-looking wrong digest that agrees with itself.
  - **`XXHASH64`, `XXHASH3` and `XXHASH128` are `501 NotImplemented`**, not
    silently accepted. Substrate has no implementation to check a caller's value
    against, and storing an unverified checksum is the precise defect this change
    removes — it would make a test pass on data real S3 would have rejected. A
    name outside all ten documented algorithms is `400 InvalidRequest`.
  - **Trailing checksums are read.** `decodeAWSChunked` previously discarded
    everything after the `aws-chunked` completion chunk, which is exactly where
    every AWS SDK puts the checksum of a streamed upload — so header-only
    verification would have silently never fired for any real SDK write. Trailers
    are now parsed and honored when `x-amz-trailer` declares them; a trailer whose
    name differs from what was declared, or a declared trailer that never arrives,
    is `400 MalformedTrailerError`.
  - **Reading a checksum back requires `x-amz-checksum-mode: ENABLED`** on
    `GetObject` or `HeadObject`. Without it neither the `x-amz-checksum-*` header
    nor `x-amz-checksum-type` is present, so the absence is observable. A ranged
    `GET` returns the whole object's checksum, not the range's.
  - **Multipart checksums distinguish `COMPOSITE` from `FULL_OBJECT`.**
    `CreateMultipartUpload` takes `x-amz-checksum-algorithm` and an optional
    `x-amz-checksum-type`, echoing both back; an absent type defaults to
    `COMPOSITE`, except for `CRC64NVME`, which has no composite form and defaults
    to `FULL_OBJECT`. An unsupported algorithm/type pairing is rejected at
    *creation*, before any part is uploaded, rather than after a consumer has paid
    to upload every part. `CompleteMultipartUpload` returns the value as an XML
    element (`<ChecksumSHA256>`, `<ChecksumType>`), not a header, and verifies a
    whole-object checksum supplied on the request itself. A `FULL_OBJECT` multipart
    checksum equals what a single-part `PutObject` of the same bytes produces —
    the invariant a consumer whose two write paths disagree needs a test to catch.
  - **`CopyObject` recomputes** as a direct full-object checksum of the copied
    bytes, under the source's algorithm unless the copy names a new one. Copying a
    `COMPOSITE` multipart object therefore changes both the value and the type
    even though the data is identical, matching S3.
  - **One deliberate divergence:** real S3 attaches a default `CRC64NVME` checksum
    to every object uploaded without one, so a checksum-mode `GET` always returns
    something. Substrate records none. Synthesizing one would make a consumer's
    round-trip assertion pass whether or not their writer actually sends a
    checksum, which is the failure this work exists to expose; an absent checksum
    in substrate means "your writer sent none".

### Fixed
- The `test/e2e` module's recorded indirect dependency versions are back in sync
  with the root module, so `cd test/e2e && go test ./...` runs as documented
  rather than stopping at `go: updates to go.mod needed`. The e2e module consumes
  the root through `replace github.com/scttfrdmn/substrate => ../../`, so bumping
  a root dependency makes its `go.mod` stale too — and the CI job runs
  `go mod tidy` immediately before the tests, repairing the inconsistency
  in-flight and reporting green while the committed file stayed stale. The gap was
  only visible to someone running the suite locally. That step now *verifies*
  tidiness and fails with the command to run, so the same drift cannot pass CI
  again.
- EC2 `Describe*` calls that name a resource ID explicitly now raise
  `Invalid<Type>.NotFound` when the ID resolves to nothing, and
  `Invalid<Type>.Malformed` when it is syntactically invalid, instead of
  returning `200` with an empty list (#391). Covers `DescribeInstances`,
  `DescribeInstanceStatus`, `DescribeVpcs`, `DescribeSubnets`,
  `DescribeSecurityGroups`, `DescribeInternetGateways`, `DescribeRouteTables`,
  `DescribeSnapshots`, `DescribeAddresses` and `DescribeNatGateways`, plus the
  mutating operations that took an ID and silently succeeded on a missing one:
  `TerminateInstances`, `StopInstances`, `StartInstances`, `DeleteVpc`,
  `DeleteSubnet`, `DeleteSecurityGroup`, `DeleteInternetGateway` and
  `DeleteRouteTable`.
  This is the "absent vs. filtered" distinction: `DescribeVpcs()` with no
  arguments legitimately returns `[]`, but naming a VPC ID is an assertion that
  the ID exists and EC2 answers it with an error. A consumer whose entire
  network-precondition check is a `try/except ClientError` on
  `InvalidVpcID.NotFound` had that branch made unreachable — the call succeeded,
  the guard was skipped, and a test asserting "a deleted VPC is reported clearly"
  passed while verifying nothing. Status-polling loops reading
  `Reservations[0]["Instances"][0]` got an `IndexError` in place of the
  `InvalidInstanceID.NotFound` they handle.
  AWS's casing is inconsistent across these codes and SDK callers match the
  literal string, so each pair is spelled out verbatim from the EC2 error
  reference rather than derived: `InvalidVpcID.NotFound` but
  `InvalidGroupId.Malformed`; `InvalidGroup.NotFound` with no `Id` at all;
  `InvalidSnapshot.NotFound` beside `InvalidSnapshotID.Malformed`. EC2 publishes
  no `Malformed` variant for allocation IDs or NAT gateway IDs, so a malformed ID
  for those surfaces as the `NotFound` code.
  Three orderings match EC2 and are covered by tests: syntax is validated before
  any lookup, so a request naming both a malformed and an absent ID reports
  `Malformed`; one present plus one absent ID fails the whole call rather than
  returning the partial set; and an ID excluded by a `Filter` rather than by
  absence still counts as resolved, so an existing ID plus a non-matching filter
  returns 200 and an empty set. ID syntax deliberately does not check length —
  substrate's generators emit 16 hex characters where AWS emits 8 or 17, and AWS
  still accepts the legacy 8-character form for several resources.
- Lambda `Invoke` omits `X-Amz-Function-Error` on a successful invocation, and
  returns `X-Amz-Log-Result` only when the caller asked for it with
  `X-Amz-Log-Type: Tail` (#393). Both were sent unconditionally, the former with an
  empty value. AWS documents `FunctionError` as "if present, indicates that an
  error occurred", so the SDK maps its absence to the key being absent — meaning
  the natural `if "FunctionError" in response` check inverted against substrate and
  every successful invocation looked like a failure. `LogType`'s valid values are
  `None` and `Tail`, and `None` now behaves like absence; the value is matched
  case-insensitively, since Go canonicalizes header names but not values.
  Fixing this exposed a second gap: the executor discarded the runtime's response
  headers, so `X-Amz-Function-Error` — the *only* signal a handler raised, because
  the runtime answers 200 either way — could never be set by any path. It is now
  propagated. Substrate still does not execute handler code, per its scope
  boundary, so a failure is reached by seeding (below) and the status stays 200:
  per the `Invoke` reference, "the status code in the API response doesn't reflect
  function errors".
- Error responses are serialized in the wire format the target service's protocol
  actually uses, so an SDK can recover the error code (#392). Reported as four
  per-service typos; three were one defect in the server's shared error writer.
  Substrate emitted a `Code` member for every JSON service, but botocore's JSON-RPC
  parser reads `__type` and falls back to the *stringified HTTP status* — it never
  reads `Code`. So SSM's already-correct `ParameterNotFound` was discarded and the
  caller saw `Error.Code == "404"`, silently defeating every
  `except ClientError` branch that compares against a symbolic code. This affected
  all ~30 JSON-RPC services, not just the one reported.
  Lambda was a second, worse case: it is REST-JSON but sends
  `Content-Type: application/json`, which failed the `application/x-amz-json`
  prefix test, so its errors went out as *XML* — a shape its SDK cannot parse at
  all. The protocol is now selected by service name, since it cannot be sniffed
  from the request: IAM sends `x-amz-json-1.1` inbound yet answers errors in XML,
  and Lambda's inbound `application/json` is indistinguishable from a plain JSON
  body. JSON-RPC errors carry `__type`, REST-JSON errors carry the
  `x-amzn-errortype` header, and query/REST-XML services keep their
  `<ErrorResponse><Error><Code>` document; each service's classification is taken
  from the `protocol` field of its model in botocore, the same value that selects
  the SDK's parser.
- IAM error codes now use their documented wire spelling, which drops the
  `Exception` suffix the API model's shape names carry (#392): `NoSuchEntity`,
  `EntityAlreadyExists`, `MalformedPolicyDocument`, `DeleteConflict` and
  `LimitExceeded`. A caller matching the documented `NoSuchEntity` never matched.
  The suffix is not a blanket rule — `ValidationError` and `AccessDeniedException`
  were already correct and are unchanged, and Lambda genuinely does use
  `ResourceNotFoundException` on the wire, so neither service's spelling can be
  inferred from the other.
  All 25 renamed branches are now asserted by name, across tagging, inline
  policies, permissions boundaries and instance profiles. Only three had a test
  before, and a rename with no test asserting the new string is the same silent
  failure this fix addresses: the response still looks like an error, so a test
  that checks only the status stays green while the SDK's typed exception never
  fires.
  A state-store failure during S3's bucket lookup is also asserted to surface as
  a server error rather than as `NoSuchBucket`. The two are opposite signals —
  `NoSuchBucket` tells a caller to stop retrying, a store failure is transient —
  so collapsing one into the other would send a consumer down a
  permanent-failure path over a blip.
- S3 `GetObject` and `HeadObject` report `NoSuchBucket` when the bucket does not
  exist, rather than `NoSuchKey` (#392). Both went straight to the object lookup,
  which conflated two distinct conditions: a caller could not tell "someone
  deleted my bucket" from "this object isn't written yet", and so could not tell a
  fatal misconfiguration from an ordinary cache miss.
- The service reference listed ACM's protocol as Query and RAM's as JSON; both
  were wrong in the opposite direction (ACM is JSON-RPC, RAM is REST-JSON).
- The region parsed from a request's `Host` header is now validated against the
  shape of a region code, and `api.<service>.<region>` hosts are understood
  (#403). `api.pricing.us-east-1.amazonaws.com` had yielded `pricing` — the
  second label of a three-label host, returned by the `<service>.<region>`
  assumption the parser fell through to. The deeper problem was that the
  function failed *open*: any host layout it did not recognize still produced a
  confident answer, so a caller could not distinguish a service name
  masquerading as a region from a real one. An unrecognized layout now yields
  no region, which routes the request through the fallbacks that already
  existed — the SigV4 credential scope, then the default region.
- The *service* parsed from a `Host` header now handles the `api.<service>.<region>`
  layout too (#401). `api.pricing.us-east-1.amazonaws.com` had resolved to a
  service literally named `api`, so the request could not route to any plugin; the
  same was true of `api.ecr.*` and `api.sagemaker.*`, which had been reachable
  only via their `X-Amz-Target` namespace.
- S3 multipart operations now return S3's documented `NoSuchUpload` and
  `MalformedXML` message text rather than abbreviated paraphrases (#400).
- S3 `HeadObject` now reports `x-amz-version-id` (#396). `GetObject` always did,
  so a caller could not learn which version it had just described from a HEAD
  alone — it had to issue a GET and download the body to find out.
- S3 `GetObject` and `HeadObject` now advertise `Accept-Ranges: bytes`, as real
  S3 does on both (#396).
- S3 `CopyObject` no longer discards the source object's `Content-Encoding` and
  object tags (#398). Both are user-controlled metadata that a default (`COPY`)
  copy preserves on real S3, so a copy of a gzipped object produced one whose body
  a client would hand to the application still compressed, and a copy silently lost
  the tags any cost-allocation or lifecycle rule keyed on. The destination's
  metadata map is also no longer aliased to the source's, so retagging one object
  no longer mutates the other.

### Changed
- The S3 error-response builder can now carry error-specific child elements and
  response headers (#396), which the `InvalidRange` body needs and which
  `EntityTooSmall` (#400) and the `CopyObject` directive errors (#398) now use. Its
  `extras ...string` parameter had been an unused stub with no call sites; it is
  replaced by an explicit `s3Error` options struct, and `s3DeleteMarkerResponse`
  now shares the same builder instead of re-declaring its own XML type.
- The GET/HEAD object response headers are built in one place, and
  `resolveTaskCompletion` returns the object and body it synthesized rather than
  a finished response (#396). The header block had been duplicated three times
  and had already drifted — the missing `x-amz-version-id` above was a
  consequence — and the early return meant a synthesized completion record
  bypassed the shared response path entirely.

### Security
- Bumped the indirect dependency `golang.org/x/text` from v0.38.0 to v0.39.0 in
  both the root and `test/e2e` modules to clear CVE-2026-56852 (#394), a HIGH
  advisory where `unicode/norm.Iter` can enter an infinite loop on certain input.
  `govulncheck` reports the vulnerable symbol as unreachable from substrate, so
  this closes a dependency-graph finding rather than an exploitable path, but it
  had turned the Trivy and container-image scans red on every pull request.

## [v0.76.0] - 2026-07-26

A documentation-reliability release driven by an external project review: the
public docs and service list are now generated from or verified against the
implementation, validated in CI, and broadened beyond the Go audience.

### Added
- Service reference is now generated from the plugin registry (#364).
  `cmd/gen-service-reference` (via `go generate ./...` or `make docs-reference`)
  rewrites the coverage matrix in `docs/services.md` between marker comments,
  listing every registered plugin so the documented count can no longer drift
  from the implementation (root cause of the 63-vs-37 contradiction). A
  `make docs-reference-check` CI job fails the build if the matrix is stale or a
  newly registered plugin has no documentation metadata; hand-written per-service
  detail is preserved.
- Documentation is validated on pull requests (#365): a `Docs CI` workflow runs
  the VitePress production build, a Markdown link check, and a stale-pinned-version
  scan (`scripts/check-doc-versions.sh` / `make docs-versions`) on any PR touching
  `docs/` or the README — the deploy workflow only ran on `main`, too late to
  catch drift.
- Python `pytest-substrate` is exercised in primary CI and surfaced in the docs
  (#366): a CI job builds the binary and runs `ruff`, `pytest`, and a wheel build;
  Getting Started gained a "First Python test" section and the README/homepage
  list pytest as a third usage mode. Added an explicit `[tool.ruff]` config.
- Runnable "journey" examples for the core differentiators, exercised in CI via
  the `test/e2e` module (#367): seeded throttling → SDK retry, record & replay, a
  cost-budget gate, and a time-travel lifecycle test (documented in
  `examples/README.md`). Terraform/CDK/pytest journeys are tracked as a follow-up
  (#380).
- Contributor guide (`docs/contributing.md`) and a Compatibility & Fidelity
  policy (`docs/fidelity.md`) defining the Implemented/Partial/Fault-aware/
  Stateful/CFN/Pricing levels and how fidelity is decided (#368).
- `TestServer` now exposes `Store() *EventStore`, `StateManager()`,
  `TimeController()`, and `Registry()` accessors; `StartTestServer` enables the
  in-memory event store and wires a `FaultController` (disabled; rules seedable
  via `POST /v1/fault/rules`) and a `CostController`, so cost summaries,
  recording/replay, and fault injection work against the test server out of the
  box (#363, #367).

### Changed
- Brought the Getting Started and Testing guides back in sync with the
  implementation (#363): corrected the documented `TestServer` API (the cost and
  recording/replay examples referenced accessors that did not exist and queried a
  detached event store), replaced pinned stale `v0.68.0` version strings with
  neutral placeholders, fixed a duplicated `[profile substrate]` block and a
  README typo. Documented Go snippets now compile against `package emulator`.
- Docs site organised around **Learn / Start / Test / Reference / Contributing**
  (#370); dated the LocalStack/moto/AWS comparison tables ("as of early 2026");
  clarified that Substrate is equally useful for human-written and AI-generated
  IaC.
- Refreshed `SECURITY.md` (#369): private vulnerability reporting is the
  documented preferred path, added acknowledgement/assessment/fix timelines, a
  supported-versions table with evergreen remediation guidance, and a release
  integrity/verification section. The existing tag-integrity advisories are
  unchanged.

## [v0.75.0] - 2026-07-26

### Added
- S3: seedable spore.host `spawn task run` task-completion outcomes (#360). A GET
  of `tasks/<task_id>/completion.json` against a bucket now resolves a seedable,
  clock-aware completion record — `task_id`, `exit_code`, `state`
  (`completed`/`failed`), `started_at`, `ended_at` — matching
  `taskproto.CompletionRecord`. New control-plane endpoints `POST` / `DELETE
  /v1/spawn/task-completion` seed by task id (`DELETE ?taskId=...` clears one,
  no query clears all). Before a seed's `ended_at` (on the simulated clock) the
  GET returns `NoSuchKey` ("still running"); at/after it the record is served —
  so a consumer's real poll loop is exercised deterministically. Absent a seed, a
  matching key resolves to the nominal `exit_code:0 / completed` success record;
  a real staged object at the key (the interim `aws s3 cp` path) always wins, and
  a GET of any non-`tasks/*/completion.json` key stays a normal 404. Substrate
  does not execute the task — this is the seedable completion observation only,
  mirroring the SSM (#345) / SageMaker / Bedrock seed pattern.

## [v0.74.0] - 2026-07-22

### Security
- Bumped `google.golang.org/grpc` v1.81.1 → v1.82.1 in both the root and
  `test/e2e` modules to clear a Trivy-flagged HIGH advisory (GHSA-hrxh-6v49-42gf,
  gRPC-Go xDS RBAC / HTTP/2). The dependency is indirect and the advisory is not
  call-reachable (`govulncheck` was already clean), but the bump keeps the
  manifest-based Trivy scan green.

### Added
- SSM: seedable `SendCommand` / `GetCommandInvocation` outcomes (#345). Substrate
  still does not execute the shell command (that is workload-internal and out of
  scope), but a test can now seed the **observable result** — `Status`, stdout,
  stderr, and `ResponseCode` (exit code) — that `GetCommandInvocation` returns,
  instead of always getting `Success` with empty output. New control-plane
  endpoints `POST` / `DELETE /v1/ssm/command-invocation` seed by document name
  (default `*` wildcard) with an optional command-parameter substring match
  (`paramMatch`), following the Athena/SageMaker/Bedrock seed pattern. Unseeded
  commands keep the nominal `Success`/empty result. `GetCommandInvocation` now
  also returns `ResponseCode`.

## [v0.73.0] - 2026-07-20

### Fixed
- EC2: `RunInstances` now returns the instance `tagSet` for launch-time tags
  applied via `TagSpecifications` (#351), matching real EC2. Previously the tags
  were stored (and appeared in a later `DescribeInstances`) but omitted from the
  `RunInstances` response, so SDK callers reading the returned instance's tags got
  an empty set. (`StartInstances`/`StopInstances` responses correctly omit
  `tagSet`, matching AWS — no change there.)

### Added
- EC2: cluster placement group actions (#344). `CreatePlacementGroup` (accepts
  `GroupName` + `Strategy`, defaults `cluster`; duplicate name →
  `InvalidPlacementGroup.Duplicate`), `DescribePlacementGroups` (with `GroupName.N`
  filter, `State=available`), and `DeletePlacementGroup` (unknown name →
  `InvalidPlacementGroup.Unknown`). `RunInstances` now validates a
  `Placement.GroupName` against known groups (unknown → `InvalidPlacementGroup.Unknown`),
  so a create → poll(available) → launch ordering (e.g. spawn's per-AZ MPI launch)
  is testable end-to-end.

## [v0.72.0] - 2026-07-20

### Added
- CORS support for browser-based AWS SDK clients (#346). The HTTP server can now
  emit `Access-Control-Allow-*` headers and answer `OPTIONS` preflight requests,
  so a page served from another origin (e.g. a Vite dev server) can drive the
  emulator. Off by default; enable via `server.cors.enabled` (with optional
  `server.cors.allowed_origins`, default reflect-any `*`). Exposes
  `x-amzn-RequestId` / `x-amz-request-id` / `x-amzn-ErrorType` / `x-amz-id-2` so
  the SDK can read request IDs and (seeded) error types from the browser.

### Security
- Bumped the Go toolchain to **go1.26.5** (`go.mod`, `test/e2e/go.mod`) to clear
  a call-reachable `crypto/tls` standard-library advisory (GO-2026-5856); CI
  reads the toolchain from `go.mod`, so `govulncheck` passes again.
- Bumped `golang.org/x/net` v0.52.0 → v0.56.0 (with `x/sys`, `x/text`) in both the
  root and `test/e2e` modules to clear Trivy-flagged advisories (CVE-2026-27136,
  -33814, -39821, -42502). The dependency is indirect and the CVEs are not
  call-reachable (`govulncheck` was already clean), but the bump keeps the
  dependency tree clean for the manifest-based Trivy scan.

### Changed
- CI: pinned `aquasecurity/trivy-action` from `@master` to `@v0.36.0`. The
  floating `master` ref regressed upstream and broke the Trivy/Container-scan jobs
  on every PR; pinning restores reproducible CI (and the new `github-actions`
  Dependabot config will surface future bumps).

### Added
- Enabled Dependabot: vulnerability alerts + automated security fixes (repo
  settings), plus a `.github/dependabot.yml` for weekly grouped version-update
  PRs across gomod (root + `test/e2e`), npm (`docs`), pip (`python`), and
  github-actions.

## [v0.71.0] - 2026-06-15

### Fixed
- EC2: `DescribeInstances` now echoes the `IamInstanceProfile` set at launch
  (#331). `RunInstances` already accepted and stored it, but the describe response
  dropped it; the response now includes `iamInstanceProfile` with `arn` and `id`
  so callers can read back the profile they attached.

### Changed
- SSM: `DescribeInstanceInformation` now lists a running instance only if it has
  an IAM instance profile attached (#331). Previously every running instance
  reported `PingStatus=Online` regardless of preconditions; in real AWS, SSM
  registration requires a profile granting `ssm:UpdateInstanceInformation`, and an
  instance with no profile never registers. This lets callers distinguish a "dead"
  instance (no profile → can never register) from an SSM-managed one. Eligibility
  is based on profile presence (substrate does not model profile→policy attachment).

## [v0.70.0] - 2026-06-09

### Added
- EC2: `RegisterImage` now registers an AMI that can point its root device at an
  existing EBS snapshot via `BlockDeviceMapping.N.Ebs.SnapshotId` (#328, follow-up
  to #322). Unlike `CreateImage` (which always materializes a fresh snapshot, as
  in real AWS), `RegisterImage` lets multiple AMIs *share* one snapshot, so the
  `DescribeImages` `block-device-mapping.snapshot-id` filter returns every AMI
  referencing it — enabling the "retain a shared snapshot" path to be tested
  end-to-end. (The filter itself already returned all matches; the gap was that
  distinct `CreateImage` calls never produced a shared snapshot to find.)

## [v0.69.0] - 2026-06-09

### Added
- EC2: `DescribeSnapshots` now lists EBS snapshots (returning `SnapshotId`,
  `VolumeSize`, `State`, `StartTime`, `Encrypted`), honoring `SnapshotId.N`
  parameters and the `snapshot-id` filter. `CreateImage` now materializes a
  backing snapshot and records it on the AMI, and `DescribeImages` surfaces it in
  the response `blockDeviceMapping` and honors the
  `block-device-mapping.snapshot-id` (and `image-id`) filters. `DeleteSnapshot`
  now removes the snapshot from state (was a no-op stub) so a subsequent
  `DescribeSnapshots` reflects the deletion. Together these let snapshot-retention
  logic — retain a snapshot shared by multiple AMIs, delete an unshared one — be
  tested end-to-end (#322).

## [v0.68.3] - 2026-06-09

### Fixed
- S3: `PutObject` (and `UploadPart`) now decode SigV4 streaming (`aws-chunked`)
  request bodies before storing them, instead of persisting the raw encoded
  stream with its `chunk-signature` framing (#321). Affected SDK clients that send
  `Content-Encoding: aws-chunked` / `x-amz-content-sha256: STREAMING-*` (e.g. the
  AWS SDK for .NET with `InputStream`); `GetObject` returned the chunk headers as
  content. Detection is header-based and decoding is a safe no-op for non-chunked
  bodies (the AWS CLI, which uses standard HTTP chunking, was already correct).

## [v0.68.2] - 2026-06-05

### Fixed
- S3: `GetObject` and `HeadObject` now return the correct error for delete markers
  instead of surfacing the marker (#318, follow-up to #316). A plain request whose
  current version is a delete marker returns `404 NoSuchKey`; a request naming the
  delete-marker version returns `405 MethodNotAllowed`. Both responses carry the
  `x-amz-delete-marker: true` header so SDKs can distinguish a deleted object from a
  never-existed key. `HeadObject` is now version-aware and no longer returns `200`
  with marker metadata for a deleted key.

## [v0.68.1] - 2026-06-04

### Fixed
- S3: `ListObjectsV2` (and `ListObjects`) no longer surfaces delete markers as live objects on versioning-enabled buckets. `DeleteObjects` correctly inserted delete markers but `loadObjectEntry` did not filter them, causing `aws s3 ls` to show objects that `GetObject` correctly returned 404 for (#316).

## [v0.68.0] - 2026-06-02

This release intentionally skips `v0.67.0` (a void, out-of-order tag — see below
and `SECURITY.md`) and supersedes it as the highest semver tag, so that
`go get github.com/scttfrdmn/substrate@latest` once again resolves to current
code rather than the void tag. No source change versus `v0.66.1`; the entries
below are the released set.

### Security
- **Repository protection rulesets + `v0.67.0` void advisory**: Added GitHub
  rulesets enforcing the release contract server-side — `refs/tags/v*` tags are
  immutable (no move/update/delete) and `main` requires changes via pull request
  (no direct or force pushes), with no bypass actors. Documented `v0.67.0` as a
  void, out-of-order tag (points to an ancestor of `v0.66.0`/`v0.66.1`; created
  by a stray manual tag, not the release process; no GitHub release). It is left
  in place per the tag-immutability rule and **not** re-pointed; the version
  number is burned and the next minor release is **v0.68.0**. Consumers must use
  `v0.66.1` or later. See `SECURITY.md`.

## [v0.66.1] - 2026-06-02

### Added
- **Scope & philosophy documentation**: Documented substrate's defining scope
  boundary — it models what is observable through an AWS API call (request/
  response shapes, resource state and its transitions over the simulated clock,
  and seedable outcomes), not what software inside a resource does. Articulated
  that **seeding** is the mechanism that lets a deterministic emulator produce
  different results (default nominal path; alternate error/capacity/terminal
  outcomes seeded via control-plane endpoints and read at request time), and why
  this determinism is preferable to container- or real-infrastructure-backed
  approaches (reproducibility by construction, exact replay of failures, history
  inspection — at the deliberate cost of workload-internal fidelity). Also
  articulated *why* determinism and replay are useful capabilities (no flakes,
  exact reproduction, time-travel inspection, testable rare paths, fast/free
  runs, regression fixtures). Added as a "Scope" section in `CLAUDE.md` and a
  fourth differentiator plus "What determinism and replay give you", "Seeding",
  and "Why determinism" sections in `doc.go`.

### Fixed
- **`DescribeInstances` multi-filter and `tag:` support (#305)**: `describeInstances`
  only read `Filter.1` and only honoured `instance-state-name`, so any second-or-later
  filter and all `tag:` filters were silently dropped — returning instances the
  caller had explicitly filtered out (e.g. a terminated instance for a
  `running`-only query when a `tag:` filter came first). It now uses the shared
  `extractEC2Filters` helper and AND-combines every filter via the new
  `ec2InstanceMatchesFilters`. Supported keys: `instance-state-name`,
  `instance-state-code`, `instance-id`, `instance-type`, `image-id`, `vpc-id`,
  `subnet-id`, `key-name`, `tag:<key>`, and `tag-key`. Unknown filter keys match
  nothing rather than passing silently. Closes #305.

### Security
- **Go toolchain bumped to 1.26.4 (`go.mod`, `test/e2e/go.mod`)**: pins the
  patched standard library to clear call-reachable stdlib advisories flagged by
  `govulncheck` — GO-2026-5039 (`net/textproto` header escaping, reachable via
  `http.Server.Serve`) and GO-2026-5037 (`crypto/x509` hostname parsing). CI
  reads the toolchain from `go.mod`, so the vulnerability check now passes with
  zero call-reachable vulnerabilities.

## [v0.66.0] - 2026-06-02

### Added
- **CloudFormation drift detection — property-level `MODIFIED` for all
  drift-checkable types (#290)**: Extended `cfnDriftComparators` with comparators
  for DynamoDB (`BillingMode`, `ProvisionedThroughput` read/write capacity),
  Lambda (`Runtime`, `Handler`, `Timeout`, `MemorySize`), IAM Role
  (`AssumeRolePolicyDocument` via the new order-insensitive `policyDocumentsEqual`
  helper, plus `Description`/`Path`), SQS (queue attributes —
  `VisibilityTimeout`, `MessageRetentionPeriod`, `DelaySeconds`,
  `ReceiveMessageWaitTimeSeconds`, `MaximumMessageSize`), and SNS (`DisplayName`).
  To make SQS/SNS comparable, `deploySQSQueue` and `deploySNSTopic` now forward
  declared properties, and SNS `CreateTopic` persists an initial `DisplayName`.
  Comparison is **template-declared-only**: a property is checked solely when the
  template explicitly declares it, so deploy-applied defaults never produce false
  drift (the S3 versioning comparator was aligned to the same rule). This
  completes #290. Also fixed latent state-key bugs in the existing SQS, SNS, and
  Lambda existence drift-checkers (spurious region component / ARN-vs-name) and a
  pre-existing bug where `deployLambdaFunction` sent `Timeout`/`MemorySize` as
  strings, causing `CreateFunction` to reject the request. Closes #290.
- **CloudFormation drift detection — MODIFIED + describe operations (#290)**:
  `DetectStackDrift` now reports `MODIFIED` (in addition to `IN_SYNC`/`DELETED`/
  `NOT_CHECKED`) via a new `cfnDriftComparators` map that performs property-level
  comparison of live state against the template (re-parsed and intrinsic/param-
  resolved at drift-check time). The S3 `VersioningConfiguration` comparator is
  implemented end-to-end (deploy a bucket with versioning, change it outside
  CloudFormation, and drift reports `MODIFIED` with a `PropertyDifferences`
  entry). Property-level drift is bounded by what each plugin persists, so the
  other drift-checkable types (DynamoDB/SQS/SNS/Lambda/IAM) remain existence-only
  for now; the comparator map makes adding more types straightforward. Added
  `StackDeployer.DescribeStackResourceDrifts(stackName, statusFilters)` (returns
  per-resource entries, optionally filtered by drift status) and
  `StartStackDriftDetection` / `DescribeStackDriftDetectionStatus` (the
  `DETECTION_IN_PROGRESS → DETECTION_COMPLETE` lifecycle, with the in-progress
  record persisted before synchronous detection runs). New types
  `CFNPropertyDiff` and `CFNDriftDetectionStatus`; `CFNResourceDriftEntry` gained
  `PropertyDifferences`. (Broadened to all drift-checkable types in the entry
  above.)
- **VPC route table completion (#293)**: Added `ReplaceRoute` (repoints an
  existing route's target; returns `InvalidRoute.NotFound` for an unknown
  destination) and `ReplaceRouteTableAssociation` (repoints a subnet association
  to a different route table, returning a fresh `newAssociationId`; returns
  `InvalidAssociationID.NotFound` for an unknown association). `DescribeRouteTables`
  now honours the `vpc-id`, `association.subnet-id`, and `association.route-table-id`
  filters (via the existing `extractEC2Filters` helper), not just `RouteTableId`.
  `ensureDefaultVPC` now creates and attaches a default internet gateway and adds
  a `0.0.0.0/0 → igw-…` route to the default VPC's main route table (previously
  only the local route was present, despite the code comment). Closes #293.
- **Bedrock control-plane batch inference (#297)**: Added the ModelInvocationJob
  APIs to `BedrockRuntimePlugin` (the control plane shares the `bedrock` SigV4
  signing name with the data plane and is distinguished by request path, so no
  parser change was needed). `CreateModelInvocationJob` (`POST
  /model-invocation-job`) stores a job in the `Submitted` state and returns its
  `jobArn`; `GetModelInvocationJob` (`GET /model-invocation-job/{jobId}`) returns
  the job; `StopModelInvocationJob` (`POST /model-invocation-job/{jobId}/stop`)
  transitions it to `Stopped`; `ListModelInvocationJobs` (`GET
  /model-invocation-jobs`) returns summaries. New control-plane endpoints `POST`
  and `DELETE /v1/bedrock/model-invocation-job-status` seed and clear a status
  override (keyed by job ID or `"*"`), letting tests drive a job through
  `InProgress`/`Completed`/`Failed` without simulated time. Cost entry:
  `bedrock-runtime/CreateModelInvocationJob: $0.000015`. Closes #297.
- **DynamoDB resource tagging (#298)**: `CreateTable` now stores the `Tags` list
  supplied at creation time, and the plugin implements `TagResource`,
  `UntagResource`, and `ListTagsOfResource`. Tags are stored on the table record
  and keyed by the table ARN (`arn:aws:dynamodb:{region}:{acct}:table/{name}`,
  parsed via the new `dynamodbTableNameFromARN` helper, which tolerates
  `/index/...` and `/stream/...` suffixes). `ListTagsOfResource` returns tags
  sorted by key for deterministic output; tagging a non-existent table returns
  `ResourceNotFoundException`. This lets tag-based ownership logic (e.g. a tool
  tagging its own tables `lagotto:managed=cli`) be tested end-to-end. Closes #298.
- **SageMaker training-job CapacityError seeding (#299)**: `DescribeTrainingJob`
  now honours a seeded terminal status and `FailureReason`, so a job can be made
  to report `Failed` with a `CapacityError` reason for capacity-retry testing
  (the default unseeded behaviour remains `Completed`). New control-plane
  endpoints `POST` and `DELETE /v1/sagemaker/training-job-status` seed and clear
  the override, keyed by training job name or `"*"` (mirroring the
  Athena/RedshiftData/Timestream/Bedrock seeding pattern). Added `FailureReason`
  to `SageMakerTrainingJob`. Closes #299.

### Security
- **Module tag integrity advisory (#296)**: Documented that tags **v0.45.1** and
  **v0.45.2** are poisoned — both were re-cut after publication, so their current
  GitHub content no longer matches the hash recorded in Go's immutable checksum
  database (`sum.golang.org`), breaking `go.sum` verification for every consumer.
  Verified the blast radius is limited to these two tags (v0.45.0 and v0.45.3+
  are clean). Consumers must upgrade to v0.45.3 or later and regenerate `go.sum`.
  The tags will not be re-cut. See `SECURITY.md` for full details and hashes.
  Added the release-process rule that published tags are never moved (a release
  mistake is fixed by a new patch version). Closes #296.

## [v0.65.0] - 2026-04-10

### Added
- **CLI enhancements (#295)**: Added 4 new subcommands to the `substrate` CLI. `substrate status` connects to a running server and displays version, plugin count, simulated time, request count, and total cost. `substrate inspect [service]` lists registered services or shows recent events for a specific service. `substrate pricing` manages pricing configuration with sub-subcommands `info` (show source/cache age), `refresh` (fetch from AWS), and `lookup --service=s3 --operation=PutObject` (price query). `substrate reset` resets all server state via `POST /v1/state/reset`. All subcommands accept `--address` flag (default `http://localhost:4566`). Closes #295.

## [v0.64.0] - 2026-04-10

### Added
- **Deepen stub services (#294)**:
  - **CloudFront**: Invalidations are now persisted in state. Added `GetInvalidation` (GET `/distribution/{id}/invalidation/{invId}`) and `ListInvalidations` (GET `/distribution/{id}/invalidation`) operations. Invalidation IDs and status are retrievable after creation.
  - **Health**: Added `state` field to `HealthPlugin` and `HealthEvent` type with seedable events via control-plane endpoints `POST /v1/health/events` and `DELETE /v1/health/events`. `DescribeEvents` returns seeded events. `DescribeEventDetails` filters by requested ARNs.
  - **Timestream**: `WriteRecords` now stores ingested records at `records:{acct}/{region}/{db}/{table}`. `Query` serves stored records for `SELECT * FROM db.table` syntax (basic SQL parsing), falling back to seeded results then empty. Write→Query roundtrip works without control-plane seeding.

## [v0.63.0] - 2026-04-10

### Added
- **EC2 security group enforcement (#292)**: `RunInstances` now validates that specified security groups exist and belong to the target VPC, returning `InvalidGroup.NotFound` on mismatch. `DescribeSecurityGroups` response expanded to include full `ipPermissions` and `ipPermissionsEgress` sets with protocol, port range, and CIDR details. Added exported `SecurityGroupAllowed(rules, protocol, port, sourceCIDR)` function for evaluating whether traffic is permitted by a set of security group rules — supports protocol matching (`-1` = all), port range checking, and CIDR containment via `net.ParseCIDR`. Closes #292.
- **VPC route table improvements (#293)**: `DescribeRouteTables` response expanded to include full `routeSet` (destination CIDR, gateway ID, state) and `associationSet` (association ID, subnet ID, main flag). `CreateVpc` and `ensureDefaultVPC` now auto-create a main route table with a local route for the VPC CIDR. Closes #293.

## [v0.62.0] - 2026-04-10

### Added
- **Step Functions ASL depth (#291)**: Added `Parameters` support — transforms state input using JSONPath `.$` references and static values before execution. Added `ResultSelector` support — filters task output before `ResultPath` merging. Added `TimeoutSeconds` enforcement for Task states — raises `States.Timeout` when exceeded, caught by `Catch` blocks. Added 6 new Choice operators: `StringGreaterThanOrEquals`, `StringLessThanOrEquals`, `NumericGreaterThanOrEquals`, `NumericLessThanOrEquals`, `IsNull`, `IsPresent`. Closes #291.

## [v0.61.0] - 2026-04-10

### Added
- **CloudFormation change sets (#289)**: Added `CreateChangeSet`, `DescribeChangeSet`, `ExecuteChangeSet`, `ListChangeSets`, and `DeleteChangeSet` to `StackDeployer`. `CreateChangeSet` diffs old vs. new template resources to produce Add/Modify/Remove entries. Type changes result in Remove+Add (replacement). Property changes use `reflect.DeepEqual` comparison. `ExecuteChangeSet` applies changes via `UpdateStack` then deletes the consumed change set. Change sets stored at `changeset:{stackName}/{name}` in the `"cfn"` namespace. Closes #289.
- **CloudFormation drift detection (#290)**: Added `DetectStackDrift` to `StackDeployer`. Checks each deployed resource against its service's plugin state to detect deletions made outside CloudFormation. Supported resource types: S3 Bucket, DynamoDB Table, SQS Queue, SNS Topic, Lambda Function, IAM Role. Returns per-resource drift status (`IN_SYNC`, `DELETED`, `NOT_CHECKED`) and aggregate stack drift status (`IN_SYNC` or `DRIFTED`). Closes #290.

## [v0.60.0] - 2026-04-10

### Added
- **SNS message delivery enhancements (#288)**: SNS `Publish` now wraps messages in the standard SNS notification JSON envelope (Type, MessageId, TopicArn, Subject, Message, Timestamp, Signature) before delivering to SQS subscribers, matching real AWS behavior. Added HTTP/S protocol support — subscriptions with protocol `http` or `https` receive a POST with the SNS envelope body and `x-amz-sns-message-type: Notification` header. Added `FilterPolicy` field to `SNSSubscription` — when set, only messages whose attributes match all policy keys are delivered. Message attributes are parsed from `MessageAttributes.entry.N.*` request params. Subject is now passed through to the envelope and Lambda event records. Closes #288.

## [v0.59.0] - 2026-04-10

### Added
- **Dynamic pricing provider (#285)**: Introduced `PricingProvider` interface with `StaticPricingProvider` (wraps the existing built-in pricing table) and `AWSPricingProvider` (fetches real prices from the AWS Price List Bulk API). AWS pricing data is cached to `~/.substrate/pricing-cache.json` with configurable TTL (default 24h). Startup never blocks on network I/O — uses cache or static fallback. Config: `pricing.provider: "static"|"aws"`, `pricing.cachePath`, `pricing.cacheTTLHours`, `pricing.region`. Control-plane endpoints: `POST /v1/pricing/refresh` (trigger fetch), `GET /v1/pricing` (source/cache info), `GET /v1/pricing/lookup?service=s3&operation=PutObject` (price lookup). Closes #285.
- **Discount and savings plan support (#286)**: Added `DiscountConfig` with `globalDiscountPercent` (e.g., 10% EDP discount) and per-service `ServiceDiscount` (fixed rate override or percentage discount). Discounts are applied in `CostForRequest` after base price lookup: service-specific first, then global. Configurable via YAML (`costs.discounts`) and control-plane endpoints: `POST/GET/DELETE /v1/pricing/discounts`. Discounts survive state reset and support hot-reload via SIGHUP. Closes #286.
- **Credits support (#287)**: Added `Credit` model with ID, description, amount, remaining balance, optional expiry, and optional service scope. Credits are deducted from costs in real-time during `CostForRequestWithCredits`: service-scoped credits consumed first, then global, earliest-expiry first. Control-plane endpoints: `POST /v1/pricing/credits` (add), `GET /v1/pricing/credits` (list), `DELETE /v1/pricing/credits/{id}` (remove). Credits survive state reset. Closes #287.

## [v0.58.2] - 2026-04-09

### Fixed
- **Dependency CVEs (#282)**: Bumped `go.opentelemetry.io/otel/sdk` v1.42.0 → v1.43.0 (CVE-2026-39883, HIGH) and `google.golang.org/grpc` v1.79.2 → v1.80.0 (CVE-2026-33186, CRITICAL). Both root and `test/e2e` modules updated.

### Security
- **Dockerfile non-root user (#283)**: Added dedicated `substrate` user and `USER substrate` directive so the container no longer runs as root. Added `HEALTHCHECK` instruction. Closes #283.
- **K8s deployment hardening (#284)**: Added pod-level and container-level `securityContext` (runAsNonRoot, readOnlyRootFilesystem, allowPrivilegeEscalation=false, drop ALL capabilities, RuntimeDefault seccomp profile). Pinned image tag from `:latest` to `v0.58.1`. Closes #284.
- **Security scanning**: Added `make security` target (govulncheck + trivy + semgrep) and `.github/workflows/security.yml` CI workflow with weekly cron.

## [v0.58.1] - 2026-04-09

### Added
- **Bedrock Runtime InvokeModel (#281)**: Added `InvokeModel` operation to the existing `BedrockRuntimePlugin`, accepting `POST /model/{modelId}/invoke`. Returns a deterministic canned response in Claude Messages API format by default (type `message`, role `assistant`, stubbed text). Control-plane endpoints `POST /v1/bedrock-runtime/responses` and `DELETE /v1/bedrock-runtime/responses` seed and clear custom responses per model ID or `"*"` wildcard, following the Athena/RedshiftData/Timestream pattern. Exact model ID matches take priority over wildcard. Cost entry: `bedrock-runtime/InvokeModel: $0.000015`. Closes #281.

## [v0.58.0] - 2026-04-02

### Added
- **Amazon Timestream plugin (#279)**: Added `TimestreamPlugin` with 12 operations covering both the Timestream Write and Query APIs under service name `"timestream"` (derived from `X-Amz-Target: Timestream_20181101.{Op}`). Write operations: `CreateDatabase`, `DescribeDatabase`, `DeleteDatabase`, `ListDatabases`, `CreateTable`, `DescribeTable`, `DeleteTable`, `ListTables`, `WriteRecords`. `WriteRecords` validates that the target database and table exist and returns `RecordsIngested` counts (records are counted but not stored). Query operations: `DescribeEndpoints` (returns a synthetic regional endpoint), `Query` (returns seeded rows or empty), `CancelQuery` (no-op acknowledgement). Control-plane endpoints `POST /v1/timestream-query/results` and `DELETE /v1/timestream-query/results` seed and clear `Query` responses, keyed by query string or `"*"` wildcard, following the Athena/RedshiftData pattern. Cost entries: `timestream/WriteRecords: $0.0000005`, `timestream/Query: $0.000001`. Closes #279.

## [v0.57.0] - 2026-04-02

### Added
- **AppSync API key operations (#278)**: Added `CreateApiKey` (`POST /v1/apis/{apiId}/ApiKeys`) and `ListApiKeys` (`GET /v1/apis/{apiId}/ApiKeys`) to the existing `AppSyncPlugin`. Both operations validate that the target GraphQL API exists (returning `NotFoundException` HTTP 404 if not). `CreateApiKey` accepts an optional `description` field and sets a one-year expiry from the server's simulated clock. `ListApiKeys` returns all keys scoped to the API. New exported type `AppSyncAPIKey` added. Closes #278.

## [v0.56.1] - 2026-04-02

### Added
- **HTTP API for fault injection rules (#280)**: Added three control-plane endpoints for managing fault injection at runtime. `GET /v1/fault/rules` returns the current `FaultConfig` as JSON. `POST /v1/fault/rules` replaces the active config (decoded from a JSON `FaultConfig` body). `DELETE /v1/fault/rules` disables fault injection and clears all rules. All three endpoints return `501 Not Implemented` when the server was started without a `FaultController`. `handleStateReset` now also clears fault rules so a state reset returns the server to a clean fault-free state. `FaultConfig` and `FaultRule` gained JSON struct tags; `FaultController` gained a `GetConfig() FaultConfig` accessor.

## [v0.56.0] - 2026-04-02

### Added
- **ElastiCache plugin (#277)**: Added `ElastiCachePlugin` with 17 operations covering cache clusters, replication groups, subnet groups, parameter groups, and resource tagging. Uses AWS Query (`Action=`) protocol with XML responses (namespace `https://elasticache.amazonaws.com/doc/2015-02-02/`). Operations: `CreateCacheCluster`, `DescribeCacheClusters` (with `CacheClusterId` filter and `MaxRecords`/`Marker` pagination), `ModifyCacheCluster`, `DeleteCacheCluster`, `CreateReplicationGroup`, `DescribeReplicationGroups`, `ModifyReplicationGroup`, `DeleteReplicationGroup`, `CreateCacheSubnetGroup`, `DescribeCacheSubnetGroups`, `DeleteCacheSubnetGroup`, `CreateCacheParameterGroup`, `DescribeCacheParameterGroups`, `DeleteCacheParameterGroup`, `AddTagsToResource`, `ListTagsForResource`, `RemoveTagsFromResource`. All clusters start with status `available`. Endpoint address uses the real AWS format `{id}.{hash}.cfg.{region}.cache.amazonaws.com:{port}` with a deterministic hash. Cost entries: `elasticache/CreateCacheCluster: $0.017`, `elasticache/CreateReplicationGroup: $0.034`.

## [v0.55.0] - 2026-04-02

### Added
- **MSK plugin completion (#276)**: Added four missing MSK operations to complete the plugin. `CreateClusterV2` (`POST /api/v2/clusters`) accepts the V2 request shape where broker configuration is nested inside a `Provisioned` sub-object and falls back to top-level fields for backward compatibility. `DescribeClusterV2` (`GET /api/v2/clusters/{arn}`) returns the V2 `ClusterInfo` shape including `ClusterType: "PROVISIONED"` and a `Provisioned` sub-object with `BrokerNodeGroupInfo`, `CurrentBrokerSoftwareInfo`, and `NumberOfBrokerNodes`. `ListClustersV2` (`GET /api/v2/clusters`) returns all clusters in the same V2 shape. `ListNodes` (`GET /v1/clusters/{arn}/nodes`) generates one `MSKNodeInfo` per broker node (1…`NumberOfBrokerNodes`), each with a synthetic node ARN, `NodeType: "BROKER"`, `BrokerId`, `ClientSubnet`, and `KafkaVersion`. Exported types `MSKNodeInfo`, `MSKBrokerNodeInfo`, and `MSKBrokerSoftwareInfo` added to `msk_types.go`.

## [v0.54.0] - 2026-04-02

### Added
- **Athena plugin completion (#275)**: Completed the Amazon Athena plugin with 5 new operations and control-plane seeding. Added `ListQueryExecutions` (paginated, with optional `WorkGroup` filter), `CreateWorkGroup`, `GetWorkGroup` (the `"primary"` workgroup auto-exists), `DeleteWorkGroup`, and `ListWorkGroups`. `GetQueryResults` now returns pre-seeded rows via the new `POST /v1/athena/results` control-plane endpoint (SQL-keyed or `"*"` wildcard, same pattern as RedshiftData); `DELETE /v1/athena/results` clears seeded results. `AthenaResultSet`, `AthenaResultRow`, `AthenaValue`, `AthenaColumnInfo`, and `AthenaWorkGroup` types are exported for test use.

## [v0.53.0] - 2026-04-02

### Added
- **DynamoDB TransactWriteItems and TransactGetItems (#272)**: Added `TransactGetItems` returning an ordered `Responses` array (one `{Item:{...}}` entry per requested key, or `{}` for missing items). Added `TransactWriteItems` with full two-phase commit semantics: all `ConditionExpression` predicates are evaluated before any mutation is applied; if any fails, the entire transaction is cancelled and a `TransactionCanceledException` (HTTP 400) is returned with a `CancellationReasons` array identifying which items caused the failure. Supports `Put`, `Update`, `Delete`, and `ConditionCheck` operations in a single transaction. Cost entries: `dynamodb/TransactGetItems: $0.00000025`, `dynamodb/TransactWriteItems: $0.00000125`.
- **S3 presigned URL expiry enforcement (#273)**: The server now validates `X-Amz-Date` + `X-Amz-Expires` on presigned requests (those carrying `X-Amz-Algorithm` in the query string instead of an `Authorization` header). Expired requests receive `403 AccessDenied` with `Request has expired.` Added `POST /v1/s3/presign` control-plane endpoint that generates substrate-compatible presigned URLs using the server's simulated clock, suitable for testing time-sensitive S3 access patterns.
- **SQS FIFO message group ordering (#274)**: `SQSMessage` now stores `MessageGroupId`. `ReceiveMessage` on FIFO queues enforces single-group-per-call isolation (locks to the first group encountered in the queue and skips messages from other groups in the same call), matching the AWS FIFO ordering guarantee. `MessageGroupId` is included in `ReceiveMessage` responses. `SendMessage` to a FIFO queue without `MessageGroupId` returns a validation error.

## [v0.52.0] - 2026-04-02

### Added
- **pytest-substrate HTTP control plane helpers (#270)**: Added `seed_result(service, result, sql="*")`, `set_status(service, status, error_message="")`, and `clear_seeds(service, sql=None)` methods to `SubstrateServer`. These wrap the `/v1/{service}/results` and `/v1/{service}/status` endpoints, so test authors no longer need to hand-craft HTTP calls. Added `redshift_rows(columns, rows, col_type="varchar")` convenience helper (exported from `pytest_substrate`) that builds a Redshift Data API result dict suitable for passing to `seed_result`. Added `substrate_isolated` function-scoped pytest fixture that starts and stops a fresh substrate process per test (heavier than the session-scoped `substrate_isolated` fixture but provides full process-level isolation).
- **OTLP span attributes (#271)**: `server.go` now sets `substrate.cost` (float64) and `substrate.stream_id` (string) attributes on every OTel tracing span, making per-request cost and stream routing visible in tracing UIs. Added `examples/docker-compose.jaeger.yml` — a Substrate + Jaeger all-in-one example that wires OTLP HTTP export to Jaeger and exposes the Jaeger UI on port 16686.

## [v0.51.0] - 2026-04-02

### Added
- **Redshift Data HTTP control plane (#269)**: Added three new server endpoints for test-time configuration of `RedshiftDataPlugin` behaviour. `POST /v1/redshift-data/results` seeds a `GetStatementResult` response keyed by SQL pattern (or `"*"` wildcard). `DELETE /v1/redshift-data/results` removes seeded results (optionally filtered by `?sql=`). `POST /v1/redshift-data/status` overrides the default statement status for new `ExecuteStatement` calls (`FINISHED`, `FAILED`, `ABORTED`, `STARTED`) with an optional `errorMessage`. Configuration is persisted in the `redshift-data-ctrl` state namespace so the plugin reads it at request time — enabling Python/SDK-level tests to configure responses without Go-level plugin initialization. In-memory `"results"` seeded at initialization continue to take precedence over state-based seeds for backward compatibility.

## [v0.50.0] - 2026-04-02

### Added
- **Redshift Data API plugin (#268)**: Added `RedshiftDataPlugin` with 3 operations: `ExecuteStatement`, `DescribeStatement`, `GetStatementResult`. Uses JSON-target protocol `RedshiftData_20191217.{Op}`; parser alias `"redshiftdata": "redshift-data"` added to `targetServiceAliases`. `ExecuteStatement` stores the statement and returns `{Id: "<uuid>"}` immediately. `DescribeStatement` always returns `Status: "FINISHED"` (deterministic, no simulated latency). `GetStatementResult` returns pre-seeded rows from the `"results"` plugin option (keyed by SQL or `"*"` wildcard), enabling tests to drive specific query responses. Row values use the Redshift Data API typed field format: `{stringValue}`, `{longValue}`, `{doubleValue}`, `{booleanValue}`, `{isNull}`. No cost entries (Redshift Data API is free within Redshift pricing).

## [v0.49.0] - 2026-04-02

### Added
- **IAM Identity Center (SSO) plugin (#266)**: Added `SSOPlugin` with 12 operations: `ListInstances`, `CreatePermissionSet`, `DescribePermissionSet`, `UpdatePermissionSet`, `DeletePermissionSet`, `ListPermissionSets`, `AttachManagedPolicyToPermissionSet`, `DetachManagedPolicyFromPermissionSet`, `ListManagedPoliciesInPermissionSet`, `CreateAccountAssignment`, `DeleteAccountAssignment`, `ListAccountAssignments`. Uses JSON-target protocol `AWSSSOAdminService.{Op}`; parser alias `"awsssoadminservice": "sso"` added to `targetServiceAliases`. SSO instance is auto-created on first `ListInstances` call (singleton per account). SSO and RAM carry no per-operation cost.
- **Resource Access Manager (RAM) plugin (#266)**: Added `RAMPlugin` with 8 operations: `CreateResourceShare`, `GetResourceShares`, `UpdateResourceShare`, `DeleteResourceShare`, `AssociateResourceShare`, `DisassociateResourceShare`, `ListPrincipals`, `ListResources`. Uses REST/JSON protocol; `parseRAMOperation` normalizes lowercase POST paths to PascalCase operation names. Resource share ARNs use `arn:aws:ram:{region}:{acct}:resource-share/{uuid}`. `GetResourceShares` supports filtering by name and by ARN list. `AssociateResourceShare`/`DisassociateResourceShare` atomically update principal and resource lists.
- **Amazon Redshift plugin (#266)**: Added `RedshiftPlugin` with 10 operations: `CreateCluster`, `DescribeClusters`, `ModifyCluster`, `DeleteCluster`, `CreateClusterParameterGroup`, `DescribeClusterParameterGroups`, `CreateClusterSubnetGroup`, `DescribeClusterSubnetGroups`, `CreateClusterSnapshot`, `DescribeClusterSnapshots`. Uses AWS Query (Action=) protocol with XML responses (`Content-Type: text/xml; charset=UTF-8`). No parser alias needed — service name is derived from `redshift.{region}.amazonaws.com` host. Clusters start with `ClusterStatus: "available"` and endpoint `{id}.{acct}.{region}.redshift.amazonaws.com:5439`. Error codes: `ClusterAlreadyExistsFault` (duplicate), `ClusterNotFoundFault` (not found). Cost entries: `redshift/CreateCluster: $0.0002`, `redshift/CreateClusterSnapshot: $0.00002`.

## [v0.48.0] - 2026-04-02

### Added
- **AWS Backup plugin (#265)**: Added `BackupPlugin` with 12 operations: `CreateBackupVault`, `DescribeBackupVault`, `DeleteBackupVault`, `ListBackupVaults`, `CreateBackupPlan`, `GetBackupPlan`, `UpdateBackupPlan`, `DeleteBackupPlan`, `ListBackupPlans`, `CreateBackupSelection`, `GetBackupSelection`, `DeleteBackupSelection`. Uses REST/JSON protocol (no X-Amz-Target); path-based routing via `parseBackupOperation` following the EFS pattern. No parser alias needed — service name is derived directly from the `backup` SigV4 credential scope or host. `UpdateBackupPlan` regenerates `VersionId`. Cost entry: `backup/CreateBackupPlan: $0.000001`.
- **AWS Transfer Family plugin (#265)**: Added `TransferPlugin` with 10 operations: `CreateServer`, `DescribeServer`, `UpdateServer`, `DeleteServer`, `ListServers`, `CreateUser`, `DescribeUser`, `UpdateUser`, `DeleteUser`, `ListUsers`. Uses JSON-target protocol `TransferService.{Op}`; parser alias `"transferservice": "transfer"` added to `targetServiceAliases`. Server IDs use real format `s-{17 hex chars}`. `DescribeServer` always returns `State: "ONLINE"`. `DeleteServer` cascades to delete all users. Cost entry: `transfer/CreateServer: $0.30`.

## [v0.47.0] - 2026-04-02

### Added
- **CodeBuild plugin (#264)**: Added `CodeBuildPlugin` with 7 operations: `CreateProject`, `BatchGetProjects`, `UpdateProject`, `DeleteProject`, `ListProjects`, `StartBuild`, `BatchGetBuilds`. Uses JSON-target protocol `CodeBuild_20161006.{Op}` (no parser alias needed). `StartBuild` immediately returns `buildStatus: "SUCCEEDED"` (deterministic). Error codes: `ResourceAlreadyExistsException` (duplicate project), `ResourceNotFoundException` (missing project). Cost entry: `codebuild/StartBuild: $0.0001`.
- **CodePipeline plugin (#264)**: Added `CodePipelinePlugin` with 8 operations: `CreatePipeline`, `GetPipeline`, `UpdatePipeline`, `DeletePipeline`, `ListPipelines`, `StartPipelineExecution`, `GetPipelineState`, `GetPipelineExecution`. Uses JSON-target protocol `CodePipeline_20150709.{Op}` (no parser alias needed). `StartPipelineExecution` immediately returns `status: "Succeeded"` (deterministic); `GetPipelineState` reports all stages as `Succeeded`; `UpdatePipeline` increments the pipeline version. Error codes: `PipelineNameInUseException`, `PipelineNotFoundException`, `PipelineExecutionNotFoundException`. Cost entry: `codepipeline/StartPipelineExecution: $0.000001`.
- **CodeDeploy plugin (#264)**: Added `CodeDeployPlugin` with 9 operations: `CreateApplication`, `GetApplication`, `DeleteApplication`, `ListApplications`, `CreateDeploymentGroup`, `GetDeploymentGroup`, `DeleteDeploymentGroup`, `CreateDeployment`, `GetDeployment`. Uses JSON-target protocol `CodeDeploy_20141006.{Op}` (no parser alias needed). `CreateDeployment` immediately returns `status: "Succeeded"` (deterministic); deployment IDs use real CodeDeploy format `d-{9-char uppercase alphanumeric}`. Error codes: `ApplicationAlreadyExistsException`, `ApplicationDoesNotExistException`, `DeploymentGroupAlreadyExistsException`, `DeploymentGroupDoesNotExistException`, `DeploymentDoesNotExistException`. Cost entry: `codedeploy/CreateDeployment: $0.000001`.

## [v0.46.0] - 2026-04-02

### Added
- **WAFv2 plugin (#263)**: Added `WAFv2Plugin` with 13 operations: `CreateWebACL`, `GetWebACL`, `UpdateWebACL`, `DeleteWebACL`, `ListWebACLs`, `AssociateWebACL`, `DisassociateWebACL`, `GetWebACLForResource`, `CreateIPSet`, `GetIPSet`, `UpdateIPSet`, `DeleteIPSet`, `ListIPSets`. Uses JSON-target protocol `AWSWAF_20190729.{Op}`; parser alias `"awswaf": "wafv2"` added to `targetServiceAliases`. LockToken-based optimistic concurrency control (UUID CAS tokens) on Update/Delete operations, regenerated after each successful mutation. Cost entries: `wafv2/CreateWebACL: $5.00`, `wafv2/AssociateWebACL: $0.000001`.
- **CloudTrail plugin (#263)**: Added `CloudTrailPlugin` with 8 operations: `CreateTrail`, `GetTrail`, `GetTrailStatus`, `UpdateTrail`, `DeleteTrail`, `DescribeTrails`, `StartLogging`, `StopLogging`. Uses JSON-target protocol `CloudTrail_20131101.{Op}` (no parser alias needed). `GetTrailStatus` deterministically returns `IsLogging: true` with `LatestDeliveryTime` set to the current simulated timestamp. Cost entry: `cloudtrail/CreateTrail: $0.000002`.
- **TestServer.SeedSSMParameter (#267)**: Added `SeedSSMParameter(name, value string)` and `SeedSSMParameters(params map[string]string)` helpers to `TestServer`. These inject SSM String parameters directly into the in-memory store (bypassing the HTTP layer), enabling tests to pre-seed public AWS SSM paths used for AMI discovery (e.g. `/aws/service/canonical/...`, `/aws/service/ami-amazon-linux-latest/...`) without requiring additional SSM client setup.

## [v0.45.10] - 2026-04-02

### Added
- **SQS JSON protocol routing (#236)**: Added `"amazonsqs": "sqs"` alias to `targetServiceAliases` in `parser.go` so that SigV4 credential-scope service names of `amazonsqs` (used by aws-sdk-go-v2 SQS JSON protocol) are correctly routed to `SQSPlugin`. The plugin already supported JSON protocol (`Content-Type: application/x-amz-json-1.0`, `X-Amz-Target: AmazonSQS.*`) via `sqsIsJSONProtocol()`; this completes the routing path for credential-scope–based dispatch.
- **CE daily granularity (#225)**: `GetCostAndUsage` with `Granularity: "DAILY"` now returns one `ResultByTime` per calendar day over the requested range (including zero-cost days), matching real AWS Cost Explorer behavior. The aggregate (non-DAILY) code path is unchanged. EC2 compute cost is correctly bucketed per day by calling `computeEC2UsageCost` over each individual day window.

## [v0.45.9] - 2026-04-02

### Added
- `POST /_substrate/reset` endpoint as an alias for `POST /v1/state/reset`, providing a stable reset URL for Docker-based test environments and LocalStack drop-in use cases (closes #259)

## [v0.45.8] - 2026-04-02

### Fixed
- **IAM**: Plugin now returns proper Query+XML protocol responses matching the AWS IAM wire format; all operations emit `<{Op}Response xmlns="https://iam.amazonaws.com/doc/2010-05-08/">` envelopes with `<{Op}Result>` inner elements and `<ResponseMetadata>` — fixes Go SDK v2 IAM client deserialization failures and enables end-to-end IAM client compatibility (closes #260)
- **SSM**: `SendCommand` `RequestedDateTime` field now serialised as Unix epoch `float64` (e.g. `1700000000.0`) instead of RFC3339 string, matching the smithy deserializer expectation of the Go SDK v2 SSM client (closes #261)
- **Parser**: route SigV4 service name `bedrock` to `bedrock-runtime` plugin when using a unified endpoint URL — fixes boto3 `bedrock-runtime` client calls that sign with service name `bedrock` (closes #262)

## [v0.45.6] - 2026-04-01

### Added

- **EC2 EBS volume operations (#256)**: Added `CreateVolume`, `DescribeVolumes`,
  `AttachVolume`, `DetachVolume`, `DeleteVolume` to `EC2Plugin` with full state
  machine (`available` → `in-use` → `available` → deleted). Volume state is
  queryable immediately after each operation. `AttachVolume` transitions the
  volume to `in-use` and stores the attachment; `DetachVolume` restores it to
  `available`. `DeleteVolume` rejects volumes in `in-use` state.
  Added `DeleteSnapshot` stub (accepts any snapshot ID, returns success; Substrate
  does not persist snapshots). New `EC2Volume`/`EC2VolumeAttachment` types in
  `ec2_types.go`. State key: `volume:{acct}/{region}/{volumeId}`.
- **IAM instance profile operations (#257)**: Added `CreateInstanceProfile`,
  `GetInstanceProfile`, `DeleteInstanceProfile`, `AddRoleToInstanceProfile`,
  `RemoveRoleFromInstanceProfile` to `IAMPlugin`. Profiles are persisted in state
  under key `instance_profile:{name}`. AWS one-role-per-profile constraint enforced
  by `AddRoleToInstanceProfile` (returns `LimitExceededException` if already
  populated). `DeleteInstanceProfile` rejects non-empty profiles with
  `DeleteConflictException`. `ListInstanceProfiles` now returns persisted profiles
  instead of a hardcoded empty list. New `IAMInstanceProfile` type in `iam_types.go`.
- **SSM Run Command (#258)**: Added `SendCommand`, `GetCommandInvocation`, and
  `DescribeInstanceInformation` to `SSMPlugin`. `SendCommand` stores a command
  record and creates per-instance invocation records immediately with
  `Status: "Success"` (deterministic test mode). `GetCommandInvocation` returns
  status and stdout/stderr for any previously sent command. `DescribeInstanceInformation`
  enumerates running EC2 instances and reports them as SSM-managed (`PingStatus:
  "Online"`). New `SSMCommand`/`SSMCommandInvocation` types. State keys:
  `command:{acct}/{region}/{commandId}`, `invocation:{acct}/{region}/{commandId}/{instanceId}`.

## [v0.45.5] - 2026-04-01

### Fixed

- **QuickSight path routing (#255)**: `parseQuickSightOperation` used the
  path prefix `datasources` but the AWS QuickSight REST API uses `data-sources`
  (with a hyphen). Calls to `create_data_source` and `describe_data_source`
  via boto3 were returning `InvalidAction: QuickSightPlugin: unsupported path`.
  Fixed by updating the prefix to `data-sources` and `data-sets`; updated
  existing tests to use the correct API paths.
- **DynamoDB empty-string attributes in Scan (#254)**: Added `Scan`-specific
  test (`TestDynamoDBPlugin_EmptyStringAttribute_Scan`) verifying that flat
  top-level `{"S":""}` attributes survive the `PutItem → Scan` round-trip.
  The root fix was applied in v0.45.3 (#252); this test documents and guards
  the Scan code path that boto3's resource `Table.scan()` exercises.
- **Config loader viper no-extension file detection**: Removed redundant
  `SetConfigType("yaml")` from `LoadConfig`; with `SetConfigName("substrate")`
  viper already searches for `substrate.yaml` via extension lookup and the extra
  `SetConfigType` caused viper to also attempt to open extension-less files
  (e.g., a locally built `./substrate` binary), producing a YAML parse error
  that failed `TestLoadConfig_Defaults` and `TestLoadConfig_EnvOverride`.

## [v0.45.4] - 2026-04-01

### Added

- **pytest-substrate Python package** (`python/`): Incorporated the `pytest-substrate`
  plugin into the project with proper build hygiene and test coverage.
  - Added `python/tests/test_server.py` (16 tests) covering `SubstrateServer._find_binary`
    (env var, home-dir candidate, PATH fallback, not-found error), `_free_port`,
    `start`/`stop` lifecycle, `_wait_healthy` retry/timeout behaviour, and `reset_state`
    HTTP POST + error handling — all using `unittest.mock`, no real binary required.
  - Added `python/tests/test_plugin.py` (4 tests) covering fixture registration and
    env-var patching.
  - Added `[tool.pytest.ini_options]` to `python/pyproject.toml` to point pytest at
    `tests/`.
  - Added `python-test`, `python-lint`, and `python-build` Makefile targets.
  - Updated `.gitignore` to exclude `python/.venv/`, `python/build/`, `python/dist/`,
    `python/src/*.egg-info/`, and `__pycache__/` artifacts.
  - Removed previously committed build artifacts (`python/build/`, `__pycache__/`,
    `python/src/pytest_substrate.egg-info/`) from git tracking.

## [v0.45.3] - 2026-04-01

### Added

- **Amazon OpenSearch Service plugin** (`opensearch_plugin.go`): New `OpenSearchPlugin`
  handling index lifecycle (`CreateIndex`, `DeleteIndex`, `GetIndex`), document operations
  (`IndexDocument` via `PUT /{index}/_doc/{id}`, `GetDocument`, `DeleteDocument`), bulk
  indexing (`Bulk`), search with query DSL (`match_all`, `term`, `terms`, `bool.must/
  should/must_not`, `exists`, `range`, `match`), aggregations (`terms`, `cardinality`,
  `value_count`, `sum`, `avg`, `max`, `min`), scroll pagination, and cluster health.
  Routed via host patterns `*.es.amazonaws.com` / `*.aoss.amazonaws.com` and SigV4
  aliases `"es"` and `"aoss"`. Documents stored under `doc:{index}/{id}` state keys;
  scroll state under `scroll:{id}`. Auto-creates index on first `IndexDocument` call.
  Closes #253.

### Fixed

- **Glue `GetTable` drops `StorageDescriptor.Columns`, `SerdeInfo`, `PartitionKeys`,
  `Parameters`**: `GlueStorageDescriptor` was missing `Columns []GlueColumn` and
  `SerdeInfo *GlueSerdeInfo`; `GlueTable` was missing `PartitionKeys []GlueColumn` and
  `Parameters map[string]string`. All four fields are now persisted through `CreateTable`
  and `UpdateTable` and returned by `GetTable`. Closes #251.
- **DynamoDB empty string attributes lost in nested maps**: `AttributeValue.S` was
  `string json:"S,omitempty"` which caused `{"S":""}` to be serialised as `{}` during
  state persistence. Changed to `*string` so that a pointer to `""` is preserved and
  round-trips correctly. Fixes the boto3 `TypeError: Value must be a nonempty dictionary
  whose key is a valid dynamodb type` error on `get_item` for items containing empty
  string values. Closes #252.

## [v0.45.2] - 2026-03-31

### Added

- **Amazon Athena plugin** (`athena_plugin.go`): New `AthenaPlugin` covering
  `StartQueryExecution`, `GetQueryExecution`, `GetQueryResults`, and
  `StopQueryExecution` via the JSON-target protocol (`AmazonAthena.{Op}`) on
  `athena.{region}.amazonaws.com`. Queries immediately transition to `SUCCEEDED`
  (deterministic), so polling loops exit on the first `GetQueryExecution` call.
  `GetQueryResults` returns an empty result set (stub; no SQL execution). State
  namespace `"athena"`, key `query:{acct}/{region}/{id}`. Parser alias
  `"amazonathena" → "athena"` added. Cost: `athena/StartQueryExecution = $0.000005`.
  Closes #249.
- **S3 SelectObjectContent** (`s3_select.go`): New `selectObjectContent` handler on
  `S3Plugin`. Accepts `POST /{bucket}/{key}?select&select-type=2` with an XML request
  body defining the SQL expression, input format (CSV with `FileHeaderInfo=USE`, or
  JSON Lines), and output serialisation. Evaluates a simplified SQL expression
  (`SELECT *` with optional `WHERE col = 'val'` and `LIMIT n`). Response is an AWS
  binary event stream (`application/vnd.amazon.eventstream`) containing Records,
  Stats, and End event frames with correct CRC32 checksums. Cost:
  `s3/SelectObjectContent = $0.0000004`. Closes #250.

## [v0.45.1] - 2026-04-01

### Added

- **Amazon QuickSight plugin** (`quicksight_plugin.go`): New `QuickSightPlugin` covering
  `CreateDataSource`, `DescribeDataSource`, `CreateDataSet`, and `DescribeIngestion` via
  REST/JSON on `quicksight.{region}.amazonaws.com`. Data sources are created with status
  `CREATION_SUCCESSFUL`; datasets return an `IngestionId` UUID; ingestions immediately
  report `COMPLETED` (deterministic). Missing resources return 404
  `ResourceNotFoundException`. State namespace `"quicksight"`. Costs:
  `quicksight/CreateDataSource = $0.000025`, `quicksight/CreateDataSet = $0.000025`.
  Closes #247.
- **Amazon Bedrock Runtime plugin** (`bedrock_runtime_plugin.go`): New
  `BedrockRuntimePlugin` covering `ApplyGuardrail` via REST/JSON on
  `bedrock-runtime.{region}.amazonaws.com`. Guardrails are auto-created on first call.
  Default behaviour is pass-through (`action: NONE`, input text echoed). When a blocklist
  is seeded under state key `guardrail:{accountId}/{guardrailId}/blocklist`, matching
  content triggers `action: GUARDRAIL_INTERVENED` with a canned blocked-response message.
  Supports both `INPUT` and `OUTPUT` sources. Costs:
  `bedrock-runtime/ApplyGuardrail = $0.000075`. Closes #248.

## [v0.45.0] - 2026-03-21

### Added

- **AWS Batch plugin** (`batch_plugin.go`): New `BatchPlugin` for the AWS Batch service.
  Handles `SubmitJob`, `DescribeJobs`, `TerminateJob`, `ListJobs`,
  `CreateComputeEnvironment`, `CreateJobQueue`, and `RegisterJobDefinition` via REST/JSON
  path routing on `batch.{region}.amazonaws.com`. Jobs are immediately stored with
  `SUCCEEDED` status (deterministic). State namespace `"batch"`, keys `job:`, `job_ids:`.
  Costs: `batch/SubmitJob = $0.00001`. Closes #237.
- **Amazon SageMaker plugin** (`sagemaker_plugin.go`): New `SageMakerPlugin` for the
  SageMaker service. Handles Studio app operations (`CreateApp`, `DeleteApp`, `DescribeApp`,
  `ListApps`, `ListDomains`, `CreatePresignedDomainUrl`) and training job operations
  (`CreateTrainingJob`, `DescribeTrainingJob`, `StopTrainingJob`, `ListTrainingJobs`) via
  JSON-target protocol (`X-Amz-Target: SageMaker.{Op}`). Training jobs are immediately
  `Completed` (deterministic). State namespace `"sagemaker"`. Costs:
  `sagemaker/CreateTrainingJob = $0.001`, `sagemaker/CreateApp = $0.0001`. Closes #238.
- **Amazon EMR Serverless plugin** (`emrserverless_plugin.go`): New `EMRServerlessPlugin`
  for the EMR Serverless service. Handles `CreateApplication`, `GetApplication`,
  `DeleteApplication`, `StartJobRun`, `GetJobRun`, `CancelJobRun`, and `ListJobRuns` via
  REST/JSON path routing on `emrserverless.{region}.amazonaws.com`. Job runs immediately
  have state `SUCCESS` (deterministic). State namespace `"emrserverless"`. Costs:
  `emrserverless/StartJobRun = $0.0001`, `emrserverless/CreateApplication = $0.00001`.
  Closes #239.
- **Amazon HealthOmics plugin** (`omics_plugin.go`): New `OmicsPlugin` for the HealthOmics
  service. Handles `StartRun`, `GetRun`, `CancelRun`, and `ListRuns` via REST/JSON path
  routing on `omics.{region}.amazonaws.com`. Runs are immediately `COMPLETED`
  (deterministic). Run IDs are 10-digit numeric strings matching real HealthOmics format.
  State namespace `"omics"`. Costs: `omics/StartRun = $0.001`. Closes #240.

## [v0.44.4] - 2026-03-21

### Added

- **EC2 launch templates** (`ec2_plugin.go`, `ec2_types.go`): Added `CreateLaunchTemplate`,
  `DescribeLaunchTemplates`, and `DeleteLaunchTemplate` operations. `RunInstances` now
  resolves `ImageId`, `InstanceType`, `KeyName`, and `SecurityGroupId` from a referenced
  launch template when those parameters are not supplied directly. New types:
  `EC2LaunchTemplate`, `EC2LaunchTemplateData`, `generateLaunchTemplateID`. State keys use
  namespace `"ec2"` with prefix `lt:`, `lt_by_name:`, `lt_ids:`. Closes #243.

## [v0.44.3] - 2026-03-22

### Fixed

- **StepFunctions routing** (`parser.go`): Added `"awsstepfunctions": "states"` alias to
  `targetServiceAliases`. The AWS SDK v2 sfn client sends `X-Amz-Target: AWSStepFunctions.*`
  which was not matched by the existing `"Amazon"` prefix stripping, causing every SDK call
  to return `ServiceNotAvailable: service not emulated: awsstepfunctions`. Closes #242.
- **ECS timestamp serialization** (`ecs_types.go`, `epochseconds.go`): Replaced `time.Time`
  fields `ECSTaskDefinition.RegisteredAt`, `ECSService.CreatedAt`, `ECSTask.StartedAt`, and
  `ECSTask.StoppedAt` with the new `EpochSeconds` type, which marshals as a JSON float64
  (Unix epoch seconds). The AWS SDK v2 ECS client uses `smithytime.ParseEpochSeconds` and
  expects a JSON number, not an RFC3339 string. Closes #241.
- **Step Functions timestamp serialization** (`stepfunctions_plugin.go`): All response maps
  (`creationDate`, `startDate`, `stopDate`, `updateDate`) now emit `float64` epoch seconds
  via the new `sfnEpoch` helper instead of RFC3339 strings. Struct fields in list-response
  entry types (`smEntry.CreationDate`, `execEntry.StartDate`/`StopDate`,
  `actEntry.CreationDate`) changed from `string` to `float64`. Fixes the companion timestamp
  issue noted in #242.

### Added

- **`EpochSeconds` type** (`epochseconds.go`): New package-level type that wraps `time.Time`
  and marshals/unmarshals as JSON float64 epoch seconds, with RFC3339 string fallback on
  unmarshal for backward-compatible state reads.

## [v0.44.2] - 2026-03-19

### Added

- **SQS JSON protocol support** (`sqs_plugin.go`): `SQSPlugin` now handles both the
  query protocol (`application/x-www-form-urlencoded`) and the AWS JSON protocol
  (`application/x-amz-json-1.0` with `X-Amz-Target: AmazonSQS.<Op>`) for all 16
  operations. Existing query-protocol behaviour is completely unchanged. New helpers:
  `sqsIsJSONProtocol`, `sqsJSONResponse`, `sqsQueueURLFromRequest`. JSON `ReceiveMessage`
  always returns `"Messages":[]` (never `null`) when the queue is empty. Closes #236.
- **12 new SQS JSON protocol tests** (`sqs_plugin_test.go`): cover create/get/delete queue,
  attributes, send/receive/delete messages, batch operations, tags, visibility changes,
  purge, error on non-existent queue, and a cross-protocol test (create via JSON, send via
  query, receive via JSON).

### Note

v0.44.0 and v0.44.1 are tagged at `e11f4b2` (servicequotas routing fix). v0.43.4 is a
later commit on the same main branch that added EC2 `DescribeRegions`; it supersedes
v0.44.0/v0.44.1 in functionality. v0.44.2 is built on top of v0.43.4 and consolidates
all changes onto the v0.44.x line.

## [v0.43.4] - 2026-03-19

### Added

- **EC2 `DescribeRegions`** (`ec2_plugin.go`): Returns a pre-seeded list of three enabled
  regions (`us-east-1`, `us-west-2`, `eu-west-1`) with `opt-in-not-required` opt-in status
  and the canonical regional EC2 endpoint. Supports `RegionName.N` filters. Enables testing
  of code that fans out EC2 calls across regions (e.g. `GetEnabledRegions` in truffle).
  Closes #235.

## [v0.43.3] - 2026-03-19

### Added

- **EC2 instance type and spot price operations** (`ec2_plugin.go`): Three new read-only
  operations enable testing of instance-type discovery and Spot pricing without real AWS
  credentials.
  - `DescribeInstanceTypes`: returns a pre-seeded catalog of 8 instance types
    (`t3.micro`, `c5.xlarge`, `c5.2xlarge`, `m5.large`, `r5.xlarge`, `p3.2xlarge`,
    `g4dn.xlarge`, `inf1.xlarge`) with vCPU, memory, architecture, and usage-class details.
    Supports `InstanceType.N` filters.
  - `DescribeInstanceTypeOfferings`: returns the same catalog for all three AZs in the
    request region. Supports `location` filter.
  - `DescribeSpotPriceHistory`: returns fixed stub prices for each seeded instance type.
    Supports `InstanceType.N`, `AvailabilityZone`, and `ProductDescriptions` filters.
  Closes #234.

## [v0.43.2] - 2026-03-19

### Fixed

- **FSx LustreConfiguration in responses** (`fsx_plugin.go`): `CreateFileSystem` and
  `DescribeFileSystems` now include a `LustreConfiguration` object (`MountName`,
  `DeploymentType`) in the wire response for LUSTRE file systems. Previously this field was
  absent, causing nil-pointer panics in any code that dereferenced
  `fs.LustreConfiguration.MountName`. `DeploymentType` defaults to `SCRATCH_2` when not
  supplied by the caller; `MountName` is `"fsx"` for SCRATCH_2 and a random hex string for
  other deployment types. Closes #233.

## [v0.43.1] - 2026-03-19

### Fixed

- **FSx SDK routing** (`parser.go`): Added `"awssimbaapiservice": "fsx"` to
  `targetServiceAliases`. The AWS SDK v2 FSx client sends
  `X-Amz-Target: AWSSimbaAPIService_v20180301.<Op>` — the parser stripped the version
  suffix (`_v20180301`) leaving `AWSSimbaAPIService` (lowercase: `awssimbaapiservice`), which
  had no alias and caused a `501 ServiceNotAvailable` error on every FSx SDK call. Closes #232.

## [v0.43.0] - 2026-03-19

### Added

- **FSx plugin** (`fsx_plugin.go`): JSON-protocol plugin on `fsx.{region}.amazonaws.com`
  (target: `AmazonFSx.<Op>`). Implements `CreateFileSystem`, `DescribeFileSystems`, and
  `DeleteFileSystem`. Supports `LUSTRE`, `WINDOWS`, `ONTAP`, and `OPENZFS` file system types.
  State keys: `fs:{acct}/{region}/{id}`, `fs_ids:{acct}/{region}`. File systems transition
  immediately to `AVAILABLE`; delete soft-marks as `DELETED`. `CreationTime` is returned as
  a Unix epoch `float64` per AWS SDK requirements. Cost: `fsx/CreateFileSystem = $0.00013`.
  Betty CFN: `AWS::FSx::FileSystem` at priority 3; `Ref` = `FileSystemId`. Closes #230.

### Fixed

- **Scheduler timestamp format** (`scheduler_plugin.go`): `GetSchedule` and `ListSchedules`
  now return `CreationDate` and `LastModificationDate` as Unix epoch `float64` values (e.g.
  `1711929600.0`) instead of RFC3339 strings. The AWS SDK v2 `scheduler` client deserializes
  these fields as `*time.Time` via a `float64` JSON path and previously panicked or returned
  a zero time when it received a quoted string. Closes #231.

## [v0.42.1] - 2026-03-19

### Fixed

- **Spurious SDK warning** (`server.go`): `writeResponse` now sets `Content-Length` on every
  response (unless the plugin already supplied one, as S3 HEAD does). This allows the AWS SDK
  v2 transport to drain and close response bodies cleanly, eliminating the
  "failed to close HTTP response body, this may affect connection reuse" warning that appeared
  in `go test -v` output when using `StartTestServer`. Closes #229.

## [v0.42.0] - 2026-03-19

### Added

- **EventBridge Scheduler plugin** (`scheduler_plugin.go`): REST-JSON plugin on
  `scheduler.{region}.amazonaws.com`. Implements `CreateSchedule` (HTTP 201),
  `GetSchedule`, `UpdateSchedule`, `DeleteSchedule`, `ListSchedules`. State keys:
  `sched:{acct}/{region}/{groupName}/{name}`. Supports `namePrefix`, `state`, `maxResults`,
  and `nextToken` (base64 integer offset) pagination on `ListSchedules`. Errors:
  `ConflictException` (409) on duplicate create, `ResourceNotFoundException` (404) on
  missing schedule. Cost: `scheduler/CreateSchedule = $0.0000001`. Closes #228.

## [v0.41.3] - 2026-03-18

### Fixed

- **`revive` naming** (`ec2_types.go`, `ec2_plugin.go`, `iam_managed.go`): Renamed struct fields
  to comply with Go naming conventions — `PublicDnsName`→`PublicDNSName`,
  `PrivateDnsName`→`PrivateDNSName`, `MapPublicIpOnLaunch`→`MapPublicIPOnLaunch`,
  `EnableDnsSupport`→`EnableDNSSupport`, `EnableDnsHostnames`→`EnableDNSHostnames`.
- **`nilerr`** (`ec2_plugin.go`, `stepfunctions_asl.go`): Added `//nolint:nilerr` on intentional
  nil returns following a non-nil error (resource-not-found pass-through and non-JSON response
  stub).
- **`staticcheck` S1016** (`ec2_plugin.go`): Added `//nolint:staticcheck` on `tagItem` struct
  literals where xml tags differ from the source `EC2Tag` json tags.

## [v0.41.2] - 2026-03-18

### Fixed

- **gofmt / linter fixes** (`ec2_plugin.go`, `ce_plugin.go`, `ce_plugin_test.go`, `ce_types.go`,
  `s3_plugin.go`, `parser.go`, and several test files): Applied `gofmt`; fixed all
  `golangci-lint` issues — misspellings (`behaviour`→`behavior`, `synthesising`→`synthesizing`,
  `Synthesise`→`Synthesize`, `modelled`→`modeled`), `ineffassign` on unused `name` reassignment
  in `deleteKeyPair`, and `nilerr` in `deleteObjects`.

## [v0.41.1] - 2026-03-18

### Fixed

- **gofmt formatting** (`ec2_plugin.go`, `ec2_plugin_test.go`, `cloudwatch_plugin.go`,
  `cloudwatch_plugin_test.go`, `parser.go`, `plugin_lifecycle_test.go`, `s3_plugin_test.go`,
  `testing.go`): Applied `gofmt` to all files that were flagged by the CI lint step.

## [v0.41.0] - 2026-03-18

### Added

- **`DescribeAvailabilityZones`** (`ec2_plugin.go`): Returns 3 synthetic AZs (`{region}a/b/c`,
  state `available`) for any region. No state required; deterministic for replay.
- **`ModifySubnetAttribute`** (`ec2_plugin.go`): Toggles `MapPublicIpOnLaunch` on a subnet and
  persists the change. Returns `InvalidSubnetID.NotFound` (HTTP 400) for unknown subnets.
- **`ModifyVpcAttribute`** (`ec2_plugin.go`): Toggles `EnableDnsSupport` and `EnableDnsHostnames`
  on a VPC and persists the changes. Returns `InvalidVpcID.NotFound` (HTTP 400) for unknown VPCs.
- **`EnableDnsSupport` / `EnableDnsHostnames` fields** (`ec2_types.go`): Added to `EC2VPC`.
  `createVPC` now sets `EnableDnsSupport: true` (AWS default). `ensureDefaultVPC` additionally
  sets `EnableDnsHostnames: true` (default VPC default).
- **Elastic IP operations** (`ec2_plugin.go`, `ec2_types.go`): `AllocateAddress`,
  `AssociateAddress`, `DisassociateAddress`, `ReleaseAddress`, `DescribeAddresses`. EIPs use
  `eipalloc-` prefixed IDs. Associating with an instance updates the instance's public IP and
  DNS. Releasing an associated EIP returns `InvalidIPAddress.InUse` (HTTP 400). State keys:
  `eip:{acct}/{region}/{allocationID}`. Costs: `ec2/AllocateAddress = $0.005`.
- **NAT Gateway operations** (`ec2_plugin.go`, `ec2_types.go`): `CreateNatGateway`,
  `DescribeNatGateways`, `DeleteNatGateway`. Private IP derived deterministically via FNV-32a
  on the NAT gateway ID. State immediately `available`. `DeleteNatGateway` soft-deletes (state
  set to `deleted`). `DescribeNatGateways` supports `NatGatewayId.N` and `Filter.N` (`state`,
  `vpc-id`). State keys: `nat:{acct}/{region}/{natGatewayID}`. Costs:
  `ec2/CreateNatGateway = $0.045`.
- **`AWS::EC2::EIP`** (`betty_cfn.go`, `betty_cfn_v41_plugins.go`): CFN support; priority 2.
  `Ref` / `PhysicalID` = allocationID (`eipalloc-…`). `Fn::GetAtt AllocationId` and `PublicIp`
  available via `Metadata`.
- **`AWS::EC2::NatGateway`** (`betty_cfn.go`, `betty_cfn_v41_plugins.go`): CFN support;
  priority 4 (after subnets and EIPs). `Ref` / `PhysicalID` = natGatewayID (`nat-…`).

## [v0.40.0] - 2026-03-18

### Added

- **EC2 public IP / DNS assignment** (`ec2_plugin.go`, `ec2_types.go`): Instances launched
  into the default VPC subnet (or any subnet with `MapPublicIpOnLaunch=true`) now receive a
  deterministic synthetic public IPv4 address in Amazon's `54.0.0.0/8` range, a
  `dnsName` (public DNS), and a `privateDnsName` (private DNS) in both `RunInstances` and
  `DescribeInstances` XML responses. The public IP is derived from the instance ID via
  FNV-32a hash, ensuring stable values across replays. DNS name format matches real AWS:
  `ec2-<a>-<b>-<c>-<d>.compute-1.amazonaws.com` (us-east-1) /
  `ec2-<a>-<b>-<c>-<d>.<region>.compute.amazonaws.com` (other regions) /
  `ip-<a>-<b>-<c>-<d>.<region>.compute.internal` (private). Closes #N.
- `EC2Subnet.MapPublicIpOnLaunch` field persisted in state and surfaced in `DescribeSubnets`
  XML responses (`mapPublicIpOnLaunch` element). Default subnets created by `ensureDefaultVPC`
  now set this field to `true`, mirroring real AWS behaviour. Closes #226.

## [v0.39.0] - 2026-03-18

### Added

- **ASL execution engine** (`stepfunctions_asl.go`): Real in-process execution of Amazon
  States Language definitions replacing the previous stub that auto-flipped executions to
  SUCCEEDED. Supports all seven state types — Task, Pass, Wait, Choice, Succeed, Fail,
  Parallel, Map — with synchronous deterministic execution. Closes #151, #152.
- **Task state Lambda invocation** (`stepfunctions_asl.go`): Task states whose `Resource`
  is a Lambda ARN dispatch to the Lambda plugin via the plugin registry (same pattern as
  S3 notifications). Non-Lambda resources return a stub `{}` output. Closes #153.
- **`StartSyncExecution`** (`stepfunctions_plugin.go`): Express workflows (`type=EXPRESS`)
  support `StartSyncExecution`, which executes the state machine synchronously and returns
  `{executionArn, startDate, stopDate, status, output}` in a single response. Attempting
  `StartSyncExecution` on a STANDARD state machine returns `InvalidDefinition`. Closes #154.
- **Catch/Retry with configurable backoff** (`stepfunctions_asl.go`): Task states honour
  `Retry` configs (`MaxAttempts`, `IntervalSeconds`, `BackoffRate`) and `Catch` configs
  (`ErrorEquals`, `Next`, `ResultPath`). The `TimeController` is advanced by the computed
  back-off on each retry so tests remain deterministic. `States.ALL` wildcard supported.
  Closes #155.
- **Real execution history** (`stepfunctions_plugin.go`): `GetExecutionHistory` now returns
  the complete ordered event list recorded during execution — `ExecutionStarted`,
  `StateEntered`, `TaskScheduled`, `TaskSucceeded`/`TaskFailed`, `StateExited`,
  `ExecutionSucceeded`/`ExecutionFailed` — instead of the previous hardcoded two-event stub.
- **New ASL types** (`stepfunctions_types.go`): `StateMachineDefinition`, `ASLState`,
  `RetryConfig`, `CatchConfig`, `ChoiceRule`, `HistoryEvent`; `ExecutionState` gains
  `History []HistoryEvent` and `ErrorDetails string`.

### Changed

- `StepFunctionsPlugin` now accepts `registry` and `time_controller` options; both are
  wired by `RegisterDefaultPlugins` (`plugins.go`).
- `StartExecution` executes the state machine synchronously before returning; the stored
  execution has a terminal status (`SUCCEEDED` or `FAILED`) by the time the response is sent.
- `DescribeExecution` returns the stored state as-is (no longer auto-flips RUNNING to
  SUCCEEDED on first describe).

## [v0.38.0] - 2026-03-18

### Added

- **RDS Aurora clusters** (`rds_plugin.go`, `rds_types.go`): `CreateDBCluster`,
  `DescribeDBClusters`, `DeleteDBCluster` — cluster ARNs, endpoints, and status.
  Closes #133.
- **MSK plugin** (`msk_plugin.go`, `msk_types.go`): Kafka cluster lifecycle —
  `CreateCluster`, `DescribeCluster`, `ListClusters`, `DeleteCluster`. Cluster ARN
  format: `arn:aws:kafka:{region}:{acct}:cluster/{name}/{uuid}`. Closes #138.
- **Email capture endpoint** (`debug_ui.go`): `GET /v1/emails` returns all SES messages
  captured in the emulator as JSON, enabling test assertions without a real mail server.
  Closes #140.
- **`RestoreDBInstanceFromDBSnapshot`** (`rds_plugin.go`): Restores an RDS DB instance
  from a DB snapshot, creating a new instance with the snapshot's engine/storage settings.
  Closes #188.
- **Betty CFN resources for ElastiCache and MSK** (`betty_cfn_*.go`): CloudFormation
  resource types `AWS::ElastiCache::ReplicationGroup` and `AWS::MSK::Cluster` now
  deployable via Betty. Closes #189.

## [v0.37.1] - 2026-03-18

### Added

- **CloudWatch `PutMetricData` and `ListMetrics`** (`cloudwatch_plugin.go`): `PutMetricData`
  now records metric names by namespace so that `ListMetrics` can return them. Actual
  data-point values continue to be discarded (no time-series storage); `GetMetricData` still
  returns an empty result set. `ListMetrics` supports `Namespace` and `MetricName` filter
  parameters. Three unit tests added. Closes #221.

### Fixed

- **S3 `HeadObject` / `GetObject` omit `Content-Encoding` header** (`s3_plugin.go`,
  `s3_types.go`): `S3Object` was missing a `ContentEncoding` field; the header set on
  `PutObject` was silently discarded and never returned by subsequent reads. Added
  `ContentEncoding string` to `S3Object`, capture it in `putObject`, and emit
  `Content-Encoding` in both `headObject` and `getObject` when non-empty. Fixes #222.
- **S3 `PutObject` to non-existent bucket returns `NoSuchBucket`** (`s3_plugin.go`):
  the bucket-existence check was already in place; regression tests added to confirm the
  behaviour and prevent future regressions. Fixes #223.
- **S3 `ListObjectsV2` object size is correct** (`s3_plugin.go`): `Size` is stored as
  `int64(len(body))` at `PutObject` time and round-trips correctly through JSON state
  storage; regression test added. Fixes #224.

## [v0.37.0] - 2026-03-18

### Added

- **HTTP time-control endpoints** (`debug_ui.go`, `server.go`, `types.go`): three new REST
  endpoints let any external test harness drive Substrate's simulated clock over HTTP without
  a Go client.
  - `GET /v1/control/time` — returns `{"simulated_time":"<RFC3339Nano>","scale":<float>}`.
  - `POST /v1/control/time` — body `{"time":"<RFC3339>"}` jumps the clock to an absolute
    instant; response is the same shape as GET.
  - `POST /v1/control/scale` — body `{"scale":<positive float>}` sets the time acceleration
    factor (1.0 = real-time, 3600.0 = one real second equals one simulated hour); response
    is the same shape as GET. Returns 400 if scale ≤ 0.
  - `TimeController.Scale() float64` accessor added to `types.go` (thread-safe, RWMutex).
  - Nine unit tests in `time_control_test.go` cover GET, POST valid/invalid inputs, and the
    accelerated-clock property test (`TestTimeScale_AcceleratesTime`). Closes #220.

### Fixed

- **`TestLoadConfig_EnvOverride` test isolation** (`.gitignore`): viper's config discovery
  was picking up the `substrate` build artifact in the project root and attempting to parse
  it as YAML, causing the test to fail. Added `/substrate` and `/substratelocal` to
  `.gitignore` so accidental root-level builds are excluded from version control and do not
  interfere with config-loading tests.

## [v0.36.21] - 2026-03-18

### Fixed

- **EC2 `CreateSecurityGroup` now reads description from `GroupDescription` parameter** (`ec2_plugin.go`): the handler was reading `req.Params["Description"]` but the AWS EC2 query protocol wire format sends `GroupDescription`. Security groups created via any AWS SDK always had an empty description. The fix changes the key to `"GroupDescription"`. Existing tests that passed the wrong parameter name were also corrected. Fixes #219.

### Added

- **Regression test for `CreateSecurityGroup` description** (`ec2_plugin_test.go`): `TestEC2_SecurityGroup_GroupDescription` creates a group with `GroupDescription=my group description` and asserts that `DescribeSecurityGroups` returns the same value in `groupDescription`.

## [v0.36.20] - 2026-03-18

### Fixed

- **EC2 `DescribeKeyPairs` now includes `createTime` field** (`ec2_plugin.go`, `ec2_types.go`): `EC2KeyPair` was missing a `CreatedAt` field; `CreateKeyPair` and `ImportKeyPair` did not record the creation timestamp and `DescribeKeyPairs` omitted `<createTime>` from the XML response. AWS SDKs that read `KeyPairInfo.CreateTime` would receive `nil`, causing a zero `time.Time` on dereference. The fix adds `CreatedAt string` to `EC2KeyPair`, stamps it with the simulated clock at creation, and emits `<createTime>` in `DescribeKeyPairs`. Fixes #218.

### Added

- **Regression test for EC2 key pair `createTime`** (`ec2_plugin_test.go`): `TestEC2_KeyPair_CreateTime` verifies that `DescribeKeyPairs` returns a non-empty `createTime` after `CreateKeyPair`.

## [v0.36.19] - 2026-03-18

### Fixed

- **S3 multipart uploads now preserve user metadata** (`s3_plugin.go`, `s3_types.go`): `x-amz-meta-*` headers supplied to `CreateMultipartUpload` were silently discarded — `S3MultipartUpload` had no `UserMetadata` field, so `completeMultipartUpload` always assembled the final object with an empty metadata map. `HeadObject`/`GetObject` on a completed multipart object would return no metadata regardless of what was provided at initiation. The fix adds `UserMetadata map[string]string` to `S3MultipartUpload`, captures `extractUserMetadata(req.Headers)` in `createMultipartUpload`, and copies it to the assembled `S3Object` in `completeMultipartUpload`. `PutObject` (single-part) was not affected. Fixes #217.

### Added

- **Regression test for multipart user metadata** (`s3_plugin_test.go`): `TestS3_MultipartUpload_UserMetadata` verifies that `X-Amz-Meta-*` headers supplied to `CreateMultipartUpload` are returned by both `HeadObject` and `GetObject` after `CompleteMultipartUpload`.

## [v0.36.18] - 2026-03-18

### Fixed

- **EC2 `DescribeImages` now includes `creationDate` field** (`ec2_plugin.go`, `ec2_types.go`): `EC2Image` was missing a `CreationDate` field; `CreateImage` did not record the timestamp and `DescribeImages` omitted `<creationDate>` from the XML response. AWS SDKs that parse `creationDate` to sort or filter AMIs would see an empty value. The fix adds `CreationDate string` to `EC2Image`, stamps it with the current simulated time at `CreateImage` time, and emits `<creationDate>` in `DescribeImages`. Fixes #214.

- **EC2 `DescribeKeyPairs` now includes `keyType` field** (`ec2_plugin.go`, `ec2_types.go`): `EC2KeyPair` was missing a `KeyType` field; `CreateKeyPair` and `ImportKeyPair` did not store the key type and `DescribeKeyPairs` omitted `<keyType>` from the XML response. `CreateKeyPair` now defaults to `"rsa"` and honours an explicit `KeyType` parameter. `ImportKeyPair` infers the type from the OpenSSH public key prefix (`ssh-ed25519` → `"ed25519"`, else `"rsa"`). All three operations now echo `<keyType>` in their XML response. Fixes #215.

- **S3 `CreateMultipartUpload` and `ListMultipartUploads` now recognised with bare or empty `?uploads` query parameter** (`s3_plugin.go`): `parseS3Operation` checked `req.Params["uploads"] == "1"` to detect `?uploads`, but both the bare form (`?uploads`) and the AWS SDK form (`?uploads=`) store an empty string (`""`) in `Params`, never `"1"`. Both operations were silently mis-routed. The fix uses a map presence check (`_, ok := req.Params["uploads"]; ok`) for both routing decisions. Fixes #216.

### Added

- **Regression tests for EC2 `keyType` and `creationDate` fields** (`ec2_plugin_test.go`): `TestEC2_KeyPair_KeyType_Default` verifies `CreateKeyPair` defaults to `"rsa"` and that `DescribeKeyPairs` echoes `keyType`. `TestEC2_KeyPair_KeyType_Ed25519` verifies an explicit `KeyType=ed25519` is stored and returned. `TestEC2_Image_CreationDate` verifies `DescribeImages` returns a non-empty `creationDate`.

- **Regression test for S3 multipart `?uploads=` routing** (`s3_plugin_test.go`): `TestS3_MultipartUpload_ExplicitEmptyUploadsParam` sends `?uploads=` (explicit empty value, AWS SDK style) and verifies `CreateMultipartUpload` and `ListMultipartUploads` are both correctly routed.

## [v0.36.17] - 2026-03-18

### Fixed

- **S3 path-style requests to IPv4/IPv6 addresses now work correctly** (`parser.go`): `normalizeS3CustomEndpointVirtualHost` treated any host containing a dot as a virtual-hosted-style request and extracted the first octet of an IPv4 address (e.g. `"127"` from `"127.0.0.1"`) as the bucket name. Requests to `127.0.0.1:<port>` would incorrectly route to a non-existent bucket and return `NoSuchBucket`. The fix adds a `net.ParseIP` check before the dot-presence check; IPv4 and IPv6 bracket-notation addresses are now correctly treated as path-style hosts. Fixes #213.

### Changed

- **`StartTestServer` now uses `localhost` instead of `127.0.0.1`** (`testing.go`): the listener address and `TestServer.URL` are now `http://localhost:<port>`. This avoids the IP-address virtual-hosting misparse for callers that use `TestServer.URL` as an S3 base endpoint with path-style requests (the `UsePathStyle = true` pattern). The fix in `parser.go` is the authoritative resolution; this change adds defence-in-depth and simplifies the `URL` field for humans reading test output.

## [v0.36.16] - 2026-03-18

### Fixed

- **S3 `PutObject` now preserves trailing-slash keys (directory markers)** (`s3_plugin.go`): `parseS3Operation` was unconditionally stripping the trailing `/` from all object keys. Keys like `"newdir/"` became `"newdir"`, breaking the common S3 directory-marker pattern. The fix narrows the trim to the degenerate `"/"` case (from `"/bucket//"` style URLs) and leaves all other keys intact. Additionally, `putObject` and `getObject` now bypass the afero filesystem entirely for directory-marker keys (key ends with `/`) because `filepath.Clean` inside `MemMapFs` would corrupt the path — state metadata is sufficient for zero-body markers. Fixes #212.

### Added

- **Regression tests for S3 directory markers** (`s3_plugin_test.go`): `TestS3_DirectoryMarker_KeyPreserved` verifies that `PutObject` / `HeadObject` / `GetObject` / `ListObjectsV2` all preserve the trailing slash in the stored key. `TestS3_DirectoryMarker_AppearsAsPrefix` verifies that a directory-marker object is correctly grouped into `CommonPrefixes` (not `Contents`) when `ListObjectsV2` is called with `delimiter="/"`.

## [v0.36.15] - 2026-03-18

### Fixed

- **`DescribeSecurityGroups` now applies `Filters` parameter** (`ec2_plugin.go`): `group-name`, `vpc-id`, and `group-id` filters are now respected. Previously the `Filters` parameter was silently ignored and all security groups in the account/region were returned. This caused `ensureCanopyDefaultSG`-style idempotency checks to see stale or mismatched groups. Added `extractEC2Filters` helper to parse EC2 query-protocol `Filter.N.Name` / `Filter.N.Value.M` parameters into a name→values map, reusable by other describe operations. Fixes #211.

## [v0.36.14] - 2026-03-18

### Fixed

- **`GetCostAndUsage` GroupBy TAG type check is now case-insensitive** (`ce_plugin.go`): the `Type` field comparison now uses `strings.EqualFold` so that callers sending `"tag"` or `"Tag"` are handled identically to `"TAG"`, matching the robustness of other case-insensitive checks in the codebase. Fixes #210.

### Added

- **Regression tests for GroupBy TAG** (`ce_plugin_test.go`): `TestCE_GetCostAndUsage_CreateTagsAfterLaunch` verifies that tags applied via `CreateTags` after `RunInstances` (the common consumer pattern) are visible in GroupBy TAG cost queries. `TestCE_GetCostAndUsage_GroupByTag_NoEventStoreLeakage` verifies that EventStore service records (`"ec2"`, `"iam"`, `"ce"`, etc.) do not appear in TAG-grouped responses — only `"TagKey$TagValue"` entries are returned.

## [v0.36.13] - 2026-03-18

### Added

- **`GetCostAndUsage` GroupBy TAG support** (`ce_plugin.go`, `ce_types.go`): `GroupBy [{Type: "TAG", Key: "Name"}]` now returns one group per unique tag value using the AWS CE `"TagKey$TagValue"` key format. An optional `Filter.Dimensions` service filter (e.g. `Key=SERVICE, Values=["Amazon Elastic Compute Cloud - Compute"]`) restricts results to the matching service. Instances without the requested tag are grouped under `"TagKey$"`, matching real AWS behaviour. Fixes #209.

### Changed

- Extracted `ec2InstanceCostInWindow` package-level helper and `clampedQueryEnd` method to eliminate duplicate logic between `computeEC2UsageCost` and the new `computeEC2UsageCostByTag`.

## [v0.36.12] - 2026-03-18

### Fixed

- **`GetCostAndUsage` metric key now mirrors the request** (`ce_plugin.go`): groups and the total bucket previously always used `"UnblendedCost"` regardless of the `Metrics` field sent by the caller. Callers that request `"BlendedCost"` (the AWS SDK default) now receive `BlendedCost` keys with non-nil `Amount` values. Fixes #208.

## [v0.36.11] - 2026-03-18

### Fixed

- **EC2 `TerminateInstances` now records termination time** (`ec2_plugin.go`, `ec2_types.go`): `EC2Instance` gains a `TerminatedTime` field (RFC3339) set via `p.tc.Now()` when an instance is terminated, ensuring simulated-clock accuracy.

### Added

- **Cost Explorer EC2 usage costs** (`ce_plugin.go`): `GetCostAndUsage` now reflects simulated EC2 compute spend. For each instance in the account, hours overlapping the query window are multiplied by a per-type on-demand rate (17 instance types defined; unknown types fall back to $0.096/hr). Cost accrues from `LaunchTime` to `TerminatedTime` (or `tc.Now()` for running instances), so simulated time advances directly drive `GetCostAndUsage` results. Result appears under the `"Amazon Elastic Compute Cloud - Compute"` service group. Fixes #207.
- **`CEPlugin` accepts `time_controller` option** (`ce_plugin.go`, `plugins.go`): the `TimeController` is now passed to `CEPlugin` via `RegisterDefaultPlugins` so cost accrual uses simulated time rather than wall-clock time.

## [v0.36.10] - 2026-03-18

### Added

- **HTTP time-control endpoints** (`server.go`, `debug_ui.go`): three REST endpoints allow any test harness to drive the simulated clock over HTTP without a Go client. `GET /v1/control/time` returns the current simulated time and scale factor; `POST /v1/control/time` jumps the clock to an arbitrary RFC3339 instant; `POST /v1/control/scale` sets the acceleration factor (e.g. `{"scale":3600}` makes 1 real second equal 1 simulated hour).
- **`TimeController.Scale()`** (`types.go`): new read-only accessor returns the current time acceleration factor.
- **`TestServer` time-control methods** (`testing.go`): `AdvanceTime(d time.Duration)`, `SetTime(t time.Time)`, and `SetScale(scale float64)` allow in-process integration tests to drive the simulated clock directly without HTTP calls. Fixes #206.

## [v0.36.9] - 2026-03-18

### Fixed

- **`TimeController` live accelerated clock** (`types.go`): `SetScale` stored the multiplier but `Now()` ignored it, so `SetScale(86400)` had no observable effect. The implementation now uses a (simulated baseline, wall baseline) pair: `Now()` returns `simBaseline + (wall_now − wallBaseline) × scale`. `SetTime` and `SetScale` both reset the wall baseline atomically so changes take effect immediately without a discontinuous jump. A scale of 3600 makes one real second equal one simulated hour; 86400 makes one real second equal one simulated day. Manual `SetTime` (for deterministic replay) is unaffected.

## [v0.36.8] - 2026-03-18

### Fixed

- **EC2 `RunInstances` `TagSpecifications`** (`ec2_plugin.go`): tags specified in `TagSpecifications` with `ResourceType=instance` at launch time were silently dropped. They are now parsed from `TagSpecification.N.Tag.M.Key/Value` params and stored on the instance, so `DescribeInstances` returns them immediately after launch. Fixes issue #205.

## [v0.36.7] - 2026-03-18

### Fixed

- **CloudWatch `GetMetricData` Smithy RPC v2 CBOR protocol** (`parser.go`, `cloudwatch_plugin.go`): AWS SDK Go v2 cloudwatch v1.55+ sends `GetMetricData` via the Smithy RPC v2 CBOR transport (`POST /service/GraniteServiceVersion20100801/operation/GetMetricData`, `Content-Type: application/cbor`). Two fixes: (1) the parser now maps the Smithy service ID `GraniteServiceVersion20100801` to `monitoring` via a new `smithyServiceAliases` table; (2) the operation is extracted from the `/operation/<Name>` URL segment as a new 3rd strategy in `extractOperation`; (3) `getMetricData` detects `Content-Type: application/cbor` and returns an empty CBOR map `{}` (`0xa0`) instead of XML. Fixes issue #204.

## [v0.36.6] - 2026-03-18

### Added

- **EC2: `CreateImage` / `DescribeImages` (AMIs) / `DeregisterImage`** (`ec2_plugin.go`, `ec2_types.go`): full AMI lifecycle — `CreateImage` stores an AMI in state (state immediately `"available"`, tags supported via `TagSpecification.N.Tag.M.*`); `DescribeImages` lists AMIs by account/region with `tag:<key>` filter support; `DeregisterImage` removes the AMI. Fixes issue #203.
- **CloudWatch: `GetMetricData`** (`cloudwatch_plugin.go`): returns a valid empty `MetricDataResults` response. Callers that degrade gracefully on zero values work correctly. Fixes issue #202.
- **IAM: `ListInstanceProfiles`** (`iam_plugin.go`): returns an empty `InstanceProfiles` list with `IsTruncated: false`. Fixes issue #201.

## [v0.36.5] - 2026-03-17

### Fixed

- **Parser: empty-value query params** (`parser.go`): bare keys (e.g. `?uploads`, `?versions`) were correctly stored as `"1"`, but keys with an explicit empty value (e.g. `?prefix=`) were incorrectly also stored as `"1"`. The fix inspects the raw query string to distinguish the two cases, so `ListObjectsV2` with an empty `Prefix` now receives `""` as intended (issue #200).

## [v0.36.4] - 2026-03-17

### Added

- **EC2: `RebootInstances`** — no-op returning 200 OK (issue #193)
- **EC2: `CreateTags` / `DeleteTags`** — applies/removes tags on instances, VPCs, subnets, security groups, internet gateways, and route tables; `DescribeInstances` now includes tags in the `tagSet` XML element (issue #194)
- **EC2: `ModifyInstanceAttribute`** — supports `InstanceType.Value` changes; updated instance type is reflected in subsequent `DescribeInstances` responses (issue #195)
- **S3: `DeleteObjects`** — `POST /<bucket>?delete` multi-object delete; supports `<Quiet>true</Quiet>` to suppress the `<Deleted>` list in the response (issue #197)

### Fixed

- **Presigned URL service identification** (`parser.go`): when no `Authorization` header is present (presigned requests), the `X-Amz-Credential` query parameter is used to synthesise a credential scope, enabling correct service and region identification for all presigned S3 (and other) URLs (issue #196)
- **Budgets `CreateBudget`** (`parser.go`): the `AWSBudgetServiceGateway` X-Amz-Target prefix (used by mutation operations) was not in the alias table; only `AmazonBudgetServiceGateway` (used by `DescribeBudgets`) was. Added `"awsbudgetservicegateway": "budgets"` alias (issue #199)
- **`ListObjectsV2` `KeyCount`** (`s3_plugin.go`): `KeyCount` now correctly equals `len(Contents) + len(CommonPrefixes)` rather than `len(Contents)` only; the existing `CommonPrefixes` logic was correct (issue #198)

## [v0.36.3] - 2026-03-17

### Added

- **EC2 key pair operations** (`ec2_plugin.go`, `ec2_types.go`): `CreateKeyPair` (generates an EC P-256 key pair, returns PEM `KeyMaterial` and SHA-256 fingerprint), `DescribeKeyPairs` (with optional `KeyName` filter), `DeleteKeyPair` (by name or `KeyPairId`), and `ImportKeyPair` (accepts base64-encoded public key material). `RunInstances` now records `KeyName` on the instance and includes it in the `RunInstancesResponse` XML. Fixes issue #192.

## [v0.36.2] - 2026-03-17

### Fixed

- **S3 virtual-hosted style with `config.WithBaseEndpoint`** (`parser.go`): AWS SDK v2 prepends the bucket name to the custom base-endpoint host (e.g. `my-bucket.localhost:4566`). `normalizeS3VirtualHost` only handled `.amazonaws.com` hosts, so the bucket was never prepended to the request path and Substrate returned a 501. A new helper `normalizeS3CustomEndpointVirtualHost` fires after the service is identified as `"s3"` (via SigV4 credential scope from v0.36.1) and strips the first DNS label as the bucket name, normalising the path for all S3 plugins. Path-style requests (`localhost:4566/bucket/key`) already worked after v0.36.1. Fixes issue #191.

## [v0.36.1] - 2026-03-17

### Fixed

- **Service identification with `config.WithBaseEndpoint`** (`parser.go`): query-protocol services (STS, EC2, IAM, CloudWatch, ELB, …) now resolve correctly when the SDK is configured with a single base endpoint URL (e.g. `http://localhost:8080`) instead of per-service hostnames. `extractService` gains a 4th strategy: reads the service name from the SigV4 `Authorization` credential scope (`…/<region>/<service>/aws4_request`). Added `"ses"→"sesv2"` alias to cover SES v2 SigV4 scope. Fixes issue #190.

## [v0.36.0] - 2026-03-16

### Added

- **Lambda Docker execution** (`lambda_exec.go`): `LambdaExecutor` manages warm Lambda RIE containers via Docker CLI; supports ZIP-deployed and container-image functions; warm pool with configurable TTL; gracefully falls back to stub response `{"statusCode":200,"body":"null"}` when Docker is unavailable (issues #111, #112)
- **Lambda replay cache** (`lambda_exec.go`): `saveReplay`/`loadReplay` persist invocation results to state keyed by `sha256(functionARN|payload)`; `ReplayMode: "recorded"` returns cached responses without Docker (issue #113)
- **SQS ESM polling** (`lambda_plugin.go`): `createEventSourceMapping` starts a `sqsPollerLoop` goroutine when the ESM is `Enabled` and targets SQS; polls via `registry.RouteRequest`, invokes Lambda with an SQS records event, and deletes messages on 2xx; `Shutdown` stops all pollers (issue #115)
- **API Gateway proxy plugin** (`apigateway_plugin.go`): `APIGatewayProxyPlugin` (service `"execute-api"`) handles runtime invocations at `{apiId}.execute-api.{region}.amazonaws.com`; resolves v1 REST APIs and v2 HTTP APIs to their `AWS_PROXY` Lambda integrations; builds v1 and v2 proxy event payloads and parses proxy response shapes (issue #114)
- **RDS Postgres container executor** (`rds_exec.go`): `RDSExecutor.StartPostgres` launches `postgres:latest` via Docker, polls `pg_isready`, and stores the handle; `StopAll` / `StopContainer` clean up containers; `rds_plugin.go` wires the executor when `cfg.RDS.Engine == "container"` (issue #136)
- `LambdaCfg` and `RDSCfg` config structs in `config.go`; viper defaults (`lambda.docker_enabled=false`, `lambda.replay_mode=live`, `lambda.warm_pool_ttl=5m`, `rds.engine=stub`)
- `parser.go`: `extractServiceFromHost` now recognises `{apiId}.execute-api.{region}.amazonaws.com` and routes to the `execute-api` plugin; `extractRegionFromHost` correctly parses region from execute-api hosts; `ParseAWSRequest` injects `Host` header into the headers map so plugins can access it
- `RegisterDefaultPlugins` gains a `cfg *Config` parameter (nil-safe) for Docker feature wiring; callers updated

### Changed

- `LambdaPlugin.Initialize` accepts `"lambda_exec"` and `"registry"` options to enable Docker execution and ESM polling
- `RDSPlugin.Initialize` accepts `"rds_executor"` option to enable container-backed instances

## [v0.35.0] - 2026-03-16

### Added

- **Embedded debug web UI** (`ui.html`, `debug_ui.go`): single-file vanilla-JS SPA served at `GET /ui`; tabs for Events, State, Diff, Costs, and Export; left-sidebar service filter; clicking an event row loads state at that sequence point (issue #156)
- **`GET /v1/debug/events`**: returns a filtered event list (query params: `?service=`, `?stream=`, `?limit=`, `?after=`); bodies are stripped to keep the payload small (issue #156)
- **`GET /v1/debug/events/{seq}/state`**: replays all events up to sequence N into a fresh in-memory state and returns the snapshot as JSON; powered by the new `stateAtSequence` private helper (issue #157)
- **`GET /v1/debug/state/diff`**: computes a symmetric JSON diff between two sequence points (`?from=`, `?to=`); returns `{"added":…,"removed":…,"changed":…}` (issue #157)
- **`GET /v1/debug/costs`**: returns `CostSummary` JSON for an account or stream (`?account=`, `?stream=`) (issue #158)
- **`GET /v1/debug/export`**: generates and returns a standalone Go `*_test.go` file that replays recorded requests via `StartTestServer` (`?stream=`, `?package=`, `?test=`) (issue #159)
- **`GenerateTestFixture`** (`replay_export.go`): exported function that converts a `[]*Event` slice into a gofmt-formatted Go test file using `text/template` + `go/format` (issue #159)

## [v0.34.0] - 2026-03-15

### Added

- **RDS Aurora cluster support** (`rds_plugin.go`, `rds_types.go`): `CreateDBCluster`, `DescribeDBClusters`, `DeleteDBCluster` operations; `RDSDBCluster` type with writer/reader endpoints; `DBClusterAlreadyExistsFault` and `DBClusterNotFoundFault` error codes; state key `dbcluster:{acct}/{region}/{id}` (issue #133)
- **RDS `RestoreDBInstanceFromDBSnapshot`** (`rds_plugin.go`): restores a DB instance from an existing snapshot, copying engine and allocated storage; returns HTTP 200 with new instance details (issue #188)
- **`MSKPlugin`** — 39th built-in plugin; supports `CreateCluster`, `DescribeCluster`, `GetBootstrapBrokers`, `ListClusters`, `DeleteCluster` via MSK REST/JSON API at `/v1/clusters/...`; `ConflictException` (409) on duplicate cluster; `NotFoundException` (404) on missing cluster; synthetic broker endpoints for `GetBootstrapBrokers` (issue #138)
- `msk_types.go` — `MSKCluster`, `MSKBrokerNodeGroupInfo`, `MSKStorageInfo`, `MSKEBSStorageInfo` types; `mskNamespace = "msk"` constant; state key `cluster:{acct}/{region}/{name}`, index `cluster_ids:{acct}/{region}`
- `parser.go`: `"kafka": "msk"` alias so `Kafka_20181101.{Op}` target routes to the MSK plugin
- **SESv2 email capture** (`sesv2_plugin.go`, `sesv2_types.go`): `sendEmail` now persists a `SESv2CapturedEmail` to state (`captured_email:{acct}/{region}/{msgID}`) with To, From, Subject, and Body fields for test assertions (issue #140)
- **`GET /v1/emails` assertion endpoint** (`server.go`): lists all captured SESv2 outbound emails as JSON; accepts optional `?to=` and `?subject=` substring filters; returns `{"Emails": [...], "Count": N}`
- `betty_cfn_v34_plugins.go`: `deployRDSDBCluster` helper for `AWS::RDS::DBCluster` (priority 3, Ref = DBClusterIdentifier, GetAtt `Endpoint.Address`); `deployMSKCluster` helper for `AWS::MSK::Cluster` (priority 3, Ref = ClusterARN)
- `betty_cfn.go`: `AWS::RDS::DBCluster` at priority 3 and `AWS::MSK::Cluster` at priority 3 added to `typePriority` map and dispatch switch
- `costs.go`: `rds/CreateDBCluster` ($0.0001), `rds/RestoreDBInstanceFromDBSnapshot` ($0.0001), `msk/CreateCluster` ($0.0002), `msk/GetBootstrapBrokers` ($0.000001)

## [v0.32.0] - 2026-03-15

### Added

- **Generic CFN fallback** (`betty_cfn.go`): unknown `AWS::*` resource types now produce a synthetic ARN and store their properties in the `cfn_stub` state namespace instead of being silently skipped; `Ref` resolves to the logical ID (issue #146)
- `betty_cfn_v32_plugins.go`: eleven stub deploy helpers for new CFN resource types — `deployOpenSearchDomain` (`AWS::OpenSearchService::Domain`, priority 2), `deployWAFv2WebACL` (`AWS::WAFv2::WebACL`, priority 2), `deployBackupBackupPlan` (`AWS::Backup::BackupPlan`, priority 2), `deployCodeBuildProject` (`AWS::CodeBuild::Project`, priority 2), `deployCodePipelinePipeline` (`AWS::CodePipeline::Pipeline`, priority 3), `deployCodeDeployDeploymentGroup` (`AWS::CodeDeploy::DeploymentGroup`, priority 3), `deployCloudTrailTrail` (`AWS::CloudTrail::Trail`, priority 2), `deployConfigConfigRule` (`AWS::Config::ConfigRule`, priority 3), `deployConfigConfigurationRecorder` (`AWS::Config::ConfigurationRecorder`, priority 2), `deployTransferServer` (`AWS::Transfer::Server`, priority 2), `deployAthenaWorkGroup` (`AWS::Athena::WorkGroup`, priority 2) (issues #147–#150)
- `betty_cfn_v32_test.go`: 13 table-driven tests covering the generic fallback, all 11 new stub types, and a Glue regression guard
- `cfnStubNamespace = "cfn_stub"` constant for generic resource property storage

## [v0.31.0] - 2026-03-15

### Added

- `AppSyncPlugin` — 39th built-in plugin; supports `CreateGraphqlApi`, `ListGraphqlApis`, `GetGraphqlApi`, `UpdateGraphqlApi`, `DeleteGraphqlApi`, `CreateDataSource`, `ListDataSources`, `GetDataSource`, `UpdateDataSource`, `DeleteDataSource`, `CreateResolver`, `ListResolvers`, `GetResolver`, `UpdateResolver`, `DeleteResolver`, `CreateFunction`, `ListFunctions`, `GetFunction`, `DeleteFunction`, `StartSchemaCreation`, `GetIntrospectionSchema`, and `ExecuteGraphQL` (stub) (issues #142–#145)
- `appsync_types.go` — `AppSyncGraphQLApi`, `AppSyncDataSource`, `AppSyncResolver`, `AppSyncFunction` types; `parseAppSyncOperation` path router; state key helpers; `generateAppSyncAPIID` / `generateAppSyncFunctionID`
- `parser.go`: `extractServiceFromHost` now recognises AppSync execution endpoints (`{apiId}.appsync-api.{region}.amazonaws.com`) and routes them to the `appsync` plugin
- `betty_cfn_v31_plugins.go`: `deployAppSyncGraphQLApi`, `deployAppSyncDataSource`, `deployAppSyncResolver`, `deployAppSyncFunction` deploy helpers for `AWS::AppSync::GraphQLApi` (priority 2), `AWS::AppSync::DataSource` (priority 3), `AWS::AppSync::Resolver` / `AWS::AppSync::FunctionConfiguration` (priority 4)
- `costs.go`: AppSync pricing — `appsync/ExecuteGraphQL` and `appsync/CreateGraphqlApi` at $0.000004 each ($4.00 per million operations)
- `doc.go`: updated plugin count from 38 to 39 and added AppSync to service description

## [v0.30.0] - 2026-03-15

### Added

- `ServiceQuotasPlugin` — 38th built-in plugin; supports `ListServices`, `ListServiceQuotas`, `GetServiceQuota`, `GetAWSDefaultServiceQuota`, `RequestServiceQuotaIncrease`, `ListRequestedServiceQuotaChangesByService`, and `GetRequestedServiceQuotaChange`; covers Lambda, S3, DynamoDB, SQS, and nine other AWS services (issue #119)
- `servicequotas_types.go` — `ServiceQuota` and `QuotaIncrease` types with built-in default quota table
- **S3 versioning** (`s3_plugin.go`): `PutBucketVersioning`, `GetBucketVersioning`, `ListObjectVersions`; version-aware `PutObject` (generates `x-amz-version-id`), `GetObject` (accepts `?versionId`), and `DeleteObject` (delete markers + permanent version deletion) (issue #126)
- **S3 lifecycle** (`s3_plugin.go`): `PutBucketLifecycleConfiguration`, `GetBucketLifecycleConfiguration`, `DeleteBucketLifecycle` — config round-trip storage (issue #127)
- **SQS FIFO** (`sqs_plugin.go`): `MessageGroupId` enforcement, 5-minute deduplication window via `MessageDeduplicationId` or content-based SHA-256, `sqsFIFODedupEntry` state type (issue #128)
- **DynamoDB Streams** (`dynamodb_plugin.go`): ring-buffer stream records (max 1000) with `appendStreamRecord` hooks in `putItem`/`updateItem`/`deleteItem`; real implementations of `DescribeStream`, `GetShardIterator` (supports TRIM_HORIZON/LATEST/AT_SEQUENCE_NUMBER/AFTER_SEQUENCE_NUMBER), and `GetRecords` replacing previous stubs; `DynamoDBStreamRecord` and `DynamoDBStreamCursor` types (issue #129)
- **DynamoDB PartiQL** (`dynamodb_plugin.go`): `ExecuteStatement` and `BatchExecuteStatement` with `tokenizePartiQL` supporting `SELECT * FROM`, `INSERT INTO … VALUE`, `UPDATE … SET`, and `DELETE FROM` (issue #130)
- **Lambda ESM** (`lambda_plugin.go`): `CreateEventSourceMapping`, `ListEventSourceMappings`, `GetEventSourceMapping`, `UpdateEventSourceMapping`, `DeleteEventSourceMapping`; `ESMConfig` type with `esm:{uuid}` and `esm_ids:{functionARN}` state keys (issue #131)
- `betty_cfn.go`: `deployLambdaEventSourceMapping` helper for `AWS::Lambda::EventSourceMapping` CFN resources (priority 5); `deployS3Bucket` extended to call `PutBucketVersioning` when `VersioningConfiguration.Status = Enabled`
- `doc.go`: updated plugin count from 37 to 38 and expanded service description

## [v0.29.0] - 2026-03-15

### Added

- `docker-compose.yml` — turnkey local development deployment; SQLite state persisted in a named Docker volume (issue #187)
- `configs/substrate-local.yaml` — ready-to-use Substrate config mounted into the Compose container (issue #187)
- `deploy/ecs/task-definition.json` — ECS Fargate task definition template with EFS volume and CloudWatch logging (issue #187)
- `deploy/ecs/README.md` — step-by-step ECS Fargate + ALB deployment guide (issue #187)
- `deploy/k8s/deployment.yaml` — Kubernetes Deployment + ClusterIP Service (issue #187)
- `deploy/k8s/configmap.yaml` — Substrate config as a Kubernetes ConfigMap (issue #187)
- `deploy/k8s/pvc.yaml` — PersistentVolumeClaim for SQLite data (issue #187)
- `deploy/README.md` — comparison table and quickstarts for all three deployment options (issue #187)
- `Makefile`: `compose-up`, `compose-down`, `compose-logs` targets (issue #187)
- `docs/getting-started.md`: Docker Compose quickstart added as first Install option (issue #187)

### Fixed

- `cmd/substrate/main.go`: `TimeController` is now constructed before `EventStore` and passed via `WithTimeController`; the server clock and event-store clock are now the same instance (issue #187)

## [v0.28.0] - 2026-03-15

### Added

- SES v2 plugin (issue #180): CreateEmailIdentity, ListEmailIdentities, GetEmailIdentity, SendEmail, DeleteEmailIdentity
- Kinesis Data Firehose plugin (issue #181): CreateDeliveryStream, DescribeDeliveryStream, PutRecord, PutRecordBatch, ListDeliveryStreams, DeleteDeliveryStream
- Betty CFN: AWS::SES::EmailIdentity, AWS::KinesisFirehose::DeliveryStream (issue #182)
- Documentation overhaul: README service matrix updated to all 37 plugins (issue #175)
- docs/getting-started.md: new first-user tutorial (issue #176)
- docs/services.md: complete service reference for all 37 plugins (issue #177)
- docs/testing-guide.md: Go testing patterns guide (issue #178)
- `Server.Serve(ctx, net.Listener)` — accepts an already-bound listener, eliminating the port TOCTOU race in `StartTestServer` (issue #183)
- `WithTimeController(tc)` EventStoreOption — event timestamps and cost-forecast windows now use the simulated clock rather than `time.Now()` (issue #185)

### Fixed

- `StartTestServer` now passes the open `net.Listener` directly to `srv.Serve`, eliminating the TOCTOU race between port reservation and bind (issue #183)
- IAMPlugin and Route53Plugin now use `TimeController.Now()` for all business-visible timestamps (CreateDate, SubmittedAt) instead of `time.Now()` (issue #184)
- `GetCostForecast` now uses `EventStore.now()` (respects the controlled clock) for the observation window and `ComputedAt` field (issue #185)
- `authz.go` SQS ARN builder: guard against empty `name` segment after splitting a trailing-slash `QueueUrl`, preventing a spurious `""` queue name lookup (issue #186)

## [v0.27.2] - 2026-03-14

### Fixed

- **CI lint:** Pinned golangci-lint to v2.11.3 in `.github/workflows/ci.yml` and removed `install-mode: goinstall`. The v2 pre-built binary supports Go 1.26 natively and correctly validates the v2 config schema (`version`, `formatters`, `linters.default`) used in `.golangci.yml`.

## [v0.27.1] - 2026-03-14

### Fixed

- **CI lint:** `golangci-lint` pre-built binary v1.64.8 (compiled with Go 1.24) rejected `go 1.26` in `go.mod`. Fixed by setting `install-mode: goinstall` in `.github/workflows/ci.yml` so golangci-lint is compiled from source with the installed Go 1.26.
- **CI e2e:** `test/e2e/go.mod` was missing OTel and gRPC transitive dependencies introduced since v0.17.0; `go test` failed with `go: updates to go.mod needed`. Fixed by running `go mod tidy` and committing the result; added an explicit tidy step to the e2e CI job to prevent future drift.

## [v0.27.0] - 2026-03-14

### Added

- **Cost Explorer plugin:** `CEPlugin` handles JSON-target requests (`X-Amz-Target: AWSInsightsIndexService.{Op}`) on `ce.us-east-1.amazonaws.com`. Parser alias `"awsinsightsindexservice" → "ce"`. Operations: `GetCostAndUsage` (derives per-service cost buckets from `EventStore.GetCostSummary`), `GetCostForecast` (uses linear-regression projection from `EventStore.GetCostForecast`), `GetDimensionValues` (scans EventStore for unique service names). No persistent state — all data derived from EventStore. `RegisterDefaultPlugins` now accepts an optional `store *EventStore` parameter passed to CEPlugin; passing `nil` returns valid empty responses (#121).

- **Budgets plugin:** `BudgetsPlugin` handles JSON-target requests (`X-Amz-Target: AmazonBudgetServiceGateway.{Op}`) on `budgets.amazonaws.com`. Parser alias `"budgetservicegateway" → "budgets"`. Operations: `CreateBudget`, `DescribeBudgets`, `DescribeBudget`, `UpdateBudget`, `DeleteBudget`. State keys: `budget:{acct}/{name}`, `budget_names:{acct}`. Error code for not-found: `NotFoundException`; duplicate: `DuplicateRecordException`. Cost entry: `budgets/CreateBudget = $0.00001` (#122).

- **Health plugin:** `HealthPlugin` provides a stub of the AWS Health API. Parser alias `"healthservice" → "health"`. Operations: `DescribeEvents`, `DescribeEventDetails`, `DescribeAffectedEntities`, `DescribeEventAggregates` — all return valid empty responses satisfying the SDK shape. No persistent state, no cost entries (#123).

- **Organizations plugin:** `OrganizationsPlugin` handles JSON-target requests (`X-Amz-Target: Organizations_20161128.{Op}`) on `organizations.*.amazonaws.com`. Operations: `DescribeOrganization` (auto-creates org + master account on first call), `ListAccounts`, `DescribeAccount`, `ListRoots` (returns single root with SCP enabled), `CreateAccount` (status `SUCCEEDED`). State keys: `org:{acct}`, `account:{id}`, `account_ids:{acct}`. Error code for not-found: `AccountNotFoundException` (#124).

- **Betty CFN: AWS::Budgets::Budget:** `deployBudgetsBudget` helper in `betty_cfn_v27_plugins.go`. CFN type priority 3. `Ref` resolves to `BudgetName` (#125).

## [v0.26.0] - 2026-03-14

### Added

- **EFS plugin:** `EFSPlugin` handles REST/JSON requests on `elasticfilesystem.{region}.amazonaws.com` at `/2015-02-01/...` paths. Operations: file system CRUD (`CreateFileSystem` returns HTTP 201, `DescribeFileSystems`, `UpdateFileSystem`, `DeleteFileSystem` returns 204), access point CRUD (`CreateAccessPoint`, `DescribeAccessPoints`, `DeleteAccessPoint` returns 204), mount target CRUD (`CreateMountTarget`, `DescribeMountTargets`, `DeleteMountTarget` returns 204), tagging (`TagResource`, `ListTagsForResource`, `UntagResource`). File systems start in `available` state immediately. `parseEFSOperation` maps HTTP method + path to operation names. Tags use `[]EFSTag{Key, Value}` slice. Cost entries: `elasticfilesystem/CreateFileSystem = $0.00003`, `elasticfilesystem/CreateAccessPoint = $0.00001`, `elasticfilesystem/CreateMountTarget = $0.00001`.

- **Glue plugin:** `GluePlugin` handles JSON-target requests (`X-Amz-Target: AWSGlue.{Op}`) on `glue.{region}.amazonaws.com`. Operations: database CRUD (`CreateDatabase`, `GetDatabase`, `GetDatabases`, `UpdateDatabase`, `DeleteDatabase`), table CRUD (`CreateTable`, `GetTable`, `GetTables`, `UpdateTable`, `DeleteTable`), connection CRUD (`CreateConnection`, `GetConnection`, `GetConnections`, `UpdateConnection`, `DeleteConnection`), crawler CRUD + start/stop (`CreateCrawler`, `GetCrawler`, `GetCrawlers`, `StartCrawler`, `StopCrawler`, `UpdateCrawler`, `DeleteCrawler`), job CRUD + runs (`CreateJob`, `GetJob`, `GetJobs`, `UpdateJob`, `DeleteJob`, `StartJobRun`, `GetJobRun`, `GetJobRuns`), tagging (`TagResource`, `UntagResource`, `GetTags`). All resources created immediately in final state (crawlers in `READY`, job runs in `SUCCEEDED`). Tags use `map[string]string`. Error code for not-found: `EntityNotFoundException`. Cost entries: `glue/CreateDatabase = $0.00002`, `glue/CreateJob = $0.0001`, `glue/StartJobRun = $0.0001`, `glue/CreateCrawler = $0.0001`.

- **Betty CFN: EFS and Glue resource types:** `deployResource` switch extended with `AWS::EFS::FileSystem` (priority 2), `AWS::EFS::AccessPoint` (priority 3), `AWS::EFS::MountTarget` (priority 4), `AWS::Glue::Database` (priority 2), `AWS::Glue::Connection` (priority 2), `AWS::Glue::Table` (priority 3), `AWS::Glue::Crawler` (priority 3), `AWS::Glue::Job` (priority 3). EFS resources set `PhysicalID` and `ARN` from response body.

- **Tagging API:** Extended `GetResources`, `TagResources`, `UntagResources` with scan functions for EFS file systems (`elasticfilesystem:file-system`) and Glue databases (`glue:database`). `resolveARN` handles `elasticfilesystem` (file-system and access-point) and `glue` (database, job, crawler, connection) ARN formats. `mergeTags` handles EFS (uses `[]EFSTag` slice) and Glue (uses `map[string]string`).

- **ABAC `buildResourceARN`:** Added cases for `elasticfilesystem` (extracts resource ID from path `/2015-02-01/file-systems/{id}`) and `glue` (uses `req.Params["Name"]`).

## [v0.25.0] - 2026-03-14

### Added

- **RDS plugin:** `RDSPlugin` handles query-protocol requests on `rds.{region}.amazonaws.com`. Operations: DB instance CRUD (`CreateDBInstance`, `DescribeDBInstances`, `ModifyDBInstance`, `DeleteDBInstance`, `StartDBInstance`, `StopDBInstance`, `RebootDBInstance`), DB snapshot CRUD (`CreateDBSnapshot`, `DescribeDBSnapshots`, `DeleteDBSnapshot`), subnet group CRUD (`CreateDBSubnetGroup`, `DescribeDBSubnetGroups`, `DeleteDBSubnetGroup`), parameter group CRUD (`CreateDBParameterGroup`, `DescribeDBParameterGroups`, `DeleteDBParameterGroup`), tagging (`ListTagsForResource`, `AddTagsToResource`, `RemoveTagsFromResource`). Instances start in `available` status immediately. Engine-appropriate port stubs (3306 MySQL, 5432 Postgres, 1433 MSSQL, etc.). Cost entries: `rds/CreateDBInstance = $0.0001`, `rds/CreateDBSnapshot = $0.00002`, `rds/ModifyDBInstance = $0.0001` (#160, #161).

- **ElastiCache plugin:** `ElastiCachePlugin` handles query-protocol requests on `elasticache.{region}.amazonaws.com`. Operations: cache cluster CRUD (`CreateCacheCluster`, `DescribeCacheClusters`, `ModifyCacheCluster`, `DeleteCacheCluster`), replication group CRUD (`CreateReplicationGroup`, `DescribeReplicationGroups`, `ModifyReplicationGroup`, `DeleteReplicationGroup`), subnet group CRUD, parameter group CRUD, tagging. Clusters start in `available` status. Redis (port 6379) and Memcached (port 11211) endpoints. Cost entries: `elasticache/CreateCacheCluster = $0.0001`, `elasticache/CreateReplicationGroup = $0.0001` (#163, #164).

- **Betty CFN: RDS and ElastiCache resource types:** `deployResource` switch extended with `AWS::RDS::DBSubnetGroup` (priority 2), `AWS::RDS::DBParameterGroup` (priority 2), `AWS::RDS::DBInstance` (priority 3), `AWS::ElastiCache::SubnetGroup` (priority 2), `AWS::ElastiCache::ParameterGroup` (priority 2), `AWS::ElastiCache::CacheCluster` (priority 3), `AWS::ElastiCache::ReplicationGroup` (priority 3). GetAtt support: `DBInstance.Endpoint.Address`, `DBInstance.Endpoint.Port`, `CacheCluster.ConfigurationEndpoint.Address`, `CacheCluster.ConfigurationEndpoint.Port`, `CacheCluster.RedisEndPoint.Address`, `CacheCluster.RedisEndPoint.Port`, `ReplicationGroup.PrimaryEndPoint.Address`, `ReplicationGroup.PrimaryEndPoint.Port` (#162, #165).

- **Tagging API:** Extended `GetResources`, `TagResources`, `UntagResources` with scan functions for RDS DB instances (`rds:db`) and ElastiCache clusters (`elasticache:cluster`). ARN resolution and tag merge/remove for both services (#166).

- **ABAC:** `buildResourceARN` in `authz.go` extended with `rds` and `elasticache` cases for attribute-based access control on DB instances and cache clusters (#167).

## [v0.24.0] - 2026-03-14

### Added

- `/_localstack/health` and `/_localstack/info` endpoints returning LocalStack-compatible service status JSON — enables Prism and other tools that poll for service readiness (#109).
- `POST /v1/state/reset` HTTP endpoint for wiping all emulator state between tests (#108).
- `StartTestServer(t *testing.T) *TestServer` Go helper for integration tests — starts an in-process server on a random port, registers all plugins, and registers `t.Cleanup` for automatic shutdown (#108).
- `RegisterDefaultPlugins` exported function extracted from the server binary so testing helpers and custom embeddings can initialise the same plugin set (#108).
- `substratelocal` CLI wrapper binary — injects `AWS_ENDPOINT_URL`, `LOCALSTACK_ENDPOINT`, and stub credentials into child process environment (#107).
- Multi-arch Docker image (`linux/amd64` + `linux/arm64`) build support via `Dockerfile` and `.github/workflows/docker.yml`, published to `ghcr.io/scttfrdmn/substrate` on tag push (#106).
- `docs/endpoint-configuration.md` — endpoint configuration reference for AWS CLI, Go SDK v2, boto3, Terraform, CDK, Prism, and Docker Compose (#110).

## [v0.23.0] - 2026-03-09

### Added

- **Kinesis Data Streams plugin:** `KinesisPlugin` handles JSON-protocol requests (`X-Amz-Target: Kinesis_20131202.{Op}`). Operations: stream CRUD (`CreateStream`, `DeleteStream`, `DescribeStream`, `DescribeStreamSummary`, `ListStreams`), shard iteration (`GetShardIterator`, `GetRecords`), producer operations (`PutRecord`, `PutRecords`), enhanced fan-out (`RegisterStreamConsumer`, `DeregisterStreamConsumer`, `DescribeStreamConsumer`), tagging. Cost entries: `kinesis/PutRecord = $0.000000014`, `kinesis/PutRecords = $0.000000014`.

- **CloudFront plugin:** `CloudFrontPlugin` handles REST/XML requests on `cloudfront.amazonaws.com` (global service). Operations: distribution CRUD (`CreateDistribution` → HTTP 201, `GetDistribution`, `UpdateDistribution`, `DeleteDistribution`), `ListDistributions`, CloudFront Origin Access Identity (OAI) CRUD, tagging. Distributions start in `InProgress` state. GetAtt `Distribution.DomainName` supported.

- **Betty CFN: Kinesis and CloudFront resource types:** `deployResource` switch extended with `AWS::Kinesis::Stream` (priority 2), `AWS::CloudFront::CloudFrontOriginAccessIdentity` (priority 2), `AWS::CloudFront::Distribution` (priority 3). GetAtt `Distribution.DomainName` and `Stream.StreamArn` supported.

- **Tagging API: scan and resolve for Kinesis:** `TaggingPlugin` now scans Kinesis streams. `resolveARN` and `mergeTags` extended to handle `kinesis` namespace.

- **ABAC: `buildResourceARN` for Kinesis:** `authz.go` `buildResourceARN` extended with case for `kinesis`.

- **Kinesis and CloudFront plugins registered in `cmd/substrate/main.go`.**

## [v0.22.0] - 2026-03-09

### Added

- **Cognito User Pools plugin:** `CognitoIDPPlugin` handles JSON-protocol requests (`X-Amz-Target: AWSCognitoIdentityProviderService.{Op}`). Operations: user pool CRUD, user pool client CRUD, domain, groups, admin user management, `ListUsers`, `InitiateAuth` (stub JWT tokens), `RespondToAuthChallenge`, `SignUp`/`ConfirmSignUp`, MFA config. Pool IDs use format `{region}_{12-char alphanum}`. Cost entry: `cognito-idp/InitiateAuth = $0.000055`.

- **Cognito Identity Pools plugin:** `CognitoIdentityPlugin` handles JSON-protocol requests (`X-Amz-Target: AWSCognitoIdentityService.{Op}`). Operations: identity pool CRUD, `GetId`, `GetCredentialsForIdentity` (stub AWS credentials), `SetIdentityPoolRoles`/`GetIdentityPoolRoles`.

- **Betty CFN: Cognito resource types:** `deployResource` switch extended with `AWS::Cognito::UserPool`, `AWS::Cognito::UserPoolClient`, `AWS::Cognito::UserPoolGroup`, `AWS::Cognito::UserPoolDomain`, `AWS::Cognito::IdentityPool`, `AWS::Cognito::IdentityPoolRoleAttachment`. GetAtt `UserPool.ProviderName` and `UserPool.ProviderURL` supported.

- **Tagging API: scan and resolve for Cognito:** `TaggingPlugin` now scans Cognito user pools. `resolveARN` and `mergeTags` extended to handle `cognito-idp` namespace.

- **ABAC: `buildResourceARN` for Cognito:** `authz.go` `buildResourceARN` extended with cases for `cognito-idp` and `cognito-identity`. Parser alias `"awscognitoidentityproviderservice" → "cognito-idp"` added.

- **CognitoIDP and CognitoIdentity plugins registered in `cmd/substrate/main.go`.**

## [v0.21.0] - 2026-03-09

### Added

- **ECR plugin:** `ECRPlugin` handles JSON-protocol requests (`X-Amz-Target: AmazonEC2ContainerRegistry_V1_1_0.{Op}`). Operations: repository CRUD, `PutImage`, `BatchGetImage`, `DescribeImages`, `BatchDeleteImage`, `ListImages`, `GetAuthorizationToken`, lifecycle policy, repository policy, tagging. Cost entry: `ecr/PutImage = $0.000001`.

- **ECS plugin:** `ECSPlugin` handles JSON-protocol requests (`X-Amz-Target: AmazonEC2ContainerServiceV20141113.{Op}`). Operations: cluster CRUD, task definition register/deregister/describe/list, service create/update/describe/delete/list, `RunTask`/`StopTask`/`DescribeTasks`/`ListTasks`, tagging. Cost entry: `ecs/RunTask = $0.000025`.

- **Betty CFN: ECR and ECS resource types:** `deployResource` switch extended with `AWS::ECR::Repository`, `AWS::ECR::LifecyclePolicy`, `AWS::ECS::Cluster`, `AWS::ECS::TaskDefinition`, `AWS::ECS::Service`, `AWS::ECS::CapacityProvider`. GetAtt `ECRRepository.RepositoryUri` supported.

- **Tagging API: scan and resolve for ECR and ECS:** `TaggingPlugin` now scans ECR repositories and ECS clusters. `resolveARN` and `mergeTags` extended to handle `ecr` and `ecs` namespaces. Helper functions `ecsTagsToTaggingTags` and `mergeECSTags` added.

- **ABAC: `buildResourceARN` for ECR and ECS:** `authz.go` `buildResourceARN` extended with cases for `ecr` and `ecs`. Parser aliases `"ec2containerservicev20141113" → "ecs"` and `"ec2containerregistry" → "ecr"` added.

- **ECR and ECS plugins registered in `cmd/substrate/main.go`.**

## [v0.20.0] - 2026-03-09

### Added

- **Step Functions plugin:** `StepFunctionsPlugin` handles JSON-protocol requests (`X-Amz-Target: AmazonStates.{Op}`). Operations: state machine CRUD, `StartExecution` (returns RUNNING), `StopExecution`, `DescribeExecution` (RUNNING → SUCCEEDED on first call), `ListExecutions`, `GetExecutionHistory` (stub events), Activity CRUD, `TagResource`/`UntagResource`/`ListTagsForResource`. Cost entry: `states/StartExecution = $0.000025`.

- **Betty CFN: Step Functions resource types:** `deployResource` switch extended with `AWS::StepFunctions::StateMachine` (priority 4) and `AWS::StepFunctions::Activity` (priority 3). GetAtt `StateMachine.Name` supported.

- **Tagging API: scan and resolve for Step Functions:** `TaggingPlugin` now scans Step Functions state machines. `resolveARN` and `mergeTags` extended to handle `states` namespace.

- **ABAC: `buildResourceARN` for Step Functions:** `authz.go` `buildResourceARN` extended with case for `states`.

- **StepFunctions plugin registered in `cmd/substrate/main.go`.**

## [v0.19.0] - 2026-03-09

### Added

- **ACM plugin:** `ACMPlugin` handles JSON-protocol requests (`X-Amz-Target: CertificateManager.{Op}`). Operations: `RequestCertificate` (immediately sets status `ISSUED`), `DescribeCertificate`, `DeleteCertificate`, `ListCertificates`, `AddTagsToCertificate`, `RemoveTagsFromCertificate`, `ListTagsForCertificate`, `RenewCertificate` (no-op). Parser alias `"certificatemanager" → "acm"` added.

- **API Gateway v1 plugin:** `APIGatewayPlugin` handles path-based REST API requests on `apigateway.{region}.amazonaws.com`. Operations cover RestApis, Resources, Methods, Integrations, Deployments, Stages, Authorizers, ApiKeys, UsagePlans, DomainNames, and BasePathMappings. `CreateRestApi` auto-creates a root resource `/` and returns `RootResourceId`. `CreateStage` returns `InvokeURL` via Betty CFN GetAtt. Cost entry: `apigateway/CreateDeployment = $0.0000035`.

- **API Gateway v2 plugin:** `APIGatewayV2Plugin` handles HTTP/WebSocket API requests on `apigatewayv2.{region}.amazonaws.com` at `/v2/apis/...`. Operations cover Apis, Routes, Integrations, Stages, Authorizers, Deployments, DomainNames, and ApiMappings. Cost entry: `apigatewayv2/CreateApi = $0.000001`.

- **Betty CFN: API Gateway and ACM resource types:** `deployResource` switch extended with 15 new resource types for `AWS::CertificateManager::Certificate`, `AWS::ApiGateway::*`, and `AWS::ApiGatewayV2::*`. Key GetAtts: `RestApi.RootResourceId`, `Stage.InvokeURL`. `DeployedResource` gains `Metadata map[string]interface{}` field for GetAtt-resolvable attributes.

- **Tagging API: scan and resolve for API Gateway:** `TaggingPlugin` now scans API Gateway REST APIs. `resolveARN` and `mergeTags` extended to handle `apigateway` namespace.

- **ABAC: `buildResourceARN` for new services:** `authz.go` `buildResourceARN` extended with cases for `apigateway`, `apigatewayv2`, and `acm`.

- **ACM, APIGateway, APIGatewayV2 plugins registered in `cmd/substrate/main.go`:** registered in dependency order after CloudWatch.

## [v0.18.0] - 2026-03-09

### Added

- **(#67) CloudWatch Logs plugin:** New `CloudWatchLogsPlugin` handles JSON-protocol (`application/x-amz-json-1.1`) requests identified by `X-Amz-Target: Logs_20140328.{Op}`. Operations: `CreateLogGroup`, `DeleteLogGroup`, `DescribeLogGroups` (prefix filter + base64 pagination), `CreateLogStream`, `DeleteLogStream`, `DescribeLogStreams`, `PutLogEvents`, `GetLogEvents` (time-range filter + pagination), `FilterLogEvents` (multi-stream substring pattern match). State keys: `loggroup:{acct}/{region}/{name}`, `logstream:{acct}/{region}/{group}/{stream}`, `logevents:{acct}/{region}/{group}/{stream}`. Package-level helpers `updateStringIndex`, `removeFromStringIndex`, `loadStringIndex` manage sorted `[]string` JSON indexes used by all three observability plugins.

- **(#68) EventBridge plugin:** New `EventBridgePlugin` handles JSON-protocol requests identified by `X-Amz-Target: AmazonEventBridge.{Op}`. Parser alias `"events" → "eventbridge"` added to `targetServiceAliases` (applied in both `extractServiceFromTarget` and `extractServiceFromHost`). Operations: `PutRule`, `DeleteRule` (validates no targets attached), `ListRules` (prefix filter + pagination), `DescribeRule`, `PutTargets` (merge by ID), `RemoveTargets`, `ListTargetsByRule`, `PutEvents` (validates Source/DetailType/Detail; ring buffer of last 100 events), `ListEventBuses` (returns default bus).

- **(#69) CloudWatch Alarms plugin:** New `CloudWatchPlugin` handles query-protocol (`Action=` param) requests on `monitoring.{region}.amazonaws.com`. Service name: `"monitoring"`. `"monitoring"` added to the server query→JSON rewrite condition. XML responses under `http://monitoring.amazonaws.com/doc/2010-08-01/` namespace. Operations: `PutMetricAlarm` (initial state `INSUFFICIENT_DATA`; preserves existing state on update), `DeleteAlarms` (bulk by `AlarmNames.member.N`), `DescribeAlarms` (filter by name list and/or `StateValue`), `DescribeAlarmsForMetric` (filter by `MetricName`+`Namespace`), `SetAlarmState`, `EnableAlarmActions`, `DisableAlarmActions`. Helper `parseMemberList` extracts query-style `.member.N` arrays.

- **(#70) Lambda auto-creates CloudWatch Logs log group:** `LambdaPlugin.createFunction` now calls `autoCreateLambdaLogGroup` after storing the function. This writes the `/aws/lambda/{name}` log group directly to state (bypassing the registry to avoid circular dependency), matching real AWS behaviour.

- **(#71) Betty CFN: Logs, Events, CloudWatch resource types:** `deployResource` switch extended with `AWS::Logs::LogGroup` (priority 2, dispatches `CreateLogGroup`), `AWS::Logs::LogStream` (priority 3), `AWS::Events::Rule` (priority 4, dispatches `PutRule`), `AWS::CloudWatch::Alarm` (priority 4, dispatches `PutMetricAlarm`). All four set `PhysicalID` and `ARN` so `Ref` and `GetAtt Arn` resolve correctly in subsequent resources.

- **(#72) Cost entries for observability services:** `logs/PutLogEvents` $0.0000005 (~$0.50/GB approximate), `eventbridge/PutEvents` $0.000001 ($1.00/M events), `monitoring/PutMetricAlarm` $0.10 ($0.10/alarm/month charged on creation).

## [v0.17.0] - 2026-03-09

### Added

- **(#65) Request latency histogram:** `MetricsCollector` now tracks `substrate_request_duration_seconds` as a Prometheus histogram with 12 default buckets (1 ms–10 s). New `RecordLatency(service, operation string, d time.Duration)` method; wired in `server.go` after every request completes. Histogram output follows the standard Prometheus text format with cumulative `_bucket`, `_sum`, and `_count` lines sorted by service/operation.

- **(#66) Enriched OTel spans:** `Tracer.StartRequest` now returns `(context.Context, trace.Span)` instead of `(context.Context, func())`, exposing the live span to `server.go` for attribute decoration and error recording. `server.go` sets `aws.region` and `aws.account_id` attributes immediately after span creation. New package-level `RecordSpanError(span trace.Span, err error)` helper (nil-safe) marks the span with `codes.Error` and calls `span.RecordError`; called on any non-nil `routeErr`.

## [v0.16.0] - 2026-03-09

### Added

- **(v0.16.0) SNS plugin:** New `SNSPlugin` handles query-protocol requests on `sns.{region}.amazonaws.com`. Operations: `CreateTopic` (idempotent), `DeleteTopic`, `ListTopics`, `Subscribe`, `Unsubscribe`, `ListSubscriptions`, `ListSubscriptionsByTopic`, `Publish` (fan-out to SQS `SendMessage` and Lambda POST `/invocations`), `SetTopicAttributes`, `GetTopicAttributes`, `TagResource`, `UntagResource`, `ListTagsForResource`. S3 `fireNotifications` extended to dispatch `TopicConfigurations` to SNS. Betty CFN support: `AWS::SNS::Topic` (priority 3), `AWS::SNS::Subscription` (priority 4), `AWS::SNS::TopicPolicy` (priority 4). Cost entry added for `sns/Publish`.

- **(v0.16.0) Secrets Manager plugin:** New `SecretsManagerPlugin` handles JSON-protocol (`application/x-amz-json-1.1`) requests identified by `X-Amz-Target: secretsmanager.{Op}`. Operations: `CreateSecret` (409 on duplicate), `GetSecretValue` (404 on missing), `PutSecretValue` (generates new versionID each call), `DeleteSecret`, `ListSecrets`, `DescribeSecret`, `UpdateSecret`, `RotateSecret` (sets `RotationEnabled: true`), `TagResource`, `UntagResource`. Betty CFN support: `AWS::SecretsManager::Secret` (priority 2), `AWS::SecretsManager::RotationSchedule` (priority 5), `AWS::SecretsManager::SecretTargetAttachment` (priority 5, stub). Cost entries added for `CreateSecret`, `GetSecretValue`, `PutSecretValue`.

- **(v0.16.0) SSM Parameter Store plugin:** New `SSMPlugin` handles JSON-protocol requests identified by `X-Amz-Target: AmazonSSM.{Op}`. Operations: `PutParameter` (versioning, 409 `ParameterAlreadyExists` without `Overwrite`), `GetParameter` (404 on missing), `GetParameters` (returns `Parameters` + `InvalidParameters`), `DeleteParameter`, `DeleteParameters`, `GetParametersByPath` (recursive/non-recursive, base64 pagination), `DescribeParameters`, `AddTagsToResource`, `RemoveTagsFromResource`, `ListTagsForResource`. Betty CFN support: `AWS::SSM::Parameter` (priority 2), `AWS::SSM::Association` (priority 5, stub). `resolveDynamicRef` added to `StackDeployer` for `{{resolve:ssm:/path}}` and `{{resolve:ssm-secure:/path}}` dynamic references. Cost entries added for `GetParameter`, `GetParameters`, `GetParametersByPath`, `PutParameter`.

- **(v0.16.0) KMS plugin:** New `KMSPlugin` handles JSON-protocol requests identified by `X-Amz-Target: TrentService.{Op}`. Parser alias `"trentservice" → "kms"` added to `targetServiceAliases`. Operations: `CreateKey`, `DescribeKey`, `ListKeys`, `EnableKey`, `DisableKey`, `ScheduleKeyDeletion`, `CancelKeyDeletion`, `Encrypt`, `Decrypt`, `GenerateDataKey`, `GenerateDataKeyWithoutPlaintext`, `ReEncrypt`, `CreateAlias`, `DeleteAlias`, `ListAliases`, `UpdateAlias`, `PutKeyPolicy`, `GetKeyPolicy`, `EnableKeyRotation`, `DisableKeyRotation`, `GetKeyRotationStatus`, `TagResource`, `UntagResource`, `ListResourceTags`. Stub crypto: `kmsEncryptStub`/`kmsDecryptStub` use `base64(kms:{keyID}:{base64(plaintext)})` for deterministic testing. Betty CFN support: `AWS::KMS::Key` (priority 1), `AWS::KMS::Alias` (priority 2), `AWS::KMS::ReplicaKey` (priority 2, stub). Cost entries added for `CreateKey`, `Encrypt`, `Decrypt`, `GenerateDataKey`, `ReEncrypt`.

- **(v0.16.0) `writeError` protocol fix:** `server.go` `writeError` now uses `strings.HasPrefix(ct, "application/x-amz-json")` to match both `1.0` and `1.1` content types, ensuring error responses are JSON for all JSON-protocol services (KMS, SSM, Secrets Manager). SNS added to the query→JSON rewrite condition in `server.go`.

- **(v0.16.0) Betty CFN `resolveFnGetAtt` extended:** `GetAtt` attribute routing for `AWS::SNS::Topic` → `TopicArn`, `AWS::KMS::Key` → `KeyArn`/`Arn`, `AWS::SSM::Parameter` → `Value` (returns physical parameter name). `resolveDynamicRef` added for SSM dynamic references.

- **(#59) ELBv2 plugin:** New `ELBPlugin` handling query-protocol requests on `elasticloadbalancing.{region}.amazonaws.com`. Load balancer operations: `CreateLoadBalancer`, `DescribeLoadBalancers`, `DeleteLoadBalancer`, `DescribeLoadBalancerAttributes`, `ModifyLoadBalancerAttributes`. Target group operations: `CreateTargetGroup`, `DescribeTargetGroups`, `DeleteTargetGroup`, `ModifyTargetGroup`. Target registration: `RegisterTargets`, `DeregisterTargets`, `DescribeTargetHealth` (always returns healthy). Listener operations: `CreateListener`, `DescribeListeners`, `DeleteListener`, `ModifyListener`. Rule operations: `CreateRule`, `DescribeRules`, `DeleteRule`, `SetRulePriorities`. Supports both `application` and `network` load balancer types. Betty CFN support added for `AWS::ElasticLoadBalancingV2::TargetGroup`, `AWS::ElasticLoadBalancingV2::LoadBalancer`, `AWS::ElasticLoadBalancingV2::Listener`, `AWS::ElasticLoadBalancingV2::ListenerRule`. Cost entries added for `CreateLoadBalancer` and `RegisterTargets`.

- **(#60) Route 53 plugin:** New `Route53Plugin` handling REST/XML requests on `route53.amazonaws.com`. Hosted zone operations: `CreateHostedZone` (returns 201), `ListHostedZones`, `GetHostedZone`, `DeleteHostedZone`. Record set operations: `ChangeResourceRecordSets` (CREATE/UPSERT/DELETE actions, returns `INSYNC` immediately), `ListResourceRecordSets`. Supports A, AAAA, CNAME, MX, NS, SOA, TXT record types and alias records. Operation routing via `parseRoute53Operation(method, path)` for path-based REST dispatch. Betty CFN support added for `AWS::Route53::HostedZone`, `AWS::Route53::RecordSet`, `AWS::Route53::RecordSetGroup`. Cost entries added for `CreateHostedZone` and `ChangeResourceRecordSets`.

- **(#56) ABAC condition keys:** `AuthController.CheckAccess` now populates `aws:ResourceTag/*` and `aws:RequestTag/*` IAM condition keys so policies can allow or deny based on resource tags or request-time tags. Resource tags are loaded from state for S3, Lambda, SQS, DynamoDB, EC2 (instances), and IAM (users/roles). Request tags are parsed from `x-amz-tagging` headers (S3), JSON bodies (IAM, Lambda), and query params (EC2 `TagSpecification.*`). `buildResourceARN` extended to produce full ARNs for EC2, Lambda, DynamoDB, and SQS requests. `DynamoDBTable` gains a `Tags map[string]string` field.

- **(#57) Resource Groups Tagging API:** New `TaggingPlugin` handles `tagging.{region}.amazonaws.com` requests identified by `X-Amz-Target: ResourceGroupsTaggingAPI_20170126.{Op}`. Supported operations: `GetResources` (with `TagFilters`, `ResourceTypeFilters`, and cursor-based pagination), `TagResources` (merge tags onto any supported resource), `UntagResources` (remove tag keys). Resources covered: S3 buckets, Lambda functions, SQS queues, DynamoDB tables, EC2 instances, IAM users and roles. Parser alias `"resourcegroupstaggingapi" → "tagging"` added to `targetServiceAliases`. `TaggingPlugin` registered in `cmd/substrate/main.go`.



- **(#52) Config hot-reload via SIGHUP:** `QuotaController.UpdateConfig`, `ConsistencyController.UpdateConfig`, `CostController.UpdateConfig`, and `FaultController.UpdateConfig` allow in-place config replacement without server restart. `cmd/substrate/main.go` installs a SIGHUP handler that reloads `substrate.yaml` and calls each controller's `UpdateConfig`.

- **(#53) EventStore bulk export:** `EventStore.ExportNDJSON` streams all matching events as newline-delimited JSON; `EventStore.ExportCSV` writes RFC 4180 CSV with an 11-column header. New `substrate export` CLI subcommand with `--format` (ndjson/csv), `--output`, `--stream`, `--service`, `--start`, `--end` flags.

- **(#55) Per-service tagging:** Lambda `TagResource`, `UntagResource`, `ListTags` via `/2015-03-31/tags/{arn}` paths. S3 `PutBucketTagging`, `GetBucketTagging`, `DeleteBucketTagging`, `PutObjectTagging`, `GetObjectTagging`, `DeleteObjectTagging` using XML `<Tagging><TagSet>` format; `S3Object.Tags` field added. IAM `TagUser`, `UntagUser`, `ListUserTags`, `TagRole`, `UntagRole`, `ListRoleTags`.

- **(#19) Fault injection middleware:** New `FaultController` with `FaultConfig` / `FaultRule` types. Rules match by service and/or operation, fire probabilistically (0.0–1.0), and inject either an `AWSError` (error fault) or a `time.Sleep` latency delay. Seeded per-instance PRNG for deterministic test replay. `fault` section added to `Config` and `substrate.yaml.example`. Server pipeline integrates fault injection between consistency check and plugin dispatch.

- **(#50 + #58) EC2 + VPC plugin:** New `EC2Plugin` handling query-protocol requests on `ec2.{region}.amazonaws.com`. Instance operations: `RunInstances`, `DescribeInstances` (with `Filter.N.*` support), `TerminateInstances`, `StopInstances`, `StartInstances`, `DescribeInstanceStatus`. VPC operations: `CreateVpc`, `DescribeVpcs`, `DeleteVpc`, `CreateSubnet`, `DescribeSubnets`, `DeleteSubnet`, `CreateSecurityGroup`, `DescribeSecurityGroups`, `DeleteSecurityGroup`, `AuthorizeSecurityGroupIngress/Egress`, `RevokeSecurityGroupIngress/Egress`, `CreateInternetGateway`, `DescribeInternetGateways`, `AttachInternetGateway`, `DetachInternetGateway`, `DeleteInternetGateway`, `CreateRouteTable`, `DescribeRouteTables`, `AssociateRouteTable`, `DisassociateRouteTable`, `CreateRoute`, `DeleteRoute`, `DeleteRouteTable`. Default VPC (`172.31.0.0/16`) auto-created on `RunInstances` when no `SubnetId` supplied. All state keys are region-scoped (`instance:{acct}/{region}/{id}`).

- **(#51) Multi-region routing:** `RegionCfg` added to `Config` with `default` and optional `allowed` allowlist. When `allowed` is non-empty, requests with unlisted regions receive `400 InvalidClientTokenId`. `region` section added to `substrate.yaml.example`.

- **(#54) Terraform plan validation:** `ParseTerraformPlan` decodes `terraform show -json` output; `ValidateTerraformPlan` estimates monthly cost and flags policy concerns. New `TerraformValidation` type with `EstimatedMonthlyCostUSD`, `ResourceCount`, `CreatedResources`, `DeletedResources`, `Warnings`, `Errors`. New `substrate validate-plan` CLI subcommand reads a JSON plan file and prints cost estimate and warnings.

- **Betty CFN EC2/VPC support:** `betty_cfn.go` now handles `AWS::EC2::VPC` (priority 1), `AWS::EC2::Subnet`, `AWS::EC2::SecurityGroup`, `AWS::EC2::InternetGateway` (priority 2), `AWS::EC2::RouteTable`, `AWS::EC2::Instance` (priority 3) resource types via new `deployEC2*` functions.

### Added

- **(#46) Prometheus metrics endpoint** (`/metrics`): hand-rolled Prometheus text-format v0.0.4 emitter with no external dependencies. New `MetricsCollector` type tracks `substrate_requests_total`, `substrate_request_errors_total`, `substrate_quota_hits_total`, `substrate_consistency_delays_total`, `substrate_cost_usd_total`, and `substrate_events_total`. Enabled via `metrics.enabled: true` in config; `MetricsCfg` added to `Config`; `/metrics` path registered before the `/*` catch-all.

- **(#49) Cost forecasting** via `EventStore.GetCostForecast`: linear regression on historical per-day cost buckets with a 95% confidence interval (±1.96σ), fallback to mean for fewer than 3 data points, and Z-score anomaly detection with a configurable sigma threshold. New types `CostForecast`, `DailyCost`, `CostAnomaly`. `ForecastCfg` added to `Config` with `forecast` section in `substrate.yaml.example`.

- **(#48) Plugin developer guide** in `doc_plugins.go` covering the `Plugin` interface, `PluginConfig`, state key naming conventions, `AWSRequest`/`AWSResponse`/`AWSError` shapes, unit-testing patterns, and integration-test patterns. New `examples/custom_plugin/main.go` demonstrates a minimal "weather" service plugin. `doc.go` extended with a `# Plugin Development` section cross-referencing both files.

- **(#47) OpenTelemetry distributed tracing** via `NewTracer`: supports `noop`, `stdout`, and `otlp_http` exporters. New `Tracer` type with `StartSpan` and `StartRequest` helpers. `TracingConfig` and `TracingCfg` structs added; `Tracer` field added to `ServerOptions`; `tracing` section added to `substrate.yaml.example`. Uses `go.opentelemetry.io/otel` v1.42.0.

- **DynamoDB table lifecycle (#43):** New `DynamoDBPlugin` implements the DynamoDB JSON-protocol
  API (`X-Amz-Target: DynamoDB_20120810.{Operation}`). Supports full table lifecycle:
  `CreateTable` (status `ACTIVE` immediately), `DeleteTable`, `DescribeTable`, `ListTables`
  (paginated via `ExclusiveStartTableName` + `Limit`), and `UpdateTable`. Table ARNs are generated
  as `arn:aws:dynamodb:{region}:{account}:table/{name}`. State stored under the `dynamodb`
  namespace using keys `table:{acct}/{name}`, `table_names:{acct}`, `item:{acct}/{tbl}/{key}`,
  `item_keys:{acct}/{tbl}`.

- **DynamoDB item CRUD and batch operations (#43):** `PutItem` (with `ConditionExpression` and
  `ReturnValues=ALL_OLD`), `GetItem` (with `ProjectionExpression`), `DeleteItem` (with
  `ConditionExpression` and `ReturnValues`), `UpdateItem` (SET/REMOVE/ADD/DELETE
  `UpdateExpression` clauses with arithmetic, set union/subtraction, and all `ReturnValues`
  modes), `BatchGetItem`, and `BatchWriteItem`. Item key encoding: hash-only PK uses `pkVal`;
  hash+range uses `pkVal#skVal`. `server.go` `writeError` extended to return JSON errors for
  `application/x-amz-json-1.0` requests.

- **DynamoDB Query and Scan (#44):** `Scan` and `Query` support `FilterExpression`,
  `ProjectionExpression`, `Limit`, `ExclusiveStartKey` pagination, and `IndexName` for
  GSI/LSI access. `Query` parses `KeyConditionExpression` (PK equality plus SK conditions `=`,
  `<`, `<=`, `>`, `>=`, `BETWEEN`, `begins_with`) and honours `ScanIndexForward`. A
  token-based recursive-descent expression evaluator handles comparisons (`=`, `<>`, `<`, `<=`,
  `>`, `>=`), logical operators (`AND`, `OR`, `NOT`), parentheses, `BETWEEN`, `IN`, and
  functions (`attribute_exists`, `attribute_not_exists`, `begins_with`, `contains`,
  `attribute_type`, `size`). Nested dotted-path attribute access supported (e.g.,
  `Meta.Region`).

- **DynamoDB GSI, LSI, TTL, and Streams stubs (#45):** `CreateTable` accepts
  `GlobalSecondaryIndexes`, `LocalSecondaryIndexes`, and `StreamSpecification`. `Query` and
  `Scan` route through `findIndexKeySchema` to use the correct key schema for the named index.
  `UpdateTimeToLive` / `DescribeTimeToLive` manage TTL attribute on the table. `ListStreams`,
  `DescribeStream`, `GetShardIterator`, and `GetRecords` provide stub stream support.

- **CloudFormation DynamoDB support (#43):** `betty_cfn.go` maps `AWS::DynamoDB::Table` to
  `CreateTable` (priority 2, deploys alongside S3). All CFN properties are forwarded:
  `KeySchema`, `AttributeDefinitions`, `BillingMode`, `ProvisionedThroughput`,
  `GlobalSecondaryIndexes`, `LocalSecondaryIndexes`, `StreamSpecification`. If
  `TimeToLiveSpecification` is present, `UpdateTimeToLive` is called automatically after table
  creation. `Ref` resolves to `TableName`; `Fn::GetAtt TableArn` resolves to the table ARN.

- **DynamoDB cost tracking:** `defaultCostTable` in `costs.go` now includes entries for
  `dynamodb/PutItem`, `dynamodb/UpdateItem`, `dynamodb/DeleteItem`, `dynamodb/BatchWriteItem`
  ($0.00000125 each) and `dynamodb/Query`, `dynamodb/Scan`, `dynamodb/BatchGetItem`
  ($0.00000025 each). `GetItem` was already present.

- **CLI DynamoDB registration:** `cmd/substrate/main.go` registers `DynamoDBPlugin` after SQS
  and before S3 in the server plugin chain.

## [v0.10.0] - 2026-03-08

### Added

- **Lambda function emulation (#40):** New `LambdaPlugin` implements the Lambda REST API
  (`/2015-03-31/functions/…`). Supports `CreateFunction`, `GetFunction`, `UpdateFunctionCode`,
  `UpdateFunctionConfiguration`, `DeleteFunction`, `ListFunctions` (paginated), `Invoke` (stub
  synchronous response), `InvokeAsync`, `AddPermission`, `RemovePermission`, `GetPolicy`, and
  `PutFunctionEventInvokeConfig`. State is stored under the `lambda` namespace. `LambdaPlugin` is
  registered automatically in the server CLI.

- **SQS queue emulation (#41):** New `SQSPlugin` implements the SQS query-protocol API. Supports
  `CreateQueue` (idempotent), `DeleteQueue`, `GetQueueUrl`, `GetQueueAttributes`,
  `SetQueueAttributes`, `ListQueues` (prefix-filtered), `TagQueue`, `UntagQueue`, `ListQueueTags`,
  `SendMessage`, `SendMessageBatch`, `ReceiveMessage` (with `VisibilityTimeout` and `DelaySeconds`
  via `TimeController`), `DeleteMessage`, `DeleteMessageBatch`, `ChangeMessageVisibility`, and
  `PurgeQueue`. Queue URLs use the local format `http://sqs.{region}.localhost/{accountID}/{name}`.
  `server.go` now includes `sqs` in the query-protocol → JSON body rewrite path. SQS cost entries
  (`sqs/SendMessage`, `sqs/ReceiveMessage` at $0.0000004 each) added to `defaultCostTable`.

- **S3 event notifications (#22):** `S3Plugin` gains `GetBucketNotificationConfiguration` and
  `PutBucketNotificationConfiguration` operations (via `?notification` query param). After each
  successful `PutObject` or `DeleteObject`, `fireNotifications` dispatches to configured Lambda
  functions (via `lambda/Invoke`) and SQS queues (via `sqs/SendMessage`) with an S3 event payload
  matching the AWS `2.1` schema. Prefix/suffix key filters and wildcard event patterns
  (`s3:ObjectCreated:*`) are supported. The `S3Plugin` accepts an optional `"registry"` key in
  `PluginConfig.Options` to enable dispatch; nil disables notifications without error.

- **Enhanced CloudFormation support (#42):** `cfnTemplate` now parses `Parameters`, `Conditions`,
  and `Outputs` sections. `StackDeployer.Deploy` accepts an optional `params map[string]string`
  argument that overrides template parameter defaults. Intrinsic functions supported:
  `Ref`, `Fn::Sub` (string and `[template, vars]` forms), `Fn::Join`, `Fn::Select`, `Fn::Split`,
  `Fn::Base64`, `Fn::GetAtt`, `Fn::If`. Condition operators: `Fn::Equals`, `Fn::Not`, `Fn::And`,
  `Fn::Or`. Resources with a false `Condition` field are skipped. `DeployResult.Outputs` is
  populated from the resolved Outputs section. Stack state is persisted under the `cfn` namespace
  via a new `CFNStackState` type. New methods: `StackDeployer.UpdateStack`,
  `StackDeployer.DeleteStack`, `StackDeployer.ListStacks`. `AWS::Lambda::Function` now dispatches
  to the Lambda plugin (no longer a stub). New `AWS::SQS::Queue` resource type supported.
  `NewStackDeployer` constructor added for direct instantiation in tests.

## [v0.9.0] - 2026-03-08

### Added

- **Multi-account credential registry (#36):** New `CredentialRegistry` and `CredentialEntry` types
  provide a thread-safe store mapping AWS access key IDs to accounts and secrets. A built-in test
  credential (`AKIATEST12345678901` → account `123456789012`) is pre-loaded by `NewCredentialRegistry`.
  `ServerOptions.Credentials` wires the registry into the request pipeline so the caller's account ID
  and principal ARN are resolved from the `Authorization` header on every request.

- **SigV4 request signature verification (#35):** `VerifySigV4` validates AWS4-HMAC-SHA256 signatures
  against secret keys from the `CredentialRegistry`. The server pre-reads the request body once and
  restores it before parsing; the SigV4 check runs in the pipeline after credential resolution and
  returns `InvalidClientTokenId` (403) for unknown keys or `SignatureDoesNotMatch` (403) for bad
  signatures. Passing `nil` as the registry disables verification (backward-compatible default).

- **IAM inline policies (#38):** `IAMPlugin` now handles `PutUserPolicy`, `GetUserPolicy`,
  `DeleteUserPolicy`, `ListUserPolicies`, `PutRolePolicy`, `GetRolePolicy`, `DeleteRolePolicy`, and
  `ListRolePolicies`. Inline policy documents are stored in state under
  `user_inline:{name}:{policyName}` / `role_inline:{name}:{policyName}` keys; a sorted name index
  is maintained under `user_inline_names:{name}` / `role_inline_names:{name}`.

- **IAM permission boundaries (#38):** `IAMUser` and `IAMRole` each gain a `PermissionsBoundary`
  field (`*IAMAttachedPolicy`). `IAMPlugin` handles `PutUserPermissionsBoundary`,
  `DeleteUserPermissionsBoundary`, `PutRolePermissionsBoundary`, and `DeleteRolePermissionsBoundary`.
  The `authorize` function enforces AWS boundary semantics: effective access = Allow in BOTH identity
  policies AND boundary policy. The `AdministratorAccess` fast path no longer bypasses the boundary
  check.

- **Cross-service IAM enforcement (#37):** New `AuthController` type (created via
  `NewAuthController`) inspects the caller principal on every request and evaluates attached managed
  policies, inline policies, and permission boundaries via the existing `Evaluate` engine.
  `ServerOptions.Auth` wires the controller into the pipeline before quota/consistency checks.
  `cmd/substrate/main.go` instantiates and wires `AuthController` automatically.

- **S3 bucket policies and object ACLs (#39):** `S3Plugin` now handles `GetBucketPolicy`,
  `PutBucketPolicy`, `DeleteBucketPolicy`, `GetBucketAcl`, `PutBucketAcl`, `GetObjectAcl`, and
  `PutObjectAcl`. Bucket policies are stored as raw JSON under `bucket_policy:{bucket}`; ACLs as
  `S3AccessControlList` XML under `bucket_acl:{bucket}` and `object_acl:{bucket}/{key}`. Canned ACL
  values (`private`, `public-read`, `public-read-write`, `authenticated-read`) are supported via the
  `x-amz-acl` header. New `S3BucketPolicy`, `S3AccessControlList`, `S3Owner`, `S3Grant`, and
  `S3Grantee` types added to `s3_types.go`.

## [v0.8.0] - 2026-03-07

### Added

- **TimeController race fix (#33):** Added `sync.RWMutex` to `TimeController`; `Now()` acquires
  a read-lock and `SetTime()`/`SetScale()` acquire a write-lock. New `types_test.go` verifies
  zero races under 50-goroutine concurrent access (`go test -race`).

- **PluginRegistry thread safety (#32):** Added `sync.RWMutex` to `PluginRegistry`; `Register`
  acquires a write-lock, `RouteRequest` acquires a read-lock. New `Names()` method returns a
  sorted slice of registered service names (used by `/ready`).

- **Health and readiness endpoints (#32):** `GET /health` returns
  `{"status":"ok","version":"<version>"}` always 200; `GET /ready` returns
  `{"status":"ok","plugins":["iam","s3",...]}` always 200. Both paths are configurable via
  `server.health_path` / `server.ready_path` (default `/health` / `/ready`). Neither endpoint
  is recorded in the EventStore. `server_test.go` gains four new tests. The E2E test now polls
  `/health` instead of sleeping.

- **`Version` package variable (#32):** `doc.go` exports `var Version = "dev"` set at build time
  via `-X github.com/scttfrdmn/substrate.Version=$(VERSION)` (Makefile updated).

- **EventStore in-memory service/operation indexes (#34):** `byService` and `byOperation`
  maps are populated in `RecordEvent`; `GetEvents` calls the new private `selectSource` which
  selects the narrowest index for single-field Service or Operation filters. New benchmark
  `BenchmarkEventStore_FilterByService` (10,000 events, 3 services) demonstrates the speedup.

- **EventStoreOption variadic options pattern (#34):** `NewEventStore` now accepts
  `...EventStoreOption`; `WithStateManager(sm)` attaches a `StateManager` for async snapshotting.
  All existing callers remain source-compatible.

- **Async snapshot goroutine (#30):** When `EventStoreConfig.SnapshotInterval > 0` and a
  `StateManager` is provided via `WithStateManager`, `NewEventStore` launches a `snapshotLoop`
  goroutine. `RecordEvent` sends a non-blocking hint to the goroutine every N events.
  `Close()` shuts down the goroutine. New tests: `TestEventStore_AsyncSnapshot_CreatesSnapshot`,
  `TestEventStore_Close_NoGoroutine`, `TestEventStore_SnapshotInterval_Zero_Disabled`.

- **File NDJSON backend (#31):** New `eventstore_file.go` with `fileBackend`; `Flush` appends
  only new events as NDJSON lines under `<persist_path>/events/<stream_id>.ndjson`; `Load` reads
  all `*.ndjson` files. Optional rotation when `event_store.max_file_size_mb > 0`. New config
  fields `EventStoreCfg.MaxFileSizeMB` / `EventStoreConfig.MaxFileSizeMB`. New tests:
  `TestEventStore_FilePersistence`, `TestEventStore_FilePersistence_AppendOnly`.

- **SQLite backend (#29):** New `eventstore_sqlite.go` with `sqliteBackend` using pure-Go
  `modernc.org/sqlite v1.37.0` (no CGO). Schema: `events` and `snapshots` tables with indexes
  on `(stream_id, sequence)`, `service`, and `operation`. Lazy init via `initSQLiteBackend`
  (thread-safe `sync.Mutex`). `Flush` uses `INSERT OR IGNORE` for idempotency; `Load` restores
  all events and snapshots into memory. New config fields `EventStoreCfg.DSN` / `EventStoreConfig.DSN`
  (default `"substrate.db"`). New tests: `TestEventStore_SQLitePersistence`,
  `TestEventStore_SQLite_IdempotentFlush`, `TestEventStore_SQLite_SnapshotRoundTrip`.

### Fixed

- `generateEventID` now includes the event's Sequence number to guarantee uniqueness when many
  events are recorded within the same nanosecond (previously SQLite `INSERT OR IGNORE` would
  silently drop events with duplicate IDs).

- `coverage_test.go` SQLite stub tests (`TestEventStore_Flush_NonMemory`,
  `TestEventStore_Load_NonMemory`) now use `t.TempDir()` so they exercise the real SQLite
  backend rather than failing silently.

## [v0.7.0] - 2026-03-07

### Added

- End-to-end tests in `test/e2e/` using the real `aws-sdk-go-v2`: `TestS3_CRUD`
  (CreateBucket/PutObject/GetObject/HeadObject/DeleteObject/DeleteBucket),
  `TestIAM_Lifecycle` (CreateUser/CreateRole/CreatePolicy/Attach/Detach/Delete),
  `TestCostReport_AfterS3Workload` (10× PutObject then cost aggregation check).
  Uses a `serviceTransport` that routes SDK requests to an in-process server while
  preserving the `Host` header for service extraction. Closes #26.
- Four benchmark functions in `benchmarks_test.go`: `BenchmarkEventStore_RecordThroughput`,
  `BenchmarkReplayEngine_Replay`, `BenchmarkServer_HTTPThroughput`, and
  `BenchmarkS3PutObject_Latency`. Run with `make bench`. Closes #27.
- `examples/betty_workflow/main.go`: runnable Betty.codes end-to-end example demonstrating
  plugin wiring, `BettyClient.Deploy`, recording, `StopRecording`/validation, `DebugSession`
  time-travel, and formatted JSON report output. Run with
  `go run examples/betty_workflow/main.go`. Closes #28.
- `Makefile` targets `bench` and `e2e`.
- CI jobs `e2e` and `benchmark` in `.github/workflows/ci.yml`.

### Fixed

- IAM/STS query-protocol body gap: when the real AWS SDK sends
  `application/x-www-form-urlencoded` bodies, `ParseAWSRequest` consumes the
  body via `r.ParseForm` leaving it empty. `handleAWSRequest` now reconstructs
  `req.Body` as JSON from `req.Params` for `iam` and `sts` services, enabling
  plugin JSON unmarshalling to succeed without affecting Betty in-process calls.

- `BettyClient` (`betty.go`): convenience wrapper orchestrating the full Betty.codes
  validation workflow — `Deploy`, `StartRecording`, `StopRecording`, `Validate`, and
  `NewDebugSession` — without requiring an HTTP server. Closes #25.
- `StackDeployer` / `DeployStack` (`betty_cfn.go`): parses JSON or YAML CloudFormation
  templates and creates resources via in-process plugin dispatch.  Deployment order:
  `AWS::IAM::Policy` → `AWS::IAM::Role` → `AWS::S3::Bucket`; unknown types are skipped
  with a warning; `AWS::Lambda::Function` returns `NotImplemented` while the rest of
  the template continues to deploy. New types: `cfnTemplate`, `cfnResource`,
  `DeployedResource`, `DeployResult`, `Intent`. Closes #23.
- `ValidateRecording` (`betty_report.go`): analyses a recorded event stream for cost,
  quota headroom, consistency incidents, and intent violations. New types:
  `ValidationReport`, `ValidationStatus`, `CostBreakdown`, `QuotaCheck`,
  `ConsistencyIncident`, `Suggestion`. Monthly cost projection extrapolated from
  observed request rate; suggestions generated for high cost, consistency incidents,
  and failed events. Closes #24.
- `DebugSession` (`betty_debug.go`): time-travel inspection wrapper over `ReplayEngine`
  with lazy stream loading. Exposes `JumpToEvent`, `StepBackward`, and `InspectState`.

- `S3Plugin`: REST+XML S3 emulator covering 12 core operations —
  `CreateBucket`, `HeadBucket`, `DeleteBucket`, `ListBuckets`,
  `PutObject`, `GetObject`, `HeadObject`, `DeleteObject`, `CopyObject`,
  `ListObjects`, `ListObjectsV2` (with continuation-token pagination and
  common-prefix / delimiter support). Object bodies stored in afero.MemMapFs;
  metadata in StateManager namespace `"s3"`. User-defined metadata round-tripped
  via `X-Amz-Meta-*` headers. `ETag` computed as MD5 hex digest. Closes #20.
- `S3Plugin` multipart upload: `CreateMultipartUpload`, `UploadPart`,
  `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListMultipartUploads`.
  Multi-part ETag uses `"<md5(concat(part_md5s))>-<N>"` format matching AWS.
  Part bodies stored in `/.multipart/<uploadID>/<partNum>` on the afero
  filesystem; cleaned up on complete or abort. Closes #21.
- S3 virtual-hosted-style URL normalisation in `ParseAWSRequest`:
  `mybucket.s3[.<region>].amazonaws.com` is transparently rewritten to
  service `"s3"` with path `/mybucket/…`, so path-style and
  virtual-hosted requests are handled identically by the plugin.
- `AWSRequest.Path`: new field carrying the effective URL path, with the
  bucket prepended for S3 virtual-hosted requests.
- Bare query-key sentinel `"1"` in the parameter parser — `?uploads` and
  `?versions` now map to `params["uploads"]=="1"` as expected by plugins.
- S3 quota defaults: `"s3"` 3500 rps / 5500 burst and
  `"s3/GetObject"` 5500 / 5500, replacing the prior `TODO(#22)` placeholder.
- `isMutating` recognises `Copy`, `Upload`, `Complete`, and `Abort` prefixes
  for consistency-controller tracking of S3 write operations.
- `S3Plugin` registered in `cmd/substrate/main.go` server command.
- `github.com/spf13/afero` promoted from indirect to direct dependency.
- `// TODO(#22)`: event-notification forwarding to Lambda/SQS deferred —
  placeholder comment in `S3Plugin.HandleRequest` default case.

- `QuotaController`: token-bucket rate limiter wired into the server pipeline;
  returns HTTP 429 `ThrottlingException` when a service or operation burst is
  exhausted. Time sourced from `TimeController` for deterministic tests.
  Replay requests bypass quota checks. Default rules mirror AWS service quotas
  (IAM 100 rps / 100 burst, STS 100 rps / 100 burst, AssumeRole 50/50, with
  per-operation overrides for CreateUser/DeleteUser/CreateRole/DeleteRole at
  20/20). S3 prefix-level rules deferred to TODO(#22). Closes #16.
- `ConsistencyController`: eventual-consistency simulation that rejects reads
  within a configurable `PropagationDelay` after a mutating request to the
  same resource key; returns HTTP 409 `InconsistentStateException`. Disabled
  by default. Replay requests are a no-op. Closes #17.
- `CostController`: stateless per-request USD cost estimator backed by a
  built-in pricing table (`s3/PutObject` $0.000005, `s3/GetObject`
  $0.0000004, `dynamodb/GetItem` $0.00000025, `lambda/Invoke` $0.0000002;
  IAM/STS free). Config overrides supported. Closes #18.
- `GetCostSummary`: new `EventStore` method that aggregates `Event.Cost`
  over an account and optional time range, returning a `CostSummary` with
  `TotalCost`, `ByService`, `ByOperation`, and `RequestCount`. Closes #18.
- `ServerOptions`: variadic options struct for `NewServer` that wires
  `QuotaController`, `ConsistencyController`, and `CostController` into the
  request pipeline. Nil fields disable the respective feature.
- `QuotaCfg`, `ConsistencyCfg`, `CostCfg`: YAML config sections with
  `To*Config()` bridge methods following the existing `EventStoreCfg` pattern.
- `substrate.yaml.example` extended with `quotas`, `consistency`, and `costs`
  sections and inline comments.

[Unreleased]: https://github.com/scttfrdmn/substrate/compare/v0.7.0...HEAD
[v0.7.0]: https://github.com/scttfrdmn/substrate/compare/v0.3.0-alpha...v0.7.0

## [v0.3.0-alpha] - 2026-03-08

### Added

- `IAMPlugin`: full IAM JSON-protocol plugin (CreateUser/GetUser/DeleteUser/ListUsers,
  CreateRole/GetRole/DeleteRole/ListRoles, CreateGroup/GetGroup/DeleteGroup/ListGroups,
  AttachUserPolicy/DetachUserPolicy/ListAttachedUserPolicies,
  AttachRolePolicy/DetachRolePolicy/ListAttachedRolePolicies,
  CreatePolicy/GetPolicy/DeletePolicy/ListPolicies,
  CreateAccessKey/DeleteAccessKey/ListAccessKeys) with alphabetically deterministic
  pagination (Marker/MaxItems) and JSON error format (`__type` field) (closes #14).
- `STSPlugin`: STS query-protocol plugin (GetCallerIdentity, AssumeRole,
  GetSessionToken) with TimeController-driven credential expiry and XML responses;
  temporary credentials persisted to state (closes #15).
- `IAMUser`, `IAMRole`, `IAMGroup`, `IAMPolicy`, `IAMAccessKey`, `IAMTag`,
  `IAMAttachedPolicy`: AWS-exact IAM entity types with JSON serialisation (closes #11).
- `PolicyDocument`, `PolicyStatement`, `PolicyPrincipal`, `StringOrSlice`:
  IAM policy document types; `StringOrSlice` and `PolicyPrincipal` implement
  custom JSON marshal/unmarshal to handle AWS's mixed string/array encoding (closes #11).
- `Evaluate`, `EvaluationRequest`, `EvaluationResult`: pure IAM policy evaluation
  engine implementing the AWS evaluation algorithm (explicit deny wins, then allow,
  then implicit deny); supports Action/NotAction, Resource/NotResource, and
  condition operators StringEquals, StringNotEquals, StringLike, StringNotLike,
  ArnEquals, ArnLike, ArnNotEquals, Bool, Null (closes #12).
- `ListManagedPolicies`, `GetManagedPolicy`: 47 bundled AWS managed policies with
  policy documents sourced from the official AWS managed policy reference; lazy-initialised
  lookup map via `sync.Once` (closes #13).
- `STSSessionCredentials`: JSON-persisted session credential type used by STSPlugin.
- `cmd/substrate/main.go`: IAMPlugin and STSPlugin registered in `newServerCmd()`.
- `substrate.yaml.example`: fully-commented server configuration example.
- README Getting Started section: install, run, SDK configuration examples (AWS CLI,
  Go SDK v2, Python boto3, Node.js SDK v3), supported services table, known limitations.
- `Server`: chi-based HTTP server with catch-all AWS request handler, graceful
  shutdown, and event recording on every request (closes #7).
- `ParseAWSRequest`: pure function extracting service, operation, region, and
  account ID from HTTP request headers, Host, Authorization SigV4 scope, and URL
  path (closes #8).
- `Config`, `ServerConfig`, `EventStoreCfg`, `StateCfg`, `LogCfg`: YAML-friendly
  config types with `mapstructure` tags; `LoadConfig` (viper, env overrides via
  `SUBSTRATE_` prefix), `DefaultConfig`, `Validate` (closes #9).
- `EventStoreCfg.ToEventStoreConfig`: bridge from YAML config to `EventStoreConfig`.
- CLI rewritten with cobra: `substrate server`, `substrate replay <stream>`, and
  `substrate debug <stream>` sub-commands; `--version` retained on root (closes #10).
- `Server.ServeHTTP`: exposes the chi router directly for httptest-based testing.
- `MemoryStateManager`: thread-safe in-memory `StateManager` and `SnapshotableStateManager`
  implementation with JSON snapshot/restore and atomic reset (closes #1, #5).
- `SnapshotableStateManager` interface extending `StateManager` with `Snapshot`,
  `Restore`, and `Reset` (closes #5).
- `SlogLogger` and `NewDefaultLogger`: structured logging backed by `log/slog` (closes #4).
- Seeded RNG (`ReplayConfig.RandomSeed`) using `math/rand/v2` PCG source for
  deterministic replay; `ReplayEngine.RandFloat64` and `RandInt64` helpers (closes #6).
- `EventStore`: immutable event log with stream grouping, filtering, and snapshots.
- `ReplayEngine`: deterministic stream replay with time-travel debugging (step
  forward/backward, jump-to-event, breakpoints, state inspection).
- `RecordingSession`: named test-recording sessions.
- Core types: `AWSRequest`, `AWSResponse`, `AWSError`, `RequestContext`,
  `Principal`, `StateManager`, `TimeController`, `Logger`, `Plugin`,
  `PluginConfig`, `PluginRegistry`.
- `JSONSerializer` for event persistence.
- Initial project structure, CI workflow, and tooling.

### Changed

- `cmd/substrate/main.go`: replaced `flag`-based stub with full cobra command tree.
- Managed policy names corrected to match actual AWS names: `AmazonECS_FullAccess`
  (was `AmazonECSFullAccess`), `CloudWatchFullAccess` (was `AmazonCloudWatchFullAccess`),
  `AWSCodePipeline_FullAccess` (was `AWSCodePipelineFullAccess`),
  `AWSCodePipeline_ReadOnlyAccess` (was `AWSCodePipelineReadOnlyAccess`),
  `AmazonCognitoPowerUser` (was `AmazonCognitoIdpFullAccess`).

### Fixed

- `golangci-lint` v2 config: moved `gofmt`/`goimports` to `formatters` section and
  removed `gosimple` (absorbed into `staticcheck` in v2).
- Unused `ctx` parameters renamed to `_` across `eventstore.go` and `replay.go` to
  satisfy `revive` linter.

[Unreleased]: https://github.com/scttfrdmn/substrate/compare/v0.27.2...HEAD
[v0.27.2]: https://github.com/scttfrdmn/substrate/compare/v0.27.1...v0.27.2
[v0.27.1]: https://github.com/scttfrdmn/substrate/compare/v0.27.0...v0.27.1
[v0.27.0]: https://github.com/scttfrdmn/substrate/compare/v0.26.0...v0.27.0
[v0.26.0]: https://github.com/scttfrdmn/substrate/compare/v0.25.0...v0.26.0
[v0.25.0]: https://github.com/scttfrdmn/substrate/compare/v0.24.0...v0.25.0
[v0.24.0]: https://github.com/scttfrdmn/substrate/compare/v0.23.0...v0.24.0
[v0.23.0]: https://github.com/scttfrdmn/substrate/compare/v0.22.0...v0.23.0
[v0.22.0]: https://github.com/scttfrdmn/substrate/compare/v0.21.0...v0.22.0
[v0.21.0]: https://github.com/scttfrdmn/substrate/compare/v0.20.0...v0.21.0
[v0.20.0]: https://github.com/scttfrdmn/substrate/compare/v0.19.0...v0.20.0
[v0.19.0]: https://github.com/scttfrdmn/substrate/compare/v0.18.0...v0.19.0
[v0.18.0]: https://github.com/scttfrdmn/substrate/compare/v0.17.0...v0.18.0
[v0.18.0]: https://github.com/scttfrdmn/substrate/compare/v0.17.0...v0.18.0
[v0.17.0]: https://github.com/scttfrdmn/substrate/compare/v0.16.0...v0.17.0
[v0.16.0]: https://github.com/scttfrdmn/substrate/compare/v0.7.0...v0.16.0
[v0.3.0-alpha]: https://github.com/scttfrdmn/substrate/releases/tag/v0.3.0-alpha
[v0.28.0]: https://github.com/scttfrdmn/substrate/compare/v0.27.2...v0.28.0
[v0.29.0]: https://github.com/scttfrdmn/substrate/compare/v0.28.0...v0.29.0
[v0.30.0]: https://github.com/scttfrdmn/substrate/compare/v0.29.0...v0.30.0
[v0.31.0]: https://github.com/scttfrdmn/substrate/compare/v0.30.0...v0.31.0
[v0.32.0]: https://github.com/scttfrdmn/substrate/compare/v0.31.0...v0.32.0
[v0.34.0]: https://github.com/scttfrdmn/substrate/compare/v0.32.0...v0.34.0
[v0.35.0]: https://github.com/scttfrdmn/substrate/compare/v0.34.0...v0.35.0
[v0.36.0]: https://github.com/scttfrdmn/substrate/compare/v0.35.0...v0.36.0
[v0.36.1]: https://github.com/scttfrdmn/substrate/compare/v0.36.0...v0.36.1
[v0.36.2]: https://github.com/scttfrdmn/substrate/compare/v0.36.1...v0.36.2
[v0.36.3]: https://github.com/scttfrdmn/substrate/compare/v0.36.2...v0.36.3
[v0.36.4]: https://github.com/scttfrdmn/substrate/compare/v0.36.3...v0.36.4
[v0.36.5]: https://github.com/scttfrdmn/substrate/compare/v0.36.4...v0.36.5
[v0.36.6]: https://github.com/scttfrdmn/substrate/compare/v0.36.5...v0.36.6
[v0.36.7]: https://github.com/scttfrdmn/substrate/compare/v0.36.6...v0.36.7
[v0.36.8]: https://github.com/scttfrdmn/substrate/compare/v0.36.7...v0.36.8
[v0.36.9]: https://github.com/scttfrdmn/substrate/compare/v0.36.8...v0.36.9
[v0.36.10]: https://github.com/scttfrdmn/substrate/compare/v0.36.9...v0.36.10
[v0.36.11]: https://github.com/scttfrdmn/substrate/compare/v0.36.10...v0.36.11
[v0.36.12]: https://github.com/scttfrdmn/substrate/compare/v0.36.11...v0.36.12
[v0.36.13]: https://github.com/scttfrdmn/substrate/compare/v0.36.12...v0.36.13
[v0.36.14]: https://github.com/scttfrdmn/substrate/compare/v0.36.13...v0.36.14
[v0.36.15]: https://github.com/scttfrdmn/substrate/compare/v0.36.14...v0.36.15
[v0.36.16]: https://github.com/scttfrdmn/substrate/compare/v0.36.15...v0.36.16
[v0.36.17]: https://github.com/scttfrdmn/substrate/compare/v0.36.16...v0.36.17
[v0.36.18]: https://github.com/scttfrdmn/substrate/compare/v0.36.17...v0.36.18
[v0.36.19]: https://github.com/scttfrdmn/substrate/compare/v0.36.18...v0.36.19
[v0.36.20]: https://github.com/scttfrdmn/substrate/compare/v0.36.19...v0.36.20
[v0.36.21]: https://github.com/scttfrdmn/substrate/compare/v0.36.20...v0.36.21
[v0.37.0]: https://github.com/scttfrdmn/substrate/compare/v0.36.21...v0.37.0
[v0.37.1]: https://github.com/scttfrdmn/substrate/compare/v0.37.0...v0.37.1
[v0.38.0]: https://github.com/scttfrdmn/substrate/compare/v0.37.1...v0.38.0
[v0.39.0]: https://github.com/scttfrdmn/substrate/compare/v0.38.0...v0.39.0
[v0.40.0]: https://github.com/scttfrdmn/substrate/compare/v0.39.0...v0.40.0
[v0.41.0]: https://github.com/scttfrdmn/substrate/compare/v0.40.0...v0.41.0
[v0.41.1]: https://github.com/scttfrdmn/substrate/compare/v0.41.0...v0.41.1
[v0.41.2]: https://github.com/scttfrdmn/substrate/compare/v0.41.1...v0.41.2
[v0.41.3]: https://github.com/scttfrdmn/substrate/compare/v0.41.2...v0.41.3
[v0.42.0]: https://github.com/scttfrdmn/substrate/compare/v0.41.3...v0.42.0
[v0.42.1]: https://github.com/scttfrdmn/substrate/compare/v0.42.0...v0.42.1
[v0.43.0]: https://github.com/scttfrdmn/substrate/compare/v0.42.1...v0.43.0
[v0.43.1]: https://github.com/scttfrdmn/substrate/compare/v0.43.0...v0.43.1
[v0.43.2]: https://github.com/scttfrdmn/substrate/compare/v0.43.1...v0.43.2
[v0.43.3]: https://github.com/scttfrdmn/substrate/compare/v0.43.2...v0.43.3
[v0.43.4]: https://github.com/scttfrdmn/substrate/compare/v0.43.3...v0.43.4
[v0.44.2]: https://github.com/scttfrdmn/substrate/compare/v0.43.4...v0.44.2
[v0.44.3]: https://github.com/scttfrdmn/substrate/compare/v0.44.2...v0.44.3
[v0.44.4]: https://github.com/scttfrdmn/substrate/compare/v0.44.3...v0.44.4
[v0.45.0]: https://github.com/scttfrdmn/substrate/compare/v0.44.4...v0.45.0
[v0.45.1]: https://github.com/scttfrdmn/substrate/compare/v0.45.0...v0.45.1
[v0.45.2]: https://github.com/scttfrdmn/substrate/compare/v0.45.1...v0.45.2
[v0.45.3]: https://github.com/scttfrdmn/substrate/compare/v0.45.2...v0.45.3
[v0.45.4]: https://github.com/scttfrdmn/substrate/compare/v0.45.3...v0.45.4
[v0.45.5]: https://github.com/scttfrdmn/substrate/compare/v0.45.4...v0.45.5
[v0.45.6]: https://github.com/scttfrdmn/substrate/compare/v0.45.5...v0.45.6
[v0.45.8]: https://github.com/scttfrdmn/substrate/compare/v0.45.6...v0.45.8
[v0.45.9]: https://github.com/scttfrdmn/substrate/compare/v0.45.8...v0.45.9
[v0.45.10]: https://github.com/scttfrdmn/substrate/compare/v0.45.9...v0.45.10
[v0.46.0]: https://github.com/scttfrdmn/substrate/compare/v0.45.10...v0.46.0
[v0.47.0]: https://github.com/scttfrdmn/substrate/compare/v0.46.0...v0.47.0
[v0.48.0]: https://github.com/scttfrdmn/substrate/compare/v0.47.0...v0.48.0
[v0.49.0]: https://github.com/scttfrdmn/substrate/compare/v0.48.0...v0.49.0
[v0.50.0]: https://github.com/scttfrdmn/substrate/compare/v0.49.0...v0.50.0
[v0.51.0]: https://github.com/scttfrdmn/substrate/compare/v0.50.0...v0.51.0
[v0.52.0]: https://github.com/scttfrdmn/substrate/compare/v0.51.0...v0.52.0
[v0.53.0]: https://github.com/scttfrdmn/substrate/compare/v0.52.0...v0.53.0
[v0.54.0]: https://github.com/scttfrdmn/substrate/compare/v0.53.0...v0.54.0
[v0.55.0]: https://github.com/scttfrdmn/substrate/compare/v0.54.0...v0.55.0
[v0.56.0]: https://github.com/scttfrdmn/substrate/compare/v0.55.0...v0.56.0
[v0.56.1]: https://github.com/scttfrdmn/substrate/compare/v0.56.0...v0.56.1
[v0.57.0]: https://github.com/scttfrdmn/substrate/compare/v0.56.1...v0.57.0
[v0.65.0]: https://github.com/scttfrdmn/substrate/compare/v0.64.0...v0.65.0
[v0.64.0]: https://github.com/scttfrdmn/substrate/compare/v0.63.0...v0.64.0
[v0.63.0]: https://github.com/scttfrdmn/substrate/compare/v0.62.0...v0.63.0
[v0.62.0]: https://github.com/scttfrdmn/substrate/compare/v0.61.0...v0.62.0
[v0.61.0]: https://github.com/scttfrdmn/substrate/compare/v0.60.0...v0.61.0
[v0.60.0]: https://github.com/scttfrdmn/substrate/compare/v0.59.0...v0.60.0
[v0.59.0]: https://github.com/scttfrdmn/substrate/compare/v0.58.2...v0.59.0
[v0.58.2]: https://github.com/scttfrdmn/substrate/compare/v0.58.1...v0.58.2
[v0.58.1]: https://github.com/scttfrdmn/substrate/compare/v0.58.0...v0.58.1
[v0.58.0]: https://github.com/scttfrdmn/substrate/compare/v0.57.0...v0.58.0
[Unreleased]: https://github.com/scttfrdmn/substrate/compare/v0.91.0...HEAD
[v0.91.0]: https://github.com/scttfrdmn/substrate/compare/v0.90.0...v0.91.0
[v0.90.0]: https://github.com/scttfrdmn/substrate/compare/v0.89.0...v0.90.0
[v0.89.0]: https://github.com/scttfrdmn/substrate/compare/v0.88.0...v0.89.0
[v0.88.0]: https://github.com/scttfrdmn/substrate/compare/v0.87.1...v0.88.0
[v0.87.1]: https://github.com/scttfrdmn/substrate/compare/v0.87.0...v0.87.1
[v0.87.0]: https://github.com/scttfrdmn/substrate/compare/v0.86.0...v0.87.0
[v0.86.0]: https://github.com/scttfrdmn/substrate/compare/v0.85.0...v0.86.0
[v0.85.0]: https://github.com/scttfrdmn/substrate/compare/v0.84.0...v0.85.0
[v0.84.0]: https://github.com/scttfrdmn/substrate/compare/v0.83.0...v0.84.0
[v0.83.0]: https://github.com/scttfrdmn/substrate/compare/v0.82.0...v0.83.0
[v0.82.0]: https://github.com/scttfrdmn/substrate/compare/v0.81.0...v0.82.0
[v0.81.0]: https://github.com/scttfrdmn/substrate/compare/v0.80.0...v0.81.0
[v0.80.0]: https://github.com/scttfrdmn/substrate/compare/v0.76.0...v0.80.0
[v0.76.0]: https://github.com/scttfrdmn/substrate/compare/v0.75.0...v0.76.0
[v0.75.0]: https://github.com/scttfrdmn/substrate/compare/v0.74.0...v0.75.0
[v0.74.0]: https://github.com/scttfrdmn/substrate/compare/v0.73.0...v0.74.0
[v0.73.0]: https://github.com/scttfrdmn/substrate/compare/v0.72.0...v0.73.0
[v0.72.0]: https://github.com/scttfrdmn/substrate/compare/v0.71.0...v0.72.0
[v0.71.0]: https://github.com/scttfrdmn/substrate/compare/v0.70.0...v0.71.0
[v0.70.0]: https://github.com/scttfrdmn/substrate/compare/v0.69.0...v0.70.0
[v0.69.0]: https://github.com/scttfrdmn/substrate/compare/v0.68.3...v0.69.0
[v0.68.3]: https://github.com/scttfrdmn/substrate/compare/v0.68.2...v0.68.3
[v0.68.2]: https://github.com/scttfrdmn/substrate/compare/v0.68.1...v0.68.2
[v0.68.1]: https://github.com/scttfrdmn/substrate/compare/v0.68.0...v0.68.1
[v0.68.0]: https://github.com/scttfrdmn/substrate/compare/v0.66.1...v0.68.0
[v0.66.1]: https://github.com/scttfrdmn/substrate/compare/v0.66.0...v0.66.1
[v0.66.0]: https://github.com/scttfrdmn/substrate/compare/v0.65.0...v0.66.0
