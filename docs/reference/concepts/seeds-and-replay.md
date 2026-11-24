# Seeds and replay

How a seeded progression advances, how a seed survives a replay, and why a replay mints
the identifiers it recorded.

## How a progression is seeded

**A resource whose published lifecycle passes through a non-terminal state can be seeded to report
it for a counted number of observations before it settles.** Substrate completes work in the
request that starts it, so without a seed a resource is in its settled state from the first read,
and a caller's poll, wait or retry loop exits on its first iteration. A seed makes the transition
observable. Every resource that progresses uses one mechanism, `emulator/progression.go`, so the
rules below hold for all of them, and each service's section says only which states it reports and
which member its seed endpoint names.

- **The endpoint.** `POST /v1/{service}/{kind}-status` writes a seed, and `DELETE` removes one
  (`?{id}=…`) or all of them (no query). The body names a resource ID, or `"*"` for every resource
  of the kind, and a count of observations. A seed whose count is negative, or that names a state
  outside the kind's published enumeration, is refused with 400 and stored nothing.
- **The count is in observations, not time.** The simulated clock advances with wall time from its
  baseline, so a duration would make a "still pending" assertion depend on how long the test ran. "The
  next two polls see `pending`" is exactly reproducible, live and on replay.
- **Describes observe; creates and preconditions peek.** Only a describe-style read spends an
  observation. A create reporting the state a resource is born in, or a precondition check refusing
  a resource that is not ready, reads the countdown without spending it, so a count means the same
  number of polls whatever else the test does.
- **The ID outranks the wildcard, and each resource counts separately.** A seed for one resource
  governs it even when a `"*"` seed is also in place. Under a `"*"` seed every resource has its own
  countdown, so one describe over five resources spends one observation of each rather than five
  of a shared one.
- **A seed never rewrites the resource.** It governs what an observation *reports*. Clearing it —
  or `POST /v1/state/reset` — makes every resource read its own record again, and an unseeded
  resource reads exactly as it would with no seed endpoint at all.
- **Seeding restarts the countdowns it governs.** A second `POST` gives a full new progression
  rather than the remainder of the first. A service may also restart a resource's countdown on an
  operation that starts a new transition — a stop after a start — so one seed covers several.
- **A settled resource writes nothing.** The countdown position is stored only while it can still
  change an answer, so polling a settled resource adds no events to the log a replay walks.

The seed is recorded and replayed in position like every control-plane write; see
[How a seed survives a replay](#how-a-seed-survives-a-replay). The two EC2 progressions are the
worked examples: [Seeding a snapshot progression](/reference/ec2#seeding-a-snapshot-progression) and
[Seeding an instance-state progression](/reference/ec2#seeding-an-instance-state-progression).

## How a seed survives a replay

**A seed is recorded as an event and re-applied where it was written, so a stream recorded under
a seed replays under the same seed.** This is stated once here rather than at each of the seeding
sections below, because it is one mechanism serving all of them.

Every seedable outcome in substrate is written through a control-plane endpoint — `POST`/`DELETE
/v1/{service}/…` — rather than through an AWS request, and until
[#1140](https://github.com/scttfrdmn/substrate/issues/1140) only AWS requests were recorded. A
replay resets the whole state manager before re-executing, and a seed lives in the state manager,
so a stream recorded under a seed replayed as the **unseeded** sequence: four `pending` snapshot
observations followed by `completed` came back as five `completed`s. Nothing failed, because every
recorded request was re-executed and every one succeeded — the divergence was in the answers, not
in the count.

A successful control-plane write is now recorded as an event of its own, carrying the method, the
request target *including its query string*, the body and the status. Replaying it re-issues that
request against the emulator's own control plane. Three consequences worth knowing:

- **Position is preserved.** A seed written between two observations is re-applied between the
  same two, so a recording whose first poll answered `completed` and whose second answered
  `pending` — because the seed landed in between — replays in that order. Preserving the seed
  across the reset instead, the obvious smaller fix, could not do this.
- **A consumed count restarts.** The reset still wipes the observation counters and the spend-down
  budgets, and the recorded write re-arms the seed as it was originally armed. That matters most
  for the seeds whose budget is spent *in place* — SQS's queue-miss count and S3's three
  conditional-conflict counters decrement the stored record — where by the end of a recording the
  stored value is zero and there is nothing left to preserve.
- **A `DELETE` replays as the same `DELETE`.** Most clear endpoints name their target in the query
  (`?snapshotId=`, `?bucket=&key=`, `?roleName=`), which is why the event records the full request
  target and not the path alone.

**A replay driven programmatically must be given the handler.** `substrate replay` wires it
itself; a test constructing an `emulator.ReplayEngine` directly passes
`emulator.WithControlPlaneHandler(ts.ControlPlaneHandler())`. Without it a recorded seed is
reported in `SkippedEvents` and the replay answers the unseeded sequence — the behaviour every
replay had before #1140, now visible in the counters rather than silent. Withholding it is
occasionally what a test wants: an unseeded replay is the sharpest check that a seed governs only
what an *observation* reports and was never written back into the resource record.

A few control-plane endpoints are deliberately **not** recorded. `/v1/state/reset`,
`/v1/control/time`, `/v1/control/scale` and `/v1/control/run` are the replay's own business — it
resets the state manager, freezes the clock at each recorded timestamp and dispatches each event
under its recorded request ID itself, so re-applying a recorded reset, time set or reseed would
fight it. `/v1/fault/rules` writes to the fault controller, which the replay rewinds
to the configuration it was armed with, so re-arming would double every rule. The rest write
nothing a replayed AWS observation can read: `/v1/s3/presign` mints a URL, `/v1/pricing/refresh`
reloads a price table, and the pricing discount and credit endpoints write to the cost tracker
rather than to the state manager the reset clears.

## An identifier a replay mints is the one it recorded

**An identifier substrate mints is derived from the request's own ID, so replaying that request
mints the same identifier.** A replayed `CreateVpc` answers with the `vpc-…` the recording
answered with, the recorded `CreateSubnet` that names it succeeds, and a replay of a stream
containing creates can be asserted to produce **zero** differences with state-hash validation on.

Before [#856](https://github.com/scttfrdmn/substrate/issues/856) most identifiers came straight
from `crypto/rand`, and three things followed. A replayed create answered with a *new* identifier,
so the next recorded request naming the old one answered `InvalidVpcID.NotFound` and every later
describe reported an absent item. `ValidateState` reported a `state_hash_after` mismatch for every
stream containing a create — the right answer, and one that made state validation useless. And a
replayed `Authorization` header named an access key absent from replayed state, so the caller
resolved to no principal and authorisation **failed open**.

The derivation is HMAC-SHA256 keyed by the request ID, over a counter that advances with each
identifier the request mints — so one `RunInstances` with `MaxCount=3` publishes three distinct
instance IDs, and two `CreateAccessKey` calls for one user publish two distinct keys even though
the requests are byte-identical. A replay derives from the *recorded* request ID, which is what
makes it reproduce the recording in this process or any other. Each identifier is rendered in the
alphabet its own API publishes: hex for an EC2 resource ID, 21 characters of uppercase base36 for
an IAM `AIDA…`/`AROA…`/`AKIA…`, base64 for an STS session token, a v4-shaped UUID where AWS
documents one.

The cost is that an identifier is **guessable** from a request ID. That is deliberate and already
true of the request ID itself: substrate is a test emulator, its identifiers name nothing outside
it, and none of them is a secret. A caller needing unpredictability from a test double is asking
the wrong tool.

**The migration is complete.** Every service's minted identifiers are derived, and no plugin draws
one from `crypto/rand`: the shared helper they all used is deleted, and the only `crypto/rand` read
left in the tree is the mint's own fallback for a request that has no ID to derive from — which is
what a hand-built request context has, and is how every plugin unit test constructs one.

Two kinds of value are not request-derived, each for its own reason:

- The request ID itself, which is the seed, and substrate's own bookkeeping IDs — an event ID, a
  snapshot ID, a replay ID — which no AWS call observes.
- Anything already derived from its inputs rather than drawn at all: a public IP from its instance
  ID, a NAT gateway's private IP from its gateway ID, a secret's ARN from its name, an
  [unresolvable instance profile's `AIPA…` ID](/reference/ec2#an-instance-s-iaminstanceprofile-is-resolved-from-iam-not-minted-per-read)
  from its own name, and CloudFormation's
  [stack and change-set ARNs](/reference/cloudformation#stack-and-change-set-arns-are-deterministic), which predate this
  rule and are what generalising it was modelled on. A value of this kind is stable across two
  *different* requests, which a request-derived one is not — the property a repeated read needs.

EC2 key-pair material was the last value outside the rule, and is now inside it (#1296). The Go
toolchain's key generators ignore a caller-supplied random reader, so the key is constructed from
mint-derived bytes instead: `ed25519.NewKeyFromSeed` over 32 of them, or an RSA key whose primes
are found by a deterministic search over an HMAC-SHA256 stream keyed by them. A replayed
`CreateKeyPair` answers the same material and fingerprint, and a seedless mint still yields a random
key.

Nine services published an identifier from one shared generator rather than declaring their own, so
they moved together: an ECS task ID, a Step Functions execution name, an SQS message ID,
an EventBridge event ID, a CloudWatch Logs upload sequence token, a Service Quotas request ID, a
Lambda revision ID, a Batch job ID and an EMR Serverless job-run ID were all the same sixteen
derived bytes rendered in UUID *shape* — `8-4-4-4-12` lowercase hex without the RFC 4122 version
and variant bits, which is the form substrate published before it derived them and is unchanged by
deriving them. The EMR Serverless job-run ID has since left that shape: its pages publish
`[0-9a-z]+`, which a dash violates (#1204).

**The shape each service in #1204's batch mints**, checked against its published pattern
([#1204](https://github.com/scttfrdmn/substrate/issues/1204)). Every value is request-derived. A row
with a pattern is asserted on the wire by `TestMintedIDs_SatisfyTheirPublishedPatterns`
(`emulator/minted_id_patterns_test.go`), one subtest per site.

| Service | Identifier | Substrate mints | Published |
|---------|------------|-----------------|-----------|
| EMR Serverless | `applicationId` | `00` + 8 lowercase hex | `[0-9a-z]+`, 1–64 |
| EMR Serverless | `jobRunId`, and the run's `arn` | `00` + 14 lowercase hex; the ARN in its published pattern | `[0-9a-z]+`, 1–64; the ARN `…\/applications\/[0-9a-zA-Z]+\/jobruns\/[0-9a-zA-Z]+`, 60–1024. Was a dashed UUID until #1204 |
| MSK | the UUID in a cluster ARN | version-4 UUID + `-` + one digit | no pattern; the form every MSK page shows. Was name length and clock nanoseconds until #1204 |
| FSx | `FileSystemId` | `fs-` + 16 lowercase hex | `^(fs-[0-9a-f]{8,})$`, 11–21 |
| FSx | Lustre `MountName` | `fsx` for `SCRATCH_1`; otherwise 8 lowercase hex | `^([A-Za-z0-9_-]{1,8})$`. Was 16 hex until #1204 |
| Transfer Family | `ServerId` | `s-` + 17 lowercase hex | `s-([0-9a-f]{17})`, fixed length 19 |
| Timestream | `QueryId` | 32 lowercase hex | `[a-zA-Z0-9]+`, 1–64 |
| Step Functions | an execution name the caller omits | `exec-` (or `sync-`) + 8 lowercase hex | `name` 1–80, no pattern beyond its forbidden characters |
| AWS Backup | plan, version and selection IDs | UUID shape, lowercase | no pattern, no length |
| CodeDeploy | `applicationId`, `deploymentGroupId` | UUID shape, lowercase | no pattern, no length |
| CodeDeploy | `deploymentId` | `d-` + 9 uppercase alphanumerics | no pattern; the page's sample `d-IIMHK0NHC` |
| Redshift | none | — | every identifier Redshift reports is the caller's |

The Transfer server ID and the Timestream query ID were the two sites #1204 found still drawing from
`crypto/rand`; #856 has since derived both, without changing either shape.

One identifier's **alphabet** changed when it was derived, and widened rather than narrowed. An API
Gateway ID is ten lowercase alphanumeric characters; the `crypto/rand` version read five bytes and
mapped each *nibble* through that 36-character alphabet, so only `a` through `p` could appear and a
digit never did. Drawing a byte per character reaches the whole published set, so an API ID,
resource ID, deployment ID, authorizer ID, usage-plan ID or API Gateway v2 route, integration and
mapping ID now looks like one AWS would issue.

**Not every derived value is an identifier.** Three kinds of minted string are in scope because a
caller hands them *back*, so a re-minted one turns a recorded success into a refusal or a silently
incomplete response. A WAFv2 `LockToken` is the clearest: the optimistic-locking contract requires a
caller to return the token to `UpdateWebACL`, so a replay that re-minted it would answer the recorded
update with `WAFOptimisticLockException` instead of the recorded success. A Secrets Manager version
ID is the quietest: `GetSecretValue` reports an unknown `VersionId` by *omitting* `SecretString`
rather than refusing, so a re-minted version ID produces a 200 that has lost a member. And a KMS
`GenerateDataKey` stub data key is observed twice in one response — as `Plaintext` and wrapped inside
`CiphertextBlob` — so both members of a recorded response depend on it. (A recorded `Decrypt` does
not: substrate's ciphertext carries its plaintext, so a replayed decrypt answers from the recorded
blob either way.)

**One service used to mint another's identifiers.** An API Gateway API key's `id` and `value` were
drawn from ACM's certificate-ID generator, which meant a change to ACM's rendering would silently
move API Gateway's. #856 split them into separate minters; both still render the UUID shape they
always did, because neither API publishes a pattern that would decide the question (#671). The same
split was needed inside QuickSight, where `CreateDataSet` drew its SPICE **ingestion ID** from the
generator that mints the `RequestId` every QuickSight response carries: an ingestion ID is a handle —
a recorded `DescribeIngestion` URL contains it — where a request ID is observed once and never sent
back, so one function serving both meant a change to how a request ID is rendered would have moved
the identifier a recorded path depends on.

**The published alphabet decides the rendering, where there is one.** Two identifiers in the
analytics family are minted from sixteen derived bytes apiece and rendered differently, and the
difference is the API model rather than a preference. Redshift Data documents a statement `Id` as a
UUID and publishes `[a-z0-9]{8}(-[a-z0-9]{4}){3}-[a-z0-9]{12}`, so the hyphenated form is required;
Timestream publishes `QueryId` as `[a-zA-Z0-9]+`, which **excludes** the hyphen, so its bytes are
rendered as 32 unbroken hex characters instead. Neither pattern constrains a position, so both are
indifferent to the RFC 4122 version and variant bits — which is why deriving them preserved the exact
rendering substrate published before, rather than quietly setting two nibbles (#671).

**One service in the family publishes no AWS pattern at all.** An OpenSearch document `_id` and
`_scroll_id` come from the domain's own REST API rather than from the `es`/`opensearch` control
plane, so no AWS reference constrains their shape and substrate's sixteen URL-safe base64 characters
are a convention rather than a published form — narrower than the 20-character Flake ID real
OpenSearch generates, and unchanged by #856. Both are values a caller hands back: a generated
document ID is the path of every later `GET`, `PUT` and `DELETE` of that document, and a scroll
cursor goes straight back to `_search/scroll`, where a re-minted one answers a recorded continuation
with `search_context_missing_exception` against a cursor the recording had just opened.

**Three renderings of the same sixteen bytes coexist in the CI/CD family, and the published model
picks each one.** A CodePipeline `pipelineExecutionId` is the only identifier of the five that
publishes a pattern — `[0-9a-f]{8}-[0-9a-f]{4}-…` — and the only draw site that set RFC 4122's
version and variant nibbles; that pattern admits any hex digit in either position, so the nibbles
are substrate's own behaviour rather than a requirement, and they are kept because a consumer
validating the value as a version-4 UUID would start failing if the `4` vanished. CodeBuild's build
ID and CodeDeploy's `applicationId` and `deploymentGroupId` publish no pattern and no length
constraints at all, so they keep the looser UUID *shape* their `crypto/rand` form produced. The
`{projectName}:` a CodeBuild build ID carries is CodeBuild's own composition, not a derived value,
and stays outside the mint.

A CodeDeploy `deploymentId` is the family's one **observed** shape: `CreateDeployment` documents it
as a String with neither a pattern nor length constraints, but the page's own sample response is
`d-IIMHK0NHC`, which is where the `d-` prefix and nine uppercase alphanumeric characters come from.
Reading AWS's example is not the same as inventing a bound from a sibling operation — the rule
[#671](https://github.com/scttfrdmn/substrate/issues/671) settled — and the example is all the
reference offers here. It is also the family's only *addressed* identifier: `GetDeployment` and
`StopDeployment` take it, so a re-minted one answered a recorded read with
`DeploymentDoesNotExistException`. The other four are reported rather than addressed, except
CodeBuild's, which is worse than a refusal — `BatchGetBuilds` reports an unknown ID under
`buildsNotFound` in a **200**, so an underived one stalled a consumer's poll loop instead of
failing it.

**A published pattern outranks the rendering a draw site had.** Everywhere else in #856 the
identifier a caller sees is byte-for-byte what substrate published before it was derived, because the
point of the change is where the bytes come from and not what they look like. Bedrock's batch-inference
job ID is the one exception, and the API model forces it: `CreateModelInvocationJob` publishes `jobArn`
as `arn:aws:bedrock:{region}:{account}:model-invocation-job/[a-z0-9]{12}` and `GetModelInvocationJob`
publishes `jobIdentifier` as that ARN or a bare `[a-z0-9]{12}`, which **excludes the hyphen** and fixes
the length at twelve — and the 36-character UUID substrate minted satisfied neither. The
no-byte-changes rule protects a rendering the model permits; it cannot protect one the model forbids,
so a job ID is now twelve characters of the published alphabet. A consumer that validates the ARN it
was handed, or hands the bare ID back to `GetModelInvocationJob`, is what the pattern exists for.
(`GetModelInvocationJob`'s own sample request shows `BATCHJOB1234`, which the pattern beside it would
reject. The pattern is the model.)

The other three services in that family publish no pattern to follow. An SSM `CommandId` publishes a
**fixed length of 36** and no pattern, which the UUID shape already met; a RAM `resourceShareArn` and
an AWS Backup `BackupPlanId`, `VersionId` and `SelectionId` publish neither, so all four keep the
rendering their draw sites produced (#671). Backup's is the only site in the tree that mints **twice
in one request** — `CreateBackupPlan` answers a plan ID and a version ID, and `UpdateBackupPlan` mints
a third — so the counter is what keeps a plan from being its own version. AWS's own sample plan ARN
shows the UUID in *upper* case where substrate renders lower; that is an observed difference the model
does not require, tracked with the [resource segment](/reference/backup#arn-shapes) rather than with the derivation.

**A request that dispatches another request has to hand the mint along with it.** Counting draw
sites finds the places that call `crypto/rand`; it cannot find a plugin that derives its
identifiers correctly and is *called* with nothing to derive from. CloudFormation's deployer is
that case: deploying a stack turns each resource into an internal EC2, IAM or S3 request, and those
requests were built with a fresh request ID and no mint, so every plugin they reached took the
seedless fallback. The consequence is larger than the count suggested — **every identifier of every
resource in every CloudFormation stack** was undirected randomness, while the deployer itself
contains no draw site at all. A stack's `vpc-…`, its subnet, its security group and its role were
all unreproducible, and a `ValidateState` replay of the outer `CreateStack` reported a
`state_hash_after` mismatch because that hash covers all of them.

Two separate things had to change, and each fixes a different failure. An internal request carries a
mint seeded from its own ID, which is what makes a replayed *internal* event reproduce. And the
internal ID is itself derived from the stack request's ID, which is what makes a replayed
`CreateStack` reproduce — replaying it re-runs the whole deployment, so the internal IDs must come
out the same way twice. The sequence is reproducible because a deploy orders resources by their
dependencies, breaking ties by type priority and then by logical ID, never by map iteration. Both halves are load-bearing and were
measured that way: dropping the mint produces seven differences in a four-resource stack, dropping
the derived request ID produces six. Notably the replay reported every event as a **success** — a
200 describing four resources that were not the recorded ones, which is the silent-wrong-answer
class rather than a visible failure.

Removing substrate's internal request ID from that path also removed it from an AWS response.
`DetectStackDrift` answered a `StackDriftDetectionId` that was literally substrate's `req-…` string,
which no AWS reference describes; it is now UUID-shaped within the published maximum length of 36.
The same dispatch fix applies to an API Gateway proxy integration, which invokes a Lambda through an
internal request. A Lambda event-source-mapping poll derives its request IDs from the request it ran
before, so its identifiers reproduce too; see
[An event-source mapping polls on the simulated clock](/reference/lambda#an-event-source-mapping-polls-on-the-simulated-clock)
([#1292](https://github.com/scttfrdmn/substrate/issues/1292)).

An ECR image digest is not minted at all: it is the SHA-256 of the manifest bytes, so it is
reproducible across a replay because it is content-derived, and it names the image it is the digest
of ([#1283](https://github.com/scttfrdmn/substrate/issues/1283); see
[An ECR digest is the SHA-256 of its manifest](/reference/ecr#an-ecr-digest-is-the-sha-256-of-its-manifest)). It
was minted until #1283, which is a change to what the digest *means* rather than to where its bytes
come from, and so was not this section's to make.

An SQS send mints a message's initial receipt handle and each receive replaces it, matching real
SQS: two `ReceiveMessage` calls that return the same message hand back different handles and only
the most recent one deletes. A replay of either call reproduces the handle that call recorded.
