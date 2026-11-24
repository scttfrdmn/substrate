# Config

**Endpoint:** `config.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: StarlingDoveService.{Op}`)

Note the target prefix. AWS Config's `targetPrefix` is `StarlingDoveService`, an
internal code name bearing no resemblance to the `config` endpoint prefix — and
every aws-sdk-go-v2, boto3 and CLI Config call routes by that target. A plugin
registered without the alias would be fully unit-tested and unreachable from every
SDK, which is what issues #561, #610 and #636 each turned out to be. The end-to-end
journey exists to keep that from recurring.

**Every Config exception is HTTP 400.** Every exception shape in the API model
carries `exception: True` with no `error` member, and every operation's reference
page states "HTTP Status Code: 400" — `NoSuchBucketException` and
`NoSuchConfigRuleException` included. A consumer branching on the status rather than
the code takes a path AWS never sends it down.

**One recorder and one delivery channel per account per Region.** Both `Put`s are
idempotent per the reference: a second call with the same name updates the role and
recording group but does **not** replace creation-time tags. A second *distinct* name
is `MaxNumberOf{ConfigurationRecorders,DeliveryChannels}ExceededException`. Both
names default to `default`, and changing either requires a delete followed by a put.
Every state key carries the Region, so a recorder put in `us-east-1` is absent in
`eu-west-1` — "recording in one Region only" being a misconfiguration that looks
like success from the Region you check.

## Supported operations

| Operation | Notes |
|-----------|-------|
| PutConfigurationRecorder | Idempotent; leaves `recording: false`; empty `roleARN` is `InvalidRoleException` |
| DescribeConfigurationRecorders | Reports the recorder whether or not it is recording — see below |
| DescribeConfigurationRecorderStatus | The only operation that answers "is it recording?"; `ValidationException` on more than one name |
| StartConfigurationRecorder | `NoAvailableDeliveryChannelException` with no channel; a no-op at 200 when already recording |
| StopConfigurationRecorder | A no-op at 200 when already stopped |
| DeleteConfigurationRecorder | `NoSuchConfigurationRecorderException`; no ordering precondition is documented |
| PutDeliveryChannel | `NoAvailableConfigurationRecorderException` **before** the bucket is looked at; then `NoSuchBucketException`, then `InsufficientDeliveryPolicyException` |
| DescribeDeliveryChannels | |
| DescribeDeliveryChannelStatus | `Not_Applicable` until the recorder first starts, then `Success`; seedable to `Failure` |
| DeleteDeliveryChannel | `LastDeliveryChannelDeleteFailedException` while the recorder records |
| PutConfigRule | Mints `ConfigRuleId`/`ConfigRuleArn` and refuses a *create* supplying either; an update may name the rule by Name, Id or Arn; 1000 rules per Region |
| DescribeConfigRules | Paginated, cap 100; honours the `EvaluationMode` filter; `ConfigRuleNames` 0–25 |
| DeleteConfigRule | Removes the rule's compliance seed and recorded evaluations with it |
| DescribeComplianceByConfigRule | `INSUFFICIENT_DATA` unless seeded — never computed |
| GetComplianceDetailsByConfigRule | Cap 100; `ComplianceTypes` 0–3; recorded `PutEvaluations` results outrank the seed |
| PutEvaluations | `ResultToken` required; `TestMode` stores nothing; `Evaluations` 0–100 and optional |
| PutConformancePack | Exactly one of `TemplateS3Uri`/`TemplateBody`/`TemplateSSMDocumentDetails`; 60 input parameters; 50 packs per Region |
| DescribeConformancePacks | Page size cap 20 |
| DescribeConformancePackStatus | `CREATE_IN_PROGRESS` → `CREATE_COMPLETE` on first observation; cap 20 |
| DescribeConformancePackCompliance | Cap 1000; seeded per rule |
| GetConformancePackComplianceSummary | Cap 20; `ConformancePackNames` 1–5 |
| DeleteConformancePack | `ResourceInUseException` while a create or delete is in flight |
| TagResource | Recorders, rules and packs; 50 tags; an `aws:`-prefixed key is refused |
| UntagResource | An absent key is a no-op, not a refusal |
| ListTagsForResource | Paginated; `Limit` capped at **100** — see Provenance below |

## A recorder that exists is not a recorder that records

This is the behaviour the release exists for. `DescribeConfigurationRecorders`
reporting a recorder says **nothing** about whether anything is being recorded; that
is `DescribeConfigurationRecorderStatus.recording`. A recorder created and never
started is the single most common real Config misconfiguration, and a consumer that
checks only the first call reports an account as covered while nothing is recorded.

So `PutConfigurationRecorder` leaves `recording: false` and only
`StartConfigurationRecorder` flips it. The two states are indistinguishable through
the operation most consumers reach for first, which is precisely why substrate
models them separately.

The ordering refusals are the other half, and they make a consumer's sequencing bug
observable instead of silently tolerated:

```
StartConfigurationRecorder, no channel   → NoAvailableDeliveryChannelException
PutDeliveryChannel, no recorder          → NoAvailableConfigurationRecorderException
PutConfigRule, no recorder               → NoAvailableConfigurationRecorderException
DeleteDeliveryChannel, recorder running  → LastDeliveryChannelDeleteFailedException
```

The last one is what makes a teardown-and-rebuild fixture — the same test run twice
— express an ordering requirement rather than pass on a sequence AWS rejects:

```
Put recorder → Put channel → Start → Delete channel   LastDeliveryChannelDeleteFailed
Stop → Delete channel → Delete recorder → Put recorder → Put channel → Start   all 200
```

## The delivery-policy check reads real S3 state, permissively

`PutDeliveryChannel` is the one Config operation whose success depends on another
service. A missing bucket is `NoSuchBucketException`; a bucket with **no policy at
all** is `InsufficientDeliveryPolicyException`, which is certain rather than a guess
about a policy's contents.

Where a policy does exist, the matcher passes if **any** `Allow` statement's
principal covers `config.amazonaws.com` or `*` and its action covers `s3:PutObject`
— including `s3:Put*`, `s3:*` and `*`. The resource ARN is **not** matched, and a
policy shape substrate's parser cannot decode **passes**, because refusing there
would be substrate blaming the consumer for its own limitation.

That asymmetry is deliberate. The two failure directions are not equivalent: always
accepting would make a bucket-policy bug invisible here and fatal at AWS, while
demanding the exact documented policy would refuse policies AWS accepts — and a
wrong refusal breaks working code, which is the worse failure. The seed below exists
for both edges of that choice.

## Compliance is seeded, never computed

Substrate does not evaluate Config rules, and will not. Evaluating a rule against
resource state is workload-internal rather than an API observation, so it falls
outside what substrate models. Computing it would mean reimplementing hundreds of
AWS-managed rules, and — worse — would make a consumer's compliance assertion
silently change meaning as unrelated plugins gained fidelity.

An unevaluated rule therefore reports **`INSUFFICIENT_DATA`**, which is what AWS
reports for a rule that has not evaluated. A default of `COMPLIANT` would make every
consumer's compliance assertion pass for free, which is worse than no answer.
Anything else comes from a seed.

`PutEvaluations` is the exception that proves the rule: a custom rule reporting its
own result *is* an API observation, so what a caller submits is recorded — and where
a rule has recorded evaluations, `GetComplianceDetailsByConfigRule` reports those in
preference to the seed. A custom rule's own report outranks a fixture default.

## Control plane

```bash
# A recorder reporting a failure. lastStatus must be a RecorderStatus member;
# lastErrorCode/lastErrorMessage apply only to Failure.
curl -X POST localhost:8080/v1/config/recorder-status \
  -d '{"lastStatus":"Failure","lastErrorCode":"InsufficientDeliveryPolicy",
       "lastErrorMessage":"Cannot write to the bucket"}'
curl -X DELETE localhost:8080/v1/config/recorder-status        # ?accountId=&region= to narrow

# A delivery stream that cannot write. Note Not_Applicable carries an underscore —
# the DeliveryStatus enum spells it differently from RecorderStatus's NotApplicable.
curl -X POST localhost:8080/v1/config/delivery-status \
  -d '{"status":"Failure","lastErrorCode":"AccessDenied"}'
curl -X DELETE localhost:8080/v1/config/delivery-status

# Force or suppress the bucket-policy refusal regardless of real S3 state:
# "insufficient" for a consumer with no S3 fixture, "ok" for one whose valid policy
# substrate's permissive matcher still cannot read.
curl -X POST localhost:8080/v1/config/delivery-policy \
  -d '{"bucket":"cfg-logs","outcome":"insufficient"}'
curl -X DELETE 'localhost:8080/v1/config/delivery-policy?bucket=cfg-logs'

# A rule's verdict. A {name} of "*" seeds every rule.
curl -X POST localhost:8080/v1/config/rule-compliance/s3-encrypted \
  -d '{"complianceType":"NON_COMPLIANT","annotation":"Bucket b1 is unencrypted",
       "resources":[{"resourceType":"AWS::S3::Bucket","resourceId":"b1"}]}'
curl -X DELETE localhost:8080/v1/config/rule-compliance/s3-encrypted

# A conformance pack's state, and its per-rule verdicts.
curl -X POST localhost:8080/v1/config/pack-status/ops -d '{"state":"CREATE_FAILED",
       "statusReason":"The template could not be read"}'
curl -X POST localhost:8080/v1/config/pack-compliance/ops \
  -d '{"rules":[{"configRuleName":"iam-password-policy",
       "complianceType":"NON_COMPLIANT","controls":["CIS 1.5"]}]}'
curl -X DELETE localhost:8080/v1/config/pack-status/ops
curl -X DELETE localhost:8080/v1/config/pack-compliance/ops
```

Every seed is applied at **read** time rather than written into the resource, so
clearing one restores the real state instead of leaving the seeded value behind.
Seeds live in their own `config-ctrl` namespace so a seeded status is never mistaken
for a real one in a state dump or during replay.

Until [#1320](https://github.com/scttfrdmn/substrate/issues/1320) that was not true of
the recorder. `StartConfigurationRecorder` and `StopConfigurationRecorder` read the
*seeded* status and wrote it back, so a seeded `Failure` and its error code survived
the seed being cleared, and a Start stored `Success` alongside the seeded
`lastErrorCode` — the combination the seed endpoint itself refuses. Both now read the
stored status, and only `DescribeConfigurationRecorderStatus` applies the seed. A
Start also clears `lastErrorCode` and `lastErrorMessage`, which `ConfigurationRecorderStatus`
describes as "from when the recorder last failed", so a status stored before the fix
does not carry them forward under `Success`.
`TestConfigSeeds_AreAppliedAtReadTimeNotWrittenIntoTheResource` runs every sequence of
recorder writes with and without the seed, and requires the stored records to be
byte-identical once it is cleared.

A seed that would be **silently ignored is refused**, which is the rule the whole
family follows: a `lastErrorCode` on a non-`Failure` status, a `statusReason` on a
pack state that is not one of the two failures, `resources` alongside
`INSUFFICIENT_DATA` (which the `EvaluationResult` shape does not support), a body
naming a different rule than the path, the same rule name twice in one
pack-compliance seed, and any value outside its enum are all a 400. In particular
`NOT_APPLICABLE` is refused for both a rule's verdict and a pack's: it is in the
rule-level `ComplianceType` enum but not in the `Compliance` shape's subset, and
`ConformancePackComplianceType` has no such member at all. Storing one would make
substrate report a value no SDK enum member matches, so a consumer's `switch` would
fall through to its default while the test passed asserting nothing.

### A seed's accountId and region scope the seed, and reach no AWS response

The `accountId` and `region` a seed body carries — and the `?accountId=&region=` a
clear accepts — are the seed's **own** scope: they are read to build its state key, and
a lookup tries the exact pair first, then either half wildcarded, then both. That is how
a fixture seeds "every account in `eu-west-1`" or "this account in every Region", and
why omitting both is the same as `"*"`.

They are therefore not the same thing as the account ID and Region that
[#756](https://github.com/scttfrdmn/substrate/issues/756) is otherwise about. Elsewhere
those are substrate's bookkeeping on a *resource* record that a handler then marshals
onto the wire, and the fix is to answer from a projection instead. Here the fields belong
to substrate's own control-plane request, they appear on no AWS Config shape, and no AWS
response carries one: each seed is read at request time and its *named* values are copied
onto the published shape. The seven responses that consult a seed are

| Seed | Responses |
|------|-----------|
| `recorder-status` | `DescribeConfigurationRecorderStatus` |
| `delivery-status` | `DescribeDeliveryChannelStatus` |
| `rule-compliance` | `DescribeComplianceByConfigRule`, `GetComplianceDetailsByConfigRule` |
| `pack-status` | `DescribeConformancePackStatus` |
| `pack-compliance` | `DescribeConformancePackCompliance`, `GetConformancePackComplianceSummary` |

and `TestConfigWire_SeededResponsesCarryNoSeedScopeMember` asserts on the raw bytes of
every one of them that neither member appears at any depth. Four more operations consult
a seed and carry none of it: `StartConfigurationRecorder` and `StopConfigurationRecorder`
answer an empty body, and `PutConformancePack` and `DeleteConformancePack` read a seeded
pack state only to decide whether the call is allowed.

## Conformance pack status advances on observation

`PutConformancePack` returns `CREATE_IN_PROGRESS`, and the first
`DescribeConformancePackStatus` resolves it to `CREATE_COMPLETE` and **persists**
that, so every later observation reports the same state and the same timestamp. A
waiter converges in one poll with no wall-clock dependence, and a status that
re-resolved on each read would make a poll-comparing waiter loop forever. A seeded
state does not advance — that is the point of seeding it.

`ConformancePackStatusDetail` carries all six required members, including a
synthesized `StackArn` of the form
`arn:aws:cloudformation:{region}:{account}:stack/awsconfigconforms-{name}-{id}/{uuid}`,
matching the `awsconfigconforms` stack-name convention Config uses for the
CloudFormation stack it deploys a pack through.

## CloudFormation

`AWS::Config::ConfigurationRecorder`, `AWS::Config::ConfigRule` and
`AWS::Config::DeliveryChannel` dispatch the real API operations. Earlier releases had
all three reporting `CREATE_COMPLETE` while creating nothing.

A template carrying both a recorder and a channel ends with `recording: true`, per
"AWS CloudFormation starts the recorder as soon as the delivery channel is
available", and it does so no matter which order the two are declared in — ordering
is carried by the deployer's type priority rather than by `DependsOn`. A template
carrying only a recorder leaves it stopped, which is what a consumer's stack actually
depends on.

`Ref` on the recorder and the channel returns the **name** (`default`, in the CFN
page's own example); neither exposes any `Fn::GetAtt` attribute, because that section
of each page is empty and exposing one would be an invention. `AWS::Config::ConfigRule`
does expose `Arn`, `ConfigRuleId` and `Compliance.Type`, which its page documents.

CloudFormation marks `RoleARN` *Required: Yes* while the API model says No, so that
refusal lives at the CloudFormation layer. Note also that CloudFormation spells the
recorder's and channel's nested members UpperCamel where the API spells them
lowerCamel (`RecordingGroup.AllSupported` vs. `recordingGroup.allSupported`); the
deployer translates them, because real Config is case-sensitive and would silently
ignore the UpperCamel form.

`AWS::Config::ConformancePack` remains unsupported in CloudFormation even though its
API operations exist.

## Provenance

- **`ListTagsForResource`'s `Limit` is documented two ways.** The prose says "The
  limit maximum is 50. You cannot specify a number greater than 50"; the Valid Range
  says 0–100. Substrate takes **100**, the API model's bound, so it never refuses a
  request the model permits.
- **The ARN templates come from the Service Authorization Reference**, which the API
  reference does not give: `config-rule/${ConfigRuleId}`,
  `conformance-pack/${Name}/${Id}`, `delivery-channel/${Name}`. The recorder's is
  two-segment — `configuration-recorder/{name}/{id}` — which corrected substrate's own
  pre-existing CloudFormation stub, that had minted `recorder/{name}`.
- **`roleARN` is required though the model does not say so**: "While the API model
  does not require this field, the server will reject a request without a defined
  `roleARN`." The exception is a **null-or-empty check**, not an assumability check,
  so a role that was never created is accepted — as AWS accepts it. Verifying
  assumability would refuse requests AWS accepts.
- **Three documented members are absent from the vendored botocore model** —
  `ConfigurationRecorder.connectorArn`, `ConfigurationRecorder.scopeConfiguration` and
  `ConfigRule.RuleEvaluationVisibility`. Substrate models the vendored shape and does
  not emit them.
- **Recorder and delivery-channel maxima are not on the service-limits page.** The
  "one per account per Region" note on each operation is what makes
  `MaxNumberOf…ExceededException` reachable at a count of two.
- **There is no delivery-channel resource type in the Service Authorization
  Reference's list of Config resource types**, so `TagResource` accepts recorders,
  rules and packs only.

## Cost

Config API calls are free. Substrate does not model Config's per-configuration-item
or per-rule-evaluation charges, because it records no configuration items and
evaluates no rules.
