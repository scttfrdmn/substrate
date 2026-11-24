# Bedrock Runtime

**Endpoint:** `bedrock-runtime.{region}.amazonaws.com`

**Protocol:** REST-JSON, path-routed. Two API versions, because two services are served here: the
`bedrock-runtime` data plane is 2023-09-30 and the `bedrock` control plane is 2023-04-20.

**Routing:** `bedrock` is aliased to `bedrock-runtime`, because boto3's `bedrock-runtime` client
signs with `bedrock` as the SigV4 signing name in the credential scope when `AWS_ENDPOINT_URL` is
set. One plugin therefore serves two AWS services: the `bedrock-runtime` data plane (`InvokeModel`,
`ApplyGuardrail`) and the four `*ModelInvocationJob` batch-inference operations, which belong to the
`bedrock` control plane.

No inference is performed. `InvokeModel` answers a seeded body or a canned one, `ApplyGuardrail`
answers a deterministic verdict, and a batch job's status is a value a seed sets — all of which is
the point: a consumer's polling and guardrail-handling paths become testable without a model ever
running.

## Supported operations

| Operation | Notes |
|-----------|-------|
| InvokeModel | `POST /model/{modelId}/invoke`. Answers a [seeded response body](#seeding-a-model-response) verbatim, or a canned Claude Messages body naming the requested model, or `{"amazon-bedrock-guardrailAction": "INTERVENED"}` when the guardrail [its headers name](#invokemodel-reads-the-guardrail-headers) intervenes. `accept` is not read |
| ApplyGuardrail | `POST /guardrail/{guardrailIdentifier}/version/{guardrailVersion}/apply`. [`NONE` or `GUARDRAIL_INTERVENED`, decided by a seeded blocklist](#how-a-guardrail-decides) for that guardrail and version |
| CreateModelInvocationJob | `POST /model-invocation-job`. Answers `{"jobArn"}`, exactly the published shape, and records the job as `Submitted` — the first state the page documents, so a batch job is deliberately not terminal at birth. All five members marked `Required: Yes` (`jobName`, `modelId`, `roleArn`, `inputDataConfig`, `outputDataConfig`) are checked, and an absent one is `ValidationException`/400 ([#1174](https://github.com/scttfrdmn/substrate/issues/1174)). The job ID is twelve characters of `[a-z0-9]`, the published `jobArn` pattern, [derived from the request ID](/reference/concepts/seeds-and-replay#an-identifier-a-replay-mints-is-the-one-it-recorded) ([#856](https://github.com/scttfrdmn/substrate/issues/856)) |
| GetModelInvocationJob | `GET /model-invocation-job/{jobIdentifier}`. Returns the stored record whole, so `accountID` and `region` reach the wire ([#756](https://github.com/scttfrdmn/substrate/issues/756)), and reports the job's [observed status](#a-batch-job-s-lifecycle): one read is one observation of its countdown |
| ListModelInvocationJobs | `GET /model-invocation-jobs`. `invocationJobSummaries` of five members each; each summary is one observation of that job's countdown, so a poll on either operation agrees, and one List over N jobs spends one observation of each. Every published query filter is ignored and no `nextToken` is emitted |
| StopModelInvocationJob | `POST /model-invocation-job/{jobIdentifier}/stop`. Leaves the job `Stopping`, then `Stopped`; a terminal job is `ConflictException`/400. See [A batch job's lifecycle](#a-batch-job-s-lifecycle) |

`InvokeModelWithResponseStream`, `Converse` and `ConverseStream` are not routed, so no streaming or
Converse-shaped call is served. Nor is the rest of the `bedrock` control plane —
`ListFoundationModels`, `GetFoundationModel`, `CreateGuardrail`, `GetGuardrail`,
`CreateModelCustomizationJob` and the provisioned-throughput operations among them.

## InvokeModel reads the guardrail headers

Until [#1183](https://github.com/scttfrdmn/substrate/issues/1183) `InvokeModel` read nothing but the
model ID, so an invocation that attached a guardrail was never filtered. It now reads
`X-Amzn-Bedrock-GuardrailIdentifier` (an ID or a guardrail ARN) and `X-Amzn-Bedrock-GuardrailVersion`
(a number or `DRAFT`). When both name a guardrail and its [blocklist](#how-a-guardrail-decides) matches
any string in the request body, the answer is `{"amazon-bedrock-guardrailAction": "INTERVENED"}` in place
of the inference: the member and its value are the page's own example response. The example's
`amazon-bedrock-trace` member, whose contents the page does not publish, is not rendered, and
`X-Amzn-Bedrock-Trace` is not read.

A non-intervening invocation answers exactly what it answered before, byte for byte, so a recorded run
replays identically. A [seeded response](#seeding-a-model-response) wins outright: a response seed is
the caller stating what the wire carries, and a guardrail decision does not rewrite it.

The page lists three request errors, and each is `ValidationException`/400, which the page publishes:

| Request | Answer |
|---------|--------|
| A body with `amazon-bedrock-guardrailConfig` and no guardrail identifier | `ValidationException`/400 |
| A guardrail identifier and no `guardrailVersion` | `ValidationException`/400 |
| A guardrail with a `Content-Type` other than `application/json` | `ValidationException`/400 |
| An identifier or version outside its published pattern | `ValidationException`/400 |
| A version and no identifier | Accepted, and no guardrail is applied: *"If you don't provide a value, no guardrail is applied to the invocation"*, and the page lists no error for it |

These are checked before anything else, so a seeded response does not mask them. The messages are
substrate's own.

## How a guardrail decides

Substrate evaluates no guardrail policy, since content filtering is resource-internal. Whether a
guardrail intervenes is **seeded**: a blocklist names the substrings it blocks, and `ApplyGuardrail` and
`InvokeModel` share the one decision.

```
POST   /v1/bedrock/guardrail-blocklist   {"guardrailIdentifier": "gr1", "guardrailVersion": "1",
                                          "accountId": "…", "region": "…", "terms": ["forbidden"]}
DELETE /v1/bedrock/guardrail-blocklist   (all, or ?guardrailIdentifier=…&guardrailVersion=…&accountId=…&region=… for one)
```

`terms` is required. Each of the other four members defaults to `"*"`, which matches anything, and a
guardrail named by ARN is keyed by the ID the ARN ends in. A request is matched against the most specific
seed only, trying the guardrail's own ID before `"*"`, then its version, then the account, then the
Region. An unseeded guardrail intervenes on nothing.

`ApplyGuardrail` matches every text content item. If a term occurs in one, the response is
`action: GUARDRAIL_INTERVENED` with a fixed output (*"Sorry, I can't help with that."*) and a single
fabricated `topicPolicy` assessment naming `blocked-topic`; otherwise it is `action: NONE` with the first
input echoed back and an empty assessment list. `usage` is a fixed set of counters in both cases.
`InvokeModel` matches every string value in its JSON body, at any depth, so a term matches wherever the
model's request format puts the prompt.

Until #1183 the blocklist was a state key with no Region and no version, writable only by a test that
wrote the state store directly, and `ApplyGuardrail` created it empty on first use. That was a state
write on a read and then a discarded decode error. Both reads now write nothing, and a seed that does not
decode is an error. `ApplyGuardrail` still never answers `ResourceNotFoundException`, since substrate
holds no guardrail resources: any identifier, for any version, is valid.

## Seeding a model response

```
POST   /v1/bedrock-runtime/responses   {"modelId": "anthropic.claude-v2", "body": {…}}
DELETE /v1/bedrock-runtime/responses   (all, or ?modelId=… for one)
```

`modelId` defaults to `"*"`, which matches any model; an exact model ID wins over the wildcard.
`body` is required and is returned verbatim as the response payload, which is what `InvokeModel`
publishes — the member named `body` *is* the HTTP body — so a seeded response is byte-exact.

## A batch job's lifecycle

A job is born `Submitted`, the first status `API_GetModelInvocationJob` documents ("submitted to a queue
for validation"), and an unseeded job stays there: substrate runs no inference, so nothing moves it on
its own. Two seeded countdowns, both on the shared [progression helper](/reference/concepts/seeds-and-replay#how-a-progression-is-seeded),
make every other published status observable. `GetModelInvocationJob` and `ListModelInvocationJobs`
are the observations; each read spends one observation of that job's own counter, and no read rewrites
the stored record ([#1174](https://github.com/scttfrdmn/substrate/issues/1174)).

```
POST   /v1/bedrock/model-invocation-job-status   {"jobId": "…", "pendingObservations": 2,
                                                  "pendingStatus": "InProgress", "status": "Failed",
                                                  "message": "…"}
DELETE /v1/bedrock/model-invocation-job-status   (all, or ?jobId=… for one)

POST   /v1/bedrock/model-invocation-job-stop     {"jobId": "…", "stoppingObservations": 3}
DELETE /v1/bedrock/model-invocation-job-stop     (all, or ?jobId=… for one)
```

**The status seed.** A seeded job reports `pendingStatus` (default `InProgress`) for
`pendingObservations` reads, then the seeded `status` with its `message`, the page's failure message.
`jobId` defaults to `"*"`, and an exact ID wins over the wildcard. `status` is required, and both
statuses must be one of the ten published values, so a misspelled status is refused rather than reported
back (#1162's class). A seed with no `pendingObservations` settles at once, which is the static override
this endpoint was before #1174, so a body written for it reads as it did. Clearing a seed returns the job
to what its own history gave it.

**Stopping.** `StopModelInvocationJob` leaves the job `Stopping`, the page's "This job is being stopped
by a user", for `stoppingObservations` reads, then `Stopped`. Unseeded, one read reports `Stopping`
before `Stopped`; `stoppingObservations: 0` settles a stop at `Stopped` on its first read. Once a job is
stopping, the status seed no longer governs it: the stop is the newer transition.

- A job whose observed status is terminal (`Completed`, `Failed`, `Stopped`, `PartiallyCompleted`,
  `Expired`) answers `ConflictException`/400, which `API_StopModelInvocationJob` publishes. Before
  #1174 the stop overwrote the terminal status with `Stopped`.
- A job already `Stopping` answers success and is left as it is, so a repeated stop neither fails nor
  restarts the countdown. That is substrate's reading: the page names no case for it.
- The stop's precondition peeks rather than observes, so a refused stop spends no observation.
- The stop answers `{}`. The page says "an empty HTTP body".

Both seeds are control-plane writes, so a recording replays them in position and reproduces every
status it observed.

## What a refusal reports

| Condition | Code | Status |
|-----------|------|--------|
| a body that will not parse, on `ApplyGuardrail` or `CreateModelInvocationJob` | `ValidationException` | 400 |
| `CreateModelInvocationJob` without one of its five `Required: Yes` members | `ValidationException` | 400 |
| a batch job that does not exist | `ResourceNotFoundException` | 404 |
| `StopModelInvocationJob` on a job in a terminal status | `ConflictException` | 400 |

All four match what the pages publish. `InvokeModel` also answers `ValidationException`/400 for the
[guardrail request errors](#invokemodel-reads-the-guardrail-headers) its page lists. It reads its body
only to match a guardrail's blocklist, so a body that will not parse is not refused.

The rest of what the pages publish has no site, because none of the conditions is modelled.
`InvokeModel` alone publishes ten errors — `AccessDeniedException`/403, `InternalServerException`/500,
`ModelErrorException`/424, `ModelNotReadyException`/429, `ModelTimeoutException`/408,
`ResourceNotFoundException`/404, `ServiceQuotaExceededException`/400,
`ServiceUnavailableException`/503, `ThrottlingException`/429 and `ValidationException`/400 — of which
substrate answers only `ValidationException`, for the guardrail request errors: there is no quota, no
model readiness, no timeout and no unknown model, so every other invocation succeeds.
`ConflictException`/400 is published on the batch create and the batch stop.
The stop answers it for a terminal job (#1174); the create does not, so a duplicate `jobName` is accepted.
`ModelStreamErrorException` belongs to `InvokeModelWithResponseStream`, which is not routed.

## The account and Region a model invocation job carries reach no response

`BedrockModelInvocationJob` declares its account and Region under wire-visible `json` tags, because the record is what
`MemoryStateManager` snapshots and a replay reads back. Neither reaches a body: `GetModelInvocationJob` used to answer the record whole, and it and `ListModelInvocationJobs` answer `bedrockInvocationJobToWire` now. The other two answer an ARN or an empty object.
`TestBedrockWire_InvocationJobResponsesCarryNoBookkeepingMember` in `emulator/bedrock_wire_test.go` drives all four routed job operations and walks each decoded document for either member at any
depth ([#756](https://github.com/scttfrdmn/substrate/issues/756)). `submitTime` is answered as the date-time string the reference publishes, not the record's epoch number, and each summary now carries the `roleArn` and data configurations `API_ModelInvocationJobSummary` marks Required.

## CloudFormation resource types

None. `AWS::Bedrock::Guardrail` and the other `AWS::Bedrock::*` types are not deployed, so a
guardrail or batch job exists only if an API call creates it.

## Cost

`InvokeModel` and `CreateModelInvocationJob` are attributed $0.000015 per call and `ApplyGuardrail`
$0.000075. Real Bedrock bills per input and output token, and batch inference at half the on-demand
token rate; Substrate counts no tokens, so these are flat per-call proxies that make a cost report
respond to call volume rather than to model size.
