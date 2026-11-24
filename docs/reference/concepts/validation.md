# Request validation

What substrate checks in a request before a plugin sees it, where that check sits in the
pipeline, and how a replay decides whether to run it.

## What is checked

**A request to a validated service is checked against its operation's published input
constraints, and a request that breaks one is refused before any plugin runs.** The constraints
come from the service's model: substrate vendors botocore's service models
(`emulator/wiremodel/`, cut from one pinned botocore release and pruned to the operations
substrate routes), and `cmd/gen-wire-model` turns them into the tables in
`emulator/wire_model_gen.go`. One interpreter, `emulator/wire_validate.go`, walks those tables
for every protocol.

By default three kinds of constraint are enforced:

- **Required members.** An absent member the input shape requires, or an explicit JSON `null`.
- **Lengths.** A string's length in characters, a blob's in bytes, a list's or a map's in
  entries, against the shape's `min` and `max`.
- **Ranges.** A number against the shape's `min` and `max`, compared exactly, so a `long`'s
  bound is not rounded.

Enum values and patterns are in the tables too, and are checked only for a service that opts
in, on a source showing the service enforces them. A pattern Go's regular-expression engine
cannot compile — botocore's are Java expressions, and lookaround such as `(?!-)` has no Go form
— is not checked even then.

Nested structures and list elements are walked. A map's size is checked; its keys and values
are not.

A request the tables do not cover passes untouched: a service not listed, an operation the
models do not define, or an operation two of a plugin's models define (Elastic Load Balancing's
classic and v2 APIs share names). So does a request whose body will not parse, or that carries a
value of the wrong kind — a string where a list belongs — because the service's answer to a body
it cannot decode is the plugin's own refusal (see [Errors](/reference/concepts/errors)), and
validation has nothing to add to it. A request is read in the wire format it arrived in, among
the protocols its model lists: CloudWatch serves CBOR, JSON and Query, and is read in each. A
request in a format the model does not list, such as the Query form SQS accepts beside its JSON
model, is not validated, and a request is read as Query only when it names an `Action` or carries
a form body.

## Which services are validated

| Service | Operations | Refusal | Enum | Pattern |
|---|---|---|---|---|
| ACM | all routed | `ValidationException`/400 | yes | yes |
| SESv2 | all routed | `BadRequestException`/400 | yes | no |
| Route 53 | all routed | `InvalidInput`/400 | yes | no |
| ElastiCache | all routed | `InvalidParameterValue`/400 | no | no |
| EC2 | key pairs and placement groups: `CreateKeyPair`, `ImportKeyPair`, `DeleteKeyPair`, `DescribeKeyPairs`, `CreatePlacementGroup`, `DeletePlacementGroup`, `DescribePlacementGroups` | `MissingParameter`/400 | no | no |

Each service's codes, statuses and message form are in `emulator/wire_validate_errors.go`,
each with its source, and its reference page says what changed for its callers.

## The message

JSON services answer AWS's front-end validator's form:

```
1 validation error detected: Value 'x' at 'name' failed to satisfy constraint: Member must have length less than or equal to 64
```

and, for several failures, `2 validation errors detected: …; …`. A missing member reads
`Value null at 'name' failed to satisfy constraint: Member must not be null`. A service may
withhold the value: ACM answers `Value of the input at 'domainName' …`, and SESv2 `Value at
'emailIdentity' …`.

Two services answer a missing member in a sentence of their own and name only the first:
ElastiCache, `The parameter SubnetIds must be provided and must not be blank.`, and EC2, `The
request must contain the parameter KeyName`. ElastiCache names the parameter as it is sent; EC2
names it by the model's `locationName` where it has one (`groupName`), as its observed answers
do. A required member present but blank where the model sets no minimum length is refused by the
plugin, with the sentence where the service has one and otherwise with `Member must not be
blank`, which is substrate's own.

The path is the member names from the model with the first letter lowered, joined by dots. A
list element is `<list>.<n>.member`, counting from 1, so the second tag's key is
`tags.2.member.key`. A list of strings whose elements fail is reported once, at the list, with
the element's constraints in brackets:
`Value '[a, b]' at 'names' failed to satisfy constraint: Member must satisfy constraint: [Member must have length less than or equal to 64, …]`.
Failures are listed in the model's member order, depth first; AWS publishes no order, so that
one is substrate's.

## Where it sits in the pipeline

Validation is step 4.6, the last gate before the plugin:

1. Authorization (step 2). A caller who may not make the request learns nothing about what it
   carried.
2. Quota (step 3) and eventual consistency (step 4).
3. Fault injection (step 4.5). An injected fault stands in for the service's whole answer,
   validation included.
4. **Validation (step 4.6).**
5. Clock-driven work and the plugin.

Only a client's request is validated. A request a plugin sends another on a caller's behalf —
a CloudFormation stack creating its resources, the API Gateway proxy invoking a function, the
Scheduler firing a target — goes through the registry directly and is not, so a stack whose
template carries a value the service would refuse from a client still deploys it. A plugin that
keeps its own check for such a request runs the gate's own walk over that member, so it answers in
the gate's words byte for byte, as ACM does for an absent `DomainName` or a malformed
`CertificateArn`.

## Turning it off

`wire.validate: false` in the configuration file, or `SUBSTRATE_WIRE_VALIDATE=false`, turns
validation off for the whole server; `emulator.WithoutWireValidation()` does the same for a
`TestServer`. Every request then reaches its plugin, as every request did before validation
existed.

## Replay

**An event records whether validation ran on it.** When the gate runs live, the recorded
event's `validation_model` names the snapshot it ran against, `botocore/<tag>`. A replay runs
the gate only for an event carrying it, so:

- a recording made before validation existed, or with it off, carries no `validation_model`
  and replays exactly as it did, with the plugin answering every request;
- a recorded refusal replays through the gate to the same refusal, and is never re-executed
  against the plugin;
- an internal dispatch never carries one and is never validated on replay either.

A replay validates against the snapshot of the binary doing the replay, even when the event
names a different one. One snapshot ships per binary, so a stream recorded by an older binary
can replay with a difference that a newer model's constraints explain: a member that became
required, a bound that moved. The recorded `validation_model` says which snapshot the recording
saw, which is where to look when that happens.

## Refreshing the snapshot

`make wire-model-fetch` downloads the pinned botocore tag (`botocoreTag` in
`cmd/gen-wire-model`) and re-prunes the snapshot to the routed operations; `make wire-model`
regenerates the tables, and `make wire-model-check`, which CI runs, fails when they disagree.
Routing a new operation needs a fetch, because the snapshot holds only the operations routed
when it was cut: the check names the operation and says so.
