# ELB v2

**Endpoint:** `elasticloadbalancing.{region}.amazonaws.com`
**Protocol:** AWS Query (form-encoded, `Action=` parameter)

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateLoadBalancer | ALB and NLB supported; accepts `Tags.member.N`. Answers the `LoadBalancer` of `API_LoadBalancer`, including the `AvailabilityZones.member.N` derived from `Subnets.member.N` and the `SecurityGroups.member.N` the create named — both persisted and reported nowhere before [#756](https://github.com/scttfrdmn/substrate/issues/756). Carries no account or Region member |
| DescribeLoadBalancers | `Names.member.N` and `LoadBalancerArns.member.N`. Same load-balancer shape as CreateLoadBalancer. Paged by `Marker`/`PageSize` with `NextMarker` ([#1244](https://github.com/scttfrdmn/substrate/issues/1244); see [Paging](#paging-the-six-paginated-operations)) |
| DeleteLoadBalancer | |
| DescribeLoadBalancerAttributes | |
| ModifyLoadBalancerAttributes | |
| CreateTargetGroup | Accepts `Tags.member.N`. Answers the `TargetGroup` of `API_TargetGroup`; `LoadBalancerArns` is absent, a target group's record holding no association to a load balancer, and the registered targets are reported by `DescribeTargetHealth` as AWS publishes them |
| DescribeTargetGroups | Same target-group shape as CreateTargetGroup. Paged by `Marker`/`PageSize` with `NextMarker` ([#1244](https://github.com/scttfrdmn/substrate/issues/1244)). An ARN in `TargetGroupArns` or a name in `Names` naming nothing is refused `TargetGroupNotFound`/400, and a `LoadBalancerArn` naming nothing `LoadBalancerNotFound`/400 ([#1370](https://github.com/scttfrdmn/substrate/issues/1370), [#1375](https://github.com/scttfrdmn/substrate/issues/1375)) |
| DeleteTargetGroup | |
| ModifyTargetGroup | Answers the modified target group, as `API_ModifyTargetGroup` publishes; it answered an **empty** `TargetGroups` list until [#756](https://github.com/scttfrdmn/substrate/issues/756). An ARN naming nothing is refused `TargetGroupNotFound`/400 ([#1313](https://github.com/scttfrdmn/substrate/issues/1313)) |
| RegisterTargets | An ARN naming nothing is refused `TargetGroupNotFound`/400 ([#1313](https://github.com/scttfrdmn/substrate/issues/1313)) |
| DeregisterTargets | An ARN naming nothing is refused `TargetGroupNotFound`/400 ([#1313](https://github.com/scttfrdmn/substrate/issues/1313)) |
| DescribeTargetHealth | An ARN naming nothing is refused `TargetGroupNotFound`/400 rather than answered an empty list ([#1313](https://github.com/scttfrdmn/substrate/issues/1313)) |
| CreateListener | Accepts `Tags.member.N`. Answers the `Listener` of `API_Listener`; a default action's `Order` is absent rather than reported `0`, no create recording one |
| DescribeListeners | Same listener shape as CreateListener. Paged by `Marker`/`PageSize` with `NextMarker` ([#1244](https://github.com/scttfrdmn/substrate/issues/1244)). An ARN in `ListenerArns` naming nothing is refused `ListenerNotFound`/400, and a `LoadBalancerArn` naming nothing `LoadBalancerNotFound`/400 ([#1370](https://github.com/scttfrdmn/substrate/issues/1370), [#1375](https://github.com/scttfrdmn/substrate/issues/1375)) |
| DeleteListener | An ARN naming nothing is refused `ListenerNotFound`/400 ([#1313](https://github.com/scttfrdmn/substrate/issues/1313)) |
| ModifyListener | Answers the modified listener. An ARN naming nothing is refused `ListenerNotFound`/400 ([#1313](https://github.com/scttfrdmn/substrate/issues/1313)) |
| CreateRule | Accepts `Tags.member.N`. Answers the `Rule` of `API_Rule`, which publishes no `ListenerArn` — the listener is how `DescribeRules` selects, not something it reports |
| DescribeRules | Same rule shape as CreateRule. Paged by `Marker`/`PageSize` with `NextMarker` ([#1244](https://github.com/scttfrdmn/substrate/issues/1244)). An ARN in `RuleArns` naming nothing is refused `RuleNotFound`/400, and a `ListenerArn` naming nothing `ListenerNotFound`/400 ([#1370](https://github.com/scttfrdmn/substrate/issues/1370), [#1375](https://github.com/scttfrdmn/substrate/issues/1375)) |
| DeleteRule | An ARN naming nothing is refused `RuleNotFound`/400 ([#1313](https://github.com/scttfrdmn/substrate/issues/1313)) |
| SetRulePriorities | Answers the rules it repriced in request order, as `API_SetRulePriorities` publishes; it answered an **empty** `Rules` list until [#756](https://github.com/scttfrdmn/substrate/issues/756). A rule ARN naming nothing is refused `RuleNotFound`/400, and every pair is resolved before any is written, so a refused call reprices nothing ([#1313](https://github.com/scttfrdmn/substrate/issues/1313)) |
| AddTags | Up to 50 user tags per resource |
| RemoveTags | |
| DescribeTags | At most 20 resources per request |
| DescribeAccountLimits | Reports 23 limits, seedable; `Marker`/`PageSize` paginated |

## Paging the six paginated operations

All six paginated operations — ELBv2 `DescribeLoadBalancers`, `DescribeTargetGroups`,
`DescribeListeners`, `DescribeRules` and `DescribeAccountLimits`, and classic
`DescribeLoadBalancers` — read `Marker` and `PageSize` through one rule
([#1244](https://github.com/scttfrdmn/substrate/issues/1244),
[#1245](https://github.com/scttfrdmn/substrate/issues/1245)):

- `PageSize` absent is the published default of 400; a value outside the published 1–400, or one
  that is not a number, is refused `ValidationError`/400.
- `Marker` is an opaque cursor substrate issues as `NextMarker`. A marker substrate could not have
  issued is refused `ValidationError`/400, never answered with page one, because a paging loop
  cannot see a cursor that resets ([#915](https://github.com/scttfrdmn/substrate/issues/915)). No
  operation publishes a code for a bad marker; `ValidationError` is the Query Common Errors page's
  code, which each operation links as its own, and the message is substrate's own.
- **The five resource describes page on a value cursor**
  ([#1528](https://github.com/scttfrdmn/substrate/issues/1528)): each lists in **ARN order** (classic
  `DescribeLoadBalancers`: **name order**), and the marker is the shared
  [cursor token](/reference/concepts/listing-order#cursor-pagination) naming the page's last ARN or
  name. The next page starts strictly after it, so a create or delete between pages neither skips
  nor repeats a record that survives, and a marker naming a deleted record resumes where it was. The
  marker is bound to its operation and to the request's filter members (`Names`, `LoadBalancerArns`,
  `TargetGroupArns`, `ListenerArns`, `RuleArns`, `LoadBalancerArn`, `ListenerArn`,
  `LoadBalancerNames`), compared as sets; a marker from another describe or reused with different
  filters is refused `ValidationError`/400. A name listed twice in classic `LoadBalancerNames` is
  listed once.
- `DescribeAccountLimits` lists a fixed set, so its marker is a base64 offset; one past the end
  answers an empty last page.
- `NextMarker` is present when a further page exists and **absent**, not empty, when the walk is
  exhausted, as the v2 pages publish ("Otherwise, this is null").
- The describes page after every named ARN or name has been resolved, so a name naming nothing is
  refused whichever page it would have fallen on.

Every operation whose output shape carries no members answers
`<OperationResponse><OperationResult/><ResponseMetadata>…</ResponseMetadata></OperationResponse>`.
The empty result element is not
decoration: ELBv2 speaks the Query protocol, where each output shape declares a
`resultWrapper`, and botocore looks that wrapper up by name — so a bare
`<OperationResponse/>` makes the AWS CLI and boto3 raise `KeyError` rather than report
success. `DeleteLoadBalancer`, `DeleteTargetGroup`, `DeleteListener`, `DeleteRule`,
`RegisterTargets` and `DeregisterTargets` answered that way and were unusable from a real
client while substrate's own tests passed, because those tests read the XML directly instead
of through an SDK's parser.

## An ARN naming nothing is refused

Eight operations address one resource by ARN, scanned the caller's records for it, and on finding
none fell through to a 200: an empty result list, or an empty success. Each now answers the
not-found code its page publishes, at HTTP 400 with the page's sentence as the message
([#1313](https://github.com/scttfrdmn/substrate/issues/1313)). The three describes that filter by an ARN *list* do the same for any ARN in it
that names nothing ([#1370](https://github.com/scttfrdmn/substrate/issues/1370)):

| Code | Operations | Message |
|------|------------|---------|
| `TargetGroupNotFound` | `ModifyTargetGroup`, `RegisterTargets`, `DeregisterTargets`, `DescribeTargetHealth`, `DescribeTargetGroups` | The specified target group does not exist. |
| `ListenerNotFound` | `ModifyListener`, `DeleteListener`, `DescribeListeners`, `DescribeRules` | The specified listener does not exist. |
| `RuleNotFound` | `SetRulePriorities`, `DeleteRule`, `DescribeRules` | The specified rule does not exist. |
| `LoadBalancerNotFound` | `DescribeTargetGroups`, `DescribeListeners` | The specified load balancer does not exist. |

Some operations are deliberately left alone. `API_DeleteTargetGroup` publishes no not-found code;
its Errors list is `ResourceInUse` alone. `API_DeleteLoadBalancer` does list
`LoadBalancerNotFound`, but its description says *"If the load balancer does not exist or has
already been deleted, the call succeeds"*. That sentence addresses exactly this case and the Errors
list does not, so the delete still succeeds. `Describe`/`ModifyLoadBalancerAttributes` read no ARN
at all.

For the three list-filtered describes, one ARN naming nothing refuses the **whole** call, even beside
ARNs that do name records. None of the three pages says whether a partial match answers the records
it found; each publishes the code with a sentence about the request ("The specified target group does
not exist."), so the request is what is refused. An ARN counts as held if the caller's account and
Region hold a record of that kind under it, whatever the describe's other filters.

The describes' other filters follow the same rule ([#1375](https://github.com/scttfrdmn/substrate/issues/1375)):

- A `Names` entry naming no target group refuses `DescribeTargetGroups` with `TargetGroupNotFound`,
  even beside a name that does.
- A `LoadBalancerArn` naming no load balancer refuses `DescribeTargetGroups` and `DescribeListeners`
  with `LoadBalancerNotFound`.
- A `ListenerArn` naming no listener refuses `DescribeRules` with `ListenerNotFound`.

`DescribeTargetGroups` still does not *narrow* by a `LoadBalancerArn` that does name a load balancer:
a target group's record holds no association to one (the listener holds it in the other direction),
so the filter refuses a value naming nothing and otherwise answers every target group.

## The response envelope, and which plugins still lack it

Every ELB response — both generations, result-bearing and memberless alike — closes with the Query
protocol's `ResponseMetadata`:

```xml
<CreateLoadBalancerResponse xmlns="http://elasticloadbalancing.amazonaws.com/doc/2012-06-01/">
  <CreateLoadBalancerResult>
    <DNSName>my-vpc-loadbalancer-1234567890.us-east-1.elb.amazonaws.com</DNSName>
  </CreateLoadBalancerResult>
  <ResponseMetadata>
    <RequestId>1549581b-12b7-11e3-895e-1334aEXAMPLE</RequestId>
  </ResponseMetadata>
</CreateLoadBalancerResponse>
```

It is built in one place rather than in the ~30 handlers, and that is the substantive part of the
rule. The builder began as ELB's and is now shared by every awsQuery plugin that answers through it
(`query_envelope.go`, #1242): ELB, Redshift, Auto Scaling, RDS and ElastiCache. Substrate emitted the element nowhere in this plugin for exactly the
reason a per-handler convention fails: each handler declared its own inline response struct, so the
member was something every handler could omit, and every handler did. The handlers now pass a
*result* and the envelope decides the root element, the namespace, the result wrapper and the
metadata; the two generations differ in the namespace and in nothing else.

The `RequestId` is the request's own — the value `Event.RequestID` records and `replayRequestID`
reproduces — never a freshly minted one. That is what keeps a replayed ELB response byte-identical to
its recording; an id minted per call would make every ELB body in a recorded stream diverge on
replay, which the replay engine's body comparison reports as a difference.

Which other Query-protocol plugins answer the envelope today:

| Plugin | Envelope | Notes |
|---|---|---|
| CloudFormation, CloudWatch, IAM, SNS, SQS, STS | yes | — |
| ELB (both generations) | yes | all 27 routed actions |
| Redshift | yes | since #1208; the result wrapper was the document root before it, so no SDK decoded it |
| Auto Scaling | yes | operations with no output shape answer no result element |
| RDS | yes | all 23 routed actions since #1242; `DeleteDBSubnetGroup`, `DeleteDBParameterGroup`, `AddTagsToResource` and `RemoveTagsFromResource` answer no result element, as their sample responses show |
| ElastiCache | yes | all 17 routed actions since #1242; `DeleteCacheSubnetGroup` and `DeleteCacheParameterGroup` answer no result element |
| EC2 | its own | a root-level lowercase `<requestId>`, not `ResponseMetadata` — a different document, rendered since #1189; see [Every response carries its requestId](/reference/ec2#every-response-carries-its-requestid) |

An XML **error** response carries the same request ID, in the Query protocol's
`<ErrorResponse>…<RequestId>` document every Query plugin shares. No ELB page publishes a sample error
response, so the document is the protocol's rather than one read off an ELB page; see
[What an error response carries](/reference/concepts/errors#what-an-error-response-carries) (#1241).

## The Classic (2012-06-01) API, and the version that routes it

Elastic Load Balancing is two APIs at one endpoint. `elasticloadbalancing.{region}.amazonaws.com`
serves both the Classic Load Balancer API (`2012-06-01`) and ELBv2 (`2015-12-01`); they share a
signing name, an IAM prefix (`elasticloadbalancing:`) and three **action names** —
`CreateLoadBalancer`, `DescribeLoadBalancers` and `DeleteLoadBalancer` — while publishing different
request members, different response shapes and different errors for each. AWS tells them apart by
the Query protocol's own `Version` parameter. Substrate read `Version` nowhere, so all three action
names were answered by the ELBv2 handler whatever the caller sent
([#844](https://github.com/scttfrdmn/substrate/issues/844)):

| A `Version=2012-06-01` call | answered, before |
|-----------------------------|------------------|
| `CreateLoadBalancer` | `ValidationError`/400 on `Name is required` — ELBv2's member name for what classic spells `LoadBalancerName` |
| `DeleteLoadBalancer` | `ValidationError`/400 on a missing `LoadBalancerArn`, for an operation that publishes **no** errors at all and documents idempotent success |
| `DescribeLoadBalancers` | **HTTP 200 carrying an ELBv2 body** |

The third is the worst of the three, and the reason is the wrapper: *both* generations name their
result element `DescribeLoadBalancersResult`, so botocore finds the wrapper it is looking for and
decodes an **empty `LoadBalancerDescriptions` list**. No error is raised anywhere. A consumer is
simply told it owns no classic load balancers.

**The discriminator is `Version`, and it applies to those three action names only.** An absent
`Version` resolves to ELBv2, as does one substrate does not recognize — every request substrate
already answered, every fixture and every recorded event log therefore answers exactly as it did
before, and no error is invented for a version AWS publishes no code for. The member names are
*not* used to discriminate, because a classic `DescribeLoadBalancers` can legitimately carry no
members at all and would be indistinguishable from an ELBv2 one.

Six operations are routed:

| Operation | Notes |
|-----------|-------|
| CreateLoadBalancer | `LoadBalancerName` + `Listeners.member.N` required; accepts `AvailabilityZones`, `Subnets`, `SecurityGroups`, `Scheme`, `Tags.member.N`; answers **`DNSName` alone** |
| DescribeLoadBalancers | `LoadBalancerNames.member.N`, `Marker`, `PageSize` (1–400, default 400); answers `LoadBalancerDescriptions.member.N` |
| DeleteLoadBalancer | `LoadBalancerName`; an absent load balancer is a **success** |
| AddTags | `LoadBalancerNames.member.N` (**one** name) + `Tags.member.N` of `Tag`; answers no members |
| RemoveTags | `LoadBalancerNames.member.N` (**one** name) + `Tags.member.N` of **`TagKeyOnly`**; answers no members |
| DescribeTags | `LoadBalancerNames.member.N` (1–20); answers `TagDescriptions.member.N` carrying **`LoadBalancerName`** |

Details a consumer can observe:

- **`CreateLoadBalancer` answers `DNSName` and nothing else.** That is the whole of its published
  Response Elements, where the ELBv2 operation of the same name answers the load balancer it made.
  An `internal` scheme prefixes the name with `internal-`, following AWS's published sample.
- **A classic record has its own key space**, so one name can be held by *both* generations at once
  and each generation's `DescribeLoadBalancers` reports only its own. AWS scopes the name per
  generation — each `CreateLoadBalancer` publishes its duplicate-name refusal against its own
  generation only.
- **A classic ARN carries one segment after `loadbalancer/`** (`…:loadbalancer/<name>`) where an
  ELBv2 one carries three (`…:loadbalancer/app/<name>/<id>`). That arity is how the two are told
  apart everywhere, and it comes from AWS's own Service Authorization Reference format strings.
- **Eight of `LoadBalancerDescription`'s sixteen members are absent**, each because the operation
  that would set it is not routed: no instance is registered, no health check is configured, no
  policy or backend-server description exists, and no source security group is minted. `VPCId` is
  absent rather than guessed, because nothing here resolves a subnet to a VPC — it is `omitempty` and
  no classic create persists one.
  `PolicyNames` *is* emitted, as an empty element, because AWS publishes it as one: "The policies.
  If there are no policies enabled, the list is empty."
- **Every refusal is a code the operation's own page publishes**: `ValidationError`/400 for a member
  that is missing or malformed (the consolidated Query Common Errors list), `UnsupportedProtocol`,
  `InvalidScheme` and `DuplicateLoadBalancerName` at 400, `LoadBalancerNotFound`/400 for a name in
  `LoadBalancerNames.member.N` that names nothing, and `InvalidConfigurationRequest` at **HTTP
  409** — the one non-400 among `CreateLoadBalancer`'s twelve published errors — for two listeners
  claiming one `LoadBalancerPort`. **That last mapping is substrate's reading**: `DuplicateListener`
  is published on `CreateLoadBalancerListeners`, an operation this one does not have, so borrowing
  it would invent a code for the page being implemented.
- **A create's `Tags.member.N` reaches the record**, and its published `DuplicateTagKeys`/400 is
  answered before the record is written, so a create carrying a tag it cannot legally apply leaves no
  load balancer behind. The tags are readable through the classic `DescribeTags` and through the
  Resource Groups Tagging API (see the tagging section below), and the same three rules apply
  wherever the tag arrives from, because they are resolved from the record.
- **A store failure is answered as one, and an unusable record is not.** A create whose record could
  not be written still has a DNS name to report and a describe whose listing could not be read still
  has an empty list to report, so both propagate as a 5xx rather than as a plausible success — the
  same silent wrong answer this section exists to remove, arriving by another route. One record the
  listing cannot read among several is the opposite case and is skipped, because failing the call
  would hide every healthy load balancer behind one bad key; the tagging resolvers read such a record
  as absent for the same reason.
- **An unissued `Marker` is refused rather than silently restarting the listing**, at
  `ValidationError`. The operation publishes no token code of its own, so the code comes from the
  Common Errors page that covers it; the refusal itself is substrate's reading, for the reason
  [#915](https://github.com/scttfrdmn/substrate/issues/915) records — a paging loop cannot see a
  cursor that resets. Every paginated ELBv2 operation now answers the same cursor the same way
  ([#1245](https://github.com/scttfrdmn/substrate/issues/1245); see [Paging](#paging-the-six-paginated-operations)).
- **`PageSize` is refused outside its published 1–400**, through the rule both generations now
  share — see [Account limits](#account-limits) for why refusing replaced the fallback the other
  operation had ([#1150](https://github.com/scttfrdmn/substrate/issues/1150)).

### Tags

The three tag operations are the same six action names' other half: `AddTags`, `RemoveTags` and
`DescribeTags` all exist in **both** APIs with different shapes, so each is routed by the same
`Version` member the create/describe/delete trio is
([#844](https://github.com/scttfrdmn/substrate/issues/844)). Before they were routed, a classic
caller's `AddTags` reached the ELBv2 handler and was refused `ResourceArns is required` — a member
the 2012-06-01 API does not have.

What differs between the generations, and therefore what a consumer has to get right:

- **The resource is named, not ARN'd.** Classic takes `LoadBalancerNames.member.N` where ELBv2 takes
  `ResourceArns.member.N`, and so does the response: the classic `TagDescription` carries
  `LoadBalancerName` where ELBv2's carries `ResourceArn`. Substrate's classic responses do **not**
  emit `ResourceArn` at all.
- **`AddTags` and `RemoveTags` take one load balancer.** "You can specify one load balancer only" and
  "You can specify a maximum of one load balancer name" respectively; ELBv2's take a list with no
  published bound. `DescribeTags` takes 1–20 names in both generations. Neither bound has a published
  code of its own, so exceeding it answers `ValidationError`/400 from the Query Common Errors page.
- **Classic `RemoveTags` names the keys as `Tags.member.N` of `TagKeyOnly`, not `TagKeys.member.N`.**
  A request in ELBv2's spelling names no key substrate reads and is refused `ValidationError`/400
  (`Tags` is `Required: Yes`) rather than reporting a removal that did not happen.
- **AWS's `RemoveTags` page contradicts itself about this member, and substrate follows the model.**
  Its parameter table publishes the indexed plural `LoadBalancerNames.member.N`; its Sample Request
  shows singular `&LoadBalancerName=my-loadbalancer`. `AddTags` and `DescribeTags` both publish *and*
  sample the indexed form, so the singular is a typo on one page rather than a second accepted
  spelling — and the indexed form is what every SDK serializes. A reader who copies AWS's own sample
  curl is therefore refused `ValidationError`/400.
- **The cap is 10, not ELBv2's 50, and each generation answers `TooManyTags` in its own published
  wording** ([#1148](https://github.com/scttfrdmn/substrate/issues/1148)). The cap is resolved from
  the stored record rather than from the API door, so one load balancer gets the same answer through
  its own create, through classic `AddTags` and through the Resource Groups Tagging API. Re-tagging a
  key already present on a load balancer at the cap succeeds, which is the published update rule.
- **A load balancer carrying no tags is still reported** by `DescribeTags`, with an empty `Tags`
  list. Omitting it would make "carries no tags" indistinguishable from "is not there", which the
  operation has `LoadBalancerNotFound`/400 to distinguish — and 400 is what all three pages publish
  for an absent load balancer, not the 404 a reader expects.
- **`DuplicateTagKeys`/400 is published on `AddTags` only**, so a key repeated within one
  `RemoveTags` request removes it once rather than being refused.

**What is deliberately not routed**, so that six operations are not read as the whole API:
`RegisterInstancesWithLoadBalancer`, `CreateLoadBalancerListeners`, and the health-check and policy
operations. Any of them answers `InvalidAction`/400, which is the Query family's unknown-action
answer.

Each is its own follow-up rather than one omission, and for one reason: every one of them needs a
`LoadBalancerDescription` member the record does not hold — the registered instance list, the
`HealthCheck` object, the policy collections, the source security group. Adding a door without the
member behind it would report a registration or a health check that no describe could show, which is
a worse answer than `InvalidAction`. A half-modelled instance surface is harder to remove later than
a missing one is to add, so each waits for the issue that models its member.

`TooManyLoadBalancers` and the 20-per-Region quota are not modelled either: substrate enforces no ELB
quota in **either** generation, and enforcing one only would be half-fidelity.

The CloudFormation type **is** routed — see
[the classic type](#the-classic-type) under the resource types below.

## Account limits

`DescribeAccountLimits` reports 23 Elastic Load Balancing limits. **Nothing in substrate
enforces the number it reports** — seeded or defaulted. No ELB operation counts a load
balancer, target group, listener or rule against a quota, and none is planned to. The number
exists to be *read*: it is what a consumer's "am I approaching my quota" branch looks at, and
making it seedable is what lets that branch be exercised. A constant nothing enforces would
be a number that does not come from where it appears to (#885).

```bash
# One limit is nearly exhausted.
curl -X POST http://localhost:4566/v1/elb/account-limits \
  -d '{"name":"application-load-balancers","max":"1"}'

# Every limit at once, for "the whole account is at quota".
curl -X POST http://localhost:4566/v1/elb/account-limits -d '{"name":"*","max":"0"}'

# Clear one seed, or all of them.
curl -X DELETE 'http://localhost:4566/v1/elb/account-limits?name=application-load-balancers'
curl -X DELETE http://localhost:4566/v1/elb/account-limits
```

`name` matches a limit name or `*` (the default) for every limit, resolved specific-first.
`max` is a **string** because the API member is one: `Limit`'s reference gives "Max … Type:
String", which is the member a caller's code will try to treat as an integer.

The provenance of the reported set splits three ways, and the split is the point:

- The **shape** is the API model's — `elasticloadbalancingv2-2015-12-01`'s
  `API_DescribeAccountLimits` and `API_Limit`: `Limits.member.N` of `Max`/`Name`, plus
  `NextMarker`, with `PageSize` valid 1–400 and `Marker` the cursor. The Errors section is
  Common Errors only; the operation publishes no error of its own.
- The limit **names** are **not published on the v2 API reference**. That page enumerates
  none of them — it says only "The name of the limit." and links the Application, Network and
  Gateway Load Balancer quota user guides. The 23 names substrate reports are taken verbatim
  from the example output on the AWS CLI v2 reference page for the operation, the only AWS
  page found that renders the tokens at all. That is an illustrative example, not the model,
  and is recorded as such rather than presented as the published API model. (Classic's
  2012-06-01 `API_Limit` *does* enumerate exactly three — `classic-listeners`,
  `classic-load-balancers`, `classic-registered-instances` — which is a published difference
  between the generations. They are absent because this handler answers v2 shapes; classic
  dispatch is #844.)
- The **values** are the current defaults from those three quota user guides, since a guide
  is AWS's normative statement of a default and the CLI example is an illustration AWS does
  not keep in step with it. Where the two disagree the guide wins:
  `condition-wildcards-per-alb-rule` is 6 per the guide against the example's 5. Two entries
  no guide row names cleanly keep the example's value and say so in the source:
  `target-id-registrations-per-application-load-balancer`, which no guide names at all, and
  `target-groups-per-action-on-network-load-balancer`, whose nearest NLB guide row is "Target
  groups per listener rule action" at 5 — whether that row is this token could not be
  established, so the example's 1 stands.

Two behaviors are substrate's decisions rather than AWS's published text:

| | |
|---|---|
| `NextMarker` | **Absent** when the walk is exhausted, not empty. v2 documents "Otherwise, this is null"; classic documents "If there are no additional results, the string is empty" — a present-but-empty element. Following v2 is following the shape this handler answers. The classic `DescribeLoadBalancers` #844 routed follows v2 here too, which is a divergence from its own page and is recorded as one |
| `PageSize` | An **absent** member is the published default of 400, which is also the published maximum, so an unparameterized call returns the whole set in one page. Anything else must be an integer within 1–400 inclusive, and a value outside it or a non-numeric one is **refused** with `ValidationError`/400 naming the range — the same answer the classic `DescribeLoadBalancers` gives, through the same function ([#1150](https://github.com/scttfrdmn/substrate/issues/1150)) |

**The two operations disagreed until #1150, and the disagreement was recorded here as
deliberate.** It was: this operation substituted its default for an unusable `PageSize` and
answered 200, and the argument for it turned on `DescribeAccountLimits` publishing no
operation-specific error — its Errors section is Common Errors only — so that refusing looked
like inventing a code. [#1064](https://github.com/scttfrdmn/substrate/issues/1064) removed
that step: the consolidated Query Common Errors page each operation's Errors section links as
*its own* publishes `ValidationError` at 400, and a code on the page an operation links is
that operation's own vocabulary rather than a borrowing from a sibling. The second half of the
old argument — that because the default equals the maximum, a `PageSize` above 400 is
indistinguishable from a clamp — was true and was no defense of the low end: a `PageSize` of 0
or -1 was answered with 400 items, the largest page a caller can get in response to asking for
the smallest. And a harness that asks for a page size it believes is legal, reads a 200, and
concludes it is legal then fails against AWS, which is the failure an emulator exists to
prevent.

The family this operation joins now agrees with it. RDS's and ElastiCache's `MaxRecords` and
CloudWatch's took any positive integer and substituted a default for anything else until
[#913](https://github.com/scttfrdmn/substrate/issues/913) made them refuse; EC2's
`DescribeTags` already refused a `MaxResults` outside 5–1000. The code differs because the
page does — `InvalidParameterValue` is what RDS and ElastiCache publish, `ValidationError` is
what ELB's Common Errors page publishes — but the rule no longer does.

**The sweep behind that decision, and its count.** The plugin has six paginated operations:
ELBv2 `DescribeLoadBalancers`, `DescribeTargetGroups`, `DescribeListeners`, `DescribeRules`
and `DescribeAccountLimits`, plus classic `DescribeLoadBalancers`, each publishing `PageSize`
1–400 and `Marker`. Two read the member and answered through one rule after #1150. The other
**four read neither `Marker` nor `PageSize` at all**, which was a third answer rather than a
second; [#1244](https://github.com/scttfrdmn/substrate/issues/1244) gave them the cursor and the
same rule. ELBv2 `DescribeTags` publishes no pagination member and is correctly unpaginated.

`Marker` is an offset into a fixed, name-ordered set, so a paged walk returns each limit exactly
once. **Until [#1245](https://github.com/scttfrdmn/substrate/issues/1245) it was a bare decimal,
and an unparseable or negative `Marker` restarted the walk at page one** — recorded here as a
decision, on the ground that the operation publishes no code for a bad marker. That is the same
"no published code" step #1064 answered for `PageSize`, while the classic `DescribeLoadBalancers`
already refused a marker it never issued, for the reason
[#915](https://github.com/scttfrdmn/substrate/issues/915) gives — a paging loop cannot see a cursor
that resets. The operation now takes the plugin's one cursor rule (see [Paging](#paging-the-six-paginated-operations)): the
marker is a base64 offset into its fixed set, an unissued one is refused
`ValidationError`/400, and an offset past the end stays an empty last page, which is a walk that has
ended rather than a malformed cursor.

Authorization needed nothing: `elasticloadbalancing:DescribeAccountLimits` was **already** in
substrate's generated authorization reference with an empty resource list, which is exactly
why this was worth fixing — a caller could be *granted* a permission substrate then answered
`InvalidAction` for. Every ELB request is decided centrally before dispatch, so the arm in
the action switch was the whole of the wiring the operation needed.

## Tagging

The four creates above store the `Tags.member.N` they are given, and `AddTags`,
`RemoveTags` and `DescribeTags` operate on all four resource types. Before this the
parameter was accepted and silently dropped, and the three tagging operations did not
exist — a `Tags` on a `CreateLoadBalancer` produced an untagged load balancer with no
error.

Limits and shapes come from the `Tag` type's API reference: a key is 1–128 characters, a
value 0–256, both matching `^([\p{L}\p{Z}\p{N}_.:/=+\-@]*)$`, and an ELBv2 resource holds at
most 50 tags. **A Classic Load Balancer holds at most 10**, and that number is the one place
the classic API reference is the *more* specific of the two: the `2012-06-01` `API_AddTags`
opens with *"Each load balancer can have a maximum of 10 tags"*, where the `2015-12-01` page
publishes no maximum anywhere and substrate's 50 is read off the user guide's restrictions
list. The cap is resolved from the record's own kind, so the classic 10 applies to a classic
load balancer's own create, to the classic `AddTags` and to the Resource Groups Tagging API alike
([#1148](https://github.com/scttfrdmn/substrate/issues/1148)). **AWS contradicts itself on
the lengths and substrate follows the model:** the ELB
user guide's restrictions list says "Maximum key length—127 Unicode characters" and
"Maximum value length—255", where the API model says 128 and 256. A caller who trusts the
user guide's smaller numbers is inside substrate's limits either way. A violated constraint
answers `ValidationError`/400: the constraints are the model's, but the model publishes no
error code for breaking one, so the code is the common one a real service answers for a
member outside its constraints and the message text is substrate's own.

Per-operation error sets are followed rather than unified, because AWS's are not uniform:

- `DuplicateTagKeys` is refused on **`AddTags` and `CreateLoadBalancer` only** — the two
  operations whose Errors sections list it. On `CreateTargetGroup`, `CreateListener` and
  `CreateRule` a repeated key resolves last-wins, because inventing a code those three do
  not publish would send a consumer's error branch down a path AWS never triggers.
- `TooManyTags` is published by `AddTags` and by all four creates, so every path that can
  apply a tag can raise it.
- An ARN naming nothing answers the code for its type — `LoadBalancerNotFound`,
  `TargetGroupNotFound`, `ListenerNotFound` or `RuleNotFound` — each at **HTTP 400**, which
  is the ELB API's own choice and not the 404 a reader expects. `TrustStoreNotFound`, the
  fifth code those operations list, cannot occur: substrate models no trust store.
- An ARN naming no ELB resource type at all answers `ValidationError`. So does a **classic**
  load-balancer ARN, whose one segment after `loadbalancer/` is an arity no ELBv2 type has, and
  which these three operations refuse even now that substrate holds classic records: ELBv2's
  `AddTags` enumerates the resources it tags — load balancers, target groups, listeners and rules —
  and Classic is absent from that list. The tagging API reads the same ARN differently; see
  [An ELBv2 resource is reachable through the tagging API](#an-elbv2-resource-is-reachable-through-the-tagging-api).

Two readings are substrate's rather than AWS's published text, both recorded because a
consumer can observe them:

- **A tagging call naming several resources applies to all of them or to none.** Every ARN
  is resolved before any write, so an `AddTags` naming one live load balancer and one absent
  one leaves the live one untouched. AWS documents no ordering for ELB's multi-resource
  tagging calls; the alternative is a partial write a caller cannot undo from the error
  alone. This mirrors `TerminateInstances`, where AWS *does* document the whole-request
  refusal.
- **A tag key beginning `aws:` is accepted.** The restrictions list says "You can't edit or
  delete tag names or values with this prefix", but no reachable page publishes a code for
  that refusal, so substrate does not invent one. The prefix *is* excluded from the 50-tag
  count, which the same list states ("Tags with this prefix do not count against your tags
  per resource limit") and which is byte-for-byte the rule EC2 already implements. This is
  also what lets CloudFormation's `aws:cloudformation:*` stamp coexist with a caller's own
  50 tags.

`RemoveTags` ignores a key that is not present — the operation publishes no error for one —
and refuses more than 128 keys in one request, its own documented array maximum. That cap is
per request and unrelated to the per-resource limit: a request may legally name more keys
than any one resource holds.

## Authorization

Every ELB request naming a resource by ARN is now authorized **against that ARN**. The
members read are `ResourceArns.member.N` (the three tagging operations, up to 20 of them)
and `LoadBalancerArn`, `TargetGroupArn`, `ListenerArn` and `RuleArn`. Previously every ELB
request was decided against the literal string `*`, which is not a wildcard on the request
side: a statement scoped to one load balancer's ARN matched nothing, so an ARN-scoped
`Allow` denied every call and an ARN-scoped `Deny` was inert. A request naming several
resources is denied unless every one of them is allowed.

An ARN that resolves to nothing in state is still the resource the decision is about, with
no tags. Substituting `*` for it would let a bogus ARN reach a statement scoped to `*` that
the real ARN would not have matched; the handler refuses it afterwards on its own terms.

A resource's tags are reported under **both** `aws:ResourceTag/<key>` and
`elasticloadbalancing:ResourceTag/<key>`. The service-specific prefix is the ELB user
guide's: "The `elasticloadbalancing:ResourceTag/{{key}}` condition key is specific to
Elastic Load Balancing. All mutating actions support this condition key." Substrate reports
it on reads too, since a read reporting fewer keys than a write could only make a condition
on a describe unsatisfiable.

`aws:RequestTag/<key>` and `aws:TagKeys` are produced from `Tags.member.N` on `AddTags` and
on the four creates, and from `TagKeys.member.N` on `RemoveTags`. A removal supplies no
value, so the key is recorded with an empty one — indistinguishable from absent to every
condition operator, while still reaching `aws:TagKeys`, which is what a "may only remove
approved tags" policy is written against.

**A tagged create is authorized twice.** AWS: "If tags are specified in the resource-creating
action, additional authorization is required on the `elasticloadbalancing:AddTags` action to
verify if users have permissions to apply tags to the resources being created." So each of
the four creates, when it carries `Tags.member.N`, is decided a second time against
`elasticloadbalancing:AddTags` with `elasticloadbalancing:CreateAction` set to the bare
operation name — and an untagged create needs no tagging grant at all, which is the converse
AWS states explicitly. The second decision's resource is the wildcard for the created type
(`…:loadbalancer/*`, `…:targetgroup/*`, `…:listener/*`, `…:listener-rule/*`); **that is
substrate's reading**, on the same reasoning as EC2's — the resource does not exist yet, and
AWS's own example policies write the `AddTags` statement's Resource as `*` or as a type
wildcard. The pass runs after the create's own decision, honours a permission boundary, and
does not report `aws:ResourceTag/*`: a condition about tags already on a resource that does
not exist yet is unsatisfiable, and fabricating one would let the tags being applied stand
in for tags already present.

Provenance for `elasticloadbalancing:CreateAction`: it is documented on the ELB user guide's
"Tag your Elastic Load Balancing resources during creation" page, which is also where the
two quotations above come from, and it is **absent** from the same guide's own list of
ELB-specific condition keys (which names `ListenerProtocol`, `SecurityPolicy`, `Scheme`,
`SecurityGroup`, `Subnet` and `ResourceTag`). Both Service Authorization Reference pages for
ELB render their key tables in JavaScript and were unreachable. The value is the bare
operation name — AWS's examples write `"elasticloadbalancing:CreateAction": "CreateTargetGroup"`
— not a service-prefixed action. The bundled `AmazonECS_FullAccess` policy's
`ELBTaggingPolicy` statement names four values: `CreateTargetGroup`, `CreateRule`,
`CreateListener` and `CreateLoadBalancer`.

One deliberate gap: **`Names.member.N` on `DescribeLoadBalancers` and `DescribeTargetGroups`
is not resolved to an ARN**, so those two requests are still decided against `*`. Whether
ELB's describes support resource-level permissions could not be verified — the pages that
would say are the unreachable ones above — and leaving them at `*` is the direction that
cannot invent a grant.

## The ARNs substrate mints

A listener and a listener rule are **siblings** of their load balancer's ARN, not children of
it. AWS's four ELBv2 formats, taken from its Service Reference Information document for
`elasticloadbalancing` (Version `v1.4`, vendored at `emulator/authzref/elasticloadbalancing.json`):

| Resource type | Format |
|---------------|--------|
| `loadbalancer/app/` | `arn:${Partition}:elasticloadbalancing:${Region}:${Account}:loadbalancer/app/${LoadBalancerName}/${LoadBalancerId}` |
| `targetgroup` | `arn:…:targetgroup/${TargetGroupName}/${TargetGroupId}` |
| `listener/app` | `arn:…:listener/app/${LoadBalancerName}/${LoadBalancerId}/${ListenerId}` |
| `listener-rule/app` | `arn:…:listener-rule/app/${LoadBalancerName}/${LoadBalancerId}/${ListenerId}/${ListenerRuleId}` |

So a child repeats its load balancer's name and id inside its *own* resource type rather than
being appended to the parent's ARN, and the resource type carries the load balancer's subtype
(`app`, `net`, `gwy` — the abbreviations, not `LoadBalancerTypeEnum`'s
`application | network | gateway`). Substrate asserts a minted ARN against those published
format strings rather than against a template copied into a test, so a drift in either fails.

Both were wrong until [#774](https://github.com/scttfrdmn/substrate/issues/774):
substrate nested the child (`…:loadbalancer/app/<name>/<id>/listener/<suffix>`) and spelled the
subtype `application`. Neither is cosmetic. Every AWS policy example scopes a listener statement
with `…:listener/*`, and that wildcard matched nothing at all against the nested shape — so a
`Deny` written the documented way silently failed to deny, while an `Allow` on `…:loadbalancer/*`
was a prefix of every listener and rule ARN and reached them. The subtype had to be fixed in the
same change: a listener ARN repeats it, so leaving `application` in place would have minted
`listener/application/…` and left `listener/app/*` matching nothing.

A `CreateListener` or `CreateRule` naming a parent ARN it cannot build a child from — a
malformed ARN, or the classic-ELB `…:loadbalancer/<name>` form, which carries too few segments —
answers `ValidationError`/400 rather than minting a child of the wrong arity that no policy
could match.

The **old** nested spelling is still *resolved* by the tagging code, deliberately: an event log
or an exported fixture recorded before #774 carries those ARNs, and a replay whose tagging
calls suddenly named no resource would defeat the property the event store exists to provide.
Nothing mints that shape any more.

## An ELBv2 resource is reachable through the tagging API

All four types are reachable through the Resource Groups Tagging API as of
[#863](https://github.com/scttfrdmn/substrate/issues/863): `TagResources` and
`UntagResources` address one by ARN, and `GetResources` reports it. A tag written either way
is readable through the other — a `TagResources` tag comes back from ELBv2's own
`DescribeTags`, and an `AddTags` tag is reported by `GetResources` — which is the rule every
tagging arm is held to, and which ELBv2 could not meet before: the tagging API had no arm for
the service at all, so the tag store this section describes was reachable only through ELB's
own three operations.

`ResourceTypeFilters` needs no ELB-specific handling. A filter's type is matched against the
ARN's own type segment, and the four segments the formats above publish —
`loadbalancer`, `targetgroup`, `listener`, `listener-rule` — each fall out of that
delimiting. The pre-#774 nested listener ARN is the case worth knowing about: its type
segment is `loadbalancer`, because that is what its ARN says, so a filter and a resource
*kind* are not the same question for a recorded ARN of that shape.

**ELB's arm finds its state key rather than building one, and it is the only one that does.**
A load balancer's and a target group's record is keyed by name, but a listener's and a rule's
is keyed by a suffix substrate mints at create time, which appears nowhere in the ARN. So the
arm delegates to the same resolver ELBv2's `AddTags` uses, rather than rebuilding a key
beside it — the two cannot then disagree about which record an ARN names, which is the defect
that reached SQS, DynamoDB and Lambda when a key was rebuilt
([#826](https://github.com/scttfrdmn/substrate/issues/826),
[#943](https://github.com/scttfrdmn/substrate/issues/943)). One consequence is observable: an
ARN naming no such resource is discovered by the resolver rather than by the write, and it
answers the same `InvalidParameterException`/400 that every other type's absent resource
does, because a caller must not be able to tell which stage found it.

**A classic load-balancer ARN is accepted here and refused at ELBv2's own tag doors, and which
door it arrives at is the whole of the difference.** ELB's tagging code once classified a resource
type by substring, so `…:loadbalancer/my-lb` — AWS's classic format, one segment after the type
where ELBv2's carries three — was read as a load balancer and looked for in the store where
ELBv2's live. Nothing was ever found, but the code a caller got, `LoadBalancerNotFound`, asserted
that an ELBv2 load balancer of that ARN could have existed. Classification is on the segment count
the vendored format strings publish, and that arity check stands. What changed with
[#844](https://github.com/scttfrdmn/substrate/issues/844) is that substrate now *holds* classic
records, and the two callers diverge because AWS's two pages do:

- **ELBv2's `AddTags`, `RemoveTags` and `DescribeTags` still refuse it**, at
  `ValidationError`/400 — matching what they already answer for an ARN of no ELB type. That page
  enumerates the resources it tags and Classic is absent from the list, and it publishes no code
  for a wrong-generation ARN.
- **The tagging API accepts it.** RGT matches on the type segment embedded in an ARN, which both
  generations spell `loadbalancer`, and the Service Authorization Reference lists classic
  `loadbalancer` under a single `AddTags` action. So `TagResources` writes to the classic record and
  `GetResources` — including under `ResourceTypeFilters=elasticloadbalancing:loadbalancer` — reports
  it, which is [#765](https://github.com/scttfrdmn/substrate/issues/765)'s cross-readability rule
  applied to a resource that finally exists.
- **CloudFormation's two tag writers resolve the same way**, one step removed: a template names a
  *resource type* (`AWS::ElasticLoadBalancing::LoadBalancer` against
  `…::ElasticLoadBalancingV2::LoadBalancer`), not an API generation. Both halves are now reachable:
  the classic type gained its deploy helper in
  [#844](https://github.com/scttfrdmn/substrate/issues/844), and because the rule had been recorded
  where the decision belonged, adding it was one map entry rather than a second tagging decision.

A classic ARN naming **nothing** is still refused by the tagging API, at the same
`InvalidParameterException`/400 every other absent resource answers: accepting the generation is
not accepting a resource that is not there.

`GetResources` reports an ELB resource that *has been* tagged, including one whose tags have
since all been removed, which is the general rule
[recorded there](/reference/tagging#getresources-reports-what-has-been-tagged-not-what-is-tagged). Each of the
four records therefore carries the same persisted `ever_tagged` flag every other scanned type
does, written by whichever writer empties the set — ELB's own `RemoveTags` or the tagging
API's `UntagResources`.

**Responses are rendered from the published shape.** All five ELB records — the four ELBv2 ones
(load balancer, target group, listener, rule) and the classic load balancer — already were, as RDS's
and ElastiCache's were: every one of the twelve operations that reports a resource has always
answered through a separate item struct, so substrate's own `AccountID`, `Region`, ARN `Suffix` and
`ever_tagged` could not reach an ELB body even before
[#756](https://github.com/scttfrdmn/substrate/issues/756). What that issue added here is the
assertion that this stays true, across both generations, and none of the four has a published home to
move to: no shape in either API publishes an account or a Region member. The four ELBv2 shapes
publish the resource's ARN, from which a caller recovers both; the classic `LoadBalancerDescription`
publishes no ARN either, the record's own being there for IAM and the tagging API rather than for a
response.

Writing that assertion settled three divergences, noted in the table above. Two load-balancer members
AWS publishes — the `AvailabilityZones` derived from the create's subnets and the `SecurityGroups` it
named — were persisted and reported nowhere, and are now reported, the zones as the
`API_AvailabilityZone` objects v2 publishes rather than the bare strings the classic shape uses. And
two operations answered an empty result for a record they had just written: `ModifyTargetGroup` and
`SetRulePriorities` now report what they modified. The stored fields themselves stay on all five
records, so the state a recorded run replays from is unchanged and the Resource Groups Tagging API —
which reads those records rather than these responses — still reports an ELB resource's tags and
whether it has ever been tagged, in **both** generations.

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::ElasticLoadBalancingV2::LoadBalancer | LoadBalancerArn | `Subnets` and `SecurityGroups` are sent. `Fn::GetAtt` answers every published attribute: `DNSName`, `LoadBalancerName`, `LoadBalancerFullName`, `LoadBalancerArn`; `SecurityGroups` as a list, so `Fn::Select`, `Fn::Join` and a list property see its members; and `CanonicalHostedZoneID`, AWS's static per-Region value for the load balancer's type from the ELB endpoints table (a gateway load balancer, which the table does not cover, is refused) ([#1591](https://github.com/scttfrdmn/substrate/issues/1591)) |
| AWS::ElasticLoadBalancingV2::TargetGroup | TargetGroupArn | `LoadBalancerArns` is refused with the reason: the target group's record holds no association to a load balancer (#1591) |
| AWS::ElasticLoadBalancingV2::Listener | ListenerArn | |
| AWS::ElasticLoadBalancingV2::ListenerRule | RuleArn | |
| AWS::ElasticLoadBalancing::LoadBalancer | the load balancer **name** | classic; see below |

The deployer does not send `Tags` for any of the four ELBv2 types, so a **template's**
resource-level tags are still dropped there. The stack-level tags do arrive: all four carry the
three `aws:cloudformation:*` keys ([#765](https://github.com/scttfrdmn/substrate/issues/765)) and
any tag on the stack itself ([#764](https://github.com/scttfrdmn/substrate/issues/764)), written
straight to the ELB record and readable through `DescribeTags`.

### The classic type

`AWS::ElasticLoadBalancing::LoadBalancer` deploys through the **2012-06-01** `CreateLoadBalancer`
([#844](https://github.com/scttfrdmn/substrate/issues/844)). Before that it had no deploy helper at
all: a stack declaring one reported `CREATE_COMPLETE` for a load balancer no `DescribeLoadBalancers`
of either generation could find. The one thing that makes the deploy classic is the Query `Version`
member — `CreateLoadBalancer` is published by both generations, so without it the same request
creates an Application Load Balancer that still answers a `Ref` and a DNS name.

Six properties are forwarded: `LoadBalancerName` (defaulting to the logical ID), `Scheme`,
`Listeners`, `AvailabilityZones`, `Subnets`, `SecurityGroups` and `Tags`. `Listeners` is the type's
one `Required: Yes` property and the classic create's one required list, so a template omitting it
reports `CREATE_FAILED` rather than a load balancer with no listener. Unlike the four ELBv2 types,
the resource-level `Tags` **are** sent — on the create, so the eleventh tag is refused by the
operation that publishes `TooManyTags` for it — and the stack-level tags are stamped on top.

The sixth published listener member, `PolicyNames`, is dropped: substrate models no classic
load-balancer policies, so forwarding it would name a policy nothing could resolve. The other eleven
properties the CFN type publishes (`AccessLoggingPolicy`, `AppCookieStickinessPolicy`,
`ConnectionDrainingPolicy`, `ConnectionSettings`, `CrossZone`, `HealthCheck`, `Instances`,
`LBCookieStickinessPolicy`, `Policies`) have no home on the record and are likewise dropped — each
needs a `LoadBalancerDescription` member substrate does not hold, which is the same boundary the
unrouted classic operations sit behind.

**`Ref` returns the load balancer's name**, which is what AWS publishes: "When you pass the logical
ID of this resource to the intrinsic `Ref` function, `Ref` returns the name of the load balancer."
Earlier releases of this document recorded the DNS name instead, as a divergence substrate could not
fix; that was a misreading of the Template Reference, and the DNS name is `Fn::GetAtt DNSName`.

| `Fn::GetAtt` attribute | Answered |
|---|---|
| `DNSName` | yes — the name the classic `DescribeLoadBalancers` reports for the same load balancer |
| `CanonicalHostedZoneName` | empty — substrate mints no hosted zone for a classic load balancer |
| `CanonicalHostedZoneNameID` | empty, for the same reason |
| `SourceSecurityGroup.GroupName` | yes — the name of the load balancer's first security group, as the classic `DescribeLoadBalancers` reports it; empty for a load balancer with no security group |
| `SourceSecurityGroup.OwnerAlias` | yes — the owning account's ID; empty when the group name is |

The two hosted-zone answers are deliberately empty
([#1013](https://github.com/scttfrdmn/substrate/issues/1013)): a plausible-looking hosted-zone ID
in a stack `Output` is worse than a missing one, because a consumer asserts against it happily.

### Source security group and VPC

A classic load balancer's create reads EC2 through the plugin registry (#1421), so
`DescribeLoadBalancers` reports three members it used to omit:

- **`VPCId`** is the VPC of the first subnet the create names.
- **`SecurityGroups`**, for a load balancer in a VPC created without any, is the VPC's `default`
  group. The Classic Load Balancer guide says a load balancer created that way in a nondefault VPC
  "is automatically associated with the default security group for the VPC". In a default VPC, AWS
  instead creates one `default_elb_<id>` group per account; substrate does not create that group, and
  uses the VPC's `default` group there too.
- **`SourceSecurityGroup`** is the first of those groups. `GroupName` is the name
  `DescribeSecurityGroups` reports for it, and `OwnerAlias` is the account ID. The account-ID
  owner alias is observed AWS behavior; the `SourceSecurityGroup` page describes the member only as
  "The owner of the security group".

A subnet or group EC2 does not know is still accepted, as before; the member derived from it is
then omitted. A load balancer with neither subnets nor security groups records no source security
group.

## Cost

ELB charges $0.008 per LCU-hour (approximated as flat per-request rate).
