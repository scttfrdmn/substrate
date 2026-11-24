# Service Quotas

**Endpoint:** `servicequotas.{region}.amazonaws.com`
**Protocol:** JSON (`X-Amz-Target: ServiceQuotasV20190624.{Op}`)

Service Quotas answers from a **built-in table of representative default quotas**,
not from the plugins that enforce them. It covers eleven services rather than
AWS's full catalog, which is why an unrecognized service code is an error rather
than an empty result — see below.

## Supported operations

| Operation | Notes |
|-----------|-------|
| ListServices | The service codes substrate publishes quotas for; paginates on `MaxResults`/`NextToken` |
| ListServiceQuotas | `ServiceCode` required; `NoSuchResourceException` for a service not in the table; filters on `QuotaCode` and `QuotaAppliedAtLevel`; paginates — see [Listings paginate](#listings-paginate) |
| ListAWSDefaultServiceQuotas | Each quota's AWS default rather than its applied value; otherwise as `ListServiceQuotas`, without its two filters |
| GetServiceQuota | `ServiceCode` **and** `QuotaCode` required |
| GetAWSDefaultServiceQuota | The AWS default. Nothing here mutates a quota, so it is the applied value for every quota but EC2's `L-1216C47A` — see [EC2 vCPU quotas](#ec2-vcpu-quotas) |
| RequestServiceQuotaIncrease | Records a `PENDING` request; the published value does not move |
| ListRequestedServiceQuotaChangeHistory | The caller's own requests; filterable by `ServiceCode` and `Status` |
| ListRequestedServiceQuotaChangesByService | The same handler, and **not an operation the Service Quotas API has**: the 2019-06-24 model declares `…ChangeHistory` and `…ChangeHistoryByQuota` and nothing resembling this name, so every SDK and CLI call got `InvalidAction` and the plugin's own tests could not reach it (#636). Both names route because a fixture may already drive the invented one directly |
| GetRequestedServiceQuotaChange | `NoSuchResourceException` for an unknown request ID |

The history operation also answers to `ListRequestedServiceQuotaChangesByService`,
which is the name it shipped under and which the Service Quotas API does not have —
the 2019-06-24 model declares `ListRequestedServiceQuotaChangeHistory` and
`ListRequestedServiceQuotaChangeHistoryByQuota` and nothing else of that shape. So
for one release the handler was reachable only by a hand-built `X-Amz-Target`, and
every SDK or CLI call answered `InvalidAction`. The invented name is kept as an
alias so a fixture that already drives it keeps working; new code should use the
real one. `…ChangeHistoryByQuota` is not modelled.

Every quota carries its `QuotaArn`,
`arn:aws:servicequotas:{region}:{account}:{serviceCode}/{quotaCode}`. A default value's
ARN names no account — `arn:aws:servicequotas:us-east-2::ec2/L-1216C47A`, as the CLI
reference's `get-aws-default-service-quota` example shows — because a default belongs to
no account.

## Listings paginate

`ListServices`, `ListServiceQuotas` and `ListAWSDefaultServiceQuotas` take `MaxResults`
(1 to 100, `IllegalArgumentException` outside that) and `NextToken`. With no
`MaxResults` a page holds 100, so every service's quotas fit in one. The last page
carries no `NextToken`, and a token substrate did not issue is
`InvalidPaginationTokenException`, never page one.

`ListServiceQuotas`' `QuotaCode` narrows the listing to that one quota, or to none for a
code the service does not publish. `QuotaAppliedAtLevel` must be `ACCOUNT`, `RESOURCE` or
`ALL`; every quota substrate publishes is account-level, so `RESOURCE` lists none.

## EC2 vCPU quotas

EC2 meters instances in vCPUs per quota class, separately for On-Demand and Spot (#1498).
Substrate publishes both tables from AWS's
[instance-type quotas page](https://docs.aws.amazon.com/ec2/latest/instancetypes/ec2-instance-quotas.html),
under the service name `Amazon Elastic Compute Cloud (Amazon EC2)`:

| Quota code | Name | Applied | Default |
|---|---|---|---|
| `L-1216C47A` | Running On-Demand Standard (A, C, D, H, I, M, R, T, Z) instances | 32 | 5 |
| `L-74FC7D96` | Running On-Demand F instances | 0 | 0 |
| `L-DB2E81BA` | Running On-Demand G and VT instances | 0 | 0 |
| `L-1945791B` | Running On-Demand Inf instances | 0 | 0 |
| `L-417A185B` | Running On-Demand P instances | 0 | 0 |
| `L-7295265B` | Running On-Demand X instances | 0 | 0 |
| `L-34B43A08` | All Standard (A, C, D, H, I, M, R, T, Z) Spot Instance Requests | 5 | 5 |
| `L-88CF9481` | All F Spot Instance Requests | 0 | 0 |
| `L-3819A6DF` | All G and VT Spot Instance Requests | 0 | 0 |
| `L-B5D1601B` | All Inf Spot Instance Requests | 0 | 0 |
| `L-7212CCBC` | All P Spot Instance Requests | 0 | 0 |
| `L-E3A00192` | All X Spot Instance Requests | 0 | 0 |

All twelve are adjustable, regional and in unit `None`, which is what the CLI reference's
`get-service-quota` example shows for `L-1216C47A`. Each names its usage metric:
`AWS/Usage` `ResourceCount` with `Service=EC2`, `Type=Resource`, `Resource=vCPU` and the
class's `Class` (`Standard/OnDemand`, `Standard/Spot`, `G/OnDemand`, …), with
`MetricStatisticRecommendation` `Maximum`. CloudWatch answers that metric — see
[EC2 vCPU usage in `AWS/Usage`](/reference/monitoring#ec2-vcpu-usage-in-aws-usage) — so `Value` minus the
metric's latest value is the headroom a launch has.

`L-1216C47A`'s applied value stays at the 32 substrate has always published, while
`GetAWSDefaultServiceQuota` reports AWS's new-account default of 5. An applied value above
the default is what a real account usually shows, since EC2 raises its On-Demand quotas
automatically with use. Every other EC2 quota's applied value is its default, so
`L-34B43A08` reads 5 — the Spot quota a new account has.

The DL, HPC, High Memory and Trn quotas are not published: none has a usage-metric
`Class` value to name, and substrate's catalog has no DL, HPC or High Memory type to
count.

**The quotas are published, not enforced.** A launch beyond a quota succeeds, as it
always has. Opt-in enforcement — `RunInstances`, `RequestSpotInstances` and Auto Scaling
refusing with `VcpuLimitExceeded` or `MaxSpotInstanceCountExceeded` — is tracked in
#1499.

## An unknown service is a refusal, not an empty list

`ListServiceQuotas` for a service code that is not in the table answers
`NoSuchResourceException`, which the API model declares for this operation. It
previously answered HTTP 200 with `{"Quotas": []}`, and that is a different claim:
*this service exists and publishes no quotas*, rather than *there is no such
service*. Only the second is true of a code substrate does not carry, and the
first sends a caller looking for a missing quota rather than a wrong service name.

`GetServiceQuota` distinguishes the two cases it can refuse for. Both are
`NoSuchResourceException` — the only code the model declares — so the **message**
is the only place they differ: an unknown service names the service and points at
`ListServices`, while a known service with an unknown quota code names the code
and points at `ListServiceQuotas`. Conflating them is what makes a missing service
look like a bad quota code.

A missing required member is `IllegalArgumentException` rather than
`NoSuchResourceException`: the request never named a resource, so reporting one as
absent would be misleading.

## An increase request does not grant anything

`RequestServiceQuotaIncrease` stores a `PENDING` record and returns it. The quota
keeps reading its old value, because AWS grants nothing synchronously — a consumer
that read the quota back expecting its `DesiredValue` would be asserting on a
state real Service Quotas never reaches on that call.

A request is filed under **the account that made it**, and the two history
operations report only that account's requests. Increase requests were previously
filed under the literal `000000000000` regardless of the caller, so two accounts
sharing one emulator shared one pile of requests — and a consumer reading the
history to decide whether it had already asked would see a sibling's request as its
own. A fixture that asserted on `000000000000` will now see the caller's real
account.

## Organizations quotas

The three ceilings the Organizations plugin enforces are readable here at the
values it enforces them at. A quota table that disagreed with its own emulator
would be worse than a missing one: a test written against the published number
would fail for a reason that has nothing to do with the code under test.

| Quota code | Name | Value | Adjustable |
|---|---|---|---|
| `L-E619E033` | Maximum number of accounts | 10 | **yes** |
| `L-29A0C5DF` | Service control policies in an organization | 10,000 | no |
| `L-0F0F51F4` | Organizational units in an organization | 2,000 | no |

All three are `GlobalQuota: true` — Organizations is a global service hosted in
`us-east-1`, so its quotas are not per-region.

Only `L-E619E033`'s code is confirmable from AWS documentation:
[Endpoints and quotas](https://docs.aws.amazon.com/general/latest/gr/ao.html)
publishes a quota code only for the **adjustable** quotas, and that is the
adjustable one. The other two codes are best-effort, and the User Guide notes that
quota codes may change — use `ListServiceQuotas` to discover them rather than
hard-coding. The *values* are documented in
[Quotas for AWS Organizations](https://docs.aws.amazon.com/organizations/latest/userguide/orgs_reference_limits.html)
and match the [Organizations quotas](/reference/organizations#quotas) above.

## Cost

Service Quotas API calls are free.
