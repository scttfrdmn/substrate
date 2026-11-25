# Route 53

**Endpoint:** `route53.amazonaws.com` (global)
**Protocol:** REST/XML

## Supported operations

| Operation | Notes |
|-----------|-------|
| CreateHostedZone | Returns HTTP 201; zone IDs prefixed `/hostedzone/Z`. Stores the zone's [default SOA and NS record sets](#the-default-soa-and-ns-record-sets), so `ResourceRecordSetCount` starts at 2 |
| GetHostedZone | |
| DeleteHostedZone | `NoSuchHostedZone`/404 for an absent zone, and `HostedZoneNotEmpty`/400 for a zone holding any record set but its apex SOA and NS — see [Deleting a hosted zone](#deleting-a-hosted-zone) |
| ListHostedZones | A zone record that cannot be read or decoded is an error, not a zone left out |
| ChangeResourceRecordSets | CREATE/DELETE/UPSERT actions; [`NoSuchHostedZone`](#a-record-set-needs-its-hosted-zone) for an absent zone. A CREATE of an existing record set, a DELETE of a missing one or one whose values differ, and a double DELETE are [`InvalidChangeBatch`/400](#a-change-batch-is-validated-whole), and the batch makes no change. So is a batch that would leave the zone without exactly one apex SOA, or without an apex NS ([defaults](#the-default-soa-and-ns-record-sets)). A record set is keyed by name, type and `SetIdentifier`; a name with and without its trailing dot, in any case, or with a character written as its [octal escape](#record-set-names) (`*` as `\052`) is one record set; a `Type` outside the published upper-case values, or a `TTL` or `Weight` that is not a whole number, is `InvalidInput`/400 |
| ListResourceRecordSets | [`NoSuchHostedZone`](#a-record-set-needs-its-hosted-zone) for an absent zone. Names are [reported](#record-set-names) lower-cased, escaped and with their trailing dot. Reports `SetIdentifier`, `Weight`, `Region`, `Failover` and `HealthCheckId`. A record set that cannot be read or decoded is an error, not a record set left out |
| GetChange | The change every `CreateHostedZone`, `DeleteHostedZone` and `ChangeResourceRecordSets` minted; `INSYNC` unless [seeded](#change-status-and-the-getchange-waiter). The ID is accepted with or without its `/change/` prefix. `NoSuchChange`/404 for an unknown ID |
| ListHostedZonesByName | Sorted by name with the labels reversed, then by ID; `dnsname`/`hostedzoneid`/`maxitems` paging with `NextDNSName`/`NextHostedZoneId` ([details](#listing-hosted-zones-by-name)). `InvalidDomainName`/400 for a bad `dnsname`, `InvalidInput`/400 for a `hostedzoneid` without one |
| UpdateHostedZoneComment | Replaces the zone's comment; an absent `Comment` clears it. `NoSuchHostedZone`/404 |
| ChangeTagsForResource | `hostedzone` and `healthcheck` ([tags](#tags)); at most 10 tags per resource. `NoSuchHostedZone`/`NoSuchHealthCheck`/404 for an absent resource, `NoSuchHealthCheckSource`/404 for every `healthchecksource` |
| ListTagsForResource | One `ResourceTagSet`, tags in key order |
| ListTagsForResources | One `ResourceTagSet` per ID, in request order; an absent resource fails the request |
| CreateHealthCheck | HTTP 201 with a `Location` header; UUID IDs. [`CallerReference` is idempotent](#health-checks): the same settings return the existing check, other settings or a deleted check's reference are `HealthCheckAlreadyExists`/409. `TooManyHealthChecks`/400 past 200 per account. Endpoint types get the published `RequestInterval` 30 and `FailureThreshold` 3 defaults |
| GetHealthCheck | `NoSuchHealthCheck`/404 for an absent check or another account's |
| ListHealthChecks | Creation order; `marker`/`maxitems` paging with `NextMarker` |
| UpdateHealthCheck | Present members replace, `ResetElements` clear; `HealthCheckVersion` increments. A version that differs from the stored one is `HealthCheckVersionMismatch`/409 |
| DeleteHealthCheck | Keeps the caller reference, so it cannot be reused |
| GetHealthCheckStatus | [Seeded](#health-check-status-is-seeded), never computed: healthy by default, one observation per checker Region. A `CALCULATED` check is `InvalidInput`/400 |
| GetHealthCheckCount | The caller's health checks |

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::Route53::HostedZone | HostedZoneId | |
| AWS::Route53::RecordSet | — | Fails its resource when `HostedZoneId` names no hosted zone. `SetIdentifier`, `Weight`, `Region`, `Failover` and `HealthCheckId` are sent. The stack delete finds the record set by name, type and `SetIdentifier` and sends its stored values; one already gone, or gone by the time the batch is sent, counts as deleted, and a `RecordSetGroup` naming one record set twice deletes it once |
| AWS::Route53::HealthCheck | HealthCheckId | `HealthCheckConfig` and `HealthCheckTags`; `Fn::GetAtt HealthCheckId`. Each create attempt sends its own `CallerReference`, so a stack deleted and deployed again, or a second stack, creates its own health check. A health check already gone counts as deleted |

## Change status and the GetChange waiter

`API_GetChange`: *"`PENDING` indicates that the changes in this request have not propagated to all
Amazon Route 53 DNS servers managing the hosted zone. This is the initial status of all change batch
requests. `INSYNC` indicates that the changes have propagated."* Terraform's `aws_route53_zone` and
`aws_route53_record`, and the SDK waiters, poll `GetChange` until `INSYNC`.

Every `ChangeInfo` that `CreateHostedZone`, `DeleteHostedZone` and `ChangeResourceRecordSets` answer
is stored with its `SubmittedAt` and `Comment`, so `GetChange` can report it. Substrate applies a
change in the request, so an unseeded change is `INSYNC` from its first observation. A seed makes the
propagation window observable:

```bash
# Every change reports PENDING for two GetChange calls, then INSYNC.
curl -X POST http://localhost:4566/v1/route53/change-status \
  -d '{"changeId":"*","pendingObservations":2}'
# One change; the ID may carry its /change/ prefix.
curl -X POST http://localhost:4566/v1/route53/change-status \
  -d '{"changeId":"/change/C0123456789ABCDEF","pendingObservations":1}'
# Clear one seed, or all of them.
curl -X DELETE 'http://localhost:4566/v1/route53/change-status?changeId=C0123456789ABCDEF'
curl -X DELETE http://localhost:4566/v1/route53/change-status
```

The countdown is counted in observations, not time ([progressions](/reference/concepts/seeds-and-replay)).
`GetChange` spends one; the response of the operation that minted the change reports the first
observation without spending it. Each change keeps its own countdown, so a wildcard seed governs every
change separately. The seed is a recorded control-plane write and replays in position.

## A change batch is validated whole

`API_ChangeResourceRecordSets` calls a batch transactional: *"Route 53 validates the changes in the
request and then either makes all or none of the changes in the change batch request."* Substrate
checks every change, in order, against the zone's record sets as the earlier changes in the batch
leave them ([#1586](https://github.com/scttfrdmn/substrate/issues/1586)):

| Change | Refused when | Message |
|--------|--------------|---------|
| `CREATE` | the record set exists | `Tried to create resource record set [name='www.example.com.', type='A'] but it already exists` |
| `DELETE` | the record set does not exist, or an earlier change in the batch deleted it | `Tried to delete resource record set [name='www.example.com.', type='A'] but it was not found` |
| `DELETE` | its values differ from the stored ones | `Tried to delete resource record set [name='www.example.com.', type='A'] but the values provided do not match the current values` |

`UPSERT` is never refused here. Every failure in the batch is reported, joined with `, `, with
`InvalidChangeBatch`/400, and nothing is written. A record set with a `SetIdentifier` adds
`, set-identifier='…'` inside the brackets. The messages are Route 53's: AWS's Knowledge Center
article *"Troubleshoot errors when creating Route 53 resource record sets with the AWS CLI"* quotes
the first and third, hashicorp/terraform-provider-aws#37806 the second, and Stack Overflow question
56960423 the `set-identifier` form.

A DELETE must match the record set's TTL, its values in any order, its alias target (the DNS name
compared without case or trailing dot), and its `Weight`, `Failover`, `Region` and `HealthCheckId`:
*"To delete a resource record set, you must specify all the same values that you specified when you
created it."* A `TTL` or `Weight` that is not a whole number is `InvalidInput`/400, with substrate's
own message.

A record set's identity is its name, type and `SetIdentifier`, so the weighted, latency and failover
record sets of one name and type are separate record sets. One with no `SetIdentifier` keeps the key
it always had. A weighted record set stored by an earlier substrate was keyed by name and type alone
and kept none of its routing members. A change naming a `SetIdentifier` still finds it: a `DELETE`
removes it, compared on its TTL, values and alias target; a `CREATE` is refused as *"already
exists"*; an `UPSERT` carrying its values moves it under the identifier's own key, so no second
record set appears, and one carrying other values is refused like the `CREATE`, since the stored
record set may be a simple one a weighted record set cannot sit beside. An `Action` other than
`CREATE`, `DELETE` or `UPSERT` is `InvalidInput`/400 (substrate's own message).

The per-change check runs before the [apex check](#the-default-soa-and-ns-record-sets), so a `CREATE`
of the zone's existing SOA or NS is answered *"Tried to create resource record set … but it already
exists"* like any other.

## Record set names

The Developer Guide's *"DNS domain name format"*: *"Amazon Route 53 stores alphabetic characters as
lowercase letters (a-z), regardless of how you specify them"*, characters are written as *"escape
codes in the format \three-digit octal code"*, and *"If the domain name includes any characters other
than a to z, 0 to 9, - (hyphen), or _ (underscore), Route 53 API actions return the characters as
escape codes. This is true whether you specify the characters as characters or as escape codes"*.
So `*.example.com`, `\052.example.com` and `\052.EXAMPLE.com.` name one record set, and
`ListResourceRecordSets` and the `InvalidChangeBatch` messages report it as `\052.example.com.`. That
is also the form Terraform's AWS provider normalizes a name to before it compares it with a listing.
A period inside a label stays `\056`. Hosted zone names, `NextDNSName` and alias targets' `DNSName`
are reported the same way. Route 53 is observed to lower-case an alias target: a load balancer whose
DNS name has capitals is read back lower-cased (hashicorp/terraform-provider-aws#306).

## Listing hosted zones by name

`API_ListHostedZonesByName` *"sorts hosted zones by name with the labels reversed. For example:
`com.example.www.`"*. Substrate compares names lower-cased, then breaks a tie by zone ID. `dnsname`
starts the page at the first zone that sorts at or after it; with `hostedzoneid` it starts at that
zone. The response echoes `DNSName`, `HostedZoneId` and `MaxItems`, and carries `NextDNSName` and
`NextHostedZoneId` only when `IsTruncated` is true. `maxitems` defaults to 100 and is capped there.
A `dnsname` with an empty label, a label over 63 bytes or more than 255 bytes in all (*"Each label
can be up to 63 bytes long. The total length of a domain name cannot exceed 255 bytes, including the
dots"*, an octal escape counting as one byte) is `InvalidDomainName`/400, and a
`hostedzoneid` without a `dnsname` is `InvalidInput`/400. Both messages are substrate's own.

## Tags

Tags live on the hosted zone or health check record, so they go when it is deleted. `AddTags` adds
or replaces, and `RemoveTagKeys` removes, an absent key included. `API_ChangeTagsForResource`: *"You
can add a maximum of 10 tags to a health check, a health check source, or a hosted zone"*, so a change
that would leave more is `InvalidInput`/400 with substrate's own message. Substrate models no health
check sources. The Resource Groups Tagging API does not report Route 53 resources.

## Health checks

`CreateHealthCheck` follows the page's `CallerReference` rules. The same reference and the same
settings return the existing health check; the settings compared are the ones the create was sent
with, so a retry still matches after an `UpdateHealthCheck`. A reference whose health check is not
there creates one. Different settings, or the reference of a deleted health
check *"regardless of the settings"*, are `HealthCheckAlreadyExists`/409. The page says Route 53
forgets a deleted check's reference *"after a number of days"*. Substrate never forgets it, because a
duration would tie the answer to the clock. The account limit is the Developer Guide's 200 active
health checks.

`HealthCheckVersion` starts at 1 and goes up by one with every `UpdateHealthCheck`. An update that
names a different version is `HealthCheckVersionMismatch`/409; one that names none is applied.
`ResetElements` sets `FullyQualifiedDomainName`, `ResourcePath` and `ChildHealthChecks` to null and
`Regions` to every Region. The endpoint types (`HTTP`, `HTTPS`, `HTTP_STR_MATCH`, `HTTPS_STR_MATCH`,
`TCP`) get `HealthCheckConfig`'s published defaults, `RequestInterval` 30 and `FailureThreshold` 3.
The error messages are the sentences each page gives the code in its Errors section, not captured
Route 53 messages, so they are substrate's own wording; the `ListHealthChecks` `marker` and `maxitems`
messages are substrate's own too.

### Health check status is seeded

Substrate never sends a request to an endpoint, so `GetHealthCheckStatus` reports a seed. It answers
one observation per Region in the check's `Regions`, or the eight default checker Regions, with no
`IPAddress`, checked at the simulated clock's now. Unseeded, every check reports `Success: HTTP Status
Code: 200. OK`, the page's example without its `Resolved IP` clause.

```bash
# Every health check reports unhealthy.
curl -X POST http://localhost:4566/v1/route53/health-check-status -d '{"healthCheckId":"*","healthy":false}'
# One health check, with exact Status text.
curl -X POST http://localhost:4566/v1/route53/health-check-status \
  -d '{"healthCheckId":"abcdef11-2222-3333-4444-555555fedcba","statusReport":"Failure: Connection timed out."}'
curl -X DELETE 'http://localhost:4566/v1/route53/health-check-status?healthCheckId=abcdef11-2222-3333-4444-555555fedcba'
curl -X DELETE http://localhost:4566/v1/route53/health-check-status
```

A check's own seed wins over the wildcard. The unhealthy text without a `statusReport` is
substrate's own. A `CALCULATED` check is `InvalidInput`/400, since the page says *"You can't use
`GetHealthCheckStatus` to get the status of a calculated health check"*.

## A record set needs its hosted zone

`ChangeResourceRecordSets` and `ListResourceRecordSets` refuse a zone ID that names no hosted zone,
including a deleted one, with `NoSuchHostedZone` at HTTP 404. Both pages publish that code for *"No
hosted zone exists with the ID that you specified"*. The change batch is refused before any of it is
written, so nothing is stored ([#1410](https://github.com/scttfrdmn/substrate/issues/1410)). A batch for a
missing zone used to be written and answered with a `ChangeInfo`, so `ListResourceRecordSets` found
records under a zone that `GetHostedZone` said did not exist. A CloudFormation `AWS::Route53::RecordSet`
naming a missing zone now fails its resource, where it used to report success over an orphan record set.

## Deleting a hosted zone

`DeleteHostedZone` used to answer 200 with a `ChangeInfo` for any ID, and to delete a zone whatever it
held ([#1416](https://github.com/scttfrdmn/substrate/issues/1416)). `API_DeleteHostedZone` publishes
`NoSuchHostedZone` (404) for *"No hosted zone exists with the ID that you specified"* and
`HostedZoneNotEmpty` (400) for *"The hosted zone contains resource records that are not SOA or NS
records"*: *"You can delete a hosted zone only if it contains only the default SOA and NS records."*

Every record set a zone holds besides its [defaults](#the-default-soa-and-ns-record-sets) was written
by `ChangeResourceRecordSets`, and each one blocks the delete except an SOA or NS at the zone's apex. A
zone has one record set per name and type, so a write there modifies the default record rather than
adding one. A delete that goes ahead removes the default record sets, and the zone's record-set index,
with the zone. The `HostedZoneNotEmpty` message is substrate's own; the page publishes none.

The two listings, and the delete's own check, skip an index entry whose record is gone, since the index
outlived it. A record that is there but cannot be read or decoded is an error, answered as a 500, where
it used to be left out of the listing silently
([#1417](https://github.com/scttfrdmn/substrate/issues/1417)).

A CloudFormation stack delete still converges: an `AWS::Route53::RecordSetGroup`'s delete removes its
record sets with a `ChangeResourceRecordSets` `DELETE` batch per zone, where the sweep used to skip the
group, so the zone it lives in can then be deleted.

## The default SOA and NS record sets

`API_CreateHostedZone`: *"For public hosted zones, Route 53 automatically creates a default SOA record
and four NS records for the zone."* Both of the page's example responses report
`<ResourceRecordSetCount>2</ResourceRecordSetCount>`, the private-zone one included, so substrate stores
the two record sets for a private zone too. Until
[#1537](https://github.com/scttfrdmn/substrate/issues/1537) a new zone held no record sets and reported
a count of 0.

| Record set | Name | TTL | Value |
|------------|------|-----|-------|
| NS | the zone's name | 172800 | the four name servers `GetHostedZone`'s `DelegationSet` reports |
| SOA | the zone's name | 900 | `ns-1.awsdns-01.com. awsdns-hostmaster.amazon.com. 1 7200 900 1209600 86400` |

The SOA's timers are the Developer Guide's example (*"NS and SOA records that Amazon Route 53 creates
for a public hosted zone"*: serial 1, refresh 7200, retry 900, expire 1209600, minimum TTL 86400), and
its TTL is that page's default of 900 seconds. The same page says only that the administrator's
address is *"an amazon.com email address that is not monitored"*; `awsdns-hostmaster.amazon.com.` is the
address real zones report. The NS TTL is the value *"Making Route 53 the DNS service for a domain
that's in use"* calls *"the typical TTL setting for the NS record"*.

`ResourceRecordSetCount` follows every change batch: it is the size of the zone's record-set index
after the batch, and `GetHostedZone`, `ListHostedZones` and `CreateHostedZone` report it.

A change batch may `UPSERT` either default, and a name with or without its trailing dot is the same
record set: `API_CreateHostedZone` says Route 53 treats the two spellings as identical. The page calls
a batch transactional, so the check reads the batch's **end state**: a zone holding its defaults must
end the batch with exactly one apex SOA and an apex NS. A batch that deletes the SOA and creates its
replacement is accepted. One that would leave no SOA or no NS is refused (a `CREATE` of the
existing SOA or NS is refused [before](#a-change-batch-is-validated-whole) this check) with `InvalidChangeBatch`/400, the code `API_ChangeResourceRecordSets`
publishes for a rejected batch, and makes none of its changes. The Developer Guide's *"Deleting
records"* says *"You can't delete the records that have a value of NS or SOA for Type"*. No page
publishes the messages; substrate answers the two Route 53 itself answers, as Terraform's AWS provider
reports quote them (hashicorp/terraform-provider-aws #37863 and #45241): *"A HostedZone must contain
exactly one SOA record."* and *"A HostedZone must contain at least one NS record for the zone
itself."*, joined with a comma when both apply.

`Type` must be one of the values `API_ResourceRecordSet` publishes, which are upper case; anything
else, a lower-case `a` included, is `InvalidInput`/400 ("The input is not valid.") before any change is
made. The message is substrate's own.

A CloudFormation `AWS::Route53::RecordSet` or `RecordSetGroup` element that writes the zone's apex SOA
or NS modifies the default, so its delete restores the default with an `UPSERT` rather than sending a
`DELETE` the zone would refuse, and the stack tears down. A zone that cannot be read fails that
resource's delete.

**State written before #1537.** A record set used to be keyed by its `Type` and `Name` exactly as the
request spelled them. Such a record set is still found: a `DELETE` removes it, and a `CREATE` or
`UPSERT` moves it to its canonical key (upper-case type, lower-case name without its trailing dot)
rather than writing a second one. A zone stored before #1537 holds no defaults and is not given them:
it is treated as having none, so no batch is refused for its apex SOA or NS, a CloudFormation delete of
its apex record set sends a plain `DELETE`, and its count is what it holds. Backfilling them would
rewrite a stored zone as a side effect of an unrelated request.

## The account a hosted zone carries reaches no response

`Route53HostedZone` declares its account under a wire-visible `json` tag, because the record is what
`MemoryStateManager` snapshots and a replay reads back. It does not reach a body: Route 53 speaks REST-XML, and every response is marshaled from an XML struct declared for its operation.
`TestRoute53Wire_HostedZoneResponsesCarryNoBookkeepingMember` in `emulator/route53_wire_test.go` drives the hosted zone and record set operations and walks each response's element names for the member
([#756](https://github.com/scttfrdmn/substrate/issues/756)). `Route53HealthCheck` carries its account the same way, and
`TestRoute53Wire_HealthCheckResponsesCarryNoBookkeepingMember` drives every operation that answers a health check.

## Request validation

Route 53 is validated against its service model before any plugin sees a client's request
(#1612; see [Request validation](/reference/concepts/validation)). Every failure answers
`InvalidInput`/400, which every routed operation that takes input publishes, in Route 53's form with the value
quoted: `1 validation error detected: Value '3600000000000' at
'changeBatch.changes.1.member.resourceRecordSet.tTL' failed to satisfy constraint: Member must
have value less than or equal to 2147483647`. The path is AWS's own, `tTL` included. Enum checks
are on, so a record `Type` or a `ListResourceRecordSets` `type` outside the published values is
refused. Route 53 lists an enum's values in an order of its own; substrate lists them in the
model's.

A hosted zone `Id` longer than 32 characters, a `CallerReference` outside 1–128 and a missing
`Name` or `CallerReference` are among the refusals. A request that reaches the plugin without the
gate — a schedule's dispatch, or any request with validation off — is checked by the plugin through
the gate's own walk: `CreateHostedZone`'s `Name`, `CreateHealthCheck`'s `CallerReference` and
`HealthCheckConfig`, `ListTagsForResources`' `ResourceIds`, and `ChangeResourceRecordSets`' whole
`ChangeBatch`, so a bad `Type` and a bad `TTL` together are reported together either way. An
empty `<Name></Name>` is refused with `Member must not be blank`, substrate's own wording, since
the model sets `Name` no minimum length. The `Value null at` form for a missing member is
substrate's too; no source shows Route 53's.

## Cost

Route 53 hosted zone: $0.50/month per zone (tracked as flat cost on CreateHostedZone).
