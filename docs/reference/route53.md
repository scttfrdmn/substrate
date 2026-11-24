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
| ChangeResourceRecordSets | CREATE/DELETE/UPSERT actions; [`NoSuchHostedZone`](#a-record-set-needs-its-hosted-zone) for an absent zone. A batch that would leave the zone without exactly one apex SOA, or without an apex NS, is [`InvalidChangeBatch`/400](#the-default-soa-and-ns-record-sets) and makes no change. A name with and without its trailing dot is one record set; a `Type` outside the published upper-case values is `InvalidInput`/400 |
| ListResourceRecordSets | [`NoSuchHostedZone`](#a-record-set-needs-its-hosted-zone) for an absent zone. A record set that cannot be read or decoded is an error, not a record set left out |

## CloudFormation resource types

| Type | Ref | Notes |
|------|-----|-------|
| AWS::Route53::HostedZone | HostedZoneId | |
| AWS::Route53::RecordSet | — | Fails its resource when `HostedZoneId` names no hosted zone |

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
replacement is accepted. One that would leave no SOA, a second SOA (a `CREATE` while one exists), no NS
or a second NS is refused with `InvalidChangeBatch`/400, the code `API_ChangeResourceRecordSets`
publishes for a rejected batch, and makes none of its changes. The Developer Guide's *"Deleting
records"* says *"You can't delete the records that have a value of NS or SOA for Type"*. No page
publishes the messages; substrate answers the two Route 53 itself answers, as Terraform's AWS provider
reports quote them (hashicorp/terraform-provider-aws #37863 and #45241): *"A HostedZone must contain
exactly one SOA record."* and *"A HostedZone must contain at least one NS record for the zone
itself."*, joined with a comma when both apply. The message for a second apex NS record set is
substrate's own.

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
`TestRoute53Wire_HostedZoneResponsesCarryNoBookkeepingMember` in `emulator/route53_wire_test.go` drives all six routed operations and walks each response's element names for the member
([#756](https://github.com/scttfrdmn/substrate/issues/756)).

## Request validation

Route 53 is validated against its service model before any plugin sees a client's request
(#1612; see [Request validation](/reference/concepts/validation)). Every failure answers
`InvalidInput`/400, which every routed operation publishes, in Route 53's form with the value
quoted: `1 validation error detected: Value '3600000000000' at
'changeBatch.changes.1.member.resourceRecordSet.tTL' failed to satisfy constraint: Member must
have value less than or equal to 2147483647`. The path is AWS's own, `tTL` included. Enum checks
are on, so a record `Type` or a `ListResourceRecordSets` `type` outside the published values is
refused. Route 53 lists an enum's values in an order of its own; substrate lists them in the
model's.

A hosted zone `Id` longer than 32 characters, a `CallerReference` outside 1–128 and a missing
`Name` or `CallerReference` are among the refusals. `CreateHostedZone`'s own `Name` check and
`ChangeResourceRecordSets`' own `Type` check answer in the same words for a request that reaches
the plugin without the gate.

## Cost

Route 53 hosted zone: $0.50/month per zone (tracked as flat cost on CreateHostedZone).
