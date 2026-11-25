package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
)

// The default record sets Route 53 creates with every hosted zone (#1537).
//
// API_CreateHostedZone: "For public hosted zones, Route 53 automatically creates a default SOA record
// and four NS records for the zone", and both of its example responses, the private-zone one
// included, report a ResourceRecordSetCount of 2 — so a private zone gets the two record sets too.
// The Developer Guide's "NS and SOA records that Amazon Route 53 creates for a public hosted zone"
// publishes their shape:
//
//   - the NS record set "has the same name as your hosted zone" and "lists the four name servers that
//     are the authoritative name servers for your hosted zone" — the four GetHostedZone's
//     DelegationSet reports;
//   - the SOA record holds the creating name server, the administrator's address ("an amazon.com
//     email address that is not monitored"), a serial of 1, and the refresh 7200, retry 900, expire
//     1209600 and minimum TTL 86400 of the page's example, and the SOA record set's own TTL "default
//     value is 900 seconds".
//
// The NS record set's TTL is 172800, the value "Making Route 53 the DNS service for a domain that's in
// use" calls "the typical TTL setting for the NS record". The administrator's address is
// awsdns-hostmaster.amazon.com., the amazon.com address real zones report; the page names only its
// domain.
//
// A zone created before #1537 holds no defaults and its record carries no DefaultRecordSets flag.
// Substrate does not backfill them: such a zone is treated as having none, so no change batch is
// refused for its apex SOA or NS, its CloudFormation deletes send a plain DELETE, and its count is what
// its index holds. Backfilling would rewrite a stored zone as a side effect of an unrelated request.
const (
	r53DefaultSOATTL     = 900
	r53DefaultNSTTL      = 172800
	r53DefaultSOAContact = "awsdns-hostmaster.amazon.com."
	r53DefaultSOATimers  = "1 7200 900 1209600 86400"
)

// r53DefaultNameServers returns the four name servers of every zone's delegation set and default NS
// record set. A fresh slice each call, so no caller can change another's.
func r53DefaultNameServers() []string {
	return []string{"ns-1.awsdns-01.com.", "ns-2.awsdns-02.net.", "ns-3.awsdns-03.org.", "ns-4.awsdns-04.co.uk."}
}

// r53DefaultRecordSets returns the apex NS and SOA record sets CreateHostedZone stores for a zone
// named zoneName, NS first, the order ListResourceRecordSets lists two record sets of one name in.
func r53DefaultRecordSets(zoneID, zoneName string) []Route53ResourceRecordSet {
	servers := r53DefaultNameServers()
	ns := Route53ResourceRecordSet{Name: zoneName, Type: "NS", TTL: r53DefaultNSTTL, ZoneID: zoneID, RoutingRecorded: true}
	for _, s := range servers {
		ns.ResourceRecords = append(ns.ResourceRecords, Route53ResourceRecord{Value: s})
	}
	soa := Route53ResourceRecordSet{
		Name: zoneName, Type: "SOA", TTL: r53DefaultSOATTL, ZoneID: zoneID, RoutingRecorded: true,
		ResourceRecords: []Route53ResourceRecord{{
			Value: servers[0] + " " + r53DefaultSOAContact + " " + r53DefaultSOATimers,
		}},
	}
	return []Route53ResourceRecordSet{ns, soa}
}

// r53IsApexDefault reports whether a record set of rrType named name is one of the default record
// sets of the zone named zoneName: its SOA or NS at the apex.
func r53IsApexDefault(rrType, name, zoneName string) bool {
	t := strings.ToUpper(rrType)
	return (t == "SOA" || t == "NS") && r53CanonicalName(name) == r53CanonicalName(zoneName)
}

// r53RRTypes is the Type enum API_ResourceRecordSet publishes ("Valid Values: SOA | A | TXT | NS |
// CNAME | MX | NAPTR | PTR | SRV | SPF | AAAA | CAA | DS | TLSA | SSHFP | SVCB | HTTPS"), read from the
// vendored model so that it is the list request validation checks. The values are upper case, and
// an enum member is matched exactly, so "a" is not "A".
func r53RRTypes() []string {
	return wireEnumAt("route53", "ChangeResourceRecordSets", "ChangeBatch", "Changes", "ResourceRecordSet", "Type")
}

// r53InvalidType refuses the record set Type of the change at index i (counting from 0) when it is
// outside [r53RRTypes], with InvalidInput (400), "The input is not valid.", which
// API_ChangeResourceRecordSets publishes. The handler checks the whole ChangeBatch through the
// gate's own walk first ([wireCheckMember]), which refuses a bad Type in the gate's words with
// every other failure in the batch; this answers only a Type that walk did not see, for a body it
// could not read, and its message is substrate's own.
func r53InvalidType(i int, rrType string) *AWSError {
	return &AWSError{
		Code: "InvalidInput",
		Message: fmt.Sprintf("change %d: %q is not a valid resource record set Type; the valid values are %s.",
			i+1, rrType, strings.Join(r53RRTypes(), ", ")),
		HTTPStatus: http.StatusBadRequest,
	}
}

// The InvalidChangeBatch messages for a batch that would leave a zone without its apex SOA or NS.
// No API page publishes them; they are what Route 53 answers, as Terraform's AWS provider reports
// quote it (hashicorp/terraform-provider-aws#37863 and #45241): "InvalidChangeBatch: [A HostedZone
// must contain at least one NS record for the zone itself., A HostedZone must contain exactly one SOA
// record.]".
const (
	r53MsgExactlyOneSOA = "A HostedZone must contain exactly one SOA record."
	r53MsgAtLeastOneNS  = "A HostedZone must contain at least one NS record for the zone itself."
)

// r53ApexChange is the part of one change the apex check reads.
type r53ApexChange struct {
	action, rrType, name string
}

// r53CheckApexDefaults refuses a change batch that would leave a zone holding its defaults without
// exactly one apex SOA, or without an apex NS (#1537). It evaluates the batch's end state rather than
// each change, so a batch that deletes the SOA and creates its replacement is accepted. index is the
// zone's record-set index before the batch. It runs after [planChangeBatch] has accepted every change,
// so a CREATE here names an apex record set the batch deleted first; an UPSERT replaces; a DELETE
// removes. The batch is transactional, so a refusal makes no change.
func r53CheckApexDefaults(index []string, changes []r53ApexChange, zoneName string) *AWSError {
	count := map[string]int{}
	for _, t := range []string{"SOA", "NS"} {
		if len(r53MatchingKeys(index, r53RRSetKey(t, zoneName))) > 0 {
			count[t] = 1
		}
	}
	for _, c := range changes {
		if !r53IsApexDefault(c.rrType, c.name, zoneName) {
			continue
		}
		t := strings.ToUpper(c.rrType)
		switch strings.ToUpper(c.action) {
		case "CREATE":
			count[t]++
		case "UPSERT":
			count[t] = max(count[t], 1)
		case "DELETE":
			count[t] = max(count[t]-1, 0)
		}
	}
	var msgs []string
	if count["NS"] == 0 {
		msgs = append(msgs, r53MsgAtLeastOneNS)
	}
	if count["SOA"] != 1 {
		msgs = append(msgs, r53MsgExactlyOneSOA)
	}
	if len(msgs) == 0 {
		return nil
	}
	return &AWSError{Code: "InvalidChangeBatch", Message: strings.Join(msgs, ", "), HTTPStatus: http.StatusBadRequest}
}

// r53MatchingKeys returns the index entries that name the record set key names, in index order.
//
// Before #1537 a record set's key was its Type and Name exactly as the request spelled them, so
// "NS:Example.com." and "ns:example.com" were two keys for one record set. An index entry is matched
// by canonicalizing it the way [r53RRSetIdentityKey] does ([r53EntryIdentity]), so a record set written under its old key is still
// found: a DELETE removes it, and a CREATE or UPSERT replaces it under the canonical key rather than
// writing a second. This is the migrate-on-access the CloudFormation stack keys use (cfn_scope.go).
func r53MatchingKeys(index []string, key string) []string {
	var out []string
	for _, k := range index {
		if r53EntryIdentity(k) == key {
			out = append(out, k)
		}
	}
	return out
}

// putRecordSet stores one record set in zone id under its canonical key ([r53RRSetIdentityKey]),
// which it returns. The caller owns the index.
func (p *Route53Plugin) putRecordSet(id string, rrset Route53ResourceRecordSet) (string, error) {
	key := r53RRSetIdentityKey(rrset.Type, rrset.Name, rrset.SetIdentifier)
	data, err := json.Marshal(rrset)
	if err != nil {
		return "", fmt.Errorf("route53 marshal record set %s: %w", key, err)
	}
	if err := p.state.Put(context.Background(), route53Namespace, "rrset:"+id+":"+key, data); err != nil {
		return "", fmt.Errorf("route53 put record set %s: %w", key, err)
	}
	return key, nil
}

// deleteRecordSetKeys removes the records stored under keys in zone id and drops them from index,
// returning the index without them.
func (p *Route53Plugin) deleteRecordSetKeys(id string, index, keys []string) ([]string, error) {
	for _, k := range keys {
		if err := p.state.Delete(context.Background(), route53Namespace, "rrset:"+id+":"+k); err != nil {
			return nil, fmt.Errorf("route53 delete record set %s: %w", k, err)
		}
		index = slices.DeleteFunc(index, func(e string) bool { return e == k })
	}
	return index, nil
}

// saveList stores a whole index list.
func (p *Route53Plugin) saveList(scope, listName string, ids []string) error {
	key := listName + ":" + scope
	data, err := json.Marshal(ids)
	if err != nil {
		return fmt.Errorf("route53 saveList %s marshal: %w", key, err)
	}
	if err := p.state.Put(context.Background(), route53Namespace, key, data); err != nil {
		return fmt.Errorf("route53 saveList %s state.Put: %w", key, err)
	}
	return nil
}

// deleteZoneRecordSets removes every record set zone id still holds, and its record-set index. It runs
// after [Route53Plugin.nonDefaultRecordSet] has found nothing but the defaults, so it removes those.
func (p *Route53Plugin) deleteZoneRecordSets(id string) error {
	keys, err := p.loadList(id, "rrset_keys")
	if err != nil {
		return fmt.Errorf("route53 deleteHostedZone record sets: %w", err)
	}
	for _, k := range keys {
		if err := p.state.Delete(context.Background(), route53Namespace, "rrset:"+id+":"+k); err != nil {
			return fmt.Errorf("route53 deleteHostedZone record set %s: %w", k, err)
		}
	}
	if err := p.state.Delete(context.Background(), route53Namespace, "rrset_keys:"+id); err != nil {
		return fmt.Errorf("route53 deleteHostedZone record-set index: %w", err)
	}
	return nil
}

// r53LoadZone reads the hosted zone a zone ID (bare or in the /hostedzone/ path form) names. found is
// false for an absent zone; a read or decode failure is an error. It is the one loader the plugin and
// the CloudFormation deployer share.
func r53LoadZone(ctx context.Context, state StateManager, zoneID string) (Route53HostedZone, bool, error) {
	data, err := state.Get(ctx, route53Namespace, "hostedzone:"+r53ZoneSuffix(zoneID))
	if err != nil {
		return Route53HostedZone{}, false, fmt.Errorf("route53 get hosted zone %s: %w", zoneID, err)
	}
	if data == nil {
		return Route53HostedZone{}, false, nil
	}
	var zone Route53HostedZone
	if err := json.Unmarshal(data, &zone); err != nil {
		return Route53HostedZone{}, false, fmt.Errorf("route53 decode hosted zone %s: %w", zoneID, err)
	}
	return zone, true, nil
}

// putZone stores a hosted zone record.
func (p *Route53Plugin) putZone(id string, zone Route53HostedZone) error {
	data, err := json.Marshal(zone)
	if err != nil {
		return fmt.Errorf("route53 marshal hosted zone %s: %w", id, err)
	}
	if err := p.state.Put(context.Background(), route53Namespace, "hostedzone:"+id, data); err != nil {
		return fmt.Errorf("route53 put hosted zone %s: %w", id, err)
	}
	return nil
}
