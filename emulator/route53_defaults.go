package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// The default record sets Route 53 creates with every hosted zone (#1537).
//
// API_CreateHostedZone: "For public hosted zones, Route 53 automatically creates a default SOA record
// and four NS records for the zone", and both of its example responses, the private-zone one
// included, report <ResourceRecordSetCount>2</ResourceRecordSetCount> — so a private zone gets the
// two record sets too. The Developer Guide's "NS and SOA records that Amazon Route 53 creates for a
// public hosted zone" publishes their shape:
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
	ns := Route53ResourceRecordSet{Name: zoneName, Type: "NS", TTL: r53DefaultNSTTL, ZoneID: zoneID}
	for _, s := range servers {
		ns.ResourceRecords = append(ns.ResourceRecords, Route53ResourceRecord{Value: s})
	}
	soa := Route53ResourceRecordSet{
		Name: zoneName, Type: "SOA", TTL: r53DefaultSOATTL, ZoneID: zoneID,
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

// r53DefaultRecordDeleteError refuses a change batch that deletes a zone's default SOA or NS record
// set. API_ChangeResourceRecordSets publishes InvalidChangeBatch (400) for a change batch it rejects;
// the Developer Guide's "Deleting records" says "You can't delete the records that have a value of NS
// or SOA for Type", and API_DeleteHostedZone says deleting the zone is how they go ("You can delete a
// hosted zone only if it contains only the default SOA and NS records"). The message is substrate's
// own; no page publishes the wording.
func r53DefaultRecordDeleteError(rrType, zoneName string) *AWSError {
	return &AWSError{
		Code: "InvalidChangeBatch",
		Message: fmt.Sprintf("The change batch deletes the %s record set at %s, which is one of the hosted zone's "+
			"default record sets; it is removed only with the hosted zone.", strings.ToUpper(rrType), zoneName),
		HTTPStatus: http.StatusBadRequest,
	}
}

// putRecordSet stores one record set in zone id and indexes it.
func (p *Route53Plugin) putRecordSet(id string, rrset Route53ResourceRecordSet) error {
	key := r53RRSetKey(rrset.Type, rrset.Name)
	data, err := json.Marshal(rrset)
	if err != nil {
		return fmt.Errorf("route53 marshal record set %s: %w", key, err)
	}
	if err := p.state.Put(context.Background(), route53Namespace, "rrset:"+id+":"+key, data); err != nil {
		return fmt.Errorf("route53 put record set %s: %w", key, err)
	}
	return p.appendToList(id, "rrset_keys", key)
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

// storeRecordSetCount recounts zone id's record sets and stores the count on zone, so
// ResourceRecordSetCount follows every change batch. It counts records rather than index entries:
// an entry that outlived its record is skipped, as the listing skips it.
func (p *Route53Plugin) storeRecordSetCount(id string, zone Route53HostedZone) error {
	keys, err := p.loadList(id, "rrset_keys")
	if err != nil {
		return fmt.Errorf("route53 count record sets: %w", err)
	}
	var n int64
	for _, k := range keys {
		_, found, err := p.loadRecordSet(id, k)
		if err != nil {
			return fmt.Errorf("route53 count record sets: %w", err)
		}
		if found {
			n++
		}
	}
	zone.ResourceRecordSetCount = n
	data, err := json.Marshal(zone)
	if err != nil {
		return fmt.Errorf("route53 marshal hosted zone %s: %w", id, err)
	}
	if err := p.state.Put(context.Background(), route53Namespace, "hostedzone:"+id, data); err != nil {
		return fmt.Errorf("route53 store record-set count %s: %w", id, err)
	}
	return nil
}
