package emulator

import (
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
)

// A change batch's own refusals (#1586).
//
// API_ChangeResourceRecordSets calls a batch transactional: "Route 53 validates the changes in the
// request and then either makes all or none of the changes in the change batch request". The page
// publishes InvalidChangeBatch (400), "This exception contains a list of messages that might contain
// one or more error messages. Each error message indicates one error in the change batch", and
// three rules the check below applies to every change in order, against an in-memory copy of the
// zone's record sets that each earlier change in the batch has already updated:
//
//   - "CREATE: Creates a resource record set that has the specified values": a CREATE of a record
//     set the zone already holds is refused;
//   - "DELETE: Deletes an existing resource record set that has the specified values" and "To delete
//     a resource record set, you must specify all the same values that you specified when you
//     created it": a DELETE of a record set the zone does not hold, or whose values differ, is
//     refused;
//   - "If you try to delete the same resource record set more than once in a single change batch,
//     Route 53 returns an InvalidChangeBatch error": the second DELETE finds the record set the
//     first one removed gone, and is refused as not found.
//
// UPSERT is never refused here: "If a resource set doesn't exist, Route 53 creates it. If a resource
// set exists Route 53 updates it with the values in the request."
//
// A record set's identity is its name, type and SetIdentifier ([r53RRSetIdentityKey]). The values a
// DELETE must match are its TTL, its resource records in any order, its alias target, and its Weight,
// Failover, Region and HealthCheckId.
//
// Every failure in the batch is reported, joined with ", " as [r53CheckApexDefaults] joins its own,
// and nothing is written. The messages are Route 53's, in the bracketed form every published capture
// uses:
//
//   - "Tried to create resource record set [name='b.domain.com.', type='CNAME'] but it already
//     exists" and "Tried to delete resource record set [name='a.domain.com.', type='A'] but the
//     values provided do not match the current values", both quoted by AWS's Knowledge Center
//     article "Troubleshoot errors when creating Route 53 resource record sets with the AWS CLI";
//   - "Tried to delete resource record set [name='xxxxx.', type='CNAME'] but it was not found",
//     quoted without a trailing period by hashicorp/terraform-provider-aws#37806;
//   - the ", set-identifier='local'" member a record set with a SetIdentifier adds inside the
//     brackets, quoted by Stack Overflow question 56960423 ("How to update (Not Create) the existing
//     Route53 Recordset with CloudFormation?").
//
// The name is reported in the form Route 53 reports every name ([r53WireName]): lower-cased, with
// octal escapes, and with the trailing dot every capture shows.

// r53BatchChange is one parsed change of a batch.
type r53BatchChange struct {
	action string
	rrset  Route53ResourceRecordSet
}

// r53StoredSet is one record set a zone holds before the batch: the record and every index entry
// that names it, which is more than one only for a record set stored under a non-canonical key.
type r53StoredSet struct {
	rrset Route53ResourceRecordSet
	keys  []string
}

// r53BatchPlan is the outcome of a validated batch: the record sets to write, in the order the batch
// first wrote them, and the index entries to remove.
type r53BatchPlan struct {
	writes  []Route53ResourceRecordSet
	removes []string
}

// r53FQDN returns name with its trailing dot. API_ResourceRecordSet: Route 53 "treats www.example.com
// (without a trailing dot) and www.example.com. (with a trailing dot) as identical", and every name a
// response reports, ListResourceRecordSets' included, is written with it.
func r53FQDN(name string) string {
	if strings.HasSuffix(name, ".") {
		return name
	}
	return name + "."
}

// r53RecordSetLabel renders a record set's bracketed label for an InvalidChangeBatch message.
func r53RecordSetLabel(rrset Route53ResourceRecordSet) string {
	label := fmt.Sprintf("[name='%s', type='%s'", r53WireName(rrset.Name), rrset.Type)
	if rrset.SetIdentifier != "" {
		label += fmt.Sprintf(", set-identifier='%s'", rrset.SetIdentifier)
	}
	return label + "]"
}

// planChangeBatch checks changes against the zone's record sets, held in stored by identity, and
// returns what applying them writes and removes, or the InvalidChangeBatch every failing change
// earns. stored is not modified.
func planChangeBatch(stored map[string]r53StoredSet, changes []r53BatchChange) (r53BatchPlan, *AWSError) {
	current := make(map[string]Route53ResourceRecordSet, len(stored))
	for id, s := range stored {
		current[id] = s.rrset
	}
	written := map[string]bool{}
	var order []string
	var msgs []string
	for _, c := range changes {
		id := r53RRSetIdentityKey(c.rrset.Type, c.rrset.Name, c.rrset.SetIdentifier)
		switch c.action {
		case "CREATE":
			_, exists := current[id]
			if !exists {
				_, _, exists = r53LegacyRoutingMatch(current, c.rrset)
			}
			if exists {
				msgs = append(msgs, "Tried to create resource record set "+r53RecordSetLabel(c.rrset)+" but it already exists")
				continue
			}
		case "UPSERT":
			// An UPSERT that names a SetIdentifier and meets a record set stored under its name and type
			// alone without routing members takes it over only when the values match, the stored
			// weighted record set restated with its identifier; it then moves to the identity's own
			// key. With other values the stored record set may be a simple one, which a weighted
			// record set of the same name and type cannot sit beside, so the change is refused as the
			// CREATE is.
			if _, exists := current[id]; !exists {
				if legacy, have, ok := r53LegacyRoutingMatch(current, c.rrset); ok {
					if !r53SameValues(have, c.rrset) {
						msgs = append(msgs, "Tried to create resource record set "+r53RecordSetLabel(c.rrset)+" but it already exists")
						continue
					}
					delete(current, legacy)
					delete(written, legacy)
				}
			}
		case "DELETE":
			have, exists := current[id]
			if !exists {
				id, have, exists = r53LegacyRoutingMatch(current, c.rrset)
			}
			switch {
			case !exists:
				msgs = append(msgs, "Tried to delete resource record set "+r53RecordSetLabel(c.rrset)+" but it was not found")
			case !r53SameValues(have, c.rrset):
				msgs = append(msgs, "Tried to delete resource record set "+r53RecordSetLabel(c.rrset)+
					" but the values provided do not match the current values")
			default:
				delete(current, id)
				delete(written, id)
			}
			continue
		default:
			// Request validation refuses an Action outside the enum; this answers a dispatch it did
			// not see. The message is substrate's own.
			return r53BatchPlan{}, &AWSError{Code: "InvalidInput",
				Message:    fmt.Sprintf("%q is not a valid Action; the valid values are CREATE, DELETE and UPSERT.", c.action),
				HTTPStatus: http.StatusBadRequest}
		}
		current[id] = c.rrset
		if !written[id] {
			written[id] = true
			order = append(order, id)
		}
	}
	if len(msgs) > 0 {
		return r53BatchPlan{}, &AWSError{Code: "InvalidChangeBatch", Message: strings.Join(msgs, ", "), HTTPStatus: http.StatusBadRequest}
	}

	var plan r53BatchPlan
	planned := map[string]bool{}
	for _, id := range order {
		if written[id] && !planned[id] {
			planned[id] = true
			plan.writes = append(plan.writes, current[id])
		}
	}
	for id, s := range stored {
		_, kept := current[id]
		for _, k := range s.keys {
			// A record set the batch rewrote keeps its canonical entry, which the write replaces; any
			// other entry naming it is a non-canonical key and goes. One the batch removed loses them all.
			if kept && (!written[id] || k == id) {
				continue
			}
			plan.removes = append(plan.removes, k)
		}
	}
	slices.Sort(plan.removes)
	return plan, nil
}

// r53LegacyRoutingMatch finds the record set a change with a SetIdentifier names when the stored
// record carries no RoutingRecorded flag: such a record holds no routing members and is keyed by its
// name and type alone, so a weighted or failover record set stored that way is one with a
// SetIdentifier the record does not hold. A DELETE removes it, so its zone can be emptied; a CREATE
// finds it existing; an UPSERT with its values moves it to the identity's own key, so it is not
// duplicated, and one with other values is refused. A record carrying the flag is never matched this
// way.
func r53LegacyRoutingMatch(current map[string]Route53ResourceRecordSet, want Route53ResourceRecordSet) (string, Route53ResourceRecordSet, bool) {
	if want.SetIdentifier == "" {
		return "", Route53ResourceRecordSet{}, false
	}
	id := r53RRSetKey(want.Type, want.Name)
	have, ok := current[id]
	if !ok || have.RoutingRecorded {
		return "", Route53ResourceRecordSet{}, false
	}
	return id, have, true
}

// r53SameValues reports whether a DELETE's record set carries the values of the stored one. A record
// set stored without its routing members ([Route53ResourceRecordSet.RoutingRecorded] false) is
// compared on what it does carry: its TTL, records and alias target.
func r53SameValues(have, want Route53ResourceRecordSet) bool {
	if have.TTL != want.TTL || !r53SameRecords(have.ResourceRecords, want.ResourceRecords) ||
		!r53SameAlias(have.AliasTarget, want.AliasTarget) {
		return false
	}
	if !have.RoutingRecorded {
		return true
	}
	return have.SetIdentifier == want.SetIdentifier && r53SameWeight(have.Weight, want.Weight) &&
		have.Failover == want.Failover && have.Region == want.Region && have.HealthCheckID == want.HealthCheckID
}

// r53SameRecords compares two record lists as multisets: a DELETE may list the values in any order.
func r53SameRecords(a, b []Route53ResourceRecord) bool {
	if len(a) != len(b) {
		return false
	}
	as := make([]string, len(a))
	bs := make([]string, len(b))
	for i := range a {
		as[i], bs[i] = a[i].Value, b[i].Value
	}
	slices.Sort(as)
	slices.Sort(bs)
	return slices.Equal(as, bs)
}

// r53SameAlias compares two alias targets, the DNS name as [r53CanonicalName] does.
func r53SameAlias(a, b *Route53AliasTarget) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return r53ZoneSuffix(a.HostedZoneID) == r53ZoneSuffix(b.HostedZoneID) &&
		r53CanonicalName(a.DNSName) == r53CanonicalName(b.DNSName) &&
		a.EvaluateTargetHealth == b.EvaluateTargetHealth
}

// r53SameWeight compares two optional weights.
func r53SameWeight(a, b *int64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// r53XMLRecordSet is a ResourceRecordSet as a change batch spells it.
type r53XMLRecordSet struct {
	Name            string `xml:"Name"`
	Type            string `xml:"Type"`
	SetIdentifier   string `xml:"SetIdentifier"`
	Weight          string `xml:"Weight"`
	Region          string `xml:"Region"`
	Failover        string `xml:"Failover"`
	HealthCheckID   string `xml:"HealthCheckId"`
	TTL             string `xml:"TTL"`
	ResourceRecords []struct {
		Value string `xml:"Value"`
	} `xml:"ResourceRecords>ResourceRecord"`
	AliasTarget *struct {
		HostedZoneID         string `xml:"HostedZoneId"`
		DNSName              string `xml:"DNSName"`
		EvaluateTargetHealth string `xml:"EvaluateTargetHealth"`
	} `xml:"AliasTarget"`
}

// recordSet converts the change at index i (counting from 0) into the record set it names in zone
// id. A TTL or Weight that is not a whole number is InvalidInput (400), the code
// API_ChangeResourceRecordSets publishes for "The input is not valid."; request validation does not
// type an XML value, so this is the one check that sees it. The message is substrate's own.
func (x r53XMLRecordSet) recordSet(i int, id string) (Route53ResourceRecordSet, *AWSError) {
	rrset := Route53ResourceRecordSet{
		Name: x.Name, Type: x.Type, ZoneID: id,
		SetIdentifier: x.SetIdentifier, Failover: x.Failover, Region: x.Region, HealthCheckID: x.HealthCheckID,
		RoutingRecorded: true,
	}
	if s := strings.TrimSpace(x.TTL); s != "" {
		ttl, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return rrset, r53NotANumber(i, "TTL", x.TTL)
		}
		rrset.TTL = ttl
	}
	if s := strings.TrimSpace(x.Weight); s != "" {
		w, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return rrset, r53NotANumber(i, "Weight", x.Weight)
		}
		rrset.Weight = &w
	}
	for _, rr := range x.ResourceRecords {
		rrset.ResourceRecords = append(rrset.ResourceRecords, Route53ResourceRecord{Value: rr.Value})
	}
	if x.AliasTarget != nil {
		rrset.AliasTarget = &Route53AliasTarget{
			HostedZoneID:         x.AliasTarget.HostedZoneID,
			DNSName:              x.AliasTarget.DNSName,
			EvaluateTargetHealth: strings.EqualFold(strings.TrimSpace(x.AliasTarget.EvaluateTargetHealth), "true"),
		}
	}
	return rrset, nil
}

// r53NotANumber refuses the member of the change at index i whose value is not a whole number.
func r53NotANumber(i int, member, value string) *AWSError {
	return &AWSError{
		Code:       "InvalidInput",
		Message:    fmt.Sprintf("change %d: %s %q is not a whole number.", i+1, member, value),
		HTTPStatus: http.StatusBadRequest,
	}
}

// loadStoredSets reads the record sets of zone id that changes name, keyed by identity: each
// change's own identity, and for one with a SetIdentifier the name-and-type identity
// [r53LegacyRoutingMatch] consults. Only those are read, so a batch costs its own size in reads, not
// the zone's. An index entry whose record is gone is skipped, as the listings skip it (#1417); a read
// or decode failure is an error.
func (p *Route53Plugin) loadStoredSets(id string, index []string, changes []r53BatchChange) (map[string]r53StoredSet, error) {
	wanted := make(map[string]bool, 2*len(changes))
	for _, c := range changes {
		wanted[r53RRSetIdentityKey(c.rrset.Type, c.rrset.Name, c.rrset.SetIdentifier)] = true
		wanted[r53RRSetKey(c.rrset.Type, c.rrset.Name)] = true
	}
	stored := make(map[string]r53StoredSet, len(wanted))
	for _, k := range index {
		ident := r53EntryIdentity(k)
		if !wanted[ident] {
			continue
		}
		rrset, found, err := p.loadRecordSet(id, k)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		s := stored[ident]
		if len(s.keys) == 0 || k == ident {
			s.rrset = rrset
		}
		s.keys = append(s.keys, k)
		stored[ident] = s
	}
	return stored, nil
}
