package emulator

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Route 53 record sets in a stack sweep (#1416, #1537).
//
// A record set has no delete operation of its own: it is removed by a change batch carrying Action
// DELETE, the mirror of the UPSERT that created it, keyed by type and name, which is why both come
// from the template rather than from the physical ID alone. A record set that is its zone's default
// SOA or NS cannot be deleted (#1537), so the stack's write there modified the default, and its delete
// restores the default instead. That needs the zone, read with the sweep's context, which is why these
// two types are sequenced deleters rather than entries in [cfnResourceDeleters].

// cfnDeleteRoute53RecordSet deletes one AWS::Route53::RecordSet.
func cfnDeleteRoute53RecordSet(ctx context.Context, d *StackDeployer, dr DeployedResource,
	props map[string]interface{}, cctx *cfnContext, streamID string,
) *CFNResourceDeletion {
	zone := resolveStringProp(props, "HostedZoneId", "", cctx)
	if zone == "" || dr.PhysicalID == "" {
		return cfnRoute53Unbuildable(dr)
	}
	set := cfnRoute53Element{name: dr.PhysicalID, rtype: resolveStringProp(props, "Type", "A", cctx),
		setID: resolveStringProp(props, "SetIdentifier", "", cctx)}
	return d.dispatchRecordSetDelete(ctx, dr, zone, []cfnRoute53Element{set}, streamID)
}

// cfnRoute53Element is one record set a stack wrote, by the identity the deploy sent.
type cfnRoute53Element struct {
	name, rtype, setID string
}

// cfnDeleteRoute53RecordSetGroup deletes an AWS::Route53::RecordSetGroup's record sets. The group's
// record sets are not recorded as resources of their own, so the group deletes them: one change batch
// per zone, in the order the zones first appear, carrying a change for every element, keyed by the
// same name, type and SetIdentifier the deploy upserted. An element naming a record set an earlier
// element of the group named is the same record set and is not deleted twice, which a batch would
// refuse. Each element is resolved with the group's zone filled in ([cfnRecordSetInGroup]); one that
// resolves to no zone or no name wrote nothing addressable. Until DeleteHostedZone refused a zone that
// still holds record sets (#1416) a group could be skipped; now a skipped group would leave its zone
// undeletable. The second and later zones' batches are sent before the first's, the order the pre-step
// table this replaced sent them in.
func cfnDeleteRoute53RecordSetGroup(ctx context.Context, d *StackDeployer, dr DeployedResource,
	props map[string]interface{}, cctx *cfnContext, streamID string,
) *CFNResourceDeletion {
	rsList, _ := props["RecordSets"].([]interface{})
	var zones []string
	elements := map[string][]cfnRoute53Element{}
	seen := map[string]bool{}
	for _, raw := range rsList {
		rs, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		rs = cfnRecordSetInGroup(props, rs)
		zone := resolveStringProp(rs, "HostedZoneId", "", cctx)
		name := resolveStringProp(rs, "Name", "", cctx)
		if zone == "" || name == "" {
			continue
		}
		el := cfnRoute53Element{name: name, rtype: resolveStringProp(rs, "Type", "A", cctx),
			setID: resolveStringProp(rs, "SetIdentifier", "", cctx)}
		identity := r53ZoneSuffix(zone) + "\x00" + r53RRSetIdentityKey(el.rtype, el.name, el.setID)
		if seen[identity] {
			continue
		}
		seen[identity] = true
		if _, ok := elements[zone]; !ok {
			zones = append(zones, zone)
		}
		elements[zone] = append(elements[zone], el)
	}
	if len(zones) == 0 {
		return cfnRoute53Unbuildable(dr)
	}
	order := append(append([]string{}, zones[1:]...), zones[0])
	for _, zone := range order {
		if failure := d.dispatchRecordSetDelete(ctx, dr, zone, elements[zone], streamID); failure != nil {
			return failure
		}
	}
	return nil
}

// dispatchRecordSetDelete removes the record sets elements names from zone in one change batch.
//
// The changes are built from the stored record sets ([StackDeployer.cfnRecordSetDeleteChange]), so a
// record set already gone builds none, and a batch with none is not sent. A record set can still go
// between the read and the batch, and a batch is transactional, so a refusal whose every failure is
// "but it was not found" is answered by building the batch again from what the zone now holds: the
// record sets that are gone drop out, and the rest are deleted. A second refusal of any kind fails the
// resource.
func (d *StackDeployer) dispatchRecordSetDelete(ctx context.Context, dr DeployedResource, zone string,
	elements []cfnRoute53Element, streamID string,
) *CFNResourceDeletion {
	for attempt := 0; ; attempt++ {
		var changes strings.Builder
		for _, el := range elements {
			change, err := d.cfnRecordSetDeleteChange(ctx, zone, el.name, el.rtype, el.setID)
			if err != nil {
				return &CFNResourceDeletion{LogicalID: dr.LogicalID, Type: dr.Type, Status: cfnDeleteFailed, Reason: err.Error()}
			}
			changes.WriteString(change)
		}
		if changes.Len() == 0 {
			return nil
		}
		_, _, err := d.dispatch(ctx, cfnRoute53ChangeBatch(zone, changes.String()), streamID)
		if err == nil || cfnDeleteIsAbsent(err) {
			return nil
		}
		if attempt > 0 || !r53OnlyNotFound(err) {
			return &CFNResourceDeletion{LogicalID: dr.LogicalID, Type: dr.Type, Status: cfnDeleteFailed, Reason: err.Error()}
		}
	}
}

// r53NotFoundMessage matches one "not found" failure of an InvalidChangeBatch ([planChangeBatch]).
var r53NotFoundMessage = regexp.MustCompile(`Tried to delete resource record set \[[^\]]*\] but it was not found(, )?`)

// r53OnlyNotFound reports whether err is an InvalidChangeBatch whose every failure is a DELETE of a
// record set the zone does not hold.
func r53OnlyNotFound(err error) bool {
	var awsErr *AWSError
	if !errors.As(err, &awsErr) || awsErr.Code != "InvalidChangeBatch" {
		return false
	}
	return awsErr.Message != "" && r53NotFoundMessage.ReplaceAllString(awsErr.Message, "") == ""
}

// cfnRoute53Unbuildable is the skip a record set that names no zone or no name reports, the reason
// [StackDeployer.deleteViaTable] gives a deleter that builds no request.
func cfnRoute53Unbuildable(dr DeployedResource) *CFNResourceDeletion {
	return &CFNResourceDeletion{LogicalID: dr.LogicalID, Type: dr.Type, Status: cfnDeleteSkipped,
		Reason: "the delete request for " + dr.Type + " could not be built"}
}

// cfnRoute53ChangeBatch wraps changes in a ChangeResourceRecordSets request for zone.
func cfnRoute53ChangeBatch(zone, changes string) *AWSRequest {
	body := `<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">` +
		`<ChangeBatch><Changes>` + changes + `</Changes></ChangeBatch></ChangeResourceRecordSetsRequest>`
	return &AWSRequest{Service: "route53", Operation: "POST",
		Path: "/2013-04-01/hostedzone/" + zone + "/rrset", Body: []byte(body),
		Headers: map[string]string{"Content-Type": "application/xml"},
		Params:  map[string]string{}}
}

// cfnRecordSetDeleteChange builds the one change that removes a record set a stack wrote, or "" when
// the record set is already gone.
//
// ChangeResourceRecordSets refuses a DELETE whose values differ from the stored record set's (#1586),
// and the template's values may not be what was stored (a Ref, a default the deploy filled in), so
// the DELETE carries the stored record set's own values. The record set is looked up by its whole
// identity, name, type and the template's SetIdentifier, so one the zone no longer holds is already
// deleted, which is what the sweep wants, and no change is built for it. For the apex SOA or NS
// of a zone that holds its defaults the change is an UPSERT of the default record set instead. A zone
// that does not exist takes a plain DELETE, whose NoSuchHostedZone the sweep reads as already gone; a
// zone or record set that cannot be read or decoded is an error, so the resource's delete fails rather
// than guessing. A zone stored before #1537 holds no defaults, so its apex record set is the stack's
// own and is deleted.
func (d *StackDeployer) cfnRecordSetDeleteChange(ctx context.Context, zoneID, name, rtype, setID string) (string, error) {
	zone, found, err := r53LoadZone(ctx, d.state, zoneID)
	if err != nil {
		return "", err
	}
	if !found {
		return `<Change><Action>DELETE</Action><ResourceRecordSet><Name>` + cfnXMLText(name) +
			`</Name><Type>` + cfnXMLText(rtype) + `</Type></ResourceRecordSet></Change>`, nil
	}
	t := strings.ToUpper(rtype)
	if zone.DefaultRecordSets && r53IsApexDefault(rtype, name, zone.Name) {
		for _, rrset := range r53DefaultRecordSets(r53ZoneSuffix(zoneID), zone.Name) {
			if rrset.Type == t {
				rrset.Name = name
				return cfnRoute53Change("UPSERT", rrset), nil
			}
		}
	}
	rrset, found, err := r53LoadRecordSetByName(ctx, d.state, r53ZoneSuffix(zoneID), rtype, name, setID)
	if err != nil || !found {
		return "", err
	}
	// A record set found under its name and type alone, for a template that names a SetIdentifier,
	// is deleted by that identifier, which is how ChangeResourceRecordSets matches it
	// ([r53LegacyRoutingMatch]).
	rrset.SetIdentifier = setID
	return cfnRoute53Change("DELETE", rrset), nil
}

// r53LoadRecordSetByName reads the record set of rtype named name with SetIdentifier setID from zone
// id, through the zone's index so a record set under a non-canonical key ([r53MatchingKeys]) is found
// too. For a setID it also finds the record set stored under the name and type alone without routing
// members, the one [r53LegacyRoutingMatch] matches; a record set with routing members of its own is
// never taken for another identifier's.
func r53LoadRecordSetByName(ctx context.Context, state StateManager, id, rtype, name, setID string) (Route53ResourceRecordSet, bool, error) {
	data, err := state.Get(ctx, route53Namespace, "rrset_keys:"+id)
	if err != nil {
		return Route53ResourceRecordSet{}, false, fmt.Errorf("route53 get record-set index %s: %w", id, err)
	}
	var index []string
	if data != nil {
		if err := json.Unmarshal(data, &index); err != nil {
			return Route53ResourceRecordSet{}, false, fmt.Errorf("route53 decode record-set index %s: %w", id, err)
		}
	}
	keys := r53MatchingKeys(index, r53RRSetIdentityKey(rtype, name, setID))
	if setID != "" {
		keys = append(keys, r53MatchingKeys(index, r53RRSetKey(rtype, name))...)
	}
	for _, k := range keys {
		raw, err := state.Get(ctx, route53Namespace, "rrset:"+id+":"+k)
		if err != nil {
			return Route53ResourceRecordSet{}, false, fmt.Errorf("route53 get record set %s: %w", k, err)
		}
		if raw == nil {
			continue
		}
		var rrset Route53ResourceRecordSet
		if err := json.Unmarshal(raw, &rrset); err != nil {
			return Route53ResourceRecordSet{}, false, fmt.Errorf("route53 decode record set %s: %w", k, err)
		}
		if rrset.SetIdentifier != setID && rrset.RoutingRecorded {
			// A record set of the name and type with routing members of its own is another one.
			continue
		}
		return rrset, true, nil
	}
	return Route53ResourceRecordSet{}, false, nil
}

// cfnRoute53Change renders one change carrying every value of rrset.
func cfnRoute53Change(action string, rrset Route53ResourceRecordSet) string {
	var b strings.Builder
	b.WriteString(`<Change><Action>` + action + `</Action><ResourceRecordSet><Name>` + cfnXMLText(rrset.Name) +
		`</Name><Type>` + cfnXMLText(rrset.Type) + `</Type>`)
	if rrset.SetIdentifier != "" {
		b.WriteString(`<SetIdentifier>` + cfnXMLText(rrset.SetIdentifier) + `</SetIdentifier>`)
	}
	if rrset.Weight != nil {
		b.WriteString(`<Weight>` + strconv.FormatInt(*rrset.Weight, 10) + `</Weight>`)
	}
	if rrset.Region != "" {
		b.WriteString(`<Region>` + cfnXMLText(rrset.Region) + `</Region>`)
	}
	if rrset.Failover != "" {
		b.WriteString(`<Failover>` + cfnXMLText(rrset.Failover) + `</Failover>`)
	}
	if rrset.AliasTarget == nil {
		b.WriteString(`<TTL>` + strconv.FormatInt(rrset.TTL, 10) + `</TTL>`)
	}
	if len(rrset.ResourceRecords) > 0 {
		b.WriteString(`<ResourceRecords>`)
		for _, rr := range rrset.ResourceRecords {
			b.WriteString(`<ResourceRecord><Value>` + cfnXMLText(rr.Value) + `</Value></ResourceRecord>`)
		}
		b.WriteString(`</ResourceRecords>`)
	}
	if a := rrset.AliasTarget; a != nil {
		b.WriteString(`<AliasTarget><HostedZoneId>` + cfnXMLText(a.HostedZoneID) + `</HostedZoneId><DNSName>` +
			cfnXMLText(a.DNSName) + `</DNSName><EvaluateTargetHealth>` + strconv.FormatBool(a.EvaluateTargetHealth) +
			`</EvaluateTargetHealth></AliasTarget>`)
	}
	if rrset.HealthCheckID != "" {
		b.WriteString(`<HealthCheckId>` + cfnXMLText(rrset.HealthCheckID) + `</HealthCheckId>`)
	}
	b.WriteString(`</ResourceRecordSet></Change>`)
	return b.String()
}

// cfnXMLText escapes s for an XML text node.
func cfnXMLText(s string) string {
	var b strings.Builder
	if err := xml.EscapeText(&b, []byte(s)); err != nil {
		return s
	}
	return b.String()
}
