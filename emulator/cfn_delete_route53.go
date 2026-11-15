package emulator

import (
	"context"
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
	change, err := d.cfnRecordSetDeleteChange(ctx, zone, dr.PhysicalID, resolveStringProp(props, "Type", "A", cctx))
	if err != nil {
		return &CFNResourceDeletion{LogicalID: dr.LogicalID, Type: dr.Type, Status: cfnDeleteFailed, Reason: err.Error()}
	}
	return d.dispatchResourceDelete(ctx, dr, cfnRoute53ChangeBatch(zone, change), streamID)
}

// cfnDeleteRoute53RecordSetGroup deletes an AWS::Route53::RecordSetGroup's record sets. The group's
// record sets are not recorded as resources of their own, so the group deletes them: one change batch
// per zone, in the order the zones first appear, carrying a change for every element, keyed by the
// same name and type the deploy upserted. Each element is resolved with the group's zone filled in
// ([cfnRecordSetInGroup]); one that resolves to no zone or no name wrote nothing addressable. Until
// DeleteHostedZone refused a zone that still holds record sets (#1416) a group could be skipped; now a
// skipped group would leave its zone undeletable. The second and later zones' batches are sent before
// the first's, the order the pre-step table this replaced sent them in.
func cfnDeleteRoute53RecordSetGroup(ctx context.Context, d *StackDeployer, dr DeployedResource,
	props map[string]interface{}, cctx *cfnContext, streamID string,
) *CFNResourceDeletion {
	rsList, _ := props["RecordSets"].([]interface{})
	var zones []string
	changes := map[string]string{}
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
		change, err := d.cfnRecordSetDeleteChange(ctx, zone, name, resolveStringProp(rs, "Type", "A", cctx))
		if err != nil {
			return &CFNResourceDeletion{LogicalID: dr.LogicalID, Type: dr.Type, Status: cfnDeleteFailed, Reason: err.Error()}
		}
		if _, seen := changes[zone]; !seen {
			zones = append(zones, zone)
		}
		changes[zone] += change
	}
	if len(zones) == 0 {
		return cfnRoute53Unbuildable(dr)
	}
	order := append(append([]string{}, zones[1:]...), zones[0])
	for _, zone := range order {
		if failure := d.dispatchResourceDelete(ctx, dr, cfnRoute53ChangeBatch(zone, changes[zone]), streamID); failure != nil {
			return failure
		}
	}
	return nil
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

// cfnRecordSetDeleteChange builds the one change that removes a record set a stack wrote: a DELETE
// keyed by name and type, or, for the apex SOA or NS of a zone that holds its defaults, an UPSERT of
// the default record set. A zone that does not exist takes the DELETE, whose NoSuchHostedZone the
// sweep reads as already gone; a zone that cannot be read or decoded is an error, so the resource's
// delete fails rather than guessing. A zone stored before #1537 holds no defaults, so its apex record
// set is the stack's own and is deleted.
func (d *StackDeployer) cfnRecordSetDeleteChange(ctx context.Context, zoneID, name, rtype string) (string, error) {
	del := `<Change><Action>DELETE</Action><ResourceRecordSet><Name>` + name +
		`</Name><Type>` + rtype + `</Type></ResourceRecordSet></Change>`
	t := strings.ToUpper(rtype)
	if t != "SOA" && t != "NS" {
		return del, nil
	}
	zone, found, err := r53LoadZone(ctx, d.state, zoneID)
	if err != nil {
		return "", err
	}
	if !found || !zone.DefaultRecordSets || !r53IsApexDefault(rtype, name, zone.Name) {
		return del, nil
	}
	for _, rrset := range r53DefaultRecordSets(r53ZoneSuffix(zoneID), zone.Name) {
		if rrset.Type != t {
			continue
		}
		var values strings.Builder
		for _, rr := range rrset.ResourceRecords {
			values.WriteString(`<ResourceRecord><Value>` + rr.Value + `</Value></ResourceRecord>`)
		}
		return `<Change><Action>UPSERT</Action><ResourceRecordSet><Name>` + name + `</Name><Type>` + t +
			`</Type><TTL>` + strconv.FormatInt(rrset.TTL, 10) + `</TTL><ResourceRecords>` + values.String() +
			`</ResourceRecords></ResourceRecordSet></Change>`, nil
	}
	return del, nil
}
