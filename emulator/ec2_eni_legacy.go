package emulator

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
)

// Reading an instance's interfaces, stored and synthesized (#1522).
//
// An instance launched by a build that stored interfaces reads its networkInterfaceSet from the
// records attached to it. An instance an older build recorded carries its interfaces on itself,
// and [ec2LegacyInterfaces] synthesizes them at read time, with IDs derived from what it recorded
// and no draw: the declared interface IDs it embedded, or eni- and the instance's hex suffix for
// a primary it never recorded, and an attachment ID that is eni-attach- and the interface ID's hex
// suffix. Each is in the VPC of its own subnet. An older multi-instance launch that named one
// interface by NetworkInterfaceId embedded that ID on every instance; the interface is reported
// once, on the lowest instance ID among the live ones carrying it ([ec2LegacyState]).
//
// The first write of any kind that names one of those interfaces or attachments —
// ModifyNetworkInterfaceAttribute, DetachNetworkInterface, CreateTags, or the instance's
// termination — stores every interface of that instance under the same IDs
// ([EC2Plugin.materializeLegacyInstance]) and then writes a marker,
// eni_legacy:{account}/{region}/{instance}, that the instance's interfaces are stored. The marker,
// not the records, is what stops the synthesis, so an interface stored and then deleted does not
// come back, and the instance record itself is never rewritten. A reader reads the markers before
// the records, the reverse of the write order, and takes a stored record over a synthesized one
// with the same ID, so a read racing the store sees each interface once.

// ec2LegacyMarkerKey is the state key of the marker that an older instance's interfaces are
// stored.
func ec2LegacyMarkerKey(acct, region, instanceID string) string {
	return "eni_legacy:" + acct + "/" + region + "/" + instanceID
}

// ec2LegacyAttachmentID is the attachment ID an older instance's interface reports: eni-attach-
// and the interface ID's suffix, so a synthesized attachment and the stored one agree.
func ec2LegacyAttachmentID(eniID string) string {
	return "eni-attach-" + strings.TrimPrefix(eniID, "eni-")
}

// ec2LegacyMaterialized is the set of instances whose interfaces have been stored.
func (p *EC2Plugin) ec2LegacyMaterialized(ctx context.Context, acct, region string) (map[string]bool, error) {
	prefix := "eni_legacy:" + acct + "/" + region + "/"
	keys, err := p.state.List(ctx, ec2Namespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("ec2 stored older interfaces: %w", err)
	}
	out := make(map[string]bool, len(keys))
	for _, k := range keys {
		out[strings.TrimPrefix(k, prefix)] = true
	}
	return out, nil
}

// ec2LegacyState is what synthesizing an older instance's interfaces needs to know beyond the
// instance: which instances' interfaces are stored, which interface IDs are stored, which live
// instance owns an interface ID several embed, and each subnet's VPC.
type ec2LegacyState struct {
	materialized map[string]bool
	stored       map[string]bool
	owner        map[string]string
	vpcOf        map[string]string
}

// ec2LegacyStateFor reads the account and Region's [ec2LegacyState]. The markers are read first.
func (p *EC2Plugin) ec2LegacyStateFor(ctx context.Context, acct, region string) (*ec2LegacyState, error) {
	materialized, err := p.ec2LegacyMaterialized(ctx, acct, region)
	if err != nil {
		return nil, err
	}
	ls := &ec2LegacyState{materialized: materialized, stored: map[string]bool{}, owner: map[string]string{}, vpcOf: map[string]string{}}
	scope := acct + "/" + region + "/"
	storedKeys, err := p.state.List(ctx, ec2Namespace, "eni:"+scope)
	if err != nil {
		return nil, fmt.Errorf("ec2 stored interface IDs: %w", err)
	}
	for _, k := range storedKeys {
		ls.stored[ec2StateKeyTail(k)] = true
	}
	if err := ec2ScanRecords(ctx, p.state, "instance:"+scope, func(_ string, inst EC2Instance) {
		if inst.InterfacesStandalone || inst.State.Name == ec2StateTerminated {
			return
		}
		for _, ifc := range inst.NetworkInterfaces {
			if o, ok := ls.owner[ifc.NetworkInterfaceID]; !ok || inst.InstanceID < o {
				ls.owner[ifc.NetworkInterfaceID] = inst.InstanceID
			}
		}
	}); err != nil {
		return nil, fmt.Errorf("ec2 older interface owners: %w", err)
	}
	if err := ec2ScanRecords(ctx, p.state, "subnet:"+scope, func(_ string, s EC2Subnet) {
		ls.vpcOf[s.SubnetID] = s.VPCID
	}); err != nil {
		return nil, fmt.Errorf("ec2 older interface subnets: %w", err)
	}
	return ls, nil
}

// ec2LegacyUnstored is [ec2LegacyInterfaces] for an instance whose interfaces have not been
// stored, less the interfaces another instance owns, each in its subnet's VPC; and nothing for an
// instance whose interfaces have been stored.
func ec2LegacyUnstored(inst EC2Instance, ls *ec2LegacyState) []EC2NetworkInterfaceRecord {
	if ls.materialized[inst.InstanceID] {
		return nil
	}
	return ls.interfaces(inst)
}

// interfaces is [ec2LegacyInterfaces] less the interfaces another instance owns and the ones
// already stored, each in its subnet's VPC.
func (ls *ec2LegacyState) interfaces(inst EC2Instance) []EC2NetworkInterfaceRecord {
	var out []EC2NetworkInterfaceRecord
	for _, rec := range ec2LegacyInterfaces(inst) {
		if o, ok := ls.owner[rec.NetworkInterfaceID]; ok && o != inst.InstanceID {
			continue
		}
		if ls.stored[rec.NetworkInterfaceID] {
			continue
		}
		if vpc, ok := ls.vpcOf[rec.SubnetID]; ok {
			rec.VPCID = vpc
		}
		out = append(out, rec)
	}
	return out
}

// ec2InterfaceIndex is every stored interface in an account and Region, read once: the attached
// ones bucketed by instance in position order, and the [ec2LegacyState] synthesis reads.
type ec2InterfaceIndex struct {
	byInstance map[string][]EC2NetworkInterfaceRecord
	legacy     *ec2LegacyState
}

// ec2InterfacesByInstance reads every stored interface once and indexes it — the read
// [EC2Plugin.ec2InstanceItemFor]'s callers make once per response, as they read block devices,
// rather than once per instance. The markers are read before the records.
func (p *EC2Plugin) ec2InterfacesByInstance(acct, region string) (*ec2InterfaceIndex, error) {
	ctx := context.Background()
	legacy, err := p.ec2LegacyStateFor(ctx, acct, region)
	if err != nil {
		return nil, err
	}
	all, err := p.ec2NetworkInterfaces(ctx, acct, region)
	if err != nil {
		return nil, err
	}
	idx := &ec2InterfaceIndex{byInstance: map[string][]EC2NetworkInterfaceRecord{}, legacy: legacy}
	for _, rec := range all {
		if rec.attached() {
			idx.byInstance[rec.Attachment.InstanceID] = append(idx.byInstance[rec.Attachment.InstanceID], rec)
		}
	}
	for id := range idx.byInstance {
		ec2SortRecordsByDeviceIndex(idx.byInstance[id])
	}
	return idx, nil
}

// ec2SortRecordsByDeviceIndex orders attached interfaces by network card, device index, then ID.
func ec2SortRecordsByDeviceIndex(recs []EC2NetworkInterfaceRecord) {
	sort.SliceStable(recs, func(i, j int) bool {
		a, b := recs[i].Attachment, recs[j].Attachment
		if a.NetworkCardIndex != b.NetworkCardIndex {
			return a.NetworkCardIndex < b.NetworkCardIndex
		}
		if a.DeviceIndex != b.DeviceIndex {
			return a.DeviceIndex < b.DeviceIndex
		}
		return recs[i].NetworkInterfaceID < recs[j].NetworkInterfaceID
	})
}

// ec2MergeSynthesized appends to stored the synthesized interfaces whose IDs none of stored has.
func ec2MergeSynthesized(stored, synthesized []EC2NetworkInterfaceRecord) []EC2NetworkInterfaceRecord {
	have := make(map[string]bool, len(stored))
	for _, rec := range stored {
		have[rec.NetworkInterfaceID] = true
	}
	out := append([]EC2NetworkInterfaceRecord(nil), stored...)
	for _, rec := range synthesized {
		if !have[rec.NetworkInterfaceID] {
			out = append(out, rec)
		}
	}
	return out
}

// ec2InstanceInterfaces returns inst's interfaces: its attached records, plus for an instance an
// older build recorded the synthesized interfaces not yet stored.
func ec2InstanceInterfaces(inst EC2Instance, idx *ec2InterfaceIndex) []EC2NetworkInterfaceRecord {
	if inst.InterfacesStandalone || idx.legacy.materialized[inst.InstanceID] {
		return idx.byInstance[inst.InstanceID]
	}
	out := ec2MergeSynthesized(idx.byInstance[inst.InstanceID], idx.legacy.interfaces(inst))
	ec2SortRecordsByDeviceIndex(out)
	return out
}

// ec2InstanceGroups returns inst's groupSet, which is its primary interface's groups, or the
// groups the instance recorded when it has no primary interface (a terminated instance).
func ec2InstanceGroups(inst EC2Instance, idx *ec2InterfaceIndex) []string {
	for _, rec := range ec2InstanceInterfaces(inst, idx) {
		if ec2IsPrimaryAttachment(rec.Attachment) {
			return rec.SecurityGroupIDs
		}
	}
	return inst.SecurityGroupIDs
}

// requireInterfaceRecord reads the interface eniID names: the stored record, or the interface an
// older build's instance reports for it, synthesized. Nothing is written.
func (p *EC2Plugin) requireInterfaceRecord(reqCtx *RequestContext, eniID string) (EC2NetworkInterfaceRecord, bool, error) {
	rec, found, err := p.loadNetworkInterface(context.Background(), reqCtx.AccountID, reqCtx.Region, eniID)
	if err != nil || found {
		return rec, found, err
	}
	_, legacy, err := p.legacyInstanceReporting(reqCtx, func(l EC2NetworkInterfaceRecord) bool {
		return l.NetworkInterfaceID == eniID
	})
	if err != nil || legacy == nil {
		return EC2NetworkInterfaceRecord{}, false, err
	}
	return *legacy, true, nil
}

// legacyInstanceReporting finds the live older instance whose unstored interfaces include one
// match accepts, returning the instance and that interface, or nils.
func (p *EC2Plugin) legacyInstanceReporting(
	reqCtx *RequestContext, match func(EC2NetworkInterfaceRecord) bool,
) (*EC2Instance, *EC2NetworkInterfaceRecord, error) {
	ctx := context.Background()
	ls, err := p.ec2LegacyStateFor(ctx, reqCtx.AccountID, reqCtx.Region)
	if err != nil {
		return nil, nil, err
	}
	var foundInst *EC2Instance
	var foundRec *EC2NetworkInterfaceRecord
	if err := ec2ScanRecords(ctx, p.state, "instance:"+reqCtx.AccountID+"/"+reqCtx.Region+"/", func(_ string, inst EC2Instance) {
		if foundInst != nil {
			return
		}
		for _, rec := range ec2LegacyUnstored(inst, ls) {
			if match(rec) {
				foundInst, foundRec = &inst, &rec
				return
			}
		}
	}); err != nil {
		return nil, nil, fmt.Errorf("ec2 network interface older lookup: %w", err)
	}
	return foundInst, foundRec, nil
}

// materializeLegacyInstance stores every interface inst reports — synthesized as it ran, so a
// termination can store them after writing the instance terminated — under their derived IDs,
// moves their addresses to the stored interfaces in txn, and then writes the marker. An address
// another older instance holds is moved too: older builds could record one address on several
// instances, and the stored interface carrying it is then its holder. An interface another
// instance owns, or one already stored, is left as it is.
func (p *EC2Plugin) materializeLegacyInstance(reqCtx *RequestContext, txn *ec2AddrTxn, inst EC2Instance) error {
	ctx := context.Background()
	ls, err := p.ec2LegacyStateFor(ctx, reqCtx.AccountID, reqCtx.Region)
	if err != nil {
		return err
	}
	running := inst
	running.State = EC2InstanceState{Name: "running"}
	// The instance being terminated is no longer live, so the owners were read without it; an
	// interface it shares is its own when it holds the lowest ID among those carrying it.
	for _, ifc := range inst.NetworkInterfaces {
		if o, ok := ls.owner[ifc.NetworkInterfaceID]; ok && inst.InstanceID < o {
			ls.owner[ifc.NetworkInterfaceID] = inst.InstanceID
		}
	}
	for _, l := range ls.interfaces(running) {
		if l.InterfaceType == "" {
			l.InterfaceType = "interface"
		}
		for _, addr := range l.addresses() {
			if err := txn.transferIf(l.SubnetID, addr, func(holder string) bool {
				return holder == inst.InstanceID || strings.HasPrefix(holder, "i-")
			}, l.NetworkInterfaceID); err != nil {
				return err
			}
		}
		if err := p.putNetworkInterface(ctx, l); err != nil {
			return fmt.Errorf("ec2 store older interface: %w", err)
		}
	}
	if err := p.putEC2Record(ctx, ec2LegacyMarkerKey(reqCtx.AccountID, reqCtx.Region, inst.InstanceID), map[string]string{
		"instance_id": inst.InstanceID,
	}); err != nil {
		return fmt.Errorf("ec2 store older interfaces marker: %w", err)
	}
	return nil
}

// materializeMatching stores the interfaces of the live older instance reporting an interface
// match accepts, reporting whether there was one.
func (p *EC2Plugin) materializeMatching(reqCtx *RequestContext, txn *ec2AddrTxn, match func(EC2NetworkInterfaceRecord) bool) (bool, error) {
	inst, _, err := p.legacyInstanceReporting(reqCtx, match)
	if err != nil || inst == nil {
		return false, err
	}
	return true, p.materializeLegacyInstance(reqCtx, txn, *inst)
}

// materializeLegacyInterfaces returns the stored record for eniID, storing first the interfaces of
// the older instance that reports it when it is not stored yet; found is false when no stored or
// synthesized interface has that ID.
func (p *EC2Plugin) materializeLegacyInterfaces(reqCtx *RequestContext, txn *ec2AddrTxn, eniID string) (EC2NetworkInterfaceRecord, bool, error) {
	ctx := context.Background()
	rec, found, err := p.loadNetworkInterface(ctx, reqCtx.AccountID, reqCtx.Region, eniID)
	if err != nil || found {
		return rec, found, err
	}
	stored, err := p.materializeMatching(reqCtx, txn, func(l EC2NetworkInterfaceRecord) bool {
		return l.NetworkInterfaceID == eniID
	})
	if err != nil || !stored {
		return EC2NetworkInterfaceRecord{}, false, err
	}
	return p.loadNetworkInterface(ctx, reqCtx.AccountID, reqCtx.Region, eniID)
}

// materializeInterfaceTagTargets stores, before CreateTags applies, the interfaces of any older
// instance reporting one of the eni- IDs the request names, so the tags land on a record. The
// caller holds [EC2Plugin.addrMu], which [EC2Plugin.lockTagWrite] takes for eni- IDs. DeleteTags
// does not call it: an unstored interface carries no tags, so there is nothing to remove.
func (p *EC2Plugin) materializeInterfaceTagTargets(reqCtx *RequestContext, ids []string) error {
	txn := p.addrTxnLocked(reqCtx)
	for _, id := range ids {
		if !strings.HasPrefix(id, "eni-") {
			continue
		}
		if _, _, err := p.materializeLegacyInterfaces(reqCtx, txn, id); err != nil {
			return err
		}
	}
	return txn.commit()
}

// otherReporter returns a resource other than those in exclude that still reports addr in
// subnetID — a stored interface, a NAT gateway not yet deleted, or a live older instance whose
// interfaces are not stored — or "" when none does. Older builds could record one address on
// several resources, so a release passes such an address to the next reporter rather than
// freeing it.
func (p *EC2Plugin) otherReporter(ctx context.Context, acct, region, subnetID, addr string, exclude []string) (string, error) {
	scope := acct + "/" + region + "/"
	ls, err := p.ec2LegacyStateFor(ctx, acct, region)
	if err != nil {
		return "", err
	}
	found := ""
	if err := ec2ScanRecords(ctx, p.state, "eni:"+scope, func(_ string, rec EC2NetworkInterfaceRecord) {
		if found == "" && rec.SubnetID == subnetID && !slices.Contains(exclude, rec.NetworkInterfaceID) &&
			slices.Contains(rec.addresses(), addr) {
			found = rec.NetworkInterfaceID
		}
	}); err != nil {
		return "", fmt.Errorf("ec2 address reporters: %w", err)
	}
	if found != "" {
		return found, nil
	}
	if err := ec2ScanRecords(ctx, p.state, "nat:"+scope, func(_ string, gw EC2NATGateway) {
		if found == "" && gw.SubnetID == subnetID && gw.State != "deleted" && gw.PrivateIP == addr &&
			!slices.Contains(exclude, gw.NatGatewayID) {
			found = gw.NatGatewayID
		}
	}); err != nil {
		return "", fmt.Errorf("ec2 address reporters: %w", err)
	}
	if found != "" {
		return found, nil
	}
	if err := ec2ScanRecords(ctx, p.state, "instance:"+scope, func(_ string, inst EC2Instance) {
		if found != "" || slices.Contains(exclude, inst.InstanceID) {
			return
		}
		for _, rec := range ec2LegacyUnstored(inst, ls) {
			if rec.SubnetID == subnetID && slices.Contains(rec.addresses(), addr) {
				found = inst.InstanceID
				return
			}
		}
	}); err != nil {
		return "", fmt.Errorf("ec2 address reporters: %w", err)
	}
	return found, nil
}

// ec2LegacyUsesGroup reports whether an older instance's unstored interfaces carry sgID.
func ec2LegacyUsesGroup(inst EC2Instance, ls *ec2LegacyState, sgID string) bool {
	for _, rec := range ec2LegacyUnstored(inst, ls) {
		if slices.Contains(rec.SecurityGroupIDs, sgID) {
			return true
		}
	}
	return false
}
