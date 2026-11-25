package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// Network interfaces as standalone resources (#1522).
//
// Every interface substrate holds is a record of its own under eni:{account}/{region}/{eni-id}:
// an instance's primary interface, which RunInstances mints for every instance; each secondary
// interface a launch's NetworkInterface.N creates; and each one CreateNetworkInterface makes. An
// interface's attachment — the instance, device index and DeleteOnTermination — lives on the
// interface, which is where AWS reports it, so an `available` interface with no attachment is
// expressible and AttachNetworkInterface and DetachNetworkInterface change one record.
//
// An instance's networkInterfaceSet is read from the records attached to it. An instance from an
// event log written before the records existed — [EC2Instance.InterfacesStandalone] unset — has
// none, and its interfaces are synthesized at read time from what it recorded
// ([ec2LegacyInterfaces]), with no ID drawn: the declared interfaces it embedded keep their IDs,
// and an instance that declared none reports a primary interface whose ID is the instance's own
// hex suffix behind eni- (i-0123456789abcdef0 reports eni-0123456789abcdef0), so two reads agree
// and a replay agrees with both.

// EC2NetworkInterfaceRecord is one stored network interface, AWS's NetworkInterface.
type EC2NetworkInterfaceRecord struct {
	// NetworkInterfaceID is the interface's eni- ID.
	NetworkInterfaceID string `json:"network_interface_id"`

	// SubnetID, VPCID and AvailabilityZone are where the interface is: its subnet, that
	// subnet's VPC and zone.
	SubnetID         string `json:"subnet_id"`
	VPCID            string `json:"vpc_id"`
	AvailabilityZone string `json:"availability_zone,omitempty"`

	// Description is the interface's description.
	Description string `json:"description,omitempty"`

	// InterfaceType is interface, efa, efa-only or trunk.
	InterfaceType string `json:"interface_type,omitempty"`

	// PrivateIPAddress and PrivateDNSName are the interface's primary private IPv4 address and
	// its hostname.
	PrivateIPAddress string `json:"private_ip_address,omitempty"`
	PrivateDNSName   string `json:"private_dns_name,omitempty"`

	// SecondaryPrivateIPAddresses are the interface's secondary private IPv4 addresses, from
	// PrivateIpAddresses.N or SecondaryPrivateIpAddressCount.
	SecondaryPrivateIPAddresses []string `json:"secondary_private_ip_addresses,omitempty"`

	// SecurityGroupIDs are the interface's security groups.
	SecurityGroupIDs []string `json:"security_group_ids,omitempty"`

	// SourceDestCheckDisabled records ModifyNetworkInterfaceAttribute's SourceDestCheck=false.
	// Stored inverted so the zero value is AWS's default, enabled.
	SourceDestCheckDisabled bool `json:"source_dest_check_disabled,omitempty"`

	// Status is available or in-use.
	Status string `json:"status"`

	// Attachment is the interface's attachment to an instance, nil when it is available.
	Attachment *EC2NetworkInterfaceAttachment `json:"attachment,omitempty"`

	// Tags are the interface's tags, from TagSpecification.N with ResourceType
	// network-interface or from CreateTags.
	Tags []EC2Tag `json:"tags,omitempty"`

	// EverTagged records that this interface has carried a tag; see [taggingEverTagged].
	EverTagged bool `json:"ever_tagged,omitempty"`

	// AccountID and Region are the interface's account and Region.
	AccountID string `json:"account_id"`
	Region    string `json:"region"`
}

// EC2NetworkInterfaceAttachment is an interface's attachment, AWS's NetworkInterfaceAttachment.
type EC2NetworkInterfaceAttachment struct {
	// AttachmentID is the eni-attach- ID DetachNetworkInterface names.
	AttachmentID string `json:"attachment_id"`

	// InstanceID is the instance the interface is attached to.
	InstanceID string `json:"instance_id"`

	// DeviceIndex and NetworkCardIndex are the interface's position on the instance.
	DeviceIndex      int `json:"device_index"`
	NetworkCardIndex int `json:"network_card_index,omitempty"`

	// DeleteOnTermination reports whether the interface is deleted with the instance.
	DeleteOnTermination bool `json:"delete_on_termination"`

	// AttachTime is when the attachment was made, on the simulated clock.
	AttachTime string `json:"attach_time,omitempty"`

	// Detached records an attachment that has ended. EC2 goes on reporting the last
	// attachment of an available interface with status detached, and the Terraform AWS
	// provider waits for that status after DetachNetworkInterface, so the attachment is kept
	// until the interface is attached again or deleted.
	Detached bool `json:"detached,omitempty"`

	// Primary records the primary interface of an instance an older build recorded with no
	// interface at device index 0 on network card 0: that build took the lowest device index as
	// primary, and its interfaces keep the decision. See [ec2IsPrimaryAttachment].
	Primary bool `json:"primary,omitempty"`
}

// ec2IsPrimaryAttachment reports whether a is its instance's primary interface: the one at the
// primary position ([ec2IsPrimaryPosition]), or one an older build's decision marks primary.
func ec2IsPrimaryAttachment(a *EC2NetworkInterfaceAttachment) bool {
	return a != nil && (a.Primary || ec2IsPrimaryPosition(a.NetworkCardIndex, a.DeviceIndex))
}

// ec2AttachmentStatus is an attachment's status: attached, or detached once it has ended.
func ec2AttachmentStatus(a *EC2NetworkInterfaceAttachment) string {
	if a.Detached {
		return "detached"
	}
	return "attached"
}

// attached reports whether the interface is attached to an instance now.
func (r EC2NetworkInterfaceRecord) attached() bool {
	return r.Attachment != nil && !r.Attachment.Detached
}

// detach ends the interface's attachment, leaving it available with the attachment
// reported as detached.
func (r *EC2NetworkInterfaceRecord) detach() {
	r.Status = "available"
	if r.Attachment != nil {
		r.Attachment.Detached = true
	}
}

// ec2NetworkInterfaceItem is one interface as a response carries it: an instance's
// networkInterfaceSet>item (InstanceNetworkInterface) in RunInstances and DescribeInstances,
// and NetworkInterface in DescribeNetworkInterfaces and CreateNetworkInterface.
//
// One type for both shapes, as [ec2GroupItem] is one type for every groupSet, so the responses
// cannot drift apart. availabilityZone, requesterManaged and tagSet are NetworkInterface members
// only and are left empty for an instance's set; see [EC2Plugin.ec2NetworkInterfaceItemFor].
type ec2NetworkInterfaceItem struct {
	NetworkInterfaceID string                    `xml:"networkInterfaceId"`
	SubnetID           string                    `xml:"subnetId,omitempty"`
	VpcID              string                    `xml:"vpcId,omitempty"`
	AvailabilityZone   string                    `xml:"availabilityZone,omitempty"`
	Description        string                    `xml:"description,omitempty"`
	OwnerID            string                    `xml:"ownerId"`
	RequesterManaged   *bool                     `xml:"requesterManaged,omitempty"`
	Status             string                    `xml:"status"`
	PrivateIPAddress   string                    `xml:"privateIpAddress,omitempty"`
	PrivateDNSName     string                    `xml:"privateDnsName,omitempty"`
	SourceDestCheck    *bool                     `xml:"sourceDestCheck,omitempty"`
	Groups             []ec2GroupItem            `xml:"groupSet>item,omitempty"`
	Attachment         *ec2NetworkAttachmentItem `xml:"attachment,omitempty"`
	Tags               *ec2TagSetXML             `xml:"tagSet,omitempty"`
	InterfaceType      string                    `xml:"interfaceType,omitempty"`
	PrivateIPAddresses []ec2NetworkPrivateIPItem `xml:"privateIpAddressesSet>item,omitempty"`
}

// ec2NetworkAttachmentItem is an interface's attachment element, which is where AWS
// reports the device index rather than on the interface itself. It is absent for an
// available interface.
//
// InstanceID and InstanceOwnerID are members of DescribeNetworkInterfaces' attachment only;
// an instance's own networkInterfaceSet leaves them empty, so they are omitted there.
type ec2NetworkAttachmentItem struct {
	AttachmentID        string `xml:"attachmentId,omitempty"`
	AttachTime          string `xml:"attachTime,omitempty"`
	InstanceID          string `xml:"instanceId,omitempty"`
	InstanceOwnerID     string `xml:"instanceOwnerId,omitempty"`
	DeviceIndex         int    `xml:"deviceIndex"`
	Status              string `xml:"status"`
	DeleteOnTermination bool   `xml:"deleteOnTermination"`
	NetworkCardIndex    int    `xml:"networkCardIndex"`
}

// ec2NetworkPrivateIPItem is one entry in an interface's privateIpAddressesSet: the primary
// address first, then each secondary one.
type ec2NetworkPrivateIPItem struct {
	PrivateIPAddress string `xml:"privateIpAddress"`
	PrivateDNSName   string `xml:"privateDnsName,omitempty"`
	Primary          bool   `xml:"primary"`
}

// addresses lists every private IPv4 address the interface holds, primary first.
func (r EC2NetworkInterfaceRecord) addresses() []string {
	var out []string
	if r.PrivateIPAddress != "" {
		out = append(out, r.PrivateIPAddress)
	}
	return append(out, r.SecondaryPrivateIPAddresses...)
}

// ec2ENIKey is the state key of one network interface.
func ec2ENIKey(accountID, region, eniID string) string {
	return "eni:" + accountID + "/" + region + "/" + eniID
}

// generateENIAttachmentID mints a network interface attachment ID, "eni-attach-" and 16 hex
// characters, the form the client-error table gives for InvalidNetworkInterfaceAttachmentId.
func generateENIAttachmentID(m *IDMint) string {
	return "eni-attach-" + m.Hex(8)
}

// ec2NetworkInterfaceAttachmentIDKind is the eni-attach- family. Both codes are the client-error
// table's, with its casing: InvalidAttachmentID.NotFound ("The specified network interface
// attachment does not exist.") and InvalidNetworkInterfaceAttachmentId.Malformed.
var ec2NetworkInterfaceAttachmentIDKind = ec2IDKind{
	Prefix: "eni-attach-", NotFound: "InvalidAttachmentID.NotFound",
	Malformed: "InvalidNetworkInterfaceAttachmentId.Malformed", Noun: "network interface attachment",
}

// ec2LegacyInterfaceID is the ID an instance predating standalone interfaces reports for the
// primary interface it never recorded: the instance's hex suffix behind eni-.
func ec2LegacyInterfaceID(instanceID string) string {
	return "eni-" + strings.TrimPrefix(instanceID, "i-")
}

// ec2LegacyInterfaces synthesizes the interfaces of an instance written before standalone
// interface records, from what it recorded: its embedded NetworkInterfaces when its launch
// declared any, and otherwise one primary interface from its flat networking fields. A
// terminated instance has none, and neither does one that recorded no address.
func ec2LegacyInterfaces(inst EC2Instance) []EC2NetworkInterfaceRecord {
	if inst.InterfacesStandalone || inst.State.Name == ec2StateTerminated {
		return nil
	}
	attached := func(ifc EC2NetworkInterface) EC2NetworkInterfaceRecord {
		return EC2NetworkInterfaceRecord{
			NetworkInterfaceID: ifc.NetworkInterfaceID,
			SubnetID:           ifc.SubnetID,
			VPCID:              inst.VPCID,
			AvailabilityZone:   inst.AvailabilityZone,
			Description:        ifc.Description,
			InterfaceType:      ifc.InterfaceType,
			PrivateIPAddress:   ifc.PrivateIPAddress,
			PrivateDNSName:     ifc.PrivateDNSName,
			SecurityGroupIDs:   ifc.SecurityGroupIDs,
			Status:             "in-use",
			Attachment: &EC2NetworkInterfaceAttachment{
				AttachmentID:        ec2LegacyAttachmentID(ifc.NetworkInterfaceID),
				InstanceID:          inst.InstanceID,
				DeviceIndex:         ifc.DeviceIndex,
				NetworkCardIndex:    ifc.NetworkCardIndex,
				DeleteOnTermination: ifc.DeleteOnTermination,
				AttachTime:          inst.LaunchTime,
			},
			AccountID: inst.AccountID,
			Region:    inst.Region,
		}
	}
	if len(inst.NetworkInterfaces) > 0 {
		out := make([]EC2NetworkInterfaceRecord, 0, len(inst.NetworkInterfaces))
		lowest, atPrimary := -1, false
		for i, ifc := range inst.NetworkInterfaces {
			out = append(out, attached(ifc))
			atPrimary = atPrimary || ec2IsPrimaryPosition(ifc.NetworkCardIndex, ifc.DeviceIndex)
			if lowest < 0 || ifc.DeviceIndex < inst.NetworkInterfaces[lowest].DeviceIndex {
				lowest = i
			}
		}
		// The older build took the lowest device index as primary when the launch declared
		// nothing at the primary position.
		if !atPrimary {
			out[lowest].Attachment.Primary = true
		}
		return out
	}
	if inst.PrivateIPAddress == "" {
		return nil
	}
	return []EC2NetworkInterfaceRecord{attached(EC2NetworkInterface{
		NetworkInterfaceID:  ec2LegacyInterfaceID(inst.InstanceID),
		SubnetID:            inst.SubnetID,
		PrivateIPAddress:    inst.PrivateIPAddress,
		PrivateDNSName:      inst.PrivateDNSName,
		SecurityGroupIDs:    inst.SecurityGroupIDs,
		InterfaceType:       "interface",
		DeleteOnTermination: true,
	})}
}

// loadNetworkInterface reads one stored interface, reporting whether it exists.
func (p *EC2Plugin) loadNetworkInterface(ctx context.Context, acct, region, eniID string) (EC2NetworkInterfaceRecord, bool, error) {
	data, err := p.state.Get(ctx, ec2Namespace, ec2ENIKey(acct, region, eniID))
	if err != nil {
		return EC2NetworkInterfaceRecord{}, false, fmt.Errorf("ec2 get network interface %s: %w", eniID, err)
	}
	if data == nil {
		return EC2NetworkInterfaceRecord{}, false, nil
	}
	var rec EC2NetworkInterfaceRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return EC2NetworkInterfaceRecord{}, false, fmt.Errorf("ec2 unmarshal network interface %s: %w", eniID, err)
	}
	return rec, true, nil
}

// putNetworkInterface writes one interface.
func (p *EC2Plugin) putNetworkInterface(ctx context.Context, rec EC2NetworkInterfaceRecord) error {
	return p.putEC2Record(ctx, ec2ENIKey(rec.AccountID, rec.Region, rec.NetworkInterfaceID), rec)
}

// ec2NetworkInterfaces reads every stored interface in the account and Region.
func (p *EC2Plugin) ec2NetworkInterfaces(ctx context.Context, acct, region string) ([]EC2NetworkInterfaceRecord, error) {
	var out []EC2NetworkInterfaceRecord
	if err := ec2ScanRecords(ctx, p.state, "eni:"+acct+"/"+region+"/", func(_ string, rec EC2NetworkInterfaceRecord) {
		out = append(out, rec)
	}); err != nil {
		return nil, fmt.Errorf("ec2 network interfaces: %w", err)
	}
	return out, nil
}

// ec2NetworkInterfaceItemFor renders one interface. forInstance selects the shape an
// instance's networkInterfaceSet carries (InstanceNetworkInterface), which has no
// availabilityZone, tagSet or attachment instanceId; DescribeNetworkInterfaces and
// CreateNetworkInterface carry all three (NetworkInterface).
//
// status is the record's, and an attachment's status is attached, or detached once it has
// ended: substrate's attachments and detachments complete within the request that makes them.
func (p *EC2Plugin) ec2NetworkInterfaceItemFor(reqCtx *RequestContext, rec EC2NetworkInterfaceRecord, forInstance bool) ec2NetworkInterfaceItem {
	sdc := !rec.SourceDestCheckDisabled
	falseVal := false
	item := ec2NetworkInterfaceItem{
		NetworkInterfaceID: rec.NetworkInterfaceID,
		SubnetID:           rec.SubnetID,
		VpcID:              rec.VPCID,
		Description:        rec.Description,
		OwnerID:            reqCtx.AccountID,
		Status:             rec.Status,
		PrivateIPAddress:   rec.PrivateIPAddress,
		PrivateDNSName:     rec.PrivateDNSName,
		SourceDestCheck:    &sdc,
		Groups:             p.ec2GroupItems(reqCtx, rec.SecurityGroupIDs),
		InterfaceType:      rec.InterfaceType,
	}
	if item.InterfaceType == "" {
		item.InterfaceType = "interface"
	}
	if !forInstance {
		item.AvailabilityZone = rec.AvailabilityZone
		item.RequesterManaged = &falseVal
		item.Tags = ec2TagSet(rec.Tags)
	}
	if a := rec.Attachment; a != nil {
		att := &ec2NetworkAttachmentItem{
			AttachmentID:        a.AttachmentID,
			AttachTime:          a.AttachTime,
			DeviceIndex:         a.DeviceIndex,
			Status:              ec2AttachmentStatus(a),
			DeleteOnTermination: a.DeleteOnTermination,
			NetworkCardIndex:    a.NetworkCardIndex,
		}
		if !forInstance {
			att.InstanceID = a.InstanceID
			att.InstanceOwnerID = rec.AccountID
		}
		item.Attachment = att
	}
	if rec.PrivateIPAddress != "" {
		item.PrivateIPAddresses = append(item.PrivateIPAddresses, ec2NetworkPrivateIPItem{
			PrivateIPAddress: rec.PrivateIPAddress,
			PrivateDNSName:   rec.PrivateDNSName,
			Primary:          true,
		})
	}
	for _, addr := range rec.SecondaryPrivateIPAddresses {
		item.PrivateIPAddresses = append(item.PrivateIPAddresses, ec2NetworkPrivateIPItem{
			PrivateIPAddress: addr,
			PrivateDNSName:   ec2PrivateDNSName(addr, rec.Region),
		})
	}
	return item
}

// ec2NetworkInterfaceItems renders an instance's networkInterfaceSet from its interfaces.
func (p *EC2Plugin) ec2NetworkInterfaceItems(reqCtx *RequestContext, recs []EC2NetworkInterfaceRecord) []ec2NetworkInterfaceItem {
	if len(recs) == 0 {
		return nil
	}
	items := make([]ec2NetworkInterfaceItem, 0, len(recs))
	for _, rec := range recs {
		items = append(items, p.ec2NetworkInterfaceItemFor(reqCtx, rec, true))
	}
	return items
}

// settleInstanceInterfaces settles a terminated instance's interfaces, as the EC2 user guide's
// "Network interface attributes" describes the DeleteOnTermination attribute: an attached
// interface whose attachment says so is deleted with the instance and its addresses released,
// and any other is detached and left available, keeping its addresses. An instance an older
// build recorded has its interfaces stored first ([EC2Plugin.materializeLegacyInstance]), so they
// settle by the same rules.
//
// Its caller runs it once per instance, on the transition to terminated.
func (p *EC2Plugin) settleInstanceInterfaces(reqCtx *RequestContext, inst EC2Instance) error {
	ctx := context.Background()
	txn := p.beginAddrTxn(reqCtx)
	defer txn.close()
	if !inst.InterfacesStandalone {
		materialized, err := p.ec2LegacyMaterialized(ctx, reqCtx.AccountID, reqCtx.Region)
		if err != nil {
			return err
		}
		if !materialized[inst.InstanceID] {
			if err := p.materializeLegacyInstance(reqCtx, txn, inst); err != nil {
				return err
			}
		}
	}
	all, err := p.ec2NetworkInterfaces(ctx, reqCtx.AccountID, reqCtx.Region)
	if err != nil {
		return err
	}
	for _, rec := range all {
		if !rec.attached() || rec.Attachment.InstanceID != inst.InstanceID {
			continue
		}
		if rec.Attachment.DeleteOnTermination {
			if err := txn.release(rec.SubnetID, rec.addresses(), rec.NetworkInterfaceID); err != nil {
				return err
			}
			if err := p.state.Delete(ctx, ec2Namespace, ec2ENIKey(rec.AccountID, rec.Region, rec.NetworkInterfaceID)); err != nil {
				return fmt.Errorf("ec2 terminate delete network interface: %w", err)
			}
			continue
		}
		rec.detach()
		if err := p.putNetworkInterface(ctx, rec); err != nil {
			return fmt.Errorf("ec2 terminate detach network interface: %w", err)
		}
	}
	return txn.commit()
}
