package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
)

// A launch's network interfaces (#1522, #1592).
//
// RunInstances gives every instance a primary interface — the one at device index 0 on network
// card 0 ([ec2PrimaryInterface]) — whether or not the request declared one, plus one interface per
// further NetworkInterface.N it declared. Each interface the launch creates gets an address from
// its subnet through the allocator (ec2_ip_alloc.go); one the request names by
// NetworkInterfaceId is attached as it is and keeps the address it has.
//
// The work is split in three, around the launch loop. Every address is allocated before the
// first instance is written, so a launch whose second instance cannot have an address (the address
// it names is taken, the subnet is full) writes nothing. Inside the loop, an interface ID is drawn
// from the request's mint for each declared interface the request left unnamed. After the loop, a
// primary interface ID for an instance that declared no interface, and an attachment ID for every
// interface, are drawn from the request's "eni" side stream ([IDMint.Stream]), so they do not
// advance the ordinal the instance, reservation and volume IDs are keyed on — not even across the
// several launches one CreateFleet, Auto Scaling reconcile or TerminateInstances makes.

// ec2InterfaceStream names the side stream a launch draws its primary-interface and attachment IDs
// from.
const ec2InterfaceStream = "eni"

// ec2PlannedInterface is one interface a launch will attach to one instance.
type ec2PlannedInterface struct {
	// rec is the interface's record as it will be stored, less the IDs minted later.
	rec EC2NetworkInterfaceRecord

	// existing reports an interface the request named by NetworkInterfaceId, which is attached
	// rather than created.
	existing bool

	// declared reports an interface from a NetworkInterface.N specification, whose ID — if the
	// request named none — is drawn inside the launch loop from the request's own mint.
	declared bool

	// order is the interface's position among the declared interfaces as
	// [ec2SortInterfacesByDeviceIndex] orders them, which is the order their IDs are drawn in.
	order int
}

// ec2LaunchNetwork is what a launch decided about networking before its loop.
type ec2LaunchNetwork struct {
	subnet     EC2Subnet
	groups     []string
	declared   []EC2NetworkInterface
	primaryIP  string
	launchTime string
}

// ec2AttachZoneMismatchError refuses an interface and an instance in different Availability
// Zones, with EC2's observed AttachNetworkInterface answer, InvalidParameterCombination "You may
// not attach a network interface to an instance if they are not in the same availability zone"
// (Stack Overflow 75854119, from the Terraform AWS provider; DevelopersIO's "複数のVPCにEC2
// インスタンスを接続する", from the CLI). A launch whose interfaces span zones answers the same.
// A different VPC is not refused: an instance may carry interfaces in several VPCs of its zone.
func ec2AttachZoneMismatchError() *AWSError {
	return &AWSError{
		Code:       "InvalidParameterCombination",
		Message:    "You may not attach a network interface to an instance if they are not in the same availability zone",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2NoPrimaryInterfaceError refuses a launch whose declared interfaces include none at device
// index 0 on network card 0, which AWS documents as where the primary interface must be ("For the
// primary network interface, specify NetworkCardIndex=0, DeviceIndex=0", EC2 User Guide,
// "Maximize network bandwidth on Amazon EC2 instances with multiple network cards"). No page
// publishes the refusal, so the message is substrate's own.
func ec2NoPrimaryInterfaceError() *AWSError {
	return ec2InvalidParameterValue("NetworkInterface", "",
		"The primary network interface must have device index 0 on network card index 0.")
}

// ec2DuplicateDeviceIndexError refuses two declared interfaces at one device index on one network
// card. The message is substrate's own.
func ec2DuplicateDeviceIndexError(card, device int) *AWSError {
	return ec2InvalidParameterValue("NetworkInterface", "",
		fmt.Sprintf("More than one network interface is declared at device index %d on network card index %d.", device, card))
}

// ec2CheckDeclaredInterfaces refuses a launch's declared interfaces that name no primary, or name
// one position twice. A launch that declares none is given a primary.
func ec2CheckDeclaredInterfaces(declared []EC2NetworkInterface) *AWSError {
	if len(declared) == 0 {
		return nil
	}
	if ec2PrimaryInterface(declared) == nil {
		return ec2NoPrimaryInterfaceError()
	}
	seen := map[[2]int]bool{}
	for _, ifc := range declared {
		at := [2]int{ifc.NetworkCardIndex, ifc.DeviceIndex}
		if seen[at] {
			return ec2DuplicateDeviceIndexError(at[0], at[1])
		}
		seen[at] = true
	}
	return nil
}

// ec2PrimaryFromExisting resolves the subnet and groups of a launch whose primary interface is
// one the request names by NetworkInterfaceId and which named no subnet: the interface's own. The
// interface must exist, as InvalidNetworkInterfaceID.NotFound otherwise.
func (p *EC2Plugin) ec2PrimaryFromExisting(reqCtx *RequestContext, primary *EC2NetworkInterface) (subnetID string, groups []string, err error) {
	if primary == nil || primary.NetworkInterfaceID == "" {
		return "", nil, nil
	}
	rec, found, err := p.requireInterfaceRecord(reqCtx, primary.NetworkInterfaceID)
	if err != nil {
		return "", nil, err
	}
	if !found {
		return "", nil, ec2NetworkInterfaceIDKind.notFoundError(primary.NetworkInterfaceID)
	}
	return rec.SubnetID, rec.SecurityGroupIDs, nil
}

// planLaunchInterfaces allocates every instance's interfaces in txn, returning them per
// instance, primary first. Nothing is written; a refusal leaves txn uncommitted.
func (p *EC2Plugin) planLaunchInterfaces(
	reqCtx *RequestContext,
	txn *ec2AddrTxn,
	net ec2LaunchNetwork,
	count int,
) ([][]ec2PlannedInterface, error) {
	ctx := context.Background()
	if awsErr := ec2CheckDeclaredInterfaces(net.declared); awsErr != nil {
		return nil, awsErr
	}
	subnets := map[string]EC2Subnet{net.subnet.SubnetID: net.subnet}
	subnetFor := func(id string) (EC2Subnet, error) {
		if s, ok := subnets[id]; ok {
			return s, nil
		}
		data, err := p.state.Get(ctx, ec2Namespace, "subnet:"+reqCtx.AccountID+"/"+reqCtx.Region+"/"+id)
		if err != nil {
			return EC2Subnet{}, fmt.Errorf("ec2 runInstances get interface subnet: %w", err)
		}
		if reqErr := ec2RequireResource(ec2SubnetIDKind, id, data != nil); reqErr != nil {
			return EC2Subnet{}, reqErr
		}
		var s EC2Subnet
		if err := json.Unmarshal(data, &s); err != nil {
			return EC2Subnet{}, fmt.Errorf("ec2 runInstances unmarshal interface subnet: %w", err)
		}
		subnets[id] = s
		return s, nil
	}
	// Every interface of an instance is in the primary's zone, which is the instance's.
	sameZone := func(az string) error {
		if az != "" && net.subnet.AvailabilityZone != "" && az != net.subnet.AvailabilityZone {
			return ec2AttachZoneMismatchError()
		}
		return nil
	}

	declared := net.declared
	synthetic := len(declared) == 0
	if synthetic {
		declared = []EC2NetworkInterface{{DeleteOnTermination: true}}
	}
	var claimed []string
	plans := make([][]ec2PlannedInterface, 0, count)
	for range count {
		var plan []ec2PlannedInterface
		for order, ifc := range declared {
			isPrimary := ec2IsPrimaryPosition(ifc.NetworkCardIndex, ifc.DeviceIndex)
			attachment := &EC2NetworkInterfaceAttachment{
				DeviceIndex: ifc.DeviceIndex, NetworkCardIndex: ifc.NetworkCardIndex,
				DeleteOnTermination: ifc.DeleteOnTermination, AttachTime: net.launchTime,
			}
			if ifc.NetworkInterfaceID != "" {
				rec, found, err := p.requireInterfaceRecord(reqCtx, ifc.NetworkInterfaceID)
				if err != nil {
					return nil, err
				}
				if reqErr := ec2RequireResource(ec2NetworkInterfaceIDKind, ifc.NetworkInterfaceID, found); reqErr != nil {
					return nil, reqErr
				}
				if rec.Status != "available" || rec.attached() || slices.Contains(claimed, rec.NetworkInterfaceID) {
					return nil, ec2NetworkInterfaceInUseError(rec.NetworkInterfaceID)
				}
				if err := sameZone(rec.AvailabilityZone); err != nil {
					return nil, err
				}
				claimed = append(claimed, rec.NetworkInterfaceID)
				rec.Status = "in-use"
				rec.Attachment = attachment
				plan = append(plan, ec2PlannedInterface{rec: rec, existing: true, declared: true, order: order})
				continue
			}
			subnet := net.subnet
			groups := filterEmpty(ifc.SecurityGroupIDs)
			explicit := ifc.PrivateIPAddress
			if isPrimary {
				// The launch's resolved groups — the request's, its primary interface's, or
				// its template's, in that precedence — are the primary interface's, so the
				// instance's groupSet and its primary interface agree.
				if len(net.groups) > 0 {
					groups = net.groups
				}
				if explicit == "" {
					explicit = net.primaryIP
				}
			} else if ifc.SubnetID != "" {
				s, err := subnetFor(ifc.SubnetID)
				if err != nil {
					return nil, err
				}
				if err := sameZone(s.AvailabilityZone); err != nil {
					return nil, err
				}
				subnet = s
			}
			if !isPrimary {
				// A secondary interface's groups are checked against its own subnet's VPC, and one
				// naming none takes that VPC's default group, as CreateNetworkInterface does.
				if len(groups) == 0 {
					if sg := ec2LookupDefaultSecurityGroup(ctx, p.state, reqCtx, subnet.VPCID); sg != "" {
						groups = []string{sg}
					}
				} else if err := p.ec2CheckSecurityGroups(reqCtx, groups, subnet.VPCID); err != nil {
					return nil, err
				}
			}
			interfaceType := ifc.InterfaceType
			if interfaceType == "" {
				interfaceType = "interface"
			}
			rec := EC2NetworkInterfaceRecord{
				NetworkInterfaceID: ifc.NetworkInterfaceID,
				SubnetID:           subnet.SubnetID,
				VPCID:              subnet.VPCID,
				AvailabilityZone:   subnet.AvailabilityZone,
				Description:        ifc.Description,
				InterfaceType:      interfaceType,
				SecurityGroupIDs:   groups,
				Status:             "in-use",
				Attachment:         attachment,
				AccountID:          reqCtx.AccountID,
				Region:             reqCtx.Region,
			}
			// An efa-only interface "do[es] not support IP addresses", so it is given none.
			if interfaceType != "efa-only" {
				addr, err := txn.allocate(subnet, explicit, "")
				if err != nil {
					return nil, err
				}
				rec.PrivateIPAddress = addr
				rec.PrivateDNSName = ec2PrivateDNSName(addr, reqCtx.Region)
			}
			plan = append(plan, ec2PlannedInterface{declared: !synthetic, rec: rec, order: order})
		}
		// Primary first, then by network card and device index, as AWS lists an instance's
		// interfaces.
		slices.SortStableFunc(plan, func(a, b ec2PlannedInterface) int {
			if d := a.rec.Attachment.NetworkCardIndex - b.rec.Attachment.NetworkCardIndex; d != 0 {
				return d
			}
			return a.rec.Attachment.DeviceIndex - b.rec.Attachment.DeviceIndex
		})
		plans = append(plans, plan)
	}
	return plans, nil
}

// ec2ApplyPlanToInstance sets inst's top-level networking from its planned primary interface and
// draws, inside the launch loop, the interface IDs a declared specification left unnamed, from
// the request's own mint in the declared order ([ec2PlannedInterface.order]) rather than the
// network-card-first order plan is in.
func ec2ApplyPlanToInstance(m *IDMint, inst *EC2Instance, plan []ec2PlannedInterface) {
	byOrder := make([]int, len(plan))
	for i := range plan {
		byOrder[i] = i
	}
	slices.SortStableFunc(byOrder, func(a, b int) int { return plan[a].order - plan[b].order })
	for _, i := range byOrder {
		if plan[i].declared && !plan[i].existing && plan[i].rec.NetworkInterfaceID == "" {
			plan[i].rec.NetworkInterfaceID = generateENIID(m)
		}
	}
	inst.InterfacesStandalone = true
	inst.NetworkInterfaces = nil
	if len(plan) == 0 {
		return
	}
	primary := plan[0].rec
	inst.PrivateIPAddress = primary.PrivateIPAddress
	inst.PrivateDNSName = primary.PrivateDNSName
	if primary.SubnetID != "" {
		inst.SubnetID = primary.SubnetID
	}
	if len(inst.SecurityGroupIDs) == 0 {
		inst.SecurityGroupIDs = primary.SecurityGroupIDs
	}
}

// finishLaunchInterfaces draws, from the request's "eni" side stream, each instance's primary
// interface ID when it declared no interface and then every attachment ID, instance by instance;
// writes the interfaces; names each address's holder; and commits the allocations.
func (p *EC2Plugin) finishLaunchInterfaces(
	reqCtx *RequestContext,
	txn *ec2AddrTxn,
	instances []EC2Instance,
	plans [][]ec2PlannedInterface,
	tags []EC2Tag,
) error {
	ctx := context.Background()
	m := reqCtx.IDs.Stream(ec2InterfaceStream)
	for i, inst := range instances {
		for n := range plans[i] {
			planned := &plans[i][n]
			if planned.rec.NetworkInterfaceID == "" {
				planned.rec.NetworkInterfaceID = generateENIID(m)
			}
			planned.rec.Attachment.AttachmentID = generateENIAttachmentID(m)
			planned.rec.Attachment.InstanceID = inst.InstanceID
			if !planned.existing {
				planned.rec.Tags = tags
				for _, addr := range planned.rec.addresses() {
					txn.setHolder(planned.rec.SubnetID, addr, planned.rec.NetworkInterfaceID)
				}
			}
			if err := p.putNetworkInterface(ctx, planned.rec); err != nil {
				return fmt.Errorf("ec2 runInstances network interface: %w", err)
			}
		}
	}
	return txn.commit()
}
