package emulator

import (
	"fmt"
	"strconv"
	"strings"
)

// ec2ParseNetworkInterfaces collects every NetworkInterface.N specification under
// prefix, contiguously from N=1 and stopping at the first index that names nothing.
//
// prefix is "" for a RunInstances request and "LaunchTemplateData." for a template's,
// so both paths parse the same shape by the same rules rather than by two hand-rolled
// readers that can drift — which is how index 1 came to be the only one either of
// them read (#455).
//
// The contiguous-from-1 rule is [indexedParams]' convention, extended from a string
// per index to a struct per index. An index is "present" when *any* of its members
// is, not when a particular one is: a specification naming only DeviceIndex and
// SubnetId is as real as one naming ten members, and requiring a chosen member would
// silently drop an interface for spelling reasons.
func ec2ParseNetworkInterfaces(params map[string]string, prefix string) []EC2NetworkInterface {
	var out []EC2NetworkInterface
	for n := 1; ; n++ {
		p := fmt.Sprintf("%sNetworkInterface.%d.", prefix, n)
		ifc, present := ec2ParseNetworkInterface(params, p)
		if !present {
			return out
		}
		out = append(out, ifc)
	}
}

// ec2ParseNetworkInterface reads one interface's members from the parameters under
// p, reporting whether the index was present at all.
//
// DeviceIndex is parsed rather than taken from the parameter index. AWS documents it
// as "the position of the network interface in the attachment order", and the two
// indices need not agree — NetworkInterface.1 may declare DeviceIndex 3. Using the
// parameter index would make a launch whose interfaces are listed out of order
// report the wrong primary, and the primary is what an instance's top-level
// subnetId, privateIpAddress and groupSet describe.
//
// An unparseable or absent DeviceIndex reads as 0. AWS documents it as required when
// a network interface is specified, but substrate does not refuse the launch: a
// specification naming a subnet and no device index is unambiguous about the subnet,
// and failing it would reject a hand-built request over a value substrate can infer.
func ec2ParseNetworkInterface(params map[string]string, p string) (EC2NetworkInterface, bool) {
	groups := indexedParams(params, p+"SecurityGroupId.%d", p+"Groups.%d")
	ifc := EC2NetworkInterface{
		NetworkInterfaceID:       params[p+"NetworkInterfaceId"],
		SubnetID:                 params[p+"SubnetId"],
		PrivateIPAddress:         params[p+"PrivateIpAddress"],
		SecurityGroupIDs:         groups,
		AssociatePublicIPAddress: params[p+"AssociatePublicIpAddress"],
		Description:              params[p+"Description"],
		InterfaceType:            params[p+"InterfaceType"],
	}
	deviceIndex, hasDeviceIndex := params[p+"DeviceIndex"]
	if n, err := strconv.Atoi(strings.TrimSpace(deviceIndex)); err == nil {
		ifc.DeviceIndex = n
	}
	if n, err := strconv.Atoi(strings.TrimSpace(params[p+"NetworkCardIndex"])); err == nil {
		ifc.NetworkCardIndex = n
	}
	// AWS defaults DeleteOnTermination to true for an interface the launch creates
	// and false for an existing one it attaches, since deleting an interface the
	// caller brought would destroy something the launch did not make. An explicit
	// value wins over both.
	ifc.DeleteOnTermination = ifc.NetworkInterfaceID == ""
	if v, ok := params[p+"DeleteOnTermination"]; ok && v != "" {
		ifc.DeleteOnTermination = strings.EqualFold(v, "true")
	}

	present := hasDeviceIndex || ifc.NetworkInterfaceID != "" || ifc.SubnetID != "" ||
		ifc.PrivateIPAddress != "" || len(groups) > 0 ||
		ifc.AssociatePublicIPAddress != "" || ifc.Description != "" ||
		ifc.InterfaceType != "" || params[p+"NetworkCardIndex"] != "" ||
		params[p+"DeleteOnTermination"] != ""
	return ifc, present
}

// ec2PrimaryInterface reports the interface an instance's top-level networking
// describes: the one at DeviceIndex 0, or the lowest device index when a launch
// declared no interface at index 0.
//
// The fallback matters because DeviceIndex defaults to 0 here rather than being
// required, so "no interface at 0" means the caller numbered them from 1 — in which
// case the lowest is the one attached first, which is what "primary" means.
func ec2PrimaryInterface(interfaces []EC2NetworkInterface) *EC2NetworkInterface {
	var primary *EC2NetworkInterface
	for i := range interfaces {
		if primary == nil || interfaces[i].DeviceIndex < primary.DeviceIndex {
			primary = &interfaces[i]
		}
	}
	return primary
}

// ec2AttachInterfaces records the launch's interfaces on inst, filling in what the
// request left to EC2: an interface ID, a private address, and a private DNS name.
//
// instanceIndex distinguishes the instances of a multi-count launch, so two instances
// from one RunInstances call do not report the same secondary addresses. AWS refuses a
// launch that names a PrivateIpAddress with a count above one for exactly this
// reason; substrate does not refuse it, so it has to make the addresses differ.
//
// The primary interface takes the instance's own address and DNS name rather than a
// separately generated one: they describe the same interface, and letting them differ
// would mean an instance whose top-level privateIpAddress is not the address of any
// interface it has.
func (p *EC2Plugin) ec2AttachInterfaces(inst *EC2Instance, declared []EC2NetworkInterface, instanceIndex int, region string) {
	if len(declared) == 0 {
		return
	}
	interfaces := make([]EC2NetworkInterface, 0, len(declared))
	primary := ec2PrimaryInterface(declared)
	for n := range declared {
		ifc := declared[n]
		ifc.SecurityGroupIDs = filterEmpty(ifc.SecurityGroupIDs)
		if ifc.NetworkInterfaceID == "" {
			ifc.NetworkInterfaceID = generateENIID()
		}
		if ifc.InterfaceType == "" {
			ifc.InterfaceType = "interface"
		}
		switch {
		case primary != nil && ifc.DeviceIndex == primary.DeviceIndex:
			ifc.SubnetID = inst.SubnetID
			ifc.PrivateIPAddress = inst.PrivateIPAddress
			ifc.PrivateDNSName = inst.PrivateDNSName
			if len(ifc.SecurityGroupIDs) == 0 {
				ifc.SecurityGroupIDs = inst.SecurityGroupIDs
			}
		case ifc.PrivateIPAddress == "":
			// A secondary interface's address comes from the same 172.31.0.0/16 space
			// the instance's does, offset by its device index so the addresses within
			// one instance are distinct and stable across a replay.
			ifc.PrivateIPAddress = fmt.Sprintf("172.31.%d.%d",
				instanceIndex+1, 10+instanceIndex+ifc.DeviceIndex*10)
		}
		if ifc.PrivateDNSName == "" && ifc.PrivateIPAddress != "" {
			ifc.PrivateDNSName = ec2PrivateDNSName(ifc.PrivateIPAddress, region)
		}
		interfaces = append(interfaces, ifc)
	}
	inst.NetworkInterfaces = interfaces
}

// ec2SortInterfacesByDeviceIndex orders interfaces by device index, which is the
// order AWS reports them in and the order that puts the primary first.
//
// An insertion sort rather than sort.Slice: the list is at most a handful of entries,
// and this keeps two interfaces declaring the same device index in the order the
// request listed them. AWS rejects that duplicate; substrate does not, and a stable
// order means a caller reading such a launch back sees something reproducible rather
// than whichever order the sort happened to produce.
func ec2SortInterfacesByDeviceIndex(interfaces []EC2NetworkInterface) {
	for i := 1; i < len(interfaces); i++ {
		for j := i; j > 0 && interfaces[j].DeviceIndex < interfaces[j-1].DeviceIndex; j-- {
			interfaces[j], interfaces[j-1] = interfaces[j-1], interfaces[j]
		}
	}
}
