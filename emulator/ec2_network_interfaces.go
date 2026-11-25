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
	ifc.DeleteOnTerminationDefaulted = true
	if v, ok := params[p+"DeleteOnTermination"]; ok && v != "" {
		ifc.DeleteOnTermination = strings.EqualFold(v, "true")
		ifc.DeleteOnTerminationDefaulted = false
	}

	present := hasDeviceIndex || ifc.NetworkInterfaceID != "" || ifc.SubnetID != "" ||
		ifc.PrivateIPAddress != "" || len(groups) > 0 ||
		ifc.AssociatePublicIPAddress != "" || ifc.Description != "" ||
		ifc.InterfaceType != "" || params[p+"NetworkCardIndex"] != "" ||
		params[p+"DeleteOnTermination"] != ""
	return ifc, present
}

// ec2IsPrimaryPosition reports whether an interface at network card card and device index device
// is an instance's primary interface. AWS fixes the position: "For the primary network interface,
// specify NetworkCardIndex=0, DeviceIndex=0" (EC2 User Guide, "Maximize network bandwidth on
// Amazon EC2 instances with multiple network cards"), and "The primary network interface must be
// assigned to network card index 0" (AttachNetworkInterface). Every primary-interface decision —
// a launch's top-level networking, DetachNetworkInterface's refusal, an instance's groupSet —
// reads this one rule.
func ec2IsPrimaryPosition(card, device int) bool {
	return card == 0 && device == 0
}

// ec2PrimaryInterface reports the interface an instance's top-level networking describes: the
// declared one at device index 0 on network card 0, or nil when none is. A launch declaring
// interfaces but no primary is refused ([ec2NoPrimaryInterfaceError]).
func ec2PrimaryInterface(interfaces []EC2NetworkInterface) *EC2NetworkInterface {
	for i := range interfaces {
		if ec2IsPrimaryPosition(interfaces[i].NetworkCardIndex, interfaces[i].DeviceIndex) {
			return &interfaces[i]
		}
	}
	return nil
}

// ec2SortInterfacesByDeviceIndex orders declared interfaces by device index alone, keeping the
// request's order among interfaces at one device index on different network cards.
//
// This is the order a launch stores a template's interfaces in and draws the IDs of the declared
// interfaces it left unnamed in, so it is the order event logs recorded those IDs under and does
// not change: sorting by network card first would hand interfaces on two cards each other's IDs
// on replay. Presentation and the primary rule read network card first instead; see
// [ec2IsPrimaryPosition] and [EC2Plugin.planLaunchInterfaces].
//
// An insertion sort rather than sort.Slice, so the order among equal device indices is the
// request's.
func ec2SortInterfacesByDeviceIndex(interfaces []EC2NetworkInterface) {
	for i := 1; i < len(interfaces); i++ {
		for j := i; j > 0 && interfaces[j].DeviceIndex < interfaces[j-1].DeviceIndex; j-- {
			interfaces[j], interfaces[j-1] = interfaces[j-1], interfaces[j]
		}
	}
}
