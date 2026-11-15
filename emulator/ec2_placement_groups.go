package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// ec2UnknownPlacementGroupError is InvalidPlacementGroup.Unknown for a group named by name or by
// ID. The code is the client-error table's ("The specified placement group cannot be found");
// the message is substrate's own, kept from #344.
func ec2UnknownPlacementGroupError(ref string) *AWSError {
	return &AWSError{
		Code:       "InvalidPlacementGroup.Unknown",
		Message:    "The placement group '" + ref + "' is unknown.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// resolveLaunchPlacementGroup returns the placement group a launch names, or nil when it names
// none (#1193).
//
// API_Placement publishes both GroupName and GroupId — "On input, you can specify GroupId or
// GroupName, but not both." Through v0.128.0 only the name was read, its existence checked and
// then dropped, so DescribeInstances could not report the group and DeletePlacementGroup could
// not tell it held instances. The group is now resolved by either form and returned, so the
// launch stores it on each instance.
//
// Sending both is InvalidParameterCombination: RunInstances' Errors section is the Common Error
// Types boilerplate, and that list's gloss for the code is "an incorrect combination of
// parameters". The message is the page's own sentence; the choice of code is substrate's
// reading. A group that resolves to nothing is InvalidPlacementGroup.Unknown, and a state read
// that fails is an error rather than a missing group, which is what the discarded read before
// #1193 reported it as.
func (p *EC2Plugin) resolveLaunchPlacementGroup(reqCtx *RequestContext, params map[string]string) (*EC2PlacementGroup, error) {
	name, id := params["Placement.GroupName"], params["Placement.GroupId"]
	switch {
	case name == "" && id == "":
		return nil, nil
	case name != "" && id != "":
		return nil, &AWSError{
			Code:       "InvalidParameterCombination",
			Message:    "On input, you can specify GroupId or GroupName, but not both.",
			HTTPStatus: http.StatusBadRequest,
		}
	case name == "":
		resolved, found := ec2NameKeyedResource(p.state,
			ec2StatePrefix("placement_group", reqCtx.AccountID, reqCtx.Region), "group_id", id)
		if !found {
			return nil, ec2UnknownPlacementGroupError(id)
		}
		name = resolved
	}
	data, err := p.state.Get(context.Background(), ec2Namespace,
		ec2PlacementGroupStateKey(reqCtx.AccountID, reqCtx.Region, name))
	if err != nil {
		return nil, fmt.Errorf("ec2 runInstances get placement group: %w", err)
	}
	if data == nil {
		return nil, ec2UnknownPlacementGroupError(name)
	}
	var pg EC2PlacementGroup
	if err := json.Unmarshal(data, &pg); err != nil {
		return nil, fmt.Errorf("ec2 runInstances unmarshal placement group: %w", err)
	}
	return &pg, nil
}

// placementGroupInUse reports whether a non-terminated instance was launched into pg (#1193).
//
// API_DeletePlacementGroup: "You must terminate all instances in the placement group before you
// can delete the placement group." An instance matches by the group's ID when both carry one
// and by name otherwise. An instance launched before #1193 recorded no group and cannot hold
// one.
func (p *EC2Plugin) placementGroupInUse(reqCtx *RequestContext, pg EC2PlacementGroup) (bool, error) {
	used := false
	err := ec2ScanRecords(context.Background(), p.state, "instance:"+reqCtx.AccountID+"/"+reqCtx.Region+"/",
		func(_ string, inst EC2Instance) {
			if inst.State.Name == "terminated" || inst.PlacementGroupName == "" {
				return
			}
			if inst.PlacementGroupID != "" && pg.GroupID != "" {
				used = used || inst.PlacementGroupID == pg.GroupID
				return
			}
			used = used || inst.PlacementGroupName == pg.GroupName
		})
	if err != nil {
		return false, fmt.Errorf("ec2 deletePlacementGroup instances: %w", err)
	}
	return used, nil
}

// ec2PlacementGroupInUseError is InvalidPlacementGroup.InUse, with the client-error table's gloss
// as its message, verbatim. API_DeletePlacementGroup publishes no operation-specific error, and
// this is the table's code for exactly the refusal it describes.
func ec2PlacementGroupInUseError() *AWSError {
	return &AWSError{
		Code: "InvalidPlacementGroup.InUse",
		Message: "The specified placement group is in use. If you are trying to delete a placement " +
			"group, ensure that its instances have been terminated.",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2InstanceArchitecture returns the architecture DescribeInstances reports for inst.
//
// API_Instance glosses architecture as "The architecture of the image", so a launch stores the
// AMI's (#1525). An instance stored before #1525 carries none, and neither does one launched
// from an image that never stated one; for those the instance type's catalog entry decides —
// arm64 when the type supports it, x86_64 when it supports that — which is the only architecture
// such a launch could have run, since [ec2CheckLaunchArchitecture] refuses a mismatch. A type the
// catalog does not carry reports nothing, and the element is omitted.
func ec2InstanceArchitecture(inst EC2Instance) string {
	if inst.Architecture != "" {
		return inst.Architecture
	}
	return ec2InstanceTypeArchitecture(inst.InstanceType)
}

// ec2InstanceTypeArchitecture returns the one launchable architecture the catalog gives an
// instance type, or "" when the catalog does not carry it.
func ec2InstanceTypeArchitecture(instanceType string) string {
	info, ok := ec2InstanceTypeIndex[instanceType]
	if !ok {
		return ""
	}
	switch {
	case containsStr(info.SupportedArchs, ec2ArchARM64):
		return ec2ArchARM64
	case containsStr(info.SupportedArchs, ec2ArchX86):
		return ec2ArchX86
	}
	return ""
}
