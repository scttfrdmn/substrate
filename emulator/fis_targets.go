package emulator

// Experiment target resolution (#1619).
//
// "AWS FIS identifies all targets at the start of the experiment, before starting any of the
// actions in the actions set. AWS FIS uses the target resources that it selects for the entire
// experiment" (Targets page). StartExperiment resolves every target once and stores the result
// on the experiment, where ListExperimentResolvedTargets reads it.
//
// An EC2 target (aws:ec2:instance, aws:ec2:spot-instance) resolves against the account's and
// Region's instances: those its resourceArns name, or those carrying every one of its
// resourceTags ("all specified tags have to be present on the target resource for it to be
// selected (AND)"), narrowed by its filters (every filter must match, any of a filter's values
// matches it). A Spot target takes only instances whose instanceLifecycle is spot. A terminated
// or shutting-down instance is not a candidate unless a State.Name filter names its state: the
// Targets page notes only that DescribeInstances "might" report recently terminated instances,
// and no EC2 action can act on one, so a COUNT or PERCENT selection taking the lowest IDs would
// otherwise pick dead instances. That default is substrate's reading. An instance's state, for
// this rule and for a State.Name or State.Code filter, is the one DescribeInstances would report
// now: shutting-down while a seeded termination countdown runs (see
// ec2_instance_state_control.go), read without spending an observation. The candidates
// are sorted by instance ID and the selection mode takes the first of them — substrate's
// deterministic reading of AWS's random choice; see [fisSelection.pick].
//
// A filter path is matched against the attributes DescribeInstances reports for the instance
// (fisInstanceAttribute lists them). A path substrate does not evaluate matches nothing, so a
// target filtered by one resolves empty rather than silently selecting every instance.
//
// A target of any other resource type resolves to its resourceArns as given; substrate does not
// look IAM roles or Auto Scaling groups up by tag, so such a target with only resourceTags
// resolves empty.

import (
	"slices"
	"sort"
	"strconv"
	"strings"
)

// fisIsEC2Target reports whether a resource type resolves against EC2 instances.
func fisIsEC2Target(resourceType string) bool {
	return resourceType == fisResourceEC2Instance || resourceType == fisResourceSpotInstance
}

// fisInstanceARN is an instance's ARN.
func fisInstanceARN(region, accountID, id string) string {
	return "arn:aws:ec2:" + region + ":" + accountID + ":instance/" + id
}

// fisInstanceIDFromARN returns the instance ID an EC2 instance ARN in accountID and region
// names, and false for any other ARN.
func fisInstanceIDFromARN(arn, accountID, region string) (string, bool) {
	prefix := "arn:aws:ec2:" + region + ":" + accountID + ":instance/"
	id, ok := strings.CutPrefix(arn, prefix)
	return id, ok && id != ""
}

// fisInstanceAttribute returns the values an instance carries at a filter path, nil for a path
// substrate does not evaluate. Paths are DescribeInstances' members in Pascal case, as the
// Targets page requires ("you should use AvailabilityZone, not availablityZone").
func fisInstanceAttribute(inst EC2Instance, path string) []string {
	one := func(v string) []string {
		if v == "" {
			return nil
		}
		return []string{v}
	}
	switch path {
	case "State.Name":
		return one(inst.State.Name)
	case "State.Code":
		return one(strconv.Itoa(inst.State.Code))
	case "InstanceId":
		return one(inst.InstanceID)
	case "InstanceType":
		return one(inst.InstanceType)
	case "ImageId":
		return one(inst.ImageID)
	case "SubnetId":
		return one(inst.SubnetID)
	case "VpcId":
		return one(inst.VPCID)
	case "KeyName":
		return one(inst.KeyName)
	case "PrivateIpAddress":
		return one(inst.PrivateIPAddress)
	case "PublicIpAddress":
		return one(inst.PublicIPAddress)
	case "PrivateDnsName":
		return one(inst.PrivateDNSName)
	case "Architecture":
		return one(ec2InstanceArchitecture(inst))
	case "InstanceLifecycle":
		return one(inst.InstanceLifecycle)
	case "SpotInstanceRequestId":
		return one(inst.SpotInstanceRequestID)
	case "Placement.AvailabilityZone":
		return one(inst.AvailabilityZone)
	case "Placement.GroupName":
		return one(inst.PlacementGroupName)
	case "SecurityGroups.GroupId", "NetworkInterfaces.Groups.GroupId":
		return inst.SecurityGroupIDs
	}
	return nil
}

// fisEndedInstance reports whether an instance is terminated or shutting down, which no EC2
// action can act on.
func fisEndedInstance(inst EC2Instance) bool {
	return inst.State.Name == ec2StateTerminated || inst.State.Name == "shutting-down"
}

// fisFilterNamesState reports whether a State.Name filter names state explicitly, which is the
// only way an ended instance is selected.
func fisFilterNamesState(filters []FISTargetFilter, state string) bool {
	for _, f := range filters {
		if f.Path == "State.Name" && slices.Contains(f.Values, state) {
			return true
		}
	}
	return false
}

// fisInstanceMatches reports whether an instance carries every tag and matches every filter.
func fisInstanceMatches(inst EC2Instance, tags map[string]string, filters []FISTargetFilter) bool {
	for k, v := range tags {
		found := false
		for _, t := range inst.Tags {
			if t.Key == k && t.Value == v {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	for _, f := range filters {
		have := fisInstanceAttribute(inst, f.Path)
		if !slices.ContainsFunc(f.Values, func(v string) bool { return slices.Contains(have, v) }) {
			return false
		}
	}
	return true
}

// resolveTargets resolves every target of a template in the caller's account and Region, by
// target name: instance IDs for an EC2 target, ARNs for any other, in selection order.
func (p *FISPlugin) resolveTargets(ctx *RequestContext, targets map[string]FISTarget) (map[string][]string, error) {
	out := make(map[string][]string, len(targets))
	var instances []EC2Instance
	loaded := false
	for _, name := range fisSortedKeys(targets) {
		tg := targets[name]
		sel, _ := parseFISSelectionMode(tg.SelectionMode)
		if !fisIsEC2Target(tg.ResourceType) {
			arns := slices.Clone(tg.ResourceArns)
			sort.Strings(arns)
			out[name] = sel.pick(arns)
			continue
		}
		if !loaded {
			ec2, err := p.ec2()
			if err != nil {
				return nil, err
			}
			if instances, err = ec2.instancesInRegion(ctx.AccountID, ctx.Region); err != nil {
				return nil, err
			}
			// Each instance is matched in the state a describe would report now — shutting-down
			// while a seeded countdown runs — read without spending an observation.
			for i := range instances {
				instances[i].State = ec2.peekInstanceState(instances[i])
			}
			loaded = true
		}
		var named map[string]bool
		if len(tg.ResourceArns) > 0 {
			named = map[string]bool{}
			for _, arn := range tg.ResourceArns {
				if id, ok := fisInstanceIDFromARN(arn, ctx.AccountID, ctx.Region); ok {
					named[id] = true
				}
			}
		}
		ids := []string{}
		for _, inst := range instances {
			if named != nil && !named[inst.InstanceID] {
				continue
			}
			if tg.ResourceType == fisResourceSpotInstance && inst.InstanceLifecycle != ec2InstanceLifecycleSpot {
				continue
			}
			if fisEndedInstance(inst) && !fisFilterNamesState(tg.Filters, inst.State.Name) {
				continue
			}
			if !fisInstanceMatches(inst, tg.ResourceTags, tg.Filters) {
				continue
			}
			ids = append(ids, inst.InstanceID)
		}
		out[name] = sel.pick(ids)
	}
	return out, nil
}

// resolvedARN renders one resolved resource of a target as its ARN.
func (e *FISExperiment) resolvedARN(target, resource string) string {
	if fisIsEC2Target(e.Targets[target].ResourceType) {
		return fisInstanceARN(e.Region, e.AccountID, resource)
	}
	return resource
}
