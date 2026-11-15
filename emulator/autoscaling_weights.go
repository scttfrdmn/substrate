package emulator

import (
	"strconv"
)

// Capacity units and instance weights (#1502).
//
// A group's MinSize, MaxSize and DesiredCapacity are in its capacity units, and each member
// contributes its weight in those units. API_CreateAutoScalingGroup: "if you configure a mixed
// instances policy that defines weights for the instance types, you must specify these sizes
// with the same units that you use for weighting instances." There are two sources of weight:
//
//   - **DesiredCapacityType** `vcpu` or `memory-mib`. "Setting the DesiredCapacityType parameter
//     automatically specifies the number of units to associate with each instance type based on
//     the value that you set for this parameter" (instance-weighting guide), so a member weighs
//     its type's vCPUs or MiB, read from [ec2InstanceTypeIndex] — the catalog
//     DescribeInstanceTypes answers from. API_CreateAutoScalingGroup: Auto Scaling "supports
//     DesiredCapacityType for attribute-based instance type selection only", which
//     [asgCheckCapacityType] enforces.
//   - **WeightedCapacity** on the overrides, with DesiredCapacityType `units`: a member weighs
//     what the override it launched from declares. An InstanceRequirements override's weight
//     applies to every type it matches, and an override naming no type — its type is the
//     template's — weighs what it declares too, carried on the launch candidate.
//
// Otherwise every member weighs one, which is the instance count.
//
// The documented behaviors follow from that, and are what reconcile implements:
//
//   - "Current capacity will either be at the desired capacity or above it": a scale-out
//     launches until the summed weight reaches desired, and the last launch may overshoot.
//   - "The weights do not change which instance types Amazon EC2 Auto Scaling chooses to
//     launch; instead, the allocation strategies do that": the candidate order is unchanged
//     (autoscaling_mixed.go), so a group whose first candidate is larger than the remainder
//     overshoots rather than reaching for a smaller type.
//   - "Amazon EC2 Auto Scaling will never go above MaxSize by more than your largest instance
//     weight" (API_CreateAutoScalingGroup): a scale-out stops as soon as capacity reaches
//     desired, which is at most MaxSize, so the overshoot is less than the last launch's weight.
//   - OnDemandBaseCapacity "has the same unit of measurement as the group's desired capacity"
//     (API_InstancesDistribution): the On-Demand/Spot split is computed in units, for the size
//     the group will be after the launch or termination being decided.
//   - A scale-in never takes the group below desired: it terminates only a member whose weight
//     fits in the excess, and stops when none does, leaving the overshoot standing. AWS
//     publishes no finer rule; this one is substrate's, and keeps "at the desired capacity or
//     above it" true after a scale-in too.
//   - "If you remove an instance type, running instances of that type keep their last weight"
//     and "when you add or change weights … launch or terminate instances to reach the desired
//     capacity based on the new weight values": every reconciliation re-reads each member's
//     weight from the current configuration ([AutoScalingGroup.refreshWeights]) and keeps the
//     stored one when the configuration no longer names the type. A member launched from an
//     override naming no type of its own is not named by type, so it keeps the weight it
//     launched with.

// asgCapacityUnits, asgCapacityVCPU and asgCapacityMemoryMiB are the three DesiredCapacityType
// values API_CreateAutoScalingGroup lists.
const (
	asgCapacityUnits     = "units"
	asgCapacityVCPU      = "vcpu"
	asgCapacityMemoryMiB = "memory-mib"
)

// capacityType returns the group's DesiredCapacityType, "units" when none was named — the
// documented default.
func (g *AutoScalingGroup) capacityType() string {
	if g.DesiredCapacityType == "" {
		return asgCapacityUnits
	}
	return g.DesiredCapacityType
}

// weightBasis is the capacity type the group's members are weighted by: its
// DesiredCapacityType when the configuration permits it, units otherwise. Create and Update
// refuse a vcpu or memory-mib group with no InstanceRequirements override (#1502), but a group
// recorded before that refusal existed can still be in state. It keeps counting instances, as
// it always did, rather than being reinterpreted — and scaled in — by its next observation.
func (g *AutoScalingGroup) weightBasis() string {
	if asgCheckCapacityType(g) != nil {
		return asgCapacityUnits
	}
	return g.capacityType()
}

// overridesWeighted reports whether the group's overrides declare WeightedCapacity.
// [asgCheckWeights] has already made that all of them or none.
func (g *AutoScalingGroup) overridesWeighted() bool {
	if m := g.MixedInstancesPolicy; m != nil {
		for _, ov := range m.Overrides {
			if ov.WeightedCapacity != "" {
				return true
			}
		}
	}
	return false
}

// weighted reports whether the group's members carry weights other than one: a vcpu or
// memory-mib group, or a units group whose overrides declare WeightedCapacity. Only a weighted
// group's members report WeightedCapacity.
func (g *AutoScalingGroup) weighted() bool {
	return g.weightBasis() != asgCapacityUnits || g.overridesWeighted()
}

// typeWeight is the weight the group's current configuration gives an instance type, and
// false when the configuration gives it none — a type no override names any more, one an
// override naming no type took from the template (whose weight is the candidate's, see
// [AutoScalingGroup.overrideWeight]), or one the catalog does not hold for a vcpu or
// memory-mib group.
func (g *AutoScalingGroup) typeWeight(instanceType string) (int, bool) {
	switch basis := g.weightBasis(); basis {
	case asgCapacityVCPU, asgCapacityMemoryMiB:
		info, ok := ec2InstanceTypeIndex[instanceType]
		if !ok {
			return 0, false
		}
		if basis == asgCapacityVCPU {
			return info.VCpus, info.VCpus > 0
		}
		return info.MemoryMiB, info.MemoryMiB > 0
	}
	if !g.overridesWeighted() {
		return 1, true
	}
	for _, ov := range g.MixedInstancesPolicy.Overrides {
		w, err := strconv.Atoi(ov.WeightedCapacity)
		if err != nil {
			continue
		}
		if ov.InstanceRequirements == nil {
			if ov.InstanceType != "" && ov.InstanceType == instanceType {
				return w, true
			}
			continue
		}
		if info, ok := ec2InstanceTypeIndex[instanceType]; ok && asgTypeSatisfies(ov.InstanceRequirements, info) {
			return w, true
		}
	}
	return 0, false
}

// overrideWeight is the weight of a launch candidate built from ov — nil for a group with no
// overrides — launching instanceType, empty when the type is the template's. It is the
// catalog's for a vcpu or memory-mib group, and for a units group the override's own
// WeightedCapacity, which is how an override naming no type of its own still weighs what it
// declares. Zero means the weight is the launched type's, known only once it has launched.
func (g *AutoScalingGroup) overrideWeight(ov *AutoScalingOverride, instanceType string) int {
	if g.weightBasis() == asgCapacityUnits {
		if ov != nil && ov.WeightedCapacity != "" {
			if w, err := strconv.Atoi(ov.WeightedCapacity); err == nil {
				return w
			}
		}
		if g.overridesWeighted() {
			return 0
		}
		return 1
	}
	if instanceType == "" {
		return 0
	}
	w, ok := g.typeWeight(instanceType)
	if !ok {
		return 0
	}
	return w
}

// weightOf is the capacity one member contributes. A stored weight of zero reads as one, which
// is every member of an unweighted group and every record written before weights existed.
func (inst *AutoScalingInstance) weightOf() int {
	if inst.Weight <= 0 {
		return 1
	}
	return inst.Weight
}

// setWeight stores a member's weight, zero for one so an unweighted group's record is the
// same as it always was.
func (inst *AutoScalingInstance) setWeight(w int) {
	if w == 1 {
		w = 0
	}
	inst.Weight = w
}

// refreshWeights re-reads each member's weight from the group's current configuration,
// keeping the stored weight for a type the configuration no longer names: "running instances
// of that type keep their last weight, even if no longer defined" (instance-weighting guide).
func (g *AutoScalingGroup) refreshWeights() {
	for i := range g.Instances {
		if w, ok := g.typeWeight(g.Instances[i].InstanceType); ok {
			g.Instances[i].setWeight(w)
		}
	}
}

// capacity is the group's current capacity in its own units: the summed weight of its members.
func (g *AutoScalingGroup) capacity() int {
	return g.tally().capacity
}

// asgTally is a group's capacity broken down the ways a reconciliation reads it — in total,
// On-Demand, and per zone and subnet. A scale-out or scale-in builds it once and keeps it
// current as it launches and terminates, rather than rescanning every member at each step.
type asgTally struct {
	capacity  int
	onDemand  int
	perAZ     map[string]int
	perSubnet map[string]int
}

// tally adds up the group's members.
func (g *AutoScalingGroup) tally() *asgTally {
	t := &asgTally{perAZ: map[string]int{}, perSubnet: map[string]int{}}
	for i := range g.Instances {
		t.add(&g.Instances[i])
	}
	return t
}

// add counts a member in.
func (t *asgTally) add(inst *AutoScalingInstance) { t.adjust(inst, inst.weightOf()) }

// remove counts a member out.
func (t *asgTally) remove(inst *AutoScalingInstance) { t.adjust(inst, -inst.weightOf()) }

// adjust moves every total by w.
func (t *asgTally) adjust(inst *AutoScalingInstance, w int) {
	t.capacity += w
	if !inst.Spot {
		t.onDemand += w
	}
	t.perAZ[inst.AvailabilityZone] += w
	t.perSubnet[inst.SubnetID] += w
}

// asgWeightedCapacityXML renders a member's WeightedCapacity, a string as the Instance and
// AutoScalingInstanceDetails shapes type it, and empty — omitted — for an unweighted group.
//
// Both shapes constrain the member to 1–999. A memory-mib group's weights are MiB, which
// exceed that for every type in the catalog. No page, sample or published output shows what
// AWS reports for such a member — another unit, a clamped value, or nothing — so substrate
// reports the MiB, the weight the capacity is summed from, and documents the departure.
func asgWeightedCapacityXML(g *AutoScalingGroup, inst *AutoScalingInstance) string {
	if !g.weighted() {
		return ""
	}
	return strconv.Itoa(inst.weightOf())
}

// asgCheckCapacityType refuses a vcpu or memory-mib group that does not select instance
// types by attribute: API_CreateAutoScalingGroup supports DesiredCapacityType "for
// attribute-based instance type selection only". The wording is real Auto Scaling's, as
// reported verbatim from a CreateAutoScalingGroup refusal (hashicorp/terraform-provider-aws
// #31521); no page publishes it.
func asgCheckCapacityType(g *AutoScalingGroup) *AWSError {
	if g.capacityType() == asgCapacityUnits {
		return nil
	}
	if m := g.MixedInstancesPolicy; m != nil && m.usesInstanceRequirements() {
		return nil
	}
	return asgValidationError("The DesiredCapacityType field is only allowed in mixed instances groups containing InstanceRequirements overrides. Please include a MixedInstancesPolicy with InstanceRequirements for your group and try again.")
}

// asgCheckWeights validates the overrides' WeightedCapacity members: each an integer from 1 to
// 999, and either on every override or on none. Both rules are API_LaunchTemplateOverrides'
// ("Value must be in the range of 1–999"; "If you specify a value for WeightedCapacity for one
// instance type, you must specify a value for WeightedCapacity for all of them"). The wording
// is real Auto Scaling's as captured against the live service, not published.
//
// The reference also gives the member a pattern, one to three digits with no leading zero, but
// the live service does not refuse by it: "0" is refused with the range message and "two" with
// the number message, neither of which a pattern check would reach first. So a value the
// pattern would exclude only by its spelling — "+4", "007" — is taken as the integer it reads
// as, and stored, and echoed, in canonical form.
func asgCheckWeights(overrides []AutoScalingOverride) *AWSError {
	weighted := 0
	for i := range overrides {
		v := overrides[i].WeightedCapacity
		if v == "" {
			continue
		}
		weighted++
		n, err := strconv.Atoi(v)
		if err != nil {
			if _, ferr := strconv.ParseFloat(v, 64); ferr != nil {
				return asgValidationError("Invalid input for WeightedCapacity. The input must be a valid number.")
			}
			return asgValidationError("Invalid input for WeightedCapacity. The input must be a valid integer.")
		}
		if n < 1 || n > 999 {
			return asgValidationError("WeightedCapacity must be a positive number between 1 and 999.")
		}
		overrides[i].WeightedCapacity = strconv.Itoa(n)
	}
	if weighted > 0 && weighted < len(overrides) {
		return asgValidationError("Not all instance types have a defined WeightedCapacity. Specify missing weights, or remove all weights, and try again.")
	}
	return nil
}
