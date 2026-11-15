package emulator

import (
	"sort"
	"strconv"
	"strings"
)

// Request parsing for CreateAutoScalingGroup and UpdateAutoScalingGroup.
//
// Both operations take the same structures — a LaunchTemplateSpecification, a
// MixedInstancesPolicy, a Tags list — flattened by the Query protocol into
// `Prefix.member.N.Field` parameters. The readers here return parsed values and leave the
// judging to the handlers, so the two operations apply one set of rules.

// asgParseLaunchTemplateSpec reads a LaunchTemplateSpecification under prefix, returning nil
// when neither identifier was sent.
func asgParseLaunchTemplateSpec(params map[string]string, prefix string) *AutoScalingLaunchTemplateSpec {
	spec := AutoScalingLaunchTemplateSpec{
		LaunchTemplateID:   params[prefix+"LaunchTemplateId"],
		LaunchTemplateName: params[prefix+"LaunchTemplateName"],
		Version:            params[prefix+"Version"],
	}
	if spec.LaunchTemplateID == "" && spec.LaunchTemplateName == "" && spec.Version == "" {
		return nil
	}
	return &spec
}

// asgParseRange reads a Min/Max pair under prefix, returning nil when neither was sent.
func asgParseRange(params map[string]string, prefix string) (*AutoScalingRange, *AWSError) {
	var r AutoScalingRange
	for _, end := range []struct {
		key string
		dst **float64
	}{{"Min", &r.Min}, {"Max", &r.Max}} {
		raw, ok := params[prefix+end.key]
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
		if err != nil {
			return nil, asgValidationError("1 validation error detected: Value '%s' at '%s' failed to satisfy constraint: Member must be a number",
				raw, lowerFirst(strings.ReplaceAll(prefix+end.key, ".", "")))
		}
		*end.dst = &v
	}
	if r.Min == nil && r.Max == nil {
		return nil, nil
	}
	return &r, nil
}

// asgParseOptionalInt reads an optional integer member as a pointer.
func asgParseOptionalInt(params map[string]string, key string) (*int, *AWSError) {
	v, ok, awsErr := asgInt(params, key)
	if awsErr != nil || !ok {
		return nil, awsErr
	}
	return &v, nil
}

// asgParseInstanceRequirements reads an InstanceRequirements structure under prefix,
// returning nil when no member of it was sent.
//
// VCpuCount.Min and MemoryMiB.Min are required by the model (both request types mark Min
// required, and InstanceRequirements marks both structures required), so their absence is
// refused rather than defaulted.
func asgParseInstanceRequirements(params map[string]string, prefix string) (*AutoScalingInstanceRequirements, *AWSError) {
	sent := false
	for k := range params {
		if strings.HasPrefix(k, prefix) {
			sent = true
			break
		}
	}
	if !sent {
		return nil, nil
	}
	var req AutoScalingInstanceRequirements
	vcpu, awsErr := asgParseRange(params, prefix+"VCpuCount.")
	if awsErr != nil {
		return nil, awsErr
	}
	if vcpu == nil || vcpu.Min == nil {
		return nil, asgMissingParameter("VCpuCount.Min")
	}
	req.VCpuCount = *vcpu
	mem, awsErr := asgParseRange(params, prefix+"MemoryMiB.")
	if awsErr != nil {
		return nil, awsErr
	}
	if mem == nil || mem.Min == nil {
		return nil, asgMissingParameter("MemoryMiB.Min")
	}
	req.MemoryMiB = *mem
	if req.MemoryGiBPerVCpu, awsErr = asgParseRange(params, prefix+"MemoryGiBPerVCpu."); awsErr != nil {
		return nil, awsErr
	}
	if req.AcceleratorCount, awsErr = asgParseRange(params, prefix+"AcceleratorCount."); awsErr != nil {
		return nil, awsErr
	}
	req.CPUManufacturers = iamMemberList(params, prefix+"CpuManufacturers")
	req.AcceleratorTypes = iamMemberList(params, prefix+"AcceleratorTypes")
	req.AllowedInstanceTypes = iamMemberList(params, prefix+"AllowedInstanceTypes")
	req.ExcludedInstanceTypes = iamMemberList(params, prefix+"ExcludedInstanceTypes")
	req.InstanceGenerations = iamMemberList(params, prefix+"InstanceGenerations")
	if len(req.AllowedInstanceTypes) > 0 && len(req.ExcludedInstanceTypes) > 0 {
		// API_InstanceRequirements: "If you specify AllowedInstanceTypes, you can't specify
		// ExcludedInstanceTypes."
		return nil, asgValidationError("You can specify either AllowedInstanceTypes or ExcludedInstanceTypes, but not both.")
	}
	for _, field := range []struct {
		key string
		dst *string
	}{
		{"BareMetal", &req.BareMetal},
		{"BurstablePerformance", &req.BurstablePerformance},
		{"LocalStorage", &req.LocalStorage},
	} {
		v := params[prefix+field.key]
		switch v {
		case "", "included", "excluded", "required":
			*field.dst = v
		default:
			return nil, asgValidationError("1 validation error detected: Value '%s' at '%s' failed to satisfy constraint: Member must satisfy enum value set: [included, excluded, required]",
				v, lowerFirst(field.key))
		}
	}
	for _, field := range []struct {
		key string
		dst **int
	}{
		{"SpotMaxPricePercentageOverLowestPrice", &req.SpotMaxPricePercentageOverLowestPrice},
		{"MaxSpotPriceAsPercentageOfOptimalOnDemandPrice", &req.MaxSpotPriceAsPercentageOfOptimalOnDemandPrice},
		{"OnDemandMaxPricePercentageOverLowestPrice", &req.OnDemandMaxPricePercentageOverLowestPrice},
	} {
		if *field.dst, awsErr = asgParseOptionalInt(params, prefix+field.key); awsErr != nil {
			return nil, awsErr
		}
	}
	return &req, nil
}

// asgApplyMixedInstancesPolicy applies a request's MixedInstancesPolicy members over base and
// returns the result, or base unchanged when the request sent none. base is nil for a create,
// and for an update of a group that had no policy, in which case the documented defaults fill
// every member the request left out: "this structure uses default values for unspecified
// parameters, except for SpotMaxPrice" (API_InstancesDistribution).
//
// For an update, member by member, because UpdateAutoScalingGroup's rule is "Any properties
// that you don't specify are not changed by this update request":
//
//   - **LaunchTemplateSpecification** is replaced when the request names a template; a request
//     sending only its Version re-points the existing template's version.
//   - **Overrides** are replaced as a list when any is sent. The setup overview's limitation is
//     the authority — "you can update the overrides with the UpdateAutoScalingGroup API call but
//     not delete them" — and an absent list is not a request to empty it.
//   - **InstancesDistribution** members are each kept unless sent. API_InstancesDistribution
//     speaks of modifying SpotAllocationStrategy, SpotInstancePools or SpotMaxPrice
//     individually, and SpotMaxPrice sent as "" clears it ("To remove a maximum price that you
//     previously set, include the property but specify an empty string").
//
// Two defaults are re-derived after the merge, because they depend on other members: an
// OnDemandAllocationStrategy the request did not name becomes lowest-price when the overrides
// now use InstanceRequirements (prioritized "cannot be used for groups that do"), and
// SpotInstancePools is reported only with the lowest-price Spot strategy, defaulting to 2.
func asgApplyMixedInstancesPolicy(params map[string]string, base *AutoScalingMixedInstancesPolicy) (*AutoScalingMixedInstancesPolicy, *AWSError) {
	const root = "MixedInstancesPolicy."
	if !hasParamPrefix(params, root) {
		return base, nil
	}
	var m AutoScalingMixedInstancesPolicy
	if base != nil {
		m = *base
		m.Overrides = append([]AutoScalingOverride(nil), base.Overrides...)
	} else {
		m.InstancesDistribution.OnDemandPercentageAboveBaseCapacity = 100
	}

	const specRoot = root + "LaunchTemplate.LaunchTemplateSpecification."
	if spec := asgParseLaunchTemplateSpec(params, specRoot); spec != nil {
		if spec.LaunchTemplateID == "" && spec.LaunchTemplateName == "" && base != nil {
			m.LaunchTemplateSpecification.Version = spec.Version
		} else {
			m.LaunchTemplateSpecification = *spec
		}
	}

	if hasParamPrefix(params, root+"LaunchTemplate.Overrides.") {
		m.Overrides = nil
		for _, fields := range iamMemberStructs(params, root+"LaunchTemplate.Overrides") {
			ov := AutoScalingOverride{
				InstanceType:     fields["InstanceType"],
				ImageID:          fields["ImageId"],
				WeightedCapacity: fields["WeightedCapacity"],
			}
			ov.LaunchTemplateSpecification = asgParseLaunchTemplateSpec(fields, "LaunchTemplateSpecification.")
			ir, awsErr := asgParseInstanceRequirements(fields, "InstanceRequirements.")
			if awsErr != nil {
				return nil, awsErr
			}
			ov.InstanceRequirements = ir
			if ov.InstanceType != "" && ov.InstanceRequirements != nil {
				// API_LaunchTemplateOverrides: "If you specify InstanceRequirements, you can't
				// specify InstanceType."
				return nil, asgValidationError("You can specify either InstanceType or InstanceRequirements in an override, but not both.")
			}
			m.Overrides = append(m.Overrides, ov)
		}
		if awsErr := asgCheckWeights(m.Overrides); awsErr != nil {
			return nil, awsErr
		}
	}

	const dist = root + "InstancesDistribution."
	d := &m.InstancesDistribution
	if v, ok := params[dist+"OnDemandAllocationStrategy"]; ok {
		d.OnDemandAllocationStrategy = v
	}
	if v, ok := params[dist+"SpotAllocationStrategy"]; ok {
		d.SpotAllocationStrategy = v
	}
	if v, ok := params[dist+"SpotMaxPrice"]; ok {
		d.SpotMaxPrice = v
	}
	if v, ok, awsErr := asgInt(params, dist+"OnDemandBaseCapacity"); awsErr != nil {
		return nil, awsErr
	} else if ok {
		if v < 0 {
			return nil, asgValidationError("OnDemandBaseCapacity must be greater than or equal to 0.")
		}
		d.OnDemandBaseCapacity = v
	}
	if v, ok, awsErr := asgInt(params, dist+"OnDemandPercentageAboveBaseCapacity"); awsErr != nil {
		return nil, awsErr
	} else if ok {
		if v < 0 || v > 100 {
			return nil, asgValidationError("OnDemandPercentageAboveBaseCapacity must be between 0 and 100.")
		}
		d.OnDemandPercentageAboveBaseCapacity = v
	}
	_, odNamed := params[dist+"OnDemandAllocationStrategy"]
	switch {
	case d.OnDemandAllocationStrategy == "":
		d.OnDemandAllocationStrategy = "prioritized"
		if m.usesInstanceRequirements() {
			d.OnDemandAllocationStrategy = "lowest-price"
		}
	case !odNamed && d.OnDemandAllocationStrategy == "prioritized" && m.usesInstanceRequirements():
		d.OnDemandAllocationStrategy = "lowest-price"
	}
	if d.SpotAllocationStrategy == "" {
		d.SpotAllocationStrategy = "lowest-price"
	}
	pools, poolsSent, awsErr := asgInt(params, dist+"SpotInstancePools")
	if awsErr != nil {
		return nil, awsErr
	}
	switch {
	case d.SpotAllocationStrategy != "lowest-price":
		d.SpotInstancePools = 0
	case poolsSent:
		d.SpotInstancePools = pools
	case d.SpotInstancePools == 0:
		d.SpotInstancePools = 2
	}
	return &m, nil
}

// asgRequestTag is one Tags.member.N of a request. The group a tag names is in ResourceId,
// which CreateAutoScalingGroup does not require because the group is the request's own.
type asgRequestTag struct {
	AutoScalingTag
	resourceID   string
	resourceType string
}

// asgParseTags reads Tags.member.N into tags in request order.
func asgParseTags(params map[string]string) []asgRequestTag {
	members := iamMemberStructs(params, "Tags")
	out := make([]asgRequestTag, 0, len(members))
	for _, m := range members {
		prop, _ := asgBool(m, "PropagateAtLaunch")
		out = append(out, asgRequestTag{
			AutoScalingTag: AutoScalingTag{Key: m["Key"], Value: m["Value"], PropagateAtLaunch: prop},
			resourceID:     m["ResourceId"],
			resourceType:   m["ResourceType"],
		})
	}
	return out
}

// asgMaxTags is the per-group tag limit, which the tagging guide states as "The maximum
// number of tags per resource is 50".
const asgMaxTags = 50

// asgCheckTagKeys refuses a tag whose key is empty or carries the reserved `aws:` prefix,
// which the tagging guide says "is reserved for AWS use".
func asgCheckTagKeys(tags []asgRequestTag) *AWSError {
	for _, t := range tags {
		if t.Key == "" {
			return asgMissingParameter("Tags.member.Key")
		}
		if len(t.Key) > 128 {
			return asgValidationError("Tag key exceeds the maximum length of 128 characters: %s", t.Key)
		}
		if len(t.Value) > 256 {
			return asgValidationError("Tag value exceeds the maximum length of 256 characters for key %s", t.Key)
		}
		if strings.HasPrefix(strings.ToLower(t.Key), "aws:") {
			return asgValidationError("Tag keys starting with 'aws:' are reserved for internal use")
		}
	}
	return nil
}

// asgMergeTags applies tags over existing, a key already present taking the new value —
// CreateOrUpdateTags: "When you specify a tag with a key that already exists, the operation
// overwrites the previous tag definition". The result is sorted by key.
func asgMergeTags(existing []AutoScalingTag, tags []AutoScalingTag) []AutoScalingTag {
	byKey := make(map[string]AutoScalingTag, len(existing)+len(tags))
	for _, t := range existing {
		byKey[t.Key] = t
	}
	for _, t := range tags {
		byKey[t.Key] = t
	}
	out := make([]AutoScalingTag, 0, len(byKey))
	for _, t := range byKey {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// asgSplitSubnets splits a VPCZoneIdentifier into its subnet IDs, in order.
func asgSplitSubnets(v string) []string {
	var out []string
	for _, s := range strings.Split(v, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// asgValidTerminationPolicies are the predefined termination policies; a Lambda function
// ARN is also accepted, see [asgCheckTerminationPolicies].
var asgValidTerminationPolicies = []string{ //nolint:gochecknoglobals // read-only reference data from API_CreateAutoScalingGroup.
	"Default", "AllocationStrategy", "ClosestToNextInstanceHour", "NewestInstance",
	"OldestInstance", "OldestLaunchConfiguration", "OldestLaunchTemplate",
}

// asgCheckTerminationPolicies refuses a policy that is neither predefined nor a Lambda ARN.
func asgCheckTerminationPolicies(policies []string) *AWSError {
	for _, p := range policies {
		if containsStr(asgValidTerminationPolicies, p) || strings.HasPrefix(p, "arn:aws:lambda:") {
			continue
		}
		return asgValidationError("Termination policy %s is not valid.", p)
	}
	return nil
}
