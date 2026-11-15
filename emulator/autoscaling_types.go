package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// State keys, all in [autoscalingNamespace]:
//
//	group:{account}/{region}/{name}     → AutoScalingGroup
//	activity:{account}/{region}/{seq}   → AutoScalingActivity, seq zero-padded so keys sort
//	seq:{account}/{region}              → the last sequence number minted for that scope
//
// A group's tags, suspended processes and member instances live on the group record rather
// than under keys of their own: every one of them is read whenever the group is, and a
// delete must take all of them with it.
//
// None of these records carries the account or Region as a member. Both are in the key,
// and the AutoScalingGroup shape publishes neither — its ARN is where a caller recovers
// them — so a field would be bookkeeping a response could leak (#756). The responses are
// rendered from the wire structs in autoscaling_wire.go regardless.

// asgGroupKey is the state key of a group.
func asgGroupKey(account, region, name string) string {
	return "group:" + account + "/" + region + "/" + name
}

// asgGroupPrefix is the key prefix every group in one account and Region shares.
func asgGroupPrefix(account, region string) string {
	return "group:" + account + "/" + region + "/"
}

// asgActivityPrefix is the key prefix every scaling activity in one account and Region
// shares.
func asgActivityPrefix(account, region string) string {
	return "activity:" + account + "/" + region + "/"
}

// AutoScalingLaunchTemplateSpec is a LaunchTemplateSpecification: which template, and
// which version of it, a group or an instance launches from.
type AutoScalingLaunchTemplateSpec struct {
	// LaunchTemplateID is the template's lt- identifier.
	LaunchTemplateID string `json:"launch_template_id,omitempty"`

	// LaunchTemplateName is the template's name.
	LaunchTemplateName string `json:"launch_template_name,omitempty"`

	// Version is "$Default", "$Latest" or a version number. On a group it is what the
	// request named, "$Default" when it named none, which is the documented default; on an
	// instance it is the number that launch resolved to.
	Version string `json:"version,omitempty"`
}

// AutoScalingRange is a Min/Max request pair from InstanceRequirements, either end
// optional.
type AutoScalingRange struct {
	// Min is the lower bound, or nil when absent.
	Min *float64 `json:"min,omitempty"`

	// Max is the upper bound, or nil when absent.
	Max *float64 `json:"max,omitempty"`
}

// contains reports whether v lies within the range, an absent end constraining nothing.
func (r *AutoScalingRange) contains(v float64) bool {
	if r == nil {
		return true
	}
	if r.Min != nil && v < *r.Min {
		return false
	}
	if r.Max != nil && v > *r.Max {
		return false
	}
	return true
}

// AutoScalingInstanceRequirements is the attribute-based instance type selection one
// override declares. Only the members substrate can evaluate against its catalog, or echo
// back, are held; see asgMatchInstanceTypes for which are evaluated.
type AutoScalingInstanceRequirements struct {
	// VCpuCount is the required vCPU range.
	VCpuCount AutoScalingRange `json:"vcpu_count"`

	// MemoryMiB is the required memory range, in MiB.
	MemoryMiB AutoScalingRange `json:"memory_mib"`

	// MemoryGiBPerVCpu is the optional memory-per-vCPU range, in GiB.
	MemoryGiBPerVCpu *AutoScalingRange `json:"memory_gib_per_vcpu,omitempty"`

	// AcceleratorCount is the optional accelerator count range.
	AcceleratorCount *AutoScalingRange `json:"accelerator_count,omitempty"`

	// CPUManufacturers lists the acceptable CPU manufacturers; empty is any.
	CPUManufacturers []string `json:"cpu_manufacturers,omitempty"`

	// AcceleratorTypes lists the acceptable accelerator types; empty is any.
	AcceleratorTypes []string `json:"accelerator_types,omitempty"`

	// AllowedInstanceTypes restricts the candidates, wildcards allowed.
	AllowedInstanceTypes []string `json:"allowed_instance_types,omitempty"`

	// ExcludedInstanceTypes removes candidates, wildcards allowed.
	ExcludedInstanceTypes []string `json:"excluded_instance_types,omitempty"`

	// InstanceGenerations is "current" and/or "previous"; empty is either.
	InstanceGenerations []string `json:"instance_generations,omitempty"`

	// BareMetal is included, excluded or required; empty reads as the default, excluded.
	BareMetal string `json:"bare_metal,omitempty"`

	// BurstablePerformance is included, excluded or required; empty reads as the default,
	// excluded.
	BurstablePerformance string `json:"burstable_performance,omitempty"`

	// LocalStorage is included, excluded or required; empty reads as the default,
	// included.
	LocalStorage string `json:"local_storage,omitempty"`

	// SpotMaxPricePercentageOverLowestPrice is echoed and not evaluated: substrate models
	// no price an instance type could exceed.
	SpotMaxPricePercentageOverLowestPrice *int `json:"spot_max_price_pct,omitempty"`

	// MaxSpotPriceAsPercentageOfOptimalOnDemandPrice is echoed and not evaluated.
	MaxSpotPriceAsPercentageOfOptimalOnDemandPrice *int `json:"max_spot_price_pct_od,omitempty"`

	// OnDemandMaxPricePercentageOverLowestPrice is echoed and not evaluated.
	OnDemandMaxPricePercentageOverLowestPrice *int `json:"od_max_price_pct,omitempty"`
}

// AutoScalingOverride is one LaunchTemplateOverrides member.
type AutoScalingOverride struct {
	// InstanceType names one instance type; exclusive with InstanceRequirements.
	InstanceType string `json:"instance_type,omitempty"`

	// InstanceRequirements selects instance types by attribute.
	InstanceRequirements *AutoScalingInstanceRequirements `json:"instance_requirements,omitempty"`

	// LaunchTemplateSpecification replaces the policy's template for this override.
	LaunchTemplateSpecification *AutoScalingLaunchTemplateSpec `json:"launch_template_specification,omitempty"`

	// ImageID replaces the template's AMI for this override.
	ImageID string `json:"image_id,omitempty"`

	// WeightedCapacity is the capacity units an instance launched from this override counts
	// toward a units group's desired capacity; see autoscaling_weights.go.
	WeightedCapacity string `json:"weighted_capacity,omitempty"`
}

// AutoScalingInstancesDistribution is a mixed-instances policy's InstancesDistribution,
// with the documented defaults already applied.
type AutoScalingInstancesDistribution struct {
	// OnDemandAllocationStrategy is prioritized, or lowest-price for a policy whose
	// overrides use InstanceRequirements — the documented defaults.
	OnDemandAllocationStrategy string `json:"on_demand_allocation_strategy"`

	// OnDemandBaseCapacity is the On-Demand floor; default 0.
	OnDemandBaseCapacity int `json:"on_demand_base_capacity"`

	// OnDemandPercentageAboveBaseCapacity is the On-Demand share above the base; default 100.
	OnDemandPercentageAboveBaseCapacity int `json:"on_demand_percentage_above_base_capacity"`

	// SpotAllocationStrategy defaults to lowest-price.
	SpotAllocationStrategy string `json:"spot_allocation_strategy"`

	// SpotInstancePools defaults to 2, and is reported only with lowest-price.
	SpotInstancePools int `json:"spot_instance_pools,omitempty"`

	// SpotMaxPrice is echoed; it has no default.
	SpotMaxPrice string `json:"spot_max_price,omitempty"`
}

// AutoScalingMixedInstancesPolicy is a group's MixedInstancesPolicy.
type AutoScalingMixedInstancesPolicy struct {
	// LaunchTemplateSpecification is the policy's template.
	LaunchTemplateSpecification AutoScalingLaunchTemplateSpec `json:"launch_template_specification"`

	// Overrides are the instance types or requirements, in priority order.
	Overrides []AutoScalingOverride `json:"overrides,omitempty"`

	// InstancesDistribution is the On-Demand/Spot split.
	InstancesDistribution AutoScalingInstancesDistribution `json:"instances_distribution"`
}

// usesInstanceRequirements reports whether any override selects by attribute, which
// changes the default On-Demand allocation strategy.
func (m *AutoScalingMixedInstancesPolicy) usesInstanceRequirements() bool {
	for _, o := range m.Overrides {
		if o.InstanceRequirements != nil {
			return true
		}
	}
	return false
}

// AutoScalingTag is one tag on a group.
type AutoScalingTag struct {
	// Key is the tag key.
	Key string `json:"key"`

	// Value is the tag value.
	Value string `json:"value"`

	// PropagateAtLaunch reports whether the tag is applied to instances the group launches.
	PropagateAtLaunch bool `json:"propagate_at_launch"`
}

// AutoScalingSuspendedProcess is one suspended scaling process.
type AutoScalingSuspendedProcess struct {
	// ProcessName is the process, e.g. "Launch".
	ProcessName string `json:"process_name"`

	// SuspensionReason is the published "User suspended at …" text.
	SuspensionReason string `json:"suspension_reason"`
}

// AutoScalingInstance is one member instance of a group.
type AutoScalingInstance struct {
	// InstanceID is the EC2 instance ID.
	InstanceID string `json:"instance_id"`

	// InstanceType is the type the instance launched as.
	InstanceType string `json:"instance_type"`

	// AvailabilityZone is the zone the instance launched in.
	AvailabilityZone string `json:"availability_zone"`

	// SubnetID is the subnet the instance launched into, empty for a zone-only group.
	SubnetID string `json:"subnet_id,omitempty"`

	// LaunchTemplate is the template and resolved version number the instance launched from.
	LaunchTemplate AutoScalingLaunchTemplateSpec `json:"launch_template"`

	// ImageID is the AMI an override supplied, empty when the template's applied — the
	// Instance shape publishes ImageId only for instances launched with an AMI override.
	ImageID string `json:"image_id,omitempty"`

	// ProtectedFromScaleIn is the instance's scale-in protection.
	ProtectedFromScaleIn bool `json:"protected_from_scale_in"`

	// HealthStatus is "Healthy" or "Unhealthy".
	HealthStatus string `json:"health_status"`

	// Spot reports whether the instance is a Spot Instance, which is how the On-Demand/Spot
	// split is kept when the group scales in. It is not a published member of Instance;
	// EC2's instanceLifecycle is where a caller observes it.
	Spot bool `json:"spot,omitempty"`

	// LaunchSeq orders the group's launches, oldest first, for OldestInstance and
	// NewestInstance.
	LaunchSeq int64 `json:"launch_seq"`

	// Weight is the capacity units the instance contributes, zero reading as one: its vCPUs or
	// MiB in a vcpu or memory-mib group, its override's WeightedCapacity in a weighted units
	// group. It is kept on the member because a type no override names any more keeps its last
	// weight; see autoscaling_weights.go.
	Weight int `json:"weight,omitempty"`

	// LaunchTime is when the group launched the instance, on the simulated clock.
	LaunchTime time.Time `json:"launch_time"`
}

// AutoScalingGroup is one Auto Scaling group's persisted record.
type AutoScalingGroup struct {
	// Name is the AutoScalingGroupName.
	Name string `json:"name"`

	// ARN is the group's ARN, carrying a UUID minted at creation.
	ARN string `json:"arn"`

	// MinSize, MaxSize and DesiredCapacity are the group's three size properties, in the
	// group's capacity units.
	MinSize int `json:"min_size"`
	// MaxSize is the group's maximum size.
	MaxSize int `json:"max_size"`
	// DesiredCapacity is the capacity the group reconciles toward.
	DesiredCapacity int `json:"desired_capacity"`

	// DefaultCooldown is in seconds; default 300.
	DefaultCooldown int `json:"default_cooldown"`

	// HealthCheckType defaults to EC2.
	HealthCheckType string `json:"health_check_type"`

	// HealthCheckGracePeriod is in seconds; default 0.
	HealthCheckGracePeriod int `json:"health_check_grace_period"`

	// VPCZoneIdentifier is the comma-separated subnet list, verbatim.
	VPCZoneIdentifier string `json:"vpc_zone_identifier,omitempty"`

	// AvailabilityZones is the zone list the group spans.
	AvailabilityZones []string `json:"availability_zones,omitempty"`

	// LaunchTemplate is set for a group launched from one template.
	LaunchTemplate *AutoScalingLaunchTemplateSpec `json:"launch_template,omitempty"`

	// MixedInstancesPolicy is set for a mixed-instances group.
	MixedInstancesPolicy *AutoScalingMixedInstancesPolicy `json:"mixed_instances_policy,omitempty"`

	// CapacityRebalance is echoed; substrate sends no rebalance recommendations.
	CapacityRebalance bool `json:"capacity_rebalance"`

	// NewInstancesProtectedFromScaleIn protects every instance launched from here on.
	NewInstancesProtectedFromScaleIn bool `json:"new_instances_protected_from_scale_in"`

	// TerminationPolicies are applied in order; default ["Default"].
	TerminationPolicies []string `json:"termination_policies,omitempty"`

	// PlacementGroup is forwarded to every launch.
	PlacementGroup string `json:"placement_group,omitempty"`

	// MaxInstanceLifetime is echoed and not enforced.
	MaxInstanceLifetime *int `json:"max_instance_lifetime,omitempty"`

	// DefaultInstanceWarmup is echoed.
	DefaultInstanceWarmup *int `json:"default_instance_warmup,omitempty"`

	// DesiredCapacityType is units, vcpu or memory-mib, empty reading as units: the unit
	// MinSize, MaxSize and DesiredCapacity are in.
	DesiredCapacityType string `json:"desired_capacity_type,omitempty"`

	// ServiceLinkedRoleARN defaults to AWSServiceRoleForAutoScaling.
	ServiceLinkedRoleARN string `json:"service_linked_role_arn"`

	// LoadBalancerNames and TargetGroupARNs are echoed; no instance is registered.
	LoadBalancerNames []string `json:"load_balancer_names,omitempty"`
	// TargetGroupARNs are echoed; no instance is registered with a target group.
	TargetGroupARNs []string `json:"target_group_arns,omitempty"`

	// Context is the reserved member, echoed.
	Context string `json:"context,omitempty"`

	// Tags are the group's tags, sorted by key.
	Tags []AutoScalingTag `json:"tags,omitempty"`

	// SuspendedProcesses are the processes currently suspended, sorted by name.
	SuspendedProcesses []AutoScalingSuspendedProcess `json:"suspended_processes,omitempty"`

	// Instances are the member instances, in launch order.
	Instances []AutoScalingInstance `json:"instances,omitempty"`

	// CreatedTime is when the group was created, on the simulated clock. Named for the
	// published member it renders as.
	CreatedTime time.Time `json:"created_time"`

	// LastActivityTime is when the group's last scaling activity ended, for HonorCooldown.
	LastActivityTime time.Time `json:"last_activity_time"`

	// LaunchSeq is the last launch sequence number the group minted.
	LaunchSeq int64 `json:"launch_seq"`

	// LastLaunchFailure is "{desired}|{StatusMessage}" of the most recent Failed launch, empty
	// once a launch succeeds. An observation whose attempt fails identically records nothing
	// new; see [AutoScalingPlugin.scaleOut].
	LastLaunchFailure string `json:"last_launch_failure,omitempty"`
}

// suspended reports whether the named process is suspended.
func (g *AutoScalingGroup) suspended(process string) bool {
	for _, sp := range g.SuspendedProcesses {
		if sp.ProcessName == process {
			return true
		}
	}
	return false
}

// tagMap returns the group's tags keyed by tag key.
func (g *AutoScalingGroup) tagMap() map[string]string {
	if len(g.Tags) == 0 {
		return nil
	}
	out := make(map[string]string, len(g.Tags))
	for _, t := range g.Tags {
		out[t.Key] = t.Value
	}
	return out
}

// instanceIndex returns the position of the member instance, or -1.
func (g *AutoScalingGroup) instanceIndex(id string) int {
	for i := range g.Instances {
		if g.Instances[i].InstanceID == id {
			return i
		}
	}
	return -1
}

// AutoScalingActivity is one scaling activity.
type AutoScalingActivity struct {
	// ActivityID is a UUID minted from the triggering request.
	ActivityID string `json:"activity_id"`

	// GroupName is the AutoScalingGroupName the activity belongs to.
	GroupName string `json:"group_name"`

	// GroupARN is the group's ARN at the time, which is what distinguishes a deleted group's
	// activities from those of a later group reusing the name.
	GroupARN string `json:"group_arn"`

	// Description is the published "Launching a new EC2 instance: i-…" text.
	Description string `json:"description"`

	// Cause is the published "At … a user request …" text.
	Cause string `json:"cause"`

	// Details is the published JSON detail string.
	Details string `json:"details"`

	// StatusCode is Successful or Failed; substrate's activities settle in the request that
	// starts them.
	StatusCode string `json:"status_code"`

	// StatusMessage explains a Failed activity.
	StatusMessage string `json:"status_message,omitempty"`

	// Progress is 100 for a settled activity.
	Progress int `json:"progress"`

	// StartTime and EndTime are on the simulated clock.
	StartTime time.Time `json:"start_time"`
	// EndTime is when the activity settled.
	EndTime time.Time `json:"end_time"`

	// Seq orders activities that share a start time.
	Seq int64 `json:"seq"`
}

// --- State access --------------------------------------------------------------

// loadGroup reads a group, returning nil when it does not exist.
func (p *AutoScalingPlugin) loadGroup(reqCtx *RequestContext, name string) (*AutoScalingGroup, error) {
	return asgLoadGroup(p.state, reqCtx, name)
}

// asgLoadGroup is [AutoScalingPlugin.loadGroup] over a bare StateManager, which the
// authorization decision needs: it reads a group's tags and ARN before the handler runs.
func asgLoadGroup(state StateManager, reqCtx *RequestContext, name string) (*AutoScalingGroup, error) {
	data, err := state.Get(context.Background(), autoscalingNamespace, asgGroupKey(reqCtx.AccountID, reqCtx.Region, name))
	if err != nil {
		return nil, fmt.Errorf("autoscaling get group %s: %w", name, err)
	}
	if data == nil {
		return nil, nil //nolint:nilnil // nil group = does not exist, handled by every caller.
	}
	var g AutoScalingGroup
	if err := json.Unmarshal(data, &g); err != nil {
		return nil, fmt.Errorf("autoscaling unmarshal group %s: %w", name, err)
	}
	return &g, nil
}

// saveGroup writes a group back.
func (p *AutoScalingPlugin) saveGroup(reqCtx *RequestContext, g *AutoScalingGroup) error {
	data, err := json.Marshal(g)
	if err != nil {
		return fmt.Errorf("autoscaling marshal group %s: %w", g.Name, err)
	}
	if err := p.state.Put(context.Background(), autoscalingNamespace, asgGroupKey(reqCtx.AccountID, reqCtx.Region, g.Name), data); err != nil {
		return fmt.Errorf("autoscaling put group %s: %w", g.Name, err)
	}
	return nil
}

// listGroups returns every group in the caller's account and Region, sorted by name.
func (p *AutoScalingPlugin) listGroups(reqCtx *RequestContext) ([]*AutoScalingGroup, error) {
	return asgListGroups(p.state, reqCtx)
}

// asgListGroups is [AutoScalingPlugin.listGroups] over a bare StateManager.
func asgListGroups(state StateManager, reqCtx *RequestContext) ([]*AutoScalingGroup, error) {
	prefix := asgGroupPrefix(reqCtx.AccountID, reqCtx.Region)
	keys, err := state.List(context.Background(), autoscalingNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("autoscaling list groups: %w", err)
	}
	sort.Strings(keys)
	out := make([]*AutoScalingGroup, 0, len(keys))
	for _, k := range keys {
		g, err := asgLoadGroup(state, reqCtx, strings.TrimPrefix(k, prefix))
		if err != nil {
			return nil, err
		}
		if g != nil {
			out = append(out, g)
		}
	}
	return out, nil
}

// asgGroupForInstance returns the group an instance belongs to, or nil.
func asgGroupForInstance(state StateManager, reqCtx *RequestContext, instanceID string) (*AutoScalingGroup, error) {
	groups, err := asgListGroups(state, reqCtx)
	if err != nil {
		return nil, err
	}
	for _, g := range groups {
		if g.instanceIndex(instanceID) >= 0 {
			return g, nil
		}
	}
	return nil, nil //nolint:nilnil // nil group = the instance is in no group.
}

// nextSeq mints the next sequence number for the caller's account and Region. It lives in
// state rather than on the plugin, so a reset clears it and a replay re-mints the same
// numbers.
func (p *AutoScalingPlugin) nextSeq(reqCtx *RequestContext) (int64, error) {
	key := "seq:" + reqCtx.AccountID + "/" + reqCtx.Region
	data, err := p.state.Get(context.Background(), autoscalingNamespace, key)
	if err != nil {
		return 0, fmt.Errorf("autoscaling get seq: %w", err)
	}
	var n int64
	if data != nil {
		n, err = strconv.ParseInt(string(data), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("autoscaling parse seq: %w", err)
		}
	}
	n++
	if err := p.state.Put(context.Background(), autoscalingNamespace, key, []byte(strconv.FormatInt(n, 10))); err != nil {
		return 0, fmt.Errorf("autoscaling put seq: %w", err)
	}
	return n, nil
}

// putActivity records a scaling activity, minting its sequence number.
func (p *AutoScalingPlugin) putActivity(reqCtx *RequestContext, a *AutoScalingActivity) error {
	seq, err := p.nextSeq(reqCtx)
	if err != nil {
		return err
	}
	a.Seq = seq
	data, err := json.Marshal(a)
	if err != nil {
		return fmt.Errorf("autoscaling marshal activity: %w", err)
	}
	key := asgActivityPrefix(reqCtx.AccountID, reqCtx.Region) + fmt.Sprintf("%016d", seq)
	if err := p.state.Put(context.Background(), autoscalingNamespace, key, data); err != nil {
		return fmt.Errorf("autoscaling put activity: %w", err)
	}
	return nil
}

// listActivities returns every activity in the caller's account and Region, in the order
// DescribeScalingActivities publishes: most recent start first.
func (p *AutoScalingPlugin) listActivities(reqCtx *RequestContext) ([]AutoScalingActivity, error) {
	prefix := asgActivityPrefix(reqCtx.AccountID, reqCtx.Region)
	keys, err := p.state.List(context.Background(), autoscalingNamespace, prefix)
	if err != nil {
		return nil, fmt.Errorf("autoscaling list activities: %w", err)
	}
	out := make([]AutoScalingActivity, 0, len(keys))
	for _, k := range keys {
		data, err := p.state.Get(context.Background(), autoscalingNamespace, k)
		if err != nil {
			return nil, fmt.Errorf("autoscaling get activity: %w", err)
		}
		if data == nil {
			continue
		}
		var a AutoScalingActivity
		if err := json.Unmarshal(data, &a); err != nil {
			return nil, fmt.Errorf("autoscaling unmarshal activity: %w", err)
		}
		out = append(out, a)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].StartTime.Equal(out[j].StartTime) {
			return out[i].StartTime.After(out[j].StartTime)
		}
		return out[i].Seq > out[j].Seq
	})
	return out, nil
}
