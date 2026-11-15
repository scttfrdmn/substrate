package emulator

import (
	"strconv"
)

// The response shapes, rendered from the persisted records rather than marshaled from them.
//
// Member names and nesting are botocore's autoscaling 2011-01-01 model: every list is a
// non-flattened `<Name><member>…</member></Name>`, which is what [asgList] renders. The
// lists AutoScalingGroup always carries — Instances, AvailabilityZones, Tags,
// SuspendedProcesses, TerminationPolicies, LoadBalancerNames, TargetGroupARNs,
// EnabledMetrics — are emitted even when empty, as the DescribeAutoScalingGroups sample
// shows `<LoadBalancerNames />`, so an SDK reads an empty list rather than an absent key:
// boto3 callers index `group["Instances"]` directly.

// asgList is a non-flattened Query-protocol list.
type asgList[T any] struct {
	Members []T `xml:"member"`
}

// asgListOf wraps a slice for rendering.
func asgListOf[T any](items []T) asgList[T] { return asgList[T]{Members: items} }

// asgOptionalList wraps a slice for an optional list member, nil when empty so the element
// is omitted.
func asgOptionalList[T any](items []T) *asgList[T] {
	if len(items) == 0 {
		return nil
	}
	return &asgList[T]{Members: items}
}

// asgLaunchTemplateSpecXML is a LaunchTemplateSpecification.
type asgLaunchTemplateSpecXML struct {
	LaunchTemplateID   string `xml:"LaunchTemplateId,omitempty"`
	LaunchTemplateName string `xml:"LaunchTemplateName,omitempty"`
	Version            string `xml:"Version,omitempty"`
}

// asgSpecXML renders a specification, nil for none.
func asgSpecXML(s *AutoScalingLaunchTemplateSpec) *asgLaunchTemplateSpecXML {
	if s == nil {
		return nil
	}
	return &asgLaunchTemplateSpecXML{LaunchTemplateID: s.LaunchTemplateID, LaunchTemplateName: s.LaunchTemplateName, Version: s.Version}
}

// asgRangeXML is a Min/Max request pair.
type asgRangeXML struct {
	Min string `xml:"Min,omitempty"`
	Max string `xml:"Max,omitempty"`
}

// asgRangeOf renders a range, nil for none. Integers render without a fraction, which is how
// every integer-typed range in the model reads back.
func asgRangeOf(r *AutoScalingRange) *asgRangeXML {
	if r == nil {
		return nil
	}
	f := func(v *float64) string {
		if v == nil {
			return ""
		}
		return strconv.FormatFloat(*v, 'f', -1, 64)
	}
	return &asgRangeXML{Min: f(r.Min), Max: f(r.Max)}
}

// asgInstanceRequirementsXML is an InstanceRequirements, echoing the members substrate holds.
type asgInstanceRequirementsXML struct {
	VCpuCount                                      *asgRangeXML     `xml:"VCpuCount"`
	MemoryMiB                                      *asgRangeXML     `xml:"MemoryMiB"`
	CPUManufacturers                               *asgList[string] `xml:"CpuManufacturers,omitempty"`
	MemoryGiBPerVCpu                               *asgRangeXML     `xml:"MemoryGiBPerVCpu,omitempty"`
	ExcludedInstanceTypes                          *asgList[string] `xml:"ExcludedInstanceTypes,omitempty"`
	InstanceGenerations                            *asgList[string] `xml:"InstanceGenerations,omitempty"`
	SpotMaxPricePercentageOverLowestPrice          *int             `xml:"SpotMaxPricePercentageOverLowestPrice,omitempty"`
	MaxSpotPriceAsPercentageOfOptimalOnDemandPrice *int             `xml:"MaxSpotPriceAsPercentageOfOptimalOnDemandPrice,omitempty"`
	OnDemandMaxPricePercentageOverLowestPrice      *int             `xml:"OnDemandMaxPricePercentageOverLowestPrice,omitempty"`
	BareMetal                                      string           `xml:"BareMetal,omitempty"`
	BurstablePerformance                           string           `xml:"BurstablePerformance,omitempty"`
	LocalStorage                                   string           `xml:"LocalStorage,omitempty"`
	AcceleratorTypes                               *asgList[string] `xml:"AcceleratorTypes,omitempty"`
	AcceleratorCount                               *asgRangeXML     `xml:"AcceleratorCount,omitempty"`
	AllowedInstanceTypes                           *asgList[string] `xml:"AllowedInstanceTypes,omitempty"`
}

// asgOverrideXML is a LaunchTemplateOverrides member.
type asgOverrideXML struct {
	InstanceType                string                      `xml:"InstanceType,omitempty"`
	WeightedCapacity            string                      `xml:"WeightedCapacity,omitempty"`
	LaunchTemplateSpecification *asgLaunchTemplateSpecXML   `xml:"LaunchTemplateSpecification,omitempty"`
	InstanceRequirements        *asgInstanceRequirementsXML `xml:"InstanceRequirements,omitempty"`
	ImageID                     string                      `xml:"ImageId,omitempty"`
}

// asgInstancesDistributionXML is an InstancesDistribution.
type asgInstancesDistributionXML struct {
	OnDemandAllocationStrategy          string `xml:"OnDemandAllocationStrategy"`
	OnDemandBaseCapacity                int    `xml:"OnDemandBaseCapacity"`
	OnDemandPercentageAboveBaseCapacity int    `xml:"OnDemandPercentageAboveBaseCapacity"`
	SpotAllocationStrategy              string `xml:"SpotAllocationStrategy"`
	SpotInstancePools                   int    `xml:"SpotInstancePools,omitempty"`
	SpotMaxPrice                        string `xml:"SpotMaxPrice,omitempty"`
}

// asgMixedInstancesPolicyXML is a MixedInstancesPolicy.
type asgMixedInstancesPolicyXML struct {
	LaunchTemplate struct {
		LaunchTemplateSpecification asgLaunchTemplateSpecXML `xml:"LaunchTemplateSpecification"`
		Overrides                   *asgList[asgOverrideXML] `xml:"Overrides,omitempty"`
	} `xml:"LaunchTemplate"`
	InstancesDistribution asgInstancesDistributionXML `xml:"InstancesDistribution"`
}

// asgMixedXML renders a mixed-instances policy, nil for none.
func asgMixedXML(m *AutoScalingMixedInstancesPolicy) *asgMixedInstancesPolicyXML {
	if m == nil {
		return nil
	}
	out := &asgMixedInstancesPolicyXML{}
	out.LaunchTemplate.LaunchTemplateSpecification = *asgSpecXML(&m.LaunchTemplateSpecification)
	var ovs []asgOverrideXML
	for _, o := range m.Overrides {
		x := asgOverrideXML{
			InstanceType:                o.InstanceType,
			WeightedCapacity:            o.WeightedCapacity,
			LaunchTemplateSpecification: asgSpecXML(o.LaunchTemplateSpecification),
			ImageID:                     o.ImageID,
		}
		if r := o.InstanceRequirements; r != nil {
			x.InstanceRequirements = &asgInstanceRequirementsXML{
				VCpuCount:             asgRangeOf(&r.VCpuCount),
				MemoryMiB:             asgRangeOf(&r.MemoryMiB),
				CPUManufacturers:      asgOptionalList(r.CPUManufacturers),
				MemoryGiBPerVCpu:      asgRangeOf(r.MemoryGiBPerVCpu),
				ExcludedInstanceTypes: asgOptionalList(r.ExcludedInstanceTypes),
				InstanceGenerations:   asgOptionalList(r.InstanceGenerations),

				SpotMaxPricePercentageOverLowestPrice:          r.SpotMaxPricePercentageOverLowestPrice,
				MaxSpotPriceAsPercentageOfOptimalOnDemandPrice: r.MaxSpotPriceAsPercentageOfOptimalOnDemandPrice,
				OnDemandMaxPricePercentageOverLowestPrice:      r.OnDemandMaxPricePercentageOverLowestPrice,

				BareMetal:            r.BareMetal,
				BurstablePerformance: r.BurstablePerformance,
				LocalStorage:         r.LocalStorage,
				AcceleratorTypes:     asgOptionalList(r.AcceleratorTypes),
				AcceleratorCount:     asgRangeOf(r.AcceleratorCount),
				AllowedInstanceTypes: asgOptionalList(r.AllowedInstanceTypes),
			}
		}
		ovs = append(ovs, x)
	}
	out.LaunchTemplate.Overrides = asgOptionalList(ovs)
	out.InstancesDistribution = asgInstancesDistributionXML(m.InstancesDistribution)
	return out
}

// asgInstanceXML is an Instance, the group-scoped view of a member.
type asgInstanceXML struct {
	InstanceID           string                    `xml:"InstanceId"`
	InstanceType         string                    `xml:"InstanceType"`
	AvailabilityZone     string                    `xml:"AvailabilityZone"`
	LifecycleState       string                    `xml:"LifecycleState"`
	HealthStatus         string                    `xml:"HealthStatus"`
	LaunchTemplate       *asgLaunchTemplateSpecXML `xml:"LaunchTemplate,omitempty"`
	ImageID              string                    `xml:"ImageId,omitempty"`
	ProtectedFromScaleIn bool                      `xml:"ProtectedFromScaleIn"`
	WeightedCapacity     string                    `xml:"WeightedCapacity,omitempty"`
}

// asgInstanceDetailsXML is an AutoScalingInstanceDetails, the account-scoped view.
type asgInstanceDetailsXML struct {
	InstanceID           string                    `xml:"InstanceId"`
	InstanceType         string                    `xml:"InstanceType"`
	AutoScalingGroupName string                    `xml:"AutoScalingGroupName"`
	AvailabilityZone     string                    `xml:"AvailabilityZone"`
	LifecycleState       string                    `xml:"LifecycleState"`
	HealthStatus         string                    `xml:"HealthStatus"`
	LaunchTemplate       *asgLaunchTemplateSpecXML `xml:"LaunchTemplate,omitempty"`
	ImageID              string                    `xml:"ImageId,omitempty"`
	ProtectedFromScaleIn bool                      `xml:"ProtectedFromScaleIn"`
	WeightedCapacity     string                    `xml:"WeightedCapacity,omitempty"`
}

// asgLifecycleState is the LifecycleState a member reports. Substrate's launches and
// terminations settle in the request that makes them, so a member is InService: EC2's
// seeded progression governs what DescribeInstances says about the instance, and a group
// does not add a second, independent one.
const asgLifecycleState = "InService"

// asgTagXML is a TagDescription.
type asgTagXML struct {
	ResourceID        string `xml:"ResourceId"`
	ResourceType      string `xml:"ResourceType"`
	Key               string `xml:"Key"`
	Value             string `xml:"Value"`
	PropagateAtLaunch bool   `xml:"PropagateAtLaunch"`
}

// asgTagsXML renders a group's tags.
func asgTagsXML(g *AutoScalingGroup) []asgTagXML {
	out := make([]asgTagXML, 0, len(g.Tags))
	for _, t := range g.Tags {
		out = append(out, asgTagXML{ResourceID: g.Name, ResourceType: "auto-scaling-group", Key: t.Key, Value: t.Value, PropagateAtLaunch: t.PropagateAtLaunch})
	}
	return out
}

// asgSuspendedProcessXML is a SuspendedProcess.
type asgSuspendedProcessXML struct {
	ProcessName      string `xml:"ProcessName"`
	SuspensionReason string `xml:"SuspensionReason"`
}

// asgGroupXML is an AutoScalingGroup.
type asgGroupXML struct {
	AutoScalingGroupName             string                          `xml:"AutoScalingGroupName"`
	AutoScalingGroupARN              string                          `xml:"AutoScalingGroupARN"`
	LaunchTemplate                   *asgLaunchTemplateSpecXML       `xml:"LaunchTemplate,omitempty"`
	MixedInstancesPolicy             *asgMixedInstancesPolicyXML     `xml:"MixedInstancesPolicy,omitempty"`
	MinSize                          int                             `xml:"MinSize"`
	MaxSize                          int                             `xml:"MaxSize"`
	DesiredCapacity                  int                             `xml:"DesiredCapacity"`
	DefaultCooldown                  int                             `xml:"DefaultCooldown"`
	AvailabilityZones                asgList[string]                 `xml:"AvailabilityZones"`
	LoadBalancerNames                asgList[string]                 `xml:"LoadBalancerNames"`
	TargetGroupARNs                  asgList[string]                 `xml:"TargetGroupARNs"`
	HealthCheckType                  string                          `xml:"HealthCheckType"`
	HealthCheckGracePeriod           int                             `xml:"HealthCheckGracePeriod"`
	Instances                        asgList[asgInstanceXML]         `xml:"Instances"`
	CreatedTime                      string                          `xml:"CreatedTime"`
	SuspendedProcesses               asgList[asgSuspendedProcessXML] `xml:"SuspendedProcesses"`
	PlacementGroup                   string                          `xml:"PlacementGroup,omitempty"`
	VPCZoneIdentifier                string                          `xml:"VPCZoneIdentifier,omitempty"`
	EnabledMetrics                   asgList[struct{}]               `xml:"EnabledMetrics"`
	Tags                             asgList[asgTagXML]              `xml:"Tags"`
	TerminationPolicies              asgList[string]                 `xml:"TerminationPolicies"`
	NewInstancesProtectedFromScaleIn bool                            `xml:"NewInstancesProtectedFromScaleIn"`
	ServiceLinkedRoleARN             string                          `xml:"ServiceLinkedRoleARN"`
	MaxInstanceLifetime              *int                            `xml:"MaxInstanceLifetime,omitempty"`
	CapacityRebalance                bool                            `xml:"CapacityRebalance"`
	Context                          string                          `xml:"Context,omitempty"`
	DesiredCapacityType              string                          `xml:"DesiredCapacityType,omitempty"`
	DefaultInstanceWarmup            *int                            `xml:"DefaultInstanceWarmup,omitempty"`
}

// asgGroupToXML renders a group. includeInstances is DescribeAutoScalingGroups'
// IncludeInstances, whose default is true.
func asgGroupToXML(g *AutoScalingGroup, includeInstances bool) asgGroupXML {
	out := asgGroupXML{
		AutoScalingGroupName:             g.Name,
		AutoScalingGroupARN:              g.ARN,
		LaunchTemplate:                   asgSpecXML(g.LaunchTemplate),
		MixedInstancesPolicy:             asgMixedXML(g.MixedInstancesPolicy),
		MinSize:                          g.MinSize,
		MaxSize:                          g.MaxSize,
		DesiredCapacity:                  g.DesiredCapacity,
		DefaultCooldown:                  g.DefaultCooldown,
		AvailabilityZones:                asgListOf(g.AvailabilityZones),
		LoadBalancerNames:                asgListOf(g.LoadBalancerNames),
		TargetGroupARNs:                  asgListOf(g.TargetGroupARNs),
		HealthCheckType:                  g.HealthCheckType,
		HealthCheckGracePeriod:           g.HealthCheckGracePeriod,
		CreatedTime:                      asgTimestamp(g.CreatedTime),
		PlacementGroup:                   g.PlacementGroup,
		VPCZoneIdentifier:                g.VPCZoneIdentifier,
		Tags:                             asgListOf(asgTagsXML(g)),
		TerminationPolicies:              asgListOf(g.TerminationPolicies),
		NewInstancesProtectedFromScaleIn: g.NewInstancesProtectedFromScaleIn,
		ServiceLinkedRoleARN:             g.ServiceLinkedRoleARN,
		MaxInstanceLifetime:              g.MaxInstanceLifetime,
		CapacityRebalance:                g.CapacityRebalance,
		Context:                          g.Context,
		DesiredCapacityType:              g.DesiredCapacityType,
		DefaultInstanceWarmup:            g.DefaultInstanceWarmup,
	}
	sps := make([]asgSuspendedProcessXML, 0, len(g.SuspendedProcesses))
	for _, sp := range g.SuspendedProcesses {
		sps = append(sps, asgSuspendedProcessXML(sp))
	}
	out.SuspendedProcesses = asgListOf(sps)
	if includeInstances {
		insts := make([]asgInstanceXML, 0, len(g.Instances))
		for i := range g.Instances {
			inst := g.Instances[i]
			lt := inst.LaunchTemplate
			insts = append(insts, asgInstanceXML{
				InstanceID:           inst.InstanceID,
				InstanceType:         inst.InstanceType,
				AvailabilityZone:     inst.AvailabilityZone,
				LifecycleState:       asgLifecycleState,
				HealthStatus:         inst.HealthStatus,
				LaunchTemplate:       asgSpecXML(&lt),
				ImageID:              inst.ImageID,
				ProtectedFromScaleIn: inst.ProtectedFromScaleIn,
				WeightedCapacity:     asgWeightedCapacityXML(g, &g.Instances[i]),
			})
		}
		out.Instances = asgListOf(insts)
	}
	return out
}

// asgActivityXML is an Activity.
type asgActivityXML struct {
	ActivityID            string `xml:"ActivityId"`
	AutoScalingGroupName  string `xml:"AutoScalingGroupName"`
	Description           string `xml:"Description,omitempty"`
	Cause                 string `xml:"Cause"`
	StartTime             string `xml:"StartTime"`
	EndTime               string `xml:"EndTime,omitempty"`
	StatusCode            string `xml:"StatusCode"`
	StatusMessage         string `xml:"StatusMessage,omitempty"`
	Progress              int    `xml:"Progress"`
	Details               string `xml:"Details,omitempty"`
	AutoScalingGroupState string `xml:"AutoScalingGroupState,omitempty"`
	AutoScalingGroupARN   string `xml:"AutoScalingGroupARN,omitempty"`
}

// asgActivityToXML renders an activity. groupState is AutoScalingGroupState, reported only
// when the request asked for deleted groups' activities too.
func asgActivityToXML(a AutoScalingActivity, groupState string) asgActivityXML {
	return asgActivityXML{
		ActivityID:            a.ActivityID,
		AutoScalingGroupName:  a.GroupName,
		Description:           a.Description,
		Cause:                 a.Cause,
		StartTime:             asgTimestamp(a.StartTime),
		EndTime:               asgTimestamp(a.EndTime),
		StatusCode:            a.StatusCode,
		StatusMessage:         a.StatusMessage,
		Progress:              a.Progress,
		Details:               a.Details,
		AutoScalingGroupState: groupState,
		AutoScalingGroupARN:   a.GroupARN,
	}
}

// asgInProgressXML renders an activity as it stood when the request that started it was
// answered: InProgress, Progress 0 and no EndTime, which is TerminateInstanceInAutoScalingGroup's
// published sample. The recorded activity is the settled one, which is what
// DescribeScalingActivities reports — the same split EC2 makes between RunInstances'
// `pending` and the `running` record behind it.
func asgInProgressXML(a AutoScalingActivity) asgActivityXML {
	x := asgActivityToXML(a, "")
	x.StatusCode = "InProgress"
	x.Progress = 0
	x.EndTime = ""
	return x
}
