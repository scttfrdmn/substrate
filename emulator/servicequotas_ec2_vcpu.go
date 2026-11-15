package emulator

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strings"
)

// EC2 vCPU quotas and the AWS/Usage metrics that report against them (#1498).
//
// EC2 meters On-Demand and Spot capacity in vCPUs per quota class, not in instances: "On-
// Demand Instance quotas are managed in terms of the number of virtual central processing
// units (vCPUs) that your running On-Demand Instances are using, regardless of the instance
// type. Each quota type specifies the maximum number of vCPUs for one or more instance
// families." Spot quotas are the same, per class, in their own table. The two tables, their
// codes and their new-account defaults are on
// https://docs.aws.amazon.com/ec2/latest/instancetypes/ec2-instance-quotas.html.
//
// This file holds the one mapping both sides of that read need — instance type to quota
// class — and the usage figure derived from it. Service Quotas publishes the quota and
// names its usage metric; CloudWatch answers the metric. Both read this file, so the class
// a quota's UsageMetric names and the class the metric is counted under cannot disagree.
//
// Nothing here enforces a quota. A launch beyond one succeeds, as it always has; the quota
// is published for a consumer to read, and the usage for it to compare against.

// ec2VCPUQuotaClass is one EC2 vCPU quota class: a group of instance families that share a
// quota, in its On-Demand and its Spot form.
type ec2VCPUQuotaClass struct {
	// Name is the class as the AWS/Usage Class dimension spells it, before the slash:
	// "Standard", "F", "G", "Inf", "P" or "X".
	Name string

	// OnDemandCode and OnDemandName are the On-Demand quota's code and name.
	OnDemandCode, OnDemandName string

	// SpotCode and SpotName are the Spot quota's code and name.
	SpotCode, SpotName string

	// OnDemandDefault and SpotDefault are AWS's published new-account defaults.
	OnDemandDefault, SpotDefault float64

	// OnDemandApplied is the On-Demand quota's applied value when it differs from
	// OnDemandDefault, and nil when the applied value is the default. Only Standard sets it;
	// see [ec2StandardOnDemandAppliedValue].
	OnDemandApplied *float64
}

// onDemandApplied returns the On-Demand quota's applied value.
func (c ec2VCPUQuotaClass) onDemandApplied() float64 {
	if c.OnDemandApplied != nil {
		return *c.OnDemandApplied
	}
	return c.OnDemandDefault
}

// ec2VCPUQuotaClasses are the classes substrate publishes, in the order ListServiceQuotas
// reports them.
//
// Each code, name and default is from the On-Demand and Spot tables on the instance-type
// quotas page. The class names are the AWS/Usage Class dimension's: "the valid values are
// Standard/OnDemand, F/OnDemand, G/OnDemand, Inf/OnDemand, P/OnDemand, and X/OnDemand"
// (EC2 User Guide, "Amazon EC2 usage metrics"). That page lists the On-Demand values only;
// the Spot values take the same class with a "/Spot" suffix, the spelling a Spot fleet's
// usage is reported under (Standard/Spot is the one #1498 names).
//
// Four classes AWS also publishes are not carried: DL, HPC, High Memory and Trn. None has a
// Class value on the usage-metrics page, so a quota for one would name a UsageMetric
// substrate could not cite, and the first three have no instance type in substrate's
// catalog to count. A Trn instance — trn1 and trn2 are in the catalog — is therefore
// counted under no class.
var ec2VCPUQuotaClasses = []ec2VCPUQuotaClass{
	{
		Name:            "Standard",
		OnDemandCode:    "L-1216C47A",
		OnDemandName:    "Running On-Demand Standard (A, C, D, H, I, M, R, T, Z) instances",
		SpotCode:        "L-34B43A08",
		SpotName:        "All Standard (A, C, D, H, I, M, R, T, Z) Spot Instance Requests",
		OnDemandDefault: 5,
		OnDemandApplied: func() *float64 { v := float64(ec2StandardOnDemandAppliedValue); return &v }(),
		SpotDefault:     5,
	},
	{
		Name:         "F",
		OnDemandCode: "L-74FC7D96", OnDemandName: "Running On-Demand F instances",
		SpotCode: "L-88CF9481", SpotName: "All F Spot Instance Requests",
	},
	{
		Name:         "G",
		OnDemandCode: "L-DB2E81BA", OnDemandName: "Running On-Demand G and VT instances",
		SpotCode: "L-3819A6DF", SpotName: "All G and VT Spot Instance Requests",
	},
	{
		Name:         "Inf",
		OnDemandCode: "L-1945791B", OnDemandName: "Running On-Demand Inf instances",
		SpotCode: "L-B5D1601B", SpotName: "All Inf Spot Instance Requests",
	},
	{
		Name:         "P",
		OnDemandCode: "L-417A185B", OnDemandName: "Running On-Demand P instances",
		SpotCode: "L-7212CCBC", SpotName: "All P Spot Instance Requests",
	},
	{
		Name:         "X",
		OnDemandCode: "L-7295265B", OnDemandName: "Running On-Demand X instances",
		SpotCode: "L-E3A00192", SpotName: "All X Spot Instance Requests",
	},
}

// ec2StandardOnDemandAppliedValue is the applied value substrate has always published for
// L-1216C47A.
//
// It is not AWS's new-account default, which is 5 and is what GetAWSDefaultServiceQuota
// reports. It is kept as the applied value because consumers already read it, and because an
// applied value above the default is what a real account usually shows: EC2 "automatically
// increases your On-Demand Instance quotas based on your usage". Every other EC2 vCPU quota's
// applied value is its default.
const ec2StandardOnDemandAppliedValue = 32

// Purchase options, as the AWS/Usage Class dimension spells them after the slash.
const (
	ec2UsagePurchaseOnDemand = "OnDemand"
	ec2UsagePurchaseSpot     = "Spot"
)

// AWS/Usage identity for an EC2 vCPU usage metric.
const (
	awsUsageNamespace     = "AWS/Usage"
	awsUsageResourceCount = "ResourceCount"
)

// ec2VCPUUsageClass returns the AWS/Usage Class value for a quota class and purchase option,
// e.g. "Standard/Spot".
func ec2VCPUUsageClass(class, purchase string) string {
	return class + "/" + purchase
}

// ec2VCPUUsageDimensions returns the four dimensions of an EC2 vCPU usage metric, in the
// order the usage-metrics page lists them. It is the one spelling of them: a quota's
// UsageMetric, a ListMetrics entry and the identity check on a query all derive from it.
func ec2VCPUUsageDimensions(usageClass string) []cwDimension {
	return []cwDimension{
		{Name: "Service", Value: "EC2"},
		{Name: "Type", Value: "Resource"},
		{Name: "Resource", Value: "vCPU"},
		{Name: "Class", Value: usageClass},
	}
}

// ec2ServiceQuotasServiceName is EC2's ServiceName in Service Quotas: the name the Service
// Quotas CLI reference's get-service-quota and get-aws-default-service-quota examples show
// for service code ec2, parenthesized short name included.
const ec2ServiceQuotasServiceName = "Amazon Elastic Compute Cloud (Amazon EC2)"

// ec2VCPUServiceQuotas returns the EC2 rows of the Service Quotas table: every class's
// On-Demand quota, then every class's Spot quota.
func ec2VCPUServiceQuotas() []ServiceQuota {
	quotas := make([]ServiceQuota, 0, 2*len(ec2VCPUQuotaClasses))
	row := func(code, name string, value, def float64, usageClass string) ServiceQuota {
		q := ServiceQuota{
			ServiceCode: "ec2",
			ServiceName: ec2ServiceQuotasServiceName,
			QuotaCode:   code,
			QuotaName:   name,
			Value:       value,
			Adjustable:  true,
			Unit:        "None",
			UsageMetric: &ServiceQuotaMetric{
				MetricNamespace:               awsUsageNamespace,
				MetricName:                    awsUsageResourceCount,
				MetricDimensions:              cwDimensionMap(ec2VCPUUsageDimensions(usageClass)),
				MetricStatisticRecommendation: "Maximum",
			},
		}
		if def != value {
			d := def
			q.defaultValue = &d
		}
		return q
	}
	for _, c := range ec2VCPUQuotaClasses {
		quotas = append(quotas, row(c.OnDemandCode, c.OnDemandName, c.onDemandApplied(), c.OnDemandDefault,
			ec2VCPUUsageClass(c.Name, ec2UsagePurchaseOnDemand)))
	}
	for _, c := range ec2VCPUQuotaClasses {
		quotas = append(quotas, row(c.SpotCode, c.SpotName, c.SpotDefault, c.SpotDefault,
			ec2VCPUUsageClass(c.Name, ec2UsagePurchaseSpot)))
	}
	return quotas
}

// ec2VCPUQuotaClassOf returns the quota class an instance type counts against, or "" when it
// counts against none substrate publishes.
//
// The rule is the usage-metrics page's: "The values for this dimension define the first
// letter of the instance types that are reported by the metric. For example,
// Standard/OnDemand returns information about all running instances with types that start
// with A, C, D, H, I, M, R, T, and Z". First-letter matching alone misfiles four prefixes,
// so they are tested first: "inf" (Inf, not the I of Standard), "vt" (the G and VT quota),
// and "trn", "dl" and "hpc", which have quotas of their own that substrate does not publish.
// "u-" is High Memory, likewise unpublished, and "mac" is a Dedicated Host family with no
// vCPU quota.
func ec2VCPUQuotaClassOf(instanceType string) string {
	family, _, _ := strings.Cut(strings.ToLower(instanceType), ".")
	switch {
	case family == "":
		return ""
	case strings.HasPrefix(family, "inf"):
		return "Inf"
	case strings.HasPrefix(family, "vt"):
		return "G"
	case strings.HasPrefix(family, "trn"), strings.HasPrefix(family, "dl"),
		strings.HasPrefix(family, "hpc"), strings.HasPrefix(family, "u-"),
		strings.HasPrefix(family, "mac"):
		return ""
	}
	switch family[0] {
	case 'a', 'c', 'd', 'h', 'i', 'm', 'r', 't', 'z':
		return "Standard"
	case 'f':
		return "F"
	case 'g':
		return "G"
	case 'p':
		return "P"
	case 'x':
		return "X"
	default:
		return ""
	}
}

// ec2VCPUUsage is the account's EC2 vCPU usage in one region, by AWS/Usage Class value.
type ec2VCPUUsage struct {
	// VCPUs is the vCPU count counting against each class: running instances, and for a Spot
	// class the open requests awaiting fulfillment.
	VCPUs map[string]float64

	// Published is the set of classes that have a metric at all: those any instance or Spot
	// request in the account and region, in any state, has counted against.
	Published map[string]bool
}

// ec2UsageInstance is the part of a stored [EC2Instance] the usage derivation reads. Decoding
// into it rather than the whole record skips the interfaces, tags, user data and the rest,
// which is most of an instance's bytes.
type ec2UsageInstance struct {
	InstanceID        string           `json:"instance_id"`
	InstanceType      string           `json:"instance_type"`
	State             EC2InstanceState `json:"state"`
	InstanceLifecycle string           `json:"instanceLifecycle,omitempty"`
}

// ec2UsageSpotRequest is the part of a stored [EC2SpotInstanceRequest] the usage derivation
// reads.
type ec2UsageSpotRequest struct {
	State               string                  `json:"state"`
	InstanceID          string                  `json:"instance_id,omitempty"`
	LaunchSpecification ec2UsageSpotRequestSpec `json:"launch_specification"`
}

// ec2UsageSpotRequestSpec is the part of a request's launch specification the usage
// derivation reads.
type ec2UsageSpotRequestSpec struct {
	InstanceType string `json:"instance_type"`
}

// ec2VCPUUsageFor derives the account's vCPU usage in region from EC2's stored instances and
// Spot requests.
//
// It is a shared StateManager read rather than a routed DescribeInstances, because it is a
// derived observation and not an act: nothing is launched or changed, so there is no request
// for the registry to authorize or record, and the CloudWatch read that calls this is itself
// recorded. It reads the EC2 plugin's key shapes, "instance:{account}/{region}/{id}" and
// "spot_request:{account}/{region}/{id}".
//
// An instance counts while it reports running. "Instances that are in the pending, stopping,
// stopped, and hibernated states do not count towards your On-Demand Instance quotas", and the
// usage metric "returns information on instances that are running"; the same rule is applied
// to Spot. What an instance reports is what a DescribeInstances at this moment would report:
// the stored record's settled state, unless an instance-state seed has the instance still
// reporting its transient state (pending, stopping, shutting-down) — see
// ec2_instance_state_control.go. The seed's countdown is peeked, never advanced, so reading
// usage spends none of a test's seeded observations and a poll loop sees the same sequence
// whether or not usage was read in between.
//
// An open Spot request awaiting fulfillment counts against its Spot class too. Spot quotas
// are "the number of vCPUs that your running Spot Instances are either using or will use
// pending the fulfillment of open Spot Instance requests", and a quota's UsageMetric is "the
// CloudWatch metric that reflects quota usage", so the metric a preflight compares against the
// quota carries both. Substrate holds such a request after a capacity or price seed leaves it
// unfulfilled, or after a persistent request's replacement launch is refused. "After a pending
// Spot Instance request is fulfilled, the request no longer counts towards the quota because
// the running instance is counted", so a request with an instance counts nothing of its own. A
// request counts its launch specification's instance type once: substrate files one request
// per instance.
//
// vCPUs are the type's default vCPU count from the seeded instance-type catalog. A type the
// catalog does not carry counts nowhere, since there is no figure to count.
//
// A class is published — has a metric, and is listed by ListMetrics — once any instance or
// request has counted against it, and stays published after that instance stops or
// terminates, so its usage then reads 0 rather than vanishing. A class nothing has ever
// launched into has no metric: that is what a real account that has never used, say, P
// instances shows.
//
// The publication set is derived here rather than kept as a marker written at launch. A
// marker would need a write in every EC2 launch path and would be absent from any state a
// launch did not produce — an event log recorded before it existed, or a record a test wrote
// straight into the StateManager — so a class would read as never used while instances of it
// ran. Deriving it costs one small decode per record, which [ec2UsageInstance] keeps cheap.
func ec2VCPUUsageFor(ctx context.Context, state StateManager, accountID, region string) (ec2VCPUUsage, error) {
	usage := ec2VCPUUsage{VCPUs: map[string]float64{}, Published: map[string]bool{}}
	count := func(instanceType, purchase string, counts bool) {
		class := ec2VCPUQuotaClassOf(instanceType)
		info, known := ec2InstanceTypeIndex[instanceType]
		if class == "" || !known {
			return
		}
		usageClass := ec2VCPUUsageClass(class, purchase)
		usage.Published[usageClass] = true
		if counts {
			usage.VCPUs[usageClass] += float64(info.VCpus)
		}
	}

	err := ec2ForEachStored(ctx, state, "instance:"+accountID+"/"+region+"/", func(key string, data []byte) error {
		var inst ec2UsageInstance
		if err := json.Unmarshal(data, &inst); err != nil {
			return fmt.Errorf("ec2 vCPU usage: decode %s: %w", key, err)
		}
		reported, err := ec2PeekInstanceState(ctx, state, inst.InstanceID, inst.State)
		if err != nil {
			return err
		}
		purchase := ec2UsagePurchaseOnDemand
		if inst.InstanceLifecycle == ec2InstanceLifecycleSpot {
			purchase = ec2UsagePurchaseSpot
		}
		count(inst.InstanceType, purchase, reported.Name == ec2StateRunning)
		return nil
	})
	if err != nil {
		return usage, err
	}

	err = ec2ForEachStored(ctx, state, "spot_request:"+accountID+"/"+region+"/", func(key string, data []byte) error {
		var r ec2UsageSpotRequest
		if err := json.Unmarshal(data, &r); err != nil {
			return fmt.Errorf("ec2 vCPU usage: decode %s: %w", key, err)
		}
		if r.State == ec2SpotStateOpen && r.InstanceID == "" {
			count(r.LaunchSpecification.InstanceType, ec2UsagePurchaseSpot, true)
		}
		return nil
	})
	return usage, err
}

// ec2ForEachStored calls fn with each EC2 record under prefix, in key order, skipping a key
// deleted between the list and the read.
func ec2ForEachStored(ctx context.Context, state StateManager, prefix string, fn func(key string, data []byte) error) error {
	keys, err := state.List(ctx, ec2Namespace, prefix)
	if err != nil {
		return fmt.Errorf("ec2 vCPU usage: list %s: %w", prefix, err)
	}
	for _, key := range keys {
		data, err := state.Get(ctx, ec2Namespace, key)
		if err != nil {
			return fmt.Errorf("ec2 vCPU usage: get %s: %w", key, err)
		}
		if data == nil {
			continue
		}
		if err := fn(key, data); err != nil {
			return err
		}
	}
	return nil
}

// ec2PeekInstanceState returns the state an observation of the instance would report now,
// without spending the observation: [EC2Plugin.observeInstanceState]'s answer, through the
// progression's peek rather than its advance.
func ec2PeekInstanceState(ctx context.Context, state StateManager, instanceID string, settled EC2InstanceState) (EC2InstanceState, error) {
	seed, seen, err := ec2InstanceProgressions.peek(ctx, state, instanceID)
	if err != nil {
		return settled, fmt.Errorf("ec2 vCPU usage: peek %s: %w", instanceID, err)
	}
	if seed == nil || seed.TransientObservations <= 0 || seen >= seed.TransientObservations {
		return settled, nil
	}
	if transient, ok := ec2TransientStateFor(settled); ok {
		return transient, nil
	}
	return settled, nil
}

// ec2VCPUUsageClassFromDimensions reports the Class value of a metric identity that is
// exactly an EC2 vCPU usage metric — namespace AWS/Usage, name ResourceCount, and precisely
// the dimensions [ec2VCPUUsageDimensions] gives a Class substrate publishes, in any order.
//
// "CloudWatch treats each unique combination of dimensions as a separate metric", so a
// query naming a subset or a superset of them names a different metric, and gets none of
// this one's data.
func ec2VCPUUsageClassFromDimensions(namespace, metricName string, dims []cwDimension) (string, bool) {
	if namespace != awsUsageNamespace || metricName != awsUsageResourceCount {
		return "", false
	}
	got := cwDimensionMap(dims)
	class := got["Class"]
	if !ec2VCPUUsageClassSet[class] || len(got) != len(dims) {
		return "", false
	}
	if !maps.Equal(got, cwDimensionMap(ec2VCPUUsageDimensions(class))) {
		return "", false
	}
	return class, true
}

// ec2VCPUUsageClassNames is every Class value substrate publishes, On-Demand first, in the
// order of [ec2VCPUQuotaClasses], and ec2VCPUUsageClassSet is the same values as a set. Both
// are computed once from the table and only ever read.
var ec2VCPUUsageClassNames, ec2VCPUUsageClassSet = buildEC2VCPUUsageClassNames()

// buildEC2VCPUUsageClassNames builds [ec2VCPUUsageClassNames] and [ec2VCPUUsageClassSet].
func buildEC2VCPUUsageClassNames() ([]string, map[string]bool) {
	names := make([]string, 0, 2*len(ec2VCPUQuotaClasses))
	set := make(map[string]bool, 2*len(ec2VCPUQuotaClasses))
	for _, purchase := range []string{ec2UsagePurchaseOnDemand, ec2UsagePurchaseSpot} {
		for _, c := range ec2VCPUQuotaClasses {
			name := ec2VCPUUsageClass(c.Name, purchase)
			names = append(names, name)
			set[name] = true
		}
	}
	return names, set
}
