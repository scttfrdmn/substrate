package emulator

import (
	"sort"
	"strings"
)

// Attribute-based instance type selection, against EC2's seeded catalog (#1460).
//
// A mixed-instances group's override may name InstanceRequirements instead of an instance
// type, and Auto Scaling then chooses the types. Substrate chooses from
// [ec2InstanceTypeCatalog] — the same catalog DescribeInstanceTypes answers from — so a type
// a group launches is a type EC2 here would also describe.
//
// # What is evaluated
//
// VCpuCount, MemoryMiB, MemoryGiBPerVCpu, AcceleratorCount, AcceleratorTypes,
// CpuManufacturers, BurstablePerformance, BareMetal, LocalStorage, InstanceGenerations,
// AllowedInstanceTypes and ExcludedInstanceTypes. Each is evaluated as API_InstanceRequirements
// states it, defaults included: BurstablePerformance and BareMetal default to `excluded`,
// LocalStorage to `included`, every list member to "any".
//
// The catalog does not carry a CPU manufacturer, a burstable flag or instance storage, so
// they are derived per family here, from AWS's own pages rather than from the name alone:
//
//   - **Manufacturer** is [asgFamilyCPUManufacturer], transcribed from the processor column of
//     the general-purpose, compute-optimized, memory-optimized and accelerated-computing
//     specification pages, and the previous-generation page for p3.
//   - **Burstable** is the T series: the naming-conventions page lists "T – Burstable
//     performance", and every T3/T3a/T4g size carries the burstable footnote.
//   - **Architecture** is not an InstanceRequirements member at all, and it still narrows:
//     the reference's CpuManufacturers note says "Instances will be launched with a
//     compatible CPU architecture based on the Amazon Machine Image (AMI) that you specify
//     in your launch template". So the override's AMI — or its template's — is resolved and
//     a type is a candidate only when its supportedArchitectures carries the AMI's
//     architecture (#1497). An arm64 AMI selects only Graviton types and an x86_64 AMI only
//     Intel and AMD ones, which is also why adding the Graviton families moved no x86
//     group's choice. An AMI that does not resolve, or whose architecture no catalog type
//     reports (i386, the mac values), does not narrow: the first launch then fails on the
//     AMI itself, or the catalog lacks the families that would answer, and neither is a
//     question this filter can settle.
//   - **Local storage** is [asgFamilyHasInstanceStore], transcribed from the accelerated
//     page's instance store table rather than read off the `d` naming option, which several
//     of those families carry storage without.
//   - **Bare metal** is never true: the catalog carries no `.metal` size.
//
// # What is not, and why
//
// The three price-protection thresholds, NetworkBandwidthGbps, BaselineEbsBandwidthMbps,
// NetworkInterfaceCount, TotalLocalStorageGB, AcceleratorManufacturers, AcceleratorNames,
// AcceleratorTotalMemoryMiB and RequireHibernateSupport are accepted and not evaluated: the
// catalog holds no price, bandwidth, interface count or accelerator model to evaluate them
// against, and inventing a spec to filter on would be an observation nothing backs. The
// price thresholds are echoed by DescribeAutoScalingGroups; the rest are not.
//
// # Which matching type launches
//
// Substrate models no capacity pools and no prices, so every allocation strategy resolves the
// same way: the first candidate in priority order. Overrides are in the order the request
// listed them, which is `prioritized`'s documented meaning; the types one InstanceRequirements
// override matches are ordered by vCPU count, then memory, then name — smallest first, which
// is the order a lowest-price strategy reaches on real prices for these families, and stable
// regardless. The order is a deterministic stand-in, not a claim about which type real Auto
// Scaling would pick.

// asgFamilyCPUManufacturer is the CPU manufacturer of each family in the seeded catalog, as
// the specification pages' processor column states it ("Intel Xeon Platinum 8175", "AMD
// EPYC 7571", "2nd Gen AMD EPYC 7R32", "AWS Graviton4 Processor", …), spelled as
// API_InstanceRequirements' CpuManufacturers values ("intel | amd | amazon-web-services |
// apple"; "For instance types with AWS CPUs, specify amazon-web-services"). A family missing
// here matches no CpuManufacturers filter, which is the conservative reading for a family
// substrate cannot attribute.
func asgFamilyCPUManufacturer(family string) string {
	switch family {
	case "t3", "m5", "c5", "r5", "p3", "p4d", "p4de", "g4dn", "inf1", "trn1", "trn2":
		return "intel"
	case "t3a", "m5a", "c5a", "p5", "g5", "g6", "inf2":
		return "amd"
	case "t4g", "m6g", "m7g", "m8g", "m9g", "c6g", "c7g", "c8g", "c9g",
		"r6g", "r7g", "r8g", "r9g":
		return "amazon-web-services"
	}
	return ""
}

// asgFamilyOf returns the family half of an instance type name.
func asgFamilyOf(instanceType string) string {
	family, _, _ := strings.Cut(instanceType, ".")
	return family
}

// asgFamilyBurstable reports whether a family is burstable performance: the T series, which
// is "t" followed by its generation digit (t2, t3, t3a, t4g). A bare "t" prefix is not
// enough — Trainium's trn1 and trn2 begin with it too.
func asgFamilyBurstable(family string) bool {
	return len(family) >= 2 && family[0] == 't' && family[1] >= '0' && family[1] <= '9'
}

// asgFamilyHasInstanceStore reports whether a family carries instance store volumes, as the
// accelerated-computing page's "Instance store specifications" table lists them. The naming
// convention's `d` option is not used: g5, g6, p5, trn1 and trn2 carry NVMe instance store
// without it. None of the twenty non-accelerated families in the catalog has any — the
// Graviton `gd` variants that would are not carried — and p3, inf1 and inf2 are EBS-only.
func asgFamilyHasInstanceStore(family string) bool {
	switch family {
	case "g4dn", "g5", "g6", "p4d", "p4de", "p5", "trn1", "trn2":
		return true
	}
	return false
}

// asgIncludeExclude evaluates one included/excluded/required attribute.
func asgIncludeExclude(setting, defaultSetting string, has bool) bool {
	if setting == "" {
		setting = defaultSetting
	}
	switch setting {
	case "required":
		return has
	case "excluded":
		return !has
	default:
		return true
	}
}

// asgMatchesAny reports whether value matches any pattern, with '*' wildcards.
func asgMatchesAny(patterns []string, value string) bool {
	for _, p := range patterns {
		if ec2FilterValueMatches(p, value) {
			return true
		}
	}
	return false
}

// asgMatchInstanceTypes returns the catalog types satisfying req and able to run an AMI of
// architecture arch, in launch-priority order. An empty arch, or one no catalog type
// reports, does not narrow; see the file comment's Architecture entry.
func asgMatchInstanceTypes(req *AutoScalingInstanceRequirements, arch string) []ec2InstanceTypeInfo {
	if arch != ec2ArchX86 && arch != ec2ArchARM64 {
		arch = ""
	}
	var out []ec2InstanceTypeInfo
	for _, info := range ec2InstanceTypeCatalog {
		if arch != "" && !containsStr(info.SupportedArchs, arch) {
			continue
		}
		if asgTypeSatisfies(req, info) {
			out = append(out, info)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].VCpus != out[j].VCpus {
			return out[i].VCpus < out[j].VCpus
		}
		if out[i].MemoryMiB != out[j].MemoryMiB {
			return out[i].MemoryMiB < out[j].MemoryMiB
		}
		return out[i].InstanceType < out[j].InstanceType
	})
	return out
}

// asgTypeSatisfies reports whether one catalog entry satisfies every evaluated attribute.
func asgTypeSatisfies(req *AutoScalingInstanceRequirements, info ec2InstanceTypeInfo) bool {
	family := asgFamilyOf(info.InstanceType)
	if !req.VCpuCount.contains(float64(info.VCpus)) || !req.MemoryMiB.contains(float64(info.MemoryMiB)) {
		return false
	}
	if req.MemoryGiBPerVCpu != nil && info.VCpus > 0 &&
		!req.MemoryGiBPerVCpu.contains(float64(info.MemoryMiB)/1024/float64(info.VCpus)) {
		return false
	}
	accelerators := info.GPU + info.NeuronDevices
	if req.AcceleratorCount != nil && !req.AcceleratorCount.contains(float64(accelerators)) {
		return false
	}
	if len(req.AcceleratorTypes) > 0 {
		var kind string
		switch {
		case info.GPU > 0:
			kind = "gpu"
		case info.NeuronDevices > 0:
			kind = "inference"
		}
		if kind == "" || !containsStr(req.AcceleratorTypes, kind) {
			return false
		}
	}
	if len(req.CPUManufacturers) > 0 && !containsStr(req.CPUManufacturers, asgFamilyCPUManufacturer(family)) {
		return false
	}
	if !asgIncludeExclude(req.BurstablePerformance, "excluded", asgFamilyBurstable(family)) {
		return false
	}
	if !asgIncludeExclude(req.BareMetal, "excluded", false) {
		return false
	}
	if !asgIncludeExclude(req.LocalStorage, "included", asgFamilyHasInstanceStore(family)) {
		return false
	}
	if len(req.InstanceGenerations) > 0 {
		gen := "previous"
		if info.CurrentGeneration {
			gen = "current"
		}
		if !containsStr(req.InstanceGenerations, gen) {
			return false
		}
	}
	if len(req.AllowedInstanceTypes) > 0 && !asgMatchesAny(req.AllowedInstanceTypes, info.InstanceType) {
		return false
	}
	if len(req.ExcludedInstanceTypes) > 0 && asgMatchesAny(req.ExcludedInstanceTypes, info.InstanceType) {
		return false
	}
	return true
}

// asgTargetOnDemand is the number of a group's total instances that should be On-Demand: the
// base capacity first, then the percentage of the rest, a fraction rounded up "in favor of
// On-Demand Instances", which is the mixed-instances setup overview's rule and reproduces
// every row of its scaling-behavior table.
func asgTargetOnDemand(total int, dist AutoScalingInstancesDistribution) int {
	if total <= dist.OnDemandBaseCapacity {
		return total
	}
	above := total - dist.OnDemandBaseCapacity
	return dist.OnDemandBaseCapacity + (above*dist.OnDemandPercentageAboveBaseCapacity+99)/100
}
