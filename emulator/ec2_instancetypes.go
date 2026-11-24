package emulator

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
)

// ec2InstanceTypeInfo holds the details for one instance type in the seeded catalog.
type ec2InstanceTypeInfo struct {
	// InstanceType is the full type name, e.g. "c5.2xlarge".
	InstanceType string
	// VCpus is reported as vCpuInfo.defaultVCpus.
	VCpus int
	// MemoryMiB is reported as memoryInfo.sizeInMiB.
	MemoryMiB int
	// GPU is the accelerator count reported through gpuInfo when non-zero.
	GPU int
	// NeuronDevices is the accelerator count reported through
	// neuronInfo.neuronDevices when non-zero. It is a separate field from GPU rather
	// than one count plus a member selector because the two members are separate in
	// the response: a type reports at most one of them, and a caller reading gpuInfo
	// for an Inferentia type must get nothing (#1029).
	NeuronDevices int
	// SpotPrice is the stub spot price in USD/hour. See [ec2InstanceTypeFamilies].
	SpotPrice string
	// SupportedArchs is reported as processorInfo.supportedArchitectures.
	SupportedArchs []string
	// SupportedUsageClasses is reported as supportedUsageClasses.
	SupportedUsageClasses []string
	// CurrentGeneration is reported as currentGeneration and matched by the
	// current-generation filter. It is derived from the family name by
	// [ec2FamilyIsCurrentGeneration] rather than stored per size, because AWS publishes
	// the generation per family and no family has sizes of two generations.
	CurrentGeneration bool
}

// ec2InstanceTypeSize is one size within an [ec2InstanceTypeFamily].
type ec2InstanceTypeSize struct {
	// Size is the part of the instance type after the dot, e.g. "2xlarge".
	Size string
	// VCpus is the default vCPU count.
	VCpus int
	// MemoryMiB is the memory size in MiB.
	MemoryMiB int
	// SpotPrice is the stub spot price in USD/hour.
	SpotPrice string
}

// ec2InstanceTypeFamily describes one non-accelerated instance-type family in the seeded
// catalog. Accelerated families are [ec2AcceleratedFamily], which carries a per-size
// accelerator count this type has no room for.
type ec2InstanceTypeFamily struct {
	// Name is the family prefix, e.g. "c5".
	Name string
	// Architecture is the one value the family's types report as
	// processorInfo.supportedArchitectures: [ec2ArchX86] or [ec2ArchARM64]. It is a
	// family property because AWS publishes no family whose sizes differ in it (#1497).
	Architecture string
	// Sizes are the family's members, smallest first.
	Sizes []ec2InstanceTypeSize
}

// The two architecture values the seeded catalog's types report, spelled as
// API_ProcessorInfo's SupportedArchitectures publishes them ("Valid Values: i386 | x86_64 |
// arm64 | x86_64_mac | arm64_mac"). No family in the catalog reports the other three.
const (
	// ec2ArchX86 is the Intel and AMD families' architecture.
	ec2ArchX86 = "x86_64"
	// ec2ArchARM64 is the AWS Graviton families' architecture.
	ec2ArchARM64 = "arm64"
)

// ec2InstanceTypeFamilies is the seeded instance-type catalog, by family.
//
// The catalog is deliberately **not** exhaustive — EC2 offers some 800 types — but it is
// complete per family, which is the property that makes the rest of the modeling honest.
// [ec2CheckInstanceTypesExist] refuses a type the catalog does not carry, so a catalog
// that stopped mid-family (as the eight-type catalog #234 shipped did: c5.xlarge in,
// c5.large out) would answer InvalidInstanceType for types that plainly exist. Whole
// families mean "absent from the catalog" and "not a real instance type" line up for the
// common general-purpose, compute-optimized, memory-optimized and burstable families.
// A family substrate does not carry at all is still refused; see the doc for which.
//
// Accelerated families live in [ec2AcceleratedFamilies] and are flattened into the same
// catalog by the same function. The split is a shape difference, not a policy one: an
// accelerator count varies across the sizes of one real family, so it cannot hang off the
// family the way everything here does.
//
// vCPU and memory figures come from the AWS instance-type guides — general purpose
// (https://docs.aws.amazon.com/ec2/latest/instancetypes/gp.html), compute optimized
// (.../co.html) and memory optimized (.../mo.html). Bare-metal sizes are excluded: they
// are real types, but their specs and behavior are not modeled anywhere else in the
// plugin, so returning them would advertise fidelity that is not there.
//
// SpotPrice values are **deterministic stubs, not AWS prices**, and are not researched
// against AWS pricing — the emulator has no price feed and the values exist so a spot
// price history response has a plausible, stable number in it. Within a family they are a
// fixed rate per GiB of memory, which keeps them monotonic in size. The five values
// #234's catalog shipped for these families (t3.micro 0.0042, c5.xlarge 0.068, c5.2xlarge
// 0.136, m5.large 0.038, r5.xlarge 0.076) are preserved verbatim and every family's rate
// is calibrated to them, so no existing fixture moves; the other three are pinned the same
// way in [ec2AcceleratedFamilies]. Assert on the shape of a spot price response, never on
// the amount.
//
// **The AWS Graviton families (#1497)** follow the seven x86 ones: t4g, then the general
// purpose, compute optimized and memory optimized lines from Graviton2 to Graviton5 — m6g,
// m7g, m8g, m9g; c6g, c7g, c8g, c9g; r6g, r7g, r8g, r9g. Every size is transcribed from the
// same three guide pages, whose processor column reads "AWS Graviton2 Processor" through
// "AWS Graviton5"; the Graviton4 and Graviton5 families add 24xlarge and 48xlarge sizes the
// older ones do not have, and their `.metal-24xl`/`.metal-48xl` sizes are excluded with the
// other bare metal. They report arm64. Their stub rates carry no #234 anchor, so they are
// substrate's, set below the x86 family of the same class and rising one step per
// generation, by the rule [ec2AcceleratedFamilies] states. The `gd` (instance store) and `gn`
// (network optimized) variants of these lines follow them (#1509): m6gd–m9gd, c6gd–c9gd,
// r6gd–r9gd, c6gn, c7gn and c8gn, every one the guide pages publish.
var ec2InstanceTypeFamilies = []ec2InstanceTypeFamily{
	// Burstable, Intel. 0.0042 USD/GiB.
	{Name: "t3", Architecture: ec2ArchX86, Sizes: []ec2InstanceTypeSize{
		{"nano", 2, 512, "0.0021"},
		{"micro", 2, 1024, "0.0042"},
		{"small", 2, 2048, "0.0084"},
		{"medium", 2, 4096, "0.0168"},
		{"large", 2, 8192, "0.0336"},
		{"xlarge", 4, 16384, "0.0672"},
		{"2xlarge", 8, 32768, "0.1344"},
	}},
	// Burstable, AMD. 0.0038 USD/GiB.
	{Name: "t3a", Architecture: ec2ArchX86, Sizes: []ec2InstanceTypeSize{
		{"nano", 2, 512, "0.0019"},
		{"micro", 2, 1024, "0.0038"},
		{"small", 2, 2048, "0.0076"},
		{"medium", 2, 4096, "0.0152"},
		{"large", 2, 8192, "0.0304"},
		{"xlarge", 4, 16384, "0.0608"},
		{"2xlarge", 8, 32768, "0.1216"},
	}},
	// General purpose, Intel. 0.00475 USD/GiB.
	{Name: "m5", Architecture: ec2ArchX86, Sizes: []ec2InstanceTypeSize{
		{"large", 2, 8192, "0.038"},
		{"xlarge", 4, 16384, "0.076"},
		{"2xlarge", 8, 32768, "0.152"},
		{"4xlarge", 16, 65536, "0.304"},
		{"8xlarge", 32, 131072, "0.608"},
		{"12xlarge", 48, 196608, "0.912"},
		{"16xlarge", 64, 262144, "1.216"},
		{"24xlarge", 96, 393216, "1.824"},
	}},
	// General purpose, AMD. 0.00425 USD/GiB.
	{Name: "m5a", Architecture: ec2ArchX86, Sizes: []ec2InstanceTypeSize{
		{"large", 2, 8192, "0.034"},
		{"xlarge", 4, 16384, "0.068"},
		{"2xlarge", 8, 32768, "0.136"},
		{"4xlarge", 16, 65536, "0.272"},
		{"8xlarge", 32, 131072, "0.544"},
		{"12xlarge", 48, 196608, "0.816"},
		{"16xlarge", 64, 262144, "1.088"},
		{"24xlarge", 96, 393216, "1.632"},
	}},
	// Compute optimized, Intel. 0.0085 USD/GiB. Note the size ladder is not shared with
	// c5a: c5 has 9xlarge and 18xlarge where c5a has 8xlarge and 16xlarge.
	{Name: "c5", Architecture: ec2ArchX86, Sizes: []ec2InstanceTypeSize{
		{"large", 2, 4096, "0.034"},
		{"xlarge", 4, 8192, "0.068"},
		{"2xlarge", 8, 16384, "0.136"},
		{"4xlarge", 16, 32768, "0.272"},
		{"9xlarge", 36, 73728, "0.612"},
		{"12xlarge", 48, 98304, "0.816"},
		{"18xlarge", 72, 147456, "1.224"},
		{"24xlarge", 96, 196608, "1.632"},
	}},
	// Compute optimized, AMD. 0.00765 USD/GiB.
	{Name: "c5a", Architecture: ec2ArchX86, Sizes: []ec2InstanceTypeSize{
		{"large", 2, 4096, "0.0306"},
		{"xlarge", 4, 8192, "0.0612"},
		{"2xlarge", 8, 16384, "0.1224"},
		{"4xlarge", 16, 32768, "0.2448"},
		{"8xlarge", 32, 65536, "0.4896"},
		{"12xlarge", 48, 98304, "0.7344"},
		{"16xlarge", 64, 131072, "0.9792"},
		{"24xlarge", 96, 196608, "1.4688"},
	}},
	// Memory optimized, Intel. 0.002375 USD/GiB.
	{Name: "r5", Architecture: ec2ArchX86, Sizes: []ec2InstanceTypeSize{
		{"large", 2, 16384, "0.038"},
		{"xlarge", 4, 32768, "0.076"},
		{"2xlarge", 8, 65536, "0.152"},
		{"4xlarge", 16, 131072, "0.304"},
		{"8xlarge", 32, 262144, "0.608"},
		{"12xlarge", 48, 393216, "0.912"},
		{"16xlarge", 64, 524288, "1.216"},
		{"24xlarge", 96, 786432, "1.824"},
	}},
	// Burstable, AWS Graviton2. 0.0034 USD/GiB.
	{Name: "t4g", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"nano", 2, 512, "0.0017"},
		{"micro", 2, 1024, "0.0034"},
		{"small", 2, 2048, "0.0068"},
		{"medium", 2, 4096, "0.0136"},
		{"large", 2, 8192, "0.0272"},
		{"xlarge", 4, 16384, "0.0544"},
		{"2xlarge", 8, 32768, "0.1088"},
	}},
	// General purpose, AWS Graviton2. 0.004 USD/GiB.
	{Name: "m6g", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 4096, "0.016"},
		{"large", 2, 8192, "0.032"},
		{"xlarge", 4, 16384, "0.064"},
		{"2xlarge", 8, 32768, "0.128"},
		{"4xlarge", 16, 65536, "0.256"},
		{"8xlarge", 32, 131072, "0.512"},
		{"12xlarge", 48, 196608, "0.768"},
		{"16xlarge", 64, 262144, "1.024"},
	}},
	// General purpose, AWS Graviton3. 0.0042 USD/GiB.
	{Name: "m7g", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 4096, "0.0168"},
		{"large", 2, 8192, "0.0336"},
		{"xlarge", 4, 16384, "0.0672"},
		{"2xlarge", 8, 32768, "0.1344"},
		{"4xlarge", 16, 65536, "0.2688"},
		{"8xlarge", 32, 131072, "0.5376"},
		{"12xlarge", 48, 196608, "0.8064"},
		{"16xlarge", 64, 262144, "1.0752"},
	}},
	// General purpose, AWS Graviton4. 0.0044 USD/GiB.
	{Name: "m8g", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 4096, "0.0176"},
		{"large", 2, 8192, "0.0352"},
		{"xlarge", 4, 16384, "0.0704"},
		{"2xlarge", 8, 32768, "0.1408"},
		{"4xlarge", 16, 65536, "0.2816"},
		{"8xlarge", 32, 131072, "0.5632"},
		{"12xlarge", 48, 196608, "0.8448"},
		{"16xlarge", 64, 262144, "1.1264"},
		{"24xlarge", 96, 393216, "1.6896"},
		{"48xlarge", 192, 786432, "3.3792"},
	}},
	// General purpose, AWS Graviton5. 0.0046 USD/GiB.
	{Name: "m9g", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 4096, "0.0184"},
		{"large", 2, 8192, "0.0368"},
		{"xlarge", 4, 16384, "0.0736"},
		{"2xlarge", 8, 32768, "0.1472"},
		{"4xlarge", 16, 65536, "0.2944"},
		{"8xlarge", 32, 131072, "0.5888"},
		{"12xlarge", 48, 196608, "0.8832"},
		{"16xlarge", 64, 262144, "1.1776"},
		{"24xlarge", 96, 393216, "1.7664"},
		{"48xlarge", 192, 786432, "3.5328"},
	}},
	// Compute optimized, AWS Graviton2. 0.0072 USD/GiB.
	{Name: "c6g", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 2048, "0.0144"},
		{"large", 2, 4096, "0.0288"},
		{"xlarge", 4, 8192, "0.0576"},
		{"2xlarge", 8, 16384, "0.1152"},
		{"4xlarge", 16, 32768, "0.2304"},
		{"8xlarge", 32, 65536, "0.4608"},
		{"12xlarge", 48, 98304, "0.6912"},
		{"16xlarge", 64, 131072, "0.9216"},
	}},
	// Compute optimized, AWS Graviton3. 0.0076 USD/GiB.
	{Name: "c7g", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 2048, "0.0152"},
		{"large", 2, 4096, "0.0304"},
		{"xlarge", 4, 8192, "0.0608"},
		{"2xlarge", 8, 16384, "0.1216"},
		{"4xlarge", 16, 32768, "0.2432"},
		{"8xlarge", 32, 65536, "0.4864"},
		{"12xlarge", 48, 98304, "0.7296"},
		{"16xlarge", 64, 131072, "0.9728"},
	}},
	// Compute optimized, AWS Graviton4. 0.008 USD/GiB.
	{Name: "c8g", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 2048, "0.016"},
		{"large", 2, 4096, "0.032"},
		{"xlarge", 4, 8192, "0.064"},
		{"2xlarge", 8, 16384, "0.128"},
		{"4xlarge", 16, 32768, "0.256"},
		{"8xlarge", 32, 65536, "0.512"},
		{"12xlarge", 48, 98304, "0.768"},
		{"16xlarge", 64, 131072, "1.024"},
		{"24xlarge", 96, 196608, "1.536"},
		{"48xlarge", 192, 393216, "3.072"},
	}},
	// Compute optimized, AWS Graviton5. 0.0084 USD/GiB.
	{Name: "c9g", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 2048, "0.0168"},
		{"large", 2, 4096, "0.0336"},
		{"xlarge", 4, 8192, "0.0672"},
		{"2xlarge", 8, 16384, "0.1344"},
		{"4xlarge", 16, 32768, "0.2688"},
		{"8xlarge", 32, 65536, "0.5376"},
		{"12xlarge", 48, 98304, "0.8064"},
		{"16xlarge", 64, 131072, "1.0752"},
		{"24xlarge", 96, 196608, "1.6128"},
		{"48xlarge", 192, 393216, "3.2256"},
	}},
	// Memory optimized, AWS Graviton2. 0.002 USD/GiB.
	{Name: "r6g", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 8192, "0.016"},
		{"large", 2, 16384, "0.032"},
		{"xlarge", 4, 32768, "0.064"},
		{"2xlarge", 8, 65536, "0.128"},
		{"4xlarge", 16, 131072, "0.256"},
		{"8xlarge", 32, 262144, "0.512"},
		{"12xlarge", 48, 393216, "0.768"},
		{"16xlarge", 64, 524288, "1.024"},
	}},
	// Memory optimized, AWS Graviton3. 0.0021 USD/GiB.
	{Name: "r7g", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 8192, "0.0168"},
		{"large", 2, 16384, "0.0336"},
		{"xlarge", 4, 32768, "0.0672"},
		{"2xlarge", 8, 65536, "0.1344"},
		{"4xlarge", 16, 131072, "0.2688"},
		{"8xlarge", 32, 262144, "0.5376"},
		{"12xlarge", 48, 393216, "0.8064"},
		{"16xlarge", 64, 524288, "1.0752"},
	}},
	// Memory optimized, AWS Graviton4. 0.0022 USD/GiB.
	{Name: "r8g", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 8192, "0.0176"},
		{"large", 2, 16384, "0.0352"},
		{"xlarge", 4, 32768, "0.0704"},
		{"2xlarge", 8, 65536, "0.1408"},
		{"4xlarge", 16, 131072, "0.2816"},
		{"8xlarge", 32, 262144, "0.5632"},
		{"12xlarge", 48, 393216, "0.8448"},
		{"16xlarge", 64, 524288, "1.1264"},
		{"24xlarge", 96, 786432, "1.6896"},
		{"48xlarge", 192, 1572864, "3.3792"},
	}},
	// Memory optimized, AWS Graviton5. 0.0023 USD/GiB.
	{Name: "r9g", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 8192, "0.0184"},
		{"large", 2, 16384, "0.0368"},
		{"xlarge", 4, 32768, "0.0736"},
		{"2xlarge", 8, 65536, "0.1472"},
		{"4xlarge", 16, 131072, "0.2944"},
		{"8xlarge", 32, 262144, "0.5888"},
		{"12xlarge", 48, 393216, "0.8832"},
		{"16xlarge", 64, 524288, "1.1776"},
		{"24xlarge", 96, 786432, "1.7664"},
		{"48xlarge", 192, 1572864, "3.5328"},
	}},
	// The Graviton gd (instance store) and gn (network optimized) variants (#1509), transcribed
	// from the same three guide pages. Their vCPU and memory figures equal their base family's
	// size for size — the pages publish them that way, the gn lines included — and the
	// Graviton4 and Graviton5 lines add 24xlarge and 48xlarge as their base families do.
	// Every gd size carries NVMe SSD instance store ("1 x 59 GB" on a medium upward, per the
	// instance store tables) and every gn size is EBS-only ("Instance store not supported");
	// the catalog reports neither, and [asgFamilyHasInstanceStore] is where the difference is
	// read. Bare metal is excluded as everywhere. Stub rates are substrate's, a step above the
	// base family's.
	// General purpose, AWS Graviton2, NVMe instance store. 0.0047 USD/GiB.
	{Name: "m6gd", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 4096, "0.0188"},
		{"large", 2, 8192, "0.0376"},
		{"xlarge", 4, 16384, "0.0752"},
		{"2xlarge", 8, 32768, "0.1504"},
		{"4xlarge", 16, 65536, "0.3008"},
		{"8xlarge", 32, 131072, "0.6016"},
		{"12xlarge", 48, 196608, "0.9024"},
		{"16xlarge", 64, 262144, "1.2032"},
	}},
	// General purpose, AWS Graviton3, NVMe instance store. 0.0049 USD/GiB.
	{Name: "m7gd", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 4096, "0.0196"},
		{"large", 2, 8192, "0.0392"},
		{"xlarge", 4, 16384, "0.0784"},
		{"2xlarge", 8, 32768, "0.1568"},
		{"4xlarge", 16, 65536, "0.3136"},
		{"8xlarge", 32, 131072, "0.6272"},
		{"12xlarge", 48, 196608, "0.9408"},
		{"16xlarge", 64, 262144, "1.2544"},
	}},
	// General purpose, AWS Graviton4, NVMe instance store. 0.0051 USD/GiB.
	{Name: "m8gd", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 4096, "0.0204"},
		{"large", 2, 8192, "0.0408"},
		{"xlarge", 4, 16384, "0.0816"},
		{"2xlarge", 8, 32768, "0.1632"},
		{"4xlarge", 16, 65536, "0.3264"},
		{"8xlarge", 32, 131072, "0.6528"},
		{"12xlarge", 48, 196608, "0.9792"},
		{"16xlarge", 64, 262144, "1.3056"},
		{"24xlarge", 96, 393216, "1.9584"},
		{"48xlarge", 192, 786432, "3.9168"},
	}},
	// General purpose, AWS Graviton5, NVMe instance store. 0.0053 USD/GiB.
	{Name: "m9gd", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 4096, "0.0212"},
		{"large", 2, 8192, "0.0424"},
		{"xlarge", 4, 16384, "0.0848"},
		{"2xlarge", 8, 32768, "0.1696"},
		{"4xlarge", 16, 65536, "0.3392"},
		{"8xlarge", 32, 131072, "0.6784"},
		{"12xlarge", 48, 196608, "1.0176"},
		{"16xlarge", 64, 262144, "1.3568"},
		{"24xlarge", 96, 393216, "2.0352"},
		{"48xlarge", 192, 786432, "4.0704"},
	}},
	// Compute optimized, AWS Graviton2, NVMe instance store. 0.0082 USD/GiB.
	{Name: "c6gd", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 2048, "0.0164"},
		{"large", 2, 4096, "0.0328"},
		{"xlarge", 4, 8192, "0.0656"},
		{"2xlarge", 8, 16384, "0.1312"},
		{"4xlarge", 16, 32768, "0.2624"},
		{"8xlarge", 32, 65536, "0.5248"},
		{"12xlarge", 48, 98304, "0.7872"},
		{"16xlarge", 64, 131072, "1.0496"},
	}},
	// Compute optimized, AWS Graviton3, NVMe instance store. 0.0086 USD/GiB.
	{Name: "c7gd", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 2048, "0.0172"},
		{"large", 2, 4096, "0.0344"},
		{"xlarge", 4, 8192, "0.0688"},
		{"2xlarge", 8, 16384, "0.1376"},
		{"4xlarge", 16, 32768, "0.2752"},
		{"8xlarge", 32, 65536, "0.5504"},
		{"12xlarge", 48, 98304, "0.8256"},
		{"16xlarge", 64, 131072, "1.1008"},
	}},
	// Compute optimized, AWS Graviton4, NVMe instance store. 0.009 USD/GiB.
	{Name: "c8gd", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 2048, "0.018"},
		{"large", 2, 4096, "0.036"},
		{"xlarge", 4, 8192, "0.072"},
		{"2xlarge", 8, 16384, "0.144"},
		{"4xlarge", 16, 32768, "0.288"},
		{"8xlarge", 32, 65536, "0.576"},
		{"12xlarge", 48, 98304, "0.864"},
		{"16xlarge", 64, 131072, "1.152"},
		{"24xlarge", 96, 196608, "1.728"},
		{"48xlarge", 192, 393216, "3.456"},
	}},
	// Compute optimized, AWS Graviton5, NVMe instance store. 0.0094 USD/GiB.
	{Name: "c9gd", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 2048, "0.0188"},
		{"large", 2, 4096, "0.0376"},
		{"xlarge", 4, 8192, "0.0752"},
		{"2xlarge", 8, 16384, "0.1504"},
		{"4xlarge", 16, 32768, "0.3008"},
		{"8xlarge", 32, 65536, "0.6016"},
		{"12xlarge", 48, 98304, "0.9024"},
		{"16xlarge", 64, 131072, "1.2032"},
		{"24xlarge", 96, 196608, "1.8048"},
		{"48xlarge", 192, 393216, "3.6096"},
	}},
	// Compute optimized, network optimized, AWS Graviton2. EBS only. 0.0088 USD/GiB.
	{Name: "c6gn", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 2048, "0.0176"},
		{"large", 2, 4096, "0.0352"},
		{"xlarge", 4, 8192, "0.0704"},
		{"2xlarge", 8, 16384, "0.1408"},
		{"4xlarge", 16, 32768, "0.2816"},
		{"8xlarge", 32, 65536, "0.5632"},
		{"12xlarge", 48, 98304, "0.8448"},
		{"16xlarge", 64, 131072, "1.1264"},
	}},
	// Compute optimized, network optimized, AWS Graviton3E. EBS only. 0.0092 USD/GiB.
	{Name: "c7gn", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 2048, "0.0184"},
		{"large", 2, 4096, "0.0368"},
		{"xlarge", 4, 8192, "0.0736"},
		{"2xlarge", 8, 16384, "0.1472"},
		{"4xlarge", 16, 32768, "0.2944"},
		{"8xlarge", 32, 65536, "0.5888"},
		{"12xlarge", 48, 98304, "0.8832"},
		{"16xlarge", 64, 131072, "1.1776"},
	}},
	// Compute optimized, network optimized, AWS Graviton4. EBS only. 0.0096 USD/GiB.
	{Name: "c8gn", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 2048, "0.0192"},
		{"large", 2, 4096, "0.0384"},
		{"xlarge", 4, 8192, "0.0768"},
		{"2xlarge", 8, 16384, "0.1536"},
		{"4xlarge", 16, 32768, "0.3072"},
		{"8xlarge", 32, 65536, "0.6144"},
		{"12xlarge", 48, 98304, "0.9216"},
		{"16xlarge", 64, 131072, "1.2288"},
		{"24xlarge", 96, 196608, "1.8432"},
		{"48xlarge", 192, 393216, "3.6864"},
	}},
	// Memory optimized, AWS Graviton2, NVMe instance store. 0.0024 USD/GiB.
	{Name: "r6gd", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 8192, "0.0192"},
		{"large", 2, 16384, "0.0384"},
		{"xlarge", 4, 32768, "0.0768"},
		{"2xlarge", 8, 65536, "0.1536"},
		{"4xlarge", 16, 131072, "0.3072"},
		{"8xlarge", 32, 262144, "0.6144"},
		{"12xlarge", 48, 393216, "0.9216"},
		{"16xlarge", 64, 524288, "1.2288"},
	}},
	// Memory optimized, AWS Graviton3, NVMe instance store. 0.0025 USD/GiB.
	{Name: "r7gd", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 8192, "0.02"},
		{"large", 2, 16384, "0.04"},
		{"xlarge", 4, 32768, "0.08"},
		{"2xlarge", 8, 65536, "0.16"},
		{"4xlarge", 16, 131072, "0.32"},
		{"8xlarge", 32, 262144, "0.64"},
		{"12xlarge", 48, 393216, "0.96"},
		{"16xlarge", 64, 524288, "1.28"},
	}},
	// Memory optimized, AWS Graviton4, NVMe instance store. 0.0026 USD/GiB.
	{Name: "r8gd", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 8192, "0.0208"},
		{"large", 2, 16384, "0.0416"},
		{"xlarge", 4, 32768, "0.0832"},
		{"2xlarge", 8, 65536, "0.1664"},
		{"4xlarge", 16, 131072, "0.3328"},
		{"8xlarge", 32, 262144, "0.6656"},
		{"12xlarge", 48, 393216, "0.9984"},
		{"16xlarge", 64, 524288, "1.3312"},
		{"24xlarge", 96, 786432, "1.9968"},
		{"48xlarge", 192, 1572864, "3.9936"},
	}},
	// Memory optimized, AWS Graviton5, NVMe instance store. 0.0027 USD/GiB.
	{Name: "r9gd", Architecture: ec2ArchARM64, Sizes: []ec2InstanceTypeSize{
		{"medium", 1, 8192, "0.0216"},
		{"large", 2, 16384, "0.0432"},
		{"xlarge", 4, 32768, "0.0864"},
		{"2xlarge", 8, 65536, "0.1728"},
		{"4xlarge", 16, 131072, "0.3456"},
		{"8xlarge", 32, 262144, "0.6912"},
		{"12xlarge", 48, 393216, "1.0368"},
		{"16xlarge", 64, 524288, "1.3824"},
		{"24xlarge", 96, 786432, "2.0736"},
		{"48xlarge", 192, 1572864, "4.1472"},
	}},
}

// ec2AcceleratedSize is one size within an [ec2AcceleratedFamily].
//
// It carries an accelerator count where [ec2InstanceTypeSize] does not, because the count
// is not a property of the family and cannot be hoisted onto one: g5.12xlarge carries four
// accelerators and g5.16xlarge, the next size up, carries one.
type ec2AcceleratedSize struct {
	// Size is the part of the instance type after the dot, e.g. "12xlarge".
	Size string
	// VCpus is the default vCPU count.
	VCpus int
	// MemoryMiB is the memory size in MiB.
	MemoryMiB int
	// Accelerators is the number of accelerator devices the size carries. Which response
	// member reports it is the family's property, not the size's; see
	// [ec2AcceleratedFamily.Reports].
	Accelerators int
	// SpotPrice is the stub spot price in USD/hour.
	SpotPrice string
}

// ec2AcceleratorMember is the InstanceTypeInfo member a family's accelerator count is
// reported through. Its value is the member's own wire name, so a message or a doc can
// name it without a second table.
//
// The zero value reports the count nowhere, which is deliberately what a family whose
// member substrate does not model must do: DescribeInstanceTypes splits accelerators
// across five members (gpuInfo, neuronInfo, inferenceAcceleratorInfo, fpgaInfo,
// mediaAcceleratorInfo), and a family added later must report nothing until somebody
// decides which of them AWS populates for it, rather than defaulting into whichever
// one happens to be modeled.
type ec2AcceleratorMember string

const (
	// ec2AcceleratorNone reports the count through no member at all.
	ec2AcceleratorNone ec2AcceleratorMember = ""
	// ec2AcceleratorGPU reports the count through gpuInfo.gpus.
	ec2AcceleratorGPU ec2AcceleratorMember = "gpuInfo"
	// ec2AcceleratorNeuron reports the count through neuronInfo.neuronDevices.
	ec2AcceleratorNeuron ec2AcceleratorMember = "neuronInfo"
)

// ec2AcceleratedFamily describes one accelerated-computing family in the seeded catalog.
type ec2AcceleratedFamily struct {
	// Name is the family prefix, e.g. "g5".
	Name string
	// Architecture is the one value the family's types report as
	// processorInfo.supportedArchitectures, as [ec2InstanceTypeFamily.Architecture] is.
	// Every accelerated family is x86_64 except g5g, the AWS Graviton2 family with NVIDIA
	// T4g GPUs (#1509).
	Architecture string
	// Reports is the response member DescribeInstanceTypes reports this family's
	// accelerator count through. See [ec2AcceleratedFamilies] for the five members AWS
	// splits accelerators across and which of them are modeled.
	Reports ec2AcceleratorMember
	// Sizes are the family's members, smallest first.
	Sizes []ec2AcceleratedSize
}

// ec2AcceleratedFamilies is the accelerated-computing half of the seeded instance-type
// catalog. [buildEC2InstanceTypeCatalog] flattens it into the same catalog and index as
// [ec2InstanceTypeFamilies].
//
// Every family here is complete, which is the invariant [ec2InstanceTypeFamilies] documents
// and the one that makes [ec2CheckInstanceTypesExist]'s refusal honest: a consumer probing
// g6.xlarge or p3.8xlarge reaches the operation's own logic rather than InvalidInstanceType
// (#891, #892, #894, #896).
//
// vCPU, memory and accelerator counts come from the AWS accelerated-computing instance-type
// guide (https://docs.aws.amazon.com/ec2/latest/instancetypes/ac.html), except p3's, which
// AWS publishes on the previous-generation page
// (https://docs.aws.amazon.com/ec2/latest/instancetypes/pg.html) — p3 is the one family
// here AWS lists as previous generation, which is also where its currentGeneration value
// comes from (#1028); see [ec2PreviousGenerationFamilies].
//
// Bare metal is excluded as it is everywhere else in the catalog, which costs this table one
// size: g4dn.metal. The families deliberately absent are the ones whose specs could not be
// pinned or that AWS lists as their own family: p3dn, trn1n, p5e and p5en are separate
// single-size families AWS publishes beside the ones here, and trn2u.48xlarge is published
// with **no** accelerator count at all, which is almost certainly an AWS documentation gap
// rather than a zero-accelerator instance — inferring 16 from trn2.48xlarge would be
// substrate inventing a spec. A family absent from the catalog is still refused, so
// widening later is additive.
//
// **Which member reports which family's count.** DescribeInstanceTypes' InstanceTypeInfo
// shape has five separate accelerator members: gpuInfo ("Describes the GPU accelerator
// settings for the instance type"), neuronInfo, inferenceAcceleratorInfo, fpgaInfo and
// mediaAcceleratorInfo. Substrate models the first two, so the NVIDIA families report
// through gpuInfo and the Inferentia and Trainium families through neuronInfo (#1029).
//
// The split between the two Inferentia-capable members is decided by AWS's own note on
// API_InferenceAcceleratorInfo — "Amazon Elastic Inference is no longer available" — which
// is what that member describes, leaving neuronInfo as the live member for a device the
// Neuron SDK drives. Real EC2 does not report Inferentia through gpuInfo (#234), and the
// count is reported through neuronInfo.
//
// Of NeuronDeviceInfo's four members only `count` is reported. `name`, `coreInfo` and
// `memoryInfo`, and NeuronInfo's own `totalNeuronDeviceMemoryInMiB`, are omitted because
// AWS publishes no valid values and no example for any of them — `name` is documented as
// "The name of the neuron accelerator" and nothing more — so a value substrate emitted
// there would be its own invention in a member a consumer can match on. An omitted member
// is honestly empty (#1013); an invented one is not.
//
// SpotPrice follows [ec2InstanceTypeFamilies]' rule — a fixed rate per GiB of memory within
// a family, deterministic stub, never an AWS price. The three #234 values are preserved
// verbatim and each family's rate is calibrated to them where one exists: g4dn 0.01175
// USD/GiB from g4dn.xlarge 0.188, inf1 0.0095 from inf1.xlarge 0.076, and p3 0.918 per 61
// GiB, which makes every p3 size an exact multiple of the seeded p3.2xlarge value. The rates
// for the families with no seeded value are substrate's, ordered so a newer generation costs
// more per GiB than the one it replaces.
var ec2AcceleratedFamilies = []ec2AcceleratedFamily{
	// NVIDIA V100. Previous generation, and the only family in either table that is, so the
	// only one whose types report currentGeneration false. 0.918/61 GiB.
	{Name: "p3", Architecture: ec2ArchX86, Reports: ec2AcceleratorGPU, Sizes: []ec2AcceleratedSize{
		{"2xlarge", 8, 62464, 1, "0.918"},
		{"8xlarge", 32, 249856, 4, "3.672"},
		{"16xlarge", 64, 499712, 8, "7.344"},
	}},
	// NVIDIA A100 40 GiB. One size is the whole family. 0.0175 USD/GiB.
	{Name: "p4d", Architecture: ec2ArchX86, Reports: ec2AcceleratorGPU, Sizes: []ec2AcceleratedSize{
		{"24xlarge", 96, 1179648, 8, "20.16"},
	}},
	// NVIDIA A100 80 GiB — AWS's own family, not a p4d size. 0.0195 USD/GiB.
	{Name: "p4de", Architecture: ec2ArchX86, Reports: ec2AcceleratorGPU, Sizes: []ec2AcceleratedSize{
		{"24xlarge", 96, 1179648, 8, "22.464"},
	}},
	// NVIDIA H100. 0.0225 USD/GiB.
	{Name: "p5", Architecture: ec2ArchX86, Reports: ec2AcceleratorGPU, Sizes: []ec2AcceleratedSize{
		{"4xlarge", 16, 262144, 1, "5.76"},
		{"48xlarge", 192, 2097152, 8, "46.08"},
	}},
	// NVIDIA T4. 0.01175 USD/GiB. The accelerator count is not monotonic in size and AWS
	// publishes it that way: the 12xlarge carries four and the 16xlarge one.
	{Name: "g4dn", Architecture: ec2ArchX86, Reports: ec2AcceleratorGPU, Sizes: []ec2AcceleratedSize{
		{"xlarge", 4, 16384, 1, "0.188"},
		{"2xlarge", 8, 32768, 1, "0.376"},
		{"4xlarge", 16, 65536, 1, "0.752"},
		{"8xlarge", 32, 131072, 1, "1.504"},
		{"12xlarge", 48, 196608, 4, "2.256"},
		{"16xlarge", 64, 262144, 1, "3.008"},
	}},
	// NVIDIA A10G. 0.0125 USD/GiB. Same non-monotonic count as g4dn, plus a 24xlarge that
	// carries four where the 48xlarge carries eight.
	{Name: "g5", Architecture: ec2ArchX86, Reports: ec2AcceleratorGPU, Sizes: []ec2AcceleratedSize{
		{"xlarge", 4, 16384, 1, "0.2"},
		{"2xlarge", 8, 32768, 1, "0.4"},
		{"4xlarge", 16, 65536, 1, "0.8"},
		{"8xlarge", 32, 131072, 1, "1.6"},
		{"12xlarge", 48, 196608, 4, "2.4"},
		{"16xlarge", 64, 262144, 1, "3.2"},
		{"24xlarge", 96, 393216, 4, "4.8"},
		{"48xlarge", 192, 786432, 8, "9.6"},
	}},
	// NVIDIA L4. 0.013 USD/GiB. g6.xlarge is the type #891, #892 and #894 probe.
	{Name: "g6", Architecture: ec2ArchX86, Reports: ec2AcceleratorGPU, Sizes: []ec2AcceleratedSize{
		{"xlarge", 4, 16384, 1, "0.208"},
		{"2xlarge", 8, 32768, 1, "0.416"},
		{"4xlarge", 16, 65536, 1, "0.832"},
		{"8xlarge", 32, 131072, 1, "1.664"},
		{"12xlarge", 48, 196608, 4, "2.496"},
		{"16xlarge", 64, 262144, 1, "3.328"},
		{"24xlarge", 96, 393216, 4, "4.992"},
		{"48xlarge", 192, 786432, 8, "9.984"},
	}},
	// AWS Inferentia. 0.0095 USD/GiB. Not reported through gpuInfo; see above.
	{Name: "inf1", Architecture: ec2ArchX86, Reports: ec2AcceleratorNeuron, Sizes: []ec2AcceleratedSize{
		{"xlarge", 4, 8192, 1, "0.076"},
		{"2xlarge", 8, 16384, 1, "0.152"},
		{"6xlarge", 24, 49152, 4, "0.456"},
		{"24xlarge", 96, 196608, 16, "1.824"},
	}},
	// AWS Inferentia2. 0.0105 USD/GiB. The count runs 1, 1, 6, 12 — not powers of two.
	{Name: "inf2", Architecture: ec2ArchX86, Reports: ec2AcceleratorNeuron, Sizes: []ec2AcceleratedSize{
		{"xlarge", 4, 16384, 1, "0.168"},
		{"8xlarge", 32, 131072, 1, "1.344"},
		{"24xlarge", 96, 393216, 6, "4.032"},
		{"48xlarge", 192, 786432, 12, "8.064"},
	}},
	// AWS Trainium. 0.0115 USD/GiB.
	{Name: "trn1", Architecture: ec2ArchX86, Reports: ec2AcceleratorNeuron, Sizes: []ec2AcceleratedSize{
		{"2xlarge", 8, 32768, 1, "0.368"},
		{"32xlarge", 128, 524288, 16, "5.888"},
	}},
	// AWS Trainium2. 0.0135 USD/GiB. The small size is a 3xlarge, not a 2xlarge.
	{Name: "trn2", Architecture: ec2ArchX86, Reports: ec2AcceleratorNeuron, Sizes: []ec2AcceleratedSize{
		{"3xlarge", 12, 131072, 1, "1.728"},
		{"48xlarge", 192, 2097152, 16, "27.648"},
	}},
	// NVIDIA T4G on AWS Graviton2 — the catalog's one arm64 accelerated family (#1509), from
	// the accelerated-computing guide: 1 GPU on every size up to 8xlarge and 2 on the 16xlarge,
	// EBS only ("Instance store not supported"). g5g.metal is excluded with the other bare
	// metal. 0.0105 USD/GiB, below g4dn's T4 rate.
	{Name: "g5g", Architecture: ec2ArchARM64, Reports: ec2AcceleratorGPU, Sizes: []ec2AcceleratedSize{
		{"xlarge", 4, 8192, 1, "0.084"},
		{"2xlarge", 8, 16384, 1, "0.168"},
		{"4xlarge", 16, 32768, 1, "0.336"},
		{"8xlarge", 32, 65536, 1, "0.672"},
		{"16xlarge", 64, 131072, 2, "1.344"},
	}},
}

// ec2PreviousGenerationFamilies are the fifteen instance families AWS publishes as previous
// generation, in the order its own table lists them.
//
// Source: the EC2 Instance Types guide's "Specifications for Amazon EC2 previous generation
// instances" (https://docs.aws.amazon.com/ec2/latest/instancetypes/pg.html), whose
// Instance family / Available instance types table is the only AWS *documentation* that
// enumerates them — API_DescribeInstanceTypes says of currentGeneration only "Indicates
// whether the instance type is current generation", naming no family.
//
// There is a second AWS list and it is not this one. https://aws.amazon.com/ec2/previous-generation/
// is narrower — it omits P3 and P3dn entirely, names G2 where the guide names G3, adds C2, CR1
// and HS1, and lists M4, R4, D2, I3 and G3 as *upgrade targets*, i.e. current. That page is about
// hardware AWS is steering customers off, not about what the API reports; taking it as the source
// would classify every type in substrate's catalog as current generation and leave the
// hardcoded true of #1028 looking correct. The guide is the citable one.
//
// Eleven of the fifteen are families the catalog does not carry at all, and P3dn is carried by
// neither. They are transcribed anyway, whole: the list is a copy of one AWS table rather than
// the intersection with [ec2InstanceTypeFamilies], so a family added to the catalog later is
// classified by AWS's answer instead of by whoever adds it remembering to look. Intersected
// with the catalog's forty-seven families today it selects exactly p3 — three of 349 types report
// false and 346 report true. (a1, the first Graviton family, is on the list and not in the
// catalog; the thirteen Graviton families #1497 added and the sixteen #1509 added are all
// current.)
var ec2PreviousGenerationFamilies = []string{ //nolint:gochecknoglobals // read-only reference data transcribed from one AWS page
	"a1", "c1", "c3", "c4", "g3", "i2", "m1", "m2", "m3", "m4",
	"p3", "p3dn", "r3", "r4", "t1",
}

// ec2FamilyIsCurrentGeneration reports whether DescribeInstanceTypes reports a family's types
// with currentGeneration true.
//
// Current generation is the default and previous generation is the enumerated exception, which
// is the direction AWS publishes: there is a page listing the previous generations and none
// listing the current ones, because the current set changes with every launch. A family the
// catalog carries that AWS has not published as previous generation is therefore current, and
// a family nobody has classified reads as current rather than as unknown — the same answer AWS
// gives for a type launched after its previous-generation page was last edited.
func ec2FamilyIsCurrentGeneration(family string) bool {
	return !slices.Contains(ec2PreviousGenerationFamilies, family)
}

// ec2InstanceTypeCatalog is the flattened [ec2InstanceTypeFamilies] followed by the
// flattened [ec2AcceleratedFamilies], in family and then size order.
// ec2InstanceTypeIndex is the same data keyed by type name.
//
// Both are built by one function so a type can never be in one and not the other: a catalog with a
// parallel spot-price map would silently drop a type missing from the map from
// DescribeSpotPriceHistory. The same reasoning is why the two family tables are flattened by one
// function rather than concatenated by their readers: DescribeInstanceTypes,
// DescribeInstanceTypeOfferings and DescribeSpotPriceHistory each walk the catalog once and cannot
// disagree about what is in it.
var ec2InstanceTypeCatalog, ec2InstanceTypeIndex = buildEC2InstanceTypeCatalog()

// buildEC2InstanceTypeCatalog flattens both family tables into the catalog slice and its
// by-name index.
//
// Every catalog entry supports both on-demand and spot. That is true of every family listed,
// so it is applied here rather than repeated per row.
//
// The architecture is the family's ([ec2InstanceTypeFamily.Architecture]), because the
// Graviton families report arm64 (#1497). The accelerated families carry the field too, for
// g5g, the Graviton row in that table ([ec2AcceleratedFamily.Architecture], #1509).
//
// CurrentGeneration is not applied that way, because it is the one property on which the
// families disagree: it is derived per family through [ec2FamilyIsCurrentGeneration], which
// reads AWS's own previous-generation table. Both loops pass the family name rather than
// re-deriving it from the type name, so the classification cannot drift from the table the
// size came out of.
func buildEC2InstanceTypeCatalog() ([]ec2InstanceTypeInfo, map[string]ec2InstanceTypeInfo) {
	var catalog []ec2InstanceTypeInfo
	index := make(map[string]ec2InstanceTypeInfo)
	add := func(family, arch, name string, vcpus, memoryMiB, gpu, neuron int, spotPrice string) {
		info := ec2InstanceTypeInfo{
			InstanceType:          name,
			VCpus:                 vcpus,
			MemoryMiB:             memoryMiB,
			GPU:                   gpu,
			NeuronDevices:         neuron,
			SpotPrice:             spotPrice,
			SupportedArchs:        []string{arch},
			SupportedUsageClasses: []string{"on-demand", "spot"},
			CurrentGeneration:     ec2FamilyIsCurrentGeneration(family),
		}
		catalog = append(catalog, info)
		index[info.InstanceType] = info
	}
	for _, family := range ec2InstanceTypeFamilies {
		for _, size := range family.Sizes {
			add(family.Name, family.Architecture, family.Name+"."+size.Size, size.VCpus, size.MemoryMiB, 0, 0, size.SpotPrice)
		}
	}
	for _, family := range ec2AcceleratedFamilies {
		for _, size := range family.Sizes {
			// The count reaches exactly one member, or none. A family naming a member
			// substrate does not model reports nothing rather than falling back to one
			// that is modeled — a count under the wrong member is worse than an absent
			// one, because a caller cannot tell it apart from a real answer.
			var gpu, neuron int
			switch family.Reports {
			case ec2AcceleratorGPU:
				gpu = size.Accelerators
			case ec2AcceleratorNeuron:
				neuron = size.Accelerators
			case ec2AcceleratorNone:
				// Reported nowhere; the count stays in the table as the record of it.
			}
			add(family.Name, family.Architecture, family.Name+"."+size.Size, size.VCpus, size.MemoryMiB, gpu, neuron, size.SpotPrice)
		}
	}
	return catalog, index
}

// ec2SeededAZSuffixes are the Availability Zone letters the emulator reports for every
// region. DescribeAvailabilityZones, DescribeInstanceTypeOfferings and
// DescribeSpotPriceHistory all derive their zone names from this one list, so a caller
// filtering an offerings query by a zone DescribeAvailabilityZones reported gets an
// answer rather than an empty set.
var ec2SeededAZSuffixes = []string{"a", "b", "c"}

// ec2InvalidInstanceTypeError returns the error EC2 raises when DescribeInstanceTypes is
// asked for types that do not exist.
//
// Provenance, which is split. The *code* is documented: EC2's client-error table lists
// InvalidInstanceType. The *message* comes from a single capture against real us-east-1,
// reported in #485 alongside the substrate response it was diffed against — one type
// produced `The following supplied instance types do not exist: [zz9.nonexistent]`, so
// the brackets and the plural phrasing are observed, and the list form is what the
// capture shows for a single element. The separator for a multi-type list is **not**
// corroborated; ", " is substrate's choice, and a consumer must dispatch on the code.
func ec2InvalidInstanceTypeError(types []string) *AWSError {
	return &AWSError{
		Code:       "InvalidInstanceType",
		Message:    "The following supplied instance types do not exist: [" + strings.Join(types, ", ") + "]",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2CheckInstanceTypesExist returns an error naming every requested type absent from
// [ec2InstanceTypeCatalog], in request order, or nil.
//
// This is deliberately asymmetric with the instance-type *filter*, which answers an
// unmatched value with an empty result set and HTTP 200. The two are different questions:
// InstanceType.N asserts the types exist, so a type that does not is a bad request, while
// a filter narrows a result set, so a value that matches nothing is a legitimate empty
// answer. #485 diffed both against real AWS and they diverge there too.
//
// All misses are collected into one error rather than raising on the first, because the
// captured message carries a list.
func ec2CheckInstanceTypesExist(types []string) *AWSError {
	var unknown []string
	for _, t := range types {
		if _, ok := ec2InstanceTypeIndex[t]; !ok {
			unknown = append(unknown, t)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	return ec2InvalidInstanceTypeError(unknown)
}

// ec2InvalidFilterError returns the error EC2 raises for a filter name an operation does
// not accept.
//
// Provenance: the code is documented — EC2's client-error table lists
// InvalidParameterValue as covering a value that "is not valid, is unsupported, or cannot
// be used". The message is substrate's own; no capture of a rejected filter name exists,
// and the API reference's Errors sections describe conditions rather than strings. It
// names the offending filter because that is the one thing a caller needs.
func ec2InvalidFilterError(name string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterValue",
		Message:    fmt.Sprintf("The filter %q is not valid for this request", name),
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2InvalidLocationTypeError returns the error EC2 raises for a LocationType outside the
// documented set.
//
// DescribeInstanceTypeOfferings' reference gives the valid values as "region |
// availability-zone | availability-zone-id | outpost", so a value outside that set is a
// bad request whether or not substrate models it. Code documented, message substrate's
// own — the wording follows [ec2UnknownInstanceAttribute]'s captured form, which is the
// closest corroborated analog for a rejected EC2 parameter value.
func ec2InvalidLocationTypeError(value string) *AWSError {
	return &AWSError{
		Code:       "InvalidParameterValue",
		Message:    "Value (" + value + ") for parameter LocationType is invalid",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2UnmodelledLocationTypeError returns the error substrate raises for a LocationType
// real EC2 accepts but substrate does not model.
//
// outpost is the only one left. Its location is an Outpost ARN — the reference reads
// "outpost - The Outpost ARN. When you specify a location filter, it must be an Outpost ARN
// for the current Region" — and substrate models no Outpost at all, so there is no ARN it
// could report. Refusing is the honest answer: treating it as availability-zone would return
// zone *names* under a locationType saying they are Outpost ARNs, which a caller matching the
// two would silently mis-read. The message names substrate so the divergence is not mistaken
// for AWS behavior.
//
// availability-zone-id is modeled because [ec2SeededZones] produces a zone's name and ID from
// one entry, and the offerings handler takes the ID from there (#893). There is no seeded
// Outpost to key by, so outpost stays refused.
func ec2UnmodelledLocationTypeError(value string) *AWSError {
	return &AWSError{
		Code: "InvalidParameterValue",
		Message: "Value (" + value + ") for parameter LocationType is not modeled by " +
			"substrate; use availability-zone, availability-zone-id or region",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2FilterValueMatches reports whether value satisfies one EC2 filter value.
//
// This is the comparison behind *every* EC2 describe filter (#697), so the rules below
// are the whole family's, not one operation's. EC2's resource-filtering documentation gives
// them for API filters: "An asterisk (*) matches zero or more characters, and a question mark
// (?) matches zero or one character", "Your search can include the literal values of the
// wildcard characters; you just need to escape them with a backslash before the character.
// For example, a value of `\*amazon\?\\` searches for the literal string `*amazon?\`", and
// "Filter values are case sensitive" — which the Filter type's own reference page repeats, so
// the comparison here is case-sensitive too.
//
// **'?' matches zero or one character, and AWS's own page disagrees with itself about that.**
// #697 reports the contradiction and this is its resolution. Using_Filtering's normative
// "Filtering considerations" list — the one that governs the API rather than the console —
// says "zero or one", and the console's wildcard section says "zero or one" and works it
// through: "if you have a data set with the values prod, prods, and production, a search of
// prod* matches all values, whereas prod? matches only prod and prods". One later sentence in
// the CLI examples says "The ? wildcard matches exactly 1 character" — and is contradicted by
// its own example in the next breath, which returns "the snapshots whose description is
// 'database' or 'database' followed by one character", and by `database????` returning
// descriptions with "database" followed by **up to** four characters. Two normative statements
// and three worked examples say zero-or-one; one sentence says otherwise and refutes itself.
// So zero-or-one stands, and substrate is not narrowing to the outlier sentence.
//
// DescribeTags' Example 4 does not settle it either way: `?ebserver` finding "webserver or
// Webserver" is consistent with both readings, because the third string a zero-or-one '?'
// would also match — "ebserver" — is simply not in AWS's data set.
//
// path.Match is not used because its '?' matches exactly one character where EC2's
// matches zero or one, and because it treats '/' specially.
func ec2FilterValueMatches(pattern, value string) bool {
	return ec2globMatch([]rune(pattern), []rune(value))
}

// ec2globMatch is [ec2FilterValueMatches] over runes. Matching is per character rather
// than per byte so a multi-byte value is not split mid-rune by '?'.
func ec2globMatch(pattern, value []rune) bool {
	for len(pattern) > 0 {
		switch pattern[0] {
		case '*':
			// Zero or more characters: try every split of the remaining value.
			for i := 0; i <= len(value); i++ {
				if ec2globMatch(pattern[1:], value[i:]) {
					return true
				}
			}
			return false
		case '?':
			// Zero or one character, so both branches must be tried.
			if ec2globMatch(pattern[1:], value) {
				return true
			}
			return len(value) > 0 && ec2globMatch(pattern[1:], value[1:])
		case '\\':
			if len(pattern) == 1 {
				// A trailing backslash has nothing to escape; match it literally.
				return len(value) == 1 && value[0] == '\\'
			}
			if len(value) == 0 || value[0] != pattern[1] {
				return false
			}
			pattern, value = pattern[2:], value[1:]
		default:
			if len(value) == 0 || value[0] != pattern[0] {
				return false
			}
			pattern, value = pattern[1:], value[1:]
		}
	}
	return len(value) == 0
}

// ec2FilterAccepts reports whether value satisfies a filter's value list, which EC2
// joins with OR.
//
// A filter carrying no values at all matches nothing, so an empty list drops every
// result. Treating such a filter as absent would be silent in the same way and wrong in the
// more dangerous direction, since a caller cannot tell an unfiltered answer from a genuine
// match on every resource. It is the rule `tag:<key>` follows (#686) and the one the
// [containsStr] matchers reach with no guard (#696).
//
// A caller must therefore not pass a bare map index: an *absent* filter constrains nothing
// and an empty value list matches nothing, and `filters[name]` yields the same nil slice for
// both. Look the name up with the two-value form and only call this when it is present, as
// [extractEC2Filters] records a valueless filter as a present key.
func ec2FilterAccepts(values []string, value string) bool {
	for _, v := range values {
		if ec2FilterValueMatches(v, value) {
			return true
		}
	}
	return false
}
