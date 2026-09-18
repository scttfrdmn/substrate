package emulator

import (
	"fmt"
	"net/http"
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
	// Sizes are the family's members, smallest first.
	Sizes []ec2InstanceTypeSize
}

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
var ec2InstanceTypeFamilies = []ec2InstanceTypeFamily{
	// Burstable, Intel. 0.0042 USD/GiB.
	{Name: "t3", Sizes: []ec2InstanceTypeSize{
		{"nano", 2, 512, "0.0021"},
		{"micro", 2, 1024, "0.0042"},
		{"small", 2, 2048, "0.0084"},
		{"medium", 2, 4096, "0.0168"},
		{"large", 2, 8192, "0.0336"},
		{"xlarge", 4, 16384, "0.0672"},
		{"2xlarge", 8, 32768, "0.1344"},
	}},
	// Burstable, AMD. 0.0038 USD/GiB.
	{Name: "t3a", Sizes: []ec2InstanceTypeSize{
		{"nano", 2, 512, "0.0019"},
		{"micro", 2, 1024, "0.0038"},
		{"small", 2, 2048, "0.0076"},
		{"medium", 2, 4096, "0.0152"},
		{"large", 2, 8192, "0.0304"},
		{"xlarge", 4, 16384, "0.0608"},
		{"2xlarge", 8, 32768, "0.1216"},
	}},
	// General purpose, Intel. 0.00475 USD/GiB.
	{Name: "m5", Sizes: []ec2InstanceTypeSize{
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
	{Name: "m5a", Sizes: []ec2InstanceTypeSize{
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
	{Name: "c5", Sizes: []ec2InstanceTypeSize{
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
	{Name: "c5a", Sizes: []ec2InstanceTypeSize{
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
	{Name: "r5", Sizes: []ec2InstanceTypeSize{
		{"large", 2, 16384, "0.038"},
		{"xlarge", 4, 32768, "0.076"},
		{"2xlarge", 8, 65536, "0.152"},
		{"4xlarge", 16, 131072, "0.304"},
		{"8xlarge", 32, 262144, "0.608"},
		{"12xlarge", 48, 393216, "0.912"},
		{"16xlarge", 64, 524288, "1.216"},
		{"24xlarge", 96, 786432, "1.824"},
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
// #234 seeded three of these as a single size each — p3.2xlarge, g4dn.xlarge, inf1.xlarge —
// with the recorded reason that "the accelerated families are large, their specs vary
// widely across sizes, and no consumer has asked for more of them". The first two clauses
// are still true and the third stopped being true: #891, #892 and #894 all arrive from a
// consumer probing g6.xlarge, and each was refused with InvalidInstanceType before any of
// its own logic ran. So the exception is retired rather than restated (#896) — every family
// here is complete, which is the invariant [ec2InstanceTypeFamilies] documents and the one
// that makes [ec2CheckInstanceTypesExist]'s refusal honest. p3.8xlarge is a type that
// plainly exists and used to be refused.
//
// vCPU, memory and accelerator counts come from the AWS accelerated-computing instance-type
// guide (https://docs.aws.amazon.com/ec2/latest/instancetypes/ac.html), except p3's, which
// AWS publishes on the previous-generation page
// (https://docs.aws.amazon.com/ec2/latest/instancetypes/pg.html) — p3 is the one family
// here AWS lists as previous generation, and DescribeInstanceTypes reports every catalog
// type as current generation regardless; see docs/services.md.
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
// Neuron SDK drives. That supersedes #234's reading, which was that "real EC2 does not
// report [Inferentia] through gpuInfo" and therefore substrate reports the count nowhere;
// the first half stands and the second no longer follows.
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
	// NVIDIA V100. Previous generation, and the only family here that is. 0.918/61 GiB.
	{Name: "p3", Reports: ec2AcceleratorGPU, Sizes: []ec2AcceleratedSize{
		{"2xlarge", 8, 62464, 1, "0.918"},
		{"8xlarge", 32, 249856, 4, "3.672"},
		{"16xlarge", 64, 499712, 8, "7.344"},
	}},
	// NVIDIA A100 40 GiB. One size is the whole family. 0.0175 USD/GiB.
	{Name: "p4d", Reports: ec2AcceleratorGPU, Sizes: []ec2AcceleratedSize{
		{"24xlarge", 96, 1179648, 8, "20.16"},
	}},
	// NVIDIA A100 80 GiB — AWS's own family, not a p4d size. 0.0195 USD/GiB.
	{Name: "p4de", Reports: ec2AcceleratorGPU, Sizes: []ec2AcceleratedSize{
		{"24xlarge", 96, 1179648, 8, "22.464"},
	}},
	// NVIDIA H100. 0.0225 USD/GiB.
	{Name: "p5", Reports: ec2AcceleratorGPU, Sizes: []ec2AcceleratedSize{
		{"4xlarge", 16, 262144, 1, "5.76"},
		{"48xlarge", 192, 2097152, 8, "46.08"},
	}},
	// NVIDIA T4. 0.01175 USD/GiB. The accelerator count is not monotonic in size and AWS
	// publishes it that way: the 12xlarge carries four and the 16xlarge one.
	{Name: "g4dn", Reports: ec2AcceleratorGPU, Sizes: []ec2AcceleratedSize{
		{"xlarge", 4, 16384, 1, "0.188"},
		{"2xlarge", 8, 32768, 1, "0.376"},
		{"4xlarge", 16, 65536, 1, "0.752"},
		{"8xlarge", 32, 131072, 1, "1.504"},
		{"12xlarge", 48, 196608, 4, "2.256"},
		{"16xlarge", 64, 262144, 1, "3.008"},
	}},
	// NVIDIA A10G. 0.0125 USD/GiB. Same non-monotonic count as g4dn, plus a 24xlarge that
	// carries four where the 48xlarge carries eight.
	{Name: "g5", Reports: ec2AcceleratorGPU, Sizes: []ec2AcceleratedSize{
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
	{Name: "g6", Reports: ec2AcceleratorGPU, Sizes: []ec2AcceleratedSize{
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
	{Name: "inf1", Reports: ec2AcceleratorNeuron, Sizes: []ec2AcceleratedSize{
		{"xlarge", 4, 8192, 1, "0.076"},
		{"2xlarge", 8, 16384, 1, "0.152"},
		{"6xlarge", 24, 49152, 4, "0.456"},
		{"24xlarge", 96, 196608, 16, "1.824"},
	}},
	// AWS Inferentia2. 0.0105 USD/GiB. The count runs 1, 1, 6, 12 — not powers of two.
	{Name: "inf2", Reports: ec2AcceleratorNeuron, Sizes: []ec2AcceleratedSize{
		{"xlarge", 4, 16384, 1, "0.168"},
		{"8xlarge", 32, 131072, 1, "1.344"},
		{"24xlarge", 96, 393216, 6, "4.032"},
		{"48xlarge", 192, 786432, 12, "8.064"},
	}},
	// AWS Trainium. 0.0115 USD/GiB.
	{Name: "trn1", Reports: ec2AcceleratorNeuron, Sizes: []ec2AcceleratedSize{
		{"2xlarge", 8, 32768, 1, "0.368"},
		{"32xlarge", 128, 524288, 16, "5.888"},
	}},
	// AWS Trainium2. 0.0135 USD/GiB. The small size is a 3xlarge, not a 2xlarge.
	{Name: "trn2", Reports: ec2AcceleratorNeuron, Sizes: []ec2AcceleratedSize{
		{"3xlarge", 12, 131072, 1, "1.728"},
		{"48xlarge", 192, 2097152, 16, "27.648"},
	}},
}

// ec2InstanceTypeCatalog is the flattened [ec2InstanceTypeFamilies] followed by the
// flattened [ec2AcceleratedFamilies], in family and then size order.
// ec2InstanceTypeIndex is the same data keyed by type name.
//
// Both are built by one function so a type can never be in one and not the other — the
// eight-type catalog and its parallel spot-price map had exactly that hazard, and a type
// missing from the price map was silently dropped from DescribeSpotPriceHistory. The same
// reasoning is why the two family tables are flattened by one function rather than
// concatenated by their readers: DescribeInstanceTypes, DescribeInstanceTypeOfferings and
// DescribeSpotPriceHistory each walk the catalog once and cannot disagree about what is in
// it.
var ec2InstanceTypeCatalog, ec2InstanceTypeIndex = buildEC2InstanceTypeCatalog()

// buildEC2InstanceTypeCatalog flattens both family tables into the catalog slice and its
// by-name index.
//
// Every catalog entry is x86_64 and supports both on-demand and spot. That is true of
// every family listed, so it is applied here rather than repeated per row; a family with
// a different architecture or usage class would need this widening first. Graviton-based
// accelerated families are the nearest real example — g5g is ARM — which is one reason the
// catalog does not carry them.
func buildEC2InstanceTypeCatalog() ([]ec2InstanceTypeInfo, map[string]ec2InstanceTypeInfo) {
	var catalog []ec2InstanceTypeInfo
	index := make(map[string]ec2InstanceTypeInfo)
	add := func(name string, vcpus, memoryMiB, gpu, neuron int, spotPrice string) {
		info := ec2InstanceTypeInfo{
			InstanceType:          name,
			VCpus:                 vcpus,
			MemoryMiB:             memoryMiB,
			GPU:                   gpu,
			NeuronDevices:         neuron,
			SpotPrice:             spotPrice,
			SupportedArchs:        []string{"x86_64"},
			SupportedUsageClasses: []string{"on-demand", "spot"},
		}
		catalog = append(catalog, info)
		index[info.InstanceType] = info
	}
	for _, family := range ec2InstanceTypeFamilies {
		for _, size := range family.Sizes {
			add(family.Name+"."+size.Size, size.VCpus, size.MemoryMiB, 0, 0, size.SpotPrice)
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
			add(family.Name+"."+size.Size, size.VCpus, size.MemoryMiB, gpu, neuron, size.SpotPrice)
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
// availability-zone-id was refused alongside it until #893, on the reasoning that substrate
// reported AZ IDs from DescribeAvailabilityZones but did not key offerings by them. That
// reasoning was sound, so the fix was to make its first clause false rather than to relax the
// refusal: [ec2SeededZones] already produced a zone's name and ID from one entry, and the
// offerings handler now takes the ID from there. Nothing about the Outpost half changed —
// there is no seeded Outpost to key by.
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
// Since #697 this is the comparison behind *every* EC2 describe filter, so the rules below
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
// result. This function used to treat such a filter as absent, on the argument that
// dropping everything turns a malformed request into a silently empty answer — but the
// unfiltered answer that argument produces is silent in the same way and wrong in the
// more dangerous direction, since a caller cannot tell it from a genuine match on every
// resource. #686 had already settled the shape for `tag:<key>`, and nine of substrate's
// eleven matchers reached the same answer by calling [containsStr] with no guard, so this
// is the majority rule rather than a new one (#696).
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
