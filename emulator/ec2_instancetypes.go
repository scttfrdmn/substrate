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

// ec2InstanceTypeFamily describes one instance-type family in the seeded catalog.
type ec2InstanceTypeFamily struct {
	// Name is the family prefix, e.g. "c5".
	Name string
	// GPUs is the accelerator count every size in the family carries. Sizes within a
	// real family differ here; the catalog holds a single accelerated size per family,
	// so one value per family is enough.
	GPUs int
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
// vCPU and memory figures come from the AWS instance-type guides — general purpose
// (https://docs.aws.amazon.com/ec2/latest/instancetypes/gp.html), compute optimized
// (.../co.html) and memory optimized (.../mo.html). Bare-metal sizes are excluded: they
// are real types, but their specs and behavior are not modeled anywhere else in the
// plugin, so returning them would advertise fidelity that is not there.
//
// SpotPrice values are **deterministic stubs, not AWS prices**, and are not researched
// against AWS pricing — the emulator has no price feed and the values exist so a spot
// price history response has a plausible, stable number in it. Within a family they are a
// fixed rate per GiB of memory, which keeps them monotonic in size. The eight values
// #234's catalog shipped (t3.micro 0.0042, c5.xlarge 0.068, c5.2xlarge 0.136, m5.large
// 0.038, r5.xlarge 0.076, p3.2xlarge 0.918, g4dn.xlarge 0.188, inf1.xlarge 0.076) are
// preserved verbatim and every family's rate is calibrated to them, so no existing
// fixture moves. Assert on the shape of a spot price response, never on the amount.
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
	// Accelerated. These are the single sizes #234 seeded rather than whole families:
	// the accelerated families are large, their specs vary widely across sizes, and no
	// consumer has asked for more of them. p3.2xlarge carries one NVIDIA V100 and
	// g4dn.xlarge one T4; inf1.xlarge's accelerator is an AWS Inferentia chip, which
	// real EC2 does not report through gpuInfo, so its GPU count stays zero.
	{Name: "p3", GPUs: 1, Sizes: []ec2InstanceTypeSize{
		{"2xlarge", 8, 62464, "0.918"},
	}},
	{Name: "g4dn", GPUs: 1, Sizes: []ec2InstanceTypeSize{
		{"xlarge", 4, 16384, "0.188"},
	}},
	{Name: "inf1", Sizes: []ec2InstanceTypeSize{
		{"xlarge", 4, 8192, "0.076"},
	}},
}

// ec2InstanceTypeCatalog is the flattened [ec2InstanceTypeFamilies], in family and then
// size order. ec2InstanceTypeIndex is the same data keyed by type name.
//
// Both are built by one function so a type can never be in one and not the other — the
// eight-type catalog and its parallel spot-price map had exactly that hazard, and a type
// missing from the price map was silently dropped from DescribeSpotPriceHistory.
var ec2InstanceTypeCatalog, ec2InstanceTypeIndex = buildEC2InstanceTypeCatalog()

// buildEC2InstanceTypeCatalog flattens [ec2InstanceTypeFamilies] into the catalog slice
// and its by-name index.
//
// Every catalog entry is x86_64 and supports both on-demand and spot. That is true of
// every family listed, so it is applied here rather than repeated per row; a family with
// a different architecture or usage class would need this widening first.
func buildEC2InstanceTypeCatalog() ([]ec2InstanceTypeInfo, map[string]ec2InstanceTypeInfo) {
	var catalog []ec2InstanceTypeInfo
	index := make(map[string]ec2InstanceTypeInfo)
	for _, family := range ec2InstanceTypeFamilies {
		for _, size := range family.Sizes {
			info := ec2InstanceTypeInfo{
				InstanceType:          family.Name + "." + size.Size,
				VCpus:                 size.VCpus,
				MemoryMiB:             size.MemoryMiB,
				GPU:                   family.GPUs,
				SpotPrice:             size.SpotPrice,
				SupportedArchs:        []string{"x86_64"},
				SupportedUsageClasses: []string{"on-demand", "spot"},
			}
			catalog = append(catalog, info)
			index[info.InstanceType] = info
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
// availability-zone-id and outpost are both valid values. Substrate reports AZ IDs from
// DescribeAvailabilityZones but does not key offerings by them, and models no Outposts at
// all. Refusing is the honest answer: treating either as availability-zone would return
// zone *names* under a locationType saying they are IDs or ARNs, which a caller matching
// the two would silently mis-read. The message names substrate so the divergence is not
// mistaken for AWS behavior.
func ec2UnmodelledLocationTypeError(value string) *AWSError {
	return &AWSError{
		Code: "InvalidParameterValue",
		Message: "Value (" + value + ") for parameter LocationType is not modeled by " +
			"substrate; use availability-zone or region",
		HTTPStatus: http.StatusBadRequest,
	}
}

// ec2FilterValueMatches reports whether value satisfies one EC2 filter value.
//
// EC2's resource-filtering documentation gives the rules for API filters: "An asterisk
// (*) matches zero or more characters, and a question mark (?) matches zero or one
// character", a literal wildcard is escaped with a preceding backslash, and filter values
// are case-sensitive — so the comparison here is too. DescribeInstanceTypes' reference
// documents the form explicitly for this filter: "instance-type - The instance type (for
// example c5.2xlarge or c5*)".
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
// A filter carrying no values at all is treated as absent rather than as matching
// nothing: [extractEC2Filters] records such a filter with an empty list, and dropping
// every result for it would turn a malformed request into a silently empty answer.
func ec2FilterAccepts(values []string, value string) bool {
	if len(values) == 0 {
		return true
	}
	for _, v := range values {
		if ec2FilterValueMatches(v, value) {
			return true
		}
	}
	return false
}
